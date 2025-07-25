use anyhow::Result;

use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Mutex;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use dashmap::DashMap;
use futures::future::join_all;

use crate::error::FsError;
use crate::object_store::{MIN_PART_SIZE, ObjectStore};

/// The state for a single, in-progress file upload.
struct InProgressWrite {
    s3_key: String,
    upload_id: String,
    // Buffer for the *next* part to be uploaded.
    buffer: Vec<u8>,
    part_number: i32,
    // Parts that have been successfully uploaded. Must be behind a Mutex
    // to be safely updated by multiple concurrent upload tasks.
    completed_parts: Arc<Mutex<Vec<CompletedPart>>>,
    // Handles to the background upload tasks.
    upload_tasks: Vec<tokio::task::JoinHandle<Result<(), FsError>>>,
    // Shared resources.
    object_store: Arc<ObjectStore>,
    rt_handle: Handle,
    total_written: u64,
}

/// Manages all active file write operations.
pub struct WriteManager {
    // Maps a file handle (`fh`) to its in-progress write state.
    active_writes: DashMap<u64, InProgressWrite>,
    object_store: Arc<ObjectStore>,
    rt: Handle,
}

impl WriteManager {
    pub fn new(object_store: Arc<ObjectStore>, rt: Handle) -> Result<Self> {
        Ok(Self {
            active_writes: DashMap::new(),
            object_store,
            rt,
        })
    }

    /// Starts a new upload process for a given file handle and S3 key.
    /// This is called from `create()` or `open()`.
    pub async fn start_upload(&self, fh: u64, s3_key: String) -> Result<(), FsError> {
        tracing::debug!("start_upload: fh={}, key='{}'", fh, s3_key);
        // Immediately create an MPU to get an Upload ID.
        let upload_id = self
            .object_store
            .s3
            .create_multipart_upload()
            .bucket(&self.object_store.bucket)
            .key(&s3_key)
            .send()
            .await?
            .upload_id
            .ok_or_else(|| FsError::S3("S3 did not return an upload ID".into()))?;

        let state = InProgressWrite {
            s3_key,
            upload_id,
            buffer: Vec::with_capacity(MIN_PART_SIZE),
            part_number: 1,
            completed_parts: Arc::new(Mutex::new(Vec::new())),
            upload_tasks: Vec::new(),
            object_store: self.object_store.clone(),
            rt_handle: self.rt.clone(),
            total_written: 0,
        };

        self.active_writes.insert(fh, state);
        Ok(())
    }

    /// Appends data to a file's buffer and flushes a part if the buffer is full.
    /// This is called from `write()`.
    pub fn write_data(&self, fh: u64, data: &[u8]) -> Result<(), FsError> {
        let mut write_state = self
            .active_writes
            .get_mut(&fh)
            .ok_or(FsError::InvalidHandle)?;

        write_state.buffer.extend_from_slice(data);

        write_state.total_written += data.len() as u64;

        // If the buffer is full enough, drain a chunk and spawn a background task to upload it.
        if write_state.buffer.len() >= MIN_PART_SIZE {
            // Leave any remainder in the buffer for the next write.
            let remainder = write_state.buffer.split_off(MIN_PART_SIZE);
            let chunk_to_upload = std::mem::replace(&mut write_state.buffer, remainder);

            self.spawn_upload_task(&mut write_state, chunk_to_upload);
        }
        Ok(())
    }

    /// Finishes an upload, handling both large (multipart) and small (single PUT) files.
    /// This is called from `release()`.
    pub async fn finish_upload(&self, fh: u64) -> Result<u64, FsError> {
        // Remove the state from the active map. If it's already gone, we're done.
        let Some((_, mut write_state)) = self.active_writes.remove(&fh) else {
            return Ok(0);
        };
        let final_size = write_state.total_written; // <-- Get the final size we tracked.
        tracing::debug!(
            "finish_upload: fh={}, key='{}', final_size={}",
            fh,
            &write_state.s3_key,
            final_size
        );

        // CASE 1: The file is small. No parts were ever uploaded.
        // We abort the MPU and do a single, efficient PutObject instead.
        if write_state.upload_tasks.is_empty() {
            tracing::debug!(
                "Finishing as a small file upload for key '{}'",
                &write_state.s3_key
            );
            // Abort the multipart upload that we started but never used.
            self.abort_mpu(&write_state).await?;

            // Perform a single PutObject.
            self.object_store
                .s3
                .put_object()
                .bucket(&self.object_store.bucket)
                .key(&write_state.s3_key)
                .body(ByteStream::from(write_state.buffer))
                .send()
                .await?;
            return Ok(final_size);
        }

        // CASE 2: This is a large file. We need to finish the MPU.
        // If there's any data left in the buffer, upload it as the final part.
        if !write_state.buffer.is_empty() {
            let final_chunk = std::mem::take(&mut write_state.buffer);
            self.spawn_upload_task(&mut write_state, final_chunk);
        }

        let tasks_to_await = std::mem::take(&mut write_state.upload_tasks);
        let task_results = join_all(tasks_to_await).await;

        //let task_results = join_all(write_state.upload_tasks).await;

        // Check for any failures. If even one task failed, we must abort the whole upload.
        for result in task_results {
            match result {
                Ok(Ok(_)) => { /* This part succeeded */ }
                _ => {
                    tracing::error!(
                        "A part failed to upload for key '{}', aborting.",
                        &write_state.s3_key
                    );
                    self.abort_mpu(&write_state).await?;
                    return Err(FsError::S3("A part failed to upload".into()));
                }
            }
        }

        // All parts succeeded. Now we complete the MPU.
        let mut final_parts = write_state.completed_parts.lock().await;
        final_parts.sort_by_key(|p| p.part_number);

        tracing::error!(
            "CRITICAL_DIAGNOSIS: About to complete MPU. Key: '{}'. Number of parts found: {}",
            &write_state.s3_key,
            final_parts.len()
        );
        // The {:?} formatter will print the contents of the vector, including ETags.
        tracing::debug!(
            "CRITICAL_DIAGNOSIS: Parts list being sent to S3: {:?}",
            final_parts
        );
        // --- END DIAGNOSTIC LOGS ---

        // For safety, let's add a check here. This turns the theory into a hard error.
        if final_parts.is_empty() && final_size > 0 {
            tracing::error!(
                "FATAL LOGIC ERROR: Upload tasks finished but no completed parts were collected. Aborting."
            );
            self.abort_mpu(&write_state).await?;
            return Err(FsError::S3(
                "FATAL: No completed parts found for multipart upload.".into(),
            ));
        }

        let mpu_parts = CompletedMultipartUpload::builder()
            .set_parts(Some(final_parts.to_vec()))
            .build();

        self.object_store
            .s3
            .complete_multipart_upload()
            .bucket(&self.object_store.bucket)
            .key(&write_state.s3_key)
            .upload_id(&write_state.upload_id)
            .multipart_upload(mpu_parts)
            .send()
            .await?;

        tracing::debug!(
            "Successfully completed multipart upload for key '{}'",
            &write_state.s3_key
        );
        Ok(final_size)
    }

    /// Helper to spawn a background task for uploading one part.
    fn spawn_upload_task(&self, write_state: &mut InProgressWrite, chunk: Vec<u8>) {
        let s3_key = write_state.s3_key.clone();
        let upload_id = write_state.upload_id.clone();
        let part_number = write_state.part_number;
        let object_store = write_state.object_store.clone();
        let completed_parts_arc = write_state.completed_parts.clone();

        tracing::debug!(
            "Spawning upload task for part {} of key '{}'",
            part_number,
            &s3_key
        );

        let task = write_state.rt_handle.spawn(async move {
            let part_resp = object_store
                .s3
                .upload_part()
                .bucket(&object_store.bucket)
                .key(&s3_key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(chunk))
                .send()
                .await
                .map_err(|e| FsError::S3(e.to_string()))?;

            let e_tag = part_resp
                .e_tag
                .ok_or_else(|| FsError::S3("UploadPart response missing ETag".into()))?;
            let completed = CompletedPart::builder()
                .e_tag(e_tag)
                .part_number(part_number)
                .build();

            completed_parts_arc.lock().await.push(completed);
            tracing::debug!("finish upload for part {} of key {}", part_number, &s3_key);
            Ok(())
        });

        write_state.upload_tasks.push(task);
        write_state.part_number += 1;
    }

    /// Helper to abort a multipart upload.
    async fn abort_mpu(&self, write_state: &InProgressWrite) -> Result<(), FsError> {
        self.object_store
            .s3
            .abort_multipart_upload()
            .bucket(&self.object_store.bucket)
            .key(&write_state.s3_key)
            .upload_id(&write_state.upload_id)
            .send()
            .await?;
        Ok(())
    }

    pub fn get_live_size(&self, fh: u64) -> Option<u64> {
        // We only need a read lock (`get`) which is highly concurrent.
        self.active_writes.get(&fh).map(|write_state| {
            // The live size is simply the total we've been tracking.
            write_state.total_written
        })
    }
}
