use crate::error::FsError;
use aws_sdk_s3::{
    Client,
    operation::head_object::HeadObjectOutput,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, MetadataDirective, ObjectIdentifier},
};
use dashmap::DashMap;
use fuser::FileType;
use futures::future::join_all;

// Constants for Multipart Upload
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB
const MULTIPART_THRESHOLD: usize = 10 * 1024 * 1024; // 10MB

pub struct ObjectStore {
    pub s3: Client,
    pub bucket: String,
    uploads: DashMap<String, String>,
}

#[derive(Debug)]
pub struct DirectoryEntry {
    /// The simple name of the file or subdirectory (e.g., "file.txt", "subdir")
    pub name: String,
    /// The type of the entry (RegularFile or Directory)
    pub kind: FileType,
}

#[derive(Debug)]
pub enum ObjectKind {
    File(HeadObjectOutput),
    Directory(Option<HeadObjectOutput>), // Implcit dirs that have no HeadObject from s3. so option will be None
}

#[derive(Debug, Clone, Default)]
pub struct ObjectUserMetadata {
    pub mode: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

// NOTE: This can be made a generic non-S3 specific trait to be implemented for various clients.
impl ObjectStore {
    pub async fn new(bucket: String) -> Result<Self, FsError> {
        let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let s3 = Client::new(&conf);

        s3.head_bucket().bucket(&bucket).send().await.map_err(|e| {
            eprintln!("Failed to connect to bucket '{}': {}", bucket, e);
            FsError::S3(format!("Bucket '{}' not found or no permissions", bucket))
        })?;

        Ok(Self {
            s3,
            bucket,
            uploads: DashMap::new(),
        })
    }
}

fn bytes_of_key(upload_id: &str) -> Result<&str, FsError> {
    // store upload_id→key mapping in a DashMap if needed;
    // for now assume caller already knows the key (most callers do)
    Err(FsError::S3(format!(
        "ObjectStore helpers need the object key for upload-id {upload_id}"
    )))
}

impl ObjectStore {
    fn key_for_upload(&self, upload_id: &str) -> Result<String, FsError> {
        self.uploads
            .get(upload_id)
            .map(|k| k.clone())
            .ok_or_else(|| FsError::S3(format!("unknown upload-id {}", upload_id)))
    }

    pub async fn list_directory(&self, prefix: &str) -> Result<Vec<DirectoryEntry>, FsError> {
        let mut entries = Vec::new();
        let mut stream = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .delimiter("/") // The key to a shallow, directory-like list
            .into_paginator()
            .send();

        while let Some(result) = stream.next().await {
            let page = result?;

            // Process common prefixes, which represent subdirectories
            if let Some(common_prefixes) = page.common_prefixes {
                for p in common_prefixes {
                    if let Some(dir_key) = p.prefix {
                        // Extract the simple name from the prefix
                        // e.g., "path/to/subdir/" -> "subdir"
                        let name = dir_key
                            .trim_end_matches('/')
                            .split('/')
                            .last()
                            .unwrap_or("")
                            .to_string();

                        if !name.is_empty() {
                            entries.push(DirectoryEntry {
                                name,
                                kind: FileType::Directory,
                            });
                        }
                    }
                }
            }

            // Process objects, which represent files
            if let Some(objects) = page.contents {
                for obj in objects {
                    if let Some(key) = obj.key {
                        // Skip the directory marker object itself
                        if key == prefix {
                            continue;
                        }
                        // Extract the simple name from the key
                        // e.g., "path/to/file.txt" -> "file.txt"
                        let name = key.split('/').last().unwrap_or("").to_string();

                        if !name.is_empty() {
                            entries.push(DirectoryEntry {
                                name,
                                kind: FileType::RegularFile,
                            });
                        }
                    }
                }
            }
        }
        Ok(entries)
    }

    /// Uploads a block of data to S3, automatically choosing between
    /// a single PutObject and a Multipart Upload based on size.
    pub async fn upload(&self, key: &str, data: Vec<u8>) -> Result<(), FsError> {
        if data.len() < MULTIPART_THRESHOLD {
            // Use simple PutObject for smaller files
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(data))
                .send()
                .await?;
        } else {
            // Use Multipart Upload for larger files
            tracing::info!(
                "upload for {} ({} bytes) is over the MULTIPART_THREADHOLD={}. Switching to multipart uploads",
                key,
                data.len(),
                MULTIPART_THRESHOLD
            );
            self.upload_multipart(key, data).await?;
        }

        Ok(())
    }

    /// Downloads a specific byte range from an S3 object.
    pub async fn download_range(
        &self,
        key: &str,
        start: u64,
        size: u32,
    ) -> Result<ByteStream, FsError> {
        let end = start + (size as u64) - 1;
        let range_str = format!("bytes={}-{}", start, end);

        let output = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range_str)
            .send()
            .await?;
        Ok(output.body)
    }

    /// Creates a new, zero-byte object and sets its initial metadata.
    /// This is the semantically correct method for `create`.
    pub async fn create_object(
        &self,
        key: &str,
        metadata: &ObjectUserMetadata,
    ) -> Result<(), FsError> {
        // Step 1: Create a zero-byte placeholder object. This is an atomic Put.
        // We don't add metadata here to avoid duplicating the logic.
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(Vec::new()))
            .send()
            .await?;

        self.replace_metadata(key, metadata).await?;

        Ok(())
    }

    /// Deletes a single object from S3.
    pub async fn delete_object(&self, key: &str) -> Result<(), FsError> {
        self.s3
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_attributes(&self, key: &str) -> Result<HeadObjectOutput, FsError> {
        let meta = self
            .s3
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|_| FsError::NotFound)?; // Treat any HeadObject error as "not found"
        Ok(meta)
    }

    pub async fn truncate_file(&self, key: &str) -> Result<(), FsError> {
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(Vec::new()))
            .send()
            .await?;
        Ok(())
    }

    /// Replaces the metadata on an object by performing a copy-in-place.
    /// S3 requires that *all* metadata be replaced, so this method takes
    /// the full desired set.
    pub async fn replace_metadata(
        &self,
        key: &str,
        metadata: &ObjectUserMetadata,
    ) -> Result<(), FsError> {
        let copy_source = format!("{}/{}", self.bucket, key);
        let mut request = self
            .s3
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(copy_source)
            .key(key)
            .metadata_directive(MetadataDirective::Replace);

        // Add the metadata fields if they are present
        if let Some(mode) = metadata.mode {
            // FORMAT the integer as an octal string before storing.
            let octal_mode = format!("{:o}", mode);
            request = request.metadata("mode", octal_mode);
        }
        if let Some(uid) = metadata.uid {
            request = request.metadata("uid", uid.to_string());
        }
        if let Some(gid) = metadata.gid {
            request = request.metadata("gid", gid.to_string());
        }

        request.send().await?;
        Ok(())
    }

    /// Determines if a path corresponds to a file or a directory (explicit or implicit).
    pub async fn stat_object(&self, key: &str) -> Result<ObjectKind, FsError> {
        // Check for an explicit directory marker first.
        let dir_key = format!("{}/", key);
        if let Ok(meta) = self.get_attributes(&dir_key).await {
            return Ok(ObjectKind::Directory(Some(meta)));
        }

        // Check for a file.
        if let Ok(meta) = self.get_attributes(key).await {
            return Ok(ObjectKind::File(meta));
        }

        // Check for an implicit directory by listing its contents.
        let entries = self.list_directory(&dir_key).await?;
        if !entries.is_empty() {
            return Ok(ObjectKind::Directory(None));
        }

        // If all checks fail, it doesn't exist.
        Err(FsError::NotFound)
    }

    /// Renames a file by copying it to a new key and deleting the old one.
    /// NOTE: This is NOT ATOMIC.
    pub async fn rename_file(&self, old_key: &str, new_key: &str) -> Result<(), FsError> {
        let copy_source = format!("{}/{}", self.bucket, old_key);
        self.s3
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(copy_source)
            .key(new_key)
            .send()
            .await?;

        self.delete_object(old_key).await?;
        Ok(())
    }

    /// Renames a "directory" by listing, copying, and deleting all objects under a prefix.
    /// NOTE: This is NOT ATOMIC and can be very slow for large directories.
    pub async fn rename_dir(&self, old_prefix: &str, new_prefix: &str) -> Result<(), FsError> {
        let mut objects_to_delete = Vec::new();
        let mut stream = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(old_prefix)
            .into_paginator()
            .send();

        while let Some(result) = stream.next().await {
            let page = result?;
            for object in page.contents.unwrap_or_default() {
                if let Some(key) = object.key {
                    let new_key = key.replacen(old_prefix, new_prefix, 1);
                    let copy_source = format!("{}/{}", self.bucket, key);
                    self.s3
                        .copy_object()
                        .bucket(&self.bucket)
                        .copy_source(copy_source)
                        .key(new_key)
                        .send()
                        .await?;

                    objects_to_delete.push(ObjectIdentifier::builder().key(key).build()?);
                }
            }
        }

        if !objects_to_delete.is_empty() {
            self.s3
                .delete_objects()
                .bucket(&self.bucket)
                .delete(
                    Delete::builder()
                        .set_objects(Some(objects_to_delete))
                        .build()?,
                )
                .send()
                .await?;
        }
        Ok(())
    }

    // In the impl ObjectStore block

    async fn upload_multipart(&self, key: &str, data: Vec<u8>) -> Result<(), FsError> {
        let mpu = self
            .s3
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let upload_id = mpu
            .upload_id
            .ok_or_else(|| FsError::S3("S3 did not return an upload ID".into()))?;

        let mut upload_tasks = Vec::new();

        for (i, chunk) in data.chunks(MIN_PART_SIZE).enumerate() {
            let part_number = (i + 1) as i32;
            let s3_client = self.s3.clone();
            let bucket_clone = self.bucket.clone();
            let key_clone = key.to_string();
            let upload_id_clone = upload_id.clone();
            let chunk_data = chunk.to_vec();

            let task = tokio::task::spawn(async move {
                // Map the S3 SDK error to our custom FsError for a consistent return type.
                let part_resp = s3_client
                    .upload_part()
                    .bucket(&bucket_clone)
                    .key(&key_clone)
                    .upload_id(&upload_id_clone)
                    .part_number(part_number)
                    .body(ByteStream::from(chunk_data))
                    .send()
                    .await
                    .map_err(|e| FsError::S3(e.to_string()))?;

                // ETag is optional in the response, but required for CompletedPart.
                // We must handle the case where it's missing.
                let e_tag = part_resp.e_tag.ok_or_else(|| {
                    FsError::S3(
                        "S3 response for an uploaded part was missing the required ETag".into(),
                    )
                })?;

                // --- FIX IS HERE ---
                // .build() is now infallible because the compiler ensures e_tag and part_number are set.
                // It returns a `CompletedPart` directly, not a `Result`.
                let completed_part = CompletedPart::builder()
                    .e_tag(e_tag)
                    .part_number(part_number)
                    .build(); // No `?` or `map_err` needed here.

                Ok::<_, FsError>(completed_part)
            });

            upload_tasks.push(task);
        }

        let task_results = join_all(upload_tasks).await;

        let mut completed_parts = Vec::new();
        for result in task_results {
            match result {
                Ok(Ok(part)) => {
                    completed_parts.push(part);
                }
                Ok(Err(fs_error)) => {
                    tracing::error!(
                        "A part upload failed for key '{}', aborting multipart upload. Error: {}",
                        key,
                        fs_error
                    );
                    self.s3
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send()
                        .await?;
                    return Err(fs_error);
                }
                Err(join_error) => {
                    tracing::error!(
                        "A part upload task failed for key '{}', aborting multipart upload. Error: {}",
                        key,
                        join_error
                    );
                    self.s3
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .send()
                        .await?;
                    return Err(FsError::S3(join_error.to_string()));
                }
            }
        }

        completed_parts.sort_by_key(|p| p.part_number);

        let completed_parts_len = completed_parts.len();
        let mpu_parts = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.s3
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(mpu_parts)
            .send()
            .await?;

        tracing::debug!(
            "Successfully uploaded {} parts for key '{}'",
            completed_parts_len,
            key
        );
        Ok(())
    }

    pub async fn start_multipart(&self, key: &str) -> Result<String, FsError> {
        let out = self
            .s3
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let upload_id = out
            .upload_id
            .ok_or_else(|| FsError::S3("S3 did not return an upload-id".into()))?;

        // remember the mapping so later calls don’t need the key
        self.uploads.insert(upload_id.clone(), key.to_string());
        Ok(upload_id)
    }

    /// Upload a single part and return the `CompletedPart`.
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_no: i32,
        bytes: Vec<u8>,
    ) -> Result<CompletedPart, FsError> {
        let key = self.key_for_upload(upload_id)?;

        let out = self
            .s3
            .upload_part()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_no)
            .body(ByteStream::from(bytes))
            .send()
            .await?;

        let etag = out.e_tag.ok_or_else(|| {
            FsError::S3("S3 response for an uploaded part was missing the required ETag".into())
        })?;

        Ok(CompletedPart::builder()
            .e_tag(etag)
            .part_number(part_no)
            .build())
    }

    /// Finish (or abort) the MPU and clean up the map.
    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        parts: &[CompletedPart],
    ) -> Result<(), FsError> {
        let key = self.key_for_upload(upload_id)?;

        let mpu_parts = CompletedMultipartUpload::builder()
            .set_parts(Some(parts.to_vec()))
            .build();

        self.s3
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(mpu_parts)
            .send()
            .await?;

        // success → drop the mapping
        self.uploads.remove(upload_id);
        Ok(())
    }

    // Helper so we don’t re-compute the key in every ca
}
