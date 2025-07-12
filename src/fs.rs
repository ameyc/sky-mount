use anyhow::Result;
use aws_sdk_s3::{
    Client, error::SdkError, operation::head_object::HeadObjectError, primitives::ByteStream,
    types::CompletedPart,
};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
    TimeOrNow,
};
use std::{
    collections::HashMap,
    ffi::OsStr,
    path::PathBuf,
    sync::Mutex,
    time::{Duration, SystemTime},
};
use tokio::runtime::Runtime;

use std::path::Path;

const TTL: Duration = Duration::from_secs(1);
const S3_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024; // 5 TiB

// Constants for Multipart Upload
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB: The minimum part size for S3 MPU
const MULTIPART_THRESHOLD: usize = 10 * 1024 * 1024; // 10MB: Files larger than this will use MPU

/// Represents the state of a file being written.
enum WriteState {
    /// For small files, buffer the entire content in memory.
    Buffered(Vec<u8>),
    /// For large files, use S3's Multipart Upload.
    Multipart {
        upload_id: String,
        completed_parts: Vec<CompletedPart>,
        // A buffer for the current part being assembled.
        buffer: Vec<u8>,
        next_part_number: i32,
    },
}

fn path_to_ino(path: &Path) -> u64 {
    // Inode 1 is special (root). Don't we dont ever generate it.
    // We hash the path string to get a unique u64.
    if path.as_os_str() == "/" {
        1
    } else {
        // Ensure that the generated hash is greater than 1
        seahash::hash(path.as_os_str().as_encoded_bytes()) + 2
    }
}

const ROOT_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: std::time::UNIX_EPOCH,
    mtime: std::time::UNIX_EPOCH,
    ctime: std::time::UNIX_EPOCH,
    crtime: std::time::UNIX_EPOCH,
    kind: fuser::FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
    blksize: 0,
};

fn s3_meta_to_file_attr(
    ino: u64,
    key: &str,
    meta: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    uid: u32,
    gid: u32,
) -> FileAttr {
    let kind = if key.ends_with('/') {
        FileType::Directory
    } else {
        FileType::RegularFile
    };
    let perm = 0o755;
    FileAttr {
        ino,
        size: meta.content_length.unwrap_or(0) as u64,
        blocks: 0,
        atime: meta
            .last_modified()
            .and_then(|dt| SystemTime::try_from(*dt).ok())
            .unwrap_or(SystemTime::UNIX_EPOCH),
        mtime: meta
            .last_modified()
            .and_then(|dt| SystemTime::try_from(*dt).ok())
            .unwrap_or(SystemTime::UNIX_EPOCH),
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        kind,
        perm,
        nlink: 1,
        uid: uid,
        gid: gid,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

pub struct S3Fuse {
    bucket: String,
    s3: Client,
    rt: Runtime,
    ino_to_path_cache: HashMap<u64, PathBuf>,
    next_fh: u64,
    open_files: Mutex<HashMap<u64, WriteState>>,
    mount_uid: u32,
    mount_gid: u32,
}

impl S3Fuse {
    pub fn new(bucket: String) -> Result<Self> {
        let rt = Runtime::new()?;

        let (_conf, client) = rt.block_on(async {
            let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = Client::new(&conf);
            (conf, client)
        });

        Ok(Self {
            bucket,
            s3: client,
            rt,
            ino_to_path_cache: {
                let mut m = HashMap::new();
                m.insert(1, PathBuf::from("/"));
                m
            },
            next_fh: 1,
            open_files: Mutex::new(HashMap::new()),
            mount_uid: 0,
            mount_gid: 0,
        })
    }

    /// Helper to bridge the sync FUSE world to the async S3 world.
    /// It spawns the async task on the Tokio runtime and blocks the current
    /// (FUSE) thread efficiently until the result is ready.
    fn block_on_s3<F, T>(&self, future: F) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        self.rt.spawn(async move {
            let result = future.await;
            let _ = tx.send(result);
        });
        rx.recv().expect("S3 task panicked or channel was closed")
    }

    async fn upload_part(
        &self,
        upload_id: &str,
        key: &str,
        part_number: i32,
        data: Vec<u8>,
    ) -> Result<CompletedPart, SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>> {
        let stream = ByteStream::from(data);
        let part_resp = self
            .s3
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(stream)
            .send()
            .await?;

        Ok(CompletedPart::builder()
            .e_tag(part_resp.e_tag.unwrap_or_default())
            .part_number(part_number)
            .build())
    }
}

impl Filesystem for S3Fuse {
    fn init(
        &mut self,
        req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        tracing::info!("Checking bucket '{}' exists...", self.bucket);
        self.mount_uid = req.uid();
        self.mount_gid = req.gid();
        match self
            .rt
            .block_on(self.s3.head_bucket().bucket(&self.bucket).send())
        {
            Ok(_) => {
                tracing::info!("Bucket found. S3Fuse initialised successfully.");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to connect to bucket '{}': {}", self.bucket, e);
                Err(libc::EIO)
            }
        }
    }

    /// Look up a file or directory by name.
    fn lookup(&mut self, _req: &Request<'_>, parent_ino: u64, name: &OsStr, reply: ReplyEntry) {
        // 1. Get the parent path from our cache.
        let parent_path = match self.ino_to_path_cache.get(&parent_ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // 2. Construct the full path for the item we're looking up.
        let child_path = parent_path.join(name);
        // S3 keys don't start with a '/', so we strip it.
        let key = child_path.to_str().unwrap_or("").trim_start_matches('/');

        if key.is_empty() {
            // Should only happen for lookup of "/" itself.
            let attr = FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            };
            reply.entry(&TTL, &attr, 0);
            return;
        }

        tracing::debug!("lookup: trying key '{}'", key);

        // 3. Query S3 for the object's metadata.
        let fut = self.s3.head_object().bucket(&self.bucket).key(key).send();
        match self.rt.block_on(fut) {
            Ok(meta) => {
                // It's a file!
                let ino = path_to_ino(&child_path);
                let attr = s3_meta_to_file_attr(ino, key, &meta, self.mount_uid, self.mount_gid);

                // Cache the new inode -> path mapping.
                self.ino_to_path_cache.insert(ino, child_path);

                tracing::debug!("lookup: found file with attr {:?}", &attr);
                reply.entry(&TTL, &attr, 0);
            }
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), HeadObjectError::NotFound(_)) =>
            {
                // Lets try key/ in case we hit an implicit directory.
                let dir_key = format!("{}/", key);
                tracing::debug!(
                    "lookup: file not found, trying as directory with prefix '{}'",
                    &dir_key
                );
                let fut = self
                    .s3
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&dir_key)
                    .delimiter("/")
                    .send();

                match self.rt.block_on(fut) {
                    Ok(list_output) => {
                        if list_output.key_count.unwrap_or(0) > 0 {
                            // It's a directory!
                            let ino = path_to_ino(&child_path);
                            let attr = FileAttr {
                                ino,
                                kind: FileType::Directory,
                                perm: 0o755,
                                uid: self.mount_uid,
                                gid: self.mount_gid,
                                ..ROOT_ATTR
                            };
                            self.ino_to_path_cache.insert(ino, child_path);
                            tracing::debug!("lookup: found directory with attr {:?}", &attr);
                            reply.entry(&TTL, &attr, 0);
                        } else {
                            // Doesn't exist as a file or a directory.
                            tracing::debug!("lookup: path not found as file or directory");
                            reply.error(libc::ENOENT);
                        }
                    }
                    Err(e) => {
                        tracing::error!("lookup: S3 list_objects_v2 error: {}", e);
                        reply.error(libc::EIO);
                    }
                }
            }
            Err(e) => {
                tracing::error!("lookup: S3 head_object error: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    /// Get file attributes.
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == 1 {
            let attr = FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            };
            reply.attr(&TTL, &attr);
            return;
        }

        // Find the path from the inode in our cache.
        let path = match self.ino_to_path_cache.get(&ino).cloned() {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let key = path.to_str().unwrap_or("").trim_start_matches('/');
        tracing::debug!("getattr: ino {} -> key '{}'", ino, key);

        // Query S3 for fresh metadata.
        let fut = self.s3.head_object().bucket(&self.bucket).key(key).send();
        match self.rt.block_on(fut) {
            Ok(meta) => {
                let attr = s3_meta_to_file_attr(ino, key, &meta, self.mount_uid, self.mount_gid);
                reply.attr(&TTL, &attr);
            }
            Err(_) => {
                // It might be a directory, which won't be found by HeadObject on the plain key.
                // For simplicity, we return the directory defaults.
                if self.ino_to_path_cache.get(&ino).map_or(false, |p| {
                    p.is_dir() || p.to_str().unwrap_or("").ends_with('/')
                }) {
                    let attr = FileAttr {
                        ino,
                        kind: FileType::Directory,
                        perm: 0o755,
                        uid: self.mount_uid,
                        gid: self.mount_gid,
                        ..ROOT_ATTR
                    };
                    reply.attr(&TTL, &attr);
                } else {
                    reply.error(libc::ENOENT);
                }
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let key = path_to_s3_key(&path);

        // Handle size changes (truncate) if requested
        if let Some(new_size) = size {
            if new_size != 0 {
                reply.error(libc::EOPNOTSUPP); // only support truncating to 0 for now
                return;
            }
            // Put an empty object to truncate
            if let Err(e) = self.rt.block_on(
                self.s3
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new()))
                    .send(),
            ) {
                tracing::error!("truncate failed: {}", e);
                reply.error(libc::EIO);
                return;
            }
        }

        let meta = self
            .rt
            .block_on(self.s3.head_object().bucket(&self.bucket).key(&key).send())
            .map_err(|_| ());

        let attr = match meta {
            Ok(m) => s3_meta_to_file_attr(ino, &key, &m, self.mount_uid, self.mount_gid),
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // 1. Get the path for the directory from its inode.
        // This part is critical. If the inode isn't in the cache, we MUST fail.
        // The `ls` error suggests this check might be failing.
        tracing::debug!("readdir called. {}", ino);
        let dir_path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.clone(),
            None => {
                tracing::info!("no inode found. returning");
                reply.error(libc::ENOENT);
                return;
            }
        };

        // 2. Add the mandatory '.' and '..' entries FIRST.
        // This is the most critical part to fix the `fts_read` error.
        // We handle the offset to prevent adding them on subsequent readdir calls for the same dir.
        if offset == 0 {
            // The first entry is '.', pointing to the directory itself.
            if reply.add(ino, 1, FileType::Directory, ".") {
                reply.ok();
                return; // Buffer is full, we're done.
            }

            // The second entry is '..', pointing to the parent.
            // The parent of the root is the root itself.
            let parent_ino = dir_path.parent().map_or(1, |p| path_to_ino(p));
            if reply.add(parent_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return; // Buffer is full.
            }
        }

        // A simple readdir implementation often re-fetches everything and just skips
        // the entries that are before the requested offset.
        let mut current_offset = 2i64; // Start after '.' and '..'

        // 3. Convert the path to an S3 prefix.
        let prefix = dir_path.to_str().unwrap_or("").trim_start_matches('/');
        let prefix = if !prefix.is_empty() && !prefix.ends_with('/') {
            format!("{}/", prefix)
        } else {
            prefix.to_string()
        };

        tracing::debug!("readdir: listing objects with prefix '{}'", &prefix);

        // 4. Call S3 and process the results.
        let fut = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .delimiter("/")
            .send();

        match self.rt.block_on(fut) {
            Ok(output) => {
                // Process subdirectories
                if let Some(common_prefixes) = output.common_prefixes {
                    for cp in common_prefixes {
                        current_offset += 1;
                        if current_offset <= offset {
                            continue;
                        } // Skip entries before our offset

                        if let Some(dir_full_path) = cp.prefix() {
                            let dir_name = Path::new(dir_full_path.trim_end_matches('/'))
                                .file_name()
                                .unwrap_or_default();
                            let child_path = dir_path.join(dir_name);
                            let child_ino = path_to_ino(&child_path);
                            self.ino_to_path_cache.insert(child_ino, child_path);
                            if reply.add(child_ino, current_offset, FileType::Directory, dir_name) {
                                break; // Buffer is full
                            }
                        }
                    }
                }

                // Process files
                if let Some(contents) = output.contents {
                    for object in contents {
                        current_offset += 1;
                        if current_offset <= offset {
                            continue;
                        } // Skip entries before our offset

                        if let Some(key) = object.key() {
                            if key.ends_with('/') {
                                continue;
                            }

                            let file_name = Path::new(key).file_name().unwrap_or_default();
                            let child_path = dir_path.join(file_name);
                            let child_ino = path_to_ino(&child_path);
                            self.ino_to_path_cache.insert(child_ino, child_path);

                            if reply.add(
                                child_ino,
                                current_offset,
                                FileType::RegularFile,
                                file_name,
                            ) {
                                break; // Buffer is full
                            }
                        }
                    }
                }

                reply.ok();
            }
            Err(e) => {
                tracing::error!("readdir: S3 list_objects_v2 failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    //TODO: support flags and lock_owners
    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.clone(),
            None => {
                tracing::info!("no inode found. returning");
                reply.error(libc::ENOENT);
                return;
            }
        };

        let s3_key = path_to_s3_key(&path);

        // using u64s to protect against overflows
        let start = offset as u64;
        let end = start + (size as u64) - 1;
        let range_str = format!("bytes={}-{}", start, end);

        tracing::debug!("Reading from S3 key '{}', range: '{}'", s3_key, range_str);

        let fut_result = self.rt.block_on(async {
            self.s3
                .get_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .range(range_str) // Setting our range for seeked reads.
                .send()
                .await
        });

        match fut_result {
            Ok(output) => match self.rt.block_on(output.body.collect()) {
                Ok(agg_bytes) => {
                    let raw_bytes = agg_bytes.into_bytes();
                    tracing::debug!("Successfully read {} bytes from S3.", raw_bytes.len());
                    reply.data(&raw_bytes);
                }
                Err(e) => {
                    tracing::error!("Failed to stream data from S3 for key '{}': {}", s3_key, e);
                    reply.error(libc::EIO);
                }
            },
            Err(e) => {
                tracing::error!("Failed to stream data from S3 for key '{}': {}", s3_key, e);
                reply.error(libc::EIO);
            }
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_path = match self.ino_to_path_cache.get(&parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let new_file_path = parent_path.join(name);
        let s3_key = path_to_s3_key(&new_file_path);

        tracing::info!("create: Creating file '{}' with mode {:o}", s3_key, mode);

        let fh = self.next_fh;
        self.next_fh += 1;
        let mut open_files_guard = self.open_files.lock().unwrap();

        open_files_guard.insert(fh, WriteState::Buffered(Vec::new()));

        let result = self.rt.block_on(async {
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                // Store permissions and ownership as S3 metadata tags.
                .metadata("mode", mode.to_string())
                .metadata("uid", _req.uid().to_string())
                .metadata("gid", _req.gid().to_string())
                .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new())) // Empty body
                .send()
                .await
        });

        match result {
            Ok(_) => {
                let now = SystemTime::now();
                let ino = path_to_ino(&new_file_path);

                self.ino_to_path_cache.insert(ino, new_file_path);

                let attr = FileAttr {
                    ino,
                    size: 0,
                    blocks: 0,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
                    kind: FileType::RegularFile,
                    perm: mode as u16,
                    nlink: 1,
                    uid: _req.uid(),
                    gid: _req.gid(),
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };

                tracing::debug!(
                    "create: Successfully created file. Replying with attr: {:?}",
                    &attr
                );

                reply.created(&TTL, &attr, 0, fh, 0);
            }
            Err(e) => {
                tracing::error!("create: S3 PutObject failed for key '{}': {}", s3_key, e);
                // If the create fails, we must remove the buffer we allocated.
                open_files_guard.remove(&fh);
                reply.error(libc::EIO);
            }
        }
    }

    // For the sake of simplicity, we are ignoring the write flags.
    // We are caching all writes by default in memory.
    // For the sake of simplicity, we are ignoring the write flags.
    // We are caching all writes by default in memory.
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if (offset as u64 + data.len() as u64) > S3_MAX_SIZE {
            reply.error(libc::E2BIG);
            return;
        }

        let mut open_files_guard = self.open_files.lock().unwrap();

        if !open_files_guard.contains_key(&fh) {
            reply.error(libc::EBADF);
            return;
        }

        // These variables will be populated inside the scoped borrow,
        // and then used *after* the borrow is released.
        let mut transition_to_mpu = false;
        let mut part_upload_data: Vec<Vec<u8>> = Vec::new();

        // Scope the mutable borrow tightly.
        {
            let state = open_files_guard.get_mut(&fh).unwrap(); // .unwrap() is safe due to check above.

            match state {
                WriteState::Buffered(buffer) => {
                    let offset = offset as usize;
                    let write_end = offset + data.len();
                    if write_end > buffer.len() {
                        buffer.resize(write_end, 0);
                    }
                    buffer[offset..write_end].copy_from_slice(data);

                    if buffer.len() >= MULTIPART_THRESHOLD {
                        transition_to_mpu = true;
                    }
                }
                WriteState::Multipart {
                    buffer,
                    next_part_number,
                    ..
                } => {
                    let current_uploaded_size = (*next_part_number - 1) as usize * MIN_PART_SIZE;

                    // Check if the write is happening where we expect it, i.e. its a sequential write at the end of last full part.
                    if offset as usize >= current_uploaded_size {
                        let buffer_offset = offset as usize - current_uploaded_size;
                        let write_end = buffer_offset + data.len();
                        if write_end > buffer.len() {
                            buffer.resize(write_end, 0);
                        }
                        buffer[buffer_offset..write_end].copy_from_slice(data);
                    } else {
                        // This is a seek to an earlier part of the file. We cannot support this in MPU
                        // without non-trivial complexity.
                        tracing::error!(
                            "Seek detected in multipart file (writing at offset {}). This is not supported.",
                            offset
                        );
                        reply.error(libc::ENOSYS);
                        return;
                    }

                    while buffer.len() >= MIN_PART_SIZE {
                        let chunk = buffer.drain(0..MIN_PART_SIZE).collect::<Vec<u8>>();
                        part_upload_data.push(chunk);
                    }
                }
            }
        } // TODO: since we now use a Mutex, this code might be easy to simplify.

        let s3_key = path_to_s3_key(&path);

        // A. Handle the transition from Buffered to Multipart
        if transition_to_mpu {
            // We take the now-large buffer out of the `Buffered` state.
            if let Some(WriteState::Buffered(buffered_data)) = open_files_guard.remove(&fh) {
                tracing::info!("write: Transitioning file '{}' to multipart.", s3_key);

                let mpu_resp = self.rt.block_on(
                    self.s3
                        .create_multipart_upload()
                        .bucket(&self.bucket)
                        .key(&s3_key)
                        .send(),
                );

                let upload_id = match mpu_resp {
                    Ok(resp) => resp.upload_id.unwrap_or_default(),
                    Err(e) => {
                        tracing::error!("Failed to create multipart upload: {}", e);
                        reply.error(libc::EIO);
                        return; // The file handle is already removed, so we can just exit.
                    }
                };

                let mut completed_parts = Vec::new();
                let mut next_part_number = 1;
                let mut remaining_buffer = Vec::new();

                // Upload the data we just took from the buffer as initial parts.
                for chunk in buffered_data.chunks(MIN_PART_SIZE) {
                    if chunk.len() < MIN_PART_SIZE {
                        remaining_buffer.extend_from_slice(chunk);
                        break;
                    }
                    match self.rt.block_on(self.upload_part(
                        &upload_id,
                        &s3_key,
                        next_part_number,
                        chunk.to_vec(),
                    )) {
                        Ok(part) => completed_parts.push(part),
                        Err(e) => {
                            tracing::error!("Failed to upload initial part: {}", e);
                            reply.error(libc::EIO);
                            return;
                        }
                    }
                    next_part_number += 1;
                }

                // Insert the new `Multipart` state back into the map for the file handle.
                open_files_guard.insert(
                    fh,
                    WriteState::Multipart {
                        upload_id,
                        completed_parts,
                        buffer: remaining_buffer,
                        next_part_number,
                    },
                );
            }
        }

        // Handle uploading new parts for an existing Multipart upload
        if !part_upload_data.is_empty() {
            // First, get the necessary info with a short-lived mutable borrow.
            let (upload_id_clone, mut current_part_number) = {
                // This scope limits the mutable borrow.
                let state = open_files_guard.get_mut(&fh).unwrap();
                if let WriteState::Multipart {
                    upload_id,
                    next_part_number,
                    ..
                } = state
                {
                    // Clone the ID and copy the part number. Now we have the data we need
                    // without holding on to the borrow.
                    (upload_id.clone(), *next_part_number)
                } else {
                    // This case should be impossible if the logic is correct, but we handle it.
                    reply.error(libc::EIO);
                    return;
                }
            };

            // Now, perform the uploads. `self` is not mutably borrowed anymore.
            let mut new_completed_parts = Vec::new();
            for chunk in part_upload_data {
                match self.rt.block_on(self.upload_part(
                    &upload_id_clone,
                    &s3_key,
                    current_part_number,
                    chunk,
                )) {
                    Ok(part) => new_completed_parts.push(part),
                    Err(e) => {
                        tracing::error!("Failed to upload part {}: {}", current_part_number, e);
                        reply.error(libc::EIO);
                        return;
                    }
                }
                current_part_number += 1;
            }

            // Finally, re-borrow mutably to update the state with the results.
            if let Some(WriteState::Multipart {
                completed_parts,
                next_part_number,
                ..
            }) = open_files_guard.get_mut(&fh)
            {
                completed_parts.extend(new_completed_parts);
                *next_part_number = current_part_number;
            }
        }

        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let mut open_files_guard = self.open_files.lock().unwrap();
        let state = match open_files_guard.remove(&fh) {
            Some(state) => state,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let s3_key: String = path_to_s3_key(&path);

        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> =
            self.rt.block_on(async {
                match state {
                    WriteState::Buffered(buffer) => {
                        tracing::info!(
                            "release: Uploading {} bytes to S3 key '{}' via PutObject",
                            buffer.len(),
                            s3_key
                        );

                        self.s3
                            .put_object()
                            .bucket(&self.bucket)
                            .key(&s3_key)
                            .body(ByteStream::from(buffer))
                            .send()
                            .await?;

                        Ok(())
                    }
                    WriteState::Multipart {
                        buffer,
                        mut completed_parts,
                        upload_id,
                        next_part_number,
                    } => {
                        let s3_key = path_to_s3_key(&path);

                        // 1. Upload any remaining data in the buffer as the last part.
                        if !buffer.is_empty() {
                            tracing::debug!(
                                "release: Uploading final part of size {}",
                                buffer.len()
                            );
                            // We can't use `?` here because we need to run abort on failure.
                            match self
                                .upload_part(&upload_id, &s3_key, next_part_number, buffer)
                                .await
                            {
                                Ok(part) => completed_parts.push(part),
                                Err(e) => {
                                    // If the last part fails, we must abort the whole upload.
                                    self.s3
                                        .abort_multipart_upload()
                                        .bucket(&self.bucket)
                                        .key(&s3_key)
                                        .upload_id(&upload_id)
                                        .send()
                                        .await?;
                                    return Err(e.into());
                                }
                            }
                        }

                        // Complete the multipart upload.
                        tracing::info!("release: Completing multipart upload for key '{}'", s3_key);
                        let mpu_parts = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                            .set_parts(Some(completed_parts))
                            .build();

                        self.s3
                            .complete_multipart_upload()
                            .bucket(&self.bucket)
                            .key(&s3_key)
                            .upload_id(&upload_id)
                            .multipart_upload(mpu_parts)
                            .send()
                            .await?;

                        Ok(())
                    }
                }
            });

        match result {
            Ok(_) => {
                tracing::info!("Successfully released and uploaded file.");
                // TODO: Here you would update the parent directory's .dir_version
                reply.ok();
            }
            Err(e) => {
                tracing::error!("S3 operation failed on release: {}", e);
                reply.error(libc::EIO);
            }
        }
    }
}

fn path_to_s3_key(path: &PathBuf) -> String {
    let path_str = path.to_string_lossy();
    path_str.trim_start_matches('/').to_string()
}
