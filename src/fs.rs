use anyhow::Result;
use aws_sdk_s3::{
    Client,
    error::SdkError,
    operation::{head_object::HeadObjectError, put_object::PutObjectOutput},
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, MetadataDirective, ObjectIdentifier},
};
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, TimeOrNow,
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

use crate::utils;

const TTL: Duration = Duration::from_secs(1);
const S3_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024; // 5 TiB

// Constants for Multipart Upload
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB: The minimum part size for S3 MPU
const MULTIPART_THRESHOLD: usize = 10 * 1024 * 1024; // 10MB: Files larger than this will use MPU

fn path_to_ino(path: &Path) -> u64 {
    // Inode 1 is special (root). We dont ever generate it.
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

#[derive(Clone)]
struct InodeCacheEntry {
    path: PathBuf,
    kind: FileType,
}

pub struct S3Fuse {
    bucket: String,
    s3: Client,
    rt: Runtime,
    ino_to_path_cache: HashMap<u64, InodeCacheEntry>,
    next_fh: u64,
    write_buffers: Mutex<HashMap<u64, Vec<u8>>>,
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
                m.insert(
                    1,
                    InodeCacheEntry {
                        path: PathBuf::from("/"),
                        kind: FileType::Directory,
                    },
                );
                m
            },
            next_fh: 1,
            write_buffers: Mutex::new(HashMap::new()),
            mount_uid: 0,
            mount_gid: 0,
        })
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

    async fn do_rename_file(&self, old_key: &str, new_key: &str) -> Result<()> {
        // 1. Copy the object to the new key. S3 metadata is preserved by default.
        self.s3
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(format!("{}/{}", self.bucket, old_key))
            .key(new_key)
            .send()
            .await?;

        // 2. Delete the old object.
        self.s3
            .delete_object()
            .bucket(&self.bucket)
            .key(old_key)
            .send()
            .await?;

        Ok(())
    }

    /// NOTE: This operation is NOT ATOMIC.
    async fn do_rename_dir(&self, old_prefix: &str, new_prefix: &str) -> Result<()> {
        let mut objects_to_delete = Vec::new();

        // 1. List all objects under the old prefix.
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
                    // a. Copy each object to its new location
                    let new_key = key.replacen(old_prefix, new_prefix, 1);
                    self.s3
                        .copy_object()
                        .bucket(&self.bucket)
                        .copy_source(format!("{}/{}", self.bucket, key))
                        .key(new_key)
                        .send()
                        .await?;

                    // b. Add the old key to a list for batch deletion
                    objects_to_delete.push(ObjectIdentifier::builder().key(key).build()?);
                }
            }
        }

        // 2. Batch delete all the old objects.
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
        let parent_path = match self.ino_to_path_cache.get(&parent_ino) {
            Some(p) => &p.path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let child_path = parent_path.join(name);
        let key = path_to_s3_key(&child_path);
        if key.is_empty() {
            let attr = FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            };
            reply.entry(&TTL, &attr, 0);
            return;
        }

        let dir_key = format!("{}/", key);
        let ino = path_to_ino(&child_path);

        // 1. Check for explicit directory marker.
        tracing::info!("checking for child_path={:?}", child_path);
        if let Ok(dir_meta) = self.rt.block_on(
            self.s3
                .head_object()
                .bucket(&self.bucket)
                .key(&dir_key)
                .send(),
        ) {
            let attr = utils::s3_meta_to_file_attr(
                ino,
                &dir_key,
                &dir_meta,
                self.mount_uid,
                self.mount_gid,
            );
            tracing::info!("child_path={:?} is a directory", child_path);
            self.ino_to_path_cache.insert(
                ino,
                InodeCacheEntry {
                    path: child_path,
                    kind: FileType::Directory,
                },
            );
            reply.entry(&TTL, &attr, 0);
            return;
        }

        // 2. Check for a file.
        if let Ok(file_meta) = self
            .rt
            .block_on(self.s3.head_object().bucket(&self.bucket).key(&key).send())
        {
            let attr =
                utils::s3_meta_to_file_attr(ino, &key, &file_meta, self.mount_uid, self.mount_gid);
            tracing::info!("child_path={:?} is a file", child_path);

            self.ino_to_path_cache.insert(
                ino,
                InodeCacheEntry {
                    path: child_path,
                    kind: FileType::RegularFile,
                },
            );
            reply.entry(&TTL, &attr, 0);
            return;
        }

        // 3. Check for an implicit directory.
        if let Ok(list_output) = self.rt.block_on(
            self.s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&dir_key)
                .max_keys(1)
                .send(),
        ) {
            tracing::info!(
                "child_path={:?} appears to be an implicit directory",
                child_path
            );
            if list_output.key_count.unwrap_or(0) > 0 {
                let attr = FileAttr {
                    ino,
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: self.mount_uid,
                    gid: self.mount_gid,
                    ..ROOT_ATTR
                };
                self.ino_to_path_cache.insert(
                    ino,
                    InodeCacheEntry {
                        path: child_path,
                        kind: FileType::Directory,
                    },
                );
                reply.entry(&TTL, &attr, 0);
                return;
            }
        }

        reply.error(libc::ENOENT);
    }

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

        // Get the path from our essential cache.
        let cache_entry = match self.ino_to_path_cache.get(&ino).cloned() {
            Some(p) => p.clone(),
            None => {
                // This is a legitimate error. If the ino isn't in our cache, we can't know the path.
                tracing::warn!("getattr: ino {} not found in cache.", ino);
                reply.error(libc::ENOENT);
                return;
            }
        };

        // 2. Build the S3 key based on the cached 'kind'. NO GUESSWORK NEEDED.
        let key = path_to_s3_key(&cache_entry.path);
        let s3_key = if cache_entry.kind == FileType::Directory {
            format!("{}/", key)
        } else {
            key
        };

        tracing::debug!(
            "getattr: looking for ino {} with known kind {:?} at key '{}'",
            ino,
            cache_entry.kind,
            s3_key
        );

        // 3. Make ONE S3 API call to get fresh metadata.
        let head_res = self.rt.block_on(
            self.s3
                .head_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .send(),
        );

        match head_res {
            Ok(meta) => {
                let attr = utils::s3_meta_to_file_attr(
                    ino,
                    &s3_key,
                    &meta,
                    self.mount_uid,
                    self.mount_gid,
                );
                reply.attr(&TTL, &attr);
            }
            Err(_) => {
                // The object might have been deleted since it was cached.
                reply.error(libc::ENOENT);
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
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
            Some(p) => p.path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let key = path_to_s3_key(&path);

        if let Some(new_size) = size {
            if new_size == 0 {
                // Truncate to 0 is a PutObject with an empty body
                let res = self
                    .rt
                    .block_on(self.s3.put_object().bucket(&self.bucket).key(&key).send());
                if res.is_err() {
                    reply.error(libc::EIO);
                    return;
                }
            } else {
                reply.error(libc::EOPNOTSUPP); // Truncating to non-zero is not supported
                return;
            }
        }

        // To change metadata, we must do a copy-in-place.
        // First, get current metadata.
        let head = match self
            .rt
            .block_on(self.s3.head_object().bucket(&self.bucket).key(&key).send())
        {
            Ok(h) => h,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        let mut current_attr =
            utils::s3_meta_to_file_attr(ino, &key, &head, _req.uid(), _req.gid());
        let mut needs_copy = false;

        if let Some(new_mode) = mode {
            current_attr.perm = new_mode as u16;
            needs_copy = true;
        }
        if let Some(new_uid) = uid {
            current_attr.uid = new_uid;
            needs_copy = true;
        }
        if let Some(new_gid) = gid {
            current_attr.gid = new_gid;
            needs_copy = true;
        }

        if needs_copy {
            let res = self.rt.block_on(
                self.s3
                    .copy_object()
                    .bucket(&self.bucket)
                    .copy_source(format!("{}/{}", self.bucket, key))
                    .key(&key)
                    .metadata_directive(MetadataDirective::Replace)
                    .metadata("uid", current_attr.uid.to_string())
                    .metadata("gid", current_attr.gid.to_string())
                    .metadata("mode", current_attr.perm.to_string())
                    .send(),
            );

            if res.is_err() {
                reply.error(libc::EIO);
                return;
            }
        }

        // After any operation, get the final state and reply.
        match self
            .rt
            .block_on(self.s3.head_object().bucket(&self.bucket).key(&key).send())
        {
            Ok(meta) => {
                let attr =
                    utils::s3_meta_to_file_attr(ino, &key, &meta, self.mount_uid, self.mount_gid);
                reply.attr(&TTL, &attr);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let dir_path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if offset == 0 {
            if reply.add(ino, 1, FileType::Directory, ".") {
                reply.ok();
                return;
            }
        }
        if offset <= 1 {
            let parent_ino = dir_path.parent().map_or(1, |p| path_to_ino(p));
            if reply.add(parent_ino, 2, FileType::Directory, "..") {
                reply.ok();
                return;
            }
        }

        let mut current_offset = 2i64;
        let prefix = path_to_s3_key(&dir_path);
        let prefix = if !prefix.is_empty() && !prefix.ends_with('/') {
            format!("{}/", prefix)
        } else {
            prefix
        };

        // Use a paginator to handle directories with many items.
        let mut paginator = self
            .s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .delimiter("/")
            .into_paginator()
            .send();

        let mut entries_to_add: Vec<(u64, FileType, String)> = Vec::new();

        // Collect all possible entries first.
        while let Some(result) = self.rt.block_on(paginator.next()) {
            match result {
                Ok(output) => {
                    if let Some(prefixes) = output.common_prefixes {
                        for p in prefixes {
                            if let Some(dir_key) = p.prefix {
                                let name = dir_key
                                    .trim_end_matches('/')
                                    .split('/')
                                    .last()
                                    .unwrap_or("")
                                    .to_string();
                                let ino = path_to_ino(&dir_path.join(&name));
                                entries_to_add.push((ino, FileType::Directory, name));
                            }
                        }
                    }
                    if let Some(objects) = output.contents {
                        for obj in objects {
                            if let Some(key) = obj.key {
                                if key.ends_with('/') {
                                    continue;
                                } // Skip directory markers
                                let name = key.split('/').last().unwrap_or("").to_string();
                                let ino = path_to_ino(&dir_path.join(&name));
                                entries_to_add.push((ino, FileType::RegularFile, name));
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("readdir: S3 list_objects_v2 failed: {}", e);
                    reply.error(libc::EIO);
                    return;
                }
            }
        }

        // Now, iterate through the collected entries and add them to the reply buffer,
        // respecting the offset.
        for (entry_ino, entry_type, entry_name) in entries_to_add {
            current_offset += 1;
            if current_offset - 1 <= offset {
                continue;
            }
            if reply.add(entry_ino, current_offset - 1, entry_type, entry_name) {
                break; // Buffer is full
            }
        }

        reply.ok();
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
            Some(p) => p.path.clone(),
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
        req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let parent_path = match self.ino_to_path_cache.get(&parent) {
            Some(p) => p.path.clone(),
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
        self.write_buffers.lock().unwrap().insert(fh, Vec::new());

        let result: Result<PutObjectOutput, _> = self.rt.block_on(async {
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(&s3_key)
                .metadata("mode", (mode as u16).to_string()) // Store the requested mode
                .metadata("uid", req.uid().to_string())
                .metadata("gid", req.gid().to_string())
                .body(ByteStream::from(Vec::new()))
                .send()
                .await
        });

        match result {
            Ok(_) => {
                let now = SystemTime::now();
                let ino = path_to_ino(&new_file_path);
                self.ino_to_path_cache.insert(
                    ino,
                    InodeCacheEntry {
                        path: new_file_path,
                        kind: FileType::RegularFile,
                    },
                );

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
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };
                reply.created(&TTL, &attr, 0, fh, 0);
            }
            Err(e) => {
                tracing::error!("create: S3 PutObject failed for key '{}': {}", s3_key, e);
                self.write_buffers.lock().unwrap().remove(&fh);
                reply.error(libc::EIO);
            }
        }
    }

    // For the sake of simplicity, we are ignoring the write flags.
    // We are caching all writes by default in memory.
    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut write_buffers_guard = self.write_buffers.lock().unwrap();

        let buffer = match write_buffers_guard.get_mut(&fh) {
            Some(buffer) => buffer,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        let offset = offset as usize;
        let write_end = offset + data.len();

        if write_end as u64 > S3_MAX_SIZE {
            reply.error(libc::E2BIG);
            return;
        }

        if write_end > buffer.len() {
            buffer.resize(write_end, 0);
        }

        buffer[offset..write_end].copy_from_slice(data);

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
        let mut write_buffers_guard = self.write_buffers.lock().unwrap();
        // Remove the buffer from the map. If it's not there, it was a read-only handle.
        let buffer = match write_buffers_guard.remove(&fh) {
            Some(buffer) => buffer,
            None => {
                reply.ok();
                return;
            }
        };

        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let s3_key: String = path_to_s3_key(&path);

        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> =
            self.rt.block_on(async {
                if buffer.len() < MULTIPART_THRESHOLD {
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
                } else {
                    tracing::info!(
                        "release: Uploading {} bytes to S3 key '{}' via Multipart Upload",
                        buffer.len(),
                        s3_key
                    );

                    let mpu = self
                        .s3
                        .create_multipart_upload()
                        .bucket(&self.bucket)
                        .key(&s3_key)
                        .send()
                        .await?;

                    let upload_id = mpu.upload_id.ok_or("S3 did not return an upload ID")?;

                    let mut completed_parts = Vec::new();
                    let mut part_number = 1;

                    for chunk in buffer.chunks(MIN_PART_SIZE) {
                        let part_data = chunk.to_vec();
                        match self
                            .upload_part(&upload_id, &s3_key, part_number, part_data)
                            .await
                        {
                            Ok(part) => completed_parts.push(part),
                            Err(e) => {
                                tracing::error!(
                                    "Failed to upload part {}: {}. Aborting MPU.",
                                    part_number,
                                    e
                                );
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
                        part_number += 1;
                    }

                    let mpu_parts = CompletedMultipartUpload::builder()
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
                }
                Ok(())
            });

        match result {
            Ok(_) => {
                tracing::info!("Successfully released and uploaded file '{}'.", s3_key);
                reply.ok();
            }
            Err(e) => {
                tracing::error!("S3 operation failed on release for key '{}': {}", s3_key, e);
                reply.error(libc::EIO);
            }
        }
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        tracing::debug!("open(ino={}, flags={:#o})", ino, flags);
        let path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let key = path_to_s3_key(&path);

        // Fetch attributes to check permissions
        let head = match self
            .rt
            .block_on(self.s3.head_object().bucket(&self.bucket).key(&key).send())
        {
            Ok(h) => h,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        let attr = utils::s3_meta_to_file_attr(ino, &key, &head, self.mount_uid, self.mount_gid);

        // Check permissions based on open flags
        let access_mask = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => 4, // Read
            libc::O_WRONLY => 2, // Write
            libc::O_RDWR => 6,   // Read-Write
            _ => 0,
        };

        if access_mask > 0 && !utils::check_permission(&attr, req, access_mask) {
            reply.error(libc::EACCES);
            return;
        }

        let fh = self.next_fh;
        self.next_fh += 1;

        if (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            self.write_buffers.lock().unwrap().insert(fh, Vec::new());
        }

        reply.opened(fh, fuser::consts::FOPEN_KEEP_CACHE);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        tracing::debug!("flush(ino={}, fh={})", ino, fh);
        // In our design, all data is buffered until the final `release` call.
        // `flush` (called on `close` and `fsync`) is therefore a no-op.
        let open_files = self.write_buffers.lock().unwrap();
        if open_files.contains_key(&fh) {
            tracing::debug!("flush: fh {} has pending writes, deferring to release", fh);
        } else {
            tracing::debug!("flush: fh {} has no pending writes.", fh);
        }
        reply.ok();
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        tracing::debug!("mkdir(parent={}, name={:?}, mode={:o})", parent, name, mode);

        // --- Permission Check (Corrected Pattern) ---
        let parent_attr = if parent == 1 {
            FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            }
        } else {
            let parent_path = match self.ino_to_path_cache.get(&parent) {
                Some(p) => p.path.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let parent_key = format!("{}/", path_to_s3_key(&parent_path));
            let parent_head = match self.rt.block_on(
                self.s3
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&parent_key)
                    .send(),
            ) {
                Ok(h) => h,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            utils::s3_meta_to_file_attr(
                parent,
                &parent_key,
                &parent_head,
                self.mount_uid,
                self.mount_gid,
            )
        };

        if !utils::check_permission(&parent_attr, req, 2) {
            // 2 = Write
            reply.error(libc::EACCES);
            return;
        }
        // --- End Permission Check ---

        let parent_path = self.ino_to_path_cache.get(&parent).unwrap().path.clone(); // Safe now
        let new_dir_path = parent_path.join(name);
        let new_dir_key = format!("{}/", path_to_s3_key(&new_dir_path));

        let result = self.rt.block_on(
            self.s3
                .put_object()
                .bucket(&self.bucket)
                .key(&new_dir_key)
                .metadata("mode", (mode as u16).to_string())
                .metadata("uid", req.uid().to_string())
                .metadata("gid", req.gid().to_string())
                .send(),
        );

        match result {
            Ok(_) => {
                let now = SystemTime::now();
                let ino = path_to_ino(&new_dir_path);
                self.ino_to_path_cache.insert(
                    ino,
                    InodeCacheEntry {
                        path: new_dir_path,
                        kind: FileType::Directory,
                    },
                );

                let attr = FileAttr {
                    ino,
                    size: 0,
                    blocks: 0,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
                    kind: FileType::Directory,
                    perm: mode as u16,
                    nlink: 2,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                };
                reply.entry(&TTL, &attr, 0);
            }
            Err(e) => {
                tracing::error!("mkdir failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        tracing::debug!("unlink(parent={}, name={:?})", parent, name);

        //  Check
        let parent_attr = if parent == 1 {
            FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            }
        } else {
            let parent_path = match self.ino_to_path_cache.get(&parent) {
                Some(p) => p.path.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let parent_key = format!("{}/", path_to_s3_key(&parent_path));
            let parent_head = match self.rt.block_on(
                self.s3
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&parent_key)
                    .send(),
            ) {
                Ok(h) => h,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            utils::s3_meta_to_file_attr(
                parent,
                &parent_key,
                &parent_head,
                self.mount_uid,
                self.mount_gid,
            )
        };
        if !utils::check_permission(&parent_attr, req, 2) {
            // Write permission on parent
            reply.error(libc::EACCES);
            return;
        }

        let parent_path = self.ino_to_path_cache.get(&parent).unwrap().path.clone();
        let file_path = parent_path.join(name);
        let file_key = path_to_s3_key(&file_path);

        match self.rt.block_on(
            self.s3
                .delete_object()
                .bucket(&self.bucket)
                .key(&file_key)
                .send(),
        ) {
            Ok(_) => {
                let ino = path_to_ino(&file_path);
                self.ino_to_path_cache.remove(&ino);
                reply.ok();
            }
            Err(e) => {
                tracing::error!("unlink failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        tracing::debug!("rmdir(parent={}, name={:?})", parent, name);

        let parent_attr = if parent == 1 {
            FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            }
        } else {
            let parent_path = match self.ino_to_path_cache.get(&parent) {
                Some(p) => p.path.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let parent_key = format!("{}/", path_to_s3_key(&parent_path));
            let parent_head = match self.rt.block_on(
                self.s3
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&parent_key)
                    .send(),
            ) {
                Ok(h) => h,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            utils::s3_meta_to_file_attr(
                parent,
                &parent_key,
                &parent_head,
                self.mount_uid,
                self.mount_gid,
            )
        };
        if !utils::check_permission(&parent_attr, req, 2) {
            reply.error(libc::EACCES);
            return;
        }

        let parent_path = self.ino_to_path_cache.get(&parent).unwrap().path.clone();
        let dir_path = parent_path.join(name);
        let dir_key = format!("{}/", path_to_s3_key(&dir_path));

        let list_res = self.rt.block_on(
            self.s3
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&dir_key)
                .max_keys(2)
                .send(),
        );

        match list_res {
            Ok(output) => {
                if output.key_count.unwrap_or(0) > 1 {
                    reply.error(libc::ENOTEMPTY);
                    return;
                }
                if let Some(contents) = output.contents {
                    if !contents.is_empty() && contents[0].key() != Some(dir_key.as_str()) {
                        reply.error(libc::ENOTEMPTY);
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::error!("rmdir list check failed: {}", e);
                reply.error(libc::EIO);
                return;
            }
        }

        match self.rt.block_on(
            self.s3
                .delete_object()
                .bucket(&self.bucket)
                .key(&dir_key)
                .send(),
        ) {
            Ok(_) => {
                let ino = path_to_ino(&dir_path);
                self.ino_to_path_cache.remove(&ino);
                reply.ok();
            }
            Err(e) => {
                tracing::error!("rmdir deletion failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn rename(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let old_parent_attr = if parent == 1 {
            FileAttr {
                uid: self.mount_uid,
                gid: self.mount_gid,
                ..ROOT_ATTR
            }
        } else {
            let parent_path = match self.ino_to_path_cache.get(&parent) {
                Some(p) => p.path.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let parent_key = format!("{}/", path_to_s3_key(&parent_path));
            let parent_head = match self.rt.block_on(
                self.s3
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&parent_key)
                    .send(),
            ) {
                Ok(h) => h,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            utils::s3_meta_to_file_attr(
                parent,
                &parent_key,
                &parent_head,
                self.mount_uid,
                self.mount_gid,
            )
        };
        if !utils::check_permission(&old_parent_attr, req, 2) {
            reply.error(libc::EACCES);
            return;
        }

        if parent != newparent {
            let new_parent_attr = if newparent == 1 {
                FileAttr {
                    uid: self.mount_uid,
                    gid: self.mount_gid,
                    ..ROOT_ATTR
                }
            } else {
                let parent_path = match self.ino_to_path_cache.get(&newparent) {
                    Some(p) => p.path.clone(),
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                let parent_key = format!("{}/", path_to_s3_key(&parent_path));
                let parent_head = match self.rt.block_on(
                    self.s3
                        .head_object()
                        .bucket(&self.bucket)
                        .key(&parent_key)
                        .send(),
                ) {
                    Ok(h) => h,
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                };
                utils::s3_meta_to_file_attr(
                    newparent,
                    &parent_key,
                    &parent_head,
                    self.mount_uid,
                    self.mount_gid,
                )
            };
            if !utils::check_permission(&new_parent_attr, req, 2) {
                reply.error(libc::EACCES);
                return;
            }
        }

        let old_parent_cache_entry = self.ino_to_path_cache.get(&parent).unwrap().clone();
        let new_parent_path_entry = self.ino_to_path_cache.get(&newparent).unwrap().clone();
        let old_path = old_parent_cache_entry.path.join(name);
        let old_ino = path_to_ino(&old_path);
        let new_path = new_parent_path_entry.path.join(newname);
        let new_ino = path_to_ino(&new_path);

        let old_key = path_to_s3_key(&old_path);
        let new_key = path_to_s3_key(&new_path);

        let result = if old_parent_cache_entry.kind == FileType::Directory {
            let old_dir_key = format!("{}/", old_key);
            let new_dir_key = format!("{}/", new_key);
            tracing::debug!("Renaming directory from {} to {}", old_dir_key, new_dir_key);
            self.rt
                .block_on(self.do_rename_dir(&old_dir_key, &new_dir_key))
        } else {
            tracing::debug!("Renaming file from {} to {}", old_key, new_key);
            self.rt.block_on(self.do_rename_file(&old_key, &new_key))
        };

        match result {
            Ok(_) => {
                self.ino_to_path_cache.remove(&old_ino);
                self.ino_to_path_cache.insert(
                    new_ino,
                    InodeCacheEntry {
                        path: new_path,
                        kind: old_parent_cache_entry.kind,
                    },
                );
                reply.ok()
            }
            Err(e) => {
                tracing::error!("rename failed: {}", e);
                reply.error(libc::EIO)
            }
        }
    }
}

fn path_to_s3_key(path: &PathBuf) -> String {
    let path_str = path.to_string_lossy();
    path_str.trim_start_matches('/').to_string()
}
