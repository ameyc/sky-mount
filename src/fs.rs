use anyhow::Result;
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

use crate::{
    object_store::{ObjectKind, ObjectStore, ObjectUserMetadata},
    utils,
};

const TTL: Duration = Duration::from_secs(1);
const S3_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024; // 5 TiB

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
    object_store: ObjectStore,
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

        let object_store = rt.block_on(ObjectStore::new(bucket))?;

        Ok(Self {
            object_store,
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
}

impl Filesystem for S3Fuse {
    fn init(
        &mut self,
        req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.mount_uid = req.uid();
        self.mount_gid = req.gid();
        Ok(())
    }

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
            // Special case for looking up the root directory itself
            reply.entry(&TTL, &ROOT_ATTR, 0);
            return;
        }

        let ino = path_to_ino(&child_path);

        let result = self.rt.block_on(self.object_store.stat_object(&key));

        match result {
            Ok(ObjectKind::File(meta)) => {
                tracing::info!("lookup: '{}' is a file", key);
                let attr =
                    utils::s3_meta_to_file_attr(ino, &key, &meta, self.mount_uid, self.mount_gid);
                self.ino_to_path_cache.insert(
                    ino,
                    InodeCacheEntry {
                        path: child_path,
                        kind: FileType::RegularFile,
                    },
                );
                reply.entry(&TTL, &attr, 0);
            }
            Ok(ObjectKind::Directory(Some(meta))) => {
                tracing::info!("lookup: '{}' is an explicit directory", key);
                let dir_key = format!("{}/", key);
                let attr = utils::s3_meta_to_file_attr(
                    ino,
                    &dir_key,
                    &meta,
                    self.mount_uid,
                    self.mount_gid,
                );
                self.ino_to_path_cache.insert(
                    ino,
                    InodeCacheEntry {
                        path: child_path,
                        kind: FileType::Directory,
                    },
                );
                reply.entry(&TTL, &attr, 0);
            }
            Ok(ObjectKind::Directory(None)) => {
                tracing::info!("lookup: '{}' is an implicit directory", key);
                // No metadata, so we create default attributes.
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
            }
            Err(_) => {
                tracing::info!("lookup: '{}' not found", key);
                reply.error(libc::ENOENT);
            }
        }
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

        let cache_entry = match self.ino_to_path_cache.get(&ino).cloned() {
            Some(p) => p,
            None => {
                tracing::warn!("getattr: ino {} not found in cache.", ino);
                reply.error(libc::ENOENT);
                return;
            }
        };

        let key = path_to_s3_key(&cache_entry.path);
        let s3_key = if cache_entry.kind == FileType::Directory {
            format!("{}/", key)
        } else {
            key
        };

        tracing::debug!(
            "getattr: calling storage.get_attributes for key '{}'",
            s3_key
        );

        match self.rt.block_on(self.object_store.get_attributes(&s3_key)) {
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
                if self
                    .rt
                    .block_on(self.object_store.truncate_file(&key))
                    .is_err()
                {
                    reply.error(libc::EIO);
                    return;
                }
            } else {
                // Truncating to non-zero size is not supported in this simple model
                reply.error(libc::EOPNOTSUPP);
                return;
            }
        }

        let needs_meta_update = mode.is_some() || uid.is_some() || gid.is_some();
        if needs_meta_update {
            // To update metadata, we must provide the *full* new set.
            // So first, we get the current attributes.
            let current_meta = match self.rt.block_on(self.object_store.get_attributes(&key)) {
                Ok(h) => h,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };
            let mut current_attr =
                utils::s3_meta_to_file_attr(ino, &key, &current_meta, _req.uid(), _req.gid());

            // Modify the attributes with the new values from the request
            if let Some(new_mode) = mode {
                current_attr.perm = new_mode as u16;
            }
            if let Some(new_uid) = uid {
                current_attr.uid = new_uid;
            }
            if let Some(new_gid) = gid {
                current_attr.gid = new_gid;
            }

            // Create the metadata payload for our storage method
            let new_metadata = ObjectUserMetadata {
                mode: Some(current_attr.perm),
                uid: Some(current_attr.uid),
                gid: Some(current_attr.gid),
            };

            if self
                .rt
                .block_on(self.object_store.replace_metadata(&key, &new_metadata))
                .is_err()
            {
                reply.error(libc::EIO);
                return;
            }
        }

        // After any operation, get the final state and reply.
        match self.rt.block_on(self.object_store.get_attributes(&key)) {
            Ok(meta) => {
                let attr =
                    utils::s3_meta_to_file_attr(ino, &key, &meta, self.mount_uid, self.mount_gid);
                reply.attr(&TTL, &attr);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    // src/fs.rs

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        tracing::info!("readdir(ino={}, offset={})", ino, offset);

        // 1. Get the path for the directory we are reading from our cache.
        let dir_path = match self.ino_to_path_cache.get(&ino) {
            Some(p) => p.path.clone(),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // 2. Fetch the actual directory listing from our clean ObjectStore.
        let prefix = path_to_s3_key(&dir_path);
        let dir_prefix = if !prefix.is_empty() && !prefix.ends_with('/') {
            format!("{}/", prefix)
        } else {
            prefix
        };

        let child_entries = match self
            .rt
            .block_on(self.object_store.list_directory(&dir_prefix))
        {
            Ok(entries) => entries,
            Err(e) => {
                tracing::error!("readdir: list_directory failed: {}", e);
                reply.error(libc::EIO);
                return;
            }
        };

        // 3. Build a complete list of all entries, including "." and "..".
        // The FUSE offset is 1-based.
        // Offset 1: .
        // Offset 2: ..
        // Offset 3+: child entries
        let mut all_entries: Vec<(u64, FileType, String)> = Vec::new();

        // Entry for "."
        all_entries.push((ino, FileType::Directory, ".".to_string()));

        // Entry for ".."
        let parent_ino = dir_path.parent().map_or(1, |p| path_to_ino(p));
        all_entries.push((parent_ino, FileType::Directory, "..".to_string()));

        // Add child entries
        for entry in child_entries {
            let child_path = dir_path.join(&entry.name);
            let child_ino = path_to_ino(&child_path);
            all_entries.push((child_ino, entry.kind, entry.name));
        }

        // 4. Iterate over the complete list, skipping entries based on the offset,
        //    and add them to the reply buffer.
        for (i, (entry_ino, entry_kind, entry_name)) in
            all_entries.iter().enumerate().skip(offset as usize)
        {
            // The `offset` for the next readdir call is the index of the *next* entry.
            let next_offset = (i + 1) as i64;

            // Add to buffer. If it's full, `add` returns true and we must stop.
            if reply.add(*entry_ino, next_offset, *entry_kind, entry_name) {
                break;
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

        let fut_result = self
            .rt
            .block_on(async { self.object_store.download_range(&s3_key, start, size).await });

        match fut_result {
            Ok(output) => match self.rt.block_on(output.collect()) {
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

        let metadata = ObjectUserMetadata {
            mode: Some(mode as u16),
            uid: Some(req.uid()),
            gid: Some(req.gid()),
        };

        let result = self
            .rt
            .block_on(self.object_store.create_object(&s3_key, &metadata));

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

        // NOTE: the decision to do a simple put_object call versus MPU is entirely delegated to the Object Store implementation
        let result = self.rt.block_on(self.object_store.upload(&s3_key, buffer));

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

        // Fetch attributes for checking permissions
        let head = match self.rt.block_on(self.object_store.get_attributes(&key)) {
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
            let parent_head = match self
                .rt
                .block_on(self.object_store.get_attributes(&parent_key))
            {
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

        let parent_path = self.ino_to_path_cache.get(&parent).unwrap().path.clone(); // Safe now
        let new_dir_path = parent_path.join(name);
        let new_dir_key = format!("{}/", path_to_s3_key(&new_dir_path));

        let metadata = ObjectUserMetadata {
            mode: Some(mode as u16),
            uid: Some(req.uid()),
            gid: Some(req.gid()),
        };

        let result = self
            .rt
            .block_on(self.object_store.create_object(&new_dir_key, &metadata));

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

        //  Check permissions
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
            let parent_head = match self
                .rt
                .block_on(self.object_store.get_attributes(&parent_key))
            {
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
            // We dont have write permission on parent
            reply.error(libc::EACCES);
            return;
        }

        let parent_path = self.ino_to_path_cache.get(&parent).unwrap().path.clone();
        let file_path = parent_path.join(name);
        let file_key = path_to_s3_key(&file_path);

        let result = self.rt.block_on(self.object_store.delete_object(&file_key));
        match result {
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
            let parent_head = match self
                .rt
                .block_on(self.object_store.get_attributes(&parent_key))
            {
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

        let list_res = self.rt.block_on(self.object_store.list_directory(&dir_key));

        match list_res {
            Ok(entries) => {
                if !entries.is_empty() {
                    reply.error(libc::ENOTEMPTY);
                    return;
                }
            }
            Err(e) => {
                tracing::error!("rmdir list check failed: {}", e);
                reply.error(libc::EIO);
                return;
            }
        }

        let delete_res = self.rt.block_on(self.object_store.delete_object(&dir_key));
        match delete_res {
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
            let parent_head = match self
                .rt
                .block_on(self.object_store.get_attributes(&parent_key))
            {
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
                let parent_head = match self
                    .rt
                    .block_on(self.object_store.get_attributes(&parent_key))
                {
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

        let result = if old_parent_cache_entry.kind == FileType::Directory {
            let old_dir_key = format!("{}/", path_to_s3_key(&old_path));
            let new_dir_key = format!("{}/", path_to_s3_key(&new_path));
            tracing::debug!("Renaming directory from {} to {}", old_dir_key, new_dir_key);
            self.rt
                .block_on(self.object_store.rename_dir(&old_dir_key, &new_dir_key))
        } else {
            let old_key = path_to_s3_key(&old_path);
            let new_key = path_to_s3_key(&new_path);
            tracing::debug!("Renaming file from {} to {}", old_key, new_key);
            self.rt
                .block_on(self.object_store.rename_file(&old_key, &new_key))
        };

        match result {
            Ok(_) => {
                self.ino_to_path_cache.remove(&old_ino);
                // Note: This is a potential bug since new entry should be like the old entry
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
