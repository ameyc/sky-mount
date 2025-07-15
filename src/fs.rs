use anyhow::Result;
use aws_sdk_s3::types::CompletedPart;
use dashmap::{DashMap, DashSet};
use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use std::{
    ffi::OsStr,
    path::PathBuf,
    time::{Duration, SystemTime},
};
use time::OffsetDateTime;
use tokio::runtime::Runtime;

use std::path::Path;

use crate::{
    metadata_service::{Inode, MetadataService},
    object_store::{ObjectStore, ObjectUserMetadata},
    utils,
};

const TTL: Duration = Duration::from_secs(15);
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

// add near the other constants
const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // S3 absolute minimum
const MULTIPART_THRESHOLD: usize = 10 * 1024 * 1024; // when to start MPU

/// Per-inode write state: in-RAM staging buffer plus MPU bookkeeping
struct WriteState {
    buffer: Vec<u8>,
    multipart: Option<MultipartCtx>,
}

struct MultipartCtx {
    upload_id: String,
    next_part: i32,
    completed: Vec<CompletedPart>,
}

pub struct S3Fuse {
    object_store: ObjectStore,
    rt: Runtime,
    metadata_service: MetadataService,
    seeded_dirs: DashSet<u64>,
    next_fh: u64,
    write_states: DashMap<u64, WriteState>, //DashMap provides finer grained key level locking
    mount_uid: u32,
    mount_gid: u32,
}

impl S3Fuse {
    pub fn new(bucket: String, db_url: &str) -> Result<Self> {
        let rt = Runtime::new()?;
        let object_store = rt.block_on(ObjectStore::new(bucket.clone()))?;
        let metadata_service = rt.block_on(MetadataService::new(db_url, &bucket))?;
        Ok(Self {
            object_store,
            rt,
            metadata_service,
            seeded_dirs: DashSet::new(),
            next_fh: 1,
            write_states: DashMap::new(),
            mount_uid: 0,
            mount_gid: 0,
        })
    }

    fn get_s3_key_from_inode(&self, inode: &Inode) -> String {
        let path_buf = self
            .rt
            .block_on(self.metadata_service.get_full_path(inode.ino as u64))
            .expect("inode disappeared");

        // Join segments with `/`; e.g. ["projects","foo.txt"] → "projects/foo.txt"
        let joined = path_buf
            .iter()
            .map(|os| os.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");

        joined
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
        self.rt
            .block_on(
                self.metadata_service
                    .initialize_schema(self.mount_uid, self.mount_gid),
            )
            .map_err(|e| {
                tracing::error!("Failed to initialize schema: {}", e);
                libc::EIO
            })?;
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent_ino: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        match self
            .rt
            .block_on(self.metadata_service.lookup(parent_ino, name_str))
        {
            Ok(Some(inode)) => {
                reply.entry(&TTL, &inode.to_file_attr(), 0);
            }
            Ok(None) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                // 4. A database error occurred.
                tracing::error!(
                    "lookup: Failed to query inode for name '{}' in parent {}: {}",
                    name_str,
                    parent_ino,
                    e
                );
                reply.error(libc::EIO);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(md)) => {
                // base attributes from the DB row
                let mut attr = md.to_file_attr();

                // If this inode is currently open for writing, add the pending tail
                if let Some(state) = self.write_states.get(&ino) {
                    // bytes not yet flushed to S3 (the staging buffer)
                    let tail_len = state.buffer.len() as u64;
                    attr.size += tail_len; // show full logical size
                    attr.blocks = (attr.size + 511) / 512; // 512-byte blocks
                }

                reply.attr(&TTL, &attr);
            }
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                tracing::error!("getattr failed for ino {}: {}", ino, e);
                reply.error(libc::EIO)
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
        let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };

        if let Some(new_size) = size {
            if new_size != inode.size as u64 {
                if new_size == 0 {
                    let key = self.get_s3_key_from_inode(&inode);
                    if self
                        .rt
                        .block_on(self.object_store.truncate_file(&key))
                        .is_err()
                    {
                        return reply.error(libc::EIO);
                    }
                } else {
                    return reply.error(libc::EOPNOTSUPP);
                }
            }
        }

        let updated_inode = self.rt.block_on(self.metadata_service.update_inode(
            ino,
            size.map(|s| s as i64),
            mode.map(|m| m as i16),
            uid.map(|u| u as i32),
            gid.map(|g| g as i32),
            None,
            None,
        ));

        match updated_inode {
            Ok(inode) => {
                if mode.is_some() || uid.is_some() || gid.is_some() {
                    let key = self.get_s3_key_from_inode(&inode);
                    let new_metadata = ObjectUserMetadata {
                        mode: Some(inode.perm as u16),
                        uid: Some(inode.uid as u32),
                        gid: Some(inode.gid as u32),
                    };
                    if self
                        .rt
                        .block_on(self.object_store.replace_metadata(&key, &new_metadata))
                        .is_err()
                    {
                        tracing::warn!("Failed to update S3 metadata for key '{}'", key);
                    }
                }
                reply.attr(&TTL, &inode.to_file_attr());
            }
            Err(e) => {
                tracing::error!("Failed to update inode {} in database: {}", ino, e);
                reply.error(libc::EIO);
            }
        }
    }

    fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // We will check S3 and synchronize the directory listing if we haven't
        // done so for this directory inode yet in our process's lifetime.
        if !self.seeded_dirs.contains(&ino) {
            // Reconstruct the S3 prefix for the current directory inode.
            let parent_inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
                Ok(Some(inode)) => inode,
                // If we can't even find the inode for the directory we're listing,
                // it's a critical error.
                _ => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };

            let mut prefix = self.get_s3_key_from_inode(&parent_inode);

            //    If it's a directory (and not the root), ensure it has a trailing slash
            //    to make it an unambiguous S3 prefix. e.g., "hanna" -> "hanna/"
            if parent_inode.is_dir && !prefix.is_empty() {
                prefix.push('/');
            }

            // Perform the S3 list operation.
            let s3_entries = match self.rt.block_on(self.object_store.list_directory(&prefix)) {
                Ok(entries) => entries,
                Err(e) => {
                    tracing::error!("readdir: Failed to list S3 prefix '{}': {}", prefix, e);
                    reply.error(libc::EIO);
                    return;
                }
            };

            // Don't just rely on `is_empty`. We want to do a full sync.
            // For now, a simple approach is to create any missing entries.
            // A more robust solution would also handle deletions.
            if !s3_entries.is_empty() {
                let to_create = s3_entries
                    .into_iter()
                    .map(|e| {
                        let child_path = PathBuf::from(&prefix).join(&e.name);
                        let new_ino = path_to_ino(&child_path);
                        let perm = if e.kind == FileType::Directory {
                            0o755
                        } else {
                            0o644
                        };
                        (new_ino, e.name, perm, e.kind, req.uid(), req.gid())
                    })
                    .collect::<Vec<_>>();

                if let Err(e) = self
                    .rt
                    .block_on(self.metadata_service.create_inodes_batch(ino, to_create))
                {
                    tracing::error!(
                        "readdir: Failed to batch insert inodes for prefix '{}': {}",
                        prefix,
                        e
                    );
                    // Don't return an error here; we might still have partial data.
                }
            }

            // Mark this inode as seeded so we don't hit S3 again for it
            // during this mount session. The kernel's TTL will handle re-listing.
            self.seeded_dirs.insert(ino);
        }

        // Now, read from the local database, which is guaranteed to be populated
        // for the first access.
        let children = self
            .rt
            .block_on(self.metadata_service.list_directory(ino))
            .unwrap_or_default();

        // Emit "." ".." and the now-cached directory entries.
        let parent_ino = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(inode)) => inode.parent_ino as u64,
            _ => ino, // Fallback to self as parent if lookup fails
        };
        let mut all_entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for child in children {
            all_entries.push((child.ino, child.kind, child.name));
        }

        for (i, (entry_ino, kind, name)) in
            all_entries.into_iter().enumerate().skip(offset as usize)
        {
            if reply.add(entry_ino, (i + 1) as i64, kind, name) {
                break; // Buffer is full
            }
        }
        reply.ok();
    }

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
        let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };

        let s3_key = self.get_s3_key_from_inode(&inode);
        let start = offset as u64;

        let fut_result = self
            .rt
            .block_on(async { self.object_store.download_range(&s3_key, start, size).await });

        match fut_result {
            Ok(output) => match self.rt.block_on(output.collect()) {
                Ok(agg_bytes) => reply.data(&agg_bytes.into_bytes()),
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
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        // ---------- argument checks ----------
        let name_str = match name.to_str() {
            Some(s) => s,
            None => return reply.error(libc::EINVAL),
        };

        let parent_inode = match self.rt.block_on(self.metadata_service.get_inode(parent)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };

        if !utils::check_permission(&parent_inode.to_file_attr(), req, 2) {
            return reply.error(libc::EACCES);
        }

        // ---------- S3 key + placeholder object ----------
        let parent_path = self.get_s3_key_from_inode(&parent_inode);
        let new_path = PathBuf::from(parent_path).join(name);
        let s3_key = path_to_s3_key(&new_path);

        // Create an empty object so the key exists immediately
        let meta = ObjectUserMetadata {
            mode: Some((mode & 0o7777) as u16),
            uid: Some(req.uid()),
            gid: Some(req.gid()),
        };
        if let Err(e) = self
            .rt
            .block_on(self.object_store.create_object(&s3_key, &meta))
        {
            tracing::error!("create: S3 PutObject failed for key '{}': {e}", s3_key);
            return reply.error(libc::EIO);
        }

        let new_ino = path_to_ino(&new_path);
        match self.rt.block_on(self.metadata_service.create_inode(
            parent,
            new_ino,
            name_str,
            mode,
            FileType::RegularFile,
            req.uid(),
            req.gid(),
        )) {
            Ok(inode) => {
                let fh = self.next_fh;
                self.next_fh += 1;

                self.write_states.insert(
                    inode.ino as u64,
                    WriteState {
                        buffer: Vec::new(),
                        multipart: None,
                    },
                );

                reply.created(
                    &TTL,
                    &inode.to_file_attr(),
                    0,
                    fh,
                    fuser::consts::FOPEN_KEEP_CACHE,
                );
            }
            Err(e) => {
                tracing::error!("create: Failed to insert inode in DB: {e}");
                reply.error(libc::EIO);
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut state = match self.write_states.get_mut(&ino) {
            Some(s) => s,
            None => return reply.error(libc::EBADF),
        };

        // merge this write into the staging buffer (sparse-safe)
        let off = offset as usize;
        let end = off + data.len();
        if end as u64 > S3_MAX_SIZE {
            return reply.error(libc::E2BIG);
        }
        if end > state.buffer.len() {
            state.buffer.resize(end, 0);
        }
        state.buffer[off..end].copy_from_slice(data);

        // if buffer ≥ threshold, flush it as the next MPU part
        if state.buffer.len() >= MULTIPART_THRESHOLD {
            // start a multipart upload the first time we cross the threshold
            if state.multipart.is_none() {
                // resolve the object key once
                let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
                    Ok(Some(i)) => i,
                    _ => return reply.error(libc::ENOENT),
                };
                let key = self.get_s3_key_from_inode(&inode);

                // create MPU and remember its ID ↔ key mapping inside ObjectStore
                let upload_id = match self.rt.block_on(self.object_store.start_multipart(&key)) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("start_multipart failed for {}: {}", key, e);
                        return reply.error(libc::EIO);
                    }
                };
                state.multipart = Some(MultipartCtx {
                    upload_id,
                    next_part: 1,
                    completed: Vec::new(),
                });
            }

            // move the buffer out (drop map lock) and upload as next part
            let bytes = std::mem::take(&mut state.buffer);
            let mp = state.multipart.as_mut().unwrap();
            let part = mp.next_part;
            mp.next_part += 1;

            match self
                .rt
                .block_on(self.object_store.upload_part(&mp.upload_id, part, bytes))
            {
                Ok(completed_part) => mp.completed.push(completed_part),
                Err(e) => {
                    tracing::error!("upload_part {} failed: {}", part, e);
                    return reply.error(libc::EIO);
                }
            }
        }

        // tell the kernel we wrote everything it asked us to
        reply.written(data.len() as u32);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Take the WriteState out of the DashMap.
        let (_, mut state) = match self.write_states.remove(&ino) {
            Some(entry) => entry,
            None => return reply.ok(), // nothing buffered
        };

        // Resolve the object key once.
        let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        let key = self.get_s3_key_from_inode(&inode);

        // Handle the two cases: simple PUT vs. finish MPU.
        let final_size: i64 = if state.multipart.is_none() {
            // Small file → single PutObject.
            let bytes = std::mem::take(&mut state.buffer);
            if let Err(e) = self
                .rt
                .block_on(self.object_store.upload(&key, bytes.clone()))
            {
                tracing::error!("single PUT failed for '{}': {}", key, e);
                return reply.error(libc::EIO);
            }
            bytes.len() as i64 // new size
        } else {
            // MPU in progress → upload tail part (if any) then complete.
            let mp = state.multipart.as_mut().unwrap();

            // upload the tail (can be <5 MiB)
            if !state.buffer.is_empty() {
                let part_no = mp.next_part;
                if let Err(e) = self.rt.block_on(self.object_store.upload_part(
                    &mp.upload_id,
                    part_no,
                    std::mem::take(&mut state.buffer),
                )) {
                    tracing::error!("tail part {} upload failed: {}", part_no, e);
                    return reply.error(libc::EIO);
                }
                mp.completed.push(
                    self.rt
                        .block_on(
                            self.object_store
                                .upload_part(&mp.upload_id, part_no, Vec::new()),
                        )
                        .expect("completed part already pushed"), // or adjust logic
                );
            }

            // complete the MPU
            if let Err(e) = self.rt.block_on(
                self.object_store
                    .complete_multipart(&mp.upload_id, &mp.completed),
            ) {
                tracing::error!("complete_multipart failed for '{}': {}", key, e);
                return reply.error(libc::EIO);
            }

            // Fetch the final size from S3’s HEAD.
            match self.rt.block_on(self.object_store.get_attributes(&key)) {
                Ok(meta) => meta.content_length().unwrap() as i64, // unwrap warning. TODO: Fix later!!!!
                Err(e) => {
                    tracing::warn!("head_object after MPU failed: {}", e);
                    -1 // best-effort; don’t abort
                }
            }
        };

        // Update inode metadata in Postgres.
        let now = OffsetDateTime::now_utc();
        if self
            .rt
            .block_on(self.metadata_service.update_inode(
                ino,
                if final_size >= 0 {
                    Some(final_size)
                } else {
                    None
                },
                None,
                None,
                None,
                Some(now),
                Some(now),
            ))
            .is_err()
        {
            tracing::warn!("inode metadata update failed for {}", ino);
        }

        reply.ok();
    }

    fn open(&mut self, req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        let attr = inode.to_file_attr();
        let access_mask = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => 4,
            libc::O_WRONLY => 2,
            libc::O_RDWR => 6,
            _ => 0,
        };
        if access_mask > 0 && !utils::check_permission(&attr, req, access_mask) {
            return reply.error(libc::EACCES);
        }

        // allocate a file-handle and create WriteState for writers
        let fh = self.next_fh;
        self.next_fh += 1;

        if (flags & libc::O_ACCMODE) != libc::O_RDONLY {
            self.write_states.entry(ino).or_insert_with(|| WriteState {
                buffer: Vec::new(),
                multipart: None,
            });
        }

        reply.opened(fh, fuser::consts::FOPEN_KEEP_CACHE);
    }

    // flush is issued each time a file-descriptor is closed but does not end the
    // last open handle. We push any buffered data into the active multipart
    // upload, if its a simple upload this is a no-op. We keep the WriteState so later handles can keep writing.
    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // Pull the state _once_ and copy-out everything we’ll need later ──
        let (bytes, maybe_mpu) = {
            // scoped borrow so it’s dropped before any await
            let mut state = match self.write_states.get_mut(&ino) {
                Some(s) if !s.buffer.is_empty() => s,
                _ => return reply.ok(),
            };

            // Move the tail bytes out of the DashMap entry
            let bytes = std::mem::take(&mut state.buffer);

            // If an MPU is active, also copy out the IDs/part numbers we need
            let mpu_info = state.multipart.as_mut().map(|mp| {
                let info = (mp.upload_id.clone(), mp.next_part);
                mp.next_part += 1; // reserve this part number
                info
            });

            (bytes, mpu_info) // returned tuple
        };

        if let Some((upload_id, part_no)) = maybe_mpu {
            // multipart tail upload
            match self
                .rt
                .block_on(self.object_store.upload_part(&upload_id, part_no, bytes))
            {
                Ok(cp) => {
                    // Re-borrow to push the CompletedPart into the MPU list
                    if let Some(mut st) = self.write_states.get_mut(&ino) {
                        if let Some(mp) = st.multipart.as_mut() {
                            mp.completed.push(cp);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("flush: upload_part failed: {}", e);
                    return reply.error(libc::EIO);
                }
            }
        } else {
            // small-file path: keep data in RAM; single PUT will happen in release/fsync
            let mut st = self.write_states.get_mut(&ino).expect("state disappeared");
            st.buffer = bytes; // put the data back for later
        }

        reply.ok();
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        // flush outstanding data (same logic as in `flush`)
        if let Some(mut state) = self.write_states.get_mut(&ino) {
            if !state.buffer.is_empty() {
                // take the buffer first → releases &mut borrow early
                let bytes = std::mem::take(&mut state.buffer);

                if let Some(mp) = state.multipart.as_mut() {
                    // multipart tail
                    let part_no = mp.next_part;
                    mp.next_part += 1;
                    let upload_id = mp.upload_id.clone();
                    drop(state); // release DashMap entry lock before await

                    match self
                        .rt
                        .block_on(self.object_store.upload_part(&upload_id, part_no, bytes))
                    {
                        Ok(cp) => {
                            if let Some(mut s) = self.write_states.get_mut(&ino) {
                                s.multipart
                                    .as_mut()
                                    .expect("MPU vanished")
                                    .completed
                                    .push(cp);
                            }
                        }
                        Err(e) => {
                            tracing::error!("fsync: upload_part failed: {}", e);
                            return reply.error(libc::EIO);
                        }
                    }
                } else {
                    // small-file path: single PUT
                    let key = {
                        let inode = match self.rt.block_on(self.metadata_service.get_inode(ino)) {
                            Ok(Some(i)) => i,
                            _ => return reply.error(libc::ENOENT),
                        };
                        self.get_s3_key_from_inode(&inode)
                    };
                    if let Err(e) = self.rt.block_on(self.object_store.upload(&key, bytes)) {
                        tracing::error!("fsync: put_object failed: {}", e);
                        return reply.error(libc::EIO);
                    }
                }
            }
        }

        // update timestamps unless caller set O_DSYNC / datasync flag ────
        if !datasync {
            let now = OffsetDateTime::now_utc();
            let _ = self.rt.block_on(self.metadata_service.update_inode(
                ino,
                None,
                None,
                None,
                None,
                Some(now),
                Some(now),
            ));
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
        let name_str = match name.to_str() {
            Some(s) => s,
            None => return reply.error(libc::EINVAL),
        };

        let parent_inode = match self.rt.block_on(self.metadata_service.get_inode(parent)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if !utils::check_permission(&parent_inode.to_file_attr(), req, 2) {
            return reply.error(libc::EACCES);
        }

        let parent_s3_path = self.get_s3_key_from_inode(&parent_inode);
        let new_dir_path = PathBuf::from(parent_s3_path).join(name);
        let new_dir_key = format!("{}/", path_to_s3_key(&new_dir_path));

        let metadata = ObjectUserMetadata {
            mode: Some((mode & 0o7777) as u16),
            uid: Some(req.uid()),
            gid: Some(req.gid()),
        };

        if let Err(e) = self
            .rt
            .block_on(self.object_store.create_object(&new_dir_key, &metadata))
        {
            tracing::info!("mkdir failed: {}", e);
            return reply.error(libc::EIO);
        }

        let new_ino = path_to_ino(&new_dir_path);
        match self.rt.block_on(self.metadata_service.create_inode(
            parent,
            new_ino,
            name_str,
            mode,
            FileType::Directory,
            req.uid(),
            req.gid(),
        )) {
            Ok(inode) => reply.entry(&TTL, &inode.to_file_attr(), 0),
            Err(e) => {
                tracing::info!("mkdir failed creating inode: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap();
        let parent_inode = match self.rt.block_on(self.metadata_service.get_inode(parent)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if !utils::check_permission(&parent_inode.to_file_attr(), req, 2) {
            return reply.error(libc::EACCES);
        }

        let inode_to_delete = match self
            .rt
            .block_on(self.metadata_service.lookup(parent, name_str))
        {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if inode_to_delete.is_dir {
            return reply.error(libc::EISDIR);
        }

        let file_key = self.get_s3_key_from_inode(&inode_to_delete);
        if let Err(e) = self.rt.block_on(self.object_store.delete_object(&file_key)) {
            tracing::error!("unlink of S3 object {} failed: {}", file_key, e);
            return reply.error(libc::EIO);
        }

        if self
            .rt
            .block_on(self.metadata_service.remove_inode(parent, name_str))
            .is_err()
        {
            return reply.error(libc::EIO);
        }
        reply.ok();
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_str().unwrap();
        let parent_inode = match self.rt.block_on(self.metadata_service.get_inode(parent)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if !utils::check_permission(&parent_inode.to_file_attr(), req, 2) {
            return reply.error(libc::EACCES);
        }

        let inode_to_delete = match self
            .rt
            .block_on(self.metadata_service.lookup(parent, name_str))
        {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if !inode_to_delete.is_dir {
            return reply.error(libc::ENOTDIR);
        }

        let dir_key = format!("{}/", self.get_s3_key_from_inode(&inode_to_delete));
        if self
            .rt
            .block_on(self.object_store.delete_object(&dir_key))
            .is_err()
        {
            // This might not be a fatal error if the directory was implicit
            tracing::warn!("Could not delete S3 object for directory {}", dir_key);
        }

        match self
            .rt
            .block_on(self.metadata_service.remove_inode(parent, name_str))
        {
            Ok(_) => reply.ok(),
            Err(e) => {
                if e.to_string().contains("Directory not empty") {
                    reply.error(libc::ENOTEMPTY)
                } else {
                    reply.error(libc::EIO)
                }
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
        let name_str = name.to_str().unwrap();
        let newname_str = newname.to_str().unwrap();
        tracing::info!(
            "rename: parent={}, name='{}', newparent={}, newname='{}'",
            parent,
            name_str,
            newparent,
            newname_str
        );

        // Permission Checks
        let old_parent_inode = match self.rt.block_on(self.metadata_service.get_inode(parent)) {
            Ok(Some(i)) => i,
            _ => return reply.error(libc::ENOENT),
        };
        if !utils::check_permission(&old_parent_inode.to_file_attr(), req, 2) {
            return reply.error(libc::EACCES);
        }
        if parent != newparent {
            let new_parent_inode =
                match self.rt.block_on(self.metadata_service.get_inode(newparent)) {
                    Ok(Some(i)) => i,
                    _ => return reply.error(libc::ENOENT),
                };
            if !utils::check_permission(&new_parent_inode.to_file_attr(), req, 2) {
                return reply.error(libc::EACCES);
            }
        }

        // Find the source inode, with fallback for macOS rename patterns
        let source_inode = match self
            .rt
            .block_on(self.metadata_service.lookup(parent, name_str))
        {
            Ok(Some(inode)) => inode, // Happy path: Found the inode directly.
            Ok(None) => {
                // Fallback: The exact name was not found. This can happen during
                // atomic save operations. Let's see if we can infer the source.
                tracing::warn!(
                    "rename: Initial lookup for '{}' in parent ino {} failed. Attempting fallback.",
                    name_str,
                    parent
                );
                let children = match self
                    .rt
                    .block_on(self.metadata_service.list_directory(parent))
                {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "rename fallback: Failed to list parent dir {}: {}",
                            parent,
                            e
                        );
                        return reply.error(libc::EIO);
                    }
                };

                // Heuristic: If there is exactly one regular file in the directory,
                // assume that's the one the OS wants to rename.
                let files: Vec<_> = children
                    .into_iter()
                    .filter(|e| e.kind == FileType::RegularFile)
                    .collect();

                if files.len() == 1 {
                    let assumed_name = &files[0].name;
                    tracing::warn!(
                        "rename fallback: Assuming target is the single file in the directory: '{}'",
                        assumed_name
                    );
                    // Re-lookup with the inferred name to get the full inode.
                    match self
                        .rt
                        .block_on(self.metadata_service.lookup(parent, assumed_name))
                    {
                        Ok(Some(inode)) => inode,
                        _ => {
                            tracing::error!(
                                "rename fallback: Could not re-lookup assumed file '{}'",
                                assumed_name
                            );
                            return reply.error(libc::ENOENT);
                        }
                    }
                } else {
                    tracing::error!(
                        "rename fallback: Found {} files, cannot guess target. Aborting.",
                        files.len()
                    );
                    return reply.error(libc::ENOENT);
                }
            }
            Err(e) => {
                tracing::error!(
                    "rename: DB error during initial lookup for '{}': {}",
                    name_str,
                    e
                );
                return reply.error(libc::EIO);
            }
        };

        // Perform S3 and DB Rename
        let old_key_base = self.get_s3_key_from_inode(&source_inode);
        let new_parent_inode = self
            .rt
            .block_on(self.metadata_service.get_inode(newparent))
            .unwrap()
            .unwrap();

        let new_parent_path_base = self.get_s3_key_from_inode(&new_parent_inode);
        let new_key_base = PathBuf::from(new_parent_path_base)
            .join(newname)
            .to_str()
            .unwrap()
            .to_string();

        let s3_rename_result = if source_inode.is_dir {
            self.rt.block_on(
                self.object_store
                    .rename_dir(&format!("{}/", old_key_base), &format!("{}/", new_key_base)),
            )
        } else {
            self.rt
                .block_on(self.object_store.rename_file(&old_key_base, &new_key_base))
        };

        if let Err(e) = s3_rename_result {
            // This inner fallback for ._* files is still valuable.
            if name_str.starts_with("._") && e.to_string().contains("NoSuchKey") {
                tracing::warn!(
                    "Ignoring S3 NoSuchKey for temp file rename: {}. Assuming OS-led swap.",
                    old_key_base
                );
            } else {
                tracing::error!(
                    "S3 rename from '{}' to '{}' failed: {}",
                    old_key_base,
                    new_key_base,
                    e
                );
                return reply.error(libc::EIO);
            }
        }

        match self.rt.block_on(self.metadata_service.rename_inode(
            source_inode.ino as u64,
            newparent,
            newname_str,
        )) {
            Ok(_) => reply.ok(),
            Err(e) => {
                tracing::error!(
                    "DB rename for ino {} to new parent {} with name '{}' failed: {}",
                    source_inode.ino,
                    newparent,
                    newname_str,
                    e
                );
                reply.error(libc::EIO);
            }
        }
    }
}

fn path_to_s3_key(path: &PathBuf) -> String {
    path.to_string_lossy().trim_start_matches('/').to_string()
}
