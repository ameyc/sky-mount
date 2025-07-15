use std::{collections::HashMap, time::SystemTime};

use fuser::{FileAttr, FileType, Request};

const DEFAULT_FILE_MODE: u16 = 0o644;
const DEFAULT_DIR_MODE: u16 = 0o755;

pub fn s3_meta_to_file_attr(
    ino: u64,
    key: &str,
    meta: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    default_uid: u32,
    default_gid: u32,
) -> FileAttr {
    let (kind, default_perm) = if key.ends_with('/') {
        (FileType::Directory, DEFAULT_DIR_MODE)
    } else {
        (FileType::RegularFile, DEFAULT_FILE_MODE)
    };

    let empty_map = HashMap::new();
    let s3_meta = meta.metadata().unwrap_or(&empty_map);

    // NOTE: This bug was a massive pain in the rear to track down.
    let perm = match s3_meta.get("mode") {
        Some(s) => match u16::from_str_radix(s.trim_start_matches("0o"), 8) {
            Ok(m) => {
                tracing::debug!("mode for {} parsed out as {}", key, m);
                m
            }
            Err(e) => {
                tracing::warn!("invalid mode metadata `{}`: {}", s, e);
                default_perm
            }
        },
        None => default_perm,
    };

    let uid = s3_meta
        .get("uid")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_uid);
    let gid = s3_meta
        .get("gid")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_gid);

    let nlink = if kind == FileType::Directory { 2 } else { 1 };

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
        nlink,
        uid,
        gid,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

// Helper function for POSIX permission checks.
pub fn check_permission(file_attr: &FileAttr, req: &Request, access_mask: u32) -> bool {
    let mode = file_attr.perm as u32;
    let req_uid = req.uid();
    let req_gid = req.gid();

    let owner_match = req_uid == file_attr.uid;
    let group_match = req_gid == file_attr.gid;

    let perms = if owner_match {
        (mode >> 6) & 0o7
    } else if group_match {
        (mode >> 3) & 0o7
    } else {
        mode & 0o7
    };

    (perms & access_mask) == access_mask
}
