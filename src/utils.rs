use std::{collections::HashMap, time::SystemTime};

use fuser::{FileAttr, FileType, Request};

pub fn s3_meta_to_file_attr(
    ino: u64,
    key: &str,
    meta: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    default_uid: u32,
    default_gid: u32,
) -> FileAttr {
    let kind = if key.ends_with('/') {
        FileType::Directory
    } else {
        FileType::RegularFile
    };

    let empty_map = HashMap::new();
    let s3_meta = meta.metadata().unwrap_or(&empty_map);
    let perm = s3_meta
        .get("mode")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0o644); // Default to rw-r--r--
    let uid = s3_meta
        .get("uid")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_uid);
    let gid = s3_meta
        .get("gid")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(default_gid);

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
