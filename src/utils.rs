use fuser::{FileAttr, Request};

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
