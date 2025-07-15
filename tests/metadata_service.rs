use anyhow::Result;
use sqlx::{Executor, PgPool};
use uuid::Uuid;

use sky_mount::metadata_service::{DirEntry, MetadataService}; // adjust path

const DB_URL: &str = "postgresql://fsuser:secret@localhost:5432/fsmeta";

/// Every test lives in its own schema so they can run in parallel ( cargo test ‑‑ --test-threads=N)
async fn fresh_service() -> Result<(MetadataService, String)> {
    let schema = format!("test_{}", Uuid::new_v4().simple());
    // create the service; this does NOT yet create the schema
    let svc = MetadataService::new(DB_URL, &schema).await?;
    // create schema + root inode
    svc.initialize_schema(1000, 1000).await?;
    Ok((svc, schema))
}

#[tokio::test]
async fn root_inode_is_created() -> Result<()> {
    let (svc, _schema) = fresh_service().await?;
    let root = svc.get_inode(1).await?.expect("root missing");
    assert!(root.is_dir);
    assert_eq!(root.nlink, 2);
    Ok(())
}

#[tokio::test]
async fn create_and_lookup_file() -> Result<()> {
    let (svc, _schema) = fresh_service().await?;
    let file = svc
        .create_inode(
            1,
            2,
            "hello.txt",
            0o644,
            fuser::FileType::RegularFile,
            1000,
            1000,
        )
        .await?;
    // lookup by name
    let by_name = svc.lookup(1, "hello.txt").await?;
    assert_eq!(by_name.unwrap().ino, file.ino);
    // lookup by ino
    let by_ino = svc.get_inode(file.ino as u64).await?;
    assert!(by_ino.is_some());
    Ok(())
}

#[tokio::test]
async fn list_directory_returns_entries() -> Result<()> {
    let (svc, _schema) = fresh_service().await?;
    let _ = svc
        .create_inode(
            1,
            2,
            "a.dat",
            0o600,
            fuser::FileType::RegularFile,
            1000,
            1000,
        )
        .await?;
    // create sub‑dir
    let _ = svc
        .create_inode(
            1,
            3,
            "subdir",
            0o755,
            fuser::FileType::Directory,
            1000,
            1000,
        )
        .await?;
    let entries = svc.list_directory(1).await?;
    assert_eq!(entries.len(), 2);
    // stable ordering not required, just check names and kinds
    let names: Vec<_> = entries.iter().map(|e| (&e.name, e.kind)).collect();
    assert!(names.contains(&(&String::from("a.dat"), fuser::FileType::RegularFile)));
    assert!(names.contains(&(&String::from("subdir"), fuser::FileType::Directory)));
    Ok(())
}

#[tokio::test]
async fn remove_inode_updates_nlink() -> Result<()> {
    let (svc, _schema) = fresh_service().await?;
    // new directory under /
    let dir = svc
        .create_inode(1, 2, "dir", 0o755, fuser::FileType::Directory, 1000, 1000)
        .await?;
    // remove it
    svc.remove_inode(1, "dir").await?;
    // make sure it is gone
    assert!(svc.get_inode(dir.ino as u64).await?.is_none());
    // root nlink decremented back to 2
    let root = svc.get_inode(1).await?.unwrap();
    assert_eq!(root.nlink, 2);
    Ok(())
}

#[tokio::test]
async fn cant_remove_non_empty_dir() -> Result<()> {
    let (svc, _schema) = fresh_service().await?;
    let _subdir = svc
        .create_inode(1, 2, "dir", 0o755, fuser::FileType::Directory, 1000, 1000)
        .await?;
    let _file = svc
        .create_inode(
            _subdir.ino as u64,
            3,
            "file",
            0o644,
            fuser::FileType::RegularFile,
            1000,
            1000,
        )
        .await?;
    let res = svc.remove_inode(1, "dir").await;
    assert!(res.is_err());
    Ok(())
}
