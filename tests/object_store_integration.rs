use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use fuser::FileType;
use sky_mount::error::FsError;
use sky_mount::object_store::{ObjectKind, ObjectStore, ObjectUserMetadata};
use uuid::Uuid;

// Helper function to set up a test environment
async fn setup_test_store() -> (ObjectStore, String) {
    // These settings point to the local MinIO container
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("AWS_REGION", "us-east-1"); // region 
    }

    let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = Client::new(&conf);

    // Create a unique bucket name for each test run to ensure isolation
    let bucket_name = format!("test-bucket-{}", Uuid::new_v4());

    let bucket_config = CreateBucketConfiguration::builder()
        .location_constraint(BucketLocationConstraint::from("us-east-1"))
        .build();
    s3_client
        .create_bucket()
        .bucket(&bucket_name)
        .create_bucket_configuration(bucket_config)
        .send()
        .await
        .expect("Failed to create test bucket");

    let store = ObjectStore::new(bucket_name.clone())
        .await
        .expect("Failed to create store");
    (store, bucket_name)
}

#[tokio::test]
async fn create_and_get_attributes() {
    let (store, _bucket) = setup_test_store().await;
    let key = "test_file.txt";
    let metadata = ObjectUserMetadata {
        mode: Some(0o644),
        uid: Some(1000),
        gid: Some(1000),
    };

    // 1. Create the object
    store
        .create_object(key, &metadata)
        .await
        .expect("Failed to create object");

    // 2. Get its attributes
    let attrs = store
        .get_attributes(key)
        .await
        .expect("Failed to get attributes");

    // 3. Verify
    assert_eq!(attrs.content_length(), Some(0));
    let s3_meta = attrs.metadata().unwrap();
    assert_eq!(s3_meta.get("mode").unwrap(), "644");
    assert_eq!(s3_meta.get("uid").unwrap(), "1000");
    assert_eq!(s3_meta.get("gid").unwrap(), "1000");
}

#[tokio::test]
async fn upload_and_download() {
    let (store, _bucket) = setup_test_store().await;
    let key = "upload_test.txt";
    let content = "hello world from s3fuse!".as_bytes().to_vec();

    // 1. Upload the data
    store
        .upload(key, content.clone())
        .await
        .expect("Upload failed");

    // 2. Verify attributes
    let attrs = store
        .get_attributes(key)
        .await
        .expect("Get attributes failed");
    assert_eq!(attrs.content_length(), Some(content.len() as i64));

    // 3. Download and verify content
    let downloaded_stream = store
        .download_range(key, 0, content.len() as u32)
        .await
        .unwrap();
    let downloaded_bytes = downloaded_stream.collect().await.unwrap().into_bytes();
    assert_eq!(downloaded_bytes.as_ref(), content.as_slice());
}

#[tokio::test]
async fn list_directory() {
    let (store, _bucket) = setup_test_store().await;

    // 1. Setup a directory structure
    store
        .create_object("dir1/file1.txt", &Default::default())
        .await
        .unwrap();
    store
        .create_object("dir1/subdir/file2.txt", &Default::default())
        .await
        .unwrap();
    store
        .create_object("file3.txt", &Default::default())
        .await
        .unwrap();

    // 2. List the root directory
    let entries = store.list_directory("").await.unwrap();
    assert_eq!(entries.len(), 2);
    assert!(
        entries
            .iter()
            .any(|e| e.name == "dir1" && e.kind == FileType::Directory)
    );
    assert!(
        entries
            .iter()
            .any(|e| e.name == "file3.txt" && e.kind == FileType::RegularFile)
    );

    // 3. List the subdirectory
    let dir1_entries = store.list_directory("dir1/").await.unwrap();
    assert_eq!(dir1_entries.len(), 2);
    assert!(
        dir1_entries
            .iter()
            .any(|e| e.name == "subdir" && e.kind == FileType::Directory)
    );
    assert!(
        dir1_entries
            .iter()
            .any(|e| e.name == "file1.txt" && e.kind == FileType::RegularFile)
    );
}

#[tokio::test]
async fn delete_object() {
    let (store, _bucket) = setup_test_store().await;
    let key = "to_be_deleted.txt";

    // 1. Create and verify it exists
    store.create_object(key, &Default::default()).await.unwrap();
    assert!(store.get_attributes(key).await.is_ok());

    // 2. Delete it
    store.delete_object(key).await.unwrap();

    // 3. Verify it's gone
    match store.get_attributes(key).await {
        Err(FsError::NotFound) => { /* Success */ }
        _ => panic!("Expected NotFound error after deletion"),
    }
}

#[tokio::test]
async fn rename_file() {
    let (store, _bucket) = setup_test_store().await;
    let old_key = "original_name.txt";
    let new_key = "new_name.txt";
    let content = b"rename test".to_vec();

    // 1. Create original file
    store.upload(old_key, content.clone()).await.unwrap();

    // 2. Rename it
    store.rename_file(old_key, new_key).await.unwrap();

    // 3. Verify old key is gone and new key exists with correct content
    assert!(store.get_attributes(old_key).await.is_err());
    let new_content = store
        .download_range(new_key, 0, content.len() as u32)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(new_content.as_ref(), content.as_slice());
}

#[tokio::test]
async fn stat_object_discovery() {
    let (store, _bucket) = setup_test_store().await;

    store
        .create_object("my_file.txt", &Default::default())
        .await
        .unwrap();

    store
        .create_object("explicit_dir/", &Default::default())
        .await
        .unwrap();

    store
        .create_object("implicit_dir/another_file.txt", &Default::default())
        .await
        .unwrap();

    let stat_file = store.stat_object("my_file.txt").await.unwrap();
    assert!(matches!(stat_file, ObjectKind::File(_)));

    let stat_explicit_dir = store.stat_object("explicit_dir").await.unwrap();
    assert!(matches!(stat_explicit_dir, ObjectKind::Directory(Some(_))));

    let stat_implicit_dir = store.stat_object("implicit_dir").await.unwrap();
    assert!(matches!(stat_implicit_dir, ObjectKind::Directory(None)));

    let stat_nonexistent = store.stat_object("nonexistent").await;
    assert!(matches!(stat_nonexistent, Err(FsError::NotFound)));
}

#[tokio::test]
async fn error_on_nonexistent() {
    let (store, _bucket) = setup_test_store().await;
    let key = "does-not-exist";

    // get_attributes should fail with NotFound
    let get_res = store.get_attributes(key).await;
    assert!(matches!(get_res, Err(FsError::NotFound)));

    // download_range should fail
    let dl_res = store.download_range(key, 0, 1).await;
    assert!(dl_res.is_err(), "Download should fail for non-existent key");

    // replace_metadata should fail
    let meta_res = store.replace_metadata(key, &Default::default()).await;
    assert!(
        meta_res.is_err(),
        "replace_metadata should fail for non-existent key"
    );
}
