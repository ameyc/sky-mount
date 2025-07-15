use aws_sdk_s3::Client;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

const DB_URL: &str = "postgresql://fsuser:secret@localhost:5432/fsmeta";

// ==========================================================================================
//  The Test Harness: MountGuard
//
//  This struct uses the RAII pattern. When it's created, it mounts the filesystem.
//  When it's dropped (at the end of the test), its `Drop` implementation
//  is called automatically, ensuring cleanup happens even if a test panics.
// ==========================================================================================
struct MountGuard {
    mount_point: PathBuf,
    child: Child,
    // We keep the TempDir object itself to ensure the directory isn't deleted until the guard is dropped.
    _temp_dir: tempfile::TempDir,
}

impl MountGuard {
    /// Mounts the filesystem for testing. Panics on failure.
    fn new(bin_path: &Path, bucket_name: &str, db_url: &str) -> Self {
        let temp_dir = tempfile::Builder::new()
            .prefix("s3fuse-e2e-")
            .tempdir()
            .expect("Failed to create temp mount point");
        let mount_point = temp_dir.path().to_path_buf();

        println!(
            "Attempting to mount bucket '{}' on '{}'",
            bucket_name,
            mount_point.display()
        );

        let mut child = Command::new(bin_path)
            .arg("--") // Use explicit flags for clarity
            .arg(bucket_name)
            .arg(db_url)
            .arg(mount_point.as_os_str())
            .spawn()
            .expect("Failed to spawn s3fuse process");

        // --- Robust, Cross-Platform Mount Polling ---
        let mut mount_ready = false;
        for _ in 0..15 {
            // Poll for up to 3 seconds
            thread::sleep(Duration::from_millis(200));
            // The `mount` command exists on both Linux and macOS. We check its output.
            let output = Command::new("mount")
                .output()
                .expect("Failed to run 'mount' command");
            tracing::info!("{:?}", output.stdout);
            if String::from_utf8_lossy(&output.stdout).contains(mount_point.to_str().unwrap()) {
                mount_ready = true;
                break;
            }
        }

        if !mount_ready {
            child.kill().expect("Failed to kill hanging FUSE process");
            let output = child
                .wait_with_output()
                .expect("Failed to get child output");
            panic!(
                "FUSE mount failed to become ready!\nSTDOUT:\n{}\nSTDERR:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        println!("Mount successful.");
        Self {
            mount_point,
            child,
            _temp_dir: temp_dir,
        }
    }

    fn mount_point(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        println!("Cleaning up mount at '{}'...", self.mount_point.display());
        // Use fusermount for a graceful unmount. This is the polite way.
        let _ = Command::new("fusermount")
            .arg("-u")
            .arg(&self.mount_point)
            .status();
        thread::sleep(Duration::from_millis(200)); // Give it a moment to process.

        // Ensure the process is terminated, in case fusermount failed.
        if let Err(e) = self.child.kill() {
            eprintln!(
                "Could not kill FUSE process {}: {}. It may have already exited.",
                self.child.id(),
                e
            );
        }
        self.child.wait().expect("Failed to wait on child process");
        println!("Cleanup complete.");
    }
}

// ==========================================================================================
//  The Test Orchestrator
// ==========================================================================================

/// This single `#[test]` function orchestrates the entire integration test suite.
#[test]
#[ignore] // This is an expensive test. Run explicitly with `cargo test -- --ignored`.
fn filesystem_contract_tests() {
    // --- 1. One-Time Setup ---
    println!("--- E2E SUITE: Global Setup ---");
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_ENDPOINT_URL", "http://127.0.0.1:9000");
        std::env::set_var("AWS_REGION", "us-east-1");
    }
    let bucket_name = format!("s3fuse-e2e-{}", Uuid::new_v4());
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(create_test_bucket(&bucket_name));

    let bin_path = build_project();
    // --- 2. Mount Filesystem (RAII Guard) ---
    let guard = MountGuard::new(&bin_path, &bucket_name, DB_URL);
    let mount_point = guard.mount_point();

    // --- 3. Run Test Scenarios ---
    // Each scenario runs in its own isolated subdirectory.
    let base_path = mount_point.join("scenario_base");
    fs::create_dir(&base_path).unwrap();

    test_scenario_write_read(&base_path.join("write_read"));
    test_scenario_directories(&base_path.join("directories"));
    test_scenario_rename_and_move(&base_path.join("movement"));
    test_scenario_deletion(&base_path.join("deletion"));
    test_scenario_metadata_and_overwrite(&base_path.join("metadata"));
    test_scenario_empty_file(&base_path.join("empty"));
    test_scenario_deeply_nested(&base_path.join("nested"));
    test_scenario_special_chars(&base_path.join("special_chars"));
    test_scenario_large_file(&base_path.join("large_file"));
    test_scenario_concurrent_writes(&base_path.join("concurrent_writes"));
    test_scenario_permission_denied(&base_path.join("perms_denied"));

    // --- 4. Automatic Teardown ---
    // The `guard` is dropped here, automatically unmounting and cleaning up.
    println!("--- E2E SUITE: All scenarios passed! ---");
}

// ==========================================================================================
//  Test Suite Setup Helpers
// ==========================================================================================

async fn create_test_bucket(bucket_name: &str) {
    let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = Client::new(&conf);
    let bucket_config = CreateBucketConfiguration::builder()
        .location_constraint(BucketLocationConstraint::from("us-east-1"))
        .build();
    s3_client
        .create_bucket()
        .bucket(bucket_name)
        .create_bucket_configuration(bucket_config)
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to create E2E test bucket '{}': {}", bucket_name, e));
}

fn build_project() -> PathBuf {
    let bin_path = PathBuf::from("target/debug/sky-mount"); // Assumes crate name

    if !bin_path.exists() {
        let build_status = Command::new("cargo")
            .arg("build")
            .status()
            .expect("Failed to run cargo build");
        assert!(build_status.success(), "Cargo build failed");
    }

    assert!(
        bin_path.exists(),
        "sky-mount binary not found in target/debug/"
    );
    bin_path
}

fn test_scenario_write_read(test_dir: &Path) {
    println!("--- Running Scenario: Write/Read ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("hello.txt");
    let content = "Hello, FUSE!";
    fs::write(&file_path, content).expect("Failed to write");
    let read_content = fs::read_to_string(&file_path).expect("Failed to read");
    assert_eq!(content, read_content);
    println!("✅ Scenario OK: Write/Read");
}

fn test_scenario_directories(test_dir: &Path) {
    println!("--- Running Scenario: Directories ---");
    fs::create_dir(test_dir).unwrap();
    fs::create_dir(test_dir.join("subdir1")).unwrap();
    File::create(test_dir.join("file1.txt")).unwrap();
    let mut entries: Vec<String> = fs::read_dir(test_dir)
        .unwrap()
        .map(|r| r.unwrap().file_name().into_string().unwrap())
        .collect();
    entries.sort();
    assert_eq!(entries, vec!["file1.txt", "subdir1"]);
    println!("✅ Scenario OK: Directories");
}

fn test_scenario_rename_and_move(test_dir: &Path) {
    println!("--- Running Scenario: Rename & Move ---");
    fs::create_dir_all(test_dir.join("dir2")).unwrap();
    let original_path = test_dir.join("original.txt");
    fs::write(&original_path, "move data").unwrap();

    let renamed_path = test_dir.join("renamed.txt");
    fs::rename(&original_path, &renamed_path).unwrap(); // Rename in place
    assert!(!original_path.exists());
    assert!(renamed_path.exists());
    let moved_path = test_dir.join("dir2/moved.txt");
    fs::rename(&renamed_path, &moved_path).unwrap(); // Move to new directory
    assert!(!renamed_path.exists());
    assert!(moved_path.exists());
    assert_eq!(fs::read_to_string(&moved_path).unwrap(), "move data");
    println!("✅ Scenario OK: Rename & Move");
}

fn test_scenario_deletion(test_dir: &Path) {
    println!("--- Running Scenario: Deletion ---");
    fs::create_dir_all(test_dir.join("dir_to_remove")).unwrap();
    let file_path = test_dir.join("dir_to_remove/file.txt");
    fs::write(&file_path, "delete me").unwrap();

    // Test rmdir on non-empty directory MUST fail
    assert!(fs::remove_dir(test_dir.join("dir_to_remove")).is_err());

    fs::remove_file(&file_path).unwrap();
    assert!(!file_path.exists());

    fs::remove_dir(test_dir.join("dir_to_remove")).unwrap();
    assert!(!test_dir.join("dir_to_remove").exists());
    println!("✅ Scenario OK: Deletion");
}

fn test_scenario_metadata_and_overwrite(test_dir: &Path) {
    println!("--- Running Scenario: Metadata & Overwrite ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("metadata.txt");
    fs::write(&file_path, "long initial line").unwrap();

    // 1. Test setting and checking permissions
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o755)).unwrap();
    let mode = fs::metadata(&file_path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755);

    // 2. Test overwriting and truncating the file
    fs::write(&file_path, "short").unwrap();
    assert_eq!(fs::metadata(&file_path).unwrap().len(), 5);
    assert_eq!(fs::read_to_string(&file_path).unwrap(), "short");

    // 3. Test explicit truncate to zero
    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();
    drop(file);
    assert_eq!(fs::metadata(&file_path).unwrap().len(), 0);
    println!("✅ Scenario OK: Metadata & Overwrite");
}

fn test_scenario_empty_file(test_dir: &Path) {
    println!("--- Running Scenario: Empty File ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("empty.txt");

    // Create an empty file
    std::fs::File::create(&file_path).unwrap();
    assert!(file_path.exists());

    // Check its metadata
    let metadata = fs::metadata(&file_path).unwrap();
    assert_eq!(metadata.len(), 0);

    // Ensure reading it produces an empty string/buffer
    let content = fs::read_to_string(&file_path).unwrap();
    assert_eq!(content, "");
    println!("✅ Scenario OK: Empty File");
}

fn test_scenario_deeply_nested(test_dir: &Path) {
    println!("--- Running Scenario: Deeply Nested ---");
    let deep_path = test_dir.join("a/b/c/d/e");
    fs::create_dir_all(&deep_path).unwrap();
    assert!(deep_path.exists());

    let file_path = deep_path.join("deep_file.txt");
    fs::write(&file_path, "deep").unwrap();
    assert_eq!(fs::read_to_string(&file_path).unwrap(), "deep");

    // Test recursive deletion if you implement it, otherwise remove directories one by one
    // For this example, assuming a helper or manual cleanup
    fs::remove_file(&file_path).unwrap();
    fs::remove_dir(test_dir.join("a/b/c/d/e")).unwrap();
    fs::remove_dir(test_dir.join("a/b/c/d")).unwrap();
    fs::remove_dir(test_dir.join("a/b/c")).unwrap();
    fs::remove_dir(test_dir.join("a/b")).unwrap();
    fs::remove_dir(test_dir.join("a")).unwrap();
    assert!(!test_dir.join("a").exists());
    println!("✅ Scenario OK: Deeply Nested");
}

fn test_scenario_special_chars(test_dir: &Path) {
    println!("--- Running Scenario: Special Characters in Filenames ---");
    fs::create_dir(test_dir).unwrap();
    // A mix of common and potentially problematic characters
    let weird_name = "file with spaces, & ampersands, maybe a % percent.txt";
    let file_path = test_dir.join(weird_name);

    fs::write(&file_path, "special chars").unwrap();
    assert!(file_path.exists());
    assert_eq!(fs::read_to_string(&file_path).unwrap(), "special chars");

    let entries: Vec<String> = fs::read_dir(test_dir)
        .unwrap()
        .map(|r| r.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec![weird_name]);

    fs::remove_file(&file_path).unwrap();
    assert!(!file_path.exists());
    println!("✅ Scenario OK: Special Characters");
}

fn test_scenario_large_file(test_dir: &Path) {
    println!("--- Running Scenario: Large File (MPU) ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("large_file.bin");

    // Create a file larger than a typical MPU threshold (e.g., > 8 MiB)
    let chunk_size = 1024 * 1024; // 1 MiB
    let chunks = 10;
    let large_data: Vec<u8> = vec![0xAB; chunk_size];

    let mut file = std::fs::File::create(&file_path).unwrap();
    for _ in 0..chunks {
        file.write_all(&large_data).unwrap();
    }
    drop(file);

    // Verify size and content
    let metadata = fs::metadata(&file_path).unwrap();
    assert_eq!(metadata.len(), (chunk_size * chunks) as u64);

    let mut read_file = std::fs::File::open(&file_path).unwrap();
    let mut read_buffer = Vec::new();
    read_file.read_to_end(&mut read_buffer).unwrap();
    assert_eq!(read_buffer.len(), (chunk_size * chunks));
    assert!(read_buffer.iter().all(|&byte| byte == 0xAB));

    println!("✅ Scenario OK: Large File (MPU)");
}

#[warn(dead_code)]
fn test_scenario_concurrent_writes(test_dir: &Path) {
    println!("--- Running Scenario: Concurrent Writes ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("concurrent.txt");
    std::fs::File::create(&file_path).unwrap(); // Create the file first

    let mut handles = vec![];
    let num_threads = 5;

    for i in 0..num_threads {
        let thread_path = file_path.clone();
        let handle = thread::spawn(move || {
            // Each thread writes a unique message. Appending makes verification easier.
            let message = format!("Message from thread {}\n", i);
            let mut file = OpenOptions::new().append(true).open(thread_path).unwrap();
            file.write_all(message.as_bytes()).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verification
    let content = fs::read_to_string(&file_path).unwrap();
    for i in 0..num_threads {
        assert!(content.contains(&format!("Message from thread {}", i)));
    }
    println!("✅ Scenario OK: Concurrent Writes");
}

fn test_scenario_permission_denied(test_dir: &Path) {
    println!("--- Running Scenario: Permission Denied ---");
    fs::create_dir(test_dir).unwrap();
    let file_path = test_dir.join("restricted.txt");
    fs::write(&file_path, "secret data").unwrap();

    // Set to read-only for everyone
    fs::set_permissions(&file_path, fs::Permissions::from_mode(0o444)).unwrap();

    // Attempting to write should fail.
    // The `fs::write` call should return a `PermissionDenied` error.
    let write_result = fs::write(&file_path, "overwrite attempt");
    assert!(write_result.is_err());
    assert_eq!(
        write_result.unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );

    // Reading should still succeed
    assert_eq!(fs::read_to_string(&file_path).unwrap(), "secret data");

    println!("✅ Scenario OK: Permission Denied");
}
