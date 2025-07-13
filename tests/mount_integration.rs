// tests/mount_integration.rs

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

/// A helper struct that manages the FUSE mount process.
/// When it's created, it mounts the filesystem.
/// When it's dropped, it kills the process and unmounts it.
struct MountGuard {
    mount_point: PathBuf,
    child: Child,
}

impl MountGuard {
    /// Mounts the S3-FUSE filesystem.
    ///
    /// # Arguments
    /// * `bin_path` - Path to the `s3-fuse` executable.
    /// * `bucket_name` - The S3 bucket to mount.
    ///
    /// # Returns
    /// A `MountGuard` instance. Panics on failure.
    fn new(bin_path: &Path, bucket_name: &str) -> Self {
        let mount_dir = tempfile::Builder::new()
            .prefix("s3-fuse-test-")
            .tempdir()
            .expect("Failed to create temp mount point");

        println!(
            "Mounting bucket '{}' on '{}'",
            bucket_name,
            mount_dir.path().display()
        );

        let mut child = Command::new(bin_path)
            .arg(bucket_name)
            .arg(mount_dir.path())
            .stdout(Stdio::piped()) // Capture stdout for debugging
            .stderr(Stdio::piped()) // Capture stderr
            .spawn()
            .expect("Failed to spawn s3-fuse process");

        // Give the FUSE mount a moment to initialize.
        // A more robust solution might poll `/proc/mounts` or use `mountpoint -q`.
        thread::sleep(Duration::from_secs(2));

        // Check if the process is still running. If it exited, something went wrong.
        if let Ok(Some(status)) = child.try_wait() {
            panic!(
                "FUSE process exited prematurely with status: {}. Check stderr/stdout.",
                status
            );
        }

        Self {
            mount_point: mount_dir.into_path(), // The TempDir is consumed and its path is kept.
            child,
        }
    }

    fn mount_point(&self) -> &Path {
        &self.mount_point
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        println!("Unmounting '{}'...", self.mount_point.display());
        // Killing the process is the primary way to unmount with fuser.
        if let Err(e) = self.child.kill() {
            eprintln!("Failed to kill FUSE process: {}", e);
        }
        // Wait for the process to ensure it has been terminated.
        self.child.wait().expect("Failed to wait on child");

        println!("Unmounted successfully.");
    }
}

#[test]
#[ignore] // This test interacts with real S3 and requires setup. Run with `cargo test -- --ignored`.
fn test_mount_and_list_directory() {
    // 1. SETUP: Get configuration from environment
    let bucket_name = env::var("S3_FUSE_TEST_BUCKET").expect("S3_FUSE_TEST_BUCKET must be set");

    // First, build the project to ensure the binary is up to date.
    let build_status = Command::new("cargo")
        .arg("build")
        .status()
        .expect("Failed to run cargo build");
    assert!(build_status.success(), "Cargo build failed");

    let bin_path = PathBuf::from("target/debug/s3-fuse"); // Assumes your crate is named s3-fuse
    assert!(
        bin_path.exists(),
        "s3-fuse binary not found. Please check your project name and build output."
    );

    // 2. EXECUTION: Mount the filesystem. The guard handles teardown automatically.
    let guard = MountGuard::new(&bin_path, &bucket_name);
    let mount_point = guard.mount_point();

    // 3. ASSERTION: Use standard `std::fs` to interact with the mount.
    println!("Reading directory '{}'...", mount_point.display());
    let entries = fs::read_dir(mount_point)
        .expect("Failed to read mounted directory")
        .map(|res| res.map(|e| e.file_name()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("Found entries: {:?}", entries);

    // Assuming you have a file named 'hello.txt' in the root of your test bucket.
    assert!(
        entries.iter().any(|name| name == "hello.txt"),
        "'hello.txt' not found in the root of the mounted filesystem"
    );

    // Bonus assertion: check the file content.
    let file_content =
        fs::read_to_string(mount_point.join("hello.txt")).expect("Failed to read hello.txt");
    assert_eq!(file_content, "world");
}
