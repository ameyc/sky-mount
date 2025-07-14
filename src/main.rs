use anyhow::Result;
use clap::Parser;
use fuser::MountOption;
use fuser::{BackgroundSession, Session}; // Import Session and BackgroundSession
use sky_mount::fs::S3Fuse;
use std::path::PathBuf;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::time::Duration;

/// Mount an S3 bucket as a FUSE filesystem.
///
/// The AWS credentials and region are determined automatically from the environment
/// (e.g., ~/.aws/credentials, IAM instance profiles, or AWS_ACCESS_KEY_ID, etc.).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// The S3 bucket name to mount.
    bucket_name: String,

    /// Postgres connection url
    db_url: String,

    /// The local directory path to mount the filesystem on.
    mount_point: PathBuf,

    /// Mount the filesystem as read-only.
    #[arg(long, default_value_t = false)]
    read_only: bool,
}
// src/main.rs

// ... (CliArgs struct remains the same) ...

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = CliArgs::parse();
    tracing::info!(
        "Mounting bucket '{}' on '{}'",
        args.bucket_name,
        args.mount_point.display()
    );

    let mut options = vec![
        MountOption::AutoUnmount,
        MountOption::FSName("s3fuse".to_string()),
    ];
    if args.read_only {
        options.push(MountOption::RO);
    }

    let s3_filesystem = S3Fuse::new(args.bucket_name, &args.db_url)?;
    let mount_point = args.mount_point.as_path();

    // 1. Create the session and spawn it in the background.
    // This is non-blocking. The FUSE loop now runs in a separate thread.
    let session = Session::new(s3_filesystem, mount_point, &options)?;
    let bg_session = BackgroundSession::new(session)?;

    tracing::info!("Filesystem mounted. Press Ctrl-C to unmount.");

    // 2. Set up a simple channel for the Ctrl+C handler to signal the main thread.
    let (tx, rx) = channel();
    ctrlc::set_handler(move || {
        // Send a simple signal. We don't care about the result.
        let _ = tx.send(());
    })?;

    // 3. The main thread now waits for EITHER the background session to fail
    //    OR for a Ctrl+C signal.
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            // Ctrl+C was received.
            Ok(()) => {
                tracing::info!("Received Ctrl-C. Initiating shutdown...");
                break; // Exit the loop to begin shutdown.
            }
            // Timeout expired, which is our normal "heartbeat".
            // We check if the background session is still alive.
            Err(RecvTimeoutError::Timeout) => {}
            // The channel was disconnected. This happens if the sender is dropped.
            Err(RecvTimeoutError::Disconnected) => {
                tracing::error!("Signal handler channel disconnected unexpectedly.");
                break;
            }
        }
    }

    // 4. This is the crucial part: Explicitly unmount.
    // `bg_session` is consumed here, which calls its `Drop` implementation,
    // which in turn unmounts the filesystem and joins the background thread.
    tracing::info!("Unmounting filesystem...");
    // Let the BackgroundSession's Drop handler do the cleanup.
    // The `exit()` method is called internally by Drop.
    drop(bg_session);

    tracing::info!("Process finished.");
    Ok(())
}
