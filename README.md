
## 1. README.md

# Sky-Mount: A FUSE Filesystem for Amazon S3

Sky-Mount is a proof-of-concept filesystem driver, written in Rust, that allows you to mount an Amazon S3 bucket as a local directory on a Unix machine. It leverages the FUSE (Filesystem in Userspace) interface to translate standard filesystem calls (like `ls`, `cat`, `mkdir`) into S3 API operations, providing a seamless bridge between local applications and cloud storage.

### Core Features
*   **Full Directory Support**: Create, delete, and list directories.
*   **POSIX Permissions**: File ownership (`uid`, `gid`) and permissions (`mode`) are stored as S3 object metadata and enforced.
*   **Efficient Reads**: Supports seeked reads to download only the necessary portions of large files.
*   **Atomic Operations**: `rename` and `unlink` operations are designed to be atomic.
*   **Optimized Writes**: In-memory write buffering minimizes latency for applications performing many small writes.

### Design & Trade-offs

Sky-Mount's design prioritizes simplicity and performance for common use cases, but this comes with important trade-offs:

1.  **Write Visibility & Concurrency**: Data written to a file on one node is **only visible to other nodes after that file has been closed**. This is because writes are buffered in memory and uploaded in a single operation on `release`. This design does not provide immediate write-to-read consistency across different machines.
2.  **"Last Writer Wins"**: If two nodes write to the same file simultaneously, the version from the node that closes the file last will overwrite the other's changes.
3.  **Large File Write Memory**: The current implementation buffers entire files in RAM for writing. This is not suitable for files larger than the available system memory. A future improvement would be to use S3 Multipart Upload to stream data from a disk buffer.

---

## **Table of Implemented Functionality**

| Feature | Implemented? | Notes |
| :--- | :---: | :--- |
| **Basic FUSE Capabilities** | ✔️ Yes | Core operations like `lookup`, `getattr`, `open`, `read`, `write`, `create`, `release` are implemented. |
| **Immediate Metadata Visibility** | ✔️ Yes | `ls`, `stat`, `mv` etc. are immediately consistent across all nodes due to the central database. |
| **Immediate Data Visibility** | ❌ No | **Close-to-Open Consistency**: File content written on one node is only visible to others after the file is closed. This does not meet a strict immediate visibility requirement for data. |
| **Directories** | ✔️ Yes | Full support for `mkdir`, `rmdir`, and `readdir`. |
| **Seeked File Reads** | ✔️ Yes | The `read` implementation correctly handles `offset` and `size`, enabling high-throughput reading of large files. |
| **Race-free Writes** | ✔️ Yes | **"Last writer wins" on close policy is implemented**. This prevents file corruption from interleaved partial writes. The final file is always consistent, but one writer's changes will overwrite another's if they write concurrently. |
| **Atomic Operations** | ✔️ Yes | `unlink` and `rename` are atomic at the metadata level and use reliable S3 patterns. |
| **Low Latency (Small Files)** | ✔️ Yes | Metadata lookups and in-memory writes are fast. Reads are subject to S3 network latency. |
| **High Throughput (Large Files)** | ⚠️ Partially | **Read throughput is high** due to seeked reads. **Write throughput is a major bottleneck**, as the entire file must be buffered in RAM before upload. Not suitable for files larger than system RAM. |
| **File-level Permissions** | ✔️ Yes | Full support for POSIX-style permissions. `uid`, `gid`, and `mode` are stored and enforced. |


---

## 3. How to Run the Project (Linux & macOS)

Follow these steps to build and run the Sky-Mount filesystem on your local machine.

### Prerequisites

First, install the necessary development tools for your operating system.

#### On macOS

1.  **Homebrew**: If you don't have it, install the package manager from [brew.sh](https://brew.sh).
2.  **Rust Toolchain**: Install `rustc` and `cargo` via `rustup`.
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
3.  **macFUSE**: Install the FUSE layer for macOS using Homebrew.
    ```bash
    brew install --cask macfuse
    ```
    > **Important**: After installation, macOS will likely require you to approve the system extension from `macFUSE` in **System Settings > Privacy & Security**. You must do this for the filesystem to mount correctly.

4.  **Docker (Optional)**: For running local integration tests, install [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/).

#### On Linux

1.  **Build Tools & Git**: Ensure you have `build-essential` (or equivalent) and `git` installed.
2.  **Rust Toolchain**: Install `rustc` and `cargo` via `rustup`.
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
3.  **FUSE Development Libraries**: You must install the FUSE development headers.
    *   **On Debian/Ubuntu**: `sudo apt-get update && sudo apt-get install -y libfuse3-dev`
    *   **On RHEL/Fedora/CentOS**: `sudo dnf install -y fuse3-devel`
4.  **Docker (Optional)**: For running local integration tests, follow the official Docker installation guide for your distribution.

#### For Both Systems

*   **AWS Credentials**: The application will automatically detect AWS credentials from standard sources (environment variables, `~/.aws/credentials`, or IAM roles). Ensure these are configured if you intend to mount a real S3 bucket.

### Running Integration Tests (Local)

The `docker-compose.yml` file is provided to spin up a local S3-compatible service (like MinIO) for testing without needing a real AWS account.

1.  **Start the Test Environment:**
    ```bash
    docker-compose up -d
    ```
2.  **Run the Tests:**
    ```bash
    cargo test
    ```

### Building the Executable

Build the project in release mode for the best performance. This command is the same for both macOS and Linux.

```bash
cargo build --release
```
The compiled binary will be located at `./target/release/sky-mount`.

### Mounting the Filesystem

1.  **Create a Mount Point:** This is an empty directory on your local machine.
    ```bash
    mkdir ~/s3-drive
    ```

2.  **Run the Mount Command:**
    Use the binary you just built, providing your S3 bucket name and the path to your mount point.
    ```bash
    # Syntax: ./target/release/sky-mount <bucket-name> <mount-point>
    ./target/release/sky-mount my-cloud-storage-bucket ~/s3-drive
    ```

3.  The filesystem is now mounted. You can use any standard command-line tools to interact with it.
    ```bash
    # List contents
    ls -la ~/s3-drive

    # Create a file
    echo "Hello from Sky-Mount!" > ~/s3-drive/hello.txt

    # Create a directory
    mkdir ~/s3-drive/a-new-folder
    ```

### Unmounting the Filesystem

1.  **Graceful Unmount (Recommended)**: Return to the terminal where `sky-mount` is running and press **`Ctrl-C`**. The program will perform a clean unmount.

2.  **Force Unmount**: If the original terminal is not accessible, you can force the unmount using the system's utility. The command is the same on both platforms.
    ```bash
    umount ~/s3-drive
    ```