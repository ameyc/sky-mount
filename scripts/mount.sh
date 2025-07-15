#!/bin/bash
#
# Mounts the Sky-Mount S3 Filesystem
#
# This script automates the process of:
# 1. Parsing arguments for bucket, mount point, and optional AWS credentials.
# 2. Checking for dependencies (docker, cargo).
# 3. Ensuring the PostgreSQL metadata database is running via Docker Compose.
# 4. Building the Rust project in release mode.
# 5. Mounting the filesystem to a specified directory.
#

# --- Configuration ---
# The name of the postgres service defined in your docker-compose.yml
POSTGRES_SERVICE_NAME="postgres"
# The name of the binary produced by `cargo build`
BINARY_NAME="sky-mount"

# !! IMPORTANT !!
# These variables MUST match the environment variables in your docker-compose.yml
DB_USER="fsuser"
DB_PASSWORD="secret"
DB_NAME="fsmeta"
DB_HOST="localhost"
DB_PORT="5432"

# The final connection string passed to the application
DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# --- Script Safeties ---
# Exit immediately if a command exits with a non-zero status.
set -e

# --- Pretty Output Functions ---
COLOR_RESET='\033[0m'
COLOR_GREEN='\033[0;32m'
COLOR_BLUE='\033[0;34m'
COLOR_YELLOW='\033[0;33m'
COLOR_RED='\033[0;31m'
BOLD='\033[1m'

info() { echo -e "${COLOR_BLUE}${BOLD}INFO:${COLOR_RESET} $1"; }
success() { echo -e "${COLOR_GREEN}${BOLD}SUCCESS:${COLOR_RESET} $1"; }
warn() { echo -e "${COLOR_YELLOW}${BOLD}WARNING:${COLOR_RESET} $1"; }
error() { echo -e "${COLOR_RED}${BOLD}ERROR:${COLOR_RESET} $1" >&2; exit 1; }
header() { echo -e "\n${BOLD}--- $1 ---${COLOR_RESET}"; }

# --- Usage Function ---
usage() {
  echo -e "${BOLD}Sky-Mount Automounter${COLOR_RESET}"
  echo ""
  echo "Usage: $0 -b <bucket> -m <mount_point> [options]"
  echo ""
  echo "${BOLD}Required Arguments:${COLOR_RESET}"
  echo "  -b <bucket_name>    The name of the S3 bucket to mount."
  echo "  -m <mount_point>    The local directory path to mount the filesystem to."
  echo ""
  echo "${BOLD}Optional Arguments:${COLOR_RESET}"
  echo "  -a <access_key_id>  Your AWS Access Key ID."
  echo "  -s <secret_key>     Your AWS Secret Access Key."
  echo "  -r <region>         The AWS region of the bucket (e.g., us-east-1)."
  echo "  -h                  Display this help message."
  echo ""
  echo "If credentials are not provided, the application will use the default AWS chain"
  echo "(e.g., ~/.aws/credentials, IAM instance roles)."
  exit 1
}

# --- Main Logic ---

# 1. Argument Parsing with getopts
S3_BUCKET=""
MOUNT_POINT=""
AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""
AWS_REGION_VAL=""

while getopts "b:m:a:s:r:h" opt; do
  case $opt in
    b) S3_BUCKET="$OPTARG" ;;
    m) MOUNT_POINT="$OPTARG" ;;
    a) AWS_ACCESS_KEY="$OPTARG" ;;
    s) AWS_SECRET_KEY="$OPTARG" ;;
    r) AWS_REGION_VAL="$OPTARG" ;;
    h) usage ;;
    \?) usage ;;
  esac
done

# Check if required arguments were provided
if [[ -z "$S3_BUCKET" || -z "$MOUNT_POINT" ]]; then
  error "Bucket name (-b) and mount point (-m) are required."
  usage
fi

header "1. Checking Configuration"

# Set AWS credentials if they were passed as arguments
if [[ -n "$AWS_ACCESS_KEY" && -n "$AWS_SECRET_KEY" ]]; then
  info "Using AWS credentials provided via arguments."
  export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"
else
  info "Using default AWS credentials from environment (e.g., ~/.aws/credentials or IAM role)."
fi

# Set AWS region if it was passed as an argument
if [[ -n "$AWS_REGION_VAL" ]]; then
  info "Using AWS Region '${AWS_REGION_VAL}' provided via argument."
  export AWS_REGION="$AWS_REGION_VAL"
fi

header "2. Checking Prerequisites"

# Check for essential commands
for cmd in docker docker cargo; do
  if ! command -v "$cmd" &> /dev/null; then
    error "'$cmd' command not found. Please install it and ensure it is in your PATH."
  fi
done
info "All dependencies appear to be installed."


header "3. Ensuring Database is Running"
info "Using database connection string: postgresql://${DB_USER}:****@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Check if the docker-compose project is running the postgres service
if ! docker compose ps -q "$POSTGRES_SERVICE_NAME" | grep -q .; then
  info "PostgreSQL container is not running. Starting via 'docker-compose up'..."
  warn "Please ensure your docker-compose.yml sets POSTGRES_USER=${DB_USER}, POSTGRES_PASSWORD=${DB_PASSWORD}, and POSTGRES_DB=${DB_NAME}"
  docker compose up -d
  info "Waiting for database to become available..."
  sleep 5
  success "Database container is now running."
else
  info "PostgreSQL container is already running."
fi


header "4. Building Sky-Mount"

info "Building project in release mode... (This may take a moment)"
if ! cargo build --release --quiet; then
    error "Cargo build failed. Please check the compilation errors."
fi
BINARY_PATH="./target/release/${BINARY_NAME}"
success "Project built successfully. Binary at ${BINARY_PATH}"


header "5. Mounting Filesystem"

# Create the mount point directory if it doesn't exist.
info "Ensuring mount point directory exists at '${MOUNT_POINT}'..."
if ! mkdir -p "$MOUNT_POINT"; then
  error "Failed to create mount point directory at '${MOUNT_POINT}'."
fi

# Check if mount point is empty
if [ -n "$(ls -A "$MOUNT_POINT")" ]; then
    warn "Mount point '${MOUNT_POINT}' is not empty."
fi

info "Mounting bucket '${S3_BUCKET}' to '${MOUNT_POINT}'..."
info "The filesystem will run in this terminal. Press Ctrl-C to unmount."

# Execute the mount command.
export DATABASE_URL="${DATABASE_URL}"
"${BINARY_PATH}" "${S3_BUCKET}" "${DATABASE_URL}" "${MOUNT_POINT}"

# This part will only be reached after the user presses Ctrl-C
echo ""
success "Filesystem unmounted gracefully."
