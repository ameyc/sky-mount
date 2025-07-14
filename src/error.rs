use aws_sdk_s3::error::{BuildError, SdkError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 API error: {0}")]
    S3(String),

    #[error("Operation is not supported")]
    NotSupported,

    #[error("Entity not found")]
    NotFound,
    // ... add other specific errors like AccessDenied as needed
}

// A helper to convert generic S3 SDK errors into our FsError::S3 type
impl<E> From<SdkError<E>> for FsError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: SdkError<E>) -> Self {
        FsError::S3(err.to_string())
    }
}

// Convert from generic Builder errors from AWS SDK
impl From<BuildError> for FsError {
    fn from(err: BuildError) -> Self {
        // It's a client-side build error, but wrapping it as an S3 error is reasonable.
        FsError::S3(format!("S3 request builder error: {}", err))
    }
}
