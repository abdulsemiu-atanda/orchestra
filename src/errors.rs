//! The error type every fallible operation in Orchestra reports through.
//!
//! AWS operation errors arrive as `SdkError<E, R>`, whose type parameters differ
//! for every S3 call. Rather than growing a variant per operation, `SdkError`
//! boxes the underlying error behind `std::error::Error`, and the blanket `From`
//! impl below lets `?` lift any client call into an `OrchestraError`.

use aws_sdk_s3::{error::SdkError, primitives::ByteStreamError};
use std::{fmt, io};
use tokio::task::JoinError;

#[derive(Debug)]
pub enum OrchestraError {
  ByteStreamError(ByteStreamError),
  FileError(io::Error),
  SdkError(Box<dyn std::error::Error + Send + Sync>),
  TaskError(JoinError),
}

impl fmt::Display for OrchestraError {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::ByteStreamError(error) => write!(formatter, "Reading response body failed: {error}"),
      Self::FileError(error) => write!(formatter, "File operation failed: {error}"),
      Self::SdkError(error) => write!(formatter, "AWS request failed: {error}"),
      Self::TaskError(error) => write!(formatter, "Download task did not finish: {error}"),
    }
  }
}

impl std::error::Error for OrchestraError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      Self::ByteStreamError(error) => Some(error),
      Self::FileError(error) => Some(error),
      Self::SdkError(error) => Some(error.as_ref()),
      Self::TaskError(error) => Some(error),
    }
  }
}

impl From<ByteStreamError> for OrchestraError {
  fn from(error: ByteStreamError) -> Self {
    Self::ByteStreamError(error)
  }
}

impl From<io::Error> for OrchestraError {
  fn from(error: io::Error) -> Self {
    Self::FileError(error)
  }
}

impl From<JoinError> for OrchestraError {
  fn from(error: JoinError) -> Self {
    Self::TaskError(error)
  }
}

impl<E, R> From<SdkError<E, R>> for OrchestraError
where
  E: std::error::Error + Send + Sync + 'static,
  R: fmt::Debug + Send + Sync + 'static,
{
  fn from(error: SdkError<E, R>) -> Self {
    Self::SdkError(Box::new(error))
  }
}
