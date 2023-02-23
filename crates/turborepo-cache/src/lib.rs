#![feature(error_generic_member_access)]
#![feature(provide_any)]

pub mod http;
pub mod signature_authentication;
use std::backtrace::Backtrace;

use thiserror::Error;

use crate::signature_authentication::SignatureError;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Timestamp is invalid {0}")]
    Timestamp(#[from] std::num::TryFromIntError),
    #[error(
        "artifact verification failed: Downloaded artifact is missing required x-artifact-tag \
         header"
    )]
    ArtifactTagMissing,
    #[error("invalid artifact verification tag")]
    InvalidTag(#[backtrace] Backtrace),
    #[error("cannot untar file to {0}")]
    InvalidFilePath(String),
    #[error("artifact verification failed: {0}")]
    ApiClientError(#[from] turborepo_api_client::Error),
    #[error("signing artifact failed: {0}")]
    SignatureError(#[from] SignatureError),
    #[error("invalid duration")]
    InvalidDuration(#[backtrace] Backtrace),
    #[error("Invalid file path: {0}")]
    PathValidationError(#[from] turbopath::PathValidationError),
    #[error("Invalid file path, link target does not exist: {0}")]
    LinkTargetDoesNotExist(String),
    #[error("Invalid symlink, link name does not exist")]
    LinkNameDoesNotExist,
}
