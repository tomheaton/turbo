use reqwest::header::ToStrError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error making HTTP request: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("skipping HTTP Request, too many failures have occurred.\nLast error: {0}")]
    TooManyFailures(#[from] Box<Error>),
    #[error("Error parsing header: {0}")]
    InvalidHeader(#[from] ToStrError),
    #[error("Error parsing URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
}

pub type Result<T> = std::result::Result<T, Error>;
