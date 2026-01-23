use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgStageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid dump format: {0}")]
    InvalidFormat(String),

    #[error("Unsupported format version: {0}")]
    UnsupportedVersion(String),

    #[error("Unknown mutation: {0}")]
    UnknownMutation(String),

    #[error("Mutation error: {0}")]
    MutationError(String),

    #[error("Unique value generation failed after {0} attempts")]
    UniqueExhausted(u32),

    #[error("Missing required parameter '{0}' for mutation '{1}'")]
    MissingParameter(String, String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Compression error: {0}")]
    CompressionError(String),

    #[error("UTF-8 decode error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
}

pub type Result<T> = std::result::Result<T, PgStageError>;
