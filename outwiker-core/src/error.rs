//! Error types for OutWikerNG core.

use thiserror::Error;

/// Core error type used across the OutWikerNG crates.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("MessagePack serialization error: {0}")]
    MsgPack(#[from] rmp_serde::encode::Error),

    #[error("MessagePack deserialization error: {0}")]
    MsgPackDecode(#[from] rmp_serde::decode::Error),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("interprocess error: {0}")]
    Interprocess(String),

    #[error("unknown method: {0}")]
    UnknownMethod(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

/// Convenient result alias for the core library.
pub type CoreResult<T> = Result<T, CoreError>;
