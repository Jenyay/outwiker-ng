//! MessagePack-RPC protocol definitions for OutWikerNG.
//!
//! Requests and responses are serialized using MessagePack via
//! `rmp-serde` and exchanged over the transport defined in
//! [`crate::transport`].

use serde::{Deserialize, Serialize};

use crate::error::{CoreError, CoreResult};

/// Built-in RPC method names.
pub const METHOD_PING: &str = "ping";
pub const METHOD_ECHO: &str = "echo";
pub const METHOD_SHUTDOWN: &str = "shutdown";

/// A single RPC request sent from a client to the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// Numeric identifier of the call. The client must pair this
    /// with the response's `id`.
    pub id: u64,
    /// Name of the remote method to invoke.
    pub method: String,
    /// Positional arguments passed to the method.
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// Response sent from the server back to the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Identifier matching the request that produced this response.
    pub id: u64,
    /// Result of a successful call. Mutually exclusive with `error`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Error message for a failed call. Mutually exclusive with `result`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// Create a successful response wrapping `result`.
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Create an error response with the given message.
    pub fn err(id: u64, message: impl Into<String>) -> Self {
        Self {
            id,
            result: None,
            error: Some(message.into()),
        }
    }

    /// Convert the response into a [`CoreResult`].
    pub fn into_result(self) -> CoreResult<serde_json::Value> {
        match self.error {
            Some(msg) => Err(CoreError::Rpc(msg)),
            None => self
                .result
                .ok_or_else(|| CoreError::InvalidRequest("response missing both result and error".into())),
        }
    }
}

/// Encode a type as a MessagePack byte vector.
pub fn encode_msgpack<T: Serialize>(value: &T) -> CoreResult<Vec<u8>> {
    Ok(rmp_serde::to_vec_named(value)?)
}

/// Decode a MessagePack byte slice into the given type.
pub fn decode_msgpack<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> CoreResult<T> {
    Ok(rmp_serde::from_slice(bytes)?)
}
