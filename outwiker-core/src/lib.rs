//! OutWikerNG core library
//!
//! Shared types, error definitions, and MessagePack-RPC protocol
//! primitives used by both the client (`outwiker-main`) and the
//! server (`outwiker-server`).

pub mod error;
pub mod protocol;
pub mod transport;

pub use error::{CoreError, CoreResult};
pub use protocol::{Request, Response, METHOD_ECHO, METHOD_PING, METHOD_SHUTDOWN};
pub use transport::{Endpoint, Listener, Transport, TransportKind};
