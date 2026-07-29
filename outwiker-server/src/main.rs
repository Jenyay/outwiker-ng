//! OutWikerNG server entry point.
//!
//! Listens on a local IPC endpoint for MessagePack-RPC requests and
//! dispatches them to handlers registered in [`Server`].

mod handlers;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use outwiker_core::transport::{listen, Endpoint};
use outwiker_core::{METHOD_PING, METHOD_SHUTDOWN};

use handlers::{dispatch, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let endpoint = parse_endpoint()?;
    info!("listening on {}", endpoint.path.display());

    let listener = listen(&endpoint).context("failed to bind IPC endpoint")?;

    let server = Arc::new(Server::new());

    loop {
        let transport = match listener.accept().await {
            Ok(t) => t,
            Err(e) => {
                error!("accept failed: {e}");
                continue;
            }
        };

        let server = server.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_connection(transport, server).await {
                error!("connection error: {e:?}");
            }
        });
    }
}

/// Parse the endpoint path from command-line arguments or fall back
/// to a sensible platform default.
fn parse_endpoint() -> anyhow::Result<Endpoint> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(default_endpoint_path);
    Ok(Endpoint::new(path))
}

#[cfg(unix)]
fn default_endpoint_path() -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("outwiker-ng.sock");
    p
}

#[cfg(windows)]
fn default_endpoint_path() -> PathBuf {
    PathBuf::from(r"\\.\pipe\outwiker-ng")
}

/// Run a single client connection to completion.
async fn serve_connection(
    mut transport: Box<dyn outwiker_core::transport::Transport>,
    server: Arc<Server>,
) -> anyhow::Result<()> {
    let mut length_buf = [0u8; 4];

    loop {
        // Length-prefixed framing: first 4 bytes = little-endian u32.
        if transport.read_exact(&mut length_buf).await.is_err() {
            return Ok(()); // client disconnected
        }
        let len = u32::from_le_bytes(length_buf) as usize;
        if len == 0 || len > 16 * 1024 * 1024 {
            anyhow::bail!("invalid frame length {len}");
        }

        let mut frame = vec![0u8; len];
        transport.read_exact(&mut frame).await?;

        let response = match outwiker_core::protocol::decode_msgpack::<
            outwiker_core::protocol::Request,
        >(&frame)
        {
            Ok(request) => {
                if request.method == METHOD_SHUTDOWN {
                    info!("received shutdown request; exiting");
                    let resp = outwiker_core::protocol::Response::ok(request.id, serde_json::json!({}));
                    let bytes = outwiker_core::protocol::encode_msgpack(&resp)?;
                    send_frame(&mut *transport, &bytes).await?;
                    std::process::exit(0);
                }
                if request.method == METHOD_PING {
                    outwiker_core::protocol::Response::ok(request.id, serde_json::json!("pong"))
                } else {
                    dispatch(&server, request)
                }
            }
            Err(e) => {
                error!("decode error: {e}");
                outwiker_core::protocol::Response::err(
                    0,
                    format!("decode error: {e}"),
                )
            }
        };

        let bytes = outwiker_core::protocol::encode_msgpack(&response)?;
        send_frame(&mut *transport, &bytes).await?;
    }
}

async fn send_frame(
    transport: &mut dyn outwiker_core::transport::Transport,
    payload: &[u8],
) -> anyhow::Result<()> {
    let len = (payload.len() as u32).to_le_bytes();
    transport.write_all(&len).await?;
    transport.write_all(payload).await?;
    transport.flush().await?;
    Ok(())
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
