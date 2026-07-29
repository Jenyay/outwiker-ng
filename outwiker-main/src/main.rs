//! OutWikerNG client entry point.
//!
//! Connects to the server over the local IPC transport, sends
//! MessagePack-RPC requests, and prints the responses. A simple
//! REPL is provided for interactive use.

use std::path::PathBuf;

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use outwiker_core::protocol::{decode_msgpack, encode_msgpack, Request, Response};
use outwiker_core::transport::{connect, Endpoint};
use outwiker_core::{CoreResult, METHOD_ECHO, METHOD_PING};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let mut args = std::env::args().skip(1);
    let endpoint_path = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(default_endpoint_path);

    // If the user passed a method + JSON arguments, perform one
    // RPC call and exit. Otherwise, drop into the REPL.
    let method = args.next();
    let params: Vec<serde_json::Value> = match args
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
    {
        s if s.is_empty() => Vec::new(),
        s => match serde_json::from_str::<serde_json::Value>(&s) {
            Ok(serde_json::Value::Array(arr)) => arr,
            Ok(other) => vec![other],
            Err(e) => {
                warn!("could not parse args as JSON ({e}); sending empty params");
                Vec::new()
            }
        },
    };

    let endpoint = Endpoint::new(endpoint_path);
    let mut transport = connect(&endpoint)
        .await
        .context("failed to connect to server")?;
    info!("connected to {}", endpoint.path.display());

    match method {
        Some(name) => {
            let resp = call(&mut *transport, name, params).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
        }
        None => repl(&mut *transport).await?,
    }

    Ok(())
}

/// Default IPC endpoint path - mirrors the server default.
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

/// Issue a single RPC call and wait for the response.
async fn call(
    transport: &mut dyn outwiker_core::transport::Transport,
    method: impl Into<String>,
    params: Vec<serde_json::Value>,
) -> anyhow::Result<Response> {
    let request = Request {
        id: next_id(),
        method: method.into(),
        params,
    };
    let payload = encode_msgpack(&request)?;
    let len = (payload.len() as u32).to_le_bytes();
    transport.write_all(&len).await?;
    transport.write_all(&payload).await?;
    transport.flush().await?;

    let mut len_buf = [0u8; 4];
    transport.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    transport.read_exact(&mut frame).await?;

    let response: Response = decode_msgpack(&frame)?;
    Ok(response)
}

fn next_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Minimal interactive shell.
///
/// Supported commands:
///   - `ping`            -> send `ping`
///   - `echo <json>`     -> send `echo` with the supplied JSON args
///   - `call <m> <json>` -> send any method with JSON args
///   - `quit` / `exit`   -> leave the REPL
async fn repl(transport: &mut dyn outwiker_core::transport::Transport) -> anyhow::Result<()> {
    use std::io::Write;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    loop {
        print!("outwiker> ");
        stdout.flush()?;

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.splitn(2, char::is_whitespace);
        let cmd = parts.next().unwrap_or("").to_lowercase();
        let rest = parts.next().unwrap_or("").trim();

        match cmd.as_str() {
            "quit" | "exit" => return Ok(()),
            "ping" => {
                let resp: CoreResult<()> =
                    call(transport, METHOD_PING, Vec::new()).await?.into_result().map(|_| ());
                match resp {
                    Ok(_) => println!("pong"),
                    Err(e) => error!("error: {e}"),
                }
            }
            "echo" => {
                let params = parse_args(rest);
                match call(transport, METHOD_ECHO, params).await {
                    Ok(resp) => println!("{}", serde_json::to_string(&resp.result)?),
                    Err(e) => error!("error: {e:?}"),
                }
            }
            "call" => {
                let mut split = rest.splitn(2, char::is_whitespace);
                let method = split.next().unwrap_or("").to_string();
                if method.is_empty() {
                    error!("usage: call <method> [json-args]");
                    continue;
                }
                let params = parse_args(split.next().unwrap_or(""));
                match call(transport, method, params).await {
                    Ok(resp) => println!("{}", serde_json::to_string(&resp.result)?),
                    Err(e) => error!("error: {e:?}"),
                }
            }
            "help" | "?" => {
                println!("commands:");
                println!("  ping");
                println!("  echo <json>");
                println!("  call <method> <json>");
                println!("  quit");
            }
            other => {
                error!("unknown command: {other} (try `help`)");
            }
        }
    }
}

fn parse_args(s: &str) -> Vec<serde_json::Value> {
    if s.is_empty() {
        return Vec::new();
    }
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Array(arr)) => arr,
        Ok(other) => vec![other],
        Err(_) => vec![serde_json::Value::String(s.to_string())],
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
