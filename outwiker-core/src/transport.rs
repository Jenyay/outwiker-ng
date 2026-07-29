//! Transport abstraction used to exchange MessagePack frames between
//! the client and the server.
//!
//! The actual link is provided by the `interprocess` crate, which
//! supports named pipes (Windows) and unix domain sockets (Unix) for
//! local IPC.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{CoreError, CoreResult};

/// Identifies the IPC backend used for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// A platform-appropriate local IPC endpoint (unix domain socket
    /// on Unix, named pipe on Windows).
    LocalIpc,
}

impl Default for TransportKind {
    fn default() -> Self {
        Self::LocalIpc
    }
}

/// Address passed to the transport to open a connection.
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// Filesystem path used as the IPC endpoint name.
    pub path: PathBuf,
    /// Which backend the address refers to.
    pub kind: TransportKind,
}

impl Endpoint {
    /// Create a new local IPC endpoint with the given path.
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            kind: TransportKind::LocalIpc,
        }
    }
}

/// A bidirectional, asynchronously-readable byte stream.
pub trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> Transport for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Server-side listener handle.
///
/// The underlying OS handle is bound at construction time and lives
/// for the lifetime of this value, so accept loops can be written
/// in straight-line async code without global state.
pub struct Listener {
    inner: ListenerImpl,
}

enum ListenerImpl {
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
    #[cfg(windows)]
    Pipe(interprocess::os::windows::named_pipe::PipeListener<
        interprocess::os::windows::named_pipe::pipe_mode::Bytes,
    >),
}

/// Open a client-side connection to the given endpoint.
pub async fn connect(endpoint: &Endpoint) -> CoreResult<Box<dyn Transport>> {
    match endpoint.kind {
        TransportKind::LocalIpc => connect_local(&endpoint.path).await,
    }
}

/// Bind a server-side listener to the given endpoint.
pub fn listen(endpoint: &Endpoint) -> CoreResult<Listener> {
    match endpoint.kind {
        TransportKind::LocalIpc => listen_local(&endpoint.path),
    }
}

impl Listener {
    /// Accept a single incoming client connection.
    pub async fn accept(&self) -> CoreResult<Box<dyn Transport>> {
        accept_local(self).await
    }
}

// --- Unix implementation --------------------------------------------

#[cfg(unix)]
fn listen_local(path: &Path) -> CoreResult<Listener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Remove a stale socket file from a previous run, if any.
    let _ = std::fs::remove_file(path);
    let listener = tokio::net::UnixListener::bind(path)
        .map_err(|e| CoreError::Interprocess(format!("bind: {e}")))?;
    Ok(Listener { inner: ListenerImpl::Unix(listener) })
}

#[cfg(unix)]
async fn connect_local(path: &Path) -> CoreResult<Box<dyn Transport>> {
    let stream = tokio::net::UnixStream::connect(path)
        .await
        .map_err(|e| CoreError::Interprocess(format!("connect: {e}")))?;
    Ok(Box::new(stream))
}

#[cfg(unix)]
async fn accept_local(listener: &Listener) -> CoreResult<Box<dyn Transport>> {
    let ListenerImpl::Unix(l) = &listener.inner;
    let (stream, _addr) = l
        .accept()
        .await
        .map_err(|e| CoreError::Interprocess(format!("accept: {e}")))?;
    Ok(Box::new(stream))
}

// --- Windows implementation -----------------------------------------

#[cfg(windows)]
fn listen_local(path: &Path) -> CoreResult<Listener> {
    use interprocess::os::windows::named_pipe::{pipe_mode::Bytes, PipeListener};
    let listener = PipeListener::bind(path)
        .map_err(|e| CoreError::Interprocess(format!("bind: {e}")))?;
    Ok(Listener { inner: ListenerImpl::Pipe(listener) })
}

#[cfg(windows)]
async fn connect_local(path: &Path) -> CoreResult<Box<dyn Transport>> {
    use interprocess::os::windows::named_pipe::{pipe_mode::Bytes, Stream};
    let stream = Stream::<Bytes>::connect(path)
        .map_err(|e| CoreError::Interprocess(format!("connect: {e}")))?;
    Ok(Box::new(PipeStream::new(stream)))
}

#[cfg(windows)]
async fn accept_local(listener: &Listener) -> CoreResult<Box<dyn Transport>> {
    use interprocess::os::windows::named_pipe::pipe_mode::Bytes;
    let ListenerImpl::Pipe(l) = &listener.inner;
    let stream = l
        .accept()
        .await
        .map_err(|e| CoreError::Interprocess(format!("accept: {e}")))?;
    Ok(Box::new(PipeStream::new(stream)))
}

#[cfg(windows)]
struct PipeStream {
    inner: interprocess::os::windows::named_pipe::Stream<
        interprocess::os::windows::named_pipe::pipe_mode::Bytes,
    >,
}

#[cfg(windows)]
impl PipeStream {
    fn new(
        inner: interprocess::os::windows::named_pipe::Stream<
            interprocess::os::windows::named_pipe::pipe_mode::Bytes,
        >,
    ) -> Self {
        Self { inner }
    }
}

#[cfg(windows)]
impl tokio::io::AsyncRead for PipeStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::io::Read;
        // Block-on-read on the worker thread. A production version
        // would integrate overlapped I/O via `tokio::task::spawn_blocking`
        // or a dedicated reactor registration; this skeleton is enough
        // to validate the protocol.
        let slice = buf.initialize_unfilled();
        match self.inner.read(slice) {
            Ok(0) => std::task::Poll::Ready(Ok(())),
            Ok(n) => {
                buf.advance(n);
                std::task::Poll::Ready(Ok(()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            Err(e) => std::task::Poll::Ready(Err(e)),
        }
    }
}

#[cfg(windows)]
impl tokio::io::AsyncWrite for PipeStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use std::io::Write;
        std::task::Poll::Ready(self.inner.write(buf))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::io::Write;
        std::task::Poll::Ready(self.inner.flush())
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::io::Write;
        std::task::Poll::Ready(self.inner.flush())
    }
}

#[allow(dead_code)]
fn _force_link(_: &dyn Transport) {}
