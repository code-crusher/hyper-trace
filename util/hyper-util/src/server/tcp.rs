//! TCP listeners and streams for serving HTTP.

use std::fmt;
use std::future::Future;
use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

pin_project_lite::pin_project! {
    /// A stream of connections yielded by an [`Incoming`], carrying the remote
    /// and local addresses alongside the underlying [`TcpStream`].
    #[derive(Debug)]
    pub struct AddrStream {
        #[pin]
        inner: TcpStream,
        remote_addr: SocketAddr,
        local_addr: SocketAddr,
    }
}

impl AddrStream {
    pub(crate) fn new(tcp: TcpStream, remote_addr: SocketAddr, local_addr: SocketAddr) -> Self {
        Self {
            inner: tcp,
            remote_addr,
            local_addr,
        }
    }

    /// Get the remote (peer) address of this connection.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Get the local address of this connection.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl hyper::rt::Read for AddrStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        use tokio::io::AsyncRead;

        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            match AsyncRead::poll_read(self.project().inner, cx, &mut tbuf) {
                Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for AddrStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        use tokio::io::AsyncWrite;

        AsyncWrite::poll_write(self.project().inner, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use tokio::io::AsyncWrite;

        AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        use tokio::io::AsyncWrite;

        AsyncWrite::poll_shutdown(self.project().inner, cx)
    }
}

/// A stream of connections from a TCP listener, for use with [`Server`].
///
/// [`Server`]: super::Server
#[must_use = "streams do nothing unless polled"]
pub struct Incoming {
    addr: SocketAddr,
    listener: Arc<TcpListener>,
    sleep_on_errors: bool,
    tcp_nodelay: bool,
    timeout: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Incoming {
    /// Creates a new `Incoming` binding to the provided socket address.
    pub fn bind(addr: &SocketAddr) -> io::Result<Self> {
        let std_listener = StdTcpListener::bind(addr)?;
        Incoming::from_std(std_listener)
    }

    /// Creates a new `Incoming` from an existing `std::net::TcpListener`.
    ///
    /// This is most useful for passing in a listener that was inherited from
    /// another process, such as with systemd socket activation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::net::TcpListener;
    /// use hyper_util::server::Incoming;
    /// # fn main() -> std::io::Result<()> {
    /// let std_listener = TcpListener::bind("127.0.0.1:0")?;
    /// let incoming = Incoming::from_std(std_listener)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_std(std_listener: StdTcpListener) -> io::Result<Self> {
        // TcpListener::from_std requires the listener to be in non-blocking
        // mode, but does not set O_NONBLOCK itself.
        std_listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(std_listener)?;
        Incoming::from_listener(listener)
    }

    /// Creates a new `Incoming` from an existing `tokio::net::TcpListener`.
    pub fn from_listener(listener: TcpListener) -> io::Result<Self> {
        let addr = listener.local_addr()?;
        Ok(Incoming {
            addr,
            listener: Arc::new(listener),
            sleep_on_errors: true,
            tcp_nodelay: false,
            timeout: None,
        })
    }

    /// Create a new handle to this `Incoming` that shares the same listener.
    pub fn share(&self) -> Self {
        Self {
            listener: self.listener.clone(),
            addr: self.addr,
            sleep_on_errors: self.sleep_on_errors,
            tcp_nodelay: self.tcp_nodelay,
            timeout: None,
        }
    }

    /// Get the local address bound to this listener.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Set the value of `TCP_NODELAY` option for accepted connections.
    pub fn set_nodelay(&mut self, enabled: bool) -> &mut Self {
        self.tcp_nodelay = enabled;
        self
    }

    /// Set whether to sleep on accept errors.
    ///
    /// A possible scenario is that the process has hit the max open files
    /// allowed, and so trying to accept a new connection will fail with
    /// `EMFILE`. In some cases, it's preferable to just wait for some time, if
    /// the application will likely close some files (or connections), and try
    /// to accept the connection again. If this option is `true`, the error
    /// will be logged at the `error` level, since it is still a big deal,
    /// and then the listener will sleep for 1 second.
    ///
    /// In other cases, hitting the max open files should be treated similarly
    /// to being out-of-memory, and simply error (and shutdown). Setting
    /// this option to `false` will allow that.
    ///
    /// Default is `true`.
    pub fn set_sleep_on_errors(&mut self, val: bool) {
        self.sleep_on_errors = val;
    }

    fn poll_next_(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<AddrStream>> {
        // Check if a previous timeout is active that was set by IO errors.
        if let Some(ref mut to) = self.timeout {
            ready!(Pin::new(to).poll(cx));
        }
        self.timeout = None;

        loop {
            match ready!(self.listener.poll_accept(cx)) {
                Ok((socket, remote_addr)) => {
                    if self.tcp_nodelay {
                        if let Err(e) = socket.set_nodelay(true) {
                            tracing::trace!("error trying to set TCP nodelay: {}", e);
                        }
                    }
                    let local_addr = socket.local_addr()?;
                    return Poll::Ready(Ok(AddrStream::new(socket, remote_addr, local_addr)));
                }
                Err(e) => {
                    // Connection errors can be ignored directly, continue by
                    // accepting the next request.
                    if is_connection_error(&e) {
                        tracing::debug!("accepted connection already errored: {}", e);
                        continue;
                    }

                    if self.sleep_on_errors {
                        tracing::error!("accept error: {}", e);

                        // Sleep 1s.
                        let mut timeout = Box::pin(tokio::time::sleep(Duration::from_secs(1)));

                        match timeout.as_mut().poll(cx) {
                            Poll::Ready(()) => {
                                // Wow, it's been a second already? Ok then...
                                continue;
                            }
                            Poll::Pending => {
                                self.timeout = Some(timeout);
                                return Poll::Pending;
                            }
                        }
                    } else {
                        return Poll::Ready(Err(e));
                    }
                }
            }
        }
    }
}

impl futures_core::Stream for Incoming {
    type Item = io::Result<AddrStream>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = ready!(self.poll_next_(cx));
        Poll::Ready(Some(result))
    }
}

impl fmt::Debug for Incoming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("addr", &self.addr)
            .field("sleep_on_errors", &self.sleep_on_errors)
            .field("tcp_nodelay", &self.tcp_nodelay)
            .finish()
    }
}

/// This function defines errors that are per-connection. Which basically
/// means that if we get this error from `accept()` system call it means
/// next connection might be ready to be accepted.
///
/// All other errors will incur a timeout before next `accept()` is performed.
/// The timeout is useful to handle resource exhaustion errors like ENFILE
/// and EMFILE. Otherwise, could enter into tight loop.
fn is_connection_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}
