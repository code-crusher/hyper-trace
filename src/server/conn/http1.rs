//! HTTP/1 Server Connections.

use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::rt::{Read, Write};
use crate::upgrade::Upgraded;
use bytes::{Bytes, BytesMut};
use futures_core::ready;
use httparse::ParserConfig;

use crate::body::{Body, Incoming as IncomingBody};
use crate::proto;
use crate::service::HttpService;
use crate::{
    common::time::{Dur, Time},
    rt::Timer,
};

type Http1Dispatcher<T, B, S> = proto::h1::Dispatcher<
    proto::h1::dispatch::Server<S, IncomingBody>,
    B,
    T,
    proto::ServerTransaction,
>;

pin_project_lite::pin_project! {
    /// A [`Future`](core::future::Future) representing an HTTP/1 connection, bound to a
    /// [`Service`](crate::service::Service), returned from
    /// [`Builder::serve_connection`](struct.Builder.html#method.serve_connection).
    ///
    /// To drive HTTP on this connection this future **must be polled**, typically with
    /// `.await`. If it isn't polled, no progress will be made on this connection.
    #[must_use = "futures do nothing unless polled"]
    pub struct Connection<T, S>
    where
        S: HttpService<IncomingBody>,
    {
        conn: Http1Dispatcher<T, S::ResBody, S>,
    }
}

/// A provider of buffers for an HTTP/1 connection.
///
/// By default, a connection allocates and keeps its read and write buffers for
/// as long as the connection lives, even while it sits idle between requests.
/// With a provider, the connection borrows buffers while a message is being
/// read or written, and gives them back when it becomes idle. This lets an
/// application pool and reuse buffers across connections, so that idle
/// keep-alive connections hold no buffers at all.
///
/// Implementations are called from the connection task, and so should be
/// cheap. A simple pool can be built with a `Mutex<Vec<_>>` of buffers.
///
/// # Example
///
/// ```
/// # use bytes::BytesMut;
/// # use hyper::server::conn::http1::BufProvider;
/// # use std::sync::{Arc, Mutex};
/// #[derive(Default)]
/// struct Pool {
///     read: Mutex<Vec<BytesMut>>,
///     write: Mutex<Vec<Vec<u8>>>,
/// }
///
/// impl BufProvider for Pool {
///     fn take_read(&self, min: usize) -> BytesMut {
///         let mut buf = self.read.lock().unwrap().pop().unwrap_or_default();
///         buf.reserve(min);
///         buf
///     }
///     fn give_read(&self, buf: BytesMut) {
///         self.read.lock().unwrap().push(buf);
///     }
///     fn take_write(&self) -> Vec<u8> {
///         self.write.lock().unwrap().pop().unwrap_or_default()
///     }
///     fn give_write(&self, buf: Vec<u8>) {
///         self.write.lock().unwrap().push(buf);
///     }
/// }
/// ```
///
/// Then set it on the [`Builder`]:
///
/// ```
/// # use hyper::server::conn::http1::{Builder, BufProvider};
/// # use std::sync::Arc;
/// # #[derive(Default)]
/// # struct Pool;
/// # impl BufProvider for Pool {
/// #     fn take_read(&self, _min: usize) -> bytes::BytesMut { unimplemented!() }
/// #     fn give_read(&self, _buf: bytes::BytesMut) {}
/// #     fn take_write(&self) -> Vec<u8> { unimplemented!() }
/// #     fn give_write(&self, _buf: Vec<u8>) {}
/// # }
/// let mut http = Builder::new();
/// http.buffer_provider(Arc::new(Pool::default()));
/// ```
pub trait BufProvider: Send + Sync + 'static {
    /// Take a buffer to read a message into.
    ///
    /// The buffer should have room for at least `min` bytes if possible. The
    /// connection always uses the returned buffer as-is; a buffer smaller
    /// than `min` is grown as needed, but reusing a large enough buffer avoids
    /// that allocation.
    fn take_read(&self, min: usize) -> BytesMut;

    /// Give back a read buffer that the connection no longer needs.
    ///
    /// The buffer is always empty.
    fn give_read(&self, buf: BytesMut);

    /// Take a buffer to encode a message head into.
    ///
    /// With the flatten write strategy, this buffer also holds message body
    /// bytes.
    fn take_write(&self) -> Vec<u8>;

    /// Give back a write buffer whose bytes have all been written.
    ///
    /// The buffer is always empty.
    fn give_write(&self, buf: Vec<u8>);
}

/// A configuration builder for HTTP/1 server connections.
///
/// **Note**: The default values of options are *not considered stable*. They
/// are subject to change at any time.
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use hyper::server::conn::http1::Builder;
/// # fn main() {
/// let mut http = Builder::new();
/// // Set options one at a time
/// http.half_close(false);
///
/// // Or, chain multiple options
/// http.keep_alive(false).title_case_headers(true).max_buf_size(8192);
///
/// # }
/// ```
///
/// Use [`Builder::serve_connection`](struct.Builder.html#method.serve_connection)
/// to bind the built connection to a service.
#[derive(Clone)]
pub struct Builder {
    h1_parser_config: ParserConfig,
    timer: Time,
    h1_half_close: bool,
    h1_keep_alive: bool,
    h1_title_case_headers: bool,
    h1_preserve_header_case: bool,
    h1_max_headers: Option<usize>,
    h1_max_header_size: Option<usize>,
    h1_header_read_timeout: Dur,
    h1_writev: Option<bool>,
    max_buf_size: Option<usize>,
    pipeline_flush: bool,
    date_header: bool,
    buffer_provider: Option<Arc<dyn BufProvider>>,
}

/// Deconstructed parts of a `Connection`.
///
/// This allows taking apart a `Connection` at a later time, in order to
/// reclaim the IO object, and additional related pieces.
#[derive(Debug)]
#[non_exhaustive]
pub struct Parts<T, S> {
    /// The original IO object used in the handshake.
    pub io: T,
    /// A buffer of bytes that have been read but not processed as HTTP.
    ///
    /// If the client sent additional bytes after its last request, and
    /// this connection "ended" with an upgrade, the read buffer will contain
    /// those bytes.
    ///
    /// You will want to check for any existing bytes if you plan to continue
    /// communicating on the IO object.
    pub read_buf: Bytes,
    /// The `Service` used to serve this connection.
    pub service: S,
}

// ===== impl Connection =====

impl<I, S> fmt::Debug for Connection<I, S>
where
    S: HttpService<IncomingBody>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl<I, B, S> Connection<I, S>
where
    S: HttpService<IncomingBody, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Start a graceful shutdown process for this connection.
    ///
    /// This `Connection` should continue to be polled until shutdown
    /// can finish.
    ///
    /// # Note
    ///
    /// This should only be called while the `Connection` future is still
    /// pending. If called after `Connection::poll` has resolved, this does
    /// nothing.
    pub fn graceful_shutdown(mut self: Pin<&mut Self>) {
        self.conn.disable_keep_alive();
    }

    /// Return the inner IO object, and additional information.
    ///
    /// If the IO object has been "rewound" the io will not contain those bytes rewound.
    /// This should only be called after `poll_without_shutdown` signals
    /// that the connection is "done". Otherwise, it may not have finished
    /// flushing all necessary HTTP bytes.
    ///
    /// # Panics
    /// This method will panic if this connection is using an h2 protocol.
    pub fn into_parts(self) -> Parts<I, S> {
        let (io, read_buf, dispatch) = self.conn.into_inner();
        Parts {
            io,
            read_buf,
            service: dispatch.into_service(),
        }
    }

    /// Poll the connection for completion, but without calling `shutdown`
    /// on the underlying IO.
    ///
    /// This is useful to allow running a connection while doing an HTTP
    /// upgrade. Once the upgrade is completed, the connection would be "done",
    /// but it is not desired to actually shutdown the IO object. Instead you
    /// would take it back using `into_parts`.
    pub fn poll_without_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>>
    where
        S: Unpin,
        S::Future: Unpin,
    {
        self.conn.poll_without_shutdown(cx)
    }

    /// Prevent shutdown of the underlying IO object at the end of service the request,
    /// instead run `into_parts`. This is a convenience wrapper over `poll_without_shutdown`.
    ///
    /// # Error
    ///
    /// This errors if the underlying connection protocol is not HTTP/1.
    #[allow(clippy::missing_panics_doc)]
    pub fn without_shutdown(self) -> impl Future<Output = crate::Result<Parts<I, S>>> {
        let mut zelf = Some(self);
        crate::common::future::poll_fn(move |cx| {
            ready!(zelf
                .as_mut()
                .expect("server connection polled after completion")
                .conn
                .poll_without_shutdown(cx))?;
            Poll::Ready(Ok(zelf
                .take()
                .expect("server connection missing before completion")
                .into_parts()))
        })
    }

    /// Enable this connection to support higher-level HTTP upgrades.
    ///
    /// See [the `upgrade` module](crate::upgrade) for more.
    pub fn with_upgrades(self) -> UpgradeableConnection<I, S>
    where
        I: Send,
    {
        UpgradeableConnection { inner: Some(self) }
    }
}

impl<I, B, S> Future for Connection<I, S>
where
    S: HttpService<IncomingBody, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Output = crate::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(Pin::new(&mut self.conn).poll(cx)) {
            Ok(done) => {
                match done {
                    proto::Dispatched::Shutdown => {}
                    proto::Dispatched::Upgrade(pending) => {
                        // With no `Send` bound on `I`, we can't try to do
                        // upgrades here. In case a user was trying to use
                        // `Body::on_upgrade` with this API, send a special
                        // error letting them know about that.
                        pending.manual();
                    }
                }
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

// ===== impl Builder =====

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The buffer provider is a trait object without a `Debug` bound, so
        // only report whether one is configured.
        f.debug_struct("Builder")
            .field("h1_parser_config", &self.h1_parser_config)
            .field("timer", &self.timer)
            .field("h1_half_close", &self.h1_half_close)
            .field("h1_keep_alive", &self.h1_keep_alive)
            .field("h1_title_case_headers", &self.h1_title_case_headers)
            .field("h1_preserve_header_case", &self.h1_preserve_header_case)
            .field("h1_max_headers", &self.h1_max_headers)
            .field("h1_max_header_size", &self.h1_max_header_size)
            .field("h1_header_read_timeout", &self.h1_header_read_timeout)
            .field("h1_writev", &self.h1_writev)
            .field("max_buf_size", &self.max_buf_size)
            .field("pipeline_flush", &self.pipeline_flush)
            .field("date_header", &self.date_header)
            .field("buffer_provider", &self.buffer_provider.is_some())
            .finish()
    }
}

impl Builder {
    /// Create a new connection builder.
    pub fn new() -> Self {
        Self {
            h1_parser_config: ParserConfig::default(),
            timer: Time::Empty,
            h1_half_close: false,
            h1_keep_alive: true,
            h1_title_case_headers: false,
            h1_preserve_header_case: false,
            h1_max_headers: None,
            h1_max_header_size: None,
            h1_header_read_timeout: Dur::Default(Some(Duration::from_secs(30))),
            h1_writev: None,
            max_buf_size: None,
            pipeline_flush: false,
            date_header: true,
            buffer_provider: None,
        }
    }
    /// Set whether HTTP/1 connections should support half-closures.
    ///
    /// Clients can chose to shutdown their write-side while waiting
    /// for the server to respond. Setting this to `true` will
    /// prevent closing the connection immediately if `read`
    /// detects an EOF in the middle of a request.
    ///
    /// Default is `false`.
    pub fn half_close(&mut self, val: bool) -> &mut Self {
        self.h1_half_close = val;
        self
    }

    /// Enables or disables HTTP/1 keep-alive.
    ///
    /// Default is `true`.
    pub fn keep_alive(&mut self, val: bool) -> &mut Self {
        self.h1_keep_alive = val;
        self
    }

    /// Set whether HTTP/1 connections will write header names as title case at
    /// the socket level.
    ///
    /// Default is `false`.
    pub fn title_case_headers(&mut self, enabled: bool) -> &mut Self {
        self.h1_title_case_headers = enabled;
        self
    }

    /// Set whether multiple spaces are allowed as delimiters in request lines.
    ///
    /// Default is `false`.
    pub fn allow_multiple_spaces_in_request_line_delimiters(&mut self, enabled: bool) -> &mut Self {
        self.h1_parser_config
            .allow_multiple_spaces_in_request_line_delimiters(enabled);
        self
    }

    /// Set whether HTTP/1 connections will silently ignored malformed header lines.
    ///
    /// If this is enabled and a header line does not start with a valid header
    /// name, or does not include a colon at all, the line will be silently ignored
    /// and no error will be reported.
    ///
    /// Default is `false`.
    pub fn ignore_invalid_headers(&mut self, enabled: bool) -> &mut Builder {
        self.h1_parser_config
            .ignore_invalid_headers_in_requests(enabled);
        self
    }

    /// Set whether to support preserving original header cases.
    ///
    /// Currently, this will record the original cases received, and store them
    /// in a private extension on the `Request`. It will also look for and use
    /// such an extension in any provided `Response`.
    ///
    /// Since the relevant extension is still private, there is no way to
    /// interact with the original cases. The only effect this can have now is
    /// to forward the cases in a proxy-like fashion.
    ///
    /// Default is `false`.
    pub fn preserve_header_case(&mut self, enabled: bool) -> &mut Self {
        self.h1_preserve_header_case = enabled;
        self
    }

    /// Set the maximum number of headers.
    ///
    /// When a request is received, the parser will reserve a buffer to store headers for optimal
    /// performance.
    ///
    /// If server receives more headers than the buffer size, it responds to the client with
    /// "431 Request Header Fields Too Large".
    ///
    /// Note that headers is allocated on the stack by default, which has higher performance. After
    /// setting this value, headers will be allocated in heap memory, that is, heap memory
    /// allocation will occur for each request, and there will be a performance drop of about 5%.
    ///
    /// Default is 100.
    pub fn max_headers(&mut self, val: usize) -> &mut Self {
        self.h1_max_headers = Some(val);
        self
    }

    /// Set the maximum size of request headers (including the start line) in bytes.
    ///
    /// If the client sends headers exceeding this limit, the server responds to the
    /// client with "431 Request Header Fields Too Large" and closes the connection.
    ///
    /// If not configured, then the [`max_buf_size`](Builder::max_buf_size) will naturally be reached and applied.
    ///
    /// This value is also used as the maximum size limit for chunked trailers.
    ///
    /// Default is `None`.
    pub fn max_header_size(&mut self, val: usize) -> &mut Self {
        self.h1_max_header_size = Some(val);
        self
    }

    /// Set a timeout for reading client request headers. If a client does not
    /// transmit the entire header within this time, the connection is closed.
    ///
    /// Requires a [`Timer`] set by [`Builder::timer`] to take effect. Panics if `header_read_timeout` is configured
    /// without a [`Timer`].
    ///
    /// Pass `None` to disable.
    ///
    /// Default is 30 seconds.
    pub fn header_read_timeout(&mut self, read_timeout: impl Into<Option<Duration>>) -> &mut Self {
        self.h1_header_read_timeout = Dur::Configured(read_timeout.into());
        self
    }

    /// Set whether HTTP/1 connections should try to use vectored writes,
    /// or always flatten into a single buffer.
    ///
    /// Note that setting this to false may mean more copies of body data,
    /// but may also improve performance when an IO transport doesn't
    /// support vectored writes well, such as most TLS implementations.
    ///
    /// Setting this to true will force hyper to use queued strategy,
    /// which may eliminate unnecessary cloning on some TLS backends.
    ///
    /// Default is `auto`. In this mode hyper will try to guess which
    /// mode to use.
    pub fn writev(&mut self, val: bool) -> &mut Self {
        self.h1_writev = Some(val);
        self
    }

    /// Set the maximum buffer size for the connection.
    ///
    /// Default is ~400kb.
    ///
    /// # Panics
    ///
    /// The minimum value allowed is 8192. This method panics if the passed `max` is less than the minimum.
    pub fn max_buf_size(&mut self, max: usize) -> &mut Self {
        assert!(
            max >= proto::h1::MINIMUM_MAX_BUFFER_SIZE,
            "the max_buf_size cannot be smaller than the minimum that h1 specifies."
        );
        self.max_buf_size = Some(max);
        self
    }

    /// Set whether the `date` header should be included in HTTP responses.
    ///
    /// Note that including the `date` header is recommended by RFC 7231.
    ///
    /// Default is `true`.
    pub fn auto_date_header(&mut self, enabled: bool) -> &mut Self {
        self.date_header = enabled;
        self
    }

    /// Aggregates flushes to better support pipelined responses.
    ///
    /// Experimental, may have bugs.
    ///
    /// Default is `false`.
    pub fn pipeline_flush(&mut self, enabled: bool) -> &mut Self {
        self.pipeline_flush = enabled;
        self
    }

    /// Set the timer used in background tasks.
    pub fn timer<M>(&mut self, timer: M) -> &mut Self
    where
        M: Timer + Send + Sync + 'static,
    {
        self.timer = Time::Timer(Arc::new(timer));
        self
    }

    /// Set a provider of buffers for connections built by this builder.
    ///
    /// When set, a connection takes its read and write buffers from the
    /// provider when a message starts, and gives them back when the connection
    /// becomes idle waiting for the next request. An idle keep-alive connection
    /// therefore holds no buffers of its own, and buffers can be reused across
    /// connections by the application.
    ///
    /// While waiting for a request head without a read buffer, the connection
    /// reads into a small stack buffer, and takes a buffer from the provider
    /// only once bytes arrive.
    ///
    /// By default, no provider is set, and each connection allocates and keeps
    /// its own buffers for its whole lifetime.
    ///
    /// See [`BufProvider`] for an example pool.
    pub fn buffer_provider(&mut self, provider: Arc<dyn BufProvider>) -> &mut Self {
        self.buffer_provider = Some(provider);
        self
    }

    /// Bind a connection together with a [`Service`](crate::service::Service).
    ///
    /// This returns a Future that must be polled in order for HTTP to be
    /// driven on the connection.
    ///
    /// # Panics
    ///
    /// If a timeout option has been configured, but a `timer` has not been
    /// provided, calling `serve_connection` will panic.
    ///
    /// # Example
    ///
    /// ```
    /// # use hyper::{body::Incoming, Request, Response};
    /// # use hyper::service::Service;
    /// # use hyper::server::conn::http1::Builder;
    /// # use hyper::rt::{Read, Write};
    /// # async fn run<I, S>(some_io: I, some_service: S)
    /// # where
    /// #     I: Read + Write + Unpin + Send + 'static,
    /// #     S: Service<hyper::Request<Incoming>, Response=hyper::Response<Incoming>> + Send + 'static,
    /// #     S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    /// #     S::Future: Send,
    /// # {
    /// let http = Builder::new();
    /// let conn = http.serve_connection(some_io, some_service);
    ///
    /// if let Err(e) = conn.await {
    ///     eprintln!("server connection error: {}", e);
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn serve_connection<I, S>(&self, io: I, service: S) -> Connection<I, S>
    where
        S: HttpService<IncomingBody>,
        S::Error: Into<Box<dyn StdError + Send + Sync>>,
        S::ResBody: 'static,
        <S::ResBody as Body>::Error: Into<Box<dyn StdError + Send + Sync>>,
        I: Read + Write + Unpin,
    {
        let mut conn = proto::Conn::new(io);
        conn.set_h1_parser_config(self.h1_parser_config.clone());
        conn.set_timer(self.timer.clone());
        if !self.h1_keep_alive {
            conn.disable_keep_alive();
        }
        if self.h1_half_close {
            conn.set_allow_half_close();
        }
        if self.h1_title_case_headers {
            conn.set_title_case_headers();
        }
        if self.h1_preserve_header_case {
            conn.set_preserve_header_case();
        }
        if let Some(max_headers) = self.h1_max_headers {
            conn.set_http1_max_headers(max_headers);
        }
        if let Some(max_header_size) = self.h1_max_header_size {
            conn.set_http1_max_header_size(max_header_size);
        }
        if let Some(dur) = self
            .timer
            .check(self.h1_header_read_timeout, "header_read_timeout")
        {
            conn.set_http1_header_read_timeout(dur);
        }
        if let Some(writev) = self.h1_writev {
            if writev {
                conn.set_write_strategy_queue();
            } else {
                conn.set_write_strategy_flatten();
            }
        }
        conn.set_flush_pipeline(self.pipeline_flush);
        if let Some(max) = self.max_buf_size {
            conn.set_max_buf_size(max);
        }
        if !self.date_header {
            conn.disable_date_header();
        }
        if let Some(provider) = self.buffer_provider.clone() {
            conn.set_buffer_provider(provider);
        }
        let sd = proto::h1::dispatch::Server::new(service);
        let proto = proto::h1::Dispatcher::new(sd, conn);
        Connection { conn: proto }
    }
}

/// A future binding a connection with a Service with Upgrade support.
#[must_use = "futures do nothing unless polled"]
#[allow(missing_debug_implementations)]
pub struct UpgradeableConnection<T, S>
where
    S: HttpService<IncomingBody>,
{
    pub(super) inner: Option<Connection<T, S>>,
}

impl<I, B, S> UpgradeableConnection<I, S>
where
    S: HttpService<IncomingBody, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    /// Start a graceful shutdown process for this connection.
    ///
    /// This `Connection` should continue to be polled until shutdown
    /// can finish.
    pub fn graceful_shutdown(mut self: Pin<&mut Self>) {
        // Connection (`inner`) is `None` if it was upgraded (and `poll` is `Ready`).
        // In that case, we don't need to call `graceful_shutdown`.
        if let Some(conn) = self.inner.as_mut() {
            Pin::new(conn).graceful_shutdown();
        }
    }

    /// Return the inner IO object, and additional information provided the connection
    /// has not yet been upgraded.
    pub fn into_parts(self) -> Option<Parts<I, S>> {
        self.inner.map(Connection::into_parts)
    }
}

impl<I, B, S> Future for UpgradeableConnection<I, S>
where
    S: HttpService<IncomingBody, ResBody = B>,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    I: Read + Write + Unpin + Send + 'static,
    B: Body + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Output = crate::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(conn) = self.inner.as_mut() {
            match ready!(Pin::new(&mut conn.conn).poll(cx)) {
                Ok(proto::Dispatched::Shutdown) => Poll::Ready(Ok(())),
                Ok(proto::Dispatched::Upgrade(pending)) => {
                    let (io, buf, _) = self
                        .inner
                        .take()
                        .expect("upgradeable server connection missing after upgrade")
                        .conn
                        .into_inner();
                    pending.fulfill(Upgraded::new(io, buf));
                    Poll::Ready(Ok(()))
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        } else {
            // inner is `None`, meaning the connection was upgraded, thus it's `Poll::Ready(Ok(()))`
            Poll::Ready(Ok(()))
        }
    }
}
