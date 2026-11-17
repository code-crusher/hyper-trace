//! High level API for serving HTTP over TCP with hyper.

use std::error::Error as StdError;
use std::io;
use std::net::SocketAddr;

use http::{Request, Response};
use http_body::Body;
use hyper::body::Incoming as IncomingBody;

use super::conn::auto::Builder as AutoBuilder;
use super::tcp::Incoming;
use crate::rt::TokioExecutor;

/// A high level HTTP/1 and HTTP/2 server over TCP.
///
/// This type restores the accept-loop plumbing that was removed from
/// `hyper::Server` with hyper 1.0 (see
/// [hyper#3474](https://github.com/hyperium/hyper/issues/3474)). It accepts
/// connections from a TCP listener and serves each one with the provided
/// [`Service`](hyper::service::Service), automatically detecting whether the
/// connection speaks HTTP/1 or HTTP/2.
///
/// It is most useful with a listener that was not created by the server
/// itself, for example one inherited from another process with systemd
/// socket activation:
///
/// ```no_run
/// # use std::{convert::Infallible, net::SocketAddr};
/// # use http::{Request, Response};
/// # use http_body_util::Full;
/// # use hyper::{body::Bytes, body::Incoming, service::service_fn};
/// # use hyper_util::rt::TokioExecutor;
/// # use hyper_util::server::Server;
/// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// let addr: SocketAddr = "127.0.0.1:8080".parse()?;
/// // Or, with systemd socket activation, get the file descriptor from
/// // `listenfd` or the `systemd` crate and build a `std::net::TcpListener`.
/// let server = Server::try_bind(&addr)?;
///
/// let service = service_fn(|_req: Request<Incoming>| async move {
///     let body = Full::new(Bytes::from("Hello, world!"));
///     Ok::<_, Infallible>(Response::new(body))
/// });
///
/// server.serve(TokioExecutor::new(), service).await?;
/// # Ok(())
/// # }
/// ```
pub struct Server {
    incoming: Incoming,
}

impl Server {
    /// Create a server bound to the provided address.
    ///
    /// # Panics
    ///
    /// This method will panic if binding to the address fails. For a method
    /// to bind to an address and return a `Result`, see [`Server::try_bind`].
    pub fn bind(addr: &SocketAddr) -> Server {
        Server::try_bind(addr).unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        })
    }

    /// Try to create a server bound to the provided address.
    pub fn try_bind(addr: &SocketAddr) -> io::Result<Server> {
        Incoming::bind(addr).map(Server::from_incoming)
    }

    /// Create a server from an existing `std::net::TcpListener`.
    ///
    /// This is most useful for using a listener that was inherited from
    /// another process, such as with systemd socket activation.
    ///
    /// ```no_run
    /// # use std::net::TcpListener;
    /// # use hyper_util::server::Server;
    /// # fn main() -> std::io::Result<()> {
    /// let std_listener = TcpListener::bind("127.0.0.1:8080")?;
    /// // Or, with systemd socket activation, get the file descriptor from
    /// // `listenfd` or the `systemd` crate and build a `TcpListener` from it.
    /// let server = Server::from_tcp(std_listener)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_tcp(listener: std::net::TcpListener) -> io::Result<Server> {
        Incoming::from_std(listener).map(Server::from_incoming)
    }

    /// Create a server from an existing `tokio::net::TcpListener`.
    pub fn from_listener(listener: tokio::net::TcpListener) -> io::Result<Server> {
        Incoming::from_listener(listener).map(Server::from_incoming)
    }

    /// Create a server from an existing [`Incoming`] stream of connections.
    pub fn from_incoming(incoming: Incoming) -> Server {
        Server { incoming }
    }

    /// Get the local address this server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.incoming.local_addr()
    }

    /// Get the underlying [`Incoming`] stream of connections.
    pub fn into_incoming(self) -> Incoming {
        self.incoming
    }

    /// Accept connections forever, serving each one with the provided
    /// service, automatically detecting whether the connection speaks
    /// HTTP/1 or HTTP/2.
    ///
    /// This future resolves only if an error occurs while accepting a
    /// connection. Individual connection errors are returned by the spawned
    /// connection tasks and do not stop the accept loop.
    ///
    /// [`serve`][Server::serve] uses a [`TokioExecutor`] to drive the HTTP/2
    /// connections, and the service is shared across all connections (it is
    /// wrapped in an [`Arc`](std::sync::Arc) so it must be
    /// `Send + Sync + 'static`).
    pub async fn serve<S, B>(&self, executor: TokioExecutor, service: S) -> io::Result<()>
    where
        S: hyper::service::Service<Request<IncomingBody>, Response = Response<B>>
            + Send
            + Sync
            + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>> + Send,
        S::Future: Send,
        B: Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        self.serve_with(move || AutoBuilder::new(executor.clone()), service)
            .await
    }

    /// Accept connections forever, serving each one with the provided
    /// service, over connections built by `mk_builder`.
    ///
    /// `mk_builder` is called for each accepted connection and produces a
    /// fresh [`auto::Builder`](super::conn::auto::Builder); this allows
    /// configuring connection options while reusing the same server. The
    /// service is shared across all connections.
    ///
    /// This future resolves only if an error occurs while accepting a
    /// connection.
    pub async fn serve_with<S, B, E>(
        &self,
        mk_builder: impl Fn() -> AutoBuilder<E>,
        service: S,
    ) -> io::Result<()>
    where
        E: super::conn::auto::HttpServerConnExec<S::Future, B> + Clone + Send + Sync + 'static,
        S: hyper::service::Service<Request<IncomingBody>, Response = Response<B>>
            + Send
            + Sync
            + 'static,
        S::Error: Into<Box<dyn StdError + Send + Sync>> + Send,
        S::Future: Send,
        B: Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn StdError + Send + Sync>>,
    {
        use futures_core::Stream;
        use std::pin::pin;
        use std::sync::Arc;

        let service = Arc::new(service);
        let mut incoming = pin!(self.incoming.share());

        while let Some(result) = std::future::poll_fn(|cx| incoming.as_mut().poll_next(cx)).await {
            let stream = result?;
            let service = service.clone();
            let builder = mk_builder();

            tokio::spawn(async move {
                let conn = builder.serve_connection(stream, service);
                if let Err(e) = conn.await {
                    tracing::debug!("connection error: {}", e);
                }
            });
        }

        Ok(())
    }
}
