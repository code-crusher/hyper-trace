//! Tests for the high level TCP server (`hyper_util::server::Server`),
//! including `Server::from_tcp` for taking over a pre-existing listener,
//! as used with systemd socket activation.
//!
//! See <https://github.com/hyperium/hyper/issues/3474>.

use std::convert::Infallible;
use std::net::{SocketAddr, TcpListener as StdTcpListener};

use http::{Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::Server;

const BODY: &[u8] = b"Hello, world!";

async fn handle(_req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::from(BODY.to_vec()))))
}

/// Spawn a server on the given address, returning once it is listening.
async fn spawn_server(server: Server) -> SocketAddr {
    let addr = server.local_addr();
    tokio::spawn(async move { server.serve(TokioExecutor::new(), service_fn(handle)).await });
    addr
}

async fn get(addr: SocketAddr) -> Vec<u8> {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(
        hyper_util::rt::TokioIo::new(stream),
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    let response = sender
        .send_request(Request::new(Empty::<Bytes>::new()))
        .await
        .unwrap();
    response.into_body().collect().await.unwrap().to_bytes().to_vec()
}

#[tokio::test]
async fn from_tcp_serves_http1() {
    // Build a std listener the way systemd socket activation would hand
    // one over, then run the server on it.
    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();

    let server = Server::from_tcp(std_listener).unwrap();
    assert_eq!(server.local_addr(), addr);

    let addr = spawn_server(server).await;
    assert_eq!(get(addr).await, BODY);
}

#[tokio::test]
async fn try_bind_serves_http1() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = Server::try_bind(&addr).unwrap();

    let addr = spawn_server(server).await;
    assert_eq!(get(addr).await, BODY);
}

#[tokio::test]
async fn from_listener_serves_http1() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    let server = Server::from_listener(listener).unwrap();

    let addr = spawn_server(server).await;
    assert_eq!(get(addr).await, BODY);
}

#[tokio::test]
async fn from_tcp_serves_http2() {
    use hyper_util::rt::TokioIo;

    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();

    let server = Server::from_tcp(std_listener).unwrap();
    let _ = tokio::spawn(async move {
        server
            .serve(TokioExecutor::new(), service_fn(handle))
            .await
    });

    let stream = TokioIo::new(tokio::net::TcpStream::connect(addr).await.unwrap());
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(stream)
        .await
        .unwrap();
    tokio::spawn(conn);

    let response = sender
        .send_request(Request::new(Empty::<Bytes>::new()))
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, BODY);
}

#[tokio::test]
async fn from_tcp_serves_multiple_connections() {
    let std_listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();

    let server = Server::from_tcp(std_listener).unwrap();
    let addr = spawn_server(server).await;

    for _ in 0..4 {
        assert_eq!(get(addr).await, BODY);
    }
}
