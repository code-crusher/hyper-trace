#![deny(warnings)]

use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use tokio::net::{TcpListener, TcpStream};

use support::TokioIo;

#[path = "support/mod.rs"]
mod support;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

// ---------------------------------------------------------------------------
// A test double of the `http_proxy` example: an HTTP proxy speaking
// absolute-form for plain requests, and tunnels for `CONNECT` upgrades.
// ---------------------------------------------------------------------------

async fn proxy(req: Request<Incoming>) -> Result<Response<BoxBody>, hyper::Error> {
    if req.method() == hyper::Method::CONNECT {
        // Received an HTTP request like:
        // ```
        // CONNECT {addr} HTTP/1.1
        // Host: {addr}
        // ```
        //
        // Reply with an empty 200, and then tunnel the upgraded connection
        // to the target address.
        let auth = req
            .uri()
            .authority()
            .expect("connect has authority")
            .to_string();
        tokio::task::spawn(async move {
            let upgraded = match hyper::upgrade::on(req).await {
                Ok(upgraded) => upgraded,
                Err(e) => {
                    eprintln!("upgrade error: {e}");
                    return;
                }
            };
            if let Err(e) = tunnel(upgraded, auth).await {
                eprintln!("tunnel io error: {e}");
            }
        });
        Ok(Response::new(empty_body()))
    } else {
        // A plain request in absolute-form: forward it to the target.
        let host = req.uri().host().expect("uri has no host");
        let port = req.uri().port_u16().unwrap_or(80);

        let stream = TcpStream::connect((host, port)).await.unwrap();
        let io = TokioIo::new(stream);

        let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
            .preserve_header_case(true)
            .title_case_headers(true)
            .handshake(io)
            .await
            .unwrap();
        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                println!("connection failed: {err}");
            }
        });

        let resp = sender.send_request(req).await?;
        Ok(resp.map(|b| b.boxed()))
    }
}

async fn tunnel(upgraded: hyper::upgrade::Upgraded, addr: String) -> std::io::Result<()> {
    let mut server = TcpStream::connect(addr).await?;
    let mut upgraded = TokioIo::new(upgraded);
    let (_from_client, _from_server) =
        tokio::io::copy_bidirectional(&mut upgraded, &mut server).await?;
    Ok(())
}

fn empty_body() -> BoxBody {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full_body<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

async fn spawn_proxy() -> SocketAddr {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::task::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(ok) => ok,
                Err(_) => return,
            };
            let io = TokioIo::new(stream);

            tokio::task::spawn(async move {
                if let Err(err) = hyper::server::conn::http1::Builder::new()
                    .preserve_header_case(true)
                    .title_case_headers(true)
                    .serve_connection(io, service_fn(proxy))
                    .with_upgrades()
                    .await
                {
                    eprintln!("failed to serve connection: {err}");
                }
            });
        }
    });

    addr
}

// ---------------------------------------------------------------------------
// The target server, responding `Hello, {path}!`.
// ---------------------------------------------------------------------------

async fn hello_service(req: Request<Incoming>) -> Result<Response<BoxBody>, hyper::Error> {
    let name = req.uri().path().strip_prefix('/').unwrap_or("");
    Ok(Response::new(full_body(format!("Hello, {name}!"))))
}

async fn spawn_server() -> SocketAddr {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::task::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(ok) => ok,
                Err(_) => return,
            };
            let io = TokioIo::new(stream);

            tokio::task::spawn(async move {
                if let Err(err) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(hello_service))
                    .await
                {
                    eprintln!("failed to serve connection: {err}");
                }
            });
        }
    });

    addr
}

// ---------------------------------------------------------------------------
// Tests: the flows shown in examples/client_proxy.rs.
// ---------------------------------------------------------------------------

// A plain request sent in absolute-form through the proxy.
#[tokio::test]
async fn client_through_http_proxy() {
    let server = spawn_server().await;
    let proxy = spawn_proxy().await;

    // Connect to the *proxy*, not the server.
    let stream = TcpStream::connect(proxy).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("connection failed: {err}");
        }
    });

    let target = format!("http://{server}/hyper");
    let req = Request::builder()
        .uri(&target)
        .body(Empty::<Bytes>::new())
        .unwrap();

    let res = sender.send_request(req).await.unwrap();
    assert_eq!(res.status(), 200, "proxy forwarded the request");
    let body = res.collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"Hello, hyper!");
}

// A `CONNECT` request tunneled through the proxy, and then a request sent
// over the tunnel, as `examples/client_proxy.rs` does.
#[tokio::test]
async fn client_connect_tunnel_through_proxy() {
    let server = spawn_server().await;
    let proxy = spawn_proxy().await;

    let stream = TcpStream::connect(proxy).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::Builder::new()
        .preserve_header_case(true)
        .title_case_headers(true)
        .handshake(io)
        .await
        .unwrap();

    // Don't forget to enable upgrades on the connection, required for
    // `CONNECT` tunneling.
    tokio::task::spawn(async move {
        if let Err(err) = conn.with_upgrades().await {
            println!("connection failed: {err}");
        }
    });

    // The CONNECT target is an authority-form (`host:port`) URI.
    let uri: hyper::Uri = format!("{server}").parse().unwrap();
    let req = Request::builder()
        .method(hyper::Method::CONNECT)
        .uri(uri)
        .body(Empty::<Bytes>::new())
        .unwrap();

    let res = sender.send_request(req).await.unwrap();
    assert_eq!(res.status(), 200, "proxy accepted the tunnel");

    let upgraded = hyper::upgrade::on(res).await.unwrap();
    let mut io = TokioIo::new(upgraded);

    // The tunnel now carries plain HTTP bytes to the server. Usually a TLS
    // handshake would happen here first.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    io.write_all(b"GET /tunnel HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    io.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("HTTP/1.1 200 OK"), "got: {text}");
    assert!(text.contains("Hello, tunnel!"), "got: {text}");
}
