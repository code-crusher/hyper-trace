#![deny(warnings)]
#![warn(rust_2018_idioms)]

use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use tokio::net::TcpStream;

#[path = "../benches/support/mod.rs"]
mod support;
use support::TokioIo;

// A simple type alias so as to DRY.
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// To try this example:
// 1. cargo run --example http_proxy (from the `http_proxy` example)
// 2. cargo run --example client_proxy
//
// An HTTP proxy understands two kinds of client requests:
//
// - For a request to an HTTP server, the client sends the request with an
//   absolute-form URI, and the proxy forwards it.
// - For a request to an HTTPS server, the client sends a `CONNECT` request,
//   so the proxy can build a tunnel, and then the request is sent over the
//   tunnel (usually after a TLS handshake).
//
// This example shows the second kind, tunneling through a proxy with a
// `CONNECT` request.
#[tokio::main]
async fn main() -> Result<()> {
    let proxy: SocketAddr = ([127, 0, 0, 1], 8100).into();
    let server: SocketAddr = ([127, 0, 0, 1], 3000).into();

    // 1. Tunnel through the proxy, to get a raw connection to the server.
    let io = tunnel(proxy, server).await?;

    // 2. Send the request over the tunnel with a normal hyper client.
    //
    // In a real client, this is where you would put in a TLS handshake, and
    // only afterwards start sending HTTP requests.
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {err}");
        }
    });

    let authority = format!("{server}");
    let req = Request::builder()
        .uri(format!("http://{authority}/"))
        .header(hyper::header::HOST, &authority)
        .body(Empty::<Bytes>::new())?;

    let res = sender.send_request(req).await?;
    println!("Response: {}", res.status());

    let body = res.collect().await?.to_bytes();
    let body = String::from_utf8_lossy(body.as_ref());
    println!("Body: {body}");

    Ok(())
}

async fn tunnel(proxy: SocketAddr, server: SocketAddr) -> Result<hyper::upgrade::Upgraded> {
    // Open a socket to the proxy, and send a `CONNECT` request for the
    // server. The target of a `CONNECT` request is an authority-form
    // (`host:port`) URI, like `CONNECT hyper.rs:443 HTTP/1.1`.
    let stream = TcpStream::connect(proxy).await?;
    let io = TokioIo::new(stream);

    let authority = format!("{server}");
    let req = Request::builder()
        .method("CONNECT")
        .uri(&authority)
        .header(hyper::header::HOST, &authority)
        .body(Empty::<Bytes>::new())?;

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        // Don't forget to enable upgrades on the connection.
        if let Err(err) = conn.with_upgrades().await {
            println!("Connection failed: {err}");
        }
    });

    let res = sender.send_request(req).await?;
    if res.status() != hyper::StatusCode::OK {
        return Err(format!("proxy rejected CONNECT request: {}", res.status()).into());
    }

    // The tunnel is established, and the response body is the tunnel.
    let upgraded = hyper::upgrade::on(res).await?;
    Ok(upgraded)
}
