#![deny(warnings)]
#![deny(rust_2018_idioms)]

// Test: hyper must not corrupt bodies whose `Buf` impl yields small chunks.
//
// Regression tests for issue #3650. A user Body whose Data type implements
// `bytes::Buf` returning a `chunk()` shorter than `remaining()` (e.g. 1 byte
// at a time) caused hyper's write paths to miscount how many bytes of the
// user's buffer had actually been transferred:
//
// - In HTTP/1, the chunked terminator queued behind the body jumped ahead of
//   the body's remaining bytes, inserting `\r\n0\r\n\r\n` after every byte
//   and skipping over real body bytes.
// - In HTTP/2, the h2 codec chains the payload with frame-header bytes, so
//   a non-contiguous view of the payload would reorder the stream the same
//   way.

use std::cmp::min;
use std::convert::Infallible;
use std::pin::{pin, Pin};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http_body_util::{BodyExt, Empty};
use hyper::body::{Body, Frame, Incoming};
use hyper::server::conn::{http1, http2};
use hyper::service::service_fn;
use hyper::{Request, Response};
use support::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

mod support;

const TEST_MESSAGE: &str =
    "test message that needs to be a few bytes long for the sake of demonstration";

/// A `Buf` that only ever exposes a single byte per `chunk()`, like the one in
/// issue #3650.
struct MinimalBytes {
    current: usize,
    data: Vec<u8>,
}

impl Buf for MinimalBytes {
    fn remaining(&self) -> usize {
        self.data.len() - self.current
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            self.current + cnt <= self.data.len(),
            "Attempted to advance past end of MinimalBytes"
        );
        self.current += cnt;
    }

    fn chunk(&self) -> &[u8] {
        let end = min(self.current + 1, self.data.len());
        &self.data[self.current..end]
    }
}

/// A `Buf` that only ever exposes one byte at a time through `chunk()`, but
/// whose `chunks_vectored` reports its first and last bytes, skipping the
/// middle. This is *within the letter* of the `Buf` contract, which only
/// bounds the slices' total length by `remaining()`; it does not promise
/// they form a prefix of the logical stream.
///
/// Any write path that forwards user `chunks_vectored` output and then
/// advances by the number of bytes written will corrupt the stream with this
/// buffer: the middle bytes are never written but are skipped over.
struct GappedBytes {
    data: Vec<u8>,
    current: usize,
}

impl GappedBytes {
    fn new(data: Vec<u8>) -> Self {
        GappedBytes { data, current: 0 }
    }
}

impl Buf for GappedBytes {
    fn remaining(&self) -> usize {
        self.data.len() - self.current
    }

    fn advance(&mut self, cnt: usize) {
        self.current += cnt;
        assert!(self.current <= self.data.len());
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.current..self.current + 1]
    }

    fn chunks_vectored<'b>(&'b self, dst: &mut [std::io::IoSlice<'b>]) -> usize {
        if dst.is_empty() {
            return 0;
        }
        if self.current >= self.data.len() {
            return 0;
        }
        dst[0] = std::io::IoSlice::new(&self.data[self.current..self.current + 1]);
        if self.remaining() > 1 && dst.len() > 1 {
            let last = self.data.len() - 1;
            dst[1] = std::io::IoSlice::new(&self.data[last..last + 1]);
            2
        } else {
            1
        }
    }
}

/// A `Body` that yields its single `Data` buffer once, matching the minimal
/// example from issue #3650.
struct MinimalBody {
    frame: Option<MinimalBytes>,
}

impl Body for MinimalBody {
    type Data = MinimalBytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(
            self.get_mut()
                .frame
                .take()
                .map(|min_bytes| Ok(Frame::data(min_bytes))),
        )
    }
}

struct GappedBody {
    frame: Option<GappedBytes>,
}

impl Body for GappedBody {
    type Data = GappedBytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(
            self.get_mut()
                .frame
                .take()
                .map(|buf| Ok(Frame::data(buf))),
        )
    }
}

fn minimal_body() -> MinimalBody {
    MinimalBody {
        frame: Some(MinimalBytes {
            current: 0,
            data: TEST_MESSAGE.as_bytes().to_vec(),
        }),
    }
}

/// The payload of the `gapped_body`, for asserting against.
fn gapped_payload() -> Vec<u8> {
    let mut data = TEST_MESSAGE.as_bytes().to_vec();
    while data.len() < 1024 {
        data.extend_from_slice(TEST_MESSAGE.as_bytes());
    }
    data
}

fn gapped_body() -> GappedBody {
    // The h2 codec only chains the payload into its vectored write once it
    // passes its chain threshold (256 bytes), so make the payload large
    // enough to take that path.
    GappedBody {
        frame: Some(GappedBytes::new(gapped_payload())),
    }
}

#[tokio::test]
async fn h1_body_buf_with_small_chunks_is_not_corrupted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(|_: Request<Incoming>| async {
            Ok::<_, Infallible>(Response::new(minimal_body()))
        });
        let _ = http1::Builder::new()
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let mut received = Vec::new();
    let read = timeout(Duration::from_secs(5), async {
        let mut buf = [0u8; 4096];
        loop {
            let n = client.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
            if received.ends_with(b"\r\n0\r\n\r\n") {
                break;
            }
        }
    })
    .await;

    assert!(
        read.is_ok(),
        "response never completed; got {:?}",
        String::from_utf8_lossy(&received)
    );

    // The response must be a chunked response whose single chunk carries the
    // whole test message, then the terminating chunk. The `date` header is
    // inserted by hyper, so only compare from the end of the headers.
    let received = String::from_utf8_lossy(&received);
    assert!(
        received.ends_with("\r\n0\r\n\r\n"),
        "missing terminating chunk; got {received:?}"
    );
    let body = received
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or_default();
    let expected = format!("{:X}\r\n{TEST_MESSAGE}\r\n0\r\n\r\n", TEST_MESSAGE.len());
    assert_eq!(body, expected, "body was corrupted");
}

#[tokio::test]
async fn h2_body_buf_with_small_chunks_is_not_corrupted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(|_: Request<Incoming>| async {
            Ok::<_, Infallible>(Response::new(minimal_body()))
        });
        let _ = http2::Builder::new(TokioExecutor)
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let (mut send_request, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor)
        .handshake(TokioIo::new(client))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::get(format!("http://{addr}/"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = send_request
        .send_request(request)
        .await
        .expect("send request");

    // The whole message must arrive intact.
    let received = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    assert_eq!(received, TEST_MESSAGE.as_bytes(), "body was corrupted");
}

#[tokio::test]
async fn h2_request_body_buf_with_small_chunks_is_not_corrupted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let received = Arc::new(Mutex::new(Vec::new()));
    let server_received = Arc::clone(&received);
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(|req: Request<Incoming>| {
            let received = Arc::clone(&server_received);
            async move {
                let mut buf = Vec::new();
                let mut body = pin!(req.into_body());
                while let Some(frame) = body.as_mut().frame().await {
                    buf.extend_from_slice(&frame.unwrap().into_data().unwrap());
                }
                received.lock().unwrap().extend_from_slice(&buf);
                Ok::<_, Infallible>(Response::new(minimal_body()))
            }
        });
        let _ = http2::Builder::new(TokioExecutor)
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let (mut send_request, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor)
        .handshake(TokioIo::new(client))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Send the request with a partial-prefix `Buf` as the body; hyper's h2
    // client uses the same `SendBuf` wrapping as the server.
    let request = Request::post(format!("http://{addr}/"))
        .body(minimal_body())
        .unwrap();
    let response = send_request
        .send_request(request)
        .await
        .expect("send request");
    response
        .into_body()
        .collect()
        .await
        .expect("collect body");

    let received = received.lock().unwrap();
    assert_eq!(&*received, TEST_MESSAGE.as_bytes(), "body was corrupted");
}

#[tokio::test]
async fn h2_body_buf_with_gapped_vectored_slices_is_not_corrupted() {
    // The `Buf` contract does not promise `chunks_vectored` returns a prefix
    // of the logical stream, but the h2 codec consumes the payload exactly
    // that way. Forwarding a user's gapped slice list would reorder or drop
    // body bytes; hyper must only hand h2 the contiguous slice `chunk()`
    // guarantees.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(|_: Request<Incoming>| async {
            Ok::<_, Infallible>(Response::new(gapped_body()))
        });
        let _ = http2::Builder::new(TokioExecutor)
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let (mut send_request, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor)
        .handshake(TokioIo::new(client))
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let request = Request::get(format!("http://{addr}/"))
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = send_request
        .send_request(request)
        .await
        .expect("send request");

    let received = response
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    assert_eq!(received, gapped_payload(), "body was corrupted");
}
