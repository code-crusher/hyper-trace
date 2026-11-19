use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures_channel::{mpsc, oneshot};
use futures_core::{ready, Stream};
use h2::{Reason, RecvStream, SendStream};
use pin_project_lite::pin_project;

use super::ping::Recorder;
use super::SendBuf;
use crate::rt::{Read, ReadBufCursor, Write};

/// Splits an upgraded HTTP/2 stream into a readable [`H2Upgraded`] and the
/// [`UpgradedSendStreamTask`] that drives its write side.
///
/// # Invariants
///
/// Once a stream has been upgraded, the tunnel must never send data from the
/// request body type `B`: the body was consumed by the protocol layer before
/// the upgrade happened. This is upheld by keeping the `h2` `SendStream` in
/// this task and only ever handing it `SendBuf::Cursor` (bytes written to the
/// upgraded tunnel) or `SendBuf::None` (end of stream), so `SendBuf::Buf(B)`
/// is unreachable on this path.
///
/// Because of that, the `SendStream<SendBuf<B>>` does not need to be
/// transmuted into a differently-typed `SendStream`. An earlier design did so
/// with a `repr(transparent)` `Neutered<B>` wrapper over an uninhabited enum;
/// that relied on layout guarantees the compiler does not make (#3500), and
/// was replaced by this task-and-channel bridge in #3967.
pub(super) fn pair<B>(
    send_stream: SendStream<SendBuf<B>>,
    recv_stream: RecvStream,
    ping: Recorder,
) -> (H2Upgraded, UpgradedSendStreamTask<B>) {
    let (tx, rx) = mpsc::channel(1);
    let (error_tx, error_rx) = oneshot::channel();

    (
        H2Upgraded {
            send_stream: UpgradedSendStreamBridge { tx, error_rx },
            recv_stream,
            ping,
            buf: Bytes::new(),
        },
        UpgradedSendStreamTask {
            h2_tx: send_stream,
            rx,
            buffered: None,
            error_tx: Some(error_tx),
        },
    )
}

pub(super) struct H2Upgraded {
    ping: Recorder,
    send_stream: UpgradedSendStreamBridge,
    recv_stream: RecvStream,
    buf: Bytes,
}

struct UpgradedSendStreamBridge {
    tx: mpsc::Sender<Cursor<Box<[u8]>>>,
    error_rx: oneshot::Receiver<crate::Error>,
}

pin_project! {
    #[must_use = "futures do nothing unless polled"]
    pub struct UpgradedSendStreamTask<B> {
        #[pin]
        h2_tx: SendStream<SendBuf<B>>,
        #[pin]
        rx: mpsc::Receiver<Cursor<Box<[u8]>>>,
        buffered: Option<Cursor<Box<[u8]>>>,
        error_tx: Option<oneshot::Sender<crate::Error>>,
    }
}

// ===== impl UpgradedSendStreamTask =====

impl<B> UpgradedSendStreamTask<B>
where
    B: Buf,
{
    fn tick(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), crate::Error>> {
        let mut me = self.project();

        // this is a manual `select()` over 3 "futures", so we always need
        // to be sure they are ready and/or we are waiting notification of
        // one of the sides hanging up, so the task doesn't live around
        // longer than it's meant to.
        loop {
            match me.h2_tx.poll_reset(cx) {
                Poll::Ready(Ok(reason)) => {
                    trace!("stream received RST_STREAM: {:?}", reason);
                    return Poll::Ready(Err(crate::Error::new_body_write(::h2::Error::from(
                        reason,
                    ))));
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(crate::Error::new_body_write(err)))
                }
                Poll::Pending => (),
            }

            // A write taken from the mpsc receiver waits here for h2 capacity,
            // and the next one isn't pulled until it has been handed to h2, so
            // the writer still sees h2 backpressure.
            if me.buffered.is_some() {
                // poll_capacity oddly needs a loop
                while me.h2_tx.capacity() == 0 {
                    match ready!(me.h2_tx.poll_capacity(cx)) {
                        Some(Ok(0)) => {}
                        Some(Ok(_)) => break,
                        Some(Err(e)) => return Poll::Ready(Err(crate::Error::new_body_write(e))),
                        None => {
                            // None means the stream is no longer in a
                            // streaming state, we either finished it
                            // somehow, or the remote reset us.
                            return Poll::Ready(Err(crate::Error::new_body_write(
                                "send stream capacity unexpectedly closed",
                            )));
                        }
                    }
                }

                let cursor = me.buffered.take().expect("checked is_some above");
                me.h2_tx
                    .send_data(SendBuf::Cursor(cursor), false)
                    .map_err(crate::Error::new_body_write)?;
                continue;
            }

            match me.rx.as_mut().poll_next(cx) {
                Poll::Ready(Some(cursor)) => {
                    // Only reserve capacity once there is something to send.
                    // Reserving while idle, even a single byte, pins that
                    // capacity on the connection-level window (#4003). As in
                    // `PipeToSendStream`, h2 raises the request to the
                    // buffered length inside `send_data`.
                    me.h2_tx.reserve_capacity(1);
                    *me.buffered = Some(cursor);
                }
                Poll::Ready(None) => {
                    me.h2_tx
                        .send_data(SendBuf::None, true)
                        .map_err(crate::Error::new_body_write)?;
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<B> Future for UpgradedSendStreamTask<B>
where
    B: Buf,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().tick(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(err)) => {
                if let Some(tx) = self.error_tx.take() {
                    let _oh_well = tx.send(err);
                }
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// ===== impl H2Upgraded =====

impl Read for H2Upgraded {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut read_buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.buf.is_empty() {
            self.buf = loop {
                match ready!(self.recv_stream.poll_data(cx)) {
                    None => return Poll::Ready(Ok(())),
                    Some(Ok(buf)) if buf.is_empty() && !self.recv_stream.is_end_stream() => {}
                    Some(Ok(buf)) => {
                        self.ping.record_data(buf.len());
                        break buf;
                    }
                    Some(Err(e)) => {
                        return Poll::Ready(match e.reason() {
                            Some(Reason::NO_ERROR | Reason::CANCEL) => Ok(()),
                            Some(Reason::STREAM_CLOSED) => {
                                Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
                            }
                            _ => Err(h2_to_io_error(e)),
                        })
                    }
                }
            };
        }
        let cnt = std::cmp::min(self.buf.len(), read_buf.remaining());
        read_buf.put_slice(&self.buf[..cnt]);
        self.buf.advance(cnt);
        let _ = self.recv_stream.flow_control().release_capacity(cnt);
        Poll::Ready(Ok(()))
    }
}

impl Write for H2Upgraded {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        match self.send_stream.tx.poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(_task_dropped)) => {
                // if the task dropped, check if there was an error
                // otherwise i guess its a broken pipe
                return match Pin::new(&mut self.send_stream.error_rx).poll(cx) {
                    Poll::Ready(Ok(reason)) => Poll::Ready(Err(io_error(reason))),
                    Poll::Ready(Err(_task_dropped)) => {
                        Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
                    }
                    Poll::Pending => Poll::Pending,
                };
            }
            Poll::Pending => return Poll::Pending,
        }

        let n = buf.len();
        match self.send_stream.tx.start_send(Cursor::new(buf.into())) {
            Ok(()) => Poll::Ready(Ok(n)),
            Err(_task_dropped) => {
                // if the task dropped, check if there was an error
                // otherwise i guess its a broken pipe
                match Pin::new(&mut self.send_stream.error_rx).poll(cx) {
                    Poll::Ready(Ok(reason)) => Poll::Ready(Err(io_error(reason))),
                    Poll::Ready(Err(_task_dropped)) => {
                        Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.send_stream.tx.poll_ready(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(_task_dropped)) => {
                // if the task dropped, check if there was an error
                // otherwise it was a clean close
                match Pin::new(&mut self.send_stream.error_rx).poll(cx) {
                    Poll::Ready(Ok(reason)) => Poll::Ready(Err(io_error(reason))),
                    Poll::Ready(Err(_task_dropped)) => Poll::Ready(Ok(())),
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.send_stream.tx.close_channel();
        match Pin::new(&mut self.send_stream.error_rx).poll(cx) {
            Poll::Ready(Ok(reason)) => Poll::Ready(Err(io_error(reason))),
            Poll::Ready(Err(_task_dropped)) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn io_error(e: crate::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e)
}

fn h2_to_io_error(e: h2::Error) -> std::io::Error {
    if e.is_io() {
        e.into_io()
            .expect("h2 error reported io cause without an underlying io error")
    } else {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Buf` that panics when used.
    ///
    /// An upgraded tunnel must never send the request body type `B`, only the
    /// bytes written to the [`H2Upgraded`] handle. If `UpgradedSendStreamTask`
    /// ever handed the `SendStream` a `SendBuf::Buf(_)`, `h2` would call into
    /// this type and the test would fail.
    struct PanicBuf;

    impl Buf for PanicBuf {
        fn remaining(&self) -> usize {
            panic!("upgraded stream tried to send the request body type")
        }

        fn chunk(&self) -> &[u8] {
            panic!("upgraded stream tried to send the request body type")
        }

        fn advance(&mut self, _cnt: usize) {
            panic!("upgraded stream tried to send the request body type")
        }
    }

    /// Writes all of `buf` to the upgraded stream, driving it to completion.
    async fn write_all(up: &mut H2Upgraded, mut buf: &[u8]) -> std::io::Result<()> {
        while !buf.is_empty() {
            let n =
                futures_util::future::poll_fn(|cx| Pin::new(&mut *up).poll_write(cx, buf)).await?;
            assert!(n > 0, "poll_write made no progress");
            buf = &buf[n..];
        }
        Ok(())
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn upgraded_send_stream_only_forwards_tunnel_bytes() {
        let (client_io, server_io) = tokio::io::duplex(1024);

        let (mut send_req, client_conn) = h2::client::Builder::new()
            .handshake::<_, SendBuf<PanicBuf>>(client_io)
            .await
            .expect("client handshake");
        tokio::spawn(async move {
            let _ = client_conn.await;
        });

        let mut server_conn = h2::server::Builder::new()
            .handshake::<_, SendBuf<PanicBuf>>(server_io)
            .await
            .expect("server handshake");

        let request = http::Request::connect("localhost")
            .body(())
            .expect("CONNECT request");
        let (response_fut, _client_send) = send_req.send_request(request, false).expect("send");

        let (request, mut respond) = server_conn.accept().await.expect("accept").expect("stream");
        assert_eq!(request.method(), http::Method::CONNECT);

        // Keep the server connection driven while the tunnel is open.
        tokio::spawn(async move {
            while let Some(res) = server_conn.accept().await {
                if res.is_err() {
                    break;
                }
            }
        });

        let response = http::Response::builder()
            .status(http::StatusCode::OK)
            .body(())
            .expect("response");
        let send_stream = respond
            .send_response(response, false)
            .expect("send response");

        let (mut h2_up, task) = pair(
            send_stream,
            request.into_body(),
            crate::proto::h2::ping::disabled(),
        );
        tokio::spawn(task);

        write_all(&mut h2_up, b"Bread?")
            .await
            .expect("write tunnel bytes");
        futures_util::future::poll_fn(|cx| Pin::new(&mut h2_up).poll_shutdown(cx))
            .await
            .expect("shutdown");

        let response = response_fut.await.expect("response");
        assert_eq!(response.status(), http::StatusCode::OK);

        let mut body = response.into_body();
        let bytes = body.data().await.expect("data").expect("body chunk");
        assert_eq!(&bytes[..], b"Bread?");
        let _ = body.flow_control().release_capacity(bytes.len());

        // The tunnel wrote nothing else; the stream is then closed with an
        // empty DATA frame carrying END_STREAM, or by returning `None`.
        if let Some(chunk) = body.data().await {
            assert!(chunk.expect("body chunk").is_empty());
        }
    }
}
