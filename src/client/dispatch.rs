use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
#[cfg(feature = "http2")]
use std::{future::Future, pin::Pin};

use atomic_waker::AtomicWaker;
#[cfg(feature = "http2")]
use http::{Request, Response};
#[cfg(feature = "http2")]
use http_body::Body;
#[cfg(feature = "http2")]
use pin_project_lite::pin_project;
use tokio::sync::oneshot;

use crate::common::lock::LockResultExt;

#[cfg(feature = "http2")]
use crate::{body::Incoming, proto::h2::client::ResponseFutMap};

pub(crate) type RetryPromise<T, U> = oneshot::Receiver<Result<U, TrySendError<T>>>;
pub(crate) type Promise<T> = oneshot::Receiver<Result<T, crate::Error>>;

/// An error when calling `try_send_request`.
///
/// There is a possibility of an error occurring on a connection in-between the
/// time that a request is queued and when it is actually written to the IO
/// transport. If that happens, it is safe to return the request back to the
/// caller, as it was never fully sent.
#[derive(Debug)]
pub struct TrySendError<T> {
    pub(crate) error: crate::Error,
    pub(crate) message: Option<T>,
}

pub(crate) fn channel<T, U>() -> (Sender<T, U>, Receiver<T, U>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            first: None,
            rest: VecDeque::new(),
            senders: 1,
            receiver_open: true,
        }),
        recv_waker: AtomicWaker::new(),
    });
    let (giver, taker) = want::new();
    let tx = Sender {
        #[cfg(feature = "http1")]
        buffered_once: false,
        giver,
        inner: SenderInner::new(shared.clone()),
    };
    let rx = Receiver { shared, taker };
    (tx, rx)
}

/// A bounded sender of requests and callbacks for when responses are ready.
///
/// While the inner sender is unbounded, the Giver is used to determine
/// if the Receiver is ready for another request.
pub(crate) struct Sender<T, U> {
    /// One message is always allowed, even if the Receiver hasn't asked
    /// for it yet. This boolean keeps track of whether we've sent one
    /// without notice.
    #[cfg(feature = "http1")]
    buffered_once: bool,
    /// The Giver helps watch that the Receiver side has been polled
    /// when the queue is empty. This helps us know when a request and
    /// response have been fully processed, and a connection is ready
    /// for more.
    giver: want::Giver,
    /// Actually bounded by the Giver, plus `buffered_once`.
    inner: SenderInner<T, U>,
}

/// An unbounded version.
///
/// Cannot poll the Giver, but can still use it to determine if the Receiver
/// has been dropped. However, this version can be cloned.
#[cfg(feature = "http2")]
pub(crate) struct UnboundedSender<T, U> {
    /// Only used for `is_closed`, since the queue cannot be checked.
    giver: want::SharedGiver,
    inner: SenderInner<T, U>,
}

impl<T, U> Sender<T, U> {
    #[cfg(feature = "http1")]
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.giver
            .poll_want(cx)
            .map_err(|_| crate::Error::new_closed())
    }

    #[cfg(feature = "http1")]
    pub(crate) fn is_ready(&self) -> bool {
        self.giver.is_wanting()
    }

    #[cfg(feature = "http1")]
    pub(crate) fn is_closed(&self) -> bool {
        self.giver.is_canceled()
    }

    #[cfg(feature = "http1")]
    fn can_send(&mut self) -> bool {
        if self.giver.give() || !self.buffered_once {
            // If the receiver is ready *now*, then of course we can send.
            //
            // If the receiver isn't ready yet, but we don't have anything
            // in the channel yet, then allow one message.
            self.buffered_once = true;
            true
        } else {
            false
        }
    }

    #[cfg(feature = "http1")]
    pub(crate) fn try_send(&mut self, val: T) -> Result<RetryPromise<T, U>, T> {
        if !self.can_send() {
            return Err(val);
        }
        let (tx, rx) = oneshot::channel();
        self.inner
            .try_push(Envelope(Some((val, Callback::Retry(Some(tx))))))
            .map(move |()| rx)
            .map_err(|mut e| e.0.take().expect("envelope not dropped").0)
    }

    #[cfg(feature = "http1")]
    pub(crate) fn send(&mut self, val: T) -> Result<Promise<U>, T> {
        if !self.can_send() {
            return Err(val);
        }
        let (tx, rx) = oneshot::channel();
        self.inner
            .try_push(Envelope(Some((val, Callback::NoRetry(Some(tx))))))
            .map(move |()| rx)
            .map_err(|mut e| e.0.take().expect("envelope not dropped").0)
    }

    #[cfg(feature = "http2")]
    pub(crate) fn unbound(self) -> UnboundedSender<T, U> {
        UnboundedSender {
            giver: self.giver.shared(),
            inner: self.inner,
        }
    }
}

#[cfg(feature = "http2")]
impl<T, U> UnboundedSender<T, U> {
    pub(crate) fn is_ready(&self) -> bool {
        !self.giver.is_canceled()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.giver.is_canceled()
    }

    pub(crate) fn try_send(&mut self, val: T) -> Result<RetryPromise<T, U>, T> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .try_push(Envelope(Some((val, Callback::Retry(Some(tx))))))
            .map(move |()| rx)
            .map_err(|mut e| e.0.take().expect("envelope not dropped").0)
    }

    pub(crate) fn send(&mut self, val: T) -> Result<Promise<U>, T> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .try_push(Envelope(Some((val, Callback::NoRetry(Some(tx))))))
            .map(move |()| rx)
            .map_err(|mut e| e.0.take().expect("envelope not dropped").0)
    }
}

#[cfg(feature = "http2")]
impl<T, U> Clone for UnboundedSender<T, U> {
    fn clone(&self) -> Self {
        UnboundedSender {
            giver: self.giver.clone(),
            inner: self.inner.clone(),
        }
    }
}

/// The sending half of the shared queue.
///
/// A separate type so that dropping it can decrement the sender count even
/// when the queue is moved from a [`Sender`] to an [`UnboundedSender`].
struct SenderInner<T, U> {
    shared: Arc<Shared<T, U>>,
}

impl<T, U> SenderInner<T, U> {
    fn new(shared: Arc<Shared<T, U>>) -> Self {
        SenderInner { shared }
    }

    fn try_push(&self, env: Envelope<T, U>) -> Result<(), Envelope<T, U>> {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return Err(env);
        }
        state.push(env);
        drop(state);
        self.shared.recv_waker.wake();
        Ok(())
    }
}

impl<T, U> Clone for SenderInner<T, U> {
    fn clone(&self) -> Self {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.senders += 1;
        drop(state);
        SenderInner::new(self.shared.clone())
    }
}

impl<T, U> Drop for SenderInner<T, U> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.senders -= 1;
        let closed = state.senders == 0 && state.receiver_open;
        drop(state);
        if closed {
            // Let the receiver observe that no more messages can arrive.
            self.shared.recv_waker.wake();
        }
    }
}

pub(crate) struct Receiver<T, U> {
    shared: Arc<Shared<T, U>>,
    taker: want::Taker,
}

impl<T, U> Receiver<T, U> {
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<(T, Callback<T, U>)>> {
        // Register before checking, so a send racing this poll cannot be lost.
        self.shared.recv_waker.register(cx.waker());
        let (item, senders, receiver_open) = {
            let mut state = self.shared.state.lock().panic_if_poisoned();
            (state.pop(), state.senders, state.receiver_open)
        };
        if let Some(mut env) = item {
            // A want signaled after finding the queue empty can land after
            // the Sender's `give()` for the message taken here.
            // Withdraw it, or the Sender reports this connection as ready
            // while it is still serving this message.
            self.taker.unwant();
            return Poll::Ready(Some(
                env.0.take().expect("envelope not dropped"),
            ));
        }
        if senders == 0 || !receiver_open {
            self.taker.unwant();
            Poll::Ready(None)
        } else {
            self.taker.want();
            Poll::Pending
        }
    }

    #[cfg(feature = "http1")]
    pub(crate) fn close(&mut self) {
        self.taker.cancel();
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.receiver_open = false;
    }

    #[cfg(feature = "http1")]
    pub(crate) fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        self.close();
        // A send that raced the close either landed before it and is dropped
        // here, canceling its callback, or failed and handed the request back
        // to the caller. Once closed, no further envelopes can be queued, so
        // the drain always completes.
        self.drain();
        Poll::Ready(())
    }

    #[cfg(feature = "http1")]
    pub(crate) fn try_recv(&mut self) -> Option<(T, Callback<T, U>)> {
        let mut env = self.shared.state.lock().panic_if_poisoned().pop()?;
        env.0.take()
    }

    /// Drop every queued envelope, canceling its callback.
    fn drain(&mut self) {
        let (first, rest) = {
            let mut state = self.shared.state.lock().panic_if_poisoned();
            (state.first.take(), std::mem::take(&mut state.rest))
        };
        drop(first);
        drop(rest);
    }
}

impl<T, U> Drop for Receiver<T, U> {
    fn drop(&mut self) {
        // Notify the giver about the closure first, before dropping the queue.
        self.taker.cancel();
        {
            let mut state = self.shared.state.lock().panic_if_poisoned();
            state.receiver_open = false;
        }
        // Cancel queued callbacks now, rather than whenever the last sender
        // handle happens to be dropped.
        self.drain();
    }
}

/// The shared queue between the sending and receiving halves.
///
/// The first envelope is stored inline so that the common case, a single
/// queued request, needs no allocation beyond the shared state itself. The
/// previous unbounded Tokio channel allocated a 32-slot block up front, which
/// cost roughly 8 KB per connection even when it stayed empty.
struct Shared<T, U> {
    state: Mutex<State<T, U>>,
    recv_waker: AtomicWaker,
}

struct State<T, U> {
    first: Option<Envelope<T, U>>,
    rest: VecDeque<Envelope<T, U>>,
    /// Number of live sending handles. The receiver observes closure when this
    /// reaches zero and the queue is empty.
    senders: usize,
    /// Set once the receiver is closed or dropped, after which sends fail.
    receiver_open: bool,
}

impl<T, U> State<T, U> {
    fn push(&mut self, env: Envelope<T, U>) {
        if self.first.is_none() && self.rest.is_empty() {
            self.first = Some(env);
        } else {
            self.rest.push_back(env);
        }
    }

    fn pop(&mut self) -> Option<Envelope<T, U>> {
        if let Some(env) = self.first.take() {
            return Some(env);
        }
        self.rest.pop_front()
    }
}

struct Envelope<T, U>(Option<(T, Callback<T, U>)>);

impl<T, U> Drop for Envelope<T, U> {
    fn drop(&mut self) {
        if let Some((val, cb)) = self.0.take() {
            cb.send(Err(TrySendError {
                error: crate::Error::new_canceled().with("connection closed"),
                message: Some(val),
            }));
        }
    }
}

pub(crate) enum Callback<T, U> {
    #[allow(unused)]
    Retry(Option<oneshot::Sender<Result<U, TrySendError<T>>>>),
    NoRetry(Option<oneshot::Sender<Result<U, crate::Error>>>),
}

impl<T, U> Drop for Callback<T, U> {
    fn drop(&mut self) {
        match self {
            Callback::Retry(tx) => {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(Err(TrySendError {
                        error: dispatch_gone(),
                        message: None,
                    }));
                }
            }
            Callback::NoRetry(tx) => {
                if let Some(tx) = tx.take() {
                    let _ = tx.send(Err(dispatch_gone()));
                }
            }
        }
    }
}

#[cold]
fn dispatch_gone() -> crate::Error {
    // FIXME(nox): What errors do we want here?
    crate::Error::new_user_dispatch_gone().with(if std::thread::panicking() {
        "user code panicked"
    } else {
        "runtime dropped the dispatch task"
    })
}

impl<T, U> Callback<T, U> {
    #[cfg(feature = "http2")]
    pub(crate) fn is_canceled(&self) -> bool {
        match self {
            Callback::Retry(Some(tx)) => tx.is_closed(),
            Callback::NoRetry(Some(tx)) => tx.is_closed(),
            _ => unreachable!(),
        }
    }

    pub(crate) fn poll_canceled(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Callback::Retry(Some(tx)) => tx.poll_closed(cx),
            Callback::NoRetry(Some(tx)) => tx.poll_closed(cx),
            _ => unreachable!(),
        }
    }

    pub(crate) fn send(mut self, val: Result<U, TrySendError<T>>) {
        match &mut self {
            Callback::Retry(tx) => {
                let _ = tx
                    .take()
                    .expect("callback sender not dropped before send")
                    .send(val);
            }
            Callback::NoRetry(tx) => {
                let _ = tx
                    .take()
                    .expect("callback sender not dropped before send")
                    .send(val.map_err(|e| e.error));
            }
        }
    }
}

impl<T> TrySendError<T> {
    /// Take the message from this error.
    ///
    /// The message will not always have been recovered. If an error occurs
    /// after the message has been serialized onto the connection, it will not
    /// be available here.
    pub fn take_message(&mut self) -> Option<T> {
        self.message.take()
    }

    /// Returns a reference to the recovered message.
    ///
    /// The message will not always have been recovered. If an error occurs
    /// after the message has been serialized onto the connection, it will not
    /// be available here.
    pub fn message(&self) -> Option<&T> {
        self.message.as_ref()
    }

    /// Consumes this to return the inner error.
    pub fn into_error(self) -> crate::Error {
        self.error
    }

    /// Returns a reference to the inner error.
    pub fn error(&self) -> &crate::Error {
        &self.error
    }
}

#[cfg(feature = "http2")]
pin_project! {
    pub struct SendWhen<B, E>
    where
        B: Body,
        B: 'static,
    {
        #[pin]
        pub(crate) when: ResponseFutMap<B, E>,
        #[pin]
        pub(crate) call_back: Option<Callback<Request<B>, Response<Incoming>>>,
    }
}

#[cfg(feature = "http2")]
impl<B, E> Future for SendWhen<B, E>
where
    B: Body + 'static,
    E: crate::rt::bounds::Http2UpgradedExec<B::Data>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        let mut call_back = this.call_back.take().expect("polled after complete");

        match Pin::new(&mut this.when).poll(cx) {
            Poll::Ready(Ok(res)) => {
                call_back.send(Ok(res));
                Poll::Ready(())
            }
            Poll::Pending => {
                // check if the callback is canceled
                match call_back.poll_canceled(cx) {
                    Poll::Ready(v) => v,
                    Poll::Pending => {
                        // Move call_back back to struct before return
                        this.call_back.set(Some(call_back));
                        return Poll::Pending;
                    }
                }
                trace!("send_when canceled");
                // Tell pipe_task to reset the h2 stream so that
                // RST_STREAM is sent and flow-control capacity freed.
                this.when.as_mut().cancel();
                Poll::Ready(())
            }
            Poll::Ready(Err((error, message))) => {
                call_back.send(Err(TrySendError { error, message }));
                Poll::Ready(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "nightly")]
    extern crate test;

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[cfg(not(miri))]
    use futures_util::task::noop_waker_ref;

    use super::{channel, Callback, Receiver};

    #[derive(Debug)]
    struct Custom(#[allow(dead_code)] i32);

    impl<T, U> Future for Receiver<T, U> {
        type Output = Option<(T, Callback<T, U>)>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.poll_recv(cx)
        }
    }

    /// Helper to check if the future is ready after polling once.
    struct PollOnce<'fut, F>(&'fut mut F);

    impl<F, T> Future for PollOnce<'_, F>
    where
        F: Future<Output = T> + Unpin,
    {
        type Output = Option<()>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(_) => Poll::Ready(Some(())),
                Poll::Pending => Poll::Ready(None),
            }
        }
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn drop_receiver_sends_cancel_errors() {
        let _ = pretty_env_logger::try_init();

        let (mut tx, mut rx) = channel::<Custom, ()>();

        // must poll once for try_send to succeed
        assert!(PollOnce(&mut rx).await.is_none(), "rx empty");

        let promise = tx.try_send(Custom(43)).unwrap();
        drop(rx);

        let fulfilled = promise.await;
        let err = fulfilled
            .expect("fulfilled")
            .expect_err("promise should error");
        match (err.error.is_canceled(), err.message) {
            (true, Some(_)) => (),
            e => panic!("expected Error::Cancel(_), found {:?}", e),
        }
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn sender_checks_for_want_on_send() {
        let (mut tx, mut rx) = channel::<Custom, ()>();

        // one is allowed to buffer, second is rejected
        tx.try_send(Custom(1)).expect("1 buffered");
        tx.try_send(Custom(2)).expect_err("2 not ready");

        assert!(PollOnce(&mut rx).await.is_some(), "rx once");

        // Even though 1 has been popped, only 1 could be buffered for the
        // lifetime of the channel.
        tx.try_send(Custom(2)).expect_err("2 still not ready");

        assert!(PollOnce(&mut rx).await.is_none(), "rx empty");

        tx.try_send(Custom(2)).expect("2 ready");
    }

    #[cfg(feature = "http2")]
    #[test]
    fn unbounded_sender_doesnt_bound_on_want() {
        let (tx, rx) = channel::<Custom, ()>();
        let mut tx = tx.unbound();

        tx.try_send(Custom(1)).unwrap();
        tx.try_send(Custom(2)).unwrap();
        tx.try_send(Custom(3)).unwrap();

        drop(rx);

        tx.try_send(Custom(4)).unwrap_err();
    }

    /// The unbounded sender (used by HTTP/2) may queue several messages at
    /// once. The first goes inline and the rest into the overflow queue, and
    /// all must come back in send order.
    #[cfg(all(feature = "http2", not(miri)))]
    #[tokio::test]
    async fn receiver_yields_messages_in_order() {
        let (tx, mut rx) = channel::<Custom, ()>();
        let mut tx = tx.unbound();

        for i in 1..=4 {
            tx.try_send(Custom(i)).expect("buffered");
        }

        for i in 1..=4 {
            let (msg, _cb) = rx_take(&mut rx);
            assert_eq!(msg.0, i);
        }
    }

    #[cfg(not(miri))]
    fn rx_take<T, U>(rx: &mut Receiver<T, U>) -> (T, Callback<T, U>) {
        match rx.poll_recv(&mut Context::from_waker(noop_waker_ref())) {
            Poll::Ready(Some(item)) => item,
            Poll::Ready(None) => panic!("expected a queued message, channel closed"),
            Poll::Pending => panic!("expected a queued message, receiver pending"),
        }
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn receiver_reports_closed_after_last_sender_drops() {
        let (mut tx, mut rx) = channel::<Custom, ()>();

        // Queue one message, then drop the only sender.
        tx.try_send(Custom(1)).expect("buffered");
        drop(tx);

        // The queued message is still delivered before closure is reported.
        let (msg, _cb) = rx_take(&mut rx);
        assert_eq!(msg.0, 1);

        assert!(
            matches!(
                rx.poll_recv(&mut Context::from_waker(noop_waker_ref())),
                Poll::Ready(None)
            ),
            "closed receiver yields None"
        );
    }

    #[cfg(not(miri))]
    #[tokio::test]
    async fn queued_message_is_delivered_after_pending_poll() {
        let (mut tx, mut rx) = channel::<Custom, ()>();

        // Poll while empty to signal want and park.
        assert!(PollOnce(&mut rx).await.is_none(), "rx empty");

        // A message sent after the pending poll must be visible to the next
        // poll, without another wake-up in between.
        tx.try_send(Custom(7)).expect("ready");
        let (msg, _cb) = rx_take(&mut rx);
        assert_eq!(msg.0, 7);
    }

    /// The queue must not allocate a large block up front. The common case is a
    /// single queued request, so the first envelope is stored inline and the
    /// overflow queue starts empty.
    #[test]
    fn first_message_does_not_allocate_the_overflow_queue() {
        let (mut tx, rx) = channel::<Custom, ()>();

        tx.try_send(Custom(1)).expect("buffered");

        let state = rx.shared.state.lock().unwrap();
        assert!(state.first.is_some());
        assert_eq!(state.rest.capacity(), 0, "overflow queue stays unallocated");
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn giver_queue_throughput(b: &mut test::Bencher) {
        use crate::{body::Incoming, Request, Response};

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (mut tx, mut rx) = channel::<Request<Incoming>, Response<Incoming>>();

        b.iter(move || {
            tx.send(Request::new(Incoming::empty())).unwrap();
            rt.block_on(async {
                loop {
                    let poll_once = PollOnce(&mut rx);
                    let opt = poll_once.await;
                    if opt.is_none() {
                        break;
                    }
                }
            });
        })
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn giver_queue_not_ready(b: &mut test::Bencher) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (_tx, mut rx) = channel::<i32, ()>();
        b.iter(move || {
            rt.block_on(async {
                let poll_once = PollOnce(&mut rx);
                assert!(poll_once.await.is_none());
            });
        })
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn giver_queue_cancel(b: &mut test::Bencher) {
        let (_tx, mut rx) = channel::<i32, ()>();

        b.iter(move || {
            rx.taker.cancel();
        })
    }
}
