// Temporary probe: measures per-connection allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::rt::{Read, ReadBufCursor, Write};

struct Counting;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static COUNT: AtomicUsize = AtomicUsize::new(0);
static RECORD: AtomicBool = AtomicBool::new(false);
static SIZES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        COUNT.fetch_add(1, Ordering::Relaxed);
        if RECORD.load(Ordering::Relaxed) {
            if let Ok(mut v) = SIZES.try_lock() {
                v.push(layout.size());
            }
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn snapshot() -> (usize, usize) {
    (
        ALLOCATED.load(Ordering::Relaxed),
        COUNT.load(Ordering::Relaxed),
    )
}

struct SilentIo;

impl Read for SilentIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl Write for SilentIo {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn probe_h1() {
    RECORD.store(true, Ordering::Relaxed);
    let before = snapshot();
    let (tx, conn) = hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(SilentIo)
        .await
        .unwrap();
    let after = snapshot();
    RECORD.store(false, Ordering::Relaxed);
    println!(
        "h1 handshake: {} bytes in {} allocations",
        after.0 - before.0,
        after.1 - before.1
    );
    println!("sizes: {:?}", SIZES.lock().unwrap());
    drop(tx);
    drop(conn);
}

#[tokio::test]
async fn probe_h2() {
    RECORD.store(true, Ordering::Relaxed);
    let before = snapshot();
    let (tx, conn) = hyper::client::conn::http2::Builder::new(
        hyper::rt::TokioExecutor::new(),
    )
    .handshake::<_, Empty<Bytes>>(SilentIo)
    .await
    .unwrap();
    let after = snapshot();
    RECORD.store(false, Ordering::Relaxed);
    println!(
        "h2 handshake: {} bytes in {} allocations",
        after.0 - before.0,
        after.1 - before.1
    );
    println!("sizes: {:?}", SIZES.lock().unwrap());
    drop(tx);
    drop(conn);
}
