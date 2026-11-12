// Test: creating an HTTP/1 client connection must not allocate a large,
// up-front buffer for the dispatch channel.
//
// Each connection used an unbounded Tokio mpsc channel for dispatching
// requests, and Tokio allocates a 32-slot block (~8 KB for a request-sized
// message) as soon as the channel is created. That doubled the per-connection
// memory on top of the 8 KB HTTP/1 read buffer, even though a pooled client
// normally only ever has one request queued. The dispatch channel now keeps
// the first message inline and only grows when more are queued, which HTTP/1
// never needs.
//
// This test measures the heap allocations made while performing the handshake
// and asserts the dispatch channel no longer contributes a large block.

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::Empty;
use hyper::client::conn::http1;
use hyper::rt::{Read, ReadBufCursor, Write};

/// Records allocation sizes while `RECORD` is set.
struct Counting;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static RECORD: AtomicBool = AtomicBool::new(false);
static SIZES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if RECORD.load(Ordering::Relaxed) {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            if let Ok(mut sizes) = SIZES.try_lock() {
                sizes.push(layout.size());
            }
        }
        // SAFETY: delegating to the system allocator with the same layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` was allocated by the system allocator above.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Accepts every write and never answers, so the connection stays open.
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
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
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
async fn h1_handshake_does_not_preallocate_a_large_dispatch_buffer() {
    // This is the only test in this binary, so the global counters only see
    // allocations from the handshake below.
    SIZES.lock().unwrap().clear();
    RECORD.store(true, Ordering::Relaxed);
    let before = ALLOCATED.load(Ordering::Relaxed);

    let (sender, conn) = http1::handshake::<_, Empty<Bytes>>(SilentIo)
        .await
        .expect("handshake");

    RECORD.store(false, Ordering::Relaxed);
    let total = ALLOCATED.load(Ordering::Relaxed) - before;
    let sizes = SIZES.lock().unwrap().clone();

    // The HTTP/1 read buffer is 8 KB and is expected. The dispatch channel used
    // to add another ~8 KB block; allow a small margin above the read buffer
    // for the shared state and the connection itself.
    let largest = sizes.iter().copied().max().unwrap_or(0);
    assert!(
        total < 12 * 1024,
        "handshake allocated {total} bytes (largest block {largest}); \
         the dispatch channel should not preallocate a large block"
    );
    assert!(
        sizes.iter().filter(|&&size| size >= 4 * 1024).count() <= 1,
        "expected at most the 8 KB read buffer, found blocks: {sizes:?}"
    );

    drop(sender);
    drop(conn);
}
