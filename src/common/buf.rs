use std::collections::VecDeque;
use std::io::IoSlice;

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) struct BufList<T> {
    bufs: VecDeque<T>,
}

/// A [`Buf`] that can expose a **contiguous prefix** of itself as vectored
/// slices.
///
/// [`Buf::chunks_vectored`] only promises that the returned slices describe
/// *some* of the remaining bytes: a `Buf` whose [`Buf::chunk`] returns a
/// partial prefix (shorter than `remaining()`) may also report slices for
/// bytes that come after the gap. Concatenating those slices produces the
/// bytes out of order.
///
/// This is the contract h1's write path actually needs: the slices it hands
/// to `writev` are consumed in order and matched against an `advance(n)`
/// of the logical buffer. `chunks_vectored_prefix` returns a prefix of the
/// logical byte stream, along with whether the prefix covers **all**
/// remaining bytes, so callers know when it is safe to continue with the
/// buffers that follow.
pub(crate) trait VectoredBuf: Buf {
    /// Fill `dst` with slices forming a contiguous prefix of this buffer.
    ///
    /// Returns the number of slices written, and whether they cover all
    /// remaining bytes.
    #[inline]
    fn chunks_vectored_prefix<'data>(&'data self, dst: &mut [IoSlice<'data>]) -> (usize, bool) {
        chunks_vectored_from_chunk(self, dst)
    }
}

/// Fill a single vectored slice from the one prefix primitive `Buf`
/// guarantees: `chunk()`.
#[inline]
pub(crate) fn chunks_vectored_from_chunk<'data, B>(
    buf: &'data B,
    dst: &mut [IoSlice<'data>],
) -> (usize, bool)
where
    B: Buf + ?Sized,
{
    let remaining = buf.remaining();
    if remaining == 0 {
        return (0, true);
    }
    if dst.is_empty() {
        return (0, false);
    }

    let chunk = buf.chunk();
    debug_assert!(
        !chunk.is_empty(),
        "Buf::chunk() must not be empty while remaining() is {}",
        remaining,
    );
    if chunk.is_empty() {
        // A `Buf` violating its contract cannot be exposed safely; fall
        // back to reporting no progress, which the write loop treats as
        // an error rather than corrupting the stream.
        return (0, false);
    }
    dst[0] = IoSlice::new(chunk);
    (1, chunk.len() == remaining)
}

// `Bytes` and slices are contiguous by construction: their default
// `chunks_vectored` reports exactly `chunk()`, which is their whole
// remaining bytes, so the slices always form a complete prefix.
impl VectoredBuf for Bytes {}
impl VectoredBuf for &'static [u8] {}

impl<T: Buf> BufList<T> {
    pub(crate) fn new() -> BufList<T> {
        BufList {
            bufs: VecDeque::new(),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, buf: T) {
        debug_assert!(buf.has_remaining());
        self.bufs.push_back(buf);
    }

    #[inline]
    pub(crate) fn bufs_cnt(&self) -> usize {
        self.bufs.len()
    }
}

impl<T: Buf> Buf for BufList<T> {
    #[inline]
    fn remaining(&self) -> usize {
        self.bufs.iter().map(Buf::remaining).sum()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        self.bufs.front().map(Buf::chunk).unwrap_or_default()
    }

    #[inline]
    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            {
                let front = &mut self.bufs[0];
                let rem = front.remaining();
                if rem > cnt {
                    front.advance(cnt);
                    return;
                } else {
                    front.advance(rem);
                    cnt -= rem;
                }
            }
            self.bufs.pop_front();
        }
    }

    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        // Our inner buffer may have an optimized version of copy_to_bytes, and if the whole
        // request can be fulfilled by the front buffer, we can take advantage.
        match self.bufs.front_mut() {
            Some(front) if front.remaining() == len => {
                let b = front.copy_to_bytes(len);
                self.bufs.pop_front();
                b
            }
            Some(front) if front.remaining() > len => front.copy_to_bytes(len),
            _ => {
                assert!(len <= self.remaining(), "`len` greater than remaining");
                let mut bm = BytesMut::with_capacity(len);
                bm.put(self.take(len));
                bm.freeze()
            }
        }
    }
}

impl<T: VectoredBuf> VectoredBuf for BufList<T> {
    #[inline]
    fn chunks_vectored_prefix<'data>(&'data self, dst: &mut [IoSlice<'data>]) -> (usize, bool) {
        let mut vecs = 0;
        for buf in &self.bufs {
            if vecs == dst.len() {
                return (vecs, false);
            }
            let (n, complete) = buf.chunks_vectored_prefix(&mut dst[vecs..]);
            vecs += n;
            if !complete {
                // Only continue with the buffers that follow once this one
                // is fully covered, otherwise bytes would come out of
                // order.
                return (vecs, false);
            }
        }
        (vecs, true)
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;

    fn hello_world_buf() -> BufList<Bytes> {
        BufList {
            bufs: vec![Bytes::from("Hello"), Bytes::from(" "), Bytes::from("World")].into(),
        }
    }

    #[test]
    fn to_bytes_shorter() {
        let mut bufs = hello_world_buf();
        let old_ptr = bufs.chunk().as_ptr();
        let start = bufs.copy_to_bytes(4);
        assert_eq!(start, "Hell");
        assert!(ptr::eq(old_ptr, start.as_ptr()));
        assert_eq!(bufs.chunk(), b"o");
        assert!(ptr::eq(old_ptr.wrapping_add(4), bufs.chunk().as_ptr()));
        assert_eq!(bufs.remaining(), 7);
    }

    #[test]
    fn to_bytes_eq() {
        let mut bufs = hello_world_buf();
        let old_ptr = bufs.chunk().as_ptr();
        let start = bufs.copy_to_bytes(5);
        assert_eq!(start, "Hello");
        assert!(ptr::eq(old_ptr, start.as_ptr()));
        assert_eq!(bufs.chunk(), b" ");
        assert_eq!(bufs.remaining(), 6);
    }

    #[test]
    fn to_bytes_longer() {
        let mut bufs = hello_world_buf();
        let start = bufs.copy_to_bytes(7);
        assert_eq!(start, "Hello W");
        assert_eq!(bufs.remaining(), 4);
    }

    #[test]
    fn one_long_buf_to_bytes() {
        let mut buf = BufList::new();
        buf.push(b"Hello World" as &[_]);
        assert_eq!(buf.copy_to_bytes(5), "Hello");
        assert_eq!(buf.chunk(), b" World");
    }

    #[test]
    #[should_panic(expected = "`len` greater than remaining")]
    fn buf_to_bytes_too_many() {
        hello_world_buf().copy_to_bytes(42);
    }

    /// A `Buf` whose `chunk()` only ever exposes a single byte, like the one
    /// in issue #3650. `remaining()` is much larger than what the default
    /// `chunks_vectored` can report.
    struct OneByteAtATime {
        data: &'static [u8],
        pos: usize,
    }

    impl Buf for OneByteAtATime {
        fn remaining(&self) -> usize {
            self.data.len() - self.pos
        }

        fn chunk(&self) -> &[u8] {
            let end = std::cmp::min(self.pos + 1, self.data.len());
            &self.data[self.pos..end]
        }

        fn advance(&mut self, cnt: usize) {
            self.pos += cnt;
        }
    }

    impl VectoredBuf for OneByteAtATime {}

    /// Drain `bufs` the way the h1 write path does: gather a prefix of
    /// slices, then advance over the bytes they cover, writev by writev.
    fn drain_vectored(mut bufs: impl VectoredBuf) -> Vec<u8> {
        let mut out = Vec::new();
        while bufs.has_remaining() {
            let mut iovs = [IoSlice::new(&[]); 8];
            let (n, _) = bufs.chunks_vectored_prefix(&mut iovs);
            assert!(n > 0, "no vectored chunks returned while bytes remain");
            let before = bufs.remaining();
            let mut wrote = 0;
            for iov in &iovs[..n] {
                out.extend_from_slice(iov);
                wrote += iov.len();
            }
            bufs.advance(wrote);
            assert_eq!(bufs.remaining(), before - wrote, "prefix bytes only");
        }
        out
    }

    // Regression test for issue #3650: a queued `Buf` that only exposes a
    // partial prefix must stop the vectored composition there, so the buffers
    // queued behind it (in h1, the chunked terminator) cannot jump ahead of
    // its remaining bytes.
    #[test]
    fn vectored_prefix_stops_at_partial_buf() {
        let mut bufs = BufList::new();
        bufs.push(OneByteAtATime {
            data: b"test",
            pos: 0,
        });
        bufs.push(OneByteAtATime {
            data: b"next",
            pos: 0,
        });

        let mut iovs = [IoSlice::new(&[]); 8];
        let (n, complete) = bufs.chunks_vectored_prefix(&mut iovs);

        assert_eq!(n, 1);
        assert_eq!(&*iovs[0], b"t");
        assert!(!complete, "the first body is only partially covered");

        // Draining the whole list still yields the bytes in order.
        let mut drained = iovs[..n].iter().fold(Vec::new(), |mut v, iov| {
            v.extend_from_slice(iov);
            v
        });
        bufs.advance(1);
        drained.extend_from_slice(&drain_vectored(bufs));
        assert_eq!(drained, b"testnext");
    }

    #[test]
    fn vectored_prefix_batches_bytes_bufs() {
        let mut bufs = BufList::new();
        bufs.push(Bytes::from_static(b"foo bar"));
        bufs.push(Bytes::from_static(b"baz quux"));

        let mut iovs = [IoSlice::new(&[]); 8];
        let (n, complete) = bufs.chunks_vectored_prefix(&mut iovs);

        assert_eq!(n, 2);
        assert_eq!(&*iovs[0], b"foo bar");
        assert_eq!(&*iovs[1], b"baz quux");
        assert!(complete);

        assert_eq!(drain_vectored(bufs), b"foo barbaz quux");
    }
}
