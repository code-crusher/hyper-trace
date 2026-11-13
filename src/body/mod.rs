//! Streaming bodies for Requests and Responses.
//!
//! For both [Clients](crate::client) and [Servers](crate::server), requests and
//! responses use streaming bodies, instead of complete buffering. This
//! allows applications to not use memory they don't need, and allows exerting
//! back-pressure on connections by only reading when asked.
//!
//! There are two pieces to this in hyper:
//!
//! - **The [`Body`] trait** describes all possible bodies.
//!   hyper allows any body type that implements `Body`, allowing
//!   applications to have fine-grained control over their streaming.
//! - **The [`Incoming`] concrete type**, which is an implementation
//!   of `Body`, and returned by hyper as a "receive stream" (so, for server
//!   requests and client responses).
//!
//! ## Which types are bodies?
//!
//! The [`Body`] trait itself is defined by the `http-body` crate, which
//! hyper re-exports here. `http-body` provides an implementation for
//! `String`, so a `String` can be used as a body directly.
//!
//! hyper cannot provide implementations for other standard buffer types,
//! such as `Vec<u8>`, `Bytes`, or `&'static [u8]`. The trait comes from
//! `http-body` and those types come from the standard library, so both are
//! foreign to hyper and Rust's [orphan rules][orphan] forbid the
//! implementation. Such impls would have to be added to `http-body` itself.
//!
//! For those types, wrap the data in a body from [`http-body-util`][], such
//! as `Full`:
//!
//! ```
//! use http_body_util::Full;
//! use hyper::body::Bytes;
//! use hyper::Request;
//!
//! // `String` implements `Body`, so it can be used directly.
//! let _req = Request::builder().body(String::from("hello")).unwrap();
//!
//! // Other buffers need a wrapper, such as `Full`.
//! let _req = Request::builder()
//!     .body(Full::<Bytes>::from(vec![0_u8, 1, 2]))
//!     .unwrap();
//! ```
//!
//! For the same reason, a `Vec<u8>` cannot be used as a body on its own. The
//! request builder accepts it, but sending the request requires `Body`:
//!
//! ```compile_fail,E0277
//! use hyper::body::Body;
//!
//! fn assert_body<B: Body>() {}
//!
//! // error[E0277]: the trait bound `Vec<u8>: Body` is not satisfied
//! assert_body::<Vec<u8>>();
//! ```
//!
//! There are additional implementations available in [`http-body-util`][],
//! such as `Full` or `Empty`.
//!
//! ## Reading a body
//!
//! The [`BodyExt`][] extension trait provides an asynchronous way to read the
//! frames of a body. A frame can contain either data or trailers:
//!
//! ```
//! use http_body_util::BodyExt as _;
//! use hyper::body::Incoming;
//!
//! async fn read_body(mut body: Incoming) -> Result<(), hyper::Error> {
//!     while let Some(frame) = body.frame().await {
//!         let frame = frame?;
//!
//!         if let Some(data) = frame.data_ref() {
//!             println!("received {} bytes", data.len());
//!         }
//!
//!         if let Some(trailers) = frame.trailers_ref() {
//!             println!("received trailers: {trailers:?}");
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! A body only advances when it is polled. Processing each frame before
//! polling for the next one preserves back-pressure on the connection.
//!
//! If a body is known to be small, it can be collected into memory instead:
//!
//! ```
//! use http_body_util::BodyExt as _;
//! use hyper::body::{Bytes, Incoming};
//!
//! /// Consider using `Limited` if the body is untrusted.
//! async fn read_entire_body(body: Incoming) -> Result<Bytes, hyper::Error> {
//!     Ok(body.collect().await?.to_bytes())
//! }
//! ```
//!
//! Collecting buffers the whole body, so it should be avoided for large or
//! untrusted bodies unless their size is limited.
//!
//! [`http-body-util`]: https://docs.rs/http-body-util
//! [`BodyExt`]: https://docs.rs/http-body-util/latest/http_body_util/trait.BodyExt.html
//! [orphan]: https://doc.rust-lang.org/reference/items/implementations.html#orphan-rules

pub use bytes::{Buf, Bytes};
pub use http_body::Body;
pub use http_body::Frame;
pub use http_body::SizeHint;

pub use self::incoming::Incoming;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http1"))]
pub(crate) use self::incoming::Sender;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
pub(crate) use self::length::DecodedLength;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http1"))]
mod chan;
mod incoming;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
mod length;

fn _assert_send_sync() {
    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    _assert_send::<Incoming>();
    _assert_sync::<Incoming>();
}

#[cfg(test)]
mod tests {
    use super::{Body, Bytes};
    use http_body_util::{BodyExt, Full};

    fn assert_body<B: Body>() {}

    // `http-body` implements `Body` for `String`, which is the only native
    // buffer type hyper can use directly. This is a regression guard: the
    // module docs promise that a `String` needs no wrapper.
    #[test]
    fn string_implements_body() {
        assert_body::<String>();
    }

    // hyper cannot implement `Body` for `Vec<u8>`, `Bytes`, or `&[u8]`: both
    // the trait (`http-body`) and those types (the standard library) are
    // foreign, so the orphan rules forbid it. The supported way to use them
    // is a wrapper from `http-body-util`, which the module docs show. That a
    // bare `Vec<u8>` is *not* a body is covered by a `compile_fail` doctest.
    #[test]
    fn wrapped_bytes_is_a_body() {
        assert_body::<Full<Bytes>>();
    }

    #[tokio::test]
    async fn wrapped_vec_body_yields_its_bytes() {
        let body = Full::<Bytes>::from(vec![0_u8, 1, 2]);

        assert_eq!(body.size_hint().exact(), Some(3));
        assert!(!body.is_end_stream());

        let collected = body.collect().await.expect("infallible body");
        assert_eq!(collected.to_bytes(), &[0_u8, 1, 2][..]);
    }
}
