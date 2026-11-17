//! Pieces pertaining to the HTTP message protocol.

cfg_feature! {
    #![feature = "http1"]

    pub(crate) mod h1;

    pub(crate) use self::h1::Conn;

    #[cfg(feature = "client")]
    pub(crate) use self::h1::dispatch;
    #[cfg(feature = "server")]
    pub(crate) use self::h1::ServerTransaction;
}

#[cfg(feature = "http2")]
pub(crate) mod h2;

/// Check if the path portion of a request target contains a `{` or `}`.
///
/// Curly braces are not allowed in the path of a URI by the HTTP specifications
/// (RFC 3986, RFC 7230), and no mainstream browser accepts them in practice. However, the
/// `http` crate tolerates them for historical reasons, so hyper validates the target itself
/// when handling requests on the server side.
///
/// The scan stops at the first `?` or `#`, so only the path portion is checked; braces in
/// the query are tolerated, and a fragment is never considered.
#[cfg(any(feature = "http1", feature = "http2"))]
pub(crate) fn request_target_has_curly_braces(target: &str) -> bool {
    for &byte in target.as_bytes() {
        match byte {
            b'?' | b'#' => return false,
            b'{' | b'}' => return true,
            _ => (),
        }
    }

    false
}

/// An Incoming Message head. Includes request/status line, and headers.
#[cfg(feature = "http1")]
#[derive(Debug, Default)]
pub(crate) struct MessageHead<S> {
    /// HTTP version of the message.
    pub(crate) version: http::Version,
    /// Subject (request line or status line) of Incoming message.
    pub(crate) subject: S,
    /// Headers of the Incoming message.
    pub(crate) headers: http::HeaderMap,
    /// Extensions.
    extensions: http::Extensions,
}

/// An incoming request message.
#[cfg(feature = "http1")]
pub(crate) type RequestHead = MessageHead<RequestLine>;

#[derive(Debug, Default, PartialEq)]
#[cfg(feature = "http1")]
pub(crate) struct RequestLine(pub(crate) http::Method, pub(crate) http::Uri);

/// An incoming response message.
#[cfg(all(feature = "http1", feature = "client"))]
pub(crate) type ResponseHead = MessageHead<http::StatusCode>;

#[derive(Debug)]
#[cfg(feature = "http1")]
pub(crate) enum BodyLength {
    /// `Content-Length`.
    Known(u64),
    /// `Transfer-Encoding: chunked` (if h1).
    Unknown,
}

/// Status of when a Dispatcher future completes.
pub(crate) enum Dispatched {
    /// Dispatcher completely shutdown connection.
    Shutdown,
    /// Dispatcher has pending upgrade, and so did not shutdown.
    #[cfg(feature = "http1")]
    Upgrade(crate::upgrade::Pending),
}

#[cfg(all(feature = "client", feature = "http1"))]
impl MessageHead<http::StatusCode> {
    fn into_response<B>(self, body: B) -> http::Response<B> {
        let mut res = http::Response::new(body);
        *res.status_mut() = self.subject;
        *res.headers_mut() = self.headers;
        *res.version_mut() = self.version;
        *res.extensions_mut() = self.extensions;
        res
    }
}
