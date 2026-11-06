//! Just enough HTTP for a browser to fetch the player page and trade one SDP
//! offer for an answer.
//!
//! Hand-written rather than a framework because the whole surface is two
//! routes, and what matters about a listener on a public address is what it
//! refuses: a head over [`MAX_HEAD`], a body over [`MAX_BODY`], anything that
//! is not plain `Content-Length` framing. Every connection carries one request
//! and is closed after the answer.

use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt};

/// The request line and headers together. A browser's are well under 2 KiB.
pub const MAX_HEAD: usize = 16 * 1024;
/// An SDP offer from a browser is a few KiB.
pub const MAX_BODY: usize = 64 * 1024;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// The first header named `name`, compared without regard to case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The first query parameter named `name`, undecoded.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HttpError {
    /// The connection closed before a whole request arrived.
    Closed,
    TooLarge,
    Malformed(&'static str),
    Io(String),
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "connection closed mid-request"),
            Self::TooLarge => write!(f, "request too large"),
            Self::Malformed(why) => write!(f, "malformed request: {why}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

/// Read one request.
pub async fn read_request<R: AsyncRead + Unpin>(r: &mut R) -> Result<Request, HttpError> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(at) = find_head_end(&buf) {
            break at;
        }
        if buf.len() > MAX_HEAD {
            return Err(HttpError::TooLarge);
        }
        let n = r
            .read(&mut chunk)
            .await
            .map_err(|e| HttpError::Io(e.to_string()))?;
        if n == 0 {
            return Err(HttpError::Closed);
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    if head_end > MAX_HEAD {
        return Err(HttpError::TooLarge);
    }
    let (mut request, length) = parse_head(&buf[..head_end])?;
    if length > MAX_BODY {
        return Err(HttpError::TooLarge);
    }
    let mut body = buf[head_end + 4..].to_vec();
    if body.len() > length {
        return Err(HttpError::Malformed("more body than Content-Length"));
    }
    while body.len() < length {
        let n = r
            .read(&mut chunk)
            .await
            .map_err(|e| HttpError::Io(e.to_string()))?;
        if n == 0 {
            return Err(HttpError::Closed);
        }
        body.extend_from_slice(&chunk[..n]);
        if body.len() > length {
            return Err(HttpError::Malformed("more body than Content-Length"));
        }
    }
    request.body = body;
    Ok(request)
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// The request line and headers, and the body length they declare.
fn parse_head(head: &[u8]) -> Result<(Request, usize), HttpError> {
    let head = std::str::from_utf8(head).map_err(|_| HttpError::Malformed("not UTF-8"))?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or(HttpError::Malformed("empty"))?;
    let mut parts = request_line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(HttpError::Malformed("request line"));
    };
    if !version.starts_with("HTTP/1.") {
        return Err(HttpError::Malformed("version"));
    }
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    let mut headers = Vec::new();
    let mut length = 0usize;
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(HttpError::Malformed("header"))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            length = value
                .parse()
                .map_err(|_| HttpError::Malformed("Content-Length"))?;
        }
        // Chunked bodies are not something a browser sends with a short fetch
        // body, and reading them is code that only exists to be got wrong.
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(HttpError::Malformed("Transfer-Encoding"));
        }
        headers.push((name.to_owned(), value.to_owned()));
    }
    Ok((
        Request {
            method: method.to_owned(),
            path: path.to_owned(),
            query: query.to_owned(),
            headers,
            body: Vec::new(),
        },
        length,
    ))
}

/// A whole response, closing the connection after it.
///
/// Every response allows any origin: the page that drives a session will not
/// be served from this machine once there is a website in front of it, and the
/// offer route is guarded by its token, not by where the request came from.
pub fn response(status: u16, content_type: &str, body: &[u8]) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let mut out = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-store\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: Authorization, Content-Type\r\n\
         Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(raw: &[u8]) -> Result<Request, HttpError> {
        let mut r = raw;
        read_request(&mut r).await
    }

    #[tokio::test]
    async fn a_post_with_a_body_is_read_whole() {
        let req = read(
            b"POST /offer?token=abc HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\
              Authorization: Bearer t\r\n\r\nhello",
        )
        .await
        .unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/offer");
        assert_eq!(req.query_param("token"), Some("abc"));
        assert_eq!(req.header("authorization"), Some("Bearer t"));
        assert_eq!(req.body, b"hello");
    }

    #[tokio::test]
    async fn a_get_without_a_body_has_an_empty_one() {
        let req = read(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await.unwrap();
        assert_eq!((req.method.as_str(), req.path.as_str()), ("GET", "/"));
        assert!(req.body.is_empty());
    }

    #[tokio::test]
    async fn what_a_listener_on_a_public_address_must_refuse() {
        let huge = format!(
            "POST /offer HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        );
        assert_eq!(
            read(huge.as_bytes()).await.unwrap_err(),
            HttpError::TooLarge
        );

        let long_head = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(MAX_HEAD));
        assert_eq!(
            read(long_head.as_bytes()).await.unwrap_err(),
            HttpError::TooLarge
        );

        assert!(matches!(
            read(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").await,
            Err(HttpError::Malformed(_))
        ));
        assert!(matches!(
            read(b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\nabc").await,
            Err(HttpError::Malformed(_))
        ));
        assert!(matches!(
            read(b"GET /\r\n\r\n").await,
            Err(HttpError::Malformed(_))
        ));
        assert_eq!(
            read(b"POST / HTTP/1.1\r\nContent-Length: 9\r\n\r\nabc")
                .await
                .unwrap_err(),
            HttpError::Closed
        );
    }

    #[test]
    fn a_response_says_how_long_it_is_and_closes() {
        let r = String::from_utf8(response(201, "application/sdp", b"v=0")).unwrap();
        assert!(r.starts_with("HTTP/1.1 201 Created\r\n"));
        assert!(r.contains("Content-Length: 3\r\n"));
        assert!(r.contains("Connection: close\r\n"));
        assert!(r.ends_with("\r\n\r\nv=0"));
    }
}
