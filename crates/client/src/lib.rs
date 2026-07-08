//! Thin async client over the arctern HTTP API.
//!
//! Speaks HTTP/1.1 over a UNIX domain socket — slice 002 binds the daemon
//! to a UDS, and `reqwest` 0.13 has no first-class UDS transport. The
//! implementation is intentionally tiny: open a fresh `UnixStream` per
//! call, run hyper's low-level `http1::handshake`, send one request,
//! collect the body. No connection pooling — sufficient for slice 002's
//! load (handful of test requests + future low-rate admin actions). Add
//! pooling when a high-rate consumer demands it.

use std::path::Path;

use arctern_api::{CreateSnapshotRequest, DatasetSummary};
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use thiserror::Error;
use tokio::net::UnixStream;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("connect to unix socket: {0}")]
    Io(#[from] std::io::Error),

    #[error("hyper transport: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("build request: {0}")]
    Http(#[from] http::Error),

    #[error("decode response body: {0}")]
    Decode(#[from] serde_json::Error),

    /// Non-2xx response. Callers detect 409 (snapshot already exists) by
    /// matching on `code == 409`; that is the documented contract.
    #[error("server returned status {code}: {body}")]
    Status { code: u16, body: String },
}

/// `GET <socket>/api/v1/datasets`.
pub async fn list_datasets(socket: &Path) -> Result<Vec<DatasetSummary>, ClientError> {
    let (status, body) = request(socket, Method::GET, "/api/v1/datasets", None).await?;
    if !status.is_success() {
        return Err(ClientError::Status {
            code: status.as_u16(),
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    Ok(serde_json::from_slice(&body)?)
}

/// `POST <socket>/api/v1/datasets/{dataset}/snapshots`. `dataset` is
/// path-encoded by this helper (only `/` needs escaping for ZFS names).
pub async fn create_snapshot(
    socket: &Path,
    dataset: &str,
    req: &CreateSnapshotRequest,
) -> Result<DatasetSummary, ClientError> {
    let path = format!("/api/v1/datasets/{}/snapshots", encode_segment(dataset));
    let body = serde_json::to_vec(req)?;
    let (status, response) = request(socket, Method::POST, &path, Some(body)).await?;
    if status != StatusCode::CREATED {
        return Err(ClientError::Status {
            code: status.as_u16(),
            body: String::from_utf8_lossy(&response).into_owned(),
        });
    }
    Ok(serde_json::from_slice(&response)?)
}

/// Raw passthrough for the stdinserver's generic proxy: forward one
/// request to the daemon's UDS and return `(status, body)` verbatim.
/// The caller owns method/path validation.
pub async fn raw(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<(u16, Bytes), ClientError> {
    let method = Method::from_bytes(method.as_bytes()).map_err(|_| ClientError::Status {
        code: 405,
        body: format!("unsupported method {method:?}"),
    })?;
    let (status, bytes) = request(socket, method, path, body).await?;
    Ok((status.as_u16(), bytes))
}

/// Open a streaming GET (the daemon's SSE endpoint) and forward each
/// `data: <json>` payload line into the returned channel. The
/// connection lives until the receiver is dropped or the daemon closes
/// the stream. Used by the stdinserver events channel to bridge the
/// daemon's event bus onto its stdout.
pub async fn stream_sse_data(
    socket: &Path,
    path: &str,
) -> Result<tokio::sync::mpsc::Receiver<String>, ClientError> {
    use http_body_util::BodyExt as _;
    let stream = UnixStream::connect(socket).await?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing_log_or_eprintln(format_args!("hyper connection: {e}"));
        }
    });
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("host", "_")
        .body(Full::new(Bytes::new()))?;
    let response = sender.send_request(req).await?;
    if !response.status().is_success() {
        return Err(ClientError::Status {
            code: response.status().as_u16(),
            body: format!("streaming GET {path}"),
        });
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    tokio::spawn(async move {
        // `sender` must outlive the body or hyper aborts the request.
        let _sender = sender;
        let mut body = response.into_body();
        let mut buf = String::new();
        while let Some(frame) = body.frame().await {
            let Ok(frame) = frame else { break };
            let Some(data) = frame.data_ref() else {
                continue;
            };
            buf.push_str(&String::from_utf8_lossy(data));
            // SSE events are separated by blank lines; payload lines
            // are `data: {...}`. Keep-alive comments start with ':'.
            while let Some(nl) = buf.find('\n') {
                let line: String = buf.drain(..=nl).collect();
                let line = line.trim_end();
                if let Some(payload) = line.strip_prefix("data: ")
                    && tx.send(payload.to_string()).await.is_err()
                {
                    return;
                }
            }
        }
    });
    Ok(rx)
}

async fn request(
    socket: &Path,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<(StatusCode, Bytes), ClientError> {
    let stream = UnixStream::connect(socket).await?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    // Drive the connection in the background; drop sender to close it.
    tokio::spawn(async move {
        // Connection close after a single request is the expected shape
        // here; an Err on close-by-peer is normal, so this log is debug.
        if let Err(e) = conn.await {
            tracing_log_or_eprintln(format_args!("hyper connection: {e}"));
        }
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "_");
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    let req = builder.body(Full::new(Bytes::from(body.unwrap_or_default())))?;

    let response = sender.send_request(req).await?;
    let status = response.status();
    let bytes = response.into_body().collect().await?.to_bytes();
    Ok((status, bytes))
}

/// Percent-encode the only character ZFS dataset names contain that
/// would split a URL path (`/`). Everything else legal in a ZFS name is
/// already URL-path-safe.
fn encode_segment(s: &str) -> String {
    s.replace('/', "%2F")
}

// `tracing` would be nice but adding it as a dep just for one log line
// in the connection-close callback is overkill. Use eprintln when no
// subscriber is around.
fn tracing_log_or_eprintln(args: std::fmt::Arguments<'_>) {
    eprintln!("{args}");
}
