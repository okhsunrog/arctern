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

/// `GET <socket>/api/v1/jobs`. Used by `stdinserver-dispatch` to proxy
/// a peer's ListJobs request into the local daemon — the two processes
/// share no state besides this socket.
pub async fn list_jobs(socket: &Path) -> Result<Vec<arctern_api::JobStatus>, ClientError> {
    let (status, body) = request(socket, Method::GET, "/api/v1/jobs", None).await?;
    if !status.is_success() {
        return Err(ClientError::Status {
            code: status.as_u16(),
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    Ok(serde_json::from_slice(&body)?)
}

/// `POST <socket>/api/v1/jobs/{name}/wakeup`. 204 on success; 404 maps
/// to `ClientError::Status { code: 404 }`.
pub async fn wakeup_job(socket: &Path, name: &str) -> Result<(), ClientError> {
    let path = format!("/api/v1/jobs/{}/wakeup", encode_segment(name));
    let (status, body) = request(socket, Method::POST, &path, None).await?;
    if !status.is_success() {
        return Err(ClientError::Status {
            code: status.as_u16(),
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    Ok(())
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
