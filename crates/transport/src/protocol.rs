//! Wire protocol for the SSH-multiplexed transport.
//!
//! Two channel kinds, each with its own framing:
//!
//! - **Control channel** (long-lived, one per peer session). Carries
//!   length-delimited JSON frames. The client writes a `RequestFrame`,
//!   the server replies with one or more `ResponseFrame`s correlated
//!   by `request_id`. Server may also push `ResponseFrame`s with
//!   `request_id == None` carrying `Response::Event` for SSE proxying.
//!   Frame size is bounded by `LengthDelimitedCodec`'s
//!   `max_frame_length` (1 MiB; see `control_codec`).
//!
//! - **Recv channel** (short-lived, one per replication step). Wire
//!   layout:
//!   ```text
//!   [ length-prefixed JSON RecvHeader ]
//!   [ raw zfs send byte stream ]
//!   <half-close>
//!   [ length-prefixed JSON Response (Ok / Error) ]
//!   ```
//!   The header is read with `recv_header_codec` (same length-delimited
//!   shape as control); the bulk bytes are then plain `tokio::io::copy`
//!   into the receiver's `zfs recv` stdin.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::LengthDelimitedCodec;

pub const PROTOCOL_VERSION: u32 = 1;

/// 1 MiB cap on any single JSON frame. Real frames are typically a few
/// hundred bytes; this is well above any benign expansion and keeps an
/// adversarial peer from exhausting memory before validation.
pub const MAX_FRAME_LEN: usize = 1 << 20;

// Backwards-compatibility alias for code paths that still talk about
// the header limit specifically. Same value as MAX_FRAME_LEN; kept for
// clarity at call sites that read RecvHeader.
pub const MAX_HEADER_LEN: usize = MAX_FRAME_LEN;

// ─── Control-channel framing ───────────────────────────────────────

/// Length-delimited codec configured for the control channel: 4-byte
/// big-endian length prefix, frames bounded by `MAX_FRAME_LEN`.
pub fn control_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_length(4)
        .max_frame_length(MAX_FRAME_LEN)
        .big_endian()
        .new_codec()
}

/// Same wire shape as `control_codec`; named separately so a future
/// recv-only tweak (smaller max_frame_length, for instance) can land
/// without churning every call site.
pub fn recv_header_codec() -> LengthDelimitedCodec {
    control_codec()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestFrame {
    /// Monotonic per-session, client-assigned. Server echoes back in the
    /// matching `ResponseFrame::request_id`.
    pub id: u64,
    #[serde(flatten)]
    pub body: Request,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseFrame {
    /// `Some(id)` for normal request/response correlation.
    /// `None` for server-pushed `Response::Event` frames routed to the
    /// peer's broadcast subscribers.
    pub request_id: Option<u64>,
    #[serde(flatten)]
    pub body: Response,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    ListSnapshots {
        dataset: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix_regex: Option<String>,
    },
    GetReceiveResumeToken {
        dataset: String,
    },
    DestroySnapshot {
        name: String,
    },
    /// Sender-driven cleanup: invoke `palimpsest::recv::abort_partial`
    /// on `dataset` before the next recv channel opens. Used when the
    /// planner picked Full / Incremental + discard against a stale
    /// receiver token.
    DiscardPartialRecv {
        dataset: String,
    },
    ListJobs,
    GetJobStatus {
        name: String,
    },
    WakeupJob {
        name: String,
    },
    /// Subscribe to event broadcasts. `since` is the last-seen
    /// `log_events.id` (see daemon::state::log_events); the server
    /// replays everything strictly greater.
    SubscribeEvents {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<u64>,
    },
    GetLogCursor,
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    ListSnapshotsOk {
        snapshots: Vec<SnapshotEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receive_resume_token: Option<String>,
    },
    GetReceiveResumeTokenOk {
        token: Option<String>,
    },
    DestroySnapshotOk,
    DiscardPartialRecvOk,
    ListJobsOk {
        jobs: Vec<JobStatusWire>,
    },
    GetJobStatusOk { job: JobStatusWire },
    WakeupJobOk,
    GetLogCursorOk {
        id: u64,
    },
    /// Recv-channel terminal response. Same shape on the control channel
    /// is reserved for "operation succeeded with no payload" replies
    /// where a richer Ok variant doesn't add value.
    Ok,
    /// Server-pushed event frame (control channel only;
    /// `ResponseFrame.request_id == None`).
    Event(EventWire),
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Request was malformed or referenced an unknown identifier.
    BadRequest,
    /// Caller is not allowed to perform the operation under the
    /// configured `[[allowed_clients]]` ACL.
    Unauthorized,
    /// Underlying ZFS operation failed. `message` carries the stderr
    /// excerpt classified upstream by palimpsest.
    Zfs,
    /// Dataset (or snapshot) referenced by the request does not exist.
    NotFound,
    /// Catch-all for I/O / serialization failures inside the handler.
    Internal,
}

/// Mirror of `arctern_api::JobStatus` but defined here so the transport
/// crate stays leaf (no dependency on `arctern_api` / `utoipa`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobStatusWire {
    pub name: String,
    pub kind: String,
    pub last_run: Option<String>,
    pub next_run: Option<String>,
    pub last_error: Option<String>,
}

/// One log event surfaced over the SSE bridge. Mirrors the
/// `daemon::state::log_events` row shape; the `id` field is the same
/// monotonic cursor used by `Request::SubscribeEvents { since }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventWire {
    pub id: u64,
    /// Unix seconds (sqlite stores INTEGER).
    pub timestamp: i64,
    pub level: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    pub message: String,
}

// ─── Recv-channel header ───────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecvHeader {
    pub version: u32,
    pub target_dataset: String,
    pub send: SendHeader,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendHeader {
    pub send_kind: SendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_snap: Option<SnapshotRef>,
    pub to_snap: SnapshotRef,
    pub flags: SendFlagsWire,
    /// Receiver-side directive: when true, run
    /// `palimpsest::recv::abort_partial` on `target_dataset` before
    /// spawning the new `zfs recv`. Set by the planner when a stale
    /// resume token is present on the receiver and the chosen plan is
    /// a fresh Full / Incremental rather than a continuation.
    /// Default `false` for forward compatibility — a sender that never
    /// writes the field still parses on a receiver that knows it.
    #[serde(default)]
    pub discard_partial_recv: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendKind {
    Full,
    Incremental,
    /// `zfs send -t <token>` resume of a prior partial recv. The wire
    /// `from_snap` is None and `to_snap` carries the decoded token's
    /// to-snapshot identity for logging only — the receiver does not
    /// validate it.
    Resume,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRef {
    pub name: String,
    /// ZFS snapshot GUID — wire-typed as `u64` to preserve values above
    /// `i64::MAX` (real-world ZFS GUIDs routinely exceed it). serde_json
    /// round-trips u64 exactly via its default integer parser.
    pub guid: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendFlagsWire {
    pub raw: bool,
    pub embedded: bool,
    pub compressed: bool,
    pub large_blocks: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub name: String,
    pub guid: u64,
    pub createtxg: u64,
}

/// Compile a `prefix_regex` string from a `Request::ListSnapshots`.
/// Lives in the transport crate so the `regex::` import stays out of
/// the daemon (constitution-IV grep gate).
pub fn compile_prefix_regex(s: Option<&str>) -> Result<Option<regex::Regex>, regex::Error> {
    s.map(regex::Regex::new).transpose()
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame length {len} exceeds limit {limit}")]
    FrameTooLarge { len: usize, limit: usize },
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u32),
    #[error("unexpected eof while reading frame")]
    UnexpectedEof,
}

// ─── Codec helpers (manual, to keep the API surface small) ─────────

/// Encode `value` as length-prefixed JSON and write it to `w`.
pub async fn write_frame<W, T>(w: &mut W, value: &T) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge {
            len: body.len(),
            limit: MAX_FRAME_LEN,
        });
    }
    let mut prefix = BytesMut::with_capacity(4 + body.len());
    prefix.put_u32(body.len() as u32);
    prefix.extend_from_slice(&body);
    let bytes: Bytes = prefix.freeze();
    w.write_all(&bytes).await?;
    Ok(())
}

/// Read a single length-prefixed JSON frame from `r` and parse it into
/// `T`. Returns `UnexpectedEof` if the stream closes mid-frame.
pub async fn read_frame<R, T>(r: &mut R) -> Result<T, ProtocolError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(ProtocolError::UnexpectedEof);
        }
        Err(e) => return Err(ProtocolError::Io(e)),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge {
            len,
            limit: MAX_FRAME_LEN,
        });
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok(serde_json::from_slice(&body)?)
}

// Backwards-compatible thin wrappers used by the legacy code paths.
// They are typed to the SSH-pivot frame shapes (RequestFrame / RecvHeader)
// rather than the deleted ReceiveHeader / ReceiveResponse pair.

pub async fn write_header<W: AsyncWrite + Unpin>(
    w: &mut W,
    h: &RecvHeader,
) -> Result<(), ProtocolError> {
    write_frame(w, h).await
}

pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<RecvHeader, ProtocolError> {
    let h: RecvHeader = read_frame(r).await?;
    if h.version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(h.version));
    }
    Ok(h)
}

pub async fn write_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    r: &ResponseFrame,
) -> Result<(), ProtocolError> {
    write_frame(w, r).await
}

pub async fn read_response<R: AsyncRead + Unpin>(r: &mut R) -> Result<ResponseFrame, ProtocolError> {
    read_frame(r).await
}

pub async fn write_request<W: AsyncWrite + Unpin>(
    w: &mut W,
    r: &RequestFrame,
) -> Result<(), ProtocolError> {
    write_frame(w, r).await
}

pub async fn read_request<R: AsyncRead + Unpin>(r: &mut R) -> Result<RequestFrame, ProtocolError> {
    read_frame(r).await
}

// ─── Compatibility shims (legacy LIST response shape) ──────────────
//
// These are kept as thin aliases so push.rs's planner module — which
// still imports them — keeps compiling. Step 9 deletes the imports;
// the aliases evaporate naturally then.

pub type ListResponse = LegacyListResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LegacyListResponse {
    Ok {
        snapshots: Vec<SnapshotEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receive_resume_token: Option<String>,
    },
    Error {
        message: String,
    },
}

pub async fn write_list_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    r: &LegacyListResponse,
) -> Result<(), ProtocolError> {
    write_frame(w, r).await
}

pub async fn read_list_response<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<LegacyListResponse, ProtocolError> {
    read_frame(r).await
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReceiveResponse {
    Ok,
    Error { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Send,
    List,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiveHeader {
    pub version: u32,
    #[serde(default = "default_op")]
    pub op: Op,
    pub target_dataset: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_regex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send: Option<SendHeader>,
}

fn default_op() -> Op {
    Op::Send
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn snap_ref() -> SnapshotRef {
        SnapshotRef {
            name: "tank/data@s1".into(),
            guid: 11587258101628135412,
        }
    }

    fn flags() -> SendFlagsWire {
        SendFlagsWire {
            raw: true,
            embedded: true,
            compressed: true,
            large_blocks: true,
        }
    }

    #[tokio::test]
    async fn recv_header_roundtrip() {
        let h = RecvHeader {
            version: PROTOCOL_VERSION,
            target_dataset: "tank/backups/laptop/data".into(),
            send: SendHeader {
                send_kind: SendKind::Full,
                from_snap: None,
                to_snap: snap_ref(),
                flags: flags(),
                discard_partial_recv: false,
            },
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
    }

    #[tokio::test]
    async fn recv_header_unsupported_version_rejected() {
        let h = RecvHeader {
            version: 99,
            target_dataset: "tank/x".into(),
            send: SendHeader {
                send_kind: SendKind::Full,
                from_snap: None,
                to_snap: snap_ref(),
                flags: flags(),
                discard_partial_recv: false,
            },
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_header(&mut cur).await.unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(99)));
    }

    #[tokio::test]
    async fn frame_too_large_rejected() {
        let mut buf = Vec::new();
        let oversize = (MAX_FRAME_LEN as u32 + 1).to_be_bytes();
        buf.extend_from_slice(&oversize);
        let mut cur = Cursor::new(buf);
        let err: Result<RecvHeader, _> = read_frame(&mut cur).await;
        assert!(matches!(
            err.unwrap_err(),
            ProtocolError::FrameTooLarge { .. }
        ));
    }

    fn check_request_roundtrip(req: Request) {
        let f = RequestFrame { id: 7, body: req.clone() };
        let s = serde_json::to_string(&f).unwrap();
        let back: RequestFrame = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, 7);
        assert_eq!(back.body, req);
    }

    #[test]
    fn request_list_snapshots_roundtrip() {
        check_request_roundtrip(Request::ListSnapshots {
            dataset: "tank/backups/laptop/data".into(),
            prefix_regex: Some("^zrepl_".into()),
        });
        check_request_roundtrip(Request::ListSnapshots {
            dataset: "tank/data".into(),
            prefix_regex: None,
        });
    }

    #[test]
    fn request_get_receive_resume_token_roundtrip() {
        check_request_roundtrip(Request::GetReceiveResumeToken {
            dataset: "tank/backups/laptop/data".into(),
        });
    }

    #[test]
    fn request_destroy_snapshot_roundtrip() {
        check_request_roundtrip(Request::DestroySnapshot {
            name: "tank/backups@old".into(),
        });
    }

    #[test]
    fn request_discard_partial_recv_roundtrip() {
        check_request_roundtrip(Request::DiscardPartialRecv {
            dataset: "tank/backups/laptop/data".into(),
        });
    }

    #[test]
    fn request_list_jobs_roundtrip() {
        check_request_roundtrip(Request::ListJobs);
    }

    #[test]
    fn request_get_job_status_roundtrip() {
        check_request_roundtrip(Request::GetJobStatus { name: "backup".into() });
    }

    #[test]
    fn request_wakeup_job_roundtrip() {
        check_request_roundtrip(Request::WakeupJob { name: "backup".into() });
    }

    #[test]
    fn request_subscribe_events_roundtrip() {
        check_request_roundtrip(Request::SubscribeEvents { since: Some(42) });
        check_request_roundtrip(Request::SubscribeEvents { since: None });
    }

    #[test]
    fn request_get_log_cursor_roundtrip() {
        check_request_roundtrip(Request::GetLogCursor);
    }

    #[test]
    fn request_shutdown_roundtrip() {
        check_request_roundtrip(Request::Shutdown);
    }

    fn check_response_roundtrip(resp: Response) {
        let f = ResponseFrame { request_id: Some(11), body: resp.clone() };
        let s = serde_json::to_string(&f).unwrap();
        let back: ResponseFrame = serde_json::from_str(&s).unwrap();
        assert_eq!(back.request_id, Some(11));
        assert_eq!(back.body, resp);
    }

    #[test]
    fn response_list_snapshots_roundtrip() {
        check_response_roundtrip(Response::ListSnapshotsOk {
            snapshots: vec![SnapshotEntry {
                name: "s1".into(),
                guid: 11587258101628135412,
                createtxg: 8,
            }],
            receive_resume_token: Some("1-deadbeef".into()),
        });
        check_response_roundtrip(Response::ListSnapshotsOk {
            snapshots: vec![],
            receive_resume_token: None,
        });
    }

    #[test]
    fn response_get_receive_resume_token_roundtrip() {
        check_response_roundtrip(Response::GetReceiveResumeTokenOk {
            token: Some("1-abc".into()),
        });
        check_response_roundtrip(Response::GetReceiveResumeTokenOk { token: None });
    }

    #[test]
    fn response_destroy_snapshot_roundtrip() {
        check_response_roundtrip(Response::DestroySnapshotOk);
    }

    #[test]
    fn response_discard_partial_recv_roundtrip() {
        check_response_roundtrip(Response::DiscardPartialRecvOk);
    }

    #[test]
    fn response_list_jobs_roundtrip() {
        check_response_roundtrip(Response::ListJobsOk {
            jobs: vec![JobStatusWire {
                name: "backup".into(),
                kind: "push".into(),
                last_run: Some("2026-05-09T00:00:00Z".into()),
                next_run: None,
                last_error: None,
            }],
        });
    }

    #[test]
    fn response_get_job_status_roundtrip() {
        check_response_roundtrip(Response::GetJobStatusOk {
            job: JobStatusWire {
                name: "backup".into(),
                kind: "push".into(),
                last_run: None,
                next_run: None,
                last_error: Some("boom".into()),
            },
        });
    }

    #[test]
    fn response_wakeup_ok_roundtrip() {
        check_response_roundtrip(Response::WakeupJobOk);
    }

    #[test]
    fn response_get_log_cursor_roundtrip() {
        check_response_roundtrip(Response::GetLogCursorOk { id: 12345 });
    }

    #[test]
    fn response_ok_roundtrip() {
        check_response_roundtrip(Response::Ok);
    }

    #[test]
    fn response_event_uses_none_request_id() {
        let f = ResponseFrame {
            request_id: None,
            body: Response::Event(EventWire {
                id: 1,
                timestamp: 1715212345,
                level: "INFO".into(),
                job_name: Some("backup".into()),
                message: "cycle ok".into(),
            }),
        };
        let s = serde_json::to_string(&f).unwrap();
        let back: ResponseFrame = serde_json::from_str(&s).unwrap();
        assert_eq!(back.request_id, None);
        assert_eq!(back, f);
    }

    #[test]
    fn response_error_roundtrip() {
        for code in [
            ErrorCode::BadRequest,
            ErrorCode::Unauthorized,
            ErrorCode::Zfs,
            ErrorCode::NotFound,
            ErrorCode::Internal,
        ] {
            check_response_roundtrip(Response::Error {
                code,
                message: "boom".into(),
            });
        }
    }

    #[tokio::test]
    async fn request_frame_wire_roundtrip() {
        let f = RequestFrame {
            id: 1,
            body: Request::ListSnapshots {
                dataset: "tank/data".into(),
                prefix_regex: None,
            },
        };
        let mut buf = Vec::new();
        write_request(&mut buf, &f).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back: RequestFrame = read_request(&mut cur).await.unwrap();
        assert_eq!(back, f);
    }

    #[tokio::test]
    async fn response_frame_wire_roundtrip() {
        let f = ResponseFrame {
            request_id: Some(1),
            body: Response::Ok,
        };
        let mut buf = Vec::new();
        write_response(&mut buf, &f).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back: ResponseFrame = read_response(&mut cur).await.unwrap();
        assert_eq!(back, f);
    }

    /// D19 risk verification (preserved from QUIC era): serde_json must
    /// round-trip a u64 GUID above i64::MAX exactly. The captured value
    /// is the real GUID of `tank/data@zrepl_001` from the palimpsest
    /// test VM.
    #[test]
    fn guid_above_i64_max_roundtrips() {
        let entry = SnapshotEntry {
            name: "zrepl_001".into(),
            guid: 11587258101628135412,
            createtxg: 8,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: SnapshotEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.guid, 11587258101628135412);
        assert_eq!(back, entry);
    }

    #[test]
    fn send_kind_resume_serializes_as_lowercase() {
        let s = serde_json::to_string(&SendKind::Resume).unwrap();
        assert_eq!(s, r#""resume""#);
    }
}
