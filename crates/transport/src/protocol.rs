//! Wire protocol for the SSH-multiplexed transport.
//!
//! Two channel kinds, both framed as `[u32 big-endian length][JSON body]`
//! via the `read_frame`/`write_frame` helpers below (frames bounded by
//! `MAX_FRAME_LEN`):
//!
//! - **Control channel** RPC lives in `crate::control` (tarpc service
//!   over the same length-delimited JSON framing). This module keeps
//!   only what the raw channels share.
//!
//! - **Recv channel** (short-lived, one per replication step). Wire
//!   layout:
//!   ```text
//!   [ length-prefixed JSON RecvHeader ]
//!   [ raw zfs send byte stream ]
//!   <half-close>
//!   [ length-prefixed JSON Response (Ok / Error) ]
//!   ```
//!   The bulk bytes after the header are plain `tokio::io::copy` into
//!   the receiver's `zfs recv` stdin.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;

/// 8 MiB cap on any single JSON frame. Real control frames are typically
/// a few hundred bytes; the headroom covers large snapshot inventories
/// (`ListSnapshotsOk` over datasets with many thousands of snapshots)
/// while still bounding what a mutually-authenticated peer can make the
/// reader allocate before validation. The planner uses the leaner
/// `ListReceiverGuids` so the critical path stays far below this.
pub const MAX_FRAME_LEN: usize = 8 << 20;

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
pub enum Response {
    /// Recv-channel terminal response: the stream was received and
    /// finalised cleanly.
    Ok,
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

/// One log event, as carried on the dedicated events channel: the
/// stream is newline-delimited JSON `EventWire` lines (no framing, no
/// request ids — events are a stream, not RPC). Mirrors the
/// `daemon::state::log_events` row shape; `id` is the SQLite cursor.
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
    /// `from_snap` is None and `to_snap` carries the snapshot *leaf*
    /// name (part after `@`) plus GUID from the decoded token — the
    /// receiver validates it like any other snapshot leaf and uses it
    /// for the last-received hold after the resumed recv completes.
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

pub async fn read_response<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<ResponseFrame, ProtocolError> {
    read_frame(r).await
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

    fn check_response_roundtrip(resp: Response) {
        let f = ResponseFrame {
            request_id: Some(11),
            body: resp.clone(),
        };
        let s = serde_json::to_string(&f).unwrap();
        let back: ResponseFrame = serde_json::from_str(&s).unwrap();
        assert_eq!(back.request_id, Some(11));
        assert_eq!(back.body, resp);
    }

    #[test]
    fn response_ok_roundtrip() {
        check_response_roundtrip(Response::Ok);
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
