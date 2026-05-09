//! Wire protocol for one operation over a QUIC bidirectional stream.
//!
//! Two operation kinds, dispatched via the header's `op` field:
//!
//! - `op = "send"` (slice 004; default for backward compat):
//!   ```text
//!   [ u32 BE: header_length ]   // 4 bytes; rejected if > MAX_HEADER_LEN
//!   [ JSON header bytes ]       // ReceiveHeader { op: Send, ..., send: Some(SendHeader) }
//!   [ raw zfs send bytes ]      // until stream FIN
//!   // server then writes:
//!   [ JSON ReceiveResponse ]    // single line, no length prefix
//!   // server finishes its send half
//!   ```
//! - `op = "list"` (slice 005):
//!   ```text
//!   [ u32 BE: header_length ]
//!   [ JSON header bytes ]       // ReceiveHeader { op: List, target_dataset, prefix_regex }
//!   // client finishes its send half (no bulk bytes)
//!   // server then writes:
//!   [ JSON ListResponse ]       // single line, no length prefix
//!   // server finishes its send half
//!   ```
//!
//! The `op` field deserializes with `#[serde(default = "default_op")]` to
//! `Op::Send` so a slice-004 header (no `op` key) parses cleanly on a
//! slice-005 sink. Adding a third op in a later slice is additive and
//! does NOT bump `PROTOCOL_VERSION` — the version envelope is for
//! backward-incompatible framing changes, not for vocabulary growth.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;

/// 1 MiB cap on the JSON header. A real header is ~150 bytes; 1 MiB is
/// well above any expansion and well below "attacker can OOM the
/// receiver before validation".
pub const MAX_HEADER_LEN: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Send,
    List,
}

fn default_op() -> Op {
    Op::Send
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiveHeader {
    pub version: u32,
    /// Operation kind. Slice-004 headers omit this field; deserialization
    /// defaults to `Op::Send` so old senders keep working against new
    /// sinks.
    #[serde(default = "default_op")]
    pub op: Op,
    pub target_dataset: String,
    /// Present only when `op == List`. None means no filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_regex: Option<String>,
    /// Present only when `op == Send`. Carries the kind (Full/Incremental),
    /// the from/to snapshot identities (by name + GUID), and the wire
    /// flags. Slice-005 receivers log these; future slices may
    /// consult them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send: Option<SendHeader>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendHeader {
    pub send_kind: SendKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_snap: Option<SnapshotRef>,
    pub to_snap: SnapshotRef,
    pub flags: SendFlagsWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendKind {
    Full,
    Incremental,
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
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReceiveResponse {
    Ok,
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ListResponse {
    Ok { snapshots: Vec<SnapshotEntry> },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub name: String,
    pub guid: u64,
    pub createtxg: u64,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("header length {len} exceeds limit {limit}")]
    HeaderTooLarge { len: usize, limit: usize },
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u32),
}

pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> Result<ReceiveHeader, ProtocolError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_HEADER_LEN {
        return Err(ProtocolError::HeaderTooLarge {
            len,
            limit: MAX_HEADER_LEN,
        });
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    let header: ReceiveHeader = serde_json::from_slice(&body)?;
    if header.version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(header.version));
    }
    Ok(header)
}

pub async fn write_header<W: AsyncWrite + Unpin>(
    w: &mut W,
    h: &ReceiveHeader,
) -> Result<(), ProtocolError> {
    let body = serde_json::to_vec(h)?;
    if body.len() > MAX_HEADER_LEN {
        return Err(ProtocolError::HeaderTooLarge {
            len: body.len(),
            limit: MAX_HEADER_LEN,
        });
    }
    let len = u32::try_from(body.len()).expect("MAX_HEADER_LEN fits in u32");
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&body).await?;
    Ok(())
}

pub async fn write_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    resp: &ReceiveResponse,
) -> Result<(), ProtocolError> {
    let body = serde_json::to_vec(resp)?;
    w.write_all(&body).await?;
    Ok(())
}

/// Read the whole response from `r` until EOF (i.e., until the peer's
/// send half is FINned). Used by the slice-005 client and by the
/// integration test.
pub async fn read_response<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<ReceiveResponse, ProtocolError> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).await?;
    let resp: ReceiveResponse = serde_json::from_slice(&buf)?;
    Ok(resp)
}

pub async fn write_list_response<W: AsyncWrite + Unpin>(
    w: &mut W,
    resp: &ListResponse,
) -> Result<(), ProtocolError> {
    let body = serde_json::to_vec(resp)?;
    w.write_all(&body).await?;
    Ok(())
}

pub async fn read_list_response<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<ListResponse, ProtocolError> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).await?;
    let resp: ListResponse = serde_json::from_slice(&buf)?;
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn header_roundtrip_send_default() {
        let h = ReceiveHeader {
            version: 1,
            op: Op::Send,
            target_dataset: "tank/backups/laptop/data".into(),
            prefix_regex: None,
            send: None,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
    }

    #[tokio::test]
    async fn op_field_defaults_to_send_when_absent() {
        // Hand-craft a slice-004-shaped header (no `op` key) and confirm
        // it parses with `op = Send` so old senders keep working.
        let body = br#"{"version":1,"target_dataset":"tank/backups/data"}"#;
        let mut buf = Vec::new();
        let len = (body.len() as u32).to_be_bytes();
        buf.extend_from_slice(&len);
        buf.extend_from_slice(body);
        let mut cur = Cursor::new(buf);
        let h = read_header(&mut cur).await.unwrap();
        assert_eq!(h.op, Op::Send);
        assert_eq!(h.target_dataset, "tank/backups/data");
        assert!(h.prefix_regex.is_none());
        assert!(h.send.is_none());
    }

    #[tokio::test]
    async fn header_with_op_list_roundtrip() {
        let h = ReceiveHeader {
            version: 1,
            op: Op::List,
            target_dataset: "tank/backups/laptop/okdata/data/home".into(),
            prefix_regex: Some("^zrepl_.*".into()),
            send: None,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
    }

    #[tokio::test]
    async fn header_with_send_full_roundtrip() {
        let h = ReceiveHeader {
            version: 1,
            op: Op::Send,
            target_dataset: "tank/sink/data".into(),
            prefix_regex: None,
            send: Some(SendHeader {
                send_kind: SendKind::Full,
                from_snap: None,
                to_snap: SnapshotRef {
                    name: "test_001".into(),
                    guid: 11587258101628135412,
                },
                flags: SendFlagsWire {
                    raw: true,
                    embedded: true,
                    compressed: true,
                    large_blocks: true,
                },
            }),
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
    }

    #[tokio::test]
    async fn header_with_send_incremental_roundtrip() {
        let h = ReceiveHeader {
            version: 1,
            op: Op::Send,
            target_dataset: "tank/sink/data".into(),
            prefix_regex: None,
            send: Some(SendHeader {
                send_kind: SendKind::Incremental,
                from_snap: Some(SnapshotRef {
                    name: "test_001".into(),
                    guid: 1711743136468914064,
                }),
                to_snap: SnapshotRef {
                    name: "test_002".into(),
                    guid: 14719774020884296672,
                },
                flags: SendFlagsWire {
                    raw: false,
                    embedded: true,
                    compressed: false,
                    large_blocks: true,
                },
            }),
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
    }

    #[tokio::test]
    async fn list_response_roundtrip_ok() {
        let r = ListResponse::Ok {
            snapshots: vec![
                SnapshotEntry {
                    name: "test_001".into(),
                    guid: 11587258101628135412,
                    createtxg: 8,
                },
                SnapshotEntry {
                    name: "test_002".into(),
                    guid: 1711743136468914064,
                    createtxg: 9,
                },
            ],
        };
        let mut buf = Vec::new();
        write_list_response(&mut buf, &r).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_list_response(&mut cur).await.unwrap();
        assert_eq!(back, r);
    }

    #[tokio::test]
    async fn list_response_roundtrip_error() {
        let r = ListResponse::Error {
            message: "permission denied".into(),
        };
        let mut buf = Vec::new();
        write_list_response(&mut buf, &r).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_list_response(&mut cur).await.unwrap();
        assert_eq!(back, r);
    }

    #[tokio::test]
    async fn response_roundtrip_ok() {
        let r = ReceiveResponse::Ok;
        let mut buf = Vec::new();
        write_response(&mut buf, &r).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_response(&mut cur).await.unwrap();
        assert_eq!(back, r);
    }

    #[tokio::test]
    async fn response_roundtrip_error() {
        let r = ReceiveResponse::Error {
            message: "recv failed: cannot receive incremental stream".into(),
        };
        let mut buf = Vec::new();
        write_response(&mut buf, &r).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_response(&mut cur).await.unwrap();
        assert_eq!(back, r);
    }

    #[tokio::test]
    async fn header_too_large_rejected() {
        let mut buf = Vec::new();
        let oversize = (MAX_HEADER_LEN as u32 + 1).to_be_bytes();
        buf.extend_from_slice(&oversize);
        let mut cur = Cursor::new(buf);
        let err = read_header(&mut cur).await.unwrap_err();
        assert!(matches!(err, ProtocolError::HeaderTooLarge { .. }));
    }

    #[tokio::test]
    async fn unsupported_version_rejected() {
        let h = ReceiveHeader {
            version: 2,
            op: Op::Send,
            target_dataset: "x".into(),
            prefix_regex: None,
            send: None,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let err = read_header(&mut cur).await.unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(2)));
    }

    #[test]
    fn response_serializes_with_status_tag() {
        let r = ReceiveResponse::Ok;
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#"{"status":"ok"}"#);
        let r = ReceiveResponse::Error {
            message: "x".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#"{"status":"error","message":"x"}"#);
    }

    #[test]
    fn list_response_serializes_with_status_tag() {
        let r = ListResponse::Ok {
            snapshots: vec![SnapshotEntry {
                name: "s".into(),
                guid: 1,
                createtxg: 2,
            }],
        };
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, r#"{"status":"ok","snapshots":[{"name":"s","guid":1,"createtxg":2}]}"#);
    }

    /// D19 risk verification: serde_json must round-trip a u64 GUID
    /// above i64::MAX exactly. The captured value is the real GUID of
    /// `tank/data@zrepl_001` from the palimpsest test VM.
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
}
