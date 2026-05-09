//! Wire protocol for one receive operation over a QUIC bidirectional
//! stream.
//!
//! Layout:
//! ```text
//! [ u32 BE: header_length ]   // 4 bytes; rejected if > MAX_HEADER_LEN
//! [ JSON header bytes ]       // exactly header_length bytes
//! [ raw zfs send bytes ]      // until stream FIN
//! // server then writes:
//! [ JSON response bytes ]     // single line, no length prefix
//! // server finishes its send half
//! ```
//!
//! The response has no length prefix because the server's send half is
//! used for nothing else — the response is the only thing the receiver
//! ever writes back, and the FIN delimits it. Adding framing later
//! requires bumping `PROTOCOL_VERSION`.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_VERSION: u32 = 1;

/// 1 MiB cap on the JSON header. A real header is ~150 bytes; 1 MiB is
/// well above any expansion and well below "attacker can OOM the
/// receiver before validation".
pub const MAX_HEADER_LEN: usize = 1 << 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiveHeader {
    pub version: u32,
    pub target_dataset: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_flags: Option<SendFlags>,
}

/// Reserved for slice 005 — it will carry the resume-token / raw /
/// embedded / large-block / properties / replicate flags the planner
/// negotiates with the receiver. Empty for slice 004 so the wire
/// shape exists but encodes nothing.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SendFlags {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReceiveResponse {
    Ok,
    Error { message: String },
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn header_roundtrip() {
        let h = ReceiveHeader {
            version: 1,
            target_dataset: "tank/backups/laptop/data".into(),
            send_flags: None,
        };
        let mut buf = Vec::new();
        write_header(&mut buf, &h).await.unwrap();
        let mut cur = Cursor::new(buf);
        let back = read_header(&mut cur).await.unwrap();
        assert_eq!(back, h);
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
        // Hand-craft a length prefix above the cap; body bytes are
        // never read because validation short-circuits.
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
            target_dataset: "x".into(),
            send_flags: None,
        };
        let mut buf = Vec::new();
        // Hand-write so write_header doesn't validate (it doesn't anyway).
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
}
