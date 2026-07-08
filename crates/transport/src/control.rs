//! Control-channel RPC service. The trait is the wire contract; tarpc
//! generates the client (`ArcternControlClient`) and the server glue
//! (`serve()`), replacing the hand-rolled request-id demux this crate
//! used to carry. Framing stays `LengthDelimitedCodec` + JSON (see
//! `transport()`), so the payloads remain inspectable in logs.
//!
//! The method set states the plane split: `list_receiver_guids` /
//! `discard_partial_recv` are the replication core and must work with
//! no daemon on the receiver; `log_cursor` doubles as the liveness
//! probe; `proxy` is the management plane and requires the receiver's
//! daemon. Events are NOT here — they ride their own stream channel.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

pub use crate::protocol::ErrorCode;

/// Application-level failure carried inside a successful RPC exchange.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct WireError {
    pub code: ErrorCode,
    pub message: String,
}

impl WireError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Reply to `list_receiver_guids`: what the planner intersects on.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GuidsReply {
    pub guids: Vec<u64>,
    pub receive_resume_token: Option<String>,
}

/// Reply to `proxy`: the local daemon's HTTP status + raw body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyReply {
    pub status: u16,
    pub body: String,
}

#[tarpc::service]
pub trait ArcternControl {
    /// Receiver snapshot GUIDs (optionally name-filtered) plus the
    /// receive resume token, root_fs-scoped. Deliberately lean so the
    /// response stays small for datasets with tens of thousands of
    /// snapshots.
    async fn list_receiver_guids(
        dataset: String,
        prefix_regex: Option<String>,
    ) -> Result<GuidsReply, WireError>;
    /// `zfs recv -A` on the target: drop stale partial-receive state
    /// before a fresh full/incremental replaces it.
    async fn discard_partial_recv(dataset: String) -> Result<(), WireError>;
    /// Current `log_events` cursor (0 without SQLite). Cheap and
    /// daemon-independent — also serves as the link liveness probe.
    async fn log_cursor() -> u64;
    /// Generic passthrough into the receiver's local daemon HTTP API.
    /// GET rides the control read scope; mutating methods require the
    /// explicit `control:proxy_admin` ACL grant.
    async fn proxy(
        method: String,
        path: String,
        body: Option<String>,
    ) -> Result<ProxyReply, WireError>;
}

/// Wrap a bidirectional byte stream (the ssh channel's stdio) into the
/// tarpc transport both ends use: length-delimited frames, JSON codec.
/// `Item`/`SinkItem` differ between client and server, hence generic.
pub fn transport<S, Item, SinkItem>(
    io: S,
) -> tarpc::serde_transport::Transport<
    S,
    Item,
    SinkItem,
    tarpc::tokio_serde::formats::Json<Item, SinkItem>,
>
where
    S: AsyncRead + AsyncWrite,
    Item: for<'de> Deserialize<'de>,
    SinkItem: Serialize,
{
    // Same ceiling as the raw channels' framing (`MAX_FRAME_LEN`): a
    // control payload larger than this is a bug, not a workload.
    let codec = tokio_util::codec::LengthDelimitedCodec::builder()
        .max_frame_length(crate::protocol::MAX_FRAME_LEN)
        .new_codec();
    let framed = tokio_util::codec::Framed::new(io, codec);
    tarpc::serde_transport::new(framed, tarpc::tokio_serde::formats::Json::default())
}
