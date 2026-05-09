//! Outbound SSH peer link. The laptop's daemon owns one `PeerLink` per
//! `[[peers]]` entry. Each link multiplexes:
//!
//! - one **control** channel: a long-lived `arctern stdinserver-dispatch
//!   <id>` child carrying length-delimited Request/Response/Event frames.
//!   Owned by `ControlClient`'s background task.
//! - zero or more **recv** channels: short-lived children spawned per
//!   replication step via `PeerLink::open_recv`.
//!
//! A single `openssh::Session` (with ControlMaster) backs both. Recv
//! channels are returned to the caller (the push executor) which owns
//! their lifetime; the control channel runs forever inside the link.
//!
//! Reconnect lives in `reconnect.rs` and runs eagerly in a background
//! task per peer per ARCHITECTURE.md: 1s, 2s, 4s, ... capped at 60s.

pub mod control;
pub mod reconnect;

use std::sync::Arc;

use arctern_transport::{Request, Response};
use thiserror::Error;
use tokio::sync::oneshot;

pub use control::{ControlClient, RpcError};

/// Outbound link to one peer. Cheaply cloneable — handlers and the
/// scheduler share the same Arc.
#[derive(Clone)]
#[allow(dead_code)] // Constructor + recv plumbing land in step 9.
pub struct PeerLink {
    pub(crate) name: String,
    pub(crate) session: Arc<openssh::Session>,
    pub(crate) control: ControlClient,
}

#[allow(dead_code)] // Methods land alongside step 9/10 wiring.
impl PeerLink {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Send a Request on the control channel and await its matching
    /// Response. Returns immediately with `RpcError::ChannelClosed`
    /// when the background task has died (e.g. session lost; reconnect
    /// is in progress) — handlers should map that to HTTP 503 with
    /// Retry-After per ARCHITECTURE.md "UI federation".
    #[allow(dead_code)]
    pub async fn rpc(&self, req: Request) -> Result<Response, RpcError> {
        self.control.send(req).await
    }
}

#[derive(Debug, Error)]
#[allow(dead_code)] // Constructed by handlers/peers (step 10) and push executor (step 9).
pub enum PeerError {
    #[error("ssh session: {0}")]
    Session(#[from] openssh::Error),
    #[error("control channel: {0}")]
    Control(#[from] RpcError),
}

/// One in-flight RPC. The control task holds the `tx` half until the
/// matching ResponseFrame arrives; the caller awaits `rx`.
pub(crate) type Pending = oneshot::Sender<Result<Response, RpcError>>;
