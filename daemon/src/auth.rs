//! Peer-credential capture for AF_UNIX connections.
//!
//! Slice 002 binds a UNIX socket and trusts `SO_PEERCRED`. This module
//! captures the connecting peer's uid via axum's `Connected` plumbing so a
//! later layer (added in T003) can enforce a same-uid policy uniformly
//! across every route.

use axum::extract::connect_info::Connected;
use axum::serve::IncomingStream;
use tokio::net::UnixListener;

/// Per-connection peer credentials captured at accept time. Stored on the
/// request's `ConnectInfo` so middleware can inspect it without re-reading
/// `SO_PEERCRED` per request.
#[derive(Clone, Debug)]
pub struct PeerCredentials {
    // `dead_code` until T003 wires the enforcement layer. Removed there.
    #[allow(dead_code)]
    pub uid: u32,
}

impl Connected<IncomingStream<'_, UnixListener>> for PeerCredentials {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        // `SO_PEERCRED` is kernel-guaranteed for AF_UNIX on Linux — failure
        // here would indicate a kernel bug, not a recoverable error.
        let cred = stream
            .io()
            .peer_cred()
            .expect("SO_PEERCRED is always available on AF_UNIX");
        Self {
            uid: cred.uid(),
        }
    }
}
