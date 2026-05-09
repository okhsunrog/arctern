//! Peer-credential capture + same-uid enforcement for AF_UNIX connections.
//!
//! Slice 002 binds a UNIX socket and trusts `SO_PEERCRED`. The `Connected`
//! impl captures the peer uid at accept time; `enforce_same_uid` rejects
//! any request whose connection's peer uid does not match the daemon's
//! effective uid. Layered on the whole router so every route inherits the
//! check by construction (no opt-in, no opt-out).

use arctern_api::ApiErrorBody;
use axum::{
    Json,
    extract::{ConnectInfo, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    serve::IncomingStream,
};
use axum::extract::connect_info::Connected;
use tokio::net::UnixListener;

#[derive(Clone, Debug)]
pub struct PeerCredentials {
    pub uid: u32,
}

impl Connected<IncomingStream<'_, UnixListener>> for PeerCredentials {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let cred = stream
            .io()
            .peer_cred()
            .expect("SO_PEERCRED is always available on AF_UNIX");
        Self { uid: cred.uid() }
    }
}

/// Same-uid policy: a request whose connection's peer uid differs from the
/// daemon's effective uid is rejected with `403`. There is no allowlist
/// this slice — multi-uid policies land in a future slice. Wired in via
/// `axum::middleware::from_fn`.
pub async fn enforce_same_uid(
    ConnectInfo(peer): ConnectInfo<PeerCredentials>,
    request: Request,
    next: Next,
) -> Response {
    // SAFETY: `geteuid` is a vDSO syscall; cannot fail.
    let daemon_uid = unsafe { libc::geteuid() };
    if peer.uid != daemon_uid {
        let body = ApiErrorBody {
            error: "peer_uid_mismatch".into(),
            message: format!(
                "peer uid {} is not allowed (daemon uid {daemon_uid})",
                peer.uid
            ),
        };
        return (StatusCode::FORBIDDEN, Json(body)).into_response();
    }
    next.run(request).await
}
