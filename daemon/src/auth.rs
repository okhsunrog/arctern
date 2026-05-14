//! Peer-credential capture + same-uid enforcement for AF_UNIX connections.
//!
//! Slice 002 binds a UNIX socket and trusts `SO_PEERCRED`. The `Connected`
//! impl captures the peer uid at accept time; `enforce_same_uid` rejects
//! any request whose connection's peer uid does not match the daemon's
//! effective uid. Layered on the whole router so every route inherits the
//! check by construction (no opt-in, no opt-out).

use arctern_api::ApiErrorBody;
use axum::extract::connect_info::Connected;
use axum::{
    Json,
    extract::{ConnectInfo, Request},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    serve::IncomingStream,
};
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

/// CSRF guard for the loopback TCP bind. Mutating methods (POST / PUT /
/// PATCH / DELETE) are blocked when the browser-supplied
/// `Sec-Fetch-Site` header indicates a cross-origin request — that
/// header is always present on modern browser-issued fetches, always
/// trustworthy (a page cannot forge it cross-origin), and absent on
/// non-browser callers (curl, `arctern-client`, `reqwest`).
///
/// The rule:
/// - GET / HEAD / OPTIONS — always allowed (no side effects).
/// - Mutating method + `Sec-Fetch-Site: same-origin` or `none` —
///   allowed.
/// - Mutating method + `Sec-Fetch-Site: same-site` or `cross-site` —
///   403.
/// - Mutating method + header absent — allowed (assumed to be a
///   non-browser CLI / library client).
///
/// CSRF threat model: a malicious page in another tab fetches into
/// `127.0.0.1:7878` with the user's "credentials" (which here are
/// just being on the same host). Sec-Fetch-Site blocks this because
/// the browser cannot be tricked into omitting or rewriting the
/// header from a cross-origin context.
pub async fn enforce_csrf(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let mutating = matches!(
        method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if mutating && let Some(sfs) = request.headers().get("sec-fetch-site") {
        let v = sfs.to_str().unwrap_or("");
        if v != "same-origin" && v != "none" {
            let body = ApiErrorBody {
                error: "cross_origin".into(),
                message: format!("cross-origin {method} blocked (Sec-Fetch-Site: {v})"),
            };
            return (StatusCode::FORBIDDEN, Json(body)).into_response();
        }
    }
    next.run(request).await
}
