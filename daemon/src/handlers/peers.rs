//! Peer-aware handlers. `GET /api/v1/peers` enumerates configured peers
//! with their current reachability; the rest proxy a Request over the
//! peer's control channel and translate the typed Response to HTTP.

use std::sync::Arc;

use arctern_api::{ApiErrorBody, LogEvent, PeerReachability, PeerRoute, PeerSummary};
use arctern_transport::{ErrorCode, Request, Response};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
};
use time::format_description::well_known::Rfc3339;

use crate::app_state::AppState;
use crate::peer::PeerLink;
use crate::peer::state::PeerStatus;

#[utoipa::path(
    get,
    path = "/api/v1/peers",
    tag = "peers",
    responses(
        (status = 200, description = "Configured peers with reachability",
         body = Vec<PeerSummary>),
    ),
)]
pub async fn list_peers(State(state): State<AppState>) -> Json<Vec<PeerSummary>> {
    let g = state.peers.read().await;
    let mut out: Vec<PeerSummary> = g
        .values()
        .map(|e| PeerSummary {
            name: e.name.clone(),
            reachability: render_reachability(&e.status),
            active_route: e.active_route.clone(),
            routes: e.routes.iter().map(render_route).collect(),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Json(out)
}

fn render_route(r: &crate::peer::state::RouteState) -> PeerRoute {
    use crate::peer::state::RouteHealth;
    let (health, last_error) = match &r.health {
        RouteHealth::Unknown => ("unknown", None),
        RouteHealth::Connected => ("connected", None),
        RouteHealth::Failed { last_error } => ("failed", Some(last_error.clone())),
    };
    PeerRoute {
        name: r.name.clone(),
        ssh_target: r.ssh_target.clone(),
        auto: r.auto,
        health: health.into(),
        last_error,
        last_checked: r.last_checked.and_then(|t| t.format(&Rfc3339).ok()),
    }
}

fn render_reachability(s: &PeerStatus) -> PeerReachability {
    match s {
        PeerStatus::Connected => PeerReachability::Connected,
        PeerStatus::Reconnecting { since } => PeerReachability::Reconnecting {
            since: since.format(&Rfc3339).unwrap_or_default(),
        },
        PeerStatus::Failed { since, last_error } => PeerReachability::Failed {
            since: since.format(&Rfc3339).unwrap_or_default(),
            last_error: last_error.clone(),
        },
    }
}

/// Look up a peer's PeerLink. On miss / not connected, returns 503 with
/// a Retry-After hint matching the reconnect cap.
async fn require_link(
    state: &AppState,
    peer: &str,
) -> Result<Arc<PeerLink>, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let g = state.peers.read().await;
    if let Some(entry) = g.get(peer) {
        if let Some(link) = &entry.link {
            return Ok(link.clone());
        }
        return Err(unavailable(format!(
            "peer {peer:?} is not currently connected"
        )));
    }
    Err((
        StatusCode::NOT_FOUND,
        HeaderMap::new(),
        Json(ApiErrorBody {
            error: "peer_not_found".into(),
            message: format!("no peer named {peer:?}"),
        }),
    ))
}

fn unavailable(message: String) -> (StatusCode, HeaderMap, Json<ApiErrorBody>) {
    let mut headers = HeaderMap::new();
    // Match the reconnect cap so a polling client backs off appropriately.
    headers.insert("Retry-After", HeaderValue::from_static("60"));
    (
        StatusCode::SERVICE_UNAVAILABLE,
        headers,
        Json(ApiErrorBody {
            error: "peer_unavailable".into(),
            message,
        }),
    )
}

fn rpc_error_to_http(message: String) -> (StatusCode, HeaderMap, Json<ApiErrorBody>) {
    unavailable(format!("peer rpc: {message}"))
}

fn map_response_error(
    code: ErrorCode,
    message: String,
) -> (StatusCode, HeaderMap, Json<ApiErrorBody>) {
    let (status, kind) = match code {
        ErrorCode::BadRequest => (StatusCode::BAD_REQUEST, "bad_request"),
        ErrorCode::Unauthorized => (StatusCode::FORBIDDEN, "unauthorized"),
        ErrorCode::Zfs => (StatusCode::INTERNAL_SERVER_ERROR, "zfs"),
        ErrorCode::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        ErrorCode::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    (
        status,
        HeaderMap::new(),
        Json(ApiErrorBody {
            error: kind.into(),
            message,
        }),
    )
}

/// Any-method passthrough `/api/v1/peers/{peer}/proxy/{*rest}` → the
/// peer's local daemon API. The UI reuses its own generated client
/// with a per-peer base path, so a peer's console IS the local console
/// pointed elsewhere. Registered as a plain axum route (wildcards
/// don't fit the OpenAPI codegen; the client never needs a schema for
/// its own mirrored endpoints). SSE is excluded — events keep their
/// dedicated streaming path.
pub async fn proxy_any(
    State(state): State<AppState>,
    Path((peer, _rest)): Path<(String, String)>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let method = request.method().as_str().to_uppercase();
    let query = request
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    // Take the forwarded path from the RAW uri, not the decoded Path
    // param: axum percent-decodes captures, which would turn an
    // encoded dataset segment (okdata%2Fdata) into literal slashes and
    // break single-segment routes on the peer.
    let raw = request.uri().path();
    let Some((_, encoded_rest)) = raw.split_once("/proxy/") else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiErrorBody {
                error: "bad_request".into(),
                message: "malformed proxy path".into(),
            }),
        )
            .into_response();
    };
    let path = format!("/{encoded_rest}{query}");
    let body = match axum::body::to_bytes(request.into_body(), 1 << 20).await {
        Ok(b) if b.is_empty() => None,
        Ok(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiErrorBody {
                    error: "bad_request".into(),
                    message: format!("read body: {e}"),
                }),
            )
                .into_response();
        }
    };
    let link = match require_link(&state, &peer).await {
        Ok(l) => l,
        Err(e) => return e.into_response(),
    };
    let resp = match link.rpc(Request::Proxy { method, path, body }).await {
        Ok(r) => r,
        Err(e) => return rpc_error_to_http(format!("{e}")).into_response(),
    };
    match resp {
        Response::ProxyOk { status, body } => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
            (
                code,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Response::Error { code, message } => map_response_error(code, message).into_response(),
        other => map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )
        .into_response(),
    }
}

/// `GET /api/v1/peers/{peer}/events` — proxied SSE. Sends
/// SubscribeEvents to the peer's control channel and yields each
/// pushed Event frame as an SSE frame.
#[utoipa::path(
    get,
    path = "/api/v1/peers/{peer}/events",
    tag = "peers",
    params(("peer" = String, Path, description = "Peer name from [[peers]]")),
    responses(
        (status = 200, description = "SSE stream of LogEvent JSON frames from the peer"),
        (status = 404, description = "No such peer", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn stream_peer_events(
    State(state): State<AppState>,
    Path(peer): Path<String>,
) -> Result<
    axum::response::Sse<
        impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
    >,
    (StatusCode, HeaderMap, Json<ApiErrorBody>),
> {
    use axum::response::Sse;
    use axum::response::sse::{Event, KeepAlive};
    use std::time::Duration;
    use tokio::sync::broadcast::error::RecvError;

    let link = require_link(&state, &peer).await?;
    // PeerLink sends the SubscribeEvents RPC once per link (first
    // subscriber) and fans frames out via its broadcast — additional
    // SSE clients reuse the same server-side pusher.
    let rx = link
        .subscribe_events()
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    // Backlog: the broadcast only carries frames pushed after the FIRST
    // subscriber attached; every later EventSource would start blank.
    // Pull a JSON tail through the generic proxy so each fresh page
    // gets context, then dedup live frames by id.
    let backlog: Vec<LogEvent> = match link
        .rpc(Request::Proxy {
            method: "GET".into(),
            path: "/api/v1/events/recent?limit=100".into(),
            body: None,
        })
        .await
    {
        Ok(Response::ProxyOk { status: 200, body }) => {
            serde_json::from_str(&body).unwrap_or_default()
        }
        _ => Vec::new(),
    };
    let last_backlog_id = backlog.last().map(|e| e.id).unwrap_or(0);
    // End the stream when the broadcast closes — which happens when the
    // peer link is torn down on reconnect. Ending it (rather than
    // swallowing the error and stalling) lets the browser's EventSource
    // auto-reconnect and re-subscribe against the new link. Lagged frames
    // are skipped so a slow consumer doesn't kill the stream.
    let backlog_frames: Vec<Result<Event, std::convert::Infallible>> = backlog
        .iter()
        .map(|ev| {
            let payload = serde_json::to_string(ev).unwrap_or_else(|_| "{}".into());
            Ok(Event::default().id(ev.id.to_string()).data(payload))
        })
        .collect();
    let live = futures_util::stream::unfold(rx, move |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    if ev.id <= last_backlog_id {
                        continue;
                    }
                    let api_ev = LogEvent {
                        id: ev.id,
                        timestamp: ev.timestamp,
                        level: ev.level,
                        job_name: ev.job_name,
                        message: ev.message,
                    };
                    let payload = serde_json::to_string(&api_ev).unwrap_or_else(|_| "{}".into());
                    let event = Event::default().id(api_ev.id.to_string()).data(payload);
                    return Some((Ok(event), rx));
                }
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    let stream = futures_util::StreamExt::chain(futures_util::stream::iter(backlog_frames), live);
    // End on daemon shutdown so graceful drain can complete.
    let stream =
        futures_util::StreamExt::take_until(stream, state.shutdown.clone().cancelled_owned());
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}
