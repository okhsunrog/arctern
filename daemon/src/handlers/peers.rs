//! Peer-aware handlers. `GET /api/v1/peers` enumerates configured peers
//! with their current reachability; the rest proxy a Request over the
//! peer's control channel and translate the typed Response to HTTP.

use std::sync::Arc;

use arctern_api::{
    ApiErrorBody, JobStatus, LogEvent, PeerReachability, PeerSnapshotEntry, PeerSummary,
};
use arctern_transport::{ErrorCode, Request, Response};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use serde::Deserialize;
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
            ssh_target: e.ssh_target.clone(),
            reachability: render_reachability(&e.status),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Json(out)
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

#[utoipa::path(
    get,
    path = "/api/v1/peers/{peer}/jobs",
    tag = "peers",
    params(("peer" = String, Path, description = "Peer name from [[peers]]")),
    responses(
        (status = 200, description = "Per-job status snapshot from the peer",
         body = Vec<JobStatus>),
        (status = 404, description = "No such peer", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn list_peer_jobs(
    State(state): State<AppState>,
    Path(peer): Path<String>,
) -> Result<Json<Vec<JobStatus>>, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let link = require_link(&state, &peer).await?;
    let resp = link
        .rpc(Request::ListJobs)
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    match resp {
        Response::ListJobsOk { jobs } => Ok(Json(
            jobs.into_iter()
                .map(|j| JobStatus {
                    name: j.name,
                    kind: j.kind,
                    last_run: j.last_run,
                    next_run: j.next_run,
                    last_error: j.last_error,
                })
                .collect(),
        )),
        Response::Error { code, message } => Err(map_response_error(code, message)),
        other => Err(map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/peers/{peer}/jobs/{name}",
    tag = "peers",
    params(
        ("peer" = String, Path, description = "Peer name from [[peers]]"),
        ("name" = String, Path, description = "Job name on the peer"),
    ),
    responses(
        (status = 200, description = "Status for one job on the peer", body = JobStatus),
        (status = 404, description = "No such peer / job", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn get_peer_job(
    State(state): State<AppState>,
    Path((peer, name)): Path<(String, String)>,
) -> Result<Json<JobStatus>, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let link = require_link(&state, &peer).await?;
    let resp = link
        .rpc(Request::GetJobStatus { name })
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    match resp {
        Response::GetJobStatusOk { job } => Ok(Json(JobStatus {
            name: job.name,
            kind: job.kind,
            last_run: job.last_run,
            next_run: job.next_run,
            last_error: job.last_error,
        })),
        Response::Error { code, message } => Err(map_response_error(code, message)),
        other => Err(map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/peers/{peer}/jobs/{name}/wakeup",
    tag = "peers",
    params(
        ("peer" = String, Path, description = "Peer name from [[peers]]"),
        ("name" = String, Path, description = "Job name on the peer"),
    ),
    responses(
        (status = 204, description = "Wakeup delivered"),
        (status = 404, description = "No such peer / job", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn wakeup_peer_job(
    State(state): State<AppState>,
    Path((peer, name)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let link = require_link(&state, &peer).await?;
    let resp = link
        .rpc(Request::WakeupJob { name })
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    match resp {
        Response::WakeupJobOk => Ok(StatusCode::NO_CONTENT),
        Response::Error { code, message } => Err(map_response_error(code, message)),
        other => Err(map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )),
    }
}

#[derive(Debug, Deserialize)]
pub struct PeerSnapshotsQuery {
    pub dataset: String,
    #[serde(default)]
    pub prefix_regex: Option<String>,
}

#[utoipa::path(
    get,
    path = "/api/v1/peers/{peer}/snapshots",
    tag = "peers",
    params(
        ("peer" = String, Path, description = "Peer name from [[peers]]"),
        ("dataset" = String, Query, description = "Dataset on the peer"),
        ("prefix_regex" = Option<String>, Query, description = "Optional name filter"),
    ),
    responses(
        (status = 200, description = "Snapshots on the peer for the dataset",
         body = Vec<PeerSnapshotEntry>),
        (status = 404, description = "No such peer / dataset", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn list_peer_snapshots(
    State(state): State<AppState>,
    Path(peer): Path<String>,
    Query(q): Query<PeerSnapshotsQuery>,
) -> Result<Json<Vec<PeerSnapshotEntry>>, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let link = require_link(&state, &peer).await?;
    let resp = link
        .rpc(Request::ListSnapshots {
            dataset: q.dataset,
            prefix_regex: q.prefix_regex,
        })
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    match resp {
        Response::ListSnapshotsOk { snapshots, .. } => Ok(Json(
            snapshots
                .into_iter()
                .map(|s| PeerSnapshotEntry {
                    name: s.name,
                    guid: s.guid.to_string(),
                    createtxg: s.createtxg,
                })
                .collect(),
        )),
        Response::Error { code, message } => Err(map_response_error(code, message)),
        other => Err(map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )),
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/peers/{peer}/snapshots/{name}/destroy",
    tag = "peers",
    params(
        ("peer" = String, Path, description = "Peer name from [[peers]]"),
        ("name" = String, Path, description = "Snapshot name (URL-encode @ and /)"),
    ),
    responses(
        (status = 204, description = "Snapshot destroyed on the peer"),
        (status = 403, description = "Not allowed under the peer's ACL", body = ApiErrorBody),
        (status = 404, description = "No such peer / snapshot", body = ApiErrorBody),
        (status = 503, description = "Peer not currently connected", body = ApiErrorBody),
    ),
)]
pub async fn destroy_peer_snapshot(
    State(state): State<AppState>,
    Path((peer, name)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, HeaderMap, Json<ApiErrorBody>)> {
    let link = require_link(&state, &peer).await?;
    let resp = link
        .rpc(Request::DestroySnapshot { name })
        .await
        .map_err(|e| rpc_error_to_http(format!("{e}")))?;
    match resp {
        Response::DestroySnapshotOk => Ok(StatusCode::NO_CONTENT),
        Response::Error { code, message } => Err(map_response_error(code, message)),
        other => Err(map_response_error(
            ErrorCode::Internal,
            format!("unexpected response: {other:?}"),
        )),
    }
}

// LogEvent appears in OpenAPI via the events SSE handler in events.rs.
// Re-export here so the type registers under the peers tag too.
#[allow(dead_code)]
fn _log_event_marker(_: LogEvent) {}

// Unused-import suppressors keep IntoResponse available if step 11
// extends this module to render error variants directly.
#[allow(dead_code)]
fn _into_response_marker(r: impl IntoResponse) -> axum::response::Response {
    r.into_response()
}
