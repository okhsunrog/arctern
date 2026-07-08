//! `GET /api/v1/jobs` — current per-job status snapshot.

use arctern_api::{JobRun, JobStatus};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;

use crate::app_state::AppState;
use crate::error::ApiError;

#[utoipa::path(
    get,
    path = "/api/v1/jobs",
    tag = "jobs",
    responses(
        (status = 200, description = "Per-job status snapshot",
         body = Vec<JobStatus>),
    ),
)]
pub async fn list_jobs(State(state): State<AppState>) -> Json<Vec<JobStatus>> {
    let snap = state.manager.statuses();
    let out = snap
        .into_iter()
        .map(|(name, kind, s)| JobStatus {
            name,
            kind: kind.to_string(),
            last_run: s.last_run.and_then(|t| t.format(&Rfc3339).ok()),
            next_run: s.next_run.and_then(|t| t.format(&Rfc3339).ok()),
            last_error: s.last_error,
            running: s.running,
            paused: s.paused,
            transfers: s.transfers,
            targets: s.targets,
        })
        .collect();
    Json(out)
}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{name}/wakeup",
    tag = "jobs",
    params(("name" = String, Path, description = "Job name as declared in arctern.toml")),
    responses(
        (status = 204, description = "Job's cycle loop was woken up"),
        (status = 404, description = "No job with that name is registered"),
    ),
)]
pub async fn wakeup(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode {
    if state.manager.wakeup_by_name(&name) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{name}/cancel",
    tag = "jobs",
    params(("name" = String, Path, description = "Job name")),
    responses(
        (status = 204, description = "In-flight transfer aborted (partial recv state on the receiver keeps it resumable)"),
        (status = 404, description = "No such job"),
        (status = 409, description = "Job kind does not support cancel"),
    ),
)]
pub async fn cancel(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode {
    match state.manager.cancel_by_name(&name) {
        Some(true) => StatusCode::NO_CONTENT,
        Some(false) => StatusCode::CONFLICT,
        None => StatusCode::NOT_FOUND,
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{name}/pause",
    tag = "jobs",
    params(("name" = String, Path, description = "Job name")),
    responses(
        (status = 204, description = "Transfer aborted resumably; scheduled cycles suspended"),
        (status = 404, description = "No such job"),
        (status = 409, description = "Job kind does not support pause"),
    ),
)]
pub async fn pause(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode {
    match state.manager.pause_by_name(&name) {
        Some(true) => StatusCode::NO_CONTENT,
        Some(false) => StatusCode::CONFLICT,
        None => StatusCode::NOT_FOUND,
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{name}/resume",
    tag = "jobs",
    params(("name" = String, Path, description = "Job name")),
    responses(
        (status = 204, description = "Job unpaused; the next cycle resumes the partial transfer"),
        (status = 404, description = "No such job"),
        (status = 409, description = "Job kind does not support resume"),
    ),
)]
pub async fn resume(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode {
    match state.manager.resume_by_name(&name) {
        Some(true) => StatusCode::NO_CONTENT,
        Some(false) => StatusCode::CONFLICT,
        None => StatusCode::NOT_FOUND,
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/jobs/{name}/push/{peer}",
    tag = "jobs",
    params(
        ("name" = String, Path, description = "Job name"),
        ("peer" = String, Path, description = "Target peer name from the job's targets"),
    ),
    responses(
        (status = 204, description = "Manual replication to the peer queued"),
        (status = 400, description = "Peer is not a target of this job", body = arctern_api::ApiErrorBody),
        (status = 404, description = "No such job"),
    ),
)]
pub async fn push_to_peer(
    State(state): State<AppState>,
    Path((name, peer)): Path<(String, String)>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match state.manager.request_push_by_name(&name, &peer) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Some(Err(message)) => (
            StatusCode::BAD_REQUEST,
            Json(arctern_api::ApiErrorBody {
                error: "bad_peer".into(),
                message,
            }),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct RunsQuery {
    /// Unix-second cutoff; rows with `started_at >= since` are returned.
    pub since: Option<i64>,
    /// Maximum number of rows to return. Defaults to 100, capped at 1000.
    pub limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/v1/jobs/{name}/runs",
    tag = "jobs",
    params(
        ("name" = String, Path, description = "Job name as declared in arctern.toml"),
        RunsQuery,
    ),
    responses(
        (status = 200, description = "Recent job runs, newest first", body = Vec<JobRun>),
    ),
)]
pub async fn list_runs(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<RunsQuery>,
) -> Result<Json<Vec<JobRun>>, ApiError> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = crate::state::job_runs::list_recent(&state.state, &name, q.since, limit)
        .await
        .map_err(|e| ApiError::internal(format!("job_runs query: {e}")))?;
    Ok(Json(rows))
}
