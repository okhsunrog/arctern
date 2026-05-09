//! `GET /api/v1/jobs` — current per-job status snapshot.

use std::sync::Arc;

use arctern_api::JobStatus;
use axum::{Json, extract::Path, extract::State, http::StatusCode};
use time::format_description::well_known::Rfc3339;

use crate::jobs::JobManager;

#[utoipa::path(
    get,
    path = "/api/v1/jobs",
    tag = "jobs",
    responses(
        (status = 200, description = "Per-job status snapshot",
         body = Vec<JobStatus>),
    ),
)]
pub async fn list_jobs(State(manager): State<Arc<JobManager>>) -> Json<Vec<JobStatus>> {
    let snap = manager.statuses();
    let out = snap
        .into_iter()
        .map(|(name, kind, s)| JobStatus {
            name,
            kind: kind.to_string(),
            last_run: s.last_run.and_then(|t| t.format(&Rfc3339).ok()),
            next_run: s.next_run.and_then(|t| t.format(&Rfc3339).ok()),
            last_error: s.last_error,
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
pub async fn wakeup(
    State(manager): State<Arc<JobManager>>,
    Path(name): Path<String>,
) -> StatusCode {
    if manager.wakeup_by_name(&name) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}
