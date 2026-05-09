//! `GET /api/v1/jobs` — current per-job status snapshot.

use std::sync::Arc;

use arctern_api::JobStatus;
use axum::{Json, extract::State};
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
