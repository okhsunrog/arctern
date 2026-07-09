//! `GET /api/v1/datasets` — list ZFS datasets visible to the daemon's runner.

use arctern_api::{ApiErrorBody, DatasetSummary};
use axum::{Json, extract::State};
use zfskit::dataset::ListOptions;

use crate::app_state::AppState;
use crate::error::ApiError;

/// List datasets reachable through the daemon's shared `CommandRunner`
/// (`AppState::runner`). RealRunner in production; SshCommandRunner
/// only when `ZFSKIT_SSH_TARGET` is set for dev/test.
#[utoipa::path(
    get,
    path = "/api/v1/datasets",
    tag = "datasets",
    responses(
        (status = 200, description = "All datasets visible to the daemon's ZFS runner",
         body = Vec<DatasetSummary>),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn list_datasets(
    State(state): State<AppState>,
) -> Result<Json<Vec<DatasetSummary>>, ApiError> {
    // usedbysnapshots rides along so the browser can answer "what do
    // this dataset's snapshots cost" without a per-dataset query.
    let opts = ListOptions {
        properties: vec!["used".into(), "usedbysnapshots".into(), "referenced".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(state.runner.as_ref(), &opts).await?;
    let summaries: Vec<DatasetSummary> = entries.into_iter().map(DatasetSummary::from).collect();
    Ok(Json(summaries))
}
