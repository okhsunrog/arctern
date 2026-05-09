//! `GET /api/v1/datasets` — list ZFS datasets visible to the daemon's runner.

use arctern_api::{ApiErrorBody, DatasetSummary};
use axum::Json;
use palimpsest::dataset::ListOptions;

use crate::error::ApiError;

/// List datasets reachable through `palimpsest`'s SSH runner. The runner
/// is constructed per-request from `PALIMPSEST_SSH_TARGET` /
/// `PALIMPSEST_SSH_PASSWORD` — cheap, and avoids shared mutable state in
/// the daemon for this slice. A future slice may pool / reuse runners.
#[utoipa::path(
    get,
    path = "/api/v1/datasets",
    tag = "datasets",
    responses(
        (status = 200, description = "All datasets visible to the daemon's ZFS runner",
         body = Vec<DatasetSummary>),
        (status = 503, description = "Could not spawn the underlying zfs subprocess",
         body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn list_datasets() -> Result<Json<Vec<DatasetSummary>>, ApiError> {
    let runner = palimpsest::SshCommandRunner::from_env().map_err(|e| {
        ApiError(palimpsest::ZfsError::Other {
            exit_code: None,
            stderr: format!("PALIMPSEST_SSH_TARGET configuration: {e}"),
        })
    })?;
    let entries = palimpsest::dataset::list(&runner, &ListOptions::default()).await?;
    let summaries: Vec<DatasetSummary> = entries.into_iter().map(DatasetSummary::from).collect();
    Ok(Json(summaries))
}
