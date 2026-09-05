//! `GET /api/v1/datasets` — list ZFS datasets visible to the daemon's runner.

use arctern_api::{ApiErrorBody, DatasetSummary, DatasetType};
use axum::{Json, extract::State};
use zfskit::dataset::{ListOptions, ZfsListEntry};

use crate::app_state::AppState;
use crate::error::ApiError;

/// The wire projection of one `zfs list` entry. Typed property values
/// flatten to their string form; the UI parses what it needs.
pub(crate) fn dataset_summary(entry: ZfsListEntry) -> DatasetSummary {
    use zfskit::models::DatasetType as Kind;
    let dataset_type = match entry.kind {
        Kind::Filesystem => DatasetType::Filesystem,
        Kind::Volume => DatasetType::Volume,
        Kind::Snapshot => DatasetType::Snapshot,
        Kind::Bookmark => DatasetType::Bookmark,
        // The listing asks for the four types above; anything else is
        // a zfs(8) newer than this build, and the closest reading of an
        // unknown named dataset is a filesystem.
        _ => DatasetType::Filesystem,
    };
    DatasetSummary {
        name: entry.name,
        dataset_type,
        properties: entry
            .properties
            .into_iter()
            .map(|(k, v)| (k, v.value))
            .collect(),
    }
}

/// List datasets through the daemon's shared typed ZFS facade. It uses
/// RealRunner in production and the SSH test runner only for dev/integration.
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
    let entries = state.zfs.list_datasets(&opts).await?;
    let summaries: Vec<DatasetSummary> = entries.into_iter().map(dataset_summary).collect();
    Ok(Json(summaries))
}
