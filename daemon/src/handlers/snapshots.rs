//! `/api/v1/datasets/{name}/snapshots` — list + create + destroy.

use arctern_api::{ApiErrorBody, CreateSnapshotRequest, DatasetSummary};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use palimpsest::dataset::{DestroyOptions, ListOptions, SnapshotOptions};
use palimpsest::models::DatasetType;
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct ListSnapshotsQuery {
    /// Filter to snapshots whose tag (after the `@`) starts with this
    /// prefix. Useful to keep zrepl_-style snapshots out of the noise
    /// when manual snapshots also exist on the dataset.
    pub prefix: Option<String>,
}

/// List snapshots of `{name}` (one dataset, non-recursive). Properties
/// `creation` and `used` come along so the UI can render age + size.
#[utoipa::path(
    get,
    path = "/api/v1/datasets/{name}/snapshots",
    tag = "snapshots",
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
        ListSnapshotsQuery,
    ),
    responses(
        (status = 200, description = "Snapshots of the dataset, oldest first",
         body = Vec<DatasetSummary>),
        (status = 404, description = "Dataset not found", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn list_snapshots(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<ListSnapshotsQuery>,
) -> Result<Json<Vec<DatasetSummary>>, ApiError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![name.clone()],
        properties: vec!["creation".into(), "used".into()],
        ..ListOptions::default()
    };
    let mut entries = palimpsest::dataset::list(state.runner.as_ref(), &opts).await?;
    if let Some(prefix) = q.prefix.as_deref() {
        let pat = format!("@{prefix}");
        entries.retain(|e| e.name.contains(&pat));
    }
    let summaries: Vec<DatasetSummary> = entries.into_iter().map(DatasetSummary::from).collect();
    Ok(Json(summaries))
}

/// Create a snapshot of `{name}` named `req.snapshot_name`.
/// `palimpsest::ZfsError::SnapshotExists` maps to 409 — the caller
/// decides whether already-exists is fatal.
#[utoipa::path(
    post,
    path = "/api/v1/datasets/{name}/snapshots",
    tag = "snapshots",
    request_body = CreateSnapshotRequest,
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
    ),
    responses(
        (status = 201, description = "Snapshot created", body = DatasetSummary),
        (status = 404, description = "Parent dataset not found", body = ApiErrorBody),
        (status = 409, description = "Snapshot already exists", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn create_snapshot(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<CreateSnapshotRequest>,
) -> Result<(StatusCode, Json<DatasetSummary>), ApiError> {
    let runner = state.runner.as_ref();
    let full = format!("{name}@{}", req.snapshot_name);

    let mut opts = SnapshotOptions::new();
    if req.recursive {
        opts = opts.recursive();
    }
    for (k, v) in &req.properties {
        opts = opts.property(k, v);
    }
    palimpsest::dataset::snapshot(runner, &full, &opts).await?;

    let list_opts = ListOptions {
        roots: vec![full.clone()],
        types: vec![DatasetType::Snapshot],
        ..ListOptions::default()
    };
    let entries = palimpsest::dataset::list(runner, &list_opts).await?;
    let entry = entries.into_iter().next().ok_or_else(|| {
        ApiError(palimpsest::ZfsError::Other {
            exit_code: None,
            stderr: format!("snapshot {full} created but not visible to subsequent list"),
        })
    })?;

    Ok((StatusCode::CREATED, Json(DatasetSummary::from(entry))))
}

/// Destroy snapshot `{name}@{snapshot}`. Path-segment escaping: the
/// dataset goes URL-encoded (`%2F` for `/`); the snapshot tag is the
/// part after the `@`.
#[utoipa::path(
    post,
    path = "/api/v1/datasets/{name}/snapshots/{snapshot}/destroy",
    tag = "snapshots",
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
        ("snapshot" = String, Path, description = "Snapshot tag (the part after `@`)"),
    ),
    responses(
        (status = 204, description = "Snapshot destroyed"),
        (status = 404, description = "Snapshot not found", body = ApiErrorBody),
        (status = 409, description = "Snapshot has a hold", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn destroy_snapshot(
    State(state): State<AppState>,
    Path((name, snapshot)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let full = format!("{name}@{snapshot}");
    palimpsest::dataset::destroy(state.runner.as_ref(), &full, &DestroyOptions::default()).await?;
    Ok(StatusCode::NO_CONTENT)
}
