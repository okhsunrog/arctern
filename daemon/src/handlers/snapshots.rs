//! `POST /api/v1/datasets/{name}/snapshots` — create a snapshot.

use arctern_api::{ApiErrorBody, CreateSnapshotRequest, DatasetSummary};
use axum::{
    Json,
    extract::Path,
    http::StatusCode,
};
use palimpsest::dataset::{ListOptions, SnapshotOptions};
use palimpsest::models::DatasetType;

use crate::error::ApiError;

/// Create a snapshot of `{name}` named `req.snapshot_name`. After ZFS
/// confirms creation, a follow-up `zfs list -j` materializes the
/// `DatasetSummary` so callers do not need a second round-trip.
///
/// `palimpsest::ZfsError::SnapshotExists` maps to 409 (not 200/201) — the
/// caller decides whether already-exists is fatal.
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
        (status = 503, description = "Could not spawn the underlying zfs subprocess",
         body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn create_snapshot(
    Path(name): Path<String>,
    Json(req): Json<CreateSnapshotRequest>,
) -> Result<(StatusCode, Json<DatasetSummary>), ApiError> {
    let runner = palimpsest::SshCommandRunner::from_env().map_err(|e| {
        ApiError(palimpsest::ZfsError::Other {
            exit_code: None,
            stderr: format!("PALIMPSEST_SSH_TARGET configuration: {e}"),
        })
    })?;

    let full = format!("{name}@{}", req.snapshot_name);

    let mut opts = SnapshotOptions::new();
    if req.recursive {
        opts = opts.recursive();
    }
    for (k, v) in &req.properties {
        opts = opts.property(k, v);
    }
    palimpsest::dataset::snapshot(&runner, &full, &opts).await?;

    let list_opts = ListOptions {
        roots: vec![full.clone()],
        types: vec![DatasetType::Snapshot],
        ..ListOptions::default()
    };
    let entries = palimpsest::dataset::list(&runner, &list_opts).await?;
    let entry = entries.into_iter().next().ok_or_else(|| {
        ApiError(palimpsest::ZfsError::Other {
            exit_code: None,
            stderr: format!("snapshot {full} created but not visible to subsequent list"),
        })
    })?;

    Ok((StatusCode::CREATED, Json(DatasetSummary::from(entry))))
}
