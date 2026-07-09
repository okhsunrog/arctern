//! `/api/v1/datasets/{name}/snapshots` — list + create + destroy.

use arctern_api::{ApiErrorBody, CreateSnapshotRequest, DatasetSummary, SnapshotHold};
use arctern_config::zfs_names::{validate_dataset_name, validate_snapshot_leaf};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use zfskit::dataset::{DestroyOptions, ListOptions, SnapshotOptions};
use zfskit::models::DatasetType;

use crate::app_state::AppState;
use crate::error::ApiError;

/// The UDS/loopback perimeter already limits callers to the local
/// user, but names still become bare `zfs` positionals — reject the
/// shapes (leading `-`, `..`, embedded `@`/`#`) that would parse as
/// flags or escape the dataset, mirroring the stdinserver handlers.
fn check_dataset(name: &str) -> Result<(), ApiError> {
    validate_dataset_name(name)
        .map_err(|e| ApiError::bad_request(format!("invalid dataset {name:?}: {e}")))
}

fn check_snapshot_leaf(tag: &str) -> Result<(), ApiError> {
    validate_snapshot_leaf(tag)
        .map_err(|e| ApiError::bad_request(format!("invalid snapshot name {tag:?}: {e}")))
}

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
    check_dataset(&name)?;
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![name.clone()],
        properties: vec!["creation".into(), "used".into()],
        ..ListOptions::default()
    };
    let mut entries = zfskit::dataset::list(state.runner.as_ref(), &opts).await?;
    if let Some(prefix) = q.prefix.as_deref() {
        let pat = format!("@{prefix}");
        entries.retain(|e| e.name.contains(&pat));
    }
    let summaries: Vec<DatasetSummary> = entries.into_iter().map(DatasetSummary::from).collect();
    Ok(Json(summaries))
}

/// Create a snapshot of `{name}` named `req.snapshot_name`.
/// `zfskit::ZfsError::SnapshotExists` maps to 409 — the caller
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
    check_dataset(&name)?;
    check_snapshot_leaf(&req.snapshot_name)?;
    let runner = state.runner.as_ref();
    let full = format!("{name}@{}", req.snapshot_name);

    let mut opts = SnapshotOptions::new();
    if req.recursive {
        opts = opts.recursive();
    }
    for (k, v) in &req.properties {
        opts = opts.property(k, v);
    }
    zfskit::dataset::snapshot(runner, &full, &opts).await?;

    let list_opts = ListOptions {
        roots: vec![full.clone()],
        types: vec![DatasetType::Snapshot],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &list_opts).await?;
    let entry = entries.into_iter().next().ok_or_else(|| {
        ApiError::internal(format!(
            "snapshot {full} created but not visible to subsequent list"
        ))
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
    check_dataset(&name)?;
    check_snapshot_leaf(&snapshot)?;
    let full = format!("{name}@{snapshot}");
    zfskit::dataset::destroy(state.runner.as_ref(), &full, &DestroyOptions::default()).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `zfs holds <snapshot>` — list user holds blocking destroy. Empty
/// vec means the snapshot is destroy-eligible (as far as holds are
/// concerned; sub-clones still block independently).
#[utoipa::path(
    get,
    path = "/api/v1/datasets/{name}/snapshots/{snapshot}/holds",
    tag = "snapshots",
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
        ("snapshot" = String, Path, description = "Snapshot tag (the part after `@`)"),
    ),
    responses(
        (status = 200, description = "Holds on the snapshot, oldest first",
         body = Vec<SnapshotHold>),
        (status = 404, description = "Snapshot not found", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn list_holds(
    State(state): State<AppState>,
    Path((name, snapshot)): Path<(String, String)>,
) -> Result<Json<Vec<SnapshotHold>>, ApiError> {
    check_dataset(&name)?;
    check_snapshot_leaf(&snapshot)?;
    let full = format!("{name}@{snapshot}");
    let holds = zfskit::hold::list_holds(state.runner.as_ref(), &full).await?;
    let mut out: Vec<SnapshotHold> = holds
        .into_iter()
        .map(|h| SnapshotHold {
            tag: h.tag,
            timestamp: h.timestamp,
        })
        .collect();
    out.sort_by_key(|h| h.timestamp);
    Ok(Json(out))
}

/// Hold tags are ZFS name components; reuse the leaf validation so a
/// tag can't smuggle whitespace or a leading `-` into the zfs argv.
fn check_hold_tag(tag: &str) -> Result<(), ApiError> {
    validate_snapshot_leaf(tag)
        .map_err(|e| ApiError::bad_request(format!("invalid hold tag {tag:?}: {e}")))
}

/// Place a user hold on `{name}@{snapshot}`. Tags with the `arctern_`
/// prefix are refused — they'd collide with the replication machinery's
/// step/last holds and be swept or misread by it.
#[utoipa::path(
    post,
    path = "/api/v1/datasets/{name}/snapshots/{snapshot}/holds",
    tag = "snapshots",
    request_body = arctern_api::CreateHoldRequest,
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
        ("snapshot" = String, Path, description = "Snapshot tag (the part after `@`)"),
    ),
    responses(
        (status = 201, description = "Hold placed"),
        (status = 400, description = "Invalid or reserved tag", body = ApiErrorBody),
        (status = 404, description = "Snapshot not found", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn create_hold(
    State(state): State<AppState>,
    Path((name, snapshot)): Path<(String, String)>,
    Json(req): Json<arctern_api::CreateHoldRequest>,
) -> Result<StatusCode, ApiError> {
    check_dataset(&name)?;
    check_snapshot_leaf(&snapshot)?;
    check_hold_tag(&req.tag)?;
    if req.tag.starts_with("arctern_") {
        return Err(ApiError::bad_request(
            "tags with the arctern_ prefix are reserved for the replication machinery",
        ));
    }
    let full = format!("{name}@{snapshot}");
    zfskit::hold::hold(state.runner.as_ref(), &full, &req.tag).await?;
    Ok(StatusCode::CREATED)
}

/// Release a user hold. `arctern_*` tags ARE allowed here — releasing
/// a stuck step hold from the UI is exactly the recovery path this
/// endpoint exists for.
#[utoipa::path(
    delete,
    path = "/api/v1/datasets/{name}/snapshots/{snapshot}/holds/{tag}",
    tag = "snapshots",
    params(
        ("name" = String, Path, description = "Parent dataset (URL-encode `/` as %2F)"),
        ("snapshot" = String, Path, description = "Snapshot tag (the part after `@`)"),
        ("tag" = String, Path, description = "Hold tag to release"),
    ),
    responses(
        (status = 204, description = "Hold released"),
        (status = 404, description = "Snapshot or hold not found", body = ApiErrorBody),
        (status = 500, description = "ZFS returned an error", body = ApiErrorBody),
    ),
)]
pub async fn release_hold(
    State(state): State<AppState>,
    Path((name, snapshot, tag)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    check_dataset(&name)?;
    check_snapshot_leaf(&snapshot)?;
    check_hold_tag(&tag)?;
    let full = format!("{name}@{snapshot}");
    zfskit::hold::release(state.runner.as_ref(), &full, &tag).await?;
    Ok(StatusCode::NO_CONTENT)
}
