//! `/api/v1/pools` — list pools + per-pool full status + scrub control.

use arctern_api::{
    ApiErrorBody, PoolStatus, PoolSummary, ScanSummary, ScrubRequest, VdevNode,
};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use palimpsest::models::{ScanStatus, VdevStatus, ZpoolStatusEntry};
use palimpsest::pool::ScrubAction;

use crate::app_state::AppState;
use crate::error::ApiError;

fn scan_to_wire(s: &ScanStatus) -> ScanSummary {
    ScanSummary {
        function: s.function.clone(),
        state: s.state.clone(),
        start_time: s.start_time.clone(),
        end_time: s.end_time.clone(),
        to_examine: s.to_examine.clone(),
        examined: s.examined.clone(),
        errors: s.errors.clone(),
        pass_start: s.pass_start.clone(),
        scrub_pause: s.scrub_pause.clone(),
        issued: s.issued.clone(),
    }
}

fn vdev_to_wire(v: &VdevStatus) -> VdevNode {
    // palimpsest models the tree as HashMap<name, VdevStatus>; flatten to a
    // Vec so wire order is deterministic (alphabetical by name). zpool
    // doesn't guarantee map order either, so this is no worse and gives
    // the UI a stable rendering.
    let mut children: Vec<&VdevStatus> = v.vdevs.values().collect();
    children.sort_by(|a, b| a.name.cmp(&b.name));
    VdevNode {
        name: v.name.clone(),
        vdev_type: v.vdev_type.clone(),
        state: v.state.clone(),
        alloc_space: v.alloc_space.clone(),
        total_space: v.total_space.clone(),
        read_errors: v.read_errors.clone(),
        write_errors: v.write_errors.clone(),
        checksum_errors: v.checksum_errors.clone(),
        path: v.path.clone(),
        children: children.iter().map(|c| vdev_to_wire(c)).collect(),
    }
}

fn entry_to_summary(e: &ZpoolStatusEntry) -> PoolSummary {
    // The root vdev carries the pool's aggregate alloc/total — keyed in
    // the map by the pool name. Fall back to zeroes if it's missing (a
    // shape we've never seen but a defensive default keeps the UI alive).
    let root = e.vdevs.get(&e.name);
    PoolSummary {
        name: e.name.clone(),
        state: e.state.clone(),
        error_count: e.error_count.clone(),
        alloc_space: root.map(|r| r.alloc_space.clone()).unwrap_or_default(),
        total_space: root.map(|r| r.total_space.clone()).unwrap_or_default(),
        scan: e.scan.as_ref().map(scan_to_wire),
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/pools",
    tag = "pools",
    responses(
        (status = 200, description = "All imported pools with health + capacity",
         body = Vec<PoolSummary>),
        (status = 500, description = "zpool returned an error", body = ApiErrorBody),
    ),
)]
pub async fn list_pools(
    State(state): State<AppState>,
) -> Result<Json<Vec<PoolSummary>>, ApiError> {
    let entries = palimpsest::pool::status_all(state.runner.as_ref()).await?;
    let mut out: Vec<PoolSummary> = entries.iter().map(entry_to_summary).collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(out))
}

#[utoipa::path(
    get,
    path = "/api/v1/pools/{name}",
    tag = "pools",
    params(("name" = String, Path, description = "Pool name")),
    responses(
        (status = 200, description = "Full status with vdev tree + scrub progress",
         body = PoolStatus),
        (status = 404, description = "No imported pool with that name", body = ApiErrorBody),
        (status = 500, description = "zpool returned an error", body = ApiErrorBody),
    ),
)]
pub async fn get_pool(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PoolStatus>, ApiError> {
    let entry = palimpsest::pool::status(state.runner.as_ref(), &name).await?;
    // The status response's `vdevs` is keyed by name. The root vdev's
    // name equals the pool name; render the *children* of that root as
    // the top-level vdev list, since the root is an implementation
    // detail that adds no info.
    let root_children = entry
        .vdevs
        .get(&entry.name)
        .map(|root| {
            let mut cs: Vec<&VdevStatus> = root.vdevs.values().collect();
            cs.sort_by(|a, b| a.name.cmp(&b.name));
            cs.iter().map(|c| vdev_to_wire(c)).collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(Json(PoolStatus {
        name: entry.name.clone(),
        state: entry.state.clone(),
        error_count: entry.error_count.clone(),
        pool_guid: entry.pool_guid.clone(),
        txg: entry.txg.clone(),
        scan: entry.scan.as_ref().map(scan_to_wire),
        vdevs: root_children,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/pools/{name}/scrub",
    tag = "pools",
    request_body = ScrubRequest,
    params(("name" = String, Path, description = "Pool name")),
    responses(
        (status = 204, description = "zpool scrub accepted the action"),
        (status = 400, description = "Unknown action", body = ApiErrorBody),
        (status = 500, description = "zpool returned an error", body = ApiErrorBody),
    ),
)]
pub async fn pool_scrub(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<ScrubRequest>,
) -> Result<StatusCode, ApiError> {
    let action = match req.action.as_str() {
        "start" => ScrubAction::Start,
        "pause" => ScrubAction::Pause,
        "resume" => ScrubAction::Resume,
        "stop" => ScrubAction::Stop,
        other => {
            return Err(ApiError(palimpsest::ZfsError::Other {
                exit_code: None,
                stderr: format!(
                    "unknown scrub action {other}; expected start|pause|resume|stop"
                ),
            }));
        }
    };
    palimpsest::pool::scrub(state.runner.as_ref(), &name, action).await?;
    Ok(StatusCode::NO_CONTENT)
}
