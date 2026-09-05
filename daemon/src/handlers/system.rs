//! `/api/v1/system/*` — host-level ZFS state outside `zfs(8)`/`zpool(8)`.
//! Today: ARC stats + history.

use arctern_api::{ApiErrorBody, ArcHistoryPoint, ArcStats, SystemInfo};
use axum::{
    Json,
    extract::{Query, State},
};
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

#[utoipa::path(
    get,
    path = "/api/v1/system/info",
    tag = "system",
    responses(
        (status = 200, description = "Daemon identity (version)", body = SystemInfo),
    ),
)]
pub async fn get_system_info() -> Json<SystemInfo> {
    Json(SystemInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/system/arc",
    tag = "system",
    responses(
        (status = 200, description = "Current ARC stats snapshot", body = ArcStats),
        (status = 500, description = "Could not read /proc/spl/kstat/zfs/arcstats",
         body = ApiErrorBody),
    ),
)]
pub async fn get_arc() -> Result<Json<ArcStats>, ApiError> {
    let s = zfskit::system::arc_stats()
        .map_err(|e| ApiError::internal(format!("arcstats read: {e}")))?;
    Ok(Json(arc_stats_wire(s)))
}

fn arc_stats_wire(s: zfskit::system::ArcStats) -> ArcStats {
    let ratio = s.hit_ratio();
    ArcStats {
        hit_ratio: ratio.is_finite().then_some(ratio),
        size: s.size,
        c: s.c,
        c_min: s.c_min,
        c_max: s.c_max,
        hits: s.hits,
        misses: s.misses,
        demand_data_hits: s.demand_data_hits,
        demand_data_misses: s.demand_data_misses,
        demand_metadata_hits: s.demand_metadata_hits,
        demand_metadata_misses: s.demand_metadata_misses,
        prefetch_data_hits: s.prefetch_data_hits,
        prefetch_data_misses: s.prefetch_data_misses,
        prefetch_metadata_hits: s.prefetch_metadata_hits,
        prefetch_metadata_misses: s.prefetch_metadata_misses,
        mru_hits: s.mru_hits,
        mfu_hits: s.mfu_hits,
        mru_ghost_hits: s.mru_ghost_hits,
        mfu_ghost_hits: s.mfu_ghost_hits,
        l2_size: s.l2_size,
        l2_hits: s.l2_hits,
        l2_misses: s.l2_misses,
        compressed_size: s.compressed_size,
        uncompressed_size: s.uncompressed_size,
    }
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct ArcHistoryQuery {
    /// Unix-second cutoff; rows with `timestamp >= since` are returned.
    pub since: Option<i64>,
    /// Maximum rows to return. Default 1440 (24h at 1m resolution).
    pub limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/v1/system/arc/history",
    tag = "system",
    params(ArcHistoryQuery),
    responses(
        (status = 200, description = "Recent ARC samples, newest first",
         body = Vec<ArcHistoryPoint>),
    ),
)]
pub async fn get_arc_history(
    State(state): State<AppState>,
    Query(q): Query<ArcHistoryQuery>,
) -> Result<Json<Vec<ArcHistoryPoint>>, ApiError> {
    let limit = q.limit.unwrap_or(1440).clamp(1, 10_000);
    let rows = crate::state::arcstats::list_recent(&state.state, q.since, limit)
        .await
        .map_err(|e| ApiError::internal(format!("arcstats history query: {e}")))?;
    Ok(Json(rows))
}
