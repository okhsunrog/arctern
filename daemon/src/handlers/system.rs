//! `/api/v1/system/*` — host-level ZFS state outside `zfs(8)`/`zpool(8)`.
//! Today: ARC stats + history.

use arctern_api::{ApiErrorBody, ArcHistoryPoint, ArcStats};
use axum::{
    Json,
    extract::{Query, State},
};
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

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
    let s = palimpsest::system::arc_stats().map_err(|e| {
        ApiError(palimpsest::ZfsError::Other {
            exit_code: None,
            stderr: format!("arcstats read: {e}"),
        })
    })?;
    Ok(Json(ArcStats::from(s)))
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
        .map_err(|e| {
            ApiError(palimpsest::ZfsError::Other {
                exit_code: None,
                stderr: format!("arcstats history query: {e}"),
            })
        })?;
    Ok(Json(rows))
}
