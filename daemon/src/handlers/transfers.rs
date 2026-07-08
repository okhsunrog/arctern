//! Inbound-transfer history. The recv channel records each completed
//! stream in `recv_transfers`; this endpoint serves it to the UI's
//! "Incoming" panel — and, via the generic peer proxy, to any sender's
//! host-scoped console.

use arctern_api::RecvTransfer;
use axum::extract::State;

use crate::app_state::AppState;

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
pub struct RecentTransfersQuery {
    /// Maximum rows, newest first. Default 50.
    pub limit: Option<i64>,
}

#[utoipa::path(
    get,
    path = "/api/v1/transfers/recent",
    tag = "transfers",
    params(RecentTransfersQuery),
    responses(
        (status = 200, description = "Most recent completed inbound transfers, newest first",
         body = Vec<RecvTransfer>),
    ),
)]
pub async fn recent_transfers(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<RecentTransfersQuery>,
) -> axum::Json<Vec<RecvTransfer>> {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let rows = crate::state::recv_transfers::recent(&state.state, limit)
        .await
        .unwrap_or_default();
    axum::Json(
        rows.into_iter()
            .map(|r| RecvTransfer {
                id: r.id,
                completed_at: r.completed_at,
                job: r.job,
                identity: r.identity,
                dataset: r.dataset,
                to_snapshot: r.to_snapshot,
                from_snapshot: r.from_snapshot,
                bytes: r.bytes,
                duration_ms: r.duration_ms,
            })
            .collect(),
    )
}
