//! `GET /api/v1/config` — return the on-disk TOML the daemon was
//! started with. Read-only by design: the UI displays it for
//! discoverability; writes still go through editing the file +
//! `systemctl reload`.

use arctern_api::{ApiErrorBody, ConfigView};
use axum::{Json, extract::State};

use crate::app_state::AppState;
use crate::error::ApiError;

#[utoipa::path(
    get,
    path = "/api/v1/config",
    tag = "config",
    responses(
        (status = 200, description = "The TOML file currently loaded", body = ConfigView),
        (status = 500, description = "Config file unreadable", body = ApiErrorBody),
    ),
)]
pub async fn get_config(State(state): State<AppState>) -> Result<Json<ConfigView>, ApiError> {
    let content_toml = tokio::fs::read_to_string(&state.config_path)
        .await
        .map_err(|e| {
            ApiError::internal(format!("read config {}: {e}", state.config_path.display()))
        })?;
    Ok(Json(ConfigView {
        path: state.config_path.display().to_string(),
        content_toml,
    }))
}
