//! HTTP error mapping for `palimpsest::ZfsError`. The single `IntoResponse`
//! impl here is the *only* place arctern translates ZFS failures to wire
//! responses; handlers `?` their way through palimpsest calls and let this
//! produce the response.

use arctern_api::ApiErrorBody;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use palimpsest::ZfsError;

/// Newtype around `palimpsest::ZfsError` carrying an `IntoResponse` impl.
/// `From<ZfsError>` means handlers can write `?` over palimpsest results.
pub struct ApiError(pub ZfsError);

impl From<ZfsError> for ApiError {
    fn from(e: ZfsError) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, category) = match &self.0 {
            ZfsError::Spawn(_) => (StatusCode::SERVICE_UNAVAILABLE, "spawn"),
            ZfsError::DatasetNotFound { .. } => (StatusCode::NOT_FOUND, "dataset_not_found"),
            ZfsError::PoolNotFound { .. } => (StatusCode::NOT_FOUND, "pool_not_found"),
            ZfsError::PermissionDenied => (StatusCode::FORBIDDEN, "permission_denied"),
            ZfsError::Busy { .. } => (StatusCode::CONFLICT, "busy"),
            ZfsError::SnapshotHeld { .. } => (StatusCode::CONFLICT, "snapshot_held"),
            ZfsError::SnapshotExists { .. } => (StatusCode::CONFLICT, "snapshot_exists"),
            ZfsError::KeyNotLoaded { .. } => (StatusCode::FORBIDDEN, "key_not_loaded"),
            ZfsError::NoSpace => (StatusCode::INSUFFICIENT_STORAGE, "no_space"),
            ZfsError::Other { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "other"),
        };
        let body = ApiErrorBody {
            error: category.to_string(),
            message: self.0.to_string(),
        };
        (status, Json(body)).into_response()
    }
}
