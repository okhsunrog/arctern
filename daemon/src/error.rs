//! HTTP error mapping. The single `IntoResponse` impl here is the *only*
//! place arctern translates handler failures to wire responses; handlers
//! `?` their way through zfskit calls (via `From<ZfsError>`) or
//! construct the request-shape variants explicitly.

use arctern_api::ApiErrorBody;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use zfskit::ZfsError;

pub enum ApiError {
    /// Underlying ZFS operation failed; status derives from the
    /// classified error.
    Zfs(ZfsError),
    /// Caller supplied a malformed name / parameter — 400.
    BadRequest(String),
    /// Non-ZFS internal failure (state DB, config file, …) — 500.
    Internal(String),
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl From<ZfsError> for ApiError {
    fn from(e: ZfsError) -> Self {
        Self::Zfs(e)
    }
}

impl From<zfskit::NameError> for ApiError {
    fn from(error: zfskit::NameError) -> Self {
        Self::Zfs(ZfsError::from(error))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, category, message) = match self {
            ApiError::Zfs(e) => {
                let (status, category) = match &e {
                    ZfsError::InvalidName(_) | ZfsError::InvalidInput { .. } => {
                        (StatusCode::BAD_REQUEST, "invalid_input")
                    }
                    ZfsError::Parse { .. } | ZfsError::IncompatibleOutput { .. } => {
                        (StatusCode::BAD_GATEWAY, "incompatible_output")
                    }
                    ZfsError::BookmarkConflict { .. } => {
                        (StatusCode::CONFLICT, "bookmark_conflict")
                    }
                    ZfsError::Spawn(_) => (StatusCode::SERVICE_UNAVAILABLE, "spawn"),
                    ZfsError::DatasetNotFound { .. } => {
                        (StatusCode::NOT_FOUND, "dataset_not_found")
                    }
                    ZfsError::PoolNotFound { .. } => (StatusCode::NOT_FOUND, "pool_not_found"),
                    ZfsError::PermissionDenied => (StatusCode::FORBIDDEN, "permission_denied"),
                    ZfsError::Busy { .. } => (StatusCode::CONFLICT, "busy"),
                    ZfsError::SnapshotHeld { .. } => (StatusCode::CONFLICT, "snapshot_held"),
                    ZfsError::SnapshotExists { .. } => (StatusCode::CONFLICT, "snapshot_exists"),
                    ZfsError::KeyNotLoaded { .. } => (StatusCode::FORBIDDEN, "key_not_loaded"),
                    ZfsError::NoSpace => (StatusCode::INSUFFICIENT_STORAGE, "no_space"),
                    ZfsError::Other { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "other"),
                    _ => (StatusCode::INTERNAL_SERVER_ERROR, "zfs"),
                };
                (status, category, e.to_string())
            }
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            ApiError::Internal(message) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", message),
        };
        let body = ApiErrorBody {
            error: category.to_string(),
            message,
        };
        (status, Json(body)).into_response()
    }
}
