//! Axum router + utoipa OpenAPI doc.
//!
//! T004 ships the scaffold with `GET /api-docs/openapi.json` exposing an
//! empty-paths document. T005 wires in `GET /api/v1/datasets` and the
//! handler-derived OpenAPI metadata.

use axum::{Json, Router, routing::get};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "arctern",
        description = "ZFS replication daemon HTTP API",
    ),
    components(schemas(arctern_api::DatasetSummary, arctern_api::ApiErrorBody)),
)]
struct ApiDoc;

pub fn build_router() -> Router {
    let api = ApiDoc::openapi();
    Router::new().route(
        "/api-docs/openapi.json",
        get(move || async move { Json(api.clone()) }),
    )
}
