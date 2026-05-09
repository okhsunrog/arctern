//! Axum router + utoipa OpenAPI doc.

use axum::{Json, Router, middleware, routing::get};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{auth, handlers};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "arctern",
        description = "ZFS replication daemon HTTP API",
    ),
    components(schemas(
        arctern_api::DatasetSummary,
        arctern_api::ApiErrorBody,
        arctern_api::CreateSnapshotRequest,
    )),
)]
struct ApiDoc;

pub fn build_router() -> Router {
    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(handlers::datasets::list_datasets))
        .routes(routes!(handlers::snapshots::create_snapshot))
        .split_for_parts();

    router
        .route(
            "/api-docs/openapi.json",
            get(move || async move { Json(api.clone()) }),
        )
        // Layer the entire router (including the OpenAPI doc) so every
        // route inherits the same-uid check by construction. New routes
        // do not need to remember to opt in.
        .layer(middleware::from_fn(auth::enforce_same_uid))
}
