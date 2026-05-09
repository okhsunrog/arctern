//! Axum router + utoipa OpenAPI doc.

use axum::{Json, Router, middleware, routing::get};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;
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
        arctern_api::JobStatus,
        arctern_api::PeerSummary,
        arctern_api::PeerReachability,
        arctern_api::PeerSnapshotEntry,
        arctern_api::LogEvent,
    )),
)]
struct ApiDoc;

pub fn build_router(state: AppState) -> Router {
    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(handlers::datasets::list_datasets))
        .routes(routes!(handlers::snapshots::create_snapshot))
        .routes(routes!(handlers::jobs::list_jobs))
        .routes(routes!(handlers::jobs::wakeup))
        .routes(routes!(handlers::peers::list_peers))
        .routes(routes!(handlers::peers::list_peer_jobs))
        .routes(routes!(handlers::peers::get_peer_job))
        .routes(routes!(handlers::peers::wakeup_peer_job))
        .routes(routes!(handlers::peers::list_peer_snapshots))
        .routes(routes!(handlers::peers::destroy_peer_snapshot))
        .routes(routes!(handlers::peers::stream_peer_events))
        .routes(routes!(handlers::events::stream_events))
        .with_state(state)
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
