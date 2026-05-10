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

fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
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
}

pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    let (_router, api) = openapi_router().split_for_parts();
    api
}

fn build_api_router(state: AppState) -> Router {
    let (router, api) = openapi_router().with_state(state).split_for_parts();
    router.route(
        "/api-docs/openapi.json",
        get(move || async move { Json(api.clone()) }),
    )
}

/// Router for the UNIX-socket bind. Same-UID middleware is applied to
/// every route (including the OpenAPI doc) so new routes inherit the
/// check by construction.
pub fn build_router(state: AppState) -> Router {
    build_api_router(state).layer(middleware::from_fn(auth::enforce_same_uid))
}

/// Router for the loopback TCP bind: API routes plus the embedded
/// admin UI from `memory-serve`. No auth layer — the perimeter is the
/// 127.0.0.1 bind itself, per ARCHITECTURE.md.
pub fn build_loopback_router(state: AppState) -> Router {
    let static_routes: Router = memory_serve::load!()
        .index_file(Some("/index.html"))
        .fallback(Some("/index.html"))
        .into_router();
    build_api_router(state).merge(static_routes)
}
