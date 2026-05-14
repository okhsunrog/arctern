//! Axum router + utoipa OpenAPI doc.

use axum::{
    Json, Router,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::get,
};
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
        arctern_api::JobRun,
        arctern_api::PeerSummary,
        arctern_api::PeerReachability,
        arctern_api::PeerSnapshotEntry,
        arctern_api::LogEvent,
        arctern_api::ConfigView,
        arctern_api::PoolSummary,
        arctern_api::PoolStatus,
        arctern_api::ScanSummary,
        arctern_api::ScrubRequest,
    )),
)]
struct ApiDoc;

fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(handlers::datasets::list_datasets))
        .routes(routes!(handlers::snapshots::list_snapshots))
        .routes(routes!(handlers::snapshots::create_snapshot))
        .routes(routes!(handlers::snapshots::destroy_snapshot))
        .routes(routes!(handlers::jobs::list_jobs))
        .routes(routes!(handlers::jobs::wakeup))
        .routes(routes!(handlers::jobs::list_runs))
        .routes(routes!(handlers::peers::list_peers))
        .routes(routes!(handlers::peers::list_peer_jobs))
        .routes(routes!(handlers::peers::get_peer_job))
        .routes(routes!(handlers::peers::wakeup_peer_job))
        .routes(routes!(handlers::peers::list_peer_snapshots))
        .routes(routes!(handlers::peers::destroy_peer_snapshot))
        .routes(routes!(handlers::peers::stream_peer_events))
        .routes(routes!(handlers::events::stream_events))
        .routes(routes!(handlers::config::get_config))
        .routes(routes!(handlers::pools::list_pools))
        .routes(routes!(handlers::pools::get_pool))
        .routes(routes!(handlers::pools::pool_scrub))
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
/// index.html is bundled at compile time so the SPA fallback handler
/// has bytes to serve regardless of debug-vs-release memory-serve mode.
const SPA_INDEX_HTML: &[u8] = include_bytes!("../../admin-ui/dist/index.html");

async fn spa_fallback() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        SPA_INDEX_HTML,
    )
}

pub fn build_loopback_router(state: AppState) -> Router {
    let static_routes: Router = memory_serve::load!()
        .index_file(Some("/index.html"))
        .into_router();
    // memory-serve handles /, /index.html, /assets/*; client-side
    // router paths (/jobs/foo, /events, ...) fall through to spa_fallback
    // which serves the embedded index.html so vue-router can render them.
    //
    // CSRF guard layered on the whole stack: GETs to /index.html and
    // /assets are unaffected (the middleware only acts on mutating
    // methods); mutating /api/v1/* requests must originate same-origin
    // (or carry no Sec-Fetch-Site, i.e. be a non-browser CLI client).
    build_api_router(state)
        .merge(static_routes)
        .fallback(spa_fallback)
        .layer(middleware::from_fn(auth::enforce_csrf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::AppState;
    use crate::jobs::JobManager;
    use crate::peer::state::new_state;
    use crate::state;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let pool = Arc::new(state::open_in_memory().await.unwrap());
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        AppState {
            manager: Arc::new(JobManager::new()),
            peers: new_state(),
            events,
            state: pool,
            runner: Arc::new(palimpsest::runner::RealRunner),
            config_path: std::path::PathBuf::from("/dev/null"),
        }
    }

    fn req(method: Method, uri: &str, sec_fetch_site: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(v) = sec_fetch_site {
            b = b.header("sec-fetch-site", v);
        }
        b.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn cross_site_post_is_blocked() {
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(
                Method::POST,
                "/api/v1/jobs/no_such_job/wakeup",
                Some("cross-site"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn same_site_post_is_blocked() {
        // same-site != same-origin: e.g. an attacker on a subdomain that
        // shares the registrable domain. We block it just like cross-site.
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(
                Method::POST,
                "/api/v1/jobs/no_such_job/wakeup",
                Some("same-site"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn same_origin_post_passes_csrf() {
        // No job named "no_such_job" is registered, so the wakeup handler
        // returns 404 — that's the signal that CSRF didn't shortcut us.
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(
                Method::POST,
                "/api/v1/jobs/no_such_job/wakeup",
                Some("same-origin"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cli_caller_without_header_passes_csrf() {
        // No Sec-Fetch-Site → assumed non-browser (curl, arctern-client,
        // reqwest). Same 404 from the handler proves we got past the guard.
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::POST, "/api/v1/jobs/no_such_job/wakeup", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cross_site_get_is_allowed() {
        // GETs have no side effects; the rule must not block them or the
        // SSE event stream + UI bootstrap break under strict referrer-
        // policy regimes.
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::GET, "/api/v1/jobs", Some("cross-site")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
