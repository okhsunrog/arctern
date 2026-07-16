//! Axum router + utoipa OpenAPI doc.

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;
use crate::{auth, handlers};

#[derive(OpenApi)]
#[openapi(
    info(title = "arctern", description = "ZFS replication daemon HTTP API",),
    components(schemas(
        arctern_api::DatasetSummary,
        arctern_api::ApiErrorBody,
        arctern_api::CreateSnapshotRequest,
        arctern_api::JobStatus,
        arctern_api::TransferInfo,
        arctern_api::TargetStatus,
        arctern_api::JobRun,
        arctern_api::PeerSummary,
        arctern_api::PeerRoute,
        arctern_api::PeerReachability,
        arctern_api::LogEvent,
        arctern_api::ConfigView,
        arctern_api::PoolSummary,
        arctern_api::PoolStatus,
        arctern_api::ScanSummary,
        arctern_api::VdevNode,
        arctern_api::ScrubRequest,
        arctern_api::ArcStats,
        arctern_api::ArcHistoryPoint,
        arctern_api::SnapshotHold,
        arctern_api::CreateHoldRequest,
    ))
)]
struct ApiDoc;

fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(handlers::datasets::list_datasets))
        .routes(routes!(handlers::snapshots::list_snapshots))
        .routes(routes!(handlers::snapshots::create_snapshot))
        .routes(routes!(handlers::snapshots::destroy_snapshot))
        .routes(routes!(handlers::snapshots::list_holds))
        .routes(routes!(handlers::snapshots::create_hold))
        .routes(routes!(handlers::snapshots::release_hold))
        .routes(routes!(handlers::jobs::list_jobs))
        .routes(routes!(handlers::jobs::stream_jobs))
        .routes(routes!(handlers::jobs::wakeup))
        .routes(routes!(handlers::jobs::cancel))
        .routes(routes!(handlers::jobs::pause))
        .routes(routes!(handlers::jobs::resume))
        .routes(routes!(handlers::jobs::push_to_peer))
        .routes(routes!(handlers::jobs::list_runs))
        .routes(routes!(handlers::peers::list_peers))
        .routes(routes!(handlers::peers::stream_peer_jobs))
        .routes(routes!(handlers::peers::stream_peer_events))
        .routes(routes!(handlers::events::stream_events))
        .routes(routes!(handlers::events::recent_events))
        .routes(routes!(handlers::transfers::recent_transfers))
        .routes(routes!(handlers::config::get_config))
        .routes(routes!(handlers::pools::list_pools))
        .routes(routes!(handlers::pools::get_pool))
        .routes(routes!(handlers::pools::pool_scrub))
        .routes(routes!(handlers::system::get_arc))
        .routes(routes!(handlers::system::get_arc_history))
}

pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    let (_router, api) = openapi_router().split_for_parts();
    api
}

fn build_api_router(state: AppState) -> Router {
    let (router, api) = openapi_router().with_state(state.clone()).split_for_parts();
    router
        .route(
            "/api-docs/openapi.json",
            get(move || async move { Json(api.clone()) }),
        )
        // Wildcard passthrough for host-scoped peer management; plain
        // axum route — the OpenAPI surface stays the mirrored local API.
        .route(
            "/api/v1/peers/{peer}/proxy/api/v1/{*rest}",
            axum::routing::any(handlers::peers::proxy_any).with_state(state),
        )
}

/// Router for the UNIX-socket bind. Same-UID middleware is applied to
/// every route (including the OpenAPI doc) so new routes inherit the
/// check by construction.
pub fn build_router(state: AppState) -> Router {
    build_api_router(state).layer(middleware::from_fn(auth::enforce_same_uid))
}

/// Router for the loopback TCP bind: authenticated API routes plus the
/// embedded admin UI from `memory-serve`. Static assets and the login
/// endpoints remain public so a logged-out browser can render the gate.
/// index.html is bundled at compile time so the SPA fallback handler
/// has bytes to serve regardless of debug-vs-release memory-serve mode.
const SPA_INDEX_HTML: &[u8] = include_bytes!("../../admin-ui/dist/index.html");

async fn spa_fallback(request: axum::extract::Request) -> axum::response::Response {
    // Unknown API paths must 404 as JSON, not 200 with the SPA shell —
    // a typo'd endpoint otherwise "succeeds" with HTML and the client
    // fails later on parse.
    if request.uri().path().starts_with("/api/") {
        let body = arctern_api::ApiErrorBody {
            error: "not_found".into(),
            message: format!("no such endpoint: {}", request.uri().path()),
        };
        return (StatusCode::NOT_FOUND, Json(body)).into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        SPA_INDEX_HTML,
    )
        .into_response()
}

pub fn build_loopback_router(state: AppState) -> Router {
    let auth = state.auth.clone();
    let protected_api = build_api_router(state).layer(middleware::from_fn_with_state(
        auth.clone(),
        auth::require_admin_session,
    ));
    let auth_routes = Router::new()
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/session", get(auth::session))
        .route("/api/v1/auth/logout", post(auth::logout))
        .layer(DefaultBodyLimit::max(4096))
        .with_state(auth);
    let static_routes: Router = memory_serve::load!()
        .index_file(Some("/index.html"))
        .into_router();
    // memory-serve handles /, /index.html, /assets/*; client-side
    // router paths (/jobs/foo, /events, ...) fall through to spa_fallback
    // which serves the embedded index.html so vue-router can render them.
    //
    // Two guards layered on the whole stack:
    // - Host check (all methods): rejects DNS-rebound origins, which
    //   would otherwise count as same-origin for Sec-Fetch-Site.
    // - CSRF guard: GETs to /index.html and /assets are unaffected (it
    //   only acts on mutating methods); mutating /api/v1/* requests
    //   must originate same-origin (or carry no Sec-Fetch-Site, i.e.
    //   be a non-browser CLI client).
    protected_api
        .merge(auth_routes)
        .merge(static_routes)
        .fallback(spa_fallback)
        .layer(middleware::from_fn(set_csp))
        .layer(middleware::from_fn(auth::enforce_csrf))
        .layer(middleware::from_fn(auth::enforce_loopback_host))
}

/// The console is fully self-contained (fonts, icons, everything rides
/// in the binary), so the browser may load resources from NOWHERE else.
/// This is the backstop for that guarantee: even if bundled code grows
/// a network path (e.g. @iconify/vue falling back to its API for an
/// icon missing from the vendored set), the browser refuses it.
/// 'unsafe-inline' for styles only — Vue binds inline style attributes.
/// The script-src hash allowlists the one inline script Nuxt UI
/// injects (its colour-variables cleanup); if a Nuxt UI upgrade
/// changes that snippet the worst case is a console warning and a
/// leftover <style> tag, not a broken console.
async fn set_csp(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        axum::http::HeaderValue::from_static(
            "default-src 'self'; img-src 'self' data:; font-src 'self' data:; \
             style-src 'self' 'unsafe-inline'; connect-src 'self'; \
             script-src 'self' 'sha256-tYCcUbFfjZ9QESuTWESGWrFg2SmiEdyD2MYUfRWUgK0='; \
             object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    response
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
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_state() -> AppState {
        let pool = Arc::new(state::open_in_memory().await.unwrap());
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        AppState {
            auth: auth::AdminAuth::for_tests([7; 32], pool.as_ref().clone()),
            manager: Arc::new(JobManager::new()),
            peers: new_state(),
            events,
            state: pool,
            zfs: zfskit::Zfs::new(),
            config_path: std::path::PathBuf::from("/dev/null"),
            shutdown: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn req(method: Method, uri: &str, sec_fetch_site: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(v) = sec_fetch_site {
            b = b.header("sec-fetch-site", v);
        }
        b.body(Body::empty()).unwrap()
    }

    async fn login_cookie(app: &Router) -> String {
        let token = URL_SAFE_NO_PAD.encode([7u8; 32]);
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/auth/login")
            .header("content-type", "application/json")
            .header("sec-fetch-site", "same-origin")
            .body(Body::from(format!(r#"{{"token":"{token}"}}"#)))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let set_cookie = response
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .expect("login set-cookie")
            .to_str()
            .unwrap();
        assert!(set_cookie.contains("Max-Age=2592000"));
        set_cookie.split(';').next().unwrap().to_string()
    }

    fn with_cookie(mut request: Request<Body>, cookie: &str) -> Request<Body> {
        request
            .headers_mut()
            .insert(axum::http::header::COOKIE, cookie.parse().unwrap());
        request
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
    async fn same_origin_authenticated_post_passes_csrf() {
        // No job named "no_such_job" is registered, so the wakeup handler
        // returns 404 — that's the signal that CSRF didn't shortcut us.
        let app = build_loopback_router(test_state().await);
        let cookie = login_cookie(&app).await;
        let resp = app
            .oneshot(with_cookie(
                req(
                    Method::POST,
                    "/api/v1/jobs/no_such_job/wakeup",
                    Some("same-origin"),
                ),
                &cookie,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unauthenticated_cli_caller_is_rejected() {
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::POST, "/api/v1/jobs/no_such_job/wakeup", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unauthenticated_cross_site_get_is_rejected_by_auth() {
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::GET, "/api/v1/jobs", Some("cross-site")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    fn req_with_host(method: Method, uri: &str, host: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("host", host)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn rebound_host_is_blocked_even_for_get() {
        // DNS rebinding: attacker.com resolving to 127.0.0.1 makes the
        // request same-origin for Sec-Fetch-Site purposes; the Host
        // check is what stops it — including plain reads.
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req_with_host(
                Method::GET,
                "/api/v1/jobs",
                "attacker.example:7878",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn unknown_api_path_returns_404_not_spa_html() {
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::GET, "/api/v1/no_such_endpoint", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let ct = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.contains("json"), "got content-type {ct:?}");
    }

    #[tokio::test]
    async fn unknown_ui_path_serves_spa_shell() {
        let app = build_loopback_router(test_state().await);
        let resp = app
            .oneshot(req(Method::GET, "/jobs/some-job", None))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn loopback_hosts_are_allowed() {
        for host in [
            "127.0.0.1:7878",
            "127.0.0.1",
            "localhost:7878",
            "localhost",
            "[::1]:7878",
        ] {
            let app = build_loopback_router(test_state().await);
            let resp = app
                .oneshot(req_with_host(Method::GET, "/", host))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "host {host}");
        }
    }

    #[tokio::test]
    async fn login_session_and_logout_flow() {
        let app = build_loopback_router(test_state().await);

        let bad = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/auth/login")
            .header("content-type", "application/json")
            .header("sec-fetch-site", "same-origin")
            .body(Body::from(r#"{"token":"wrong"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(bad).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let cookie = login_cookie(&app).await;
        let session = with_cookie(req(Method::GET, "/api/v1/auth/session", None), &cookie);
        assert_eq!(
            app.clone().oneshot(session).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );

        let jobs = with_cookie(req(Method::GET, "/api/v1/jobs", None), &cookie);
        assert_eq!(
            app.clone().oneshot(jobs).await.unwrap().status(),
            StatusCode::OK
        );

        let logout = with_cookie(
            req(Method::POST, "/api/v1/auth/logout", Some("same-origin")),
            &cookie,
        );
        assert_eq!(
            app.clone().oneshot(logout).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );

        let expired = with_cookie(req(Method::GET, "/api/v1/jobs", None), &cookie);
        assert_eq!(
            app.oneshot(expired).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn authenticated_job_stream_starts_with_a_snapshot() {
        use futures_util::StreamExt as _;

        let app = build_loopback_router(test_state().await);
        let cookie = login_cookie(&app).await;
        let response = app
            .oneshot(with_cookie(
                req(Method::GET, "/api/v1/jobs/stream", None),
                &cookie,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "text/event-stream"
        );

        let mut body = response.into_body().into_data_stream();
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
            .await
            .expect("first job snapshot timed out")
            .expect("stream ended before first snapshot")
            .expect("job stream body error");
        let text = String::from_utf8_lossy(&chunk);
        assert!(
            text.contains("event: jobs"),
            "unexpected SSE frame: {text:?}"
        );
        assert!(text.contains("data: []"), "unexpected SSE frame: {text:?}");
    }

    #[tokio::test]
    async fn unix_router_does_not_require_http_session() {
        let app = build_router(test_state().await);
        let mut request = req(Method::GET, "/api/v1/jobs", None);
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(auth::PeerCredentials {
                uid: unsafe { libc::geteuid() },
            }));
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
