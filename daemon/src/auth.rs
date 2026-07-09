//! Authentication for both local transports.
//!
//! AF_UNIX requests trust `SO_PEERCRED` and require the caller's uid to match
//! the daemon's. Loopback TCP requests exchange a private administrator token
//! for a short-lived, server-side browser session.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arctern_api::ApiErrorBody;
use axum::extract::connect_info::Connected;
use axum::{
    Json,
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    serve::IncomingStream,
};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tokio::net::UnixListener;

const ADMIN_TOKEN_FILE: &str = "admin.token";
const SESSION_COOKIE_PREFIX: &str = "arctern_session";
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const MAX_SESSIONS: usize = 128;

#[derive(Clone)]
pub struct AdminAuth {
    inner: Arc<AdminAuthInner>,
    token_path: Arc<PathBuf>,
    cookie_name: Arc<str>,
}

struct AdminAuthInner {
    token: [u8; 32],
    sessions: tokio::sync::RwLock<HashMap<[u8; 32], Instant>>,
}

#[derive(serde::Deserialize)]
pub struct LoginRequest {
    token: String,
}

impl AdminAuth {
    /// Load the persistent administrator token, creating it atomically on
    /// first startup. The token is deliberately separate from arctern.toml:
    /// that file is exposed by GET /api/v1/config after authentication.
    pub fn load_or_create(state_dir: &Path) -> io::Result<Self> {
        validate_state_dir(state_dir)?;
        let token_path = state_dir.join(ADMIN_TOKEN_FILE);
        let token = match read_token(&token_path) {
            Ok(token) => token,
            Err(e) if e.kind() == io::ErrorKind::NotFound => match create_token(&token_path) {
                Ok(token) => token,
                // Another process may have won the create_new race.
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => read_token(&token_path)?,
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        Ok(Self::new(token, token_path))
    }

    fn new(token: [u8; 32], token_path: PathBuf) -> Self {
        let cookie_name = session_cookie_name(&token);
        Self {
            inner: Arc::new(AdminAuthInner {
                token,
                sessions: tokio::sync::RwLock::new(HashMap::new()),
            }),
            token_path: Arc::new(token_path),
            cookie_name: Arc::from(cookie_name),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests(token: [u8; 32]) -> Self {
        Self::new(token, PathBuf::from("<test-admin-token>"))
    }

    pub fn token_path(&self) -> &Path {
        self.token_path.as_ref()
    }

    fn cookie_name(&self) -> &str {
        self.cookie_name.as_ref()
    }

    fn token_matches(&self, encoded: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded.trim()) else {
            return false;
        };
        let Ok(candidate) = <[u8; 32]>::try_from(decoded.as_slice()) else {
            return false;
        };
        bool::from(self.inner.token.ct_eq(&candidate))
    }

    async fn create_session(&self) -> io::Result<[u8; 32]> {
        let mut id = [0u8; 32];
        getrandom::fill(&mut id).map_err(random_error)?;
        let now = Instant::now();
        let mut sessions = self.inner.sessions.write().await;
        sessions.retain(|_, expires| *expires > now);
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, expires)| **expires)
                .map(|(id, _)| *id)
        {
            sessions.remove(&oldest);
        }
        sessions.insert(id, now + SESSION_TTL);
        Ok(id)
    }

    async fn session_is_valid(&self, id: &[u8; 32]) -> bool {
        let now = Instant::now();
        let sessions = self.inner.sessions.read().await;
        sessions.get(id).is_some_and(|expires| *expires > now)
    }

    async fn revoke_session(&self, id: &[u8; 32]) {
        self.inner.sessions.write().await.remove(id);
    }
}

fn session_cookie_name(token: &[u8; 32]) -> String {
    let digest = Sha256::digest(token);
    let namespace = URL_SAFE_NO_PAD.encode(&digest[..9]);
    format!("{SESSION_COOKIE_PREFIX}_{namespace}")
}

fn random_error(error: getrandom::Error) -> io::Error {
    io::Error::other(format!("operating-system random source: {error}"))
}

fn validate_state_dir(path: &Path) -> io::Result<()> {
    let metadata = std::fs::metadata(path)?;
    let daemon_uid = unsafe { libc::geteuid() };
    if !metadata.is_dir() || metadata.uid() != daemon_uid || metadata.mode() & 0o022 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "state directory {} must be owned by uid {daemon_uid} and not writable by group/other",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn validate_token_file(file: &File, path: &Path) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("admin token {} is not a regular file", path.display()),
        ));
    }
    // The daemon commonly runs as root, but the same invariant makes local
    // development safe when it runs under an ordinary account.
    let daemon_uid = unsafe { libc::geteuid() };
    if metadata.uid() != daemon_uid || metadata.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "admin token {} must be owned by uid {daemon_uid} and not accessible by group/other",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn read_token(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    validate_token_file(&file, path)?;
    let mut encoded = String::new();
    Read::by_ref(&mut file)
        .take(1024)
        .read_to_string(&mut encoded)?;
    let decoded = URL_SAFE_NO_PAD.decode(encoded.trim()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decode admin token {}: {e}", path.display()),
        )
    })?;
    <[u8; 32]>::try_from(decoded.as_slice()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("admin token {} must decode to 32 bytes", path.display()),
        )
    })
}

fn create_token(path: &Path) -> io::Result<[u8; 32]> {
    let mut token = [0u8; 32];
    getrandom::fill(&mut token).map_err(random_error)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    writeln!(file, "{}", URL_SAFE_NO_PAD.encode(token))?;
    file.sync_all()?;
    Ok(token)
}

fn session_id(headers: &HeaderMap, cookie_name: &str) -> Option<[u8; 32]> {
    let jar = CookieJar::from_headers(headers);
    let encoded = jar.get(cookie_name)?.value();
    let decoded = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    <[u8; 32]>::try_from(decoded.as_slice()).ok()
}

fn unauthorized() -> Response {
    let body = ApiErrorBody {
        error: "authentication_required".into(),
        message: "administrator login required".into(),
    };
    let mut response = (StatusCode::UNAUTHORIZED, Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

pub async fn login(
    State(auth): State<AdminAuth>,
    jar: CookieJar,
    Json(request): Json<LoginRequest>,
) -> Response {
    if !auth.token_matches(&request.token) {
        tokio::time::sleep(Duration::from_millis(250)).await;
        return unauthorized();
    }
    let id = match auth.create_session().await {
        Ok(id) => id,
        Err(e) => {
            let body = ApiErrorBody {
                error: "internal".into(),
                message: format!("create administrator session: {e}"),
            };
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response();
        }
    };
    let cookie = Cookie::build((auth.cookie_name().to_owned(), URL_SAFE_NO_PAD.encode(id)))
        .http_only(true)
        .same_site(SameSite::Strict)
        .path("/")
        .max_age(time::Duration::seconds(SESSION_TTL.as_secs() as i64))
        .build();
    no_store((jar.add(cookie), StatusCode::NO_CONTENT).into_response())
}

pub async fn session(State(auth): State<AdminAuth>, headers: HeaderMap) -> Response {
    let Some(id) = session_id(&headers, auth.cookie_name()) else {
        return unauthorized();
    };
    if auth.session_is_valid(&id).await {
        no_store(StatusCode::NO_CONTENT.into_response())
    } else {
        unauthorized()
    }
}

pub async fn logout(State(auth): State<AdminAuth>, headers: HeaderMap, jar: CookieJar) -> Response {
    if let Some(id) = session_id(&headers, auth.cookie_name()) {
        auth.revoke_session(&id).await;
    }
    let removal = Cookie::build((auth.cookie_name().to_owned(), ""))
        .path("/")
        .build();
    no_store((jar.remove(removal), StatusCode::NO_CONTENT).into_response())
}

pub async fn require_admin_session(
    State(auth): State<AdminAuth>,
    request: Request,
    next: Next,
) -> Response {
    let Some(id) = session_id(request.headers(), auth.cookie_name()) else {
        return unauthorized();
    };
    if !auth.session_is_valid(&id).await {
        return unauthorized();
    }
    next.run(request).await
}

#[derive(Clone, Debug)]
pub struct PeerCredentials {
    pub uid: u32,
}

impl Connected<IncomingStream<'_, UnixListener>> for PeerCredentials {
    fn connect_info(stream: IncomingStream<'_, UnixListener>) -> Self {
        let cred = stream
            .io()
            .peer_cred()
            .expect("SO_PEERCRED is always available on AF_UNIX");
        Self { uid: cred.uid() }
    }
}

/// Same-uid policy: a request whose connection's peer uid differs from the
/// daemon's effective uid is rejected with `403`. There is no allowlist
/// this slice — multi-uid policies land in a future slice. Wired in via
/// `axum::middleware::from_fn`.
pub async fn enforce_same_uid(
    ConnectInfo(peer): ConnectInfo<PeerCredentials>,
    request: Request,
    next: Next,
) -> Response {
    // SAFETY: `geteuid` is a vDSO syscall; cannot fail.
    let daemon_uid = unsafe { libc::geteuid() };
    if peer.uid != daemon_uid {
        let body = ApiErrorBody {
            error: "peer_uid_mismatch".into(),
            message: format!(
                "peer uid {} is not allowed (daemon uid {daemon_uid})",
                peer.uid
            ),
        };
        return (StatusCode::FORBIDDEN, Json(body)).into_response();
    }
    next.run(request).await
}

/// DNS-rebinding guard for the loopback TCP bind. `Sec-Fetch-Site`
/// compares *names*, not addresses: if `attacker.com` resolves to
/// 127.0.0.1, a fetch to `http://attacker.com:7878` is `same-origin`
/// from the browser's point of view and sails past the CSRF guard —
/// for reads as well as writes, so this check applies to every method.
/// The daemon is only ever legitimately addressed by a loopback name;
/// anything else in `Host` means a rebound origin.
///
/// A missing `Host` header is allowed: browsers always send Host (or
/// `:authority`, which hyper maps into the URI), so its absence implies
/// a non-browser client that carries no rebinding risk.
pub async fn enforce_loopback_host(request: Request, next: Next) -> Response {
    let host = request
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(str::to_string)
        .or_else(|| request.uri().host().map(str::to_string));
    if let Some(host) = host {
        let name = if let Some(rest) = host.strip_prefix('[') {
            // Bracketed IPv6 literal: `[::1]` or `[::1]:7878`.
            rest.split_once(']').map(|(h, _)| h).unwrap_or(rest)
        } else {
            host.rsplit_once(':').map(|(h, _)| h).unwrap_or(&host)
        };
        if !matches!(name, "127.0.0.1" | "localhost" | "::1") {
            let body = ApiErrorBody {
                error: "bad_host".into(),
                message: format!("host {host:?} is not a loopback name"),
            };
            return (StatusCode::FORBIDDEN, Json(body)).into_response();
        }
    }
    next.run(request).await
}

/// CSRF guard for the loopback TCP bind. Mutating methods (POST / PUT /
/// PATCH / DELETE) are blocked when the browser-supplied
/// `Sec-Fetch-Site` header indicates a cross-origin request — that
/// header is always present on modern browser-issued fetches, always
/// trustworthy (a page cannot forge it cross-origin), and absent on
/// non-browser callers (curl, `arctern-client`, `reqwest`).
///
/// The rule:
/// - GET / HEAD / OPTIONS — always allowed (no side effects).
/// - Mutating method + `Sec-Fetch-Site: same-origin` or `none` —
///   allowed.
/// - Mutating method + `Sec-Fetch-Site: same-site` or `cross-site` —
///   403.
/// - Mutating method + header absent — allowed (assumed to be a
///   non-browser CLI / library client).
///
/// CSRF threat model: a malicious page in another tab fetches into
/// `127.0.0.1:7878` with the user's session cookie. Sec-Fetch-Site blocks
/// this because the browser cannot be tricked into omitting or rewriting
/// the header from a cross-origin context.
pub async fn enforce_csrf(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let mutating = matches!(
        method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if mutating && let Some(sfs) = request.headers().get("sec-fetch-site") {
        let v = sfs.to_str().unwrap_or("");
        if v != "same-origin" && v != "none" {
            let body = ApiErrorBody {
                error: "cross_origin".into(),
                message: format!("cross-origin {method} blocked (Sec-Fetch-Site: {v})"),
            };
            return (StatusCode::FORBIDDEN, Json(body)).into_response();
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn temp_dir(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "arctern-auth-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn token_file_is_created_private_and_reloads() {
        let dir = temp_dir("create");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();

        let auth = AdminAuth::load_or_create(&dir).unwrap();
        let encoded = std::fs::read_to_string(auth.token_path()).unwrap();
        assert!(auth.token_matches(encoded.trim()));
        assert_eq!(
            std::fs::metadata(auth.token_path()).unwrap().mode() & 0o777,
            0o600
        );

        let reloaded = AdminAuth::load_or_create(&dir).unwrap();
        assert!(reloaded.token_matches(encoded.trim()));
        assert_eq!(auth.cookie_name(), reloaded.cookie_name());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn cookie_namespace_is_stable_and_daemon_specific() {
        let first = AdminAuth::for_tests([1; 32]);
        let same = AdminAuth::for_tests([1; 32]);
        let second = AdminAuth::for_tests([2; 32]);

        assert_eq!(first.cookie_name(), same.cookie_name());
        assert_ne!(first.cookie_name(), second.cookie_name());
        assert!(first.cookie_name().starts_with("arctern_session_"));
    }

    #[test]
    fn token_file_rejects_group_or_other_access() {
        let dir = temp_dir("permissions");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let auth = AdminAuth::load_or_create(&dir).unwrap();
        std::fs::set_permissions(auth.token_path(), std::fs::Permissions::from_mode(0o644))
            .unwrap();

        let error = AdminAuth::load_or_create(&dir)
            .err()
            .expect("insecure token rejected");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
