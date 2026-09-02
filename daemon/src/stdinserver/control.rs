//! Server side of the control channel: a tarpc `ArcternControl`
//! implementation over the channel's stdio. tarpc owns the demux —
//! each in-flight request runs on its own task (so one slow
//! `list_receiver_guids` over a huge dataset cannot head-of-line block
//! the UI proxy's other queries) and responses correlate by tarpc's
//! request ids, not arrival order.
//!
//! Handlers translate `zfskit::ZfsError` and friends into
//! `WireError { code, message }` rather than letting them escape; the
//! caller never sees a process exit short of EOF.

use std::sync::Arc;

use arctern_config::zfs_names::validate_dataset_name;
use arctern_config::{AllowedClient, Config};
use arctern_transport::{
    ArcternControl, ErrorCode, GuidsReply, ProxyReply, SnapshotEntry, WireError,
    compile_prefix_regex,
};
use futures_util::StreamExt;
use sqlx::SqlitePool;
use tarpc::server::{BaseChannel, Channel};
use tokio::io::{AsyncRead, AsyncWrite};
use zfskit::ZfsError;
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;

use super::recv_lock::RecvLocks;

/// Run the control channel until stdin EOF or a fatal transport error.
/// `acl` scopes destroy / discard operations; `runner` is the
/// zfskit CommandRunner the dispatch process opened (typically a
/// `RealRunner` invoking local `zfs(8)`).
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    config: Arc<Config>,
    acl: AllowedClient,
    pool: Option<Arc<SqlitePool>>,
    recv_locks: RecvLocks,
    reader: R,
    writer: W,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let transport = arctern_transport::transport(tokio::io::join(reader, writer));
    let server = ControlServer {
        runner,
        config,
        acl,
        pool,
        recv_locks,
    };
    BaseChannel::with_defaults(transport)
        .execute(server.serve())
        .for_each_concurrent(None, |handler| async {
            tokio::spawn(handler);
        })
        .await;
    Ok(())
}

#[derive(Clone)]
struct ControlServer {
    runner: Arc<dyn CommandRunner>,
    config: Arc<Config>,
    acl: AllowedClient,
    pool: Option<Arc<SqlitePool>>,
    recv_locks: RecvLocks,
}

impl ArcternControl for ControlServer {
    async fn list_receiver_guids(
        self,
        _ctx: tarpc::context::Context,
        dataset: String,
        prefix_regex: Option<String>,
    ) -> Result<GuidsReply, WireError> {
        enforce_control_acl(&self.acl, "control:list_snapshots", true)?;
        let (snapshots, receive_resume_token) = collect_receiver_snapshots(
            self.runner.as_ref(),
            &self.acl,
            &dataset,
            prefix_regex.as_deref(),
        )
        .await?;
        Ok(GuidsReply {
            guids: snapshots.into_iter().map(|s| s.guid).collect(),
            receive_resume_token,
        })
    }

    async fn discard_partial_recv(
        self,
        _ctx: tarpc::context::Context,
        dataset: String,
    ) -> Result<(), WireError> {
        enforce_control_acl(&self.acl, "control:discard_partial_recv", false)?;
        validate_dataset_name(&dataset).map_err(|e| {
            WireError::new(
                ErrorCode::BadRequest,
                format!("invalid dataset {dataset:?}: {e}"),
            )
        })?;
        enforce_root_fs(&self.acl, &dataset)?;
        // Never abort `%recv` while a receive channel still owns it. This
        // also serializes cleanup with admission of the replacement stream.
        let _recv_lock = self.recv_locks.acquire(&dataset).map_err(|e| {
            tracing::warn!(target = %dataset, error = %e, "partial receive cleanup refused");
            WireError::new(ErrorCode::Zfs, e.to_string())
        })?;
        zfskit::recv::abort_partial(self.runner.as_ref(), &dataset)
            .await
            .map_err(|e| {
                WireError::new(zfs_error_code(&e), format!("abort_partial {dataset}: {e}"))
            })
    }

    async fn log_cursor(self, _ctx: tarpc::context::Context) -> u64 {
        // ACL-free by design: this doubles as the link liveness probe
        // and leaks nothing but a monotonically increasing counter.
        match &self.pool {
            Some(p) => match crate::state::log_events::cursor(p).await {
                Ok(id) => id as u64,
                Err(e) => {
                    tracing::warn!(error = %e, "log_events cursor query failed");
                    0
                }
            },
            None => 0,
        }
    }

    async fn proxy(
        self,
        _ctx: tarpc::context::Context,
        method: String,
        path: String,
        body: Option<String>,
    ) -> Result<ProxyReply, WireError> {
        handle_proxy(&self.config, &self.acl, &method, &path, body).await
    }
}

fn enforce_control_acl(
    acl: &AllowedClient,
    op: &'static str,
    allow_legacy_control: bool,
) -> Result<(), WireError> {
    if acl.operations.iter().any(|configured| configured == op)
        || (allow_legacy_control
            && acl
                .operations
                .iter()
                .any(|configured| configured == "control"))
    {
        return Ok(());
    }
    Err(WireError::new(
        ErrorCode::Unauthorized,
        format!(
            "identity {:?} is not allowed for control operation {op:?}",
            acl.identity
        ),
    ))
}

/// Reject `dataset` if the ACL has a `root_fs` set and `dataset` is not
/// equal to or a descendant of it. No root_fs configured means no
/// restriction.
fn enforce_root_fs<'a>(acl: &'a AllowedClient, dataset: &'a str) -> Result<(), WireError> {
    let Some(root) = acl.root_fs.as_deref() else {
        return Ok(());
    };
    if dataset == root {
        return Ok(());
    }
    let prefix = format!("{root}/");
    if dataset.starts_with(&prefix) {
        return Ok(());
    }
    Err(WireError::new(
        ErrorCode::Unauthorized,
        format!("{dataset:?} is not under allowed root_fs {root:?}"),
    ))
}

/// Shared core for the snapshot-inventory requests. Validates the
/// dataset, enforces `root_fs`, lists matching snapshots and reads the
/// receive resume token. A missing dataset (first replication) is the
/// non-error empty case.
async fn collect_receiver_snapshots(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
    prefix_regex: Option<&str>,
) -> Result<(Vec<SnapshotEntry>, Option<String>), WireError> {
    validate_dataset_name(dataset).map_err(|e| {
        WireError::new(
            ErrorCode::BadRequest,
            format!("invalid dataset {dataset:?}: {e}"),
        )
    })?;
    enforce_root_fs(acl, dataset)?;
    let regex = compile_prefix_regex(prefix_regex).map_err(|e| {
        WireError::new(
            ErrorCode::BadRequest,
            format!("compile prefix_regex {:?}: {e}", prefix_regex.unwrap_or("")),
        )
    })?;
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = match zfskit::dataset::list(runner, &opts).await {
        Ok(v) => v,
        // First-replication shape: receiver dataset doesn't exist yet.
        Err(ZfsError::DatasetNotFound { .. }) => return Ok((vec![], None)),
        Err(e) => {
            return Err(WireError::new(
                zfs_error_code(&e),
                format!("list {dataset}: {e}"),
            ));
        }
    };
    let snapshots: Vec<SnapshotEntry> = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if let Some(re) = &regex
                && !re.is_match(&snap_name)
            {
                return None;
            }
            let guid = e
                .properties
                .get("guid")
                .and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(SnapshotEntry {
                name: snap_name,
                guid,
                createtxg,
            })
        })
        .collect();
    let receive_resume_token = match zfskit::recv::receive_resume_token(runner, dataset).await {
        Ok(opt) => opt,
        Err(ZfsError::DatasetNotFound { .. }) => None,
        Err(e) => {
            tracing::warn!(error = %e, dataset, "receive_resume_token query failed");
            None
        }
    };
    Ok((snapshots, receive_resume_token))
}

fn zfs_error_code(e: &ZfsError) -> ErrorCode {
    match e {
        ZfsError::DatasetNotFound { .. } => ErrorCode::NotFound,
        _ => ErrorCode::Zfs,
    }
}

/// Generic passthrough to the local daemon's HTTP API. GET rides the
/// read scope (legacy `control` allowed); mutating methods require the
/// explicit `control:proxy_admin` grant — that single line in the
/// receiver's config is the switch between "sender may watch this
/// host" and "sender may manage this host like its own".
/// Ceiling on a proxied response body.
///
/// The reply crosses the control channel as one length-delimited frame
/// capped at `MAX_FRAME_LEN`, with the body re-encoded as a JSON string
/// inside it — escaping only grows it. Exceeding the frame does not fail
/// the request, it fails the ENCODE, and that tears down the whole tarpc
/// transport: the control channel drops and replication stops until the
/// link reconnects. One view losing its data is the better failure.
///
/// Well under the frame so escaping cannot close the gap. A response
/// this large is a dataset with tens of thousands of snapshots, which
/// the console cannot usefully render anyway.
const MAX_PROXY_BODY: usize = 2 << 20;

/// Endpoints that stream rather than answer. Matched exactly:
/// `/api/v1/events/recent` is an ordinary JSON tail and the peer events
/// bridge fetches it through the proxy.
fn is_streaming_path(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    matches!(path, "/api/v1/events" | "/api/v1/jobs/stream")
}

async fn handle_proxy(
    config: &Config,
    acl: &AllowedClient,
    method: &str,
    path: &str,
    body: Option<String>,
) -> Result<ProxyReply, WireError> {
    if method == "GET" {
        enforce_control_acl(acl, "control:proxy_read", true)?;
    } else if method == "POST" || method == "DELETE" {
        enforce_control_acl(acl, "control:proxy_admin", false)?;
    } else {
        return Err(WireError::new(
            ErrorCode::BadRequest,
            format!("unsupported proxy method {method:?}"),
        ));
    }
    // Absolute API paths only — no scheme/host smuggling, no traversal.
    if !path.starts_with("/api/v1/") || path.contains("..") {
        return Err(WireError::new(
            ErrorCode::BadRequest,
            format!("proxy path must be under /api/v1/: {path:?}"),
        ));
    }
    // The proxy grants a client the API of THIS host, not of the hosts
    // this host can reach. `/api/v1/peers/{peer}/proxy/...` is itself a
    // forwarding route, so allowing it would let a client with
    // `control:proxy_read` read a third host's config under our identity
    // — and with `control:proxy_admin`, mutate it. `/api/v1/peers` alone
    // is refused for the same reason: it enumerates our peers, their ssh
    // targets and their fingerprints.
    if path == "/api/v1/peers" || path.starts_with("/api/v1/peers/") {
        return Err(WireError::new(
            ErrorCode::Unauthorized,
            "proxy does not forward peer routes: the grant covers this host only".to_string(),
        ));
    }
    // The proxy reads a response to completion, so an endpoint that never
    // completes never returns: the request would hold a handler task and
    // an open socket for as long as the client cared to keep it. Live
    // data has its own channels — `stdinserver <job> events` and the
    // dedicated peer job stream — which is what the console actually
    // uses; `/api/v1/events/recent` stays reachable because the events
    // bridge fetches its backlog through here.
    if is_streaming_path(path) {
        return Err(WireError::new(
            ErrorCode::BadRequest,
            format!("{path:?} is a stream; the proxy carries request/response only"),
        ));
    }
    match arctern_client::raw(
        &daemon_socket(config),
        method,
        path,
        body.map(String::into_bytes),
    )
    .await
    {
        Ok((_, bytes)) if bytes.len() > MAX_PROXY_BODY => Err(WireError::new(
            ErrorCode::Internal,
            format!(
                "proxied response is {} bytes, over the {MAX_PROXY_BODY}-byte proxy limit",
                bytes.len()
            ),
        )),
        Ok((status, bytes)) => Ok(ProxyReply {
            status,
            body: String::from_utf8_lossy(&bytes).into_owned(),
        }),
        Err(e) => Err(WireError::new(
            ErrorCode::Internal,
            format!(
                "local daemon unreachable at {}: {e}",
                daemon_socket(config).display()
            ),
        )),
    }
}

/// Where the local daemon's API socket lives. The stdinserver process
/// is spawned by sshd, so the daemon's `--socket` flag is invisible
/// here — the config's `socket` key is the shared rendezvous point.
fn daemon_socket(config: &Config) -> std::path::PathBuf {
    config
        .socket
        .clone()
        .unwrap_or_else(crate::default_socket_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_transport::ArcternControlClient;
    use std::sync::Arc;
    use zfskit::runner::{Cmd, RecordingRunner};

    fn acl(root_fs: Option<&str>) -> AllowedClient {
        acl_with_ops(root_fs, &["control", "recv"])
    }

    fn acl_with_ops(root_fs: Option<&str>, operations: &[&str]) -> AllowedClient {
        AllowedClient {
            identity: "test".into(),
            fingerprint: None,
            jobs: vec!["backup".into()],
            operations: operations.iter().map(|op| (*op).to_string()).collect(),
            root_fs: root_fs.map(str::to_string),
            recv: Default::default(),
        }
    }

    fn cfg() -> Arc<Config> {
        // Hermetic: point the UDS-proxy paths at a socket that cannot
        // exist so tests never talk to a real daemon on the dev host.
        Arc::new(Config {
            socket: Some(std::path::PathBuf::from("/nonexistent/arctern-test.sock")),
            ..Config::default()
        })
    }

    /// End-to-end tarpc roundtrip over duplex pipes with a
    /// RecordingRunner for ZFS. Dropping the returned client ends the
    /// server loop via transport EOF.
    fn client(runner: Arc<dyn CommandRunner>, acl: AllowedClient) -> ArcternControlClient {
        let lock_dir = std::env::temp_dir().join(format!(
            "arctern-control-test-{}-{}",
            std::process::id(),
            getrandom::u64().expect("random suffix")
        ));
        client_with_locks(runner, acl, RecvLocks::new(&lock_dir))
    }

    fn client_with_locks(
        runner: Arc<dyn CommandRunner>,
        acl: AllowedClient,
        recv_locks: RecvLocks,
    ) -> ArcternControlClient {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (areader, awriter) = tokio::io::split(a);
        tokio::spawn(run(runner, cfg(), acl, None, recv_locks, areader, awriter));
        ArcternControlClient::new(
            tarpc::client::Config::default(),
            arctern_transport::transport(b),
        )
        .spawn()
    }

    fn ctx() -> tarpc::context::Context {
        tarpc::context::current()
    }

    #[tokio::test]
    async fn list_receiver_guids_returns_empty_on_dataset_not_found() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid",
                "tank/missing",
            ]),
            Vec::new(),
            b"cannot open 'tank/missing': dataset does not exist".to_vec(),
            1,
        ));
        let c = client(runner, acl(None));
        let r = c
            .list_receiver_guids(ctx(), "tank/missing".into(), None)
            .await
            .unwrap()
            .unwrap();
        assert!(r.guids.is_empty());
        assert_eq!(r.receive_resume_token, None);
    }

    #[tokio::test]
    async fn list_receiver_guids_enforces_root_fs() {
        let c = client(
            Arc::new(RecordingRunner::new()),
            acl(Some("tank/backups/laptop")),
        );
        let e = c
            .list_receiver_guids(ctx(), "tank/other".into(), None)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Unauthorized);
    }

    #[tokio::test]
    async fn discard_partial_recv_rejects_invalid_dataset_name() {
        let c = client(
            Arc::new(RecordingRunner::new()),
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:discard_partial_recv", "recv"],
            ),
        );
        let e = c
            .discard_partial_recv(ctx(), "tank/backups/laptop#bookmark".into())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::BadRequest);
        assert!(e.message.contains("invalid dataset"));
    }

    #[tokio::test]
    async fn discard_partial_recv_requires_fine_grained_acl() {
        let c = client(
            Arc::new(RecordingRunner::new()),
            acl(Some("tank/backups/laptop")),
        );
        let e = c
            .discard_partial_recv(ctx(), "tank/backups/laptop".into())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Unauthorized);
        assert!(e.message.contains("control:discard_partial_recv"));
    }

    #[tokio::test]
    async fn discard_partial_recv_refuses_active_receiver() {
        let dir = std::env::temp_dir().join(format!(
            "arctern-control-busy-test-{}-{}",
            std::process::id(),
            getrandom::u64().expect("random suffix")
        ));
        let recv_locks = RecvLocks::new(&dir);
        let active = recv_locks.acquire("tank/backups/laptop").unwrap();
        let c = client_with_locks(
            Arc::new(RecordingRunner::new()),
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:discard_partial_recv", "recv"],
            ),
            recv_locks,
        );
        let e = c
            .discard_partial_recv(ctx(), "tank/backups/laptop".into())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Zfs);
        assert!(e.message.contains("receive already active"));
        drop(active);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn list_receiver_guids_accepts_root_itself() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid",
                "tank/backups/laptop",
            ]),
            Vec::new(),
            b"cannot open 'tank/backups/laptop': dataset does not exist".to_vec(),
            1,
        ));
        let c = client(runner, acl(Some("tank/backups/laptop")));
        let r = c
            .list_receiver_guids(ctx(), "tank/backups/laptop".into(), None)
            .await
            .unwrap();
        assert!(r.is_ok(), "got {r:?}");
    }

    #[tokio::test]
    async fn list_receiver_guids_rejects_invalid_dataset_name() {
        let c = client(
            Arc::new(RecordingRunner::new()),
            acl(Some("tank/backups/laptop")),
        );
        let e = c
            .list_receiver_guids(ctx(), "tank/backups/laptop/../escape".into(), None)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn log_cursor_is_zero_without_sqlite() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        assert_eq!(c.log_cursor(ctx()).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn proxy_get_errors_honestly_when_local_daemon_unreachable() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        let e = c
            .proxy(ctx(), "GET".into(), "/api/v1/jobs".into(), None)
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal);
        assert!(
            e.message.contains("local daemon unreachable"),
            "got: {}",
            e.message
        );
    }

    // The proxy forwards to THIS host's API. Peer routes are themselves
    // forwarding routes, so honouring them would turn one `control` grant
    // into read (or, with proxy_admin, write) access to every host this
    // one can reach, under this host's identity.
    #[tokio::test]
    async fn proxy_refuses_to_forward_peer_routes() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        for path in [
            "/api/v1/peers",
            "/api/v1/peers/elsewhere/proxy/api/v1/config",
            "/api/v1/peers/elsewhere/jobs/stream",
        ] {
            let e = c
                .proxy(ctx(), "GET".into(), path.into(), None)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(e.code, ErrorCode::Unauthorized, "path {path} was allowed");
            assert!(e.message.contains("this host only"), "got: {}", e.message);
        }
    }

    // A dataset segment may legitimately contain the word "peers"; only
    // the route prefix is refused.
    #[tokio::test]
    async fn proxy_still_forwards_paths_that_merely_mention_peers() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        let e = c
            .proxy(
                ctx(),
                "GET".into(),
                "/api/v1/datasets/tank%2Fpeers/snapshots".into(),
                None,
            )
            .await
            .unwrap()
            .unwrap_err();
        // Reaches the forwarding step (no local daemon in the test), so it
        // was not refused by the guard.
        assert_eq!(e.code, ErrorCode::Internal);
    }

    // The proxy reads a response to completion, so a stream would never
    // return: the request would pin a handler task and a socket for as
    // long as the client liked. Live data has its own channels.
    #[tokio::test]
    async fn proxy_refuses_streaming_endpoints() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        for path in ["/api/v1/events", "/api/v1/jobs/stream"] {
            let e = c
                .proxy(ctx(), "GET".into(), path.into(), None)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(e.code, ErrorCode::BadRequest, "{path} was allowed");
            assert!(e.message.contains("stream"), "got: {}", e.message);
        }
    }

    // The events bridge fetches its backlog through the proxy, so the
    // JSON tail must stay reachable — matching by prefix would have
    // broken it.
    #[test]
    fn the_json_event_tail_is_not_a_stream() {
        assert!(!is_streaming_path("/api/v1/events/recent"));
        assert!(!is_streaming_path("/api/v1/events/recent?limit=100"));
        assert!(is_streaming_path("/api/v1/events"));
        // A query string does not smuggle a stream past the check.
        assert!(is_streaming_path("/api/v1/events?since=5"));
    }

    #[tokio::test]
    async fn proxy_post_requires_proxy_admin() {
        let c = client(Arc::new(RecordingRunner::new()), acl(None));
        let e = c
            .proxy(
                ctx(),
                "POST".into(),
                "/api/v1/jobs/databak/wakeup".into(),
                None,
            )
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Unauthorized);
    }
}
