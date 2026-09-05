//! arctern daemon binary.
//!
//! - `daemon` runs the scheduler, the UNIX-socket API and the loopback
//!   web console.
//! - `stdinserver-dispatch <identity>` is the SSH transport entry point
//!   invoked by sshd via authorized_keys `command="..."`.
//! - `configcheck <path>` validates a config file for CI / pre-deploy.
//! - `openapi` prints the OpenAPI spec for the UI's generated client.

// musl's allocator is noticeably slower under multithreaded load than
// glibc's; mimalloc levels the static-musl release builds with the
// glibc ones (and is a mild win there too).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::future::Future;
use std::io::{ErrorKind, Write};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clap::{Parser, Subcommand};
use sqlx::SqlitePool;
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

mod app_state;
mod auth;
mod configcheck;
mod error;
mod handlers;
mod inventory;
mod jobs;
mod peer;
mod router;
mod state;
mod stdinserver;

const DEFAULT_STATE_DIR: &str = "/var/lib/arctern";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
/// Servers and jobs stop together — see `orderly_shutdown` — so they
/// share one stage: while it is current, either could be what is holding
/// things up, and the message says so.
const SHUTDOWN_STAGE_SERVERS_AND_JOBS: u8 = 0;
const SHUTDOWN_STAGE_PEERS: u8 = 1;
const SHUTDOWN_STAGE_ARC: u8 = 2;
const SHUTDOWN_STAGE_RETENTION: u8 = 3;
const SHUTDOWN_STAGE_SOCKET: u8 = 4;
const SHUTDOWN_STAGE_COMPLETE: u8 = 5;

fn shutdown_stage_name(stage: u8) -> &'static str {
    match stage {
        SHUTDOWN_STAGE_SERVERS_AND_JOBS => "server drain and job shutdown",
        SHUTDOWN_STAGE_PEERS => "peer reconnect shutdown",
        SHUTDOWN_STAGE_ARC => "ARC sweeper shutdown",
        SHUTDOWN_STAGE_RETENTION => "retention sweeper shutdown",
        SHUTDOWN_STAGE_SOCKET => "UNIX socket cleanup",
        SHUTDOWN_STAGE_COMPLETE => "complete",
        _ => "unknown",
    }
}

async fn supervise_shutdown<F>(
    cancellation: CancellationToken,
    shutdown: F,
    stage: Arc<AtomicU8>,
    timeout: Duration,
    socket_path: PathBuf,
) -> eyre::Result<()>
where
    F: Future<Output = eyre::Result<()>>,
{
    tokio::pin!(shutdown);
    tokio::select! {
        result = &mut shutdown => result,
        _ = cancellation.cancelled() => {
            match tokio::time::timeout(timeout, &mut shutdown).await {
                Ok(result) => result,
                Err(_) => {
                    let stage = shutdown_stage_name(stage.load(Ordering::SeqCst));
                    tracing::error!(stage, timeout_seconds = timeout.as_secs(), "shutdown deadline exceeded");
                    if let Err(error) = std::fs::remove_file(&socket_path)
                        && error.kind() != ErrorKind::NotFound
                    {
                        tracing::warn!(path = %socket_path.display(), %error, "remove UNIX socket after shutdown timeout failed");
                    }
                    Err(eyre::eyre!(
                        "shutdown timed out after {}s during {stage}",
                        timeout.as_secs()
                    ))
                }
            }
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "arctern", version, about = "ZFS replication daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the daemon (HTTP API server over a UNIX socket).
    Daemon {
        /// Override the socket path. Default resolution order:
        /// `$XDG_RUNTIME_DIR/arctern.sock`, falling back to
        /// `/run/arctern.sock` when `$XDG_RUNTIME_DIR` is unset.
        #[arg(long)]
        socket: Option<PathBuf>,

        /// Path to the TOML configuration file. Defaults to
        /// `/etc/arctern/arctern.toml`. The daemon refuses to start
        /// without a readable, valid config.
        #[arg(long, default_value = "/etc/arctern/arctern.toml")]
        config: PathBuf,

        /// Loopback address for the admin UI and HTTP API.
        /// Port 0 is useful for isolated test instances.
        #[arg(long, default_value = "127.0.0.1:7878")]
        http_address: SocketAddr,
    },
    /// SSH transport entry point invoked by sshd via authorized_keys
    /// `command="..."`. The single positional is the identity name —
    /// the actual command (`arctern stdinserver <job> <op>`) arrives
    /// via `SSH_ORIGINAL_COMMAND`.
    StdinserverDispatch {
        identity: String,
        /// Path to the daemon's config (same default as `daemon`).
        #[arg(long, default_value = "/etc/arctern/arctern.toml")]
        config: PathBuf,
    },
    /// One-shot validation for CI / pre-deploy.
    Configcheck { path: PathBuf },
    /// Print the OpenAPI spec as JSON to stdout and exit. Used by the
    /// admin-ui build to regenerate `admin-ui/openapi.json` and the TS
    /// client. No daemon startup, no config load.
    Openapi,
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Daemon {
            socket,
            config,
            http_address,
        } => run_daemon(socket, config, http_address),
        Command::StdinserverDispatch { identity, config } => {
            run_stdinserver_dispatch(identity, config)
        }
        Command::Configcheck { path } => configcheck::run(&path),
        Command::Openapi => {
            let spec = router::openapi_spec();
            let json = serde_json::to_string_pretty(&spec)
                .map_err(|e| eyre::eyre!("serialize openapi: {e}"))?;
            println!("{json}");
            Ok(())
        }
    }
}

fn state_dir(config: &arctern_config::Config) -> PathBuf {
    config
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR))
}

/// The tracing layers both processes share: `RUST_LOG`-filtered stderr
/// (ANSI only on a terminal; under systemd or an SSH pipe it would land
/// in a log), plus the SQLite layer when a pool is available. tarpc's
/// per-RPC INFO lines are protocol noise, so they drop to WARN. Returns
/// the broadcast the SQLite layer publishes events on.
fn init_tracing(
    pool: Option<Arc<SqlitePool>>,
) -> tokio::sync::broadcast::Sender<arctern_api::LogEvent> {
    use std::io::IsTerminal as _;
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tarpc=warn"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    let (events_tx, _) = tokio::sync::broadcast::channel::<arctern_api::LogEvent>(256);
    match pool {
        Some(pool) => {
            let (sqlite_layer, _writer) =
                state::log_events::SqliteLogLayer::with_writer(pool, events_tx.clone());
            Registry::default()
                .with(env_filter)
                .with(fmt_layer)
                .with(sqlite_layer.with_filter(state::log_events::SqliteLogLayer::filter()))
                .init();
        }
        None => Registry::default().with(env_filter).with(fmt_layer).init(),
    }
    events_tx
}

#[tokio::main(flavor = "current_thread")]
async fn run_stdinserver_dispatch(identity: String, config: PathBuf) -> eyre::Result<()> {
    let cfg =
        arctern_config::load_from_path(&config).map_err(|e| eyre::eyre!("config load: {e}"))?;
    // A failure to open the pool falls back to stderr-only tracing so the
    // dispatch still runs.
    let pool = match state::open(&state_dir(&cfg)).await {
        Ok(p) => Some(Arc::new(p)),
        Err(e) => {
            eprintln!(
                "stdinserver-dispatch: state open failed ({e}); continuing without SQLite event log"
            );
            None
        }
    };
    init_tracing(pool.clone());
    stdinserver::dispatch::run_with(&identity, cfg, pool).await
}

/// Resolve the socket path the daemon should bind to. Priority:
/// `--socket` flag, then the config's `socket` key (which
/// `stdinserver-dispatch` also reads, so the two processes agree),
/// then the environment default.
fn resolve_socket_path(arg: Option<PathBuf>, config: Option<&Path>) -> PathBuf {
    arg.or_else(|| config.map(Path::to_path_buf))
        .unwrap_or_else(default_socket_path)
}

/// Environment fallback shared by the daemon bind and the
/// stdinserver's client side.
pub(crate) fn default_socket_path() -> PathBuf {
    if let Ok(rt) = std::env::var("XDG_RUNTIME_DIR")
        && !rt.is_empty()
    {
        return PathBuf::from(rt).join("arctern.sock");
    }
    PathBuf::from("/run/arctern.sock")
}

fn bind_unix_socket(path: &Path) -> eyre::Result<UnixListener> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(eyre::eyre!("remove stale socket {}: {e}", path.display())),
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// The daemon runs on the ZFS host, so its runner is `RealRunner`.
/// `ZFSKIT_SSH_TARGET` swaps in the SSH runner the integration tests
/// use to drive the daemon against the test VM.
fn zfs_facade() -> eyre::Result<zfskit::Zfs> {
    match std::env::var("ZFSKIT_SSH_TARGET") {
        Ok(s) if !s.is_empty() => Ok(zfskit::Zfs::with_runner(
            zfskit::SshCommandRunner::from_env()
                .map_err(|e| eyre::eyre!("ZFSKIT_SSH_TARGET configuration: {e}"))?,
        )),
        _ => Ok(zfskit::Zfs::new()),
    }
}

/// One eager-reconnect task per `[[peers]]` entry, all sharing the
/// peers map the push jobs and handlers read.
struct PeerLinks {
    state: peer::state::PeersState,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl PeerLinks {
    fn spawn(peers: &[arctern_config::PeerConfig]) -> Self {
        let state = peer::state::PeersState::new();
        let cancel = CancellationToken::new();
        let tasks = peers
            .iter()
            .map(|p| {
                let state = state.clone();
                let cancel = cancel.clone();
                let name = p.name.clone();
                let routes = p.routes.clone();
                tokio::spawn(async move {
                    peer::reconnect::run_for_peer(state, name, routes, cancel).await;
                })
            })
            .collect();
        Self {
            state,
            cancel,
            tasks,
        }
    }

    async fn shutdown(self) {
        self.cancel.cancel();
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

fn spawn_jobs(
    config: &arctern_config::Config,
    jobs: Vec<arctern_config::JobConfig>,
    ctx: &jobs::JobContext,
    peers: &peer::state::PeersState,
) -> eyre::Result<Arc<jobs::JobManager>> {
    let manager = Arc::new(jobs::JobManager::new());
    for job in jobs {
        match job {
            arctern_config::JobConfig::Snap(s) => {
                manager.spawn(Arc::new(jobs::snap::SnapCycle::job(s)), ctx.clone());
            }
            arctern_config::JobConfig::Push(s) => {
                let job = jobs::push::PushJob::new(s, Some(peers.clone()), &config.peers)
                    .map_err(|e| eyre::eyre!("push job filter regex: {e}"))?;
                manager.spawn(Arc::new(job), ctx.clone());
            }
            arctern_config::JobConfig::Prune(s) => {
                manager.spawn(Arc::new(jobs::prune::PruneCycle::job(s)), ctx.clone());
            }
        }
    }
    Ok(manager)
}

/// A background task with its own cancel token, stopped in its own
/// shutdown stage.
struct Sweeper {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

impl Sweeper {
    fn spawn(start: impl FnOnce(CancellationToken) -> JoinHandle<()>) -> Self {
        let cancel = CancellationToken::new();
        let task = start(cancel.clone());
        Self { cancel, task }
    }

    async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn run_daemon(
    socket_arg: Option<PathBuf>,
    config_path: PathBuf,
    http_address: SocketAddr,
) -> eyre::Result<()> {
    // Load and validate the config before binding any socket so a bad
    // file fails loudly and leaves nothing behind.
    let mut config = arctern_config::load_from_path(&config_path)
        .map_err(|e| eyre::eyre!("config load: {e}"))?;
    let socket_path = resolve_socket_path(socket_arg, config.socket.as_deref());
    let listener = bind_unix_socket(&socket_path)?;
    let zfs = zfs_facade()?;

    let state_dir = state_dir(&config);
    std::fs::create_dir_all(&state_dir)
        .map_err(|e| eyre::eyre!("create state_dir {}: {e}", state_dir.display()))?;
    let pool = Arc::new(
        state::open_for_daemon(&state_dir)
            .await
            .map_err(|e| eyre::eyre!("state open: {e}"))?,
    );
    let admin_auth = auth::AdminAuth::load_or_create(&state_dir, pool.as_ref().clone())
        .map_err(|e| eyre::eyre!("load or create admin token in {}: {e}", state_dir.display()))?;
    let events_tx = init_tracing(Some(pool.clone()));

    let ctx = jobs::JobContext {
        zfs: zfs.clone(),
        state: Some(pool.clone()),
    };
    let peers = PeerLinks::spawn(&config.peers);
    let job_configs = std::mem::take(&mut config.jobs);
    let manager = spawn_jobs(&config, job_configs, &ctx, &peers.state)?;
    let arc_sweeper = Sweeper::spawn(|cancel| state::arcstats::spawn_sweeper(pool.clone(), cancel));
    let trim_sweeper = Sweeper::spawn(|cancel| state::spawn_trim_sweeper(pool.clone(), cancel));

    let shutdown_token = CancellationToken::new();
    let app_state = app_state::AppState {
        auth: admin_auth.clone(),
        manager: manager.clone(),
        peers: peers.state.clone(),
        events: events_tx,
        state: pool.clone(),
        zfs,
        config_path: config_path
            .canonicalize()
            .unwrap_or_else(|_| config_path.clone()),
        shutdown: shutdown_token.clone(),
    };
    let app = router::build_router(app_state.clone());
    let loopback_app = router::build_loopback_router(app_state);

    // Loopback TCP serves the embedded admin UI + the same API. Tests use
    // port 0 to avoid colliding with an installed daemon.
    let loopback_listener = tokio::net::TcpListener::bind(http_address).await?;
    let loopback_addr = loopback_listener.local_addr()?;

    println!("LISTEN unix:{}", socket_path.display());
    println!("LISTEN http://{loopback_addr}");
    println!("ADMIN_TOKEN_FILE {}", admin_auth.token_path().display());
    std::io::stdout().flush().ok();
    tracing::info!(path = %socket_path.display(), "arctern daemon listening");
    tracing::info!(addr = %loopback_addr, "arctern admin UI listening");

    spawn_signal_handler(shutdown_token.clone())?;

    let uds_serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<auth::PeerCredentials>(),
    )
    .with_graceful_shutdown(shutdown_token.clone().cancelled_owned());
    let tcp_serve = axum::serve(loopback_listener, loopback_app.into_make_service())
        .with_graceful_shutdown(shutdown_token.clone().cancelled_owned());

    let stage = Arc::new(AtomicU8::new(SHUTDOWN_STAGE_SERVERS_AND_JOBS));
    let orderly_shutdown = {
        let stage = stage.clone();
        let shutdown_token = shutdown_token.clone();
        let socket_path = socket_path.clone();
        async move {
            // Servers and jobs stop concurrently: draining first meant one
            // slow in-flight request spent the budget a job needed to
            // record its terminal state.
            let servers = async {
                let result =
                    tokio::try_join!(uds_serve.into_future(), tcp_serve.into_future()).map(|_| ());
                // A listener can also terminate on its own; that starts the
                // same bounded cleanup and releases the job half.
                shutdown_token.cancel();
                result
            };
            let jobs = async {
                shutdown_token.cancelled().await;
                manager.shutdown(Duration::from_secs(5)).await;
            };
            let (result, ()) = tokio::join!(servers, jobs);

            stage.store(SHUTDOWN_STAGE_PEERS, Ordering::SeqCst);
            peers.shutdown().await;
            stage.store(SHUTDOWN_STAGE_ARC, Ordering::SeqCst);
            arc_sweeper.shutdown().await;
            stage.store(SHUTDOWN_STAGE_RETENTION, Ordering::SeqCst);
            trim_sweeper.shutdown().await;
            stage.store(SHUTDOWN_STAGE_SOCKET, Ordering::SeqCst);
            let _ = std::fs::remove_file(&socket_path);
            stage.store(SHUTDOWN_STAGE_COMPLETE, Ordering::SeqCst);

            result?;
            Ok(())
        }
    };

    supervise_shutdown(
        shutdown_token,
        orderly_shutdown,
        stage,
        SHUTDOWN_TIMEOUT,
        socket_path,
    )
    .await
}

fn spawn_signal_handler(shutdown: CancellationToken) -> eyre::Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM"),
            _ = sigint.recv() => tracing::info!("SIGINT"),
        }
        shutdown.cancel();
    });
    Ok(())
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;

    fn temp_socket_path() -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "arctern-shutdown-test-{}-{nonce}.sock",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn shutdown_completes_without_waiting_for_cancellation() {
        let cancellation = CancellationToken::new();
        let stage = Arc::new(AtomicU8::new(SHUTDOWN_STAGE_SERVERS_AND_JOBS));
        let socket_path = temp_socket_path();

        supervise_shutdown(
            cancellation,
            async { Ok(()) },
            stage,
            Duration::from_millis(20),
            socket_path,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn shutdown_timeout_reports_stage_and_removes_socket() {
        let cancellation = CancellationToken::new();
        let cancellation_from_shutdown = cancellation.clone();
        let stage = Arc::new(AtomicU8::new(SHUTDOWN_STAGE_PEERS));
        let socket_path = temp_socket_path();
        std::fs::write(&socket_path, b"test socket placeholder").unwrap();

        let error = supervise_shutdown(
            cancellation,
            async move {
                cancellation_from_shutdown.cancel();
                std::future::pending().await
            },
            stage,
            Duration::from_millis(20),
            socket_path.clone(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("peer reconnect shutdown"));
        assert!(!socket_path.exists());
    }

    #[test]
    fn socket_path_prefers_the_flag_then_the_config() {
        let flag = PathBuf::from("/tmp/flag.sock");
        let cfg = PathBuf::from("/tmp/cfg.sock");
        assert_eq!(resolve_socket_path(Some(flag.clone()), Some(&cfg)), flag);
        assert_eq!(resolve_socket_path(None, Some(&cfg)), cfg);
        assert_eq!(resolve_socket_path(None, None), default_socket_path());
    }
}
