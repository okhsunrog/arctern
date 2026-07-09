//! arctern daemon binary.
//!
//! Three subcommands per the constitution and CLAUDE.md "Out-of-scope CLI":
//! - `daemon` runs the axum server (the only fully-implemented subcommand
//!   this slice).
//! - `stdinserver <ident>` is the SSH transport entry point invoked by sshd
//!   via authorized_keys `command="..."`. Stub through slice 003.
//! - `configcheck <path>` validates a config file for CI / pre-deploy.

// musl's allocator is noticeably slower under multithreaded load than
// glibc's; mimalloc levels the static-musl release builds with the
// glibc ones (and is a mild win there too).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::future::Future;
use std::io::{ErrorKind, Write};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};

mod app_state;
mod auth;
mod configcheck;
mod error;
mod handlers;
mod jobs;
mod peer;
mod router;
mod state;
mod stdinserver;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
const SHUTDOWN_STAGE_SERVERS: u8 = 0;
const SHUTDOWN_STAGE_JOBS: u8 = 1;
const SHUTDOWN_STAGE_PEERS: u8 = 2;
const SHUTDOWN_STAGE_ARC: u8 = 3;
const SHUTDOWN_STAGE_RETENTION: u8 = 4;
const SHUTDOWN_STAGE_SOCKET: u8 = 5;
const SHUTDOWN_STAGE_COMPLETE: u8 = 6;

fn shutdown_stage_name(stage: u8) -> &'static str {
    match stage {
        SHUTDOWN_STAGE_SERVERS => "HTTP and UNIX server drain",
        SHUTDOWN_STAGE_JOBS => "job shutdown",
        SHUTDOWN_STAGE_PEERS => "peer reconnect shutdown",
        SHUTDOWN_STAGE_ARC => "ARC sweeper shutdown",
        SHUTDOWN_STAGE_RETENTION => "retention sweeper shutdown",
        SHUTDOWN_STAGE_SOCKET => "UNIX socket cleanup",
        SHUTDOWN_STAGE_COMPLETE => "complete",
        _ => "unknown",
    }
}

async fn supervise_shutdown<F>(
    cancellation: tokio_util::sync::CancellationToken,
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

#[tokio::main(flavor = "current_thread")]
async fn run_stdinserver_dispatch(identity: String, config: PathBuf) -> eyre::Result<()> {
    // The dispatcher logs structured events; pipe them to stderr so
    // sshd's wrapping channel only sees the protocol bytes on stdout.
    // EnvFilter respects RUST_LOG so operators can crank verbosity.
    // The SQLite layer mirrors INFO+ events into the per-host state.db
    // so the receiver-side SubscribeEvents handler can stream them back.
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // Resolve config + state_dir up front so the subscriber has access
    // to the same SQLite the daemon writes to. A failure to open the
    // pool falls back to stderr-only tracing so the dispatch still runs.
    let cfg =
        arctern_config::load_from_path(&config).map_err(|e| eyre::eyre!("config load: {e}"))?;
    let state_dir = cfg
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("/var/lib/arctern"));
    let pool = match state::open(&state_dir).await {
        Ok(p) => Some(Arc::new(p)),
        Err(e) => {
            eprintln!(
                "stdinserver-dispatch: state open failed ({e}); continuing without SQLite event log"
            );
            None
        }
    };

    // tarpc traces every RPC at INFO (BeginRequest/SendResponse, four
    // lines per probe); over the stderr bridge that would flood the
    // sender's event log every 15s. WARN keeps real tarpc failures.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tarpc=warn"));
    // stderr here is an SSH pipe read by the peer's stderr drain (or
    // journald); ANSI colour codes would travel into the peer's event
    // log as garbage.
    use std::io::IsTerminal as _;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    if let Some(p) = pool.clone() {
        // No in-process subscribers here — the broadcast half exists so
        // the writer task has a uniform signature; the daemon is the
        // one who fans events out.
        let (events_tx, _) = tokio::sync::broadcast::channel(16);
        let (layer, _writer) = state::log_events::SqliteLogLayer::with_writer(p, events_tx);
        let sqlite_layer = layer.with_filter(state::log_events::SqliteLogLayer::filter());
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(sqlite_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    }

    stdinserver::dispatch::run_with(&identity, cfg, pool).await
}

/// Resolve the socket path the daemon should bind to. Priority:
/// `--socket` flag, then the config's `socket` key (which
/// `stdinserver-dispatch` also reads, so the two processes agree),
/// then the environment default.
fn resolve_socket_path(arg: Option<PathBuf>, config: Option<&std::path::Path>) -> PathBuf {
    if let Some(p) = arg {
        return p;
    }
    if let Some(p) = config {
        return p.to_path_buf();
    }
    default_socket_path()
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

#[tokio::main(flavor = "multi_thread")]
async fn run_daemon(
    socket_arg: Option<PathBuf>,
    config_path: PathBuf,
    http_address: SocketAddr,
) -> eyre::Result<()> {
    // Load and validate the config BEFORE binding any socket — fail
    // loudly if the operator's file is missing or malformed.
    let config = arctern_config::load_from_path(&config_path)
        .map_err(|e| eyre::eyre!("config load: {e}"))?;

    let socket_path = resolve_socket_path(socket_arg, config.socket.as_deref());

    match std::fs::remove_file(&socket_path) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            return Err(eyre::eyre!(
                "remove stale socket {}: {e}",
                socket_path.display()
            ));
        }
    }

    let listener = UnixListener::bind(&socket_path)?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;

    // Under the SSH-transport pivot the daemon runs on the actual ZFS
    // host, so its local CommandRunner is RealRunner. SshCommandRunner
    // is kept as a dev/test override: setting ZFSKIT_SSH_TARGET
    // selects it, matching the integration-test harness convention.
    let zfs = match std::env::var("ZFSKIT_SSH_TARGET") {
        Ok(s) if !s.is_empty() => zfskit::Zfs::with_runner(
            zfskit::SshCommandRunner::from_env()
                .map_err(|e| eyre::eyre!("ZFSKIT_SSH_TARGET configuration: {e}"))?,
        ),
        _ => zfskit::Zfs::new(),
    };

    // Resolve the state directory and ensure it exists; SQLite + the
    // tracing layer's table both live under this path.
    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("/var/lib/arctern"));
    std::fs::create_dir_all(&state_dir)
        .map_err(|e| eyre::eyre!("create state_dir {}: {e}", state_dir.display()))?;

    let admin_auth = auth::AdminAuth::load_or_create(&state_dir)
        .map_err(|e| eyre::eyre!("load or create admin token in {}: {e}", state_dir.display()))?;

    let pool = Arc::new(
        state::open(&state_dir)
            .await
            .map_err(|e| eyre::eyre!("state open: {e}"))?,
    );

    // Tracing fan-out: stderr fmt for live debugging, SQLite layer for
    // INFO+ persistence. The fmt layer keeps DEBUG/TRACE; the SQLite
    // layer carries a per-layer filter (INFO+, minus the sqlx target) so
    // it alone drops those events without affecting the fmt layer.
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    // tarpc=warn: its per-RPC INFO tracing (four lines per control-
    // channel call) is protocol noise, not operator signal.
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tarpc=warn"));
    // Under systemd stderr is a pipe to journald — no ANSI there either.
    use std::io::IsTerminal as _;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    // Event bus: tracing layer → writer task (SQLite assigns ids) →
    // this broadcast. SSE handlers subscribe directly; there is no
    // polling anywhere in the daemon-side pipeline.
    let (events_tx, _events_rx) = tokio::sync::broadcast::channel::<arctern_api::LogEvent>(256);
    let (sqlite_layer, _events_writer) =
        state::log_events::SqliteLogLayer::with_writer(pool.clone(), events_tx.clone());
    let sqlite_layer = sqlite_layer.with_filter(state::log_events::SqliteLogLayer::filter());
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(sqlite_layer)
        .init();

    let manager = Arc::new(jobs::JobManager::new());
    let ctx = jobs::JobContext {
        zfs: zfs.clone(),
        state: Some(pool.clone()),
    };
    // One eager-reconnect background task per [[peers]] entry. Each
    // task owns its peer's PeerLink lifecycle and updates the shared
    // peers map; push jobs and HTTP handlers read from there. A
    // CancellationToken drives graceful shutdown.
    let peers_state = peer::state::new_state();
    // Connectivity edge signal: reconnect tasks bump this on every
    // publish; push jobs re-evaluate due-ness immediately instead of
    // waiting out their nap.
    let (peers_changed_tx, peers_changed_rx) = tokio::sync::watch::channel(0u64);
    let peers_cancel = tokio_util::sync::CancellationToken::new();
    let mut reconnect_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for p in &config.peers {
        let state_for_task = peers_state.clone();
        let cancel = peers_cancel.clone();
        let name = p.name.clone();
        let routes = p.routes.clone();
        let changed = peers_changed_tx.clone();
        reconnect_handles.push(tokio::spawn(async move {
            peer::reconnect::run_for_peer(state_for_task, name, routes, changed, cancel).await;
        }));
    }
    for job in config.jobs {
        match job {
            arctern_config::JobConfig::Snap(s) => {
                let job = Arc::new(jobs::snap::SnapJob::new(s));
                manager.spawn(job, ctx.clone());
            }
            arctern_config::JobConfig::Push(s) => {
                let job = jobs::push::PushJob::new(
                    s,
                    Some(peers_state.clone()),
                    &config.peers,
                    Some(peers_changed_rx.clone()),
                )
                .map_err(|e| eyre::eyre!("push job filter regex: {e}"))?;
                manager.spawn(Arc::new(job), ctx.clone());
            }
            arctern_config::JobConfig::Prune(s) => {
                let job = Arc::new(jobs::prune::PruneJob::new(s));
                manager.spawn(job, ctx.clone());
            }
        }
    }

    // ARC stats sweeper: writes /proc/spl/kstat/zfs/arcstats into
    // arcstats_history every minute, prunes rows older than 24h. The
    // dashboard chart reads from there.
    let arc_cancel = tokio_util::sync::CancellationToken::new();
    let arc_sweeper = state::arcstats::spawn_sweeper(pool.clone(), arc_cancel.clone());

    // Retention sweep for the observability tables (job_runs, log_events).
    let trim_cancel = tokio_util::sync::CancellationToken::new();
    let trim_sweeper = state::spawn_trim_sweeper(pool.clone(), trim_cancel.clone());

    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let app_state = app_state::AppState {
        auth: admin_auth.clone(),
        manager: manager.clone(),
        peers: peers_state.clone(),
        events: events_tx,
        state: pool.clone(),
        zfs: zfs.clone(),
        config_path: config_path
            .canonicalize()
            .unwrap_or_else(|_| config_path.clone()),
        shutdown: shutdown_token.clone(),
    };
    let app = router::build_router(app_state.clone());
    let loopback_app = router::build_loopback_router(app_state);

    // Loopback TCP serves the embedded admin UI + the same API. The default
    // port matches the dev proxy; tests use port 0 to avoid colliding with an
    // installed daemon.
    let loopback_listener = tokio::net::TcpListener::bind(http_address).await?;
    let loopback_addr = loopback_listener.local_addr()?;

    println!("LISTEN unix:{}", socket_path.display());
    println!("LISTEN http://{loopback_addr}");
    println!("ADMIN_TOKEN_FILE {}", admin_auth.token_path().display());
    std::io::stdout().flush().ok();

    tracing::info!(path = %socket_path.display(), "arctern daemon listening");
    tracing::info!(addr = %loopback_addr, "arctern admin UI listening");

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let cleanup_path = socket_path.clone();
    let shutdown_token_uds = shutdown_token.clone();
    let shutdown_token_tcp = shutdown_token.clone();
    let shutdown_deadline_token = shutdown_token.clone();
    let shutdown_after_server_token = shutdown_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM"),
            _ = sigint.recv() => tracing::info!("SIGINT"),
        }
        shutdown_token.cancel();
    });

    let uds_serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<auth::PeerCredentials>(),
    )
    .with_graceful_shutdown(async move { shutdown_token_uds.cancelled().await });

    let tcp_serve = axum::serve(loopback_listener, loopback_app.into_make_service())
        .with_graceful_shutdown(async move { shutdown_token_tcp.cancelled().await });

    let shutdown_stage = Arc::new(AtomicU8::new(SHUTDOWN_STAGE_SERVERS));
    let orderly_stage = shutdown_stage.clone();
    let timeout_cleanup_path = cleanup_path.clone();
    let orderly_shutdown = async move {
        let result = tokio::try_join!(uds_serve.into_future(), tcp_serve.into_future()).map(|_| ());
        // A listener can also terminate independently of SIGTERM/SIGINT.
        // Start the same bounded cleanup deadline in that case.
        shutdown_after_server_token.cancel();

        orderly_stage.store(SHUTDOWN_STAGE_JOBS, Ordering::SeqCst);
        manager.shutdown(Duration::from_secs(5)).await;

        orderly_stage.store(SHUTDOWN_STAGE_PEERS, Ordering::SeqCst);
        peers_cancel.cancel();
        for handle in reconnect_handles {
            let _ = handle.await;
        }

        orderly_stage.store(SHUTDOWN_STAGE_ARC, Ordering::SeqCst);
        arc_cancel.cancel();
        let _ = arc_sweeper.await;

        orderly_stage.store(SHUTDOWN_STAGE_RETENTION, Ordering::SeqCst);
        trim_cancel.cancel();
        let _ = trim_sweeper.await;

        orderly_stage.store(SHUTDOWN_STAGE_SOCKET, Ordering::SeqCst);
        let _ = std::fs::remove_file(&cleanup_path);
        orderly_stage.store(SHUTDOWN_STAGE_COMPLETE, Ordering::SeqCst);

        result?;
        Ok(())
    };

    supervise_shutdown(
        shutdown_deadline_token,
        orderly_shutdown,
        shutdown_stage,
        SHUTDOWN_TIMEOUT,
        timeout_cleanup_path,
    )
    .await
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
        let cancellation = tokio_util::sync::CancellationToken::new();
        let stage = Arc::new(AtomicU8::new(SHUTDOWN_STAGE_SERVERS));
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
        let cancellation = tokio_util::sync::CancellationToken::new();
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
}
