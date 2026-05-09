//! arctern daemon binary.
//!
//! Three subcommands per the constitution and CLAUDE.md "Out-of-scope CLI":
//! - `daemon` runs the axum server (the only fully-implemented subcommand
//!   this slice).
//! - `stdinserver <ident>` is the SSH transport entry point invoked by sshd
//!   via authorized_keys `command="..."`. Stub through slice 003.
//! - `configcheck <path>` validates a config file for CI / pre-deploy.

use std::io::{ErrorKind, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};

mod auth;
mod configcheck;
mod error;
mod handlers;
mod jobs;
mod peer;
mod router;
mod state;
mod stdinserver;

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
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Daemon { socket, config } => run_daemon(socket, config),
        Command::StdinserverDispatch { identity, config } => {
            run_stdinserver_dispatch(identity, config)
        }
        Command::Configcheck { path } => configcheck::run(&path),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run_stdinserver_dispatch(identity: String, config: PathBuf) -> eyre::Result<()> {
    // The dispatcher logs structured events; pipe them to stderr so
    // sshd's wrapping channel only sees the protocol bytes on stdout.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    stdinserver::dispatch::run(&identity, &config).await
}

/// Resolve the socket path the daemon should bind to.
fn resolve_socket_path(arg: Option<PathBuf>) -> PathBuf {
    if let Some(p) = arg {
        return p;
    }
    if let Ok(rt) = std::env::var("XDG_RUNTIME_DIR")
        && !rt.is_empty()
    {
        return PathBuf::from(rt).join("arctern.sock");
    }
    PathBuf::from("/run/arctern.sock")
}

#[tokio::main(flavor = "multi_thread")]
async fn run_daemon(socket_arg: Option<PathBuf>, config_path: PathBuf) -> eyre::Result<()> {
    // Load and validate the config BEFORE binding any socket — fail
    // loudly if the operator's file is missing or malformed.
    let config = arctern_config::load_from_path(&config_path)
        .map_err(|e| eyre::eyre!("config load: {e}"))?;

    let socket_path = resolve_socket_path(socket_arg);

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
    // is kept as a dev/test override: setting PALIMPSEST_SSH_TARGET
    // selects it, matching the integration-test harness convention.
    let runner: Arc<dyn palimpsest::runner::CommandRunner> =
        match std::env::var("PALIMPSEST_SSH_TARGET") {
            Ok(s) if !s.is_empty() => Arc::new(
                palimpsest::SshCommandRunner::from_env().map_err(|e| {
                    eyre::eyre!("PALIMPSEST_SSH_TARGET configuration: {e}")
                })?,
            ),
            _ => Arc::new(palimpsest::runner::RealRunner),
        };

    // Resolve the state directory and ensure it exists; SQLite + the
    // tracing layer's table both live under this path.
    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("/var/lib/arctern"));
    std::fs::create_dir_all(&state_dir).map_err(|e| {
        eyre::eyre!("create state_dir {}: {e}", state_dir.display())
    })?;

    let pool = Arc::new(
        state::open(&state_dir)
            .await
            .map_err(|e| eyre::eyre!("state open: {e}"))?,
    );

    // Tracing fan-out: stderr fmt for live debugging, SQLite layer for
    // INFO+ persistence. The fmt layer keeps DEBUG/TRACE; the SQLite
    // layer filters those out at `enabled()` so they never reach the DB.
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let sqlite_layer = state::log_events::SqliteLogLayer::new(pool.clone());
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(sqlite_layer)
        .init();

    let manager = Arc::new(jobs::JobManager::new());
    let ctx = jobs::JobContext {
        runner: runner.clone(),
        state: Some(pool.clone()),
    };
    // Connect once per [[peers]] entry; push jobs reference these by name.
    // A failed connect is non-fatal — the job's run_cycle reports it on
    // every tick and the operator can fix the SSH config and restart.
    let mut peer_links: std::collections::BTreeMap<String, Arc<peer::PeerLink>> =
        std::collections::BTreeMap::new();
    for p in &config.peers {
        // The job-name namespace passed to stdinserver-dispatch is
        // synthesised from the receiver-side allowed_clients ACL; the
        // push job carries it on PushJobConfig.name. Connect uses a
        // placeholder "control" job because the openssh::Session is
        // shared across recv channels — the per-job arg only matters
        // when opening individual children. To keep the connect step
        // simple and avoid waiting for the first push job, we connect
        // with a sentinel job name here; open_recv passes the real
        // job name on each recv channel.
        match peer::PeerLink::connect(p.name.clone(), &p.ssh_target, "control").await {
            Ok(link) => {
                peer_links.insert(p.name.clone(), Arc::new(link));
            }
            Err(e) => {
                tracing::warn!(peer = %p.name, error = %e, "peer connect failed; push jobs targeting this peer will retry on each cycle");
            }
        }
    }
    for job in config.jobs {
        match job {
            arctern_config::JobConfig::Snap(s) => {
                let job = Arc::new(jobs::snap::SnapJob::new(s));
                manager.spawn(job, ctx.clone());
            }
            arctern_config::JobConfig::Sink(s) => {
                // Sink jobs are the legacy QUIC receiver shape. The SSH
                // pivot moves their behaviour into stdinserver/recv on
                // the receiving host. Configs that still mention sink
                // are accepted (so existing files keep parsing) but a
                // warning is logged and the job is not spawned.
                tracing::warn!(
                    name = %s.name,
                    "sink jobs are obsolete under the SSH transport; ignoring"
                );
            }
            arctern_config::JobConfig::Push(s) => {
                let peer_link = s
                    .peer
                    .as_deref()
                    .and_then(|name| peer_links.get(name).cloned());
                if peer_link.is_none() {
                    tracing::warn!(
                        name = %s.name,
                        peer = ?s.peer,
                        "push job has no resolvable peer; cycles will report errors"
                    );
                }
                let job = jobs::push::PushJob::new(s, peer_link)
                    .map_err(|e| eyre::eyre!("push job filter regex: {e}"))?;
                manager.spawn(Arc::new(job), ctx.clone());
            }
        }
    }

    let app = router::build_router(manager.clone());

    println!("LISTEN unix:{}", socket_path.display());
    std::io::stdout().flush().ok();

    tracing::info!(path = %socket_path.display(), "arctern daemon listening");

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let cleanup_path = socket_path.clone();
    let shutdown = async move {
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM"),
            _ = sigint.recv() => tracing::info!("SIGINT"),
        }
    };

    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<auth::PeerCredentials>(),
    )
    .with_graceful_shutdown(shutdown);

    let result = serve.await;
    manager.shutdown(Duration::from_secs(5)).await;
    let _ = std::fs::remove_file(&cleanup_path);
    result?;
    Ok(())
}
