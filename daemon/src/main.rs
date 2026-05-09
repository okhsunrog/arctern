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
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};

mod auth;
mod configcheck;
mod error;
mod handlers;
mod jobs;
mod router;

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
    /// SSH transport entry point invoked by sshd. Stub through slice 003.
    Stdinserver { ident: String },
    /// One-shot validation for CI / pre-deploy.
    Configcheck { path: PathBuf },
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Daemon { socket, config } => run_daemon(socket, config),
        Command::Stdinserver { ident } => {
            eprintln!("arctern stdinserver {ident}: not implemented in slice 003");
            Ok(())
        }
        Command::Configcheck { path } => configcheck::run(&path),
    }
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
    // Tracing on stderr — stdout is reserved for the LISTEN handshake
    // line (a single line then unused; integration tests close their
    // read end of the pipe immediately after parsing it). Writing
    // tracing to stdout produces "broken pipe" warnings under tests.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

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

    // Construct a single shared CommandRunner for jobs (D14 in plan).
    // Handlers continue to construct their own per-request runners
    // (slice-002 behaviour preserved).
    let runner: Arc<dyn palimpsest::runner::CommandRunner> =
        Arc::new(palimpsest::SshCommandRunner::from_env().map_err(|e| {
            eyre::eyre!("PALIMPSEST_SSH_TARGET configuration: {e}")
        })?);

    // Resolve the state directory and ensure it exists before any
    // sink-job cert bootstrap touches it.
    let state_dir = config
        .state_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("/var/lib/arctern"));
    std::fs::create_dir_all(&state_dir).map_err(|e| {
        eyre::eyre!(
            "create state_dir {}: {e}",
            state_dir.display()
        )
    })?;

    // Lazily load (or generate) the QUIC TLS identity if any sink job
    // exists. Future slices' push-side will reuse the same identity
    // via the same Arc.
    let needs_identity = config
        .jobs
        .iter()
        .any(|j| matches!(j, arctern_config::JobConfig::Sink(_)));
    let identity = if needs_identity {
        Some(Arc::new(
            arctern_transport::load_or_generate_identity(&state_dir)
                .map_err(|e| eyre::eyre!("load/generate transport identity: {e}"))?,
        ))
    } else {
        None
    };

    let manager = Arc::new(jobs::JobManager::new());
    let ctx = jobs::JobContext {
        runner: runner.clone(),
    };
    let mut sinks: Vec<Arc<jobs::sink::SinkJob>> = Vec::new();
    for job in config.jobs {
        match job {
            arctern_config::JobConfig::Snap(s) => {
                let job = Arc::new(jobs::snap::SnapJob::new(s));
                manager.spawn(job, ctx.clone());
            }
            arctern_config::JobConfig::Sink(s) => {
                let id = identity
                    .clone()
                    .expect("identity loaded when sink jobs exist");
                let job = Arc::new(jobs::sink::SinkJob::new(s, id));
                manager.spawn(job.clone(), ctx.clone());
                sinks.push(job);
            }
        }
    }

    let app = router::build_router(manager.clone());

    println!("LISTEN unix:{}", socket_path.display());
    std::io::stdout().flush().ok();

    // Wait briefly for each sink to bind so the LISTEN_QUIC handshake
    // line names the OS-assigned port (jobs may use :0). 5s budget per
    // sink is generous; the bind itself is microseconds in practice.
    for sink in &sinks {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(addr) = sink.bound_addr() {
                println!("LISTEN_QUIC {addr}");
                std::io::stdout().flush().ok();
                break;
            }
            if Instant::now() > deadline {
                use crate::jobs::Job;
                tracing::warn!(name = sink.name(), "sink did not report bound_addr in 5s");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

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
