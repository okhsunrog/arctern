//! arctern daemon binary.
//!
//! Three subcommands per the constitution and CLAUDE.md "Out-of-scope CLI":
//! - `daemon` runs the axum server (the only fully-implemented subcommand
//!   this slice).
//! - `stdinserver <ident>` is the SSH transport entry point invoked by sshd
//!   via authorized_keys `command="..."`. Stub this slice.
//! - `configcheck <path>` validates a config file for CI / pre-deploy. Stub
//!   this slice.

use std::io::{ErrorKind, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};

mod auth;
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
    },
    /// SSH transport entry point invoked by sshd. Stub this slice.
    Stdinserver { ident: String },
    /// One-shot YAML validation for CI / pre-deploy. Stub this slice.
    Configcheck { path: PathBuf },
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Daemon { socket } => run_daemon(socket),
        Command::Stdinserver { ident } => {
            eprintln!("arctern stdinserver {ident}: not implemented in slice 002");
            Ok(())
        }
        Command::Configcheck { path } => {
            eprintln!("arctern configcheck {}: not implemented in slice 002", path.display());
            Ok(())
        }
    }
}

/// Resolve the socket path the daemon should bind to.
///
/// Returns the explicit `--socket` argument if present; otherwise
/// `$XDG_RUNTIME_DIR/arctern.sock` if `$XDG_RUNTIME_DIR` is set;
/// otherwise `/run/arctern.sock`.
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
async fn run_daemon(socket_arg: Option<PathBuf>) -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let socket_path = resolve_socket_path(socket_arg);

    // Best-effort cleanup of a stale socket from a prior crash. ENOENT is
    // expected on the happy path; anything else is fatal because we cannot
    // know the path is safe to reuse.
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
    // Owner-only — same-uid policy is also enforced at the protocol layer
    // by `auth::PeerAuth`, but defence in depth never hurts.
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;

    let app = router::build_router();

    // Single-line, line-buffered handshake the integration test parses.
    // Path may legally contain spaces or colons; everything after `unix:`
    // is taken literally.
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
    let _ = std::fs::remove_file(&cleanup_path);
    result?;
    Ok(())
}
