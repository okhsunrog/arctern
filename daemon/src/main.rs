//! arctern daemon binary.
//!
//! Three subcommands per the constitution and CLAUDE.md "Out-of-scope CLI":
//! - `daemon` runs the axum server (the only fully-implemented subcommand
//!   this slice).
//! - `stdinserver <ident>` is the SSH transport entry point invoked by sshd
//!   via authorized_keys `command="..."`. Stub this slice.
//! - `configcheck <path>` validates a config file for CI / pre-deploy. Stub
//!   this slice.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod router;

#[derive(Parser, Debug)]
#[command(name = "arctern", version, about = "ZFS replication daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the daemon (HTTP API server).
    Daemon,
    /// SSH transport entry point invoked by sshd. Stub this slice.
    Stdinserver { ident: String },
    /// One-shot YAML validation for CI / pre-deploy. Stub this slice.
    Configcheck { path: PathBuf },
}

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Daemon => run_daemon(),
        Command::Stdinserver { ident } => {
            eprintln!("arctern stdinserver {ident}: not implemented in slice 001");
            Ok(())
        }
        Command::Configcheck { path } => {
            eprintln!("arctern configcheck {}: not implemented in slice 001", path.display());
            Ok(())
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn run_daemon() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let app = router::build_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Single-line, line-buffered handshake the integration test parses.
    println!("LISTEN {addr}");
    use std::io::Write;
    std::io::stdout().flush().ok();

    tracing::info!(%addr, "arctern daemon listening");
    axum::serve(listener, app).await?;
    Ok(())
}
