//! Control-channel client: the tarpc `ArcternControlClient` over the
//! remote `arctern stdinserver-dispatch ... control` child's stdio.
//! tarpc owns the request-id demux; this module keeps only the error
//! mapping and the per-request deadline policy.

use arctern_transport::{ArcternControlClient, WireError};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("control channel closed")]
    ChannelClosed,
    #[error("control request timed out")]
    Timeout,
    #[error("server returned error: {0}")]
    Server(WireError),
    #[error("transport: {0}")]
    Transport(String),
}

impl From<tarpc::client::RpcError> for RpcError {
    fn from(e: tarpc::client::RpcError) -> Self {
        match e {
            tarpc::client::RpcError::Shutdown => RpcError::ChannelClosed,
            tarpc::client::RpcError::DeadlineExceeded => RpcError::Timeout,
            other => RpcError::Transport(other.to_string()),
        }
    }
}

impl From<WireError> for RpcError {
    fn from(e: WireError) -> Self {
        RpcError::Server(e)
    }
}

/// Per-request ceiling. The control channel carries only small RPC
/// frames (bulk transfer goes over recv channels), so any request that
/// outlives this has hit a dead or half-open session — surface it as an
/// error rather than letting the caller (or the reconnect probe) hang
/// forever on a connection the kernel hasn't yet torn down.
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// A fresh tarpc context with the control-channel deadline (tarpc's
/// default is 10s — too tight for a snapshot listing on a dataset with
/// tens of thousands of snapshots).
pub fn ctx() -> tarpc::context::Context {
    let mut c = tarpc::context::current();
    c.deadline = std::time::Instant::now() + REQUEST_TIMEOUT;
    c
}

/// Build the client over a pair of byte streams (the channel's
/// stdout/stdin in production; duplex pipes in tests) and spawn its
/// dispatch task.
pub fn spawn<R, W>(reader: R, writer: W) -> ArcternControlClient
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let transport = arctern_transport::transport(tokio::io::join(reader, writer));
    ArcternControlClient::new(tarpc::client::Config::default(), transport).spawn()
}
