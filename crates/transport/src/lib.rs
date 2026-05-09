//! arctern transport — QUIC + TLS plumbing shared between sink (slice 004)
//! and push (slice 005). Leaf crate: no `axum`, no `palimpsest`, no
//! `arctern-config`. Owns the TLS identity bootstrap, the accept-any
//! verifier (WireGuard is the security perimeter; constitution V
//! deferral), and the on-the-wire receive header / response framing.

pub mod identity;
pub mod tls;

pub use identity::{TransportIdentity, load_or_generate_identity};
pub use tls::{ALPN, client_config_accept_any, server_config};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("pem {path}: {source}")]
    Pem {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cert chain in {path} is empty")]
    EmptyCertChain { path: String },
    #[error("no private key found in {path}")]
    NoPrivateKey { path: String },
    #[error("identity half-missing: have {present}, need {missing}")]
    IdentityHalfMissing { present: String, missing: String },
    #[error("rcgen: {0}")]
    Rcgen(#[source] rcgen::Error),
    #[error("rustls: {0}")]
    Rustls(String),
}
