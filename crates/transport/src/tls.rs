//! QUIC TLS configuration.
//!
//! Server side: `quinn::ServerConfig` built from the daemon's persisted
//! self-signed `TransportIdentity`. Client side: `quinn::ClientConfig`
//! with an accept-any verifier — every peer cert is trusted.
//!
//! Why accept-any: WireGuard is the security perimeter for the QUIC
//! link in slices 004-005. Pinned-cert authentication requires a
//! credential exchange mechanism that does not exist yet (it would
//! pair with the planner / configcheck flow that knows which peers
//! are expected to dial). The verifier here is the documented escape
//! hatch; revisit when peer credentials land.

use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::identity::TransportIdentity;
use crate::TransportError;

pub fn server_config(identity: &TransportIdentity) -> Result<quinn::ServerConfig, TransportError> {
    let cert_chain = identity.cert_chain.clone();
    let key = identity.clone_key();
    let mut tls = rustls::ServerConfig::builder_with_provider(default_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TransportError::Rustls(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| TransportError::Rustls(e.to_string()))?;
    // ALPN — match what the client_config below advertises.
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let qsc = QuicServerConfig::try_from(tls).map_err(|e| TransportError::Rustls(e.to_string()))?;
    let mut sc = quinn::ServerConfig::with_crypto(Arc::new(qsc));
    sc.transport_config(transport_config());
    Ok(sc)
}

pub fn client_config_accept_any() -> Result<quinn::ClientConfig, TransportError> {
    let mut tls = rustls::ClientConfig::builder_with_provider(default_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| TransportError::Rustls(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyVerifier))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let qcc = QuicClientConfig::try_from(tls).map_err(|e| TransportError::Rustls(e.to_string()))?;
    let mut cc = quinn::ClientConfig::new(Arc::new(qcc));
    cc.transport_config(transport_config());
    Ok(cc)
}

/// Shared QUIC transport config for both ends. quinn's defaults give
/// ~30 s max_idle_timeout, which closes the connection during a
/// long-lived `zfs send`/recv pump if no application-level data
/// happens to flow within the window (the SSH-mediated test runner
/// makes this trivially reproducible; production WG links can hit
/// the same edge during a multi-GB initial bootstrap). Bump to
/// 10 minutes and enable a 30 s keep-alive so the connection stays
/// up across slow batches without spamming the network.
fn transport_config() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    t.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(std::time::Duration::from_secs(600))
            .expect("600 s fits in QUIC's idle-timeout encoding"),
    ));
    t.keep_alive_interval(Some(std::time::Duration::from_secs(30)));
    Arc::new(t)
}

/// Application-Layer Protocol Negotiation byte string. The exact value
/// is opaque; both ends must agree. Bumping this is a wire-incompat
/// change (intentional — useful as a kill-switch for slice-X-over-slice-Y
/// peering).
pub const ALPN: &[u8] = b"arctern/1";

fn default_provider() -> Arc<rustls::crypto::CryptoProvider> {
    // aws-lc-rs is what `cargo add` resolves for rustls 0.23 by default.
    // Ring would also work; pick one explicitly so a future feature-flag
    // change does not silently swap providers.
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// Trust-everything verifier. Every method asserts validity. See module
/// docs for the constitution V deferral rationale.
#[derive(Debug)]
struct AcceptAnyVerifier;

impl ServerCertVerifier for AcceptAnyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // Cover everything modern rustls supports. The list is matched
        // against the server's offered schemes; supplying the union is
        // safe because the verifier short-circuits everything anyway.
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::load_or_generate_identity;
    use std::path::PathBuf;

    fn tempdir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("arctern_tls_test_{nanos}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn server_config_builds() {
        let dir = tempdir();
        let id = load_or_generate_identity(&dir).expect("identity");
        let _sc = server_config(&id).expect("server config");
    }

    #[test]
    fn client_config_builds() {
        let _cc = client_config_accept_any().expect("client config");
    }
}
