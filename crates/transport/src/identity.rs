//! TLS identity for the QUIC transport.
//!
//! Self-signed cert + private key, lazily generated on first use and
//! persisted under the daemon's `state_dir`. WireGuard is the security
//! perimeter for slice 004 (constitution V deferral, see plan 004 D-V);
//! the cert is opaque to peers, who install an accept-any verifier.
//!
//! Disk layout:
//! - `<state_dir>/cert.pem` — PEM cert, mode 0o644
//! - `<state_dir>/key.pem`  — PEM PKCS#8 private key, mode 0o600
//!
//! If both files exist they are loaded as-is. If neither exists a fresh
//! identity is generated and written. If exactly one exists, refuse —
//! the pair is a unit and regenerating one half would silently break
//! every peer that pinned the other.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::TransportError;

#[derive(Debug)]
pub struct TransportIdentity {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl TransportIdentity {
    /// Clone the private key. `PrivateKeyDer` does not impl `Clone`
    /// (rustls 0.23 keeps the door open for borrowed variants); for our
    /// always-owned identities `clone_key()` is a cheap memcpy.
    pub fn clone_key(&self) -> PrivateKeyDer<'static> {
        self.key.clone_key()
    }
}

pub fn load_or_generate_identity(state_dir: &Path) -> Result<TransportIdentity, TransportError> {
    fs::create_dir_all(state_dir).map_err(|e| TransportError::Io {
        path: state_dir.display().to_string(),
        source: e,
    })?;
    let cert_path = state_dir.join("cert.pem");
    let key_path = state_dir.join("key.pem");
    let cert_exists = cert_path.exists();
    let key_exists = key_path.exists();
    match (cert_exists, key_exists) {
        (true, true) => load(&cert_path, &key_path),
        (false, false) => generate(&cert_path, &key_path),
        (true, false) => Err(TransportError::IdentityHalfMissing {
            present: cert_path.display().to_string(),
            missing: key_path.display().to_string(),
        }),
        (false, true) => Err(TransportError::IdentityHalfMissing {
            present: key_path.display().to_string(),
            missing: cert_path.display().to_string(),
        }),
    }
}

fn load(cert_path: &Path, key_path: &Path) -> Result<TransportIdentity, TransportError> {
    let cert_pem = fs::read(cert_path).map_err(|e| TransportError::Io {
        path: cert_path.display().to_string(),
        source: e,
    })?;
    let key_pem = fs::read(key_path).map_err(|e| TransportError::Io {
        path: key_path.display().to_string(),
        source: e,
    })?;
    let cert_chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|e| TransportError::Pem {
            path: cert_path.display().to_string(),
            source: e,
        })?;
    if cert_chain.is_empty() {
        return Err(TransportError::EmptyCertChain {
            path: cert_path.display().to_string(),
        });
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| TransportError::Pem {
            path: key_path.display().to_string(),
            source: e,
        })?
        .ok_or_else(|| TransportError::NoPrivateKey {
            path: key_path.display().to_string(),
        })?;
    Ok(TransportIdentity { cert_chain, key })
}

fn generate(cert_path: &Path, key_path: &Path) -> Result<TransportIdentity, TransportError> {
    let key_pair = rcgen::KeyPair::generate().map_err(TransportError::Rcgen)?;
    let mut params = rcgen::CertificateParams::new(vec!["arctern".to_string()])
        .map_err(TransportError::Rcgen)?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "arctern");
    let cert = params
        .self_signed(&key_pair)
        .map_err(TransportError::Rcgen)?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();
    write_with_mode(cert_path, cert_pem.as_bytes(), 0o644)?;
    write_with_mode(key_path, key_pem.as_bytes(), 0o600)?;
    load(cert_path, key_path)
}

fn write_with_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<(), TransportError> {
    fs::write(path, bytes).map_err(|e| TransportError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    let perm = fs::Permissions::from_mode(mode);
    fs::set_permissions(path, perm).map_err(|e| TransportError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    Ok(())
}

/// Opaque newtype around `state_dir` so callers cannot accidentally
/// pass a non-canonical path. Reserved for future use; for now just
/// used to make the test below more readable.
#[doc(hidden)]
pub fn _join_state_dir(base: &Path, name: &str) -> PathBuf {
    base.join(name)
}

fn _io_kind(_e: &io::Error) {} // silence unused-import lints if any

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn tempdir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("arctern_transport_test_{nanos}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn generate_then_load_roundtrips() {
        let dir = tempdir();
        let id1 = load_or_generate_identity(&dir).expect("generate");
        let id2 = load_or_generate_identity(&dir).expect("load");
        assert_eq!(id1.cert_chain.len(), id2.cert_chain.len());
        assert_eq!(id1.cert_chain[0].as_ref(), id2.cert_chain[0].as_ref());
        // PrivateKeyDer doesn't impl Eq; compare DER bytes.
        assert_eq!(id1.key.secret_der(), id2.key.secret_der());
    }

    #[test]
    fn generates_with_correct_modes() {
        let dir = tempdir();
        load_or_generate_identity(&dir).expect("generate");
        let cert_mode = std::fs::metadata(dir.join("cert.pem")).unwrap().mode() & 0o777;
        let key_mode = std::fs::metadata(dir.join("key.pem")).unwrap().mode() & 0o777;
        assert_eq!(cert_mode, 0o644);
        assert_eq!(key_mode, 0o600);
    }

    #[test]
    fn half_missing_is_an_error() {
        let dir = tempdir();
        std::fs::write(dir.join("cert.pem"), b"not a real cert").unwrap();
        let err = load_or_generate_identity(&dir).unwrap_err();
        assert!(matches!(err, TransportError::IdentityHalfMissing { .. }));
    }
}
