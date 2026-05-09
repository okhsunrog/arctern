//! arctern transport — QUIC + TLS plumbing shared between sink (slice 004)
//! and push (slice 005). Leaf crate: no `axum`, no `palimpsest`, no
//! `arctern-config`. Owns the TLS identity bootstrap, the accept-any
//! verifier (WireGuard is the security perimeter; constitution V
//! deferral), and the on-the-wire receive header / response framing.
