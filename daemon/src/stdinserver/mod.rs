//! SSH transport entry point. `sshd` invokes
//! `arctern stdinserver-dispatch <identity>` via `authorized_keys`
//! `command="..."`. The dispatcher reads `SSH_ORIGINAL_COMMAND`,
//! validates the identity against the daemon's config, and forks into
//! the matching channel handler (control or recv).
//!
//! Implemented incrementally:
//!   - `dispatch.rs`  — argv + env parsing, ACL lookup (this commit)
//!   - `control.rs`   — Request/Response handlers (step 7)
//!   - `recv.rs`      — recv channel (step 8)

pub mod dispatch;
