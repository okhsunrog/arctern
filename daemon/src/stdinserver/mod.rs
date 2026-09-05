//! SSH transport entry point. `sshd` invokes
//! `arctern stdinserver-dispatch <identity>` via `authorized_keys`
//! `command="..."`. The dispatcher reads `SSH_ORIGINAL_COMMAND`,
//! validates the identity against the daemon's config, and forks into
//! the matching channel handler (control, recv or events).

pub mod acl;
pub mod control;
pub mod dispatch;
pub mod events;
pub mod recv;
mod recv_lock;
