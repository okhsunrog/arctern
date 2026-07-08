//! arctern transport — wire protocol types shared between the active
//! sender (push job) and the passive receiver (stdinserver). Pure types
//! and codec helpers; no I/O construction. The SSH session itself lives
//! in `daemon::peer`; this crate stays leaf — no `axum`, `palimpsest`,
//! or `arctern-config`.

pub mod protocol;

pub use protocol::{
    ErrorCode, EventWire, MAX_FRAME_LEN, PROTOCOL_VERSION, ProtocolError, RecvHeader, Request,
    RequestFrame, Response, ResponseFrame, SendFlagsWire, SendHeader, SendKind, SnapshotEntry,
    SnapshotRef, compile_prefix_regex, read_header, read_request, read_response, write_header,
    write_request, write_response,
};
pub use regex;
