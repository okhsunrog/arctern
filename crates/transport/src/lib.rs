//! arctern transport — wire protocol types shared between the active
//! sender (push job) and the passive receiver (stdinserver). Pure types
//! and codec helpers; no I/O construction. The SSH session itself lives
//! in `daemon::peer`; this crate stays leaf — no `axum`, `palimpsest`,
//! or `arctern-config`.

pub mod protocol;

pub use protocol::{
    EventWire, ErrorCode, JobStatusWire, ListResponse, MAX_FRAME_LEN, MAX_HEADER_LEN, Op,
    PROTOCOL_VERSION, ProtocolError, ReceiveHeader, ReceiveResponse, RecvHeader, Request,
    RequestFrame, Response, ResponseFrame, SendFlagsWire, SendHeader, SendKind, SnapshotEntry,
    SnapshotRef, compile_prefix_regex, control_codec, read_frame, read_header, read_list_response,
    read_request, read_response, recv_header_codec, write_frame, write_header,
    write_list_response, write_request, write_response,
};
pub use regex;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}
