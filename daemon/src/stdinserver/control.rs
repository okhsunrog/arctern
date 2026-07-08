//! Server-side control-channel handler. Reads RequestFrames off stdin,
//! dispatches each to the matching `handle_*` function on its own task
//! (so one slow `ListSnapshots` over a huge dataset cannot head-of-line
//! block the UI proxy's other queries — responses correlate by
//! `request_id`, not arrival order), writes ResponseFrames to stdout
//! through a shared mutex-serialised writer.
//!
//! Per-Request handlers translate `palimpsest::ZfsError` and friends
//! into `Response::Error { code, message }` rather than letting them
//! escape; the caller never sees a process exit short of EOF.

use std::sync::Arc;

use arctern_config::zfs_names::{parse_snapshot_target, validate_dataset_name};
use arctern_config::{AllowedClient, Config};
use arctern_transport::{
    ErrorCode, EventWire, Request, RequestFrame, Response, ResponseFrame, SnapshotEntry,
    compile_prefix_regex, read_request, write_response,
};
use palimpsest::ZfsError;
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use sqlx::SqlitePool;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

/// Run the control channel until stdin EOF or a fatal write error.
/// `acl` scopes destroy / discard operations; `runner` is the
/// palimpsest CommandRunner the dispatch process opened (typically a
/// `RealRunner` invoking local `zfs(8)`).
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    config: Arc<Config>,
    acl: AllowedClient,
    pool: Option<Arc<SqlitePool>>,
    mut reader: R,
    writer: W,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    // Stdout is shared between the concurrent request tasks and any
    // background SubscribeEvents pusher; serialise writes through one
    // mutex so frames never interleave on the wire.
    let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
    // At most one pusher per control channel: the client sends
    // SubscribeEvents once per link, and a duplicate (e.g. a client
    // predating that dedup) must not double every Event frame.
    let mut event_pusher: Option<tokio::task::JoinHandle<()>> = None;
    let mut inflight = tokio::task::JoinSet::new();
    let result = loop {
        let frame: RequestFrame = match read_request(&mut reader).await {
            Ok(f) => f,
            Err(arctern_transport::ProtocolError::UnexpectedEof) => break Ok(()),
            Err(arctern_transport::ProtocolError::Io(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break Ok(());
            }
            Err(e) => {
                tracing::warn!(error = %e, "control: bad request frame; closing channel");
                break Ok(());
            }
        };
        let RequestFrame { id, body } = frame;
        if matches!(body, Request::Shutdown) {
            // Let in-flight requests finish (and write their responses)
            // before acknowledging shutdown, so the ack is the last frame.
            while inflight.join_next().await.is_some() {}
            let resp = ResponseFrame {
                request_id: Some(id),
                body: Response::Ok,
            };
            let mut w = writer.lock().await;
            let _ = write_response(&mut *w, &resp).await;
            let _ = w.flush().await;
            break Ok(());
        }
        // SubscribeEvents is special: reply Ok immediately, then spawn
        // a background task that polls log_events and writes Event
        // frames into the shared writer.
        if let Request::SubscribeEvents { since } = &body {
            let since = since.unwrap_or(0);
            let already_running = event_pusher.as_ref().is_some_and(|h| !h.is_finished());
            match pool.clone() {
                Some(p) => {
                    if let Err(r) = enforce_control_acl(&acl, "control:subscribe_events", true) {
                        let resp = ResponseFrame {
                            request_id: Some(id),
                            body: r,
                        };
                        let mut w = writer.lock().await;
                        let _ = write_response(&mut *w, &resp).await;
                        let _ = w.flush().await;
                        continue;
                    }
                    let resp = ResponseFrame {
                        request_id: Some(id),
                        body: Response::Ok,
                    };
                    {
                        let mut w = writer.lock().await;
                        let _ = write_response(&mut *w, &resp).await;
                        let _ = w.flush().await;
                    }
                    if !already_running {
                        let writer_for_task = writer.clone();
                        event_pusher = Some(tokio::spawn(async move {
                            push_events(p, since, writer_for_task).await;
                        }));
                    }
                    continue;
                }
                None => {
                    let resp = ResponseFrame {
                        request_id: Some(id),
                        body: Response::Error {
                            code: ErrorCode::Internal,
                            message: "stdinserver has no SQLite log layer".into(),
                        },
                    };
                    let mut w = writer.lock().await;
                    let _ = write_response(&mut *w, &resp).await;
                    let _ = w.flush().await;
                    continue;
                }
            }
        }
        let runner = runner.clone();
        let config = config.clone();
        let acl = acl.clone();
        let pool = pool.clone();
        let writer = writer.clone();
        inflight.spawn(async move {
            let resp_body = dispatch(runner.as_ref(), &config, &acl, pool.as_deref(), body).await;
            let resp = ResponseFrame {
                request_id: Some(id),
                body: resp_body,
            };
            let mut w = writer.lock().await;
            if let Err(e) = write_response(&mut *w, &resp).await {
                tracing::warn!(error = %e, "control: write_response failed");
                return;
            }
            if let Err(e) = w.flush().await {
                tracing::warn!(error = %e, "control: flush failed");
            }
        });
        // Reap finished request tasks as we go — a days-long control
        // channel would otherwise accumulate one completed-task slot
        // per request in the JoinSet until EOF.
        while inflight.try_join_next().is_some() {}
    };
    // EOF path: let in-flight tasks finish writing before tearing down.
    while inflight.join_next().await.is_some() {}
    if let Some(h) = event_pusher {
        h.abort();
    }
    result
}

/// Background task: poll log_events for new rows since `since` and push
/// them as Event frames (request_id = None) until the writer breaks.
async fn push_events<W>(pool: Arc<SqlitePool>, mut since: u64, writer: Arc<Mutex<BufWriter<W>>>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    use std::time::Duration;
    let poll_interval = Duration::from_millis(500);
    loop {
        let rows = match crate::state::log_events::since(&pool, since as i64, 256).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "control: log_events poll failed");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };
        for row in &rows {
            let ev = EventWire {
                id: row.id as u64,
                timestamp: row.timestamp,
                level: row.level.clone(),
                job_name: row.job_name.clone(),
                message: row.message.clone(),
            };
            let frame = ResponseFrame {
                request_id: None,
                body: Response::Event(ev),
            };
            let mut w = writer.lock().await;
            if write_response(&mut *w, &frame).await.is_err() {
                return;
            }
            if w.flush().await.is_err() {
                return;
            }
            since = row.id as u64;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn dispatch(
    runner: &dyn CommandRunner,
    config: &Config,
    acl: &AllowedClient,
    pool: Option<&SqlitePool>,
    req: Request,
) -> Response {
    match req {
        Request::ListSnapshots {
            dataset,
            prefix_regex,
        } => {
            if let Err(r) = enforce_control_acl(acl, "control:list_snapshots", true) {
                return r;
            }
            handle_list_snapshots(runner, acl, &dataset, prefix_regex.as_deref()).await
        }
        Request::ListReceiverGuids {
            dataset,
            prefix_regex,
        } => {
            if let Err(r) = enforce_control_acl(acl, "control:list_snapshots", true) {
                return r;
            }
            handle_list_receiver_guids(runner, acl, &dataset, prefix_regex.as_deref()).await
        }
        Request::GetReceiveResumeToken { dataset } => {
            if let Err(r) = enforce_control_acl(acl, "control:get_resume_token", true) {
                return r;
            }
            handle_get_receive_resume_token(runner, acl, &dataset).await
        }
        Request::ListDatasets => {
            if let Err(r) = enforce_control_acl(acl, "control:list_datasets", true) {
                return r;
            }
            handle_list_datasets(runner, acl).await
        }
        Request::ListHolds { snapshot } => {
            if let Err(r) = enforce_control_acl(acl, "control:list_holds", true) {
                return r;
            }
            handle_list_holds(runner, acl, &snapshot).await
        }
        Request::DestroySnapshot { name } => {
            if let Err(r) = enforce_control_acl(acl, "control:destroy_snapshot", false) {
                return r;
            }
            handle_destroy_snapshot(runner, acl, &name).await
        }
        Request::DiscardPartialRecv { dataset } => {
            if let Err(r) = enforce_control_acl(acl, "control:discard_partial_recv", false) {
                return r;
            }
            handle_discard_partial_recv(runner, acl, &dataset).await
        }
        Request::ListJobs => {
            if let Err(r) = enforce_control_acl(acl, "control:list_jobs", true) {
                return r;
            }
            handle_list_jobs(config).await
        }
        Request::GetJobStatus { name } => {
            if let Err(r) = enforce_control_acl(acl, "control:get_job_status", true) {
                return r;
            }
            match handle_list_jobs(config).await {
                Response::ListJobsOk { jobs } => match jobs.into_iter().find(|j| j.name == name) {
                    Some(job) => Response::GetJobStatusOk { job },
                    None => Response::Error {
                        code: ErrorCode::NotFound,
                        message: format!("no job named {name:?}"),
                    },
                },
                other => other,
            }
        }
        Request::WakeupJob { name } => {
            if let Err(r) = enforce_control_acl(acl, "control:wakeup_job", false) {
                return r;
            }
            match arctern_client::wakeup_job(&daemon_socket(config), &name).await {
                Ok(()) => Response::WakeupJobOk,
                Err(arctern_client::ClientError::Status { code: 404, .. }) => Response::Error {
                    code: ErrorCode::NotFound,
                    message: format!("no job named {name:?}"),
                },
                Err(e) => Response::Error {
                    code: ErrorCode::Internal,
                    message: format!("local daemon: {e}"),
                },
            }
        }
        Request::SubscribeEvents { .. } => unreachable!("handled in run()"),
        Request::GetLogCursor => {
            if let Err(r) = enforce_control_acl(acl, "control:get_log_cursor", true) {
                return r;
            }
            match pool {
                Some(p) => match crate::state::log_events::cursor(p).await {
                    Ok(id) => Response::GetLogCursorOk { id: id as u64 },
                    Err(e) => Response::Error {
                        code: ErrorCode::Internal,
                        message: format!("log_events cursor: {e}"),
                    },
                },
                None => Response::GetLogCursorOk { id: 0 },
            }
        }
        Request::Shutdown => unreachable!("handled in run()"),
    }
}

// Err carries a ready-to-send Response (>128 bytes); fine for a
// per-request cold path, not worth boxing at every call site.
#[allow(clippy::result_large_err)]
fn enforce_control_acl(
    acl: &AllowedClient,
    op: &'static str,
    allow_legacy_control: bool,
) -> Result<(), Response> {
    if acl.operations.iter().any(|configured| configured == op)
        || (allow_legacy_control
            && acl
                .operations
                .iter()
                .any(|configured| configured == "control"))
    {
        return Ok(());
    }
    Err(Response::Error {
        code: ErrorCode::Unauthorized,
        message: format!(
            "identity {:?} is not allowed for control operation {op:?}",
            acl.identity
        ),
    })
}

/// Reject `dataset` if the ACL has a `root_fs` set and `dataset` is not
/// equal to or a descendant of it. No root_fs configured means no
/// restriction.
#[allow(clippy::result_large_err)] // Err is a ready-to-send Response; cold path.
fn enforce_root_fs<'a>(acl: &'a AllowedClient, dataset: &'a str) -> Result<(), Response> {
    let Some(root) = acl.root_fs.as_deref() else {
        return Ok(());
    };
    if dataset == root {
        return Ok(());
    }
    let prefix = format!("{root}/");
    if dataset.starts_with(&prefix) {
        return Ok(());
    }
    Err(Response::Error {
        code: ErrorCode::Unauthorized,
        message: format!("{dataset:?} is not under allowed root_fs {root:?}"),
    })
}

async fn handle_list_snapshots(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
    prefix_regex: Option<&str>,
) -> Response {
    match collect_receiver_snapshots(runner, acl, dataset, prefix_regex).await {
        Ok((snapshots, receive_resume_token)) => Response::ListSnapshotsOk {
            snapshots,
            receive_resume_token,
        },
        Err(r) => r,
    }
}

/// Lean variant for the planner: returns only the receiver GUIDs (plus
/// the resume token), so the response stays small for datasets with many
/// thousands of snapshots. The planner intersects on GUID alone.
async fn handle_list_receiver_guids(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
    prefix_regex: Option<&str>,
) -> Response {
    match collect_receiver_snapshots(runner, acl, dataset, prefix_regex).await {
        Ok((snapshots, receive_resume_token)) => Response::ListReceiverGuidsOk {
            guids: snapshots.into_iter().map(|s| s.guid).collect(),
            receive_resume_token,
        },
        Err(r) => r,
    }
}

/// Shared core for the snapshot-inventory requests. Validates the
/// dataset, enforces `root_fs`, lists matching snapshots and reads the
/// receive resume token. A missing dataset (first replication) is the
/// non-error empty case. Returns the entries + token on success, or a
/// ready-to-send `Response::Error` on failure.
async fn collect_receiver_snapshots(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
    prefix_regex: Option<&str>,
) -> Result<(Vec<SnapshotEntry>, Option<String>), Response> {
    if let Err(e) = validate_dataset_name(dataset) {
        return Err(Response::Error {
            code: ErrorCode::BadRequest,
            message: format!("invalid dataset {dataset:?}: {e}"),
        });
    }
    enforce_root_fs(acl, dataset)?;
    let regex = match compile_prefix_regex(prefix_regex) {
        Ok(opt) => opt,
        Err(e) => {
            return Err(Response::Error {
                code: ErrorCode::BadRequest,
                message: format!("compile prefix_regex {:?}: {e}", prefix_regex.unwrap_or("")),
            });
        }
    };
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        // creation/used ride along for the UI browser; the planner path
        // (ListReceiverGuids) ignores them and two extra columns on one
        // `zfs list` are noise-level cost.
        properties: vec!["guid".into(), "creation".into(), "used".into()],
        ..ListOptions::default()
    };
    let entries = match palimpsest::dataset::list(runner, &opts).await {
        Ok(v) => v,
        // First-replication shape: receiver dataset doesn't exist yet.
        Err(ZfsError::DatasetNotFound { .. }) => return Ok((vec![], None)),
        Err(e) => {
            return Err(Response::Error {
                code: zfs_error_code(&e),
                message: format!("list {dataset}: {e}"),
            });
        }
    };
    let snapshots: Vec<SnapshotEntry> = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if let Some(re) = &regex
                && !re.is_match(&snap_name)
            {
                return None;
            }
            let guid = e
                .properties
                .get("guid")
                .and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(SnapshotEntry {
                name: snap_name,
                guid,
                createtxg,
                creation: e
                    .properties
                    .get("creation")
                    .and_then(|p| p.value.parse::<i64>().ok()),
                used: e
                    .properties
                    .get("used")
                    .and_then(|p| p.value.parse::<u64>().ok()),
            })
        })
        .collect();
    let receive_resume_token = match palimpsest::recv::receive_resume_token(runner, dataset).await {
        Ok(opt) => opt,
        Err(ZfsError::DatasetNotFound { .. }) => None,
        Err(e) => {
            tracing::warn!(error = %e, dataset, "receive_resume_token query failed");
            None
        }
    };
    Ok((snapshots, receive_resume_token))
}

/// List filesystems/volumes under the client's `root_fs` — the ACL
/// scope IS the listing root, so nothing outside the subtree leaks.
/// A client without `root_fs` (validation requires one for control
/// clients, so this is belt-and-suspenders) gets an empty list rather
/// than the whole pool.
async fn handle_list_datasets(runner: &dyn CommandRunner, acl: &AllowedClient) -> Response {
    let Some(root) = acl.root_fs.as_deref() else {
        return Response::ListDatasetsOk { datasets: vec![] };
    };
    let opts = ListOptions {
        recursive: true,
        types: vec![DatasetType::Filesystem, DatasetType::Volume],
        roots: vec![root.to_string()],
        properties: vec!["used".into(), "usedbysnapshots".into()],
        ..ListOptions::default()
    };
    let entries = match palimpsest::dataset::list(runner, &opts).await {
        Ok(v) => v,
        // Nothing received yet — the subtree root doesn't exist.
        Err(ZfsError::DatasetNotFound { .. }) => {
            return Response::ListDatasetsOk { datasets: vec![] };
        }
        Err(e) => {
            return Response::Error {
                code: zfs_error_code(&e),
                message: format!("list {root}: {e}"),
            };
        }
    };
    Response::ListDatasetsOk {
        datasets: entries
            .into_iter()
            .map(|e| arctern_transport::DatasetWire {
                used: e.properties.get("used").map(|p| p.value.clone()),
                usedbysnapshots: e.properties.get("usedbysnapshots").map(|p| p.value.clone()),
                kind: e.kind.cli_name().to_string(),
                name: e.name,
            })
            .collect(),
    }
}

/// User holds on one snapshot, root_fs-scoped through the dataset part
/// of the target.
async fn handle_list_holds(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    snapshot: &str,
) -> Response {
    let target = match parse_snapshot_target(snapshot) {
        Ok(t) => t,
        Err(e) => {
            return Response::Error {
                code: ErrorCode::BadRequest,
                message: format!("invalid snapshot target {snapshot:?}: {e}"),
            };
        }
    };
    if let Err(r) = enforce_root_fs(acl, target.dataset) {
        return r;
    }
    match palimpsest::hold::list_holds(runner, snapshot).await {
        Ok(holds) => Response::ListHoldsOk {
            holds: holds
                .into_iter()
                .map(|h| arctern_transport::HoldWire {
                    tag: h.tag,
                    timestamp: h.timestamp,
                })
                .collect(),
        },
        Err(ZfsError::DatasetNotFound { .. }) => Response::ListHoldsOk { holds: vec![] },
        Err(e) => Response::Error {
            code: zfs_error_code(&e),
            message: format!("holds {snapshot}: {e}"),
        },
    }
}

async fn handle_get_receive_resume_token(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
) -> Response {
    if let Err(e) = validate_dataset_name(dataset) {
        return Response::Error {
            code: ErrorCode::BadRequest,
            message: format!("invalid dataset {dataset:?}: {e}"),
        };
    }
    if let Err(r) = enforce_root_fs(acl, dataset) {
        return r;
    }
    match palimpsest::recv::receive_resume_token(runner, dataset).await {
        Ok(token) => Response::GetReceiveResumeTokenOk { token },
        Err(ZfsError::DatasetNotFound { .. }) => Response::GetReceiveResumeTokenOk { token: None },
        Err(e) => Response::Error {
            code: zfs_error_code(&e),
            message: format!("receive_resume_token {dataset}: {e}"),
        },
    }
}

async fn handle_destroy_snapshot(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    name: &str,
) -> Response {
    let target = match parse_snapshot_target(name) {
        Ok(target) => target,
        Err(e) => {
            return Response::Error {
                code: ErrorCode::BadRequest,
                message: format!("invalid destroy snapshot target {name:?}: {e}"),
            };
        }
    };
    if let Err(r) = enforce_root_fs(acl, target.dataset) {
        return r;
    }
    let opts = palimpsest::dataset::DestroyOptions::new();
    match palimpsest::dataset::destroy(runner, name, &opts).await {
        Ok(()) => Response::DestroySnapshotOk,
        Err(e) => Response::Error {
            code: zfs_error_code(&e),
            message: format!("destroy {name}: {e}"),
        },
    }
}

async fn handle_discard_partial_recv(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
) -> Response {
    if let Err(e) = validate_dataset_name(dataset) {
        return Response::Error {
            code: ErrorCode::BadRequest,
            message: format!("invalid dataset {dataset:?}: {e}"),
        };
    }
    if let Err(r) = enforce_root_fs(acl, dataset) {
        return r;
    }
    match palimpsest::recv::abort_partial(runner, dataset).await {
        Ok(()) => Response::DiscardPartialRecvOk,
        Err(e) => Response::Error {
            code: zfs_error_code(&e),
            message: format!("abort_partial {dataset}: {e}"),
        },
    }
}

fn zfs_error_code(e: &ZfsError) -> ErrorCode {
    match e {
        ZfsError::DatasetNotFound { .. } => ErrorCode::NotFound,
        _ => ErrorCode::Zfs,
    }
}

/// Where the local daemon's API socket lives. The stdinserver process
/// is spawned by sshd, so the daemon's `--socket` flag is invisible
/// here — the config's `socket` key is the shared rendezvous point.
fn daemon_socket(config: &Config) -> std::path::PathBuf {
    config
        .socket
        .clone()
        .unwrap_or_else(crate::default_socket_path)
}

/// Proxy `ListJobs` into the local daemon over its UDS. The
/// stdinserver process has no JobManager of its own; the daemon is the
/// scheduler, and this bridge is what makes the sender's "peer jobs"
/// view show real data.
async fn handle_list_jobs(config: &Config) -> Response {
    match arctern_client::list_jobs(&daemon_socket(config)).await {
        Ok(jobs) => Response::ListJobsOk {
            jobs: jobs
                .into_iter()
                .map(|j| arctern_transport::JobStatusWire {
                    name: j.name,
                    kind: j.kind,
                    last_run: j.last_run,
                    next_run: j.next_run,
                    last_error: j.last_error,
                    running: j.running,
                })
                .collect(),
        },
        Err(e) => Response::Error {
            code: ErrorCode::Internal,
            message: format!(
                "local daemon unreachable at {}: {e}",
                daemon_socket(config).display()
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_transport::{read_response, write_request};
    use palimpsest::runner::{Cmd, RecordingRunner};
    use std::sync::Arc;

    fn acl(root_fs: Option<&str>) -> AllowedClient {
        acl_with_ops(root_fs, &["control", "recv"])
    }

    fn acl_with_ops(root_fs: Option<&str>, operations: &[&str]) -> AllowedClient {
        AllowedClient {
            identity: "test".into(),
            fingerprint: None,
            jobs: vec!["backup".into()],
            operations: operations.iter().map(|op| (*op).to_string()).collect(),
            root_fs: root_fs.map(str::to_string),
            recv: Default::default(),
        }
    }

    fn cfg() -> Arc<Config> {
        // Hermetic: point the UDS-proxy paths at a socket that cannot
        // exist so tests never talk to a real daemon on the dev host.
        Arc::new(Config {
            socket: Some(std::path::PathBuf::from("/nonexistent/arctern-test.sock")),
            ..Config::default()
        })
    }

    /// One end-to-end roundtrip per request kind, using duplex pipes
    /// for the framed transport and a RecordingRunner for ZFS.
    async fn rpc(runner: Arc<dyn CommandRunner>, acl: AllowedClient, req: Request) -> Response {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (areader, awriter) = tokio::io::split(a);
        let (mut breader, mut bwriter) = tokio::io::split(b);
        let server =
            tokio::spawn(async move { run(runner, cfg(), acl, None, areader, awriter).await });
        let frame = RequestFrame { id: 1, body: req };
        write_request(&mut bwriter, &frame).await.unwrap();
        // Send Shutdown to make the server exit cleanly after the reply.
        let frame = RequestFrame {
            id: 2,
            body: Request::Shutdown,
        };
        write_request(&mut bwriter, &frame).await.unwrap();
        let resp = read_response(&mut breader).await.unwrap();
        // Drain the Shutdown reply so the server can exit.
        let _ = read_response(&mut breader).await;
        let _ = server.await.unwrap();
        resp.body
    }

    #[tokio::test]
    async fn list_snapshots_returns_empty_on_dataset_not_found() {
        // RecordingRunner with no recorded commands returns the
        // "no matching command" io::Error path; instead, record the
        // expected `zfs list` invocation returning a `dataset does not
        // exist` stderr.
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid,creation,used",
                "tank/missing",
            ]),
            Vec::new(),
            b"cannot open 'tank/missing': dataset does not exist".to_vec(),
            1,
        ));
        let r = rpc(
            runner,
            acl(None),
            Request::ListSnapshots {
                dataset: "tank/missing".into(),
                prefix_regex: None,
            },
        )
        .await;
        match r {
            Response::ListSnapshotsOk {
                snapshots,
                receive_resume_token,
            } => {
                assert!(snapshots.is_empty());
                assert_eq!(receive_resume_token, None);
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_receiver_guids_returns_empty_on_dataset_not_found() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid,creation,used",
                "tank/missing",
            ]),
            Vec::new(),
            b"cannot open 'tank/missing': dataset does not exist".to_vec(),
            1,
        ));
        let r = rpc(
            runner,
            acl(None),
            Request::ListReceiverGuids {
                dataset: "tank/missing".into(),
                prefix_regex: None,
            },
        )
        .await;
        match r {
            Response::ListReceiverGuidsOk {
                guids,
                receive_resume_token,
            } => {
                assert!(guids.is_empty());
                assert_eq!(receive_resume_token, None);
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_receiver_guids_enforces_root_fs() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::ListReceiverGuids {
                dataset: "tank/other".into(),
                prefix_regex: None,
            },
        )
        .await;
        match r {
            Response::Error { code, .. } => assert_eq!(code, ErrorCode::Unauthorized),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn root_fs_acl_rejects_outside_subtree() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::ListSnapshots {
                dataset: "tank/other".into(),
                prefix_regex: None,
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::Unauthorized);
                assert!(message.contains("tank/backups/laptop"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn root_fs_acl_accepts_root_itself() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid,creation,used",
                "tank/backups/laptop",
            ]),
            Vec::new(),
            b"cannot open 'tank/backups/laptop': dataset does not exist".to_vec(),
            1,
        ));
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::ListSnapshots {
                dataset: "tank/backups/laptop".into(),
                prefix_regex: None,
            },
        )
        .await;
        assert!(matches!(r, Response::ListSnapshotsOk { .. }), "got {r:?}");
    }

    #[tokio::test]
    async fn destroy_snapshot_rejects_dataset_target() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:destroy_snapshot", "recv"],
            ),
            Request::DestroySnapshot {
                name: "tank/backups/laptop".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::BadRequest);
                assert!(message.contains("dataset@snapshot"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn destroy_snapshot_rejects_bookmark_target() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:destroy_snapshot", "recv"],
            ),
            Request::DestroySnapshot {
                name: "tank/backups/laptop#cursor".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, .. } => assert_eq!(code, ErrorCode::BadRequest),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn destroy_snapshot_requires_fine_grained_acl() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::DestroySnapshot {
                name: "tank/backups/laptop@s1".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::Unauthorized);
                assert!(message.contains("control:destroy_snapshot"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn destroy_snapshot_accepts_snapshot_inside_root() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args(["destroy", "tank/backups/laptop@s1"]),
            vec![],
            vec![],
            0,
        ));
        let r = rpc(
            runner,
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:destroy_snapshot", "recv"],
            ),
            Request::DestroySnapshot {
                name: "tank/backups/laptop@s1".into(),
            },
        )
        .await;
        assert!(matches!(r, Response::DestroySnapshotOk), "got {r:?}");
    }

    #[tokio::test]
    async fn list_snapshots_rejects_invalid_dataset_name() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::ListSnapshots {
                dataset: "tank/backups/laptop/../escape".into(),
                prefix_regex: None,
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::BadRequest);
                assert!(message.contains("invalid dataset"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn destroy_snapshot_rejects_invalid_snapshot_name() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:destroy_snapshot", "recv"],
            ),
            Request::DestroySnapshot {
                name: "tank/backups/laptop@bad snap".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::BadRequest);
                assert!(message.contains("invalid destroy snapshot target"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn discard_partial_recv_rejects_invalid_dataset_name() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl_with_ops(
                Some("tank/backups/laptop"),
                &["control", "control:discard_partial_recv", "recv"],
            ),
            Request::DiscardPartialRecv {
                dataset: "tank/backups/laptop#bookmark".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::BadRequest);
                assert!(message.contains("invalid dataset"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn discard_partial_recv_requires_fine_grained_acl() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::DiscardPartialRecv {
                dataset: "tank/backups/laptop".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::Unauthorized);
                assert!(message.contains("control:discard_partial_recv"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn legacy_control_still_allows_read_only_requests() {
        let runner = Arc::new(RecordingRunner::new().record(
            Cmd::new("zfs").args([
                "list",
                "-j",
                "-p",
                "-t",
                "snapshot",
                "-o",
                "guid,creation,used",
                "tank/backups/laptop",
            ]),
            Vec::new(),
            b"cannot open 'tank/backups/laptop': dataset does not exist".to_vec(),
            1,
        ));
        let r = rpc(
            runner,
            acl(Some("tank/backups/laptop")),
            Request::ListSnapshots {
                dataset: "tank/backups/laptop".into(),
                prefix_regex: None,
            },
        )
        .await;
        assert!(matches!(r, Response::ListSnapshotsOk { .. }), "got {r:?}");
    }

    #[tokio::test]
    async fn unimplemented_subscribe_events_reports_internal_error() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(runner, acl(None), Request::SubscribeEvents { since: None }).await;
        match r {
            Response::Error { code, .. } => assert_eq!(code, ErrorCode::Internal),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_jobs_errors_honestly_when_local_daemon_unreachable() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(runner, acl(None), Request::ListJobs).await;
        match r {
            Response::Error { code, message } => {
                assert_eq!(code, ErrorCode::Internal);
                assert!(
                    message.contains("local daemon unreachable"),
                    "got: {message}"
                );
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wakeup_job_errors_honestly_when_local_daemon_unreachable() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl_with_ops(None, &["control", "control:wakeup_job"]),
            Request::WakeupJob {
                name: "backup".into(),
            },
        )
        .await;
        match r {
            Response::Error { code, .. } => assert_eq!(code, ErrorCode::Internal),
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
