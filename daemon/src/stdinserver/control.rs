//! Server-side control-channel handler. Reads RequestFrames off stdin,
//! dispatches each to the matching `handle_*` function, writes
//! ResponseFrames to stdout. Stays single-task per channel (sshd
//! spawns one process per session) — concurrency on the receiver side
//! comes from sshd accepting parallel sessions, not from this loop.
//!
//! Per-Request handlers translate `palimpsest::ZfsError` and friends
//! into `Response::Error { code, message }` rather than letting them
//! escape; the caller never sees a process exit short of EOF.

use std::sync::Arc;

use arctern_config::{AllowedClient, Config};
use arctern_transport::{
    ErrorCode, EventWire, JobStatusWire, Request, RequestFrame, Response, ResponseFrame,
    SnapshotEntry, compile_prefix_regex, read_request, write_response,
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
    // Stdout is shared between the main request/response loop and any
    // background SubscribeEvents pusher; serialise writes through one
    // mutex so frames never interleave on the wire.
    let writer = Arc::new(Mutex::new(BufWriter::new(writer)));
    let mut event_pushers: Vec<tokio::task::JoinHandle<()>> = Vec::new();
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
                    let writer_for_task = writer.clone();
                    event_pushers.push(tokio::spawn(async move {
                        push_events(p, since, writer_for_task).await;
                    }));
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
        let resp_body = dispatch(runner.as_ref(), &config, &acl, pool.as_deref(), body).await;
        let resp = ResponseFrame {
            request_id: Some(id),
            body: resp_body,
        };
        let mut w = writer.lock().await;
        if let Err(e) = write_response(&mut *w, &resp).await {
            tracing::warn!(error = %e, "control: write_response failed; closing");
            break Ok(());
        }
        if let Err(e) = w.flush().await {
            tracing::warn!(error = %e, "control: flush failed; closing");
            break Ok(());
        }
    };
    for h in event_pushers {
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
    _config: &Config,
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
        Request::GetReceiveResumeToken { dataset } => {
            if let Err(r) = enforce_control_acl(acl, "control:get_resume_token", true) {
                return r;
            }
            handle_get_receive_resume_token(runner, acl, &dataset).await
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
            Response::ListJobsOk { jobs: Vec::new() }
        }
        Request::GetJobStatus { name: _ } => {
            if let Err(r) = enforce_control_acl(acl, "control:get_job_status", true) {
                return r;
            }
            Response::Error {
                code: ErrorCode::NotFound,
                message: "GetJobStatus not yet implemented on the receiver".into(),
            }
        }
        Request::WakeupJob { name: _ } => {
            if let Err(r) = enforce_control_acl(acl, "control:wakeup_job", false) {
                return r;
            }
            Response::Error {
                code: ErrorCode::NotFound,
                message: "WakeupJob not yet implemented on the receiver".into(),
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
/// equal to or a descendant of it. Returns Ok((root_fs, dataset)) on
/// success — the second element is just `dataset` borrowed back so the
/// caller doesn't have to repeat the path.
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
    if let Err(r) = enforce_root_fs(acl, dataset) {
        return r;
    }
    let regex = match compile_prefix_regex(prefix_regex) {
        Ok(opt) => opt,
        Err(e) => {
            return Response::Error {
                code: ErrorCode::BadRequest,
                message: format!("compile prefix_regex {:?}: {e}", prefix_regex.unwrap_or("")),
            };
        }
    };
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = match palimpsest::dataset::list(runner, &opts).await {
        Ok(v) => v,
        Err(ZfsError::DatasetNotFound { .. }) => {
            // First-replication shape: receiver dataset doesn't exist yet.
            return Response::ListSnapshotsOk {
                snapshots: vec![],
                receive_resume_token: None,
            };
        }
        Err(e) => {
            return Response::Error {
                code: zfs_error_code(&e),
                message: format!("list {dataset}: {e}"),
            };
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
    Response::ListSnapshotsOk {
        snapshots,
        receive_resume_token,
    }
}

async fn handle_get_receive_resume_token(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
) -> Response {
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
    let Some((dataset, snapshot)) = parse_snapshot_target(name) else {
        return Response::Error {
            code: ErrorCode::BadRequest,
            message: format!("destroy snapshot target must be dataset@snapshot, got {name:?}"),
        };
    };
    if let Err(r) = enforce_root_fs(acl, dataset) {
        return r;
    }
    let opts = palimpsest::dataset::DestroyOptions::new();
    match palimpsest::dataset::destroy(runner, &format!("{dataset}@{snapshot}"), &opts).await {
        Ok(()) => Response::DestroySnapshotOk,
        Err(e) => Response::Error {
            code: zfs_error_code(&e),
            message: format!("destroy {name}: {e}"),
        },
    }
}

fn parse_snapshot_target(name: &str) -> Option<(&str, &str)> {
    let (dataset, snapshot) = name.split_once('@')?;
    if dataset.is_empty()
        || snapshot.is_empty()
        || dataset.contains('@')
        || snapshot.contains('@')
        || dataset.contains('#')
        || snapshot.contains('#')
    {
        return None;
    }
    Some((dataset, snapshot))
}

async fn handle_discard_partial_recv(
    runner: &dyn CommandRunner,
    acl: &AllowedClient,
    dataset: &str,
) -> Response {
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

// Small helper kept here (rather than as a free fn next to JobStatusWire)
// because it's not used outside the daemon.
#[allow(dead_code)]
fn job_status_wire(name: String, kind: String) -> JobStatusWire {
    JobStatusWire {
        name,
        kind,
        last_run: None,
        next_run: None,
        last_error: None,
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
        Arc::new(Config::default())
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
                "guid",
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
                "guid",
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
                "guid",
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
    async fn list_jobs_returns_empty() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(runner, acl(None), Request::ListJobs).await;
        match r {
            Response::ListJobsOk { jobs } => assert!(jobs.is_empty()),
            other => panic!("expected ListJobsOk, got {other:?}"),
        }
    }
}
