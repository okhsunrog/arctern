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
    ErrorCode, JobStatusWire, Request, RequestFrame, Response, ResponseFrame, SnapshotEntry,
    compile_prefix_regex, read_request, write_response,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use palimpsest::ZfsError;
use tokio::io::{AsyncRead, AsyncWrite, BufWriter};

/// Run the control channel until stdin EOF or a fatal write error.
/// `acl` scopes destroy / discard operations; `runner` is the
/// palimpsest CommandRunner the dispatch process opened (typically a
/// `RealRunner` invoking local `zfs(8)`).
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    config: Arc<Config>,
    acl: AllowedClient,
    mut reader: R,
    writer: W,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut writer = BufWriter::new(writer);
    loop {
        let frame: RequestFrame = match read_request(&mut reader).await {
            Ok(f) => f,
            Err(arctern_transport::ProtocolError::UnexpectedEof) => return Ok(()),
            Err(arctern_transport::ProtocolError::Io(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(error = %e, "control: bad request frame; closing channel");
                return Ok(());
            }
        };
        let RequestFrame { id, body } = frame;
        if matches!(body, Request::Shutdown) {
            // Reply Ok then exit so the caller sees the ack before EOF.
            let resp = ResponseFrame {
                request_id: Some(id),
                body: Response::Ok,
            };
            let _ = write_response(&mut writer, &resp).await;
            use tokio::io::AsyncWriteExt;
            let _ = writer.flush().await;
            return Ok(());
        }
        let resp_body = dispatch(runner.as_ref(), &config, &acl, body).await;
        let resp = ResponseFrame {
            request_id: Some(id),
            body: resp_body,
        };
        if let Err(e) = write_response(&mut writer, &resp).await {
            tracing::warn!(error = %e, "control: write_response failed; closing");
            return Ok(());
        }
        use tokio::io::AsyncWriteExt;
        if let Err(e) = writer.flush().await {
            tracing::warn!(error = %e, "control: flush failed; closing");
            return Ok(());
        }
    }
}

async fn dispatch(
    runner: &dyn CommandRunner,
    _config: &Config,
    acl: &AllowedClient,
    req: Request,
) -> Response {
    match req {
        Request::ListSnapshots {
            dataset,
            prefix_regex,
        } => handle_list_snapshots(runner, acl, &dataset, prefix_regex.as_deref()).await,
        Request::GetReceiveResumeToken { dataset } => {
            handle_get_receive_resume_token(runner, acl, &dataset).await
        }
        Request::DestroySnapshot { name } => {
            handle_destroy_snapshot(runner, acl, &name).await
        }
        Request::DiscardPartialRecv { dataset } => {
            handle_discard_partial_recv(runner, acl, &dataset).await
        }
        Request::ListJobs => Response::ListJobsOk { jobs: Vec::new() },
        Request::GetJobStatus { name: _ } => Response::Error {
            code: ErrorCode::NotFound,
            message: "GetJobStatus not yet implemented on the receiver".into(),
        },
        Request::WakeupJob { name: _ } => Response::Error {
            code: ErrorCode::NotFound,
            message: "WakeupJob not yet implemented on the receiver".into(),
        },
        Request::SubscribeEvents { since: _ } => Response::Error {
            code: ErrorCode::Internal,
            message: "SubscribeEvents requires the SSE bridge (step 11)".into(),
        },
        Request::GetLogCursor => Response::GetLogCursorOk { id: 0 },
        Request::Shutdown => unreachable!("handled in run()"),
    }
}

/// Reject `dataset` if the ACL has a `root_fs` set and `dataset` is not
/// equal to or a descendant of it. Returns Ok((root_fs, dataset)) on
/// success — the second element is just `dataset` borrowed back so the
/// caller doesn't have to repeat the path.
fn enforce_root_fs<'a>(
    acl: &'a AllowedClient,
    dataset: &'a str,
) -> Result<(), Response> {
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
        message: format!(
            "{dataset:?} is not under allowed root_fs {root:?}"
        ),
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
                message: format!(
                    "compile prefix_regex {:?}: {e}",
                    prefix_regex.unwrap_or("")
                ),
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
            let guid = e.properties.get("guid").and_then(|p| p.value.parse::<u64>().ok())?;
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
        Err(ZfsError::DatasetNotFound { .. }) => {
            Response::GetReceiveResumeTokenOk { token: None }
        }
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
    // Snapshot names look like `dataset@snap`; root_fs ACL applies to
    // the dataset half.
    let dataset = name.split('@').next().unwrap_or(name);
    if let Err(r) = enforce_root_fs(acl, dataset) {
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
        AllowedClient {
            identity: "test".into(),
            fingerprint: None,
            jobs: vec!["backup".into()],
            operations: vec!["control".into(), "recv".into()],
            root_fs: root_fs.map(str::to_string),
        }
    }

    fn cfg() -> Arc<Config> {
        Arc::new(Config::default())
    }

    /// One end-to-end roundtrip per request kind, using duplex pipes
    /// for the framed transport and a RecordingRunner for ZFS.
    async fn rpc(
        runner: Arc<dyn CommandRunner>,
        acl: AllowedClient,
        req: Request,
    ) -> Response {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (areader, awriter) = tokio::io::split(a);
        let (mut breader, mut bwriter) = tokio::io::split(b);
        let server =
            tokio::spawn(async move { run(runner, cfg(), acl, areader, awriter).await });
        let frame = RequestFrame { id: 1, body: req };
        write_request(&mut bwriter, &frame).await.unwrap();
        // Send Shutdown to make the server exit cleanly after the reply.
        let frame = RequestFrame { id: 2, body: Request::Shutdown };
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
    async fn unimplemented_subscribe_events_reports_internal_error() {
        let runner = Arc::new(RecordingRunner::new());
        let r = rpc(
            runner,
            acl(None),
            Request::SubscribeEvents { since: None },
        )
        .await;
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
