//! Server-side recv-channel handler. One process per replication step:
//!
//!   1. Read a single `RecvHeader` from stdin (length-prefixed JSON).
//!   2. Optionally `zfskit::recv::abort_partial` against the target
//!      when `header.send.discard_partial_recv` is set.
//!   3. Spawn `zfs recv -s -u` via zfskit's streaming recv.
//!   4. Copy stdin bytes into the recv child's stdin until EOF.
//!   5. Wait for the recv child to exit.
//!   6. Advance the last-received hold (`arctern_last_J_<job>`) to the
//!      just-received snapshot so a receiver-side prune job cannot
//!      destroy the last common snapshot between syncs.
//!   7. Record the completed transfer (bytes, duration) in
//!      `recv_transfers` and emit a structured completion event —
//!      receiver-side visibility for the "Incoming" panel.
//!   8. Write a single `ResponseFrame` (Ok / Error) to stdout.

use std::sync::Arc;

use arctern_config::AllowedClient;
use arctern_config::zfs_names::{validate_dataset_name, validate_snapshot_leaf};
use arctern_transport::{
    ErrorCode, RecvHeader, Response, ResponseFrame, read_header, write_response,
};
use sqlx::SqlitePool;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::recv::{RecvArgs, recv as zfs_recv};
use zfskit::runner::CommandRunner;

/// Drive one recv channel from start to finish. Errors are surfaced as
/// `Response::Error` written back to the caller; the function only
/// returns `Err` on stdin/stdout I/O failures so the calling process
/// can exit with a non-zero code.
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    acl: AllowedClient,
    pool: Option<Arc<SqlitePool>>,
    job: &str,
    mut reader: R,
    mut writer: W,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let header = match read_header(&mut reader).await {
        Ok(h) => h,
        Err(e) => {
            let resp = ResponseFrame {
                request_id: None,
                body: Response::Error {
                    code: ErrorCode::BadRequest,
                    message: format!("read RecvHeader: {e}"),
                },
            };
            let _ = write_response(&mut writer, &resp).await;
            let _ = writer.flush().await;
            return Ok(());
        }
    };
    let started = std::time::Instant::now();
    let outcome = drive(&runner, &acl, &header, &mut reader).await;
    if let Ok(bytes) = outcome {
        advance_last_hold(
            runner.as_ref(),
            job,
            &header.target_dataset,
            &header.send.to_snap.name,
        )
        .await;
        report_transfer(pool.as_deref(), job, &acl.identity, &header, bytes, started).await;
    }
    let resp = ResponseFrame {
        request_id: None,
        body: match outcome {
            Ok(_) => Response::Ok,
            Err((code, message)) => Response::Error { code, message },
        },
    };
    if let Err(e) = write_response(&mut writer, &resp).await {
        tracing::warn!(error = %e, "recv: write final response failed");
    }
    let _ = writer.flush().await;
    Ok(())
}

fn last_hold_tag(job: &str) -> String {
    format!("arctern_last_J_{job}")
}

/// Place the last-received hold on the just-received snapshot, then
/// release the tag from every other snapshot of the dataset (the
/// previous holder, plus any stale ones). Best-effort: the stream has
/// already landed, so failures here degrade retention protection but
/// must not fail the replication step.
async fn advance_last_hold(
    runner: &dyn CommandRunner,
    job: &str,
    target_dataset: &str,
    to_leaf: &str,
) {
    let tag = last_hold_tag(job);
    let new_snap = format!("{target_dataset}@{to_leaf}");
    if let Err(e) = zfskit::hold::hold(runner, &new_snap, &tag).await {
        tracing::warn!(snapshot = %new_snap, tag = %tag, error = %e, "recv: last-received hold failed");
        return;
    }
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![target_dataset.to_string()],
        ..ListOptions::default()
    };
    let snaps = match zfskit::dataset::list(runner, &opts).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(dataset = %target_dataset, error = %e, "recv: last-hold sweep list failed");
            return;
        }
    };
    let others: Vec<&str> = snaps
        .iter()
        .map(|s| s.name.as_str())
        .filter(|n| *n != new_snap)
        .collect();
    let holds = match zfskit::hold::list_holds_many(runner, &others).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(dataset = %target_dataset, error = %e, "recv: last-hold sweep holds query failed");
            return;
        }
    };
    for h in holds.iter().filter(|h| h.tag == tag) {
        if let Err(e) = zfskit::hold::release(runner, &h.dataset, &tag).await {
            tracing::warn!(snapshot = %h.dataset, tag = %tag, error = %e, "recv: release stale last-hold failed");
        }
    }
}

async fn drive<R>(
    runner: &Arc<dyn CommandRunner>,
    acl: &AllowedClient,
    header: &RecvHeader,
    reader: &mut R,
) -> Result<u64, (ErrorCode, String)>
where
    R: AsyncRead + Unpin,
{
    validate_header(header)?;
    if let Err(msg) = enforce_root_fs(acl, &header.target_dataset) {
        return Err((ErrorCode::Unauthorized, msg));
    }
    if header.send.discard_partial_recv {
        tracing::info!(
            target = %header.target_dataset,
            "recv: discarding partial recv per sender request"
        );
        if let Err(e) = zfskit::recv::abort_partial(runner.as_ref(), &header.target_dataset).await {
            return Err((
                ErrorCode::Zfs,
                format!("abort_partial {}: {e}", header.target_dataset),
            ));
        }
    }
    // Ensure the receive parent exists; the leaf dataset is created by
    // `zfs recv` itself.
    if let Some((parent, _)) = header.target_dataset.rsplit_once('/') {
        let opts = zfskit::dataset::CreateOptions::new()
            .create_parents()
            .property("mountpoint", "none");
        match zfskit::dataset::create(runner.as_ref(), parent, &opts).await {
            Ok(()) => {}
            Err(e) => {
                let stderr = format!("{e}");
                if !stderr.contains("already exists") {
                    return Err((ErrorCode::Zfs, format!("ensure parent {parent}: {e}")));
                }
            }
        }
    }
    // -s for resumable, -u to keep the receive unmounted (the operator's
    // mountpoint policy is set elsewhere). `acl.recv` carries any
    // per-client `-o k=v` / `-x k` flags from arctern.toml — mirrors
    // zrepl's `recv.properties.override` / `recv.properties.inherit`.
    let mut args = RecvArgs::new(header.target_dataset.clone())
        .unmounted()
        .resumable();
    for key in &acl.recv.inherit_properties {
        args = args.property_inherit(key);
    }
    for (k, v) in &acl.recv.override_properties {
        args = args.property_override(k, v);
    }
    let mut handle = zfs_recv(runner.as_ref(), &args)
        .await
        .map_err(|e| (ErrorCode::Zfs, format!("spawn zfs recv: {e}")))?;
    let mut child_stdin = handle
        .stdin
        .take()
        .ok_or((ErrorCode::Internal, "no stdin on recv child".into()))?;
    let mut child_stderr = handle
        .stderr
        .take()
        .ok_or((ErrorCode::Internal, "no stderr on recv child".into()))?;
    let stderr_drain = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = child_stderr.read_to_end(&mut buf).await;
        buf
    });
    // `copy` returns the byte count — that IS the transfer size report.
    let copy_res = tokio::io::copy(reader, &mut child_stdin).await;
    let _ = child_stdin.shutdown().await;
    drop(child_stdin);
    let stderr_bytes = stderr_drain.await.unwrap_or_default();
    let exit = handle
        .wait()
        .await
        .map_err(|e| (ErrorCode::Zfs, format!("recv wait: {e}")))?;
    if let Err(e) = copy_res {
        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        return Err((
            ErrorCode::Zfs,
            format!("stream copy: {e}; recv stderr: {}", stderr_text.trim()),
        ));
    }
    if !exit.success() {
        let stderr_text = String::from_utf8_lossy(&stderr_bytes);
        return Err((
            ErrorCode::Zfs,
            format!(
                "zfs recv failed (exit {:?}): {}",
                exit.code(),
                stderr_text.trim()
            ),
        ));
    }
    Ok(copy_res.unwrap_or(0))
}

/// Persist + announce one completed transfer. Best-effort on both
/// counts: the stream has already landed, so a reporting failure must
/// never fail the replication step.
async fn report_transfer(
    pool: Option<&SqlitePool>,
    job: &str,
    identity: &str,
    header: &RecvHeader,
    bytes: u64,
    started: std::time::Instant,
) {
    let duration_ms = started.elapsed().as_millis() as i64;
    let from = header.send.from_snap.as_ref().map(|s| s.name.as_str());
    tracing::info!(
        dataset = %header.target_dataset,
        snapshot = %header.send.to_snap.name,
        bytes,
        duration_ms,
        "recv: transfer complete"
    );
    let Some(pool) = pool else { return };
    if let Err(e) = crate::state::recv_transfers::record(
        pool,
        time::OffsetDateTime::now_utc().unix_timestamp(),
        job,
        identity,
        &header.target_dataset,
        &header.send.to_snap.name,
        from,
        bytes as i64,
        duration_ms,
    )
    .await
    {
        tracing::warn!(error = %e, "recv: transfer record failed");
    }
}

fn validate_header(header: &RecvHeader) -> Result<(), (ErrorCode, String)> {
    if let Err(e) = validate_dataset_name(&header.target_dataset) {
        return Err((
            ErrorCode::BadRequest,
            format!("invalid target_dataset {:?}: {e}", header.target_dataset),
        ));
    }
    validate_snapshot_ref("to_snap", &header.send.to_snap.name)?;
    if let Some(from) = &header.send.from_snap {
        validate_snapshot_ref("from_snap", &from.name)?;
    }
    Ok(())
}

fn validate_snapshot_ref(field: &str, name: &str) -> Result<(), (ErrorCode, String)> {
    if let Err(e) = validate_snapshot_leaf(name) {
        return Err((
            ErrorCode::BadRequest,
            format!("invalid {field} snapshot name {name:?}: {e}"),
        ));
    }
    Ok(())
}

fn enforce_root_fs(acl: &AllowedClient, dataset: &str) -> Result<(), String> {
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
    Err(format!("{dataset:?} is not under allowed root_fs {root:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_transport::{SendFlagsWire, SendHeader, SendKind, SnapshotRef};

    fn header(target_dataset: &str, from_snap: Option<&str>, to_snap: &str) -> RecvHeader {
        RecvHeader {
            version: arctern_transport::PROTOCOL_VERSION,
            target_dataset: target_dataset.to_string(),
            send: SendHeader {
                send_kind: SendKind::Full,
                from_snap: from_snap.map(|name| SnapshotRef {
                    name: name.to_string(),
                    guid: 1,
                }),
                to_snap: SnapshotRef {
                    name: to_snap.to_string(),
                    guid: 2,
                },
                flags: SendFlagsWire {
                    raw: false,
                    embedded: false,
                    compressed: false,
                    large_blocks: false,
                },
                discard_partial_recv: false,
            },
        }
    }

    #[test]
    fn validate_header_rejects_invalid_target_dataset() {
        let h = header("tank/backups#bookmark", None, "snap1");
        let err = validate_header(&h).unwrap_err();
        assert_eq!(err.0, ErrorCode::BadRequest);
        assert!(err.1.contains("invalid target_dataset"));
    }

    #[test]
    fn validate_header_rejects_invalid_snapshot_refs() {
        let h = header("tank/backups", Some("base snap"), "snap1");
        let err = validate_header(&h).unwrap_err();
        assert_eq!(err.0, ErrorCode::BadRequest);
        assert!(err.1.contains("from_snap"));

        let h = header("tank/backups", None, "snap/child");
        let err = validate_header(&h).unwrap_err();
        assert_eq!(err.0, ErrorCode::BadRequest);
        assert!(err.1.contains("to_snap"));
    }

    #[test]
    fn validate_header_accepts_common_names() {
        let h = header(
            "tank/backups/laptop",
            Some("zrepl_2026-05-15"),
            "zrepl_2026-05-16",
        );
        validate_header(&h).unwrap();
    }
}
