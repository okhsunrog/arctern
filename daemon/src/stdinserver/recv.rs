//! Server-side recv-channel handler. One process per replication step:
//!
//!   1. Read a single `RecvHeader` from stdin (length-prefixed JSON).
//!   2. Optionally `palimpsest::recv::abort_partial` against the target
//!      when `header.send.discard_partial_recv` is set.
//!   3. Spawn `zfs recv -s -u` via palimpsest's streaming recv.
//!   4. Copy stdin bytes into the recv child's stdin until EOF.
//!   5. Wait for the recv child to exit.
//!   6. Write a single `ResponseFrame` (Ok / Error) to stdout.

use std::sync::Arc;

use arctern_config::AllowedClient;
use arctern_transport::{
    ErrorCode, RecvHeader, Response, ResponseFrame, read_header, write_response,
};
use palimpsest::recv::{RecvArgs, recv as zfs_recv};
use palimpsest::runner::CommandRunner;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Drive one recv channel from start to finish. Errors are surfaced as
/// `Response::Error` written back to the caller; the function only
/// returns `Err` on stdin/stdout I/O failures so the calling process
/// can exit with a non-zero code.
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    acl: AllowedClient,
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
    let outcome = drive(&runner, &acl, &header, &mut reader).await;
    let resp = ResponseFrame {
        request_id: None,
        body: match outcome {
            Ok(()) => Response::Ok,
            Err((code, message)) => Response::Error { code, message },
        },
    };
    if let Err(e) = write_response(&mut writer, &resp).await {
        tracing::warn!(error = %e, "recv: write final response failed");
    }
    let _ = writer.flush().await;
    Ok(())
}

async fn drive<R>(
    runner: &Arc<dyn CommandRunner>,
    acl: &AllowedClient,
    header: &RecvHeader,
    reader: &mut R,
) -> Result<(), (ErrorCode, String)>
where
    R: AsyncRead + Unpin,
{
    if let Err(msg) = enforce_root_fs(acl, &header.target_dataset) {
        return Err((ErrorCode::Unauthorized, msg));
    }
    if header.send.discard_partial_recv {
        tracing::info!(
            target = %header.target_dataset,
            "recv: discarding partial recv per sender request"
        );
        if let Err(e) =
            palimpsest::recv::abort_partial(runner.as_ref(), &header.target_dataset).await
        {
            return Err((
                ErrorCode::Zfs,
                format!("abort_partial {}: {e}", header.target_dataset),
            ));
        }
    }
    // Ensure the receive parent exists; the leaf dataset is created by
    // `zfs recv` itself.
    if let Some((parent, _)) = header.target_dataset.rsplit_once('/') {
        let opts = palimpsest::dataset::CreateOptions::new()
            .create_parents()
            .property("mountpoint", "none");
        match palimpsest::dataset::create(runner.as_ref(), parent, &opts).await {
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
    Err(format!(
        "{dataset:?} is not under allowed root_fs {root:?}"
    ))
}
