//! Executing one planned step: open the recv channel, pipe `zfs
//! send` into it, publish progress, then place and sweep the holds.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use arctern_api::{TransferInfo, TransferPhase};
use arctern_config::SendFlagsConfig;
use arctern_transport::{ErrorCode, PROTOCOL_VERSION, RecvHeader, Response, SnapshotEntry};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use zfskit::ZfsError;
use zfskit::runner::CommandRunner;
use zfskit::send::send as zfs_send;

use super::holds::HoldScope;
use super::limiter::RateLimiter;
use super::plan::{SnapshotPlan, build_send_args, build_send_header};
use crate::peer::{PeerError, PeerLink};

/// Why one replication step stopped. Cancellation is a variant so no
/// caller can mistake an interruption for a failure by wrapping it.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// The cycle token fired: operator pause/stop, or daemon shutdown.
    #[error("cancelled")]
    Cancelled,
    #[error("step hold on {snapshot} with tag {tag}: {source}")]
    Hold {
        snapshot: String,
        tag: String,
        #[source]
        source: ZfsError,
    },
    #[error("open recv channel: {0}")]
    OpenRecv(#[source] PeerError),
    #[error("spawn zfs send: {0}")]
    SpawnSend(#[source] ZfsError),
    #[error("zfs send failed: {0}")]
    SendFailed(#[source] ZfsError),
    #[error("stream copy: {0}")]
    StreamCopy(#[source] std::io::Error),
    /// The receiver refused or failed the stream and said why.
    #[error("receiver: {message}")]
    Receiver { code: ErrorCode, message: String },
    #[error("read recv response: {0}")]
    ReadResponse(#[source] PeerError),
    #[error("{0}")]
    Internal(&'static str),
}

/// How long to wait for a receiver's terminal Response after the bulk
/// copy failed. A refusing `zfs recv` has already exited by the time the
/// sender sees EPIPE, so the frame is normally there at once.
const RECV_REASON_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// After this long without progress the transfer reports which side it
/// is waiting for.
const STALL_AFTER: StdDuration = StdDuration::from_secs(2);

/// What stays the same for every filesystem in one cycle.
pub(super) struct StepCtx<'a> {
    pub(super) runner: &'a dyn CommandRunner,
    pub(super) peer: &'a PeerLink,
    pub(super) job_name: &'a str,
    pub(super) peer_name: &'a str,
    pub(super) flags: &'a SendFlagsConfig,
    pub(super) limiter: Option<&'a RateLimiter>,
    pub(super) cancel: &'a CancellationToken,
    pub(super) transfers: &'a Mutex<HashMap<String, TransferInfo>>,
}

impl StepCtx<'_> {
    fn set_phase(&self, transfer_key: &str, phase: TransferPhase) {
        if let Some(transfer) = self.transfers.lock().unwrap().get_mut(transfer_key)
            && transfer.phase != phase
        {
            transfer.phase = phase;
            transfer.phase_since = OffsetDateTime::now_utc().unix_timestamp();
        }
    }

    fn set_bytes(&self, transfer_key: &str, bytes: u64) {
        if let Some(t) = self.transfers.lock().unwrap().get_mut(transfer_key) {
            t.bytes_sent = bytes;
        }
    }

    /// Drive one I/O future to completion, racing the cancel token.
    /// When it has not completed after `STALL_AFTER` the transfer's
    /// phase switches to `stalled` (and back to `sending` once it
    /// does), so the console can say which side is slow. The future is
    /// polled to completion, never dropped mid-way: `write_all` is not
    /// cancellation-safe, and dropping a pending read loses bytes.
    async fn io_with_stall<T>(
        &self,
        transfer_key: &str,
        stalled: TransferPhase,
        io: impl Future<Output = std::io::Result<T>>,
    ) -> Result<T, StepError> {
        tokio::pin!(io);
        let slow = sleep(STALL_AFTER);
        tokio::pin!(slow);
        let mut waiting = false;
        let value = loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(StepError::Cancelled),
                r = &mut io => break r.map_err(StepError::StreamCopy)?,
                _ = &mut slow, if !waiting => {
                    waiting = true;
                    self.set_phase(transfer_key, stalled);
                }
            }
        };
        if waiting {
            self.set_phase(transfer_key, TransferPhase::Sending);
        }
        Ok(value)
    }
}

/// Open a recv channel for one plan, spawn `zfs send` locally, copy
/// stdout into the channel, await the receiver's terminal Response.
/// On cancel the recv channel's stdin closes (SIGPIPE to the remote
/// `zfs recv`, which keeps its resumable partial state) and the local
/// send child is killed.
async fn execute_one_plan(
    ctx: &StepCtx<'_>,
    plan: &SnapshotPlan,
    target_dataset: &str,
    sender_dataset: &str,
    transfer_key: &str,
) -> Result<(), StepError> {
    let send_header = build_send_header(plan, ctx.flags)
        .ok_or(StepError::Internal("no send header for a Nothing plan"))?;
    let args = build_send_args(plan, sender_dataset, ctx.flags)
        .ok_or(StepError::Internal("no send args for a Nothing plan"))?;

    let header = RecvHeader {
        version: PROTOCOL_VERSION,
        target_dataset: target_dataset.to_string(),
        send: send_header,
    };
    let mut channel = ctx
        .peer
        .open_recv(ctx.job_name, &header)
        .await
        .map_err(StepError::OpenRecv)?;

    let mut child = zfs_send(ctx.runner, &args)
        .await
        .map_err(StepError::SpawnSend)?;
    let mut child_stdout = child
        .take_stdout()
        .ok_or(StepError::Internal("no stdout on send child"))?;
    let mut channel_stdin = channel
        .stdin
        .take()
        .ok_or(StepError::Internal("no stdin on recv channel"))?;

    // A manual copy loop rather than tokio::io::copy: it publishes
    // progress, races the cancel token, and throttles per chunk.
    let copy_res: Result<u64, StepError> = async {
        let mut buf = vec![0u8; 256 * 1024];
        let mut copied: u64 = 0;
        let mut last_published: u64 = 0;
        let mut last_publish_at = tokio::time::Instant::now();
        loop {
            let n = ctx
                .io_with_stall(
                    transfer_key,
                    TransferPhase::WaitingSender,
                    child_stdout.read(&mut buf),
                )
                .await?;
            if n == 0 {
                break;
            }
            ctx.io_with_stall(
                transfer_key,
                TransferPhase::WaitingReceiver,
                channel_stdin.write_all(&buf[..n]),
            )
            .await?;
            copied += n as u64;
            // Publish accepted bytes before any rate-limit sleep, or
            // deliberate throttling looks like missing progress.
            if copied - last_published >= 8 * 1024 * 1024
                || last_publish_at.elapsed() >= StdDuration::from_millis(250)
            {
                last_published = copied;
                last_publish_at = tokio::time::Instant::now();
                ctx.set_bytes(transfer_key, copied);
            }
            if let Some(l) = ctx.limiter {
                l.throttle(n as u64).await;
            }
        }
        ctx.set_bytes(transfer_key, copied);
        Ok(copied)
    }
    .await;
    if let Err(error) = copy_res {
        close_stream_writer(channel_stdin).await;
        let _ = child.cancel().await;
        if matches!(error, StepError::Cancelled) {
            // EOF only asks the remote zfs recv to stop. Keep the channel
            // alive until it has actually exited so a retry cannot race
            // the old receiver for the same dataset.
            ctx.set_phase(transfer_key, TransferPhase::Cancelling);
            let _ = channel.finish().await;
            return Err(StepError::Cancelled);
        }
        // A receiver that refuses the stream exits, and the copy dies
        // with EPIPE once the channel's buffers are full. The reason is
        // in the Response frame it wrote before exiting. Bounded: if the
        // remote is still alive the pipe broke for some other reason and
        // there may never be a frame to read.
        let reason = tokio::time::timeout(RECV_REASON_TIMEOUT, channel.finish()).await;
        return Err(match reason {
            Ok(Ok(Response::Error { code, message })) => StepError::Receiver { code, message },
            _ => error,
        });
    }
    ctx.set_phase(transfer_key, TransferPhase::Finalizing);
    close_stream_writer(channel_stdin).await;
    child.finish().await.map_err(StepError::SendFailed)?;
    match channel.finish().await.map_err(StepError::ReadResponse)? {
        Response::Ok => Ok(()),
        Response::Error { code, message } => Err(StepError::Receiver { code, message }),
    }
}

/// Close an SSH stream writer and consume its handle before the caller waits
/// for the remote process. `shutdown()` alone does not deliver EOF while the
/// final writer handle remains alive.
async fn close_stream_writer<W: tokio::io::AsyncWrite + Unpin>(mut writer: W) {
    let _ = writer.shutdown().await;
}

/// Step hold + cursor bookmark choreography around a successful execute.
/// Holds are placed BEFORE the send so a concurrent prune cannot kill
/// the snapshot mid-stream. On success the step-hold tag is swept from
/// every filtered snapshot; on failure holds stay so a retry can find
/// the snapshot.
pub(super) async fn run_one_filesystem(
    ctx: &StepCtx<'_>,
    sender_dataset: &str,
    target_dataset: &str,
    plan: &SnapshotPlan,
    sender_snaps: &[SnapshotEntry],
    transfer_key: &str,
) -> Result<(), StepError> {
    let to_hold_target: Option<(String, u64)> = match plan {
        SnapshotPlan::Full { to, .. }
        | SnapshotPlan::Incremental { to, .. }
        | SnapshotPlan::IncrementalAll { to, .. }
        | SnapshotPlan::IncrementalFromBookmark { to, .. } => {
            Some((format!("{sender_dataset}@{}", to.name), to.guid))
        }
        // The token's toname is the full sender-side dataset@snap.
        SnapshotPlan::Resume { decoded, .. } => Some((decoded.to_name.clone(), decoded.to_guid)),
        SnapshotPlan::Nothing => None,
    };
    // The `from` base needs the same protection for the duration of the
    // step (zrepl holds both ends): losing it mid-send or between a
    // failed step and its retry breaks incrementality / resumability.
    // Bookmark bases can't be held and don't need to be: snapshot prune
    // can't destroy a bookmark. A resume's base is whichever sender
    // snapshot still carries the token's from GUID; when only a bookmark
    // carries it there is nothing to hold. The intermediates of a `-I`
    // stream need no hold either: `zfs send` keeps them busy for as long
    // as it runs, and prune treats busy as skip.
    let from_hold_target: Option<String> = match plan {
        SnapshotPlan::Incremental { from, .. } | SnapshotPlan::IncrementalAll { from, .. } => {
            Some(format!("{sender_dataset}@{}", from.name))
        }
        SnapshotPlan::Resume { decoded, .. } => decoded.from_guid.and_then(|guid| {
            sender_snaps
                .iter()
                .find(|s| s.guid == guid)
                .map(|s| format!("{sender_dataset}@{}", s.name))
        }),
        _ => None,
    };
    let holds = HoldScope {
        runner: ctx.runner,
        dataset: sender_dataset,
        job_name: ctx.job_name,
        peer_name: ctx.peer_name,
    };
    let mut held: Vec<&str> = Vec::new();
    for snap in from_hold_target
        .iter()
        .chain(to_hold_target.iter().map(|(s, _)| s))
    {
        holds.place(snap).await.map_err(|source| StepError::Hold {
            snapshot: snap.clone(),
            tag: holds.tag(),
            source,
        })?;
        held.push(snap.as_str());
    }
    holds.sweep_stale(sender_snaps, &held).await;

    // `?` propagates without a release: the step hold protects the
    // snapshot for the next cycle's retry.
    execute_one_plan(ctx, plan, target_dataset, sender_dataset, transfer_key).await?;

    if let Some((snap, guid)) = &to_hold_target {
        ctx.set_phase(transfer_key, TransferPhase::Committing);
        holds.commit(sender_snaps, snap, *guid).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TrackedWriter {
        shutdown: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl tokio::io::AsyncWrite for TrackedWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            self.shutdown.store(true, Ordering::Relaxed);
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl Drop for TrackedWriter {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn close_stream_writer_shuts_down_and_drops_handle() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        close_stream_writer(TrackedWriter {
            shutdown: shutdown.clone(),
            dropped: dropped.clone(),
        })
        .await;
        assert!(shutdown.load(Ordering::Relaxed));
        assert!(dropped.load(Ordering::Relaxed));
    }
}
