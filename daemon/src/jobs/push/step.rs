//! Executing one planned step: open the recv channel, pipe `zfs
//! send` into it, publish progress, then place and sweep the holds.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use arctern_api::TransferInfo;
use arctern_config::SendFlagsConfig;
use arctern_transport::{PROTOCOL_VERSION, RecvHeader, Response, SnapshotEntry};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use zfskit::runner::CommandRunner;
use zfskit::send::send as zfs_send;

use super::holds::{commit_step, step_hold_tag, sweep_stale_step_holds};
use super::limiter::RateLimiter;
use super::plan::{SnapshotPlan, build_send_args, build_send_header};
use crate::peer::PeerLink;

/// Why one replication step stopped.
///
/// Cancellation is a variant, not a message: it used to travel as the
/// literal string `"cancelled"` inside `Result<_, String>` and be
/// recognised by comparing that string. Any caller that wrapped the
/// message for context — `format!("execute {path}: {e}")` — silently
/// turned an interruption into a failure, and the peer-level and
/// run-level classifiers had already drifted apart because of it.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// The cycle token fired: operator pause/stop, or daemon shutdown.
    /// The replication itself did not fail.
    #[error("cancelled")]
    Cancelled,
    #[error("{0}")]
    Failed(String),
}

impl From<String> for StepError {
    fn from(message: String) -> Self {
        StepError::Failed(message)
    }
}

impl From<&str> for StepError {
    fn from(message: &str) -> Self {
        StepError::Failed(message.to_string())
    }
}

impl From<std::io::Error> for StepError {
    fn from(e: std::io::Error) -> Self {
        StepError::Failed(e.to_string())
    }
}

/// How long to wait for a receiver's terminal Response after the bulk
/// copy failed. A refusing `zfs recv` has already exited by the time the
/// sender sees EPIPE, so the frame is normally there at once.
const RECV_REASON_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// What stays the same for every filesystem in one cycle.
///
/// `run_one_filesystem` took fourteen parameters and `execute_one_plan`
/// twelve; eight of them were these, threaded unchanged through every
/// call. Grouping them leaves each signature describing what the step is
/// actually about — which dataset, which plan, which transfer slot.
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

/// Open a recv channel for one plan, spawn `zfs send` locally, copy
/// stdout into the channel, await the receiver's terminal Response.
/// Cancellation: the `cancel` token races against the bulk copy loop;
/// on cancel we drop the recv channel (closing the SSH child's stdin)
/// and `start_kill` the local send child.
async fn execute_one_plan(
    ctx: &StepCtx<'_>,
    plan: &SnapshotPlan,
    target_dataset: &str,
    sender_dataset: &str,
    transfer_key: &str,
) -> Result<(), StepError> {
    let (runner, peer, job_name, flags, limiter, cancel, transfers) = (
        ctx.runner,
        ctx.peer,
        ctx.job_name,
        ctx.flags,
        ctx.limiter,
        ctx.cancel,
        ctx.transfers,
    );
    let Some(send_header) = build_send_header(plan, flags) else {
        return Err("build_send_header returned None for non-Nothing plan".into());
    };
    let Some(args) = build_send_args(plan, sender_dataset, flags) else {
        return Err("build_send_args returned None for non-Nothing plan".into());
    };

    let header = RecvHeader {
        version: PROTOCOL_VERSION,
        target_dataset: target_dataset.to_string(),
        send: send_header,
    };
    let mut channel = peer
        .open_recv(job_name, &header)
        .await
        .map_err(|e| format!("open_recv: {e}"))?;

    let mut child = zfs_send(runner, &args)
        .await
        .map_err(|e| format!("spawn zfs send: {e}"))?;
    let mut child_stdout = child
        .take_stdout()
        .ok_or_else(|| "no stdout on send child".to_string())?;

    let mut channel_stdin = channel
        .stdin
        .take()
        .ok_or_else(|| "no stdin on recv channel".to_string())?;
    // Manual copy loop instead of tokio::io::copy: publishes progress
    // into `transfer` for the live job-status stream and races
    // the job/cycle CancellationToken. On cancel the recv channel's
    // stdin closes (SIGPIPE to the remote zfs recv, which keeps its
    // resumable partial state) and the local send child is killed.
    let copy_res: Result<u64, StepError> = async {
        let mut buf = vec![0u8; 256 * 1024];
        let mut copied: u64 = 0;
        let mut last_published: u64 = 0;
        let mut last_publish_at = tokio::time::Instant::now();
        loop {
            // A stalled read means zfs send is still producing the next
            // record. Keep the same read future alive so no data is lost.
            let (n, read_waited) = {
                let read = child_stdout.read(&mut buf);
                tokio::pin!(read);
                let slow = sleep(StdDuration::from_secs(2));
                tokio::pin!(slow);
                let mut waiting = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(StepError::Cancelled);
                        }
                        r = &mut read => break (r?, waiting),
                        _ = &mut slow, if !waiting => {
                            waiting = true;
                            set_transfer_phase(transfers, transfer_key, "waiting_sender");
                        }
                    }
                }
            };
            if n == 0 {
                break;
            }
            if read_waited {
                set_transfer_phase(transfers, transfer_key, "sending");
            }

            // A stalled write means the SSH channel has applied
            // backpressure: network or (most commonly) receiver zfs recv /
            // storage. write_all is not cancellation-safe, so retain and
            // continue polling this exact future after publishing the phase.
            let write_waited = {
                let write = channel_stdin.write_all(&buf[..n]);
                tokio::pin!(write);
                let slow = sleep(StdDuration::from_secs(2));
                tokio::pin!(slow);
                let mut waiting = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(StepError::Cancelled);
                        }
                        r = &mut write => {
                            r?;
                            break waiting;
                        }
                        _ = &mut slow, if !waiting => {
                            waiting = true;
                            set_transfer_phase(transfers, transfer_key, "waiting_receiver");
                        }
                    }
                }
            };
            copied += n as u64;
            if write_waited {
                set_transfer_phase(transfers, transfer_key, "sending");
            }
            // Publish accepted bytes before any configured rate-limit sleep;
            // otherwise deliberate throttling looks like missing progress.
            if copied - last_published >= 8 * 1024 * 1024
                || last_publish_at.elapsed() >= StdDuration::from_millis(250)
            {
                last_published = copied;
                last_publish_at = tokio::time::Instant::now();
                if let Some(t) = transfers.lock().unwrap().get_mut(transfer_key) {
                    t.bytes_sent = copied;
                }
            }
            // Chunk-grained (256 KiB) throttling, plenty smooth at
            // network scales. The bucket is shared job-wide so
            // parallel sends stay under the aggregate limit.
            if let Some(l) = limiter {
                l.throttle(n as u64).await;
            }
        }
        if let Some(t) = transfers.lock().unwrap().get_mut(transfer_key) {
            t.bytes_sent = copied;
        }
        Ok(copied)
    }
    .await;
    if let Err(error) = copy_res {
        close_stream_writer(channel_stdin).await;
        let _ = child.cancel().await;
        // Cancellation arrives as its own variant now, so this no longer
        // has to re-read the token and guess whether an I/O error was
        // really an interruption.
        if matches!(error, StepError::Cancelled) {
            // EOF only asks the remote zfs recv to stop. Keep the channel
            // alive until it has actually exited so a retry cannot race the
            // old receiver for the same dataset.
            set_transfer_phase(transfers, transfer_key, "cancelling");
            let _ = channel.finish().await;
            return Err(StepError::Cancelled);
        }
        // A receiver that refuses the stream exits, and the copy dies with
        // EPIPE once the channel's buffers are full. The reason it refused
        // is in the Response frame it wrote before exiting — dropping the
        // channel here threw that away and reported "Broken pipe" for any
        // stream too large to fit in the buffers, which is every real one.
        // Bounded: if the remote is still alive the pipe broke for some
        // other reason and there may never be a frame to read.
        let reason = tokio::time::timeout(RECV_REASON_TIMEOUT, channel.finish()).await;
        return Err(StepError::Failed(match reason {
            Ok(Ok(Response::Error { message, .. })) => format!("receiver: {message}"),
            _ => format!("stream copy: {error}"),
        }));
    }
    set_transfer_phase(transfers, transfer_key, "finalizing");
    close_stream_writer(channel_stdin).await;
    child
        .finish()
        .await
        .map_err(|e| StepError::Failed(format!("zfs send failed: {e}")))?;
    let resp = channel
        .finish()
        .await
        .map_err(|e| StepError::Failed(format!("read recv response: {e}")))?;
    match resp {
        Response::Ok => Ok(()),
        Response::Error { message, .. } => Err(StepError::Failed(format!("receiver: {message}"))),
    }
}

/// Close an SSH stream writer and consume its handle before the caller waits
/// for the remote process. `shutdown()` alone does not deliver EOF while the
/// final writer handle remains alive.
async fn close_stream_writer<W: tokio::io::AsyncWrite + Unpin>(mut writer: W) {
    let _ = writer.shutdown().await;
}

fn set_transfer_phase(
    transfers: &Mutex<HashMap<String, TransferInfo>>,
    transfer_key: &str,
    phase: &str,
) {
    if let Some(transfer) = transfers.lock().unwrap().get_mut(transfer_key)
        && transfer.phase != phase
    {
        transfer.phase = phase.to_string();
        transfer.phase_since = OffsetDateTime::now_utc().unix_timestamp();
    }
}

/// Step hold + cursor bookmark choreography around a successful execute.
/// Holds are placed BEFORE the send so a concurrent prune cannot kill
/// the snapshot mid-stream. On success the step-hold tag is swept from
/// every filtered snapshot (current `to` plus stale holds from earlier
/// failed cycles); on failure holds stay so a retry can find the
/// snapshot. The cursor bookmark is GUID-named: the new one is created
/// first, stale same-(job, peer) cursors destroyed after — crash-safe.
pub(super) async fn run_one_filesystem(
    ctx: &StepCtx<'_>,
    sender_dataset: &str,
    target_dataset: &str,
    plan: &SnapshotPlan,
    sender_snaps: &[SnapshotEntry],
    transfer_key: &str,
) -> Result<(), StepError> {
    let (runner, job_name, peer_name, transfers) =
        (ctx.runner, ctx.job_name, ctx.peer_name, ctx.transfers);
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
    // Bookmark bases can't be held — snapshot prune can't destroy a
    // bookmark, so they're safe without one. The intermediate snapshots
    // of a `-I` stream need no hold either: `zfs send` keeps them busy
    // for as long as it runs, and prune treats busy as skip.
    let from_hold_target: Option<String> = match plan {
        SnapshotPlan::Incremental { from, .. } | SnapshotPlan::IncrementalAll { from, .. } => {
            Some(format!("{sender_dataset}@{}", from.name))
        }
        _ => None,
    };
    let tag = step_hold_tag(job_name, peer_name);
    let mut held: Vec<&str> = Vec::new();
    for snap in from_hold_target
        .iter()
        .chain(to_hold_target.iter().map(|(s, _)| s))
    {
        // hold is idempotent at the zfskit layer (no-op when the
        // tag already exists for that snapshot).
        if let Err(e) = zfskit::hold::hold(runner, snap, &tag).await {
            return Err(StepError::Failed(format!(
                "step hold failed for {snap} with tag {tag}: {e}"
            )));
        }
        held.push(snap.as_str());
    }
    // Holds this step needs are in place, so anything else still tagged
    // is a leftover from an earlier failed cycle. Releasing it here
    // bounds the tag at the two snapshots a step actually protects.
    sweep_stale_step_holds(runner, sender_dataset, sender_snaps, &held, &tag).await;

    // Leave the step hold in place on failure — it protects the snapshot
    // for the next cycle's retry. Hence `?` propagates without a release.
    execute_one_plan(ctx, plan, target_dataset, sender_dataset, transfer_key).await?;

    if let Some((snap, guid)) = &to_hold_target {
        set_transfer_phase(transfers, transfer_key, "committing");
        commit_step(
            runner,
            sender_dataset,
            job_name,
            peer_name,
            sender_snaps,
            snap,
            *guid,
            &tag,
        )
        .await;
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
