//! The pure planner: what to send, from where, and the `zfs send`
//! argv and wire header that express it. No I/O beyond listing the
//! sender's snapshots and bookmarks.

use std::collections::BTreeSet;

use arctern_config::{ReplicateMode, SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    SendFlagsWire, SendHeader, SendKind, SnapshotEntry, SnapshotRef, compile_prefix_regex, regex,
};
use thiserror::Error;
use tracing::warn;
use zfskit::ZfsError;
use zfskit::runner::CommandRunner;
use zfskit::send::SendArgs;

pub use crate::inventory::BookmarkRef;
use crate::inventory::{list_bookmarks, list_snapshots};
use crate::peer::{PeerLink, RpcError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    Nothing,
    Full {
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    Incremental {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// `zfs send -I <from> <to>`: the same delta as `Incremental`, but
    /// the stream also carries every snapshot between the two (ZFS does
    /// not know the job's filter, so manual snapshots travel too), and
    /// `zfs recv` commits each as it arrives. `ReplicateMode::All`
    /// picks this whenever there is at least one filtered snapshot in
    /// between.
    /// A bookmark cannot be the base of `-I`, so a cursor-based step
    /// stays `IncrementalFromBookmark` to the first snapshot past the
    /// bookmark and the catch-up loop continues from there.
    IncrementalAll {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    /// `zfs send -i <dataset>#<bookmark> <dataset>@<to>` — incremental
    /// whose base is a bookmark instead of a snapshot. Picked when the
    /// receiver and sender share no common *snapshot* (the sender's
    /// copy was pruned) but a sender bookmark's GUID is still present
    /// on the receiver. This is what makes cursor bookmarks (arctern's
    /// own, or zrepl's `#zrepl_CURSOR_*` during migration) load-bearing:
    /// an offline gap longer than the sender's retention window resyncs
    /// incrementally instead of forcing a full resend.
    /// `from.name` carries the bookmark leaf (part after `#`).
    IncrementalFromBookmark {
        from: SnapshotRef,
        to: SnapshotRef,
        discard_partial_recv: bool,
    },
    Resume {
        token: String,
        decoded: zfskit::resume_token::ResumeToken,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledFilter {
    re: Option<regex::Regex>,
}

impl CompiledFilter {
    pub fn from_config(cfg: &SnapshotFilterConfig) -> Result<Self, regex::Error> {
        let re = compile_prefix_regex(cfg.as_regex_str().as_deref())?;
        Ok(Self { re })
    }

    pub fn matches(&self, snap_name: &str) -> bool {
        match &self.re {
            None => true,
            Some(r) => r.is_match(snap_name),
        }
    }
}

/// Why no plan could be made for one filesystem.
///
/// The two `Unreceivable` shapes are decided here rather than left to
/// `zfs recv`: sending anyway failed every cycle, forever, after paying
/// for a step hold and an opened stream, with whatever wording `zfs
/// recv` chose. arctern never emits `recv -F`, so a receiver that
/// already has data is never rolled back.
#[derive(Debug, Error)]
pub enum PlanError {
    #[error("list sender snapshots on {dataset}: {source}")]
    SenderList {
        dataset: String,
        #[source]
        source: ZfsError,
    },
    #[error("list receiver snapshots on {target}: {source}")]
    ReceiverList {
        target: String,
        #[source]
        source: RpcError,
    },
    /// The receiver holds snapshots but none shares a GUID with the
    /// sender, and no bookmark rescues it: the two sides have diverged.
    #[error(
        "{target} holds {receiver_snapshots} snapshot(s) but shares no snapshot or bookmark with \
         the sender, so only a full send applies and a full send cannot be received over existing \
         data. The two have diverged; reconcile by hand — destroy {target} to allow a fresh full \
         send, or restore a common base on the sender."
    )]
    Diverged {
        target: String,
        receiver_snapshots: usize,
    },
    /// The target exists without snapshots. Nothing diverged, but `zfs
    /// recv` will not lay a full stream over an existing dataset either.
    /// This is what an earlier child receive leaves behind when it
    /// created its parent as a placeholder.
    #[error(
        "{target} exists but has no snapshots, and a full send cannot be received over an \
         existing dataset. It is most likely a placeholder created for a child's receive. If \
         nothing lives under it, destroy it and the next cycle will send it in full; if received \
         children already live under it, move them aside first."
    )]
    Placeholder { target: String },
}

fn snap_ref(s: &SnapshotEntry) -> SnapshotRef {
    SnapshotRef {
        name: s.name.clone(),
        guid: s.guid,
    }
}

pub fn pick_plan(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    mode: ReplicateMode,
) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false, mode)
}

/// `sender` is sorted by createtxg. In `All` mode a first replication
/// starts from the OLDEST filtered snapshot so the history that follows
/// can be carried over too; `Latest` goes straight to the head.
fn pick_plan_with_discard(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    discard_partial_recv: bool,
    mode: ReplicateMode,
) -> SnapshotPlan {
    let (Some(oldest), Some(latest)) = (sender.first(), sender.last()) else {
        return SnapshotPlan::Nothing;
    };
    let full_target = match mode {
        ReplicateMode::All => oldest,
        ReplicateMode::Latest => latest,
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full {
            to: snap_ref(full_target),
            discard_partial_recv,
        };
    }
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let from = sender.iter().rposition(|s| recv_guids.contains(&s.guid));
    match from {
        None => SnapshotPlan::Full {
            to: snap_ref(full_target),
            discard_partial_recv,
        },
        Some(i) if sender[i].guid == latest.guid => SnapshotPlan::Nothing,
        // At least one filtered snapshot sits between the base and the
        // head: `-I` carries them all in one stream.
        Some(i) if mode == ReplicateMode::All && i + 2 < sender.len() => {
            SnapshotPlan::IncrementalAll {
                from: snap_ref(&sender[i]),
                to: snap_ref(latest),
                discard_partial_recv,
            }
        }
        Some(i) => SnapshotPlan::Incremental {
            from: snap_ref(&sender[i]),
            to: snap_ref(latest),
            discard_partial_recv,
        },
    }
}

/// Prefer a sender bookmark as the incremental base when it is a
/// STRICTLY newer common point than any common snapshot (or when there
/// is no common snapshot at all and the plan degraded to Full). Any
/// bookmark qualifies — arctern's own cursors, zrepl's
/// `#zrepl_CURSOR_*` left over from a migration, or a hand-made
/// `zfs bookmark`.
///
/// The Incremental upgrade matters: after retention prunes the
/// sender's copy of the receiver's newest snapshot, an OLDER snapshot
/// may still be common — but an incremental from it is unreceivable
/// (the receiver's head is newer than the base, so `zfs recv` refuses
/// with "destination has been modified"). The cursor bookmark IS the
/// receiver's head; sending from it is the only plan that applies.
/// Resume / Nothing plans pass through untouched.
///
/// `-I` cannot take a bookmark as its base, so in `All` mode the step
/// from a bookmark goes only to the first filtered snapshot past it; the
/// catch-up loop then continues with a snapshot base and `-I`.
pub fn apply_bookmark_fallback(
    plan: SnapshotPlan,
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    bookmarks: &[BookmarkRef],
    mode: ReplicateMode,
) -> SnapshotPlan {
    // First replication — Full is correct, not a degraded case.
    if receiver.is_empty() {
        return plan;
    }
    let (to, discard_partial_recv, base_txg) = match &plan {
        SnapshotPlan::Full {
            to,
            discard_partial_recv,
        } => (to.clone(), *discard_partial_recv, None),
        SnapshotPlan::Incremental {
            from,
            to,
            discard_partial_recv,
        }
        | SnapshotPlan::IncrementalAll {
            from,
            to,
            discard_partial_recv,
        } => {
            let txg = sender
                .iter()
                .find(|s| s.guid == from.guid)
                .map(|s| s.createtxg);
            (to.clone(), *discard_partial_recv, txg)
        }
        _ => return plan,
    };
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let best = bookmarks
        .iter()
        .filter(|b| recv_guids.contains(&b.guid))
        .max_by_key(|b| b.createtxg);
    match (best, base_txg) {
        // A common snapshot base at least as new as the bookmark wins:
        // snapshots can carry holds, bookmarks cannot.
        (Some(b), Some(txg)) if b.createtxg <= txg => plan,
        (Some(b), _) => {
            let to = match mode {
                ReplicateMode::Latest => to,
                ReplicateMode::All => match sender.iter().find(|s| s.createtxg > b.createtxg) {
                    Some(next) => snap_ref(next),
                    // Every filtered snapshot is at or below the bookmark:
                    // the receiver already has everything the sender does.
                    None => return SnapshotPlan::Nothing,
                },
            };
            if b.guid == to.guid {
                return SnapshotPlan::Nothing;
            }
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: b.leaf.clone(),
                    guid: b.guid,
                },
                to,
                discard_partial_recv,
            }
        }
        (None, _) => plan,
    }
}

pub fn pick_plan_with_token(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    token: Option<&str>,
    decoded: Option<&zfskit::resume_token::ResumeToken>,
    sender_bookmarks: &[BookmarkRef],
    mode: ReplicateMode,
) -> SnapshotPlan {
    let (Some(token), Some(decoded)) = (token, decoded) else {
        return pick_plan(sender, receiver, mode);
    };
    let sender_guids: BTreeSet<u64> = sender.iter().map(|s| s.guid).collect();
    let to_live = sender_guids.contains(&decoded.to_guid);
    // The `from` base of an interrupted send is a BOOKMARK guid
    // whenever the send was cursor-based — which is the normal daily
    // case once the retention grid has thinned the sender's copy of
    // the receiver's newest snapshot. Checking snapshots alone here
    // discarded perfectly resumable partial receives.
    let from_live = decoded
        .from_guid
        .map(|g| sender_guids.contains(&g) || sender_bookmarks.iter().any(|b| b.guid == g))
        .unwrap_or(true);
    if to_live && from_live {
        SnapshotPlan::Resume {
            token: token.to_string(),
            decoded: decoded.clone(),
        }
    } else {
        pick_plan_with_discard(sender, receiver, true, mode)
    }
}

pub fn build_send_header(plan: &SnapshotPlan, flags: &SendFlagsConfig) -> Option<SendHeader> {
    let wire_flags = SendFlagsWire {
        raw: flags.encrypted,
        embedded: flags.embedded_data,
        compressed: flags.compressed,
        large_blocks: flags.large_blocks,
    };
    let (send_kind, from_snap, to_snap, discard_partial_recv) = match plan {
        SnapshotPlan::Nothing => return None,
        SnapshotPlan::Full {
            to,
            discard_partial_recv,
        } => (SendKind::Full, None, to.clone(), *discard_partial_recv),
        SnapshotPlan::Incremental {
            from,
            to,
            discard_partial_recv,
        }
        // On the wire a bookmark base is indistinguishable from a
        // snapshot base, and a `-I` stream from a `-i` one: the receiver
        // only logs `from_snap`/`to_snap` and holds `to` — the stream
        // itself carries the real base identity and any intermediate
        // snapshots, which `zfs recv` commits on its own.
        | SnapshotPlan::IncrementalAll {
            from,
            to,
            discard_partial_recv,
        }
        | SnapshotPlan::IncrementalFromBookmark {
            from,
            to,
            discard_partial_recv,
        } => (
            SendKind::Incremental,
            Some(from.clone()),
            to.clone(),
            *discard_partial_recv,
        ),
        SnapshotPlan::Resume { decoded, .. } => (
            SendKind::Resume,
            None,
            SnapshotRef {
                // The token's toname is the full sender-side
                // `dataset@snap`; the wire carries only the leaf — the
                // receiver validates it as a snapshot leaf (no `/` or
                // `@`) and names its own `target_dataset@<leaf>` with it.
                name: decoded
                    .to_name
                    .split_once('@')
                    .map(|(_, leaf)| leaf)
                    .unwrap_or(&decoded.to_name)
                    .to_string(),
                guid: decoded.to_guid,
            },
            // Resume MUST NOT discard the partial — that IS the partial
            // we are continuing.
            false,
        ),
    };
    debug_assert!(
        !(matches!(plan, SnapshotPlan::Resume { .. }) && discard_partial_recv),
        "Resume plan must not set discard_partial_recv"
    );
    Some(SendHeader {
        send_kind,
        from_snap,
        to_snap,
        flags: wire_flags,
        discard_partial_recv,
    })
}

pub fn build_send_args(
    plan: &SnapshotPlan,
    sender_dataset: &str,
    flags: &SendFlagsConfig,
) -> Option<SendArgs> {
    if let SnapshotPlan::Resume { token, .. } = plan {
        return Some(SendArgs::resume(token));
    }
    let to_full = match plan {
        SnapshotPlan::Nothing => return None,
        SnapshotPlan::Full { to, .. }
        | SnapshotPlan::Incremental { to, .. }
        | SnapshotPlan::IncrementalAll { to, .. }
        | SnapshotPlan::IncrementalFromBookmark { to, .. } => {
            format!("{sender_dataset}@{}", to.name)
        }
        SnapshotPlan::Resume { .. } => unreachable!("handled above"),
    };
    let mut args = SendArgs::new(to_full);
    if flags.encrypted {
        args = args.raw();
    }
    if flags.embedded_data {
        args = args.embedded();
    }
    if flags.compressed {
        args = args.compressed();
    }
    if flags.large_blocks {
        args = args.large_blocks();
    }
    match plan {
        SnapshotPlan::Incremental { from, .. } => {
            args = args.incremental(format!("{sender_dataset}@{}", from.name));
        }
        SnapshotPlan::IncrementalAll { from, .. } => {
            args = args.incremental_all(format!("{sender_dataset}@{}", from.name));
        }
        SnapshotPlan::IncrementalFromBookmark { from, .. } => {
            args = args.incremental(format!("{sender_dataset}#{}", from.name));
        }
        _ => {}
    }
    Some(args)
}

/// A `Full` plan only applies to a target that does not exist yet.
/// `pick_plan` degrades to `Full` when no sender snapshot shares a GUID
/// with the receiver and no bookmark rescued it; against an existing
/// target that is a refusal, not a plan.
fn reject_unreceivable_full(
    plan: SnapshotPlan,
    target_dataset: &str,
    receiver: &[SnapshotEntry],
    target_exists: bool,
) -> Result<SnapshotPlan, PlanError> {
    if !matches!(plan, SnapshotPlan::Full { .. }) {
        return Ok(plan);
    }
    if !receiver.is_empty() {
        return Err(PlanError::Diverged {
            target: target_dataset.to_string(),
            receiver_snapshots: receiver.len(),
        });
    }
    if target_exists {
        return Err(PlanError::Placeholder {
            target: target_dataset.to_string(),
        });
    }
    Ok(plan)
}

pub(super) async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
    mode: ReplicateMode,
) -> Result<(SnapshotPlan, Vec<SnapshotEntry>), PlanError> {
    let sender = list_snapshots(runner, sender_dataset, |name| filter.matches(name))
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    if sender.is_empty() {
        return Ok((SnapshotPlan::Nothing, sender));
    }
    // Deliberately UNFILTERED: the planner intersects by GUID, and a
    // common snapshot (or bookmark-fallback base) may carry a different
    // prefix than this job's filter — zrepl_* history after a prefix
    // switch, a manual snapshot that travelled in a send stream. The
    // sender-side list stays filtered, so the filter still decides what
    // gets SENT; the receiver list only decides what counts as a
    // common base.
    let reply = peer
        .list_receiver_guids(target_dataset.to_string(), None)
        .await
        .map_err(|source| PlanError::ReceiverList {
            target: target_dataset.to_string(),
            source,
        })?;
    let (guids, token, target_exists) = (reply.guids, reply.receive_resume_token, reply.exists);
    // The planner intersects on GUID only (see pick_plan); the receiver's
    // snapshot names and createtxg are unused, so carry each GUID in an
    // otherwise-empty SnapshotEntry to keep the pure planner signature.
    let receiver: Vec<SnapshotEntry> = guids
        .into_iter()
        .map(|guid| SnapshotEntry {
            name: String::new(),
            guid,
            createtxg: 0,
        })
        .collect();
    let decoded = match token.as_deref() {
        Some(t) => match zfskit::resume_token::decode(runner, t).await {
            Ok(d) => Some(d),
            Err(e) => {
                tracing::info!(
                    target = %target_dataset,
                    error = %e,
                    "push: receiver token failed to decode, treating as stale"
                );
                let plan = pick_plan_with_discard(&sender, &receiver, true, mode);
                let plan =
                    maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver, mode)
                        .await;
                let plan =
                    reject_unreceivable_full(plan, target_dataset, &receiver, target_exists)?;
                return Ok((plan, sender));
            }
        },
        None => None,
    };
    // Bookmarks participate in resume validation; the common no-token
    // path pays nothing extra and the fallback path lists lazily.
    let bookmarks = if decoded.is_some() {
        list_bookmarks(runner, sender_dataset)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let plan = pick_plan_with_token(
        &sender,
        &receiver,
        token.as_deref(),
        decoded.as_ref(),
        &bookmarks,
        mode,
    );
    let plan =
        maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver, mode).await;
    let plan = reject_unreceivable_full(plan, target_dataset, &receiver, target_exists)?;
    Ok((plan, sender))
}

/// Wrap `apply_bookmark_fallback` with the bookmark listing, skipping
/// the extra `zfs list` entirely when the plan can't benefit. A listing
/// failure degrades to the original plan with a warning — a full resend
/// (or a refused incremental retried next cycle) is correct, just
/// expensive.
async fn maybe_bookmark_fallback(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    plan: SnapshotPlan,
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    mode: ReplicateMode,
) -> SnapshotPlan {
    if !matches!(
        plan,
        SnapshotPlan::Full { .. }
            | SnapshotPlan::Incremental { .. }
            | SnapshotPlan::IncrementalAll { .. }
    ) || receiver.is_empty()
    {
        return plan;
    }
    match list_bookmarks(runner, sender_dataset).await {
        Ok(bookmarks) => {
            let plan = apply_bookmark_fallback(plan, sender, receiver, &bookmarks, mode);
            if let SnapshotPlan::IncrementalFromBookmark { from, .. } = &plan {
                tracing::info!(
                    dataset = %sender_dataset,
                    bookmark = %from.name,
                    "push: incremental base is the cursor bookmark (sender's copy of the \
                     receiver's newest snapshot already pruned — expected between syncs)"
                );
            }
            plan
        }
        Err(e) => {
            warn!(dataset = %sender_dataset, error = %e, "push: bookmark listing failed; keeping the snapshot-based plan");
            plan
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str, guid: u64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            guid,
            createtxg: guid,
        }
    }
    fn r(name: &str, guid: u64) -> SnapshotRef {
        SnapshotRef {
            name: name.into(),
            guid,
        }
    }
    fn e(name: &str, guid: u64, createtxg: u64) -> SnapshotEntry {
        SnapshotEntry {
            name: name.into(),
            guid,
            createtxg,
        }
    }

    #[test]
    fn empty_sender_means_nothing() {
        for mode in [ReplicateMode::Latest, ReplicateMode::All] {
            assert_eq!(pick_plan(&[], &[], mode), SnapshotPlan::Nothing);
            assert_eq!(pick_plan(&[], &[e("a", 1, 1)], mode), SnapshotPlan::Nothing);
        }
    }

    #[test]
    fn empty_receiver_means_full_send_of_latest() {
        let sender = vec![s("a", 1), s("b", 2)];
        assert_eq!(
            pick_plan(&sender, &[], ReplicateMode::Latest),
            SnapshotPlan::Full {
                to: r("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    // A full send over an existing receiver is refused by `zfs recv`,
    // because arctern never emits `-F`. Emitting the plan anyway meant
    // an unreadable ZFS error every cycle, forever, after paying for a
    // step hold and an opened stream each time.
    #[test]
    fn a_full_plan_against_a_populated_receiver_is_refused_with_a_remedy() {
        let plan = SnapshotPlan::Full {
            to: r("b", 2),
            discard_partial_recv: false,
        };
        let err = reject_unreceivable_full(plan, "backup/nova/data", &[s("x", 9)], true)
            .expect_err("a full send cannot land on a populated receiver");
        assert!(
            matches!(
                &err,
                PlanError::Diverged {
                    target,
                    receiver_snapshots: 1
                } if target == "backup/nova/data"
            ),
            "got: {err:?}"
        );
        let text = err.to_string();
        assert!(text.contains("diverged"), "got: {text}");
        assert!(text.contains("destroy"), "no remedy offered: {text}");
    }

    #[test]
    fn a_full_plan_against_an_empty_receiver_is_the_normal_first_sync() {
        let plan = SnapshotPlan::Full {
            to: r("b", 2),
            discard_partial_recv: false,
        };
        assert_eq!(
            reject_unreceivable_full(plan.clone(), "backup/nova/data", &[], false).unwrap(),
            plan
        );
    }

    // A child received first creates its parent as a placeholder; the
    // parent's own first sync then finds a dataset with no snapshots,
    // which `zfs recv` refuses for a full stream — every cycle, with a
    // ZFS error that never named the placeholder as the cause.
    #[test]
    fn a_full_plan_against_an_existing_empty_dataset_names_the_placeholder() {
        let plan = SnapshotPlan::Full {
            to: r("b", 2),
            discard_partial_recv: false,
        };
        let err = reject_unreceivable_full(plan, "backup/nova/data", &[], true)
            .expect_err("a full send cannot land on an existing dataset");
        assert!(matches!(err, PlanError::Placeholder { .. }), "got: {err:?}");
        let text = err.to_string();
        assert!(text.contains("placeholder"), "got: {text}");
        assert!(text.contains("destroy"), "no remedy offered: {text}");
        assert!(!text.contains("diverged"), "nothing diverged here: {text}");
    }

    // Divergence is about the FULL plan specifically: the fallback runs
    // first, and an incremental from a bookmark lands fine on a receiver
    // that is not empty — that is the ordinary case between syncs.
    #[test]
    fn other_plans_pass_a_populated_receiver_untouched() {
        for plan in [
            SnapshotPlan::Nothing,
            SnapshotPlan::Incremental {
                from: r("a", 1),
                to: r("b", 2),
                discard_partial_recv: false,
            },
            SnapshotPlan::IncrementalFromBookmark {
                from: r("cursor", 1),
                to: r("b", 2),
                discard_partial_recv: false,
            },
        ] {
            assert_eq!(
                reject_unreceivable_full(plan.clone(), "backup/nova/data", &[s("x", 9)], true)
                    .unwrap(),
                plan
            );
        }
    }

    #[test]
    fn sender_already_at_receiver_latest_means_nothing() {
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::Latest),
            SnapshotPlan::Nothing
        );
    }

    #[test]
    fn sender_ahead_by_one_means_incremental() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::Latest),
            SnapshotPlan::Incremental {
                from: r("b", 2),
                to: r("c", 3),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn intersects_correctly_at_u64_above_i64_max() {
        let sender = vec![
            s("zrepl_001", 11587258101628135412),
            s("zrepl_002", 1711743136468914064),
            s("manual_001", 14719774020884296672),
        ];
        let receiver = vec![e("zrepl_001", 11587258101628135412, 8)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::Latest),
            SnapshotPlan::Incremental {
                from: r("zrepl_001", 11587258101628135412),
                to: r("manual_001", 14719774020884296672),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn compiled_filter_prefix_matches() {
        let cfg = SnapshotFilterConfig {
            prefix: Some("zrepl_".into()),
            regex: None,
        };
        let f = CompiledFilter::from_config(&cfg).unwrap();
        assert!(f.matches("zrepl_001"));
        assert!(!f.matches("manual_001"));
    }

    #[test]
    fn build_send_args_full_with_all_flags() {
        let plan = SnapshotPlan::Full {
            to: r("snap1", 1),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "tank/data@snap1"]);
    }

    #[test]
    fn build_send_args_resume_uses_dash_t() {
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@snap1".into(),
            to_guid: 42,
            from_guid: None,
            bytes_received: 1024,
        };
        let plan = SnapshotPlan::Resume {
            token: "1-abc".into(),
            decoded,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-t", "1-abc"]);
    }

    #[test]
    fn resume_token_with_bookmark_base_stays_resume() {
        // Interrupted cursor-based send: the token's from_guid is the
        // BOOKMARK guid, absent from the snapshot list. The plan must
        // stay Resume instead of discarding the partial receive.
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@zrepl_new".into(),
            to_guid: 42,
            from_guid: Some(777),
            bytes_received: 4096,
        };
        let sender = vec![s("zrepl_new", 42)];
        let receiver = vec![e("old", 1, 1)];
        let bookmarks = vec![BookmarkRef {
            leaf: "arctern_cursor_G_309_J_push_P_mira".into(),
            guid: 777,
            createtxg: 10,
        }];
        let got = pick_plan_with_token(
            &sender,
            &receiver,
            Some("1-abc"),
            Some(&decoded),
            &bookmarks,
            ReplicateMode::Latest,
        );
        assert!(matches!(got, SnapshotPlan::Resume { .. }), "got {got:?}");
    }

    #[test]
    fn resume_token_with_vanished_base_discards() {
        // Neither a snapshot nor a bookmark carries the token's
        // from_guid — the partial is genuinely unresumable.
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@zrepl_new".into(),
            to_guid: 42,
            from_guid: Some(777),
            bytes_received: 4096,
        };
        let sender = vec![s("zrepl_new", 42)];
        let receiver = vec![e("zrepl_new", 42, 9)];
        let got = pick_plan_with_token(
            &sender,
            &receiver,
            Some("1-abc"),
            Some(&decoded),
            &[],
            ReplicateMode::Latest,
        );
        assert!(
            !matches!(got, SnapshotPlan::Resume { .. }),
            "must not resume, got {got:?}"
        );
    }

    #[test]
    fn bookmark_fallback_downgrades_full_to_incremental() {
        // Mirrors the zrepl-migration shape: receiver's newest snapshot
        // was pruned on the sender, but the cursor bookmark survives.
        let plan = SnapshotPlan::Full {
            to: r("zrepl_new", 42),
            discard_partial_recv: false,
        };
        let receiver = vec![e("zrepl_old", 13681249742552200977, 100)];
        let bookmarks = vec![
            BookmarkRef {
                leaf: "zrepl_CURSOR_G_bddd90278c3a7711_J_push_to_local".into(),
                guid: 13681249742552200977,
                createtxg: 100,
            },
            BookmarkRef {
                leaf: "unrelated".into(),
                guid: 7,
                createtxg: 999,
            },
        ];
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks, ReplicateMode::Latest);
        assert_eq!(
            got,
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "zrepl_CURSOR_G_bddd90278c3a7711_J_push_to_local".into(),
                    guid: 13681249742552200977,
                },
                to: r("zrepl_new", 42),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn bookmark_fallback_picks_youngest_matching_base() {
        let plan = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: true,
        };
        let receiver = vec![e("a", 1, 10), e("b", 2, 20)];
        let bookmarks = vec![
            BookmarkRef {
                leaf: "old_cursor".into(),
                guid: 1,
                createtxg: 10,
            },
            BookmarkRef {
                leaf: "newer_cursor".into(),
                guid: 2,
                createtxg: 20,
            },
        ];
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks, ReplicateMode::Latest);
        let SnapshotPlan::IncrementalFromBookmark {
            from,
            discard_partial_recv,
            ..
        } = got
        else {
            panic!("expected IncrementalFromBookmark, got {got:?}");
        };
        assert_eq!(from.name, "newer_cursor");
        assert_eq!(from.guid, 2);
        // The discard directive survives the downgrade.
        assert!(discard_partial_recv);
    }

    #[test]
    fn bookmark_fallback_keeps_full_when_no_guid_matches() {
        let plan = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: false,
        };
        let receiver = vec![e("a", 1, 10)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 999,
            createtxg: 10,
        }];
        let got = apply_bookmark_fallback(
            plan.clone(),
            &[],
            &receiver,
            &bookmarks,
            ReplicateMode::Latest,
        );
        assert_eq!(got, plan);
    }

    #[test]
    fn bookmark_fallback_ignores_first_replication_and_non_full_plans() {
        // Empty receiver: Full is the correct first-replication plan.
        let full = SnapshotPlan::Full {
            to: r("new", 42),
            discard_partial_recv: false,
        };
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 42,
            createtxg: 10,
        }];
        assert_eq!(
            apply_bookmark_fallback(full.clone(), &[], &[], &bookmarks, ReplicateMode::Latest),
            full
        );
        // An Incremental whose bookmarks share no GUID with the receiver
        // passes through untouched.
        let incr = SnapshotPlan::Incremental {
            from: r("a", 1),
            to: r("b", 2),
            discard_partial_recv: false,
        };
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 10)];
        assert_eq!(
            apply_bookmark_fallback(
                incr.clone(),
                &sender,
                &receiver,
                &bookmarks,
                ReplicateMode::Latest
            ),
            incr
        );
    }

    /// The 2026-07-09 production incident: retention pruned the sender's
    /// copy of the receiver's newest snapshots, an OLDER snapshot was
    /// still common, and the planner sent an incremental from it — which
    /// the receiver refused ("destination has been modified": its head
    /// was newer than the base). The cursor bookmark carried the
    /// receiver's head GUID the whole time and must win as the base.
    #[test]
    fn bookmark_newer_than_common_snapshot_replaces_incremental_base() {
        // Sender: old common snapshot (txg 10) + brand-new one (txg 40).
        let sender = vec![s("old_common", 10), s("new", 40)];
        // Receiver also has GUID 30 — the pruned-on-sender head.
        let receiver = vec![e("", 10, 0), e("", 30, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "arctern_cursor_G_1e_J_push_P_mira".into(),
            guid: 30,
            createtxg: 30,
        }];
        let plan = pick_plan(&sender, &receiver, ReplicateMode::Latest);
        // Baseline picks the (unreceivable) old snapshot base...
        assert_eq!(
            plan,
            SnapshotPlan::Incremental {
                from: r("old_common", 10),
                to: r("new", 40),
                discard_partial_recv: false,
            }
        );
        // ...and the fallback upgrades it to the cursor bookmark.
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks, ReplicateMode::Latest),
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "arctern_cursor_G_1e_J_push_P_mira".into(),
                    guid: 30,
                },
                to: r("new", 40),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn common_snapshot_at_least_as_new_as_bookmark_keeps_snapshot_base() {
        // Snapshots can carry holds; bookmarks cannot — prefer the
        // snapshot when it is the same replication point.
        let sender = vec![s("common", 30), s("new", 40)];
        let receiver = vec![e("", 30, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor_same_point".into(),
            guid: 30,
            createtxg: 30,
        }];
        let plan = pick_plan(&sender, &receiver, ReplicateMode::Latest);
        assert_eq!(
            apply_bookmark_fallback(
                plan.clone(),
                &sender,
                &receiver,
                &bookmarks,
                ReplicateMode::Latest
            ),
            plan
        );
    }

    #[test]
    fn bookmark_of_latest_snapshot_means_nothing_to_do() {
        // The receiver already holds the sender's newest GUID; the only
        // sender-side witness of that is the bookmark. Sending an
        // incremental to the same point would be an empty stream.
        let sender = vec![s("old_common", 10), s("new", 40)];
        let receiver = vec![e("", 10, 0), e("", 40, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor_at_head".into(),
            guid: 40,
            createtxg: 40,
        }];
        let plan = pick_plan(&sender, &receiver, ReplicateMode::Latest);
        assert_eq!(plan, SnapshotPlan::Nothing);
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks, ReplicateMode::Latest),
            SnapshotPlan::Nothing
        );
    }

    #[test]
    fn build_send_args_incremental_from_bookmark_uses_hash_base() {
        let plan = SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: "zrepl_CURSOR_G_bddd_J_push".into(),
                guid: 1,
            },
            to: r("zrepl_new", 2),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(
            v,
            vec![
                "send",
                "-w",
                "-c",
                "-L",
                "-e",
                "-i",
                "tank/data#zrepl_CURSOR_G_bddd_J_push",
                "tank/data@zrepl_new"
            ]
        );
    }

    #[test]
    fn build_send_header_incremental_from_bookmark_is_wire_incremental() {
        let plan = SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: "zrepl_CURSOR_G_bddd_J_push".into(),
                guid: 1,
            },
            to: r("zrepl_new", 2),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Incremental);
        assert_eq!(
            h.from_snap.as_ref().map(|f| f.name.as_str()),
            Some("zrepl_CURSOR_G_bddd_J_push")
        );
        assert_eq!(h.to_snap.name, "zrepl_new");
    }

    #[test]
    fn build_send_header_resume_does_not_set_discard_and_uses_leaf_name() {
        let decoded = zfskit::resume_token::ResumeToken {
            token: "1-abc".into(),
            to_name: "tank/data@snap1".into(),
            to_guid: 42,
            from_guid: None,
            bytes_received: 1024,
        };
        let plan = SnapshotPlan::Resume {
            token: "1-abc".into(),
            decoded,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Resume);
        assert!(!h.discard_partial_recv);
        // The receiver validates to_snap.name as a snapshot leaf (no
        // '/' or '@') and uses it to name target_dataset@<leaf> — the
        // full sender-side toname would be rejected.
        assert_eq!(h.to_snap.name, "snap1");
        assert_eq!(h.to_snap.guid, 42);
    }

    // ── replicate = "all" ───────────────────────────────────────────

    // zrepl replicates every filtered snapshot. With snapshots between the
    // common base and the head, one `-I` stream carries them all.
    #[test]
    fn all_mode_carries_the_intermediate_snapshots() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3), s("d", 4)];
        let receiver = vec![e("a", 1, 1)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::All),
            SnapshotPlan::IncrementalAll {
                from: r("a", 1),
                to: r("d", 4),
                discard_partial_recv: false,
            }
        );
        // Latest skips them, as before.
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::Latest),
            SnapshotPlan::Incremental {
                from: r("a", 1),
                to: r("d", 4),
                discard_partial_recv: false,
            }
        );
    }

    // Nothing in between: `-I` and `-i` would send the same stream, and
    // the plain incremental keeps the argv the receiver already knows.
    #[test]
    fn all_mode_with_no_intermediate_snapshot_is_a_plain_incremental() {
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 1)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::All),
            SnapshotPlan::Incremental {
                from: r("a", 1),
                to: r("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    // A first replication in `all` mode starts from the OLDEST snapshot so
    // the history can follow; the catch-up loop then sends `-I` to the
    // head. `latest` goes straight to the head with one full stream.
    #[test]
    fn all_mode_first_sync_starts_from_the_oldest_snapshot() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        assert_eq!(
            pick_plan(&sender, &[], ReplicateMode::All),
            SnapshotPlan::Full {
                to: r("a", 1),
                discard_partial_recv: false,
            }
        );
        assert_eq!(
            pick_plan(&sender, &[], ReplicateMode::Latest),
            SnapshotPlan::Full {
                to: r("c", 3),
                discard_partial_recv: false,
            }
        );
    }

    // `-I` cannot start from a bookmark. In `all` mode the bookmark step
    // reaches only the first snapshot past the bookmark; the next step of
    // the loop has a snapshot base again and can use `-I`.
    #[test]
    fn all_mode_bookmark_fallback_stops_at_the_first_snapshot_past_it() {
        // Sender: b (txg 20) and c (txg 30) are new; the receiver's head is
        // the pruned snapshot the cursor (txg 15) still points at.
        let sender = vec![s("b", 20), s("c", 30)];
        let receiver = vec![e("", 15, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 15,
            createtxg: 15,
        }];
        let plan = pick_plan(&sender, &receiver, ReplicateMode::All);
        assert!(matches!(plan, SnapshotPlan::Full { .. }), "got {plan:?}");
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks, ReplicateMode::All),
            SnapshotPlan::IncrementalFromBookmark {
                from: SnapshotRef {
                    name: "cursor".into(),
                    guid: 15,
                },
                to: r("b", 20),
                discard_partial_recv: false,
            }
        );
        // Once `b` is common, the remaining history goes as one stream.
        let receiver = vec![e("", 15, 0), e("", 20, 0)];
        let sender = vec![s("b", 20), s("c", 30), s("d", 40)];
        assert_eq!(
            pick_plan(&sender, &receiver, ReplicateMode::All),
            SnapshotPlan::IncrementalAll {
                from: r("b", 20),
                to: r("d", 40),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn all_mode_bookmark_at_or_past_every_snapshot_means_nothing() {
        let sender = vec![s("a", 10)];
        let receiver = vec![e("", 15, 0)];
        let bookmarks = vec![BookmarkRef {
            leaf: "cursor".into(),
            guid: 15,
            createtxg: 15,
        }];
        let plan = pick_plan(&sender, &receiver, ReplicateMode::All);
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks, ReplicateMode::All),
            SnapshotPlan::Nothing
        );
    }

    #[test]
    fn build_send_args_incremental_all_uses_dash_capital_i() {
        let plan = SnapshotPlan::IncrementalAll {
            from: r("a", 1),
            to: r("d", 4),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(
            v,
            vec![
                "send",
                "-w",
                "-c",
                "-L",
                "-e",
                "-I",
                "tank/data@a",
                "tank/data@d"
            ]
        );
        // On the wire it is an incremental like any other: the receiver
        // holds `to`, and `zfs recv` commits the intermediates itself.
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Incremental);
        assert_eq!(h.from_snap.as_ref().map(|f| f.name.as_str()), Some("a"));
        assert_eq!(h.to_snap.name, "d");
    }
}
