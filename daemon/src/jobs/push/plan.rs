//! The pure planner: what to send, from where, and the `zfs send`
//! argv and wire header that express it. No I/O beyond listing the
//! sender's snapshots and bookmarks.

use std::collections::BTreeSet;

use arctern_config::{SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    SendFlagsWire, SendHeader, SendKind, SnapshotEntry, SnapshotRef, compile_prefix_regex, regex,
};
use thiserror::Error;
use tracing::warn;
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::runner::CommandRunner;
use zfskit::send::SendArgs;

use crate::peer::PeerLink;

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

/// One sender-side bookmark, as listed for the fallback planner.
/// `leaf` is the part after `#`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookmarkRef {
    pub leaf: String,
    pub guid: u64,
    pub createtxg: u64,
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

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("list sender snapshots on {dataset}: {source}")]
    SenderList {
        dataset: String,
        #[source]
        source: zfskit::ZfsError,
    },
}

pub async fn list_sender_snaps(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    filter: &CompiledFilter,
) -> Result<Vec<SnapshotEntry>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts)
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    let mut snaps: Vec<SnapshotEntry> = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if !filter.matches(&snap_name) {
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
    snaps.sort_by_key(|s| s.createtxg);
    Ok(snaps)
}

fn snap_ref(s: &SnapshotEntry) -> SnapshotRef {
    SnapshotRef {
        name: s.name.clone(),
        guid: s.guid,
    }
}

pub fn pick_plan(sender: &[SnapshotEntry], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false)
}

fn pick_plan_with_discard(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    discard_partial_recv: bool,
) -> SnapshotPlan {
    let Some(latest) = sender.last() else {
        return SnapshotPlan::Nothing;
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full {
            to: snap_ref(latest),
            discard_partial_recv,
        };
    }
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let mut from: Option<&SnapshotEntry> = None;
    for s in sender.iter().rev() {
        if recv_guids.contains(&s.guid) {
            from = Some(s);
            break;
        }
    }
    match from {
        None => SnapshotPlan::Full {
            to: snap_ref(latest),
            discard_partial_recv,
        },
        Some(f) if f.guid == latest.guid => SnapshotPlan::Nothing,
        Some(f) => SnapshotPlan::Incremental {
            from: snap_ref(f),
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
pub fn apply_bookmark_fallback(
    plan: SnapshotPlan,
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    bookmarks: &[BookmarkRef],
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
        (Some(b), _) if b.guid == to.guid => SnapshotPlan::Nothing,
        (Some(b), _) => SnapshotPlan::IncrementalFromBookmark {
            from: SnapshotRef {
                name: b.leaf.clone(),
                guid: b.guid,
            },
            to,
            discard_partial_recv,
        },
        (None, _) => plan,
    }
}

/// List every bookmark of `sender_dataset` with its GUID. Unfiltered by
/// name on purpose — the fallback matches by GUID, and foreign bookmarks
/// (zrepl cursors) are exactly the migration case.
pub async fn list_sender_bookmarks(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
) -> Result<Vec<BookmarkRef>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Bookmark],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries = zfskit::dataset::list(runner, &opts)
        .await
        .map_err(|source| PlanError::SenderList {
            dataset: sender_dataset.to_string(),
            source,
        })?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            let leaf = e.name.split_once('#').map(|(_, l)| l.to_string())?;
            let guid = e
                .properties
                .get("guid")
                .and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(BookmarkRef {
                leaf,
                guid,
                createtxg,
            })
        })
        .collect())
}

pub fn pick_plan_with_token(
    sender: &[SnapshotEntry],
    receiver: &[SnapshotEntry],
    token: Option<&str>,
    decoded: Option<&zfskit::resume_token::ResumeToken>,
    sender_bookmarks: &[BookmarkRef],
) -> SnapshotPlan {
    let (Some(token), Some(decoded)) = (token, decoded) else {
        return pick_plan(sender, receiver);
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
        pick_plan_with_discard(sender, receiver, true)
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
        // snapshot base: the receiver only logs `from_snap` — the
        // stream itself carries the real base identity.
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
        SnapshotPlan::IncrementalFromBookmark { from, .. } => {
            args = args.incremental(format!("{sender_dataset}#{}", from.name));
        }
        _ => {}
    }
    Some(args)
}

/// Naming conventions pinned in ARCHITECTURE.md. Peer-namespaced so a
/// multi-target push job tracks each receiver's cursor independently:
/// peer A can be a week behind peer B and still catch up cleanly from
/// its own bookmark instead of triggering a full resend.
pub(super) async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    peer: &PeerLink,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<(SnapshotPlan, Vec<SnapshotEntry>), String> {
    let sender = list_sender_snaps(runner, sender_dataset, filter)
        .await
        .map_err(|e| format!("{e}"))?;
    if sender.is_empty() {
        return Ok((SnapshotPlan::Nothing, sender));
    }
    // Deliberately UNFILTERED: the planner intersects by GUID, and a
    // common snapshot (or bookmark-fallback base) may carry a different
    // prefix than this job's filter — zrepl_* history after a prefix
    // switch, a manual snapshot that travelled in a send stream. The
    // sender-side list stays filtered, so the filter still decides what
    // gets SENT; the receiver list only decides what counts as a
    // common base. Filtering here forced a full resend in exactly the
    // migration scenarios the bookmark fallback exists for.
    let reply = peer
        .list_receiver_guids(target_dataset.to_string(), None)
        .await
        .map_err(|e| format!("list_receiver_guids: {e}"))?;
    let (guids, token) = (reply.guids, reply.receive_resume_token);
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
                let plan = pick_plan_with_discard(&sender, &receiver, true);
                let plan =
                    maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver).await;
                return Ok((plan, sender));
            }
        },
        None => None,
    };
    // Bookmarks participate in resume validation; list them once here
    // (only when a token is in play — the common no-token path pays
    // nothing extra; the fallback path lists lazily as before).
    let bookmarks = if decoded.is_some() {
        list_sender_bookmarks(runner, sender_dataset)
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
    );
    let plan = maybe_bookmark_fallback(runner, sender_dataset, plan, &sender, &receiver).await;
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
) -> SnapshotPlan {
    if !matches!(
        plan,
        SnapshotPlan::Full { .. } | SnapshotPlan::Incremental { .. }
    ) || receiver.is_empty()
    {
        return plan;
    }
    match list_sender_bookmarks(runner, sender_dataset).await {
        Ok(bookmarks) => {
            let plan = apply_bookmark_fallback(plan, sender, receiver, &bookmarks);
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
        assert_eq!(pick_plan(&[], &[]), SnapshotPlan::Nothing);
        assert_eq!(pick_plan(&[], &[e("a", 1, 1)]), SnapshotPlan::Nothing);
    }

    #[test]
    fn empty_receiver_means_full_send_of_latest() {
        let sender = vec![s("a", 1), s("b", 2)];
        assert_eq!(
            pick_plan(&sender, &[]),
            SnapshotPlan::Full {
                to: r("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn sender_already_at_receiver_latest_means_nothing() {
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(pick_plan(&sender, &receiver), SnapshotPlan::Nothing);
    }

    #[test]
    fn sender_ahead_by_one_means_incremental() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        let receiver = vec![e("a", 1, 1), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
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
            pick_plan(&sender, &receiver),
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
        let got = pick_plan_with_token(&sender, &receiver, Some("1-abc"), Some(&decoded), &[]);
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
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks);
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
        let got = apply_bookmark_fallback(plan, &[], &receiver, &bookmarks);
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
        let got = apply_bookmark_fallback(plan.clone(), &[], &receiver, &bookmarks);
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
            apply_bookmark_fallback(full.clone(), &[], &[], &bookmarks),
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
            apply_bookmark_fallback(incr.clone(), &sender, &receiver, &bookmarks),
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
        let plan = pick_plan(&sender, &receiver);
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
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks),
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
        let plan = pick_plan(&sender, &receiver);
        assert_eq!(
            apply_bookmark_fallback(plan.clone(), &sender, &receiver, &bookmarks),
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
        let plan = pick_plan(&sender, &receiver);
        assert_eq!(plan, SnapshotPlan::Nothing);
        assert_eq!(
            apply_bookmark_fallback(plan, &sender, &receiver, &bookmarks),
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
}
