//! Push job — active sender. Each cycle, for every configured
//! filesystem: list the sender's matching snapshots, ask the receiver
//! over QUIC LIST what it already has, intersect by GUID to choose
//! Full vs Incremental, then open a SEND stream and pipe
//! `palimpsest::send::send`'s stdout into it.
//!
//! This file holds the planner (T004), executor (T005), and PushJob
//! cycle loop (T006). Splitting into submodules would cost more in
//! re-exports than it saves; the surface stays inside one file
//! together with its tests.
//!
//! T004 lands the planner only — the executor consumes `PlanError`,
//! `plan_one_filesystem`, etc. in T005, so the dead_code allowance
//! holds the bin compileable until then.

#![allow(dead_code)]

use arctern_config::SnapshotFilterConfig;
use arctern_transport::{
    Op, ProtocolError, ReceiveHeader, SnapshotEntry, SnapshotRef, compile_prefix_regex, regex,
    write_header,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use thiserror::Error;

/// What the planner decided to do for one filesystem this cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPlan {
    /// Sender has no matching snapshots, OR the latest sender snapshot
    /// is already on the receiver. No QUIC SEND stream this cycle.
    Nothing,
    /// First-replication path. Send the sender's latest matching
    /// snapshot in full.
    Full { to: SnapshotRef },
    /// Send the delta from `from` (highest-createtxg common GUID with
    /// the receiver) to `to` (sender's latest matching snapshot).
    Incremental { from: SnapshotRef, to: SnapshotRef },
}

/// Compiled snapshot filter. Owns the optional regex (so the planner
/// doesn't recompile each cycle) and the original wire string (so the
/// LIST request can carry the same regex the planner is using).
#[derive(Debug, Clone)]
pub struct CompiledFilter {
    re: Option<regex::Regex>,
    wire: Option<String>,
}

impl CompiledFilter {
    pub fn from_config(cfg: &SnapshotFilterConfig) -> Result<Self, regex::Error> {
        let wire = cfg.as_regex_str();
        let re = compile_prefix_regex(wire.as_deref())?;
        Ok(Self { re, wire })
    }

    pub fn matches(&self, snap_name: &str) -> bool {
        match &self.re {
            None => true,
            Some(r) => r.is_match(snap_name),
        }
    }

    pub fn wire_regex(&self) -> Option<&str> {
        self.wire.as_deref()
    }
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("list sender snapshots on {dataset}: {source}")]
    SenderList {
        dataset: String,
        #[source]
        source: palimpsest::ZfsError,
    },
    #[error("LIST request: {0}")]
    Wire(#[from] ProtocolError),
    #[error("LIST receiver error: {message}")]
    Receiver { message: String },
    #[error("quinn: {0}")]
    Quinn(String),
}

/// Sender-side snapshot pulled out of palimpsest into the planner's
/// internal representation. Kept private; the planner emits
/// `SnapshotRef`s on `SnapshotPlan` for the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnap {
    name: String,
    guid: u64,
    createtxg: u64,
}

/// List the sender's snapshots under `sender_dataset`, filter by
/// `filter`, sort by `createtxg` ascending. Public for testability.
pub async fn list_sender_snaps(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    filter: &CompiledFilter,
) -> Result<Vec<SnapshotRef>, PlanError> {
    let opts = ListOptions {
        recursive: false,
        types: vec![DatasetType::Snapshot],
        roots: vec![sender_dataset.to_string()],
        properties: vec!["guid".into()],
        ..ListOptions::default()
    };
    let entries =
        palimpsest::dataset::list(runner, &opts)
            .await
            .map_err(|source| PlanError::SenderList {
                dataset: sender_dataset.to_string(),
                source,
            })?;
    let mut snaps: Vec<LocalSnap> = entries
        .into_iter()
        .filter_map(|e| {
            let snap_name = e.snapshot_name.clone()?;
            if !filter.matches(&snap_name) {
                return None;
            }
            let guid = e.properties.get("guid").and_then(|p| p.value.parse::<u64>().ok())?;
            let createtxg = e.createtxg.parse::<u64>().ok()?;
            Some(LocalSnap {
                name: snap_name,
                guid,
                createtxg,
            })
        })
        .collect();
    snaps.sort_by_key(|s| s.createtxg);
    Ok(snaps
        .into_iter()
        .map(|s| SnapshotRef {
            name: s.name,
            guid: s.guid,
        })
        .collect())
}

/// Pure planner core: given sender + receiver snapshot lists (sender
/// sorted by createtxg ascending, receiver in any order), return the
/// `SnapshotPlan`. Split out so the GUID-intersection algorithm is
/// trivially testable without ZFS or QUIC.
pub fn pick_plan(sender: &[SnapshotRef], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    let Some(latest) = sender.last() else {
        return SnapshotPlan::Nothing;
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full { to: latest.clone() };
    }
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    // Walk sender from highest createtxg down; first GUID-hit is the from.
    let mut from: Option<&SnapshotRef> = None;
    for s in sender.iter().rev() {
        if recv_guids.contains(&s.guid) {
            from = Some(s);
            break;
        }
    }
    match from {
        None => SnapshotPlan::Full { to: latest.clone() },
        Some(f) if f.guid == latest.guid => SnapshotPlan::Nothing,
        Some(f) => SnapshotPlan::Incremental {
            from: f.clone(),
            to: latest.clone(),
        },
    }
}

/// Open a fresh QUIC bi stream, send a LIST request, read the
/// receiver's snapshot list. Returns the parsed receiver snapshots
/// (an empty Vec means the dataset doesn't exist yet — sink maps
/// DatasetNotFound to an empty Ok per slice 005 D16).
pub async fn fetch_receiver_snaps(
    connection: &quinn::Connection,
    target_dataset: &str,
    filter: &CompiledFilter,
) -> Result<Vec<SnapshotEntry>, PlanError> {
    use arctern_transport::{ListResponse, read_list_response};

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| PlanError::Quinn(format!("open_bi (list): {e}")))?;
    let header = ReceiveHeader {
        version: arctern_transport::PROTOCOL_VERSION,
        op: Op::List,
        target_dataset: target_dataset.to_string(),
        prefix_regex: filter.wire_regex().map(String::from),
        send: None,
    };
    write_header(&mut send, &header).await?;
    send.finish()
        .map_err(|e| PlanError::Quinn(format!("finish (list): {e}")))?;
    let resp = read_list_response(&mut recv).await?;
    match resp {
        ListResponse::Ok { snapshots } => Ok(snapshots),
        ListResponse::Error { message } => Err(PlanError::Receiver { message }),
    }
}

/// Plan one filesystem cycle: list sender, ask receiver via LIST,
/// decide via `pick_plan`. The QUIC connection is borrowed; the
/// caller manages its lifecycle (opens it once per cycle and closes
/// it after every filesystem is processed).
pub async fn plan_one_filesystem(
    runner: &dyn CommandRunner,
    sender_dataset: &str,
    target_dataset: &str,
    filter: &CompiledFilter,
    connection: &quinn::Connection,
) -> Result<SnapshotPlan, PlanError> {
    let sender = list_sender_snaps(runner, sender_dataset, filter).await?;
    if sender.is_empty() {
        return Ok(SnapshotPlan::Nothing);
    }
    let receiver = fetch_receiver_snaps(connection, target_dataset, filter).await?;
    Ok(pick_plan(&sender, &receiver))
}

// EXECUTOR + PushJob cycle loop land in T005 + T006.

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str, guid: u64) -> SnapshotRef {
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
        assert_eq!(
            pick_plan(&[], &[e("a", 1, 1)]),
            SnapshotPlan::Nothing
        );
    }

    #[test]
    fn empty_receiver_means_full_send_of_latest() {
        let sender = vec![s("a", 1), s("b", 2)];
        assert_eq!(
            pick_plan(&sender, &[]),
            SnapshotPlan::Full { to: s("b", 2) }
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
                from: s("b", 2),
                to: s("c", 3),
            }
        );
    }

    #[test]
    fn sender_ahead_by_many_picks_highest_common_guid() {
        // Sender: a, b, c, d, e  GUIDs 1..5
        // Receiver has b + d (out of order), neither is the latest
        let sender = vec![s("a", 1), s("b", 2), s("c", 3), s("d", 4), s("e", 5)];
        let receiver = vec![e("d", 4, 4), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: s("d", 4),
                to: s("e", 5),
            }
        );
    }

    #[test]
    fn disjoint_guids_means_full_send() {
        // Operator rolled back receiver by hand; new GUIDs everywhere.
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("x", 100, 1), e("y", 200, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Full { to: s("b", 2) }
        );
    }

    /// Real ZFS GUIDs from the palimpsest test VM (all > i64::MAX).
    /// The intersection map keys on u64 so these are correct.
    #[test]
    fn intersects_correctly_at_u64_above_i64_max() {
        let sender = vec![
            s("zrepl_001", 11587258101628135412),
            s("zrepl_002", 1711743136468914064),
            s("manual_001", 14719774020884296672),
        ];
        let receiver = vec![e("zrepl_001", 11587258101628135412, 8)];
        // The latest sender snap is manual_001; receiver has only zrepl_001
        // which is the OLDEST sender snap. Incremental from zrepl_001 to
        // manual_001 (the createtxg-ascending sort places manual_001 last
        // because it has the highest createtxg in the test fixture).
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: s("zrepl_001", 11587258101628135412),
                to: s("manual_001", 14719774020884296672),
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
        // The wire form escapes regex metacharacters in the prefix.
        assert_eq!(f.wire_regex(), Some("^zrepl_"));
    }

    #[test]
    fn compiled_filter_regex_passthrough() {
        let cfg = SnapshotFilterConfig {
            prefix: None,
            regex: Some("^auto_[0-9]+$".into()),
        };
        let f = CompiledFilter::from_config(&cfg).unwrap();
        assert!(f.matches("auto_42"));
        assert!(!f.matches("auto_x"));
        assert_eq!(f.wire_regex(), Some("^auto_[0-9]+$"));
    }
}
