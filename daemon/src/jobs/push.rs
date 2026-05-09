//! Push job — active sender. The executor is being rewritten on top of
//! the SSH-based PeerLink (ARCHITECTURE.md, step 9). For now this file
//! holds only the pure planner + header/args builders so the daemon
//! still compiles and the planner unit tests stay green.

// The planner + builders below are wired back into the executor in
// step 9; until then they exist purely for the unit tests in this
// module.
#![allow(dead_code)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration as StdDuration;

use arctern_config::PushJobConfig;
use time::OffsetDateTime;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use super::{Job, JobContext, JobStatusInner};

use arctern_config::{SendFlagsConfig, SnapshotFilterConfig};
use arctern_transport::{
    ProtocolError, SendFlagsWire, SendHeader, SendKind, SnapshotEntry, SnapshotRef,
    compile_prefix_regex, regex,
};
use palimpsest::dataset::ListOptions;
use palimpsest::models::DatasetType;
use palimpsest::runner::CommandRunner;
use palimpsest::send::SendArgs;
use thiserror::Error;

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
    Resume {
        token: String,
        decoded: palimpsest::resume_token::ResumeToken,
    },
}

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
    #[error("wire: {0}")]
    Wire(#[from] ProtocolError),
    #[error("LIST receiver error: {message}")]
    Receiver { message: String },
    #[error("decode resume token: {0}")]
    ResumeTokenDecode(#[from] palimpsest::resume_token::ResumeTokenError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnap {
    name: String,
    guid: u64,
    createtxg: u64,
}

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

pub fn pick_plan(sender: &[SnapshotRef], receiver: &[SnapshotEntry]) -> SnapshotPlan {
    pick_plan_with_discard(sender, receiver, false)
}

fn pick_plan_with_discard(
    sender: &[SnapshotRef],
    receiver: &[SnapshotEntry],
    discard_partial_recv: bool,
) -> SnapshotPlan {
    let Some(latest) = sender.last() else {
        return SnapshotPlan::Nothing;
    };
    if receiver.is_empty() {
        return SnapshotPlan::Full {
            to: latest.clone(),
            discard_partial_recv,
        };
    }
    use std::collections::BTreeSet;
    let recv_guids: BTreeSet<u64> = receiver.iter().map(|s| s.guid).collect();
    let mut from: Option<&SnapshotRef> = None;
    for s in sender.iter().rev() {
        if recv_guids.contains(&s.guid) {
            from = Some(s);
            break;
        }
    }
    match from {
        None => SnapshotPlan::Full {
            to: latest.clone(),
            discard_partial_recv,
        },
        Some(f) if f.guid == latest.guid => SnapshotPlan::Nothing,
        Some(f) => SnapshotPlan::Incremental {
            from: f.clone(),
            to: latest.clone(),
            discard_partial_recv,
        },
    }
}

pub fn pick_plan_with_token(
    sender: &[SnapshotRef],
    receiver: &[SnapshotEntry],
    token: Option<&str>,
    decoded: Option<&palimpsest::resume_token::ResumeToken>,
) -> SnapshotPlan {
    let (Some(token), Some(decoded)) = (token, decoded) else {
        return pick_plan(sender, receiver);
    };
    use std::collections::BTreeSet;
    let sender_guids: BTreeSet<u64> = sender.iter().map(|s| s.guid).collect();
    let to_live = sender_guids.contains(&decoded.to_guid);
    let from_live = decoded
        .from_guid
        .map(|g| sender_guids.contains(&g))
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
                name: decoded.to_name.clone(),
                guid: decoded.to_guid,
            },
            // Resume MUST NOT discard the partial — that IS the
            // partial we are continuing.
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
        let mut args = SendArgs::new("ignored").resume_token(token);
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
        return Some(args);
    }
    let to_full = match plan {
        SnapshotPlan::Nothing => return None,
        SnapshotPlan::Full { to, .. } => format!("{sender_dataset}@{}", to.name),
        SnapshotPlan::Incremental { to, .. } => format!("{sender_dataset}@{}", to.name),
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
    if let SnapshotPlan::Incremental { from, .. } = plan {
        args = args.incremental(format!("{sender_dataset}@{}", from.name));
    }
    Some(args)
}

pub const KIND: &str = arctern_api::JOB_KIND_PUSH;

pub struct PushJob {
    config: PushJobConfig,
    #[allow(dead_code)]
    filter: CompiledFilter,
    status: Mutex<JobStatusInner>,
    wakeup: Arc<tokio::sync::Notify>,
}

impl PushJob {
    pub fn new(config: PushJobConfig) -> Result<Self, regex::Error> {
        let filter = CompiledFilter::from_config(&config.snapshot_filter)?;
        Ok(Self {
            config,
            filter,
            status: Mutex::new(JobStatusInner::default()),
            wakeup: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn record_cycle(&self, last_error: Option<String>, interval: StdDuration) {
        let mut s = self.status.lock().unwrap();
        let now = OffsetDateTime::now_utc();
        s.last_run = Some(now);
        s.next_run = Some(now + time::Duration::try_from(interval).unwrap_or(time::Duration::ZERO));
        s.last_error = last_error;
    }
}

impl Job for PushJob {
    fn name(&self) -> &str {
        &self.config.name
    }
    fn kind(&self) -> &'static str {
        KIND
    }
    fn status(&self) -> JobStatusInner {
        self.status.lock().unwrap().clone()
    }
    fn wakeup(&self) {
        self.wakeup.notify_one();
    }
    fn run(
        self: Arc<Self>,
        _ctx: JobContext,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let span = info_span!("push_job", name = %self.config.name);
        Box::pin(
            async move {
                let interval = self.config.interval;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = sleep(interval) => {}
                        _ = self.wakeup.notified() => {}
                    }
                    // Executor is being rebuilt on top of PeerLink in
                    // ARCHITECTURE.md step 9. Until then the cycle is
                    // a no-op so the daemon keeps booting cleanly.
                    self.record_cycle(
                        Some("push executor not yet wired to SSH transport".into()),
                        interval,
                    );
                }
            }
            .instrument(span),
        )
    }
}

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
        assert_eq!(pick_plan(&[], &[e("a", 1, 1)]), SnapshotPlan::Nothing);
    }

    #[test]
    fn empty_receiver_means_full_send_of_latest() {
        let sender = vec![s("a", 1), s("b", 2)];
        assert_eq!(
            pick_plan(&sender, &[]),
            SnapshotPlan::Full {
                to: s("b", 2),
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
                from: s("b", 2),
                to: s("c", 3),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn sender_ahead_by_many_picks_highest_common_guid() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3), s("d", 4), s("e", 5)];
        let receiver = vec![e("d", 4, 4), e("b", 2, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Incremental {
                from: s("d", 4),
                to: s("e", 5),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn disjoint_guids_means_full_send() {
        let sender = vec![s("a", 1), s("b", 2)];
        let receiver = vec![e("x", 100, 1), e("y", 200, 2)];
        assert_eq!(
            pick_plan(&sender, &receiver),
            SnapshotPlan::Full {
                to: s("b", 2),
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
                from: s("zrepl_001", 11587258101628135412),
                to: s("manual_001", 14719774020884296672),
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
        assert_eq!(f.wire_regex(), Some("^zrepl_"));
    }

    #[test]
    fn build_send_header_full_uses_all_default_flags() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 42),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Full);
        assert!(h.from_snap.is_none());
        assert_eq!(h.to_snap.guid, 42);
        assert!(!h.discard_partial_recv);
        assert_eq!(
            h.flags,
            SendFlagsWire {
                raw: true,
                embedded: true,
                compressed: true,
                large_blocks: true
            }
        );
    }

    #[test]
    fn build_send_header_incremental_carries_from_and_to() {
        let plan = SnapshotPlan::Incremental {
            from: s("snap1", 1),
            to: s("snap2", 2),
            discard_partial_recv: false,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert_eq!(h.send_kind, SendKind::Incremental);
        assert_eq!(h.from_snap.unwrap().guid, 1);
        assert_eq!(h.to_snap.guid, 2);
        assert!(!h.discard_partial_recv);
    }

    #[test]
    fn build_send_header_nothing_yields_none() {
        assert!(build_send_header(&SnapshotPlan::Nothing, &SendFlagsConfig::default()).is_none());
    }

    #[test]
    fn build_send_args_full_with_all_flags() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 1),
            discard_partial_recv: false,
        };
        let args = build_send_args(&plan, "tank/data", &SendFlagsConfig::default()).unwrap();
        let v = args.build_args(false).unwrap();
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "tank/data@snap1"]);
    }

    #[test]
    fn build_send_args_incremental_uses_dash_i() {
        let plan = SnapshotPlan::Incremental {
            from: s("snap1", 1),
            to: s("snap2", 2),
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
                "tank/data@snap1",
                "tank/data@snap2"
            ]
        );
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

    fn rt(to_guid: u64, from_guid: Option<u64>) -> palimpsest::resume_token::ResumeToken {
        palimpsest::resume_token::ResumeToken {
            token: "1-deadbeef".into(),
            to_name: "tank/data@snap_x".into(),
            to_guid,
            from_guid,
            bytes_received: 1024,
        }
    }

    #[test]
    fn pick_plan_with_token_none_falls_through_to_full() {
        let sender = vec![s("a", 1), s("b", 2)];
        let p = pick_plan_with_token(&sender, &[], None, None);
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: false,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_live_full_emits_resume() {
        let sender = vec![s("a", 1), s("b", 11587258101628135412)];
        let decoded = rt(11587258101628135412, None);
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        match p {
            SnapshotPlan::Resume { token, decoded: d } => {
                assert_eq!(token, "1-deadbeef");
                assert_eq!(d.to_guid, 11587258101628135412);
            }
            other => panic!("expected Resume, got {other:?}"),
        }
    }

    #[test]
    fn pick_plan_with_token_live_incremental_emits_resume() {
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(2, Some(1));
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert!(matches!(p, SnapshotPlan::Resume { .. }));
    }

    #[test]
    fn pick_plan_with_token_to_guid_dead_emits_full_with_discard() {
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(99999, None);
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_from_guid_dead_emits_with_discard() {
        let sender = vec![s("a", 1), s("b", 2)];
        let decoded = rt(2, Some(99999));
        let p = pick_plan_with_token(&sender, &[], Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Full {
                to: s("b", 2),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn pick_plan_with_token_dead_but_common_snap_exists_emits_incremental_with_discard() {
        let sender = vec![s("a", 1), s("b", 2), s("c", 3)];
        let receiver = vec![e("b", 2, 5)];
        let decoded = rt(99999, None);
        let p = pick_plan_with_token(&sender, &receiver, Some("1-deadbeef"), Some(&decoded));
        assert_eq!(
            p,
            SnapshotPlan::Incremental {
                from: s("b", 2),
                to: s("c", 3),
                discard_partial_recv: true,
            }
        );
    }

    #[test]
    fn build_send_header_full_with_discard_sets_flag() {
        let plan = SnapshotPlan::Full {
            to: s("snap1", 42),
            discard_partial_recv: true,
        };
        let h = build_send_header(&plan, &SendFlagsConfig::default()).unwrap();
        assert!(h.discard_partial_recv);
        assert_eq!(h.send_kind, SendKind::Full);
    }

    #[test]
    fn build_send_header_resume_uses_decoded_to_snap() {
        let decoded = palimpsest::resume_token::ResumeToken {
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
        assert!(h.from_snap.is_none());
        assert_eq!(h.to_snap.name, "tank/data@snap1");
        assert_eq!(h.to_snap.guid, 42);
        assert!(!h.discard_partial_recv);
    }

    #[test]
    fn build_send_args_resume_uses_dash_t() {
        let decoded = palimpsest::resume_token::ResumeToken {
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
        assert_eq!(v, vec!["send", "-w", "-c", "-L", "-e", "-t", "1-abc"]);
    }
}
