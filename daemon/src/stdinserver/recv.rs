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
//!      `recv_transfers` and emit a structured completion event.
//!   8. Write a single `ResponseFrame` (Ok / Error) to stdout.

use std::sync::Arc;

use arctern_config::AllowedClient;
use arctern_config::zfs_names::{validate_dataset_name, validate_snapshot_leaf};
use arctern_transport::{
    ErrorCode, ProtocolError, RecvHeader, Response, ResponseFrame, read_header, write_response,
};
use sqlx::SqlitePool;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use zfskit::ZfsError;
use zfskit::dataset::ListOptions;
use zfskit::models::DatasetType;
use zfskit::recv::{RecvArgs, recv as zfs_recv};
use zfskit::runner::CommandRunner;

use super::acl::{AclError, check_operation, check_root_fs};
use super::recv_lock::{RecvLockError, RecvLocks};

/// Properties that decide WHERE and WHETHER a received dataset mounts.
/// Mount policy is the receiving host's to set, never the sending
/// host's, so these are inherited from the receive parent (which the
/// dispatcher creates with `mountpoint=none`) instead of being taken
/// from the stream.
const MOUNT_POLICY_PROPERTIES: [&str; 2] = ["mountpoint", "canmount"];

/// Why a receive was refused or failed, with the wire code the sender
/// gets to see.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error("read RecvHeader: {0}")]
    Header(#[source] ProtocolError),
    #[error("invalid target_dataset {name:?}: {reason}")]
    InvalidTarget { name: String, reason: String },
    #[error("invalid {field} snapshot name {name:?}: {reason}")]
    InvalidSnapshot {
        field: &'static str,
        name: String,
        reason: String,
    },
    #[error(transparent)]
    Acl(#[from] AclError),
    #[error(transparent)]
    Lock(#[from] RecvLockError),
    #[error("abort_partial {dataset}: {source}")]
    AbortPartial {
        dataset: String,
        #[source]
        source: ZfsError,
    },
    #[error("probe ancestor {name}: {source}")]
    ProbeAncestor {
        name: String,
        #[source]
        source: ZfsError,
    },
    #[error("create ancestor {name}: {source}")]
    CreateAncestor {
        name: String,
        #[source]
        source: ZfsError,
    },
    #[error("spawn zfs recv: {0}")]
    SpawnRecv(#[source] zfskit::recv::RecvError),
    #[error("zfs recv failed: {0}")]
    RecvFailed(#[source] zfskit::recv::RecvError),
    #[error("stream copy: {0}")]
    StreamCopy(#[source] std::io::Error),
    #[error("no stdin on recv child")]
    NoChildStdin,
}

impl RecvError {
    fn code(&self) -> ErrorCode {
        match self {
            RecvError::Header(_)
            | RecvError::InvalidTarget { .. }
            | RecvError::InvalidSnapshot { .. } => ErrorCode::BadRequest,
            RecvError::Acl(_) => ErrorCode::Unauthorized,
            RecvError::NoChildStdin => ErrorCode::Internal,
            RecvError::Lock(_)
            | RecvError::AbortPartial { .. }
            | RecvError::ProbeAncestor { .. }
            | RecvError::CreateAncestor { .. }
            | RecvError::SpawnRecv(_)
            | RecvError::RecvFailed(_)
            | RecvError::StreamCopy(_) => ErrorCode::Zfs,
        }
    }

    fn response(&self) -> ResponseFrame {
        ResponseFrame {
            request_id: None,
            body: Response::Error {
                code: self.code(),
                message: self.to_string(),
            },
        }
    }
}

/// Drive one recv channel from start to finish. Errors are surfaced as
/// `Response::Error` written back to the caller; the function only
/// returns `Err` on stdin/stdout I/O failures so the calling process
/// can exit with a non-zero code.
pub async fn run<R, W>(
    runner: Arc<dyn CommandRunner>,
    acl: AllowedClient,
    pool: Option<Arc<SqlitePool>>,
    recv_locks: RecvLocks,
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
            let _ = write_response(&mut writer, &RecvError::Header(e).response()).await;
            let _ = writer.flush().await;
            return Ok(());
        }
    };
    let started = std::time::Instant::now();
    let outcome = drive(&runner, &acl, &recv_locks, &header, &mut reader).await;
    let resp = match &outcome {
        Ok(bytes) => {
            advance_last_hold(
                runner.as_ref(),
                job,
                &header.target_dataset,
                &header.send.to_snap.name,
            )
            .await;
            report_transfer(
                pool.as_deref(),
                job,
                &acl.identity,
                &header,
                *bytes,
                started,
            )
            .await;
            ResponseFrame {
                request_id: None,
                body: Response::Ok,
            }
        }
        Err(e) => e.response(),
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

/// Build the `zfs recv` invocation for one step.
///
/// `-s` for resumable, `-u` so the receive itself does not mount. But
/// `-u` only covers THIS receive — the properties the stream writes
/// outlive it, and a `zfs send -p` carries the sender's `mountpoint` and
/// `canmount`. Without stripping those, a sender could land a dataset
/// with `mountpoint=/root/.ssh, canmount=on` and the receiver's next
/// `zfs mount -a` would honour it. Mount policy belongs to the receiving
/// host, so both are inherited from the receive parent (created with
/// `mountpoint=none`) rather than taken from the stream.
///
/// `[allowed_clients.recv]` still wins where it speaks: setting `-o` and
/// `-x` for the same property is an error, so the config is applied
/// first and the defaults only fill what it left alone.
fn recv_args(target: &str, acl: &AllowedClient) -> RecvArgs {
    let mut args = RecvArgs::new(target.to_string()).unmounted().resumable();
    for key in &acl.recv.inherit_properties {
        args = args.property_inherit(key);
    }
    for (k, v) in &acl.recv.override_properties {
        args = args.property_override(k, v);
    }
    for key in MOUNT_POLICY_PROPERTIES {
        let spoken_for = acl.recv.override_properties.contains_key(key)
            || acl.recv.inherit_properties.iter().any(|k| k == key);
        if !spoken_for {
            args = args.property_inherit(key);
        }
    }
    args
}

/// Ancestors of `target` that the receive is allowed to create, shallowest
/// first: everything strictly below `root_fs` (the operator creates that
/// one) up to and including the direct parent. Without a `root_fs` the
/// pool takes that role, since it always exists.
fn creatable_ancestors(target: &str, root_fs: Option<&str>) -> Vec<String> {
    let Some((parent, _)) = target.rsplit_once('/') else {
        return Vec::new();
    };
    let stop = root_fs.unwrap_or_else(|| target.split('/').next().unwrap_or(target));
    let mut out = Vec::new();
    let mut prefix = String::new();
    for component in parent.split('/') {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        if prefix.len() > stop.len() && prefix.starts_with(stop) {
            out.push(prefix.clone());
        }
    }
    out
}

/// Create the missing ancestors of the receive target, each with
/// `mountpoint=none` and `canmount=off`. The leaf itself is created by
/// `zfs recv`.
///
/// Not `zfs create -p -o ...`: `-p` applies the `-o` properties to the
/// named dataset only, and `canmount` is not inherited, so every ancestor
/// `-p` created in between would come up `canmount=on` under whatever
/// mountpoint it inherited from the receive root and mount on the next
/// `zfs mount -a`.
async fn ensure_receive_ancestors(
    runner: &dyn CommandRunner,
    root_fs: Option<&str>,
    target: &str,
) -> Result<(), RecvError> {
    let candidates = creatable_ancestors(target, root_fs);
    // Datasets nest, so the first missing ancestor means every deeper one
    // is missing too: one probe per receive on the common path.
    let mut first_missing = candidates.len();
    for (i, name) in candidates.iter().enumerate() {
        let opts = ListOptions {
            recursive: false,
            types: vec![DatasetType::Filesystem, DatasetType::Volume],
            roots: vec![name.clone()],
            properties: vec!["name".into()],
            ..ListOptions::default()
        };
        match zfskit::dataset::list(runner, &opts).await {
            Ok(_) => {}
            Err(ZfsError::DatasetNotFound { .. }) => {
                first_missing = i;
                break;
            }
            Err(source) => {
                return Err(RecvError::ProbeAncestor {
                    name: name.clone(),
                    source,
                });
            }
        }
    }
    for name in &candidates[first_missing..] {
        let opts = zfskit::dataset::CreateOptions::new()
            .property("mountpoint", "none")
            .property("canmount", "off");
        zfskit::dataset::create(runner, name, &opts)
            .await
            .map_err(|source| RecvError::CreateAncestor {
                name: name.clone(),
                source,
            })?;
    }
    Ok(())
}

async fn drive<R>(
    runner: &Arc<dyn CommandRunner>,
    acl: &AllowedClient,
    recv_locks: &RecvLocks,
    header: &RecvHeader,
    reader: &mut R,
) -> Result<u64, RecvError>
where
    R: AsyncRead + Unpin,
{
    validate_header(header)?;
    check_root_fs(acl, &header.target_dataset)?;
    // Held across abort, stream ingestion and ZFS finalization. The
    // control RPC uses the same lock, so it cannot destroy `%recv` under
    // an active receiver and a second recv channel cannot enter.
    let _recv_lock = recv_locks
        .acquire(&header.target_dataset)
        .inspect_err(|e| {
            tracing::warn!(target = %header.target_dataset, error = %e, "recv admission refused");
        })?;
    if header.send.discard_partial_recv {
        // Same operation, same grant as the control RPC: a client with
        // only `recv` must not be able to throw away a resumable receive
        // by setting a flag.
        check_operation(acl, "control:discard_partial_recv", false)?;
        tracing::info!(
            target = %header.target_dataset,
            "recv: discarding partial recv per sender request"
        );
        zfskit::recv::abort_partial(runner.as_ref(), &header.target_dataset)
            .await
            .map_err(|source| RecvError::AbortPartial {
                dataset: header.target_dataset.clone(),
                source,
            })?;
    }
    ensure_receive_ancestors(
        runner.as_ref(),
        acl.root_fs.as_deref(),
        &header.target_dataset,
    )
    .await?;
    let args = recv_args(&header.target_dataset, acl);
    let mut handle = zfs_recv(runner.as_ref(), &args)
        .await
        .map_err(RecvError::SpawnRecv)?;
    let mut child_stdin = handle.take_stdin().ok_or(RecvError::NoChildStdin)?;
    let copy_res = tokio::io::copy(reader, &mut child_stdin).await;
    let _ = child_stdin.shutdown().await;
    drop(child_stdin);
    if let Err(copy_err) = copy_res {
        // A receiver that refuses the stream ("destination has been
        // modified", out of space, a bad property) exits and closes its
        // stdin, so the copy fails with EPIPE. The reason is in the
        // child's stderr, which killing it would throw away. It is
        // reaped, not killed: its stdin is shut down, so it reaches EOF
        // and exits.
        return Err(match handle.finish().await {
            Err(e) => RecvError::RecvFailed(e),
            Ok(()) => RecvError::StreamCopy(copy_err),
        });
    }
    handle.finish().await.map_err(RecvError::RecvFailed)?;
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
    tracing::info!(
        dataset = %header.target_dataset,
        snapshot = %header.send.to_snap.name,
        bytes,
        duration_ms,
        "recv: transfer complete"
    );
    let Some(pool) = pool else { return };
    let transfer = crate::state::recv_transfers::NewTransfer {
        completed_at: time::OffsetDateTime::now_utc().unix_timestamp(),
        job,
        identity,
        dataset: &header.target_dataset,
        to_snapshot: &header.send.to_snap.name,
        from_snapshot: header.send.from_snap.as_ref().map(|s| s.name.as_str()),
        bytes: bytes as i64,
        duration_ms,
    };
    if let Err(e) = crate::state::recv_transfers::record(pool, transfer).await {
        tracing::warn!(error = %e, "recv: transfer record failed");
    }
}

fn validate_header(header: &RecvHeader) -> Result<(), RecvError> {
    validate_dataset_name(&header.target_dataset).map_err(|reason| RecvError::InvalidTarget {
        name: header.target_dataset.clone(),
        reason,
    })?;
    validate_snapshot_ref("to_snap", &header.send.to_snap.name)?;
    if let Some(from) = &header.send.from_snap {
        validate_snapshot_ref("from_snap", &from.name)?;
    }
    Ok(())
}

fn validate_snapshot_ref(field: &'static str, name: &str) -> Result<(), RecvError> {
    validate_snapshot_leaf(name).map_err(|reason| RecvError::InvalidSnapshot {
        field,
        name: name.to_string(),
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_transport::{SendFlagsWire, SendHeader, SendKind, SnapshotRef};
    use std::sync::Mutex;
    use zfskit::runner::Cmd;

    #[test]
    fn creatable_ancestors_stop_at_the_root_fs() {
        assert_eq!(
            creatable_ancestors("tank/backups/nova/data/home", Some("tank/backups")),
            vec!["tank/backups/nova", "tank/backups/nova/data"]
        );
        // Direct child of the root: nothing to create.
        assert!(creatable_ancestors("tank/backups/nova", Some("tank/backups")).is_empty());
        // No root_fs: the pool is the floor.
        assert_eq!(
            creatable_ancestors("tank/a/b", None),
            vec!["tank/a".to_string()]
        );
        assert!(creatable_ancestors("tank", None).is_empty());
    }

    /// Answers existence probes from a fixed set and records every
    /// `zfs create` argv.
    struct AncestorRunner {
        existing: Vec<&'static str>,
        created: Mutex<Vec<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl CommandRunner for AncestorRunner {
        async fn run(&self, cmd: Cmd) -> Result<std::process::Output, std::io::Error> {
            use std::os::unix::process::ExitStatusExt;
            let args: Vec<String> = cmd
                .args_list()
                .iter()
                .map(|a| a.to_string_lossy().into())
                .collect();
            let output = |code: i32, stdout: &str, stderr: &str| std::process::Output {
                status: std::process::ExitStatus::from_raw(code << 8),
                stdout: stdout.as_bytes().to_vec(),
                stderr: stderr.as_bytes().to_vec(),
            };
            match args.first().map(String::as_str) {
                Some("list") => {
                    let name = args.last().expect("list names a dataset");
                    if self.existing.contains(&name.as_str()) {
                        Ok(output(
                            0,
                            &format!(
                                r#"{{"output_version":{{"command":"zfs list","vers_major":0,"vers_minor":1}},"datasets":{{"{name}":{{"name":"{name}","type":"FILESYSTEM","pool":"tank","createtxg":"1","properties":{{}}}}}}}}"#
                            ),
                            "",
                        ))
                    } else {
                        Ok(output(
                            1,
                            "",
                            &format!("cannot open '{name}': dataset does not exist"),
                        ))
                    }
                }
                Some("create") => {
                    self.created.lock().unwrap().push(args);
                    Ok(output(0, "", ""))
                }
                other => panic!("unexpected zfs subcommand {other:?}"),
            }
        }
    }

    // `zfs create -p -o mountpoint=none <parent>` applied the property to
    // the parent alone; the ancestors `-p` filled in between came up
    // `canmount=on` under the receive root's mountpoint and mounted on
    // the next `zfs mount -a`. Every created ancestor now gets both.
    #[tokio::test]
    async fn missing_ancestors_are_created_one_by_one_unmounted() {
        let r = AncestorRunner {
            existing: vec!["tank/backups", "tank/backups/nova"],
            created: Mutex::new(Vec::new()),
        };
        ensure_receive_ancestors(&r, Some("tank/backups"), "tank/backups/nova/data/x/home")
            .await
            .expect("ancestors created");
        let created = r.created.lock().unwrap().clone();
        assert_eq!(
            created,
            vec![
                vec![
                    "create",
                    "-o",
                    "mountpoint=none",
                    "-o",
                    "canmount=off",
                    "tank/backups/nova/data",
                ],
                vec![
                    "create",
                    "-o",
                    "mountpoint=none",
                    "-o",
                    "canmount=off",
                    "tank/backups/nova/data/x",
                ],
            ]
        );
        assert!(
            created.iter().all(|argv| !argv.contains(&"-p".to_string())),
            "-p would leave intermediate datasets mountable"
        );
    }

    #[tokio::test]
    async fn existing_ancestors_are_left_alone() {
        let r = AncestorRunner {
            existing: vec!["tank/backups", "tank/backups/nova"],
            created: Mutex::new(Vec::new()),
        };
        ensure_receive_ancestors(&r, Some("tank/backups"), "tank/backups/nova/home")
            .await
            .expect("nothing to create");
        assert!(r.created.lock().unwrap().is_empty());
    }

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

    fn acl_with(inherit: &[&str], overrides: &[(&str, &str)]) -> AllowedClient {
        AllowedClient {
            identity: "laptop".into(),
            fingerprint: None,
            jobs: vec![],
            operations: vec!["recv".into()],
            root_fs: None,
            recv: arctern_config::RecvConfig {
                inherit_properties: inherit.iter().map(|s| s.to_string()).collect(),
                override_properties: overrides
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
        }
    }

    // A `zfs send -p` stream carries the sender's mountpoint and canmount.
    // Verified against real ZFS: with these flags a stream claiming
    // `mountpoint=/root/.ssh, canmount=on` lands as `mountpoint=none
    // (inherited from the parent)`, on both full and incremental receives.
    #[test]
    fn mount_policy_is_taken_away_from_the_stream_by_default() {
        let args = recv_args("tank/backups/laptop", &acl_with(&[], &[]))
            .build_args()
            .expect("args build");
        for property in MOUNT_POLICY_PROPERTIES {
            let at = args.iter().position(|a| a == property);
            assert!(at.is_some(), "{property} not stripped: {args:?}");
            assert_eq!(args[at.unwrap() - 1], "-x", "{property} not inherited");
        }
        assert!(args.iter().any(|a| a == "-u"));
        assert!(args.iter().any(|a| a == "-s"));
        assert!(!args.iter().any(|a| a == "-F"), "never force rollback");
    }

    // Setting both -o and -x for one property is an error, so an operator
    // who states a policy must win outright rather than collide with ours.
    #[test]
    fn an_explicit_override_replaces_the_default_rather_than_joining_it() {
        let args = recv_args(
            "tank/backups/laptop",
            &acl_with(&[], &[("mountpoint", "/srv/backups")]),
        )
        .build_args()
        .expect("args build");
        assert!(
            args.windows(2)
                .any(|w| w[0] == "-o" && w[1] == "mountpoint=/srv/backups"),
            "operator override missing: {args:?}"
        );
        assert!(
            !args
                .windows(2)
                .any(|w| w[0] == "-x" && w[1] == "mountpoint"),
            "-x and -o for one property is a zfs error: {args:?}"
        );
        // The property the operator did not mention keeps its default.
        assert!(args.windows(2).any(|w| w[0] == "-x" && w[1] == "canmount"));
    }

    #[test]
    fn an_explicit_inherit_is_not_duplicated() {
        let args = recv_args("tank/backups/laptop", &acl_with(&["canmount"], &[]))
            .build_args()
            .expect("args build");
        assert_eq!(
            args.iter().filter(|a| *a == "canmount").count(),
            1,
            "canmount listed twice: {args:?}"
        );
    }

    #[test]
    fn validate_header_rejects_invalid_target_dataset() {
        let h = header("tank/backups#bookmark", None, "snap1");
        let err = validate_header(&h).unwrap_err();
        assert_eq!(err.code(), ErrorCode::BadRequest);
        assert!(matches!(err, RecvError::InvalidTarget { .. }), "{err:?}");
    }

    #[test]
    fn validate_header_rejects_invalid_snapshot_refs() {
        let h = header("tank/backups", Some("base snap"), "snap1");
        let err = validate_header(&h).unwrap_err();
        assert_eq!(err.code(), ErrorCode::BadRequest);
        assert!(
            matches!(
                err,
                RecvError::InvalidSnapshot {
                    field: "from_snap",
                    ..
                }
            ),
            "{err:?}"
        );

        let h = header("tank/backups", None, "snap/child");
        let err = validate_header(&h).unwrap_err();
        assert!(
            matches!(
                err,
                RecvError::InvalidSnapshot {
                    field: "to_snap",
                    ..
                }
            ),
            "{err:?}"
        );
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

    // The recv header's discard flag runs the same `zfs recv -A` as the
    // control RPC; the grant it answers to is the same fine-grained one,
    // and the wire code says so.
    #[test]
    fn an_acl_refusal_is_unauthorized_on_the_wire() {
        let err = RecvError::from(AclError::OperationNotGranted {
            identity: "laptop".into(),
            op: "control:discard_partial_recv",
        });
        assert_eq!(err.code(), ErrorCode::Unauthorized);
        assert!(err.to_string().contains("control:discard_partial_recv"));
    }
}
