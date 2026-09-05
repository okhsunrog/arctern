# Migrating from zrepl

arctern borrows zrepl's snapshot idiom (`<prefix><RFC3339-utc>`), its
grid retention and its hold + cursor-bookmark discipline, but it is not
wire-compatible: the two daemons cannot talk to each other, and arctern
does not read `zrepl.yml`. A migration is therefore a cutover, not an
upgrade. This guide is written for the topology arctern targets, a
sender that pushes to one or more receivers over SSH, and assumes
[`install.md`](install.md) for the mechanics of installing either side.

Two paths are covered:

- **In place**: arctern keeps replicating into the tree zrepl filled,
  continuing incrementally from the last snapshot zrepl sent. Nothing is
  resent. This is what the planner's bookmark fallback exists for.
- **Clean slate**: arctern gets a fresh receive tree and starts with one
  full send per filesystem. Simpler to reason about, costs one full
  transfer, and leaves zrepl's tree untouched as a fallback until you
  delete it.

Either way, run both daemons side by side first and cut over only when
arctern's first sync has landed.

## What maps to what

| zrepl | arctern | notes |
|---|---|---|
| `snap` job | `[[jobs]] type = "snap"` | same prefix + grid idiom; grid keeps the oldest snapshot per bucket, as zrepl does |
| `push` job | `[[jobs]] type = "push"` + `[[peers]]` | a peer is one host with prioritised `routes` |
| `sink` job | `[[allowed_clients]]` in the receiver's config | no daemon needed on the receiver for replication |
| `client_identity` | `identity` | the argument to `arctern stdinserver-dispatch` in `authorized_keys` |
| sink `root_fs` | `root_fs` on the push job **and** on the ACL | arctern lands datasets at `root_fs/<sender path>`; zrepl landed them at `root_fs/<client_identity>/<sender path>` |
| `pruning.keep_sender` | the snap job's `[jobs.pruning]` | |
| `pruning.keep_receiver` | a `prune` job in the receiver's config | needs the receiver's daemon |
| `replication.protection` / step holds | automatic step holds + cursor bookmarks | peer-namespaced, so multi-target jobs track each receiver |
| `send.raw`, `send.encrypted`, ... | `[jobs.send]` | raw + embedded + compressed + large_blocks by default |
| TLS / TCP transports | none | SSH only, driven through the system `ssh(1)` |
| `zrepl status` | the web console | there is no status CLI |

Two behaviours differ by default and deserve a conscious decision:

- **How much history reaches the receiver.** zrepl replicates every
  snapshot. arctern's `replicate = "latest"` (the default) sends one
  snapshot per push, the newest, and lets the sender keep the
  fine-grained history. Set `replicate = "all"` on the push job for
  zrepl's behaviour; it uses `zfs send -I`, which carries every snapshot
  between the common base and the head, manual ones included.
- **arctern never runs `zfs recv -F`.** A receiver that has data with no
  common base is refused with a message that says so, rather than rolled
  back. zrepl's sink would refuse as well in its default configuration,
  but if you relied on forced rollback anywhere, plan the reconciliation
  by hand.

## The snapshot prefix

Both daemons prune only snapshots matching their own prefix regex and
protect everything else. That makes the prefix the safety rail during
the side-by-side period, and the choice you make here decides what the
cutover looks like.

**Keep `zrepl_`.** arctern adopts the existing history as its own: the
planner finds common snapshots by name and GUID directly, the retention
grid applies to old and new snapshots alike, and nothing needs cleaning
up afterwards. The cost is that while both daemons run, both consider
the same snapshots theirs to prune. Do this only with zrepl's snap job
for those filesystems already disabled.

**Switch to `arctern_`.** The two daemons cannot interfere: each prunes
only its own prefix and protects the other's. This is the safer shape
for a trial. Afterwards the `zrepl_` snapshots stay behind, protected by
arctern's non-prefixed rule, until you remove them by hand once
arctern's own retention has built up. Replication still continues
incrementally in place: the first push finds no `arctern_` snapshot on
the receiver, falls back to zrepl's cursor bookmark
(`#zrepl_CURSOR_G_<guid>_J_<job>`, whose GUID the receiver still has)
and sends `zfs send -i <bookmark> <first arctern snapshot>`.

The examples below use `arctern_`.

## 1. Install, keep zrepl running

Install the binary on both hosts as in [`install.md`](install.md) §1.
Leave zrepl running on both; nothing here stops it.

Pick one low-churn filesystem for the trial rather than the whole job.
Every step below is per filesystem, and the first one teaches you what
the events look like.

## 2. Receiver: ACL and forced command

Create the dedicated key on the sender ([`install.md`](install.md) §2a)
and install it on the receiver with the forced command
([`install.md`](install.md) §3a). The receiver's `/etc/arctern/arctern.toml`
gets an ACL row; `root_fs` is where the two paths diverge.

In place, pointing at the tree zrepl filled. With a zrepl sink
`root_fs: backup/zrepl` and `client_identity: laptop`, the sender's
`tank/data` lives at `backup/zrepl/laptop/tank/data`, so:

```toml
state_dir = "/var/lib/arctern"

[[allowed_clients]]
identity = "laptop"
jobs = ["push_to_nas"]
operations = ["control", "control:discard_partial_recv", "recv"]
root_fs = "backup/zrepl/laptop"
```

Clean slate, a fresh tree next to zrepl's:

```toml
state_dir = "/var/lib/arctern"

[[allowed_clients]]
identity = "laptop"
jobs = ["push_to_nas"]
operations = ["control", "control:discard_partial_recv", "recv"]
root_fs = "backup/arctern/laptop"
```

```sh
sudo zfs create -p -o mountpoint=none -o canmount=off backup/arctern/laptop
sudo arctern configcheck /etc/arctern/arctern.toml
```

If the receiver should keep pruning what it receives (zrepl's
`keep_receiver`), add a prune job and run the daemon there; see
[`install.md`](install.md) §3d. Its rules should name the prefix you
chose, so with `arctern_` it leaves zrepl's snapshots alone.

## 3. Sender: snap and push jobs, dry run first

`/etc/arctern/arctern.toml` on the sender, with the push job in plan-only
mode:

```toml
state_dir = "/var/lib/arctern"
socket = "/run/arctern/arctern.sock"

[defaults]
prefix = "arctern_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m(keep=all) | 24x1h | 3x1d"

[[peers]]
name = "nas"
auto_interval = "1d"
[[peers.routes]]
name = "lan"
ssh_target = "arctern-nas"           # Host alias, see install.md 2b

[[jobs]]
type = "snap"
name = "snap_local"
filesystems = { "tank/data" = true } # the trial filesystem

[[jobs]]
type = "push"
name = "push_to_nas"
targets = ["nas"]
dry_run = true                       # plan and log, send nothing
# replicate = "all"                  # zrepl's behaviour; default is "latest"
filesystems = { "tank/data" = true }
[jobs.target]
root_fs = "backup/zrepl/laptop"      # or backup/arctern/laptop for a clean slate
```

Start the daemon ([`install.md`](install.md) §2d), open the console and
press "Send now" on the push job. With `dry_run = true` the cycle plans
every filesystem and records what it would send; the Events view shows
one of these lines per filesystem:

- `push: incremental send from bookmark` with `from_bookmark=zrepl_CURSOR_G_..._J_...`:
  the in-place path is working. The base is zrepl's cursor, the target
  is the newest `arctern_` snapshot.
- `push: incremental send` with `from=zrepl_...`: also fine; this is the
  in-place path with the `zrepl_` prefix kept.
- `push: full send`: expected only on the clean-slate path. On the
  in-place path it means the planner found no common base; see the
  troubleshooting list below before going further.
- `push: nothing to do`: the receiver already holds the sender's newest
  filtered snapshot.

The job's status stays `dry_run` rather than `ok` until you turn the
flag off; a dry run is never counted as a sync.

## 4. Cut over

Per filesystem, in this order:

1. Remove the filesystem from zrepl's snap and push jobs on the sender
   and `systemctl restart zrepl`. If you kept the `zrepl_` prefix this
   step is what stops the two pruners from overlapping.
2. Set `dry_run = false`, restart arctern, press "Send now".
3. Verify the snapshot landed with the same GUID on both ends:

   ```sh
   zfs get -H -o value guid tank/data@arctern_<TAG>                       # sender
   zfs get -H -o value guid backup/zrepl/laptop/tank/data@arctern_<TAG>   # receiver
   ```

4. Check the sender has a cursor bookmark and no lingering step hold:

   ```sh
   zfs list -t bookmark tank/data      # tank/data#arctern_cursor_G_<guid>_J_push_to_nas_P_nas
   zfs holds tank/data@arctern_<TAG>   # empty once the cycle committed
   ```

Repeat with the remaining filesystems, then disable zrepl on both hosts:

```sh
sudo systemctl disable --now zrepl
```

Leave the package installed until the cleanup below is done and a couple
of weeks of arctern-only operation look clean.

## 5. Clean up zrepl's leftovers

zrepl leaves holds and bookmarks behind, and arctern never touches
anything it did not create. Holds matter most: arctern's pruner skips
held snapshots, so a snapshot zrepl held stays forever.

List them on each host:

```sh
zfs list -H -o name -t snapshot -r tank | xargs -r zfs holds -H | grep zrepl_
zfs list -H -o name -t bookmark -r tank | grep '#zrepl_'
```

Release the holds and destroy the bookmarks once arctern has completed
at least one successful push for the filesystem. arctern's own cursor
(`#arctern_cursor_*`) has taken over from zrepl's by then, and its
`arctern_last_J_<job>` hold on the receiver protects the common
snapshot in place of zrepl's:

```sh
zfs release <tag> <snapshot>       # per line of the holds listing
zfs destroy tank/data#zrepl_CURSOR_G_..._J_...
```

If you switched prefixes, the `zrepl_` snapshots remain, protected by
the non-prefixed rule. Remove them once arctern's grid covers the same
horizon (the length of your longest bucket run):

```sh
zfs list -H -o name -t snapshot -r tank | grep '@zrepl_' | xargs -r -n1 zfs destroy
```

Do the same on the receiver for the tree arctern now owns. On the
clean-slate path, `zfs destroy -r` zrepl's tree when you no longer want
it as a fallback.

## Rollback

While zrepl is still installed and its config untouched:

```sh
sudo systemctl disable --now arctern
sudo systemctl enable --now zrepl
```

On the in-place path zrepl continues from its own cursor as long as the
snapshot it points at still exists on both sides, which arctern's prune
does not guarantee once its grid has run for a while. On the clean-slate
path zrepl's tree was never touched. `arctern_` snapshots left behind on
either side are protected by zrepl's own non-prefixed rule and can be
removed by hand.

## Troubleshooting

- **`holds N snapshot(s) but shares no snapshot or bookmark with the
  sender`** on the first push: the in-place path found no common base.
  Usual causes: `root_fs` on the push job does not point at
  `<zrepl root_fs>/<client_identity>`, so arctern is looking at a
  different dataset; or zrepl's cursor bookmark on the sender was
  destroyed and the sender's copy of the receiver's newest snapshot has
  already been pruned. Fix the path, or fall back to the clean-slate
  tree.
- **`exists but has no snapshots`**: the target dataset is an empty
  placeholder, typically created by a child's receive before the parent
  ever synced, or by hand. Destroy it if it has no children.
- **Snapshots the grid should have destroyed are still there**: run
  `zfs holds` on them; a `zrepl_*` tag is pinning them (§5).
- **`requires SSH key fingerprint verification, but no SSH auth info is
  available`**: `fingerprint` is set in the ACL but sshd lacks
  `ExposeAuthInfo yes` ([`install.md`](install.md) §3c).
- **Auth failures or a fail2ban ban** on the receiver while zrepl and
  arctern coexist: the daemon's Host alias needs `IdentitiesOnly yes`,
  or ssh offers every agent key before the dedicated one.
- **The binary built on the sender does not start on the receiver**
  (glibc version mismatch): use the static musl release for the
  receiver; the two hosts need not run the same build.
