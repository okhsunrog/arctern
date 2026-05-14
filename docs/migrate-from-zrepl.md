# Migrate from zrepl to arctern

Concrete cutover playbook for the laptop + NAS setup audited in the
session that produced this document. Translates the user's actual
zrepl configs to idiomatic arctern, lists pre-migration prep, and
walks the staged migration so you can run zrepl + arctern side-by-side
during the trial.

This is not a generic doc — it targets your specific pair of hosts.
For an unrelated migration, use the high-level deploy docs
(`deploy-snap-only.md`, `deploy-full-mirror.md`) and adapt.

## What changes vs. zrepl

| Aspect | zrepl | arctern |
|---|---|---|
| Transport | `tcp:8888` + IP-ACL | SSH + key auth |
| Receiver | `sink` job listening on port | `arctern stdinserver-dispatch` invoked by sshd |
| Job model | `push`, `pull`, `sink`, `snap` | `push`, `snap`, `prune` (no `pull`, no `sink`) |
| Receiver retention | `push.keep_receiver` on sender | separate `prune` job on receiver |
| Resume tokens | `protection: guarantee_resumability` | implicit; always on |
| Wire format | zrepl-specific | not wire-compat (both ends run arctern) |
| Snapshot tags | `zrepl_<RFC3339>` | identical — wire-compat by design |

Everything else (encrypted-raw send, grid retention, `^zrepl_.*` regex
+ negate idiom) translates 1:1.

## Pre-flight — both hosts

### Build + install arctern

On a build host with the laptop's libc:

```bash
cd ~/code/arctern
just build
# target/release/arctern is the single binary
```

Install on both laptop and NAS:

```bash
sudo install -m 0755 target/release/arctern /usr/local/bin/arctern
sudo install -m 0644 packaging/systemd/arctern.service /etc/systemd/system/arctern.service
sudo install -d -m 0755 /etc/arctern /var/lib/arctern
```

### ZFS delegated permissions

#### Laptop (sender)

```bash
sudo zfs allow -u okhsunrog \
  snapshot,hold,release,bookmark,send,destroy,userprop \
  novafs/arch0
```

Note: `mount` is intentionally absent on the sender — snap jobs never
mount. Adjust the user (`-u okhsunrog`) if you run the daemon as a
different account.

#### NAS (receiver)

The dedicated `arctern-replicator` user that sshd will dispatch into
needs receive-side perms on the target subtree:

```bash
sudo useradd -r -s /usr/sbin/nologin -d /var/lib/arctern-replicator arctern-replicator
sudo install -d -m 0700 -o arctern-replicator -g arctern-replicator /var/lib/arctern-replicator/.ssh

sudo zfs create -p -o mountpoint=none okdata/backups/laptop
sudo zfs allow -u arctern-replicator \
  create,receive,destroy,hold,release,mount,rollback,userprop,canmount,mountpoint \
  okdata/backups
```

Also the system arctern daemon's user (root by default in the unit)
needs snap perms on `databak`/`rootbak` datasets. With the daemon
running as root that's automatic; for an unprivileged-daemon
deployment, add:

```bash
sudo zfs allow -u arctern \
  snapshot,hold,release,bookmark,destroy,userprop \
  okdata/data
sudo zfs allow -u arctern \
  snapshot,hold,release,bookmark,destroy,userprop \
  okdata/ROOT/default
```

### SSH key + sshd ForcedCommand

On the laptop:

```bash
sudo install -d -m 0700 /var/lib/arctern/ssh
sudo ssh-keygen -t ed25519 -N "" -C "arctern@laptop" \
  -f /var/lib/arctern/ssh/id_ed25519
sudo cat /var/lib/arctern/ssh/id_ed25519.pub
# Copy this single line — you'll paste it on the NAS next.
```

On the NAS, prepend the ForcedCommand to the pubkey and add it to
`arctern-replicator`'s `authorized_keys`:

```bash
echo 'command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA...keymaterial... arctern@laptop' \
  | sudo tee -a /var/lib/arctern-replicator/.ssh/authorized_keys
sudo chmod 600 /var/lib/arctern-replicator/.ssh/authorized_keys
sudo chown arctern-replicator:arctern-replicator /var/lib/arctern-replicator/.ssh/authorized_keys
```

The `laptop_nova` argv is the identity string the dispatcher looks
up in `[[allowed_clients]]`. Pick whatever, just keep it consistent.

Optional: pin the SSH fingerprint into `allowed_clients.fingerprint`
for belt-and-suspenders. Compute on the laptop:

```bash
ssh-keygen -lf /var/lib/arctern/ssh/id_ed25519.pub
# SHA256:<fingerprint>  arctern@laptop (ED25519)
```

### Verify the SSH path from laptop to NAS

```bash
sudo -u <daemon-user> ssh \
  -i /var/lib/arctern/ssh/id_ed25519 \
  -o StrictHostKeyChecking=accept-new \
  arctern-replicator@10.44.44.1 \
  arctern stdinserver-dispatch laptop_nova
# Expect: protocol handshake bytes, then connection close (no shell)
```

If you see a shell prompt, the ForcedCommand didn't apply. Check
authorized_keys for typos.

Repeat for `10.66.66.2`. The same key works for both peers — only the
host changes.

## Translated configs

### Laptop — `/etc/arctern/arctern.toml`

```toml
state_dir = "/var/lib/arctern"

# Defaults applied to every job. Per-job overrides win.
[defaults]
prefix = "zrepl_"
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m | 24x1h | 14x1d"
# protect_non_prefixed = true (the default) appends the regex+negate
# rule so manual / non-zrepl_-prefixed snapshots survive prune.

# Two peers — one per WG endpoint. The push job lists them in
# preferred order; whichever connects, that's the cycle's target.
[[peers]]
name = "nas-home"
ssh_target = "arctern-replicator@10.44.44.1"
[[peers]]
name = "nas-remote"
ssh_target = "arctern-replicator@10.66.66.2"

# zrepl: snapshot_arch0
#   filesystems: { "novafs/arch0<": true, "novafs/arch0": false,
#                  "novafs/arch0/data": false }
#   pruning: 4x15m | 24x1h | 3x1d  →  defaults' 4x15m | 24x1h | 14x1d
#     (1d retention bumped to 14 days to match the home-server keep;
#     if you want zrepl's exact 3x1d locally, override per-job.)
[[jobs]]
type = "snap"
name = "snapshot_arch0"
filesystems = { "novafs/arch0/" = true, "novafs/arch0" = false, "novafs/arch0/data" = false }

# zrepl: push_to_local + push_to_remote — collapsed into one job
# with two targets. First-reachable wins each cycle. Each peer gets
# its own cursor bookmark, so a peer that's been offline for a week
# catches up from its own state when it's reachable again.
[[jobs]]
type = "push"
name = "push_to_nas"
targets = ["nas-home", "nas-remote"]
interval = "15m"
filesystems = { "novafs/arch0/data/home" = true, "novafs/arch0/data/root" = true }
[jobs.target]
root_fs = "okdata/backups/laptop"
# All four send flags default true (encrypted+embedded+compressed+
# large_blocks) — zrepl's canonical push uses the same.
```

That's 25 lines. The zrepl original was ~85.

### NAS — `/etc/arctern/arctern.toml`

```toml
state_dir = "/var/lib/arctern"

[defaults]
prefix = "zrepl_"
[defaults.pruning]
# Receiver-side fade-out (was zrepl's `databak` grid).
grid = "6x4h | 14x1d"

# zrepl: databak — 4-hourly snaps of three filesystems.
[[jobs]]
type = "snap"
name = "databak"
filesystems = { "okdata/data/nas" = true, "okdata/data/root" = true, "okdata/data/home" = true }
[jobs.snapshotting]
interval = "4h"

# zrepl: rootbak — daily snap of the system dataset.
[[jobs]]
type = "snap"
name = "rootbak"
filesystems = { "okdata/ROOT/default" = true }
[jobs.snapshotting]
interval = "1d"
[jobs.pruning]
# rootbak in zrepl was 7x1d — different from databak, so override.
keep = [
  { type = "grid", grid = "7x1d", regex = "^zrepl_.*" },
  { type = "regex", regex = "^zrepl_.*", negate = true },
]

# zrepl: push_to_local/push_to_remote → push.keep_receiver collapse
# into one local prune job over the received subtree. The prune job
# only deletes; the sending laptop creates the snapshots via its push
# stream.
[[jobs]]
type = "prune"
name = "received_prune"
interval = "1h"
filesystems = { "okdata/backups/laptop/" = true, "okdata/backups/laptop" = false }
[jobs.pruning]
keep = [
  { type = "grid", grid = "1x1h | 24x1h | 14x1d | 8x1w", regex = "^zrepl_.*" },
  { type = "regex", regex = "^zrepl_.*", negate = true },
]

# zrepl: from_remote sink. ACL replaces the `clients` IP map; sshd's
# ForcedCommand identifies the caller by `identity`.
[[allowed_clients]]
identity = "laptop_nova"
# Optional: paste the laptop's SSH key fingerprint for defense-in-depth.
# fingerprint = "SHA256:..."
jobs = ["push_to_nas"]
operations = ["control", "recv"]
root_fs = "okdata/backups/laptop"

# Maps zrepl's `recv.properties.inherit` + `recv.properties.override`
# 1:1 — applied as `zfs recv -x mountpoint -o canmount=off
# -o org.openzfs.systemd:ignore=on` on every dataset received from
# this client.
[allowed_clients.recv]
inherit_properties = ["mountpoint"]
override_properties = { canmount = "off", "org.openzfs.systemd:ignore" = "on" }
```

## Validate before deploy

On each host, with the binary installed:

```bash
sudo arctern configcheck /etc/arctern/arctern.toml
# Expect: ok
```

If you see an error, fix the config; non-zero exit blocks systemd
start.

## Staged cutover

### Phase 1 — Server-side trial (snap-only, ≥1 week)

The NAS's `databak` + `rootbak` are independent of the replication
flow. Start them first to gain operational confidence.

1. Don't yet enable `received_prune` or `[[allowed_clients]]` — leave
   the NAS's zrepl `sink` running for now.
2. Edit zrepl's config to remove `databak` and `rootbak`, then
   `systemctl reload zrepl`. (Or simpler — disable them by commenting.)
3. `systemctl enable --now arctern` on the NAS.
4. Verify within 4 hours: `zfs list -t snapshot okdata/data/home | grep zrepl_`.
   New snapshots should have the same `zrepl_<RFC3339>` tag format.
5. Watch for 1 week. Check `journalctl -fu arctern` for errors. The
   admin UI at `http://127.0.0.1:7878/` (`ssh -L 7878:127.0.0.1:7878`
   from your workstation) shows job status + recent events.

Rollback if anything weird: `systemctl stop arctern && systemctl
reload zrepl` (after un-commenting databak/rootbak).

### Phase 2 — Replication for ONE filesystem (≥1 week)

Pick the lowest-churn dataset for the trial. Suggest
`novafs/arch0/data/root` — small, infrequent changes, easy to verify.

1. On the laptop, edit zrepl to remove just `novafs/arch0/data/root`
   from `push_to_local` + `push_to_remote` filesystems. Reload zrepl.
2. Verify zrepl stopped pushing that dataset.
3. Edit the laptop arctern config to ONLY include
   `novafs/arch0/data/root` in `push_to_nas.filesystems`. Save.
4. On the NAS, finalize the arctern config: add the
   `[[allowed_clients]]` block + `received_prune` job. Reload.
5. On the laptop: `systemctl enable --now arctern`.
6. Within ~30 min: a full send completes. Verify on the NAS:

   ```bash
   zfs list -r okdata/backups/laptop/novafs/arch0/data/root
   zfs list -t snapshot okdata/backups/laptop/novafs/arch0/data/root | grep zrepl_
   ```
7. Trigger a deliberate WG bounce on the laptop (or move between
   home and remote WG endpoints). Verify the next cycle resumes
   cleanly against whichever peer becomes reachable. The admin UI's
   Peers tab shows each peer's reachability.
8. Watch for 1 week. Zero `last_error` is the bar.

### Phase 3 — Widen to data/home (the real TB-scale test)

`data/home` is the production-hours validation. First full send may
run for hours.

1. On the laptop, remove `novafs/arch0/data/home` from zrepl's push
   filesystems. Reload zrepl.
2. Add `"novafs/arch0/data/home" = true` to arctern's push
   `filesystems`. Reload arctern.
3. First push cycle starts a full send. Monitor:

   ```bash
   journalctl -fu arctern
   ```

   The send may take many hours. Resume tokens protect against
   interruption — if the connection drops, the next cycle resumes
   from the partial recv.
4. Once the full send completes, incremental sends every 15min from
   then on. Verify with `zfs list -t snapshot` on both ends.
5. Watch 1 week minimum.

### Phase 4 — Cutover the remaining datasets + retire zrepl

```bash
# laptop
sudo systemctl stop zrepl
sudo systemctl disable zrepl
# edit arctern config to enable all four datasets
sudo systemctl reload arctern
```

```bash
# NAS — only after laptop is fully migrated AND all snapshots in
# okdata/backups/* are arctern-managed
sudo systemctl stop zrepl
sudo systemctl disable zrepl
```

Don't `purge` zrepl yet — keep it installed for a month so rollback
is a `systemctl start zrepl` away.

## Rollback

At any phase: stop arctern, re-enable zrepl, undo the config edits
that removed the dataset from zrepl.

```bash
sudo systemctl stop arctern
sudo systemctl disable arctern
# revert /etc/zrepl/zrepl.yml from your last-good backup
sudo systemctl restart zrepl
```

The snapshots arctern created are normal `zrepl_`-prefixed snapshots;
zrepl will happily continue managing them. The cursor bookmarks
arctern creates (`arctern_cursor_J_<job>_P_<peer>`) are bookmarks,
not snapshots — they don't interfere with zrepl. If you want them
gone for cleanliness:

```bash
zfs list -t bookmark -r novafs/arch0 | grep arctern_ \
  | awk '{print $1}' | xargs -I{} zfs destroy {}
```

## Caveats specific to this setup

### TB-scale send is unvalidated

I have not seen arctern do a multi-hour send in production. The
plumbing is right (resume tokens, holds, cursor bookmarks all work in
the test harness), but the first `data/home` send is genuine new
information. Plan a window where you can babysit it.

### Both WG endpoints share the same daemon

When you switch networks (home → away), the laptop's daemon doesn't
need to do anything special. The next 15-min cycle attempts `nas-home`,
fails to connect (10.44.44.1 unreachable off-WG), falls through to
`nas-remote`, and uses that. The reconnect background task is per-
peer and keeps retrying both, so when you come home again, `nas-home`
reconnects and becomes the preferred target on the cycle after.

There IS a quiet window between "network change" and "next cycle":
worst case 15 minutes (one cycle interval) of no replication. Use
`POST /api/v1/jobs/push_to_nas/wakeup` if you want an immediate cycle
after a network change. The admin UI's Jobs page has a button.

### `placeholder.encryption: off` semantics

zrepl creates intermediate placeholder datasets with `encryption=off`
when receiving deep paths. arctern doesn't create intermediates with
an explicit encryption setting — they inherit from the parent
(`okdata/backups`, which should be unencrypted in your setup).

If `okdata/backups` IS encrypted: incoming raw-encrypted children
(`-w` sends) carry their own keys and don't need placeholder
encryption, but the placeholder ITSELF will inherit encryption from
`okdata/backups`. That's fine for `mountpoint=none` placeholders, but
if you ever want to mount one, you'd need to load its key. Match
zrepl's behavior by keeping `okdata/backups` unencrypted.

### Receiver still mounts on `okdata/backups/laptop`?

`allowed_clients.recv.override_properties = { canmount = "off", ... }`
+ palimpsest's hardcoded `-u` flag (unmount-after-recv) + the
`mountpoint=none` set on the placeholder parents means received
datasets stay unmounted. If you see them appear in `df`, check those
properties — `org.openzfs.systemd:ignore=on` also tells systemd's
zfs-mount generator to skip them.
