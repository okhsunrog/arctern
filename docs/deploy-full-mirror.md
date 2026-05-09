# Full-mirror deployment (laptop + server, replicating zrepl)

Goal: replace zrepl's snap+push+sink topology with arctern, end to end:

```
                                  WireGuard
laptop (novafs)  ──── push ────►  server (okdata)
   snap_arch0                       databak + rootbak (snap)
   push_to_local  → 10.44.44.1      sink_from_laptop (sink)
   push_to_remote → 10.66.66.2
```

This deployment assumes you have already completed the snap-only-on-server
trial in `deploy-snap-only.md` and run it for at least a week without
incident. Don't skip that — this doc trusts that arctern's snap+prune
behavior on YOUR data has been validated.

The cutover is staged in three phases. **Do not collapse them.** Each
phase isolates one new failure mode so when something breaks (something
will), you know which change caused it.

## Phase 0 — Prerequisites

- Snap-only trial passed on server, ≥1 week, zero `last_error`.
- Laptop has ZFS pool `novafs/arch0<` with the dataset list matching
  your zrepl `push_to_local` / `push_to_remote`.
- WireGuard tunnel is up, both directions, between laptop and server.
- You can `ssh root@10.77.77.100` from the laptop and reach
  `10.44.44.1` / `10.66.66.2` from the server.
- You've decided which ONE filesystem to trial replication with first.
  Pick the lowest-churn one (e.g. `novafs/arch0/data/home` if home
  changes slowly, or pick a dedicated test dataset). Do NOT trial all
  four filesystems at once.

## Phase 1 — Server-side sink

Add a sink job to the server's existing arctern config. zrepl's
`from_remote` job stays running on the same `:8888` port, so the sink
must use a DIFFERENT port (e.g. `:8889`) to avoid collision.

Append to `/etc/arctern/arctern.toml` on the server:

```toml
[[jobs]]
type = "sink"
name = "sink_from_laptop_trial"
listen = "0.0.0.0:8889"
root_fs = "okdata/backups/arctern_trial/laptop"

# Match zrepl's recv config. canmount=off prevents auto-mount of the
# received hierarchy; org.openzfs.systemd:ignore stops systemd from
# trying to do anything with these mountpoints.
[jobs.recv.properties]
override = { canmount = "off", "org.openzfs.systemd:ignore" = "on" }
inherit = ["mountpoint"]
```

Pre-create the receiving root **before starting the daemon** so its
encryption disposition is intentional, not inherited:

```bash
zfs create -p -o encryption=off -o canmount=off okdata/backups/arctern_trial
zfs create    -o encryption=off -o canmount=off okdata/backups/arctern_trial/laptop
```

If `okdata/backups` itself is encrypted, the `encryption=off` on the
trial parent is critical for raw-encrypted recv to work (see
`recv.placeholder.encryption=off` in your zrepl config — same reason).
The integration test for this is in
`daemon/tests/integration_quic_push_encrypted.rs` (added separately).

Validate + restart:

```bash
arctern configcheck /etc/arctern/arctern.toml
systemctl restart arctern
journalctl -fu arctern
```

You should see two log lines for the sink: `LISTEN unix:...` (existing)
and a new `LISTEN_QUIC 0.0.0.0:8889`. If the QUIC bind fails (port
collision, EACCES on low ports), fix and retry.

Smoke test from the laptop:

```bash
nc -uvz 10.77.77.100 8889    # UDP probe; "succeeded" means kernel
                              # passed the packet (QUIC won't reply,
                              # but absence of "no route" is enough)
```

## Phase 2 — Laptop side, snap + push for ONE filesystem

Build + install on the laptop the same way as the server (see
`deploy-snap-only.md` §1–§2). Same systemd unit; same paths.

Write `/etc/arctern/arctern.toml` on the laptop:

```toml
state_dir = "/var/lib/arctern"

# Snap job — distinct prefix so zrepl's snap_arch0 keeps owning zrepl_*.
# This is the firewall: arctern's prune touches only arctern_*; zrepl's
# prune touches only zrepl_*. They cannot fight even if you accidentally
# misconfigure overlapping filesystems.
[[jobs]]
type = "snap"
name = "snap_trial"

[[jobs.filesystems]]
path = "novafs/arch0/data/home"   # the ONE dataset you picked
recursive = false

[jobs.snapshotting]
type = "periodic"
interval = "15m"
prefix = "arctern_"

[[jobs.pruning.keep]]
type = "grid"
grid = "4x15m | 24x1h | 3x1d"
regex = "^arctern_.*"

[[jobs.pruning.keep]]
type = "regex"
regex = "^arctern_.*"
negate = true

# Push to local server. Note: TWO push jobs (one per peer) — QUIC
# connection migration was theoretical, not implemented. If your laptop
# roams between LAN and remote, both jobs run; whichever route is
# reachable succeeds, the other fails fast and retries next cycle.
[[jobs]]
type = "push"
name = "push_local_trial"
connect = "10.44.44.1:8889"
interval = "15m"

[[jobs.filesystems]]
path = "novafs/arch0/data/home"

[jobs.target]
root_fs = "okdata/backups/arctern_trial/laptop"

[jobs.send]
encrypted = true
embedded_data = true
compressed = true
large_blocks = true

[jobs.snapshot_filter]
prefix = "arctern_"

# Second push to the remote IP. Only one of the two will be reachable
# at a time depending on where the laptop is.
[[jobs]]
type = "push"
name = "push_remote_trial"
connect = "10.66.66.2:8889"
interval = "15m"

[[jobs.filesystems]]
path = "novafs/arch0/data/home"

[jobs.target]
root_fs = "okdata/backups/arctern_trial/laptop"

[jobs.send]
encrypted = true
embedded_data = true
compressed = true
large_blocks = true

[jobs.snapshot_filter]
prefix = "arctern_"
```

Validate + start:

```bash
arctern configcheck /etc/arctern/arctern.toml
systemctl daemon-reload
systemctl start arctern
journalctl -fu arctern
```

zrepl is untouched and still running — it owns `zrepl_*` snapshots and
its own push jobs to `:8888`.

## Phase 3 — Verify the round-trip

Within ~16 minutes (one snap cycle + one push cycle):

On the laptop:
```bash
zfs list -t snapshot novafs/arch0/data/home | grep arctern_
# Expect ≥1 arctern_-prefixed snapshot.

curl -s --unix-socket /run/arctern/arctern.sock \
  http://localhost/api/v1/jobs | jq '.[] | {name, last_error, last_run}'
# Expect snap_trial + one of push_local/push_remote with last_error:null.
# The other push will likely have last_error set to a connect failure
# if its peer IP is unreachable — that's expected.
```

On the server:
```bash
zfs list -r okdata/backups/arctern_trial/laptop
# Expect: okdata/backups/arctern_trial/laptop/novafs/arch0/data/home
# AND its snapshot

zfs list -t snapshot okdata/backups/arctern_trial/laptop/novafs/arch0/data/home
# Expect the same arctern_<RFC3339> tag the laptop has.
```

Trigger an immediate push to test wakeup:

```bash
# On the laptop:
curl -s -X POST --unix-socket /run/arctern/arctern.sock \
  http://localhost/api/v1/jobs/push_local_trial/wakeup
# Expect HTTP 204. Cycle should run within seconds.
```

Verify GUID match (correctness check):

```bash
# On the laptop:
zfs get -Hp -o value guid novafs/arch0/data/home@arctern_<TAG>
# On the server:
zfs get -Hp -o value guid okdata/backups/arctern_trial/laptop/novafs/arch0/data/home@arctern_<TAG>
# Both numbers must be identical. If they differ, replication is broken
# even if both ends "have a snapshot" — STOP and debug.
```

## Phase 4 — Trial period

Run for **≥1 week**. zrepl is still running the canonical replication
for everything else (`okdata/backups/laptop/...`); arctern is running
in parallel for the trial dataset (`okdata/backups/arctern_trial/laptop/...`).

Watch for:
- `last_error` accumulating on push jobs (transient WG drops should
  self-heal next cycle; persistent errors are a real bug)
- Snapshot count drift between sender and receiver — they should be
  identical (modulo the most recent one if a cycle is in flight)
- Resume token activity in the journal: `resumed from token` after a
  network blip should be visible. If you NEVER see it across a week of
  WG roaming, the resume path may not be exercising — concerning but
  not blocking.
- Receiver disk usage growing as expected from prune retention math.
  Server has its own arctern snap retention pruning the receiver-side
  snapshots IF you've set a snap job pointing at `okdata/backups/...`
  (you haven't yet — receiver-side retention currently just keeps
  whatever the sender pushes). You may want to add a server-side snap
  job for the trial path with a permissive grid (e.g.,
  `48x1h | 30x1d`) before confidence in long-term receiver-side
  retention is established.

Compare against zrepl's parallel data:

```bash
# On the server, show what zrepl + arctern are storing for the same
# logical dataset
du -sh /okdata/backups/laptop/novafs/arch0/data/home
du -sh /okdata/backups/arctern_trial/laptop/novafs/arch0/data/home
# Should be within a few % — both stores hold the same snapshots
# modulo prune timing.
```

## Phase 5 — Broaden, then cut over

Only after the trial dataset has been clean for a week:

1. Stop arctern on the laptop: `systemctl stop arctern`.
2. Edit `/etc/arctern/arctern.toml` on the laptop:
   - Add the other three filesystems under each push job's
     `[[jobs.filesystems]]`
   - Add them under the snap job too
3. Restart arctern: `systemctl start arctern`.
4. Run for another week. Verify all four filesystems land on the
   server.
5. **Now** disable zrepl's push jobs on the laptop:
   ```bash
   # Edit /etc/zrepl/zrepl.yml — comment out push_to_local and push_to_remote
   systemctl restart zrepl   # zrepl now only snapshots, no longer pushes
   ```
6. Verify arctern still pushes after zrepl stops pushing (sanity: no
   shared state, but worth checking).
7. After another week of arctern-only push: switch the prefix.
   - Stop arctern on laptop: `systemctl stop arctern`.
   - Edit `/etc/arctern/arctern.toml`: change `prefix = "arctern_"` to
     `prefix = "zrepl_"` everywhere (snap job + both snapshot_filters).
   - Disable zrepl entirely on the laptop:
     `systemctl disable --now zrepl`. Optionally `pacman -R zrepl`.
   - Start arctern: `systemctl start arctern`. arctern now adopts the
     existing `zrepl_*` snapshot history and continues from there.
8. On the server: same prefix switch, then disable zrepl's `databak` /
   `rootbak` (the receiver-side snap jobs zrepl was running). Keep
   zrepl's `from_remote` sink running for ONE more cycle so any in-flight
   push from the laptop doesn't get dropped, then stop zrepl.

## Rollback

At any phase:

```bash
# On laptop — stop arctern, zrepl resumes full duty:
systemctl stop arctern
systemctl disable arctern

# On server — same:
systemctl stop arctern
systemctl disable arctern
# zrepl never stopped, so it's already handling everything.

# Cleanup arctern_-prefixed snapshots if you want a clean state:
zfs list -t snapshot -r novafs/arch0 | awk '/arctern_/ {print $1}' | \
  xargs -r -n1 zfs destroy
zfs list -t snapshot -r okdata/backups/arctern_trial | awk '/arctern_/ {print $1}' | \
  xargs -r -n1 zfs destroy
zfs destroy -r okdata/backups/arctern_trial   # only if the trial root is empty / abandoned
```

The TLS cert at `/var/lib/arctern/cert.pem` is regenerated on next
start; safe to leave or delete.

## Known limitations carried into this deployment

- **No cursor/state persistence on sender.** Receiver state is the
  source of truth (Approach B). If receiver loses data, sender's next
  cycle re-LISTs and falls back to full send. No manual cursor reset
  needed (this is an improvement over zrepl's bookmark-based design).
- **One peer per push job.** Connection migration is not implemented.
  Two push jobs (one per peer IP) is the workaround.
- **No metrics endpoint.** Only `GET /api/v1/jobs` for status. Wrap in
  a node_exporter textfile cron if you want Prometheus.
- **No graceful drain on shutdown.** A cycle in flight when systemd
  sends SIGTERM will be killed mid-stream; receiver gets a partial,
  next cycle resumes from the token. Should work — exercised by slice
  006 integration tests at small scale, untested at TB scale.
- **`last_error` is cycle-level summary text.** Per-fs detail is not
  tracked. If a cycle pushes 4 filesystems and 1 fails, you see "fs X:
  <error message>" but the other 3 successes aren't itemized.
- **Sink IP allowlist not implemented.** WireGuard `AllowedIPs` is the
  only network-level access control. If WG is misconfigured, anyone
  on the underlay can connect to `:8889`. Verify your WG `AllowedIPs`
  before exposing this on a public IP.
- **No SIGHUP / config hot-reload.** Edit + `systemctl restart arctern`.

## What NOT to do

- Do not switch the prefix to `zrepl_` while zrepl is still running.
  Both daemons will see the snapshots as theirs to prune; you will
  lose data.
- Do not collapse Phase 1 + Phase 2 into a single "install everything
  at once" step. Without the server sink running first, the laptop
  push will fail at QUIC handshake and you won't know if it's a config
  problem or a network problem.
- Do not skip the trial period. arctern has zero production hours
  before this deployment.
- Do not delete the zrepl config until at least 2 weeks of arctern
  running side-by-side without errors. Easier to roll back from
  `systemctl disable arctern` than to reconstruct a zrepl config.
