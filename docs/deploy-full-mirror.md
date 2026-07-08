# Full-mirror deployment (laptop + server, replicating zrepl)

Goal: replace zrepl's snap+push+sink topology with arctern's snap +
push + SSH-stdinserver topology, end to end:

```
                                      SSH (multi-channel)
laptop (novafs)  ──────── push ──────►  home server (okdata)
   snap_arch0                              databak + rootbak (snap)
   push_to_home (peer = home)              authorized_keys: arctern
                                            stdinserver-dispatch
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
- The `arctern` binary on both hosts was produced by `just build` (so
  the embedded admin UI is up to date with the daemon's API surface).
  Build host needs `bun` + `vp` on `$PATH`; runtime hosts do not.
- Laptop has ZFS pool `novafs/arch0` with the dataset list matching
  your zrepl `push_to_local` job.
- A working SSH path from laptop to server (WireGuard underneath is
  fine — sshd binds inside the WG namespace, OR you reach the server's
  public sshd through a ProxyJump from `~/.ssh/config`. Either is OK;
  arctern doesn't care, openssh handles whatever ssh(1) handles).
- You can `ssh root@<server>` from the laptop without a password
  prompt (key-based auth set up).
- You've decided which ONE filesystem to trial replication with first.
  Pick the lowest-churn one (e.g. `novafs/arch0/data/home` if home
  changes slowly, or pick a dedicated test dataset). Do NOT trial all
  filesystems at once.

## Phase 1 — Server-side ACL + authorized_keys

The server doesn't need a long-lived listener for replication — sshd
spawns one `arctern stdinserver-dispatch` per channel on demand. What
the server DOES need is:

1. The arctern binary in PATH.
2. An entry in the dedicated user's `authorized_keys` with a
   ForcedCommand pointing at `arctern stdinserver-dispatch <identity>`.
3. An `[[allowed_clients]]` row in the server's arctern.toml that
   authorises that identity for the requested operations.

### 1a. Generate a dedicated SSH key on the laptop

A separate key with no passphrase, so the daemon can use it
non-interactively:

```bash
sudo install -d -m 0700 -o root -g root /var/lib/arctern/ssh
sudo ssh-keygen -t ed25519 -N "" -C "arctern@laptop" \
  -f /var/lib/arctern/ssh/id_ed25519
```

### 1b. Create the dedicated user on the server

```bash
sudo useradd --system --create-home --shell /bin/bash arctern-replicator
sudo install -d -m 0700 -o arctern-replicator -g arctern-replicator \
  /home/arctern-replicator/.ssh
```

### 1c. Install the laptop's pubkey with a ForcedCommand

Append to `/home/arctern-replicator/.ssh/authorized_keys` on the server:

```
command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict ssh-ed25519 AAAA...laptop-key arctern@laptop
```

The identity name (`laptop_nova`) is hardcoded per key. `restrict`
disables every channel feature except command exec. The full requested
command (`arctern stdinserver <job> <op>`) arrives via
`SSH_ORIGINAL_COMMAND`; the dispatcher parses it and validates against
the ACL.

```bash
sudo chown arctern-replicator:arctern-replicator \
  /home/arctern-replicator/.ssh/authorized_keys
sudo chmod 0600 /home/arctern-replicator/.ssh/authorized_keys
```

### 1d. Update the server's arctern.toml

Append:

```toml
[[allowed_clients]]
identity = "laptop_nova"     # matches the argv to stdinserver-dispatch
jobs = ["push_to_home_trial"]
# control:discard_partial_recv lets the sender clear stale partial-recv
# state over RPC before opening a fresh recv channel; without it the
# discard still happens (the recv header carries the same directive),
# but every stale-token cycle logs an Unauthorized warning first.
operations = ["control", "control:discard_partial_recv", "recv"]
root_fs = "okdata/backups/arctern_trial/laptop"
```

If you want defense-in-depth: pin the SSH key fingerprint:

```toml
fingerprint = "SHA256:abc123..."   # ssh-keygen -lf .../id_ed25519.pub
```

When `fingerprint` is set, the dispatcher compares it to
`SSH_AUTH_INFO_0` (OpenSSH ≥ 7.4) on every connection.

### 1e. Pre-create the receiving root

Server-side, before the first push lands:

```bash
sudo zfs create -p -o canmount=off okdata/backups/arctern_trial
sudo zfs create    -o canmount=off okdata/backups/arctern_trial/laptop
```

Encryption note: on OpenZFS ≥ 2.4.1, raw-encrypted recv beneath an
encrypted parent succeeds and the received dataset retains its own
encryptionroot and key. No `encryption=off` placeholder is required.
(Verified in the palimpsest test VM.) On OpenZFS < 2.2 you may want
the `encryption=off` parents zrepl historically used; on modern
OpenZFS it's defensive but optional.

### 1f. Reload arctern config on the server

```bash
sudo arctern configcheck /etc/arctern/arctern.toml
sudo systemctl restart arctern
sudo journalctl -fu arctern
```

### 1g. Smoke-test the dispatch path

From the laptop:

```bash
sudo SSH_ORIGINAL_COMMAND="arctern stdinserver push_to_home_trial control" \
  ssh -i /var/lib/arctern/ssh/id_ed25519 \
  arctern-replicator@home-server-host echo ignored
```

(The remote `command=` ignores any argv you pass; sshd records the
real one in `SSH_ORIGINAL_COMMAND`. The dispatcher refuses to do
anything if the parsed `<job>` / `<op>` aren't in the ACL.)

Expected: the connection opens, the dispatcher reads stdin/stdout in
length-delimited frame mode, then closes when you Ctrl-C. No errors
in the server's `journalctl -fu arctern` apart from EOF on the
control channel.

## Phase 2 — Laptop side, snap + push for ONE filesystem

Build + install on the laptop the same way as the server (see
`deploy-snap-only.md` §1–§2). Same systemd unit; same paths.

Configure SSH so the daemon picks up the dedicated key. Easiest is
a host alias in `/root/.ssh/config` (or whatever user the daemon
runs as):

```
Host arctern-home
    HostName home-server-host
    User arctern-replicator
    IdentityFile /var/lib/arctern/ssh/id_ed25519
    IdentitiesOnly yes
    ControlMaster auto
    ControlPath /var/lib/arctern/ssh/cm-%r@%h:%p
    ControlPersist 10m
```

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

# SSH peer the push job pushes to. `ssh_target` is single-route
# shorthand; multi-homed hosts list ordered [[peers.routes]] instead
# (see docs/example-config.toml).
[[peers]]
name = "home"
ssh_target = "arctern-home"   # matches the Host alias above

[[jobs]]
type = "push"
name = "push_to_home_trial"
peer = "home"
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
sudo arctern configcheck /etc/arctern/arctern.toml
sudo systemctl daemon-reload
sudo systemctl start arctern
sudo journalctl -fu arctern
```

zrepl is untouched and still running — it owns `zrepl_*` snapshots and
its own push jobs.

## Phase 3 — Verify the round-trip

Within ~16 minutes (one snap cycle + one push cycle):

On the laptop:
```bash
zfs list -t snapshot novafs/arch0/data/home | grep arctern_
# Expect ≥1 arctern_-prefixed snapshot.

curl -s --unix-socket /run/arctern/arctern.sock \
  http://localhost/api/v1/jobs | jq '.[] | {name, last_error, last_run}'
# Expect snap_trial + push_to_home_trial with last_error:null.

curl -s --unix-socket /run/arctern/arctern.sock \
  http://localhost/api/v1/peers | jq .
# Expect: [{name:"home", reachability:{kind:"connected"}, ...}]
```

Or open the admin UI in a browser (recommended for ongoing monitoring):

```
# Laptop: arctern listens on 127.0.0.1:7878 by default — open it locally.
xdg-open http://127.0.0.1:7878/

# Server: SSH-forward the loopback bind to your workstation.
ssh -L 7879:127.0.0.1:7878 root@server  # 7879 to avoid clashing with the laptop's
# then open http://127.0.0.1:7879/
```

"Peer links" on the laptop should show `home: connected` with the
active route; picking the peer in the sidebar's Hosts group opens the
same console scoped to the server (read-only with the ACL above — add
`control:proxy_admin` to manage it). The Events view streams both
hosts' logs; job detail charts show cycle duration and bytes sent once
a push cycle completes (snap cycles record `bytes_sent = null`).

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
  http://localhost/api/v1/jobs/push_to_home_trial/wakeup
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

Verify the cursor bookmark + step hold mechanics:

```bash
# On the laptop, after a successful push cycle:
zfs list -t bookmark novafs/arch0/data/home
# Expect: novafs/arch0/data/home#arctern_cursor_G_<guid>_J_push_to_home_trial_P_home

zfs holds novafs/arch0/data/home@arctern_<TAG>
# Expect: empty (the step hold was released after the cycle succeeded).
```

## Phase 4 — Trial period

Run for **≥1 week**. zrepl is still running the canonical replication
for everything else; arctern is running in parallel for the trial
dataset (`okdata/backups/arctern_trial/laptop/...`).

Watch for:
- `last_error` accumulating on the push job. SSH transient drops
  should self-heal next cycle thanks to ControlMaster + the eager
  reconnect background task; persistent errors are real bugs.
- Snapshot count drift between sender and receiver — they should be
  identical (modulo the most recent one if a cycle is in flight).
- Resume token activity in the journal: `resuming from token` after a
  network blip should be visible.
- Cursor bookmark advancing on the sender (one entry per push job per
  source dataset; old entries destroyed after each successful cycle).
- Step holds (`arctern_step_J_push_to_home_trial_P_home`) NOT lingering on
  any sender snapshot when no cycle is in flight.
- Receiver disk usage growing as expected.

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
   - Add the other filesystems under the push job's `[[jobs.filesystems]]`
   - Add them under the snap job too
3. Restart arctern: `systemctl start arctern`.
4. Run for another week. Verify all filesystems land on the server.
5. **Now** disable zrepl's push job on the laptop:
   ```bash
   # Edit /etc/zrepl/zrepl.yml — comment out the push job
   systemctl restart zrepl   # zrepl now only snapshots, no longer pushes
   ```
6. Verify arctern still pushes after zrepl stops pushing.
7. After another week of arctern-only push: switch the prefix.
   - Stop arctern on laptop: `systemctl stop arctern`.
   - Edit `/etc/arctern/arctern.toml`: change `prefix = "arctern_"` to
     `prefix = "zrepl_"` everywhere (snap job + snapshot_filter).
   - Disable zrepl entirely on the laptop:
     `systemctl disable --now zrepl`. Optionally `pacman -R zrepl`.
   - Start arctern: `systemctl start arctern`. arctern now adopts the
     existing `zrepl_*` snapshot history and continues from there.
8. On the server: same prefix switch, then disable zrepl's `databak` /
   `rootbak`. Keep zrepl's `from_remote` sink running for ONE more
   cycle so any in-flight push from the laptop doesn't get dropped,
   then stop zrepl.

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

`/var/lib/arctern/state.db` is just an observability log; safe to
leave or delete.

## Known limitations carried into this deployment

- **Pull jobs are not implemented.** Direction is laptop → home server
  only. If you want the home server to also push somewhere else, run a
  separate push job on the home server's daemon.
- **No metrics endpoint.** The HTTP API (`/api/v1/jobs`,
  `/api/v1/transfers/recent`, SSE at `/api/v1/events`) is the
  observability surface. Wrap in a node_exporter textfile cron if you
  want Prometheus.
- **No graceful drain on shutdown.** A cycle in flight when systemd
  sends SIGTERM will be killed mid-stream; receiver gets a partial,
  next cycle resumes from the token.
- **`last_error` is cycle-level summary text.** Per-fs detail is not
  tracked.
- **No SIGHUP / config hot-reload.** Edit + `systemctl restart arctern`.

## What NOT to do

- Do not switch the prefix to `zrepl_` while zrepl is still running.
  Both daemons will see the snapshots as theirs to prune; you will
  lose data.
- Do not collapse Phase 1 + Phase 2 into a single "install everything
  at once" step. Without the server-side ACL + authorized_keys in
  place first, the laptop's push will fail at SSH connect and you
  won't know if it's a config problem or a network problem.
- Do not skip the trial period.
- Do not delete the zrepl config until at least 2 weeks of arctern
  running side-by-side without errors.
