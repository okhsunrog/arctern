# Snap-only deployment (server, side-by-side with zrepl)

Goal: run arctern as the snapshot+pruning daemon on the server (`okdata`)
**alongside** zrepl, on disjoint filesystems, for ~1 week. If the snapshots
arctern produces look identical to zrepl's grid (modulo timing jitter) and
no errors accumulate in the journal, decommission zrepl's `databak` /
`rootbak` jobs and let arctern own snapshotting.

This deployment is snap-only. Push (sender side) and the SSH stdinserver
(receiver side) come in a later wave; see `docs/deploy-full-mirror.md`
for the SSH-transport mirror deployment, and `docs/example-config.toml`
for the full schema.

## Pre-flight

- ZFS pool exists, datasets to back up are imported.
- Server has a Rust toolchain or you have a way to cross-build for it.
- Build host has [`bun`](https://bun.com) and [Vite+ (`vp`)](https://vite.plus)
  on `$PATH` — the admin UI is bundled into the daemon binary at compile
  time, so the build host must produce `admin-ui/dist/` before
  `cargo build`. (Just-the-server hosts do not need bun/`vp`.)
- You have root on the server.
- Decide: which datasets does arctern manage during the trial, and which
  stay with zrepl? They MUST NOT overlap. Suggested split: pick one
  low-traffic dataset (e.g., `okdata/data/nas`) for arctern; leave
  `okdata/data/{root,home}` and `okdata/ROOT/default` with zrepl. Move
  more over as confidence grows.

## 1. Build

On a build host with the same libc as the server:

```bash
cd /path/to/arctern
just build
# Binary is target/release/arctern with admin-ui/dist embedded.
```

`just build` runs `vp install` + `vp build` for the admin UI first, then
`cargo build --release -p arctern-daemon`. The cargo build's `build.rs`
calls `memory_serve::load_directory("admin-ui/dist")` — so the UI
bundle is part of the daemon binary; no separate static-file deploy.

If the server runs musl or a different libc, build with the matching
target after running `just build-ui`:

```bash
just build-ui
cargo build --release --target x86_64-unknown-linux-musl -p arctern-daemon
```

## 2. Install

On the server:

```bash
# Binary
install -m 0755 arctern /usr/local/bin/arctern

# systemd unit
install -m 0644 packaging/systemd/arctern.service /etc/systemd/system/arctern.service

# Config dir (the unit reads /etc/arctern/arctern.toml)
install -d -m 0755 /etc/arctern
```

## 3. Write the trial config

`/etc/arctern/arctern.toml` — a minimal version of `databak` covering ONE
dataset that is currently NOT in zrepl's job list (or that you've removed
from zrepl's job list as part of the cutover). Adjust `path` to match the
dataset you picked above.

```toml
state_dir = "/var/lib/arctern"

[[jobs]]
type = "snap"
name = "arctern_databak_trial"

[[jobs.filesystems]]
path = "okdata/data/nas"

[jobs.snapshotting]
type = "periodic"
interval = "4h"
prefix = "arctern_"   # NOTE: distinct from zrepl_ to avoid pruning collisions

[[jobs.pruning.keep]]
type = "grid"
grid = "6x4h | 14x1d"
regex = "^arctern_.*"

# Anything not matching ^arctern_.* (manual snapshots, zrepl snapshots,
# whatever else) is protected — never touched by this rule.
[[jobs.pruning.keep]]
type = "regex"
regex = "^arctern_.*"
negate = true
```

**Why `arctern_` prefix instead of `zrepl_`**: protects against the
unlikely case where someone misconfigures the trial dataset to overlap
with a zrepl-managed one. The prefix is the firewall — arctern's prune
will only consider its own snapshots. After the trial, if you want
wire-compat with old zrepl history, switch the prefix back to `zrepl_`.

Validate before starting:

```bash
arctern configcheck /etc/arctern/arctern.toml
```

Expected: prints `ok` and exits 0. Any non-zero exit means fix the file.

## 4. Start

```bash
systemctl daemon-reload
systemctl start arctern
systemctl status arctern
```

Expected `status` output:
- Active: active (running)
- Recent journal lines: `arctern daemon listening`, `LISTEN unix:/run/arctern/arctern.sock`, `LISTEN http://127.0.0.1:7878`, `creating snapshot dataset=okdata/data/nas snapshot=arctern_<RFC3339>`

## 4b. Reach the admin UI

The daemon binds a loopback HTTP listener on `127.0.0.1:7878` for the
embedded Vue admin UI. It is not reachable off-host; SSH-forward the
port from your workstation:

```bash
ssh -L 7878:127.0.0.1:7878 root@server
# then open http://127.0.0.1:7878/ in a local browser
```

Retrieve the administrator token on the server and paste it into the login
screen:

```bash
ssh root@server cat /var/lib/arctern/admin.token
```

The UI exposes Dashboard / Jobs / Snapshots / Peers / Events views
backed by the same `/api/v1/*` routes you can `curl --unix-socket`
below. The browser-facing TCP API requires an authenticated session; the
same-UID UNIX socket deliberately does not require HTTP credentials.

## 5. Verify it's actually doing the right thing

Within ~10 minutes (startup-immediate snapshot fires once on first
launch even if the interval hasn't elapsed):

```bash
zfs list -t snapshot okdata/data/nas | grep arctern_
```

Should show one snapshot. Wait 4 hours and re-check; should show two.

Inspect job state via the local API:

```bash
# curl speaks UDS directly with --unix-socket; no socat tunneling needed.
curl -s --unix-socket /run/arctern/arctern.sock http://localhost/api/v1/jobs | jq .
```

(Or use `arctern-client` if you've built it.)

Expected response: one job with `kind = "snap"`, `last_run` populated,
`last_error: null`, `next_run` ~4h in the future.

Tail the journal during normal operation:

```bash
journalctl -fu arctern
```

You should see one cycle every 4h, with snapshot creation and pruning
log lines. Errors should be zero.

## 6. Trial period

Run side-by-side with zrepl for **at least one week** (so the prune
algorithm has time to actually destroy things — first prune can take
24h+ depending on grid math). Watch for:

- `last_error` ever becoming non-null in `GET /api/v1/jobs`
- Snapshots NOT being created on the expected interval
- Snapshots being destroyed that shouldn't be (prune algorithm bug)
- Unexpected disk usage on `/var/lib/arctern` (currently only the SQLite
  state.db, ~tens of KiB; if it grows fast, the trim sweep is broken)
- Daemon RSS climbing (currently no large allocations)

Compare arctern's snapshot timing + retention against zrepl's on the
control datasets:

```bash
# zrepl's view
zfs list -t snapshot -o name,creation okdata/data/home | grep zrepl_
# arctern's view
zfs list -t snapshot -o name,creation okdata/data/nas  | grep arctern_
```

The intervals should match within seconds. The retained set should
follow the same grid math (6 most-recent 4h slots + 14 most-recent
daily slots = 20 retained snapshots in steady state, after >14d).

## 7. Cutover (after the trial passes)

1. Stop arctern: `systemctl stop arctern`
2. Edit `/etc/arctern/arctern.toml`:
   - Add `okdata/data/{root,home}` (and any other zrepl-owned filesystems)
     under `[[jobs.filesystems]]`
   - Optionally rename the job from `arctern_databak_trial` to `databak`
   - Optionally switch `prefix` from `arctern_` to `zrepl_` — at that
     point arctern will adopt zrepl's existing snapshot history. Do this
     ONLY after disabling zrepl's snap jobs (next step), otherwise both
     daemons will fight over the prune.
3. Disable zrepl's `databak` job in `/etc/zrepl/zrepl.yml` (comment it
   out or delete it). Keep the sink job — the SSH-transport push job
   replaces zrepl's QUIC-style replication, see `docs/deploy-full-mirror.md`.
4. `systemctl restart zrepl` (so zrepl drops the snap loop), then
   `systemctl start arctern`.
5. Re-run the verification steps from §5 + §6 for another week with the
   broader dataset list before unifying further.

## Rollback

If anything goes wrong during the trial:

```bash
systemctl stop arctern
systemctl disable arctern
# zrepl is untouched; it owns everything as before. Trial datasets just
# accumulate arctern_-prefixed snapshots until you destroy them by hand:
zfs list -t snapshot okdata/data/nas | awk '/arctern_/ {print $1}' | \
  xargs -n1 zfs destroy
```

`/var/lib/arctern/state.db` is just an observability log; remove the dir
if you want a clean state: `rm -rf /var/lib/arctern`.

## What this deployment does NOT yet cover

- **Replication** (push job + receiver-side stdinserver): see
  `docs/deploy-full-mirror.md` for the SSH-transport mirror.
- **HTTP/HTTPS network exposure**: the API is UDS-only. SSH-tunnel
  to read it remotely; there's no plan to expose it on TCP.
- **Hot reload of config**: no SIGHUP support. To change the config, edit
  the file then `systemctl restart arctern`.
- **Multiple concurrent jobs against the same dataset**: undefined
  behavior. One snap job per dataset.
