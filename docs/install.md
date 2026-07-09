# Installing arctern

A complete walkthrough from nothing to a working sender → receiver pair.
Everything here is done by hand on purpose: arctern deliberately has no
installer that touches your SSH or ZFS configuration — you should see
every key, every `authorized_keys` line, and every dataset it will use.

Terminology used throughout:

- **sender** — the machine that has the data and drives replication
  (a laptop, a workstation). Runs the `arctern` daemon under systemd.
- **receiver** — the machine that stores the backups (a NAS, a server).
  Needs only `sshd` and the `arctern` binary; running the daemon there
  is optional.

Both need OpenZFS ≥ 2.2 (`zfs`/`zpool` on `PATH`) and OpenSSH.

## 1. Install the binary (both hosts)

Releases ship static musl binaries with the web UI embedded — no
libraries, no glibc version requirements, nothing else to install:

```sh
arch=$(uname -m)   # x86_64 or aarch64
curl -LO "https://github.com/okhsunrog/arctern/releases/latest/download/arctern-${arch}-linux-musl.tar.gz"
curl -LO "https://github.com/okhsunrog/arctern/releases/latest/download/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf "arctern-${arch}-linux-musl.tar.gz"
sudo install -m 755 arctern /usr/local/bin/arctern
arctern --version
```

Repeat on both hosts. That is the entire "installation" — the rest of
this document is configuration.

## 2. Sender setup

### 2a. A dedicated SSH key

The daemon opens SSH sessions non-interactively, so it needs a key
without a passphrase — and it should be a *dedicated* key, so the
receiver's forced command (below) constrains exactly this key and
nothing you use interactively:

```sh
sudo install -d -m 0700 /var/lib/arctern/ssh
sudo ssh-keygen -t ed25519 -N "" -C "arctern@$(hostname)" \
  -f /var/lib/arctern/ssh/id_ed25519
```

Keep the printed public key handy — the receiver step installs it.

### 2b. An SSH host alias

arctern hands `ssh_target` verbatim to the system `ssh(1)`, so the
clean way to configure the connection is a `Host` alias. The daemon
runs as root, so the alias lives in `/root/.ssh/config`:

```
Host arctern-nas
    HostName nas.example.lan
    User root
    IdentityFile /var/lib/arctern/ssh/id_ed25519
    IdentitiesOnly yes
    ControlMaster auto
    ControlPath /var/lib/arctern/ssh/cm-%r@%h:%p
    ControlPersist 10m
```

Notes:

- `IdentitiesOnly yes` matters: without it ssh offers every agent key
  first, which racks up auth failures (and gets you banned by fail2ban
  on hosts that run it).
- `ControlMaster`/`ControlPersist` let the control channel, the bulk
  recv streams, and the events stream share one TCP connection.
- Anything `ssh(1)` supports works here — `ProxyJump`, a WireGuard
  address, a nonstandard port. Multi-homed receivers get one alias per
  path (see `routes` below).

### 2c. The config

`/etc/arctern/arctern.toml`:

```toml
state_dir = "/var/lib/arctern"
socket = "/run/arctern/arctern.sock"

[defaults]
prefix = "arctern_"              # snapshot tag: arctern_2026-07-09T120000Z
[defaults.snapshotting]
interval = "15m"
[defaults.pruning]
grid = "4x15m(keep=all) | 24x1h | 3x1d"   # local fade-out

# One peer = one physical receiver. Multiple routes = multiple network
# paths to it, highest priority first; the link picks the best
# reachable one and re-ranks automatically. `auto = false` marks a
# route that must never carry scheduled syncs (a metered link) —
# manual "Send now" still works over it.
[[peers]]
name = "nas"
auto_interval = "1d"             # scheduled sync at most once a day
[[peers.routes]]
name = "lan"
ssh_target = "arctern-nas"       # the Host alias from 2b
# [[peers.routes]]
# name = "wg"
# ssh_target = "arctern-nas-wg"
# auto = false

# Snapshot the interesting filesystems every 15 minutes.
[[jobs]]
type = "snap"
name = "snap_local"
# "tank/data/" (trailing slash) = the subtree, recursively;
# a plain path = just that dataset. `false` excludes.
filesystems = { "tank/data/" = true, "tank/data/scratch" = false }

# Replicate them to the receiver.
[[jobs]]
type = "push"
name = "push_to_nas"
targets = ["nas"]
parallel = 2                     # filesystems replicated concurrently
# bandwidth_limit = "10MiB"      # shared cap across parallel sends
filesystems = { "tank/data/" = true, "tank/data/scratch" = false }
[jobs.target]
root_fs = "backup/mylaptop"      # receiver-side subtree; datasets land
                                 # at backup/mylaptop/tank/data
```

Snapshots default to encrypted raw sends (`zfs send -w -e -c -L`);
see [`example-config.toml`](example-config.toml) for every knob,
including per-job send flags, prune jobs, and manual-snapshot
protection.

Validate before starting anything:

```sh
sudo arctern configcheck /etc/arctern/arctern.toml
```

### 2d. The systemd service

The repo ships a hardened unit —
[`packaging/systemd/arctern.service`](../packaging/systemd/arctern.service):

```sh
sudo curl -o /etc/systemd/system/arctern.service \
  https://raw.githubusercontent.com/okhsunrog/arctern/main/packaging/systemd/arctern.service
sudo systemctl daemon-reload
sudo systemctl enable --now arctern
sudo journalctl -fu arctern
```

It runs as root (required for `zfs(8)` and the replication key),
manages `/var/lib/arctern` and `/run/arctern` via
`StateDirectory`/`RuntimeDirectory`, and applies the usual sandboxing
(`ProtectSystem=full`, `NoNewPrivileges`, ...).

The console is now at `http://127.0.0.1:7878/` — loopback only, by
design. The daemon creates a private administrator token on first start;
retrieve it with:

```sh
sudo cat /var/lib/arctern/admin.token
```

Paste that token into the login screen. Browser sessions last 12 hours and
are revoked whenever the daemon restarts. From another machine, tunnel the
console as usual; authentication still applies through the tunnel:

```sh
ssh -L 7878:127.0.0.1:7878 you@sender
```

The push job will show the peer as unreachable until the receiver side
below is done — that's expected.

## 3. Receiver setup

The receiver runs **no arctern service** for replication. sshd spawns
one short-lived `arctern` process per SSH channel, and an ACL in the
receiver's config decides what each identity may do. Two files to
edit, both by hand:

### 3a. authorized_keys with a forced command

Append the sender's public key (from 2a) to
`/root/.ssh/authorized_keys` — prefixed with a forced command:

```
command="/usr/local/bin/arctern stdinserver-dispatch mylaptop",restrict ssh-ed25519 AAAA...the-senders-key... arctern@mylaptop
```

What this line does:

- `command="..."` — whatever the client asks to run, sshd runs *this*
  instead. The key cannot open a shell, copy files, or do anything but
  talk to arctern's dispatcher.
- `mylaptop` is the **identity**: the name this key gets in the ACL
  below. One line (and ideally one key) per sender.
- `restrict` disables port forwarding, X11, agent forwarding, PTY —
  every channel feature except the command itself.

The real requested operation (`arctern stdinserver <job> <op>`)
arrives in `SSH_ORIGINAL_COMMAND`; the dispatcher parses it and checks
it against the ACL. A stolen key can therefore do exactly what the ACL
grants — nothing else.

### 3b. The ACL config

`/etc/arctern/arctern.toml` on the receiver:

```toml
state_dir = "/var/lib/arctern"

[[allowed_clients]]
identity = "mylaptop"            # matches the forced command's argument
jobs = ["push_to_nas"]           # sender job names this key may drive
operations = [
  "control",                          # read RPC + events stream
  "control:discard_partial_recv",     # clear stale partial receives
  "recv",                             # receive replication streams
]
root_fs = "backup/mylaptop"      # recv is confined to this subtree
```

Then pre-create the receive root so the first stream has a parent:

```sh
sudo zfs create -p -o canmount=off backup/mylaptop
sudo arctern configcheck /etc/arctern/arctern.toml
```

That's it — replication works now. Trigger it from the sender's
console ("Send now" on the push job) or wait for the schedule, and
watch `journalctl -t sshd` / the sender's Events view.

### 3c. Optional: fingerprint pinning

Defense in depth for the case where an attacker can write to
`authorized_keys` but not to `/etc/arctern`: pin the key fingerprint
in the ACL and the dispatcher re-verifies it on every connection.

```toml
# in the [[allowed_clients]] entry:
fingerprint = "SHA256:..."       # ssh-keygen -lf id_ed25519.pub
```

This needs sshd to expose auth info (off by default):

```sh
echo 'ExposeAuthInfo yes' | sudo tee /etc/ssh/sshd_config.d/50-arctern.conf
sudo systemctl reload sshd
```

### 3d. Optional: the receiver's own daemon

Run `arctern daemon` on the receiver too (same unit as 2d) if you
want any of:

- **its own snap/prune jobs** — e.g. pruning what it received on its
  own retention grid, or snapshotting its own datasets;
- **a local console** on the receiver (`http://127.0.0.1:7878/`);
- **managing the receiver from the sender's console**: the sender
  proxies the receiver's API over the SSH control channel, so the
  receiver shows up as a host in the sender's sidebar. Read-only view
  comes with the `control` scope; full management (create snapshots,
  start scrubs, wake jobs) additionally needs:

```toml
socket = "/run/arctern/arctern.sock"   # top-level: where dispatch finds the daemon
# and in [[allowed_clients]].operations:
#   "control:proxy_admin"
```

A receiver-side prune job for the received tree, protecting whatever
the sender still needs via holds (arctern places those automatically):

```toml
[[jobs]]
type = "prune"
name = "received_prune"
interval = "1h"
filesystems = { "backup/mylaptop" = true }
[[jobs.pruning.keep]]
type = "grid"
grid = "1x1h(keep=all) | 24x1h | 14x1d | 8x1w"
regex = "^arctern_.*"
[[jobs.pruning.keep]]
type = "regex"
regex = "^arctern_.*"
negate = true                    # never touch foreign/manual snapshots
```

### Root or a dedicated user?

The examples above use root on the receiver — simplest, and `zfs recv`
needs broad delegation anyway. To avoid remote root, create a system
user, give the key to it instead, and delegate exactly the receive
subtree:

```sh
sudo useradd --system --create-home arctern-recv
sudo zfs allow -u arctern-recv create,mount,receive,hold,release,destroy backup/mylaptop
```

The forced-command line then goes to
`/home/arctern-recv/.ssh/authorized_keys`, and the sender's Host alias
sets `User arctern-recv`. Mounting received filesystems and some
property operations may still require root depending on your ZFS
version and `zfs allow` coverage — test the first full cycle.

## 4. Verify

On the sender:

```sh
# Peer connected?
curl -s --unix-socket /run/arctern/arctern.sock http://localhost/api/v1/peers | jq
# Trigger a sync now:
curl -s -X POST --unix-socket /run/arctern/arctern.sock \
  http://localhost/api/v1/jobs/push_to_nas/push/nas
```

On the receiver, the datasets appear under `root_fs` with the same
snapshot GUIDs as the sender:

```sh
zfs list -r backup/mylaptop -t all
zfs get -H -o value guid tank/data@arctern_<TAG>            # sender
zfs get -H -o value guid backup/mylaptop/tank/data@arctern_<TAG>  # receiver — must match
```

Everything after this point is day-to-day operation through the
console: job cards with live transfer progress, per-dataset snapshot
browser, pools and scrub control, the live event feed — for the local
host and every configured peer.

## 5. Updating

```sh
# download + checksum as in step 1, then:
sudo install -m 755 arctern /usr/local/bin/arctern
sudo systemctl restart arctern       # senders / receivers running the daemon
```

Receivers without a daemon need no restart — sshd spawns the new
binary on the next connection. Check the job cards first (or
`GET /api/v1/jobs`) so a restart doesn't interrupt a running transfer;
an interrupted one resumes via `recv -s` partial state, but there's no
reason to make it.

## Troubleshooting

- **"requires SSH key fingerprint verification, but no SSH auth info
  is available"** — you set `fingerprint` in the ACL but sshd doesn't
  have `ExposeAuthInfo yes` (see 3c).
- **Peer unreachable, ssh works interactively** — the daemon runs as
  root: the Host alias must be in `/root/.ssh/config`, and the key
  readable by root. Test exactly what the daemon does:
  `sudo ssh -o BatchMode=yes arctern-nas true`.
- **Auth failures / fail2ban bans** — add `IdentitiesOnly yes` to the
  Host alias so ssh offers only the dedicated key.
- **"destination ... has been modified"** on first setup — something
  wrote to the received dataset (mounted it, touched atime). Received
  trees should stay unmounted; `canmount=off` on the parent (3b) plus
  arctern's `recv -u` handle this by default.
- **Identity refused for job/op** — the `jobs` list in the ACL must
  contain the *sender's* push job name, and `operations` must cover
  what the sender asks (see 3b; the daemon's journal on the receiver
  names the missing grant).
