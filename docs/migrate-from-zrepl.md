# zrepl → arctern: as-built migration record (nova + mira)

Executed 2026-07-07. This replaces the earlier staged-cutover plan —
the actual migration was a clean-slate cutover: the old zrepl-received
backup tree was destroyed and everything re-pushed from scratch. No
zrepl wire/naming compatibility was kept (the snapshot prefix is now
`arctern_`); zrepl remains installed on both hosts for rollback but is
disabled.

## What runs where

| Host | Role | Jobs |
|---|---|---|
| nova (laptop) | sender | `snapshot_arch0` (snap, 15m), `push_to_mira` (push, 15m, targets mira-lan → mira-wg) |
| mira (server) | receiver + own snaps | `databak` (snap, 4h), `rootbak` (snap, 1d), `received_prune` (prune, 1h) |

Transport: SSH. The laptop's daemon connects as `root@mira` using the
dedicated key `/var/lib/arctern/ssh/id_ed25519` via the host aliases
`arctern-mira-lan` (10.77.77.100) / `arctern-mira-wg` (10.66.66.2)
defined in `/root/.ssh/config`. On mira the key is bound in
`/root/.ssh/authorized_keys` to
`command="/usr/local/bin/arctern stdinserver-dispatch laptop_nova",restrict`.

Receiver tree: `okdata/backups/nova` (push `root_fs`); the laptop's
datasets land at `okdata/backups/nova/novafs/arch0/data/{home,root}`.
`okdata/backups/nova/novafs/archold` is the preserved backup of an old
machine — the `received_prune` job is scoped to exactly the two active
datasets so archold is never touched.

## Gotchas encountered (so you don't rediscover them)

- **Fingerprint pinning needs `ExposeAuthInfo yes`** in sshd_config
  (default is no). Without it every channel dies with "requires SSH key
  fingerprint verification, but no SSH auth info is available".
  Configured via `/etc/ssh/sshd_config.d/50-arctern.conf` on mira.
- **fail2ban on mira bans multi-key SSH clients.** An ssh client that
  offers several agent keys racks up auth failures and gets banned even
  though the final key succeeds. The arctern daemon is immune
  (`IdentitiesOnly yes` + a single key in the Host block); interactive
  users beware. Unban: `fail2ban-client set sshd unbanip <ip>`.
- **openssh crate child lifetime**: the control channel's `Child` must
  be kept alive inside `PeerLink` — both dropping it and
  `disconnect()` tear down the mux channel and the remote dispatcher
  exits on stdin EOF.
- **glibc skew**: nova (Arch, glibc 2.43) builds don't run on mira
  (Debian 13, glibc 2.41). mira gets the static
  `x86_64-unknown-linux-musl` build (`CC_x86_64_unknown_linux_musl=musl-gcc`).

## Leftovers + scheduled cleanup

zrepl's own snapshots are protected by arctern's `protect_non_prefixed`
rule (they don't match `^arctern_`), so they never age out on their own.
After arctern has built up equivalent retention, remove them:

```bash
# nova — after ~3 days of arctern_ history (local grid horizon):
zfs list -H -o name -t snapshot -r novafs/arch0 | grep '@zrepl_' \
  | xargs -r -n1 zfs destroy

# mira — after ~14 days (databak grid horizon):
zfs list -H -o name -t snapshot okdata/data/nas okdata/data/root okdata/data/home okdata/ROOT/default \
  | grep '@zrepl_' | xargs -r -n1 zfs destroy
```

zrepl cursor/step bookmarks on nova (`#zrepl_CURSOR*`) were destroyed
at cutover. zrepl itself is still installed on both hosts; purge the
packages once a few weeks of arctern operation look clean.

## Rollback

`systemctl disable --now arctern && systemctl enable --now zrepl` on
both hosts restores the previous (stale) zrepl setup — but note the
receiver tree for the laptop was wiped at cutover, so zrepl would also
start from a full resend. There is no path back to the pre-migration
receiver state.
