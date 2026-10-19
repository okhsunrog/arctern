# NixOS

`nix build` builds the Arctern daemon and embeds the real administration SPA.
Crane keeps compiled Rust dependencies separate from application sources; the
Bun dependency output is checksum-pinned. SQLx uses the committed `.sqlx/`
metadata, so no development database or network is needed inside the build.
The existing application and transport configuration formats are unchanged.

```nix
# flake inputs
inputs.arctern.url = "github:okhsunrog/arctern";
inputs.arctern.inputs.nixpkgs.follows = "nixpkgs";

# Installing the binary without starting replication
environment.systemPackages = [ arctern.lib.mkPackage pkgs ];

# Optional: import arctern.nixosModules.default and enable the daemon
services.arctern = {
  enable = true;
  configFile = "/etc/arctern/arctern.toml";
};
```

`configFile` is a runtime path; do not put SSH private keys or access tokens in
Nix expressions. State stays under `/var/lib/arctern` and the UNIX socket under
`/run/arctern`. Enabling the service starts the configured jobs: review snapshot,
replication and prune policies before enabling an imported configuration.
Installing only the package does not start a daemon or alter ZFS datasets.

The module retains the packaged systemd unit's restrictions and stop timeout.
It supplies the host's configured ZFS package in the service PATH. The executable
wrapper supplies OpenSSH and basic tools, while leaving ZFS selection to the
running host rather than bundling an unrelated kernel's ZFS userland.

Builds run the Rust workspace tests without the opt-in `integration` feature.
Real replication, forced SSH commands and dataset permissions remain covered by
the separate disposable-ZFS integration workflow described in `AGENTS.md`.
To update dependencies, use the normal Cargo/Bun workflows and commit both
locks; update the Bun output hash in `nix/frontend.nix` when its lock changes.
