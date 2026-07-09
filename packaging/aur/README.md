# AUR packaging

`arctern-bin/` is the canonical source for the AUR package. The AUR's own
`arctern-bin.git` repository is the publication target, not a second source of
truth.

## Update a release

1. Change `pkgver` and reset `pkgrel` to `1` in `arctern-bin/PKGBUILD`.
2. Copy the release-archive hashes from the release's `SHA256SUMS` into the
   architecture-specific checksum arrays.
3. Refresh the hashes for the version-pinned service, example configuration,
   and upstream license.
4. Build and inspect the package:

   ```sh
   cd packaging/aur/arctern-bin
   makepkg --syncdeps --cleanbuild
   makepkg --printsrcinfo > .SRCINFO
   ```

5. Review and commit `PKGBUILD`, `.SRCINFO`, `arctern-bin.install`, and the
   packaging `LICENSE` here.
6. Copy those four files into a clean clone of
   `ssh://aur@aur.archlinux.org/arctern-bin.git`, commit them, and push its
   `master` branch.

Do not enable or start the service from the package. The operator must first
create `/etc/arctern/arctern.toml`; the example config is installed under
`/usr/share/doc/arctern/`.
