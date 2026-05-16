set shell := ["bash", "-euo", "pipefail", "-c"]

# Shared test VM is managed by palimpsest's justfile (one VM, port 2226,
# both projects' integration tests use it). vm-up / vm-down delegate so
# there's exactly one source of truth.
PALIMPSEST_DIR := env_var('HOME') + "/code/palimpsest"
SSH_PORT := "2226"
SSH_TARGET := "root@localhost:" + SSH_PORT

# Show this list
default:
    @just --list

# ─── Cargo ─────────────────────────────────────────────

check:
    cargo check --workspace

test:
    cargo test --workspace

lint:
    cargo clippy --workspace --all-targets -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

# Full pre-push gate: rust fmt + clippy + tests + UI typecheck/lint.
ci: fmt-check lint test
    cd admin-ui && vp check

# ─── Admin UI ──────────────────────────────────────────

# Install JS deps + typecheck + build the Vue SPA into admin-ui/dist.
# The daemon's build.rs embeds that directory via memory-serve.
build-ui:
    cd admin-ui && vp install && vp exec vue-tsc --build && vp build

# Release artifact: UI bundle first, then cargo release build.
build: build-ui
    cargo build --release -p arctern-daemon

# Regenerate admin-ui/openapi.json + TypeScript client from the live
# router. Re-run whenever crates/api types or handler signatures change.
openapi:
    cargo run -q -p arctern-daemon -- openapi > admin-ui/openapi.json
    cd admin-ui && vp exec openapi-ts

# Vite dev server with hot reload. Proxies /api/v1 and /api-docs to
# the daemon's loopback bind on 127.0.0.1:7878 (start the daemon
# separately in another shell).
ui-dev:
    cd admin-ui && vp dev

# ─── VM lifecycle (delegates to palimpsest) ────────────

vm-up:
    just --justfile {{PALIMPSEST_DIR}}/justfile vm-up

vm-down:
    just --justfile {{PALIMPSEST_DIR}}/justfile vm-down

vm-ssh:
    just --justfile {{PALIMPSEST_DIR}}/justfile vm-ssh

vm-log:
    just --justfile {{PALIMPSEST_DIR}}/justfile vm-log

# Sweep stale palimpsest_test_* pools/files inside the VM.
test-cleanup:
    just --justfile {{PALIMPSEST_DIR}}/justfile test-cleanup

# ─── Integration tests ─────────────────────────────────

# Requires the VM to be running (`just vm-up`).
test-integration:
    PALIMPSEST_SSH_TARGET={{SSH_TARGET}} \
    PALIMPSEST_SSH_PASSWORD="" \
        cargo test -p arctern-daemon --features integration -- --test-threads=1

# Real OpenSSH forced-command control-channel test. Requires the shared VM.
test-openssh: vm-up
    PALIMPSEST_SSH_TARGET={{SSH_TARGET}} \
    PALIMPSEST_SSH_PASSWORD="" \
    ARCTERN_OPENSSH_INTEGRATION=1 \
        cargo test -p arctern-daemon --test integration_openssh_forced_command --features integration -- --nocapture

# One-shot: vm-up + integration + vm-down. For CI / clean checks.
test-vm: vm-up
    just test-integration
    just vm-down
