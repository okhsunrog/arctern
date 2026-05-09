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

# One-shot: vm-up + integration + vm-down. For CI / clean checks.
test-vm: vm-up
    just test-integration
    just vm-down
