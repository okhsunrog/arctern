//! Asserts the docs/example-config.toml file in the repo parses
//! cleanly. Catches accidental drift between the example and the
//! schema.

use std::path::PathBuf;

#[test]
fn example_config_parses() {
    // CARGO_MANIFEST_DIR points at crates/config/; ../../docs/...
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../docs/example-config.toml");
    let cfg = arctern_config::load_from_path(&p)
        .unwrap_or_else(|e| panic!("failed to load {}: {e}", p.display()));
    let names: Vec<&str> = cfg.jobs.iter().map(|j| j.name()).collect();
    assert!(names.contains(&"databak"), "expected databak job: {names:?}");
    assert!(names.contains(&"rootbak"), "expected rootbak job: {names:?}");
}
