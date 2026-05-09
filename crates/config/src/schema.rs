//! TOML schema for `arctern.toml`. Field shapes are inspired by zrepl's
//! YAML schema but Rust-idiomatic — see `docs/example-config.toml` for
//! the mapping. `#[serde(deny_unknown_fields)]` everywhere so a typo in
//! the operator's file fails loud, not silent.

use std::time::Duration;

use serde::Deserialize;

use crate::grid::GridSpec;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobConfig {
    Snap(SnapJobConfig),
}

impl JobConfig {
    pub fn name(&self) -> &str {
        match self {
            JobConfig::Snap(s) => &s.name,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapJobConfig {
    pub name: String,
    pub filesystems: Vec<FilesystemFilter>,
    pub snapshotting: SnapshottingConfig,
    pub pruning: PruningConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemFilter {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshottingConfig {
    Periodic {
        // humantime accepts "15m", "4h", "1d", etc.
        #[serde(with = "humantime_serde")]
        interval: Duration,
        prefix: String,
    },
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PruningConfig {
    #[serde(default)]
    pub keep: Vec<KeepRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum KeepRule {
    Grid {
        grid: GridSpec,
        regex: String,
    },
    Regex {
        regex: String,
        #[serde(default)]
        negate: bool,
    },
}
