//! `arctern configcheck <path>` — load + validate without touching ZFS.

use std::path::Path;

pub fn run(path: &Path) -> eyre::Result<()> {
    match arctern_config::load_from_path(path) {
        Ok(_) => {
            println!("ok");
            Ok(())
        }
        Err(e) => Err(eyre::eyre!("{e}")),
    }
}
