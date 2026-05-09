//! Thin async client over the arctern HTTP API.
//!
//! Used by daemon-to-daemon code (later slices) and by integration tests.
//! Wire types come from [`arctern_api`] so client and server cannot drift.

use arctern_api::DatasetSummary;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("HTTP transport: {0}")]
    Http(#[from] reqwest::Error),

    #[error("decode response body: {0}")]
    Decode(#[from] serde_json::Error),

    #[error("server returned status {code}: {body}")]
    Status { code: u16, body: String },
}

/// `GET <base>/api/v1/datasets`. Returns the list of datasets visible to
/// the daemon's `palimpsest` runner.
pub async fn list_datasets(base: &str) -> Result<Vec<DatasetSummary>, ClientError> {
    let url = format!("{base}/api/v1/datasets");
    let resp = reqwest::get(&url).await?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(ClientError::Status {
            code: status.as_u16(),
            body,
        });
    }
    let datasets: Vec<DatasetSummary> = resp.json().await?;
    Ok(datasets)
}
