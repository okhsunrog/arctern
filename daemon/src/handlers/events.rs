//! `GET /api/v1/events` — SSE stream of locally-generated log events.

use std::convert::Infallible;
use std::time::Duration;

use arctern_api::LogEvent;
use axum::{
    extract::State,
    response::{
        Sse,
        sse::{Event, KeepAlive},
    },
};
use futures_util::stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::app_state::AppState;

/// Subscribe to the daemon's log-event broadcast and yield each as an
/// SSE frame. Backlog replay is intentionally out of scope for v1 — the
/// receiver-side SubscribeEvents handler is built around polling
/// log_events directly. UI clients that want history hit a
/// future `/api/v1/events?since=…` endpoint or the SQLite directly.
#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    responses(
        (status = 200, description = "SSE stream of LogEvent JSON frames"),
    ),
)]
pub async fn stream_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.events.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| match r {
        Ok(ev) => Some(Ok(serialise(&ev))),
        // Lagged: the broadcast dropped some frames because this
        // subscriber was slow. Skip them silently — the cursor
        // mechanism is what guarantees no-loss for clients that care.
        Err(_) => None,
    });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

fn serialise(ev: &LogEvent) -> Event {
    let payload = serde_json::to_string(ev).unwrap_or_else(|_| "{}".into());
    Event::default().id(ev.id.to_string()).data(payload)
}
