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
/// SSE frame, preceded by a replay of the most recent events so a
/// freshly opened page shows context instead of an empty feed until
/// something new happens.
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
    // Subscribe BEFORE reading the backlog so nothing falls between
    // them; live events already present in the backlog are dropped by
    // the id filter below.
    let rx = state.events.subscribe();
    let backlog = crate::state::log_events::recent(&state.state, 100)
        .await
        .unwrap_or_default();
    let last_backlog_id = backlog.last().map(|r| r.id as u64).unwrap_or(0);
    let backlog_frames: Vec<Result<Event, Infallible>> = backlog
        .into_iter()
        .map(|row| {
            Ok(serialise(&LogEvent {
                id: row.id as u64,
                timestamp: row.timestamp,
                level: row.level,
                job_name: row.job_name,
                message: row.message,
            }))
        })
        .collect();
    let live = BroadcastStream::new(rx).filter_map(move |r| match r {
        Ok(ev) if ev.id > last_backlog_id => Some(Ok(serialise(&ev))),
        // Duplicate of a backlog row, or Lagged (the broadcast dropped
        // frames because this subscriber was slow) — skip silently.
        _ => None,
    });
    // End the stream on daemon shutdown, or graceful shutdown would
    // wait forever on the browser's open EventSource.
    let stream = futures_util::StreamExt::take_until(
        futures_util::stream::iter(backlog_frames).chain(live),
        state.shutdown.clone().cancelled_owned(),
    );
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
pub struct RecentEventsQuery {
    /// Maximum rows, newest kept, returned oldest-first. Default 100.
    pub limit: Option<i64>,
}

/// JSON tail of the event log. The SSE stream carries live data; this
/// endpoint exists for backlog replay — in particular the peer-events
/// bridge fetches it through the generic proxy so a freshly opened
/// peer console shows context instead of an empty feed.
#[utoipa::path(
    get,
    path = "/api/v1/events/recent",
    tag = "events",
    params(RecentEventsQuery),
    responses(
        (status = 200, description = "Most recent log events, oldest first",
         body = Vec<LogEvent>),
    ),
)]
pub async fn recent_events(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<RecentEventsQuery>,
) -> axum::Json<Vec<LogEvent>> {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = crate::state::log_events::recent(&state.state, limit)
        .await
        .unwrap_or_default();
    axum::Json(
        rows.into_iter()
            .map(|row| LogEvent {
                id: row.id as u64,
                timestamp: row.timestamp,
                level: row.level,
                job_name: row.job_name,
                message: row.message,
            })
            .collect(),
    )
}

fn serialise(ev: &LogEvent) -> Event {
    let payload = serde_json::to_string(ev).unwrap_or_else(|_| "{}".into());
    Event::default().id(ev.id.to_string()).data(payload)
}
