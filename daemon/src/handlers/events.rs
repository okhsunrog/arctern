//! `GET /api/v1/events` — SSE stream of locally-generated log events.

use std::convert::Infallible;
use std::time::Duration;

use arctern_api::LogEvent;
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::{
        Sse,
        sse::{Event, KeepAlive},
    },
};
use futures_util::stream::Stream;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

use crate::app_state::AppState;

#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
pub struct StreamEventsQuery {
    /// Resume after this event id. A client that already holds part of
    /// the log passes its newest id so the replay is not re-delivered.
    pub since: Option<i64>,
}

/// The resume cursor, from either the standard SSE header the browser
/// sets on its own retry, or the query parameter — which is what a
/// client that opens a *fresh* EventSource has to use, since the header
/// only travels on the browser's automatic reconnect of the same object.
fn resume_from(headers: &HeaderMap, query: &StreamEventsQuery) -> i64 {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .or(query.since)
        .unwrap_or(0)
        .max(0)
}

/// Subscribe to the daemon's log-event broadcast and yield each as an
/// SSE frame, preceded by a replay of the most recent events so a
/// freshly opened page shows context instead of an empty feed until
/// something new happens. A reconnecting client that names its cursor
/// gets only what it missed.
#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    params(StreamEventsQuery),
    responses(
        (status = 200, description = "SSE stream of LogEvent JSON frames"),
    ),
)]
pub async fn stream_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<StreamEventsQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // Subscribe BEFORE reading the backlog so nothing falls between
    // them; live events already present in the backlog are dropped by
    // the id filter below.
    let rx = state.events.subscribe();
    let since = resume_from(&headers, &query);
    let backlog = crate::state::log_events::recent_since(&state.state, since, 100)
        .await
        .unwrap_or_default();
    // Everything the client already has: what the replay just sent, or —
    // when the replay was empty because the client is caught up — the
    // cursor it named. Without the cursor here a resumed stream would
    // re-deliver anything the broadcast still had buffered.
    let last_backlog_id = backlog
        .last()
        .map(|r| r.id as u64)
        .unwrap_or(0)
        .max(since as u64);
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(last_event_id: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(v) = last_event_id {
            h.insert("last-event-id", HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn a_fresh_client_gets_the_whole_replay() {
        assert_eq!(
            resume_from(&headers(None), &StreamEventsQuery::default()),
            0
        );
    }

    // What the browser sets when it retries an EventSource itself.
    #[test]
    fn the_standard_header_is_honoured() {
        assert_eq!(
            resume_from(&headers(Some("41")), &StreamEventsQuery::default()),
            41
        );
    }

    // Our own reconnects build a NEW EventSource — on tab wake and on
    // `online` — and a fresh one sends no header, so the cursor has to
    // be able to travel in the URL.
    #[test]
    fn the_query_parameter_covers_a_deliberate_reconnect() {
        let q = StreamEventsQuery { since: Some(41) };
        assert_eq!(resume_from(&headers(None), &q), 41);
    }

    #[test]
    fn a_malformed_cursor_replays_rather_than_skipping() {
        for bad in ["not a number", "-7"] {
            let h = headers(Some(bad));
            assert_eq!(
                resume_from(&h, &StreamEventsQuery::default()),
                0,
                "{bad:?} should fall back to a full replay"
            );
        }
    }
}
