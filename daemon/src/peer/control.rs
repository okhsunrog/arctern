//! Control-channel client. Owns the framed read+write halves of the
//! remote `arctern stdinserver-dispatch ... control` child and demuxes
//! ResponseFrames by request_id.
//!
//! Public API is intentionally small: callers build a `ControlClient`
//! over any `(reader, writer)` pair (the production case is the
//! channel's stdout/stdin; tests use in-memory duplex pipes), call
//! `send(Request)`, and await the `Response`. Server-pushed Event
//! frames (request_id == None) fan out via a `broadcast::Sender`
//! exposed by `subscribe_events`.

// The full client surface is consumed by handlers/peers in step 10
// and the push executor in step 9. Until then the unit tests below
// are the sole live caller — silence dead-code warnings module-wide.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arctern_transport::{
    EventWire, Request, RequestFrame, Response, ResponseFrame, read_response, write_request,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use super::Pending;

const EVENT_BROADCAST_CAPACITY: usize = 256;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("control channel closed")]
    ChannelClosed,
    #[error("server returned error: {0}")]
    Server(String),
    #[error("transport: {0}")]
    Transport(#[from] arctern_transport::ProtocolError),
}

#[derive(Clone)]
pub struct ControlClient {
    tx: mpsc::Sender<Outbound>,
    events: broadcast::Sender<EventWire>,
    next_id: Arc<AtomicU64>,
}

struct Outbound {
    frame: RequestFrame,
    reply: Pending,
}

impl ControlClient {
    /// Construct a client over a pair of byte streams. Spawns a single
    /// background task that owns both halves and demuxes responses.
    /// Returns the client + the join handle so callers can detect a
    /// terminated background task (and trigger reconnect).
    pub fn spawn<R, W>(reader: R, writer: W) -> (Self, JoinHandle<()>)
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<Outbound>(64);
        let (events_tx, _events_rx) = broadcast::channel::<EventWire>(EVENT_BROADCAST_CAPACITY);
        let next_id = Arc::new(AtomicU64::new(1));
        let pending = Arc::new(Mutex::new(HashMap::<u64, Pending>::new()));
        let task = {
            let events = events_tx.clone();
            let pending = pending.clone();
            tokio::spawn(async move {
                run_loop(reader, writer, rx, pending, events).await;
            })
        };
        let client = Self {
            tx,
            events: events_tx,
            next_id,
        };
        (client, task)
    }

    /// Send a Request and await its Response. Allocates a fresh request
    /// id and routes the matching ResponseFrame back through a oneshot.
    pub async fn send(&self, body: Request) -> Result<Response, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = RequestFrame { id, body };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Outbound {
                frame,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RpcError::ChannelClosed)?;
        match reply_rx.await {
            Ok(res) => res,
            Err(_) => Err(RpcError::ChannelClosed),
        }
    }

    /// Subscribe to server-pushed events. New subscribers see events
    /// emitted after they subscribe; backlog replay is the SSE bridge's
    /// job (queries log_events directly via SQLite).
    pub fn subscribe_events(&self) -> broadcast::Receiver<EventWire> {
        self.events.subscribe()
    }
}

async fn run_loop<R, W>(
    mut reader: R,
    mut writer: W,
    mut rx: mpsc::Receiver<Outbound>,
    pending: Arc<Mutex<HashMap<u64, Pending>>>,
    events: broadcast::Sender<EventWire>,
) where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    loop {
        tokio::select! {
            // Outbound: a caller wants to send a Request.
            out = rx.recv() => {
                let Some(out) = out else {
                    // Last sender dropped; nothing more to write. The
                    // read side stays open in case in-flight responses
                    // are still arriving — but with no senders we can
                    // tear down cleanly.
                    break;
                };
                {
                    let mut p = pending.lock().await;
                    p.insert(out.frame.id, out.reply);
                }
                if let Err(e) = write_request(&mut writer, &out.frame).await {
                    let mut p = pending.lock().await;
                    if let Some(reply) = p.remove(&out.frame.id) {
                        let _ = reply.send(Err(RpcError::Transport(e)));
                    }
                    break;
                }
            }
            // Inbound: server sent a frame.
            frame = read_response(&mut reader) => {
                match frame {
                    Ok(ResponseFrame { request_id: None, body: Response::Event(ev) }) => {
                        let _ = events.send(ev);
                    }
                    Ok(ResponseFrame { request_id: Some(id), body }) => {
                        let mut p = pending.lock().await;
                        if let Some(reply) = p.remove(&id) {
                            let result = match body {
                                Response::Error { code: _, message } => Err(RpcError::Server(message)),
                                other => Ok(other),
                            };
                            let _ = reply.send(result);
                        }
                        // Unmatched id = server bug or stale frame; drop it.
                    }
                    Ok(ResponseFrame { request_id: None, body: _ }) => {
                        // Non-event frame with None id: protocol violation; ignore.
                    }
                    Err(_e) => {
                        // Read side died — peer hung up or stream broke.
                        break;
                    }
                }
            }
        }
    }
    // Drain any in-flight callers with ChannelClosed so they unblock.
    {
        let mut p = pending.lock().await;
        for (_id, reply) in p.drain() {
            let _ = reply.send(Err(RpcError::ChannelClosed));
        }
    }
    // Drain any outbound requests still queued — their oneshot replies
    // would otherwise hang waiting for a response that never comes.
    rx.close();
    while let Some(out) = rx.recv().await {
        let _ = out.reply.send(Err(RpcError::ChannelClosed));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arctern_transport::{ErrorCode, JobStatusWire};
    use tokio::io::duplex;

    /// End-to-end demux smoke: spin a fake "server" task on the other
    /// end of a duplex pipe, send three concurrent requests, have the
    /// server respond out of order, assert the demux routes each to the
    /// matching caller.
    #[tokio::test]
    async fn demux_routes_responses_by_request_id_out_of_order() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_reader, client_writer) = tokio::io::split(client_io);
        let (mut server_reader, mut server_writer) = tokio::io::split(server_io);

        let (client, task) = ControlClient::spawn(client_reader, client_writer);

        // Server: read three requests, reply in reverse order.
        let server = tokio::spawn(async move {
            let mut frames = Vec::new();
            for _ in 0..3 {
                let req: RequestFrame = arctern_transport::read_request(&mut server_reader)
                    .await
                    .unwrap();
                frames.push(req);
            }
            for req in frames.into_iter().rev() {
                let resp = ResponseFrame {
                    request_id: Some(req.id),
                    body: Response::ListJobsOk {
                        jobs: vec![JobStatusWire {
                            name: format!("job-{}", req.id),
                            kind: "snap".into(),
                            last_run: None,
                            next_run: None,
                            last_error: None,
                        }],
                    },
                };
                arctern_transport::write_response(&mut server_writer, &resp)
                    .await
                    .unwrap();
            }
        });

        let r1 = client.send(Request::ListJobs);
        let r2 = client.send(Request::ListJobs);
        let r3 = client.send(Request::ListJobs);
        let (a, b, c) = tokio::join!(r1, r2, r3);
        for r in [a, b, c] {
            let resp = r.unwrap();
            match resp {
                Response::ListJobsOk { jobs } => {
                    assert_eq!(jobs.len(), 1);
                    assert!(jobs[0].name.starts_with("job-"));
                }
                other => panic!("unexpected response {other:?}"),
            }
        }
        server.await.unwrap();
        drop(client);
        let _ = task.await;
    }

    #[tokio::test]
    async fn server_error_response_surfaces_as_rpc_error_server() {
        let (client_io, server_io) = duplex(8192);
        let (client_reader, client_writer) = tokio::io::split(client_io);
        let (mut server_reader, mut server_writer) = tokio::io::split(server_io);
        let (client, _task) = ControlClient::spawn(client_reader, client_writer);

        let server = tokio::spawn(async move {
            let req: RequestFrame = arctern_transport::read_request(&mut server_reader)
                .await
                .unwrap();
            let resp = ResponseFrame {
                request_id: Some(req.id),
                body: Response::Error {
                    code: ErrorCode::Unauthorized,
                    message: "nope".into(),
                },
            };
            arctern_transport::write_response(&mut server_writer, &resp)
                .await
                .unwrap();
        });

        let err = client.send(Request::ListJobs).await.unwrap_err();
        match err {
            RpcError::Server(m) => assert_eq!(m, "nope"),
            other => panic!("expected RpcError::Server, got {other:?}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn event_frames_fan_out_to_subscribers() {
        let (client_io, server_io) = duplex(8192);
        let (client_reader, client_writer) = tokio::io::split(client_io);
        let (_server_reader, mut server_writer) = tokio::io::split(server_io);
        let (client, _task) = ControlClient::spawn(client_reader, client_writer);
        let mut sub = client.subscribe_events();

        let ev = EventWire {
            id: 7,
            timestamp: 1715200000,
            level: "INFO".into(),
            job_name: Some("backup".into()),
            message: "hello".into(),
        };
        let frame = ResponseFrame {
            request_id: None,
            body: Response::Event(ev.clone()),
        };
        arctern_transport::write_response(&mut server_writer, &frame)
            .await
            .unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(1), sub.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, ev);
    }

    #[tokio::test]
    async fn read_side_eof_unblocks_inflight_callers() {
        let (client_io, server_io) = duplex(8192);
        let (client_reader, client_writer) = tokio::io::split(client_io);
        // Drop the entire server end so both halves close — client_reader
        // sees EOF, client_writer sees BrokenPipe on next write.
        drop(server_io);

        let (client, task) = ControlClient::spawn(client_reader, client_writer);
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.send(Request::ListJobs),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(err, RpcError::ChannelClosed | RpcError::Transport(_)),
            "got {err:?}"
        );
        let _ = task.await;
    }
}
