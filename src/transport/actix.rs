//! Actix-web WebSocket adapter.
//!
//! Wraps `actix_ws::Session` + `actix_ws::MessageStream` as a Rift
//! `TransportConnection` via a channel bridge (actix types are
//! `!Send`, so the bridge keeps them on the actix runtime).

use std::net::SocketAddr;

use crate::transport::TransportConnection;
use crate::transport::bridge::spawn_bridge_local;

/// Wrap an actix-web WebSocket pair into a `TransportConnection`.
///
/// The adapter spawns reader/writer tasks on the actix runtime that
/// bridge between the WS and tokio channels. The returned connection
/// is `Send` and can be passed to `RiftServer::accept_and_spawn()`.
///
/// ```ignore
/// use actix_web::{web, HttpRequest, HttpResponse, Error};
///
/// async fn handler(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
///     let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
///     let peer = req.peer_addr().ok();
///     let conn = rift::transport::actix::into_connection(session, msg_stream, peer);
///     tokio::spawn(async move {
///         rift_server.accept_and_spawn(conn);
///     });
///     Ok(res)
/// }
/// ```
pub fn into_connection(
    session: actix_ws::Session,
    mut stream: actix_ws::MessageStream,
    peer: Option<SocketAddr>,
) -> Box<dyn TransportConnection> {
    spawn_bridge_local(
        peer,
        256,
        // Reader task: pull messages from actix stream → send to tokio channel.
        move |tx| {
            actix_web::rt::spawn(async move {
                use futures_util::StreamExt;
                while let Some(msg) = stream.next().await {
                    let raw = match msg {
                        Ok(actix_ws::Message::Binary(bin)) => {
                            let mut v = Vec::with_capacity(1 + bin.len());
                            v.push(b'B');
                            v.extend_from_slice(&bin);
                            v
                        }
                        Ok(actix_ws::Message::Text(text)) => {
                            let mut v = Vec::with_capacity(1 + text.len());
                            v.push(b'T');
                            v.extend_from_slice(text.as_bytes());
                            v
                        }
                        Ok(actix_ws::Message::Close(_)) => vec![b'C'],
                        Ok(_) => continue, // skip ping/pong/nop/continuation
                        Err(_) => break,
                    };
                    if tx.send(raw).await.is_err() {
                        break;
                    }
                }
            });
        },
        // Writer task: receive from tokio channel → write to actix session.
        move |mut rx| {
            let mut session = session;
            actix_web::rt::spawn(async move {
                while let Some(raw) = rx.recv().await {
                    if raw.first() == Some(&b'C') {
                        drop(session.close(None));
                        break;
                    }
                    // Skip the tag byte; write the payload.
                    if raw.len() > 1 {
                        drop(session.binary(raw[1..].to_vec()));
                    }
                }
            });
        },
    )
}
