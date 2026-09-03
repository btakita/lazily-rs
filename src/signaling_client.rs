//! Async Rust client for the #yxjw Cloudflare signaling endpoint (#s0fc).
//!
//! This lets a Rust project (e.g. agent-doc) depend on lazily-rs for
//! distributed peer discovery: connect to the signaling Worker over a
//! WebSocket, join a session, learn the roster, and exchange the WebRTC
//! SDP/ICE handshake (or relay opaque payloads) to reach other peers.
//!
//! The wire protocol is the single source of truth shared with the TypeScript
//! client (`signaling/`); see `SPEC.md` → *Signaling wire protocol*. The
//! [`ClientMessage`] / [`ServerMessage`] enums are `serde`-tagged to match that
//! protocol byte-for-byte (`PeerId` serializes as a bare JSON number, matching
//! the TS `number` peer id), and `signaling_protocol_conformance` tests assert
//! the exact JSON shapes.
//!
//! Enabled by the `signaling-client` feature.

// `SignalingError` is public and changing its WebSocket variant to a box would
// be an API break. Keep the transparent transport error until the next planned
// breaking release; Rust 1.98's new size heuristic does not change correctness.
#![allow(clippy::result_large_err)]

use crate::distributed::PeerId;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

/// A message the client sends to the signaling server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ClientMessage {
    /// Join the session as `peer`, optionally advertising capabilities.
    Join {
        peer: PeerId,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        capabilities: Option<Vec<String>>,
    },
    /// WebRTC offer for `to`.
    Offer { to: PeerId, sdp: String },
    /// WebRTC answer for `to`.
    Answer { to: PeerId, sdp: String },
    /// ICE candidate for `to`.
    Ice { to: PeerId, candidate: String },
    /// Opaque application payload (e.g. a CRDT delta) relayed to `to`.
    Relay { to: PeerId, payload: Value },
    /// Leave the session.
    Leave,
}

/// A message the signaling server sends to the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ServerMessage {
    /// Sent on join: this peer's id and the current roster (excluding self).
    Welcome { peer: PeerId, peers: Vec<PeerId> },
    /// Another peer joined.
    PeerJoined { peer: PeerId },
    /// Another peer left.
    PeerLeft { peer: PeerId },
    /// Forwarded WebRTC offer from `from`.
    Offer { from: PeerId, sdp: String },
    /// Forwarded WebRTC answer from `from`.
    Answer { from: PeerId, sdp: String },
    /// Forwarded ICE candidate from `from`.
    Ice { from: PeerId, candidate: String },
    /// Forwarded opaque payload from `from`.
    Relay { from: PeerId, payload: Value },
    /// Server-side error (e.g. `permission_denied`, `unknown_target`).
    Error { code: String, message: String },
}

/// Errors surfaced by [`SignalingClient`].
#[derive(Debug)]
pub enum SignalingError {
    /// WebSocket transport error.
    WebSocket(tokio_tungstenite::tungstenite::Error),
    /// JSON encode/decode error for a protocol frame.
    Protocol(serde_json::Error),
    /// The server closed the connection.
    Closed,
}

impl std::fmt::Display for SignalingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignalingError::WebSocket(e) => write!(f, "signaling websocket error: {e}"),
            SignalingError::Protocol(e) => write!(f, "signaling protocol error: {e}"),
            SignalingError::Closed => write!(f, "signaling connection closed"),
        }
    }
}

impl std::error::Error for SignalingError {}

impl From<tokio_tungstenite::tungstenite::Error> for SignalingError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        SignalingError::WebSocket(e)
    }
}
impl From<serde_json::Error> for SignalingError {
    fn from(e: serde_json::Error) -> Self {
        SignalingError::Protocol(e)
    }
}

impl ServerMessage {
    /// Decode and validate an untrusted server frame.
    ///
    /// Serde enforces the closed tagged union and exact per-variant fields;
    /// this method additionally enforces semantic constraints that JSON Schema
    /// cannot express, such as a welcome roster excluding the joining peer.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let message: Self = serde_json::from_slice(bytes)?;
        if let Self::Welcome { peer, peers } = &message
            && peers.contains(peer)
        {
            return Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "welcome roster must exclude the joining peer",
            )));
        }
        Ok(message)
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A connected signaling-session client.
///
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use lazily::{PeerId, SignalingClient, ServerMessage};
///
/// let mut client = SignalingClient::connect("wss://signaling.example.com", "room-1", PeerId(1)).await?;
/// while let Some(msg) = client.recv().await {
///     match msg? {
///         ServerMessage::Welcome { peers, .. } => println!("roster: {peers:?}"),
///         ServerMessage::PeerJoined { peer } => {
///             client.relay(peer, serde_json::json!({ "hello": true })).await?;
///         }
///         _ => {}
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct SignalingClient {
    ws: WsStream,
    peer: PeerId,
}

impl SignalingClient {
    /// Connect to `{base_url}/session/{session}` and join as `peer`.
    ///
    /// `base_url` is the Worker origin, e.g. `wss://signaling.example.com`.
    pub async fn connect(
        base_url: &str,
        session: &str,
        peer: PeerId,
    ) -> Result<Self, SignalingError> {
        Self::connect_with_capabilities(base_url, session, peer, None).await
    }

    /// Connect and join, advertising `capabilities` to other peers.
    pub async fn connect_with_capabilities(
        base_url: &str,
        session: &str,
        peer: PeerId,
        capabilities: Option<Vec<String>>,
    ) -> Result<Self, SignalingError> {
        let url = format!("{}/session/{}", base_url.trim_end_matches('/'), session);
        let (ws, _response) = tokio_tungstenite::connect_async(&url).await?;
        let mut client = Self { ws, peer };
        client
            .send(&ClientMessage::Join { peer, capabilities })
            .await?;
        Ok(client)
    }

    /// This client's peer id.
    pub fn peer(&self) -> PeerId {
        self.peer
    }

    /// Send a protocol message to the server.
    pub async fn send(&mut self, message: &ClientMessage) -> Result<(), SignalingError> {
        let json = serde_json::to_string(message)?;
        self.ws.send(Message::Text(json)).await?;
        Ok(())
    }

    /// Send a WebRTC offer to `to`.
    pub async fn offer(
        &mut self,
        to: PeerId,
        sdp: impl Into<String>,
    ) -> Result<(), SignalingError> {
        self.send(&ClientMessage::Offer {
            to,
            sdp: sdp.into(),
        })
        .await
    }

    /// Send a WebRTC answer to `to`.
    pub async fn answer(
        &mut self,
        to: PeerId,
        sdp: impl Into<String>,
    ) -> Result<(), SignalingError> {
        self.send(&ClientMessage::Answer {
            to,
            sdp: sdp.into(),
        })
        .await
    }

    /// Send an ICE candidate to `to`.
    pub async fn ice(
        &mut self,
        to: PeerId,
        candidate: impl Into<String>,
    ) -> Result<(), SignalingError> {
        self.send(&ClientMessage::Ice {
            to,
            candidate: candidate.into(),
        })
        .await
    }

    /// Relay an opaque payload (e.g. a CRDT delta) to `to`.
    pub async fn relay(&mut self, to: PeerId, payload: Value) -> Result<(), SignalingError> {
        self.send(&ClientMessage::Relay { to, payload }).await
    }

    /// Announce departure and close.
    pub async fn leave(&mut self) -> Result<(), SignalingError> {
        self.send(&ClientMessage::Leave).await?;
        self.ws.close(None).await?;
        Ok(())
    }

    /// Receive the next server message. Returns `None` once the connection
    /// closes; control frames (ping/pong/close) are handled internally.
    pub async fn recv(&mut self) -> Option<Result<ServerMessage, SignalingError>> {
        loop {
            match self.ws.next().await? {
                Ok(Message::Text(text)) => {
                    return Some(
                        ServerMessage::from_json_slice(text.as_bytes())
                            .map_err(SignalingError::from),
                    );
                }
                Ok(Message::Binary(bytes)) => {
                    return Some(
                        ServerMessage::from_json_slice(&bytes).map_err(SignalingError::from),
                    );
                }
                Ok(Message::Close(_)) => return None,
                // Ping/Pong/frame: keep waiting for an application frame.
                Ok(_) => continue,
                Err(e) => return Some(Err(SignalingError::WebSocket(e))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Conformance: these exact JSON shapes are the contract shared with the
    // TypeScript client/Worker (`signaling/src/protocol.ts`).
    #[test]
    fn client_messages_match_wire_format() {
        assert_eq!(
            serde_json::to_value(ClientMessage::Join {
                peer: PeerId(1),
                capabilities: None
            })
            .unwrap(),
            json!({ "type": "join", "peer": 1 })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Join {
                peer: PeerId(1),
                capabilities: Some(vec!["crdt".into()]),
            })
            .unwrap(),
            json!({ "type": "join", "peer": 1, "capabilities": ["crdt"] })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Offer {
                to: PeerId(2),
                sdp: "x".into()
            })
            .unwrap(),
            json!({ "type": "offer", "to": 2, "sdp": "x" })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Ice {
                to: PeerId(2),
                candidate: "c".into()
            })
            .unwrap(),
            json!({ "type": "ice", "to": 2, "candidate": "c" })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Relay {
                to: PeerId(2),
                payload: json!({"d": 1})
            })
            .unwrap(),
            json!({ "type": "relay", "to": 2, "payload": { "d": 1 } })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Leave).unwrap(),
            json!({ "type": "leave" })
        );
    }

    #[test]
    fn server_messages_match_wire_format() {
        assert_eq!(
            serde_json::from_value::<ServerMessage>(
                json!({ "type": "welcome", "peer": 1, "peers": [2, 3] })
            )
            .unwrap(),
            ServerMessage::Welcome {
                peer: PeerId(1),
                peers: vec![PeerId(2), PeerId(3)]
            }
        );
        assert_eq!(
            serde_json::from_value::<ServerMessage>(json!({ "type": "peer-joined", "peer": 5 }))
                .unwrap(),
            ServerMessage::PeerJoined { peer: PeerId(5) }
        );
        assert_eq!(
            serde_json::from_value::<ServerMessage>(json!({ "type": "peer-left", "peer": 5 }))
                .unwrap(),
            ServerMessage::PeerLeft { peer: PeerId(5) }
        );
        assert_eq!(
            serde_json::from_value::<ServerMessage>(
                json!({ "type": "relay", "from": 1, "payload": [1, 2] })
            )
            .unwrap(),
            ServerMessage::Relay {
                from: PeerId(1),
                payload: json!([1, 2])
            }
        );
        assert_eq!(
            serde_json::from_value::<ServerMessage>(
                json!({ "type": "error", "code": "permission_denied", "message": "no" })
            )
            .unwrap(),
            ServerMessage::Error {
                code: "permission_denied".into(),
                message: "no".into()
            }
        );
    }

    #[test]
    fn server_message_round_trips() {
        for msg in [
            ServerMessage::Welcome {
                peer: PeerId(1),
                peers: vec![],
            },
            ServerMessage::Offer {
                from: PeerId(2),
                sdp: "s".into(),
            },
            ServerMessage::Answer {
                from: PeerId(2),
                sdp: "s".into(),
            },
            ServerMessage::Ice {
                from: PeerId(2),
                candidate: "c".into(),
            },
        ] {
            let s = serde_json::to_string(&msg).unwrap();
            assert_eq!(serde_json::from_str::<ServerMessage>(&s).unwrap(), msg);
        }
    }
}
