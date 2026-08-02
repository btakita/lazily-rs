//! Glue that drives the networked [`Str0mNet`] WebRTC backend through the #yxjw
//! [`SignalingClient`] relay (#lzwebrtcwire), behind both the `signaling-client`
//! and `webrtc-str0m` features.
//!
//! [`SignalingClient`] speaks the #yxjw wire protocol (SDP offer/answer + trickled
//! ICE candidates over a WebSocket), and [`Str0mNet`] drives one `Rtc` over a real
//! UDP socket — but on their own neither knows about the other. This module is the
//! missing wire: it produces a [`Str0mNet`] offer/answer, pushes the SDP and the
//! local ICE candidate over the signaling channel, and pumps incoming
//! `ServerMessage`s into [`Str0mNet::accept_answer`] / [`Str0mNet::add_remote_candidate`]
//! until the SCTP data channel opens.
//!
//! ```no_run
//! # #[cfg(all(feature = "signaling-client", feature = "webrtc-str0m"))]
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use lazily::{PeerId, SignalingClient, offer_to_peer};
//!
//! let mut client = SignalingClient::connect("wss://signaling.example.com", "room-1", PeerId(1)).await?;
//! // The peer's id is learned from the `welcome` roster / `peer-joined` frames.
//! let net = offer_to_peer(&mut client, PeerId(2), "0.0.0.0:0".parse()?, Duration::from_secs(20)).await?;
//! // `net.channel()` is now a `DataChannel` ready for `WebRtcSink` / `WebRtcSource`.
//! # Ok(())
//! # }
//! ```
//!
//! The caller is responsible for knowing the target peer is present (from the
//! `welcome` roster or a `peer-joined` frame) before [`offer_to_peer`]; offering
//! to an absent peer is dropped by the relay and surfaces here only as a timeout.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::{Instant, timeout};

use crate::distributed::PeerId;
use crate::signaling_client::{ServerMessage, SignalingClient, SignalingError};
use crate::str0m_net::{Str0mNet, Str0mNetError};

/// How often the handshake pump re-checks [`Str0mNet::is_open`] while waiting for
/// the next signaling frame — the channel opens on the driver thread, off the
/// signaling path, so we must poll it rather than block solely on `recv`.
const POLL_TICK: Duration = Duration::from_millis(25);

/// Error from driving a [`Str0mNet`] handshake over a [`SignalingClient`].
///
/// The two transport errors are boxed: each is large enough that an unboxed
/// `Result<_, WebrtcSignalingError>` would trip `clippy::result_large_err`.
#[derive(Debug)]
pub enum WebrtcSignalingError {
    /// The signaling channel failed (WebSocket / protocol error, or closed).
    Signaling(Box<SignalingError>),
    /// The networked str0m backend failed (socket / SDP / ICE / driver).
    Str0mNet(Box<Str0mNetError>),
    /// The signaling connection closed before the handshake completed.
    Closed,
    /// The data channel did not open before the timeout elapsed.
    Timeout,
    /// The signaling server rejected the handshake with an `error` frame
    /// (`permission_denied`, `unknown_target`, …).
    Server {
        /// The server's machine-readable error code.
        code: String,
        /// The server's human-readable message.
        message: String,
    },
}

impl std::fmt::Display for WebrtcSignalingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Signaling(e) => write!(f, "webrtc signaling: {e}"),
            Self::Str0mNet(e) => write!(f, "webrtc signaling str0m: {e}"),
            Self::Closed => write!(f, "webrtc signaling: connection closed mid-handshake"),
            Self::Timeout => write!(f, "webrtc signaling: data channel did not open in time"),
            Self::Server { code, message } => {
                write!(f, "webrtc signaling: server error `{code}`: {message}")
            }
        }
    }
}

impl std::error::Error for WebrtcSignalingError {}

impl From<SignalingError> for WebrtcSignalingError {
    fn from(e: SignalingError) -> Self {
        Self::Signaling(Box::new(e))
    }
}

impl From<Str0mNetError> for WebrtcSignalingError {
    fn from(e: Str0mNetError) -> Self {
        Self::Str0mNet(Box::new(e))
    }
}

/// Offerer side: bind a UDP socket, send the SDP **offer** and local ICE candidate
/// to `peer` over `client`, then pump signaling frames (applying the peer's answer
/// and trickled candidates) until the data channel opens.
///
/// Returns the connected [`Str0mNet`]; obtain a [`DataChannel`](crate::DataChannel)
/// with [`Str0mNet::channel`].
pub async fn offer_to_peer(
    client: &mut SignalingClient,
    peer: PeerId,
    bind: SocketAddr,
    open_timeout: Duration,
) -> Result<Str0mNet, WebrtcSignalingError> {
    let (net, offer_sdp) = Str0mNet::offer(bind)?;
    client.offer(peer, offer_sdp).await?;
    client.ice(peer, net.local_candidate().to_string()).await?;

    let deadline = Instant::now() + open_timeout;
    pump_until_open(client, &net, deadline).await?;
    Ok(net)
}

/// Answerer side: wait for the next SDP **offer** to arrive over `client`, bind a
/// UDP socket and produce the SDP answer, send the answer and local ICE candidate
/// back, then pump until the data channel opens.
///
/// Returns the offering peer's id alongside the connected [`Str0mNet`]. ICE
/// candidates that arrive before the offer (out-of-order delivery) are buffered
/// and applied once the backend exists.
pub async fn answer_next_offer(
    client: &mut SignalingClient,
    bind: SocketAddr,
    open_timeout: Duration,
) -> Result<(PeerId, Str0mNet), WebrtcSignalingError> {
    let deadline = Instant::now() + open_timeout;

    // Wait for the first offer, stashing any candidate that races ahead of it.
    let mut early_candidates: Vec<String> = Vec::new();
    let (peer, offer_sdp) = loop {
        match classify_handshake_frame(recv_before(client, deadline).await?)? {
            HandshakeFrame::Offer { from, sdp } => break (from, sdp),
            HandshakeFrame::Ice { candidate } => early_candidates.push(candidate),
            // Roster / relay frames, and an answer to an offer we never sent, are
            // not part of accepting an offer. A server `error` frame is NOT in
            // this set — `classify_handshake_frame` has already failed on it.
            HandshakeFrame::Answer { .. } | HandshakeFrame::Ignored => {}
        }
    };

    let (net, answer_sdp) = Str0mNet::answer(bind, &offer_sdp)?;
    client.answer(peer, answer_sdp).await?;
    client.ice(peer, net.local_candidate().to_string()).await?;
    for candidate in early_candidates {
        net.add_remote_candidate(&candidate)?;
    }

    pump_until_open(client, &net, deadline).await?;
    Ok((peer, net))
}

/// Receive the next signaling frame, failing with [`WebrtcSignalingError::Timeout`]
/// at `deadline` and [`WebrtcSignalingError::Closed`] if the channel ends.
async fn recv_before(
    client: &mut SignalingClient,
    deadline: Instant,
) -> Result<ServerMessage, WebrtcSignalingError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(WebrtcSignalingError::Timeout);
    }
    match timeout(remaining, client.recv()).await {
        Ok(Some(Ok(msg))) => Ok(msg),
        Ok(Some(Err(e))) => Err(e.into()),
        Ok(None) => Err(WebrtcSignalingError::Closed),
        Err(_elapsed) => Err(WebrtcSignalingError::Timeout),
    }
}

/// Pump signaling frames into the str0m backend until its data channel opens or
/// `deadline` elapses. Re-checks [`Str0mNet::is_open`] at least every [`POLL_TICK`]
/// even when no frame arrives, since the channel opens off the signaling path.
async fn pump_until_open(
    client: &mut SignalingClient,
    net: &Str0mNet,
    deadline: Instant,
) -> Result<(), WebrtcSignalingError> {
    while !net.is_open() {
        let now = Instant::now();
        if now >= deadline {
            return Err(WebrtcSignalingError::Timeout);
        }
        let slice = (deadline - now).min(POLL_TICK);
        match timeout(slice, client.recv()).await {
            Ok(Some(Ok(msg))) => apply_handshake_frame(net, msg)?,
            Ok(Some(Err(e))) => return Err(e.into()),
            Ok(None) => return Err(WebrtcSignalingError::Closed),
            // Poll tick elapsed with no frame: loop and re-check `is_open`.
            Err(_elapsed) => {}
        }
    }
    Ok(())
}

/// What one decoded signaling frame means to an in-flight handshake.
#[derive(Debug)]
enum HandshakeFrame {
    /// A peer is offering us a connection.
    Offer { from: PeerId, sdp: String },
    /// Our offer was answered.
    Answer { sdp: String },
    /// A trickled ICE candidate.
    Ice { candidate: String },
    /// A roster / relay frame: understood, and not part of a handshake.
    Ignored,
}

/// Classify one signaling frame for the handshake loops, failing the handshake on
/// a server `error` frame.
///
/// Both loops previously ended in `_ => {}`, which swallowed
/// [`ServerMessage::Error`] — the frame the relay sends for `permission_denied`
/// or `unknown_target`. A rejected handshake then sat in the loop until
/// `open_timeout` elapsed and reported [`WebrtcSignalingError::Timeout`], i.e. it
/// diagnosed a network stall for what the server had already explained. The match
/// is exhaustive by variant, so a new `ServerMessage` is a compile error here
/// rather than one more silently dropped frame.
fn classify_handshake_frame(msg: ServerMessage) -> Result<HandshakeFrame, WebrtcSignalingError> {
    match msg {
        ServerMessage::Offer { from, sdp } => Ok(HandshakeFrame::Offer { from, sdp }),
        ServerMessage::Answer { sdp, .. } => Ok(HandshakeFrame::Answer { sdp }),
        ServerMessage::Ice { candidate, .. } => Ok(HandshakeFrame::Ice { candidate }),
        ServerMessage::Error { code, message } => {
            Err(WebrtcSignalingError::Server { code, message })
        }
        ServerMessage::Welcome { .. }
        | ServerMessage::PeerJoined { .. }
        | ServerMessage::PeerLeft { .. }
        | ServerMessage::Relay { .. } => Ok(HandshakeFrame::Ignored),
    }
}

/// Apply a single signaling frame to the in-flight handshake. Only the offerer's
/// `answer` and either side's trickled `ice` advance an in-progress connection;
/// roster / relay frames are ignored here and a server `error` frame fails the
/// handshake (see [`classify_handshake_frame`]).
fn apply_handshake_frame(net: &Str0mNet, msg: ServerMessage) -> Result<(), WebrtcSignalingError> {
    match classify_handshake_frame(msg)? {
        HandshakeFrame::Answer { sdp } => net.accept_answer(&sdp)?,
        HandshakeFrame::Ice { candidate } => net.add_remote_candidate(&candidate)?,
        HandshakeFrame::Offer { .. } | HandshakeFrame::Ignored => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_error_frame_fails_the_handshake_instead_of_being_ignored() {
        let frame = ServerMessage::Error {
            code: "permission_denied".to_string(),
            message: "peer 2 is not in room-1".to_string(),
        };
        match classify_handshake_frame(frame) {
            Err(WebrtcSignalingError::Server { code, message }) => {
                assert_eq!(code, "permission_denied");
                assert_eq!(message, "peer 2 is not in room-1");
            }
            other => panic!("a server error frame must fail the handshake, got {other:?}"),
        }
    }

    #[test]
    fn the_server_error_is_reported_with_its_code_and_message() {
        let err = WebrtcSignalingError::Server {
            code: "unknown_target".to_string(),
            message: "no such peer".to_string(),
        };
        let rendered = err.to_string();
        assert!(
            rendered.contains("unknown_target") && rendered.contains("no such peer"),
            "the error names the server's code and message: {rendered}"
        );
    }

    #[test]
    fn roster_frames_are_still_ignored_by_the_handshake() {
        for frame in [
            ServerMessage::Welcome {
                peer: PeerId(1),
                peers: vec![PeerId(2)],
            },
            ServerMessage::PeerJoined { peer: PeerId(2) },
            ServerMessage::PeerLeft { peer: PeerId(2) },
            ServerMessage::Relay {
                from: PeerId(2),
                payload: serde_json::Value::Null,
            },
        ] {
            let label = format!("{frame:?}");
            match classify_handshake_frame(frame) {
                Ok(HandshakeFrame::Ignored) => {}
                other => panic!("{label} must stay ignored, got {other:?}"),
            }
        }
    }

    #[test]
    fn handshake_frames_still_classify_as_themselves() {
        let offer = ServerMessage::Offer {
            from: PeerId(7),
            sdp: "v=0-offer".to_string(),
        };
        match classify_handshake_frame(offer) {
            Ok(HandshakeFrame::Offer { from, sdp }) => {
                assert_eq!(from, PeerId(7));
                assert_eq!(sdp, "v=0-offer");
            }
            other => panic!("an offer must classify as an offer, got {other:?}"),
        }
        let answer = ServerMessage::Answer {
            from: PeerId(7),
            sdp: "v=0-answer".to_string(),
        };
        match classify_handshake_frame(answer) {
            Ok(HandshakeFrame::Answer { sdp }) => assert_eq!(sdp, "v=0-answer"),
            other => panic!("an answer must classify as an answer, got {other:?}"),
        }
        let ice = ServerMessage::Ice {
            from: PeerId(7),
            candidate: "candidate:1".to_string(),
        };
        match classify_handshake_frame(ice) {
            Ok(HandshakeFrame::Ice { candidate }) => assert_eq!(candidate, "candidate:1"),
            other => panic!("an ice frame must classify as ice, got {other:?}"),
        }
    }
}
