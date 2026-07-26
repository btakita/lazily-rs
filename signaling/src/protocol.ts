/**
 * Wire protocol for the lazily-distributed signaling server (#yxjw).
 *
 * The signaling server brokers peer discovery for the lazily-distributed CRDT
 * cell plane: peers join a session, learn the current roster, and exchange the
 * WebRTC SDP/ICE needed to establish direct P2P data channels (or relay opaque
 * payloads through the server when a direct channel is not available). It is
 * intentionally a *discovery + relay* layer — it does not interpret CRDT state.
 *
 * `PeerId` matches the Rust `lazily::distributed::PeerId(u64)` newtype, which
 * `serde` serializes as a bare JSON number. JavaScript numbers lose precision
 * above 2^53, so callers must keep peer ids within `Number.MAX_SAFE_INTEGER`;
 * `isPeerId` enforces that.
 */

/** Stable identifier for a peer, mirroring Rust `PeerId(u64)`. */
export type PeerId = number;

/** Opaque identifier for a signaling session (room). */
export type SessionId = string;

/** Messages a client sends to the signaling server. */
export type ClientMessage =
  | { type: "join"; peer: PeerId; capabilities?: string[] }
  | { type: "offer"; to: PeerId; sdp: string }
  | { type: "answer"; to: PeerId; sdp: string }
  | { type: "ice"; to: PeerId; candidate: string }
  | { type: "relay"; to: PeerId; payload: unknown }
  | { type: "leave" };

/** Messages the signaling server sends to a client. */
export type ServerMessage =
  | { type: "welcome"; peer: PeerId; peers: PeerId[] }
  | { type: "peer-joined"; peer: PeerId }
  | { type: "peer-left"; peer: PeerId }
  | { type: "offer"; from: PeerId; sdp: string }
  | { type: "answer"; from: PeerId; sdp: string }
  | { type: "ice"; from: PeerId; candidate: string }
  | { type: "relay"; from: PeerId; payload: unknown }
  | { type: "error"; code: ErrorCode; message: string };

export type ErrorCode =
  | "bad_message"
  | "not_joined"
  | "already_joined"
  | "duplicate_peer"
  | "unknown_target"
  | "permission_denied";

/** A directed signaling op, used for permission gating (mirrors #39c5). */
export type DirectedKind = "offer" | "answer" | "ice" | "relay";

/** True when `value` is a safe-integer peer id. */
export function isPeerId(value: unknown): value is PeerId {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

/**
 * Validate and narrow an untrusted parsed JSON value to a `ClientMessage`.
 * Returns `null` for anything malformed so the caller can reply with a
 * `bad_message` error instead of throwing.
 */
export function parseClientMessage(value: unknown): ClientMessage | null {
  if (typeof value !== "object" || value === null) return null;
  const msg = value as Record<string, unknown>;
  switch (msg.type) {
    case "join":
      if (!hasOnlyKeys(msg, ["type", "peer", "capabilities"])) return null;
      if (!isPeerId(msg.peer)) return null;
      if (msg.capabilities !== undefined && !isStringArray(msg.capabilities)) {
        return null;
      }
      return {
        type: "join",
        peer: msg.peer,
        ...(msg.capabilities !== undefined
          ? { capabilities: msg.capabilities as string[] }
          : {}),
      };
    case "offer":
    case "answer":
      if (!hasOnlyKeys(msg, ["type", "to", "sdp"])) return null;
      if (!isPeerId(msg.to) || typeof msg.sdp !== "string") return null;
      return { type: msg.type, to: msg.to, sdp: msg.sdp };
    case "ice":
      if (!hasOnlyKeys(msg, ["type", "to", "candidate"])) return null;
      if (!isPeerId(msg.to) || typeof msg.candidate !== "string") return null;
      return { type: "ice", to: msg.to, candidate: msg.candidate };
    case "relay":
      if (!hasOnlyKeys(msg, ["type", "to", "payload"])) return null;
      if (!isPeerId(msg.to) || !("payload" in msg)) return null;
      return { type: "relay", to: msg.to, payload: msg.payload };
    case "leave":
      if (!hasOnlyKeys(msg, ["type"])) return null;
      return { type: "leave" };
    default:
      return null;
  }
}

/** Parse a raw WebSocket string frame into a `ClientMessage`, or `null`. */
export function decodeClientFrame(data: string): ClientMessage | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  return parseClientMessage(parsed);
}

/** Serialize a `ServerMessage` to a WebSocket string frame. */
export function encodeServerMessage(message: ServerMessage): string {
  return JSON.stringify(message);
}

/** Serialize a `ClientMessage` to a WebSocket string frame (client side). */
export function encodeClientMessage(message: ClientMessage): string {
  return JSON.stringify(message);
}

/**
 * Parse and fully validate a raw server frame into a `ServerMessage`.
 */
export function decodeServerFrame(data: string): ServerMessage | null {
  try {
    const parsed: unknown = JSON.parse(data);
    return parseServerMessage(parsed);
  } catch {
    return null;
  }
}

function parseServerMessage(value: unknown): ServerMessage | null {
  if (typeof value !== "object" || value === null) return null;
  const msg = value as Record<string, unknown>;
  switch (msg.type) {
    case "welcome":
      if (
        !hasOnlyKeys(msg, ["type", "peer", "peers"]) ||
        !isPeerId(msg.peer) ||
        !Array.isArray(msg.peers) ||
        !msg.peers.every(isPeerId) ||
        msg.peers.includes(msg.peer)
      ) {
        return null;
      }
      return { type: "welcome", peer: msg.peer, peers: msg.peers as PeerId[] };
    case "peer-joined":
    case "peer-left":
      return hasOnlyKeys(msg, ["type", "peer"]) && isPeerId(msg.peer)
        ? { type: msg.type, peer: msg.peer }
        : null;
    case "offer":
    case "answer":
      return hasOnlyKeys(msg, ["type", "from", "sdp"]) &&
        isPeerId(msg.from) &&
        typeof msg.sdp === "string"
        ? { type: msg.type, from: msg.from, sdp: msg.sdp }
        : null;
    case "ice":
      return hasOnlyKeys(msg, ["type", "from", "candidate"]) &&
        isPeerId(msg.from) &&
        typeof msg.candidate === "string"
        ? { type: "ice", from: msg.from, candidate: msg.candidate }
        : null;
    case "relay":
      return hasOnlyKeys(msg, ["type", "from", "payload"]) &&
        isPeerId(msg.from) &&
        "payload" in msg
        ? { type: "relay", from: msg.from, payload: msg.payload }
        : null;
    case "error":
      return hasOnlyKeys(msg, ["type", "code", "message"]) &&
        isErrorCode(msg.code) &&
        typeof msg.message === "string"
        ? { type: "error", code: msg.code, message: msg.message }
        : null;
    default:
      return null;
  }
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}

function hasOnlyKeys(
  value: Record<string, unknown>,
  allowed: readonly string[],
): boolean {
  const keys = new Set(allowed);
  return Object.keys(value).every((key) => keys.has(key));
}

function isErrorCode(value: unknown): value is ErrorCode {
  return (
    typeof value === "string" &&
    [
      "bad_message",
      "not_joined",
      "already_joined",
      "duplicate_peer",
      "unknown_target",
      "permission_denied",
    ].includes(value)
  );
}
