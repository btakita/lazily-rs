# lazily Wire Protocol

Language-agnostic protocol reference for the lazily reactive-graph family
(lazily-rs, lazily-py, lazily-zig). This document describes the wire format,
message schemas, and transport contracts. Language-specific APIs live in each
binding's own documentation.

## Message Plane

All channels (FFI, IPC, WebSocket, WebRTC data) carry the same two message
kinds:

- **`Snapshot`** — full graph image, sent on connect and on resync
- **`Delta`** — incremental change set, sent once per outermost batch flush

These are tagged as `IpcMessage`:

```json
{ "Snapshot": { ... } }
{ "Delta": { ... } }
```

## Wire Types

### NodeId

Stable wire identifier for a reactive node (cell or slot). Decoupled from
language-internal allocation IDs.

```json
{ "node": 1 }
```

Wire format: `u64` wrapped in a `"node"` field.

### NodeKey

Optional wire-stable keyed address for a collection entry (a `SourceMap` /
`CellFamily` entry). Unlike `NodeId` — the volatile internal handle — a
`NodeKey` is producer-defined and **stable across NodeId churn**: an entry that
is removed and later re-added is re-minted under a *new* `NodeId` but keeps the
same `NodeKey`, so a peer can subscribe to "entry `scores/alice`" without
maintaining an out-of-band key→NodeId map.

```json
"scores/alice"
"sheet/A1"
"outer/k1/inner/k2"
```

Wire format: a `/`-joined path string. A multi-segment path addresses nested
collections (an entry of a `SourceMap` inside a `SourceMap` entry) with no extra
machinery — each `/` introduces a deeper segment.

Bounds (reject on construction and on the wire):

| Bound | Limit |
|-------|-------|
| Max path length | 1024 bytes |
| Max segment count | 32 |
| Empty path | rejected |
| Empty segment (leading/trailing/double `/`) | rejected |

`NodeKey` is **additive** addressing — it never changes `NodeId` semantics. It
appears only as the optional `key` field on `NodeSnapshot` and the `NodeAdd`
delta op; absent ⇒ today's opaque-NodeId-only behavior. Key uniqueness across
multiple producers (the multi-writer distributed boundary) is owned by the
distributed CRDT plane's last-writer rule, not this protocol.

### PeerId

Identifies a remote peer.

```json
{ "peer": 42 }
```

Wire format: `u64`. JavaScript peers must keep this at or below
`Number.MAX_SAFE_INTEGER`.

### OpKind

Access category for a remote operation.

| Value | Meaning |
|-------|---------|
| `"Read"` | Read node value into snapshot/delta |
| `"Write"` | Write new value to source cell |
| `"TriggerEffect"` | Trigger effect on irreversible-effect plane |

### RemoteOp

A single operation a remote peer may request.

```json
{ "kind": "Read", "node": 1 }
```

### IpcPayload

Opaque serialized value bytes. The producing language owns type-aware encoding
through `type_tag`; the channel only moves bytes.

Wire format: array of `u8` (JSON array of integers).

### NodeState

Serialization state for a node.

```json
{ "Payload": [1, 2, 3, 4] }
"Opaque"
{ "SharedBlob": { "offset": 0, "len": 16, "generation": 1, "epoch": 9, "checksum": 123456789 } }
```

| Variant | Meaning |
|---------|---------|
| `{ "Payload": [...] }` | Inline serialized value bytes |
| `"Opaque"` | Known node whose value cannot be serialized |
| `{ "SharedBlob": { ... } }` | Descriptor for bytes in shared memory |

### ShmBlobRef

Descriptor for a payload stored in a shared-memory blob arena.

| Field | Type | Meaning |
|-------|------|---------|
| `offset` | `u64` | Byte offset from arena start |
| `len` | `u64` | Payload length in bytes |
| `generation` | `u64` | Per-write generation (stale rejection) |
| `epoch` | `u64` | IPC epoch of the publishing message |
| `checksum` | `u64` | FNV-1a payload checksum |

### IpcValue

Value stored inline or by shared-memory blob reference.

```json
{ "Inline": [10, 20, 30] }
{ "SharedBlob": { "offset": 40, "len": 17, "generation": 2, "epoch": 9, "checksum": 987654321 } }
```

## Snapshot Message

Full graph image sent on connect or resync.

### Schema

```
Snapshot {
  epoch: u64,
  nodes: Vec<NodeSnapshot>,
  edges: Vec<EdgeSnapshot>,
  roots: Vec<NodeId>
}
```

### NodeSnapshot

```
NodeSnapshot {
  node: NodeId,
  type_tag: string,
  state: NodeState,
  key: NodeKey?   // optional; absent in JSON/MessagePack when not set
}
```

The optional trailing `key` field carries a wire-stable `NodeKey` for
collection entries (see [NodeKey](#nodekey)). In the self-describing codecs
(JSON, MessagePack) it is omitted when absent, so pre-`key` encoders and
decoders round-trip unchanged. In Postcard (positional) the field is always
present as an optional discriminant for schema stability.

### EdgeSnapshot

```
EdgeSnapshot {
  dependent: NodeId,
  dependency: NodeId
}
```

### Example: Minimal snapshot

```json
{
  "Snapshot": {
    "epoch": 1,
    "nodes": [
      {
        "node": 1,
        "type_tag": "i32",
        "state": { "Payload": [1, 2, 3, 4] }
      }
    ],
    "edges": [],
    "roots": [1]
  }
}
```

### Example: Multi-node snapshot with opaque node

```json
{
  "Snapshot": {
    "epoch": 7,
    "nodes": [
      { "node": 1, "type_tag": "i32", "state": { "Payload": [1, 2, 3] } },
      { "node": 2, "type_tag": "f64", "state": { "Payload": [0, 0, 0, 0, 0, 0, 240, 63] } },
      { "node": 3, "type_tag": "opaque-type", "state": "Opaque" }
    ],
    "edges": [
      { "dependent": 2, "dependency": 1 },
      { "dependent": 3, "dependency": 1 }
    ],
    "roots": [1, 2]
  }
}
```

### Example: Snapshot with shared-blob node

```json
{
  "Snapshot": {
    "epoch": 9,
    "nodes": [
      {
        "node": 7,
        "type_tag": "text/plain",
        "state": {
          "SharedBlob": {
            "offset": 0,
            "len": 16,
            "generation": 1,
            "epoch": 9,
            "checksum": 123456789
          }
        }
      }
    ],
    "edges": [],
    "roots": [7]
  }
}
```

## Delta Message

Incremental change set emitted after one outermost batch flush.

### Schema

```
Delta {
  base_epoch: u64,
  epoch: u64,
  ops: Vec<DeltaOp>
}
```

Sequential deltas satisfy `epoch == base_epoch + 1`. A receiver detects gaps,
reorders, or sender restarts by checking `base_epoch == last_epoch`.

### DeltaOp Variants

| Variant | Fields | Meaning |
|---------|--------|---------|
| `CellSet` | `node`, `payload` (IpcValue) | Source cell changed to new value |
| `SlotValue` | `node`, `payload` (IpcValue) | Lazily recomputed slot published a value |
| `Invalidate` | `node` | Node dirtied without a concrete value |
| `NodeAdd` | `node`, `type_tag`, `state` (NodeState), `key` (NodeKey, optional) | New node became visible |
| `NodeRemove` | `node` | Node was removed |
| `EdgeAdd` | `dependent`, `dependency` | Dependency edge added |
| `EdgeRemove` | `dependent`, `dependency` | Dependency edge removed |

### Example: Sequential delta with all op variants

```json
{
  "Delta": {
    "base_epoch": 40,
    "epoch": 41,
    "ops": [
      { "CellSet": { "node": 1, "payload": { "Inline": [10] } } },
      { "SlotValue": { "node": 2, "payload": { "Inline": [20] } } },
      { "Invalidate": { "node": 3 } },
      { "NodeAdd": { "node": 4, "type_tag": "u64", "state": { "Payload": [64] } } },
      { "NodeRemove": { "node": 5 } },
      { "EdgeAdd": { "dependent": 2, "dependency": 1 } },
      { "EdgeRemove": { "dependent": 3, "dependency": 1 } }
    ]
  }
}
```

### Example: Non-sequential delta (gap)

```json
{
  "Delta": {
    "base_epoch": 12,
    "epoch": 13,
    "ops": []
  }
}
```

When the receiver's `last_epoch` is 10, this delta has a gap (expected 10→11,
got 12→13). The receiver must discard it and request a fresh `Snapshot`.

### Example: Delta with shared-blob payload

```json
{
  "Delta": {
    "base_epoch": 8,
    "epoch": 9,
    "ops": [
      {
        "SlotValue": {
          "node": 7,
          "payload": {
            "SharedBlob": {
              "offset": 40,
              "len": 17,
              "generation": 2,
              "epoch": 9,
              "checksum": 987654321
            }
          }
        }
      }
    ]
  }
}
```

## Epoch Contract

- `ipc_epoch` is a monotonic `u64` that advances once per outermost batch flush.
- `Snapshot` carries `epoch`.
- `Delta` carries `{ base_epoch, epoch }` with `epoch == base_epoch + 1`.
- On `Delta` where `base_epoch != last_epoch`: discard the delta, request a
  fresh `Snapshot`, resume from the snapshot's `epoch`.

## Consistency Invariants

- **PartialEq cell guard:** equal `CellSet` produces no wire ops.
- **Memo equality suppression:** a dirty memo slot that recomputes to an equal
  value emits no `SlotValue` or downstream `Invalidate`.
- **Coalesced frontier:** a dependent reached through many changed cells in one
  batch appears at most once per delta.
- **Eager Signal nodes always carry a value:** an eager `Signal` (see below) is
  recomputed during the invalidation flush, so when it changes it appears in the
  delta as a concrete `SlotValue` (never a bare `Invalidate`). A purely lazy slot
  that was not read before the flush may instead appear as `Invalidate` with no
  value. Both are valid wire states for the same `SlotValue`/`Invalidate` op set;
  the distinction is computation timing, not message format.

## Eager Signal Nodes

A `Signal` is the eager derived value in the `Slot -> Cell -> Signal` family: it
recomputes the instant a dependency invalidates rather than on next read. It is
**not a new wire type**. A Signal is composed from a memoized backing slot plus a
local puller effect, and only the backing slot is graph state, so on the wire a
Signal node is an ordinary slot node:

- **Snapshot:** the backing slot appears as a `NodeSnapshot` with its materialized
  value in `NodeState` (`Payload`/`SharedBlob`), like any other readable slot.
- **Delta:** a value change appears as `SlotValue` for the backing slot's
  `NodeId`. Because the value is eagerly materialized at flush time it is always
  concrete; eager nodes do not emit bare `Invalidate`.
- **Memo guard still applies:** an eager recompute that yields an equal value
  (`PartialEq`) suppresses the `SlotValue` and any downstream `Invalidate`, exactly
  as for `ctx.memo` slots.
- **The puller effect is local:** it drives eager recomputation but is not
  serialized as a node and produces no `TriggerEffect` op. Eagerness is a
  producer-side scheduling property; remote peers receive the same
  permission-filtered `Snapshot`/`Delta` state plane regardless of whether a node
  is lazy or eager.

Peers therefore need no protocol change to consume signals from an eager
producer — a Signal is observed as a slot that is reliably present in every delta
that changes it.

## Permission Boundary

Only nodes on the per-peer allowlist are serialized. Non-allowlisted nodes are
**omitted entirely** (not even as `Opaque`) so a peer cannot infer their
existence. Edges are retained only when both endpoints are readable.

This filter is applied at snapshot/delta construction time, before
serialization, on all channels without exception.

## Serialization

### JSON (canonical/default)

`serde_json` with derived `Serialize`/`Deserialize`. All examples above use
JSON. This is the canonical text form, the default transport codec, and the
fixture format that every language binding must be able to render for debugging
and agent inspection.

### MessagePack (optional, cross-language binary)

Named MessagePack encoding via the `ipc-msgpack` feature. It preserves the same
serde field names as JSON while reducing frame size and parse cost for
production transports. Peers negotiate this as codec `"msgpack"` and must still
be able to render any frame back to canonical JSON for diagnostics.

MessagePack frames decode through `IpcMessage::decode_msgpack(bytes)` and encode
through `IpcMessage::encode_msgpack()`.

### Postcard (optional, Rust/same-schema binary)

`postcard` compact binary encoding via the `ipc-binary` feature. Smaller and
faster than JSON, but **not self-describing** — peers must agree on the schema.
For same-language Rust or postcard-aware transports only.

Binary frames decode through `IpcMessage::decode_binary(bytes)` and encode
through `IpcMessage::encode_binary()`.

## Transport Contracts

### FFI (C ABI)

- Opaque channel handle + owned byte buffers
- Functions: `channel_new`, `channel_free`, `channel_send`, `channel_recv`,
  `ipc_message_validate`, `ipc_message_kind`, `ipc_message_clone`,
  `bytes_free`
- Binary variants: same functions with `_binary` suffix
- Ownership: caller owns input bytes; Rust owns output buffers until the paired
  free function is called
- Errors return `LazilyFfiStatus` enum; panics are caught before the C ABI

### IPC (Unix socket / pipe / local TCP)

- Length-prefixed serialized `IpcMessage` frames
- Shared-memory optional for large `IpcValue::SharedBlob` payloads
- `IpcSink` / `IpcSource` trait interface

### WebSocket

- One WebSocket text/binary frame carries one serialized `IpcMessage`
- Signaling server (#yxjw) relays frames as opaque payload
- Server must not parse CRDT/IPC state

### WebRTC Data Channel

- Reliable ordered data channels only (for graph state)
- Length-prefixed framing: 4-byte LE length + payload
- JSON or binary codec negotiated during capability handshake
- On channel failure: re-signaling via `SignalingClient`, delta resync covers gaps
- Unordered/unreliable channels only for optional lossy telemetry

## Capability Negotiation

Each non-local session starts with a handshake:

| Field | Description |
|-------|-------------|
| Protocol id | `"lazily-ipc"` |
| Protocol major version | `1` |
| Codec | `"json"`, `"msgpack"`, or `"postcard"` |
| Maximum frame size | Negotiated maximum |
| Ordered/reliable | Required for graph state |
| PeerId | Session participant |
| Supported features | `shared-blob`, `crdt-cell-plane`, etc. |

If peers disagree on protocol major version, codec, or ordering guarantees,
they fail closed before applying any `Snapshot` or `Delta`.

## Cross-Language Family Rules

- Compute closures are language-local. Cross-language sync shares the cell
  state plane; derived slots converge remotely only when peers use a shared
  compiled graph or explicit compute descriptors.
- Permission filtering happens before serialization on every channel.
- Channel code must preserve back-pressure and resync behavior.
- All channels carry the same permission-filtered `IpcMessage` state plane.

## Conformance Test Vectors

Canonical JSON fixtures in `tests/conformance/` validate wire-format agreement
across all language bindings:

| Fixture | Coverage |
|---------|----------|
| `snapshot_minimal.json` | Single payload node, no edges |
| `snapshot_multi_node.json` | Multiple nodes, opaque state, edges |
| `snapshot_shared_blob.json` | Shared-memory blob reference |
| `delta_sequential.json` | All 7 DeltaOp variants |
| `delta_non_sequential.json` | Gap requiring resync |
| `delta_shared_blob.json` | Delta with shared-blob payload |

Each fixture contains:

```json
{
  "description": "...",
  "protocol_version": 1,
  "kind": "Snapshot" | "Delta",
  "assertions": { ... },
  "wire": { <IpcMessage> }
}
```

Language bindings should:
1. Parse `wire` into native types
2. Validate `assertions` (field values, counts, state kinds)
3. Re-serialize and verify byte-exact match
