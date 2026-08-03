//! Transport-agnostic snapshot/delta protocol for `lazily-ipc`.
//!
//! This module deliberately does not know whether messages move through a Unix
//! socket, pipe, WebSocket, or shared-memory ring buffer. It defines the stable
//! serializable state image and the permission-filtered construction helpers
//! that transports can carry.
//!
//! FFI adapters should treat encoded [`IpcMessage`] values as owned byte buffers
//! crossing the ABI. They should not expose live Rust contexts, references,
//! closures, or typed handles to foreign runtimes.

use crate::distributed::{NodeId, PeerId, PeerPermissions, RemoteOp};
use std::collections::HashMap;
use std::fmt;

/// Bytes reserved before every shared-memory blob payload.
pub const SHM_BLOB_HEADER_LEN: usize = 40;

const SHM_BLOB_MAGIC: u32 = 0x4c5a_5348; // "LZSH"
const SHM_BLOB_VERSION: u16 = 1;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Serialized value bytes for a node.
///
/// The higher `lazily-serde` layer owns type-aware encoding and decoding. IPC
/// treats the payload as opaque bytes after the producing graph has serialized
/// the node value.
pub type IpcPayload = Vec<u8>;

/// Which pluggable blob backend holds a descriptor's bytes (zero-copy
/// transport, `#lzzcpy`).
///
/// Spec: `lazily-spec/docs/zero-copy-transport.md`. The descriptor
/// ([`ShmBlobRef`]) carries this discriminator so a receiver routes resolution
/// to the right backend. Defaults to [`BlobBackendKind::Shm`] for backward
/// compatibility — legacy descriptors absent the field resolve as POSIX shared
/// memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum BlobBackendKind {
    /// POSIX shared-memory region (`shm_open` + `mmap`) — the default
    /// cross-process backend (same host).
    #[default]
    Shm,
    /// Apache Arrow IPC stream / Flight-resolved buffer — columnar zero-copy.
    /// The descriptor's bytes are an Arrow IPC stream the receiver imports
    /// zero-copy.
    Arrow,
    /// An in-process arena (single address space — the FFI host / an editor
    /// plugin loaded in the same process).
    InProcess,
}

impl BlobBackendKind {
    /// Returns the lowercase wire string for this backend (`"shm"`, `"arrow"`,
    /// `"in_process"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shm => "shm",
            Self::Arrow => "arrow",
            Self::InProcess => "in_process",
        }
    }

    /// Whether this is the default backend ([`Shm`]). Used by
    /// `skip_serializing_if` so legacy descriptors omit the field.
    ///
    /// [`Shm`]: BlobBackendKind::Shm
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::Shm)
    }
}

/// A `backend` discriminator that is present on the wire but names no backend
/// this build knows about (`#failclosedsweep`).
///
/// Carries the offending string so the rejection names the value rather than
/// reporting a generic decode failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownBlobBackend(pub String);

impl fmt::Display for UnknownBlobBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown blob backend `{}` (expected \"shm\" | \"arrow\" | \"in_process\")",
            self.0
        )
    }
}

impl std::error::Error for UnknownBlobBackend {}

impl std::str::FromStr for BlobBackendKind {
    type Err = UnknownBlobBackend;

    /// Parses a backend discriminator from its wire string.
    ///
    /// An unrecognised string is **rejected**, not defaulted. Forward
    /// compatibility for this field is carried by its *absence* — `backend` is
    /// `#[serde(default, skip_serializing_if = "is_default")]`, so a legacy
    /// encoder that predates the discriminator omits it and decodes as `Shm`
    /// without ever reaching this parse. A backend string this build does not
    /// know therefore means a peer spilled the bytes into a backend that is not
    /// present here (spec `zero-copy-transport.md`: "New backends … plug in by
    /// implementing the contract and adding a `backend` enum value"), and
    /// silently reading it as `Shm` would route resolution into the shared-memory
    /// arena — exactly the misroute the `resolve_wrong_backend` theorem forbids.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "shm" => Ok(Self::Shm),
            "arrow" => Ok(Self::Arrow),
            "in_process" => Ok(Self::InProcess),
            other => Err(UnknownBlobBackend(other.to_string())),
        }
    }
}

impl fmt::Display for BlobBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for BlobBackendKind {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// The `backend` discriminator's visitor, parameterised by whether an explicit
/// null (JSON `null`, msgpack nil) counts as the ABSENT form.
///
/// `NULL_IS_ABSENT = false` is the enum's own `Deserialize`: standing alone, a
/// `BlobBackendKind` is a required value and a null is a type error.
/// `NULL_IS_ABSENT = true` is the [`ShmBlobRef::backend`] FIELD, where absence
/// is meaningful and a null is one of its two spellings — see
/// [`deserialize_backend_null_as_absent`]. The leniency belongs to the optional
/// field, not to the enum, which is the same split
/// `Option<NodeKey>`/`#lzkeynullstrict` makes.
///
/// Both variants route the string case through the same
/// [`FromStr`](std::str::FromStr) parse, so a present-but-unknown token is
/// rejected NAMING the token in either mode.
struct BackendVisitor<const NULL_IS_ABSENT: bool>;

impl<'de, const NULL_IS_ABSENT: bool> serde::de::Visitor<'de> for BackendVisitor<NULL_IS_ABSENT> {
    type Value = BlobBackendKind;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a blob backend string (\"shm\" | \"arrow\" | \"in_process\")")?;
        if NULL_IS_ABSENT {
            f.write_str(" or null")?;
        }
        Ok(())
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        // A present-but-unknown discriminator fails the decode naming the
        // offending value; an ABSENT field is what stays lenient, via
        // `#[serde(default)]` on `ShmBlobRef::backend`.
        v.parse().map_err(serde::de::Error::custom)
    }

    /// JSON `null` / msgpack nil (`0xc0`), reached through `deserialize_option`.
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        if NULL_IS_ABSENT {
            Ok(BlobBackendKind::Shm)
        } else {
            Err(E::invalid_type(serde::de::Unexpected::Unit, &self))
        }
    }

    /// A present value under `deserialize_option`. It is re-read as a STRING by
    /// the strict visitor, so the token rules — reject an unknown token naming
    /// it, reject a non-string as a type error — are the same whether or not the
    /// null slot is lenient.
    fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_str(BackendVisitor::<false>)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        self.visit_none()
    }
}

impl<'de> serde::Deserialize<'de> for BlobBackendKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(BackendVisitor::<false>)
    }
}

/// Field deserializer for [`ShmBlobRef::backend`]: an explicit null is the
/// ABSENT form and decodes as [`BlobBackendKind::Shm`] (`#lzblobbackendstrict`,
/// following § NodeKey / `#lzkeynullstrict`).
///
/// `#[serde(default)]` alone does NOT make a non-`Option` field null-tolerant —
/// it supplies the default when the KEY is missing, and a present null still
/// reaches the field's own `Deserialize`, which is a type error for an enum.
/// The two forms are the same fact on the wire, though: a serde-style peer that
/// omitted `skip_serializing_if` on its optional field writes `null` exactly
/// where a conforming encoder writes nothing, so refusing it would be stricter
/// than this implementation is on frames this implementation's own shape
/// produces.
///
/// The strictness the clause is actually about is untouched: a present value
/// outside `{shm, arrow, in_process}` is still rejected, naming the token, and a
/// present value that is not a string at all is still rejected as a type error
/// through the codec's own decode-error family. Only the null slot moves.
///
/// Reading the null needs `deserialize_option`, not `deserialize_str`:
/// `serde_json` answers `deserialize_str` on a null by constructing the
/// `invalid_type` error itself, without ever calling the visitor, so a
/// `visit_unit` override never runs. `deserialize_option` is also the reason
/// this branches on `is_human_readable` — the crate reads that flag as
/// SELF-DESCRIBING (`#lzmsgpackparity`), and a non-self-describing codec writes
/// no option tag for this field, so asking it for one would misalign the frame.
/// Postcard therefore keeps the strict `deserialize_str` path unchanged.
fn deserialize_backend_null_as_absent<'de, D>(deserializer: D) -> Result<BlobBackendKind, D::Error>
where
    D: serde::Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        deserializer.deserialize_option(BackendVisitor::<true>)
    } else {
        deserializer.deserialize_str(BackendVisitor::<false>)
    }
}

/// Descriptor for a payload stored in a blob backend (shared-memory arena,
/// Arrow buffer, or in-process arena).
///
/// The `backend` field is optional and defaults to [`BlobBackendKind::Shm`]:
/// it is omitted on the wire (self-describing codecs) when `Shm` so legacy
/// descriptors validate unchanged, and an explicit null on the way IN is read as
/// that same absence. The arena header itself is backend-agnostic and does not
/// store `backend` — the discriminator is wire-level routing metadata only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ShmBlobRef {
    /// Offset of the blob header from the beginning of the shared arena.
    pub offset: u64,
    /// Payload length in bytes, excluding the arena header.
    pub len: u64,
    /// Per-write generation used to reject stale descriptors after wraparound.
    pub generation: u64,
    /// IPC epoch associated with the message that published this blob.
    pub epoch: u64,
    /// Non-cryptographic payload checksum for torn-write/stale-region checks.
    pub checksum: u64,
    /// Which pluggable backend resolves this descriptor. Optional; defaults to
    /// [`BlobBackendKind::Shm`] and is omitted on the wire when `Shm` for
    /// backward compatibility.
    ///
    /// Absence has two spellings and both mean `Shm`: no map entry at all
    /// (`serde(default)`), or an explicit null
    /// ([`deserialize_backend_null_as_absent`]).
    #[serde(
        default,
        deserialize_with = "deserialize_backend_null_as_absent",
        skip_serializing_if = "BlobBackendKind::is_default"
    )]
    pub backend: BlobBackendKind,
}

/// IPC value stored inline or by shared-memory blob reference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IpcValue {
    /// Inline serialized bytes.
    Inline(IpcPayload),
    /// Descriptor for bytes stored in a shared-memory blob arena.
    SharedBlob(ShmBlobRef),
}

impl From<IpcPayload> for IpcValue {
    fn from(payload: IpcPayload) -> Self {
        Self::Inline(payload)
    }
}

impl From<ShmBlobRef> for IpcValue {
    fn from(blob: ShmBlobRef) -> Self {
        Self::SharedBlob(blob)
    }
}

/// Error returned by [`ShmBlobArena`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShmBlobArenaError {
    /// The backing buffer cannot hold even one blob header plus one payload byte.
    CapacityTooSmall {
        /// Actual backing-buffer capacity.
        capacity: usize,
        /// Minimum usable capacity.
        min_capacity: usize,
    },
    /// Payload is larger than the largest single blob this arena can hold.
    BlobTooLarge {
        /// Requested payload length.
        len: usize,
        /// Maximum payload length.
        max_len: usize,
    },
    /// Descriptor points outside this arena.
    DescriptorOutOfBounds {
        /// Descriptor offset.
        offset: u64,
        /// Descriptor payload length.
        len: u64,
        /// Backing-buffer capacity.
        capacity: usize,
    },
    /// Descriptor/header metadata did not match.
    DescriptorMismatch {
        /// Mismatched field name.
        field: &'static str,
    },
    /// Payload checksum did not match the descriptor/header checksum.
    ChecksumMismatch {
        /// Expected checksum.
        expected: u64,
        /// Actual checksum.
        actual: u64,
    },
    /// The arena generation counter overflowed.
    GenerationOverflow,
    /// A blob backend setup or I/O failure (e.g. a POSIX `shm` `shm_open` /
    /// `mmap` failure in [`ShmBackend`](crate::ShmBackend)).
    BackendIo {
        /// Human-readable backend/OS error detail.
        detail: String,
    },
}

impl fmt::Display for ShmBlobArenaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityTooSmall {
                capacity,
                min_capacity,
            } => write!(
                f,
                "SHM blob arena capacity {capacity} is smaller than minimum {min_capacity}"
            ),
            Self::BlobTooLarge { len, max_len } => {
                write!(f, "SHM blob length {len} exceeds maximum {max_len}")
            }
            Self::DescriptorOutOfBounds {
                offset,
                len,
                capacity,
            } => write!(
                f,
                "SHM blob descriptor offset={offset} len={len} exceeds arena capacity {capacity}"
            ),
            Self::DescriptorMismatch { field } => {
                write!(f, "SHM blob descriptor mismatch for {field}")
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "SHM blob checksum mismatch: expected {expected:#x}, got {actual:#x}"
            ),
            Self::GenerationOverflow => write!(f, "SHM blob generation counter overflowed"),
            Self::BackendIo { detail } => write!(f, "blob backend I/O error: {detail}"),
        }
    }
}

impl std::error::Error for ShmBlobArenaError {}

/// Fixed-size blob arena suitable for a shared-memory transport.
///
/// The arena writes a small header before each payload. Readers validate the
/// header, generation, epoch, payload length, and checksum before returning a
/// slice. The backing storage is generic so callers can use an owned `Vec<u8>`
/// for tests or an OS-specific memory mapping in a transport crate.
#[derive(Debug, Clone)]
pub struct ShmBlobArena<B = Vec<u8>> {
    bytes: B,
    write_offset: usize,
    next_generation: u64,
}

impl ShmBlobArena<Vec<u8>> {
    /// Create a Vec-backed arena with `capacity` bytes.
    pub fn with_capacity(capacity: usize) -> Result<Self, ShmBlobArenaError> {
        Self::from_buffer(vec![0; capacity])
    }
}

impl<B> ShmBlobArena<B>
where
    B: AsRef<[u8]> + AsMut<[u8]>,
{
    /// Wrap an existing byte buffer.
    pub fn from_buffer(bytes: B) -> Result<Self, ShmBlobArenaError> {
        let capacity = bytes.as_ref().len();
        let min_capacity = SHM_BLOB_HEADER_LEN + 1;
        if capacity < min_capacity {
            return Err(ShmBlobArenaError::CapacityTooSmall {
                capacity,
                min_capacity,
            });
        }

        Ok(Self {
            bytes,
            write_offset: 0,
            next_generation: 1,
        })
    }

    /// Total arena capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.bytes.as_ref().len()
    }

    /// Maximum payload length this arena can hold in one blob.
    pub fn max_blob_len(&self) -> usize {
        self.capacity() - SHM_BLOB_HEADER_LEN
    }

    /// Current write cursor offset.
    pub fn write_offset(&self) -> usize {
        self.write_offset
    }

    /// Borrow the backing bytes.
    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    /// Mutably borrow the backing bytes.
    ///
    /// Transport implementations can use this to expose the backing memory to
    /// OS mapping setup. Mutating bytes after writing descriptors may make old
    /// descriptors fail validation.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes.as_mut()
    }

    /// Consume the arena and return its backing storage.
    pub fn into_inner(self) -> B {
        self.bytes
    }

    /// Write a payload and return a descriptor suitable for an IPC message.
    pub fn write_blob(
        &mut self,
        epoch: u64,
        payload: &[u8],
    ) -> Result<ShmBlobRef, ShmBlobArenaError> {
        let capacity = self.capacity();
        let max_len = self.max_blob_len();
        if payload.len() > max_len {
            return Err(ShmBlobArenaError::BlobTooLarge {
                len: payload.len(),
                max_len,
            });
        }

        let total_len = SHM_BLOB_HEADER_LEN + payload.len();
        if self.write_offset + total_len > capacity {
            self.write_offset = 0;
        }

        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or(ShmBlobArenaError::GenerationOverflow)?;

        let offset = self.write_offset;
        let checksum = checksum(payload);
        let descriptor = ShmBlobRef {
            offset: offset as u64,
            len: payload.len() as u64,
            generation,
            epoch,
            checksum,
            // The arena is backend-agnostic; the default `Shm` lets an
            // `InProcessBackend` overwrite this after minting.
            backend: BlobBackendKind::Shm,
        };

        let payload_offset = offset + SHM_BLOB_HEADER_LEN;
        let payload_end = payload_offset + payload.len();
        write_header(self.bytes.as_mut(), offset, descriptor);
        self.bytes.as_mut()[payload_offset..payload_end].copy_from_slice(payload);

        self.write_offset += total_len;
        if self.write_offset == capacity {
            self.write_offset = 0;
        }

        Ok(descriptor)
    }

    /// Read and validate a previously written blob.
    pub fn read_blob(&self, descriptor: ShmBlobRef) -> Result<&[u8], ShmBlobArenaError> {
        let capacity = self.capacity();
        let offset = usize::try_from(descriptor.offset).map_err(|_| {
            ShmBlobArenaError::DescriptorOutOfBounds {
                offset: descriptor.offset,
                len: descriptor.len,
                capacity,
            }
        })?;
        let len = usize::try_from(descriptor.len).map_err(|_| {
            ShmBlobArenaError::DescriptorOutOfBounds {
                offset: descriptor.offset,
                len: descriptor.len,
                capacity,
            }
        })?;
        let total_len = SHM_BLOB_HEADER_LEN.checked_add(len).ok_or(
            ShmBlobArenaError::DescriptorOutOfBounds {
                offset: descriptor.offset,
                len: descriptor.len,
                capacity,
            },
        )?;
        if offset
            .checked_add(total_len)
            .is_none_or(|end| end > capacity)
        {
            return Err(ShmBlobArenaError::DescriptorOutOfBounds {
                offset: descriptor.offset,
                len: descriptor.len,
                capacity,
            });
        }

        let mut header = read_header(self.bytes.as_ref(), offset)?;
        // The arena header does not store `backend`; align it to the descriptor
        // so a non-Shm descriptor validates against the backend-agnostic header.
        header.backend = descriptor.backend;
        if header != descriptor {
            return Err(mismatch_field(header, descriptor));
        }

        let payload_offset = offset + SHM_BLOB_HEADER_LEN;
        let payload = &self.bytes.as_ref()[payload_offset..payload_offset + len];
        let actual = checksum(payload);
        if actual != descriptor.checksum {
            return Err(ShmBlobArenaError::ChecksumMismatch {
                expected: descriptor.checksum,
                actual,
            });
        }

        Ok(payload)
    }
}

/// Maximum encoded byte length of a [`NodeKey`] path.
pub const NODE_KEY_MAX_LEN: usize = 1024;
/// Maximum number of `/`-separated segments in a [`NodeKey`].
pub const NODE_KEY_MAX_SEGMENTS: usize = 32;

/// Why a [`NodeKey`] failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKeyError {
    /// The path was empty.
    Empty,
    /// The path exceeded [`NODE_KEY_MAX_LEN`] bytes.
    TooLong {
        /// Byte length of the offending path.
        len: usize,
    },
    /// The path had more than [`NODE_KEY_MAX_SEGMENTS`] segments.
    TooManySegments {
        /// Segment count of the offending path.
        segments: usize,
    },
    /// The path contained an empty segment (leading/trailing/double `/`).
    EmptySegment,
}

impl fmt::Display for NodeKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "node key path is empty"),
            Self::TooLong { len } => {
                write!(
                    f,
                    "node key path is {len} bytes, exceeds {NODE_KEY_MAX_LEN}"
                )
            }
            Self::TooManySegments { segments } => write!(
                f,
                "node key has {segments} segments, exceeds {NODE_KEY_MAX_SEGMENTS}"
            ),
            Self::EmptySegment => write!(f, "node key path has an empty segment"),
        }
    }
}

impl std::error::Error for NodeKeyError {}

/// Wire-stable keyed address for a collection entry.
///
/// A `NodeKey` is a `/`-joined path (e.g. `scores/alice`, `sheet/A1`,
/// `outer/k1/inner/k2`). Unlike [`NodeId`] — the volatile internal handle —
/// a `NodeKey` is producer-defined and stable across NodeId churn: a
/// removed-then-readded entry keeps the same key, so a peer can subscribe to
/// "entry `scores/alice`" without maintaining an out-of-band key→NodeId map.
/// A multi-segment path addresses nested collections (an entry of a `SourceMap`
/// inside a `SourceMap` entry) with no extra machinery.
///
/// Length and segment count are bounded ([`NODE_KEY_MAX_LEN`],
/// [`NODE_KEY_MAX_SEGMENTS`]) to cap attacker-controlled growth; oversized keys
/// are rejected on construction and on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeKey(String);

impl NodeKey {
    /// Construct a validated key from a `/`-joined path.
    pub fn new(path: impl Into<String>) -> Result<Self, NodeKeyError> {
        let path = path.into();
        Self::validate(&path)?;
        Ok(Self(path))
    }

    /// Construct a key from already-validated segments.
    ///
    /// Segments are joined with `/`; the result is validated.
    pub fn from_segments<I, S>(segments: I) -> Result<Self, NodeKeyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let joined = segments
            .into_iter()
            .map(|s| s.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join("/");
        Self::new(joined)
    }

    fn validate(path: &str) -> Result<(), NodeKeyError> {
        if path.is_empty() {
            return Err(NodeKeyError::Empty);
        }
        if path.len() > NODE_KEY_MAX_LEN {
            return Err(NodeKeyError::TooLong { len: path.len() });
        }
        let mut segments = 0usize;
        for segment in path.split('/') {
            if segment.is_empty() {
                return Err(NodeKeyError::EmptySegment);
            }
            segments += 1;
        }
        if segments > NODE_KEY_MAX_SEGMENTS {
            return Err(NodeKeyError::TooManySegments { segments });
        }
        Ok(())
    }

    /// The full `/`-joined path.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterate the path segments.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }
}

impl fmt::Display for NodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for NodeKey {
    type Error = NodeKeyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NodeKey {
    type Error = NodeKeyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl serde::Serialize for NodeKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for NodeKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let path = String::deserialize(deserializer)?;
        Self::new(path).map_err(serde::de::Error::custom)
    }
}

/// Serializable state for one allowlisted node in a [`Snapshot`] or `NodeAdd`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeState {
    /// Concrete serialized value bytes.
    Payload(IpcPayload),
    /// Descriptor for a concrete value stored in a shared-memory blob arena.
    SharedBlob(ShmBlobRef),
    /// A known node whose value cannot be serialized.
    Opaque,
}

/// Full state for one node in a snapshot.
///
/// `key` serialization is format-aware: self-describing codecs (JSON,
/// MessagePack) omit it when `None` so pre-`key` encoders/decoders round-trip
/// unchanged; positional Postcard always writes the optional discriminant so
/// the binary schema stays stable. See [`NodeKey`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSnapshot {
    /// Wire-stable node identifier.
    pub node: NodeId,
    /// Producer-defined type tag for decoding `state`.
    pub type_tag: String,
    /// Serialized value bytes, or `Opaque` when the node is visible but
    /// type-erased serialization was not available.
    pub state: NodeState,
    /// Optional wire-stable keyed address for this node (a `SourceMap`/`ComputedMap`
    /// entry's path). `None` keeps today's opaque-NodeId-only addressing.
    pub key: Option<NodeKey>,
}

impl serde::Serialize for NodeSnapshot {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        // Omit a `None` key only in self-describing formats; positional codecs
        // (Postcard) must always carry the field so the schema stays stable.
        //
        // `is_human_readable` is serde's only encoder-supplied signal here and
        // this crate reads it as "self-describing". msgpack IS self-describing
        // but rmp-serde reports `false` by default, so `IpcMessage::encode_msgpack`
        // opts in with `.with_human_readable()` (`#lzmsgpackparity`). A msgpack
        // serializer built without that flag writes `key: null` and violates
        // protocol.md § NodeKey.
        let emit_key = self.key.is_some() || !serializer.is_human_readable();
        let mut st = serializer.serialize_struct("NodeSnapshot", 3 + emit_key as usize)?;
        st.serialize_field("node", &self.node)?;
        st.serialize_field("type_tag", &self.type_tag)?;
        st.serialize_field("state", &self.state)?;
        if emit_key {
            st.serialize_field("key", &self.key)?;
        }
        st.end()
    }
}

impl<'de> serde::Deserialize<'de> for NodeSnapshot {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename = "NodeSnapshot")]
        struct Raw {
            node: NodeId,
            type_tag: String,
            state: NodeState,
            #[serde(default)]
            key: Option<NodeKey>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(Self {
            node: raw.node,
            type_tag: raw.type_tag,
            state: raw.state,
            key: raw.key,
        })
    }
}

impl NodeSnapshot {
    /// Create a visible node carrying serialized value bytes.
    pub fn payload(node: NodeId, type_tag: impl Into<String>, payload: IpcPayload) -> Self {
        Self {
            node,
            type_tag: type_tag.into(),
            state: NodeState::Payload(payload),
            key: None,
        }
    }

    /// Create a visible node whose value cannot be serialized.
    pub fn opaque(node: NodeId, type_tag: impl Into<String>) -> Self {
        Self {
            node,
            type_tag: type_tag.into(),
            state: NodeState::Opaque,
            key: None,
        }
    }

    /// Create a visible node whose value lives in a shared-memory blob arena.
    pub fn shared_blob(node: NodeId, type_tag: impl Into<String>, blob: ShmBlobRef) -> Self {
        Self {
            node,
            type_tag: type_tag.into(),
            state: NodeState::SharedBlob(blob),
            key: None,
        }
    }

    /// Attach a wire-stable [`NodeKey`] to this node (builder style).
    pub fn with_key(mut self, key: NodeKey) -> Self {
        self.key = Some(key);
        self
    }
}

/// Directed dependency edge in a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EdgeSnapshot {
    /// Node that depends on `dependency`.
    pub dependent: NodeId,
    /// Node read by `dependent`.
    pub dependency: NodeId,
}

impl EdgeSnapshot {
    /// Create a dependency edge.
    pub fn new(dependent: NodeId, dependency: NodeId) -> Self {
        Self {
            dependent,
            dependency,
        }
    }

    fn is_readable_by(self, permissions: &PeerPermissions, peer: PeerId) -> bool {
        can_read(permissions, peer, self.dependent) && can_read(permissions, peer, self.dependency)
    }
}

/// Full graph image sent on connect or resync.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Snapshot {
    /// Context-level wire sequence number.
    pub epoch: u64,
    /// Visible node state.
    pub nodes: Vec<NodeSnapshot>,
    /// Visible dependency edges.
    pub edges: Vec<EdgeSnapshot>,
    /// Visible root/source nodes.
    pub roots: Vec<NodeId>,
}

impl Snapshot {
    /// Construct a snapshot.
    pub fn new(
        epoch: u64,
        nodes: Vec<NodeSnapshot>,
        edges: Vec<EdgeSnapshot>,
        roots: Vec<NodeId>,
    ) -> Self {
        Self {
            epoch,
            nodes,
            edges,
            roots,
        }
    }

    /// Return a peer-specific snapshot that omits non-readable nodes entirely.
    ///
    /// Edges are retained only when both endpoint nodes are readable, and roots
    /// preserve their input order after filtering.
    pub fn filter_readable(&self, permissions: &PeerPermissions, peer: PeerId) -> Self {
        let nodes = self
            .nodes
            .iter()
            .filter(|node| can_read(permissions, peer, node.node))
            .cloned()
            .collect();
        let edges = self
            .edges
            .iter()
            .copied()
            .filter(|edge| edge.is_readable_by(permissions, peer))
            .collect();
        let roots = permissions.filter_readable(peer, self.roots.iter().copied());

        Self {
            epoch: self.epoch,
            nodes,
            edges,
            roots,
        }
    }
}

/// One incremental graph mutation in a [`Delta`].
///
/// `NodeAdd`'s `key` serialization is format-aware (see [`NodeSnapshot`]):
/// self-describing codecs omit a `None` key; positional Postcard keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaOp {
    /// A source cell was changed to `payload`.
    CellSet { node: NodeId, payload: IpcValue },
    /// A lazily recomputed slot published a concrete value.
    SlotValue { node: NodeId, payload: IpcValue },
    /// A node was dirtied without publishing a concrete value.
    Invalidate { node: NodeId },
    /// A new node became visible.
    NodeAdd {
        node: NodeId,
        type_tag: String,
        state: NodeState,
        /// Optional wire-stable keyed address for the new node (see
        /// [`NodeKey`]). `None` keeps opaque-NodeId-only addressing.
        key: Option<NodeKey>,
    },
    /// A node was removed.
    NodeRemove { node: NodeId },
    /// A dependency edge was added.
    EdgeAdd {
        dependent: NodeId,
        dependency: NodeId,
    },
    /// A dependency edge was removed.
    EdgeRemove {
        dependent: NodeId,
        dependency: NodeId,
    },
}

impl DeltaOp {
    /// Construct a `CellSet`.
    pub fn cell_set(node: NodeId, payload: impl Into<IpcValue>) -> Self {
        Self::CellSet {
            node,
            payload: payload.into(),
        }
    }

    /// Construct a `SlotValue`.
    pub fn slot_value(node: NodeId, payload: impl Into<IpcValue>) -> Self {
        Self::SlotValue {
            node,
            payload: payload.into(),
        }
    }

    /// Construct a `CellSet` whose value is stored in a shared-memory blob.
    pub fn cell_set_blob(node: NodeId, blob: ShmBlobRef) -> Self {
        Self::cell_set(node, blob)
    }

    /// Construct a `SlotValue` whose value is stored in a shared-memory blob.
    pub fn slot_value_blob(node: NodeId, blob: ShmBlobRef) -> Self {
        Self::slot_value(node, blob)
    }

    /// Construct an `Invalidate`.
    pub fn invalidate(node: NodeId) -> Self {
        Self::Invalidate { node }
    }

    fn filter_readable(&self, permissions: &PeerPermissions, peer: PeerId) -> Option<Self> {
        match self {
            Self::CellSet { node, .. }
            | Self::SlotValue { node, .. }
            | Self::Invalidate { node }
            | Self::NodeAdd { node, .. }
            | Self::NodeRemove { node } => can_read(permissions, peer, *node).then(|| self.clone()),
            Self::EdgeAdd {
                dependent,
                dependency,
            }
            | Self::EdgeRemove {
                dependent,
                dependency,
            } => (can_read(permissions, peer, *dependent)
                && can_read(permissions, peer, *dependency))
            .then(|| self.clone()),
        }
    }
}

fn delta_op_key_ref_is_none(key: &&Option<NodeKey>) -> bool {
    key.is_none()
}

impl serde::Serialize for DeltaOp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Two borrowed shadows differing only in `NodeAdd.key` omission. The
        // human-readable shadow omits a `None` key; the binary shadow always
        // writes it so positional Postcard keeps a stable schema. "Human
        // readable" means SELF-DESCRIBING here — msgpack takes the `Hr` branch
        // because `encode_msgpack` sets the flag (`#lzmsgpackparity`).
        #[derive(serde::Serialize)]
        #[serde(rename = "DeltaOp")]
        enum Hr<'a> {
            CellSet {
                node: &'a NodeId,
                payload: &'a IpcValue,
            },
            SlotValue {
                node: &'a NodeId,
                payload: &'a IpcValue,
            },
            Invalidate {
                node: &'a NodeId,
            },
            NodeAdd {
                node: &'a NodeId,
                type_tag: &'a String,
                state: &'a NodeState,
                #[serde(skip_serializing_if = "delta_op_key_ref_is_none")]
                key: &'a Option<NodeKey>,
            },
            NodeRemove {
                node: &'a NodeId,
            },
            EdgeAdd {
                dependent: &'a NodeId,
                dependency: &'a NodeId,
            },
            EdgeRemove {
                dependent: &'a NodeId,
                dependency: &'a NodeId,
            },
        }
        #[derive(serde::Serialize)]
        #[serde(rename = "DeltaOp")]
        enum Bin<'a> {
            CellSet {
                node: &'a NodeId,
                payload: &'a IpcValue,
            },
            SlotValue {
                node: &'a NodeId,
                payload: &'a IpcValue,
            },
            Invalidate {
                node: &'a NodeId,
            },
            NodeAdd {
                node: &'a NodeId,
                type_tag: &'a String,
                state: &'a NodeState,
                key: &'a Option<NodeKey>,
            },
            NodeRemove {
                node: &'a NodeId,
            },
            EdgeAdd {
                dependent: &'a NodeId,
                dependency: &'a NodeId,
            },
            EdgeRemove {
                dependent: &'a NodeId,
                dependency: &'a NodeId,
            },
        }

        if serializer.is_human_readable() {
            match self {
                DeltaOp::CellSet { node, payload } => Hr::CellSet { node, payload },
                DeltaOp::SlotValue { node, payload } => Hr::SlotValue { node, payload },
                DeltaOp::Invalidate { node } => Hr::Invalidate { node },
                DeltaOp::NodeAdd {
                    node,
                    type_tag,
                    state,
                    key,
                } => Hr::NodeAdd {
                    node,
                    type_tag,
                    state,
                    key,
                },
                DeltaOp::NodeRemove { node } => Hr::NodeRemove { node },
                DeltaOp::EdgeAdd {
                    dependent,
                    dependency,
                } => Hr::EdgeAdd {
                    dependent,
                    dependency,
                },
                DeltaOp::EdgeRemove {
                    dependent,
                    dependency,
                } => Hr::EdgeRemove {
                    dependent,
                    dependency,
                },
            }
            .serialize(serializer)
        } else {
            match self {
                DeltaOp::CellSet { node, payload } => Bin::CellSet { node, payload },
                DeltaOp::SlotValue { node, payload } => Bin::SlotValue { node, payload },
                DeltaOp::Invalidate { node } => Bin::Invalidate { node },
                DeltaOp::NodeAdd {
                    node,
                    type_tag,
                    state,
                    key,
                } => Bin::NodeAdd {
                    node,
                    type_tag,
                    state,
                    key,
                },
                DeltaOp::NodeRemove { node } => Bin::NodeRemove { node },
                DeltaOp::EdgeAdd {
                    dependent,
                    dependency,
                } => Bin::EdgeAdd {
                    dependent,
                    dependency,
                },
                DeltaOp::EdgeRemove {
                    dependent,
                    dependency,
                } => Bin::EdgeRemove {
                    dependent,
                    dependency,
                },
            }
            .serialize(serializer)
        }
    }
}

impl<'de> serde::Deserialize<'de> for DeltaOp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(rename = "DeltaOp")]
        enum Wire {
            CellSet {
                node: NodeId,
                payload: IpcValue,
            },
            SlotValue {
                node: NodeId,
                payload: IpcValue,
            },
            Invalidate {
                node: NodeId,
            },
            NodeAdd {
                node: NodeId,
                type_tag: String,
                state: NodeState,
                #[serde(default)]
                key: Option<NodeKey>,
            },
            NodeRemove {
                node: NodeId,
            },
            EdgeAdd {
                dependent: NodeId,
                dependency: NodeId,
            },
            EdgeRemove {
                dependent: NodeId,
                dependency: NodeId,
            },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::CellSet { node, payload } => DeltaOp::CellSet { node, payload },
            Wire::SlotValue { node, payload } => DeltaOp::SlotValue { node, payload },
            Wire::Invalidate { node } => DeltaOp::Invalidate { node },
            Wire::NodeAdd {
                node,
                type_tag,
                state,
                key,
            } => DeltaOp::NodeAdd {
                node,
                type_tag,
                state,
                key,
            },
            Wire::NodeRemove { node } => DeltaOp::NodeRemove { node },
            Wire::EdgeAdd {
                dependent,
                dependency,
            } => DeltaOp::EdgeAdd {
                dependent,
                dependency,
            },
            Wire::EdgeRemove {
                dependent,
                dependency,
            } => DeltaOp::EdgeRemove {
                dependent,
                dependency,
            },
        })
    }
}

/// Incremental change set emitted after one outermost batch flush.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Delta {
    /// Receiver epoch this delta must apply after.
    pub base_epoch: u64,
    /// New epoch after applying this delta.
    pub epoch: u64,
    /// Coalesced operations for this flush.
    pub ops: Vec<DeltaOp>,
}

impl Delta {
    /// Construct a strictly sequential delta with `epoch == base_epoch + 1`.
    ///
    /// Panics if `base_epoch` is `u64::MAX`; a sender cannot advance beyond the
    /// maximum epoch and must start a fresh snapshot/session instead.
    pub fn next(base_epoch: u64, ops: Vec<DeltaOp>) -> Self {
        let epoch = base_epoch
            .checked_add(1)
            .expect("ipc epoch overflow requires a fresh snapshot/session");
        Self {
            base_epoch,
            epoch,
            ops,
        }
    }

    /// Construct a delta with explicit epochs. A multi-epoch-span delta
    /// (`epoch > base_epoch + 1`) coalesces several accepted-event epochs into one
    /// op batch (`#lzsync`, spec § Multi-epoch-span delta); `epoch == base_epoch + 1`
    /// is the ordinary single-flush case.
    pub fn new(base_epoch: u64, epoch: u64, ops: Vec<DeltaOp>) -> Self {
        Self {
            base_epoch,
            epoch,
            ops,
        }
    }

    /// The accepted-event span this delta advances: `epoch - base_epoch` (usually
    /// 1, `> 1` for a coalesced multi-epoch-span delta). Saturates at 0 for a
    /// malformed backward delta.
    pub fn span(&self) -> u64 {
        self.epoch.saturating_sub(self.base_epoch)
    }

    /// Whether this delta is exactly the next delta after `last_epoch`.
    pub fn is_next_after(&self, last_epoch: u64) -> bool {
        self.base_epoch == last_epoch
            && self
                .base_epoch
                .checked_add(1)
                .is_some_and(|next| self.epoch == next)
    }

    /// Return the receiver action for this delta.
    pub fn apply_status(&self, last_epoch: u64) -> DeltaApplyStatus {
        if self.is_next_after(last_epoch) {
            DeltaApplyStatus::Apply
        } else {
            DeltaApplyStatus::ResyncRequired {
                last_epoch,
                base_epoch: self.base_epoch,
                epoch: self.epoch,
            }
        }
    }

    /// Return a peer-specific delta that omits non-readable operations entirely.
    pub fn filter_readable(&self, permissions: &PeerPermissions, peer: PeerId) -> Self {
        let ops = self
            .ops
            .iter()
            .filter_map(|op| op.filter_readable(permissions, peer))
            .collect();

        Self {
            base_epoch: self.base_epoch,
            epoch: self.epoch,
            ops,
        }
    }
}

/// Receiver decision for an incoming [`Delta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaApplyStatus {
    /// Apply the delta and advance the receiver epoch.
    Apply,
    /// Discard this delta and request a fresh [`Snapshot`].
    ResyncRequired {
        /// Receiver's current epoch.
        last_epoch: u64,
        /// Delta's advertised base epoch.
        base_epoch: u64,
        /// Delta's advertised target epoch.
        epoch: u64,
    },
}

/// Consumer-side registry mapping wire-stable [`NodeKey`]s to current
/// [`NodeId`]s.
///
/// A subscriber expresses interest as a key (`scores/alice`); the index keeps
/// that key resolvable across NodeId churn. When an entry is removed and later
/// re-added under a fresh NodeId, ingesting the `NodeRemove` + keyed `NodeAdd`
/// (or a fresh `Snapshot`) repoints the key at the new NodeId, so the
/// key-expressed subscription stays valid. The mapping is bijective: each
/// NodeId resolves back to at most one key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyIndex {
    forward: HashMap<NodeKey, NodeId>,
    reverse: HashMap<NodeId, NodeKey>,
}

impl KeyIndex {
    /// Create an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the index with the keyed nodes of a full snapshot.
    pub fn ingest_snapshot(&mut self, snapshot: &Snapshot) {
        self.forward.clear();
        self.reverse.clear();
        for node in &snapshot.nodes {
            if let Some(key) = &node.key {
                self.insert(key.clone(), node.node);
            }
        }
    }

    /// Apply one delta's keyed `NodeAdd` / `NodeRemove` ops to the index.
    pub fn apply_delta(&mut self, delta: &Delta) {
        for op in &delta.ops {
            match op {
                DeltaOp::NodeAdd {
                    node,
                    key: Some(key),
                    ..
                } => self.insert(key.clone(), *node),
                DeltaOp::NodeRemove { node } => self.remove_node(*node),
                // Every remaining wire op is listed rather than absorbed by a
                // catch-all, so adding a `DeltaOp` variant that carries a key is
                // a compile error here instead of a silently unindexed key.
                DeltaOp::NodeAdd { key: None, .. }
                | DeltaOp::CellSet { .. }
                | DeltaOp::SlotValue { .. }
                | DeltaOp::Invalidate { .. }
                | DeltaOp::EdgeAdd { .. }
                | DeltaOp::EdgeRemove { .. } => {}
            }
        }
    }

    /// Bind `key` to `node`, dropping any stale forward/reverse entries so the
    /// mapping stays bijective across re-keying and NodeId churn.
    pub fn insert(&mut self, key: NodeKey, node: NodeId) {
        if let Some(old_key) = self.reverse.insert(node, key.clone())
            && old_key != key
        {
            self.forward.remove(&old_key);
        }
        if let Some(old_node) = self.forward.insert(key, node)
            && old_node != node
        {
            self.reverse.remove(&old_node);
        }
    }

    /// Drop whatever key currently resolves to `node`.
    pub fn remove_node(&mut self, node: NodeId) {
        if let Some(key) = self.reverse.remove(&node) {
            self.forward.remove(&key);
        }
    }

    /// Resolve a wire-stable key to its current NodeId.
    pub fn node_for_key(&self, key: &NodeKey) -> Option<NodeId> {
        self.forward.get(key).copied()
    }

    /// Resolve a NodeId back to its wire-stable key.
    pub fn key_for_node(&self, node: NodeId) -> Option<&NodeKey> {
        self.reverse.get(&node)
    }

    /// Number of keyed entries.
    pub fn len(&self) -> usize {
        self.forward.len()
    }

    /// Whether the index has no keyed entries.
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }
}

/// Tagged IPC protocol message.
/// A wire-stable mirror of a hybrid-logical-clock stamp — all plain integers, so
/// the IPC layer carries CRDT causal-stability metadata without depending on the
/// `distributed` feature's [`HlcStamp`](crate::HlcStamp).
///
/// Total order is `(wall_time, logical, peer)`, identical to `HlcStamp`, so the
/// two convert losslessly; the `distributed` + `ipc` integration layer
/// (`#lzcrdtplane5b`) owns that conversion. Defining it here keeps the wire
/// format usable (and codec-stable) whether or not `distributed` is compiled in.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct WireStamp {
    /// Wall-clock microseconds since the Unix epoch.
    pub wall_time: u64,
    /// Logical counter advancing causality within equal `wall_time`.
    pub logical: u64,
    /// Originating peer; final tiebreak so equal `(wall, logical)` is a total order.
    pub peer: u64,
}

/// One CRDT cell op on the wire (state-based / CvRDT): the converged register,
/// sequence, or text `state` for `node`, tagged with the [`WireStamp`] that
/// produced it and the optional wire-stable [`NodeKey`] that survives `NodeId`
/// churn (`#lzwirekey`).
///
/// The receiver merges `state` into its local replica. Because every cell CRDT
/// merge is commutative, associative, and idempotent, out-of-order, duplicated,
/// or batched delivery all converge — so a `CrdtOp` is safe to resend.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtOp {
    /// Target node (volatile id; pair with `key` for stable addressing).
    pub node: NodeId,
    /// Wire-stable keyed address, if the producer assigned one.
    pub key: Option<NodeKey>,
    /// The HLC stamp that produced this state.
    pub stamp: WireStamp,
    /// The converged CRDT state to merge into the receiver's replica.
    pub state: IpcValue,
}

impl CrdtOp {
    /// Construct a keyless op (addressed only by `node`).
    pub fn new(node: NodeId, stamp: WireStamp, state: impl Into<IpcValue>) -> Self {
        Self {
            node,
            key: None,
            stamp,
            state: state.into(),
        }
    }

    /// Construct an op carrying a wire-stable [`NodeKey`].
    pub fn keyed(node: NodeId, key: NodeKey, stamp: WireStamp, state: impl Into<IpcValue>) -> Self {
        Self {
            node,
            key: Some(key),
            stamp,
            state: state.into(),
        }
    }
}

/// A CRDT anti-entropy sync frame (the multi-writer plane, `#lzcrdtplane5`): the
/// sender advertises its per-peer **stamp frontier** (the highest [`WireStamp`]
/// it has observed from each peer) and ships a batch of [`CrdtOp`]s.
///
/// The `frontier` is the `StampFrontier` exchange: it lets the receiver compute
/// which ops it is still missing (anti-entropy) and feeds the causal-stability
/// watermark — `min` over membership — that drives tombstone GC
/// (`SeqCrdt::gc` / `TextCrdt::gc_with`). The exchange is bounded, idempotent,
/// and resumable; re-sending a frame the receiver already has is a no-op.
///
/// **Frontier suppression (`#lzspecfrontiersuppress`).** Under the ratified spec
/// relaxation, an empty/omitted `frontier` means "unchanged since the last frame
/// the receiver accepted" — the receiver reuses its last-merged frontier. The
/// serde attribute `skip_serializing_if = "Vec::is_empty"` omits the field on
/// the wire; a missing field deserializes as an empty vec (backward-compatible).
/// A cold-start receiver with no prior frontier MUST request a full sync.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CrdtSync {
    /// Per-peer highest observed stamp: `(peer, stamp)`, the sender's frontier.
    /// Empty/omitted means "unchanged since last accepted frame"
    /// (`#lzspecfrontiersuppress`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<(u64, WireStamp)>,
    /// The op batch this frame ships.
    pub ops: Vec<CrdtOp>,
}

impl CrdtSync {
    /// Construct a sync frame from a frontier advertisement and an op batch.
    pub fn new(frontier: Vec<(u64, WireStamp)>, ops: Vec<CrdtOp>) -> Self {
        Self { frontier, ops }
    }

    /// Return a peer-specific frame that omits ops for non-readable nodes
    /// entirely (omission, not redaction — mirroring [`Delta::filter_readable`]).
    ///
    /// The `frontier` advertisement is retained: it names peers and stamps, not
    /// node content, and the receiver needs the full frontier to compute its
    /// causal-stability watermark soundly.
    pub fn filter_readable(&self, permissions: &PeerPermissions, peer: PeerId) -> Self {
        let ops = self
            .ops
            .iter()
            .filter(|op| can_read(permissions, peer, op.node))
            .cloned()
            .collect();
        Self {
            frontier: self.frontier.clone(),
            ops,
        }
    }

    /// Build an **ops-only** frame that suppresses the frontier advertisement
    /// (`#lzspecfrontiersuppress`). The sender ships `ops` with an empty
    /// frontier; on the wire the `frontier` field is omitted entirely. The
    /// receiver reuses its last-merged frontier to compute the watermark. A
    /// sender MUST NOT use this for a frame whose frontier has advanced since
    /// the last accepted frame.
    pub fn ops_only(ops: Vec<CrdtOp>) -> Self {
        Self {
            frontier: Vec::new(),
            ops,
        }
    }

    /// Whether this frame suppresses its frontier advertisement (empty/omitted).
    pub fn is_frontier_suppressed(&self) -> bool {
        self.frontier.is_empty()
    }
}

/// Reliable-sync reverse-channel control frame: request a covering `Snapshot`
/// on a detected gap (`#lzsync`, spec § ResyncCoordinator). Carries no node
/// content, so it is permission-filter- and blob-spill-transparent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ResyncRequest {
    /// The requesting receiver's `last_epoch`; the sender replies with a
    /// `Snapshot { epoch >= from_epoch }`.
    pub from_epoch: u64,
}

/// Reliable-sync reverse-channel control frame: prove receipt through
/// `through_epoch` (`#lzsync`, spec § DurableOutbox). Advances the sender's
/// outbox retention cursor and doubles as the reconnect resume cursor. Carries
/// no node content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OutboxAck {
    /// Highest epoch the receiver has fully applied.
    pub through_epoch: u64,
}

/// Delta-CRDT sync request (`#lzspecdeltacrdt`): an explicit, lightweight
/// control frame that asks a peer to ship only the CRDT cell states the
/// requester has not yet observed (past `their_frontier`), rather than the
/// full converged state per anti-entropy round.
///
/// The receiver responds with a [`CrdtSync`] whose `ops` carry only the states
/// past `their_frontier`. The join is the same semilattice (`apply_delta` ≡
/// `merge`), so a delta is safe to resend and applies in any order. A binding
/// that does not implement delta-CRDT sync falls back to full-state shipping
/// when it does not recognize this frame.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeltaSinceRequest {
    /// The requester's per-peer stamp frontier — the highest [`WireStamp`] it
    /// has observed from each peer. The receiver ships only ops whose stamp
    /// dominates this frontier.
    pub their_frontier: Vec<(u64, WireStamp)>,
}

impl DeltaSinceRequest {
    /// Construct a delta request from a wire frontier.
    pub fn new(their_frontier: Vec<(u64, WireStamp)>) -> Self {
        Self { their_frontier }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IpcMessage {
    /// Full graph image.
    Snapshot(Snapshot),
    /// Incremental graph update.
    Delta(Delta),
    /// A CRDT anti-entropy sync frame: op batch + stamp-frontier advertisement
    /// for the multi-writer plane (`#lzcrdtplane5`).
    CrdtSync(CrdtSync),
    /// Reliable-sync control frame: request a covering `Snapshot` on a gap
    /// (`#lzsync`). Reverse channel (receiver → sender).
    ResyncRequest(ResyncRequest),
    /// Reliable-sync control frame: ack/resume cursor (`#lzsync`). Reverse
    /// channel (receiver → sender).
    OutboxAck(OutboxAck),
    /// Delta-CRDT sync request (`#lzspecdeltacrdt`): asks for only the cell
    /// states past the requester's frontier, not the full converged state.
    DeltaSinceRequest(DeltaSinceRequest),
}

impl IpcMessage {
    /// Whether this is a reliable-sync reverse-channel control frame
    /// (`ResyncRequest` / `OutboxAck`) — no node content, so permission
    /// filtering and blob spilling are the identity on it.
    pub fn is_control(&self) -> bool {
        matches!(
            self,
            IpcMessage::ResyncRequest(_)
                | IpcMessage::OutboxAck(_)
                | IpcMessage::DeltaSinceRequest(_)
        )
    }
}

/// Negotiated codec for serialized [`IpcMessage`] frames.
#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpcCodec {
    /// Canonical, inspectable JSON frame encoding.
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    Json,
    /// Named MessagePack encoding for cross-language binary frames.
    #[cfg(feature = "ipc-msgpack")]
    MessagePack,
    /// Compact postcard encoding for Rust/same-schema peers.
    #[cfg(feature = "ipc-binary")]
    Postcard,
}

#[cfg(any(feature = "ffi", feature = "webrtc"))]
#[allow(clippy::derivable_impls)]
impl Default for IpcCodec {
    fn default() -> Self {
        Self::Json
    }
}

#[cfg(all(not(any(feature = "ffi", feature = "webrtc")), feature = "ipc-msgpack"))]
impl Default for IpcCodec {
    fn default() -> Self {
        Self::MessagePack
    }
}

#[cfg(all(
    not(any(feature = "ffi", feature = "webrtc", feature = "ipc-msgpack")),
    feature = "ipc-binary"
))]
impl Default for IpcCodec {
    fn default() -> Self {
        Self::Postcard
    }
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
impl IpcCodec {
    /// Stable negotiation token for capability handshakes.
    pub const fn name(self) -> &'static str {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json => "json",
            #[cfg(feature = "ipc-msgpack")]
            Self::MessagePack => "msgpack",
            #[cfg(feature = "ipc-binary")]
            Self::Postcard => "postcard",
        }
    }

    /// Encode an IPC message with this codec.
    pub fn encode(self, message: &IpcMessage) -> Result<Vec<u8>, EncodeError> {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json => message.encode_json(),
            #[cfg(feature = "ipc-msgpack")]
            Self::MessagePack => message.encode_msgpack(),
            #[cfg(feature = "ipc-binary")]
            Self::Postcard => message.encode_binary(),
        }
    }

    /// Decode an IPC message with this codec.
    pub fn decode(self, bytes: &[u8]) -> Result<IpcMessage, DecodeError> {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json => IpcMessage::decode_json(bytes),
            #[cfg(feature = "ipc-msgpack")]
            Self::MessagePack => IpcMessage::decode_msgpack(bytes),
            #[cfg(feature = "ipc-binary")]
            Self::Postcard => IpcMessage::decode_binary(bytes),
        }
    }
}

// ---------------------------------------------------------------------------
// Capability negotiation (protocol.md § Capability Negotiation)
// ---------------------------------------------------------------------------

/// The protocol identifier every `lazily-ipc` peer must advertise.
pub const PROTOCOL_ID: &str = "lazily-ipc";

/// The current protocol major version.
pub const PROTOCOL_MAJOR_VERSION: u64 = 1;

fn default_ordered_reliable() -> bool {
    true
}

/// The compatibility handshake exchanged before any graph state flows
/// (protocol.md § Capability Negotiation).
///
/// Each non-local session starts with this frame. If the peers disagree on
/// `protocol_major_version`, `codec`, or `ordered_reliable`, they fail closed
/// before applying any [`Snapshot`] or [`Delta`].
///
/// Serialized as a plain JSON object (externally tagged like the rest of the
/// IPC layer is not applicable — this is a standalone frame, not an
/// [`IpcMessage`] variant):
///
/// ```json
/// {
///   "protocol_id": "lazily-ipc",
///   "protocol_major_version": 1,
///   "codec": "json",
///   "max_frame_size": 1048576,
///   "fragmentation_supported": false,
///   "ordered_reliable": true,
///   "peer_id": 1,
///   "session_id": "abc-123",
///   "features": ["shared-blob", "signaling-relay"]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CapabilityHandshake {
    /// Must be [`PROTOCOL_ID`].
    pub protocol_id: String,
    /// Breaking-change indicator; must equal [`PROTOCOL_MAJOR_VERSION`].
    pub protocol_major_version: u64,
    /// Codec negotiation token (`"json"`, `"msgpack"`, `"postcard"`).
    pub codec: String,
    /// Maximum frame size in bytes.
    pub max_frame_size: u64,
    /// Whether frame fragmentation is supported.
    #[serde(default)]
    pub fragmentation_supported: bool,
    /// Delivery guarantee requirement; both peers must require ordered reliable
    /// delivery for the session to proceed.
    #[serde(default = "default_ordered_reliable")]
    pub ordered_reliable: bool,
    /// The [`PeerId`] for this session endpoint.
    pub peer_id: PeerId,
    /// Session/graph identifier.
    pub session_id: String,
    /// Supported feature flags (e.g. `"shared-blob"`, `"signaling-relay"`).
    #[serde(default)]
    pub features: Vec<String>,
}

impl CapabilityHandshake {
    /// Create a handshake with protocol defaults (JSON codec, 1 MiB frame size,
    /// ordered-reliable, no features).
    pub fn new(peer_id: PeerId, session_id: impl Into<String>) -> Self {
        Self {
            protocol_id: PROTOCOL_ID.to_owned(),
            protocol_major_version: PROTOCOL_MAJOR_VERSION,
            codec: "json".to_owned(),
            max_frame_size: 1_048_576,
            fragmentation_supported: false,
            ordered_reliable: true,
            peer_id,
            session_id: session_id.into(),
            features: Vec::new(),
        }
    }

    /// Builder: set the codec negotiation token.
    #[must_use]
    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = codec.into();
        self
    }

    /// Builder: set the max frame size.
    #[must_use]
    pub fn with_max_frame_size(mut self, max_frame_size: u64) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Builder: set the features list.
    #[must_use]
    pub fn with_features(mut self, features: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.features = features.into_iter().map(Into::into).collect();
        self
    }

    /// Builder: set fragmentation support.
    #[must_use]
    pub fn with_fragmentation(mut self, supported: bool) -> Self {
        self.fragmentation_supported = supported;
        self
    }

    /// Whether this handshake is mutually compatible with `other`.
    ///
    /// Peers are compatible when both advertise [`PROTOCOL_ID`], both advertise
    /// [`PROTOCOL_MAJOR_VERSION`], their major versions and codecs agree, and
    /// both require ordered reliable delivery. Feature negotiation is
    /// caller-driven via [`features`](Self::features) / [`has_feature`](Self::has_feature).
    #[must_use]
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        self.protocol_id == PROTOCOL_ID
            && other.protocol_id == PROTOCOL_ID
            && self.protocol_major_version == PROTOCOL_MAJOR_VERSION
            && other.protocol_major_version == PROTOCOL_MAJOR_VERSION
            && self.protocol_major_version == other.protocol_major_version
            && self.codec == other.codec
            && self.ordered_reliable
            && other.ordered_reliable
    }

    /// Whether this peer advertises `feature`.
    #[must_use]
    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == feature)
    }
}

impl IpcMessage {
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    pub fn encode_json(&self) -> Result<Vec<u8>, EncodeError> {
        serde_json::to_vec(self).map_err(EncodeError::Json)
    }

    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    pub fn decode_json(bytes: &[u8]) -> Result<Self, DecodeError> {
        serde_json::from_slice(bytes).map_err(DecodeError::Json)
    }

    /// Encode using the `json-base64` capability (`#lzspecbase64`): `Inline` and
    /// `Payload` byte arrays travel as base64 strings instead of JSON arrays of
    /// integers (~4× wire reduction, ~3× parse cost). The field structure is
    /// otherwise identical to [`Self::encode_json`].
    #[cfg(feature = "json-base64")]
    pub fn encode_json_base64(&self) -> Result<Vec<u8>, EncodeError> {
        let mut value = serde_json::to_value(self).map_err(EncodeError::Json)?;
        base64_transform::encode_byte_arrays(&mut value);
        serde_json::to_vec(&value).map_err(EncodeError::Json)
    }

    /// Decode a `json-base64`-encoded frame (`#lzspecbase64`).
    #[cfg(feature = "json-base64")]
    pub fn decode_json_base64(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(DecodeError::Json)?;
        base64_transform::decode_byte_arrays(&mut value);
        serde_json::from_value(value).map_err(DecodeError::Json)
    }

    /// Encode with a sidecar string-intern table (`#lzspecintern`): repeated
    /// `type_tag` strings within the batch are deduplicated into a small
    /// `intern.strings` table and replaced by integer ids, cutting wire size
    /// when many nodes share few type tags. The decoded message is identical.
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    pub fn encode_json_intern(&self) -> Result<Vec<u8>, EncodeError> {
        let mut value = serde_json::to_value(self).map_err(EncodeError::Json)?;
        intern_transform::encode_intern(&mut value);
        serde_json::to_vec(&value).map_err(EncodeError::Json)
    }

    /// Decode an intern-table-encoded frame (`#lzspecintern`).
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    pub fn decode_json_intern(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(DecodeError::Json)?;
        intern_transform::decode_intern(&mut value);
        serde_json::from_value(value).map_err(DecodeError::Json)
    }

    #[cfg(feature = "ipc-msgpack")]
    pub fn encode_msgpack(&self) -> Result<Vec<u8>, EncodeError> {
        let mut buf = Vec::new();
        // `with_struct_map` is what `to_vec_named` does: named-field maps, not
        // positional arrays (protocol.md § Frame codecs).
        //
        // `with_human_readable` is the SELF-DESCRIBING flag (`#lzmsgpackparity`).
        // The optional-`key` omission in `NodeSnapshot`/`DeltaOp::NodeAdd` keys
        // off `Serializer::is_human_readable`, which rmp-serde reports as
        // `false` by default — so msgpack, a self-describing codec, was getting
        // the positional-Postcard treatment and writing `key: null` where
        // protocol.md § NodeKey requires it omitted. The flag is serde's only
        // encoder-supplied signal here, and for these types it means exactly
        // "self-describing": every branch that reads it is an omit-when-absent
        // decision, never a formatting one.
        let mut ser = rmp_serde::Serializer::new(&mut buf)
            .with_struct_map()
            .with_human_readable();
        serde::Serialize::serialize(self, &mut ser).map_err(EncodeError::Msgpack)?;
        Ok(buf)
    }

    #[cfg(feature = "ipc-msgpack")]
    pub fn decode_msgpack(bytes: &[u8]) -> Result<Self, DecodeError> {
        // Symmetric with `encode_msgpack`: the decoder must agree that msgpack
        // is self-describing, or a `Deserialize` impl that branches on the flag
        // would read the frame under the positional schema.
        let mut de = rmp_serde::Deserializer::from_read_ref(bytes).with_human_readable();
        serde::Deserialize::deserialize(&mut de).map_err(DecodeError::Msgpack)
    }

    #[cfg(feature = "ipc-binary")]
    pub fn encode_binary(&self) -> Result<Vec<u8>, EncodeError> {
        postcard::to_allocvec(self).map_err(EncodeError::Binary)
    }

    #[cfg(feature = "ipc-binary")]
    pub fn decode_binary(bytes: &[u8]) -> Result<Self, DecodeError> {
        postcard::from_bytes(bytes).map_err(DecodeError::Binary)
    }
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
#[derive(Debug)]
pub enum EncodeError {
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    Json(serde_json::Error),
    #[cfg(feature = "ipc-msgpack")]
    Msgpack(rmp_serde::encode::Error),
    #[cfg(feature = "ipc-binary")]
    Binary(postcard::Error),
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json(e) => write!(f, "JSON encode: {e}"),
            #[cfg(feature = "ipc-msgpack")]
            Self::Msgpack(e) => write!(f, "MessagePack encode: {e}"),
            #[cfg(feature = "ipc-binary")]
            Self::Binary(e) => write!(f, "binary encode: {e}"),
        }
    }
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json(e) => Some(e),
            #[cfg(feature = "ipc-msgpack")]
            Self::Msgpack(e) => Some(e),
            #[cfg(feature = "ipc-binary")]
            Self::Binary(e) => Some(e),
        }
    }
}

#[cfg(any(feature = "ffi", feature = "webrtc"))]
impl From<serde_json::Error> for EncodeError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(feature = "ipc-msgpack")]
impl From<rmp_serde::encode::Error> for EncodeError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::Msgpack(e)
    }
}

#[cfg(feature = "ipc-binary")]
impl From<postcard::Error> for EncodeError {
    fn from(e: postcard::Error) -> Self {
        Self::Binary(e)
    }
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
#[derive(Debug)]
pub enum DecodeError {
    #[cfg(any(feature = "ffi", feature = "webrtc"))]
    Json(serde_json::Error),
    #[cfg(feature = "ipc-msgpack")]
    Msgpack(rmp_serde::decode::Error),
    #[cfg(feature = "ipc-binary")]
    Binary(postcard::Error),
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json(e) => write!(f, "JSON decode: {e}"),
            #[cfg(feature = "ipc-msgpack")]
            Self::Msgpack(e) => write!(f, "MessagePack decode: {e}"),
            #[cfg(feature = "ipc-binary")]
            Self::Binary(e) => write!(f, "binary decode: {e}"),
        }
    }
}

#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(any(feature = "ffi", feature = "webrtc"))]
            Self::Json(e) => Some(e),
            #[cfg(feature = "ipc-msgpack")]
            Self::Msgpack(e) => Some(e),
            #[cfg(feature = "ipc-binary")]
            Self::Binary(e) => Some(e),
        }
    }
}

#[cfg(any(feature = "ffi", feature = "webrtc"))]
impl From<serde_json::Error> for DecodeError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[cfg(feature = "ipc-msgpack")]
impl From<rmp_serde::decode::Error> for DecodeError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::Msgpack(e)
    }
}

#[cfg(feature = "ipc-binary")]
impl From<postcard::Error> for DecodeError {
    fn from(e: postcard::Error) -> Self {
        Self::Binary(e)
    }
}

/// Transport sink for IPC messages.
pub trait IpcSink {
    /// Transport-specific error type.
    type Error;

    /// Send one IPC protocol message.
    fn send(&mut self, message: &IpcMessage) -> Result<(), Self::Error>;

    /// Send a snapshot.
    fn send_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Self::Error> {
        self.send(&IpcMessage::Snapshot(snapshot.clone()))
    }

    /// Send a delta.
    fn send_delta(&mut self, delta: &Delta) -> Result<(), Self::Error> {
        self.send(&IpcMessage::Delta(delta.clone()))
    }

    /// Send a CRDT anti-entropy sync frame (multi-writer plane, `#lzcrdtplane5`).
    fn send_crdt_sync(&mut self, sync: &CrdtSync) -> Result<(), Self::Error> {
        self.send(&IpcMessage::CrdtSync(sync.clone()))
    }
}

/// Transport source for IPC messages.
pub trait IpcSource {
    /// Transport-specific error type.
    type Error;

    /// Receive the next IPC message.
    ///
    /// `Ok(None)` means the source is currently exhausted or closed, depending
    /// on the transport implementation.
    fn recv(&mut self) -> Result<Option<IpcMessage>, Self::Error>;
}

fn can_read(permissions: &PeerPermissions, peer: PeerId, node: NodeId) -> bool {
    permissions.is_allowed(peer, RemoteOp::read(node))
}

fn write_header(bytes: &mut [u8], offset: usize, descriptor: ShmBlobRef) {
    let header = &mut bytes[offset..offset + SHM_BLOB_HEADER_LEN];
    write_u32(header, 0, SHM_BLOB_MAGIC);
    write_u16(header, 4, SHM_BLOB_VERSION);
    write_u16(header, 6, SHM_BLOB_HEADER_LEN as u16);
    write_u64(header, 8, descriptor.generation);
    write_u64(header, 16, descriptor.epoch);
    write_u64(header, 24, descriptor.len);
    write_u64(header, 32, descriptor.checksum);
}

fn read_header(bytes: &[u8], offset: usize) -> Result<ShmBlobRef, ShmBlobArenaError> {
    let header = &bytes[offset..offset + SHM_BLOB_HEADER_LEN];
    let magic = read_u32(header, 0);
    if magic != SHM_BLOB_MAGIC {
        return Err(ShmBlobArenaError::DescriptorMismatch { field: "magic" });
    }
    let version = read_u16(header, 4);
    if version != SHM_BLOB_VERSION {
        return Err(ShmBlobArenaError::DescriptorMismatch { field: "version" });
    }
    let header_len = read_u16(header, 6);
    if usize::from(header_len) != SHM_BLOB_HEADER_LEN {
        return Err(ShmBlobArenaError::DescriptorMismatch {
            field: "header_len",
        });
    }

    Ok(ShmBlobRef {
        offset: offset as u64,
        generation: read_u64(header, 8),
        epoch: read_u64(header, 16),
        len: read_u64(header, 24),
        checksum: read_u64(header, 32),
        // The arena header does not store `backend` — it is wire-level routing
        // metadata only. Default to `Shm`; `read_blob` normalizes it before
        // the equality check so a non-Shm descriptor still validates.
        backend: BlobBackendKind::Shm,
    })
}

fn mismatch_field(actual: ShmBlobRef, expected: ShmBlobRef) -> ShmBlobArenaError {
    let field = if actual.generation != expected.generation {
        "generation"
    } else if actual.epoch != expected.epoch {
        "epoch"
    } else if actual.len != expected.len {
        "len"
    } else if actual.checksum != expected.checksum {
        "checksum"
    } else {
        "offset"
    };

    ShmBlobArenaError::DescriptorMismatch { field }
}

fn checksum(payload: &[u8]) -> u64 {
    payload.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("slice size checked"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("slice size checked"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("slice size checked"),
    )
}

/// `#lzspecbase64`: walk a `serde_json::Value` tree to convert `Inline`/`Payload`
/// byte arrays between the JSON-u8 array form (canonical) and the base64 string
/// form (capability-gated). The variants are externally-tagged (`{"Inline":
/// [...]}` / `{"Payload": [...]}`) and always carry bytes, so there is no
/// ambiguity between a base64 string and a legitimate string field.
#[cfg(feature = "json-base64")]
mod base64_transform {
    use base64::{Engine as _, engine::general_purpose};
    use serde_json::Value;

    /// Field names whose array value is a byte payload.
    const BYTE_FIELDS: [&str; 2] = ["Inline", "Payload"];

    /// Replace every `{"Inline": [u8]}` / `{"Payload": [u8]}` array with a
    /// base64 string.
    pub fn encode_byte_arrays(value: &mut Value) {
        match value {
            Value::Object(map) => {
                for field in BYTE_FIELDS {
                    let replacement = map.get(field).filter(|v| v.is_array()).map(|arr| {
                        let bytes = json_array_to_bytes(arr);
                        Value::String(general_purpose::STANDARD.encode(&bytes))
                    });
                    if let Some(encoded) = replacement {
                        map.insert((*field).to_string(), encoded);
                    }
                }
                for (_, v) in map.iter_mut() {
                    encode_byte_arrays(v);
                }
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    encode_byte_arrays(item);
                }
            }
            _ => {}
        }
    }

    /// Replace every `{"Inline": "...string..."}` / `{"Payload": "...string..."}`
    /// base64 string with the JSON-u8 array form so `serde_json::from_value`
    /// reconstructs the typed struct.
    pub fn decode_byte_arrays(value: &mut Value) {
        match value {
            Value::Object(map) => {
                for field in BYTE_FIELDS {
                    let replacement = map
                        .get(field)
                        .and_then(|s| s.as_str())
                        .and_then(|text| general_purpose::STANDARD.decode(text).ok())
                        .map(|bytes| {
                            Value::Array(
                                bytes
                                    .into_iter()
                                    .map(|b| Value::from(u64::from(b)))
                                    .collect(),
                            )
                        });
                    if let Some(arr) = replacement {
                        map.insert((*field).to_string(), arr);
                    }
                }
                for (_, v) in map.iter_mut() {
                    decode_byte_arrays(v);
                }
            }
            Value::Array(items) => {
                for item in items.iter_mut() {
                    decode_byte_arrays(item);
                }
            }
            _ => {}
        }
    }

    fn json_array_to_bytes(value: &Value) -> Vec<u8> {
        value
            .as_array()
            .expect("checked array")
            .iter()
            .map(|n| {
                // This walks a tree THIS crate just produced with
                // `serde_json::to_value(&IpcMessage)`, so an `Inline`/`Payload`
                // element is always a `u8`. A non-integer or out-of-range element
                // means the tree was built by something else; substituting 0 (the
                // former `unwrap_or(0) as u8`) would silently ship corrupted bytes
                // under a checksum computed over the corruption.
                let raw = n.as_u64().unwrap_or_else(|| {
                    panic!("json-base64 encode: byte-array element is not an integer: {n}")
                });
                u8::try_from(raw).unwrap_or_else(|_| {
                    panic!("json-base64 encode: byte-array element out of u8 range: {raw}")
                })
            })
            .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_non_integer_byte_element_is_rejected_not_zeroed() {
            let mut value = serde_json::json!({ "Inline": ["nope"] });
            let panicked = std::panic::catch_unwind(move || encode_byte_arrays(&mut value));
            let err = panicked.expect_err("a non-integer byte element must not encode");
            let message = err
                .downcast_ref::<String>()
                .expect("panic payload is a formatted string");
            assert!(
                message.contains("not an integer") && message.contains("nope"),
                "the rejection names the offending element: {message}"
            );
        }

        #[test]
        fn an_out_of_range_byte_element_is_rejected_not_truncated() {
            let mut value = serde_json::json!({ "Payload": [256] });
            let panicked = std::panic::catch_unwind(move || encode_byte_arrays(&mut value));
            let err = panicked.expect_err("an out-of-range byte element must not encode");
            let message = err
                .downcast_ref::<String>()
                .expect("panic payload is a formatted string");
            assert!(
                message.contains("out of u8 range") && message.contains("256"),
                "the rejection names the offending element: {message}"
            );
        }

        #[test]
        fn a_well_formed_byte_array_still_encodes() {
            let mut value = serde_json::json!({ "Inline": [104, 105] });
            encode_byte_arrays(&mut value);
            assert_eq!(value["Inline"], serde_json::json!("aGk="));
        }
    }
}

/// `#lzspecintern`: walk a `serde_json::Value` tree to deduplicate repeated
/// `type_tag` strings into a sidecar `intern.strings` table, replacing each tag
/// with an integer id (`type_tag_id`). On decode, the reverse expansion restores
/// the canonical `type_tag` string so `serde_json::from_value` is unchanged.
#[cfg(any(feature = "ffi", feature = "webrtc"))]
mod intern_transform {
    use std::collections::HashMap;

    use serde_json::{Map, Value};

    const TYPE_TAG: &str = "type_tag";
    const TYPE_TAG_ID: &str = "type_tag_id";
    const INTERN: &str = "intern";

    /// Deduplicate `type_tag` strings under the batch root and emit
    /// `intern.strings` + per-node `type_tag_id`.
    pub fn encode_intern(value: &mut Value) {
        let Some(root) = value.as_object_mut() else {
            return;
        };
        // The batch root is the single key (Snapshot/Delta/CrdtSync).
        let Some(batch) = root.values_mut().next() else {
            return;
        };
        let Some(batch_map) = batch.as_object_mut() else {
            return;
        };

        let mut table: Vec<String> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        walk_encode(batch_map, &mut table, &mut index);

        if !table.is_empty() {
            let strings: Vec<Value> = table.into_iter().map(Value::String).collect();
            let mut intern = Map::new();
            intern.insert("strings".to_string(), Value::Array(strings));
            batch_map.insert(INTERN.to_string(), Value::Object(intern));
        }
    }

    /// Expand `intern.strings` + per-node `type_tag_id` back to canonical
    /// `type_tag` strings.
    pub fn decode_intern(value: &mut Value) {
        let Some(root) = value.as_object_mut() else {
            return;
        };
        let Some(batch) = root.values_mut().next() else {
            return;
        };
        let Some(batch_map) = batch.as_object_mut() else {
            return;
        };

        let strings: Vec<String> = batch_map
            .remove(INTERN)
            .and_then(|v| v.get("strings").cloned())
            .and_then(|s| s.as_array().map(|a| a.to_vec()))
            .map(|arr| {
                arr.into_iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        if !strings.is_empty() {
            walk_decode(batch_map, &strings);
        }
    }

    fn walk_encode(
        map: &mut Map<String, Value>,
        table: &mut Vec<String>,
        index: &mut HashMap<String, usize>,
    ) {
        if let Some(tag) = map.get(TYPE_TAG).and_then(|v| v.as_str()).map(String::from) {
            let id = *index.entry(tag).or_insert_with(|| {
                let id = table.len();
                table.push(map.get(TYPE_TAG).unwrap().as_str().unwrap().to_string());
                id
            });
            map.remove(TYPE_TAG);
            map.insert(TYPE_TAG_ID.to_string(), Value::from(id));
        }
        for (_, v) in map.iter_mut() {
            if let Some(child) = v.as_object_mut() {
                walk_encode(child, table, index);
            } else if let Some(items) = v.as_array_mut() {
                for item in items.iter_mut() {
                    if let Some(child) = item.as_object_mut() {
                        walk_encode(child, table, index);
                    }
                }
            }
        }
    }

    fn walk_decode(map: &mut Map<String, Value>, strings: &[String]) {
        if let Some(tag) = map
            .remove(TYPE_TAG_ID)
            .and_then(|v| v.as_u64())
            .and_then(|id| strings.get(id as usize))
        {
            map.insert(TYPE_TAG.to_string(), Value::String(tag.clone()));
        }
        for (_, v) in map.iter_mut() {
            if let Some(child) = v.as_object_mut() {
                walk_decode(child, strings);
            } else if let Some(items) = v.as_array_mut() {
                for item in items.iter_mut() {
                    if let Some(child) = item.as_object_mut() {
                        walk_decode(child, strings);
                    }
                }
            }
        }
    }
}
