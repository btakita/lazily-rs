//! Cross-process zero-copy transport — pluggable blob backends (`#lzzcpy`).
//!
//! Spec: `lazily-spec/docs/zero-copy-transport.md`.
//! Formal: `lazily-formal/LazilyFormal/ZeroCopyTransport.lean`.
//! C++ reference: `lazily-cpp/include/lazily/transport.hpp`.
//!
//! A large payload is not copied through the wire codec. The producer **spills**
//! it to a blob backend (the backend mints a [`ShmBlobRef`] descriptor) and ships
//! only the descriptor; the receiver **resolves** the descriptor against the same
//! backend and reads the bytes in place — zero copy. The [`BlobBackend`] trait
//! is the adapter seam:
//!
//! - [`InProcessBackend`] wraps [`ShmBlobArena`] — single address space (the FFI
//!   host / an editor plugin loaded in the same process).
//! - [`ArrowBackend`] holds Apache Arrow IPC stream bytes — the descriptor's
//!   bytes are an Arrow IPC stream the receiver imports as an `Array` /
//!   `RecordBatch` with no copy (bring your own `arrow` crate around the
//!   resolved `&[u8]`).
//! - [`ShmBackend`] is a POSIX `shm_open` + `mmap` region (Linux) — the
//!   cross-process backend. Gated behind the `shm` feature.
//!
//! Because the formal laws (spill-then-resolve identity, backend isolation,
//! ABA generation safety, checksum integrity) are stated only over a backend's
//! issued-blob table, they hold uniformly for every backend that maintains the
//! [`BlobBackend`] contract.

use crate::ipc::{
    BlobBackendKind, DeltaOp, IpcMessage, IpcValue, NodeState, ShmBlobArena, ShmBlobArenaError,
    ShmBlobRef,
};

/// A zero-copy view into a backend's resolved bytes.
///
/// Resolves to `None` (not `Some([])`) when the descriptor did not resolve
/// (unknown / stale-generation / corrupt-checksum / wrong-backend). An empty
/// payload that resolves correctly is `Some(&[])`.
pub type BlobView<'a> = Option<&'a [u8]>;

/// The adapter seam: a backend mints descriptors via [`write`](BlobBackend::write)
/// and resolves them zero-copy via [`read_view`](BlobBackend::read_view).
///
/// Entries are immutable + stable-addressed for any descriptor's lifetime. The
/// formal laws ([`resolve_write`](https://lazily.dev) identity, backend
/// isolation, ABA generation safety, checksum rejection) hold for every backend
/// by construction.
///
/// # Object safety
///
/// The trait is object-safe (`dyn BlobBackend`) so a [`BlobRouter`] can hold
/// heterogeneous backends. `read_view` returns a slice borrowing `&self`, so the
/// backend must outlive any resolved view.
pub trait BlobBackend {
    /// Which backend discriminator this adapter serves.
    fn kind(&self) -> BlobBackendKind;

    /// Mint a fresh descriptor for `bytes`: allocate a stable-addressed slot,
    /// store the bytes immutably, and return a descriptor whose checksum is the
    /// bytes' FNV-1a-64.
    fn write(&mut self, bytes: &[u8]) -> Result<ShmBlobRef, ShmBlobArenaError>;

    /// Resolve `descriptor` zero-copy — return the stored bytes iff
    /// `generation + epoch + len + checksum` all match; `None` otherwise.
    /// **No copy, no checksum recompute.**
    fn read_view(&self, descriptor: &ShmBlobRef) -> BlobView<'_>;

    /// Advance the validity epoch. Descriptors minted before an epoch advance
    /// no longer resolve (models compaction / restart).
    fn advance_epoch(&mut self);
}

// ─────────────────────────────────────────────────────────────────────────────
// InProcessBackend — wraps the existing ShmBlobArena (single address space).
// ─────────────────────────────────────────────────────────────────────────────

/// Default in-process backend: wraps [`ShmBlobArena`] for the single-address-space
/// case (the FFI host ↔ a binding loaded in the same process, an editor plugin).
///
/// Descriptors carry [`backend = InProcess`](BlobBackendKind::InProcess). The
/// backing [`ShmBlobArena`] is a fixed-capacity bump-allocate buffer with
/// wraparound; the generation/epoch/checksum guards reject stale descriptors
/// after wraparound or epoch advance. For an unbounded store, spill to a
/// [`ShmBackend`] (cross-process) instead.
pub struct InProcessBackend {
    arena: ShmBlobArena<Vec<u8>>,
    epoch: u64,
}

/// Default backing capacity (1 MiB). Tunable via [`InProcessBackend::with_capacity`].
pub const IN_PROCESS_DEFAULT_CAPACITY: usize = 1 << 20;

impl InProcessBackend {
    /// Create an in-process backend with the default 1 MiB capacity.
    pub fn new() -> Result<Self, ShmBlobArenaError> {
        Self::with_capacity(IN_PROCESS_DEFAULT_CAPACITY)
    }

    /// Create an in-process backend backed by a `capacity`-byte arena.
    pub fn with_capacity(capacity: usize) -> Result<Self, ShmBlobArenaError> {
        Ok(Self {
            arena: ShmBlobArena::with_capacity(capacity)?,
            epoch: 0,
        })
    }

    /// Wrap an existing arena at epoch 0.
    pub fn from_arena(arena: ShmBlobArena<Vec<u8>>) -> Self {
        Self { arena, epoch: 0 }
    }

    /// Borrow the backing arena.
    pub fn arena(&self) -> &ShmBlobArena<Vec<u8>> {
        &self.arena
    }

    /// Current validity epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Default for InProcessBackend {
    fn default() -> Self {
        Self::new().expect("IN_PROCESS_DEFAULT_CAPACITY >= SHM_BLOB_HEADER_LEN + 1")
    }
}

impl BlobBackend for InProcessBackend {
    fn kind(&self) -> BlobBackendKind {
        BlobBackendKind::InProcess
    }

    fn write(&mut self, bytes: &[u8]) -> Result<ShmBlobRef, ShmBlobArenaError> {
        let mut descriptor = self.arena.write_blob(self.epoch, bytes)?;
        descriptor.backend = BlobBackendKind::InProcess;
        Ok(descriptor)
    }

    fn read_view(&self, descriptor: &ShmBlobRef) -> BlobView<'_> {
        // Immediate epoch invalidation: a descriptor minted before an epoch
        // advance does not resolve even if its slot bytes are still intact.
        if descriptor.epoch != self.epoch {
            return None;
        }
        self.arena.read_blob(*descriptor).ok()
    }

    fn advance_epoch(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ArrowBackend — holds Apache Arrow IPC stream bytes (bring your own arrow).
// ─────────────────────────────────────────────────────────────────────────────

/// Apache Arrow blob backend: holds spilled payloads as Arrow IPC stream bytes
/// and resolves a descriptor to the buffer's raw bytes with no copy.
///
/// The descriptor's bytes **are** an Arrow IPC stream — a columnar consumer
/// imports them as an `Array` / `RecordBatch` zero-copy (the Arrow IPC format is
/// itself zero-copy across a shared buffer). This adapter stores the raw stream
/// bytes and tags the descriptor [`backend = Arrow`](BlobBackendKind::Arrow);
/// bring your own `arrow` crate to wrap the resolved `&[u8]` into typed Arrow.
///
/// Because Arrow's IPC format is zero-copy over a shared buffer, `shm` and
/// `arrow` compose: an Arrow batch can live in a [`ShmBackend`] region and be
/// resolved by either backend. New backends (RDMA/verbs, CUDA IPC) plug in by
/// implementing [`BlobBackend`] and adding a [`BlobBackendKind`] value.
pub struct ArrowBackend {
    arena: ShmBlobArena<Vec<u8>>,
    epoch: u64,
}

/// Default Arrow backing capacity (4 MiB — analytics payloads tend to be larger).
pub const ARROW_DEFAULT_CAPACITY: usize = 1 << 22;

impl ArrowBackend {
    /// Create an Arrow backend with the default 4 MiB capacity.
    pub fn new() -> Result<Self, ShmBlobArenaError> {
        Self::with_capacity(ARROW_DEFAULT_CAPACITY)
    }

    /// Create an Arrow backend backed by a `capacity`-byte arena.
    pub fn with_capacity(capacity: usize) -> Result<Self, ShmBlobArenaError> {
        Ok(Self {
            arena: ShmBlobArena::with_capacity(capacity)?,
            epoch: 0,
        })
    }

    /// Current validity epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Default for ArrowBackend {
    fn default() -> Self {
        Self::new().expect("ARROW_DEFAULT_CAPACITY >= SHM_BLOB_HEADER_LEN + 1")
    }
}

impl BlobBackend for ArrowBackend {
    fn kind(&self) -> BlobBackendKind {
        BlobBackendKind::Arrow
    }

    fn write(&mut self, bytes: &[u8]) -> Result<ShmBlobRef, ShmBlobArenaError> {
        let mut descriptor = self.arena.write_blob(self.epoch, bytes)?;
        descriptor.backend = BlobBackendKind::Arrow;
        Ok(descriptor)
    }

    fn read_view(&self, descriptor: &ShmBlobRef) -> BlobView<'_> {
        if descriptor.epoch != self.epoch {
            return None;
        }
        self.arena.read_blob(*descriptor).ok()
    }

    fn advance_epoch(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// POSIX shared-memory backend (Linux). Behind the `shm` feature.
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(all(unix, feature = "shm"))]
mod shm {
    use super::{BlobBackend, BlobBackendKind, BlobView, ShmBlobArenaError, ShmBlobRef};
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};

    const SHM_MAGIC: u64 = 0x4c5a_5348_424c_4f42; // "LZSHBLOB"
    const SLOT_HEADER_LEN: usize = 24; // generation + len + checksum (3 × u64)

    /// On-disk-ish header at the front of the mmap'd region.
    #[repr(C)]
    struct Header {
        magic: AtomicU64,
        capacity: u64,
        bump: AtomicU64,
        generation: AtomicU64,
        epoch: AtomicU64,
    }

    /// Per-slot inline metadata (written immediately before the payload bytes).
    #[repr(C)]
    struct SlotHeader {
        generation: u64,
        len: u64,
        checksum: u64,
    }

    const HEADER_LEN: usize = 40; // 5 × u64

    /// POSIX shared-memory backend: a fixed-capacity `shm_open` + `mmap` region
    /// with an atomic bump allocator. `write` is lock-free; cross-process
    /// multi-writer relies on the atomics being address-free (holds on
    /// Linux/x86-64). Validated by a `fork()` cross-process smoke test.
    ///
    /// # Limitations
    ///
    /// No GC/reclamation (bumps until capacity, then returns
    /// [`BlobTooLarge`](ShmBlobArenaError::BlobTooLarge)); Unix-only (POSIX
    /// `shm`). A managed region with reclamation plugs in behind the same
    /// [`BlobBackend`] interface.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lazily::{ShmBackend, BlobBackend};
    /// let mut parent = ShmBackend::create("/lazily_app_shm", 1 << 20).unwrap();
    /// let desc = parent.write(b"hello, shared world").unwrap();
    /// assert_eq!(parent.read_view(&desc), Some(&b"hello, shared world"[..]));
    /// // a child process opens the region by name and resolves `desc` zero-copy:
    /// //   let child = ShmBackend::open("/lazily_app_shm").unwrap();
    /// //   assert_eq!(child.read_view(&desc), Some(&b"hello, shared world"[..]));
    /// ShmBackend::unlink("/lazily_app_shm");
    /// ```
    pub struct ShmBackend {
        name: String,
        fd: std::os::fd::RawFd,
        base: *mut u8,
        capacity: usize,
        header: *mut Header,
    }

    // The mmap'd region is shared across processes; ShmBackend is Send + Sync
    // only within a process (the atomics on the region are address-free).
    unsafe impl Send for ShmBackend {}
    unsafe impl Sync for ShmBackend {}

    impl ShmBackend {
        fn open_raw(name: &str, capacity: usize, create: bool) -> io::Result<Self> {
            let c_name = ensure_leading_slash(name);
            let flags = if create {
                libc::O_RDWR | libc::O_CREAT
            } else {
                libc::O_RDWR
            };
            let fd = unsafe { libc::shm_open(c_name.as_ptr(), flags, 0o600) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            if create && unsafe { libc::ftruncate(fd, capacity as libc::off_t) } != 0 {
                let e = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(e);
            }
            let base = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    capacity,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if base == libc::MAP_FAILED {
                let e = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(e);
            }
            let base = base as *mut u8;
            let header = base as *mut Header;
            if create {
                unsafe {
                    (*header).magic.store(SHM_MAGIC, Ordering::Relaxed);
                    (*header).capacity = capacity as u64;
                    (*header).bump.store(HEADER_LEN as u64, Ordering::Relaxed);
                    (*header).generation.store(0, Ordering::Relaxed);
                    (*header).epoch.store(0, Ordering::Relaxed);
                }
            }
            Ok(Self {
                name: name.to_string(),
                fd,
                base,
                capacity,
                header,
            })
        }

        /// Create (or truncate) a named POSIX shared-memory region of `capacity`
        /// bytes and map it. The caller owns unlink timing — call [`unlink`]
        /// (`ShmBackend::unlink`) when no further readers/writers remain.
        pub fn create(name: &str, capacity: usize) -> Result<Self, ShmBlobArenaError> {
            Self::open_raw(name, capacity, true).map_err(shm_io_err)
        }

        /// Open (without creating) an existing named POSIX shared-memory region.
        /// A distinct process uses this to resolve descriptors minted by the
        /// creator.
        pub fn open(name: &str) -> Result<Self, ShmBlobArenaError> {
            // capacity is read from the in-region header after mapping a
            // best-effort size; the creator's ftruncate set the real size.
            let probe = Self::open_raw(name, HEADER_LEN, false).map_err(shm_io_err)?;
            let capacity = unsafe { (*probe.header).capacity } as usize;
            // remap at the real capacity.
            let name_owned = probe.name.clone();
            drop(probe);
            Self::open_raw(&name_owned, capacity, false).map_err(shm_io_err)
        }

        /// Remove the named region so it is reclaimed when all users unmap.
        pub fn unlink(name: &str) {
            let c_name = ensure_leading_slash(name);
            unsafe {
                libc::shm_unlink(c_name.as_ptr());
            }
        }

        /// The backend's validity epoch.
        pub fn epoch(&self) -> u64 {
            unsafe { (*self.header).epoch.load(Ordering::Acquire) }
        }

        /// The backend's current write cursor (bytes after the header).
        pub fn bump_offset(&self) -> u64 {
            unsafe { (*self.header).bump.load(Ordering::Acquire) }
        }
    }

    impl Drop for ShmBackend {
        fn drop(&mut self) {
            unsafe {
                if !self.base.is_null() {
                    libc::munmap(self.base as *mut libc::c_void, self.capacity);
                    self.base = std::ptr::null_mut();
                }
                if self.fd >= 0 {
                    libc::close(self.fd);
                    self.fd = -1;
                }
            }
        }
    }

    impl BlobBackend for ShmBackend {
        fn kind(&self) -> BlobBackendKind {
            BlobBackendKind::Shm
        }

        fn write(&mut self, bytes: &[u8]) -> Result<ShmBlobRef, ShmBlobArenaError> {
            let need = SLOT_HEADER_LEN + bytes.len();
            let header = unsafe { &*self.header };
            let off = header.bump.fetch_add(need as u64, Ordering::AcqRel);
            if off as usize + need > self.capacity {
                return Err(ShmBlobArenaError::BlobTooLarge {
                    len: bytes.len(),
                    max_len: self.capacity.saturating_sub(SLOT_HEADER_LEN + HEADER_LEN),
                });
            }
            let generation = header.generation.fetch_add(1, Ordering::AcqRel) + 1;
            let ep = header.epoch.load(Ordering::Acquire);
            let csum = fnv1a_64(bytes);
            unsafe {
                let slot = (self.base.add(off as usize)) as *mut SlotHeader;
                std::ptr::write_unaligned(
                    slot,
                    SlotHeader {
                        generation,
                        len: bytes.len() as u64,
                        checksum: csum,
                    },
                );
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    self.base.add(off as usize + SLOT_HEADER_LEN),
                    bytes.len(),
                );
            }
            Ok(ShmBlobRef {
                offset: off + SLOT_HEADER_LEN as u64,
                len: bytes.len() as u64,
                generation,
                epoch: ep,
                checksum: csum,
                backend: BlobBackendKind::Shm,
            })
        }

        fn read_view(&self, descriptor: &ShmBlobRef) -> BlobView<'_> {
            let off = descriptor.offset as usize;
            let slot_off = off.saturating_sub(SLOT_HEADER_LEN);
            if slot_off + SLOT_HEADER_LEN > self.capacity {
                return None;
            }
            let slot = unsafe { &*(self.base.add(slot_off) as *const SlotHeader) };
            if slot.generation != descriptor.generation {
                return None;
            }
            if slot.len != descriptor.len {
                return None;
            }
            if slot.checksum != descriptor.checksum {
                return None;
            }
            if unsafe { (*self.header).epoch.load(Ordering::Acquire) } != descriptor.epoch {
                return None;
            }
            if off + descriptor.len as usize > self.capacity {
                return None;
            }
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.base.add(off),
                    descriptor.len as usize,
                ))
            }
        }

        fn advance_epoch(&mut self) {
            unsafe {
                (*self.header).epoch.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn ensure_leading_slash(name: &str) -> std::ffi::CString {
        let prefixed = if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{name}")
        };
        std::ffi::CString::new(prefixed).expect("shm name contains no NUL")
    }

    fn shm_io_err(e: io::Error) -> ShmBlobArenaError {
        ShmBlobArenaError::BackendIo {
            detail: e.to_string(),
        }
    }

    fn fnv1a_64(bytes: &[u8]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        bytes.iter().fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
        })
    }
}

#[cfg(all(unix, feature = "shm"))]
pub use shm::ShmBackend;

// ─────────────────────────────────────────────────────────────────────────────
// Spill policy: replace large Inline payloads with a SharedBlob descriptor.
// ─────────────────────────────────────────────────────────────────────────────

/// If an [`IpcValue`] is [`Inline`](IpcValue::Inline) and `>= threshold` bytes,
/// write it to `backend` and replace it with a [`SharedBlob`](IpcValue::SharedBlob)
/// descriptor. Returns the number of bytes spilled (`0` if not spilled).
///
/// Payloads below the threshold stay inline — cheaper than a backend round-trip
/// for tiny values. The threshold is a session/deployment knob.
pub fn spill_value(value: &mut IpcValue, backend: &mut dyn BlobBackend, threshold: usize) -> usize {
    if let IpcValue::Inline(bytes) = value
        && bytes.len() >= threshold
    {
        match backend.write(bytes) {
            Ok(descriptor) => {
                let spilled = bytes.len();
                *value = IpcValue::SharedBlob(descriptor);
                return spilled;
            }
            Err(_) => return 0,
        }
    }
    0
}

/// Spill a [`NodeState::Payload`] above `threshold` to a SharedBlob descriptor.
fn spill_state(state: &mut NodeState, backend: &mut dyn BlobBackend, threshold: usize) -> usize {
    if let NodeState::Payload(bytes) = state
        && bytes.len() >= threshold
    {
        match backend.write(bytes) {
            Ok(descriptor) => {
                let spilled = bytes.len();
                *state = NodeState::SharedBlob(descriptor);
                return spilled;
            }
            Err(_) => return 0,
        }
    }
    0
}

/// Spill large payloads across an [`IpcMessage`]'s value/state sites: Snapshot
/// node states, Delta `CellSet`/`SlotValue` payloads + `NodeAdd` states, and
/// `CrdtSync` op states. Returns the total bytes spilled.
///
/// Each [`Inline`](IpcValue::Inline)/[`Payload`](NodeState::Payload) above
/// `threshold` is written to `backend` and replaced with a descriptor — the
/// message stays small on the wire. Sites already carrying a descriptor are
/// left untouched.
pub fn spill_message(
    message: &mut IpcMessage,
    backend: &mut dyn BlobBackend,
    threshold: usize,
) -> usize {
    let mut total = 0;
    match message {
        IpcMessage::Snapshot(snap) => {
            for node in &mut snap.nodes {
                total += spill_state(&mut node.state, backend, threshold);
            }
        }
        IpcMessage::Delta(delta) => {
            for op in &mut delta.ops {
                match op {
                    DeltaOp::CellSet { payload, .. } => {
                        total += spill_value(payload, backend, threshold);
                    }
                    DeltaOp::SlotValue { payload, .. } => {
                        total += spill_value(payload, backend, threshold);
                    }
                    DeltaOp::NodeAdd { state, .. } => {
                        total += spill_state(state, backend, threshold);
                    }
                    // The payload-free ops are listed rather than absorbed by a
                    // catch-all, so a future `DeltaOp` that carries bytes is a
                    // compile error here instead of a payload that silently
                    // bypasses spilling and ships inline.
                    DeltaOp::Invalidate { .. }
                    | DeltaOp::NodeRemove { .. }
                    | DeltaOp::EdgeAdd { .. }
                    | DeltaOp::EdgeRemove { .. } => {}
                }
            }
        }
        IpcMessage::CrdtSync(sync) => {
            for op in &mut sync.ops {
                total += spill_value(&mut op.state, backend, threshold);
            }
        }
        // Reliable-sync control frames carry no blob payload to spill.
        IpcMessage::ResyncRequest(_)
        | IpcMessage::OutboxAck(_)
        | IpcMessage::DeltaSinceRequest(_) => {}
    }
    total
}

/// Resolve an [`IpcValue`] against a single backend: inline bytes returned
/// directly, [`SharedBlob`](IpcValue::SharedBlob) resolved zero-copy. Returns
/// `None` if a SharedBlob fails to resolve (unknown/stale/corrupt). The
/// returned slice borrows from whichever of `value` or `backend` has the
/// shorter lifetime.
pub fn resolve_value<'a>(value: &'a IpcValue, backend: &'a dyn BlobBackend) -> BlobView<'a> {
    match value {
        IpcValue::Inline(bytes) => Some(bytes.as_slice()),
        IpcValue::SharedBlob(descriptor) => backend.read_view(descriptor),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BlobRouter — receiver-side multi-backend resolver.
// ─────────────────────────────────────────────────────────────────────────────

/// Receiver-side multi-backend resolver. Holds backends by [`BlobBackendKind`]
/// and resolves any descriptor by its `backend` discriminator — a `shm`
/// descriptor routes to the shm backend, an `arrow` descriptor to the arrow
/// backend, etc. (the `resolve_wrong_backend` theorem: a descriptor never
/// resolves against a backend of the wrong kind).
///
/// ```
/// # use lazily::{InProcessBackend, ArrowBackend, BlobBackend, BlobRouter, resolve_value, IpcValue};
/// let mut inproc = InProcessBackend::new().unwrap();
/// let mut arrow = ArrowBackend::new().unwrap();
/// let mut router = BlobRouter::new();
/// router.register(&inproc).register(&arrow);
///
/// let desc = inproc.write(b"in-process payload").unwrap();
/// let value = IpcValue::SharedBlob(desc);
/// // routes to the in_process backend by the descriptor's `backend` kind:
/// assert_eq!(resolve_value(&value, &inproc), Some(&b"in-process payload"[..]));
/// ```
pub struct BlobRouter<'a> {
    backends: [Option<&'a dyn BlobBackend>; 3],
}

impl<'a> BlobRouter<'a> {
    /// Create an empty router with no backends registered.
    pub fn new() -> Self {
        Self {
            backends: [None, None, None],
        }
    }

    /// Register a backend for its [`kind`](BlobBackend::kind). Replaces any
    /// previously-registered backend of the same kind. Returns `&mut self` for
    /// chaining.
    pub fn register(&mut self, backend: &'a dyn BlobBackend) -> &mut Self {
        self.backends[backend.kind() as usize] = Some(backend);
        self
    }

    /// Resolve a descriptor by routing to its `backend` kind. Returns `None` if
    /// no backend is registered for this kind, or the descriptor did not resolve.
    pub fn read_view(&self, descriptor: &ShmBlobRef) -> BlobView<'_> {
        let idx = descriptor.backend as usize;
        self.backends[idx].and_then(|b| b.read_view(descriptor))
    }

    /// Resolve an [`IpcValue`]: inline bytes returned directly, SharedBlob
    /// routed by the descriptor's `backend` discriminator. The returned slice
    /// borrows from whichever of `value` or `self` has the shorter lifetime.
    pub fn resolve<'b>(&'b self, value: &'b IpcValue) -> BlobView<'b> {
        match value {
            IpcValue::Inline(bytes) => Some(bytes.as_slice()),
            IpcValue::SharedBlob(descriptor) => self.read_view(descriptor),
        }
    }
}

impl<'a> Default for BlobRouter<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_eq(view: BlobView<'_>, expected: &[u8]) -> bool {
        match view {
            Some(b) => b == expected,
            None => false,
        }
    }

    // resolve_write identity: bytes spilled to the backend resolve zero-copy.
    #[test]
    fn in_process_resolve_write() {
        let mut backend = InProcessBackend::new().unwrap();
        let payload = [1, 2, 3, 4, 5, 6, 7, 8];
        let desc = backend.write(&payload).unwrap();
        assert_eq!(desc.backend, BlobBackendKind::InProcess);
        assert!(bytes_eq(backend.read_view(&desc), &payload));
    }

    #[test]
    fn arrow_resolve_write() {
        let mut backend = ArrowBackend::new().unwrap();
        let payload = [10, 20, 30, 40];
        let desc = backend.write(&payload).unwrap();
        assert_eq!(desc.backend, BlobBackendKind::Arrow);
        assert!(bytes_eq(backend.read_view(&desc), &payload));
    }

    // Backend isolation (resolve_wrong_backend): an in_process descriptor does
    // not resolve in an empty router; a shm descriptor does not resolve in an
    // in_process-only router.
    #[test]
    fn backend_isolation() {
        let mut inproc = InProcessBackend::new().unwrap();
        let desc = inproc.write(&[9, 9, 9]).unwrap();
        let router = BlobRouter::new(); // no backends registered
        assert_eq!(router.read_view(&desc), None);

        let mut router = BlobRouter::new();
        router.register(&inproc);
        assert!(router.read_view(&desc).is_some());

        // a shm-kind descriptor with no shm backend registered → None
        let mut shm_desc = desc;
        shm_desc.backend = BlobBackendKind::Shm;
        assert_eq!(router.read_view(&shm_desc), None);
    }

    // ABA generation safety (resolve_stale_generation): a stale generation rejects.
    #[test]
    fn stale_generation_rejects() {
        let mut backend = InProcessBackend::new().unwrap();
        let desc = backend.write(&[1, 2, 3]).unwrap();
        let mut stale = desc;
        stale.generation += 1;
        assert_eq!(backend.read_view(&stale), None);
    }

    // Checksum integrity (resolve_corrupt_checksum): a corrupted checksum rejects.
    #[test]
    fn corrupt_checksum_rejects() {
        let mut backend = InProcessBackend::new().unwrap();
        let desc = backend.write(&[4, 5, 6]).unwrap();
        let mut corrupt = desc;
        corrupt.checksum = corrupt.checksum.wrapping_add(1);
        assert_eq!(backend.read_view(&corrupt), None);
    }

    // epoch advance invalidates prior descriptors.
    #[test]
    fn epoch_advance_invalidates() {
        let mut backend = InProcessBackend::new().unwrap();
        let desc = backend.write(&[7, 8]).unwrap();
        assert!(backend.read_view(&desc).is_some());
        backend.advance_epoch();
        assert_eq!(backend.read_view(&desc), None);
    }

    // End-to-end transport round-trip: spill a large Inline payload → the
    // message now carries a descriptor; resolve via a BlobRouter yields the
    // original bytes (transport_roundtrip).
    #[test]
    fn spill_resolve_round_trip() {
        use crate::{Delta, DeltaOp, NodeId};

        let mut backend = InProcessBackend::new().unwrap();

        let big = vec![0x5Au8; 500];
        let mut msg = IpcMessage::Delta(Delta::next(
            1,
            vec![DeltaOp::slot_value(NodeId(7), big.clone())],
        ));

        let spilled = spill_message(&mut msg, &mut backend, 64);
        assert_eq!(spilled, big.len());

        // Register the backend after spilling (avoids overlapping borrows).
        let router = BlobRouter::new();
        let mut router = router;
        router.register(&backend);

        let IpcMessage::Delta(delta) = &msg else {
            panic!("expected Delta");
        };
        let DeltaOp::SlotValue { payload, .. } = &delta.ops[0] else {
            panic!("expected SlotValue");
        };
        assert!(matches!(payload, IpcValue::SharedBlob(_)));
        assert!(bytes_eq(router.resolve(payload), &big));
    }

    // Spill across Snapshot NodeState + CrdtSync op state.
    #[test]
    fn spill_snapshot_and_crdt() {
        use crate::{CrdtOp, CrdtSync, NodeId, NodeSnapshot, Snapshot, WireStamp};

        let mut backend = InProcessBackend::new().unwrap();
        let big = vec![0xABu8; 300];

        let mut msg = IpcMessage::Snapshot(Snapshot::new(
            1,
            vec![NodeSnapshot::payload(NodeId(1), "blob", big.clone())],
            vec![],
            vec![NodeId(1)],
        ));
        let spilled = spill_message(&mut msg, &mut backend, 64);
        assert_eq!(spilled, big.len());

        let stamp = WireStamp {
            wall_time: 1,
            logical: 0,
            peer: 1,
        };
        let mut crdt_msg = IpcMessage::CrdtSync(CrdtSync::new(
            vec![(1, stamp)],
            vec![CrdtOp::new(NodeId(1), stamp, big.clone())],
        ));
        let spilled = spill_message(&mut crdt_msg, &mut backend, 64);
        assert_eq!(spilled, big.len());
    }

    // Sub-threshold payloads stay inline.
    #[test]
    fn sub_threshold_stays_inline() {
        use crate::{Delta, DeltaOp, NodeId};

        let mut backend = InProcessBackend::new().unwrap();
        let mut msg = IpcMessage::Delta(Delta::next(
            1,
            vec![DeltaOp::slot_value(NodeId(1), vec![1, 2, 3])],
        ));
        let spilled = spill_message(&mut msg, &mut backend, 64);
        assert_eq!(spilled, 0);

        let IpcMessage::Delta(delta) = &msg else {
            panic!("expected Delta");
        };
        assert!(
            matches!(delta.ops[0], DeltaOp::SlotValue { ref payload, .. } if matches!(payload, IpcValue::Inline(_)))
        );
    }

    // Multi-backend routing: an arrow descriptor routes to the arrow backend,
    // an in_process descriptor to the in_process backend.
    #[test]
    fn multi_backend_routing() {
        let mut inproc = InProcessBackend::new().unwrap();
        let mut arrow = ArrowBackend::new().unwrap();

        let inproc_desc = inproc.write(b"inproc bytes").unwrap();
        let arrow_desc = arrow.write(b"arrow bytes").unwrap();

        let mut router = BlobRouter::new();
        router.register(&inproc).register(&arrow);

        assert!(bytes_eq(router.read_view(&inproc_desc), b"inproc bytes"));
        assert!(bytes_eq(router.read_view(&arrow_desc), b"arrow bytes"));
    }

    // Arrow IPC stream composition: the descriptor's bytes are an Arrow IPC
    // stream the receiver reads zero-copy (here a stand-in byte payload).
    #[test]
    fn arrow_ipc_stream_bytes() {
        let mut arrow = ArrowBackend::new().unwrap();
        // A real Arrow IPC stream; here a stand-in. The backend stores it and
        // resolves to the raw bytes — a columnar consumer wraps arrow-rs.
        let ipc_stream = [0x41, 0x52, 0x52, 0x4f, 0x57, 0x31, 0x00, 0x00];
        let desc = arrow.write(&ipc_stream).unwrap();
        assert_eq!(desc.backend, BlobBackendKind::Arrow);
        assert!(bytes_eq(arrow.read_view(&desc), &ipc_stream));
    }

    #[cfg(all(unix, feature = "shm"))]
    #[test]
    fn shm_backend_round_trip() {
        let name = format!("/lazily_shm_test_{}", std::process::id());
        ShmBackend::unlink(&name);
        let mut backend = ShmBackend::create(&name, 1 << 20).unwrap();
        let payload: Vec<u8> = (0..1000).map(|i| (i * 7 + 1) as u8).collect();
        let desc = backend.write(&payload).unwrap();
        assert_eq!(desc.backend, BlobBackendKind::Shm);
        assert!(bytes_eq(backend.read_view(&desc), &payload));
        backend.advance_epoch();
        assert_eq!(backend.read_view(&desc), None); // epoch advance invalidates
        ShmBackend::unlink(&name);
    }

    #[cfg(all(unix, feature = "shm"))]
    #[test]
    fn shm_backend_cross_process() {
        let name = format!("/lazily_shm_xproc_{}", std::process::id());
        ShmBackend::unlink(&name);
        let payload: Vec<u8> = (0..1000).map(|i| (i * 7 + 1) as u8).collect();

        let mut parent = ShmBackend::create(&name, 1 << 20).unwrap();
        let desc = parent.write(&payload).unwrap();
        assert_eq!(desc.backend, BlobBackendKind::Shm);

        let pid = unsafe { libc::fork() };
        if pid == 0 {
            // child: distinct address space; opens the region by name.
            let child = ShmBackend::open(&name).unwrap();
            let view = child.read_view(&desc);
            let ok = matches!(view, Some(b) if b == payload.as_slice());
            unsafe { libc::_exit(if ok { 0 } else { 1 }) };
        }
        let mut status = 0i32;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
        ShmBackend::unlink(&name);
    }
}
