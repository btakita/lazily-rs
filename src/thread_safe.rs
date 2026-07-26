use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::MaybeUninit;

#[cfg(not(feature = "vec_edges"))]
use smallvec::SmallVec;
#[cfg(feature = "instrumentation")]
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

use arc_swap::ArcSwapOption;
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};
use std::sync::Arc;

#[cfg(feature = "std_sync_mutex")]
type StateRwLock<T> = std::sync::RwLock<T>;
#[cfg(not(feature = "std_sync_mutex"))]
type StateRwLock<T> = parking_lot::RwLock<T>;

#[cfg(feature = "std_sync_mutex")]
fn read_state_inner(
    m: &std::sync::RwLock<ThreadSafeState>,
) -> std::sync::RwLockReadGuard<'_, ThreadSafeState> {
    m.read().expect("state rwlock poisoned")
}
#[cfg(not(feature = "std_sync_mutex"))]
fn read_state_inner(
    m: &parking_lot::RwLock<ThreadSafeState>,
) -> parking_lot::RwLockReadGuard<'_, ThreadSafeState> {
    m.read()
}

#[cfg(feature = "std_sync_mutex")]
fn lock_state_inner(
    m: &std::sync::RwLock<ThreadSafeState>,
) -> std::sync::RwLockWriteGuard<'_, ThreadSafeState> {
    m.write().expect("state rwlock poisoned")
}
#[cfg(not(feature = "std_sync_mutex"))]
fn lock_state_inner(
    m: &parking_lot::RwLock<ThreadSafeState>,
) -> parking_lot::RwLockWriteGuard<'_, ThreadSafeState> {
    m.write()
}

#[cfg(feature = "std_sync_mutex")]
type StateReadGuard<'a> = std::sync::RwLockReadGuard<'a, ThreadSafeState>;
#[cfg(not(feature = "std_sync_mutex"))]
type StateReadGuard<'a> = parking_lot::RwLockReadGuard<'a, ThreadSafeState>;

#[cfg(feature = "std_sync_mutex")]
type StateWriteGuard<'a> = std::sync::RwLockWriteGuard<'a, ThreadSafeState>;
#[cfg(not(feature = "std_sync_mutex"))]
type StateWriteGuard<'a> = parking_lot::RwLockWriteGuard<'a, ThreadSafeState>;
#[cfg(feature = "instrumentation")]
use std::time::Instant;

use crate::cell::{Computed, Source};
use crate::context::DrainExhaustion;
use crate::context::GraphNode;
use crate::context::SlotId;
use crate::context::{Read, Write};
use crate::effect::Effect;
#[cfg(feature = "instrumentation")]
use crate::instrumentation::ThreadSafeLockSite;
use crate::merge::MergePolicy;

type ThreadSafeAny = dyn Any + Send + Sync;
type ThreadSafeComputeFn = dyn Fn(&ThreadSafeContext) -> Box<ThreadSafeAny> + Send + Sync;
type ThreadSafeEqualsFn = dyn Fn(&ThreadSafeAny, &ThreadSafeAny) -> bool + Send + Sync;
type ThreadSafeCleanup = dyn FnOnce() + Send + Sync;
type ThreadSafeEffectFn =
    dyn Fn(&ThreadSafeContext) -> Option<Box<ThreadSafeCleanup>> + Send + Sync;

#[cfg(not(feature = "vec_edges"))]
type EdgeVec = SmallVec<[SlotId; 4]>;
#[cfg(feature = "vec_edges")]
type EdgeVec = Vec<SlotId>;

#[cfg(not(feature = "vec_edges"))]
type RootVec = SmallVec<[ThreadSafeInvalidationRoot; 4]>;
#[cfg(feature = "vec_edges")]
type RootVec = Vec<ThreadSafeInvalidationRoot>;

const HYBRID_THRESHOLD: usize = 16;

/// `SlotId`-keyed collections hashed with `SlotIdHasher` rather than SipHash
/// (#lzspecedgeindex). Same reasoning as `Context`: these keys are internally
/// allocated sequential integers, so collision resistance buys nothing and is
/// paid on every lookup. Measured 33% on wide fan-out in the single-threaded
/// context.
type SlotIdSet = HashSet<SlotId, crate::context::SlotIdHashBuilder>;
type SlotIdMap<V> = HashMap<SlotId, V, crate::context::SlotIdHashBuilder>;

/// Promotion threshold for dependency-edge lists (#lzspecedgeindex).
///
/// Higher than `HYBRID_THRESHOLD`, which governs short-lived propagation
/// scratch structures. Edge lists are scanned on every registration, and a
/// linear scan over a contiguous `SlotId` vector beats hashing well past the
/// scratch threshold — measured crossover in the single-threaded context is
/// near width 170. `HybridSet` never demotes, so there is no boundary
/// oscillation to absorb.
const EDGE_HYBRID_THRESHOLD: usize = 128;

enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Iterator for Either<L, R>
where
    L: Iterator,
    R: Iterator<Item = L::Item>,
{
    type Item = L::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Left(l) => l.next(),
            Self::Right(r) => r.next(),
        }
    }
}

enum HybridMap<V> {
    Small(Vec<(SlotId, V)>),
    Large(SlotIdMap<V>),
}

impl<V> Default for HybridMap<V> {
    fn default() -> Self {
        Self::Small(Vec::new())
    }
}

impl<V> HybridMap<V> {
    fn get(&self, id: SlotId) -> Option<&V> {
        match self {
            Self::Small(vec) => vec.iter().find(|(sid, _)| *sid == id).map(|(_, v)| v),
            Self::Large(map) => map.get(&id),
        }
    }

    fn get_mut(&mut self, id: SlotId) -> Option<&mut V> {
        match self {
            Self::Small(vec) => vec.iter_mut().find(|(sid, _)| *sid == id).map(|(_, v)| v),
            Self::Large(map) => map.get_mut(&id),
        }
    }

    /// Insert or overwrite, promoting above `threshold`.
    fn upsert_at(&mut self, id: SlotId, value: V, threshold: usize) {
        match self {
            Self::Small(vec) => {
                if let Some(entry) = vec.iter_mut().find(|(sid, _)| *sid == id) {
                    entry.1 = value;
                    return;
                }
                vec.push((id, value));
                if vec.len() > threshold {
                    *self = Self::Large(std::mem::take(vec).into_iter().collect());
                }
            }
            Self::Large(map) => {
                map.insert(id, value);
            }
        }
    }

    fn remove(&mut self, id: SlotId) {
        match self {
            Self::Small(vec) => {
                if let Some(pos) = vec.iter().position(|(sid, _)| *sid == id) {
                    vec.swap_remove(pos);
                }
            }
            Self::Large(map) => {
                map.remove(&id);
            }
        }
    }

    fn push(&mut self, id: SlotId, value: V) {
        match self {
            Self::Small(vec) => {
                vec.push((id, value));
                if vec.len() > HYBRID_THRESHOLD {
                    *self = Self::Large(vec.drain(..).collect());
                }
            }
            Self::Large(map) => {
                map.insert(id, value);
            }
        }
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (SlotId, &V)> {
        match self {
            Self::Small(vec) => Either::Left(vec.iter().map(|(id, v)| (*id, v))),
            Self::Large(map) => Either::Right(map.iter().map(|(id, v)| (*id, v))),
        }
    }

    fn into_entries(self) -> Vec<(SlotId, V)> {
        match self {
            Self::Small(vec) => vec,
            Self::Large(map) => map.into_iter().collect(),
        }
    }
}

#[derive(Clone)]
enum HybridSet {
    Small(Vec<SlotId>),
    Large(SlotIdSet),
}

impl Default for HybridSet {
    fn default() -> Self {
        Self::Small(Vec::new())
    }
}

impl HybridSet {
    fn from_vec(entries: Vec<SlotId>) -> Self {
        let mut set = Self::default();
        for id in entries {
            set.insert_at(id, EDGE_HYBRID_THRESHOLD);
        }
        set
    }

    fn contains(&self, id: SlotId) -> bool {
        match self {
            Self::Small(vec) => vec.contains(&id),
            Self::Large(set) => set.contains(&id),
        }
    }

    fn insert(&mut self, id: SlotId) -> bool {
        self.insert_at(id, HYBRID_THRESHOLD)
    }

    /// Insert, promoting to a hash set above `threshold`.
    fn insert_at(&mut self, id: SlotId, threshold: usize) -> bool {
        match self {
            Self::Small(vec) => {
                if vec.contains(&id) {
                    return false;
                }
                vec.push(id);
                if vec.len() > threshold {
                    *self = Self::Large(vec.drain(..).collect());
                }
                true
            }
            Self::Large(set) => set.insert(id),
        }
    }

    /// Remove, returning whether an entry was present. Swap-removes in the
    /// small representation, so order is not preserved — callers already
    /// tolerate that.
    fn remove(&mut self, id: SlotId) -> bool {
        match self {
            Self::Small(vec) => {
                if let Some(pos) = vec.iter().position(|eid| *eid == id) {
                    vec.swap_remove(pos);
                    true
                } else {
                    false
                }
            }
            Self::Large(set) => set.remove(&id),
        }
    }

    fn iter(&self) -> impl Iterator<Item = SlotId> + '_ {
        match self {
            Self::Small(vec) => Either::Left(vec.iter().copied()),
            Self::Large(set) => Either::Right(set.iter().copied()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Small(vec) => vec.len(),
            Self::Large(set) => set.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_vec(&self) -> Vec<SlotId> {
        self.iter().collect()
    }

    fn into_entries(self) -> Vec<SlotId> {
        match self {
            Self::Small(vec) => vec,
            Self::Large(set) => set.into_iter().collect(),
        }
    }
}
fn edge_insert(edges: &mut HybridSet, id: SlotId) -> bool {
    edges.insert_at(id, EDGE_HYBRID_THRESHOLD)
}

fn edge_remove(edges: &mut HybridSet, id: SlotId) -> bool {
    edges.remove(id)
}

fn dependent_edge_insert(
    edges: &mut HybridMap<ThreadSafeDependentKind>,
    id: SlotId,
    kind: ThreadSafeDependentKind,
) {
    edges.upsert_at(id, kind, EDGE_HYBRID_THRESHOLD);
}

fn dependent_edge_remove(edges: &mut HybridMap<ThreadSafeDependentKind>, id: SlotId) {
    edges.remove(id);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ThreadSafeContextId(usize);

struct ThreadSafeTrackingFrame {
    context_id: ThreadSafeContextId,
    node_id: SlotId,
    known_dependencies: EdgeVec,
    dependencies: SlotIdSet,
}

#[derive(Default)]
struct ThreadSafeBatchChanges {
    cells: EdgeVec,
    cell_clears: EdgeVec,
    slots: EdgeVec,
}

struct ThreadSafeBatchFrame {
    context_id: ThreadSafeContextId,
    changes: ThreadSafeBatchChanges,
}

thread_local! {
    static THREAD_SAFE_TRACKING_STACK: RefCell<Vec<ThreadSafeTrackingFrame>> =
        const { RefCell::new(Vec::new()) };
}

thread_local! {
    static THREAD_SAFE_BATCH_STACK: RefCell<Vec<ThreadSafeBatchFrame>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(feature = "instrumentation")]
thread_local! {
    static THREAD_SAFE_LOCK_SITE_STACK: RefCell<Vec<ThreadSafeLockSite>> =
        const { RefCell::new(Vec::new()) };
}

struct TrackingGuard {
    active: bool,
}

impl TrackingGuard {
    fn finish(mut self) -> SlotIdSet {
        self.active = false;
        THREAD_SAFE_TRACKING_STACK.with(|stack| {
            stack
                .borrow_mut()
                .pop()
                .map(|frame| frame.dependencies)
                .unwrap_or_default()
        })
    }
}

impl Drop for TrackingGuard {
    fn drop(&mut self) {
        if self.active {
            THREAD_SAFE_TRACKING_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
}

fn push_tracking_frame_with_known_dependencies(
    context_id: ThreadSafeContextId,
    node_id: SlotId,
    known_dependencies: EdgeVec,
) -> TrackingGuard {
    THREAD_SAFE_TRACKING_STACK.with(|stack| {
        stack.borrow_mut().push(ThreadSafeTrackingFrame {
            context_id,
            node_id,
            known_dependencies,
            dependencies: SlotIdSet::default(),
        });
    });
    TrackingGuard { active: true }
}

fn track_dependency(context_id: ThreadSafeContextId, dependency_id: SlotId) -> Option<SlotId> {
    THREAD_SAFE_TRACKING_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        for frame in stack.iter_mut().rev() {
            if frame.context_id == context_id {
                let newly_tracked = frame.dependencies.insert(dependency_id);
                if newly_tracked && !frame.known_dependencies.contains(&dependency_id) {
                    return Some(frame.node_id);
                }
                return None;
            }
        }
        None
    })
}

fn push_batch_frame(context_id: ThreadSafeContextId) {
    THREAD_SAFE_BATCH_STACK.with(|stack| {
        stack.borrow_mut().push(ThreadSafeBatchFrame {
            context_id,
            changes: ThreadSafeBatchChanges::default(),
        });
    });
}

fn pop_batch_frame(context_id: ThreadSafeContextId) -> ThreadSafeBatchChanges {
    THREAD_SAFE_BATCH_STACK.with(|stack| {
        let frame = stack
            .borrow_mut()
            .pop()
            .expect("ThreadSafeContext batch frame stack underflow");
        assert_eq!(
            frame.context_id, context_id,
            "ThreadSafeContext batch frame mismatch"
        );
        frame.changes
    })
}

fn queue_batch_change<F>(context_id: ThreadSafeContextId, apply: F) -> bool
where
    F: FnOnce(&mut ThreadSafeBatchChanges),
{
    THREAD_SAFE_BATCH_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(frame) = stack
            .iter_mut()
            .rev()
            .find(|frame| frame.context_id == context_id)
        else {
            return false;
        };
        apply(&mut frame.changes);
        true
    })
}

#[cfg(feature = "instrumentation")]
struct ThreadSafeLockSiteGuard;

#[cfg(feature = "instrumentation")]
impl Drop for ThreadSafeLockSiteGuard {
    fn drop(&mut self) {
        THREAD_SAFE_LOCK_SITE_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

#[cfg(feature = "instrumentation")]
fn push_thread_safe_lock_site(site: ThreadSafeLockSite) -> ThreadSafeLockSiteGuard {
    THREAD_SAFE_LOCK_SITE_STACK.with(|stack| {
        stack.borrow_mut().push(site);
    });
    ThreadSafeLockSiteGuard
}

#[cfg(feature = "instrumentation")]
fn current_thread_safe_lock_site() -> ThreadSafeLockSite {
    THREAD_SAFE_LOCK_SITE_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .copied()
            .unwrap_or(ThreadSafeLockSite::Other)
    })
}

struct ThreadSafeComputedNode {
    value: Option<Arc<ThreadSafeAny>>,
    equals: Option<Arc<ThreadSafeEqualsFn>>,
    dependencies: HybridSet,
    dependents: HybridSet,
    fast_path: Arc<ThreadSafeSlotFastPath>,
    dirty: bool,
    force_recompute: bool,
    revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThreadSafeDependentKind {
    Slot,
    Effect,
}

/// Runtime-selectable cached-read strategy for [`ThreadSafeContext`]
/// (#rdstrat1). Both paths are compiled in; the mode is chosen at context
/// construction (`ThreadSafeContext::with_read_strategy`), defaulting to
/// `LowConcurrency`. See `SPEC.md` → *Lock strategy evaluation*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadStrategy {
    /// `parking_lot::RwLock` reads — optimal uncontended / low core counts.
    #[default]
    LowConcurrency,
    /// `arc_swap::ArcSwapOption` wait-free reads — optimal at 8+ cores
    /// (#vd5v); a few percent slower uncontended.
    HighConcurrency,
}

/// Maximum inline byte capacity for the small-`Copy` seqlock fast path
/// (#rdstrat2). Covers common small reactive values (scalars, small `Copy`
/// structs, a `usize` triple) without heap indirection. A `Copy` type larger
/// than this — or any non-`Copy` type — falls back to the strategy-selected
/// `Locked`/`LockFree` path.
const INLINE_CAP: usize = 24;
/// Maximum alignment the inline buffer guarantees. The buffer is declared with
/// this alignment; a `Copy` type whose alignment exceeds it falls back.
const INLINE_ALIGN: usize = 16;

/// Per-slot inline-eligibility record, captured at slot creation while the
/// concrete `T` is in scope. `size` is needed on the type-erased store path to
/// memcpy exactly `size_of::<T>()` bytes out of the published value.
#[derive(Clone, Copy)]
struct InlineSpec {
    size: usize,
}

/// Wait-free single-writer / multi-reader seqlock storing a small `Copy` value
/// inline (#rdstrat2). The single-writer invariant is upheld by the caller:
/// every `write` runs while holding the `ThreadSafeContext` graph state write
/// lock (value publish in `recompute_slot_now`, clear in `apply_locked`), while
/// readers (`read`) run lock-free. `seq` even = stable, odd = write in
/// progress; a reader that observes an odd or changed `seq` discards its byte
/// snapshot and retries, so no `T` is ever reconstructed from a torn buffer.
///
/// The value bytes live in `[AtomicU8; INLINE_CAP]` and are read/written with
/// **relaxed atomic** per-byte ops — not a plain `memcpy` — so a reader racing
/// the writer is well-defined (no data race / UB), unlike a classic non-atomic
/// seqlock. The `seq` counter (Release on the writer's closing store, Acquire
/// on the reader's bracketing loads, plus the canonical Acquire fence before
/// the second load) makes an accepted byte snapshot a consistent image of one
/// publish. The seqlock is the inner safety envelope; the outer atomic
/// `cache_revision` + `dirty`/`force_recompute` checks in `read_fresh` reject
/// reads racing a publish/invalidation, identically to the `Locked`/`LockFree`
/// paths. The orderings are modeled in `thread_safe_loom::inline_seqlock_*`
/// (raw torn-read rejection exhaustively; the combined envelope model is
/// preemption-bounded because the unbounded space is non-terminating).
struct InlineSeqlock {
    seq: AtomicUsize,
    occupied: AtomicBool,
    size: usize,
    buf: [AtomicU8; INLINE_CAP],
}

impl InlineSeqlock {
    fn new(spec: InlineSpec) -> Self {
        Self {
            seq: AtomicUsize::new(0),
            occupied: AtomicBool::new(false),
            size: spec.size,
            buf: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }

    /// Publish (single writer). `src` points at `self.size` readable bytes of a
    /// `Copy` `T` to store, or `None` to clear. Serialized against other writers
    /// by the graph state write lock.
    ///
    /// # Safety
    /// When `src` is `Some(ptr)`, `ptr` must be valid for reads of `self.size`
    /// bytes for the duration of the call.
    unsafe fn write(&self, src: Option<*const u8>) {
        let begin = self.seq.load(Ordering::Relaxed).wrapping_add(1);
        // Enter the odd (write-in-progress) phase, then order it before the byte
        // mutation so a reader that observes the odd marker retries.
        self.seq.store(begin, Ordering::Release);
        std::sync::atomic::fence(Ordering::Release);
        match src {
            Some(ptr) => {
                for i in 0..self.size {
                    // SAFETY: `ptr` is valid for `self.size` reads (caller
                    // contract); `i < self.size <= INLINE_CAP` indexes the buffer.
                    let byte = unsafe { *ptr.add(i) };
                    self.buf[i].store(byte, Ordering::Relaxed);
                }
                self.occupied.store(true, Ordering::Relaxed);
            }
            None => {
                self.occupied.store(false, Ordering::Relaxed);
            }
        }
        // Leave the even (stable) phase; Release publishes the byte stores to a
        // reader whose closing Acquire-fenced load observes this even value.
        self.seq.store(begin.wrapping_add(1), Ordering::Release);
    }

    /// Wait-free read of the published value as `T`. Returns `None` when the
    /// slot is empty. The bytes are interpreted as `T` only after a stable even
    /// `seq` proves the snapshot is not torn.
    ///
    /// # Safety
    /// `T` must be the `Copy` type captured at slot creation, with
    /// `size_of::<T>() == self.size`. The caller asserts the matching `TypeId`.
    unsafe fn read<T>(&self) -> Option<T> {
        debug_assert_eq!(std::mem::size_of::<T>(), self.size);
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                // Write in progress; retry.
                std::hint::spin_loop();
                continue;
            }
            let occupied = self.occupied.load(Ordering::Relaxed);
            let mut snapshot = MaybeUninit::<T>::uninit();
            if occupied {
                let dst = snapshot.as_mut_ptr() as *mut u8;
                for i in 0..self.size {
                    let byte = self.buf[i].load(Ordering::Relaxed);
                    // SAFETY: `i < self.size == size_of::<T>()`, so `dst.add(i)`
                    // is in bounds of the `MaybeUninit<T>` storage.
                    unsafe { *dst.add(i) = byte };
                }
            }
            // Pair with the writer's closing Release store: if `s2` equals the
            // even `s1`, the byte loads above observed exactly this publish.
            std::sync::atomic::fence(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Relaxed);
            if s1 == s2 {
                return if occupied {
                    // SAFETY: the stable, unchanged even `seq` proves no write
                    // overlapped the byte loads, so `snapshot` holds a complete
                    // `T` (which is `Copy`, so the bitwise read is sound).
                    Some(unsafe { snapshot.assume_init() })
                } else {
                    None
                };
            }
            std::hint::spin_loop();
        }
    }
}

/// Read-mostly cached-value sidecar storage. The variant is fixed per slot at
/// creation; `Locked`/`LockFree` are chosen from the owning context's
/// [`ReadStrategy`], while `Inline` subsumes both for small `Copy` values
/// (#rdstrat2). All three carry the same atomic `cache_revision` validation
/// envelope, so correctness is identical.
///
/// `LockFree` stores `Arc<Arc<dyn Any>>` because `arc-swap`'s `RefCnt` is
/// `Sized`-only (the inner `Arc` is a sized fat pointer); the extra outer `Arc`
/// is allocated only on the cold publish path, never on the read. `Inline`
/// stores the value's bytes directly behind a seqlock — no `Arc`, no heap
/// indirection, no refcount traffic on either read or publish.
enum CachedReadStorage {
    Locked(RwLock<Option<Arc<ThreadSafeAny>>>),
    LockFree(ArcSwapOption<Arc<ThreadSafeAny>>),
    Inline(InlineSeqlock),
}

impl CachedReadStorage {
    fn new(strategy: ReadStrategy, inline: Option<InlineSpec>) -> Self {
        if let Some(spec) = inline {
            // Inline is optimal for small `Copy` values in both modes; the
            // runtime `ReadStrategy` only governs the large/non-`Copy` fallback.
            return CachedReadStorage::Inline(InlineSeqlock::new(spec));
        }
        match strategy {
            ReadStrategy::LowConcurrency => CachedReadStorage::Locked(RwLock::new(None)),
            ReadStrategy::HighConcurrency => CachedReadStorage::LockFree(ArcSwapOption::empty()),
        }
    }

    fn store(&self, value: Option<Arc<ThreadSafeAny>>) {
        match self {
            CachedReadStorage::Locked(lock) => *lock.write() = value,
            CachedReadStorage::LockFree(swap) => swap.store(value.map(Arc::new)),
            CachedReadStorage::Inline(seqlock) => match &value {
                Some(arc) => {
                    // The erased value's data pointer addresses the `T` bytes.
                    // `arc` is alive across the call, so the source is valid.
                    let erased: &ThreadSafeAny = &**arc;
                    let src = erased as *const ThreadSafeAny as *const u8;
                    // SAFETY: `src` is valid for `seqlock.size` (== size_of::<T>)
                    // reads while `arc` is held; single-writer invariant holds.
                    unsafe { seqlock.write(Some(src)) };
                }
                None => {
                    // SAFETY: clear path; no source bytes read.
                    unsafe { seqlock.write(None) };
                }
            },
        }
    }
}

/// Read-mostly cached-value sidecar storage for cells. Mirrors the slot
/// [`CachedReadStorage`] read-scaling design but adapted for cell semantics: the
/// value is always present (a cell is never unset), and `set_if_changed` needs
/// an atomic compare-and-set against the prior value (#lzcellread).
///
/// - `Locked`: `RwLock` shared reads (the read-scaling win over the prior plain
///   `Mutex` — concurrent readers no longer serialize); exclusive writes for
///   `set_if_changed`. Used for non-`Copy` values in both [`ReadStrategy`] modes
///   (`RwLock` shared reads already scale well; `arc-swap`'s compare-and-swap
///   for `set_if_changed` adds complexity with marginal gain).
/// - `Inline`: wait-free seqlock reads for small `Copy` values. Writers are
///   serialized by a lightweight `Mutex<()>` (readers never touch it); the
///   [`InlineSeqlock`] is the inner torn-read safety envelope, identical to the
///   slot inline path.
enum CellCachedReadStorage {
    Locked(RwLock<Arc<ThreadSafeAny>>),
    Inline {
        seqlock: InlineSeqlock,
        writer: Mutex<()>,
    },
}

impl CellCachedReadStorage {
    fn new(
        strategy: ReadStrategy,
        inline: Option<InlineSpec>,
        initial: Arc<ThreadSafeAny>,
    ) -> Self {
        if let Some(spec) = inline {
            let seqlock = InlineSeqlock::new(spec);
            let erased: &ThreadSafeAny = &*initial;
            let src = erased as *const ThreadSafeAny as *const u8;
            // SAFETY: `initial` is alive across the call; `spec.size` bytes from
            // its data pointer are valid reads. Single-writer: the cell was just
            // created, so no other writer can race. The seqlock copies the bytes
            // into its buffer before `write` returns, so `initial` may drop
            // after this call.
            unsafe { seqlock.write(Some(src)) };
            // Rebind the writer Mutex; `initial`'s bytes now live in the seqlock
            // buffer, so the source `Arc` is no longer needed.
            drop(initial);
            Self::Inline {
                seqlock,
                writer: Mutex::new(()),
            }
        } else {
            // RwLock shared reads are the right choice for both read strategies:
            // the win over the former Mutex is shared reads, and arc-swap's
            // compare-and-swap for set_if_changed is not warranted.
            let _ = strategy;
            Self::Locked(RwLock::new(initial))
        }
    }

    fn get<T>(&self) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        match self {
            Self::Locked(lock) => {
                let value = lock.read();
                // SAFETY: the caller checked `type_id == TypeId::of::<T>()`, so
                // the stored value is a `T`.
                unsafe { &*(&**value as *const ThreadSafeAny as *const T) }.clone()
            }
            Self::Inline { seqlock, .. } => {
                // SAFETY: `Inline` is only created for the `Copy` type captured
                // at cell creation; the caller's `type_id` assert proves `T`
                // matches, so `size_of::<T>() == seqlock.size` and the bitwise
                // read is sound. A cell value is always present (never unset),
                // so the `Option` is always `Some`.
                unsafe { seqlock.read::<T>() }.expect("cell inline value never unset")
            }
        }
    }

    fn set_if_changed<T>(&self, new_value: T) -> bool
    where
        T: PartialEq + Send + Sync + 'static,
    {
        match self {
            Self::Locked(lock) => {
                let mut value = lock.write();
                // SAFETY: the caller checked `type_id == TypeId::of::<T>()`.
                let old = unsafe { &*(&**value as *const ThreadSafeAny as *const T) };
                if *old == new_value {
                    return false;
                }
                *value = Arc::new(new_value);
                true
            }
            Self::Inline { seqlock, writer } => {
                // Serialize compare+write so two concurrent `set_if_changed`
                // calls cannot both pass the PartialEq check against the same
                // stale old value. Readers stay lock-free via the seqlock and
                // never contend on this Mutex.
                let _guard = writer.lock();
                // SAFETY: `Inline` is only created for the matching `Copy` `T`.
                // Holding the writer Mutex guarantees no concurrent seqlock
                // write, so this read observes a stable even `seq`.
                let old = unsafe { seqlock.read::<T>() }.expect("cell inline value never unset");
                if old == new_value {
                    return false;
                }
                let src = &new_value as *const T as *const u8;
                // SAFETY: `new_value` is alive across the call; single-writer
                // (writer Mutex held). The seqlock copies the bytes before
                // `write` returns, so `new_value` may drop after.
                unsafe { seqlock.write(Some(src)) };
                true
            }
        }
    }
}

/// Inline-eligibility decision for a value type. Returns `Some` only for `Copy`
/// `T` that fits the inline buffer (size + alignment), so the unchecked bitwise
/// store/read is sound (a torn read of a `Copy` value has no `Drop`/ownership
/// hazard, and the size/align bound keeps the memcpy in-bounds and aligned).
///
/// This is a `Copy`-bounded free function rather than an automatic probe on the
/// generic slot constructors **by necessity**: stable Rust cannot branch on
/// `T: Copy` inside a generic fn that lacks the bound (method resolution is
/// pre-monomorphization, so a `Copy`-gated inherent impl is never *applicable*
/// where the bound is unprovable; that needs nightly `specialization`). The
/// inline path is therefore opt-in through the `*_copy` constructors, which
/// carry the `Copy` bound and call this. See `SPEC.md` → *Typed cache
/// fast-path* and `plan-lazily-0.10-read-strategy.md` Phase 2.
fn inline_spec_for<T: Copy + 'static>() -> Option<InlineSpec> {
    if std::mem::size_of::<T>() <= INLINE_CAP && std::mem::align_of::<T>() <= INLINE_ALIGN {
        Some(InlineSpec {
            size: std::mem::size_of::<T>(),
        })
    } else {
        None
    }
}

struct ThreadSafeSlotFastPath {
    // Read-mostly cached-value sidecar (#vd5v / #rdstrat1 / #rdstrat2). The
    // inline `type_id` proves `T` so a reader can reconstruct `&T`/`T` without a
    // vtable; the atomic `cache_revision` envelope rejects mid-read
    // invalidation / publish races. Storage variant (RwLock vs arc-swap vs
    // inline seqlock) is selected at creation from the context's `ReadStrategy`
    // and the value's inline eligibility (small `Copy`).
    value: CachedReadStorage,
    type_id: TypeId,
    cache_revision: AtomicU64,
    dirty: AtomicBool,
    force_recompute: AtomicBool,
    /// Bumped by `mark_dirty` / `clear` to detect concurrent invalidation during
    /// recompute. Captured by `begin_recompute` and checked after compute
    /// (#lzstateinvalidation — moved from the recompute Mutex to an atomic so
    /// `mark_dirty` is Mutex-free).
    invalidation_revision: AtomicU64,
    compute: Arc<ThreadSafeComputeFn>,
    dependencies: Mutex<HybridSet>,
    slot_dependency_count: AtomicUsize,
    recompute: Mutex<ThreadSafeSlotRecomputeState>,
    recompute_condvar: Condvar,
    dependents: Mutex<HybridMap<ThreadSafeDependentKind>>,
}

impl ThreadSafeSlotFastPath {
    fn new(
        compute: Arc<ThreadSafeComputeFn>,
        initial_dependencies: EdgeVec,
        type_id: TypeId,
        strategy: ReadStrategy,
        inline: Option<InlineSpec>,
    ) -> Self {
        let slot_dependency_count = initial_dependencies.len();
        Self {
            value: CachedReadStorage::new(strategy, inline),
            type_id,
            cache_revision: AtomicU64::default(),
            dirty: AtomicBool::default(),
            force_recompute: AtomicBool::default(),
            invalidation_revision: AtomicU64::default(),
            compute,
            dependencies: Mutex::new(HybridSet::from_vec(
                initial_dependencies.into_iter().collect(),
            )),
            slot_dependency_count: AtomicUsize::new(slot_dependency_count),
            recompute: Mutex::new(ThreadSafeSlotRecomputeState::default()),
            recompute_condvar: Condvar::new(),
            dependents: Mutex::new(HybridMap::default()),
        }
    }

    fn compute(&self) -> Arc<ThreadSafeComputeFn> {
        Arc::clone(&self.compute)
    }

    /// `read_fresh` for `get_arc` (`#lzrsgetarc`): clones the published `Arc`
    /// instead of the value behind it, under the same
    /// `cache_revision`/`dirty`/`force_recompute` envelope.
    ///
    /// `Inline` returns `None` — it stores `T` bitwise with no box to share, so
    /// the caller falls back to the locked node read. That is the right split:
    /// inline storage is only ever selected for small `Copy` values, which is
    /// exactly the case where `get` is cheaper than `get_arc` anyway.
    fn read_fresh_arc<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let cache_revision = self.cache_revision.load(Ordering::Acquire);
        if self.dirty.load(Ordering::Acquire) || self.force_recompute.load(Ordering::Acquire) {
            return None;
        }

        let value = match &self.value {
            CachedReadStorage::Locked(lock) => lock.read().as_ref().map(|value| {
                assert!(self.type_id == TypeId::of::<T>(), "type mismatch in slot");
                Arc::clone(value)
                    .downcast::<T>()
                    .expect("type mismatch in slot")
            }),
            CachedReadStorage::LockFree(swap) => {
                let snapshot = swap.load();
                snapshot.as_ref().map(|outer| {
                    let value: &Arc<ThreadSafeAny> = outer;
                    assert!(self.type_id == TypeId::of::<T>(), "type mismatch in slot");
                    Arc::clone(value)
                        .downcast::<T>()
                        .expect("type mismatch in slot")
                })
            }
            CachedReadStorage::Inline(_) => None,
        };
        if self.cache_revision.load(Ordering::Acquire) != cache_revision
            || self.dirty.load(Ordering::Acquire)
            || self.force_recompute.load(Ordering::Acquire)
        {
            return None;
        }
        value
    }

    fn read_fresh<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cache_revision = self.cache_revision.load(Ordering::Acquire);
        if self.dirty.load(Ordering::Acquire) || self.force_recompute.load(Ordering::Acquire) {
            return None;
        }

        // Read the published snapshot via the selected strategy (wait-free load
        // for LockFree, read lock for Locked, wait-free seqlock for Inline).
        let value = match &self.value {
            CachedReadStorage::Locked(lock) => lock.read().as_ref().map(|value| {
                assert!(self.type_id == TypeId::of::<T>(), "type mismatch in slot");
                let erased: &ThreadSafeAny = &**value;
                unsafe { &*(erased as *const ThreadSafeAny as *const T) }.clone()
            }),
            CachedReadStorage::LockFree(swap) => {
                let snapshot = swap.load();
                snapshot.as_ref().map(|outer| {
                    let value: &Arc<ThreadSafeAny> = outer;
                    assert!(self.type_id == TypeId::of::<T>(), "type mismatch in slot");
                    let erased: &ThreadSafeAny = &**value;
                    unsafe { &*(erased as *const ThreadSafeAny as *const T) }.clone()
                })
            }
            CachedReadStorage::Inline(seqlock) => {
                assert!(self.type_id == TypeId::of::<T>(), "type mismatch in slot");
                // SAFETY: `Inline` is only created for the `Copy` type captured
                // at slot creation; the `type_id` assert proves `T` matches it,
                // so `size_of::<T>() == seqlock.size` and the bitwise read is
                // sound.
                unsafe { seqlock.read::<T>() }
            }
        };
        if self.cache_revision.load(Ordering::Acquire) != cache_revision
            || self.dirty.load(Ordering::Acquire)
            || self.force_recompute.load(Ordering::Acquire)
        {
            return None;
        }

        value
    }

    fn needs_refresh(&self) -> bool {
        self.dirty.load(Ordering::Acquire) || self.force_recompute.load(Ordering::Acquire)
    }

    fn needs_refresh_without_slot_dependencies(&self) -> bool {
        self.needs_refresh() && self.slot_dependency_count.load(Ordering::Acquire) == 0
    }

    fn store_value(&self, value: Option<Arc<ThreadSafeAny>>) {
        self.value.store(value);
        self.cache_revision.fetch_add(1, Ordering::AcqRel);
    }

    /// Mark dirty using atomics only — no per-node Mutex (#lzstateinvalidation).
    /// Called from `apply_locked` under the state write lock; the recompute
    /// Mutex is NOT touched, eliminating N per-node Mutex acquisitions during a
    /// fan-out N invalidation pass.
    fn mark_dirty(&self, force_recompute: bool) {
        self.invalidation_revision.fetch_add(1, Ordering::AcqRel);
        if force_recompute {
            self.force_recompute.store(true, Ordering::Release);
        }
        self.dirty.store(true, Ordering::Release);
        self.cache_revision.fetch_add(1, Ordering::AcqRel);
    }

    fn mark_fresh(&self, has_value: bool) {
        {
            let mut recompute = self.lock_recompute_state();
            recompute.has_value = has_value;
        }
        self.force_recompute.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
    }

    fn clear(&self) {
        self.store_value(None);
        self.invalidation_revision.fetch_add(1, Ordering::AcqRel);
        {
            let mut recompute = self.lock_recompute_state();
            recompute.has_value = false;
        }
        self.force_recompute.store(false, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
    }

    fn lock_recompute_state(&self) -> MutexGuard<'_, ThreadSafeSlotRecomputeState> {
        self.recompute.lock()
    }

    fn begin_recompute(&self) -> Option<ThreadSafeRecomputeStart> {
        let mut recompute = self.lock_recompute_state();
        if recompute.computing {
            return None;
        }
        recompute.computing = true;
        Some(ThreadSafeRecomputeStart {
            revision: self.invalidation_revision.load(Ordering::Acquire),
            was_unset: !recompute.has_value,
        })
    }

    fn recompute_in_flight(&self) -> bool {
        self.lock_recompute_state().computing
    }

    fn current_recompute_revision(&self) -> u64 {
        self.invalidation_revision.load(Ordering::Acquire)
    }

    fn finish_recompute(&self) {
        let notify_waiter = {
            let mut recompute = self.lock_recompute_state();
            recompute.computing = false;
            recompute.waiters > 0
        };
        if notify_waiter {
            self.recompute_condvar.notify_one();
        }
    }

    fn wait_for_recompute(&self) -> ThreadSafeRecomputeResult {
        let mut recompute = self.lock_recompute_state();
        let mut registered_waiter = false;
        if recompute.computing {
            recompute.waiters = recompute.waiters.saturating_add(1);
            registered_waiter = true;
        }
        while recompute.computing {
            self.recompute_condvar.wait(&mut recompute);
        }

        let notify_next_waiter = if registered_waiter {
            debug_assert!(recompute.waiters > 0);
            recompute.waiters -= 1;
            recompute.waiters > 0
        } else {
            false
        };
        if recompute.has_value
            && !self.dirty.load(Ordering::Acquire)
            && !self.force_recompute.load(Ordering::Acquire)
        {
            drop(recompute);
            if notify_next_waiter {
                self.recompute_condvar.notify_one();
            }
            ThreadSafeRecomputeResult::Fresh(false)
        } else {
            drop(recompute);
            if notify_next_waiter {
                self.recompute_condvar.notify_one();
            }
            ThreadSafeRecomputeResult::Stale
        }
    }

    fn dependencies_snapshot(&self) -> EdgeVec {
        self.dependencies.lock().iter().collect()
    }

    fn insert_dependency(&self, dependency_id: SlotId, dependency_is_slot: bool) {
        let inserted = edge_insert(&mut self.dependencies.lock(), dependency_id);
        if inserted && dependency_is_slot {
            self.slot_dependency_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn remove_dependency(&self, dependency_id: SlotId, dependency_is_slot: bool) {
        let removed = edge_remove(&mut self.dependencies.lock(), dependency_id);
        if removed && dependency_is_slot {
            self.slot_dependency_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn insert_dependent(&self, dependent_id: SlotId, kind: ThreadSafeDependentKind) {
        dependent_edge_insert(&mut self.dependents.lock(), dependent_id, kind);
    }

    fn remove_dependent(&self, dependent_id: SlotId) {
        dependent_edge_remove(&mut self.dependents.lock(), dependent_id);
    }
}

#[derive(Default)]
struct ThreadSafeSlotRecomputeState {
    has_value: bool,
    computing: bool,
    waiters: usize,
}

struct ThreadSafeRecomputeStart {
    revision: u64,
    was_unset: bool,
}

struct ThreadSafeSourceNode {
    dependents: HybridSet,
    fast_path: Arc<ThreadSafeCellFastPath>,
}

struct ThreadSafeCellFastPath {
    value: CellCachedReadStorage,
    type_id: TypeId,
    dependents: Mutex<HybridMap<ThreadSafeDependentKind>>,
}

impl ThreadSafeCellFastPath {
    fn new<T>(value: T, strategy: ReadStrategy, inline: Option<InlineSpec>) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            value: CellCachedReadStorage::new(strategy, inline, Arc::new(value)),
            type_id: TypeId::of::<T>(),
            dependents: Mutex::new(HybridMap::default()),
        }
    }

    fn get<T>(&self) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        assert!(self.type_id == TypeId::of::<T>(), "type mismatch in cell");
        self.value.get()
    }

    fn set_if_changed<T>(&self, new_value: T) -> bool
    where
        T: PartialEq + Send + Sync + 'static,
    {
        assert!(
            self.type_id == TypeId::of::<T>(),
            "type mismatch in cell set"
        );
        self.value.set_if_changed(new_value)
    }

    fn insert_dependent(&self, dependent_id: SlotId, kind: ThreadSafeDependentKind) {
        dependent_edge_insert(&mut self.dependents.lock(), dependent_id, kind);
    }

    fn remove_dependent(&self, dependent_id: SlotId) {
        dependent_edge_remove(&mut self.dependents.lock(), dependent_id);
    }
}

struct ThreadSafeEffectNode {
    run: Arc<ThreadSafeEffectFn>,
    dependencies: HybridSet,
    cleanup: Option<Box<ThreadSafeCleanup>>,
    force_run: bool,
}

enum ThreadSafeNode {
    Computed(ThreadSafeComputedNode),
    Source(ThreadSafeSourceNode),
    Effect(ThreadSafeEffectNode),
}

enum ThreadSafeRecomputeResult {
    Fresh(bool),
    Stale,
}

enum ThreadSafeSlotRead<T> {
    Fresh(T),
    Refresh(EdgeVec),
}

#[derive(Clone, Copy)]
struct ThreadSafeInvalidationRoot {
    id: SlotId,
    force_recompute: bool,
}

#[derive(Default)]
struct ThreadSafeInvalidationPlan {
    slot_marks: HybridMap<bool>,
    slot_clears: HybridSet,
    effect_schedules: HybridMap<bool>,
}

impl ThreadSafeInvalidationPlan {
    fn from_roots_locked<I>(state: &ThreadSafeState, roots: I) -> Self
    where
        I: IntoIterator<Item = ThreadSafeInvalidationRoot>,
    {
        let mut queue = VecDeque::new();
        let mut requested_force: HybridMap<bool> = HybridMap::default();
        for root in roots {
            Self::enqueue_root(&mut queue, &mut requested_force, root);
        }

        let mut plan = Self::default();
        let mut simulated_slots: HybridMap<(bool, bool)> = HybridMap::default();

        while let Some(root) = queue.pop_front() {
            let force_recompute = match requested_force.get(root.id) {
                Some(f) if root.force_recompute == *f => *f,
                _ => continue,
            };

            let dependents = match state.get_node(root.id) {
                Some(ThreadSafeNode::Computed(slot)) => {
                    let (dirty, force_state) = match simulated_slots.get(root.id) {
                        Some(s) => *s,
                        None => (slot.dirty, slot.force_recompute),
                    };
                    let should_propagate = !dirty || (force_recompute && !force_state);
                    if let Some(sim) = simulated_slots.get_mut(root.id) {
                        *sim = (true, force_state || force_recompute);
                    } else {
                        simulated_slots.push(root.id, (true, force_state || force_recompute));
                    }

                    plan.add_slot_mark(root.id, force_recompute);

                    if should_propagate {
                        sorted_slot_ids(slot.dependents.iter())
                    } else {
                        Vec::new()
                    }
                }
                Some(ThreadSafeNode::Effect(_)) => {
                    plan.add_effect_schedule(root.id, force_recompute);
                    Vec::new()
                }
                Some(ThreadSafeNode::Source(_)) | None => Vec::new(),
            };

            for dependent_id in dependents {
                Self::enqueue_root(
                    &mut queue,
                    &mut requested_force,
                    ThreadSafeInvalidationRoot {
                        id: dependent_id,
                        force_recompute: false,
                    },
                );
            }
        }

        plan
    }

    fn from_clear_roots_locked<I>(state: &ThreadSafeState, roots: I) -> Self
    where
        I: IntoIterator<Item = SlotId>,
    {
        let mut plan = Self::default();
        let mut queue = roots.into_iter().collect::<VecDeque<_>>();
        let mut visited_slots: HybridSet = HybridSet::default();

        while let Some(id) = queue.pop_front() {
            match state.get_node(id) {
                Some(ThreadSafeNode::Computed(slot)) => {
                    if visited_slots.contains(id) {
                        continue;
                    }
                    visited_slots.insert(id);
                    if slot.value.is_none() && !slot.dirty {
                        continue;
                    }
                    plan.add_slot_clear(id);
                    for dependent_id in sorted_slot_ids(slot.dependents.iter()) {
                        queue.push_back(dependent_id);
                    }
                }
                Some(ThreadSafeNode::Effect(_)) => {
                    plan.add_effect_schedule(id, true);
                }
                Some(ThreadSafeNode::Source(_)) | None => {}
            }
        }

        plan
    }

    fn apply_locked(self, state: &mut ThreadSafeState) {
        let slot_clears = self.slot_clears.into_entries();
        let slot_marks = self.slot_marks.into_entries();
        let effect_schedules = self.effect_schedules.into_entries();
        let clear_set: SlotIdSet = slot_clears.iter().copied().collect();

        for id in &slot_clears {
            let Some(ThreadSafeNode::Computed(slot)) = state.get_node_mut(*id) else {
                continue;
            };
            slot.value = None;
            slot.dirty = false;
            slot.force_recompute = false;
            slot.revision = slot.revision.wrapping_add(1);
            slot.fast_path.clear();
        }

        #[cfg(feature = "instrumentation")]
        let mut dirty_epoch_advances = 0usize;
        for (id, force_recompute) in &slot_marks {
            if clear_set.contains(id) {
                continue;
            }
            let Some(ThreadSafeNode::Computed(slot)) = state.get_node_mut(*id) else {
                continue;
            };
            slot.revision = slot.revision.wrapping_add(1);
            slot.dirty = true;
            if *force_recompute {
                slot.force_recompute = true;
            }
            slot.fast_path.mark_dirty(slot.force_recompute);
            #[cfg(feature = "instrumentation")]
            {
                dirty_epoch_advances = dirty_epoch_advances.saturating_add(1);
            }
        }
        #[cfg(feature = "instrumentation")]
        if dirty_epoch_advances > 0 {
            state
                .instrumentation
                .record_dirty_epoch_advances(dirty_epoch_advances);
        }

        for (id, force) in &effect_schedules {
            ThreadSafeContext::schedule_effect_locked(state, *id, *force);
        }
    }

    /// Drop every effect this plan would have scheduled, keeping the slot
    /// marks and clears.
    ///
    /// Disposal needs the dependent cone dirtied but must not *run* anything:
    /// an effect scheduled here would re-enter a compute that reads the node
    /// currently being torn down. See
    /// [`ThreadSafeContext::invalidate_disposed_dependents_locked`].
    fn without_effect_schedules(mut self) -> Self {
        self.effect_schedules = HybridMap::default();
        self
    }

    fn add_slot_mark(&mut self, id: SlotId, force_recompute: bool) {
        if let Some(force) = self.slot_marks.get_mut(id) {
            if force_recompute {
                *force = true;
            }
        } else {
            self.slot_marks.push(id, force_recompute);
        }
    }

    fn add_slot_clear(&mut self, id: SlotId) {
        self.slot_clears.insert(id);
    }

    fn add_effect_schedule(&mut self, id: SlotId, force: bool) {
        if let Some(existing) = self.effect_schedules.get_mut(id) {
            if force {
                *existing = true;
            }
        } else {
            self.effect_schedules.push(id, force);
        }
    }

    fn enqueue_root(
        queue: &mut VecDeque<ThreadSafeInvalidationRoot>,
        force_map: &mut HybridMap<bool>,
        root: ThreadSafeInvalidationRoot,
    ) {
        if let Some(force) = force_map.get_mut(root.id) {
            if root.force_recompute {
                *force = true;
            }
        } else {
            force_map.push(root.id, root.force_recompute);
            queue.push_back(root);
        }
    }
}

fn sorted_slot_ids<I>(ids: I) -> Vec<SlotId>
where
    I: IntoIterator<Item = SlotId>,
{
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_by_key(|id| id.0);
    ids
}

#[derive(Default)]
struct ThreadSafeState {
    nodes: Vec<Option<ThreadSafeNode>>,
    next_id: u64,
    free_ids: Vec<u64>,
    pending_effects: VecDeque<SlotId>,
    scheduled_effects: Vec<bool>,
    flushing_effects: bool,
    batch_depth: usize,
    batched_cells: EdgeVec,
    batched_cell_clears: EdgeVec,
    batched_slots: EdgeVec,
    dependent_scratch: Vec<SlotId>,
    /// Effect-drain iteration budget (`#lzfeedbackdrain`). Zero means "use
    /// `DEFAULT_DRAIN_BUDGET`", so `#[derive(Default)]` need not name the
    /// constant. See [`ThreadSafeContext::flush_effects`].
    drain_budget: usize,
    /// The most recent exhausted drain, or `None` if every drain so far ended
    /// with an empty worklist. The observable `feedback_drain_bound_...` asserts.
    last_drain_exhaustion: Option<DrainExhaustion>,
    /// Per-effect run counts for the current drain, indexed by node index —
    /// attribution for the exhaustion report, mirroring `Context::drain_runs`.
    drain_runs: Vec<u32>,
    #[cfg(feature = "instrumentation")]
    instrumentation: crate::instrumentation::InstrumentationCounters,
}

/// Effect-drain iteration budget for `ThreadSafeContext` (`#lzfeedbackdrain`).
/// Mirrors `Context`'s `DEFAULT_DRAIN_BUDGET`: a scheduler-closed feedback loop
/// runs flat at re-entry depth 1, so the only structural exit is this bound.
const DEFAULT_DRAIN_BUDGET: usize = 100_000;

fn node_index(id: SlotId) -> Option<usize> {
    usize::try_from(id.0).ok()
}

impl ThreadSafeState {
    fn get_node(&self, id: SlotId) -> Option<&ThreadSafeNode> {
        self.nodes.get(node_index(id)?).and_then(|opt| opt.as_ref())
    }

    fn get_node_mut(&mut self, id: SlotId) -> Option<&mut ThreadSafeNode> {
        self.nodes
            .get_mut(node_index(id)?)
            .and_then(|opt| opt.as_mut())
    }

    fn insert_node(&mut self, id: SlotId, node: ThreadSafeNode) {
        let idx = node_index(id).expect("SlotId does not fit usize");
        if idx >= self.nodes.len() {
            self.nodes.resize_with(idx + 1, || None);
        }
        self.nodes[idx] = Some(node);
    }

    fn remove_node(&mut self, id: SlotId) -> Option<ThreadSafeNode> {
        let idx = node_index(id).expect("SlotId does not fit usize");
        self.nodes.get_mut(idx).and_then(|slot| slot.take())
    }

    fn deschedule_effect(&mut self, id: SlotId) {
        let idx = node_index(id).expect("SlotId does not fit usize");
        if idx < self.scheduled_effects.len() {
            self.scheduled_effects[idx] = false;
        }
    }

    /// Pop the next effect that is still actually scheduled, discarding
    /// tombstones left behind by `dispose_effect`. Mirrors
    /// `Context::pop_scheduled_effect`; see that doc comment for why an entry
    /// whose flag is clear is safe to drop even across id recycling.
    fn pop_scheduled_effect(&mut self) -> Option<SlotId> {
        while let Some(id) = self.pending_effects.pop_front() {
            let idx = node_index(id).expect("SlotId does not fit usize");
            if idx < self.scheduled_effects.len() && self.scheduled_effects[idx] {
                self.scheduled_effects[idx] = false;
                return Some(id);
            }
        }
        None
    }

    #[cfg(test)]
    fn is_effect_scheduled(&self, id: SlotId) -> bool {
        let idx = node_index(id).expect("SlotId does not fit usize");
        idx < self.scheduled_effects.len() && self.scheduled_effects[idx]
    }

    fn fill_dependent_scratch(&mut self, id: SlotId) {
        self.dependent_scratch.clear();
        let idx = node_index(id).expect("SlotId does not fit usize");
        // A promoted edge list is not contiguous, so materialise rather than
        // borrow a slice.
        let deps: Vec<SlotId> = match self.nodes.get(idx).and_then(|opt| opt.as_ref()) {
            Some(ThreadSafeNode::Computed(slot)) => slot.dependents.to_vec(),
            Some(ThreadSafeNode::Source(cell)) => cell.dependents.to_vec(),
            _ => return,
        };
        self.dependent_scratch.extend_from_slice(&deps);
    }
}

struct ThreadSafeInner {
    state: StateRwLock<ThreadSafeState>,
    slot_fast_paths: RwLock<Vec<Option<Arc<ThreadSafeSlotFastPath>>>>,
    cell_fast_paths: RwLock<Vec<Option<Arc<ThreadSafeCellFastPath>>>>,
    read_strategy: ReadStrategy,
    batch_depth: AtomicUsize,
    active_callbacks: AtomicUsize,
    #[cfg(feature = "instrumentation")]
    lock_instrumentation: crate::instrumentation::ThreadSafeLockInstrumentation,
    #[cfg(feature = "instrumentation")]
    invalidation_instrumentation: crate::instrumentation::ThreadSafeInvalidationInstrumentation,
}

impl Default for ThreadSafeInner {
    fn default() -> Self {
        Self {
            state: StateRwLock::new(ThreadSafeState::default()),
            slot_fast_paths: RwLock::new(Vec::new()),
            cell_fast_paths: RwLock::new(Vec::new()),
            read_strategy: ReadStrategy::default(),
            batch_depth: AtomicUsize::new(0),
            active_callbacks: AtomicUsize::new(0),
            #[cfg(feature = "instrumentation")]
            lock_instrumentation: crate::instrumentation::ThreadSafeLockInstrumentation::default(),
            #[cfg(feature = "instrumentation")]
            invalidation_instrumentation:
                crate::instrumentation::ThreadSafeInvalidationInstrumentation::default(),
        }
    }
}

#[cfg(feature = "instrumentation")]
struct ProfiledReadGuard<'a> {
    guard: Option<StateReadGuard<'a>>,
    lock_instrumentation: &'a crate::instrumentation::ThreadSafeLockInstrumentation,
    site: ThreadSafeLockSite,
    acquired_at: Instant,
}

#[cfg(feature = "instrumentation")]
struct ProfiledWriteGuard<'a> {
    guard: Option<StateWriteGuard<'a>>,
    lock_instrumentation: &'a crate::instrumentation::ThreadSafeLockInstrumentation,
    site: ThreadSafeLockSite,
    acquired_at: Instant,
}

#[cfg(feature = "instrumentation")]
impl Deref for ProfiledReadGuard<'_> {
    type Target = ThreadSafeState;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("profiled mutex read guard missing during deref")
    }
}

#[cfg(feature = "instrumentation")]
impl Drop for ProfiledReadGuard<'_> {
    fn drop(&mut self) {
        if self.guard.is_some() {
            self.lock_instrumentation
                .record_lock_hold(self.site, self.acquired_at.elapsed());
        }
    }
}

#[cfg(feature = "instrumentation")]
impl Deref for ProfiledWriteGuard<'_> {
    type Target = ThreadSafeState;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("profiled mutex write guard missing during deref")
    }
}

#[cfg(feature = "instrumentation")]
impl DerefMut for ProfiledWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("profiled mutex write guard missing during mutable deref")
    }
}

#[cfg(feature = "instrumentation")]
impl Drop for ProfiledWriteGuard<'_> {
    fn drop(&mut self) {
        if self.guard.is_some() {
            self.lock_instrumentation
                .record_lock_hold(self.site, self.acquired_at.elapsed());
        }
    }
}

/// Return value accepted by [`ThreadSafeContext::effect`].
///
/// Returning `()` registers no cleanup. Returning a `Send` cleanup closure
/// registers that closure for the current effect run.
pub trait ThreadSafeEffectCallbackResult {
    fn into_thread_safe_cleanup(self) -> Option<Box<ThreadSafeCleanup>>;
}

impl ThreadSafeEffectCallbackResult for () {
    fn into_thread_safe_cleanup(self) -> Option<Box<ThreadSafeCleanup>> {
        None
    }
}

impl<F> ThreadSafeEffectCallbackResult for F
where
    F: FnOnce() + Send + Sync + 'static,
{
    fn into_thread_safe_cleanup(self) -> Option<Box<ThreadSafeCleanup>> {
        Some(Box::new(self))
    }
}

/// A typed handle to an **eager** derived value within a [`ThreadSafeContext`].
///
/// This is the thread-safe counterpart to [`crate::Computed`]. Like the
/// single-threaded handle it is a memoized backing slot plus a small puller
/// effect that re-materializes the slot after every invalidation, so reading a
/// signal always returns a materialized, up-to-date value with no observable
/// intermediate "unset" state. See [`ThreadSafeContext::signal`].
pub struct ThreadSafeSignalHandle<T> {
    /// Memoized backing slot that holds the derived value.
    pub(crate) slot: Computed<T>,
    /// Puller effect that keeps `slot` eagerly materialized.
    pub(crate) effect: Effect,
}

impl<T> ThreadSafeSignalHandle<T> {
    pub(crate) fn new(slot: Computed<T>, effect: Effect) -> Self {
        Self { slot, effect }
    }

    /// Read this signal's current value through its owning context.
    ///
    /// Ergonomic alias for [`ThreadSafeContext::get_signal`]. The value is
    /// always materialized; there is no unset state to observe.
    pub fn get(&self, ctx: &ThreadSafeContext) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        ctx.get_signal(self)
    }

    /// Dispose this signal's eager puller.
    ///
    /// After disposal the signal stops eagerly recomputing on invalidation; the
    /// backing value remains readable and behaves like a lazy memo slot
    /// (recomputed on the next read).
    pub fn dispose(&self, ctx: &ThreadSafeContext) {
        ctx.dispose_signal(self);
    }

    /// Check whether this signal's eager puller is still active.
    pub fn is_active(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.is_signal_active(self)
    }
}

// Handles are Copy/Clone since they're just ids.
impl<T> Clone for ThreadSafeSignalHandle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ThreadSafeSignalHandle<T> {}

/// Lock-backed context for sharing lazy reactive state across OS threads.
///
/// This type mirrors the core [`crate::Context`] API while requiring
/// `Send + Sync + 'static` values and callbacks. The graph lock is released
/// before user compute/effect/cleanup callbacks run, so callbacks may re-enter
/// the same context without deadlocking.
/// A teardown scope over a [`ThreadSafeContext`]: nodes created through it are
/// disposed when it drops.
///
/// Holds an **owned** context handle rather than a borrow. `ThreadSafeContext`
/// is already a cheap cloneable `Arc` handle over shared state, so owning one
/// costs a single refcount bump and makes the scope `Send` and `'static`-able —
/// which is what a per-connection scope on a worker thread needs. `Context`
/// owns its state directly and so its scope must borrow; the two shapes differ
/// because the ownership models differ, not by oversight.
///
/// Records only ids and reads each node's kind from the graph at teardown.
pub struct ThreadSafeTeardownScope {
    ctx: ThreadSafeContext,
    owned: Mutex<Vec<SlotId>>,
}

impl ThreadSafeTeardownScope {
    fn track<H>(&self, handle: H, id: SlotId) -> H {
        self.owned.lock().push(id);
        handle
    }

    /// Create a guarded computed cell owned by this scope.
    pub fn computed<T, F>(&self, compute: F) -> Computed<T>
    where
        T: PartialEq + Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        let handle = self.ctx.computed(compute);
        self.track(handle, handle.id)
    }

    /// Create a source cell owned by this scope.
    pub fn source<T: PartialEq + Send + Sync + 'static>(&self, value: T) -> Source<T> {
        let handle = self.ctx.source(value);
        self.track(handle, handle.id)
    }

    /// Compatibility constructor for the pre-Cell-kernel API.
    #[deprecated(note = "use `ThreadSafeTeardownScope::source`")]
    pub fn cell<T: PartialEq + Send + Sync + 'static>(&self, value: T) -> Source<T> {
        self.source(value)
    }

    /// Register an effect owned by this scope.
    pub fn effect<F, R>(&self, run: F) -> Effect
    where
        F: Fn(&ThreadSafeContext) -> R + Send + Sync + 'static,
        R: ThreadSafeEffectCallbackResult + 'static,
    {
        let handle = self.ctx.effect(run);
        self.track(handle, handle.id)
    }

    /// The context this scope belongs to.
    pub fn context(&self) -> &ThreadSafeContext {
        &self.ctx
    }

    /// How many nodes this scope owns.
    pub fn len(&self) -> usize {
        self.owned.lock().len()
    }

    /// Whether this scope owns nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Disarm the scope: ending it afterwards disposes nothing, and its nodes
    /// revert to plain context ownership. The nodes themselves are untouched.
    pub fn disarm(self) {
        self.owned.lock().clear();
    }
}

impl Drop for ThreadSafeTeardownScope {
    fn drop(&mut self) {
        // Reverse creation order: dependents before what they read, so a scope
        // never transiently dangles inside itself.
        let owned = std::mem::take(&mut *self.owned.lock());
        for id in owned.into_iter().rev() {
            self.ctx.dispose_id(id);
        }
    }
}

#[derive(Clone, Default)]
pub struct ThreadSafeContext {
    inner: Arc<ThreadSafeInner>,
}

struct BatchGuard {
    ctx: ThreadSafeContext,
    context_id: ThreadSafeContextId,
}

impl Drop for BatchGuard {
    fn drop(&mut self) {
        let changes = pop_batch_frame(self.context_id);
        self.ctx.finish_batch(changes);
    }
}

struct RecomputeGuard {
    fast_path: Arc<ThreadSafeSlotFastPath>,
    active: bool,
}

impl Drop for RecomputeGuard {
    fn drop(&mut self) {
        if self.active {
            self.fast_path.finish_recompute();
        }
    }
}

struct CallbackActivityGuard {
    inner: Arc<ThreadSafeInner>,
}

impl Drop for CallbackActivityGuard {
    fn drop(&mut self) {
        self.inner.active_callbacks.fetch_sub(1, Ordering::AcqRel);
    }
}

struct FlushGuard {
    ctx: ThreadSafeContext,
    active: bool,
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.ctx.lock_state();
            state.flushing_effects = false;
        }
    }
}

impl ThreadSafeContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a context with an explicit cached-read [`ReadStrategy`] (#rdstrat1).
    ///
    /// Both read paths are compiled in; this selects the mode at runtime for the
    /// context's lifetime. `new()` / `default()` use `ReadStrategy::LowConcurrency`.
    pub fn with_read_strategy(strategy: ReadStrategy) -> Self {
        Self {
            inner: Arc::new(ThreadSafeInner {
                read_strategy: strategy,
                ..ThreadSafeInner::default()
            }),
        }
    }

    /// The cached-read strategy selected for this context.
    pub fn read_strategy(&self) -> ReadStrategy {
        self.inner.read_strategy
    }

    fn context_id(&self) -> ThreadSafeContextId {
        ThreadSafeContextId(Arc::as_ptr(&self.inner) as usize)
    }

    #[cfg(not(feature = "instrumentation"))]
    fn read_state(&self) -> StateReadGuard<'_> {
        read_state_inner(&self.inner.state)
    }

    #[cfg(not(feature = "instrumentation"))]
    fn lock_state(&self) -> StateWriteGuard<'_> {
        lock_state_inner(&self.inner.state)
    }

    #[cfg(feature = "instrumentation")]
    fn read_state(&self) -> ProfiledReadGuard<'_> {
        let site = current_thread_safe_lock_site();
        let wait_started = Instant::now();
        let guard = read_state_inner(&self.inner.state);
        self.inner
            .lock_instrumentation
            .record_lock_wait(site, wait_started.elapsed());
        ProfiledReadGuard {
            guard: Some(guard),
            lock_instrumentation: &self.inner.lock_instrumentation,
            site,
            acquired_at: Instant::now(),
        }
    }

    #[cfg(feature = "instrumentation")]
    fn lock_state(&self) -> ProfiledWriteGuard<'_> {
        let site = current_thread_safe_lock_site();
        let wait_started = Instant::now();
        let guard = lock_state_inner(&self.inner.state);
        self.inner
            .lock_instrumentation
            .record_lock_wait(site, wait_started.elapsed());
        ProfiledWriteGuard {
            guard: Some(guard),
            lock_instrumentation: &self.inner.lock_instrumentation,
            site,
            acquired_at: Instant::now(),
        }
    }

    #[cfg(feature = "instrumentation")]
    fn record_coordination_lock(&self, site: ThreadSafeLockSite) {
        self.inner
            .lock_instrumentation
            .record_lock_wait(site, std::time::Duration::ZERO);
        self.inner
            .lock_instrumentation
            .record_lock_hold(site, std::time::Duration::ZERO);
    }

    fn alloc_id(&self) -> SlotId {
        let mut state = self.lock_state();
        let slot_id = match state.free_ids.pop() {
            Some(id) => SlotId(id),
            None => {
                let id = SlotId(state.next_id);
                state.next_id += 1;
                id
            }
        };
        #[cfg(feature = "instrumentation")]
        {
            state.instrumentation.record_node_allocation();
        }
        slot_id
    }

    fn slot_fast_path(&self, id: SlotId) -> Option<Arc<ThreadSafeSlotFastPath>> {
        self.inner
            .slot_fast_paths
            .read()
            .get(node_index(id)?)
            .and_then(|opt| opt.as_ref().cloned())
    }

    fn cell_fast_path(&self, id: SlotId) -> Option<Arc<ThreadSafeCellFastPath>> {
        self.inner
            .cell_fast_paths
            .read()
            .get(node_index(id)?)
            .and_then(|opt| opt.as_ref().cloned())
    }

    fn try_read_fresh_slot_fast_path<T>(&self, id: SlotId) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let idx = node_index(id)?;
        // Hold the `slot_fast_paths` read guard across `read_fresh` instead of
        // cloning the `Arc` (refcount inc/dec) per cached read. `read_fresh`
        // takes `&self` and only touches the fast path's own value storage /
        // atomics, never re-entering `slot_fast_paths`; the only writer of that
        // registry is slot creation (never nested with the value lock), so the
        // overlapping read locks cannot deadlock.
        let guard = self.inner.slot_fast_paths.read();
        guard
            .get(idx)
            .and_then(|opt| opt.as_ref())
            .and_then(|fast_path| fast_path.read_fresh())
    }

    /// `try_read_fresh_slot_fast_path` for [`Self::get_arc`] (`#lzrsgetarc`).
    /// Same guard-held-across-read reasoning as its sibling.
    fn try_read_fresh_slot_arc_fast_path<T>(&self, id: SlotId) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let idx = node_index(id)?;
        let guard = self.inner.slot_fast_paths.read();
        guard
            .get(idx)
            .and_then(|opt| opt.as_ref())
            .and_then(|fast_path| fast_path.read_fresh_arc())
    }

    fn slot_recompute_in_flight(&self, id: SlotId) -> bool {
        self.slot_fast_path(id)
            .map(|fast_path| fast_path.recompute_in_flight())
            .unwrap_or(false)
    }

    fn slot_needs_refresh_without_slot_dependencies(&self, id: SlotId) -> bool {
        self.slot_fast_path(id)
            .map(|fast_path| fast_path.needs_refresh_without_slot_dependencies())
            .unwrap_or(false)
    }

    fn callback_activity(&self) -> CallbackActivityGuard {
        self.inner.active_callbacks.fetch_add(1, Ordering::AcqRel);
        CallbackActivityGuard {
            inner: Arc::clone(&self.inner),
        }
    }

    fn dependent_kind_locked(
        state: &ThreadSafeState,
        dependent_id: SlotId,
    ) -> Option<ThreadSafeDependentKind> {
        match state.get_node(dependent_id) {
            Some(ThreadSafeNode::Computed(_)) => Some(ThreadSafeDependentKind::Slot),
            Some(ThreadSafeNode::Effect(_)) => Some(ThreadSafeDependentKind::Effect),
            Some(ThreadSafeNode::Source(_)) | None => None,
        }
    }

    fn insert_dependent_sidecar_locked(
        state: &ThreadSafeState,
        dependency_id: SlotId,
        dependent_id: SlotId,
        dependent_kind: ThreadSafeDependentKind,
    ) {
        match state.get_node(dependency_id) {
            Some(ThreadSafeNode::Computed(slot)) => {
                slot.fast_path
                    .insert_dependent(dependent_id, dependent_kind);
            }
            Some(ThreadSafeNode::Source(cell)) => {
                cell.fast_path
                    .insert_dependent(dependent_id, dependent_kind);
            }
            Some(ThreadSafeNode::Effect(_)) | None => {}
        }
    }

    fn remove_dependent_sidecar_locked(
        state: &ThreadSafeState,
        dependency_id: SlotId,
        dependent_id: SlotId,
    ) {
        match state.get_node(dependency_id) {
            Some(ThreadSafeNode::Computed(slot)) => {
                slot.fast_path.remove_dependent(dependent_id);
            }
            Some(ThreadSafeNode::Source(cell)) => {
                cell.fast_path.remove_dependent(dependent_id);
            }
            Some(ThreadSafeNode::Effect(_)) | None => {}
        }
    }

    fn register_dependency(&self, dependency_id: SlotId, dependent_id: SlotId) {
        if dependency_id == dependent_id {
            return;
        }

        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::DependencyEdge);
        #[cfg(feature = "instrumentation")]
        let mut edge_added = false;
        let mut state = self.lock_state();
        let dependency_is_slot = matches!(
            state.get_node(dependency_id),
            Some(ThreadSafeNode::Computed(_))
        );
        let dependent_kind = Self::dependent_kind_locked(&state, dependent_id);
        if let Some(node) = state.get_node_mut(dependency_id) {
            match node {
                ThreadSafeNode::Computed(slot) => {
                    edge_insert(&mut slot.dependents, dependent_id);
                }
                ThreadSafeNode::Source(cell) => {
                    edge_insert(&mut cell.dependents, dependent_id);
                }
                ThreadSafeNode::Effect(_) => {}
            }
        }

        if let Some(node) = state.get_node_mut(dependent_id) {
            match node {
                ThreadSafeNode::Computed(parent) => {
                    let inserted = edge_insert(&mut parent.dependencies, dependency_id);
                    if inserted {
                        parent
                            .fast_path
                            .insert_dependency(dependency_id, dependency_is_slot);
                    }
                    #[cfg(feature = "instrumentation")]
                    {
                        edge_added = inserted;
                    }
                }
                ThreadSafeNode::Effect(parent) => {
                    #[cfg(feature = "instrumentation")]
                    {
                        edge_added = edge_insert(&mut parent.dependencies, dependency_id);
                    }
                    #[cfg(not(feature = "instrumentation"))]
                    {
                        edge_insert(&mut parent.dependencies, dependency_id);
                    }
                }
                ThreadSafeNode::Source(_) => {}
            }
        }
        if let Some(dependent_kind) = dependent_kind {
            Self::insert_dependent_sidecar_locked(
                &state,
                dependency_id,
                dependent_id,
                dependent_kind,
            );
        }
        #[cfg(feature = "instrumentation")]
        if edge_added {
            state.instrumentation.record_dependency_edge_added();
        }
    }

    fn remove_dependent_edge(&self, dependency_id: SlotId, dependent_id: SlotId) {
        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::DependencyEdge);
        let mut state = self.lock_state();
        Self::remove_dependent_edge_locked(&mut state, dependency_id, dependent_id);
    }

    fn remove_stale_dependencies_locked(
        state: &mut ThreadSafeState,
        dependent_id: SlotId,
        old_dependencies: &EdgeVec,
        new_dependencies: &SlotIdSet,
    ) {
        for dependency_id in old_dependencies.iter() {
            if !new_dependencies.contains(dependency_id) {
                Self::remove_parent_dependency_locked(state, dependent_id, *dependency_id);
                Self::remove_dependent_edge_locked(state, *dependency_id, dependent_id);
            }
        }
    }

    fn remove_parent_dependency_locked(
        state: &mut ThreadSafeState,
        dependent_id: SlotId,
        dependency_id: SlotId,
    ) -> bool {
        let dependency_is_slot = matches!(
            state.get_node(dependency_id),
            Some(ThreadSafeNode::Computed(_))
        );
        match state.get_node_mut(dependent_id) {
            Some(ThreadSafeNode::Computed(slot)) => {
                let removed = edge_remove(&mut slot.dependencies, dependency_id);
                if removed {
                    slot.fast_path
                        .remove_dependency(dependency_id, dependency_is_slot);
                }
                removed
            }
            Some(ThreadSafeNode::Effect(effect)) => {
                edge_remove(&mut effect.dependencies, dependency_id)
            }
            Some(ThreadSafeNode::Source(_)) | None => false,
        }
    }

    fn remove_dependent_edge_locked(
        state: &mut ThreadSafeState,
        dependency_id: SlotId,
        dependent_id: SlotId,
    ) {
        let _edge_removed = match state.get_node_mut(dependency_id) {
            Some(ThreadSafeNode::Computed(slot)) => edge_remove(&mut slot.dependents, dependent_id),
            Some(ThreadSafeNode::Source(cell)) => edge_remove(&mut cell.dependents, dependent_id),
            Some(ThreadSafeNode::Effect(_)) | None => false,
        };
        if _edge_removed {
            Self::remove_dependent_sidecar_locked(state, dependency_id, dependent_id);
        }

        #[cfg(feature = "instrumentation")]
        if _edge_removed {
            state.instrumentation.record_dependency_edge_removed();
        }
    }

    /// Create a new lazily-computed thread-safe slot.
    pub fn slot<T, F>(&self, compute: F) -> Computed<T>
    where
        T: Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        self.slot_with_equals(compute, None)
    }

    /// Create a **guarded computed cell** (`#lzcellkernel`): an equal recompute
    /// suppresses downstream invalidation, so `T: PartialEq`. This is the
    /// primary derived constructor; the former `memo` constructor is retired
    /// because `computed` now *is* the guarded form.
    pub fn computed<T, F>(&self, compute: F) -> Computed<T>
    where
        T: PartialEq + Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        self.slot_with_equals(
            compute,
            Some(Arc::new(|old, new| {
                let old = old.downcast_ref::<T>().expect("type mismatch in slot");
                let new = new.downcast_ref::<T>().expect("type mismatch in slot");
                old == new
            })),
        )
    }

    /// Like [`slot`](Self::slot), but opts the cached-read sidecar into the
    /// inline small-`Copy` seqlock fast path (#rdstrat2): when `T` is `Copy` and
    /// fits the inline buffer (`size_of::<T>() <= 24`, `align <= 16`), the value
    /// is stored inline behind a wait-free seqlock — no heap `Arc`, no refcount
    /// traffic on read or publish, optimal under both [`ReadStrategy`] modes.
    /// `T` that exceeds the bound transparently falls back to the
    /// strategy-selected `RwLock`/`arc-swap` path.
    ///
    /// This is a separate constructor (rather than automatic) because stable
    /// Rust cannot detect `T: Copy` inside the unbounded generic [`slot`]; see
    /// [`inline_spec_for`].
    pub fn slot_copy<T, F>(&self, compute: F) -> Computed<T>
    where
        T: Copy + Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        self.slot_with_equals_inline(compute, None, inline_spec_for::<T>())
    }

    /// Like [`computed`](Self::computed) (guarded, `#lzcellkernel`), but opts
    /// into the inline small-`Copy` seqlock fast path (#rdstrat2). See
    /// [`slot_copy`](Self::slot_copy).
    pub fn computed_copy<T, F>(&self, compute: F) -> Computed<T>
    where
        T: Copy + PartialEq + Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        self.slot_with_equals_inline(
            compute,
            Some(Arc::new(|old, new| {
                let old = old.downcast_ref::<T>().expect("type mismatch in slot");
                let new = new.downcast_ref::<T>().expect("type mismatch in slot");
                old == new
            })),
            inline_spec_for::<T>(),
        )
    }

    fn slot_with_equals<T, F>(
        &self,
        compute: F,
        equals: Option<Arc<ThreadSafeEqualsFn>>,
    ) -> Computed<T>
    where
        T: Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        self.slot_with_equals_inline(compute, equals, None)
    }

    fn slot_with_equals_inline<T, F>(
        &self,
        compute: F,
        equals: Option<Arc<ThreadSafeEqualsFn>>,
        inline: Option<InlineSpec>,
    ) -> Computed<T>
    where
        T: Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        let id = self.alloc_id();
        let compute: Arc<ThreadSafeComputeFn> =
            Arc::new(move |ctx: &ThreadSafeContext| Box::new(compute(ctx)));
        let fast_path = Arc::new(ThreadSafeSlotFastPath::new(
            Arc::clone(&compute),
            EdgeVec::new(),
            TypeId::of::<T>(),
            self.inner.read_strategy,
            inline,
        ));
        let node = ThreadSafeComputedNode {
            value: None,
            equals,
            dependencies: HybridSet::default(),
            dependents: HybridSet::default(),
            fast_path: Arc::clone(&fast_path),
            dirty: false,
            force_recompute: false,
            revision: 0,
        };
        let mut slot_fast_paths = self.inner.slot_fast_paths.write();
        let idx = node_index(id).expect("SlotId does not fit usize");
        if idx >= slot_fast_paths.len() {
            slot_fast_paths.resize_with(idx + 1, || None);
        }
        slot_fast_paths[idx] = Some(fast_path);
        self.lock_state()
            .insert_node(id, ThreadSafeNode::Computed(node));
        Computed::from_id(id)
    }

    /// Read the current value of a cell — a [`Computed`] or a [`Source`]
    /// (`#lzcellkernel`). The unified read that supersedes the deprecated
    /// `get_cell`.
    pub fn get<H: Read<Self> + ?Sized>(&self, handle: &H) -> <H as Read<Self>>::Output {
        handle.read(self)
    }

    /// Write a new value to a [`Source`] cell (`#lzcellkernel`). The unified
    /// write that supersedes the deprecated `set_cell`; only source handles
    /// implement [`Write`].
    pub fn set<H: Write<Self> + ?Sized>(&self, handle: &H, value: <H as Write<Self>>::Value) {
        handle.write(self, value)
    }

    /// Read a slot without cloning its value (`#lzrsgetarc`) — the `Send + Sync`
    /// counterpart to [`Context::get_rc`].
    ///
    /// [`ThreadSafeContext::get`] deep-clones on every read, which is pure waste
    /// when the caller only wants to observe a large value (a `String`, a `Vec`,
    /// a map). Slot values are already stored behind an `Arc`, so handing that
    /// `Arc` out costs a refcount bump instead.
    ///
    /// Prefer [`ThreadSafeContext::get`] for small `Copy` values: those slots use
    /// the inline cached-read fast path, which this method deliberately bypasses
    /// (there is no shared box to share, so `get` is strictly cheaper).
    pub fn get_arc<T>(&self, handle: &Computed<T>) -> Arc<T>
    where
        T: Send + Sync + 'static,
    {
        self.get_slot_arc(handle.id)
    }

    fn get_slot_arc<T>(&self, id: SlotId) -> Arc<T>
    where
        T: Send + Sync + 'static,
    {
        if let Some(parent_id) = track_dependency(self.context_id(), id) {
            self.register_dependency(id, parent_id);
        }

        loop {
            if self.slot_recompute_in_flight(id) {
                let _ = self.wait_for_slot_recompute(id);
                continue;
            }

            if self.slot_needs_refresh_without_slot_dependencies(id) {
                let _ = self.recompute_slot_now(id);
                continue;
            }

            match self.read_slot_arc_or_dependencies::<T>(id) {
                ThreadSafeSlotRead::Fresh(value) => return value,
                ThreadSafeSlotRead::Refresh(dependencies) => {
                    self.refresh_slot_with_dependencies(id, dependencies);
                }
            }
        }
    }

    /// `read_slot_or_dependencies` for [`Self::get_arc`]: reads the authoritative
    /// `slot.value` `Arc` rather than the cached-read sidecar, which stores `T`
    /// by value and so cannot hand back a shared box. `recompute_slot_now`
    /// publishes both, so the `Arc` is fresh whenever the sidecar is.
    fn read_slot_arc_or_dependencies<T>(&self, id: SlotId) -> ThreadSafeSlotRead<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::GetRefresh);
        if let Some(value) = self.try_read_fresh_slot_arc_fast_path(id) {
            return ThreadSafeSlotRead::Fresh(value);
        }

        let state = self.read_state();
        match state.get_node(id) {
            Some(ThreadSafeNode::Computed(slot)) => {
                if !slot.fast_path.needs_refresh()
                    && let (false, false, Some(value)) =
                        (slot.dirty, slot.force_recompute, &slot.value)
                {
                    assert!(
                        slot.fast_path.type_id == TypeId::of::<T>(),
                        "type mismatch in slot"
                    );
                    ThreadSafeSlotRead::Fresh(
                        Arc::clone(value)
                            .downcast::<T>()
                            .expect("type mismatch in slot"),
                    )
                } else {
                    ThreadSafeSlotRead::Refresh(
                        slot.dependencies
                            .iter()
                            .filter(|dependency_id| {
                                matches!(
                                    state.get_node(*dependency_id),
                                    Some(ThreadSafeNode::Computed(_))
                                )
                            })
                            .collect(),
                    )
                }
            }
            _ => panic!("get_arc called on non-slot id"),
        }
    }

    fn get_slot<T>(&self, id: SlotId) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(parent_id) = track_dependency(self.context_id(), id) {
            self.register_dependency(id, parent_id);
        }

        loop {
            if self.slot_recompute_in_flight(id) {
                let _ = self.wait_for_slot_recompute(id);
                continue;
            }

            if self.slot_needs_refresh_without_slot_dependencies(id) {
                let _ = self.recompute_slot_now(id);
                continue;
            }

            match self.read_slot_or_dependencies(id) {
                ThreadSafeSlotRead::Fresh(value) => return value,
                ThreadSafeSlotRead::Refresh(dependencies) => {
                    self.refresh_slot_with_dependencies(id, dependencies);
                }
            }
        }
    }

    fn read_slot_or_dependencies<T>(&self, id: SlotId) -> ThreadSafeSlotRead<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::GetRefresh);
        if let Some(value) = self.try_read_fresh_slot_fast_path(id) {
            return ThreadSafeSlotRead::Fresh(value);
        }

        let state = self.read_state();
        match state.get_node(id) {
            Some(ThreadSafeNode::Computed(slot)) => {
                if !slot.fast_path.needs_refresh()
                    && let (false, false, Some(value)) =
                        (slot.dirty, slot.force_recompute, &slot.value)
                {
                    assert!(
                        slot.fast_path.type_id == TypeId::of::<T>(),
                        "type mismatch in slot"
                    );
                    ThreadSafeSlotRead::Fresh(
                        unsafe { &*(&**value as *const ThreadSafeAny as *const T) }.clone(),
                    )
                } else {
                    ThreadSafeSlotRead::Refresh(
                        slot.dependencies
                            .iter()
                            .filter(|dependency_id| {
                                matches!(
                                    state.get_node(*dependency_id),
                                    Some(ThreadSafeNode::Computed(_))
                                )
                            })
                            .collect(),
                    )
                }
            }
            _ => panic!("get_slot called on non-slot id"),
        }
    }

    fn refresh_slot(&self, id: SlotId) -> bool {
        if self.slot_recompute_in_flight(id) {
            return match self.wait_for_slot_recompute(id) {
                ThreadSafeRecomputeResult::Fresh(changed) => changed,
                ThreadSafeRecomputeResult::Stale => self.refresh_slot(id),
            };
        }

        if self.slot_needs_refresh_without_slot_dependencies(id) {
            return match self.recompute_slot_now(id) {
                ThreadSafeRecomputeResult::Fresh(changed) => changed,
                ThreadSafeRecomputeResult::Stale => self.refresh_slot(id),
            };
        }

        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::GetRefresh);
        let dependencies = {
            let state = self.read_state();
            match state.get_node(id) {
                Some(ThreadSafeNode::Computed(slot)) => slot
                    .dependencies
                    .iter()
                    .filter(|dependency_id| {
                        matches!(
                            state.get_node(*dependency_id),
                            Some(ThreadSafeNode::Computed(_))
                        )
                    })
                    .collect(),
                _ => return false,
            }
        };

        self.refresh_slot_with_dependencies(id, dependencies)
    }

    fn refresh_slot_with_dependencies(&self, id: SlotId, dependencies: EdgeVec) -> bool {
        let mut dependency_changed = false;
        for dependency_id in dependencies.iter().copied() {
            if self.refresh_slot(dependency_id) {
                dependency_changed = true;
            }
        }

        let needs_recompute = {
            #[cfg(feature = "instrumentation")]
            let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::GetRefresh);
            let mut state = self.lock_state();
            let slot = match state.get_node_mut(id) {
                Some(ThreadSafeNode::Computed(slot)) => slot,
                _ => return false,
            };

            if slot.value.is_none()
                || slot.force_recompute
                || slot.fast_path.needs_refresh()
                || dependency_changed
            {
                true
            } else {
                slot.dirty = false;
                slot.force_recompute = false;
                slot.fast_path.mark_fresh(true);
                false
            }
        };

        if !needs_recompute {
            return false;
        }

        loop {
            match self.recompute_slot_now(id) {
                ThreadSafeRecomputeResult::Fresh(changed) => return changed,
                ThreadSafeRecomputeResult::Stale => {}
            }
        }
    }

    fn recompute_slot_now(&self, id: SlotId) -> ThreadSafeRecomputeResult {
        if self.slot_recompute_in_flight(id) {
            return self.wait_for_slot_recompute(id);
        }

        let fast_path = self
            .slot_fast_path(id)
            .unwrap_or_else(|| panic!("get_slot called on non-slot id"));
        let Some(recompute_start) = fast_path.begin_recompute() else {
            return self.wait_for_slot_recompute(id);
        };
        let compute = fast_path.compute();
        let old_dependencies = fast_path.dependencies_snapshot();
        let mut recompute_guard = RecomputeGuard {
            fast_path: Arc::clone(&fast_path),
            active: true,
        };

        let _tracking = push_tracking_frame_with_known_dependencies(
            self.context_id(),
            id,
            old_dependencies.clone(),
        );
        let _callback_activity = self.callback_activity();
        let result = compute(self);
        let new_dependencies = _tracking.finish();
        let result = Arc::<ThreadSafeAny>::from(result);

        {
            #[cfg(feature = "instrumentation")]
            let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::Publish);
            let mut state = self.lock_state();
            #[cfg(feature = "instrumentation")]
            {
                state.instrumentation.record_slot_recompute();
            }
            {
                let slot = match state.get_node_mut(id) {
                    Some(ThreadSafeNode::Computed(slot)) => slot,
                    _ => {
                        recompute_guard.active = false;
                        fast_path.finish_recompute();
                        return ThreadSafeRecomputeResult::Fresh(false);
                    }
                };

                if slot.fast_path.current_recompute_revision() != recompute_start.revision {
                    slot.fast_path.finish_recompute();
                    recompute_guard.active = false;
                    return ThreadSafeRecomputeResult::Stale;
                }
            }

            Self::remove_stale_dependencies_locked(
                &mut state,
                id,
                &old_dependencies,
                &new_dependencies,
            );

            let (publish_fast_path, duplicate_speculative, notify_dependents, changed) = {
                let slot = match state.get_node_mut(id) {
                    Some(ThreadSafeNode::Computed(slot)) => slot,
                    _ => {
                        recompute_guard.active = false;
                        fast_path.finish_recompute();
                        return ThreadSafeRecomputeResult::Fresh(false);
                    }
                };
                let publish_fast_path = Arc::clone(&slot.fast_path);
                if recompute_start.was_unset
                    && slot.value.is_some()
                    && !slot.dirty
                    && !slot.force_recompute
                {
                    (publish_fast_path, true, false, false)
                } else {
                    let had_value = slot.value.is_some();
                    let unchanged = match (&slot.value, &slot.equals) {
                        (Some(old), Some(equals)) => equals(old.as_ref(), result.as_ref()),
                        _ => false,
                    };
                    slot.dirty = false;
                    slot.force_recompute = false;
                    if unchanged {
                        (publish_fast_path, false, false, false)
                    } else {
                        slot.value = Some(Arc::clone(&result));
                        publish_fast_path.store_value(Some(result));
                        (publish_fast_path, false, had_value, had_value)
                    }
                }
            };

            if duplicate_speculative {
                #[cfg(feature = "instrumentation")]
                state
                    .instrumentation
                    .record_duplicate_speculative_recompute();
                publish_fast_path.mark_fresh(true);
                publish_fast_path.finish_recompute();
                recompute_guard.active = false;
                return ThreadSafeRecomputeResult::Fresh(false);
            }

            if notify_dependents {
                Self::notify_slot_value_changed_locked(&mut state, id);
            }
            publish_fast_path.mark_fresh(true);
            publish_fast_path.finish_recompute();
            recompute_guard.active = false;
            ThreadSafeRecomputeResult::Fresh(changed)
        }
    }

    fn wait_for_slot_recompute(&self, id: SlotId) -> ThreadSafeRecomputeResult {
        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::InFlightWait);
        #[cfg(feature = "instrumentation")]
        self.record_coordination_lock(ThreadSafeLockSite::InFlightWait);
        self.slot_fast_path(id)
            .map(|fast_path| fast_path.wait_for_recompute())
            .unwrap_or(ThreadSafeRecomputeResult::Fresh(false))
    }

    /// Create a mutable thread-safe source.
    ///
    /// The cell's cached value is stored behind a read-scaling sidecar
    /// (#lzcellread): concurrent `get` reads take a *shared* `RwLock` read
    /// (or a wait-free seqlock path for `source_copy`), so readers no longer
    /// serialize through an exclusive lock as they did before v0.23.0.
    pub fn source<T>(&self, value: T) -> Source<T>
    where
        T: PartialEq + Send + Sync + 'static,
    {
        let id = self.alloc_id();
        let fast_path = Arc::new(ThreadSafeCellFastPath::new(
            value,
            self.inner.read_strategy,
            None,
        ));
        let node = ThreadSafeSourceNode {
            dependents: HybridSet::default(),
            fast_path: Arc::clone(&fast_path),
        };
        let mut cell_fast_paths = self.inner.cell_fast_paths.write();
        let idx = node_index(id).expect("SlotId does not fit usize");
        if idx >= cell_fast_paths.len() {
            cell_fast_paths.resize_with(idx + 1, || None);
        }
        cell_fast_paths[idx] = Some(fast_path);
        self.lock_state()
            .insert_node(id, ThreadSafeNode::Source(node));
        Source::from_id(id)
    }

    /// Compatibility constructor for the pre-Cell-kernel API.
    #[deprecated(note = "use `ThreadSafeContext::source`")]
    pub fn cell<T>(&self, value: T) -> Source<T>
    where
        T: PartialEq + Send + Sync + 'static,
    {
        self.source(value)
    }

    /// Like [`source`](Self::source), but opts the cached-value sidecar into the
    /// inline small-`Copy` seqlock fast path (#lzcellread / #rdstrat2): when `T`
    /// is `Copy` and fits the inline buffer (`size_of::<T>() <= 24`,
    /// `align <= 16`), the value is stored inline behind a wait-free seqlock —
    /// no heap `Arc`, no refcount traffic on read or publish, optimal under both
    /// [`ReadStrategy`] modes. `T` that exceeds the bound transparently falls
    /// back to the `RwLock` path. Writers are serialized by a lightweight
    /// `Mutex<()>`; readers never contend on it.
    ///
    /// This is a separate constructor (rather than automatic) because stable
    /// Rust cannot detect `T: Copy` inside the unbounded generic [`source`]; see
    /// [`inline_spec_for`].
    pub fn source_copy<T>(&self, value: T) -> Source<T>
    where
        T: Copy + PartialEq + Send + Sync + 'static,
    {
        let id = self.alloc_id();
        let fast_path = Arc::new(ThreadSafeCellFastPath::new(
            value,
            self.inner.read_strategy,
            inline_spec_for::<T>(),
        ));
        let node = ThreadSafeSourceNode {
            dependents: HybridSet::default(),
            fast_path: Arc::clone(&fast_path),
        };
        let mut cell_fast_paths = self.inner.cell_fast_paths.write();
        let idx = node_index(id).expect("SlotId does not fit usize");
        if idx >= cell_fast_paths.len() {
            cell_fast_paths.resize_with(idx + 1, || None);
        }
        cell_fast_paths[idx] = Some(fast_path);
        self.lock_state()
            .insert_node(id, ThreadSafeNode::Source(node));
        Source::from_id(id)
    }

    /// Compatibility constructor for the pre-Cell-kernel API.
    #[deprecated(note = "use `ThreadSafeContext::source_copy`")]
    pub fn cell_copy<T>(&self, value: T) -> Source<T>
    where
        T: Copy + PartialEq + Send + Sync + 'static,
    {
        self.source_copy(value)
    }

    /// Get the value of a thread-safe cell.
    #[deprecated(note = "use `ThreadSafeContext::get` — the unified cell read (#lzcellkernel)")]
    pub fn get_cell<T>(&self, handle: &Source<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        self.get(handle)
    }

    /// Read a thread-safe source cell (impl of the unified [`ThreadSafeContext::get`]).
    fn read_source<T>(&self, handle: &Source<T>) -> T
    where
        T: Clone + Send + Sync + 'static,
    {
        if let Some(parent_id) = track_dependency(self.context_id(), handle.id) {
            self.register_dependency(handle.id, parent_id);
        }

        self.cell_fast_path(handle.id)
            .map(|fast_path| fast_path.get())
            .unwrap_or_else(|| panic!("get called on non-cell id"))
    }

    /// Set a cell value. Changed values invalidate dependents.
    ///
    /// All invalidation goes through the state-locked path (v0.24.0+,
    /// #lzstateinvalidation): the former `try_invalidate_cell_dependents_fast`
    /// sidecar path acquired 3 per-node Mutexes + 1 RwLock per BFS node. The
    /// state-locked path reads node fields directly under one lock — the same
    /// model lazily-cpp uses (one recursive_mutex, raw-pointer inner loop).
    #[deprecated(note = "use `ThreadSafeContext::set` — the unified cell write (#lzcellkernel)")]
    pub fn set_cell<T>(&self, handle: &Source<T>, new_value: T)
    where
        T: PartialEq + Send + Sync + 'static,
    {
        self.set(handle, new_value)
    }

    /// Write a thread-safe source cell (impl of the unified [`ThreadSafeContext::set`]).
    fn write_source<T>(&self, handle: &Source<T>, new_value: T)
    where
        T: PartialEq + Send + Sync + 'static,
    {
        let fast_path = self
            .cell_fast_path(handle.id)
            .unwrap_or_else(|| panic!("set on non-cell id"));
        if !fast_path.set_if_changed(new_value) {
            return;
        }

        if self.queue_batched_cell(handle.id) {
            return;
        }

        let should_flush = self.invalidate_changed_cell_locked(handle.id);

        if should_flush {
            self.flush_effects();
        }
    }

    /// Fold `op` into a cell's value under policy `M` (the merge write), the
    /// thread-safe port of [`Context::apply_merge`] (design §9.1).
    ///
    /// Reads the current value **untracked** (through the cell fast path, not
    /// [`get`](Self::get), so no dependency edge is registered),
    /// computes `M::merge(old, op)` synchronously, then routes through
    /// [`set`](Self::set) so the `PartialEq` store-guard, batching, and
    /// store-without-cascade all apply unchanged. The fold runs on the caller's
    /// stack; per §9.2.1 a `MergePolicy::merge` MUST be cheap and non-blocking
    /// because — unlike the single-threaded `Context` — here it may run while a
    /// writer holds the state lock.
    pub fn apply_merge<T, M>(&self, handle: &Source<T>, op: T)
    where
        T: PartialEq + Clone + Send + Sync + 'static,
        M: MergePolicy<T>,
    {
        let old: T = self
            .cell_fast_path(handle.id)
            .map(|fast_path| fast_path.get::<T>())
            .unwrap_or_else(|| panic!("apply_merge on non-cell id"));
        let merged = M::merge(&old, op);
        self.set(handle, merged);
    }

    fn invalidate_changed_cell_locked(&self, id: SlotId) -> bool {
        #[cfg(feature = "instrumentation")]
        let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::SetCellInvalidation);
        let mut state = self.lock_state();
        match state.get_node(id) {
            Some(ThreadSafeNode::Source(_)) => {}
            _ => panic!("set_cell on non-cell id"),
        }
        let batching = state.batch_depth > 0;
        if batching {
            state.batched_cells.push(id);
        } else {
            Self::invalidate_cell_dependents_locked(&mut state, id);
        }
        !batching
    }

    /// Run several updates as one invalidation pass.
    pub fn batch<F, R>(&self, run: F) -> R
    where
        F: FnOnce(&ThreadSafeContext) -> R,
    {
        let context_id = self.context_id();
        {
            let mut state = self.lock_state();
            state.batch_depth += 1;
            self.inner
                .batch_depth
                .store(state.batch_depth, Ordering::Release);
        }
        push_batch_frame(context_id);
        let _guard = BatchGuard {
            ctx: self.clone(),
            context_id,
        };
        run(self)
    }

    fn finish_batch(&self, changes: ThreadSafeBatchChanges) {
        let should_flush = {
            let mut state = self.lock_state();
            assert!(state.batch_depth > 0, "finish_batch called without batch");
            state.batched_cells.extend(changes.cells);
            state.batched_cell_clears.extend(changes.cell_clears);
            state.batched_slots.extend(changes.slots);
            state.batch_depth -= 1;
            self.inner
                .batch_depth
                .store(state.batch_depth, Ordering::Release);
            if state.batch_depth == 0 {
                state.batched_cells.sort_unstable();
                state.batched_cells.dedup();
                state.batched_cell_clears.sort_unstable();
                state.batched_cell_clears.dedup();
                state.batched_slots.sort_unstable();
                state.batched_slots.dedup();
            }
            state.batch_depth == 0
        };

        if should_flush {
            self.flush_batched_invalidations();
        }
    }

    fn is_batching(&self) -> bool {
        self.read_state().batch_depth > 0
    }

    fn queue_batched_cell(&self, id: SlotId) -> bool {
        queue_batch_change(self.context_id(), |changes| {
            changes.cells.push(id);
        })
    }

    fn queue_batched_cell_clear(&self, id: SlotId) -> bool {
        queue_batch_change(self.context_id(), |changes| {
            changes.cell_clears.push(id);
        })
    }

    fn queue_batched_slot_clear(&self, id: SlotId) -> bool {
        queue_batch_change(self.context_id(), |changes| {
            changes.slots.push(id);
        })
    }

    fn flush_batched_invalidations(&self) {
        {
            #[cfg(feature = "instrumentation")]
            let _lock_site = push_thread_safe_lock_site(ThreadSafeLockSite::SetCellInvalidation);
            let mut state = self.lock_state();
            let cells = std::mem::take(&mut state.batched_cells);
            let cell_clears = std::mem::take(&mut state.batched_cell_clears);
            let slots = std::mem::take(&mut state.batched_slots);

            let mut invalidation_roots = RootVec::new();
            for cell_id in cells.iter() {
                state.fill_dependent_scratch(*cell_id);
                invalidation_roots.extend(state.dependent_scratch.iter().map(|&id| {
                    ThreadSafeInvalidationRoot {
                        id,
                        force_recompute: true,
                    }
                }));
            }
            Self::invalidate_frontier_locked(&mut state, invalidation_roots);

            let mut clear_roots = EdgeVec::new();
            for cell_id in cell_clears.iter() {
                state.fill_dependent_scratch(*cell_id);
                let deps: EdgeVec = state.dependent_scratch.as_slice().into();
                clear_roots.extend(deps.iter().copied());
            }
            clear_roots.extend(slots.iter().copied());
            Self::clear_frontier_locked(&mut state, clear_roots);
        }
        self.flush_effects();
    }

    /// Create an effect, run it immediately, and rerun it after tracked
    /// dependencies invalidate.
    pub fn effect<F, R>(&self, run: F) -> Effect
    where
        F: Fn(&ThreadSafeContext) -> R + Send + Sync + 'static,
        R: ThreadSafeEffectCallbackResult + 'static,
    {
        let id = self.alloc_id();
        let node = ThreadSafeEffectNode {
            run: Arc::new(move |ctx| run(ctx).into_thread_safe_cleanup()),
            dependencies: HybridSet::default(),
            cleanup: None,
            force_run: true,
        };
        self.lock_state()
            .insert_node(id, ThreadSafeNode::Effect(node));
        let handle = Effect::new(id);
        self.schedule_effect(id, false);
        self.flush_effects();
        handle
    }

    /// Dispose an effect by handle.
    pub fn dispose_effect(&self, handle: &Effect) {
        let (dependencies, cleanup) = {
            let mut state = self.lock_state();
            // #lzspecedgeindex: deschedule in O(1) and leave any queue entry as
            // a tombstone rather than scanning `pending_effects` for it. Mass
            // teardown during a flush disposes W effects while the queue still
            // holds W of them, making the scan O(W^2) overall.
            // `pop_scheduled_effect` discards the tombstone, which is what
            // keeps a recycled id from triggering a spurious run.
            #[cfg(naive_dispose_scan)]
            state.pending_effects.retain(|queued| *queued != handle.id);
            state.deschedule_effect(handle.id);
            let Some(ThreadSafeNode::Effect(effect)) = state.remove_node(handle.id) else {
                return;
            };
            state.free_ids.push(handle.id.0);
            (effect.dependencies, effect.cleanup)
        };

        for dependency_id in dependencies.iter() {
            self.remove_dependent_edge(dependency_id, handle.id);
        }
        if let Some(cleanup) = cleanup {
            cleanup();
        }
    }

    /// How many nodes currently depend on `node` — the size of its reverse edge
    /// set (`#lzspecedgeindex`).
    ///
    /// [`Context::dependent_count`](crate::Context::dependent_count) for the
    /// shared graph. Takes a read lock only.
    pub fn dependent_count(&self, node: &impl GraphNode) -> usize {
        let state = self.read_state();
        match state.get_node(node.node_id()) {
            Some(ThreadSafeNode::Computed(slot)) => slot.dependents.len(),
            Some(ThreadSafeNode::Source(cell)) => cell.dependents.len(),
            // Effects are pure sinks: nothing can read one.
            Some(ThreadSafeNode::Effect(_)) | None => 0,
        }
    }

    /// How many nodes `node` currently depends on — the size of its forward
    /// edge set (`#lzspecedgeindex`).
    pub fn dependency_count(&self, node: &impl GraphNode) -> usize {
        let state = self.read_state();
        match state.get_node(node.node_id()) {
            Some(ThreadSafeNode::Computed(slot)) => slot.dependencies.len(),
            Some(ThreadSafeNode::Effect(effect)) => effect.dependencies.len(),
            // Cells are pure sources.
            Some(ThreadSafeNode::Source(_)) | None => 0,
        }
    }

    /// Dirty the cone that read a node being disposed (`#lzspecedgeindex`).
    ///
    /// Mirrors [`Context`](crate::Context)'s disposal invalidation: detaching
    /// the edges is not enough, because a dependent holding a cached value would
    /// keep serving it forever once its dependency edge is gone. Effects reached
    /// by the walk are deliberately not scheduled — disposal is not a publish,
    /// and running one here would re-enter a compute that reads the node being
    /// torn down.
    fn invalidate_disposed_dependents_locked(state: &mut ThreadSafeState, dependents: &HybridSet) {
        if dependents.is_empty() {
            return;
        }
        let roots: Vec<ThreadSafeInvalidationRoot> = dependents
            .iter()
            .map(|id| ThreadSafeInvalidationRoot {
                id,
                force_recompute: true,
            })
            .collect();
        ThreadSafeInvalidationPlan::from_roots_locked(state, roots)
            .without_effect_schedules()
            .apply_locked(state);
    }

    /// Tear down a derived slot on the shared graph: detach both edge
    /// directions, dirty the surviving readers, drop the lock-free fast path,
    /// and recycle the id.
    ///
    /// # Concurrency
    ///
    /// **Disposal is atomic with respect to any single read.** A concurrent
    /// reader either acquires state before this call and returns a value
    /// computed from a live node, or acquires it after and finds the node gone.
    /// There is no window in which a reader observes a half-detached node,
    /// because edge detach, invalidation, node removal, fast-path clearing, and
    /// id recycling all happen while this call holds both locks.
    ///
    /// The registry write lock is taken **before** the state lock, matching the
    /// order slot creation already uses. Taking them the other way round would
    /// invert that order and can deadlock.
    ///
    /// Clearing the fast-path registry entry is load-bearing, not tidy-up: the
    /// registry is an index-keyed side table, so a stale entry would alias onto
    /// whatever node next claims the recycled id — exactly the owner-keyed
    /// aliasing `recycled_id_inherits_nothing.json` pins.
    ///
    /// Same caveat as [`Context::dispose_slot`](crate::Context::dispose_slot):
    /// callers must ensure nothing still reads the slot in a live compute.
    pub fn dispose_slot<T>(&self, handle: &Computed<T>) {
        let torn_down = {
            let mut registry = self.inner.slot_fast_paths.write();
            let mut state = self.lock_state();
            // Check the kind BEFORE removing: a stale handle whose id has been
            // recycled must not tear down whatever now owns it.
            if !matches!(state.get_node(handle.id), Some(ThreadSafeNode::Computed(_))) {
                return;
            }
            let Some(ThreadSafeNode::Computed(slot)) = state.remove_node(handle.id) else {
                return;
            };
            for dependency_id in slot.dependencies.iter() {
                Self::remove_dependent_edge_locked(&mut state, dependency_id, handle.id);
            }
            for dependent_id in slot.dependents.iter() {
                Self::remove_parent_dependency_locked(&mut state, dependent_id, handle.id);
            }
            Self::invalidate_disposed_dependents_locked(&mut state, &slot.dependents);
            if let Some(idx) = node_index(handle.id)
                && idx < registry.len()
            {
                registry[idx] = None;
            }
            state.free_ids.push(handle.id.0);
            slot
        };
        // Drop outside both locks: the node owns its compute closure and
        // everything that closure captured, whose Drop may re-enter the context.
        drop(torn_down);
    }

    /// Tear down a source cell on the shared graph: detach its dependents,
    /// dirty them, drop the lock-free fast path, and recycle the id.
    ///
    /// Cells are pure sources with no dependencies, so only downstream edges
    /// need detaching. Same concurrency guarantee and same lock order as
    /// [`Self::dispose_slot`], against the cell registry.
    pub fn dispose_cell<T>(&self, handle: &Source<T>) {
        let torn_down = {
            let mut registry = self.inner.cell_fast_paths.write();
            let mut state = self.lock_state();
            if !matches!(state.get_node(handle.id), Some(ThreadSafeNode::Source(_))) {
                return;
            }
            let Some(ThreadSafeNode::Source(cell)) = state.remove_node(handle.id) else {
                return;
            };
            for dependent_id in cell.dependents.iter() {
                Self::remove_parent_dependency_locked(&mut state, dependent_id, handle.id);
            }
            Self::invalidate_disposed_dependents_locked(&mut state, &cell.dependents);
            if let Some(idx) = node_index(handle.id)
                && idx < registry.len()
            {
                registry[idx] = None;
            }
            state.free_ids.push(handle.id.0);
            cell
        };
        drop(torn_down);
    }

    /// Open a teardown scope: nodes created through it are disposed when it
    /// drops.
    ///
    /// ```
    /// # use lazily::ThreadSafeContext;
    /// let ctx = ThreadSafeContext::new();
    /// let topic = ctx.source(0u64);
    /// {
    ///     let conn = ctx.scope();
    ///     let a = conn.computed(move |c| c.get(&topic) + 1);
    ///     assert_eq!(ctx.get(&a), 1);
    /// } // disposed here
    /// ```
    ///
    /// Unlike [`Context::scope`](crate::Context::scope), which borrows its
    /// context, this holds an **owned** clone of the context handle. That is not
    /// arbitrary divergence — it follows from the ownership model. `Context`
    /// owns its state directly (`RefCell<ContextInner>`), so a borrow is the
    /// only option there; `ThreadSafeContext` is *already* a cheap cloneable
    /// `Arc` handle over shared state, so an owned scope costs one refcount bump
    /// and buys a `'static`, `Send` scope. That is what the motivating case — a
    /// per-connection scope living on a worker thread — actually needs. Please
    /// do not "unify" the three scope types; each fits its context.
    pub fn scope(&self) -> ThreadSafeTeardownScope {
        ThreadSafeTeardownScope {
            ctx: self.clone(),
            owned: Mutex::new(Vec::new()),
        }
    }

    /// Tear down whatever node `id` names, dispatching on its own kind.
    fn dispose_id(&self, id: SlotId) {
        let kind = match self.read_state().get_node(id) {
            Some(ThreadSafeNode::Computed(_)) => 0u8,
            Some(ThreadSafeNode::Source(_)) => 1,
            Some(ThreadSafeNode::Effect(_)) => 2,
            None => return,
        };
        match kind {
            0 => self.dispose_slot(&Computed::<()>::from_id(id)),
            1 => self.dispose_cell(&Source::<()>::from_id(id)),
            _ => self.dispose_effect(&Effect::new(id)),
        }
    }

    /// Check whether an effect is still registered.
    pub fn is_effect_active(&self, handle: &Effect) -> bool {
        let state = self.read_state();
        matches!(state.get_node(handle.id), Some(ThreadSafeNode::Effect(_)))
    }

    // -- Signal API --------------------------------------------------------

    /// Create an **eager** derived value over the shared graph that recomputes
    /// immediately whenever one of its dependencies is invalidated.
    ///
    /// This is the [`ThreadSafeContext`] counterpart to [`Context::signal`]. A
    /// signal sits one step beyond [`computed`](Self::computed)
    /// on the `Slot -> Cell -> Signal` progression:
    ///
    /// - A [`computed`](Self::computed) slot is **lazy**:
    ///   invalidation only marks it dirty, and the value is not recomputed until
    ///   the next read.
    /// - A `Signal` is **eager**: it recomputes the instant any of its
    ///   dependencies are invalidated — before the invalidating
    ///   `set_cell`/`set`/`batch` call returns.
    ///
    /// Because it is backed by a memoized slot and recomputes eagerly, a signal
    /// never exposes an intermediate "unset" value: a dependency change drives
    /// the value directly from `v1` to `v2`. The memo guard means a
    /// recomputation that yields an equal value (via `PartialEq`) does not churn
    /// downstream dependents. Recomputation is pull-based and therefore
    /// glitch-free.
    ///
    /// Internally a signal is a memoized slot plus a small puller effect that
    /// re-materializes the slot after every invalidation.
    pub fn signal<T, F>(&self, compute: F) -> ThreadSafeSignalHandle<T>
    where
        T: PartialEq + Send + Sync + 'static,
        F: Fn(&ThreadSafeContext) -> T + Send + Sync + 'static,
    {
        let slot = self.computed(compute);
        let slot_id = slot.id;
        // Eager puller: re-materializes the slot after every invalidation and
        // registers the slot as a dependency so future invalidations reschedule
        // it. `refresh_signal_slot` refreshes without deep-cloning the value.
        let effect = self.effect(move |ctx: &ThreadSafeContext| {
            ctx.refresh_signal_slot(slot_id);
        });
        ThreadSafeSignalHandle::new(slot, effect)
    }

    /// Read a signal's current value. Always returns a materialized value.
    pub fn get_signal<T: Clone + Send + Sync + 'static>(
        &self,
        handle: &ThreadSafeSignalHandle<T>,
    ) -> T {
        self.get(&handle.slot)
    }

    /// Dispose a signal's eager puller.
    ///
    /// Stops eager recomputation; the backing value remains readable and
    /// reverts to lazy (recomputed on next read) behavior.
    pub fn dispose_signal<T>(&self, handle: &ThreadSafeSignalHandle<T>) {
        self.dispose_effect(&handle.effect);
    }

    /// Check whether a signal's eager puller is still active.
    pub fn is_signal_active<T>(&self, handle: &ThreadSafeSignalHandle<T>) -> bool {
        self.is_effect_active(&handle.effect)
    }

    /// Refresh a signal's backing slot and register it as a dependency of the
    /// running puller effect, without deep-cloning the value out.
    fn refresh_signal_slot(&self, id: SlotId) {
        if let Some(parent_id) = track_dependency(self.context_id(), id) {
            self.register_dependency(id, parent_id);
        }
        let _ = self.refresh_slot(id);
    }

    fn schedule_effect(&self, id: SlotId, force: bool) {
        let mut state = self.lock_state();
        Self::schedule_effect_locked(&mut state, id, force);
    }

    fn schedule_effect_locked(state: &mut ThreadSafeState, id: SlotId, force: bool) {
        match state.get_node_mut(id) {
            Some(ThreadSafeNode::Effect(effect)) => {
                if force {
                    effect.force_run = true;
                }
            }
            _ => return,
        }

        let idx = node_index(id).expect("SlotId does not fit usize");
        let already_scheduled = if idx < state.scheduled_effects.len() {
            state.scheduled_effects[idx]
        } else {
            false
        };
        if !already_scheduled {
            if idx >= state.scheduled_effects.len() {
                state.scheduled_effects.resize(idx + 1, false);
            }
            state.scheduled_effects[idx] = true;
            state.pending_effects.push_back(id);
            #[cfg(feature = "instrumentation")]
            {
                let depth = state.pending_effects.len();
                state.instrumentation.record_effect_queue_push(depth);
            }
        }
    }

    fn flush_effects(&self) {
        let budget = {
            let mut state = self.lock_state();
            if state.flushing_effects {
                return;
            }
            state.flushing_effects = true;
            // Only the outer drain owns the attribution buffer; clearing here
            // (not per iteration) keeps a terminating flush free of scans.
            state.drain_runs.clear();
            if state.drain_budget == 0 {
                DEFAULT_DRAIN_BUDGET
            } else {
                state.drain_budget
            }
        };
        let mut guard = FlushGuard {
            ctx: self.clone(),
            active: true,
        };

        // `#lzfeedbackdrain`: the bound is on iterations, not re-entry depth. A
        // nested `flush_effects` returns immediately at the guard above, so a
        // scheduler-closed feedback loop is a flat unbounded drain here, and the
        // only structural exit is this budget. On exhaustion the drain stops and
        // records a `DrainExhaustion` rather than spinning forever.
        let mut iterations: usize = 0;
        loop {
            let id = {
                let mut state = self.lock_state();
                if let Some(id) = state.pop_scheduled_effect() {
                    if let Some(idx) = node_index(id) {
                        if idx >= state.drain_runs.len() {
                            state.drain_runs.resize(idx + 1, 0);
                        }
                        state.drain_runs[idx] = state.drain_runs[idx].saturating_add(1);
                    }
                    iterations += 1;
                    if iterations >= budget {
                        let report = Self::drain_exhaustion_report(&state, iterations, budget);
                        state.last_drain_exhaustion = Some(report);
                        state.flushing_effects = false;
                        guard.active = false;
                        None
                    } else {
                        Some(id)
                    }
                } else {
                    state.flushing_effects = false;
                    guard.active = false;
                    None
                }
            };
            let Some(id) = id else {
                break;
            };
            self.run_effect(id);
        }
    }

    /// Build the exhaustion report: the busiest effects in the current drain,
    /// descending. Mirrors [`Context::drain_exhaustion_report`].
    fn drain_exhaustion_report(
        state: &ThreadSafeState,
        iterations: usize,
        budget: usize,
    ) -> DrainExhaustion {
        const TOP_N: usize = 8;
        let mut top: Vec<(u64, u32)> = state
            .drain_runs
            .iter()
            .enumerate()
            .filter(|&(_, &runs)| runs > 0)
            .map(|(idx, &runs)| (idx as u64, runs))
            .collect();
        top.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        top.truncate(TOP_N);
        DrainExhaustion {
            iterations,
            budget,
            top_effects: top,
        }
    }

    /// The most recent drain exhaustion, if any drain has been cut short
    /// (`#lzfeedbackdrain`). `None` means every drain so far ended with an empty
    /// worklist. A divergent scheduler-closed loop surfaces here rather than
    /// hanging. Mirrors [`Context::last_drain_exhaustion`].
    pub fn last_drain_exhaustion(&self) -> Option<DrainExhaustion> {
        self.read_state().last_drain_exhaustion.clone()
    }

    /// Clear the recorded exhaustion so a later drain can be observed
    /// independently. Mirrors [`Context::clear_drain_exhaustion`].
    pub fn clear_drain_exhaustion(&self) {
        self.lock_state().last_drain_exhaustion = None;
    }

    /// Current effect-drain iteration budget (`#lzfeedbackdrain`).
    pub fn drain_budget(&self) -> usize {
        let budget = self.read_state().drain_budget;
        if budget == 0 {
            DEFAULT_DRAIN_BUDGET
        } else {
            budget
        }
    }

    /// Override the effect-drain iteration budget. Lowering it is how a test
    /// exercises divergence without waiting for the full default.
    ///
    /// # Panics
    /// If `budget` is zero.
    pub fn set_drain_budget(&self, budget: usize) {
        assert!(budget > 0, "drain budget must be non-zero");
        self.lock_state().drain_budget = budget;
    }

    fn run_effect(&self, id: SlotId) {
        if !self.effect_should_run(id) {
            return;
        }

        let (run, old_dependencies, cleanup) = {
            let mut state = self.lock_state();
            // #lzspecedgeindex: the scheduled-effects bitset is authoritative —
            // an id is queued only if `schedule_effect` pushed it, and that
            // push is gated on (and sets) the flag. `flush_effects` pops and
            // deschedules before calling here, so the id is provably absent
            // and the O(queue) scan can be skipped. Scanning anyway cost O(W)
            // per effect, i.e. O(W^2) per publish.
            let scheduled = node_index(id).is_some_and(|idx| {
                idx < state.scheduled_effects.len() && state.scheduled_effects[idx]
            });
            if scheduled {
                state.pending_effects.retain(|queued| *queued != id);
                state.deschedule_effect(id);
            }
            let effect = match state.get_node_mut(id) {
                Some(ThreadSafeNode::Effect(effect)) => effect,
                _ => return,
            };
            let old_dependencies: EdgeVec = effect.dependencies.iter().collect();
            let cleanup = effect.cleanup.take();
            effect.force_run = false;
            (Arc::clone(&effect.run), old_dependencies, cleanup)
        };

        if let Some(cleanup) = cleanup {
            cleanup();
        }

        let _tracking = push_tracking_frame_with_known_dependencies(
            self.context_id(),
            id,
            old_dependencies.clone(),
        );
        let _callback_activity = self.callback_activity();
        let next_cleanup = run(self);
        let new_dependencies = _tracking.finish();

        let mut state = self.lock_state();
        if matches!(state.get_node(id), Some(ThreadSafeNode::Effect(_))) {
            Self::remove_stale_dependencies_locked(
                &mut state,
                id,
                &old_dependencies,
                &new_dependencies,
            );
        }
        if let Some(ThreadSafeNode::Effect(effect)) = state.get_node_mut(id) {
            effect.cleanup = next_cleanup;
        } else if let Some(cleanup) = next_cleanup {
            drop(state);
            cleanup();
        }
    }

    fn effect_should_run(&self, id: SlotId) -> bool {
        let (force_run, dependencies) = {
            let state = self.read_state();
            let Some(ThreadSafeNode::Effect(effect)) = state.get_node(id) else {
                return false;
            };
            (effect.force_run, effect.dependencies.clone())
        };

        if force_run {
            return true;
        }

        dependencies
            .iter()
            .any(|dependency_id| self.refresh_slot(dependency_id))
    }

    /// Hard-clear a slot and recursively clear dependents.
    pub fn clear<T>(&self, handle: &Computed<T>) {
        self.clear_slot(handle.id);
        self.flush_effects_after_invalidation();
    }

    fn clear_slot(&self, id: SlotId) {
        if self.queue_batched_slot_clear(id) {
            return;
        }

        let should_clear = {
            let mut state = self.lock_state();
            if state.batch_depth > 0 {
                state.batched_slots.push(id);
                false
            } else {
                true
            }
        };

        if should_clear {
            self.clear_slot_now(id);
        }
    }

    fn flush_effects_after_invalidation(&self) {
        if !self.is_batching() {
            self.flush_effects();
        }
    }

    fn clear_slot_now_locked(state: &mut ThreadSafeState, id: SlotId) {
        Self::clear_frontier_locked(state, [id]);
    }

    fn clear_slot_now(&self, id: SlotId) {
        let mut state = self.lock_state();
        Self::clear_slot_now_locked(&mut state, id);
    }

    /// Clear all dependent slots without changing the cell value.
    pub fn clear_cell_dependents<T>(&self, handle: &Source<T>) {
        if self.queue_batched_cell_clear(handle.id) {
            return;
        }

        let should_flush = {
            let mut state = self.lock_state();
            if state.batch_depth > 0 {
                state.batched_cell_clears.push(handle.id);
                false
            } else {
                Self::clear_cell_dependents_locked(&mut state, handle.id);
                true
            }
        };

        if should_flush {
            self.flush_effects();
        }
    }

    fn invalidate_cell_dependents_locked(state: &mut ThreadSafeState, id: SlotId) {
        state.fill_dependent_scratch(id);
        let roots: RootVec = state
            .dependent_scratch
            .iter()
            .map(|&id| ThreadSafeInvalidationRoot {
                id,
                force_recompute: true,
            })
            .collect();
        Self::invalidate_frontier_locked(state, roots);
    }

    fn clear_cell_dependents_locked(state: &mut ThreadSafeState, id: SlotId) {
        state.fill_dependent_scratch(id);
        let deps: EdgeVec = state.dependent_scratch.as_slice().into();
        Self::clear_frontier_locked(state, deps);
    }

    fn notify_slot_value_changed_locked(state: &mut ThreadSafeState, id: SlotId) {
        state.fill_dependent_scratch(id);
        let roots: RootVec = state
            .dependent_scratch
            .iter()
            .map(|&id| ThreadSafeInvalidationRoot {
                id,
                force_recompute: true,
            })
            .collect();
        Self::invalidate_frontier_locked(state, roots);
    }

    #[cfg(test)]
    fn dependents_locked(state: &ThreadSafeState, id: SlotId) -> EdgeVec {
        match state.get_node(id) {
            Some(ThreadSafeNode::Computed(slot)) => slot.dependents.iter().collect(),
            Some(ThreadSafeNode::Source(cell)) => cell.dependents.iter().collect(),
            Some(ThreadSafeNode::Effect(_)) | None => EdgeVec::new(),
        }
    }

    fn invalidate_frontier_locked<I>(state: &mut ThreadSafeState, roots: I)
    where
        I: IntoIterator<Item = ThreadSafeInvalidationRoot>,
    {
        ThreadSafeInvalidationPlan::from_roots_locked(state, roots).apply_locked(state);
    }

    fn clear_frontier_locked<I>(state: &mut ThreadSafeState, roots: I)
    where
        I: IntoIterator<Item = SlotId>,
    {
        ThreadSafeInvalidationPlan::from_clear_roots_locked(state, roots).apply_locked(state);
    }

    /// Check whether a slot currently has a cached, fresh value.
    pub fn is_set<T>(&self, handle: &Computed<T>) -> bool
    where
        T: Send + Sync + 'static,
    {
        let state = self.read_state();
        if let Some(ThreadSafeNode::Computed(slot)) = state.get_node(handle.id) {
            slot.value.is_some() && !slot.dirty && !slot.fast_path.needs_refresh()
        } else {
            false
        }
    }

    /// Return the current benchmark instrumentation counters.
    #[cfg(feature = "instrumentation")]
    pub fn instrumentation_snapshot(&self) -> crate::instrumentation::InstrumentationSnapshot {
        let mut snapshot = {
            let state = lock_state_inner(&self.inner.state);
            state.instrumentation.snapshot()
        };
        self.inner
            .lock_instrumentation
            .apply_to_snapshot(&mut snapshot);
        self.inner
            .invalidation_instrumentation
            .apply_to_snapshot(&mut snapshot);
        snapshot
    }

    /// Return ThreadSafeContext lock and coordination counters grouped by
    /// operation.
    #[cfg(feature = "instrumentation")]
    pub fn lock_profile_snapshot(
        &self,
    ) -> [crate::instrumentation::ThreadSafeLockSiteSnapshot;
        crate::instrumentation::THREAD_SAFE_LOCK_SITE_COUNT] {
        self.inner.lock_instrumentation.site_snapshots()
    }

    /// Reset benchmark instrumentation counters to zero.
    #[cfg(feature = "instrumentation")]
    pub fn reset_instrumentation(&self) {
        {
            let mut state = lock_state_inner(&self.inner.state);
            state.instrumentation.reset();
        }
        self.inner.lock_instrumentation.reset();
        self.inner.invalidation_instrumentation.reset();
    }
}

// -- Capability trait impls (#lzspecedgeindex) -------------------------------

impl crate::reactive_graph::Teardown for ThreadSafeTeardownScope {
    fn len(&self) -> usize {
        ThreadSafeTeardownScope::len(self)
    }
    fn disarm(self) {
        ThreadSafeTeardownScope::disarm(self);
    }
}

impl crate::reactive_graph::ReactiveGraph for ThreadSafeContext {
    type Computed<T> = crate::cell::Computed<T>;
    type Source<T> = crate::cell::Source<T>;
    type Effect = crate::effect::Effect;
    // Owned, so the GAT lifetime is unused: the scope outlives the borrow that
    // produced it and is `Send`.
    type Scope<'a> = ThreadSafeTeardownScope;

    fn dispose_slot<T: 'static>(&self, handle: &Self::Computed<T>) {
        ThreadSafeContext::dispose_slot(self, handle);
    }
    fn dispose_cell<T: 'static>(&self, handle: &Self::Source<T>) {
        ThreadSafeContext::dispose_cell(self, handle);
    }
    fn dispose_effect(&self, handle: &Self::Effect) {
        ThreadSafeContext::dispose_effect(self, handle);
    }
    fn scope(&self) -> Self::Scope<'_> {
        ThreadSafeContext::scope(self)
    }
    fn batch<R>(&self, run: impl FnOnce(&Self) -> R) -> R {
        ThreadSafeContext::batch(self, run)
    }
    fn dependent_count(&self, node: &impl GraphNode) -> usize {
        ThreadSafeContext::dependent_count(self, node)
    }
    fn dependency_count(&self, node: &impl GraphNode) -> usize {
        ThreadSafeContext::dependency_count(self, node)
    }
}

impl crate::reactive_graph::SyncReactiveGraph for ThreadSafeContext {
    // `ThreadSafeContext` keeps its own ambient tracking engine
    // (`THREAD_SAFE_TRACKING_STACK`), so its per-recompute view is the context
    // itself: `computed`/`effect` closures receive `&ThreadSafeContext`, exactly
    // as its inherent API expects. The GAT lifetime is unused here.
    type Compute<'a> = ThreadSafeContext;

    fn source<T>(&self, value: T) -> Self::Source<T>
    where
        T: PartialEq + Send + Sync + 'static,
    {
        ThreadSafeContext::source(self, value)
    }
    fn computed<T, F>(&self, compute: F) -> Self::Computed<T>
    where
        T: PartialEq + Send + Sync + 'static,
        F: Fn(&Self::Compute<'_>) -> T + Send + Sync + 'static,
    {
        ThreadSafeContext::computed(self, compute)
    }
    fn effect<F, C>(&self, run: F) -> Self::Effect
    where
        F: Fn(&Self::Compute<'_>) -> C + Send + Sync + 'static,
        C: FnOnce() + Send + Sync + 'static,
    {
        ThreadSafeContext::effect(self, run)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<ThreadSafeContext> for Computed<T> {
    type Output = T;
    fn read(&self, ctx: &ThreadSafeContext) -> T {
        ctx.get_slot(self.id)
    }
}

impl<T: Clone + Send + Sync + 'static> Read<ThreadSafeContext> for Source<T> {
    type Output = T;
    fn read(&self, ctx: &ThreadSafeContext) -> T {
        ctx.read_source(self)
    }
}

impl<T: PartialEq + Send + Sync + 'static> Write<ThreadSafeContext> for Source<T> {
    type Value = T;
    fn write(&self, ctx: &ThreadSafeContext, value: T) {
        ctx.write_source(self, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // #lzqfrs Phase-3 prerequisite: does ThreadSafeContext need a multi-root
    // `clear_slots`, or does `batch()` already close that gap?
    //
    // The queue-family plan assumed four reader-kind handles meant four frontier
    // walks and queued a new multi-root clear API. It does not: `clear_slot` pushes
    // into `state.batched_slots` while `batch_depth > 0`, and `finish_batch` sorts,
    // dedups, and calls `flush_batched_invalidations` exactly once, handing every
    // collected root to a single `clear_frontier_locked`.
    //
    // Measuring it took a second attempt worth recording. Counting recomputes of a
    // shared downstream does NOT separate the cases: `computed` is guarded, so when
    // the cleared nodes recompute to the same values the downstream is correctly
    // suppressed and batched/unbatched look identical. The observable that does
    // separate them is the instrumented lock site the flush runs under — one flush
    // is one `SetCellInvalidation` acquisition, N flushes are N.
    //
    // Lives in-crate because `site_snapshots()` is `pub(crate)`; widening the public
    // API just to measure this was not worth it.
    #[cfg(feature = "instrumentation")]
    #[test]
    fn batch_gives_clear_a_single_multi_root_frontier() {
        use crate::instrumentation::ThreadSafeLockSite;

        fn invalidation_acquisitions(ctx: &ThreadSafeContext) -> u64 {
            ctx.inner
                .lock_instrumentation
                .site_snapshots()
                .iter()
                .find(|s| s.site == ThreadSafeLockSite::SetCellInvalidation)
                .map(|s| s.lock_acquisitions)
                .expect("SetCellInvalidation is a known lock site")
        }

        let ctx = ThreadSafeContext::new();

        // Four independent roots, mirroring a queue's four reader-kind handles.
        let roots: Vec<_> = (0..4).map(|i| ctx.source(i as i64)).collect();
        let derived: Vec<_> = roots
            .iter()
            .map(|root| {
                let root = *root;
                ctx.computed(move |cx| cx.get(&root) * 2)
            })
            .collect();
        for d in &derived {
            let _ = ctx.get(d);
        }

        // Batched: four clears, one flush.
        let before = invalidation_acquisitions(&ctx);
        ctx.batch(|_| {
            for d in &derived {
                ctx.clear(d);
            }
        });
        let batched = invalidation_acquisitions(&ctx) - before;

        for d in &derived {
            let _ = ctx.get(d);
        }

        // The discriminator is ROOT COUNT, not batched-vs-unbatched. Two earlier
        // probes failed and both failures are informative: recompute counts cannot
        // separate the cases (the guard suppresses an unchanged downstream), and
        // batched-vs-unbatched cannot either, because `SetCellInvalidation` is
        // recorded only by `flush_batched_invalidations` — the unbatched path is not
        // instrumented, so that control read 0 and proved nothing.
        //
        // What the claim actually says is "one flush no matter how many roots". So
        // clear twice as many handles in one batch: still one flush if the roots are
        // coalesced, eight if each walks on its own.
        let wide: Vec<_> = (0..8)
            .map(|i| {
                let root = ctx.source(i as i64);
                ctx.computed(move |cx| cx.get(&root) * 2)
            })
            .collect();
        for d in &wide {
            let _ = ctx.get(d);
        }
        let before = invalidation_acquisitions(&ctx);
        ctx.batch(|_| {
            for d in &wide {
                ctx.clear(d);
            }
        });
        let wide_batched = invalidation_acquisitions(&ctx) - before;

        assert_eq!(
            wide_batched, 1,
            "eight clears in one batch must ALSO be one flush, got {wide_batched}; \
             a count that tracks the number of roots means no coalescing"
        );
        assert_eq!(
            batched, wide_batched,
            "flush count must be independent of how many roots were cleared \
             (four roots -> {batched}, eight roots -> {wide_batched})"
        );

        for (i, d) in derived.iter().enumerate() {
            assert_eq!(ctx.get(d), (i as i64) * 2);
        }
    }

    // -- #lzspecedgeindex disposal ----------------------------------------

    #[test]
    fn dispose_slot_detaches_both_directions_and_invalidates_readers() {
        let ctx = ThreadSafeContext::new();
        let src = ctx.source(4i64);
        let derived = ctx.computed(move |c| c.get(&src));
        let reader = ctx.computed(move |c| c.get(&derived) + 1);
        assert_eq!(ctx.get(&reader), 5);
        assert_eq!(ctx.dependent_count(&src), 1);
        assert_eq!(ctx.dependency_count(&reader), 1);

        ctx.dispose_slot(&derived);
        // Both directions detached.
        assert_eq!(ctx.dependent_count(&src), 0);
        assert_eq!(ctx.dependency_count(&derived), 0);
        // And the surviving reader must not serve its pre-disposal cache.
        let after = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ctx.get(&reader)));
        assert!(
            after.is_err(),
            "reader must not serve its pre-disposal cache"
        );
    }

    #[test]
    fn dispose_clears_the_fast_path_registry_so_a_recycled_id_inherits_nothing() {
        // The registry is an index-keyed side table; a stale entry would alias
        // onto whatever next claims the recycled id.
        let ctx = ThreadSafeContext::new();
        let topic = ctx.source(1i64);
        let wide = ctx.computed(move |c| c.get(&topic));
        assert_eq!(ctx.get(&wide), 1);
        let wide_idx = node_index(wide.id).unwrap();
        assert!(ctx.inner.slot_fast_paths.read()[wide_idx].is_some());

        ctx.dispose_slot(&wide);
        assert!(
            ctx.inner
                .slot_fast_paths
                .read()
                .get(wide_idx)
                .and_then(|o| o.as_ref())
                .is_none(),
            "disposal must drop the fast-path registry entry"
        );

        // A node minted after the disposal starts with an empty edge set.
        let reused = ctx.computed(|_| 7i64);
        assert_eq!(ctx.dependent_count(&reused), 0);
        assert_eq!(ctx.dependency_count(&reused), 0);
        assert_eq!(ctx.get(&reused), 7);
    }

    #[test]
    fn dispose_cell_detaches_dependents_and_is_kind_checked() {
        let ctx = ThreadSafeContext::new();
        let topic = ctx.source(1i64);
        let reader = ctx.computed(move |c| c.get(&topic) + 1);
        assert_eq!(ctx.get(&reader), 2);
        assert_eq!(ctx.dependent_count(&topic), 1);

        ctx.dispose_cell(&topic);
        assert_eq!(ctx.dependency_count(&reader), 0);
        // Disposing twice is a no-op, not an error.
        ctx.dispose_cell(&topic);
        // A stale *slot* handle over the recycled id must not tear down a cell.
        let stale = Computed::<i64>::from_id(topic.id);
        ctx.dispose_slot(&stale);
    }

    #[test]
    fn teardown_scope_disposes_in_reverse_creation_order_and_disarm_cancels() {
        let ctx = ThreadSafeContext::new();
        let topic = ctx.source(1i64);
        {
            let scope = ctx.scope();
            let a = scope.computed(move |c| c.get(&topic) + 1);
            let _b = scope.computed(move |c| c.get(&a) + 1);
            assert_eq!(scope.len(), 2);
            assert_eq!(ctx.get(&a), 2);
            assert_eq!(ctx.dependent_count(&topic), 1);
        }
        assert_eq!(ctx.dependent_count(&topic), 0);

        // Disarmed: ending the scope disposes nothing.
        let kept = {
            let scope = ctx.scope();
            let a = scope.computed(move |c| c.get(&topic) + 5);
            assert_eq!(ctx.get(&a), 6);
            scope.disarm();
            a
        };
        assert_eq!(ctx.get(&kept), 6);
        assert_eq!(ctx.dependent_count(&topic), 1);
    }

    #[test]
    fn teardown_scope_is_send_and_owns_its_context() {
        // The owned-handle design exists so a scope can move to another thread;
        // a borrow-based scope could not.
        let ctx = ThreadSafeContext::new();
        let topic = ctx.source(1i64);
        let scope = ctx.scope();
        let a = scope.computed(move |c| c.get(&topic) + 1);
        assert_eq!(ctx.get(&a), 2);
        std::thread::spawn(move || drop(scope)).join().unwrap();
        assert_eq!(ctx.dependent_count(&topic), 0);
    }

    #[test]
    fn read_strategy_defaults_to_low_concurrency() {
        assert_eq!(
            ThreadSafeContext::new().read_strategy(),
            ReadStrategy::LowConcurrency
        );
        assert_eq!(
            ThreadSafeContext::with_read_strategy(ReadStrategy::HighConcurrency).read_strategy(),
            ReadStrategy::HighConcurrency
        );
    }

    #[test]
    fn both_read_strategies_cache_and_invalidate_correctly() {
        for strategy in [ReadStrategy::LowConcurrency, ReadStrategy::HighConcurrency] {
            let ctx = ThreadSafeContext::with_read_strategy(strategy);
            let cell = ctx.source(2_i32);
            let doubled = ctx.computed(move |c| c.get(&cell) * 10);
            // cold compute + cached read
            assert_eq!(ctx.get(&doubled), 20, "{strategy:?}");
            assert_eq!(ctx.get(&doubled), 20, "{strategy:?} cached");
            // invalidate via cell write, recompute
            ctx.set(&cell, 5);
            assert_eq!(ctx.get(&doubled), 50, "{strategy:?} after set");
        }
    }

    // #lzrsgetarc: `get` deep-clones the value on every read; `get_arc` hands
    // out the stored `Arc` instead, so repeat reads of an expensive value cost
    // a refcount bump.
    #[test]
    fn get_arc_shares_one_allocation_across_reads() {
        for strategy in [ReadStrategy::LowConcurrency, ReadStrategy::HighConcurrency] {
            let ctx = ThreadSafeContext::with_read_strategy(strategy);
            let cell = ctx.source(3_usize);
            let text = ctx.computed(move |c| "ab".repeat(c.get(&cell)));

            let first = ctx.get_arc(&text);
            let second = ctx.get_arc(&text);
            assert_eq!(&*first, "ababab", "{strategy:?}");
            assert!(
                Arc::ptr_eq(&first, &second),
                "{strategy:?}: cached reads must share one allocation"
            );
            assert_eq!(
                ctx.get(&text),
                *first,
                "{strategy:?}: get agrees with get_arc"
            );
        }
    }

    #[test]
    fn get_arc_recomputes_after_invalidation() {
        let ctx = ThreadSafeContext::new();
        let cell = ctx.source(1_usize);
        let text = ctx.computed(move |c| "x".repeat(c.get(&cell)));

        let stale = ctx.get_arc(&text);
        assert_eq!(&*stale, "x");

        ctx.set(&cell, 4);
        let fresh = ctx.get_arc(&text);
        assert_eq!(&*fresh, "xxxx");
        assert!(
            !Arc::ptr_eq(&stale, &fresh),
            "recompute publishes a new allocation"
        );
        // The handle taken before the write stays valid and unmutated — that is
        // the point of handing out an `Arc` rather than a borrow.
        assert_eq!(&*stale, "x");
    }

    #[test]
    fn get_arc_tracks_dependencies_like_get() {
        let ctx = ThreadSafeContext::new();
        let cell = ctx.source(2_usize);
        let inner = ctx.computed(move |c| "y".repeat(c.get(&cell)));
        let outer = ctx.computed(move |c| c.get_arc(&inner).len());

        assert_eq!(ctx.get(&outer), 2);
        ctx.set(&cell, 5);
        assert_eq!(
            ctx.get(&outer),
            5,
            "a get_arc read inside a compute must register the edge"
        );
    }

    fn slot_storage_kind<T>(ctx: &ThreadSafeContext, handle: &Computed<T>) -> &'static str
    where
        T: Send + Sync + 'static,
    {
        let state = ctx.lock_state();
        match state.get_node(handle.id) {
            Some(ThreadSafeNode::Computed(slot)) => match &slot.fast_path.value {
                CachedReadStorage::Locked(_) => "locked",
                CachedReadStorage::LockFree(_) => "lockfree",
                CachedReadStorage::Inline(_) => "inline",
            },
            _ => panic!("slot_storage_kind called on non-slot id"),
        }
    }

    #[test]
    fn slot_copy_uses_inline_storage_in_both_strategies(/* #rdstrat2 */) {
        for strategy in [ReadStrategy::LowConcurrency, ReadStrategy::HighConcurrency] {
            let ctx = ThreadSafeContext::with_read_strategy(strategy);
            let cell = ctx.source(2_i32);
            let doubled = ctx.computed_copy(move |c| c.get(&cell) * 10);
            // The inline seqlock subsumes the read-strategy tradeoff for small
            // `Copy` values, so it is selected regardless of `strategy`.
            assert_eq!(slot_storage_kind(&ctx, &doubled), "inline", "{strategy:?}");
            assert_eq!(ctx.get(&doubled), 20, "{strategy:?}");
            assert_eq!(ctx.get(&doubled), 20, "{strategy:?} cached");
            ctx.set(&cell, 5);
            assert_eq!(ctx.get(&doubled), 50, "{strategy:?} after set");
        }
    }

    #[test]
    fn slot_copy_large_copy_value_falls_back_to_strategy_path() {
        // 32 bytes > INLINE_CAP (24) → not inline-eligible; uses the strategy
        // path (RwLock for the default LowConcurrency).
        let ctx = ThreadSafeContext::with_read_strategy(ReadStrategy::LowConcurrency);
        let cell = ctx.source(7u8);
        let big = ctx.slot_copy(move |c| [c.get(&cell); 32]);
        assert_eq!(slot_storage_kind(&ctx, &big), "locked");
        assert_eq!(ctx.get(&big), [7u8; 32]);
        ctx.set(&cell, 9);
        assert_eq!(ctx.get(&big), [9u8; 32]);
    }

    #[test]
    fn memo_copy_inline_roundtrips_multifield_struct() {
        #[derive(Clone, Copy, PartialEq, Debug)]
        struct Point {
            x: i32,
            y: i32,
            z: i32,
        }
        let ctx = ThreadSafeContext::new();
        let cell = ctx.source(1_i32);
        let p = ctx.computed_copy(move |c| {
            let v = c.get(&cell);
            Point {
                x: v,
                y: v * 2,
                z: v * 3,
            }
        });
        assert_eq!(slot_storage_kind(&ctx, &p), "inline");
        assert_eq!(ctx.get(&p), Point { x: 1, y: 2, z: 3 });
        ctx.set(&cell, 4);
        assert_eq!(ctx.get(&p), Point { x: 4, y: 8, z: 12 });
    }

    #[test]
    fn inline_seqlock_concurrent_readers_never_observe_torn_value() {
        // A `Copy` value whose two halves a writer always keeps equal (and a
        // monotonically non-decreasing tag). A torn read (half old / half new)
        // would surface as `a != b`; the seqlock must make every lock-free
        // `read_fresh` observe a complete, self-consistent snapshot — that is
        // the inline-storage safety property (freshness vs the latest publish
        // is the same envelope as the other strategies and is covered by the
        // single-threaded tests; under concurrent recompute a reader may win the
        // publish race, so exact freshness is not asserted here — only that no
        // value is ever observed torn). Loom proves the orderings in
        // `thread_safe_loom::inline_seqlock_*` (exhaustive for the raw
        // seqlock; the combined envelope model is preemption-bounded).
        #[derive(Clone, Copy)]
        struct Pair {
            a: u64,
            b: u64,
        }
        let ctx = Arc::new(ThreadSafeContext::new());
        let cell = ctx.source(0u64);
        let pair = ctx.slot_copy(move |c| {
            let v = c.get(&cell);
            Pair { a: v, b: v }
        });
        assert_eq!(slot_storage_kind(&ctx, &pair), "inline");
        let _ = ctx.get(&pair);

        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let ctx = Arc::clone(&ctx);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let p = ctx.get(&pair);
                        assert_eq!(p.a, p.b, "torn inline read: {} != {}", p.a, p.b);
                    }
                })
            })
            .collect();

        for i in 1..3000u64 {
            // Each publish (seqlock write under the state lock) races the
            // lock-free readers above.
            ctx.set(&cell, i);
            let _ = ctx.get(&pair);
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().expect("reader thread panicked (torn read)");
        }

        // After quiescence the inline value reflects the final publish.
        ctx.set(&cell, 4242);
        let p = ctx.get(&pair);
        assert_eq!(p.a, 4242);
        assert_eq!(p.b, 4242);
    }

    fn cell_storage_kind<T>(ctx: &ThreadSafeContext, handle: &Source<T>) -> &'static str
    where
        T: Send + Sync + 'static,
    {
        let state = ctx.lock_state();
        match state.get_node(handle.id) {
            Some(ThreadSafeNode::Source(cell)) => match &cell.fast_path.value {
                CellCachedReadStorage::Locked(_) => "locked",
                CellCachedReadStorage::Inline { .. } => "inline",
            },
            _ => panic!("cell_storage_kind called on non-cell id"),
        }
    }

    #[test]
    fn cell_uses_locked_storage_in_both_strategies(/* #lzcellread */) {
        for strategy in [ReadStrategy::LowConcurrency, ReadStrategy::HighConcurrency] {
            let ctx = ThreadSafeContext::with_read_strategy(strategy);
            let cell = ctx.source(42_i32);
            assert_eq!(cell_storage_kind(&ctx, &cell), "locked", "{strategy:?}");
            assert_eq!(ctx.get(&cell), 42, "{strategy:?}");
            ctx.set(&cell, 7);
            assert_eq!(ctx.get(&cell), 7, "{strategy:?} after set");
        }
    }

    #[test]
    fn cell_copy_uses_inline_storage_for_small_copy_value(/* #lzcellread */) {
        for strategy in [ReadStrategy::LowConcurrency, ReadStrategy::HighConcurrency] {
            let ctx = ThreadSafeContext::with_read_strategy(strategy);
            let cell = ctx.source_copy(42_i32);
            // The inline seqlock subsumes the read-strategy tradeoff for small
            // `Copy` values, so it is selected regardless of `strategy`.
            assert_eq!(cell_storage_kind(&ctx, &cell), "inline", "{strategy:?}");
            assert_eq!(ctx.get(&cell), 42, "{strategy:?}");
            assert_eq!(ctx.get(&cell), 42, "{strategy:?} cached");
            ctx.set(&cell, 7);
            assert_eq!(ctx.get(&cell), 7, "{strategy:?} after set");
            // set_if_changed suppresses when value is unchanged.
            let doubled = ctx.computed_copy(move |c| c.get(&cell) * 10);
            assert_eq!(ctx.get(&doubled), 70, "{strategy:?}");
            ctx.set(&cell, 7); // no-op
            assert_eq!(cell_dependents_len(&ctx, &cell), 1, "{strategy:?}");
        }
    }

    #[test]
    fn cell_copy_large_copy_value_falls_back_to_locked() {
        // 32 bytes > INLINE_CAP (24) → not inline-eligible; uses the Locked
        // (RwLock) path.
        let ctx = ThreadSafeContext::new();
        let cell = ctx.source_copy([0u8; 32]);
        assert_eq!(cell_storage_kind(&ctx, &cell), "locked");
        ctx.set(&cell, [9u8; 32]);
        assert_eq!(ctx.get(&cell), [9u8; 32]);
    }

    #[test]
    fn cell_copy_inline_roundtrips_multifield_struct() {
        #[derive(Clone, Copy, PartialEq, Debug)]
        struct Point {
            x: i32,
            y: i32,
            z: i32,
        }
        let ctx = ThreadSafeContext::new();
        let cell = ctx.source_copy(Point { x: 1, y: 2, z: 3 });
        assert_eq!(cell_storage_kind(&ctx, &cell), "inline");
        assert_eq!(ctx.get(&cell), Point { x: 1, y: 2, z: 3 });
        ctx.set(&cell, Point { x: 4, y: 8, z: 12 });
        assert_eq!(ctx.get(&cell), Point { x: 4, y: 8, z: 12 });
    }

    #[test]
    fn cell_copy_concurrent_readers_never_observe_torn_value(/* #lzcellread */) {
        // A `Copy` value whose two halves a writer always keeps equal. A torn
        // read (half old / half new) would surface as `a != b`; the cell inline
        // seqlock must make every lock-free `get_cell` observe a complete,
        // self-consistent snapshot. Mirrors the slot inline seqlock test.
        #[derive(Clone, Copy, PartialEq)]
        struct Pair {
            a: u64,
            b: u64,
        }
        let ctx = Arc::new(ThreadSafeContext::new());
        let cell = ctx.source_copy(Pair { a: 0, b: 0 });
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let ctx = Arc::clone(&ctx);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let p = ctx.get(&cell);
                        assert_eq!(p.a, p.b, "torn inline cell read: {} != {}", p.a, p.b);
                    }
                })
            })
            .collect();
        for i in 1..3000u64 {
            ctx.set(&cell, Pair { a: i, b: i });
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().expect("reader thread panicked (torn read)");
        }
        ctx.set(&cell, Pair { a: 4242, b: 4242 });
        assert_eq!(ctx.get(&cell).a, 4242);
    }

    #[test]
    fn cell_locked_concurrent_readers_scale(/* #lzcellread */) {
        // The Locked (RwLock) cell path: concurrent get_cell readers take a
        // shared read lock (not an exclusive Mutex). Asserts correctness under
        // concurrent reads + writes for a non-Copy value (String).
        let ctx = Arc::new(ThreadSafeContext::new());
        let cell = ctx.source("init".to_string());
        let stop = Arc::new(AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let ctx = Arc::clone(&ctx);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let _v = ctx.get(&cell);
                    }
                })
            })
            .collect();
        for i in 0..1000u64 {
            ctx.set(&cell, format!("v{i}"));
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().expect("reader thread panicked");
        }
        assert_eq!(ctx.get(&cell), "v999");
    }

    fn slot_revision<T>(ctx: &ThreadSafeContext, handle: &Computed<T>) -> u64
    where
        T: Send + Sync + 'static,
    {
        let state = ctx.lock_state();
        match state.get_node(handle.id) {
            Some(ThreadSafeNode::Computed(slot)) => slot.revision,
            _ => panic!("slot_revision called on non-slot id"),
        }
    }

    fn slot_dirty_force<T>(ctx: &ThreadSafeContext, handle: &Computed<T>) -> (bool, bool)
    where
        T: Send + Sync + 'static,
    {
        let state = ctx.lock_state();
        match state.get_node(handle.id) {
            Some(ThreadSafeNode::Computed(slot)) => (slot.dirty, slot.force_recompute),
            _ => panic!("slot_dirty_force called on non-slot id"),
        }
    }

    fn cell_dependents_len<T>(ctx: &ThreadSafeContext, handle: &Source<T>) -> usize
    where
        T: Send + Sync + 'static,
    {
        let state = ctx.lock_state();
        match state.get_node(handle.id) {
            Some(ThreadSafeNode::Source(cell)) => cell.dependents.len(),
            _ => panic!("cell_dependents_len called on non-cell id"),
        }
    }

    fn effect_is_scheduled(ctx: &ThreadSafeContext, handle: &Effect) -> bool {
        let state = ctx.lock_state();
        state.is_effect_scheduled(handle.id)
    }

    fn pending_effect_count(ctx: &ThreadSafeContext) -> usize {
        ctx.lock_state().pending_effects.len()
    }

    #[test]
    fn invalidation_plan_snapshots_frontier_before_apply() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(0usize);
        let left = ctx.computed(move |ctx| ctx.get(&root).wrapping_add(1));
        let right = ctx.computed(move |ctx| ctx.get(&root).wrapping_add(2));
        let joined = ctx.computed(move |ctx| ctx.get(&left).wrapping_add(ctx.get(&right)));
        let runs = Arc::new(AtomicUsize::new(0));
        let runs_for_effect = Arc::clone(&runs);
        let effect = ctx.effect(move |ctx| {
            runs_for_effect.fetch_add(1, Ordering::SeqCst);
            let _ = ctx.get(&joined);
        });

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(ctx.get(&joined), 3);

        let plan = {
            let state = ctx.lock_state();
            let roots = ThreadSafeContext::dependents_locked(&state, root.id)
                .into_iter()
                .map(|id| ThreadSafeInvalidationRoot {
                    id,
                    force_recompute: true,
                });
            let plan = ThreadSafeInvalidationPlan::from_roots_locked(&state, roots);
            let planned_slots: HashSet<SlotId> = plan.slot_marks.iter().map(|(id, _)| id).collect();
            let expected_slots = [left.id, right.id, joined.id]
                .into_iter()
                .collect::<HashSet<_>>();

            assert_eq!(planned_slots, expected_slots);
            assert_eq!(
                plan.slot_marks
                    .iter()
                    .find(|(sid, _)| *sid == left.id)
                    .map(|(_, f)| *f),
                Some(true)
            );
            assert_eq!(
                plan.slot_marks
                    .iter()
                    .find(|(sid, _)| *sid == right.id)
                    .map(|(_, f)| *f),
                Some(true)
            );
            assert_eq!(
                plan.slot_marks
                    .iter()
                    .find(|(sid, _)| *sid == joined.id)
                    .map(|(_, f)| *f),
                Some(false)
            );
            assert_eq!(
                plan.effect_schedules
                    .iter()
                    .find(|(sid, _)| *sid == effect.id)
                    .map(|(_, f)| *f),
                Some(false)
            );
            match state.get_node(joined.id) {
                Some(ThreadSafeNode::Computed(slot)) => {
                    assert!(!slot.dirty);
                    assert!(!slot.force_recompute);
                }
                _ => panic!("joined should be a slot"),
            }
            plan
        };

        assert_eq!(slot_dirty_force(&ctx, &joined), (false, false));
        assert!(!effect_is_scheduled(&ctx, &effect));

        {
            let mut state = ctx.lock_state();
            plan.apply_locked(&mut state);
        }

        assert_eq!(slot_dirty_force(&ctx, &left), (true, true));
        assert_eq!(slot_dirty_force(&ctx, &right), (true, true));
        assert_eq!(slot_dirty_force(&ctx, &joined), (true, false));
        assert!(effect_is_scheduled(&ctx, &effect));
    }

    #[test]
    fn invalidation_plan_snapshots_hard_clears_before_apply() {
        let ctx = ThreadSafeContext::new();
        let root = ctx.source(1usize);
        let doubled = ctx.computed(move |ctx| ctx.get(&root).wrapping_mul(2));
        let labeled = ctx.computed(move |ctx| ctx.get(&doubled).wrapping_add(1));
        let runs = Arc::new(AtomicUsize::new(0));
        let runs_for_effect = Arc::clone(&runs);
        let effect = ctx.effect(move |ctx| {
            runs_for_effect.fetch_add(1, Ordering::SeqCst);
            let _ = ctx.get(&labeled);
        });

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(ctx.get(&labeled), 3);
        assert!(ctx.is_set(&doubled));
        assert!(ctx.is_set(&labeled));

        let plan = {
            let state = ctx.lock_state();
            let roots = ThreadSafeContext::dependents_locked(&state, root.id);
            let plan = ThreadSafeInvalidationPlan::from_clear_roots_locked(&state, roots);
            let planned_clears: HashSet<SlotId> = plan.slot_clears.iter().collect();
            let expected_clears = [doubled.id, labeled.id].into_iter().collect::<HashSet<_>>();

            assert_eq!(planned_clears, expected_clears);
            assert_eq!(
                plan.effect_schedules
                    .iter()
                    .find(|(sid, _)| *sid == effect.id)
                    .map(|(_, f)| *f),
                Some(true)
            );
            match state.get_node(labeled.id) {
                Some(ThreadSafeNode::Computed(slot)) => {
                    assert!(slot.value.is_some());
                    assert!(!slot.dirty);
                }
                _ => panic!("labeled should be a slot"),
            }
            plan
        };

        assert!(ctx.is_set(&doubled));
        assert!(ctx.is_set(&labeled));
        assert!(!effect_is_scheduled(&ctx, &effect));

        {
            let mut state = ctx.lock_state();
            plan.apply_locked(&mut state);
        }

        assert!(!ctx.is_set(&doubled));
        assert!(!ctx.is_set(&labeled));
        assert!(effect_is_scheduled(&ctx, &effect));
    }

    #[test]
    fn batched_cell_invalidations_mark_shared_dependent_once() {
        let ctx = ThreadSafeContext::new();
        let cells = [
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
            ctx.source(0usize),
        ];
        let total = ctx.computed(move |ctx| {
            cells
                .iter()
                .fold(0usize, |sum, cell| sum.wrapping_add(ctx.get(cell)))
        });

        assert_eq!(ctx.get(&total), 0);
        assert_eq!(slot_revision(&ctx, &total), 0);

        ctx.batch(|ctx| {
            for (offset, cell) in cells.iter().enumerate() {
                ctx.set(cell, offset + 1);
            }
        });

        assert_eq!(
            slot_revision(&ctx, &total),
            1,
            "one batch should apply one coalesced dirty/revision mark to the shared frontier"
        );
        assert_eq!(slot_dirty_force(&ctx, &total), (true, true));
        assert_eq!(ctx.get(&total), 10);
        assert_eq!(
            slot_revision(&ctx, &total),
            1,
            "recompute should publish the new value without additional invalidation marks"
        );
    }

    #[test]
    fn batched_cell_invalidations_schedule_shared_effect_once() {
        let ctx = ThreadSafeContext::new();
        let left = ctx.source(0usize);
        let right = ctx.source(0usize);
        let runs = Arc::new(AtomicUsize::new(0));
        let runs_for_effect = Arc::clone(&runs);
        let effect = ctx.effect(move |ctx| {
            runs_for_effect.fetch_add(1, Ordering::SeqCst);
            let _ = ctx.get(&left);
            let _ = ctx.get(&right);
        });

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(cell_dependents_len(&ctx, &left), 1);
        assert_eq!(cell_dependents_len(&ctx, &right), 1);

        ctx.batch(|ctx| {
            ctx.set(&left, 1);
            ctx.set(&right, 1);
        });

        assert_eq!(runs.load(Ordering::SeqCst), 2);
        assert_eq!(pending_effect_count(&ctx), 0);
        assert!(!effect_is_scheduled(&ctx, &effect));
    }
}
