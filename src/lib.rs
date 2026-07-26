//! Lazy reactive primitives with dependency tracking and cache invalidation.
//!
//! # Threading contract
//!
//! [`Context`] is intentionally single-threaded. It owns `RefCell` graph state
//! and non-`Send` callbacks, so sharing a live context across OS threads is
//! rejected by the type system. Create independent contexts per thread today;
//! use [`ThreadSafeContext`] when a single reactive graph must be shared across
//! threads.
//!
//! ```compile_fail
//! use lazily::Context;
//!
//! let ctx = Context::new();
//! let slot = ctx.computed(|_| 1);
//!
//! std::thread::spawn(move || ctx.get(&slot));
//! ```
//!
//! # Async contract
//!
//! [`ThreadSafeContext`] can be used from async runtimes, but slot and effect
//! callbacks are still synchronous. Async computations need a separate API
//! because futures introduce in-flight state, cancellation, stale completion,
//! and dependency tracking across `.await`.
//!
//! ```compile_fail
//! use lazily::ThreadSafeContext;
//!
//! let ctx = ThreadSafeContext::new();
//! let pending = ctx.computed(|_| async { 1usize });
//!
//! // The graph does not await async slot callbacks.
//! let _ = ctx.get(&pending);
//! ```

/// Define a schema marker type for typed lazily contexts.
///
/// The generated type is an uninhabited enum, so it cannot accidentally carry
/// runtime state. Use it as the schema parameter for [`TypedContext`].
///
/// ```
/// lazily::define_schema!(CounterSchema);
/// type CounterContext = lazily::TypedContext<CounterSchema>;
/// ```
#[macro_export]
macro_rules! define_schema {
    ($(#[$attr:meta])* $vis:vis $name:ident) => {
        $(#[$attr])*
        $vis enum $name {}
    };
}

#[cfg(feature = "async")]
#[allow(dead_code)]
mod async_context;
#[cfg(feature = "async")]
mod async_reactive_family;
#[cfg(feature = "webrtc")]
mod bridge;
mod cell;
mod cell_family;
#[cfg(feature = "ipc")]
mod command;
mod context;
pub mod reactive_graph;
mod source_tree;
/// #lzspecedgeindex audit probe (`--cfg audit_probe`): records the
/// `pending_effects` depth seen by `dispose_effect`, so the audit harness can
/// prove it actually exercises the dispose-during-flush path. An earlier
/// harness reported a flat cost column while disposing against an *empty*
/// queue; this exists so that false negative cannot recur silently.
#[cfg(audit_probe)]
pub use context::audit_probe;
#[cfg(feature = "async")]
mod async_queue;
mod coordination;
#[cfg(feature = "distributed")]
mod crdt;
#[cfg(all(feature = "distributed", feature = "webrtc"))]
mod crdt_plane;
#[cfg(feature = "lossless-tree")]
mod crdt_tree;
#[cfg(any(feature = "distributed", feature = "ipc", feature = "signaling-client"))]
mod distributed;
mod effect;
#[cfg(feature = "ffi")]
pub mod ffi;
#[cfg(feature = "instrumentation")]
mod instrumentation;
#[cfg(feature = "ipc")]
mod ipc;
mod keyed_order;
#[cfg(feature = "lossless-tree")]
mod lossless_tree_crdt;
mod membership;
mod merge;
#[cfg(feature = "ipc")]
pub mod outbox;
mod presence;
mod queue;
mod rateshape;
mod receipt;
mod reconcile;
mod relay;
mod relay_policy;
mod relay_roles;
mod relay_transport;
#[cfg(feature = "ipc")]
mod reliable_sync;
mod resilience;
mod sem_tree;
#[cfg(feature = "distributed")]
mod seq_crdt;
mod service;
#[cfg(feature = "signaling-client")]
mod signaling_client;
mod spill;
mod stable_id;
mod state_machine;
pub mod statechart;
#[cfg(feature = "webrtc-str0m")]
mod str0m_backend;
#[cfg(feature = "webrtc-str0m")]
mod str0m_net;
mod text_crdt;
#[cfg(feature = "thread-safe")]
mod thread_safe;
#[cfg(feature = "thread-safe")]
mod thread_safe_queue;
#[cfg(feature = "thread-safe")]
mod thread_safe_reactive_family;
mod time;
#[cfg(feature = "ipc")]
mod transport;
mod typed_context;
#[cfg(all(feature = "signaling-client", feature = "webrtc-str0m"))]
mod webrtc_signaling;
#[cfg(feature = "webrtc")]
mod webrtc_transport;
mod windowing;
mod work_queue;
#[cfg(feature = "websocket")]
mod ws_backend;

#[cfg(feature = "async")]
#[allow(deprecated)]
pub use async_context::{
    AsyncCellHandle, AsyncComputeContext, AsyncComputed, AsyncContext, AsyncContextId,
    AsyncEffectHandle, AsyncSignalHandle, AsyncSlotHandle, AsyncSlotState, AsyncSlotStateView,
    AsyncSource, AsyncTeardownScope,
};
#[cfg(feature = "async")]
pub use async_queue::AsyncQueueCell;
#[cfg(feature = "async")]
#[allow(deprecated)]
pub use async_reactive_family::{AsyncCellMap, AsyncSlotMap};
#[cfg(feature = "async")]
pub use async_reactive_family::{
    AsyncComputedMap, AsyncMapHandle, AsyncReactiveMap, AsyncSourceMap,
};
#[cfg(feature = "webrtc")]
pub use bridge::{BridgeHub, HubError};
pub use cell::{Computed, Source};
#[allow(deprecated)]
pub use cell_family::{CellMap, SlotMap};
pub use cell_family::{ComputedMap, EntryKind, MapHandle, ReactiveMap, SourceMap};
#[cfg(feature = "ipc")]
pub use command::{
    CallState, CommandApplyStatus, CommandCancel, CommandEvent, CommandEventKind, CommandEvents,
    CommandMessage, CommandPolicy, CommandProjection, CommandProjectionEntry,
    CommandProjectionImage, CommandRpcClient, CommandStatus, CommandSubmit, CommandTransport,
    DedupePolicy, applied_receipt, rejected_receipt,
};
pub use context::{
    Compute, ComputeOps, Context, DrainExhaustion, GraphNode, Read, ReadRc, TeardownScope, Write,
};
pub use coordination::{
    BarrierCell, BarrierCore, LeaderCell, LeaderRole, LeaseCell, LeaseCore, LockCell,
    SemaphoreCell, SemaphoreCore,
};
#[cfg(feature = "distributed")]
pub use crdt::{
    CellCrdt, CrdtPlane, Hlc, HlcStamp, LwwRegister, MergeMechanism, MvRegister, OpIdFrontier,
    OpLog, PnCounter, RegisterCrdt, ReplicatedCell, StampFrontier, UnsupportedMechanism,
    VersionVector,
};
#[cfg(all(feature = "distributed", feature = "webrtc"))]
pub use crdt_plane::CrdtPlaneRuntime;
#[cfg(feature = "lossless-tree")]
pub use crdt_tree::CrdtTree;
#[cfg(any(feature = "distributed", feature = "ipc", feature = "signaling-client"))]
pub use distributed::{NodeId, OpKind, PeerId, PeerPermissions, PermissionDenied, RemoteOp};
pub use effect::{Effect, EffectCallbackResult};
#[cfg(feature = "ffi")]
pub use ffi::{
    LazilyFfiBytes, LazilyFfiChannel, LazilyFfiMessageKind, LazilyFfiStatus, lazily_ffi_bytes_free,
    lazily_ffi_channel_free, lazily_ffi_channel_len, lazily_ffi_channel_new,
    lazily_ffi_channel_recv_json, lazily_ffi_channel_send_json, lazily_ffi_ipc_message_clone_json,
    lazily_ffi_ipc_message_kind_json, lazily_ffi_ipc_message_validate_json,
};
#[cfg(all(feature = "ffi", feature = "ipc-binary"))]
pub use ffi::{
    lazily_ffi_channel_recv_binary, lazily_ffi_channel_send_binary,
    lazily_ffi_ipc_message_clone_binary, lazily_ffi_ipc_message_kind_binary,
    lazily_ffi_ipc_message_validate_binary,
};
#[cfg(all(feature = "ffi", feature = "ipc-msgpack"))]
pub use ffi::{
    lazily_ffi_channel_recv_msgpack, lazily_ffi_channel_send_msgpack,
    lazily_ffi_ipc_message_clone_msgpack, lazily_ffi_ipc_message_kind_msgpack,
    lazily_ffi_ipc_message_validate_msgpack,
};
#[cfg(feature = "instrumentation")]
pub use instrumentation::{
    InstrumentationSnapshot, THREAD_SAFE_LOCK_SITE_COUNT, ThreadSafeLockSite,
    ThreadSafeLockSiteSnapshot,
};
#[cfg(any(
    feature = "ffi",
    feature = "webrtc",
    feature = "ipc-binary",
    feature = "ipc-msgpack"
))]
pub use ipc::IpcCodec;
#[cfg(feature = "ipc")]
pub use ipc::{
    BlobBackendKind, CapabilityHandshake, CrdtOp, CrdtSync, Delta, DeltaApplyStatus, DeltaOp,
    DeltaSinceRequest, EdgeSnapshot, IpcMessage, IpcPayload, IpcSink, IpcSource, IpcValue,
    KeyIndex, NODE_KEY_MAX_LEN, NODE_KEY_MAX_SEGMENTS, NodeKey, NodeKeyError, NodeSnapshot,
    NodeState, OutboxAck, PROTOCOL_ID, PROTOCOL_MAJOR_VERSION, ResyncRequest, SHM_BLOB_HEADER_LEN,
    ShmBlobArena, ShmBlobArenaError, ShmBlobRef, Snapshot, WireStamp,
};
#[cfg(all(
    feature = "ipc",
    any(
        feature = "ffi",
        feature = "webrtc",
        feature = "ipc-binary",
        feature = "ipc-msgpack"
    )
))]
pub use ipc::{DecodeError, EncodeError};
pub use lazily_macros::{cell, computed, slot, source};
#[cfg(feature = "lossless-tree")]
pub use lossless_tree_crdt::{
    LeafKind, LosslessTreeCrdt, NodeSeed, TreeError, TreeNodeId, TreeOp, TreeOpId, TreeUpdate,
    TreeVersionFrontier,
};
pub use membership::{
    MembershipCell, MembershipConfig, MembershipCore, PeerChangeEvent, PeerSet, PeerState,
    PhiAccrual,
};
#[cfg(feature = "distributed")]
pub use merge::CrdtJoin;
pub use merge::{KeepLatest, Max, MergePolicy, RawFifo, SetUnion, Sum};
#[cfg(feature = "ipc")]
pub use outbox::{DurableOutbox, InMemoryOutbox, InMemoryStore, OutboxStore};
#[cfg(feature = "durable-sqlite")]
pub use outbox::{SqliteOutbox, SqliteStore, SqliteStoreError, ensure_outbox_schema};
pub use presence::{
    AwarenessCell, Durable, Ephemeral, EphemeralCell, EphemeralCore, EphemeralMapCore,
    EphemeralValue, PresenceCell,
};
pub use queue::{
    QueueCell, QueuePopError, QueuePushError, QueueReaderHandles, QueueStorage, TopicCell,
    TopicDurability, TopicSnapshot, TopicSubscribeOutcome, TopicSubscriptionSnapshot,
    VecDequeStorage,
};
pub use rateshape::{
    DebounceCell, DebounceCore, ExpiryPolicy, Lcg, ProbabilisticSampleCell,
    ProbabilisticSampleCore, RatePolicy, SampleCell, SampleCore, SampleMode, SampleRng,
    ThrottleCell, ThrottleCore, ThrottleEdge, WindowPolicy,
};
pub use reactive_graph::{
    AsyncReactiveGraph, ReactiveGraph, SyncReactiveGraph, Teardown, ThreadSafeReactiveGraph,
};
pub use receipt::{
    CausalReceipt, CausalReceipts, ReceiptApplyStatus, ReceiptMessage, ReceiptOutcome,
    ReceiptProjection,
};
pub use reconcile::{DiffOp, apply_to_map, apply_to_tree, reconcile};
pub use relay::{
    BackpressurePolicy, BoundDim, IngressOutcome, Overflow, RelayCell, RelayConfigError,
};
pub use relay_policy::{KeyedRelay, PriorityStorage};
pub use relay_roles::{Inbox, Outbox};
pub use relay_transport::{FramedTransport, InProcTransport, Transport};
#[cfg(feature = "ipc")]
pub use reliable_sync::{
    Clock, DriverError, OrSet, Progress, ResyncAction, ResyncCoordinator, SnapshotProvider,
    SyncDriver, WireLwwRegister,
};
pub use resilience::{
    BreakerState, BulkheadCell, BulkheadCore, CircuitBreakerCell, CircuitBreakerCore,
    RetryPolicyCell, RetryPolicyCore, TimeoutCell, TimeoutCore,
};
pub use sem_tree::SemTree;
#[cfg(feature = "distributed")]
pub use seq_crdt::{Position, SeqCrdt};
pub use service::{
    DiscoveryCell, DiscoveryCore, Health, HealthCell, HealthCore, ReadinessCell, ReadinessCore,
    RegistryOp, ServiceRegistry, ServiceRegistryCore,
};
#[cfg(feature = "signaling-client")]
pub use signaling_client::{ClientMessage, ServerMessage, SignalingClient, SignalingError};
#[allow(deprecated)]
pub use source_tree::CellTree;
pub use source_tree::SourceTree;
pub use spill::{SpillMode, SpillPage, SpillStore};
pub use stable_id::{
    Alignment, Block, BlockKey, Match, align, assign_stable_keys, block_key, similarity,
};
#[cfg(feature = "async")]
pub use state_machine::AsyncStateMachine;
pub use state_machine::StateMachine;
#[cfg(feature = "thread-safe")]
pub use state_machine::ThreadSafeStateMachine;
#[cfg(feature = "async")]
pub use statechart::AsyncStateChart;
#[cfg(feature = "thread-safe")]
pub use statechart::ThreadSafeStateChart;
pub use statechart::{ChartBuilder, StateBuilder, TransitionBuilder};
pub use statechart::{ChartDef, StateChart};
#[cfg(feature = "webrtc-str0m")]
pub use str0m_backend::{Side, Str0mChannel, Str0mError, Str0mLoopback};
#[cfg(feature = "webrtc-str0m")]
pub use str0m_net::{Str0mNet, Str0mNetChannel, Str0mNetError};
pub use text_crdt::{OpId, TextCrdt, TextOp, TextVersionVector, parse_blocks};
#[cfg(feature = "thread-safe")]
pub use thread_safe::{
    ReadStrategy, ThreadSafeContext, ThreadSafeEffectCallbackResult, ThreadSafeSignalHandle,
    ThreadSafeTeardownScope,
};
#[cfg(feature = "thread-safe")]
pub use thread_safe_queue::ThreadSafeQueueCell;
#[cfg(feature = "thread-safe")]
#[allow(deprecated)]
pub use thread_safe_reactive_family::{ThreadSafeCellMap, ThreadSafeSlotMap};
#[cfg(feature = "thread-safe")]
pub use thread_safe_reactive_family::{
    ThreadSafeComputedMap, ThreadSafeMapHandle, ThreadSafeReactiveMap, ThreadSafeSourceMap,
};
pub use time::{
    CronCell, CronCore, DeadlineCell, DeadlineCore, Deadlined, IntervalCell, IntervalCore,
    ManualClock, TimelineSource, TimerCell, TimerCore,
};
#[cfg(all(unix, feature = "shm"))]
pub use transport::ShmBackend;
#[cfg(feature = "ipc")]
pub use transport::{
    ARROW_DEFAULT_CAPACITY, ArrowBackend, BlobBackend, BlobRouter, IN_PROCESS_DEFAULT_CAPACITY,
    InProcessBackend, resolve_value, spill_message, spill_value,
};
pub use typed_context::{
    TypedCellFactorySource, TypedCellHandle, TypedCellHandleSource, TypedContext,
    TypedContextFamily, TypedContextRef, TypedFactoryContext, TypedGet, TypedGetRef, TypedSet,
    TypedSlotFactorySource, TypedSlotHandle, TypedSlotHandleSource,
};
#[cfg(feature = "thread-safe")]
pub use typed_context::{
    TypedThreadSafeCellHandle, TypedThreadSafeContext, TypedThreadSafeContextRef,
    TypedThreadSafeSlotHandle,
};
#[cfg(all(feature = "signaling-client", feature = "webrtc-str0m"))]
pub use webrtc_signaling::{WebrtcSignalingError, answer_next_offer, offer_to_peer};
#[cfg(feature = "webrtc")]
pub use webrtc_transport::{
    DataChannel, InMemoryDataChannel, WebRtcSink, WebRtcSource, WebRtcTransportError,
};
pub use windowing::{
    SessionCore, SessionWindow, SlidingCore, SlidingWindow, TumblingCountCore, TumblingCountWindow,
    TumblingTimeCore, TumblingTimeWindow,
};
pub use work_queue::{
    WorkQueueCell, WorkQueueDeadLetter, WorkQueueDeadLetterReason, WorkQueueDelivery,
    WorkQueueItem, WorkQueueReaderHandles,
};
#[cfg(feature = "websocket")]
pub use ws_backend::{WsDataChannel, WsError};
