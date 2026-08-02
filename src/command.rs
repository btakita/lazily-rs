//! Command / RPC message plane (`command-plane-v1`).
//!
//! An evented command message family that is an additive sibling to
//! `Snapshot` / `Delta` / `CrdtSync`. Editor and runtime integrations submit
//! commands ([`CommandSubmit`]), observe progress ([`CommandEvents`]), preempt
//! ([`CommandCancel`]), and resync after reconnect ([`CommandProjection`]).
//!
//! The one hard rule: **terminal authority is the causal receipt**, not the
//! event or the transport. `observed` / `accepted` / `started` events are
//! non-terminal progress; a command becomes terminal only when a terminal
//! [`CausalReceipt`](crate::receipt::CausalReceipt) folds in. The RPC facade
//! ([`CommandRpcClient`]) is derived behavior over the [`CommandProjection`]
//! reducer: a unary `call` resolves only on a terminal projection, never on an
//! ACK or an `accepted` event.
//!
//! Framing helpers are transport-agnostic; Unix-socket / WebSocket I/O stays
//! outside this module and is provided through the [`CommandTransport`] trait.

use std::collections::{BTreeMap, BTreeSet};

use crate::ipc::IpcValue;
use crate::receipt::{CausalReceipt, ReceiptOutcome};

/// How the admitter collapses concurrent / duplicate submits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum DedupePolicy {
    /// No dedupe.
    None,
    /// Collapse submits sharing an idempotency key.
    SameIdempotencyKey,
    /// Collapse submits sharing a command id.
    SameCommandId,
}

/// Admission policy for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandPolicy {
    /// Dedupe strategy.
    pub dedupe: DedupePolicy,
    /// Whether a newer submit with the same idempotency key supersedes an older
    /// non-terminal command.
    pub supersede: bool,
    /// Whether the admitter may cancel a non-terminal command on preemption.
    pub cancel_on_preempt: bool,
}

/// A command submission. lazily owns this envelope; the `namespace` owns the
/// `payload` body, which lazily never interprets.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandSubmit {
    /// Stable, replay-safe command id.
    pub command_id: String,
    /// Causal parent id (a command or event id); self-caused submits set this
    /// equal to `command_id`.
    pub causation_id: String,
    /// Submitter identity (e.g. `vscode-plugin`).
    pub source: String,
    /// Intended handler identity (e.g. `project-controller`).
    pub target: String,
    /// Domain namespace owning the payload schema (e.g. `agent-doc`).
    pub namespace: String,
    /// Command name within the namespace (e.g. `editor_route`).
    pub name: String,
    /// Authority/controller generation the command targets.
    pub authority_generation: u64,
    /// Dedupe / supersede key.
    pub idempotency_key: String,
    /// Deadline in milliseconds; `0` means no deadline.
    pub deadline_ms: u64,
    /// Admission policy.
    pub policy: CommandPolicy,
    /// Fully-qualified domain payload type (e.g. `agent-doc.editor_route.v1`).
    pub payload_type: String,
    /// Content hash of the payload body (e.g. `sha256:…`).
    pub payload_hash: String,
    /// Domain payload as inline bytes or a shared-memory blob reference.
    pub payload: IpcValue,
    /// Features the target must advertise or the submit fails closed.
    pub required_features: Vec<String>,
}

/// A cancel/preempt request for a still-non-terminal command.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandCancel {
    /// Command to cancel.
    pub command_id: String,
    /// Id of the cancel request itself (for its own receipt/replay).
    pub causation_id: String,
    /// Requester identity.
    pub source: String,
    /// Generation the cancel targets; a stale-generation cancel is ignored.
    pub authority_generation: u64,
    /// Optional cancel reason.
    pub reason: Option<String>,
}

/// Progress/detail event kind. UX and diagnostics only — never terminal proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum CommandEventKind {
    /// The target observed the command.
    Observed,
    /// The command was accepted/queued.
    Accepted,
    /// Execution started.
    Started,
    /// Mid-execution progress detail.
    Progress,
    /// A cancel was surfaced (terminal authority is a matching rejected receipt).
    Cancelled,
    /// The command was superseded by a newer submit.
    Superseded,
    /// The command deadline elapsed.
    TimedOut,
}

/// One progress/detail event keyed by `command_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandEvent {
    /// Idempotency key for this event.
    pub event_id: String,
    /// Command this event describes.
    pub command_id: String,
    /// Event kind.
    pub kind: CommandEventKind,
    /// Authority generation; events outside the current generation are ignored.
    pub generation: u64,
    /// Optional human/diagnostics detail. Not proof of effect.
    pub detail: Option<String>,
}

/// A batch of command events.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandEvents {
    /// Events in this batch.
    pub events: Vec<CommandEvent>,
}

/// Folded projection status for one command. `submitted` / `accepted` /
/// `running` are non-terminal; the rest are terminal and backed by a terminal
/// causal receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum CommandStatus {
    /// Submitted, not yet acknowledged.
    Submitted,
    /// Accepted/queued by the target.
    Accepted,
    /// Execution started.
    Running,
    /// Terminal: applied.
    Applied,
    /// Terminal: rejected.
    Rejected,
    /// Terminal: cancelled (rejected receipt, reason `cancelled`).
    Cancelled,
    /// Terminal: superseded.
    Superseded,
    /// Terminal: timed out.
    TimedOut,
}

impl CommandStatus {
    /// Whether this status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Applied | Self::Rejected | Self::Cancelled | Self::Superseded | Self::TimedOut
        )
    }
}

/// One command's folded projection entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandProjectionEntry {
    /// Command id.
    pub command_id: String,
    /// Folded status.
    pub status: CommandStatus,
    /// True iff a terminal causal receipt has folded in.
    pub terminal: bool,
    /// Current authority generation for the command.
    pub generation: u64,
    /// Terminal reason, or `null` while non-terminal / applied without reason.
    pub reason: Option<String>,
    /// Receipt id that made the command terminal, or `null`.
    pub terminal_receipt_id: Option<String>,
    /// Last folded event id, or `null`.
    pub last_event_id: Option<String>,
}

/// A queryable image of command state; also the reconnect resync frame.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommandProjectionImage {
    /// Authority generation this image was taken at.
    pub generation: u64,
    /// Command entries, ordered by command id.
    pub commands: Vec<CommandProjectionEntry>,
}

/// Externally-tagged command-plane wire message. Sibling to `IpcMessage` /
/// `ReceiptMessage`, not a new state-plane variant.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CommandMessage {
    /// Submit a command. Boxed because the submit envelope is far larger than
    /// the other variants.
    CommandSubmit(Box<CommandSubmit>),
    /// Cancel a command.
    CommandCancel(CommandCancel),
    /// A batch of progress events.
    CommandEvents(CommandEvents),
    /// A projection resync image.
    CommandProjection(CommandProjectionImage),
}

/// Result of folding one command-plane input into a [`CommandProjection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandApplyStatus {
    /// The input updated the projection.
    Recorded,
    /// The input id (command/event/receipt) was already seen; no change.
    Duplicate,
    /// The input targets a command not present in the projection; no change.
    Unknown,
    /// The input's generation does not match the command's current generation.
    StaleGeneration {
        /// Command's current generation.
        expected: u64,
        /// Generation carried by the input.
        actual: u64,
    },
    /// A different terminal outcome already exists for this command; fail closed.
    TerminalConflict {
        /// Command id with conflicting terminal outcomes.
        command_id: String,
        /// Existing terminal status.
        existing: CommandStatus,
        /// Incoming conflicting terminal status.
        incoming: CommandStatus,
    },
}

/// The folded command projection reducer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandProjection {
    generation: u64,
    entries: BTreeMap<String, CommandProjectionEntry>,
    seen_event_ids: BTreeSet<String>,
    seen_receipt_ids: BTreeSet<String>,
    seen_cancel_ids: BTreeSet<String>,
    conflicts: BTreeSet<String>,
}

/// Map a terminal receipt outcome + reason to a folded [`CommandStatus`].
fn terminal_status_of(outcome: ReceiptOutcome, reason: Option<&str>) -> CommandStatus {
    match outcome {
        ReceiptOutcome::Applied => CommandStatus::Applied,
        ReceiptOutcome::Rejected => match reason {
            Some("cancelled") => CommandStatus::Cancelled,
            Some("superseded") => CommandStatus::Superseded,
            Some("timed_out") => CommandStatus::TimedOut,
            // INTENTIONAL leniency (`#failclosedsweep`). Terminality is decided by
            // `outcome`, a closed enum; `reason` only REFINES an already-terminal
            // rejection. The wire type is an open string — `schemas/receipts.json`
            // types it `OptionalString` and documents it as the "terminal reason
            // (rejection cause, cancel reason, timeout)", so a producer is expected
            // to send free text a consumer has never seen. Folding an unrecognised
            // reason (and a missing one) to `Rejected` keeps the fold total without
            // inventing a status: the command is still terminal, still rejected, and
            // the original string stays on the receipt for the caller to read.
            // Pinned by `an_unknown_rejection_reason_still_folds_to_rejected`.
            _ => CommandStatus::Rejected,
        },
        // Non-terminal outcomes never reach here (guarded by is_terminal).
        ReceiptOutcome::Observed | ReceiptOutcome::Accepted => CommandStatus::Accepted,
    }
}

/// The non-terminal status a progress event advances to, if any.
fn progress_status_of(kind: CommandEventKind) -> Option<CommandStatus> {
    match kind {
        CommandEventKind::Observed | CommandEventKind::Accepted => Some(CommandStatus::Accepted),
        CommandEventKind::Started | CommandEventKind::Progress => Some(CommandStatus::Running),
        // cancelled/superseded/timed_out events are UX only; status change waits
        // for the terminal receipt.
        CommandEventKind::Cancelled | CommandEventKind::Superseded | CommandEventKind::TimedOut => {
            None
        }
    }
}

/// Monotonic phase rank so progress never regresses status.
fn phase_rank(status: CommandStatus) -> u8 {
    match status {
        CommandStatus::Submitted => 0,
        CommandStatus::Accepted => 1,
        CommandStatus::Running => 2,
        _ => 3, // terminal
    }
}

impl CommandProjection {
    /// Create an empty projection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The current authority generation the projection has folded to.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Fold a command-plane message.
    pub fn apply_message(&mut self, message: &CommandMessage) -> CommandApplyStatus {
        match message {
            CommandMessage::CommandSubmit(s) => self.submit(s),
            CommandMessage::CommandCancel(c) => self.cancel(c),
            CommandMessage::CommandEvents(b) => {
                let mut last = CommandApplyStatus::Unknown;
                for event in &b.events {
                    last = self.event(event);
                }
                last
            }
            CommandMessage::CommandProjection(image) => self.apply_projection(image),
        }
    }

    /// Fold a command submission.
    pub fn submit(&mut self, submit: &CommandSubmit) -> CommandApplyStatus {
        if self.entries.contains_key(&submit.command_id) {
            return CommandApplyStatus::Duplicate;
        }
        self.generation = self.generation.max(submit.authority_generation);
        self.entries.insert(
            submit.command_id.clone(),
            CommandProjectionEntry {
                command_id: submit.command_id.clone(),
                status: CommandStatus::Submitted,
                terminal: false,
                generation: submit.authority_generation,
                reason: None,
                terminal_receipt_id: None,
                last_event_id: None,
            },
        );
        CommandApplyStatus::Recorded
    }

    /// Fold a progress event.
    pub fn event(&mut self, event: &CommandEvent) -> CommandApplyStatus {
        if self.seen_event_ids.contains(&event.event_id) {
            return CommandApplyStatus::Duplicate;
        }
        let Some(entry) = self.entries.get_mut(&event.command_id) else {
            return CommandApplyStatus::Unknown;
        };
        if event.generation != entry.generation {
            return CommandApplyStatus::StaleGeneration {
                expected: entry.generation,
                actual: event.generation,
            };
        }
        self.seen_event_ids.insert(event.event_id.clone());
        entry.last_event_id = Some(event.event_id.clone());
        if !entry.terminal
            && let Some(next) = progress_status_of(event.kind)
            && phase_rank(next) >= phase_rank(entry.status)
        {
            entry.status = next;
        }
        CommandApplyStatus::Recorded
    }

    /// Fold a cancel request. A cancel is non-terminal by itself; the terminal
    /// outcome folds through the matching rejected receipt.
    pub fn cancel(&mut self, cancel: &CommandCancel) -> CommandApplyStatus {
        if self.seen_cancel_ids.contains(&cancel.causation_id) {
            return CommandApplyStatus::Duplicate;
        }
        let Some(entry) = self.entries.get(&cancel.command_id) else {
            return CommandApplyStatus::Unknown;
        };
        if cancel.authority_generation != entry.generation {
            return CommandApplyStatus::StaleGeneration {
                expected: entry.generation,
                actual: cancel.authority_generation,
            };
        }
        self.seen_cancel_ids.insert(cancel.causation_id.clone());
        // A cancel after a terminal outcome is ignored (recorded but no change).
        CommandApplyStatus::Recorded
    }

    /// Fold a causal receipt (terminal authority) keyed by `causation_id` ==
    /// `command_id`.
    pub fn observe_receipt(&mut self, receipt: &CausalReceipt) -> CommandApplyStatus {
        if self.seen_receipt_ids.contains(&receipt.receipt_id) {
            return CommandApplyStatus::Duplicate;
        }
        let Some(entry) = self.entries.get(&receipt.causation_id) else {
            return CommandApplyStatus::Unknown;
        };
        if receipt.generation != entry.generation {
            return CommandApplyStatus::StaleGeneration {
                expected: entry.generation,
                actual: receipt.generation,
            };
        }
        if !receipt.outcome.is_terminal() {
            // Non-terminal receipt: record id, advance progress, keep non-terminal.
            self.seen_receipt_ids.insert(receipt.receipt_id.clone());
            let entry = self
                .entries
                .get_mut(&receipt.causation_id)
                .expect("present");
            if !entry.terminal && phase_rank(CommandStatus::Accepted) >= phase_rank(entry.status) {
                entry.status = CommandStatus::Accepted;
            }
            return CommandApplyStatus::Recorded;
        }
        let incoming = terminal_status_of(receipt.outcome, receipt.reason.as_deref());
        if entry.terminal {
            if entry.status == incoming {
                // Idempotent terminal.
                self.seen_receipt_ids.insert(receipt.receipt_id.clone());
                return CommandApplyStatus::Recorded;
            }
            let existing = entry.status;
            self.conflicts.insert(receipt.causation_id.clone());
            return CommandApplyStatus::TerminalConflict {
                command_id: receipt.causation_id.clone(),
                existing,
                incoming,
            };
        }
        self.seen_receipt_ids.insert(receipt.receipt_id.clone());
        let entry = self
            .entries
            .get_mut(&receipt.causation_id)
            .expect("present");
        entry.terminal = true;
        entry.status = incoming;
        entry.reason = receipt.reason.clone();
        entry.terminal_receipt_id = Some(receipt.receipt_id.clone());
        CommandApplyStatus::Recorded
    }

    /// Fold a projection resync image (reconnect / handoff).
    pub fn apply_projection(&mut self, image: &CommandProjectionImage) -> CommandApplyStatus {
        self.generation = self.generation.max(image.generation);
        for entry in &image.commands {
            self.entries.insert(entry.command_id.clone(), entry.clone());
            if let Some(ev) = &entry.last_event_id {
                self.seen_event_ids.insert(ev.clone());
            }
            if let Some(rc) = &entry.terminal_receipt_id {
                self.seen_receipt_ids.insert(rc.clone());
            }
        }
        CommandApplyStatus::Recorded
    }

    /// Look up a command's entry.
    #[must_use]
    pub fn entry(&self, command_id: &str) -> Option<&CommandProjectionEntry> {
        self.entries.get(command_id)
    }

    /// The terminal entry for a command, if it has reached a terminal outcome.
    #[must_use]
    pub fn terminal_for(&self, command_id: &str) -> Option<&CommandProjectionEntry> {
        self.entries.get(command_id).filter(|e| e.terminal)
    }

    /// Whether a command has recorded a terminal conflict (fail-closed).
    #[must_use]
    pub fn has_conflict(&self, command_id: &str) -> bool {
        self.conflicts.contains(command_id)
    }

    /// Snapshot the projection as a wire image, ordered by command id.
    #[must_use]
    pub fn to_image(&self) -> CommandProjectionImage {
        CommandProjectionImage {
            generation: self.generation,
            commands: self.entries.values().cloned().collect(),
        }
    }
}

/// Convenience: a terminal `applied` receipt keyed by a command id.
#[must_use]
pub fn applied_receipt(
    receipt_id: impl Into<String>,
    command_id: impl Into<String>,
    observer: impl Into<String>,
    generation: u64,
) -> CausalReceipt {
    CausalReceipt::applied(receipt_id, command_id, observer, generation)
}

/// Convenience: a terminal `rejected` receipt keyed by a command id, with reason.
#[must_use]
pub fn rejected_receipt(
    receipt_id: impl Into<String>,
    command_id: impl Into<String>,
    observer: impl Into<String>,
    generation: u64,
    reason: impl Into<String>,
) -> CausalReceipt {
    CausalReceipt::rejected(receipt_id, command_id, observer, generation).with_reason(reason)
}

/// Transport used by [`CommandRpcClient`] to emit command-plane frames. I/O
/// specifics (Unix socket, WebSocket) live behind this trait, outside the pure
/// type module.
pub trait CommandTransport {
    /// Transport error type.
    type Error;
    /// Send one command-plane message.
    ///
    /// # Errors
    /// Returns the transport's error if the frame could not be enqueued.
    fn send(&mut self, message: &CommandMessage) -> Result<(), Self::Error>;
}

/// Resolution state of an RPC `call`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallState {
    /// The command has not reached a terminal outcome; `call` must keep waiting.
    Pending,
    /// The command reached a terminal outcome; `call` may resolve with it.
    Resolved(CommandProjectionEntry),
    /// The command failed closed on a terminal conflict.
    Conflict,
}

/// RPC facade over the command plane. `submit`/`call` build and send
/// `CommandSubmit`; incoming frames and receipts are folded via `ingest_*`; a
/// unary `call` resolves only when the projection reaches a terminal outcome.
#[derive(Debug)]
pub struct CommandRpcClient<T: CommandTransport> {
    transport: T,
    projection: CommandProjection,
}

impl<T: CommandTransport> CommandRpcClient<T> {
    /// Wrap a transport.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            projection: CommandProjection::new(),
        }
    }

    /// Borrow the folded projection.
    #[must_use]
    pub fn projection(&self) -> &CommandProjection {
        &self.projection
    }

    /// Submit a command: send `CommandSubmit`, fold it locally, return its id.
    ///
    /// # Errors
    /// Returns the transport error if the submit frame could not be sent.
    pub fn submit(&mut self, submit: CommandSubmit) -> Result<String, T::Error> {
        let command_id = submit.command_id.clone();
        let message = CommandMessage::CommandSubmit(Box::new(submit));
        self.transport.send(&message)?;
        self.projection.apply_message(&message);
        Ok(command_id)
    }

    /// Send a `CommandCancel` for a command.
    ///
    /// # Errors
    /// Returns the transport error if the cancel frame could not be sent.
    pub fn cancel(&mut self, cancel: CommandCancel) -> Result<(), T::Error> {
        let message = CommandMessage::CommandCancel(cancel);
        self.transport.send(&message)?;
        self.projection.apply_message(&message);
        Ok(())
    }

    /// Fold an incoming command-plane message (events / projection).
    pub fn ingest_command(&mut self, message: &CommandMessage) -> CommandApplyStatus {
        self.projection.apply_message(message)
    }

    /// Fold an incoming causal receipt (terminal authority).
    pub fn ingest_receipt(&mut self, receipt: &CausalReceipt) -> CommandApplyStatus {
        self.projection.observe_receipt(receipt)
    }

    /// Poll a unary `call`: `Pending` until a terminal causal receipt folds in.
    /// A transport ACK or `accepted`/`queued` event never resolves the call.
    #[must_use]
    pub fn poll_call(&self, command_id: &str) -> CallState {
        if self.projection.has_conflict(command_id) {
            return CallState::Conflict;
        }
        match self.projection.terminal_for(command_id) {
            Some(entry) => CallState::Resolved(entry.clone()),
            None => CallState::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submit_fixture(command_id: &str, generation: u64) -> CommandSubmit {
        CommandSubmit {
            command_id: command_id.to_string(),
            causation_id: command_id.to_string(),
            source: "vscode-plugin".to_string(),
            target: "project-controller".to_string(),
            namespace: "agent-doc".to_string(),
            name: "editor_route".to_string(),
            authority_generation: generation,
            idempotency_key: "project-root:plan.md:run".to_string(),
            deadline_ms: 120_000,
            policy: CommandPolicy {
                dedupe: DedupePolicy::SameIdempotencyKey,
                supersede: false,
                cancel_on_preempt: true,
            },
            payload_type: "agent-doc.editor_route.v1".to_string(),
            payload_hash: "sha256:deadbeef".to_string(),
            payload: IpcValue::Inline(vec![1, 2, 3]),
            required_features: vec!["causal-receipts".to_string()],
        }
    }

    #[test]
    fn command_status_terminality_is_explicit() {
        assert!(!CommandStatus::Submitted.is_terminal());
        assert!(!CommandStatus::Accepted.is_terminal());
        assert!(!CommandStatus::Running.is_terminal());
        assert!(CommandStatus::Applied.is_terminal());
        assert!(CommandStatus::Cancelled.is_terminal());
        assert!(CommandStatus::TimedOut.is_terminal());
    }

    #[test]
    fn accepted_progress_is_not_terminal() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        p.event(&CommandEvent {
            event_id: "ev-1".into(),
            command_id: "cmd-1".into(),
            kind: CommandEventKind::Accepted,
            generation: 42,
            detail: Some("queued".into()),
        });
        let entry = p.entry("cmd-1").unwrap();
        assert!(!entry.terminal);
        assert_eq!(entry.status, CommandStatus::Accepted);
        assert!(p.terminal_for("cmd-1").is_none());
    }

    #[test]
    fn applied_receipt_makes_command_terminal() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        assert_eq!(
            p.observe_receipt(&applied_receipt(
                "rcpt-1",
                "cmd-1",
                "project-controller",
                42
            )),
            CommandApplyStatus::Recorded
        );
        let entry = p.terminal_for("cmd-1").unwrap();
        assert_eq!(entry.status, CommandStatus::Applied);
        assert_eq!(entry.terminal_receipt_id.as_deref(), Some("rcpt-1"));
    }

    /// Pins the INTENTIONAL leniency in `terminal_status_of`: `reason` is an open
    /// wire string, so a value outside the three protocol-significant ones folds
    /// to `Rejected` — terminal, rejected, reason preserved on the receipt — while
    /// the three known reasons still refine the status.
    #[test]
    fn an_unknown_rejection_reason_still_folds_to_rejected() {
        for (reason, expected) in [
            ("cancelled", CommandStatus::Cancelled),
            ("superseded", CommandStatus::Superseded),
            ("timed_out", CommandStatus::TimedOut),
            ("disk_on_fire", CommandStatus::Rejected),
            ("", CommandStatus::Rejected),
        ] {
            let mut p = CommandProjection::new();
            p.submit(&submit_fixture("cmd-1", 42));
            assert_eq!(
                p.observe_receipt(&rejected_receipt(
                    "rcpt-1",
                    "cmd-1",
                    "project-controller",
                    42,
                    reason,
                )),
                CommandApplyStatus::Recorded
            );
            let entry = p
                .terminal_for("cmd-1")
                .expect("a rejection is terminal whatever its reason");
            assert_eq!(
                entry.status, expected,
                "reason `{reason}` folds to {expected:?}"
            );
            assert!(entry.terminal, "reason `{reason}` is still terminal");
            assert_eq!(
                entry.reason.as_deref(),
                Some(reason),
                "the original reason string survives the fold"
            );
        }
    }

    #[test]
    fn stale_generation_receipt_is_ignored() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        assert_eq!(
            p.observe_receipt(&applied_receipt(
                "rcpt-old",
                "cmd-1",
                "project-controller",
                41
            )),
            CommandApplyStatus::StaleGeneration {
                expected: 42,
                actual: 41
            }
        );
        assert!(p.terminal_for("cmd-1").is_none());
    }

    #[test]
    fn duplicate_submit_is_idempotent() {
        let mut p = CommandProjection::new();
        assert_eq!(
            p.submit(&submit_fixture("cmd-1", 42)),
            CommandApplyStatus::Recorded
        );
        assert_eq!(
            p.submit(&submit_fixture("cmd-1", 99)),
            CommandApplyStatus::Duplicate
        );
        assert_eq!(p.entry("cmd-1").unwrap().generation, 42);
    }

    #[test]
    fn cancel_before_terminal_then_rejected_receipt_is_cancelled() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        p.event(&CommandEvent {
            event_id: "ev-1".into(),
            command_id: "cmd-1".into(),
            kind: CommandEventKind::Accepted,
            generation: 42,
            detail: None,
        });
        p.cancel(&CommandCancel {
            command_id: "cmd-1".into(),
            causation_id: "cancel-1".into(),
            source: "vscode-plugin".into(),
            authority_generation: 42,
            reason: Some("operator cleared run".into()),
        });
        p.observe_receipt(&rejected_receipt(
            "rcpt-cancel",
            "cmd-1",
            "project-controller",
            42,
            "cancelled",
        ));
        let entry = p.terminal_for("cmd-1").unwrap();
        assert_eq!(entry.status, CommandStatus::Cancelled);
        assert_eq!(entry.reason.as_deref(), Some("cancelled"));
    }

    #[test]
    fn cancel_after_applied_does_not_override() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        p.observe_receipt(&applied_receipt(
            "rcpt-applied",
            "cmd-1",
            "project-controller",
            42,
        ));
        p.cancel(&CommandCancel {
            command_id: "cmd-1".into(),
            causation_id: "cancel-late".into(),
            source: "vscode-plugin".into(),
            authority_generation: 42,
            reason: Some("too late".into()),
        });
        assert_eq!(
            p.terminal_for("cmd-1").unwrap().status,
            CommandStatus::Applied
        );
    }

    #[test]
    fn conflicting_terminal_receipts_fail_closed() {
        let mut p = CommandProjection::new();
        p.submit(&submit_fixture("cmd-1", 42));
        p.observe_receipt(&applied_receipt(
            "rcpt-applied",
            "cmd-1",
            "project-controller",
            42,
        ));
        let status = p.observe_receipt(&rejected_receipt(
            "rcpt-rejected",
            "cmd-1",
            "project-controller",
            42,
            "conflicting terminal",
        ));
        assert!(matches!(
            status,
            CommandApplyStatus::TerminalConflict { .. }
        ));
        assert!(p.has_conflict("cmd-1"));
        // The applied outcome is not overwritten by winner selection.
        assert_eq!(p.entry("cmd-1").unwrap().status, CommandStatus::Applied);
    }

    #[test]
    fn reconnect_projection_is_fold_equivalent() {
        let mut source = CommandProjection::new();
        source.submit(&submit_fixture("cmd-1", 43));
        source.observe_receipt(&applied_receipt(
            "rcpt-1",
            "cmd-1",
            "project-controller",
            43,
        ));
        let image = source.to_image();

        let mut reconnected = CommandProjection::new();
        reconnected.apply_projection(&image);
        assert_eq!(reconnected.to_image(), image);
        assert_eq!(
            reconnected.terminal_for("cmd-1").unwrap().status,
            CommandStatus::Applied
        );
    }

    struct VecTransport {
        sent: Vec<CommandMessage>,
    }

    impl CommandTransport for VecTransport {
        type Error = ();
        fn send(&mut self, message: &CommandMessage) -> Result<(), ()> {
            self.sent.push(message.clone());
            Ok(())
        }
    }

    #[test]
    fn rpc_call_resolves_only_on_terminal_receipt() {
        let mut client = CommandRpcClient::new(VecTransport { sent: Vec::new() });
        let id = client.submit(submit_fixture("cmd-1", 42)).unwrap();

        // accepted / started progress must NOT resolve the call.
        client.ingest_command(&CommandMessage::CommandEvents(CommandEvents {
            events: vec![
                CommandEvent {
                    event_id: "ev-1".into(),
                    command_id: id.clone(),
                    kind: CommandEventKind::Accepted,
                    generation: 42,
                    detail: Some("queued".into()),
                },
                CommandEvent {
                    event_id: "ev-2".into(),
                    command_id: id.clone(),
                    kind: CommandEventKind::Started,
                    generation: 42,
                    detail: None,
                },
            ],
        }));
        assert_eq!(client.poll_call(&id), CallState::Pending);

        // The terminal receipt resolves it.
        client.ingest_receipt(&applied_receipt("rcpt-1", &id, "project-controller", 42));
        match client.poll_call(&id) {
            CallState::Resolved(entry) => assert_eq!(entry.status, CommandStatus::Applied),
            other => panic!("expected Resolved, got {other:?}"),
        }
    }
}
