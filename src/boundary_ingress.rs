//! Reactive sync shell for [`BoundaryIngressCore`](crate::BoundaryIngressCore).
//!
//! Every boundary observation updates Sources in one batch. Computeds own all
//! derived reads; callers attach Effects to [`effect_intent`](Self::effect_intent)
//! and publish the resulting receipt facts back through this adapter.

use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

use crate::cell::{Computed, Source};
use crate::{
    BoundaryFreshness, BoundaryIngressAction, BoundaryIngressAuthority, BoundaryIngressConfig,
    BoundaryIngressCore, BoundaryIngressEffectIntent, BoundaryIngressEvent, BoundaryIngressPhase,
    BoundaryIngressProjection, BoundaryIngressReadiness, BoundaryIngressSnapshot,
    BoundaryValidation, Context, IngressCell, IngressConfigError, IngressPolicy,
    IngressTransportKind, MergePolicy, SourceMap,
};

struct Inner<K, T, M> {
    core: RefCell<BoundaryIngressCore<K, T>>,
    ingress: IngressCell<K, T, M>,
    projection: Source<BoundaryIngressProjection<K>>,
    members: SourceMap<String, bool>,
    readiness: Computed<BoundaryIngressReadiness>,
    authority: Computed<Option<BoundaryIngressAuthority>>,
    freshness: Computed<BoundaryFreshness>,
    membership: Computed<Vec<String>>,
    delivery_converged: Computed<bool>,
    validation: Computed<BoundaryValidation>,
    effect_intent: Computed<BoundaryIngressEffectIntent>,
}

/// Event-channel-first adapter around the existing single-threaded IngressCell.
pub struct BoundaryIngressCell<K, T, M> {
    inner: Rc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for BoundaryIngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<K, T, M> BoundaryIngressCell<K, T, M>
where
    K: Ord + Eq + Hash + Clone + 'static,
    T: PartialEq + Clone + 'static,
    M: MergePolicy<T> + 'static,
{
    /// Build a boundary adapter and its existing IngressCell value plane.
    pub fn new(
        ctx: &Context,
        boundary: BoundaryIngressConfig,
        ingress: IngressPolicy,
    ) -> Result<Self, IngressConfigError> {
        let core = BoundaryIngressCore::<K, T>::new(boundary);
        let initial = core.projection();
        let projection = ctx.source(initial);
        let members = SourceMap::new(ctx);
        let readiness = ctx.computed(move |c| projection.get(c).readiness());
        let authority = ctx.computed(move |c| projection.get(c).authority());
        let freshness = ctx.computed(move |c| projection.get(c).freshness);
        let membership = {
            let members = members.clone();
            ctx.computed(move |c| members.keys(c))
        };
        let delivery_converged = ctx.computed(move |c| projection.get(c).delivery_converged());
        let validation = ctx.computed(move |c| projection.get(c).validation);
        let effect_intent = ctx.computed(move |c| projection.get(c).effect_intent());
        let ingress_cell = IngressCell::new(ctx, ingress, IngressTransportKind::EventChannel, 1)?;
        Ok(Self {
            inner: Rc::new(Inner {
                core: RefCell::new(core),
                ingress: ingress_cell,
                projection,
                members,
                readiness,
                authority,
                freshness,
                membership,
                delivery_converged,
                validation,
                effect_intent,
            }),
        })
    }

    fn settle(&self, ctx: &Context, actions: Vec<BoundaryIngressAction<K, T>>, changed: bool) {
        if !changed && actions.is_empty() {
            return;
        }
        let projection = self.inner.core.borrow().projection();
        ctx.batch(|ctx| {
            for action in actions {
                match action {
                    BoundaryIngressAction::Admit(envelope) => {
                        self.inner.ingress.admit(ctx, envelope);
                    }
                    BoundaryIngressAction::Close(key) => self.inner.ingress.close(ctx, &key),
                }
            }
            for member in self.inner.members.present_keys() {
                if !projection.members.contains(&member) {
                    self.inner.members.remove(ctx, &member);
                }
            }
            for member in &projection.members {
                self.inner.members.set(ctx, member.clone(), true);
            }
            self.inner.projection.set(ctx, projection);
        });
    }

    /// Start or hot-restart the subscription generation.
    pub fn subscribe(&self, ctx: &Context, generation: u64) {
        let transition = self.inner.core.borrow_mut().subscribe(generation);
        self.settle(ctx, transition.actions, transition.changed);
    }

    /// Atomically install a covering snapshot and buffered successors.
    pub fn apply_snapshot(&self, ctx: &Context, snapshot: BoundaryIngressSnapshot<K, T>) {
        let transition = self.inner.core.borrow_mut().apply_snapshot(snapshot);
        self.settle(ctx, transition.actions, transition.changed);
    }

    /// Publish one event-channel frame.
    pub fn apply_event(&self, ctx: &Context, event: BoundaryIngressEvent<K, T>) {
        let transition = self.inner.core.borrow_mut().apply_event(event);
        self.settle(ctx, transition.actions, transition.changed);
    }

    /// Publish a member-join Source fact.
    pub fn member_join(&self, ctx: &Context, member: impl Into<String>) {
        let changed = self.inner.core.borrow_mut().member_join(member);
        self.settle(ctx, Vec::new(), changed);
    }

    /// Publish a member-leave Source fact.
    pub fn member_leave(&self, ctx: &Context, member: &str) {
        let changed = self.inner.core.borrow_mut().member_leave(member);
        self.settle(ctx, Vec::new(), changed);
    }

    /// Open one durable delivery receipt.
    pub fn open_delivery(&self, ctx: &Context, receipt_id: impl Into<String>) {
        let changed = self.inner.core.borrow_mut().open_delivery(receipt_id);
        self.settle(ctx, Vec::new(), changed);
    }

    /// Fold one idempotent acknowledgement receipt.
    pub fn acknowledge(&self, ctx: &Context, receipt_id: &str, member: &str) {
        let changed = self.inner.core.borrow_mut().acknowledge(receipt_id, member);
        self.settle(ctx, Vec::new(), changed);
    }

    /// Advance logical time.
    pub fn tick(&self, ctx: &Context, now: u64) {
        let changed = self.inner.core.borrow_mut().tick(now);
        self.settle(ctx, Vec::new(), changed);
    }

    /// Existing IngressCell value plane.
    pub fn ingress(&self) -> &IngressCell<K, T, M> {
        &self.inner.ingress
    }

    /// Atomic Source projection.
    pub fn projection(&self, ctx: &Context) -> BoundaryIngressProjection<K> {
        ctx.get(&self.inner.projection)
    }

    pub fn readiness(&self, ctx: &Context) -> BoundaryIngressReadiness {
        ctx.get(&self.inner.readiness)
    }

    pub fn authority(&self, ctx: &Context) -> Option<BoundaryIngressAuthority> {
        ctx.get(&self.inner.authority)
    }

    pub fn freshness(&self, ctx: &Context) -> BoundaryFreshness {
        ctx.get(&self.inner.freshness)
    }

    pub fn membership(&self, ctx: &Context) -> Vec<String> {
        ctx.get(&self.inner.membership)
    }

    pub fn delivery_converged(&self, ctx: &Context) -> bool {
        ctx.get(&self.inner.delivery_converged)
    }

    pub fn validation(&self, ctx: &Context) -> BoundaryValidation {
        ctx.get(&self.inner.validation)
    }

    pub fn effect_intent(&self, ctx: &Context) -> BoundaryIngressEffectIntent {
        ctx.get(&self.inner.effect_intent)
    }

    pub fn phase(&self, ctx: &Context) -> BoundaryIngressPhase {
        ctx.get(&self.inner.projection).phase
    }

    pub fn readiness_handle(&self) -> Computed<BoundaryIngressReadiness> {
        self.inner.readiness
    }

    pub fn authority_handle(&self) -> Computed<Option<BoundaryIngressAuthority>> {
        self.inner.authority
    }

    pub fn freshness_handle(&self) -> Computed<BoundaryFreshness> {
        self.inner.freshness
    }

    pub fn membership_handle(&self) -> Computed<Vec<String>> {
        self.inner.membership
    }

    pub fn delivery_converged_handle(&self) -> Computed<bool> {
        self.inner.delivery_converged
    }

    pub fn validation_handle(&self) -> Computed<BoundaryValidation> {
        self.inner.validation
    }

    pub fn effect_intent_handle(&self) -> Computed<BoundaryIngressEffectIntent> {
        self.inner.effect_intent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BoundaryIngressPayload, IngressEnvelope, KeepLatest};

    type Cell = BoundaryIngressCell<String, u64, KeepLatest>;

    fn cell(ctx: &Context, max_buffered: usize) -> Cell {
        BoundaryIngressCell::new(
            ctx,
            BoundaryIngressConfig {
                max_buffered,
                freshness_horizon: 10,
            },
            IngressPolicy::default(),
        )
        .expect("boundary ingress")
    }

    fn envelope(
        key: &str,
        generation: u64,
        sequence: u64,
        value: u64,
    ) -> IngressEnvelope<String, u64> {
        IngressEnvelope::new(key.to_string(), generation, sequence, sequence, value)
    }

    #[test]
    fn snapshot_plus_buffered_successor_has_no_torn_observation() {
        let ctx = Context::new();
        let adapter = cell(&ctx, 4);
        adapter.subscribe(&ctx, 1);
        adapter.apply_event(
            &ctx,
            BoundaryIngressEvent {
                generation: 1,
                cursor: 2,
                stamped_at: 2,
                payload: BoundaryIngressPayload::Upsert(envelope("b", 1, 0, 2)),
            },
        );

        let observations = Rc::new(RefCell::new(Vec::new()));
        let observed = Rc::clone(&observations);
        let projection = adapter.inner.projection;
        let membership = adapter.membership_handle();
        let effect = ctx.effect(move |c| {
            observed
                .borrow_mut()
                .push((projection.get(c), c.get(&membership)));
        });
        observations.borrow_mut().clear();

        adapter.apply_snapshot(
            &ctx,
            BoundaryIngressSnapshot {
                generation: 1,
                cursor: 1,
                stamped_at: 1,
                entries: vec![envelope("a", 1, 0, 1)],
                members: vec!["editor-a".to_string()],
                validation: BoundaryValidation::Valid,
            },
        );

        let observations = observations.borrow();
        assert!(!observations.is_empty());
        for (projection, members) in observations.iter() {
            assert_eq!(projection.cursor, Some(2));
            assert_eq!(
                projection.source_keys,
                vec!["a".to_string(), "b".to_string()]
            );
            assert_eq!(members, &vec!["editor-a".to_string()]);
        }
        assert_eq!(adapter.ingress().value(&ctx, &"a".to_string()), Some(1));
        assert_eq!(adapter.ingress().value(&ctx, &"b".to_string()), Some(2));
        effect.dispose(&ctx);
    }

    #[test]
    fn generation_gap_receipts_and_freshness_are_derived() {
        let ctx = Context::new();
        let adapter = cell(&ctx, 1);
        adapter.subscribe(&ctx, 7);
        adapter.apply_snapshot(
            &ctx,
            BoundaryIngressSnapshot {
                generation: 7,
                cursor: 10,
                stamped_at: 10,
                entries: vec![],
                members: vec![],
                validation: BoundaryValidation::Valid,
            },
        );
        assert_eq!(adapter.readiness(&ctx), BoundaryIngressReadiness::Ready);
        assert_eq!(adapter.freshness(&ctx), BoundaryFreshness::Fresh);

        adapter.apply_event(
            &ctx,
            BoundaryIngressEvent {
                generation: 7,
                cursor: 12,
                stamped_at: 12,
                payload: BoundaryIngressPayload::Upsert(envelope("c", 7, 0, 3)),
            },
        );
        assert_eq!(
            adapter.effect_intent(&ctx),
            BoundaryIngressEffectIntent::ColdReplay {
                generation: 7,
                from_cursor: 11,
            }
        );

        adapter.open_delivery(&ctx, "response-1");
        assert!(!adapter.delivery_converged(&ctx));
        adapter.member_join(&ctx, "editor-a");
        adapter.member_leave(&ctx, "editor-a");
        assert!(!adapter.delivery_converged(&ctx));
        adapter.acknowledge(&ctx, "response-1", "editor-a");
        assert!(adapter.delivery_converged(&ctx));
        let revision = adapter.projection(&ctx).revision;
        adapter.acknowledge(&ctx, "response-1", "editor-a");
        assert_eq!(adapter.projection(&ctx).revision, revision);

        adapter.tick(&ctx, 20);
        assert_eq!(adapter.freshness(&ctx), BoundaryFreshness::Fresh);
        adapter.tick(&ctx, 21);
        assert_eq!(adapter.freshness(&ctx), BoundaryFreshness::Stale);
    }
}
