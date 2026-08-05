//! `Send + Sync` boundary adapter around [`ThreadSafeIngressCell`].

use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::cell::{Computed, Source};
use crate::{
    BoundaryFreshness, BoundaryIngressAction, BoundaryIngressAuthority, BoundaryIngressConfig,
    BoundaryIngressCore, BoundaryIngressEffectIntent, BoundaryIngressEvent,
    BoundaryIngressProjection, BoundaryIngressReadiness, BoundaryIngressSnapshot,
    BoundaryValidation, IngressConfigError, IngressPolicy, IngressTransportKind, MergePolicy,
    ThreadSafeContext, ThreadSafeIngressCell, ThreadSafeSourceMap,
};

struct Inner<K, T, M> {
    core: Mutex<BoundaryIngressCore<K, T>>,
    ingress: ThreadSafeIngressCell<K, T, M>,
    projection: Source<BoundaryIngressProjection<K>>,
    members: ThreadSafeSourceMap<String, bool>,
    readiness: Computed<BoundaryIngressReadiness>,
    authority: Computed<Option<BoundaryIngressAuthority>>,
    freshness: Computed<BoundaryFreshness>,
    membership: Computed<Vec<String>>,
    delivery_converged: Computed<bool>,
    validation: Computed<BoundaryValidation>,
    effect_intent: Computed<BoundaryIngressEffectIntent>,
}

/// Event-channel-first adapter for the thread-safe IngressCell flavor.
pub struct ThreadSafeBoundaryIngressCell<K, T, M> {
    inner: Arc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for ThreadSafeBoundaryIngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T, M> ThreadSafeBoundaryIngressCell<K, T, M>
where
    K: Ord + Eq + Hash + Clone + Send + Sync + 'static,
    T: PartialEq + Clone + Send + Sync + 'static,
    M: MergePolicy<T> + Send + Sync + 'static,
{
    pub fn new(
        ctx: &ThreadSafeContext,
        boundary: BoundaryIngressConfig,
        ingress: IngressPolicy,
    ) -> Result<Self, IngressConfigError> {
        let core = BoundaryIngressCore::<K, T>::new(boundary);
        let projection = ctx.source(core.projection());
        let members = ThreadSafeSourceMap::new(ctx);
        let readiness = ctx.computed(move |c| c.get(&projection).readiness());
        let authority = ctx.computed(move |c| c.get(&projection).authority());
        let freshness = ctx.computed(move |c| c.get(&projection).freshness);
        let membership = {
            let members = members.clone();
            ctx.computed(move |c| members.keys(c))
        };
        let delivery_converged = ctx.computed(move |c| c.get(&projection).delivery_converged());
        let validation = ctx.computed(move |c| c.get(&projection).validation);
        let effect_intent = ctx.computed(move |c| c.get(&projection).effect_intent());
        let ingress_cell =
            ThreadSafeIngressCell::new(ctx, ingress, IngressTransportKind::EventChannel, 1)?;
        Ok(Self {
            inner: Arc::new(Inner {
                core: Mutex::new(core),
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

    fn settle(
        &self,
        ctx: &ThreadSafeContext,
        actions: Vec<BoundaryIngressAction<K, T>>,
        changed: bool,
    ) {
        if !changed && actions.is_empty() {
            return;
        }
        let projection = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .projection();
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
            ctx.set(&self.inner.projection, projection);
        });
    }

    pub fn subscribe(&self, ctx: &ThreadSafeContext, generation: u64) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .subscribe(generation);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn apply_snapshot(&self, ctx: &ThreadSafeContext, snapshot: BoundaryIngressSnapshot<K, T>) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .apply_snapshot(snapshot);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn apply_event(&self, ctx: &ThreadSafeContext, event: BoundaryIngressEvent<K, T>) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .apply_event(event);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn member_join(&self, ctx: &ThreadSafeContext, member: impl Into<String>) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .member_join(member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn member_leave(&self, ctx: &ThreadSafeContext, member: &str) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .member_leave(member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn open_delivery(&self, ctx: &ThreadSafeContext, receipt_id: impl Into<String>) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .open_delivery(receipt_id);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn acknowledge(&self, ctx: &ThreadSafeContext, receipt_id: &str, member: &str) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .acknowledge(receipt_id, member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn tick(&self, ctx: &ThreadSafeContext, now: u64) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .tick(now);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn ingress(&self) -> &ThreadSafeIngressCell<K, T, M> {
        &self.inner.ingress
    }

    pub fn projection(&self, ctx: &ThreadSafeContext) -> BoundaryIngressProjection<K> {
        ctx.get(&self.inner.projection)
    }

    pub fn readiness(&self, ctx: &ThreadSafeContext) -> BoundaryIngressReadiness {
        ctx.get(&self.inner.readiness)
    }

    pub fn authority(&self, ctx: &ThreadSafeContext) -> Option<BoundaryIngressAuthority> {
        ctx.get(&self.inner.authority)
    }

    pub fn freshness(&self, ctx: &ThreadSafeContext) -> BoundaryFreshness {
        ctx.get(&self.inner.freshness)
    }

    pub fn membership(&self, ctx: &ThreadSafeContext) -> Vec<String> {
        ctx.get(&self.inner.membership)
    }

    pub fn delivery_converged(&self, ctx: &ThreadSafeContext) -> bool {
        ctx.get(&self.inner.delivery_converged)
    }

    pub fn validation(&self, ctx: &ThreadSafeContext) -> BoundaryValidation {
        ctx.get(&self.inner.validation)
    }

    pub fn effect_intent(&self, ctx: &ThreadSafeContext) -> BoundaryIngressEffectIntent {
        ctx.get(&self.inner.effect_intent)
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
