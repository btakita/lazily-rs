//! Async-graph boundary adapter around [`AsyncIngressCell`].
//!
//! Admission stays synchronous; only the external transport Effect is async.

use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::async_context::{AsyncComputed, AsyncContext, AsyncSource};
use crate::{
    AsyncIngressCell, AsyncSourceMap, BoundaryFreshness, BoundaryIngressAction,
    BoundaryIngressAuthority, BoundaryIngressConfig, BoundaryIngressCore,
    BoundaryIngressEffectIntent, BoundaryIngressEvent, BoundaryIngressProjection,
    BoundaryIngressReadiness, BoundaryIngressSnapshot, BoundaryValidation, IngressConfigError,
    IngressPolicy, IngressTransportKind, MergePolicy,
};

struct Inner<K, T, M> {
    core: Mutex<BoundaryIngressCore<K, T>>,
    ingress: AsyncIngressCell<K, T, M>,
    projection: AsyncSource<BoundaryIngressProjection<K>>,
    members: AsyncSourceMap<String, bool>,
    readiness: AsyncComputed<BoundaryIngressReadiness>,
    authority: AsyncComputed<Option<BoundaryIngressAuthority>>,
    freshness: AsyncComputed<BoundaryFreshness>,
    membership: AsyncComputed<Vec<String>>,
    delivery_converged: AsyncComputed<bool>,
    validation: AsyncComputed<BoundaryValidation>,
    effect_intent: AsyncComputed<BoundaryIngressEffectIntent>,
}

/// Event-channel-first adapter for the AsyncContext IngressCell flavor.
pub struct AsyncBoundaryIngressCell<K, T, M> {
    inner: Arc<Inner<K, T, M>>,
}

impl<K, T, M> Clone for AsyncBoundaryIngressCell<K, T, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, T, M> AsyncBoundaryIngressCell<K, T, M>
where
    K: Ord + Eq + Hash + Clone + Send + Sync + 'static,
    T: PartialEq + Clone + Send + Sync + 'static,
    M: MergePolicy<T> + Send + Sync + 'static,
{
    pub fn new(
        ctx: &AsyncContext,
        boundary: BoundaryIngressConfig,
        ingress: IngressPolicy,
    ) -> Result<Self, IngressConfigError> {
        let core = BoundaryIngressCore::<K, T>::new(boundary);
        let projection = ctx.source(core.projection());
        let members = AsyncSourceMap::new(ctx);
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
            AsyncIngressCell::new(ctx, ingress, IngressTransportKind::EventChannel, 1)?;
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

    fn settle(&self, ctx: &AsyncContext, actions: Vec<BoundaryIngressAction<K, T>>, changed: bool) {
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

    pub fn subscribe(&self, ctx: &AsyncContext, generation: u64) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .subscribe(generation);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn apply_snapshot(&self, ctx: &AsyncContext, snapshot: BoundaryIngressSnapshot<K, T>) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .apply_snapshot(snapshot);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn apply_event(&self, ctx: &AsyncContext, event: BoundaryIngressEvent<K, T>) {
        let transition = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .apply_event(event);
        self.settle(ctx, transition.actions, transition.changed);
    }

    pub fn member_join(&self, ctx: &AsyncContext, member: impl Into<String>) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .member_join(member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn member_leave(&self, ctx: &AsyncContext, member: &str) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .member_leave(member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn open_delivery(&self, ctx: &AsyncContext, receipt_id: impl Into<String>) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .open_delivery(receipt_id);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn acknowledge(&self, ctx: &AsyncContext, receipt_id: &str, member: &str) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .acknowledge(receipt_id, member);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn tick(&self, ctx: &AsyncContext, now: u64) {
        let changed = self
            .inner
            .core
            .lock()
            .expect("boundary ingress core")
            .tick(now);
        self.settle(ctx, Vec::new(), changed);
    }

    pub fn ingress(&self) -> &AsyncIngressCell<K, T, M> {
        &self.inner.ingress
    }

    pub fn projection(&self, ctx: &AsyncContext) -> BoundaryIngressProjection<K> {
        ctx.get(&self.inner.projection)
    }

    pub fn readiness(&self, ctx: &AsyncContext) -> BoundaryIngressReadiness {
        ctx.get(&self.inner.readiness)
            .expect("sync compute resolves inline")
    }

    pub fn authority(&self, ctx: &AsyncContext) -> Option<BoundaryIngressAuthority> {
        ctx.get(&self.inner.authority)
            .expect("sync compute resolves inline")
    }

    pub fn freshness(&self, ctx: &AsyncContext) -> BoundaryFreshness {
        ctx.get(&self.inner.freshness)
            .expect("sync compute resolves inline")
    }

    pub fn membership(&self, ctx: &AsyncContext) -> Vec<String> {
        ctx.get(&self.inner.membership)
            .expect("sync compute resolves inline")
    }

    pub fn delivery_converged(&self, ctx: &AsyncContext) -> bool {
        ctx.get(&self.inner.delivery_converged)
            .expect("sync compute resolves inline")
    }

    pub fn validation(&self, ctx: &AsyncContext) -> BoundaryValidation {
        ctx.get(&self.inner.validation)
            .expect("sync compute resolves inline")
    }

    pub fn effect_intent(&self, ctx: &AsyncContext) -> BoundaryIngressEffectIntent {
        ctx.get(&self.inner.effect_intent)
            .expect("sync compute resolves inline")
    }

    pub fn readiness_handle(&self) -> AsyncComputed<BoundaryIngressReadiness> {
        self.inner.readiness
    }

    pub fn authority_handle(&self) -> AsyncComputed<Option<BoundaryIngressAuthority>> {
        self.inner.authority
    }

    pub fn freshness_handle(&self) -> AsyncComputed<BoundaryFreshness> {
        self.inner.freshness
    }

    pub fn membership_handle(&self) -> AsyncComputed<Vec<String>> {
        self.inner.membership
    }

    pub fn delivery_converged_handle(&self) -> AsyncComputed<bool> {
        self.inner.delivery_converged
    }

    pub fn validation_handle(&self) -> AsyncComputed<BoundaryValidation> {
        self.inner.validation
    }

    pub fn effect_intent_handle(&self) -> AsyncComputed<BoundaryIngressEffectIntent> {
        self.inner.effect_intent
    }
}
