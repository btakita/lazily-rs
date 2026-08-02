//! Full Harel/SCXML state charts — native Rust, conforming to
//! `lazily-spec/docs/state-charts.md`.
//!
//! A chart is **compute, not protocol**: it is never serialized as a distinct
//! wire kind. In this reactive binding the active configuration lives in a
//! [`Source`], so any slot/signal/effect reading [`StateChart::configuration`],
//! [`StateChart::active_leaves`], or [`StateChart::matches`] is invalidated on a
//! real transition; a no-op self-transition is suppressed by the cell's
//! `PartialEq` guard (see the spec's "Self-transitions" section).
//!
//! Implemented subset (per the spec's implementation-status note): compound
//! states, orthogonal (parallel) regions, shallow + deep history, entry/exit/
//! transition actions, named guards, external + internal transitions. Extended
//! state `{"expr": …}` guards and `run` actions are rejected explicitly; `final`
//! states are accepted as leaves without raising completion (`done`) events.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

#[cfg(feature = "thread-safe")]
use crate::ThreadSafeContext;
#[cfg(feature = "async")]
use crate::{AsyncContext, AsyncSource};
use crate::{Context, Source};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Atomic,
    Compound,
    Parallel,
    History(HistoryKind),
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryKind {
    Shallow,
    Deep,
}

#[derive(Debug, Clone)]
struct Transition {
    target: String,
    guard: Option<String>,
    action: Vec<String>,
    internal: bool,
}

#[derive(Debug, Clone)]
struct StateDef {
    parent: Option<String>,
    kind: Kind,
    initial: Option<String>,
    default: Option<String>,
    transitions: HashMap<String, Transition>,
    entry: Vec<String>,
    exit: Vec<String>,
}

/// A parsed, immutable chart definition.
#[derive(Debug, Clone)]
pub struct ChartDef {
    states: HashMap<String, StateDef>,
    children: HashMap<String, Vec<String>>,
    order: HashMap<String, usize>,
    depth: HashMap<String, usize>,
    root: String,
}

/// A history recording for a region exited at least once.
#[derive(Debug, Clone)]
enum Recording {
    /// Direct child of the region that was active.
    Shallow(String),
    /// Full active sub-configuration below the region (leaves + ancestors).
    Deep(BTreeSet<String>),
}

/// A reactive full-Harel state chart backed by a configuration cell.
pub struct StateChart {
    def: ChartDef,
    config: Source<BTreeSet<String>>,
    history: RefCell<HashMap<String, Recording>>,
    last_actions: RefCell<Vec<String>>,
}

impl ChartDef {
    /// Parse a chart definition from a `serde_json::Value` of the declarative
    /// form. Returns an error string for malformed charts or unsupported
    /// features (`run` actions, `{"expr": …}` guards).
    #[cfg(feature = "statechart-json")]
    pub fn from_json(value: &serde_json::Value) -> Result<ChartDef, String> {
        let obj = value
            .as_object()
            .ok_or_else(|| "chart must be a JSON object".to_string())?;
        // Validates `chart.initial` is present; descent uses each compound's
        // own `initial` from the root, so the value itself is not stored.
        let _top_initial = obj
            .get("initial")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "chart.initial is required".to_string())?
            .to_string();

        let states_obj = obj
            .get("states")
            .and_then(|v| v.as_object())
            .ok_or_else(|| "chart.states is required".to_string())?;

        let mut states: HashMap<String, StateDef> = HashMap::new();
        let mut order: HashMap<String, usize> = HashMap::new();
        for (idx, (id, raw)) in states_obj.iter().enumerate() {
            order.insert(id.clone(), idx);
            states.insert(id.clone(), parse_state(id, raw)?);
        }

        Self::from_states(states, order)
    }

    /// Assemble a validated [`ChartDef`] from parsed states plus their document
    /// order. Shared by [`ChartDef::from_json`] and the Rust [`ChartBuilder`], so
    /// both definition paths derive parent→children, the single parent-less root,
    /// and per-node depth identically. `order` maps each state id to the position
    /// that fixes deterministic parallel-region descent.
    fn from_states(
        states: HashMap<String, StateDef>,
        order: HashMap<String, usize>,
    ) -> Result<ChartDef, String> {
        // Derived structure: children, depth, root.
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut root: Option<String> = None;
        for (id, def) in &states {
            match &def.parent {
                Some(p) => children.entry(p.clone()).or_default().push(id.clone()),
                None => {
                    if root.is_some() {
                        return Err("chart has more than one root (parent-less state)".into());
                    }
                    root = Some(id.clone());
                }
            }
        }
        // Sort children by document order for deterministic parallel descent.
        for kids in children.values_mut() {
            kids.sort_by_key(|k| order.get(k).copied().unwrap_or(usize::MAX));
        }
        let root = root.ok_or_else(|| "chart has no root (parent-less state)".to_string())?;

        let mut depth: HashMap<String, usize> = HashMap::new();
        compute_depth(&states, &root, 0, &mut depth);

        Ok(ChartDef {
            states,
            children,
            order,
            depth,
            root,
        })
    }

    fn kind(&self, id: &str) -> Kind {
        self.states.get(id).map(|s| s.kind).unwrap_or(Kind::Atomic)
    }

    fn ancestors_inclusive(&self, id: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut cur = Some(id.to_string());
        while let Some(cid) = cur {
            out.push(cid.clone());
            cur = self.states.get(&cid).and_then(|s| s.parent.clone());
        }
        out
    }

    fn lca(&self, a: &str, b: &str) -> String {
        let anc_a: std::collections::HashSet<String> =
            self.ancestors_inclusive(a).into_iter().collect();
        for cid in self.ancestors_inclusive(b) {
            if anc_a.contains(&cid) {
                return cid;
            }
        }
        self.root.clone()
    }

    fn is_proper_descendant(&self, desc: &str, anc: &str) -> bool {
        desc != anc && self.ancestors_inclusive(desc).iter().any(|x| x == anc)
    }

    fn depth(&self, id: &str) -> usize {
        self.depth.get(id).copied().unwrap_or(0)
    }
}

/// Read an optional string field, rejecting a present-but-non-string value.
///
/// `.and_then(|v| v.as_str())` reads a present `{"parent": 7}` as *absent*, which
/// for `parent` promotes the state to a second root and for `initial` demotes a
/// compound state to atomic. A malformed field is a malformed chart, not an
/// omitted one (`#failclosedsweep`).
#[cfg(feature = "statechart-json")]
fn optional_str(
    id: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>, String> {
    match obj.get(field) {
        None => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(format!(
            "state {id}: `{field}` must be a string, got {other}"
        )),
    }
}

/// Read an optional boolean field, rejecting a present-but-non-boolean value.
///
/// spec `state-charts.md` § Implementation status: a binding "MUST reject any
/// feature it does not implement explicitly (never silently ignore a `parallel`,
/// `history`, `entry/exit/run`, or `{"expr": …}` guard)". `{"parallel": "true"}`
/// read through `as_bool()` is precisely that silent ignore.
#[cfg(feature = "statechart-json")]
fn optional_bool(
    what: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<bool>, String> {
    match obj.get(field) {
        None => Ok(None),
        Some(serde_json::Value::Bool(b)) => Ok(Some(*b)),
        Some(other) => Err(format!("{what}: `{field}` must be a boolean, got {other}")),
    }
}

#[cfg(feature = "statechart-json")]
fn parse_state(id: &str, raw: &serde_json::Value) -> Result<StateDef, String> {
    let obj = raw
        .as_object()
        .ok_or_else(|| format!("state {id} must be an object"))?;
    let parent = optional_str(id, obj, "parent")?;
    let initial = optional_str(id, obj, "initial")?;
    let default = optional_str(id, obj, "default")?;

    if obj.get("run").is_some() {
        return Err(format!(
            "state {id} uses `run` actions, which are not supported (rejecting explicitly per spec)"
        ));
    }

    let history = optional_str(id, obj, "history")?;
    let parallel = optional_bool(&format!("state {id}"), obj, "parallel")?;
    let kind = if let Some(h) = &history {
        Kind::History(match h.as_str() {
            "shallow" => HistoryKind::Shallow,
            "deep" => HistoryKind::Deep,
            other => return Err(format!("state {id}: unknown history kind `{other}`")),
        })
    } else if parallel == Some(true) {
        Kind::Parallel
    } else if obj.contains_key("initial") {
        Kind::Compound
    } else {
        Kind::Atomic
    };

    // `kind` is documented as OPTIONAL and inferred (spec `state-charts.md`:
    // "`kind` is optional and inferred"), over the closed domain
    // `atomic | compound | parallel | history | final`. It was previously read
    // with `matches!(…, Some("final"))`, so every other value — including the
    // `parallel` and `history` the spec forbids ignoring, and outright typos —
    // fell through to the inferred kind with no diagnostic. Now an unknown value
    // is rejected by name, and a known value must agree with what the rest of the
    // state declares, since `final` is the only kind carrying information the
    // other fields do not.
    let kind = match obj.get("kind") {
        None => kind,
        Some(serde_json::Value::String(declared)) => match declared.as_str() {
            // `final` is the one kind nothing else implies, so it is taken at its
            // word — but not over a state that has ALSO declared itself a history
            // pseudo-state or an AND-state, which is a contradiction the reader
            // must not silently resolve in either direction.
            "final" if matches!(kind, Kind::History(_) | Kind::Parallel) => {
                return Err(format!(
                    "state {id}: `kind: final` contradicts the state's own `history`/`parallel` declaration"
                ));
            }
            "final" => Kind::Final,
            "atomic" | "compound" | "parallel" | "history" => {
                let inferred = match kind {
                    Kind::Atomic => "atomic",
                    Kind::Compound => "compound",
                    Kind::Parallel => "parallel",
                    Kind::History(_) => "history",
                    Kind::Final => "final",
                };
                if declared != inferred {
                    return Err(format!(
                        "state {id}: declared kind `{declared}` contradicts the state's own fields (inferred `{inferred}`)"
                    ));
                }
                kind
            }
            other => {
                return Err(format!(
                    "state {id}: unknown kind `{other}` (expected atomic|compound|parallel|history|final)"
                ));
            }
        },
        Some(other) => return Err(format!("state {id}: `kind` must be a string, got {other}")),
    };

    let entry = parse_action_list(obj.get("entry"))?;
    let exit = parse_action_list(obj.get("exit"))?;

    let mut transitions = HashMap::new();
    if let Some(on) = obj.get("on").and_then(|v| v.as_object()) {
        for (event, raw_t) in on {
            transitions.insert(event.clone(), parse_transition(raw_t)?);
        }
    }

    Ok(StateDef {
        parent,
        kind,
        initial,
        default,
        transitions,
        entry,
        exit,
    })
}

#[cfg(feature = "statechart-json")]
fn parse_action_list(raw: Option<&serde_json::Value>) -> Result<Vec<String>, String> {
    match raw {
        None => Ok(Vec::new()),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or_else(|| "action must be a string".into())
            })
            .collect(),
        Some(_) => Err("entry/exit must be an array of strings".into()),
    }
}

#[cfg(feature = "statechart-json")]
fn parse_transition(raw: &serde_json::Value) -> Result<Transition, String> {
    match raw {
        serde_json::Value::String(target) => Ok(Transition {
            target: target.clone(),
            guard: None,
            action: Vec::new(),
            internal: false,
        }),
        serde_json::Value::Object(o) => {
            let target = o
                .get("target")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "transition requires `target`".to_string())?
                .to_string();
            let guard = match o.get("guard") {
                None => None,
                Some(serde_json::Value::String(name)) => Some(name.clone()),
                Some(serde_json::Value::Object(_)) => {
                    return Err(
                        "context-expression `{expr: …}` guards are not supported (rejecting explicitly per spec)"
                            .into(),
                    );
                }
                Some(_) => return Err("guard must be a string".into()),
            };
            let action = parse_action_list(o.get("action"))?;
            // `{"internal": "true"}` used to read as `false` here, silently
            // turning an internal transition into an external one (which exits
            // and re-enters the source state, firing entry/exit actions the chart
            // author asked to suppress).
            let internal = optional_bool(&format!("transition to `{target}`"), o, "internal")?
                .unwrap_or(false);
            Ok(Transition {
                target,
                guard,
                action,
                internal,
            })
        }
        _ => Err("transition must be a string or object".into()),
    }
}

fn compute_depth(
    states: &HashMap<String, StateDef>,
    id: &str,
    current: usize,
    out: &mut HashMap<String, usize>,
) {
    out.insert(id.to_string(), current);
    let next: Vec<String> = states
        .iter()
        .filter(|(_, d)| d.parent.as_deref() == Some(id))
        .map(|(k, _)| k.clone())
        .collect();
    for child in next {
        compute_depth(states, &child, current + 1, out);
    }
}

// ---------------------------------------------------------------------------
// Typed Rust builder — define a `ChartDef` in Rust instead of (or alongside) the
// JSON form. Both paths funnel through `ChartDef::from_states`, so a chart built
// in Rust is byte-for-byte equivalent to the same chart parsed from JSON.
// ---------------------------------------------------------------------------

/// A single transition, built in Rust. Equivalent to a JSON transition object.
pub struct TransitionBuilder {
    inner: Transition,
}

impl TransitionBuilder {
    /// An external transition to `target` with no guard or actions.
    pub fn to(target: impl Into<String>) -> Self {
        Self {
            inner: Transition {
                target: target.into(),
                guard: None,
                action: Vec::new(),
                internal: false,
            },
        }
    }

    /// Attach a named boolean guard (resolved at `send` time; absent → false).
    pub fn guard(mut self, name: impl Into<String>) -> Self {
        self.inner.guard = Some(name.into());
        self
    }

    /// Append a transition action name.
    pub fn action(mut self, name: impl Into<String>) -> Self {
        self.inner.action.push(name.into());
        self
    }

    /// Mark this transition internal (no exit/re-entry of the source subtree
    /// when the target is the source or a proper descendant).
    pub fn internal(mut self) -> Self {
        self.inner.internal = true;
        self
    }
}

/// A single chart state, built in Rust. Mirrors the JSON state object.
pub struct StateBuilder {
    id: String,
    def: StateDef,
}

impl StateBuilder {
    fn with_kind(id: impl Into<String>, kind: Kind, initial: Option<String>) -> Self {
        Self {
            id: id.into(),
            def: StateDef {
                parent: None,
                kind,
                initial,
                default: None,
                transitions: HashMap::new(),
                entry: Vec::new(),
                exit: Vec::new(),
            },
        }
    }

    /// An atomic leaf state.
    pub fn atomic(id: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::Atomic, None)
    }

    /// A compound state with the given initial child.
    pub fn compound(id: impl Into<String>, initial: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::Compound, Some(initial.into()))
    }

    /// A parallel (orthogonal) state; all its child regions are entered together.
    pub fn parallel(id: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::Parallel, None)
    }

    /// A final leaf state (accepted as a leaf; raises no completion event).
    pub fn final_state(id: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::Final, None)
    }

    /// A shallow-history pseudostate for its parent region.
    pub fn history_shallow(id: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::History(HistoryKind::Shallow), None)
    }

    /// A deep-history pseudostate for its parent region.
    pub fn history_deep(id: impl Into<String>) -> Self {
        Self::with_kind(id, Kind::History(HistoryKind::Deep), None)
    }

    /// Set the parent state id. Omit only for the single chart root.
    pub fn parent(mut self, parent: impl Into<String>) -> Self {
        self.def.parent = Some(parent.into());
        self
    }

    /// Set the default target used on a history pseudostate's first entry.
    pub fn default_child(mut self, target: impl Into<String>) -> Self {
        self.def.default = Some(target.into());
        self
    }

    /// Append an entry action name.
    pub fn entry(mut self, action: impl Into<String>) -> Self {
        self.def.entry.push(action.into());
        self
    }

    /// Append an exit action name.
    pub fn exit(mut self, action: impl Into<String>) -> Self {
        self.def.exit.push(action.into());
        self
    }

    /// Add an unguarded external transition on `event` to `target`.
    pub fn on(self, event: impl Into<String>, target: impl Into<String>) -> Self {
        self.on_transition(event, TransitionBuilder::to(target))
    }

    /// Add a guarded external transition on `event` to `target`.
    pub fn on_guarded(
        self,
        event: impl Into<String>,
        target: impl Into<String>,
        guard: impl Into<String>,
    ) -> Self {
        self.on_transition(event, TransitionBuilder::to(target).guard(guard))
    }

    /// Add a fully-specified transition on `event`.
    pub fn on_transition(mut self, event: impl Into<String>, t: TransitionBuilder) -> Self {
        self.def.transitions.insert(event.into(), t.inner);
        self
    }
}

/// Fluent builder assembling a [`ChartDef`] from typed Rust states, the
/// definition path parallel to [`ChartDef::from_json`]. State insertion order
/// fixes deterministic parallel-region descent, exactly as JSON key order does.
#[derive(Default)]
pub struct ChartBuilder {
    states: Vec<StateBuilder>,
}

impl ChartBuilder {
    /// A new, empty builder.
    pub fn new() -> Self {
        Self { states: Vec::new() }
    }

    /// Add a state. The first parent-less state added becomes the root.
    pub fn state(mut self, state: StateBuilder) -> Self {
        self.states.push(state);
        self
    }

    /// Validate and assemble the [`ChartDef`]. Fails on a duplicate state id, on
    /// zero or more than one parent-less root, matching the JSON path.
    pub fn build(self) -> Result<ChartDef, String> {
        let mut states: HashMap<String, StateDef> = HashMap::new();
        let mut order: HashMap<String, usize> = HashMap::new();
        for (idx, sb) in self.states.into_iter().enumerate() {
            if states.contains_key(&sb.id) {
                return Err(format!("duplicate state id `{}`", sb.id));
            }
            order.insert(sb.id.clone(), idx);
            states.insert(sb.id, sb.def);
        }
        ChartDef::from_states(states, order)
    }
}

impl StateChart {
    /// Create a chart over `ctx`, entering the initial configuration by
    /// descending from the root via each compound's `initial` (and every region
    /// for parallel states). Initial entry actions are recorded and available
    /// via [`StateChart::last_actions`].
    pub fn new(ctx: &Context, def: ChartDef) -> Self {
        let mut enter = BTreeSet::new();
        let mut actions = Vec::new();
        enter_subtree(&def, &def.root, &mut enter, &mut actions);

        let config = ctx.source(enter);
        Self {
            def,
            config,
            history: RefCell::new(HashMap::new()),
            last_actions: RefCell::new(actions),
        }
    }

    /// Ordered action names fired by the initial entry or the most recent
    /// [`StateChart::send`] (exit → transition → entry).
    pub fn last_actions(&self) -> Vec<String> {
        self.last_actions.borrow().clone()
    }

    /// The full active configuration (active leaves plus all active ancestors).
    pub fn configuration(&self, ctx: &Context) -> BTreeSet<String> {
        ctx.get(&self.config)
    }

    /// Active atomic leaves, sorted (one per parallel region; one for single-region).
    pub fn active_leaves(&self, ctx: &Context) -> Vec<String> {
        let config = self.configuration(ctx);
        let mut leaves: Vec<String> = config
            .iter()
            .filter(|id| matches!(self.def.kind(id), Kind::Atomic | Kind::Final))
            .cloned()
            .collect();
        leaves.sort();
        leaves
    }

    /// Hierarchical "state-in" predicate: `true` iff `id` is in the active configuration.
    pub fn matches(&self, ctx: &Context, id: &str) -> bool {
        self.configuration(ctx).contains(id)
    }

    /// Send an event. Returns `true` if any transition was taken, `false` if
    /// rejected (configuration unchanged, no actions fired). `guards` resolves
    /// named guards for this send (absent/unknown name → fail-closed `false`).
    pub fn send(&self, ctx: &Context, event: &str, guards: &HashMap<String, bool>) -> bool {
        let config = self.configuration(ctx);
        let mut history = self.history.borrow_mut();
        match engine_send(&self.def, &mut history, &config, event, guards) {
            Some((new_config, actions)) => {
                drop(history);
                *self.last_actions.borrow_mut() = actions;
                if new_config != config {
                    ctx.set(&self.config, new_config);
                }
                true
            }
            None => {
                drop(history);
                *self.last_actions.borrow_mut() = Vec::new();
                false
            }
        }
    }
}

/// A reactive full-Harel state chart backed by a [`ThreadSafeContext`]. Same
/// chart semantics as [`StateChart`]; the configuration cell is shared safely
/// across threads and history/last-actions use a `parking_lot::Mutex`, so the
/// whole chart is `Send + Sync`. A status observer on one thread can read
/// [`ThreadSafeStateChart::configuration`] while another drives
/// [`ThreadSafeStateChart::send`].
#[cfg(feature = "thread-safe")]
pub struct ThreadSafeStateChart {
    def: ChartDef,
    config: Source<BTreeSet<String>>,
    history: parking_lot::Mutex<HashMap<String, Recording>>,
    last_actions: parking_lot::Mutex<Vec<String>>,
}

#[cfg(feature = "thread-safe")]
impl ThreadSafeStateChart {
    /// Create a chart over `ctx`, entering the initial configuration.
    pub fn new(ctx: &ThreadSafeContext, def: ChartDef) -> Self {
        let mut enter = BTreeSet::new();
        let mut actions = Vec::new();
        enter_subtree(&def, &def.root, &mut enter, &mut actions);
        let config = ctx.source(enter);
        Self {
            def,
            config,
            history: parking_lot::Mutex::new(HashMap::new()),
            last_actions: parking_lot::Mutex::new(actions),
        }
    }

    /// Ordered action names fired by the initial entry or the most recent `send`.
    pub fn last_actions(&self) -> Vec<String> {
        self.last_actions.lock().clone()
    }

    /// The full active configuration (active leaves plus all active ancestors).
    pub fn configuration(&self, ctx: &ThreadSafeContext) -> BTreeSet<String> {
        ctx.get(&self.config)
    }

    /// Active atomic leaves, sorted (one per parallel region).
    pub fn active_leaves(&self, ctx: &ThreadSafeContext) -> Vec<String> {
        let config = self.configuration(ctx);
        let mut leaves: Vec<String> = config
            .iter()
            .filter(|id| matches!(self.def.kind(id), Kind::Atomic | Kind::Final))
            .cloned()
            .collect();
        leaves.sort();
        leaves
    }

    /// Hierarchical "state-in" predicate.
    pub fn matches(&self, ctx: &ThreadSafeContext, id: &str) -> bool {
        self.configuration(ctx).contains(id)
    }

    /// Send an event. Returns `true` if any transition was taken, `false` if
    /// rejected (configuration unchanged, no actions fired).
    pub fn send(
        &self,
        ctx: &ThreadSafeContext,
        event: &str,
        guards: &HashMap<String, bool>,
    ) -> bool {
        let config = self.configuration(ctx);
        let mut history = self.history.lock();
        match engine_send(&self.def, &mut history, &config, event, guards) {
            Some((new_config, actions)) => {
                drop(history);
                *self.last_actions.lock() = actions;
                if new_config != config {
                    ctx.set(&self.config, new_config);
                }
                true
            }
            None => {
                drop(history);
                *self.last_actions.lock() = Vec::new();
                false
            }
        }
    }
}

/// A reactive full-Harel state chart backed by an [`AsyncContext`]. Because
/// cells are the synchronous input layer of `AsyncContext`, `send`/`state`
/// remain synchronous; reactive observers (`ctx.effect`/`ctx.signal` reading the
/// configuration) drive async recomputation. Same chart semantics as
/// [`StateChart`].
#[cfg(feature = "async")]
pub struct AsyncStateChart {
    def: ChartDef,
    config: AsyncSource<BTreeSet<String>>,
    history: parking_lot::Mutex<HashMap<String, Recording>>,
    last_actions: parking_lot::Mutex<Vec<String>>,
}

#[cfg(feature = "async")]
impl AsyncStateChart {
    /// Create a chart over `ctx`, entering the initial configuration.
    pub fn new(ctx: &AsyncContext, def: ChartDef) -> Self {
        let mut enter = BTreeSet::new();
        let mut actions = Vec::new();
        enter_subtree(&def, &def.root, &mut enter, &mut actions);
        let config = ctx.source(enter);
        Self {
            def,
            config,
            history: parking_lot::Mutex::new(HashMap::new()),
            last_actions: parking_lot::Mutex::new(actions),
        }
    }

    /// Ordered action names fired by the initial entry or the most recent `send`.
    pub fn last_actions(&self) -> Vec<String> {
        self.last_actions.lock().clone()
    }

    /// The full active configuration (active leaves plus all active ancestors).
    pub fn configuration(&self, ctx: &AsyncContext) -> BTreeSet<String> {
        ctx.get(&self.config)
    }

    /// Active atomic leaves, sorted (one per parallel region).
    pub fn active_leaves(&self, ctx: &AsyncContext) -> Vec<String> {
        let config = self.configuration(ctx);
        let mut leaves: Vec<String> = config
            .iter()
            .filter(|id| matches!(self.def.kind(id), Kind::Atomic | Kind::Final))
            .cloned()
            .collect();
        leaves.sort();
        leaves
    }

    /// Hierarchical "state-in" predicate.
    pub fn matches(&self, ctx: &AsyncContext, id: &str) -> bool {
        self.configuration(ctx).contains(id)
    }

    /// Send an event (synchronous). Returns `true` if any transition was taken.
    pub fn send(&self, ctx: &AsyncContext, event: &str, guards: &HashMap<String, bool>) -> bool {
        let config = self.configuration(ctx);
        let mut history = self.history.lock();
        match engine_send(&self.def, &mut history, &config, event, guards) {
            Some((new_config, actions)) => {
                drop(history);
                *self.last_actions.lock() = actions;
                if new_config != config {
                    ctx.set(&self.config, new_config);
                }
                true
            }
            None => {
                drop(history);
                *self.last_actions.lock() = Vec::new();
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Context-free transition engine. Every `StateChart` variant (single-threaded,
// thread-safe, async) delegates here; only the cell storage and interior-
// mutability wrapper differ per variant, never the Harel semantics. This is the
// algorithm the spec conformance fixtures and the Lean formal model pin down —
// keep it byte-for-byte behaviourally identical across refactors.
// ---------------------------------------------------------------------------

/// Compute one Harel macrostep. Returns `Some((new_config, actions))` when a
/// transition is taken (the new configuration may equal the old for an internal
/// self-transition) and `None` when the event is rejected (no enabled
/// transition; configuration and history unchanged). `history` is mutated only
/// when a transition is actually taken.
fn engine_send(
    def: &ChartDef,
    history: &mut HashMap<String, Recording>,
    config: &BTreeSet<String>,
    event: &str,
    guards: &HashMap<String, bool>,
) -> Option<(BTreeSet<String>, Vec<String>)> {
    // 1. Enabled transitions: per active leaf, innermost passing match.
    struct Cand<'a> {
        source: String,
        transition: &'a Transition,
        leaf: String,
    }
    let mut candidates: Vec<Cand> = Vec::new();
    let leaves: Vec<String> = config
        .iter()
        .filter(|id| matches!(def.kind(id), Kind::Atomic | Kind::Final))
        .cloned()
        .collect();
    for leaf in &leaves {
        for anc in def.ancestors_inclusive(leaf) {
            if let Some(sd) = def.states.get(&anc)
                && let Some(t) = sd.transitions.get(event)
                && guard_passes(t, guards)
            {
                candidates.push(Cand {
                    source: anc.clone(),
                    transition: t,
                    leaf: leaf.clone(),
                });
                break; // innermost wins for this leaf's chain
            }
        }
    }

    if candidates.is_empty() {
        return None;
    }

    // 2. Conflict resolution: order by source depth desc, then document order;
    //    take greedily, skipping any whose exit set intersects the taken union.
    candidates.sort_by(|a, b| {
        def.depth(&b.source)
            .cmp(&def.depth(&a.source))
            .then_with(|| def.order.get(&a.source).cmp(&def.order.get(&b.source)))
    });

    let mut exit_union: BTreeSet<String> = BTreeSet::new();
    let mut enter_union: BTreeSet<String> = BTreeSet::new();
    let mut taken_transitions: Vec<&Transition> = Vec::new();
    for cand in &candidates {
        let (exit_set, enter_set) = engine_compute_exit_enter(
            def,
            history,
            &cand.source,
            cand.transition,
            &cand.leaf,
            config,
        );
        if exit_set.intersection(&exit_union).next().is_some() {
            continue; // conflicts with an already-taken transition
        }
        exit_union.extend(exit_set);
        enter_union.extend(enter_set);
        taken_transitions.push(cand.transition);
    }

    if taken_transitions.is_empty() {
        return None;
    }

    // 3. Record history for regions being exited that own a history child.
    for s in &exit_union {
        if let Some(h_child) = history_child_of(def, s) {
            record_region(def, s, h_child, config, history);
        }
    }

    // 4. Action trace: exit (innermost-first) → transition → entry (outermost-first).
    let mut actions = Vec::new();
    let mut exit_sorted: Vec<&String> = exit_union.iter().collect();
    exit_sorted.sort_by_key(|s| std::cmp::Reverse(def.depth(s)));
    for s in &exit_sorted {
        actions.extend(def.states[*s].exit.iter().cloned());
    }
    for t in &taken_transitions {
        actions.extend(t.action.iter().cloned());
    }
    let mut enter_sorted: Vec<&String> = enter_union.iter().collect();
    enter_sorted.sort_by_key(|s| def.depth(s));
    for s in &enter_sorted {
        actions.extend(def.states[*s].entry.iter().cloned());
    }

    // 5. Compute new configuration.
    let mut new_config = config.clone();
    for s in &exit_union {
        new_config.remove(s);
    }
    for s in &enter_union {
        new_config.insert(s.clone());
    }

    Some((new_config, actions))
}

fn engine_compute_exit_enter(
    def: &ChartDef,
    history: &HashMap<String, Recording>,
    source: &str,
    transition: &Transition,
    leaf: &str,
    config: &BTreeSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let target = &transition.target;
    let internal =
        transition.internal && (target == source || def.is_proper_descendant(target, source));
    let lca = if internal {
        source.to_string()
    } else {
        def.lca(leaf, target)
    };

    // Exit set: active proper-descendants of the lca.
    let exit_set: BTreeSet<String> = config
        .iter()
        .filter(|s| def.is_proper_descendant(s, &lca))
        .cloned()
        .collect();

    // Enter set.
    let mut enter: BTreeSet<String> = BTreeSet::new();
    if matches!(def.kind(target), Kind::History(_)) {
        let region = def.states[target]
            .parent
            .clone()
            .unwrap_or_else(|| def.root.clone());
        for s in path_below(def, &lca, &region) {
            enter.insert(s);
        }
        engine_restore_via_history(def, history, target, &region, &mut enter);
    } else {
        for s in path_below(def, &lca, target) {
            enter.insert(s);
        }
        let mut entry_actions = Vec::new();
        enter_subtree(def, target, &mut enter, &mut entry_actions);
    }

    (exit_set, enter)
}

fn engine_restore_via_history(
    def: &ChartDef,
    history: &HashMap<String, Recording>,
    hist: &str,
    region: &str,
    enter: &mut BTreeSet<String>,
) {
    match history.get(hist) {
        Some(Recording::Shallow(child)) => {
            let child = child.clone();
            enter.insert(child.clone());
            let mut tmp = Vec::new();
            enter_subtree(def, &child, enter, &mut tmp);
        }
        Some(Recording::Deep(set)) => {
            for s in set {
                enter.insert(s.clone());
            }
        }
        None => {
            // First entry: descend via `default`, else the region's `initial`.
            let start = def.states[hist]
                .default
                .clone()
                .or_else(|| def.states[region].initial.clone());
            if let Some(start) = start {
                for s in path_below(def, region, &start) {
                    enter.insert(s);
                }
                let mut tmp = Vec::new();
                enter_subtree(def, &start, enter, &mut tmp);
            }
        }
    }
}

fn guard_passes(t: &Transition, guards: &HashMap<String, bool>) -> bool {
    match &t.guard {
        None => true,
        Some(name) => guards.get(name).copied().unwrap_or(false), // fail-closed
    }
}

/// Enter `state` and its default descendants, recording entry actions top-down.
fn enter_subtree(
    def: &ChartDef,
    state: &str,
    enter: &mut BTreeSet<String>,
    actions: &mut Vec<String>,
) {
    enter.insert(state.to_string());
    collect_entry(def, state, actions);
    match def.kind(state) {
        Kind::Atomic | Kind::Final | Kind::History(_) => {}
        Kind::Compound => {
            if let Some(init) = def.states[state].initial.as_deref() {
                enter_subtree(def, init, enter, actions);
            }
        }
        Kind::Parallel => {
            for region in def.children.get(state).cloned().unwrap_or_default() {
                enter_subtree(def, &region, enter, actions);
            }
        }
    }
}

fn collect_entry(def: &ChartDef, state: &str, actions: &mut Vec<String>) {
    actions.extend(def.states[state].entry.iter().cloned());
}

/// Path from just-below `lca` down to `target` (exclusive lca, inclusive target).
fn path_below(def: &ChartDef, lca: &str, target: &str) -> Vec<String> {
    let mut chain = def.ancestors_inclusive(target); // [target, ..., root]
    let idx = chain.iter().position(|x| x == lca).unwrap_or(chain.len());
    chain.truncate(idx); // drop lca and above
    chain.reverse(); // [child-of-lca, ..., target]
    chain
}

fn history_child_of(def: &ChartDef, region: &str) -> Option<String> {
    def.children.get(region).and_then(|kids| {
        kids.iter()
            .find(|k| matches!(def.kind(k), Kind::History(_)))
            .cloned()
    })
}

fn record_region(
    def: &ChartDef,
    region: &str,
    hist_child: String,
    config: &BTreeSet<String>,
    history: &mut HashMap<String, Recording>,
) {
    let kind = match def.kind(&hist_child) {
        Kind::History(h) => h,
        _ => return,
    };
    match kind {
        HistoryKind::Shallow => {
            // Record the direct child of `region` that was active.
            let child = def
                .children
                .get(region)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .find(|c| config.contains(c) && !matches!(def.kind(c), Kind::History(_)));
            if let Some(c) = child {
                history.insert(hist_child, Recording::Shallow(c));
            }
        }
        HistoryKind::Deep => {
            // Record every active state strictly below `region`.
            let set: BTreeSet<String> = config
                .iter()
                .filter(|s| def.is_proper_descendant(s, region))
                .cloned()
                .collect();
            history.insert(hist_child, Recording::Deep(set));
        }
    }
}

/// `#failclosedsweep`: the chart loader rejects a malformed discriminator instead
/// of reading it as an absent one.
///
/// spec `state-charts.md` § Implementation status: a binding "MUST reject any
/// feature it does not implement explicitly (never silently ignore a `parallel`,
/// `history`, `entry/exit/run`, or `{"expr": …}` guard)". Every case below used to
/// parse successfully into a chart that quietly meant something else.
#[cfg(all(test, feature = "statechart-json"))]
mod parse_strictness_tests {
    use super::*;

    fn chart(states: &str) -> Result<ChartDef, String> {
        let text = format!(r#"{{ "initial": "root", "states": {states} }}"#);
        let v: serde_json::Value = serde_json::from_str(&text).expect("test json parses");
        ChartDef::from_json(&v)
    }

    fn rejection(states: &str) -> String {
        chart(states).expect_err("this chart must be rejected")
    }

    #[test]
    fn an_unknown_state_kind_is_rejected_by_name() {
        let err = rejection(r#"{ "root": { "kind": "banana" } }"#);
        assert!(
            err.contains("unknown kind") && err.contains("banana"),
            "the rejection names the offending value: {err}"
        );
    }

    #[test]
    fn a_declared_kind_that_contradicts_the_state_is_rejected() {
        // `kind: "parallel"` with no `parallel: true` is the silent-ignore case the
        // spec calls out: it used to parse as an atomic state.
        let err = rejection(r#"{ "root": { "kind": "parallel" } }"#);
        assert!(
            err.contains("contradicts") && err.contains("parallel"),
            "the rejection explains the contradiction: {err}"
        );
    }

    #[test]
    fn a_final_kind_over_a_parallel_or_history_state_is_rejected() {
        for states in [
            r#"{ "root": { "kind": "final", "parallel": true } }"#,
            r#"{ "root": { "initial": "h" }, "h": { "parent": "root", "kind": "final", "history": "deep" } }"#,
        ] {
            let err = rejection(states);
            assert!(
                err.contains("`kind: final` contradicts"),
                "a final state cannot also be parallel/history: {err}"
            );
        }
    }

    #[test]
    fn a_consistent_declared_kind_is_still_accepted() {
        let def = chart(
            r#"{
                "root": { "kind": "compound", "initial": "leaf" },
                "leaf": { "parent": "root", "kind": "atomic" }
            }"#,
        )
        .expect("a chart whose declared kinds agree with its fields still loads");
        assert_eq!(def.kind("root"), Kind::Compound);
        assert_eq!(def.kind("leaf"), Kind::Atomic);

        let parallel = chart(
            r#"{
                "root": { "kind": "parallel", "parallel": true },
                "a": { "parent": "root" },
                "b": { "parent": "root" }
            }"#,
        )
        .expect("`kind: parallel` with `parallel: true` agrees");
        assert_eq!(parallel.kind("root"), Kind::Parallel);
    }

    #[test]
    fn a_non_boolean_parallel_flag_is_rejected_not_ignored() {
        let err = rejection(r#"{ "root": { "parallel": "true" } }"#);
        assert!(
            err.contains("`parallel` must be a boolean"),
            "a string `parallel` is rejected rather than read as not-parallel: {err}"
        );
    }

    #[test]
    fn a_non_string_history_is_rejected_not_ignored() {
        let err =
            rejection(r#"{ "root": { "initial": "h" }, "h": { "parent": "root", "history": 5 } }"#);
        assert!(
            err.contains("`history` must be a string"),
            "a numeric `history` is rejected rather than demoting the pseudo-state: {err}"
        );
    }

    #[test]
    fn a_non_string_parent_is_rejected_not_promoted_to_root() {
        // Read through `as_str()` this was `None`, i.e. a SECOND parent-less root.
        let err = rejection(r#"{ "root": { "initial": "leaf" }, "leaf": { "parent": 7 } }"#);
        assert!(
            err.contains("`parent` must be a string"),
            "a numeric `parent` is rejected rather than making the state a root: {err}"
        );
    }

    #[test]
    fn a_non_string_initial_is_rejected_not_demoted_to_atomic() {
        let err = rejection(r#"{ "root": { "initial": 3 } }"#);
        assert!(
            err.contains("`initial` must be a string"),
            "a numeric `initial` is rejected rather than read as absent: {err}"
        );
    }

    #[test]
    fn a_non_boolean_transition_internal_flag_is_rejected_not_ignored() {
        let err = rejection(
            r#"{
                "root": { "initial": "a" },
                "a": { "parent": "root", "on": { "go": { "target": "b", "internal": "yes" } } },
                "b": { "parent": "root" }
            }"#,
        );
        assert!(
            err.contains("`internal` must be a boolean"),
            "a string `internal` is rejected rather than silently turning the transition external: {err}"
        );
    }

    #[test]
    fn a_well_formed_internal_transition_still_parses() {
        let def = chart(
            r#"{
                "root": { "initial": "a" },
                "a": { "parent": "root", "on": { "go": { "target": "a", "internal": true } } }
            }"#,
        )
        .expect("a boolean `internal` still loads");
        assert!(
            def.states["a"].transitions["go"].internal,
            "the internal flag survives the stricter parse"
        );
    }

    #[test]
    fn an_unknown_history_kind_is_still_rejected() {
        let err = rejection(
            r#"{ "root": { "initial": "h" }, "h": { "parent": "root", "history": "sideways" } }"#,
        );
        assert!(
            err.contains("unknown history kind") && err.contains("sideways"),
            "the pre-existing history rejection is unchanged: {err}"
        );
    }
}

#[cfg(test)]
mod builder_variant_tests {
    use super::*;

    /// A small chart used by both the JSON and Rust-builder equivalence check:
    /// a parallel root over two regions, one guarded transition, one final.
    #[cfg(feature = "statechart-json")]
    fn json_chart() -> ChartDef {
        let v: serde_json::Value = serde_json::from_str(
            r#"{
              "initial": "root",
              "states": {
                "root": { "parallel": true },
                "flow": { "parent": "root", "initial": "idle" },
                "idle": { "parent": "flow", "on": { "go": { "target": "done", "guard": "ready" } } },
                "done": { "parent": "flow", "kind": "final" },
                "net": { "parent": "root", "initial": "up" },
                "up": { "parent": "net", "on": { "drop": { "target": "down" } } },
                "down": { "parent": "net", "on": { "restore": { "target": "up" } } }
              }
            }"#,
        )
        .unwrap();
        ChartDef::from_json(&v).unwrap()
    }

    fn built_chart() -> ChartDef {
        ChartBuilder::new()
            .state(StateBuilder::parallel("root"))
            .state(StateBuilder::compound("flow", "idle").parent("root"))
            .state(
                StateBuilder::atomic("idle")
                    .parent("flow")
                    .on_guarded("go", "done", "ready"),
            )
            .state(StateBuilder::final_state("done").parent("flow"))
            .state(StateBuilder::compound("net", "up").parent("root"))
            .state(StateBuilder::atomic("up").parent("net").on("drop", "down"))
            .state(
                StateBuilder::atomic("down")
                    .parent("net")
                    .on("restore", "up"),
            )
            .build()
            .unwrap()
    }

    /// The typed builder + engine are core (no feature): a built chart enters the
    /// initial configuration of every parallel region and drives a guarded edge.
    #[test]
    fn builder_engine_is_core() {
        use crate::Context;
        let ctx = Context::new();
        let chart = StateChart::new(&ctx, built_chart());
        // Both parallel regions enter their initial leaves.
        assert!(chart.matches(&ctx, "idle") && chart.matches(&ctx, "up"));
        // Guarded edge rejected when the guard is false, taken when true.
        let mut guards = HashMap::new();
        guards.insert("ready".to_string(), false);
        assert!(!chart.send(&ctx, "go", &guards));
        assert!(chart.matches(&ctx, "idle"));
        guards.insert("ready".to_string(), true);
        assert!(chart.send(&ctx, "go", &guards));
        assert!(chart.matches(&ctx, "done"));
    }

    /// The Rust builder and JSON paths produce charts that behave identically:
    /// same initial configuration and same response to the same event stream.
    #[cfg(feature = "statechart-json")]
    #[test]
    fn builder_matches_json_behaviour() {
        use crate::Context;
        let ctx_j = Context::new();
        let ctx_b = Context::new();
        let cj = StateChart::new(&ctx_j, json_chart());
        let cb = StateChart::new(&ctx_b, built_chart());
        assert_eq!(cj.configuration(&ctx_j), cb.configuration(&ctx_b));

        let mut ready = HashMap::new();
        ready.insert("ready".to_string(), false);
        // Guard false: rejected on both.
        assert_eq!(cj.send(&ctx_j, "go", &ready), cb.send(&ctx_b, "go", &ready));
        assert_eq!(cj.configuration(&ctx_j), cb.configuration(&ctx_b));
        // Orthogonal region transition on both.
        let empty = HashMap::new();
        assert!(cj.send(&ctx_j, "drop", &empty));
        assert!(cb.send(&ctx_b, "drop", &empty));
        // Guard true: accepted on both.
        ready.insert("ready".to_string(), true);
        assert!(cj.send(&ctx_j, "go", &ready));
        assert!(cb.send(&ctx_b, "go", &ready));
        assert_eq!(cj.configuration(&ctx_j), cb.configuration(&ctx_b));
        assert!(cj.matches(&ctx_j, "done") && cj.matches(&ctx_j, "down"));
    }

    #[cfg(feature = "thread-safe")]
    #[test]
    fn thread_safe_chart_is_send_sync_and_transitions() {
        use crate::ThreadSafeContext;
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ThreadSafeStateChart>();

        let ctx = ThreadSafeContext::new();
        let chart = ThreadSafeStateChart::new(&ctx, built_chart());
        assert!(chart.matches(&ctx, "idle") && chart.matches(&ctx, "up"));
        let empty = HashMap::new();
        // Guard absent -> fail-closed rejection.
        assert!(!chart.send(&ctx, "go", &empty));
        assert!(chart.matches(&ctx, "idle"));
        let mut ready = HashMap::new();
        ready.insert("ready".to_string(), true);
        assert!(chart.send(&ctx, "go", &ready));
        assert!(chart.matches(&ctx, "done"));
    }

    #[cfg(feature = "async")]
    #[test]
    fn async_chart_transitions() {
        use crate::AsyncContext;
        let ctx = AsyncContext::new();
        let chart = AsyncStateChart::new(&ctx, built_chart());
        assert!(chart.matches(&ctx, "idle"));
        let empty = HashMap::new();
        assert!(chart.send(&ctx, "drop", &empty));
        assert!(chart.matches(&ctx, "down"));
    }
}
