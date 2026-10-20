//! Loading progress, recorded per node: what the add-on's progress
//! snapshot shows (docs/mobile-progress-api.md).
//!
//! freedom-ipfs's crates emit a `tracing` event with a `phase` field at
//! each step of a request, a preload or a lookup. [`progress_layer`] turns
//! those into a [`ProgressRecorder`]'s bounded snapshot: the targets still
//! active, what each is doing, and the latest 512 events.
//!
//! # One recorder per node
//!
//! The subscriber is process-wide, so one layer serves every node and
//! routes each event to the recorder of the node whose work emitted it:
//!
//! - **A thread** records into a recorder while it is in that recorder's
//!   scope ([`ProgressRecorder::enter`], [`ProgressRecorder::scope`]), and
//!   for its whole life if it is a thread of a runtime marked with
//!   [`ProgressRecorder::mark_runtime_threads`]: the runtime's workers, and
//!   its blocking pool's threads, which run its `spawn_blocking` jobs and
//!   take over from a worker that enters `block_in_place`.
//! - **A span** of freedom-ipfs's takes its recorder when it is created,
//!   and keeps it for its life, on whatever thread it is entered: a gateway
//!   request's root span takes its gateway's
//!   ([`GatewayCore::with_progress`](crate::GatewayCore::with_progress)),
//!   whatever span is current; any other span its parent span's, or else
//!   the creating thread's, or else the default. So a request's descendants
//!   and its response body record into the request's node, wherever its
//!   reader polls it.
//! - **An event** goes to the recorder of its span: its explicit parent, or
//!   else the current span, unless it is a root event (`parent: None`).
//!   Without a span, it goes to its thread's recorder, or else the
//!   process's [`default_recorder`].
//!
//! An [`Instance`](crate::Instance) marks its runtime with a recorder of its
//! own ([`Instance::progress`](crate::Instance::progress)), so everything
//! that runs there records there: its requests, the tasks it starts for
//! itself (the Bitswap swarm's, preconnects) and its blocking jobs. A
//! node's gateways bind each request's span to its recorder, and run the
//! request in its scope ([`GatewayCore::handle`](crate::GatewayCore::handle)),
//! so a request a host polls on a thread of its own, or under another
//! node's span, records there too, and the request's spans carry the
//! recorder to its body. Two instances' progress never mixes.
//!
//! What still reaches the default recorder is work on a thread no node
//! marked, outside any node's scope and spans:
//!
//! - for a node built with [`OnlineNodeBuilder::build`](crate::OnlineNodeBuilder::build)
//!   on a runtime its host shares, the work it starts for itself, such as
//!   the Bitswap swarm's task. A host that wants it marks that runtime;
//! - the work of a host that calls a node's block provider or name
//!   resolver itself, outside a request. It enters the node's scope around
//!   the call, outside other nodes' spans: a current span's recorder
//!   outranks the thread's.
//!
//! A host that emits events of its own on a node's behalf, as the add-on
//! does for its preloads, emits them as root events (`parent: None`) in the
//! node's scope, so no span it happens to be in can take them.
//!
//! A host that gives no node a recorder of its own finds everything in the
//! default recorder.
//!
//! # Cancelled requests
//!
//! A gateway request's target is active from its `request_start` until its
//! `request_done`, or, when its body streams, until `gateway_stream_done`
//! or `gateway_stream_failed`. A request dropped before then (its host
//! cancelled it, or its reader dropped the body) emits neither. Its span
//! closes once nothing holds it: its future, its body, and the spans
//! opened inside it, such as a background prefetch's. If its target is
//! still open then, and still its own, the recorder closes it with an event
//! of its own: raw phase `request_cancelled`, phase and status
//! `cancelled`. No other layer sees that event.
//!
//! "Its own": the recorder keeps, for each active target, the span that
//! owns it, which is the last span whose event for the target it stands
//! for left it active. That changes under the recorder's lock with the
//! event itself, so it follows the order the recorder takes events in,
//! whichever threads emit them. A closing span cancels only a target it
//! still owns: not one a newer request reusing its id took over, nor one
//! the host cleared.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::task::Poll;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

const MAX_PROGRESS_EVENTS: usize = 512;

/// The raw phase of the event that closes a request whose span ended while
/// its target was open: see `ProgressLayer::on_close`.
const REQUEST_CANCELLED: &str = "request_cancelled";

static DEFAULT_RECORDER: OnceLock<ProgressRecorder> = OnceLock::new();
static PROGRESS_TRACING_INIT: Once = Once::new();
static PROGRESS_LAYER_ISSUED: AtomicBool = AtomicBool::new(false);
/// The next span's owner token: unique for the process's life, unlike span
/// ids, which the registry reuses.
static NEXT_SPAN_OWNER: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The recorder this thread's events go to when their span names none.
    static THREAD_RECORDER: RefCell<Option<ProgressRecorder>> = const { RefCell::new(None) };
    /// The recorder that owns the spans this thread creates now, whatever
    /// their parent: a gateway's, around its request's root span.
    static SPAN_OWNER: RefCell<Option<ProgressRecorder>> = const { RefCell::new(None) };
}

/// The current thread's recorder, if any. None while the thread's locals
/// are being torn down.
fn thread_recorder() -> Option<ProgressRecorder> {
    THREAD_RECORDER
        .try_with(|current| current.borrow().clone())
        .ok()
        .flatten()
}

/// The recorder that owns the spans the current thread creates now, if any.
fn span_owner() -> Option<ProgressRecorder> {
    SPAN_OWNER
        .try_with(|owner| owner.borrow().clone())
        .ok()
        .flatten()
}

/// Sets the current thread's recorder, returning the one it had.
fn set_thread_recorder(recorder: Option<ProgressRecorder>) -> Option<ProgressRecorder> {
    THREAD_RECORDER
        .try_with(|current| current.replace(recorder))
        .ok()
        .flatten()
}

#[derive(Clone, Default)]
struct ProgressSpanFields {
    request_id: Option<u64>,
    progress_request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
    namespace: Option<String>,
    path: Option<String>,
}

/// A node's progress: the targets still active and the latest events, at
/// most 512. Clones share them.
///
/// Which events reach it is the [module's](self) routing: a recorder records
/// only what its scopes, its marked runtimes and their spans emit.
#[derive(Clone, Default)]
pub struct ProgressRecorder {
    inner: Arc<Mutex<ProgressInner>>,
}

impl fmt::Debug for ProgressRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgressRecorder").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct ProgressInner {
    next_event_id: u64,
    events: VecDeque<ProgressEvent>,
    active_targets: HashMap<String, ProgressTarget>,
    /// The owner token of the span that owns each active target: the last
    /// one whose event for the target it stands for left it active. Only an
    /// active target has an owner, and only its owner's close cancels it.
    owners: HashMap<String, u64>,
}

/// Which events `ProgressRecorder::record` takes.
enum Admit {
    /// An event, emitted in the span with this owner token, if any.
    Event { owner: Option<u64> },
    /// The cancel of a closed span: only if the span with this owner token
    /// still owns its target.
    IfOwnedBy(u64),
}

#[derive(Clone, Debug, Serialize)]
struct ProgressEvent {
    event_id: u64,
    target_id: u64,
    request_id: Option<u64>,
    parent_id: Option<u64>,
    kind: String,
    path: Option<String>,
    top_level_path: Option<String>,
    namespace: Option<String>,
    phase: String,
    raw_phase: String,
    status: String,
    source: Option<String>,
    transport: Option<String>,
    delivery: Option<String>,
    bytes_loaded: Option<u64>,
    bytes_total: Option<u64>,
    providers_found: Option<u64>,
    candidate_peers: Option<u64>,
    blocks_loaded: u64,
    retry_count: u64,
    elapsed_ms: Option<u64>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ProgressTarget {
    id: u64,
    request_id: Option<u64>,
    parent_id: Option<u64>,
    kind: String,
    path: Option<String>,
    top_level_path: Option<String>,
    namespace: Option<String>,
    phase: String,
    status: String,
    source: Option<String>,
    transport: Option<String>,
    delivery: Option<String>,
    bytes_loaded: Option<u64>,
    bytes_total: Option<u64>,
    active_subrequests: usize,
    elapsed_ms: Option<u64>,
    blocks_loaded: u64,
    retry_count: u64,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    last_event_id: u64,
    updated_ms: u64,
}

/// A recorder's snapshot, which serializes to the JSON
/// docs/mobile-progress-api.md describes.
#[derive(Clone, Debug, Serialize)]
pub struct ProgressSnapshot {
    generated_at_unix_ms: u64,
    active_count: usize,
    event_count: usize,
    active: Vec<ProgressTarget>,
    events: Vec<ProgressEvent>,
}

impl ProgressRecorder {
    /// A recorder of its own, empty.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the current thread's events into this recorder until the
    /// scope is dropped, then into whichever it recorded into before. Spans
    /// created meanwhile keep this recorder; an event in a span that already
    /// has one still goes to the span's.
    pub fn enter(&self) -> ProgressScope {
        ProgressScope {
            previous: set_thread_recorder(Some(self.clone())),
            _thread: PhantomData,
        }
    }

    /// `future`, recording into this recorder whenever it is polled or
    /// dropped, on whichever thread does that.
    pub fn scope<F: Future>(&self, future: F) -> impl Future<Output = F::Output> {
        Scoped {
            recorder: self.clone(),
            future: Some(Box::pin(future)),
        }
    }

    /// Makes the spans the current thread creates until the guard is
    /// dropped belong to this recorder, whatever their parent. A gateway
    /// creates its request's root span under it, so a request it serves
    /// records into its node even while another node's span is current;
    /// the root's descendants and its body inherit it as usual.
    pub(crate) fn own_new_spans(&self) -> SpanOwnerScope {
        SpanOwnerScope {
            previous: SPAN_OWNER
                .try_with(|owner| owner.replace(Some(self.clone())))
                .ok()
                .flatten(),
            _thread: PhantomData,
        }
    }

    /// Makes every thread of the runtime `builder` builds record into this
    /// recorder: its workers, and its blocking pool's threads. This takes
    /// the builder's `on_thread_start` and `on_thread_stop`.
    pub fn mark_runtime_threads<'b>(
        &self,
        builder: &'b mut tokio::runtime::Builder,
    ) -> &'b mut tokio::runtime::Builder {
        let recorder = self.clone();
        builder
            .on_thread_start(move || drop(set_thread_recorder(Some(recorder.clone()))))
            .on_thread_stop(|| drop(set_thread_recorder(None)))
    }

    /// Records an event emitted in `span`, whose owner token is `owner`
    /// (`None` outside a span).
    fn record_event(&self, span: ProgressSpanFields, fields: ProgressFields, owner: Option<u64>) {
        self.record(span, fields, Admit::Event { owner });
    }

    /// The span with owner token `owner` closed: if it still owns the
    /// target it stands for, nothing ended that target, so it is closed as
    /// cancelled.
    fn close_span(&self, owner: u64, span: ProgressSpanFields) {
        let mut fields = ProgressFields::default();
        fields
            .values
            .insert("phase".into(), REQUEST_CANCELLED.into());
        self.record(span, fields, Admit::IfOwnedBy(owner));
    }

    /// Records an event that has a phase, as `admit` says. The event, its
    /// target and the target's owner change under one lock, so they agree
    /// in the order events are recorded, whichever threads emit them.
    fn record(&self, span: ProgressSpanFields, fields: ProgressFields, admit: Admit) {
        let Some(raw_phase) = fields.get("phase").cloned() else {
            return;
        };
        let own_target = span_target_key(&span);
        let kind = progress_kind(&fields, &raw_phase, &span);
        let target_id = progress_target_id(&fields, &span);
        let path = fields.get("path").cloned().or(span.path);
        let top_level_path = fields
            .get("top_level_path")
            .filter(|path| !path.is_empty())
            .cloned()
            .or(span.top_level_path);
        let namespace = fields.get("namespace").cloned().or(span.namespace);
        let request_id = fields.get_u64("request_id").or(span.request_id);
        let parent_id = fields
            .get_u64("parent_request_id")
            .filter(|id| *id != 0)
            .or(span.parent_request_id);
        let status = progress_status(&raw_phase, &fields);
        let phase = progress_phase(&raw_phase, &fields, &status);
        let elapsed_ms = fields.get_u64("elapsed_ms");
        let timestamp_ms = now_ms();
        let source = progress_source(&raw_phase, &fields);
        let transport = fields
            .get("source_transport")
            .cloned()
            .or_else(|| fields.get("transport").cloned());
        let delivery = fields.get("bitswap_delivery").cloned();
        let last_error_message = fields.get("error").cloned();
        let last_error_code = progress_error_code(&raw_phase, &fields, &status);
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(_) => return,
        };
        let target_key = format!("{kind}:{target_id}");
        // A span owns only the target it stands for.
        let owner = match admit {
            Admit::Event { owner } => {
                owner.filter(|_| own_target.as_deref() == Some(target_key.as_str()))
            }
            Admit::IfOwnedBy(owner) => {
                if inner.owners.get(&target_key) != Some(&owner) {
                    return;
                }
                Some(owner)
            }
        };
        inner.next_event_id = inner.next_event_id.saturating_add(1);
        let previous_target = inner.active_targets.get(&target_key);
        let (
            previous_bytes_loaded,
            previous_bytes_total,
            previous_blocks_loaded,
            previous_retry_count,
        ) = previous_target
            .map(|target| {
                (
                    target.bytes_loaded,
                    target.bytes_total,
                    target.blocks_loaded,
                    target.retry_count,
                )
            })
            .unwrap_or_default();
        let target_source = source
            .clone()
            .or_else(|| previous_target.and_then(|target| target.source.clone()));
        let target_transport = transport
            .clone()
            .or_else(|| previous_target.and_then(|target| target.transport.clone()));
        let target_delivery = delivery
            .clone()
            .or_else(|| previous_target.and_then(|target| target.delivery.clone()));
        let blocks_loaded =
            previous_blocks_loaded.saturating_add(u64::from(raw_phase == "block_fetch_total"));
        let retry_count = previous_retry_count.saturating_add(u64::from(phase == "retrying"));
        let bytes_loaded = progress_bytes_loaded(&fields).or(previous_bytes_loaded);
        let bytes_total = progress_bytes_total(&fields).or(previous_bytes_total);
        let event = ProgressEvent {
            event_id: inner.next_event_id,
            target_id,
            request_id,
            parent_id,
            kind: kind.clone(),
            path: path.clone(),
            top_level_path: top_level_path.clone(),
            namespace: namespace.clone(),
            phase: phase.clone(),
            raw_phase: raw_phase.clone(),
            status: status.clone(),
            source: target_source.clone(),
            transport: target_transport.clone(),
            delivery: target_delivery.clone(),
            bytes_loaded,
            bytes_total,
            providers_found: fields
                .get_u64("provider_count")
                .or_else(|| fields.get_u64("retry_provider_count")),
            candidate_peers: fields
                .get_u64("peer_count")
                .or_else(|| fields.get_u64("candidate_peer_count")),
            blocks_loaded,
            retry_count,
            elapsed_ms,
            last_error_code: last_error_code.clone(),
            last_error_message: last_error_message.clone(),
            timestamp_ms,
        };
        inner.events.push_back(event.clone());
        while inner.events.len() > MAX_PROGRESS_EVENTS {
            inner.events.pop_front();
        }

        if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
            inner.active_targets.remove(&target_key);
            inner.owners.remove(&target_key);
        } else if target_id != 0 {
            // The span this event is in takes the target over; an event from
            // elsewhere leaves its owner as it is.
            if let Some(owner) = owner {
                inner.owners.insert(target_key.clone(), owner);
            }
            inner.active_targets.insert(
                target_key,
                ProgressTarget {
                    id: target_id,
                    request_id,
                    parent_id,
                    kind,
                    path,
                    top_level_path,
                    namespace,
                    phase,
                    status,
                    source: target_source,
                    transport: target_transport,
                    delivery: target_delivery,
                    bytes_loaded,
                    bytes_total,
                    active_subrequests: 0,
                    elapsed_ms,
                    blocks_loaded,
                    retry_count,
                    last_error_code,
                    last_error_message,
                    last_event_id: event.event_id,
                    updated_ms: timestamp_ms,
                },
            );
        }
    }

    /// The targets still active and the latest events.
    pub fn snapshot(&self) -> ProgressSnapshot {
        match self.inner.lock() {
            Ok(inner) => ProgressSnapshot {
                generated_at_unix_ms: now_ms(),
                active_count: inner.active_targets.len(),
                event_count: inner.events.len(),
                active: progress_active_targets(&inner),
                events: inner.events.iter().cloned().collect(),
            },
            Err(_) => ProgressSnapshot {
                generated_at_unix_ms: now_ms(),
                active_count: 0,
                event_count: 0,
                active: Vec::new(),
                events: Vec::new(),
            },
        }
    }

    /// The snapshot, as the JSON docs/mobile-progress-api.md describes.
    pub fn snapshot_json(&self) -> String {
        serde_json::to_string(&self.snapshot())
            .unwrap_or_else(|_| "{\"active\":[],\"events\":[]}".into())
    }

    /// Forgets every target and event.
    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.events.clear();
            inner.active_targets.clear();
            inner.owners.clear();
        }
    }
}

fn progress_active_targets(inner: &ProgressInner) -> Vec<ProgressTarget> {
    let mut active = inner.active_targets.values().cloned().collect::<Vec<_>>();
    let mut active_subrequest_counts = HashMap::<u64, usize>::new();
    for target in &active {
        if let Some(parent_id) = target.parent_id {
            *active_subrequest_counts.entry(parent_id).or_default() += 1;
        }
    }
    for target in &mut active {
        target.active_subrequests = active_subrequest_counts
            .get(&target.id)
            .copied()
            .unwrap_or_default();
    }
    active.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.path.cmp(&right.path))
    });
    active
}

/// The current thread's time in a recorder's scope: see
/// [`ProgressRecorder::enter`].
#[must_use = "the thread records into the recorder only while the scope lives"]
pub struct ProgressScope {
    previous: Option<ProgressRecorder>,
    /// Not `Send`: it restores the recorder of the thread that entered it.
    _thread: PhantomData<*const ()>,
}

impl Drop for ProgressScope {
    fn drop(&mut self) {
        drop(set_thread_recorder(self.previous.take()));
    }
}

impl fmt::Debug for ProgressScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgressScope").finish_non_exhaustive()
    }
}

/// A future in a recorder's scope: see [`ProgressRecorder::scope`].
struct Scoped<F> {
    recorder: ProgressRecorder,
    /// Taken once it is ready, and on drop, so it is dropped in scope too.
    future: Option<Pin<Box<F>>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let _scope = this.recorder.enter();
        let Some(future) = this.future.as_mut() else {
            return Poll::Pending;
        };
        let output = future.as_mut().poll(cx);
        if output.is_ready() {
            this.future = None;
        }
        output
    }
}

impl<F> Drop for Scoped<F> {
    fn drop(&mut self) {
        if self.future.is_some() {
            let _scope = self.recorder.enter();
            self.future = None;
        }
    }
}

// Not `Clone`: a clone dropping would release the claim of the original.
struct ProgressLayer {
    /// Held by the copy `progress_layer()` hands out; see `LayerClaim`.
    _claim: Option<LayerClaim>,
}

/// What the layer keeps on a span besides its fields.
struct SpanRoute {
    /// The recorder its events go to, fixed when it is created: its
    /// gateway's, for a gateway request's root span; else its parent span's;
    /// else the creating thread's; else the default.
    recorder: ProgressRecorder,
    /// The span's owner token, which the recorder keeps for the target the
    /// span stands for (`span_target_key`) while the span owns it.
    owner: u64,
}

/// The time a gateway binds the spans the current thread creates to its
/// recorder: see [`ProgressRecorder::own_new_spans`].
pub(crate) struct SpanOwnerScope {
    previous: Option<ProgressRecorder>,
    /// Not `Send`: it restores the owner of the thread that set it.
    _thread: PhantomData<*const ()>,
}

impl Drop for SpanOwnerScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        drop(SPAN_OWNER.try_with(|owner| owner.replace(previous)));
    }
}

/// The target a span stands for by its own fields, keyed as `record_event`
/// keys an event in it that names no target of its own: a gateway
/// request's.
fn span_target_key(span: &ProgressSpanFields) -> Option<String> {
    let none = ProgressFields::default();
    let id = progress_target_id(&none, span);
    (id != 0).then(|| format!("{}:{id}", progress_kind(&none, "", span)))
}

/// The "a recording layer is out" flag, released when the layer holding
/// it is dropped. A layer lives as long as its subscriber, so the claim
/// sticks once the subscriber is installed (a global subscriber is never
/// dropped) but comes back when the host discards the subscriber instead —
/// `try_init` failing, a throwaway or test subscriber — and the host's
/// next `progress_layer()` call records again.
struct LayerClaim(&'static AtomicBool);

impl LayerClaim {
    fn take(flag: &'static AtomicBool) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            .then_some(Self(flag))
    }
}

impl Drop for LayerClaim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl<S> Layer<S> for ProgressLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = ProgressFields::default();
        attrs.record(&mut fields);
        let span_fields = ProgressSpanFields {
            request_id: fields.get_u64("request_id"),
            progress_request_id: fields.get_u64("progress_request_id").filter(|id| *id != 0),
            parent_request_id: fields.get_u64("parent_request_id").filter(|id| *id != 0),
            top_level_path: fields
                .get("top_level_path")
                .filter(|path| !path.is_empty())
                .cloned(),
            namespace: fields.get("namespace").cloned(),
            path: fields.get("path").cloned(),
        };
        if let Some(span) = ctx.span(id) {
            // A gateway's request belongs to the gateway's node; any other
            // span to its parent's, or else to the thread's.
            let recorder = span_owner()
                .or_else(|| {
                    span.parent().and_then(|parent| {
                        parent
                            .extensions()
                            .get::<SpanRoute>()
                            .map(|route| route.recorder.clone())
                    })
                })
                .or_else(thread_recorder)
                .unwrap_or_else(default_recorder);
            let mut extensions = span.extensions_mut();
            extensions.insert(span_fields);
            extensions.insert(SpanRoute {
                recorder,
                owner: NEXT_SPAN_OWNER.fetch_add(1, Ordering::Relaxed),
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = ProgressFields::default();
        event.record(&mut fields);
        // The event's own span: its explicit parent, or else the current
        // span, unless it is a root event.
        let span = ctx.event_span(event);
        let extensions = span.as_ref().map(|span| span.extensions());
        let span_fields = extensions
            .as_ref()
            .and_then(|extensions| extensions.get::<ProgressSpanFields>().cloned())
            .unwrap_or_default();
        let route = extensions
            .as_ref()
            .and_then(|extensions| extensions.get::<SpanRoute>())
            .map(|route| (route.recorder.clone(), route.owner));
        drop(extensions);
        // Outside a span, by the thread.
        let (recorder, owner) = match route {
            Some((recorder, owner)) => (recorder, Some(owner)),
            None => (thread_recorder().unwrap_or_else(default_recorder), None),
        };
        recorder.record_event(span_fields, fields, owner);
        #[cfg(test)]
        tests::after_record();
    }

    /// A span closes once nothing holds it: its request's future, a
    /// streamed response's body, and the spans opened inside it. If it
    /// still owns the target it stands for then, no event ended it: the
    /// request was dropped first, that is cancelled. Its entry would
    /// otherwise stay active until the next `clear`, so its recorder closes
    /// it here, as cancelled, whichever thread closes the span. A target a
    /// newer span took over, or that was cleared, is not the closing span's
    /// to cancel.
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let extensions = span.extensions();
        let (Some(fields), Some(route)) = (
            extensions.get::<ProgressSpanFields>(),
            extensions.get::<SpanRoute>(),
        ) else {
            return;
        };
        let (fields, recorder, owner) = (fields.clone(), route.recorder.clone(), route.owner);
        drop(extensions);
        recorder.close_span(owner, fields);
    }
}

#[derive(Default)]
struct ProgressFields {
    values: HashMap<String, String>,
}

impl ProgressFields {
    fn get(&self, key: &str) -> Option<&String> {
        self.values.get(key)
    }

    fn get_u64(&self, key: &str) -> Option<u64> {
        self.get(key).and_then(|value| value.parse::<u64>().ok())
    }
}

impl Visit for ProgressFields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.values
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.values
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.values
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// The process's recorder for the events no node's recorder takes (see the
/// [module docs](self)).
pub fn default_recorder() -> ProgressRecorder {
    DEFAULT_RECORDER.get_or_init(ProgressRecorder::new).clone()
}

/// The `tracing` layer that feeds every [`ProgressRecorder`], each event
/// its own node's (see the [module docs](self)).
///
/// [`init_progress_tracing`] installs it as the process's global
/// subscriber, but that slot is process-wide and the first claim wins: a
/// host that links this crate next to another library that installs a
/// subscriber first (freedom-mobile-ffi, where ant-ffi's log subscriber
/// comes up before the IPFS node) must put this layer on its own
/// subscriber, alongside its other layers, before either library
/// initialises — otherwise the snapshot stays empty.
///
/// Filtered to itself: it only sees freedom-ipfs's spans and the events
/// that carry a `phase` (the only ones the recorder keeps), so sharing a
/// subscriber with a chattier library costs one cached callsite check
/// per foreign callsite, not a field walk per event.
///
/// Only per-layer filters leave it alone. The phase events are emitted at
/// `info`, so a *global* filter on the host subscriber — one added with
/// `.with(filter)` as a layer of its own, e.g.
/// `registry().with(EnvFilter::new("warn")).with(log).with(progress_layer())`
/// — disables them before this layer's filter is consulted, and the
/// snapshot stays empty. Attach the host's filter to its own layer
/// instead (`log.with_filter(EnvFilter::new("warn"))`), or make the global
/// filter admit `freedom_ipfs=info`.
///
/// Add it once. Every copy sends an event to the same recorder, so a
/// second copy would record each phase event twice; while the layer
/// from an earlier call is still alive, later calls therefore return a
/// layer that does nothing. Dropping that earlier layer (or the subscriber
/// it was added to, e.g. after a failed `try_init`) frees the slot again.
///
/// The choice is made when this function is called, not later: a layer
/// handed out as a no-op stays a no-op for its whole life, even after the
/// recording layer that blocked it is dropped. So when replacing a
/// subscriber, drop the old one (and any probe or test subscriber that
/// carries this layer) *before* calling `progress_layer()` for the new one
/// — `drop(old); let new = build();`, not `let new = build(); drop(old);`,
/// which leaves `new` with the no-op layer and the snapshot empty for good.
/// A subscriber installed with `set_global_default` / `try_init` is never
/// dropped, so its layer holds the slot for the rest of the process.
#[must_use]
pub fn progress_layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    progress_layer_once(&PROGRESS_LAYER_ISSUED)
}

/// The recording layer while `issued` is free, a no-op while an earlier
/// recording layer is still alive.
fn progress_layer_once<S>(issued: &'static AtomicBool) -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    LayerClaim::take(issued).map(|claim| progress_layer_with(Some(claim)))
}

/// A recording layer that holds no claim.
fn progress_layer_unclaimed<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    progress_layer_with(None)
}

fn progress_layer_with<S>(claim: Option<LayerClaim>) -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    ProgressLayer { _claim: claim }.with_filter(tracing_subscriber::filter::filter_fn(|meta| {
        meta.target().starts_with("freedom_ipfs")
            && (meta.is_span() || meta.fields().field("phase").is_some())
    }))
}

/// Installs a global subscriber that feeds only the progress recorders,
/// unless one is installed already: then, if that one carries
/// [`progress_layer`], the snapshots still fill. Once per process.
pub fn init_progress_tracing() {
    PROGRESS_TRACING_INIT.call_once(|| {
        // Not `progress_layer()`: that hands out the host's one copy, and
        // claiming it here would leave a host that installs its subscriber
        // after the node with a no-op layer. The two can't both be live —
        // only one global subscriber wins.
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        // Fails when the host already installed a subscriber; if that one
        // carries `progress_layer()` the snapshot still fills (see above).
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

fn progress_bytes_loaded(fields: &ProgressFields) -> Option<u64> {
    fields
        .get_u64("bytes")
        .or_else(|| fields.get_u64("body_len"))
}

fn progress_bytes_total(fields: &ProgressFields) -> Option<u64> {
    fields
        .get_u64("bytes_total")
        .or_else(|| fields.get_u64("file_len"))
        .or_else(|| fields.get_u64("body_len"))
}

fn progress_kind(fields: &ProgressFields, raw_phase: &str, span: &ProgressSpanFields) -> String {
    if fields.get("preload_id").is_some() || raw_phase.starts_with("preload_") {
        "preload".into()
    } else if span.request_id.is_some() || fields.get("request_id").is_some() {
        "gateway_request".into()
    } else if raw_phase.contains("provider") || raw_phase.contains("routing") {
        "provider_lookup".into()
    } else if raw_phase.contains("name") || raw_phase.contains("ipns") {
        "name_resolution".into()
    } else if fields.get("cid").is_some() {
        "block_fetch".into()
    } else {
        "event".into()
    }
}

fn progress_target_id(fields: &ProgressFields, span: &ProgressSpanFields) -> u64 {
    fields
        .get_u64("preload_id")
        .or_else(|| fields.get_u64("progress_request_id"))
        .or(span.progress_request_id)
        .or_else(|| fields.get_u64("request_id"))
        .or(span.request_id)
        .or_else(|| fields.get_u64("task_id"))
        .unwrap_or(0)
}

fn progress_status(raw_phase: &str, fields: &ProgressFields) -> String {
    match raw_phase {
        "gateway_stream_done" => "completed",
        "gateway_stream_failed" => "failed",
        "request_done" => match fields.get_u64("status") {
            Some(status)
                if status < 400
                    && fields.get("body_mode").map(String::as_str) == Some("stream") =>
            {
                "active"
            }
            Some(status) if status < 400 => "completed",
            Some(_) => "failed",
            None => "completed",
        },
        "preload_done" => match fields.get("ok").map(String::as_str) {
            Some("true") => "completed",
            Some("false") => "failed",
            _ => "completed",
        },
        "preload_cancelled" | REQUEST_CANCELLED => "cancelled",
        "gateway_limiter" if fields.get("acquired").map(String::as_str) == Some("false") => {
            "failed"
        }
        _ if fields.get("ok").map(String::as_str) == Some("false") => "active",
        _ => "active",
    }
    .into()
}

fn progress_source(raw_phase: &str, fields: &ProgressFields) -> Option<String> {
    if let Some(source) = fields.get("source") {
        return Some(source.clone());
    }
    match raw_phase {
        "block_store_get" | "gateway_conditional"
            if fields.get("cache_hit").map(String::as_str) == Some("true") =>
        {
            Some("cache".into())
        }
        "gateway_small_body_cache"
            if fields.get("cache_hit").map(String::as_str) == Some("true") =>
        {
            Some("cache".into())
        }
        "http_provider_fetch"
        | "http_provider_candidate_cancelled"
        | "http_provider_hedge"
        | "http_provider_race"
        | "http_provider_self_hedge_skip"
        | "http_provider_bitswap_hedge_skip"
        | "http_provider_race_result" => Some("http_provider".into()),
        "http_provider_bitswap_hedge" => Some("bitswap".into()),
        "http_provider_bitswap_hedge_result" => fields
            .get("source")
            .cloned()
            .or_else(|| Some("unknown".into())),
        "delegated_provider_lookup" | "delegated_provider_empty_retry" => {
            Some("delegated_routing".into())
        }
        "provider_diversity_low" | "light_dht_provider_lookup" | "dht_provider_lookup" => {
            Some("dht".into())
        }
        phase if phase.starts_with("bitswap_") => Some("bitswap".into()),
        _ => None,
    }
}

fn progress_phase(raw_phase: &str, fields: &ProgressFields, status: &str) -> String {
    match raw_phase {
        "request_start" | "preload_start" => "started",
        "request_done"
            if status == "active"
                && fields.get("body_mode").map(String::as_str) == Some("stream") =>
        {
            "streaming"
        }
        "request_done" if status == "completed" => "completed",
        "request_done" => "failed",
        "gateway_stream_done" => "completed",
        "gateway_stream_failed" => "failed",
        "preload_done" if status == "completed" => "completed",
        "preload_done" => "failed",
        "preload_cancelled" | REQUEST_CANCELLED => "cancelled",
        "block_store_get" if fields.get("cache_hit").map(String::as_str) == Some("true") => {
            "cache_hit"
        }
        "block_store_get" => "checking_cache",
        "gateway_small_body_cache"
            if fields.get("cache_hit").map(String::as_str) == Some("true") =>
        {
            "cache_hit"
        }
        "gateway_small_body_cache"
            if fields.get("cache_inserted").map(String::as_str) == Some("true") =>
        {
            "streaming"
        }
        "gateway_small_body_cache" => "checking_cache",
        "block_fetch_total" if fields.get("source").map(String::as_str) == Some("cache") => {
            "cache_hit"
        }
        "block_fetch_total" if fields.get("source").map(String::as_str) == Some("bitswap") => {
            "fetching_bitswap"
        }
        "block_fetch_total"
            if fields.get("source").map(String::as_str) == Some("http_provider") =>
        {
            "fetching_http_provider"
        }
        "block_range_batch_fetch" if fields.get("source").map(String::as_str) == Some("cache") => {
            "cache_hit"
        }
        "block_range_batch_fetch"
            if fields.get("source").map(String::as_str) == Some("bitswap") =>
        {
            "fetching_bitswap"
        }
        "block_range_batch_fetch"
            if fields.get("source").map(String::as_str) == Some("http_provider") =>
        {
            "fetching_http_provider"
        }
        "block_fetch_total" | "block_fetch_coalesced" | "block_range_batch_fetch" => "streaming",
        "name_cache" if fields.get("cache_hit").map(String::as_str) == Some("true") => {
            "name_resolved"
        }
        "name_cache" => "resolving_name",
        "name_persistent_cache" if fields.get("cache_hit").map(String::as_str) == Some("true") => {
            "name_resolved"
        }
        "name_persistent_cache" => "resolving_name",
        "name_resolve" if fields.get("ok").map(String::as_str) == Some("false") => "failed",
        "name_resolve" => "name_resolved",
        "provider_cache"
            if fields.get("cache_hit").map(String::as_str) == Some("true")
                && fields.get("provider_count").map(String::as_str) == Some("0") =>
        {
            "failed"
        }
        "provider_cache" if fields.get("cache_hit").map(String::as_str) == Some("true") => {
            "providers_found"
        }
        "provider_cache" => "provider_lookup",
        "provider_lookup" | "provider_refresh_skipped_empty_provider_set"
            if fields.get("error").is_some() =>
        {
            "failed"
        }
        "provider_lookup" => "providers_found",
        "provider_diversity_low" => "provider_diversity_low",
        "light_dht_provider_lookup" | "dht_provider_lookup" => "dht_fallback_started",
        "provider_fetch_start" => "providers_found",
        "delegated_provider_lookup"
        | "delegated_provider_empty_retry"
        | "bitswap_dns_prefetch"
        | "bitswap_dnsaddr_expand"
        | "bitswap_dns_multiaddr_expand" => "provider_lookup",
        "http_provider_fetch"
        | "http_provider_candidate_cancelled"
        | "http_provider_hedge"
        | "http_provider_race"
        | "http_provider_self_hedge_skip"
        | "http_provider_bitswap_hedge_skip"
        | "http_provider_race_result" => "fetching_http_provider",
        "http_provider_bitswap_hedge" => "fetching_bitswap",
        "http_provider_bitswap_hedge_result"
            if fields.get("source").map(String::as_str) == Some("bitswap") =>
        {
            "fetching_bitswap"
        }
        "http_provider_bitswap_hedge_result" => "fetching_http_provider",
        "bitswap_fetch"
        | "bitswap_connection_established"
        | "bitswap_incoming_block"
        | "bitswap_incoming_batch"
        | "bitswap_peer_attempt"
        | "bitswap_peer_attempt_cancelled"
        | "bitswap_peer_attempt_start"
        | "bitswap_peer_expand"
        | "bitswap_dial_plan"
        | "bitswap_session_shortcut"
        | "bitswap_session_shortcut_start"
        | "bitswap_session_shortcut_pre_lookup"
        | "bitswap_session_late_peer_wait"
        | "bitswap_session_shortcut_empty_providers_wait"
        | "bitswap_session_shortcut_post_lookup_wait"
        | "bitswap_session_shortcut_post_lookup_race" => "fetching_bitswap",
        "bitswap_fetch_cancelled" => "cancelled",
        "bitswap_request_timeout_detail"
        | "retry_provider_count"
        | "provider_retry_after_connection_timeout"
        | "bitswap_connection_error_backoff"
        | "bitswap_connection_error_peer_skipped" => "retrying",
        "bad_peer_skipped"
        | "bitswap_client_reset"
        | "bitswap_connection_error"
        | "bitswap_dial_rejected"
        | "bitswap_dial_waiters_dropped"
        | "bitswap_incoming_stream_read"
        | "bitswap_peer_timeout"
        | "bitswap_peer_timeout_suppressed"
        | "bitswap_provider_candidates_empty" => "retrying",
        "bitswap_request_timeout"
        | "provider_retry_after_timeout"
        | "provider_retry_after_request_timeout"
        | "provider_refresh_after_timeout"
        | "provider_refresh_after_failure" => "retrying",
        "ipfs_path_parse"
        | "gateway_direct_body"
        | "mime_total"
        | "mime_detect"
        | "mime_sniff_read"
        | "unixfs_resource"
        | "unixfs_metadata_cache"
        | "unixfs_file_size"
        | "unixfs_index_lookup"
        | "unixfs_list_directory" => "streaming",
        "gateway_conditional" => "cache_hit",
        "gateway_limiter" if fields.get("acquired").map(String::as_str) == Some("false") => {
            "failed"
        }
        "gateway_limiter" => "queued",
        _ => raw_phase,
    }
    .into()
}

fn progress_error_code(raw_phase: &str, fields: &ProgressFields, status: &str) -> Option<String> {
    if raw_phase == "request_done" && status == "failed" {
        return fields.get("status").map(|status| format!("http_{status}"));
    }
    if raw_phase == "gateway_stream_failed" {
        return Some("gateway_stream_failed".into());
    }
    if raw_phase == "gateway_limiter" && fields.get("acquired").map(String::as_str) == Some("false")
    {
        return Some("gateway_busy".into());
    }
    if raw_phase == "bitswap_incoming_stream_read"
        && fields.get("ok").map(String::as_str) == Some("false")
    {
        return Some("bitswap_incoming_stream_read".into());
    }
    if fields.get("error").is_some() {
        Some(raw_phase.to_string())
    } else {
        None
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        /// Runs on this thread after its next event is recorded, before the
        /// layer returns: where per-span bookkeeping outside the recorder's
        /// lock would race with another thread's event.
        static AFTER_RECORD: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    pub(super) fn after_record() {
        if let Some(hook) = AFTER_RECORD.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    #[test]
    fn progress_snapshot_accumulates_target_counters() {
        let recorder = ProgressRecorder::default();
        let span = ProgressSpanFields {
            request_id: Some(1),
            ..ProgressSpanFields::default()
        };

        recorder.record_event(
            span.clone(),
            progress_fields([("phase", "request_start"), ("request_id", "1")]),
            None,
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "bitswap_connection_error"),
                ("request_id", "1"),
                ("error", "timeout"),
            ]),
            None,
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "block_fetch_total"),
                ("request_id", "1"),
                ("source", "bitswap"),
            ]),
            None,
        );
        recorder.record_event(
            span,
            progress_fields([
                ("phase", "request_done"),
                ("request_id", "1"),
                ("status", "200"),
            ]),
            None,
        );

        let snapshot = recorder.snapshot_json();
        let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(value["active_count"].as_u64().unwrap(), 0);
        let events = value["events"].as_array().unwrap();
        let retrying = events
            .iter()
            .find(|event| event["phase"] == "retrying")
            .unwrap();
        assert_eq!(retrying["source"], "bitswap");
        let completed = events
            .iter()
            .find(|event| event["phase"] == "completed")
            .unwrap();
        assert_eq!(completed["source"], "bitswap");
        assert_eq!(completed["blocks_loaded"].as_u64().unwrap(), 1);
        assert_eq!(completed["retry_count"].as_u64().unwrap(), 1);
    }

    #[test]
    fn progress_snapshot_records_stream_body_bytes() {
        let recorder = ProgressRecorder::default();
        let span = ProgressSpanFields {
            request_id: Some(1),
            progress_request_id: Some(4242),
            path: Some("/ipfs/root".into()),
            ..ProgressSpanFields::default()
        };

        recorder.record_event(
            span.clone(),
            progress_fields([("phase", "request_start")]),
            None,
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "unixfs_resource"),
                ("resource", "file"),
                ("file_len", "600000"),
            ]),
            None,
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "request_done"),
                ("status", "200"),
                ("body_mode", "stream"),
            ]),
            None,
        );
        recorder.record_event(
            span,
            progress_fields([
                ("phase", "gateway_stream_done"),
                ("body_len", "600000"),
                ("chunks", "10"),
            ]),
            None,
        );

        let snapshot = recorder.snapshot_json();
        let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(value["active_count"].as_u64().unwrap(), 0);
        let events = value["events"].as_array().unwrap();
        let response_ready = events
            .iter()
            .find(|event| event["raw_phase"] == "request_done")
            .unwrap();
        assert_eq!(response_ready["phase"], "streaming");
        assert_eq!(response_ready["status"], "active");
        assert_eq!(response_ready["bytes_total"].as_u64().unwrap(), 600000);
        let completed = events
            .iter()
            .find(|event| event["raw_phase"] == "gateway_stream_done")
            .unwrap();
        assert_eq!(completed["target_id"].as_u64().unwrap(), 4242);
        assert_eq!(completed["path"], "/ipfs/root");
        assert_eq!(completed["phase"], "completed");
        assert_eq!(completed["status"], "completed");
        assert_eq!(completed["bytes_loaded"].as_u64().unwrap(), 600000);
        assert_eq!(completed["bytes_total"].as_u64().unwrap(), 600000);
    }

    #[test]
    fn progress_snapshot_counts_active_subrequests() {
        let recorder = ProgressRecorder::default();
        let root_span = ProgressSpanFields {
            request_id: Some(1),
            progress_request_id: Some(100),
            path: Some("/ipns/site/".into()),
            ..ProgressSpanFields::default()
        };
        let child_span = ProgressSpanFields {
            request_id: Some(2),
            progress_request_id: Some(101),
            parent_request_id: Some(100),
            path: Some("/ipns/site/app.js".into()),
            ..ProgressSpanFields::default()
        };
        let second_child_span = ProgressSpanFields {
            request_id: Some(3),
            progress_request_id: Some(102),
            parent_request_id: Some(100),
            path: Some("/ipns/site/app.css".into()),
            ..ProgressSpanFields::default()
        };

        recorder.record_event(
            root_span,
            progress_fields([("phase", "request_start")]),
            None,
        );
        recorder.record_event(
            child_span,
            progress_fields([("phase", "request_start")]),
            None,
        );
        recorder.record_event(
            second_child_span,
            progress_fields([("phase", "request_start")]),
            None,
        );

        let snapshot = recorder.snapshot_json();
        let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(value["active_count"].as_u64().unwrap(), 3);
        let active = value["active"].as_array().unwrap();
        let root = active.iter().find(|target| target["id"] == 100).unwrap();
        assert_eq!(root["active_subrequests"].as_u64().unwrap(), 2);
        let child = active.iter().find(|target| target["id"] == 101).unwrap();
        assert_eq!(child["parent_id"].as_u64().unwrap(), 100);
        assert_eq!(child["active_subrequests"].as_u64().unwrap(), 0);
    }

    #[test]
    fn progress_phase_maps_trace_events_to_ui_states() {
        assert_eq!(
            progress_source(
                "bitswap_fetch",
                &progress_fields([("phase", "bitswap_fetch"), ("bitswap_delivery", "incoming")]),
            )
            .as_deref(),
            Some("bitswap")
        );
        assert_eq!(
            progress_source(
                "delegated_provider_lookup",
                &progress_fields([("phase", "delegated_provider_lookup")]),
            )
            .as_deref(),
            Some("delegated_routing")
        );
        assert_eq!(
            progress_source(
                "delegated_provider_empty_retry",
                &progress_fields([("phase", "delegated_provider_empty_retry")]),
            )
            .as_deref(),
            Some("delegated_routing")
        );
        assert_eq!(
            progress_source(
                "http_provider_race",
                &progress_fields([("phase", "http_provider_race")]),
            )
            .as_deref(),
            Some("http_provider")
        );
        assert_eq!(
            progress_source(
                "http_provider_hedge",
                &progress_fields([("phase", "http_provider_hedge")]),
            )
            .as_deref(),
            Some("http_provider")
        );
        assert_eq!(
            progress_source(
                "http_provider_bitswap_hedge",
                &progress_fields([("phase", "http_provider_bitswap_hedge")]),
            )
            .as_deref(),
            Some("bitswap")
        );
        assert_eq!(
            progress_source(
                "http_provider_self_hedge_skip",
                &progress_fields([("phase", "http_provider_self_hedge_skip")]),
            )
            .as_deref(),
            Some("http_provider")
        );
        assert_eq!(
            progress_source(
                "http_provider_bitswap_hedge_skip",
                &progress_fields([("phase", "http_provider_bitswap_hedge_skip")]),
            )
            .as_deref(),
            Some("http_provider")
        );
        assert_eq!(
            progress_source(
                "http_provider_bitswap_hedge_result",
                &progress_fields([
                    ("phase", "http_provider_bitswap_hedge_result"),
                    ("source", "bitswap")
                ]),
            )
            .as_deref(),
            Some("bitswap")
        );
        assert_eq!(
            progress_source(
                "gateway_small_body_cache",
                &progress_fields([("phase", "gateway_small_body_cache"), ("cache_hit", "true")]),
            )
            .as_deref(),
            Some("cache")
        );
        assert_eq!(
            progress_phase(
                "name_cache",
                &progress_fields([("phase", "name_cache"), ("cache_hit", "false")]),
                "active",
            ),
            "resolving_name"
        );
        assert_eq!(
            progress_phase(
                "name_resolve",
                &progress_fields([("phase", "name_resolve"), ("ok", "true")]),
                "active",
            ),
            "name_resolved"
        );
        assert_eq!(
            progress_phase(
                "name_persistent_cache",
                &progress_fields([("phase", "name_persistent_cache"), ("cache_hit", "true")]),
                "active",
            ),
            "name_resolved"
        );
        assert_eq!(
            progress_phase(
                "provider_cache",
                &progress_fields([("phase", "provider_cache"), ("cache_hit", "false")]),
                "active",
            ),
            "provider_lookup"
        );
        assert_eq!(
            progress_phase(
                "provider_cache",
                &progress_fields([
                    ("phase", "provider_cache"),
                    ("cache_hit", "true"),
                    ("provider_count", "0")
                ]),
                "active",
            ),
            "failed"
        );
        assert_eq!(
            progress_phase(
                "delegated_provider_lookup",
                &progress_fields([
                    ("phase", "delegated_provider_lookup"),
                    ("provider_count", "8")
                ]),
                "active",
            ),
            "provider_lookup"
        );
        assert_eq!(
            progress_phase(
                "delegated_provider_empty_retry",
                &progress_fields([
                    ("phase", "delegated_provider_empty_retry"),
                    ("provider_count", "1")
                ]),
                "active",
            ),
            "provider_lookup"
        );
        assert_eq!(
            progress_phase(
                "bitswap_dnsaddr_expand",
                &progress_fields([("phase", "bitswap_dnsaddr_expand"), ("record_count", "2")]),
                "active",
            ),
            "provider_lookup"
        );
        assert_eq!(
            progress_phase(
                "bitswap_dns_prefetch",
                &progress_fields([
                    ("phase", "bitswap_dns_prefetch"),
                    ("dns_ip_host_count", "4")
                ]),
                "active",
            ),
            "provider_lookup"
        );
        assert_eq!(
            progress_phase(
                "bitswap_peer_expand",
                &progress_fields([("phase", "bitswap_peer_expand"), ("peer_count", "3")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "bitswap_connection_established",
                &progress_fields([("phase", "bitswap_connection_established")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "bitswap_incoming_batch",
                &progress_fields([("phase", "bitswap_incoming_batch")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "bitswap_session_shortcut_pre_lookup",
                &progress_fields([("phase", "bitswap_session_shortcut_pre_lookup")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "bitswap_session_shortcut_post_lookup_race",
                &progress_fields([("phase", "bitswap_session_shortcut_post_lookup_race")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "bitswap_session_shortcut_empty_providers_wait",
                &progress_fields([("phase", "bitswap_session_shortcut_empty_providers_wait")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "unixfs_resource",
                &progress_fields([("phase", "unixfs_resource")]),
                "active",
            ),
            "streaming"
        );
        assert_eq!(
            progress_phase(
                "gateway_direct_body",
                &progress_fields([("phase", "gateway_direct_body"), ("body_len", "4096")]),
                "active",
            ),
            "streaming"
        );
        assert_eq!(
            progress_phase(
                "gateway_small_body_cache",
                &progress_fields([
                    ("phase", "gateway_small_body_cache"),
                    ("cache_hit", "true"),
                    ("body_len", "4096")
                ]),
                "active",
            ),
            "cache_hit"
        );
        assert_eq!(
            progress_phase(
                "gateway_small_body_cache",
                &progress_fields([
                    ("phase", "gateway_small_body_cache"),
                    ("cache_hit", "false")
                ]),
                "active",
            ),
            "checking_cache"
        );
        assert_eq!(
            progress_phase(
                "gateway_small_body_cache",
                &progress_fields([
                    ("phase", "gateway_small_body_cache"),
                    ("cache_inserted", "true")
                ]),
                "active",
            ),
            "streaming"
        );
        assert_eq!(
            progress_phase(
                "request_done",
                &progress_fields([
                    ("phase", "request_done"),
                    ("status", "200"),
                    ("body_mode", "stream")
                ]),
                "active",
            ),
            "streaming"
        );
        assert_eq!(
            progress_phase(
                "gateway_stream_done",
                &progress_fields([
                    ("phase", "gateway_stream_done"),
                    ("body_len", "600000"),
                    ("chunks", "3")
                ]),
                "completed",
            ),
            "completed"
        );
        assert_eq!(
            progress_phase(
                "gateway_stream_failed",
                &progress_fields([
                    ("phase", "gateway_stream_failed"),
                    ("error", "missing block")
                ]),
                "failed",
            ),
            "failed"
        );
        assert_eq!(
            progress_phase(
                "gateway_conditional",
                &progress_fields([
                    ("phase", "gateway_conditional"),
                    ("outcome", "not_modified")
                ]),
                "active",
            ),
            "cache_hit"
        );
        assert_eq!(
            progress_phase(
                "block_fetch_total",
                &progress_fields([("phase", "block_fetch_total"), ("source", "http_provider")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "block_range_batch_fetch",
                &progress_fields([
                    ("phase", "block_range_batch_fetch"),
                    ("source", "bitswap"),
                    ("range_len", "32768")
                ]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "block_range_batch_fetch",
                &progress_fields([
                    ("phase", "block_range_batch_fetch"),
                    ("source", "http_provider"),
                    ("range_len", "32768")
                ]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_race",
                &progress_fields([("phase", "http_provider_race")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_candidate_cancelled",
                &progress_fields([("phase", "http_provider_candidate_cancelled")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_hedge",
                &progress_fields([("phase", "http_provider_hedge")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_bitswap_hedge",
                &progress_fields([("phase", "http_provider_bitswap_hedge")]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "http_provider_self_hedge_skip",
                &progress_fields([("phase", "http_provider_self_hedge_skip")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_bitswap_hedge_skip",
                &progress_fields([("phase", "http_provider_bitswap_hedge_skip")]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "http_provider_bitswap_hedge_result",
                &progress_fields([
                    ("phase", "http_provider_bitswap_hedge_result"),
                    ("source", "bitswap")
                ]),
                "active",
            ),
            "fetching_bitswap"
        );
        assert_eq!(
            progress_phase(
                "http_provider_bitswap_hedge_result",
                &progress_fields([
                    ("phase", "http_provider_bitswap_hedge_result"),
                    ("source", "http_provider")
                ]),
                "active",
            ),
            "fetching_http_provider"
        );
        assert_eq!(
            progress_phase(
                "bitswap_connection_error",
                &progress_fields([("phase", "bitswap_connection_error"), ("error", "timeout")]),
                "active",
            ),
            "retrying"
        );
        assert_eq!(
            progress_phase(
                "bitswap_dial_waiters_dropped",
                &progress_fields([
                    ("phase", "bitswap_dial_waiters_dropped"),
                    ("waiter_count", "2")
                ]),
                "active",
            ),
            "retrying"
        );
        assert_eq!(
            progress_phase(
                "bitswap_incoming_stream_read",
                &progress_fields([
                    ("phase", "bitswap_incoming_stream_read"),
                    ("ok", "false"),
                    ("timed_out", "true")
                ]),
                "active",
            ),
            "retrying"
        );
        assert_eq!(
            progress_phase(
                "provider_retry_after_connection_timeout",
                &progress_fields([("phase", "provider_retry_after_connection_timeout")]),
                "active",
            ),
            "retrying"
        );
        assert_eq!(
            progress_phase(
                "provider_refresh_skipped_empty_provider_set",
                &progress_fields([
                    ("phase", "provider_refresh_skipped_empty_provider_set"),
                    ("error", "no bitswap providers")
                ]),
                "active",
            ),
            "failed"
        );
    }

    #[test]
    fn progress_error_code_marks_incoming_stream_read_failures() {
        assert_eq!(
            progress_error_code(
                "bitswap_incoming_stream_read",
                &progress_fields([
                    ("phase", "bitswap_incoming_stream_read"),
                    ("ok", "false"),
                    ("timed_out", "true")
                ]),
                "active",
            ),
            Some("bitswap_incoming_stream_read".into())
        );
    }

    /// A host that owns the global subscriber (freedom-mobile-ffi, next
    /// to ant-ffi's log layer) must still fill the snapshot through
    /// `progress_layer`, whatever the other layers filter.
    #[test]
    fn progress_layer_records_when_composed_with_other_layers() {
        let recorder = ProgressRecorder::new();
        let quiet_logs = tracing_subscriber::fmt::layer()
            .with_writer(std::io::sink)
            .with_filter(tracing_subscriber::filter::LevelFilter::ERROR);
        let subscriber = Registry::default()
            .with(quiet_logs)
            .with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = tracing::info_span!(
                target: "freedom_ipfs_gateway",
                "gateway_request",
                request_id = 9191u64,
                path = "/ipfs/bafycomposed"
            );
            let _entered = span.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
            // Another library's event, even with a `phase`, isn't ours.
            tracing::info!(target: "ant_retrieval", phase = "request_start", request_id = 1u64);
        });
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        let events = value["events"].as_array().unwrap();
        // The request never ended, so closing its span cancelled it.
        assert_eq!(
            raw_phases(&recorder),
            ["request_start", "request_cancelled"],
            "{value}"
        );
        assert_eq!(events[0]["target_id"], 9191, "{value}");
        assert_eq!(events[0]["path"], "/ipfs/bafycomposed", "{value}");
    }

    /// Adding the layer twice (directly and via a helper) must not record
    /// every phase event twice in the recorder it routes them to.
    #[test]
    fn progress_layer_added_twice_records_each_event_once() {
        static ISSUED: AtomicBool = AtomicBool::new(false);
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default()
            .with(progress_layer_once(&ISSUED))
            .with(progress_layer_once(&ISSUED));
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = tracing::info_span!(
                target: "freedom_ipfs_gateway",
                "gateway_request",
                request_id = 4343u64,
                path = "/ipfs/bafytwice"
            );
            let _entered = span.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
        });
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        let events = value["events"].as_array().unwrap();
        assert_eq!(
            raw_phases(&recorder),
            ["request_start", "request_cancelled"],
            "{value}"
        );
        assert_eq!(events[0]["target_id"], 4343, "{value}");
    }

    /// A layer the host builds but throws away (its `try_init` failed, or it
    /// was a throwaway subscriber) must not use up the one recording copy:
    /// the next call, for the subscriber that does get installed, records.
    #[test]
    fn progress_layer_dropped_unused_frees_the_recording_copy() {
        static ISSUED: AtomicBool = AtomicBool::new(false);
        let recorder = ProgressRecorder::new();
        let throwaway = Registry::default().with(progress_layer_once(&ISSUED));
        drop(throwaway);
        let subscriber = Registry::default().with(progress_layer_once(&ISSUED));
        // While the installed one is alive, another copy is still a no-op.
        let extra = Registry::default().with(progress_layer_once(&ISSUED));
        tracing::subscriber::with_default(extra, || {
            let _scope = recorder.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
        });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&recorder.snapshot_json()).unwrap()["events"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = tracing::info_span!(
                target: "freedom_ipfs_gateway",
                "gateway_request",
                request_id = 4545u64,
                path = "/ipfs/bafyretry"
            );
            let _entered = span.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
        });
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        let events = value["events"].as_array().unwrap();
        assert_eq!(
            raw_phases(&recorder),
            ["request_start", "request_cancelled"],
            "{value}"
        );
        assert_eq!(events[0]["target_id"], 4545, "{value}");
    }

    /// Pins the ordering the `progress_layer` doc warns about: a layer built
    /// while the recording copy is alive stays a no-op after that copy is
    /// dropped; building after the drop gets the recording copy.
    #[test]
    fn progress_layer_built_before_old_is_dropped_stays_a_no_op() {
        static ISSUED: AtomicBool = AtomicBool::new(false);
        let recorder = ProgressRecorder::new();
        let emit = |id: u64| {
            let _scope = recorder.enter();
            let span = tracing::info_span!(
                target: "freedom_ipfs_gateway",
                "gateway_request",
                request_id = id,
                path = "/ipfs/bafyrebuild"
            );
            let _entered = span.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
        };
        let events = |r: &ProgressRecorder| {
            raw_phases(r)
                .iter()
                .filter(|raw_phase| *raw_phase == "request_start")
                .count()
        };
        let old = Registry::default().with(progress_layer_once(&ISSUED));
        let too_early = Registry::default().with(progress_layer_once(&ISSUED));
        drop(old);
        tracing::subscriber::with_default(too_early, || emit(1));
        assert_eq!(events(&recorder), 0);
        let rebuilt = Registry::default().with(progress_layer_once(&ISSUED));
        tracing::subscriber::with_default(rebuilt, || emit(2));
        assert_eq!(events(&recorder), 1);
    }

    /// A host whose filter sits on its own log layer, not on the whole
    /// subscriber, leaves the info-level phase events to `progress_layer`.
    #[test]
    fn progress_layer_records_next_to_a_per_layer_warn_filter() {
        let recorder = ProgressRecorder::new();
        let logs = tracing_subscriber::fmt::layer()
            .with_writer(std::io::sink)
            .with_filter(tracing_subscriber::filter::LevelFilter::WARN);
        let subscriber = Registry::default()
            .with(logs)
            .with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 7u64);
        });
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        assert_eq!(value["events"].as_array().unwrap().len(), 1, "{value}");
    }

    fn request_span(request_id: u64, progress_request_id: u64) -> tracing::Span {
        tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            request_id,
            progress_request_id,
            path = "/ipfs/bafycancel"
        )
    }

    /// The events `recorder` holds, as (target, raw phase, phase, status).
    fn recorded(recorder: &ProgressRecorder) -> Vec<(u64, String, String, String)> {
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        value["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| {
                (
                    event["target_id"].as_u64().unwrap(),
                    event["raw_phase"].as_str().unwrap().into(),
                    event["phase"].as_str().unwrap().into(),
                    event["status"].as_str().unwrap().into(),
                )
            })
            .collect()
    }

    fn raw_phases(recorder: &ProgressRecorder) -> Vec<String> {
        recorded(recorder)
            .into_iter()
            .map(|(_, raw_phase, _, _)| raw_phase)
            .collect()
    }

    fn event(
        target: u64,
        raw_phase: &str,
        phase: &str,
        status: &str,
    ) -> (u64, String, String, String) {
        (target, raw_phase.into(), phase.into(), status.into())
    }

    /// A request whose span closes before any event ended its target was
    /// dropped: its entry leaves `active` as cancelled, keeping what the
    /// target had.
    #[test]
    fn a_span_closed_with_its_target_open_closes_it_as_cancelled() {
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = request_span(1, 900);
            span.in_scope(|| {
                tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
                tracing::info!(
                    target: "freedom_ipfs_retrieval",
                    phase = "block_fetch_total",
                    source = "bitswap",
                    bytes = 4096u64
                );
            });
            assert_eq!(recorder.snapshot().active_count, 1);
            drop(span);
        });
        let value: serde_json::Value = serde_json::from_str(&recorder.snapshot_json()).unwrap();
        assert_eq!(value["active_count"], 0, "{value}");
        let last = value["events"].as_array().unwrap().last().unwrap().clone();
        assert_eq!(last["raw_phase"], "request_cancelled", "{value}");
        assert_eq!(last["phase"], "cancelled", "{value}");
        assert_eq!(last["status"], "cancelled", "{value}");
        assert_eq!(last["kind"], "gateway_request", "{value}");
        assert_eq!(last["target_id"], 900, "{value}");
        assert_eq!(last["request_id"], 1, "{value}");
        assert_eq!(last["path"], "/ipfs/bafycancel", "{value}");
        assert_eq!(last["source"], "bitswap", "{value}");
        assert_eq!(last["bytes_loaded"], 4096, "{value}");
        assert_eq!(last["blocks_loaded"], 1, "{value}");
    }

    /// A span whose events ended its target closes without a word: a
    /// completed request, a failed one, a streamed one whose stream ended,
    /// and one whose target a newer request with its id holds now.
    #[test]
    fn a_span_whose_target_ended_closes_quietly() {
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let emit = |span: &tracing::Span, phases: &[(&str, u16, &str)]| {
                span.in_scope(|| {
                    for &(phase, status, body_mode) in phases {
                        tracing::info!(
                            target: "freedom_ipfs_gateway",
                            phase,
                            status,
                            body_mode
                        );
                    }
                })
            };
            let completed = request_span(1, 910);
            emit(
                &completed,
                &[("request_start", 0, ""), ("request_done", 200, "direct")],
            );
            drop(completed);
            let failed = request_span(2, 911);
            emit(
                &failed,
                &[("request_start", 0, ""), ("request_done", 404, "direct")],
            );
            drop(failed);
            let streamed = request_span(3, 912);
            emit(
                &streamed,
                &[
                    ("request_start", 0, ""),
                    ("request_done", 200, "stream"),
                    ("gateway_stream_done", 0, ""),
                ],
            );
            drop(streamed);
            // The first ends, a second takes the id, then the first closes.
            let first = request_span(4, 913);
            emit(
                &first,
                &[("request_start", 0, ""), ("request_done", 200, "direct")],
            );
            let second = request_span(5, 913);
            emit(&second, &[("request_start", 0, "")]);
            drop(first);
            assert_eq!(recorder.snapshot().active_count, 1);
            emit(&second, &[("request_done", 200, "direct")]);
            drop(second);
        });
        assert_eq!(recorder.snapshot().active_count, 0);
        assert!(
            recorded(&recorder)
                .iter()
                .all(|(_, raw_phase, _, _)| raw_phase != "request_cancelled"),
            "{:?}",
            recorded(&recorder)
        );
    }

    /// A streamed response's target stays open while its body lives, and is
    /// cancelled when the body is dropped before its stream ends.
    #[test]
    fn a_streamed_body_dropped_before_its_end_cancels_its_target() {
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = request_span(1, 920);
            span.in_scope(|| {
                tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
                tracing::info!(
                    target: "freedom_ipfs_gateway",
                    phase = "request_done",
                    status = 200u16,
                    body_mode = "stream"
                );
            });
            // The body holds the span, as the gateway's stream does.
            let body = span.clone();
            drop(span);
            assert_eq!(recorder.snapshot().active_count, 1);
            body.in_scope(|| {
                tracing::info!(target: "freedom_ipfs_gateway", phase = "block_fetch_total", source = "cache");
            });
            drop(body);
        });
        assert_eq!(
            recorded(&recorder),
            [
                event(920, "request_start", "started", "active"),
                event(920, "request_done", "streaming", "active"),
                event(920, "block_fetch_total", "cache_hit", "active"),
                event(920, "request_cancelled", "cancelled", "cancelled"),
            ]
        );
    }

    /// A target the host cleared stays cleared: closing its span adds no
    /// event for it.
    #[test]
    fn a_cleared_target_is_not_cancelled_when_its_span_closes() {
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        tracing::subscriber::with_default(subscriber, || {
            let _scope = recorder.enter();
            let span = request_span(1, 930);
            span.in_scope(|| {
                tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
            });
            recorder.clear();
            drop(span);
        });
        assert_eq!(recorded(&recorder), []);
    }

    /// The cancel goes to the recorder the target is open in, whatever
    /// thread closes the span.
    #[test]
    fn a_span_closed_on_another_thread_cancels_in_its_recorder() {
        let recorder = ProgressRecorder::new();
        let subscriber = Registry::default().with(progress_layer_unclaimed());
        let dispatch = tracing::Dispatch::new(subscriber);
        let span = tracing::dispatcher::with_default(&dispatch, || {
            let _scope = recorder.enter();
            let span = request_span(1, 940);
            span.in_scope(|| {
                tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start");
            });
            span
        });
        std::thread::scope(|threads| {
            threads.spawn(|| tracing::dispatcher::with_default(&dispatch, || drop(span)));
        });
        assert_eq!(
            recorded(&recorder),
            [
                event(940, "request_start", "started", "active"),
                event(940, "request_cancelled", "cancelled", "cancelled"),
            ]
        );
    }

    /// The schedule that orphaned targets while per-span bookkeeping ran
    /// outside the recorder's lock: a terminal event is recorded and its
    /// thread pauses before the layer returns; meanwhile another event of
    /// the same span re-opens the target. Once the span closes, nothing may
    /// stay active, for any number of spans (the 512-event cap doesn't
    /// bound active targets).
    #[test]
    fn codex_concurrent_terminal_and_progress_cannot_leave_an_orphan() {
        let recorder = ProgressRecorder::new();
        let dispatch = tracing::Dispatch::new(Registry::default().with(progress_layer_unclaimed()));
        let wait = std::time::Duration::from_secs(5);
        for request_id in 1u64..=600 {
            let span = tracing::dispatcher::with_default(&dispatch, || {
                let _scope = recorder.enter();
                let span = tracing::info_span!(
                    target: "freedom_ipfs_gateway",
                    parent: None,
                    "gateway_request",
                    request_id
                );
                span.in_scope(
                    || tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start"),
                );
                span
            });
            let (paused_tx, paused) = std::sync::mpsc::channel();
            let (release, held) = std::sync::mpsc::channel::<()>();
            let terminal = {
                let (span, dispatch) = (span.clone(), dispatch.clone());
                std::thread::spawn(move || {
                    AFTER_RECORD.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move || {
                            paused_tx.send(()).unwrap();
                            held.recv_timeout(wait).unwrap();
                        }))
                    });
                    tracing::dispatcher::with_default(&dispatch, || {
                        span.in_scope(|| {
                            tracing::info!(
                                target: "freedom_ipfs_gateway",
                                phase = "request_done",
                                status = 200u16,
                                body_mode = "direct"
                            )
                        })
                    });
                })
            };
            paused.recv_timeout(wait).unwrap();
            tracing::dispatcher::with_default(&dispatch, || {
                span.in_scope(|| {
                    tracing::info!(target: "freedom_ipfs_gateway", phase = "block_fetch_total", source = "cache")
                })
            });
            release.send(()).unwrap();
            terminal.join().unwrap();
            drop(span);
        }
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.active_count, 0, "{}", recorder.snapshot_json());
        assert_eq!(snapshot.event_count, MAX_PROGRESS_EVENTS);
        assert!(recorder.inner.lock().unwrap().owners.is_empty());
    }

    fn progress_fields(
        values: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ProgressFields {
        ProgressFields {
            values: values
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }
}
