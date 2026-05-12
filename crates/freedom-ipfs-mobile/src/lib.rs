use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use freedom_ipfs_core::{parse_cid, BlockProvider};
use freedom_ipfs_gateway::{GatewayCore, GatewayCoreRequest};
use freedom_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsResolver, NameResolver,
};
use freedom_ipfs_retrieval::FetchingBlockProvider;
use freedom_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, RoutingStatsHandle, DEFAULT_DELEGATED_ROUTER,
};
use freedom_ipfs_store::SqliteBlockStore;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::{c_char, CStr, CString};
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Once, OnceLock};
use std::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::field::{Field, Visit};
use tracing::{Event, Id, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const LOW_MEMORY_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const CACHE_DB_FILE: &str = "freedom-ipfs.sqlite3";
const ROUTING_MODE_AUTO: u32 = 0;
const ROUTING_MODE_DELEGATED: u32 = 1;
const ROUTING_MODE_LIGHT_DHT: u32 = 2;
const ROUTING_MODE_OFFLINE: u32 = 3;
const PRELOAD_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_PROGRESS_EVENTS: usize = 512;
const NATIVE_BODY_CHANNEL_CAPACITY: usize = 2;
const X_FREEDOM_REQUEST_ID: &str = "X-Freedom-Request-ID";
const X_FREEDOM_PARENT_REQUEST_ID: &str = "X-Freedom-Parent-Request-ID";
const X_FREEDOM_TOP_LEVEL_PATH: &str = "X-Freedom-Top-Level-Path";

const FREEDOM_IPFS_GATEWAY_READ_PENDING: u32 = 0;
const FREEDOM_IPFS_GATEWAY_READ_BYTES: u32 = 1;
const FREEDOM_IPFS_GATEWAY_READ_END: u32 = 2;
const FREEDOM_IPFS_GATEWAY_READ_CANCELLED: u32 = 3;
const FREEDOM_IPFS_GATEWAY_READ_FAILED: u32 = 4;
const FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE: u32 = 5;

const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK: u32 = 0;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT: u32 = 1;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE: u32 = 2;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED: u32 = 3;

const FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY: u32 = 1 << 0;
const FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY: u32 = 1 << 1;
const FREEDOM_IPFS_GATEWAY_EVENT_END: u32 = 1 << 2;
const FREEDOM_IPFS_GATEWAY_EVENT_FAILED: u32 = 1 << 3;
const FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED: u32 = 1 << 4;
const FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED: u32 = 1 << 5;

static PROGRESS_RECORDER: OnceLock<Arc<ProgressRecorder>> = OnceLock::new();
static PROGRESS_TRACING_INIT: Once = Once::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleState {
    Foreground,
    Background,
}

pub struct FreedomIpfsNode {
    runtime: Runtime,
    store: SqliteBlockStore,
    gateway_addr: Mutex<Option<SocketAddr>>,
    gateway_task: Mutex<Option<JoinHandle<()>>>,
    retrieval_stats_provider: Mutex<Option<FetchingBlockProvider>>,
    routing_stats: Mutex<Option<RoutingStatsHandle>>,
    lifecycle_state: Mutex<LifecycleState>,
    next_preload_id: AtomicU64,
    preload_tasks: Mutex<HashMap<u64, JoinHandle<()>>>,
    native_gateway_state: Mutex<NativeGatewayState>,
    native_gateway_events: Arc<NativeGatewayEventMux>,
    next_native_request_id: AtomicU64,
}

#[repr(C)]
pub struct FreedomIpfsBuffer {
    pub data: *mut u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreedomIpfsGatewayReadResult {
    pub status: u32,
    pub bytes_read: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreedomIpfsGatewayEvent {
    pub status: u32,
    pub events: u32,
    pub request_handle: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreedomIpfsRetrievalStats {
    pub cache_hits: u64,
    pub http_provider_blocks: u64,
    pub bitswap_blocks: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreedomIpfsRoutingStats {
    pub delegated_provider_lookups: u64,
    pub delegated_provider_results: u64,
    pub delegated_provider_errors: u64,
    pub dht_provider_lookups: u64,
    pub dht_provider_results: u64,
    pub dht_provider_errors: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreedomIpfsDiagnostics {
    pub block_count: u64,
    pub total_bytes: u64,
    pub cache_hits: u64,
    pub http_provider_blocks: u64,
    pub bitswap_blocks: u64,
    pub delegated_provider_lookups: u64,
    pub delegated_provider_results: u64,
    pub delegated_provider_errors: u64,
    pub dht_provider_lookups: u64,
    pub dht_provider_results: u64,
    pub dht_provider_errors: u64,
    pub active_preload_count: u64,
    pub gateway_running: u64,
    pub lifecycle_background: u64,
}

struct NativeGatewayState {
    core: GatewayCore,
    requests: HashMap<u64, Arc<NativeGatewayRequest>>,
}

struct NativeGatewayRequest {
    handle: u64,
    meta: Arc<Mutex<NativeGatewayRequestMeta>>,
    wake: Arc<NativeGatewayRequestWake>,
    events: Arc<NativeGatewayEventMux>,
    body_rx: Mutex<mpsc::Receiver<NativeBodyMessage>>,
    pending: Mutex<VecDeque<u8>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl NativeGatewayRequest {
    fn cancel(&self) {
        if let Ok(mut meta) = self.meta.lock() {
            meta.cancelled = true;
        }
        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
        self.wake.notify();
        self.events
            .push(self.handle, FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED);
    }
}

impl Drop for NativeGatewayRequest {
    fn drop(&mut self) {
        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

enum NativeBodyMessage {
    Data(Vec<u8>),
    Failed(NativeGatewayErrorJson),
}

struct NativeGatewayRequestWake {
    generation: AtomicU64,
    wait_mutex: Mutex<()>,
    condvar: Condvar,
}

impl NativeGatewayRequestWake {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            wait_mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    fn notify(&self) {
        let guard = self.wait_mutex.lock().ok();
        self.generation.fetch_add(1, Ordering::Release);
        drop(guard);
        self.condvar.notify_all();
    }
}

#[derive(Default)]
struct NativeGatewayEventMux {
    state: Mutex<NativeGatewayEventMuxState>,
    condvar: Condvar,
}

#[derive(Default)]
struct NativeGatewayEventMuxState {
    queue: VecDeque<u64>,
    pending: HashMap<u64, u32>,
    stop_generation: u64,
}

impl NativeGatewayEventMux {
    fn push(&self, request_handle: u64, events: u32) {
        if request_handle == 0 || events == 0 {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let pending = state.pending.get(&request_handle).copied().unwrap_or(0);
        if pending == 0 {
            state.queue.push_back(request_handle);
        }
        state.pending.insert(request_handle, pending | events);
        self.condvar.notify_all();
    }

    fn signal_gateway_stopped(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stop_generation = state.stop_generation.wrapping_add(1);
            self.condvar.notify_all();
        }
    }

    fn wait_next(&self, timeout_ms: u64) -> FreedomIpfsGatewayEvent {
        let deadline = native_gateway_wait_deadline(timeout_ms);
        let Ok(mut state) = self.state.lock() else {
            return gateway_event_result(FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED, 0, 0);
        };
        let observed_stop_generation = state.stop_generation;
        loop {
            while let Some(request_handle) = state.queue.pop_front() {
                let events = state.pending.remove(&request_handle).unwrap_or(0);
                if events != 0 {
                    return gateway_event_result(
                        FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK,
                        events,
                        request_handle,
                    );
                }
            }
            if state.stop_generation != observed_stop_generation {
                return gateway_event_result(
                    FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED,
                    0,
                    0,
                );
            }
            let Some(remaining) = native_gateway_wait_remaining(deadline) else {
                return gateway_event_result(FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT, 0, 0);
            };
            match self.condvar.wait_timeout(state, remaining) {
                Ok((next_state, wait_result)) => {
                    state = next_state;
                    if wait_result.timed_out() {
                        return gateway_event_result(
                            FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT,
                            0,
                            0,
                        );
                    }
                }
                Err(_) => {
                    return gateway_event_result(
                        FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED,
                        0,
                        0,
                    );
                }
            }
        }
    }
}

#[derive(Default)]
struct NativeGatewayRequestMeta {
    method: String,
    path: String,
    namespace: String,
    request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
    response: Option<NativeGatewayResponseMeta>,
    completed: bool,
    cancelled: bool,
    error: Option<NativeGatewayErrorJson>,
}

#[derive(Clone)]
struct NativeGatewayResponseMeta {
    status: u16,
    headers: Vec<NativeGatewayHeader>,
}

#[derive(Clone, Deserialize, Serialize)]
struct NativeGatewayHeader {
    name: String,
    value: String,
}

#[derive(Clone, Serialize)]
struct NativeGatewayErrorJson {
    code: String,
    message: String,
}

#[derive(Deserialize)]
struct NativeGatewayStartRequest {
    method: Option<String>,
    path: String,
    headers: Option<Vec<NativeGatewayHeader>>,
    request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
}

#[derive(Serialize)]
struct NativeGatewayResponseJson {
    handle: u64,
    state: String,
    method: String,
    path: String,
    namespace: String,
    request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
    status: Option<u16>,
    headers: Vec<NativeGatewayHeader>,
    completed: bool,
    cancelled: bool,
    error: Option<NativeGatewayErrorJson>,
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

#[derive(Default)]
struct ProgressRecorder {
    inner: Mutex<ProgressInner>,
}

#[derive(Default)]
struct ProgressInner {
    next_event_id: u64,
    events: VecDeque<ProgressEvent>,
    active_targets: HashMap<String, ProgressTarget>,
}

#[derive(Clone, Serialize)]
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

#[derive(Clone, Serialize)]
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

#[derive(Serialize)]
struct ProgressSnapshot {
    generated_at_unix_ms: u64,
    active_count: usize,
    event_count: usize,
    active: Vec<ProgressTarget>,
    events: Vec<ProgressEvent>,
}

impl ProgressRecorder {
    fn record_event(&self, span: ProgressSpanFields, fields: ProgressFields, _metadata_name: &str) {
        let Some(raw_phase) = fields.get("phase").cloned() else {
            return;
        };
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
        inner.next_event_id = inner.next_event_id.saturating_add(1);
        let target_key = format!("{kind}:{target_id}");
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
        } else if target_id != 0 {
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

    fn snapshot_json(&self) -> String {
        let snapshot = match self.inner.lock() {
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
        };
        serde_json::to_string(&snapshot).unwrap_or_else(|_| "{\"active\":[],\"events\":[]}".into())
    }

    fn clear(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.events.clear();
            inner.active_targets.clear();
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

#[derive(Clone)]
struct ProgressLayer {
    recorder: Arc<ProgressRecorder>,
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
            span.extensions_mut().insert(span_fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = ProgressFields::default();
        event.record(&mut fields);
        let span = ctx
            .lookup_current()
            .and_then(|span| span.extensions().get::<ProgressSpanFields>().cloned())
            .unwrap_or_default();
        self.recorder
            .record_event(span, fields, event.metadata().name());
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

fn progress_recorder() -> Arc<ProgressRecorder> {
    PROGRESS_RECORDER
        .get_or_init(|| Arc::new(ProgressRecorder::default()))
        .clone()
}

fn ensure_progress_tracing() {
    let recorder = progress_recorder();
    PROGRESS_TRACING_INIT.call_once(|| {
        let layer = ProgressLayer { recorder };
        let subscriber = Registry::default().with(layer);
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
        "preload_cancelled" => "cancelled",
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
        "preload_cancelled" => "cancelled",
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

#[no_mangle]
pub extern "C" fn freedom_ipfs_version() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION"))
        .expect("version has no nul")
        .into_raw()
}

/// # Safety
///
/// `ptr` must be a pointer returned by `freedom_ipfs_version` and must not be
/// freed more than once.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn freedom_ipfs_node_new_in_memory() -> *mut FreedomIpfsNode {
    let store = match SqliteBlockStore::in_memory(DEFAULT_CACHE_BYTES) {
        Ok(store) => store,
        Err(_) => return ptr::null_mut(),
    };
    node_from_store(store)
}

/// # Safety
///
/// `data_dir` must point to a NUL-terminated UTF-8 path string for the
/// duration of this call. `max_cache_bytes` may be 0 to use the default 256 MiB
/// cache budget.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_new_with_data_dir(
    data_dir: *const c_char,
    max_cache_bytes: u64,
) -> *mut FreedomIpfsNode {
    if data_dir.is_null() {
        return ptr::null_mut();
    }
    let data_dir = match CStr::from_ptr(data_dir).to_str() {
        Ok(path) => PathBuf::from(path),
        Err(_) => return ptr::null_mut(),
    };
    if fs::create_dir_all(&data_dir).is_err() {
        return ptr::null_mut();
    }
    let max_cache_bytes = if max_cache_bytes == 0 {
        DEFAULT_CACHE_BYTES
    } else {
        max_cache_bytes
    };
    let store = match SqliteBlockStore::open(data_dir.join(CACHE_DB_FILE), max_cache_bytes) {
        Ok(store) => store,
        Err(_) => return ptr::null_mut(),
    };
    node_from_store(store)
}

fn node_from_store(store: SqliteBlockStore) -> *mut FreedomIpfsNode {
    ensure_progress_tracing();
    let runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(_) => return ptr::null_mut(),
    };
    let native_gateway = cache_only_gateway_core(store.clone());
    let native_gateway_events = Arc::new(NativeGatewayEventMux::default());
    Box::into_raw(Box::new(FreedomIpfsNode {
        runtime,
        store,
        gateway_addr: Mutex::new(None),
        gateway_task: Mutex::new(None),
        retrieval_stats_provider: Mutex::new(None),
        routing_stats: Mutex::new(None),
        lifecycle_state: Mutex::new(LifecycleState::Foreground),
        next_preload_id: AtomicU64::new(1),
        preload_tasks: Mutex::new(HashMap::new()),
        native_gateway_state: Mutex::new(NativeGatewayState {
            core: native_gateway,
            requests: HashMap::new(),
        }),
        native_gateway_events,
        next_native_request_id: AtomicU64::new(1),
    }))
}

/// # Safety
///
/// `ptr` must be a valid node pointer. The returned string is UTF-8 JSON and
/// must be released with `freedom_ipfs_string_free`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_progress_snapshot_json(
    ptr: *mut FreedomIpfsNode,
) -> *mut c_char {
    if ptr.is_null() {
        return CString::new("{\"active\":[],\"events\":[]}")
            .expect("static JSON has no nul")
            .into_raw();
    }
    ensure_progress_tracing();
    CString::new(progress_recorder().snapshot_json())
        .unwrap_or_else(|_| CString::new("{\"active\":[],\"events\":[]}").unwrap())
        .into_raw()
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_clear_progress(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    progress_recorder().clear();
    true
}

/// # Safety
///
/// `ptr` must be a pointer returned by `freedom_ipfs_node_new_in_memory` and
/// must not be used after this function returns.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_free(ptr: *mut FreedomIpfsNode) {
    if !ptr.is_null() {
        let node = &*ptr;
        stop_native_gateway_requests(node);
        stop_gateway(node);
        stop_preloads(node);
        let _ = Box::from_raw(ptr);
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `data` must point to `len` readable
/// bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_import_car(
    ptr: *mut FreedomIpfsNode,
    data: *const u8,
    len: usize,
) -> bool {
    if ptr.is_null() || data.is_null() {
        return false;
    }
    let node = &*ptr;
    let bytes = std::slice::from_raw_parts(data, len);
    node.store.import_car(bytes).is_ok()
}

/// # Safety
///
/// `ptr` must be a valid node pointer. The returned buffer must be released
/// with `freedom_ipfs_buffer_free`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_export_car(
    ptr: *mut FreedomIpfsNode,
) -> FreedomIpfsBuffer {
    if ptr.is_null() {
        return empty_buffer();
    }
    let node = &*ptr;
    match node.store.export_car() {
        Ok(bytes) => buffer_from_vec(bytes),
        Err(_) => empty_buffer(),
    }
}

/// # Safety
///
/// `buffer` must be a buffer returned by `freedom_ipfs_node_export_car` and
/// must not be freed more than once.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_buffer_free(buffer: FreedomIpfsBuffer) {
    if buffer.data.is_null() {
        return;
    }
    let slice = std::ptr::slice_from_raw_parts_mut(buffer.data, buffer.len);
    let _ = Box::from_raw(slice);
}

fn buffer_from_vec(bytes: Vec<u8>) -> FreedomIpfsBuffer {
    if bytes.is_empty() {
        return empty_buffer();
    }
    let mut bytes = bytes.into_boxed_slice();
    let data = bytes.as_mut_ptr();
    let len = bytes.len();
    let _ = Box::into_raw(bytes);
    FreedomIpfsBuffer { data, len }
}

fn empty_buffer() -> FreedomIpfsBuffer {
    FreedomIpfsBuffer {
        data: ptr::null_mut(),
        len: 0,
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_block_count(ptr: *mut FreedomIpfsNode) -> u64 {
    if ptr.is_null() {
        return 0;
    }
    let node = &*ptr;
    node.store.block_count().unwrap_or(0)
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_total_bytes(ptr: *mut FreedomIpfsNode) -> u64 {
    if ptr.is_null() {
        return 0;
    }
    let node = &*ptr;
    node.store.total_bytes().unwrap_or(0)
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_retrieval_stats(
    ptr: *mut FreedomIpfsNode,
) -> FreedomIpfsRetrievalStats {
    if ptr.is_null() {
        return FreedomIpfsRetrievalStats::default();
    }
    let node = &*ptr;
    let Ok(provider) = node.retrieval_stats_provider.lock() else {
        return FreedomIpfsRetrievalStats::default();
    };
    let Some(provider) = provider.as_ref() else {
        return FreedomIpfsRetrievalStats::default();
    };
    let stats = provider.stats();
    FreedomIpfsRetrievalStats {
        cache_hits: stats.cache_hits,
        http_provider_blocks: stats.http_provider_blocks,
        bitswap_blocks: stats.bitswap_blocks,
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_routing_stats(
    ptr: *mut FreedomIpfsNode,
) -> FreedomIpfsRoutingStats {
    if ptr.is_null() {
        return FreedomIpfsRoutingStats::default();
    }
    let node = &*ptr;
    let Ok(stats) = node.routing_stats.lock() else {
        return FreedomIpfsRoutingStats::default();
    };
    let Some(stats) = stats.as_ref() else {
        return FreedomIpfsRoutingStats::default();
    };
    let stats = stats.snapshot();
    FreedomIpfsRoutingStats {
        delegated_provider_lookups: stats.delegated_provider_lookups,
        delegated_provider_results: stats.delegated_provider_results,
        delegated_provider_errors: stats.delegated_provider_errors,
        dht_provider_lookups: stats.dht_provider_lookups,
        dht_provider_results: stats.dht_provider_results,
        dht_provider_errors: stats.dht_provider_errors,
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_active_preload_count(ptr: *mut FreedomIpfsNode) -> u64 {
    if ptr.is_null() {
        return 0;
    }
    let node = &*ptr;
    active_preload_count(node) as u64
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_diagnostics(
    ptr: *mut FreedomIpfsNode,
) -> FreedomIpfsDiagnostics {
    if ptr.is_null() {
        return FreedomIpfsDiagnostics::default();
    }
    let node = &*ptr;
    let retrieval = freedom_ipfs_node_retrieval_stats(ptr);
    let routing = freedom_ipfs_node_routing_stats(ptr);
    FreedomIpfsDiagnostics {
        block_count: node.store.block_count().unwrap_or(0),
        total_bytes: node.store.total_bytes().unwrap_or(0),
        cache_hits: retrieval.cache_hits,
        http_provider_blocks: retrieval.http_provider_blocks,
        bitswap_blocks: retrieval.bitswap_blocks,
        delegated_provider_lookups: routing.delegated_provider_lookups,
        delegated_provider_results: routing.delegated_provider_results,
        delegated_provider_errors: routing.delegated_provider_errors,
        dht_provider_lookups: routing.dht_provider_lookups,
        dht_provider_results: routing.dht_provider_results,
        dht_provider_errors: routing.dht_provider_errors,
        active_preload_count: active_preload_count(node) as u64,
        gateway_running: if gateway_is_running(node) { 1 } else { 0 },
        lifecycle_background: if lifecycle_is_background(node) { 1 } else { 0 },
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_clear_cache(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    node.store.clear().is_ok()
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_trim_cache(
    ptr: *mut FreedomIpfsNode,
    max_bytes: u64,
) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    node.store.trim_blocks_to(max_bytes).is_ok()
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_enter_background(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    stop_preloads(node);
    let Ok(mut state) = node.lifecycle_state.lock() else {
        return false;
    };
    *state = LifecycleState::Background;
    true
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_enter_foreground(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    prune_finished_preloads(node);
    let Ok(mut state) = node.lifecycle_state.lock() else {
        return false;
    };
    *state = LifecycleState::Foreground;
    true
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `max_cache_bytes` may be 0 to use the
/// built-in low-memory trim target.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_handle_low_memory(
    ptr: *mut FreedomIpfsNode,
    max_cache_bytes: u64,
) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    stop_preloads(node);
    let max_cache_bytes = if max_cache_bytes == 0 {
        LOW_MEMORY_CACHE_BYTES
    } else {
        max_cache_bytes
    };
    node.store.trim_blocks_to(max_cache_bytes).is_ok()
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_handle_network_change(
    ptr: *mut FreedomIpfsNode,
) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    stop_preloads(node);
    node.store.clear_provider_metadata().is_ok()
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `addr` must point to a NUL-terminated
/// UTF-8 loopback socket address string for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_gateway(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
) -> bool {
    if ptr.is_null() || addr.is_null() {
        return false;
    }
    let node = &*ptr;
    if gateway_is_running(node) {
        return true;
    }
    let addr = match parse_loopback_gateway_addr(addr) {
        Some(addr) => addr,
        None => return false,
    };

    let store = node.store.clone();
    if start_gateway_with_router(node, addr, freedom_ipfs_gateway::router(store)) {
        set_native_gateway_core(node, offline_gateway_core(node.store.clone()));
        clear_online_stats(node);
        true
    } else {
        false
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `addr` must point to a NUL-terminated
/// UTF-8 loopback socket address string for the duration of this call.
/// `delegated_router` may be null to use the default delegated routing
/// endpoint, otherwise it must point to a NUL-terminated UTF-8 URL string or
/// comma-separated URL list.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_gateway_online(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
) -> bool {
    freedom_ipfs_node_start_gateway_online_with_config(
        ptr,
        addr,
        delegated_router,
        ROUTING_MODE_AUTO,
        0,
    )
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `addr` must point to a NUL-terminated
/// UTF-8 loopback socket address string for the duration of this call.
/// `delegated_router` may be null to use the default delegated routing
/// endpoint, otherwise it must point to a NUL-terminated UTF-8 URL string or
/// comma-separated URL list.
/// `routing_mode` must be one of the `FREEDOM_IPFS_ROUTING_MODE_*` constants
/// from the C header.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_gateway_online_with_config(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
) -> bool {
    freedom_ipfs_node_start_gateway_online_with_config_v2(
        ptr,
        addr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        0,
        0,
    )
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `addr` must point to a NUL-terminated
/// UTF-8 loopback socket address string for the duration of this call.
/// `delegated_router` may be null to use the default delegated routing
/// endpoint, otherwise it must point to a NUL-terminated UTF-8 URL string or
/// comma-separated URL list.
/// `routing_mode` must be one of the `FREEDOM_IPFS_ROUTING_MODE_*` constants
/// from the C header. `dht_*` values may be 0 to use the built-in mobile
/// defaults.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_gateway_online_with_config_v2(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
) -> bool {
    if ptr.is_null() || addr.is_null() {
        return false;
    }
    let node = &*ptr;
    if gateway_is_running(node) {
        return true;
    }
    let Some(parts) = gateway_router_for_routing_mode(
        node,
        addr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        dht_query_timeout_secs,
        dht_max_providers,
    ) else {
        return false;
    };

    if start_gateway_with_router(node, parts.addr, parts.router) {
        set_native_gateway_core(node, parts.native_gateway);
        set_online_stats(node, parts.retrieval_provider, parts.routing_stats);
        true
    } else {
        false
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `addr` must point to a NUL-terminated
/// UTF-8 loopback socket address string for the duration of this call.
/// `delegated_router` may be null to use the default delegated routing
/// endpoint, otherwise it must point to a NUL-terminated UTF-8 URL string or
/// comma-separated URL list.
/// `routing_mode` must be one of the `FREEDOM_IPFS_ROUTING_MODE_*` constants
/// from the C header. `dht_*` values may be 0 to use the built-in mobile
/// defaults. On success this cancels active preloads, stops the current gateway,
/// and starts a new online gateway with the supplied routing configuration. On
/// validation failure the currently running gateway is left untouched.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_restart_gateway_online_with_config_v2(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
) -> bool {
    if ptr.is_null() || addr.is_null() {
        return false;
    }
    let node = &*ptr;
    let Some(parts) = gateway_router_for_routing_mode(
        node,
        addr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        dht_query_timeout_secs,
        dht_max_providers,
    ) else {
        return false;
    };

    stop_preloads(node);
    stop_gateway(node);
    if start_gateway_with_router(node, parts.addr, parts.router) {
        set_native_gateway_core(node, parts.native_gateway);
        set_online_stats(node, parts.retrieval_provider, parts.routing_stats);
        true
    } else {
        clear_online_stats(node);
        false
    }
}

struct OnlineGatewayParts {
    addr: SocketAddr,
    router: axum::Router,
    native_gateway: GatewayCore,
    retrieval_provider: FetchingBlockProvider,
    routing_stats: RoutingStatsHandle,
}

unsafe fn gateway_router_for_routing_mode(
    node: &FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
) -> Option<OnlineGatewayParts> {
    let addr = parse_loopback_gateway_addr(addr)?;
    let gateway_config = if max_concurrent_requests == 0 {
        freedom_ipfs_gateway::GatewayConfig::default()
    } else {
        freedom_ipfs_gateway::GatewayConfig::new(max_concurrent_requests)
    };
    if routing_mode == ROUTING_MODE_OFFLINE {
        let routing_stats = RoutingStatsHandle::default();
        let provider = FetchingBlockProvider::new(
            node.store.clone(),
            ProviderRoutingClient::Offline.with_stats(routing_stats.clone()),
        );
        let provider_arc: Arc<dyn BlockProvider> = Arc::new(provider.clone());
        let name_resolver: Arc<dyn NameResolver> = Arc::new(
            freedom_ipfs_gateway::PersistentNameResolver::cache_only(node.store.clone()),
        );
        let router = freedom_ipfs_gateway::router_with_provider_and_name_resolver_config(
            provider_arc.clone(),
            name_resolver.clone(),
            gateway_config.clone(),
        );
        let native_gateway = GatewayCore::with_provider_and_name_resolver_config(
            provider_arc,
            name_resolver,
            gateway_config,
        );
        return Some(OnlineGatewayParts {
            addr,
            router,
            native_gateway,
            retrieval_provider: provider,
            routing_stats,
        });
    }

    let delegated_routers = if delegated_router.is_null() {
        DEFAULT_DELEGATED_ROUTER.to_string()
    } else {
        CStr::from_ptr(delegated_router).to_str().ok()?.to_string()
    };

    let delegated = delegated_routing_client(&delegated_routers);
    let delegated_router_endpoints = delegated_router_endpoints(&delegated_routers);
    let dht = light_dht_client(dht_query_timeout_secs, dht_max_providers);
    let routing = match routing_mode {
        ROUTING_MODE_AUTO => {
            ProviderRoutingClient::from(AutoRoutingClient::new(delegated, dht.clone()))
        }
        ROUTING_MODE_DELEGATED => ProviderRoutingClient::from(delegated),
        ROUTING_MODE_LIGHT_DHT => ProviderRoutingClient::from(dht.clone()),
        _ => return None,
    };
    let routing_stats = RoutingStatsHandle::default();
    let routing = routing.with_stats(routing_stats.clone());
    let provider = FetchingBlockProvider::new(node.store.clone(), routing);
    let provider_arc: Arc<dyn BlockProvider> = Arc::new(provider.clone());
    let name_resolver = CachedNameResolver::new(freedom_ipfs_gateway::PersistentNameResolver::new(
        DefaultNameResolver::new(
            CloudflareDohResolver::default(),
            ipns_resolver(routing_mode, delegated_router_endpoints, dht),
        ),
        node.store.clone(),
    ));
    let name_resolver: Arc<dyn NameResolver> = Arc::new(name_resolver);
    let router = freedom_ipfs_gateway::router_with_provider_and_name_resolver_config(
        provider_arc.clone(),
        name_resolver.clone(),
        gateway_config.clone(),
    );
    let native_gateway = GatewayCore::with_provider_and_name_resolver_config(
        provider_arc,
        name_resolver,
        gateway_config,
    );
    Some(OnlineGatewayParts {
        addr,
        router,
        native_gateway,
        retrieval_provider: provider,
        routing_stats,
    })
}

unsafe fn parse_loopback_gateway_addr(addr: *const c_char) -> Option<SocketAddr> {
    let addr = CStr::from_ptr(addr)
        .to_str()
        .ok()
        .and_then(|s| s.parse::<SocketAddr>().ok())?;
    addr.ip().is_loopback().then_some(addr)
}

fn light_dht_client(dht_query_timeout_secs: u64, dht_max_providers: usize) -> LightDhtClient {
    let mut dht = LightDhtClient::default();
    if dht_query_timeout_secs != 0 {
        dht = dht.with_query_timeout(Duration::from_secs(dht_query_timeout_secs));
    }
    if dht_max_providers != 0 {
        dht = dht.with_max_providers(dht_max_providers);
    }
    dht
}

fn delegated_routing_client(delegated_routers: &str) -> DelegatedRoutingClient {
    DelegatedRoutingClient::with_endpoints(delegated_router_endpoints(delegated_routers))
}

fn delegated_router_endpoints(delegated_routers: &str) -> Vec<String> {
    delegated_routers
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect()
}

fn ipns_resolver(
    routing_mode: u32,
    delegated_routers: Vec<String>,
    dht: LightDhtClient,
) -> Arc<dyn IpnsResolver> {
    match routing_mode {
        ROUTING_MODE_AUTO => Arc::new(FallbackIpnsResolver::new(
            DelegatedIpnsResolver::with_endpoints(delegated_routers),
            DhtIpnsResolver::new(dht),
        )),
        ROUTING_MODE_DELEGATED => {
            Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers))
        }
        ROUTING_MODE_LIGHT_DHT => Arc::new(DhtIpnsResolver::new(dht)),
        _ => Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers)),
    }
}

fn start_gateway_with_router(
    node: &FreedomIpfsNode,
    addr: SocketAddr,
    router: axum::Router,
) -> bool {
    let mut gateway_task = match node.gateway_task.lock() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    if gateway_task.is_some() {
        return true;
    }

    let listener = match node.runtime.block_on(TcpListener::bind(addr)) {
        Ok(listener) => listener,
        Err(_) => return false,
    };
    let bound = match listener.local_addr() {
        Ok(bound) => bound,
        Err(_) => return false,
    };
    let task = node.runtime.spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    *gateway_task = Some(task);
    if let Ok(mut gateway_addr) = node.gateway_addr.lock() {
        *gateway_addr = Some(bound);
    }
    true
}

fn gateway_is_running(node: &FreedomIpfsNode) -> bool {
    node.gateway_task
        .lock()
        .map(|task| task.is_some())
        .unwrap_or(false)
}

fn lifecycle_is_background(node: &FreedomIpfsNode) -> bool {
    node.lifecycle_state
        .lock()
        .map(|state| *state == LifecycleState::Background)
        .unwrap_or(false)
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_gateway_url(ptr: *mut FreedomIpfsNode) -> *mut c_char {
    if ptr.is_null() {
        return ptr::null_mut();
    }
    let node = &*ptr;
    let Ok(gateway_addr) = node.gateway_addr.lock() else {
        return ptr::null_mut();
    };
    let Some(addr) = *gateway_addr else {
        return ptr::null_mut();
    };
    match CString::new(format!("http://{addr}")) {
        Ok(url) => url.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `request_json` must point to a
/// NUL-terminated UTF-8 JSON request for the duration of this call. Returns 0
/// when the request cannot be parsed or queued.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_start(
    ptr: *mut FreedomIpfsNode,
    request_json: *const c_char,
) -> u64 {
    if ptr.is_null() || request_json.is_null() {
        return 0;
    }
    let node = &*ptr;
    let request_json = match CStr::from_ptr(request_json).to_str() {
        Ok(request_json) => request_json,
        Err(_) => return 0,
    };
    let parsed = match parse_native_gateway_start_request(request_json) {
        Some(parsed) => parsed,
        None => return 0,
    };
    let handle = node.next_native_request_id.fetch_add(1, Ordering::Relaxed);
    if handle == 0 {
        return 0;
    }
    let (body_tx, body_rx) = mpsc::channel(NATIVE_BODY_CHANNEL_CAPACITY);
    let Ok(mut native_state) = node.native_gateway_state.lock() else {
        return 0;
    };
    let core = native_state.core.clone();
    let wake = Arc::new(NativeGatewayRequestWake::new());
    let events = node.native_gateway_events.clone();
    let meta = Arc::new(Mutex::new(NativeGatewayRequestMeta {
        method: parsed.method_label,
        path: parsed.rendered_path,
        namespace: parsed.namespace_label,
        request_id: parsed.request_id,
        parent_request_id: parsed.parent_request_id,
        top_level_path: parsed.top_level_path,
        response: None,
        completed: false,
        cancelled: false,
        error: None,
    }));
    let task_meta = meta.clone();
    let task_wake = wake.clone();
    let task_events = events.clone();
    let (registered_tx, registered_rx) = oneshot::channel();
    let task = node.runtime.spawn(async move {
        if registered_rx.await.is_err() {
            return;
        }
        run_native_gateway_request(
            core,
            parsed.request,
            handle,
            task_meta,
            task_wake,
            task_events,
            body_tx,
        )
        .await;
    });
    let request = Arc::new(NativeGatewayRequest {
        handle,
        meta,
        wake,
        events,
        body_rx: Mutex::new(body_rx),
        pending: Mutex::new(VecDeque::new()),
        task: Mutex::new(Some(task)),
    });
    native_state.requests.insert(handle, request);
    drop(native_state);
    let _ = registered_tx.send(());
    handle
}

/// # Safety
///
/// `ptr` must be a valid node pointer. The returned string is UTF-8 JSON and
/// must be released with `freedom_ipfs_string_free`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_response_json(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
) -> *mut c_char {
    if ptr.is_null() || request_handle == 0 {
        return native_gateway_json_string(native_gateway_invalid_handle_json(request_handle));
    }
    let node = &*ptr;
    let Some(request) = native_gateway_request(node, request_handle) else {
        return native_gateway_json_string(native_gateway_invalid_handle_json(request_handle));
    };
    native_gateway_json_string(native_gateway_response_json(request_handle, &request))
}

/// # Safety
///
/// `ptr` must be a valid node pointer. The returned string is UTF-8 JSON and
/// must be released with `freedom_ipfs_string_free`. Blocks up to
/// `timeout_ms` for response metadata, failure, completion, cancellation, or
/// handle invalidation. A zero timeout performs the same immediate check as
/// `freedom_ipfs_gateway_request_response_json`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_response_json_wait(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
    timeout_ms: u64,
) -> *mut c_char {
    if ptr.is_null() || request_handle == 0 {
        return native_gateway_json_string(native_gateway_invalid_handle_json(request_handle));
    }
    let node = &*ptr;
    let Some(request) = native_gateway_request(node, request_handle) else {
        return native_gateway_json_string(native_gateway_invalid_handle_json(request_handle));
    };
    native_gateway_json_string(native_gateway_response_json_wait(
        request_handle,
        &request,
        timeout_ms,
    ))
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `buffer` must point to `buffer_len`
/// writable bytes for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_read(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
    buffer: *mut u8,
    buffer_len: usize,
) -> FreedomIpfsGatewayReadResult {
    if ptr.is_null() || request_handle == 0 {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE, 0);
    }
    let node = &*ptr;
    let Some(request) = native_gateway_request(node, request_handle) else {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE, 0);
    };
    if buffer.is_null() || buffer_len == 0 {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0);
    }
    native_gateway_read(&request, std::slice::from_raw_parts_mut(buffer, buffer_len))
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `buffer` must point to `buffer_len`
/// writable bytes for the duration of this call. Blocks up to `timeout_ms` for
/// bytes, end, failure, cancellation, or handle invalidation. A zero timeout
/// performs the same immediate check as `freedom_ipfs_gateway_request_read`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_read_wait(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
    buffer: *mut u8,
    buffer_len: usize,
    timeout_ms: u64,
) -> FreedomIpfsGatewayReadResult {
    if ptr.is_null() || request_handle == 0 {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE, 0);
    }
    let node = &*ptr;
    let Some(request) = native_gateway_request(node, request_handle) else {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE, 0);
    };
    if buffer.is_null() || buffer_len == 0 {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0);
    }
    native_gateway_read_wait(
        &request,
        std::slice::from_raw_parts_mut(buffer, buffer_len),
        timeout_ms,
    )
}

/// # Safety
///
/// `ptr` must be a valid node pointer. Blocks up to `timeout_ms` for readiness
/// from any active native gateway request. A zero timeout performs an immediate
/// nonblocking check. Events are readiness bitflags only; body bytes still flow
/// through `freedom_ipfs_gateway_request_read`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_wait_next_event(
    ptr: *mut FreedomIpfsNode,
    timeout_ms: u64,
) -> FreedomIpfsGatewayEvent {
    if ptr.is_null() {
        return gateway_event_result(FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE, 0, 0);
    }
    let node = &*ptr;
    node.native_gateway_events.wait_next(timeout_ms)
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `request_handle` must be a handle
/// returned by `freedom_ipfs_gateway_request_start`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_cancel(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
) -> bool {
    if ptr.is_null() || request_handle == 0 {
        return false;
    }
    let node = &*ptr;
    let Some(request) = native_gateway_request(node, request_handle) else {
        return false;
    };
    request.cancel();
    true
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `request_handle` must be a handle
/// returned by `freedom_ipfs_gateway_request_start` and must not be freed more
/// than once.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_gateway_request_free(
    ptr: *mut FreedomIpfsNode,
    request_handle: u64,
) -> bool {
    if ptr.is_null() || request_handle == 0 {
        return false;
    }
    let node = &*ptr;
    let Ok(mut native_state) = node.native_gateway_state.lock() else {
        return false;
    };
    let Some(request) = native_state.requests.remove(&request_handle) else {
        return false;
    };
    request.cancel();
    node.native_gateway_events
        .push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);
    true
}

struct ParsedNativeGatewayRequest {
    request: GatewayCoreRequest,
    method_label: String,
    rendered_path: String,
    namespace_label: String,
    request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
}

fn parse_native_gateway_start_request(request_json: &str) -> Option<ParsedNativeGatewayRequest> {
    let request: NativeGatewayStartRequest = serde_json::from_str(request_json).ok()?;
    let method_label = request.method.unwrap_or_else(|| "GET".to_string());
    let method = match method_label.to_ascii_uppercase().as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        _ => return None,
    };
    let path = normalize_gateway_request_path(&request.path)?;
    let (namespace, path_without_namespace, rendered_path) = split_gateway_namespace_path(&path)?;
    let mut headers = HeaderMap::new();
    if let Some(request_headers) = request.headers {
        for header in request_headers {
            insert_native_header(&mut headers, &header.name, &header.value)?;
        }
    }
    if let Some(request_id) = request.request_id {
        insert_native_header(&mut headers, X_FREEDOM_REQUEST_ID, &request_id.to_string())?;
    }
    if let Some(parent_request_id) = request.parent_request_id {
        insert_native_header(
            &mut headers,
            X_FREEDOM_PARENT_REQUEST_ID,
            &parent_request_id.to_string(),
        )?;
    }
    if let Some(top_level_path) = request
        .top_level_path
        .as_ref()
        .filter(|top_level_path| !top_level_path.is_empty())
    {
        insert_native_header(&mut headers, X_FREEDOM_TOP_LEVEL_PATH, top_level_path)?;
    }
    let gateway_request = match namespace {
        "ipfs" => GatewayCoreRequest::ipfs(path_without_namespace, method, headers),
        "ipns" => GatewayCoreRequest::ipns(path_without_namespace, method, headers),
        _ => return None,
    };
    Some(ParsedNativeGatewayRequest {
        request: gateway_request,
        method_label: method_label.to_ascii_uppercase(),
        rendered_path,
        namespace_label: namespace.to_string(),
        request_id: request.request_id,
        parent_request_id: request.parent_request_id,
        top_level_path: request.top_level_path,
    })
}

fn normalize_gateway_request_path(path: &str) -> Option<String> {
    let path = normalize_preload_path(path)?;
    let end = path.find(['?', '#']).unwrap_or(path.len());
    let path = &path[..end];
    (!path.is_empty()).then(|| path.to_string())
}

fn split_gateway_namespace_path(path: &str) -> Option<(&'static str, String, String)> {
    if let Some(path) = path.strip_prefix("/ipfs/") {
        return (!path.is_empty()).then(|| ("ipfs", path.to_string(), format!("/ipfs/{path}")));
    }
    if let Some(path) = path.strip_prefix("/ipns/") {
        return (!path.is_empty()).then(|| ("ipns", path.to_string(), format!("/ipns/{path}")));
    }
    None
}

fn insert_native_header(headers: &mut HeaderMap, name: &str, value: &str) -> Option<()> {
    let name = HeaderName::from_bytes(name.as_bytes()).ok()?;
    let value = HeaderValue::from_str(value).ok()?;
    headers.insert(name, value);
    Some(())
}

async fn run_native_gateway_request(
    core: GatewayCore,
    request: GatewayCoreRequest,
    request_handle: u64,
    meta: Arc<Mutex<NativeGatewayRequestMeta>>,
    wake: Arc<NativeGatewayRequestWake>,
    events: Arc<NativeGatewayEventMux>,
    body_tx: mpsc::Sender<NativeBodyMessage>,
) {
    let response = core.handle(request).await;
    let status = response.status().as_u16();
    let headers = native_gateway_headers(response.headers());
    if let Ok(mut meta) = meta.lock() {
        if !meta.cancelled {
            meta.response = Some(NativeGatewayResponseMeta { status, headers });
        }
    }
    wake.notify();
    events.push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY);
    let mut body = response.into_body().into_data_stream();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(chunk) => {
                if body_tx
                    .send(NativeBodyMessage::Data(chunk.to_vec()))
                    .await
                    .is_err()
                {
                    return;
                }
                wake.notify();
                events.push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
            }
            Err(err) => {
                let error = NativeGatewayErrorJson {
                    code: "body_stream_error".to_string(),
                    message: err.to_string(),
                };
                if let Ok(mut meta) = meta.lock() {
                    meta.error = Some(error.clone());
                }
                let _ = body_tx.send(NativeBodyMessage::Failed(error)).await;
                wake.notify();
                events.push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_FAILED);
                return;
            }
        }
    }
    if let Ok(mut meta) = meta.lock() {
        if !meta.cancelled && meta.error.is_none() {
            meta.completed = true;
        }
    }
    wake.notify();
    events.push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_END);
}

fn native_gateway_headers(headers: &HeaderMap) -> Vec<NativeGatewayHeader> {
    headers
        .iter()
        .map(|(name, value)| NativeGatewayHeader {
            name: name.as_str().to_string(),
            value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
        })
        .collect()
}

fn native_gateway_request(
    node: &FreedomIpfsNode,
    request_handle: u64,
) -> Option<Arc<NativeGatewayRequest>> {
    let native_state = node.native_gateway_state.lock().ok()?;
    native_state.requests.get(&request_handle).cloned()
}

fn native_gateway_read(
    request: &NativeGatewayRequest,
    buffer: &mut [u8],
) -> FreedomIpfsGatewayReadResult {
    if native_gateway_request_is_cancelled(request) {
        return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_CANCELLED, 0);
    }
    match drain_native_pending(request, buffer) {
        Ok(Some(bytes_read)) => {
            return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_BYTES, bytes_read);
        }
        Ok(None) => {}
        Err(()) => return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0),
    }
    let message = {
        let Ok(mut body_rx) = request.body_rx.lock() else {
            return gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0);
        };
        body_rx.try_recv()
    };
    match message {
        Ok(NativeBodyMessage::Data(chunk)) => {
            let bytes_read = copy_native_chunk(request, &chunk, buffer);
            gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_BYTES, bytes_read)
        }
        Ok(NativeBodyMessage::Failed(error)) => {
            if let Ok(mut meta) = request.meta.lock() {
                meta.error = Some(error);
            }
            gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0)
        }
        Err(mpsc::error::TryRecvError::Empty) => native_gateway_empty_read_status(request),
        Err(mpsc::error::TryRecvError::Disconnected) => {
            native_gateway_disconnected_read_status(request)
        }
    }
}

fn native_gateway_response_json_wait(
    handle: u64,
    request: &NativeGatewayRequest,
    timeout_ms: u64,
) -> NativeGatewayResponseJson {
    if timeout_ms == 0 {
        return native_gateway_response_json(handle, request);
    }
    let deadline = native_gateway_wait_deadline(timeout_ms);
    loop {
        let response = native_gateway_response_json(handle, request);
        if response.state != "pending" {
            return response;
        }
        let observed = request.wake.generation.load(Ordering::Acquire);
        let response = native_gateway_response_json(handle, request);
        if response.state != "pending" {
            return response;
        }
        let Some(remaining) = native_gateway_wait_remaining(deadline) else {
            return response;
        };
        if request.wake.generation.load(Ordering::Acquire) != observed {
            continue;
        }
        let Ok(guard) = request.wake.wait_mutex.lock() else {
            return native_gateway_response_json(handle, request);
        };
        if request.wake.generation.load(Ordering::Acquire) != observed {
            continue;
        }
        match request.wake.condvar.wait_timeout(guard, remaining) {
            Ok((_, wait_result)) if wait_result.timed_out() => {
                return native_gateway_response_json(handle, request);
            }
            Ok(_) => {}
            Err(_) => return native_gateway_response_json(handle, request),
        }
    }
}

fn native_gateway_read_wait(
    request: &NativeGatewayRequest,
    buffer: &mut [u8],
    timeout_ms: u64,
) -> FreedomIpfsGatewayReadResult {
    if timeout_ms == 0 {
        return native_gateway_read(request, buffer);
    }
    let deadline = native_gateway_wait_deadline(timeout_ms);
    loop {
        let result = native_gateway_read(request, buffer);
        if result.status != FREEDOM_IPFS_GATEWAY_READ_PENDING {
            return result;
        }
        let observed = request.wake.generation.load(Ordering::Acquire);
        let result = native_gateway_read(request, buffer);
        if result.status != FREEDOM_IPFS_GATEWAY_READ_PENDING {
            return result;
        }
        let Some(remaining) = native_gateway_wait_remaining(deadline) else {
            return result;
        };
        if request.wake.generation.load(Ordering::Acquire) != observed {
            continue;
        }
        let Ok(guard) = request.wake.wait_mutex.lock() else {
            return native_gateway_read(request, buffer);
        };
        if request.wake.generation.load(Ordering::Acquire) != observed {
            continue;
        }
        match request.wake.condvar.wait_timeout(guard, remaining) {
            Ok((_, wait_result)) if wait_result.timed_out() => {
                return native_gateway_read(request, buffer);
            }
            Ok(_) => {}
            Err(_) => return native_gateway_read(request, buffer),
        }
    }
}

fn native_gateway_wait_deadline(timeout_ms: u64) -> Instant {
    let now = Instant::now();
    now.checked_add(Duration::from_millis(timeout_ms))
        .or_else(|| now.checked_add(Duration::from_secs(365 * 24 * 60 * 60)))
        .unwrap_or(now)
}

fn native_gateway_wait_remaining(deadline: Instant) -> Option<Duration> {
    let now = Instant::now();
    (now < deadline).then(|| deadline.saturating_duration_since(now))
}

fn drain_native_pending(
    request: &NativeGatewayRequest,
    buffer: &mut [u8],
) -> Result<Option<usize>, ()> {
    let mut pending = request.pending.lock().map_err(|_| ())?;
    if pending.is_empty() {
        return Ok(None);
    }
    let bytes_read = pending.len().min(buffer.len());
    for byte in buffer.iter_mut().take(bytes_read) {
        if let Some(value) = pending.pop_front() {
            *byte = value;
        }
    }
    if !pending.is_empty() {
        request
            .events
            .push(request.handle, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
    }
    Ok(Some(bytes_read))
}

fn copy_native_chunk(request: &NativeGatewayRequest, chunk: &[u8], buffer: &mut [u8]) -> usize {
    let bytes_read = chunk.len().min(buffer.len());
    buffer[..bytes_read].copy_from_slice(&chunk[..bytes_read]);
    if bytes_read < chunk.len() {
        if let Ok(mut pending) = request.pending.lock() {
            pending.extend(chunk[bytes_read..].iter().copied());
        }
        request
            .events
            .push(request.handle, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
    }
    bytes_read
}

fn native_gateway_empty_read_status(
    request: &NativeGatewayRequest,
) -> FreedomIpfsGatewayReadResult {
    if native_gateway_request_is_cancelled(request) {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_CANCELLED, 0)
    } else if native_gateway_request_has_error(request) {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0)
    } else if native_gateway_request_is_completed(request) {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_END, 0)
    } else {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_PENDING, 0)
    }
}

fn native_gateway_disconnected_read_status(
    request: &NativeGatewayRequest,
) -> FreedomIpfsGatewayReadResult {
    if native_gateway_request_is_cancelled(request) {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_CANCELLED, 0)
    } else if native_gateway_request_has_error(request) {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_FAILED, 0)
    } else {
        gateway_read_result(FREEDOM_IPFS_GATEWAY_READ_END, 0)
    }
}

fn native_gateway_response_json(
    handle: u64,
    request: &NativeGatewayRequest,
) -> NativeGatewayResponseJson {
    let Ok(meta) = request.meta.lock() else {
        return NativeGatewayResponseJson {
            handle,
            state: "failed".to_string(),
            method: String::new(),
            path: String::new(),
            namespace: String::new(),
            request_id: None,
            parent_request_id: None,
            top_level_path: None,
            status: None,
            headers: Vec::new(),
            completed: false,
            cancelled: false,
            error: Some(NativeGatewayErrorJson {
                code: "metadata_unavailable".to_string(),
                message: "request metadata is unavailable".to_string(),
            }),
        };
    };
    let state = if meta.cancelled {
        "cancelled"
    } else if meta.error.is_some() {
        "failed"
    } else if meta.completed {
        "completed"
    } else if meta.response.is_some() {
        "streaming"
    } else {
        "pending"
    };
    NativeGatewayResponseJson {
        handle,
        state: state.to_string(),
        method: meta.method.clone(),
        path: meta.path.clone(),
        namespace: meta.namespace.clone(),
        request_id: meta.request_id,
        parent_request_id: meta.parent_request_id,
        top_level_path: meta.top_level_path.clone(),
        status: meta.response.as_ref().map(|response| response.status),
        headers: meta
            .response
            .as_ref()
            .map(|response| response.headers.clone())
            .unwrap_or_default(),
        completed: meta.completed,
        cancelled: meta.cancelled,
        error: meta.error.clone(),
    }
}

fn native_gateway_invalid_handle_json(handle: u64) -> NativeGatewayResponseJson {
    NativeGatewayResponseJson {
        handle,
        state: "failed".to_string(),
        method: String::new(),
        path: String::new(),
        namespace: String::new(),
        request_id: None,
        parent_request_id: None,
        top_level_path: None,
        status: None,
        headers: Vec::new(),
        completed: false,
        cancelled: false,
        error: Some(NativeGatewayErrorJson {
            code: "invalid_handle".to_string(),
            message: "native gateway request handle was not found".to_string(),
        }),
    }
}

fn native_gateway_json_string(response: NativeGatewayResponseJson) -> *mut c_char {
    let json = serde_json::to_string(&response).unwrap_or_else(|_| {
        "{\"state\":\"failed\",\"error\":{\"code\":\"json_encode_failed\",\"message\":\"failed to encode native gateway response\"}}".to_string()
    });
    CString::new(json)
        .unwrap_or_else(|_| CString::new("{\"state\":\"failed\"}").unwrap())
        .into_raw()
}

fn native_gateway_request_is_cancelled(request: &NativeGatewayRequest) -> bool {
    request
        .meta
        .lock()
        .map(|meta| meta.cancelled)
        .unwrap_or(true)
}

fn native_gateway_request_has_error(request: &NativeGatewayRequest) -> bool {
    request
        .meta
        .lock()
        .map(|meta| meta.error.is_some())
        .unwrap_or(true)
}

fn native_gateway_request_is_completed(request: &NativeGatewayRequest) -> bool {
    request
        .meta
        .lock()
        .map(|meta| meta.completed)
        .unwrap_or(false)
}

fn gateway_read_result(status: u32, bytes_read: usize) -> FreedomIpfsGatewayReadResult {
    FreedomIpfsGatewayReadResult { status, bytes_read }
}

fn gateway_event_result(status: u32, events: u32, request_handle: u64) -> FreedomIpfsGatewayEvent {
    FreedomIpfsGatewayEvent {
        status,
        events,
        request_handle,
    }
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `path` must point to a NUL-terminated
/// UTF-8 path or URI for the duration of this call. Accepted inputs are
/// `/ipfs/...`, `/ipns/...`, `ipfs://...`, `ipns://...`, or a bare CID. Returns
/// 0 when the gateway is not running or the path is invalid.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_preload_path(
    ptr: *mut FreedomIpfsNode,
    path: *const c_char,
) -> u64 {
    if ptr.is_null() || path.is_null() {
        return 0;
    }
    let node = &*ptr;
    let path = match CStr::from_ptr(path).to_str() {
        Ok(path) => match normalize_preload_path(path) {
            Some(path) => path,
            None => return 0,
        },
        _ => return 0,
    };
    let Ok(gateway_addr) = node.gateway_addr.lock() else {
        return 0;
    };
    let Some(addr) = *gateway_addr else {
        return 0;
    };
    drop(gateway_addr);

    let id = node.next_preload_id.fetch_add(1, Ordering::Relaxed);
    let url = format!("http://{addr}{path}");
    tracing::info!(phase = "preload_start", preload_id = id, path = %path);
    let preload_path = path.clone();
    let task = node.runtime.spawn(async move {
        let Ok(client) = reqwest::Client::builder().timeout(PRELOAD_TIMEOUT).build() else {
            tracing::info!(
                phase = "preload_done",
                preload_id = id,
                path = %preload_path,
                ok = false,
                error = "client_build_failed"
            );
            return;
        };
        let Ok(response) = client.get(url).send().await else {
            tracing::info!(
                phase = "preload_done",
                preload_id = id,
                path = %preload_path,
                ok = false,
                error = "request_failed"
            );
            return;
        };
        let Ok(mut response) = response.error_for_status() else {
            tracing::info!(
                phase = "preload_done",
                preload_id = id,
                path = %preload_path,
                ok = false,
                error = "http_status"
            );
            return;
        };
        while matches!(response.chunk().await, Ok(Some(_))) {}
        tracing::info!(
            phase = "preload_done",
            preload_id = id,
            path = %preload_path,
            ok = true
        );
    });

    let Ok(mut tasks) = node.preload_tasks.lock() else {
        task.abort();
        return 0;
    };
    tasks.retain(|_, task| !task.is_finished());
    tasks.insert(id, task);
    id
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `task_id` must be an id returned by
/// `freedom_ipfs_node_preload_path`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_cancel_preload(
    ptr: *mut FreedomIpfsNode,
    task_id: u64,
) -> bool {
    if ptr.is_null() || task_id == 0 {
        return false;
    }
    let node = &*ptr;
    let Ok(mut tasks) = node.preload_tasks.lock() else {
        return false;
    };
    let Some(task) = tasks.remove(&task_id) else {
        return false;
    };
    task.abort();
    tracing::info!(phase = "preload_cancelled", preload_id = task_id);
    true
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_stop_gateway(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    stop_gateway(node);
    true
}

fn stop_gateway(node: &FreedomIpfsNode) {
    stop_native_gateway_requests(node);
    node.native_gateway_events.signal_gateway_stopped();
    if let Ok(mut gateway_task) = node.gateway_task.lock() {
        if let Some(task) = gateway_task.take() {
            task.abort();
        }
    }
    if let Ok(mut gateway_addr) = node.gateway_addr.lock() {
        *gateway_addr = None;
    }
    set_native_gateway_core(node, cache_only_gateway_core(node.store.clone()));
    clear_online_stats(node);
}

fn stop_native_gateway_requests(node: &FreedomIpfsNode) {
    let requests = if let Ok(mut native_state) = node.native_gateway_state.lock() {
        native_state
            .requests
            .drain()
            .map(|(_, request)| request)
            .collect()
    } else {
        Vec::new()
    };
    for request in requests {
        request.cancel();
    }
}

fn stop_preloads(node: &FreedomIpfsNode) {
    if let Ok(mut tasks) = node.preload_tasks.lock() {
        for (task_id, task) in tasks.drain() {
            if !task.is_finished() {
                task.abort();
                tracing::info!(phase = "preload_cancelled", preload_id = task_id);
            }
        }
    }
}

fn prune_finished_preloads(node: &FreedomIpfsNode) {
    if let Ok(mut tasks) = node.preload_tasks.lock() {
        tasks.retain(|_, task| !task.is_finished());
    }
}

fn active_preload_count(node: &FreedomIpfsNode) -> usize {
    if let Ok(mut tasks) = node.preload_tasks.lock() {
        tasks.retain(|_, task| !task.is_finished());
        tasks.len()
    } else {
        0
    }
}

fn set_online_stats(
    node: &FreedomIpfsNode,
    retrieval_provider: FetchingBlockProvider,
    routing_stats: RoutingStatsHandle,
) {
    if let Ok(mut stats_provider) = node.retrieval_stats_provider.lock() {
        *stats_provider = Some(retrieval_provider);
    }
    if let Ok(mut stats) = node.routing_stats.lock() {
        *stats = Some(routing_stats);
    }
}

fn clear_online_stats(node: &FreedomIpfsNode) {
    if let Ok(mut stats_provider) = node.retrieval_stats_provider.lock() {
        *stats_provider = None;
    }
    if let Ok(mut stats) = node.routing_stats.lock() {
        *stats = None;
    }
}

fn set_native_gateway_core(node: &FreedomIpfsNode, core: GatewayCore) {
    let requests = if let Ok(mut native_state) = node.native_gateway_state.lock() {
        let requests = native_state
            .requests
            .drain()
            .map(|(_, request)| request)
            .collect();
        native_state.core = core;
        requests
    } else {
        Vec::new()
    };
    let cancelled_requests = !requests.is_empty();
    for request in requests {
        request.cancel();
    }
    if cancelled_requests {
        node.native_gateway_events.signal_gateway_stopped();
    }
}

fn offline_gateway_core(store: SqliteBlockStore) -> GatewayCore {
    GatewayCore::new(store)
}

fn cache_only_gateway_core(store: SqliteBlockStore) -> GatewayCore {
    let provider: Arc<dyn BlockProvider> = Arc::new(store.clone());
    let name_resolver: Arc<dyn NameResolver> = Arc::new(
        freedom_ipfs_gateway::PersistentNameResolver::cache_only(store),
    );
    GatewayCore::with_provider_and_name_resolver(provider, name_resolver)
}

fn normalize_preload_path(path: &str) -> Option<String> {
    let path = path.trim();
    if path.starts_with("/ipfs/") || path.starts_with("/ipns/") {
        return Some(path.to_string());
    }
    if let Some(rest) = path.strip_prefix("ipfs://") {
        let rest = rest.trim_start_matches('/');
        if !rest.is_empty() {
            return Some(format!("/ipfs/{rest}"));
        }
    }
    if let Some(rest) = path.strip_prefix("ipns://") {
        let rest = rest.trim_start_matches('/');
        if !rest.is_empty() {
            return Some(format!("/ipns/{rest}"));
        }
    }
    parse_cid(path).ok().map(|cid| format!("/ipfs/{cid}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{cid_from_data, parse_car_v1, CODEC_RAW};
    use freedom_ipfs_store::CachedProviderRecord;
    use serde_json::json;
    use std::io::{Read, Write};

    #[test]
    fn starts_gateway_and_reports_bound_url() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway(node, addr.as_ptr()));

            assert_gateway_health(node);

            assert!(freedom_ipfs_node_stop_gateway(node));
            assert!(freedom_ipfs_node_gateway_url(node).is_null());
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn rejects_non_loopback_gateway_bind_addresses() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            for bind_addr in ["0.0.0.0:0", "[::]:0"] {
                let addr = CString::new(bind_addr).unwrap();
                assert!(!freedom_ipfs_node_start_gateway(node, addr.as_ptr()));
                assert!(freedom_ipfs_node_gateway_url(node).is_null());
            }

            let ipv6_loopback = CString::new("[::1]:0").unwrap();
            assert!(parse_loopback_gateway_addr(ipv6_loopback.as_ptr()).is_some());

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn starts_online_gateway_and_reports_bound_url() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online(
                node,
                addr.as_ptr(),
                ptr::null(),
            ));

            assert_gateway_health(node);

            assert!(freedom_ipfs_node_stop_gateway(node));
            assert!(freedom_ipfs_node_gateway_url(node).is_null());
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn starts_online_gateway_with_config() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_DELEGATED,
                1,
            ));

            assert_gateway_health(node);

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn starts_online_gateway_with_dht_budget_config() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_AUTO,
                1,
                5,
                2,
            ));

            assert_gateway_health(node);

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn offline_routing_mode_is_cache_only() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"offline routing mode";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                ptr::null(),
                ROUTING_MODE_OFFLINE,
                1,
                0,
                0,
            ));

            assert_gateway_health(node);
            assert_gateway_path(node, &format!("/ipfs/{cid}"), data);
            let ipns_response = gateway_response(node, "/ipns/example.com");
            assert!(ipns_response.contains("404 Not Found"), "{ipns_response}");

            let retrieval = freedom_ipfs_node_retrieval_stats(node);
            assert!(retrieval.cache_hits > 0);
            assert_eq!(retrieval.http_provider_blocks, 0);
            assert_eq!(retrieval.bitswap_blocks, 0);
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert!(diagnostics.cache_hits > 0);
            assert_eq!(diagnostics.delegated_provider_lookups, 0);
            assert_eq!(diagnostics.dht_provider_lookups, 0);
            assert_eq!(diagnostics.gateway_running, 1);

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn offline_routing_mode_uses_persistent_name_cache() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"offline ipns cache";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            (*node)
                .store
                .put_name_record(
                    "example.com",
                    &format!("/ipfs/{cid}"),
                    Duration::from_secs(60),
                )
                .unwrap();

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                ptr::null(),
                ROUTING_MODE_OFFLINE,
                1,
                0,
                0,
            ));

            assert_gateway_health(node);
            assert_gateway_path(node, "/ipns/example.com", data);
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn online_gateway_idles_without_network_work_before_requests() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_AUTO,
                1,
                1,
                1,
            ));

            assert_gateway_health(node);
            std::thread::sleep(Duration::from_millis(100));

            assert_eq!(
                freedom_ipfs_node_retrieval_stats(node),
                FreedomIpfsRetrievalStats::default()
            );
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert_eq!(diagnostics.block_count, 0);
            assert_eq!(diagnostics.total_bytes, 0);
            assert_eq!(diagnostics.cache_hits, 0);
            assert_eq!(diagnostics.http_provider_blocks, 0);
            assert_eq!(diagnostics.bitswap_blocks, 0);
            assert_eq!(diagnostics.delegated_provider_lookups, 0);
            assert_eq!(diagnostics.dht_provider_lookups, 0);
            assert_eq!(diagnostics.active_preload_count, 0);
            assert_eq!(diagnostics.gateway_running, 1);
            assert_eq!(diagnostics.lifecycle_background, 0);

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn reports_mobile_transport_and_routing_stats() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());
            assert_eq!(
                freedom_ipfs_node_retrieval_stats(node),
                FreedomIpfsRetrievalStats::default()
            );
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );
            assert_eq!(freedom_ipfs_node_active_preload_count(node), 0);
            assert_eq!(
                freedom_ipfs_node_diagnostics(node),
                FreedomIpfsDiagnostics::default()
            );

            let data = b"mobile transport stats";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let addr = CString::new("127.0.0.1:0").unwrap();
            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_DELEGATED,
                1,
                0,
                0,
            ));

            assert_gateway_path(node, &format!("/ipfs/{cid}"), data);
            let retrieval = freedom_ipfs_node_retrieval_stats(node);
            assert!(retrieval.cache_hits > 0);
            assert_eq!(retrieval.http_provider_blocks, 0);
            assert_eq!(retrieval.bitswap_blocks, 0);
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert_eq!(diagnostics.block_count, 1);
            assert_eq!(diagnostics.total_bytes, data.len() as u64);
            assert!(diagnostics.cache_hits > 0);
            assert_eq!(diagnostics.http_provider_blocks, 0);
            assert_eq!(diagnostics.bitswap_blocks, 0);
            assert_eq!(diagnostics.delegated_provider_lookups, 0);
            assert_eq!(diagnostics.delegated_provider_results, 0);
            assert_eq!(diagnostics.delegated_provider_errors, 0);
            assert_eq!(diagnostics.dht_provider_lookups, 0);
            assert_eq!(diagnostics.dht_provider_results, 0);
            assert_eq!(diagnostics.dht_provider_errors, 0);
            assert_eq!(diagnostics.active_preload_count, 0);
            assert_eq!(diagnostics.gateway_running, 1);
            assert_eq!(diagnostics.lifecycle_background, 0);

            assert!(freedom_ipfs_node_enter_background(node));
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert_eq!(diagnostics.gateway_running, 1);
            assert_eq!(diagnostics.lifecycle_background, 1);

            assert!(freedom_ipfs_node_enter_foreground(node));
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert_eq!(diagnostics.gateway_running, 1);
            assert_eq!(diagnostics.lifecycle_background, 0);

            assert!(freedom_ipfs_node_stop_gateway(node));
            assert_eq!(
                freedom_ipfs_node_retrieval_stats(node),
                FreedomIpfsRetrievalStats::default()
            );
            assert_eq!(
                freedom_ipfs_node_routing_stats(node),
                FreedomIpfsRoutingStats::default()
            );
            let diagnostics = freedom_ipfs_node_diagnostics(node);
            assert_eq!(diagnostics.block_count, 1);
            assert_eq!(diagnostics.total_bytes, data.len() as u64);
            assert_eq!(diagnostics.cache_hits, 0);
            assert_eq!(diagnostics.http_provider_blocks, 0);
            assert_eq!(diagnostics.bitswap_blocks, 0);
            assert_eq!(diagnostics.gateway_running, 0);
            assert_eq!(diagnostics.lifecycle_background, 0);
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn progress_snapshot_records_gateway_request_phases() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());
            assert!(freedom_ipfs_node_clear_progress(node));

            let data = b"progress fixture";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway(node, addr.as_ptr()));

            let path = format!("/ipfs/{cid}");
            let top_level_path = format!("{path}?top=1");
            let response = gateway_response_with_headers(
                node,
                &path,
                &[
                    ("X-Freedom-Request-ID", "4242"),
                    ("X-Freedom-Parent-Request-ID", "7"),
                    ("X-Freedom-Top-Level-Path", &top_level_path),
                ],
            );
            assert!(
                response.as_bytes().ends_with(data),
                "response did not end with expected body: {response}"
            );

            let snapshot = progress_snapshot_json(node);
            let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
            let events = value["events"].as_array().unwrap();
            assert!(value["generated_at_unix_ms"].as_u64().unwrap() > 0);
            let has_active_request = value["active"]
                .as_array()
                .unwrap()
                .iter()
                .any(|target| target["kind"] == "gateway_request" && target["id"] == 4242);
            assert!(!has_active_request, "{snapshot}");
            assert_eq!(value["event_count"].as_u64().unwrap(), events.len() as u64);
            assert!(
                events.iter().any(|event| event["path"] == path
                    && event["phase"] == "started"
                    && event["kind"] == "gateway_request"
                    && event["target_id"] == 4242
                    && event["parent_id"] == 7
                    && event["top_level_path"] == top_level_path),
                "{snapshot}"
            );
            assert!(
                events.iter().any(|event| event["path"] == path
                    && event["phase"] == "completed"
                    && event["status"] == "completed"
                    && event["blocks_loaded"] == 0
                    && event["retry_count"] == 0),
                "{snapshot}"
            );
            assert!(
                events
                    .iter()
                    .any(|event| event["path"] == path && event["raw_phase"] == "unixfs_resource"),
                "{snapshot}"
            );

            assert!(freedom_ipfs_node_stop_gateway(node));
            freedom_ipfs_node_free(node);
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
            "test",
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "bitswap_connection_error"),
                ("request_id", "1"),
                ("error", "timeout"),
            ]),
            "test",
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "block_fetch_total"),
                ("request_id", "1"),
                ("source", "bitswap"),
            ]),
            "test",
        );
        recorder.record_event(
            span,
            progress_fields([
                ("phase", "request_done"),
                ("request_id", "1"),
                ("status", "200"),
            ]),
            "test",
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
            "test",
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "unixfs_resource"),
                ("resource", "file"),
                ("file_len", "600000"),
            ]),
            "test",
        );
        recorder.record_event(
            span.clone(),
            progress_fields([
                ("phase", "request_done"),
                ("status", "200"),
                ("body_mode", "stream"),
            ]),
            "test",
        );
        recorder.record_event(
            span,
            progress_fields([
                ("phase", "gateway_stream_done"),
                ("body_len", "600000"),
                ("chunks", "10"),
            ]),
            "test",
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
            "test",
        );
        recorder.record_event(
            child_span,
            progress_fields([("phase", "request_start")]),
            "test",
        );
        recorder.record_event(
            second_child_span,
            progress_fields([("phase", "request_start")]),
            "test",
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

    #[test]
    fn restarts_online_gateway_for_routing_mode_changes() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(freedom_ipfs_node_start_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_DELEGATED,
                1,
                0,
                0,
            ));
            let first_url = gateway_url_string(node);
            assert_gateway_health(node);

            assert!(!freedom_ipfs_node_restart_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                99,
                1,
                0,
                0,
            ));
            assert_eq!(gateway_url_string(node), first_url);
            assert_gateway_health(node);

            let non_loopback_addr = CString::new("0.0.0.0:0").unwrap();
            assert!(!freedom_ipfs_node_restart_gateway_online_with_config_v2(
                node,
                non_loopback_addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_AUTO,
                1,
                0,
                0,
            ));
            assert_eq!(gateway_url_string(node), first_url);
            assert_gateway_health(node);

            assert!(freedom_ipfs_node_restart_gateway_online_with_config_v2(
                node,
                addr.as_ptr(),
                router.as_ptr(),
                ROUTING_MODE_LIGHT_DHT,
                1,
                5,
                2,
            ));
            assert_gateway_health(node);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn parses_comma_separated_delegated_router_endpoints() {
        assert_eq!(
            delegated_router_endpoints(" https://one.example/routing/v1, ,https://two.example "),
            vec![
                "https://one.example/routing/v1".to_string(),
                "https://two.example".to_string()
            ]
        );
        assert!(delegated_router_endpoints(" , ").is_empty());
    }

    #[test]
    fn rejects_invalid_routing_mode() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(!freedom_ipfs_node_start_gateway_online_with_config(
                node,
                addr.as_ptr(),
                ptr::null(),
                99,
                0,
            ));
            assert!(freedom_ipfs_node_gateway_url(node).is_null());
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn preloads_gateway_path_and_allows_cancel() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"preload me";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway(node, addr.as_ptr()));

            let bad_path = CString::new("https://example.com/ipfs/not-local").unwrap();
            assert_eq!(freedom_ipfs_node_preload_path(node, bad_path.as_ptr()), 0);

            let path = CString::new(format!("/ipfs/{cid}")).unwrap();
            let task_id = freedom_ipfs_node_preload_path(node, path.as_ptr());
            assert!(task_id > 0);
            assert!(freedom_ipfs_node_cancel_preload(node, task_id));
            assert!(!freedom_ipfs_node_cancel_preload(node, task_id));

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn normalizes_preload_paths_and_uris() {
        let cid = cid_from_data(CODEC_RAW, b"preload cid");

        assert_eq!(
            normalize_preload_path(&format!("{cid}")),
            Some(format!("/ipfs/{cid}"))
        );
        assert_eq!(
            normalize_preload_path(&format!("ipfs://{cid}/index.html?x=1")),
            Some(format!("/ipfs/{cid}/index.html?x=1"))
        );
        assert_eq!(
            normalize_preload_path("ipns://example.com/site"),
            Some("/ipns/example.com/site".to_string())
        );
        assert_eq!(
            normalize_preload_path(" /ipfs/bafyfixture "),
            Some("/ipfs/bafyfixture".to_string())
        );
        assert_eq!(normalize_preload_path("https://example.com/ipfs/no"), None);
        assert_eq!(normalize_preload_path("ipfs://"), None);
    }

    #[test]
    fn reports_and_clears_cache_stats() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"mobile stats";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();

            assert_eq!(freedom_ipfs_node_block_count(node), 1);
            assert_eq!(freedom_ipfs_node_total_bytes(node), data.len() as u64);
            assert!(freedom_ipfs_node_clear_cache(node));
            assert_eq!(freedom_ipfs_node_block_count(node), 0);
            assert_eq!(freedom_ipfs_node_total_bytes(node), 0);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn exports_cache_as_car_buffer() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"mobile export";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();

            let buffer = freedom_ipfs_node_export_car(node);
            assert!(!buffer.data.is_null());
            assert!(buffer.len > 0);
            let bytes = std::slice::from_raw_parts(buffer.data, buffer.len);
            let car = parse_car_v1(bytes).unwrap();
            assert_eq!(car.blocks.len(), 1);
            assert_eq!(car.blocks[0].cid, cid);
            assert_eq!(car.blocks[0].data, data);
            freedom_ipfs_buffer_free(buffer);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn trims_cache_to_requested_budget() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let first = vec![1u8; 16];
            let second = vec![2u8; 16];
            let first_cid = cid_from_data(CODEC_RAW, &first);
            let second_cid = cid_from_data(CODEC_RAW, &second);
            (*node).store.put_block(&first_cid, &first).unwrap();
            (*node).store.put_block(&second_cid, &second).unwrap();

            assert!(freedom_ipfs_node_trim_cache(node, 20));
            assert!(freedom_ipfs_node_total_bytes(node) <= 20);
            assert_eq!(freedom_ipfs_node_block_count(node), 1);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn lifecycle_hooks_cancel_preloads_and_trim_cache() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let first = vec![1u8; 16];
            let second = vec![2u8; 16];
            let first_cid = cid_from_data(CODEC_RAW, &first);
            let second_cid = cid_from_data(CODEC_RAW, &second);
            (*node).store.put_block(&first_cid, &first).unwrap();
            (*node).store.put_block(&second_cid, &second).unwrap();

            let addr = CString::new("127.0.0.1:0").unwrap();
            assert!(freedom_ipfs_node_start_gateway(node, addr.as_ptr()));
            let path = CString::new(format!("/ipfs/{first_cid}")).unwrap();
            let task_id = freedom_ipfs_node_preload_path(node, path.as_ptr());
            assert!(task_id > 0);

            assert!(freedom_ipfs_node_enter_background(node));
            assert!(!freedom_ipfs_node_cancel_preload(node, task_id));
            assert_eq!(
                *(*node).lifecycle_state.lock().unwrap(),
                LifecycleState::Background
            );
            assert_gateway_health(node);

            assert!(freedom_ipfs_node_enter_foreground(node));
            assert_eq!(
                *(*node).lifecycle_state.lock().unwrap(),
                LifecycleState::Foreground
            );

            assert!(freedom_ipfs_node_handle_low_memory(node, 20));
            assert!(freedom_ipfs_node_total_bytes(node) <= 20);
            assert_eq!(freedom_ipfs_node_block_count(node), 1);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn lifecycle_preload_cancellation_clears_progress_target() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());
            assert!(freedom_ipfs_node_clear_progress(node));

            let preload_id = 77;
            tracing::info!(
                phase = "preload_start",
                preload_id,
                path = "/ipfs/bafyprogress"
            );
            let task = (*node).runtime.spawn(async {
                std::future::pending::<()>().await;
            });
            (*node)
                .preload_tasks
                .lock()
                .unwrap()
                .insert(preload_id, task);

            let snapshot = progress_snapshot_json(node);
            let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
            let active = value["active"].as_array().unwrap();
            assert!(
                active.iter().any(|target| target["kind"] == "preload"
                    && target["id"] == preload_id
                    && target["status"] == "active"),
                "{snapshot}"
            );

            assert!(freedom_ipfs_node_enter_background(node));

            let snapshot = progress_snapshot_json(node);
            let value: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
            let active = value["active"].as_array().unwrap();
            let events = value["events"].as_array().unwrap();
            assert!(
                !active
                    .iter()
                    .any(|target| target["kind"] == "preload" && target["id"] == preload_id),
                "{snapshot}"
            );
            assert!(
                events.iter().any(|event| event["kind"] == "preload"
                    && event["target_id"] == preload_id
                    && event["phase"] == "cancelled"
                    && event["status"] == "cancelled"),
                "{snapshot}"
            );

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn network_change_clears_provider_metadata_without_blocks() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"mobile provider metadata";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            (*node)
                .store
                .put_provider_records(
                    &cid,
                    &[CachedProviderRecord {
                        id: Some("peer".to_string()),
                        addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                    }],
                    Duration::from_secs(60),
                )
                .unwrap();
            (*node)
                .store
                .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
                .unwrap();

            assert!(freedom_ipfs_node_handle_network_change(node));

            assert_eq!(freedom_ipfs_node_block_count(node), 1);
            assert_eq!((*node).store.get_provider_records(&cid).unwrap(), None);
            assert!(!(*node).store.is_bad_provider("peer").unwrap());

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn opens_persistent_data_dir_cache() {
        unsafe {
            let tempdir = tempfile::tempdir().unwrap();
            let data_dir = CString::new(tempdir.path().to_str().unwrap()).unwrap();
            let node = freedom_ipfs_node_new_with_data_dir(data_dir.as_ptr(), 1024 * 1024);
            assert!(!node.is_null());

            let data = b"persisted mobile cache";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            assert_eq!(freedom_ipfs_node_block_count(node), 1);
            freedom_ipfs_node_free(node);

            let reopened = freedom_ipfs_node_new_with_data_dir(data_dir.as_ptr(), 1024 * 1024);
            assert!(!reopened.is_null());
            assert_eq!(freedom_ipfs_node_block_count(reopened), 1);
            assert_eq!(freedom_ipfs_node_total_bytes(reopened), data.len() as u64);
            freedom_ipfs_node_free(reopened);
        }
    }

    #[test]
    fn native_gateway_request_reads_full_body_and_metadata() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"native gateway ffi body";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let path = format!("/ipfs/{cid}");
            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": path,
                    "request_id": 9001,
                    "parent_request_id": 77,
                    "top_level_path": "/ipfs/top"
                }),
            );

            let metadata = wait_native_gateway_response(node, handle);
            assert_eq!(metadata["handle"], handle);
            assert!(
                metadata["state"] == "streaming" || metadata["state"] == "completed",
                "{metadata}"
            );
            assert_eq!(metadata["status"], 200);
            assert_eq!(metadata["path"], path);
            assert_eq!(metadata["namespace"], "ipfs");
            assert_eq!(metadata["request_id"], 9001);
            assert_eq!(metadata["parent_request_id"], 77);
            assert_eq!(metadata["top_level_path"], "/ipfs/top");
            let content_length = data.len().to_string();
            assert!(metadata["headers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|header| {
                    header["name"] == "content-length" && header["value"] == content_length
                }));

            let body = read_native_gateway_body(node, handle, 4);
            assert_eq!(body, data);
            let metadata = native_gateway_response_json(node, handle);
            assert_eq!(metadata["state"], "completed");
            assert!(freedom_ipfs_gateway_request_free(node, handle));
            assert_eq!(
                freedom_ipfs_gateway_request_read(node, handle, [0u8; 1].as_mut_ptr(), 1).status,
                FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE
            );

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_request_supports_head_and_range() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"0123456789abcdef";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let path = format!("/ipfs/{cid}");

            let head_handle = start_native_gateway_request(
                node,
                json!({
                    "method": "HEAD",
                    "path": path
                }),
            );
            let head = wait_native_gateway_response(node, head_handle);
            assert_eq!(head["status"], 200);
            let content_length = data.len().to_string();
            assert!(head["headers"].as_array().unwrap().iter().any(|header| {
                header["name"] == "content-length" && header["value"] == content_length
            }));
            assert_eq!(
                read_native_gateway_status(node, head_handle),
                FREEDOM_IPFS_GATEWAY_READ_END
            );
            assert!(freedom_ipfs_gateway_request_free(node, head_handle));

            let range_handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": path,
                    "headers": [
                        { "name": "Range", "value": "bytes=2-5" }
                    ]
                }),
            );
            let range = wait_native_gateway_response(node, range_handle);
            assert_eq!(range["status"], 206);
            assert!(range["headers"].as_array().unwrap().iter().any(|header| {
                header["name"] == "content-range" && header["value"] == "bytes 2-5/16"
            }));
            assert_eq!(read_native_gateway_body(node, range_handle, 2), b"2345");
            assert!(freedom_ipfs_gateway_request_free(node, range_handle));

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_request_cancel_and_free_are_safe() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = vec![42u8; 512 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            (*node).store.put_block(&cid, &data).unwrap();
            let path = format!("/ipfs/{cid}");
            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": path
                }),
            );
            let _ = wait_native_gateway_response(node, handle);
            let mut buffer = [0u8; 4096];
            let first = poll_native_gateway_read(node, handle, &mut buffer);
            assert_eq!(first.status, FREEDOM_IPFS_GATEWAY_READ_BYTES);
            assert!(first.bytes_read > 0);

            assert!(freedom_ipfs_gateway_request_cancel(node, handle));
            assert_eq!(
                read_native_gateway_status(node, handle),
                FREEDOM_IPFS_GATEWAY_READ_CANCELLED
            );
            let metadata = native_gateway_response_json(node, handle);
            assert_eq!(metadata["state"], "cancelled");
            assert!(freedom_ipfs_gateway_request_free(node, handle));
            assert!(!freedom_ipfs_gateway_request_free(node, handle));
            assert!(!freedom_ipfs_gateway_request_cancel(node, handle));

            for _ in 0..8 {
                let handle = start_native_gateway_request(
                    node,
                    json!({
                        "method": "GET",
                        "path": path
                    }),
                );
                assert!(freedom_ipfs_gateway_request_cancel(node, handle));
                assert!(freedom_ipfs_gateway_request_free(node, handle));
            }
            assert_eq!(
                (*node).native_gateway_state.lock().unwrap().requests.len(),
                0
            );

            let mut buffer = [0u8; 8];
            assert_eq!(
                freedom_ipfs_gateway_request_read(node, 987_654, buffer.as_mut_ptr(), buffer.len())
                    .status,
                FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE
            );
            let invalid = native_gateway_response_json(node, 987_654);
            assert_eq!(invalid["state"], "failed");
            assert_eq!(invalid["error"]["code"], "invalid_handle");

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_stop_cancels_registered_requests() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = vec![7u8; 512 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            (*node).store.put_block(&cid, &data).unwrap();
            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": format!("/ipfs/{cid}")
                }),
            );
            let _ = wait_native_gateway_response(node, handle);
            assert_eq!(
                (*node).native_gateway_state.lock().unwrap().requests.len(),
                1
            );

            assert!(freedom_ipfs_node_stop_gateway(node));
            assert_eq!(
                (*node).native_gateway_state.lock().unwrap().requests.len(),
                0
            );
            let mut buffer = [0u8; 8];
            assert_eq!(
                freedom_ipfs_gateway_request_read(node, handle, buffer.as_mut_ptr(), buffer.len())
                    .status,
                FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE
            );

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_wait_api_returns_metadata_and_bytes() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"native wait api body";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let path = format!("/ipfs/{cid}");
            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": path
                }),
            );

            let metadata = native_gateway_response_json_wait_ffi(node, handle, 5_000);
            assert!(
                metadata["state"] == "streaming" || metadata["state"] == "completed",
                "{metadata}"
            );
            assert_eq!(metadata["status"], 200);

            let mut buffer = [0u8; 3];
            let first = native_gateway_read_wait_ffi(node, handle, &mut buffer, 5_000);
            assert_eq!(first.status, FREEDOM_IPFS_GATEWAY_READ_BYTES);
            assert!(first.bytes_read > 0);

            let mut body = Vec::from(&buffer[..first.bytes_read]);
            body.extend(read_native_gateway_body_wait(node, handle, 3));
            assert_eq!(body, data);
            assert!(freedom_ipfs_gateway_request_free(node, handle));

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_wait_api_timeout_zero_matches_nonblocking() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let (handle, _body_tx) = insert_pending_native_gateway_request(node, false);
            let metadata = native_gateway_response_json_wait_ffi(node, handle, 0);
            assert_eq!(metadata["state"], "pending");
            let metadata = native_gateway_response_json_wait_ffi(node, handle, 10);
            assert_eq!(metadata["state"], "pending");

            let mut buffer = [0u8; 8];
            assert_eq!(
                native_gateway_read_wait_ffi(node, handle, &mut buffer, 0).status,
                FREEDOM_IPFS_GATEWAY_READ_PENDING
            );
            assert_eq!(
                native_gateway_read_wait_ffi(node, handle, &mut buffer, 10).status,
                FREEDOM_IPFS_GATEWAY_READ_PENDING
            );
            assert!(freedom_ipfs_gateway_request_free(node, handle));

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_wait_api_cancel_and_free_wake_waiters() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());
            let node_addr = node as usize;

            let (response_handle, _response_tx) =
                insert_pending_native_gateway_request(node, false);
            let response_waiter = std::thread::spawn(move || {
                native_gateway_response_json_wait_ffi(
                    node_addr as *mut FreedomIpfsNode,
                    response_handle,
                    5_000,
                )
            });
            std::thread::sleep(Duration::from_millis(25));
            assert!(freedom_ipfs_gateway_request_cancel(node, response_handle));
            let metadata = response_waiter.join().unwrap();
            assert_eq!(metadata["state"], "cancelled");
            assert!(freedom_ipfs_gateway_request_free(node, response_handle));

            let node_addr = node as usize;
            let (read_handle, _read_tx) = insert_pending_native_gateway_request(node, true);
            let read_waiter = std::thread::spawn(move || {
                let mut buffer = [0u8; 8];
                native_gateway_read_wait_ffi(
                    node_addr as *mut FreedomIpfsNode,
                    read_handle,
                    &mut buffer,
                    5_000,
                )
            });
            std::thread::sleep(Duration::from_millis(25));
            assert!(freedom_ipfs_gateway_request_cancel(node, read_handle));
            assert_eq!(
                read_waiter.join().unwrap().status,
                FREEDOM_IPFS_GATEWAY_READ_CANCELLED
            );
            assert!(freedom_ipfs_gateway_request_free(node, read_handle));

            let node_addr = node as usize;
            let (free_handle, _free_tx) = insert_pending_native_gateway_request(node, true);
            let free_waiter = std::thread::spawn(move || {
                let mut buffer = [0u8; 8];
                native_gateway_read_wait_ffi(
                    node_addr as *mut FreedomIpfsNode,
                    free_handle,
                    &mut buffer,
                    5_000,
                )
            });
            std::thread::sleep(Duration::from_millis(25));
            assert!(freedom_ipfs_gateway_request_free(node, free_handle));
            assert_eq!(
                free_waiter.join().unwrap().status,
                FREEDOM_IPFS_GATEWAY_READ_CANCELLED
            );
            let mut buffer = [0u8; 8];
            assert_eq!(
                native_gateway_read_wait_ffi(node, free_handle, &mut buffer, 0).status,
                FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE
            );

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_event_api_reports_readiness_and_lifecycle_events() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let idle = native_gateway_next_event(node, 0);
            assert_eq!(idle.status, FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT);
            assert_eq!(
                freedom_ipfs_gateway_wait_next_event(ptr::null_mut(), 0).status,
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE
            );

            let (response_handle, body_tx) = insert_pending_native_gateway_request(node, true);
            native_gateway_push_event(
                node,
                response_handle,
                FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY,
            );
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.status, FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK);
            assert_eq!(event.request_handle, response_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY);
            let metadata = native_gateway_response_json(node, response_handle);
            assert_eq!(metadata["status"], 200);

            body_tx
                .try_send(NativeBodyMessage::Data(b"ready".to_vec()))
                .unwrap();
            native_gateway_push_event(node, response_handle, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, response_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
            let mut buffer = [0u8; 8];
            let read = freedom_ipfs_gateway_request_read(
                node,
                response_handle,
                buffer.as_mut_ptr(),
                buffer.len(),
            );
            assert_eq!(read.status, FREEDOM_IPFS_GATEWAY_READ_BYTES);
            assert_eq!(&buffer[..read.bytes_read], b"ready");

            complete_pending_native_gateway_request(node, response_handle);
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, response_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_END);
            assert_eq!(
                freedom_ipfs_gateway_request_read(
                    node,
                    response_handle,
                    buffer.as_mut_ptr(),
                    buffer.len(),
                )
                .status,
                FREEDOM_IPFS_GATEWAY_READ_END
            );
            assert!(freedom_ipfs_gateway_request_free(node, response_handle));
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, response_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);

            let (failed_handle, _failed_tx) = insert_pending_native_gateway_request(node, true);
            fail_pending_native_gateway_request(node, failed_handle);
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, failed_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_FAILED);
            assert!(freedom_ipfs_gateway_request_free(node, failed_handle));
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, failed_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);

            let (cancel_handle, _cancel_tx) = insert_pending_native_gateway_request(node, true);
            assert!(freedom_ipfs_gateway_request_cancel(node, cancel_handle));
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, cancel_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED);
            assert!(freedom_ipfs_gateway_request_free(node, cancel_handle));
            let event = native_gateway_next_event(node, 1_000);
            assert_eq!(event.request_handle, cancel_handle);
            assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);

            let node_addr = node as usize;
            let (free_handle, _free_tx) = insert_pending_native_gateway_request(node, true);
            let free_waiter =
                std::thread::spawn(move || native_gateway_next_event(node_addr as *mut _, 5_000));
            std::thread::sleep(Duration::from_millis(25));
            assert!(freedom_ipfs_gateway_request_free(node, free_handle));
            let mut event = free_waiter.join().unwrap();
            assert_eq!(event.request_handle, free_handle);
            assert!(
                event.events
                    & (FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED
                        | FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED)
                    != 0,
                "unexpected free waiter event {event:?}"
            );
            if event.events & FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED == 0 {
                event = native_gateway_next_event(node, 1_000);
                assert_eq!(event.request_handle, free_handle);
                assert_event_contains(event, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);
            }

            let node_addr = node as usize;
            let waiter =
                std::thread::spawn(move || native_gateway_next_event(node_addr as *mut _, 5_000));
            std::thread::sleep(Duration::from_millis(25));
            assert!(freedom_ipfs_node_stop_gateway(node));
            let event = waiter.join().unwrap();
            assert_eq!(
                event.status,
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED
            );

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_event_api_coalesces_and_preserves_fairness() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let (noisy_handle, _noisy_tx) = insert_pending_native_gateway_request(node, true);
            let (small_handle, _small_tx) = insert_pending_native_gateway_request(node, true);

            for _ in 0..10 {
                native_gateway_push_event(
                    node,
                    noisy_handle,
                    FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY,
                );
            }
            native_gateway_push_event(
                node,
                small_handle,
                FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY,
            );

            let first = native_gateway_next_event(node, 1_000);
            assert_eq!(first.request_handle, noisy_handle);
            assert_event_contains(first, FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);

            let second = native_gateway_next_event(node, 1_000);
            assert_eq!(second.request_handle, small_handle);
            assert_event_contains(second, FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY);

            let idle = native_gateway_next_event(node, 0);
            assert_eq!(idle.status, FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT);

            assert!(freedom_ipfs_gateway_request_free(node, noisy_handle));
            assert!(freedom_ipfs_gateway_request_free(node, small_handle));
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_event_api_preserves_head_range_and_not_modified() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"0123456789abcdef";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();
            let path = format!("/ipfs/{cid}");

            let get = run_single_event_request(
                node,
                json!({
                    "method": "GET",
                    "path": path
                }),
            );
            assert_eq!(get.status, Some(200));
            assert_eq!(get.body, data);
            let etag = get
                .header("etag")
                .expect("GET response should include ETag");

            let head = run_single_event_request(
                node,
                json!({
                    "method": "HEAD",
                    "path": path
                }),
            );
            assert_eq!(head.status, Some(200));
            assert!(head.body.is_empty());
            assert_eq!(head.header("content-length").as_deref(), Some("16"));

            let range = run_single_event_request(
                node,
                json!({
                    "method": "GET",
                    "path": path,
                    "headers": [
                        { "name": "Range", "value": "bytes=2-5" }
                    ]
                }),
            );
            assert_eq!(range.status, Some(206));
            assert_eq!(range.body, b"2345");
            assert_eq!(
                range.header("content-range").as_deref(),
                Some("bytes 2-5/16")
            );

            let not_modified = run_single_event_request(
                node,
                json!({
                    "method": "GET",
                    "path": path,
                    "headers": [
                        { "name": "If-None-Match", "value": etag }
                    ]
                }),
            );
            assert_eq!(not_modified.status, Some(304));
            assert!(not_modified.body.is_empty());

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn native_gateway_event_api_drives_50_requests_with_one_dispatcher() {
        native_gateway_event_api_drives_many_requests(1);
    }

    #[test]
    fn native_gateway_event_api_drives_50_requests_with_four_dispatchers() {
        native_gateway_event_api_drives_many_requests(4);
    }

    #[test]
    fn native_gateway_event_api_registers_cache_hot_handles_before_events() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let mut fixtures = Vec::new();
            let mut expected = HashMap::new();
            for index in 0..50 {
                let data = format!("cache hot native event body {index:02}").into_bytes();
                let cid = cid_from_data(CODEC_RAW, &data);
                (*node).store.put_block(&cid, &data).unwrap();
                fixtures.push((cid, data));
            }

            let node_addr = node as usize;
            let dispatcher =
                std::thread::spawn(move || -> Result<HashMap<u64, Vec<u8>>, String> {
                    let mut states: HashMap<u64, EventDrivenRequestState> = HashMap::new();
                    let mut completed = 0usize;
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut buffer = [0u8; 7];
                    while completed < 50 {
                        if Instant::now() >= deadline {
                            return Err(format!(
                                "dispatcher timed out after completing {completed} requests"
                            ));
                        }
                        let event = freedom_ipfs_gateway_wait_next_event(
                            node_addr as *mut FreedomIpfsNode,
                            100,
                        );
                        match event.status {
                            FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT => continue,
                            FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK => {}
                            status => return Err(format!("unexpected event status {status}")),
                        }
                        let state = states.entry(event.request_handle).or_default();
                        if state.done {
                            continue;
                        }
                        if event.events & FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY != 0 {
                            let metadata = native_gateway_response_json(
                                node_addr as *mut FreedomIpfsNode,
                                event.request_handle,
                            );
                            if metadata["error"]["code"] == "invalid_handle" {
                                return Err(format!(
                                    "received event before handle {} was registered",
                                    event.request_handle
                                ));
                            }
                            state.status = metadata["status"].as_u64().map(|status| status as u16);
                        }
                        if event.events
                            & (FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY
                                | FREEDOM_IPFS_GATEWAY_EVENT_END
                                | FREEDOM_IPFS_GATEWAY_EVENT_FAILED
                                | FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED
                                | FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED)
                            != 0
                        {
                            loop {
                                let read = freedom_ipfs_gateway_request_read(
                                    node_addr as *mut FreedomIpfsNode,
                                    event.request_handle,
                                    buffer.as_mut_ptr(),
                                    buffer.len(),
                                );
                                match read.status {
                                    FREEDOM_IPFS_GATEWAY_READ_BYTES => {
                                        state.body.extend_from_slice(&buffer[..read.bytes_read]);
                                    }
                                    FREEDOM_IPFS_GATEWAY_READ_PENDING => break,
                                    FREEDOM_IPFS_GATEWAY_READ_END => {
                                        state.done = true;
                                        completed += 1;
                                        break;
                                    }
                                    status => {
                                        return Err(format!(
                                            "request {} read failed with status {status}",
                                            event.request_handle
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    Ok(states
                        .into_iter()
                        .map(|(handle, state)| (handle, state.body))
                        .collect())
                });

            let mut handles = Vec::new();
            for (cid, data) in fixtures {
                let handle = start_native_gateway_request(
                    node,
                    json!({
                        "method": "GET",
                        "path": format!("/ipfs/{cid}")
                    }),
                );
                expected.insert(handle, data);
                handles.push(handle);
            }

            let results = dispatcher
                .join()
                .expect("event dispatcher panicked")
                .expect("event dispatcher failed");
            assert_eq!(results.len(), expected.len());
            for (handle, expected_body) in expected {
                assert_eq!(results.get(&handle), Some(&expected_body));
            }
            for handle in handles {
                assert!(freedom_ipfs_gateway_request_free(node, handle));
            }

            freedom_ipfs_node_free(node);
        }
    }

    fn native_gateway_event_api_drives_many_requests(dispatchers: usize) {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let mut expected = HashMap::new();
            let mut handles = Vec::new();
            for index in 0..50 {
                let data = format!("native event body {index:02}").into_bytes();
                let cid = cid_from_data(CODEC_RAW, &data);
                (*node).store.put_block(&cid, &data).unwrap();
                let handle = start_native_gateway_request(
                    node,
                    json!({
                        "method": "GET",
                        "path": format!("/ipfs/{cid}")
                    }),
                );
                expected.insert(handle, data);
                handles.push(handle);
            }

            let results = drive_event_requests(node, &handles, dispatchers);
            assert_eq!(results.len(), expected.len());
            for (handle, expected_body) in expected {
                assert_eq!(
                    results.get(&handle).map(|result| &result.body),
                    Some(&expected_body)
                );
            }
            for handle in handles {
                assert!(freedom_ipfs_gateway_request_free(node, handle));
            }

            freedom_ipfs_node_free(node);
        }
    }

    unsafe fn assert_gateway_health(node: *mut FreedomIpfsNode) {
        let response = gateway_response(node, "/health");
        assert!(response.contains("200 OK"));
        assert!(response.ends_with("ok\n"));
    }

    unsafe fn assert_gateway_path(node: *mut FreedomIpfsNode, path: &str, expected: &[u8]) {
        let response = gateway_response(node, path);
        assert!(response.contains("200 OK"), "{response}");
        assert!(
            response.as_bytes().ends_with(expected),
            "response did not end with expected body: {response}"
        );
    }

    unsafe fn gateway_response(node: *mut FreedomIpfsNode, path: &str) -> String {
        gateway_response_with_headers(node, path, &[])
    }

    unsafe fn gateway_response_with_headers(
        node: *mut FreedomIpfsNode,
        path: &str,
        headers: &[(&str, &str)],
    ) -> String {
        let url = gateway_url_string(node);
        assert!(url.starts_with("http://127.0.0.1:"));

        let addr = url.strip_prefix("http://").unwrap();
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        let extra_headers = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: localhost\r\n{extra_headers}Connection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    unsafe fn gateway_url_string(node: *mut FreedomIpfsNode) -> String {
        let url_ptr = freedom_ipfs_node_gateway_url(node);
        assert!(!url_ptr.is_null());
        let url = CStr::from_ptr(url_ptr).to_str().unwrap().to_string();
        freedom_ipfs_string_free(url_ptr);
        url
    }

    unsafe fn progress_snapshot_json(node: *mut FreedomIpfsNode) -> String {
        let snapshot_ptr = freedom_ipfs_node_progress_snapshot_json(node);
        assert!(!snapshot_ptr.is_null());
        let snapshot = CStr::from_ptr(snapshot_ptr).to_str().unwrap().to_string();
        freedom_ipfs_string_free(snapshot_ptr);
        snapshot
    }

    unsafe fn start_native_gateway_request(
        node: *mut FreedomIpfsNode,
        request: serde_json::Value,
    ) -> u64 {
        let request = CString::new(request.to_string()).unwrap();
        let handle = freedom_ipfs_gateway_request_start(node, request.as_ptr());
        assert!(handle > 0);
        handle
    }

    unsafe fn native_gateway_response_json(
        node: *mut FreedomIpfsNode,
        handle: u64,
    ) -> serde_json::Value {
        let metadata_ptr = freedom_ipfs_gateway_request_response_json(node, handle);
        assert!(!metadata_ptr.is_null());
        let metadata = CStr::from_ptr(metadata_ptr).to_str().unwrap().to_string();
        freedom_ipfs_string_free(metadata_ptr);
        serde_json::from_str(&metadata).unwrap()
    }

    unsafe fn native_gateway_response_json_wait_ffi(
        node: *mut FreedomIpfsNode,
        handle: u64,
        timeout_ms: u64,
    ) -> serde_json::Value {
        let metadata_ptr =
            freedom_ipfs_gateway_request_response_json_wait(node, handle, timeout_ms);
        assert!(!metadata_ptr.is_null());
        let metadata = CStr::from_ptr(metadata_ptr).to_str().unwrap().to_string();
        freedom_ipfs_string_free(metadata_ptr);
        serde_json::from_str(&metadata).unwrap()
    }

    unsafe fn wait_native_gateway_response(
        node: *mut FreedomIpfsNode,
        handle: u64,
    ) -> serde_json::Value {
        let metadata = native_gateway_response_json_wait_ffi(node, handle, 5_000);
        assert_ne!(
            metadata["state"], "pending",
            "native gateway response metadata did not become ready"
        );
        metadata
    }

    unsafe fn read_native_gateway_body(
        node: *mut FreedomIpfsNode,
        handle: u64,
        buffer_len: usize,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        let mut buffer = vec![0u8; buffer_len];
        for _ in 0..5_000 {
            let result =
                freedom_ipfs_gateway_request_read(node, handle, buffer.as_mut_ptr(), buffer.len());
            match result.status {
                FREEDOM_IPFS_GATEWAY_READ_BYTES => {
                    body.extend_from_slice(&buffer[..result.bytes_read]);
                }
                FREEDOM_IPFS_GATEWAY_READ_END => return body,
                FREEDOM_IPFS_GATEWAY_READ_PENDING => {
                    std::thread::sleep(Duration::from_millis(2));
                }
                status => panic!("unexpected native gateway read status {status}"),
            }
        }
        panic!("native gateway body did not finish");
    }

    unsafe fn read_native_gateway_body_wait(
        node: *mut FreedomIpfsNode,
        handle: u64,
        buffer_len: usize,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        let mut buffer = vec![0u8; buffer_len];
        for _ in 0..5_000 {
            let result = native_gateway_read_wait_ffi(node, handle, &mut buffer, 5_000);
            match result.status {
                FREEDOM_IPFS_GATEWAY_READ_BYTES => {
                    body.extend_from_slice(&buffer[..result.bytes_read]);
                }
                FREEDOM_IPFS_GATEWAY_READ_END => return body,
                status => panic!("unexpected native gateway read status {status}"),
            }
        }
        panic!("native gateway body did not finish");
    }

    unsafe fn read_native_gateway_status(node: *mut FreedomIpfsNode, handle: u64) -> u32 {
        let mut buffer = [0u8; 8];
        for _ in 0..1_000 {
            let result =
                freedom_ipfs_gateway_request_read(node, handle, buffer.as_mut_ptr(), buffer.len());
            if result.status != FREEDOM_IPFS_GATEWAY_READ_PENDING {
                return result.status;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("native gateway read did not leave pending state");
    }

    unsafe fn poll_native_gateway_read(
        node: *mut FreedomIpfsNode,
        handle: u64,
        buffer: &mut [u8],
    ) -> FreedomIpfsGatewayReadResult {
        for _ in 0..1_000 {
            let result =
                freedom_ipfs_gateway_request_read(node, handle, buffer.as_mut_ptr(), buffer.len());
            if result.status != FREEDOM_IPFS_GATEWAY_READ_PENDING {
                return result;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("native gateway read did not produce a non-pending status");
    }

    unsafe fn native_gateway_read_wait_ffi(
        node: *mut FreedomIpfsNode,
        handle: u64,
        buffer: &mut [u8],
        timeout_ms: u64,
    ) -> FreedomIpfsGatewayReadResult {
        freedom_ipfs_gateway_request_read_wait(
            node,
            handle,
            buffer.as_mut_ptr(),
            buffer.len(),
            timeout_ms,
        )
    }

    unsafe fn native_gateway_next_event(
        node: *mut FreedomIpfsNode,
        timeout_ms: u64,
    ) -> FreedomIpfsGatewayEvent {
        freedom_ipfs_gateway_wait_next_event(node, timeout_ms)
    }

    unsafe fn native_gateway_push_event(node: *mut FreedomIpfsNode, handle: u64, events: u32) {
        (*node).native_gateway_events.push(handle, events);
    }

    fn assert_event_contains(event: FreedomIpfsGatewayEvent, expected: u32) {
        assert_eq!(
            event.events & expected,
            expected,
            "event {event:?} did not contain expected bit {expected}"
        );
    }

    unsafe fn complete_pending_native_gateway_request(node: *mut FreedomIpfsNode, handle: u64) {
        let request = native_gateway_request(&*node, handle).expect("request should exist");
        if let Ok(mut meta) = request.meta.lock() {
            meta.completed = true;
        }
        request.wake.notify();
        request.events.push(handle, FREEDOM_IPFS_GATEWAY_EVENT_END);
    }

    unsafe fn fail_pending_native_gateway_request(node: *mut FreedomIpfsNode, handle: u64) {
        let request = native_gateway_request(&*node, handle).expect("request should exist");
        if let Ok(mut meta) = request.meta.lock() {
            meta.error = Some(NativeGatewayErrorJson {
                code: "test_failure".to_string(),
                message: "test failure".to_string(),
            });
        }
        request.wake.notify();
        request
            .events
            .push(handle, FREEDOM_IPFS_GATEWAY_EVENT_FAILED);
    }

    unsafe fn insert_pending_native_gateway_request(
        node: *mut FreedomIpfsNode,
        response_ready: bool,
    ) -> (u64, mpsc::Sender<NativeBodyMessage>) {
        let (body_tx, body_rx) = mpsc::channel(NATIVE_BODY_CHANNEL_CAPACITY);
        let handle = (*node)
            .next_native_request_id
            .fetch_add(1, Ordering::Relaxed);
        let response = response_ready.then(|| NativeGatewayResponseMeta {
            status: 200,
            headers: Vec::new(),
        });
        let request = Arc::new(NativeGatewayRequest {
            handle,
            meta: Arc::new(Mutex::new(NativeGatewayRequestMeta {
                method: "GET".to_string(),
                path: "/ipfs/test".to_string(),
                namespace: "ipfs".to_string(),
                request_id: None,
                parent_request_id: None,
                top_level_path: None,
                response,
                completed: false,
                cancelled: false,
                error: None,
            })),
            wake: Arc::new(NativeGatewayRequestWake::new()),
            events: (*node).native_gateway_events.clone(),
            body_rx: Mutex::new(body_rx),
            pending: Mutex::new(VecDeque::new()),
            task: Mutex::new(None),
        });
        (*node)
            .native_gateway_state
            .lock()
            .unwrap()
            .requests
            .insert(handle, request);
        (handle, body_tx)
    }

    #[derive(Default)]
    struct EventDrivenRequestState {
        status: Option<u16>,
        headers: Vec<NativeGatewayHeader>,
        body: Vec<u8>,
        done: bool,
        failed_status: Option<u32>,
    }

    struct EventDrivenResponse {
        status: Option<u16>,
        headers: Vec<NativeGatewayHeader>,
        body: Vec<u8>,
    }

    impl EventDrivenResponse {
        fn header(&self, name: &str) -> Option<String> {
            self.headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case(name))
                .map(|header| header.value.clone())
        }
    }

    unsafe fn run_single_event_request(
        node: *mut FreedomIpfsNode,
        request: serde_json::Value,
    ) -> EventDrivenResponse {
        let handle = start_native_gateway_request(node, request);
        let mut results = drive_event_requests(node, &[handle], 1);
        assert!(freedom_ipfs_gateway_request_free(node, handle));
        results.remove(&handle).expect("request should complete")
    }

    unsafe fn drive_event_requests(
        node: *mut FreedomIpfsNode,
        handles: &[u64],
        dispatcher_count: usize,
    ) -> HashMap<u64, EventDrivenResponse> {
        let dispatcher_count = dispatcher_count.max(1);
        let mut states = HashMap::new();
        for handle in handles {
            states.insert(
                *handle,
                Arc::new(Mutex::new(EventDrivenRequestState::default())),
            );
        }
        let states = Arc::new(states);
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let total = handles.len();
        let node_addr = node as usize;
        let mut workers = Vec::new();
        for _ in 0..dispatcher_count {
            let states = states.clone();
            let completed = completed.clone();
            workers.push(std::thread::spawn(move || -> Result<(), String> {
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut buffer = [0u8; 5];
                while completed.load(Ordering::Acquire) < total {
                    if Instant::now() >= deadline {
                        return Err("event dispatcher timed out".to_string());
                    }
                    let event = unsafe {
                        freedom_ipfs_gateway_wait_next_event(node_addr as *mut FreedomIpfsNode, 100)
                    };
                    match event.status {
                        FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT => continue,
                        FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK => {}
                        status => {
                            return Err(format!("unexpected event status {status}"));
                        }
                    }
                    let Some(state) = states.get(&event.request_handle).cloned() else {
                        continue;
                    };
                    let mut state = state.lock().map_err(|_| "state poisoned".to_string())?;
                    if state.done {
                        continue;
                    }
                    if event.events & FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY != 0 {
                        let metadata = unsafe {
                            native_gateway_response_json(
                                node_addr as *mut FreedomIpfsNode,
                                event.request_handle,
                            )
                        };
                        state.status = metadata["status"].as_u64().map(|status| status as u16);
                        state.headers = serde_json::from_value(metadata["headers"].clone())
                            .map_err(|err| format!("failed to decode response headers: {err}"))?;
                    }
                    if event.events
                        & (FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY
                            | FREEDOM_IPFS_GATEWAY_EVENT_END
                            | FREEDOM_IPFS_GATEWAY_EVENT_FAILED
                            | FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED
                            | FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED)
                        != 0
                    {
                        loop {
                            let read = unsafe {
                                freedom_ipfs_gateway_request_read(
                                    node_addr as *mut FreedomIpfsNode,
                                    event.request_handle,
                                    buffer.as_mut_ptr(),
                                    buffer.len(),
                                )
                            };
                            match read.status {
                                FREEDOM_IPFS_GATEWAY_READ_BYTES => {
                                    state.body.extend_from_slice(&buffer[..read.bytes_read]);
                                }
                                FREEDOM_IPFS_GATEWAY_READ_PENDING => break,
                                FREEDOM_IPFS_GATEWAY_READ_END => {
                                    state.done = true;
                                    completed.fetch_add(1, Ordering::AcqRel);
                                    break;
                                }
                                status => {
                                    state.done = true;
                                    state.failed_status = Some(status);
                                    completed.fetch_add(1, Ordering::AcqRel);
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }));
        }
        for worker in workers {
            worker
                .join()
                .expect("event dispatcher panicked")
                .expect("event dispatcher failed");
        }
        let mut results = HashMap::new();
        for (handle, state) in states.iter() {
            let state = state.lock().unwrap();
            assert_eq!(
                state.failed_status, None,
                "request {handle} failed with {:?}",
                state.failed_status
            );
            assert!(state.done, "request {handle} did not finish");
            results.insert(
                *handle,
                EventDrivenResponse {
                    status: state.status,
                    headers: state.headers.clone(),
                    body: state.body.clone(),
                },
            );
        }
        results
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
