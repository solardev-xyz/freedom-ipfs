use axum::http::HeaderMap;
use freedom_ipfs_core::BlockProvider;
use freedom_ipfs_gateway::progress::{self, init_progress_tracing, ProgressRecorder};
use freedom_ipfs_gateway::{
    delegated_router_endpoints, normalize_content_path, parse_native_request_json, GatewayCore,
    GatewayCoreRequest, OnlineNode, RoutingMode,
};
use freedom_ipfs_namesys::NameResolver;
use freedom_ipfs_retrieval::FetchingBlockProvider;
use freedom_ipfs_routing::{RoutingStatsHandle, DEFAULT_DELEGATED_ROUTER};
use freedom_ipfs_store::SqliteBlockStore;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::{c_char, CStr, CString};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

const MOBILE_FFI_ABI_VERSION: u32 = 1;
const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const LOW_MEMORY_CACHE_BYTES: u64 = 32 * 1024 * 1024;
const CACHE_DB_FILE: &str = "freedom-ipfs.sqlite3";
const ROUTING_MODE_AUTO: u32 = 0;
const ROUTING_MODE_DELEGATED: u32 = 1;
const ROUTING_MODE_LIGHT_DHT: u32 = 2;
const ROUTING_MODE_OFFLINE: u32 = 3;
const PRELOAD_TIMEOUT: Duration = Duration::from_secs(60);
/// Upper bound on how long `freedom_ipfs_node_free` waits for blocking-pool
/// work (e.g. a stuck DNS lookup) after async tasks are aborted. Dropping the
/// runtime instead would wait for that work indefinitely.
const NODE_FREE_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Upper bound on how long `freedom_ipfs_node_free` waits for an SQLite
/// statement in progress before closing the cache database.
const NODE_FREE_STORE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const NATIVE_BODY_CHANNEL_CAPACITY: usize = 2;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleState {
    Foreground,
    Background,
}

/// Its size matters to CI: the add-on's jemalloc smoke (`smoke.js`, run
/// with a single arena and no thread cache in `electron-addon.yml`) needs a
/// freed node's address to be handed to the next node, which happens in
/// jemalloc's 768-byte class and not in the next one up. See
/// `the_node_stays_in_the_reuse_smokes_size_class`.
pub struct FreedomIpfsNode {
    runtime: Runtime,
    store: SqliteBlockStore,
    gateway_addr: Mutex<Option<SocketAddr>>,
    gateway_task: Mutex<Option<JoinHandle<()>>>,
    retrieval_stats_provider: Mutex<Option<FetchingBlockProvider>>,
    routing_stats: Mutex<Option<RoutingStatsHandle>>,
    lifecycle_state: Mutex<LifecycleState>,
    next_preload_id: AtomicU64,
    /// Boxed, to make room for `progress` within the size class.
    preload_tasks: Box<Mutex<HashMap<u64, JoinHandle<()>>>>,
    native_gateway_state: Mutex<NativeGatewayState>,
    native_gateway_events: Arc<NativeGatewayEventMux>,
    native_gateway_stats: Arc<NativeGatewayStats>,
    next_native_request_id: AtomicU64,
    /// The node's progress, which its snapshot reads: everything its
    /// runtime's threads emit, its gateways' requests, and its preloads'
    /// events from the host's thread.
    progress: ProgressRecorder,
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
    stats: Arc<NativeGatewayStats>,
    body_rx: Mutex<mpsc::Receiver<NativeBodyMessage>>,
    pending: Mutex<VecDeque<u8>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl NativeGatewayRequest {
    fn is_terminal(&self) -> bool {
        self.meta
            .lock()
            .map(|meta| meta.cancelled || meta.completed || meta.error.is_some())
            .unwrap_or(true)
    }

    fn cancel(&self) {
        let mut newly_cancelled = false;
        if let Ok(mut meta) = self.meta.lock() {
            if !meta.cancelled {
                meta.cancelled = true;
                newly_cancelled = true;
            }
        }
        if newly_cancelled {
            self.stats.total_cancelled.fetch_add(1, Ordering::Relaxed);
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

struct NativeGatewayTask {
    core: GatewayCore,
    request: GatewayCoreRequest,
    request_handle: u64,
    meta: Arc<Mutex<NativeGatewayRequestMeta>>,
    wake: Arc<NativeGatewayRequestWake>,
    events: Arc<NativeGatewayEventMux>,
    stats: Arc<NativeGatewayStats>,
    body_tx: mpsc::Sender<NativeBodyMessage>,
}

#[derive(Default)]
struct NativeGatewayStats {
    total_started: AtomicU64,
    total_completed: AtomicU64,
    total_failed: AtomicU64,
    total_cancelled: AtomicU64,
    total_freed: AtomicU64,
    total_gateway_busy_responses: AtomicU64,
    bytes_read: AtomicU64,
    max_active_handles: AtomicU64,
    last_error: Mutex<Option<NativeGatewayErrorJson>>,
}

#[derive(Serialize)]
struct NativeGatewayStatsSnapshot {
    active_native_handles: u64,
    total_started: u64,
    total_completed: u64,
    total_failed: u64,
    total_cancelled: u64,
    total_freed: u64,
    total_gateway_busy_responses: u64,
    bytes_read: u64,
    max_active_handles: u64,
    events_enqueued: u64,
    events_delivered: u64,
    events_coalesced: u64,
    max_event_queue_depth: u64,
    pending_event_queue_depth: u64,
    pending_event_handle_count: u64,
    stop_generation: u64,
    last_native_error_code: Option<String>,
    last_native_error_message: Option<String>,
}

#[derive(Default)]
struct NativeGatewayEventMuxSnapshot {
    events_enqueued: u64,
    events_delivered: u64,
    events_coalesced: u64,
    max_event_queue_depth: u64,
    pending_event_queue_depth: u64,
    pending_event_handle_count: u64,
    stop_generation: u64,
}

impl NativeGatewayStats {
    fn record_started(&self, active_handles: usize) {
        self.total_started.fetch_add(1, Ordering::Relaxed);
        update_atomic_max(&self.max_active_handles, active_handles as u64);
    }

    fn record_active_handles(&self, active_handles: usize) {
        update_atomic_max(&self.max_active_handles, active_handles as u64);
    }

    fn record_failed(&self, error: &NativeGatewayErrorJson) {
        self.total_failed.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(error.clone());
        }
    }

    fn record_gateway_busy_response(&self) {
        self.total_gateway_busy_responses
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.lock() {
            *last_error = Some(NativeGatewayErrorJson {
                code: "gateway_busy".to_string(),
                message: "gateway busy".to_string(),
            });
        }
    }

    fn snapshot(
        &self,
        active_native_handles: usize,
        events: NativeGatewayEventMuxSnapshot,
    ) -> NativeGatewayStatsSnapshot {
        let last_error = self.last_error.lock().ok().and_then(|error| error.clone());
        NativeGatewayStatsSnapshot {
            active_native_handles: active_native_handles as u64,
            total_started: self.total_started.load(Ordering::Relaxed),
            total_completed: self.total_completed.load(Ordering::Relaxed),
            total_failed: self.total_failed.load(Ordering::Relaxed),
            total_cancelled: self.total_cancelled.load(Ordering::Relaxed),
            total_freed: self.total_freed.load(Ordering::Relaxed),
            total_gateway_busy_responses: self.total_gateway_busy_responses.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            max_active_handles: self.max_active_handles.load(Ordering::Relaxed),
            events_enqueued: events.events_enqueued,
            events_delivered: events.events_delivered,
            events_coalesced: events.events_coalesced,
            max_event_queue_depth: events.max_event_queue_depth,
            pending_event_queue_depth: events.pending_event_queue_depth,
            pending_event_handle_count: events.pending_event_handle_count,
            stop_generation: events.stop_generation,
            last_native_error_code: last_error.as_ref().map(|error| error.code.clone()),
            last_native_error_message: last_error.map(|error| error.message),
        }
    }
}

fn update_atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
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
    events_enqueued: u64,
    events_delivered: u64,
    events_coalesced: u64,
    max_event_queue_depth: u64,
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
            state.events_enqueued = state.events_enqueued.saturating_add(1);
            state.max_event_queue_depth = state.max_event_queue_depth.max(state.queue.len() as u64);
        } else {
            state.events_coalesced = state.events_coalesced.saturating_add(1);
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
                    state.events_delivered = state.events_delivered.saturating_add(1);
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

    fn snapshot(&self) -> NativeGatewayEventMuxSnapshot {
        let Ok(state) = self.state.lock() else {
            return NativeGatewayEventMuxSnapshot::default();
        };
        NativeGatewayEventMuxSnapshot {
            events_enqueued: state.events_enqueued,
            events_delivered: state.events_delivered,
            events_coalesced: state.events_coalesced,
            max_event_queue_depth: state.max_event_queue_depth,
            pending_event_queue_depth: state.queue.len() as u64,
            pending_event_handle_count: state.pending.len() as u64,
            stop_generation: state.stop_generation,
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

/// The `tracing` layer that feeds `freedom_ipfs_node_progress_snapshot_json`:
/// [`progress::progress_layer`], whose rules apply (add it once, and only
/// per-layer filters leave it alone).
///
/// The node installs it as the process's global subscriber on its own
/// (see [`progress::init_progress_tracing`]), but that slot is process-wide
/// and the first claim wins: a host that links this crate next to another
/// library that installs a subscriber first (freedom-mobile-ffi, where
/// ant-ffi's log subscriber comes up before the IPFS node) must put this
/// layer on its own subscriber, alongside its other layers, before
/// either library initialises — otherwise the snapshot stays empty.
#[must_use]
pub fn progress_layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    progress::progress_layer()
}

#[no_mangle]
pub extern "C" fn freedom_ipfs_version() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION"))
        .expect("version has no nul")
        .into_raw()
}

#[no_mangle]
pub extern "C" fn freedom_ipfs_build_info_json() -> *mut c_char {
    let info = freedom_ipfs_core::build_info::build_info();
    let json = serde_json::json!({
        "name": info.name,
        "version": info.version,
        "release_tag": info.release_tag(),
        "mobile_ffi_abi_version": MOBILE_FFI_ABI_VERSION,
        "git_commit": info.git_commit,
        "git_describe": info.git_describe,
        "target": {
            "os": info.os,
            "arch": info.arch,
            "family": info.family
        },
        "features": {
            "loopback_gateway": true,
            "native_gateway": true,
            "native_event_mux": true,
            "native_gateway_stats": true,
            "progress_snapshot": true
        }
    });
    CString::new(json.to_string())
        .expect("build info JSON has no nul")
        .into_raw()
}

/// # Safety
///
/// `ptr` must be a pointer returned by a Freedom IPFS string-returning
/// function and must not be freed more than once.
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
    init_progress_tracing();
    // `Runtime::new()`'s runtime, its threads marked with the node's
    // recorder.
    let progress = ProgressRecorder::new();
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();
    let runtime = match progress.mark_runtime_threads(&mut runtime).build() {
        Ok(runtime) => runtime,
        Err(_) => return ptr::null_mut(),
    };
    let native_gateway = cache_only_gateway_core(store.clone()).with_progress(progress.clone());
    let native_gateway_events = Arc::new(NativeGatewayEventMux::default());
    let native_gateway_stats = Arc::new(NativeGatewayStats::default());
    Box::into_raw(Box::new(FreedomIpfsNode {
        runtime,
        store,
        gateway_addr: Mutex::new(None),
        gateway_task: Mutex::new(None),
        retrieval_stats_provider: Mutex::new(None),
        routing_stats: Mutex::new(None),
        lifecycle_state: Mutex::new(LifecycleState::Foreground),
        next_preload_id: AtomicU64::new(1),
        preload_tasks: Box::new(Mutex::new(HashMap::new())),
        native_gateway_state: Mutex::new(NativeGatewayState {
            core: native_gateway,
            requests: HashMap::new(),
        }),
        native_gateway_events,
        native_gateway_stats,
        next_native_request_id: AtomicU64::new(1),
        progress,
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
    init_progress_tracing();
    CString::new((*ptr).progress.snapshot_json())
        .unwrap_or_else(|_| CString::new("{\"active\":[],\"events\":[]}").unwrap())
        .into_raw()
}

/// # Safety
///
/// `ptr` must be a valid node pointer. The returned string is UTF-8 JSON and
/// must be released with `freedom_ipfs_string_free`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_native_gateway_stats_json(
    ptr: *mut FreedomIpfsNode,
) -> *mut c_char {
    if ptr.is_null() {
        return native_gateway_stats_json_string(
            NativeGatewayStats::default().snapshot(0, NativeGatewayEventMuxSnapshot::default()),
        );
    }
    let node = &*ptr;
    let active_native_handles = node
        .native_gateway_state
        .lock()
        .map(|state| state.requests.len())
        .unwrap_or_default();
    native_gateway_stats_json_string(
        node.native_gateway_stats
            .snapshot(active_native_handles, node.native_gateway_events.snapshot()),
    )
}

/// # Safety
///
/// `ptr` must be a valid node pointer.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_clear_progress(ptr: *mut FreedomIpfsNode) -> bool {
    if ptr.is_null() {
        return false;
    }
    (*ptr).progress.clear();
    true
}

/// # Safety
///
/// `ptr` must be a pointer returned by `freedom_ipfs_node_new_in_memory` and
/// must not be used after this function returns.
///
/// Normally returns once the node's cache database is closed, so the same data
/// dir can be reopened immediately. Blocking work that is still running after
/// the runtime shutdown timeout is left to finish on its own, but it can no
/// longer reach the database: the store is closed underneath it. Closing waits
/// at most `NODE_FREE_STORE_CLOSE_TIMEOUT` for a statement such work is still
/// running; if it does not finish in time this logs
/// `node_free_store_close_timeout` and returns with the database still open,
/// and a background thread closes it as soon as that statement releases the
/// connection (until then the leaked work can still run statements against
/// the file). That thread waits as long as the statement does: one that
/// never returns leaves a parked `freedom-ipfs-store-close` thread for the
/// life of the process, one per such free. Worst case this blocks for
/// about `NODE_FREE_RUNTIME_SHUTDOWN_TIMEOUT + NODE_FREE_STORE_CLOSE_TIMEOUT`.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_free(ptr: *mut FreedomIpfsNode) {
    if !ptr.is_null() {
        let node = &*ptr;
        stop_native_gateway_requests(node);
        stop_gateway(node);
        stop_preloads(node);
        let node = Box::from_raw(ptr);
        let FreedomIpfsNode { runtime, store, .. } = *node;
        runtime.shutdown_timeout(NODE_FREE_RUNTIME_SHUTDOWN_TIMEOUT);
        if !store.close_or_close_later(NODE_FREE_STORE_CLOSE_TIMEOUT) {
            tracing::warn!(phase = "node_free_store_close_timeout");
        }
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
    freedom_ipfs_node_start_gateway_online_with_config_v3(
        ptr,
        addr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        dht_query_timeout_secs,
        dht_max_providers,
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
/// defaults. `request_queue_timeout_ms` may be 0 to use the gateway default.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_gateway_online_with_config_v3(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
    request_queue_timeout_ms: u64,
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
        GatewayStartConfig {
            routing_mode,
            max_concurrent_requests,
            dht_query_timeout_secs,
            dht_max_providers,
            request_queue_timeout_ms,
        },
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
    freedom_ipfs_node_restart_gateway_online_with_config_v3(
        ptr,
        addr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        dht_query_timeout_secs,
        dht_max_providers,
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
/// defaults. `request_queue_timeout_ms` may be 0 to use the gateway default. On
/// success this cancels active preloads, stops the current gateway, and starts a
/// new online gateway with the supplied routing configuration. On validation
/// failure the currently running gateway is left untouched.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_restart_gateway_online_with_config_v3(
    ptr: *mut FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
    request_queue_timeout_ms: u64,
) -> bool {
    if ptr.is_null() || addr.is_null() {
        return false;
    }
    let node = &*ptr;
    let Some(parts) = gateway_router_for_routing_mode(
        node,
        addr,
        delegated_router,
        GatewayStartConfig {
            routing_mode,
            max_concurrent_requests,
            dht_query_timeout_secs,
            dht_max_providers,
            request_queue_timeout_ms,
        },
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

/// # Safety
///
/// `ptr` must be a valid node pointer. `delegated_router` may be null to use
/// the default delegated routing endpoint, otherwise it must point to a
/// NUL-terminated UTF-8 URL string or comma-separated URL list.
/// `routing_mode` must be one of the `FREEDOM_IPFS_ROUTING_MODE_*` constants
/// from the C header. This configures the native request/event API for online
/// retrieval without binding the loopback HTTP gateway.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_native_gateway_online_with_config_v2(
    ptr: *mut FreedomIpfsNode,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
) -> bool {
    freedom_ipfs_node_start_native_gateway_online_with_config_v3(
        ptr,
        delegated_router,
        routing_mode,
        max_concurrent_requests,
        dht_query_timeout_secs,
        dht_max_providers,
        0,
    )
}

/// # Safety
///
/// `ptr` must be a valid node pointer. `delegated_router` may be null to use
/// the default delegated routing endpoint, otherwise it must point to a
/// NUL-terminated UTF-8 URL string or comma-separated URL list.
/// `routing_mode` must be one of the `FREEDOM_IPFS_ROUTING_MODE_*` constants
/// from the C header. `request_queue_timeout_ms` may be 0 to use the gateway
/// default. This configures the native request/event API for online retrieval
/// without binding the loopback HTTP gateway.
#[no_mangle]
pub unsafe extern "C" fn freedom_ipfs_node_start_native_gateway_online_with_config_v3(
    ptr: *mut FreedomIpfsNode,
    delegated_router: *const c_char,
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
    request_queue_timeout_ms: u64,
) -> bool {
    if ptr.is_null() {
        return false;
    }
    let node = &*ptr;
    let addr = CString::new("127.0.0.1:0").expect("static addr has no nul");
    let Some(parts) = gateway_router_for_routing_mode(
        node,
        addr.as_ptr(),
        delegated_router,
        GatewayStartConfig {
            routing_mode,
            max_concurrent_requests,
            dht_query_timeout_secs,
            dht_max_providers,
            request_queue_timeout_ms,
        },
    ) else {
        return false;
    };

    stop_preloads(node);
    stop_gateway(node);
    set_native_gateway_core(node, parts.native_gateway);
    set_online_stats(node, parts.retrieval_provider, parts.routing_stats);
    true
}

struct OnlineGatewayParts {
    addr: SocketAddr,
    router: axum::Router,
    native_gateway: GatewayCore,
    retrieval_provider: FetchingBlockProvider,
    routing_stats: RoutingStatsHandle,
}

#[derive(Clone, Copy)]
struct GatewayStartConfig {
    routing_mode: u32,
    max_concurrent_requests: usize,
    dht_query_timeout_secs: u64,
    dht_max_providers: usize,
    request_queue_timeout_ms: u64,
}

unsafe fn gateway_router_for_routing_mode(
    node: &FreedomIpfsNode,
    addr: *const c_char,
    delegated_router: *const c_char,
    start_config: GatewayStartConfig,
) -> Option<OnlineGatewayParts> {
    let addr = parse_loopback_gateway_addr(addr)?;
    let mut gateway_config = if start_config.max_concurrent_requests == 0 {
        freedom_ipfs_gateway::GatewayConfig::default()
    } else {
        freedom_ipfs_gateway::GatewayConfig::new(start_config.max_concurrent_requests)
    };
    if start_config.request_queue_timeout_ms > 0 {
        gateway_config = gateway_config.with_request_queue_timeout(Duration::from_millis(
            start_config.request_queue_timeout_ms,
        ));
    }
    let routing_mode = match start_config.routing_mode {
        ROUTING_MODE_AUTO => RoutingMode::Auto,
        ROUTING_MODE_DELEGATED => RoutingMode::Delegated,
        ROUTING_MODE_LIGHT_DHT => RoutingMode::LightDht,
        ROUTING_MODE_OFFLINE => RoutingMode::Offline,
        _ => return None,
    };
    let mut builder = OnlineNode::builder(node.store.clone())
        .routing_mode(routing_mode)
        .gateway_config(gateway_config)
        .progress(node.progress.clone());
    if routing_mode != RoutingMode::Offline {
        let delegated_routers = if delegated_router.is_null() {
            DEFAULT_DELEGATED_ROUTER.to_string()
        } else {
            CStr::from_ptr(delegated_router).to_str().ok()?.to_string()
        };
        builder = builder.delegated_routers(delegated_router_endpoints(&delegated_routers));
        // Zero keeps the light DHT's own default.
        if start_config.dht_query_timeout_secs != 0 {
            builder =
                builder.dht_query_timeout(Duration::from_secs(start_config.dht_query_timeout_secs));
        }
        if start_config.dht_max_providers != 0 {
            builder = builder.dht_max_providers(start_config.dht_max_providers);
        }
    }
    let online = builder.build();
    Some(OnlineGatewayParts {
        addr,
        router: online.router(),
        native_gateway: online.gateway_core(),
        retrieval_provider: online.provider().clone(),
        routing_stats: online.routing_stats().clone(),
    })
}

unsafe fn parse_loopback_gateway_addr(addr: *const c_char) -> Option<SocketAddr> {
    let addr = CStr::from_ptr(addr)
        .to_str()
        .ok()
        .and_then(|s| s.parse::<SocketAddr>().ok())?;
    addr.ip().is_loopback().then_some(addr)
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
    let parsed = match parse_native_request_json(request_json) {
        Ok(parsed) => parsed,
        Err(_) => return 0,
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
    let stats = node.native_gateway_stats.clone();
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
    let task_stats = stats.clone();
    let (registered_tx, registered_rx) = oneshot::channel();
    let task = node.runtime.spawn(async move {
        if registered_rx.await.is_err() {
            return;
        }
        run_native_gateway_request(NativeGatewayTask {
            core,
            request: parsed.request,
            request_handle: handle,
            meta: task_meta,
            wake: task_wake,
            events: task_events,
            stats: task_stats,
            body_tx,
        })
        .await;
    });
    let request = Arc::new(NativeGatewayRequest {
        handle,
        meta,
        wake,
        events,
        stats: stats.clone(),
        body_rx: Mutex::new(body_rx),
        pending: Mutex::new(VecDeque::new()),
        task: Mutex::new(Some(task)),
    });
    native_state.requests.insert(handle, request);
    let active_handles = native_state.requests.len();
    drop(native_state);
    stats.record_started(active_handles);
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
    let active_handles = native_state.requests.len();
    node.native_gateway_stats
        .total_freed
        .fetch_add(1, Ordering::Relaxed);
    node.native_gateway_stats
        .record_active_handles(active_handles);
    if !request.is_terminal() {
        request.cancel();
    }
    node.native_gateway_events
        .push(request_handle, FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);
    true
}

async fn run_native_gateway_request(task: NativeGatewayTask) {
    let NativeGatewayTask {
        core,
        request,
        request_handle,
        meta,
        wake,
        events,
        stats,
        body_tx,
    } = task;
    let response = core.handle(request).await;
    let status = response.status().as_u16();
    let headers = native_gateway_headers(response.headers());
    if native_gateway_is_busy_response(status, &headers) {
        stats.record_gateway_busy_response();
    }
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
                stats.record_failed(&error);
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
            stats.total_completed.fetch_add(1, Ordering::Relaxed);
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

fn native_gateway_is_busy_response(status: u16, headers: &[NativeGatewayHeader]) -> bool {
    status == 503
        && headers.iter().any(|header| {
            header
                .name
                .eq_ignore_ascii_case("x-freedom-ipfs-error-code")
                && header.value == "gateway_busy"
        })
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
            request
                .stats
                .bytes_read
                .fetch_add(bytes_read as u64, Ordering::Relaxed);
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
            request
                .stats
                .bytes_read
                .fetch_add(bytes_read as u64, Ordering::Relaxed);
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

fn native_gateway_stats_json_string(snapshot: NativeGatewayStatsSnapshot) -> *mut c_char {
    let json = serde_json::to_string(&snapshot).unwrap_or_else(|_| {
        "{\"active_native_handles\":0,\"total_started\":0,\"total_completed\":0,\"total_failed\":0,\"total_cancelled\":0,\"total_freed\":0,\"total_gateway_busy_responses\":0,\"bytes_read\":0,\"max_active_handles\":0,\"events_enqueued\":0,\"events_delivered\":0,\"events_coalesced\":0,\"max_event_queue_depth\":0,\"pending_event_queue_depth\":0,\"pending_event_handle_count\":0,\"stop_generation\":0}".to_string()
    });
    CString::new(json)
        .unwrap_or_else(|_| CString::new("{\"active_native_handles\":0}").unwrap())
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
    {
        // On the host's thread: in the node's scope. The preload's own task
        // runs on the node's runtime.
        let _scope = node.progress.enter();
        tracing::info!(phase = "preload_start", preload_id = id, path = %path);
    }
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
    let _scope = node.progress.enter();
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
    node.native_gateway_stats.record_active_handles(0);
    for request in requests {
        request.cancel();
    }
}

fn stop_preloads(node: &FreedomIpfsNode) {
    // Called on the host's thread.
    let _scope = node.progress.enter();
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
    let core = core.with_progress(node.progress.clone());
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
    node.native_gateway_stats.record_active_handles(0);
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
    normalize_content_path(path)
}

#[cfg(test)]
mod tests {
    /// A node of more than 768 bytes moves to jemalloc's 896-byte class,
    /// where the next node doesn't get a freed node's address, so the
    /// add-on's address-reuse smoke never takes its path and fails; one of
    /// 640 or less moves down a class, with the same risk. Keep it in.
    #[test]
    fn the_node_stays_in_the_reuse_smokes_size_class() {
        let size = std::mem::size_of::<super::FreedomIpfsNode>();
        assert!(
            (641..=768).contains(&size),
            "FreedomIpfsNode is {size} bytes, outside jemalloc's 768-byte class: \
             move a field behind an Arc or Box (or re-check the jemalloc smoke)"
        );
    }

    use super::*;
    use cid::Cid;
    use freedom_ipfs_core::{cid_from_data, parse_car_v1, Block, CODEC_RAW};
    use freedom_ipfs_store::CachedProviderRecord;
    use serde_json::json;
    use std::io::{Read, Write};

    struct SlowTestProvider {
        cid: Cid,
        data: Vec<u8>,
        entered: Arc<std::sync::atomic::AtomicBool>,
        delay: Duration,
    }

    impl BlockProvider for SlowTestProvider {
        fn get_block(&self, cid: &Cid) -> freedom_ipfs_core::Result<Option<Block>> {
            if *cid != self.cid {
                return Ok(None);
            }
            self.entered.store(true, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            Ok(Some(Block::new(self.cid, self.data.clone())?))
        }
    }

    #[test]
    fn version_and_build_info_match_release_contract() {
        unsafe {
            let version_ptr = freedom_ipfs_version();
            assert!(!version_ptr.is_null());
            let version = CStr::from_ptr(version_ptr).to_string_lossy().into_owned();
            freedom_ipfs_string_free(version_ptr);

            assert_eq!(version, env!("CARGO_PKG_VERSION"));

            let build_info_ptr = freedom_ipfs_build_info_json();
            assert!(!build_info_ptr.is_null());
            let build_info_json = CStr::from_ptr(build_info_ptr)
                .to_string_lossy()
                .into_owned();
            freedom_ipfs_string_free(build_info_ptr);

            let build_info: serde_json::Value =
                serde_json::from_str(&build_info_json).expect("build info should be JSON");
            assert_eq!(build_info["name"], "freedom-ipfs");
            assert_eq!(build_info["version"], version);
            assert_eq!(build_info["release_tag"], format!("v{version}"));
            assert_eq!(
                build_info["mobile_ffi_abi_version"],
                u64::from(MOBILE_FFI_ABI_VERSION)
            );
            assert_eq!(build_info["target"]["os"], std::env::consts::OS);
            assert_eq!(build_info["target"]["arch"], std::env::consts::ARCH);
            assert_eq!(build_info["features"]["native_gateway"], true);
            assert_eq!(build_info["features"]["native_event_mux"], true);
        }
    }

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
    fn starts_native_online_gateway_v3_from_data_dir_with_delegated_routing() {
        unsafe {
            let tempdir = tempfile::tempdir().unwrap();
            let data_dir = CString::new(tempdir.path().to_str().unwrap()).unwrap();
            let node = freedom_ipfs_node_new_with_data_dir(data_dir.as_ptr(), 0);
            assert!(!node.is_null());

            let router = CString::new("http://127.0.0.1:9/routing/v1").unwrap();
            assert!(
                freedom_ipfs_node_start_native_gateway_online_with_config_v3(
                    node,
                    router.as_ptr(),
                    ROUTING_MODE_DELEGATED,
                    1,
                    5,
                    2,
                    50,
                )
            );

            assert!(freedom_ipfs_node_gateway_url(node).is_null());
            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn starts_native_online_gateway_without_binding_http_gateway() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"native online gateway without loopback";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();

            assert!(
                freedom_ipfs_node_start_native_gateway_online_with_config_v2(
                    node,
                    ptr::null(),
                    ROUTING_MODE_OFFLINE,
                    1,
                    0,
                    0,
                )
            );
            assert!(freedom_ipfs_node_gateway_url(node).is_null());

            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": format!("/ipfs/{cid}")
                }),
            );
            let metadata = wait_native_gateway_response(node, handle);
            assert_eq!(metadata["status"], 200);
            assert_eq!(read_native_gateway_body(node, handle, 8), data);
            assert!(freedom_ipfs_gateway_request_free(node, handle));

            let stats = native_gateway_stats_json(node);
            assert_eq!(stats["active_native_handles"], 0);
            assert_eq!(stats["total_started"], 1);
            assert_eq!(stats["total_completed"], 1);
            assert_eq!(stats["total_freed"], 1);

            freedom_ipfs_node_free(node);
        }
    }

    #[test]
    fn starts_native_online_gateway_v3_from_data_dir_without_binding_http_gateway() {
        unsafe {
            let tempdir = tempfile::tempdir().unwrap();
            let data_dir = CString::new(tempdir.path().to_str().unwrap()).unwrap();
            let node = freedom_ipfs_node_new_with_data_dir(data_dir.as_ptr(), 1024 * 1024);
            assert!(!node.is_null());

            let data = b"native online gateway v3 data dir";
            let cid = cid_from_data(CODEC_RAW, data);
            (*node).store.put_block(&cid, data).unwrap();

            assert!(
                freedom_ipfs_node_start_native_gateway_online_with_config_v3(
                    node,
                    ptr::null(),
                    ROUTING_MODE_OFFLINE,
                    1,
                    0,
                    0,
                    50,
                )
            );
            assert!(freedom_ipfs_node_gateway_url(node).is_null());

            let handle = start_native_gateway_request(
                node,
                json!({
                    "method": "GET",
                    "path": format!("/ipfs/{cid}")
                }),
            );
            let metadata = wait_native_gateway_response(node, handle);
            assert_eq!(metadata["status"], 200);
            assert_eq!(read_native_gateway_body(node, handle, 8), data);
            assert!(freedom_ipfs_gateway_request_free(node, handle));

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
                    && event["kind"] == "gateway_request"
                    && event["target_id"] == 4242
                    && event["parent_id"] == 7
                    && event["top_level_path"] == top_level_path),
                "{snapshot}"
            );
            assert!(
                events.iter().any(|event| event["path"] == path
                    && event["target_id"] == 4242
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

            // Both are emitted on this thread, into the node's recorder.
            let snapshot: serde_json::Value =
                serde_json::from_str(&progress_snapshot_json(node)).unwrap();
            for raw_phase in ["preload_start", "preload_cancelled"] {
                assert!(
                    snapshot["events"].as_array().unwrap().iter().any(|event| {
                        event["kind"] == "preload"
                            && event["target_id"] == task_id
                            && event["raw_phase"] == raw_phase
                    }),
                    "{raw_phase}: {snapshot}"
                );
            }
            assert!(
                !snapshot["active"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|target| target["kind"] == "preload"),
                "{snapshot}"
            );

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
            {
                // As `freedom_ipfs_node_preload_path` emits it.
                let _scope = (*node).progress.enter();
                tracing::info!(
                    phase = "preload_start",
                    preload_id,
                    path = "/ipfs/bafyprogress"
                );
            }
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
    fn node_free_does_not_wait_for_stuck_blocking_request() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"stuck blocking fixture".to_vec();
            let cid = cid_from_data(CODEC_RAW, &data);
            let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let provider: Arc<dyn BlockProvider> = Arc::new(SlowTestProvider {
                cid,
                data,
                entered: entered.clone(),
                delay: Duration::from_secs(30),
            });
            set_native_gateway_core(&*node, GatewayCore::with_provider(provider));

            let _handle = start_native_gateway_request(
                node,
                json!({ "method": "GET", "path": format!("/ipfs/{cid}") }),
            );
            for _ in 0..500 {
                if entered.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(entered.load(Ordering::SeqCst));

            let started = Instant::now();
            freedom_ipfs_node_free(node);
            let elapsed = started.elapsed();
            assert!(
                elapsed < NODE_FREE_RUNTIME_SHUTDOWN_TIMEOUT + Duration::from_secs(2),
                "node free waited {elapsed:?} for a stuck blocking request"
            );
        }
    }

    #[test]
    fn node_free_closes_the_store_even_when_a_clone_outlives_the_runtime() {
        unsafe {
            let tempdir = tempfile::tempdir().unwrap();
            let db_path = tempdir.path().join(CACHE_DB_FILE);
            let store = SqliteBlockStore::open(&db_path, 0).unwrap();
            let data = b"store survives free fixture".to_vec();
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            // Stands in for blocking-pool work still holding a store clone
            // after the runtime shutdown timeout.
            let leaked = store.clone();
            let node = node_from_store(store);
            assert!(!node.is_null());

            freedom_ipfs_node_free(node);

            assert!(leaked.block_count().is_err());
            #[cfg(target_os = "linux")]
            {
                let db_path = fs::canonicalize(&db_path).unwrap();
                let open_fds = fs::read_dir("/proc/self/fd")
                    .unwrap()
                    .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
                    .filter(|target| *target == db_path)
                    .count();
                assert_eq!(open_fds, 0, "cache db still open after node free");
            }
            let reopened = SqliteBlockStore::open(&db_path, 0).unwrap();
            assert_eq!(reopened.get(&cid).unwrap().unwrap().data(), data.as_slice());
        }
    }

    #[test]
    fn native_gateway_busy_response_is_machine_readable_and_counted() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let data = b"native busy fixture".to_vec();
            let cid = cid_from_data(CODEC_RAW, &data);
            let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let provider: Arc<dyn BlockProvider> = Arc::new(SlowTestProvider {
                cid,
                data,
                entered: entered.clone(),
                delay: Duration::from_millis(150),
            });
            let core = GatewayCore::with_provider_config(
                provider,
                freedom_ipfs_gateway::GatewayConfig::new(1)
                    .with_request_queue_timeout(Duration::from_millis(25)),
            );
            set_native_gateway_core(&*node, core);

            let request = json!({
                "method": "GET",
                "path": format!("/ipfs/{cid}")
            });
            let first = start_native_gateway_request(node, request.clone());
            for _ in 0..50 {
                if entered.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            assert!(entered.load(Ordering::SeqCst));

            let second = start_native_gateway_request(node, request);
            let second_metadata = native_gateway_response_json_wait_ffi(node, second, 5_000);
            assert_eq!(second_metadata["status"], 503);
            let headers = second_metadata["headers"].as_array().unwrap();
            assert!(headers.iter().any(|header| {
                header["name"]
                    .as_str()
                    .map(|name| name.eq_ignore_ascii_case("x-freedom-ipfs-error-code"))
                    .unwrap_or(false)
                    && header["value"] == "gateway_busy"
            }));
            assert!(headers.iter().any(|header| {
                header["name"]
                    .as_str()
                    .map(|name| name.eq_ignore_ascii_case("retry-after"))
                    .unwrap_or(false)
                    && header["value"] == "1"
            }));

            let second_body = read_native_gateway_body_wait(node, second, 64);
            assert!(
                String::from_utf8_lossy(&second_body).contains("gateway busy"),
                "busy body should remain HTTP-compatible"
            );
            assert!(freedom_ipfs_gateway_request_free(node, second));

            let first_metadata = wait_native_gateway_response(node, first);
            assert_eq!(first_metadata["status"], 200);
            let first_body = read_native_gateway_body_wait(node, first, 64);
            assert_eq!(first_body, b"native busy fixture");
            assert!(freedom_ipfs_gateway_request_free(node, first));

            let stats = native_gateway_stats_json(node);
            assert_eq!(stats["total_gateway_busy_responses"], 1);
            assert_eq!(stats["last_native_error_code"], "gateway_busy");

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
    fn native_gateway_stats_json_reports_transport_counters() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());

            let (noisy_handle, _noisy_tx) = insert_pending_native_gateway_request(node, true);
            let (small_handle, _small_tx) = insert_pending_native_gateway_request(node, true);
            for _ in 0..5 {
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

            let stats = native_gateway_stats_json(node);
            assert_eq!(stats["active_native_handles"], 2);
            assert_eq!(stats["total_started"], 2);
            assert_eq!(stats["events_enqueued"], 2);
            assert!(stats["events_coalesced"].as_u64().unwrap() >= 4);
            assert_eq!(stats["max_event_queue_depth"], 2);
            assert_eq!(stats["pending_event_queue_depth"], 2);
            assert_eq!(stats["pending_event_handle_count"], 2);

            let first = native_gateway_next_event(node, 1_000);
            assert_eq!(first.request_handle, noisy_handle);
            let second = native_gateway_next_event(node, 1_000);
            assert_eq!(second.request_handle, small_handle);
            let stats = native_gateway_stats_json(node);
            assert_eq!(stats["events_delivered"], 2);
            assert_eq!(stats["pending_event_queue_depth"], 0);
            assert_eq!(stats["pending_event_handle_count"], 0);

            assert!(freedom_ipfs_gateway_request_free(node, noisy_handle));
            assert!(freedom_ipfs_gateway_request_free(node, small_handle));
            let stats = native_gateway_stats_json(node);
            assert_eq!(stats["active_native_handles"], 0);
            assert_eq!(stats["total_freed"], 2);
            assert_eq!(stats["total_cancelled"], 2);
            assert_eq!(stats["max_active_handles"], 2);

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

    /// A fixed sequence of phase events touching every snapshot field.
    fn emit_golden_progress_sequence() {
        let root = tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            process_id = 1u32,
            request_id = 11u64,
            progress_request_id = 500u64,
            parent_request_id = 0u64,
            top_level_path = "",
            namespace = "ipfs",
            path = "/ipfs/bafygolden",
            range = ""
        );
        {
            let _entered = root.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 11u64, path = "/ipfs/bafygolden");
            tracing::info!(target: "freedom_ipfs_gateway", phase = "gateway_limiter", acquired = true, request_id = 11u64);
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "block_store_get", cache_hit = false, cid = "bafyblock");
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "provider_lookup", provider_count = 3u64, cid = "bafyblock");
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "bitswap_fetch", bitswap_delivery = "incoming", source_transport = "tcp", peer_count = 4u64);
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "block_fetch_total", source = "bitswap", bytes = 1000u64, elapsed_ms = 12u64);
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "bitswap_request_timeout", error = "timeout", retry_provider_count = 2u64);
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "block_fetch_total", source = "http_provider", transport = "https", bytes = 2000u64, file_len = 3000u64);
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_done", request_id = 11u64, status = 200u16, body_mode = "stream", elapsed_ms = 40u64);
            tracing::info!(target: "freedom_ipfs_gateway", phase = "gateway_stream_done", cid = "bafygolden", body_len = 3000u64, chunks = 1u64, elapsed_ms = 41u64);
        }
        let page = tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            request_id = 15u64,
            progress_request_id = 600u64,
            parent_request_id = 0u64,
            top_level_path = "",
            namespace = "ipfs",
            path = "/ipfs/bafypage"
        );
        {
            let _entered = page.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 15u64, path = "/ipfs/bafypage");
            tracing::info!(target: "freedom_ipfs_gateway", phase = "unixfs_resource", resource = "file", file_len = 9000u64);
        }
        let child = tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            request_id = 12u64,
            progress_request_id = 501u64,
            parent_request_id = 600u64,
            top_level_path = "/ipfs/bafypage",
            namespace = "ipfs",
            path = "/ipfs/bafygolden/style.css"
        );
        {
            let _entered = child.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 12u64, path = "/ipfs/bafygolden/style.css");
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "delegated_provider_lookup", candidate_peer_count = 5u64);
            tracing::info!(target: "freedom_ipfs_retrieval", phase = "bitswap_incoming_stream_read", ok = false, error = "reset");
        }
        let busy = tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            request_id = 13u64,
            progress_request_id = 502u64,
            parent_request_id = 500u64,
            top_level_path = "/ipfs/bafygolden",
            namespace = "ipfs",
            path = "/ipfs/bafygolden/app.js"
        );
        {
            let _entered = busy.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 13u64, path = "/ipfs/bafygolden/app.js");
            tracing::info!(target: "freedom_ipfs_gateway", phase = "gateway_limiter", acquired = false, request_id = 13u64);
        }
        let missing = tracing::info_span!(
            target: "freedom_ipfs_gateway",
            "gateway_request",
            request_id = 14u64,
            progress_request_id = 0u64,
            parent_request_id = 0u64,
            top_level_path = "",
            namespace = "ipns",
            path = "/ipns/example.org"
        );
        {
            let _entered = missing.enter();
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_start", request_id = 14u64, path = "/ipns/example.org");
            tracing::info!(target: "freedom_ipfs_gateway", phase = "name_cache", cache_hit = false, name = "example.org");
            tracing::info!(target: "freedom_ipfs_gateway", phase = "name_resolve", ok = true, offset = -3i64);
            tracing::info!(target: "freedom_ipfs_gateway", phase = "request_done", request_id = 14u64, status = 404u16, body_mode = "direct", elapsed_ms = 7u64);
        }
        tracing::info!(target: "freedom_ipfs_mobile", phase = "preload_start", preload_id = 9u64, path = "/ipfs/bafypreload");
        tracing::info!(target: "freedom_ipfs_mobile", phase = "preload_start", preload_id = 10u64, path = "/ipfs/bafystill");
        tracing::info!(target: "freedom_ipfs_mobile", phase = "preload_done", preload_id = 9u64, path = "/ipfs/bafypreload", ok = false, error = "request_failed");
        tracing::info!(target: "freedom_ipfs_routing", phase = "light_dht_provider_lookup", task_id = 77u64, cid = "bafyblock");
        tracing::info!(target: "freedom_ipfs_retrieval", phase = "unknown_phase_kept_raw", cid = "bafyblock", debug_field = ?Some(3u8));
        tracing::info!(target: "freedom_ipfs_retrieval", no_phase = true);
        tracing::info!(target: "ant_retrieval", phase = "request_start", request_id = 1u64);
    }

    /// The snapshot with its clock readings zeroed, for a golden comparison.
    fn zero_progress_timestamps(snapshot: &str) -> String {
        let mut out = String::with_capacity(snapshot.len());
        let mut rest = snapshot;
        loop {
            let next = [
                "\"generated_at_unix_ms\":",
                "\"timestamp_ms\":",
                "\"updated_ms\":",
            ]
            .iter()
            .filter_map(|key| rest.find(key).map(|at| (at, key.len())))
            .min();
            let Some((at, len)) = next else {
                out.push_str(rest);
                return out;
            };
            out.push_str(&rest[..at + len]);
            out.push('0');
            rest = rest[at + len..].trim_start_matches(|c: char| c.is_ascii_digit());
        }
    }

    /// The snapshot from before nodes had recorders of their own, for the
    /// events `emit_golden_progress_sequence` emits, clocks zeroed: recorded
    /// on 90c3c6c.
    const GOLDEN_PROGRESS_SNAPSHOT: &str =
        include_str!("../../../tests/fixtures/progress-snapshot-golden.json");

    /// A node's recorder keeps the snapshot's shape and semantics: the same
    /// events give the JSON the process-wide recorder gave, byte for byte
    /// but for the clocks.
    #[test]
    fn progress_snapshot_matches_the_golden_from_before_per_node_recorders() {
        unsafe {
            let node = freedom_ipfs_node_new_in_memory();
            assert!(!node.is_null());
            {
                let _scope = (*node).progress.enter();
                emit_golden_progress_sequence();
            }
            let snapshot = zero_progress_timestamps(&progress_snapshot_json(node));
            assert_eq!(snapshot, GOLDEN_PROGRESS_SNAPSHOT.trim_end());
            freedom_ipfs_node_free(node);
        }
    }

    /// Two nodes in one process, serving at once, each see only their own
    /// requests.
    #[test]
    fn two_nodes_progress_snapshots_never_mix() {
        unsafe {
            let nodes = [
                (freedom_ipfs_node_new_in_memory(), 81_000u64),
                (freedom_ipfs_node_new_in_memory(), 82_000u64),
            ];
            let mut requests = Vec::new();
            for &(node, base) in &nodes {
                assert!(!node.is_null());
                let data = format!("the block of node {base}").into_bytes();
                let cid = cid_from_data(CODEC_RAW, &data);
                (*node).store.put_block(&cid, &data).unwrap();
                for id in base..base + 10 {
                    let handle = start_native_gateway_request(
                        node,
                        json!({
                            "method": "GET",
                            "path": format!("/ipfs/{cid}"),
                            "headers": [
                                { "name": "X-Freedom-Request-ID", "value": id.to_string() }
                            ]
                        }),
                    );
                    requests.push((node, handle, data.len()));
                }
            }
            for (node, handle, len) in requests {
                assert_eq!(wait_native_gateway_response(node, handle)["status"], 200);
                assert_eq!(read_native_gateway_body_wait(node, handle, 64).len(), len);
                assert!(freedom_ipfs_gateway_request_free(node, handle));
            }
            for &(node, base) in &nodes {
                let snapshot: serde_json::Value =
                    serde_json::from_str(&progress_snapshot_json(node)).unwrap();
                let events = snapshot["events"].as_array().unwrap();
                let completed: std::collections::BTreeSet<u64> = events
                    .iter()
                    .filter(|event| event["raw_phase"] == "request_done")
                    .map(|event| event["target_id"].as_u64().unwrap())
                    .collect();
                assert_eq!(completed, (base..base + 10).collect(), "{snapshot}");
                assert!(
                    events.iter().all(|event| event["kind"] != "gateway_request"
                        || (base..base + 10).contains(&event["target_id"].as_u64().unwrap())),
                    "{snapshot}"
                );
                freedom_ipfs_node_free(node);
            }
        }
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
            stats: (*node).native_gateway_stats.clone(),
            body_rx: Mutex::new(body_rx),
            pending: Mutex::new(VecDeque::new()),
            task: Mutex::new(None),
        });
        let active_handles = {
            let mut native_state = (*node).native_gateway_state.lock().unwrap();
            native_state.requests.insert(handle, request);
            native_state.requests.len()
        };
        (*node).native_gateway_stats.record_started(active_handles);
        (handle, body_tx)
    }

    unsafe fn native_gateway_stats_json(node: *mut FreedomIpfsNode) -> serde_json::Value {
        let stats_ptr = freedom_ipfs_node_native_gateway_stats_json(node);
        let stats = CStr::from_ptr(stats_ptr).to_string_lossy().into_owned();
        freedom_ipfs_string_free(stats_ptr);
        serde_json::from_str(&stats).unwrap()
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
}
