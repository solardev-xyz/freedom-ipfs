use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH,
    RANGE,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use cid::Cid;
use freedom_ipfs_core::{parse_cid, BlockProvider};
use freedom_ipfs_namesys::{NameResolver, NamesysError, ResolvedName};
use freedom_ipfs_retrieval::{with_retrieval_request_context, RetrievalRequestContext};
use freedom_ipfs_store::SqliteBlockStore;
use freedom_ipfs_unixfs::{
    DirectoryEntry, NodeKind, UnixfsError, UnixfsMetadataCacheStats, UnixfsResolver,
    DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY,
};
use futures::stream;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

pub const DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS: usize = 8;
const GATEWAY_REQUEST_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const GATEWAY_STREAM_CHUNK_SIZE: u64 = 64 * 1024;
const GATEWAY_SMALL_BODY_CACHE_MAX_ENTRY_BYTES: usize = GATEWAY_STREAM_CHUNK_SIZE as usize;
pub const DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_GATEWAY_HTML_PREFETCH_MAX_ASSETS: usize = 0;
const DEFAULT_GATEWAY_HTML_PREFETCH_MAX_BYTES: u64 = GATEWAY_STREAM_CHUNK_SIZE;
const DEFAULT_GATEWAY_HTML_PREFETCH_CONCURRENCY: usize = 2;
const GATEWAY_HTML_PREFETCH_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_DIRS: usize = 0;
const DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_BYTES: u64 = GATEWAY_STREAM_CHUNK_SIZE;
const DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_CONCURRENCY: usize = 1;
const GATEWAY_HTML_DIRECTORY_PREFETCH_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_GATEWAY_HTML_RANGE_WARM_MAX_BYTES: u64 = 0;
const DEFAULT_GATEWAY_HTML_RANGE_WARM_CONCURRENCY: usize = 1;
const GATEWAY_HTML_RANGE_WARM_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);
const X_FREEDOM_REQUEST_ID: &str = "x-freedom-request-id";
const X_FREEDOM_PARENT_REQUEST_ID: &str = "x-freedom-parent-request-id";
const X_FREEDOM_TOP_LEVEL_PATH: &str = "x-freedom-top-level-path";
const CACHE_CONTROL_IPFS_FILE: &str = "public, max-age=31536000, immutable";
const CACHE_CONTROL_IPNS_FILE: &str = "no-cache";
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
const DEFAULT_PERSISTENT_NAME_CACHE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Copy)]
enum GatewayBodyMode {
    Direct,
    Stream,
}

impl GatewayBodyMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Stream => "stream",
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SmallBodyCacheKey {
    file_cid: Cid,
    len: u64,
}

#[derive(Debug)]
struct SmallBodyCache {
    max_bytes: usize,
    inner: Mutex<SmallBodyCacheInner>,
}

#[derive(Debug, Default)]
struct SmallBodyCacheInner {
    entries: HashMap<SmallBodyCacheKey, Bytes>,
    order: VecDeque<SmallBodyCacheKey>,
    bytes: usize,
}

impl SmallBodyCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            inner: Mutex::new(SmallBodyCacheInner::default()),
        }
    }

    fn get(&self, key: &SmallBodyCacheKey) -> Option<Bytes> {
        if self.max_bytes == 0 {
            return None;
        }
        let mut inner = self.lock_inner();
        let body = inner.entries.get(key).cloned();
        if body.is_some() {
            touch_small_body_cache_order(&mut inner, key);
        }
        body
    }

    fn insert(&self, key: SmallBodyCacheKey, body: Bytes) -> SmallBodyCacheInsert {
        let body_len = body.len();
        if self.max_bytes == 0
            || body_len > self.max_bytes
            || body_len > GATEWAY_SMALL_BODY_CACHE_MAX_ENTRY_BYTES
        {
            return SmallBodyCacheInsert {
                inserted: false,
                evicted: 0,
                cache_len: self.len(),
                cache_bytes: self.bytes(),
            };
        }

        let mut inner = self.lock_inner();
        if let Some(previous) = inner.entries.insert(key.clone(), body) {
            inner.bytes = inner.bytes.saturating_sub(previous.len());
        }
        inner.bytes = inner.bytes.saturating_add(body_len);
        touch_small_body_cache_order(&mut inner, &key);

        let mut evicted = 0usize;
        while inner.bytes > self.max_bytes {
            let Some(evicted_key) = inner.order.pop_front() else {
                break;
            };
            if evicted_key == key {
                inner.order.push_back(evicted_key);
                break;
            }
            if let Some(evicted_body) = inner.entries.remove(&evicted_key) {
                inner.bytes = inner.bytes.saturating_sub(evicted_body.len());
                evicted = evicted.saturating_add(1);
            }
        }

        SmallBodyCacheInsert {
            inserted: true,
            evicted,
            cache_len: inner.entries.len(),
            cache_bytes: inner.bytes,
        }
    }

    fn len(&self) -> usize {
        self.lock_inner().entries.len()
    }

    fn bytes(&self) -> usize {
        self.lock_inner().bytes
    }

    fn lock_inner(&self) -> std::sync::MutexGuard<'_, SmallBodyCacheInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug)]
struct SmallBodyCacheInsert {
    inserted: bool,
    evicted: usize,
    cache_len: usize,
    cache_bytes: usize,
}

fn touch_small_body_cache_order(inner: &mut SmallBodyCacheInner, key: &SmallBodyCacheKey) {
    inner.order.retain(|candidate| candidate != key);
    inner.order.push_back(key.clone());
}

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    max_concurrent_requests: usize,
    unixfs_metadata_cache_capacity: usize,
    small_body_cache_max_bytes: usize,
    html_prefetch: GatewayHtmlPrefetchConfig,
    html_directory_prefetch: GatewayHtmlDirectoryPrefetchConfig,
    html_range_warm: GatewayHtmlRangeWarmConfig,
    raw_link_tsize_fast_headers: bool,
    stream_small_bodies: bool,
}

impl GatewayConfig {
    pub fn new(max_concurrent_requests: usize) -> Self {
        Self {
            max_concurrent_requests: max_concurrent_requests.max(1),
            unixfs_metadata_cache_capacity: DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY,
            small_body_cache_max_bytes: DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES,
            html_prefetch: GatewayHtmlPrefetchConfig::default(),
            html_directory_prefetch: GatewayHtmlDirectoryPrefetchConfig::default(),
            html_range_warm: GatewayHtmlRangeWarmConfig::default(),
            raw_link_tsize_fast_headers: false,
            stream_small_bodies: false,
        }
    }

    pub fn max_concurrent_requests(&self) -> usize {
        self.max_concurrent_requests
    }

    pub fn unixfs_metadata_cache_capacity(&self) -> usize {
        self.unixfs_metadata_cache_capacity
    }

    pub fn with_unixfs_metadata_cache_capacity(mut self, capacity: usize) -> Self {
        self.unixfs_metadata_cache_capacity = capacity;
        self
    }

    pub fn small_body_cache_max_bytes(&self) -> usize {
        self.small_body_cache_max_bytes
    }

    pub fn with_small_body_cache_max_bytes(mut self, max_bytes: usize) -> Self {
        self.small_body_cache_max_bytes = max_bytes;
        self
    }

    pub fn html_prefetch(&self) -> GatewayHtmlPrefetchConfig {
        self.html_prefetch
    }

    pub fn with_html_prefetch(mut self, html_prefetch: GatewayHtmlPrefetchConfig) -> Self {
        self.html_prefetch = html_prefetch;
        self
    }

    pub fn html_directory_prefetch(&self) -> GatewayHtmlDirectoryPrefetchConfig {
        self.html_directory_prefetch
    }

    pub fn with_html_directory_prefetch(
        mut self,
        html_directory_prefetch: GatewayHtmlDirectoryPrefetchConfig,
    ) -> Self {
        self.html_directory_prefetch = html_directory_prefetch;
        self
    }

    pub fn html_range_warm(&self) -> GatewayHtmlRangeWarmConfig {
        self.html_range_warm
    }

    pub fn with_html_range_warm(mut self, html_range_warm: GatewayHtmlRangeWarmConfig) -> Self {
        self.html_range_warm = html_range_warm;
        self
    }

    pub fn raw_link_tsize_fast_headers(&self) -> bool {
        self.raw_link_tsize_fast_headers
    }

    pub fn with_raw_link_tsize_fast_headers(mut self, enabled: bool) -> Self {
        self.raw_link_tsize_fast_headers = enabled;
        self
    }

    pub fn stream_small_bodies(&self) -> bool {
        self.stream_small_bodies
    }

    pub fn with_stream_small_bodies(mut self, enabled: bool) -> Self {
        self.stream_small_bodies = enabled;
        self
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self::new(DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayHtmlPrefetchConfig {
    max_assets: usize,
    max_bytes: u64,
    concurrency: usize,
}

impl GatewayHtmlPrefetchConfig {
    pub fn new(max_assets: usize, max_bytes: u64, concurrency: usize) -> Self {
        Self {
            max_assets,
            max_bytes,
            concurrency: concurrency.max(1),
        }
    }

    pub fn disabled() -> Self {
        Self::new(
            DEFAULT_GATEWAY_HTML_PREFETCH_MAX_ASSETS,
            DEFAULT_GATEWAY_HTML_PREFETCH_MAX_BYTES,
            DEFAULT_GATEWAY_HTML_PREFETCH_CONCURRENCY,
        )
    }

    pub fn is_enabled(self) -> bool {
        self.max_assets > 0 && self.max_bytes > 0
    }

    pub fn max_assets(self) -> usize {
        self.max_assets
    }

    pub fn max_bytes(self) -> u64 {
        self.max_bytes
    }

    pub fn concurrency(self) -> usize {
        self.concurrency
    }
}

impl Default for GatewayHtmlPrefetchConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayHtmlDirectoryPrefetchConfig {
    max_dirs: usize,
    max_bytes: u64,
    concurrency: usize,
}

impl GatewayHtmlDirectoryPrefetchConfig {
    pub fn new(max_dirs: usize, max_bytes: u64, concurrency: usize) -> Self {
        Self {
            max_dirs,
            max_bytes,
            concurrency: concurrency.max(1),
        }
    }

    pub fn disabled() -> Self {
        Self::new(
            DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_DIRS,
            DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_BYTES,
            DEFAULT_GATEWAY_HTML_DIRECTORY_PREFETCH_CONCURRENCY,
        )
    }

    pub fn is_enabled(self) -> bool {
        self.max_dirs > 0 && self.max_bytes > 0
    }

    pub fn max_dirs(self) -> usize {
        self.max_dirs
    }

    pub fn max_bytes(self) -> u64 {
        self.max_bytes
    }

    pub fn concurrency(self) -> usize {
        self.concurrency
    }
}

impl Default for GatewayHtmlDirectoryPrefetchConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GatewayHtmlRangeWarmConfig {
    max_bytes: u64,
    concurrency: usize,
}

impl GatewayHtmlRangeWarmConfig {
    pub fn new(max_bytes: u64, concurrency: usize) -> Self {
        Self {
            max_bytes,
            concurrency: concurrency.max(1),
        }
    }

    pub fn disabled() -> Self {
        Self::new(
            DEFAULT_GATEWAY_HTML_RANGE_WARM_MAX_BYTES,
            DEFAULT_GATEWAY_HTML_RANGE_WARM_CONCURRENCY,
        )
    }

    pub fn is_enabled(self) -> bool {
        self.max_bytes > 0
    }

    pub fn max_bytes(self) -> u64 {
        self.max_bytes
    }

    pub fn concurrency(self) -> usize {
        self.concurrency
    }
}

impl Default for GatewayHtmlRangeWarmConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone)]
struct HtmlPrefetchRuntime {
    config: GatewayHtmlPrefetchConfig,
    limiter: Arc<Semaphore>,
}

impl HtmlPrefetchRuntime {
    fn new(config: GatewayHtmlPrefetchConfig) -> Self {
        Self {
            config,
            limiter: Arc::new(Semaphore::new(config.concurrency())),
        }
    }

    fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }
}

#[derive(Clone)]
struct HtmlDirectoryPrefetchRuntime {
    config: GatewayHtmlDirectoryPrefetchConfig,
    limiter: Arc<Semaphore>,
}

impl HtmlDirectoryPrefetchRuntime {
    fn new(config: GatewayHtmlDirectoryPrefetchConfig) -> Self {
        Self {
            config,
            limiter: Arc::new(Semaphore::new(config.concurrency())),
        }
    }

    fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }
}

#[derive(Clone)]
struct HtmlRangeWarmRuntime {
    config: GatewayHtmlRangeWarmConfig,
    limiter: Arc<Semaphore>,
}

impl HtmlRangeWarmRuntime {
    fn new(config: GatewayHtmlRangeWarmConfig) -> Self {
        Self {
            config,
            limiter: Arc::new(Semaphore::new(config.concurrency())),
        }
    }

    fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }
}

#[derive(Clone)]
pub struct GatewayState {
    provider: Arc<dyn BlockProvider>,
    name_resolver: Arc<dyn NameResolver>,
    unixfs: UnixfsResolver,
    request_limiter: Arc<Semaphore>,
    small_body_cache: Arc<SmallBodyCache>,
    html_prefetch: HtmlPrefetchRuntime,
    html_directory_prefetch: HtmlDirectoryPrefetchRuntime,
    html_range_warm: HtmlRangeWarmRuntime,
    raw_link_tsize_fast_headers: bool,
    stream_small_bodies: bool,
}

#[derive(Debug, Clone, Copy)]
enum FileCachePolicy {
    ImmutableIpfs,
    RevalidateIpns,
}

impl FileCachePolicy {
    fn header_value(self) -> &'static str {
        match self {
            Self::ImmutableIpfs => CACHE_CONTROL_IPFS_FILE,
            Self::RevalidateIpns => CACHE_CONTROL_IPNS_FILE,
        }
    }
}

impl GatewayState {
    pub fn new(store: SqliteBlockStore) -> Self {
        Self::with_provider(Arc::new(store))
    }

    pub fn with_provider(provider: Arc<dyn BlockProvider>) -> Self {
        Self::with_provider_config(provider, GatewayConfig::default())
    }

    pub fn with_provider_config(provider: Arc<dyn BlockProvider>, config: GatewayConfig) -> Self {
        Self::with_provider_and_name_resolver_config(
            provider,
            Arc::new(OfflineNameResolver),
            config,
        )
    }

    pub fn with_provider_and_name_resolver(
        provider: Arc<dyn BlockProvider>,
        name_resolver: Arc<dyn NameResolver>,
    ) -> Self {
        Self::with_provider_and_name_resolver_config(
            provider,
            name_resolver,
            GatewayConfig::default(),
        )
    }

    pub fn with_provider_and_name_resolver_config(
        provider: Arc<dyn BlockProvider>,
        name_resolver: Arc<dyn NameResolver>,
        config: GatewayConfig,
    ) -> Self {
        let unixfs =
            UnixfsResolver::with_metadata_cache_capacity(config.unixfs_metadata_cache_capacity());
        Self {
            provider,
            name_resolver,
            unixfs,
            request_limiter: Arc::new(Semaphore::new(config.max_concurrent_requests())),
            small_body_cache: Arc::new(SmallBodyCache::new(config.small_body_cache_max_bytes())),
            html_prefetch: HtmlPrefetchRuntime::new(config.html_prefetch()),
            html_directory_prefetch: HtmlDirectoryPrefetchRuntime::new(
                config.html_directory_prefetch(),
            ),
            html_range_warm: HtmlRangeWarmRuntime::new(config.html_range_warm()),
            raw_link_tsize_fast_headers: config.raw_link_tsize_fast_headers(),
            stream_small_bodies: config.stream_small_bodies(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayNamespace {
    Ipfs,
    Ipns,
}

impl GatewayNamespace {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ipfs => "ipfs",
            Self::Ipns => "ipns",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Ipfs => "/ipfs",
            Self::Ipns => "/ipns",
        }
    }
}

#[derive(Debug, Clone)]
pub struct GatewayCoreRequest {
    pub namespace: GatewayNamespace,
    pub path: String,
    pub method: Method,
    pub headers: HeaderMap,
}

impl GatewayCoreRequest {
    pub fn ipfs(path: impl Into<String>, method: Method, headers: HeaderMap) -> Self {
        Self {
            namespace: GatewayNamespace::Ipfs,
            path: normalize_namespace_path(path.into()),
            method,
            headers,
        }
    }

    pub fn ipns(path: impl Into<String>, method: Method, headers: HeaderMap) -> Self {
        Self {
            namespace: GatewayNamespace::Ipns,
            path: normalize_namespace_path(path.into()),
            method,
            headers,
        }
    }

    fn request_path(&self) -> String {
        format!(
            "{}{path}",
            self.namespace.prefix(),
            path = prefixed_path(&self.path)
        )
    }
}

#[derive(Clone)]
pub struct GatewayCore {
    state: GatewayState,
}

impl GatewayCore {
    pub fn new(store: SqliteBlockStore) -> Self {
        Self::with_provider(Arc::new(store))
    }

    pub fn with_provider(provider: Arc<dyn BlockProvider>) -> Self {
        Self::from_state(GatewayState::with_provider(provider))
    }

    pub fn with_provider_config(provider: Arc<dyn BlockProvider>, config: GatewayConfig) -> Self {
        Self::from_state(GatewayState::with_provider_config(provider, config))
    }

    pub fn with_provider_and_name_resolver(
        provider: Arc<dyn BlockProvider>,
        name_resolver: Arc<dyn NameResolver>,
    ) -> Self {
        Self::from_state(GatewayState::with_provider_and_name_resolver(
            provider,
            name_resolver,
        ))
    }

    pub fn with_provider_and_name_resolver_config(
        provider: Arc<dyn BlockProvider>,
        name_resolver: Arc<dyn NameResolver>,
        config: GatewayConfig,
    ) -> Self {
        Self::from_state(GatewayState::with_provider_and_name_resolver_config(
            provider,
            name_resolver,
            config,
        ))
    }

    fn from_state(state: GatewayState) -> Self {
        Self { state }
    }

    pub async fn handle(&self, request: GatewayCoreRequest) -> Response {
        handle_gateway_core_request(self.state.clone(), request).await
    }
}

fn prefixed_path(path: &str) -> String {
    if path.is_empty() || path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

fn normalize_namespace_path(path: String) -> String {
    path.trim_start_matches('/').to_string()
}

pub fn router(store: SqliteBlockStore) -> Router {
    router_with_provider(Arc::new(store))
}

pub fn router_with_provider(provider: Arc<dyn BlockProvider>) -> Router {
    router_with_provider_and_name_resolver(provider, Arc::new(OfflineNameResolver))
}

pub fn router_with_provider_config(
    provider: Arc<dyn BlockProvider>,
    config: GatewayConfig,
) -> Router {
    router_with_provider_and_name_resolver_config(provider, Arc::new(OfflineNameResolver), config)
}

pub fn router_with_provider_and_name_resolver(
    provider: Arc<dyn BlockProvider>,
    name_resolver: Arc<dyn NameResolver>,
) -> Router {
    router_with_provider_and_name_resolver_config(provider, name_resolver, GatewayConfig::default())
}

pub fn router_with_provider_and_name_resolver_config(
    provider: Arc<dyn BlockProvider>,
    name_resolver: Arc<dyn NameResolver>,
    config: GatewayConfig,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ipfs/{*path}", get(ipfs_get))
        .route("/ipns/{*path}", get(ipns_get))
        .with_state(GatewayState::with_provider_and_name_resolver_config(
            provider,
            name_resolver,
            config,
        ))
}

#[derive(Debug, Clone)]
pub struct OfflineNameResolver;

#[async_trait::async_trait]
impl NameResolver for OfflineNameResolver {
    async fn resolve_name(&self, name: &str) -> freedom_ipfs_namesys::Result<String> {
        Err(NamesysError::NotFound(name.to_string()))
    }
}

#[derive(Clone)]
pub struct PersistentNameResolver<R> {
    inner: R,
    store: SqliteBlockStore,
    ttl: Duration,
}

impl<R> PersistentNameResolver<R> {
    pub fn new(inner: R, store: SqliteBlockStore) -> Self {
        Self::with_ttl(inner, store, DEFAULT_PERSISTENT_NAME_CACHE_TTL)
    }

    pub fn with_ttl(inner: R, store: SqliteBlockStore, ttl: Duration) -> Self {
        Self { inner, store, ttl }
    }
}

impl PersistentNameResolver<OfflineNameResolver> {
    pub fn cache_only(store: SqliteBlockStore) -> Self {
        Self::new(OfflineNameResolver, store)
    }
}

#[async_trait::async_trait]
impl<R> NameResolver for PersistentNameResolver<R>
where
    R: NameResolver,
{
    async fn resolve_name(&self, name: &str) -> freedom_ipfs_namesys::Result<String> {
        self.resolve_name_with_ttl(name)
            .await
            .map(|resolved| resolved.value)
    }

    async fn resolve_name_with_ttl(
        &self,
        name: &str,
    ) -> freedom_ipfs_namesys::Result<ResolvedName> {
        match self.store.get_name_record(name) {
            Ok(Some(value)) => {
                tracing::info!(
                    phase = "name_persistent_cache",
                    name,
                    cache_hit = true,
                    resolved_target = %value
                );
                return Ok(ResolvedName::new(value));
            }
            Ok(None) => {
                tracing::info!(phase = "name_persistent_cache", name, cache_hit = false);
            }
            Err(err) => {
                tracing::warn!(
                    phase = "name_persistent_cache",
                    name,
                    cache_hit = false,
                    error = %err
                );
            }
        }

        let resolved = self.inner.resolve_name_with_ttl(name).await?;
        if is_cacheable_name_target(&resolved.value) {
            let ttl = resolved.ttl.map_or(self.ttl, |ttl| ttl.min(self.ttl));
            if let Err(err) = self.store.put_name_record(name, &resolved.value, ttl) {
                tracing::warn!(
                    phase = "name_persistent_cache_store",
                    name,
                    resolved_target = %resolved.value,
                    error = %err
                );
            }
        }
        Ok(resolved)
    }
}

fn is_cacheable_name_target(value: &str) -> bool {
    value.starts_with("/ipfs/") || value.starts_with("/ipns/")
}

pub async fn serve(store: SqliteBlockStore, addr: SocketAddr) -> std::io::Result<SocketAddr> {
    serve_config(store, addr, GatewayConfig::default()).await
}

pub async fn serve_config(
    store: SqliteBlockStore,
    addr: SocketAddr,
    config: GatewayConfig,
) -> std::io::Result<SocketAddr> {
    serve_with_provider_config(Arc::new(store), addr, config).await
}

pub async fn serve_with_provider(
    provider: Arc<dyn BlockProvider>,
    addr: SocketAddr,
) -> std::io::Result<SocketAddr> {
    serve_with_provider_config(provider, addr, GatewayConfig::default()).await
}

pub async fn serve_with_provider_config(
    provider: Arc<dyn BlockProvider>,
    addr: SocketAddr,
    config: GatewayConfig,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    axum::serve(listener, router_with_provider_config(provider, config)).await?;
    Ok(bound)
}

pub async fn serve_with_provider_and_name_resolver_config(
    provider: Arc<dyn BlockProvider>,
    name_resolver: Arc<dyn NameResolver>,
    addr: SocketAddr,
    config: GatewayConfig,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    axum::serve(
        listener,
        router_with_provider_and_name_resolver_config(provider, name_resolver, config),
    )
    .await?;
    Ok(bound)
}

async fn health() -> &'static str {
    "ok\n"
}

async fn ipfs_get(
    State(state): State<GatewayState>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    GatewayCore::from_state(state)
        .handle(GatewayCoreRequest::ipfs(path, method, headers))
        .await
}

async fn ipns_get(
    State(state): State<GatewayState>,
    Path(path): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    GatewayCore::from_state(state)
        .handle(GatewayCoreRequest::ipns(path, method, headers))
        .await
}

async fn handle_gateway_core_request(state: GatewayState, request: GatewayCoreRequest) -> Response {
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();
    let request_path = request.request_path();
    let range = header_value_for_trace(request.headers.get(RANGE));
    let progress_request_id = header_u64_for_trace(&request.headers, X_FREEDOM_REQUEST_ID);
    let parent_request_id = header_u64_for_trace(&request.headers, X_FREEDOM_PARENT_REQUEST_ID);
    let top_level_path = header_value_for_trace(request.headers.get(X_FREEDOM_TOP_LEVEL_PATH));
    let span = tracing::info_span!(
        "gateway_request",
        process_id,
        request_id,
        progress_request_id = progress_request_id.unwrap_or_default(),
        parent_request_id = parent_request_id.unwrap_or_default(),
        top_level_path = %top_level_path,
        namespace = request.namespace.as_str(),
        path = %request_path,
        range = %range
    );

    async move {
        let request_started = Instant::now();
        tracing::info!(phase = "request_start", request_id, path = %request_path);

        let Some(_permit) =
            acquire_gateway_request_permit(state.request_limiter.clone(), request_id).await
        else {
            let response = gateway_error(GatewayError::Busy);
            tracing::info!(
                phase = "request_done",
                request_id,
                status = response.status().as_u16(),
                body_mode = gateway_body_mode(&response),
                elapsed_ms = request_started.elapsed().as_millis()
            );
            return response;
        };

        let context_top_level_path = if top_level_path.is_empty() {
            request_path.clone()
        } else {
            top_level_path.clone()
        };
        let retrieval_context = RetrievalRequestContext::gateway_request_with_top_level(
            parent_request_id,
            Some(context_top_level_path),
        );
        let range = request.headers.get(RANGE);
        let is_head = request.method == Method::HEAD;
        let response_features = GatewayResponseFeatures {
            small_body_cache: state.small_body_cache.clone(),
            html_prefetch: html_prefetch_runtime(&state, is_head, range, parent_request_id),
            html_directory_prefetch: html_directory_prefetch_runtime(
                &state,
                is_head,
                range,
                parent_request_id,
            ),
            html_range_warm: html_range_warm_runtime(&state, is_head, range, parent_request_id),
            raw_link_tsize_fast_headers: state.raw_link_tsize_fast_headers,
            stream_small_bodies: state.stream_small_bodies,
        };
        let response = with_retrieval_request_context(retrieval_context, async {
            let request_headers = GatewayRequestHeaders {
                range,
                if_none_match: request.headers.get(IF_NONE_MATCH),
                is_head,
            };
            let result = match request.namespace {
                GatewayNamespace::Ipfs => {
                    serve_ipfs_path(
                        state.provider.clone(),
                        state.unixfs.clone(),
                        response_features,
                        &request.path,
                        request_headers,
                    )
                    .await
                }
                GatewayNamespace::Ipns => {
                    serve_ipns_path(
                        state.provider.clone(),
                        state.unixfs.clone(),
                        response_features,
                        state.name_resolver.as_ref(),
                        &request.path,
                        request_headers,
                    )
                    .await
                }
            };
            result.unwrap_or_else(gateway_error)
        })
        .await;
        let response = if is_head {
            head_response_without_body(response)
        } else {
            response
        };
        tracing::info!(
            phase = "request_done",
            request_id,
            status = response.status().as_u16(),
            body_mode = gateway_body_mode(&response),
            elapsed_ms = request_started.elapsed().as_millis()
        );
        response
    }
    .instrument(span)
    .await
}

fn html_prefetch_runtime(
    state: &GatewayState,
    is_head: bool,
    range: Option<&HeaderValue>,
    parent_request_id: Option<u64>,
) -> Option<HtmlPrefetchRuntime> {
    if is_head
        || range.is_some()
        || parent_request_id.is_some()
        || !state.html_prefetch.is_enabled()
    {
        None
    } else {
        Some(state.html_prefetch.clone())
    }
}

fn html_directory_prefetch_runtime(
    state: &GatewayState,
    is_head: bool,
    range: Option<&HeaderValue>,
    parent_request_id: Option<u64>,
) -> Option<HtmlDirectoryPrefetchRuntime> {
    if is_head
        || range.is_some()
        || parent_request_id.is_some()
        || !state.html_directory_prefetch.is_enabled()
    {
        None
    } else {
        Some(state.html_directory_prefetch.clone())
    }
}

fn html_range_warm_runtime(
    state: &GatewayState,
    is_head: bool,
    range: Option<&HeaderValue>,
    parent_request_id: Option<u64>,
) -> Option<HtmlRangeWarmRuntime> {
    if is_head
        || range.is_none()
        || parent_request_id.is_some()
        || !state.html_range_warm.is_enabled()
    {
        None
    } else {
        Some(state.html_range_warm.clone())
    }
}

async fn acquire_gateway_request_permit(
    limiter: Arc<Semaphore>,
    request_id: u64,
) -> Option<OwnedSemaphorePermit> {
    let limiter_started = Instant::now();
    match tokio::time::timeout(GATEWAY_REQUEST_QUEUE_TIMEOUT, limiter.acquire_owned()).await {
        Ok(Ok(permit)) => {
            tracing::info!(
                phase = "gateway_limiter",
                request_id,
                acquired = true,
                timeout_ms = GATEWAY_REQUEST_QUEUE_TIMEOUT.as_millis(),
                elapsed_ms = limiter_started.elapsed().as_millis()
            );
            Some(permit)
        }
        Ok(Err(err)) => {
            tracing::info!(
                phase = "gateway_limiter",
                request_id,
                acquired = false,
                error = %err,
                timeout_ms = GATEWAY_REQUEST_QUEUE_TIMEOUT.as_millis(),
                elapsed_ms = limiter_started.elapsed().as_millis()
            );
            None
        }
        Err(_) => {
            tracing::info!(
                phase = "gateway_limiter",
                request_id,
                acquired = false,
                timeout_ms = GATEWAY_REQUEST_QUEUE_TIMEOUT.as_millis(),
                elapsed_ms = limiter_started.elapsed().as_millis()
            );
            None
        }
    }
}

fn header_value_for_trace(value: Option<&HeaderValue>) -> String {
    value
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn gateway_body_mode(response: &Response) -> &'static str {
    response
        .extensions()
        .get::<GatewayBodyMode>()
        .copied()
        .unwrap_or(GatewayBodyMode::Direct)
        .as_str()
}

fn set_gateway_body_mode(response: &mut Response, mode: GatewayBodyMode) {
    response.extensions_mut().insert(mode);
}

fn header_u64_for_trace(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn head_response_without_body(response: Response) -> Response {
    let (parts, _body) = response.into_parts();
    Response::from_parts(parts, Body::empty())
}

#[derive(Clone)]
struct GatewayResponseFeatures {
    small_body_cache: Arc<SmallBodyCache>,
    html_prefetch: Option<HtmlPrefetchRuntime>,
    html_directory_prefetch: Option<HtmlDirectoryPrefetchRuntime>,
    html_range_warm: Option<HtmlRangeWarmRuntime>,
    raw_link_tsize_fast_headers: bool,
    stream_small_bodies: bool,
}

#[derive(Clone, Copy)]
struct GatewayRequestHeaders<'a> {
    range: Option<&'a HeaderValue>,
    if_none_match: Option<&'a HeaderValue>,
    is_head: bool,
}

async fn serve_ipfs_path(
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    features: GatewayResponseFeatures,
    path: &str,
    request_headers: GatewayRequestHeaders<'_>,
) -> Result<Response, GatewayError> {
    serve_ipfs_path_with_listing_path(
        provider,
        unixfs,
        features,
        path,
        request_headers,
        GatewayIpfsPathOptions::default(),
    )
    .await
}

#[derive(Clone, Copy)]
struct GatewayIpfsPathOptions<'a> {
    listing_path: Option<&'a DirectoryListingPath>,
    cache_policy: FileCachePolicy,
}

impl Default for GatewayIpfsPathOptions<'_> {
    fn default() -> Self {
        Self {
            listing_path: None,
            cache_policy: FileCachePolicy::ImmutableIpfs,
        }
    }
}

async fn serve_ipfs_path_with_listing_path(
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    features: GatewayResponseFeatures,
    path: &str,
    request_headers: GatewayRequestHeaders<'_>,
    options: GatewayIpfsPathOptions<'_>,
) -> Result<Response, GatewayError> {
    let parse_started = Instant::now();
    let (cid, unixfs_path) = split_ipfs_path(path)?;
    tracing::info!(
        phase = "ipfs_path_parse",
        cid = %cid,
        unixfs_path,
        elapsed_ms = parse_started.elapsed().as_millis()
    );

    let cache_before = unixfs.metadata_cache_stats();
    let resource_started = Instant::now();
    let resource = served_resource(
        &unixfs,
        provider.as_ref(),
        &cid,
        unixfs_path,
        features.raw_link_tsize_fast_headers && !request_headers.is_head,
    )?;
    let resource_elapsed_ms = resource_started.elapsed().as_millis();
    let response = match resource {
        ServedResource::File {
            path,
            cid: file_cid,
            len,
        } => {
            let target = FileResponseTarget {
                root_cid: cid,
                file_cid,
                path,
                len,
            };
            tracing::info!(
                phase = "unixfs_resource",
                cid = %target.root_cid,
                file_cid = %target.file_cid,
                unixfs_path = %target.path,
                resource = "file",
                file_len = target.len,
                elapsed_ms = resource_elapsed_ms
            );
            let etag = file_etag(&target.root_cid, &target.path, target.len);
            if request_headers.range.is_none()
                && if_none_match_matches(request_headers.if_none_match, &etag)
            {
                tracing::info!(
                    phase = "gateway_conditional",
                    cid = %target.root_cid,
                    file_cid = %target.file_cid,
                    unixfs_path = %target.path,
                    etag = %etag,
                    outcome = "not_modified"
                );
                return not_modified_response(&etag, options.cache_policy);
            }
            let parsed_range = request_headers
                .range
                .map(|range| parse_range_header(range, target.len))
                .transpose()?;
            let mime_started = Instant::now();
            let mime = mime_for_served_file(
                &unixfs,
                provider.as_ref(),
                &target.root_cid,
                &target.file_cid,
                &target.path,
                target.len,
                mime_sniff_end(target.len, parsed_range),
            )?;
            tracing::info!(
                phase = "mime_total",
                cid = %target.root_cid,
                file_cid = %target.file_cid,
                unixfs_path = %target.path,
                mime = %mime,
                elapsed_ms = mime_started.elapsed().as_millis()
            );
            let headers = FileResponseHeaders {
                mime: &mime,
                etag: &etag,
                cache_policy: options.cache_policy,
                is_head: request_headers.is_head,
            };
            if let Some((start, end)) = parsed_range {
                ranged_response(
                    provider,
                    unixfs.clone(),
                    features.clone(),
                    target,
                    start,
                    end,
                    headers,
                )?
            } else {
                streaming_response(provider, unixfs.clone(), features, target, headers)?
            }
        }
        ServedResource::Directory { path, entries } => {
            tracing::info!(
                phase = "unixfs_resource",
                cid = %cid,
                unixfs_path = %path,
                resource = "directory",
                entry_count = entries.len(),
                elapsed_ms = resource_elapsed_ms
            );
            if request_headers.range.is_some() {
                return Err(GatewayError::BadRequest(
                    "Range requests are not supported for directory listings".into(),
                ));
            }
            let default_listing_path;
            let listing_path = if let Some(listing_path) = options.listing_path {
                listing_path
            } else {
                default_listing_path = DirectoryListingPath::ipfs(&cid, &path);
                &default_listing_path
            };
            directory_listing_response(listing_path, &entries)?
        }
    };
    trace_unixfs_metadata_cache_delta(cache_before, unixfs.metadata_cache_stats());
    Ok(response)
}

fn trace_unixfs_metadata_cache_delta(
    before: UnixfsMetadataCacheStats,
    after: UnixfsMetadataCacheStats,
) {
    tracing::info!(
        phase = "unixfs_metadata_cache",
        elapsed_ms = 0u64,
        cache_capacity = after.capacity,
        cache_len = after.len,
        hits = after.hits.saturating_sub(before.hits),
        misses = after.misses.saturating_sub(before.misses),
        inserts = after.inserts.saturating_sub(before.inserts),
        evictions = after.evictions.saturating_sub(before.evictions),
        oversized_skips = after.oversized_skips.saturating_sub(before.oversized_skips),
        path_cache_len = after.path_len,
        path_hits = after.path_hits.saturating_sub(before.path_hits),
        path_misses = after.path_misses.saturating_sub(before.path_misses),
        path_inserts = after.path_inserts.saturating_sub(before.path_inserts),
        path_evictions = after.path_evictions.saturating_sub(before.path_evictions),
        path_oversized_skips = after
            .path_oversized_skips
            .saturating_sub(before.path_oversized_skips),
        file_size_cache_len = after.file_size_len,
        file_size_hits = after.file_size_hits.saturating_sub(before.file_size_hits),
        file_size_misses = after
            .file_size_misses
            .saturating_sub(before.file_size_misses),
        file_size_inserts = after
            .file_size_inserts
            .saturating_sub(before.file_size_inserts),
        file_size_evictions = after
            .file_size_evictions
            .saturating_sub(before.file_size_evictions)
    );
}

fn mime_for_served_file(
    unixfs: &UnixfsResolver,
    provider: &dyn BlockProvider,
    cid: &Cid,
    file_cid: &Cid,
    path: &str,
    len: u64,
    sniff_end: Option<u64>,
) -> Result<String, GatewayError> {
    let started = Instant::now();
    if let Some(mime) = mime_guess::from_path(path).first() {
        tracing::info!(
            phase = "mime_detect",
            cid = %cid,
            unixfs_path = path,
            source = "extension",
            elapsed_ms = started.elapsed().as_millis()
        );
        return Ok(mime.to_string());
    }
    let sniffed = if let Some(end) = sniff_end {
        let sniff_started = Instant::now();
        let prefix = unixfs
            .read_file_cid_range(provider, file_cid, 0, end)
            .map_err(GatewayError::Unixfs)?;
        tracing::info!(
            phase = "mime_sniff_read",
            cid = %cid,
            file_cid = %file_cid,
            unixfs_path = path,
            bytes = prefix.len(),
            elapsed_ms = sniff_started.elapsed().as_millis()
        );
        if looks_like_html(&prefix) {
            tracing::info!(
                phase = "mime_detect",
                cid = %cid,
                unixfs_path = path,
                source = "sniff_html",
                elapsed_ms = started.elapsed().as_millis()
            );
            return Ok("text/html".to_string());
        }
        true
    } else {
        false
    };
    tracing::info!(
        phase = "mime_detect",
        cid = %cid,
        unixfs_path = path,
        source = mime_fallback_source(len, sniffed),
        elapsed_ms = started.elapsed().as_millis()
    );
    Ok("application/octet-stream".to_string())
}

fn mime_fallback_source(len: u64, sniffed: bool) -> &'static str {
    match (len, sniffed) {
        (0, _) => "fallback_empty",
        (_, true) => "fallback_after_sniff",
        (_, false) => "fallback_no_sniff",
    }
}

fn mime_sniff_end(len: u64, parsed_range: Option<(u64, u64)>) -> Option<u64> {
    if len == 0 {
        return None;
    }
    let last_sniff_byte = (len - 1).min(512);
    match parsed_range {
        Some((0, end)) => Some(end.min(last_sniff_byte)),
        Some(_) => None,
        None => Some(last_sniff_byte),
    }
}

fn looks_like_html(bytes: &[u8]) -> bool {
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text
        .trim_start_matches('\u{feff}')
        .trim_start_matches(|ch: char| ch.is_ascii_whitespace());
    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("<!doctype html")
        || lower.starts_with("<html")
        || lower.starts_with("<head")
        || lower.starts_with("<body")
}

struct HtmlPrefetchSeed<'a> {
    root_cid: Cid,
    base_path: &'a str,
    mime: &'a str,
    body: &'a Bytes,
}

struct HtmlRangeWarmSeed<'a> {
    root_cid: Cid,
    file_cid: Cid,
    path: &'a str,
    mime: &'a str,
    total_len: u64,
    range_start: u64,
    range_end: u64,
}

fn maybe_spawn_html_directory_prefetch(
    runtime: Option<&HtmlDirectoryPrefetchRuntime>,
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    seed: HtmlPrefetchSeed<'_>,
) {
    let Some(runtime) = runtime.filter(|runtime| runtime.is_enabled()) else {
        return;
    };
    if !seed.mime.starts_with("text/html") || seed.body.len() as u64 > runtime.config.max_bytes() {
        return;
    }
    let html = String::from_utf8_lossy(seed.body);
    let paths = html_prefetch_directory_paths(&html, seed.base_path, runtime.config.max_dirs());
    if paths.is_empty() {
        return;
    }
    tracing::info!(
        phase = "gateway_html_directory_prefetch_schedule",
        cid = %seed.root_cid,
        unixfs_path = %seed.base_path,
        directory_count = paths.len(),
        max_dirs = runtime.config.max_dirs(),
        max_bytes = runtime.config.max_bytes(),
        concurrency = runtime.config.concurrency(),
    );

    for path in paths {
        let provider = provider.clone();
        let unixfs = unixfs.clone();
        let limiter = runtime.limiter.clone();
        let root_cid = seed.root_cid;
        let span = tracing::info_span!(
            "gateway_html_directory_prefetch",
            cid = %root_cid,
            unixfs_path = %path
        );
        tokio::spawn(
            async move {
                let queued = Instant::now();
                let permit = match tokio::time::timeout(
                    GATEWAY_HTML_DIRECTORY_PREFETCH_QUEUE_TIMEOUT,
                    limiter.acquire_owned(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(err)) => {
                        tracing::info!(
                            phase = "gateway_html_directory_prefetch_failed",
                            cid = %root_cid,
                            unixfs_path = %path,
                            error = %err,
                            elapsed_ms = queued.elapsed().as_millis()
                        );
                        return;
                    }
                    Err(_) => {
                        tracing::info!(
                            phase = "gateway_html_directory_prefetch_skip",
                            cid = %root_cid,
                            unixfs_path = %path,
                            reason = "queue_timeout",
                            timeout_ms =
                                GATEWAY_HTML_DIRECTORY_PREFETCH_QUEUE_TIMEOUT.as_millis(),
                            elapsed_ms = queued.elapsed().as_millis()
                        );
                        return;
                    }
                };
                let _permit = permit;
                let task_path = path.clone();
                let joined = tokio::task::spawn_blocking(move || {
                    html_directory_prefetch_one(provider.as_ref(), &unixfs, root_cid, &task_path)
                })
                .await;
                if let Err(err) = joined {
                    tracing::info!(
                        phase = "gateway_html_directory_prefetch_failed",
                        cid = %root_cid,
                        unixfs_path = %path,
                        error = %err
                    );
                }
            }
            .instrument(span),
        );
    }
}

fn maybe_spawn_html_prefetch(
    runtime: Option<&HtmlPrefetchRuntime>,
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    small_body_cache: Arc<SmallBodyCache>,
    seed: HtmlPrefetchSeed<'_>,
) {
    let Some(runtime) = runtime.filter(|runtime| runtime.is_enabled()) else {
        return;
    };
    if !seed.mime.starts_with("text/html") || seed.body.len() as u64 > runtime.config.max_bytes() {
        return;
    }
    let html = String::from_utf8_lossy(seed.body);
    let paths = html_prefetch_asset_paths(&html, seed.base_path, runtime.config.max_assets());
    if paths.is_empty() {
        return;
    }
    tracing::info!(
        phase = "gateway_html_prefetch_schedule",
        cid = %seed.root_cid,
        unixfs_path = %seed.base_path,
        asset_count = paths.len(),
        max_assets = runtime.config.max_assets(),
        max_bytes = runtime.config.max_bytes(),
        concurrency = runtime.config.concurrency(),
    );

    for path in paths {
        let provider = provider.clone();
        let unixfs = unixfs.clone();
        let small_body_cache = small_body_cache.clone();
        let config = runtime.config;
        let limiter = runtime.limiter.clone();
        let root_cid = seed.root_cid;
        let span = tracing::info_span!(
            "gateway_html_prefetch",
            cid = %root_cid,
            unixfs_path = %path
        );
        tokio::spawn(
            async move {
                let queued = Instant::now();
                let permit = match tokio::time::timeout(
                    GATEWAY_HTML_PREFETCH_QUEUE_TIMEOUT,
                    limiter.acquire_owned(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(err)) => {
                        tracing::info!(
                            phase = "gateway_html_prefetch_failed",
                            cid = %root_cid,
                            unixfs_path = %path,
                            error = %err,
                            elapsed_ms = queued.elapsed().as_millis()
                        );
                        return;
                    }
                    Err(_) => {
                        tracing::info!(
                            phase = "gateway_html_prefetch_skip",
                            cid = %root_cid,
                            unixfs_path = %path,
                            reason = "queue_timeout",
                            timeout_ms = GATEWAY_HTML_PREFETCH_QUEUE_TIMEOUT.as_millis(),
                            elapsed_ms = queued.elapsed().as_millis()
                        );
                        return;
                    }
                };
                let _permit = permit;
                let task_path = path.clone();
                let joined = tokio::task::spawn_blocking(move || {
                    html_prefetch_one(
                        provider.as_ref(),
                        &unixfs,
                        &small_body_cache,
                        root_cid,
                        &task_path,
                        config,
                    )
                })
                .await;
                if let Err(err) = joined {
                    tracing::info!(
                        phase = "gateway_html_prefetch_failed",
                        cid = %root_cid,
                        unixfs_path = %path,
                        error = %err
                    );
                }
            }
            .instrument(span),
        );
    }
}

fn maybe_spawn_html_range_warm(
    runtime: Option<&HtmlRangeWarmRuntime>,
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    small_body_cache: Arc<SmallBodyCache>,
    seed: HtmlRangeWarmSeed<'_>,
) {
    let Some(runtime) = runtime.filter(|runtime| runtime.is_enabled()) else {
        return;
    };
    let skip_reason = if seed.range_start != 0 {
        Some("non_prefix_range")
    } else if !seed.mime.starts_with("text/html") {
        Some("mime")
    } else if seed.total_len == 0 || seed.total_len > runtime.config.max_bytes() {
        Some("size")
    } else if seed.range_end >= seed.total_len.saturating_sub(1) {
        Some("already_complete")
    } else {
        None
    };
    if let Some(reason) = skip_reason {
        tracing::info!(
            phase = "gateway_html_range_warm_skip",
            cid = %seed.root_cid,
            file_cid = %seed.file_cid,
            unixfs_path = %seed.path,
            file_len = seed.total_len,
            range_start = seed.range_start,
            range_end = seed.range_end,
            max_bytes = runtime.config.max_bytes(),
            reason
        );
        return;
    }

    tracing::info!(
        phase = "gateway_html_range_warm_schedule",
        cid = %seed.root_cid,
        file_cid = %seed.file_cid,
        unixfs_path = %seed.path,
        file_len = seed.total_len,
        range_start = seed.range_start,
        range_end = seed.range_end,
        max_bytes = runtime.config.max_bytes(),
        concurrency = runtime.config.concurrency(),
    );

    let limiter = runtime.limiter.clone();
    let config = runtime.config;
    let root_cid = seed.root_cid;
    let file_cid = seed.file_cid;
    let path = seed.path.to_string();
    let span = tracing::info_span!(
        "gateway_html_range_warm",
        cid = %root_cid,
        file_cid = %file_cid,
        unixfs_path = %path
    );
    tokio::spawn(
        async move {
            let queued = Instant::now();
            let permit = match tokio::time::timeout(
                GATEWAY_HTML_RANGE_WARM_QUEUE_TIMEOUT,
                limiter.acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(err)) => {
                    tracing::info!(
                        phase = "gateway_html_range_warm_failed",
                        cid = %root_cid,
                        file_cid = %file_cid,
                        unixfs_path = %path,
                        error = %err,
                        elapsed_ms = queued.elapsed().as_millis()
                    );
                    return;
                }
                Err(_) => {
                    tracing::info!(
                        phase = "gateway_html_range_warm_skip",
                        cid = %root_cid,
                        file_cid = %file_cid,
                        unixfs_path = %path,
                        reason = "queue_timeout",
                        timeout_ms = GATEWAY_HTML_RANGE_WARM_QUEUE_TIMEOUT.as_millis(),
                        elapsed_ms = queued.elapsed().as_millis()
                    );
                    return;
                }
            };
            let _permit = permit;
            let task_path = path.clone();
            let joined = tokio::task::spawn_blocking(move || {
                html_range_warm_one(
                    provider.as_ref(),
                    &unixfs,
                    &small_body_cache,
                    root_cid,
                    file_cid,
                    &task_path,
                    config,
                )
            })
            .await;
            if let Err(err) = joined {
                tracing::info!(
                    phase = "gateway_html_range_warm_failed",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    error = %err
                );
            }
        }
        .instrument(span),
    );
}

fn html_range_warm_one(
    provider: &dyn BlockProvider,
    unixfs: &UnixfsResolver,
    small_body_cache: &SmallBodyCache,
    root_cid: Cid,
    file_cid: Cid,
    path: &str,
    config: GatewayHtmlRangeWarmConfig,
) {
    let started = Instant::now();
    let len = match unixfs.file_size_cid(provider, &file_cid) {
        Ok(len) => len,
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_range_warm_failed",
                cid = %root_cid,
                file_cid = %file_cid,
                unixfs_path = %path,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
    };
    if len == 0 || len > config.max_bytes() {
        tracing::info!(
            phase = "gateway_html_range_warm_skip",
            cid = %root_cid,
            file_cid = %file_cid,
            unixfs_path = %path,
            file_len = len,
            max_bytes = config.max_bytes(),
            reason = "size",
            elapsed_ms = started.elapsed().as_millis()
        );
        return;
    }

    let cache_key = SmallBodyCacheKey { file_cid, len };
    if small_body_cache.get(&cache_key).is_some() {
        tracing::info!(
            phase = "gateway_html_range_warm_skip",
            cid = %root_cid,
            file_cid = %file_cid,
            unixfs_path = %path,
            file_len = len,
            reason = "cache_hit",
            elapsed_ms = started.elapsed().as_millis()
        );
        return;
    }

    let end = len - 1;
    let bytes = match unixfs.read_file_cid_range(provider, &file_cid, 0, end) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_range_warm_failed",
                cid = %root_cid,
                file_cid = %file_cid,
                unixfs_path = %path,
                file_len = len,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
    };
    let body_len = bytes.len();
    let insert = small_body_cache.insert(cache_key, bytes);
    tracing::info!(
        phase = "gateway_html_range_warm_done",
        cid = %root_cid,
        file_cid = %file_cid,
        unixfs_path = %path,
        file_len = len,
        body_len,
        cache_inserted = insert.inserted,
        evicted = insert.evicted,
        cache_len = insert.cache_len,
        cache_bytes = insert.cache_bytes,
        elapsed_ms = started.elapsed().as_millis()
    );
}

fn html_directory_prefetch_one(
    provider: &dyn BlockProvider,
    unixfs: &UnixfsResolver,
    root_cid: Cid,
    path: &str,
) {
    let started = Instant::now();
    match unixfs.resolve_path(provider, &root_cid, path) {
        Ok(resolved) if matches!(resolved.kind, NodeKind::Directory | NodeKind::HamtShard) => {
            tracing::info!(
                phase = "gateway_html_directory_prefetch_done",
                cid = %root_cid,
                directory_cid = %resolved.cid,
                unixfs_path = %path,
                node_kind = node_kind_label(resolved.kind),
                elapsed_ms = started.elapsed().as_millis()
            );
        }
        Ok(resolved) => {
            tracing::info!(
                phase = "gateway_html_directory_prefetch_skip",
                cid = %root_cid,
                directory_cid = %resolved.cid,
                unixfs_path = %path,
                node_kind = node_kind_label(resolved.kind),
                reason = "not_directory",
                elapsed_ms = started.elapsed().as_millis()
            );
        }
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_directory_prefetch_failed",
                cid = %root_cid,
                unixfs_path = %path,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
        }
    }
}

fn html_prefetch_one(
    provider: &dyn BlockProvider,
    unixfs: &UnixfsResolver,
    small_body_cache: &SmallBodyCache,
    root_cid: Cid,
    path: &str,
    config: GatewayHtmlPrefetchConfig,
) {
    let started = Instant::now();
    let resolved = match unixfs.resolve_path(provider, &root_cid, path) {
        Ok(resolved) if matches!(resolved.kind, NodeKind::Raw | NodeKind::File) => resolved,
        Ok(_) => {
            tracing::info!(
                phase = "gateway_html_prefetch_skip",
                cid = %root_cid,
                unixfs_path = %path,
                reason = "not_file",
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_prefetch_failed",
                cid = %root_cid,
                unixfs_path = %path,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
    };
    let len = match unixfs.file_size_cid(provider, &resolved.cid) {
        Ok(len) => len,
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_prefetch_failed",
                cid = %root_cid,
                file_cid = %resolved.cid,
                unixfs_path = %path,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
    };
    if len == 0
        || len > config.max_bytes()
        || len as usize > GATEWAY_SMALL_BODY_CACHE_MAX_ENTRY_BYTES
    {
        tracing::info!(
            phase = "gateway_html_prefetch_skip",
            cid = %root_cid,
            file_cid = %resolved.cid,
            unixfs_path = %path,
            file_len = len,
            reason = "size",
            elapsed_ms = started.elapsed().as_millis()
        );
        return;
    }
    let cache_key = SmallBodyCacheKey {
        file_cid: resolved.cid,
        len,
    };
    if small_body_cache.get(&cache_key).is_some() {
        tracing::info!(
            phase = "gateway_html_prefetch_skip",
            cid = %root_cid,
            file_cid = %resolved.cid,
            unixfs_path = %path,
            file_len = len,
            reason = "cache_hit",
            elapsed_ms = started.elapsed().as_millis()
        );
        return;
    }
    let end = len - 1;
    let bytes = match unixfs.read_file_cid_range(provider, &resolved.cid, 0, end) {
        Ok(bytes) => Bytes::from(bytes),
        Err(err) => {
            tracing::info!(
                phase = "gateway_html_prefetch_failed",
                cid = %root_cid,
                file_cid = %resolved.cid,
                unixfs_path = %path,
                file_len = len,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return;
        }
    };
    let insert = small_body_cache.insert(cache_key, bytes);
    tracing::info!(
        phase = "gateway_html_prefetch_done",
        cid = %root_cid,
        file_cid = %resolved.cid,
        unixfs_path = %path,
        file_len = len,
        cache_inserted = insert.inserted,
        evicted = insert.evicted,
        cache_len = insert.cache_len,
        cache_bytes = insert.cache_bytes,
        elapsed_ms = started.elapsed().as_millis()
    );
}

fn html_prefetch_asset_paths(html: &str, base_path: &str, max_assets: usize) -> Vec<String> {
    if max_assets == 0 {
        return Vec::new();
    }
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    let base_path = html_prefetch_base_path(base_path);

    for tag in parse_html_tags(html) {
        for raw in html_prefetch_candidates(&tag) {
            if paths.len() >= max_assets {
                return paths;
            }
            let Some(path) = resolve_prefetch_asset_path(raw, &base_path) else {
                continue;
            };
            if seen.insert(path.clone()) {
                paths.push(path);
            }
        }
    }
    paths
}

fn html_prefetch_directory_paths(html: &str, base_path: &str, max_dirs: usize) -> Vec<String> {
    if max_dirs == 0 {
        return Vec::new();
    }
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    let base_path = html_prefetch_base_path(base_path);

    for tag in parse_html_tags(html) {
        for raw in html_prefetch_candidates(&tag) {
            if paths.len() >= max_dirs {
                return paths;
            }
            let Some(asset_path) = resolve_prefetch_asset_path(raw, &base_path) else {
                continue;
            };
            let dir_path = parent_path(&asset_path);
            if dir_path.is_empty() {
                continue;
            }
            if seen.insert(dir_path.to_string()) {
                paths.push(dir_path.to_string());
            }
        }
    }
    paths
}

fn html_prefetch_base_path(base_path: &str) -> String {
    let mut base_path = base_path.trim_start_matches('/').to_string();
    if base_path.is_empty() || base_path.ends_with('/') {
        base_path.push_str("index.html");
    }
    base_path
}

fn html_prefetch_candidates(tag: &ParsedTag) -> Vec<&str> {
    let mut candidates = Vec::new();
    match tag.name.as_str() {
        "script" => {
            if let Some(src) = tag.attr("src") {
                candidates.push(src);
            }
        }
        "link" => {
            let rel = tag.attr("rel").unwrap_or("").to_ascii_lowercase();
            if rel.contains("stylesheet")
                || rel.contains("modulepreload")
                || rel.contains("preload")
                || rel.contains("prefetch")
            {
                if let Some(href) = tag.attr("href") {
                    candidates.push(href);
                }
            }
        }
        "img" | "source" => {
            if let Some(src) = tag.attr("src") {
                candidates.push(src);
            }
            if let Some(srcset) = tag.attr("srcset") {
                candidates.extend(srcset.split(',').filter_map(|candidate| {
                    candidate
                        .split_ascii_whitespace()
                        .next()
                        .filter(|candidate| !candidate.is_empty())
                }));
            }
        }
        _ => {}
    }
    candidates
}

fn resolve_prefetch_asset_path(raw: &str, base_path: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty()
        || raw.starts_with('#')
        || raw.starts_with("//")
        || raw.contains("://")
        || raw.contains("${")
        || raw.contains("{{")
        || starts_with_scheme(raw, "data")
        || starts_with_scheme(raw, "blob")
        || starts_with_scheme(raw, "javascript")
        || starts_with_scheme(raw, "mailto")
        || starts_with_scheme(raw, "tel")
        || raw.starts_with("/ipfs/")
        || raw.starts_with("/ipns/")
    {
        return None;
    }
    let raw = raw.split(['?', '#']).next().unwrap_or(raw).trim();
    if raw.is_empty() {
        return None;
    }
    let candidate = if raw.starts_with('/') {
        raw.trim_start_matches('/').to_string()
    } else {
        append_path(parent_path(base_path), raw)
    };
    normalize_prefetch_path(&candidate)
}

fn parent_path(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn node_kind_label(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Raw => "raw",
        NodeKind::File => "file",
        NodeKind::Directory => "directory",
        NodeKind::HamtShard => "hamt_shard",
    }
}

fn normalize_prefetch_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        if segment == "." {
            continue;
        }
        if segment == ".." {
            return None;
        }
        if is_traversal_segment(segment) {
            return None;
        }
        parts.push(segment);
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

fn starts_with_scheme(value: &str, scheme: &str) -> bool {
    value
        .get(..scheme.len() + 1)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&format!("{scheme}:")))
}

#[derive(Debug, Clone)]
struct ParsedTag {
    name: String,
    attrs: Vec<(String, String)>,
}

impl ParsedTag {
    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }
}

fn parse_html_tags(html: &str) -> Vec<ParsedTag> {
    let mut tags = Vec::new();
    let lower = html.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(start) = html[offset..].find('<') {
        let start = offset + start + 1;
        let Some(end) = html[start..].find('>') else {
            break;
        };
        let raw = &html[start..start + end];
        if let Some(tag) = parse_html_tag(raw) {
            let raw_name = tag.name.clone();
            tags.push(tag);
            if matches!(raw_name.as_str(), "script" | "style") {
                let close_tag = format!("</{raw_name}");
                if let Some(close_start) = lower[start + end + 1..].find(&close_tag) {
                    let close_start = start + end + 1 + close_start;
                    if let Some(close_end) = lower[close_start..].find('>') {
                        offset = close_start + close_end + 1;
                        continue;
                    }
                }
            }
        }
        offset = start + end + 1;
    }
    tags
}

fn parse_html_tag(raw: &str) -> Option<ParsedTag> {
    let raw = raw.trim();
    if raw.is_empty()
        || raw.starts_with('/')
        || raw.starts_with('!')
        || raw.starts_with('?')
        || raw.starts_with("--")
    {
        return None;
    }

    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'/' {
        index += 1;
    }
    if index == 0 {
        return None;
    }
    let name = raw[..index].to_ascii_lowercase();
    let mut attrs = Vec::new();

    while index < bytes.len() {
        while index < bytes.len() && (bytes[index].is_ascii_whitespace() || bytes[index] == b'/') {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let name_start = index;
        while index < bytes.len()
            && !bytes[index].is_ascii_whitespace()
            && bytes[index] != b'='
            && bytes[index] != b'/'
        {
            index += 1;
        }
        if index == name_start {
            break;
        }
        let attr_name = raw[name_start..index].to_ascii_lowercase();
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let attr_value = if index < bytes.len() && bytes[index] == b'=' {
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index < bytes.len() && (bytes[index] == b'"' || bytes[index] == b'\'') {
                let quote = bytes[index];
                index += 1;
                let value_start = index;
                while index < bytes.len() && bytes[index] != quote {
                    index += 1;
                }
                let value = raw[value_start..index].to_string();
                if index < bytes.len() {
                    index += 1;
                }
                value
            } else {
                let value_start = index;
                while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                    index += 1;
                }
                raw[value_start..index].to_string()
            }
        } else {
            String::new()
        };
        attrs.push((attr_name, html_unescape_minimal(&attr_value)));
    }

    Some(ParsedTag { name, attrs })
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

enum ServedResource {
    File {
        path: String,
        cid: Cid,
        len: u64,
    },
    Directory {
        path: String,
        entries: Vec<DirectoryEntry>,
    },
}

fn resolve_served_path(
    unixfs: &UnixfsResolver,
    provider: &dyn BlockProvider,
    cid: &Cid,
    unixfs_path: &str,
    raw_link_tsize_fast_headers: bool,
) -> Result<(freedom_ipfs_unixfs::ResolvedNode, Option<u64>), GatewayError> {
    if raw_link_tsize_fast_headers {
        return unixfs
            .resolve_path_with_raw_link_tsize_hint(provider, cid, unixfs_path)
            .map_err(GatewayError::Unixfs);
    }

    unixfs
        .resolve_path(provider, cid, unixfs_path)
        .map(|resolved| (resolved, None))
        .map_err(GatewayError::Unixfs)
}

fn file_size_with_optional_hint(
    unixfs: &UnixfsResolver,
    provider: &dyn BlockProvider,
    cid: &Cid,
    size_hint: Option<u64>,
) -> Result<(u64, &'static str), GatewayError> {
    if let Some(size_hint) = size_hint {
        return Ok((size_hint, "raw_link_tsize"));
    }

    unixfs
        .file_size_cid(provider, cid)
        .map(|size| (size, "unixfs_file_size"))
        .map_err(GatewayError::Unixfs)
}

struct DirectoryListingPath {
    display: String,
    href_base: String,
}

impl DirectoryListingPath {
    fn ipfs(cid: &Cid, unixfs_path: &str) -> Self {
        let display = if unixfs_path.is_empty() {
            format!("/ipfs/{cid}")
        } else {
            format!("/ipfs/{cid}/{unixfs_path}")
        };
        let href_base = if unixfs_path.is_empty() {
            format!("/ipfs/{cid}")
        } else {
            format!("/ipfs/{cid}/{}", encode_gateway_path(unixfs_path))
        };
        Self { display, href_base }
    }

    fn ipns(path: &str) -> Self {
        Self {
            display: format!("/ipns/{path}"),
            href_base: format!("/ipns/{}", encode_gateway_path(path)),
        }
    }
}

fn served_resource(
    unixfs: &UnixfsResolver,
    provider: &dyn BlockProvider,
    cid: &Cid,
    unixfs_path: &str,
    raw_link_tsize_fast_headers: bool,
) -> Result<ServedResource, GatewayError> {
    let file_size_started = Instant::now();
    let (resolved, size_hint) = resolve_served_path(
        unixfs,
        provider,
        cid,
        unixfs_path,
        raw_link_tsize_fast_headers,
    )?;
    match resolved.kind {
        NodeKind::Raw | NodeKind::File => {
            let (len, size_source) =
                file_size_with_optional_hint(unixfs, provider, &resolved.cid, size_hint)?;
            tracing::info!(
                phase = "unixfs_file_size",
                cid = %cid,
                file_cid = %resolved.cid,
                unixfs_path,
                outcome = "file",
                file_len = len,
                size_source,
                elapsed_ms = file_size_started.elapsed().as_millis()
            );
            Ok(ServedResource::File {
                path: unixfs_path.to_string(),
                cid: resolved.cid,
                len,
            })
        }
        NodeKind::Directory | NodeKind::HamtShard => {
            tracing::info!(
                phase = "unixfs_file_size",
                cid = %cid,
                unixfs_path,
                outcome = "directory",
                elapsed_ms = file_size_started.elapsed().as_millis()
            );
            let index_path = append_path(unixfs_path, "index.html");
            let index_started = Instant::now();
            match resolve_served_path(
                unixfs,
                provider,
                cid,
                &index_path,
                raw_link_tsize_fast_headers,
            ) {
                Ok((index_resolved, index_size_hint))
                    if matches!(index_resolved.kind, NodeKind::Raw | NodeKind::File) =>
                {
                    let (len, size_source) = file_size_with_optional_hint(
                        unixfs,
                        provider,
                        &index_resolved.cid,
                        index_size_hint,
                    )?;
                    tracing::info!(
                        phase = "unixfs_index_lookup",
                        cid = %cid,
                        file_cid = %index_resolved.cid,
                        unixfs_path = %index_path,
                        outcome = "file",
                        file_len = len,
                        size_source,
                        elapsed_ms = index_started.elapsed().as_millis()
                    );
                    Ok(ServedResource::File {
                        path: index_path,
                        cid: index_resolved.cid,
                        len,
                    })
                }
                Ok(_) => Err(GatewayError::Unixfs(UnixfsError::IsDirectory)),
                Err(GatewayError::Unixfs(UnixfsError::PathNotFound(_))) => {
                    tracing::info!(
                        phase = "unixfs_index_lookup",
                        cid = %cid,
                        unixfs_path = %index_path,
                        outcome = "not_found",
                        elapsed_ms = index_started.elapsed().as_millis()
                    );
                    let list_started = Instant::now();
                    let entries = unixfs
                        .list_directory(provider, cid, unixfs_path)
                        .map_err(GatewayError::Unixfs)?;
                    tracing::info!(
                        phase = "unixfs_list_directory",
                        cid = %cid,
                        unixfs_path,
                        entry_count = entries.len(),
                        elapsed_ms = list_started.elapsed().as_millis()
                    );
                    Ok(ServedResource::Directory {
                        path: unixfs_path.to_string(),
                        entries,
                    })
                }
                Err(err) => Err(err),
            }
        }
    }
}

fn directory_listing_response(
    listing_path: &DirectoryListingPath,
    entries: &[DirectoryEntry],
) -> Result<Response, GatewayError> {
    let mut body = format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>Index of {title}</title><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><main><h1>Index of {title}</h1><ul>"#,
        title = escape_html(&listing_path.display)
    );
    for entry in entries {
        let href = append_path(
            &listing_path.href_base,
            &percent_encode_segment(&entry.name),
        );
        let size = entry
            .size
            .map(|size| format!(" <small>{size} bytes</small>"))
            .unwrap_or_default();
        body.push_str(&format!(
            r#"<li><a href="{href}">{name}</a>{size}</li>"#,
            href = escape_html(&href),
            name = escape_html(&entry.name),
            size = size,
        ));
    }
    body.push_str("</ul></main></body></html>");

    let len = body.len();
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string())
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    Ok(response)
}

fn split_ipfs_path(path: &str) -> Result<(Cid, &str), GatewayError> {
    let mut parts = path.splitn(2, '/');
    let cid = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| {
            GatewayError::BadRequest("expected /ipfs/{cid} or /ipfs/{cid}/{path}".into())
        })?;
    let cid = parse_cid(cid).map_err(|err| GatewayError::BadRequest(err.to_string()))?;
    let unixfs_path = parts.next().unwrap_or_default();
    validate_relative_gateway_path(unixfs_path)?;
    Ok((cid, unixfs_path))
}

async fn serve_ipns_path(
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    features: GatewayResponseFeatures,
    name_resolver: &dyn NameResolver,
    path: &str,
    request_headers: GatewayRequestHeaders<'_>,
) -> Result<Response, GatewayError> {
    let listing_path = DirectoryListingPath::ipns(path);
    let mut target = format!("/ipns/{path}");

    for _ in 0..4 {
        if let Some(ipfs) = target.strip_prefix("/ipfs/") {
            return serve_ipfs_path_with_listing_path(
                provider.clone(),
                unixfs.clone(),
                features.clone(),
                ipfs,
                request_headers,
                GatewayIpfsPathOptions {
                    listing_path: Some(&listing_path),
                    cache_policy: FileCachePolicy::RevalidateIpns,
                },
            )
            .await;
        }

        let Some(ipns) = target.strip_prefix("/ipns/") else {
            return Err(GatewayError::BadGateway(format!(
                "dnslink target is not /ipfs or /ipns: {target}"
            )));
        };
        let (name, rest) = split_name_path(ipns)?;
        let resolve_started = Instant::now();
        let resolved = match name_resolver.resolve_name(name).await {
            Ok(resolved) => {
                tracing::info!(
                    phase = "name_resolve",
                    name,
                    resolved_target = %resolved,
                    elapsed_ms = resolve_started.elapsed().as_millis(),
                    ok = true
                );
                resolved
            }
            Err(err) => {
                tracing::info!(
                    phase = "name_resolve",
                    name,
                    error = %err,
                    elapsed_ms = resolve_started.elapsed().as_millis(),
                    ok = false
                );
                return Err(if matches!(err, NamesysError::NotFound(_)) {
                    GatewayError::NotFound(format!("name not found: {name}"))
                } else {
                    GatewayError::BadGateway(format!("ipns/dnslink resolution failed: {err}"))
                });
            }
        };
        target = append_path(&resolved, rest);
    }

    Err(GatewayError::BadRequest(
        "dnslink recursion limit exceeded".into(),
    ))
}

fn split_name_path(path: &str) -> Result<(&str, &str), GatewayError> {
    let mut parts = path.splitn(2, '/');
    let name = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| {
            GatewayError::BadRequest("expected /ipns/{name} or /ipns/{name}/{path}".into())
        })?;
    reject_traversal_segment(name)?;
    let rest = parts.next().unwrap_or_default();
    validate_relative_gateway_path(rest)?;
    Ok((name, rest))
}

fn validate_relative_gateway_path(path: &str) -> Result<(), GatewayError> {
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        reject_traversal_segment(segment)?;
    }
    Ok(())
}

fn reject_traversal_segment(segment: &str) -> Result<(), GatewayError> {
    if is_traversal_segment(segment) {
        return Err(GatewayError::BadRequest(
            "path traversal segments are not allowed".into(),
        ));
    }
    Ok(())
}

fn is_traversal_segment(segment: &str) -> bool {
    matches!(segment, "." | "..")
        || percent_decode_ascii(segment)
            .as_deref()
            .is_some_and(|decoded| matches!(decoded, "." | ".."))
}

fn percent_decode_ascii(segment: &str) -> Option<String> {
    if !segment.as_bytes().contains(&b'%') {
        return None;
    }
    let mut decoded = Vec::with_capacity(segment.len());
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1)?;
            let low = *bytes.get(index + 2)?;
            decoded.push(hex_value(high)? << 4 | hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn append_path(base: &str, rest: &str) -> String {
    if rest.is_empty() {
        base.to_string()
    } else {
        format!(
            "{}/{}",
            base.trim_end_matches('/'),
            rest.trim_start_matches('/')
        )
    }
}

fn encode_gateway_path(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_segment(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            byte => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[derive(Clone, Copy)]
struct FileResponseHeaders<'a> {
    mime: &'a str,
    etag: &'a str,
    cache_policy: FileCachePolicy,
    is_head: bool,
}

struct FileResponseTarget {
    root_cid: Cid,
    file_cid: Cid,
    path: String,
    len: u64,
}

#[derive(Clone, Copy)]
struct GatewayStreamState {
    offset: u64,
    chunks: u64,
    started: Instant,
    prefetch_started: bool,
}

fn streaming_response(
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    features: GatewayResponseFeatures,
    target: FileResponseTarget,
    headers: FileResponseHeaders<'_>,
) -> Result<Response, GatewayError> {
    let small_body_cache = features.small_body_cache;
    let html_prefetch = features.html_prefetch;
    let html_directory_prefetch = features.html_directory_prefetch;
    let stream_small_bodies = features.stream_small_bodies;
    let FileResponseTarget {
        root_cid,
        file_cid,
        path,
        len,
    } = target;
    if !headers.is_head && !stream_small_bodies && len <= GATEWAY_STREAM_CHUNK_SIZE {
        let end = len.saturating_sub(1);
        let body = if len == 0 {
            Bytes::new()
        } else {
            let cache_key = SmallBodyCacheKey { file_cid, len };
            let cache_started = Instant::now();
            if let Some(bytes) = small_body_cache.get(&cache_key) {
                tracing::info!(
                    phase = "gateway_small_body_cache",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    cache_hit = true,
                    body_len = bytes.len(),
                    elapsed_ms = cache_started.elapsed().as_millis()
                );
                tracing::info!(
                    phase = "gateway_direct_body",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    range_start = 0u64,
                    range_end = end,
                    body_len = bytes.len(),
                    source = "small_body_cache",
                    elapsed_ms = cache_started.elapsed().as_millis()
                );
                bytes
            } else {
                tracing::info!(
                    phase = "gateway_small_body_cache",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    cache_hit = false,
                    elapsed_ms = cache_started.elapsed().as_millis()
                );
                let started = Instant::now();
                let bytes = unixfs
                    .read_file_cid_range(provider.as_ref(), &file_cid, 0, end)
                    .map(Bytes::from)
                    .map_err(GatewayError::Unixfs)?;
                tracing::info!(
                    phase = "gateway_direct_body",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    range_start = 0u64,
                    range_end = end,
                    body_len = bytes.len(),
                    source = "unixfs",
                    elapsed_ms = started.elapsed().as_millis()
                );
                let insert = small_body_cache.insert(cache_key, bytes.clone());
                tracing::info!(
                    phase = "gateway_small_body_cache",
                    cid = %root_cid,
                    file_cid = %file_cid,
                    unixfs_path = %path,
                    cache_inserted = insert.inserted,
                    evicted = insert.evicted,
                    cache_len = insert.cache_len,
                    cache_bytes = insert.cache_bytes
                );
                bytes
            }
        };
        maybe_spawn_html_directory_prefetch(
            html_directory_prefetch.as_ref(),
            provider.clone(),
            unixfs.clone(),
            HtmlPrefetchSeed {
                root_cid,
                base_path: &path,
                mime: headers.mime,
                body: &body,
            },
        );
        maybe_spawn_html_prefetch(
            html_prefetch.as_ref(),
            provider,
            unixfs,
            small_body_cache,
            HtmlPrefetchSeed {
                root_cid,
                base_path: &path,
                mime: headers.mime,
                body: &body,
            },
        );
        let mut response = Body::from(body).into_response();
        set_gateway_body_mode(&mut response, GatewayBodyMode::Direct);
        insert_full_file_headers(&mut response, len, headers)?;
        return Ok(response);
    }

    let provider = Arc::new(ScopedBlockProvider::new(provider));
    let span = tracing::Span::current();
    let stream_mime = headers.mime.to_string();
    let stream = stream::unfold(
        Some(GatewayStreamState {
            offset: 0,
            chunks: 0,
            started: Instant::now(),
            prefetch_started: false,
        }),
        move |state| {
            let provider = provider.clone();
            let unixfs = unixfs.clone();
            let small_body_cache = small_body_cache.clone();
            let html_prefetch = html_prefetch.clone();
            let html_directory_prefetch = html_directory_prefetch.clone();
            let path = path.clone();
            let span = span.clone();
            let stream_mime = stream_mime.clone();
            async move {
                let state = state?;
                let offset = state.offset;
                if offset >= len {
                    return None;
                }
                let last = len - 1;
                let end = offset
                    .saturating_add(GATEWAY_STREAM_CHUNK_SIZE - 1)
                    .min(last);
                let chunks = state.chunks.saturating_add(1);
                let next = if end == last {
                    None
                } else {
                    Some(GatewayStreamState {
                        offset: end.saturating_add(1),
                        chunks,
                        started: state.started,
                        prefetch_started: state.prefetch_started || offset == 0,
                    })
                };
                let chunk = unixfs
                    .read_file_cid_range(
                        provider.as_ref() as &dyn BlockProvider,
                        &file_cid,
                        offset,
                        end,
                    )
                    .map(Bytes::from)
                    .map_err(|err| io::Error::other(err.to_string()));
                if !state.prefetch_started {
                    if let Ok(bytes) = &chunk {
                        let prefetch_provider: Arc<dyn BlockProvider> = provider.clone();
                        maybe_spawn_html_directory_prefetch(
                            html_directory_prefetch.as_ref(),
                            prefetch_provider.clone(),
                            unixfs.clone(),
                            HtmlPrefetchSeed {
                                root_cid,
                                base_path: &path,
                                mime: &stream_mime,
                                body: bytes,
                            },
                        );
                        maybe_spawn_html_prefetch(
                            html_prefetch.as_ref(),
                            prefetch_provider,
                            unixfs.clone(),
                            small_body_cache,
                            HtmlPrefetchSeed {
                                root_cid,
                                base_path: &path,
                                mime: &stream_mime,
                                body: bytes,
                            },
                        );
                    }
                }
                match &chunk {
                    Ok(_) if end == last => {
                        tracing::info!(
                            phase = "gateway_stream_done",
                            cid = %root_cid,
                            file_cid = %file_cid,
                            unixfs_path = %path,
                            range_start = 0u64,
                            range_end = last,
                            body_len = len,
                            chunks,
                            elapsed_ms = state.started.elapsed().as_millis()
                        );
                    }
                    Err(err) => {
                        tracing::info!(
                            phase = "gateway_stream_failed",
                            cid = %root_cid,
                            file_cid = %file_cid,
                            unixfs_path = %path,
                            range_start = offset,
                            range_end = end,
                            body_len = len,
                            chunks = state.chunks,
                            error = %err,
                            elapsed_ms = state.started.elapsed().as_millis()
                        );
                    }
                    _ => {}
                }
                Some((chunk, next))
            }
            .instrument(span)
        },
    );

    let mut response = Body::from_stream(stream).into_response();
    set_gateway_body_mode(
        &mut response,
        if headers.is_head {
            GatewayBodyMode::Direct
        } else {
            GatewayBodyMode::Stream
        },
    );
    insert_full_file_headers(&mut response, len, headers)?;
    Ok(response)
}

fn insert_full_file_headers(
    response: &mut Response,
    len: u64,
    headers: FileResponseHeaders<'_>,
) -> Result<(), GatewayError> {
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(headers.mime)
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string())
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    insert_cache_headers(response, headers.etag, headers.cache_policy)?;
    Ok(())
}

struct ScopedBlockProvider {
    inner: Arc<dyn BlockProvider>,
    retained: Mutex<HashSet<Cid>>,
}

impl ScopedBlockProvider {
    fn new(inner: Arc<dyn BlockProvider>) -> Self {
        Self {
            inner,
            retained: Mutex::new(HashSet::new()),
        }
    }
}

impl BlockProvider for ScopedBlockProvider {
    fn get_block(&self, cid: &Cid) -> freedom_ipfs_core::Result<Option<freedom_ipfs_core::Block>> {
        let mut retained_here = false;
        {
            let mut retained = self.retained.lock().map_err(|err| {
                freedom_ipfs_core::CoreError::Storage(format!(
                    "stream retention lock poisoned: {err}"
                ))
            })?;
            if retained.insert(*cid) {
                self.inner.retain_block(cid)?;
                retained_here = true;
            }
        }

        match self.inner.get_block(cid)? {
            Some(block) => Ok(Some(block)),
            None => {
                if retained_here {
                    if let Ok(mut retained) = self.retained.lock() {
                        retained.remove(cid);
                    }
                    self.inner.release_block(cid);
                }
                Ok(None)
            }
        }
    }

    fn get_block_range(
        &self,
        cid: &Cid,
        start: u64,
        end: u64,
    ) -> freedom_ipfs_core::Result<Option<Vec<u8>>> {
        let mut retained_here = false;
        {
            let mut retained = self.retained.lock().map_err(|err| {
                freedom_ipfs_core::CoreError::Storage(format!(
                    "stream retention lock poisoned: {err}"
                ))
            })?;
            if retained.insert(*cid) {
                self.inner.retain_block(cid)?;
                retained_here = true;
            }
        }

        match self.inner.get_block_range(cid, start, end)? {
            Some(bytes) => Ok(Some(bytes)),
            None => {
                if retained_here {
                    if let Ok(mut retained) = self.retained.lock() {
                        retained.remove(cid);
                    }
                    self.inner.release_block(cid);
                }
                Ok(None)
            }
        }
    }
}

impl Drop for ScopedBlockProvider {
    fn drop(&mut self) {
        if let Ok(mut retained) = self.retained.lock() {
            for cid in retained.drain() {
                self.inner.release_block(&cid);
            }
        }
    }
}

fn ranged_response(
    provider: Arc<dyn BlockProvider>,
    unixfs: UnixfsResolver,
    features: GatewayResponseFeatures,
    target: FileResponseTarget,
    start: u64,
    end: u64,
    headers: FileResponseHeaders<'_>,
) -> Result<Response, GatewayError> {
    let FileResponseTarget {
        root_cid,
        file_cid,
        path,
        len: total_len,
    } = target;
    let range_len = end - start + 1;
    if !headers.is_head && range_len <= GATEWAY_STREAM_CHUNK_SIZE {
        let started = Instant::now();
        let body = unixfs
            .read_file_cid_range(provider.as_ref(), &file_cid, start, end)
            .map(Bytes::from)
            .map_err(GatewayError::Unixfs)?;
        tracing::info!(
            phase = "gateway_direct_body",
            cid = %root_cid,
            file_cid = %file_cid,
            unixfs_path = %path,
            range_start = start,
            range_end = end,
            body_len = body.len(),
            elapsed_ms = started.elapsed().as_millis()
        );
        maybe_spawn_html_range_warm(
            features.html_range_warm.as_ref(),
            provider,
            unixfs,
            features.small_body_cache,
            HtmlRangeWarmSeed {
                root_cid,
                file_cid,
                path: &path,
                mime: headers.mime,
                total_len,
                range_start: start,
                range_end: end,
            },
        );
        let mut response = Body::from(body).into_response();
        set_gateway_body_mode(&mut response, GatewayBodyMode::Direct);
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        insert_range_file_headers(&mut response, total_len, start, end, headers)?;
        return Ok(response);
    }

    let provider = Arc::new(ScopedBlockProvider::new(provider));
    let span = tracing::Span::current();
    let stream = stream::unfold(
        Some(GatewayStreamState {
            offset: start,
            chunks: 0,
            started: Instant::now(),
            prefetch_started: true,
        }),
        move |state| {
            let provider = provider.clone();
            let unixfs = unixfs.clone();
            let path = path.clone();
            let span = span.clone();
            async move {
                let state = state?;
                let offset = state.offset;
                if offset > end {
                    return None;
                }
                let chunk_end = offset
                    .saturating_add(GATEWAY_STREAM_CHUNK_SIZE - 1)
                    .min(end);
                let chunks = state.chunks.saturating_add(1);
                let next = if chunk_end == end {
                    None
                } else {
                    Some(GatewayStreamState {
                        offset: chunk_end.saturating_add(1),
                        chunks,
                        started: state.started,
                        prefetch_started: true,
                    })
                };
                let chunk = unixfs
                    .read_file_cid_range(
                        provider.as_ref() as &dyn BlockProvider,
                        &file_cid,
                        offset,
                        chunk_end,
                    )
                    .map(Bytes::from)
                    .map_err(|err| io::Error::other(err.to_string()));
                match &chunk {
                    Ok(_) if chunk_end == end => {
                        tracing::info!(
                            phase = "gateway_stream_done",
                            cid = %root_cid,
                            file_cid = %file_cid,
                            unixfs_path = %path,
                            range_start = start,
                            range_end = end,
                            body_len = range_len,
                            chunks,
                            elapsed_ms = state.started.elapsed().as_millis()
                        );
                    }
                    Err(err) => {
                        tracing::info!(
                            phase = "gateway_stream_failed",
                            cid = %root_cid,
                            file_cid = %file_cid,
                            unixfs_path = %path,
                            range_start = offset,
                            range_end = chunk_end,
                            body_len = range_len,
                            chunks = state.chunks,
                            error = %err,
                            elapsed_ms = state.started.elapsed().as_millis()
                        );
                    }
                    _ => {}
                }
                Some((chunk, next))
            }
            .instrument(span)
        },
    );

    let mut response = Body::from_stream(stream).into_response();
    set_gateway_body_mode(
        &mut response,
        if headers.is_head {
            GatewayBodyMode::Direct
        } else {
            GatewayBodyMode::Stream
        },
    );
    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    insert_range_file_headers(&mut response, total_len, start, end, headers)?;
    Ok(response)
}

fn parse_range_header(range: &HeaderValue, total_len: u64) -> Result<(u64, u64), GatewayError> {
    let range = range
        .to_str()
        .map_err(|_| GatewayError::BadRequest("invalid Range header".into()))?;
    let Some(spec) = range.strip_prefix("bytes=") else {
        return Err(GatewayError::BadRequest(
            "only bytes ranges are supported".into(),
        ));
    };
    parse_range_spec(spec, total_len)
}

fn insert_range_file_headers(
    response: &mut Response,
    total_len: u64,
    start: u64,
    end: u64,
    headers: FileResponseHeaders<'_>,
) -> Result<(), GatewayError> {
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_str(headers.mime)
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes {start}-{end}/{total_len}"))
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&(end - start + 1).to_string())
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    insert_cache_headers(response, headers.etag, headers.cache_policy)?;
    Ok(())
}

fn not_modified_response(
    etag: &str,
    cache_policy: FileCachePolicy,
) -> Result<Response, GatewayError> {
    let mut response = StatusCode::NOT_MODIFIED.into_response();
    insert_cache_headers(&mut response, etag, cache_policy)?;
    Ok(response)
}

fn insert_cache_headers(
    response: &mut Response,
    etag: &str,
    cache_policy: FileCachePolicy,
) -> Result<(), GatewayError> {
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(etag).map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static(cache_policy.header_value()),
    );
    Ok(())
}

fn file_etag(cid: &Cid, path: &str, len: u64) -> String {
    let path = if path.is_empty() {
        ".".to_string()
    } else {
        encode_gateway_path(path)
    };
    format!("\"fi1:{cid}:{path}:{len}\"")
}

fn if_none_match_matches(value: Option<&HeaderValue>, etag: &str) -> bool {
    let Some(value) = value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
    })
}

fn parse_range_spec(spec: &str, len: u64) -> Result<(u64, u64), GatewayError> {
    if len == 0 {
        return Err(GatewayError::RangeNotSatisfiable);
    }
    let (start, end) = spec
        .split_once('-')
        .ok_or_else(|| GatewayError::BadRequest("invalid range syntax".into()))?;

    if start.is_empty() {
        let suffix = end
            .parse::<u64>()
            .map_err(|_| GatewayError::BadRequest("invalid suffix range".into()))?;
        if suffix == 0 {
            return Err(GatewayError::RangeNotSatisfiable);
        }
        let start = len.saturating_sub(suffix);
        return Ok((start, len - 1));
    }

    let start = start
        .parse::<u64>()
        .map_err(|_| GatewayError::BadRequest("invalid range start".into()))?;
    let end = if end.is_empty() {
        len - 1
    } else {
        end.parse::<u64>()
            .map_err(|_| GatewayError::BadRequest("invalid range end".into()))?
    };

    if start >= len || start > end {
        return Err(GatewayError::RangeNotSatisfiable);
    }
    Ok((start, end.min(len - 1)))
}

#[derive(Debug)]
enum GatewayError {
    BadRequest(String),
    NotFound(String),
    Unixfs(UnixfsError),
    RangeNotSatisfiable,
    Busy,
    BadGateway(String),
    Internal(String),
}

fn gateway_error(err: GatewayError) -> Response {
    let (status, title, detail) = match err {
        GatewayError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "Bad Request", msg),
        GatewayError::NotFound(msg) => (StatusCode::NOT_FOUND, "Not Found", msg),
        GatewayError::Unixfs(UnixfsError::NotFound(_))
        | GatewayError::Unixfs(UnixfsError::PathNotFound(_)) => {
            (StatusCode::NOT_FOUND, "Not Found", "not found".into())
        }
        GatewayError::Unixfs(UnixfsError::IsDirectory) => (
            StatusCode::BAD_REQUEST,
            "Bad Request",
            "directory path could not be served".into(),
        ),
        GatewayError::Unixfs(err) if is_timeout_error(&err) => (
            StatusCode::GATEWAY_TIMEOUT,
            "Gateway Timeout",
            format!("retrieval timeout: {err}"),
        ),
        GatewayError::Unixfs(err) => (
            StatusCode::BAD_GATEWAY,
            "Bad Gateway",
            format!("unixfs error: {err}"),
        ),
        GatewayError::RangeNotSatisfiable => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            "Range Not Satisfiable",
            "range not satisfiable".into(),
        ),
        GatewayError::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Service Unavailable",
            "gateway busy".into(),
        ),
        GatewayError::BadGateway(msg) => (StatusCode::BAD_GATEWAY, "Bad Gateway", msg),
        GatewayError::Internal(msg) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Server Error",
            msg,
        ),
    };

    html_error_response(status, title, &detail)
}

fn html_error_response(status: StatusCode, title: &str, detail: &str) -> Response {
    let body = format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>{code} {title}</title><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><main><h1>{title}</h1><p>{detail}</p></main></body></html>"#,
        code = status.as_u16(),
        title = escape_html(title),
        detail = escape_html(detail)
    );
    let len = body.len();
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    if let Ok(value) = HeaderValue::from_str(&len.to_string()) {
        response.headers_mut().insert(CONTENT_LENGTH, value);
    }
    response
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn is_timeout_error(err: &UnixfsError) -> bool {
    let UnixfsError::Provider(message) = err else {
        return false;
    };
    let message = message.to_ascii_lowercase();
    message.contains("timed out") || message.contains("timeout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::header::{LAST_MODIFIED, LOCATION};
    use freedom_ipfs_core::{
        cid_from_data, Block, CoreError, Result as CoreResult, CODEC_DAG_PB, CODEC_RAW,
    };
    use freedom_ipfs_namesys::{NamesysError, Result as NamesysResult};
    use futures::StreamExt;
    use prost::Message;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn gateway_core_serves_cached_raw_block_without_tcp() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>core</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let core = GatewayCore::new(store);
        let response = core
            .handle(GatewayCoreRequest::ipfs(
                cid.to_string(),
                Method::GET,
                HeaderMap::new(),
            ))
            .await;
        let (status, headers, body) = collect_core_response(response).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(CONTENT_LENGTH).unwrap(),
            HeaderValue::from_static("17")
        );
        assert_eq!(body, Bytes::from_static(data));
    }

    #[tokio::test]
    async fn gateway_core_matches_http_adapter_for_browser_semantics() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let raw = b"0123456789";
        let raw_cid = cid_from_data(CODEC_RAW, raw);
        store.put_block(&raw_cid, raw).unwrap();
        let streamed_raw = (0..((GATEWAY_STREAM_CHUNK_SIZE * 2 + 17) as usize))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let streamed_raw_cid = cid_from_data(CODEC_RAW, &streamed_raw);
        store.put_block(&streamed_raw_cid, &streamed_raw).unwrap();

        let index = b"<!doctype html><title>core parity</title>";
        let index_block = test_pb_file(index);
        let index_cid = cid_from_data(CODEC_DAG_PB, &index_block);
        store.put_block(&index_cid, &index_block).unwrap();
        let plain = b"plain listing";
        let plain_cid = cid_from_data(CODEC_RAW, plain);
        store.put_block(&plain_cid, plain).unwrap();

        let index_dir_block = test_pb_directory(vec![test_link("index.html", &index_cid)]);
        let index_dir_cid = cid_from_data(CODEC_DAG_PB, &index_dir_block);
        store.put_block(&index_dir_cid, &index_dir_block).unwrap();
        let listing_dir_block = test_pb_directory(vec![test_link("plain.txt", &plain_cid)]);
        let listing_dir_cid = cid_from_data(CODEC_DAG_PB, &listing_dir_block);
        store
            .put_block(&listing_dir_cid, &listing_dir_block)
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store.clone()),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{index_dir_cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let core = GatewayCore::with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{index_dir_cid}"),
            }),
        );
        let client = reqwest::Client::new();
        let base_url = format!("http://{addr}");

        assert_core_http_parity(
            "ipfs file mime",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(
                format!("{index_dir_cid}/index.html"),
                Method::GET,
                HeaderMap::new(),
            ),
        )
        .await;
        assert_core_http_parity(
            "ipfs head",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(
                format!("{index_dir_cid}/index.html"),
                Method::HEAD,
                HeaderMap::new(),
            ),
        )
        .await;

        let mut range_headers = HeaderMap::new();
        range_headers.insert(RANGE, HeaderValue::from_static("bytes=2-5"));
        assert_core_http_parity(
            "ipfs range",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(raw_cid.to_string(), Method::GET, range_headers),
        )
        .await;
        assert_core_http_parity(
            "ipfs streamed full file",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(streamed_raw_cid.to_string(), Method::GET, HeaderMap::new()),
        )
        .await;

        let mut not_modified_headers = HeaderMap::new();
        not_modified_headers.insert(
            IF_NONE_MATCH,
            HeaderValue::from_str(&file_etag(&raw_cid, "", raw.len() as u64)).unwrap(),
        );
        assert_core_http_parity(
            "ipfs if-none-match",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(raw_cid.to_string(), Method::GET, not_modified_headers),
        )
        .await;

        let mut invalid_range_headers = HeaderMap::new();
        invalid_range_headers.insert(RANGE, HeaderValue::from_static("bytes=99-200"));
        assert_core_http_parity(
            "ipfs invalid range",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(raw_cid.to_string(), Method::GET, invalid_range_headers),
        )
        .await;

        assert_core_http_parity(
            "ipfs missing path",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(
                format!("{index_dir_cid}/missing.js"),
                Method::GET,
                HeaderMap::new(),
            ),
        )
        .await;
        assert_core_http_parity(
            "ipfs directory listing",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipfs(listing_dir_cid.to_string(), Method::GET, HeaderMap::new()),
        )
        .await;
        assert_core_http_parity(
            "ipns resolved file",
            &core,
            &client,
            &base_url,
            GatewayCoreRequest::ipns("example.com/index.html", Method::GET, HeaderMap::new()),
        )
        .await;
    }

    #[tokio::test]
    async fn gateway_core_streams_large_ranges_incrementally_and_stops_on_drop() {
        let len = (GATEWAY_STREAM_CHUNK_SIZE * 3) as usize;
        let data = (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let cid = cid_from_data(CODEC_RAW, &data);
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingProvider {
            cid,
            data,
            calls: calls.clone(),
        });
        let core = GatewayCore::with_provider(provider);
        let mut headers = HeaderMap::new();
        let range_end = GATEWAY_STREAM_CHUNK_SIZE * 2 + 9;
        headers.insert(
            RANGE,
            HeaderValue::from_str(&format!("bytes=0-{range_end}")).unwrap(),
        );

        let response = core
            .handle(GatewayCoreRequest::ipfs(
                cid.to_string(),
                Method::GET,
                headers,
            ))
            .await;

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(gateway_body_mode(&response), "stream");
        let calls_before_body = calls.load(Ordering::SeqCst);
        let mut body = response.into_body().into_data_stream();
        let first = body.next().await.unwrap().unwrap();
        assert_eq!(first.len() as u64, GATEWAY_STREAM_CHUNK_SIZE);
        let calls_after_first = calls.load(Ordering::SeqCst);
        assert!(
            calls_after_first <= calls_before_body + 2,
            "core should not eagerly read the entire response before the first chunk"
        );

        drop(body);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            calls_after_first,
            "dropping the native body stream should stop further range reads"
        );
    }

    #[tokio::test]
    async fn serves_cached_raw_block_through_gateway() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>offline</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(data));
    }

    #[tokio::test]
    async fn ipfs_file_responses_include_etag_and_support_not_modified() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>cache me</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/ipfs/{cid}");
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers().get(ETAG).unwrap().clone();
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            HeaderValue::from_static(CACHE_CONTROL_IPFS_FILE)
        );
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(data));

        let response = client
            .get(&url)
            .header(IF_NONE_MATCH, etag.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(ETAG), Some(&etag));
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            HeaderValue::from_static(CACHE_CONTROL_IPFS_FILE)
        );
        assert!(response.bytes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn range_requests_include_etag_but_ignore_if_none_match() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/ipfs/{cid}");
        let etag = file_etag(&cid, "", data.len() as u64);
        let response = client
            .get(url)
            .header(RANGE, "bytes=2-5")
            .header(IF_NONE_MATCH, &etag)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_static("bytes 2-5/10")
        );
        assert_eq!(
            response.headers().get(ETAG).unwrap(),
            HeaderValue::from_str(&etag).unwrap()
        );
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            HeaderValue::from_static(CACHE_CONTROL_IPFS_FILE)
        );
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(b"2345"));
    }

    #[tokio::test]
    async fn serves_directory_index_with_path_based_mime_type() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let index = b"<main>browser data plane</main>";
        let index_block = test_pb_file(index);
        let index_cid = cid_from_data(CODEC_DAG_PB, &index_block);
        store.put_block(&index_cid, &index_block).unwrap();

        let dir_block = test_pb_directory(vec![test_link("index.html", &index_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        store.put_block(&dir_cid, &dir_block).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{dir_cid}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html")
        );
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(index));
    }

    #[tokio::test]
    async fn gateway_reuses_unixfs_metadata_within_dagpb_response() {
        let data = b"<!doctype html><html>cached metadata</html>";
        let file_block = test_pb_file(data);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let dir_block = test_pb_directory(vec![test_link("index.html", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (file_cid, file_block),
        ])));
        let state = GatewayState::with_provider(provider.clone());

        let response = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/index.html")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), data);
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
    }

    #[tokio::test]
    async fn gateway_reuses_small_direct_body_cache_for_repeated_assets() {
        let data = b"console.log('small cached asset');";
        let file_block = test_pb_file(data);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let dir_block = test_pb_directory(vec![test_link("app.js", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (file_cid, file_block),
        ])));
        let state = GatewayState::with_provider(provider.clone());

        let first = ipfs_get(
            State(state.clone()),
            Path(format!("{dir_cid}/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(first_body.as_ref(), data);
        let dir_calls = provider.call_count(&dir_cid);
        let file_calls = provider.call_count(&file_cid);

        let second = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(second_body.as_ref(), data);
        assert_eq!(provider.call_count(&dir_cid), dir_calls);
        assert_eq!(provider.call_count(&file_cid), file_calls);
    }

    #[tokio::test]
    async fn gateway_can_stream_small_full_bodies_when_configured() {
        let data = b"console.log('small streamed asset');";
        let file_block = test_pb_file(data);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let dir_block = test_pb_directory(vec![test_link("app.js", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (file_cid, file_block),
        ])));
        let config = GatewayConfig::new(8).with_stream_small_bodies(true);
        let state = GatewayState::with_provider_config(provider.clone(), config);

        let response = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(gateway_body_mode(&response), "stream");
        assert_eq!(
            response
                .headers()
                .get(CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            data.len().to_string()
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), data);
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn top_level_html_prefetch_populates_small_body_cache() {
        let app = b"console.log('prefetched asset');";
        let app_block = test_pb_file(app);
        let app_cid = cid_from_data(CODEC_DAG_PB, &app_block);
        let html = b"<!doctype html><script src=\"/app.js\"></script>";
        let index_block = test_pb_file(html);
        let index_cid = cid_from_data(CODEC_DAG_PB, &index_block);
        let dir_block = test_pb_directory(vec![
            test_link("index.html", &index_cid),
            test_link("app.js", &app_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (index_cid, index_block),
            (app_cid, app_block),
        ])));
        let config = GatewayConfig::new(8).with_html_prefetch(GatewayHtmlPrefetchConfig::new(
            4,
            GATEWAY_STREAM_CHUNK_SIZE,
            2,
        ));
        let state = GatewayState::with_provider_config(provider.clone(), config);

        let root = ipfs_get(
            State(state.clone()),
            Path(dir_cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(root.status(), StatusCode::OK);
        let root_body = axum::body::to_bytes(root.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(root_body.as_ref(), html);

        for _ in 0..100 {
            if provider.call_count(&app_cid) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let prefetched_app_calls = provider.call_count(&app_cid);
        assert!(prefetched_app_calls > 0);

        let asset = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(asset.status(), StatusCode::OK);
        let asset_body = axum::body::to_bytes(asset.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(asset_body.as_ref(), app);
        assert_eq!(provider.call_count(&app_cid), prefetched_app_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn top_level_html_directory_prefetch_warms_parent_directory_only() {
        let app = b"console.log('directory parent warmed');";
        let app_cid = cid_from_data(CODEC_RAW, app);
        let assets_dir_block = test_pb_directory(vec![test_link("app.js", &app_cid)]);
        let assets_dir_cid = cid_from_data(CODEC_DAG_PB, &assets_dir_block);
        let html = b"<!doctype html><script src=\"/assets/app.js\"></script>";
        let index_block = test_pb_file(html);
        let index_cid = cid_from_data(CODEC_DAG_PB, &index_block);
        let dir_block = test_pb_directory(vec![
            test_link("index.html", &index_cid),
            test_link("assets", &assets_dir_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (index_cid, index_block),
            (assets_dir_cid, assets_dir_block),
            (app_cid, app.to_vec()),
        ])));
        let config = GatewayConfig::new(8).with_html_directory_prefetch(
            GatewayHtmlDirectoryPrefetchConfig::new(1, GATEWAY_STREAM_CHUNK_SIZE, 1),
        );
        let state = GatewayState::with_provider_config(provider.clone(), config);

        let root = ipfs_get(
            State(state.clone()),
            Path(dir_cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(root.status(), StatusCode::OK);
        let root_body = axum::body::to_bytes(root.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(root_body.as_ref(), html);

        for _ in 0..100 {
            if provider.call_count(&assets_dir_cid) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            provider.call_count(&assets_dir_cid) > 0,
            "enabled directory prefetch should warm the shared asset directory"
        );
        assert_eq!(
            provider.call_count(&app_cid),
            0,
            "directory prefetch should not read asset bodies"
        );

        let asset = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/assets/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(asset.status(), StatusCode::OK);
        let asset_body = axum::body::to_bytes(asset.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(asset_body.as_ref(), app);
        assert!(provider.call_count(&app_cid) > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streamed_top_level_html_prefetches_from_first_chunk() {
        let app = b"console.log('stream prefetched asset');";
        let app_block = test_pb_file(app);
        let app_cid = cid_from_data(CODEC_DAG_PB, &app_block);
        let mut html = b"<!doctype html><script src=\"/app.js\"></script>".to_vec();
        html.resize(GATEWAY_STREAM_CHUNK_SIZE as usize + 1024, b' ');
        let index_block = test_pb_file(&html);
        let index_cid = cid_from_data(CODEC_DAG_PB, &index_block);
        let dir_block = test_pb_directory(vec![
            test_link("index.html", &index_cid),
            test_link("app.js", &app_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (dir_cid, dir_block),
            (index_cid, index_block),
            (app_cid, app_block),
        ])));
        let config = GatewayConfig::new(8).with_html_prefetch(GatewayHtmlPrefetchConfig::new(
            4,
            GATEWAY_STREAM_CHUNK_SIZE,
            2,
        ));
        let state = GatewayState::with_provider_config(provider.clone(), config);

        let root = ipfs_get(
            State(state.clone()),
            Path(dir_cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(root.status(), StatusCode::OK);
        let root_body = axum::body::to_bytes(root.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(root_body.as_ref(), html.as_slice());

        for _ in 0..100 {
            if provider.call_count(&app_cid) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let prefetched_app_calls = provider.call_count(&app_cid);
        assert!(prefetched_app_calls > 0);

        let asset = ipfs_get(
            State(state),
            Path(format!("{dir_cid}/app.js")),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(asset.status(), StatusCode::OK);
        let asset_body = axum::body::to_bytes(asset.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(asset_body.as_ref(), app);
        assert_eq!(provider.call_count(&app_cid), prefetched_app_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn html_prefix_range_warm_is_disabled_by_default() {
        let first = b"<!doctype html><html><body>prefix</body>";
        let second = vec![b'x'; GATEWAY_STREAM_CHUNK_SIZE as usize + 1024];
        let first_cid = cid_from_data(CODEC_RAW, first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let file_block = test_pb_file_with_links(
            vec![test_link("", &first_cid), test_link("", &second_cid)],
            (first.len() + second.len()) as u64,
            vec![first.len() as u64, second.len() as u64],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (file_cid, file_block),
            (first_cid, first.to_vec()),
            (second_cid, second),
        ])));
        let state = GatewayState::with_provider(provider.clone());

        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=0-14"));
        let response = ipfs_get(
            State(state),
            Path(file_cid.to_string()),
            Method::GET,
            headers,
        )
        .await;

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"<!doctype html>");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            provider.call_count(&second_cid),
            0,
            "default range handling should not warm unread file blocks"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enabled_html_prefix_range_warm_reads_remaining_file_blocks() {
        let first = b"<!doctype html><html><body>prefix</body>";
        let second = vec![b'x'; GATEWAY_STREAM_CHUNK_SIZE as usize + 1024];
        let first_cid = cid_from_data(CODEC_RAW, first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let total_len = (first.len() + second.len()) as u64;
        let file_block = test_pb_file_with_links(
            vec![test_link("", &first_cid), test_link("", &second_cid)],
            total_len,
            vec![first.len() as u64, second.len() as u64],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (file_cid, file_block),
            (first_cid, first.to_vec()),
            (second_cid, second),
        ])));
        let config = GatewayConfig::new(8)
            .with_html_range_warm(GatewayHtmlRangeWarmConfig::new(total_len, 1));
        let state = GatewayState::with_provider_config(provider.clone(), config);

        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=0-14"));
        let response = ipfs_get(
            State(state),
            Path(file_cid.to_string()),
            Method::GET,
            headers,
        )
        .await;

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), b"<!doctype html>");
        for _ in 0..100 {
            if provider.call_count(&second_cid) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            provider.call_count(&second_cid) > 0,
            "enabled range warm should fetch blocks outside the requested prefix"
        );
    }

    #[test]
    fn small_body_cache_evicts_to_byte_budget() {
        let first = cid_from_data(CODEC_RAW, b"one");
        let second = cid_from_data(CODEC_RAW, b"two");
        let cache = SmallBodyCache::new(5);
        let first_key = SmallBodyCacheKey {
            file_cid: first,
            len: 3,
        };
        let second_key = SmallBodyCacheKey {
            file_cid: second,
            len: 3,
        };

        let first_insert = cache.insert(first_key.clone(), Bytes::from_static(b"one"));
        assert!(first_insert.inserted);
        assert_eq!(first_insert.evicted, 0);
        let second_insert = cache.insert(second_key.clone(), Bytes::from_static(b"two"));

        assert!(second_insert.inserted);
        assert_eq!(second_insert.evicted, 1);
        assert!(cache.get(&first_key).is_none());
        assert_eq!(cache.get(&second_key).unwrap(), Bytes::from_static(b"two"));
    }

    #[tokio::test]
    async fn serves_root_html_file_with_sniffed_mime_type() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let html = b"<!DOCTYPE html><html><head><title>DAICO</title></head><body>ok</body></html>";
        let file_block = test_pb_file(html);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        store.put_block(&file_cid, &file_block).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{file_cid}");
        let client = reqwest::Client::new();
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html")
        );
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(html));

        let response = client
            .get(&url)
            .header(RANGE, "bytes=0-14")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html")
        );
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_str(&format!("bytes 0-14/{}", html.len())).unwrap()
        );
        assert_eq!(
            response.bytes().await.unwrap(),
            Bytes::from_static(b"<!DOCTYPE html>")
        );
    }

    #[tokio::test]
    async fn serves_directory_listing_when_index_is_missing() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let plain = b"plain";
        let plain_cid = cid_from_data(CODEC_RAW, plain);
        store.put_block(&plain_cid, plain).unwrap();
        let spaced = b"spaced";
        let spaced_cid = cid_from_data(CODEC_RAW, spaced);
        store.put_block(&spaced_cid, spaced).unwrap();
        let escaped = b"escaped";
        let escaped_cid = cid_from_data(CODEC_RAW, escaped);
        store.put_block(&escaped_cid, escaped).unwrap();

        let dir_block = test_pb_directory(vec![
            test_link("space name #1.txt", &spaced_cid),
            test_link("plain.txt", &plain_cid),
            test_link("<bad>.txt", &escaped_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        store.put_block(&dir_cid, &dir_block).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{dir_cid}");
        let response = reqwest::get(url).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html; charset=utf-8")
        );
        let body = response.text().await.unwrap();
        assert!(body.contains(&format!("Index of /ipfs/{dir_cid}")));
        assert!(body.contains(&format!(r#"href="/ipfs/{dir_cid}/plain.txt""#)));
        assert!(body.contains(&format!(
            r#"href="/ipfs/{dir_cid}/space%20name%20%231.txt""#
        )));
        assert!(body.contains("&lt;bad&gt;.txt"));
        assert!(!body.contains("<bad>.txt"));

        let ranged = reqwest::Client::new()
            .get(format!("http://{addr}/ipfs/{dir_cid}"))
            .header(RANGE, "bytes=0-10")
            .send()
            .await
            .unwrap();
        assert_eq!(ranged.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn raw_link_tsize_fast_headers_skip_pre_response_raw_leaf_fetch() {
        let leaf_data = vec![b'a'; (GATEWAY_STREAM_CHUNK_SIZE + 17) as usize];
        let leaf_cid = cid_from_data(CODEC_RAW, &leaf_data);
        let dir_data = test_pb_directory(vec![TestPbLink {
            tsize: Some(leaf_data.len() as u64),
            ..test_link("app.js", &leaf_cid)
        }]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        let provider =
            MultiCountingProvider::new(HashMap::from([(leaf_cid, leaf_data), (dir_cid, dir_data)]));
        let unixfs = UnixfsResolver::with_metadata_cache_capacity(8);

        let resource = served_resource(&unixfs, &provider, &dir_cid, "app.js", true).unwrap();

        match resource {
            ServedResource::File { cid, len, .. } => {
                assert_eq!(cid, leaf_cid);
                assert_eq!(len, GATEWAY_STREAM_CHUNK_SIZE + 17);
            }
            ServedResource::Directory { .. } => panic!("expected file resource"),
        }
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&leaf_cid), 0);
    }

    #[test]
    fn raw_link_tsize_fast_headers_stay_opt_in() {
        let leaf_data = vec![b'a'; (GATEWAY_STREAM_CHUNK_SIZE + 17) as usize];
        let leaf_cid = cid_from_data(CODEC_RAW, &leaf_data);
        let dir_data = test_pb_directory(vec![TestPbLink {
            tsize: Some(leaf_data.len() as u64),
            ..test_link("app.js", &leaf_cid)
        }]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        let provider =
            MultiCountingProvider::new(HashMap::from([(leaf_cid, leaf_data), (dir_cid, dir_data)]));
        let unixfs = UnixfsResolver::with_metadata_cache_capacity(8);

        let resource = served_resource(&unixfs, &provider, &dir_cid, "app.js", false).unwrap();

        assert!(matches!(resource, ServedResource::File { .. }));
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&leaf_cid), 2);
    }

    #[tokio::test]
    async fn resolves_percent_encoded_browser_paths() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"encoded browser path";
        let file_block = test_pb_file(data);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        store.put_block(&file_cid, &file_block).unwrap();

        let dir_block = test_pb_directory(vec![test_link("space name #1.txt", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        store.put_block(&dir_cid, &dir_block).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{dir_cid}/space%20name%20%231.txt");
        let response = reqwest::get(url).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(data));
    }

    #[tokio::test]
    async fn supports_byte_ranges() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let client = reqwest::Client::new();
        for (range, expected_range, expected) in [
            ("bytes=2-5", "bytes 2-5/10", b"2345".as_slice()),
            ("bytes=7-", "bytes 7-9/10", b"789".as_slice()),
            ("bytes=-3", "bytes 7-9/10", b"789".as_slice()),
        ] {
            let response = client.get(&url).header(RANGE, range).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT, "{range}");
            assert_eq!(
                response.headers().get(CONTENT_RANGE).unwrap(),
                HeaderValue::from_static(expected_range),
                "{range}"
            );
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                expected,
                "{range}"
            );
        }
    }

    #[test]
    fn mime_fallback_source_distinguishes_sniff_status() {
        assert_eq!(mime_fallback_source(0, false), "fallback_empty");
        assert_eq!(mime_fallback_source(10, true), "fallback_after_sniff");
        assert_eq!(mime_fallback_source(10, false), "fallback_no_sniff");
    }

    #[test]
    fn html_prefetch_asset_paths_extracts_same_root_subresources() {
        let html = r#"
            <!doctype html>
            <base href="https://example.invalid/ignored/">
            <link rel="stylesheet" href="/assets/app.css?x=1">
            <link rel="modulepreload" href="chunks/app.js">
            <script src="./main.js"></script>
            <img srcset="hero.avif 1x, hero@2x.avif 2x">
            <script>var ignored = "<img src='/late.png'>";</script>
            <script src="https://cdn.example/app.js"></script>
            <img src="data:image/png;base64,aaa">
        "#;

        assert_eq!(
            html_prefetch_asset_paths(html, "/pages/index.html", 8),
            vec![
                "assets/app.css",
                "pages/chunks/app.js",
                "pages/main.js",
                "pages/hero.avif",
                "pages/hero@2x.avif",
            ]
        );
    }

    #[tokio::test]
    async fn deep_byte_ranges_skip_mime_sniff_prefix_read() {
        let first = b"<!DOCTYPE html><html><body>prefix block</body></html>";
        let second = b"range payload";
        let first_cid = cid_from_data(CODEC_RAW, first);
        let second_cid = cid_from_data(CODEC_RAW, second);
        let file_block = test_pb_file_with_links(
            vec![test_link("", &first_cid), test_link("", &second_cid)],
            (first.len() + second.len()) as u64,
            vec![first.len() as u64, second.len() as u64],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_block);
        let provider = Arc::new(MultiCountingProvider::new(HashMap::from([
            (file_cid, file_block),
            (first_cid, first.to_vec()),
            (second_cid, second.to_vec()),
        ])));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider(provider.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let start = first.len();
        let end = start + second.len() - 1;
        let response = reqwest::Client::new()
            .get(format!("http://{addr}/ipfs/{file_cid}"))
            .header(RANGE, format!("bytes={start}-{end}"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("application/octet-stream")
        );
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_str(&format!(
                "bytes {start}-{end}/{}",
                first.len() + second.len()
            ))
            .unwrap()
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), second);
        assert_eq!(
            provider.call_count(&first_cid),
            0,
            "deep range should not fetch byte 0 only for MIME sniffing"
        );
        assert_eq!(provider.call_count(&second_cid), 1);
    }

    #[tokio::test]
    async fn streams_large_byte_ranges_across_chunks() {
        let len = (GATEWAY_STREAM_CHUNK_SIZE * 3) as usize;
        let data = (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let cid = cid_from_data(CODEC_RAW, &data);
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingProvider {
            cid,
            data: data.clone(),
            calls: calls.clone(),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider(provider);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let range_end = GATEWAY_STREAM_CHUNK_SIZE * 2 + 9;
        let url = format!("http://{addr}/ipfs/{cid}");
        let response = reqwest::Client::new()
            .get(url)
            .header(RANGE, format!("bytes=0-{range_end}"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_str(&format!("bytes 0-{range_end}/{len}")).unwrap()
        );
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            &data[..=range_end as usize]
        );
        assert!(
            calls.load(Ordering::SeqCst) > 3,
            "large range should be read in multiple chunks"
        );
    }

    #[tokio::test]
    async fn supports_head_requests_without_response_bodies() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let client = reqwest::Client::new();

        let response = client.head(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_LENGTH).unwrap(),
            HeaderValue::from_static("10")
        );
        assert_eq!(
            response.headers().get(ACCEPT_RANGES).unwrap(),
            HeaderValue::from_static("bytes")
        );
        assert!(response.bytes().await.unwrap().is_empty());

        let response = client
            .head(&url)
            .header(RANGE, "bytes=2-5")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_static("bytes 2-5/10")
        );
        assert_eq!(
            response.headers().get(CONTENT_LENGTH).unwrap(),
            HeaderValue::from_static("4")
        );
        assert!(response.bytes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_invalid_and_unsatisfiable_byte_ranges() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let client = reqwest::Client::new();
        for (range, status) in [
            ("items=2-5", StatusCode::BAD_REQUEST),
            ("bytes=abc-5", StatusCode::BAD_REQUEST),
            ("bytes=8-2", StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=20-30", StatusCode::RANGE_NOT_SATISFIABLE),
            ("bytes=-0", StatusCode::RANGE_NOT_SATISFIABLE),
        ] {
            let response = client.get(&url).header(RANGE, range).send().await.unwrap();
            assert_eq!(response.status(), status, "{range}");
        }
    }

    #[tokio::test]
    async fn returns_browser_facing_html_error_pages() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"do not traverse";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}/%2e%2e/index.html");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html; charset=utf-8")
        );
        let body = response.text().await.unwrap();
        assert!(body.contains("<!doctype html>"));
        assert!(body.contains("<h1>Bad Request</h1>"));
        assert!(body.contains("<p>"));
    }

    #[tokio::test]
    async fn html_error_pages_escape_details() {
        let response = gateway_error(GatewayError::BadRequest(
            r#"<script>alert("cid")</script> & bad"#.into(),
        ));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("&lt;script&gt;alert(&quot;cid&quot;)&lt;/script&gt; &amp; bad"));
        assert!(!body.contains("<script>"));
    }

    #[tokio::test]
    async fn streams_full_response_across_chunks() {
        let data = (0..(GATEWAY_STREAM_CHUNK_SIZE as usize + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let cid = cid_from_data(CODEC_RAW, &data);
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingProvider {
            cid,
            data: data.clone(),
            calls: calls.clone(),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider(provider);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_LENGTH)
                .unwrap()
                .to_str()
                .unwrap(),
            data.len().to_string()
        );
        assert_eq!(response.bytes().await.unwrap(), Bytes::from(data));
        assert!(
            calls.load(Ordering::SeqCst) > 2,
            "large response should be read in multiple chunks"
        );
    }

    #[tokio::test]
    async fn maps_provider_timeouts_to_gateway_timeout() {
        let data = b"timeout target";
        let cid = cid_from_data(CODEC_RAW, data);
        let provider = Arc::new(TimeoutProvider);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider(provider);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipfs/{cid}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn default_router_does_not_resolve_names_online() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/example.com");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn does_not_expose_kubo_rpc_or_webui_routes() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(store);
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        for path in ["/api/v0/version", "/api/v0/id", "/api/v0/refs", "/webui"] {
            let get = client
                .get(format!("http://{addr}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(get.status(), StatusCode::NOT_FOUND, "GET {path}");
            assert_ne!(
                get.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json")),
                "GET {path}"
            );

            let post = client
                .post(format!("http://{addr}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(post.status(), StatusCode::NOT_FOUND, "POST {path}");
            assert_ne!(
                post.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json")),
                "POST {path}"
            );
        }
    }

    #[tokio::test]
    async fn resolves_ipns_path_through_name_resolver() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>ipns</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "k51fixture".to_string(),
                target: format!("/ipfs/{cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/k51fixture");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(data));
    }

    #[tokio::test]
    async fn ipns_file_responses_use_revalidation_cache_policy() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>ipns cache</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/ipns/example.com");
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response.headers().get(ETAG).unwrap().clone();
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            HeaderValue::from_static(CACHE_CONTROL_IPNS_FILE)
        );

        let response = client
            .get(&url)
            .header(IF_NONE_MATCH, format!("W/{}", etag.to_str().unwrap()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(ETAG), Some(&etag));
        assert_eq!(
            response.headers().get(CACHE_CONTROL).unwrap(),
            HeaderValue::from_static(CACHE_CONTROL_IPNS_FILE)
        );
    }

    #[tokio::test]
    async fn persistent_name_resolver_stores_successful_resolution() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let resolver = PersistentNameResolver::new(
            StaticNameResolver {
                name: "example.com".to_string(),
                target: "/ipfs/bafyroot".to_string(),
            },
            store.clone(),
        );

        assert_eq!(
            resolver.resolve_name("example.com").await.unwrap(),
            "/ipfs/bafyroot"
        );
        assert_eq!(
            store.get_name_record("example.com").unwrap().as_deref(),
            Some("/ipfs/bafyroot")
        );
    }

    #[tokio::test]
    async fn offline_router_resolves_ipns_from_persistent_name_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"<html>offline ipns</html>";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store
            .put_name_record(
                "example.com",
                &format!("/ipfs/{cid}"),
                Duration::from_secs(60),
            )
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store.clone()),
            Arc::new(PersistentNameResolver::cache_only(store)),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/example.com");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap(), Bytes::from_static(data));
    }

    #[tokio::test]
    async fn serves_directory_listing_through_ipns_resolution() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let plain = b"ipns linked file";
        let plain_cid = cid_from_data(CODEC_RAW, plain);
        store.put_block(&plain_cid, plain).unwrap();
        let spaced = b"ipns linked file with escaped path";
        let spaced_cid = cid_from_data(CODEC_RAW, spaced);
        store.put_block(&spaced_cid, spaced).unwrap();

        let dir_block = test_pb_directory(vec![
            test_link("plain.txt", &plain_cid),
            test_link("space name #1.txt", &spaced_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_block);
        store.put_block(&dir_cid, &dir_block).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{dir_cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/example.com");
        let client = reqwest::Client::new();

        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html; charset=utf-8")
        );
        let body = response.text().await.unwrap();
        assert!(body.contains("Index of /ipns/example.com"));
        assert!(body.contains(r#"href="/ipns/example.com/plain.txt""#));
        assert!(body.contains(r#"href="/ipns/example.com/space%20name%20%231.txt""#));

        let response = client.head(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            HeaderValue::from_static("text/html; charset=utf-8")
        );
        assert!(response.bytes().await.unwrap().is_empty());

        let response = client
            .get(&url)
            .header(RANGE, "bytes=0-10")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn supports_head_requests_through_ipns_resolution() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/example.com");
        let client = reqwest::Client::new();

        let response = client.head(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_LENGTH).unwrap(),
            HeaderValue::from_static("10")
        );
        assert_eq!(
            response.headers().get(ACCEPT_RANGES).unwrap(),
            HeaderValue::from_static("bytes")
        );
        assert!(response.bytes().await.unwrap().is_empty());

        let response = client
            .head(&url)
            .header(RANGE, "bytes=2-5")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            HeaderValue::from_static("bytes 2-5/10")
        );
        assert_eq!(
            response.headers().get(CONTENT_LENGTH).unwrap(),
            HeaderValue::from_static("4")
        );
        assert!(response.bytes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rejects_ipns_resolution_loops() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "loop.example".to_string(),
                target: "/ipns/loop.example".to_string(),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/ipns/loop.example");
        let response = reqwest::get(url).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(response
            .text()
            .await
            .unwrap()
            .contains("recursion limit exceeded"));
    }

    #[tokio::test]
    async fn rejects_path_traversal_segments() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"do not traverse";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router_with_provider_and_name_resolver(
            Arc::new(store),
            Arc::new(StaticNameResolver {
                name: "example.com".to_string(),
                target: format!("/ipfs/{cid}"),
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        assert!(split_ipfs_path(&format!("{cid}/../index.html")).is_err());
        assert!(split_ipfs_path(&format!("{cid}/./index.html")).is_err());
        assert!(split_ipfs_path(&format!("{cid}/%2e%2e/index.html")).is_err());
        assert!(split_ipfs_path(&format!("{cid}/%2e/index.html")).is_err());
        assert!(split_name_path("../index.html").is_err());
        assert!(split_name_path("example.com/%2e%2e/index.html").is_err());

        for path in [format!("/ipfs/{cid}/%2e%2e/index.html")] {
            let url = format!("http://{addr}{path}");
            let response = reqwest::get(url).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queues_requests_above_concurrency_limit() {
        let data = b"limited gateway";
        let cid = cid_from_data(CODEC_RAW, data);
        let entered = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(SlowProvider {
            cid,
            data: data.to_vec(),
            entered: entered.clone(),
            delay: Duration::from_millis(300),
        });

        let state = GatewayState::with_provider_config(provider, GatewayConfig::new(1));
        let first = tokio::spawn(ipfs_get(
            State(state.clone()),
            Path(cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        ));

        for _ in 0..50 {
            if entered.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(entered.load(Ordering::SeqCst));

        let second = ipfs_get(
            State(state),
            Path(cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);

        let first = first.await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn times_out_requests_waiting_too_long_for_concurrency_permit() {
        let data = b"limited gateway";
        let cid = cid_from_data(CODEC_RAW, data);
        let entered = Arc::new(AtomicBool::new(false));
        let provider = Arc::new(SlowProvider {
            cid,
            data: data.to_vec(),
            entered: entered.clone(),
            delay: GATEWAY_REQUEST_QUEUE_TIMEOUT + Duration::from_millis(250),
        });

        let state = GatewayState::with_provider_config(provider, GatewayConfig::new(1));
        let first = tokio::spawn(ipfs_get(
            State(state.clone()),
            Path(cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        ));

        for _ in 0..50 {
            if entered.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(entered.load(Ordering::SeqCst));

        let second = ipfs_get(
            State(state),
            Path(cid.to_string()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

        let first = first.await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
    }

    async fn collect_core_response(response: Response) -> (StatusCode, HeaderMap, Bytes) {
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, headers, body)
    }

    async fn collect_http_response(
        client: &reqwest::Client,
        base_url: &str,
        request: &GatewayCoreRequest,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let url = format!(
            "{}{}",
            base_url.trim_end_matches('/'),
            request.request_path()
        );
        let mut builder = client.request(request.method.clone(), url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let response = builder.send().await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        (status, headers, body)
    }

    async fn assert_core_http_parity(
        label: &str,
        core: &GatewayCore,
        client: &reqwest::Client,
        base_url: &str,
        request: GatewayCoreRequest,
    ) {
        let (core_status, core_headers, core_body) =
            collect_core_response(core.handle(request.clone()).await).await;
        let (http_status, http_headers, http_body) =
            collect_http_response(client, base_url, &request).await;

        assert_eq!(core_status, http_status, "{label}: status");
        for header in [
            CONTENT_TYPE.as_str(),
            CONTENT_LENGTH.as_str(),
            ACCEPT_RANGES.as_str(),
            CONTENT_RANGE.as_str(),
            ETAG.as_str(),
            CACHE_CONTROL.as_str(),
            LAST_MODIFIED.as_str(),
            LOCATION.as_str(),
        ] {
            assert_eq!(
                core_headers.get(header),
                http_headers.get(header),
                "{label}: header {header}"
            );
        }
        assert_eq!(core_body, http_body, "{label}: body");
    }

    struct StaticNameResolver {
        name: String,
        target: String,
    }

    #[async_trait::async_trait]
    impl NameResolver for StaticNameResolver {
        async fn resolve_name(&self, name: &str) -> NamesysResult<String> {
            if name == self.name {
                Ok(self.target.clone())
            } else {
                Err(NamesysError::NotFound(name.to_string()))
            }
        }
    }

    struct SlowProvider {
        cid: Cid,
        data: Vec<u8>,
        entered: Arc<AtomicBool>,
        delay: Duration,
    }

    impl BlockProvider for SlowProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            if cid != &self.cid {
                return Err(CoreError::Storage("unexpected cid".into()));
            }
            self.entered.store(true, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            Ok(Some(Block::unchecked(*cid, self.data.clone())))
        }
    }

    struct CountingProvider {
        cid: Cid,
        data: Vec<u8>,
        calls: Arc<AtomicUsize>,
    }

    impl BlockProvider for CountingProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            if cid != &self.cid {
                return Ok(None);
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Block::unchecked(*cid, self.data.clone())))
        }
    }

    struct MultiCountingProvider {
        blocks: HashMap<Cid, Vec<u8>>,
        calls: std::sync::Mutex<HashMap<Cid, usize>>,
    }

    impl MultiCountingProvider {
        fn new(blocks: HashMap<Cid, Vec<u8>>) -> Self {
            Self {
                blocks,
                calls: std::sync::Mutex::new(HashMap::new()),
            }
        }

        fn call_count(&self, cid: &Cid) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(cid)
                .unwrap_or(&0)
        }
    }

    impl BlockProvider for MultiCountingProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            if let Some(data) = self.blocks.get(cid) {
                let mut calls = self
                    .calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls.entry(*cid).or_default() += 1;
                return Ok(Some(Block::unchecked(*cid, data.clone())));
            }
            Ok(None)
        }
    }

    struct TimeoutProvider;

    impl BlockProvider for TimeoutProvider {
        fn get_block(&self, _cid: &Cid) -> CoreResult<Option<Block>> {
            Err(CoreError::Storage("bitswap request timed out".into()))
        }
    }

    #[derive(Clone, PartialEq, Message)]
    struct TestPbNode {
        #[prost(bytes = "vec", optional, tag = "1")]
        data: Option<Vec<u8>>,
        #[prost(message, repeated, tag = "2")]
        links: Vec<TestPbLink>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct TestPbLink {
        #[prost(bytes = "vec", optional, tag = "1")]
        hash: Option<Vec<u8>>,
        #[prost(string, optional, tag = "2")]
        name: Option<String>,
        #[prost(uint64, optional, tag = "3")]
        tsize: Option<u64>,
    }

    #[derive(Clone, PartialEq, Message)]
    struct TestUnixfsData {
        #[prost(enumeration = "TestDataType", optional, tag = "1")]
        r#type: Option<i32>,
        #[prost(bytes = "vec", optional, tag = "2")]
        data: Option<Vec<u8>>,
        #[prost(uint64, optional, tag = "3")]
        filesize: Option<u64>,
        #[prost(uint64, repeated, tag = "4")]
        blocksizes: Vec<u64>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    enum TestDataType {
        Directory = 1,
        File = 2,
    }

    fn test_pb_file(data: &[u8]) -> Vec<u8> {
        TestPbNode {
            data: Some(
                TestUnixfsData {
                    r#type: Some(TestDataType::File as i32),
                    data: Some(data.to_vec()),
                    filesize: Some(data.len() as u64),
                    blocksizes: Vec::new(),
                }
                .encode_to_vec(),
            ),
            links: Vec::new(),
        }
        .encode_to_vec()
    }

    fn test_pb_file_with_links(
        links: Vec<TestPbLink>,
        filesize: u64,
        blocksizes: Vec<u64>,
    ) -> Vec<u8> {
        TestPbNode {
            data: Some(
                TestUnixfsData {
                    r#type: Some(TestDataType::File as i32),
                    data: Some(Vec::new()),
                    filesize: Some(filesize),
                    blocksizes,
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    fn test_pb_directory(links: Vec<TestPbLink>) -> Vec<u8> {
        TestPbNode {
            data: Some(
                TestUnixfsData {
                    r#type: Some(TestDataType::Directory as i32),
                    data: Some(Vec::new()),
                    filesize: Some(0),
                    blocksizes: Vec::new(),
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    fn test_link(name: &str, cid: &Cid) -> TestPbLink {
        TestPbLink {
            hash: Some(cid.to_bytes()),
            name: Some(name.to_string()),
            tsize: None,
        }
    }
}
