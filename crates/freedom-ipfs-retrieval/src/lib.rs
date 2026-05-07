use cid::Cid;
use freedom_ipfs_core::{
    block_data_range, verify_block, Block, BlockProvider, CoreError, Result as CoreResult,
    CODEC_DAG_PB, DEFAULT_MAX_BLOCK_SIZE, HASH_IDENTITY, HASH_SHA2_256,
};
use freedom_ipfs_namesys::{CloudflareDohResolver, DnsTxtResolver};
use freedom_ipfs_routing::{Provider, ProviderRoutingClient};
use freedom_ipfs_store::{CachedProviderRecord, SqliteBlockStore};
use futures::future::{join_all, BoxFuture, FutureExt, Shared};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::stream::{select_all, FuturesUnordered};
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::{NetworkBehaviour, Stream as Libp2pStream, SwarmEvent};
use libp2p::StreamProtocol;
use libp2p::{
    connection_limits, identify, noise, ping, tcp, tls, websocket, yamux, Multiaddr, PeerId,
    SwarmBuilder, Transport,
};
use libp2p_stream::{Control as StreamControl, IncomingStreams};
use multihash::Multihash;
use multihash_codetable::{Code, MultihashDigest};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error as StdError;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use url::Url;

const PROVIDER_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const PROVIDER_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(30);
const BAD_HTTP_PROVIDER_TTL: Duration = Duration::from_secs(10 * 60);
const BAD_BITSWAP_PROVIDER_TTL: Duration = Duration::from_secs(30);
const HTTP_PROVIDER_TIMEOUT: Duration = Duration::from_secs(20);
const HTTP_PROVIDER_RACE_WIDTH: usize = 2;
const HTTP_PROVIDER_HEDGE_AFTER: Duration = Duration::from_millis(250);
const SINGLE_HTTP_PROVIDER_SELF_HEDGE_AFTER: Duration = Duration::from_millis(200);
const SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER: Duration = Duration::from_millis(150);
const HTTP_PROVIDER_SCORE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_HTTP_PROVIDER_SCORE_ENTRIES: usize = 64;
const DISABLE_HTTP_PROVIDER_SCORING_ENV: &str = "FREEDOM_IPFS_DISABLE_HTTP_PROVIDER_SCORING";
const DISABLE_SINGLE_HTTP_SELF_HEDGE_ENV: &str = "FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE";
const SINGLE_HTTP_SELF_HEDGE_AFTER_MS_ENV: &str = "FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS";
const SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS";
const ENABLE_SINGLE_HTTP_BITSWAP_HEDGE_ENV: &str = "FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE";
const SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS";
const MAX_CONCURRENT_HTTP_PROVIDER_FETCHES: usize = 8;
const MAX_CONCURRENT_HTTP_PROVIDER_FETCHES_ENV: &str =
    "FREEDOM_IPFS_MAX_CONCURRENT_HTTP_PROVIDER_FETCHES";
const BITSWAP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
// Start provider retry before a full dial timeout can dominate gateway TTFB.
const BITSWAP_CONNECTION_READY_TIMEOUT: Duration = Duration::from_secs(5);
const BITSWAP_CONNECTION_READY_TIMEOUT_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_CONNECTION_READY_TIMEOUT_MS";
// Keep WANT_HAVE as a short peer-selection probe; slow probes otherwise sit
// directly on the gateway TTFB path before we request the block.
const BITSWAP_WANT_HAVE_TIMEOUT: Duration = Duration::from_millis(750);
const BITSWAP_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(6);
const BITSWAP_SINGLE_UNTRUSTED_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(3);
const BITSWAP_INCOMING_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(6);
// Multi-CID range batches should consume immediately adjacent Bitswap response
// messages, then start bounded fallbacks for any missing CIDs. Longer waits
// inflated local range TTFB without reducing command count in harness sweeps.
const BITSWAP_INCOMING_BATCH_PARTIAL_GRACE: Duration = Duration::from_millis(5);
const BITSWAP_INCOMING_BATCH_PARTIAL_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_INCOMING_BATCH_PARTIAL_GRACE_MS";
const BITSWAP_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);
const BITSWAP_SUCCESSFUL_PEER_TTL: Duration = Duration::from_secs(10 * 60);
const BITSWAP_SUCCESSFUL_PEER_MAX_LATENCY_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SUCCESSFUL_PEER_MAX_LATENCY_MS";
const BITSWAP_CONNECTION_ERROR_BACKOFF_TTL: Duration = Duration::from_secs(30);
const BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD: usize = 2;
const BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD";
const BITSWAP_SESSION_SHORTCUT_GRACE: Duration = Duration::from_millis(0);
const BITSWAP_SESSION_SHORTCUT_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_SHORTCUT_GRACE_MS";
// Start provider lookup immediately. Recent session peers still race during
// lookup/post-lookup; a separate pre-lookup head start became a median tax once
// the post-lookup race covered useful session hits.
const BITSWAP_SESSION_PRE_LOOKUP_GRACE: Duration = Duration::from_millis(0);
const BITSWAP_SESSION_PRE_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_PRE_LOOKUP_GRACE_MS";
const BITSWAP_SESSION_POST_LOOKUP_GRACE: Duration = Duration::from_millis(100);
const BITSWAP_SESSION_MULTI_HTTP_POST_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_MULTI_HTTP_POST_LOOKUP_GRACE_MS";
// Give a recent Bitswap session peer a short chance to win before falling back
// to the only HTTP provider. Longer waits inflated page-asset tails on mobile
// browsing workloads without enough reliability benefit.
const BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE: Duration = Duration::from_millis(125);
const BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE_MS";
const DISABLE_SINGLE_HTTP_POST_LOOKUP_RACE_ENV: &str =
    "FREEDOM_IPFS_DISABLE_SINGLE_HTTP_POST_LOOKUP_RACE";
const SINGLE_HTTP_POST_LOOKUP_RACE_MIN_SCORE_MS_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_POST_LOOKUP_RACE_MIN_SCORE_MS";
const ENABLE_MULTI_HTTP_POST_LOOKUP_RACE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_MULTI_HTTP_POST_LOOKUP_RACE";
const DISABLE_MULTI_HTTP_POST_LOOKUP_RACE_ENV: &str =
    "FREEDOM_IPFS_DISABLE_MULTI_HTTP_POST_LOOKUP_RACE";
const DISABLE_MULTI_HTTP_FAST_POST_LOOKUP_RACE_ENV: &str =
    "FREEDOM_IPFS_DISABLE_MULTI_HTTP_FAST_POST_LOOKUP_RACE";
const MULTI_HTTP_FAST_POST_LOOKUP_RACE_MAX_SCORE_MS_ENV: &str =
    "FREEDOM_IPFS_MULTI_HTTP_FAST_POST_LOOKUP_RACE_MAX_SCORE_MS";
const MULTI_HTTP_FAST_POST_LOOKUP_RACE_MAX_SCORE: Duration = Duration::from_millis(100);
const ENABLE_ZERO_HTTP_POST_LOOKUP_RACE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_POST_LOOKUP_RACE";
const BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS: usize = 2;
const ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK";
const BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS";
const BITSWAP_SESSION_SHORTCUT_TIMEOUT: Duration = Duration::from_secs(2);
const BITSWAP_SESSION_LATE_PEER_WAIT: Duration = Duration::from_secs(2);
const BITSWAP_SESSION_LATE_PEER_POLL: Duration = Duration::from_millis(50);
// The per-peer read path has its own 10s timeout. This caps broader shared
// swarm stalls so one stuck command cannot sit on a browser request for 45s.
const BITSWAP_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
// Follow-on page blocks with a recent successful peer should either reuse that
// peer quickly or move to a fresh swarm/retry. Keep this narrower than the cold
// request cap, but only for mixed trusted+provider candidate sets.
const BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const BITSWAP_MAX_PENDING_OUTGOING_CONNECTIONS: u32 = 16;
const BITSWAP_MAX_ESTABLISHED_CONNECTIONS: u32 = 16;
// Keep one command from filling every pending outgoing dial slot. Page loads
// often request child blocks immediately after the root, so preserving headroom
// lets follow-on blocks dial instead of waiting behind stale public providers.
const MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND: usize = 5;
const MAX_BITSWAP_PEERS_PER_BLOCK: usize = 16;
const MAX_BITSWAP_SESSION_PEERS: usize = 4;
const MAX_BITSWAP_ADDRS_PER_PEER: usize = 2;
const MAX_BITSWAP_SESSION_RANGE_BATCH_CIDS: usize = 4;
const BITSWAP_SESSION_RANGE_BATCH_TIMEOUT: Duration = Duration::from_millis(750);
const ENABLE_BITSWAP_SESSION_RANGE_BATCH_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_SESSION_RANGE_BATCH";
const MAX_PENDING_INCOMING_BITSWAP_READS: usize = 32;
// Race a small number of untrusted providers with WANT_BLOCK before falling
// back to conservative WANT_HAVE probes for the rest. This lowers page-asset
// tails without requesting every block from every provider candidate.
const MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS: usize = 3;
const BITSWAP_DNS_PREFETCH_CONCURRENCY: usize = 8;
const MAX_BITSWAP_FAILURE_DETAILS: usize = 8;
const MAX_RECORDED_DIAL_ERRORS_PER_PEER: usize = 6;
const MAX_INFLIGHT_BLOCK_FETCHES: usize = 256;
const BLOCK_FETCH_COALESCE_HEDGE_AFTER: Duration = Duration::from_secs(8);
const CID_VERSION_0: u64 = 0;
const CID_VERSION_1: u64 = 1;
const BLOCK_PRESENCE_HAVE: i32 = 0;
const BLOCK_PRESENCE_DONT_HAVE: i32 = 1;

#[derive(Debug, Error)]
pub enum RetrievalError {
    #[error("routing: {0}")]
    Routing(#[from] freedom_ipfs_routing::RoutingError),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("core: {0}")]
    Core(#[from] freedom_ipfs_core::CoreError),
    #[error("store: {0}")]
    Store(#[from] freedom_ipfs_store::StoreError),
    #[error("url: {0}")]
    Url(#[from] url::ParseError),
    #[error("bitswap: {0}")]
    Bitswap(String),
    #[error("bitswap: {message}")]
    BitswapPeerFailures {
        message: String,
        timeout_peers: Vec<String>,
        connection_timeout_peers: Vec<String>,
    },
    #[error("bitswap request timed out")]
    BitswapTimeout,
    #[error("no HTTP-capable providers found")]
    NoHttpProviders,
    #[error("no Bitswap-capable providers found")]
    NoBitswapProviders,
}

pub type Result<T> = std::result::Result<T, RetrievalError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrievalSource {
    Cache,
    HttpProvider,
    Bitswap,
}

impl RetrievalSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::HttpProvider => "http_provider",
            Self::Bitswap => "bitswap",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetrievalStats {
    pub cache_hits: u64,
    pub http_provider_blocks: u64,
    pub bitswap_blocks: u64,
}

#[derive(Default)]
struct RetrievalStatsInner {
    cache_hits: AtomicU64,
    http_provider_blocks: AtomicU64,
    bitswap_blocks: AtomicU64,
}

impl RetrievalStatsInner {
    fn record(&self, source: RetrievalSource) {
        match source {
            RetrievalSource::Cache => &self.cache_hits,
            RetrievalSource::HttpProvider => &self.http_provider_blocks,
            RetrievalSource::Bitswap => &self.bitswap_blocks,
        }
        .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RetrievalStats {
        RetrievalStats {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            http_provider_blocks: self.http_provider_blocks.load(Ordering::Relaxed),
            bitswap_blocks: self.bitswap_blocks.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct HttpRetriever {
    client: reqwest::Client,
    routing: ProviderRoutingClient,
    store: SqliteBlockStore,
    bitswap: Arc<tokio::sync::Mutex<Option<SharedBitswapClient>>>,
    inflight: Arc<tokio::sync::Mutex<HashMap<Cid, SharedBlockFetch>>>,
    successful_bitswap_peers: Arc<tokio::sync::Mutex<HashMap<PeerId, SuccessfulBitswapPeer>>>,
    http_provider_scores: Arc<tokio::sync::Mutex<HashMap<String, HttpProviderScore>>>,
    http_provider_fetch_limiter: Arc<tokio::sync::Semaphore>,
    http_provider_fetch_limit: usize,
}

type SharedBlockFetch = Shared<BoxFuture<'static, Arc<SharedBlockFetchResult>>>;
type SharedBlockFetchResult = std::result::Result<(Block, RetrievalSource), String>;

#[derive(Clone, Copy)]
struct MissingBlockRange {
    index: usize,
    cid: Cid,
    start: u64,
    end: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetrievalRequestContext {
    gateway_subresource: bool,
}

impl RetrievalRequestContext {
    pub fn gateway_request(parent_request_id: Option<u64>) -> Self {
        Self {
            gateway_subresource: parent_request_id.is_some(),
        }
    }

    pub fn gateway_subresource(&self) -> bool {
        self.gateway_subresource
    }
}

tokio::task_local! {
    static RETRIEVAL_REQUEST_CONTEXT: RetrievalRequestContext;
}

pub async fn with_retrieval_request_context<F>(
    context: RetrievalRequestContext,
    future: F,
) -> F::Output
where
    F: Future,
{
    RETRIEVAL_REQUEST_CONTEXT.scope(context, future).await
}

fn current_retrieval_request_context() -> Option<RetrievalRequestContext> {
    RETRIEVAL_REQUEST_CONTEXT.try_with(|context| *context).ok()
}

impl HttpRetriever {
    pub fn new(routing: impl Into<ProviderRoutingClient>, store: SqliteBlockStore) -> Self {
        let http_provider_fetch_limit = max_concurrent_http_provider_fetches();
        Self {
            client: timeout_http_client(HTTP_PROVIDER_TIMEOUT),
            routing: routing.into(),
            store,
            bitswap: Arc::new(tokio::sync::Mutex::new(None)),
            inflight: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            successful_bitswap_peers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            http_provider_scores: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            http_provider_fetch_limiter: Arc::new(tokio::sync::Semaphore::new(
                http_provider_fetch_limit,
            )),
            http_provider_fetch_limit,
        }
    }

    pub async fn fetch_block(&self, cid: &Cid) -> Result<Block> {
        self.fetch_block_with_source(cid)
            .await
            .map(|(block, _source)| block)
    }

    pub async fn fetch_block_with_source(&self, cid: &Cid) -> Result<(Block, RetrievalSource)> {
        self.fetch_block_with_source_with_context(cid, None).await
    }

    async fn fetch_block_with_source_with_context(
        &self,
        cid: &Cid,
        context: Option<RetrievalRequestContext>,
    ) -> Result<(Block, RetrievalSource)> {
        let fetch_started = Instant::now();
        let cache_started = Instant::now();
        if let Some(block) = self.store.get(cid)? {
            tracing::info!(
                phase = "block_store_get",
                cid = %cid,
                cache_hit = true,
                elapsed_ms = cache_started.elapsed().as_millis()
            );
            tracing::info!(
                phase = "block_fetch_total",
                cid = %cid,
                source = "cache",
                elapsed_ms = fetch_started.elapsed().as_millis()
            );
            return Ok((block, RetrievalSource::Cache));
        }
        tracing::info!(
            phase = "block_store_get",
            cid = %cid,
            cache_hit = false,
            elapsed_ms = cache_started.elapsed().as_millis()
        );

        let (block, source) = self.fetch_block_uncached_coalesced(*cid, context).await?;
        tracing::info!(
            phase = "block_fetch_total",
            cid = %cid,
            source = retrieval_source_label(source),
            elapsed_ms = fetch_started.elapsed().as_millis()
        );
        Ok((block, source))
    }

    async fn fetch_block_uncached_coalesced(
        &self,
        cid: Cid,
        context: Option<RetrievalRequestContext>,
    ) -> Result<(Block, RetrievalSource)> {
        let wait_started = Instant::now();
        let mut inflight = self.inflight.lock().await;
        if let Some(fetch) = inflight.get(&cid).cloned() {
            drop(inflight);
            return tokio::select! {
                result = fetch => {
                    tracing::info!(
                        phase = "block_fetch_coalesced",
                        cid = %cid,
                        leader = false,
                        hedged = false,
                        elapsed_ms = wait_started.elapsed().as_millis()
                    );
                    shared_block_fetch_result(result)
                }
                _ = tokio::time::sleep(BLOCK_FETCH_COALESCE_HEDGE_AFTER) => {
                    tracing::info!(
                        phase = "block_fetch_coalesced",
                        cid = %cid,
                        leader = false,
                        hedged = true,
                        elapsed_ms = wait_started.elapsed().as_millis()
                    );
                    self.fetch_block_uncached_with_source(&cid, context).await
                }
            };
        }

        if inflight.len() >= MAX_INFLIGHT_BLOCK_FETCHES {
            drop(inflight);
            tracing::info!(
                phase = "block_fetch_coalesced",
                cid = %cid,
                leader = true,
                bypassed = true,
                inflight_count = MAX_INFLIGHT_BLOCK_FETCHES,
                elapsed_ms = wait_started.elapsed().as_millis()
            );
            return self.fetch_block_uncached_with_source(&cid, context).await;
        }

        let retriever = self.clone();
        let fetch = async move {
            Arc::new(
                retriever
                    .fetch_block_uncached_with_source(&cid, context)
                    .await
                    .map_err(|err| err.to_string()),
            )
        }
        .boxed()
        .shared();
        inflight.insert(cid, fetch.clone());
        let inflight_count = inflight.len();
        drop(inflight);

        let result = fetch.await;
        let mut inflight = self.inflight.lock().await;
        inflight.remove(&cid);
        drop(inflight);
        tracing::info!(
            phase = "block_fetch_coalesced",
            cid = %cid,
            leader = true,
            bypassed = false,
            inflight_count,
            elapsed_ms = wait_started.elapsed().as_millis()
        );
        shared_block_fetch_result(result)
    }

    async fn fetch_block_uncached_with_source(
        &self,
        cid: &Cid,
        context: Option<RetrievalRequestContext>,
    ) -> Result<(Block, RetrievalSource)> {
        let provider_cache_started = Instant::now();
        let providers = match self.cached_providers(cid)? {
            Some(providers) => {
                tracing::info!(
                    phase = "provider_cache",
                    cid = %cid,
                    cache_hit = true,
                    provider_count = providers.len(),
                    elapsed_ms = provider_cache_started.elapsed().as_millis()
                );
                providers
            }
            None => {
                tracing::info!(
                    phase = "provider_cache",
                    cid = %cid,
                    cache_hit = false,
                    elapsed_ms = provider_cache_started.elapsed().as_millis()
                );
                let recent_peers = self.recent_bitswap_peers_for_fetch().await;
                let (providers, provider_lookup_elapsed_ms) = if recent_peers.is_empty() {
                    let routing_started = Instant::now();
                    let provider_lookup = self.routing.providers(cid);
                    tokio::pin!(provider_lookup);
                    let late_peers = self
                        .wait_for_recent_bitswap_peers_for_fetch(BITSWAP_SESSION_LATE_PEER_WAIT);
                    tokio::pin!(late_peers);
                    let providers = tokio::select! {
                        lookup_result = &mut provider_lookup => {
                            match lookup_result {
                                Ok(providers) => providers,
                                Err(err) => {
                                    tracing::info!(
                                        phase = "provider_lookup",
                                        cid = %cid,
                                        error = %err,
                                        elapsed_ms = routing_started.elapsed().as_millis()
                                    );
                                    return Err(err.into());
                                }
                            }
                        }
                        (late_recent_peers, late_peer_elapsed_ms) = &mut late_peers => {
                            if late_recent_peers.is_empty() {
                                tracing::info!(
                                    phase = "bitswap_session_late_peer_wait",
                                    cid = %cid,
                                    outcome = "miss",
                                    timeout_ms = BITSWAP_SESSION_LATE_PEER_WAIT.as_millis(),
                                    elapsed_ms = late_peer_elapsed_ms
                                );
                                match provider_lookup.await {
                                    Ok(providers) => providers,
                                    Err(err) => {
                                        tracing::info!(
                                            phase = "provider_lookup",
                                            cid = %cid,
                                            error = %err,
                                            elapsed_ms = routing_started.elapsed().as_millis()
                                        );
                                        return Err(err.into());
                                    }
                                }
                            } else {
                                tracing::info!(
                                    phase = "bitswap_session_late_peer_wait",
                                    cid = %cid,
                                    outcome = "hit",
                                    peer_count = late_recent_peers.len(),
                                    timeout_ms = BITSWAP_SESSION_LATE_PEER_WAIT.as_millis(),
                                    elapsed_ms = late_peer_elapsed_ms
                                );
                                let shortcut = self.fetch_from_recent_bitswap_peers(cid, late_recent_peers);
                                tokio::pin!(shortcut);
                                tokio::select! {
                                    shortcut_result = &mut shortcut => {
                                        if let Some(block) = shortcut_result? {
                                            return Ok((block, RetrievalSource::Bitswap));
                                        }
                                        match provider_lookup.await {
                                            Ok(providers) => providers,
                                            Err(err) => {
                                                tracing::info!(
                                                    phase = "provider_lookup",
                                                    cid = %cid,
                                                    error = %err,
                                                    elapsed_ms = routing_started.elapsed().as_millis()
                                                );
                                                return Err(err.into());
                                            }
                                        }
                                    }
                                    lookup_result = &mut provider_lookup => {
                                        match lookup_result {
                                            Ok(providers) => {
                                                if providers.is_empty() {
                                                    match shortcut.await? {
                                                        Some(block) => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_empty_providers_wait",
                                                                cid = %cid,
                                                                outcome = "hit"
                                                            );
                                                            return Ok((block, RetrievalSource::Bitswap));
                                                        }
                                                        None => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_empty_providers_wait",
                                                                cid = %cid,
                                                                outcome = "miss"
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    let post_lookup_grace =
                                                        bitswap_session_post_lookup_grace(&providers);
                                                    let http_provider_count =
                                                        provider_http_url_count(&providers);
                                                    let mut post_lookup_race_allowed =
                                                        explicit_post_lookup_race_enabled_for_width(
                                                            http_provider_count,
                                                        ) || self
                                                            .single_http_post_lookup_race_allows(
                                                                cid,
                                                                &providers,
                                                                http_provider_count,
                                                            )
                                                            .await;
                                                    if !post_lookup_race_allowed {
                                                        post_lookup_race_allowed = self
                                                            .multi_http_fast_post_lookup_race_allows(
                                                                cid,
                                                                &providers,
                                                                http_provider_count,
                                                            )
                                                            .await;
                                                    }
                                                    if post_lookup_race_allowed
                                                    {
                                                        if let Some((block, source)) = self
                                                            .fetch_after_session_shortcut_provider_lookup(
                                                                cid,
                                                                &providers,
                                                                context,
                                                                shortcut.as_mut(),
                                                            )
                                                            .await?
                                                        {
                                                            return Ok((block, source));
                                                        }
                                                    } else {
                                                        let post_lookup_started = Instant::now();
                                                        match timeout(post_lookup_grace, &mut shortcut).await {
                                                        Ok(shortcut_result) => match shortcut_result {
                                                            Ok(Some(block)) => {
                                                                tracing::info!(
                                                                    phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                    cid = %cid,
                                                                    outcome = "hit",
                                                                    timeout_ms = post_lookup_grace.as_millis(),
                                                                    elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                    provider_count = providers.len(),
                                                                    http_provider_count
                                                                );
                                                                return Ok((block, RetrievalSource::Bitswap));
                                                            }
                                                            Ok(None) => {
                                                                tracing::info!(
                                                                    phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                    cid = %cid,
                                                                    outcome = "miss",
                                                                    timeout_ms = post_lookup_grace.as_millis(),
                                                                    elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                    provider_count = providers.len(),
                                                                    http_provider_count
                                                                );
                                                            }
                                                            Err(err) => {
                                                                tracing::info!(
                                                                    phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                    cid = %cid,
                                                                    outcome = "error",
                                                                    timeout_ms = post_lookup_grace.as_millis(),
                                                                    elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                    provider_count = providers.len(),
                                                                    http_provider_count,
                                                                    error = %err
                                                                );
                                                                return Err(err);
                                                            }
                                                        },
                                                        Err(_) => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                cid = %cid,
                                                                outcome = "timeout",
                                                                timeout_ms = post_lookup_grace.as_millis(),
                                                                elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                provider_count = providers.len(),
                                                                http_provider_count
                                                            );
                                                        }
                                                    }
                                                    }
                                                }
                                                providers
                                            }
                                            Err(lookup_err) => {
                                                if let Some(block) = shortcut.await? {
                                                    return Ok((block, RetrievalSource::Bitswap));
                                                }
                                                tracing::info!(
                                                    phase = "provider_lookup",
                                                    cid = %cid,
                                                    error = %lookup_err,
                                                    elapsed_ms = routing_started.elapsed().as_millis()
                                                );
                                                return Err(lookup_err.into());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    };
                    (providers, routing_started.elapsed().as_millis())
                } else {
                    let shortcut = async {
                        tokio::time::sleep(bitswap_session_shortcut_grace()).await;
                        self.fetch_from_recent_bitswap_peers(cid, recent_peers)
                            .await
                    };
                    tokio::pin!(shortcut);
                    let pre_lookup_started = Instant::now();
                    let pre_lookup_grace = bitswap_session_pre_lookup_grace();
                    match timeout(pre_lookup_grace, &mut shortcut).await {
                        Ok(shortcut_result) => {
                            if let Some(block) = shortcut_result? {
                                tracing::info!(
                                    phase = "bitswap_session_shortcut_pre_lookup",
                                    cid = %cid,
                                    timeout_ms = pre_lookup_grace.as_millis(),
                                    outcome = "hit",
                                    elapsed_ms = pre_lookup_started.elapsed().as_millis()
                                );
                                return Ok((block, RetrievalSource::Bitswap));
                            }
                            tracing::info!(
                                phase = "bitswap_session_shortcut_pre_lookup",
                                cid = %cid,
                                timeout_ms = pre_lookup_grace.as_millis(),
                                outcome = "miss",
                                elapsed_ms = pre_lookup_started.elapsed().as_millis()
                            );
                            let routing_started = Instant::now();
                            match self.routing.providers(cid).await {
                                Ok(providers) => (providers, routing_started.elapsed().as_millis()),
                                Err(err) => {
                                    tracing::info!(
                                        phase = "provider_lookup",
                                        cid = %cid,
                                        error = %err,
                                        elapsed_ms = routing_started.elapsed().as_millis()
                                    );
                                    return Err(err.into());
                                }
                            }
                        }
                        Err(_) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_pre_lookup",
                                cid = %cid,
                                timeout_ms = pre_lookup_grace.as_millis(),
                                outcome = "timeout",
                                elapsed_ms = pre_lookup_started.elapsed().as_millis()
                            );
                            let routing_started = Instant::now();
                            let provider_lookup = self.routing.providers(cid);
                            tokio::pin!(provider_lookup);
                            let providers = tokio::select! {
                                shortcut_result = &mut shortcut => {
                                    if let Some(block) = shortcut_result? {
                                        return Ok((block, RetrievalSource::Bitswap));
                                    }
                                    match provider_lookup.await {
                                        Ok(providers) => providers,
                                        Err(err) => {
                                            tracing::info!(
                                                phase = "provider_lookup",
                                                cid = %cid,
                                                error = %err,
                                                elapsed_ms = routing_started.elapsed().as_millis()
                                            );
                                            return Err(err.into());
                                        }
                                    }
                                }
                                lookup_result = &mut provider_lookup => {
                                    match lookup_result {
                                        Ok(providers) => {
                                            if providers.is_empty() {
                                                match shortcut.await? {
                                                    Some(block) => {
                                                        tracing::info!(
                                                            phase = "bitswap_session_shortcut_empty_providers_wait",
                                                            cid = %cid,
                                                            outcome = "hit"
                                                        );
                                                        return Ok((block, RetrievalSource::Bitswap));
                                                    }
                                                    None => {
                                                        tracing::info!(
                                                            phase = "bitswap_session_shortcut_empty_providers_wait",
                                                            cid = %cid,
                                                            outcome = "miss"
                                                        );
                                                    }
                                                }
                                            } else {
                                                let post_lookup_grace =
                                                    bitswap_session_post_lookup_grace(&providers);
                                                let http_provider_count =
                                                    provider_http_url_count(&providers);
                                                let mut post_lookup_race_allowed =
                                                    explicit_post_lookup_race_enabled_for_width(
                                                        http_provider_count,
                                                    ) || self
                                                        .single_http_post_lookup_race_allows(
                                                            cid,
                                                            &providers,
                                                            http_provider_count,
                                                        )
                                                        .await;
                                                if !post_lookup_race_allowed {
                                                    post_lookup_race_allowed = self
                                                        .multi_http_fast_post_lookup_race_allows(
                                                            cid,
                                                            &providers,
                                                            http_provider_count,
                                                        )
                                                        .await;
                                                }
                                                if post_lookup_race_allowed
                                                {
                                                    if let Some((block, source)) = self
                                                        .fetch_after_session_shortcut_provider_lookup(
                                                            cid,
                                                            &providers,
                                                            context,
                                                            shortcut.as_mut(),
                                                        )
                                                        .await?
                                                    {
                                                        return Ok((block, source));
                                                    }
                                                } else {
                                                    let post_lookup_started = Instant::now();
                                                    match timeout(post_lookup_grace, &mut shortcut).await {
                                                    Ok(shortcut_result) => match shortcut_result {
                                                        Ok(Some(block)) => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                cid = %cid,
                                                                outcome = "hit",
                                                                timeout_ms = post_lookup_grace.as_millis(),
                                                                elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                provider_count = providers.len(),
                                                                http_provider_count
                                                            );
                                                            return Ok((block, RetrievalSource::Bitswap));
                                                        }
                                                        Ok(None) => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                cid = %cid,
                                                                outcome = "miss",
                                                                timeout_ms = post_lookup_grace.as_millis(),
                                                                elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                provider_count = providers.len(),
                                                                http_provider_count
                                                            );
                                                        }
                                                        Err(err) => {
                                                            tracing::info!(
                                                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                                                cid = %cid,
                                                                outcome = "error",
                                                                timeout_ms = post_lookup_grace.as_millis(),
                                                                elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                                provider_count = providers.len(),
                                                                http_provider_count,
                                                                error = %err
                                                            );
                                                            return Err(err);
                                                        }
                                                    },
                                                    Err(_) => {
                                                        tracing::info!(
                                                            phase = "bitswap_session_shortcut_post_lookup_wait",
                                                            cid = %cid,
                                                            outcome = "timeout",
                                                            timeout_ms = post_lookup_grace.as_millis(),
                                                            elapsed_ms = post_lookup_started.elapsed().as_millis(),
                                                            provider_count = providers.len(),
                                                            http_provider_count
                                                        );
                                                    }
                                                }
                                                }
                                            }
                                            providers
                                        }
                                        Err(lookup_err) => {
                                            if let Some(block) = shortcut.await? {
                                                return Ok((block, RetrievalSource::Bitswap));
                                            }
                                            tracing::info!(
                                                phase = "provider_lookup",
                                                cid = %cid,
                                                error = %lookup_err,
                                                elapsed_ms = routing_started.elapsed().as_millis()
                                            );
                                            return Err(lookup_err.into());
                                        }
                                    }
                                }
                            };
                            (providers, routing_started.elapsed().as_millis())
                        }
                    }
                };
                tracing::info!(
                    phase = "provider_lookup",
                    cid = %cid,
                    provider_count = providers.len(),
                    elapsed_ms = provider_lookup_elapsed_ms
                );
                self.cache_providers(cid, &providers)?;
                providers
            }
        };
        if let Some(block) = self.recheck_block_store(cid)? {
            return Ok((block, RetrievalSource::Cache));
        }
        match self
            .fetch_from_providers_with_source(cid, &providers, context)
            .await
        {
            Ok((block, source)) => Ok((block, source)),
            Err(err) if providers.is_empty() && is_no_provider_error(&err) => {
                tracing::info!(
                    phase = "provider_refresh_skipped_empty_provider_set",
                    cid = %cid,
                    error = %err,
                    initial_error = %err
                );
                Err(err)
            }
            Err(err) if should_refresh_providers_after_failure(&err) => {
                let timeout_peer_count = bitswap_timeout_peers(&err).len();
                let connection_timeout_peer_count = bitswap_connection_timeout_peers(&err).len();
                let request_timeout = is_bitswap_request_timeout(&err);
                tracing::info!(
                    phase = if timeout_peer_count > 0 || request_timeout {
                        "provider_refresh_after_timeout"
                    } else {
                        "provider_refresh_after_failure"
                    },
                    cid = %cid,
                    timeout_peer_count,
                    connection_timeout_peer_count,
                    request_timeout,
                    initial_error = %err
                );
                let routing_started = Instant::now();
                let refreshed = match self.routing.providers(cid).await {
                    Ok(providers) => {
                        tracing::info!(
                            phase = "provider_lookup",
                            cid = %cid,
                            provider_count = providers.len(),
                            refreshed = true,
                            elapsed_ms = routing_started.elapsed().as_millis()
                        );
                        providers
                    }
                    Err(refresh_err) => {
                        tracing::info!(
                            phase = "provider_lookup",
                            cid = %cid,
                            refreshed = true,
                            error = %refresh_err,
                            elapsed_ms = routing_started.elapsed().as_millis()
                        );
                        tracing::debug!(
                            cid = %cid,
                            error = %refresh_err,
                            "provider refresh after retrieval failure failed"
                        );
                        return Err(err);
                    }
                };
                let same_providers = same_provider_set(&providers, &refreshed);
                let same_bitswap_peers = if request_timeout {
                    same_bitswap_peer_set(&providers, &refreshed).await
                } else {
                    false
                };
                tracing::info!(
                    phase = "retry_provider_count",
                    cid = %cid,
                    previous_provider_count = providers.len(),
                    retry_provider_count = refreshed.len(),
                    same_provider_set = same_providers,
                    same_bitswap_peer_set = same_bitswap_peers,
                    timeout_peer_count,
                    connection_timeout_peer_count,
                    request_timeout
                );
                if same_providers {
                    if timeout_peer_count > 0 || request_timeout {
                        tracing::info!(
                            phase = if request_timeout {
                                "provider_retry_after_request_timeout"
                            } else {
                                "provider_retry_after_timeout"
                            },
                            cid = %cid,
                            provider_count = providers.len(),
                            timeout_peer_count,
                            request_timeout,
                            initial_error = %err
                        );
                        return match self
                            .fetch_from_providers_with_source(cid, &providers, context)
                            .await
                        {
                            Ok((block, source)) => Ok((block, source)),
                            Err(retry_err) => Err(RetrievalError::Bitswap(format!(
                                "initial provider retrieval failed ({err}); same-provider retry after timeout failed ({retry_err})"
                            ))),
                        };
                    }
                    if is_bitswap_connection_ready_failure(&err) {
                        tracing::info!(
                            phase = "provider_retry_after_connection_timeout",
                            cid = %cid,
                            provider_count = providers.len(),
                            initial_error = %err
                        );
                        return match self
                            .fetch_from_providers_with_source(cid, &providers, context)
                            .await
                        {
                            Ok((block, source)) => Ok((block, source)),
                            Err(retry_err) => Err(RetrievalError::Bitswap(format!(
                                "initial provider retrieval failed ({err}); same-provider retry failed ({retry_err})"
                            ))),
                        };
                    }
                    return Err(err);
                }
                self.cache_providers(cid, &refreshed)?;
                match self
                    .fetch_from_providers_with_source(cid, &refreshed, context)
                    .await
                {
                    Ok((block, source)) => Ok((block, source)),
                    Err(refresh_err) => Err(RetrievalError::Bitswap(format!(
                        "initial provider retrieval failed ({err}); refreshed provider retrieval failed ({refresh_err})"
                    ))),
                }
            }
            Err(err) => Err(err),
        }
    }

    fn recheck_block_store(&self, cid: &Cid) -> Result<Option<Block>> {
        let cache_started = Instant::now();
        match self.store.get(cid)? {
            Some(block) => {
                tracing::info!(
                    phase = "block_store_get",
                    cid = %cid,
                    cache_hit = true,
                    rechecked = true,
                    elapsed_ms = cache_started.elapsed().as_millis()
                );
                Ok(Some(block))
            }
            None => {
                tracing::info!(
                    phase = "block_store_get",
                    cid = %cid,
                    cache_hit = false,
                    rechecked = true,
                    elapsed_ms = cache_started.elapsed().as_millis()
                );
                Ok(None)
            }
        }
    }

    pub async fn fetch_from_providers(&self, cid: &Cid, providers: &[Provider]) -> Result<Block> {
        self.fetch_from_providers_with_source(cid, providers, None)
            .await
            .map(|(block, _source)| block)
    }

    pub async fn fetch_from_providers_with_source(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
    ) -> Result<(Block, RetrievalSource)> {
        tracing::info!(
            phase = "provider_fetch_start",
            cid = %cid,
            provider_count = providers.len()
        );
        let mut http_provider_bases = Vec::new();
        for provider in providers {
            for base in &provider.http_urls {
                if self.store.is_bad_provider(base.as_str())? {
                    tracing::debug!(provider = %base, "skipping temporarily bad HTTP provider");
                    continue;
                }
                http_provider_bases.push(base.clone());
            }
        }
        if http_provider_bases.len() == 1
            && single_http_provider_bitswap_hedge_enabled()
            && has_bitswap_provider_candidate(providers)
            && self
                .single_http_provider_bitswap_hedge_score_allows(
                    cid,
                    http_provider_bases
                        .first()
                        .expect("single HTTP provider base is present"),
                )
                .await
        {
            return self
                .fetch_single_http_provider_with_bitswap_hedge(
                    cid,
                    http_provider_bases,
                    providers.to_vec(),
                )
                .await;
        }
        if let Some(block) = self
            .fetch_from_http_provider_candidates(cid, http_provider_bases)
            .await?
        {
            return Ok((block, RetrievalSource::HttpProvider));
        }
        match self
            .fetch_from_bitswap_providers(cid, providers, context)
            .await
        {
            Ok(block) => Ok((block, RetrievalSource::Bitswap)),
            Err(RetrievalError::NoBitswapProviders) => Err(RetrievalError::NoHttpProviders),
            Err(err) => Err(err),
        }
    }

    async fn fetch_after_session_shortcut_provider_lookup<F>(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        mut shortcut: Pin<&mut F>,
    ) -> Result<Option<(Block, RetrievalSource)>>
    where
        F: Future<Output = Result<Option<Block>>>,
    {
        let post_lookup_grace = bitswap_session_post_lookup_grace(providers);
        let http_provider_count = provider_http_url_count(providers);
        let provider_fetch = self.fetch_from_providers_with_source(cid, providers, context);
        tokio::pin!(provider_fetch);
        let race_started = Instant::now();
        let provider_count = providers.len();
        tokio::select! {
            biased;

            shortcut_result = shortcut.as_mut() => {
                match shortcut_result {
                Ok(Some(block)) => {
                    tracing::info!(
                        phase = "bitswap_session_shortcut_post_lookup_race",
                        cid = %cid,
                        outcome = "bitswap_won",
                        timeout_ms = post_lookup_grace.as_millis(),
                        elapsed_ms = race_started.elapsed().as_millis(),
                        provider_count,
                        http_provider_count
                    );
                    Ok(Some((block, RetrievalSource::Bitswap)))
                }
                Ok(None) => {
                    tracing::info!(
                        phase = "bitswap_session_shortcut_post_lookup_race",
                        cid = %cid,
                        outcome = "bitswap_miss",
                        timeout_ms = post_lookup_grace.as_millis(),
                        elapsed_ms = race_started.elapsed().as_millis(),
                        provider_count,
                        http_provider_count
                    );
                    match provider_fetch.await {
                        Ok((block, source)) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_post_lookup_race",
                                cid = %cid,
                                outcome = "provider_after_bitswap_miss",
                                source = source.as_str(),
                                timeout_ms = post_lookup_grace.as_millis(),
                                elapsed_ms = race_started.elapsed().as_millis(),
                                provider_count,
                                http_provider_count
                            );
                            Ok(Some((block, source)))
                        }
                        Err(err) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_post_lookup_race",
                                cid = %cid,
                                outcome = "provider_error_after_bitswap_miss",
                                timeout_ms = post_lookup_grace.as_millis(),
                                elapsed_ms = race_started.elapsed().as_millis(),
                                provider_count,
                                http_provider_count,
                                error = %err
                            );
                            Ok(None)
                        }
                    }
                }
                Err(err) => {
                    tracing::info!(
                        phase = "bitswap_session_shortcut_post_lookup_race",
                        cid = %cid,
                        outcome = "bitswap_error",
                        timeout_ms = post_lookup_grace.as_millis(),
                        elapsed_ms = race_started.elapsed().as_millis(),
                        provider_count,
                        http_provider_count,
                        error = %err
                    );
                    Err(err)
                }
                }
            }
            provider_result = &mut provider_fetch => {
                match provider_result {
                    Ok((block, source)) => {
                        tracing::info!(
                            phase = "bitswap_session_shortcut_post_lookup_race",
                            cid = %cid,
                            outcome = "provider_won",
                            source = source.as_str(),
                            timeout_ms = post_lookup_grace.as_millis(),
                            elapsed_ms = race_started.elapsed().as_millis(),
                            provider_count,
                            http_provider_count
                        );
                        Ok(Some((block, source)))
                    }
                    Err(err) => {
                        tracing::info!(
                            phase = "bitswap_session_shortcut_post_lookup_race",
                            cid = %cid,
                            outcome = "provider_error",
                            timeout_ms = post_lookup_grace.as_millis(),
                            elapsed_ms = race_started.elapsed().as_millis(),
                            provider_count,
                            http_provider_count,
                            error = %err
                        );
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn single_http_post_lookup_race_allows(
        &self,
        cid: &Cid,
        providers: &[Provider],
        http_provider_count: usize,
    ) -> bool {
        if !single_http_post_lookup_race_enabled() || http_provider_count != 1 {
            return false;
        }
        let Some(min_score) = single_http_post_lookup_race_min_score() else {
            return true;
        };
        let Some(base) = single_http_provider_base(providers) else {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                reason = "provider_unavailable",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        self.single_http_post_lookup_race_score_allows_with_min(cid, base, Some(min_score))
            .await
    }

    async fn single_http_post_lookup_race_score_allows_with_min(
        &self,
        cid: &Cid,
        base: &Url,
        min_score: Option<Duration>,
    ) -> bool {
        let Some(min_score) = min_score else {
            return true;
        };
        if !http_provider_scoring_enabled() {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                provider = %base,
                reason = "scoring_disabled",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        let Some(key) = http_provider_score_key(base) else {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_unkeyed",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        let mut scores = self.http_provider_scores.lock().await;
        let now = Instant::now();
        scores.retain(|_, score| {
            now.saturating_duration_since(score.last_seen) <= HTTP_PROVIDER_SCORE_TTL
        });
        let Some(score) = scores.get(&key) else {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_unscored",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        if score.ewma_elapsed < min_score {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_score_below_threshold",
                provider_scored = true,
                provider_score_ms = score.ewma_elapsed.as_millis(),
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        true
    }

    async fn multi_http_fast_post_lookup_race_allows(
        &self,
        cid: &Cid,
        providers: &[Provider],
        http_provider_count: usize,
    ) -> bool {
        self.multi_http_fast_post_lookup_race_allows_with_max(
            cid,
            providers,
            http_provider_count,
            multi_http_fast_post_lookup_race_max_score(),
        )
        .await
    }

    async fn multi_http_fast_post_lookup_race_allows_with_max(
        &self,
        cid: &Cid,
        providers: &[Provider],
        http_provider_count: usize,
        max_score: Option<Duration>,
    ) -> bool {
        let Some(max_score) = max_score else {
            return false;
        };
        if http_provider_count <= 1 {
            return false;
        }
        if !http_provider_scoring_enabled() {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                reason = "multi_http_scoring_disabled",
                provider_scored = false,
                provider_score_ms = 0u128,
                max_score_ms = max_score.as_millis(),
                http_provider_count
            );
            return false;
        }

        let bases = providers
            .iter()
            .flat_map(|provider| provider.http_urls.iter().cloned())
            .collect::<Vec<_>>();
        if bases.len() <= 1 {
            return false;
        }

        let candidates = self.scored_http_provider_candidates(bases).await;
        let scored_provider_count = candidates
            .iter()
            .filter(|candidate| candidate.score_elapsed.is_some())
            .count();
        let Some(best_score) = candidates
            .iter()
            .filter_map(|candidate| candidate.score_elapsed)
            .min()
        else {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                reason = "multi_http_providers_unscored",
                provider_scored = false,
                provider_score_ms = 0u128,
                max_score_ms = max_score.as_millis(),
                http_provider_count,
                scored_provider_count
            );
            return false;
        };

        if best_score > max_score {
            tracing::info!(
                phase = "bitswap_session_shortcut_post_lookup_race_skip",
                cid = %cid,
                reason = "multi_http_provider_score_above_threshold",
                provider_scored = true,
                provider_score_ms = best_score.as_millis(),
                max_score_ms = max_score.as_millis(),
                http_provider_count,
                scored_provider_count
            );
            return false;
        }

        tracing::info!(
            phase = "bitswap_session_shortcut_post_lookup_race_gate",
            cid = %cid,
            reason = "multi_http_fast_provider_score",
            provider_score_ms = best_score.as_millis(),
            max_score_ms = max_score.as_millis(),
            http_provider_count,
            scored_provider_count
        );
        true
    }

    fn cached_providers(&self, cid: &Cid) -> Result<Option<Vec<Provider>>> {
        let Some(records) = self.store.get_provider_records(cid)? else {
            return Ok(None);
        };
        let providers = records
            .into_iter()
            .map(|record| Provider::from_parts(record.id, record.addrs))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Some(providers))
    }

    fn cache_providers(&self, cid: &Cid, providers: &[Provider]) -> Result<()> {
        let records = providers
            .iter()
            .map(|provider| CachedProviderRecord {
                id: provider.id.clone(),
                addrs: provider.addrs.clone(),
            })
            .collect::<Vec<_>>();
        let ttl = if records.is_empty() {
            PROVIDER_NEGATIVE_CACHE_TTL
        } else {
            PROVIDER_CACHE_TTL
        };
        self.store.put_provider_records(cid, &records, ttl)?;
        Ok(())
    }

    async fn fetch_from_http_provider_candidates(
        &self,
        cid: &Cid,
        bases: Vec<Url>,
    ) -> Result<Option<Block>> {
        if bases.is_empty() {
            return Ok(None);
        }

        let started = Instant::now();
        let candidates = self.scored_http_provider_candidates(bases).await;
        let provider_count = candidates.len();
        let scored_provider_count = candidates
            .iter()
            .filter(|candidate| candidate.score_elapsed.is_some())
            .count();
        let scoring_enabled = http_provider_scoring_enabled();
        tracing::info!(
            phase = "http_provider_race",
            cid = %cid,
            provider_count,
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            scored_provider_count,
            scoring_enabled
        );

        if provider_count == 1 && single_http_provider_self_hedge_enabled() {
            let Some(candidate) = candidates.first() else {
                return Ok(None);
            };
            if self
                .single_http_provider_self_hedge_score_allows(cid, &candidate.base)
                .await
            {
                let Some(candidate) = candidates.into_iter().next() else {
                    return Ok(None);
                };
                return self
                    .fetch_single_http_provider_candidate_with_self_hedge(
                        cid,
                        candidate,
                        provider_count,
                        scored_provider_count,
                        started,
                    )
                    .await;
            }
        }

        let mut next_bases = candidates.into_iter().enumerate();
        let mut pending = FuturesUnordered::new();
        let mut attempted_provider_count = 0usize;
        let mut failed_provider_count = 0usize;
        for _ in 0..HTTP_PROVIDER_RACE_WIDTH {
            let Some((scheduled_index, candidate)) = next_bases.next() else {
                break;
            };
            attempted_provider_count += 1;
            pending.push(self.fetch_from_http_provider_candidate_with_index(
                *cid,
                scheduled_index,
                candidate,
                0,
            ));
        }

        let hedge = tokio::time::sleep(HTTP_PROVIDER_HEDGE_AFTER);
        tokio::pin!(hedge);
        let mut completion_seen_before_hedge = false;
        let mut hedge_fired = false;

        while !pending.is_empty() {
            tokio::select! {
                biased;

                result = pending.next() => {
                    let Some(result) = result else {
                        break;
                    };
                    completion_seen_before_hedge = true;
                    match result.result {
                        Ok(block) => {
                            tracing::info!(
                                phase = "http_provider_race_result",
                                cid = %cid,
                                ok = true,
                                provider = %result.base,
                                winner_provider_index = result.provider_index,
                                winner_attempt_index = result.attempt_index,
                                winner_provider_rank = result.provider_index + 1,
                                winner_original_provider_rank = result.original_provider_index + 1,
                                winner_within_initial_width = (result.provider_index < HTTP_PROVIDER_RACE_WIDTH),
                                provider_count,
                                race_width = HTTP_PROVIDER_RACE_WIDTH,
                                scored_provider_count,
                                winner_provider_scored = result.score_elapsed.is_some(),
                                winner_provider_score_ms = result
                                    .score_elapsed
                                    .map(|elapsed| elapsed.as_millis())
                                    .unwrap_or_default(),
                                attempted_provider_count,
                                failed_provider_count,
                                hedge_fired,
                                elapsed_ms = started.elapsed().as_millis()
                            );
                            return Ok(Some(block));
                        }
                        Err(_) => {
                            failed_provider_count += 1;
                            if let Some((scheduled_index, candidate)) = next_bases.next() {
                                attempted_provider_count += 1;
                                pending.push(self.fetch_from_http_provider_candidate_with_index(
                                    *cid,
                                    scheduled_index,
                                    candidate,
                                    0,
                                ));
                            }
                        }
                    }
                }
                _ = &mut hedge, if !hedge_fired
                    && !completion_seen_before_hedge
                    && next_bases.len() > 0 => {
                    hedge_fired = true;
                    if let Some((scheduled_index, candidate)) = next_bases.next() {
                        tracing::info!(
                            phase = "http_provider_hedge",
                            cid = %cid,
                            provider = %candidate.base,
                            timeout_ms = HTTP_PROVIDER_HEDGE_AFTER.as_millis(),
                            provider_index = scheduled_index,
                            original_provider_rank = candidate.original_index + 1,
                            provider_scored = candidate.score_elapsed.is_some(),
                            provider_score_ms = candidate
                                .score_elapsed
                                .map(|elapsed| elapsed.as_millis())
                                .unwrap_or_default(),
                            pending_count = pending.len(),
                            remaining_provider_count = next_bases.len()
                        );
                        attempted_provider_count += 1;
                        pending.push(self.fetch_from_http_provider_candidate_with_index(
                            *cid,
                            scheduled_index,
                            candidate,
                            0,
                        ));
                    }
                }
            }
        }

        tracing::info!(
            phase = "http_provider_race_result",
            cid = %cid,
            ok = false,
            provider_count,
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            attempted_provider_count,
            failed_provider_count,
            hedge_fired,
            elapsed_ms = started.elapsed().as_millis()
        );
        Ok(None)
    }

    async fn fetch_single_http_provider_with_bitswap_hedge(
        &self,
        cid: &Cid,
        http_provider_bases: Vec<Url>,
        providers: Vec<Provider>,
    ) -> Result<(Block, RetrievalSource)> {
        let started = Instant::now();
        let provider_count = providers.len();
        let candidates = self
            .scored_http_provider_candidates(http_provider_bases)
            .await;
        let scored_provider_count = candidates
            .iter()
            .filter(|candidate| candidate.score_elapsed.is_some())
            .count();
        let scoring_enabled = http_provider_scoring_enabled();
        tracing::info!(
            phase = "http_provider_race",
            cid = %cid,
            provider_count = candidates.len(),
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            scored_provider_count,
            scoring_enabled,
            single_provider_bitswap_hedge = true
        );
        let Some(candidate) = candidates.into_iter().next() else {
            return Err(RetrievalError::NoHttpProviders);
        };
        let mut pending =
            FuturesUnordered::<BoxFuture<'static, SingleHttpProviderBitswapHedgeResult>>::new();

        let retriever = self.clone();
        let http_cid = *cid;
        pending.push(
            async move {
                SingleHttpProviderBitswapHedgeResult::HttpCandidate(
                    retriever
                        .fetch_from_http_provider_candidate_with_index(http_cid, 0, candidate, 0)
                        .await,
                )
            }
            .boxed(),
        );

        let hedge = tokio::time::sleep(SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER);
        tokio::pin!(hedge);
        let mut bitswap_started = false;
        let mut http_done = false;
        let mut bitswap_error = None;

        loop {
            tokio::select! {
                biased;

                result = pending.next(), if !pending.is_empty() => {
                    let Some(result) = result else {
                        break;
                    };
                    match result {
                        SingleHttpProviderBitswapHedgeResult::HttpCandidate(result) => {
                            http_done = true;
                            match result.result {
                                Ok(block) => {
                                    tracing::info!(
                                        phase = "http_provider_race_result",
                                        cid = %cid,
                                        ok = true,
                                        provider = %result.base,
                                        winner_provider_index = result.provider_index,
                                        winner_attempt_index = result.attempt_index,
                                        winner_provider_rank = result.provider_index + 1,
                                        winner_original_provider_rank = result.original_provider_index + 1,
                                        winner_within_initial_width = true,
                                        provider_count = 1usize,
                                        race_width = HTTP_PROVIDER_RACE_WIDTH,
                                        scored_provider_count,
                                        winner_provider_scored = result.score_elapsed.is_some(),
                                        winner_provider_score_ms = result
                                            .score_elapsed
                                            .map(|elapsed| elapsed.as_millis())
                                            .unwrap_or_default(),
                                        single_provider_bitswap_hedge = true,
                                        bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    tracing::info!(
                                        phase = "http_provider_bitswap_hedge_result",
                                        cid = %cid,
                                        source = "http_provider",
                                        provider_count,
                                        bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    return Ok((block, RetrievalSource::HttpProvider));
                                }
                                Err(_) => {
                                    if !bitswap_started {
                                        bitswap_started = true;
                                        push_single_http_bitswap_hedge(
                                            &mut pending,
                                            self.clone(),
                                            *cid,
                                            providers.clone(),
                                            provider_count,
                                            started,
                                            "http_provider_failed",
                                        );
                                    }
                                }
                            }
                        }
                        SingleHttpProviderBitswapHedgeResult::Bitswap(Ok(block)) => {
                            tracing::info!(
                                phase = "http_provider_bitswap_hedge_result",
                                cid = %cid,
                                source = "bitswap",
                                provider_count,
                                http_done,
                                elapsed_ms = started.elapsed().as_millis()
                            );
                            return Ok((block, RetrievalSource::Bitswap));
                        }
                        SingleHttpProviderBitswapHedgeResult::Bitswap(Err(err)) => {
                            bitswap_error = Some(err);
                            if http_done {
                                break;
                            }
                        }
                    }
                }
                _ = &mut hedge, if !bitswap_started => {
                    bitswap_started = true;
                    push_single_http_bitswap_hedge(
                        &mut pending,
                        self.clone(),
                        *cid,
                        providers.clone(),
                        provider_count,
                        started,
                        "slow_single_http_provider",
                    );
                }
                else => break,
            }
        }

        match bitswap_error {
            Some(RetrievalError::NoBitswapProviders) | None => Err(RetrievalError::NoHttpProviders),
            Some(err) => Err(err),
        }
    }

    async fn fetch_single_http_provider_candidate_with_self_hedge(
        &self,
        cid: &Cid,
        candidate: ScoredHttpProviderBase,
        provider_count: usize,
        scored_provider_count: usize,
        started: Instant,
    ) -> Result<Option<Block>> {
        let mut pending = FuturesUnordered::new();
        pending.push(self.fetch_from_http_provider_candidate_with_index(
            *cid,
            0,
            candidate.clone(),
            0,
        ));
        let self_hedge_after = single_http_provider_self_hedge_after();
        let hedge = tokio::time::sleep(self_hedge_after);
        tokio::pin!(hedge);
        let mut attempted_provider_count = 1usize;
        let mut failed_provider_count = 0usize;
        let mut hedge_fired = false;

        while !pending.is_empty() {
            tokio::select! {
                biased;

                result = pending.next() => {
                    let Some(result) = result else {
                        break;
                    };
                    match result.result {
                        Ok(block) => {
                            tracing::info!(
                                phase = "http_provider_race_result",
                                cid = %cid,
                                ok = true,
                                provider = %result.base,
                                winner_provider_index = result.provider_index,
                                winner_attempt_index = result.attempt_index,
                                winner_self_hedge_attempt = result.attempt_index > 0,
                                winner_provider_rank = result.provider_index + 1,
                                winner_original_provider_rank = result.original_provider_index + 1,
                                winner_within_initial_width = true,
                                provider_count,
                                race_width = HTTP_PROVIDER_RACE_WIDTH,
                                scored_provider_count,
                                winner_provider_scored = result.score_elapsed.is_some(),
                                winner_provider_score_ms = result
                                    .score_elapsed
                                    .map(|elapsed| elapsed.as_millis())
                                    .unwrap_or_default(),
                                single_provider_self_hedge = true,
                                attempted_provider_count,
                                failed_provider_count,
                                hedge_fired,
                                elapsed_ms = started.elapsed().as_millis()
                            );
                            return Ok(Some(block));
                        }
                        Err(_) => {
                            failed_provider_count += 1;
                            if pending.is_empty() && !hedge_fired {
                                attempted_provider_count += 1;
                                hedge_fired = true;
                                tracing::info!(
                                    phase = "http_provider_self_hedge",
                                    cid = %cid,
                                    provider = %candidate.base,
                                    timeout_ms = self_hedge_after.as_millis(),
                                    provider_index = 0,
                                    attempt_index = 1usize,
                                    original_provider_rank = candidate.original_index + 1,
                                    provider_scored = candidate.score_elapsed.is_some(),
                                    provider_score_ms = candidate
                                        .score_elapsed
                                        .map(|elapsed| elapsed.as_millis())
                                        .unwrap_or_default(),
                                    reason = "initial_failure"
                                );
                                pending.push(self.fetch_from_http_provider_candidate_with_index(
                                    *cid,
                                    0,
                                    candidate.clone(),
                                    1,
                                ));
                            }
                        }
                    }
                }
                _ = &mut hedge, if !hedge_fired => {
                    hedge_fired = true;
                    attempted_provider_count += 1;
                    tracing::info!(
                        phase = "http_provider_self_hedge",
                        cid = %cid,
                        provider = %candidate.base,
                        timeout_ms = self_hedge_after.as_millis(),
                        provider_index = 0,
                        attempt_index = 1usize,
                        original_provider_rank = candidate.original_index + 1,
                        provider_scored = candidate.score_elapsed.is_some(),
                        provider_score_ms = candidate
                            .score_elapsed
                            .map(|elapsed| elapsed.as_millis())
                            .unwrap_or_default(),
                        reason = "slow_single_provider"
                    );
                    pending.push(self.fetch_from_http_provider_candidate_with_index(
                        *cid,
                        0,
                        candidate.clone(),
                        1,
                    ));
                }
            }
        }

        tracing::info!(
            phase = "http_provider_race_result",
            cid = %cid,
            ok = false,
            provider_count,
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            single_provider_self_hedge = true,
            attempted_provider_count,
            failed_provider_count,
            hedge_fired,
            elapsed_ms = started.elapsed().as_millis()
        );
        Ok(None)
    }

    async fn fetch_from_http_provider_candidate_with_index(
        &self,
        cid: Cid,
        provider_index: usize,
        candidate: ScoredHttpProviderBase,
        attempt_index: usize,
    ) -> HttpProviderCandidateResult {
        let base = candidate.base;
        let result = self
            .fetch_from_http_provider_candidate(cid, base.clone())
            .await;
        HttpProviderCandidateResult {
            provider_index,
            attempt_index,
            original_provider_index: candidate.original_index,
            score_elapsed: candidate.score_elapsed,
            base,
            result,
        }
    }

    async fn fetch_from_http_provider_candidate(&self, cid: Cid, base: Url) -> Result<Block> {
        let limiter_started = Instant::now();
        let _permit = match self
            .http_provider_fetch_limiter
            .clone()
            .acquire_owned()
            .await
        {
            Ok(permit) => {
                tracing::info!(
                    phase = "http_provider_fetch_limiter",
                    cid = %cid,
                    provider = %base,
                    acquired = true,
                    max_concurrent = self.http_provider_fetch_limit,
                    elapsed_ms = limiter_started.elapsed().as_millis()
                );
                permit
            }
            Err(err) => {
                tracing::info!(
                    phase = "http_provider_fetch_limiter",
                    cid = %cid,
                    provider = %base,
                    acquired = false,
                    max_concurrent = self.http_provider_fetch_limit,
                    error = %err,
                    elapsed_ms = limiter_started.elapsed().as_millis()
                );
                return Err(RetrievalError::Bitswap(format!(
                    "HTTP provider limiter closed: {err}"
                )));
            }
        };
        let started = Instant::now();
        match self.fetch_from_http_provider(&cid, &base).await {
            Ok((block, stats)) => {
                self.record_http_provider_success(&base, stats.body_elapsed)
                    .await;
                tracing::info!(
                    phase = "http_provider_fetch",
                    cid = %cid,
                    provider = %base,
                    ok = true,
                    bytes = block.data().len(),
                    response_bytes = stats.response_bytes,
                    response_headers_elapsed_ms = stats.headers_elapsed.as_millis(),
                    response_first_chunk_seen = stats.first_chunk_elapsed.is_some(),
                    response_first_chunk_elapsed_ms = stats
                        .first_chunk_elapsed
                        .map(|elapsed| elapsed.as_millis())
                        .unwrap_or_default(),
                    response_body_elapsed_ms = stats.body_elapsed.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                Ok(block)
            }
            Err(err) => {
                tracing::info!(
                    phase = "http_provider_fetch",
                    cid = %cid,
                    provider = %base,
                    ok = false,
                    error = %err,
                    elapsed_ms = started.elapsed().as_millis()
                );
                let _ = self.store.mark_bad_provider(
                    base.as_str(),
                    &err.to_string(),
                    BAD_HTTP_PROVIDER_TTL,
                );
                Err(err)
            }
        }
    }

    async fn scored_http_provider_candidates(
        &self,
        bases: Vec<Url>,
    ) -> Vec<ScoredHttpProviderBase> {
        if !http_provider_scoring_enabled() {
            return bases
                .into_iter()
                .enumerate()
                .map(|(original_index, base)| ScoredHttpProviderBase {
                    original_index,
                    score_elapsed: None,
                    base,
                })
                .collect();
        }

        let mut scores = self.http_provider_scores.lock().await;
        let now = Instant::now();
        scores.retain(|_, score| {
            now.saturating_duration_since(score.last_seen) <= HTTP_PROVIDER_SCORE_TTL
        });

        let mut candidates = bases
            .into_iter()
            .enumerate()
            .map(|(original_index, base)| {
                let score_elapsed = http_provider_score_key(&base)
                    .and_then(|key| scores.get(&key).map(|score| score.ewma_elapsed));
                ScoredHttpProviderBase {
                    original_index,
                    score_elapsed,
                    base,
                }
            })
            .collect::<Vec<_>>();
        drop(scores);

        candidates.sort_by(|a, b| match (a.score_elapsed, b.score_elapsed) {
            (Some(left), Some(right)) => left
                .cmp(&right)
                .then_with(|| a.original_index.cmp(&b.original_index)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.original_index.cmp(&b.original_index),
        });
        candidates
    }

    async fn single_http_provider_bitswap_hedge_score_allows(&self, cid: &Cid, base: &Url) -> bool {
        let Some(min_score) = single_http_provider_bitswap_hedge_min_score() else {
            return true;
        };
        if !http_provider_scoring_enabled() {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "scoring_disabled",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        let Some(key) = http_provider_score_key(base) else {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_unkeyed",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        let mut scores = self.http_provider_scores.lock().await;
        let now = Instant::now();
        scores.retain(|_, score| {
            now.saturating_duration_since(score.last_seen) <= HTTP_PROVIDER_SCORE_TTL
        });
        let Some(score) = scores.get(&key) else {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_unscored",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        if score.ewma_elapsed < min_score {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_score_below_threshold",
                provider_scored = true,
                provider_score_ms = score.ewma_elapsed.as_millis(),
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        true
    }

    async fn single_http_provider_self_hedge_score_allows(&self, cid: &Cid, base: &Url) -> bool {
        self.single_http_provider_self_hedge_score_allows_with_min(
            cid,
            base,
            single_http_provider_self_hedge_min_score(),
        )
        .await
    }

    async fn single_http_provider_self_hedge_score_allows_with_min(
        &self,
        cid: &Cid,
        base: &Url,
        min_score: Option<Duration>,
    ) -> bool {
        let Some(min_score) = min_score else {
            return true;
        };
        if !http_provider_scoring_enabled() {
            tracing::info!(
                phase = "http_provider_self_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "scoring_disabled",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        let Some(key) = http_provider_score_key(base) else {
            tracing::info!(
                phase = "http_provider_self_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_unkeyed",
                provider_scored = false,
                provider_score_ms = 0u128,
                min_score_ms = min_score.as_millis()
            );
            return false;
        };
        let mut scores = self.http_provider_scores.lock().await;
        let now = Instant::now();
        scores.retain(|_, score| {
            now.saturating_duration_since(score.last_seen) <= HTTP_PROVIDER_SCORE_TTL
        });
        let Some(score) = scores.get(&key) else {
            return true;
        };
        if score.ewma_elapsed < min_score {
            tracing::info!(
                phase = "http_provider_self_hedge_skip",
                cid = %cid,
                provider = %base,
                reason = "provider_score_below_threshold",
                provider_scored = true,
                provider_score_ms = score.ewma_elapsed.as_millis(),
                min_score_ms = min_score.as_millis()
            );
            return false;
        }
        true
    }

    async fn record_http_provider_success(&self, base: &Url, elapsed: Duration) {
        if !http_provider_scoring_enabled() {
            return;
        }
        let Some(key) = http_provider_score_key(base) else {
            return;
        };
        let mut scores = self.http_provider_scores.lock().await;
        let now = Instant::now();
        scores.retain(|_, score| {
            now.saturating_duration_since(score.last_seen) <= HTTP_PROVIDER_SCORE_TTL
        });
        scores
            .entry(key)
            .and_modify(|score| {
                score.ewma_elapsed = weighted_duration_average(score.ewma_elapsed, elapsed, 3, 1);
                score.successes = score.successes.saturating_add(1);
                score.last_seen = now;
            })
            .or_insert(HttpProviderScore {
                ewma_elapsed: elapsed,
                successes: 1,
                last_seen: now,
            });
        prune_http_provider_scores(&mut scores, now);
    }

    async fn fetch_from_http_provider(
        &self,
        cid: &Cid,
        base: &Url,
    ) -> Result<(Block, HttpProviderResponseStats)> {
        let started = Instant::now();
        let url = base
            .join(&format!("/ipfs/{cid}?format=raw"))
            .map_err(RetrievalError::Url)?;
        let response = self
            .client
            .get(url)
            .header("accept", "application/vnd.ipld.raw")
            .send()
            .await?
            .error_for_status()?;
        let headers_elapsed = started.elapsed();
        let LimitedResponseBytes {
            bytes,
            stats: body_stats,
        } = limited_response_bytes(response, DEFAULT_MAX_BLOCK_SIZE).await?;
        let body_elapsed = started.elapsed();
        verify_block(cid, &bytes)?;
        let bytes = self
            .store_block_with_trace(*cid, bytes, "http_provider", true)
            .await?;
        Ok((
            Block::unchecked(*cid, bytes),
            HttpProviderResponseStats {
                response_bytes: body_stats.bytes_read,
                headers_elapsed,
                first_chunk_elapsed: body_stats
                    .first_chunk_elapsed
                    .map(|elapsed| headers_elapsed + elapsed),
                body_elapsed,
            },
        ))
    }

    async fn fetch_from_bitswap_providers(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
    ) -> Result<Block> {
        let peer_started = Instant::now();
        let BitswapProviderCandidates { mut peers, quality } =
            bitswap_peers_with_quality(providers).await;
        let provider_peer_count = peers.len();
        if provider_peer_count == 0 && !providers.is_empty() {
            tracing::info!(
                phase = "bitswap_provider_candidates_empty",
                cid = %cid,
                providers = %format_provider_candidates(providers)
            );
        }
        self.apply_successful_bitswap_peer_scores(&mut peers).await;
        let session_peer_count = self.insert_recent_bitswap_session_peers(&mut peers).await;
        let gateway_subresource = context
            .as_ref()
            .is_some_and(RetrievalRequestContext::gateway_subresource);
        let zero_http_direct_want_block_peer_count =
            maybe_force_zero_http_direct_want_block_peers(providers, &mut peers, context);
        let addr_stats = bitswap_peer_addr_stats(&peers);
        let trusted_peer_count = peers.iter().filter(|peer| peer.skip_want_have).count();
        tracing::info!(
            phase = "bitswap_peer_expand",
            cid = %cid,
            provider_count = providers.len(),
            peer_count = peers.len(),
            provider_peer_count,
            session_peer_count,
            trusted_peer_count,
            gateway_subresource,
            zero_http_direct_want_block_peer_count,
            tcp_addr_count = addr_stats.tcp,
            quic_addr_count = addr_stats.quic,
            ws_addr_count = addr_stats.ws,
            wss_addr_count = addr_stats.wss,
            dns_addr_count = addr_stats.dns,
            ip4_addr_count = addr_stats.ip4,
            ip6_addr_count = addr_stats.ip6,
            provider_addr_count = quality.provider_addr_count,
            expanded_provider_addr_count = quality.expanded_addr_count,
            supported_provider_addr_count = quality.supported_addr_count,
            rejected_provider_addr_count = quality.rejected_addr_count(),
            id_only_provider_count = quality.id_only_provider_count,
            invalid_provider_id_count = quality.invalid_provider_id_count,
            provider_without_supported_bitswap_addr_count = quality
                .provider_without_supported_bitswap_addr_count,
            unsupported_relay_addr_count = quality.unsupported_relay_addr_count,
            unsupported_webtransport_addr_count = quality.unsupported_webtransport_addr_count,
            unsupported_webrtc_addr_count = quality.unsupported_webrtc_addr_count,
            unsupported_certhash_addr_count = quality.unsupported_certhash_addr_count,
            unsupported_transport_addr_count = quality.unsupported_transport_addr_count,
            missing_peer_addr_count = quality.missing_peer_addr_count,
            unparsable_addr_count = quality.unparsable_addr_count,
            addr_with_relay_count = quality.addr_with_relay_count,
            addr_with_webtransport_count = quality.addr_with_webtransport_count,
            addr_with_webrtc_count = quality.addr_with_webrtc_count,
            addr_with_certhash_count = quality.addr_with_certhash_count,
            elapsed_ms = peer_started.elapsed().as_millis()
        );
        peers.retain(
            |peer| match self.store.is_bad_provider(&peer.id.to_string()) {
                Ok(false) => true,
                Ok(true) => {
                    tracing::info!(
                        phase = "bad_peer_skipped",
                        cid = %cid,
                        peer = %peer.id,
                        reason = "temporary bitswap suppression"
                    );
                    false
                }
                Err(_) => true,
            },
        );
        tracing::debug!(
            provider_count = providers.len(),
            peer_count = peers.len(),
            "bitswap provider candidates"
        );
        if peers.is_empty() {
            return Err(RetrievalError::NoBitswapProviders);
        }

        let bitswap_started = Instant::now();
        let peer_count = peers.len();
        let peers_for_record = peers.clone();
        let result = self.shared_bitswap_client().await?.fetch(*cid, peers).await;
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                self.mark_bitswap_timeout_peers(cid, &err, peer_count);
                tracing::info!(
                    phase = "bitswap_fetch",
                    cid = %cid,
                    peer_count,
                    provider_peer_count,
                    session_peer_count,
                    trusted_peer_count,
                    ok = false,
                    error = %err,
                    elapsed_ms = bitswap_started.elapsed().as_millis()
                );
                return Err(err);
            }
            Err(err) => {
                if is_bitswap_request_timeout(&err) {
                    let reset_client = self.reset_shared_bitswap_client().await;
                    tracing::info!(
                        phase = "bitswap_request_timeout",
                        cid = %cid,
                        peer_count,
                        trusted_peer_count,
                        timeout_ms = bitswap_request_timeout(peer_count, trusted_peer_count).as_millis(),
                        reset_client,
                        elapsed_ms = bitswap_started.elapsed().as_millis()
                    );
                }
                tracing::info!(
                    phase = "bitswap_fetch",
                    cid = %cid,
                    peer_count,
                    provider_peer_count,
                    session_peer_count,
                    trusted_peer_count,
                    ok = false,
                    error = %err,
                    elapsed_ms = bitswap_started.elapsed().as_millis()
                );
                return Err(err);
            }
        };
        let source_peer = result.source_peer;
        let source_peer_trusted = source_peer
            .map(|peer| {
                peers_for_record
                    .iter()
                    .any(|candidate| candidate.id == peer && candidate.skip_want_have)
            })
            .unwrap_or(false);
        let elapsed = bitswap_started.elapsed();
        tracing::info!(
            phase = "bitswap_fetch",
            cid = %cid,
            peer_count,
            provider_peer_count,
            session_peer_count,
            trusted_peer_count,
            ok = true,
            source_peer = source_peer.map(|peer| peer.to_string()).unwrap_or_default(),
            source_transport = result.source_transport.unwrap_or("unknown"),
            bitswap_delivery = result.delivery,
            source_peer_trusted,
            extra_blocks = result.extra_blocks.len(),
            bytes = result.requested_block.len(),
            elapsed_ms = elapsed.as_millis()
        );
        if let Some(peer) = source_peer {
            self.record_successful_bitswap_peer_from_peers(peer, &peers_for_record, elapsed)
                .await;
        }
        self.store_bitswap_result(cid, result).await
    }

    fn mark_bitswap_timeout_peers(
        &self,
        cid: &Cid,
        err: &RetrievalError,
        attempted_peer_count: usize,
    ) {
        let timeout_peers = bitswap_timeout_peers(err);
        if timeout_peers.is_empty() {
            return;
        }
        if suppress_bitswap_timeout_suppression(timeout_peers.len(), attempted_peer_count) {
            tracing::info!(
                phase = "bitswap_peer_timeout_suppressed",
                cid = %cid,
                timeout_peer_count = timeout_peers.len(),
                attempted_peer_count,
                reason = "broad_timeout"
            );
            return;
        }
        for peer in timeout_peers {
            tracing::info!(
                phase = "bitswap_peer_timeout",
                cid = %cid,
                peer = %peer,
                ttl_secs = BAD_BITSWAP_PROVIDER_TTL.as_secs()
            );
            let _ = self.store.mark_bad_provider(
                peer,
                "bitswap stream read timed out",
                BAD_BITSWAP_PROVIDER_TTL,
            );
        }
    }

    async fn shared_bitswap_client(&self) -> Result<SharedBitswapClient> {
        let mut client = self.bitswap.lock().await;
        if let Some(client) = client.as_ref() {
            return Ok(client.clone());
        }
        let spawned = SharedBitswapClient::spawn().await?;
        *client = Some(spawned.clone());
        Ok(spawned)
    }

    async fn reset_shared_bitswap_client(&self) -> bool {
        let mut client = self.bitswap.lock().await;
        if client.take().is_some() {
            tracing::info!(phase = "bitswap_client_reset");
            true
        } else {
            false
        }
    }

    async fn apply_successful_bitswap_peer_scores(&self, peers: &mut [BitswapPeer]) {
        let now = Instant::now();
        let mut successes = self.successful_bitswap_peers.lock().await;
        successes.retain(|_, success| {
            now.duration_since(success.seen_at) <= BITSWAP_SUCCESSFUL_PEER_TTL
        });
        for peer in peers.iter_mut() {
            peer.skip_want_have = successes.contains_key(&peer.id);
        }
        peers.sort_by(
            |left, right| match (successes.get(&left.id), successes.get(&right.id)) {
                (Some(left), Some(right)) => left
                    .last_latency
                    .cmp(&right.last_latency)
                    .then_with(|| right.seen_at.cmp(&left.seen_at)),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            },
        );
    }

    async fn insert_recent_bitswap_session_peers(&self, peers: &mut Vec<BitswapPeer>) -> usize {
        let recent_peers = self.recent_bitswap_peers_for_fetch().await;
        if recent_peers.is_empty() {
            return 0;
        }

        let mut session_only_peers = Vec::new();
        for mut recent in recent_peers {
            if let Some(existing) = peers.iter_mut().find(|peer| peer.id == recent.id) {
                existing.addrs.append(&mut recent.addrs);
                existing.addrs.sort_by_key(bitswap_addr_score);
                existing.addrs.dedup();
                existing.addrs.truncate(MAX_BITSWAP_ADDRS_PER_PEER);
            } else {
                session_only_peers.push(recent);
            }
        }

        let inserted = session_only_peers.len();
        let insert_at = peers
            .iter()
            .position(|peer| !peer.skip_want_have)
            .unwrap_or(peers.len());
        peers.splice(insert_at..insert_at, session_only_peers);
        peers.truncate(MAX_BITSWAP_PEERS_PER_BLOCK);
        inserted.min(peers.len().saturating_sub(insert_at))
    }

    async fn record_successful_bitswap_peer(
        &self,
        peer: PeerId,
        addrs: Vec<Multiaddr>,
        last_latency: Duration,
    ) {
        if let Some(max_latency) = bitswap_successful_peer_max_latency() {
            if last_latency > max_latency {
                tracing::info!(
                    phase = "bitswap_successful_peer_skipped",
                    peer = %peer,
                    reason = "latency_above_threshold",
                    latency_ms = last_latency.as_millis(),
                    max_latency_ms = max_latency.as_millis()
                );
                return;
            }
        }
        let mut successes = self.successful_bitswap_peers.lock().await;
        successes.insert(
            peer,
            SuccessfulBitswapPeer {
                seen_at: Instant::now(),
                addrs,
                last_latency,
            },
        );
    }

    async fn record_successful_bitswap_peer_from_peers(
        &self,
        peer: PeerId,
        peers: &[BitswapPeer],
        last_latency: Duration,
    ) {
        if let Some(candidate) = peers.iter().find(|candidate| candidate.id == peer) {
            self.record_successful_bitswap_peer(peer, candidate.addrs.clone(), last_latency)
                .await;
        }
    }

    async fn recent_bitswap_peers(&self) -> Vec<BitswapPeer> {
        let now = Instant::now();
        let mut successes = self.successful_bitswap_peers.lock().await;
        successes.retain(|_, success| {
            now.duration_since(success.seen_at) <= BITSWAP_SUCCESSFUL_PEER_TTL
                && !success.addrs.is_empty()
        });
        let mut peers = successes
            .iter()
            .map(|(id, success)| {
                (
                    *id,
                    success.seen_at,
                    success.last_latency,
                    success.addrs.clone(),
                )
            })
            .collect::<Vec<_>>();
        peers.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| right.1.cmp(&left.1)));
        peers
            .into_iter()
            .take(MAX_BITSWAP_SESSION_PEERS)
            .map(|(id, _seen_at, _last_latency, addrs)| BitswapPeer {
                id,
                addrs,
                skip_want_have: true,
                force_want_block: false,
            })
            .collect()
    }

    async fn recent_bitswap_peers_for_fetch(&self) -> Vec<BitswapPeer> {
        let mut peers = self.recent_bitswap_peers().await;
        peers.retain(
            |peer| match self.store.is_bad_provider(&peer.id.to_string()) {
                Ok(false) => true,
                Ok(true) => {
                    tracing::debug!(peer = %peer.id, "skipping temporarily bad Bitswap session peer");
                    false
                }
                Err(_) => true,
            },
        );
        peers
    }

    async fn wait_for_recent_bitswap_peers_for_fetch(
        &self,
        wait: Duration,
    ) -> (Vec<BitswapPeer>, u128) {
        let started = Instant::now();
        loop {
            let peers = self.recent_bitswap_peers_for_fetch().await;
            if !peers.is_empty() {
                return (peers, started.elapsed().as_millis());
            }
            let elapsed = started.elapsed();
            if elapsed >= wait {
                return (Vec::new(), elapsed.as_millis());
            }
            tokio::time::sleep((wait - elapsed).min(BITSWAP_SESSION_LATE_PEER_POLL)).await;
        }
    }

    async fn fetch_from_recent_bitswap_peers(
        &self,
        cid: &Cid,
        peers: Vec<BitswapPeer>,
    ) -> Result<Option<Block>> {
        if peers.is_empty() {
            return Ok(None);
        }

        let peer_count = peers.len();
        tracing::info!(
            phase = "bitswap_session_shortcut_start",
            cid = %cid,
            peer_count,
            trusted_peer_count = peer_count
        );
        let peers_for_record = peers.clone();
        let started = Instant::now();
        let fetch = async {
            let client = self.shared_bitswap_client().await?;
            client.fetch(*cid, peers).await
        };
        let result = match timeout(BITSWAP_SESSION_SHORTCUT_TIMEOUT, fetch).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(err))) => {
                tracing::info!(
                    phase = "bitswap_session_shortcut",
                    cid = %cid,
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    error = %err,
                    elapsed_ms = started.elapsed().as_millis()
                );
                return Ok(None);
            }
            Ok(Err(err)) => {
                tracing::info!(
                    phase = "bitswap_session_shortcut",
                    cid = %cid,
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    error = %err,
                    elapsed_ms = started.elapsed().as_millis()
                );
                return Ok(None);
            }
            Err(_) => {
                tracing::info!(
                    phase = "bitswap_session_shortcut",
                    cid = %cid,
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    timeout = true,
                    elapsed_ms = started.elapsed().as_millis()
                );
                self.mark_single_session_shortcut_timeout_peer(cid, &peers_for_record);
                return Ok(None);
            }
        };

        let elapsed = started.elapsed();
        tracing::info!(
            phase = "bitswap_session_shortcut",
            cid = %cid,
            peer_count,
            trusted_peer_count = peer_count,
            ok = true,
            source_peer = result.source_peer.map(|peer| peer.to_string()).unwrap_or_default(),
            source_transport = result.source_transport.unwrap_or("unknown"),
            bitswap_delivery = result.delivery,
            source_peer_trusted = true,
            extra_blocks = result.extra_blocks.len(),
            bytes = result.requested_block.len(),
            elapsed_ms = elapsed.as_millis()
        );
        if let Some(peer) = result.source_peer {
            self.record_successful_bitswap_peer_from_peers(peer, &peers_for_record, elapsed)
                .await;
        }
        self.store_bitswap_result(cid, result).await.map(Some)
    }

    async fn fetch_many_from_recent_bitswap_peers(
        &self,
        cids: Vec<Cid>,
    ) -> Result<Option<HashMap<Cid, Block>>> {
        if cids.len() < 2 {
            return Ok(None);
        }

        let peers = self.recent_bitswap_peers_for_fetch().await;
        if peers.is_empty() {
            tracing::info!(
                phase = "bitswap_session_range_batch",
                cids = %format_cids(&cids),
                cid_count = cids.len(),
                ok = false,
                outcome = "no_recent_peers",
                timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis()
            );
            return Ok(None);
        }

        let peer_count = peers.len();
        let peers_for_record = peers.clone();
        let started = Instant::now();
        tracing::info!(
            phase = "bitswap_session_range_batch_start",
            cids = %format_cids(&cids),
            cid_count = cids.len(),
            peer_count,
            trusted_peer_count = peer_count,
            timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis()
        );

        let fetch = async {
            let client = self.shared_bitswap_client().await?;
            client.fetch_many(cids.clone(), peers).await
        };
        let result = match timeout(BITSWAP_SESSION_RANGE_BATCH_TIMEOUT, fetch).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(err))) => {
                tracing::info!(
                    phase = "bitswap_session_range_batch",
                    cids = %format_cids(&cids),
                    cid_count = cids.len(),
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    error = %err,
                    timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                return Ok(None);
            }
            Ok(Err(err)) => {
                tracing::info!(
                    phase = "bitswap_session_range_batch",
                    cids = %format_cids(&cids),
                    cid_count = cids.len(),
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    error = %err,
                    timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                return Ok(None);
            }
            Err(_) => {
                tracing::info!(
                    phase = "bitswap_session_range_batch",
                    cids = %format_cids(&cids),
                    cid_count = cids.len(),
                    peer_count,
                    trusted_peer_count = peer_count,
                    ok = false,
                    timeout = true,
                    timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                return Ok(None);
            }
        };

        let elapsed = started.elapsed();
        if let Some(peer) = result.source_peer {
            self.record_successful_bitswap_peer_from_peers(peer, &peers_for_record, elapsed)
                .await;
        }
        let requested_block_count = result.requested_blocks.len();
        let extra_block_count = result.extra_blocks.len();
        let source_peer = result.source_peer;
        let source_transport = result.source_transport;
        let delivery = result.delivery;
        let blocks = self.store_bitswap_batch_result(result).await?;
        tracing::info!(
            phase = "bitswap_session_range_batch",
            cids = %format_cids(&cids),
            cid_count = cids.len(),
            peer_count,
            trusted_peer_count = peer_count,
            ok = true,
            source_peer = %source_peer.map(|peer| peer.to_string()).unwrap_or_default(),
            source_transport = source_transport.unwrap_or("unknown"),
            bitswap_delivery = delivery,
            source_peer_trusted = true,
            requested_blocks = requested_block_count,
            extra_blocks = extra_block_count,
            bytes = blocks.values().map(|block| block.data().len()).sum::<usize>(),
            elapsed_ms = elapsed.as_millis()
        );
        Ok(Some(blocks))
    }

    fn mark_single_session_shortcut_timeout_peer(&self, cid: &Cid, peers: &[BitswapPeer]) {
        let [peer] = peers else {
            if !peers.is_empty() {
                tracing::info!(
                    phase = "bitswap_peer_timeout_suppressed",
                    cid = %cid,
                    timeout_peer_count = peers.len(),
                    attempted_peer_count = peers.len(),
                    reason = "session_shortcut_timeout_broad"
                );
            }
            return;
        };
        tracing::info!(
            phase = "bitswap_peer_timeout",
            cid = %cid,
            peer = %peer.id,
            ttl_secs = BAD_BITSWAP_PROVIDER_TTL.as_secs(),
            reason = "session_shortcut_timeout"
        );
        let _ = self.store.mark_bad_provider(
            &peer.id.to_string(),
            "bitswap session shortcut timed out",
            BAD_BITSWAP_PROVIDER_TTL,
        );
    }

    async fn store_bitswap_result(&self, cid: &Cid, result: BitswapFetchResult) -> Result<Block> {
        let BitswapFetchResult {
            requested_block,
            extra_blocks,
            ..
        } = result;
        for (extra_cid, extra_data) in extra_blocks {
            if extra_cid != *cid {
                let _ = self
                    .store_block_with_trace(extra_cid, extra_data, "bitswap_extra", false)
                    .await;
            }
        }
        let bytes = self
            .store_block_with_trace(*cid, requested_block, "bitswap", true)
            .await?;
        Ok(Block::unchecked(*cid, bytes))
    }

    async fn store_bitswap_batch_result(
        &self,
        result: BitswapFetchBatchResult,
    ) -> Result<HashMap<Cid, Block>> {
        let BitswapFetchBatchResult {
            requested_blocks,
            extra_blocks,
            ..
        } = result;
        for (extra_cid, extra_data) in extra_blocks {
            if !requested_blocks.contains_key(&extra_cid) {
                let _ = self
                    .store_block_with_trace(extra_cid, extra_data, "bitswap_extra", false)
                    .await;
            }
        }

        let mut blocks = HashMap::with_capacity(requested_blocks.len());
        for (cid, data) in requested_blocks {
            let bytes = self
                .store_block_with_trace(cid, data, "bitswap", true)
                .await?;
            blocks.insert(cid, Block::unchecked(cid, bytes));
        }
        Ok(blocks)
    }

    async fn store_block_with_trace(
        &self,
        cid: Cid,
        bytes: Vec<u8>,
        source: &'static str,
        required: bool,
    ) -> Result<Vec<u8>> {
        let started = Instant::now();
        let byte_count = bytes.len();
        let store = self.store.clone();
        let (bytes, result) = tokio::task::spawn_blocking(move || {
            let result = store.put_block(&cid, &bytes);
            (bytes, result)
        })
        .await
        .map_err(|err| RetrievalError::Bitswap(format!("block store task failed: {err}")))?;
        match result {
            Ok(()) => {
                tracing::info!(
                    phase = "block_store_put",
                    cid = %cid,
                    source,
                    required,
                    ok = true,
                    bytes = byte_count,
                    elapsed_ms = started.elapsed().as_millis()
                );
                Ok(bytes)
            }
            Err(err) => {
                tracing::info!(
                    phase = "block_store_put",
                    cid = %cid,
                    source,
                    required,
                    ok = false,
                    bytes = byte_count,
                    error = %err,
                    elapsed_ms = started.elapsed().as_millis()
                );
                Err(err.into())
            }
        }
    }
}

fn retrieval_source_label(source: RetrievalSource) -> &'static str {
    match source {
        RetrievalSource::Cache => "cache",
        RetrievalSource::HttpProvider => "http_provider",
        RetrievalSource::Bitswap => "bitswap",
    }
}

fn provider_http_url_count(providers: &[Provider]) -> usize {
    providers
        .iter()
        .map(|provider| provider.http_urls.len())
        .sum()
}

fn single_http_provider_base(providers: &[Provider]) -> Option<&Url> {
    let mut bases = providers
        .iter()
        .flat_map(|provider| provider.http_urls.iter());
    let base = bases.next()?;
    if bases.next().is_some() {
        None
    } else {
        Some(base)
    }
}

fn bitswap_session_post_lookup_grace(providers: &[Provider]) -> Duration {
    let single_http_override =
        std::env::var_os(BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE_MS_ENV);
    let single_http_override = single_http_override
        .as_ref()
        .map(|value| value.to_string_lossy());
    let multi_http_override = std::env::var_os(BITSWAP_SESSION_MULTI_HTTP_POST_LOOKUP_GRACE_MS_ENV);
    let multi_http_override = multi_http_override
        .as_ref()
        .map(|value| value.to_string_lossy());

    bitswap_session_post_lookup_grace_from_env_value(
        providers,
        single_http_override.as_deref(),
        multi_http_override.as_deref(),
    )
}

fn bitswap_session_post_lookup_grace_from_env_value(
    providers: &[Provider],
    single_http_grace_ms: Option<&str>,
    multi_http_grace_ms: Option<&str>,
) -> Duration {
    match provider_http_url_count(providers) {
        0 => BITSWAP_SESSION_POST_LOOKUP_GRACE,
        1 => post_lookup_grace_from_env_value(
            single_http_grace_ms,
            BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE,
        ),
        _ => {
            post_lookup_grace_from_env_value(multi_http_grace_ms, BITSWAP_SESSION_POST_LOOKUP_GRACE)
        }
    }
}

fn post_lookup_grace_from_env_value(value: Option<&str>, default: Duration) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn shared_block_fetch_result(
    result: Arc<SharedBlockFetchResult>,
) -> Result<(Block, RetrievalSource)> {
    match result.as_ref() {
        Ok((block, source)) => Ok((block.clone(), *source)),
        Err(err) => Err(RetrievalError::Bitswap(format!(
            "coalesced block fetch failed: {err}"
        ))),
    }
}

#[derive(Clone)]
pub struct FetchingBlockProvider {
    store: SqliteBlockStore,
    retriever: HttpRetriever,
    stats: Arc<RetrievalStatsInner>,
}

impl FetchingBlockProvider {
    pub fn new(store: SqliteBlockStore, routing: impl Into<ProviderRoutingClient>) -> Self {
        let retriever = HttpRetriever::new(routing, store.clone());
        Self {
            store,
            retriever,
            stats: Arc::new(RetrievalStatsInner::default()),
        }
    }

    pub fn stats(&self) -> RetrievalStats {
        self.stats.snapshot()
    }

    async fn get_block_ranges_async(
        &self,
        ranges: Vec<(Cid, u64, u64)>,
    ) -> CoreResult<Vec<Option<Vec<u8>>>> {
        self.get_block_ranges_async_inner(ranges, bitswap_session_range_batch_enabled())
            .await
    }

    async fn get_block_ranges_async_inner(
        &self,
        ranges: Vec<(Cid, u64, u64)>,
        enable_session_batch: bool,
    ) -> CoreResult<Vec<Option<Vec<u8>>>> {
        let range_count = ranges.len();
        let mut results = vec![None; ranges.len()];
        let mut misses = Vec::new();
        for (index, (cid, start, end)) in ranges.iter().copied().enumerate() {
            let cache_started = Instant::now();
            match self
                .store
                .get_range(&cid, start, end)
                .map_err(|err| CoreError::Storage(err.to_string()))?
            {
                Some(bytes) => {
                    tracing::info!(
                        phase = "block_store_get_range",
                        cid = %cid,
                        cache_hit = true,
                        elapsed_ms = cache_started.elapsed().as_millis()
                    );
                    self.stats.record(RetrievalSource::Cache);
                    results[index] = Some(bytes);
                }
                None => {
                    tracing::info!(
                        phase = "block_store_get_range",
                        cid = %cid,
                        cache_hit = false,
                        batch = true,
                        elapsed_ms = cache_started.elapsed().as_millis()
                    );
                    misses.push(MissingBlockRange {
                        index,
                        cid,
                        start,
                        end,
                    });
                }
            }
        }

        let uncached_range_count = misses.len();
        let mut remaining = Vec::new();
        if enable_session_batch && uncached_range_count > 1 {
            for chunk in misses.chunks(MAX_BITSWAP_SESSION_RANGE_BATCH_CIDS) {
                let mut cids = Vec::new();
                for missing in chunk {
                    if !cids.contains(&missing.cid) {
                        cids.push(missing.cid);
                    }
                }
                let batch_started = Instant::now();
                match self
                    .retriever
                    .fetch_many_from_recent_bitswap_peers(cids)
                    .await
                {
                    Ok(Some(blocks)) => {
                        let batch_elapsed_ms = batch_started.elapsed().as_millis();
                        for missing in chunk {
                            let Some(block) = blocks.get(&missing.cid) else {
                                remaining.push(*missing);
                                continue;
                            };
                            self.stats.record(RetrievalSource::Bitswap);
                            let range_len = if missing.start <= missing.end {
                                missing.end.saturating_sub(missing.start).saturating_add(1)
                            } else {
                                0
                            };
                            tracing::info!(
                                phase = "block_range_batch_fetch",
                                cid = %missing.cid,
                                source = "bitswap_batch",
                                range_start = missing.start,
                                range_end = missing.end,
                                range_len,
                                range_count,
                                uncached_range_count,
                                elapsed_ms = batch_elapsed_ms
                            );
                            results[missing.index] =
                                Some(block_data_range(block.data(), missing.start, missing.end));
                        }
                    }
                    Ok(None) => remaining.extend_from_slice(chunk),
                    Err(err) => {
                        tracing::info!(
                            phase = "bitswap_session_range_batch",
                            cids = %format_cids(&chunk.iter().map(|missing| missing.cid).collect::<Vec<_>>()),
                            cid_count = chunk.len(),
                            ok = false,
                            error = %err,
                            fallback = true
                        );
                        remaining.extend_from_slice(chunk);
                    }
                }
            }
        } else {
            remaining = misses;
        }

        let mut fetches = Vec::new();
        let context = current_retrieval_request_context();
        for MissingBlockRange {
            index,
            cid,
            start,
            end,
        } in remaining
        {
            let retriever = self.retriever.clone();
            fetches.push(async move {
                let fetch_started = Instant::now();
                let fetched = retriever
                    .fetch_block_with_source_with_context(&cid, context)
                    .await;
                (
                    index,
                    cid,
                    start,
                    end,
                    fetch_started.elapsed().as_millis(),
                    fetched,
                )
            });
        }

        for (index, cid, start, end, elapsed_ms, fetched) in join_all(fetches).await {
            let (block, source) = fetched.map_err(|err| CoreError::Storage(err.to_string()))?;
            self.stats.record(source);
            let range_len = if start <= end {
                end.saturating_sub(start).saturating_add(1)
            } else {
                0
            };
            tracing::info!(
                phase = "block_range_batch_fetch",
                cid = %cid,
                source = retrieval_source_label(source),
                range_start = start,
                range_end = end,
                range_len,
                range_count,
                uncached_range_count,
                elapsed_ms
            );
            results[index] = Some(block_data_range(block.data(), start, end));
        }
        Ok(results)
    }
}

impl BlockProvider for FetchingBlockProvider {
    fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
        let cache_started = Instant::now();
        if let Some(block) = self
            .store
            .get(cid)
            .map_err(|err| CoreError::Storage(err.to_string()))?
        {
            tracing::info!(
                phase = "block_store_get",
                cid = %cid,
                cache_hit = true,
                elapsed_ms = cache_started.elapsed().as_millis()
            );
            self.stats.record(RetrievalSource::Cache);
            return Ok(Some(block));
        }
        tracing::info!(
            phase = "block_store_get",
            cid = %cid,
            cache_hit = false,
            elapsed_ms = cache_started.elapsed().as_millis()
        );

        let context = current_retrieval_request_context();
        let fetched = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(
                    self.retriever
                        .fetch_block_with_source_with_context(cid, context),
                )
            }),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| CoreError::Storage(err.to_string()))?;
                runtime.block_on(
                    self.retriever
                        .fetch_block_with_source_with_context(cid, context),
                )
            }
        };

        match fetched {
            Ok((block, source)) => {
                self.stats.record(source);
                Ok(Some(block))
            }
            Err(err) => Err(CoreError::Storage(err.to_string())),
        }
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
        let cache_started = Instant::now();
        if let Some(bytes) = self
            .store
            .get_range(cid, start, end)
            .map_err(|err| CoreError::Storage(err.to_string()))?
        {
            tracing::info!(
                phase = "block_store_get_range",
                cid = %cid,
                cache_hit = true,
                elapsed_ms = cache_started.elapsed().as_millis()
            );
            self.stats.record(RetrievalSource::Cache);
            return Ok(Some(bytes));
        }
        tracing::info!(
            phase = "block_store_get_range",
            cid = %cid,
            cache_hit = false,
            elapsed_ms = cache_started.elapsed().as_millis()
        );

        let context = current_retrieval_request_context();
        let fetched = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(
                    self.retriever
                        .fetch_block_with_source_with_context(cid, context),
                )
            }),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| CoreError::Storage(err.to_string()))?;
                runtime.block_on(
                    self.retriever
                        .fetch_block_with_source_with_context(cid, context),
                )
            }
        };

        match fetched {
            Ok((block, source)) => {
                self.stats.record(source);
                Ok(Some(block_data_range(block.data(), start, end)))
            }
            Err(err) => Err(CoreError::Storage(err.to_string())),
        }
    }

    fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> CoreResult<Vec<Option<Vec<u8>>>> {
        if ranges.len() <= 1 {
            return ranges
                .iter()
                .map(|(cid, start, end)| self.get_block_range(cid, *start, *end))
                .collect();
        }
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.get_block_ranges_async(ranges.to_vec()))
            }),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| CoreError::Storage(err.to_string()))?;
                runtime.block_on(self.get_block_ranges_async(ranges.to_vec()))
            }
        }
    }

    fn retain_block(&self, cid: &Cid) -> CoreResult<()> {
        self.store.retain_block(cid)
    }

    fn release_block(&self, cid: &Cid) {
        self.store.release_block(cid);
    }
}

#[derive(Clone)]
struct BitswapPeer {
    id: PeerId,
    addrs: Vec<Multiaddr>,
    skip_want_have: bool,
    force_want_block: bool,
}

struct SuccessfulBitswapPeer {
    seen_at: Instant,
    addrs: Vec<Multiaddr>,
    last_latency: Duration,
}

struct HttpProviderScore {
    ewma_elapsed: Duration,
    successes: u64,
    last_seen: Instant,
}

struct HttpProviderResponseStats {
    response_bytes: usize,
    headers_elapsed: Duration,
    first_chunk_elapsed: Option<Duration>,
    body_elapsed: Duration,
}

#[derive(Clone)]
struct ScoredHttpProviderBase {
    original_index: usize,
    score_elapsed: Option<Duration>,
    base: Url,
}

struct HttpProviderCandidateResult {
    provider_index: usize,
    attempt_index: usize,
    original_provider_index: usize,
    score_elapsed: Option<Duration>,
    base: Url,
    result: Result<Block>,
}

fn http_provider_score_key(base: &Url) -> Option<String> {
    let host = base.host_str()?;
    let mut key = format!("{}://{}", base.scheme(), host);
    if let Some(port) = base.port_or_known_default() {
        key.push(':');
        key.push_str(&port.to_string());
    }
    Some(key)
}

fn http_provider_scoring_enabled() -> bool {
    std::env::var_os(DISABLE_HTTP_PROVIDER_SCORING_ENV).is_none()
}

fn single_http_provider_self_hedge_enabled() -> bool {
    std::env::var_os(DISABLE_SINGLE_HTTP_SELF_HEDGE_ENV).is_none()
}

fn single_http_provider_self_hedge_after() -> Duration {
    std::env::var_os(SINGLE_HTTP_SELF_HEDGE_AFTER_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(SINGLE_HTTP_PROVIDER_SELF_HEDGE_AFTER)
}

fn single_http_provider_self_hedge_min_score() -> Option<Duration> {
    std::env::var_os(SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn single_http_provider_bitswap_hedge_enabled() -> bool {
    std::env::var_os(ENABLE_SINGLE_HTTP_BITSWAP_HEDGE_ENV).is_some()
}

fn max_concurrent_http_provider_fetches() -> usize {
    let override_value = std::env::var_os(MAX_CONCURRENT_HTTP_PROVIDER_FETCHES_ENV);
    max_concurrent_http_provider_fetches_from_env_value(
        override_value.as_deref().and_then(|value| value.to_str()),
    )
}

fn max_concurrent_http_provider_fetches_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(MAX_CONCURRENT_HTTP_PROVIDER_FETCHES)
}

fn single_http_post_lookup_race_enabled() -> bool {
    std::env::var_os(DISABLE_SINGLE_HTTP_POST_LOOKUP_RACE_ENV).is_none()
}

fn multi_http_post_lookup_race_enabled() -> bool {
    multi_http_post_lookup_race_enabled_from_env_value(
        std::env::var_os(DISABLE_MULTI_HTTP_POST_LOOKUP_RACE_ENV).is_some(),
        std::env::var_os(ENABLE_MULTI_HTTP_POST_LOOKUP_RACE_ENV).is_some(),
    )
}

fn multi_http_post_lookup_race_enabled_from_env_value(disabled: bool, _enabled: bool) -> bool {
    !disabled
}

fn explicit_post_lookup_race_enabled_for_width(http_provider_count: usize) -> bool {
    (http_provider_count == 0 && zero_http_post_lookup_race_enabled())
        || (http_provider_count > 1 && multi_http_post_lookup_race_enabled())
}

fn single_http_post_lookup_race_min_score() -> Option<Duration> {
    std::env::var_os(SINGLE_HTTP_POST_LOOKUP_RACE_MIN_SCORE_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn multi_http_fast_post_lookup_race_max_score() -> Option<Duration> {
    multi_http_fast_post_lookup_race_max_score_from_env_value(
        std::env::var_os(DISABLE_MULTI_HTTP_FAST_POST_LOOKUP_RACE_ENV).is_some(),
        std::env::var_os(MULTI_HTTP_FAST_POST_LOOKUP_RACE_MAX_SCORE_MS_ENV)
            .as_ref()
            .map(|value| value.to_string_lossy()),
    )
}

fn multi_http_fast_post_lookup_race_max_score_from_env_value(
    disabled: bool,
    value: Option<std::borrow::Cow<'_, str>>,
) -> Option<Duration> {
    if disabled {
        return None;
    }
    Some(
        value
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(MULTI_HTTP_FAST_POST_LOOKUP_RACE_MAX_SCORE),
    )
}

fn zero_http_post_lookup_race_enabled() -> bool {
    std::env::var_os(ENABLE_ZERO_HTTP_POST_LOOKUP_RACE_ENV).is_some()
}

fn bitswap_zero_http_direct_want_block_peers(
    context: Option<RetrievalRequestContext>,
) -> Option<usize> {
    bitswap_zero_http_direct_want_block_peers_from_values(
        std::env::var_os(ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_ENV).is_some(),
        std::env::var_os(BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
        context,
    )
}

fn bitswap_zero_http_direct_want_block_peers_from_env_value(value: Option<&str>) -> Option<usize> {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn bitswap_zero_http_direct_want_block_peers_from_values(
    subresource_enabled: bool,
    override_value: Option<&str>,
    context: Option<RetrievalRequestContext>,
) -> Option<usize> {
    bitswap_zero_http_direct_want_block_peers_from_env_value(override_value).or_else(|| {
        if !subresource_enabled {
            return None;
        }
        context
            .filter(RetrievalRequestContext::gateway_subresource)
            .map(|_| BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS)
    })
}

fn bitswap_session_shortcut_grace() -> Duration {
    let override_value = std::env::var_os(BITSWAP_SESSION_SHORTCUT_GRACE_MS_ENV);
    bitswap_session_shortcut_grace_from_env_value(
        override_value.as_deref().and_then(|value| value.to_str()),
    )
}

fn bitswap_session_shortcut_grace_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(BITSWAP_SESSION_SHORTCUT_GRACE)
}

fn bitswap_session_pre_lookup_grace() -> Duration {
    let override_value = std::env::var_os(BITSWAP_SESSION_PRE_LOOKUP_GRACE_MS_ENV);
    bitswap_session_pre_lookup_grace_from_env_value(
        override_value.as_deref().and_then(|value| value.to_str()),
    )
}

fn bitswap_session_pre_lookup_grace_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(BITSWAP_SESSION_PRE_LOOKUP_GRACE)
}

fn bitswap_successful_peer_max_latency() -> Option<Duration> {
    std::env::var_os(BITSWAP_SUCCESSFUL_PEER_MAX_LATENCY_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn single_http_provider_bitswap_hedge_min_score() -> Option<Duration> {
    std::env::var_os(SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn bitswap_session_range_batch_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_SESSION_RANGE_BATCH_ENV).is_some()
}

fn bitswap_incoming_batch_partial_grace() -> Duration {
    let override_value = std::env::var_os(BITSWAP_INCOMING_BATCH_PARTIAL_GRACE_MS_ENV);
    let override_value = override_value.as_ref().map(|value| value.to_string_lossy());
    bitswap_incoming_batch_partial_grace_from_env_value(override_value.as_deref())
}

fn bitswap_incoming_batch_partial_grace_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(BITSWAP_INCOMING_BATCH_PARTIAL_GRACE)
}

fn bitswap_connection_ready_timeout() -> Duration {
    let override_value = std::env::var_os(BITSWAP_CONNECTION_READY_TIMEOUT_MS_ENV);
    let override_value = override_value.as_ref().map(|value| value.to_string_lossy());
    bitswap_connection_ready_timeout_from_env_value(override_value.as_deref())
}

fn bitswap_connection_ready_timeout_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(BITSWAP_CONNECTION_READY_TIMEOUT)
}

fn has_bitswap_provider_candidate(providers: &[Provider]) -> bool {
    providers.iter().any(|provider| {
        provider.id.is_some()
            && provider.addrs.iter().any(|addr| {
                let mut parts = addr.split('/').filter(|part| !part.is_empty());
                !parts.any(|part| matches!(part, "http" | "https"))
            })
    })
}

enum SingleHttpProviderBitswapHedgeResult {
    HttpCandidate(HttpProviderCandidateResult),
    Bitswap(Result<Block>),
}

fn push_single_http_bitswap_hedge(
    pending: &mut FuturesUnordered<BoxFuture<'static, SingleHttpProviderBitswapHedgeResult>>,
    retriever: HttpRetriever,
    cid: Cid,
    providers: Vec<Provider>,
    provider_count: usize,
    started: Instant,
    reason: &'static str,
) {
    tracing::info!(
        phase = "http_provider_bitswap_hedge",
        cid = %cid,
        provider_count,
        timeout_ms = SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER.as_millis(),
        reason,
        elapsed_ms = started.elapsed().as_millis()
    );
    pending.push(
        async move {
            SingleHttpProviderBitswapHedgeResult::Bitswap(
                retriever
                    .fetch_from_bitswap_providers(&cid, &providers, None)
                    .await,
            )
        }
        .boxed(),
    );
}

fn weighted_duration_average(
    old: Duration,
    new: Duration,
    old_weight: u128,
    new_weight: u128,
) -> Duration {
    let total_weight = old_weight.saturating_add(new_weight).max(1);
    let nanos = old
        .as_nanos()
        .saturating_mul(old_weight)
        .saturating_add(new.as_nanos().saturating_mul(new_weight))
        / total_weight;
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

fn prune_http_provider_scores(scores: &mut HashMap<String, HttpProviderScore>, now: Instant) {
    if scores.len() <= MAX_HTTP_PROVIDER_SCORE_ENTRIES {
        return;
    }

    let mut entries = scores
        .iter()
        .map(|(key, score)| (key.clone(), now.saturating_duration_since(score.last_seen)))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(_, age)| *age);
    let retain_keys = entries
        .into_iter()
        .take(MAX_HTTP_PROVIDER_SCORE_ENTRIES)
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    scores.retain(|key, _| retain_keys.contains(key));
}

struct BitswapPeerTarget {
    id: PeerId,
    addrs: Vec<Multiaddr>,
    skip_want_have: bool,
    force_want_block: bool,
    connection_ready: Option<oneshot::Receiver<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitswapPeerFailureKind {
    ConnectionTimeout,
    ReadTimeout,
    Other,
}

#[derive(Debug, Clone)]
struct BitswapProtocolFailure {
    kind: BitswapPeerFailureKind,
    detail: String,
}

impl BitswapProtocolFailure {
    fn other(detail: String) -> Self {
        Self {
            kind: BitswapPeerFailureKind::Other,
            detail,
        }
    }

    fn read_timeout(detail: String) -> Self {
        Self {
            kind: BitswapPeerFailureKind::ReadTimeout,
            detail,
        }
    }
}

#[derive(Debug, Clone)]
struct BitswapPeerFailure {
    id: PeerId,
    kind: BitswapPeerFailureKind,
    detail: String,
}

#[derive(Clone)]
struct BitswapFetchResult {
    requested_block: Vec<u8>,
    extra_blocks: Vec<(Cid, Vec<u8>)>,
    source_peer: Option<PeerId>,
    source_transport: Option<&'static str>,
    delivery: &'static str,
}

#[derive(Clone, Debug)]
struct BitswapFetchBatchResult {
    requested_blocks: HashMap<Cid, Vec<u8>>,
    extra_blocks: Vec<(Cid, Vec<u8>)>,
    source_peer: Option<PeerId>,
    source_transport: Option<&'static str>,
    delivery: &'static str,
}

struct BitswapFetchResults {
    requested_blocks: HashMap<Cid, Vec<u8>>,
    extra_blocks: Vec<(Cid, Vec<u8>)>,
}

#[derive(Clone)]
struct SharedBitswapClient {
    commands: mpsc::Sender<BitswapCommand>,
}

struct BitswapCommand {
    cids: Vec<Cid>,
    peers: Vec<BitswapPeer>,
    sent_at: Instant,
    respond: oneshot::Sender<Result<BitswapFetchBatchResult>>,
}

struct PendingIncomingBitswapResult {
    sent_at: Instant,
    sender: mpsc::UnboundedSender<BitswapFetchBatchResult>,
}

struct IncomingBitswapRead {
    peer: PeerId,
    stream: Libp2pStream,
    result: io::Result<Vec<ReceivedBitswapBlock>>,
    elapsed_ms: u128,
    timed_out: bool,
}

type DialErrorLog = Arc<tokio::sync::Mutex<HashMap<PeerId, Vec<String>>>>;
type PeerTransportLog = Arc<tokio::sync::Mutex<HashMap<PeerId, PeerTransportState>>>;

#[derive(Default)]
struct PeerTransportState {
    counts: BTreeMap<&'static str, usize>,
    current: Option<&'static str>,
}

struct ConnectionErrorBackoff {
    count: usize,
    last_seen: Instant,
    suppress_until: Option<Instant>,
    class: &'static str,
}

impl SharedBitswapClient {
    async fn spawn() -> Result<Self> {
        let swarm = build_bitswap_swarm().await?;
        let mut control = swarm.behaviour().stream.new_control();
        let incoming = accept_bitswap_streams(&mut control)?;
        let (commands, receiver) = mpsc::channel(64);
        tokio::spawn(run_shared_bitswap_swarm(swarm, control, incoming, receiver));
        Ok(Self { commands })
    }

    async fn fetch(&self, cid: Cid, peers: Vec<BitswapPeer>) -> Result<Result<BitswapFetchResult>> {
        let batch = self.fetch_many(vec![cid], peers).await?;
        Ok(batch.map(|mut result| BitswapFetchResult {
            requested_block: result
                .requested_blocks
                .remove(&cid)
                .expect("single-CID Bitswap batch omitted requested block"),
            extra_blocks: result.extra_blocks,
            source_peer: result.source_peer,
            source_transport: result.source_transport,
            delivery: result.delivery,
        }))
    }

    async fn fetch_many(
        &self,
        cids: Vec<Cid>,
        peers: Vec<BitswapPeer>,
    ) -> Result<Result<BitswapFetchBatchResult>> {
        if cids.is_empty() {
            return Err(RetrievalError::Bitswap(
                "shared bitswap batch cannot be empty".into(),
            ));
        }
        let peer_count = peers.len();
        let trusted_peer_count = peers.iter().filter(|peer| peer.skip_want_have).count();
        let request_timeout = bitswap_request_timeout(peer_count, trusted_peer_count);
        let stream_read_timeout = bitswap_stream_read_timeout(peer_count, trusted_peer_count);
        let (want_block_target_count, want_have_target_count) =
            bitswap_request_target_mode_counts(&peers);
        let target_summary =
            tracing::enabled!(tracing::Level::INFO).then(|| format_bitswap_peers(&peers));
        let cid_count = cids.len();
        let cid_summary = tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
        let (respond, response) = oneshot::channel();
        self.commands
            .send(BitswapCommand {
                cids,
                peers,
                sent_at: Instant::now(),
                respond,
            })
            .await
            .map_err(|_| RetrievalError::Bitswap("shared bitswap swarm stopped".into()))?;

        let wait_started = Instant::now();
        match timeout(request_timeout, response).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(RetrievalError::Bitswap(
                "shared bitswap response dropped".into(),
            )),
            Err(_) => {
                tracing::info!(
                    phase = "bitswap_request_timeout_detail",
                    cids = %cid_summary.as_deref().unwrap_or(""),
                    cid_count,
                    peer_count,
                    trusted_peer_count,
                    want_block_target_count,
                    want_have_target_count,
                    timeout_ms = request_timeout.as_millis(),
                    stream_read_timeout_ms = stream_read_timeout.as_millis(),
                    elapsed_ms = wait_started.elapsed().as_millis(),
                    targets = %target_summary.as_deref().unwrap_or("")
                );
                Err(RetrievalError::BitswapTimeout)
            }
        }
    }
}

fn bitswap_request_target_mode_counts(peers: &[BitswapPeer]) -> (usize, usize) {
    let has_multiple_peers = peers.len() > 1;
    let mut direct_untrusted_want_block_count = 0usize;
    let mut want_block_count = 0usize;
    let mut want_have_count = 0usize;
    for peer in peers {
        if bitswap_prefer_want_have(
            has_multiple_peers,
            peer.skip_want_have,
            peer.force_want_block,
            &mut direct_untrusted_want_block_count,
        ) {
            want_have_count += 1;
        } else {
            want_block_count += 1;
        }
    }
    (want_block_count, want_have_count)
}

fn bitswap_request_timeout(peer_count: usize, trusted_peer_count: usize) -> Duration {
    if trusted_peer_count > 0 && peer_count > trusted_peer_count {
        BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT
    } else {
        BITSWAP_REQUEST_TIMEOUT
    }
}

fn bitswap_stream_read_timeout(peer_count: usize, trusted_peer_count: usize) -> Duration {
    if peer_count == 1 && trusted_peer_count == 0 {
        BITSWAP_SINGLE_UNTRUSTED_STREAM_READ_TIMEOUT
    } else {
        BITSWAP_STREAM_READ_TIMEOUT
    }
}

#[derive(Clone, Copy)]
struct BitswapRequestTimeouts {
    want_have: Duration,
    stream_read: Duration,
}

async fn read_incoming_bitswap_stream(
    peer: PeerId,
    mut stream: Libp2pStream,
) -> IncomingBitswapRead {
    let (result, elapsed_ms, timed_out) =
        read_incoming_bitswap_blocks(&mut stream, BITSWAP_INCOMING_STREAM_READ_TIMEOUT).await;
    IncomingBitswapRead {
        peer,
        stream,
        result,
        elapsed_ms,
        timed_out,
    }
}

async fn read_incoming_bitswap_blocks<T>(
    stream: &mut T,
    read_timeout: Duration,
) -> (io::Result<Vec<ReceivedBitswapBlock>>, u128, bool)
where
    T: AsyncRead + Unpin,
{
    let started = Instant::now();
    match timeout(read_timeout, read_bitswap_blocks(stream)).await {
        Ok(result) => (result, started.elapsed().as_millis(), false),
        Err(_) => (
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "incoming bitswap stream read timed out",
            )),
            started.elapsed().as_millis(),
            true,
        ),
    }
}

async fn run_shared_bitswap_swarm(
    mut swarm: libp2p::Swarm<BitswapBehaviour>,
    control: StreamControl,
    incoming: Vec<IncomingStreams>,
    mut commands: mpsc::Receiver<BitswapCommand>,
) {
    let mut incoming = select_all(incoming);
    let mut fetches = FuturesUnordered::<BoxFuture<'static, Vec<Cid>>>::new();
    let mut incoming_reads = FuturesUnordered::<BoxFuture<'static, IncomingBitswapRead>>::new();
    let mut pending_incoming = HashMap::<Cid, Vec<PendingIncomingBitswapResult>>::new();
    let mut pending_counts = HashMap::<Cid, usize>::new();
    let mut connected_peers = HashMap::<PeerId, usize>::new();
    let mut connection_waiters = HashMap::<PeerId, Vec<oneshot::Sender<()>>>::new();
    let mut connection_wait_started = HashMap::<PeerId, Instant>::new();
    let mut connection_error_backoff = HashMap::<PeerId, ConnectionErrorBackoff>::new();
    let dial_errors = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let peer_transports = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command_queued_ms = command.sent_at.elapsed().as_millis();
                prune_connection_waiters(&mut connection_waiters, &mut connection_wait_started);
                prune_connection_error_backoff(&mut connection_error_backoff, Instant::now());
                let cids = command.cids;
                let cid_count = cids.len();
                let primary_cid = cids[0];
                let cid_summary =
                    tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
                let (incoming_result, incoming_results) = mpsc::unbounded_channel();
                for cid in &cids {
                    pending_incoming.entry(*cid).or_default().push(
                        PendingIncomingBitswapResult {
                            sent_at: command.sent_at,
                            sender: incoming_result.clone(),
                        },
                    );
                    *pending_counts.entry(*cid).or_default() += 1;
                }

                let mut peer_plans = Vec::new();
                let mut dial_candidates = Vec::new();
                for peer in command.peers {
                    if let Some(remaining_ms) = connection_error_backoff_remaining_ms(
                        &connection_error_backoff,
                        &peer.id,
                        Instant::now(),
                    ) {
                        tracing::info!(
                            phase = "bitswap_connection_error_peer_skipped",
                            cid = %primary_cid,
                            cids = %cid_summary.as_deref().unwrap_or(""),
                            cid_count,
                            peer = %peer.id,
                            remaining_ms
                        );
                        continue;
                    }
                    tracing::debug!(peer = %peer.id, addrs = ?peer.addrs, "adding bitswap peer");
                    let already_connected = connected_peers.contains_key(&peer.id);
                    let already_pending = !already_connected && connection_waiters.contains_key(&peer.id);
                    let should_dial = should_start_bitswap_dial(
                        &peer.id,
                        &connected_peers,
                        &connection_waiters,
                    );
                    for addr in &peer.addrs {
                        swarm.add_peer_address(peer.id, addr.clone());
                    }
                    if should_dial {
                        dial_candidates.push(peer.clone());
                    }
                    peer_plans.push((peer, already_connected, already_pending));
                }
                let (dial_addrs, suppressed_dial_addr_count) =
                    limited_interleaved_bitswap_dials(&dial_candidates);
                let scheduled_dial_peers = dial_addrs
                    .iter()
                    .map(|(peer, _)| *peer)
                    .collect::<BTreeSet<_>>();
                let candidate_dial_peer_count = dial_candidates.len();
                let suppressed_dial_peer_count = dial_candidates
                    .iter()
                    .filter(|peer| !scheduled_dial_peers.contains(&peer.id))
                    .count();
                let mut peer_targets = Vec::new();
                let mut connected_peer_count = 0usize;
                let mut pending_dial_peer_count = 0usize;
                let candidate_peer_count = peer_plans.len();
                for (peer, already_connected, already_pending) in peer_plans {
                    let connection_ready = if already_connected {
                        connected_peer_count += 1;
                        None
                    } else if already_pending || scheduled_dial_peers.contains(&peer.id) {
                        if already_pending {
                            pending_dial_peer_count += 1;
                        }
                        let (ready, wait) = oneshot::channel();
                        connection_wait_started
                            .entry(peer.id)
                            .or_insert_with(Instant::now);
                        connection_waiters.entry(peer.id).or_default().push(ready);
                        Some(wait)
                    } else {
                        continue;
                    };
                    peer_targets.push(BitswapPeerTarget {
                        id: peer.id,
                        addrs: peer.addrs,
                        skip_want_have: peer.skip_want_have,
                        force_want_block: peer.force_want_block,
                        connection_ready,
                    });
                }
                tracing::info!(
                    phase = "bitswap_dial_plan",
                    cid = %primary_cid,
                    cids = %cid_summary.as_deref().unwrap_or(""),
                    cid_count,
                    peer_count = peer_targets.len(),
                    candidate_peer_count,
                    candidate_dial_peer_count,
                    new_dial_peer_count = scheduled_dial_peers.len(),
                    new_dial_addr_count = dial_addrs.len(),
                    suppressed_dial_addr_count,
                    suppressed_dial_peer_count,
                    pending_dial_peer_count,
                    connected_peer_count,
                    command_queued_ms
                );

                let mut started_dial_peers = BTreeSet::new();
                for (peer_id, addr) in dial_addrs {
                    let transport = bitswap_transport_label(&addr);
                    let dial_addr = addr.with_p2p(peer_id).unwrap_or_else(|addr| addr);
                    match swarm.dial(dial_addr) {
                        Ok(()) => {
                            started_dial_peers.insert(peer_id);
                        }
                        Err(err) => {
                            let error_detail = format_error_detail(&err);
                            let connection_limit = is_connection_limit_error(&error_detail);
                            record_dial_error(&dial_errors, peer_id, error_detail.clone()).await;
                            tracing::info!(
                                phase = "bitswap_dial_rejected",
                                peer = %peer_id,
                                transport,
                                connection_limit,
                                error = %err,
                                error_debug = ?err
                            );
                        }
                    }
                }
                let failed_dial_waiter_count = drop_failed_bitswap_dial_waiters(
                    &scheduled_dial_peers,
                    &started_dial_peers,
                    &mut connection_waiters,
                    &mut connection_wait_started,
                );
                if failed_dial_waiter_count > 0 {
                    tracing::info!(
                        phase = "bitswap_dial_waiters_dropped",
                        cid = %primary_cid,
                        cids = %cid_summary.as_deref().unwrap_or(""),
                        cid_count,
                        peer_count = scheduled_dial_peers.len().saturating_sub(started_dial_peers.len()),
                        waiter_count = failed_dial_waiter_count
                    );
                }

                let control = control.clone();
                let fetch_cids = cids.clone();
                let mut respond = command.respond;
                let dial_errors = dial_errors.clone();
                let peer_transports = peer_transports.clone();
                let fetch_started = Instant::now();
                fetches.push(Box::pin(async move {
                    let result = tokio::select! {
                        result = fetch_bitswap_batch_with_incoming_streams(
                            control,
                            peer_targets,
                            fetch_cids.clone(),
                            incoming_results,
                            dial_errors,
                            peer_transports,
                        ) => Some(result),
                        _ = respond.closed() => None,
                    };
                    if let Some(result) = result {
                        let _ = respond.send(result);
                    } else {
                        tracing::info!(
                            phase = "bitswap_fetch_cancelled",
                            cid = %primary_cid,
                            cids = %format_cids(&fetch_cids),
                            cid_count = fetch_cids.len(),
                            command_queued_ms,
                            elapsed_ms = fetch_started.elapsed().as_millis()
                        );
                    }
                    fetch_cids
                }));
            }
            Some(cids) = fetches.next(), if !fetches.is_empty() => {
                for cid in cids {
                    if let Some(count) = pending_counts.get_mut(&cid) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            pending_counts.remove(&cid);
                            pending_incoming.remove(&cid);
                        }
                    }
                }
            }
            maybe_stream = incoming.next() => {
                let Some((peer, stream)) = maybe_stream else {
                    continue;
                };
                if incoming_reads.len() >= MAX_PENDING_INCOMING_BITSWAP_READS {
                    tracing::info!(
                        phase = "bitswap_incoming_stream_read",
                        peer = %peer,
                        ok = false,
                        dropped = true,
                        pending_reads = incoming_reads.len()
                    );
                    continue;
                }
                incoming_reads.push(read_incoming_bitswap_stream(peer, stream).boxed());
            }
            Some(mut incoming_read) = incoming_reads.next(), if !incoming_reads.is_empty() => {
                match incoming_read.result {
                    Ok(blocks) => {
                        let mut matched = false;
                        let source_transport =
                            current_peer_transport(&peer_transports, incoming_read.peer).await;
                        for cid in pending_incoming.keys().copied().collect::<Vec<_>>() {
                            if let Some(mut result) = collect_bitswap_result(&cid, blocks.clone()) {
                                result.source_peer = Some(incoming_read.peer);
                                result.source_transport = source_transport;
                                result.delivery = "incoming";
                                let block_len = result.requested_block.len();
                                let batch_result = BitswapFetchBatchResult {
                                    requested_blocks: HashMap::from([(cid, result.requested_block)]),
                                    extra_blocks: result.extra_blocks,
                                    source_peer: result.source_peer,
                                    source_transport: result.source_transport,
                                    delivery: result.delivery,
                                };
                                matched = true;
                                let mut pending_waiter_count = 0usize;
                                let mut oldest_pending_ms = 0u128;
                                let mut newest_pending_ms = u128::MAX;
                                if let Some(waiters) = pending_incoming.get(&cid) {
                                    for waiter in waiters {
                                        let pending_ms = waiter.sent_at.elapsed().as_millis();
                                        pending_waiter_count += 1;
                                        oldest_pending_ms = oldest_pending_ms.max(pending_ms);
                                        newest_pending_ms = newest_pending_ms.min(pending_ms);
                                    }
                                }
                                if pending_waiter_count == 0 {
                                    newest_pending_ms = 0;
                                }
                                let mut delivered_waiter_count = 0usize;
                                let mut dropped_waiter_count = 0usize;
                                if let Some(senders) = pending_incoming.get_mut(&cid) {
                                    senders.retain(|pending| {
                                        if pending.sender.send(batch_result.clone()).is_ok() {
                                            delivered_waiter_count += 1;
                                            true
                                        } else {
                                            dropped_waiter_count += 1;
                                            false
                                        }
                                    });
                                }
                                tracing::info!(
                                    phase = "bitswap_incoming_block",
                                    cid = %cid,
                                    peer = %incoming_read.peer,
                                    source_transport = source_transport.unwrap_or("unknown"),
                                    block_count = blocks.len(),
                                    bytes = block_len,
                                    pending_waiter_count,
                                    delivered_waiter_count,
                                    dropped_waiter_count,
                                    oldest_pending_ms,
                                    newest_pending_ms
                                );
                                let _ = write_bitswap_cancel(&mut incoming_read.stream, &cid).await;
                            }
                        }
                        if !matched {
                            let _ = write_empty_bitswap_message(&mut incoming_read.stream).await;
                        }
                    }
                    Err(err) => {
                        if incoming_read.timed_out {
                            tracing::info!(
                                phase = "bitswap_incoming_stream_read",
                                peer = %incoming_read.peer,
                                ok = false,
                                timed_out = true,
                                timeout_ms = BITSWAP_INCOMING_STREAM_READ_TIMEOUT.as_millis(),
                                elapsed_ms = incoming_read.elapsed_ms
                            );
                        } else {
                            tracing::debug!(error = %err, "incoming bitswap stream read failed");
                        }
                    }
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        endpoint,
                        num_established,
                        concurrent_dial_errors,
                        established_in,
                        ..
                    } => {
                        *connected_peers.entry(peer_id).or_default() += 1;
                        let wait_elapsed_ms = connection_wait_started
                            .remove(&peer_id)
                            .map(|started| started.elapsed().as_millis())
                            .unwrap_or_default();
                        let failed_dial_count =
                            concurrent_dial_errors.as_ref().map_or(0, Vec::len);
                        let remote_addr = endpoint.get_remote_address();
                        let transport = bitswap_transport_label(remote_addr);
                        record_peer_transport_established(&peer_transports, peer_id, transport)
                            .await;
                        tracing::info!(
                            phase = "bitswap_connection_established",
                            peer = %peer_id,
                            remote_addr = %remote_addr,
                            transport,
                            endpoint = ?endpoint,
                            num_established = num_established.get(),
                            established_ms = established_in.as_millis(),
                            wait_elapsed_ms,
                            failed_dial_count
                        );
                        if let Some(waiters) = connection_waiters.remove(&peer_id) {
                            for waiter in waiters {
                                let _ = waiter.send(());
                            }
                        }
                    }
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        endpoint,
                        num_established,
                        cause,
                        ..
                    } => {
                        let remote_addr = endpoint.get_remote_address();
                        let transport = bitswap_transport_label(remote_addr);
                        record_peer_transport_closed(&peer_transports, peer_id, transport).await;
                        tracing::debug!(
                            phase = "bitswap_connection_closed",
                            peer = %peer_id,
                            transport,
                            endpoint = ?endpoint,
                            num_established,
                            cause = cause.as_ref().map(ToString::to_string).unwrap_or_default()
                        );
                        if let Some(count) = connected_peers.get_mut(&peer_id) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                connected_peers.remove(&peer_id);
                            }
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        let error_detail = format_error_detail(&error);
                        if let Some(peer_id) = peer_id {
                            record_dial_error(&dial_errors, peer_id, error_detail.clone()).await;
                            if let Some(backoff) = record_connection_error_backoff(
                                &mut connection_error_backoff,
                                peer_id,
                                &error_detail,
                                Instant::now(),
                            ) {
                                tracing::info!(
                                    phase = "bitswap_connection_error_backoff",
                                    peer = %peer_id,
                                    error_class = backoff.class,
                                    count = backoff.count,
                                    threshold = bitswap_connection_error_backoff_threshold(),
                                    ttl_ms = BITSWAP_CONNECTION_ERROR_BACKOFF_TTL.as_millis()
                                );
                            }
                        }
                        tracing::info!(
                            phase = "bitswap_connection_error",
                            peer = peer_id.map(|peer| peer.to_string()).unwrap_or_default(),
                            error = %error,
                            error_debug = ?error
                        );
                    }
                    _ => {}
                }
            }
            else => break,
        }
    }
}

fn prune_connection_waiters(
    waiters: &mut HashMap<PeerId, Vec<oneshot::Sender<()>>>,
    started: &mut HashMap<PeerId, Instant>,
) {
    waiters.retain(|_, peer_waiters| {
        peer_waiters.retain(|waiter| !waiter.is_closed());
        !peer_waiters.is_empty()
    });
    started.retain(|peer, _| waiters.contains_key(peer));
}

fn drop_failed_bitswap_dial_waiters(
    scheduled_peers: &BTreeSet<PeerId>,
    started_peers: &BTreeSet<PeerId>,
    waiters: &mut HashMap<PeerId, Vec<oneshot::Sender<()>>>,
    started: &mut HashMap<PeerId, Instant>,
) -> usize {
    let mut dropped = 0usize;
    for peer in scheduled_peers.difference(started_peers) {
        dropped += waiters.remove(peer).map_or(0, |waiters| waiters.len());
        started.remove(peer);
    }
    dropped
}

fn should_start_bitswap_dial(
    peer: &PeerId,
    connected_peers: &HashMap<PeerId, usize>,
    connection_waiters: &HashMap<PeerId, Vec<oneshot::Sender<()>>>,
) -> bool {
    !connected_peers.contains_key(peer) && !connection_waiters.contains_key(peer)
}

fn prune_connection_error_backoff(
    backoff: &mut HashMap<PeerId, ConnectionErrorBackoff>,
    now: Instant,
) {
    backoff.retain(|_, state| {
        now.duration_since(state.last_seen) <= BITSWAP_CONNECTION_ERROR_BACKOFF_TTL
            || state
                .suppress_until
                .is_some_and(|suppress_until| suppress_until > now)
    });
}

fn connection_error_backoff_remaining_ms(
    backoff: &HashMap<PeerId, ConnectionErrorBackoff>,
    peer: &PeerId,
    now: Instant,
) -> Option<u128> {
    backoff
        .get(peer)
        .and_then(|state| state.suppress_until)
        .and_then(|suppress_until| {
            (suppress_until > now).then(|| suppress_until.duration_since(now).as_millis())
        })
}

fn record_connection_error_backoff<'a>(
    backoff: &'a mut HashMap<PeerId, ConnectionErrorBackoff>,
    peer: PeerId,
    detail: &str,
    now: Instant,
) -> Option<&'a ConnectionErrorBackoff> {
    let class = bitswap_connection_error_backoff_class(detail)?;
    let state = backoff.entry(peer).or_insert(ConnectionErrorBackoff {
        count: 0,
        last_seen: now,
        suppress_until: None,
        class,
    });
    if now.duration_since(state.last_seen) > BITSWAP_CONNECTION_ERROR_BACKOFF_TTL {
        state.count = 0;
        state.suppress_until = None;
    }
    if state.class != class {
        state.count = 0;
        state.suppress_until = None;
        state.class = class;
    }
    state.count += 1;
    state.last_seen = now;
    if state.count >= bitswap_connection_error_backoff_threshold() {
        state.suppress_until = Some(now + BITSWAP_CONNECTION_ERROR_BACKOFF_TTL);
        Some(state)
    } else {
        None
    }
}

fn bitswap_connection_error_backoff_threshold() -> usize {
    let override_value = std::env::var_os(BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD_ENV);
    let override_value = override_value.as_ref().map(|value| value.to_string_lossy());
    bitswap_connection_error_backoff_threshold_from_env_value(override_value.as_deref())
}

fn bitswap_connection_error_backoff_threshold_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threshold| *threshold > 0)
        .unwrap_or(BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD)
}

fn bitswap_connection_error_backoff_class(detail: &str) -> Option<&'static str> {
    if detail.contains("Protocol negotiation failed") {
        Some("protocol_negotiation_failed")
    } else if detail.contains("Connection refused") {
        Some("connection_refused")
    } else if detail.contains("Connection reset by peer") {
        Some("connection_reset")
    } else if detail.contains("No route to host") {
        Some("no_route_to_host")
    } else {
        None
    }
}

async fn record_dial_error(errors: &DialErrorLog, peer: PeerId, detail: String) {
    let mut errors = errors.lock().await;
    let peer_errors = errors.entry(peer).or_default();
    peer_errors.push(detail);
    if peer_errors.len() > MAX_RECORDED_DIAL_ERRORS_PER_PEER {
        let extra = peer_errors.len() - MAX_RECORDED_DIAL_ERRORS_PER_PEER;
        peer_errors.drain(0..extra);
    }
}

async fn recent_dial_errors(errors: &DialErrorLog, peer: PeerId) -> String {
    let errors = errors.lock().await;
    match errors.get(&peer) {
        Some(values) if !values.is_empty() => values.join(" | "),
        _ => "none".to_string(),
    }
}

async fn record_peer_transport_established(
    transports: &PeerTransportLog,
    peer: PeerId,
    transport: &'static str,
) {
    let mut transports = transports.lock().await;
    let state = transports.entry(peer).or_default();
    *state.counts.entry(transport).or_default() += 1;
    state.current = Some(transport);
}

async fn record_peer_transport_closed(
    transports: &PeerTransportLog,
    peer: PeerId,
    transport: &'static str,
) {
    let mut transports = transports.lock().await;
    let Some(state) = transports.get_mut(&peer) else {
        return;
    };
    if let Some(count) = state.counts.get_mut(transport) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            state.counts.remove(transport);
        }
    }
    if state.counts.is_empty() {
        transports.remove(&peer);
    } else if state.current == Some(transport) {
        state.current = state.counts.keys().next().copied();
    }
}

async fn current_peer_transport(
    transports: &PeerTransportLog,
    peer: PeerId,
) -> Option<&'static str> {
    transports
        .lock()
        .await
        .get(&peer)
        .and_then(|state| state.current)
}

fn format_bitswap_peers(peers: &[BitswapPeer]) -> String {
    let has_multiple_peers = peers.len() > 1;
    let mut direct_untrusted_want_block_count = 0usize;
    peers
        .iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(|peer| {
            let prefer_want_have = bitswap_prefer_want_have(
                has_multiple_peers,
                peer.skip_want_have,
                peer.force_want_block,
                &mut direct_untrusted_want_block_count,
            );
            let mode = if prefer_want_have {
                "want-have"
            } else {
                "want-block"
            };
            format!("{}:{}@{}", peer.id, mode, format_multiaddrs(&peer.addrs))
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_cids(cids: &[Cid]) -> String {
    cids.iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_bitswap_targets(peers: &[BitswapPeerTarget]) -> String {
    peers
        .iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(|peer| format!("{}@{}", peer.id, format_multiaddrs(&peer.addrs)))
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_multiaddrs(addrs: &[Multiaddr]) -> String {
    if addrs.is_empty() {
        return "[]".to_string();
    }
    let rendered = addrs
        .iter()
        .take(MAX_BITSWAP_ADDRS_PER_PEER)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    if addrs.len() > MAX_BITSWAP_ADDRS_PER_PEER {
        format!(
            "[{rendered},+{} more]",
            addrs.len() - MAX_BITSWAP_ADDRS_PER_PEER
        )
    } else {
        format!("[{rendered}]")
    }
}

fn format_provider_candidates(providers: &[Provider]) -> String {
    providers
        .iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(|provider| {
            format!(
                "{}@{}",
                provider.id.as_deref().unwrap_or("<unknown>"),
                provider.addrs.join(",")
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_error_detail<E>(err: &E) -> String
where
    E: StdError + Debug,
{
    let mut detail = format!("{err}");
    let debug = format!("{err:?}");
    if debug != detail {
        detail.push_str("; debug=");
        detail.push_str(&debug);
    }

    let mut source = err.source();
    while let Some(err) = source {
        detail.push_str("; caused_by=");
        detail.push_str(&err.to_string());
        source = err.source();
    }

    detail
}

fn is_connection_limit_error(detail: &str) -> bool {
    detail.contains("ConnectionDenied") && detail.contains("Exceeded")
}

#[derive(Clone)]
struct ReceivedBitswapBlock {
    cid: Option<Cid>,
    data: Vec<u8>,
}

#[derive(Default)]
struct BitswapResponse {
    blocks: Vec<ReceivedBitswapBlock>,
    block_presences: Vec<ReceivedBlockPresence>,
}

impl BitswapResponse {
    fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.block_presences.is_empty()
    }

    fn has_presence(&self, cid: &Cid, type_pb: i32) -> bool {
        self.block_presences
            .iter()
            .any(|presence| &presence.cid == cid && presence.type_pb == type_pb)
    }
}

struct ReceivedBlockPresence {
    cid: Cid,
    type_pb: i32,
}

enum WantHaveFailure {
    TryOtherProtocols(BitswapProtocolFailure),
    PeerDoesNotHave(BitswapProtocolFailure),
}

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct BitswapBehaviour {
    stream: libp2p_stream::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    limits: connection_limits::Behaviour,
}

fn bitswap_connection_limits() -> connection_limits::ConnectionLimits {
    connection_limits::ConnectionLimits::default()
        .with_max_pending_outgoing(Some(BITSWAP_MAX_PENDING_OUTGOING_CONNECTIONS))
        .with_max_established_outgoing(Some(BITSWAP_MAX_ESTABLISHED_CONNECTIONS))
        .with_max_established(Some(BITSWAP_MAX_ESTABLISHED_CONNECTIONS))
}

async fn build_bitswap_swarm() -> Result<libp2p::Swarm<BitswapBehaviour>> {
    let swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            (tls::Config::new, noise::Config::new),
            yamux::Config::default,
        )
        .map_err(|err| RetrievalError::Bitswap(err.to_string()))?
        .with_quic()
        // WebSocket providers need DNS names preserved for WSS SNI, so this
        // transport has its own explicit Cloudflare resolver instead of the
        // builder's system-DNS websocket shortcut.
        .with_other_transport(cloudflare_websocket_transport)
        .map_err(|err| RetrievalError::Bitswap(err.to_string()))?
        .with_behaviour(|key| BitswapBehaviour {
            stream: libp2p_stream::Behaviour::new(),
            identify: identify::Behaviour::new(identify::Config::new(
                format!("freedom-ipfs/{}", env!("CARGO_PKG_VERSION")),
                key.public(),
            )),
            ping: ping::Behaviour::new(ping::Config::new()),
            limits: connection_limits::Behaviour::new(bitswap_connection_limits()),
        })
        .map_err(|err| RetrievalError::Bitswap(err.to_string()))?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(BITSWAP_IDLE_CONNECTION_TIMEOUT))
        .with_connection_timeout(BITSWAP_CONNECTION_TIMEOUT)
        .build();
    Ok(swarm)
}

fn cloudflare_websocket_transport(
    keypair: &libp2p::identity::Keypair,
) -> std::result::Result<Boxed<(PeerId, StreamMuxerBox)>, Box<dyn std::error::Error + Send + Sync>>
{
    let tcp = tcp::tokio::Transport::new(tcp::Config::default());
    let dns_tcp = libp2p::dns::tokio::Transport::custom(
        tcp,
        libp2p::dns::ResolverConfig::cloudflare(),
        libp2p::dns::ResolverOpts::default(),
    );
    let security = noise::Config::new(keypair)?;
    Ok(websocket::Config::new(dns_tcp)
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(security)
        .multiplex(yamux::Config::default())
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed())
}

#[derive(Default)]
struct BitswapProviderCandidates {
    peers: Vec<BitswapPeer>,
    quality: BitswapProviderAddrQuality,
}

#[derive(Default)]
struct BitswapProviderAddrQuality {
    provider_addr_count: usize,
    expanded_addr_count: usize,
    supported_addr_count: usize,
    id_only_provider_count: usize,
    invalid_provider_id_count: usize,
    provider_without_supported_bitswap_addr_count: usize,
    unsupported_relay_addr_count: usize,
    unsupported_webtransport_addr_count: usize,
    unsupported_webrtc_addr_count: usize,
    unsupported_certhash_addr_count: usize,
    unsupported_transport_addr_count: usize,
    missing_peer_addr_count: usize,
    unparsable_addr_count: usize,
    addr_with_relay_count: usize,
    addr_with_webtransport_count: usize,
    addr_with_webrtc_count: usize,
    addr_with_certhash_count: usize,
}

#[derive(Clone)]
struct CachedDnsaddrRecords {
    records: Vec<String>,
    log_as_cached: bool,
}

#[derive(Clone)]
struct CachedDnsIpRecords {
    addrs: Vec<IpAddr>,
    log_as_cached: bool,
}

type DnsaddrCache = HashMap<String, CachedDnsaddrRecords>;
type DnsIpCache = HashMap<String, CachedDnsIpRecords>;

impl BitswapProviderAddrQuality {
    fn rejected_addr_count(&self) -> usize {
        self.unsupported_relay_addr_count
            + self.unsupported_webtransport_addr_count
            + self.unsupported_webrtc_addr_count
            + self.unsupported_certhash_addr_count
            + self.unsupported_transport_addr_count
            + self.missing_peer_addr_count
            + self.unparsable_addr_count
    }

    fn record_rejection(&mut self, rejection: BitswapAddrRejection) {
        match rejection {
            BitswapAddrRejection::Relay => self.unsupported_relay_addr_count += 1,
            BitswapAddrRejection::WebTransport => self.unsupported_webtransport_addr_count += 1,
            BitswapAddrRejection::WebRtc => self.unsupported_webrtc_addr_count += 1,
            BitswapAddrRejection::Certhash => self.unsupported_certhash_addr_count += 1,
            BitswapAddrRejection::UnsupportedTransport => {
                self.unsupported_transport_addr_count += 1
            }
            BitswapAddrRejection::MissingPeer => self.missing_peer_addr_count += 1,
            BitswapAddrRejection::InvalidMultiaddr => self.unparsable_addr_count += 1,
        }
    }

    fn record_features(&mut self, features: BitswapAddrFeatures) {
        self.addr_with_relay_count += usize::from(features.relay);
        self.addr_with_webtransport_count += usize::from(features.webtransport);
        self.addr_with_webrtc_count += usize::from(features.webrtc);
        self.addr_with_certhash_count += usize::from(features.certhash);
    }
}

async fn bitswap_peers(providers: &[Provider]) -> Vec<BitswapPeer> {
    bitswap_peers_with_quality(providers).await.peers
}

async fn bitswap_peers_with_quality(providers: &[Provider]) -> BitswapProviderCandidates {
    let mut peers = Vec::new();
    let mut quality = BitswapProviderAddrQuality::default();
    let mut dnsaddr_cache = DnsaddrCache::new();
    let mut dns_ip_cache = DnsIpCache::new();
    prefetch_bitswap_dns_expansions(providers, &mut dnsaddr_cache, &mut dns_ip_cache).await;

    for provider in providers {
        let provider_peer = provider.id.as_deref().and_then(parse_peer_id);
        if provider.id.is_some() && provider_peer.is_none() {
            quality.invalid_provider_id_count += 1;
        }
        if provider_peer.is_some() && provider.addrs.is_empty() {
            quality.id_only_provider_count += 1;
        }
        quality.provider_addr_count += provider.addrs.len();
        let mut addrs = Vec::new();
        let mut peer_id = provider_peer;
        let mut provider_has_supported_addr = false;

        for addr in
            expand_provider_multiaddrs(&provider.addrs, &mut dnsaddr_cache, &mut dns_ip_cache).await
        {
            quality.expanded_addr_count += 1;
            match analyze_bitswap_multiaddr(&addr, provider_peer) {
                Ok((addr_peer, dial_addr, features)) => {
                    quality.record_features(features);
                    quality.supported_addr_count += 1;
                    provider_has_supported_addr = true;
                    peer_id.get_or_insert(addr_peer);
                    addrs.push(dial_addr);
                }
                Err((rejection, features)) => {
                    quality.record_features(features);
                    quality.record_rejection(rejection);
                }
            }
        }

        if provider_peer.is_some() && !provider_has_supported_addr {
            quality.provider_without_supported_bitswap_addr_count += 1;
        }

        if let Some(id) = peer_id {
            if !addrs.is_empty() {
                addrs.sort_by_key(bitswap_addr_score);
                addrs.dedup();
                addrs.truncate(MAX_BITSWAP_ADDRS_PER_PEER);
                merge_bitswap_peer(&mut peers, id, addrs);
            }
        }
    }

    peers.truncate(MAX_BITSWAP_PEERS_PER_BLOCK);
    BitswapProviderCandidates { peers, quality }
}

fn maybe_force_zero_http_direct_want_block_peers(
    providers: &[Provider],
    peers: &mut [BitswapPeer],
    context: Option<RetrievalRequestContext>,
) -> usize {
    maybe_force_zero_http_direct_want_block_peers_with_limit(
        providers,
        peers,
        bitswap_zero_http_direct_want_block_peers(context),
    )
}

fn maybe_force_zero_http_direct_want_block_peers_with_limit(
    providers: &[Provider],
    peers: &mut [BitswapPeer],
    limit: Option<usize>,
) -> usize {
    if provider_http_url_count(providers) != 0 {
        return 0;
    }
    let Some(limit) = limit else {
        return 0;
    };

    let mut marked = 0usize;
    for peer in peers.iter_mut().filter(|peer| !peer.skip_want_have) {
        if marked >= limit {
            break;
        }
        peer.force_want_block = true;
        marked += 1;
    }
    marked
}

fn merge_bitswap_peer(peers: &mut Vec<BitswapPeer>, id: PeerId, addrs: Vec<Multiaddr>) {
    if let Some(peer) = peers.iter_mut().find(|peer| peer.id == id) {
        peer.addrs.extend(addrs);
        peer.addrs.sort_by_key(bitswap_addr_score);
        peer.addrs.dedup();
        peer.addrs.truncate(MAX_BITSWAP_ADDRS_PER_PEER);
    } else {
        peers.push(BitswapPeer {
            id,
            addrs,
            skip_want_have: false,
            force_want_block: false,
        });
    }
}

fn interleaved_bitswap_dials(peers: &[BitswapPeer]) -> Vec<(PeerId, Multiaddr)> {
    let max_addrs = peers
        .iter()
        .map(|peer| peer.addrs.len())
        .max()
        .unwrap_or_default();
    let mut dials = Vec::new();
    for addr_index in 0..max_addrs {
        for peer in peers {
            if let Some(addr) = peer.addrs.get(addr_index) {
                dials.push((peer.id, addr.clone()));
            }
        }
    }
    dials
}

fn limited_interleaved_bitswap_dials(peers: &[BitswapPeer]) -> (Vec<(PeerId, Multiaddr)>, usize) {
    let all_dials = interleaved_bitswap_dials(peers);
    let suppressed_dial_count = all_dials
        .len()
        .saturating_sub(MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND);
    (
        all_dials
            .into_iter()
            .take(MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND)
            .collect(),
        suppressed_dial_count,
    )
}

#[derive(Default)]
struct BitswapPeerAddrStats {
    tcp: usize,
    quic: usize,
    ws: usize,
    wss: usize,
    dns: usize,
    ip4: usize,
    ip6: usize,
}

fn bitswap_peer_addr_stats(peers: &[BitswapPeer]) -> BitswapPeerAddrStats {
    let mut stats = BitswapPeerAddrStats::default();
    for addr in peers.iter().flat_map(|peer| &peer.addrs) {
        let mut has_tcp = false;
        let mut has_quic = false;
        let mut has_ws = false;
        let mut has_wss = false;
        let mut has_dns = false;
        let mut has_ip4 = false;
        let mut has_ip6 = false;
        for protocol in addr.iter() {
            match protocol {
                Protocol::Tcp(_) => has_tcp = true,
                Protocol::Quic | Protocol::QuicV1 => has_quic = true,
                Protocol::Ws(_) => has_ws = true,
                Protocol::Wss(_) => has_wss = true,
                Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                    has_dns = true
                }
                Protocol::Ip4(_) => has_ip4 = true,
                Protocol::Ip6(_) => has_ip6 = true,
                _ => {}
            }
        }
        stats.tcp += usize::from(has_tcp);
        stats.quic += usize::from(has_quic);
        stats.ws += usize::from(has_ws);
        stats.wss += usize::from(has_wss);
        stats.dns += usize::from(has_dns);
        stats.ip4 += usize::from(has_ip4);
        stats.ip6 += usize::from(has_ip6);
    }
    stats
}

fn bitswap_transport_label(addr: &Multiaddr) -> &'static str {
    let mut has_tcp = false;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Wss(_) => return "wss",
            Protocol::Ws(_) => return "ws",
            Protocol::Quic | Protocol::QuicV1 => return "quic",
            Protocol::Tcp(_) => has_tcp = true,
            _ => {}
        }
    }
    if has_tcp {
        "tcp"
    } else {
        "other"
    }
}

async fn prefetch_bitswap_dns_expansions(
    providers: &[Provider],
    dnsaddr_cache: &mut DnsaddrCache,
    dns_ip_cache: &mut DnsIpCache,
) {
    let started = Instant::now();
    let dnsaddr_hosts = providers
        .iter()
        .flat_map(|provider| provider.addrs.iter())
        .filter_map(|addr| dnsaddr_host(addr).map(str::to_string))
        .filter(|host| !dnsaddr_cache.contains_key(host))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let dnsaddr_host_count = dnsaddr_hosts.len();
    for (host, records) in resolve_dnsaddr_hosts(dnsaddr_hosts).await {
        dnsaddr_cache.insert(
            host,
            CachedDnsaddrRecords {
                records,
                log_as_cached: false,
            },
        );
    }

    let dns_names = providers
        .iter()
        .flat_map(|provider| provider.addrs.iter())
        .flat_map(|addr| {
            if let Some(host) = dnsaddr_host(addr) {
                dnsaddr_cache
                    .get(host)
                    .map(|entry| entry.records.clone())
                    .unwrap_or_default()
            } else {
                vec![addr.clone()]
            }
        })
        .filter(|addr| !websocket_multiaddr(addr))
        .filter_map(|addr| dns_multiaddr_name(&addr))
        .filter(|host| !dns_ip_cache.contains_key(host))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let dns_ip_host_count = dns_names.len();
    for (host, addrs) in resolve_dns_ip_hosts(dns_names).await {
        dns_ip_cache.insert(
            host,
            CachedDnsIpRecords {
                addrs,
                log_as_cached: false,
            },
        );
    }

    if dnsaddr_host_count > 0 || dns_ip_host_count > 0 {
        tracing::info!(
            phase = "bitswap_dns_prefetch",
            dnsaddr_host_count,
            dns_ip_host_count,
            elapsed_ms = started.elapsed().as_millis()
        );
    }
}

async fn resolve_dnsaddr_hosts(hosts: Vec<String>) -> Vec<(String, Vec<String>)> {
    let resolver = Arc::new(CloudflareDohResolver::default());
    let mut pending = FuturesUnordered::<BoxFuture<'static, (String, Vec<String>)>>::new();
    let mut hosts = hosts.into_iter();
    for _ in 0..BITSWAP_DNS_PREFETCH_CONCURRENCY {
        let Some(host) = hosts.next() else {
            break;
        };
        pending.push(resolve_dnsaddr_host(resolver.clone(), host).boxed());
    }

    let mut resolved = Vec::new();
    while let Some(result) = pending.next().await {
        resolved.push(result);
        if let Some(host) = hosts.next() {
            pending.push(resolve_dnsaddr_host(resolver.clone(), host).boxed());
        }
    }
    resolved
}

async fn resolve_dnsaddr_host(
    resolver: Arc<CloudflareDohResolver>,
    host: String,
) -> (String, Vec<String>) {
    let lookup = format!("_dnsaddr.{host}");
    let records = match resolver.txt_lookup(&lookup).await {
        Ok(records) => dnsaddr_records(records),
        Err(_) => Vec::new(),
    };
    (host, records)
}

async fn resolve_dns_ip_hosts(hosts: Vec<String>) -> Vec<(String, Vec<IpAddr>)> {
    let resolver = Arc::new(CloudflareDohResolver::default());
    let mut pending = FuturesUnordered::<BoxFuture<'static, (String, Vec<IpAddr>)>>::new();
    let mut hosts = hosts.into_iter();
    for _ in 0..BITSWAP_DNS_PREFETCH_CONCURRENCY {
        let Some(host) = hosts.next() else {
            break;
        };
        pending.push(resolve_dns_ip_host(resolver.clone(), host).boxed());
    }

    let mut resolved = Vec::new();
    while let Some(result) = pending.next().await {
        resolved.push(result);
        if let Some(host) = hosts.next() {
            pending.push(resolve_dns_ip_host(resolver.clone(), host).boxed());
        }
    }
    resolved
}

async fn resolve_dns_ip_host(
    resolver: Arc<CloudflareDohResolver>,
    host: String,
) -> (String, Vec<IpAddr>) {
    let addrs = resolver.ip_lookup(&host).await.unwrap_or_default();
    (host, addrs)
}

async fn expand_provider_multiaddrs(
    addrs: &[String],
    dnsaddr_cache: &mut DnsaddrCache,
    dns_ip_cache: &mut DnsIpCache,
) -> Vec<String> {
    let resolver = CloudflareDohResolver::default();
    let mut expanded_dnsaddr = Vec::new();

    for addr in addrs {
        let Some(host) = dnsaddr_host(addr) else {
            expanded_dnsaddr.push(addr.clone());
            continue;
        };
        if let Some(entry) = dnsaddr_cache.get_mut(host) {
            tracing::info!(
                phase = "bitswap_dnsaddr_expand",
                host,
                cached = entry.log_as_cached,
                ok = !entry.records.is_empty(),
                record_count = entry.records.len()
            );
            entry.log_as_cached = true;
            expanded_dnsaddr.extend(entry.records.iter().cloned());
            continue;
        }
        let lookup = format!("_dnsaddr.{host}");
        let records = match resolver.txt_lookup(&lookup).await {
            Ok(records) => dnsaddr_records(records),
            Err(_) => Vec::new(),
        };
        tracing::info!(
            phase = "bitswap_dnsaddr_expand",
            host,
            cached = false,
            ok = !records.is_empty(),
            record_count = records.len()
        );
        dnsaddr_cache.insert(
            host.to_string(),
            CachedDnsaddrRecords {
                records: records.clone(),
                log_as_cached: true,
            },
        );
        expanded_dnsaddr.extend(records);
    }

    let mut expanded = Vec::new();
    for addr in expanded_dnsaddr {
        if websocket_multiaddr(&addr) {
            expanded.push(addr);
            continue;
        }
        let Some(dns_name) = dns_multiaddr_name(&addr) else {
            expanded.push(addr);
            continue;
        };
        let addrs = if let Some(entry) = dns_ip_cache.get_mut(&dns_name) {
            tracing::info!(
                phase = "bitswap_dns_multiaddr_expand",
                host = %dns_name,
                cached = entry.log_as_cached,
                ip_count = entry.addrs.len()
            );
            entry.log_as_cached = true;
            entry.addrs.clone()
        } else {
            match resolver.ip_lookup(&dns_name).await {
                Ok(addrs) => {
                    tracing::info!(
                        phase = "bitswap_dns_multiaddr_expand",
                        host = %dns_name,
                        cached = false,
                        ip_count = addrs.len()
                    );
                    dns_ip_cache.insert(
                        dns_name.clone(),
                        CachedDnsIpRecords {
                            addrs: addrs.clone(),
                            log_as_cached: true,
                        },
                    );
                    addrs
                }
                Err(_) => {
                    dns_ip_cache.insert(
                        dns_name.clone(),
                        CachedDnsIpRecords {
                            addrs: Vec::new(),
                            log_as_cached: true,
                        },
                    );
                    expanded.push(addr);
                    continue;
                }
            }
        };
        if addrs.is_empty() {
            expanded.push(addr);
            continue;
        }
        expanded.extend(
            addrs
                .into_iter()
                .map(|ip| replace_dns_multiaddr(&addr, ip))
                .filter_map(|addr| addr.map(|addr| addr.to_string())),
        );
    }

    expanded
}

fn dnsaddr_records(records: Vec<String>) -> Vec<String> {
    records
        .into_iter()
        .filter_map(|record| {
            record
                .trim()
                .strip_prefix("dnsaddr=")
                .map(ToOwned::to_owned)
        })
        .collect()
}

fn dnsaddr_host(addr: &str) -> Option<&str> {
    let mut parts = addr.split('/').filter(|part| !part.is_empty());
    if parts.next()? == "dnsaddr" {
        parts.next()
    } else {
        None
    }
}

fn websocket_multiaddr(addr: &str) -> bool {
    Multiaddr::from_str(addr).is_ok_and(|addr| {
        addr.iter()
            .any(|protocol| matches!(protocol, Protocol::Ws(_) | Protocol::Wss(_)))
    })
}

fn dns_multiaddr_name(addr: &str) -> Option<String> {
    Multiaddr::from_str(addr)
        .ok()?
        .iter()
        .find_map(|protocol| match protocol {
            Protocol::Dns(name) | Protocol::Dns4(name) | Protocol::Dns6(name) => {
                Some(name.to_string())
            }
            _ => None,
        })
}

fn replace_dns_multiaddr(addr: &str, ip: IpAddr) -> Option<Multiaddr> {
    let mut replaced = Multiaddr::empty();
    let mut replaced_dns = false;
    for protocol in Multiaddr::from_str(addr).ok()?.iter() {
        match protocol {
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) if !replaced_dns => {
                match ip {
                    IpAddr::V4(ip) => replaced.push(Protocol::Ip4(ip)),
                    IpAddr::V6(ip) => replaced.push(Protocol::Ip6(ip)),
                }
                replaced_dns = true;
            }
            other => replaced.push(other.to_owned()),
        }
    }
    replaced_dns.then_some(replaced)
}

#[cfg(test)]
fn parse_bitswap_multiaddr(
    addr: &str,
    provider_peer: Option<PeerId>,
) -> Option<(PeerId, Multiaddr)> {
    analyze_bitswap_multiaddr(addr, provider_peer)
        .ok()
        .map(|(peer, addr, _)| (peer, addr))
}

#[derive(Clone, Copy)]
enum BitswapAddrRejection {
    InvalidMultiaddr,
    MissingPeer,
    Relay,
    WebTransport,
    WebRtc,
    Certhash,
    UnsupportedTransport,
}

#[derive(Clone, Copy, Default)]
struct BitswapAddrFeatures {
    relay: bool,
    webtransport: bool,
    webrtc: bool,
    certhash: bool,
}

fn analyze_bitswap_multiaddr(
    addr: &str,
    provider_peer: Option<PeerId>,
) -> std::result::Result<
    (PeerId, Multiaddr, BitswapAddrFeatures),
    (BitswapAddrRejection, BitswapAddrFeatures),
> {
    let mut multiaddr = Multiaddr::from_str(addr).map_err(|_| {
        (
            BitswapAddrRejection::InvalidMultiaddr,
            BitswapAddrFeatures::default(),
        )
    })?;
    let addr_peer = match multiaddr.iter().last() {
        Some(Protocol::P2p(peer)) => {
            multiaddr.pop();
            Some(peer)
        }
        _ => None,
    };
    let features = bitswap_addr_features(&multiaddr);
    let peer_id = addr_peer
        .or(provider_peer)
        .ok_or((BitswapAddrRejection::MissingPeer, features))?;
    unsupported_bitswap_addr_reason(&multiaddr)
        .map_or(Ok((peer_id, multiaddr, features)), |reason| {
            Err((reason, features))
        })
}

fn parse_peer_id(id: &str) -> Option<PeerId> {
    PeerId::from_str(id).ok()
}

fn unsupported_bitswap_addr_reason(addr: &Multiaddr) -> Option<BitswapAddrRejection> {
    let mut has_tcp = false;
    let mut has_udp = false;
    let mut has_quic = false;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Tcp(_) => has_tcp = true,
            Protocol::Udp(_) => has_udp = true,
            Protocol::Quic | Protocol::QuicV1 => has_quic = true,
            Protocol::P2pCircuit => return Some(BitswapAddrRejection::Relay),
            Protocol::WebTransport => return Some(BitswapAddrRejection::WebTransport),
            Protocol::WebRTC | Protocol::WebRTCDirect | Protocol::P2pWebRtcDirect => {
                return Some(BitswapAddrRejection::WebRtc);
            }
            Protocol::Certhash(_) => return Some(BitswapAddrRejection::Certhash),
            _ => {}
        }
    }
    if has_tcp || (has_udp && has_quic) {
        None
    } else {
        Some(BitswapAddrRejection::UnsupportedTransport)
    }
}

fn bitswap_addr_features(addr: &Multiaddr) -> BitswapAddrFeatures {
    let mut features = BitswapAddrFeatures::default();
    for protocol in addr.iter() {
        match protocol {
            Protocol::P2pCircuit => features.relay = true,
            Protocol::WebTransport => features.webtransport = true,
            Protocol::WebRTC | Protocol::WebRTCDirect | Protocol::P2pWebRtcDirect => {
                features.webrtc = true;
            }
            Protocol::Certhash(_) => features.certhash = true,
            _ => {}
        }
    }
    features
}

fn bitswap_addr_score(addr: &Multiaddr) -> u8 {
    let mut has_ip = false;
    let mut has_dns = false;
    let mut has_tcp = false;
    let mut has_quic = false;
    let mut has_ws = false;

    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(_) | Protocol::Ip6(_) => has_ip = true,
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                has_dns = true
            }
            Protocol::Tcp(_) => has_tcp = true,
            Protocol::Quic | Protocol::QuicV1 => has_quic = true,
            Protocol::Ws(_) | Protocol::Wss(_) => has_ws = true,
            _ => {}
        }
    }

    match (has_ip, has_dns, has_tcp, has_quic, has_ws) {
        (true, _, true, _, false) => 0,
        (true, _, _, true, false) => 1,
        (_, true, true, _, false) => 2,
        (_, true, _, true, false) => 3,
        (true, _, true, _, true) => 4,
        (_, true, true, _, true) => 5,
        _ => 9,
    }
}

fn should_refresh_providers_after_failure(err: &RetrievalError) -> bool {
    matches!(
        err,
        RetrievalError::Bitswap(_)
            | RetrievalError::BitswapPeerFailures { .. }
            | RetrievalError::BitswapTimeout
            | RetrievalError::NoHttpProviders
            | RetrievalError::NoBitswapProviders
    )
}

fn is_no_provider_error(err: &RetrievalError) -> bool {
    matches!(
        err,
        RetrievalError::NoHttpProviders | RetrievalError::NoBitswapProviders
    )
}

fn is_bitswap_connection_ready_failure(err: &RetrievalError) -> bool {
    match err {
        RetrievalError::Bitswap(message) => {
            message.contains("bitswap connection was not established")
        }
        RetrievalError::BitswapPeerFailures {
            connection_timeout_peers,
            ..
        } => !connection_timeout_peers.is_empty(),
        _ => false,
    }
}

fn is_bitswap_request_timeout(err: &RetrievalError) -> bool {
    matches!(err, RetrievalError::BitswapTimeout)
}

fn bitswap_timeout_peers(err: &RetrievalError) -> &[String] {
    match err {
        RetrievalError::BitswapPeerFailures { timeout_peers, .. } => timeout_peers,
        _ => &[],
    }
}

fn bitswap_connection_timeout_peers(err: &RetrievalError) -> &[String] {
    match err {
        RetrievalError::BitswapPeerFailures {
            connection_timeout_peers,
            ..
        } => connection_timeout_peers,
        _ => &[],
    }
}

fn suppress_bitswap_timeout_suppression(
    timeout_peer_count: usize,
    attempted_peer_count: usize,
) -> bool {
    attempted_peer_count >= 4 && timeout_peer_count * 2 >= attempted_peer_count
}

fn same_provider_set(left: &[Provider], right: &[Provider]) -> bool {
    normalized_provider_set(left) == normalized_provider_set(right)
}

async fn same_bitswap_peer_set(left: &[Provider], right: &[Provider]) -> bool {
    normalized_bitswap_peer_set(left).await == normalized_bitswap_peer_set(right).await
}

async fn normalized_bitswap_peer_set(providers: &[Provider]) -> BTreeSet<(String, Vec<String>)> {
    bitswap_peers(providers)
        .await
        .into_iter()
        .map(|peer| {
            let mut addrs = peer
                .addrs
                .into_iter()
                .map(|addr| addr.to_string())
                .collect::<Vec<_>>();
            addrs.sort();
            addrs.dedup();
            (peer.id.to_string(), addrs)
        })
        .collect()
}

fn normalized_provider_set(providers: &[Provider]) -> BTreeSet<(Option<String>, Vec<String>)> {
    providers
        .iter()
        .map(|provider| {
            let mut addrs = provider.addrs.clone();
            addrs.sort();
            addrs.dedup();
            (provider.id.clone(), addrs)
        })
        .collect()
}

#[derive(Debug)]
struct LimitedResponseBytes {
    bytes: Vec<u8>,
    stats: LimitedResponseByteStats,
}

#[derive(Debug, Default)]
struct LimitedResponseByteStats {
    bytes_read: usize,
    first_chunk_elapsed: Option<Duration>,
}

async fn limited_response_bytes(
    response: reqwest::Response,
    max_size: usize,
) -> Result<LimitedResponseBytes> {
    let started = Instant::now();
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let mut first_chunk_elapsed = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        first_chunk_elapsed.get_or_insert_with(|| started.elapsed());
        if body.len().saturating_add(chunk.len()) > max_size {
            return Err(RetrievalError::Core(CoreError::BlockTooLarge {
                actual: body.len().saturating_add(chunk.len()),
                max: max_size,
            }));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(LimitedResponseBytes {
        stats: LimitedResponseByteStats {
            bytes_read: body.len(),
            first_chunk_elapsed,
        },
        bytes: body,
    })
}

fn accept_bitswap_streams(control: &mut StreamControl) -> Result<Vec<IncomingStreams>> {
    bitswap_protocols()
        .into_iter()
        .map(|protocol| {
            control
                .accept(protocol)
                .map_err(|err| RetrievalError::Bitswap(err.to_string()))
        })
        .collect()
}

async fn fetch_bitswap_batch_with_incoming_streams(
    control: StreamControl,
    peers: Vec<BitswapPeerTarget>,
    cids: Vec<Cid>,
    incoming_results: mpsc::UnboundedReceiver<BitswapFetchBatchResult>,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> Result<BitswapFetchBatchResult> {
    let incoming_cids = cids.clone();
    tokio::select! {
        result = fetch_bitswap_batch_over_outgoing_streams(control, peers, cids, dial_errors, peer_transports) => result,
        incoming = collect_incoming_bitswap_batch(incoming_cids, incoming_results) => incoming,
    }
}

async fn collect_incoming_bitswap_batch(
    cids: Vec<Cid>,
    mut incoming_results: mpsc::UnboundedReceiver<BitswapFetchBatchResult>,
) -> Result<BitswapFetchBatchResult> {
    let started = Instant::now();
    let partial_grace = bitswap_incoming_batch_partial_grace();
    let primary_cid = cids[0];
    let cid_count = cids.len();
    let cid_summary = tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
    let wanted = cids.iter().copied().collect::<BTreeSet<_>>();
    let mut requested_blocks = HashMap::new();
    let mut extra_blocks = Vec::new();
    let mut source_peer = None;
    let mut source_transport = None;

    while requested_blocks.len() < wanted.len() {
        let next_result = if requested_blocks.is_empty() {
            incoming_results.recv().await
        } else if partial_grace.is_zero() {
            tokio::task::yield_now().await;
            match incoming_results.try_recv() {
                Ok(result) => Some(result),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        } else {
            match timeout(partial_grace, incoming_results.recv()).await {
                Ok(result) => result,
                Err(_) => break,
            }
        };
        let Some(result) = next_result else {
            if requested_blocks.is_empty() {
                return Err(RetrievalError::Bitswap(
                    "incoming bitswap result channel closed".into(),
                ));
            }
            break;
        };
        if source_peer.is_none() {
            source_peer = result.source_peer;
            source_transport = result.source_transport;
        }
        for (cid, data) in result.requested_blocks {
            if wanted.contains(&cid) {
                requested_blocks.entry(cid).or_insert(data);
            } else {
                extra_blocks.push((cid, data));
            }
        }
        extra_blocks.extend(result.extra_blocks);
    }

    if cid_count > 1 {
        tracing::info!(
            phase = "bitswap_incoming_batch",
            cid = %primary_cid,
            cids = %cid_summary.as_deref().unwrap_or(""),
            cid_count,
            requested_blocks = requested_blocks.len(),
            missing_blocks = wanted.len().saturating_sub(requested_blocks.len()),
            extra_blocks = extra_blocks.len(),
            source_peer = %source_peer.map(|peer| peer.to_string()).unwrap_or_default(),
            source_transport = source_transport.unwrap_or("unknown"),
            partial = requested_blocks.len() < wanted.len(),
            partial_grace_ms = partial_grace.as_millis(),
            elapsed_ms = started.elapsed().as_millis()
        );
    }
    Ok(BitswapFetchBatchResult {
        requested_blocks,
        extra_blocks,
        source_peer,
        source_transport,
        delivery: "incoming",
    })
}

async fn fetch_bitswap_batch_over_outgoing_streams(
    control: StreamControl,
    peers: Vec<BitswapPeerTarget>,
    cids: Vec<Cid>,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> Result<BitswapFetchBatchResult> {
    let mut attempts = FuturesUnordered::new();
    let has_multiple_peers = peers.len() > 1;
    let cid_count = cids.len();
    let primary_cid = cids[0];
    let cid_summary = tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
    let stream_read_timeout = bitswap_stream_read_timeout(
        peers.len(),
        peers.iter().filter(|peer| peer.skip_want_have).count(),
    );
    let request_timeouts = BitswapRequestTimeouts {
        want_have: BITSWAP_WANT_HAVE_TIMEOUT,
        stream_read: stream_read_timeout,
    };
    let target_summary = format_bitswap_targets(&peers);
    let mut direct_untrusted_want_block_count = 0usize;
    for peer in peers {
        let prefer_want_have = bitswap_prefer_want_have(
            has_multiple_peers,
            peer.skip_want_have,
            peer.force_want_block,
            &mut direct_untrusted_want_block_count,
        );
        attempts.push(request_bitswap_blocks_after_connection(
            control.clone(),
            peer,
            cids.clone(),
            prefer_want_have,
            request_timeouts,
            dial_errors.clone(),
            peer_transports.clone(),
        ));
    }

    let mut failures = Vec::new();
    while let Some(result) = attempts.next().await {
        match result {
            Ok(data) => return Ok(data),
            Err(err) => failures.push(err),
        }
    }

    let detail = if failures.is_empty() {
        "no bitswap request attempts completed".to_string()
    } else {
        failures
            .iter()
            .take(MAX_BITSWAP_FAILURE_DETAILS)
            .map(|failure| failure.detail.as_str())
            .collect::<Vec<_>>()
            .join("; ")
    };
    let message = format!(
        "all bitswap stream requests failed for cids {}; targets={target_summary}; detail={detail}",
        format_cids(&cids)
    );
    let timeout_peers = failures
        .iter()
        .filter(|failure| failure.kind == BitswapPeerFailureKind::ReadTimeout)
        .map(|failure| failure.id.to_string())
        .collect::<Vec<_>>();
    let connection_timeout_peers = failures
        .iter()
        .filter(|failure| failure.kind == BitswapPeerFailureKind::ConnectionTimeout)
        .map(|failure| failure.id.to_string())
        .collect::<Vec<_>>();
    if timeout_peers.is_empty() && connection_timeout_peers.is_empty() {
        tracing::info!(
            phase = "bitswap_batch_failed",
            cid = %primary_cid,
            cids = %cid_summary.as_deref().unwrap_or(""),
            cid_count,
            failure_count = failures.len(),
        );
        Err(RetrievalError::Bitswap(message))
    } else {
        tracing::info!(
            phase = "bitswap_batch_failed",
            cid = %primary_cid,
            cids = %cid_summary.as_deref().unwrap_or(""),
            cid_count,
            failure_count = failures.len(),
            read_timeout_peer_count = timeout_peers.len(),
            connection_timeout_peer_count = connection_timeout_peers.len(),
        );
        Err(RetrievalError::BitswapPeerFailures {
            message,
            timeout_peers,
            connection_timeout_peers,
        })
    }
}

fn bitswap_prefer_want_have(
    has_multiple_peers: bool,
    skip_want_have: bool,
    force_want_block: bool,
    direct_untrusted_want_block_count: &mut usize,
) -> bool {
    if !skip_want_have
        && (force_want_block
            || *direct_untrusted_want_block_count < MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS)
    {
        *direct_untrusted_want_block_count += 1;
        return false;
    }
    has_multiple_peers && !skip_want_have
}

async fn request_bitswap_blocks_after_connection(
    control: StreamControl,
    peer: BitswapPeerTarget,
    cids: Vec<Cid>,
    prefer_want_have: bool,
    request_timeouts: BitswapRequestTimeouts,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> std::result::Result<BitswapFetchBatchResult, BitswapPeerFailure> {
    let BitswapPeerTarget {
        id: peer_id,
        addrs,
        connection_ready,
        force_want_block,
        ..
    } = peer;

    let attempt_started = Instant::now();
    let primary_cid = cids[0];
    let cid_count = cids.len();
    let cid_summary = tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
    let connection_ready_timeout = bitswap_connection_ready_timeout();
    tracing::info!(
        phase = "bitswap_peer_attempt_start",
        cid = %primary_cid,
        cids = %cid_summary.as_deref().unwrap_or(""),
        cid_count,
        peer = %peer_id,
        prefer_want_have,
        force_want_block,
        connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
        want_have_timeout_ms = request_timeouts.want_have.as_millis(),
        stream_read_timeout_ms = request_timeouts.stream_read.as_millis()
    );

    if let Some(connection_ready) = connection_ready {
        match timeout(connection_ready_timeout, connection_ready).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                tracing::info!(
                    phase = "bitswap_peer_attempt",
                    cid = %primary_cid,
                    cids = %cid_summary.as_deref().unwrap_or(""),
                    cid_count,
                    peer = %peer_id,
                    ok = false,
                    failure_kind = "connection_waiter_dropped",
                    prefer_want_have,
                    force_want_block,
                    connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
                    want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                    stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                return Err(BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::Other,
                    detail: format!(
                        "{peer_id}: bitswap connection waiter was dropped before connection"
                    ),
                });
            }
            Err(_) => {
                let recent_dial_errors = recent_dial_errors(&dial_errors, peer_id).await;
                tracing::info!(
                    phase = "bitswap_peer_attempt",
                    cid = %primary_cid,
                    cids = %cid_summary.as_deref().unwrap_or(""),
                    cid_count,
                    peer = %peer_id,
                    ok = false,
                    failure_kind = "connection_timeout",
                    prefer_want_have,
                    force_want_block,
                    connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
                    want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                    stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                return Err(BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::ConnectionTimeout,
                    detail: format!(
                        "{}: bitswap connection was not established within {}ms; addrs={}; recent_dial_errors={}",
                        peer_id,
                        connection_ready_timeout.as_millis(),
                        format_multiaddrs(&addrs),
                        recent_dial_errors
                    ),
                });
            }
        }
    }
    let result = request_bitswap_blocks(
        control,
        peer_id,
        addrs,
        cids,
        prefer_want_have,
        request_timeouts,
        peer_transports,
    )
    .await;
    match &result {
        Ok(result) => {
            let requested_bytes = result
                .requested_blocks
                .values()
                .map(Vec::len)
                .sum::<usize>();
            tracing::info!(
                phase = "bitswap_peer_attempt",
                cid = %primary_cid,
                cids = %cid_summary.as_deref().unwrap_or(""),
                cid_count,
                peer = %peer_id,
                ok = true,
                prefer_want_have,
                force_want_block,
                want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                source_transport = result.source_transport.unwrap_or("unknown"),
                bytes = requested_bytes,
                requested_blocks = result.requested_blocks.len(),
                extra_blocks = result.extra_blocks.len(),
                elapsed_ms = attempt_started.elapsed().as_millis()
            );
        }
        Err(err) => {
            tracing::info!(
                phase = "bitswap_peer_attempt",
                cid = %primary_cid,
                cids = %cid_summary.as_deref().unwrap_or(""),
                cid_count,
                peer = %peer_id,
                ok = false,
                failure_kind = bitswap_peer_failure_kind_label(err.kind),
                prefer_want_have,
                force_want_block,
                want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                error = %err.detail,
                elapsed_ms = attempt_started.elapsed().as_millis()
            );
        }
    }
    result
}

fn bitswap_peer_failure_kind_label(kind: BitswapPeerFailureKind) -> &'static str {
    match kind {
        BitswapPeerFailureKind::ConnectionTimeout => "connection_timeout",
        BitswapPeerFailureKind::ReadTimeout => "read_timeout",
        BitswapPeerFailureKind::Other => "other",
    }
}

async fn request_bitswap_blocks(
    mut control: StreamControl,
    peer_id: PeerId,
    addrs: Vec<Multiaddr>,
    cids: Vec<Cid>,
    prefer_want_have: bool,
    request_timeouts: BitswapRequestTimeouts,
    peer_transports: PeerTransportLog,
) -> std::result::Result<BitswapFetchBatchResult, BitswapPeerFailure> {
    let mut failures = Vec::new();
    let primary_cid = cids[0];
    let cid_count = cids.len();
    let cid_summary = format_cids(&cids);
    for protocol in bitswap_protocols() {
        let protocol_name = protocol.to_string();
        let stream = timeout(
            Duration::from_secs(10),
            control.open_stream(peer_id, protocol.clone()),
        )
        .await;
        let mut stream = match stream {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                failures.push(BitswapProtocolFailure::other(format!(
                    "{protocol_name}: open failed for {peer_id} addrs={}: {}",
                    format_multiaddrs(&addrs),
                    format_error_detail(&err)
                )));
                continue;
            }
            Err(_) => {
                failures.push(BitswapProtocolFailure::other(format!(
                    "{protocol_name}: open timed out"
                )));
                continue;
            }
        };

        if cid_count == 1 && prefer_want_have && protocol_name == "/ipfs/bitswap/1.2.0" {
            match request_bitswap_block_after_want_have(
                &mut stream,
                &primary_cid,
                &protocol_name,
                request_timeouts,
            )
            .await
            {
                Ok(result) => {
                    return Ok(BitswapFetchBatchResult {
                        requested_blocks: HashMap::from([(primary_cid, result.requested_block)]),
                        extra_blocks: result.extra_blocks,
                        source_peer: Some(peer_id),
                        source_transport: current_peer_transport(&peer_transports, peer_id).await,
                        delivery: result.delivery,
                    });
                }
                Err(WantHaveFailure::TryOtherProtocols(err)) => {
                    let read_timed_out = err.kind == BitswapPeerFailureKind::ReadTimeout;
                    failures.push(err);
                    if read_timed_out {
                        break;
                    }
                    continue;
                }
                Err(WantHaveFailure::PeerDoesNotHave(err)) => {
                    failures.push(err);
                    break;
                }
            }
        }

        match request_bitswap_blocks_on_stream(
            &mut stream,
            &cids,
            &protocol_name,
            request_timeouts.stream_read,
        )
        .await
        {
            Ok(result) => {
                return Ok(BitswapFetchBatchResult {
                    requested_blocks: result.requested_blocks,
                    extra_blocks: result.extra_blocks,
                    source_peer: Some(peer_id),
                    source_transport: current_peer_transport(&peer_transports, peer_id).await,
                    delivery: "outgoing",
                });
            }
            Err(err) => {
                let read_timed_out = err.kind == BitswapPeerFailureKind::ReadTimeout;
                failures.push(err);
                if read_timed_out {
                    break;
                }
            }
        }
    }
    let kind = if failures
        .iter()
        .any(|failure| failure.kind == BitswapPeerFailureKind::ReadTimeout)
    {
        BitswapPeerFailureKind::ReadTimeout
    } else {
        BitswapPeerFailureKind::Other
    };
    Err(BitswapPeerFailure {
        id: peer_id,
        kind,
        detail: format!(
            "{peer_id}: no supported Bitswap protocol returned cids {cid_summary}; addrs={}; failures=({})",
            format_multiaddrs(&addrs),
            failures
                .iter()
                .map(|failure| failure.detail.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        ),
    })
}

async fn request_bitswap_block_after_want_have<T>(
    stream: &mut T,
    cid: &Cid,
    protocol_name: &str,
    request_timeouts: BitswapRequestTimeouts,
) -> std::result::Result<BitswapFetchResult, WantHaveFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(err) = write_bitswap_want_have(stream, cid).await {
        return Err(WantHaveFailure::TryOtherProtocols(
            BitswapProtocolFailure::other(format!(
                "{protocol_name}: write want-have failed: {err}"
            )),
        ));
    }
    let response = match timeout(request_timeouts.want_have, read_bitswap_response(stream)).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            return Err(WantHaveFailure::TryOtherProtocols(
                BitswapProtocolFailure::other(format!(
                    "{protocol_name}: read want-have failed: {err}"
                )),
            ))
        }
        Err(_) => {
            return request_bitswap_block_on_stream(
                stream,
                cid,
                protocol_name,
                request_timeouts.stream_read,
            )
            .await
            .map_err(WantHaveFailure::TryOtherProtocols);
        }
    };
    let has_dont_have = response.has_presence(cid, BLOCK_PRESENCE_DONT_HAVE);
    let has_have = response.has_presence(cid, BLOCK_PRESENCE_HAVE);
    if let Some(result) = collect_bitswap_result(cid, response.blocks) {
        let _ = write_bitswap_cancel(stream, cid).await;
        return Ok(result);
    }
    if has_dont_have {
        return Err(WantHaveFailure::PeerDoesNotHave(
            BitswapProtocolFailure::other(format!("{protocol_name}: peer returned DONT_HAVE")),
        ));
    }
    if !has_have {
        return request_bitswap_block_on_stream(
            stream,
            cid,
            protocol_name,
            request_timeouts.stream_read,
        )
        .await
        .map_err(WantHaveFailure::TryOtherProtocols);
    }
    request_bitswap_block_on_stream(stream, cid, protocol_name, request_timeouts.stream_read)
        .await
        .map_err(WantHaveFailure::TryOtherProtocols)
}

async fn request_bitswap_block_on_stream<T>(
    stream: &mut T,
    cid: &Cid,
    protocol_name: &str,
    stream_read_timeout: Duration,
) -> std::result::Result<BitswapFetchResult, BitswapProtocolFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut results =
        request_bitswap_blocks_on_stream(stream, &[*cid], protocol_name, stream_read_timeout)
            .await?;
    let Some(requested_block) = results.requested_blocks.remove(cid) else {
        return Err(BitswapProtocolFailure::other(format!(
            "{protocol_name}: no valid block returned"
        )));
    };
    Ok(BitswapFetchResult {
        requested_block,
        extra_blocks: results.extra_blocks,
        source_peer: None,
        source_transport: None,
        delivery: "outgoing",
    })
}

async fn request_bitswap_blocks_on_stream<T>(
    stream: &mut T,
    cids: &[Cid],
    protocol_name: &str,
    stream_read_timeout: Duration,
) -> std::result::Result<BitswapFetchResults, BitswapProtocolFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(err) = write_bitswap_wants(stream, cids).await {
        return Err(BitswapProtocolFailure::other(format!(
            "{protocol_name}: write failed: {err}"
        )));
    }
    let blocks = match timeout(stream_read_timeout, read_bitswap_blocks(stream)).await {
        Ok(Ok(blocks)) => blocks,
        Ok(Err(err)) => {
            return Err(BitswapProtocolFailure::other(format!(
                "{protocol_name}: read failed: {err}"
            )))
        }
        Err(_) => {
            return Err(BitswapProtocolFailure::read_timeout(format!(
                "{protocol_name}: read timed out"
            )))
        }
    };
    let results = collect_bitswap_results(cids, blocks);
    if results.requested_blocks.len() == cids.len() {
        let _ = write_bitswap_cancels(stream, cids).await;
        return Ok(results);
    }
    Err(BitswapProtocolFailure::other(format!(
        "{protocol_name}: no valid block returned"
    )))
}

fn bitswap_protocols() -> [StreamProtocol; 3] {
    [
        StreamProtocol::new("/ipfs/bitswap/1.2.0"),
        StreamProtocol::new("/ipfs/bitswap/1.1.0"),
        StreamProtocol::new("/ipfs/bitswap/1.0.0"),
    ]
}

async fn write_bitswap_wants<T>(io: &mut T, cids: &[Cid]) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let message = bitswap_want_message_with_type(cids, false, WantType::Block);
    write_length_prefixed(io, &message.encode_to_vec()).await?;
    io.flush().await
}

async fn write_bitswap_want_have<T>(io: &mut T, cid: &Cid) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let message = bitswap_want_message_with_type(&[*cid], false, WantType::Have);
    write_length_prefixed(io, &message.encode_to_vec()).await?;
    io.flush().await
}

async fn write_bitswap_cancel<T>(io: &mut T, cid: &Cid) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    write_bitswap_cancels(io, &[*cid]).await
}

async fn write_bitswap_cancels<T>(io: &mut T, cids: &[Cid]) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let message = bitswap_want_message(cids, true);
    write_length_prefixed(io, &message.encode_to_vec()).await?;
    io.flush().await
}

async fn write_empty_bitswap_message<T>(io: &mut T) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    write_length_prefixed(io, &BitswapMessage::default().encode_to_vec()).await?;
    io.flush().await
}

fn bitswap_want_message(cids: &[Cid], cancel: bool) -> BitswapMessage {
    bitswap_want_message_with_type(cids, cancel, WantType::Block)
}

fn bitswap_want_message_with_type(
    cids: &[Cid],
    cancel: bool,
    want_type: WantType,
) -> BitswapMessage {
    BitswapMessage {
        wantlist: Some(Wantlist {
            entries: cids
                .iter()
                .map(|cid| WantEntry {
                    block: cid.to_bytes(),
                    priority: 1,
                    cancel,
                    want_type: want_type as i32,
                    send_dont_have: true,
                    tokens: Vec::new(),
                })
                .collect(),
            full: false,
        }),
        blocks: Vec::new(),
        payload: Vec::new(),
        block_presences: Vec::new(),
        pending_bytes: 0,
        tokens: Vec::new(),
    }
}

async fn read_bitswap_blocks<T>(io: &mut T) -> io::Result<Vec<ReceivedBitswapBlock>>
where
    T: AsyncRead + Unpin,
{
    for _ in 0..4 {
        let response = read_bitswap_response_once(io).await?;
        if !response.blocks.is_empty() {
            return Ok(response.blocks);
        }
    }
    Ok(Vec::new())
}

async fn read_bitswap_response<T>(io: &mut T) -> io::Result<BitswapResponse>
where
    T: AsyncRead + Unpin,
{
    for _ in 0..4 {
        let response = read_bitswap_response_once(io).await?;
        if !response.is_empty() {
            return Ok(response);
        }
    }
    Ok(BitswapResponse::default())
}

async fn read_bitswap_response_once<T>(io: &mut T) -> io::Result<BitswapResponse>
where
    T: AsyncRead + Unpin,
{
    let bytes = read_length_prefixed(io, 2 * 1024 * 1024 + 4096).await?;
    let message = BitswapMessage::decode(bytes.as_slice()).map_err(invalid_data)?;
    Ok(decode_bitswap_response(message))
}

fn decode_bitswap_response(message: BitswapMessage) -> BitswapResponse {
    let mut blocks = message
        .blocks
        .into_iter()
        .map(|data| ReceivedBitswapBlock { cid: None, data })
        .collect::<Vec<_>>();
    blocks.extend(message.payload.into_iter().map(|payload| {
        let cid = cid_from_bitswap_payload_prefix(&payload.prefix, &payload.data);
        ReceivedBitswapBlock {
            cid,
            data: payload.data,
        }
    }));
    let block_presences = message
        .block_presences
        .into_iter()
        .filter_map(|presence| {
            let cid = Cid::read_bytes(&mut io::Cursor::new(presence.cid)).ok()?;
            Some(ReceivedBlockPresence {
                cid,
                type_pb: presence.type_pb,
            })
        })
        .collect();
    BitswapResponse {
        blocks,
        block_presences,
    }
}

fn collect_bitswap_result(
    requested: &Cid,
    blocks: Vec<ReceivedBitswapBlock>,
) -> Option<BitswapFetchResult> {
    let mut results = collect_bitswap_results(&[*requested], blocks);
    let requested_block = results.requested_blocks.remove(requested)?;
    Some(BitswapFetchResult {
        requested_block,
        extra_blocks: results.extra_blocks,
        source_peer: None,
        source_transport: None,
        delivery: "outgoing",
    })
}

fn collect_bitswap_results(
    requested: &[Cid],
    blocks: Vec<ReceivedBitswapBlock>,
) -> BitswapFetchResults {
    let requested = requested.iter().copied().collect::<BTreeSet<_>>();
    let mut requested_blocks = HashMap::new();
    let mut extra_blocks = Vec::new();

    for block in blocks {
        match block.cid {
            Some(block_cid) if requested.contains(&block_cid) => {
                if verify_block(&block_cid, &block.data).is_ok() {
                    requested_blocks.insert(block_cid, block.data);
                }
            }
            Some(block_cid) => {
                if verify_block(&block_cid, &block.data).is_ok() {
                    extra_blocks.push((block_cid, block.data));
                }
            }
            None => {
                for requested in &requested {
                    if verify_block(requested, &block.data).is_ok() {
                        requested_blocks.insert(*requested, block.data);
                        break;
                    }
                }
            }
        }
    }

    BitswapFetchResults {
        requested_blocks,
        extra_blocks,
    }
}

fn cid_from_bitswap_payload_prefix(prefix: &[u8], data: &[u8]) -> Option<Cid> {
    let (version, rest) = unsigned_varint::decode::u64(prefix).ok()?;
    if !matches!(version, CID_VERSION_0 | CID_VERSION_1) {
        return None;
    }
    let (codec, rest) = unsigned_varint::decode::u64(rest).ok()?;
    let (hash_code, rest) = unsigned_varint::decode::u64(rest).ok()?;
    let (hash_len, rest) = unsigned_varint::decode::u64(rest).ok()?;
    if !rest.is_empty() {
        return None;
    }

    let hash_len = usize::try_from(hash_len).ok()?;
    let hash = match hash_code {
        HASH_SHA2_256 => {
            let hash = Code::Sha2_256.digest(data);
            if hash.digest().len() != hash_len {
                return None;
            }
            hash
        }
        HASH_IDENTITY => {
            if data.len() != hash_len {
                return None;
            }
            Multihash::<64>::wrap(HASH_IDENTITY, data).ok()?
        }
        _ => return None,
    };

    match version {
        CID_VERSION_0 if codec == CODEC_DAG_PB => Cid::new_v0(hash).ok(),
        CID_VERSION_1 => Some(Cid::new_v1(codec, hash)),
        _ => None,
    }
}

async fn read_length_prefixed<T>(io: &mut T, max_size: usize) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin,
{
    let len = read_varint_usize(io).await?;
    if len > max_size {
        return Err(invalid_data("bitswap message too large"));
    }
    let mut bytes = vec![0; len];
    io.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn read_varint_usize<T>(io: &mut T) -> io::Result<usize>
where
    T: AsyncRead + Unpin,
{
    let mut value = 0usize;
    for shift in (0..35).step_by(7) {
        let mut byte = [0u8; 1];
        io.read_exact(&mut byte).await?;
        value |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid_data("bitswap message length varint is too large"))
}

async fn write_length_prefixed<T>(io: &mut T, bytes: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let len = u32::try_from(bytes.len()).map_err(|_| invalid_data("bitswap message too large"))?;
    let mut buffer = unsigned_varint::encode::u32_buffer();
    io.write_all(unsigned_varint::encode::u32(len, &mut buffer))
        .await?;
    io.write_all(bytes).await
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn timeout_http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("HTTP provider client config is valid")
}

#[derive(Clone, PartialEq, Message)]
struct BitswapMessage {
    #[prost(message, optional, tag = "1")]
    wantlist: Option<Wantlist>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    blocks: Vec<Vec<u8>>,
    #[prost(message, repeated, tag = "3")]
    payload: Vec<BlockPayload>,
    #[prost(message, repeated, tag = "4")]
    block_presences: Vec<BlockPresence>,
    #[prost(int32, tag = "5")]
    pending_bytes: i32,
    #[prost(bytes = "vec", repeated, tag = "6")]
    tokens: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct Wantlist {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<WantEntry>,
    #[prost(bool, tag = "2")]
    full: bool,
}

#[derive(Clone, PartialEq, Message)]
struct WantEntry {
    #[prost(bytes = "vec", tag = "1")]
    block: Vec<u8>,
    #[prost(int32, tag = "2")]
    priority: i32,
    #[prost(bool, tag = "3")]
    cancel: bool,
    #[prost(enumeration = "WantType", tag = "4")]
    want_type: i32,
    #[prost(bool, tag = "5")]
    send_dont_have: bool,
    #[prost(int32, repeated, tag = "7")]
    tokens: Vec<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum WantType {
    Block = 0,
    Have = 1,
}

#[derive(Clone, PartialEq, Message)]
struct BlockPayload {
    #[prost(bytes = "vec", tag = "1")]
    prefix: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    data: Vec<u8>,
    #[prost(int32, repeated, tag = "4")]
    tokens: Vec<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct BlockPresence {
    #[prost(bytes = "vec", tag = "1")]
    cid: Vec<u8>,
    #[prost(int32, tag = "2")]
    type_pb: i32,
    #[prost(int32, repeated, tag = "4")]
    tokens: Vec<i32>,
}

#[cfg(test)]
mod bitswap_tests {
    use super::*;
    use serde::Deserialize;
    use std::ffi::OsStr;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use tokio::io::{AsyncReadExt as TokioAsyncReadExt, AsyncWriteExt as TokioAsyncWriteExt};

    #[test]
    fn extracts_supported_peer_multiaddr() {
        let provider = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP");
        let (peer, addr) =
            parse_bitswap_multiaddr("/ip4/164.92.225.198/tcp/4001", provider).unwrap();
        assert_eq!(Some(peer), provider);
        assert_eq!(addr.to_string(), "/ip4/164.92.225.198/tcp/4001");
    }

    #[test]
    fn accepts_quic_multiaddr() {
        let provider = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP");
        let (peer, addr) =
            parse_bitswap_multiaddr("/ip4/164.92.225.198/udp/4001/quic-v1", provider).unwrap();
        assert_eq!(Some(peer), provider);
        assert_eq!(addr.to_string(), "/ip4/164.92.225.198/udp/4001/quic-v1");
    }

    #[test]
    fn accepts_wss_multiaddr() {
        let provider = parse_peer_id("Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4");
        let (peer, addr) = parse_bitswap_multiaddr(
            "/dns4/bitswap-v3.pinata.cloud/tcp/443/wss/p2p/Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4",
            provider,
        )
        .unwrap();
        assert_eq!(Some(peer), provider);
        assert_eq!(
            addr.to_string(),
            "/dns4/bitswap-v3.pinata.cloud/tcp/443/wss"
        );
    }

    #[test]
    fn detects_websocket_dns_multiaddr() {
        assert!(websocket_multiaddr(
            "/dns4/bitswap-v3.pinata.cloud/tcp/443/wss"
        ));
        assert!(websocket_multiaddr("/dns4/example.com/tcp/4001/tls/ws"));
        assert!(!websocket_multiaddr("/dns4/example.com/tcp/4001"));
    }

    #[test]
    fn replaces_non_websocket_dns_multiaddr_with_ip() {
        let replaced = replace_dns_multiaddr(
            "/dns4/example.com/tcp/4001/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP",
            "203.0.113.10".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(
            replaced.to_string(),
            "/ip4/203.0.113.10/tcp/4001/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP"
        );
    }

    #[tokio::test]
    async fn cached_dns_expansion_reuses_dnsaddr_and_ip_results() {
        let mut dnsaddr_cache = HashMap::from([(
            "bootstrap.example".to_string(),
            CachedDnsaddrRecords {
                records: vec![
                    "/dns4/example.com/tcp/4001".to_string(),
                    "/dns4/ws.example/tcp/443/wss".to_string(),
                ],
                log_as_cached: true,
            },
        )]);
        let mut dns_ip_cache = HashMap::from([(
            "example.com".to_string(),
            CachedDnsIpRecords {
                addrs: vec!["203.0.113.10".parse().unwrap()],
                log_as_cached: true,
            },
        )]);

        let expanded = expand_provider_multiaddrs(
            &[
                "/dnsaddr/bootstrap.example".to_string(),
                "/dns4/example.com/tcp/4002".to_string(),
            ],
            &mut dnsaddr_cache,
            &mut dns_ip_cache,
        )
        .await;

        assert_eq!(
            expanded,
            vec![
                "/ip4/203.0.113.10/tcp/4001",
                "/dns4/ws.example/tcp/443/wss",
                "/ip4/203.0.113.10/tcp/4002",
            ]
        );
        assert_eq!(dnsaddr_cache.len(), 1);
        assert_eq!(dns_ip_cache.len(), 1);
    }

    #[test]
    fn rejects_relay_only_bitswap_multiaddr() {
        let provider = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP");
        assert!(
            parse_bitswap_multiaddr("/ip4/164.92.225.198/tcp/4001/p2p-circuit", provider,)
                .is_none()
        );
    }

    #[tokio::test]
    async fn reports_bitswap_provider_address_quality() {
        let peer = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
        let providers = vec![
            Provider::from_parts(
                Some(peer.to_string()),
                vec![
                    "/ip4/164.92.225.198/tcp/4001".to_string(),
                    "/ip4/164.92.225.198/tcp/4001/p2p-circuit".to_string(),
                    "/ip4/198.244.179.206/udp/4001/quic-v1/webtransport/certhash/uEiCc2vNnfKaaSdrNo5CnuqGRSj_kEJASptn3ReObl-hxYw/certhash/uEiCTfMTTtwAYGsEOVyPqyhHxn_BF-I1DCOou7dq9yWizAA/p2p-circuit".to_string(),
                    "/memory/1234".to_string(),
                ],
            )
            .unwrap(),
            Provider::from_parts(Some(peer.to_string()), Vec::new()).unwrap(),
            Provider::from_parts(None, vec!["/ip4/164.92.225.199/tcp/4001".to_string()]).unwrap(),
            Provider::from_parts(
                Some("not-a-peer".to_string()),
                vec!["/ip4/164.92.225.200/tcp/4001".to_string()],
            )
            .unwrap(),
            Provider::from_parts(Some(peer.to_string()), vec!["not-a-multiaddr".to_string()])
                .unwrap(),
        ];

        let candidates = bitswap_peers_with_quality(&providers).await;
        let quality = candidates.quality;

        assert_eq!(candidates.peers.len(), 1);
        assert_eq!(quality.provider_addr_count, 7);
        assert_eq!(quality.expanded_addr_count, 7);
        assert_eq!(quality.supported_addr_count, 1);
        assert_eq!(quality.rejected_addr_count(), 6);
        assert_eq!(quality.id_only_provider_count, 1);
        assert_eq!(quality.invalid_provider_id_count, 1);
        assert_eq!(quality.provider_without_supported_bitswap_addr_count, 2);
        assert_eq!(quality.unsupported_relay_addr_count, 1);
        assert_eq!(quality.unsupported_webtransport_addr_count, 1);
        assert_eq!(quality.unsupported_transport_addr_count, 1);
        assert_eq!(quality.missing_peer_addr_count, 2);
        assert_eq!(quality.unparsable_addr_count, 1);
        assert_eq!(quality.addr_with_relay_count, 2);
        assert_eq!(quality.addr_with_webtransport_count, 1);
        assert_eq!(quality.addr_with_webrtc_count, 0);
        assert_eq!(quality.addr_with_certhash_count, 1);
    }

    #[tokio::test]
    async fn deduplicates_and_caps_bitswap_peer_addresses() {
        let peer = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
        let providers = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec![
                "/ip4/164.92.225.198/udp/4001/quic-v1".to_string(),
                "/ip4/164.92.225.198/tcp/4001".to_string(),
                "/ip4/164.92.225.198/tcp/4001".to_string(),
                "/dns4/example.com/tcp/4002/ws".to_string(),
                "/ip4/164.92.225.199/tcp/4001".to_string(),
                "/ip4/164.92.225.200/tcp/4001".to_string(),
            ],
        )
        .unwrap()];

        let peers = bitswap_peers(&providers).await;

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addrs.len(), MAX_BITSWAP_ADDRS_PER_PEER);
        assert_eq!(
            peers[0].addrs[0].to_string(),
            "/ip4/164.92.225.198/tcp/4001"
        );
        assert_eq!(
            peers[0].addrs[1].to_string(),
            "/ip4/164.92.225.199/tcp/4001"
        );
    }

    #[test]
    fn interleaves_bitswap_dials_by_address_rank() {
        let first = parse_peer_id("12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3").unwrap();
        let second = parse_peer_id("12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12").unwrap();
        let peers = vec![
            BitswapPeer {
                id: first,
                addrs: vec![
                    "/ip4/127.0.0.1/tcp/1001".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/1002".parse().unwrap(),
                ],
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: second,
                addrs: vec![
                    "/ip4/127.0.0.1/tcp/2001".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/2002".parse().unwrap(),
                ],
                skip_want_have: false,
                force_want_block: false,
            },
        ];

        let dials = interleaved_bitswap_dials(&peers);
        let peer_order = dials.iter().map(|(peer, _)| *peer).collect::<Vec<_>>();
        let addr_order = dials
            .iter()
            .map(|(_, addr)| addr.to_string())
            .collect::<Vec<_>>();

        assert_eq!(peer_order, vec![first, second, first, second]);
        assert_eq!(
            addr_order,
            vec![
                "/ip4/127.0.0.1/tcp/1001",
                "/ip4/127.0.0.1/tcp/2001",
                "/ip4/127.0.0.1/tcp/1002",
                "/ip4/127.0.0.1/tcp/2002",
            ]
        );
    }

    #[test]
    fn caps_bitswap_dial_addresses_per_command() {
        let peer_ids = [
            "12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3",
            "12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12",
            "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP",
            "12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH",
        ];
        let peers = peer_ids
            .iter()
            .enumerate()
            .map(|(index, peer)| BitswapPeer {
                id: parse_peer_id(peer).unwrap(),
                addrs: (1..=4)
                    .map(|rank| {
                        format!("/ip4/127.0.0.{}/tcp/{}", index + 1, 1000 + rank)
                            .parse()
                            .unwrap()
                    })
                    .collect(),
                skip_want_have: false,
                force_want_block: false,
            })
            .collect::<Vec<_>>();

        let (dials, suppressed) = limited_interleaved_bitswap_dials(&peers);
        let addr_order = dials
            .iter()
            .map(|(_, addr)| addr.to_string())
            .collect::<Vec<_>>();

        assert_eq!(dials.len(), MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND);
        assert_eq!(suppressed, 11);
        assert_eq!(
            addr_order,
            vec![
                "/ip4/127.0.0.1/tcp/1001",
                "/ip4/127.0.0.2/tcp/1001",
                "/ip4/127.0.0.3/tcp/1001",
                "/ip4/127.0.0.4/tcp/1001",
                "/ip4/127.0.0.1/tcp/1002",
            ]
        );
    }

    #[test]
    fn formats_bitswap_peer_timeout_summary() {
        let first = parse_peer_id("12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3").unwrap();
        let second = parse_peer_id("12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12").unwrap();
        let third = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let fourth = parse_peer_id("12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH").unwrap();
        let fifth = parse_peer_id("12D3KooWCL2pXbQVaVnJntNZFNvz58PdY9gXo2R6NJajnDyhzxc4").unwrap();
        let peers = vec![
            BitswapPeer {
                id: first,
                addrs: vec![
                    "/ip4/127.0.0.1/tcp/1001".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/1002".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/1003".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/1004".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/1005".parse().unwrap(),
                ],
                skip_want_have: true,
                force_want_block: false,
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: third,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: fourth,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: fifth,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
        ];

        assert_eq!(
            format_bitswap_peers(&peers),
            format!(
                "{first}:want-block@[/ip4/127.0.0.1/tcp/1001,/ip4/127.0.0.1/tcp/1002,+3 more]; {second}:want-block@[]; {third}:want-block@[]; {fourth}:want-block@[]; {fifth}:want-have@[]"
            )
        );
        assert_eq!(bitswap_request_target_mode_counts(&peers), (4, 1));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bitswap_client_swarm_does_not_listen_for_inbound_peers() {
        let swarm = build_bitswap_swarm().await.unwrap();

        assert_eq!(swarm.listeners().count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_bitswap_swarm_exits_when_commands_close() {
        let swarm = build_bitswap_swarm().await.unwrap();
        let mut control = swarm.behaviour().stream.new_control();
        let incoming = accept_bitswap_streams(&mut control).unwrap();
        let (commands, receiver) = mpsc::channel(1);
        let task = tokio::spawn(run_shared_bitswap_swarm(swarm, control, incoming, receiver));

        drop(commands);

        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn incoming_bitswap_stream_reads_are_bounded() {
        let mut stream = PendingReadStream;
        let (result, elapsed_ms, timed_out) =
            read_incoming_bitswap_blocks(&mut stream, Duration::from_millis(10)).await;

        assert!(timed_out);
        let Err(err) = result else {
            panic!("pending incoming stream unexpectedly returned blocks");
        };
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(elapsed_ms < 1000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_bitswap_fetch_cancels_open_peer_stream() {
        let data = b"dropped bitswap fetch block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (peer_id, addr, swarm_task, stream_task, want_seen) =
            spawn_bitswap_peer_waiting_for_stream_close(cid).await;
        let client = SharedBitswapClient::spawn().await.unwrap();
        let fetch_task = tokio::spawn(async move {
            client
                .fetch(
                    cid,
                    vec![BitswapPeer {
                        id: peer_id,
                        addrs: vec![addr],
                        skip_want_have: true,
                        force_want_block: false,
                    }],
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(5), want_seen)
            .await
            .unwrap()
            .unwrap();
        fetch_task.abort();
        let _ = fetch_task.await;

        let closed_promptly = tokio::time::timeout(Duration::from_secs(3), stream_task)
            .await
            .unwrap()
            .unwrap();
        assert!(closed_promptly);
        swarm_task.abort();
    }

    #[test]
    fn decodes_bitswap_payload_prefix_to_cid() {
        let data = b"payload block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let prefix = bitswap_payload_prefix(&cid);

        assert_eq!(cid_from_bitswap_payload_prefix(&prefix, data), Some(cid));
    }

    #[test]
    fn decodes_cidv0_bitswap_payload_prefix_to_cid() {
        let data = b"legacy dag-pb payload";
        let cid = Cid::new_v0(Code::Sha2_256.digest(data)).unwrap();
        let mut prefix = Vec::new();
        append_uvarint(&mut prefix, CID_VERSION_0);
        append_uvarint(&mut prefix, freedom_ipfs_core::CODEC_DAG_PB);
        append_uvarint(&mut prefix, cid.hash().code());
        append_uvarint(&mut prefix, cid.hash().digest().len() as u64);

        assert_eq!(cid_from_bitswap_payload_prefix(&prefix, data), Some(cid));
    }

    #[test]
    fn collects_requested_and_extra_bitswap_payload_blocks() {
        let requested_data = b"requested block";
        let extra_data = b"extra linked block";
        let requested =
            freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, requested_data);
        let extra = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, extra_data);

        let result = collect_bitswap_result(
            &requested,
            vec![
                ReceivedBitswapBlock {
                    cid: Some(extra),
                    data: extra_data.to_vec(),
                },
                ReceivedBitswapBlock {
                    cid: Some(requested),
                    data: requested_data.to_vec(),
                },
            ],
        )
        .unwrap();

        assert_eq!(result.requested_block, requested_data);
        assert_eq!(result.extra_blocks, vec![(extra, extra_data.to_vec())]);
    }

    #[test]
    fn collects_multiple_requested_bitswap_payload_blocks() {
        let first_data = b"first requested block";
        let second_data = b"second requested block";
        let invalid_data = b"invalid requested block";
        let extra_data = b"extra multi-want block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let invalid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, invalid_data);
        let extra = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, extra_data);

        let result = collect_bitswap_results(
            &[first, second, invalid],
            vec![
                ReceivedBitswapBlock {
                    cid: Some(second),
                    data: second_data.to_vec(),
                },
                ReceivedBitswapBlock {
                    cid: Some(extra),
                    data: extra_data.to_vec(),
                },
                ReceivedBitswapBlock {
                    cid: Some(first),
                    data: first_data.to_vec(),
                },
                ReceivedBitswapBlock {
                    cid: Some(invalid),
                    data: b"wrong bytes".to_vec(),
                },
            ],
        );

        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert_eq!(result.requested_blocks.get(&second).unwrap(), second_data);
        assert!(!result.requested_blocks.contains_key(&invalid));
        assert_eq!(result.extra_blocks, vec![(extra, extra_data.to_vec())]);
    }

    #[test]
    fn cancel_bitswap_message_revokes_block_want() {
        let data = b"cancel me";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let message = bitswap_want_message(&[cid], true);
        let wantlist = message.wantlist.unwrap();
        let entry = wantlist.entries.first().unwrap();

        assert!(entry.cancel);
        assert_eq!(entry.block, cid.to_bytes());
        assert_eq!(entry.want_type, WantType::Block as i32);
    }

    #[test]
    fn want_have_message_queries_block_presence() {
        let data = b"want-have me";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let message = bitswap_want_message_with_type(&[cid], false, WantType::Have);
        let wantlist = message.wantlist.unwrap();
        let entry = wantlist.entries.first().unwrap();

        assert!(!entry.cancel);
        assert_eq!(entry.block, cid.to_bytes());
        assert_eq!(entry.want_type, WantType::Have as i32);
        assert!(entry.send_dont_have);
    }

    #[test]
    fn multi_want_message_preserves_requested_cids() {
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, b"first");
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, b"second");
        let message = bitswap_want_message_with_type(&[first, second], false, WantType::Block);
        let wantlist = message.wantlist.unwrap();
        let entries = wantlist.entries;

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].block, first.to_bytes());
        assert_eq!(entries[1].block, second.to_bytes());
        assert!(entries.iter().all(|entry| !entry.cancel));
        assert!(entries
            .iter()
            .all(|entry| entry.want_type == WantType::Block as i32));
        assert!(entries.iter().all(|entry| entry.send_dont_have));
    }

    #[test]
    fn labels_bitswap_connection_transport_from_multiaddr() {
        let tcp = Multiaddr::from_str("/ip4/127.0.0.1/tcp/4001").unwrap();
        let quic = Multiaddr::from_str("/ip4/127.0.0.1/udp/4001/quic-v1").unwrap();
        let ws = Multiaddr::from_str("/dns4/example.com/tcp/443/ws").unwrap();
        let wss = Multiaddr::from_str("/dns4/example.com/tcp/443/wss").unwrap();
        let memory = Multiaddr::from_str("/memory/1").unwrap();

        assert_eq!(bitswap_transport_label(&tcp), "tcp");
        assert_eq!(bitswap_transport_label(&quic), "quic");
        assert_eq!(bitswap_transport_label(&ws), "ws");
        assert_eq!(bitswap_transport_label(&wss), "wss");
        assert_eq!(bitswap_transport_label(&memory), "other");
    }

    #[tokio::test]
    async fn tracks_current_bitswap_peer_transport() {
        let transports = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let peer = PeerId::random();

        assert_eq!(current_peer_transport(&transports, peer).await, None);

        record_peer_transport_established(&transports, peer, "tcp").await;
        assert_eq!(current_peer_transport(&transports, peer).await, Some("tcp"));

        record_peer_transport_established(&transports, peer, "quic").await;
        assert_eq!(
            current_peer_transport(&transports, peer).await,
            Some("quic")
        );

        record_peer_transport_closed(&transports, peer, "quic").await;
        assert_eq!(current_peer_transport(&transports, peer).await, Some("tcp"));

        record_peer_transport_closed(&transports, peer, "tcp").await;
        assert_eq!(current_peer_transport(&transports, peer).await, None);
    }

    #[tokio::test]
    async fn multi_want_stream_collects_requested_blocks_and_cancels() {
        let first_data = b"first multi-want stream block";
        let second_data = b"second multi-want stream block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let response = BitswapMessage {
            payload: vec![
                BlockPayload {
                    prefix: bitswap_payload_prefix(&second),
                    data: second_data.to_vec(),
                    tokens: Vec::new(),
                },
                BlockPayload {
                    prefix: bitswap_payload_prefix(&first),
                    data: first_data.to_vec(),
                    tokens: Vec::new(),
                },
            ],
            ..BitswapMessage::default()
        };
        let mut stream = ScriptedBitswapStream::new(length_prefixed_bytes(&response));

        let result = request_bitswap_blocks_on_stream(
            &mut stream,
            &[first, second],
            "/ipfs/bitswap/1.2.0",
            BITSWAP_STREAM_READ_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert_eq!(result.requested_blocks.get(&second).unwrap(), second_data);
        let mut written = futures::io::Cursor::new(stream.written);
        let want = BitswapMessage::decode(
            read_length_prefixed(&mut written, 1024)
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let want_entries = want.wantlist.unwrap().entries;
        assert_eq!(want_entries.len(), 2);
        assert_eq!(want_entries[0].block, first.to_bytes());
        assert_eq!(want_entries[1].block, second.to_bytes());
        assert!(want_entries.iter().all(|entry| !entry.cancel));

        let cancel = BitswapMessage::decode(
            read_length_prefixed(&mut written, 1024)
                .await
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let cancel_entries = cancel.wantlist.unwrap().entries;
        assert_eq!(cancel_entries.len(), 2);
        assert_eq!(cancel_entries[0].block, first.to_bytes());
        assert_eq!(cancel_entries[1].block, second.to_bytes());
        assert!(cancel_entries.iter().all(|entry| entry.cancel));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_want_stream_fetches_multiple_blocks_from_local_peer() {
        let first_data = b"first local multi-want block";
        let second_data = b"second local multi-want block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let (peer_id, addr, peer_swarm_task, peer_stream_task) =
            spawn_multi_want_bitswap_peer(vec![
                (first, first_data.to_vec()),
                (second, second_data.to_vec()),
            ])
            .await;

        let mut swarm = build_bitswap_swarm().await.unwrap();
        let mut control = swarm.behaviour().stream.new_control();
        swarm
            .dial(addr.with_p2p(peer_id).unwrap_or_else(|addr| addr))
            .unwrap();
        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });

        let mut stream = tokio::time::timeout(
            Duration::from_secs(5),
            control.open_stream(peer_id, StreamProtocol::new("/ipfs/bitswap/1.2.0")),
        )
        .await
        .unwrap()
        .unwrap();
        let result = request_bitswap_blocks_on_stream(
            &mut stream,
            &[first, second],
            "/ipfs/bitswap/1.2.0",
            BITSWAP_STREAM_READ_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert_eq!(result.requested_blocks.get(&second).unwrap(), second_data);
        tokio::time::timeout(Duration::from_secs(5), peer_stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
        peer_swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_bitswap_client_fetch_many_requests_multiple_blocks_from_one_peer() {
        let first_data = b"first shared client multi-want block";
        let second_data = b"second shared client multi-want block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let (peer_id, addr, peer_swarm_task, peer_stream_task) =
            spawn_multi_want_bitswap_peer(vec![
                (first, first_data.to_vec()),
                (second, second_data.to_vec()),
            ])
            .await;
        let client = SharedBitswapClient::spawn().await.unwrap();

        let result = client
            .fetch_many(
                vec![first, second],
                vec![BitswapPeer {
                    id: peer_id,
                    addrs: vec![addr],
                    skip_want_have: true,
                    force_want_block: false,
                }],
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert_eq!(result.requested_blocks.get(&second).unwrap(), second_data);
        assert_eq!(result.extra_blocks, Vec::<(Cid, Vec<u8>)>::new());
        assert_eq!(result.source_peer, Some(peer_id));
        assert_eq!(result.delivery, "outgoing");
        tokio::time::timeout(Duration::from_secs(5), peer_stream_task)
            .await
            .unwrap()
            .unwrap();
        peer_swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks() {
        let first_data = b"first incoming shared client multi-want block";
        let second_data = b"second incoming shared client multi-want block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let (peer_id, addr, peer_swarm_task, peer_stream_task) =
            spawn_incoming_multi_want_bitswap_peer(vec![
                (first, first_data.to_vec()),
                (second, second_data.to_vec()),
            ])
            .await;
        let client = SharedBitswapClient::spawn().await.unwrap();

        let result = client
            .fetch_many(
                vec![first, second],
                vec![BitswapPeer {
                    id: peer_id,
                    addrs: vec![addr],
                    skip_want_have: true,
                    force_want_block: false,
                }],
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert_eq!(result.requested_blocks.get(&second).unwrap(), second_data);
        assert_eq!(result.extra_blocks, Vec::<(Cid, Vec<u8>)>::new());
        assert_eq!(result.source_peer, Some(peer_id));
        assert_eq!(result.delivery, "incoming");
        tokio::time::timeout(Duration::from_secs(5), peer_stream_task)
            .await
            .unwrap()
            .unwrap();
        peer_swarm_task.abort();
    }

    #[tokio::test]
    async fn incoming_bitswap_batch_returns_partial_after_quiet_grace() {
        let first_data = b"first partial incoming shared client multi-want block";
        let second_data = b"second missing incoming shared client multi-want block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let (sender, receiver) = mpsc::unbounded_channel();
        sender
            .send(BitswapFetchBatchResult {
                requested_blocks: HashMap::from([(first, first_data.to_vec())]),
                extra_blocks: Vec::new(),
                source_peer: None,
                source_transport: None,
                delivery: "incoming",
            })
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            collect_incoming_bitswap_batch(vec![first, second], receiver),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(result.requested_blocks.len(), 1);
        assert_eq!(result.requested_blocks.get(&first).unwrap(), first_data);
        assert!(!result.requested_blocks.contains_key(&second));
        assert_eq!(result.extra_blocks, Vec::<(Cid, Vec<u8>)>::new());
        assert_eq!(result.delivery, "incoming");
        drop(sender);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_batch_can_use_recent_bitswap_multiwant_peer() {
        let first_data = b"first range batch multiwant block";
        let second_data = b"second range batch multiwant block";
        let first = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first_data);
        let second = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second_data);
        let (peer_id, addr, peer_swarm_task, peer_stream_task) =
            spawn_multi_want_bitswap_peer(vec![
                (first, first_data.to_vec()),
                (second, second_data.to_vec()),
            ])
            .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(peer_id, vec![addr], Duration::from_millis(10))
            .await;
        let provider = FetchingBlockProvider {
            store: store.clone(),
            retriever,
            stats: Arc::new(RetrievalStatsInner::default()),
        };

        let ranges = vec![(first, 1, 5), (second, 2, 7)];
        let result = provider
            .get_block_ranges_async_inner(ranges, true)
            .await
            .unwrap();

        assert_eq!(result[0].as_deref(), Some(&first_data[1..=5]));
        assert_eq!(result[1].as_deref(), Some(&second_data[2..=7]));
        assert_eq!(provider.stats().bitswap_blocks, 2);
        assert_eq!(store.get(&first).unwrap().unwrap().data(), first_data);
        assert_eq!(store.get(&second).unwrap().unwrap().data(), second_data);
        tokio::time::timeout(Duration::from_secs(5), peer_stream_task)
            .await
            .unwrap()
            .unwrap();
        peer_swarm_task.abort();
    }

    #[tokio::test]
    async fn shared_bitswap_client_fetch_many_rejects_empty_batch() {
        let client = SharedBitswapClient::spawn().await.unwrap();
        let err = client.fetch_many(Vec::new(), Vec::new()).await.unwrap_err();

        assert!(
            matches!(err, RetrievalError::Bitswap(message) if message.contains("batch cannot be empty"))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fetches_block_from_local_bitswap_peer() {
        let data = b"local bitswap block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (peer_id, addr, swarm_task, stream_task) =
            spawn_local_bitswap_peer(cid, data.to_vec()).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bitswap_fetch_caches_verified_extra_blocks() {
        let requested_data = b"bitswap requested block with extra";
        let extra_data = b"bitswap verified extra block";
        let requested =
            freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, requested_data);
        let extra = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, extra_data);
        let (peer_id, addr, swarm_task, stream_task) = spawn_bitswap_peer_with_extra_payload(
            requested,
            requested_data.to_vec(),
            vec![(extra, extra_data.to_vec())],
        )
        .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&requested, &[provider], None)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), requested_data);

        let (block, source) = retriever.fetch_block_with_source(&extra).await.unwrap();
        assert_eq!(source, RetrievalSource::Cache);
        assert_eq!(block.data(), extra_data);
        assert_eq!(store.get(&extra).unwrap().unwrap().data(), extra_data);

        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rechecks_cache_after_provider_lookup_before_network_fetch() {
        let data = b"late cached block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (endpoint, request_seen, release_response, routing_task) =
            spawn_gated_delegated_response(r#"{"Providers":[]}"#.to_string()).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        let fetch_cid = cid;
        let fetch = tokio::spawn({
            let retriever = retriever.clone();
            async move { retriever.fetch_block_with_source(&fetch_cid).await }
        });

        request_seen.await.unwrap();
        store.put_block(&cid, data).unwrap();
        release_response.send(()).unwrap();

        let (block, source) = tokio::time::timeout(Duration::from_secs(5), fetch)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(source, RetrievalSource::Cache);
        assert_eq!(block.data(), data);
        routing_task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_provider_lookups_are_cached_briefly() {
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"negative provider cache",
        );
        let requests = Arc::new(AtomicU64::new(0));
        let (endpoint, routing_task) =
            spawn_counting_delegated_response(r#"{"Providers":[]}"#.to_string(), requests.clone())
                .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store,
        );

        let first = retriever
            .fetch_block_uncached_with_source(&cid, None)
            .await
            .unwrap_err();
        let second = retriever
            .fetch_block_uncached_with_source(&cid, None)
            .await
            .unwrap_err();

        assert!(
            is_no_provider_error(&first),
            "unexpected first error: {first:?}"
        );
        assert!(
            is_no_provider_error(&second),
            "unexpected second error: {second:?}"
        );
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_bitswap_client_handles_repeated_block_fetches() {
        let first = b"first shared bitswap block";
        let second = b"second shared bitswap block";
        let first_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first);
        let second_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second);
        let (peer_id, addr, swarm_task, stream_task) = spawn_multi_block_bitswap_peer(vec![
            (first_cid, first.to_vec()),
            (second_cid, second.to_vec()),
        ])
        .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&first_cid, std::slice::from_ref(&provider), None)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), first);

        let (block, source) = retriever
            .fetch_from_providers_with_source(&second_cid, &[provider], None)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), second);
        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);

        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires KUBO_BIN=/path/to/ipfs; starts a loopback Kubo daemon"]
    async fn kubo_bitswap_retrieves_raw_block_from_loopback_daemon() {
        let kubo = std::env::var("KUBO_BIN").expect("set KUBO_BIN=/path/to/ipfs");
        let tempdir = tempfile::tempdir().unwrap();
        let repo = tempdir.path().join("kubo-repo");
        let file = tempdir.path().join("block.txt");
        let data = b"kubo bitswap interop block\n";
        fs::write(&file, data).unwrap();

        kubo_ok(&kubo, &repo, ["init", "--empty-repo"]);
        kubo_ok(
            &kubo,
            &repo,
            [
                "config",
                "Addresses.Swarm",
                "--json",
                r#"["/ip4/127.0.0.1/tcp/0"]"#,
            ],
        );
        kubo_ok(
            &kubo,
            &repo,
            [
                "config",
                "Addresses.API",
                "--json",
                r#"["/ip4/127.0.0.1/tcp/0"]"#,
            ],
        );
        kubo_ok(
            &kubo,
            &repo,
            [
                "config",
                "Addresses.Gateway",
                "--json",
                r#"["/ip4/127.0.0.1/tcp/0"]"#,
            ],
        );

        let cid = kubo_stdout(
            &kubo,
            &repo,
            [
                OsStr::new("add"),
                OsStr::new("-Q"),
                OsStr::new("--cid-version=1"),
                OsStr::new("--raw-leaves=true"),
                file.as_os_str(),
            ],
        );
        let cid = String::from_utf8(cid)
            .unwrap()
            .trim()
            .parse::<Cid>()
            .unwrap();
        let expected = kubo_stdout(
            &kubo,
            &repo,
            [
                OsStr::new("block"),
                OsStr::new("get"),
                OsStr::new(&cid.to_string()),
            ],
        );
        assert_eq!(expected, data);

        let mut daemon = KuboDaemon::spawn(&kubo, repo.clone()).await;
        let id = daemon.id().await;
        let addr = id
            .addresses
            .unwrap_or_default()
            .into_iter()
            .find(|addr| addr.starts_with("/ip4/127.0.0.1/") && addr.contains("/tcp/"))
            .unwrap_or_else(|| panic!("Kubo did not advertise a loopback TCP address"));

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(Some(id.id), vec![addr]).unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected.as_slice());
        assert_eq!(
            store.get(&cid).unwrap().unwrap().data(),
            expected.as_slice()
        );
        daemon.stop();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_peer_bitswap_directs_first_untrusted_then_uses_want_have() {
        let data = b"want-have selected block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (first_peer_id, first_addr, first_swarm, first_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (second_peer_id, second_addr, second_swarm, second_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (third_peer_id, third_addr, third_swarm, third_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (present_peer_id, present_addr, present_swarm, present_stream) =
            spawn_want_have_bitswap_peer_with_presence_delay(
                cid,
                data.to_vec(),
                true,
                Some(Duration::from_millis(250)),
            )
            .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let providers = vec![
            Provider::from_parts(
                Some(first_peer_id.to_string()),
                vec![first_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(second_peer_id.to_string()),
                vec![second_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(third_peer_id.to_string()),
                vec![third_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(present_peer_id.to_string()),
                vec![present_addr.to_string()],
            )
            .unwrap(),
        ];

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &providers, None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        tokio::time::timeout(Duration::from_secs(5), first_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), second_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), third_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), present_stream)
            .await
            .unwrap()
            .unwrap();
        first_swarm.abort();
        second_swarm.abort();
        third_swarm.abort();
        present_swarm.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn want_have_probe_falls_back_to_want_block_quickly() {
        let data = b"want-have timeout fallback block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (first_peer_id, first_addr, first_swarm, first_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (second_peer_id, second_addr, second_swarm, second_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (third_peer_id, third_addr, third_swarm, third_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (missing_peer_id, missing_addr, missing_swarm, missing_stream) =
            spawn_want_have_bitswap_peer(cid, data.to_vec(), false).await;
        let (present_peer_id, present_addr, present_swarm, present_stream) =
            spawn_silent_want_have_bitswap_peer(cid, data.to_vec()).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let providers = vec![
            Provider::from_parts(
                Some(first_peer_id.to_string()),
                vec![first_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(second_peer_id.to_string()),
                vec![second_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(third_peer_id.to_string()),
                vec![third_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(missing_peer_id.to_string()),
                vec![missing_addr.to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(present_peer_id.to_string()),
                vec![present_addr.to_string()],
            )
            .unwrap(),
        ];

        let started = Instant::now();
        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &providers, None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "WANT_HAVE fallback should not add seconds to TTFB"
        );
        tokio::time::timeout(Duration::from_secs(5), first_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), second_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), third_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), missing_stream)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), present_stream)
            .await
            .unwrap()
            .unwrap();
        first_swarm.abort();
        second_swarm.abort();
        third_swarm.abort();
        missing_swarm.abort();
        present_swarm.abort();
    }

    #[tokio::test]
    async fn rejects_redirected_http_provider_blocks() {
        let data = b"redirect target block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (addr, server_task) = spawn_redirecting_http_provider(cid, data.to_vec()).await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![format!("/ip4/{}/tcp/{}/http", addr.ip(), addr.port())],
        )
        .unwrap_or_else(|_| panic!("failed to build HTTP provider for {addr}"));

        let err = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap_err();

        assert!(matches!(err, RetrievalError::NoHttpProviders));
        assert!(store.get(&cid).unwrap().is_none());
        server_task.abort();
    }

    #[tokio::test]
    async fn rejects_invalid_http_provider_blocks() {
        let expected = b"expected block";
        let invalid = b"wrong block bytes";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let (addr, server_task) = spawn_static_http_provider(invalid.to_vec()).await;
        let provider_url = format!("http://{}:{}/", addr.ip(), addr.port());
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![format!("/ip4/{}/tcp/{}/http", addr.ip(), addr.port())],
        )
        .unwrap_or_else(|_| panic!("failed to build HTTP provider for {addr}"));

        let err = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap_err();

        assert!(matches!(err, RetrievalError::NoHttpProviders));
        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.is_bad_provider(&provider_url).unwrap());
        server_task.abort();
    }

    #[tokio::test]
    async fn rejects_oversized_http_provider_blocks() {
        let expected = b"expected small block";
        let oversized = vec![0u8; DEFAULT_MAX_BLOCK_SIZE + 1];
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let (addr, server_task) = spawn_static_http_provider(oversized).await;
        let provider_url = format!("http://{}:{}/", addr.ip(), addr.port());
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![format!("/ip4/{}/tcp/{}/http", addr.ip(), addr.port())],
        )
        .unwrap_or_else(|_| panic!("failed to build HTTP provider for {addr}"));

        let err = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap_err();

        assert!(matches!(err, RetrievalError::NoHttpProviders));
        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.is_bad_provider(&provider_url).unwrap());
        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn races_http_provider_candidates_and_returns_first_verified_block() {
        let expected = b"verified fast HTTP provider block";
        let invalid = b"slow invalid HTTP provider block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let slow_requests = Arc::new(AtomicU64::new(0));
        let fast_requests = Arc::new(AtomicU64::new(0));
        let (slow_addr, slow_task) = spawn_counting_http_provider(
            invalid.to_vec(),
            Duration::from_millis(250),
            slow_requests.clone(),
        )
        .await;
        let (fast_addr, fast_task) =
            spawn_counting_http_provider(expected.to_vec(), Duration::ZERO, fast_requests.clone())
                .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![
                format!("/ip4/{}/tcp/{}/http", slow_addr.ip(), slow_addr.port()),
                format!("/ip4/{}/tcp/{}/http", fast_addr.ip(), fast_addr.port()),
            ],
        )
        .unwrap_or_else(|_| panic!("failed to build HTTP providers"));

        let started = Instant::now();
        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), expected);
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "HTTP provider race should not wait for the slow invalid provider"
        );
        assert_eq!(fast_requests.load(Ordering::Relaxed), 1);
        assert_eq!(slow_requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        slow_task.abort();
        fast_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hedges_slow_http_provider_race_with_extra_candidate() {
        let expected = b"verified hedged HTTP provider block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let slow_requests = Arc::new(AtomicU64::new(0));
        let fast_requests = Arc::new(AtomicU64::new(0));
        let (slow_a_addr, slow_a_task) = spawn_hanging_http_provider(slow_requests.clone()).await;
        let (slow_b_addr, slow_b_task) = spawn_hanging_http_provider(slow_requests.clone()).await;
        let (fast_addr, fast_task) =
            spawn_counting_http_provider(expected.to_vec(), Duration::ZERO, fast_requests.clone())
                .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![
                format!("/ip4/{}/tcp/{}/http", slow_a_addr.ip(), slow_a_addr.port()),
                format!("/ip4/{}/tcp/{}/http", slow_b_addr.ip(), slow_b_addr.port()),
                format!("/ip4/{}/tcp/{}/http", fast_addr.ip(), fast_addr.port()),
            ],
        )
        .unwrap_or_else(|_| panic!("failed to build HTTP providers"));

        let started = Instant::now();
        let (block, source) = tokio::time::timeout(
            Duration::from_secs(2),
            retriever.fetch_from_providers_with_source(&cid, &[provider], None),
        )
        .await
        .expect("hedged HTTP provider fetch timed out")
        .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), expected);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "hedged provider should return before the base HTTP timeout"
        );
        assert_eq!(fast_requests.load(Ordering::Relaxed), 1);
        assert_eq!(slow_requests.load(Ordering::Relaxed), 2);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        slow_a_task.abort();
        slow_b_task.abort();
        fast_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefers_scored_fast_http_provider_in_initial_race_width() {
        let expected = b"verified scored HTTP provider block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let slow_requests = Arc::new(AtomicU64::new(0));
        let fast_requests = Arc::new(AtomicU64::new(0));
        let (slow_a_addr, slow_a_task) = spawn_hanging_http_provider(slow_requests.clone()).await;
        let (slow_b_addr, slow_b_task) = spawn_hanging_http_provider(slow_requests.clone()).await;
        let (fast_addr, fast_task) =
            spawn_counting_http_provider(expected.to_vec(), Duration::ZERO, fast_requests.clone())
                .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let fast_only_provider = Provider::from_parts(
            None,
            vec![format!(
                "/ip4/{}/tcp/{}/http",
                fast_addr.ip(),
                fast_addr.port()
            )],
        )
        .unwrap_or_else(|_| panic!("failed to build fast HTTP provider"));

        let (_block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[fast_only_provider], None)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::HttpProvider);

        let mixed_provider = Provider::from_parts(
            None,
            vec![
                format!("/ip4/{}/tcp/{}/http", slow_a_addr.ip(), slow_a_addr.port()),
                format!("/ip4/{}/tcp/{}/http", slow_b_addr.ip(), slow_b_addr.port()),
                format!("/ip4/{}/tcp/{}/http", fast_addr.ip(), fast_addr.port()),
            ],
        )
        .unwrap_or_else(|_| panic!("failed to build mixed HTTP providers"));

        let started = Instant::now();
        let (block, source) = tokio::time::timeout(
            Duration::from_secs(2),
            retriever.fetch_from_providers_with_source(&cid, &[mixed_provider], None),
        )
        .await
        .expect("scored HTTP provider fetch timed out")
        .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), expected);
        assert!(
            started.elapsed() < HTTP_PROVIDER_HEDGE_AFTER,
            "scored fast provider should be in the initial race window"
        );
        assert_eq!(fast_requests.load(Ordering::Relaxed), 2);
        assert_eq!(slow_requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        slow_a_task.abort();
        slow_b_task.abort();
        fast_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn self_hedges_slow_single_http_provider() {
        let expected = b"verified single HTTP self hedge block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let self_hedge_after = single_http_provider_self_hedge_after();
        let requests = Arc::new(AtomicU64::new(0));
        let (addr, task) = spawn_sequenced_http_provider(
            expected.to_vec(),
            std::collections::VecDeque::from([
                self_hedge_after + Duration::from_millis(300),
                Duration::ZERO,
            ]),
            requests.clone(),
        )
        .await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider = Provider::from_parts(
            None,
            vec![format!("/ip4/{}/tcp/{}/http", addr.ip(), addr.port())],
        )
        .unwrap_or_else(|_| panic!("failed to build single HTTP provider"));

        let started = Instant::now();
        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), expected);
        assert!(
            started.elapsed() < self_hedge_after + Duration::from_millis(250),
            "self hedge should return before the first slow request"
        );
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        task.abort();
    }

    #[tokio::test]
    async fn single_http_self_hedge_score_gate_skips_fast_scored_provider() {
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"score gated single HTTP self hedge",
        );
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let base = Url::parse("https://fast-provider.example/").unwrap();

        assert!(
            retriever
                .single_http_provider_self_hedge_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "unscored providers keep the existing self-hedge protection"
        );

        retriever
            .record_http_provider_success(&base, Duration::from_millis(50))
            .await;
        assert!(
            !retriever
                .single_http_provider_self_hedge_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "recently fast providers should skip opt-in duplicate self-hedges"
        );

        retriever
            .record_http_provider_success(&base, Duration::from_millis(500))
            .await;
        assert!(
            retriever
                .single_http_provider_self_hedge_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "slow scored providers should still get the self-hedge tail guard"
        );
    }

    #[tokio::test]
    async fn single_http_post_lookup_race_score_gate_skips_unscored_and_fast_providers() {
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"score gated single HTTP post lookup race",
        );
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let base = Url::parse("https://post-lookup-provider.example/").unwrap();

        assert!(
            !retriever
                .single_http_post_lookup_race_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "unscored providers should skip the narrower duplicate race"
        );

        retriever
            .record_http_provider_success(&base, Duration::from_millis(50))
            .await;
        assert!(
            !retriever
                .single_http_post_lookup_race_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "recently fast providers should not race the shortcut"
        );

        retriever
            .record_http_provider_success(&base, Duration::from_millis(500))
            .await;
        assert!(
            retriever
                .single_http_post_lookup_race_score_allows_with_min(
                    &cid,
                    &base,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "slow scored providers should still race the shortcut"
        );
    }

    #[tokio::test]
    async fn multi_http_fast_post_lookup_race_requires_fast_scored_provider() {
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"score gated multi HTTP post lookup race",
        );
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let fast_base = Url::parse("https://fast-post-lookup-provider.example/").unwrap();
        let other_base = Url::parse("https://other-post-lookup-provider.example/").unwrap();
        let providers = vec![Provider {
            id: None,
            addrs: Vec::new(),
            http_urls: vec![fast_base.clone(), other_base],
        }];

        assert!(
            !retriever
                .multi_http_fast_post_lookup_race_allows_with_max(
                    &cid,
                    &providers,
                    2,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "unscored multi-HTTP providers should keep the existing wait"
        );

        retriever
            .record_http_provider_success(&fast_base, Duration::from_millis(50))
            .await;
        assert!(
            retriever
                .multi_http_fast_post_lookup_race_allows_with_max(
                    &cid,
                    &providers,
                    2,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "recently fast multi-HTTP providers should skip the wait tax"
        );

        retriever
            .record_http_provider_success(&fast_base, Duration::from_millis(500))
            .await;
        assert!(
            !retriever
                .multi_http_fast_post_lookup_race_allows_with_max(
                    &cid,
                    &providers,
                    2,
                    Some(Duration::from_millis(100)),
                )
                .await,
            "slow scored multi-HTTP providers should keep the shortcut wait"
        );
    }

    #[test]
    fn multi_http_post_lookup_race_default_is_enabled_with_rollback() {
        assert!(multi_http_post_lookup_race_enabled_from_env_value(
            false, false
        ));
        assert!(multi_http_post_lookup_race_enabled_from_env_value(
            false, true
        ));
        assert!(!multi_http_post_lookup_race_enabled_from_env_value(
            true, false
        ));
        assert!(!multi_http_post_lookup_race_enabled_from_env_value(
            true, true
        ));
    }

    #[test]
    fn zero_http_direct_want_block_peer_limit_parses_override() {
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_env_value(None),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_env_value(Some("0")),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_env_value(Some("bad")),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_env_value(Some("5")),
            Some(5)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(false, None, None),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                false,
                None,
                Some(RetrievalRequestContext::gateway_request(Some(1))),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                true,
                None,
                Some(RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                true,
                None,
                Some(RetrievalRequestContext::gateway_request(None)),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                false,
                Some("5"),
                Some(RetrievalRequestContext::gateway_request(None)),
            ),
            Some(5)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                true,
                Some("5"),
                Some(RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(5)
        );
    }

    #[test]
    fn zero_http_direct_want_block_peer_limit_marks_only_zero_http_untrusted() {
        let providers = vec![Provider {
            id: None,
            addrs: Vec::new(),
            http_urls: Vec::new(),
        }];
        let mut peers = (0..5)
            .map(|index| BitswapPeer {
                id: PeerId::random(),
                addrs: Vec::new(),
                skip_want_have: index == 0,
                force_want_block: false,
            })
            .collect::<Vec<_>>();

        let marked = maybe_force_zero_http_direct_want_block_peers_with_limit(
            &providers,
            &mut peers,
            Some(2),
        );

        assert_eq!(marked, 2);
        assert!(!peers[0].force_want_block, "trusted peer is already direct");
        assert!(peers[1].force_want_block);
        assert!(peers[2].force_want_block);
        assert!(!peers[3].force_want_block);
        assert!(!peers[4].force_want_block);

        let http_providers = vec![Provider {
            id: None,
            addrs: Vec::new(),
            http_urls: vec![Url::parse("https://provider.example/").unwrap()],
        }];
        let marked = maybe_force_zero_http_direct_want_block_peers_with_limit(
            &http_providers,
            &mut peers,
            Some(5),
        );

        assert_eq!(marked, 0);
    }

    #[test]
    fn multi_http_fast_post_lookup_race_default_is_enabled_with_rollback() {
        assert_eq!(
            multi_http_fast_post_lookup_race_max_score_from_env_value(false, None),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            multi_http_fast_post_lookup_race_max_score_from_env_value(
                false,
                Some(std::borrow::Cow::Borrowed("50")),
            ),
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            multi_http_fast_post_lookup_race_max_score_from_env_value(
                false,
                Some(std::borrow::Cow::Borrowed("invalid")),
            ),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            multi_http_fast_post_lookup_race_max_score_from_env_value(
                true,
                Some(std::borrow::Cow::Borrowed("50")),
            ),
            None
        );
    }

    #[test]
    fn max_concurrent_http_provider_fetches_env_value_parses_override() {
        assert_eq!(
            max_concurrent_http_provider_fetches_from_env_value(None),
            MAX_CONCURRENT_HTTP_PROVIDER_FETCHES
        );
        assert_eq!(
            max_concurrent_http_provider_fetches_from_env_value(Some("6")),
            6
        );
        assert_eq!(
            max_concurrent_http_provider_fetches_from_env_value(Some("0")),
            MAX_CONCURRENT_HTTP_PROVIDER_FETCHES
        );
        assert_eq!(
            max_concurrent_http_provider_fetches_from_env_value(Some("not-a-number")),
            MAX_CONCURRENT_HTTP_PROVIDER_FETCHES
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bitswap_hedge_can_win_against_slow_single_http_provider() {
        let expected = b"verified single HTTP bitswap hedge block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_counting_http_provider(
            expected.to_vec(),
            Duration::from_secs(5),
            http_requests.clone(),
        )
        .await;
        let (peer_id, bitswap_addr, bitswap_swarm, bitswap_stream) =
            spawn_local_bitswap_peer(cid, expected.to_vec()).await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let http_base = Url::parse(&format!("http://{http_addr}/")).unwrap();
        let providers = vec![
            Provider::from_parts(
                None,
                vec![format!(
                    "/ip4/{}/tcp/{}/http",
                    http_addr.ip(),
                    http_addr.port()
                )],
            )
            .unwrap(),
            Provider::from_parts(Some(peer_id.to_string()), vec![bitswap_addr.to_string()])
                .unwrap(),
        ];

        let started = Instant::now();
        let (block, source) = tokio::time::timeout(
            Duration::from_secs(3),
            retriever.fetch_single_http_provider_with_bitswap_hedge(
                &cid,
                vec![http_base],
                providers,
            ),
        )
        .await
        .expect("single HTTP bitswap hedge timed out")
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "bitswap hedge should beat the slow HTTP provider"
        );
        assert_eq!(http_requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        tokio::time::timeout(Duration::from_secs(5), bitswap_stream)
            .await
            .unwrap()
            .unwrap();
        bitswap_swarm.abort();
        http_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn limited_http_response_bytes_reports_body_stats() {
        let data = b"http response byte stats";
        let (addr, server_task) = spawn_static_http_provider(data.to_vec()).await;
        let response = reqwest::get(format!("http://{addr}/ipfs/test"))
            .await
            .unwrap();

        let response = limited_response_bytes(response, DEFAULT_MAX_BLOCK_SIZE)
            .await
            .unwrap();

        assert_eq!(response.bytes, data);
        assert_eq!(response.stats.bytes_read, data.len());
        assert!(response.stats.first_chunk_elapsed.is_some());
        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_concurrent_fetches_for_same_missing_cid() {
        let data = b"shared missing block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let requests = Arc::new(AtomicU64::new(0));
        let provider_delay =
            single_http_provider_self_hedge_after().saturating_sub(Duration::from_millis(50));
        let (addr, server_task) =
            spawn_counting_http_provider(data.to_vec(), provider_delay, requests.clone()).await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: None,
                    addrs: vec![format!("/ip4/{}/tcp/{}/http", addr.ip(), addr.port())],
                }],
                Duration::from_secs(60),
            )
            .unwrap();
        let retriever = Arc::new(HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        ));
        let barrier = Arc::new(tokio::sync::Barrier::new(6));
        let mut tasks = Vec::new();

        for _ in 0..6 {
            let retriever = retriever.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                retriever.fetch_block_with_source(&cid).await.unwrap()
            }));
        }

        for task in tasks {
            let (block, source) = task.await.unwrap();
            assert_eq!(source, RetrievalSource::HttpProvider);
            assert_eq!(block.data(), data);
        }
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        server_task.abort();
    }

    #[tokio::test]
    async fn successful_bitswap_peers_are_preferred_without_want_have() {
        let preferred =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let other = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        retriever
            .record_successful_bitswap_peer(preferred, Vec::new(), Duration::from_millis(25))
            .await;
        let mut peers = vec![
            BitswapPeer {
                id: other,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: preferred,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
        ];

        retriever
            .apply_successful_bitswap_peer_scores(&mut peers)
            .await;

        assert_eq!(peers[0].id, preferred);
        assert!(peers[0].skip_want_have);
        assert_eq!(peers[1].id, other);
        assert!(!peers[1].skip_want_have);
    }

    #[tokio::test]
    async fn lower_latency_successful_bitswap_peers_are_preferred() {
        let slow = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let fast = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let now = Instant::now();
        {
            let mut successes = retriever.successful_bitswap_peers.lock().await;
            successes.insert(
                slow,
                SuccessfulBitswapPeer {
                    seen_at: now,
                    addrs: Vec::new(),
                    last_latency: Duration::from_secs(2),
                },
            );
            successes.insert(
                fast,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_secs(1),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(80),
                },
            );
        }
        let mut peers = vec![
            BitswapPeer {
                id: slow,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
            BitswapPeer {
                id: fast,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
            },
        ];

        retriever
            .apply_successful_bitswap_peer_scores(&mut peers)
            .await;

        assert_eq!(peers[0].id, fast);
        assert!(peers[0].skip_want_have);
        assert_eq!(peers[1].id, slow);
        assert!(peers[1].skip_want_have);
    }

    #[tokio::test]
    async fn recent_bitswap_shortcut_peers_are_latency_ordered() {
        let slow = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let fast = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let now = Instant::now();
        {
            let mut successes = retriever.successful_bitswap_peers.lock().await;
            successes.insert(
                slow,
                SuccessfulBitswapPeer {
                    seen_at: now,
                    addrs: vec!["/ip4/127.0.0.1/tcp/4001".parse().unwrap()],
                    last_latency: Duration::from_secs(2),
                },
            );
            successes.insert(
                fast,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_secs(1),
                    addrs: vec!["/ip4/127.0.0.1/tcp/4002".parse().unwrap()],
                    last_latency: Duration::from_millis(80),
                },
            );
        }

        let peers = retriever.recent_bitswap_peers().await;

        assert_eq!(peers[0].id, fast);
        assert_eq!(peers[1].id, slow);
    }

    #[tokio::test]
    async fn recent_session_only_bitswap_peers_keep_want_block_shortcut() {
        let session_peer =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let provider_peer =
            parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        retriever
            .record_successful_bitswap_peer(
                session_peer,
                vec!["/ip4/127.0.0.1/tcp/4001".parse().unwrap()],
                Duration::from_millis(25),
            )
            .await;
        let mut peers = vec![BitswapPeer {
            id: provider_peer,
            addrs: Vec::new(),
            skip_want_have: false,
            force_want_block: false,
        }];

        let inserted = retriever
            .insert_recent_bitswap_session_peers(&mut peers)
            .await;

        assert_eq!(inserted, 1);
        assert_eq!(peers[0].id, session_peer);
        assert!(peers[0].skip_want_have);
        assert_eq!(peers[1].id, provider_peer);
        assert!(!peers[1].skip_want_have);
    }

    #[tokio::test]
    async fn single_session_shortcut_timeout_temporarily_suppresses_peer() {
        let session_peer =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let peer = BitswapPeer {
            id: session_peer,
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".parse().unwrap()],
            skip_want_have: true,
            force_want_block: false,
        };

        retriever.mark_single_session_shortcut_timeout_peer(&cid, &[peer]);

        assert!(store.is_bad_provider(&session_peer.to_string()).unwrap());
    }

    #[tokio::test]
    async fn broad_session_shortcut_timeout_does_not_suppress_all_peers() {
        let first = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let second = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let peers = [
            BitswapPeer {
                id: first,
                addrs: vec!["/ip4/127.0.0.1/tcp/4001".parse().unwrap()],
                skip_want_have: true,
                force_want_block: false,
            },
            BitswapPeer {
                id: second,
                addrs: vec!["/ip4/127.0.0.2/tcp/4001".parse().unwrap()],
                skip_want_have: true,
                force_want_block: false,
            },
        ];

        retriever.mark_single_session_shortcut_timeout_peer(&cid, &peers);

        assert!(!store.is_bad_provider(&first.to_string()).unwrap());
        assert!(!store.is_bad_provider(&second.to_string()).unwrap());
    }

    #[test]
    fn shortens_request_timeout_for_mixed_trusted_bitswap_candidates() {
        assert_eq!(
            bitswap_request_timeout(10, 1),
            BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT
        );
        assert_eq!(bitswap_request_timeout(10, 0), BITSWAP_REQUEST_TIMEOUT);
        assert_eq!(bitswap_request_timeout(1, 1), BITSWAP_REQUEST_TIMEOUT);
        assert_eq!(
            bitswap_stream_read_timeout(1, 0),
            BITSWAP_SINGLE_UNTRUSTED_STREAM_READ_TIMEOUT
        );
        assert_eq!(
            bitswap_stream_read_timeout(2, 0),
            BITSWAP_STREAM_READ_TIMEOUT
        );
        assert_eq!(
            bitswap_stream_read_timeout(1, 1),
            BITSWAP_STREAM_READ_TIMEOUT
        );
    }

    #[test]
    fn detects_bitswap_connection_ready_failures() {
        let err = RetrievalError::Bitswap(format!(
            "all bitswap stream requests failed: peer: bitswap connection was not established within {}ms",
            BITSWAP_CONNECTION_READY_TIMEOUT.as_millis()
        ));
        assert!(is_bitswap_connection_ready_failure(&err));

        let err = RetrievalError::Bitswap("all bitswap stream requests failed".to_string());
        assert!(!is_bitswap_connection_ready_failure(&err));
    }

    #[test]
    fn bitswap_connection_ready_timeout_env_value_parses_override() {
        assert_eq!(
            bitswap_connection_ready_timeout_from_env_value(None),
            BITSWAP_CONNECTION_READY_TIMEOUT
        );
        assert_eq!(
            bitswap_connection_ready_timeout_from_env_value(Some("3000")),
            Duration::from_secs(3)
        );
        assert_eq!(
            bitswap_connection_ready_timeout_from_env_value(Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_connection_ready_timeout_from_env_value(Some("not-a-number")),
            BITSWAP_CONNECTION_READY_TIMEOUT
        );
    }

    #[test]
    fn identifies_no_provider_errors_for_empty_lookup_retry_skip() {
        assert!(is_no_provider_error(&RetrievalError::NoHttpProviders));
        assert!(is_no_provider_error(&RetrievalError::NoBitswapProviders));
        assert!(!is_no_provider_error(&RetrievalError::Bitswap(
            "peer failed".into()
        )));
    }

    #[test]
    fn detects_connection_limit_dial_errors() {
        assert!(is_connection_limit_error(
            "ConnectionDenied { cause: Exceeded { limit: 16, kind: EstablishedOutgoing } }"
        ));
        assert!(!is_connection_limit_error("Transport failed"));
    }

    #[test]
    fn skips_bitswap_dials_for_connected_or_pending_peers() {
        let connected =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let pending =
            parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let fresh = parse_peer_id("12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3").unwrap();
        let mut connected_peers = HashMap::new();
        connected_peers.insert(connected, 1);
        let (ready, _wait) = oneshot::channel();
        let mut connection_waiters = HashMap::new();
        connection_waiters.insert(pending, vec![ready]);

        assert!(!should_start_bitswap_dial(
            &connected,
            &connected_peers,
            &connection_waiters
        ));
        assert!(!should_start_bitswap_dial(
            &pending,
            &connected_peers,
            &connection_waiters
        ));
        assert!(should_start_bitswap_dial(
            &fresh,
            &connected_peers,
            &connection_waiters
        ));
    }

    #[test]
    fn drops_waiters_for_dials_that_never_started() {
        let failed = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let started =
            parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let (failed_ready, mut failed_wait) = oneshot::channel();
        let (started_ready, _started_wait) = oneshot::channel();
        let mut scheduled_peers = BTreeSet::new();
        scheduled_peers.insert(failed);
        scheduled_peers.insert(started);
        let mut started_peers = BTreeSet::new();
        started_peers.insert(started);
        let mut waiters = HashMap::new();
        waiters.insert(failed, vec![failed_ready]);
        waiters.insert(started, vec![started_ready]);
        let mut wait_started = HashMap::new();
        wait_started.insert(failed, Instant::now());
        wait_started.insert(started, Instant::now());

        let dropped = drop_failed_bitswap_dial_waiters(
            &scheduled_peers,
            &started_peers,
            &mut waiters,
            &mut wait_started,
        );

        assert_eq!(dropped, 1);
        assert!(!waiters.contains_key(&failed));
        assert!(!wait_started.contains_key(&failed));
        assert!(waiters.contains_key(&started));
        assert!(wait_started.contains_key(&started));
        assert!(failed_wait.try_recv().is_err());
    }

    #[test]
    fn backs_off_repeated_connection_error_peers() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let mut backoff = HashMap::new();
        let now = Instant::now();

        assert!(record_connection_error_backoff(
            &mut backoff,
            peer,
            "Multistream select failed: Protocol negotiation failed.",
            now,
        )
        .is_none());
        let state = record_connection_error_backoff(
            &mut backoff,
            peer,
            "Multistream select failed: Protocol negotiation failed.",
            now + Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(state.count, 2);
        assert_eq!(state.class, "protocol_negotiation_failed");
        assert!(connection_error_backoff_remaining_ms(
            &backoff,
            &peer,
            now + Duration::from_millis(20)
        )
        .is_some());
    }

    #[test]
    fn connection_error_backoff_threshold_env_value_parses_override() {
        assert_eq!(
            bitswap_connection_error_backoff_threshold_from_env_value(None),
            BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_from_env_value(Some("1")),
            1
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_from_env_value(Some("0")),
            BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_from_env_value(Some("not-a-number")),
            BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD
        );
    }

    #[test]
    fn connection_error_backoff_resets_after_ttl() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let mut backoff = HashMap::new();
        let now = Instant::now();

        record_connection_error_backoff(
            &mut backoff,
            peer,
            "Multistream select failed: Protocol negotiation failed.",
            now,
        );
        record_connection_error_backoff(
            &mut backoff,
            peer,
            "Multistream select failed: Protocol negotiation failed.",
            now + Duration::from_millis(10),
        );
        prune_connection_error_backoff(
            &mut backoff,
            now + BITSWAP_CONNECTION_ERROR_BACKOFF_TTL + Duration::from_millis(11),
        );

        assert!(backoff.is_empty());
    }

    #[test]
    fn ignores_non_backoff_connection_errors() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let mut backoff = HashMap::new();

        assert!(record_connection_error_backoff(
            &mut backoff,
            peer,
            "Timeout has been reached",
            Instant::now(),
        )
        .is_none());
        assert!(backoff.is_empty());
    }

    #[test]
    fn summarizes_bitswap_peer_address_mix() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let peers = vec![BitswapPeer {
            id: peer,
            addrs: vec![
                "/ip4/127.0.0.1/tcp/4001".parse().unwrap(),
                "/ip6/::1/udp/4001/quic-v1".parse().unwrap(),
                "/dns4/example.com/tcp/443/wss".parse().unwrap(),
                "/dnsaddr/bootstrap.example/tcp/4001/ws".parse().unwrap(),
            ],
            skip_want_have: false,
            force_want_block: false,
        }];

        let stats = bitswap_peer_addr_stats(&peers);

        assert_eq!(stats.tcp, 3);
        assert_eq!(stats.quic, 1);
        assert_eq!(stats.ws, 1);
        assert_eq!(stats.wss, 1);
        assert_eq!(stats.dns, 2);
        assert_eq!(stats.ip4, 1);
        assert_eq!(stats.ip6, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_bitswap_peer_shortcut_fetches_when_provider_lookup_fails() {
        let first = b"session shortcut first block";
        let second = b"session shortcut second block";
        let first_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first);
        let second_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second);
        let (peer_id, addr, swarm_task, stream_task) = spawn_multi_block_bitswap_peer(vec![
            (first_cid, first.to_vec()),
            (second_cid, second.to_vec()),
        ])
        .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&first_cid, &[provider], None)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), first);

        let (block, source) = retriever
            .fetch_block_with_source(&second_cid)
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), second);
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);

        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_bitswap_peer_can_win_after_immediate_provider_lookup() {
        let first = b"session head start first block";
        let second = b"session head start second block";
        let first_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first);
        let second_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second);
        let (peer_id, addr, swarm_task, stream_task) = spawn_multi_block_bitswap_peer(vec![
            (first_cid, first.to_vec()),
            (second_cid, second.to_vec()),
        ])
        .await;
        let delegated_requests = Arc::new(AtomicU64::new(0));
        let (endpoint, routing_task) = spawn_counting_delegated_response(
            r#"{"Providers":[]}"#.into(),
            delegated_requests.clone(),
        )
        .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        retriever
            .fetch_from_providers_with_source(&first_cid, &[provider], None)
            .await
            .unwrap();

        let (block, source) = retriever
            .fetch_block_with_source(&second_cid)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), second);
        assert_eq!(delegated_requests.load(Ordering::Relaxed), 1);

        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_recent_bitswap_peer_can_win_during_slow_provider_lookup() {
        let data = b"late session peer shortcut block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (peer_id, addr, swarm_task, stream_task) =
            spawn_local_bitswap_peer(cid, data.to_vec()).await;
        let (endpoint, request_seen_rx, release_tx, routing_task) =
            spawn_gated_delegated_response(r#"{"Providers":[]}"#.into()).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        let fetch_retriever = retriever.clone();
        let fetch_task =
            tokio::spawn(async move { fetch_retriever.fetch_block_with_source(&cid).await });

        tokio::time::timeout(Duration::from_secs(2), request_seen_rx)
            .await
            .unwrap()
            .unwrap();
        retriever
            .record_successful_bitswap_peer(peer_id, vec![addr], Duration::from_millis(25))
            .await;

        let (block, source) = tokio::time::timeout(Duration::from_secs(3), fetch_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_provider_lookup_waits_for_recent_bitswap_peer() {
        let first = b"empty provider first block";
        let second = b"empty provider follow-on block";
        let first_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, first);
        let second_cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, second);
        let (peer_id, addr, swarm_task, stream_task) = spawn_delayed_multi_block_bitswap_peer(
            vec![(first_cid, first.to_vec()), (second_cid, second.to_vec())],
            Duration::from_millis(250),
        )
        .await;
        let delegated_requests = Arc::new(AtomicU64::new(0));
        let (endpoint, routing_task) = spawn_counting_delegated_response(
            r#"{"Providers":[]}"#.into(),
            delegated_requests.clone(),
        )
        .await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        let provider =
            Provider::from_parts(Some(peer_id.to_string()), vec![addr.to_string()]).unwrap();

        retriever
            .fetch_from_providers_with_source(&first_cid, &[provider], None)
            .await
            .unwrap();

        let (block, source) = retriever
            .fetch_block_with_source(&second_cid)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), second);
        assert_eq!(delegated_requests.load(Ordering::Relaxed), 1);

        tokio::time::timeout(Duration::from_secs(5), stream_task)
            .await
            .unwrap()
            .unwrap();
        swarm_task.abort();
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_bitswap_peer_can_win_after_fast_provider_lookup() {
        let data = b"post lookup session shortcut block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (session_peer_id, session_addr, session_swarm, session_stream) =
            spawn_local_bitswap_peer(cid, data.to_vec()).await;
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_hanging_http_provider(http_requests.clone()).await;
        let response = format!(
            r#"{{"Providers":[{{"ID":"slow-http","Addrs":["/ip4/127.0.0.1/tcp/{}/http"]}}]}}"#,
            http_addr.port()
        );
        let (endpoint, routing_task) = spawn_sequence_delegated_response(vec![response]).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(
                session_peer_id,
                vec![session_addr],
                Duration::from_millis(25),
            )
            .await;

        let (block, source) = tokio::time::timeout(
            Duration::from_secs(2),
            retriever.fetch_block_with_source(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        assert_eq!(http_requests.load(Ordering::Relaxed), 1);

        tokio::time::timeout(Duration::from_secs(5), session_stream)
            .await
            .unwrap()
            .unwrap();
        session_swarm.abort();
        http_task.abort();
        routing_task.abort();
    }

    #[test]
    fn post_lookup_grace_env_value_parses_override() {
        assert_eq!(
            post_lookup_grace_from_env_value(None, BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE),
            BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE
        );
        assert_eq!(
            post_lookup_grace_from_env_value(
                Some("125"),
                BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE
            ),
            Duration::from_millis(125)
        );
        assert_eq!(
            post_lookup_grace_from_env_value(
                Some("0"),
                BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE
            ),
            Duration::from_millis(0)
        );
        assert_eq!(
            post_lookup_grace_from_env_value(
                Some("not-a-number"),
                BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE
            ),
            BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE
        );
    }

    #[test]
    fn pre_lookup_grace_env_value_parses_override() {
        assert_eq!(
            bitswap_session_pre_lookup_grace_from_env_value(None),
            BITSWAP_SESSION_PRE_LOOKUP_GRACE
        );
        assert_eq!(
            bitswap_session_pre_lookup_grace_from_env_value(Some("25")),
            Duration::from_millis(25)
        );
        assert_eq!(
            bitswap_session_pre_lookup_grace_from_env_value(Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_session_pre_lookup_grace_from_env_value(Some("not-a-number")),
            BITSWAP_SESSION_PRE_LOOKUP_GRACE
        );
    }

    #[test]
    fn shortcut_grace_env_value_parses_override() {
        assert_eq!(
            bitswap_session_shortcut_grace_from_env_value(None),
            BITSWAP_SESSION_SHORTCUT_GRACE
        );
        assert_eq!(
            bitswap_session_shortcut_grace_from_env_value(Some("25")),
            Duration::from_millis(25)
        );
        assert_eq!(
            bitswap_session_shortcut_grace_from_env_value(Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_session_shortcut_grace_from_env_value(Some("not-a-number")),
            BITSWAP_SESSION_SHORTCUT_GRACE
        );
    }

    #[test]
    fn incoming_batch_partial_grace_env_value_parses_override() {
        assert_eq!(
            bitswap_incoming_batch_partial_grace_from_env_value(None),
            BITSWAP_INCOMING_BATCH_PARTIAL_GRACE
        );
        assert_eq!(
            bitswap_incoming_batch_partial_grace_from_env_value(Some("15")),
            Duration::from_millis(15)
        );
        assert_eq!(
            bitswap_incoming_batch_partial_grace_from_env_value(Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_incoming_batch_partial_grace_from_env_value(Some("not-a-number")),
            BITSWAP_INCOMING_BATCH_PARTIAL_GRACE
        );
    }

    #[test]
    fn post_lookup_grace_overrides_are_http_width_scoped() {
        let single_http = vec![Provider::from_parts(
            Some("single-http".into()),
            vec!["/ip4/127.0.0.1/tcp/8080/http".into()],
        )
        .unwrap()];
        let multi_http = vec![Provider::from_parts(
            Some("multi-http".into()),
            vec![
                "/ip4/127.0.0.1/tcp/8080/http".into(),
                "/ip4/127.0.0.1/tcp/8081/http".into(),
            ],
        )
        .unwrap()];
        let bitswap_only = vec![Provider::from_parts(
            Some("bitswap-only".into()),
            vec!["/ip4/127.0.0.1/tcp/4001".into()],
        )
        .unwrap()];

        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(&single_http, Some("125"), Some("0")),
            Duration::from_millis(125)
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(&multi_http, Some("125"), None),
            BITSWAP_SESSION_POST_LOOKUP_GRACE
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(&multi_http, Some("125"), Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(&bitswap_only, Some("125"), Some("0")),
            BITSWAP_SESSION_POST_LOOKUP_GRACE
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_http_provider_races_recent_bitswap_peer_by_default() {
        let data = b"single http gives recent peer short grace";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (session_peer_id, session_addr, session_swarm, session_stream) =
            spawn_delayed_multi_block_bitswap_peer(
                vec![(cid, data.to_vec())],
                Duration::from_millis(75),
            )
            .await;
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_hanging_http_provider(http_requests.clone()).await;
        let response = format!(
            r#"{{"Providers":[{{"ID":"slow-http","Addrs":["/ip4/127.0.0.1/tcp/{}/http"]}}]}}"#,
            http_addr.port()
        );
        let (endpoint, routing_task) = spawn_sequence_delegated_response(vec![response]).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(
                session_peer_id,
                vec![session_addr],
                Duration::from_millis(25),
            )
            .await;

        let (block, source) = tokio::time::timeout(
            Duration::from_secs(2),
            retriever.fetch_block_with_source(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert_eq!(http_requests.load(Ordering::Relaxed), 1);

        tokio::time::timeout(Duration::from_secs(5), session_stream)
            .await
            .unwrap()
            .unwrap();
        session_swarm.abort();
        http_task.abort();
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multi_http_provider_keeps_short_recent_peer_wait() {
        let data = b"multi http keeps short recent peer wait";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (session_peer_id, session_addr, session_swarm, session_stream) =
            spawn_delayed_multi_block_bitswap_peer(
                vec![(cid, data.to_vec())],
                Duration::from_millis(175),
            )
            .await;
        let first_http_requests = Arc::new(AtomicU64::new(0));
        let second_http_requests = Arc::new(AtomicU64::new(0));
        let (first_http_addr, first_http_task) = spawn_counting_http_provider(
            data.to_vec(),
            Duration::ZERO,
            first_http_requests.clone(),
        )
        .await;
        let (second_http_addr, second_http_task) = spawn_counting_http_provider(
            data.to_vec(),
            Duration::ZERO,
            second_http_requests.clone(),
        )
        .await;
        let response = format!(
            r#"{{"Providers":[{{"ID":"multi-http","Addrs":["/ip4/127.0.0.1/tcp/{}/http","/ip4/127.0.0.1/tcp/{}/http"]}}]}}"#,
            first_http_addr.port(),
            second_http_addr.port()
        );
        let (endpoint, routing_task) = spawn_sequence_delegated_response(vec![response]).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(
                session_peer_id,
                vec![session_addr],
                Duration::from_millis(25),
            )
            .await;

        let (block, source) = tokio::time::timeout(
            Duration::from_secs(2),
            retriever.fetch_block_with_source(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), data);
        assert!(
            first_http_requests.load(Ordering::Relaxed)
                + second_http_requests.load(Ordering::Relaxed)
                > 0
        );

        session_stream.abort();
        session_swarm.abort();
        first_http_task.abort();
        second_http_task.abort();
        routing_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_bitswap_session_peers_are_raced_with_provider_candidates() {
        let data = b"session peer candidate block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (missing_peer_id, missing_addr, missing_swarm, missing_stream) =
            spawn_closing_bitswap_peer_expect_want_block(cid).await;
        let (session_peer_id, session_addr, session_swarm, session_stream) =
            spawn_local_bitswap_peer(cid, data.to_vec()).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(
                session_peer_id,
                vec![session_addr],
                Duration::from_millis(25),
            )
            .await;
        let provider = Provider::from_parts(
            Some(missing_peer_id.to_string()),
            vec![missing_addr.to_string()],
        )
        .unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider], None)
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        missing_stream.abort();
        tokio::time::timeout(Duration::from_secs(5), session_stream)
            .await
            .unwrap()
            .unwrap();
        missing_swarm.abort();
        session_swarm.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn provider_refresh_after_bitswap_timeout_uses_new_peer() {
        let data = b"provider refresh after silent peer";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let (silent_peer_id, silent_addr, silent_swarm, silent_stream) =
            spawn_silent_bitswap_peer(cid).await;
        let (good_peer_id, good_addr, good_swarm, good_stream) =
            spawn_local_bitswap_peer(cid, data.to_vec()).await;
        let first_response =
            format!(r#"{{"Providers":[{{"ID":"{silent_peer_id}","Addrs":["{silent_addr}"]}}]}}"#);
        let second_response =
            format!(r#"{{"Providers":[{{"ID":"{good_peer_id}","Addrs":["{good_addr}"]}}]}}"#);
        let (endpoint, routing_task) =
            spawn_sequence_delegated_response(vec![first_response, second_response]).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );

        let started = Instant::now();
        let (block, source) = tokio::time::timeout(
            Duration::from_secs(20),
            retriever.fetch_block_with_source(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
        assert!(
            started.elapsed() < BITSWAP_STREAM_READ_TIMEOUT,
            "single untrusted stale provider should refresh before the default stream read timeout"
        );
        assert!(store.is_bad_provider(&silent_peer_id.to_string()).unwrap());
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        tokio::time::timeout(Duration::from_secs(5), good_stream)
            .await
            .unwrap()
            .unwrap();
        silent_stream.abort();
        silent_swarm.abort();
        good_swarm.abort();
        routing_task.abort();
    }

    #[test]
    fn single_bitswap_timeout_peer_is_temporarily_suppressed() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"single timeout peer suppression",
        );
        let peer = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
        let err = RetrievalError::BitswapPeerFailures {
            message: "timed out".into(),
            timeout_peers: vec![peer.to_string()],
            connection_timeout_peers: Vec::new(),
        };

        retriever.mark_bitswap_timeout_peers(&cid, &err, 1);

        assert!(store.is_bad_provider(peer).unwrap());
    }

    #[test]
    fn broad_bitswap_timeouts_are_not_mass_suppressed() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let cid = freedom_ipfs_core::cid_from_data(
            freedom_ipfs_core::CODEC_RAW,
            b"broad timeout peer suppression",
        );
        let peers = ["peer-a", "peer-b", "peer-c", "peer-d"];
        let err = RetrievalError::BitswapPeerFailures {
            message: "broad timeout".into(),
            timeout_peers: peers.iter().map(|peer| (*peer).to_string()).collect(),
            connection_timeout_peers: Vec::new(),
        };

        retriever.mark_bitswap_timeout_peers(&cid, &err, peers.len());

        for peer in peers {
            assert!(!store.is_bad_provider(peer).unwrap());
        }
    }

    async fn spawn_static_http_provider(
        data: Vec<u8>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let data = data.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    if stream.read(&mut request).await.is_err() {
                        return;
                    }
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        data.len()
                    )
                    .into_bytes()
                    .into_iter()
                    .chain(data)
                    .collect::<Vec<_>>();
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        (addr, task)
    }

    async fn spawn_counting_http_provider(
        data: Vec<u8>,
        delay: Duration,
        requests: Arc<AtomicU64>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let data = data.clone();
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    if stream.read(&mut request).await.is_err() {
                        return;
                    }
                    requests.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(delay).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        data.len()
                    )
                    .into_bytes()
                    .into_iter()
                    .chain(data)
                    .collect::<Vec<_>>();
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        (addr, task)
    }

    async fn spawn_sequenced_http_provider(
        data: Vec<u8>,
        delays: std::collections::VecDeque<Duration>,
        requests: Arc<AtomicU64>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let delays = Arc::new(tokio::sync::Mutex::new(delays));
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let data = data.clone();
                let requests = requests.clone();
                let delays = delays.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    if stream.read(&mut request).await.is_err() {
                        return;
                    }
                    requests.fetch_add(1, Ordering::Relaxed);
                    let delay = delays.lock().await.pop_front().unwrap_or_default();
                    tokio::time::sleep(delay).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        data.len()
                    )
                    .into_bytes()
                    .into_iter()
                    .chain(data)
                    .collect::<Vec<_>>();
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        (addr, task)
    }

    async fn spawn_hanging_http_provider(
        requests: Arc<AtomicU64>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    if stream.read(&mut request).await.is_ok() {
                        requests.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                });
            }
        });
        (addr, task)
    }

    async fn spawn_redirecting_http_provider(
        cid: Cid,
        data: Vec<u8>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let data = data.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    let Ok(read) = stream.read(&mut request).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&request[..read]);
                    let response = if request.starts_with("GET /redirected ") {
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            data.len()
                        )
                        .into_bytes()
                        .into_iter()
                        .chain(data)
                        .collect::<Vec<_>>()
                    } else {
                        let location = format!("/redirected?cid={cid}");
                        format!(
                            "HTTP/1.1 302 Found\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        )
                        .into_bytes()
                    };
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        (addr, task)
    }

    fn bitswap_payload_prefix(cid: &Cid) -> Vec<u8> {
        let mut prefix = Vec::new();
        append_uvarint(&mut prefix, CID_VERSION_1);
        append_uvarint(&mut prefix, cid.codec());
        append_uvarint(&mut prefix, cid.hash().code());
        append_uvarint(&mut prefix, cid.hash().digest().len() as u64);
        prefix
    }

    fn append_uvarint(buffer: &mut Vec<u8>, value: u64) {
        let mut encode_buffer = unsigned_varint::encode::u64_buffer();
        buffer.extend_from_slice(unsigned_varint::encode::u64(value, &mut encode_buffer));
    }

    fn length_prefixed_bytes(message: &BitswapMessage) -> Vec<u8> {
        let bytes = message.encode_to_vec();
        let mut buffer = Vec::new();
        let mut encode_buffer = unsigned_varint::encode::u32_buffer();
        buffer.extend_from_slice(unsigned_varint::encode::u32(
            bytes.len() as u32,
            &mut encode_buffer,
        ));
        buffer.extend_from_slice(&bytes);
        buffer
    }

    struct ScriptedBitswapStream {
        read: std::io::Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    struct PendingReadStream;

    impl AsyncRead for PendingReadStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut [u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Pending
        }
    }

    impl ScriptedBitswapStream {
        fn new(read: Vec<u8>) -> Self {
            Self {
                read: std::io::Cursor::new(read),
                written: Vec::new(),
            }
        }
    }

    impl AsyncRead for ScriptedBitswapStream {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut [u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(std::io::Read::read(&mut self.read, buf))
        }
    }

    impl AsyncWrite for ScriptedBitswapStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    async fn spawn_local_bitswap_peer(
        cid: Cid,
        data: Vec<u8>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entry = want.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert!(!entry.cancel);

            let response = BitswapMessage {
                payload: vec![BlockPayload {
                    prefix: bitswap_payload_prefix(&cid),
                    data,
                    tokens: Vec::new(),
                }],
                ..BitswapMessage::default()
            };
            write_length_prefixed(&mut stream, &response.encode_to_vec())
                .await
                .unwrap();
            stream.flush().await.unwrap();

            let cancel_bytes = tokio::time::timeout(
                Duration::from_secs(5),
                read_length_prefixed(&mut stream, 1024),
            )
            .await
            .unwrap()
            .unwrap();
            let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
            let entry = cancel.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert!(entry.cancel);
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_bitswap_peer_with_extra_payload(
        cid: Cid,
        data: Vec<u8>,
        extra_blocks: Vec<(Cid, Vec<u8>)>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entry = want.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert!(!entry.cancel);

            let mut payload = extra_blocks
                .into_iter()
                .map(|(cid, data)| BlockPayload {
                    prefix: bitswap_payload_prefix(&cid),
                    data,
                    tokens: Vec::new(),
                })
                .collect::<Vec<_>>();
            payload.push(BlockPayload {
                prefix: bitswap_payload_prefix(&cid),
                data,
                tokens: Vec::new(),
            });
            let response = BitswapMessage {
                payload,
                ..BitswapMessage::default()
            };
            write_length_prefixed(&mut stream, &response.encode_to_vec())
                .await
                .unwrap();
            stream.flush().await.unwrap();

            let cancel_bytes = tokio::time::timeout(
                Duration::from_secs(5),
                read_length_prefixed(&mut stream, 1024),
            )
            .await
            .unwrap()
            .unwrap();
            let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
            let entry = cancel.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert!(entry.cancel);
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_multi_want_bitswap_peer(
        blocks: Vec<(Cid, Vec<u8>)>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let block_map = blocks.into_iter().collect::<HashMap<_, _>>();
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entries = want.wantlist.unwrap().entries;
            assert_eq!(entries.len(), block_map.len());
            assert!(entries.iter().all(|entry| !entry.cancel));

            let mut payload = Vec::new();
            let mut requested = Vec::new();
            for entry in entries {
                let mut cid_bytes = entry.block.as_slice();
                let cid = Cid::read_bytes(&mut cid_bytes).unwrap();
                let data = block_map.get(&cid).expect("requested known block").clone();
                requested.push(cid);
                payload.push(BlockPayload {
                    prefix: bitswap_payload_prefix(&cid),
                    data,
                    tokens: Vec::new(),
                });
            }
            let response = BitswapMessage {
                payload,
                ..BitswapMessage::default()
            };
            write_length_prefixed(&mut stream, &response.encode_to_vec())
                .await
                .unwrap();
            stream.flush().await.unwrap();

            let cancel_bytes = tokio::time::timeout(
                Duration::from_secs(5),
                read_length_prefixed(&mut stream, 1024),
            )
            .await
            .unwrap()
            .unwrap();
            let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
            let cancel_entries = cancel.wantlist.unwrap().entries;
            assert_eq!(cancel_entries.len(), requested.len());
            assert!(cancel_entries.iter().all(|entry| entry.cancel));
            let cancelled = cancel_entries
                .into_iter()
                .map(|entry| {
                    let mut cid_bytes = entry.block.as_slice();
                    Cid::read_bytes(&mut cid_bytes).unwrap()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(cancelled, requested.into_iter().collect::<BTreeSet<_>>());
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_incoming_multi_want_bitswap_peer(
        blocks: Vec<(Cid, Vec<u8>)>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let block_map = blocks.into_iter().collect::<HashMap<_, _>>();
            let (request_peer, mut request_stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut request_stream, 1024)
                .await
                .unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entries = want.wantlist.unwrap().entries;
            assert_eq!(entries.len(), block_map.len());
            assert!(entries.iter().all(|entry| !entry.cancel));

            let mut requested = Vec::new();
            for entry in entries {
                let mut cid_bytes = entry.block.as_slice();
                let cid = Cid::read_bytes(&mut cid_bytes).unwrap();
                assert!(block_map.contains_key(&cid));
                requested.push(cid);
            }

            for cid in requested {
                let data = block_map.get(&cid).expect("requested known block").clone();
                let mut stream = tokio::time::timeout(
                    Duration::from_secs(5),
                    control.open_stream(request_peer, StreamProtocol::new("/ipfs/bitswap/1.2.0")),
                )
                .await
                .unwrap()
                .unwrap();
                let response = BitswapMessage {
                    payload: vec![BlockPayload {
                        prefix: bitswap_payload_prefix(&cid),
                        data,
                        tokens: Vec::new(),
                    }],
                    ..BitswapMessage::default()
                };
                write_length_prefixed(&mut stream, &response.encode_to_vec())
                    .await
                    .unwrap();
                stream.flush().await.unwrap();

                let cancel_bytes = tokio::time::timeout(
                    Duration::from_secs(5),
                    read_length_prefixed(&mut stream, 1024),
                )
                .await
                .unwrap()
                .unwrap();
                let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
                let mut cancel_entries = cancel.wantlist.unwrap().entries;
                assert_eq!(cancel_entries.len(), 1);
                let entry = cancel_entries.remove(0);
                assert_eq!(entry.block, cid.to_bytes());
                assert!(entry.cancel);
            }
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_multi_block_bitswap_peer(
        blocks: Vec<(Cid, Vec<u8>)>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_delayed_multi_block_bitswap_peer(blocks, Duration::ZERO).await
    }

    async fn spawn_delayed_multi_block_bitswap_peer(
        blocks: Vec<(Cid, Vec<u8>)>,
        response_delay: Duration,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let mut blocks = blocks.into_iter().collect::<HashMap<_, _>>();
            while !blocks.is_empty() {
                let (_peer, mut stream) = incoming.next().await.unwrap();
                let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
                let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
                let entry = want.wantlist.unwrap().entries.remove(0);
                assert!(!entry.cancel);
                let mut cid_bytes = entry.block.as_slice();
                let cid = Cid::read_bytes(&mut cid_bytes).unwrap();
                let data = blocks.remove(&cid).expect("requested known block");
                if !response_delay.is_zero() {
                    tokio::time::sleep(response_delay).await;
                }

                let response = BitswapMessage {
                    payload: vec![BlockPayload {
                        prefix: bitswap_payload_prefix(&cid),
                        data,
                        tokens: Vec::new(),
                    }],
                    ..BitswapMessage::default()
                };
                write_length_prefixed(&mut stream, &response.encode_to_vec())
                    .await
                    .unwrap();
                stream.flush().await.unwrap();

                let cancel_bytes = tokio::time::timeout(
                    Duration::from_secs(5),
                    read_length_prefixed(&mut stream, 1024),
                )
                .await
                .unwrap()
                .unwrap();
                let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
                let entry = cancel.wantlist.unwrap().entries.remove(0);
                assert_eq!(entry.block, cid.to_bytes());
                assert!(entry.cancel);
            }
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_silent_bitswap_peer(
        cid: Cid,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entry = want.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            tokio::time::sleep(Duration::from_secs(30)).await;
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_bitswap_peer_waiting_for_stream_close(
        cid: Cid,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<bool>,
        oneshot::Receiver<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let (want_seen_tx, want_seen_rx) = oneshot::channel();
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entry = want.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert_eq!(entry.want_type, WantType::Block as i32);
            assert!(!entry.cancel);
            let _ = want_seen_tx.send(());

            matches!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    read_length_prefixed(&mut stream, 1024),
                )
                .await,
                Ok(Err(_))
            )
        });

        (peer_id, addr, swarm_task, stream_task, want_seen_rx)
    }

    async fn spawn_sequence_delegated_response(
        responses: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{addr}/routing/v1");
        let task = tokio::spawn(async move {
            for body in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut request = vec![0u8; 4096];
                if stream.read(&mut request).await.is_err() {
                    continue;
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (endpoint, task)
    }

    async fn spawn_counting_delegated_response(
        body: String,
        requests: Arc<AtomicU64>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{addr}/routing/v1");
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                requests.fetch_add(1, Ordering::Relaxed);
                let mut request = vec![0u8; 4096];
                if stream.read(&mut request).await.is_err() {
                    continue;
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (endpoint, task)
    }

    async fn spawn_gated_delegated_response(
        body: String,
    ) -> (
        String,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{addr}/routing/v1");
        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = stream.read(&mut request).await;
            let _ = request_seen_tx.send(());
            let _ = release_rx.await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (endpoint, request_seen_rx, release_tx, task)
    }

    struct KuboDaemon {
        kubo: String,
        repo: PathBuf,
        log: PathBuf,
        api: String,
        child: Child,
    }

    impl KuboDaemon {
        async fn spawn(kubo: &str, repo: PathBuf) -> Self {
            let log = repo.with_file_name("kubo-daemon.log");
            let log_file = fs::File::create(&log).unwrap();
            let child = Command::new(kubo)
                .env("IPFS_PATH", &repo)
                .env("IPFS_TELEMETRY", "off")
                .arg("daemon")
                .arg("--migrate=false")
                .stdout(Stdio::from(log_file.try_clone().unwrap()))
                .stderr(Stdio::from(log_file))
                .spawn()
                .unwrap_or_else(|err| panic!("failed to start Kubo daemon: {err}"));
            let mut daemon = Self {
                kubo: kubo.to_string(),
                repo,
                log,
                api: String::new(),
                child,
            };
            daemon.wait_until_ready().await;
            daemon
        }

        async fn wait_until_ready(&mut self) {
            for _ in 0..120 {
                if let Some(status) = self.child.try_wait().unwrap() {
                    panic!(
                        "Kubo daemon exited before readiness with {status}; log:\n{}",
                        self.log_contents()
                    );
                }
                if let Ok(api) = fs::read_to_string(self.repo.join("api")) {
                    let api = api.trim().to_string();
                    if !api.is_empty() && self.try_id(&api).is_some() {
                        self.api = api;
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            panic!(
                "Kubo daemon did not become ready; log:\n{}",
                self.log_contents()
            );
        }

        async fn id(&mut self) -> KuboId {
            for _ in 0..20 {
                if let Some(id) = self.try_id(&self.api) {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            panic!(
                "Kubo id did not return successfully; log:\n{}",
                self.log_contents()
            );
        }

        fn try_id(&self, api: &str) -> Option<KuboId> {
            let output = Command::new(&self.kubo)
                .env("IPFS_TELEMETRY", "off")
                .arg("--api")
                .arg(api)
                .arg("id")
                .output()
                .ok()?;
            if !output.status.success() {
                return None;
            }
            serde_json::from_slice(&output.stdout).ok()
        }

        fn stop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }

        fn log_contents(&self) -> String {
            fs::read_to_string(&self.log).unwrap_or_default()
        }
    }

    impl Drop for KuboDaemon {
        fn drop(&mut self) {
            self.stop();
        }
    }

    #[derive(Deserialize)]
    struct KuboId {
        #[serde(rename = "ID")]
        id: String,
        #[serde(rename = "Addresses")]
        addresses: Option<Vec<String>>,
    }

    fn kubo_command(kubo: &str, repo: &Path) -> Command {
        let mut command = Command::new(kubo);
        command.env("IPFS_PATH", repo).env("IPFS_TELEMETRY", "off");
        command
    }

    fn kubo_ok<I, S>(kubo: &str, repo: &Path, args: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = kubo_command(kubo, repo).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "Kubo command failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn kubo_stdout<I, S>(kubo: &str, repo: &Path, args: I) -> Vec<u8>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = kubo_command(kubo, repo).args(args).output().unwrap();
        assert!(
            output.status.success(),
            "Kubo command failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    async fn spawn_want_have_bitswap_peer(
        cid: Cid,
        data: Vec<u8>,
        has_block: bool,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_want_have_bitswap_peer_with_presence_delay(cid, data, has_block, Some(Duration::ZERO))
            .await
    }

    async fn spawn_silent_want_have_bitswap_peer(
        cid: Cid,
        data: Vec<u8>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        spawn_want_have_bitswap_peer_with_presence_delay(cid, data, true, None).await
    }

    async fn spawn_closing_bitswap_peer_expect_want_block(
        cid: Cid,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_bytes = read_length_prefixed(&mut stream, 1024).await.unwrap();
            let want = BitswapMessage::decode(want_bytes.as_slice()).unwrap();
            let entry = want.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert_eq!(entry.want_type, WantType::Block as i32);
            assert!(!entry.cancel);
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    async fn spawn_want_have_bitswap_peer_with_presence_delay(
        cid: Cid,
        data: Vec<u8>,
        has_block: bool,
        presence_delay: Option<Duration>,
    ) -> (
        PeerId,
        Multiaddr,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| libp2p_stream::Behaviour::new())
            .unwrap()
            .build();
        let peer_id = *swarm.local_peer_id();
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control
            .accept(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
            .unwrap();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } =
                swarm.select_next_some().await
            {
                break address;
            }
        };

        let swarm_task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        let stream_task = tokio::spawn(async move {
            let (_peer, mut stream) = incoming.next().await.unwrap();
            let want_have_bytes = match read_length_prefixed(&mut stream, 1024).await {
                Ok(bytes) => bytes,
                Err(err) if !has_block && err.kind() == io::ErrorKind::UnexpectedEof => return,
                Err(err) => panic!("failed to read want-have: {err}"),
            };
            let want_have = BitswapMessage::decode(want_have_bytes.as_slice()).unwrap();
            let entry = want_have.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert_eq!(entry.want_type, WantType::Have as i32);
            assert!(!entry.cancel);

            if let Some(delay) = presence_delay {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let presence = BitswapMessage {
                    block_presences: vec![BlockPresence {
                        cid: cid.to_bytes(),
                        type_pb: if has_block {
                            BLOCK_PRESENCE_HAVE
                        } else {
                            BLOCK_PRESENCE_DONT_HAVE
                        },
                        tokens: Vec::new(),
                    }],
                    ..BitswapMessage::default()
                };
                write_length_prefixed(&mut stream, &presence.encode_to_vec())
                    .await
                    .unwrap();
                stream.flush().await.unwrap();

                if !has_block {
                    return;
                }
            } else {
                assert!(has_block);
            }

            let want_block_bytes = tokio::time::timeout(
                BITSWAP_WANT_HAVE_TIMEOUT + Duration::from_secs(2),
                read_length_prefixed(&mut stream, 1024),
            )
            .await
            .unwrap()
            .unwrap();
            let want_block = BitswapMessage::decode(want_block_bytes.as_slice()).unwrap();
            let entry = want_block.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert_eq!(entry.want_type, WantType::Block as i32);
            assert!(!entry.cancel);

            let response = BitswapMessage {
                payload: vec![BlockPayload {
                    prefix: bitswap_payload_prefix(&cid),
                    data,
                    tokens: Vec::new(),
                }],
                ..BitswapMessage::default()
            };
            write_length_prefixed(&mut stream, &response.encode_to_vec())
                .await
                .unwrap();
            stream.flush().await.unwrap();

            let cancel_bytes = tokio::time::timeout(
                Duration::from_secs(5),
                read_length_prefixed(&mut stream, 1024),
            )
            .await
            .unwrap()
            .unwrap();
            let cancel = BitswapMessage::decode(cancel_bytes.as_slice()).unwrap();
            let entry = cancel.wantlist.unwrap().entries.remove(0);
            assert_eq!(entry.block, cid.to_bytes());
            assert!(entry.cancel);
        });

        (peer_id, addr, swarm_task, stream_task)
    }

    #[test]
    fn fetching_provider_records_cache_hits() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cached retrieval";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let provider = FetchingBlockProvider::new(
            store,
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
        );

        let block = provider.get_block(&cid).unwrap().unwrap();
        assert_eq!(block.data(), data);
        assert_eq!(
            provider.stats(),
            RetrievalStats {
                cache_hits: 1,
                http_provider_blocks: 0,
                bitswap_blocks: 0,
            }
        );
    }
}
