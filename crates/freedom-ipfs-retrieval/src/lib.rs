use cid::Cid;
use freedom_ipfs_core::{
    verify_block, Block, BlockProvider, CoreError, Result as CoreResult, CODEC_DAG_PB,
    DEFAULT_MAX_BLOCK_SIZE, HASH_IDENTITY, HASH_SHA2_256,
};
use freedom_ipfs_namesys::{CloudflareDohResolver, DnsTxtResolver};
use freedom_ipfs_routing::{Provider, ProviderRoutingClient};
use freedom_ipfs_store::{CachedProviderRecord, SqliteBlockStore};
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::stream::{select_all, FuturesUnordered};
use futures::StreamExt;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::Boxed;
use libp2p::core::upgrade;
use libp2p::multiaddr::Protocol;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
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
use std::io;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use url::Url;

const PROVIDER_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const BAD_HTTP_PROVIDER_TTL: Duration = Duration::from_secs(10 * 60);
const BAD_BITSWAP_PROVIDER_TTL: Duration = Duration::from_secs(30);
const HTTP_PROVIDER_TIMEOUT: Duration = Duration::from_secs(20);
const BITSWAP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
// Start provider retry before a full dial timeout can dominate gateway TTFB.
const BITSWAP_CONNECTION_READY_TIMEOUT: Duration = Duration::from_secs(5);
// Keep WANT_HAVE as a short peer-selection probe; slow probes otherwise sit
// directly on the gateway TTFB path before we request the block.
const BITSWAP_WANT_HAVE_TIMEOUT: Duration = Duration::from_millis(750);
const BITSWAP_STREAM_READ_TIMEOUT: Duration = Duration::from_secs(6);
const BITSWAP_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);
const BITSWAP_SUCCESSFUL_PEER_TTL: Duration = Duration::from_secs(10 * 60);
const BITSWAP_SESSION_SHORTCUT_GRACE: Duration = Duration::from_millis(0);
const BITSWAP_SESSION_POST_LOOKUP_GRACE: Duration = Duration::from_millis(200);
const BITSWAP_SESSION_SHORTCUT_TIMEOUT: Duration = Duration::from_secs(2);
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
const MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND: usize = 8;
const MAX_BITSWAP_PEERS_PER_BLOCK: usize = 16;
const MAX_BITSWAP_SESSION_PEERS: usize = 4;
const MAX_BITSWAP_ADDRS_PER_PEER: usize = 2;
// Race a small number of untrusted providers with WANT_BLOCK before falling
// back to conservative WANT_HAVE probes for the rest. This lowers page-asset
// tails without requesting every block from every provider candidate.
const MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS: usize = 2;
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
}

type SharedBlockFetch = Shared<BoxFuture<'static, Arc<SharedBlockFetchResult>>>;
type SharedBlockFetchResult = std::result::Result<(Block, RetrievalSource), String>;

impl HttpRetriever {
    pub fn new(routing: impl Into<ProviderRoutingClient>, store: SqliteBlockStore) -> Self {
        Self {
            client: timeout_http_client(HTTP_PROVIDER_TIMEOUT),
            routing: routing.into(),
            store,
            bitswap: Arc::new(tokio::sync::Mutex::new(None)),
            inflight: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            successful_bitswap_peers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn fetch_block(&self, cid: &Cid) -> Result<Block> {
        self.fetch_block_with_source(cid)
            .await
            .map(|(block, _source)| block)
    }

    pub async fn fetch_block_with_source(&self, cid: &Cid) -> Result<(Block, RetrievalSource)> {
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

        let (block, source) = self.fetch_block_uncached_coalesced(*cid).await?;
        tracing::info!(
            phase = "block_fetch_total",
            cid = %cid,
            source = retrieval_source_label(source),
            elapsed_ms = fetch_started.elapsed().as_millis()
        );
        Ok((block, source))
    }

    async fn fetch_block_uncached_coalesced(&self, cid: Cid) -> Result<(Block, RetrievalSource)> {
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
                    self.fetch_block_uncached_with_source(&cid).await
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
            return self.fetch_block_uncached_with_source(&cid).await;
        }

        let retriever = self.clone();
        let fetch = async move {
            Arc::new(
                retriever
                    .fetch_block_uncached_with_source(&cid)
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
                let routing_started = Instant::now();
                let provider_lookup = self.routing.providers(cid);
                tokio::pin!(provider_lookup);
                let recent_peers = self.recent_bitswap_peers_for_fetch().await;
                let providers = if recent_peers.is_empty() {
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
                    let shortcut = async {
                        tokio::time::sleep(BITSWAP_SESSION_SHORTCUT_GRACE).await;
                        self.fetch_from_recent_bitswap_peers(cid, recent_peers)
                            .await
                    };
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
                                    match timeout(BITSWAP_SESSION_POST_LOOKUP_GRACE, &mut shortcut).await {
                                        Ok(shortcut_result) => {
                                            if let Some(block) = shortcut_result? {
                                                return Ok((block, RetrievalSource::Bitswap));
                                            }
                                        }
                                        Err(_) => {
                                            tracing::info!(
                                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                                cid = %cid,
                                                timeout_ms = BITSWAP_SESSION_POST_LOOKUP_GRACE.as_millis()
                                            );
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
                };
                tracing::info!(
                    phase = "provider_lookup",
                    cid = %cid,
                    provider_count = providers.len(),
                    elapsed_ms = routing_started.elapsed().as_millis()
                );
                self.cache_providers(cid, &providers)?;
                providers
            }
        };
        if let Some(block) = self.recheck_block_store(cid)? {
            return Ok((block, RetrievalSource::Cache));
        }
        match self.fetch_from_providers_with_source(cid, &providers).await {
            Ok((block, source)) => Ok((block, source)),
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
                        return match self.fetch_from_providers_with_source(cid, &providers).await {
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
                        return match self.fetch_from_providers_with_source(cid, &providers).await {
                            Ok((block, source)) => Ok((block, source)),
                            Err(retry_err) => Err(RetrievalError::Bitswap(format!(
                                "initial provider retrieval failed ({err}); same-provider retry failed ({retry_err})"
                            ))),
                        };
                    }
                    return Err(err);
                }
                self.cache_providers(cid, &refreshed)?;
                match self.fetch_from_providers_with_source(cid, &refreshed).await {
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
        self.fetch_from_providers_with_source(cid, providers)
            .await
            .map(|(block, _source)| block)
    }

    pub async fn fetch_from_providers_with_source(
        &self,
        cid: &Cid,
        providers: &[Provider],
    ) -> Result<(Block, RetrievalSource)> {
        tracing::info!(
            phase = "provider_fetch_start",
            cid = %cid,
            provider_count = providers.len()
        );
        for provider in providers {
            for base in &provider.http_urls {
                if self.store.is_bad_provider(base.as_str())? {
                    tracing::debug!(provider = %base, "skipping temporarily bad HTTP provider");
                    continue;
                }
                let started = Instant::now();
                match self.fetch_from_http_provider(cid, base).await {
                    Ok(block) => {
                        tracing::info!(
                            phase = "http_provider_fetch",
                            cid = %cid,
                            provider = %base,
                            ok = true,
                            elapsed_ms = started.elapsed().as_millis()
                        );
                        return Ok((block, RetrievalSource::HttpProvider));
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
                        continue;
                    }
                }
            }
        }
        match self.fetch_from_bitswap_providers(cid, providers).await {
            Ok(block) => Ok((block, RetrievalSource::Bitswap)),
            Err(RetrievalError::NoBitswapProviders) => Err(RetrievalError::NoHttpProviders),
            Err(err) => Err(err),
        }
    }

    fn cached_providers(&self, cid: &Cid) -> Result<Option<Vec<Provider>>> {
        let Some(records) = self.store.get_provider_records(cid)? else {
            return Ok(None);
        };
        let providers = records
            .into_iter()
            .map(|record| Provider::from_parts(record.id, record.addrs))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if providers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(providers))
        }
    }

    fn cache_providers(&self, cid: &Cid, providers: &[Provider]) -> Result<()> {
        let records = providers
            .iter()
            .map(|provider| CachedProviderRecord {
                id: provider.id.clone(),
                addrs: provider.addrs.clone(),
            })
            .collect::<Vec<_>>();
        self.store
            .put_provider_records(cid, &records, PROVIDER_CACHE_TTL)?;
        Ok(())
    }

    async fn fetch_from_http_provider(&self, cid: &Cid, base: &Url) -> Result<Block> {
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
        let bytes = limited_response_bytes(response, DEFAULT_MAX_BLOCK_SIZE).await?;
        verify_block(cid, &bytes)?;
        self.store.put_block(cid, &bytes)?;
        Ok(Block::unchecked(*cid, bytes))
    }

    async fn fetch_from_bitswap_providers(
        &self,
        cid: &Cid,
        providers: &[Provider],
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
                    tracing::info!(
                        phase = "bitswap_request_timeout",
                        cid = %cid,
                        peer_count,
                        trusted_peer_count,
                        timeout_ms = bitswap_request_timeout(peer_count, trusted_peer_count).as_millis(),
                        elapsed_ms = bitswap_started.elapsed().as_millis()
                    );
                    self.reset_shared_bitswap_client().await;
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
            elapsed_ms = bitswap_started.elapsed().as_millis()
        );
        if let Some(peer) = source_peer {
            self.record_successful_bitswap_peer_from_peers(peer, &peers_for_record)
                .await;
        }
        self.store_bitswap_result(cid, result)
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

    async fn reset_shared_bitswap_client(&self) {
        let mut client = self.bitswap.lock().await;
        if client.take().is_some() {
            tracing::info!(phase = "bitswap_client_reset");
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
                (Some(left), Some(right)) => right.seen_at.cmp(&left.seen_at),
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

    async fn record_successful_bitswap_peer(&self, peer: PeerId, addrs: Vec<Multiaddr>) {
        let mut successes = self.successful_bitswap_peers.lock().await;
        successes.insert(
            peer,
            SuccessfulBitswapPeer {
                seen_at: Instant::now(),
                addrs,
            },
        );
    }

    async fn record_successful_bitswap_peer_from_peers(&self, peer: PeerId, peers: &[BitswapPeer]) {
        if let Some(candidate) = peers.iter().find(|candidate| candidate.id == peer) {
            self.record_successful_bitswap_peer(peer, candidate.addrs.clone())
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
            .map(|(id, success)| (*id, success.seen_at, success.addrs.clone()))
            .collect::<Vec<_>>();
        peers.sort_by_key(|peer| std::cmp::Reverse(peer.1));
        peers
            .into_iter()
            .take(MAX_BITSWAP_SESSION_PEERS)
            .map(|(id, _seen_at, addrs)| BitswapPeer {
                id,
                addrs,
                skip_want_have: true,
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
                return Ok(None);
            }
        };

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
            elapsed_ms = started.elapsed().as_millis()
        );
        if let Some(peer) = result.source_peer {
            self.record_successful_bitswap_peer_from_peers(peer, &peers_for_record)
                .await;
        }
        self.store_bitswap_result(cid, result).map(Some)
    }

    fn store_bitswap_result(&self, cid: &Cid, result: BitswapFetchResult) -> Result<Block> {
        for (extra_cid, extra_data) in &result.extra_blocks {
            if extra_cid != cid {
                let _ = self.store.put_block(extra_cid, extra_data);
            }
        }
        self.store.put_block(cid, &result.requested_block)?;
        Ok(Block::unchecked(*cid, result.requested_block))
    }
}

fn retrieval_source_label(source: RetrievalSource) -> &'static str {
    match source {
        RetrievalSource::Cache => "cache",
        RetrievalSource::HttpProvider => "http_provider",
        RetrievalSource::Bitswap => "bitswap",
    }
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
}

impl BlockProvider for FetchingBlockProvider {
    fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
        if let Some(block) = self
            .store
            .get(cid)
            .map_err(|err| CoreError::Storage(err.to_string()))?
        {
            self.stats.record(RetrievalSource::Cache);
            return Ok(Some(block));
        }

        let fetched = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.retriever.fetch_block_with_source(cid))
            }),
            Err(_) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|err| CoreError::Storage(err.to_string()))?;
                runtime.block_on(self.retriever.fetch_block_with_source(cid))
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
}

struct SuccessfulBitswapPeer {
    seen_at: Instant,
    addrs: Vec<Multiaddr>,
}

struct BitswapPeerTarget {
    id: PeerId,
    addrs: Vec<Multiaddr>,
    skip_want_have: bool,
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

struct BitswapFetchResults {
    requested_blocks: HashMap<Cid, Vec<u8>>,
    extra_blocks: Vec<(Cid, Vec<u8>)>,
}

#[derive(Clone)]
struct SharedBitswapClient {
    commands: mpsc::Sender<BitswapCommand>,
}

struct BitswapCommand {
    cid: Cid,
    peers: Vec<BitswapPeer>,
    sent_at: Instant,
    respond: oneshot::Sender<Result<BitswapFetchResult>>,
}

struct PendingIncomingBitswapResult {
    sent_at: Instant,
    sender: mpsc::UnboundedSender<BitswapFetchResult>,
}

type DialErrorLog = Arc<tokio::sync::Mutex<HashMap<PeerId, Vec<String>>>>;
type PeerTransportLog = Arc<tokio::sync::Mutex<HashMap<PeerId, PeerTransportState>>>;

#[derive(Default)]
struct PeerTransportState {
    counts: BTreeMap<&'static str, usize>,
    current: Option<&'static str>,
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
        let peer_count = peers.len();
        let trusted_peer_count = peers.iter().filter(|peer| peer.skip_want_have).count();
        let request_timeout = bitswap_request_timeout(peer_count, trusted_peer_count);
        let target_summary =
            tracing::enabled!(tracing::Level::INFO).then(|| format_bitswap_peers(&peers));
        let (respond, response) = oneshot::channel();
        self.commands
            .send(BitswapCommand {
                cid,
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
                    cid = %cid,
                    peer_count,
                    trusted_peer_count,
                    timeout_ms = request_timeout.as_millis(),
                    elapsed_ms = wait_started.elapsed().as_millis(),
                    targets = %target_summary.as_deref().unwrap_or("")
                );
                Err(RetrievalError::BitswapTimeout)
            }
        }
    }
}

fn bitswap_request_timeout(peer_count: usize, trusted_peer_count: usize) -> Duration {
    if trusted_peer_count > 0 && peer_count > trusted_peer_count {
        BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT
    } else {
        BITSWAP_REQUEST_TIMEOUT
    }
}

async fn run_shared_bitswap_swarm(
    mut swarm: libp2p::Swarm<BitswapBehaviour>,
    control: StreamControl,
    incoming: Vec<IncomingStreams>,
    mut commands: mpsc::Receiver<BitswapCommand>,
) {
    let mut incoming = select_all(incoming);
    let mut fetches = FuturesUnordered::<BoxFuture<'static, Cid>>::new();
    let mut pending_incoming = HashMap::<Cid, Vec<PendingIncomingBitswapResult>>::new();
    let mut pending_counts = HashMap::<Cid, usize>::new();
    let mut connected_peers = HashMap::<PeerId, usize>::new();
    let mut connection_waiters = HashMap::<PeerId, Vec<oneshot::Sender<()>>>::new();
    let mut connection_wait_started = HashMap::<PeerId, Instant>::new();
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
                let (incoming_result, incoming_results) = mpsc::unbounded_channel();
                pending_incoming.entry(command.cid).or_default().push(
                    PendingIncomingBitswapResult {
                        sent_at: command.sent_at,
                        sender: incoming_result,
                    },
                );
                *pending_counts.entry(command.cid).or_default() += 1;

                let mut peer_plans = Vec::new();
                let mut dial_candidates = Vec::new();
                for peer in command.peers {
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
                        connection_ready,
                    });
                }
                tracing::info!(
                    phase = "bitswap_dial_plan",
                    cid = %command.cid,
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

                for (peer_id, addr) in dial_addrs {
                    let transport = bitswap_transport_label(&addr);
                    let dial_addr = addr.with_p2p(peer_id).unwrap_or_else(|addr| addr);
                    if let Err(err) = swarm.dial(dial_addr) {
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

                let control = control.clone();
                let cid = command.cid;
                let mut respond = command.respond;
                let dial_errors = dial_errors.clone();
                let peer_transports = peer_transports.clone();
                let fetch_started = Instant::now();
                fetches.push(Box::pin(async move {
                    let result = tokio::select! {
                        result = fetch_bitswap_with_incoming_streams(
                            control,
                            peer_targets,
                            cid,
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
                            cid = %cid,
                            command_queued_ms,
                            elapsed_ms = fetch_started.elapsed().as_millis()
                        );
                    }
                    cid
                }));
            }
            Some(cid) = fetches.next(), if !fetches.is_empty() => {
                if let Some(count) = pending_counts.get_mut(&cid) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        pending_counts.remove(&cid);
                        pending_incoming.remove(&cid);
                    }
                }
            }
            maybe_stream = incoming.next() => {
                let Some((peer, mut stream)) = maybe_stream else {
                    continue;
                };
                match read_bitswap_blocks(&mut stream).await {
                    Ok(blocks) => {
                        let mut matched = false;
                        let source_transport =
                            current_peer_transport(&peer_transports, peer).await;
                        for cid in pending_incoming.keys().copied().collect::<Vec<_>>() {
                            if let Some(mut result) = collect_bitswap_result(&cid, blocks.clone()) {
                                result.source_peer = Some(peer);
                                result.source_transport = source_transport;
                                result.delivery = "incoming";
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
                                tracing::info!(
                                    phase = "bitswap_incoming_block",
                                    cid = %cid,
                                    peer = %peer,
                                    source_transport = source_transport.unwrap_or("unknown"),
                                    block_count = blocks.len(),
                                    bytes = result.requested_block.len(),
                                    pending_waiter_count,
                                    oldest_pending_ms,
                                    newest_pending_ms
                                );
                                if let Some(senders) = pending_incoming.get_mut(&cid) {
                                    senders.retain(|pending| {
                                        pending.sender.send(result.clone()).is_ok()
                                    });
                                }
                                let _ = write_bitswap_cancel(&mut stream, &cid).await;
                            }
                        }
                        if !matched {
                            let _ = write_empty_bitswap_message(&mut stream).await;
                        }
                    }
                    Err(err) => {
                        tracing::debug!(error = %err, "incoming bitswap stream read failed");
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

fn should_start_bitswap_dial(
    peer: &PeerId,
    connected_peers: &HashMap<PeerId, usize>,
    connection_waiters: &HashMap<PeerId, Vec<oneshot::Sender<()>>>,
) -> bool {
    !connected_peers.contains_key(peer) && !connection_waiters.contains_key(peer)
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
    let mut dnsaddr_cache = HashMap::<String, Vec<String>>::new();
    let mut dns_ip_cache = HashMap::<String, Vec<IpAddr>>::new();

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

async fn expand_provider_multiaddrs(
    addrs: &[String],
    dnsaddr_cache: &mut HashMap<String, Vec<String>>,
    dns_ip_cache: &mut HashMap<String, Vec<IpAddr>>,
) -> Vec<String> {
    let resolver = CloudflareDohResolver::default();
    let mut expanded_dnsaddr = Vec::new();

    for addr in addrs {
        let Some(host) = dnsaddr_host(addr) else {
            expanded_dnsaddr.push(addr.clone());
            continue;
        };
        if let Some(records) = dnsaddr_cache.get(host) {
            tracing::info!(
                phase = "bitswap_dnsaddr_expand",
                host,
                cached = true,
                ok = true,
                record_count = records.len()
            );
            expanded_dnsaddr.extend(records.iter().cloned());
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
        dnsaddr_cache.insert(host.to_string(), records.clone());
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
        let addrs = if let Some(addrs) = dns_ip_cache.get(&dns_name) {
            tracing::info!(
                phase = "bitswap_dns_multiaddr_expand",
                host = %dns_name,
                cached = true,
                ip_count = addrs.len()
            );
            addrs.clone()
        } else {
            match resolver.ip_lookup(&dns_name).await {
                Ok(addrs) => {
                    tracing::info!(
                        phase = "bitswap_dns_multiaddr_expand",
                        host = %dns_name,
                        cached = false,
                        ip_count = addrs.len()
                    );
                    dns_ip_cache.insert(dns_name.clone(), addrs.clone());
                    addrs
                }
                Err(_) => {
                    dns_ip_cache.insert(dns_name.clone(), Vec::new());
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

async fn limited_response_bytes(response: reqwest::Response, max_size: usize) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > max_size {
            return Err(RetrievalError::Core(CoreError::BlockTooLarge {
                actual: body.len().saturating_add(chunk.len()),
                max: max_size,
            }));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
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

async fn fetch_bitswap_with_incoming_streams(
    control: StreamControl,
    peers: Vec<BitswapPeerTarget>,
    cid: Cid,
    mut incoming_results: mpsc::UnboundedReceiver<BitswapFetchResult>,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> Result<BitswapFetchResult> {
    tokio::select! {
        result = fetch_bitswap_over_outgoing_streams(control, peers, cid, dial_errors, peer_transports) => result,
        incoming = incoming_results.recv() => incoming.ok_or_else(|| {
            RetrievalError::Bitswap("incoming bitswap result channel closed".into())
        }),
    }
}

async fn fetch_bitswap_over_outgoing_streams(
    control: StreamControl,
    peers: Vec<BitswapPeerTarget>,
    cid: Cid,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> Result<BitswapFetchResult> {
    let mut attempts = FuturesUnordered::new();
    let has_multiple_peers = peers.len() > 1;
    let target_summary = format_bitswap_targets(&peers);
    let mut direct_untrusted_want_block_count = 0usize;
    for peer in peers {
        let prefer_want_have = bitswap_prefer_want_have(
            has_multiple_peers,
            peer.skip_want_have,
            &mut direct_untrusted_want_block_count,
        );
        attempts.push(request_bitswap_block_after_connection(
            control.clone(),
            peer,
            cid,
            prefer_want_have,
            BITSWAP_WANT_HAVE_TIMEOUT,
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
    let message =
        format!("all bitswap stream requests failed for cid {cid}; targets={target_summary}; detail={detail}");
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
        Err(RetrievalError::Bitswap(message))
    } else {
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
    direct_untrusted_want_block_count: &mut usize,
) -> bool {
    let direct_untrusted_want_block = !skip_want_have
        && *direct_untrusted_want_block_count < MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS;
    if !skip_want_have {
        *direct_untrusted_want_block_count += 1;
    }
    has_multiple_peers && !skip_want_have && !direct_untrusted_want_block
}

async fn request_bitswap_block_after_connection(
    control: StreamControl,
    peer: BitswapPeerTarget,
    cid: Cid,
    prefer_want_have: bool,
    want_have_timeout: Duration,
    dial_errors: DialErrorLog,
    peer_transports: PeerTransportLog,
) -> std::result::Result<BitswapFetchResult, BitswapPeerFailure> {
    let BitswapPeerTarget {
        id: peer_id,
        addrs,
        connection_ready,
        ..
    } = peer;

    let attempt_started = Instant::now();
    tracing::info!(
        phase = "bitswap_peer_attempt_start",
        cid = %cid,
        peer = %peer_id,
        prefer_want_have,
        want_have_timeout_ms = want_have_timeout.as_millis()
    );

    if let Some(connection_ready) = connection_ready {
        match timeout(BITSWAP_CONNECTION_READY_TIMEOUT, connection_ready).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                tracing::info!(
                    phase = "bitswap_peer_attempt",
                    cid = %cid,
                    peer = %peer_id,
                    ok = false,
                    failure_kind = "connection_waiter_dropped",
                    prefer_want_have,
                    want_have_timeout_ms = want_have_timeout.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                return Err(BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::Other,
                    detail: format!(
                        "{}: bitswap connection waiter was dropped before connection",
                        peer_id
                    ),
                });
            }
            Err(_) => {
                let recent_dial_errors = recent_dial_errors(&dial_errors, peer_id).await;
                tracing::info!(
                    phase = "bitswap_peer_attempt",
                    cid = %cid,
                    peer = %peer_id,
                    ok = false,
                    failure_kind = "connection_timeout",
                    prefer_want_have,
                    want_have_timeout_ms = want_have_timeout.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                return Err(BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::ConnectionTimeout,
                    detail: format!(
                        "{}: bitswap connection was not established within {}ms; addrs={}; recent_dial_errors={}",
                        peer_id,
                        BITSWAP_CONNECTION_READY_TIMEOUT.as_millis(),
                        format_multiaddrs(&addrs),
                        recent_dial_errors
                    ),
                });
            }
        }
    }
    let result = request_bitswap_block(
        control,
        peer_id,
        addrs,
        cid,
        prefer_want_have,
        want_have_timeout,
        peer_transports,
    )
    .await;
    match &result {
        Ok(result) => {
            tracing::info!(
                phase = "bitswap_peer_attempt",
                cid = %cid,
                peer = %peer_id,
                ok = true,
                prefer_want_have,
                want_have_timeout_ms = want_have_timeout.as_millis(),
                source_transport = result.source_transport.unwrap_or("unknown"),
                bytes = result.requested_block.len(),
                extra_blocks = result.extra_blocks.len(),
                elapsed_ms = attempt_started.elapsed().as_millis()
            );
        }
        Err(err) => {
            tracing::info!(
                phase = "bitswap_peer_attempt",
                cid = %cid,
                peer = %peer_id,
                ok = false,
                failure_kind = bitswap_peer_failure_kind_label(err.kind),
                prefer_want_have,
                want_have_timeout_ms = want_have_timeout.as_millis(),
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

async fn request_bitswap_block(
    mut control: StreamControl,
    peer_id: PeerId,
    addrs: Vec<Multiaddr>,
    cid: Cid,
    prefer_want_have: bool,
    want_have_timeout: Duration,
    peer_transports: PeerTransportLog,
) -> std::result::Result<BitswapFetchResult, BitswapPeerFailure> {
    let mut failures = Vec::new();
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

        if prefer_want_have && protocol_name == "/ipfs/bitswap/1.2.0" {
            match request_bitswap_block_after_want_have(
                &mut stream,
                &cid,
                &protocol_name,
                want_have_timeout,
            )
            .await
            {
                Ok(mut result) => {
                    result.source_peer = Some(peer_id);
                    result.source_transport =
                        current_peer_transport(&peer_transports, peer_id).await;
                    return Ok(result);
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

        match request_bitswap_block_on_stream(&mut stream, &cid, &protocol_name).await {
            Ok(mut result) => {
                result.source_peer = Some(peer_id);
                result.source_transport = current_peer_transport(&peer_transports, peer_id).await;
                return Ok(result);
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
            "{peer_id}: no supported Bitswap protocol returned cid {cid}; addrs={}; failures=({})",
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
    want_have_timeout: Duration,
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
    let response = match timeout(want_have_timeout, read_bitswap_response(stream)).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            return Err(WantHaveFailure::TryOtherProtocols(
                BitswapProtocolFailure::other(format!(
                    "{protocol_name}: read want-have failed: {err}"
                )),
            ))
        }
        Err(_) => {
            return request_bitswap_block_on_stream(stream, cid, protocol_name)
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
        return request_bitswap_block_on_stream(stream, cid, protocol_name)
            .await
            .map_err(WantHaveFailure::TryOtherProtocols);
    }
    request_bitswap_block_on_stream(stream, cid, protocol_name)
        .await
        .map_err(WantHaveFailure::TryOtherProtocols)
}

async fn request_bitswap_block_on_stream<T>(
    stream: &mut T,
    cid: &Cid,
    protocol_name: &str,
) -> std::result::Result<BitswapFetchResult, BitswapProtocolFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut results = request_bitswap_blocks_on_stream(stream, &[*cid], protocol_name).await?;
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
) -> std::result::Result<BitswapFetchResults, BitswapProtocolFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    if let Err(err) = write_bitswap_wants(stream, cids).await {
        return Err(BitswapProtocolFailure::other(format!(
            "{protocol_name}: write failed: {err}"
        )));
    }
    let blocks = match timeout(BITSWAP_STREAM_READ_TIMEOUT, read_bitswap_blocks(stream)).await {
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
            vec![
                "/dns4/example.com/tcp/4001".to_string(),
                "/dns4/ws.example/tcp/443/wss".to_string(),
            ],
        )]);
        let mut dns_ip_cache = HashMap::from([(
            "example.com".to_string(),
            vec!["203.0.113.10".parse().unwrap()],
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
            },
            BitswapPeer {
                id: second,
                addrs: vec![
                    "/ip4/127.0.0.1/tcp/2001".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/2002".parse().unwrap(),
                ],
                skip_want_have: false,
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
            })
            .collect::<Vec<_>>();

        let (dials, suppressed) = limited_interleaved_bitswap_dials(&peers);
        let addr_order = dials
            .iter()
            .map(|(_, addr)| addr.to_string())
            .collect::<Vec<_>>();

        assert_eq!(dials.len(), MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND);
        assert_eq!(suppressed, 8);
        assert_eq!(
            addr_order,
            vec![
                "/ip4/127.0.0.1/tcp/1001",
                "/ip4/127.0.0.2/tcp/1001",
                "/ip4/127.0.0.3/tcp/1001",
                "/ip4/127.0.0.4/tcp/1001",
                "/ip4/127.0.0.1/tcp/1002",
                "/ip4/127.0.0.2/tcp/1002",
                "/ip4/127.0.0.3/tcp/1002",
                "/ip4/127.0.0.4/tcp/1002",
            ]
        );
    }

    #[test]
    fn formats_bitswap_peer_timeout_summary() {
        let first = parse_peer_id("12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3").unwrap();
        let second = parse_peer_id("12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12").unwrap();
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
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: false,
            },
        ];

        assert_eq!(
            format_bitswap_peers(&peers),
            format!(
                "{first}:want-block@[/ip4/127.0.0.1/tcp/1001,/ip4/127.0.0.1/tcp/1002,+3 more]; {second}:want-block@[]"
            )
        );
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

        let result =
            request_bitswap_blocks_on_stream(&mut stream, &[first, second], "/ipfs/bitswap/1.2.0")
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
        let result =
            request_bitswap_blocks_on_stream(&mut stream, &[first, second], "/ipfs/bitswap/1.2.0")
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
            .fetch_from_providers_with_source(&cid, &[provider])
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
            .fetch_from_providers_with_source(&requested, &[provider])
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
            .fetch_from_providers_with_source(&first_cid, std::slice::from_ref(&provider))
            .await
            .unwrap();
        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), first);

        let (block, source) = retriever
            .fetch_from_providers_with_source(&second_cid, &[provider])
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
            .fetch_from_providers_with_source(&cid, &[provider])
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
                Some(present_peer_id.to_string()),
                vec![present_addr.to_string()],
            )
            .unwrap(),
        ];

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &providers)
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
        tokio::time::timeout(Duration::from_secs(5), present_stream)
            .await
            .unwrap()
            .unwrap();
        first_swarm.abort();
        second_swarm.abort();
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
            .fetch_from_providers_with_source(&cid, &providers)
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
            .fetch_from_providers_with_source(&cid, &[provider])
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
            .fetch_from_providers_with_source(&cid, &[provider])
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
            .fetch_from_providers_with_source(&cid, &[provider])
            .await
            .unwrap_err();

        assert!(matches!(err, RetrievalError::NoHttpProviders));
        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.is_bad_provider(&provider_url).unwrap());
        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesces_concurrent_fetches_for_same_missing_cid() {
        let data = b"shared missing block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, data);
        let requests = Arc::new(AtomicU64::new(0));
        let (addr, server_task) = spawn_counting_http_provider(
            data.to_vec(),
            Duration::from_millis(200),
            requests.clone(),
        )
        .await;
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
            .record_successful_bitswap_peer(preferred, Vec::new())
            .await;
        let mut peers = vec![
            BitswapPeer {
                id: other,
                addrs: Vec::new(),
                skip_want_have: false,
            },
            BitswapPeer {
                id: preferred,
                addrs: Vec::new(),
                skip_want_have: false,
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
            )
            .await;
        let mut peers = vec![BitswapPeer {
            id: provider_peer,
            addrs: Vec::new(),
            skip_want_have: false,
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

    #[test]
    fn shortens_request_timeout_for_mixed_trusted_bitswap_candidates() {
        assert_eq!(
            bitswap_request_timeout(10, 1),
            BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT
        );
        assert_eq!(bitswap_request_timeout(10, 0), BITSWAP_REQUEST_TIMEOUT);
        assert_eq!(bitswap_request_timeout(1, 1), BITSWAP_REQUEST_TIMEOUT);
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
            .fetch_from_providers_with_source(&first_cid, &[provider])
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
            .record_successful_bitswap_peer(session_peer_id, vec![session_addr])
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
        assert_eq!(http_requests.load(Ordering::Relaxed), 0);

        tokio::time::timeout(Duration::from_secs(5), session_stream)
            .await
            .unwrap()
            .unwrap();
        session_swarm.abort();
        http_task.abort();
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
            .record_successful_bitswap_peer(session_peer_id, vec![session_addr])
            .await;
        let provider = Provider::from_parts(
            Some(missing_peer_id.to_string()),
            vec![missing_addr.to_string()],
        )
        .unwrap();

        let (block, source) = retriever
            .fetch_from_providers_with_source(&cid, &[provider])
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
        let first_response = format!(
            r#"{{"Providers":[{{"ID":"{}","Addrs":["{}"]}}]}}"#,
            silent_peer_id, silent_addr
        );
        let second_response = format!(
            r#"{{"Providers":[{{"ID":"{}","Addrs":["{}"]}}]}}"#,
            good_peer_id, good_addr
        );
        let (endpoint, routing_task) =
            spawn_sequence_delegated_response(vec![first_response, second_response]).await;

        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new(endpoint),
            store.clone(),
        );

        let (block, source) = tokio::time::timeout(
            Duration::from_secs(20),
            retriever.fetch_block_with_source(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), data);
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

    async fn spawn_multi_block_bitswap_peer(
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
