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
const SINGLE_HTTP_BITSWAP_HEDGE_AFTER_MS_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_AFTER_MS";
const SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS";
const SINGLE_HTTP_BITSWAP_HEDGE_MAX_PER_TOP_LEVEL_ENV: &str =
    "FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MAX_PER_TOP_LEVEL";
const ENABLE_SINGLE_HTTP_SESSION_BITSWAP_HEDGE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_SINGLE_HTTP_SESSION_BITSWAP_HEDGE";
const ENABLE_SINGLE_HTTP_5XX_FAST_BITSWAP_FALLBACK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_SINGLE_HTTP_5XX_FAST_BITSWAP_FALLBACK";
const ENABLE_TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK";
const TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS_ENV: &str =
    "FREEDOM_IPFS_TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS";
const TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS: usize = 32;
const ENABLE_TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK";
const TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS_ENV: &str =
    "FREEDOM_IPFS_TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS";
const TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS: usize = 32;
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
const BITSWAP_WANT_HAVE_TIMEOUT: Duration = Duration::from_millis(500);
const BITSWAP_WANT_HAVE_TIMEOUT_MS_ENV: &str = "FREEDOM_IPFS_BITSWAP_WANT_HAVE_TIMEOUT_MS";
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
const ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION: Duration = Duration::from_millis(750);
const ENABLE_ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION";
const ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION_MS_ENV: &str =
    "FREEDOM_IPFS_ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION_MS";
const ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION: Duration = Duration::from_millis(750);
const ENABLE_ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION";
const ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION_MS_ENV: &str =
    "FREEDOM_IPFS_ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION_MS";
const BITSWAP_CONNECTION_ERROR_BACKOFF_TTL: Duration = Duration::from_secs(30);
const BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD: usize = 2;
const BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD";
const ENABLE_ZERO_HTTP_SUBRESOURCE_CONNECTION_ERROR_BACKOFF_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_SUBRESOURCE_CONNECTION_ERROR_BACKOFF";
const ENABLE_BITSWAP_INCOMING_READ_TIMEOUT_BACKOFF_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_INCOMING_READ_TIMEOUT_BACKOFF";
const ENABLE_BITSWAP_INCOMING_SOURCE_ADDR_SESSION_PEERS_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_INCOMING_SOURCE_ADDR_SESSION_PEERS";
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
const BITSWAP_SESSION_ZERO_HTTP_POST_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_ZERO_HTTP_POST_LOOKUP_GRACE_MS";
const BITSWAP_SESSION_MULTI_HTTP_POST_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_MULTI_HTTP_POST_LOOKUP_GRACE_MS";
// Give a recent Bitswap session peer a short chance to win before falling back
// to the only HTTP provider. Longer waits inflated page-asset tails on mobile
// browsing workloads without enough reliability benefit.
const BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE: Duration = Duration::from_millis(125);
const BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE_MS";
const TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE: Duration = Duration::from_millis(150);
const ENABLE_TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE";
const TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE_MS_ENV: &str =
    "FREEDOM_IPFS_TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE_MS";
const ENABLE_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_ENV: &str =
    "FREEDOM_IPFS_ENABLE_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT";
const TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS: usize = 2;
const TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS_ENV: &str =
    "FREEDOM_IPFS_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS";
const TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS: usize = 8;
const TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS_ENV: &str =
    "FREEDOM_IPFS_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS";
const MAX_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_TRACKED_PATHS: usize = 64;
const MAX_SINGLE_HTTP_BITSWAP_HEDGE_TRACKED_PATHS: usize = 64;
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
const ENABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH";
const DISABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_ENV: &str =
    "FREEDOM_IPFS_DISABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH";
const ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS_ENV: &str =
    "FREEDOM_IPFS_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS";
const ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS: usize = 32;
const BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS: usize = 2;
const ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK";
const BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS";
const BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS";
const BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS: usize = 5;
const ENABLE_BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK";
const BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS";
const BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS";
const BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_MIN_PROVIDERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_MIN_PROVIDERS";
const BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_MIN_PROVIDERS: usize = 32;
const ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_PEER_ROTATION_ENV: &str =
    "FREEDOM_IPFS_ENABLE_ZERO_HTTP_SUBRESOURCE_PEER_ROTATION";
const ENABLE_BITSWAP_EARLY_PROVIDER_PEER_CAP_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_EARLY_PROVIDER_PEER_CAP";
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
const BITSWAP_MAX_DIAL_ADDRS_PER_COMMAND_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_MAX_DIAL_ADDRS_PER_COMMAND";
const MAX_BITSWAP_PEERS_PER_BLOCK: usize = 16;
const MAX_BITSWAP_SESSION_PEERS: usize = 4;
const BITSWAP_SESSION_PEER_LIMIT_ENV: &str = "FREEDOM_IPFS_BITSWAP_SESSION_PEER_LIMIT";
const BITSWAP_SESSION_PEER_MIN_SUCCESSES: u64 = 1;
const BITSWAP_SESSION_PEER_MIN_SUCCESSES_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_SESSION_PEER_MIN_SUCCESSES";
const BITSWAP_TRUSTED_DIRECT_WANT_BLOCK_PEERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_TRUSTED_DIRECT_WANT_BLOCK_PEERS";
const ENABLE_BITSWAP_TOP_LEVEL_SCOPED_SESSION_PEERS_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_TOP_LEVEL_SCOPED_SESSION_PEERS";
const ENABLE_BITSWAP_DOMINANT_SESSION_PEER_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_DOMINANT_SESSION_PEER";
const ENABLE_BITSWAP_TOP_LEVEL_DOMINANT_SESSION_PEER_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_TOP_LEVEL_DOMINANT_SESSION_PEER";
const BITSWAP_DOMINANT_SESSION_PEER_ALTERNATES_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_DOMINANT_SESSION_PEER_ALTERNATES";
const BITSWAP_DOMINANT_SESSION_PEER_MIN_SUCCESSES: u64 = 8;
const BITSWAP_DOMINANT_SESSION_PEER_RATIO: u64 = 4;
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
const BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS_ENV: &str =
    "FREEDOM_IPFS_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS";
const ENABLE_BITSWAP_PROVIDER_ADDR_SCORE_ORDER_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_PROVIDER_ADDR_SCORE_ORDER";
const ENABLE_BITSWAP_DIRECT_IP_PROVIDER_CANDIDATES_ONLY_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_DIRECT_IP_PROVIDER_CANDIDATES_ONLY";
const BITSWAP_DNS_PREFETCH_CONCURRENCY: usize = 8;
const BITSWAP_DNS_LOOKUP_TIMEOUT_MS_ENV: &str = "FREEDOM_IPFS_BITSWAP_DNS_LOOKUP_TIMEOUT_MS";
const BITSWAP_DNS_EXPANSION_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_BITSWAP_DNS_EXPANSION_CACHE_ENTRIES: usize = 128;
const ENABLE_BITSWAP_DNS_EXPANSION_CACHE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_DNS_EXPANSION_CACHE";
const ENABLE_BITSWAP_TOP_LEVEL_DNS_EXPANSION_CACHE_ENV: &str =
    "FREEDOM_IPFS_ENABLE_BITSWAP_TOP_LEVEL_DNS_EXPANSION_CACHE";
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
    bitswap_dnsaddr_cache: Arc<tokio::sync::Mutex<SharedDnsaddrCache>>,
    bitswap_dns_ip_cache: Arc<tokio::sync::Mutex<SharedDnsIpCache>>,
    top_level_bitswap_provider_preconnect_counts: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
    single_http_bitswap_hedge_counts: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetrievalRequestContext {
    gateway_subresource: bool,
    top_level_path: Option<String>,
    zero_http_post_lookup_shortcut_timeout: bool,
}

impl RetrievalRequestContext {
    pub fn gateway_request(parent_request_id: Option<u64>) -> Self {
        Self::gateway_request_with_top_level(parent_request_id, None)
    }

    pub fn gateway_request_with_top_level(
        parent_request_id: Option<u64>,
        top_level_path: Option<String>,
    ) -> Self {
        Self {
            gateway_subresource: parent_request_id.is_some(),
            top_level_path,
            zero_http_post_lookup_shortcut_timeout: false,
        }
    }

    pub fn gateway_subresource(&self) -> bool {
        self.gateway_subresource
    }

    pub fn top_level_path(&self) -> Option<&str> {
        self.top_level_path.as_deref()
    }

    fn with_zero_http_post_lookup_shortcut_timeout(mut self) -> Self {
        self.zero_http_post_lookup_shortcut_timeout = true;
        self
    }

    fn zero_http_post_lookup_shortcut_timeout(&self) -> bool {
        self.zero_http_post_lookup_shortcut_timeout
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
    RETRIEVAL_REQUEST_CONTEXT
        .try_with(|context| context.clone())
        .ok()
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
            bitswap_dnsaddr_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            bitswap_dns_ip_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            top_level_bitswap_provider_preconnect_counts: Arc::new(tokio::sync::Mutex::new(
                HashMap::new(),
            )),
            single_http_bitswap_hedge_counts: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
        let mut context = context;
        let mut zero_http_post_lookup_shortcut_timeout = false;
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
                                                                context.clone(),
                                                                shortcut.as_mut(),
                                                            )
                                                            .await?
                                                        {
                                                            return Ok((block, source));
                                                        }
                                                    } else {
                                                        match self
                                                            .fetch_after_zero_http_post_lookup_dns_prefetch(
                                                                cid,
                                                                &providers,
                                                                context.clone(),
                                                                shortcut.as_mut(),
                                                            )
                                                            .await?
                                                        {
                                                            ZeroHttpPostLookupDnsPrefetchOutcome::Fetched(
                                                                block,
                                                                source,
                                                            ) => {
                                                                return Ok((block, source));
                                                            }
                                                            ZeroHttpPostLookupDnsPrefetchOutcome::ContinueAfterWait {
                                                                shortcut_timed_out,
                                                            } => {
                                                                if http_provider_count == 0 && shortcut_timed_out {
                                                                    zero_http_post_lookup_shortcut_timeout = true;
                                                                }
                                                            }
                                                            ZeroHttpPostLookupDnsPrefetchOutcome::NotAttempted => {
                                                                match self
                                                                    .wait_for_session_shortcut_post_lookup(
                                                                        cid,
                                                                        &providers,
                                                                        http_provider_count,
                                                                        shortcut.as_mut(),
                                                                    )
                                                                    .await?
                                                                {
                                                                    SessionShortcutPostLookupWait::Hit(block) => {
                                                                        return Ok((
                                                                            block,
                                                                            RetrievalSource::Bitswap,
                                                                        ));
                                                                    }
                                                                    SessionShortcutPostLookupWait::Miss => {}
                                                                    SessionShortcutPostLookupWait::Timeout => {
                                                                        if http_provider_count == 0 {
                                                                            zero_http_post_lookup_shortcut_timeout = true;
                                                                        }
                                                                    }
                                                                }
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
                                                            context.clone(),
                                                            shortcut.as_mut(),
                                                        )
                                                        .await?
                                                    {
                                                        return Ok((block, source));
                                                    }
                                                } else {
                                                    match self
                                                        .fetch_after_zero_http_post_lookup_dns_prefetch(
                                                            cid,
                                                            &providers,
                                                            context.clone(),
                                                            shortcut.as_mut(),
                                                        )
                                                        .await?
                                                    {
                                                        ZeroHttpPostLookupDnsPrefetchOutcome::Fetched(
                                                            block,
                                                            source,
                                                        ) => {
                                                            return Ok((block, source));
                                                        }
                                                        ZeroHttpPostLookupDnsPrefetchOutcome::ContinueAfterWait {
                                                            shortcut_timed_out,
                                                        } => {
                                                            if http_provider_count == 0 && shortcut_timed_out {
                                                                zero_http_post_lookup_shortcut_timeout = true;
                                                            }
                                                        }
                                                        ZeroHttpPostLookupDnsPrefetchOutcome::NotAttempted => {
                                                            match self
                                                                .wait_for_session_shortcut_post_lookup(
                                                                    cid,
                                                                    &providers,
                                                                    http_provider_count,
                                                                    shortcut.as_mut(),
                                                                )
                                                                .await?
                                                            {
                                                                SessionShortcutPostLookupWait::Hit(block) => {
                                                                    return Ok((
                                                                        block,
                                                                        RetrievalSource::Bitswap,
                                                                    ));
                                                                }
                                                                SessionShortcutPostLookupWait::Miss => {}
                                                                SessionShortcutPostLookupWait::Timeout => {
                                                                    if http_provider_count == 0 {
                                                                        zero_http_post_lookup_shortcut_timeout = true;
                                                                    }
                                                                }
                                                            }
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
        if zero_http_post_lookup_shortcut_timeout {
            if let Some(current_context) = context.take() {
                context = Some(current_context.with_zero_http_post_lookup_shortcut_timeout());
            }
        }
        if let Some(block) = self.recheck_block_store(cid)? {
            return Ok((block, RetrievalSource::Cache));
        }
        match self
            .fetch_from_providers_with_source(cid, &providers, context.clone())
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
                            .fetch_from_providers_with_source(cid, &providers, context.clone())
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
                            .fetch_from_providers_with_source(cid, &providers, context.clone())
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
                    .fetch_from_providers_with_source(cid, &refreshed, context.clone())
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
        self.fetch_from_providers_with_source_with_options(
            cid,
            providers,
            context,
            ProviderFetchOptions::from_env(),
        )
        .await
    }

    async fn fetch_from_providers_with_source_with_options(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        options: ProviderFetchOptions,
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
        let bitswap_provider_candidate_available = has_bitswap_provider_candidate(providers);
        let http_provider_count = http_provider_bases.len();
        self.maybe_spawn_top_level_bitswap_provider_preconnect(
            providers,
            context.clone(),
            http_provider_count,
            bitswap_provider_candidate_available,
        );
        if http_provider_count == 1 {
            let single_http_base = http_provider_bases
                .first()
                .expect("single HTTP provider base is present")
                .clone();
            if options.single_http_session_bitswap_hedge
                && self
                    .single_http_provider_bitswap_hedge_score_allows(cid, &single_http_base)
                    .await
            {
                let recent_peers = self.recent_bitswap_peers_for_fetch().await;
                if !recent_peers.is_empty() {
                    return self
                        .fetch_single_http_provider_with_session_bitswap_hedge(
                            cid,
                            http_provider_bases.clone(),
                            recent_peers,
                        )
                        .await;
                }
                tracing::info!(
                    phase = "http_provider_bitswap_hedge_skip",
                    cid = %cid,
                    provider = %single_http_base,
                    reason = "no_session_peers",
                    provider_scored = false,
                    provider_score_ms = 0u128,
                    min_score_ms = single_http_provider_bitswap_hedge_min_score()
                        .map(|score| score.as_millis())
                        .unwrap_or_default(),
                    session_peer_only = true
                );
            }
            if single_http_provider_bitswap_hedge_enabled()
                && bitswap_provider_candidate_available
                && self
                    .single_http_provider_bitswap_hedge_score_allows(cid, &single_http_base)
                    .await
            {
                return self
                    .fetch_single_http_provider_with_bitswap_hedge(
                        cid,
                        http_provider_bases,
                        providers.to_vec(),
                        context.clone(),
                    )
                    .await;
            }
        }
        if top_level_multi_http_failed_direct_ip_bitswap_fallback_allows(
            options.top_level_multi_http_failed_direct_ip_bitswap_fallback,
            context.as_ref(),
            http_provider_count,
            providers.len(),
            bitswap_provider_candidate_available,
            has_direct_ip_bitswap_provider_candidate(providers),
        ) {
            if let Some((block, source)) = self
                .fetch_top_level_multi_http_failed_direct_ip_bitswap_race(
                    cid,
                    http_provider_bases,
                    providers,
                    context.clone(),
                )
                .await?
            {
                return Ok((block, source));
            }
        } else {
            if let Some(block) = self
                .fetch_from_http_provider_candidates(
                    cid,
                    http_provider_bases,
                    options.single_http_5xx_fast_bitswap_fallback
                        && bitswap_provider_candidate_available,
                )
                .await?
            {
                return Ok((block, RetrievalSource::HttpProvider));
            }
        }
        if top_level_single_http_failed_direct_ip_bitswap_fallback_allows(
            options.top_level_single_http_failed_direct_ip_bitswap_fallback,
            context.as_ref(),
            http_provider_count,
            providers.len(),
            bitswap_provider_candidate_available,
            has_direct_ip_bitswap_provider_candidate(providers),
        ) {
            match self
                .fetch_top_level_single_http_failed_direct_ip_bitswap_race(
                    cid,
                    providers,
                    context.clone(),
                    http_provider_count,
                )
                .await
            {
                Ok(block) => return Ok((block, RetrievalSource::Bitswap)),
                Err(RetrievalError::NoBitswapProviders) => {
                    return Err(RetrievalError::NoHttpProviders);
                }
                Err(err) => return Err(err),
            }
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

    async fn fetch_top_level_single_http_failed_direct_ip_bitswap_race(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        http_provider_count: usize,
    ) -> Result<Block> {
        let started = Instant::now();
        let provider_count = providers.len();
        tracing::info!(
            phase = "top_level_single_http_failed_direct_ip_bitswap_fallback_start",
            cid = %cid,
            provider_count,
            http_provider_count,
            min_provider_count = top_level_single_http_failed_direct_ip_bitswap_min_providers()
        );

        let mut pending =
            FuturesUnordered::<BoxFuture<'static, DirectIpBitswapFallbackResult>>::new();
        let direct_retriever = self.clone();
        let direct_cid = *cid;
        let direct_providers = providers.to_vec();
        let direct_context = context.clone();
        pending.push(
            async move {
                DirectIpBitswapFallbackResult::DirectIp(
                    direct_retriever
                        .fetch_from_bitswap_providers_with_candidate_mode(
                            &direct_cid,
                            &direct_providers,
                            direct_context,
                            BitswapProviderCandidateMode::DirectIpOnly,
                        )
                        .await,
                )
            }
            .boxed(),
        );

        let normal_retriever = self.clone();
        let normal_cid = *cid;
        let normal_providers = providers.to_vec();
        pending.push(
            async move {
                DirectIpBitswapFallbackResult::Normal(
                    normal_retriever
                        .fetch_from_bitswap_providers_with_candidate_mode(
                            &normal_cid,
                            &normal_providers,
                            context,
                            BitswapProviderCandidateMode::Default,
                        )
                        .await,
                )
            }
            .boxed(),
        );

        let mut direct_error = None;
        let mut normal_error = None;
        while let Some(result) = pending.next().await {
            match result {
                DirectIpBitswapFallbackResult::DirectIp(Ok(block)) => {
                    tracing::info!(
                        phase = "top_level_single_http_failed_direct_ip_bitswap_fallback_result",
                        cid = %cid,
                        ok = true,
                        source = "direct_ip",
                        provider_count,
                        http_provider_count,
                        normal_done = normal_error.is_some(),
                        elapsed_ms = started.elapsed().as_millis()
                    );
                    return Ok(block);
                }
                DirectIpBitswapFallbackResult::Normal(Ok(block)) => {
                    tracing::info!(
                        phase = "top_level_single_http_failed_direct_ip_bitswap_fallback_result",
                        cid = %cid,
                        ok = true,
                        source = "normal",
                        provider_count,
                        http_provider_count,
                        direct_done = direct_error.is_some(),
                        elapsed_ms = started.elapsed().as_millis()
                    );
                    return Ok(block);
                }
                DirectIpBitswapFallbackResult::DirectIp(Err(err)) => {
                    direct_error = Some(err);
                }
                DirectIpBitswapFallbackResult::Normal(Err(err)) => {
                    normal_error = Some(err);
                }
            }
        }

        let direct_error_detail = direct_error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let normal_error_detail = normal_error
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        tracing::info!(
            phase = "top_level_single_http_failed_direct_ip_bitswap_fallback_result",
            cid = %cid,
            ok = false,
            provider_count,
            http_provider_count,
            direct_error = %direct_error_detail,
            normal_error = %normal_error_detail,
            elapsed_ms = started.elapsed().as_millis()
        );
        match normal_error.or(direct_error) {
            Some(err) => Err(err),
            None => Err(RetrievalError::NoBitswapProviders),
        }
    }

    async fn fetch_top_level_multi_http_failed_direct_ip_bitswap_race(
        &self,
        cid: &Cid,
        http_provider_bases: Vec<Url>,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
    ) -> Result<Option<(Block, RetrievalSource)>> {
        if http_provider_bases.is_empty() {
            return Ok(None);
        }

        let started = Instant::now();
        let provider_count = providers.len();
        let candidates = self
            .scored_http_provider_candidates(http_provider_bases)
            .await;
        let http_provider_count = candidates.len();
        let scored_provider_count = candidates
            .iter()
            .filter(|candidate| candidate.score_elapsed.is_some())
            .count();
        let scoring_enabled = http_provider_scoring_enabled();
        tracing::info!(
            phase = "top_level_multi_http_failed_direct_ip_bitswap_fallback_gate",
            cid = %cid,
            provider_count,
            http_provider_count,
            min_provider_count = top_level_multi_http_failed_direct_ip_bitswap_min_providers()
        );
        tracing::info!(
            phase = "http_provider_race",
            cid = %cid,
            provider_count = http_provider_count,
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            scored_provider_count,
            scoring_enabled,
            multi_http_failed_direct_ip_bitswap_fallback = true
        );

        let mut next_bases = candidates.into_iter().enumerate();
        let mut pending = FuturesUnordered::<
            BoxFuture<'static, MultiHttpFailedDirectIpBitswapFallbackResult>,
        >::new();
        let mut attempted_provider_count = 0usize;
        let mut failed_provider_count = 0usize;
        for _ in 0..HTTP_PROVIDER_RACE_WIDTH {
            let Some((scheduled_index, candidate)) = next_bases.next() else {
                break;
            };
            attempted_provider_count += 1;
            push_multi_http_provider_candidate(
                &mut pending,
                self.clone(),
                *cid,
                scheduled_index,
                candidate,
                0,
            );
        }

        let hedge = tokio::time::sleep(HTTP_PROVIDER_HEDGE_AFTER);
        tokio::pin!(hedge);
        let mut completion_seen_before_hedge = false;
        let mut hedge_fired = false;
        let mut direct_ip_bitswap_started = false;
        let mut direct_ip_error = None;

        while !pending.is_empty() {
            tokio::select! {
                biased;

                result = pending.next() => {
                    let Some(result) = result else {
                        break;
                    };
                    match result {
                        MultiHttpFailedDirectIpBitswapFallbackResult::HttpCandidate(result) => {
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
                                        provider_count = http_provider_count,
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
                                        multi_http_failed_direct_ip_bitswap_fallback = true,
                                        direct_ip_bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    tracing::info!(
                                        phase = "top_level_multi_http_failed_direct_ip_bitswap_fallback_result",
                                        cid = %cid,
                                        ok = true,
                                        source = "http_provider",
                                        provider_count,
                                        http_provider_count,
                                        attempted_provider_count,
                                        failed_provider_count,
                                        direct_ip_bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    return Ok(Some((block, RetrievalSource::HttpProvider)));
                                }
                                Err(_) => {
                                    failed_provider_count += 1;
                                    if !direct_ip_bitswap_started {
                                        direct_ip_bitswap_started = true;
                                        push_multi_http_failed_direct_ip_bitswap(
                                            &mut pending,
                                            self.clone(),
                                            *cid,
                                            providers.to_vec(),
                                            context.clone(),
                                            MultiHttpFailedDirectIpBitswapTrace {
                                                provider_count,
                                                http_provider_count,
                                                attempted_provider_count,
                                                failed_provider_count,
                                                started,
                                            },
                                        );
                                    }
                                    if let Some((scheduled_index, candidate)) = next_bases.next() {
                                        attempted_provider_count += 1;
                                        push_multi_http_provider_candidate(
                                            &mut pending,
                                            self.clone(),
                                            *cid,
                                            scheduled_index,
                                            candidate,
                                            0,
                                        );
                                    }
                                }
                            }
                        }
                        MultiHttpFailedDirectIpBitswapFallbackResult::DirectIp(Ok(block)) => {
                            tracing::info!(
                                phase = "top_level_multi_http_failed_direct_ip_bitswap_fallback_result",
                                cid = %cid,
                                ok = true,
                                source = "direct_ip",
                                provider_count,
                                http_provider_count,
                                attempted_provider_count,
                                failed_provider_count,
                                elapsed_ms = started.elapsed().as_millis()
                            );
                            return Ok(Some((block, RetrievalSource::Bitswap)));
                        }
                        MultiHttpFailedDirectIpBitswapFallbackResult::DirectIp(Err(err)) => {
                            direct_ip_error = Some(err);
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
                            remaining_provider_count = next_bases.len(),
                            multi_http_failed_direct_ip_bitswap_fallback = true
                        );
                        attempted_provider_count += 1;
                        push_multi_http_provider_candidate(
                            &mut pending,
                            self.clone(),
                            *cid,
                            scheduled_index,
                            candidate,
                            0,
                        );
                    }
                }
            }
        }

        tracing::info!(
            phase = "http_provider_race_result",
            cid = %cid,
            ok = false,
            provider_count = http_provider_count,
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            attempted_provider_count,
            failed_provider_count,
            hedge_fired,
            multi_http_failed_direct_ip_bitswap_fallback = true,
            direct_ip_bitswap_started,
            elapsed_ms = started.elapsed().as_millis()
        );
        if let Some(err) = direct_ip_error {
            tracing::info!(
                phase = "top_level_multi_http_failed_direct_ip_bitswap_fallback_result",
                cid = %cid,
                ok = false,
                source = "direct_ip",
                provider_count,
                http_provider_count,
                attempted_provider_count,
                failed_provider_count,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
        }
        Ok(None)
    }

    fn maybe_spawn_top_level_bitswap_provider_preconnect(
        &self,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        http_provider_count: usize,
        bitswap_provider_candidate_available: bool,
    ) {
        if !top_level_bitswap_provider_preconnect_enabled()
            || http_provider_count == 0
            || !bitswap_provider_candidate_available
        {
            return;
        }
        let Some(context) = context else {
            return;
        };
        if !context.gateway_subresource() {
            return;
        }
        let Some(top_level_path) = context.top_level_path().map(ToOwned::to_owned) else {
            return;
        };

        let retriever = self.clone();
        let providers = providers.to_vec();
        tokio::spawn(async move {
            with_retrieval_request_context(context, async move {
                let started = Instant::now();
                let current_context = current_retrieval_request_context();
                let gateway_subresource = current_context
                    .as_ref()
                    .is_some_and(RetrievalRequestContext::gateway_subresource);
                let Some(preconnect_request_index) = retriever
                    .reserve_top_level_bitswap_provider_preconnect(&top_level_path)
                    .await
                else {
                    tracing::info!(
                        phase = "bitswap_provider_preconnect_start",
                        top_level_path = %top_level_path,
                        gateway_subresource,
                        provider_count = providers.len(),
                        http_provider_count,
                        peer_count = 0usize,
                        skipped = true,
                        reason = "top_level_budget_exhausted",
                        max_request_count = top_level_bitswap_provider_preconnect_max_requests(),
                        elapsed_ms = started.elapsed().as_millis()
                    );
                    return;
                };
                let provider_count = providers.len();
                let BitswapProviderCandidates { mut peers, quality } = retriever
                    .bitswap_peers_with_quality(&providers, current_context.as_ref())
                    .await;
                let provider_peer_count = peers.len();
                if provider_peer_count == 0 {
                    tracing::info!(
                        phase = "bitswap_provider_preconnect_start",
                        top_level_path = %top_level_path,
                        gateway_subresource,
                        provider_count,
                        http_provider_count,
                        provider_peer_count,
                        peer_count = 0usize,
                        skipped = true,
                        reason = "no_bitswap_peers",
                        preconnect_request_index,
                        elapsed_ms = started.elapsed().as_millis()
                    );
                    return;
                }

                retriever
                    .apply_successful_bitswap_peer_scores(&mut peers)
                    .await;
                let peer_limit = top_level_bitswap_provider_preconnect_peers();
                peers.truncate(peer_limit);
                let peer_count = peers.len();
                tracing::info!(
                    phase = "bitswap_provider_preconnect_start",
                    top_level_path = %top_level_path,
                    gateway_subresource,
                    provider_count,
                    http_provider_count,
                    provider_peer_count,
                    peer_count,
                    peer_limit,
                    preconnect_request_index,
                    provider_addr_count = quality.provider_addr_count,
                    supported_provider_addr_count = quality.supported_addr_count,
                    rejected_provider_addr_count = quality.rejected_addr_count(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                match retriever.shared_bitswap_client().await {
                    Ok(client) => {
                        if let Err(err) = client
                            .preconnect(
                                peers,
                                "top_level_provider_preconnect",
                                Some(top_level_path),
                                gateway_subresource,
                            )
                            .await
                        {
                            tracing::info!(
                                phase = "bitswap_provider_preconnect_error",
                                error = %err,
                                elapsed_ms = started.elapsed().as_millis()
                            );
                        }
                    }
                    Err(err) => {
                        tracing::info!(
                            phase = "bitswap_provider_preconnect_error",
                            error = %err,
                            elapsed_ms = started.elapsed().as_millis()
                        );
                    }
                }
            })
            .await;
        });
    }

    async fn reserve_top_level_bitswap_provider_preconnect(
        &self,
        top_level_path: &str,
    ) -> Option<usize> {
        let max_request_count = top_level_bitswap_provider_preconnect_max_requests();
        let mut counts = self
            .top_level_bitswap_provider_preconnect_counts
            .lock()
            .await;
        if !counts.contains_key(top_level_path)
            && counts.len() >= MAX_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_TRACKED_PATHS
        {
            counts.clear();
        }
        let count = counts.entry(top_level_path.to_owned()).or_default();
        if *count >= max_request_count {
            return None;
        }
        *count += 1;
        Some(*count)
    }

    async fn single_http_provider_bitswap_hedge_budget_allows(
        &self,
        cid: &Cid,
        context: Option<&RetrievalRequestContext>,
        provider: &Url,
    ) -> bool {
        let Some(max_per_top_level) = single_http_provider_bitswap_hedge_max_per_top_level() else {
            return true;
        };
        let Some(context) = context else {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %provider,
                reason = "budget_missing_context",
                max_per_top_level
            );
            return false;
        };
        if !context.gateway_subresource() {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %provider,
                reason = "budget_non_subresource",
                max_per_top_level
            );
            return false;
        }
        let Some(top_level_path) = context.top_level_path() else {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %provider,
                reason = "budget_missing_top_level_path",
                max_per_top_level
            );
            return false;
        };
        let Some(used_count) = self
            .reserve_single_http_provider_bitswap_hedge(top_level_path, max_per_top_level)
            .await
        else {
            tracing::info!(
                phase = "http_provider_bitswap_hedge_skip",
                cid = %cid,
                provider = %provider,
                top_level_path = %top_level_path,
                reason = "top_level_budget_exhausted",
                max_per_top_level
            );
            return false;
        };
        tracing::info!(
            phase = "http_provider_bitswap_hedge_budget",
            cid = %cid,
            provider = %provider,
            top_level_path = %top_level_path,
            used_count,
            max_per_top_level
        );
        true
    }

    async fn reserve_single_http_provider_bitswap_hedge(
        &self,
        top_level_path: &str,
        max_per_top_level: usize,
    ) -> Option<usize> {
        if max_per_top_level == 0 {
            return None;
        }
        let mut counts = self.single_http_bitswap_hedge_counts.lock().await;
        if !counts.contains_key(top_level_path)
            && counts.len() >= MAX_SINGLE_HTTP_BITSWAP_HEDGE_TRACKED_PATHS
        {
            counts.clear();
        }
        let count = counts.entry(top_level_path.to_owned()).or_default();
        if *count >= max_per_top_level {
            return None;
        }
        *count += 1;
        Some(*count)
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
        let provider_fetch = self.fetch_from_providers_with_source(cid, providers, context.clone());
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
                        let provider_result_elapsed_ms = race_started.elapsed().as_millis();
                        let mut provider_win_grace_outcome = "disabled";
                        if source == RetrievalSource::HttpProvider {
                            if let Some(provider_win_grace) =
                                top_level_single_http_provider_win_bitswap_grace(
                                    context.as_ref(),
                                    http_provider_count,
                                )
                            {
                                let provider_win_grace_started = Instant::now();
                                match timeout(provider_win_grace, shortcut.as_mut()).await {
                                    Ok(Ok(Some(bitswap_block))) => {
                                        tracing::info!(
                                            phase = "bitswap_session_shortcut_provider_win_grace",
                                            cid = %cid,
                                            outcome = "bitswap_won",
                                            timeout_ms = provider_win_grace.as_millis(),
                                            elapsed_ms = provider_win_grace_started.elapsed().as_millis(),
                                            provider_result_elapsed_ms,
                                            provider_count,
                                            http_provider_count
                                        );
                                        tracing::info!(
                                            phase = "bitswap_session_shortcut_post_lookup_race",
                                            cid = %cid,
                                            outcome = "bitswap_won_after_provider_win_grace",
                                            source = RetrievalSource::Bitswap.as_str(),
                                            timeout_ms = post_lookup_grace.as_millis(),
                                            elapsed_ms = race_started.elapsed().as_millis(),
                                            provider_result_elapsed_ms,
                                            provider_count,
                                            http_provider_count
                                        );
                                        return Ok(Some((bitswap_block, RetrievalSource::Bitswap)));
                                    }
                                    Ok(Ok(None)) => {
                                        provider_win_grace_outcome = "bitswap_miss";
                                        tracing::info!(
                                            phase = "bitswap_session_shortcut_provider_win_grace",
                                            cid = %cid,
                                            outcome = provider_win_grace_outcome,
                                            timeout_ms = provider_win_grace.as_millis(),
                                            elapsed_ms = provider_win_grace_started.elapsed().as_millis(),
                                            provider_result_elapsed_ms,
                                            provider_count,
                                            http_provider_count
                                        );
                                    }
                                    Ok(Err(err)) => {
                                        provider_win_grace_outcome = "bitswap_error";
                                        tracing::info!(
                                            phase = "bitswap_session_shortcut_provider_win_grace",
                                            cid = %cid,
                                            outcome = provider_win_grace_outcome,
                                            timeout_ms = provider_win_grace.as_millis(),
                                            elapsed_ms = provider_win_grace_started.elapsed().as_millis(),
                                            provider_result_elapsed_ms,
                                            provider_count,
                                            http_provider_count,
                                            error = %err
                                        );
                                    }
                                    Err(_) => {
                                        provider_win_grace_outcome = "timeout";
                                        tracing::info!(
                                            phase = "bitswap_session_shortcut_provider_win_grace",
                                            cid = %cid,
                                            outcome = provider_win_grace_outcome,
                                            timeout_ms = provider_win_grace.as_millis(),
                                            elapsed_ms = provider_win_grace_started.elapsed().as_millis(),
                                            provider_result_elapsed_ms,
                                            provider_count,
                                            http_provider_count
                                        );
                                    }
                                }
                            }
                        }
                        tracing::info!(
                            phase = "bitswap_session_shortcut_post_lookup_race",
                            cid = %cid,
                            outcome = "provider_won",
                            source = source.as_str(),
                            timeout_ms = post_lookup_grace.as_millis(),
                            elapsed_ms = race_started.elapsed().as_millis(),
                            provider_result_elapsed_ms,
                            provider_win_grace_outcome,
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

    async fn wait_for_session_shortcut_post_lookup<F>(
        &self,
        cid: &Cid,
        providers: &[Provider],
        http_provider_count: usize,
        mut shortcut: Pin<&mut F>,
    ) -> Result<SessionShortcutPostLookupWait>
    where
        F: Future<Output = Result<Option<Block>>>,
    {
        let post_lookup_grace = bitswap_session_post_lookup_grace(providers);
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
                    Ok(SessionShortcutPostLookupWait::Hit(block))
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
                    Ok(SessionShortcutPostLookupWait::Miss)
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
                    Err(err)
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
                Ok(SessionShortcutPostLookupWait::Timeout)
            }
        }
    }

    async fn fetch_after_zero_http_post_lookup_dns_prefetch<F>(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        mut shortcut: Pin<&mut F>,
    ) -> Result<ZeroHttpPostLookupDnsPrefetchOutcome>
    where
        F: Future<Output = Result<Option<Block>>>,
    {
        let post_lookup_grace = bitswap_session_post_lookup_grace(providers);
        let http_provider_count = provider_http_url_count(providers);
        let provider_count = providers.len();
        if !zero_http_post_lookup_dns_prefetch_enabled() {
            return Ok(ZeroHttpPostLookupDnsPrefetchOutcome::NotAttempted);
        }
        if !zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            context.as_ref(),
            http_provider_count,
            provider_count,
            has_bitswap_provider_candidate(providers),
            zero_http_post_lookup_dns_prefetch_min_providers(),
        ) {
            return Ok(ZeroHttpPostLookupDnsPrefetchOutcome::NotAttempted);
        }

        let started = Instant::now();
        tracing::info!(
            phase = "zero_http_post_lookup_dns_prefetch_start",
            cid = %cid,
            provider_count,
            http_provider_count,
            timeout_ms = post_lookup_grace.as_millis(),
            min_provider_count = zero_http_post_lookup_dns_prefetch_min_providers()
        );

        let dns_prefetch = self.prefetch_bitswap_dns_expansion_caches(providers);
        tokio::pin!(dns_prefetch);
        let post_lookup_wait = tokio::time::sleep(post_lookup_grace);
        tokio::pin!(post_lookup_wait);
        let mut dns_caches = None;
        let mut wait_timed_out = false;

        loop {
            tokio::select! {
                shortcut_result = shortcut.as_mut() => {
                    match shortcut_result {
                        Ok(Some(block)) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                cid = %cid,
                                outcome = "hit",
                                timeout_ms = post_lookup_grace.as_millis(),
                                elapsed_ms = started.elapsed().as_millis(),
                                provider_count,
                                http_provider_count
                            );
                            return Ok(ZeroHttpPostLookupDnsPrefetchOutcome::Fetched(
                                block,
                                RetrievalSource::Bitswap,
                            ));
                        }
                        Ok(None) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                cid = %cid,
                                outcome = "miss",
                                timeout_ms = post_lookup_grace.as_millis(),
                                elapsed_ms = started.elapsed().as_millis(),
                                provider_count,
                                http_provider_count
                            );
                            break;
                        }
                        Err(err) => {
                            tracing::info!(
                                phase = "bitswap_session_shortcut_post_lookup_wait",
                                cid = %cid,
                                outcome = "error",
                                timeout_ms = post_lookup_grace.as_millis(),
                                elapsed_ms = started.elapsed().as_millis(),
                                provider_count,
                                http_provider_count,
                                error = %err
                            );
                            return Err(err);
                        }
                    }
                }
                prefetch_result = &mut dns_prefetch, if dns_caches.is_none() => {
                    let dnsaddr_cache_len = prefetch_result.dnsaddr_cache.len();
                    let dns_ip_cache_len = prefetch_result.dns_ip_cache.len();
                    dns_caches = Some(prefetch_result);
                    tracing::info!(
                        phase = "zero_http_post_lookup_dns_prefetch_ready",
                        cid = %cid,
                        provider_count,
                        http_provider_count,
                        timeout_ms = post_lookup_grace.as_millis(),
                        dnsaddr_cache_len,
                        dns_ip_cache_len,
                        elapsed_ms = started.elapsed().as_millis()
                    );
                }
                _ = &mut post_lookup_wait, if !wait_timed_out => {
                    wait_timed_out = true;
                    tracing::info!(
                        phase = "bitswap_session_shortcut_post_lookup_wait",
                        cid = %cid,
                        outcome = "timeout",
                        timeout_ms = post_lookup_grace.as_millis(),
                        elapsed_ms = started.elapsed().as_millis(),
                        provider_count,
                        http_provider_count
                    );
                    break;
                }
            }
        }

        let dns_caches = match dns_caches {
            Some(dns_caches) => dns_caches,
            None => dns_prefetch.await,
        };
        let fetch_started = Instant::now();
        match self
            .fetch_from_bitswap_providers_with_candidate_mode_and_dns_caches(
                cid,
                providers,
                context,
                BitswapProviderCandidateMode::Default,
                Some(dns_caches),
            )
            .await
        {
            Ok(block) => {
                tracing::info!(
                    phase = "zero_http_post_lookup_dns_prefetch_result",
                    cid = %cid,
                    ok = true,
                    provider_count,
                    http_provider_count,
                    wait_timed_out,
                    fetch_elapsed_ms = fetch_started.elapsed().as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                Ok(ZeroHttpPostLookupDnsPrefetchOutcome::Fetched(
                    block,
                    RetrievalSource::Bitswap,
                ))
            }
            Err(err) => {
                tracing::info!(
                    phase = "zero_http_post_lookup_dns_prefetch_result",
                    cid = %cid,
                    ok = false,
                    provider_count,
                    http_provider_count,
                    wait_timed_out,
                    error = %err,
                    fetch_elapsed_ms = fetch_started.elapsed().as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                Ok(ZeroHttpPostLookupDnsPrefetchOutcome::ContinueAfterWait {
                    shortcut_timed_out: wait_timed_out,
                })
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
        single_http_5xx_fast_bitswap_fallback: bool,
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
                        single_http_5xx_fast_bitswap_fallback,
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
        context: Option<RetrievalRequestContext>,
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
        let candidate_base = candidate.base.clone();
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

        let bitswap_hedge_after = single_http_provider_bitswap_hedge_after();
        let hedge = tokio::time::sleep(bitswap_hedge_after);
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
                    if self
                        .single_http_provider_bitswap_hedge_budget_allows(
                            cid,
                            context.as_ref(),
                            &candidate_base,
                        )
                        .await
                    {
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
                }
                else => break,
            }
        }

        match bitswap_error {
            Some(RetrievalError::NoBitswapProviders) | None => Err(RetrievalError::NoHttpProviders),
            Some(err) => Err(err),
        }
    }

    async fn fetch_single_http_provider_with_session_bitswap_hedge(
        &self,
        cid: &Cid,
        http_provider_bases: Vec<Url>,
        recent_peers: Vec<BitswapPeer>,
    ) -> Result<(Block, RetrievalSource)> {
        let started = Instant::now();
        let candidates = self
            .scored_http_provider_candidates(http_provider_bases)
            .await;
        let scored_provider_count = candidates
            .iter()
            .filter(|candidate| candidate.score_elapsed.is_some())
            .count();
        let scoring_enabled = http_provider_scoring_enabled();
        let session_peer_count = recent_peers.len();
        tracing::info!(
            phase = "http_provider_race",
            cid = %cid,
            provider_count = candidates.len(),
            race_width = HTTP_PROVIDER_RACE_WIDTH,
            scored_provider_count,
            scoring_enabled,
            single_provider_bitswap_hedge = true,
            session_peer_only = true,
            session_peer_count
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

        let bitswap_hedge_after = single_http_provider_bitswap_hedge_after();
        let hedge = tokio::time::sleep(bitswap_hedge_after);
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
                                        session_peer_only = true,
                                        session_peer_count,
                                        bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    tracing::info!(
                                        phase = "http_provider_bitswap_hedge_result",
                                        cid = %cid,
                                        source = "http_provider",
                                        provider_count = 1usize,
                                        session_peer_only = true,
                                        session_peer_count,
                                        bitswap_started,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    return Ok((block, RetrievalSource::HttpProvider));
                                }
                                Err(_) => {
                                    if !bitswap_started {
                                        bitswap_started = true;
                                        push_single_http_session_bitswap_hedge(
                                            &mut pending,
                                            self.clone(),
                                            *cid,
                                            recent_peers.clone(),
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
                                provider_count = 1usize,
                                session_peer_only = true,
                                session_peer_count,
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
                    push_single_http_session_bitswap_hedge(
                        &mut pending,
                        self.clone(),
                        *cid,
                        recent_peers.clone(),
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
        single_http_5xx_fast_bitswap_fallback: bool,
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
                        Err(err) => {
                            failed_provider_count += 1;
                            let server_error_status = http_provider_server_error_status(&err);
                            if pending.is_empty()
                                && !hedge_fired
                                && single_http_5xx_fast_bitswap_fallback
                            {
                                if let Some(server_error_status) = server_error_status {
                                    tracing::info!(
                                        phase = "http_provider_self_hedge_skip",
                                        cid = %cid,
                                        provider = %candidate.base,
                                        provider_index = 0,
                                        original_provider_rank = candidate.original_index + 1,
                                        provider_scored = candidate.score_elapsed.is_some(),
                                        provider_score_ms = candidate
                                            .score_elapsed
                                            .map(|elapsed| elapsed.as_millis())
                                            .unwrap_or_default(),
                                        reason = "server_error_fast_bitswap_fallback",
                                        http_status = server_error_status,
                                        attempted_provider_count,
                                        failed_provider_count,
                                        elapsed_ms = started.elapsed().as_millis()
                                    );
                                    return Ok(None);
                                }
                            }
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

    async fn bitswap_peers_with_quality(
        &self,
        providers: &[Provider],
        context: Option<&RetrievalRequestContext>,
    ) -> BitswapProviderCandidates {
        self.bitswap_peers_with_quality_with_candidate_mode(
            providers,
            context,
            BitswapProviderCandidateMode::Default,
        )
        .await
    }

    async fn bitswap_peers_with_quality_with_candidate_mode(
        &self,
        providers: &[Provider],
        context: Option<&RetrievalRequestContext>,
        candidate_mode: BitswapProviderCandidateMode,
    ) -> BitswapProviderCandidates {
        let direct_ip_candidate_only = candidate_mode.direct_ip_candidate_only()
            || bitswap_direct_ip_provider_candidates_only_enabled();
        if direct_ip_candidate_only {
            let mut dnsaddr_cache = DnsaddrCache::new();
            let mut dns_ip_cache = DnsIpCache::new();
            return bitswap_peers_with_quality_using_caches_with_options(
                providers,
                &mut dnsaddr_cache,
                &mut dns_ip_cache,
                bitswap_early_provider_peer_cap_enabled(),
                true,
            )
            .await;
        }

        let Some(cache_scope) = bitswap_dns_expansion_cache_scope(context) else {
            return bitswap_peers_with_quality(providers).await;
        };

        let mut dnsaddr_cache = DnsaddrCache::new();
        let mut dns_ip_cache = DnsIpCache::new();
        let stats = self
            .seed_bitswap_dns_expansion_caches(providers, &mut dnsaddr_cache, &mut dns_ip_cache)
            .await;
        let candidates = bitswap_peers_with_quality_using_caches(
            providers,
            &mut dnsaddr_cache,
            &mut dns_ip_cache,
        )
        .await;
        self.record_bitswap_dns_expansion_caches(&dnsaddr_cache, &dns_ip_cache)
            .await;
        tracing::info!(
            phase = "bitswap_dns_expansion_cache",
            cache_scope,
            provider_count = providers.len(),
            candidate_peer_count = candidates.peers.len(),
            dnsaddr_requested = stats.dnsaddr_requested,
            dnsaddr_hits = stats.dnsaddr_hits,
            dnsaddr_misses = stats.dnsaddr_misses,
            dns_ip_requested = stats.dns_ip_requested,
            dns_ip_hits = stats.dns_ip_hits,
            dns_ip_misses = stats.dns_ip_misses,
            dnsaddr_cache_len = stats.dnsaddr_cache_len,
            dns_ip_cache_len = stats.dns_ip_cache_len
        );
        candidates
    }

    async fn bitswap_peers_with_quality_with_candidate_mode_and_dns_caches(
        &self,
        providers: &[Provider],
        context: Option<&RetrievalRequestContext>,
        candidate_mode: BitswapProviderCandidateMode,
        mut dns_caches: BitswapDnsExpansionCaches,
    ) -> BitswapProviderCandidates {
        if candidate_mode.direct_ip_candidate_only()
            || bitswap_direct_ip_provider_candidates_only_enabled()
        {
            return self
                .bitswap_peers_with_quality_with_candidate_mode(providers, context, candidate_mode)
                .await;
        }

        let candidates = bitswap_peers_with_quality_using_caches(
            providers,
            &mut dns_caches.dnsaddr_cache,
            &mut dns_caches.dns_ip_cache,
        )
        .await;
        self.record_bitswap_dns_expansion_caches(
            &dns_caches.dnsaddr_cache,
            &dns_caches.dns_ip_cache,
        )
        .await;
        tracing::info!(
            phase = "bitswap_dns_expansion_cache",
            cache_scope = "zero_http_post_lookup_prefetch",
            provider_count = providers.len(),
            candidate_peer_count = candidates.peers.len(),
            dnsaddr_cache_len = dns_caches.dnsaddr_cache.len(),
            dns_ip_cache_len = dns_caches.dns_ip_cache.len()
        );
        candidates
    }

    async fn prefetch_bitswap_dns_expansion_caches(
        &self,
        providers: &[Provider],
    ) -> BitswapDnsExpansionCaches {
        let mut dnsaddr_cache = DnsaddrCache::new();
        let mut dns_ip_cache = DnsIpCache::new();
        self.seed_bitswap_dns_expansion_caches(providers, &mut dnsaddr_cache, &mut dns_ip_cache)
            .await;
        prefetch_bitswap_dns_expansions(providers, &mut dnsaddr_cache, &mut dns_ip_cache).await;
        self.record_bitswap_dns_expansion_caches(&dnsaddr_cache, &dns_ip_cache)
            .await;
        BitswapDnsExpansionCaches {
            dnsaddr_cache,
            dns_ip_cache,
        }
    }

    async fn seed_bitswap_dns_expansion_caches(
        &self,
        providers: &[Provider],
        dnsaddr_cache: &mut DnsaddrCache,
        dns_ip_cache: &mut DnsIpCache,
    ) -> BitswapDnsExpansionCacheStats {
        let now = Instant::now();
        let dnsaddr_hosts = provider_dnsaddr_hosts(providers);
        let mut stats = BitswapDnsExpansionCacheStats {
            dnsaddr_requested: dnsaddr_hosts.len(),
            ..BitswapDnsExpansionCacheStats::default()
        };
        {
            let mut shared = self.bitswap_dnsaddr_cache.lock().await;
            prune_shared_dnsaddr_cache(&mut shared, now);
            for host in &dnsaddr_hosts {
                if let Some(entry) = shared.get_mut(host) {
                    entry.seen_at = now;
                    dnsaddr_cache.insert(
                        host.clone(),
                        CachedDnsaddrRecords {
                            records: entry.records.clone(),
                            log_as_cached: true,
                        },
                    );
                    stats.dnsaddr_hits += 1;
                } else {
                    stats.dnsaddr_misses += 1;
                }
            }
            stats.dnsaddr_cache_len = shared.len();
        }

        let dns_names = provider_dns_ip_names(providers, dnsaddr_cache);
        stats.dns_ip_requested = dns_names.len();
        {
            let mut shared = self.bitswap_dns_ip_cache.lock().await;
            prune_shared_dns_ip_cache(&mut shared, now);
            for host in &dns_names {
                if let Some(entry) = shared.get_mut(host) {
                    entry.seen_at = now;
                    dns_ip_cache.insert(
                        host.clone(),
                        CachedDnsIpRecords {
                            addrs: entry.addrs.clone(),
                            log_as_cached: true,
                        },
                    );
                    stats.dns_ip_hits += 1;
                } else {
                    stats.dns_ip_misses += 1;
                }
            }
            stats.dns_ip_cache_len = shared.len();
        }
        stats
    }

    async fn record_bitswap_dns_expansion_caches(
        &self,
        dnsaddr_cache: &DnsaddrCache,
        dns_ip_cache: &DnsIpCache,
    ) {
        let now = Instant::now();
        {
            let mut shared = self.bitswap_dnsaddr_cache.lock().await;
            prune_shared_dnsaddr_cache(&mut shared, now);
            for (host, entry) in dnsaddr_cache {
                shared.insert(
                    host.clone(),
                    SharedCachedDnsaddrRecords {
                        records: entry.records.clone(),
                        seen_at: now,
                    },
                );
            }
            prune_shared_dnsaddr_cache_len(&mut shared);
        }
        {
            let mut shared = self.bitswap_dns_ip_cache.lock().await;
            prune_shared_dns_ip_cache(&mut shared, now);
            for (host, entry) in dns_ip_cache {
                shared.insert(
                    host.clone(),
                    SharedCachedDnsIpRecords {
                        addrs: entry.addrs.clone(),
                        seen_at: now,
                    },
                );
            }
            prune_shared_dns_ip_cache_len(&mut shared);
        }
    }

    async fn fetch_from_bitswap_providers(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
    ) -> Result<Block> {
        self.fetch_from_bitswap_providers_with_candidate_mode(
            cid,
            providers,
            context,
            BitswapProviderCandidateMode::Default,
        )
        .await
    }

    async fn fetch_from_bitswap_providers_with_candidate_mode(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        candidate_mode: BitswapProviderCandidateMode,
    ) -> Result<Block> {
        self.fetch_from_bitswap_providers_with_candidate_mode_and_dns_caches(
            cid,
            providers,
            context,
            candidate_mode,
            None,
        )
        .await
    }

    async fn fetch_from_bitswap_providers_with_candidate_mode_and_dns_caches(
        &self,
        cid: &Cid,
        providers: &[Provider],
        context: Option<RetrievalRequestContext>,
        candidate_mode: BitswapProviderCandidateMode,
        dns_caches: Option<BitswapDnsExpansionCaches>,
    ) -> Result<Block> {
        let peer_started = Instant::now();
        let BitswapProviderCandidates { mut peers, quality } = match dns_caches {
            Some(dns_caches) => {
                self.bitswap_peers_with_quality_with_candidate_mode_and_dns_caches(
                    providers,
                    context.as_ref(),
                    candidate_mode,
                    dns_caches,
                )
                .await
            }
            None => {
                self.bitswap_peers_with_quality_with_candidate_mode(
                    providers,
                    context.as_ref(),
                    candidate_mode,
                )
                .await
            }
        };
        let provider_peer_count = peers.len();
        let provider_addr_score_order = bitswap_provider_addr_score_order_enabled();
        if provider_addr_score_order {
            sort_bitswap_peers_by_addr_score(&mut peers);
        }
        if provider_peer_count == 0 && !providers.is_empty() {
            tracing::info!(
                phase = "bitswap_provider_candidates_empty",
                cid = %cid,
                providers = %format_provider_candidates(providers)
            );
        }
        self.apply_successful_bitswap_peer_scores(&mut peers).await;
        let session_peer_count = self.insert_recent_bitswap_session_peers(&mut peers).await;
        let bitswap_command_context = BitswapCommandContext::from_retrieval_context(
            context.as_ref(),
            provider_http_url_count(providers),
        );
        let zero_http_subresource_peer_rotation =
            maybe_rotate_zero_http_subresource_peers(providers, context.as_ref(), cid, &mut peers);
        let gateway_context = GatewayBitswapSourceContext {
            gateway_request: context.is_some(),
            gateway_subresource: context
                .as_ref()
                .is_some_and(RetrievalRequestContext::gateway_subresource),
        };
        let zero_http_post_lookup_shortcut_timeout = context
            .as_ref()
            .is_some_and(RetrievalRequestContext::zero_http_post_lookup_shortcut_timeout);
        let zero_http_direct_want_block_limit =
            bitswap_zero_http_direct_want_block_peers(providers.len(), context.as_ref());
        let zero_http_direct_want_block_peer_count =
            maybe_force_zero_http_direct_want_block_peers_with_limit(
                providers,
                &mut peers,
                zero_http_direct_want_block_limit,
            );
        let trusted_want_have_probe_count =
            maybe_force_trusted_bitswap_want_have_probes(&mut peers);
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
            gateway_subresource = gateway_context.gateway_subresource,
            zero_http_post_lookup_shortcut_timeout,
            zero_http_direct_want_block_peer_count,
            zero_http_direct_want_block_limit = zero_http_direct_want_block_limit
                .map(|limit| limit as i64)
                .unwrap_or(-1),
            zero_http_direct_want_block_min_provider_count =
                bitswap_high_provider_zero_http_direct_want_block_min_providers()
                    .map(|min_provider_count| min_provider_count as i64)
                    .unwrap_or(-1),
            trusted_want_have_probe_count,
            zero_http_subresource_peer_rotation = zero_http_subresource_peer_rotation
                .map(|offset| offset as i64)
                .unwrap_or(-1),
            trusted_direct_want_block_limit = bitswap_trusted_direct_want_block_peers()
                .map(|limit| limit as i64)
                .unwrap_or(-1),
            direct_untrusted_want_block_limit = bitswap_direct_want_block_untrusted_peer_limit(),
            tcp_addr_count = addr_stats.tcp,
            quic_addr_count = addr_stats.quic,
            ws_addr_count = addr_stats.ws,
            wss_addr_count = addr_stats.wss,
            dns_addr_count = addr_stats.dns,
            ip4_addr_count = addr_stats.ip4,
            ip6_addr_count = addr_stats.ip6,
            provider_addr_count = quality.provider_addr_count,
            processed_provider_count = quality.processed_provider_count,
            skipped_provider_count = quality.skipped_provider_count,
            early_provider_peer_cap = quality.early_peer_cap,
            early_provider_peer_cap_hit = quality.early_peer_cap_hit,
            provider_addr_score_order,
            direct_ip_candidate_only = quality.direct_ip_candidate_only,
            direct_ip_candidate_skipped_addr_count = quality
                .direct_ip_candidate_skipped_addr_count,
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
        let result = self
            .shared_bitswap_client()
            .await?
            .fetch_with_context(*cid, peers, bitswap_command_context)
            .await;
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
        let elapsed = bitswap_started.elapsed();
        let source_peer = result.source_peer;
        let source_addr = result.source_addr.clone();
        let source_trace = self
            .bitswap_source_peer_trace(source_peer, source_addr.as_ref(), &peers_for_record)
            .await;
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
            source_peer_remote_addr = %source_addr
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            bitswap_delivery = result.delivery,
            source_peer_trusted = source_trace.skip_want_have,
            source_peer_candidate_index = source_trace
                .candidate_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_index = source_trace
                .source_addr_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_known = source_trace.source_addr_known,
            source_peer_addr_matches_candidate = source_trace.source_addr_matches_candidate,
            source_peer_addr_transport = source_trace.source_addr_transport,
            source_peer_addr_family = source_trace.source_addr_family,
            source_peer_request_mode = source_trace.request_mode,
            source_peer_force_want_block = source_trace.force_want_block,
            source_peer_force_want_have = source_trace.force_want_have,
            source_peer_addr_count = source_trace.addr_count,
            source_peer_previous_success_count = source_trace.previous_success_count,
            source_peer_previous_latency_ms = source_trace.previous_latency_ms,
            source_peer_previous_seen_age_ms = source_trace.previous_seen_age_ms,
            source_peer_previous_top_level_path = %source_trace
                .previous_top_level_path
                .as_deref()
                .unwrap_or(""),
            source_peer_same_top_level = source_trace.same_top_level,
            source_peer_cross_top_level = source_trace.cross_top_level,
            source_peer_unknown_top_level = source_trace.unknown_top_level,
            extra_blocks = result.extra_blocks.len(),
            bytes = result.requested_block.len(),
            elapsed_ms = elapsed.as_millis()
        );
        if let Some(peer) = source_peer {
            self.maybe_mark_slow_zero_http_source_peer(
                cid,
                providers,
                gateway_context,
                source_peer,
                &source_trace,
                elapsed,
            );
            self.record_successful_bitswap_peer_from_fetch_source(
                peer,
                &peers_for_record,
                source_addr.as_ref(),
                elapsed,
            )
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

    fn maybe_mark_slow_zero_http_source_peer(
        &self,
        cid: &Cid,
        providers: &[Provider],
        gateway_context: GatewayBitswapSourceContext,
        source_peer: Option<PeerId>,
        source_trace: &BitswapSourcePeerTrace,
        elapsed: Duration,
    ) -> bool {
        if source_peer.is_none()
            || source_trace.skip_want_have
            || source_trace.candidate_index.is_none()
        {
            return false;
        }
        if let Some(threshold) = zero_http_gateway_slow_source_suppression_threshold() {
            if self.maybe_mark_slow_zero_http_source_peer_with_threshold(
                cid,
                SlowZeroHttpSourcePeer {
                    gateway_request: gateway_context.gateway_request,
                    gateway_subresource: gateway_context.gateway_subresource,
                    http_provider_count: provider_http_url_count(providers),
                    source_peer,
                    source_trace,
                    elapsed,
                    threshold,
                    trace_phase: "bitswap_slow_zero_http_gateway_source_suppressed",
                    bad_provider_reason: "slow zero-http gateway bitswap source",
                },
            ) {
                return true;
            }
        }
        if !gateway_context.gateway_subresource {
            return false;
        }
        let Some(threshold) = zero_http_subresource_slow_source_suppression_threshold() else {
            return false;
        };
        self.maybe_mark_slow_zero_http_source_peer_with_threshold(
            cid,
            SlowZeroHttpSourcePeer {
                gateway_request: gateway_context.gateway_request,
                gateway_subresource: gateway_context.gateway_subresource,
                http_provider_count: provider_http_url_count(providers),
                source_peer,
                source_trace,
                elapsed,
                threshold,
                trace_phase: "bitswap_slow_zero_http_subresource_source_suppressed",
                bad_provider_reason: "slow zero-http subresource bitswap source",
            },
        )
    }

    fn maybe_mark_slow_zero_http_source_peer_with_threshold(
        &self,
        cid: &Cid,
        candidate: SlowZeroHttpSourcePeer<'_>,
    ) -> bool {
        let Some(peer) = candidate.source_peer else {
            return false;
        };
        if !candidate.gateway_request
            || candidate.http_provider_count != 0
            || candidate.elapsed <= candidate.threshold
            || candidate.source_trace.skip_want_have
            || candidate.source_trace.candidate_index.is_none()
        {
            return false;
        }
        tracing::info!(
            phase = candidate.trace_phase,
            cid = %cid,
            peer = %peer,
            gateway_subresource = candidate.gateway_subresource,
            latency_ms = candidate.elapsed.as_millis(),
            threshold_ms = candidate.threshold.as_millis(),
            source_peer_candidate_index = candidate.source_trace
                .candidate_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_request_mode = candidate.source_trace.request_mode,
            ttl_secs = BAD_BITSWAP_PROVIDER_TTL.as_secs()
        );
        let _ = self.store.mark_bad_provider(
            &peer.to_string(),
            candidate.bad_provider_reason,
            BAD_BITSWAP_PROVIDER_TTL,
        );
        true
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
        let success_count = successes
            .get(&peer)
            .map(|success| success.success_count.saturating_add(1))
            .unwrap_or(1);
        let top_level_path = current_retrieval_request_context()
            .and_then(|context| context.top_level_path().map(ToOwned::to_owned));
        let addr_count = addrs.len();
        tracing::info!(
            phase = "bitswap_successful_peer_recorded",
            peer = %peer,
            success_count,
            latency_ms = last_latency.as_millis(),
            addr_count,
            top_level_path = %top_level_path.as_deref().unwrap_or("")
        );
        successes.insert(
            peer,
            SuccessfulBitswapPeer {
                seen_at: Instant::now(),
                addrs,
                last_latency,
                success_count,
                top_level_path,
            },
        );
    }

    async fn record_successful_bitswap_peer_from_fetch_source(
        &self,
        peer: PeerId,
        peers: &[BitswapPeer],
        source_addr: Option<&Multiaddr>,
        last_latency: Duration,
    ) {
        self.record_successful_bitswap_peer_from_fetch_source_with_enabled(
            peer,
            peers,
            source_addr,
            last_latency,
            bitswap_incoming_source_addr_session_peers_enabled(),
        )
        .await;
    }

    async fn record_successful_bitswap_peer_from_fetch_source_with_enabled(
        &self,
        peer: PeerId,
        peers: &[BitswapPeer],
        source_addr: Option<&Multiaddr>,
        last_latency: Duration,
        allow_source_addr: bool,
    ) {
        if let Some(candidate) = peers.iter().find(|candidate| candidate.id == peer) {
            self.record_successful_bitswap_peer(peer, candidate.addrs.clone(), last_latency)
                .await;
            return;
        }

        let Some(source_addr) = source_addr else {
            tracing::info!(
                phase = "bitswap_successful_peer_source_addr",
                peer = %peer,
                recorded = false,
                enabled = allow_source_addr,
                reason = "source_addr_unavailable",
                latency_ms = last_latency.as_millis()
            );
            return;
        };

        let session_addr = bitswap_session_addr_from_remote_addr(source_addr);

        if !allow_source_addr {
            tracing::info!(
                phase = "bitswap_successful_peer_source_addr",
                peer = %peer,
                source_addr = %session_addr,
                recorded = false,
                enabled = false,
                reason = "flag_disabled",
                latency_ms = last_latency.as_millis()
            );
            return;
        }

        tracing::info!(
            phase = "bitswap_successful_peer_source_addr",
            peer = %peer,
            source_addr = %session_addr,
            recorded = true,
            enabled = true,
            latency_ms = last_latency.as_millis()
        );
        self.record_successful_bitswap_peer(peer, vec![session_addr], last_latency)
            .await;
    }

    async fn bitswap_source_peer_trace(
        &self,
        source_peer: Option<PeerId>,
        source_addr: Option<&Multiaddr>,
        peers: &[BitswapPeer],
    ) -> BitswapSourcePeerTrace {
        let mut trace = bitswap_source_peer_trace_from_peers(source_peer, source_addr, peers);
        if let Some(peer) = source_peer {
            let successes = self.successful_bitswap_peers.lock().await;
            if let Some(success) = successes.get(&peer) {
                trace.previous_success_count = success.success_count;
                trace.previous_latency_ms = success.last_latency.as_millis();
                trace.previous_seen_age_ms = success.seen_at.elapsed().as_millis();
                trace.previous_top_level_path = success.top_level_path.clone();
                let current_top_level_path = current_retrieval_request_context()
                    .and_then(|context| context.top_level_path().map(ToOwned::to_owned));
                match (
                    success.top_level_path.as_deref(),
                    current_top_level_path.as_deref(),
                ) {
                    (Some(previous), Some(current)) if previous == current => {
                        trace.same_top_level = true;
                    }
                    (Some(_), Some(_)) => {
                        trace.cross_top_level = true;
                    }
                    _ => {
                        trace.unknown_top_level = true;
                    }
                }
            }
        }
        trace
    }

    async fn recent_bitswap_peers(&self) -> Vec<BitswapPeer> {
        self.recent_bitswap_peers_with_min_successes(bitswap_session_peer_min_successes())
            .await
    }

    async fn recent_bitswap_peers_with_min_successes(
        &self,
        min_successes: u64,
    ) -> Vec<BitswapPeer> {
        let now = Instant::now();
        let mut successes = self.successful_bitswap_peers.lock().await;
        successes.retain(|_, success| {
            now.duration_since(success.seen_at) <= BITSWAP_SUCCESSFUL_PEER_TTL
                && !success.addrs.is_empty()
        });
        let scoped_session_peers = bitswap_top_level_scoped_session_peers_enabled();
        let min_successes = min_successes.max(BITSWAP_SESSION_PEER_MIN_SUCCESSES);
        let current_top_level_path = scoped_session_peers
            .then(current_retrieval_request_context)
            .flatten()
            .and_then(|context| context.top_level_path().map(ToOwned::to_owned));
        let peer_count_before_scope = successes.len();
        let scoped_successes = successes
            .iter()
            .filter(|(_, success)| {
                successful_peer_matches_top_level_scope(
                    success,
                    scoped_session_peers,
                    current_top_level_path.as_deref(),
                )
            })
            .map(|(id, success)| {
                (
                    *id,
                    success.seen_at,
                    success.last_latency,
                    success.addrs.clone(),
                    success.success_count,
                )
            })
            .collect::<Vec<_>>();
        let peer_count_after_scope = scoped_successes.len();
        let mut peers = scoped_successes
            .into_iter()
            .filter(|(_, _, _, _, success_count)| *success_count >= min_successes)
            .collect::<Vec<_>>();
        if scoped_session_peers {
            tracing::info!(
                phase = "bitswap_session_peer_scope",
                top_level_path = %current_top_level_path.as_deref().unwrap_or(""),
                peer_count_before = peer_count_before_scope,
                peer_count_after = peer_count_after_scope,
                skipped_peer_count = peer_count_before_scope.saturating_sub(peer_count_after_scope)
            );
        }
        if min_successes > BITSWAP_SESSION_PEER_MIN_SUCCESSES {
            tracing::info!(
                phase = "bitswap_session_peer_min_successes",
                min_successes,
                peer_count_before = peer_count_after_scope,
                peer_count_after = peers.len(),
                skipped_peer_count = peer_count_after_scope.saturating_sub(peers.len())
            );
        }
        if let Some(dominant_mode) = bitswap_dominant_session_peer_mode() {
            if let Some(selection) = select_dominant_recent_bitswap_peers(
                &mut peers,
                bitswap_dominant_session_peer_alternates(),
            ) {
                tracing::info!(
                    phase = "bitswap_dominant_session_peer",
                    peer = %selection.peer,
                    peer_count = selection.peer_count,
                    selected_peer_count = selection.selected_peer_count,
                    alternate_count = selection.alternate_count,
                    alternate_limit = selection.alternate_limit,
                    success_count = selection.success_count,
                    next_success_count = selection.next_success_count,
                    latency_ms = selection.latency.as_millis(),
                    mode = dominant_mode.as_str(),
                    min_success_count = BITSWAP_DOMINANT_SESSION_PEER_MIN_SUCCESSES,
                    dominance_ratio = BITSWAP_DOMINANT_SESSION_PEER_RATIO
                );
            }
        }
        peers.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| right.1.cmp(&left.1)));
        peers
            .into_iter()
            .take(bitswap_session_peer_limit())
            .map(
                |(id, _seen_at, _last_latency, addrs, _success_count)| BitswapPeer {
                    id,
                    addrs,
                    skip_want_have: true,
                    force_want_block: false,
                    force_want_have: false,
                },
            )
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
        mut peers: Vec<BitswapPeer>,
    ) -> Result<Option<Block>> {
        if peers.is_empty() {
            return Ok(None);
        }

        let trusted_want_have_probe_count =
            maybe_force_trusted_bitswap_want_have_probes(&mut peers);
        let peer_count = peers.len();
        let peer_quality = self.bitswap_session_peer_quality(&peers).await;
        tracing::info!(
            phase = "bitswap_session_shortcut_start",
            cid = %cid,
            peer_count,
            trusted_peer_count = peer_count,
            trusted_want_have_probe_count,
            session_peer_scored_count = peer_quality.scored_count,
            session_peer_success_count_min = peer_quality.success_count_min,
            session_peer_success_count_max = peer_quality.success_count_max,
            session_peer_latency_ms_min = peer_quality.latency_ms_min,
            session_peer_latency_ms_max = peer_quality.latency_ms_max,
            session_peer_seen_age_ms_min = peer_quality.seen_age_ms_min,
            session_peer_seen_age_ms_max = peer_quality.seen_age_ms_max,
            session_peer_same_top_level_count = peer_quality.same_top_level_count,
            session_peer_cross_top_level_count = peer_quality.cross_top_level_count,
            session_peer_unknown_top_level_count = peer_quality.unknown_top_level_count,
            trusted_direct_want_block_limit = bitswap_trusted_direct_want_block_peers()
                .map(|limit| limit as i64)
                .unwrap_or(-1)
        );
        let peers_for_record = peers.clone();
        let started = Instant::now();
        let request_context = current_retrieval_request_context();
        let bitswap_command_context =
            BitswapCommandContext::from_retrieval_context(request_context.as_ref(), 0);
        let fetch = async {
            let client = self.shared_bitswap_client().await?;
            client
                .fetch_with_context(*cid, peers, bitswap_command_context)
                .await
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
        let source_addr = result.source_addr.clone();
        let source_trace = self
            .bitswap_source_peer_trace(result.source_peer, source_addr.as_ref(), &peers_for_record)
            .await;
        tracing::info!(
            phase = "bitswap_session_shortcut",
            cid = %cid,
            peer_count,
            trusted_peer_count = peer_count,
            ok = true,
            source_peer = result.source_peer.map(|peer| peer.to_string()).unwrap_or_default(),
            source_transport = result.source_transport.unwrap_or("unknown"),
            source_peer_remote_addr = %source_addr
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            bitswap_delivery = result.delivery,
            source_peer_trusted = source_trace.skip_want_have,
            source_peer_candidate_index = source_trace
                .candidate_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_index = source_trace
                .source_addr_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_known = source_trace.source_addr_known,
            source_peer_addr_matches_candidate = source_trace.source_addr_matches_candidate,
            source_peer_addr_transport = source_trace.source_addr_transport,
            source_peer_addr_family = source_trace.source_addr_family,
            source_peer_request_mode = source_trace.request_mode,
            source_peer_force_want_block = source_trace.force_want_block,
            source_peer_force_want_have = source_trace.force_want_have,
            source_peer_addr_count = source_trace.addr_count,
            source_peer_previous_success_count = source_trace.previous_success_count,
            source_peer_previous_latency_ms = source_trace.previous_latency_ms,
            source_peer_previous_seen_age_ms = source_trace.previous_seen_age_ms,
            source_peer_previous_top_level_path = %source_trace
                .previous_top_level_path
                .as_deref()
                .unwrap_or(""),
            source_peer_same_top_level = source_trace.same_top_level,
            source_peer_cross_top_level = source_trace.cross_top_level,
            source_peer_unknown_top_level = source_trace.unknown_top_level,
            extra_blocks = result.extra_blocks.len(),
            bytes = result.requested_block.len(),
            elapsed_ms = elapsed.as_millis()
        );
        if let Some(peer) = result.source_peer {
            self.record_successful_bitswap_peer_from_fetch_source(
                peer,
                &peers_for_record,
                source_addr.as_ref(),
                elapsed,
            )
            .await;
        }
        self.store_bitswap_result(cid, result).await.map(Some)
    }

    async fn bitswap_session_peer_quality(
        &self,
        peers: &[BitswapPeer],
    ) -> BitswapSessionPeerQuality {
        let successes = self.successful_bitswap_peers.lock().await;
        let current_top_level_path = current_retrieval_request_context()
            .and_then(|context| context.top_level_path().map(ToOwned::to_owned));
        bitswap_session_peer_quality_from_successes(
            peers,
            &successes,
            Instant::now(),
            current_top_level_path.as_deref(),
        )
    }

    async fn fetch_many_from_recent_bitswap_peers(
        &self,
        cids: Vec<Cid>,
    ) -> Result<Option<HashMap<Cid, Block>>> {
        if cids.len() < 2 {
            return Ok(None);
        }

        let mut peers = self.recent_bitswap_peers_for_fetch().await;
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

        let trusted_want_have_probe_count =
            maybe_force_trusted_bitswap_want_have_probes(&mut peers);
        let peer_count = peers.len();
        let peers_for_record = peers.clone();
        let started = Instant::now();
        tracing::info!(
            phase = "bitswap_session_range_batch_start",
            cids = %format_cids(&cids),
            cid_count = cids.len(),
            peer_count,
            trusted_peer_count = peer_count,
            trusted_want_have_probe_count,
            trusted_direct_want_block_limit = bitswap_trusted_direct_want_block_peers()
                .map(|limit| limit as i64)
                .unwrap_or(-1),
            timeout_ms = BITSWAP_SESSION_RANGE_BATCH_TIMEOUT.as_millis()
        );

        let request_context = current_retrieval_request_context();
        let bitswap_command_context =
            BitswapCommandContext::from_retrieval_context(request_context.as_ref(), 0);
        let fetch = async {
            let client = self.shared_bitswap_client().await?;
            client
                .fetch_many_with_context(cids.clone(), peers, bitswap_command_context)
                .await
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
        let source_addr = result.source_addr.clone();
        let source_trace = self
            .bitswap_source_peer_trace(result.source_peer, source_addr.as_ref(), &peers_for_record)
            .await;
        if let Some(peer) = result.source_peer {
            self.record_successful_bitswap_peer_from_fetch_source(
                peer,
                &peers_for_record,
                source_addr.as_ref(),
                elapsed,
            )
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
            source_peer_remote_addr = %source_addr
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            bitswap_delivery = delivery,
            source_peer_trusted = source_trace.skip_want_have,
            source_peer_candidate_index = source_trace
                .candidate_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_index = source_trace
                .source_addr_index
                .map(|index| index as i64)
                .unwrap_or(-1),
            source_peer_addr_known = source_trace.source_addr_known,
            source_peer_addr_matches_candidate = source_trace.source_addr_matches_candidate,
            source_peer_addr_transport = source_trace.source_addr_transport,
            source_peer_addr_family = source_trace.source_addr_family,
            source_peer_request_mode = source_trace.request_mode,
            source_peer_force_want_block = source_trace.force_want_block,
            source_peer_force_want_have = source_trace.force_want_have,
            source_peer_addr_count = source_trace.addr_count,
            source_peer_previous_success_count = source_trace.previous_success_count,
            source_peer_previous_latency_ms = source_trace.previous_latency_ms,
            source_peer_previous_seen_age_ms = source_trace.previous_seen_age_ms,
            source_peer_previous_top_level_path = %source_trace
                .previous_top_level_path
                .as_deref()
                .unwrap_or(""),
            source_peer_same_top_level = source_trace.same_top_level,
            source_peer_cross_top_level = source_trace.cross_top_level,
            source_peer_unknown_top_level = source_trace.unknown_top_level,
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
    let zero_http_override = std::env::var_os(BITSWAP_SESSION_ZERO_HTTP_POST_LOOKUP_GRACE_MS_ENV);
    let zero_http_override = zero_http_override
        .as_ref()
        .map(|value| value.to_string_lossy());
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
        zero_http_override.as_deref(),
        single_http_override.as_deref(),
        multi_http_override.as_deref(),
    )
}

fn bitswap_session_post_lookup_grace_from_env_value(
    providers: &[Provider],
    zero_http_grace_ms: Option<&str>,
    single_http_grace_ms: Option<&str>,
    multi_http_grace_ms: Option<&str>,
) -> Duration {
    match provider_http_url_count(providers) {
        0 => {
            post_lookup_grace_from_env_value(zero_http_grace_ms, BITSWAP_SESSION_POST_LOOKUP_GRACE)
        }
        1 => post_lookup_grace_from_env_value(
            single_http_grace_ms,
            BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE,
        ),
        _ => {
            post_lookup_grace_from_env_value(multi_http_grace_ms, BITSWAP_SESSION_POST_LOOKUP_GRACE)
        }
    }
}

fn top_level_single_http_provider_win_bitswap_grace(
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
) -> Option<Duration> {
    top_level_single_http_provider_win_bitswap_grace_from_values(
        std::env::var_os(ENABLE_TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE_ENV).is_some(),
        std::env::var_os(TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE_MS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
        context.is_some_and(RetrievalRequestContext::gateway_subresource),
        http_provider_count,
    )
}

fn top_level_single_http_provider_win_bitswap_grace_from_values(
    enabled: bool,
    grace_ms: Option<&str>,
    gateway_subresource: bool,
    http_provider_count: usize,
) -> Option<Duration> {
    if !enabled || gateway_subresource || http_provider_count != 1 {
        return None;
    }
    Some(
        grace_ms
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE),
    )
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
            let context = context.clone();
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
    force_want_have: bool,
}

struct SuccessfulBitswapPeer {
    seen_at: Instant,
    addrs: Vec<Multiaddr>,
    last_latency: Duration,
    success_count: u64,
    top_level_path: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct BitswapSessionPeerQuality {
    scored_count: usize,
    success_count_min: u64,
    success_count_max: u64,
    latency_ms_min: u128,
    latency_ms_max: u128,
    seen_age_ms_min: u128,
    seen_age_ms_max: u128,
    same_top_level_count: usize,
    cross_top_level_count: usize,
    unknown_top_level_count: usize,
}

fn bitswap_session_peer_quality_from_successes(
    peers: &[BitswapPeer],
    successes: &HashMap<PeerId, SuccessfulBitswapPeer>,
    now: Instant,
    current_top_level_path: Option<&str>,
) -> BitswapSessionPeerQuality {
    let mut quality = BitswapSessionPeerQuality::default();
    for peer in peers {
        let Some(success) = successes.get(&peer.id) else {
            continue;
        };
        let latency_ms = success.last_latency.as_millis();
        let seen_age_ms = now.saturating_duration_since(success.seen_at).as_millis();
        if quality.scored_count == 0 {
            quality.success_count_min = success.success_count;
            quality.success_count_max = success.success_count;
            quality.latency_ms_min = latency_ms;
            quality.latency_ms_max = latency_ms;
            quality.seen_age_ms_min = seen_age_ms;
            quality.seen_age_ms_max = seen_age_ms;
        } else {
            quality.success_count_min = quality.success_count_min.min(success.success_count);
            quality.success_count_max = quality.success_count_max.max(success.success_count);
            quality.latency_ms_min = quality.latency_ms_min.min(latency_ms);
            quality.latency_ms_max = quality.latency_ms_max.max(latency_ms);
            quality.seen_age_ms_min = quality.seen_age_ms_min.min(seen_age_ms);
            quality.seen_age_ms_max = quality.seen_age_ms_max.max(seen_age_ms);
        }
        match (success.top_level_path.as_deref(), current_top_level_path) {
            (Some(previous), Some(current)) if previous == current => {
                quality.same_top_level_count += 1;
            }
            (Some(_), Some(_)) => {
                quality.cross_top_level_count += 1;
            }
            _ => {
                quality.unknown_top_level_count += 1;
            }
        }
        quality.scored_count += 1;
    }
    quality
}

#[derive(Debug, PartialEq, Eq)]
struct BitswapSourcePeerTrace {
    candidate_index: Option<usize>,
    source_addr_index: Option<usize>,
    source_addr_known: bool,
    source_addr_matches_candidate: bool,
    source_addr_transport: &'static str,
    source_addr_family: &'static str,
    request_mode: &'static str,
    skip_want_have: bool,
    force_want_block: bool,
    force_want_have: bool,
    addr_count: usize,
    previous_success_count: u64,
    previous_latency_ms: u128,
    previous_seen_age_ms: u128,
    previous_top_level_path: Option<String>,
    same_top_level: bool,
    cross_top_level: bool,
    unknown_top_level: bool,
}

#[derive(Clone, Copy)]
struct GatewayBitswapSourceContext {
    gateway_request: bool,
    gateway_subresource: bool,
}

struct SlowZeroHttpSourcePeer<'a> {
    gateway_request: bool,
    gateway_subresource: bool,
    http_provider_count: usize,
    source_peer: Option<PeerId>,
    source_trace: &'a BitswapSourcePeerTrace,
    elapsed: Duration,
    threshold: Duration,
    trace_phase: &'static str,
    bad_provider_reason: &'static str,
}

impl Default for BitswapSourcePeerTrace {
    fn default() -> Self {
        Self {
            candidate_index: None,
            source_addr_index: None,
            source_addr_known: false,
            source_addr_matches_candidate: false,
            source_addr_transport: "unknown",
            source_addr_family: "unknown",
            request_mode: "unknown",
            skip_want_have: false,
            force_want_block: false,
            force_want_have: false,
            addr_count: 0,
            previous_success_count: 0,
            previous_latency_ms: 0,
            previous_seen_age_ms: 0,
            previous_top_level_path: None,
            same_top_level: false,
            cross_top_level: false,
            unknown_top_level: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DominantSessionPeerMode {
    Global,
    GatewayTopLevel,
}

impl DominantSessionPeerMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::GatewayTopLevel => "gateway_top_level",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct DominantSessionPeerSelection {
    peer: PeerId,
    peer_count: usize,
    selected_peer_count: usize,
    alternate_count: usize,
    alternate_limit: usize,
    success_count: u64,
    next_success_count: u64,
    latency: Duration,
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

#[derive(Clone, Copy, Default)]
struct ProviderFetchOptions {
    single_http_5xx_fast_bitswap_fallback: bool,
    single_http_session_bitswap_hedge: bool,
    top_level_single_http_failed_direct_ip_bitswap_fallback: bool,
    top_level_multi_http_failed_direct_ip_bitswap_fallback: bool,
}

impl ProviderFetchOptions {
    fn from_env() -> Self {
        Self {
            single_http_5xx_fast_bitswap_fallback: single_http_5xx_fast_bitswap_fallback_enabled(),
            single_http_session_bitswap_hedge: single_http_session_bitswap_hedge_enabled(),
            top_level_single_http_failed_direct_ip_bitswap_fallback:
                top_level_single_http_failed_direct_ip_bitswap_fallback_enabled(),
            top_level_multi_http_failed_direct_ip_bitswap_fallback:
                top_level_multi_http_failed_direct_ip_bitswap_fallback_enabled(),
        }
    }
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

fn single_http_session_bitswap_hedge_enabled() -> bool {
    std::env::var_os(ENABLE_SINGLE_HTTP_SESSION_BITSWAP_HEDGE_ENV).is_some()
}

fn single_http_provider_bitswap_hedge_after() -> Duration {
    single_http_provider_bitswap_hedge_after_from_env_value(
        std::env::var_os(SINGLE_HTTP_BITSWAP_HEDGE_AFTER_MS_ENV)
            .as_ref()
            .and_then(|value| value.to_str()),
    )
}

fn single_http_5xx_fast_bitswap_fallback_enabled() -> bool {
    std::env::var_os(ENABLE_SINGLE_HTTP_5XX_FAST_BITSWAP_FALLBACK_ENV).is_some()
}

fn top_level_single_http_failed_direct_ip_bitswap_fallback_enabled() -> bool {
    std::env::var_os(ENABLE_TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK_ENV).is_some()
}

fn top_level_multi_http_failed_direct_ip_bitswap_fallback_enabled() -> bool {
    std::env::var_os(ENABLE_TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_FALLBACK_ENV).is_some()
}

fn top_level_single_http_failed_direct_ip_bitswap_min_providers() -> usize {
    top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(
        std::env::var_os(TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(
    value: Option<&str>,
) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS)
}

fn top_level_multi_http_failed_direct_ip_bitswap_min_providers() -> usize {
    top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(
        std::env::var_os(TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(
    value: Option<&str>,
) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS)
}

fn top_level_single_http_failed_direct_ip_bitswap_fallback_allows(
    enabled: bool,
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
    provider_count: usize,
    bitswap_provider_candidate_available: bool,
    direct_ip_bitswap_provider_candidate_available: bool,
) -> bool {
    top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
        enabled,
        context,
        http_provider_count,
        provider_count,
        bitswap_provider_candidate_available,
        direct_ip_bitswap_provider_candidate_available,
        top_level_single_http_failed_direct_ip_bitswap_min_providers(),
    )
}

fn top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
    enabled: bool,
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
    provider_count: usize,
    bitswap_provider_candidate_available: bool,
    direct_ip_bitswap_provider_candidate_available: bool,
    min_provider_count: usize,
) -> bool {
    enabled
        && context.is_some_and(|context| !context.gateway_subresource())
        && http_provider_count == 1
        && provider_count >= min_provider_count
        && bitswap_provider_candidate_available
        && direct_ip_bitswap_provider_candidate_available
}

fn top_level_multi_http_failed_direct_ip_bitswap_fallback_allows(
    enabled: bool,
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
    provider_count: usize,
    bitswap_provider_candidate_available: bool,
    direct_ip_bitswap_provider_candidate_available: bool,
) -> bool {
    top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
        enabled,
        context,
        http_provider_count,
        provider_count,
        bitswap_provider_candidate_available,
        direct_ip_bitswap_provider_candidate_available,
        top_level_multi_http_failed_direct_ip_bitswap_min_providers(),
    )
}

fn top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
    enabled: bool,
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
    provider_count: usize,
    bitswap_provider_candidate_available: bool,
    direct_ip_bitswap_provider_candidate_available: bool,
    min_provider_count: usize,
) -> bool {
    enabled
        && context.is_some_and(|context| !context.gateway_subresource())
        && http_provider_count > 1
        && provider_count >= min_provider_count
        && bitswap_provider_candidate_available
        && direct_ip_bitswap_provider_candidate_available
}

fn http_provider_server_error_status(err: &RetrievalError) -> Option<u16> {
    match err {
        RetrievalError::Http(err) => err
            .status()
            .filter(|status| status.is_server_error())
            .map(|status| status.as_u16()),
        _ => None,
    }
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

fn zero_http_post_lookup_dns_prefetch_enabled() -> bool {
    zero_http_post_lookup_dns_prefetch_enabled_from_values(
        std::env::var_os(DISABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_ENV).is_some(),
        std::env::var_os(ENABLE_ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_ENV).is_some(),
    )
}

fn zero_http_post_lookup_dns_prefetch_enabled_from_values(disabled: bool, _enabled: bool) -> bool {
    !disabled
}

fn zero_http_post_lookup_dns_prefetch_min_providers() -> usize {
    zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(
        std::env::var_os(ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS)
}

fn zero_http_post_lookup_dns_prefetch_allows_from_values(
    enabled: bool,
    context: Option<&RetrievalRequestContext>,
    http_provider_count: usize,
    provider_count: usize,
    bitswap_provider_candidate_available: bool,
    min_provider_count: usize,
) -> bool {
    enabled
        && context.is_some_and(|context| !context.gateway_subresource())
        && http_provider_count == 0
        && provider_count >= min_provider_count
        && bitswap_provider_candidate_available
}

fn bitswap_zero_http_direct_want_block_peers(
    provider_count: usize,
    context: Option<&RetrievalRequestContext>,
) -> Option<usize> {
    let post_lookup_timeout_override =
        std::env::var_os(BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS_ENV);
    let post_lookup_timeout_config = PostLookupTimeoutDirectWantBlockConfig {
        enabled: std::env::var_os(
            ENABLE_BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_ENV,
        )
        .is_some(),
        override_value: post_lookup_timeout_override
            .as_deref()
            .and_then(|value| value.to_str()),
    };
    bitswap_zero_http_direct_want_block_peers_from_values(
        SubresourceDirectWantBlockConfig {
            enabled: std::env::var_os(ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_ENV)
                .is_some(),
            override_value: std::env::var_os(
                BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS_ENV,
            )
            .as_deref()
            .and_then(|value| value.to_str()),
        },
        std::env::var_os(BITSWAP_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
        std::env::var_os(BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
        bitswap_high_provider_zero_http_direct_want_block_min_providers(),
        post_lookup_timeout_config,
        provider_count,
        context,
    )
}

fn bitswap_zero_http_direct_want_block_peers_from_env_value(value: Option<&str>) -> Option<usize> {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

#[derive(Clone, Copy)]
struct PostLookupTimeoutDirectWantBlockConfig<'a> {
    enabled: bool,
    override_value: Option<&'a str>,
}

#[cfg(test)]
impl PostLookupTimeoutDirectWantBlockConfig<'_> {
    fn disabled() -> Self {
        Self {
            enabled: false,
            override_value: None,
        }
    }

    fn enabled(override_value: Option<&str>) -> PostLookupTimeoutDirectWantBlockConfig<'_> {
        PostLookupTimeoutDirectWantBlockConfig {
            enabled: true,
            override_value,
        }
    }
}

#[derive(Clone, Copy)]
struct SubresourceDirectWantBlockConfig<'a> {
    enabled: bool,
    override_value: Option<&'a str>,
}

#[cfg(test)]
impl SubresourceDirectWantBlockConfig<'_> {
    fn disabled() -> Self {
        Self {
            enabled: false,
            override_value: None,
        }
    }

    fn enabled(override_value: Option<&str>) -> SubresourceDirectWantBlockConfig<'_> {
        SubresourceDirectWantBlockConfig {
            enabled: true,
            override_value,
        }
    }
}

fn bitswap_zero_http_direct_want_block_peers_from_values(
    subresource_config: SubresourceDirectWantBlockConfig<'_>,
    override_value: Option<&str>,
    high_provider_override_value: Option<&str>,
    high_provider_min_providers: Option<usize>,
    post_lookup_timeout_config: PostLookupTimeoutDirectWantBlockConfig<'_>,
    provider_count: usize,
    context: Option<&RetrievalRequestContext>,
) -> Option<usize> {
    bitswap_zero_http_direct_want_block_peers_from_env_value(override_value).or_else(|| {
        if high_provider_min_providers.is_some_and(|minimum| provider_count >= minimum) {
            if let Some(limit) = bitswap_zero_http_direct_want_block_peers_from_env_value(
                high_provider_override_value,
            ) {
                return Some(limit);
            }
        }
        if post_lookup_timeout_config.enabled
            && context.is_some_and(|context| {
                context.gateway_subresource() && context.zero_http_post_lookup_shortcut_timeout()
            })
        {
            return bitswap_zero_http_direct_want_block_peers_from_env_value(
                post_lookup_timeout_config.override_value,
            )
            .or(Some(
                BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS,
            ));
        }
        if !subresource_config.enabled {
            return None;
        }
        if context.is_some_and(RetrievalRequestContext::gateway_subresource) {
            return bitswap_zero_http_direct_want_block_peers_from_env_value(
                subresource_config.override_value,
            )
            .or(Some(BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS));
        }
        None
    })
}

fn bitswap_zero_http_subresource_peer_rotation_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_ZERO_HTTP_SUBRESOURCE_PEER_ROTATION_ENV).is_some()
}

fn maybe_rotate_zero_http_subresource_peers(
    providers: &[Provider],
    context: Option<&RetrievalRequestContext>,
    cid: &Cid,
    peers: &mut [BitswapPeer],
) -> Option<usize> {
    if !bitswap_zero_http_subresource_peer_rotation_enabled()
        || provider_http_url_count(providers) != 0
    {
        return None;
    }
    let context = context.filter(|context| context.gateway_subresource())?;
    let top_level_path = context.top_level_path();
    let untrusted_peer_count = peers.iter().filter(|peer| !peer.skip_want_have).count();
    if untrusted_peer_count < 2 {
        return None;
    }
    let offset = stable_zero_http_subresource_peer_rotation_offset(
        cid,
        top_level_path,
        untrusted_peer_count,
    );
    rotate_untrusted_bitswap_peer_suffix(peers, offset)
}

fn stable_zero_http_subresource_peer_rotation_offset(
    cid: &Cid,
    top_level_path: Option<&str>,
    peer_count: usize,
) -> usize {
    if peer_count == 0 {
        return 0;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in cid.to_string().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    if let Some(top_level_path) = top_level_path {
        for byte in top_level_path.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    (hash as usize) % peer_count
}

fn rotate_untrusted_bitswap_peer_suffix(peers: &mut [BitswapPeer], offset: usize) -> Option<usize> {
    let start = peers.iter().position(|peer| !peer.skip_want_have)?;
    let len = peers.len().saturating_sub(start);
    if len < 2 {
        return None;
    }
    let offset = offset % len;
    peers[start..].rotate_left(offset);
    Some(offset)
}

fn bitswap_high_provider_zero_http_direct_want_block_min_providers() -> Option<usize> {
    std::env::var_os(BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_PEERS_ENV)?;
    Some(
        std::env::var_os(BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_MIN_PROVIDERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str())
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(BITSWAP_HIGH_PROVIDER_ZERO_HTTP_DIRECT_WANT_BLOCK_MIN_PROVIDERS),
    )
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

fn zero_http_subresource_slow_source_suppression_threshold() -> Option<Duration> {
    zero_http_subresource_slow_source_suppression_threshold_from_values(
        std::env::var_os(ENABLE_ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION_ENV).is_some(),
        std::env::var_os(ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION_MS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn zero_http_subresource_slow_source_suppression_threshold_from_values(
    enabled: bool,
    override_value: Option<&str>,
) -> Option<Duration> {
    if !enabled {
        return None;
    }
    Some(
        override_value
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(Duration::from_millis)
            .unwrap_or(ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION),
    )
}

fn zero_http_gateway_slow_source_suppression_threshold() -> Option<Duration> {
    zero_http_gateway_slow_source_suppression_threshold_from_values(
        std::env::var_os(ENABLE_ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION_ENV).is_some(),
        std::env::var_os(ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION_MS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn zero_http_gateway_slow_source_suppression_threshold_from_values(
    enabled: bool,
    override_value: Option<&str>,
) -> Option<Duration> {
    if !enabled {
        return None;
    }
    Some(
        override_value
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(Duration::from_millis)
            .unwrap_or(ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION),
    )
}

fn single_http_provider_bitswap_hedge_min_score() -> Option<Duration> {
    std::env::var_os(SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS_ENV)
        .and_then(|value| value.to_string_lossy().parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn single_http_provider_bitswap_hedge_max_per_top_level() -> Option<usize> {
    single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(
        std::env::var_os(SINGLE_HTTP_BITSWAP_HEDGE_MAX_PER_TOP_LEVEL_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(
    value: Option<&str>,
) -> Option<usize> {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn single_http_provider_bitswap_hedge_after_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER)
}

fn bitswap_session_range_batch_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_SESSION_RANGE_BATCH_ENV).is_some()
}

fn bitswap_dns_expansion_cache_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_DNS_EXPANSION_CACHE_ENV).is_some()
}

fn bitswap_top_level_dns_expansion_cache_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_TOP_LEVEL_DNS_EXPANSION_CACHE_ENV).is_some()
}

fn bitswap_dns_expansion_cache_scope(
    context: Option<&RetrievalRequestContext>,
) -> Option<&'static str> {
    bitswap_dns_expansion_cache_scope_from_values(
        bitswap_dns_expansion_cache_enabled(),
        bitswap_top_level_dns_expansion_cache_enabled(),
        context,
    )
}

fn bitswap_dns_expansion_cache_scope_from_values(
    global_enabled: bool,
    top_level_enabled: bool,
    context: Option<&RetrievalRequestContext>,
) -> Option<&'static str> {
    if global_enabled {
        return Some("global");
    }
    if top_level_enabled && context.is_some_and(|context| !context.gateway_subresource()) {
        return Some("gateway_top_level");
    }
    None
}

fn bitswap_dns_lookup_timeout() -> Option<Duration> {
    bitswap_dns_lookup_timeout_from_env_value(
        std::env::var_os(BITSWAP_DNS_LOOKUP_TIMEOUT_MS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_dns_lookup_timeout_from_env_value(value: Option<&str>) -> Option<Duration> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
}

fn bitswap_dominant_session_peer_mode() -> Option<DominantSessionPeerMode> {
    dominant_session_peer_mode_for_context(
        std::env::var_os(ENABLE_BITSWAP_DOMINANT_SESSION_PEER_ENV).is_some(),
        std::env::var_os(ENABLE_BITSWAP_TOP_LEVEL_DOMINANT_SESSION_PEER_ENV).is_some(),
        current_retrieval_request_context(),
    )
}

fn dominant_session_peer_mode_for_context(
    global_enabled: bool,
    top_level_enabled: bool,
    context: Option<RetrievalRequestContext>,
) -> Option<DominantSessionPeerMode> {
    if global_enabled {
        return Some(DominantSessionPeerMode::Global);
    }
    if top_level_enabled && context.is_some_and(|context| !context.gateway_subresource()) {
        return Some(DominantSessionPeerMode::GatewayTopLevel);
    }
    None
}

fn dominant_recent_bitswap_peer_index<T>(
    peers: &[(PeerId, Instant, Duration, T, u64)],
) -> Option<usize> {
    let (dominant_index, dominant_success_count) = peers
        .iter()
        .enumerate()
        .max_by_key(|(_, peer)| peer.4)
        .map(|(index, peer)| (index, peer.4))?;
    if dominant_success_count < BITSWAP_DOMINANT_SESSION_PEER_MIN_SUCCESSES {
        return None;
    }
    let next_success_count = peers
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != dominant_index)
        .map(|(_, peer)| peer.4)
        .max()
        .unwrap_or_default();
    if next_success_count > 0
        && dominant_success_count
            < next_success_count.saturating_mul(BITSWAP_DOMINANT_SESSION_PEER_RATIO)
    {
        return None;
    }
    Some(dominant_index)
}

fn select_dominant_recent_bitswap_peers<T>(
    peers: &mut Vec<(PeerId, Instant, Duration, T, u64)>,
    alternate_limit: usize,
) -> Option<DominantSessionPeerSelection> {
    let dominant_index = dominant_recent_bitswap_peer_index(peers)?;
    let peer_count = peers.len();
    let (id, seen_at, last_latency, addrs, success_count) = peers.swap_remove(dominant_index);
    let next_success_count = peers.iter().map(|peer| peer.4).max().unwrap_or_default();
    let alternate_limit = alternate_limit.min(MAX_BITSWAP_SESSION_PEERS.saturating_sub(1));
    peers.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| right.1.cmp(&left.1)));
    peers.truncate(alternate_limit);
    let alternate_count = peers.len();
    peers.push((id, seen_at, last_latency, addrs, success_count));

    Some(DominantSessionPeerSelection {
        peer: id,
        peer_count,
        selected_peer_count: alternate_count + 1,
        alternate_count,
        alternate_limit,
        success_count,
        next_success_count,
        latency: last_latency,
    })
}

fn bitswap_dominant_session_peer_alternates() -> usize {
    bitswap_dominant_session_peer_alternates_from_env_value(
        std::env::var_os(BITSWAP_DOMINANT_SESSION_PEER_ALTERNATES_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_dominant_session_peer_alternates_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default()
        .min(MAX_BITSWAP_SESSION_PEERS.saturating_sub(1))
}

fn bitswap_top_level_scoped_session_peers_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_TOP_LEVEL_SCOPED_SESSION_PEERS_ENV).is_some()
}

fn top_level_bitswap_provider_preconnect_enabled() -> bool {
    std::env::var_os(ENABLE_TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_ENV).is_some()
}

fn top_level_bitswap_provider_preconnect_peers() -> usize {
    top_level_bitswap_provider_preconnect_peers_from_env_value(
        std::env::var_os(TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn top_level_bitswap_provider_preconnect_peers_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_BITSWAP_PEERS_PER_BLOCK))
        .unwrap_or(TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS)
}

fn top_level_bitswap_provider_preconnect_max_requests() -> usize {
    top_level_bitswap_provider_preconnect_max_requests_from_env_value(
        std::env::var_os(TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn top_level_bitswap_provider_preconnect_max_requests_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS)
}

fn bitswap_early_provider_peer_cap_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_EARLY_PROVIDER_PEER_CAP_ENV).is_some()
}

fn bitswap_provider_addr_score_order_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_PROVIDER_ADDR_SCORE_ORDER_ENV).is_some()
}

fn bitswap_direct_ip_provider_candidates_only_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_DIRECT_IP_PROVIDER_CANDIDATES_ONLY_ENV).is_some()
}

fn bitswap_direct_want_block_untrusted_peer_limit() -> usize {
    bitswap_direct_want_block_untrusted_peer_limit_from_env_value(
        std::env::var_os(BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_direct_want_block_untrusted_peer_limit_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.min(MAX_BITSWAP_PEERS_PER_BLOCK))
        .unwrap_or(MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS)
}

fn bitswap_max_dial_addrs_per_command() -> usize {
    bitswap_max_dial_addrs_per_command_from_env_value(
        std::env::var_os(BITSWAP_MAX_DIAL_ADDRS_PER_COMMAND_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_max_dial_addrs_per_command_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_BITSWAP_PEERS_PER_BLOCK * MAX_BITSWAP_ADDRS_PER_PEER))
        .unwrap_or(MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND)
}

fn successful_peer_matches_top_level_scope(
    success: &SuccessfulBitswapPeer,
    scoped_session_peers: bool,
    current_top_level_path: Option<&str>,
) -> bool {
    !scoped_session_peers
        || current_top_level_path.is_none_or(|path| success.top_level_path.as_deref() == Some(path))
}

fn bitswap_session_peer_limit() -> usize {
    bitswap_session_peer_limit_from_env_value(
        std::env::var_os(BITSWAP_SESSION_PEER_LIMIT_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_session_peer_limit_from_env_value(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(MAX_BITSWAP_SESSION_PEERS))
        .unwrap_or(MAX_BITSWAP_SESSION_PEERS)
}

fn bitswap_session_peer_min_successes() -> u64 {
    bitswap_session_peer_min_successes_from_env_value(
        std::env::var_os(BITSWAP_SESSION_PEER_MIN_SUCCESSES_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_session_peer_min_successes_from_env_value(value: Option<&str>) -> u64 {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(BITSWAP_SESSION_PEER_MIN_SUCCESSES)
}

fn bitswap_trusted_direct_want_block_peers() -> Option<usize> {
    bitswap_trusted_direct_want_block_peers_from_env_value(
        std::env::var_os(BITSWAP_TRUSTED_DIRECT_WANT_BLOCK_PEERS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_trusted_direct_want_block_peers_from_env_value(value: Option<&str>) -> Option<usize> {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .map(|value| value.min(MAX_BITSWAP_SESSION_PEERS))
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

fn has_direct_ip_bitswap_provider_candidate(providers: &[Provider]) -> bool {
    providers.iter().any(|provider| {
        provider.id.is_some()
            && provider
                .addrs
                .iter()
                .any(|addr| direct_ip_bitswap_multiaddr(addr))
    })
}

#[derive(Clone, Copy)]
enum BitswapProviderCandidateMode {
    Default,
    DirectIpOnly,
}

impl BitswapProviderCandidateMode {
    fn direct_ip_candidate_only(self) -> bool {
        matches!(self, Self::DirectIpOnly)
    }
}

enum SingleHttpProviderBitswapHedgeResult {
    HttpCandidate(HttpProviderCandidateResult),
    Bitswap(Result<Block>),
}

enum DirectIpBitswapFallbackResult {
    DirectIp(Result<Block>),
    Normal(Result<Block>),
}

enum MultiHttpFailedDirectIpBitswapFallbackResult {
    HttpCandidate(HttpProviderCandidateResult),
    DirectIp(Result<Block>),
}

enum SessionShortcutPostLookupWait {
    Hit(Block),
    Miss,
    Timeout,
}

#[derive(Clone, Copy)]
struct MultiHttpFailedDirectIpBitswapTrace {
    provider_count: usize,
    http_provider_count: usize,
    attempted_provider_count: usize,
    failed_provider_count: usize,
    started: Instant,
}

enum ZeroHttpPostLookupDnsPrefetchOutcome {
    NotAttempted,
    ContinueAfterWait { shortcut_timed_out: bool },
    Fetched(Block, RetrievalSource),
}

struct BitswapDnsExpansionCaches {
    dnsaddr_cache: DnsaddrCache,
    dns_ip_cache: DnsIpCache,
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
        timeout_ms = single_http_provider_bitswap_hedge_after().as_millis(),
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

fn push_single_http_session_bitswap_hedge(
    pending: &mut FuturesUnordered<BoxFuture<'static, SingleHttpProviderBitswapHedgeResult>>,
    retriever: HttpRetriever,
    cid: Cid,
    recent_peers: Vec<BitswapPeer>,
    started: Instant,
    reason: &'static str,
) {
    let session_peer_count = recent_peers.len();
    tracing::info!(
        phase = "http_provider_bitswap_hedge",
        cid = %cid,
        provider_count = 1usize,
        timeout_ms = single_http_provider_bitswap_hedge_after().as_millis(),
        reason,
        session_peer_only = true,
        session_peer_count,
        elapsed_ms = started.elapsed().as_millis()
    );
    pending.push(
        async move {
            let result = retriever
                .fetch_from_recent_bitswap_peers(&cid, recent_peers)
                .await
                .and_then(|block| block.ok_or(RetrievalError::NoBitswapProviders));
            SingleHttpProviderBitswapHedgeResult::Bitswap(result)
        }
        .boxed(),
    );
}

fn push_multi_http_provider_candidate(
    pending: &mut FuturesUnordered<
        BoxFuture<'static, MultiHttpFailedDirectIpBitswapFallbackResult>,
    >,
    retriever: HttpRetriever,
    cid: Cid,
    provider_index: usize,
    candidate: ScoredHttpProviderBase,
    attempt_index: usize,
) {
    pending.push(
        async move {
            MultiHttpFailedDirectIpBitswapFallbackResult::HttpCandidate(
                retriever
                    .fetch_from_http_provider_candidate_with_index(
                        cid,
                        provider_index,
                        candidate,
                        attempt_index,
                    )
                    .await,
            )
        }
        .boxed(),
    );
}

fn push_multi_http_failed_direct_ip_bitswap(
    pending: &mut FuturesUnordered<
        BoxFuture<'static, MultiHttpFailedDirectIpBitswapFallbackResult>,
    >,
    retriever: HttpRetriever,
    cid: Cid,
    providers: Vec<Provider>,
    context: Option<RetrievalRequestContext>,
    trace: MultiHttpFailedDirectIpBitswapTrace,
) {
    tracing::info!(
        phase = "top_level_multi_http_failed_direct_ip_bitswap_fallback_start",
        cid = %cid,
        provider_count = trace.provider_count,
        http_provider_count = trace.http_provider_count,
        attempted_provider_count = trace.attempted_provider_count,
        failed_provider_count = trace.failed_provider_count,
        min_provider_count = top_level_multi_http_failed_direct_ip_bitswap_min_providers(),
        elapsed_ms = trace.started.elapsed().as_millis()
    );
    pending.push(
        async move {
            MultiHttpFailedDirectIpBitswapFallbackResult::DirectIp(
                retriever
                    .fetch_from_bitswap_providers_with_candidate_mode(
                        &cid,
                        &providers,
                        context,
                        BitswapProviderCandidateMode::DirectIpOnly,
                    )
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
    candidate_index: usize,
    target_peer_count: usize,
    skip_want_have: bool,
    force_want_block: bool,
    force_want_have: bool,
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

#[derive(Clone, Copy)]
struct BitswapWantHaveProbeTrace {
    peer_candidate_index: usize,
    target_peer_count: usize,
    peer_addr_count: usize,
    peer_first_addr_transport: &'static str,
    peer_first_addr_family: &'static str,
    prefer_want_have: bool,
    skip_want_have: bool,
    force_want_block: bool,
    force_want_have: bool,
}

impl BitswapWantHaveProbeTrace {
    fn request_mode(self) -> &'static str {
        if self.prefer_want_have {
            "want_have"
        } else {
            "want_block"
        }
    }
}

struct BitswapPeerAttemptCancelTrace {
    started: Instant,
    primary_cid: Cid,
    cid_summary: String,
    cid_count: usize,
    peer_id: PeerId,
    probe: BitswapWantHaveProbeTrace,
    connection_ready_timeout: Duration,
    want_have_timeout: Duration,
    stream_read_timeout: Duration,
    stage: &'static str,
    completed: bool,
}

impl BitswapPeerAttemptCancelTrace {
    fn new(
        primary_cid: Cid,
        cid_summary: String,
        cid_count: usize,
        peer_id: PeerId,
        probe: BitswapWantHaveProbeTrace,
        request_timeouts: BitswapRequestTimeouts,
        connection_ready_timeout: Duration,
    ) -> Self {
        Self {
            started: Instant::now(),
            primary_cid,
            cid_summary,
            cid_count,
            peer_id,
            probe,
            connection_ready_timeout,
            want_have_timeout: request_timeouts.want_have,
            stream_read_timeout: request_timeouts.stream_read,
            stage: "starting",
            completed: false,
        }
    }

    fn set_stage(&mut self, stage: &'static str) {
        self.stage = stage;
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for BitswapPeerAttemptCancelTrace {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        tracing::info!(
            phase = "bitswap_peer_attempt_cancelled",
            cid = %self.primary_cid,
            cids = %self.cid_summary,
            cid_count = self.cid_count,
            peer = %self.peer_id,
            stage = self.stage,
            prefer_want_have = self.probe.prefer_want_have,
            force_want_block = self.probe.force_want_block,
            force_want_have = self.probe.force_want_have,
            probe_peer_candidate_index = self.probe.peer_candidate_index as i64,
            probe_peer_addr_count = self.probe.peer_addr_count,
            probe_peer_first_addr_transport = self.probe.peer_first_addr_transport,
            probe_peer_first_addr_family = self.probe.peer_first_addr_family,
            probe_peer_request_mode = self.probe.request_mode(),
            probe_peer_skip_want_have = self.probe.skip_want_have,
            probe_target_peer_count = self.probe.target_peer_count,
            connection_ready_timeout_ms = self.connection_ready_timeout.as_millis(),
            want_have_timeout_ms = self.want_have_timeout.as_millis(),
            stream_read_timeout_ms = self.stream_read_timeout.as_millis(),
            elapsed_ms = self.started.elapsed().as_millis()
        );
    }
}

struct BitswapOutgoingStreamRequest {
    peer_id: PeerId,
    addrs: Vec<Multiaddr>,
    prefer_want_have: bool,
    request_timeouts: BitswapRequestTimeouts,
    want_have_probe_trace: BitswapWantHaveProbeTrace,
    peer_transports: PeerTransportLog,
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
    source_addr: Option<Multiaddr>,
    delivery: &'static str,
}

#[derive(Clone, Debug)]
struct BitswapFetchBatchResult {
    requested_blocks: HashMap<Cid, Vec<u8>>,
    extra_blocks: Vec<(Cid, Vec<u8>)>,
    source_peer: Option<PeerId>,
    source_transport: Option<&'static str>,
    source_addr: Option<Multiaddr>,
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
    respond: Option<oneshot::Sender<Result<BitswapFetchBatchResult>>>,
    reason: &'static str,
    context: BitswapCommandContext,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct BitswapCommandContext {
    top_level_path: Option<String>,
    gateway_subresource: bool,
    zero_http_provider: bool,
}

impl BitswapCommandContext {
    fn from_retrieval_context(
        context: Option<&RetrievalRequestContext>,
        http_provider_count: usize,
    ) -> Self {
        Self {
            top_level_path: context
                .and_then(|context| context.top_level_path().map(ToOwned::to_owned)),
            gateway_subresource: context.is_some_and(RetrievalRequestContext::gateway_subresource),
            zero_http_provider: http_provider_count == 0,
        }
    }
}

#[derive(Clone, Debug)]
struct BitswapDialContext {
    reason: &'static str,
    command: BitswapCommandContext,
    started: Instant,
}

impl BitswapDialContext {
    fn new(reason: &'static str, command: BitswapCommandContext) -> Self {
        Self {
            reason,
            command,
            started: Instant::now(),
        }
    }
}

struct PendingIncomingBitswapResult {
    sent_at: Instant,
    sender: mpsc::UnboundedSender<BitswapFetchBatchResult>,
}

struct HeldPreconnectWaiter {
    started: Instant,
    receiver: oneshot::Receiver<()>,
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
    current_addr: Option<Multiaddr>,
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

    #[cfg(test)]
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
            source_addr: result.source_addr,
            delivery: result.delivery,
        }))
    }

    async fn fetch_with_context(
        &self,
        cid: Cid,
        peers: Vec<BitswapPeer>,
        context: BitswapCommandContext,
    ) -> Result<Result<BitswapFetchResult>> {
        let batch = self
            .fetch_many_with_context(vec![cid], peers, context)
            .await?;
        Ok(batch.map(|mut result| BitswapFetchResult {
            requested_block: result
                .requested_blocks
                .remove(&cid)
                .expect("single-CID Bitswap batch omitted requested block"),
            extra_blocks: result.extra_blocks,
            source_peer: result.source_peer,
            source_transport: result.source_transport,
            source_addr: result.source_addr,
            delivery: result.delivery,
        }))
    }

    #[cfg(test)]
    async fn fetch_many(
        &self,
        cids: Vec<Cid>,
        peers: Vec<BitswapPeer>,
    ) -> Result<Result<BitswapFetchBatchResult>> {
        self.fetch_many_with_context(cids, peers, BitswapCommandContext::default())
            .await
    }

    async fn fetch_many_with_context(
        &self,
        cids: Vec<Cid>,
        peers: Vec<BitswapPeer>,
        context: BitswapCommandContext,
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
                respond: Some(respond),
                reason: "fetch",
                context,
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

    async fn preconnect(
        &self,
        peers: Vec<BitswapPeer>,
        reason: &'static str,
        top_level_path: Option<String>,
        gateway_subresource: bool,
    ) -> Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        self.commands
            .send(BitswapCommand {
                cids: Vec::new(),
                peers,
                sent_at: Instant::now(),
                respond: None,
                reason,
                context: BitswapCommandContext {
                    top_level_path,
                    gateway_subresource,
                    zero_http_provider: false,
                },
            })
            .await
            .map_err(|_| RetrievalError::Bitswap("shared bitswap swarm stopped".into()))?;
        Ok(())
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
            peer.force_want_have,
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

fn bitswap_want_have_timeout() -> Duration {
    bitswap_want_have_timeout_from_env_value(
        std::env::var_os(BITSWAP_WANT_HAVE_TIMEOUT_MS_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn bitswap_want_have_timeout_from_env_value(value: Option<&str>) -> Duration {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or(BITSWAP_WANT_HAVE_TIMEOUT)
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
    let mut preconnect_waiters = Vec::<HeldPreconnectWaiter>::new();
    let mut connection_error_backoff = HashMap::<PeerId, ConnectionErrorBackoff>::new();
    let mut connection_dial_contexts = HashMap::<PeerId, BitswapDialContext>::new();
    let dial_errors = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let peer_transports = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command_queued_ms = command.sent_at.elapsed().as_millis();
                prune_preconnect_waiters(&mut preconnect_waiters);
                prune_connection_waiters(&mut connection_waiters, &mut connection_wait_started);
                prune_connection_error_backoff(&mut connection_error_backoff, Instant::now());
                prune_connection_dial_contexts(&mut connection_dial_contexts, Instant::now());
                if command.cids.is_empty() {
                    let reason = command.reason;
                    let command_context = command.context;
                    let dial_context = BitswapDialContext::new(reason, command_context.clone());
                    let mut peer_plans = Vec::new();
                    let mut dial_candidates = Vec::new();
                    for peer in command.peers {
                        if let Some(remaining_ms) = connection_error_backoff_remaining_ms(
                            &connection_error_backoff,
                            &peer.id,
                            Instant::now(),
                        ) {
                            tracing::info!(
                                phase = "bitswap_provider_preconnect_peer_skipped",
                                reason,
                                top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                                gateway_subresource = command_context.gateway_subresource,
                                zero_http_provider = command_context.zero_http_provider,
                                peer = %peer.id,
                                remaining_ms
                            );
                            continue;
                        }
                        tracing::debug!(peer = %peer.id, addrs = ?peer.addrs, "adding bitswap preconnect peer");
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

                    let max_dial_addr_count = bitswap_max_dial_addrs_per_command();
                    let (dial_addrs, suppressed_dial_addrs) =
                        limited_interleaved_bitswap_dials_with_limit(
                            &dial_candidates,
                            max_dial_addr_count,
                        );
                    let scheduled_dial_peers = dial_addrs
                        .iter()
                        .map(|dial| dial.peer)
                        .collect::<BTreeSet<_>>();
                    let scheduled_dial_summary = tracing::enabled!(tracing::Level::INFO)
                        .then(|| format_bitswap_dial_addrs(&dial_addrs));
                    let suppressed_dial_summary = tracing::enabled!(tracing::Level::INFO)
                        .then(|| format_bitswap_dial_addrs(&suppressed_dial_addrs));
                    let candidate_dial_peer_count = dial_candidates.len();
                    let suppressed_dial_peer_count = dial_candidates
                        .iter()
                        .filter(|peer| !scheduled_dial_peers.contains(&peer.id))
                        .count();
                    let mut connected_peer_count = 0usize;
                    let mut pending_dial_peer_count = 0usize;
                    let mut preconnect_waiter_count = 0usize;
                    let candidate_peer_count = peer_plans.len();
                    for (peer, already_connected, already_pending) in peer_plans {
                        if already_connected {
                            connected_peer_count += 1;
                        } else if already_pending {
                            pending_dial_peer_count += 1;
                        } else if scheduled_dial_peers.contains(&peer.id) {
                            let (ready, wait) = oneshot::channel();
                            connection_wait_started
                                .entry(peer.id)
                                .or_insert_with(Instant::now);
                            connection_waiters.entry(peer.id).or_default().push(ready);
                            preconnect_waiters.push(HeldPreconnectWaiter {
                                started: Instant::now(),
                                receiver: wait,
                            });
                            preconnect_waiter_count += 1;
                        }
                    }
                    tracing::info!(
                        phase = "bitswap_provider_preconnect_plan",
                        reason,
                        top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                        gateway_subresource = command_context.gateway_subresource,
                        zero_http_provider = command_context.zero_http_provider,
                        peer_count = candidate_peer_count,
                        candidate_peer_count,
                        candidate_dial_peer_count,
                        new_dial_peer_count = scheduled_dial_peers.len(),
                        new_dial_addr_count = dial_addrs.len(),
                        max_dial_addr_count,
                        suppressed_dial_addr_count = suppressed_dial_addrs.len(),
                        suppressed_dial_peer_count,
                        pending_dial_peer_count,
                        connected_peer_count,
                        preconnect_waiter_count,
                        command_queued_ms,
                        scheduled_dials = %scheduled_dial_summary.as_deref().unwrap_or(""),
                        suppressed_dials = %suppressed_dial_summary.as_deref().unwrap_or("")
                    );

                    let mut started_dial_peers = BTreeSet::new();
                    for dial in dial_addrs {
                        let peer_id = dial.peer;
                        let addr = dial.addr;
                        let transport = bitswap_transport_label(&addr);
                        let dial_addr = addr.with_p2p(peer_id).unwrap_or_else(|addr| addr);
                        match swarm.dial(dial_addr) {
                            Ok(()) => {
                                started_dial_peers.insert(peer_id);
                                connection_dial_contexts.insert(peer_id, dial_context.clone());
                            }
                            Err(err) => {
                                let error_detail = format_error_detail(&err);
                                let connection_limit = is_connection_limit_error(&error_detail);
                                record_dial_error(&dial_errors, peer_id, error_detail.clone()).await;
                                tracing::info!(
                                    phase = "bitswap_provider_preconnect_dial_rejected",
                                    reason,
                                    top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                                    gateway_subresource = command_context.gateway_subresource,
                                    zero_http_provider = command_context.zero_http_provider,
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
                            phase = "bitswap_provider_preconnect_dial_waiters_dropped",
                            reason,
                            top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                            gateway_subresource = command_context.gateway_subresource,
                            zero_http_provider = command_context.zero_http_provider,
                            peer_count = scheduled_dial_peers.len().saturating_sub(started_dial_peers.len()),
                            waiter_count = failed_dial_waiter_count
                        );
                    }
                    prune_preconnect_waiters(&mut preconnect_waiters);
                    continue;
                }
                let cids = command.cids;
                let cid_count = cids.len();
                let primary_cid = cids[0];
                let command_context = command.context;
                let dial_context =
                    BitswapDialContext::new(command.reason, command_context.clone());
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
                for (candidate_index, peer) in command.peers.into_iter().enumerate() {
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
                            top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                            gateway_subresource = command_context.gateway_subresource,
                            zero_http_provider = command_context.zero_http_provider,
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
                    peer_plans.push((candidate_index, peer, already_connected, already_pending));
                }
                let max_dial_addr_count = bitswap_max_dial_addrs_per_command();
                let (dial_addrs, suppressed_dial_addrs) =
                    limited_interleaved_bitswap_dials_with_limit(
                        &dial_candidates,
                        max_dial_addr_count,
                    );
                let scheduled_dial_peers = dial_addrs
                    .iter()
                    .map(|dial| dial.peer)
                    .collect::<BTreeSet<_>>();
                let scheduled_dial_summary = tracing::enabled!(tracing::Level::INFO)
                    .then(|| format_bitswap_dial_addrs(&dial_addrs));
                let suppressed_dial_summary = tracing::enabled!(tracing::Level::INFO)
                    .then(|| format_bitswap_dial_addrs(&suppressed_dial_addrs));
                let candidate_dial_peer_count = dial_candidates.len();
                let suppressed_dial_peer_count = dial_candidates
                    .iter()
                    .filter(|peer| !scheduled_dial_peers.contains(&peer.id))
                    .count();
                let mut peer_targets = Vec::new();
                let mut connected_peer_count = 0usize;
                let mut pending_dial_peer_count = 0usize;
                let candidate_peer_count = peer_plans.len();
                for (candidate_index, peer, already_connected, already_pending) in peer_plans {
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
                        candidate_index,
                        target_peer_count: 0,
                        skip_want_have: peer.skip_want_have,
                        force_want_block: peer.force_want_block,
                        force_want_have: peer.force_want_have,
                        connection_ready,
                    });
                }
                let target_summary =
                    tracing::enabled!(tracing::Level::INFO).then(|| format_bitswap_targets(&peer_targets));
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
                    max_dial_addr_count,
                    suppressed_dial_addr_count = suppressed_dial_addrs.len(),
                    suppressed_dial_peer_count,
                    pending_dial_peer_count,
                    connected_peer_count,
                    top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                    gateway_subresource = command_context.gateway_subresource,
                    zero_http_provider = command_context.zero_http_provider,
                    command_queued_ms,
                    direct_untrusted_want_block_limit = bitswap_direct_want_block_untrusted_peer_limit(),
                    targets = %target_summary.as_deref().unwrap_or(""),
                    scheduled_dials = %scheduled_dial_summary.as_deref().unwrap_or(""),
                    suppressed_dials = %suppressed_dial_summary.as_deref().unwrap_or("")
                );

                let mut started_dial_peers = BTreeSet::new();
                for dial in dial_addrs {
                    let peer_id = dial.peer;
                    let addr = dial.addr;
                    let transport = bitswap_transport_label(&addr);
                    let dial_addr = addr.with_p2p(peer_id).unwrap_or_else(|addr| addr);
                    match swarm.dial(dial_addr) {
                        Ok(()) => {
                            started_dial_peers.insert(peer_id);
                            connection_dial_contexts.insert(peer_id, dial_context.clone());
                        }
                        Err(err) => {
                            let error_detail = format_error_detail(&err);
                            let connection_limit = is_connection_limit_error(&error_detail);
                            record_dial_error(&dial_errors, peer_id, error_detail.clone()).await;
                            tracing::info!(
                                phase = "bitswap_dial_rejected",
                                cid = %primary_cid,
                                cids = %cid_summary.as_deref().unwrap_or(""),
                                cid_count,
                                top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                                gateway_subresource = command_context.gateway_subresource,
                                zero_http_provider = command_context.zero_http_provider,
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
                        top_level_path = %command_context.top_level_path.as_deref().unwrap_or(""),
                        gateway_subresource = command_context.gateway_subresource,
                        zero_http_provider = command_context.zero_http_provider,
                        peer_count = scheduled_dial_peers.len().saturating_sub(started_dial_peers.len()),
                        waiter_count = failed_dial_waiter_count
                    );
                }

                let control = control.clone();
                let fetch_cids = cids.clone();
                let Some(mut respond) = command.respond else {
                    tracing::info!(
                        phase = "bitswap_fetch_cancelled",
                        cid = %primary_cid,
                        cids = %format_cids(&fetch_cids),
                        cid_count = fetch_cids.len(),
                        command_queued_ms,
                        reason = "missing_responder"
                    );
                    continue;
                };
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
                        let (source_transport, source_addr) =
                            current_peer_connection(&peer_transports, incoming_read.peer).await;
                        for cid in pending_incoming.keys().copied().collect::<Vec<_>>() {
                            if let Some(mut result) = collect_bitswap_result(&cid, blocks.clone()) {
                                result.source_peer = Some(incoming_read.peer);
                                result.source_transport = source_transport;
                                result.source_addr = source_addr.clone();
                                result.delivery = "incoming";
                                let block_len = result.requested_block.len();
                                let batch_result = BitswapFetchBatchResult {
                                    requested_blocks: HashMap::from([(cid, result.requested_block)]),
                                    extra_blocks: result.extra_blocks,
                                    source_peer: result.source_peer,
                                    source_transport: result.source_transport,
                                    source_addr: result.source_addr,
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
                                    source_peer_remote_addr = %source_addr
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .unwrap_or_default(),
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
                            if bitswap_incoming_read_timeout_backoff_enabled() {
                                let backoff = record_incoming_read_timeout_backoff(
                                    &mut connection_error_backoff,
                                    incoming_read.peer,
                                    Instant::now(),
                                );
                                tracing::info!(
                                    phase = "bitswap_incoming_read_timeout_backoff",
                                    peer = %incoming_read.peer,
                                    count = backoff.count,
                                    ttl_ms = BITSWAP_CONNECTION_ERROR_BACKOFF_TTL.as_millis()
                                );
                            }
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
                        connection_dial_contexts.remove(&peer_id);
                        let wait_elapsed_ms = connection_wait_started
                            .remove(&peer_id)
                            .map(|started| started.elapsed().as_millis())
                            .unwrap_or_default();
                        let failed_dial_count =
                            concurrent_dial_errors.as_ref().map_or(0, Vec::len);
                        let remote_addr = endpoint.get_remote_address();
                        let transport = bitswap_transport_label(remote_addr);
                        record_peer_transport_established(
                            &peer_transports,
                            peer_id,
                            transport,
                            remote_addr.clone(),
                        )
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
                        record_peer_transport_closed(
                            &peer_transports,
                            peer_id,
                            transport,
                            remote_addr,
                        )
                        .await;
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
                            let dial_context = connection_dial_contexts.get(&peer_id);
                            record_dial_error(&dial_errors, peer_id, error_detail.clone()).await;
                            let backoff_threshold =
                                bitswap_connection_error_backoff_threshold_for_context(
                                    dial_context,
                                );
                            if let Some(backoff) = record_connection_error_backoff_with_threshold(
                                &mut connection_error_backoff,
                                peer_id,
                                &error_detail,
                                Instant::now(),
                                backoff_threshold,
                            ) {
                                tracing::info!(
                                    phase = "bitswap_connection_error_backoff",
                                    peer = %peer_id,
                                    reason = dial_context.map(|context| context.reason).unwrap_or(""),
                                    top_level_path = %dial_context
                                        .and_then(|context| context.command.top_level_path.as_deref())
                                        .unwrap_or(""),
                                    gateway_subresource = dial_context
                                        .is_some_and(|context| context.command.gateway_subresource),
                                    zero_http_provider = dial_context
                                        .is_some_and(|context| context.command.zero_http_provider),
                                    error_class = backoff.class,
                                    count = backoff.count,
                                    threshold = backoff_threshold,
                                    ttl_ms = BITSWAP_CONNECTION_ERROR_BACKOFF_TTL.as_millis()
                                );
                            }
                        }
                        tracing::info!(
                            phase = "bitswap_connection_error",
                            peer = peer_id.map(|peer| peer.to_string()).unwrap_or_default(),
                            reason = peer_id
                                .and_then(|peer| connection_dial_contexts.get(&peer))
                                .map(|context| context.reason)
                                .unwrap_or(""),
                            top_level_path = %peer_id
                                .and_then(|peer| connection_dial_contexts.get(&peer))
                                .and_then(|context| context.command.top_level_path.as_deref())
                                .unwrap_or(""),
                            gateway_subresource = peer_id
                                .and_then(|peer| connection_dial_contexts.get(&peer))
                                .is_some_and(|context| context.command.gateway_subresource),
                            zero_http_provider = peer_id
                                .and_then(|peer| connection_dial_contexts.get(&peer))
                                .is_some_and(|context| context.command.zero_http_provider),
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

fn prune_preconnect_waiters(waiters: &mut Vec<HeldPreconnectWaiter>) {
    let now = Instant::now();
    let timeout = bitswap_connection_ready_timeout();
    waiters.retain_mut(|waiter| match waiter.receiver.try_recv() {
        Ok(()) | Err(oneshot::error::TryRecvError::Closed) => false,
        Err(oneshot::error::TryRecvError::Empty) => now.duration_since(waiter.started) <= timeout,
    });
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

fn prune_connection_dial_contexts(
    contexts: &mut HashMap<PeerId, BitswapDialContext>,
    now: Instant,
) {
    contexts.retain(|_, context| {
        now.duration_since(context.started) <= BITSWAP_CONNECTION_ERROR_BACKOFF_TTL
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

#[cfg(test)]
fn record_connection_error_backoff<'a>(
    backoff: &'a mut HashMap<PeerId, ConnectionErrorBackoff>,
    peer: PeerId,
    detail: &str,
    now: Instant,
) -> Option<&'a ConnectionErrorBackoff> {
    record_connection_error_backoff_with_threshold(
        backoff,
        peer,
        detail,
        now,
        bitswap_connection_error_backoff_threshold(),
    )
}

fn record_connection_error_backoff_with_threshold<'a>(
    backoff: &'a mut HashMap<PeerId, ConnectionErrorBackoff>,
    peer: PeerId,
    detail: &str,
    now: Instant,
    threshold: usize,
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
    if state.count >= threshold.max(1) {
        state.suppress_until = Some(now + BITSWAP_CONNECTION_ERROR_BACKOFF_TTL);
        Some(state)
    } else {
        None
    }
}

fn record_incoming_read_timeout_backoff(
    backoff: &mut HashMap<PeerId, ConnectionErrorBackoff>,
    peer: PeerId,
    now: Instant,
) -> &ConnectionErrorBackoff {
    let class = "incoming_stream_read_timeout";
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
    state.suppress_until = Some(now + BITSWAP_CONNECTION_ERROR_BACKOFF_TTL);
    state
}

fn bitswap_incoming_read_timeout_backoff_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_INCOMING_READ_TIMEOUT_BACKOFF_ENV).is_some()
}

fn bitswap_incoming_source_addr_session_peers_enabled() -> bool {
    std::env::var_os(ENABLE_BITSWAP_INCOMING_SOURCE_ADDR_SESSION_PEERS_ENV).is_some()
}

fn bitswap_connection_error_backoff_threshold() -> usize {
    let override_value = std::env::var_os(BITSWAP_CONNECTION_ERROR_BACKOFF_THRESHOLD_ENV);
    let override_value = override_value.as_ref().map(|value| value.to_string_lossy());
    bitswap_connection_error_backoff_threshold_from_env_value(override_value.as_deref())
}

fn bitswap_connection_error_backoff_threshold_for_context(
    context: Option<&BitswapDialContext>,
) -> usize {
    bitswap_connection_error_backoff_threshold_for_context_from_values(
        bitswap_connection_error_backoff_threshold(),
        std::env::var_os(ENABLE_ZERO_HTTP_SUBRESOURCE_CONNECTION_ERROR_BACKOFF_ENV).is_some(),
        context.map(|context| &context.command),
    )
}

fn bitswap_connection_error_backoff_threshold_for_context_from_values(
    default_threshold: usize,
    zero_http_subresource_enabled: bool,
    context: Option<&BitswapCommandContext>,
) -> usize {
    if zero_http_subresource_enabled
        && context.is_some_and(|context| context.gateway_subresource && context.zero_http_provider)
    {
        1
    } else {
        default_threshold.max(1)
    }
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
    remote_addr: Multiaddr,
) {
    let mut transports = transports.lock().await;
    let state = transports.entry(peer).or_default();
    *state.counts.entry(transport).or_default() += 1;
    state.current = Some(transport);
    state.current_addr = Some(remote_addr);
}

async fn record_peer_transport_closed(
    transports: &PeerTransportLog,
    peer: PeerId,
    transport: &'static str,
    remote_addr: &Multiaddr,
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
        if state.current_addr.as_ref() == Some(remote_addr) {
            state.current_addr = None;
        }
    }
}

async fn current_peer_connection(
    transports: &PeerTransportLog,
    peer: PeerId,
) -> (Option<&'static str>, Option<Multiaddr>) {
    transports
        .lock()
        .await
        .get(&peer)
        .map(|state| (state.current, state.current_addr.clone()))
        .unwrap_or((None, None))
}

fn bitswap_session_addr_from_remote_addr(addr: &Multiaddr) -> Multiaddr {
    let mut addr = addr.clone();
    if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
        addr.pop();
    }
    addr
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
                peer.force_want_have,
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

fn bitswap_source_peer_trace_from_peers(
    source_peer: Option<PeerId>,
    source_addr: Option<&Multiaddr>,
    peers: &[BitswapPeer],
) -> BitswapSourcePeerTrace {
    let Some(source_peer) = source_peer else {
        return BitswapSourcePeerTrace::default();
    };
    let has_multiple_peers = peers.len() > 1;
    let mut direct_untrusted_want_block_count = 0usize;
    for (index, peer) in peers.iter().enumerate() {
        let prefer_want_have = bitswap_prefer_want_have(
            has_multiple_peers,
            peer.skip_want_have,
            peer.force_want_block,
            peer.force_want_have,
            &mut direct_untrusted_want_block_count,
        );
        if peer.id == source_peer {
            let source_addr_index = bitswap_source_addr_index(source_addr, peer);
            return BitswapSourcePeerTrace {
                candidate_index: Some(index),
                source_addr_index,
                source_addr_known: source_addr.is_some(),
                source_addr_matches_candidate: source_addr_index.is_some(),
                source_addr_transport: source_addr
                    .map(bitswap_transport_label)
                    .unwrap_or("unknown"),
                source_addr_family: source_addr
                    .map(bitswap_addr_family_label)
                    .unwrap_or("unknown"),
                request_mode: if prefer_want_have {
                    "want_have"
                } else {
                    "want_block"
                },
                skip_want_have: peer.skip_want_have,
                force_want_block: peer.force_want_block,
                force_want_have: peer.force_want_have,
                addr_count: peer.addrs.len(),
                ..BitswapSourcePeerTrace::default()
            };
        }
    }
    BitswapSourcePeerTrace::default()
}

fn bitswap_source_addr_index(source_addr: Option<&Multiaddr>, peer: &BitswapPeer) -> Option<usize> {
    let source_addr = source_addr?;
    let normalized_source_addr = bitswap_session_addr_from_remote_addr(source_addr);
    peer.addrs
        .iter()
        .position(|candidate| candidate == &normalized_source_addr)
}

fn format_cids(cids: &[Cid]) -> String {
    cids.iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn format_bitswap_targets(peers: &[BitswapPeerTarget]) -> String {
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
                peer.force_want_have,
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
    processed_provider_count: usize,
    skipped_provider_count: usize,
    early_peer_cap: bool,
    early_peer_cap_hit: bool,
    provider_addr_count: usize,
    expanded_addr_count: usize,
    supported_addr_count: usize,
    id_only_provider_count: usize,
    invalid_provider_id_count: usize,
    provider_without_supported_bitswap_addr_count: usize,
    direct_ip_candidate_only: bool,
    direct_ip_candidate_skipped_addr_count: usize,
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

#[derive(Clone)]
struct SharedCachedDnsaddrRecords {
    records: Vec<String>,
    seen_at: Instant,
}

#[derive(Clone)]
struct SharedCachedDnsIpRecords {
    addrs: Vec<IpAddr>,
    seen_at: Instant,
}

type SharedDnsaddrCache = HashMap<String, SharedCachedDnsaddrRecords>;
type SharedDnsIpCache = HashMap<String, SharedCachedDnsIpRecords>;

#[derive(Default)]
struct BitswapDnsExpansionCacheStats {
    dnsaddr_requested: usize,
    dnsaddr_hits: usize,
    dnsaddr_misses: usize,
    dns_ip_requested: usize,
    dns_ip_hits: usize,
    dns_ip_misses: usize,
    dnsaddr_cache_len: usize,
    dns_ip_cache_len: usize,
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
    let mut dnsaddr_cache = DnsaddrCache::new();
    let mut dns_ip_cache = DnsIpCache::new();
    bitswap_peers_with_quality_using_caches(providers, &mut dnsaddr_cache, &mut dns_ip_cache).await
}

async fn bitswap_peers_with_quality_using_caches(
    providers: &[Provider],
    dnsaddr_cache: &mut DnsaddrCache,
    dns_ip_cache: &mut DnsIpCache,
) -> BitswapProviderCandidates {
    bitswap_peers_with_quality_using_caches_with_options(
        providers,
        dnsaddr_cache,
        dns_ip_cache,
        bitswap_early_provider_peer_cap_enabled(),
        bitswap_direct_ip_provider_candidates_only_enabled(),
    )
    .await
}

async fn bitswap_peers_with_quality_using_caches_with_options(
    providers: &[Provider],
    dnsaddr_cache: &mut DnsaddrCache,
    dns_ip_cache: &mut DnsIpCache,
    early_peer_cap: bool,
    direct_ip_candidate_only: bool,
) -> BitswapProviderCandidates {
    let mut peers = Vec::new();
    let mut quality = BitswapProviderAddrQuality {
        early_peer_cap,
        direct_ip_candidate_only,
        ..BitswapProviderAddrQuality::default()
    };
    if !early_peer_cap && !direct_ip_candidate_only {
        prefetch_bitswap_dns_expansions(providers, dnsaddr_cache, dns_ip_cache).await;
    }

    for (index, provider) in providers.iter().enumerate() {
        quality.processed_provider_count += 1;
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

        let expanded_addrs = if direct_ip_candidate_only {
            let (addrs, skipped) = direct_ip_provider_multiaddrs(&provider.addrs);
            quality.direct_ip_candidate_skipped_addr_count += skipped;
            addrs
        } else {
            expand_provider_multiaddrs(&provider.addrs, dnsaddr_cache, dns_ip_cache).await
        };

        for addr in expanded_addrs {
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

        if early_peer_cap && peers.len() >= MAX_BITSWAP_PEERS_PER_BLOCK {
            quality.early_peer_cap_hit = index + 1 < providers.len();
            quality.skipped_provider_count = providers.len().saturating_sub(index + 1);
            break;
        }
    }

    peers.truncate(MAX_BITSWAP_PEERS_PER_BLOCK);
    BitswapProviderCandidates { peers, quality }
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

fn maybe_force_trusted_bitswap_want_have_probes(peers: &mut [BitswapPeer]) -> usize {
    maybe_force_trusted_bitswap_want_have_probes_with_limit(
        peers,
        bitswap_trusted_direct_want_block_peers(),
    )
}

fn maybe_force_trusted_bitswap_want_have_probes_with_limit(
    peers: &mut [BitswapPeer],
    limit: Option<usize>,
) -> usize {
    let Some(limit) = limit else {
        return 0;
    };

    let mut direct_trusted = 0usize;
    let mut probed = 0usize;
    for peer in peers.iter_mut().filter(|peer| peer.skip_want_have) {
        if direct_trusted < limit {
            direct_trusted += 1;
        } else {
            peer.force_want_have = true;
            probed += 1;
        }
    }
    probed
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
            force_want_have: false,
        });
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BitswapDialAddress {
    peer: PeerId,
    peer_index: usize,
    addr_index: usize,
    addr: Multiaddr,
}

fn interleaved_bitswap_dials(peers: &[BitswapPeer]) -> Vec<BitswapDialAddress> {
    let max_addrs = peers
        .iter()
        .map(|peer| peer.addrs.len())
        .max()
        .unwrap_or_default();
    let mut dials = Vec::new();
    for addr_index in 0..max_addrs {
        for (peer_index, peer) in peers.iter().enumerate() {
            if let Some(addr) = peer.addrs.get(addr_index) {
                dials.push(BitswapDialAddress {
                    peer: peer.id,
                    peer_index,
                    addr_index,
                    addr: addr.clone(),
                });
            }
        }
    }
    dials
}

fn limited_interleaved_bitswap_dials_with_limit(
    peers: &[BitswapPeer],
    limit: usize,
) -> (Vec<BitswapDialAddress>, Vec<BitswapDialAddress>) {
    let mut all_dials = interleaved_bitswap_dials(peers);
    let split_at = limit.min(all_dials.len());
    let suppressed_dials = all_dials.split_off(split_at);
    (all_dials, suppressed_dials)
}

fn format_bitswap_dial_addrs(dials: &[BitswapDialAddress]) -> String {
    dials
        .iter()
        .take(MAX_BITSWAP_FAILURE_DETAILS)
        .map(|dial| {
            format!(
                "peer_index={} addr_index={} peer={} transport={} family={} addr={}",
                dial.peer_index,
                dial.addr_index,
                dial.peer,
                bitswap_transport_label(&dial.addr),
                bitswap_addr_family_label(&dial.addr),
                dial.addr
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
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

fn bitswap_addr_family_label(addr: &Multiaddr) -> &'static str {
    let mut has_ip4 = false;
    let mut has_ip6 = false;
    let mut has_dns = false;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(_) => has_ip4 = true,
            Protocol::Ip6(_) => has_ip6 = true,
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                has_dns = true
            }
            _ => {}
        }
    }
    match (has_ip4, has_ip6, has_dns) {
        (true, true, _) => "mixed_ip",
        (true, false, _) => "ip4",
        (false, true, _) => "ip6",
        (false, false, true) => "dns",
        _ => "other",
    }
}

fn provider_dnsaddr_hosts(providers: &[Provider]) -> Vec<String> {
    providers
        .iter()
        .flat_map(|provider| provider.addrs.iter())
        .filter_map(|addr| dnsaddr_host(addr).map(str::to_string))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn provider_dns_ip_names(providers: &[Provider], dnsaddr_cache: &DnsaddrCache) -> Vec<String> {
    providers
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
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn prune_shared_dnsaddr_cache(cache: &mut SharedDnsaddrCache, now: Instant) {
    cache.retain(|_, entry| {
        now.saturating_duration_since(entry.seen_at) <= BITSWAP_DNS_EXPANSION_CACHE_TTL
    });
}

fn prune_shared_dns_ip_cache(cache: &mut SharedDnsIpCache, now: Instant) {
    cache.retain(|_, entry| {
        now.saturating_duration_since(entry.seen_at) <= BITSWAP_DNS_EXPANSION_CACHE_TTL
    });
}

fn prune_shared_dnsaddr_cache_len(cache: &mut SharedDnsaddrCache) {
    if cache.len() <= MAX_BITSWAP_DNS_EXPANSION_CACHE_ENTRIES {
        return;
    }
    let mut entries = cache
        .iter()
        .map(|(host, entry)| (host.clone(), entry.seen_at))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(_, seen_at)| *seen_at);
    for (host, _) in entries
        .into_iter()
        .take(cache.len() - MAX_BITSWAP_DNS_EXPANSION_CACHE_ENTRIES)
    {
        cache.remove(&host);
    }
}

fn prune_shared_dns_ip_cache_len(cache: &mut SharedDnsIpCache) {
    if cache.len() <= MAX_BITSWAP_DNS_EXPANSION_CACHE_ENTRIES {
        return;
    }
    let mut entries = cache
        .iter()
        .map(|(host, entry)| (host.clone(), entry.seen_at))
        .collect::<Vec<_>>();
    entries.sort_by_key(|(_, seen_at)| *seen_at);
    for (host, _) in entries
        .into_iter()
        .take(cache.len() - MAX_BITSWAP_DNS_EXPANSION_CACHE_ENTRIES)
    {
        cache.remove(&host);
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
    let records = match bitswap_dns_lookup_timeout() {
        Some(lookup_timeout) => match timeout(lookup_timeout, resolver.txt_lookup(&lookup)).await {
            Ok(Ok(records)) => dnsaddr_records(records),
            Ok(Err(_)) => Vec::new(),
            Err(_) => {
                tracing::info!(
                    phase = "bitswap_dns_lookup_timeout",
                    lookup_kind = "dnsaddr",
                    host = %host,
                    timeout_ms = lookup_timeout.as_millis()
                );
                Vec::new()
            }
        },
        None => match resolver.txt_lookup(&lookup).await {
            Ok(records) => dnsaddr_records(records),
            Err(_) => Vec::new(),
        },
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
    let addrs = match bitswap_dns_lookup_timeout() {
        Some(lookup_timeout) => match timeout(lookup_timeout, resolver.ip_lookup(&host)).await {
            Ok(addrs) => addrs.unwrap_or_default(),
            Err(_) => {
                tracing::info!(
                    phase = "bitswap_dns_lookup_timeout",
                    lookup_kind = "dns_ip",
                    host = %host,
                    timeout_ms = lookup_timeout.as_millis()
                );
                Vec::new()
            }
        },
        None => resolver.ip_lookup(&host).await.unwrap_or_default(),
    };
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

fn direct_ip_provider_multiaddrs(addrs: &[String]) -> (Vec<String>, usize) {
    let mut direct = Vec::new();
    let mut skipped = 0usize;
    for addr in addrs {
        if direct_ip_bitswap_multiaddr(addr) {
            direct.push(addr.clone());
        } else {
            skipped += 1;
        }
    }
    (direct, skipped)
}

fn direct_ip_bitswap_multiaddr(addr: &str) -> bool {
    let Ok(addr) = Multiaddr::from_str(addr) else {
        return false;
    };
    let mut has_ip = false;
    let mut has_dns = false;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(_) | Protocol::Ip6(_) => has_ip = true,
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                has_dns = true
            }
            _ => {}
        }
    }
    has_ip && !has_dns && unsupported_bitswap_addr_reason(&addr).is_none()
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

fn bitswap_peer_min_addr_score(peer: &BitswapPeer) -> u8 {
    peer.addrs.iter().map(bitswap_addr_score).min().unwrap_or(9)
}

fn sort_bitswap_peers_by_addr_score(peers: &mut [BitswapPeer]) {
    peers.sort_by(|left, right| {
        bitswap_peer_min_addr_score(left)
            .cmp(&bitswap_peer_min_addr_score(right))
            .then_with(|| right.addrs.len().cmp(&left.addrs.len()))
    });
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
    let mut source_addr = None;

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
            source_addr = result.source_addr;
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
            source_peer_remote_addr = %source_addr
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
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
        source_addr,
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
        want_have: bitswap_want_have_timeout(),
        stream_read: stream_read_timeout,
    };
    let target_summary = format_bitswap_targets(&peers);
    let target_peer_count = peers.len();
    let mut direct_untrusted_want_block_count = 0usize;
    for mut peer in peers {
        peer.target_peer_count = target_peer_count;
        let prefer_want_have = bitswap_prefer_want_have(
            has_multiple_peers,
            peer.skip_want_have,
            peer.force_want_block,
            peer.force_want_have,
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
    force_want_have: bool,
    direct_untrusted_want_block_count: &mut usize,
) -> bool {
    bitswap_prefer_want_have_with_direct_limit(
        has_multiple_peers,
        skip_want_have,
        force_want_block,
        force_want_have,
        direct_untrusted_want_block_count,
        bitswap_direct_want_block_untrusted_peer_limit(),
    )
}

fn bitswap_prefer_want_have_with_direct_limit(
    has_multiple_peers: bool,
    skip_want_have: bool,
    force_want_block: bool,
    force_want_have: bool,
    direct_untrusted_want_block_count: &mut usize,
    direct_untrusted_want_block_limit: usize,
) -> bool {
    if force_want_block {
        if !skip_want_have {
            *direct_untrusted_want_block_count += 1;
        }
        return false;
    }
    if force_want_have {
        return true;
    }
    if !skip_want_have && *direct_untrusted_want_block_count < direct_untrusted_want_block_limit {
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
        candidate_index,
        target_peer_count,
        connection_ready,
        skip_want_have,
        force_want_block,
        force_want_have,
    } = peer;

    let attempt_started = Instant::now();
    let primary_cid = cids[0];
    let cid_count = cids.len();
    let cid_summary = tracing::enabled!(tracing::Level::INFO).then(|| format_cids(&cids));
    let connection_ready_timeout = bitswap_connection_ready_timeout();
    let want_have_probe_trace = BitswapWantHaveProbeTrace {
        peer_candidate_index: candidate_index,
        target_peer_count,
        peer_addr_count: addrs.len(),
        peer_first_addr_transport: addrs.first().map(bitswap_transport_label).unwrap_or("none"),
        peer_first_addr_family: addrs
            .first()
            .map(bitswap_addr_family_label)
            .unwrap_or("none"),
        prefer_want_have,
        skip_want_have,
        force_want_block,
        force_want_have,
    };
    tracing::info!(
        phase = "bitswap_peer_attempt_start",
        cid = %primary_cid,
        cids = %cid_summary.as_deref().unwrap_or(""),
        cid_count,
        peer = %peer_id,
        prefer_want_have,
        force_want_block,
        force_want_have,
        connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
        want_have_timeout_ms = request_timeouts.want_have.as_millis(),
        stream_read_timeout_ms = request_timeouts.stream_read.as_millis()
    );

    let mut cancel_trace = BitswapPeerAttemptCancelTrace::new(
        primary_cid,
        cid_summary.clone().unwrap_or_else(|| format_cids(&cids)),
        cid_count,
        peer_id,
        want_have_probe_trace,
        request_timeouts,
        connection_ready_timeout,
    );

    if let Some(connection_ready) = connection_ready {
        cancel_trace.set_stage("waiting_connection");
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
                    force_want_have,
                    connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
                    want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                    stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                let failure = BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::Other,
                    detail: format!(
                        "{peer_id}: bitswap connection waiter was dropped before connection"
                    ),
                };
                cancel_trace.complete();
                return Err(failure);
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
                    force_want_have,
                    connection_ready_timeout_ms = connection_ready_timeout.as_millis(),
                    want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                    stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                    elapsed_ms = attempt_started.elapsed().as_millis()
                );
                let failure = BitswapPeerFailure {
                    id: peer_id,
                    kind: BitswapPeerFailureKind::ConnectionTimeout,
                    detail: format!(
                        "{}: bitswap connection was not established within {}ms; addrs={}; recent_dial_errors={}",
                        peer_id,
                        connection_ready_timeout.as_millis(),
                        format_multiaddrs(&addrs),
                        recent_dial_errors
                    ),
                };
                cancel_trace.complete();
                return Err(failure);
            }
        }
    }
    cancel_trace.set_stage("requesting_blocks");
    let result = request_bitswap_blocks(
        control,
        cids,
        BitswapOutgoingStreamRequest {
            peer_id,
            addrs,
            prefer_want_have,
            request_timeouts,
            want_have_probe_trace,
            peer_transports,
        },
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
                force_want_have,
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
                force_want_have,
                want_have_timeout_ms = request_timeouts.want_have.as_millis(),
                stream_read_timeout_ms = request_timeouts.stream_read.as_millis(),
                error = %err.detail,
                elapsed_ms = attempt_started.elapsed().as_millis()
            );
        }
    }
    cancel_trace.complete();
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
    cids: Vec<Cid>,
    request: BitswapOutgoingStreamRequest,
) -> std::result::Result<BitswapFetchBatchResult, BitswapPeerFailure> {
    let BitswapOutgoingStreamRequest {
        peer_id,
        addrs,
        prefer_want_have,
        request_timeouts,
        want_have_probe_trace,
        peer_transports,
    } = request;
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
                peer_id,
                &protocol_name,
                request_timeouts,
                want_have_probe_trace,
            )
            .await
            {
                Ok(result) => {
                    let (source_transport, source_addr) =
                        current_peer_connection(&peer_transports, peer_id).await;
                    return Ok(BitswapFetchBatchResult {
                        requested_blocks: HashMap::from([(primary_cid, result.requested_block)]),
                        extra_blocks: result.extra_blocks,
                        source_peer: Some(peer_id),
                        source_transport,
                        source_addr,
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
                let (source_transport, source_addr) =
                    current_peer_connection(&peer_transports, peer_id).await;
                return Ok(BitswapFetchBatchResult {
                    requested_blocks: result.requested_blocks,
                    extra_blocks: result.extra_blocks,
                    source_peer: Some(peer_id),
                    source_transport,
                    source_addr,
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
    peer_id: PeerId,
    protocol_name: &str,
    request_timeouts: BitswapRequestTimeouts,
    probe_trace: BitswapWantHaveProbeTrace,
) -> std::result::Result<BitswapFetchResult, WantHaveFailure>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let started = Instant::now();
    if let Err(err) = write_bitswap_want_have(stream, cid).await {
        tracing::info!(
            phase = "bitswap_want_have_probe",
            cid = %cid,
            peer = %peer_id,
            probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
            probe_peer_addr_count = probe_trace.peer_addr_count,
            probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
            probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
            probe_peer_request_mode = probe_trace.request_mode(),
            probe_peer_skip_want_have = probe_trace.skip_want_have,
            probe_peer_force_want_block = probe_trace.force_want_block,
            probe_peer_force_want_have = probe_trace.force_want_have,
            probe_target_peer_count = probe_trace.target_peer_count,
            protocol = protocol_name,
            ok = false,
            outcome = "write_failed",
            error = %err,
            elapsed_ms = started.elapsed().as_millis()
        );
        return Err(WantHaveFailure::TryOtherProtocols(
            BitswapProtocolFailure::other(format!(
                "{protocol_name}: write want-have failed: {err}"
            )),
        ));
    }
    let response = match timeout(request_timeouts.want_have, read_bitswap_response(stream)).await {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            tracing::info!(
                phase = "bitswap_want_have_probe",
                cid = %cid,
                peer = %peer_id,
                probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
                probe_peer_addr_count = probe_trace.peer_addr_count,
                probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
                probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
                probe_peer_request_mode = probe_trace.request_mode(),
                probe_peer_skip_want_have = probe_trace.skip_want_have,
                probe_peer_force_want_block = probe_trace.force_want_block,
                probe_peer_force_want_have = probe_trace.force_want_have,
                probe_target_peer_count = probe_trace.target_peer_count,
                protocol = protocol_name,
                ok = false,
                outcome = "read_failed",
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            );
            return Err(WantHaveFailure::TryOtherProtocols(
                BitswapProtocolFailure::other(format!(
                    "{protocol_name}: read want-have failed: {err}"
                )),
            ));
        }
        Err(_) => {
            tracing::info!(
                phase = "bitswap_want_have_probe",
                cid = %cid,
                peer = %peer_id,
                probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
                probe_peer_addr_count = probe_trace.peer_addr_count,
                probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
                probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
                probe_peer_request_mode = probe_trace.request_mode(),
                probe_peer_skip_want_have = probe_trace.skip_want_have,
                probe_peer_force_want_block = probe_trace.force_want_block,
                probe_peer_force_want_have = probe_trace.force_want_have,
                probe_target_peer_count = probe_trace.target_peer_count,
                protocol = protocol_name,
                ok = false,
                outcome = "timeout_fallback_want_block",
                timeout_ms = request_timeouts.want_have.as_millis(),
                elapsed_ms = started.elapsed().as_millis()
            );
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
    let presence_count = response.block_presences.len();
    if let Some(result) = collect_bitswap_result(cid, response.blocks) {
        tracing::info!(
            phase = "bitswap_want_have_probe",
            cid = %cid,
            peer = %peer_id,
            probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
            probe_peer_addr_count = probe_trace.peer_addr_count,
            probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
            probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
            probe_peer_request_mode = probe_trace.request_mode(),
            probe_peer_skip_want_have = probe_trace.skip_want_have,
            probe_peer_force_want_block = probe_trace.force_want_block,
            probe_peer_force_want_have = probe_trace.force_want_have,
            probe_target_peer_count = probe_trace.target_peer_count,
            protocol = protocol_name,
            ok = true,
            outcome = "block",
            has_have,
            has_dont_have,
            presence_count,
            extra_blocks = result.extra_blocks.len(),
            bytes = result.requested_block.len(),
            elapsed_ms = started.elapsed().as_millis()
        );
        let _ = write_bitswap_cancel(stream, cid).await;
        return Ok(result);
    }
    if has_dont_have {
        tracing::info!(
            phase = "bitswap_want_have_probe",
            cid = %cid,
            peer = %peer_id,
            probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
            probe_peer_addr_count = probe_trace.peer_addr_count,
            probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
            probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
            probe_peer_request_mode = probe_trace.request_mode(),
            probe_peer_skip_want_have = probe_trace.skip_want_have,
            probe_peer_force_want_block = probe_trace.force_want_block,
            probe_peer_force_want_have = probe_trace.force_want_have,
            probe_target_peer_count = probe_trace.target_peer_count,
            protocol = protocol_name,
            ok = false,
            outcome = "dont_have",
            has_have,
            has_dont_have,
            presence_count,
            elapsed_ms = started.elapsed().as_millis()
        );
        return Err(WantHaveFailure::PeerDoesNotHave(
            BitswapProtocolFailure::other(format!("{protocol_name}: peer returned DONT_HAVE")),
        ));
    }
    tracing::info!(
        phase = "bitswap_want_have_probe",
        cid = %cid,
        peer = %peer_id,
        probe_peer_candidate_index = probe_trace.peer_candidate_index as i64,
        probe_peer_addr_count = probe_trace.peer_addr_count,
        probe_peer_first_addr_transport = probe_trace.peer_first_addr_transport,
        probe_peer_first_addr_family = probe_trace.peer_first_addr_family,
        probe_peer_request_mode = probe_trace.request_mode(),
        probe_peer_skip_want_have = probe_trace.skip_want_have,
        probe_peer_force_want_block = probe_trace.force_want_block,
        probe_peer_force_want_have = probe_trace.force_want_have,
        probe_target_peer_count = probe_trace.target_peer_count,
        protocol = protocol_name,
        ok = true,
        outcome = if has_have {
            "have_then_want_block"
        } else {
            "no_presence_fallback_want_block"
        },
        has_have,
        has_dont_have,
        presence_count,
        elapsed_ms = started.elapsed().as_millis()
    );
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
        source_addr: None,
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
        source_addr: None,
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

    #[tokio::test]
    async fn shared_dns_expansion_cache_seeds_records_and_prunes_stale_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let now = Instant::now();
        let stale = now
            .checked_sub(BITSWAP_DNS_EXPANSION_CACHE_TTL + Duration::from_secs(1))
            .unwrap();
        {
            let mut shared = retriever.bitswap_dnsaddr_cache.lock().await;
            shared.insert(
                "bootstrap.example".to_string(),
                SharedCachedDnsaddrRecords {
                    records: vec![
                        "/dns4/inner.example/tcp/4001".to_string(),
                        "/dns4/ws.example/tcp/443/wss".to_string(),
                    ],
                    seen_at: now,
                },
            );
            shared.insert(
                "stale-bootstrap.example".to_string(),
                SharedCachedDnsaddrRecords {
                    records: vec!["/dns4/stale.example/tcp/4001".to_string()],
                    seen_at: stale,
                },
            );
        }
        {
            let mut shared = retriever.bitswap_dns_ip_cache.lock().await;
            shared.insert(
                "inner.example".to_string(),
                SharedCachedDnsIpRecords {
                    addrs: vec!["203.0.113.10".parse().unwrap()],
                    seen_at: now,
                },
            );
            shared.insert(
                "direct.example".to_string(),
                SharedCachedDnsIpRecords {
                    addrs: vec!["203.0.113.20".parse().unwrap()],
                    seen_at: now,
                },
            );
            shared.insert(
                "stale.example".to_string(),
                SharedCachedDnsIpRecords {
                    addrs: vec!["203.0.113.30".parse().unwrap()],
                    seen_at: stale,
                },
            );
        }

        let providers = vec![Provider {
            id: None,
            addrs: vec![
                "/dnsaddr/bootstrap.example".to_string(),
                "/dns4/direct.example/tcp/4001".to_string(),
            ],
            http_urls: Vec::new(),
        }];
        let mut dnsaddr_cache = DnsaddrCache::new();
        let mut dns_ip_cache = DnsIpCache::new();
        let stats = retriever
            .seed_bitswap_dns_expansion_caches(&providers, &mut dnsaddr_cache, &mut dns_ip_cache)
            .await;

        assert_eq!(stats.dnsaddr_requested, 1);
        assert_eq!(stats.dnsaddr_hits, 1);
        assert_eq!(stats.dnsaddr_misses, 0);
        assert_eq!(stats.dns_ip_requested, 2);
        assert_eq!(stats.dns_ip_hits, 2);
        assert_eq!(stats.dns_ip_misses, 0);
        assert!(
            dnsaddr_cache
                .get("bootstrap.example")
                .unwrap()
                .log_as_cached
        );
        assert!(dns_ip_cache.get("inner.example").unwrap().log_as_cached);
        assert!(dns_ip_cache.get("direct.example").unwrap().log_as_cached);
        assert!(!dns_ip_cache.contains_key("ws.example"));
        assert!(!retriever
            .bitswap_dnsaddr_cache
            .lock()
            .await
            .contains_key("stale-bootstrap.example"));
        assert!(!retriever
            .bitswap_dns_ip_cache
            .lock()
            .await
            .contains_key("stale.example"));

        dnsaddr_cache.insert(
            "new-bootstrap.example".to_string(),
            CachedDnsaddrRecords {
                records: vec!["/dns4/new.example/tcp/4001".to_string()],
                log_as_cached: false,
            },
        );
        dns_ip_cache.insert(
            "new.example".to_string(),
            CachedDnsIpRecords {
                addrs: vec!["203.0.113.40".parse().unwrap()],
                log_as_cached: false,
            },
        );
        retriever
            .record_bitswap_dns_expansion_caches(&dnsaddr_cache, &dns_ip_cache)
            .await;

        assert!(retriever
            .bitswap_dnsaddr_cache
            .lock()
            .await
            .contains_key("new-bootstrap.example"));
        assert!(retriever
            .bitswap_dns_ip_cache
            .lock()
            .await
            .contains_key("new.example"));
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
        assert_eq!(quality.processed_provider_count, providers.len());
        assert_eq!(quality.skipped_provider_count, 0);
        assert!(!quality.early_peer_cap);
        assert!(!quality.early_peer_cap_hit);
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

    #[test]
    fn provider_addr_score_order_prefers_direct_dial_addresses() {
        let dns_peer = PeerId::random();
        let tcp_peer = PeerId::random();
        let quic_peer = PeerId::random();
        let ws_peer = PeerId::random();
        let mut peers = vec![
            BitswapPeer {
                id: ws_peer,
                addrs: vec!["/ip4/127.0.0.4/tcp/4001/ws".parse().unwrap()],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: dns_peer,
                addrs: vec!["/dns4/provider.example/tcp/4001".parse().unwrap()],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: quic_peer,
                addrs: vec!["/ip4/127.0.0.3/udp/4001/quic-v1".parse().unwrap()],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: tcp_peer,
                addrs: vec!["/ip4/127.0.0.2/tcp/4001".parse().unwrap()],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
        ];

        sort_bitswap_peers_by_addr_score(&mut peers);

        assert_eq!(peers[0].id, tcp_peer);
        assert_eq!(peers[1].id, quic_peer);
        assert_eq!(peers[2].id, dns_peer);
        assert_eq!(peers[3].id, ws_peer);
    }

    #[tokio::test]
    async fn early_provider_peer_cap_stops_after_enough_bitswap_peers() {
        let providers = (0..(MAX_BITSWAP_PEERS_PER_BLOCK + 5))
            .map(|index| {
                let peer = libp2p::identity::Keypair::generate_ed25519()
                    .public()
                    .to_peer_id();
                Provider::from_parts(
                    Some(peer.to_string()),
                    vec![format!("/ip4/127.0.0.{}/tcp/4001", index + 1)],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut dnsaddr_cache = DnsaddrCache::new();
        let mut dns_ip_cache = DnsIpCache::new();

        let candidates = bitswap_peers_with_quality_using_caches_with_options(
            &providers,
            &mut dnsaddr_cache,
            &mut dns_ip_cache,
            true,
            false,
        )
        .await;

        assert_eq!(candidates.peers.len(), MAX_BITSWAP_PEERS_PER_BLOCK);
        assert!(candidates.quality.early_peer_cap);
        assert!(candidates.quality.early_peer_cap_hit);
        assert_eq!(
            candidates.quality.processed_provider_count,
            MAX_BITSWAP_PEERS_PER_BLOCK
        );
        assert_eq!(candidates.quality.skipped_provider_count, 5);
    }

    #[test]
    fn direct_ip_bitswap_multiaddr_filters_dns_and_unsupported_addresses() {
        assert!(direct_ip_bitswap_multiaddr("/ip4/127.0.0.1/tcp/4001"));
        assert!(direct_ip_bitswap_multiaddr(
            "/ip6/2001:db8::1/udp/4001/quic-v1"
        ));
        assert!(!direct_ip_bitswap_multiaddr(
            "/dns4/provider.example/tcp/4001"
        ));
        assert!(!direct_ip_bitswap_multiaddr(
            "/ip4/127.0.0.1/tcp/4001/p2p-circuit"
        ));
        assert!(!direct_ip_bitswap_multiaddr(
            "/ip4/127.0.0.1/udp/4001/webrtc-direct"
        ));
        assert!(!direct_ip_bitswap_multiaddr(
            "/ip4/127.0.0.1/udp/4001/quic-v1/webtransport/p2p/12D3KooWJdw4Tux8MAkbsVY6nLNtLnM25jS2EGNC7AFiJRQ6H7Cu"
        ));
    }

    #[tokio::test]
    async fn direct_ip_candidate_mode_skips_dns_but_keeps_late_ip_peers() {
        let first_peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let late_peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let providers = vec![
            Provider::from_parts(
                Some(first_peer.to_string()),
                vec!["/dns4/provider.example/tcp/4001".to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(late_peer.to_string()),
                vec!["/ip4/127.0.0.2/tcp/4001".to_string()],
            )
            .unwrap(),
        ];
        let mut dnsaddr_cache = DnsaddrCache::new();
        let mut dns_ip_cache = DnsIpCache::new();

        let candidates = bitswap_peers_with_quality_using_caches_with_options(
            &providers,
            &mut dnsaddr_cache,
            &mut dns_ip_cache,
            false,
            true,
        )
        .await;

        assert_eq!(candidates.peers.len(), 1);
        assert_eq!(candidates.peers[0].id, late_peer);
        assert!(candidates.quality.direct_ip_candidate_only);
        assert_eq!(candidates.quality.processed_provider_count, 2);
        assert_eq!(candidates.quality.direct_ip_candidate_skipped_addr_count, 1);
        assert!(dnsaddr_cache.is_empty());
        assert!(dns_ip_cache.is_empty());
    }

    #[test]
    fn top_level_single_http_failed_direct_ip_fallback_gate_is_narrow() {
        let top_level = RetrievalRequestContext::gateway_request_with_top_level(
            None,
            Some("/ipfs/example".to_string()),
        );
        let subresource = RetrievalRequestContext::gateway_request_with_top_level(
            Some(1),
            Some("/ipfs/example".to_string()),
        );

        assert!(
            top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                1,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                false,
                Some(&top_level),
                1,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true, None, 1, 4, true, true, 4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&subresource),
                1,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                2,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                1,
                3,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                1,
                4,
                false,
                true,
                4,
            )
        );
        assert!(
            !top_level_single_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                1,
                4,
                true,
                false,
                4,
            )
        );
    }

    #[test]
    fn top_level_single_http_failed_direct_ip_min_provider_override_is_validated() {
        assert_eq!(
            top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(None),
            TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
        assert_eq!(
            top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some("8")),
            8
        );
        assert_eq!(
            top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some("0")),
            TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
        assert_eq!(
            top_level_single_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some(
                "bad"
            )),
            TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
    }

    #[test]
    fn top_level_multi_http_failed_direct_ip_fallback_gate_is_narrow() {
        let top_level = RetrievalRequestContext::gateway_request_with_top_level(
            None,
            Some("/ipfs/example".to_string()),
        );
        let subresource = RetrievalRequestContext::gateway_request_with_top_level(
            Some(1),
            Some("/ipfs/example".to_string()),
        );

        assert!(
            top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                2,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                false,
                Some(&top_level),
                2,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true, None, 2, 4, true, true, 4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&subresource),
                2,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                1,
                4,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                2,
                3,
                true,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                2,
                4,
                false,
                true,
                4,
            )
        );
        assert!(
            !top_level_multi_http_failed_direct_ip_bitswap_fallback_allows_from_values(
                true,
                Some(&top_level),
                2,
                4,
                true,
                false,
                4,
            )
        );
    }

    #[test]
    fn top_level_multi_http_failed_direct_ip_min_provider_override_is_validated() {
        assert_eq!(
            top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(None),
            TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
        assert_eq!(
            top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some("8")),
            8
        );
        assert_eq!(
            top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some("0")),
            TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
        assert_eq!(
            top_level_multi_http_failed_direct_ip_bitswap_min_providers_from_env_value(Some("bad")),
            TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS
        );
    }

    #[test]
    fn zero_http_post_lookup_dns_prefetch_gate_is_narrow() {
        let top_level = RetrievalRequestContext::gateway_request_with_top_level(
            None,
            Some("/ipfs/example".to_string()),
        );
        let subresource = RetrievalRequestContext::gateway_request_with_top_level(
            Some(1),
            Some("/ipfs/example".to_string()),
        );

        assert!(zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            Some(&top_level),
            0,
            4,
            true,
            4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            false,
            Some(&top_level),
            0,
            4,
            true,
            4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            true, None, 0, 4, true, 4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            Some(&subresource),
            0,
            4,
            true,
            4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            Some(&top_level),
            1,
            4,
            true,
            4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            Some(&top_level),
            0,
            3,
            true,
            4,
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_allows_from_values(
            true,
            Some(&top_level),
            0,
            4,
            false,
            4,
        ));
    }

    #[test]
    fn zero_http_post_lookup_dns_prefetch_min_provider_override_is_validated() {
        assert_eq!(
            zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(None),
            ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS
        );
        assert_eq!(
            zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(Some("8")),
            8
        );
        assert_eq!(
            zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(Some("0")),
            ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS
        );
        assert_eq!(
            zero_http_post_lookup_dns_prefetch_min_providers_from_env_value(Some("bad")),
            ZERO_HTTP_POST_LOOKUP_DNS_PREFETCH_MIN_PROVIDERS
        );
    }

    #[test]
    fn zero_http_post_lookup_dns_prefetch_default_is_enabled_with_rollback() {
        assert!(zero_http_post_lookup_dns_prefetch_enabled_from_values(
            false, false
        ));
        assert!(zero_http_post_lookup_dns_prefetch_enabled_from_values(
            false, true
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_enabled_from_values(
            true, false
        ));
        assert!(!zero_http_post_lookup_dns_prefetch_enabled_from_values(
            true, true
        ));
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
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: vec![
                    "/ip4/127.0.0.1/tcp/2001".parse().unwrap(),
                    "/ip4/127.0.0.1/tcp/2002".parse().unwrap(),
                ],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
        ];

        let dials = interleaved_bitswap_dials(&peers);
        let peer_order = dials.iter().map(|dial| dial.peer).collect::<Vec<_>>();
        let addr_order = dials
            .iter()
            .map(|dial| dial.addr.to_string())
            .collect::<Vec<_>>();
        let indexes = dials
            .iter()
            .map(|dial| (dial.peer_index, dial.addr_index))
            .collect::<Vec<_>>();

        assert_eq!(peer_order, vec![first, second, first, second]);
        assert_eq!(indexes, vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
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
                force_want_have: false,
            })
            .collect::<Vec<_>>();

        let (dials, suppressed) = limited_interleaved_bitswap_dials_with_limit(
            &peers,
            MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND,
        );
        let addr_order = dials
            .iter()
            .map(|dial| dial.addr.to_string())
            .collect::<Vec<_>>();
        let indexes = dials
            .iter()
            .map(|dial| (dial.peer_index, dial.addr_index))
            .collect::<Vec<_>>();

        assert_eq!(dials.len(), MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND);
        assert_eq!(suppressed.len(), 11);
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
        assert_eq!(indexes, vec![(0, 0), (1, 0), (2, 0), (3, 0), (0, 1)]);
        assert_eq!(suppressed[0].peer_index, 1);
        assert_eq!(suppressed[0].addr_index, 1);
    }

    #[test]
    fn wider_bitswap_dial_address_limit_keeps_interleaved_order() {
        let peer_ids = [
            "12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3",
            "12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12",
            "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP",
        ];
        let peers = peer_ids
            .iter()
            .enumerate()
            .map(|(index, peer)| BitswapPeer {
                id: parse_peer_id(peer).unwrap(),
                addrs: (1..=3)
                    .map(|rank| {
                        format!("/ip4/127.0.0.{}/tcp/{}", index + 1, 1000 + rank)
                            .parse()
                            .unwrap()
                    })
                    .collect(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            })
            .collect::<Vec<_>>();

        let (dials, suppressed) = limited_interleaved_bitswap_dials_with_limit(&peers, 6);
        let addr_order = dials
            .iter()
            .map(|dial| dial.addr.to_string())
            .collect::<Vec<_>>();
        let indexes = dials
            .iter()
            .map(|dial| (dial.peer_index, dial.addr_index))
            .collect::<Vec<_>>();

        assert_eq!(dials.len(), 6);
        assert_eq!(suppressed.len(), 3);
        assert_eq!(
            addr_order,
            vec![
                "/ip4/127.0.0.1/tcp/1001",
                "/ip4/127.0.0.2/tcp/1001",
                "/ip4/127.0.0.3/tcp/1001",
                "/ip4/127.0.0.1/tcp/1002",
                "/ip4/127.0.0.2/tcp/1002",
                "/ip4/127.0.0.3/tcp/1002",
            ]
        );
        assert_eq!(
            indexes,
            vec![(0, 0), (1, 0), (2, 0), (0, 1), (1, 1), (2, 1)]
        );
    }

    #[test]
    fn bitswap_max_dial_address_limit_env_value_is_bounded() {
        assert_eq!(
            bitswap_max_dial_addrs_per_command_from_env_value(None),
            MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND
        );
        assert_eq!(
            bitswap_max_dial_addrs_per_command_from_env_value(Some("0")),
            MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND
        );
        assert_eq!(
            bitswap_max_dial_addrs_per_command_from_env_value(Some("8")),
            8
        );
        assert_eq!(
            bitswap_max_dial_addrs_per_command_from_env_value(Some("999")),
            MAX_BITSWAP_PEERS_PER_BLOCK * MAX_BITSWAP_ADDRS_PER_PEER
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
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: third,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: fourth,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: fifth,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
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

    #[test]
    fn traces_bitswap_source_peer_candidate_mode() {
        let first = parse_peer_id("12D3KooWLSFr3c4K1dxWavx5XFsUjeSXap3VPMuEbe28zeL5B1v3").unwrap();
        let second = parse_peer_id("12D3KooWGU3fJrHaWtRSWyrrzCpdgFX5bxbS69hqL1MSdKMGez12").unwrap();
        let third = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let fourth = parse_peer_id("12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH").unwrap();
        let first_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/1001").unwrap();
        let first_alt_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/1002").unwrap();
        let fourth_addr = Multiaddr::from_str("/ip6/::1/tcp/4001").unwrap();
        let unknown_addr = Multiaddr::from_str("/ip4/127.0.0.9/tcp/9999").unwrap();
        let peers = vec![
            BitswapPeer {
                id: first,
                addrs: vec![first_addr, first_alt_addr.clone()],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: third,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: fourth,
                addrs: vec![fourth_addr],
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
        ];

        let first_trace =
            bitswap_source_peer_trace_from_peers(Some(first), Some(&first_alt_addr), &peers);
        assert_eq!(first_trace.candidate_index, Some(0));
        assert_eq!(first_trace.source_addr_index, Some(1));
        assert!(first_trace.source_addr_known);
        assert!(first_trace.source_addr_matches_candidate);
        assert_eq!(first_trace.source_addr_transport, "tcp");
        assert_eq!(first_trace.source_addr_family, "ip4");
        assert_eq!(first_trace.request_mode, "want_block");

        let fourth_trace =
            bitswap_source_peer_trace_from_peers(Some(fourth), Some(&unknown_addr), &peers);
        assert_eq!(fourth_trace.candidate_index, Some(3));
        assert_eq!(fourth_trace.source_addr_index, None);
        assert!(fourth_trace.source_addr_known);
        assert!(!fourth_trace.source_addr_matches_candidate);
        assert_eq!(fourth_trace.source_addr_transport, "tcp");
        assert_eq!(fourth_trace.source_addr_family, "ip4");
        assert_eq!(fourth_trace.request_mode, "want_have");

        assert_eq!(
            bitswap_source_peer_trace_from_peers(None, None, &peers),
            BitswapSourcePeerTrace::default()
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
                        force_want_have: false,
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
        let tcp = Multiaddr::from_str("/ip4/127.0.0.1/tcp/4001").unwrap();
        let quic = Multiaddr::from_str("/ip4/127.0.0.1/udp/4001/quic-v1").unwrap();

        assert_eq!(current_peer_connection(&transports, peer).await.0, None);

        record_peer_transport_established(&transports, peer, "tcp", tcp.clone()).await;
        assert_eq!(
            current_peer_connection(&transports, peer).await.0,
            Some("tcp")
        );
        assert_eq!(
            current_peer_connection(&transports, peer).await.1,
            Some(tcp.clone())
        );

        record_peer_transport_established(&transports, peer, "quic", quic.clone()).await;
        assert_eq!(
            current_peer_connection(&transports, peer).await.0,
            Some("quic")
        );
        assert_eq!(
            current_peer_connection(&transports, peer).await.1,
            Some(quic.clone())
        );

        record_peer_transport_closed(&transports, peer, "quic", &quic).await;
        assert_eq!(
            current_peer_connection(&transports, peer).await.0,
            Some("tcp")
        );
        assert_eq!(current_peer_connection(&transports, peer).await.1, None);

        record_peer_transport_closed(&transports, peer, "tcp", &tcp).await;
        assert_eq!(current_peer_connection(&transports, peer).await.0, None);
        assert_eq!(current_peer_connection(&transports, peer).await.1, None);
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
                    force_want_have: false,
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
                    force_want_have: false,
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
                source_addr: None,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_http_5xx_default_keeps_self_hedge_recovery() {
        let expected = b"verified single HTTP 5xx retry recovery block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let requests = Arc::new(AtomicU64::new(0));
        let (addr, task) = spawn_sequenced_status_http_provider(
            expected.to_vec(),
            std::collections::VecDeque::from([500, 200]),
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

        let (block, source) = retriever
            .fetch_from_providers_with_source_with_options(
                &cid,
                &[provider],
                None,
                ProviderFetchOptions {
                    single_http_5xx_fast_bitswap_fallback: false,
                    ..ProviderFetchOptions::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::HttpProvider);
        assert_eq!(block.data(), expected);
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_http_5xx_fast_fallback_skips_duplicate_http_and_uses_bitswap() {
        let expected = b"verified single HTTP 5xx fast Bitswap fallback block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_sequenced_status_http_provider(
            expected.to_vec(),
            std::collections::VecDeque::from([500, 200]),
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

        let (block, source) = retriever
            .fetch_from_providers_with_source_with_options(
                &cid,
                &providers,
                None,
                ProviderFetchOptions {
                    single_http_5xx_fast_bitswap_fallback: true,
                    ..ProviderFetchOptions::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected);
        assert_eq!(http_requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        tokio::time::timeout(Duration::from_secs(5), bitswap_stream)
            .await
            .unwrap()
            .unwrap();
        bitswap_swarm.abort();
        http_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn top_level_single_http_failed_direct_ip_fallback_uses_bitswap() {
        let expected = b"verified top-level single HTTP failed direct-IP Bitswap fallback block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_sequenced_status_http_provider(
            expected.to_vec(),
            std::collections::VecDeque::from([500, 500]),
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
        let mut providers = vec![
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
        for _ in providers.len()..TOP_LEVEL_SINGLE_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS {
            let filler_peer = libp2p::identity::Keypair::generate_ed25519()
                .public()
                .to_peer_id();
            providers
                .push(Provider::from_parts(Some(filler_peer.to_string()), Vec::new()).unwrap());
        }

        let context = RetrievalRequestContext::gateway_request_with_top_level(
            None,
            Some("/ipfs/example".to_string()),
        );
        let (block, source) = retriever
            .fetch_from_providers_with_source_with_options(
                &cid,
                &providers,
                Some(context),
                ProviderFetchOptions {
                    top_level_single_http_failed_direct_ip_bitswap_fallback: true,
                    ..ProviderFetchOptions::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected);
        assert_eq!(http_requests.load(Ordering::Relaxed), 2);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        tokio::time::timeout(Duration::from_secs(5), bitswap_stream)
            .await
            .unwrap()
            .unwrap();
        bitswap_swarm.abort();
        http_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn top_level_multi_http_failed_direct_ip_fallback_uses_bitswap() {
        let expected = b"verified top-level multi HTTP failed direct-IP Bitswap fallback block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let failing_http_requests = Arc::new(AtomicU64::new(0));
        let slow_http_requests = Arc::new(AtomicU64::new(0));
        let (failing_http_addr, failing_http_task) = spawn_sequenced_status_http_provider(
            expected.to_vec(),
            std::collections::VecDeque::from([500]),
            failing_http_requests.clone(),
        )
        .await;
        let (slow_http_addr, slow_http_task) = spawn_counting_http_provider(
            expected.to_vec(),
            Duration::from_secs(5),
            slow_http_requests.clone(),
        )
        .await;
        let (peer_id, bitswap_addr, bitswap_swarm, bitswap_stream) =
            spawn_local_bitswap_peer(cid, expected.to_vec()).await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let mut providers = vec![
            Provider::from_parts(
                None,
                vec![format!(
                    "/ip4/{}/tcp/{}/http",
                    failing_http_addr.ip(),
                    failing_http_addr.port()
                )],
            )
            .unwrap(),
            Provider::from_parts(
                None,
                vec![format!(
                    "/ip4/{}/tcp/{}/http",
                    slow_http_addr.ip(),
                    slow_http_addr.port()
                )],
            )
            .unwrap(),
            Provider::from_parts(Some(peer_id.to_string()), vec![bitswap_addr.to_string()])
                .unwrap(),
        ];
        for _ in providers.len()..TOP_LEVEL_MULTI_HTTP_FAILED_DIRECT_IP_BITSWAP_MIN_PROVIDERS {
            let filler_peer = libp2p::identity::Keypair::generate_ed25519()
                .public()
                .to_peer_id();
            providers
                .push(Provider::from_parts(Some(filler_peer.to_string()), Vec::new()).unwrap());
        }

        let context = RetrievalRequestContext::gateway_request_with_top_level(
            None,
            Some("/ipfs/example".to_string()),
        );
        let (block, source) = tokio::time::timeout(
            Duration::from_secs(3),
            retriever.fetch_from_providers_with_source_with_options(
                &cid,
                &providers,
                Some(context),
                ProviderFetchOptions {
                    top_level_multi_http_failed_direct_ip_bitswap_fallback: true,
                    ..ProviderFetchOptions::default()
                },
            ),
        )
        .await
        .expect("multi-HTTP direct-IP fallback should beat the slow HTTP provider")
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected);
        assert_eq!(failing_http_requests.load(Ordering::Relaxed), 1);
        assert_eq!(slow_http_requests.load(Ordering::Relaxed), 1);
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), expected);
        tokio::time::timeout(Duration::from_secs(5), bitswap_stream)
            .await
            .unwrap()
            .unwrap();
        bitswap_swarm.abort();
        failing_http_task.abort();
        slow_http_task.abort();
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
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                None
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(None),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(Some("6")),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(6)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(Some("0")),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(Some("bad")),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(None)),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(None),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(None)),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                Some("5"),
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(None)),
            ),
            Some(5)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(Some("6")),
                Some("5"),
                Some("8"),
                Some(32),
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(5)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                Some("8"),
                Some(32),
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(None)),
            ),
            Some(8)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                Some("8"),
                Some(32),
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                16,
                Some(&RetrievalRequestContext::gateway_request(None)),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::enabled(None),
                None,
                Some("bad"),
                Some(32),
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                64,
                Some(&RetrievalRequestContext::gateway_request(Some(1))),
            ),
            Some(BITSWAP_ZERO_HTTP_SUBRESOURCE_DIRECT_WANT_BLOCK_PEERS)
        );
    }

    #[test]
    fn zero_http_post_lookup_timeout_direct_want_block_is_scoped() {
        let timed_out_subresource = RetrievalRequestContext::gateway_request(Some(1))
            .with_zero_http_post_lookup_shortcut_timeout();
        let timed_out_top_level = RetrievalRequestContext::gateway_request(None)
            .with_zero_http_post_lookup_shortcut_timeout();
        let normal_subresource = RetrievalRequestContext::gateway_request(Some(1));

        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::enabled(None),
                16,
                Some(&timed_out_subresource),
            ),
            Some(BITSWAP_ZERO_HTTP_POST_LOOKUP_TIMEOUT_DIRECT_WANT_BLOCK_PEERS)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::enabled(Some("3")),
                16,
                Some(&timed_out_subresource),
            ),
            Some(3)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                Some("2"),
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::enabled(Some("3")),
                16,
                Some(&timed_out_subresource),
            ),
            Some(2)
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::disabled(),
                16,
                Some(&timed_out_subresource),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::enabled(None),
                16,
                Some(&timed_out_top_level),
            ),
            None
        );
        assert_eq!(
            bitswap_zero_http_direct_want_block_peers_from_values(
                SubresourceDirectWantBlockConfig::disabled(),
                None,
                None,
                None,
                PostLookupTimeoutDirectWantBlockConfig::enabled(None),
                16,
                Some(&normal_subresource),
            ),
            None
        );
    }

    #[test]
    fn direct_untrusted_want_block_peer_limit_parses_override() {
        assert_eq!(
            bitswap_direct_want_block_untrusted_peer_limit_from_env_value(None),
            MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS
        );
        assert_eq!(
            bitswap_direct_want_block_untrusted_peer_limit_from_env_value(Some("bad")),
            MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS
        );
        assert_eq!(
            bitswap_direct_want_block_untrusted_peer_limit_from_env_value(Some("0")),
            0
        );
        assert_eq!(
            bitswap_direct_want_block_untrusted_peer_limit_from_env_value(Some("1")),
            1
        );
        assert_eq!(
            bitswap_direct_want_block_untrusted_peer_limit_from_env_value(Some("999")),
            MAX_BITSWAP_PEERS_PER_BLOCK
        );
    }

    #[test]
    fn bitswap_want_have_timeout_parses_optional_override() {
        assert_eq!(
            bitswap_want_have_timeout_from_env_value(None),
            BITSWAP_WANT_HAVE_TIMEOUT
        );
        assert_eq!(
            bitswap_want_have_timeout_from_env_value(Some("bad")),
            BITSWAP_WANT_HAVE_TIMEOUT
        );
        assert_eq!(
            bitswap_want_have_timeout_from_env_value(Some("0")),
            BITSWAP_WANT_HAVE_TIMEOUT
        );
        assert_eq!(
            bitswap_want_have_timeout_from_env_value(Some("250")),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn direct_untrusted_want_block_peer_limit_controls_request_modes() {
        let mut direct = 0;
        assert!(
            !bitswap_prefer_want_have_with_direct_limit(true, false, false, false, &mut direct, 1),
            "first untrusted peer should get the optimistic want-block"
        );
        assert!(
            bitswap_prefer_want_have_with_direct_limit(true, false, false, false, &mut direct, 1),
            "later untrusted peers should use want-have after the limit"
        );
        assert!(
            !bitswap_prefer_want_have_with_direct_limit(true, true, false, false, &mut direct, 1),
            "trusted/session peers still bypass want-have"
        );
        assert!(
            !bitswap_prefer_want_have_with_direct_limit(true, false, true, false, &mut direct, 0),
            "forced peers still get want-block even when the generic direct limit is zero"
        );
        assert!(
            bitswap_prefer_want_have_with_direct_limit(true, true, false, true, &mut direct, 1),
            "trusted/session peers can be forced to use want-have as a lab control"
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
                force_want_have: false,
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
    fn zero_http_subresource_peer_rotation_preserves_trusted_prefix() {
        let trusted =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let first = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let second = parse_peer_id("12D3KooWQpU6Qg7vHmzZDQ1kEU1vq3Jua3BMyyMi5LfQcwTJTQJJ").unwrap();
        let third = parse_peer_id("12D3KooWGLJ5YV1mbFWfvUXCLDn3gXVKqvKiLR1zNVzFCSkSBGSA").unwrap();
        let mut peers = vec![
            BitswapPeer {
                id: trusted,
                addrs: Vec::new(),
                skip_want_have: true,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: first,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: third,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
        ];

        let offset = rotate_untrusted_bitswap_peer_suffix(&mut peers, 1);

        assert_eq!(offset, Some(1));
        assert_eq!(peers[0].id, trusted);
        assert_eq!(peers[1].id, second);
        assert_eq!(peers[2].id, third);
        assert_eq!(peers[3].id, first);
    }

    #[test]
    fn zero_http_subresource_peer_rotation_hash_is_stable_and_bounded() {
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();

        let offset =
            stable_zero_http_subresource_peer_rotation_offset(&cid, Some("/ipns/ipfs.tech/"), 8);

        assert_eq!(
            offset,
            stable_zero_http_subresource_peer_rotation_offset(&cid, Some("/ipns/ipfs.tech/"), 8)
        );
        assert!(offset < 8);
        assert_eq!(
            stable_zero_http_subresource_peer_rotation_offset(&cid, Some("/ipns/ipfs.tech/"), 0),
            0
        );
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

    #[test]
    fn bitswap_session_peer_limit_env_value_parses_capped_override() {
        assert_eq!(
            bitswap_session_peer_limit_from_env_value(None),
            MAX_BITSWAP_SESSION_PEERS
        );
        assert_eq!(bitswap_session_peer_limit_from_env_value(Some("1")), 1);
        assert_eq!(bitswap_session_peer_limit_from_env_value(Some("2")), 2);
        assert_eq!(
            bitswap_session_peer_limit_from_env_value(Some("0")),
            MAX_BITSWAP_SESSION_PEERS
        );
        assert_eq!(
            bitswap_session_peer_limit_from_env_value(Some("bad")),
            MAX_BITSWAP_SESSION_PEERS
        );
        assert_eq!(
            bitswap_session_peer_limit_from_env_value(Some("999")),
            MAX_BITSWAP_SESSION_PEERS
        );
    }

    #[test]
    fn top_level_bitswap_provider_preconnect_peer_limit_parses_override() {
        assert_eq!(
            top_level_bitswap_provider_preconnect_peers_from_env_value(None),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_peers_from_env_value(Some("0")),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_peers_from_env_value(Some("bad")),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_PEERS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_peers_from_env_value(Some("5")),
            5.min(MAX_BITSWAP_PEERS_PER_BLOCK)
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_peers_from_env_value(Some("999")),
            MAX_BITSWAP_PEERS_PER_BLOCK
        );
    }

    #[test]
    fn top_level_bitswap_provider_preconnect_budget_parses_override() {
        assert_eq!(
            top_level_bitswap_provider_preconnect_max_requests_from_env_value(None),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_max_requests_from_env_value(Some("0")),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_max_requests_from_env_value(Some("bad")),
            TOP_LEVEL_BITSWAP_PROVIDER_PRECONNECT_MAX_REQUESTS
        );
        assert_eq!(
            top_level_bitswap_provider_preconnect_max_requests_from_env_value(Some("3")),
            3
        );
    }

    #[test]
    fn single_http_bitswap_hedge_budget_parses_override() {
        assert_eq!(
            single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(None),
            None
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(Some("0")),
            None
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(Some("bad")),
            None
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_max_per_top_level_from_env_value(Some("2")),
            Some(2)
        );
    }

    #[tokio::test]
    async fn single_http_bitswap_hedge_budget_is_top_level_scoped() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );

        assert_eq!(
            retriever
                .reserve_single_http_provider_bitswap_hedge("/ipns/site-a/", 2)
                .await,
            Some(1)
        );
        assert_eq!(
            retriever
                .reserve_single_http_provider_bitswap_hedge("/ipns/site-a/", 2)
                .await,
            Some(2)
        );
        assert_eq!(
            retriever
                .reserve_single_http_provider_bitswap_hedge("/ipns/site-a/", 2)
                .await,
            None
        );
        assert_eq!(
            retriever
                .reserve_single_http_provider_bitswap_hedge("/ipns/site-b/", 2)
                .await,
            Some(1)
        );
        assert_eq!(
            retriever
                .reserve_single_http_provider_bitswap_hedge("/ipns/site-c/", 0)
                .await,
            None
        );
    }

    #[test]
    fn bitswap_session_peer_min_successes_env_value_parses_override() {
        assert_eq!(
            bitswap_session_peer_min_successes_from_env_value(None),
            BITSWAP_SESSION_PEER_MIN_SUCCESSES
        );
        assert_eq!(
            bitswap_session_peer_min_successes_from_env_value(Some("0")),
            BITSWAP_SESSION_PEER_MIN_SUCCESSES
        );
        assert_eq!(
            bitswap_session_peer_min_successes_from_env_value(Some("bad")),
            BITSWAP_SESSION_PEER_MIN_SUCCESSES
        );
        assert_eq!(
            bitswap_session_peer_min_successes_from_env_value(Some("2")),
            2
        );
    }

    #[test]
    fn trusted_direct_want_block_peer_limit_parses_optional_override() {
        assert_eq!(
            bitswap_trusted_direct_want_block_peers_from_env_value(None),
            None
        );
        assert_eq!(
            bitswap_trusted_direct_want_block_peers_from_env_value(Some("bad")),
            None
        );
        assert_eq!(
            bitswap_trusted_direct_want_block_peers_from_env_value(Some("0")),
            Some(0)
        );
        assert_eq!(
            bitswap_trusted_direct_want_block_peers_from_env_value(Some("1")),
            Some(1)
        );
        assert_eq!(
            bitswap_trusted_direct_want_block_peers_from_env_value(Some("999")),
            Some(MAX_BITSWAP_SESSION_PEERS)
        );
    }

    #[test]
    fn trusted_direct_want_block_peer_limit_marks_extra_trusted_peers_as_probes() {
        let mut peers = (0..5)
            .map(|index| BitswapPeer {
                id: PeerId::random(),
                addrs: Vec::new(),
                skip_want_have: index < 3,
                force_want_block: false,
                force_want_have: false,
            })
            .collect::<Vec<_>>();

        let marked = maybe_force_trusted_bitswap_want_have_probes_with_limit(&mut peers, Some(1));

        assert_eq!(marked, 2);
        assert!(!peers[0].force_want_have);
        assert!(peers[1].force_want_have);
        assert!(peers[2].force_want_have);
        assert!(!peers[3].force_want_have);
        assert!(!peers[4].force_want_have);
    }

    #[test]
    fn dominant_session_peer_alternates_env_value_parses_capped_override() {
        assert_eq!(
            bitswap_dominant_session_peer_alternates_from_env_value(None),
            0
        );
        assert_eq!(
            bitswap_dominant_session_peer_alternates_from_env_value(Some("bad")),
            0
        );
        assert_eq!(
            bitswap_dominant_session_peer_alternates_from_env_value(Some("2")),
            2
        );
        assert_eq!(
            bitswap_dominant_session_peer_alternates_from_env_value(Some("999")),
            MAX_BITSWAP_SESSION_PEERS - 1
        );
    }

    #[test]
    fn bitswap_dns_lookup_timeout_env_value_parses_optional_override() {
        assert_eq!(bitswap_dns_lookup_timeout_from_env_value(None), None);
        assert_eq!(bitswap_dns_lookup_timeout_from_env_value(Some("0")), None);
        assert_eq!(bitswap_dns_lookup_timeout_from_env_value(Some("bad")), None);
        assert_eq!(
            bitswap_dns_lookup_timeout_from_env_value(Some("250")),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn bitswap_dns_expansion_cache_scope_can_be_top_level_only() {
        let top_level = RetrievalRequestContext::gateway_request(None);
        let subresource = RetrievalRequestContext::gateway_request(Some(42));

        assert_eq!(
            bitswap_dns_expansion_cache_scope_from_values(false, false, Some(&top_level)),
            None
        );
        assert_eq!(
            bitswap_dns_expansion_cache_scope_from_values(true, false, Some(&subresource)),
            Some("global")
        );
        assert_eq!(
            bitswap_dns_expansion_cache_scope_from_values(false, true, Some(&top_level)),
            Some("gateway_top_level")
        );
        assert_eq!(
            bitswap_dns_expansion_cache_scope_from_values(false, true, Some(&subresource)),
            None
        );
        assert_eq!(
            bitswap_dns_expansion_cache_scope_from_values(false, true, None),
            None
        );
    }

    #[test]
    fn dominant_recent_bitswap_peer_requires_clear_success_lead() {
        let dominant =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let second = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let now = Instant::now();

        let below_threshold = vec![
            (dominant, now, Duration::from_millis(50), (), 7),
            (second, now, Duration::from_millis(25), (), 1),
        ];
        assert_eq!(dominant_recent_bitswap_peer_index(&below_threshold), None);

        let not_dominant = vec![
            (dominant, now, Duration::from_millis(50), (), 8),
            (second, now, Duration::from_millis(25), (), 3),
        ];
        assert_eq!(dominant_recent_bitswap_peer_index(&not_dominant), None);

        let clear_lead = vec![
            (dominant, now, Duration::from_millis(50), (), 16),
            (second, now, Duration::from_millis(25), (), 4),
        ];
        assert_eq!(dominant_recent_bitswap_peer_index(&clear_lead), Some(0));
    }

    #[test]
    fn dominant_session_peer_selection_can_keep_low_latency_alternates() {
        let dominant =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let fast = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let slow = parse_peer_id("12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG").unwrap();
        let now = Instant::now();
        let mut peers = vec![
            (dominant, now, Duration::from_millis(80), "dominant", 16),
            (fast, now, Duration::from_millis(20), "fast", 4),
            (slow, now, Duration::from_millis(200), "slow", 1),
        ];

        let selection = select_dominant_recent_bitswap_peers(&mut peers, 1)
            .expect("dominant peer should be selected");

        assert_eq!(selection.peer, dominant);
        assert_eq!(selection.peer_count, 3);
        assert_eq!(selection.selected_peer_count, 2);
        assert_eq!(selection.alternate_count, 1);
        assert_eq!(selection.next_success_count, 4);
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().any(|peer| peer.0 == dominant));
        assert!(peers.iter().any(|peer| peer.0 == fast));
        assert!(!peers.iter().any(|peer| peer.0 == slow));
    }

    #[test]
    fn successful_peer_scope_matches_current_top_level_path_when_enabled() {
        let success = SuccessfulBitswapPeer {
            seen_at: Instant::now(),
            addrs: Vec::new(),
            last_latency: Duration::from_millis(25),
            success_count: 1,
            top_level_path: Some("/ipns/ipfs.tech/".to_string()),
        };

        assert!(successful_peer_matches_top_level_scope(
            &success,
            false,
            Some("/ipns/en.wikipedia-on-ipfs.org")
        ));
        assert!(successful_peer_matches_top_level_scope(
            &success,
            true,
            Some("/ipns/ipfs.tech/")
        ));
        assert!(!successful_peer_matches_top_level_scope(
            &success,
            true,
            Some("/ipns/en.wikipedia-on-ipfs.org")
        ));
        assert!(successful_peer_matches_top_level_scope(
            &success, true, None
        ));
    }

    #[test]
    fn bitswap_session_peer_quality_summarizes_recent_peer_state() {
        let first = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let second = parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let third = parse_peer_id("12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG").unwrap();
        let now = Instant::now();
        let peers = vec![
            BitswapPeer {
                id: first,
                addrs: Vec::new(),
                skip_want_have: true,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: Vec::new(),
                skip_want_have: true,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: third,
                addrs: Vec::new(),
                skip_want_have: true,
                force_want_block: false,
                force_want_have: false,
            },
        ];
        let successes = HashMap::from([
            (
                first,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_millis(250),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(40),
                    success_count: 1,
                    top_level_path: Some("/ipns/ipfs.tech/".to_owned()),
                },
            ),
            (
                second,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_millis(25),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(120),
                    success_count: 3,
                    top_level_path: Some("/ipns/en.wikipedia-on-ipfs.org".to_owned()),
                },
            ),
            (
                third,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_millis(100),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(80),
                    success_count: 2,
                    top_level_path: None,
                },
            ),
        ]);

        let quality = bitswap_session_peer_quality_from_successes(
            &peers,
            &successes,
            now,
            Some("/ipns/ipfs.tech/"),
        );

        assert_eq!(
            quality,
            BitswapSessionPeerQuality {
                scored_count: 3,
                success_count_min: 1,
                success_count_max: 3,
                latency_ms_min: 40,
                latency_ms_max: 120,
                seen_age_ms_min: 25,
                seen_age_ms_max: 250,
                same_top_level_count: 1,
                cross_top_level_count: 1,
                unknown_top_level_count: 1,
            }
        );
    }

    #[tokio::test]
    async fn bitswap_source_peer_trace_reports_top_level_relation() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let peers = vec![BitswapPeer {
            id: peer,
            addrs: Vec::new(),
            skip_want_have: true,
            force_want_block: false,
            force_want_have: false,
        }];
        {
            let mut successes = retriever.successful_bitswap_peers.lock().await;
            successes.insert(
                peer,
                SuccessfulBitswapPeer {
                    seen_at: Instant::now() - Duration::from_millis(50),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(42),
                    success_count: 2,
                    top_level_path: Some("/ipns/ipfs.tech/".to_owned()),
                },
            );
        }

        let trace = with_retrieval_request_context(
            RetrievalRequestContext::gateway_request_with_top_level(
                None,
                Some("/ipns/en.wikipedia-on-ipfs.org".to_owned()),
            ),
            retriever.bitswap_source_peer_trace(Some(peer), None, &peers),
        )
        .await;

        assert_eq!(trace.previous_success_count, 2);
        assert_eq!(trace.previous_latency_ms, 42);
        assert_eq!(
            trace.previous_top_level_path.as_deref(),
            Some("/ipns/ipfs.tech/")
        );
        assert!(!trace.same_top_level);
        assert!(trace.cross_top_level);
        assert!(!trace.unknown_top_level);
    }

    #[test]
    fn dominant_session_peer_mode_can_be_scoped_to_top_level_gateway_requests() {
        assert_eq!(
            dominant_session_peer_mode_for_context(false, false, None),
            None
        );
        assert_eq!(
            dominant_session_peer_mode_for_context(
                true,
                false,
                Some(RetrievalRequestContext::gateway_request(Some(1)))
            ),
            Some(DominantSessionPeerMode::Global)
        );
        assert_eq!(
            dominant_session_peer_mode_for_context(
                false,
                true,
                Some(RetrievalRequestContext::gateway_request(None))
            ),
            Some(DominantSessionPeerMode::GatewayTopLevel)
        );
        assert_eq!(
            dominant_session_peer_mode_for_context(
                false,
                true,
                Some(RetrievalRequestContext::gateway_request(Some(1)))
            ),
            None
        );
        assert_eq!(
            dominant_session_peer_mode_for_context(false, true, None),
            None
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
                None,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_bitswap_hedge_uses_recent_peer_without_cold_provider_expansion() {
        let expected = b"verified single HTTP session bitswap hedge block";
        let cid = freedom_ipfs_core::cid_from_data(freedom_ipfs_core::CODEC_RAW, expected);
        let http_requests = Arc::new(AtomicU64::new(0));
        let (http_addr, http_task) = spawn_hanging_http_provider(http_requests.clone()).await;
        let (peer_id, bitswap_addr, bitswap_swarm, bitswap_stream) =
            spawn_local_bitswap_peer(cid, expected.to_vec()).await;
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        retriever
            .record_successful_bitswap_peer(peer_id, vec![bitswap_addr], Duration::from_millis(25))
            .await;
        let provider = Provider::from_parts(
            None,
            vec![format!(
                "/ip4/{}/tcp/{}/http",
                http_addr.ip(),
                http_addr.port()
            )],
        )
        .unwrap();

        let (block, source) = tokio::time::timeout(
            Duration::from_secs(3),
            retriever.fetch_from_providers_with_source_with_options(
                &cid,
                &[provider],
                None,
                ProviderFetchOptions {
                    single_http_session_bitswap_hedge: true,
                    ..ProviderFetchOptions::default()
                },
            ),
        )
        .await
        .expect("single HTTP session bitswap hedge timed out")
        .unwrap();

        assert_eq!(source, RetrievalSource::Bitswap);
        assert_eq!(block.data(), expected);
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
                force_want_have: false,
            },
            BitswapPeer {
                id: preferred,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
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
                    success_count: 1,
                    top_level_path: None,
                },
            );
            successes.insert(
                fast,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_secs(1),
                    addrs: Vec::new(),
                    last_latency: Duration::from_millis(80),
                    success_count: 1,
                    top_level_path: None,
                },
            );
        }
        let mut peers = vec![
            BitswapPeer {
                id: slow,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
            },
            BitswapPeer {
                id: fast,
                addrs: Vec::new(),
                skip_want_have: false,
                force_want_block: false,
                force_want_have: false,
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
                    success_count: 1,
                    top_level_path: None,
                },
            );
            successes.insert(
                fast,
                SuccessfulBitswapPeer {
                    seen_at: now - Duration::from_secs(1),
                    addrs: vec!["/ip4/127.0.0.1/tcp/4002".parse().unwrap()],
                    last_latency: Duration::from_millis(80),
                    success_count: 1,
                    top_level_path: None,
                },
            );
        }

        let peers = retriever.recent_bitswap_peers().await;

        assert_eq!(peers[0].id, fast);
        assert_eq!(peers[1].id, slow);
    }

    #[tokio::test]
    async fn recent_bitswap_shortcut_peers_can_require_repeated_successes() {
        let one_hit =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let repeated =
            parse_peer_id("12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );
        let now = Instant::now();
        {
            let mut successes = retriever.successful_bitswap_peers.lock().await;
            successes.insert(
                one_hit,
                SuccessfulBitswapPeer {
                    seen_at: now,
                    addrs: vec!["/ip4/127.0.0.1/tcp/4001".parse().unwrap()],
                    last_latency: Duration::from_millis(25),
                    success_count: 1,
                    top_level_path: None,
                },
            );
            successes.insert(
                repeated,
                SuccessfulBitswapPeer {
                    seen_at: now,
                    addrs: vec!["/ip4/127.0.0.1/tcp/4002".parse().unwrap()],
                    last_latency: Duration::from_millis(80),
                    success_count: 2,
                    top_level_path: None,
                },
            );
        }

        let peers = retriever.recent_bitswap_peers_with_min_successes(2).await;

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, repeated);
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
            force_want_have: false,
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
    async fn incoming_source_addr_can_seed_session_peer_when_enabled() {
        let incoming_peer =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let source_addr = Multiaddr::from_str(
            "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP",
        )
        .unwrap();
        let session_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/4001").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );

        retriever
            .record_successful_bitswap_peer_from_fetch_source_with_enabled(
                incoming_peer,
                &[],
                Some(&source_addr),
                Duration::from_millis(75),
                true,
            )
            .await;

        let peers = retriever.recent_bitswap_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, incoming_peer);
        assert_eq!(peers[0].addrs, vec![session_addr]);
        assert!(peers[0].skip_want_have);
    }

    #[tokio::test]
    async fn incoming_source_addr_is_not_session_peer_by_default() {
        let incoming_peer =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let source_addr = Multiaddr::from_str("/ip4/127.0.0.1/tcp/4001").unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store,
        );

        retriever
            .record_successful_bitswap_peer_from_fetch_source_with_enabled(
                incoming_peer,
                &[],
                Some(&source_addr),
                Duration::from_millis(75),
                false,
            )
            .await;

        assert!(retriever.recent_bitswap_peers().await.is_empty());
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
            force_want_have: false,
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
                force_want_have: false,
            },
            BitswapPeer {
                id: second,
                addrs: vec!["/ip4/127.0.0.2/tcp/4001".parse().unwrap()],
                skip_want_have: true,
                force_want_block: false,
                force_want_have: false,
            },
        ];

        retriever.mark_single_session_shortcut_timeout_peer(&cid, &peers);

        assert!(!store.is_bad_provider(&first.to_string()).unwrap());
        assert!(!store.is_bad_provider(&second.to_string()).unwrap());
    }

    #[test]
    fn zero_http_subresource_slow_source_suppression_threshold_is_opt_in() {
        assert_eq!(
            zero_http_subresource_slow_source_suppression_threshold_from_values(false, None),
            None
        );
        assert_eq!(
            zero_http_subresource_slow_source_suppression_threshold_from_values(true, None),
            Some(ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION)
        );
        assert_eq!(
            zero_http_subresource_slow_source_suppression_threshold_from_values(true, Some("500")),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            zero_http_subresource_slow_source_suppression_threshold_from_values(true, Some("0")),
            Some(ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION)
        );
        assert_eq!(
            zero_http_subresource_slow_source_suppression_threshold_from_values(true, Some("bad")),
            Some(ZERO_HTTP_SUBRESOURCE_SLOW_SOURCE_SUPPRESSION)
        );
    }

    #[test]
    fn zero_http_gateway_slow_source_suppression_threshold_is_opt_in() {
        assert_eq!(
            zero_http_gateway_slow_source_suppression_threshold_from_values(false, None),
            None
        );
        assert_eq!(
            zero_http_gateway_slow_source_suppression_threshold_from_values(true, None),
            Some(ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION)
        );
        assert_eq!(
            zero_http_gateway_slow_source_suppression_threshold_from_values(true, Some("500")),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            zero_http_gateway_slow_source_suppression_threshold_from_values(true, Some("0")),
            Some(ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION)
        );
        assert_eq!(
            zero_http_gateway_slow_source_suppression_threshold_from_values(true, Some("bad")),
            Some(ZERO_HTTP_GATEWAY_SLOW_SOURCE_SUPPRESSION)
        );
    }

    #[tokio::test]
    async fn slow_zero_http_subresource_source_suppression_can_be_temporarily_suppressed() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let source_trace = BitswapSourcePeerTrace {
            candidate_index: Some(0),
            request_mode: "want_block",
            ..BitswapSourcePeerTrace::default()
        };

        let marked = retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
            &cid,
            SlowZeroHttpSourcePeer {
                gateway_request: true,
                gateway_subresource: true,
                http_provider_count: 0,
                source_peer: Some(peer),
                source_trace: &source_trace,
                elapsed: Duration::from_millis(900),
                threshold: Duration::from_millis(750),
                trace_phase: "bitswap_slow_zero_http_subresource_source_suppressed",
                bad_provider_reason: "slow zero-http subresource bitswap source",
            },
        );

        assert!(marked);
        assert!(store.is_bad_provider(&peer.to_string()).unwrap());
    }

    #[tokio::test]
    async fn slow_zero_http_gateway_root_source_suppression_can_be_temporarily_suppressed() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let source_trace = BitswapSourcePeerTrace {
            candidate_index: Some(2),
            request_mode: "want_block",
            ..BitswapSourcePeerTrace::default()
        };

        let marked = retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
            &cid,
            SlowZeroHttpSourcePeer {
                gateway_request: true,
                gateway_subresource: false,
                http_provider_count: 0,
                source_peer: Some(peer),
                source_trace: &source_trace,
                elapsed: Duration::from_millis(1400),
                threshold: Duration::from_millis(750),
                trace_phase: "bitswap_slow_zero_http_gateway_source_suppressed",
                bad_provider_reason: "slow zero-http gateway bitswap source",
            },
        );

        assert!(marked);
        assert!(store.is_bad_provider(&peer.to_string()).unwrap());
    }

    #[tokio::test]
    async fn slow_source_suppression_ignores_non_gateway_context() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let source_trace = BitswapSourcePeerTrace {
            candidate_index: Some(0),
            request_mode: "want_block",
            ..BitswapSourcePeerTrace::default()
        };

        assert!(
            !retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
                &cid,
                SlowZeroHttpSourcePeer {
                    gateway_request: false,
                    gateway_subresource: false,
                    http_provider_count: 0,
                    source_peer: Some(peer),
                    source_trace: &source_trace,
                    elapsed: Duration::from_millis(900),
                    threshold: Duration::from_millis(750),
                    trace_phase: "bitswap_slow_zero_http_gateway_source_suppressed",
                    bad_provider_reason: "slow zero-http gateway bitswap source",
                },
            )
        );
        assert!(!store.is_bad_provider(&peer.to_string()).unwrap());
    }

    #[tokio::test]
    async fn slow_source_suppression_ignores_fast_trusted_or_http_sources() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let cid = "bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u"
            .parse::<Cid>()
            .unwrap();
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let retriever = HttpRetriever::new(
            freedom_ipfs_routing::DelegatedRoutingClient::new("http://127.0.0.1:9/routing/v1"),
            store.clone(),
        );
        let untrusted_source = BitswapSourcePeerTrace {
            candidate_index: Some(0),
            request_mode: "want_block",
            ..BitswapSourcePeerTrace::default()
        };
        let trusted_source = BitswapSourcePeerTrace {
            candidate_index: Some(0),
            request_mode: "want_block",
            skip_want_have: true,
            ..BitswapSourcePeerTrace::default()
        };
        let threshold = Duration::from_millis(750);

        assert!(
            !retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
                &cid,
                SlowZeroHttpSourcePeer {
                    gateway_request: true,
                    gateway_subresource: true,
                    http_provider_count: 0,
                    source_peer: Some(peer),
                    source_trace: &untrusted_source,
                    elapsed: Duration::from_millis(700),
                    threshold,
                    trace_phase: "bitswap_slow_zero_http_subresource_source_suppressed",
                    bad_provider_reason: "slow zero-http subresource bitswap source",
                },
            )
        );
        assert!(
            !retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
                &cid,
                SlowZeroHttpSourcePeer {
                    gateway_request: true,
                    gateway_subresource: true,
                    http_provider_count: 1,
                    source_peer: Some(peer),
                    source_trace: &untrusted_source,
                    elapsed: Duration::from_millis(900),
                    threshold,
                    trace_phase: "bitswap_slow_zero_http_subresource_source_suppressed",
                    bad_provider_reason: "slow zero-http subresource bitswap source",
                },
            )
        );
        assert!(
            !retriever.maybe_mark_slow_zero_http_source_peer_with_threshold(
                &cid,
                SlowZeroHttpSourcePeer {
                    gateway_request: true,
                    gateway_subresource: true,
                    http_provider_count: 0,
                    source_peer: Some(peer),
                    source_trace: &trusted_source,
                    elapsed: Duration::from_millis(900),
                    threshold,
                    trace_phase: "bitswap_slow_zero_http_subresource_source_suppressed",
                    bad_provider_reason: "slow zero-http subresource bitswap source",
                },
            )
        );
        assert!(!store.is_bad_provider(&peer.to_string()).unwrap());
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
    fn prunes_completed_closed_and_expired_preconnect_waiters() {
        let (completed_ready, completed_wait) = oneshot::channel();
        let (closed_ready, closed_wait) = oneshot::channel::<()>();
        let (_pending_ready, pending_wait) = oneshot::channel::<()>();
        let (_expired_ready, expired_wait) = oneshot::channel::<()>();
        let expired_started = Instant::now()
            .checked_sub(bitswap_connection_ready_timeout() + Duration::from_millis(1))
            .unwrap_or_else(Instant::now);
        let mut waiters = vec![
            HeldPreconnectWaiter {
                started: Instant::now(),
                receiver: completed_wait,
            },
            HeldPreconnectWaiter {
                started: Instant::now(),
                receiver: closed_wait,
            },
            HeldPreconnectWaiter {
                started: Instant::now(),
                receiver: pending_wait,
            },
            HeldPreconnectWaiter {
                started: expired_started,
                receiver: expired_wait,
            },
        ];

        completed_ready.send(()).unwrap();
        drop(closed_ready);
        prune_preconnect_waiters(&mut waiters);

        assert_eq!(waiters.len(), 1);
    }

    #[test]
    fn bitswap_command_context_captures_gateway_zero_http_subresource() {
        let context = RetrievalRequestContext::gateway_request_with_top_level(
            Some(7),
            Some("/ipns/ipfs.tech/".to_owned()),
        );

        let command_context = BitswapCommandContext::from_retrieval_context(Some(&context), 0);

        assert_eq!(
            command_context.top_level_path.as_deref(),
            Some("/ipns/ipfs.tech/")
        );
        assert!(command_context.gateway_subresource);
        assert!(command_context.zero_http_provider);

        let http_backed_context = BitswapCommandContext::from_retrieval_context(Some(&context), 1);
        assert!(!http_backed_context.zero_http_provider);
    }

    #[test]
    fn bitswap_command_context_defaults_for_non_gateway_fetch() {
        let command_context = BitswapCommandContext::from_retrieval_context(None, 0);

        assert!(command_context.top_level_path.is_none());
        assert!(!command_context.gateway_subresource);
        assert!(command_context.zero_http_provider);
    }

    #[test]
    fn prunes_expired_bitswap_dial_contexts() {
        let retained_peer =
            parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let expired_peer =
            parse_peer_id("12D3KooWLrHNtnKacjkq6cAVdmQDy1n3GUrZrJ8qzcyzqcnzMQ1x").unwrap();
        let now = Instant::now();
        let mut contexts = HashMap::from([
            (
                retained_peer,
                BitswapDialContext {
                    reason: "fetch",
                    command: BitswapCommandContext::default(),
                    started: now,
                },
            ),
            (
                expired_peer,
                BitswapDialContext {
                    reason: "fetch",
                    command: BitswapCommandContext::default(),
                    started: now
                        .checked_sub(
                            BITSWAP_CONNECTION_ERROR_BACKOFF_TTL + Duration::from_millis(1),
                        )
                        .unwrap(),
                },
            ),
        ]);

        prune_connection_dial_contexts(&mut contexts, now);

        assert!(contexts.contains_key(&retained_peer));
        assert!(!contexts.contains_key(&expired_peer));
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
    fn incoming_read_timeout_backoff_suppresses_peer_immediately() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let mut backoff = HashMap::new();
        let now = Instant::now();

        let state = record_incoming_read_timeout_backoff(&mut backoff, peer, now);

        assert_eq!(state.count, 1);
        assert_eq!(state.class, "incoming_stream_read_timeout");
        assert!(connection_error_backoff_remaining_ms(
            &backoff,
            &peer,
            now + Duration::from_millis(1)
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
    fn scoped_zero_http_subresource_backoff_threshold_is_opt_in() {
        let top_level_zero_http = BitswapCommandContext {
            top_level_path: Some("/ipns/ipfs.tech/".to_owned()),
            gateway_subresource: false,
            zero_http_provider: true,
        };
        let subresource_http_backed = BitswapCommandContext {
            top_level_path: Some("/ipns/ipfs.tech/".to_owned()),
            gateway_subresource: true,
            zero_http_provider: false,
        };
        let subresource_zero_http = BitswapCommandContext {
            top_level_path: Some("/ipns/ipfs.tech/".to_owned()),
            gateway_subresource: true,
            zero_http_provider: true,
        };

        assert_eq!(
            bitswap_connection_error_backoff_threshold_for_context_from_values(
                2,
                false,
                Some(&subresource_zero_http),
            ),
            2
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_for_context_from_values(
                2,
                true,
                Some(&top_level_zero_http),
            ),
            2
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_for_context_from_values(
                2,
                true,
                Some(&subresource_http_backed),
            ),
            2
        );
        assert_eq!(
            bitswap_connection_error_backoff_threshold_for_context_from_values(
                2,
                true,
                Some(&subresource_zero_http),
            ),
            1
        );
    }

    #[test]
    fn connection_error_backoff_can_use_explicit_threshold() {
        let peer = parse_peer_id("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP").unwrap();
        let mut backoff = HashMap::new();
        let now = Instant::now();

        let state = record_connection_error_backoff_with_threshold(
            &mut backoff,
            peer,
            "Transport failed: Connection refused",
            now,
            1,
        )
        .unwrap();

        assert_eq!(state.count, 1);
        assert_eq!(state.class, "connection_refused");
        assert!(connection_error_backoff_remaining_ms(
            &backoff,
            &peer,
            now + Duration::from_millis(1)
        )
        .is_some());
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
            force_want_have: false,
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
    fn single_http_bitswap_hedge_after_env_value_parses_override() {
        assert_eq!(
            single_http_provider_bitswap_hedge_after_from_env_value(None),
            SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_after_from_env_value(Some("500")),
            Duration::from_millis(500)
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_after_from_env_value(Some("0")),
            Duration::from_millis(0)
        );
        assert_eq!(
            single_http_provider_bitswap_hedge_after_from_env_value(Some("not-a-number")),
            SINGLE_HTTP_PROVIDER_BITSWAP_HEDGE_AFTER
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
            bitswap_session_post_lookup_grace_from_env_value(
                &single_http,
                Some("25"),
                Some("125"),
                Some("0")
            ),
            Duration::from_millis(125)
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(
                &multi_http,
                Some("25"),
                Some("125"),
                None
            ),
            BITSWAP_SESSION_POST_LOOKUP_GRACE
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(
                &multi_http,
                Some("25"),
                Some("125"),
                Some("0")
            ),
            Duration::from_millis(0)
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(
                &bitswap_only,
                Some("25"),
                Some("125"),
                Some("0")
            ),
            Duration::from_millis(25)
        );
        assert_eq!(
            bitswap_session_post_lookup_grace_from_env_value(
                &bitswap_only,
                Some("not-a-number"),
                Some("125"),
                Some("0")
            ),
            BITSWAP_SESSION_POST_LOOKUP_GRACE
        );
    }

    #[test]
    fn top_level_single_http_provider_win_bitswap_grace_is_narrowly_scoped() {
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(false, None, false, 1),
            None
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(true, None, true, 1),
            None
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(true, None, false, 0),
            None
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(true, None, false, 2),
            None
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(true, None, false, 1),
            Some(TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE)
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(
                true,
                Some("75"),
                false,
                1,
            ),
            Some(Duration::from_millis(75))
        );
        assert_eq!(
            top_level_single_http_provider_win_bitswap_grace_from_values(
                true,
                Some("bad"),
                false,
                1,
            ),
            Some(TOP_LEVEL_SINGLE_HTTP_PROVIDER_WIN_BITSWAP_GRACE)
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

    async fn spawn_sequenced_status_http_provider(
        data: Vec<u8>,
        statuses: std::collections::VecDeque<u16>,
        requests: Arc<AtomicU64>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let statuses = Arc::new(tokio::sync::Mutex::new(statuses));
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let data = data.clone();
                let requests = requests.clone();
                let statuses = statuses.clone();
                tokio::spawn(async move {
                    let mut request = vec![0u8; 4096];
                    if stream.read(&mut request).await.is_err() {
                        return;
                    }
                    requests.fetch_add(1, Ordering::Relaxed);
                    let status = statuses.lock().await.pop_front().unwrap_or(200);
                    let (reason, body) = if status == 200 {
                        ("OK", data)
                    } else {
                        ("Internal Server Error", Vec::new())
                    };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes()
                    .into_iter()
                    .chain(body)
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
