use cid::Cid;
use freedom_ipfs_namesys::{
    ipns_dht_record_key, verify_ipns_record, IpnsRecord, IpnsResolver, NamesysError,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use libp2p::kad::{
    self, store::MemoryStore, store::RecordStore, GetProvidersOk, GetRecordError, GetRecordOk,
    QueryResult,
};
use libp2p::multiaddr::Protocol;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{
    connection_limits, identify, noise, ping, tcp, tls, yamux, Multiaddr, PeerId, SwarmBuilder,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use url::Url;

pub const DEFAULT_DELEGATED_ROUTER: &str = "https://delegated-ipfs.dev/routing/v1";
pub const DEFAULT_DHT_QUERY_TIMEOUT: Duration = Duration::from_secs(25);
pub const DEFAULT_MAX_DHT_PROVIDERS: usize = 32;
const DEFAULT_DELEGATED_ROUTING_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_DELEGATED_ROUTING_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_DELEGATED_ROUTING_PROVIDERS: usize = 64;
const STREAMING_DELEGATED_HTTP_PROVIDER_TARGET: usize = 3;
const STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE: Duration = Duration::from_millis(100);
const STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET: usize = 4;
const STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_GRACE: Duration = Duration::from_millis(125);
const ENABLE_STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_ENV: &str =
    "FREEDOM_IPFS_ENABLE_STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET";
const STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_MIN_ELAPSED_ENV: &str =
    "FREEDOM_IPFS_STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_MIN_ELAPSED_MS";
const LAB_DROP_HTTP_PROVIDERS_FOR_CIDS_ENV: &str = "FREEDOM_IPFS_LAB_DROP_HTTP_PROVIDERS_FOR_CIDS";
const SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER: Duration = Duration::from_millis(750);
const DISABLE_SINGLE_DELEGATED_SELF_HEDGE_ENV: &str =
    "FREEDOM_IPFS_DISABLE_SINGLE_DELEGATED_SELF_HEDGE";
const MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY: usize = 2;
const LOW_DIVERSITY_DELEGATED_MERGE_TIMEOUT: Duration = Duration::from_millis(750);
const LOW_DIVERSITY_DHT_FALLBACK_TIMEOUT: Duration = Duration::from_millis(250);
const EMPTY_DELEGATED_PROVIDER_RETRY_DELAY: Duration = Duration::from_millis(100);
const LAB_SKIP_LOW_DIVERSITY_DHT_FOR_SINGLE_WSS_ENV: &str =
    "FREEDOM_IPFS_LAB_SKIP_LOW_DIVERSITY_DHT_FOR_SINGLE_WSS";
const DHT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const DHT_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);
const DHT_MAX_PENDING_OUTGOING_CONNECTIONS: u32 = 8;
const DHT_MAX_ESTABLISHED_CONNECTIONS: u32 = 16;
const DEFAULT_BOOTSTRAP_PEERS: &[&str] = &[
    "/dnsaddr/sg1.bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/dnsaddr/sv15.bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/am6.bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/ny5.bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
];

#[derive(Debug, Error)]
pub enum RoutingError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid router response: {0}")]
    InvalidResponse(String),
    #[error("invalid provider url: {0}")]
    InvalidProviderUrl(String),
    #[error("dht: {0}")]
    Dht(String),
}

pub type Result<T> = std::result::Result<T, RoutingError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub id: Option<String>,
    pub addrs: Vec<String>,
    pub http_urls: Vec<Url>,
}

impl Provider {
    pub fn from_parts(id: Option<String>, addrs: Vec<String>) -> Result<Self> {
        let mut http_urls = Vec::new();
        for addr in &addrs {
            if let Some(url) = http_url_from_multiaddr(addr)? {
                http_urls.push(url);
            }
        }
        Ok(Self {
            id,
            addrs,
            http_urls,
        })
    }
}

fn lab_drop_http_providers_for_cid(cid: &Cid, providers: Vec<Provider>) -> Vec<Provider> {
    let env_value = std::env::var_os(LAB_DROP_HTTP_PROVIDERS_FOR_CIDS_ENV);
    lab_drop_http_providers_for_cid_with_env_value(
        cid,
        providers,
        env_value.as_deref().and_then(|value| value.to_str()),
    )
}

fn lab_drop_http_providers_for_cid_with_env_value(
    cid: &Cid,
    mut providers: Vec<Provider>,
    env_value: Option<&str>,
) -> Vec<Provider> {
    if !lab_drop_http_providers_for_cid_from_env_value(cid, env_value) {
        return providers;
    }
    let dropped_http_provider_count: usize = providers
        .iter()
        .map(|provider| provider.http_urls.len())
        .sum();
    if dropped_http_provider_count == 0 {
        return providers;
    }
    for provider in &mut providers {
        provider.http_urls.clear();
    }
    tracing::info!(
        phase = "lab_http_provider_drop",
        cid = %cid,
        provider_count = providers.len(),
        dropped_http_provider_count,
        env = LAB_DROP_HTTP_PROVIDERS_FOR_CIDS_ENV
    );
    providers
}

fn lab_drop_http_providers_for_cid_from_env_value(cid: &Cid, env_value: Option<&str>) -> bool {
    let cid = cid.to_string();
    env_value
        .into_iter()
        .flat_map(|value| value.split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace()))
        .any(|value| {
            let value = value.trim();
            value == "*" || value == cid
        })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoutingStats {
    pub delegated_provider_lookups: u64,
    pub delegated_provider_results: u64,
    pub delegated_provider_errors: u64,
    pub dht_provider_lookups: u64,
    pub dht_provider_results: u64,
    pub dht_provider_errors: u64,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingStatsHandle {
    inner: Arc<RoutingStatsInner>,
}

#[derive(Debug, Default)]
struct RoutingStatsInner {
    delegated_provider_lookups: AtomicU64,
    delegated_provider_results: AtomicU64,
    delegated_provider_errors: AtomicU64,
    dht_provider_lookups: AtomicU64,
    dht_provider_results: AtomicU64,
    dht_provider_errors: AtomicU64,
}

impl RoutingStatsHandle {
    pub fn snapshot(&self) -> RoutingStats {
        RoutingStats {
            delegated_provider_lookups: self
                .inner
                .delegated_provider_lookups
                .load(Ordering::Relaxed),
            delegated_provider_results: self
                .inner
                .delegated_provider_results
                .load(Ordering::Relaxed),
            delegated_provider_errors: self.inner.delegated_provider_errors.load(Ordering::Relaxed),
            dht_provider_lookups: self.inner.dht_provider_lookups.load(Ordering::Relaxed),
            dht_provider_results: self.inner.dht_provider_results.load(Ordering::Relaxed),
            dht_provider_errors: self.inner.dht_provider_errors.load(Ordering::Relaxed),
        }
    }

    fn record_delegated_lookup(&self) {
        self.inner
            .delegated_provider_lookups
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_delegated_result(&self, provider_count: usize) {
        self.inner
            .delegated_provider_results
            .fetch_add(provider_count as u64, Ordering::Relaxed);
    }

    fn record_delegated_error(&self) {
        self.inner
            .delegated_provider_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_dht_lookup(&self) {
        self.inner
            .dht_provider_lookups
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_dht_result(&self, provider_count: usize) {
        self.inner
            .dht_provider_results
            .fetch_add(provider_count as u64, Ordering::Relaxed);
    }

    fn record_dht_error(&self) {
        self.inner
            .dht_provider_errors
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub enum ProviderRoutingClient {
    Offline,
    Delegated(DelegatedRoutingClient),
    Auto(AutoRoutingClient),
    LightDht(LightDhtClient),
    Observed {
        inner: Box<ProviderRoutingClient>,
        stats: RoutingStatsHandle,
    },
}

impl ProviderRoutingClient {
    pub async fn providers(&self, cid: &Cid) -> Result<Vec<Provider>> {
        let providers = match self {
            Self::Offline => Ok(Vec::new()),
            Self::Delegated(client) => client.providers(cid).await,
            Self::Auto(client) => client.providers(cid).await,
            Self::LightDht(client) => client.providers(cid).await,
            Self::Observed { inner, stats } => inner.providers_with_stats(cid, stats).await,
        }?;
        Ok(lab_drop_http_providers_for_cid(cid, providers))
    }

    pub fn with_stats(self, stats: RoutingStatsHandle) -> Self {
        match self {
            Self::Observed { inner, .. } => Self::Observed { inner, stats },
            other => Self::Observed {
                inner: Box::new(other),
                stats,
            },
        }
    }

    async fn providers_with_stats(
        &self,
        cid: &Cid,
        stats: &RoutingStatsHandle,
    ) -> Result<Vec<Provider>> {
        match self {
            Self::Offline => Ok(Vec::new()),
            Self::Delegated(client) => {
                stats.record_delegated_lookup();
                match client.providers(cid).await {
                    Ok(providers) => {
                        stats.record_delegated_result(providers.len());
                        Ok(providers)
                    }
                    Err(err) => {
                        stats.record_delegated_error();
                        Err(err)
                    }
                }
            }
            Self::Auto(client) => client.providers_with_stats(cid, Some(stats)).await,
            Self::LightDht(client) => {
                stats.record_dht_lookup();
                match client.providers(cid).await {
                    Ok(providers) => {
                        stats.record_dht_result(providers.len());
                        Ok(providers)
                    }
                    Err(err) => {
                        stats.record_dht_error();
                        Err(err)
                    }
                }
            }
            Self::Observed { .. } => Err(RoutingError::InvalidResponse(
                "nested observed routing clients are unsupported".into(),
            )),
        }
    }
}

impl From<DelegatedRoutingClient> for ProviderRoutingClient {
    fn from(client: DelegatedRoutingClient) -> Self {
        Self::Delegated(client)
    }
}

impl From<AutoRoutingClient> for ProviderRoutingClient {
    fn from(client: AutoRoutingClient) -> Self {
        Self::Auto(client)
    }
}

impl From<LightDhtClient> for ProviderRoutingClient {
    fn from(client: LightDhtClient) -> Self {
        Self::LightDht(client)
    }
}

#[derive(Debug, Clone)]
pub struct DelegatedRoutingClient {
    endpoints: Vec<String>,
    client: reqwest::Client,
}

impl Default for DelegatedRoutingClient {
    fn default() -> Self {
        Self::new(DEFAULT_DELEGATED_ROUTER)
    }
}

impl DelegatedRoutingClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self::with_endpoints([endpoint])
    }

    pub fn with_endpoints<I, S>(endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.into().trim_end_matches('/').to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            endpoints.push(DEFAULT_DELEGATED_ROUTER.to_string());
        }
        Self {
            endpoints,
            client: timeout_http_client(DEFAULT_DELEGATED_ROUTING_TIMEOUT),
        }
    }

    pub async fn providers(&self, cid: &Cid) -> Result<Vec<Provider>> {
        if self.endpoints.len() == 1 {
            if single_delegated_endpoint_self_hedge_enabled() {
                return self
                    .providers_from_single_endpoint_with_self_hedge(cid)
                    .await;
            }
            return self.providers_from_endpoint(&self.endpoints[0], cid).await;
        }

        let mut queries = self
            .endpoints
            .iter()
            .map(|endpoint| self.providers_from_endpoint(endpoint, cid))
            .collect::<FuturesUnordered<_>>();
        let mut merged_providers = Vec::new();
        let mut saw_provider_response = false;
        let mut saw_empty_response = false;
        let mut first_error = None;
        let low_diversity_deadline = tokio::time::sleep(LOW_DIVERSITY_DELEGATED_MERGE_TIMEOUT);
        tokio::pin!(low_diversity_deadline);
        let mut waiting_for_low_diversity_merge = false;

        loop {
            let result = if waiting_for_low_diversity_merge {
                tokio::select! {
                    result = queries.next() => result,
                    _ = &mut low_diversity_deadline => break,
                }
            } else {
                queries.next().await
            };
            let Some(result) = result else {
                break;
            };
            match result {
                Ok(providers) if !providers.is_empty() => {
                    saw_provider_response = true;
                    merged_providers = merge_provider_lists(merged_providers, providers)?;
                    if bitswap_provider_diversity(&merged_providers)
                        >= MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY
                    {
                        return Ok(merged_providers);
                    }
                    if !waiting_for_low_diversity_merge {
                        waiting_for_low_diversity_merge = true;
                        low_diversity_deadline.as_mut().reset(
                            tokio::time::Instant::now() + LOW_DIVERSITY_DELEGATED_MERGE_TIMEOUT,
                        );
                    }
                }
                Ok(_) => saw_empty_response = true,
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if saw_provider_response {
            Ok(merged_providers)
        } else if saw_empty_response {
            Ok(Vec::new())
        } else {
            Err(first_error.unwrap_or_else(|| {
                RoutingError::InvalidResponse("no delegated routing endpoints configured".into())
            }))
        }
    }

    async fn providers_from_single_endpoint_with_self_hedge(
        &self,
        cid: &Cid,
    ) -> Result<Vec<Provider>> {
        let endpoint = self.endpoints[0].as_str();
        let started = Instant::now();
        let mut pending = FuturesUnordered::new();
        pending.push(self.providers_from_endpoint_attempt(endpoint, cid, 0));
        let hedge = tokio::time::sleep(SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER);
        tokio::pin!(hedge);
        let mut hedge_fired = false;
        let mut attempted_request_count = 1usize;
        let mut empty_response_count = 0usize;
        let mut error_count = 0usize;
        let mut first_error = None;

        while !pending.is_empty() {
            tokio::select! {
                biased;

                result = pending.next() => {
                    let Some((attempt, result)) = result else {
                        break;
                    };
                    match result {
                        Ok(providers) => {
                            empty_response_count += usize::from(providers.is_empty());
                            if hedge_fired {
                                tracing::info!(
                                    phase = "delegated_provider_self_hedge_result",
                                    cid = %cid,
                                    endpoint,
                                    ok = true,
                                    provider_count = providers.len(),
                                    winner_attempt = attempt,
                                    attempted_request_count,
                                    empty_response_count,
                                    error_count,
                                    hedge_fired,
                                    elapsed_ms = started.elapsed().as_millis()
                                );
                            }
                            return Ok(providers);
                        }
                        Err(err) => {
                            error_count += 1;
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                }
                _ = &mut hedge, if !hedge_fired => {
                    hedge_fired = true;
                    attempted_request_count += 1;
                    tracing::info!(
                        phase = "delegated_provider_self_hedge",
                        cid = %cid,
                        endpoint,
                        timeout_ms = SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER.as_millis(),
                        reason = "slow_single_endpoint"
                    );
                    pending.push(self.providers_from_endpoint_attempt(endpoint, cid, 1));
                }
            }
        }

        let error = first_error.unwrap_or_else(|| {
            RoutingError::InvalidResponse("no delegated routing endpoint response".into())
        });
        if hedge_fired {
            tracing::info!(
                phase = "delegated_provider_self_hedge_result",
                cid = %cid,
                endpoint,
                ok = false,
                error = %error,
                attempted_request_count,
                empty_response_count,
                error_count,
                hedge_fired,
                elapsed_ms = started.elapsed().as_millis()
            );
        }
        Err(error)
    }

    async fn providers_from_endpoint_attempt(
        &self,
        endpoint: &str,
        cid: &Cid,
        attempt: usize,
    ) -> (usize, Result<Vec<Provider>>) {
        (attempt, self.providers_from_endpoint(endpoint, cid).await)
    }

    async fn providers_from_endpoint(&self, endpoint: &str, cid: &Cid) -> Result<Vec<Provider>> {
        let started = Instant::now();
        let result = async {
            let lookup_cid = delegated_lookup_cid(cid);
            let url = format!("{endpoint}/providers/{lookup_cid}");
            let response = self
                .client
                .get(url)
                .header("accept", "application/x-ndjson, application/json")
                .send()
                .await?
                .error_for_status()?;
            let response_headers_elapsed_ms = started.elapsed().as_millis();
            let response =
                limited_response_providers(response, MAX_DELEGATED_ROUTING_RESPONSE_BYTES).await?;
            Ok((response, response_headers_elapsed_ms))
        }
        .await;
        match &result {
            Ok((response, response_headers_elapsed_ms)) => tracing::info!(
                phase = "delegated_provider_lookup",
                cid = %cid,
                endpoint,
                ok = true,
                provider_count = response.providers.len(),
                http_provider_count = response.stats.http_provider_count,
                response_bytes = response.stats.bytes_read,
                response_lines = response.stats.line_count,
                response_headers_elapsed_ms,
                response_first_chunk_seen = response.stats.first_chunk_elapsed.is_some(),
                response_first_chunk_elapsed_ms = response
                    .stats
                    .first_chunk_elapsed
                    .map(|elapsed| response_headers_elapsed_ms + elapsed.as_millis())
                    .unwrap_or_default(),
                response_first_http_provider_seen = response
                    .stats
                    .first_http_provider_elapsed
                    .is_some(),
                response_first_http_provider_elapsed_ms = response
                    .stats
                    .first_http_provider_elapsed
                    .map(|elapsed| response_headers_elapsed_ms + elapsed.as_millis())
                    .unwrap_or_default(),
                response_target_met = response.stats.target_met_elapsed.is_some(),
                response_target_kind = response.stats.target_kind.unwrap_or("none"),
                response_target_returned_early = response.stats.target_returned_early,
                response_target_met_elapsed_ms = response
                    .stats
                    .target_met_elapsed
                    .map(|elapsed| response_headers_elapsed_ms + elapsed.as_millis())
                    .unwrap_or_default(),
                elapsed_ms = started.elapsed().as_millis()
            ),
            Err(err) => tracing::info!(
                phase = "delegated_provider_lookup",
                cid = %cid,
                endpoint,
                ok = false,
                error = %err,
                elapsed_ms = started.elapsed().as_millis()
            ),
        }
        result.map(|(response, _)| response.providers)
    }
}

fn delegated_lookup_cid(cid: &Cid) -> String {
    Cid::new_v1(cid.codec(), *cid.hash()).to_string()
}

fn single_delegated_endpoint_self_hedge_enabled() -> bool {
    std::env::var_os(DISABLE_SINGLE_DELEGATED_SELF_HEDGE_ENV).is_none()
}

#[derive(Debug, Clone)]
pub struct AutoRoutingClient {
    delegated: DelegatedRoutingClient,
    dht: LightDhtClient,
}

impl Default for AutoRoutingClient {
    fn default() -> Self {
        Self::new(DelegatedRoutingClient::default(), LightDhtClient::default())
    }
}

impl AutoRoutingClient {
    pub fn new(delegated: DelegatedRoutingClient, dht: LightDhtClient) -> Self {
        Self { delegated, dht }
    }

    pub async fn providers(&self, cid: &Cid) -> Result<Vec<Provider>> {
        self.providers_with_stats(cid, None).await
    }

    async fn providers_with_stats(
        &self,
        cid: &Cid,
        stats: Option<&RoutingStatsHandle>,
    ) -> Result<Vec<Provider>> {
        if let Some(stats) = stats {
            stats.record_delegated_lookup();
        }
        match self.delegated.providers(cid).await {
            Ok(providers) if !providers.is_empty() => {
                if let Some(stats) = stats {
                    stats.record_delegated_result(providers.len());
                }
                self.providers_after_delegated_results(cid, providers, stats)
                    .await
            }
            Ok(_) => {
                if let Some(stats) = stats {
                    stats.record_delegated_result(0);
                }
                if let Some(providers) = self.retry_empty_delegated_lookup(cid, stats).await {
                    return self
                        .providers_after_delegated_results(cid, providers, stats)
                        .await;
                }
                if let Some(stats) = stats {
                    stats.record_dht_lookup();
                }
                match self.dht.providers(cid).await {
                    Ok(providers) => {
                        if let Some(stats) = stats {
                            stats.record_dht_result(providers.len());
                        }
                        Ok(providers)
                    }
                    Err(err) => {
                        if let Some(stats) = stats {
                            stats.record_dht_error();
                        }
                        Err(err)
                    }
                }
            }
            Err(delegated_err) => match self.dht.providers(cid).await {
                Ok(providers) => {
                    if let Some(stats) = stats {
                        stats.record_delegated_error();
                        stats.record_dht_lookup();
                        stats.record_dht_result(providers.len());
                    }
                    Ok(providers)
                }
                Err(dht_err) => {
                    if let Some(stats) = stats {
                        stats.record_delegated_error();
                        stats.record_dht_lookup();
                        stats.record_dht_error();
                    }
                    Err(RoutingError::Dht(format!(
                        "delegated routing failed ({delegated_err}); light DHT failed ({dht_err})"
                    )))
                }
            },
        }
    }

    async fn providers_after_delegated_results(
        &self,
        cid: &Cid,
        providers: Vec<Provider>,
        stats: Option<&RoutingStatsHandle>,
    ) -> Result<Vec<Provider>> {
        self.providers_after_delegated_results_inner(
            cid,
            providers,
            stats,
            lab_skip_low_diversity_dht_for_single_wss_enabled(),
        )
        .await
    }

    async fn providers_after_delegated_results_inner(
        &self,
        cid: &Cid,
        providers: Vec<Provider>,
        stats: Option<&RoutingStatsHandle>,
        skip_single_wss_dht_fallback: bool,
    ) -> Result<Vec<Provider>> {
        let bitswap_provider_count = bitswap_provider_diversity(&providers);
        if bitswap_provider_count >= MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY {
            return Ok(providers);
        }
        tracing::info!(
            phase = "provider_diversity_low",
            cid = %cid,
            provider_count = providers.len(),
            bitswap_provider_count,
            min_bitswap_provider_count = MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY,
            fallback = "light_dht"
        );
        if skip_single_wss_dht_fallback
            && single_supported_wss_or_dnsaddr_bitswap_provider(&providers)
        {
            tracing::info!(
                phase = "provider_diversity_low",
                cid = %cid,
                provider_count = providers.len(),
                bitswap_provider_count,
                min_bitswap_provider_count = MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY,
                fallback = "light_dht",
                skipped = true,
                skip_reason = "single_dns_or_wss_provider_lab",
                env = LAB_SKIP_LOW_DIVERSITY_DHT_FOR_SINGLE_WSS_ENV
            );
            return Ok(providers);
        }
        if let Some(stats) = stats {
            stats.record_dht_lookup();
        }
        let dht_started = Instant::now();
        match tokio::time::timeout(LOW_DIVERSITY_DHT_FALLBACK_TIMEOUT, self.dht.providers(cid))
            .await
        {
            Ok(Ok(dht_providers)) => {
                if let Some(stats) = stats {
                    stats.record_dht_result(dht_providers.len());
                }
                let dht_provider_count = dht_providers.len();
                let merged = merge_provider_lists(providers, dht_providers)?;
                tracing::info!(
                    phase = "provider_diversity_low",
                    cid = %cid,
                    provider_count = merged.len(),
                    dht_provider_count,
                    bitswap_provider_count = bitswap_provider_diversity(&merged),
                    fallback = "light_dht",
                    ok = true
                );
                Ok(merged)
            }
            Ok(Err(err)) => {
                if let Some(stats) = stats {
                    stats.record_dht_error();
                }
                tracing::info!(
                    phase = "provider_diversity_low",
                    cid = %cid,
                    bitswap_provider_count,
                    fallback = "light_dht",
                    ok = false,
                    error = %err
                );
                Ok(providers)
            }
            Err(_) => {
                if let Some(stats) = stats {
                    stats.record_dht_error();
                }
                tracing::info!(
                    phase = "dht_provider_lookup",
                    cid = %cid,
                    ok = false,
                    error = "low diversity DHT fallback timed out",
                    fallback = "light_dht",
                    cancelled = true,
                    max_providers = self.dht.max_providers,
                    timeout_ms = LOW_DIVERSITY_DHT_FALLBACK_TIMEOUT.as_millis(),
                    query_timeout_ms = self.dht.query_timeout.as_millis(),
                    elapsed_ms = dht_started.elapsed().as_millis()
                );
                tracing::info!(
                    phase = "provider_diversity_low",
                    cid = %cid,
                    bitswap_provider_count,
                    fallback = "light_dht",
                    ok = false,
                    timeout_ms = LOW_DIVERSITY_DHT_FALLBACK_TIMEOUT.as_millis()
                );
                Ok(providers)
            }
        }
    }

    async fn retry_empty_delegated_lookup(
        &self,
        cid: &Cid,
        stats: Option<&RoutingStatsHandle>,
    ) -> Option<Vec<Provider>> {
        let started = Instant::now();
        tokio::time::sleep(EMPTY_DELEGATED_PROVIDER_RETRY_DELAY).await;
        if let Some(stats) = stats {
            stats.record_delegated_lookup();
        }
        match self.delegated.providers(cid).await {
            Ok(providers) => {
                if let Some(stats) = stats {
                    stats.record_delegated_result(providers.len());
                }
                tracing::info!(
                    phase = "delegated_provider_empty_retry",
                    cid = %cid,
                    ok = true,
                    provider_count = providers.len(),
                    delay_ms = EMPTY_DELEGATED_PROVIDER_RETRY_DELAY.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                if providers.is_empty() {
                    None
                } else {
                    Some(providers)
                }
            }
            Err(err) => {
                if let Some(stats) = stats {
                    stats.record_delegated_error();
                }
                tracing::info!(
                    phase = "delegated_provider_empty_retry",
                    cid = %cid,
                    ok = false,
                    error = %err,
                    delay_ms = EMPTY_DELEGATED_PROVIDER_RETRY_DELAY.as_millis(),
                    elapsed_ms = started.elapsed().as_millis()
                );
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LightDhtClient {
    bootstrap_peers: Vec<String>,
    query_timeout: Duration,
    max_providers: usize,
}

impl Default for LightDhtClient {
    fn default() -> Self {
        Self {
            bootstrap_peers: DEFAULT_BOOTSTRAP_PEERS
                .iter()
                .map(|peer| (*peer).to_string())
                .collect(),
            query_timeout: DEFAULT_DHT_QUERY_TIMEOUT,
            max_providers: DEFAULT_MAX_DHT_PROVIDERS,
        }
    }
}

impl LightDhtClient {
    pub fn new(bootstrap_peers: Vec<String>) -> Self {
        Self {
            bootstrap_peers,
            ..Self::default()
        }
    }

    pub fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = if timeout.is_zero() {
            Duration::from_secs(1)
        } else {
            timeout
        };
        self
    }

    pub fn with_max_providers(mut self, max_providers: usize) -> Self {
        self.max_providers = max_providers.max(1);
        self
    }

    pub async fn providers(&self, cid: &Cid) -> Result<Vec<Provider>> {
        let started = Instant::now();
        let result = self.providers_inner(cid).await;
        match &result {
            Ok(providers) => tracing::info!(
                phase = "dht_provider_lookup",
                cid = %cid,
                ok = true,
                provider_count = providers.len(),
                max_providers = self.max_providers,
                timeout_ms = self.query_timeout.as_millis(),
                elapsed_ms = started.elapsed().as_millis()
            ),
            Err(err) => tracing::info!(
                phase = "dht_provider_lookup",
                cid = %cid,
                ok = false,
                error = %err,
                max_providers = self.max_providers,
                timeout_ms = self.query_timeout.as_millis(),
                elapsed_ms = started.elapsed().as_millis()
            ),
        }
        result
    }

    async fn providers_inner(&self, cid: &Cid) -> Result<Vec<Provider>> {
        let mut swarm = self.bootstrapped_swarm().await?;

        let key = kad::RecordKey::new(&cid.hash().to_bytes());
        let query_id = swarm.behaviour_mut().kad.get_providers(key.clone());
        let mut provider_ids = HashSet::new();
        let deadline = tokio::time::sleep(self.query_timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    break;
                }
                event = swarm.select_next_some() => {
                    let Some(kad::Event::OutboundQueryProgressed { id, result, .. }) = dht_event(event) else {
                        continue;
                    };
                    if id != query_id {
                        continue;
                    }
                    match result {
                        QueryResult::GetProviders(Ok(GetProvidersOk::FoundProviders { providers, .. })) => {
                            provider_ids.extend(providers);
                            if provider_ids.len() >= self.max_providers {
                                break;
                            }
                        }
                        QueryResult::GetProviders(Ok(GetProvidersOk::FinishedWithNoAdditionalRecord { .. })) => {
                            break;
                        }
                        QueryResult::GetProviders(Err(err)) => {
                            if provider_ids.is_empty() {
                                return Err(RoutingError::Dht(err.to_string()));
                            }
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        let providers = providers_from_dht(&mut swarm, &key, provider_ids, self.max_providers)?;
        resolve_missing_provider_addresses(&mut swarm, providers, self.query_timeout).await
    }

    pub async fn records(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut swarm = self.bootstrapped_swarm().await?;
        let key = kad::RecordKey::new(&key.to_vec());
        let query_id = swarm.behaviour_mut().kad.get_record(key);
        let deadline = tokio::time::sleep(self.query_timeout);
        tokio::pin!(deadline);
        let mut records = Vec::new();

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    if records.is_empty() {
                        return Err(RoutingError::Dht("DHT record lookup timed out".into()));
                    }
                    break;
                }
                event = swarm.select_next_some() => {
                    let Some(kad::Event::OutboundQueryProgressed { id, result, .. }) = dht_event(event) else {
                        continue;
                    };
                    if id != query_id {
                        continue;
                    }
                    match result {
                        QueryResult::GetRecord(Ok(GetRecordOk::FoundRecord(record))) => {
                            records.push(record.record.value);
                        }
                        QueryResult::GetRecord(Ok(GetRecordOk::FinishedWithNoAdditionalRecord { .. })) => {
                            break;
                        }
                        QueryResult::GetRecord(Err(GetRecordError::QuorumFailed { records: found_records, .. })) => {
                            records.extend(found_records.into_iter().map(|record| record.record.value));
                            break;
                        }
                        QueryResult::GetRecord(Err(GetRecordError::NotFound { .. })) => {
                            break;
                        }
                        QueryResult::GetRecord(Err(err)) => {
                            if records.is_empty() {
                                return Err(RoutingError::Dht(err.to_string()));
                            }
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(records)
    }

    async fn bootstrapped_swarm(&self) -> Result<libp2p::Swarm<DhtBehaviour>> {
        let mut swarm = build_dht_swarm(self.query_timeout).await?;
        let mut bootstrap_count = 0usize;
        for addr in &self.bootstrap_peers {
            let Some((peer, addr)) = parse_p2p_multiaddr(addr) else {
                tracing::debug!(addr, "ignoring invalid DHT bootstrap peer");
                continue;
            };
            swarm.behaviour_mut().kad.add_address(&peer, addr.clone());
            swarm.add_peer_address(peer, addr.clone());
            if let Ok(dial_addr) = addr.with_p2p(peer) {
                if let Err(err) = swarm.dial(dial_addr) {
                    tracing::debug!(peer = %peer, error = %err, "DHT bootstrap dial rejected");
                }
            }
            bootstrap_count += 1;
        }
        if bootstrap_count == 0 {
            return Err(RoutingError::Dht("no valid DHT bootstrap peers".into()));
        }
        Ok(swarm)
    }
}

#[derive(Debug, Clone)]
pub struct DhtIpnsResolver {
    dht: LightDhtClient,
}

impl Default for DhtIpnsResolver {
    fn default() -> Self {
        Self::new(LightDhtClient::default())
    }
}

impl DhtIpnsResolver {
    pub fn new(dht: LightDhtClient) -> Self {
        Self { dht }
    }
}

#[async_trait::async_trait]
impl IpnsResolver for DhtIpnsResolver {
    async fn resolve_ipns(&self, name: &str) -> freedom_ipfs_namesys::Result<IpnsRecord> {
        let key = ipns_dht_record_key(name)?;
        let records =
            self.dht.records(&key).await.map_err(|err| {
                NamesysError::NotFound(format!("DHT IPNS record for {name}: {err}"))
            })?;

        let mut best = None;
        for record in records {
            let Ok(record) = verify_ipns_record(name, &record) else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|best: &IpnsRecord| record.sequence > best.sequence)
            {
                best = Some(record);
            }
        }

        best.ok_or_else(|| NamesysError::NotFound(name.to_string()))
    }
}

fn bitswap_provider_diversity(providers: &[Provider]) -> usize {
    providers
        .iter()
        .filter(|provider| provider.id.is_some() && !provider.addrs.is_empty())
        .filter_map(provider_dedupe_key)
        .collect::<HashSet<_>>()
        .len()
}

fn merge_provider_lists(
    mut primary: Vec<Provider>,
    secondary: Vec<Provider>,
) -> Result<Vec<Provider>> {
    for provider in secondary {
        let Some(key) = provider_dedupe_key(&provider) else {
            continue;
        };
        if let Some(existing) = primary
            .iter_mut()
            .find(|existing| provider_dedupe_key(existing).as_deref() == Some(key.as_str()))
        {
            let mut addrs = existing.addrs.clone();
            addrs.extend(provider.addrs.clone());
            addrs.sort();
            addrs.dedup();
            *existing = Provider::from_parts(existing.id.clone().or(provider.id), addrs)?;
        } else {
            primary.push(provider);
        }
    }
    primary.truncate(MAX_DELEGATED_ROUTING_PROVIDERS);
    Ok(primary)
}

fn provider_dedupe_key(provider: &Provider) -> Option<String> {
    provider.id.clone().or_else(|| {
        (!provider.addrs.is_empty()).then(|| {
            let mut addrs = provider.addrs.clone();
            addrs.sort();
            addrs.join("|")
        })
    })
}

pub fn parse_provider_response(body: &str) -> Result<Vec<Provider>> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if trimmed.starts_with('{') && trimmed.contains("\"Providers\"") {
        if let Ok(response) = serde_json::from_str::<ProvidersResponse>(trimmed) {
            return response.into_providers();
        }
    }

    let mut providers = Vec::new();
    for line in trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        providers.extend(parse_provider_response_line(line)?);
    }
    Ok(providers)
}

fn parse_provider_response_line(line: &str) -> Result<Vec<Provider>> {
    if let Ok(provider) = serde_json::from_str::<ProviderRecord>(line) {
        if provider.id.is_some() || provider.addrs.is_some() {
            return provider.into_provider().map(|provider| vec![provider]);
        }
    }
    let response: ProvidersResponse =
        serde_json::from_str(line).map_err(|err| RoutingError::InvalidResponse(err.to_string()))?;
    response.into_providers()
}

fn limit_delegated_providers(mut providers: Vec<Provider>) -> Vec<Provider> {
    providers.truncate(MAX_DELEGATED_ROUTING_PROVIDERS);
    providers
}

#[derive(Debug)]
struct LimitedProviderResponse {
    providers: Vec<Provider>,
    stats: DelegatedResponseStats,
}

#[derive(Debug, Default)]
struct DelegatedResponseStats {
    bytes_read: usize,
    line_count: usize,
    first_chunk_elapsed: Option<Duration>,
    first_http_provider_elapsed: Option<Duration>,
    target_met_elapsed: Option<Duration>,
    target_kind: Option<&'static str>,
    target_returned_early: bool,
    http_provider_count: usize,
}

async fn limited_response_providers(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<LimitedProviderResponse> {
    limited_response_providers_with_direct_bitswap_target(
        response,
        max_bytes,
        streaming_delegated_direct_bitswap_target_enabled(),
        streaming_delegated_direct_bitswap_target_min_elapsed(),
    )
    .await
}

async fn limited_response_providers_with_direct_bitswap_target(
    response: reqwest::Response,
    max_bytes: usize,
    direct_bitswap_target_enabled: bool,
    direct_bitswap_target_min_elapsed: Duration,
) -> Result<LimitedProviderResponse> {
    let started = Instant::now();
    let started_tokio = tokio::time::Instant::now();
    let is_ndjson = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("application/x-ndjson"));

    if !is_ndjson {
        let body = limited_response_text(response, max_bytes).await?;
        let providers = limit_delegated_providers(parse_provider_response(&body)?);
        return Ok(LimitedProviderResponse {
            stats: DelegatedResponseStats {
                bytes_read: body.len(),
                line_count: body.lines().filter(|line| !line.trim().is_empty()).count(),
                http_provider_count: http_provider_url_count(&providers),
                ..Default::default()
            },
            providers,
        });
    }

    let mut stream = response.bytes_stream();
    let mut buffered = Vec::new();
    let mut bytes_read = 0usize;
    let mut line_count = 0usize;
    let mut providers = Vec::new();
    let mut first_chunk_elapsed = None;
    let mut first_http_provider_elapsed = None;
    let mut first_http_provider_deadline = None;
    let mut direct_bitswap_target_elapsed = None;
    let mut direct_bitswap_target_deadline = None;
    loop {
        let next_deadline =
            earliest_deadline(first_http_provider_deadline, direct_bitswap_target_deadline);
        let chunk = if let Some(deadline) = next_deadline {
            match tokio::time::timeout_at(deadline, stream.next()).await {
                Ok(chunk) => chunk,
                Err(_) => {
                    let direct_bitswap_target_ready = direct_bitswap_target_deadline
                        .is_some_and(|deadline| deadline <= tokio::time::Instant::now());
                    let providers = limit_delegated_providers(providers);
                    return Ok(LimitedProviderResponse {
                        stats: DelegatedResponseStats {
                            bytes_read,
                            line_count,
                            first_chunk_elapsed,
                            first_http_provider_elapsed,
                            target_met_elapsed: direct_bitswap_target_elapsed
                                .filter(|_| direct_bitswap_target_ready),
                            target_kind: direct_bitswap_target_ready.then_some("direct_bitswap"),
                            target_returned_early: direct_bitswap_target_ready,
                            http_provider_count: http_provider_url_count(&providers),
                        },
                        providers,
                    });
                }
            }
        } else {
            stream.next().await
        };
        let Some(chunk) = chunk else {
            break;
        };
        let chunk = chunk?;
        first_chunk_elapsed.get_or_insert_with(|| started.elapsed());
        bytes_read = bytes_read.saturating_add(chunk.len());
        if bytes_read > max_bytes {
            return Err(RoutingError::InvalidResponse(format!(
                "delegated routing response exceeded {max_bytes} bytes"
            )));
        }
        buffered.extend_from_slice(&chunk);

        while let Some(newline_index) = buffered.iter().position(|byte| *byte == b'\n') {
            let line = buffered.drain(..=newline_index).collect::<Vec<_>>();
            append_provider_response_line(&line, &mut providers)?;
            line_count = line_count.saturating_add(1);
            if first_http_provider_elapsed.is_none() && http_provider_url_count(&providers) > 0 {
                first_http_provider_elapsed = Some(started.elapsed());
                first_http_provider_deadline = Some(
                    tokio::time::Instant::now() + STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE,
                );
            }
            if should_return_streamed_providers(&providers) {
                let target_kind = streamed_provider_target_kind(&providers);
                let providers = limit_delegated_providers(providers);
                return Ok(LimitedProviderResponse {
                    stats: DelegatedResponseStats {
                        bytes_read,
                        line_count,
                        first_chunk_elapsed,
                        first_http_provider_elapsed,
                        target_met_elapsed: Some(started.elapsed()),
                        target_kind,
                        target_returned_early: true,
                        http_provider_count: http_provider_url_count(&providers),
                    },
                    providers,
                });
            }
            if direct_bitswap_target_enabled
                && direct_bitswap_target_deadline.is_none()
                && supported_direct_bitswap_provider_diversity(&providers)
                    >= STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET
            {
                direct_bitswap_target_elapsed = Some(started.elapsed());
                let grace_deadline =
                    tokio::time::Instant::now() + STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_GRACE;
                let min_elapsed_deadline = started_tokio + direct_bitswap_target_min_elapsed;
                direct_bitswap_target_deadline = Some(grace_deadline.max(min_elapsed_deadline));
            }
        }
    }

    append_provider_response_line(&buffered, &mut providers)?;
    if !trim_ascii_whitespace(&buffered).is_empty() {
        line_count = line_count.saturating_add(1);
    }
    if first_http_provider_elapsed.is_none() && http_provider_url_count(&providers) > 0 {
        first_http_provider_elapsed = Some(started.elapsed());
    }
    let direct_bitswap_target_met = direct_bitswap_target_elapsed.is_some();
    let providers = limit_delegated_providers(providers);
    Ok(LimitedProviderResponse {
        stats: DelegatedResponseStats {
            bytes_read,
            line_count,
            first_chunk_elapsed,
            first_http_provider_elapsed,
            target_met_elapsed: direct_bitswap_target_elapsed,
            target_kind: direct_bitswap_target_met.then_some("direct_bitswap"),
            target_returned_early: false,
            http_provider_count: http_provider_url_count(&providers),
        },
        providers,
    })
}

fn append_provider_response_line(line: &[u8], providers: &mut Vec<Provider>) -> Result<()> {
    let line = trim_ascii_whitespace(line);
    if line.is_empty() {
        return Ok(());
    }
    let line =
        std::str::from_utf8(line).map_err(|err| RoutingError::InvalidResponse(err.to_string()))?;
    providers.extend(parse_provider_response_line(line)?);
    Ok(())
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

fn should_return_streamed_providers(providers: &[Provider]) -> bool {
    streamed_provider_target_kind(providers).is_some()
}

fn streamed_provider_target_kind(providers: &[Provider]) -> Option<&'static str> {
    if http_provider_url_count(providers) >= STREAMING_DELEGATED_HTTP_PROVIDER_TARGET {
        Some("http_provider")
    } else if providers.len() >= MAX_DELEGATED_ROUTING_PROVIDERS {
        Some("max_providers")
    } else {
        None
    }
}

fn earliest_deadline(
    first: Option<tokio::time::Instant>,
    second: Option<tokio::time::Instant>,
) -> Option<tokio::time::Instant> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

fn streaming_delegated_direct_bitswap_target_enabled() -> bool {
    std::env::var_os(ENABLE_STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_ENV).is_some()
}

fn streaming_delegated_direct_bitswap_target_min_elapsed() -> Duration {
    streaming_delegated_direct_bitswap_target_min_elapsed_from_env_value(
        std::env::var_os(STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_MIN_ELAPSED_ENV)
            .as_deref()
            .and_then(|value| value.to_str()),
    )
}

fn streaming_delegated_direct_bitswap_target_min_elapsed_from_env_value(
    value: Option<&str>,
) -> Duration {
    value
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::ZERO)
}

fn lab_skip_low_diversity_dht_for_single_wss_enabled() -> bool {
    std::env::var_os(LAB_SKIP_LOW_DIVERSITY_DHT_FOR_SINGLE_WSS_ENV).is_some()
}

fn supported_direct_bitswap_provider_diversity(providers: &[Provider]) -> usize {
    providers
        .iter()
        .filter(|provider| {
            provider.addrs.iter().any(|addr| {
                supported_direct_bitswap_multiaddr(
                    addr,
                    provider.id.as_deref().and_then(parse_peer_id),
                )
            })
        })
        .filter_map(provider_dedupe_key)
        .collect::<HashSet<_>>()
        .len()
}

fn single_supported_wss_or_dnsaddr_bitswap_provider(providers: &[Provider]) -> bool {
    let mut provider_keys = HashSet::new();
    for provider in providers {
        let Some(key) = provider_dedupe_key(provider) else {
            continue;
        };
        let provider_peer = provider.id.as_deref().and_then(parse_peer_id);
        let mut has_supported_wss_or_dnsaddr = false;
        for addr in &provider.addrs {
            if bitswap_multiaddr_uses_dnsaddr(addr) {
                has_supported_wss_or_dnsaddr = true;
                continue;
            }
            if !supported_direct_bitswap_multiaddr(addr, provider_peer) {
                continue;
            }
            if !bitswap_multiaddr_uses_wss(addr) {
                return false;
            }
            has_supported_wss_or_dnsaddr = true;
        }
        if has_supported_wss_or_dnsaddr {
            provider_keys.insert(key);
        }
    }
    provider_keys.len() == 1
}

fn bitswap_multiaddr_uses_wss(addr: &str) -> bool {
    Multiaddr::from_str(addr).ok().is_some_and(|addr| {
        addr.iter()
            .any(|protocol| matches!(protocol, Protocol::Wss(_)))
    })
}

fn bitswap_multiaddr_uses_dnsaddr(addr: &str) -> bool {
    Multiaddr::from_str(addr).ok().is_some_and(|addr| {
        addr.iter()
            .any(|protocol| matches!(protocol, Protocol::Dnsaddr(_)))
    })
}

fn supported_direct_bitswap_multiaddr(addr: &str, provider_peer: Option<PeerId>) -> bool {
    let mut multiaddr = match Multiaddr::from_str(addr) {
        Ok(addr) => addr,
        Err(_) => return false,
    };
    let addr_peer = match multiaddr.iter().last() {
        Some(Protocol::P2p(peer)) => {
            multiaddr.pop();
            Some(peer)
        }
        _ => None,
    };
    if provider_peer.or(addr_peer).is_none() {
        return false;
    }

    let mut has_tcp = false;
    let mut has_udp = false;
    let mut has_quic = false;
    for protocol in multiaddr.iter() {
        match protocol {
            Protocol::Tcp(_) => has_tcp = true,
            Protocol::Udp(_) => has_udp = true,
            Protocol::Quic | Protocol::QuicV1 => has_quic = true,
            Protocol::Http | Protocol::Https => return false,
            Protocol::P2pCircuit => return false,
            Protocol::WebTransport => return false,
            Protocol::WebRTC | Protocol::WebRTCDirect | Protocol::P2pWebRtcDirect => return false,
            Protocol::Certhash(_) => return false,
            _ => {}
        }
    }

    has_tcp || (has_udp && has_quic)
}

fn http_provider_url_count(providers: &[Provider]) -> usize {
    providers
        .iter()
        .map(|provider| provider.http_urls.len())
        .sum()
}

async fn limited_response_text(response: reqwest::Response, max_bytes: usize) -> Result<String> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(RoutingError::InvalidResponse(format!(
                "delegated routing response exceeded {max_bytes} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|err| RoutingError::InvalidResponse(err.to_string()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProvidersResponse {
    providers: Option<Vec<ProviderRecord>>,
}

impl ProvidersResponse {
    fn into_providers(self) -> Result<Vec<Provider>> {
        self.providers
            .unwrap_or_default()
            .into_iter()
            .map(ProviderRecord::into_provider)
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ProviderRecord {
    #[serde(rename = "ID", alias = "Id")]
    id: Option<String>,
    addrs: Option<Vec<String>>,
}

impl ProviderRecord {
    fn into_provider(self) -> Result<Provider> {
        Provider::from_parts(self.id, self.addrs.unwrap_or_default())
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct DhtBehaviour {
    kad: kad::Behaviour<MemoryStore>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    limits: connection_limits::Behaviour,
}

async fn build_dht_swarm(query_timeout: Duration) -> Result<libp2p::Swarm<DhtBehaviour>> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            (tls::Config::new, noise::Config::new),
            yamux::Config::default,
        )
        .map_err(|err| RoutingError::Dht(err.to_string()))?
        .with_quic()
        // Avoid libp2p's system DNS path: iOS devices do not expose a
        // Unix-style /etc/resolv.conf, and bootstrap/provider addresses often
        // arrive as /dnsaddr multiaddrs.
        .with_dns_config(
            libp2p::dns::ResolverConfig::cloudflare(),
            libp2p::dns::ResolverOpts::default(),
        )
        .with_behaviour(move |key| {
            let peer_id = key.public().to_peer_id();
            let store = MemoryStore::new(peer_id);
            let mut config = kad::Config::new(kad::PROTOCOL_NAME);
            config.set_query_timeout(query_timeout);
            config.set_periodic_bootstrap_interval(None);
            let mut behaviour = kad::Behaviour::with_config(peer_id, store, config);
            behaviour.set_mode(Some(kad::Mode::Client));
            DhtBehaviour {
                kad: behaviour,
                identify: identify::Behaviour::new(identify::Config::new(
                    format!("freedom-ipfs/{}", env!("CARGO_PKG_VERSION")),
                    key.public(),
                )),
                ping: ping::Behaviour::new(ping::Config::new()),
                limits: connection_limits::Behaviour::new(dht_connection_limits()),
            }
        })
        .map_err(|err| RoutingError::Dht(err.to_string()))
        .map(|builder| {
            builder
                .with_swarm_config(|cfg| {
                    cfg.with_idle_connection_timeout(DHT_IDLE_CONNECTION_TIMEOUT)
                })
                .with_connection_timeout(DHT_CONNECTION_TIMEOUT)
                .build()
        })
}

fn providers_from_dht(
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    key: &kad::RecordKey,
    provider_ids: HashSet<PeerId>,
    max_providers: usize,
) -> Result<Vec<Provider>> {
    let records = swarm.behaviour_mut().kad.store_mut().providers(key);
    let mut providers = Vec::new();
    for peer_id in provider_ids.into_iter().take(max_providers) {
        let mut addrs = records
            .iter()
            .filter(|record| record.provider == peer_id)
            .flat_map(|record| record.addresses.iter().cloned())
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            addrs = peer_addresses_from_kbuckets(&mut swarm.behaviour_mut().kad, &peer_id);
        }
        providers.push(Provider::from_parts(
            Some(peer_id.to_string()),
            addrs.into_iter().map(|addr| addr.to_string()).collect(),
        )?);
    }
    Ok(providers)
}

fn dht_connection_limits() -> connection_limits::ConnectionLimits {
    connection_limits::ConnectionLimits::default()
        .with_max_pending_outgoing(Some(DHT_MAX_PENDING_OUTGOING_CONNECTIONS))
        .with_max_established_outgoing(Some(DHT_MAX_ESTABLISHED_CONNECTIONS))
        .with_max_established(Some(DHT_MAX_ESTABLISHED_CONNECTIONS))
        .with_max_established_per_peer(Some(1))
}

fn dht_event(event: SwarmEvent<DhtBehaviourEvent>) -> Option<kad::Event> {
    match event {
        SwarmEvent::Behaviour(DhtBehaviourEvent::Kad(event)) => Some(event),
        _ => None,
    }
}

async fn resolve_missing_provider_addresses(
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    mut providers: Vec<Provider>,
    query_timeout: Duration,
) -> Result<Vec<Provider>> {
    let mut pending = Vec::new();
    for (index, provider) in providers.iter().enumerate() {
        if !provider.addrs.is_empty() {
            continue;
        }
        let Some(peer) = provider.id.as_deref().and_then(parse_peer_id) else {
            continue;
        };
        let query_id = swarm.behaviour_mut().kad.get_closest_peers(peer.to_bytes());
        pending.push((query_id, peer, index));
    }

    if pending.is_empty() {
        providers.retain(|provider| !provider.addrs.is_empty());
        return Ok(providers);
    }

    let deadline = tokio::time::sleep(query_timeout);
    tokio::pin!(deadline);
    while !pending.is_empty() {
        tokio::select! {
            _ = &mut deadline => break,
            event = swarm.select_next_some() => {
                let Some(kad::Event::OutboundQueryProgressed { id, result, .. }) = dht_event(event) else {
                    continue;
                };
                let Some(pos) = pending.iter().position(|(query_id, _, _)| *query_id == id) else {
                    continue;
                };
                let (_, target_peer, provider_index) = pending.swap_remove(pos);
                let peer_info = match result {
                    QueryResult::GetClosestPeers(Ok(ok)) => ok
                        .peers
                        .into_iter()
                        .find(|peer| peer.peer_id == target_peer),
                    QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout { peers, .. })) => peers
                        .into_iter()
                        .find(|peer| peer.peer_id == target_peer),
                    _ => None,
                };
                let Some(peer_info) = peer_info else {
                    continue;
                };
                providers[provider_index] = Provider::from_parts(
                    Some(target_peer.to_string()),
                    peer_info
                        .addrs
                        .into_iter()
                        .map(|addr| addr.to_string())
                        .collect(),
                )?;
            }
        }
    }

    providers.retain(|provider| !provider.addrs.is_empty());
    Ok(providers)
}

fn peer_addresses_from_kbuckets(
    behaviour: &mut kad::Behaviour<MemoryStore>,
    peer_id: &PeerId,
) -> Vec<Multiaddr> {
    let mut addrs = Vec::new();
    for bucket in behaviour.kbuckets() {
        for entry in bucket.iter() {
            if entry.node.key.preimage() == peer_id {
                addrs.extend(entry.node.value.iter().cloned());
            }
        }
    }
    addrs
}

fn parse_peer_id(id: &str) -> Option<PeerId> {
    PeerId::from_str(id).ok()
}

fn parse_p2p_multiaddr(addr: &str) -> Option<(PeerId, Multiaddr)> {
    let mut multiaddr = Multiaddr::from_str(addr).ok()?;
    let peer = match multiaddr.iter().last()? {
        Protocol::P2p(peer) => peer,
        _ => return None,
    };
    multiaddr.pop();
    Some((peer, multiaddr))
}

fn http_url_from_multiaddr(addr: &str) -> Result<Option<Url>> {
    let parts: Vec<&str> = addr.split('/').filter(|part| !part.is_empty()).collect();
    let Some(http_pos) = parts
        .iter()
        .position(|part| *part == "http" || *part == "https")
    else {
        return Ok(None);
    };
    let scheme =
        if parts[http_pos] == "https" || parts.get(http_pos.wrapping_sub(1)) == Some(&"tls") {
            "https"
        } else {
            "http"
        };

    let host = parts
        .windows(2)
        .find_map(|pair| match pair[0] {
            "dns" | "dns4" | "dns6" | "ip4" | "ip6" => Some(pair[1]),
            _ => None,
        })
        .ok_or_else(|| RoutingError::InvalidProviderUrl(addr.to_string()))?;
    let port = parts.windows(2).find_map(|pair| {
        if pair[0] == "tcp" {
            Some(pair[1])
        } else {
            None
        }
    });

    let url = if let Some(port) = port {
        format!("{scheme}://{host}:{port}")
    } else {
        format!("{scheme}://{host}")
    };
    Url::parse(&url)
        .map(Some)
        .map_err(|_| RoutingError::InvalidProviderUrl(addr.to_string()))
}

fn timeout_http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .build()
        .expect("delegated routing HTTP client config is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipld_core::ipld::Ipld;
    use libp2p_identity::Keypair;
    use multihash::Multihash;
    use prost::Message;
    use std::collections::BTreeMap;

    const TEST_IPNS_VALIDITY: &str = "2126-01-01T00:00:00.000000000Z";
    const IPNS_SIGNATURE_PREFIX: &[u8] = b"ipns-signature:";
    const LIBP2P_KEY_CODEC: u64 = 0x72;

    #[test]
    fn extracts_tls_http_provider_urls() {
        let body =
            r#"{"Providers":[{"ID":"peer","Addrs":["/dns4/example.com/tcp/443/tls/http"]}]}"#;
        let providers = parse_provider_response(body).unwrap();
        assert_eq!(providers[0].id.as_deref(), Some("peer"));
        assert_eq!(providers[0].http_urls[0].as_str(), "https://example.com/");
    }

    #[test]
    fn lab_drop_http_providers_for_cid_clears_only_matching_http_urls() {
        let cid = Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[1; 32]).unwrap());
        let other = Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[2; 32]).unwrap());
        let providers = vec![
            Provider::from_parts(
                Some("http-peer".into()),
                vec!["/dns4/example.com/tcp/443/tls/http".into()],
            )
            .unwrap(),
            Provider::from_parts(
                Some("bitswap-peer".into()),
                vec!["/ip4/127.0.0.1/tcp/4001".into()],
            )
            .unwrap(),
        ];

        assert!(!lab_drop_http_providers_for_cid_from_env_value(&cid, None));
        assert!(!lab_drop_http_providers_for_cid_from_env_value(
            &cid,
            Some(&other.to_string())
        ));
        assert!(lab_drop_http_providers_for_cid_from_env_value(
            &cid,
            Some(&format!("  {other}, {}  ", cid))
        ));
        assert!(lab_drop_http_providers_for_cid_from_env_value(
            &cid,
            Some("*")
        ));

        let unchanged = lab_drop_http_providers_for_cid_with_env_value(
            &cid,
            providers.clone(),
            Some(&other.to_string()),
        );
        assert_eq!(unchanged[0].http_urls.len(), 1);
        assert_eq!(unchanged[0].addrs.len(), 1);

        let filtered =
            lab_drop_http_providers_for_cid_with_env_value(&cid, providers, Some(&cid.to_string()));
        assert!(filtered[0].http_urls.is_empty());
        assert_eq!(filtered[0].addrs[0], "/dns4/example.com/tcp/443/tls/http");
        assert!(filtered[1].http_urls.is_empty());
        assert_eq!(filtered[1].addrs[0], "/ip4/127.0.0.1/tcp/4001");
    }

    #[test]
    fn parses_ndjson_peer_records_with_uppercase_id() {
        let body = r#"{"Addrs":["/ip4/164.92.225.198/tcp/4001"],"ID":"12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP","Schema":"peer"}"#;
        let providers = parse_provider_response(body).unwrap();
        assert_eq!(
            providers[0].id.as_deref(),
            Some("12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP")
        );
        assert_eq!(providers[0].addrs[0], "/ip4/164.92.225.198/tcp/4001");
    }

    #[test]
    fn delegated_routing_normalizes_cidv0_to_cidv1_base32() {
        let hash = Multihash::<64>::wrap(0x12, &[0u8; 32]).unwrap();
        let cidv0 = Cid::new_v0(hash).unwrap();

        let lookup = delegated_lookup_cid(&cidv0);

        assert!(lookup.starts_with("bafy"), "{lookup}");
        assert_ne!(lookup, cidv0.to_string());
        let reparsed = lookup.parse::<Cid>().unwrap();
        assert_eq!(reparsed.codec(), cidv0.codec());
        assert_eq!(reparsed.hash(), cidv0.hash());
    }

    #[tokio::test]
    async fn delegated_routing_races_multiple_endpoints_until_success() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (bad_endpoint, bad_task) = spawn_delegated_response("not-json").await;
        let (empty_endpoint, empty_task) = spawn_delegated_response(r#"{"Providers":[]}"#).await;
        let (good_endpoint, good_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer","Addrs":["/dns4/example.com/tcp/443/tls/http"]}]}"#,
        )
        .await;

        let providers =
            DelegatedRoutingClient::with_endpoints([bad_endpoint, empty_endpoint, good_endpoint])
                .providers(&cid)
                .await
                .unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some("peer"));
        assert_eq!(providers[0].http_urls[0].as_str(), "https://example.com/");
        for task in [bad_task, empty_task, good_task] {
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn delegated_routing_merges_low_diversity_endpoint_results() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (first_endpoint, first_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer-a","Addrs":["/ip4/127.0.0.1/tcp/4001"]}]}"#,
        )
        .await;
        let (second_endpoint, second_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer-b","Addrs":["/ip4/127.0.0.2/tcp/4001"]}]}"#,
        )
        .await;

        let providers = DelegatedRoutingClient::with_endpoints([first_endpoint, second_endpoint])
            .providers(&cid)
            .await
            .unwrap();

        assert_eq!(providers.len(), 2);
        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("peer-a")));
        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("peer-b")));
        assert_eq!(bitswap_provider_diversity(&providers), 2);
        for task in [first_task, second_task] {
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn delegated_routing_returns_single_low_diversity_result_when_others_empty() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (first_endpoint, first_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer-a","Addrs":["/ip4/127.0.0.1/tcp/4001"]}]}"#,
        )
        .await;
        let (empty_endpoint, empty_task) = spawn_delegated_response(r#"{"Providers":[]}"#).await;

        let providers = DelegatedRoutingClient::with_endpoints([first_endpoint, empty_endpoint])
            .providers(&cid)
            .await
            .unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some("peer-a"));
        for task in [first_task, empty_task] {
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn delegated_routing_low_diversity_merge_wait_is_bounded() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (first_endpoint, first_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer-a","Addrs":["/ip4/127.0.0.1/tcp/4001"]}]}"#,
        )
        .await;
        let (slow_endpoint, slow_task) = spawn_delayed_delegated_response(
            r#"{"Providers":[{"ID":"peer-b","Addrs":["/ip4/127.0.0.2/tcp/4001"]}]}"#,
            Duration::from_secs(5),
        )
        .await;

        let started = std::time::Instant::now();
        let providers = DelegatedRoutingClient::with_endpoints([first_endpoint, slow_endpoint])
            .providers(&cid)
            .await
            .unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some("peer-a"));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "low-diversity delegated merge wait should stay bounded"
        );
        for task in [first_task, slow_task] {
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn delegated_routing_self_hedges_slow_single_endpoint() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (endpoint, task, request_count) = spawn_sequence_delegated_responses_with_delays(vec![
            (
                r#"{"Providers":[{"ID":"slow-peer","Addrs":["/dns4/slow.example/tcp/443/tls/http"]}]}"#,
                SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER + Duration::from_millis(300),
            ),
            (
                r#"{"Providers":[{"ID":"fast-peer","Addrs":["/dns4/fast.example/tcp/443/tls/http"]}]}"#,
                Duration::ZERO,
            ),
        ])
        .await;

        let started = std::time::Instant::now();
        let providers = DelegatedRoutingClient::new(endpoint)
            .providers(&cid)
            .await
            .unwrap();

        assert!(
            started.elapsed()
                < SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER + Duration::from_millis(250),
            "delegated self hedge should return before the first slow response"
        );
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some("fast-peer"));
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            2,
            "slow single endpoint should receive a duplicate request"
        );
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn observed_delegated_routing_records_provider_lookup_stats() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (endpoint, task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer","Addrs":["/dns4/example.com/tcp/443/tls/http"]}]}"#,
        )
        .await;
        let stats = RoutingStatsHandle::default();
        let client = ProviderRoutingClient::from(DelegatedRoutingClient::new(endpoint))
            .with_stats(stats.clone());

        let providers = client.providers(&cid).await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 1,
                delegated_provider_results: 1,
                delegated_provider_errors: 0,
                dht_provider_lookups: 0,
                dht_provider_results: 0,
                dht_provider_errors: 0,
            }
        );
        task.abort();
        let _ = task.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_routing_augments_low_delegated_diversity_with_dht() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let delegated_peer = "peer";
        let (dht_peer, dht_addr, dht_task) = spawn_local_dht_provider(cid).await;
        let dht_peer_string = dht_peer.to_string();
        let (endpoint, delegated_task) = spawn_delegated_response_owned(format!(
            r#"{{"Providers":[{{"ID":"{delegated_peer}","Addrs":["/ip4/127.0.0.1/tcp/4101"]}}]}}"#
        ))
        .await;
        let stats = RoutingStatsHandle::default();
        let dht = LightDhtClient::new(vec![format!("{dht_addr}/p2p/{dht_peer}")])
            .with_query_timeout(Duration::from_secs(5))
            .with_max_providers(1);
        let client = ProviderRoutingClient::from(AutoRoutingClient::new(
            DelegatedRoutingClient::new(endpoint),
            dht,
        ))
        .with_stats(stats.clone());

        let providers = client.providers(&cid).await.unwrap();

        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some(delegated_peer)));
        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some(dht_peer_string.as_str())));
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 1,
                delegated_provider_results: 1,
                delegated_provider_errors: 0,
                dht_provider_lookups: 1,
                dht_provider_results: 1,
                dht_provider_errors: 0,
            }
        );
        delegated_task.abort();
        dht_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_routing_bounds_low_diversity_dht_fallback_timeout() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (endpoint, delegated_task) = spawn_delegated_response(
            r#"{"Providers":[{"ID":"peer-a","Addrs":["/ip4/127.0.0.1/tcp/4101"]}]}"#,
        )
        .await;
        let dht = LightDhtClient::new(vec![
            "/ip4/203.0.113.1/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
                .to_string(),
        ])
        .with_query_timeout(Duration::from_secs(5))
        .with_max_providers(1);
        let stats = RoutingStatsHandle::default();
        let client = ProviderRoutingClient::from(AutoRoutingClient::new(
            DelegatedRoutingClient::new(endpoint),
            dht,
        ))
        .with_stats(stats.clone());

        let started = std::time::Instant::now();
        let providers = client.providers(&cid).await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some("peer-a"));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "low-diversity DHT fallback should use the short fallback cap"
        );
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 1,
                delegated_provider_results: 1,
                delegated_provider_errors: 0,
                dht_provider_lookups: 1,
                dht_provider_results: 0,
                dht_provider_errors: 1,
            }
        );
        delegated_task.abort();
        let _ = delegated_task.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_routing_lab_skips_low_diversity_dht_for_single_wss_provider() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let peer = Keypair::generate_ed25519().public().to_peer_id();
        let peer_string = peer.to_string();
        let delegated_providers = vec![Provider::from_parts(
            Some(peer_string.clone()),
            vec!["/dnsaddr/bitswap-v3.pinata.cloud".to_string()],
        )
        .unwrap()];
        let dht = LightDhtClient::new(vec![
            "/ip4/203.0.113.1/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
                .to_string(),
        ])
        .with_query_timeout(Duration::from_secs(5))
        .with_max_providers(1);
        let stats = RoutingStatsHandle::default();
        let client = AutoRoutingClient::new(DelegatedRoutingClient::new("http://127.0.0.1:1"), dht);

        let providers = client
            .providers_after_delegated_results_inner(&cid, delegated_providers, Some(&stats), true)
            .await
            .unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some(peer_string.as_str()));
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 0,
                delegated_provider_results: 0,
                delegated_provider_errors: 0,
                dht_provider_lookups: 0,
                dht_provider_results: 0,
                dht_provider_errors: 0,
            }
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn auto_routing_retries_empty_delegated_result_before_dht() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (endpoint, delegated_task, request_count) = spawn_sequence_delegated_responses(vec![
            r#"{"Providers":[]}"#,
            r#"{"Providers":[{"ID":"peer-a","Addrs":["/ip4/127.0.0.1/tcp/4101"]},{"ID":"peer-b","Addrs":["/ip4/127.0.0.2/tcp/4101"]}]}"#,
        ])
        .await;
        let stats = RoutingStatsHandle::default();
        let dht = LightDhtClient::new(vec![
            "/ip4/203.0.113.1/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
                .to_string(),
        ])
        .with_query_timeout(Duration::from_secs(5))
        .with_max_providers(1);
        let client = ProviderRoutingClient::from(AutoRoutingClient::new(
            DelegatedRoutingClient::new(endpoint),
            dht,
        ))
        .with_stats(stats.clone());

        let providers = client.providers(&cid).await.unwrap();

        assert_eq!(providers.len(), 2);
        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("peer-a")));
        assert!(providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("peer-b")));
        assert_eq!(request_count.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 2,
                delegated_provider_results: 2,
                delegated_provider_errors: 0,
                dht_provider_lookups: 0,
                dht_provider_results: 0,
                dht_provider_errors: 0,
            }
        );
        delegated_task.abort();
        let _ = delegated_task.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn observed_light_dht_records_provider_lookup_stats() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let stats = RoutingStatsHandle::default();
        let client = ProviderRoutingClient::from(
            LightDhtClient::new(Vec::new()).with_query_timeout(Duration::from_millis(1)),
        )
        .with_stats(stats.clone());

        let err = client.providers(&cid).await.unwrap_err();

        assert!(matches!(err, RoutingError::Dht(_)));
        assert_eq!(
            stats.snapshot(),
            RoutingStats {
                delegated_provider_lookups: 0,
                delegated_provider_results: 0,
                delegated_provider_errors: 0,
                dht_provider_lookups: 1,
                dht_provider_results: 0,
                dht_provider_errors: 1,
            }
        );
    }

    #[test]
    fn rejects_malformed_delegated_provider_responses() {
        assert!(matches!(
            parse_provider_response("not-json").unwrap_err(),
            RoutingError::InvalidResponse(_)
        ));
        assert!(matches!(
            parse_provider_response(r#"{"Providers":[{"Addrs":["/tcp/443/tls/http"]}]}"#)
                .unwrap_err(),
            RoutingError::InvalidProviderUrl(_)
        ));
    }

    #[test]
    fn caps_delegated_provider_records() {
        let providers = (0..(MAX_DELEGATED_ROUTING_PROVIDERS + 8))
            .map(|index| {
                Provider::from_parts(
                    Some(format!("peer-{index}")),
                    vec![format!("/ip4/127.0.0.1/tcp/{}", 4000 + index)],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        let capped = limit_delegated_providers(providers);

        assert_eq!(capped.len(), MAX_DELEGATED_ROUTING_PROVIDERS);
        assert_eq!(capped[0].id.as_deref(), Some("peer-0"));
        let last_expected = format!("peer-{}", MAX_DELEGATED_ROUTING_PROVIDERS - 1);
        assert_eq!(
            capped[MAX_DELEGATED_ROUTING_PROVIDERS - 1].id.as_deref(),
            Some(last_expected.as_str())
        );
    }

    #[tokio::test]
    async fn delegated_routing_returns_after_enough_streamed_http_providers() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let fast_head = (0..STREAMING_DELEGATED_HTTP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"peer-{index}","Addrs":["/dns4/provider-{index}.example/tcp/443/tls/http"]}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let slow_tail = r#"{"ID":"late-peer","Addrs":["/dns4/late.example/tcp/443/tls/http"]}"#;
        let (endpoint, task) =
            spawn_streaming_delegated_response(fast_head, slow_tail.into(), Duration::from_secs(2))
                .await;

        let providers = tokio::time::timeout(
            Duration::from_millis(500),
            DelegatedRoutingClient::new(endpoint).providers(&cid),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(providers.len(), STREAMING_DELEGATED_HTTP_PROVIDER_TARGET);
        assert_eq!(
            providers[0].http_urls[0].as_str(),
            "https://provider-0.example/"
        );
        assert!(!providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("late-peer")));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn streamed_delegated_response_reports_response_stats() {
        let fast_head = (0..STREAMING_DELEGATED_HTTP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"peer-{index}","Addrs":["/dns4/provider-{index}.example/tcp/443/tls/http"]}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let slow_tail = r#"{"ID":"late-peer","Addrs":["/dns4/late.example/tcp/443/tls/http"]}"#;
        let (endpoint, task) =
            spawn_streaming_delegated_response(fast_head, slow_tail.into(), Duration::from_secs(2))
                .await;
        let response = reqwest::get(format!("{endpoint}/providers/test"))
            .await
            .unwrap();

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            limited_response_providers(response, MAX_DELEGATED_ROUTING_RESPONSE_BYTES),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            response.providers.len(),
            STREAMING_DELEGATED_HTTP_PROVIDER_TARGET
        );
        assert_eq!(
            response.stats.http_provider_count,
            STREAMING_DELEGATED_HTTP_PROVIDER_TARGET
        );
        assert_eq!(
            response.stats.line_count,
            STREAMING_DELEGATED_HTTP_PROVIDER_TARGET
        );
        assert!(response.stats.bytes_read > 0);
        assert!(response.stats.first_chunk_elapsed.is_some());
        assert!(response.stats.first_http_provider_elapsed.is_some());
        assert!(response.stats.target_met_elapsed.is_some());
        assert!(response.stats.target_returned_early);
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn streamed_delegated_response_returns_after_first_http_provider_grace() {
        let fast_head = r#"{"ID":"peer-0","Addrs":["/dns4/provider-0.example/tcp/443/tls/http"]}"#
            .to_string()
            + "\n";
        let slow_tail = r#"{"ID":"late-peer","Addrs":["/dns4/late.example/tcp/443/tls/http"]}"#;
        let (endpoint, task) =
            spawn_streaming_delegated_response(fast_head, slow_tail.into(), Duration::from_secs(2))
                .await;
        let response = reqwest::get(format!("{endpoint}/providers/test"))
            .await
            .unwrap();

        let response = tokio::time::timeout(
            Duration::from_millis(800),
            limited_response_providers(response, MAX_DELEGATED_ROUTING_RESPONSE_BYTES),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.providers.len(), 1);
        assert_eq!(response.providers[0].id.as_deref(), Some("peer-0"));
        assert_eq!(response.stats.http_provider_count, 1);
        assert_eq!(response.stats.line_count, 1);
        assert!(response.stats.first_http_provider_elapsed.is_some());
        assert!(response.stats.target_met_elapsed.is_none());
        assert!(!response.stats.target_returned_early);
        assert!(!response
            .providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("late-peer")));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn streamed_delegated_response_can_return_after_direct_bitswap_target() {
        let fast_head = (0..STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"{}","Addrs":["/ip4/127.0.0.{}/tcp/{}"]}}"#,
                    Keypair::generate_ed25519().public().to_peer_id(),
                    index + 1,
                    4100 + index
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let slow_tail = r#"{"ID":"late-peer","Addrs":["/dns4/late.example/tcp/443/tls/http"]}"#;
        let (endpoint, task) =
            spawn_streaming_delegated_response(fast_head, slow_tail.into(), Duration::from_secs(2))
                .await;
        let response = reqwest::get(format!("{endpoint}/providers/test"))
            .await
            .unwrap();

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            limited_response_providers_with_direct_bitswap_target(
                response,
                MAX_DELEGATED_ROUTING_RESPONSE_BYTES,
                true,
                Duration::ZERO,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            supported_direct_bitswap_provider_diversity(&response.providers),
            STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET
        );
        assert_eq!(response.stats.target_kind, Some("direct_bitswap"));
        assert!(response.stats.target_met_elapsed.is_some());
        assert!(response.stats.target_returned_early);
        assert_eq!(response.stats.http_provider_count, 0);
        assert!(!response
            .providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("late-peer")));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn direct_bitswap_target_waits_for_http_tail_inside_grace() {
        let fast_head = (0..STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"{}","Addrs":["/ip4/127.0.0.{}/tcp/{}"]}}"#,
                    Keypair::generate_ed25519().public().to_peer_id(),
                    index + 1,
                    4100 + index
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let http_tail = (0..STREAMING_DELEGATED_HTTP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"http-peer-{index}","Addrs":["/dns4/provider-{index}.example/tcp/443/tls/http"]}}"#
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let (endpoint, task) = spawn_streaming_delegated_response(
            fast_head,
            http_tail,
            Duration::from_millis(
                (STREAMING_DELEGATED_DIRECT_BITSWAP_TARGET_GRACE.as_millis() as u64) / 2,
            ),
        )
        .await;
        let response = reqwest::get(format!("{endpoint}/providers/test"))
            .await
            .unwrap();

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            limited_response_providers_with_direct_bitswap_target(
                response,
                MAX_DELEGATED_ROUTING_RESPONSE_BYTES,
                true,
                Duration::ZERO,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            response.stats.http_provider_count,
            STREAMING_DELEGATED_HTTP_PROVIDER_TARGET
        );
        assert_eq!(response.stats.target_kind, Some("http_provider"));
        assert!(response.stats.target_returned_early);
        assert!(response
            .providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("http-peer-0")));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn direct_bitswap_target_can_wait_for_min_elapsed_floor() {
        let fast_head = (0..STREAMING_DELEGATED_DIRECT_BITSWAP_PROVIDER_TARGET)
            .map(|index| {
                format!(
                    r#"{{"ID":"{}","Addrs":["/ip4/127.0.0.{}/tcp/{}"]}}"#,
                    Keypair::generate_ed25519().public().to_peer_id(),
                    index + 1,
                    4100 + index
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let slow_tail = r#"{"ID":"late-peer","Addrs":["/dns4/late.example/tcp/443/tls/http"]}"#;
        let (endpoint, task) =
            spawn_streaming_delegated_response(fast_head, slow_tail.into(), Duration::from_secs(1))
                .await;
        let response = reqwest::get(format!("{endpoint}/providers/test"))
            .await
            .unwrap();

        let min_elapsed = Duration::from_millis(250);
        let started = Instant::now();
        let response = tokio::time::timeout(
            Duration::from_millis(700),
            limited_response_providers_with_direct_bitswap_target(
                response,
                MAX_DELEGATED_ROUTING_RESPONSE_BYTES,
                true,
                min_elapsed,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(
            started.elapsed() >= Duration::from_millis(200),
            "direct Bitswap target should wait for the configured floor before returning"
        );
        assert_eq!(response.stats.target_kind, Some("direct_bitswap"));
        assert!(response.stats.target_returned_early);
        assert!(!response
            .providers
            .iter()
            .any(|provider| provider.id.as_deref() == Some("late-peer")));
        task.abort();
        let _ = task.await;
    }

    #[test]
    fn direct_bitswap_target_ignores_http_relay_and_peerless_addrs() {
        let peer = Keypair::generate_ed25519().public().to_peer_id();
        let providers = vec![
            Provider::from_parts(
                Some(peer.to_string()),
                vec![
                    "/dns4/provider.example/tcp/443/tls/http".to_string(),
                    "/ip4/127.0.0.1/tcp/4001/p2p-circuit".to_string(),
                    "/ip4/127.0.0.1/udp/4001/quic-v1/webtransport".to_string(),
                    "/ip4/127.0.0.1/tcp/4001".to_string(),
                ],
            )
            .unwrap(),
            Provider::from_parts(None, vec!["/ip4/127.0.0.2/tcp/4002".to_string()]).unwrap(),
        ];

        assert_eq!(supported_direct_bitswap_provider_diversity(&providers), 1);
    }

    #[test]
    fn detects_single_supported_wss_or_dnsaddr_bitswap_provider() {
        let peer = Keypair::generate_ed25519().public().to_peer_id();
        let peer_two = Keypair::generate_ed25519().public().to_peer_id();
        let single_wss = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec!["/dns4/bitswap-v3.pinata.cloud/tcp/443/wss".to_string()],
        )
        .unwrap()];
        assert!(single_supported_wss_or_dnsaddr_bitswap_provider(
            &single_wss
        ));

        let single_dnsaddr = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec!["/dnsaddr/bitswap-v3.pinata.cloud".to_string()],
        )
        .unwrap()];
        assert!(single_supported_wss_or_dnsaddr_bitswap_provider(
            &single_dnsaddr
        ));

        let mixed_wss_and_tcp = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec![
                "/dns4/bitswap-v3.pinata.cloud/tcp/443/wss".to_string(),
                "/ip4/127.0.0.1/tcp/4001".to_string(),
            ],
        )
        .unwrap()];
        assert!(!single_supported_wss_or_dnsaddr_bitswap_provider(
            &mixed_wss_and_tcp
        ));

        let two_wss = vec![
            Provider::from_parts(
                Some(peer.to_string()),
                vec!["/dns4/bitswap-a.example/tcp/443/wss".to_string()],
            )
            .unwrap(),
            Provider::from_parts(
                Some(peer_two.to_string()),
                vec!["/dns4/bitswap-b.example/tcp/443/wss".to_string()],
            )
            .unwrap(),
        ];
        assert!(!single_supported_wss_or_dnsaddr_bitswap_provider(&two_wss));

        let http_only = vec![Provider::from_parts(
            Some(peer.to_string()),
            vec!["/dns4/provider.example/tcp/443/tls/http".to_string()],
        )
        .unwrap()];
        assert!(!single_supported_wss_or_dnsaddr_bitswap_provider(
            &http_only
        ));
    }

    #[test]
    fn direct_bitswap_target_min_elapsed_parses_override() {
        assert_eq!(
            streaming_delegated_direct_bitswap_target_min_elapsed_from_env_value(None),
            Duration::ZERO
        );
        assert_eq!(
            streaming_delegated_direct_bitswap_target_min_elapsed_from_env_value(Some("250")),
            Duration::from_millis(250)
        );
        assert_eq!(
            streaming_delegated_direct_bitswap_target_min_elapsed_from_env_value(Some("bad")),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn rejects_oversized_delegated_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let response =
                b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\nconnection: close\r\n\r\n12345678";
            tokio::io::AsyncWriteExt::write_all(&mut stream, response)
                .await
                .unwrap();
        });

        let response = reqwest::get(format!("http://{addr}/routing/v1/providers/test"))
            .await
            .unwrap();
        let err = limited_response_providers(response, 4).await.unwrap_err();

        assert!(matches!(err, RoutingError::InvalidResponse(_)));
        task.await.unwrap();
    }

    async fn spawn_delegated_response(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        spawn_delegated_response_owned(body.to_string()).await
    }

    async fn spawn_delayed_delegated_response(
        body: &'static str,
        delay: Duration,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = body.to_string();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request).await;
            tokio::time::sleep(delay).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                .await
                .unwrap();
        });
        (format!("http://{addr}/routing/v1"), task)
    }

    async fn spawn_delegated_response_owned(body: String) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                .await
                .unwrap();
        });
        (format!("http://{addr}/routing/v1"), task)
    }

    async fn spawn_streaming_delegated_response(
        fast_head: String,
        slow_tail: String,
        tail_delay: Duration,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request).await;
            let response_head = b"HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\nconnection: close\r\n\r\n";
            tokio::io::AsyncWriteExt::write_all(&mut stream, response_head)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut stream, fast_head.as_bytes())
                .await
                .unwrap();
            tokio::time::sleep(tail_delay).await;
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, slow_tail.as_bytes()).await;
        });
        (format!("http://{addr}/routing/v1"), task)
    }

    async fn spawn_sequence_delegated_responses(
        bodies: Vec<&'static str>,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicU64>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let task_request_count = request_count.clone();
        let bodies = bodies.into_iter().map(str::to_string).collect::<Vec<_>>();
        let task = tokio::spawn(async move {
            for body in bodies {
                let (mut stream, _) = listener.accept().await.unwrap();
                task_request_count.fetch_add(1, Ordering::Relaxed);
                let mut request = vec![0u8; 4096];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        (format!("http://{addr}/routing/v1"), task, request_count)
    }

    async fn spawn_sequence_delegated_responses_with_delays(
        responses: Vec<(&'static str, Duration)>,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        Arc<std::sync::atomic::AtomicU64>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let task_request_count = request_count.clone();
        let responses = responses
            .into_iter()
            .map(|(body, delay)| (body.to_string(), delay))
            .collect::<std::collections::VecDeque<_>>();
        let responses = Arc::new(tokio::sync::Mutex::new(responses));
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let Some((body, delay)) = responses.lock().await.pop_front() else {
                    break;
                };
                let request_count = task_request_count.clone();
                tokio::spawn(async move {
                    request_count.fetch_add(1, Ordering::Relaxed);
                    let mut request = vec![0u8; 4096];
                    let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request).await;
                    tokio::time::sleep(delay).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ =
                        tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}/routing/v1"), task, request_count)
    }

    #[test]
    fn parses_dht_bootstrap_multiaddr() {
        let (peer, addr) = parse_p2p_multiaddr(
            "/dnsaddr/ny5.bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
        )
        .unwrap();
        assert_eq!(
            peer.to_string(),
            "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa"
        );
        assert_eq!(addr.to_string(), "/dnsaddr/ny5.bootstrap.libp2p.io");
    }

    #[test]
    fn configures_max_dht_providers_with_floor() {
        assert_eq!(
            LightDhtClient::default()
                .with_max_providers(4)
                .max_providers,
            4
        );
        assert_eq!(
            LightDhtClient::default()
                .with_max_providers(0)
                .max_providers,
            1
        );
    }

    #[test]
    fn configures_dht_query_timeout_with_floor() {
        assert_eq!(
            LightDhtClient::default()
                .with_query_timeout(Duration::from_secs(7))
                .query_timeout,
            Duration::from_secs(7)
        );
        assert_eq!(
            LightDhtClient::default()
                .with_query_timeout(Duration::ZERO)
                .query_timeout,
            Duration::from_secs(1)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn light_dht_swarm_is_client_mode_and_has_no_listeners() {
        let swarm = build_dht_swarm(Duration::from_secs(5)).await.unwrap();

        assert!(matches!(swarm.behaviour().kad.mode(), kad::Mode::Client));
        assert_eq!(swarm.listeners().count(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn light_dht_finds_provider_from_local_server_peer() {
        let cid = "bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u"
            .parse::<Cid>()
            .unwrap();
        let (peer_id, addr, swarm_task) = spawn_local_dht_provider(cid).await;
        let bootstrap = format!("{addr}/p2p/{peer_id}");

        let providers = LightDhtClient::new(vec![bootstrap])
            .with_query_timeout(Duration::from_secs(5))
            .with_max_providers(1)
            .providers(&cid)
            .await
            .unwrap();

        let expected_peer_id = peer_id.to_string();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id.as_deref(), Some(expected_peer_id.as_str()));
        assert!(providers[0].addrs.iter().any(|provider_addr| {
            provider_addr == &addr.to_string() || provider_addr.starts_with("/ip4/127.0.0.1/tcp/")
        }));
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dht_ipns_resolver_reads_verified_record_from_local_server_peer() {
        let value = "/ipfs/bafkqaddwgevxmmraojswg33smq";
        let (name, record) = signed_ipns_record(value);
        let (peer_id, addr, swarm_task) = spawn_local_dht_record(&name, record).await;
        let bootstrap = format!("{addr}/p2p/{peer_id}");
        let resolver = DhtIpnsResolver::new(
            LightDhtClient::new(vec![bootstrap]).with_query_timeout(Duration::from_secs(5)),
        );

        let resolved = resolver.resolve_ipns(&name).await.unwrap();

        assert_eq!(resolved.value, value);
        assert_eq!(resolved.sequence, 7);
        swarm_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "network smoke test against the public Amino DHT; set FREEDOM_IPFS_LIVE_DHT_CID"]
    async fn live_light_dht_finds_public_providers() {
        let Ok(cid) = std::env::var("FREEDOM_IPFS_LIVE_DHT_CID") else {
            eprintln!(
                "skipping public DHT smoke; set FREEDOM_IPFS_LIVE_DHT_CID to a CID with known Amino DHT providers"
            );
            return;
        };
        let cid = cid.parse::<Cid>().unwrap();
        let providers = LightDhtClient::default()
            .with_query_timeout(Duration::from_secs(30))
            .providers(&cid)
            .await
            .unwrap();
        eprintln!("DHT found {} providers for {cid}", providers.len());
        for provider in &providers {
            eprintln!(
                "provider {} addrs={:?}",
                provider.id.as_deref().unwrap_or("<unknown>"),
                provider.addrs
            );
        }
        assert!(!providers.is_empty());
    }

    fn signed_ipns_record(value: &str) -> (String, Vec<u8>) {
        let keypair = Keypair::generate_ed25519();
        let public = keypair.public();
        let name = Cid::new_v1(
            LIBP2P_KEY_CODEC,
            Multihash::<64>::from_bytes(&public.to_peer_id().to_bytes()).unwrap(),
        )
        .to_string();
        let data = ipns_data(value);
        let mut signed = IPNS_SIGNATURE_PREFIX.to_vec();
        signed.extend_from_slice(&data);
        let signature = keypair.sign(&signed).unwrap();

        let entry = TestIpnsEntry {
            signature_v2: Some(signature),
            data: Some(data),
            ..TestIpnsEntry::default()
        };
        (name, entry.encode_to_vec())
    }

    fn ipns_data(value: &str) -> Vec<u8> {
        let mut map = BTreeMap::new();
        map.insert("Sequence".to_string(), Ipld::Integer(7));
        map.insert("TTL".to_string(), Ipld::Integer(300_000_000_000));
        map.insert(
            "Validity".to_string(),
            Ipld::Bytes(TEST_IPNS_VALIDITY.as_bytes().to_vec()),
        );
        map.insert("ValidityType".to_string(), Ipld::Integer(0));
        map.insert("Value".to_string(), Ipld::Bytes(value.as_bytes().to_vec()));
        serde_ipld_dagcbor::to_vec(&Ipld::Map(map)).unwrap()
    }

    async fn spawn_local_dht_provider(
        cid: Cid,
    ) -> (PeerId, Multiaddr, tokio::task::JoinHandle<()>) {
        let mut swarm = build_local_dht_server().await;
        let peer_id = *swarm.local_peer_id();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                break address;
            }
        };
        let key = kad::RecordKey::new(&cid.hash().to_bytes());
        let provider = kad::ProviderRecord::new(key, peer_id, vec![addr.clone()]);
        swarm
            .behaviour_mut()
            .store_mut()
            .add_provider(provider)
            .unwrap();

        let task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        (peer_id, addr, task)
    }

    async fn spawn_local_dht_record(
        name: &str,
        value: Vec<u8>,
    ) -> (PeerId, Multiaddr, tokio::task::JoinHandle<()>) {
        let mut swarm = build_local_dht_server().await;
        let peer_id = *swarm.local_peer_id();
        swarm
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let addr = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                break address;
            }
        };
        let key = kad::RecordKey::new(&ipns_dht_record_key(name).unwrap());
        swarm
            .behaviour_mut()
            .store_mut()
            .put(kad::Record::new(key, value))
            .unwrap();

        let task = tokio::spawn(async move {
            loop {
                let _ = swarm.select_next_some().await;
            }
        });
        (peer_id, addr, task)
    }

    async fn build_local_dht_server() -> libp2p::Swarm<kad::Behaviour<MemoryStore>> {
        SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|key| {
                let peer_id = key.public().to_peer_id();
                let store = MemoryStore::new(peer_id);
                let mut config = kad::Config::new(kad::PROTOCOL_NAME);
                config.set_query_timeout(Duration::from_secs(5));
                config.set_periodic_bootstrap_interval(None);
                let mut behaviour = kad::Behaviour::with_config(peer_id, store, config);
                behaviour.set_mode(Some(kad::Mode::Server));
                behaviour
            })
            .unwrap()
            .build()
    }

    #[derive(Clone, PartialEq, Message)]
    struct TestIpnsEntry {
        #[prost(bytes = "vec", optional, tag = "1")]
        value: Option<Vec<u8>>,
        #[prost(bytes = "vec", optional, tag = "2")]
        signature_v1: Option<Vec<u8>>,
        #[prost(enumeration = "TestValidityType", optional, tag = "3")]
        validity_type: Option<i32>,
        #[prost(bytes = "vec", optional, tag = "4")]
        validity: Option<Vec<u8>>,
        #[prost(uint64, optional, tag = "5")]
        sequence: Option<u64>,
        #[prost(uint64, optional, tag = "6")]
        ttl: Option<u64>,
        #[prost(bytes = "vec", optional, tag = "7")]
        pub_key: Option<Vec<u8>>,
        #[prost(bytes = "vec", optional, tag = "8")]
        signature_v2: Option<Vec<u8>>,
        #[prost(bytes = "vec", optional, tag = "9")]
        data: Option<Vec<u8>>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
    #[repr(i32)]
    enum TestValidityType {
        Eol = 0,
    }
}
