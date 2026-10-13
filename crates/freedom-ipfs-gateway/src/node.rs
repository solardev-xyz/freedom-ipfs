//! The online node, built in one place: its routing, its block provider, its
//! name resolver, and the gateway over them.
//!
//! The mobile FFI crate and the gateway binary used to wire these by hand,
//! each its own way. [`OnlineNode::builder`] is that wiring as a library
//! function, so the add-on, the binary and any other host build the same
//! node from the same settings:
//!
//! ```no_run
//! # use freedom_ipfs_gateway::{GatewayConfig, OnlineNode, RoutingMode};
//! # use freedom_ipfs_store::SqliteBlockStore;
//! # use std::time::Duration;
//! # fn main() -> anyhow::Result<()> {
//! let store = SqliteBlockStore::in_memory(256 * 1024 * 1024)?;
//! let node = OnlineNode::builder(store)
//!     .routing_mode(RoutingMode::Auto)
//!     .delegated_routers(["https://delegated-ipfs.dev/routing/v1"])
//!     .dht_query_timeout(Duration::from_secs(3))
//!     .gateway_config(GatewayConfig::new(8))
//!     .build();
//! let gateway = node.gateway_core();
//! # Ok(()) }
//! ```
//!
//! Building opens nothing: no socket, no task. Routing, retrieval and name
//! resolution start their work on the first request.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use freedom_ipfs_core::BlockProvider;
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

use crate::{
    router_with_provider_and_name_resolver_config, GatewayConfig, GatewayCore,
    PersistentNameResolver,
};

/// How a node finds providers and IPNS records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RoutingMode {
    /// Delegated routing first, then the light DHT; IPNS likewise.
    #[default]
    Auto,
    /// Delegated routing only.
    Delegated,
    /// The light DHT only.
    LightDht,
    /// The cache only: no provider lookups, and names resolve from the
    /// store's records alone.
    Offline,
}

/// The settings of an [`OnlineNode`], with today's defaults.
pub struct OnlineNodeBuilder {
    store: SqliteBlockStore,
    routing_mode: RoutingMode,
    delegated_routers: Vec<String>,
    dht_query_timeout: Option<Duration>,
    dht_max_providers: Option<usize>,
    gateway_config: GatewayConfig,
}

impl OnlineNodeBuilder {
    /// The routing mode; [`RoutingMode::Auto`] by default.
    pub fn routing_mode(mut self, routing_mode: RoutingMode) -> Self {
        self.routing_mode = routing_mode;
        self
    }

    /// The delegated routers, raced and failed over in order;
    /// [`DEFAULT_DELEGATED_ROUTER`] alone by default.
    pub fn delegated_routers<I, S>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.delegated_routers = endpoints.into_iter().map(Into::into).collect();
        self
    }

    /// The light DHT's query timeout; the light DHT's own default unless set.
    pub fn dht_query_timeout(mut self, timeout: Duration) -> Self {
        self.dht_query_timeout = Some(timeout);
        self
    }

    /// The most providers a light-DHT lookup collects; the light DHT's own
    /// default unless set.
    pub fn dht_max_providers(mut self, max_providers: usize) -> Self {
        self.dht_max_providers = Some(max_providers);
        self
    }

    /// The gateway's configuration; [`GatewayConfig::default`] unless set.
    pub fn gateway_config(mut self, config: GatewayConfig) -> Self {
        self.gateway_config = config;
        self
    }

    /// Wires the node. This opens no socket and starts no task.
    pub fn build(self) -> OnlineNode {
        let Self {
            store,
            routing_mode,
            delegated_routers,
            dht_query_timeout,
            dht_max_providers,
            gateway_config,
        } = self;
        let routing_stats = RoutingStatsHandle::default();
        let (routing, name_resolver): (ProviderRoutingClient, Arc<dyn NameResolver>) =
            match routing_mode {
                RoutingMode::Offline => (
                    ProviderRoutingClient::Offline,
                    Arc::new(PersistentNameResolver::cache_only(store.clone())),
                ),
                online => {
                    let mut dht = LightDhtClient::default();
                    if let Some(timeout) = dht_query_timeout {
                        dht = dht.with_query_timeout(timeout);
                    }
                    if let Some(max_providers) = dht_max_providers {
                        dht = dht.with_max_providers(max_providers);
                    }
                    let delegated =
                        DelegatedRoutingClient::with_endpoints(delegated_routers.clone());
                    let (routing, ipns): (ProviderRoutingClient, Arc<dyn IpnsResolver>) =
                        match online {
                            RoutingMode::Delegated => (
                                ProviderRoutingClient::from(delegated),
                                Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers)),
                            ),
                            RoutingMode::LightDht => (
                                ProviderRoutingClient::from(dht.clone()),
                                Arc::new(DhtIpnsResolver::new(dht)),
                            ),
                            _ => (
                                ProviderRoutingClient::from(AutoRoutingClient::new(
                                    delegated,
                                    dht.clone(),
                                )),
                                Arc::new(FallbackIpnsResolver::new(
                                    DelegatedIpnsResolver::with_endpoints(delegated_routers),
                                    DhtIpnsResolver::new(dht),
                                )),
                            ),
                        };
                    let name_resolver = CachedNameResolver::new(PersistentNameResolver::new(
                        DefaultNameResolver::new(CloudflareDohResolver::default(), ipns),
                        store.clone(),
                    ));
                    (routing, Arc::new(name_resolver))
                }
            };
        let provider =
            FetchingBlockProvider::new(store.clone(), routing.with_stats(routing_stats.clone()));
        let block_provider: Arc<dyn BlockProvider> = Arc::new(provider.clone());
        OnlineNode {
            store,
            routing_mode,
            provider,
            block_provider,
            name_resolver,
            routing_stats,
            gateway_config,
        }
    }
}

/// A wired node: one block provider and one name resolver, shared by every
/// gateway built from it.
#[derive(Clone)]
pub struct OnlineNode {
    store: SqliteBlockStore,
    routing_mode: RoutingMode,
    provider: FetchingBlockProvider,
    block_provider: Arc<dyn BlockProvider>,
    name_resolver: Arc<dyn NameResolver>,
    routing_stats: RoutingStatsHandle,
    gateway_config: GatewayConfig,
}

impl OnlineNode {
    /// A builder for a node over `store`, with today's defaults: routing
    /// [`RoutingMode::Auto`] through [`DEFAULT_DELEGATED_ROUTER`], the light
    /// DHT's own settings, and [`GatewayConfig::default`].
    pub fn builder(store: SqliteBlockStore) -> OnlineNodeBuilder {
        OnlineNodeBuilder {
            store,
            routing_mode: RoutingMode::Auto,
            delegated_routers: vec![DEFAULT_DELEGATED_ROUTER.to_string()],
            dht_query_timeout: None,
            dht_max_providers: None,
            gateway_config: GatewayConfig::default(),
        }
    }

    /// The gateway over the node's provider and name resolver, as the native
    /// request API serves it.
    pub fn gateway_core(&self) -> GatewayCore {
        GatewayCore::with_provider_and_name_resolver_config(
            self.block_provider.clone(),
            self.name_resolver.clone(),
            self.gateway_config.clone(),
        )
    }

    /// The same gateway as an HTTP router, for a host that serves it on a
    /// local socket.
    pub fn router(&self) -> Router {
        router_with_provider_and_name_resolver_config(
            self.block_provider.clone(),
            self.name_resolver.clone(),
            self.gateway_config.clone(),
        )
    }

    /// The retrieving block provider, for its statistics.
    pub fn provider(&self) -> &FetchingBlockProvider {
        &self.provider
    }

    /// The block provider the gateways read through.
    pub fn block_provider(&self) -> Arc<dyn BlockProvider> {
        self.block_provider.clone()
    }

    /// The name resolver the gateways resolve through.
    pub fn name_resolver(&self) -> Arc<dyn NameResolver> {
        self.name_resolver.clone()
    }

    /// The routing lookup counters.
    pub fn routing_stats(&self) -> &RoutingStatsHandle {
        &self.routing_stats
    }

    /// The node's store.
    pub fn store(&self) -> &SqliteBlockStore {
        &self.store
    }

    /// The routing mode it was built with.
    pub fn routing_mode(&self) -> RoutingMode {
        self.routing_mode
    }

    /// The gateway configuration it was built with.
    pub fn gateway_config(&self) -> &GatewayConfig {
        &self.gateway_config
    }
}

/// Parses a comma-separated list of delegated routers, as the add-on and the
/// gateway binary take it: each trimmed, empty entries dropped.
pub fn delegated_router_endpoints(delegated_routers: &str) -> Vec<String> {
    delegated_routers
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GatewayCoreRequest;
    use axum::body::to_bytes;
    use axum::http::{HeaderMap, Method, StatusCode};
    use freedom_ipfs_core::{cid_from_data, CODEC_RAW};

    fn store_with(data: &[u8]) -> (SqliteBlockStore, String) {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        (store, cid.to_string())
    }

    async fn get(core: &GatewayCore, path: &str) -> (StatusCode, Vec<u8>) {
        let response = core
            .handle(GatewayCoreRequest::ipfs(
                path,
                Method::GET,
                HeaderMap::new(),
            ))
            .await;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, body.to_vec())
    }

    #[test]
    fn defaults_are_todays() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let builder = OnlineNode::builder(store);
        assert_eq!(builder.routing_mode, RoutingMode::Auto);
        assert_eq!(builder.delegated_routers, [DEFAULT_DELEGATED_ROUTER]);
        assert_eq!(builder.dht_query_timeout, None);
        assert_eq!(builder.dht_max_providers, None);
        let config = GatewayConfig::default();
        assert_eq!(
            builder.gateway_config.max_concurrent_requests(),
            config.max_concurrent_requests()
        );
        assert_eq!(
            builder.gateway_config.request_queue_timeout(),
            config.request_queue_timeout()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_mode_serves_cached_blocks_through_one_provider() {
        for mode in [
            RoutingMode::Auto,
            RoutingMode::Delegated,
            RoutingMode::LightDht,
            RoutingMode::Offline,
        ] {
            let (store, cid) = store_with(b"cached bytes");
            let node = OnlineNode::builder(store)
                .routing_mode(mode)
                .delegated_routers(["http://127.0.0.1:9/routing/v1"])
                .dht_query_timeout(Duration::from_millis(1))
                .dht_max_providers(1)
                .build();
            assert_eq!(node.routing_mode(), mode);
            let (status, body) = get(&node.gateway_core(), &cid).await;
            assert_eq!(
                (status, body.as_slice()),
                (StatusCode::OK, &b"cached bytes"[..]),
                "{mode:?}"
            );
            // The gateway and the node share the provider: its reads count.
            assert!(node.provider().stats().cache_hits > 0, "{mode:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offline_mode_never_looks_anything_up() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let missing = cid_from_data(CODEC_RAW, b"not here");
        let node = OnlineNode::builder(store)
            .routing_mode(RoutingMode::Offline)
            .build();
        let (status, _) = get(&node.gateway_core(), &missing.to_string()).await;
        assert!(!status.is_success(), "{status}");
        let stats = node.routing_stats().snapshot();
        assert_eq!(
            (stats.delegated_provider_lookups, stats.dht_provider_lookups),
            (0, 0)
        );
        let response = node
            .gateway_core()
            .handle(GatewayCoreRequest::ipns(
                "example.com",
                Method::GET,
                HeaderMap::new(),
            ))
            .await;
        assert!(!response.status().is_success());
    }

    #[test]
    fn delegated_routers_parse_as_the_hosts_take_them() {
        assert_eq!(
            delegated_router_endpoints(" https://one.example/routing/v1, ,https://two.example "),
            ["https://one.example/routing/v1", "https://two.example"]
        );
        assert!(delegated_router_endpoints(" , ").is_empty());
    }
}
