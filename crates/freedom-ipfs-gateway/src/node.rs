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

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use axum::Router;
use freedom_ipfs_core::BlockProvider;
use freedom_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsResolver, NameResolver,
};
use freedom_ipfs_retrieval::{CancellationToken, FetchingBlockProvider, InstanceContext};
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
        self.build_with(None)
    }

    /// Wires the node as an owned [`Instance`] on a runtime of its own (see
    /// [`Instance`]). This starts the runtime's threads, and nothing else.
    pub fn build_instance(self, config: InstanceConfig) -> std::io::Result<Instance> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads.max(1))
            .max_blocking_threads(config.max_blocking_threads.max(1))
            .thread_name(config.thread_name)
            .enable_all()
            .build()?;
        let context = InstanceContext::new(runtime.handle().clone());
        let node = self.build_with(Some(context.clone()));
        Ok(Instance {
            runtime: OwnedRuntime {
                runtime: Some(runtime),
                context: context.clone(),
            },
            context,
            node,
        })
    }

    fn build_with(self, instance: Option<InstanceContext>) -> OnlineNode {
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
        let mut provider =
            FetchingBlockProvider::new(store.clone(), routing.with_stats(routing_stats.clone()));
        if let Some(instance) = instance {
            provider = provider.with_instance(instance);
        }
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

/// The runtime of an [`Instance`].
#[derive(Clone, Debug)]
pub struct InstanceConfig {
    /// Its threads' name. The OS shows at most 15 bytes of it.
    pub thread_name: String,
    /// Its worker threads: by default the core count, at most 8.
    pub worker_threads: usize,
    /// The cap on its blocking pool's threads: 512 by default. Every block
    /// fetch in flight holds one.
    pub max_blocking_threads: usize,
}

impl Default for InstanceConfig {
    fn default() -> Self {
        Self {
            thread_name: "freedom-ipfs".into(),
            worker_threads: std::thread::available_parallelism().map_or(1, |n| n.get().min(8)),
            max_blocking_threads: 512,
        }
    }
}

/// An owned node: everything it runs, on a runtime of its own, so it can be
/// torn down and its end proven (design 0007, "Instances and their
/// teardown", upstream item 7).
///
/// - **Requests** run with [`Instance::spawn_request`], each under its own
///   token, a child of the instance's root ([`Instance::request_token`]).
///   Cancelling the token drops the request at its next await; a fetch
///   blocked inside the synchronous block provider ends too, because every
///   blocking region observes the token.
/// - **Its task set** holds every request and every task the node spawns
///   for itself, the shared Bitswap swarm's included. A light-DHT query's
///   swarm is no task of its own: it is driven inside the lookup, so it ends
///   with the request. The tasks the libraries spawn (hyper's connections,
///   libp2p's), and the gateway's HTML prefetch and warm tasks (off by
///   default), aren't in the set but run on the same runtime.
/// - **[`Instance::shutdown`]** cancels everything, waits for the task set
///   and the blocking regions, shuts the runtime down, which drops every
///   task still on it and closes its sockets, and closes the store. It
///   reports whether quiescence was reached. Cancelling alone isn't enough:
///   the libraries' tasks keep pooled and dialled connections open after
///   every request has ended, and only the runtime's shutdown ends them.
///
/// Dropping an instance without shutting it down cancels it and lets its
/// runtime wind down in the background, proving nothing.
pub struct Instance {
    runtime: OwnedRuntime,
    context: InstanceContext,
    node: OnlineNode,
}

/// The instance's runtime. Dropped without [`Instance::shutdown`], it
/// cancels the instance and lets the runtime wind down in the background:
/// dropping a runtime that way never blocks, so it is safe anywhere.
struct OwnedRuntime {
    runtime: Option<Runtime>,
    context: InstanceContext,
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        self.context.cancel();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// What [`Instance::shutdown`] proved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quiescence {
    /// Nothing of the instance runs any more: no task, no blocking region,
    /// and the store is closed.
    pub reached: bool,
    /// Tasks still running after the runtime's shutdown: stuck in a poll.
    pub tasks_left: usize,
    /// Blocking regions still running after the runtime's shutdown.
    pub blocking_left: usize,
    /// Tasks that hadn't ended by themselves at the deadline, so the
    /// runtime's shutdown dropped them.
    pub tasks_dropped: usize,
    /// Whether the store closed in time.
    pub store_closed: bool,
}

/// How long [`Instance::shutdown`] waits for a statement in progress before
/// it gives up closing the store (as `close_or_close_later` does).
const STORE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

impl Instance {
    /// The node.
    pub fn node(&self) -> &OnlineNode {
        &self.node
    }

    /// The gateway over the node.
    pub fn gateway_core(&self) -> GatewayCore {
        self.node.gateway_core()
    }

    /// The instance's group: its runtime, its tokens, its task set and its
    /// blocking regions.
    pub fn context(&self) -> &InstanceContext {
        &self.context
    }

    /// A new request's token: a child of the instance's root.
    pub fn request_token(&self) -> CancellationToken {
        self.context.request_token()
    }

    /// Runs a request on the instance's runtime, in its task set, under
    /// `token`: the request ends when `token` or the instance is cancelled,
    /// with `None`. A host reads a response body inside the same request.
    pub fn spawn_request<F>(
        &self,
        token: CancellationToken,
        future: F,
    ) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.context.spawn_request(token, future)
    }

    /// Runs a task of the instance on its runtime, in its task set, until the
    /// instance is cancelled.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.context.spawn(future)
    }

    /// Tears the instance down and reports whether quiescence was reached,
    /// within `timeout` (design 0007's teardown, steps 3 and 4):
    ///
    /// 1. cancels the root token, and so every request's, and closes the
    ///    task set;
    /// 2. waits until the task set is empty and no blocking region runs;
    /// 3. drops the node (its clients and caches) and shuts the runtime
    ///    down: every task still on it is dropped, the libraries' connection
    ///    tasks included, and their sockets are closed;
    /// 4. closes the store, waiting at most 2 s for a statement in progress.
    ///
    /// Step 2 of the design's teardown, revoking the network capability,
    /// comes with the network policy (upstream item 3).
    ///
    /// It blocks: call it from a thread that may block, such as a
    /// `spawn_blocking` job, never from an async task.
    pub fn shutdown(self, timeout: Duration) -> Quiescence {
        let deadline = Instant::now() + timeout;
        let Instance {
            mut runtime,
            context,
            node,
        } = self;
        context.cancel();
        let runtime = runtime.runtime.take();
        if let Some(runtime) = &runtime {
            let context = context.clone();
            runtime.block_on(async move {
                let _ = tokio::time::timeout_at(deadline.into(), context.wait_tasks()).await;
            });
        }
        context.wait_blocking_regions(deadline);
        let tasks_dropped = context.tasks();
        let store = node.store().clone();
        drop(node);
        if let Some(runtime) = runtime {
            runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        }
        let tasks_left = context.tasks();
        let blocking_left = context.blocking_regions();
        let close_timeout = deadline
            .saturating_duration_since(Instant::now())
            .min(STORE_CLOSE_TIMEOUT);
        let store_closed = store.close(close_timeout);
        Quiescence {
            reached: tasks_left == 0 && blocking_left == 0 && store_closed,
            tasks_left,
            blocking_left,
            tasks_dropped,
            store_closed,
        }
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
