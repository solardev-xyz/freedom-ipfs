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
//!
//! A host that runs several nodes in one process gives each builder a clone
//! of one [`SharedLimits`] ([`OnlineNodeBuilder::shared_limits`]), so their
//! block fetches, HTTP provider requests, Bitswap connections and DHT queries
//! count against one set of process-wide limits. Each node also bounds its
//! own background work ([`OnlineNode::background_queue`]).
//!
//! Its caches have fixed capacities, which together are its fixed
//! reservation ([`OnlineNodeBuilder::fixed_reservation`]): the store's hot
//! cache, and the small-body and UnixFS metadata caches that all of its
//! gateways share. An [`Instance`] built with a [`MemoryBudget`] takes that
//! reservation from it before it starts, and gives it back when its teardown
//! ends.

use std::future::Future;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use axum::Router;
use freedom_ipfs_core::{BlockProvider, BudgetError, Reservation};
use freedom_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsResolver, NameResolver,
};
use freedom_ipfs_retrieval::instance::Fenced;
use freedom_ipfs_retrieval::{
    BackgroundQueue, CancellationToken, FetchingBlockProvider, InstanceContext, MemoryBudget,
    SharedLimits,
};
use freedom_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, RoutingStatsHandle, DEFAULT_DELEGATED_ROUTER,
};
use freedom_ipfs_store::SqliteBlockStore;

use crate::{
    router_with_state, GatewayCaches, GatewayConfig, GatewayCore, GatewayState,
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
    shared_limits: Option<SharedLimits>,
    memory_budget: Option<MemoryBudget>,
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

    /// Process-wide limits the node shares with every other node built with
    /// clones of `limits` (design 0007, "Budgets"): block fetches, HTTP
    /// provider requests, Bitswap connections and DHT queries. The node's
    /// own limits still apply inside them. Unless set, the node has none of
    /// these limits.
    pub fn shared_limits(mut self, limits: SharedLimits) -> Self {
        self.shared_limits = Some(limits);
        self
    }

    /// A process-wide memory budget the node shares with every other node
    /// built with clones of `budget` (design 0007, "Budgets"): every receive
    /// buffer of its fetches reserves from it before it is allocated, and
    /// the block keeps the charge until its last owner drops it. Request
    /// work waits for memory; background work fails at once. Unless set,
    /// the node reserves nothing.
    ///
    /// An [`Instance`] also takes its fixed reservation from it
    /// ([`OnlineNodeBuilder::fixed_reservation`]) before it starts, and
    /// fails to start if that doesn't fit. A node built with
    /// [`OnlineNodeBuilder::build`] takes none: its host reserves it.
    pub fn memory_budget(mut self, budget: MemoryBudget) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// What the node's caches can hold, which is fixed (design 0007,
    /// "Budgets"): the store's hot cache
    /// ([`SqliteBlockStore::hot_cache_capacity`]) and its gateways' caches
    /// ([`GatewayConfig::cache_capacity`]), which all of its gateways share,
    /// so they count once. Each cache stays within its capacity. An
    /// [`Instance`] built with a memory budget reserves this much before it
    /// starts.
    pub fn fixed_reservation(&self) -> usize {
        fixed_reservation(&self.store, &self.gateway_config)
    }

    /// Wires the node. This opens no socket and starts no task.
    pub fn build(self) -> OnlineNode {
        self.build_with(None)
    }

    /// Wires the node as an owned [`Instance`] on a runtime of its own (see
    /// [`Instance`]). This starts the runtime's threads, and nothing else.
    ///
    /// With a memory budget ([`OnlineNodeBuilder::memory_budget`]), it first
    /// reserves the node's fixed reservation
    /// ([`OnlineNodeBuilder::fixed_reservation`]) from the budget, without
    /// waiting: if that doesn't fit, nothing is started and it fails with
    /// [`InstanceStartError::FixedReservation`]. The reservation is held
    /// until the instance's teardown proves quiescence
    /// ([`Instance::begin_shutdown`]) **and** the last of the cache memory
    /// it stands for is freed: a cache a retained node or gateway still
    /// holds, a block or body a reader still holds. Without a budget it
    /// reserves nothing.
    pub fn build_instance(self, config: InstanceConfig) -> Result<Instance, InstanceStartError> {
        let fixed = match &self.memory_budget {
            Some(budget) => {
                let bytes = self.fixed_reservation();
                let reservation = budget
                    .try_reserve(bytes)
                    .map_err(|source| InstanceStartError::FixedReservation { bytes, source })?;
                Some(Arc::new(reservation))
            }
            None => None,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads.max(1))
            .max_blocking_threads(config.max_blocking_threads.max(1))
            .thread_name(config.thread_name)
            .enable_all()
            .build()
            .map_err(InstanceStartError::Runtime)?;
        let context = InstanceContext::new(runtime.handle().clone());
        let node = self.build_with(Some(context.clone()));
        if let Some(lease) = &fixed {
            // The caches keep it too, for as long as their memory lives.
            node.store.keep_hot_cache_lease(lease.clone());
            node.caches.keep_lease(lease);
        }
        Ok(Instance {
            runtime: OwnedRuntime {
                runtime: Some(runtime),
                context: context.clone(),
            },
            context,
            node,
            fixed: FixedReservation(fixed),
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
            shared_limits,
            memory_budget,
        } = self;
        let routing_stats = RoutingStatsHandle::default();
        // The node's background queue, shared by its retriever and by every
        // gateway built from it.
        let background = BackgroundQueue::default();
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
                    if let Some(limits) = &shared_limits {
                        dht = dht.with_query_limiter(limits.dht_query_limiter());
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
            FetchingBlockProvider::new(store.clone(), routing.with_stats(routing_stats.clone()))
                .with_background_queue(background.clone());
        if let Some(instance) = &instance {
            provider = provider.with_instance(instance.clone());
        }
        if let Some(limits) = &shared_limits {
            provider = provider.with_shared_limits(limits.clone());
        }
        if let Some(budget) = &memory_budget {
            provider = provider.with_memory_budget(budget.clone());
        }
        let block_provider: Arc<dyn BlockProvider> = Arc::new(provider.clone());
        let caches = GatewayCaches::new(&gateway_config);
        OnlineNode {
            store,
            routing_mode,
            provider,
            block_provider,
            name_resolver,
            routing_stats,
            gateway_config,
            caches,
            instance,
            background,
            shared_limits,
            memory_budget,
        }
    }
}

/// A node's fixed reservation: its store's hot cache and its gateways'
/// caches.
fn fixed_reservation(store: &SqliteBlockStore, gateway_config: &GatewayConfig) -> usize {
    usize::try_from(store.hot_cache_capacity())
        .unwrap_or(usize::MAX)
        .saturating_add(gateway_config.cache_capacity())
}

/// Why an [`Instance`] didn't start.
#[derive(Debug)]
pub enum InstanceStartError {
    /// Its fixed reservation ([`OnlineNodeBuilder::fixed_reservation`])
    /// doesn't fit in the memory budget it was given.
    FixedReservation { bytes: usize, source: BudgetError },
    /// Its runtime couldn't be started.
    Runtime(std::io::Error),
}

impl std::fmt::Display for InstanceStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FixedReservation { bytes, source } => write!(
                f,
                "the instance's fixed reservation of {bytes} bytes (its caches) \
                 doesn't fit in the memory budget: {source}"
            ),
            Self::Runtime(err) => write!(f, "the instance's runtime could not be started: {err}"),
        }
    }
}

impl std::error::Error for InstanceStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::FixedReservation { source, .. } => Some(source),
            Self::Runtime(err) => Some(err),
        }
    }
}

impl From<InstanceStartError> for std::io::Error {
    fn from(err: InstanceStartError) -> Self {
        match err {
            InstanceStartError::Runtime(err) => err,
            other => std::io::Error::new(std::io::ErrorKind::OutOfMemory, other),
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
    /// The caches all of its gateways share.
    caches: GatewayCaches,
    /// The owned instance whose gateways' background jobs it runs, if any.
    instance: Option<InstanceContext>,
    /// Its background queue, shared by its retriever and its gateways.
    background: BackgroundQueue,
    shared_limits: Option<SharedLimits>,
    memory_budget: Option<MemoryBudget>,
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
            shared_limits: None,
            memory_budget: None,
        }
    }

    /// The gateway over the node's provider and name resolver, as the native
    /// request API serves it. Every gateway of the node shares its caches.
    pub fn gateway_core(&self) -> GatewayCore {
        GatewayCore::from_state(self.gateway_state())
    }

    /// The same gateway as an HTTP router, for a host that serves it on a
    /// local socket. It shares the node's caches with
    /// [`OnlineNode::gateway_core`]'s.
    pub fn router(&self) -> Router {
        router_with_state(self.gateway_state())
    }

    /// A gateway's state over the node's caches. Each has its own request
    /// limiter, as before.
    fn gateway_state(&self) -> GatewayState {
        let state = GatewayState::with_caches(
            self.block_provider.clone(),
            self.name_resolver.clone(),
            self.gateway_config.clone(),
            self.caches.clone(),
        )
        .with_background_queue(&self.background);
        match &self.instance {
            Some(instance) => state.with_instance(instance.clone()),
            None => state,
        }
    }

    /// What the node's caches can hold: see
    /// [`OnlineNodeBuilder::fixed_reservation`].
    pub fn fixed_reservation(&self) -> usize {
        fixed_reservation(&self.store, &self.gateway_config)
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

    /// Its background queue: the work it started that no request awaits,
    /// at most [`BackgroundQueue::capacity`] items, and what was dropped.
    pub fn background_queue(&self) -> &BackgroundQueue {
        &self.background
    }

    /// The process-wide limits it was built with, if any.
    pub fn shared_limits(&self) -> Option<&SharedLimits> {
        self.shared_limits.as_ref()
    }

    /// The memory budget it was built with, if any.
    pub fn memory_budget(&self) -> Option<&MemoryBudget> {
        self.memory_budget.as_ref()
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
///   and the blocking regions, drops and joins the runtime, which ends
///   every task and blocking job still on it and closes their sockets, and
///   closes the store. Its [`Teardown`] reports quiescence only after that
///   join. Cancelling alone isn't enough: the libraries' tasks keep pooled
///   and dialled connections open after every request has ended, and their
///   blocking jobs (a `getaddrinfo` lookup) aren't counted anywhere.
///
/// Dropping an instance without shutting it down cancels it and lets its
/// runtime wind down in the background, proving nothing.
///
/// **Its fixed reservation** (with a memory budget, see
/// [`OnlineNodeBuilder::build_instance`]) is let go of only by a teardown
/// that proves quiescence, as its last step, and is given back once the
/// caches' memory is freed too: the caches themselves, held by the instance
/// and by any node or gateway a host retained, and every block or body
/// they cover that a reader still holds. An instance dropped without a
/// teardown, or whose teardown is unproven, keeps it for the rest of the
/// process: the memory it stands for may still be in use (0007: such a
/// process exits 70).
pub struct Instance {
    runtime: OwnedRuntime,
    context: InstanceContext,
    node: OnlineNode,
    fixed: FixedReservation,
}

/// An instance's hold on its fixed reservation, which its caches' pools
/// share ([`freedom_ipfs_core::FixedPool::keep_lease`]): the reservation
/// goes with the last of them. Only [`FixedReservation::release`] lets go of
/// the instance's hold; dropped, it keeps the reservation for the rest of
/// the process.
struct FixedReservation(Option<Arc<Reservation>>);

impl FixedReservation {
    fn bytes(&self) -> usize {
        self.0.as_deref().map_or(0, Reservation::bytes)
    }

    /// Lets go of the instance's hold: once quiescence is proven.
    fn release(mut self) {
        drop(self.0.take());
    }
}

impl Drop for FixedReservation {
    fn drop(&mut self) {
        if let Some(lease) = self.0.take() {
            std::mem::forget(lease);
        }
    }
}

/// The instance's runtime. Dropped while it still holds the runtime (an
/// instance dropped without [`Instance::shutdown`], or a teardown whose
/// thread couldn't be started), it cancels the instance and lets the
/// runtime wind down in the background: that never blocks, so it is safe
/// anywhere, an async context included, and it proves nothing.
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

/// What a [`Teardown`] proved, once the instance's runtime was joined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quiescence {
    /// Quiescence is proven: the runtime was dropped and joined, so no task
    /// and no blocking thread of it runs (the libraries' untracked blocking
    /// jobs included), and then the store closed.
    pub reached: bool,
    /// The runtime was dropped and joined. False only for a report made
    /// before the teardown finished ([`Instance::shutdown`]'s timeout), or
    /// when no teardown thread could be started.
    pub joined: bool,
    /// Whether the task set and the blocking regions drained within the
    /// drain bound, before the runtime was dropped.
    pub drained: bool,
    /// Tasks that hadn't ended by themselves when the drain bound passed;
    /// dropping the runtime dropped them.
    pub tasks_dropped: usize,
    /// Whether the store closed (at most 2 s for a statement in progress).
    pub store_closed: bool,
}

impl Quiescence {
    /// Nothing proven: the runtime wasn't joined and the store wasn't
    /// closed.
    const UNPROVEN: Quiescence = Quiescence {
        reached: false,
        joined: false,
        drained: false,
        tasks_dropped: 0,
        store_closed: false,
    };
}

/// An instance's teardown in progress, on a thread of its own. It finishes
/// only once the runtime has been dropped and joined and the store closed,
/// however long that takes: its caller bounds it with a watchdog of its own
/// (0007: 5 s, then `teardown_unproven` and exit 70), and releases nothing
/// of the instance before it finishes.
///
/// If no teardown thread can be started, it finishes at once, unproven
/// (`reached` and `joined` false): the runtime is then left to wind down in
/// the background, and its caller must release nothing (0007: exit 70).
pub struct Teardown {
    done: Arc<(Mutex<Option<Quiescence>>, Condvar)>,
}

impl Teardown {
    /// Waits until quiescence is proven or refused.
    pub fn wait(&self) -> Quiescence {
        let (result, finished) = &*self.done;
        let mut result = lock(result);
        loop {
            if let Some(quiescence) = *result {
                return quiescence;
            }
            result = finished
                .wait(result)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Waits at most `timeout`: `None` means the teardown hasn't finished,
    /// so quiescence isn't proven.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<Quiescence> {
        let deadline = Instant::now() + timeout;
        let (result, finished) = &*self.done;
        let mut result = lock(result);
        loop {
            if let Some(quiescence) = *result {
                return Some(quiescence);
            }
            let left = deadline.checked_duration_since(Instant::now())?;
            result = match finished.wait_timeout(result, left) {
                Ok((result, _)) => result,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }

    /// Whether the teardown has finished.
    pub fn is_finished(&self) -> bool {
        lock(&self.done.0).is_some()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// How long the teardown waits for a statement in progress before it gives
/// up closing the store (as `close_or_close_later` does).
const STORE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

impl Instance {
    /// The node.
    pub fn node(&self) -> &OnlineNode {
        &self.node
    }

    /// The bytes of fixed reservation it holds: none without a memory
    /// budget.
    pub fn fixed_reservation(&self) -> usize {
        self.fixed.bytes()
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
    ) -> Result<JoinHandle<Option<F::Output>>, Fenced>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.context.spawn_request(token, future)
    }

    /// Runs a task of the instance on its runtime, in its task set, until the
    /// instance is cancelled. Refused once the teardown has begun.
    pub fn spawn<F>(&self, future: F) -> Result<JoinHandle<Option<F::Output>>, Fenced>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.context.spawn(future)
    }

    /// Tears the instance down and waits at most `timeout` for quiescence to
    /// be proven: [`Instance::begin_shutdown`], draining for up to `timeout`,
    /// then [`Teardown::wait_timeout`] for what is left of it. If the
    /// runtime's join and the store's close haven't both finished by then,
    /// quiescence is unproven: `reached` and `joined` are false, the
    /// teardown goes on in the background, and nothing of the instance may
    /// be released (0007: exit 70).
    pub fn shutdown(self, timeout: Duration) -> Quiescence {
        let deadline = Instant::now() + timeout;
        let teardown = self.begin_shutdown(timeout);
        teardown
            .wait_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Quiescence::UNPROVEN)
    }

    /// Tears the instance down (design 0007's teardown, steps 3 and 4) and
    /// returns its [`Teardown`]. It cancels and fences the root token, and
    /// so every request's, at once; then, on a dedicated teardown thread:
    ///
    /// 1. waits, at most `drain`, until the task set is empty and no
    ///    blocking region runs;
    /// 2. drops the node (its clients and caches), then **drops the runtime
    ///    and joins it**: this returns only once every worker and every
    ///    blocking-pool thread has exited, the jobs the libraries spawned
    ///    included, and every task left on it has been dropped with its
    ///    sockets. No timed shutdown is used: one may return while such work
    ///    still runs;
    /// 3. closes the store, at most 2 s for a statement in progress;
    /// 4. lets go of its fixed reservation, if quiescence is proven: it is
    ///    given back once the caches' memory is freed too.
    ///
    /// The teardown finishes only after the join, however long it takes, so
    /// a teardown that hasn't finished proves nothing. Bound it with a
    /// watchdog of your own.
    ///
    /// It never blocks its caller and never joins the runtime on the
    /// caller's thread, so it may be called from an async context. If the
    /// OS refuses the teardown thread, the teardown finishes at once,
    /// unproven, and the runtime winds down in the background.
    ///
    /// Revoking the network capability (the design's step 2) comes with the
    /// network policy (upstream item 3).
    pub fn begin_shutdown(self, drain: Duration) -> Teardown {
        let Instance {
            runtime,
            context,
            node,
            fixed,
        } = self;
        context.cancel();
        let done = Arc::new((Mutex::new(None), Condvar::new()));
        let teardown = Teardown { done: done.clone() };
        // The runtime stays in its guard until the teardown thread runs. If
        // the thread can't be started, the closure is dropped right here,
        // and the guard lets the runtime wind down in the background: a bare
        // runtime dropped here would be joined on the caller's thread (or
        // panic, in an async context).
        let work = move || {
            let mut guard = runtime;
            let quiescence = tear_down(guard.runtime.take(), context, node, drain);
            // Last: the instance lets go of the reservation only once
            // nothing of it runs. The caches' pools keep it until the
            // memory it stands for is freed: a retained node's caches, a
            // block a reader holds.
            if quiescence.reached {
                fixed.release();
            } else {
                drop(fixed);
            }
            finish(&done, quiescence);
        };
        let builder = std::thread::Builder::new().name("freedom-ipfs-teardown".into());
        #[cfg(test)]
        let builder = tests::teardown_thread(builder);
        if builder.spawn(work).is_err() {
            finish(&teardown.done, Quiescence::UNPROVEN);
        }
        teardown
    }
}

fn finish(done: &(Mutex<Option<Quiescence>>, Condvar), quiescence: Quiescence) {
    let (result, finished) = done;
    *lock(result) = Some(quiescence);
    finished.notify_all();
}

/// The teardown thread's work: drain, join the runtime, close the store.
fn tear_down(
    runtime: Option<Runtime>,
    context: InstanceContext,
    node: OnlineNode,
    drain: Duration,
) -> Quiescence {
    let deadline = Instant::now() + drain;
    if let Some(runtime) = &runtime {
        let waiting = context.clone();
        runtime.block_on(async move {
            let _ = tokio::time::timeout_at(deadline.into(), waiting.wait_tasks()).await;
        });
    }
    context.wait_blocking_regions(deadline);
    let tasks_dropped = context.tasks();
    let drained = tasks_dropped == 0 && context.blocking_regions() == 0;
    let store = node.store().clone();
    drop(node);
    // The join: dropping a runtime waits for its workers and every
    // blocking-pool thread.
    drop(runtime);
    let store_closed = store.close(STORE_CLOSE_TIMEOUT);
    Quiescence {
        reached: store_closed && context.tasks() == 0 && context.blocking_regions() == 0,
        joined: true,
        drained,
        tasks_dropped,
        store_closed,
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
    use cid::Cid;
    use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
    use std::cell::Cell;

    thread_local! {
        static REFUSE_TEARDOWN_THREAD: Cell<bool> = const { Cell::new(false) };
    }

    /// The hook [`Instance::begin_shutdown`] calls: for a test thread that
    /// asked, the teardown thread gets a stack no OS can reserve, so its
    /// creation really fails.
    pub(super) fn teardown_thread(builder: std::thread::Builder) -> std::thread::Builder {
        if REFUSE_TEARDOWN_THREAD.with(Cell::get) {
            builder.stack_size(usize::MAX / 2)
        } else {
            builder
        }
    }

    /// An instance holding an untracked job on its blocking pool, as a
    /// library's would be (reqwest's `getaddrinfo`): only a join of the
    /// runtime waits for it. The job ends `hold` from now.
    #[cfg(target_os = "linux")]
    fn instance_with_held_job(thread_name: &str, hold: Duration) -> (Instance, SqliteBlockStore) {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let instance = OnlineNode::builder(store.clone())
            .routing_mode(RoutingMode::Offline)
            .build_instance(InstanceConfig {
                thread_name: thread_name.into(),
                worker_threads: 1,
                max_blocking_threads: 4,
            })
            .unwrap();
        let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
        drop(instance.context().handle().spawn_blocking(move || {
            let _ = entered_tx.send(());
            std::thread::sleep(hold);
        }));
        entered.recv().unwrap();
        assert_eq!(instance.context().blocking_regions(), 0, "untracked");
        (instance, store)
    }

    #[cfg(target_os = "linux")]
    fn store_is_open(store: &SqliteBlockStore) -> bool {
        store.get(&cid_from_data(CODEC_RAW, b"probe")).is_ok()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn without_a_teardown_thread_a_bounded_shutdown_is_unproven_at_once() {
        let (instance, store) = instance_with_held_job("fipfs-nothread", Duration::from_secs(2));
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(true));
        let started = Instant::now();
        let quiescence = instance.shutdown(Duration::from_millis(20));
        let took = started.elapsed();
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(false));
        // Not joined on this thread: the held job would have taken 2 s.
        assert!(took < Duration::from_millis(500), "shutdown took {took:?}");
        assert_eq!(quiescence, Quiescence::UNPROVEN);
        // And nothing of the instance released.
        assert!(store_is_open(&store));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn without_a_teardown_thread_an_async_caller_neither_blocks_nor_panics() {
        let (instance, store) = instance_with_held_job("fipfs-nothread-a", Duration::from_secs(2));
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(true));
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let started = Instant::now();
        let teardown =
            caller.block_on(async move { instance.begin_shutdown(Duration::from_millis(20)) });
        let took = started.elapsed();
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(false));
        assert!(
            took < Duration::from_millis(500),
            "begin_shutdown took {took:?}"
        );
        assert!(teardown.is_finished());
        assert_eq!(teardown.wait(), Quiescence::UNPROVEN);
        assert!(store_is_open(&store));
    }

    #[test]
    fn with_a_teardown_thread_quiescence_is_proven() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let instance = OnlineNode::builder(store)
            .routing_mode(RoutingMode::Offline)
            .build_instance(InstanceConfig {
                thread_name: "fipfs-thread".into(),
                worker_threads: 1,
                max_blocking_threads: 4,
            })
            .unwrap();
        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // From an async caller too: the join happens on the teardown thread.
        let teardown =
            caller.block_on(async move { instance.begin_shutdown(Duration::from_secs(1)) });
        let quiescence = teardown.wait();
        assert!(quiescence.reached && quiescence.joined, "{quiescence:?}");
    }

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
        assert!(builder.shared_limits.is_none(), "no shared limits");
        assert!(builder.memory_budget.is_none(), "no memory budget");
        let config = GatewayConfig::default();
        assert_eq!(
            builder.gateway_config.max_concurrent_requests(),
            config.max_concurrent_requests()
        );
        assert_eq!(
            builder.gateway_config.request_queue_timeout(),
            config.request_queue_timeout()
        );
        assert_eq!(
            builder.gateway_config.cache_capacity(),
            config.cache_capacity()
        );
    }

    #[test]
    fn a_nodes_fixed_reservation_is_its_caches_capacities() {
        let store = SqliteBlockStore::in_memory(64 * 1024 * 1024).unwrap();
        let builder = OnlineNode::builder(store.clone());
        assert_eq!(GatewayConfig::default().cache_capacity(), 3 * 1024 * 1024);
        // The hot cache (16 MiB), the small-body cache (2 MiB) and the UnixFS
        // metadata cache (1 MiB): the persistent instance's 19 of 0007's 27.
        assert_eq!(builder.fixed_reservation(), 19 * 1024 * 1024);
        let node = builder
            .gateway_config(
                GatewayConfig::default()
                    .with_small_body_cache_max_bytes(64 * 1024)
                    .with_unixfs_metadata_cache_max_bytes(128 * 1024),
            )
            .build();
        assert_eq!(node.fixed_reservation(), (16 * 1024 + 192) * 1024);
        // The caches are built at those capacities.
        let state = node.gateway_state();
        assert_eq!(state.small_body_cache.pool.capacity(), 64 * 1024);
        assert_eq!(state.unixfs.metadata_cache_max_bytes(), 128 * 1024);
        assert_eq!(store.hot_cache_capacity(), 16 * 1024 * 1024);
        let node = OnlineNode::builder(store)
            .gateway_config(
                GatewayConfig::default()
                    .with_small_body_cache_max_bytes(0)
                    .with_unixfs_metadata_cache_capacity(0),
            )
            .build();
        // Caches that cache nothing still count what they keep of
        // themselves: a few hundred bytes.
        let disabled = node.gateway_config().cache_capacity();
        assert_eq!(node.fixed_reservation(), 16 * 1024 * 1024 + disabled);
        assert!(disabled > 0 && disabled < 1024, "{disabled}");
        let state = node.gateway_state();
        assert_eq!(
            state.small_body_cache.pool.capacity() + state.unixfs.metadata_cache_max_bytes(),
            disabled
        );
    }

    #[tokio::test]
    async fn one_nodes_gateway_core_and_router_share_one_set_of_caches() {
        use crate::tests::{test_link, test_pb_directory};
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let file = b"one set of caches";
        let file_cid = cid_from_data(CODEC_RAW, file);
        store.put_block(&file_cid, file).unwrap();
        let directory = test_pb_directory(vec![test_link("a.txt", &file_cid)]);
        let directory_cid = cid_from_data(freedom_ipfs_core::CODEC_DAG_PB, &directory);
        store.put_block(&directory_cid, &directory).unwrap();
        let node = OnlineNode::builder(store.clone())
            .routing_mode(RoutingMode::Offline)
            .build();
        assert!(node
            .gateway_core()
            .state
            .shares_caches_with(&node.gateway_state()));
        assert!(node
            .gateway_core()
            .state
            .shares_caches_with(&node.clone().gateway_state()));

        let path = format!("{directory_cid}/a.txt");
        let (status, body) = get(&node.gateway_core(), &path).await;
        assert_eq!((status, body.as_slice()), (StatusCode::OK, &file[..]));
        // With the blocks gone, the router serves the file from what the
        // native gateway cached: its path, its size and its body.
        store.clear().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = node.router();
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let response = reqwest::get(format!("http://{addr}/ipfs/{path}"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), &file[..]);
        server.abort();
    }

    fn fixed_instance_config(thread_name: &str) -> InstanceConfig {
        InstanceConfig {
            thread_name: thread_name.into(),
            worker_threads: 1,
            max_blocking_threads: 4,
        }
    }

    fn offline_builder() -> OnlineNodeBuilder {
        OnlineNode::builder(SqliteBlockStore::in_memory(1024 * 1024).unwrap())
            .routing_mode(RoutingMode::Offline)
    }

    #[test]
    fn an_instance_starts_only_if_its_fixed_reservation_fits_and_gives_it_back_after_teardown() {
        let fixed = offline_builder().fixed_reservation();
        assert_eq!(fixed, 1024 * 1024 + 3 * 1024 * 1024);

        // Past the whole budget: nothing is reserved, and nothing started.
        let budget = MemoryBudget::new(fixed - 1);
        match offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-fixed-no"))
        {
            Err(InstanceStartError::FixedReservation { bytes, source }) => {
                assert_eq!(bytes, fixed);
                assert!(matches!(source, BudgetError::TooLarge { .. }), "{source}");
            }
            Err(other) => panic!("{other}"),
            Ok(_) => panic!("started without its fixed reservation"),
        }
        assert_eq!(budget.used(), 0);

        // It fits: held while the instance lives, so a second one beside it
        // doesn't fit.
        let budget = MemoryBudget::new(fixed + fixed / 2);
        let instance = offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-fixed"))
            .unwrap();
        assert_eq!(
            (budget.used(), instance.fixed_reservation()),
            (fixed, fixed)
        );
        match offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-fixed-2"))
        {
            Err(InstanceStartError::FixedReservation { source, .. }) => {
                assert!(matches!(source, BudgetError::Exhausted { .. }), "{source}");
            }
            Err(other) => panic!("{other}"),
            Ok(_) => panic!("two fixed reservations passed the budget"),
        }
        assert_eq!(budget.used(), fixed);

        // Given back when its teardown ends, once quiescence is proven.
        let quiescence = instance.shutdown(Duration::from_secs(5));
        assert!(quiescence.reached, "{quiescence:?}");
        assert_eq!(budget.used(), 0);
        let instance = offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-fixed-3"))
            .unwrap();
        assert_eq!(budget.used(), fixed);
        assert!(instance.shutdown(Duration::from_secs(5)).reached);
        assert_eq!(budget.used(), 0);

        // Without a budget, nothing is reserved.
        let instance = offline_builder()
            .build_instance(fixed_instance_config("fipfs-fixed-0"))
            .unwrap();
        assert_eq!(instance.fixed_reservation(), 0);
        assert!(instance.shutdown(Duration::from_secs(5)).reached);
    }

    #[test]
    fn a_retained_node_or_gateway_keeps_the_fixed_reservation_until_its_caches_go() {
        for retain_the_gateway in [false, true] {
            let store = SqliteBlockStore::in_memory_with(freedom_ipfs_store::StoreOptions {
                max_bytes: 1 << 20,
                hot_cache_bytes: Some(0),
                ..Default::default()
            })
            .unwrap();
            let data = vec![7u8; 32 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            let builder = OnlineNode::builder(store)
                .routing_mode(RoutingMode::Offline)
                .gateway_config(
                    GatewayConfig::default()
                        .with_small_body_cache_max_bytes(64 * 1024)
                        .with_unixfs_metadata_cache_capacity(0),
                );
            let fixed = builder.fixed_reservation();
            let budget = MemoryBudget::new(fixed);
            let instance = builder
                .memory_budget(budget.clone())
                .build_instance(fixed_instance_config("fipfs-retained"))
                .unwrap();
            let node = instance.node().clone();
            let gateway = instance.gateway_core();
            // A request fills the shared small-body cache.
            let core = instance.gateway_core();
            let path = cid.to_string();
            let request = instance
                .spawn_request(
                    instance.request_token(),
                    async move { get(&core, &path).await },
                )
                .unwrap();
            let (status, body) = instance
                .context()
                .handle()
                .block_on(request)
                .unwrap()
                .unwrap();
            assert_eq!((status, body), (StatusCode::OK, data.clone()));
            assert!(node.gateway_state().small_body_cache.pool.used() > 32 * 1024);
            // The host keeps one of them past the teardown.
            let retained: Box<dyn std::any::Any> = if retain_the_gateway {
                drop(node);
                Box::new(gateway)
            } else {
                drop(gateway);
                Box::new(node)
            };
            assert!(instance.shutdown(Duration::from_secs(5)).reached);
            // Its caches still hold the body: the reservation stays.
            assert_eq!(
                budget.used(),
                fixed,
                "retaining the gateway: {retain_the_gateway}"
            );
            assert!(budget.try_reserve(1).is_err());
            drop(retained);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn a_retained_block_keeps_the_fixed_reservation_until_it_is_freed() {
        for fetched in [false, true] {
            let store = SqliteBlockStore::in_memory_with(freedom_ipfs_store::StoreOptions {
                max_bytes: 1 << 20,
                hot_cache_bytes: Some(64 * 1024),
                ..Default::default()
            })
            .unwrap();
            let data = vec![9u8; 32 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            let builder = OnlineNode::builder(store.clone())
                .routing_mode(RoutingMode::Offline)
                .gateway_config(
                    GatewayConfig::default()
                        .with_small_body_cache_max_bytes(0)
                        .with_unixfs_metadata_cache_capacity(0),
                );
            let fixed = builder.fixed_reservation();
            let budget = MemoryBudget::new(fixed + 64 * 1024);
            if fetched {
                // A fetched block, whose charge the hot cache takes over
                // once it holds the block alone.
                let mut buffer = freedom_ipfs_core::ReservedVec::zeroed(
                    data.len(),
                    budget.try_reserve(data.len()).unwrap(),
                )
                .unwrap();
                buffer.as_mut_slice().copy_from_slice(&data);
                let block = buffer.into_charged();
                store.put_charged(&cid, &block).unwrap();
                drop(block);
                assert_eq!(budget.used(), 0, "taken over");
            } else {
                store.put_block(&cid, &data).unwrap();
            }
            let instance = builder
                .memory_budget(budget.clone())
                .build_instance(fixed_instance_config("fipfs-reader"))
                .unwrap();
            let reader = instance.node().store().get(&cid).unwrap().unwrap();
            assert!(reader.charged().is_covered());
            assert!(instance.shutdown(Duration::from_secs(5)).reached);
            // The store is closed, but the block a reader holds is still
            // covered: the reservation stays until it is freed.
            assert_eq!(budget.used(), fixed, "fetched: {fetched}");
            assert_eq!(reader.data(), &data[..]);
            drop(reader);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn a_failed_start_gives_the_reservation_back_and_a_dropped_instance_keeps_it() {
        let budget = MemoryBudget::new(64 * 1024 * 1024);
        // A thread name the OS refuses fails the runtime's start by a
        // panic: the reservation taken for it unwinds with it.
        let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            offline_builder()
                .memory_budget(budget.clone())
                .build_instance(fixed_instance_config("invalid\0thread"))
        }));
        assert!(started.is_err());
        assert_eq!(budget.used(), 0);
        let instance = offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-dropped"))
            .unwrap();
        let held = instance.fixed_reservation();
        drop(instance);
        assert_eq!(budget.used(), held, "a drop proves nothing");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_unproven_teardown_keeps_the_fixed_reservation() {
        let budget = MemoryBudget::new(64 * 1024 * 1024);
        let instance = offline_builder()
            .memory_budget(budget.clone())
            .build_instance(fixed_instance_config("fipfs-fixed-up"))
            .unwrap();
        let fixed = instance.fixed_reservation();
        assert!(fixed > 0);
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(true));
        let quiescence = instance.shutdown(Duration::from_millis(20));
        REFUSE_TEARDOWN_THREAD.with(|refuse| refuse.set(false));
        assert_eq!(quiescence, Quiescence::UNPROVEN);
        // Nothing of the instance is released: its memory may be in use.
        assert_eq!(budget.used(), fixed);
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

    /// A local HTTP provider whose answer for `held` waits until released;
    /// it records each request's CID as it arrives.
    struct HeldProvider {
        addr: std::net::SocketAddr,
        arrivals: Arc<Mutex<Vec<String>>>,
        release: tokio::sync::watch::Sender<bool>,
    }

    impl HeldProvider {
        async fn spawn(blocks: Vec<(Cid, Vec<u8>)>, held: Cid) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let blocks: Arc<Vec<(String, Vec<u8>)>> = Arc::new(
                blocks
                    .into_iter()
                    .map(|(cid, data)| (cid.to_string(), data))
                    .collect(),
            );
            let arrivals = Arc::new(Mutex::new(Vec::new()));
            let (release, released) = tokio::sync::watch::channel(false);
            let recorded = arrivals.clone();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let (blocks, recorded) = (blocks.clone(), recorded.clone());
                    let mut released = released.clone();
                    tokio::spawn(async move {
                        let mut request = vec![0u8; 4096];
                        let Ok(len) = stream.read(&mut request).await else {
                            return;
                        };
                        let request = String::from_utf8_lossy(&request[..len]).to_string();
                        let Some((cid, data)) = blocks
                            .iter()
                            .find(|(cid, _)| request.contains(&format!("/ipfs/{cid}")))
                        else {
                            return;
                        };
                        lock(&recorded).push(cid.clone());
                        if *cid == held.to_string() {
                            let _ = released.wait_for(|released| *released).await;
                        }
                        let mut response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                            data.len()
                        )
                        .into_bytes();
                        response.extend_from_slice(data);
                        let _ = stream.write_all(&response).await;
                    });
                }
            });
            Self {
                addr,
                arrivals,
                release,
            }
        }

        fn record(&self) -> freedom_ipfs_store::CachedProviderRecord {
            freedom_ipfs_store::CachedProviderRecord {
                id: None,
                addrs: vec![format!("/ip4/127.0.0.1/tcp/{}/http", self.addr.port())],
            }
        }

        fn saw(&self, cid: &Cid) -> bool {
            lock(&self.arrivals).contains(&cid.to_string())
        }
    }

    fn wait_until(what: &str, done: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn instances_sharing_limits_take_turns_and_a_cancelled_wait_ends_at_once() {
        let fixtures = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let block = |data: &[u8]| (cid_from_data(CODEC_RAW, data), data.to_vec());
        let (held, held_data) = block(b"the first instance's block");
        let (cancelled, cancelled_data) = block(b"a block whose request is cancelled");
        let (waiting, waiting_data) = block(b"a block whose request waits its turn");
        let provider = fixtures.block_on(HeldProvider::spawn(
            vec![
                (held, held_data.clone()),
                (cancelled, cancelled_data),
                (waiting, waiting_data.clone()),
            ],
            held,
        ));
        // One block fetch at a time, across both instances.
        let limits = SharedLimits::new(freedom_ipfs_retrieval::LimitSizes {
            block_fetches: 1,
            ..Default::default()
        });
        let instance = |name: &str, cids: &[Cid]| {
            let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
            for cid in cids {
                store
                    .put_provider_records(cid, &[provider.record()], Duration::from_secs(60))
                    .unwrap();
            }
            OnlineNode::builder(store)
                .routing_mode(RoutingMode::Offline)
                .shared_limits(limits.clone())
                .build_instance(InstanceConfig {
                    thread_name: name.into(),
                    worker_threads: 2,
                    max_blocking_threads: 8,
                })
                .unwrap()
        };
        let first = instance("fipfs-limits-1", &[held]);
        let second = instance("fipfs-limits-2", &[cancelled, waiting]);
        let request = |instance: &Instance, token: CancellationToken, cid: Cid| {
            let core = instance.gateway_core();
            instance
                .spawn_request(token, async move { get(&core, &cid.to_string()).await })
                .unwrap()
        };

        // The first instance's fetch holds the only permit.
        let leader = request(&first, first.request_token(), held);
        wait_until("the first fetch never started", || provider.saw(&held));
        assert_eq!(limits.block_fetches_in_flight(), 1);

        // The second instance's requests wait for it, inside their blocking
        // regions, before their fetches start.
        let token = second.request_token();
        let doomed = request(&second, token.clone(), cancelled);
        let patient = request(&second, second.request_token(), waiting);
        wait_until("the second instance's requests never blocked", || {
            second.context().blocking_regions() == 2
        });
        std::thread::sleep(Duration::from_millis(200));
        assert!(!doomed.is_finished() && !patient.is_finished());
        assert!(!provider.saw(&cancelled) && !provider.saw(&waiting));

        // Cancelling one request ends its wait at once.
        let started = Instant::now();
        token.cancel();
        assert_eq!(fixtures.block_on(doomed).unwrap(), None);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        wait_until("the cancelled region never ended", || {
            second.context().blocking_regions() == 1
        });
        assert!(!patient.is_finished(), "the other request still waits");
        assert_eq!(limits.block_fetches_in_flight(), 1);

        // Once the first fetch ends, the waiting one has its turn.
        provider.release.send_replace(true);
        let (status, body) = fixtures.block_on(leader).unwrap().unwrap();
        assert_eq!((status, body), (StatusCode::OK, held_data));
        let (status, body) = fixtures.block_on(patient).unwrap().unwrap();
        assert_eq!((status, body), (StatusCode::OK, waiting_data));
        assert!(
            !provider.saw(&cancelled),
            "the cancelled fetch never started"
        );
        assert_eq!(limits.block_fetches_in_flight(), 0);

        for instance in [first, second] {
            let quiescence = instance.shutdown(Duration::from_secs(5));
            assert!(quiescence.reached, "{quiescence:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_request_short_of_memory_past_its_deadline_is_busy() {
        let data = b"a block behind a full memory budget".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let unused = cid_from_data(CODEC_RAW, b"never held");
        let provider = HeldProvider::spawn(vec![(cid, data.clone())], unused).await;
        let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
        store
            .put_provider_records(&cid, &[provider.record()], Duration::from_secs(60))
            .unwrap();
        let budget = MemoryBudget::new(8 << 20);
        let node = OnlineNode::builder(store)
            .routing_mode(RoutingMode::Offline)
            .memory_budget(budget.clone())
            .build();
        assert!(node.memory_budget().is_some());
        let full = budget.try_reserve(budget.limit()).unwrap();

        // Its host's memory deadline passes: busy, as the memory budget's.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        let response = crate::with_memory_wait(
            crate::MemoryWait::until(deadline),
            node.gateway_core().handle(GatewayCoreRequest::ipfs(
                cid.to_string(),
                Method::GET,
                HeaderMap::new(),
            )),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get("x-freedom-ipfs-error-code").unwrap(),
            "memory_budget"
        );
        assert!(!provider.saw(&cid), "nothing was asked for");

        // With memory, the same request is served.
        drop(full);
        let (status, body) = get(&node.gateway_core(), &cid.to_string()).await;
        assert_eq!((status, body), (StatusCode::OK, data));
    }

    #[test]
    fn a_node_shares_one_background_queue_with_its_gateways() {
        let store = SqliteBlockStore::in_memory(1 << 20).unwrap();
        let node = OnlineNode::builder(store)
            .routing_mode(RoutingMode::Offline)
            .build();
        assert!(node.shared_limits().is_none());
        let queue = node.background_queue();
        let slots: Vec<_> = (0..queue.capacity())
            .map(|_| queue.try_admit().unwrap())
            .collect();
        assert_eq!(queue.capacity(), 64);
        // The retriever's and every gateway's background work count in it.
        assert!(node.provider().background_queue().try_admit().is_none());
        let core = node.gateway_core();
        assert!(core.state.html_prefetch.jobs.queue.try_admit().is_none());
        assert!(core.state.html_range_warm.jobs.queue.try_admit().is_none());
        assert_eq!(queue.dropped(), 3);
        drop(slots);
        assert_eq!(queue.outstanding(), 0);
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
