//! Process-wide admission limits, and an instance's background queue.
//!
//! A host that runs many nodes in one process (Freedom's `freedom-ipfsd`,
//! design 0007, "Budgets") needs limits that span all of them: upstream's
//! own limits are per instance. [`SharedLimits`] is that set of limits.
//! Every node built with clones of one value shares its counts:
//!
//! - **block fetches** in flight: a permit per block, from before an
//!   uncached fetch starts (its provider lookup included) until it has
//!   ended, its store put included. Coalesced callers that only wait on
//!   another caller's fetch take none. A Bitswap batch takes one per CID,
//!   and is split so that it never asks for more than the limit holds;
//! - **HTTP provider requests**: a permit taken inside the retriever's own
//!   limit, held through the body read, the verification and the store put;
//! - **DHT queries**: a permit per light-DHT query, held for the query;
//! - **Bitswap connections**: established and pending, across every node's
//!   swarm. At the swarm, a connection past the cap is denied. Above it, a
//!   request's dial that was denied for the cap waits, holding no slot, and
//!   is retried when any swarm releases one; background dials (preconnects)
//!   are dropped instead.
//!
//! Work beyond a limit waits; it never fails for waiting. A wait ends when
//! its request is cancelled: every wait is an `await` inside the request's
//! future, so dropping the future ends it, and the synchronous block
//! provider runs that future in a blocking region that drops it as soon as
//! the request's token is cancelled ([`crate::InstanceContext::block_on`]).
//! A dial waiting for a slot is dropped once no request waits on it.
//!
//! A permit is released only once the operation and every blocking job it
//! started (the store put) have ended, so a cancelled fetch keeps its permit
//! until its put returns.
//!
//! [`BackgroundQueue`] is per instance: it bounds the work a node starts that
//! no request awaits, and drops what doesn't fit.
//!
//! A node built without [`SharedLimits`] has none of these limits.

use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use libp2p::core::transport::PortUse;
use libp2p::core::Endpoint;
use libp2p::swarm::behaviour::{ConnectionClosed, DialFailure, ListenFailure};
use libp2p::swarm::{
    dummy, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

/// Block fetches in flight across a process, by default.
pub const DEFAULT_SHARED_BLOCK_FETCHES: usize = 256;
/// HTTP provider requests in flight across a process, by default.
pub const DEFAULT_SHARED_HTTP_PROVIDER_REQUESTS: usize = 16;
/// Bitswap connections, established and pending, across a process, by
/// default.
pub const DEFAULT_SHARED_BITSWAP_CONNECTIONS: usize = 32;
/// Light-DHT queries in flight across a process, by default.
pub const DEFAULT_SHARED_DHT_QUERIES: usize = 4;
/// Background work an instance has outstanding, at most, by default.
pub const DEFAULT_BACKGROUND_QUEUE_CAPACITY: usize = 64;

/// The sizes of a [`SharedLimits`]. [`LimitSizes::default`] is design 0007's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitSizes {
    pub block_fetches: usize,
    pub http_provider_requests: usize,
    pub bitswap_connections: usize,
    pub dht_queries: usize,
}

impl Default for LimitSizes {
    fn default() -> Self {
        Self {
            block_fetches: DEFAULT_SHARED_BLOCK_FETCHES,
            http_provider_requests: DEFAULT_SHARED_HTTP_PROVIDER_REQUESTS,
            bitswap_connections: DEFAULT_SHARED_BITSWAP_CONNECTIONS,
            dht_queries: DEFAULT_SHARED_DHT_QUERIES,
        }
    }
}

impl LimitSizes {
    /// The same sizes, each at least 1: a limit of 0 would admit nothing,
    /// and so wait forever.
    fn at_least_one(self) -> Self {
        Self {
            block_fetches: self.block_fetches.max(1),
            http_provider_requests: self.http_provider_requests.max(1),
            bitswap_connections: self.bitswap_connections.max(1),
            dht_queries: self.dht_queries.max(1),
        }
    }
}

/// Process-wide admission limits for block fetches, HTTP provider requests,
/// Bitswap connections and DHT queries (see the [module](self)). Clones
/// share the counts: pass one to every node's builder.
#[derive(Clone)]
pub struct SharedLimits {
    inner: Arc<Limits>,
}

struct Limits {
    sizes: LimitSizes,
    block_fetches: Arc<Semaphore>,
    http_provider_requests: Arc<Semaphore>,
    dht_queries: Arc<Semaphore>,
    bitswap_connections: ConnectionCount,
}

impl Default for SharedLimits {
    /// Design 0007's limits: 256 block fetches, 16 HTTP provider requests,
    /// 32 Bitswap connections and 4 DHT queries.
    fn default() -> Self {
        Self::new(LimitSizes::default())
    }
}

impl fmt::Debug for SharedLimits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedLimits")
            .field("sizes", &self.inner.sizes)
            .field("block_fetches_in_flight", &self.block_fetches_in_flight())
            .field(
                "http_provider_requests_in_flight",
                &self.http_provider_requests_in_flight(),
            )
            .field("bitswap_connections", &self.bitswap_connections())
            .field("dht_queries_in_flight", &self.dht_queries_in_flight())
            .finish()
    }
}

impl SharedLimits {
    /// Limits of the given sizes; a size of 0 counts as 1.
    pub fn new(sizes: LimitSizes) -> Self {
        let sizes = sizes.at_least_one();
        Self {
            inner: Arc::new(Limits {
                sizes,
                block_fetches: Arc::new(Semaphore::new(sizes.block_fetches)),
                http_provider_requests: Arc::new(Semaphore::new(sizes.http_provider_requests)),
                dht_queries: Arc::new(Semaphore::new(sizes.dht_queries)),
                bitswap_connections: ConnectionCount::new(sizes.bitswap_connections),
            }),
        }
    }

    /// The sizes in force.
    pub fn sizes(&self) -> LimitSizes {
        self.inner.sizes
    }

    /// Block fetches holding a permit now, across every node.
    pub fn block_fetches_in_flight(&self) -> usize {
        in_use(&self.inner.block_fetches, self.inner.sizes.block_fetches)
    }

    /// HTTP provider requests holding a permit now, across every node.
    pub fn http_provider_requests_in_flight(&self) -> usize {
        in_use(
            &self.inner.http_provider_requests,
            self.inner.sizes.http_provider_requests,
        )
    }

    /// DHT queries holding a permit now, across every node.
    pub fn dht_queries_in_flight(&self) -> usize {
        in_use(&self.inner.dht_queries, self.inner.sizes.dht_queries)
    }

    /// Bitswap connections, established and pending, across every node.
    pub fn bitswap_connections(&self) -> usize {
        self.inner.bitswap_connections.current()
    }

    /// Bitswap connections denied because the cap was reached.
    pub fn bitswap_connections_denied(&self) -> u64 {
        self.inner.bitswap_connections.denied()
    }

    /// Bitswap dials waiting for a free connection slot, across every node:
    /// each was denied for the cap, and is retried when a slot is released.
    pub fn bitswap_connections_waiting(&self) -> usize {
        self.inner.bitswap_connections.waiting()
    }

    /// The DHT query limiter, for [`freedom_ipfs_routing::LightDhtClient`]'s
    /// `with_query_limiter`.
    pub fn dht_query_limiter(&self) -> Arc<Semaphore> {
        self.inner.dht_queries.clone()
    }

    /// Waits for `count` block-fetch permits, one per block. `None`, at once,
    /// when `count` is 0 or more than the limit's size: such a fetch could
    /// never be admitted whole, so its caller must split it. (`None` too if
    /// the limiter were closed, which it never is.)
    pub(crate) async fn acquire_block_fetches(&self, count: usize) -> Option<OwnedSemaphorePermit> {
        if count == 0 || count > self.inner.sizes.block_fetches {
            return None;
        }
        let count = u32::try_from(count).ok()?;
        self.inner
            .block_fetches
            .clone()
            .acquire_many_owned(count)
            .await
            .ok()
    }

    /// Waits for an HTTP provider request permit.
    pub(crate) async fn acquire_http_provider_request(&self) -> Option<OwnedSemaphorePermit> {
        self.inner
            .http_provider_requests
            .clone()
            .acquire_owned()
            .await
            .ok()
    }

    /// A swarm's share of the Bitswap connection cap.
    pub(crate) fn bitswap_connection_limit(&self) -> SharedConnectionLimit {
        SharedConnectionLimit::new(self.inner.bitswap_connections.clone())
    }
}

fn in_use(semaphore: &Semaphore, size: usize) -> usize {
    size.saturating_sub(semaphore.available_permits())
}

tokio::task_local! {
    static HELD_PERMITS: HeldPermits;
}

/// The shared permits the current operation holds. A blocking job the
/// operation starts takes a clone, so each permit is released only once the
/// operation and its blocking jobs have all ended, after a cancel too.
#[derive(Clone, Default)]
pub(crate) struct HeldPermits(Vec<Arc<OwnedSemaphorePermit>>);

impl HeldPermits {
    /// The permits of the operation running now; none outside one.
    pub(crate) fn current() -> Self {
        HELD_PERMITS.try_with(Clone::clone).unwrap_or_default()
    }

    /// These permits and `permit`.
    pub(crate) fn with(mut self, permit: OwnedSemaphorePermit) -> Self {
        self.0.push(Arc::new(permit));
        self
    }

    /// Runs `future` as an operation holding these permits.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        HELD_PERMITS.scope(self, future).await
    }
}

/// The process-wide count behind the Bitswap connection cap.
#[derive(Clone)]
struct ConnectionCount {
    inner: Arc<ConnectionCountInner>,
}

struct ConnectionCountInner {
    max: usize,
    current: AtomicUsize,
    denied: AtomicU64,
    /// Dials waiting for a slot, in every swarm.
    waiting: AtomicUsize,
    /// Bumped on every release, so a swarm with dials waiting for a slot
    /// learns of one without missing it (a receiver sees any change since it
    /// last looked).
    released: watch::Sender<u64>,
}

impl ConnectionCount {
    fn new(max: usize) -> Self {
        Self {
            inner: Arc::new(ConnectionCountInner {
                max,
                current: AtomicUsize::new(0),
                denied: AtomicU64::new(0),
                waiting: AtomicUsize::new(0),
                released: watch::Sender::new(0),
            }),
        }
    }

    /// Takes a slot if one is free, atomically against every swarm.
    fn try_reserve(&self) -> bool {
        let max = self.inner.max;
        let reserved = self
            .inner
            .current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < max).then_some(current + 1)
            })
            .is_ok();
        if !reserved {
            self.inner.denied.fetch_add(1, Ordering::Relaxed);
        }
        reserved
    }

    fn release(&self) {
        let released = self
            .inner
            .current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .is_ok();
        if released {
            self.inner
                .released
                .send_modify(|count| *count = count.wrapping_add(1));
        }
    }

    fn current(&self) -> usize {
        self.inner.current.load(Ordering::Acquire)
    }

    fn denied(&self) -> u64 {
        self.inner.denied.load(Ordering::Relaxed)
    }

    fn waiting(&self) -> usize {
        self.inner.waiting.load(Ordering::Acquire)
    }
}

/// A swarm's view of the cap for its dials that wait for a slot: it tells
/// when a slot is released, in any swarm, and counts the waiting dials.
pub(crate) struct ConnectionSlotWaits {
    count: ConnectionCount,
    released: watch::Receiver<u64>,
}

impl ConnectionSlotWaits {
    /// Counts a waiting dial until the guard is dropped.
    pub(crate) fn waiting(&self) -> WaitingConnection {
        self.count.inner.waiting.fetch_add(1, Ordering::AcqRel);
        WaitingConnection {
            count: self.count.clone(),
        }
    }

    /// Returns once a slot has been released since this was last called
    /// (or since the waits were made), at once if one has. A dial denied
    /// for the cap is retried then.
    pub(crate) async fn released(&mut self) {
        if self.released.changed().await.is_err() {
            // The count outlives every swarm sharing it; never reached.
            std::future::pending::<()>().await;
        }
    }

    /// Marks every release so far as seen, before the waiting dials are
    /// retried: a release after this wakes [`ConnectionSlotWaits::released`].
    pub(crate) fn mark_released_seen(&mut self) {
        self.released.borrow_and_update();
    }
}

/// A dial counted as waiting for a slot, until dropped.
pub(crate) struct WaitingConnection {
    count: ConnectionCount,
}

impl Drop for WaitingConnection {
    fn drop(&mut self) {
        self.count.inner.waiting.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Why a connection was denied: the process-wide Bitswap connection cap was
/// reached. (Its name says `Exceeded`, as libp2p's own limits do, so the
/// swarm's logs class it as a connection limit, not a peer failure.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedConnectionLimitExceeded {
    pub limit: usize,
}

impl fmt::Display for SharedConnectionLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "process-wide Bitswap connection limit exceeded: at most {} connections are allowed",
            self.limit
        )
    }
}

impl std::error::Error for SharedConnectionLimitExceeded {}

/// A swarm's part in the process-wide Bitswap connection cap: it takes a
/// slot of the shared count for each connection, from the moment it is
/// pending until it is closed or fails, and denies a connection when no slot
/// is free. libp2p's `connection_limits` is per swarm; this spans swarms.
/// Dropping the swarm gives its slots back.
pub(crate) struct SharedConnectionLimit {
    count: ConnectionCount,
    held: HashSet<ConnectionId>,
}

impl SharedConnectionLimit {
    fn new(count: ConnectionCount) -> Self {
        Self {
            count,
            held: HashSet::new(),
        }
    }

    /// The waits of dials this swarm defers because the cap is reached.
    pub(crate) fn slot_waits(&self) -> ConnectionSlotWaits {
        ConnectionSlotWaits {
            released: self.count.inner.released.subscribe(),
            count: self.count.clone(),
        }
    }

    /// Admits `connection` (pending or established): a connection already
    /// holding a slot keeps it; a new one takes one, or is denied.
    fn admit(&mut self, connection: ConnectionId) -> Result<(), ConnectionDenied> {
        if self.held.contains(&connection) {
            return Ok(());
        }
        if !self.count.try_reserve() {
            return Err(ConnectionDenied::new(SharedConnectionLimitExceeded {
                limit: self.count.inner.max,
            }));
        }
        self.held.insert(connection);
        Ok(())
    }

    fn release(&mut self, connection: ConnectionId) {
        if self.held.remove(&connection) {
            self.count.release();
        }
    }
}

impl Drop for SharedConnectionLimit {
    fn drop(&mut self) {
        for _ in self.held.drain() {
            self.count.release();
        }
    }
}

impl NetworkBehaviour for SharedConnectionLimit {
    type ConnectionHandler = dummy::ConnectionHandler;
    type ToSwarm = Infallible;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.admit(connection_id)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.admit(connection_id)?;
        Ok(dummy::ConnectionHandler)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.admit(connection_id)?;
        Ok(Vec::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.admit(connection_id)?;
        Ok(dummy::ConnectionHandler)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. })
            | FromSwarm::DialFailure(DialFailure { connection_id, .. })
            | FromSwarm::ListenFailure(ListenFailure { connection_id, .. }) => {
                self.release(connection_id);
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {}
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }
}

/// An instance's background queue: work the node starts that no request
/// awaits (Bitswap preconnects, the gateway's HTML prefetches and range
/// warmer). At most `capacity` items are outstanding, from being spawned
/// until they end; past that, new background work is dropped, and counted.
/// Request work never goes through it, and neither does the shared Bitswap
/// swarm's own task, which is the node's machinery rather than an item of
/// work. Clones share the queue.
#[derive(Clone)]
pub struct BackgroundQueue {
    inner: Arc<Queue>,
}

struct Queue {
    capacity: usize,
    outstanding: AtomicUsize,
    dropped: AtomicU64,
}

impl Default for BackgroundQueue {
    /// A queue of [`DEFAULT_BACKGROUND_QUEUE_CAPACITY`] items.
    fn default() -> Self {
        Self::new(DEFAULT_BACKGROUND_QUEUE_CAPACITY)
    }
}

impl fmt::Debug for BackgroundQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackgroundQueue")
            .field("capacity", &self.capacity())
            .field("outstanding", &self.outstanding())
            .field("dropped", &self.dropped())
            .finish()
    }
}

impl BackgroundQueue {
    /// A queue of at most `capacity` outstanding items.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Queue {
                capacity,
                outstanding: AtomicUsize::new(0),
                dropped: AtomicU64::new(0),
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Items admitted and not yet ended.
    pub fn outstanding(&self) -> usize {
        self.inner.outstanding.load(Ordering::Acquire)
    }

    /// Items dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// Admits one item, which stays outstanding until the slot is dropped;
    /// `None`, counted as dropped, when the queue is full.
    pub fn try_admit(&self) -> Option<BackgroundSlot> {
        let capacity = self.inner.capacity;
        let admitted = self
            .inner
            .outstanding
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |outstanding| {
                (outstanding < capacity).then_some(outstanding + 1)
            })
            .is_ok();
        if !admitted {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(BackgroundSlot {
            queue: self.inner.clone(),
        })
    }
}

/// An outstanding item of a [`BackgroundQueue`], until dropped.
#[must_use = "the item is outstanding only while its slot lives"]
pub struct BackgroundSlot {
    queue: Arc<Queue>,
}

impl Drop for BackgroundSlot {
    fn drop(&mut self) {
        self.queue.outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::core::ConnectedPoint;
    use libp2p::swarm::DialError;
    use std::time::Duration;

    #[test]
    fn defaults_are_the_designs() {
        let limits = SharedLimits::default();
        assert_eq!(
            limits.sizes(),
            LimitSizes {
                block_fetches: 256,
                http_provider_requests: 16,
                bitswap_connections: 32,
                dht_queries: 4,
            }
        );
        assert_eq!(BackgroundQueue::default().capacity(), 64);
        assert_eq!(
            (
                limits.block_fetches_in_flight(),
                limits.http_provider_requests_in_flight(),
                limits.dht_queries_in_flight(),
                limits.bitswap_connections(),
            ),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn explicit_sizes_are_kept_and_zero_counts_as_one() {
        let sizes = LimitSizes {
            block_fetches: 3,
            http_provider_requests: 2,
            bitswap_connections: 5,
            dht_queries: 1,
        };
        assert_eq!(SharedLimits::new(sizes).sizes(), sizes);
        let zero = SharedLimits::new(LimitSizes {
            block_fetches: 0,
            http_provider_requests: 0,
            bitswap_connections: 0,
            dht_queries: 0,
        });
        assert_eq!(
            zero.sizes(),
            LimitSizes {
                block_fetches: 1,
                http_provider_requests: 1,
                bitswap_connections: 1,
                dht_queries: 1,
            }
        );
    }

    #[tokio::test]
    async fn clones_share_their_counts() {
        let limits = SharedLimits::new(LimitSizes {
            block_fetches: 2,
            http_provider_requests: 1,
            bitswap_connections: 1,
            dht_queries: 1,
        });
        let other = limits.clone();
        let first = limits.acquire_block_fetches(1).await.unwrap();
        let held = other.acquire_http_provider_request().await.unwrap();
        assert_eq!(other.block_fetches_in_flight(), 1);
        assert_eq!(limits.http_provider_requests_in_flight(), 1);
        // The other clone sees the permit taken: the next one waits.
        let second = other.acquire_block_fetches(1).await.unwrap();
        assert_eq!(limits.block_fetches_in_flight(), 2);
        let waiting =
            tokio::time::timeout(Duration::from_millis(50), limits.acquire_block_fetches(1)).await;
        assert!(waiting.is_err(), "a third fetch waits");
        drop((first, second, held));
        assert_eq!(limits.block_fetches_in_flight(), 0);
        // A fetch of more blocks than the limit admits is refused at once,
        // never charged less than its blocks, and never left waiting.
        assert!(other.acquire_block_fetches(3).await.is_none());
        assert!(other.acquire_block_fetches(0).await.is_none());
        assert_eq!(limits.block_fetches_in_flight(), 0);
        let all = other.acquire_block_fetches(2).await.unwrap();
        assert_eq!(all.num_permits(), 2);
        assert_eq!(limits.block_fetches_in_flight(), 2);
        drop(all);
        // The DHT limiter is the shared one.
        let dht = limits.dht_query_limiter();
        let _query = dht.clone().acquire_owned().await.unwrap();
        assert_eq!(other.dht_queries_in_flight(), 1);
        assert_eq!(other.dht_query_limiter().available_permits(), 0);
    }

    #[test]
    fn held_permits_outlive_the_operation_in_its_blocking_jobs() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let limits = SharedLimits::new(LimitSizes {
            block_fetches: 1,
            ..LimitSizes::default()
        });
        let job = runtime.block_on(async {
            let permit = limits.acquire_block_fetches(1).await.unwrap();
            HeldPermits::default()
                .with(permit)
                .scope(async { HeldPermits::current() })
                .await
        });
        // The operation ended; its blocking job's clone still holds it.
        assert_eq!(limits.block_fetches_in_flight(), 1);
        drop(job);
        assert_eq!(limits.block_fetches_in_flight(), 0);
        assert!(HeldPermits::current().0.is_empty(), "none outside one");
    }

    fn connection(id: usize) -> ConnectionId {
        ConnectionId::new_unchecked(id)
    }

    fn dial(limit: &mut SharedConnectionLimit, id: usize) -> bool {
        limit
            .handle_pending_outbound_connection(connection(id), None, &[], Endpoint::Dialer)
            .is_ok()
    }

    fn establish(limit: &mut SharedConnectionLimit, id: usize) -> bool {
        limit
            .handle_established_outbound_connection(
                connection(id),
                PeerId::random(),
                &"/ip4/127.0.0.1/tcp/1".parse().unwrap(),
                Endpoint::Dialer,
                PortUse::New,
            )
            .is_ok()
    }

    fn close(limit: &mut SharedConnectionLimit, id: usize) {
        let endpoint = ConnectedPoint::Dialer {
            address: "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
            role_override: Endpoint::Dialer,
            port_use: PortUse::New,
        };
        limit.on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
            peer_id: PeerId::random(),
            connection_id: connection(id),
            endpoint: &endpoint,
            cause: None,
            remaining_established: 0,
        }));
    }

    fn fail(limit: &mut SharedConnectionLimit, id: usize) {
        limit.on_swarm_event(FromSwarm::DialFailure(DialFailure {
            peer_id: None,
            error: &DialError::NoAddresses,
            connection_id: connection(id),
        }));
    }

    #[test]
    fn the_bitswap_connection_cap_spans_swarms() {
        let limits = SharedLimits::new(LimitSizes {
            bitswap_connections: 2,
            ..LimitSizes::default()
        });
        let mut first = limits.bitswap_connection_limit();
        let mut second = limits.bitswap_connection_limit();
        // One pending connection in each swarm fills the cap.
        assert!(dial(&mut first, 1));
        assert!(dial(&mut second, 1));
        assert_eq!(limits.bitswap_connections(), 2);
        // Past it, either swarm is denied, pending or established.
        assert!(!dial(&mut first, 2));
        assert!(!establish(&mut second, 3));
        assert_eq!(limits.bitswap_connections_denied(), 2);
        // A connection that was pending keeps its slot once established.
        assert!(establish(&mut first, 1));
        assert_eq!(limits.bitswap_connections(), 2);
        // A failed dial releases its slot, and only its own swarm's: the
        // first swarm never held connection 2.
        fail(&mut first, 2);
        assert_eq!(limits.bitswap_connections(), 2);
        fail(&mut second, 1);
        assert_eq!(limits.bitswap_connections(), 1);
        assert!(dial(&mut first, 4));
        // A closed connection releases its slot.
        close(&mut first, 1);
        assert_eq!(limits.bitswap_connections(), 1);
        assert!(dial(&mut second, 5));
        assert_eq!(limits.bitswap_connections(), 2);
        // A dropped swarm gives back all it held.
        drop(first);
        assert_eq!(limits.bitswap_connections(), 1);
        drop(second);
        assert_eq!(limits.bitswap_connections(), 0);
    }

    #[test]
    fn background_work_past_the_capacity_is_dropped_and_counted() {
        let queue = BackgroundQueue::default();
        let slots: Vec<_> = (0..64).map(|_| queue.try_admit().unwrap()).collect();
        assert_eq!(queue.outstanding(), 64);
        let clone = queue.clone();
        assert!(clone.try_admit().is_none());
        assert!(queue.try_admit().is_none());
        assert_eq!((queue.outstanding(), queue.dropped()), (64, 2));
        drop(slots);
        assert_eq!(queue.outstanding(), 0);
        let _slot = clone.try_admit().unwrap();
        assert_eq!((queue.outstanding(), queue.dropped()), (1, 2));
    }
}
