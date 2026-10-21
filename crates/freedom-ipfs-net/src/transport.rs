//! The gate in the libp2p transports (design 0007, "The gate", check 3:
//! "Before each connection, at the transport").
//!
//! **The check wraps the transport**, not the dial sites, so it covers every
//! dial a swarm makes: the node's own, Kademlia's, and those to addresses
//! learned through identify. Under `direct` it sits twice ([`Gated`]):
//! - **outside the DNS layer** ([`Gated::outer`]): every dial takes a
//!   permit of the network's capability, held through its DNS lookups, its
//!   connect and its handshake ([`crate::capability::Permit::run`]), so a
//!   revoke refuses every later dial and drops those in flight, before any
//!   query starts. Then the target is classified: the whole shape, an onion
//!   (`direct` has no route to one), and the transport. A `/dnsaddr` is
//!   expanded here, and every multiaddr its TXT records yield is classified
//!   again (check 2) before any of its names is resolved;
//! - **below it** ([`Gated::inner`]): every resolved address is checked
//!   before `connect()`, by the address policy with checked addresses
//!   (configured targets exempt by exact address and port), and by its
//!   shape again in every case. So the address connected to is the address
//!   that was checked.
//!
//! Refusals are [`TransportError::Other`], never `MultiaddrNotSupported`, so
//! an `Or` above doesn't try another transport with a refused address.
//!
//! Under the legacy policy the transports are those the crates built before,
//! by the same constructors, behind a capability-only outer gate.

mod select_security;

use std::error::Error as StdError;
use std::fmt;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::TokioResolver;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::{Boxed, DialOpts, ListenerId, TransportError, TransportEvent};
use libp2p::core::upgrade::Version;
use libp2p::identity::Keypair;
use libp2p::{noise, quic, tcp, tls, websocket, yamux, PeerId, Transport};
use multiaddr::{Multiaddr, Protocol};
use parking_lot::Mutex;

pub use select_security::SelectSecurityUpgrade;

use crate::capability::{NetworkCapability, Revoked};
use crate::dial::{DialPolicy, DialRefusal};
use crate::gate::AddressRefusal;
use crate::policy::{Mode, Network};

/// The most TXT lookups one `/dnsaddr` dial makes, nested ones included:
/// libp2p-dns's own bound.
const MAX_DNSADDR_LOOKUPS: usize = 32;
/// The most multiaddrs one TXT answer adds: libp2p-dns's own bound.
const MAX_TXT_RECORDS: usize = 16;
/// The most addresses one `/dnsaddr` dial tries: libp2p-dns's own bound.
const MAX_DIAL_ATTEMPTS: usize = 16;

/// A transport behind the gate: [`Gated::outer`] or [`Gated::inner`].
pub struct Gated<T> {
    inner: Arc<Mutex<T>>,
    layer: Layer,
}

#[derive(Clone)]
enum Layer {
    Outer {
        capability: NetworkCapability,
        policy: DialPolicy,
        /// Expands `/dnsaddr` here, under the gate, rather than in the DNS
        /// layer below.
        dnsaddr: Option<Box<TokioResolver>>,
    },
    Inner {
        policy: DialPolicy,
    },
}

impl<T> fmt::Debug for Gated<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.layer {
            Layer::Outer { .. } => "Gated::outer",
            Layer::Inner { .. } => "Gated::inner",
        })
    }
}

impl<T> Gated<T> {
    /// The gate outside the DNS layer: the capability for every dial, held
    /// for its whole course, then the target's classification under
    /// `policy` (none under the legacy policy).
    pub fn outer(inner: T, capability: NetworkCapability, policy: DialPolicy) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            layer: Layer::Outer {
                capability,
                policy,
                dnsaddr: None,
            },
        }
    }

    /// The outer gate, expanding `/dnsaddr` itself through `resolver`: every
    /// multiaddr a TXT record yields is classified before it goes on, and
    /// the layers below see no `/dnsaddr`.
    pub fn outer_expanding_dnsaddr(
        inner: T,
        capability: NetworkCapability,
        policy: DialPolicy,
        resolver: TokioResolver,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            layer: Layer::Outer {
                capability,
                policy,
                dnsaddr: Some(Box::new(resolver)),
            },
        }
    }

    /// The gate below the DNS layer: every address it is given is checked
    /// under `policy` before `inner` sees it. An IPv6 address waits for NAT64
    /// discovery while it is pending, within the dial's own timeout.
    pub fn inner(inner: T, policy: DialPolicy) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            layer: Layer::Inner { policy },
        }
    }
}

/// Why a gated dial failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GatedError<E> {
    /// The network's capability is revoked: no dial starts, and one in
    /// flight is dropped.
    #[error("{0}")]
    Revoked(#[source] Revoked),
    /// The policy refused the target: nothing was resolved or connected for
    /// it.
    #[error("refused by the network policy: {0}")]
    Refused(#[source] DialRefusal),
    /// A `/dnsaddr` gave no address to dial.
    #[error("the /dnsaddr gave no address to dial: {0}")]
    Dnsaddr(String),
    /// The transport below failed.
    #[error(transparent)]
    Transport(E),
}

impl<E> GatedError<E> {
    /// Whether the gate refused the dial (revoked, or refused by the policy),
    /// rather than the transport failing.
    pub fn is_refusal(&self) -> bool {
        matches!(self, Self::Revoked(_) | Self::Refused(_))
    }
}

type GatedDial<T> =
    BoxFuture<'static, Result<<T as Transport>::Output, GatedError<<T as Transport>::Error>>>;

impl<T> Transport for Gated<T>
where
    T: Transport + Send + Unpin + 'static,
    T::Output: Send + 'static,
    T::Error: Send + 'static,
    T::Dial: Send + 'static,
    T::ListenerUpgrade: Send + 'static,
{
    type Output = T::Output;
    type Error = GatedError<T::Error>;
    type ListenerUpgrade = BoxFuture<'static, Result<T::Output, Self::Error>>;
    type Dial = GatedDial<T>;

    fn listen_on(
        &mut self,
        id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        self.inner
            .lock()
            .listen_on(id, addr)
            .map_err(|err| err.map(GatedError::Transport))
    }

    fn remove_listener(&mut self, id: ListenerId) -> bool {
        self.inner.lock().remove_listener(id)
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        match self.layer.clone() {
            Layer::Outer {
                capability,
                policy,
                dnsaddr,
            } => {
                let permit = capability
                    .admit()
                    .map_err(|revoked| TransportError::Other(GatedError::Revoked(revoked)))?;
                policy
                    .check_target(&addr)
                    .map_err(|refusal| TransportError::Other(GatedError::Refused(refusal)))?;
                let dial: GatedDial<T> = match dnsaddr {
                    Some(resolver) if is_dnsaddr(&addr) => {
                        dial_dnsaddr(self.inner.clone(), *resolver, policy, addr, opts).boxed()
                    }
                    _ => self
                        .inner
                        .lock()
                        .dial(addr, opts)
                        .map_err(|err| err.map(GatedError::Transport))?
                        .map_err(GatedError::Transport)
                        .boxed(),
                };
                Ok(async move {
                    match permit.run(dial).await {
                        Ok(result) => result,
                        Err(revoked) => Err(GatedError::Revoked(revoked)),
                    }
                }
                .boxed())
            }
            Layer::Inner { policy } => match inner_check(&policy, &addr) {
                Ok(()) => Ok(self
                    .inner
                    .lock()
                    .dial(addr, opts)
                    .map_err(|err| err.map(GatedError::Transport))?
                    .map_err(GatedError::Transport)
                    .boxed()),
                Err(DialRefusal::Address(AddressRefusal::Nat64Pending)) => {
                    // The transport claims the address (or doesn't, and an
                    // `Or` above tries the next) before the wait: its future
                    // connects only when polled, after the address passed.
                    let dial = self
                        .inner
                        .lock()
                        .dial(addr.clone(), opts)
                        .map_err(|err| err.map(GatedError::Transport))?;
                    Ok(async move {
                        policy
                            .check_settled(&addr)
                            .await
                            .map_err(GatedError::Refused)?;
                        dial.await.map_err(GatedError::Transport)
                    }
                    .boxed())
                }
                Err(refusal) => Err(TransportError::Other(GatedError::Refused(refusal))),
            },
        }
    }

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        let mut inner = self.inner.lock();
        Transport::poll(Pin::new(&mut *inner), cx).map(|event| {
            event
                .map_upgrade(|upgrade| upgrade.map_err(GatedError::Transport).boxed())
                .map_err(GatedError::Transport)
        })
    }
}

thread_local! {
    /// The address a configured target's answer was admitted for, while
    /// [`dial_configured_answer`] dials it.
    static CONFIGURED_ANSWER: std::cell::RefCell<Option<(IpAddr, u16)>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `dial` (a swarm's `dial`, which asks its transport there and then)
/// with `addr` exempt from the address policy at the inner gate: an
/// address candidate choice admitted as a configured target's answer (a
/// configured name and port, whose A or AAAA answer replaced the name;
/// design 0007: configured targets are exempt by exact host and port). The
/// exemption is that address's and port's, for this call only: a dial the
/// transport makes later, or for another address, is judged as untrusted,
/// and the shape, the classification and the capability still apply.
pub fn dial_configured_answer<R>(addr: &Multiaddr, dial: impl FnOnce() -> R) -> R {
    let exempt = ip_and_port(addr);
    let previous = CONFIGURED_ANSWER.with(|cell| cell.replace(exempt));
    let result = dial();
    CONFIGURED_ANSWER.with(|cell| cell.replace(previous));
    result
}

/// The inner gate's check: the policy's, but for the address a configured
/// answer is being dialed at, which is checked without the address table.
fn inner_check(policy: &DialPolicy, addr: &Multiaddr) -> Result<(), DialRefusal> {
    let exempt = CONFIGURED_ANSWER.with(|cell| *cell.borrow());
    match exempt {
        Some(exempt) if ip_and_port(addr) == Some(exempt) => policy.check_target(addr),
        _ => policy.check_now(addr),
    }
}

/// A dialed address's IP and port, if it has one.
fn ip_and_port(addr: &Multiaddr) -> Option<(IpAddr, u16)> {
    let mut ip = None;
    let mut port = None;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(v4) => ip = Some(IpAddr::V4(v4)),
            Protocol::Ip6(v6) => ip = Some(IpAddr::V6(v6)),
            Protocol::Tcp(p) | Protocol::Udp(p) => port = Some(p),
            _ => {}
        }
    }
    Some((ip?, port?))
}

fn is_dnsaddr(addr: &Multiaddr) -> bool {
    matches!(addr.iter().next(), Some(Protocol::Dnsaddr(_)))
}

/// A `/dnsaddr` dial, expanded under the gate (check 2): each TXT record's
/// multiaddr that ends with the dialed address's suffix (its `/p2p`, as
/// libp2p-dns matches it) is classified whole before it goes on; nested
/// `/dnsaddr` records are expanded the same way; every other is dialed
/// through the layers below, which resolve its names and check its
/// addresses, until one connects.
async fn dial_dnsaddr<T>(
    inner: Arc<Mutex<T>>,
    resolver: TokioResolver,
    policy: DialPolicy,
    addr: Multiaddr,
    opts: DialOpts,
) -> Result<T::Output, GatedError<T::Error>>
where
    T: Transport + Send + Unpin + 'static,
    T::Error: Send + 'static,
    T::Dial: Send + 'static,
{
    let mut unresolved = vec![addr];
    let mut lookups = 0usize;
    let mut attempts = 0usize;
    let mut last_error = None;
    let mut refusals = Vec::new();
    while let Some(addr) = unresolved.pop() {
        let Some(Protocol::Dnsaddr(name)) = addr.iter().next() else {
            if attempts == MAX_DIAL_ATTEMPTS {
                break;
            }
            let dial = inner.lock().dial(addr, opts);
            match dial {
                Ok(dial) => {
                    attempts += 1;
                    match dial.await {
                        Ok(output) => return Ok(output),
                        Err(err) => last_error = Some(err),
                    }
                }
                Err(TransportError::Other(err)) => last_error = Some(err),
                Err(TransportError::MultiaddrNotSupported(_)) => {
                    refusals.push("an address no transport supports".to_string());
                }
            }
            continue;
        };
        if lookups == MAX_DNSADDR_LOOKUPS {
            refusals.push("too many /dnsaddr lookups".to_string());
            continue;
        }
        lookups += 1;
        let suffix: Multiaddr = addr.iter().skip(1).collect();
        let records = match resolver.txt_lookup(format!("_dnsaddr.{name}")).await {
            Ok(records) => records,
            Err(err) => {
                refusals.push(err.kind().to_string());
                continue;
            }
        };
        let mut added = 0usize;
        for record in records.iter() {
            let Some(text) = record
                .txt_data()
                .first()
                .and_then(|chars| std::str::from_utf8(chars).ok())
            else {
                continue;
            };
            let Some(Ok(expanded)) = text
                .strip_prefix("dnsaddr=")
                .map(|text| text.parse::<Multiaddr>())
            else {
                continue;
            };
            if !expanded.ends_with(&suffix) || added == MAX_TXT_RECORDS {
                continue;
            }
            added += 1;
            match policy.check_target(&expanded) {
                Ok(()) => unresolved.push(expanded),
                Err(refusal) => refusals.push(refusal.to_string()),
            }
        }
    }
    match last_error {
        Some(err) => Err(GatedError::Transport(err)),
        None => Err(GatedError::Dnsaddr(refusals.join("; "))),
    }
}

/// How the transports a network's swarms dial through are built.
#[derive(Clone, Debug)]
pub struct Transports {
    network: Network,
    dns: ResolverConfig,
}

/// Why a transport couldn't be built: a security upgrade refused the
/// keypair.
pub type TransportBuildError = Box<dyn StdError + Send + Sync>;

/// A swarm's transport, as `SwarmBuilder::with_other_transport` takes it.
pub type SwarmTransport = Boxed<(PeerId, StreamMuxerBox)>;

impl Transports {
    /// `network`'s transports, with libp2p's DNS layer asking Cloudflare's
    /// resolvers, as the crates' swarms did before.
    pub fn new(network: &Network) -> Self {
        Self {
            network: network.clone(),
            dns: ResolverConfig::cloudflare(),
        }
    }

    /// The same, with the DNS layer asking `dns`: tests.
    #[doc(hidden)]
    pub fn with_dns(mut self, dns: ResolverConfig) -> Self {
        self.dns = dns;
        self
    }

    pub fn network(&self) -> &Network {
        &self.network
    }

    fn resolver(&self) -> TokioResolver {
        TokioResolver::builder_with_config(self.dns.clone(), TokioConnectionProvider::default())
            .with_options(ResolverOpts::default())
            .build()
    }

    fn dns<T>(&self, inner: T) -> libp2p::dns::tokio::Transport<T> {
        libp2p::dns::tokio::Transport::custom(inner, self.dns.clone(), ResolverOpts::default())
    }

    fn gated(&self) -> bool {
        !matches!(self.network.policy().mode(), Mode::Legacy)
    }

    fn outer<T>(&self, inner: T, expand_dnsaddr: bool) -> Gated<T> {
        let capability = self.network.capability().clone();
        let policy = self.network.dial_policy();
        if expand_dnsaddr && self.gated() {
            Gated::outer_expanding_dnsaddr(inner, capability, policy, self.resolver())
        } else {
            Gated::outer(inner, capability, policy)
        }
    }

    /// The light DHT's transport: TCP (TLS or Noise, then Yamux) or QUIC,
    /// under libp2p's DNS layer, as `SwarmBuilder`'s
    /// `with_tcp(..).with_quic().with_dns_config(..)` builds it. Under
    /// `direct`, with the outer gate above the DNS layer (expanding
    /// `/dnsaddr` itself) and the inner gate below it.
    pub fn dht(&self, keypair: &Keypair) -> Result<SwarmTransport, TransportBuildError> {
        let tcp = tcp_upgraded(keypair)?;
        let quic = quic_transport(keypair);
        let direct = tcp.or_transport(quic).map(|either, _| either.into_inner());
        Ok(if self.gated() {
            let inner = Gated::inner(direct, self.network.dial_policy());
            self.outer(self.dns(inner), true).boxed()
        } else {
            self.outer(self.dns(direct), false).boxed()
        })
    }

    /// Bitswap's transport, as the retriever built it: TCP (TLS or Noise,
    /// then Yamux) and QUIC without DNS, or WebSocket (Noise, then Yamux)
    /// over libp2p's DNS layer, so a WSS name reaches TLS for SNI. Under
    /// `direct`, with the outer gate above both and the inner gate below
    /// the DNS layer and below TCP and QUIC.
    pub fn bitswap(&self, keypair: &Keypair) -> Result<SwarmTransport, TransportBuildError> {
        let gated = self.gated();
        let policy = self.network.dial_policy();
        let direct = tcp_upgraded(keypair)?
            .or_transport(quic_transport(keypair))
            .map(|either, _| either.into_inner());
        let direct = if gated {
            Gated::inner(direct, policy.clone()).boxed()
        } else {
            direct.boxed()
        };
        let tcp = tcp::tokio::Transport::new(tcp::Config::default());
        let websocket = if gated {
            websocket_upgraded(self.dns(Gated::inner(tcp, policy)), keypair)?
        } else {
            websocket_upgraded(self.dns(tcp), keypair)?
        };
        let both = direct
            .or_transport(websocket)
            .map(|either, _| either.into_inner());
        Ok(self.outer(both, false).boxed())
    }
}

/// TCP with TLS or Noise, then Yamux: what `SwarmBuilder::with_tcp` builds
/// for `(tls::Config::new, noise::Config::new)` and
/// `yamux::Config::default`.
fn tcp_upgraded(keypair: &Keypair) -> Result<SwarmTransport, TransportBuildError> {
    let security =
        SelectSecurityUpgrade::new(tls::Config::new(keypair)?, noise::Config::new(keypair)?);
    Ok(tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(Version::V1Lazy)
        .authenticate(security)
        .multiplex(yamux::Config::default())
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed())
}

/// QUIC, as `SwarmBuilder::with_quic` builds it.
fn quic_transport(keypair: &Keypair) -> SwarmTransport {
    quic::tokio::Transport::new(quic::Config::new(keypair))
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed()
}

/// WebSocket over `inner` with Noise, then Yamux, as the retriever built it.
fn websocket_upgraded<T>(inner: T, keypair: &Keypair) -> Result<SwarmTransport, TransportBuildError>
where
    T: Transport + Send + Unpin + 'static,
    T::Output: futures::AsyncRead + futures::AsyncWrite + Unpin + Send + 'static,
    T::Error: Send + Sync + 'static,
    T::Dial: Send + 'static,
    T::ListenerUpgrade: Send + 'static,
{
    Ok(websocket::Config::new(inner)
        .upgrade(Version::V1Lazy)
        .authenticate(noise::Config::new(keypair)?)
        .multiplex(yamux::Config::default())
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
        .boxed())
}
