//! The network policy and the network an instance runs under (design 0007,
//! "Network policy", "Modes").

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;
use url::Url;

use crate::capability::NetworkCapability;
use crate::dial::{DialPolicy, TransportPolicy};
use crate::gate::{ConfiguredTargets, Nat64State, Refusal};
use crate::http::{ClientConfig, HttpClient};
use crate::resolve::{system_lookup, NameLookup};

/// What a node's network may do.
///
/// [`NetworkPolicy::legacy`] and [`NetworkPolicy::direct`] can be built,
/// and with the cargo feature `socks`, `NetworkPolicy::socks5`.
#[derive(Clone, Debug)]
pub struct NetworkPolicy {
    mode: Mode,
}

impl NetworkPolicy {
    /// What the crates did before the policy, and the builder's default: the
    /// system proxy (reqwest reads the environment's proxy variables), each
    /// client's own redirect policy, reqwest's resolver and libp2p's
    /// transports as they were. No gate.
    pub fn legacy() -> Self {
        Self { mode: Mode::Legacy }
    }

    /// Direct connections behind the gate (design 0007, "Modes"):
    /// - **always:** no environment proxy (`no_proxy()`), redirects followed
    ///   by the policy's own loop, every hop classified (an onion is refused
    ///   before any socket), and every request and hop asking the network's
    ///   capability;
    /// - **with [`Addresses::Checked`]:** the address policy too, through
    ///   the gate's resolver.
    ///
    /// `configured` names the delegated routers (and any other endpoint
    /// configuration names): they and `doh`, which is added to them, are
    /// exempt from the address policy by exact host and port. No configured
    /// target may be an onion: `direct` has no route to one.
    pub fn direct(
        mut configured: ConfiguredTargets,
        doh: Url,
        addresses: Addresses,
    ) -> Result<Self, NetworkError> {
        configured.add_url(&doh).map_err(NetworkError::Configured)?;
        if configured.has_onion() {
            return Err(NetworkError::OnionConfigured);
        }
        Ok(Self {
            mode: Mode::Direct(Direct {
                addresses,
                configured: Arc::new(configured),
                doh,
            }),
        })
    }

    /// Every connection through a SOCKS5 proxy, failing closed (design
    /// 0007, "Modes", `socks5`):
    /// - `proxy` is an IP literal and a port, so reaching it needs no DNS;
    /// - ordinary names are resolved through the proxy (Tor's `RESOLVE`),
    ///   every answer passes the gate, and the connection goes to the
    ///   checked address; only onions go to the proxy as names, on their
    ///   own route;
    /// - QUIC and the local resolvers are off.
    ///
    /// `configured` names the delegated routers (and any other endpoint
    /// configuration names): they and `doh`, which is added to them, are
    /// exempt from the address policy by exact host and port, and are
    /// reached through the proxy like everything else. The proxy's own
    /// endpoint isn't a target, and isn't exempt (design 0007 as amended by
    /// freedom-core #158).
    #[cfg(feature = "socks")]
    pub fn socks5(
        proxy: SocketAddr,
        mut configured: ConfiguredTargets,
        doh: Url,
    ) -> Result<Self, NetworkError> {
        if proxy.ip().is_unspecified() || proxy.port() == 0 {
            return Err(NetworkError::ProxyEndpoint);
        }
        configured.add_url(&doh).map_err(NetworkError::Configured)?;
        Ok(Self {
            mode: Mode::Socks5(Socks5 {
                proxy,
                configured: Arc::new(configured),
                doh,
            }),
        })
    }

    pub fn mode(&self) -> &Mode {
        &self.mode
    }

    /// The DoH endpoint the policy names: `None` under
    /// [`NetworkPolicy::legacy`], whose resolver keeps its own default.
    pub fn doh(&self) -> Option<&str> {
        match &self.mode {
            Mode::Direct(direct) => Some(direct.doh.as_str()),
            Mode::Socks5(socks5) => Some(socks5.doh.as_str()),
            Mode::Legacy => None,
        }
    }

    /// The targets the policy exempts from the address policy: none under
    /// [`NetworkPolicy::legacy`], which has no address policy.
    pub fn configured(&self) -> Option<&ConfiguredTargets> {
        match &self.mode {
            Mode::Direct(direct) => Some(&direct.configured),
            Mode::Socks5(socks5) => Some(&socks5.configured),
            Mode::Legacy => None,
        }
    }
}

impl Default for NetworkPolicy {
    fn default() -> Self {
        Self::legacy()
    }
}

/// A policy's mode.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Mode {
    /// See [`NetworkPolicy::legacy`].
    Legacy,
    /// See [`NetworkPolicy::direct`].
    Direct(Direct),
    /// Every connection through a SOCKS5 proxy, failing closed.
    Socks5(Socks5),
}

/// `direct` (design 0007, "Modes"). The gate comes to it in two parts:
/// - **always:** the classification and the capability. Onion targets and
///   non-network multiaddrs are refused before any socket, the policy's own
///   hop loop follows redirects, and revoking the capability stops every
///   later socket, DNS query and hop;
/// - **with [`Addresses::Checked`]:** the address policy too, which needs
///   NAT64 discovery's prefixes.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Direct {
    pub addresses: Addresses,
    /// The configured targets, the DoH endpoint among them.
    pub configured: Arc<ConfiguredTargets>,
    /// The DoH endpoint.
    pub doh: Url,
}

/// Whether `direct` applies the address policy.
#[derive(Clone, Debug)]
pub enum Addresses {
    /// Not yet: names resolve as before (reqwest's resolver), and no
    /// address is refused. Only the classification and the capability
    /// apply.
    Unchecked,
    /// Every untrusted address is checked (design 0007, "Addresses"), with
    /// what NAT64 discovery has learned: names through the gate's resolver,
    /// IP literals in the hop loop.
    Checked(Nat64Handle),
}

/// `socks5` (design 0007, "Modes"); see `NetworkPolicy::socks5`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Socks5 {
    /// The proxy's endpoint: an IP literal, so reaching it needs no DNS.
    pub proxy: SocketAddr,
    /// The configured targets, the DoH endpoint among them; not the proxy.
    pub configured: Arc<ConfiguredTargets>,
    /// The DoH endpoint.
    pub doh: Url,
}

/// A view of what NAT64 discovery knows, which the host's discovery keeps
/// current (design 0007, "Network-specific NAT64 prefixes"). The result
/// belongs to the process: clones share it, and every instance's policy can
/// hold one.
#[derive(Clone, Debug)]
pub struct Nat64Handle {
    state: watch::Receiver<Nat64State>,
}

/// What publishes discovery's state to its [`Nat64Handle`]s.
#[derive(Debug)]
pub struct Nat64Publisher {
    state: watch::Sender<Nat64State>,
}

impl Nat64Handle {
    /// A handle whose state never changes: tests, or a host that knows.
    pub fn fixed(state: Nat64State) -> Self {
        let (_, handle) = Self::channel(state);
        handle
    }

    /// A handle and the publisher that updates it.
    pub fn channel(initial: Nat64State) -> (Nat64Publisher, Self) {
        let (sender, receiver) = watch::channel(initial);
        (Nat64Publisher { state: sender }, Self { state: receiver })
    }

    /// The state now.
    pub fn current(&self) -> Nat64State {
        self.state.borrow().clone()
    }

    /// Waits until the state changes after the last one this handle saw:
    /// `false` once nothing will publish again.
    pub async fn changed(&mut self) -> bool {
        self.state.changed().await.is_ok()
    }
}

impl Nat64Publisher {
    /// Publishes a new state to every handle.
    pub fn publish(&self, state: Nat64State) {
        self.state.send_replace(state);
    }
}

/// The network an instance runs under: its policy and its capability. Every
/// HTTP client it hands out ([`Network::http_client`]) asks the capability
/// before each request and each hop, so revoking it ([`Network::revoke`])
/// refuses every later one of every one of them. Clones share both.
#[derive(Clone, Debug)]
pub struct Network {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    policy: NetworkPolicy,
    capability: NetworkCapability,
    /// The gate's resolver's lookups, under `direct` with checked addresses.
    lookup: Option<Arc<dyn NameLookup>>,
    /// The SOCKS5 client every request through the proxy goes by, under
    /// `socks5`: one per network.
    #[cfg(feature = "socks")]
    socks: Option<Arc<crate::socks::SocksClient>>,
}

impl Network {
    /// The network for `policy`, with a capability of its own. Refused for a
    /// mode this build can't honour, never approximated: a policy that
    /// silently connected another way would fail open. `socks5` needs the
    /// cargo feature `socks`, which brings both its HTTP clients and its
    /// libp2p transports.
    ///
    /// Under `direct` with checked addresses, the gate's resolver uses the
    /// system's DNS configuration, or Cloudflare's resolvers where it can't
    /// be read. Under `socks5` it asks the proxy (`RESOLVE`), and every
    /// stream carries the network's own SOCKS5 credentials (design 0007,
    /// "Stream isolation"), made here from the operating system's
    /// randomness: if that fails, so does this.
    pub fn new(policy: NetworkPolicy) -> Result<Self, NetworkError> {
        let lookup = match policy.mode() {
            Mode::Legacy => None,
            Mode::Direct(Direct {
                addresses: Addresses::Unchecked,
                ..
            }) => None,
            Mode::Direct(Direct {
                addresses: Addresses::Checked(_),
                ..
            }) => Some(system_lookup()),
            // Its lookups are `RESOLVE`'s, through the network's SOCKS
            // client.
            #[cfg(feature = "socks")]
            Mode::Socks5(_) => None,
            #[cfg(not(feature = "socks"))]
            Mode::Socks5(_) => return Err(NetworkError::Unsupported("socks5")),
        };
        Self::with_lookup(policy, lookup)
    }

    /// The network for `policy` with the gate's resolver asking `lookup`:
    /// tests, and hosts that bring their own resolver. Under `socks5`, its
    /// credentials come from the operating system's randomness, whose
    /// failure fails it.
    #[doc(hidden)]
    pub fn with_lookup(
        policy: NetworkPolicy,
        lookup: Option<Arc<dyn NameLookup>>,
    ) -> Result<Self, NetworkError> {
        #[cfg(feature = "socks")]
        {
            Self::with_lookup_and_random(policy, lookup, crate::socks::os_random)
        }
        #[cfg(not(feature = "socks"))]
        {
            Ok(Self::build(policy, lookup))
        }
    }

    /// [`Network::with_lookup`], with the credentials' bits from `random`:
    /// a test's failing randomness. Nothing falls back to a fixed password
    /// or to no authentication.
    #[cfg(feature = "socks")]
    #[doc(hidden)]
    pub fn with_lookup_and_random(
        policy: NetworkPolicy,
        lookup: Option<Arc<dyn NameLookup>>,
        random: fn(&mut [u8]) -> Result<(), crate::socks::RandomFailed>,
    ) -> Result<Self, NetworkError> {
        let socks = match policy.mode() {
            Mode::Socks5(socks5) => Some(Arc::new(crate::socks::SocksClient::new(
                socks5.proxy,
                crate::socks::SocksCredentials::new(random)
                    .map_err(|_| NetworkError::Credentials)?,
            ))),
            Mode::Legacy | Mode::Direct(_) => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                policy,
                capability: NetworkCapability::new(),
                lookup,
                socks,
            }),
        })
    }

    #[cfg(not(feature = "socks"))]
    fn build(policy: NetworkPolicy, lookup: Option<Arc<dyn NameLookup>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                policy,
                capability: NetworkCapability::new(),
                lookup,
            }),
        }
    }

    /// A [`NetworkPolicy::legacy`] network, with a capability of its own.
    pub fn legacy() -> Self {
        Self {
            inner: Arc::new(Inner {
                policy: NetworkPolicy::legacy(),
                capability: NetworkCapability::new(),
                lookup: None,
                #[cfg(feature = "socks")]
                socks: None,
            }),
        }
    }

    pub fn policy(&self) -> &NetworkPolicy {
        &self.inner.policy
    }

    pub fn capability(&self) -> &NetworkCapability {
        &self.inner.capability
    }

    /// Revokes the capability: every later request and hop of every client
    /// of this network fails with [`crate::http::HttpError::Revoked`].
    pub fn revoke(&self) {
        self.inner.capability.revoke();
    }

    pub(crate) fn lookup(&self) -> Option<&Arc<dyn NameLookup>> {
        self.inner.lookup.as_ref()
    }

    /// The network's SOCKS5 client, under `socks5`.
    #[cfg(feature = "socks")]
    pub(crate) fn socks(&self) -> Option<&Arc<crate::socks::SocksClient>> {
        self.inner.socks.as_ref()
    }

    /// What the network may dial, as candidate choice and address expansion
    /// ask it (design 0007, checks 1 and 2): every check asks this network's
    /// capability first.
    pub fn dial_policy(&self) -> DialPolicy {
        DialPolicy::for_network(&self.inner.policy, &self.inner.capability)
    }

    /// The transports the network's dials can use.
    pub fn transport_policy(&self) -> TransportPolicy {
        self.dial_policy().transports()
    }

    /// A new HTTP client for `config`, built by the crates' one constructor
    /// for this network's policy, which asks this network's capability
    /// before every request and every hop. Each call builds a client of its
    /// own, with its own connection pools.
    ///
    /// It never panics: a client that couldn't be built holds the error
    /// ([`HttpClient::error`]) and fails every request with it.
    pub fn http_client(&self, config: ClientConfig) -> HttpClient {
        HttpClient::new(self.clone(), config)
    }
}

/// Why a network or one of its clients can't be built.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkError {
    /// reqwest refused the client's configuration.
    #[error("the HTTP client couldn't be built: {0}")]
    ClientBuild(Arc<reqwest::Error>),
    /// The mode isn't available in this version.
    #[error("network mode {0} isn't available yet")]
    Unsupported(&'static str),
    /// A configured endpoint isn't an `http` or `https` URL without
    /// userinfo.
    #[error("a configured endpoint is refused: {0}")]
    Configured(Refusal),
    /// A configured target is an onion, which `direct` can't reach.
    #[error("a configured endpoint is an onion, which direct mode can't reach")]
    OnionConfigured,
    /// The SOCKS5 proxy's endpoint is unspecified, or has port 0.
    #[error("the SOCKS5 proxy's endpoint must be a specified IP address and a nonzero port")]
    ProxyEndpoint,
    /// The network's SOCKS5 credentials couldn't be made: the operating
    /// system's randomness failed.
    #[error("the SOCKS5 credentials couldn't be made: the operating system's randomness failed")]
    Credentials,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{parse_url, Nat64Prefix};

    fn url(text: &str) -> Url {
        Url::parse(text).unwrap()
    }

    #[cfg(not(feature = "socks"))]
    #[test]
    fn socks5_networks_need_the_socks_feature() {
        assert!(matches!(
            Network::new(NetworkPolicy::legacy()).map(|n| n.policy().mode().clone()),
            Ok(Mode::Legacy)
        ));
        let socks5 = NetworkPolicy {
            mode: Mode::Socks5(Socks5 {
                proxy: "127.0.0.1:9150".parse().unwrap(),
                configured: Arc::new(ConfiguredTargets::new()),
                doh: url("https://dns.example/dns-query"),
            }),
        };
        assert!(matches!(
            Network::new(socks5),
            Err(NetworkError::Unsupported("socks5"))
        ));
    }

    #[test]
    fn a_direct_policy_adds_its_doh_endpoint_and_refuses_onions() {
        let mut configured = ConfiguredTargets::new();
        configured
            .add_url(&parse_url("http://127.0.0.1:5001/routing/v1").unwrap())
            .unwrap();
        let policy = NetworkPolicy::direct(
            configured.clone(),
            url("https://dns.example/dns-query"),
            Addresses::Unchecked,
        )
        .unwrap();
        assert_eq!(policy.doh(), Some("https://dns.example/dns-query"));
        let configured_now = policy.configured().unwrap();
        assert!(configured_now.contains(&crate::gate::Target::Name("dns.example".into()), 443));
        assert!(
            configured_now.contains(&crate::gate::Target::Ip("127.0.0.1".parse().unwrap()), 5001)
        );
        assert!(Network::new(policy).is_ok());

        assert!(matches!(
            NetworkPolicy::direct(
                configured.clone(),
                url("https://user:pw@dns.example/dns-query"),
                Addresses::Unchecked,
            ),
            Err(NetworkError::Configured(Refusal::Userinfo))
        ));
        assert!(matches!(
            NetworkPolicy::direct(
                configured.clone(),
                url("ftp://dns.example/"),
                Addresses::Unchecked
            ),
            Err(NetworkError::Configured(Refusal::Scheme))
        ));
        let mut onion = configured;
        onion
            .add_url(&parse_url("http://router.onion/routing/v1").unwrap())
            .unwrap();
        assert!(matches!(
            NetworkPolicy::direct(onion, url("https://dns.example/"), Addresses::Unchecked),
            Err(NetworkError::OnionConfigured)
        ));
        assert!(matches!(
            NetworkPolicy::direct(
                ConfiguredTargets::new(),
                url("https://dns.onion/"),
                Addresses::Unchecked
            ),
            Err(NetworkError::OnionConfigured)
        ));
    }

    #[test]
    fn a_networks_clones_share_its_capability_and_networks_dont() {
        let network = Network::legacy();
        let clone = network.clone();
        let other = Network::legacy();
        network.revoke();
        assert!(clone.capability().is_revoked());
        assert!(!other.capability().is_revoked());
    }

    #[tokio::test]
    async fn nat64_handles_follow_their_publisher() {
        let (publisher, mut handle) = Nat64Handle::channel(Nat64State::Pending);
        let clone = handle.clone();
        assert_eq!(handle.current(), Nat64State::Pending);
        let prefix = Nat64Prefix::new("2a00:1::".parse().unwrap(), 32).unwrap();
        publisher.publish(Nat64State::known([prefix]));
        assert!(handle.changed().await);
        assert_eq!(clone.current(), Nat64State::known([prefix]));
        drop(publisher);
        assert!(!handle.changed().await);
        assert_eq!(
            Nat64Handle::fixed(Nat64State::Unknown(vec![prefix])).current(),
            Nat64State::Unknown(vec![prefix])
        );
    }
}
