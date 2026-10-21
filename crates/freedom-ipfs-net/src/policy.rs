//! The network policy and the network an instance runs under (design 0007,
//! "Network policy", "Modes").

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;

use crate::capability::NetworkCapability;
use crate::gate::Nat64State;
use crate::http::{build_client, ClientConfig, HttpClient};

/// What a node's network may do.
///
/// Only [`NetworkPolicy::legacy`] can be built so far: `direct` and
/// `socks5` arrive with the later parts of design 0007's upstream item 3,
/// and [`Network::new`] refuses them until then.
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

    pub fn mode(&self) -> &Mode {
        &self.mode
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
    /// Direct connections, without the environment's proxy, behind the gate.
    Direct(Direct),
    /// Every connection through a SOCKS5 proxy, failing closed.
    Socks5(Socks5),
}

/// `direct` (design 0007, "Modes"). The gate comes to it in two parts:
/// - **always:** the classification and the capability. Onion targets and
///   non-network multiaddrs are refused before any socket, the service's own
///   hop loop follows redirects, and revoking the capability stops every
///   later socket, DNS query and hop;
/// - **with [`Addresses::Checked`]:** the address policy too, which needs
///   NAT64 discovery's prefixes.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Direct {
    pub addresses: Addresses,
}

/// Whether `direct` applies the address policy.
#[derive(Clone, Debug)]
pub enum Addresses {
    /// Not yet: names resolve as before, and no address is refused.
    Unchecked,
    /// Every untrusted address is checked (design 0007, "Addresses"), with
    /// what NAT64 discovery has learned.
    Checked(Nat64Handle),
}

/// `socks5` (design 0007, "Modes").
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Socks5 {
    /// The proxy's endpoint: an IP literal, so reaching it needs no DNS.
    pub proxy: SocketAddr,
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
/// before each request, so revoking it ([`Network::revoke`]) refuses every
/// later request of every one of them. Clones share both.
#[derive(Clone, Debug)]
pub struct Network {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    policy: NetworkPolicy,
    capability: NetworkCapability,
}

impl Network {
    /// The network for `policy`, with a capability of its own. Refused for a
    /// mode this version can't honour yet, never approximated: a policy
    /// that silently connected another way would fail open.
    pub fn new(policy: NetworkPolicy) -> Result<Self, NetworkError> {
        match policy.mode() {
            Mode::Legacy => {}
            Mode::Direct(_) => return Err(NetworkError::Unsupported("direct")),
            Mode::Socks5(_) => return Err(NetworkError::Unsupported("socks5")),
        }
        Ok(Self {
            inner: Arc::new(Inner {
                policy,
                capability: NetworkCapability::new(),
            }),
        })
    }

    /// A [`NetworkPolicy::legacy`] network, with a capability of its own.
    pub fn legacy() -> Self {
        Self {
            inner: Arc::new(Inner {
                policy: NetworkPolicy::legacy(),
                capability: NetworkCapability::new(),
            }),
        }
    }

    pub fn policy(&self) -> &NetworkPolicy {
        &self.inner.policy
    }

    pub fn capability(&self) -> &NetworkCapability {
        &self.inner.capability
    }

    /// Revokes the capability: every later request of every client of this
    /// network fails with [`crate::http::HttpError::Revoked`].
    pub fn revoke(&self) {
        self.inner.capability.revoke();
    }

    /// A new HTTP client for `config`, built by the crates' one constructor
    /// for this network's policy, which asks this network's capability
    /// before every request. Each call builds a client of its own, with its
    /// own connection pool.
    ///
    /// It never panics: a client that couldn't be built holds the error
    /// ([`HttpClient::error`]) and fails every request with it.
    pub fn http_client(&self, config: ClientConfig) -> HttpClient {
        HttpClient::new(
            build_client(config, self.policy()),
            self.capability().clone(),
            config,
        )
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::Nat64Prefix;

    #[test]
    fn only_legacy_networks_can_be_built_yet() {
        assert!(matches!(
            Network::new(NetworkPolicy::legacy()).map(|n| n.policy().mode().clone()),
            Ok(Mode::Legacy)
        ));
        let direct = NetworkPolicy {
            mode: Mode::Direct(Direct {
                addresses: Addresses::Unchecked,
            }),
        };
        assert!(matches!(
            Network::new(direct),
            Err(NetworkError::Unsupported("direct"))
        ));
        let socks5 = NetworkPolicy {
            mode: Mode::Socks5(Socks5 {
                proxy: "127.0.0.1:9150".parse().unwrap(),
            }),
        };
        assert!(matches!(
            Network::new(socks5),
            Err(NetworkError::Unsupported("socks5"))
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
            Nat64Handle::fixed(Nat64State::Unknown).current(),
            Nat64State::Unknown
        );
    }
}
