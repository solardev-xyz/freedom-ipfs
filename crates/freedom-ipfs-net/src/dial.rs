//! The gate where candidates are chosen and addresses expanded (design
//! 0007, "The gate", *Where it is applied*, checks 1 and 2, and *Candidate
//! choice knows the transports*).
//!
//! - **Before expansion (check 1):** a provider's multiaddr is checked
//!   whole, against the shapes the transports support, and classified. A
//!   refused shape or an onion stops there: nothing is expanded or resolved
//!   for it. With checked addresses, an IP literal must pass the address
//!   policy too.
//! - **After expansion (check 2):** every multiaddr a `/dnsaddr` TXT answer
//!   yields is checked the same way, and with checked addresses every A or
//!   AAAA answer passes the address policy before it replaces a `/dns*`
//!   component.
//! - **Candidate choice:** an address the policy's transports can't carry
//!   (QUIC under `socks5`) is refused like a refused target, so it goes
//!   before candidates are ranked and cut to two per peer.
//!
//! Under the legacy policy there is no gate: every address passes, as
//! before. The transports are the last word (design 0007, check 3): these
//! checks choose what to try, and keep refused targets from spending a
//! lookup or a candidate's place.

use std::net::IpAddr;
use std::sync::Arc;

use multiaddr::Multiaddr;
use tokio::time::Instant;
use url::Url;

use crate::capability::{NetworkCapability, Revoked};
use crate::gate::{
    check_address, classify_dialed, classify_url, AddressRefusal, ConfiguredTargets, Nat64State,
    Refusal, Shape, Target,
};
use crate::policy::{Addresses, Mode, Nat64Handle, NetworkPolicy};
use crate::resolve::check_waiting;

/// The transports a policy's dials can use, as candidate choice sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransportPolicy {
    quic: bool,
}

impl TransportPolicy {
    /// Every transport the node has: the legacy policy's and `direct`'s.
    pub const ALL: Self = Self { quic: true };
    /// TCP only, as under `socks5`: a SOCKS proxy carries no QUIC.
    pub const TCP_ONLY: Self = Self { quic: false };

    /// Whether QUIC is dialed.
    pub fn quic(&self) -> bool {
        self.quic
    }

    /// Whether a dialed shape goes over a transport the policy has.
    pub fn carries(&self, shape: Shape) -> bool {
        shape != Shape::QuicV1 || self.quic
    }
}

/// Why an address is refused at candidate choice or expansion. Each text
/// names the rule, never the address.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DialRefusal {
    /// The network's capability is revoked: nothing more is admitted.
    #[error("{0}")]
    Revoked(#[from] Revoked),
    /// Not one of the shapes the node dials, or a host the gate refuses.
    #[error("{0}")]
    Target(Refusal),
    /// An onion: `direct` has no route to one.
    #[error("an onion can't be reached in direct mode")]
    Onion,
    /// The address policy refuses the address.
    #[error("{0}")]
    Address(AddressRefusal),
    /// The policy's transports can't carry the shape (QUIC under `socks5`).
    #[error("the network policy's transports can't carry this address")]
    Transport,
}

/// What a network may dial, as candidate choice and address expansion ask
/// it ([`crate::Network::dial_policy`]). Clones share the NAT64 state and the
/// network's capability.
///
/// **Every check asks the network's capability first** (design 0007, "The
/// gate"), in every mode: once it is revoked, nothing is admitted. A check
/// that waits for NAT64 discovery waits under a permit of the capability,
/// so a revoke ends the wait, and asks once more before it admits.
#[derive(Clone, Debug)]
pub struct DialPolicy {
    transports: TransportPolicy,
    gate: Gate,
    /// The network's capability; `None` for [`DialPolicy::legacy`], which
    /// belongs to no network.
    capability: Option<NetworkCapability>,
}

#[derive(Clone, Debug)]
enum Gate {
    /// The legacy policy: no gate.
    Off,
    /// `direct` with unchecked addresses: the shapes and the classification.
    Classify,
    /// The address policy too, with configured targets exempt by exact host
    /// and port.
    Check {
        configured: Arc<ConfiguredTargets>,
        nat64: Nat64Handle,
    },
}

impl DialPolicy {
    /// The legacy policy's, outside any network: no gate, every transport,
    /// and no capability.
    pub fn legacy() -> Self {
        Self {
            transports: TransportPolicy::ALL,
            gate: Gate::Off,
            capability: None,
        }
    }

    pub(crate) fn for_network(policy: &NetworkPolicy, capability: &NetworkCapability) -> Self {
        Self {
            capability: Some(capability.clone()),
            ..Self::for_policy(policy)
        }
    }

    fn for_policy(policy: &NetworkPolicy) -> Self {
        match policy.mode() {
            Mode::Legacy => Self::legacy(),
            Mode::Direct(direct) => Self {
                capability: None,
                transports: TransportPolicy::ALL,
                gate: match &direct.addresses {
                    Addresses::Unchecked => Gate::Classify,
                    Addresses::Checked(nat64) => Gate::Check {
                        configured: direct.configured.clone(),
                        nat64: nat64.clone(),
                    },
                },
            },
            // Never built yet (`Network::new` refuses it): the design's
            // socks5 learns no NAT64 prefix, so only the fixed forms apply,
            // and its proxy is its one configured target.
            Mode::Socks5(socks5) => {
                let mut configured = ConfiguredTargets::new();
                configured.add_endpoint(socks5.proxy);
                Self {
                    capability: None,
                    transports: TransportPolicy::TCP_ONLY,
                    gate: Gate::Check {
                        configured: Arc::new(configured),
                        nat64: Nat64Handle::fixed(Nat64State::known([])),
                    },
                }
            }
        }
    }

    /// The same policy over `transports`: what its dials can use when a
    /// transport is off.
    pub fn with_transports(mut self, transports: TransportPolicy) -> Self {
        self.transports = transports;
        self
    }

    /// The transports the policy's dials can use.
    pub fn transports(&self) -> TransportPolicy {
        self.transports
    }

    /// Whether the policy has a gate: `false` under the legacy policy, where
    /// every address passes.
    pub fn is_gated(&self) -> bool {
        !matches!(self.gate, Gate::Off)
    }

    /// Whether every check passes: no gate, and a capability that isn't
    /// revoked. A caller may skip the checks then, as the legacy policy
    /// did before.
    pub fn admits_everything(&self) -> bool {
        !self.is_gated() && self.ask().is_ok()
    }

    /// The capability, first in every check.
    fn ask(&self) -> Result<(), DialRefusal> {
        match &self.capability {
            Some(capability) => capability.check().map_err(DialRefusal::Revoked),
            None => Ok(()),
        }
    }

    /// Runs a wait for NAT64 discovery under a permit of the capability (a
    /// revoke ends it), and asks the capability once more before admitting.
    async fn admit_after<F>(&self, wait: F) -> Result<(), DialRefusal>
    where
        F: std::future::Future<Output = Result<(), DialRefusal>>,
    {
        let Some(capability) = &self.capability else {
            return wait.await;
        };
        let permit = capability.admit()?;
        permit.run(wait).await??;
        self.ask()
    }

    /// The same policy with NAT64 discovery's state frozen, once none of
    /// `addrs` waits for it any more (or `deadline` has passed): every
    /// check of the copy judges against that one state, so candidates
    /// judged one after the other can't each see a different one (design
    /// 0007: an address accepted before discovery changed the state might
    /// be refused after it). The wait runs under a permit.
    pub async fn settled(
        &self,
        addrs: &[Multiaddr],
        deadline: Instant,
    ) -> Result<DialPolicy, DialRefusal> {
        self.ask()?;
        let Gate::Check { configured, nat64 } = &self.gate else {
            return Ok(self.clone());
        };
        let ips: Vec<IpAddr> = addrs
            .iter()
            .filter_map(|addr| match classify_dialed(addr) {
                Ok(dial) if !configured.contains_dial(&dial) => match dial.target {
                    Target::Ip(ip) => Some(ip),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        let mut handle = nat64.clone();
        let state = std::sync::Mutex::new(handle.current());
        let wait = async {
            loop {
                let current = handle.current();
                let waits = ips.iter().any(|ip| {
                    matches!(
                        check_address(*ip, &current),
                        Err(AddressRefusal::Nat64Pending)
                    )
                });
                *state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = current;
                if !waits || !handle.changed().await {
                    return Ok(());
                }
            }
        };
        // Past the deadline, the state as it is: a pending IPv6 address is
        // refused.
        let _ = tokio::time::timeout_at(deadline, self.admit_after(wait)).await;
        self.ask()?;
        let state = state
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(Self {
            gate: Gate::Check {
                configured: configured.clone(),
                nat64: Nat64Handle::fixed(state),
            },
            ..self.clone()
        })
    }

    /// Checks a multiaddr to be dialed, or expanded (a `/dnsaddr`), now:
    /// its whole shape and its target (an onion is refused), the transport
    /// it needs, and with checked addresses an untrusted IP literal's
    /// address. While NAT64 discovery is pending an IPv6 literal is refused
    /// with [`AddressRefusal::Nat64Pending`]; [`DialPolicy::check`] waits
    /// for discovery instead.
    pub fn check_now(&self, addr: &Multiaddr) -> Result<(), DialRefusal> {
        self.ask()?;
        match self.classify(addr)? {
            Some((ip, nat64)) => check_address(ip, &nat64.current())
                .map(drop)
                .map_err(DialRefusal::Address),
            None => Ok(()),
        }
    }

    /// [`DialPolicy::check_now`], waiting while NAT64 discovery is pending,
    /// until `deadline`: past it, the IPv6 literal is refused with
    /// [`AddressRefusal::Nat64Pending`].
    pub async fn check(&self, addr: &Multiaddr, deadline: Instant) -> Result<(), DialRefusal> {
        self.ask()?;
        match self.classify(addr)? {
            Some((ip, nat64)) => {
                self.admit_after(wait_for_address(ip, nat64, deadline))
                    .await
            }
            None => Ok(()),
        }
    }

    /// Checks one A or AAAA answer for `addr`'s `/dns*` host before it
    /// replaces the name (check 2): with checked addresses, it must pass the
    /// address policy, unless `addr`'s name and port are a configured
    /// target. Without checked addresses every answer passes; `addr` itself
    /// was checked before its name was resolved.
    pub async fn check_answer(
        &self,
        addr: &Multiaddr,
        ip: IpAddr,
        deadline: Instant,
    ) -> Result<(), DialRefusal> {
        self.ask()?;
        let Gate::Check { configured, nat64 } = &self.gate else {
            return Ok(());
        };
        let dial = classify_dialed(addr).map_err(DialRefusal::Target)?;
        if configured.contains_dial(&dial) {
            return Ok(());
        }
        self.admit_after(wait_for_address(ip, nat64.clone(), deadline))
            .await
    }

    /// Checks an address an A or AAAA answer put in place of `original`'s
    /// name, now: `expanded`'s own shape and transport, and, with checked
    /// addresses, its address, unless `original`'s name and port are a
    /// configured target. The exemption is that name's and port's only: the
    /// same address reached any other way is judged as untrusted.
    pub fn check_answer_now(
        &self,
        original: &Multiaddr,
        expanded: &Multiaddr,
    ) -> Result<(), DialRefusal> {
        self.ask()?;
        let Some((ip, nat64)) = self.classify(expanded)? else {
            return Ok(());
        };
        if self.is_configured_answer(original, expanded) {
            return Ok(());
        }
        check_address(ip, &nat64.current())
            .map(drop)
            .map_err(DialRefusal::Address)
    }

    /// Whether `addr`'s target and port are configured, and so exempt from
    /// the address policy: never without checked addresses, which have no
    /// address policy to be exempt from.
    pub fn is_configured(&self, addr: &Multiaddr) -> bool {
        let Gate::Check { configured, .. } = &self.gate else {
            return false;
        };
        classify_dialed(addr).is_ok_and(|dial| configured.contains_dial(&dial))
    }

    /// Whether `expanded` is an answer for `original`'s configured name and
    /// port: `original` is configured, and `expanded` is `original` with
    /// its `/dns*` host, and only it, replaced by an IP address, so the
    /// port and every other component are the ones the exemption was
    /// given for.
    pub fn is_configured_answer(&self, original: &Multiaddr, expanded: &Multiaddr) -> bool {
        self.is_configured(original) && is_answer_for(original, expanded)
    }

    /// Checks an HTTP provider's base URL before it is tried, now: `http` or
    /// `https`, no userinfo, not an onion, and with checked addresses an
    /// untrusted IP literal's address (an IPv6 one is refused while NAT64
    /// discovery is pending). A name is checked when it is resolved, by the
    /// client's own resolver.
    pub fn check_url_now(&self, url: &Url) -> Result<(), DialRefusal> {
        self.ask()?;
        match self.classify_url(url)? {
            Some((ip, nat64)) => check_address(ip, &nat64.current())
                .map(drop)
                .map_err(DialRefusal::Address),
            None => Ok(()),
        }
    }

    /// [`DialPolicy::check_url_now`], waiting while NAT64 discovery is
    /// pending, until `deadline`.
    pub async fn check_url(&self, url: &Url, deadline: Instant) -> Result<(), DialRefusal> {
        self.ask()?;
        match self.classify_url(url)? {
            Some((ip, nat64)) => {
                self.admit_after(wait_for_address(ip, nat64, deadline))
                    .await
            }
            None => Ok(()),
        }
    }

    /// A URL's scheme, userinfo and target checks: then the untrusted IP
    /// literal the address policy must still pass, if any.
    fn classify_url(&self, url: &Url) -> Result<Option<(IpAddr, Nat64Handle)>, DialRefusal> {
        if matches!(self.gate, Gate::Off) {
            return Ok(None);
        }
        let target = classify_url(url).map_err(DialRefusal::Target)?;
        if target.target.is_onion() {
            return Err(DialRefusal::Onion);
        }
        Ok(match (&self.gate, &target.target) {
            (Gate::Check { configured, nat64 }, Target::Ip(ip))
                if !configured.contains(&target.target, target.port) =>
            {
                Some((*ip, nat64.clone()))
            }
            _ => None,
        })
    }

    /// The shape, target and transport checks: then the untrusted IP
    /// literal the address policy must still pass, if any (`None` without a
    /// gate).
    fn classify(&self, addr: &Multiaddr) -> Result<Option<(IpAddr, Nat64Handle)>, DialRefusal> {
        if matches!(self.gate, Gate::Off) {
            return Ok(None);
        }
        let dial = classify_dialed(addr).map_err(DialRefusal::Target)?;
        if dial.target.is_onion() {
            return Err(DialRefusal::Onion);
        }
        if !self.transports.carries(dial.shape) {
            return Err(DialRefusal::Transport);
        }
        Ok(match (&self.gate, &dial.target) {
            (Gate::Check { configured, nat64 }, Target::Ip(ip))
                if !configured.contains_dial(&dial) =>
            {
                Some((*ip, nat64.clone()))
            }
            _ => None,
        })
    }
}

/// Whether `expanded` is `original` with its `/dns*` host component, and
/// only it, replaced by an IP address (a `/p2p` suffix on either aside).
fn is_answer_for(original: &Multiaddr, expanded: &Multiaddr) -> bool {
    use multiaddr::Protocol;
    fn without_peer(addr: &Multiaddr) -> Vec<Protocol<'_>> {
        let mut protocols: Vec<Protocol<'_>> = addr.iter().collect();
        if matches!(protocols.last(), Some(Protocol::P2p(_))) {
            protocols.pop();
        }
        protocols
    }
    let (original, expanded) = (without_peer(original), without_peer(expanded));
    match (original.split_first(), expanded.split_first()) {
        (
            Some((Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_), original_rest)),
            Some((Protocol::Ip4(_) | Protocol::Ip6(_), expanded_rest)),
        ) => original_rest == expanded_rest,
        _ => false,
    }
}

/// The address policy for `ip`, waiting while NAT64 discovery is pending
/// until `deadline`.
async fn wait_for_address(
    ip: IpAddr,
    mut nat64: Nat64Handle,
    deadline: Instant,
) -> Result<(), DialRefusal> {
    match tokio::time::timeout_at(deadline, check_waiting(ip, &mut nat64)).await {
        Ok(result) => result.map(drop).map_err(DialRefusal::Address),
        Err(_) => Err(DialRefusal::Address(AddressRefusal::Nat64Pending)),
    }
}
