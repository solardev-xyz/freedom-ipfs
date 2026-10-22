//! The network policy's crate (freedom-core design 0007, "Network policy",
//! upstream item 3).
//!
//! - [`gate`], the policy's pure half:
//!   - what a target is ([`gate::Target`]: an onion, an IP literal or a
//!     name), decided once, before any expansion, for URL hosts and
//!     multiaddrs alike;
//!   - whether an address may be reached ([`gate::check_address`]), from one
//!     table of special-purpose blocks ([`gate::table`]), the IPv6 forms that
//!     embed an IPv4 address, and what NAT64 discovery has learned
//!     ([`gate::Nat64State`]);
//!   - which targets are configured, and so exempt
//!     ([`gate::ConfiguredTargets`]).
//! - [`policy`]: the [`NetworkPolicy`] a node runs under, and its
//!   [`Network`], which holds the instance's [`NetworkCapability`].
//! - [`http`]: the crates' HTTP clients, from one constructor, each request
//!   asking the capability first, and under `direct` the policy's own hop
//!   loop.
//! - [`resolve`]: the gate's resolver, under `direct` with checked
//!   addresses.
//! - [`dial`]: the gate where candidates are chosen and addresses expanded
//!   ([`DialPolicy`]), and the transports a policy's dials can use
//!   ([`TransportPolicy`]).
//! - [`transport`]: the gate in the libp2p transports, around and below
//!   their DNS layer ([`transport::Gated`]), and the swarms' transports
//!   built for a network ([`transport::Transports`]); under `socks5`, TCP
//!   through the proxy (`transport::Socks5Transport`).
//! - `socks` (cargo feature `socks`): the SOCKS5 client `socks5` goes
//!   through, and with `test-util` a test proxy (`socks::fixture`).
//!
//! The legacy policy (what the crates did before) and `direct` can be
//! built. `socks5`'s libp2p transports exist (feature `socks`, off by
//! default, so mobile builds compile none of it); [`Network::new`] refuses
//! the mode until its HTTP clients do too.

pub mod capability;
pub mod dial;
pub mod gate;
pub mod http;
pub mod policy;
pub mod resolve;
#[cfg(feature = "socks")]
pub mod socks;
pub mod transport;

pub use capability::{NetworkCapability, Revoked};
pub use dial::{DialPolicy, DialRefusal, TransportPolicy};
pub use http::{ClientConfig, HttpClient, HttpError, HttpRequest, Refused};
pub use policy::{
    Addresses, Direct, Mode, Nat64Handle, Nat64Publisher, Network, NetworkError, NetworkPolicy,
    Socks5,
};
