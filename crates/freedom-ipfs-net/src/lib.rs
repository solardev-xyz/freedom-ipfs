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
//!   asking the capability first.
//!
//! Only the legacy policy, what the crates did before, can be built yet;
//! `direct` and `socks5` come with the later parts of the item.

pub mod capability;
pub mod gate;
pub mod http;
pub mod policy;

pub use capability::{NetworkCapability, Revoked};
pub use http::{ClientConfig, HttpClient, HttpError, HttpRequest};
pub use policy::{
    Addresses, Direct, Mode, Nat64Handle, Nat64Publisher, Network, NetworkError, NetworkPolicy,
    Socks5,
};
