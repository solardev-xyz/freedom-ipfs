//! The gate's pure half (freedom-core design 0007, "The gate" and
//! "Addresses"): how a target is classified, and which addresses an
//! untrusted target may reach. No I/O, no async runtime, no network stack:
//! `freedom-ipfs-net` builds the gate's resolvers and transports on it, and
//! re-exports it as `freedom_ipfs_net::gate`, and a host process can judge
//! its own peers' addresses with the same table (design 0008: "one table and
//! one classifier serve both services").
//!
//! **One classification, before anything else.** Every target is
//! classified once, before any DNS or TXT expansion, by one function shared
//! by URLs and multiaddrs ([`classify_host`], [`classify_url`], and
//! `freedom-ipfs-net`'s multiaddr classifiers, which need libp2p's types):
//! - a host is lowercased, loses one trailing dot, and is refused if it
//!   isn't ASCII: names must already be in punycode;
//! - URL hosts follow the WHATWG rules, so numeric IPv4 forms (`0x7f.1`,
//!   `2130706433`) are literals;
//! - an address is classified by its parsed binary value, never by its
//!   text, and an IPv4-mapped IPv6 address is that IPv4 address
//!   ([`canonical`]);
//! - the classes are onion (the last label is `onion`, or a `/onion3` or
//!   `/onion` component), IP literal, and name.
//!
//! **The address policy** ([`check_address`]) accepts an untrusted target
//! only at a globally reachable unicast address: the [`table`] of refused
//! blocks, everything outside IPv6 global unicast, and the IPv6 forms that
//! embed a refused IPv4 address. While NAT64 discovery hasn't learned the
//! network's prefixes, IPv6 addresses outside the fixed forms are refused
//! ([`Nat64State`]).
//!
//! **Configured targets** (the delegated routers, the DoH endpoint) are
//! exempt by exact host and port ([`ConfiguredTargets`]). A SOCKS proxy's
//! own endpoint isn't one: it is the transport, not a target.
//!
//! **The onion route** takes only an [`OnionHost`]: a well-formed v3 onion
//! address, which only the classifier's onion targets can become.

mod address;
mod cidr;
mod classify;
mod configured;
mod nat64;
mod onion;
pub mod table;

pub use address::{check_address, AddressRefusal, Form};
pub use cidr::Cidr;
pub use classify::{
    canonical, classify_host, classify_url, parse_url, DialTarget, Refusal, Shape, Target,
    UrlTarget,
};
pub use configured::ConfiguredTargets;
pub use nat64::{Nat64Prefix, Nat64State};
pub use onion::{OnionHost, OnionUrl};
