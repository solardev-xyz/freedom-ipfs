//! The gate's pure half (design 0007, "The gate" and "Addresses"): the
//! classification and the address policy are `freedom-ipfs-addr`'s,
//! re-exported here at their old paths; the multiaddr classifier, which
//! needs libp2p's types, is this crate's ([`classify_multiaddr`],
//! [`classify_dialed`]).

mod dial;

pub use dial::{classify_dialed, classify_multiaddr};
#[cfg(feature = "socks")]
pub(crate) use freedom_ipfs_addr::canonical;
pub use freedom_ipfs_addr::{
    check_address, classify_host, classify_url, parse_url, table, AddressRefusal, Cidr,
    ConfiguredTargets, DialTarget, Form, Nat64Prefix, Nat64State, OnionHost, OnionUrl, Refusal,
    Shape, Target, UrlTarget,
};
