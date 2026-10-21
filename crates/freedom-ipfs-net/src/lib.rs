//! The network policy's crate (freedom-core design 0007, "Network policy",
//! upstream item 3).
//!
//! So far it holds [`gate`], the policy's pure half:
//! - what a target is ([`gate::Target`]: an onion, an IP literal or a name),
//!   decided once, before any expansion, for URL hosts and multiaddrs alike;
//! - whether an address may be reached ([`gate::check_address`]), from one
//!   table of special-purpose blocks ([`gate::table`]), the IPv6 forms that
//!   embed an IPv4 address, and what NAT64 discovery has learned
//!   ([`gate::Nat64State`]);
//! - which targets are configured, and so exempt
//!   ([`gate::ConfiguredTargets`]).
//!
//! It does no I/O: no socket, no lookup, no clock. The HTTP clients, the
//! resolver and the transports that apply it come in later parts of the
//! item. Nothing calls it yet.

pub mod gate;
