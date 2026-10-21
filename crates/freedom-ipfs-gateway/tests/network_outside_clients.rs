//! Every place the node's crates build an HTTP client outside any
//! `Network`: such a client has a capability nobody can revoke
//! (`HttpClient::legacy`), so an instance's teardown doesn't stop it.
//!
//! The crates' `Default`s and plain constructors keep building them, for
//! hosts that pass no network; a node built by `OnlineNodeBuilder` replaces
//! each of those with its network's client. Bitswap's DNS address expansion
//! still builds its own DoH resolver, and so its own client, on every call:
//! design 0007's later part that takes DoH from the policy replaces those.
//! This list changes only with such a change, so the sites to replace are
//! never a guess.

/// `(file, pattern, occurrences)`: the source of each crate, whole.
const SITES: &[(&str, &str, &str, usize)] = &[
    // `CloudflareDohResolver::default` and
    // `DelegatedIpnsResolver::with_endpoints`.
    (
        "namesys",
        include_str!("../../freedom-ipfs-namesys/src/lib.rs"),
        "HttpClient::legacy(",
        2,
    ),
    // `DelegatedRoutingClient::with_endpoints`.
    (
        "routing",
        include_str!("../../freedom-ipfs-routing/src/lib.rs"),
        "HttpClient::legacy(",
        1,
    ),
    // `HttpRetriever::new`.
    (
        "retrieval",
        include_str!("../../freedom-ipfs-retrieval/src/lib.rs"),
        "HttpClient::legacy(",
        1,
    ),
    // Bitswap's DNS address expansion: `resolve_dnsaddr_hosts`,
    // `resolve_dns_ip_hosts` and `expand_provider_multiaddrs`. In a node's
    // paths, and not yet the network's.
    (
        "retrieval",
        include_str!("../../freedom-ipfs-retrieval/src/lib.rs"),
        "CloudflareDohResolver::default()",
        3,
    ),
    // The builder's fallback when it has no DoH client: never taken, since
    // only an offline node has none, and an offline node resolves no name.
    (
        "gateway node",
        include_str!("../src/node.rs"),
        "CloudflareDohResolver::default()",
        1,
    ),
    (
        "gateway node",
        include_str!("../src/node.rs"),
        "HttpClient::legacy(",
        0,
    ),
    (
        "gateway",
        include_str!("../src/lib.rs"),
        "CloudflareDohResolver::default()",
        0,
    ),
    (
        "mobile",
        include_str!("../../freedom-ipfs-mobile/src/lib.rs"),
        "CloudflareDohResolver::default()",
        0,
    ),
];

#[test]
fn the_clients_outside_a_network_are_these() {
    for (name, source, pattern, expected) in SITES {
        assert_eq!(
            source.matches(pattern).count(),
            *expected,
            "{pattern} in {name}: update this list, and design 0007's plan, with the change"
        );
    }
}
