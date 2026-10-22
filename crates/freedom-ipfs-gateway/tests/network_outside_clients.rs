//! Every place the node's crates build an HTTP client outside any
//! `Network`: such a client has a capability nobody can revoke
//! (`HttpClient::legacy`), so an instance's teardown doesn't stop it.
//!
//! **A node built by `OnlineNodeBuilder` uses none of them.** Every client
//! in its paths is its network's: the DoH, IPNS, routing and provider
//! clients, and since design 0007's DoH part, the client Bitswap's DNS
//! address expansion asks, which the retriever builds from its provider
//! client's network (and so its DoH endpoint is the policy's).
//!
//! What is left:
//! - the crates' `Default`s and plain constructors, for hosts that pass no
//!   network; a node built by the builder replaces each of them with its
//!   network's client;
//! - the mobile preload's client, which only reaches the add-on's own
//!   loopback gateway.
//!
//! This list changes only with such a change, so the sites are never a
//! guess.

/// `(crate, source, pattern, occurrences)`: the source of each crate, whole.
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
    // `HttpRetriever::new`. Bitswap's DNS address expansion builds its DoH
    // client from that client's network.
    (
        "retrieval",
        include_str!("../../freedom-ipfs-retrieval/src/lib.rs"),
        "HttpClient::legacy(",
        1,
    ),
    // None: Bitswap's DNS address expansion asks the retriever's network's
    // DoH endpoint, through a client of that network.
    (
        "retrieval",
        include_str!("../../freedom-ipfs-retrieval/src/lib.rs"),
        "CloudflareDohResolver::default()",
        0,
    ),
    // None: the builder's DNSLink resolver always gets its network's client.
    (
        "gateway node",
        include_str!("../src/node.rs"),
        "CloudflareDohResolver::default()",
        0,
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
    (
        "mobile",
        include_str!("../../freedom-ipfs-mobile/src/lib.rs"),
        "HttpClient::legacy(",
        0,
    ),
    // The preload's client: the add-on's own loopback gateway, only.
    (
        "mobile",
        include_str!("../../freedom-ipfs-mobile/src/lib.rs"),
        "reqwest::Client::builder()",
        1,
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
