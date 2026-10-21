//! Bitswap's DNS address expansion asks the retriever's network's DoH
//! endpoint through a client of that network, so revoking the network stops
//! it (#45 lifecycle review, finding 2: the review's probe, now asserting
//! the refusal).
//!
//! A binary of its own: it points the process's proxy variables at a local
//! listener, which a client outside the network (a legacy one, which
//! honours them) would connect to.

use std::time::Duration;

use cid::Cid;
use freedom_ipfs_net::Network;
use freedom_ipfs_retrieval::{HttpRetriever, HTTP_PROVIDER_CLIENT_CONFIG};
use freedom_ipfs_routing::{Provider, ProviderRoutingClient};
use freedom_ipfs_store::SqliteBlockStore;
use libp2p::identity::Keypair;
use multihash::Multihash;

#[tokio::test(flavor = "multi_thread")]
async fn revoked_retrieval_starts_no_doh_request() {
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    for name in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"] {
        std::env::set_var(name, &proxy_url);
    }
    for name in ["NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy"] {
        std::env::remove_var(name);
    }

    let network = Network::legacy();
    let provider_client = network.http_client(HTTP_PROVIDER_CLIENT_CONFIG);
    network.revoke();
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever =
        HttpRetriever::new(ProviderRoutingClient::Offline, store).with_http_client(provider_client);
    let peer = Keypair::generate_ed25519().public().to_peer_id();
    let providers = vec![Provider {
        id: Some(peer.to_string()),
        addrs: vec![
            format!("/dns4/revoked.example/tcp/4001/p2p/{peer}"),
            format!("/dnsaddr/revoked.example/p2p/{peer}"),
        ],
        http_urls: Vec::new(),
    }];
    let cid = Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[8; 32]).unwrap());

    let fetch = tokio::spawn(async move { retriever.fetch_from_providers(&cid, &providers).await });
    let accepted = tokio::time::timeout(Duration::from_secs(2), proxy.accept()).await;
    fetch.abort();
    assert!(
        accepted.is_err(),
        "Bitswap DNS expansion connected through a client outside the node's revoked network"
    );

    // The control, in the same test (the process's proxy variables are
    // read once): unrevoked, the same expansion reaches the proxy, so the
    // check above watched the path it means to.
    let network = Network::legacy();
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let providers = vec![Provider {
        id: Some(peer.to_string()),
        addrs: vec![format!("/dns4/unrevoked.example/tcp/4001/p2p/{peer}")],
        http_urls: Vec::new(),
    }];
    let fetch = tokio::spawn(async move { retriever.fetch_from_providers(&cid, &providers).await });
    let accepted = tokio::time::timeout(Duration::from_secs(10), proxy.accept()).await;
    fetch.abort();
    assert!(
        accepted.is_ok(),
        "the control's expansion never reached the proxy"
    );
}
