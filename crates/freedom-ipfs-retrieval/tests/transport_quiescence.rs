//! The retriever's Bitswap swarm dials through its network's transports
//! (design 0007, check 3): a dial in flight holds a permit of the network's
//! capability through its connect and handshake, so a revoke drops it and
//! the capability's quiescence waits for it.

use std::io::Read;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use freedom_ipfs_net::Network;
use freedom_ipfs_retrieval::{HttpRetriever, HTTP_PROVIDER_CLIENT_CONFIG};
use freedom_ipfs_routing::{Provider, ProviderRoutingClient};
use freedom_ipfs_store::SqliteBlockStore;
use libp2p::identity::Keypair;
use multihash::Multihash;

#[tokio::test(flavor = "multi_thread")]
async fn a_bitswap_handshake_in_flight_holds_a_permit_and_is_dropped_at_the_revoke() {
    // A listener that accepts and never answers: the dial's handshake waits.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let held = accepted.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            held.lock().unwrap().push(stream);
        }
    });

    let network = Network::legacy();
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let peer = Keypair::generate_ed25519().public().to_peer_id();
    let providers = vec![Provider::from_parts(
        Some(peer.to_string()),
        vec![format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer}")],
    )
    .unwrap()];
    let cid = Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[3; 32]).unwrap());
    let fetch = tokio::spawn(async move {
        let _ = retriever.fetch_from_providers(&cid, &providers).await;
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while accepted.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dial never connected"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        network.capability().permits() >= 1,
        "the dial in flight holds no permit of the network's capability"
    );
    network.revoke();
    tokio::time::timeout(Duration::from_secs(2), network.capability().quiesced())
        .await
        .expect("quiescence once the dial is dropped");
    // The dropped dial closed its socket.
    let mut stream = accepted.lock().unwrap().remove(0);
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buffer = [0u8; 4096];
    let mut closed = false;
    for _ in 0..16 {
        match stream.read(&mut buffer) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    fetch.abort();
    assert!(closed, "the dropped dial's socket stayed open");
}
