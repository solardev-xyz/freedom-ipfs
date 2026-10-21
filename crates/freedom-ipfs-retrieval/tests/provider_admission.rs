//! Check 1 (design 0007), where providers come in: a delegated router's
//! answer, the store's provider cache, and a caller's list each pass the
//! policy, and the refusals are counted (`network_policy_providers`).
//!
//! The expansion and candidate choice refuse the same addresses again, so
//! no socket tells these admissions apart: their counts do. A binary of
//! its own, for its process-wide subscriber.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cid::Cid;
use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::{Addresses, Network, NetworkPolicy};
use freedom_ipfs_retrieval::{HttpRetriever, HTTP_PROVIDER_CLIENT_CONFIG};
use freedom_ipfs_routing::{DelegatedRoutingClient, Provider, ProviderRoutingClient};
use freedom_ipfs_store::{CachedProviderRecord, SqliteBlockStore};
use libp2p::identity::Keypair;
use multihash::Multihash;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;
use url::Url;

/// The refused-address counts of the admission's events.
#[derive(Clone, Default)]
struct Admissions(Arc<Mutex<Vec<u64>>>);

impl Admissions {
    fn take(&self) -> Vec<u64> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

#[derive(Default)]
struct Fields {
    phase: Option<String>,
    refused: Option<u64>,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "phase" {
            self.phase = Some(value.to_string());
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "refused_addr_count" {
            self.refused = Some(value);
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for Admissions {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.phase.as_deref() == Some("network_policy_providers") {
            self.0.lock().unwrap().push(fields.refused.unwrap_or(0));
        }
    }
}

/// A delegated router answering every lookup with `body`.
fn router(body: String) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let body = body.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = reader.get_mut().write_all(response.as_bytes());
            });
        }
    });
    port
}

fn cid() -> Cid {
    Cid::new_v1(0x55, Multihash::<64>::wrap(0x12, &[5; 32]).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn routing_answers_cached_records_and_callers_lists_are_admitted() {
    let admissions = Admissions::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(admissions.clone()),
    )
    .unwrap();

    let peer = Keypair::generate_ed25519().public().to_peer_id();
    // An onion, which `direct` refuses, and a dialable address that leads
    // nowhere (port 9 on loopback, discard).
    let onion = format!("/dns4/x.onion/tcp/4001/p2p/{peer}");
    let kept = format!("/ip4/127.0.0.1/tcp/9/p2p/{peer}");
    let router_port = router(format!(
        r#"{{"Providers":[{{"Schema":"peer","ID":"{peer}","Addrs":["{onion}","{kept}"]}}]}}"#
    ));
    let network = Network::new(
        NetworkPolicy::direct(
            ConfiguredTargets::new(),
            Url::parse("http://127.0.0.1:9/dns-query").unwrap(),
            Addresses::Unchecked,
        )
        .unwrap(),
    )
    .unwrap();
    let run = |retriever: HttpRetriever, providers: Option<Vec<Provider>>| async move {
        let fetch = tokio::spawn(async move {
            match providers {
                Some(providers) => {
                    let _ = retriever.fetch_from_providers(&cid(), &providers).await;
                }
                None => {
                    let _ = retriever.fetch_block(&cid()).await;
                }
            }
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        fetch.abort();
    };

    // The router's answer.
    let routing = DelegatedRoutingClient::with_endpoints_and_client(
        [format!("http://127.0.0.1:{router_port}")],
        network.http_client(freedom_ipfs_routing::HTTP_CLIENT_CONFIG),
    );
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(routing, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    run(retriever, None).await;
    assert_eq!(admissions.take().first(), Some(&1), "the router's answer");

    // The store's provider cache.
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    store
        .put_provider_records(
            &cid(),
            &[CachedProviderRecord {
                id: Some(peer.to_string()),
                addrs: vec![onion.clone(), kept.clone()],
            }],
            Duration::from_secs(60),
        )
        .unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    run(retriever, None).await;
    assert_eq!(admissions.take().first(), Some(&1), "the cached record");

    // A caller's list.
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let retriever = HttpRetriever::new(ProviderRoutingClient::Offline, store)
        .with_http_client(network.http_client(HTTP_PROVIDER_CLIENT_CONFIG));
    let providers = vec![Provider::from_parts(Some(peer.to_string()), vec![onion, kept]).unwrap()];
    run(retriever, Some(providers)).await;
    assert_eq!(admissions.take().first(), Some(&1), "a caller's list");
}
