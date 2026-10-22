//! What the network policy's end-to-end tests share: a delegated router's
//! and a DoH endpoint's stand-ins on loopback, and a node built through the
//! builder for a network.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use cid::Cid;
use freedom_ipfs_gateway::{Network, OnlineNode, RoutingMode};
use freedom_ipfs_net::gate::ConfiguredTargets;
use freedom_ipfs_net::socks::fixture::{HttpServer, Reply};
use freedom_ipfs_store::SqliteBlockStore;
use reqwest::Url;

pub const PEER_A: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
pub const PEER_B: &str = "12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA";
pub const PEER_C: &str = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
pub const DNSLINK: &str = "site.example";

/// A 404.
pub fn not_found() -> Reply {
    Reply {
        status: 404,
        location: None,
        body: String::new(),
    }
}

/// A DoH endpoint (JSON) on loopback: `_dnslink.site.example` answers with
/// `cid`; every other question has no answer. It records each question as
/// `name/type`.
pub async fn doh(cid: Cid, questions: Arc<Mutex<Vec<String>>>) -> HttpServer {
    HttpServer::start(move |path, _| {
        let query = path.split_once('?').map_or("", |(_, query)| query);
        let mut name = String::new();
        let mut kind = String::new();
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("name", value)) => name = value.to_string(),
                Some(("type", value)) => kind = value.to_string(),
                _ => {}
            }
        }
        questions.lock().unwrap().push(format!("{name}/{kind}"));
        let owner = format!("_dnslink.{DNSLINK}");
        let body = if name.trim_end_matches('.') == owner && matches!(kind.as_str(), "TXT" | "16") {
            serde_json::json!({ "Status": 0, "Answer": [
                { "name": owner, "type": 16, "TTL": 60,
                  "data": format!("\"dnslink=/ipfs/{cid}\"") },
            ]})
        } else {
            serde_json::json!({ "Status": 3 })
        };
        Reply::ok(&body.to_string())
    })
    .await
}

/// The node's routers and DoH endpoint, configured by IP literal.
pub fn configured(router: &HttpServer, doh: &HttpServer) -> (ConfiguredTargets, String, Url) {
    let router_url = format!("http://{}/routing/v1", router.addr());
    let doh_url = Url::parse(&format!("http://{}/dns-query", doh.addr())).unwrap();
    let mut configured = ConfiguredTargets::new();
    configured
        .add_url(&Url::parse(&router_url).unwrap())
        .unwrap();
    (configured, router_url, doh_url)
}

pub fn node(network: Network, router_url: &str) -> OnlineNode {
    OnlineNode::builder(SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap())
        .routing_mode(RoutingMode::Delegated)
        .delegated_routers([router_url.to_string()])
        .network(network)
        .build()
}
