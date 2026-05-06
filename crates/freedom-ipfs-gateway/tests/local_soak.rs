use axum::http::StatusCode;
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::router;
use freedom_ipfs_store::SqliteBlockStore;
use std::env;
use tokio::net::TcpListener;

const DEFAULT_SOAK_REQUESTS: usize = 500;
const DEFAULT_MAX_RSS_GROWTH_KIB: u64 = 32 * 1024;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "local gateway soak test; opt in with make local-soak"]
async fn local_gateway_repeated_cached_reads_keep_rss_bounded() {
    let requests = env_usize("FREEDOM_IPFS_SOAK_REQUESTS", DEFAULT_SOAK_REQUESTS);
    let max_rss_growth_kib = env_u64(
        "FREEDOM_IPFS_MAX_RSS_GROWTH_KIB",
        DEFAULT_MAX_RSS_GROWTH_KIB,
    );
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let data = vec![b'x'; 4096];
    let cid = cid_from_data(CODEC_RAW, &data);
    store.put_block(&cid, &data).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(store)).await.unwrap();
    });

    let rss_before = current_rss_kib();
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/ipfs/{cid}");
    for _ in 0..requests {
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), data.as_slice());
    }
    let rss_after = current_rss_kib();

    eprintln!(
        "local gateway soak completed: requests={requests} rss_before_kib={rss_before:?} rss_after_kib={rss_after:?}"
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        assert!(
            after <= before.saturating_add(max_rss_growth_kib),
            "RSS grew by {} KiB across {requests} cached gateway requests, max allowed {max_rss_growth_kib} KiB",
            after.saturating_sub(before)
        );
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(target_os = "linux")]
fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kib() -> Option<u64> {
    None
}
