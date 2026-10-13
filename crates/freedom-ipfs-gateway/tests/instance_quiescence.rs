//! An owned instance's teardown proves quiescence by what the kernel sees,
//! not only by task handles (design 0007, "Instances and their teardown").
//!
//! The instance fetches blocks that never arrive: a delegated router names
//! an HTTP provider that accepts and never answers, and a Bitswap peer that
//! accepts and never speaks. So when it is torn down, requests are blocked
//! inside the synchronous block provider, with sockets open to the router
//! (pooled), the provider and the peer. After `Instance::shutdown`:
//! - no socket the instance opened is still open (`/proc/self/fd`, each
//!   socket matched in `/proc/self/net/*`);
//! - no thread of its runtime is still running (`/proc/self/task/*/comm`),
//!   so no task and no blocking job is either;
//! - its blocking regions and task set are empty, and its store is closed;
//! - no new connection reaches the servers afterwards (a canary).
//!
//! Linux only: it reads `/proc`.

#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::to_bytes;
use axum::http::{HeaderMap, Method};
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::{GatewayCoreRequest, InstanceConfig, OnlineNode, RoutingMode};
use freedom_ipfs_store::SqliteBlockStore;

const THREAD_NAME: &str = "fipfs-quiesce";
const PEER_A: &str = "12D3KooWAC7ALVECw2xQHccT3hSUcQU7pJ3hvwV77w1kEevC5kKG";
const PEER_B: &str = "12D3KooWAtxJkDLacJdK7yZkk2iPp8iMdSVh1bDHzmJ3t8oKUkqA";

/// A server on its own thread, outside every runtime, counting connections
/// and keeping each one open.
struct Server {
    port: u16,
    accepted: Arc<AtomicUsize>,
}

impl Server {
    fn start(answer: Option<String>) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicUsize::new(0));
        let held = Arc::new(Mutex::new(Vec::new()));
        let counter = accepted.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                counter.fetch_add(1, Ordering::SeqCst);
                match &answer {
                    // An HTTP server that answers every request on the
                    // connection with `answer`, keeping it alive.
                    Some(body) => {
                        let body = body.clone();
                        std::thread::spawn(move || serve_http(stream, &body));
                    }
                    // A peer that accepts and never says a word.
                    None => held.lock().unwrap().push(stream),
                }
            }
        });
        Server { port, accepted }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

fn serve_http(mut stream: TcpStream, body: &str) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        while let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            buffer.drain(..end + 4);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            if stream.write_all(response.as_bytes()).is_err() {
                return;
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
    }
}

/// The inodes of every socket this process has open.
fn open_sockets() -> BTreeSet<u64> {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter_map(|target| {
            let target = target.to_string_lossy().into_owned();
            target
                .strip_prefix("socket:[")?
                .strip_suffix(']')?
                .parse()
                .ok()
        })
        .collect()
}

/// The inodes of TCP sockets whose local port is one of `ports`: the
/// servers' side of their connections.
fn server_side(ports: &[u16]) -> BTreeSet<u64> {
    let mut inodes = BTreeSet::new();
    for table in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        let Ok(text) = std::fs::read_to_string(table) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (Some(local), Some(inode)) = (fields.get(1), fields.get(9)) else {
                continue;
            };
            let port = local
                .rsplit(':')
                .next()
                .and_then(|port| u16::from_str_radix(port, 16).ok());
            if port.is_some_and(|port| ports.contains(&port)) {
                if let Ok(inode) = inode.parse() {
                    inodes.insert(inode);
                }
            }
        }
    }
    inodes
}

/// Sockets opened since `baseline` that aren't the servers' side.
fn instance_sockets(baseline: &BTreeSet<u64>, ports: &[u16]) -> BTreeSet<u64> {
    let servers = server_side(ports);
    open_sockets()
        .into_iter()
        .filter(|inode| !baseline.contains(inode) && !servers.contains(inode))
        .collect()
}

/// The process's threads that belong to the instance's runtime.
fn instance_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path().join("comm")).ok())
        .filter(|name| name.trim_end().starts_with(THREAD_NAME))
        .count()
}

fn wait_for(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn teardown_leaves_no_socket_thread_task_or_blocking_job() {
    let provider = Server::start(None);
    let peer = Server::start(None);
    let providers = serde_json::json!({ "Providers": [
        { "Schema": "peer", "ID": PEER_A,
          "Addrs": [format!("/ip4/127.0.0.1/tcp/{}/http", provider.port)] },
        { "Schema": "peer", "ID": PEER_B,
          "Addrs": [format!("/ip4/127.0.0.1/tcp/{}", peer.port)] },
    ]});
    let router = Server::start(Some(providers.to_string()));
    let ports = [provider.port, peer.port, router.port];

    // The test's own runtime, built first: the first runtime with signal
    // handling creates tokio's process-wide signal pipe (a socket pair that
    // lives as long as the process), so it belongs to the baseline, not to
    // the instance.
    let waiter = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let baseline = open_sockets();
    assert_eq!(instance_threads(), 0);
    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    let instance = OnlineNode::builder(store.clone())
        .routing_mode(RoutingMode::Delegated)
        .delegated_routers([format!("http://127.0.0.1:{}/routing/v1", router.port)])
        .build_instance(InstanceConfig {
            thread_name: THREAD_NAME.into(),
            worker_threads: 2,
            max_blocking_threads: 16,
        })
        .unwrap();
    // Its runtime starts its workers right after it is built.
    wait_for(
        "the instance's runtime threads",
        Duration::from_secs(5),
        || instance_threads() > 0,
    );

    // Requests for blocks nobody will deliver, each under its own token.
    let core = instance.gateway_core();
    let mut requests = Vec::new();
    for n in 0..3 {
        let cid = cid_from_data(CODEC_RAW, format!("never delivered {n}").as_bytes());
        let token = instance.request_token();
        let core = core.clone();
        let handle = instance.spawn_request(token.clone(), async move {
            let response = core
                .handle(GatewayCoreRequest::ipfs(
                    cid.to_string(),
                    Method::GET,
                    HeaderMap::new(),
                ))
                .await;
            to_bytes(response.into_body(), usize::MAX).await.ok()
        });
        requests.push((token, handle.unwrap()));
    }

    // Wait until the requests are blocked inside the block provider, with
    // connections open to the router and the provider.
    wait_for("the provider's connection", Duration::from_secs(20), || {
        provider.accepted() > 0 && router.accepted() > 0
    });
    wait_for("blocking regions", Duration::from_secs(20), || {
        instance.context().blocking_regions() > 0
    });
    let open = instance_sockets(&baseline, &ports);
    assert!(
        !open.is_empty(),
        "the instance has sockets open before teardown"
    );

    // One request's own cancel ends it while it is blocked, and leaves the
    // others running.
    let (token, handle) = requests.remove(0);
    token.cancel();
    let cancelled =
        waiter.block_on(async { tokio::time::timeout(Duration::from_secs(5), handle).await });
    assert_eq!(
        cancelled.unwrap().unwrap(),
        None,
        "the request ended as cancelled"
    );
    assert!(instance.context().tasks() >= requests.len());

    // Teardown.
    let quiescence = instance.shutdown(Duration::from_secs(5));
    assert!(quiescence.reached, "{quiescence:?}");
    assert!(quiescence.joined && quiescence.drained, "{quiescence:?}");
    assert_eq!(quiescence.tasks_dropped, 0);
    assert!(quiescence.store_closed);

    // What the kernel sees, right away: no socket of the instance, and no
    // thread of its runtime.
    let left = instance_sockets(&baseline, &ports);
    assert!(
        left.is_empty(),
        "sockets still open after teardown: {left:?} (were {open:?})"
    );
    assert_eq!(instance_threads(), 0, "runtime threads still running");

    // The requests ended, as cancelled.
    for (_, handle) in requests {
        let ended =
            waiter.block_on(async { tokio::time::timeout(Duration::from_secs(5), handle).await });
        assert!(matches!(ended, Ok(Ok(None)) | Ok(Err(_))), "{ended:?}");
    }

    // The store is closed for every clone.
    assert!(store.get(&cid_from_data(CODEC_RAW, b"x")).is_err());

    // Canary: nothing of the instance connects after its teardown.
    let counts = (provider.accepted(), peer.accepted(), router.accepted());
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        counts,
        (provider.accepted(), peer.accepted(), router.accepted())
    );
    assert!(instance_sockets(&baseline, &ports).is_empty());
}

#[test]
fn an_idle_instance_shuts_down_at_once() {
    let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
    let instance = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .build_instance(InstanceConfig {
            thread_name: "fipfs-idle".into(),
            worker_threads: 1,
            max_blocking_threads: 4,
        })
        .unwrap();
    let started = Instant::now();
    let quiescence = instance.shutdown(Duration::from_secs(5));
    assert!(quiescence.reached, "{quiescence:?}");
    assert_eq!(quiescence.tasks_dropped, 0);
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// An instance with an idle offline node, for the stuck-work cases.
fn idle_instance(thread_name: &str) -> (freedom_ipfs_gateway::Instance, SqliteBlockStore) {
    let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
    let instance = OnlineNode::builder(store.clone())
        .routing_mode(RoutingMode::Offline)
        .build_instance(InstanceConfig {
            thread_name: thread_name.into(),
            worker_threads: 1,
            max_blocking_threads: 4,
        })
        .unwrap();
    (instance, store)
}

fn store_is_open(store: &SqliteBlockStore) -> bool {
    store.get(&cid_from_data(CODEC_RAW, b"probe")).is_ok()
}

#[test]
fn an_untracked_library_blocking_job_holds_quiescence_until_it_ends() {
    // As a library's own blocking work would (reqwest's getaddrinfo, for
    // one): a job on the instance's blocking pool that no counter sees.
    let (instance, store) = idle_instance("fipfs-untracked");
    let (release, stuck) = std::sync::mpsc::channel::<()>();
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let job = instance.context().handle().spawn_blocking(move || {
        let _ = entered_tx.send(());
        stuck.recv().is_ok()
    });
    entered.recv().unwrap();
    assert_eq!(instance.context().blocking_regions(), 0, "untracked");

    // Nothing proves quiescence while it runs: not a bounded shutdown...
    let teardown = instance.begin_shutdown(Duration::from_millis(50));
    assert_eq!(teardown.wait_timeout(Duration::from_millis(700)), None);
    assert!(!teardown.is_finished());
    // ...and nothing of the instance is released: the store stays open.
    assert!(store_is_open(&store));
    assert!(instance_thread_named("fipfs-untracked"));

    // Once it ends, the join returns, and then the store closes.
    release.send(()).unwrap();
    let quiescence = teardown.wait();
    assert!(quiescence.reached && quiescence.joined, "{quiescence:?}");
    assert!(
        quiescence.drained,
        "the counters were zero all along: {quiescence:?}"
    );
    assert!(!store_is_open(&store));
    assert!(!instance_thread_named("fipfs-untracked"));
    drop(job);
}

#[test]
fn a_bounded_shutdown_reports_unproven_quiescence_while_work_runs() {
    let (instance, store) = idle_instance("fipfs-bounded");
    let (release, stuck) = std::sync::mpsc::channel::<()>();
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let _job = instance.context().handle().spawn_blocking(move || {
        let _ = entered_tx.send(());
        stuck.recv().is_ok()
    });
    entered.recv().unwrap();
    let started = Instant::now();
    let quiescence = instance.shutdown(Duration::from_millis(300));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "bounded by its timeout"
    );
    assert!(!quiescence.reached && !quiescence.joined, "{quiescence:?}");
    assert!(store_is_open(&store), "nothing released");
    // The teardown finishes in the background once the job ends.
    release.send(()).unwrap();
    wait_for("the background teardown", Duration::from_secs(5), || {
        !store_is_open(&store)
    });
}

fn instance_thread_named(prefix: &str) -> bool {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path().join("comm")).ok())
        .any(|name| name.trim_end().starts_with(prefix))
}

#[test]
fn a_stuck_blocking_job_holds_quiescence_until_it_ends() {
    // A counted blocking job that doesn't return in time, as a statement
    // stuck in SQLite would: quiescence is unproven until it ends (the
    // service exits 70 past its bound).
    let (instance, store) = idle_instance("fipfs-stuck");
    let (release, stuck) = std::sync::mpsc::channel::<()>();
    let (entered_tx, entered) = std::sync::mpsc::channel::<()>();
    let _job = instance
        .context()
        .spawn_blocking(move || {
            let _ = entered_tx.send(());
            stuck.recv()
        })
        .unwrap();
    // Counted from admission; wait until it has really started (a queued
    // job would be skipped by the cancel).
    entered.recv().unwrap();
    assert_eq!(instance.context().blocking_regions(), 1);
    let context = instance.context().clone();
    let teardown = instance.begin_shutdown(Duration::from_millis(100));
    assert_eq!(teardown.wait_timeout(Duration::from_millis(500)), None);
    assert!(store_is_open(&store));
    release.send(()).unwrap();
    let quiescence = teardown.wait();
    assert!(quiescence.reached, "{quiescence:?}");
    assert!(!quiescence.drained, "it outlived the drain: {quiescence:?}");
    assert_eq!(context.blocking_regions(), 0);
}

#[test]
fn a_provider_call_from_a_plain_thread_stays_in_the_instance() {
    // The instance's block provider, called from a thread with no runtime,
    // while its delegated router never answers: the fetch runs on the
    // instance's runtime, in a counted region that the teardown cancels and
    // waits for, so it has ended before quiescence is reported.
    let router = Server::start(None);
    let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
    let instance = OnlineNode::builder(store.clone())
        .routing_mode(RoutingMode::Delegated)
        .delegated_routers([format!("http://127.0.0.1:{}/routing/v1", router.port)])
        .build_instance(InstanceConfig {
            thread_name: "fipfs-plain".into(),
            worker_threads: 1,
            max_blocking_threads: 4,
        })
        .unwrap();
    let provider = instance.node().block_provider();
    let cid = cid_from_data(CODEC_RAW, b"asked from a plain thread");
    let caller = std::thread::spawn(move || provider.get_block(&cid).map(|_| ()));
    wait_for("the router's connection", Duration::from_secs(10), || {
        router.accepted() > 0
    });
    assert_eq!(instance.context().blocking_regions(), 1, "counted");
    let quiescence = instance.shutdown(Duration::from_secs(5));
    assert!(quiescence.reached, "{quiescence:?}");
    assert!(caller.is_finished(), "the call ended before quiescence");
    assert!(caller.join().unwrap().is_err(), "as cancelled");
    assert!(!store_is_open(&store));
}
