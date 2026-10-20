//! Each node's progress goes to its own recorder (design 0007, upstream
//! item 4), through one process-wide subscriber, as a host installs it:
//! the routing `freedom_ipfs_gateway::progress` describes.

use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};

use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use freedom_ipfs_core::{cid_from_data, CODEC_RAW};
use freedom_ipfs_gateway::progress::{default_recorder, init_progress_tracing, ProgressRecorder};
use freedom_ipfs_gateway::{
    GatewayCore, GatewayCoreRequest, InstanceConfig, OnlineNode, RoutingMode,
};
use freedom_ipfs_store::SqliteBlockStore;
use serde_json::Value;

const TARGET: &str = "freedom_ipfs_test";

fn snapshot(recorder: &ProgressRecorder) -> Value {
    serde_json::from_str(&recorder.snapshot_json()).unwrap()
}

/// The ids of the active targets of `kind`.
fn active(recorder: &ProgressRecorder, kind: &str) -> BTreeSet<u64> {
    snapshot(recorder)["active"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|target| target["kind"] == kind)
        .map(|target| target["id"].as_u64().unwrap())
        .collect()
}

/// The target ids of the events of `kind`, with their raw phases.
fn events(recorder: &ProgressRecorder, kind: &str) -> BTreeSet<(u64, String)> {
    snapshot(recorder)["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == kind)
        .map(|event| {
            (
                event["target_id"].as_u64().unwrap(),
                event["raw_phase"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

fn marked_runtime(recorder: &ProgressRecorder) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all();
    recorder.mark_runtime_threads(&mut builder).build().unwrap()
}

fn preload_start(id: u64) {
    tracing::info!(target: TARGET, phase = "preload_start", preload_id = id);
}

/// Opens preload targets `base..base + 4 * rounds` from each place a
/// node's runtime runs work: a task, a task it spawns, a blocking job, and
/// a worker in `block_in_place`.
async fn open_targets_everywhere(base: u64, rounds: u64) {
    for round in 0..rounds {
        let id = base + 4 * round;
        preload_start(id);
        tokio::spawn(async move { preload_start(id + 1) })
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || preload_start(id + 2))
            .await
            .unwrap();
        tokio::task::block_in_place(|| preload_start(id + 3));
        tokio::task::yield_now().await;
    }
}

#[test]
fn two_marked_runtimes_record_apart_while_both_run() {
    init_progress_tracing();
    let recorders = [ProgressRecorder::new(), ProgressRecorder::new()];
    let bases = [10_000, 20_000];
    let barrier = Arc::new(Barrier::new(2));
    let runs: Vec<_> = recorders
        .iter()
        .zip(bases)
        .map(|(recorder, base)| {
            let runtime = marked_runtime(recorder);
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let work = runtime.spawn(open_targets_everywhere(base, 50));
                runtime.block_on(work).unwrap();
            })
        })
        .collect();
    for run in runs {
        run.join().unwrap();
    }
    for (recorder, base) in recorders.iter().zip(bases) {
        assert_eq!(
            active(recorder, "preload"),
            (base..base + 200).collect::<BTreeSet<_>>()
        );
    }
    let unclaimed = active(&default_recorder(), "preload");
    assert!(
        !unclaimed
            .iter()
            .any(|id| (10_000..10_200).contains(id) || (20_000..20_200).contains(id)),
        "{unclaimed:?}"
    );
}

#[test]
fn unmarked_threads_record_into_the_default_and_scopes_nest() {
    init_progress_tracing();
    let (outer, inner) = (ProgressRecorder::new(), ProgressRecorder::new());
    std::thread::scope(|threads| {
        threads.spawn(|| {
            preload_start(30_001);
            {
                let _outer = outer.enter();
                preload_start(30_002);
                {
                    let _inner = inner.enter();
                    preload_start(30_003);
                }
                preload_start(30_004);
            }
            preload_start(30_005);
        });
    });
    assert_eq!(active(&outer, "preload"), BTreeSet::from([30_002, 30_004]));
    assert_eq!(active(&inner, "preload"), BTreeSet::from([30_003]));
    let unclaimed = active(&default_recorder(), "preload");
    assert!(
        unclaimed.contains(&30_001) && unclaimed.contains(&30_005),
        "{unclaimed:?}"
    );
    assert!(
        !(30_002..=30_004).any(|id| unclaimed.contains(&id)),
        "{unclaimed:?}"
    );
}

#[test]
fn a_span_keeps_its_recorder_on_any_thread_and_hands_it_to_its_children() {
    init_progress_tracing();
    let recorder = ProgressRecorder::new();
    let span = {
        let _scope = recorder.enter();
        tracing::info_span!(target: TARGET, "gateway_request", request_id = 40_000u64)
    };
    std::thread::scope(|threads| {
        threads.spawn(|| {
            let _entered = span.enter();
            tracing::info!(target: TARGET, phase = "request_start");
            let child = tracing::info_span!(target: TARGET, "child");
            let _child = child.enter();
            preload_start(40_001);
        });
    });
    assert_eq!(
        active(&recorder, "gateway_request"),
        BTreeSet::from([40_000])
    );
    assert_eq!(active(&recorder, "preload"), BTreeSet::from([40_001]));
    assert!(!active(&default_recorder(), "gateway_request").contains(&40_000));
}

fn store_with(data: &[u8]) -> (SqliteBlockStore, String) {
    let store = SqliteBlockStore::in_memory(4 * 1024 * 1024).unwrap();
    let cid = cid_from_data(CODEC_RAW, data);
    store.put_block(&cid, data).unwrap();
    (store, cid.to_string())
}

fn request(cid: &str, progress_id: u64) -> GatewayCoreRequest {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-freedom-request-id",
        HeaderValue::from_str(&progress_id.to_string()).unwrap(),
    );
    GatewayCoreRequest::ipfs(cid, Method::GET, headers)
}

fn instance_config(thread_name: &str) -> InstanceConfig {
    InstanceConfig {
        thread_name: thread_name.into(),
        worker_threads: 2,
        max_blocking_threads: 8,
    }
}

#[test]
fn two_instances_record_their_own_requests() {
    init_progress_tracing();
    let given = ProgressRecorder::new();
    let (store, first_cid) = store_with(b"the first instance's block");
    let first = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .progress(given.clone())
        .build_instance(instance_config("fipfs-progress-1"))
        .unwrap();
    let (store, second_cid) = store_with(b"the second instance's block");
    let second = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .build_instance(instance_config("fipfs-progress-2"))
        .unwrap();
    // Each request runs, and reads its body, on its instance's runtime.
    let mut requests = Vec::new();
    for round in 0..20 {
        for (instance, cid, base) in [(&first, &first_cid, 50_000), (&second, &second_cid, 60_000)]
        {
            let core = instance.gateway_core();
            let request = request(cid, base + round);
            requests.push(
                instance
                    .spawn_request(instance.request_token(), async move {
                        let response = core.handle(request).await;
                        let status = response.status();
                        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                        (status, body.len())
                    })
                    .unwrap(),
            );
        }
    }
    let host = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for request in requests {
        let (status, len) = host.block_on(request).unwrap().unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(len > 0);
    }

    // The builder's recorder is the instance's.
    {
        let _scope = given.enter();
        preload_start(55_555);
    }
    assert!(active(first.progress(), "preload").contains(&55_555));
    assert!(!active(second.progress(), "preload").contains(&55_555));

    for (instance, own, other) in [
        (&first, 50_000..50_020, 60_000..60_020),
        (&second, 60_000..60_020, 50_000..50_020),
    ] {
        let recorded = events(instance.progress(), "gateway_request");
        let completed: BTreeSet<u64> = recorded
            .iter()
            .filter(|(_, raw_phase)| raw_phase == "request_done")
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(completed, own.clone().collect::<BTreeSet<_>>());
        assert!(
            recorded
                .iter()
                .all(|(id, _)| own.contains(id) && !other.contains(id)),
            "{recorded:?}"
        );
        assert!(active(instance.progress(), "gateway_request").is_empty());
    }
    let unclaimed = events(&default_recorder(), "gateway_request");
    assert!(
        !unclaimed
            .iter()
            .any(|(id, _)| (50_000..50_020).contains(id) || (60_000..60_020).contains(id)),
        "{unclaimed:?}"
    );
    assert!(first.shutdown(std::time::Duration::from_secs(5)).reached);
    assert!(second.shutdown(std::time::Duration::from_secs(5)).reached);
}

/// A host that polls a node's request on a thread of its own (neither
/// marked nor in a scope), and reads its streamed body on yet another, still
/// records into the node's recorder: the request runs in its scope, and its
/// span carries the recorder to the body.
#[test]
fn a_request_polled_on_host_threads_records_into_its_node() {
    init_progress_tracing();
    // Four 64 KiB chunks, so the body streams.
    let data = vec![3u8; 256 * 1024];
    let check = |core: GatewayCore, recorder: &ProgressRecorder, cid: &str, id: u64| {
        let host = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let response = host.block_on(core.handle(request(cid, id)));
        assert_eq!(response.status(), StatusCode::OK);
        let len = std::thread::spawn(move || {
            let reader = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            reader
                .block_on(to_bytes(response.into_body(), usize::MAX))
                .unwrap()
                .len()
        })
        .join()
        .unwrap();
        assert_eq!(len, 256 * 1024);
        let recorded = events(recorder, "gateway_request");
        for raw_phase in ["request_start", "request_done", "gateway_stream_done"] {
            assert!(
                recorded.contains(&(id, raw_phase.to_string())),
                "{raw_phase}: {recorded:?}"
            );
        }
        assert!(active(recorder, "gateway_request").is_empty());
        let unclaimed = events(&default_recorder(), "gateway_request");
        assert!(
            !unclaimed.iter().any(|(event_id, _)| *event_id == id),
            "{unclaimed:?}"
        );
    };

    // A node a host runs on its own threads, given a recorder.
    let recorder = ProgressRecorder::new();
    let (store, cid) = store_with(&data);
    let node = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .progress(recorder.clone())
        .build();
    assert!(node.progress().is_some());
    check(node.gateway_core(), &recorder, &cid, 70_000);

    // An instance's request, polled off its runtime.
    let (store, cid) = store_with(&data);
    let instance = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .build_instance(instance_config("fipfs-progress-3"))
        .unwrap();
    check(instance.gateway_core(), instance.progress(), &cid, 70_001);
    assert!(instance.shutdown(std::time::Duration::from_secs(5)).reached);
}
