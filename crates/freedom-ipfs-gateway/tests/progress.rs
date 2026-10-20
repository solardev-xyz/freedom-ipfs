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

/// A name that never resolves, so an `/ipns` request waits for good.
struct NeverResolves;

#[async_trait::async_trait]
impl freedom_ipfs_namesys::NameResolver for NeverResolves {
    async fn resolve_name(&self, _name: &str) -> freedom_ipfs_namesys::Result<String> {
        std::future::pending().await
    }
}

/// Waits, at most 5 s, until `done` holds for `recorder`.
fn wait_until(recorder: &ProgressRecorder, done: impl Fn(&ProgressRecorder) -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !done(recorder) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

/// What the recorder holds of `id`'s events: (raw phase, phase, status).
fn phases_of(recorder: &ProgressRecorder, id: u64) -> Vec<(String, String, String)> {
    snapshot(recorder)["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == "gateway_request" && event["target_id"] == id)
        .map(|event| {
            let field = |name: &str| event[name].as_str().unwrap().to_string();
            (field("raw_phase"), field("phase"), field("status"))
        })
        .collect()
}

fn cancelled() -> (String, String, String) {
    (
        "request_cancelled".into(),
        "cancelled".into(),
        "cancelled".into(),
    )
}

/// A request cancelled before it answers never reaches `request_done`: its
/// entry still leaves `active`, as cancelled.
#[test]
fn a_request_cancelled_before_it_answers_leaves_active_as_cancelled() {
    init_progress_tracing();
    let recorder = ProgressRecorder::new();
    let (store, _) = store_with(b"unused");
    let core =
        GatewayCore::with_provider_and_name_resolver(Arc::new(store), Arc::new(NeverResolves))
            .with_progress(recorder.clone());
    let runtime = marked_runtime(&ProgressRecorder::new());
    let mut headers = HeaderMap::new();
    headers.insert("x-freedom-request-id", HeaderValue::from_static("80000"));
    let request = runtime.spawn(async move {
        core.handle(GatewayCoreRequest::ipns(
            "example.org",
            Method::GET,
            headers,
        ))
        .await
    });
    wait_until(&recorder, |recorder| {
        active(recorder, "gateway_request").contains(&80_000)
    });
    assert!(active(&recorder, "gateway_request").contains(&80_000));

    request.abort();
    wait_until(&recorder, |recorder| {
        !active(recorder, "gateway_request").contains(&80_000)
    });
    assert!(active(&recorder, "gateway_request").is_empty());
    let phases = phases_of(&recorder, 80_000);
    assert_eq!(phases.first().unwrap().0, "request_start", "{phases:?}");
    assert_eq!(phases.last().unwrap(), &cancelled(), "{phases:?}");
}

/// A streamed body dropped after its first chunk never reaches
/// `gateway_stream_done`: its entry still leaves `active`, as cancelled.
/// Read to its end, or a `HEAD`, it ends as completed, and nothing is
/// cancelled.
#[test]
fn a_streamed_body_dropped_midway_leaves_active_as_cancelled() {
    use futures::StreamExt;

    init_progress_tracing();
    let recorder = ProgressRecorder::new();
    // Four 64 KiB chunks.
    let (store, cid) = store_with(&vec![5u8; 256 * 1024]);
    let node = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .progress(recorder.clone())
        .build();
    let runtime = marked_runtime(&ProgressRecorder::new());
    let core = node.gateway_core();
    runtime.block_on(async {
        let response = core.handle(request(&cid, 80_001)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        assert_eq!(body.next().await.unwrap().unwrap().len(), 64 * 1024);
        assert!(active(&recorder, "gateway_request").contains(&80_001));
        drop(body);

        let response = core.handle(request(&cid, 80_002)).await;
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .len(),
            256 * 1024
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-freedom-request-id", HeaderValue::from_static("80003"));
        let response = core
            .handle(GatewayCoreRequest::ipfs(&cid, Method::HEAD, headers))
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty());
    });
    assert!(active(&recorder, "gateway_request").is_empty());
    let dropped = phases_of(&recorder, 80_001);
    assert!(
        dropped.contains(&("request_done".into(), "streaming".into(), "active".into())),
        "{dropped:?}"
    );
    assert_eq!(dropped.last().unwrap(), &cancelled(), "{dropped:?}");
    for id in [80_002, 80_003] {
        let phases = phases_of(&recorder, id);
        assert_eq!(phases.last().unwrap().2, "completed", "{id}: {phases:?}");
        assert!(!phases.contains(&cancelled()), "{id}: {phases:?}");
    }
}

/// The ids of the gateway requests `recorder` has any event of.
fn request_ids(recorder: &ProgressRecorder) -> BTreeSet<u64> {
    events(recorder, "gateway_request")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// A gateway with a recorder records its requests there even while another
/// node's span is current where they are polled: the request's root span
/// belongs to its gateway, not to its parent.
#[test]
fn a_gateway_request_records_into_its_node_under_another_nodes_span() {
    use tracing::Instrument;

    init_progress_tracing();
    // Two instances: B's request, polled on a host runtime inside A's span.
    let (store, _) = store_with(b"the first instance's block");
    let first = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .build_instance(instance_config("fipfs-owner-a"))
        .unwrap();
    let (store, cid) = store_with(b"the second instance's block");
    let second = OnlineNode::builder(store)
        .routing_mode(RoutingMode::Offline)
        .build_instance(instance_config("fipfs-owner-b"))
        .unwrap();
    let foreign = {
        let _scope = first.progress().enter();
        tracing::info_span!(target: TARGET, "host_operation")
    };
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let core = second.gateway_core();
    host.block_on(
        async {
            let response = core.handle(request(&cid, 920_000)).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(body.as_ref(), b"the second instance's block");
        }
        .instrument(foreign),
    );
    let recorded = events(second.progress(), "gateway_request");
    for raw_phase in ["request_start", "request_done"] {
        assert!(
            recorded.contains(&(920_000, raw_phase.to_string())),
            "{raw_phase}: {recorded:?}"
        );
    }
    assert!(request_ids(first.progress()).is_empty());
    assert!(first.shutdown(std::time::Duration::from_secs(5)).reached);
    assert!(second.shutdown(std::time::Duration::from_secs(5)).reached);

    // A gateway given a recorder, polled while a span of another's is
    // entered on the polling thread.
    let (foreign_recorder, own) = (ProgressRecorder::new(), ProgressRecorder::new());
    let foreign = {
        let _scope = foreign_recorder.enter();
        tracing::info_span!(target: TARGET, parent: None, "gateway_request", request_id = 10u64)
    };
    let (store, cid) = store_with(b"only this gateway has this");
    let core = GatewayCore::new(store).with_progress(own.clone());
    let response = foreign.in_scope(|| host.block_on(core.handle(request(&cid, 9_902))));
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    assert!(request_ids(&own).contains(&9_902));
    assert!(request_ids(&foreign_recorder).is_empty());
}

/// A request's body keeps its node's recorder when another node's runtime
/// reads it.
#[test]
fn a_carried_body_read_on_another_nodes_runtime_records_into_its_own_node() {
    init_progress_tracing();
    let (own, other) = (ProgressRecorder::new(), ProgressRecorder::new());
    // Four 64 KiB chunks, so the body streams.
    let (store, cid) = store_with(&vec![9u8; 256 * 1024]);
    let core = GatewayCore::new(store).with_progress(own.clone());
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = host.block_on(core.handle(request(&cid, 920_001)));
    assert_eq!(response.status(), StatusCode::OK);
    let reader = marked_runtime(&other);
    let len = reader
        .block_on(reader.spawn(async move {
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .len()
        }))
        .unwrap();
    assert_eq!(len, 256 * 1024);
    assert!(events(&own, "gateway_request").contains(&(920_001, "gateway_stream_done".into())));
    assert!(active(&own, "gateway_request").is_empty());
    assert!(request_ids(&other).is_empty());
}

/// A gateway with no recorder of its own keeps the plain inheritance: its
/// request records into the recorder of the span it is polled in.
#[test]
fn a_gateway_without_a_recorder_records_into_its_parent_spans() {
    init_progress_tracing();
    let parent_recorder = ProgressRecorder::new();
    let parent = {
        let _scope = parent_recorder.enter();
        tracing::info_span!(target: TARGET, "host_operation")
    };
    let (store, cid) = store_with(b"a gateway without a recorder");
    let core = GatewayCore::new(store);
    let host = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let response = parent.in_scope(|| host.block_on(core.handle(request(&cid, 920_002))));
    assert_eq!(response.status(), StatusCode::OK);
    drop(response);
    assert!(events(&parent_recorder, "gateway_request").contains(&(920_002, "request_done".into())));
}

fn root_span(recorder: &ProgressRecorder, request_id: u64, progress_id: u64) -> tracing::Span {
    let _scope = recorder.enter();
    tracing::info_span!(
        target: TARGET,
        parent: None,
        "gateway_request",
        request_id,
        progress_request_id = progress_id,
        path = "/ipfs/owned"
    )
}

fn start(span: &tracing::Span) {
    span.in_scope(|| tracing::info!(target: TARGET, phase = "request_start"));
}

/// An event with an explicit parent records into that span's recorder,
/// whatever scope its thread is in, and the parent's close still settles
/// the target it opened.
#[test]
fn an_event_records_into_its_explicit_parents_recorder() {
    init_progress_tracing();
    let (parents, threads) = (ProgressRecorder::new(), ProgressRecorder::new());
    let parent = root_span(&parents, 11, 11);
    {
        let _scope = threads.enter();
        tracing::info!(
            target: TARGET,
            parent: &parent,
            phase = "request_start",
            request_id = 11u64,
            progress_request_id = 11u64
        );
    }
    assert_eq!(snapshot(&parents)["event_count"], 1);
    assert_eq!(active(&parents, "gateway_request"), BTreeSet::from([11]));
    assert_eq!(snapshot(&threads)["event_count"], 0);
    drop(parent);
    assert!(active(&parents, "gateway_request").is_empty());
    assert!(events(&parents, "gateway_request").contains(&(11, "request_cancelled".into())));
    assert_eq!(snapshot(&threads)["event_count"], 0);
}

/// A root event (`parent: None`) has no span, whatever span is current: it
/// records into its thread's recorder.
#[test]
fn a_root_event_records_into_its_threads_recorder() {
    init_progress_tracing();
    let (spans, threads) = (ProgressRecorder::new(), ProgressRecorder::new());
    let current = root_span(&spans, 12, 12);
    {
        let _scope = threads.enter();
        let _entered = current.enter();
        tracing::info!(target: TARGET, parent: None, phase = "preload_start", preload_id = 930_000u64);
        tracing::info!(target: TARGET, phase = "preload_start", preload_id = 930_001u64);
    }
    assert_eq!(active(&threads, "preload"), BTreeSet::from([930_000]));
    assert_eq!(active(&spans, "preload"), BTreeSet::from([930_001]));
}

/// An old span still open when a newer request reuses its progress id
/// mustn't cancel the newer one when it closes: the newer span owns the
/// target now. With `clear()` between the two, likewise.
#[test]
fn codex_old_span_must_not_cancel_reused_id() {
    init_progress_tracing();
    for clear in [false, true] {
        let recorder = ProgressRecorder::new();
        let old = root_span(&recorder, 1, 700);
        start(&old);
        if clear {
            recorder.clear();
        }
        let new = root_span(&recorder, 2, 700);
        start(&new);
        drop(old);
        let after = snapshot(&recorder);
        assert_eq!(after["active_count"], 1, "clear={clear}: {after}");
        assert_eq!(
            after["active"][0]["request_id"], 2,
            "clear={clear}: {after}"
        );
        assert!(
            !after["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["raw_phase"] == "request_cancelled"),
            "clear={clear}: {after}"
        );
        // The newer span still owns it: its own close cancels it.
        drop(new);
        let after = snapshot(&recorder);
        assert_eq!(after["active_count"], 0, "clear={clear}: {after}");
        let last = after["events"].as_array().unwrap().last().unwrap().clone();
        assert_eq!(
            last["raw_phase"], "request_cancelled",
            "clear={clear}: {after}"
        );
        assert_eq!(last["request_id"], 2, "clear={clear}: {after}");
    }
}

/// `clear()` lets go of every owner: an old span closing after a clear and a
/// reuse cancels nothing.
#[test]
fn codex_clear_then_reuse_must_invalidate_old_close() {
    init_progress_tracing();
    let recorder = ProgressRecorder::new();
    let old = root_span(&recorder, 1, 701);
    start(&old);
    recorder.clear();
    let new = root_span(&recorder, 2, 701);
    start(&new);
    drop(old);
    let after = snapshot(&recorder);
    assert_eq!(after["active_count"], 1, "{after}");
    assert_eq!(after["event_count"], 1, "{after}");
    new.in_scope(|| {
        tracing::info!(target: TARGET, phase = "request_done", status = 200u16, body_mode = "direct")
    });
    drop(new);
    let after = snapshot(&recorder);
    assert_eq!(after["active_count"], 0, "{after}");
    assert_eq!(after["event_count"], 2, "{after}");
}
