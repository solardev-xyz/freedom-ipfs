# Native Gateway Core

`freedom-ipfs-gateway` now exposes an in-process gateway entry point for
transport-independent request handling:

```rust
GatewayCore::handle(GatewayCoreRequest)
```

The existing Axum localhost gateway remains the production adapter. Its
`/ipfs/{path}` and `/ipns/{path}` handlers now forward to `GatewayCore`, so HTTP
and native callers share the same request limiter, retrieval context,
progress-correlation headers, UnixFS path handling, range handling, ETag/cache
policy, MIME detection, directory listing, and error-page behavior.

## Current Shape

```text
GatewayCore
  input: namespace, method, namespace-relative path, headers
  output: HTTP-shaped status, headers, axum Body stream

Adapters:
  Axum HTTP gateway  -> GatewayCore
  mobile-web-harness rust-native -> GatewayCore
  mobile-web-harness rust-native-ffi -> freedom-ipfs-mobile C ABI/event mux
```

The native harness adapter is deliberately HTTP-shaped internally because
browser resource loading still needs status codes, headers, and byte streams.
It does not bind a TCP port and does not make loopback HTTP requests.
It consumes response bodies incrementally and records stream metrics in the
harness report rather than proving success by collecting through an unbounded
whole-body adapter.

## Rust API

```rust
let core = GatewayCore::with_provider_and_name_resolver_config(
    provider,
    name_resolver,
    gateway_config,
);

let response = core
    .handle(GatewayCoreRequest::ipfs(
        "bafy.../index.html",
        Method::GET,
        headers,
    ))
    .await;
```

Use `GatewayCoreRequest::ipfs` for namespace-relative `/ipfs/...` paths and
`GatewayCoreRequest::ipns` for namespace-relative `/ipns/...` paths. Request
headers are ordinary HTTP headers; range, conditional revalidation, and
`X-Freedom-*` progress correlation headers are interpreted the same way as the
HTTP gateway.

`HEAD` responses returned by `GatewayCore` have an empty body. This matches
wire-visible HTTP gateway behavior instead of relying on Hyper to strip the
body later.

## Harness Usage

The mobile web harness supports four engines:

```text
rust-http      localhost HTTP gateway adapter
rust-native    direct GatewayCore adapter, no TCP listener
rust-native-ffi
               mobile FFI/event-mux adapter, no TCP listener
kubo           external Kubo gateway
```

`--engine rust` remains accepted as an alias for `rust-http`.

Example deterministic native run:

```sh
cargo run -p mobile-web-harness -- \
  --engine rust-native \
  --gateway-import-car /tmp/mobile-web-multiblock.car \
  --corpus /tmp/mobile-web-multiblock-corpus.json \
  --fresh-gateway-per-run
```

The same corpus can be run through `rust-http` for adapter parity:

```sh
cargo run -p mobile-web-harness -- \
  --engine rust-http \
  --build-gateway \
  --gateway-import-car /tmp/mobile-web-multiblock.car \
  --corpus /tmp/mobile-web-multiblock-corpus.json \
  --fresh-gateway-per-run
```

Native runs can also write gateway/retrieval trace JSONL directly from the
in-process Rust stack:

```sh
cargo run -p mobile-web-harness -- \
  --engine rust-native \
  --gateway-import-car /tmp/mobile-web-multiblock.car \
  --corpus /tmp/mobile-web-multiblock-corpus.json \
  --fresh-gateway-per-run \
  --trace-output /tmp/native-gateway-trace.jsonl
```

The normal harness report includes per-response stream metrics for roots,
assets, and revalidations:

- `stream.chunk_count`
- `stream.first_byte_ms`
- `stream.max_chunk_bytes`
- `stream.max_buffered_bytes`
- `stream.completed`
- `stream.cancelled`

Case summaries aggregate root/asset first-byte, chunk-count, and max-buffered
metrics so `rust-native` and `rust-http` can be compared without manually
inspecting individual rows.

`body_bytes` still reports the full body retained by the harness for corpus
validation, hashing, asset discovery, and previews. `stream.max_buffered_bytes`
tracks the largest per-read adapter buffer/chunk observed while consuming the
body incrementally; it is the signal that native mode is not waiting for a
whole-body `to_bytes` collection before observing body data.

The `rust-native-ffi` engine is the Linux-side simulator for the iOS native
transport. It creates a `FreedomIpfsNode`, imports optional CAR fixtures through
the mobile FFI, starts native gateway request handles, waits on
`freedom_ipfs_gateway_wait_next_event`, then reads ready handles through
`freedom_ipfs_gateway_request_read`. It does not call `GatewayCore` directly and
does not make loopback HTTP requests.

Example native FFI simulator run:

```sh
cargo run -p mobile-web-harness -- \
  --engine rust-native-ffi \
  --gateway-import-car /tmp/mobile-web-multiblock.car \
  --corpus /tmp/mobile-web-multiblock-corpus.json \
  --routing-mode offline \
  --native-dispatchers 1 \
  --native-read-buffer-bytes 65536
```

Useful stress knobs:

- `--native-dispatchers 1|4`: number of event dispatcher workers.
- `--native-read-buffer-bytes N`: caller-owned read buffer size.
- `--request-queue-timeout-ms N`: gateway admission wait before a saturated
  gateway returns `503 gateway_busy`.
- `--native-slow-consumer-ms N`: delay after each read to model slow Swift/WebKit consumption.
- `--native-cancel-after-first-byte`: cancel each request after the first body bytes.
- `--native-cancel-after-ms N`: cancel each request after a time limit.
- `--native-stop-node-mid-run-ms N`: stop the node while requests are active.
- `--native-max-active-requests N`: lab-only cap before starting FFI handles.
- `--ens-corpus docs/mobile-web-readiness/ens-live-corpus.txt`: resolve an
  opt-in live ENS name list outside the gateway and append the resulting `/ipfs`
  or `/ipns` targets to the run corpus.

`RunResult.native_ffi` records simulator counters such as started requests,
responses, completed bodies, cancellations, freed handles, active handles at
shutdown, events by flag, read calls, bytes read, max active handles, and the
largest response body retained by the harness for validation. It also embeds a
`mobile_layer` snapshot from `freedom-ipfs-mobile` with active-handle counts,
total started/completed/failed/cancelled/freed requests, native read bytes,
`total_gateway_busy_responses`, event mux enqueue/delivery/coalescing counts,
pending event queue depth, max event queue depth, stop generation, and last
sanitized native error metadata.
`stashed_event_handles_at_end` should be zero for normal successful runs; late
events for handles already completed/freed are counted as stale instead of
being retained in the app-side pre-registration stash.
Body-channel occupancy is not yet exported; use per-response stream metrics,
caller buffer size, and native FFI counters as the current boundedness signals.

## Current Boundaries

- The native harness path is Linux/Rust testable and does not require iOS or
  Xcode.
- The local HTTP gateway and `cargo run -p freedom-ipfs-gateway` remain intact.
- Native trace output is wired for `rust-native` through the in-process tracing
  subscriber. Kubo still does not produce Rust trace output.
- An experimental mobile FFI request ABI exists in `freedom-ipfs-mobile`. It is
  Linux-tested through Rust unit tests and through the `rust-native-ffi` harness
  engine. The iOS app has also proven the native path under a feature-flagged
  integration; Linux simulator coverage remains the first place to harden
  transport behavior.
- WebKit response URL/origin policy remains a Swift adapter responsibility.

## Experimental Mobile FFI API

The native request ABI is handle-based and polling/read-oriented. It does not
expose Rust async tasks, `Stream`, futures, `Bytes`, borrowed slices, or owned
Rust containers across the C boundary.

```text
uint64_t freedom_ipfs_gateway_request_start(
    FreedomIpfsNode *node,
    const char *request_json);

char *freedom_ipfs_gateway_request_response_json(
    FreedomIpfsNode *node,
    uint64_t request_handle);

FreedomIpfsGatewayReadResult freedom_ipfs_gateway_request_read(
    FreedomIpfsNode *node,
    uint64_t request_handle,
    uint8_t *buffer,
    size_t buffer_len);

bool freedom_ipfs_gateway_request_cancel(
    FreedomIpfsNode *node,
    uint64_t request_handle);

bool freedom_ipfs_gateway_request_free(
    FreedomIpfsNode *node,
    uint64_t request_handle);
```

`request_json` contains:

```json
{
  "method": "GET",
  "path": "/ipfs/bafy.../index.html",
  "headers": [
    { "name": "Range", "value": "bytes=0-1023" }
  ],
  "request_id": 42,
  "parent_request_id": 7,
  "top_level_path": "/ipfs/bafy..."
}
```

Accepted methods are `GET` and `HEAD`. Accepted paths are `/ipfs/...`,
`/ipns/...`, `ipfs://...`, `ipns://...`, or a bare CID. Query/fragment text is
stripped before handing the namespace-relative path to `GatewayCore`, matching
the path-only portion the HTTP adapter sees. Request correlation fields are
also inserted as the existing `X-Freedom-*` headers so progress events remain
associated with the caller's request IDs.

`freedom_ipfs_gateway_request_start` returns `0` when the request cannot be
parsed or queued. Otherwise it returns an opaque handle that must eventually be
passed to `freedom_ipfs_gateway_request_free`.

`freedom_ipfs_gateway_request_response_json` returns metadata for the handle:

```json
{
  "handle": 1,
  "state": "streaming",
  "method": "GET",
  "path": "/ipfs/bafy.../index.html",
  "namespace": "ipfs",
  "request_id": 42,
  "parent_request_id": 7,
  "top_level_path": "/ipfs/bafy...",
  "status": 200,
  "headers": [
    { "name": "content-type", "value": "text/html; charset=utf-8" },
    { "name": "content-length", "value": "1234" }
  ],
  "completed": false,
  "cancelled": false,
  "error": null
}
```

`state` is one of:

- `pending`: request is queued or response metadata is not ready yet
- `streaming`: response metadata exists and body reads may produce data
- `completed`: body stream reached end
- `cancelled`: caller cancelled the request
- `failed`: handle/request failed; see `error.code` and `error.message`

`completed` means Rust has finished producing the body into the internal
bounded request channel. It does not mean the Swift caller has consumed all
bytes. A WebKit adapter should call `WKURLSchemeTask.didFinish()` only after
`freedom_ipfs_gateway_request_read` returns
`FREEDOM_IPFS_GATEWAY_READ_END`.

The returned JSON string uses the existing `freedom_ipfs_string_free`
ownership rule.

`freedom_ipfs_gateway_request_response_json_wait` has the same return schema
and ownership rule, but blocks up to `timeout_ms` for response metadata,
failure, completion, cancellation, or handle invalidation. A timeout with no
metadata still returns `"state": "pending"`. `timeout_ms = 0` is the immediate
nonblocking check and matches `freedom_ipfs_gateway_request_response_json`.

`read_result` should distinguish:

- `FREEDOM_IPFS_GATEWAY_READ_PENDING`
- `FREEDOM_IPFS_GATEWAY_READ_BYTES`
- `FREEDOM_IPFS_GATEWAY_READ_END`
- `FREEDOM_IPFS_GATEWAY_READ_CANCELLED`
- `FREEDOM_IPFS_GATEWAY_READ_FAILED`
- `FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE`

When the status is `FREEDOM_IPFS_GATEWAY_READ_BYTES`, `bytes_read` tells the
caller how many bytes were copied into the caller-owned buffer. The Rust side
uses a bounded body channel plus per-handle remainder storage for chunks larger
than the caller's buffer. It does not return Rust-owned byte buffers across the
ABI.

`freedom_ipfs_gateway_request_read_wait` has the same caller-owned-buffer
contract and result statuses, but blocks up to `timeout_ms` for bytes, end,
failure, cancellation, or handle invalidation. If the timeout expires with no
data or terminal state, it returns `FREEDOM_IPFS_GATEWAY_READ_PENDING`.
`timeout_ms = 0` matches `freedom_ipfs_gateway_request_read`.

The wait functions are intended for background Swift tasks/threads, not the
MainActor. They use request-local wakeups so Swift can avoid a short-sleep
polling loop while still keeping cancellation and bounded memory behavior.

For high-subresource pages, the per-request wait API still implies one blocked
Swift worker per active resource. The event multiplexer provides a node-level
dispatcher API so one to four Swift workers can drive many handles:

```c
typedef struct FreedomIpfsGatewayEvent {
    uint32_t status;
    uint32_t events;
    uint64_t request_handle;
} FreedomIpfsGatewayEvent;

FreedomIpfsGatewayEvent freedom_ipfs_gateway_wait_next_event(
    FreedomIpfsNode *ptr,
    uint64_t timeout_ms);

char *freedom_ipfs_node_native_gateway_stats_json(FreedomIpfsNode *ptr);
```

`status` is one of:

- `FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK`
- `FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT`
- `FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE`
- `FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED`

When `status` is `OK`, `events` is a bitmask over:

- `FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY`
- `FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY`
- `FREEDOM_IPFS_GATEWAY_EVENT_END`
- `FREEDOM_IPFS_GATEWAY_EVENT_FAILED`
- `FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED`
- `FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED`

The event API does not transfer body bytes. It only says which handle is worth
servicing. Response metadata still comes from
`freedom_ipfs_gateway_request_response_json`, and body bytes still come from
`freedom_ipfs_gateway_request_read` or `read_wait` into caller-owned buffers.

Events are coalesced by request handle. If a handle already has unobserved
`BODY_READY`, Rust will not enqueue unlimited duplicate body events for that
handle. New readiness after the dispatcher services a handle may enqueue
another event. This bounds event growth to ready handles instead of ready
chunks.

`END` is readiness, not permission to finish WebKit immediately. A WebKit
adapter should still call `didFinish()` only after `read` or `read_wait` returns
`FREEDOM_IPFS_GATEWAY_READ_END`.

Recommended Swift shape:

```text
start many native gateway requests
run 1-4 dispatcher tasks
dispatcher waits on waitNextNativeGatewayEvent(timeoutMilliseconds:)
dispatcher routes the returned handle to the matching WebKit task
task fetches metadata and drains read/read_wait until PENDING or END
```

`freedom_ipfs_gateway_request_cancel` aborts the background request task and
makes later reads report `CANCELLED`. `freedom_ipfs_gateway_request_free`
removes the handle and also cancels any remaining work. Invalid handles return
safe errors rather than dereferencing freed state.

`cancel` wakes response/read waiters and makes them report `cancelled` JSON or
`FREEDOM_IPFS_GATEWAY_READ_CANCELLED`. `free` removes the handle for future
calls, cancels remaining work, and wakes in-flight waiters that already hold the
request; those in-flight waiters may safely observe `CANCELLED`.

`cancel` also produces a native gateway event with `CANCELLED`. `free` produces
`HANDLE_FREED` for event waiters and stale events for freed handles are safe:
per-handle response/read APIs return invalid-handle results after free. Gateway
stop or lifecycle core swaps wake node-level event waiters with
`GATEWAY_STOPPED`.

`freedom_ipfs_node_native_gateway_stats_json` returns a compact diagnostics
snapshot for TestFlight logging and Linux simulator reports:

```json
{
  "active_native_handles": 0,
  "total_started": 51,
  "total_completed": 51,
  "total_failed": 0,
  "total_cancelled": 0,
  "total_freed": 51,
  "bytes_read": 12345,
  "max_active_handles": 7,
  "events_enqueued": 118,
  "events_delivered": 118,
  "events_coalesced": 12,
  "max_event_queue_depth": 9,
  "pending_event_queue_depth": 0,
  "pending_event_handle_count": 0,
  "stop_generation": 0,
  "last_native_error_code": null,
  "last_native_error_message": null
}
```

This is observability only. It does not transfer ownership of requests or body
bytes, and it should not be used as the source of truth for a specific WebKit
task. Per-request state still comes from the response/read/event APIs.

`ffi/swift/FreedomIpfsReader.swift` exposes a thin wrapper:

- `startNativeGatewayRequest(json:)`
- `nativeGatewayResponseJSON(requestHandle:)`
- `nativeGatewayResponseJSON(requestHandle:timeoutMilliseconds:)`
- `readNativeGatewayRequest(_:into:)`
- `readNativeGatewayRequest(_:into:timeoutMilliseconds:)`
- `waitNextNativeGatewayEvent(timeoutMilliseconds:)`
- `nativeGatewayStatsJSON`
- `cancelNativeGatewayRequest(_:)`
- `freeNativeGatewayRequest(_:)`

The Swift wrapper is intentionally not a WebKit integration yet. A later iOS
adapter can map this API to
`WKURLSchemeTask.didReceive(response)`, repeated `didReceive(data)`, and
`didFinish()`/`didFailWithError()`.

## Validation Added

Gateway unit tests now cover direct core serving and HTTP-vs-core parity for:

- full file GET and MIME headers
- `HEAD`
- byte range `206`
- `If-None-Match` / `304`
- invalid range errors
- missing paths
- directory listings
- fake IPNS resolution

The harness test suite covers the new engine enum and existing report logic.
It also covers the incremental body collector and native drop-after-first-chunk
behavior so native mode cannot silently regress to whole-body buffering.
It now also covers the `rust-native-ffi` simulator with CAR-backed fixtures,
one-dispatcher and four-dispatcher browser-like loads, slow consumers, and
cancel-after-first-byte, cancellation-storm, missing-path error response, and
node-stop wakeup behavior.

`freedom-ipfs-mobile` tests cover the experimental FFI API:

- start request and read response metadata JSON
- read a full body through repeated small caller-buffer reads
- `HEAD` response metadata with no body bytes
- `Range` response with `206` and `Content-Range`
- cancel mid-stream
- free handles
- invalid handle behavior
- repeated start/cancel/free cycles without leaked handles
- wait API metadata and body reads without Swift-style polling
- short-timeout `PENDING` behavior
- `timeout_ms = 0` equivalence with the nonblocking calls
- cancel/free wakeup behavior for blocked waiters
- machine-readable gateway-busy responses and native busy counters
- event API idle timeout, metadata, body, end, failure, cancel, free, and
  gateway-stop readiness
- event coalescing and fairness across noisy and quiet handles
- event-driven `HEAD`, `Range`, and `If-None-Match` / `304`
- 50 concurrent native requests driven by one dispatcher and by four
  dispatchers without per-handle waiters
