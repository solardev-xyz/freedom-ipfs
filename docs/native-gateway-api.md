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

The mobile web harness supports three engines:

```text
rust-http      localhost HTTP gateway adapter
rust-native    direct GatewayCore adapter, no TCP listener
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

## Current Boundaries

- The native harness path is Linux/Rust testable and does not require iOS or
  Xcode.
- The local HTTP gateway and `cargo run -p freedom-ipfs-gateway` remain intact.
- Native trace output is wired for `rust-native` through the in-process tracing
  subscriber. Kubo still does not produce Rust trace output.
- No mobile FFI request ABI has been added yet. The shape below is the current
  design target, not an implemented ABI.
- WebKit response URL/origin policy remains a Swift adapter responsibility.

## Experimental Mobile FFI Design

The native request ABI should stay handle-based and polling/read-oriented. It
must not expose Rust async tasks, `Stream`, futures, `Bytes`, or owned Rust
containers across the C boundary.

Proposed shape:

```text
freedom_ipfs_gateway_request_start(node, request_json) -> request_handle
freedom_ipfs_gateway_request_response_json(node, request_handle) -> char*
freedom_ipfs_gateway_request_read(node, request_handle, buffer, buffer_len) -> read_result
freedom_ipfs_gateway_request_cancel(node, request_handle) -> status
freedom_ipfs_gateway_request_free(node, request_handle)
```

`request_json` should contain method, `/ipfs/...` or `/ipns/...` path, request
headers, optional request ID, optional parent request ID, and optional
top-level path. Response metadata JSON should contain status, headers,
correlation IDs, and a stable error code/message if metadata creation fails.

`read_result` should distinguish:

- pending/not-ready
- bytes read
- end of stream
- cancelled
- failed
- invalid handle

The Rust owner for each request handle must bound in-memory state, allow
prompt cancellation when Swift/WebKit stops loading, and make repeated
start/cancel/free cycles safe. The Swift wrapper can later map this to
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
