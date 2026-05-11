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
- An experimental mobile FFI request ABI exists in `freedom-ipfs-mobile`. It is
  Linux-tested through Rust unit tests, but it is not wired into the iOS
  `WKURLSchemeHandler` yet.
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

`freedom_ipfs_gateway_request_cancel` aborts the background request task and
makes later reads report `CANCELLED`. `freedom_ipfs_gateway_request_free`
removes the handle and also cancels any remaining work. Invalid handles return
safe errors rather than dereferencing freed state.

`ffi/swift/FreedomIpfsReader.swift` exposes a thin wrapper:

- `startNativeGatewayRequest(json:)`
- `nativeGatewayResponseJSON(requestHandle:)`
- `readNativeGatewayRequest(_:into:)`
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

`freedom-ipfs-mobile` tests cover the experimental FFI API:

- start request and read response metadata JSON
- read a full body through repeated small caller-buffer reads
- `HEAD` response metadata with no body bytes
- `Range` response with `206` and `Content-Range`
- cancel mid-stream
- free handles
- invalid handle behavior
- repeated start/cancel/free cycles without leaked handles
