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

## Current Boundaries

- The native harness path is Linux/Rust testable and does not require iOS or
  Xcode.
- The local HTTP gateway and `cargo run -p freedom-ipfs-gateway` remain intact.
- Native trace output is not wired yet; `--trace-output` is still restricted to
  `rust-http`.
- No mobile FFI request ABI has been added yet. That should remain phase-gated
  until `GatewayCore` and native harness parity are reviewed.
- WebKit response URL/origin policy remains a Swift adapter responsibility.

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
