# Mobile Progress API

The mobile progress API lets the iOS browser explain a specific IPFS/IPNS load while the Rust node is resolving names, finding providers, reading cache, fetching blocks, and streaming bytes through the local gateway.

This is intentionally a small polling API. Swift can poll every 250-500 ms while a navigation is active without callbacks crossing the FFI boundary.

## Mobile FFI

```c
char *freedom_ipfs_node_progress_snapshot_json(FreedomIpfsNode *ptr);
void freedom_ipfs_node_progress_clear(FreedomIpfsNode *ptr);
```

`freedom_ipfs_node_progress_snapshot_json` returns a heap-allocated UTF-8 JSON string. Free it with `freedom_ipfs_string_free`.

`freedom_ipfs_node_progress_clear` drops the bounded in-memory event history. It does not cancel active retrievals or gateway requests.

The Swift wrapper exposes:

```swift
public var progressSnapshotJSON: String
public func clearProgress()
```

## JSON Shape

Example:

```json
{
  "generated_at_unix_ms": 1778051212000,
  "active_count": 1,
  "events": [
    {
      "id": 42,
      "parent_id": null,
      "kind": "gateway_request",
      "status": "active",
      "path": "/ipns/ipfs.tech/",
      "root": "ipfs.tech",
      "phase": "provider_lookup",
      "source": "delegated_routing",
      "message": "Finding providers",
      "bytes_loaded": 0,
      "bytes_total": null,
      "blocks_loaded": 0,
      "blocks_total": null,
      "providers_found": 0,
      "candidate_peers": 0,
      "active_subrequests": 0,
      "elapsed_ms": 830,
      "retry_count": 0,
      "last_error_code": null,
      "last_error_message": null
    }
  ]
}
```

Event history is bounded in memory. Completed, failed, and cancelled entries are retained briefly for UI polling and then pruned. Active entries are not pruned solely because the event cap is reached.

## Targets

Current event kinds:

- `gateway_request`: local HTTP gateway `/ipfs/...` or `/ipns/...` request.
- `block_fetch`: cache/network block retrieval by CID.
- `preload`: mobile preload request started through the FFI.

The gateway accepts optional request correlation headers:

- `X-Freedom-Request-ID`: stable unsigned integer request id.
- `X-Freedom-Parent-Request-ID`: parent request id, useful for grouping subresources under a page load.
- `X-Freedom-Top-Level-Path`: top-level navigation path shown as the event path.

If the headers are absent, the gateway still emits useful request-scoped events with an internal id and the requested path.

## Phases

Currently emitted phases include:

- `queued`
- `started`
- `resolving_name`
- `name_resolved`
- `checking_cache`
- `cache_hit`
- `cache_miss`
- `provider_lookup`
- `providers_found`
- `provider_diversity_low`
- `dht_fallback_started`
- `fetching_bitswap`
- `fetching_http_provider`
- `first_byte`
- `streaming`
- `retrying`
- `completed`
- `cancelled`
- `failed`

Not every path can emit every phase yet. The first implementation emits gateway start/done/fail, IPNS resolution start/done/fail, cache hit/miss, block fetch source, preload start/stream/done/cancel/fail, and gateway byte streaming. Provider diversity, retry, and per-peer counters are represented in the model and should be filled in as retrieval internals expose those moments.

## UI Mapping

The `message` field is a short user-safe default:

- `resolving_name`: `Resolving IPNS name`
- `provider_lookup`: `Finding providers`
- `fetching_bitswap`: `Trying Bitswap peers`
- `fetching_http_provider`: `Fetching from HTTP provider`
- `cache_hit`: `Loaded from cache`
- `first_byte` / `streaming`: `Receiving content`
- `retrying`: `Retrying slow provider`
- `failed`: `Load failed`

Swift can either show `message` directly or map `phase` to app-specific copy.

## Swift Polling Sketch

```swift
reader.clearProgress()

let timer = Timer.scheduledTimer(withTimeInterval: 0.3, repeats: true) { _ in
    let json = reader.progressSnapshotJSON
    // Decode the JSON and select active events for the current navigation id/path.
}

// Stop polling after WebKit finishes, fails, or the navigation is replaced.
timer.invalidate()
```

For a top-level navigation, the app should generate one request id and pass it through the local scheme/gateway request path when possible. Subresources should use their own id with `parent_id` set to the top-level id. If WebKit integration cannot attach headers for some paths, the default gateway events still include path, phase, status, bytes, and errors.

## Gateway Harness

The standalone gateway can expose the same JSON snapshot endpoint for local testing:

```bash
cargo run -p freedom-ipfs-gateway -- --online --progress
curl http://127.0.0.1:<port>/_freedom/progress
curl -X POST http://127.0.0.1:<port>/_freedom/progress
```

The endpoint is only present when the gateway is built with a progress tracker, which the CLI enables with `--progress`.

The mobile web harness can poll progress while it runs:

```bash
cargo run -p mobile-web-harness -- \
  --gateway-bin target/debug/freedom-ipfs-gateway \
  --case daicowtf-page-assets \
  --collect-progress \
  --output target/mobile-web-progress.json
```

When `--collect-progress` spawns the gateway, it passes `--progress`, clears progress before each case, sends the request correlation headers, polls every 300 ms, and writes a compact per-case progress summary into the report.

## Current Gaps

- Provider counts and candidate peer counts are still sparse for some retrieval paths.
- Bitswap peer retry/timeout events are not yet emitted at every internal retry point.
- Subresource grouping depends on the caller passing request headers; default path-based events remain useful without them.
- `bytes_total` is only known when the gateway or preload response has a content length or UnixFS size.
