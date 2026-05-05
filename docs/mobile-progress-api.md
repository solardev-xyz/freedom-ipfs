# Mobile Progress API

The mobile library exposes a bounded JSON progress snapshot for Swift polling:

- C ABI: `freedom_ipfs_node_progress_snapshot_json(node)`
- Swift: `FreedomIpfsReader.progressSnapshotJSON`
- Clear history: `freedom_ipfs_node_clear_progress(node)` /
  `FreedomIpfsReader.clearProgress()`

The returned string is owned by Rust and must be released with
`freedom_ipfs_string_free` when called through the C ABI.

## Shape

```json
{
  "active": [
    {
      "id": 1,
      "kind": "gateway_request",
      "path": "/ipfs/bafy...",
      "namespace": "ipfs",
      "phase": "fetching_bitswap",
      "status": "active",
      "elapsed_ms": 812,
      "last_error_code": null,
      "last_error_message": null,
      "last_event_id": 18,
      "updated_ms": 1777956660888
    }
  ],
  "events": [
    {
      "event_id": 18,
      "target_id": 1,
      "request_id": 12,
      "parent_id": null,
      "kind": "gateway_request",
      "path": "/ipfs/bafy...",
      "top_level_path": "/ipfs/bafy...",
      "namespace": "ipfs",
      "phase": "fetching_bitswap",
      "raw_phase": "bitswap_fetch",
      "status": "active",
      "source": "incoming",
      "transport": "tcp",
      "bytes_loaded": 38394,
      "providers_found": null,
      "candidate_peers": 10,
      "elapsed_ms": 812,
      "last_error_code": null,
      "last_error_message": null,
      "timestamp_ms": 1777956660888
    }
  ]
}
```

`events` is capped to the most recent 512 events. `active` contains currently
active targets only; completed, failed, and cancelled targets remain visible in
recent `events`.

For gateway requests, Swift may pass optional correlation headers:

- `X-Freedom-Request-ID`: unsigned integer used as `target_id`
- `X-Freedom-Parent-Request-ID`: unsigned integer exposed as `parent_id`
- `X-Freedom-Top-Level-Path`: copied into `top_level_path`

When these headers are absent, the gateway's local request counter is used as
the target id.

## Current Event Sources

The first implementation records existing structured Rust phases from the local
gateway, UnixFS path handling, IPNS/name resolution, routing/provider lookup,
and retrieval/Bitswap paths. It also records explicit mobile preload
`started`, `completed`, `failed`, and `cancelled` events.

Important stable phases Swift can map immediately:

- `started`
- `checking_cache`
- `cache_hit`
- `providers_found`
- `provider_diversity_low`
- `dht_fallback_started`
- `fetching_bitswap`
- `fetching_http_provider`
- `retrying`
- `completed`
- `failed`
- `cancelled`

`raw_phase` preserves the lower-level Rust diagnostic phase for logs and future
debugging.

## Swift Polling

Swift can poll every 250-500ms while WebKit is loading:

```swift
let snapshot = reader.progressSnapshotJSON
```

The app should treat `phase` as the UI-facing state and keep `raw_phase`,
`last_error_code`, and `last_error_message` for diagnostics. A simple mapping is:

- `checking_cache` / `cache_hit`: "Checking local cache"
- `providers_found` / `provider_diversity_low`: "Finding providers"
- `dht_fallback_started`: "Searching the network"
- `fetching_bitswap`: "Fetching from IPFS peers"
- `fetching_http_provider`: "Fetching from HTTP provider"
- `retrying`: "Retrying slow provider"
- `completed`: loaded
- `failed`: failed
