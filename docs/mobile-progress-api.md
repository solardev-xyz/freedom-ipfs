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
  "generated_at_unix_ms": 1777956660888,
  "active_count": 1,
  "event_count": 18,
  "active": [
    {
      "id": 1,
      "kind": "gateway_request",
      "path": "/ipfs/bafy...",
      "namespace": "ipfs",
      "phase": "fetching_bitswap",
      "status": "active",
      "source": "bitswap",
      "transport": "tcp",
      "delivery": "incoming",
      "bytes_loaded": null,
      "bytes_total": 600000,
      "active_subrequests": 2,
      "elapsed_ms": 812,
      "blocks_loaded": 2,
      "retry_count": 1,
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
      "source": "bitswap",
      "transport": "tcp",
      "delivery": "incoming",
      "bytes_loaded": 38394,
      "bytes_total": 600000,
      "providers_found": null,
      "candidate_peers": 10,
      "blocks_loaded": 2,
      "retry_count": 1,
      "elapsed_ms": 812,
      "last_error_code": null,
      "last_error_message": null,
      "timestamp_ms": 1777956660888
    }
  ]
}
```

`events` is capped to the most recent 512 events. `event_count` is the current
bounded event array length, not a lifetime total. `active_count` mirrors the
number of currently active targets. `active` contains currently active targets
only; completed, failed, and cancelled targets remain visible in recent
`events`.

`bytes_loaded`, `bytes_total`, `blocks_loaded`, and `retry_count` are per-target
fields. `bytes_total` is filled when the gateway knows the UnixFS file length or
streamed response body length. `blocks_loaded` and `retry_count` accumulate
while the target is active, and the final completed/failed/cancelled event
carries the last values even though the target is removed from `active`.

`active_subrequests` is computed for active targets in each snapshot from
`parent_id` relationships. A top-level gateway request can use it to show that
subresources are still loading under the same page/navigation.

`source` is a stable high-level source such as `cache`, `bitswap`,
`http_provider`, `delegated_routing`, or `dht`. `transport` is the network
transport when known, such as `tcp` or `quic`. `delivery` preserves lower-level
Bitswap delivery details such as `incoming` or `outgoing`.

The gateway's small in-memory body cache is exposed through the stable cache
phases rather than as a UI-facing internal phase: cache hits report
`phase: "cache_hit"` with `source: "cache"`, misses report
`phase: "checking_cache"`, and cache insert bookkeeping reports
`phase: "streaming"`.

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

Lifecycle hooks that stop active preloads also emit `cancelled` preload events
before pruning the preload task. This includes `enterBackground`, low-memory
handling, network changes, and node teardown. Swift can treat those events as a
signal to remove stale preload indicators from the loading UI.

Important stable phases Swift can map immediately:

- `queued`
- `started`
- `resolving_name`
- `name_resolved`
- `checking_cache`
- `cache_hit`
- `provider_lookup`
- `providers_found`
- `provider_diversity_low`
- `dht_fallback_started`
- `fetching_bitswap`
- `fetching_http_provider`
- `streaming`
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

- `queued`: "Waiting for gateway capacity"
- `resolving_name`: "Resolving IPNS name"
- `name_resolved`: "Name resolved"
- `checking_cache` / `cache_hit`: "Checking local cache"
- `provider_lookup` / `providers_found` / `provider_diversity_low`: "Finding providers"
- `dht_fallback_started`: "Searching the network"
- `fetching_bitswap`: "Fetching from IPFS peers"
- `fetching_http_provider`: "Fetching from HTTP provider"
- `streaming`: "Receiving content"
- `retrying`: "Retrying slow provider" or "Retrying slow peer"
- `completed`: loaded
- `failed`: failed

Examples of `retrying` include request timeouts, temporarily skipped peers,
connection errors, and incoming Bitswap stream read timeouts. `raw_phase`
distinguishes these cases for logs.
