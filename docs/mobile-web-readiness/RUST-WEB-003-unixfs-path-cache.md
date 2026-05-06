# RUST-WEB-003: UnixFS Path Cache

Date: 2026-05-06

## Hypothesis

Browser page loads issue many sibling UnixFS paths under the same immutable root, such as
`/_nuxt/*.js` on `ipfs.tech`. The gateway was resolving each path by rereading and
redecoding the same parent directories for file size checks, MIME sniffing, streaming,
and ranges.

A small bounded cache keyed by immutable CIDs should reduce repeated local store reads
and DAG-PB decoding without increasing network fanout, Bitswap timeouts, or mobile
resource use.

## Implementation

- Added `UnixfsPathCache` in `freedom-ipfs-unixfs`.
- Added cache-aware UnixFS APIs:
  - `resolve_path_with_cache`
  - `file_size_with_cache`
  - `read_file_range_with_cache`
  - `list_directory_with_cache`
- Cached only immutable, successful metadata:
  - directory child links
  - small directory listings
  - node kind
  - file size
- Kept errors and misses uncached.
- Skipped very large directory listings and very long segment names so a single
  unusual path cannot dominate mobile memory.
- Wired `GatewayState` to a shared bounded cache with default capacity
  `DEFAULT_GATEWAY_UNIXFS_PATH_CACHE_ENTRIES = 1024`.
- Added `GatewayConfig::with_unixfs_path_cache_entries` so the cache can be
  tuned or disabled with capacity `0`.
- Added `UnixfsPathCache::stats()` and a gateway `unixfs_path_cache` trace phase
  with cumulative entries, hits, misses, inserts, and evictions.

## Deterministic Evidence

Command:

```sh
cargo test -p freedom-ipfs-unixfs
```

Result:

```text
9 passed; 0 failed
```

The new `cached_path_resolution_reuses_directory_links_for_sibling_paths`
test proves that resolving `_nuxt/alpha.js` and `_nuxt/beta.js` with the
same cache reads the root directory once, the `_nuxt` directory once, and
each file block once.

Command:

```sh
cargo test -p freedom-ipfs-gateway
```

Result:

```text
22 lib tests passed plus gateway binary/integration tests passed
```

The new `reuses_unixfs_path_cache_across_sibling_gateway_requests` test
proves the cache is shared across separate HTTP gateway requests.

## Live A/B

Baseline command from `origin/main`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/unixfs-path-cache-baseline-trace.jsonl \
  --output /tmp/unixfs-path-cache-baseline.json \
  --timeout-secs 180
```

Baseline result:

```text
passed=1 failed=0
root_ttfb=2569ms
asset_ttfb p50=215ms p90=801ms p95=906ms max=1026ms
asset_total p50=215ms p90=801ms p95=906ms max=1027ms
```

Experiment command:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/unixfs-path-cache-current-trace.jsonl \
  --output /tmp/unixfs-path-cache-current.json \
  --timeout-secs 180
```

Experiment result:

```text
passed=1 failed=0
root_ttfb=2476ms
asset_ttfb p50=182ms p90=1202ms p95=1369ms max=1423ms
asset_total p50=182ms p90=1202ms p95=1369ms max=1423ms
```

Stats trace confirmation command:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/unixfs-path-cache-stats-current-trace.jsonl \
  --output /tmp/unixfs-path-cache-stats-current.json \
  --timeout-secs 180
```

Result:

```text
passed=1 failed=0
trace phases=19
final unixfs_path_cache sample: entries=239 capacity=1024 hits=196 misses=40 inserts=239 evictions=0
```

## Decision

Keep.

The live result is too network-dominated to claim a cold-network tail win from
one sample. The deterministic tests do prove the intended local work reduction,
and the cache is bounded, opt-in through the gateway state, read-only, and stores
only immutable CID-derived metadata. This is a resource-efficiency improvement
that should compound with future session and provider optimizations.

## Follow-Up

- Compare on repeated same-gateway page loads and range-heavy media cases.
- Revisit capacity after larger page corpora report typical sibling asset counts.
