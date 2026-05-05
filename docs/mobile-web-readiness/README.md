# Mobile Web Readiness Lab

This branch is a sidecar lab for black-box and regression testing of
`freedom-ipfs` as a read-only mobile web node. Keep `main` free for the
long-running upstream agent; use this branch to collect reproducible browser
compatibility bugs, local fixes, and scenarios that should survive rebases.

## Workflow

- Fetch `origin/main`, then merge it into this lab branch when refreshing.
- Record each concrete failure under `docs/mobile-web-readiness/`.
- Prefer a failing regression test before a fix when the bug is understood.
- Keep live-network scenarios opt-in; default tests should stay deterministic.

## Harness

The black-box harness lives in `tools/mobile-web-harness`.

Use an already-running gateway:

```sh
cargo run -p mobile-web-harness -- --gateway-url http://127.0.0.1:50017
```

Run one or more focused cases:

```sh
cargo run -p mobile-web-harness -- --case ipfs-tech-page-assets
```

Or let it spawn the standalone gateway:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- --output /tmp/mobile-web-run.json
```

Run a focused case repeatedly and write an aggregate JSON report:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 20 \
  --output /tmp/ipfs-tech-repeat.json
```

The repeat report includes measured pass/fail counts, pass rate, root and asset
TTFB/total-time p50/p90/p95/max summaries, failed asset kinds, and failed URLs
grouped by status/error. Use `--warmup-runs` to separate warm-cache behavior, or
`--fresh-gateway-per-run` when measuring repeated cold gateways. Spawning uses
the same routing, DHT, request-concurrency, and asset-concurrency knobs as the
single-run harness, with an 8-request gateway default and a 6-asset crawl
default to model bounded browser pressure.

For noisy live experiments, `--run-timeout-secs N` adds a wall-clock cap around
one full corpus run. If the cap fires, the harness records matching cases as
failed with `run timed out after Ns`, still writes the JSON report, and stops
the spawned gateway. This is separate from `--timeout-secs`, which remains the
per-request HTTP timeout.

For fresh-process warm-store measurements, pass `--gateway-db /tmp/cache.db`
while the harness is spawning the gateway. This forwards the path to the
gateway's SQLite cache. Combined with `--fresh-gateway-per-run --warmup-runs 1`,
the warmup process populates the DB and measured runs start new gateway
processes against the same persistent cache. On Linux, spawned-gateway RSS is
sampled from `/proc/<pid>/status` after each run and included in the JSON report,
along with FD count, direct child process count, and cache/repo storage bytes
when available. Measured-run summaries aggregate run time, RSS, FD count, child
process count, and storage bytes so resource regressions are visible without
manual per-run JSON parsing. Rust-vs-Kubo comparison output prints p50 and p95
root/asset TTFB ratios plus max RSS, FD, and storage ratios for quick terminal
triage.

For cache-completeness checks, pass `--offline-replay`. The harness starts a
Rust gateway online against a persistent SQLite DB, runs the selected corpus,
stops that gateway, restarts the same DB with `--routing-mode offline`, and
replays the same corpus. If `--gateway-db` is omitted, the harness creates a
temporary DB path and reports it. The JSON report contains separate `online` and
`offline` run reports plus an offline replay summary with missing root/asset
URLs and offline cache storage bytes. If `--trace-output /tmp/replay.jsonl` is
also set, the online and offline trace files are written as
`/tmp/replay-online.jsonl` and `/tmp/replay-offline.jsonl`.

For gateway phase tracing, pass `--trace-output /tmp/run.jsonl`. When the
harness spawns the Rust gateway it forwards this path to the gateway, parses the
JSONL events, and adds raw phase and mobile-style progress phase summaries to
the report. This is the preferred way to distinguish DNSLink/name resolution,
provider lookup, cache checks, Bitswap fetch, HTTP-provider fetch, retry,
UnixFS path traversal, MIME sniffing, and gateway limiter behavior during live
runs.
The spawned gateway uses a trace-friendly filter by default whenever
`--trace-output` is set, so an ambient `RUST_LOG=warn` will not hide phase
events. Use `--trace-filter` only when intentionally overriding the default.
The trace summary also includes bounded `slow_requests` and `slow_events` lists.
`slow_requests` groups each gateway request by path/request ID, status, phases,
CIDs, max event latency, and total elapsed time; `slow_events` preserves the
slowest individual events with useful fields such as CID, path, source, provider
count, peer count, trusted/session peer count, source peer, and bounded target
summaries. The console output prints both sections so optimization runs
immediately show which URL/CID/request caused the tail. Reports also include
`progress_phases`, `block_sources`, and `bitswap_source_peers` counts, which
help quantify user-visible loading states, cache/Bitswap/HTTP-provider mix, and
peer reuse during provider/session experiments. Gateway response statuses and
limiter denials are aggregated as well, making overload or `503` pressure
visible in the normal report. Trace errors are grouped by phase and sanitized
error string so provider-quality runs can show repeated DHT/Bitswap failure
signatures without manual JSONL greps.
Bitswap session shortcut summaries include both started shortcut races and
completed shortcut attempts, which makes hidden dropped background work visible
when tuning recent-peer races. `bitswap_session_shortcut_post_lookup_wait`
marks cases where a quick provider lookup was briefly held to let a recent
known-good peer finish first, and the session summary counts those waits as
`shortcut_post_lookup_waits`.
Bitswap peer expansion traces include address mix counters, and the report
aggregates them as `bitswap_addr_mix` for transport policy work. Established
Bitswap connections are also counted by transport as
`bitswap_connection_transports`, so provider experiments can see whether actual
connections are using TCP, QUIC, WS, or WSS. Immediate dial rejections are
counted by transport as `bitswap_dial_rejected_transports`, which helps spot
connection-limit pressure by transport. UnixFS metadata-cache traces are also
aggregated so reports show cache events, hits, misses, inserts, evictions, skip
counts, maximum length, and capacity without manual JSONL greps.
Bitswap DNS expansion traces are aggregated as well, including cached versus
uncached expansion events, failed DNSAddr lookups, TXT records, and resolved IPs.
The `slow_cids` list groups elapsed trace events by CID with phase and path
counts, which helps separate a slow root from a slow child block. Successful
Bitswap source peers are also summarized with count, total/max latency, and
bytes for session/provider-quality analysis.

The harness can also spawn Kubo as a comparison engine when a Kubo `ipfs`
binary is available:

```sh
cargo run -p mobile-web-harness -- \
  --engine kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --output /tmp/ipfs-tech-kubo.json
```

Omit `--kubo-repo` to create an isolated temporary repo per spawned Kubo daemon,
or pass `--kubo-repo /tmp/kubo-repo` to reuse a repo across fresh daemon runs.
Kubo is configured with loopback API/gateway/swarm listeners and lowpower
profile settings so comparisons stay local to the harness process except for
normal outbound IPFS retrieval.

For a paired Rust-vs-Kubo run with the same corpus and repeat settings:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --output /tmp/vitalik-rust-vs-kubo.json
```

The comparison report embeds both engine reports and adds per-case ratios for
root/asset TTFB plus RSS and storage size. Rust-only trace summaries still use
`--trace-output`; Kubo runs do not produce Rust gateway phase traces.

The default live corpus is `tools/mobile-web-harness/corpus/mobile-web.json`.
It captures browser-facing checks such as status, MIME type, byte ranges,
minimum body size, body snippets, TTFB, and total response time. The corpus now
covers ENS-derived immutable HTML snapshots, DNSLink/IPNS page crawls, a
DNSLink image byte-range case, and a small independent Wikipedia-on-IPFS
DNSLink root.

Entries can also enable a page crawl. A crawl fetches the root HTML, extracts
same-origin browser subresources from HTML and CSS, resolves root-relative paths
as the iOS `ipfs://` / `ipns://` scheme handler would, and checks each asset's
status, MIME type, byte count, and timing.

## Findings

- `RUST-WEB-001`: root UnixFS HTML could be served as `application/octet-stream`
  when the path lacked an extension. Fixed in this branch with a regression test.
- `RUST-WEB-002`: `ipfs.tech` page asset crawls intermittently lost JS chunks to
  gateway `502` / `504` responses. Mitigated by the shared Bitswap client and
  harness repeat reporting. A latency follow-up added phase tracing, bounded
  in-flight block fetch coalescing, successful Bitswap peer preference, a
  conservative recent-peer race for slow provider-cache misses, and
  connection-ready Bitswap stream opening; fresh `ipfs.tech` 5-run totals
  improved from roughly 35-38s to roughly 10.6-19.3s while preserving 5/5 pass
  rate. A later same-window comparison reproduced the 30s root 504 tail on the
  previous pushed commit, then a connection-ready stream fix passed 3/3 with root
  max 10.9s and no `no addresses for peer` Bitswap errors. A follow-up now
  retries an identical provider set once when every Bitswap connection wait
  times out; in one fresh `ipfs.tech` repeat=3 sample this converted two root
  connection-timeout failures into slow successes, yielding 3/3 pass rate with a
  16.9s root max. The current follow-up keeps `WANT_HAVE` as a 750ms probe and
  starts same-provider retry after a 5s connection-ready miss; fresh
  `ipfs.tech` root-only repeat=3 passed with a 3.6s root max, while
  `ipfs.tech-page-assets` repeat=3 passed with root p50 1.9s and max 8.5s.
  A longer repeat=5 also passed 5/5; assets stayed tight with p95 1.3s and max
  1.8s, while live root TTFB still varied from 4.0s to 10.4s. Interleaving
  Bitswap dials by address rank then removed that root tail in the next
  same-window sample: fresh `ipfs.tech-page-assets` repeat=5 passed 5/5 with
  root p50 1.4s, root max 1.8s, asset p95 1.2s, and asset max 1.5s. A later
  UnixFS follow-up added a bounded per-gateway decoded DAG-PB metadata cache so
  path traversal, MIME/range checks, and streaming can reuse verified directory
  and file metadata without extra network fanout. Gateway traces now include
  `unixfs_metadata_cache` hit/miss/insert/eviction counters. A Bitswap DNS
  follow-up now reuses `/dnsaddr` and DNS multiaddr expansion results within one
  provider candidate set; a same-window `vitalik-root-html-range` comparison
  dropped Rust root TTFB from `4907ms` to `3080ms` and root
  `bitswap_peer_expand` from `1397ms` to `940ms`.

## Next Scenario Targets

- ENS-to-CID controls: resolve `.eth` names outside the gateway, then test the
  resulting `/ipfs/<cid>/` path directly so ENS bugs stay separate from IPFS
  retrieval bugs.
- Range-heavy media fixtures: request first, middle, and suffix byte ranges from
  known audio/video CIDs and verify `206`, `Content-Range`, and bounded memory.
- Cold/warm timing pairs: run each case twice against the same gateway and record
  cache-hit speedups, provider lookup counts, and outlier latencies.
- Failure classification: distinguish name resolution failures, provider
  discovery failures, block retrieval failures, MIME bugs, and browser-origin
  incompatibilities in the report output.
