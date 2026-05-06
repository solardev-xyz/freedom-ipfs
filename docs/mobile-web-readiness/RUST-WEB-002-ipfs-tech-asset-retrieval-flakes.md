# RUST-WEB-002: ipfs.tech page assets intermittently fail with gateway 502/504

## Status

Mitigated on `agent/mobile-web-reliability-and-latency`.

The gateway no longer creates a fresh Bitswap swarm for each missing block.
`HttpRetriever` now keeps one shared, bounded Bitswap swarm per retriever so page
loads can reuse provider connections across root HTML and asset block reads. The
shared client also routes incoming Bitswap streams to active block requests,
which preserved the behavior that the previous one-shot swarms relied on.

The harness now supports repeat/fresh-gateway measurement and groups failures by
status/error, making this class of regression visible without hand-counting
terminal output.

## Scenario

The mobile app needs IPFS/IPNS websites to load as full browser pages, not only
as single root documents. `ipfs.tech` is a useful live page because it resolves
through DNSLink/IPNS and then fans out into many same-origin Nuxt JS chunks.

The harness emulates the iOS scheme handler's path semantics: root-relative page
assets under `/ipns/ipfs.tech/` are fetched as `/ipns/ipfs.tech/<asset>`.

## Reproduction

```sh
cargo run -p mobile-web-harness -- --case ipfs-tech-page-assets
```

The broader corpus also reproduces it:

```sh
cargo run -p mobile-web-harness -- --output /tmp/mobile-web-run.json
```

## Observed

The focused case can pass, but repeated cold runs fail often enough to matter for
mobile browsing. On 2026-05-03, the full corpus failed with 2 of 32 assets:

```text
FAIL ipfs-tech-page-assets
  - crawl had 2 failed assets, allowed 0
  assets: discovered=32 fetched=32 passed=30 failed=2
    - /ipns/ipfs.tech/_nuxt/DIs1UAle.js -> 504 after 35433ms
    - /ipns/ipfs.tech/_nuxt/AKg0Znx-.js -> 504 after 10576ms
```

A focused run with `--asset-concurrency 2` still failed with 3 of 32 assets,
including one fast `502`, so this is not only an over-wide page fan-out problem:

```text
FAIL ipfs-tech-page-assets --asset-concurrency 2
  - /ipns/ipfs.tech/_nuxt/Duo5E1ke.js -> 504 after 45247ms
  - /ipns/ipfs.tech/_nuxt/CFmqYC7r.js -> 502 after 313ms
  - /ipns/ipfs.tech/_nuxt/AKg0Znx-.js -> 504 after 10455ms
```

Repeat-mode baseline on this branch before the retrieval fix:

```text
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 5 \
  --fresh-gateway-per-run \
  --output /tmp/ipfs-tech-cold-before.json

passed=0 failed=5 pass_rate=0.0%
root_ttfb p50=11747ms p90=12193ms p95=12193ms max=12193ms
asset_ttfb p50=5593ms p90=12510ms p95=30638ms max=42600ms
failed asset kinds: script=11, stylesheet=2
```

After keeping a shared Bitswap swarm per retriever and aligning the harness
defaults to 8 gateway requests and 6 asset fetches:

```text
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 5 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/ipfs-tech-cold-after-shared-asset6-5.json

passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=11461ms p90=13639ms p95=13639ms max=13639ms
asset_ttfb p50=5610ms p90=11383ms p95=11527ms max=14639ms
measured run totals: 37036ms, 34941ms, 34842ms, 37692ms, 35684ms
```

An extended 20-run cold check showed the remaining public-network tail:

```text
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 20 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/ipfs-tech-cold-after-shared-asset6-20.json

passed=19 failed=1 pass_rate=95.0%
root_ttfb p50=11781ms p90=13656ms p95=13917ms max=45822ms
asset_ttfb p50=5605ms p90=11245ms p95=11428ms max=40045ms
```

Warm-cache behavior against one reused gateway remained fast:

```text
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 5 \
  --output /tmp/ipfs-tech-warm-after-shared-default6.json

passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=25ms p90=40ms p95=40ms max=40ms
asset_ttfb p50=32ms p90=104ms p95=134ms max=236ms
measured run totals: 348ms, 330ms, 341ms, 354ms, 307ms
```

## Expected

All same-origin JS/CSS/image/font/media assets discovered from a reachable page
should return a 2xx response with a browser-appropriate MIME type. Cold loads can
be slower than warm loads, but a static site should not randomly lose JS chunks.

## Impact

For the iOS app this shows up as pages that render blank, partially styled, or
with broken client-side navigation. It is exactly the class of failure that makes
the Rust node feel "not yet Kubo-like" even when the root HTML request succeeds.

## Notes For Root Cause Work

- This was reproduced against the standalone Rust gateway, independent of iOS.
- Reducing harness asset concurrency from 4 to 2 did not eliminate failures.
- The failed URLs vary between runs, suggesting provider/retrieval reliability,
  timeout behavior, retry/fallback behavior, or cache/provider coalescing rather
  than one permanently bad path.
- The main confirmed issue was Bitswap churn: every cold missing block built a
  new libp2p swarm, redialed providers, and discarded any useful connections
  immediately after the block request. Full-page asset fan-out amplified this
  into flaky `504` responses.
- Marking every Bitswap peer as bad after one block timeout was also too
  aggressive for page workloads, because a timeout in one short-lived swarm does
  not prove that provider is bad for all nearby blocks.
- A later fix should reduce cold full-page time substantially. Reliability is
  better and asset fan-out is much less flaky, but ~35-40s cold `ipfs.tech`
  loads plus occasional root-provider timeouts are still not a mobile-quality
  target.

## 2026-05-03 Latency Follow-Up

Hypothesis:
Cold full-page time was dominated by per-block Bitswap behavior, not DNSLink,
provider lookup, SQLite, MIME detection, or gateway queueing.

Change:
Added opt-in gateway JSONL tracing and harness `--trace-output` collection, then
used the trace to make two retrieval changes:

- Bounded in-flight block fetch coalescing keyed by CID, with an 8s hedge so one
  stuck leader cannot hold every waiter until the 45s Bitswap timeout.
- Short-lived successful Bitswap peer preference. Once a peer serves a block,
  later block requests in the same process try that peer without a preliminary
  `WANT_HAVE`; unknown peers keep the conservative `WANT_HAVE` flow.
- Recent successful Bitswap peers now retain their dial addresses. On a provider
  cache miss, the retriever starts provider lookup immediately and only races a
  direct recent-peer `WANT_BLOCK` shortcut after a 150ms grace period. This keeps
  fast delegated lookups on the normal path while allowing known-good page
  session peers to win when provider lookup is slow or errors.
- Outgoing Bitswap requests now wait for a real libp2p connection-established
  event before opening a stream. This avoids the previous 500ms sleep where the
  stream behavior could issue a peer-ID dial without the explicit provider
  addresses and fail with `Dial error: no addresses for peer` while address dials
  were still in progress.

The harness also gained `--gateway-db` so fresh gateway processes can be
measured against the same persistent SQLite cache.

Commands:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 5 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-current-5-trace.jsonl \
  --output /tmp/ipfs-tech-current-5.json

cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-current-warm-trace.jsonl \
  --output /tmp/ipfs-tech-current-warm.json
```

Before:

```text
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=11461ms p90=13639ms p95=13639ms max=13639ms
asset_ttfb p50=5610ms p90=11383ms p95=11527ms max=14639ms
measured run totals: 37036ms, 34941ms, 34842ms, 37692ms, 35684ms
```

After:

```text
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=7265ms p90=7419ms p95=7419ms max=7419ms
asset_ttfb p50=633ms p90=2047ms p95=2396ms max=12042ms
measured run totals: 12466ms, 19318ms, 11104ms, 10648ms, 11372ms
```

Warm after one warmup remained fast:

```text
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=37ms p90=47ms p95=47ms max=47ms
asset_ttfb p50=39ms p90=106ms p95=144ms max=278ms
measured run totals: 342ms, 427ms, 407ms, 331ms, 266ms
```

Fresh process with persistent warm store:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-rss.db
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-rss.db \
  --trace-output /tmp/ipfs-tech-persistent-rss-3-trace.jsonl \
  --output /tmp/ipfs-tech-persistent-rss-3.json
```

```text
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=92ms p90=169ms p95=169ms max=169ms
asset_ttfb p50=33ms p90=139ms p95=165ms max=219ms
measured run totals: 460ms, 394ms, 393ms
measured gateway RSS: 28084 KiB, 28844 KiB, 28132 KiB
warmup gateway RSS: 52664 KiB
```

Trace evidence:

```text
before trace smoke:
bitswap_fetch count=26 total=143051ms p50=5602ms p95=6190ms max=6896ms
one shared directory/data CID fetched over Bitswap 6 times

after trace repeat:
bitswap_fetch count=105 total=134163ms p50=810ms p95=5605ms max=6608ms
hot shared CIDs are fetched once per fresh gateway run
```

Session-peer race check, compared with the previous pushed commit
`dfee107386c1189ee1d9b69b00637ac495108522` in the same live-network window:

```text
previous commit repeat=3:
passed=2 failed=1 pass_rate=66.7%
root_ttfb p50=9633ms p90=30759ms max=30759ms
asset_ttfb p50=627ms p90=1393ms p95=5795ms max=10662ms
bitswap_fetch count=43 total=96575ms p50=631ms p95=8672ms max=30515ms
provider_lookup count=72 total=1712ms p50=17ms p95=61ms max=89ms

recent-peer race repeat=3:
passed=2 failed=1 pass_rate=66.7%
root_ttfb p50=8811ms p90=30676ms max=30676ms
asset_ttfb p50=610ms p90=2101ms p95=3971ms max=4306ms
bitswap_session_shortcut count=2 total=1220ms p50=565ms max=655ms
bitswap_fetch count=41 total=70003ms p50=633ms p95=5558ms max=30519ms
provider_lookup count=70 total=5872ms p50=20ms p95=575ms max=740ms
```

Both samples hit the same remaining root 504 pattern: the first root block spent
about 30.5s in provider-derived Bitswap, then refreshed to effectively the same
provider set and returned 504 before any asset crawl. The raced shortcut did not
participate in that root failure; it only won two slow asset block misses in the
successful runs.

Connection-ready Bitswap stream check:

```text
connection-ready repeat=3:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=6972ms p90=10869ms max=10869ms
asset_ttfb p50=207ms p90=5274ms p95=5481ms max=6148ms
measured run totals: 21099ms, 14315ms, 12281ms
bitswap_fetch count=61 total=120425ms p50=431ms p95=5454ms max=5853ms
provider_lookup count=101 total=6192ms p50=17ms p95=417ms max=1122ms
bitswap_session_shortcut count=4 total=1217ms p50=213ms max=661ms
```

The trace for this run had no `no addresses for peer` Bitswap errors and no
failed `bitswap_fetch` events. This directly addresses the failure signature seen
in the same-window baseline/race runs above.

Connection-timeout same-provider retry:

After the connection-ready change, one fresh `ipfs.tech` repeat=3 sample still
hit a root `502` when all seven delegated Bitswap peers failed to establish a
connection within 10s. The refreshed provider lookup returned the same provider
set, so the node used to return the error immediately. The retrieval path now
retries that identical provider set once only for this connection-ready timeout
signature. In the validation run below, the retry fired twice and converted both
would-be root failures into slow successes:

```text
connection-timeout-retry repeat=3:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=15706ms p90=16930ms max=16930ms
asset_ttfb p50=177ms p90=631ms p95=5287ms max=5602ms
measured run totals: 9824ms, 18618ms, 22257ms
bitswap_fetch count=65 total=80785ms p50=156ms p95=6506ms max=10014ms
provider_lookup count=107 total=2513ms p50=20ms p95=48ms max=69ms
unixfs_index_lookup count=3 total=2065ms p50=253ms max=1622ms
```

This is intentionally a reliability tradeoff, not a latency win: the retry keeps
the gateway from returning a fast root `502`, but it can add another connection
window to a cold root request. The paired `daicowtf-page-assets` repeat=3 check
still passed 3/3 with root TTFB p50=5842ms and max=6071ms.

Short WANT_HAVE and connection-ready budgets:

Follow-up tracing showed that the useful `ipfs.tech` Bitswap peers usually
established libp2p connections in 80-300ms, while the old 5s `WANT_HAVE` probe
often expired before the node sent `WANT_BLOCK`. That injected about 5s per cold
root/index block even when a provider could serve the block immediately after a
direct request. The current code keeps `WANT_HAVE` as a conservative multi-peer
filter, but lowers the probe budget to 750ms. It also starts the existing
same-provider retry after a 5s connection-ready miss instead of waiting the full
10s ready window before refreshing providers.

Focused root-only validation:

```text
ipfs-tech-root-html-range repeat=3, fresh gateway:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=2611ms p90=3645ms max=3645ms
bitswap_fetch count=6 total=6579ms p50=967ms p90=2503ms max=2503ms
```

The same root-only trace before shortening `WANT_HAVE` had root TTFB p50=9866ms
and max=10952ms, with Bitswap fetch p50=5100ms.

Fresh full-page validation:

```text
ipfs-tech-page-assets repeat=3, fresh gateway, asset_concurrency=6:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=1856ms p90=8473ms max=8473ms
asset_ttfb p50=147ms p90=478ms p95=668ms max=10114ms
measured run totals: 12922ms, 2626ms, 9727ms
bitswap_fetch count=64 total=27456ms p50=96ms p90=804ms p95=1287ms max=10086ms
```

The 8.5s root tail was a same-provider retry after all initial connection-ready
waits hit the new 5s window. The 10.1s asset tail was a successful Bitswap block
response from a slow peer, not a gateway failure.

Longer fresh full-page sample in the same window:

```text
ipfs-tech-page-assets repeat=5, fresh gateway, asset_concurrency=6:
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=7993ms p90=10428ms max=10428ms
asset_ttfb p50=138ms p90=844ms p95=1306ms max=1776ms
measured run totals: 4378ms, 3991ms, 10005ms, 11547ms, 9794ms
bitswap_fetch count=105 total=46250ms p50=128ms p90=794ms p95=2593ms max=5014ms
```

This longer sample is the better reliability signal: the asset path remained
much faster than the earlier shared-swarm baseline, but live root startup still
has a public-provider tail that Kubo handles better.

Interleaved Bitswap dials:

The remaining root tail came from exhausting the connection-ready window across
all provider peers, then succeeding on a same-provider retry. The dial loop was
still attempting all addresses for one peer before moving to the next peer,
which could spend the bounded pending-dial budget on later addresses for stale
peers before every candidate got its best address attempted. The current code
adds every provider address to the swarm address book, then dials by address
rank across peers: first address for every peer, then second address for every
peer, and so on.

Focused root-only validation:

```text
ipfs-tech-root-html-range repeat=5, fresh gateway:
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=1510ms p90=2100ms max=2100ms
bitswap_fetch count=10 total=7112ms p50=575ms p90=1132ms max=1286ms
```

Fresh full-page validation:

```text
ipfs-tech-page-assets repeat=5, fresh gateway, asset_concurrency=6:
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=1415ms p90=1796ms max=1796ms
asset_ttfb p50=119ms p90=512ms p95=1186ms max=1510ms
measured run totals: 3648ms, 2560ms, 3075ms, 3261ms, 3185ms
bitswap_fetch count=104 total=29683ms p50=104ms p90=1033ms p95=1061ms max=1401ms
```

Secondary page validation:

```text
daicowtf-page-assets repeat=3, fresh gateway, asset_concurrency=6:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=1746ms p90=1891ms max=1891ms
bitswap_fetch count=9 total=4507ms p50=167ms p90=1346ms max=1346ms
```

Persistent warm-store validation after interleaved dials:

```text
ipfs-tech-page-assets warmup=1 repeat=3, fresh gateway per run, persistent DB:
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=83ms p90=88ms max=88ms
asset_ttfb p50=33ms p90=76ms p95=95ms max=145ms
measured run totals: 338ms, 375ms, 305ms
measured gateway RSS: 28064 KiB, 28324 KiB, 28700 KiB
SQLite cache DB: 2.3 MiB
```

Resource impact:
The changes keep existing caps: gateway request concurrency remains 8, asset
concurrency remains harness-side, Bitswap connection limits are unchanged, and
in-flight block coalescing is capped at 256 CIDs with hedged waiters. Interleaved
dials do not raise the connection limits or candidate caps. The fresh-process
persistent warm-store `ipfs.tech` run produced a 2.3 MiB SQLite cache DB for the
warmed page and measured fresh-process RSS of about 28-29 MiB after each
warm-cache run.

Failed experiments:

- Earlier standalone `WANT_HAVE` timeout reductions to 750ms and 2s made asset
  samples fast but caused repeated root failures when delegated providers were
  stale. That version predated the connection-ready stream fix and
  same-provider retry. The current implementation reintroduced a 750ms
  `WANT_HAVE` probe with those safeguards and kept the fresh validation above at
  3/3 pass rate.
- Reducing the connection-ready window from 5s to 2s made root-only
  `ipfs.tech` repeat=5 pass with a 4.3s max, but the full page regressed to 4/5:
  one run returned 17 asset `504`s after 45s Bitswap request timeouts. Reverted
  and replaced with interleaved provider dials.
- One optimistic direct `WANT_BLOCK` attempt for an all-unknown peer set also
  regressed reliability: `ipfs-tech-page-assets` fresh repeat=5 passed 4/5,
  with one root 504 at 30.8s and run totals 14.3-30.8s. Reverted.
- Reusing the resolved UnixFS file CID for MIME/range/stream reads avoided
  repeated path walks in a deterministic gateway test, but live `ipfs.tech`
  evidence was worse: two fresh repeat=3 runs passed 1/3 and 2/3 with root
  504s, while the pushed `ac77017` code passed 3/3 in the same window. Reverted.
- Failure-only light-DHT fallback after a stale delegated provider set added
  about 10s to failed roots and did not recover `ipfs.tech` during the test
  window. Reverted.
- An aggressive recent-peer shortcut before provider lookup was also tested. It
  produced many successful direct session fetches, but because it ran before the
  delegated lookup it added avoidable 2s stalls on misses. Replaced with the
  conservative 150ms-grace race described above.
- IPv4/TCP-first Bitswap address ordering was tested after traces showed many
  failed IPv6 dials. It did not move the `ipfs.tech` root tail in the live
  window: fresh repeat=3 still passed 2/3 with a ~30.9s root 504, and passing
  run totals were not better. Reverted.

Kubo comparison:

Kubo v0.41.0 lowpower/auto in a fresh temp repo, same harness:

```text
ipfs-tech-page-assets repeat=3:
passed=3 failed=0
run totals: 3087ms, 36ms, 32ms
root_ttfb p50=2ms p90=2449ms max=2449ms
asset_ttfb p50=3ms p90=103ms max=208ms

additional mobile-web corpus cases repeat=3:
passed=3 failed=0
run totals: 6509ms, 11ms, 12ms
vitalik-root-html-range root_ttfb p50=4ms p90=3781ms max=3781ms
ipfs-tech-developers-hero-range root_ttfb p50=3ms p90=2369ms max=2369ms
wikipedia-on-ipfs-root root_ttfb p50=4ms p90=357ms max=357ms
```

Conclusion:
Rust cold full-page latency is now materially better for this case, but Kubo is
still much faster on the first load and dramatically faster once its repo is
warm. Remaining evidence points to provider quality/session behavior and root
UnixFS path startup cost.

## 2026-05-04 Kubo Harness And Dial-Dedupe Follow-Up

The harness now has a Kubo engine and paired comparison mode, so Rust changes
can be checked against the same corpus/options instead of comparing separate
manual runs:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --output /tmp/ipfs-tech-rust-vs-kubo-dial-dedupe.json
```

After deduplicating shared Bitswap dials for peers that are already connected or
already have a pending connection waiter:

```text
Rust: passed=3 failed=0
Kubo: passed=3 failed=0
Rust root_ttfb p50=1888ms p95=2154ms
Kubo root_ttfb p50=4731ms p95=5398ms
Rust asset_ttfb p50=230ms p95=1893ms
Kubo asset_ttfb p50=242ms p95=493ms
Rust max RSS=53148 KiB
Kubo max RSS=307812 KiB
```

Focused Rust trace:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --trace-output /tmp/ipfs-tech-dial-dedupe-trace.jsonl \
  --output /tmp/ipfs-tech-dial-dedupe.json
```

```text
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=1712ms p95=1914ms max=1914ms
asset_ttfb p50=278ms p95=1857ms max=3715ms
bitswap_fetch count=105 total=36437ms p50=113ms p95=1294ms max=2226ms
```

The earlier same-window trace before pending-dial dedupe logged 3827
connection-limit `bitswap_dial_rejected` events. After dedupe the same focused
case logged 1115, while keeping 3/3 pass rate. This is a resource win: less
connection churn under page asset fan-out without raising mobile connection
limits.

## 2026-05-04 Lightweight Timeout Diagnostics

A later live trace showed that the heaviest missing evidence was whether a
15-second shared Bitswap request timeout meant "all peers stalled" or "the
command never got meaningful swarm time." The current diagnostics add:

- `command_queued_ms` on `bitswap_dial_plan`, measured from enqueue to swarm
  command processing.
- `bitswap_request_timeout_detail` when the shared Bitswap request timeout
  fires, including CID, peer count, trusted/session peer count, and a bounded
  target summary.

Validation command:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 5 \
  --output /tmp/ipfs-tech-light-timeout-detail.json \
  --trace-output /tmp/ipfs-tech-light-timeout-detail-trace.jsonl
```

Result:

```text
passed=5 failed=0 pass_rate=100.0%
root_ttfb p50=43ms p95=2119ms max=2119ms
asset_ttfb p50=52ms p95=932ms max=2533ms
bitswap_fetch count=29 total=11625ms p50=189ms p95=1266ms max=1330ms
bitswap_dial_plan count=40
bitswap_request_timeout_detail count=0
command_queued_ms p50=4ms p90=109ms max=146ms
bitswap_dial_rejected count=324, all connection-limit rejections
```

This run did not reproduce the 15-second shared request timeout, so the timeout
detail event remains a diagnostic hook for the next bad live sample rather than
evidence for a behavior change. The low queue times in the successful run argue
against command-queue starvation during normal `ipfs.tech` fan-out.

After making the target summary lazy so normal non-INFO runs avoid formatting
work, the exact-code follow-up remained clean:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --output /tmp/ipfs-tech-light-timeout-detail-final.json \
  --trace-output /tmp/ipfs-tech-light-timeout-detail-final-trace.jsonl
```

```text
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=43ms p95=1660ms max=1660ms
asset_ttfb p50=63ms p95=1440ms max=3639ms
bitswap_dial_plan count=35
bitswap_request_timeout_detail count=0
command_queued_ms p50=0ms p90=80ms max=134ms
```

The harness trace summary now also records bounded slow-event details. A smoke
run of that report shape:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --output /tmp/ipfs-tech-slow-events.json \
  --trace-output /tmp/ipfs-tech-slow-events-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
root_ttfb p50=2018ms max=2018ms
asset_ttfb p50=203ms p95=2617ms max=3601ms
slowest event: request_done 3598ms path=/ipns/ipfs.tech/_nuxt/DlAUqK2U.js request_id=28 status=200
slowest block: bafkreiglqwypey634jhik4zygaoamjyhmwjot4pruvtcskpynuwbezcevi
slowest block_fetch_total: 3595ms source=bitswap
```

This makes future live runs much easier to triage: the report points directly at
the slow URL/CID pair instead of requiring manual trace greps.

Secondary same-window checks:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/daicowtf-current-ab83a70.json \
  --trace-output /tmp/daicowtf-current-ab83a70-trace.jsonl
```

```text
branch ab83a70: passed=1 failed=2 pass_rate=33.3%
root_ttfb p50=10920ms p95=30938ms max=30938ms
slowest failed block: bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u
slow-event finding: one run had provider_lookup DHT timeout; one had provider_count=0; one one-peer Bitswap attempt timed out after 10009ms
```

The same command on canonical `origin/main` in the same network window failed
0/3 with the same root `504` shape:

```text
main 113d2d9: passed=0 failed=3 pass_rate=0.0%
root_ttfb p50=10949ms p95=30984ms max=30984ms
```

Conclusion: this `daicowtf` sample was not evidence of a regression from the
branch. It is a useful provider-quality failure sample for the next lab: the
slow CID had sparse/stale providers and light-DHT fallback did not recover it in
that window.

The exact branch also passed the `vitalik` range case while proving the new
timeout diagnostic:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-current-ab83a70.json \
  --trace-output /tmp/vitalik-current-ab83a70-trace.jsonl
```

```text
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=1782ms p95=16994ms max=16994ms
bitswap_request_timeout_detail count=1
bitswap_dial_plan count=7
command_queued_ms p50=0ms p90=0ms max=0ms
```

The `vitalik` timeout detail showed 16 candidate peers, zero trusted/session
peers, and 0ms command queue time. Follow-up instrumentation adds elapsed timing
to `bitswap_request_timeout_detail` and lets the harness slow-event summary keep
trusted peer count plus a bounded target summary. That makes the remaining tail
look like peer quality / Bitswap session behavior, not shared-swarm command
starvation.

The trace summary now also aggregates block source and successful Bitswap source
peer counts for provider/session experiments:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-trace-aggregation.json \
  --trace-output /tmp/vitalik-trace-aggregation-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
root_ttfb p50=1495ms max=1495ms
block sources: bitswap=2
bitswap source peers: 12D3KooWHoyPRFHDVesYiActeZtUmqQPaJHdSw2qGJTMi3YejoJ4=2
```

This gives later content-root session experiments an automatic signal for source
peer reuse instead of requiring manual trace parsing.

Trace summaries now also aggregate error signatures by phase. A noisy
`daicowtf` sample shows the intended provider-quality signal:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/daicowtf-trace-errors.json \
  --trace-output /tmp/daicowtf-trace-errors-trace.jsonl
```

```text
passed=0 failed=1 pass_rate=0.0%
root_ttfb p50=10920ms max=10920ms
block sources: bitswap=1
trace errors: bitswap_session_shortcut: ok=false=1, provider_diversity_low: ok=false=1
slow failed CID: bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u
```

This is still the known `daicowtf` sparse-provider failure shape. The useful
change is that future provider-quality runs now expose repeated error
signatures directly in the report.

Bitswap peer expansion now also reports provider address mix for transport
policy work:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-addr-mix-final.json \
  --trace-output /tmp/vitalik-addr-mix-final-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
root_ttfb p50=8808ms max=8808ms
block sources: bitswap=2
bitswap addr mix: ip4=68, tcp=58, quic=28, ip6=15, dns=3, ws=3, wss=0
```

The counts are summed from `bitswap_peer_expand` events, so they are intended
for comparing policies in the same harness window rather than as globally unique
provider counts.

The harness now also groups slow trace events by CID:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-slow-cids.json \
  --trace-output /tmp/vitalik-slow-cids-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
root_ttfb p50=14668ms max=14668ms
slow cids:
  bafkreibny3ionuayaittbxl2tn5dgfae7sbu45ymd35vhdm3634lmakxqi total=37938ms max=12649ms
  bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u total=35332ms max=14659ms
```

The totals are trace-phase sums, not wall-clock exclusive time. Their value is
ranking: they show which CIDs repeatedly sit under slow phases and which URL
paths referenced them.

Successful Bitswap source peers now get latency/byte aggregates too:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-peer-fetches.json \
  --trace-output /tmp/vitalik-peer-fetches-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
bitswap source peers: 12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH=2
bitswap peer fetches:
  12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH count=2 total=1526ms max=763ms bytes=38773
```

In that run, successful peer fetches were fast; the tail came from a separate
timeout path for the same child CID. That is exactly the split future session
experiments need to see.

The harness now records direct child process counts for spawned gateways on
Linux, which keeps Kubo comparisons honest about process shape as well as RSS,
FD count, and storage:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-child-process-count.json \
  --trace-output /tmp/vitalik-child-process-count-trace.jsonl
```

```text
passed=1 failed=0 pass_rate=100.0%
gateway_child_process_count=0
gateway_fd_count=34
gateway_rss_kib=39296
```

For the Rust gateway this should normally stay at zero. The field is most useful
when paired with `--compare-kubo`, where resource comparisons should not hide
extra worker processes behind a single parent PID.

## 2026-05-04 Bitswap Dial Headroom Experiment

Hypothesis: one Bitswap command was filling all 16 pending outgoing dial slots
with speculative public-provider addresses, leaving immediate child-block
requests unable to dial and forcing slow request-timeout retries.

Baseline same-window command:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-rust-kubo-child-process-0e28390.json \
  --trace-output /tmp/vitalik-rust-kubo-child-process-0e28390-trace.jsonl
```

Baseline result: Rust failed the cold range request with `504` after `32180ms`;
Kubo passed in `1939ms`. The Rust trace had `54` `bitswap_dial_rejected`
events from `PendingOutgoing` connection-limit denials before the child CID timed
out twice.

Experiment: cap scheduled Bitswap dial addresses per command at `8`, below the
swarm's global pending-outgoing limit of `16`. Suppressed peers are not registered
as connection waiters for that command. The dial plan now reports
`candidate_peer_count`, `candidate_dial_peer_count`, `new_dial_addr_count`,
`suppressed_dial_addr_count`, and `suppressed_dial_peer_count`.

Focused Rust validation:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/vitalik-dial-cap-targeted-rust-r3.json \
  --trace-output /tmp/vitalik-dial-cap-targeted-rust-r3-trace.jsonl
```

```text
passed=3 failed=0 pass_rate=100.0%
root_ttfb p50=1519ms max=1944ms
bitswap_dial_rejected=0
bitswap_request_timeout=0
```

Same-window Kubo comparison:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-rust-kubo-dial-cap-targeted.json \
  --trace-output /tmp/vitalik-rust-kubo-dial-cap-targeted-trace.jsonl
```

```text
rust passed=true ttfb=1135ms rss=37376KiB fds=29 children=0
kubo passed=true ttfb=2020ms rss=117420KiB fds=85 children=0
root_ttfb_ratio=0.56x
bitswap_dial_rejected=0
bitswap_request_timeout=0
```

Decision: keep. This is a mobile-resource-friendly reliability improvement and
the trace proves it removes pending-dial-limit churn. It does not eliminate all
remaining tail latency; future experiments should tune provider/session
selection and retry timing.

Additional coverage after the keep decision:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/ipfs-tech-dial-cap-targeted-r3.json \
  --trace-output /tmp/ipfs-tech-dial-cap-targeted-r3-trace.jsonl
```

Result: `3/3`, root TTFB p50 `1954ms`, asset TTFB p95 `1770ms`, asset max
`4164ms`. This high-concurrency page still produced `bitswap_dial_rejected`
events, so the cap fixes single-command pending-slot saturation but not aggregate
page-level dial pressure across many concurrent assets.

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/daicowtf-dial-cap-targeted-r3.json \
  --trace-output /tmp/daicowtf-dial-cap-targeted-r3-trace.jsonl
```

Result: `0/3`, same as canonical main in the same window:
`/tmp/daicowtf-main-same-window-r3.json` and
`/tmp/daicowtf-main-same-window-r3-trace.jsonl`. Both failed with root `504`
around `10950ms`, `provider_diversity_low`, and `bitswap_session_shortcut`
failures for child CID `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u`.
That points at the existing sparse-provider/session fallback gap rather than the
dial-headroom cap.

Follow-up diagnostic patch:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/daicowtf-provider-lookup-error-trace.json \
  --trace-output /tmp/daicowtf-provider-lookup-error-trace.jsonl
```

Result: still expected failure, but the trace now includes the missing child-CID
provider lookup error:

```text
trace errors: bitswap_session_shortcut: ok=false=1,
  provider_diversity_low: ok=false=1,
  provider_lookup: dht: the request timed out=1
slow event: provider_lookup cid=bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u
  elapsed=10020ms error="dht: the request timed out"
```

That makes the next experiment concrete: child-CID fallback needs either better
provider discovery diversity, a bounded retry policy after DHT timeout, or a
stronger content-root session model than "ask the root source peer once for 2s".

Rejected follow-up: increasing `BITSWAP_SESSION_SHORTCUT_TIMEOUT` from `2s` to
`5s`.

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/daicowtf-session-shortcut-5s.json \
  --trace-output /tmp/daicowtf-session-shortcut-5s-trace.jsonl
```

Result: worse. The request still failed, but TTFB stretched to `30973ms`. The
longer shortcut did not recover the child block and allowed the failure path to
stack a `5000ms` session shortcut, two empty/slow child provider lookups, and a
`10009ms` one-peer Bitswap failure. Decision: revert the timeout to `2s`.

Rejected follow-up: adding a swarm-level scheduled pending dial-address cap of
`12` on top of the per-command cap.

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/ipfs-tech-global-dial-cap-r3.json \
  --trace-output /tmp/ipfs-tech-global-dial-cap-r3-trace.jsonl
```

Result: worse. The cap reduced `bitswap_dial_rejected` to `0`, but it starved
asset fetches under page concurrency: pass rate dropped to `2/3`, one run took
`62778ms`, and trace showed `15` Bitswap request timeouts. The trace also showed
`47` dial plans with candidates but `new_dial_addr_count=0`. Decision: revert.
The next version needs fair queuing or session-aware peer selection, not a blunt
global dial-address cap.

Rejected follow-up: exact `(root CID, UnixFS path)` gateway resource metadata
cache.

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --output /tmp/ipfs-tech-resource-cache-r3.json \
  --trace-output /tmp/ipfs-tech-resource-cache-r3-trace.jsonl
```

Result: not useful for this workload. The run landed in a bad provider window
and failed `0/3`, but the important finding is diagnostic: the new
`unixfs_resource_cache` trace saw only three root-path misses and no useful hits.
The repeated work in `ipfs-tech` is not mostly exact duplicate path resolution;
it is repeated directory/root decoding across many different asset paths.
Decision: revert. A useful metadata cache needs to live at the decoded DAG-PB /
directory-entry level or in UnixFS path traversal, not only at the final gateway
resource result.

## 2026-05-04 Multi-Want Groundwork

Priority 1 in the long-running roadmap is bounded Bitswap multi-want batching.
The first safe step is deterministic support and tests, without enabling page
load batching yet.

Implementation:

- Single-block stream fetch now goes through a multi-CID stream helper with a
  one-CID slice, preserving current live behavior.
- Bitswap want/cancel message construction accepts multiple CIDs.
- Response collection can verify and return multiple requested blocks while
  still treating verified non-requested blocks as extras.

Focused validation:

```sh
cargo test -p freedom-ipfs-retrieval --lib
```

```text
test bitswap_tests::multi_want_message_preserves_requested_cids ... ok
test bitswap_tests::collects_multiple_requested_bitswap_payload_blocks ... ok
test bitswap_tests::multi_want_stream_collects_requested_blocks_and_cancels ... ok
```

Decision: keep as test-covered groundwork only. The live retriever still requests
one block at a time. The next experiment should wire this into a tiny bounded
window only for known-good session peers and measure `bitswap_fetch` count,
source-peer reuse, RSS, and asset p95 before keeping any behavior change.

## 2026-05-04 UnixFS Decoded Metadata Cache

Hypothesis:
The rejected exact `(root CID, UnixFS path)` gateway resource cache missed the
real repeated work. Browser page loads walk many different paths under the same
root and then re-check the same DAG-PB file metadata for file size, MIME/range,
and streaming. A small decoded DAG-PB metadata cache inside UnixFS traversal can
remove repeated block-store reads and protobuf decodes without changing
retrieval policy, trust boundaries, or public-network behavior.

Implementation:

- Added `UnixfsResolver`, a per-gateway resolver with a bounded decoded DAG-PB
  metadata cache.
- Default cache capacity is `256` decoded nodes; individual DAG-PB blocks larger
  than `64 KiB` are skipped to keep mobile memory bounded.
- Cache entries store only metadata decoded from already verified blocks; the
  block provider remains responsible for fetching/verifying blocks before they
  can enter this path.
- Existing stateless UnixFS free functions remain uncached for compatibility.
- Gateway state now owns one resolver and reuses it across resource detection,
  MIME sniffing, range responses, and streaming responses.
- Gateway tracing emits `unixfs_metadata_cache` with per-request cache hit/miss,
  insert, eviction, skip, capacity, and length counters.

Deterministic validation:

```sh
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-gateway
```

Key covered cases:

- sibling paths under the same directory read the root directory block once
- repeated ranges over the same DAG-PB file read/decode the file metadata once
- capacity `1` evicts older metadata and re-fetches it when needed
- a gateway DAG-PB directory/index response reads the directory and file
  metadata blocks once while still streaming the response body

Live baseline attempt before the patch:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-unixfs-metadata-cache-baseline-trace.jsonl \
  --output /tmp/ipfs-tech-unixfs-metadata-cache-baseline.json
```

Result: not useful as a cache comparator. The run landed in a bad provider
window and failed `0/3` before asset traversal:

```text
status 504 x3
root_ttfb p50=30251ms max=30471ms
bitswap_request_timeout_detail count=6
failed CID bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq
```

Live smoke after the patch:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-unixfs-metadata-cache-r3-trace.jsonl \
  --output /tmp/vitalik-unixfs-metadata-cache-r3.json
```

Result: `3/3`, root TTFB p50 `4286ms`, max `4416ms`, RSS about `37 MiB`,
FD count `28-29`, child processes `0`.

Final trace-shape check after adding `elapsed_ms=0` to the cache event:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-unixfs-metadata-cache-final-trace.jsonl \
  --output /tmp/vitalik-unixfs-metadata-cache-final.json
```

Result: `1/1`; trace summary includes `unixfs_metadata_cache`. The raw event
showed:

```text
cache_capacity=256 cache_len=1 hits=1 misses=1 inserts=1 evictions=0 oversized_skips=0
```

Follow-up: the harness trace summary now aggregates `unixfs_metadata_cache`
events directly, including total hits, misses, inserts, evictions, oversize
skips, maximum cache length, and capacity, so future UnixFS runs do not need
manual JSONL greps for cache effectiveness.

Post-summary live attempt:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-unixfs-cache-summary-trace.jsonl \
  --output /tmp/vitalik-unixfs-cache-summary.json
```

Result: failed `0/1` with a child block Bitswap timeout after the root block had
loaded: status `504`, TTFB `33314ms`, child CID
`bafkreibny3ionuayaittbxl2tn5dgfae7sbu45ymd35vhdm3634lmakxqi`, and
`bitswap_request_timeout_detail count=2`. This did not exercise the successful
cache-summary path because the gateway returned an error before building the
served resource response. Treat it as another provider/session-quality sample,
not as evidence against the metadata cache or harness aggregation.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Decision: keep. This is a bounded CPU/store-read optimization with deterministic
evidence and no additional network fanout, timeout budget, public gateway
fallback, or unverified-block cache path. Live timing remains provider dominated
for cold roots, so this is not expected to fix root 504s by itself. The next
metadata follow-up should measure full-page `ipfs.tech` in a cleaner provider
window and consider caching decoded directory entry vectors for HAMT/listing
heavy paths if traces show directory-entry rebuilding cost.

## 2026-05-04 Bitswap DNS Expansion Cache

Hypothesis:
Bitswap peer candidate construction repeats `/dnsaddr` TXT expansion and
ordinary DNS multiaddr IP expansion for the same host several times within one
provider set. On mobile cold loads this is wasted network/CPU time before the
node even starts dialing peers. A per-candidate-set cache should lower
`bitswap_peer_expand` latency without changing provider selection, dial caps,
timeouts, or trust boundaries.

Evidence before change:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-rust-kubo-post-cache-timeout.json \
  --trace-output /tmp/vitalik-rust-kubo-post-cache-timeout-trace.jsonl
```

Result: Rust and Kubo both passed, but Rust root TTFB was `4907ms` versus Kubo
`2037ms` (`2.41x`). Trace showed root `bitswap_peer_expand=1397ms`; repeated
events included `bitswap.filebase.io` DNS multiaddr expansion and
`bitswap.dget.top` DNSAddr expansion in the same request.

Implementation:

- `bitswap_peers()` now keeps two local caches while expanding one provider set:
  `/dnsaddr` host -> TXT-derived multiaddrs, and DNS host -> resolved IPs.
- WebSocket/WSS multiaddrs are still preserved as DNS names for SNI.
- The cache is per provider-set expansion only; it does not persist stale DNS
  answers across gateway requests.
- Trace events include `cached=true|false` on `bitswap_dnsaddr_expand` and
  `bitswap_dns_multiaddr_expand`.

Deterministic validation:

```sh
cargo test -p freedom-ipfs-retrieval --lib
```

Added coverage: `cached_dns_expansion_reuses_dnsaddr_and_ip_results`, using
pre-seeded caches so the test performs no live DNS.

Same-window check after change:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-rust-kubo-dns-expand-cache.json \
  --trace-output /tmp/vitalik-rust-kubo-dns-expand-cache-trace.jsonl
```

Result: Rust and Kubo both passed. Rust root TTFB improved to `3080ms`; Kubo was
`2030ms` (`1.52x`). Root `bitswap_peer_expand` dropped to `940ms`; child
`bitswap_peer_expand` was `79ms`. Trace showed cached expansion hits for
`bitswap.filebase.io` and `bitswap.dget.top`. Rust stayed resource-light:
`37248 KiB` RSS, `28` FDs, `0` child processes versus Kubo `180700 KiB`, `89`
FDs, `0` child processes.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p freedom-ipfs-gateway
```

Decision: keep. This is a bounded, low-risk latency/resource optimization:
fewer repeated DNS queries while preserving DNS names for WSS and all existing
verification-before-cache behavior. The next measurement improvement should
aggregate DNS expansion cache hit/miss counts in the harness trace summary, and
the next behavior experiment can revisit multi-want or session-aware child CID
fetching now that root peer expansion has less avoidable overhead.

Follow-up: the harness now aggregates Bitswap DNS expansion traces directly:
event count, cached versus uncached events, failed DNSAddr expansions, TXT
records, and resolved IPs. This makes future DNS/provider-expansion experiments
visible in the console report and JSON output without manual greps.

Harness summary smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-dns-summary-trace.jsonl \
  --output /tmp/vitalik-dns-summary.json
```

Result: `1/1` with a slow-but-successful child block retry path. The console
summary now printed:

```text
bitswap dns expansion: events=19 cached=11 uncached=8 failed=0 records=117 ips=25
```

The same run also showed the remaining child-CID session/provider tail:
`bitswap_request_timeout_detail count=1` for child CID
`bafkreibny3ionuayaittbxl2tn5dgfae7sbu45ymd35vhdm3634lmakxqi`, then recovery
from the same source peer. That reinforces that DNS expansion was avoidable
overhead, while the next larger behavior gap is still child-CID session
reliability.

## Bitswap Session/Trusted-Peer Diagnostics

Hypothesis: some slow follow-on UnixFS child blocks are not provider lookup
limited. They are session/reuse limited: the root block source is promoted to a
trusted want-block target for the child, but an existing shared Bitswap
connection can stall until the 15-second shared request timeout. A fresh-client
retry then often succeeds quickly, sometimes from the same peer.

The previous DNS-summary smoke provided the motivating sample:

- root CID `bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u`
  succeeded from peer
  `12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH`.
- child CID `bafkreibny3ionuayaittbxl2tn5dgfae7sbu45ymd35vhdm3634lmakxqi`
  then expanded `trusted_peer_count=1` with that same peer first in the target
  list as `want-block`.
- the first child request hit `bitswap_request_timeout_detail` after about
  `15002ms`.
- the same-provider retry reset the shared Bitswap client and then fetched the
  child from `12D3KooWGtYkBAaqJMJEmywMxaCiNP7LCEFUAFiLEBASe232c2VH` in
  `751ms`.

Implementation:

- `bitswap_fetch` now emits `provider_peer_count`, `session_peer_count`,
  `trusted_peer_count`, and `source_peer_trusted`.
- `bitswap_request_timeout` now includes `trusted_peer_count`.
- `bitswap_session_shortcut` now includes `trusted_peer_count` and marks
  successful shortcut sources as trusted.
- The mobile web harness now aggregates a `bitswap_session` trace summary:
  total fetches, fetches with trusted peers, trusted versus untrusted
  successes, trusted failures, request timeouts with trusted peers, session
  shortcut starts, and shortcut hits/misses.

Deterministic validation:

```sh
cargo test -p freedom-ipfs-retrieval --lib
cargo test -p mobile-web-harness
```

Live smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-session-summary-trace.jsonl \
  --output /tmp/vitalik-session-summary.json
```

Result: `1/1`. Root TTFB was `4241ms`; RSS/FD checks stayed within the harness
defaults. The trace summary printed:

```text
bitswap session: fetches=2 with_trusted=1 trusted_successes=0 untrusted_successes=2 trusted_failures=0 request_timeouts_with_trusted=0 shortcut_starts=0 shortcut_attempts=0 shortcut_hits=0 shortcut_misses=0
```

This smoke did not reproduce the 15-second trusted-peer stall. It did show that
one child fetch had a trusted candidate, but the winning source was a different
peer. Keep this diagnostic slice. It gives future iterations a direct counter
for the stale trusted-peer case before trying a behavior change such as a
shorter trusted-peer request retry budget or a fresh-session retry for child
CIDs.

Same-window Rust/Kubo baseline with the new diagnostics:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-session-baseline-kubo.json \
  --trace-output /tmp/vitalik-session-baseline-kubo-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50 was `2808ms`; Kubo
root TTFB p50 was `2807ms` (`1.00x`). Rust p95 was `3969ms`; Kubo p95 was
`3034ms` (`1.31x`). Rust stayed much smaller: RSS samples around
`37248-37376 KiB` and `25-27` FDs versus Kubo `115200-166736 KiB` and `51-81`
FDs.

The Rust trace summary showed `fetches=6`, `with_trusted=3`,
`trusted_successes=0`, `untrusted_successes=6`, `trusted_failures=0`, and
`request_timeouts_with_trusted=0`. In this network window the child blocks had
trusted candidates, but the trusted peer was not the winning source and did not
stall. Decision: do not add the shorter trusted-peer timeout behavior yet. Keep
collecting this counter across harder page/asset runs and only change behavior
when the trace shows repeatable `request_timeouts_with_trusted` or
`trusted_failures` under same-window comparison.

## Mixed Trusted-Peer Request Timeout

The harder `ipfs-tech-page-assets` case did reproduce the stale trusted-peer
pattern under same-window Rust/Kubo comparison.

Baseline diagnostic run:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-session-kubo-r1.json \
  --trace-output /tmp/ipfs-tech-session-kubo-r1-trace.jsonl
```

Result: Rust and Kubo both passed, but Rust root TTFB was `17081ms` while Kubo
was `1351ms` (`12.64x`). The slow root path fetched the root DAG-PB block, then
the `/index.html` child CID `bafkreibnzgajg3gsyn5c4p5e2h7racpy6dy7tnhwe5l4v4vx5e32qmn4bi`
had `trusted_peer_count=1`, hit the full `15000ms` Bitswap request timeout, and
then fetched from the trusted peer in `244ms` after the shared client reset.

Experiment:

- Add `BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT` as a narrow cap for mixed
  trusted/provider candidate sets. The initial live run used `5s`; follow-up
  tuning below keeps `4s`.
- Use it only when a Bitswap command has at least one trusted peer and at least
  one non-trusted provider candidate.
- Keep the existing `15s` cap for cold requests and trusted-only requests.
- Emit `timeout_ms` on `bitswap_request_timeout` and
  `bitswap_request_timeout_detail` so traces prove which cap fired.

This keeps the change narrow: it does not raise timeout budgets, add public
gateway fallback, increase fanout, or bypass block verification.

Deterministic validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib shortens_request_timeout_for_mixed_trusted_bitswap_candidates
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo build -p freedom-ipfs-gateway
```

Same-window check after change:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-trusted-timeout-kubo-r1.json \
  --trace-output /tmp/ipfs-tech-trusted-timeout-kubo-r1-trace.jsonl
```

Result: Rust and Kubo both passed. Rust root TTFB was `2971ms`; Kubo was
`3185ms` (`0.93x`). One mixed trusted request still timed out, but at
`timeout_ms=5000`, then recovered on retry.

Repeat check:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-trusted-timeout-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-trusted-timeout-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50 was `1890ms`; Kubo
root TTFB p50 was `2237ms` (`0.84x`). Rust p95 was `1904ms`; Kubo p95 was
`2971ms` (`0.64x`). Rust RSS stayed around `51956-53104 KiB` with `34-56` FDs;
Kubo RSS was `122052-228044 KiB` with `49-218` FDs.

Asset fetches are still slower than Kubo: Rust asset TTFB p50 was `449ms`
versus Kubo `153ms`, and Rust asset p95 was `2882ms` versus Kubo `312ms`.
That leaves a separate asset/session optimization track.

Regression checks:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-trusted-timeout-kubo-r3.json \
  --trace-output /tmp/vitalik-trusted-timeout-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50 was `3128ms`; Kubo
was `2833ms` (`1.10x`). There were no `request_timeouts_with_trusted`; Rust RSS
was `37760-38144 KiB` versus Kubo `106560-116964 KiB`.

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/daicowtf-trusted-timeout-kubo-r1.json \
  --trace-output /tmp/daicowtf-trusted-timeout-kubo-r1-trace.jsonl
```

Result: both Rust and Kubo failed with `504`s in this network window. Rust root
TTFB was `10934ms`; Kubo was `30004ms`. Rust failed on a follow-on child
provider lookup DHT timeout, not on a mixed trusted Bitswap timeout
(`request_timeouts_with_trusted=0`). This is not evidence against the mixed
trusted timeout experiment.

Follow-up tuning:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-trusted-timeout-3s-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-trusted-timeout-3s-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`, but `3s` was too aggressive. Rust root
TTFB p50 was `2837ms` and p95 was `7591ms`, compared with Kubo p50 `3681ms` and
p95 `6017ms`. The trace summary showed `request_timeouts_with_trusted=3` and
`session_shortcut_hits=3`, meaning the cap was firing on too many root/session
requests. Decision: reject `3s`.

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-trusted-timeout-4s-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-trusted-timeout-4s-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50 was `1943ms`; Kubo
was `1750ms` (`1.11x`). Rust p95 was `2173ms`; Kubo p95 was `1774ms`
(`1.22x`). Rust asset TTFB p50 was `315ms` versus Kubo `122ms`, and Rust asset
p95 was `2089ms` versus Kubo `1249ms`. Rust RSS stayed at `52604-53320 KiB`
with `33-50` FDs; Kubo reached `181952 KiB` and `178` FDs. The trace had one
mixed trusted timeout at `timeout_ms=4000` and avoided the `3s` root p95
regression while improving the asset tail relative to `5s`.

Regression check for the `4s` tuning:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/vitalik-trusted-timeout-4s-kubo-r3.json \
  --trace-output /tmp/vitalik-trusted-timeout-4s-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50 was `1461ms`; Kubo
was `2951ms` (`0.50x`). Rust p95 was `1549ms`; Kubo p95 was `3009ms`
(`0.51x`). The trace summary showed `request_timeouts_with_trusted=0`. Rust RSS
stayed at `37632-37888 KiB`; Kubo reached `164472 KiB`.

Decision: keep at `4s`. The experiment removes a repeatable 15-second
page-load cliff for mixed trusted/provider candidate sets while preserving
cold-request timeout behavior and mobile resource bounds. The `4s` cap improves
the `ipfs.tech` asset tail compared with `5s` and avoids the root p95 regression
seen at `3s`. Continue measuring asset/session behavior next.

## Rejected: Cross-Request Bitswap DNS Expansion Cache

Hypothesis: the `4s` `ipfs.tech` trace still showed slow `bitswap_peer_expand`
outliers. In `/tmp/ipfs-tech-trusted-timeout-4s-kubo-r3-trace.jsonl`,
`bootstrap.libp2p.io` was expanded `45` times with `42` uncached DNS lookups,
because DNS expansion caches were local to one provider set. A bounded
per-retriever DNS expansion cache might reduce sibling-asset latency without
changing provider records or block verification.

Implementation tried:

- cache `/dnsaddr` TXT expansions and DNS-to-IP expansions on the retriever
- cap entries at `64` DNSADDR hosts and `128` DNS/IP hosts
- TTL `5m`
- first variant cached empty/failed expansions; second variant cached only
  successful non-empty expansions

Validation before live runs:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib cached_dns_expansion_reuses_dnsaddr_and_ip_results
cargo build -p freedom-ipfs-gateway
```

Live run, first variant:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-dns-expansion-cache-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-dns-expansion-cache-kubo-r3-trace.jsonl
```

The run stopped making progress and was interrupted without producing
comparison JSON. Partial trace: `970` events, request statuses `23x200`,
`7x206`, `9x503`, `1x504`, and `9` gateway-limiter denials. DNS expansion did
get faster: `bitswap_peer_expand` p95 was `44ms`, max `72ms`, versus the
previous `4s` run's p95 `164ms`, max `3188ms`. Reliability regressed, likely
because page-wide negative DNS caching is too risky.

Live run, positive-only variant:

```sh
timeout 240s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-dns-expansion-cache-positive-kubo-r1.json \
  --trace-output /tmp/ipfs-tech-dns-expansion-cache-positive-kubo-r1-trace.jsonl
```

Result: outer timeout exited `124`; no comparison JSON. Partial trace had
`449` events, request statuses `11x200`, `1x206`, and `13x503`, with `13`
gateway-limiter denials. DNS expansion remained faster (`bitswap_peer_expand`
p95 `128ms`, max `170ms`), but the live page run still did not complete
cleanly.

Decision: revert. The measurement confirmed DNS expansion can be a real
latency component, but the cache experiment did not preserve page-load
reliability. Future work should revisit this only with request coalescing or
stricter per-host success semantics, and must pass a full same-window
Rust/Kubo page run before keeping any DNS expansion cache.

## Harness Run Timeout Guard

The rejected DNS-cache experiment exposed a harness sharp edge: live page runs
could sit for multiple request-timeout waves before producing JSON. That makes
failed experiments harder to compare and can leave the useful trace evidence
separate from the structured report.

Harness change:

- add `--run-timeout-secs N` as an optional wall-clock cap around one full corpus
  run
- synthesize failed `CaseResult`s for matched cases when the cap fires
- keep writing JSON reports before returning the normal failure exit status
- switch asset fetch scheduling from detached `JoinHandle`s to `JoinSet`, so
  dropping a timed-out crawl aborts in-flight asset tasks instead of leaving
  them running in the background

Validation:

```sh
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 1 \
  --output /tmp/harness-run-timeout-smoke.json \
  --trace-output /tmp/harness-run-timeout-smoke-trace.jsonl
```

Result: exited with the expected harness failure status after writing
`/tmp/harness-run-timeout-smoke.json`. The report recorded one failed
`ipfs-tech-page-assets` case with `run timed out after 1s`, and the trace
summary was still present. This is a harness/diagnostics improvement only; it
does not change gateway behavior.

## Rejected Session Shortcut 25ms Grace

Hypothesis: lowering `BITSWAP_SESSION_SHORTCUT_GRACE` from `150ms` to `25ms`
would let recent Bitswap session peers win more asset fetches before provider
lookup completes, improving warm page asset latency without increasing mobile
resource pressure.

First, keep the behavior at `150ms` but add an explicit
`bitswap_session_shortcut_start` trace before the shortcut request is sent. The
existing `bitswap_session_shortcut` event only records completed shortcut
futures. If provider lookup wins the race after the shortcut request starts,
the future can be dropped without a completed hit/miss event, hiding background
work. The mobile web harness now aggregates this as
`bitswap_session.session_shortcut_starts`.

Diagnostic validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib recent_bitswap
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Then run the live A/B:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-session-grace-25ms-start-trace-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-grace-25ms-start-trace-kubo-r3-trace.jsonl
```

Result at `25ms`: Rust and Kubo both passed `3/3`, with no limiter denials and
statuses `81x200`, `18x206`. Rust root latency regressed badly
(`p50=3168ms`, `p95=max=5270ms`) versus Kubo (`p50=1350ms`, `p95=max=1632ms`).
Rust assets were still slower than Kubo (`p50=223ms`, `p95=1813ms`,
`max=2271ms` versus Kubo `p50=83ms`, `p95=240ms`, `max=342ms`). The trace
showed `session_shortcut_starts=33`, but only `session_shortcut_attempts=11`;
all completed attempts were hits. That confirms the lower grace can start
background shortcut work that does not produce a completed shortcut event.

Same-diagnostics `150ms` control:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-session-grace-150ms-start-trace-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-grace-150ms-start-trace-kubo-r3-trace.jsonl
```

Result at `150ms`: Rust and Kubo both passed `3/3`, with no limiter denials and
the same `81x200`, `18x206` response mix. Rust root latency was much closer to
Kubo (`p50=1427ms`, `p95=max=3650ms` versus Kubo `p50=1366ms`,
`p95=max=2680ms`). Asset latency remained slower (`p50=264ms`, `p95=1648ms`,
`max=2689ms` versus Kubo `p50=115ms`, `p95=424ms`, `max=567ms`), but the
session summary showed `session_shortcut_starts=0`, `attempts=0`, and no hidden
shortcut pressure.

Decision: reject the `25ms` grace. The completed shortcut hits are real, but
they did not improve the page-level result in the same diagnostic window, and
the new start counter shows extra background work. Keep `150ms` until there is
a bounded design that can make recent-peer starts selective, cancellable, or
fairly scheduled against provider lookup.

## Harness Resource Summary

The 25ms shortcut-grace analysis again required checking RSS, file descriptors,
child processes, and cache/repo storage alongside latency. Those fields already
existed per measured run, but the top-level summary and comparison JSON did not
aggregate them. That made resource checks more manual than the roadmap's
Priority 0 benchmark loop wants.

Harness change:

- add measured-run `run_total_ms` summary to `summary`
- add measured-run `gateway_rss_kib`, `gateway_fd_count`,
  `gateway_child_process_count`, and `gateway_storage_bytes` summaries
- include FD max/ratio in Rust-vs-Kubo `cases` comparison JSON
- print a compact resource summary in normal harness output
- print p50/p95 root and asset ratios plus max RSS, FD, and storage ratios in
  comparison output

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 60 \
  --output /tmp/harness-resource-summary-smoke.json \
  --trace-output /tmp/harness-resource-summary-smoke-trace.jsonl
```

Result: `1/1`, root TTFB `518ms`, RSS `37760KiB`, FD count `27`, and child
process count `0`. The console printed the new `resources:` line and the JSON
summary included `run_total_ms`, `gateway_rss_kib`, `gateway_fd_count`,
`gateway_child_process_count`, and `gateway_storage_bytes`.

Comparison-output smoke after adding p95/resource ratios:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 60 \
  --comparison-output /tmp/vitalik-comparison-output-smoke.json \
  --trace-output /tmp/vitalik-comparison-output-smoke-trace.jsonl
```

Result: Rust and Kubo both passed `1/1`. The terminal summary printed
`root_ttfb` p50/p95 ratios and resource ratios directly: Rust root p50/p95
`2441ms`, Kubo root p50/p95 `3420ms`, RSS ratio `0.33x`, and FD ratio `0.37x`.

This is a harness/diagnostics improvement only. It does not change gateway or
retrieval behavior.

## Daicowtf Same-Window Resource Baseline

After adding resource summaries, rerun `daicowtf-page-assets` against Kubo to
collect a current resource-aware comparison:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/daicowtf-resource-summary-kubo-r3.json \
  --trace-output /tmp/daicowtf-resource-summary-kubo-r3-trace.jsonl
```

Result: not an optimization signal. Rust failed `3/3`, and Kubo also failed
`3/3` in the same window. Rust returned root `504` responses with root
`p50=11044ms`, `p95=max=30938ms`; Kubo returned `504` responses around the
30-second request cap with root `p50=30005ms`, `p95=max=30006ms`. Assets were
not crawled for either engine because the root response never passed.

Resource summary from the same report:

- Rust: max RSS `46464KiB`, max FD count `20`, child processes `0`
- Kubo: max RSS `153536KiB`, max FD count `202`, child processes `0`, max repo
  bytes `33700`

Rust trace shape: statuses `3x504`, limiter denials `0`,
`provider_diversity_low` failures `3`, DHT provider lookup timeouts `3`, and
`bitswap_session_shortcut` misses `3`. Treat this as another sparse-provider /
content-availability window for `daicowtf`, not evidence that the current Rust
branch regressed relative to Kubo.

## Bitswap Connection Transport Trace

Provider and session experiments need to know not only which transports appear
in candidate multiaddrs, but which transports actually connect. The gateway now
emits `bitswap_connection_established` at info level with `remote_addr` and a
bounded `transport` label (`tcp`, `quic`, `ws`, `wss`, or `other`). The mobile
web harness aggregates this as `bitswap_connection_transports`. Immediate
`swarm.dial` rejections also include the attempted transport and are aggregated
as `bitswap_dial_rejected_transports`, making connection-limit pressure easier
to distinguish from remote transport failures.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib labels_bitswap_connection_transport_from_multiaddr
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Live smoke:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 60 \
  --output /tmp/vitalik-connection-transport-smoke-2.json \
  --trace-output /tmp/vitalik-connection-transport-smoke-2-trace.jsonl
```

Result: `1/1`, root TTFB `3975ms`, RSS `38144KiB`, FD count `28`, and the
trace summary printed `bitswap connection transports: tcp=10`.

First page-load baseline with the new transport counter:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-connection-transport-r3.json \
  --trace-output /tmp/ipfs-tech-connection-transport-r3-trace.jsonl
```

Result: `3/3`, root TTFB p50/p95/max `1390/2453/2453ms`, asset TTFB
p50/p95/max `214/1210/2157ms`, max RSS `52912KiB`, max FD count `53`, response
statuses `81x200`, `18x206`, limiter denials `0`. Candidate address mix
included `tcp=2363`, `quic=895`, `ws=252`, and `wss=0`, but established
connections were `tcp=35`. This is evidence that current successful
`ipfs.tech` Bitswap retrieval is effectively TCP-only in this window. The trace
was collected before rejected-dial transport aggregation, so rerun it when
testing transport policy changes. Future transport experiments should measure
whether QUIC/WSS can improve tails without raising dial pressure.

Rejected-dial transport smoke:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-dial-transport-smoke.json \
  --trace-output /tmp/ipfs-tech-dial-transport-smoke-trace.jsonl
```

Result: `1/1`, but a slow root TTFB of `11993ms`. The trace showed established
connection transports `tcp=13`, `quic=1`, `ws=1`, and rejected dial transports
`tcp=95`, `quic=34`, `ws=8`. This confirms the new rejected-dial aggregation is
visible in live output and gives future transport experiments a pressure signal
to compare against.

This is diagnostic only. It does not change peer selection, connection limits,
Bitswap request behavior, or block verification.

## Rejected Direct QUIC-First Address Scoring

Hypothesis: since direct QUIC addresses are available for many `ipfs.tech`
providers, ranking direct QUIC before direct TCP in `bitswap_addr_score` might
avoid slow TCP dials and reduce page-load tails.

Baseline, current TCP-first scoring:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-transport-tcp-first-r3.json \
  --trace-output /tmp/ipfs-tech-transport-tcp-first-r3-trace.jsonl
```

Result: `3/3`. Root TTFB p50/p95/max `1529/1685/1685ms`; asset TTFB
p50/p95/max `268/1603/4264ms`; max RSS `53372KiB`; max FD count `50`;
established transports `tcp=36`; rejected dial transports `tcp=275`,
`quic=88`, `ws=22`. One mixed trusted Bitswap request hit the 4s cap.

Experiment: temporarily rank direct QUIC before direct TCP and rebuild
`freedom-ipfs-gateway`.

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-transport-quic-first-r3.json \
  --trace-output /tmp/ipfs-tech-transport-quic-first-r3-trace.jsonl
```

Result: `3/3`. Root TTFB p50/p95/max worsened to `1932/2732/2732ms`; asset
TTFB p50 improved to `215ms`, but p95 worsened to `1779ms`; asset max improved
to `1919ms`. Max RSS rose to `54436KiB`, max FD count fell to `33`,
established transports became `quic=18`, `tcp=18`, and rejected dial transports
shifted to `quic=270`, `tcp=107`, `ws=16`.

Same-window TCP-first recheck:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-transport-tcp-first-recheck-r3.json \
  --trace-output /tmp/ipfs-tech-transport-tcp-first-recheck-r3-trace.jsonl
```

Result: `3/3`. Root TTFB p50/p95/max `2124/2590/2590ms`; asset TTFB
p50/p95/max `170/1037/1411ms`; max RSS `54456KiB`; max FD count `52`;
established transports `tcp=40`; rejected dial transports `tcp=265`,
`quic=75`, `ws=17`.

Decision: reject broad QUIC-first scoring and leave TCP-first behavior. The
experiment proved QUIC can establish and sometimes lowers per-block Bitswap
latency, but broad QUIC-first increased QUIC rejected-dial pressure and did not
beat the same-window TCP recheck on asset p50, asset p95, asset max, or run
total. Future transport work should be selective, for example by preferring
QUIC only for peers with recent QUIC success or by adding per-peer transport
quality, not by globally ranking QUIC ahead of TCP.

## Bitswap Source Transport Attribution

After rejecting broad QUIC-first scoring, the next missing signal was source
transport attribution: we could count established connection transports and
rejected dial transports, but not which transport supplied successful Bitswap
blocks.

This change records the currently known transport for a successful Bitswap
source peer and emits it on successful `bitswap_fetch` and
`bitswap_session_shortcut` events as `source_transport`. The harness summarizes
that as global `bitswap source transports` and per-peer transport counts in
`bitswap peer fetches`.

Validation smoke:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-source-transport-smoke.json \
  --trace-output /tmp/ipfs-tech-source-transport-smoke-trace.jsonl
```

Result: `1/1`. Run total `6146ms`; root TTFB `2042ms`; asset TTFB
p50/p95/max `280/2740/3524ms`; max RSS `55600KiB`; max FD count `48`;
gateway statuses `27x200`, `6x206`; no limiter denials. The trace reported
`bitswap source transports: tcp=27`, connection transports `tcp=13`, rejected
dial transports `tcp=68`, `quic=15`, `ws=5`, and per-peer source transport
counts of `tcp=8`, `tcp=7`, and `tcp=12`.

This is diagnostic only. It does not change peer selection, address scoring,
connection limits, Bitswap request behavior, or block verification. Future
selective QUIC experiments should use this signal to prove that a transport
preference changes the transport that actually returns verified blocks, not just
the transport mix of connection attempts.

## Keep Two Addresses Per Bitswap Peer

Hypothesis: each Bitswap peer currently retains up to four ranked dial addresses,
but recent page traces show verified successful blocks coming almost entirely
from TCP while secondary QUIC/WS addresses still add rejected-dial pressure.
Capping each peer at its best two addresses may keep one fallback path while
reducing speculative address churn.

Same-window default cap `4` baseline:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-source-transport-baseline-r3.json \
  --trace-output /tmp/ipfs-tech-source-transport-baseline-r3-trace.jsonl
```

Result: `3/3`. Run total p50/p95/max `11038/12750/12750ms`; root TTFB
p50/p95/max `8146/11250/11250ms`; asset TTFB p50/p95/max
`224/1143/1924ms`; source transports `tcp=105`; rejected dial transports
`tcp=285`, `quic=73`, `ws=14`.

Temporary cap `1`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-peer-addr-cap1-r3.json \
  --trace-output /tmp/ipfs-tech-peer-addr-cap1-r3-trace.jsonl
```

Result: `3/3`. Run total p50/p95/max improved to `7459/9887/9887ms`, and
root TTFB p50/p95/max improved to `2560/4698/4698ms`, but asset TTFB
p50/p95/max worsened to `278/2592/4576ms` and the trace introduced two 4s
trusted Bitswap request timeouts. Decision: reject cap `1`; it removes too much
fallback.

Temporary cap `2`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-peer-addr-cap2-r3.json \
  --trace-output /tmp/ipfs-tech-peer-addr-cap2-r3-trace.jsonl
```

Result: `3/3`. Run total p50/p95/max `5373/6644/6644ms`; root TTFB
p50/p95/max `2097/4026/4026ms`; asset TTFB p50/p95/max `180/1712/2717ms`;
source transports `tcp=105`; rejected dial transports `tcp=243`, `quic=49`,
`ws=5`; no Bitswap request timeouts. This was a better balance than cap `1`.

Same-window default cap `4` recheck:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-peer-addr-cap4-recheck-r3.json \
  --trace-output /tmp/ipfs-tech-peer-addr-cap4-recheck-r3-trace.jsonl
```

Result: `3/3`, but worse than cap `2` in the same window: run total
p50/p95/max `10505/14994/14994ms`; root TTFB p50/p95/max
`4306/12454/12454ms`; asset TTFB p50/p95/max `226/2073/4424ms`; one 4s
trusted Bitswap request timeout; rejected dial transports `tcp=250`, `quic=79`,
`ws=22`.

Secondary range case:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/vitalik-peer-addr-cap2-r3.json \
  --trace-output /tmp/vitalik-peer-addr-cap2-r3-trace.jsonl
```

Cap `2` passed `3/3`, root TTFB p50/p95/max `5919/7645/7645ms`, source
transports `tcp=6`, and two trusted request timeouts.

Default cap `4` in the same window:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/vitalik-peer-addr-cap4-recheck-r3.json \
  --trace-output /tmp/vitalik-peer-addr-cap4-recheck-r3-trace.jsonl
```

Result: `0/3`. All three requests returned `504`, with six trusted Bitswap
request timeouts on the child CID. This is the strongest keep signal for cap
`2`: it passed a case that default cap `4` failed in the same network window.

Daicowtf check:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/daicowtf-peer-addr-cap2-r3.json \
  --trace-output /tmp/daicowtf-peer-addr-cap2-r3-trace.jsonl
```

Cap `2` failed `0/3` with root `504` after about `30960ms`. Default cap `4` in
the same window also failed `0/3`:
`/tmp/daicowtf-peer-addr-cap4-recheck-r3.json` and
`/tmp/daicowtf-peer-addr-cap4-recheck-r3-trace.jsonl`. The failing child CID was
again dominated by low provider diversity and DHT/provider lookup timeouts, so
this remains a provider discovery/session fallback gap rather than evidence for
or against the per-peer address cap.

Same-window Kubo comparison:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/daicowtf-cap2-kubo-compare-r1.json \
  --trace-output /tmp/daicowtf-cap2-kubo-compare-r1-trace.jsonl
```

Result: Rust and Kubo both failed `1/1`. Rust root TTFB was `30950ms`; Kubo
root TTFB was `30003ms`. Rust RSS/FD max was `42880KiB`/`21`; Kubo RSS/FD max
was `158484KiB`/`295`. This confirms the daicowtf window was not a Rust-only cap
regression.

Decision: keep cap `2`. It reduces secondary transport dial pressure while
preserving one fallback address, improves `ipfs.tech` page tails in same-window
A/B, and avoids the vitalik child-CID failure seen with cap `4`. Continue
watching daicowtf separately; that case needs better sparse-provider fallback,
not more addresses per peer.

## Current Cap-2 Kubo Comparison

After keeping the two-address Bitswap peer cap, run a fresh Rust/Kubo comparison
before changing another behavior knob.

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-cap2-kubo-compare-r3.json \
  --trace-output /tmp/ipfs-tech-cap2-kubo-compare-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`1752/2086ms`; Kubo was `1268/1303ms`. Rust asset TTFB p50/p95 was
`254/2408ms`; Kubo was `109/377ms`. Rust stayed much smaller:
max RSS/FD `53152KiB`/`51` versus Kubo `137284KiB`/`66`. Trace summary showed
Rust source transports `tcp=98`, connection transports `tcp=37`, rejected dial
transports `tcp=263`, `quic=56`, `ws=9`, no trusted request timeouts, and
asset tails dominated by successful per-block Bitswap fetches rather than
gateway/UnixFS work.

## Rejected 500ms WANT_HAVE Probe

Hypothesis: untrusted provider peers currently get a `750ms` `WANT_HAVE` probe
before falling back to `WANT_BLOCK`. Lowering the probe budget to `500ms` might
reduce small asset tails without changing dial fanout or block verification.

Temporary 500ms experiment:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-want-have-500ms-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-want-have-500ms-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 improved to
`1129/1245ms`, but Rust asset TTFB p50/p95 was `334/1763ms`.

Same-window 750ms recheck:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-want-have-750ms-recheck-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-want-have-750ms-recheck-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`1555/1580ms`; Rust asset TTFB p50/p95 was better at `306/1509ms`.

Decision: reject `500ms` and keep `750ms`. The lower probe budget helps root
startup in this window, but the active gap is asset p95 versus Kubo, and the
same-window recheck showed worse Rust asset p50/p95 at `500ms`. Future
WANT_HAVE work should compare direct WANT_BLOCK or batched session requests,
not just trim this timeout.

## Kept Direct WANT_BLOCK For Two Untrusted Providers

Hypothesis: the remaining `ipfs.tech` asset p95 gap is caused by spending one
`WANT_HAVE` probe round trip on all unknown provider peers. Racing a small
number of unknown providers with direct `WANT_BLOCK` should reduce page-asset
tails while keeping the rest of the provider set conservative and bounded.

Temporary experiment with at most two unknown provider peers using direct
`WANT_BLOCK`; later unknown peers still use `WANT_HAVE`:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-direct-two-want-block-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-direct-two-want-block-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`707/746ms`; Kubo was `1321/1392ms`, so Rust was `0.54x` Kubo on root startup.
Rust asset TTFB p50/p95 was `251/1092ms`; Kubo was `88/203ms`. That is still
slower than Kubo for small assets, but materially better than the current cap-2
baseline asset p95 of `2408ms` and the same-window `750ms` WANT_HAVE recheck at
`1509ms`. Rust stayed small: max RSS/FD `54432KiB`/`45` versus Kubo
`127692KiB`/`50`.

Control with only one unknown direct `WANT_BLOCK`:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-direct-one-want-block-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-direct-one-want-block-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`, and Rust root startup was fast
(`653/744ms` p50/p95 versus Kubo `1447/1794ms`), but Rust asset p95 regressed
to `3159ms` versus Kubo `327ms`. Decision: reject direct `1`; it does not give
enough parallelism to collapse the asset tail.

Regression checks:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/vitalik-direct-two-want-block-r3.json \
  --trace-output /tmp/vitalik-direct-two-want-block-r3-trace.jsonl

cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/daicowtf-direct-two-want-block-kubo-r1.json \
  --trace-output /tmp/daicowtf-direct-two-want-block-kubo-r1-trace.jsonl
```

`vitalik-root-html-range` passed `3/3` with root TTFB p50/p95/max
`6062/6441/6441ms`, max RSS `39168KiB`, and max FD count `35`; the trace still
showed `3` request timeouts with trusted peers on a child CID, so keep watching
that path in later sessions. `daicowtf-page-assets` failed for both Rust and
Kubo in the same network window: Rust root TTFB was `30990ms`, Kubo was
`30003ms`, and Rust stayed smaller at max RSS/FD `45184KiB`/`17` versus Kubo
`144428KiB`/`181`. That failure remains a sparse/stale public provider problem,
not a Rust-only regression from this change.

Decision: keep direct `WANT_BLOCK` for the first two untrusted provider peers.
It improves the active `ipfs.tech` asset tail without adding public gateway
fallback, serving unverifiable data, or broadening the full peer/address fanout.
Continue testing this against provider-sparse pages and consider a future
adaptive rule that raises or lowers the direct count based on observed provider
quality.

Rejected direct `3` follow-up:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-direct-three-want-block-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-direct-three-want-block-kubo-r3-trace.jsonl
```

Result: Rust regressed to `1/3` while Kubo passed `3/3`. Rust root TTFB stayed
fast at p50/p95 `759/923ms` versus Kubo `1415/1573ms`, but Rust asset p95
blew out to `8844ms` versus Kubo `183ms`. Max RSS/FD stayed acceptable at
`55964KiB`/`45`, so this was not a resource exhaustion signal; the third direct
unknown peer likely increases request contention/noise without improving peer
quality. Decision: reject direct `3` and keep direct `2`.

Rejected adaptive direct-budget follow-up:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-adaptive-direct-want-block-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-adaptive-direct-want-block-kubo-r3-trace.jsonl
```

This variant reduced the unknown direct `WANT_BLOCK` budget by the number of
trusted session peers already in the race. Result: Rust and Kubo both passed
`3/3`, and Rust asset p50 was near Kubo (`202ms` versus `213ms`), but absolute
latency regressed versus the fixed direct-2 baseline: Rust root TTFB p50/p95
was `2133/2486ms`, and asset p95 was `2149ms`. The fixed direct-2 run was much
faster on the same target (`707/746ms` root p50/p95, `1092ms` asset p95).
Decision: reject the adaptive subtraction rule. Trusted session peers are useful,
but keeping two unknown direct races still helps avoid stale or incomplete warm
peer state during page loads.

Rejected `3500ms` mixed-trusted timeout follow-up under fixed direct-2:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-trusted-timeout-3500ms-direct-two-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-trusted-timeout-3500ms-direct-two-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`, but it did not beat the fixed direct-2
baseline at `4s`. Rust root TTFB p50/p95 was `778/2095ms`; asset TTFB p50/p95
was `232/1347ms`. The kept direct-2 baseline was better on the same target:
root p50/p95 `707/746ms` and asset p95 `1092ms`. Decision: reject `3500ms` and
keep the mixed-trusted cap at `4s`; the remaining tail is not solved by trimming
that timeout further.

Rejected harness-side asset concurrency `4` signal:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 4 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-direct-two-concurrency4-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-direct-two-concurrency4-kubo-r3-trace.jsonl
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB stayed good at
`806/976ms` p50/p95, but Rust asset TTFB p95 worsened to `1448ms` versus the
fixed direct-2/concurrency-6 baseline at `1092ms`. Rust RSS/FD improved slightly
to `50688KiB`/`47`, but latency is the active gap. Decision: do not prototype an
internal lower fetch-concurrency limiter from this signal; it likely trades away
parallelism without solving slow connected-peer stalls.

## Slow Request Trace Summary

The direct-2 runs exposed a diagnostic gap: `slow_events` showed individual
phase tails, but a page load with many overlapping assets still required manual
JSONL greps to reconstruct which gateway request owned the slow Bitswap,
UnixFS, and response events. The harness now emits a bounded `slow_requests`
summary that groups trace events by gateway request path/request ID, including
status, request elapsed time, max event time, phase counts, and top CIDs.

Validation smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/ipfs-tech-slow-requests-smoke-r1.json \
  --trace-output /tmp/ipfs-tech-slow-requests-smoke-r1-trace.jsonl
```

Result: Rust passed `1/1`. Root TTFB was `4934ms`; asset TTFB p50/p95/max was
`223/807/1036ms`; max RSS/FD was `53436KiB`/`67`. The new `slow_requests`
console section immediately pointed at the root request:
`/ipns/ipfs.tech/` with status `200`, request ID `1`, elapsed `4920ms`, and CIDs
`bafkreibnzgajg3gsyn5c4p5e2h7racpy6dy7tnhwe5l4v4vx5e32qmn4bi` plus
`bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq`. Its top events
included the same `4000ms` mixed trusted Bitswap timeout seen in `slow_events`,
but without losing the request-level context. This is diagnostic only; it does
not change gateway/retrieval behavior, public fallback policy, block
verification, or resource limits.

Follow-up diagnostic fix: timeout target summaries now use the same direct
`WANT_BLOCK` versus `WANT_HAVE` mode planner as the actual Bitswap request path.
Before this, `bitswap_request_timeout_detail.targets` could label the first
untrusted direct-2 peers as `want-have` because it formatted the pre-plan peer
list. Future timeout traces should now describe the actual request mode used for
each listed peer.

Rejected Bitswap connection limit `24`:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-conn-limit-24-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-conn-limit-24-kubo-r3-trace.jsonl

cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --comparison-output /tmp/ipfs-tech-conn-limit-16-recheck-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-conn-limit-16-recheck-kubo-r3-trace.jsonl
```

The same-window `ipfs.tech` A/B made `24` look plausible: Rust root p50/p95 was
`752/756ms` at limit `24` versus `672/917ms` at limit `16`, and asset p95
improved from `1601ms` to `1293ms`. RSS stayed flat near `54MiB`; FD max rose
from `47` to `57`.

Regression check:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --output /tmp/vitalik-conn-limit-24-r3.json \
  --trace-output /tmp/vitalik-conn-limit-24-r3-trace.jsonl
```

Result: reject. `vitalik-root-html-range` regressed to `2/3` with one `504`.
Root p50/p95/max was `6667/9278/9278ms`; max RSS/FD remained modest at
`38784KiB`/`31`, but the trace had `3` `request_timeouts_with_trusted` and
multiple resets on the child CID. The small `ipfs.tech` tail improvement does
not justify a reliability regression on a known mobile smoke path. Keep the
shared Bitswap connection limits at `16`; future connection-limit work needs a
more selective policy than globally raising the swarm cap.

## 2026-05-04 Post-Lookup Session Shortcut

Hypothesis: the existing recent-peer shortcut was usually losing before it
started. Delegated provider lookup often returns in `20-60ms`, while the
shortcut slept `150ms` before asking known-good Bitswap peers. That meant page
asset requests still entered the full provider fanout, where mixed
trusted/provider commands occasionally hit the `4000ms` timeout.

Baseline:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-session-grace150-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-grace150-kubo-r3-trace.jsonl
```

Result: Rust/Kubo both passed `3/3`. Rust root p50/p95 was `672/710ms` versus
Kubo `3560/4377ms`, but Rust asset p50/p95 was `193/2073ms` versus Kubo
`226/583ms`. The trace showed `session_shortcut_starts=11`,
`session_shortcut_hits=9`, and `request_timeouts_with_trusted=1`.

Rejected first try: set the shortcut grace to `0ms` without changing the
provider-lookup race.

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-session-grace0-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-grace0-kubo-r3-trace.jsonl
```

Result: Rust/Kubo both passed `3/3`, and Rust asset p95 moved to `1577ms`, but
the trace showed `session_shortcut_starts=102` with `0` completed attempts or
hits. Provider lookup still won and cancelled the shortcut before it could
return, so this was mostly extra hidden work plus network variance. Decision:
reject grace-only `0ms`.

Kept experiment: start the shortcut immediately and, when provider lookup wins
first, wait up to `100ms` for the recent-peer fetch before falling back to the
normal provider fanout. This keeps the full provider path as a fallback, keeps
the node read-only, and still verifies every returned block before caching.

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-session-postlookup100-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-postlookup100-kubo-r3-trace.jsonl
```

Result: Rust/Kubo both passed `3/3`. Rust root p50/p95 was `913/934ms` versus
Kubo `3871/5305ms`; Rust asset p50/p95 improved to `168/1306ms` versus Kubo
`197/642ms`. Max RSS/FD stayed mobile-friendly at `51448KiB`/`51`, and the
trace showed a real behavior change: `session_shortcut_hits=59`,
`request_timeouts_with_trusted=0`, `bitswap_fetch` count dropped to `46`, and
`bitswap_dial_rejected` dropped to `144`. Decision: keep the post-lookup
`100ms` wait despite the small root p50 cost because it materially reduces
asset tail latency and provider-fanout pressure.

Regression check:

```sh
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/session-postlookup100-regression-kubo-r3.json \
  --trace-output /tmp/session-postlookup100-regression-kubo-r3-trace.jsonl
```

`vitalik-root-html-range` passed `3/3` for both Rust and Kubo; Rust root
p50/p95 was `479/2217ms` versus Kubo `1953/2017ms`. `daicowtf-page-assets`
failed `0/3` for both Rust and Kubo with root `504` responses and small error
bodies, so this run is not a Rust-only regression signal; rerun daicowtf in a
later network window before drawing behavior conclusions for that corpus item.

Rejected follow-up: only allow the post-lookup wait when at least two recent
session peers are available.

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --comparison-output /tmp/ipfs-tech-session-postlookup100-min2-kubo-r3.json \
  --trace-output /tmp/ipfs-tech-session-postlookup100-min2-kubo-r3-trace.jsonl
```

Result: reject. Rust regressed to `2/3` while Kubo passed `3/3`; two script
assets returned `504`, asset p95 rose to `4712ms`, and
`request_timeouts_with_trusted` jumped to `7`. The narrower wait skipped useful
single-peer opportunities and let the full mixed-provider request path recreate
the timeout tail.

## 2026-05-05 Keep 200ms Post-Lookup Session Wait

Hypothesis: the `100ms` post-lookup wait still drops too many useful session
peer shortcuts. A slightly longer bounded wait may let known-good peers serve
nearby page blocks while still falling back to provider fanout quickly.

Current `100ms` one-run baseline after the relay experiment was reverted:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-current-post-relay-revert-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-current-post-relay-revert-r1.json
```

Result: Rust/Kubo both passed `1/1`. Rust root TTFB was `2032ms` versus Kubo
`1882ms`; Rust asset p50/p95 was `580/2011ms` versus Kubo `198/424ms`. The
trace showed `provider_lookup=31`, `bitswap_fetch=31`, `dial_rejected=127`,
`session_shortcut_starts=34`, and only `4` shortcut hits.

Experiment: increase `BITSWAP_SESSION_POST_LOOKUP_GRACE` from `100ms` to
`200ms`.

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib recent_bitswap
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-postlookup200-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-postlookup200-r3.json
```

Result at `200ms`: Rust/Kubo both passed `3/3`. Rust root p50/p95 was
`1557/2320ms` versus Kubo `1426/3848ms`. Rust asset p50/p95 was `136/1369ms`
versus Kubo `224/1734ms`. RSS/FD stayed low at Rust `51108KiB`/`46` versus
Kubo `313148KiB`/`518`.

Trace behavior changed in the intended direction:

```text
provider_lookup=31
bitswap_fetch=31
dial_rejected=46
session_shortcut_starts=102
session_shortcut_post_lookup_waits=28
session_shortcut_hits=74
request_timeouts_with_trusted=0
```

Nearby `100ms` recheck:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-postlookup100-recheck-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-postlookup100-recheck-r3.json
```

Result at `100ms`: Rust/Kubo both passed `3/3`. Rust root p50/p95 was
`2472/3335ms` versus Kubo `2241/2500ms`. Rust asset p50/p95 was `188/1447ms`
versus Kubo `178/1930ms`. The trace had more provider fanout:

```text
provider_lookup=56
bitswap_fetch=56
dial_rejected=217
session_shortcut_starts=102
session_shortcut_post_lookup_waits=52
session_shortcut_hits=50
request_timeouts_with_trusted=1
```

Regression check at `200ms`:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/postlookup200-regression-r3-trace.jsonl \
  --comparison-output /tmp/postlookup200-regression-r3.json
```

`vitalik-root-html-range` passed `3/3` for both Rust and Kubo; Rust root p50/p95
was `389/687ms` versus Kubo `1688/1857ms`. `daicowtf-page-assets` failed `0/3`
for both Rust and Kubo with root timeouts, so this run is not a Rust-only
regression signal.

Decision: keep `200ms`. It increases the number of completed shortcut hits,
cuts provider lookups and full Bitswap fanout, lowers dial pressure, and did
not create a Rust-only failure in the regression cases. Keep watching root TTFB
and range-heavy asset tails in future live windows.

## 2026-05-04 Provider Refresh Equivalence Follow-Up

Hypothesis: a later `ipfs.tech` failure window was not caused by low provider
counts alone. Delegated routing returned different raw provider counts across
the initial lookup and timeout refresh, but the useful expanded Bitswap peer set
often stayed effectively the same. That makes the retry look fresh while still
asking the same reachable public-network peers for the root block.

Baseline:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --trace-output /tmp/ipfs-tech-postlookup100-current-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-postlookup100-current-kubo-r3.json
```

Result: Rust failed `0/3`; Kubo passed `3/3`. Rust root p50/p95 was
`30331/30388ms`; Kubo root p50/p95 was `1372/2989ms`. Rust spent two bounded
`15s` Bitswap request windows per root against delegated providers. The first
delegated lookup returned about `10-11` providers expanding to `6` Bitswap
peers; refresh returned about `11-14` providers expanding to the same or similar
`6-8` usable peers.

Kept diagnostic:

- `retry_provider_count` now logs both `same_provider_set` and
  `same_bitswap_peer_set`.
- `same_bitswap_peer_set` normalizes expanded Bitswap peers by peer ID plus
  sorted deduped multiaddrs, after DNS multiaddr expansion.
- This is intentionally diagnostic only. It does not change provider selection,
  retries, timeout budgets, trust rules, block verification, or caching.

Rejected experiment: after a Bitswap request timeout, force a short light-DHT
provider augmentation when the refreshed delegated lookup produced the same
expanded Bitswap peer set.

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --trace-output /tmp/ipfs-tech-quality-fallback-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-quality-fallback-kubo-r3.json

FREEDOM_IPFS_LIVE_DHT_CID=bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq \
timeout 35s cargo test -p freedom-ipfs-routing \
  live_light_dht_finds_public_providers -- --ignored --nocapture
```

Result: reject. Rust still failed `0/3`; Kubo passed `3/3`. Rust root p50/p95
was `31268/31420ms`; Kubo root p50/p95 was `1471/1598ms`; RSS/FD stayed modest
at `42496KiB`/`20` for Rust versus `310728KiB`/`481` for Kubo. The fallback did
trigger, but the short DHT lookup timed out after `750ms` and added no
providers. The live DHT test also found `0` providers for
`bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq` after `22.12s`.
Conclusion: light-DHT augmentation is not the missing recovery path for this
root in this network window.

Rejected experiment: cap each expanded Bitswap peer to transport-diverse
addresses, preferring one TCP address plus one QUIC address instead of the first
two TCP addresses.

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --trace-output /tmp/ipfs-tech-transport-diverse-cap2-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-transport-diverse-cap2-kubo-r3.json
```

Result: reject. Rust still failed `0/3`; Kubo passed `3/3`. Rust root p50/p95
was `30493/30980ms`; Kubo root p50/p95 was `2555/3225ms`; RSS/FD stayed modest
at `43392KiB`/`18` for Rust versus `314720KiB`/`514` for Kubo. The trace showed
QUIC was not simply absent: first expanded providers included
`peer_count=8,tcp=9,quic=6,ws=1`, later refreshes included
`peer_count=6,tcp=6,quic=6,ws=0`, and established transports included
`tcp=5` and `quic=4`. QUIC fallback connected, but no peer returned the root.

Rejected investigation: delegated provider cap prioritization by direct HTTP or
Bitswap dialability. A direct delegated response saved at
`/tmp/ipfs-tech-delegated-providers.ndjson` had only `21` providers and `8`
direct provider peers, so the existing cap did not appear to be dropping a large
tail of directly dialable providers for this root in this window.

Kubo comparison notes:

- Kubo `v0.41.0` lowpower config used `Routing.Type = autoclient` and
  `Routing.DelegatedRouters = ["auto"]`.
- Kubo autoconf included `https://cid.contact` for IPNI provider lookups and
  `https://delegated-ipfs.dev` for AminoDHT/IPNI providers, peers, and IPNS.
- `https://cid.contact/routing/v1/providers/<root>` returned `404` in this
  window.
- Kubo local provider discovery returned many more provider IDs than Rust can
  use directly from delegated provider records.
- Some peer-routing lookups for those provider IDs returned mostly relay/circuit
  addresses. Rust currently rejects `/p2p-circuit`, WebRTC Direct, and
  WebTransport addresses and only dials usable direct addresses in provider
  records.

Next hypothesis: the remaining Kubo gap for this root is likely peer-routing and
provider-address quality, not raw delegated provider count or a short DHT
fallback. Future work should add explicit diagnostics for unsupported provider
address families and investigate a bounded peer-routing address-resolution path
for ID-only providers before considering heavier relay support.

## 2026-05-05 Provider Address Quality Diagnostics

Follow-up diagnostic:

- `bitswap_peer_expand` now reports provider address quality counters, not only
  the supported expanded peer set.
- The trace can distinguish direct Bitswap candidates from providers discarded
  because they are ID-only, have invalid peer IDs, have no embedded/provider
  peer ID, contain unparsable multiaddrs, or use unsupported address families.
- Unsupported address families are split into relay (`/p2p-circuit`),
  WebTransport, WebRTC, certhash, and other unsupported transports.
- The mobile web harness includes these fields in slow-event details and also
  aggregates them under `trace_summary.bitswap_provider_quality`, so live Kubo
  comparisons can show whether Rust is losing because delegated routing returned
  no providers, because provider records contained only unsupported addresses,
  or because reachable direct Bitswap peers did not serve the block.

Important fields:

- `provider_addr_count`
- `expanded_provider_addr_count`
- `supported_provider_addr_count`
- `rejected_provider_addr_count`
- `id_only_provider_count`
- `invalid_provider_id_count`
- `provider_without_supported_bitswap_addr_count`
- `unsupported_relay_addr_count`
- `unsupported_webtransport_addr_count`
- `unsupported_webrtc_addr_count`
- `unsupported_certhash_addr_count`
- `unsupported_transport_addr_count`
- `missing_peer_addr_count`
- `unparsable_addr_count`

This is still diagnostic only. It does not dial relays, change provider ranking,
change timeout caps, skip verification, or alter the read-only behavior.

Validation run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-provider-quality-aggregate-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-provider-quality-aggregate-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `470ms` versus Kubo
`3811ms`; Rust asset p50/p95 was `158/995ms` versus Kubo `176/474ms`. Rust
resource use stayed much lower: max RSS/FD `51504KiB`/`45` versus Kubo
`296280KiB`/`347`.

The provider-quality aggregate showed that this page load was not provider-poor,
but most advertised addresses were not usable direct Bitswap dials:

```text
events=19
provider_addr_count=5803
expanded_provider_addr_count=5848
supported_provider_addr_count=1569
rejected_provider_addr_count=4279
id_only_provider_count=34
provider_without_supported_bitswap_addr_count=183
unsupported_relay_addr_count=1480
unsupported_webtransport_addr_count=1338
unsupported_webrtc_addr_count=1425
unsupported_transport_addr_count=36
```

The root CID itself had `297` provider addrs, `77` supported addrs, and `220`
rejected addrs: `78` relay, `75` WebTransport, and `67` WebRTC. This supports
the next hypothesis that provider-address quality and possibly bounded
peer-routing/relay-aware behavior matter more than increasing direct provider
fanout for this class of Kubo gap.

## 2026-05-05 Rejected ID-Only Peer-Routing Augmentation

Hypothesis: some delegated provider records are provider IDs with no addresses,
and Kubo may recover those by asking delegated peer routing for `/peers/{peer}`.
A bounded Rust version might cheaply fill in addresses before Bitswap expansion.

Experiment:

- For each delegated provider response, query `/peers/{peer}` for at most the
  first `4` ID-only providers.
- Run those peer-routing requests concurrently.
- Cap each peer-routing request at `750ms`.
- Treat peer-routing errors/timeouts as non-fatal and keep the original provider
  response.

The deterministic test passed, but the live run did not justify keeping the
behavior:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-peer-routing-idonly-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-peer-routing-idonly-r1.json
```

Result: Rust and Kubo both passed `1/1`, but Rust asset p95 was still much
worse: Rust asset p50/p95 `163/1498ms` versus Kubo `153/337ms`. Rust root TTFB
was `2269ms` versus Kubo `3222ms`, and resources remained mobile-friendly at
Rust RSS/FD `53348KiB`/`44` versus Kubo `243352KiB`/`225`.

Trace evidence:

```text
delegated_peer_routing count=6 total_ms=1368 p50=19ms p95=752ms max=752ms
resolved=0 in every delegated_peer_routing event
timed_out=2 for one request
bitswap_provider_quality.id_only_provider_count=17
bitswap_provider_quality.provider_without_supported_bitswap_addr_count=132
```

Decision: reject and revert the behavior. In this window, delegated peer
routing for ID-only provider records added bounded but real latency and resolved
no useful provider addresses. The more important remaining signal is still the
large unsupported direct-address mix: relay, WebTransport, and WebRTC records,
not ID-only records.

## 2026-05-05 Relay/WebTransport Feasibility Note

Dependency check:

```sh
rg -n "libp2p|webtransport|webrtc|relay|p2p-circuit" \
  Cargo.toml crates/*/Cargo.toml Cargo.lock

cargo tree -p freedom-ipfs-retrieval -i libp2p --features ''
```

Current workspace libp2p features are:

```text
dns, ed25519, identify, kad, macros, noise, ping, quic, rsa, tcp, tls, tokio,
websocket, yamux
```

Not enabled:

```text
relay
webrtc-websys
webtransport-websys
```

Local crate inspection for `libp2p v0.56.0` shows:

- `relay` is available as a normal feature and exposes a relay client behaviour
  through the libp2p swarm builder.
- `webrtc-websys` and `webtransport-websys` are gated for `wasm32`/websys, so
  they are not an obvious native Linux/mobile-reader transport toggle.

Conclusion: relay support is the more realistic next transport experiment than
native WebTransport/WebRTC in the current dependency set, but it is not a tiny
address-parser change. A serious relay experiment needs at least:

- enable libp2p `relay`
- add relay client behaviour to the shared Bitswap swarm
- stop rejecting selected `/p2p-circuit` provider addrs
- preserve current connection/resource caps for mobile
- add deterministic relay-loopback coverage
- run same-window Kubo comparisons before keeping it

Given the rejected ID-only peer-routing result, relay support should be treated
as the next substantial experiment, not as a small follow-on to provider lookup.

## 2026-05-05 Inclusive Unsupported Address Counters

Follow-up diagnostic:

The provider-quality trace now includes inclusive address-family counters in
addition to primary rejection reasons:

- `addr_with_relay_count`
- `addr_with_webtransport_count`
- `addr_with_webrtc_count`
- `addr_with_certhash_count`

This matters because many public provider addresses contain more than one
unsupported feature, for example WebTransport plus `/p2p-circuit`. The old
primary-reason counters were still useful for explaining why Rust rejected an
address, but they understated how many records would become relevant to a relay
experiment.

Validation run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-inclusive-provider-quality-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-inclusive-provider-quality-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `1075ms` versus
Kubo `1362ms`; Rust asset p50/p95 was `111/1118ms` versus Kubo `93/230ms`.
Rust resource use stayed low at RSS/FD `50740KiB`/`44` versus Kubo
`121024KiB`/`48`.

Provider-quality aggregate:

```text
events=15
provider_addr_count=5557
expanded_provider_addr_count=5602
supported_provider_addr_count=1370
rejected_provider_addr_count=4232
unsupported_relay_addr_count=1491
unsupported_webtransport_addr_count=1319
unsupported_webrtc_addr_count=1374
unsupported_transport_addr_count=48
addr_with_relay_count=2526
addr_with_webtransport_count=1319
addr_with_webrtc_count=1374
addr_with_certhash_count=2691
```

Root CID signal:

```text
provider_addr_count=357
supported_provider_addr_count=85
rejected_provider_addr_count=278
unsupported_relay_addr_count=98
addr_with_relay_count=157
addr_with_webtransport_count=83
addr_with_webrtc_count=89
addr_with_certhash_count=172
```

Conclusion: relay exposure is larger than the primary rejection bucket implied.
For this run, `45%` of expanded provider addresses contained `/p2p-circuit`
(`2526/5602`), while only `27%` were primarily classified as relay rejection
(`1491/5602`). That strengthens the case that relay support deserves a
dedicated experiment, while WebTransport/WebRTC remain non-trivial native
transport work in the current dependency set.

## 2026-05-05 Relay Builder Sequencing Correction

Follow-up inspection:

The libp2p relay builder shortcut needs to be inserted at the right phase, but
it does not require a full manual transport rewrite.

Current Bitswap transport construction in
`crates/freedom-ipfs-retrieval/src/lib.rs` is:

```text
with_tcp(...)
with_quic()
with_other_transport(cloudflare_websocket_transport)
with_behaviour(...)
```

That custom WebSocket transport is intentional: WSS providers need DNS names
preserved for SNI, and it uses explicit Cloudflare DNS instead of the builder's
system-DNS WebSocket shortcut.

Local `libp2p v0.56.0` builder inspection initially looked risky because the
relay shortcut methods route through helpers named `without_*`:

- `OtherTransportPhase::with_relay_client(...)` calls
  `without_any_other_transports().without_dns().without_websocket()...`
- `QuicPhase::with_relay_client(...)` calls
  `without_quic().without_any_other_transports().without_dns().without_websocket()...`
- `WebsocketPhase::with_relay_client(...)` calls
  `without_websocket()...`

On closer inspection those helpers preserve the already-accumulated transport
value while moving the type-state builder to the next phase. The unsafe case is
calling relay before a transport has been added. The safe insertion point for
the current Bitswap swarm is after:

```text
with_tcp(...)
with_quic()
with_other_transport(cloudflare_websocket_transport)
```

and before `with_behaviour(...)`.

The inspected crate files were:

- `/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/libp2p-0.56.0/src/builder/phase/other_transport.rs`
- `/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/libp2p-0.56.0/src/builder/phase/quic.rs`
- `/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/libp2p-0.56.0/src/builder/phase/websocket.rs`
- `/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/libp2p-0.56.0/src/builder/phase/relay.rs`

Decision: the builder insertion point is feasible, but the relay implementation
experiment was rejected and reverted.

Rejected prototype:

- added libp2p `relay`
- added `relay::client::Behaviour` to the shared Bitswap swarm
- accepted `/p2p-circuit` addresses with a concrete relay peer
- kept WebTransport/WebRTC relay records rejected
- tried both broad relay candidates and fallback/capped relay-only candidates
- traced accepted relay candidate count with `relay_addr_count`

Deterministic coverage passed while the prototype existed:

```sh
cargo test -p freedom-ipfs-retrieval --lib
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo build -p freedom-ipfs-gateway
```

Live broad relay run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-relay-prototype-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-relay-prototype-r1.json
```

Result: Rust and Kubo both passed, but Rust did not improve enough to justify
the added transport surface. Rust root TTFB was `5414ms` versus Kubo `3713ms`;
Rust asset p50/p95 was `328/7688ms` versus Kubo `191/435ms`. Trace aggregate:
`relay_addr_count=301`, `conn_relay=3`, and no successful block fetch from a
relay-connected peer.

Live fallback-only relay run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-relay-fallback-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-relay-fallback-r1.json
```

Result: reject. Rust failed `0/1` while Kubo passed `1/1`. Rust produced `12`
gateway `504` responses, `17` Bitswap request timeouts, `relay_addr_count=332`,
`conn_relay=3`, and no successful relay-served block. One useful concrete error
was `Relay has no reservation for destination.`

Live capped relay-only run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-relay-capped-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-relay-capped-r1.json
```

Result: Rust and Kubo both passed, with Rust root TTFB `942ms` versus Kubo
`3007ms`, and Rust asset p50/p95 `146/1184ms` versus Kubo `149/878ms`.
However, trace showed `relay_addr_count=43`, `dial_rejected_relay=16`,
`conn_relay=0`, and all successful Bitswap fetches over direct TCP. The best
version was effectively a no-op plus extra rejected dials.

Conclusion: do not keep relay support yet. Relay may still be useful for sparse
provider cases, but it needs a more selective design:

- never let relay-only candidates displace enough direct candidates for a block
- only consider relay when direct provider diversity is genuinely low
- budget relay dials separately from direct dials
- suppress relays that return `NoReservation`
- add a deterministic relay-loopback test before any future live run

## 2026-05-05 Harness Request Correlation Fix

Issue:
`mobile-web-harness --fresh-gateway-per-run` appends trace events from multiple
short-lived gateway processes into one JSONL file. The gateway request counter
starts at `1` in each process, so harness `slow_requests` aggregation could
merge unrelated requests that reused the same `(request_id, path)` across
runs. That made multi-run trace summaries misleading while inspecting the
post-lookup session wait and UnixFS range path.

Implementation:

- Gateway request spans now include `process_id` for both `/ipfs` and `/ipns`
  requests.
- Harness request aggregation keys on `(process_id, request_id, path)`, falling
  back to the old key for older traces without `process_id`.
- Slow request console output renders non-empty ids as `process_id:request_id`.
- Added a deterministic harness regression test with two restarted gateway
  traces that both use `request_id=1` and the same path.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_keeps_restarted_gateway_request_ids_separate
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-gateway request
```

Decision: keep. This is a diagnostics-only fix with no retrieval behavior
change. Future multi-run live traces should no longer overstate per-request
event counts or merge CIDs from different gateway processes.

## 2026-05-05 Bitswap Per-Peer Attempt Tracing

Hypothesis:
The remaining cold-root tail in the corrected `ipfs.tech` comparison is inside
Bitswap after provider lookup and peer expansion are already done. To tune peer
selection or batching safely, traces need to show more than the final
`bitswap_fetch` winner.

Current same-window baseline before this diagnostic patch:

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-trace-correlation-current-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-trace-correlation-current-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust asset p50/p95 was `190/592ms`
versus Kubo `253/753ms`, and Rust RSS/FD max was `52104KiB`/`49` versus Kubo
`313704KiB`/`481`. Rust root p50 was better (`2465ms` versus `3884ms`), but
root p95 was worse (`6382ms` versus `4086ms`). The corrected slow-request
summary separated the three root requests by process id. The slow Rust root
request spent `5821ms` in `bitswap_fetch` for the root CID after a normal
`47ms` provider lookup and `68ms` peer expansion.

Implementation:

- Trace `bitswap_peer_attempt_start` for every scheduled outgoing peer attempt.
- Trace `bitswap_peer_attempt` when an outgoing peer attempt completes before
  the command is satisfied.
- Trace `bitswap_incoming_block` when an inbound Bitswap stream matches a
  pending CID.
- Harness summaries now print outgoing peer-attempt starts/completions and
  matched incoming block totals.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib
cargo build -p freedom-ipfs-gateway
```

Live trace-shape check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-peer-attempt-incoming-r1-trace.jsonl \
  --output /tmp/ipfs-tech-peer-attempt-incoming-r1.json
```

Result: passed `1/1`, root TTFB `1341ms`, asset TTFB p50/p95 `131/1117ms`,
RSS `49100KiB`, FD count `40`. The trace showed `bitswap peer attempts:
starts=157 outgoing_completed=0` and `bitswap incoming blocks: matches=36
blocks=44 bytes=906061`. That confirms the common success path is peers
responding over inbound Bitswap streams; outgoing futures are usually cancelled
once the shared swarm receives a matching inbound block.

Decision: keep. This changes diagnostics only. The next behavior experiment
should account for the inbound-response model, likely by measuring whether
smaller initial root peer races, a short first-byte hedge, or batching session
wants can reduce root/asset p95 without increasing mobile dial pressure.

Rejected follow-up: cold-only `500ms` WANT_HAVE fallback.

Temporary experiment:

- keep the existing `750ms` WANT_HAVE probe when a candidate set contains a
  trusted/session peer
- use a shorter `500ms` probe only for cold peer sets with no trusted peer

```sh
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-cold-want-have500-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cold-want-have500-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was `2239/3901ms`
versus Kubo `3068/3319ms`, so the cold root p95 remained worse than Kubo.
Rust asset p50 matched Kubo (`124ms` versus `123ms`), but asset p95 regressed
badly: Rust `1739ms` versus Kubo `394ms`. Rust remained mobile-light at max
RSS/FD `52928KiB`/`48` versus Kubo `239240KiB`/`251`.

Decision: reject and revert. The narrower timeout avoided changing warm/session
candidate sets directly, but it still did not improve the root tail enough and
made the asset tail materially worse in the same live window.

Rejected follow-up: latency-aware successful Bitswap peer ordering.

Hypothesis:
Recent trace data showed large source-peer latency differences. Session peer
reuse was still recency-only, so a slow peer that happened to respond recently
could stay hot. A lightweight EWMA of successful Bitswap fetch latency might
prefer faster peers during warm page loads without increasing dial fanout.

Temporary broad prototype:

- store `latency_ewma_ms` with each successful Bitswap peer
- sort successful provider candidates by latency EWMA before recency
- sort inserted recent session peers by latency EWMA before recency
- trace successful peer recordings with `latency_ms` and `latency_ewma_ms`

Deterministic coverage passed while the broad prototype existed:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib lower_latency
cargo test -p freedom-ipfs-retrieval --lib
cargo build -p freedom-ipfs-gateway
```

Live broad prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-latency-peer-score-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-latency-peer-score-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`1128/2463ms` versus Kubo `1482/2381ms`; Rust asset p50/p95 was
`116/1461ms` versus Kubo `119/1197ms`. Resource use stayed mobile-light at
RSS/FD `51912KiB`/`55` versus Kubo `248900KiB`/`161`.

Immediate committed-baseline comparison from a detached worktree at
`c63e095`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-baseline-c63e095-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-baseline-c63e095-r3.json
```

Baseline result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was
`1080/1107ms` versus Kubo `2427/3486ms`; Rust asset p50/p95 was `144/834ms`
versus Kubo `124/248ms`.

Second broad prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-latency-peer-score-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-latency-peer-score-r3b.json
```

Result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was `897/1342ms`
versus Kubo `2229/3839ms`; Rust asset p50/p95 was `171/687ms` versus Kubo
`134/908ms`.

Second committed-baseline comparison:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-baseline-c63e095-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-baseline-c63e095-r3b.json
```

Baseline result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was
`871/878ms` versus Kubo `2033/2419ms`; Rust asset p50/p95 was `179/846ms`
versus Kubo `108/479ms`.

Narrowed prototype:

- restore recency sorting for successful peers already present in provider
  candidate sets
- keep latency EWMA only for ordering the warm recent-session peer shortcut
  list

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-session-latency-score-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-session-latency-score-r3.json
```

Result: Rust and Kubo both passed `3/3`, but the asset tail was poor. Rust
root p50/p95 was `966/1271ms` versus Kubo `1435/1637ms`; Rust asset p50/p95
was `158/1385ms` versus Kubo `80/179ms`.

Decision: reject and revert both broad and session-only latency ordering. The
signal was unstable and not enough to beat the committed baseline on absolute
Rust p95. Also, the measured `bitswap_fetch` elapsed time is not a pure peer
latency signal: it includes request scheduling, CID/block size effects, and the
inbound-response model where the winning peer may satisfy the want through the
shared swarm while outgoing attempts are cancelled. Future peer scoring should
first collect a cleaner signal, such as per-peer first-byte timing from the
Bitswap swarm, byte-normalized throughput over multiple blocks, or bounded
session-level batching outcomes.

## 2026-05-05 Bitswap Incoming Pending-Age Trace

Hypothesis:
The per-peer attempt trace showed many successful blocks arrive through inbound
Bitswap streams while outgoing attempts are cancelled before completion. The
next behavior experiments need a cleaner timing signal around that inbound path,
especially how long a CID was pending before the inbound block matched it.

Implementation:

- Store the enqueue time next to each pending inbound Bitswap result sender.
- Add `pending_waiter_count`, `oldest_pending_ms`, and `newest_pending_ms` to
  `bitswap_incoming_block` trace events.
- Extend the harness trace summary with `max_oldest_pending_ms` and
  `max_pending_waiters` for inbound Bitswap blocks.
- Include the new fields in trace slow-event details for any future events that
  carry elapsed timing.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo test -p freedom-ipfs-retrieval --lib bitswap_tests::fetches_block_from_local_bitswap_peer
cargo build -p freedom-ipfs-gateway
```

Live trace-shape check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-incoming-pending-age-clean-r1-trace.jsonl \
  --output /tmp/ipfs-tech-incoming-pending-age-clean-r1.json
```

Result: passed `1/1`, root TTFB `911ms`, asset TTFB p50/p95 `186/1036ms`,
RSS/FD `50048KiB`/`42`. The trace showed `bitswap incoming blocks:
matches=37 blocks=87 bytes=969736 max_oldest_pending_ms=898
max_pending_waiters=2`. Sample event:

```json
{
  "phase": "bitswap_incoming_block",
  "cid": "bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq",
  "peer": "12D3KooWDpp7U7W9Q8feMZPPEpPP5FKXTUakLgnVLbavfjb9mzrT",
  "source_transport": "tcp",
  "block_count": 1,
  "bytes": 1362,
  "pending_waiter_count": 1,
  "oldest_pending_ms": "237",
  "newest_pending_ms": "237"
}
```

Decision: keep. This is diagnostics-only and does not alter provider lookup,
peer selection, Bitswap request behavior, caching, or verification. It gives
future session batching or first-byte hedge experiments a direct way to tell
whether a slow asset was waiting on inbound Bitswap delivery, command queuing,
provider lookup, or peer expansion.

Rejected follow-up: increase post-lookup session wait from `200ms` to `400ms`.

Hypothesis:
The clean pending-age trace showed `session_shortcut_post_lookup_waits=6` and
`max_oldest_pending_ms=898`. A longer post-lookup wait might allow more
recent-peer session shortcuts to finish and reduce fallback provider fanout.

Temporary experiment:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib recent_bitswap
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-postlookup400-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-postlookup400-r3.json
```

Result: reject. Rust and Kubo both passed `3/3`, but Rust regressed badly:
root TTFB p50/p95 was `3124/3544ms` versus Kubo `2409/2480ms`, and asset TTFB
p50/p95 was `179/2265ms` versus Kubo `303/711ms`. The trace showed
`session_shortcut_starts=102`, `session_shortcut_post_lookup_waits=40`,
`session_shortcut_hits=62`, `bitswap_fetches=43`, `peer_attempt_starts=697`,
and inbound `max_oldest_pending_ms=1983`. Compared with the kept `200ms`
baseline, the longer wait did not produce enough shortcut wins and held too
many requests in the slow session path. Reverted to `200ms`.

## 2026-05-05 Bitswap Dial Plan Harness Summary

Follow-up diagnostic:
Future batching and fanout experiments need dial-plan totals in the harness
summary, not only raw JSONL. The retrieval layer already emits
`bitswap_dial_plan`; the harness now aggregates:

- plan event count
- total peer targets and candidate peers
- new dial peers and addresses
- suppressed dial peers and addresses
- pending and already-connected peers
- max command queue delay

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
```

Decision: keep. This is harness-only and does not change gateway or retrieval
behavior. It makes future session batching experiments easier to judge by
showing whether a change actually lowers dial pressure or merely shifts latency
elsewhere.

## 2026-05-05 Cancel Dropped Bitswap Commands

Hypothesis:
The provider-lookup/session shortcut race can drop a `SharedBitswapClient::fetch`
future after the bounded post-lookup wait, but the shared swarm command may keep
running until completion even though the caller no longer wants the result.
Cancelling work when the response receiver is dropped should reduce hidden
Bitswap dials and inbound block traffic without changing the successful path.

Implementation:

- In `run_shared_bitswap_swarm`, race each per-command Bitswap fetch against
  `respond.closed()`.
- If the receiver is dropped first, abort the command future, let the normal
  pending-count cleanup remove the CID when appropriate, and trace
  `bitswap_fetch_cancelled` with `command_queued_ms` and `elapsed_ms`.
- This does not change provider discovery, candidate ordering, request
  timeouts, block verification, or cache writes.

Prototype validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib dropped_bitswap_fetch_cancels_open_peer_stream
cargo test -p freedom-ipfs-retrieval --lib recent_bitswap
cargo build -p freedom-ipfs-gateway
```

Live prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-cancel-dropped-bitswap-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cancel-dropped-bitswap-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`1842/2768ms` versus Kubo `1977/2207ms`; Rust asset p50/p95 was `137/1015ms`
versus Kubo `147/1771ms`. The trace showed `bitswap_fetch_cancelled=23`,
`session_shortcut_hits=78`, `bitswap_fetches=27`, `peer_attempt_starts=432`,
`new_dial_peers=114`, and inbound Bitswap bytes `2395134`.

Same-window no-cancel baseline from a detached worktree at `18dcf3a`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-nocancel-18dcf3a-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-nocancel-18dcf3a-r3.json
```

Baseline result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was
`1022/2604ms` versus Kubo `1613/2219ms`; Rust asset p50/p95 was `148/1101ms`
versus Kubo `139/861ms`. No-cancel trace totals were
`session_shortcut_hits=74`, `bitswap_fetches=31`, `peer_attempt_starts=490`,
`new_dial_peers=133`, and inbound Bitswap bytes `2950417`.

Regression check:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/cancel-dropped-bitswap-regression-r3-trace.jsonl \
  --comparison-output /tmp/cancel-dropped-bitswap-regression-r3.json
```

`vitalik-root-html-range` passed `3/3` for both Rust and Kubo; Rust root
p50/p95 was `365/547ms` versus Kubo `1884/1899ms`. `daicowtf-page-assets`
failed `0/3` for both Rust and Kubo with root timeouts, so this is not a
Rust-only regression signal.

Decision: keep. The root p95 was slightly worse in the `ipfs.tech` A/B window,
but the prototype reduced hidden Bitswap work in the intended direction:
peer attempts `490 -> 432`, new dial peers `133 -> 114`, inbound bytes
`2950417 -> 2395134`, and asset p95 `1101ms -> 1015ms`. This is aligned with
mobile resource goals and removes work after the caller has already moved on.

## 2026-05-05 Bitswap Delivery Source Trace

Hypothesis:
Peer-attempt traces repeatedly showed `outgoing_completed=0` while inbound
Bitswap streams satisfied the requested CIDs. The successful `bitswap_fetch`
and `bitswap_session_shortcut` events should identify whether the returned
block was delivered by the inbound stream path or by a direct outgoing stream,
so future batching/hedging experiments do not infer this indirectly.

Implementation:

- Add `bitswap_delivery="incoming"` to Bitswap results matched from inbound
  streams.
- Add `bitswap_delivery="outgoing"` to Bitswap results returned from an
  outgoing request stream.
- Include the field on successful `bitswap_fetch` and
  `bitswap_session_shortcut` trace events.
- Add a harness `bitswap deliveries:` summary and slow-event detail support.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p freedom-ipfs-retrieval --lib collects_
cargo build -p freedom-ipfs-gateway
```

Live trace-shape check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-bitswap-delivery-r1-trace.jsonl \
  --output /tmp/ipfs-tech-bitswap-delivery-r1.json
```

Result: passed `1/1`, root TTFB `1000ms`, asset p50/p95 `138/570ms`,
RSS/FD `49072KiB`/`46`. The trace showed `bitswap deliveries: incoming=35`,
`bitswap peer attempts: starts=135 outgoing_completed=0`, and
`bitswap incoming blocks: matches=35 blocks=35 bytes=793822
max_oldest_pending_ms=601 max_pending_waiters=2`.

Decision: keep. This is diagnostics-only and confirms the current public
network success path is overwhelmingly inbound delivery. Future behavior work
should optimize scheduling, cancellation, and batching around that model rather
than relying on completed outgoing stream attempts as the primary signal.

## 2026-05-05 Keep 6s Bitswap Stream Read Timeout

Hypothesis:
The same-stream Bitswap read path still waited up to `10s` for a response, but
recent public-network traces showed successful blocks arriving through inbound
Bitswap streams while outgoing stream attempts rarely completed. A shorter
per-stream read cap should reduce stale attempt pressure and failure tails
without changing the request-level timeout or the inbound success path.

Implementation:

- Add `BITSWAP_STREAM_READ_TIMEOUT`.
- Use `6s` instead of the previous inline `10s` timeout in
  `request_bitswap_blocks_on_stream`.
- Keep request-level caps unchanged: mixed trusted+provider requests remain
  `4s`, broader cold Bitswap requests remain `15s`.

Prototype validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib want_have_probe_falls_back_to_want_block_quickly
cargo test -p freedom-ipfs-retrieval --lib dropped_bitswap_fetch_cancels_open_peer_stream
cargo build -p freedom-ipfs-gateway
```

Live `6s` run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-stream-read6-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-stream-read6-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`921/1776ms` versus Kubo `1718/2362ms`; Rust asset p50/p95 was `134/902ms`
versus Kubo `162/2297ms`. Trace totals: `bitswap_fetches=26`,
`session_shortcut_hits=79`, `peer_attempt_starts=427`, inbound
`max_oldest_pending_ms=1064`, and `bitswap deliveries: incoming=105`.

Same-window `10s` baseline from a detached worktree at `238a019`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-stream-read10-238a019-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-stream-read10-238a019-r3.json
```

Baseline result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was
`1141/2074ms` versus Kubo `1704/2487ms`; Rust asset p50/p95 was `171/1443ms`
versus Kubo `188/1048ms`. Trace totals: `bitswap_fetches=33`,
`session_shortcut_hits=72`, `peer_attempt_starts=552`, inbound
`max_oldest_pending_ms=1601`, and `bitswap deliveries: incoming=105`.

Regression check:

```sh
cargo run -p mobile-web-harness -- \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/stream-read6-regression-r3-trace.jsonl \
  --comparison-output /tmp/stream-read6-regression-r3.json
```

`vitalik-root-html-range` passed `3/3` for both Rust and Kubo; Rust root
p50/p95 was `551/563ms` versus Kubo `1970/2043ms`. `daicowtf-page-assets`
failed `0/3` for both Rust and Kubo with root timeouts, so this is not a
Rust-only regression signal. Rust failed faster than Kubo in that window:
root p50/p95 `10970/26949ms` versus Kubo `30002/30002ms`.

Decision: keep. The `6s` cap improved the same-window `ipfs.tech` root and
asset tails, reduced peer attempts `552 -> 427`, and lowered inbound pending
age `1601ms -> 1064ms` without producing a Rust-only regression. This is
consistent with the inbound-delivery model and mobile resource goals.

Rejected follow-up: lower `BITSWAP_STREAM_READ_TIMEOUT` further from `6s` to
`4s`.

Focused validation for the prototype passed:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib want_have_probe_falls_back_to_want_block_quickly
cargo test -p freedom-ipfs-retrieval --lib dropped_bitswap_fetch_cancels_open_peer_stream
cargo test -p freedom-ipfs-retrieval --lib fetches_block_from_local_bitswap_peer
cargo build -p freedom-ipfs-gateway
```

Live `4s` run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-stream-read4-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-stream-read4-r3.json
```

Result: Rust and Kubo both passed `3/3`, but Rust tail latency regressed
badly. Rust root TTFB p50/p95 was `1376/15765ms` versus Kubo `1946/3246ms`;
Rust asset p50/p95 was `155/846ms` versus Kubo `122/415ms`. Resource usage
remained mobile-friendly at RSS/FD `51156KiB`/`51` versus Kubo
`264432KiB`/`333`, but the root tail was unacceptable.

Trace summary from `/tmp/ipfs-tech-stream-read4-r3-trace.jsonl`:
`bitswap_fetches=21`, `session_shortcut_hits=85`,
`bitswap_fetch_cancelled=15`, `peer_attempt_starts=380`, inbound
`max_oldest_pending_ms=1365`, `bitswap deliveries: incoming=105`, and one
`bitswap_request_timeout_detail` at `15001ms`.

Decision: reject. The `4s` stream cap reduced attempt pressure further but
introduced a severe request-level root TTFB tail, including a full `15s`
Bitswap request timeout. Keep `6s` as the current balance point unless a later
change makes earlier stream cutoff safe.

## 2026-05-05 Summarize Bitswap Extra Blocks

Hypothesis:
Priority-1 multi-want and prefetch experiments need a cheap way to see whether
Bitswap responses are returning verified blocks beyond the CID that unblocked
the request. Retrieval already traces `extra_blocks`, but the live harness did
not summarize it, making the signal easy to miss during longer runs.

Implementation:

- Add `trace_summary.bitswap_extra_blocks` to `mobile-web-harness` JSON output.
- Print `events`, `total`, `max`, and delivery split
  `incoming`/`outgoing`/`unknown` in the console trace summary.
- Include `extra_blocks` in slow-event details.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
git diff --check
```

Live smoke:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-extra-blocks-trace.jsonl \
  --output /tmp/vitalik-extra-blocks.json
```

Result: passed `1/1`, root TTFB `696ms`, RSS/FD `37760KiB`/`21`. The trace
summary reported `bitswap deliveries: incoming=2` and
`bitswap extra blocks: events=2 total=0 max=0 incoming=0 outgoing=0 unknown=0`.

Decision: keep. This is diagnostics-only, but it gives future multi-want,
locality prefetch, and extra-block caching experiments a first-class harness
metric instead of requiring ad hoc trace parsing.

Follow-up coverage:

- Existing traces showed that extra blocks can be a real signal:
  `/tmp/ipfs-tech-stream-read6-r3-trace.jsonl` had `events=105 total=13
  max=5 incoming=13`, while `/tmp/ipfs-tech-stream-read4-r3-trace.jsonl` had
  `events=105 total=58 max=3 incoming=58`.
- Add deterministic retrieval coverage that a verified extra Bitswap payload
  block is stored and served as a cache hit on the next request:
  `bitswap_fetch_caches_verified_extra_blocks`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib bitswap_fetch_caches_verified_extra_blocks
cargo test -p freedom-ipfs-retrieval --lib fetches_block_from_local_bitswap_peer
cargo test -p freedom-ipfs-retrieval --lib
git diff --check
```

Current same-window Kubo comparison with the extra-block summary:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-extra-blocks-current-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-extra-blocks-current-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root TTFB p50/p95 was
`935/3654ms` versus Kubo `4585/5191ms`; Rust asset p50/p95 was `158/891ms`
versus Kubo `214/683ms`. Rust stayed much lighter at max RSS/FD
`50664KiB`/`49` versus Kubo `323272KiB`/`537`.

Trace summary: `bitswap deliveries: incoming=105`; `bitswap extra blocks:
events=105 total=27 max=3 incoming=27 outgoing=0 unknown=0`;
`bitswap_session_shortcut_hits=85`; `peer_attempt_starts=341`;
`outgoing_completed=0`; inbound `blocks=141 bytes=2622475
max_oldest_pending_ms=1748`. This keeps pointing at inbound Bitswap delivery
and verified extra blocks as the practical optimization surface, rather than
completed outgoing stream reads.

Priority-1 multi-want building block:

- Add `multi_want_stream_fetches_multiple_blocks_from_local_peer`, which opens
  a real local libp2p Bitswap stream, sends one WANT_BLOCK message containing
  two CIDs, receives both verified payload blocks, and confirms the client sends
  one cancel message covering both CIDs.
- This does not change live page behavior yet. It establishes deterministic
  loopback coverage for the existing multi-want stream machinery before wiring
  any UnixFS or page-session batching into retrieval.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib multi_want_stream_fetches_multiple_blocks_from_local_peer
cargo test -p freedom-ipfs-retrieval --lib
git diff --check
```

Rejected shared-client `fetch_many` refactor:

- Attempted to refactor the shared Bitswap client around an internal
  `fetch_many` path while keeping the existing single-CID `fetch` API as a
  wrapper.
- Added a deterministic local test proving the shared client could fetch two
  CIDs through one local peer request.
- Focused retrieval tests, full retrieval lib tests, gateway tests, `cargo
  check`, `cargo clippy`, and a `vitalik-root-html-range` live smoke all passed.
- Rejected anyway because same-window `ipfs.tech` evidence showed either failed
  root loads or much worse asset latency than the current `f81d1f7` baseline.

Validation that passed before rejection:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib shared_bitswap_client_fetches_multiple_blocks_in_one_request
cargo test -p freedom-ipfs-retrieval --lib fetches_block_from_local_bitswap_peer
cargo test -p freedom-ipfs-retrieval --lib dropped_bitswap_fetch_cancels_open_peer_stream
cargo test -p freedom-ipfs-retrieval --lib want_have_probe_falls_back_to_want_block_quickly
cargo test -p freedom-ipfs-retrieval --lib shared_bitswap_client_handles_repeated_block_fetches
cargo test -p freedom-ipfs-retrieval --lib
cargo check -p freedom-ipfs-retrieval --all-targets
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo build -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-gateway
```

Passing smoke from the rejected branch:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-fetch-many-smoke-trace.jsonl \
  --output /tmp/vitalik-fetch-many-smoke.json
```

Result: passed `1/1`, root TTFB `1488ms`, RSS/FD `38784KiB`/`28`. Trace
summary showed `bitswap_fetch=2`, `bitswap deliveries: incoming=2`, and no
request timeout. This proved the branch could still handle a narrow live
gateway case, but it was insufficient because the broader `ipfs.tech` comparison
regressed.

Rejected live comparison:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-fetch-many-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-fetch-many-r3.json
```

Result: Rust failed `0/3` while Kubo passed `3/3`. The Rust root requests
timed out with 504s, and the trace showed Bitswap request timeouts.

Recheck:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 2 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-fetch-many-r2-recheck-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-fetch-many-r2-recheck.json
```

Result: Rust and Kubo both passed `2/2`, but Rust asset p95 was `4842ms` with
max `9897ms`, versus Kubo asset p95 `562ms`. The trace showed 11 Bitswap
request timeouts and trusted provider failures.

Baseline from `f81d1f7` in `/tmp/freedom-ipfs-pre-fetch-many`:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 2 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-pre-fetch-many-f81d-r2-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-pre-fetch-many-f81d-r2.json
```

Result: Rust and Kubo both passed `2/2`. Rust root p50/p95 was `806/930ms`,
asset p50/p95 was `129/269ms`, and max asset latency was `1026ms`; there was no
matching request-timeout cluster.

Decision: keep the stream-level multi-want test as a building block, but reject
the shared-client production `fetch_many` refactor for now. The next attempt
should not alter the current single-CID shared-client scheduling path until it
can preserve same-window `ipfs.tech` root reliability and asset p95.

Experiment: cache recheck after provider lookup:

Hypothesis:

- During page-asset bursts, one request can miss the store, spend time in
  provider lookup or session-peer racing, while another request receives and
  verifies the same block as an extra Bitswap payload.
- A cheap second store lookup after provider lookup, before any provider fetch,
  can avoid an unnecessary network fetch in that window.

Implementation:

- Add a `block_store_get` recheck with `rechecked=true` immediately before
  `fetch_from_providers_with_source`.
- If the recheck hits, return `RetrievalSource::Cache`.
- Extend the harness trace summary with block-store counters:
  `events`, `hits`, `misses`, `rechecks`, `recheck_hits`, and
  `recheck_misses`.
- Add deterministic coverage with a gated delegated routing response: the fetch
  misses the store, starts provider lookup, the test inserts the verified block
  locally, then the provider response is released. The fetch must return from
  cache instead of attempting empty providers.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib rechecks_cache_after_provider_lookup_before_network_fetch
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib
cargo build -p freedom-ipfs-gateway
git diff --check
```

Live regression check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 2 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-cache-recheck-r2-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cache-recheck-r2.json
```

Result: Rust and Kubo both passed `2/2`. Rust root p50/p95 was `722/5480ms`
versus Kubo `3566/8109ms`; Rust asset p50/p95 was `120/1040ms` versus Kubo
`292/745ms`. Rust stayed much lighter at max RSS/FD `51824KiB`/`60` versus Kubo
`339272KiB`/`716`.

Trace summary: `bitswap_fetches=16`, `session_shortcut_hits=55`,
`request_timeouts_with_trusted=1`, `bitswap extra blocks: events=70 total=43
max=4 incoming=43 outgoing=0 unknown=0`, inbound `blocks=125 bytes=2003094
max_oldest_pending_ms=849`. Manual trace count before the harness summary
counter landed showed `rechecked_total=15`, `recheck_hits=0`, and
`recheck_misses=15`.

Decision: keep. This is intentionally conservative: the live run did not hit
the new fast path, but the deterministic test proves the race exists and the
live run shows the added local lookup has negligible overhead. The new harness
counter will show whether future page sessions convert extra-block arrivals
into recheck cache hits.

Rejected experiment: late session-shortcut cache fill:

Hypothesis:

- The `/tmp/ipfs-tech-cache-recheck-r2-trace.jsonl` run showed a recent-peer
  shortcut delivering the root/index child after the `200ms` post-lookup wait
  had already timed out, and the following provider fanout then hit the `4s`
  mixed trusted request timeout.
- Keeping only those timed-out shortcut futures alive in the background, bounded
  by the existing `2s` shortcut timeout, might cache verified late successes for
  nearby requests without increasing foreground wait time.

Prototype:

- Move the timed-out post-lookup shortcut future into a background task.
- Let `fetch_from_recent_bitswap_peers` continue to verify and store successful
  late results.
- Trace `bitswap_session_shortcut_late_cache`.
- Add a deterministic delayed-peer test proving a foreground miss can still
  populate the store from a late shortcut result.
- Add harness counters for late-cache events/hits/misses.

Validation that passed before rejection:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib late_session_shortcut_result_is_cached_after_post_lookup_timeout
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib
cargo build -p freedom-ipfs-gateway
git diff --check
```

Same-window baseline from detached `7c08cc1`:

```sh
git worktree add /tmp/freedom-ipfs-before-late-cache 7c08cc1
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 2 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-before-late-cache-r2-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-before-late-cache-r2.json
```

Baseline result: Rust and Kubo both passed `2/2`. Rust root p50/p95 was
`922/1496ms` versus Kubo `1838/2403ms`; Rust asset p50/p95 was `490/5098ms`
versus Kubo `63/116ms`. Trace totals included `bitswap_fetches=48`,
`request_timeouts_with_trusted=8`, `session_shortcut_hits=30`,
`bitswap extra blocks: events=70 total=34 max=4`, `peer_attempt_starts=524`,
and inbound `blocks=116 bytes=2091038 max_oldest_pending_ms=1664`.

Prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 2 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-late-cache-r2-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-late-cache-r2.json
```

Prototype result: reject. Rust passed only `1/2` while Kubo passed `2/2`.
Rust root p50/p95 was `1448/1695ms` versus Kubo `3870/3980ms`, but Rust asset
p50/p95 regressed to `232/8523ms` versus Kubo `295/737ms`. The trace showed
`bitswap_session_shortcut_late_cache=34` with `29` hits, but mixed trusted
request timeouts worsened to `13`, and one page run failed.

Decision: reject and revert the prototype. The late-cache idea can recover
verified blocks, but keeping those timed-out session futures alive increased
contention enough to worsen the request-timeout tail. A future version would
need tighter gating, such as one late cache fill per page/root or only when no
provider fanout is already in flight for the CID.

## 2026-05-05 Incoming Bitswap Waiter Delivery Diagnostics

Hypothesis:

- Recent live traces showed inbound Bitswap blocks arriving while session
  shortcut/provider races were active, but the trace did not say whether the
  delivered block reached a still-live receiver or was dropped because the
  waiting request had already moved on.
- Counting delivered versus dropped waiters for each inbound block can guide a
  future bounded mitigation without changing current scheduling behavior.

Implementation:

- Extend `bitswap_incoming_block` tracing with:
  - `delivered_waiter_count`
  - `dropped_waiter_count`
- Keep the existing pending-waiter age fields:
  - `pending_waiter_count`
  - `oldest_pending_ms`
  - `newest_pending_ms`
- Extend the mobile web harness trace summary with incoming-block totals:
  `delivered_waiters`, `dropped_waiters`, and `max_dropped_waiters`.

Validation:

```sh
cargo fmt --all --check
git diff --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib
cargo build -p freedom-ipfs-gateway
```

Result: all passed.

Live check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-incoming-waiter-delivery-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-incoming-waiter-delivery-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `1676ms` versus
Kubo `2831ms`. Rust asset p50/p95 was `739/1876ms` versus Kubo `108/245ms`.
Rust stayed lighter at max RSS/FD `53320KiB`/`47` versus Kubo
`220104KiB`/`152`.

Trace summary from
`/tmp/ipfs-tech-incoming-waiter-delivery-r1.json`: `bitswap_fetches=23`,
`with_trusted_peers=22`, `trusted_successes=15`, `untrusted_successes=8`,
`request_timeouts_with_trusted=0`, `session_shortcut_starts=34`,
`session_shortcut_hits=12`, and `block_store rechecks=23` with no recheck hits.
Incoming block summary: `matches=39`, `blocks=41`, `bytes=1247574`,
`delivered_waiters=39`, `dropped_waiters=4`, `max_oldest_pending_ms=1281`,
`max_pending_waiters=2`, and `max_dropped_waiters=1`.

Decision: keep. This is diagnostics-only and records a useful signal for the
next cancellation/cache-fill experiment. The one-run live check confirms that
dropped receivers happen in normal `ipfs.tech` page loading, but the count is
small enough that it does not justify reintroducing the rejected late-cache
prototype without tighter gating.

## 2026-05-05 Comparison-Mode Trace Console Summary

Problem:

- `--compare-kubo` runs already embed the Rust trace summary in the comparison
  JSON, but the console only printed pass rates, latency ratios, and resource
  ratios.
- That made every live A/B iteration require a separate JSON inspection step to
  answer the most important Rust-side questions: cache rechecks, trusted-peer
  timeouts, session shortcut wins, extra blocks, incoming blocks, and trace
  errors.

Implementation:

- Add a concise comparison-mode trace summary for any engine report with a
  `trace_summary`.
- Print the trace path, line/event/phase counts, block-store counters, Bitswap
  session counters, extra-block counters, incoming-block waiter counters, and
  trace-error counts.
- Kubo is silent unless a future Kubo report also has trace data.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
git diff --check
```

Result: all passed.

Live validation, unstable `ipfs.tech` run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-comparison-trace-print-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-comparison-trace-print-r1.json
```

Result: command exited with failure because Rust passed `0/1` while Kubo passed
`1/1`. The new console summary printed immediately and explained the Rust
failure: `bitswap_fetch: bitswap request timed out=2`,
`request_timeouts_with_trusted=2`, and incoming block delivery had only
`matches=1`, `delivered_waiters=1`, `dropped_waiters=0`.

Live validation, smaller passing case:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-comparison-trace-print-r1-trace.jsonl \
  --comparison-output /tmp/vitalik-comparison-trace-print-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `8882ms` versus
Kubo `2917ms`. Rust stayed lighter at max RSS/FD `39168KiB`/`34` versus Kubo
`127696KiB`/`112`. The console printed the trace summary, including
`request_timeouts_with_trusted=1`, incoming `matches=2`, `delivered_waiters=2`,
and `dropped_waiters=0`.

Decision: keep. This is harness-only, does not change gateway behavior, and
shortens the evidence loop for every future Rust/Kubo comparison.

## 2026-05-05 Provider Retry Trace Summary

Motivation:

- The failed comparison in
  `/tmp/ipfs-tech-comparison-trace-print-r1-trace.jsonl` showed the root
  `ipfs.tech` request fetching the small DAG root quickly, then timing out
  twice on `bafkreibnzgajg3gsyn5c4p5e2h7racpy6dy7tnhwe5l4v4vx5e32qmn4bi`.
- Provider refresh returned the same provider set and the same Bitswap peer set:
  `same_provider_set=true`, `same_bitswap_peer_set=true`,
  `request_timeout=true`.
- A tempting change would be to skip same-set request-timeout retries, but older
  passing traces such as `/tmp/ipfs-tech-before-late-cache-r2-trace.jsonl`
  contain same-set request-timeout retries during successful page loads. That
  makes a behavior change too speculative without better measurement.

Implementation:

- Add `provider_retries` to the harness trace summary.
- Count provider refreshes after timeout/failure.
- Count `retry_provider_count` events, same-provider sets, same-Bitswap-peer
  sets, request-timeout retry-count events, and same-Bitswap request-timeout
  retry-count events.
- Count actual retry phases:
  `provider_retry_after_request_timeout`, `provider_retry_after_timeout`, and
  `provider_retry_after_connection_timeout`.
- Print a concise provider-retry line in normal and comparison trace summaries
  only when events exist.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
git diff --check
```

Result: all passed.

Live validation, passing small case:

```sh
cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-provider-retry-summary-r1-trace.jsonl \
  --comparison-output /tmp/vitalik-provider-retry-summary-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `553ms` versus Kubo
`2950ms`; Rust max RSS/FD was `37760KiB`/`21` versus Kubo `128512KiB`/`95`.
No provider retry events occurred, so the console provider-retry line was
correctly omitted. The JSON contains `provider_retries` with zero counts.

Live validation, passing `ipfs.tech` case:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-provider-retry-summary-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-provider-retry-summary-r1.json
```

Result: Rust and Kubo both passed `1/1`. Rust root TTFB was `526ms` versus Kubo
`7417ms`; Rust asset p50/p95 was `128/195ms` versus Kubo `409/868ms`; Rust max
RSS/FD was `48572KiB`/`27` versus Kubo `358192KiB`/`818`. This particular run
had no provider retry events, while the earlier failed run remains the evidence
that the new aggregate is needed.

Decision: keep. This is diagnostics-only and avoids a premature retry-policy
change while making same-provider/same-Bitswap timeout loops visible in future
comparison reports.

## 2026-05-05 Bitswap Dial Rejection Cause Summary

Motivation:

- A repeat `ipfs.tech` comparison with the provider-retry aggregate showed no
  provider-retry loops, but it still produced many `bitswap_dial_rejected`
  trace errors under page asset fan-out.
- A previous blunt global pending-dial cap was already rejected because it
  removed dial rejections but starved concurrent page loads. The next step
  should be better measurement, not another broad cap.

Implementation:

- Add `bitswap_dial_rejections` to the harness trace summary with:
  `events`, `connection_limit`, and `other`.
- Preserve the existing rejected transport breakdown.
- Print one concise comparison summary line:
  `bitswap dial rejections: events=... connection_limit=... other=... transports=...`

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
git diff --check
```

Result: all passed.

Live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-dial-rejection-summary-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-dial-rejection-summary-r3.json
```

Result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was `6677/7210ms`
versus Kubo `2119/4548ms`; Rust asset p50/p95 was `149/1480ms` versus Kubo
`213/553ms`. Rust remained much lighter at max RSS/FD `50908KiB`/`57` versus
Kubo `280840KiB`/`715`.

Trace summary: `provider_retries refresh_timeout=2`, `request_timeout_counts=2`,
`same_bitswap_request_timeouts=1`, `request_timeouts_with_trusted=2`,
incoming `matches=108`, `delivered_waiters=108`, `dropped_waiters=0`, and
`bitswap dial rejections: events=9 connection_limit=9 other=0 transports=tcp=7,
quic=1, ws=1`.

Decision: keep. This is diagnostics-only and gives future dial-pressure
experiments a direct signal for connection-limit churn without replaying the
previously rejected blunt global cap.

## 2026-05-05 Rejected: Drop Waiters After Immediate Dial Rejection

Hypothesis:

- When `swarm.dial` immediately returns a connection-limit error for every
  address scheduled for a peer, the fetch task still waits up to
  `BITSWAP_CONNECTION_READY_TIMEOUT` for that peer's `connection_ready` signal.
- Dropping those connection waiters immediately might fail impossible peer
  attempts faster and reduce page-tail latency without raising connection
  limits or fanout.

Prototype:

- Track immediate dial outcomes per peer inside the shared Bitswap swarm.
- If all scheduled dial attempts for a peer were rejected immediately, remove
  that peer's connection waiters and emit `bitswap_dial_waiters_dropped`.
- Add focused test coverage for the helper: drop waiters when every scheduled
  dial rejects, but keep waiters when at least one dial attempt is accepted.
- Extend the harness summary temporarily with waiter-drop counts.

Validation that passed before rejection:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib drops_waiters_only_when_all_scheduled_dials_reject_immediately
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo build -p freedom-ipfs-gateway
git diff --check
```

Baseline immediately before the prototype:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-dial-rejection-summary-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-dial-rejection-summary-r3.json
```

Baseline result: Rust and Kubo both passed `3/3`. Rust root p50/p95 was
`6677/7210ms`, asset p50/p95 was `149/1480ms`, and max RSS/FD was
`50908KiB`/`57`. Trace summary showed `provider_retries refresh_timeout=2`,
`request_timeouts_with_trusted=2`, and `bitswap dial rejections: events=9
connection_limit=9 other=0`.

Prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3.json
```

Prototype result: reject. Rust and Kubo still passed `3/3`, and Rust root
p50/p95 improved to `1015/1370ms`, but asset p95 regressed badly to `5070ms`.
Connection-limit churn also increased: `bitswap dial rejections: events=34
connection_limit=34 other=0 waiter_drop_events=28 dropped_waiters=28`. The run
still had `request_timeouts_with_trusted=2`.

Decision: reject and revert. Failing fully rejected dial waiters faster removes
some 5s waits, but it appears to re-open those peers for more immediate redial
attempts under page fan-out, increasing connection-limit churn and worsening the
asset tail. A future version needs fair scheduling/backoff for rejected peers,
not immediate waiter removal by itself.

## 2026-05-05 Rejected: Keep Shared Client After Mixed Timeout

Hypothesis:

- Slow `ipfs.tech` root loads are often the large `/index.html` raw block
  hitting the `4s` mixed trusted/provider Bitswap request timeout, then
  succeeding on retry.
- The current timeout path resets the shared Bitswap client. For the short
  mixed-timeout case, keeping the shared client alive might let the retry reuse
  established connections and reduce redial churn.

Prototype:

- Keep resetting the shared Bitswap client after cold or trusted-only request
  timeouts.
- Skip the reset only when `trusted_peer_count > 0` and the candidate set also
  includes untrusted provider peers, which is exactly the
  `BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT` path.
- Add `reset_client` to the `bitswap_request_timeout` trace event during the
  prototype.

Validation that passed before rejection:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib keeps_bitswap_client_after_mixed_trusted_request_timeout
cargo build -p freedom-ipfs-gateway
git diff --check
```

Baseline immediately before this prototype is the same run used above:
`/tmp/ipfs-tech-dial-rejection-summary-r3.json` and
`/tmp/ipfs-tech-dial-rejection-summary-r3-trace.jsonl`. Rust and Kubo both
passed `3/3`; Rust root p50/p95 was `6677/7210ms`, asset p50/p95 was
`149/1480ms`, max RSS/FD was `50908KiB`/`57`, and dial rejections were
`events=9 connection_limit=9`.

Prototype run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-skip-mixed-timeout-reset-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-skip-mixed-timeout-reset-r3.json
```

Prototype result: reject. Rust passed only `2/3` while Kubo passed `3/3`.
Rust root p50/p95 improved to `2452/2580ms`, but reliability regressed and
asset p95 remained poor at `1065ms` versus Kubo `161ms`. Connection-limit churn
also worsened substantially: `bitswap dial rejections: events=72
connection_limit=72 other=0`. The trace still showed
`request_timeouts_with_trusted=2`.

Decision: reject and revert. Resetting the shared client after the mixed
request timeout is still important for reliability. Keeping it alive can improve
root latency in a good window, but it worsens connection-limit pressure and can
turn a passing page session into a failure.

## 2026-05-05 Keep: Summarize Bitswap Timeout Recovery

Goal:

- Before changing retry policy again, make the harness answer whether
  request-timeout provider retries actually recover the timed-out CID.
- Distinguish same-provider retries from refreshed-provider retries, and record
  whether retry success comes from a trusted/session peer or an untrusted
  provider peer.

Implementation:

- Add `bitswap_timeout_recovery` to the mobile web harness trace summary.
- Count:
  - `bitswap_request_timeout_detail` events, including mixed trusted/provider
    timeouts.
  - `bitswap_client_reset` events.
  - `retry_provider_count` with `request_timeout=true`, split into
    same-provider and refreshed-provider retries.
  - the next same-CID `bitswap_fetch` success/failure after each retry start.
  - trusted versus untrusted retry successes and retry success latency.
- Print the aggregate in both normal trace summaries and Rust/Kubo comparison
  summaries.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway
git diff --check
```

Important validation note: rebuild `freedom-ipfs-gateway` before live harness
runs after changing retrieval behavior. `cargo run -p mobile-web-harness`
rebuilds the harness but can still execute an older gateway binary.

Current-source live sanity run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-timeout-recovery-current-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-timeout-recovery-current-r1.json
```

Result: Rust and Kubo both passed `1/1`; Rust root p50/p95 was `2460/2460ms`
versus Kubo `3366/3366ms`, Rust asset p50/p95 was `91/1207ms` versus Kubo
`173/483ms`, and Rust max RSS/FD was `50512KiB`/`46` versus Kubo
`241740KiB`/`272`. This window did not hit request timeouts, so the new timeout
recovery aggregate was correctly omitted.

Current-source timeout run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-timeout-recovery-current-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-timeout-recovery-current-r3.json
```

Result: Rust passed `2/3`; Kubo passed `3/3`. Rust root p50/p95 was
`1992/2715ms` versus Kubo `2187/3628ms`, but Rust asset p50/p95 was
`168/4971ms` versus Kubo `129/415ms`. Rust max RSS/FD stayed low at
`52772KiB`/`56` versus Kubo `278928KiB`/`388`.

Trace summary:

- `bitswap timeout recovery: request_timeouts=15 mixed_trusted=15
  client_resets=8 retry_starts=14 same_provider_retries=14
  refreshed_provider_retries=0 retry_successes=13 trusted_retry_successes=9
  untrusted_retry_successes=4 retry_failures=1 retry_unresolved=0
  retry_success_elapsed=p50=278ms p90=901ms p95=1076ms max=1076ms`
- `provider_retries refresh_timeout=14`, `same_bitswap_request_timeouts=14`.
- `bitswap session request_timeouts_with_trusted=15`.
- `bitswap incoming blocks: matches=108 blocks=202 bytes=2771754
  delivered_waiters=108 dropped_waiters=0`.
- `bitswap dial rejections: events=13 connection_limit=13`.

Decision: keep. This is diagnostics-only and gives the next behavior
experiment a sharper target: same-provider retry usually recovers
mixed-trusted request timeouts, but one retry failure can still fail the page
and asset p95 remains far behind Kubo. The next likely experiment should focus
on reducing repeated same-provider mixed timeouts under page fan-out without
discarding the retry path that succeeds most of the time.

## 2026-05-05 Keep: Count Bitswap Timeout Reset Outcomes

Goal:

- Explain why request timeout counts can exceed `bitswap_client_reset` counts.
- Determine whether concurrent mixed-trusted request timeouts are racing against
  the same stored shared Bitswap client reset.

Implementation:

- Make `reset_shared_bitswap_client()` return whether it actually removed a
  stored shared client.
- Emit `reset_client=true|false` on `bitswap_request_timeout`.
- Extend the harness `bitswap_timeout_recovery` aggregate with:
  - `request_timeout_events`
  - `reset_true`
  - `reset_false`

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib shortens_request_timeout_for_mixed_trusted_bitswap_candidates
cargo build -p freedom-ipfs-gateway
git diff --check
```

Live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-timeout-reset-field-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-timeout-reset-field-r3.json
```

Result: Rust failed `0/3`; Kubo passed `3/3`. Rust root p50/p95 was
`6544/17891ms` versus Kubo `4617/4947ms`, and Rust asset p50/p95 was
`5004/16266ms` versus Kubo `268/643ms`. Rust still used much less memory and
file descriptors: max RSS/FD `58484KiB`/`88` versus Kubo `314780KiB`/`574`.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=88 mixed_trusted=87
  request_timeout_events=88 reset_true=57 reset_false=31 client_resets=57
  retry_starts=53 same_provider_retries=46 refreshed_provider_retries=7
  retry_successes=19 trusted_retry_successes=11 untrusted_retry_successes=8
  retry_failures=34 retry_unresolved=0
  retry_success_elapsed=p50=894ms p90=1654ms p95=1992ms max=1992ms`
- `provider_retries refresh_timeout=53`, `same_bitswap_request_timeouts=46`.
- `bitswap session request_timeouts_with_trusted=87`.
- `bitswap dial rejections: events=90 connection_limit=90`.

Decision: keep. This is diagnostics-only and confirms timeout/reset contention:
about one third of outer request timeout handlers found that another concurrent
timeout had already reset the stored shared client. The bad live window also
shows that repeated reset plus same-provider retry can collapse under asset
fan-out. A behavior experiment should now target retry fan-out and reset
coordination, not the existence of the retry itself.

## 2026-05-05 Rejected: Gate Same-Provider Timeout Retries

Hypothesis:

- Same-provider retry after mixed-trusted request timeout usually helps, but
  many concurrent retries can pressure the shared Bitswap path and connection
  limits.
- A small per-retriever semaphore around same-provider request-timeout retries
  might reduce retry storms without removing the useful retry behavior.

Prototype:

- Add `MAX_BITSWAP_REQUEST_TIMEOUT_RETRIES = 2`.
- Add a shared semaphore to `HttpRetriever`.
- Acquire a permit only around the same-provider
  `provider_retry_after_request_timeout` retry fetch.
- Emit `provider_retry_gate` and summarize `retry_gate_events` plus
  `retry_gate_max_wait_ms` in the harness.

Validation before live rejection:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib shortens_request_timeout_for_mixed_trusted_bitswap_candidates
cargo build -p freedom-ipfs-gateway
git diff --check
```

Prototype live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-request-timeout-retry-gate-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-request-timeout-retry-gate-r3.json
```

Result: reject. Rust failed `0/3`; Kubo passed `3/3`. Rust root p50/p95 was
`2881/5198ms` versus Kubo `2682/2704ms`, and Rust asset p50/p95 was
`218/12694ms` versus Kubo `92/200ms`. Rust max RSS/FD was `54600KiB`/`82`
versus Kubo `204452KiB`/`124`.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=35 mixed_trusted=35
  request_timeout_events=35 reset_true=21 reset_false=14 client_resets=21
  retry_starts=21 same_provider_retries=16 refreshed_provider_retries=5
  retry_gate_events=16 retry_gate_max_wait_ms=4176 retry_successes=7
  trusted_retry_successes=5 untrusted_retry_successes=2 retry_failures=14
  retry_unresolved=0
  retry_success_elapsed=p50=183ms p90=1325ms p95=1325ms max=1325ms`
- `bitswap session request_timeouts_with_trusted=35`.
- `bitswap dial rejections: events=63 connection_limit=63`.

Decision: reject and revert. The gate reduced some retry concurrency, but it
queued retries for up to `4176ms`, did not restore reliability, and left the
asset tail far behind Kubo. A useful fix likely needs smarter peer/provider
choice or reset coordination, not a blunt semaphore around all same-provider
timeout retries.

## 2026-05-05 Rejected: Shared Client Reset Cooldown

Hypothesis:

- Reset contention is real, but skipping all mixed-timeout resets was too broad.
- A short shared-client reset cooldown might preserve the first reset in a
  timeout burst while preventing immediate reset thrash from follow-on timeout
  handlers.

Prototype:

- Add a `1s` request-timeout reset cooldown to `HttpRetriever`.
- Keep the first reset in a burst.
- Suppress resets inside the cooldown window and emit `reset_suppressed`,
  `reset_cooldown_ms`, and `since_last_reset_ms` on `bitswap_request_timeout`.
- Extend the harness timeout-recovery summary with `reset_suppressed`.

Validation before live rejection:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval --lib shortens_request_timeout_for_mixed_trusted_bitswap_candidates
cargo build -p freedom-ipfs-gateway
git diff --check
```

Prototype live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-request-timeout-reset-cooldown-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-request-timeout-reset-cooldown-r3.json
```

Result: reject. Rust failed `0/3`; Kubo passed `3/3`. Rust root p50/p95 was
`30851/30998ms` versus Kubo `1579/2032ms`, and Rust did not successfully reach
the asset phase in this run (`asset_ttfb=n/a`). Rust max RSS/FD stayed low at
`37888KiB`/`19` versus Kubo `175028KiB`/`102`, but this was because the page
loads failed early.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=6 mixed_trusted=0
  request_timeout_events=6 reset_true=6 reset_false=0 reset_suppressed=0
  client_resets=6 retry_starts=3 same_provider_retries=2
  refreshed_provider_retries=1 retry_successes=0 retry_failures=3`
- `bitswap session request_timeouts_with_trusted=0`.

Decision: reject and revert. The run did not exercise the intended mixed-trusted
burst; instead it regressed cold root retrieval badly. A reset cooldown is not a
safe next step without a narrower trigger and stronger evidence that it only
acts after a successful warm/session peer exists.

## 2026-05-05 Rejected: Suppress Connection-Timeout Peers

Hypothesis:

- Bitswap read-timeout peers are temporarily suppressed, but
  connection-ready-timeout peers are only counted for retry decisions.
- Suppressing narrow connection-timeout peer sets with the same short bad-peer
  TTL might reduce repeated dials to slow or unreachable peers without pruning
  broad failures.

Prototype:

- Rename the timeout marking helper to cover failed peer sets.
- Keep existing read-timeout suppression behavior.
- Add `bitswap_peer_connection_timeout` and
  `bitswap_peer_connection_timeout_suppressed` phases.
- Mark narrow connection-timeout peers bad for the existing
  `BAD_BITSWAP_PROVIDER_TTL`.
- Preserve the broad-failure guard so mass connection timeout sets are not
  suppressed.

Validation before live rejection:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval --lib bitswap_tests::single_bitswap_connection_timeout_peer_is_temporarily_suppressed
cargo test -p freedom-ipfs-retrieval --lib bitswap_tests::single_bitswap_timeout_peer_is_temporarily_suppressed
cargo test -p freedom-ipfs-retrieval --lib bitswap_tests::broad_bitswap_timeouts_are_not_mass_suppressed
cargo build -p freedom-ipfs-gateway
git diff --check
```

Prototype live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-connection-timeout-suppression-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-connection-timeout-suppression-r3.json
```

Result: reject. Rust failed `0/3`; Kubo passed `3/3`. Rust root p50/p95 was
`30516/30583ms` versus Kubo `1946/1980ms`; Rust did not reach successful asset
fetches (`asset_ttfb=n/a`). Rust max RSS/FD was `37760KiB`/`21` versus Kubo
`152664KiB`/`111`, again mostly because Rust failed early.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=6 mixed_trusted=0
  request_timeout_events=6 reset_true=6 reset_false=0 client_resets=6
  retry_starts=3 same_provider_retries=2 refreshed_provider_retries=1
  retry_successes=0 retry_failures=3`
- `bitswap session request_timeouts_with_trusted=0`.
- Trace errors were dominated by request-level timeouts and connection errors;
  the prototype did not meaningfully exercise the new per-peer suppression path.

Decision: reject and revert. The live failure shape was cold root request
timeouts, not repeated per-peer connection-ready failures. Suppressing
connection-timeout peers may still be useful later, but this run gives no
evidence that it helps the current `ipfs.tech` failure mode.

## 2026-05-05 Keep: Classify Bitswap Request Timeouts

Goal:

- Make cold root request-timeout failures obvious in the harness summary.
- Avoid manually grepping `bitswap_request_timeout_detail` events to distinguish
  cold, mixed trusted/provider, and trusted-only timeout shapes.

Implementation:

- Extend `bitswap_timeout_recovery` with:
  - `cold_request_timeouts`
  - `trusted_only_request_timeouts`
  - `request_timeout_budgets`
  - `max_request_timeout_peer_count`
- Keep the existing `mixed_trusted_request_timeouts`, retry recovery, and reset
  outcome counters.
- Print the new fields in normal and Rust/Kubo comparison trace summaries.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_bitswap_timeout_recovery
cargo test -p mobile-web-harness
git diff --check
```

Motivation from the immediately preceding rejected live runs:

- `/tmp/ipfs-tech-request-timeout-reset-cooldown-r3.json` showed
  `request_timeout_details=6 mixed_trusted=0`, Rust `0/3`, Kubo `3/3`, and root
  p50/p95 around `30s`.
- `/tmp/ipfs-tech-connection-timeout-suppression-r3.json` showed the same cold
  shape: `request_timeout_details=6 mixed_trusted=0`, Rust `0/3`, Kubo `3/3`,
  and no successful asset fetches.

Decision: keep. This is diagnostics-only. The next live run will now print
whether request timeouts are cold (`trusted_peer_count=0`), mixed
trusted/provider, or trusted-only, and which timeout budget fired (`4000ms` vs
`15000ms`). That prevents the warm-session and cold-root failure modes from
being conflated.

Post-keep live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-timeout-classifier-current-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-timeout-classifier-current-r1.json
```

Result: Rust failed `0/1`; Kubo passed `1/1`. Rust root TTFB was `30663ms`
versus Kubo `2390ms`; Rust did not reach successful asset fetches. The new
summary classified the failure as cold-root, not mixed-session:

- `bitswap timeout recovery: request_timeout_details=2 cold=2 mixed_trusted=0
  trusted_only=0 timeout_ms=15000=2 max_peers=8 request_timeout_events=2
  reset_true=2 reset_false=0 client_resets=2 retry_starts=1
  same_provider_retries=0 refreshed_provider_retries=1 retry_successes=0
  retry_failures=1`

This confirms the next behavior track should address cold provider/peer quality
or cold Bitswap request timeout recovery, separately from the warm mixed-trusted
asset/session path.

## 2026-05-05 Keep: Show Peer Attempts In Comparison Summaries

Goal:

- Make Rust/Kubo comparison runs print the Bitswap peer-attempt aggregate that
  normal single-run summaries already showed.
- Keep cold-root diagnosis visible without manually grepping trace JSONL for
  `bitswap_peer_attempt` and `bitswap_peer_attempt_start` events.

Implementation:

- Reuse one formatter for `bitswap peer attempts`.
- Call it from both normal harness summaries and Rust/Kubo comparison trace
  summaries.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness bitswap_peer_attempt
cargo test -p mobile-web-harness
git diff --check
```

Live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-peer-attempt-comparison-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-peer-attempt-comparison-r1.json
```

Result: Rust passed `1/1`; Kubo passed `1/1`. Rust root TTFB was `1063ms`
versus Kubo `1783ms`; Rust asset TTFB p50/p95 was `143/372ms` versus Kubo
`96/228ms`. Rust max RSS was `49028KiB` versus Kubo `120576KiB`.

The comparison trace summary now includes:

- `bitswap peer attempts: starts=99 outgoing_completed=0 successes=0
  failures=0 connection_timeouts=0 read_timeouts=0 other_failures=0
  prefer_want_have=0`
- `bitswap incoming blocks: matches=35 blocks=46 bytes=793822
  delivered_waiters=35 dropped_waiters=0 max_oldest_pending_ms=269`

Decision: keep. This does not change retrieval behavior, but it makes live
comparison output capture whether a run is dominated by scheduled outgoing
attempts, completed peer-attempt failures, or incoming-session wins before the
outgoing attempts finish.

## 2026-05-05 Keep: Count Timeout Target Modes

Goal:

- Make cold and mixed Bitswap request timeout summaries show how many selected
  peers received full want-block requests versus want-have probes.
- Avoid relying on the long `targets` string in
  `bitswap_request_timeout_detail` to understand whether a timeout batch was
  mostly probes or full block requests.

Implementation:

- Add `want_block_target_count` and `want_have_target_count` to
  `bitswap_request_timeout_detail`.
- Aggregate total and max want-block/want-have timeout targets in the mobile web
  harness.
- Print a separate `bitswap timeout target modes` line only when the new fields
  are present.

Validation:

```sh
cargo test -p freedom-ipfs-retrieval --lib formats_bitswap_peer_timeout_summary
cargo test -p mobile-web-harness trace_summary_counts_bitswap_timeout_recovery
cargo test -p freedom-ipfs-retrieval --lib
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway
```

Live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-timeout-target-modes-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-timeout-target-modes-r3.json
```

Result: Rust passed `1/3`; Kubo passed `3/3`. Rust root TTFB p50/p95 was
`28207/30500ms` versus Kubo `1596/3199ms`. Rust asset TTFB p50/p95 was
`139/5495ms` versus Kubo `114/250ms`. Rust max RSS/FD was `52652KiB`/`67`
versus Kubo `202480KiB`/`141`.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=5 cold=3
  mixed_trusted=2 trusted_only=0 timeout_ms=15000=3, 4000=2 max_peers=16
  request_timeout_events=5 reset_true=4 reset_false=1 client_resets=4
  retry_starts=4 same_provider_retries=3 refreshed_provider_retries=1
  retry_successes=2 trusted_retry_successes=2 untrusted_retry_successes=0
  retry_failures=2 retry_unresolved=0`
- `bitswap timeout target modes: want_block=12 want_have=32
  max_want_block=3 max_want_have=13`
- `bitswap peer attempts: starts=219 outgoing_completed=26 successes=0
  failures=26 connection_timeouts=24 read_timeouts=2 other_failures=0
  prefer_want_have=18`
- `bitswap dial rejections: events=34 connection_limit=34 other=0
  transports=tcp=29, quic=4, ws=1`

Decision: keep. The failure shape is more specific now: selected timeout
batches are mostly want-have probes, but the completed outgoing attempts are
overwhelmingly connection timeouts. The next behavior experiment should focus
on cold provider/peer quality and connection-slot pressure, not on raising
request timeout budgets.

## 2026-05-05 Reject: Drop Waiters After Rejected Dials

Experiment: when every newly scheduled dial for a peer is rejected immediately
or all pending dials for a peer later fail, drop that peer's connection waiters
instead of letting the request sit for the full `BITSWAP_CONNECTION_READY_TIMEOUT`.

Rationale:

- The preceding run showed `bitswap dial rejections: events=34
  connection_limit=34`.
- Completed outgoing peer attempts were overwhelmingly connection timeouts.
- If a dial cannot possibly become ready, waiting `5s` on its waiter only adds
  latency and holds the request open.

Prototype validation before live runs:

```sh
cargo test -p freedom-ipfs-retrieval --lib drops_waiters_when_scheduled_dials_are_all_rejected
cargo test -p freedom-ipfs-retrieval --lib decrements_pending_dial_counts
cargo test -p freedom-ipfs-retrieval --lib
cargo fmt --all --check
git diff --check
cargo build -p freedom-ipfs-gateway
```

First live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3.json
```

Result: promising but not enough evidence. Rust passed `3/3`; Kubo passed
`3/3`. Rust root TTFB p50/p95 was `648/16687ms` versus Kubo `3701/5376ms`.
Rust asset TTFB p50/p95 was `150/634ms` versus Kubo `207/672ms`. The trace had
one recovered cold timeout, only four dial rejections, and one
`bitswap_connection_waiters_dropped` event that dropped four waiters
immediately.

Second live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-drop-rejected-dial-waiters-r3b.json
```

Result: reject. Rust passed `2/3`; Kubo passed `3/3`. Rust root TTFB p50/p95
was `1101/15732ms` versus Kubo `1914/4518ms`; Rust asset TTFB p95 regressed to
`16310ms` versus Kubo `979ms`.

The second trace showed the failure mode clearly:

- `bitswap timeout recovery: request_timeout_details=30 cold=1
  mixed_trusted=29 trusted_only=0 timeout_ms=4000=29, 15000=1`
- `bitswap peer attempts: starts=581 outgoing_completed=88 successes=0
  failures=88 connection_timeouts=8 read_timeouts=0 other_failures=80`
- `bitswap dial rejections: events=37 connection_limit=37 other=0`

Decision: reject and revert. Dropping waiters converts connection pressure into
fast `connection_waiter_dropped` failures, but under asset load that caused many
mixed trusted/provider retries and did not eliminate failures. A better version
would need queueing/backpressure or peer selection changes, not immediate waiter
failure.

## 2026-05-05 Reject: Suppress Completed Unusable Peer Attempts

Experiment: add a narrow `unusable_peers` lane to `BitswapPeerFailures` and
temporarily suppress peers whose completed outgoing Bitswap attempt failed with
clear unusable-peer strings such as protocol negotiation failure, connection
refused, or connection reset. Keep the existing broad-failure guard so a whole
provider set is not mass-suppressed.

Rationale:

- Rejected waiter-drop runs showed repeated protocol negotiation and connection
  refused errors for the same public peers.
- Current suppression only marks read-timeout peers. Protocol failures can be
  retried repeatedly across warm asset requests.

Prototype validation before live runs:

```sh
cargo test -p freedom-ipfs-retrieval --lib single_unusable_bitswap_peer_is_temporarily_suppressed
cargo test -p freedom-ipfs-retrieval --lib broad_unusable_bitswap_peers_are_not_mass_suppressed
cargo test -p freedom-ipfs-retrieval --lib classifies_suppressible_bitswap_peer_failures
cargo test -p freedom-ipfs-retrieval --lib
cargo fmt --all --check
git diff --check
cargo build -p freedom-ipfs-gateway
```

First live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-unusable-peer-suppression-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-unusable-peer-suppression-r3.json
```

Result: Rust passed `3/3`; Kubo passed `3/3`. Rust root TTFB p50/p95 was
`1029/19986ms` versus Kubo `4693/4768ms`; Rust asset TTFB p50/p95 was
`134/331ms` versus Kubo `210/519ms`. But the trace did not exercise the new
path: `unusable_peer_count=0`, no `bitswap_peer_unusable` events, and the only
completed peer-attempt failures were connection timeouts.

Second live run:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-unusable-peer-suppression-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-unusable-peer-suppression-r3b.json
```

Result: reject. Rust failed `0/3`; Kubo passed `3/3`. Rust root TTFB p50/p95
was `11484/30640ms` versus Kubo `1646/1680ms`.

Trace summary:

- `bitswap timeout recovery: request_timeout_details=16 cold=2
  mixed_trusted=14 trusted_only=0 timeout_ms=4000=14, 15000=2`
- `bitswap peer attempts: starts=157 outgoing_completed=7 successes=0
  failures=7 connection_timeouts=7 read_timeouts=0 other_failures=0`
- Trace errors still showed repeated protocol negotiation/connection refused
  failures, but they surfaced as `bitswap_connection_error` swarm events and
  then request-level connection timeouts, not as completed `Other` peer attempts.

Decision: reject and revert. The hypothesis targeted the wrong layer. Repeated
protocol negotiation failures need to be tracked from swarm dial/connection
events or addressed with peer selection/backpressure, not by suppressing
completed outgoing Bitswap attempts that rarely materialize for this failure
shape.

## 2026-05-05 Keep: Summarize Bitswap Connection Errors

Goal:

- Move repeated `bitswap_connection_error` details out of the long
  `trace_errors` string and into a compact aggregate by failure class and peer.
- Make the next peer-selection/backpressure experiment measurable without
  manually grepping trace JSONL.

Implementation:

- Add `bitswap connection errors` to normal and Rust/Kubo comparison trace
  summaries.
- Count total events, with-peer versus without-peer events, top peers, and
  classes:
  - `protocol_negotiation_failed`
  - `connection_refused`
  - `connection_reset`
  - `no_route_to_host`
  - `timeout`
  - `other`

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p mobile-web-harness
git diff --check
```

Live validation:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-connection-error-summary-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-connection-error-summary-r1.json
```

Result: Rust failed `0/1`; Kubo passed `1/1`. Rust root TTFB was `6409ms`
versus Kubo `3115ms`; Rust asset TTFB p95 was `8343ms` versus Kubo `241ms`.

The new summary line exposed the connection failure shape directly:

- `bitswap connection errors: events=20 with_peer=20 without_peer=0
  classes=protocol_negotiation_failed=6, timeout=5, connection_refused=3,
  connection_reset=3, no_route_to_host=2, other=1`
- Top peer:
  `12D3KooWDpp7U7W9Q8feMZPPEpPP5FKXTUakLgnVLbavfjb9mzrT=6`

Decision: keep. This is diagnostics-only, but it identifies repeated
connection-level failures by peer and class. The next behavior work should use
this signal for bounded peer backoff or smarter candidate selection, instead of
trying to infer repeated bad peers from request timeouts alone.

## 2026-05-05 Keep: Back Off Repeated Connection-Error Peers

Goal:

- Avoid repeatedly scheduling public peers that have just produced concrete
  connection-level failures inside the same shared Bitswap swarm.
- Keep the response bounded and mobile-safe: no higher connection limits, no
  public gateway fallback, no unbounded peer blacklist.

Implementation:

- Add an in-memory, per-swarm backoff for peers that hit repeated concrete
  connection errors:
  - protocol negotiation failure
  - connection refused
  - connection reset
  - no route to host
- Ignore generic connection timeout errors for this backoff path, since earlier
  connection-timeout suppression did not help.
- Threshold: `2` matching concrete errors for the same peer/class.
- TTL: `30s`.
- While active, skip that peer for new Bitswap commands in the same swarm and
  emit `bitswap_connection_error_peer_skipped`.
- Emit `bitswap_connection_error_backoff` when a peer enters backoff.
- Extend the harness summary with `bitswap connection backoff`.

Validation:

```sh
cargo test -p freedom-ipfs-retrieval --lib connection_error
cargo test -p freedom-ipfs-retrieval --lib
cargo fmt --all --check
cargo test -p mobile-web-harness
git diff --check
cargo build -p freedom-ipfs-gateway
```

Live validation 1:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-connection-error-backoff-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-connection-error-backoff-r3.json
```

Result: Rust passed `3/3`; Kubo passed `3/3`. Rust root TTFB p50/p95 was
`1994/2767ms` versus Kubo `3091/3645ms`. Rust asset TTFB p50/p95 was
`161/821ms` versus Kubo `209/1125ms`. This run did not exercise backoff; no
peer hit the repeated-error threshold inside a single fresh gateway process.

Live validation 2:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-connection-error-backoff-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-connection-error-backoff-r3b.json
```

Result: Rust passed `3/3`; Kubo passed `3/3`. Rust root TTFB p50/p95 was
`1187/1848ms` versus Kubo `3704/5033ms`. Rust asset TTFB p50/p95 was
`142/886ms` versus Kubo `178/451ms`.

Backoff evidence:

- `bitswap_connection_error_backoff` fired once for
  `12D3KooWCqqNtp7WKk3eQfsN7o3VRmUtPwaq8YpS4LUKc9tbzM7P`
  with `error_class=connection_reset`.
- The same peer was skipped for two later CIDs.

Live validation 3:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --kubo-bin /root/codex/freedom-ipfs/target/tools/kubo/kubo/ipfs \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-connection-error-backoff-r3c-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-connection-error-backoff-r3c.json
```

Result: Rust passed `3/3`; Kubo passed `3/3`. Rust root TTFB p50/p95 was
`993/1014ms` versus Kubo `3736/4347ms`. Rust asset TTFB p50/p95 was
`147/1094ms` versus Kubo `224/561ms`.

Backoff evidence:

- `bitswap_connection_error_backoff` fired for
  `12D3KooWF1vFVwEbAqHMnXPnKVJmjT5Ncj39zZkZPk87KCBvTFfo`
  with `error_class=connection_refused`.
- That peer was skipped for five later CIDs.
- A second peer,
  `12D3KooWCqqNtp7WKk3eQfsN7o3VRmUtPwaq8YpS4LUKc9tbzM7P`,
  also entered backoff with `error_class=connection_reset`.

Decision: keep. Across three `repeat=3` samples Rust passed `9/9` while Kubo
passed `9/9`, and two samples exercised the new path. The asset p95 can still
lose to Kubo in some passing runs, but the backoff is bounded, in-memory,
resource-neutral, and prevents repeatedly scheduling peers that have just
proven unusable at the connection layer.

## 2026-05-05 Mobile Progress Snapshot API

Goal:

- Start the parallel mobile progress/event API track without changing retrieval
  behavior or adding callback ABI complexity.
- Let Swift poll a bounded per-request JSON snapshot while real local-gateway
  requests load through `WKURLSchemeHandler`.

Implementation:

- Add a mobile progress recorder backed by a `tracing_subscriber` layer in
  `freedom-ipfs-mobile`.
- Reuse existing structured phases emitted by the gateway, UnixFS, retrieval,
  provider lookup, Bitswap, routing, and name-system paths.
- Record explicit mobile preload `started`, `completed`, `failed`, and
  `cancelled` events.
- Bound recent event history to `512` events and keep an active-target map for
  currently loading requests.
- Carry optional gateway correlation headers into progress events:
  `X-Freedom-Request-ID`, `X-Freedom-Parent-Request-ID`, and
  `X-Freedom-Top-Level-Path`.
- Expose:
  - `freedom_ipfs_node_progress_snapshot_json(node)`
  - `freedom_ipfs_node_clear_progress(node)`
  - Swift `FreedomIpfsReader.progressSnapshotJSON`
  - Swift `FreedomIpfsReader.clearProgress()`
- Add `docs/mobile-progress-api.md` with JSON shape and suggested Swift UI
  mapping.
- Include snapshot-level `generated_at_unix_ms`, `active_count`, and
  `event_count` metadata so Swift can cheaply inspect polling freshness and
  bounded history size.
- Update `FetchingBlockProvider` to emit `block_store_get` for direct store
  hits/misses, so cache activity is visible to the same progress recorder.
- Update the XCFramework verifier to require the new C exports and make the
  generated Swift smoke parse a progress snapshot after a gateway request.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-mobile progress_snapshot_records_gateway_request_phases
cargo test -p freedom-ipfs-mobile
cargo test -p freedom-ipfs-gateway --lib
cargo test -p freedom-ipfs-retrieval --lib bitswap_fetch_caches_verified_extra_blocks
cargo test -p freedom-ipfs-retrieval --lib
cargo test -p freedom-ipfs-gateway
cargo check -p xtask --all-targets
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Result: validation passed. This is an API/diagnostics change; it does not
change provider selection, Bitswap fanout, timeout budgets, block verification,
public gateway fallback, or mobile resource caps.

Known follow-up:

- Add richer page-load grouping semantics on top of the current request-header
  correlation if Swift needs more than numeric parent/top-level IDs.

## 2026-05-05 Harness Progress Phase Summary

Goal:

- Make long-running live harness runs show whether a page is spending time in
  user-visible progress states, without manually grepping raw gateway JSONL.
- Keep this diagnostics-only and reuse existing trace events; do not change the
  gateway retrieval path.

Implementation:

- Extend `TraceSummary` with `progress_phases`.
- Derive progress phases from existing trace events using the same UI-oriented
  vocabulary as the mobile progress API: `queued`, `started`,
  `resolving_name`, `name_resolved`, `checking_cache`, `cache_hit`,
  `provider_lookup`, `providers_found`, `provider_diversity_low`,
  `dht_fallback_started`, `fetching_bitswap`, `fetching_http_provider`,
  `streaming`, `retrying`, `completed`, `cancelled`, and `failed`.
- Print `progress phases: ...` in normal and Rust-vs-Kubo comparison trace
  summaries when `--trace-output` is enabled.
- This harness summary does not poll the mobile FFI snapshot because the current
  harness usually drives an out-of-process CLI gateway, not an in-process mobile
  node.
- Tighten the mobile progress mapper for name cache/resolution, provider cache,
  Bitswap peer expansion/dial plans, retry/backoff events, block-source totals,
  Bitswap DNS/address/connection events, and UnixFS/MIME response work so Swift
  sees stable UI phases instead of raw trace names for common page-load events.
- Update `docs/mobile-progress-api.md`, `docs/mobile-web-readiness/README.md`,
  and the top-level README with the new harness summary and stable phase list.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p freedom-ipfs-mobile
cargo test -p freedom-ipfs-gateway --lib
cargo test -p freedom-ipfs-retrieval --lib
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
cargo build -p freedom-ipfs-gateway
cargo run -p mobile-web-harness -- --case vitalik-root-html-range --repeat 1 \
  --fresh-gateway-per-run --asset-concurrency 6 \
  --trace-output /tmp/vitalik-progress-summary-trace-2.jsonl \
  --output /tmp/vitalik-progress-summary-2.json
```

Result: validation passed. The live `vitalik-root-html-range` smoke passed
`1/1`; after folding common Bitswap DNS/address/connection events into stable
progress states, the console printed:

```text
progress phases: fetching_bitswap=20, provider_lookup=13, streaming=9,
checking_cache=5, cache_hit=3, providers_found=2, completed=1, queued=1
```

## 2026-05-05 Offline Replay Harness Mode

Goal:

- Start the cache-completeness track from the roadmap with a harness mode that
  proves what a warmed page can replay when the node is restarted without
  routing.
- Keep this as a measurement tool only; do not change gateway/retrieval
  behavior.

Implementation:

- Add `--offline-replay` to `mobile-web-harness`.
- The mode runs the selected corpus once through an online Rust gateway backed
  by a persistent SQLite DB, then restarts the same DB with
  `--routing-mode offline` and replays the corpus.
- If `--gateway-db` is omitted, the harness creates and reports a temporary DB
  path under `/tmp`.
- The JSON output is an `OfflineReplayReport` containing:
  - `online: RunReport`
  - `offline: RunReport`
  - `summary.missing_url_count`
  - `summary.missing_urls[]` with root/asset kind, case id, URL, and failures
  - `summary.offline_storage_bytes`
  - `summary.offline_request_statuses`
  - `summary.offline_trace_errors`
  - `summary.offline_progress_phases`
- If `--trace-output /tmp/replay.jsonl` is passed, online/offline traces are
  split into `/tmp/replay-online.jsonl` and `/tmp/replay-offline.jsonl`.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness offline_replay
cargo test -p mobile-web-harness labeled_trace_output
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
rm -f /tmp/freedom-ipfs-offline-replay-vitalik.db \
  /tmp/freedom-ipfs-offline-replay-vitalik.db-* \
  /tmp/vitalik-offline-replay*.json \
  /tmp/vitalik-offline-replay*.jsonl
cargo run -p mobile-web-harness -- --case vitalik-root-html-range \
  --repeat 1 --asset-concurrency 6 \
  --gateway-db /tmp/freedom-ipfs-offline-replay-vitalik.db \
  --offline-replay \
  --trace-output /tmp/vitalik-offline-replay-trace.jsonl \
  --output /tmp/vitalik-offline-replay.json
```

Result: validation passed. The live replay reported online `1/1`, offline
`1/1`, `missing_urls=0`, and `offline_storage_bytes=4096`. Evidence files:

- `/tmp/vitalik-offline-replay.json`
- `/tmp/vitalik-offline-replay-trace-online.jsonl` (`55` lines)
- `/tmp/vitalik-offline-replay-trace-offline.jsonl` (`10` lines)
- `/tmp/freedom-ipfs-offline-replay-vitalik.db`

The offline trace showed no Bitswap/provider activity and only cache-backed
gateway/UnixFS work, with progress phases:

```text
streaming=7, completed=1, queued=1, started=1
```

Follow-up live IPNS/DNSLink replay:

```sh
rm -f /tmp/freedom-ipfs-offline-replay-ipfs-tech.db \
  /tmp/freedom-ipfs-offline-replay-ipfs-tech.db-* \
  /tmp/ipfs-tech-offline-replay*.json \
  /tmp/ipfs-tech-offline-replay*.jsonl
cargo run -p mobile-web-harness -- --case ipfs-tech-page-assets \
  --repeat 1 --asset-concurrency 6 --run-timeout-secs 120 \
  --gateway-db /tmp/freedom-ipfs-offline-replay-ipfs-tech.db \
  --offline-replay \
  --trace-output /tmp/ipfs-tech-offline-replay-trace.jsonl \
  --output /tmp/ipfs-tech-offline-replay.json
```

Result: online passed `1/1`, offline failed `0/1`, `missing_urls=1`. The
missing URL was the root `/ipns/ipfs.tech/`, with status `404`. The online trace
resolved `ipfs.tech` to
`/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq`; the
offline trace then failed at `name_resolve` with
`dnslink record not found for ipfs.tech` before any block lookup. Evidence:

- `/tmp/ipfs-tech-offline-replay.json`
- `/tmp/ipfs-tech-offline-replay-trace-online.jsonl` (`931` lines)
- `/tmp/ipfs-tech-offline-replay-trace-offline.jsonl` (`4` lines)

Conclusion: after warming, immutable `/ipfs` replay works for the tested range
case, but an `/ipns`/DNSLink URL does not currently replay offline after a
process restart because name resolution state is not persisted or rewritten to
the resolved `/ipfs` target. This gives three concrete follow-up options:
persist bounded successful name resolutions, let the host app replay the
resolved `/ipfs` URL offline, or improve the offline `/ipns` error page with the
missing-name cause.

## 2026-05-05 Resolved-IPFS Offline Replay Harness Mode

Hypothesis: the failing offline `ipfs.tech` replay above is a name-resolution
state gap, not a missing-block/cache-completeness gap. Replaying the online
observed `/ipfs` target offline should pass if the warmed blocks are complete.

Implementation:

- Add `--offline-replay-resolved-ipfs` to `mobile-web-harness`.
- In offline replay mode, the online pass always has a trace path when this flag
  is set, even if the caller did not request `--trace-output`.
- Successful online `name_resolve` events with `/ipfs/...` `resolved_target`
  values are used to rewrite selected offline `/ipns/{name}/...` corpus paths.
- The JSON report records `resolved_ipfs_replay` and
  `resolved_ipfs_rewrites[]` with case id, original path, resolved target, and
  rewritten path.
- This is diagnostics-only; it does not persist DNSLink/IPNS records or change
  gateway behavior.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
rm -f /tmp/freedom-ipfs-offline-replay-ipfs-tech-resolved-selected.db \
  /tmp/freedom-ipfs-offline-replay-ipfs-tech-resolved-selected.db-* \
  /tmp/ipfs-tech-offline-replay-resolved-selected*.json \
  /tmp/ipfs-tech-offline-replay-resolved-selected*.jsonl
cargo run -p mobile-web-harness -- --case ipfs-tech-page-assets \
  --repeat 1 --asset-concurrency 6 --run-timeout-secs 120 \
  --gateway-db /tmp/freedom-ipfs-offline-replay-ipfs-tech-resolved-selected.db \
  --offline-replay --offline-replay-resolved-ipfs \
  --trace-output /tmp/ipfs-tech-offline-replay-resolved-selected-trace.jsonl \
  --output /tmp/ipfs-tech-offline-replay-resolved-selected.json
```

Result: validation passed. The live run reported online `1/1`, offline `1/1`,
`missing_urls=0`, and one selected rewrite:

```text
/ipns/ipfs.tech/ ->
/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/
```

Offline replay statuses were `200=27, 206=6`; offline progress phases were
`streaming=199, completed=33, queued=33, started=33`; offline trace errors were
empty. Evidence:

- `/tmp/ipfs-tech-offline-replay-resolved-selected.json`
- `/tmp/ipfs-tech-offline-replay-resolved-selected-trace-online.jsonl` (`926`
  lines)
- `/tmp/ipfs-tech-offline-replay-resolved-selected-trace-offline.jsonl` (`298`
  lines)
- `/tmp/freedom-ipfs-offline-replay-ipfs-tech-resolved-selected.db`

Conclusion: the current warmed `ipfs.tech` page-assets cache is complete enough
to replay via immutable `/ipfs` paths after a restart. The remaining offline
failure for the original `/ipns/ipfs.tech/` URL is specifically the lack of
offline name-resolution state or host-side rewrite policy.

## 2026-05-05 Persistent Name Cache For Offline IPNS Replay

Hypothesis: if successful DNSLink/IPNS resolutions are persisted with bounded
TTL, a warmed `/ipns/...` page can replay offline after a process restart
without rewriting the URL to `/ipfs/...`.

Implementation:

- Add a bounded `name_cache` table to the SQLite store:
  - `name`
  - `resolved_target`
  - `expires_at`
  - `updated_at`
- Store at most 128 name records and prune expired records on insert/read.
- Add `PersistentNameResolver` in the gateway crate. Online gateways wrap the
  normal resolver with it, so successful `/ipfs/...` or `/ipns/...` resolutions
  are persisted with `min(upstream_ttl, 1h)`.
- Offline CLI and mobile gateways use `PersistentNameResolver::cache_only`, so
  they can resolve still-valid names from SQLite but do not perform network
  name resolution.
- Map `name_persistent_cache` trace events to mobile/harness progress phase
  `name_resolved` on cache hit and `resolving_name` on miss.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-store
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway
rm -f /tmp/freedom-ipfs-offline-replay-ipfs-tech-persistent-name.db \
  /tmp/freedom-ipfs-offline-replay-ipfs-tech-persistent-name.db-* \
  /tmp/ipfs-tech-offline-replay-persistent-name*.json \
  /tmp/ipfs-tech-offline-replay-persistent-name*.jsonl
cargo run -p mobile-web-harness -- --case ipfs-tech-page-assets \
  --repeat 1 --asset-concurrency 6 --run-timeout-secs 120 \
  --gateway-db /tmp/freedom-ipfs-offline-replay-ipfs-tech-persistent-name.db \
  --offline-replay \
  --trace-output /tmp/ipfs-tech-offline-replay-persistent-name-trace.jsonl \
  --output /tmp/ipfs-tech-offline-replay-persistent-name.json
```

Result: validation passed. The original `/ipns/ipfs.tech/` offline replay now
passed without the resolved-IPFS rewrite: online `1/1`, offline `1/1`,
`missing_urls=0`. Offline statuses were `200=27, 206=6`; offline progress
phases were `streaming=199, name_resolved=66, completed=33, queued=33,
started=33`; offline trace errors were empty. The offline trace showed
`name_persistent_cache` hits for `ipfs.tech` and zero provider/Bitswap/HTTP
provider fetch phases:

```text
provider_lookup=0
bitswap_fetch=0
http_provider_fetch=0
provider_cache=0
block_fetch_total=0
```

Evidence:

- `/tmp/ipfs-tech-offline-replay-persistent-name.json`
- `/tmp/ipfs-tech-offline-replay-persistent-name-trace-online.jsonl` (`907`
  lines)
- `/tmp/ipfs-tech-offline-replay-persistent-name-trace-offline.jsonl` (`364`
  lines)
- `/tmp/freedom-ipfs-offline-replay-ipfs-tech-persistent-name.db`

Conclusion: immediate offline replay for warmed DNSLink/IPNS pages now works
for the original URL while the resolved name record is TTL-valid. This keeps the
node read-only and cache-only offline; the remaining product decision is how
strictly the app should treat expired mutable-name records versus offering a
host-side "last resolved immutable path" replay affordance.

## 2026-05-05 Browser Cache Validators

Hypothesis: WebKit can avoid unnecessary local-gateway reads on repeated loads
if immutable content carries stable validators and mutable name paths are cheap
to revalidate.

Implementation:

- Add stable file ETags derived from root CID, resolved UnixFS file path, and
  file length.
- Add `Cache-Control: public, max-age=31536000, immutable` for original
  `/ipfs/...` file responses.
- Add `Cache-Control: no-cache` for original `/ipns/...` file responses so the
  browser revalidates mutable names rather than treating them as immutable.
- Return `304 Not Modified` for matching non-range `If-None-Match` requests
  before MIME sniffing or streaming file bytes.
- Keep range semantics intact: range requests still return `206` with `ETag`,
  `Cache-Control`, `Accept-Ranges`, `Content-Range`, and the requested slice.
- Map the new `gateway_conditional` trace phase to mobile/harness progress
  `cache_hit`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result: validation passed. Deterministic gateway tests cover:

- `/ipfs` `ETag` and immutable `Cache-Control`
- matching `If-None-Match` returning `304 Not Modified`
- `/ipns` file responses using revalidation cache policy
- weak `If-None-Match` matching for `/ipns`
- range requests retaining `206`, `Content-Range`, `ETag`, and
  `Cache-Control` while ignoring `If-None-Match`

Conclusion: this is a low-risk warm-path and browser-compatibility improvement.
It does not change retrieval, routing, or block verification; it only lets a
browser avoid asking the Rust node to stream bytes again when its local cached
copy is still valid.

## 2026-05-05 Conditional Revalidation Harness Mode

Hypothesis: the gateway `ETag`/`304` behavior needs a black-box harness path so
future live page runs can prove browser-cache revalidation remains cheap for
real `/ipfs` and `/ipns` resources.

Implementation:

- Add `--conditional-revalidate` to `mobile-web-harness`.
- Capture `ETag` and `Cache-Control` on root and crawl asset responses.
- For each successful non-range `GET`, require an `ETag` and issue a second
  `GET` with `If-None-Match`.
- Record each revalidation as JSON on the root/asset result:
  `status`, `etag`, `cache_control`, `body_bytes`, `ttfb_ms`, `total_ms`,
  `passed`, and `failures`.
- Aggregate root and asset revalidation attempts, passes, failures, and TTFB
  summaries per case.
- Treat a missing `ETag` or attempted revalidation that does not return an empty
  `304` as a harness failure. Range requests are skipped.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo build -p freedom-ipfs-gateway
timeout 300s cargo run -p mobile-web-harness -- \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --asset-concurrency 6 \
  --conditional-revalidate \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-conditional-revalidate-strict-trace.jsonl \
  --output /tmp/ipfs-tech-conditional-revalidate-strict.json
```

Live result:

- `ipfs-tech-page-assets` passed `1/1`.
- Root TTFB was `943ms`; total run was `2684ms`.
- Root revalidation passed `1/1`, `304`, empty body, TTFB `21ms`.
- Asset revalidation passed `26/26`, all `304`, empty bodies, TTFB p50 `3ms`,
  p90 `5ms`, p95 `12ms`, max `32ms`.
- Trace showed gateway response statuses `200=27`, `304=27`, `206=6`.
- Trace contained `27` `gateway_conditional` events.
- Evidence paths:
  - `/tmp/ipfs-tech-conditional-revalidate-strict.json`
  - `/tmp/ipfs-tech-conditional-revalidate-strict-trace.jsonl`

Conclusion: keep the harness mode. It gives future warm-path and browser-cache
experiments a direct regression signal without changing normal harness behavior.

## 2026-05-05 Current Kubo Comparison And DAICO Corpus Refresh

Hypothesis: after provider retry hardening and cache-validator work, the next
retrieval experiment needs fresh Rust-vs-Kubo evidence. The default corpus also
needs to avoid stale public targets that fail for both engines.

Commands:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-current-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-current-rust-vs-kubo.json

timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-current-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-current-rust-vs-kubo.json
```

Results:

- `ipfs-tech-page-assets`: Rust `3/3`, Kubo `3/3`.
  - Root TTFB p50/p95: Rust `1250/1684ms`, Kubo `1405/2726ms`.
  - Asset TTFB p50/p95: Rust `196/1352ms`, Kubo `106/1339ms`.
  - Max RSS/FD: Rust `52416KiB`/`51`, Kubo `184168KiB`/`112`.
- `vitalik-root-html-range`: Rust `3/3`, Kubo `3/3`.
  - Root TTFB p50/p95: Rust `6664/10458ms`, Kubo `1657/2055ms`.
  - Max RSS/FD: Rust `38400KiB`/`29`, Kubo `188912KiB`/`120`.

Interpretation:

- Rust is now meaningfully faster than Kubo for the current `ipfs.tech` root
  page-load sample and uses far less RSS/FDs.
- Rust still loses badly to Kubo for the `vitalik` root range case. The trace
  shows repeated request-timeout recovery with mixed trusted peers, making this
  a better next session/provider experiment target than `ipfs.tech`.
- Rust `ipfs.tech` asset p50 is still slower than Kubo, while p95 is roughly
  parity. That points at per-asset warm/session scheduling overhead rather than
  root discovery alone.

DAICO corpus check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/daicowtf-current-rust-vs-kubo-trace.jsonl \
  --output /tmp/daicowtf-current-rust-vs-kubo.json
```

The old DAICO CID failed for both engines: Rust `0/3`, Kubo `0/3`. Kubo reported
`found 3 provider(s), attempted 3, but none were reachable`; Rust traces showed
low provider diversity, repeated single-peer Bitswap request timeouts, and no
asset fetches.

The corpus was then refreshed to the newer checked-in `daicowtf.eth` snapshot:

```text
/ipfs/bafybeidznfolm74c5cephzdycedx7hk76iawno45wemcvkflieotzo2lne/
```

Same-window result for the refreshed CID:

- Rust `0/3`, Kubo `0/3`.
- Root TTFB p50/p95: Rust `31603/31660ms`, Kubo `30005/30005ms`.
- Rust trace: `provider_diversity_low=12`, `bitswap request timed out=6`,
  no successful asset fetches.
- Evidence:
  - `/tmp/daicowtf-current-corpus-rust-vs-kubo.json`
  - `/tmp/daicowtf-current-corpus-rust-vs-kubo-trace.jsonl`

Decision:

- Keep the DAICO corpus path refreshed to the current checked-in ENS snapshot,
  but mark the DAICO mobile-web cases `"default_enabled": false` so default
  harness runs do not fail on a public availability window that also breaks
  Kubo.
- Remove `daicowtf-home` from the default opt-in `live-corpus` fixture for now.
- Keep DAICO available as an explicit sparse-provider target with
  `--case daicowtf-page-assets`.

Follow-up: the next behavior experiment should target `vitalik-root-html-range`
or `ipfs.tech` asset p50, not DAICO, unless the goal is specifically
sparse-provider failure handling.

## 2026-05-05 Direct Untrusted WANT_BLOCK Cap Recheck

Hypothesis: the `vitalik-root-html-range` tail is dominated by a mixed-trusted
Bitswap request timeout, followed by a retry that succeeds only after another
provider response arrives. Letting one more untrusted provider receive an
optimistic direct `WANT_BLOCK` before falling back to `WANT_HAVE` may reduce the
post-timeout recovery delay without materially increasing mobile resource use.

Experiment:

- Increase `MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS` from `2` to `3`.
- Keep the existing bounded peer caps and request timeout behavior unchanged.
- Rebuild `freedom-ipfs-gateway` before each live run because the harness starts
  `target/debug/freedom-ipfs-gateway`.

The first cap-3 run at `/tmp/vitalik-direct3-rust-vs-kubo.*` was discarded
because it used a stale gateway binary. The first rebuilt run was consistent
with the second run below:

- Evidence: `/tmp/vitalik-direct3-rebuilt-rust-vs-kubo.json`,
  `/tmp/vitalik-direct3-rebuilt-rust-vs-kubo-trace.jsonl`.
- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `5681/5819ms`, Kubo `1642/2141ms`.
- Retry success elapsed p50/max: Rust `133/133ms`.

Second cap-3 Rust/Kubo sample:

```sh
cargo build -p freedom-ipfs-gateway

timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-direct3-rebuilt-r2-trace.jsonl \
  --output /tmp/vitalik-direct3-rebuilt-r2.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `5707/6033ms`, Kubo `1702/2055ms`.
- Max RSS/FD: Rust `38912KiB`/`28`, Kubo `191032KiB`/`110`.
- Trace: `request_timeout_details=3`, all `mixed_trusted`, all `timeout_ms=4000`.
- Retry recovery: `retry_successes=3`, all `untrusted_retry_successes=3`,
  `retry_success_elapsed=p50=133ms p95=137ms max=137ms`.
- Target modes: `want_block=12`, `want_have=15`, `max_want_block=4`,
  `max_want_have=5`.

Temporary cap-2 recheck in the same network window:

```sh
# Temporarily restore only MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS=2.
cargo build -p freedom-ipfs-gateway

timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-direct2-recheck-trace.jsonl \
  --output /tmp/vitalik-direct2-recheck.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `6394/6597ms`, Kubo `1689/1949ms`.
- Max RSS/FD: Rust `39168KiB`/`28`, Kubo `180252KiB`/`105`.
- Trace: `request_timeout_details=3`, all `mixed_trusted`, all `timeout_ms=4000`.
- Retry recovery: `retry_successes=3`, all `trusted_retry_successes=3`,
  `retry_success_elapsed=p50=880ms p95=1042ms max=1042ms`.
- Target modes: `want_block=9`, `want_have=18`, `max_want_block=3`,
  `max_want_have=6`.

The cap-3 change does not eliminate the initial 4s mixed-trusted timeout, so it
is not the final `vitalik` fix. It does make the bounded retry recover much
faster in this repeatable failure shape, with no observed RSS/FD increase.

Page workload guardrail:

```sh
cargo build -p freedom-ipfs-gateway

timeout 180s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-direct3-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-direct3-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `2852/3428ms`, Kubo `3789/11305ms`.
- Asset TTFB p50/p95: Rust `137/1065ms`, Kubo `190/1250ms`.
- Max RSS/FD: Rust `50716KiB`/`49`, Kubo `298840KiB`/`564`.
- Trace: `request_timeouts_with_trusted=0`, `trusted_successes=8`,
  `untrusted_successes=8`, `shortcut_hits=89`.
- Connection churn stayed bounded: `bitswap connection backoff: backoffs=1
  skipped=1`; dial rejections were present but did not break the run.

Decision: keep the cap-3 experiment. It is a small, bounded increase in
optimistic direct Bitswap fanout, improves the observed `vitalik` timeout
recovery path, and does not regress the current `ipfs.tech` page-assets
guardrail. The remaining `vitalik` gap is the initial mixed-trusted request
timeout itself; future experiments should try to avoid waiting the full 4s when
the trusted/session candidate is stale and untrusted candidates are already
delivering nearby blocks.

Rejected follow-up: `3500ms` mixed-trusted timeout under cap-3.

```sh
# Temporarily set BITSWAP_TRUSTED_MIXED_REQUEST_TIMEOUT to 3500ms.
cargo build -p freedom-ipfs-gateway

timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-direct3-timeout3500-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-direct3-timeout3500-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `6410/6685ms`, Kubo `1658/2161ms`.
- Max RSS/FD: Rust `39296KiB`/`27`, Kubo `134176KiB`/`75`.
- Trace: `request_timeout_details=3`, all `mixed_trusted`, all
  `timeout_ms=3500`.
- Retry recovery regressed versus the kept 4s cap-3 sample:
  `retry_success_elapsed=p50=1320ms p95=1529ms max=1529ms`.
- Target modes expanded to `want_block=12`, `want_have=19`, `max_want_block=4`,
  `max_want_have=7`.

Decision: reject and restore `4s`. The lower timeout fires earlier, but in this
network window it caused a slower retry path and worse total TTFB than the kept
4s cap-3 run. The remaining `vitalik` gap is not solved by shaving another
500ms from the mixed-trusted request cap.

Rejected follow-up: biased shared Bitswap command select.

Hypothesis: the kept cap-3 `vitalik` trace showed the first provider command
timing out without a corresponding `bitswap_dial_plan`, suggesting that command
processing inside the shared Bitswap swarm might be delayed behind swarm events.
Prioritizing `commands.recv()` in the swarm `select!` might make provider
commands enter dial planning before the 4s caller-side timeout expires.

```sh
# Temporarily add `biased;` to run_shared_bitswap_swarm's tokio::select! with
# the command branch first.
cargo build -p freedom-ipfs-gateway

timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-biased-command-select-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-biased-command-select-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `6660/6678ms`, Kubo `1898/1972ms`.
- Max RSS/FD: Rust `39296KiB`/`33`, Kubo `165764KiB`/`82`.
- Trace still had `request_timeout_details=3`, all `mixed_trusted`, all
  `timeout_ms=4000`.
- Retry recovery regressed versus the kept cap-3 sample:
  `retry_success_elapsed=p50=784ms p95=817ms max=817ms`.
- Peer attempts increased to `62`, and target modes widened to `want_block=12`,
  `want_have=21`, `max_want_block=4`, `max_want_have=7`.

Decision: reject and restore fair `tokio::select!`. Prioritizing command intake
did not remove the timeout shape and made the retry path noisier. The next useful
step should improve diagnostics for request timeouts that lack a dial-plan event,
or target peer scoring/selection for the stale trusted peer, not globally bias
the swarm event loop.

## 2026-05-05 Keep: Timeout Without Dial Plan Summary

Goal:
Future experiments need to distinguish a real peer stall from a caller-side
Bitswap request timeout that fires before the shared swarm records any
`bitswap_dial_plan` for that provider fetch.

Implementation:

- Track `provider_fetch_start` by CID in the harness trace summarizer.
- Mark the CID when a later `bitswap_dial_plan` is observed.
- When `bitswap_request_timeout_detail` arrives before a dial plan for that
  provider fetch, increment:
  - `request_timeouts_without_dial_plan`
  - `mixed_trusted_request_timeouts_without_dial_plan` when the timeout was a
    mixed trusted/provider timeout.
- Print these as `no_dial_plan` and `mixed_no_dial_plan` in the existing
  `bitswap timeout recovery` summary line.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway

timeout 180s cargo run -p mobile-web-harness -- \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-timeout-no-dial-plan-summary-trace.jsonl \
  --output /tmp/vitalik-timeout-no-dial-plan-summary.json
```

Live smoke result:

- Rust passed `1/1`.
- Root TTFB: `6568ms`.
- Trace summary reported:
  `request_timeout_details=1`, `mixed_trusted=1`, `timeout_ms=4000=1`,
  `no_dial_plan=1`, `mixed_no_dial_plan=1`.
- Evidence:
  - `/tmp/vitalik-timeout-no-dial-plan-summary.json`
  - `/tmp/vitalik-timeout-no-dial-plan-summary-trace.jsonl`

Decision: keep. This is harness-only and does not change gateway or retrieval
behavior. It turns the current `vitalik` failure shape into a first-class metric
for future behavior experiments.

## 2026-05-05 Keep: Offload Incoming Bitswap Stream Reads

Hypothesis:
The new no-dial-plan metric showed the `vitalik` child CID
`bafkreibny3ionuayaittbxl2tn5dgfae7sbu45ymd35vhdm3634lmakxqi` timing out
after `provider_fetch_start` and `bitswap_peer_expand`, but before any
`bitswap_dial_plan`. Inspecting `run_shared_bitswap_swarm` showed that the
shared Bitswap loop awaited `read_bitswap_blocks(&mut stream)` directly inside
the incoming-stream branch. A slow inbound Bitswap read could therefore occupy
the main shared swarm future and delay queued provider commands until their
caller-side request timeout fired.

Implementation:

- Move incoming Bitswap stream reads into a bounded `FuturesUnordered` so the
  shared swarm loop can continue processing commands, dial plans, swarm events,
  and other incoming streams while a peer's inbound stream is still reading.
- Cap pending incoming reads at `32` to keep mobile resource use bounded. When
  the cap is hit, the stream is dropped and a `bitswap_incoming_stream_read`
  trace event records `dropped=true`.
- Apply a `6s` timeout to each incoming stream read. Timed-out reads emit
  `bitswap_incoming_stream_read` with `timed_out=true`.
- Preserve the previous delivery behavior after a read completes: match received
  blocks to active pending requests, send a Bitswap cancel for matched CIDs, and
  send an empty Bitswap message when no active request matches.
- Add a deterministic unit test covering the bounded-read timeout helper.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval bitswap
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
cargo build -p freedom-ipfs-gateway
```

Live comparison, exact current code:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-bounded-async-incoming-read-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-bounded-async-incoming-read-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1551/1605ms`, Kubo `1875/1932ms`.
- Max RSS/FD: Rust `38912KiB`/`29`, Kubo `123888KiB`/`79`.
- Trace: `request_timeouts_with_trusted=0`; the previous mixed-trusted
  no-dial-plan timeout shape disappeared in this sample.
- Incoming Bitswap delivery stayed prompt: `max_oldest_pending_ms=690`.
- Peer attempts stayed much lower than the noisier rejected command-bias run:
  `54`.
- Evidence:
  - `/tmp/vitalik-bounded-async-incoming-read-rust-vs-kubo.json`
  - `/tmp/vitalik-bounded-async-incoming-read-rust-vs-kubo-trace.jsonl`

Page workload guardrail, exact current code:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-bounded-async-incoming-read-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-bounded-async-incoming-read-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1048/2159ms`, Kubo `2283/2567ms`.
- Asset TTFB p50/p95: Rust `187/1119ms`, Kubo `95/491ms`.
- Max RSS/FD: Rust `53300KiB`/`50`, Kubo `190032KiB`/`112`.
- Trace: `request_timeouts_with_trusted=0`.
- Dial pressure increased versus the previous cap-3 `ipfs.tech` guardrail:
  `bitswap peer attempts=495`, `dial rejections=105`.
- Evidence:
  - `/tmp/ipfs-tech-bounded-async-incoming-read-rust-vs-kubo.json`
  - `/tmp/ipfs-tech-bounded-async-incoming-read-rust-vs-kubo-trace.jsonl`

Decision: keep. This directly targets a scheduler blockage exposed by the
no-dial-plan metric, removes the repeatable `vitalik` mixed-trusted timeout
shape in the measured window, and makes Rust beat Kubo on both `vitalik` root
HTML and `ipfs.tech` root TTFB while staying much lighter on RSS and file
descriptors. The tradeoff is higher `ipfs.tech` Bitswap dial pressure and a
remaining asset-tail gap versus Kubo; future work should focus on fair
scheduling and peer/dial pressure after incoming-read offload, not on reverting
this change.

## 2026-05-05 Keep: Lower Per-Command Bitswap Dial Cap To 5

Hypothesis:
After incoming-read offload removed the shared-swarm scheduler blockage,
`ipfs.tech` still showed elevated Bitswap peer attempts, connection-limit dial
rejections, and an asset p95 gap versus Kubo. The existing per-command dial cap
of `8` was chosen before incoming reads were made non-blocking. With the swarm
loop now processing commands promptly, a smaller per-command dial budget might
preserve enough peer diversity while reducing connection-limit churn and page
asset tail latency.

Implementation:

- Lower `MAX_BITSWAP_DIAL_ADDRS_PER_COMMAND` from `8` to `5`.
- Keep the global Bitswap connection limits unchanged.
- Keep peer/provider candidate caps unchanged.
- Update deterministic dial-cap coverage to assert that the interleaved planner
  keeps the first ranked address for four peers plus the next best ranked
  address, and suppresses the remaining eleven dial addresses.

Rejected first try:

- Cap `4` improved live `ipfs.tech` in one same-window run, but full retrieval
  tests showed it was too narrow for the deterministic five-provider fallback
  case `want_have_probe_falls_back_to_want_block_quickly`.
- A one-peer budget reduction from cap `5` to cap `4` is not worth dropping that
  fallback coverage.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval caps_bitswap_dial_addresses_per_command
cargo test -p freedom-ipfs-retrieval want_have_probe_falls_back_to_want_block_quickly
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
cargo build -p freedom-ipfs-gateway
```

`ipfs.tech` same-window comparison:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-dial-cap5-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-dial-cap5-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `617/1368ms`, Kubo `2130/3694ms`.
- Asset TTFB p50/p95: Rust `170/593ms`, Kubo `203/590ms`.
- Max RSS/FD: Rust `50920KiB`/`46`, Kubo `268552KiB`/`464`.
- Trace: `request_timeouts_with_trusted=0`, `shortcut_hits=86`,
  `bitswap peer attempts=281`, `dial rejections=3` all connection-limit.
- Incoming delivery stayed prompt: `max_oldest_pending_ms=524`.
- Evidence:
  - `/tmp/ipfs-tech-dial-cap5-rust-vs-kubo.json`
  - `/tmp/ipfs-tech-dial-cap5-rust-vs-kubo-trace.jsonl`

`vitalik` guardrail:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/vitalik-dial-cap5-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-dial-cap5-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1490/1709ms`, Kubo `2013/2277ms`.
- Max RSS/FD: Rust `38656KiB`/`23`, Kubo `191688KiB`/`95`.
- Trace: `request_timeouts_with_trusted=0`; no timeout recovery cluster.
- Evidence:
  - `/tmp/vitalik-dial-cap5-rust-vs-kubo.json`
  - `/tmp/vitalik-dial-cap5-rust-vs-kubo-trace.jsonl`

`daicowtf` guardrail was inconclusive in this network window:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/daicowtf-dial-cap5-rust-vs-kubo-r1-trace.jsonl \
  --output /tmp/daicowtf-dial-cap5-rust-vs-kubo-r1.json
```

Result:

- Rust and Kubo both failed `0/1`.
- Root TTFB: Rust `22327ms`, Kubo `30002ms`.
- The failure is not strong evidence against cap 5 because Kubo could not load
  the same page roots in the same window.
- Rust trace showed one-provider/provider-diversity failure shape:
  `provider_diversity_low=2`, `retry_timeout=1`, `read_timeouts=1`.
- Evidence:
  - `/tmp/daicowtf-dial-cap5-rust-vs-kubo-r1.json`
  - `/tmp/daicowtf-dial-cap5-rust-vs-kubo-r1-trace.jsonl`

Decision: keep. Compared with the prior exact-code cap-8 `ipfs.tech` guardrail
(`asset p95=1119ms`, `dial rejections=105`), cap 5 cut the asset tail and dial
rejection count substantially while keeping `vitalik` healthy and beating Kubo
in the measured windows. The remaining follow-up is to rerun `daicowtf` when
Kubo can load it again, because the same-window failure was public-network or
provider availability rather than a clear Rust regression.

## 2026-05-05 Keep: Summarize Incoming Bitswap Stream Read Pressure

Motivation:
Incoming-read offload added protective `bitswap_incoming_stream_read` events for
two mobile-resource cases:

- dropping an incoming stream when pending incoming reads already hit the cap
- timing out an incoming stream read after `6s`

Those events showed up in the `daicowtf` cap-5 trace as generic trace errors,
but the harness did not summarize whether the resource cap was actually being
hit or whether reads were merely timing out.

Implementation:

- Add `bitswap_incoming_reads` to the trace summary JSON.
- Track `events`, `failures`, `dropped`, `timed_out`, `max_pending_reads`, and
  `max_elapsed_ms` from `bitswap_incoming_stream_read` trace events.
- Print a concise summary line:
  `bitswap incoming stream reads: events=... failures=... dropped=... timed_out=... max_pending_reads=... max_elapsed_ms=...`
- Extend deterministic harness coverage with one dropped read and one timed-out
  read.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. This is diagnostics-only and makes future incoming-read cap or
timeout regressions visible without hand-searching trace errors.

## 2026-05-05 Keep: Merge Low-Diversity Delegated Router Results

Motivation:
The recurring `daicowtf` failure shape is a provider-diversity gap: delegated
routing returns one Bitswap provider for the root, the root block succeeds, then
the linked child CID has no routed providers and the root source peer stalls.
The gateway already accepts comma-separated delegated routing endpoints, but
`DelegatedRoutingClient` returned the first non-empty endpoint response. If a
configured secondary endpoint had an additional provider, it would be ignored.

Implementation:

- For multiple delegated routing endpoints, continue racing requests in
  parallel.
- If an endpoint returns enough Bitswap provider diversity, return immediately.
- If the first non-empty response is low-diversity, merge additional endpoint
  responses for a bounded `750ms` window.
- Deduplicate providers with the existing provider merge logic.
- Return the low-diversity result if other endpoints are empty, errored, or too
  slow.
- Keep single-endpoint behavior unchanged.

Validation:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-routing delegated_routing_merges_low_diversity_endpoint_results
cargo test -p freedom-ipfs-routing delegated_routing_returns_single_low_diversity_result_when_others_empty
cargo test -p freedom-ipfs-routing delegated_routing_low_diversity_merge_wait_is_bounded
cargo test -p freedom-ipfs-routing delegated_routing_races_multiple_endpoints_until_success
cargo test -p freedom-ipfs-routing
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. This does not invent a public gateway fallback and does not
trust remote bytes; it only improves provider candidate discovery when the app
or harness explicitly configures more than one delegated routing endpoint. The
merge wait is bounded so a slow secondary router cannot add a 10s mobile latency
tail.

## 2026-05-05 Keep: Map Incoming Bitswap Read Timeouts To Mobile Retrying

Motivation:
The incoming-read pressure summary made `bitswap_incoming_stream_read` failures
visible in harness output, but the mobile progress mapper and harness
progress-phase mapper still treated that raw phase as an unmapped diagnostic
string. Swift should see a stable UI phase when an incoming Bitswap stream read
times out or is dropped.

Implementation:

- Map `bitswap_incoming_stream_read` to the stable `retrying` progress phase in
  `freedom-ipfs-mobile`.
- Add `bitswap_incoming_stream_read` as a mobile `last_error_code` when the
  event records `ok=false`.
- Map the same raw phase to `retrying` in the mobile web harness progress
  summary.
- Update `docs/mobile-progress-api.md` to mention incoming Bitswap read timeouts
  as a `retrying` example.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p freedom-ipfs-mobile progress_error_code_marks_incoming_stream_read_failures
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. This is diagnostics/API polish only; it does not change
retrieval behavior or the mobile ABI.

## 2026-05-05 Keep: Cache Positive UnixFS Path Resolution

Motivation:
Warm same-daemon `ipfs.tech-page-assets` replay is reliable and resource-light,
but still visibly slower than Kubo on hot page loads. Before this experiment,
the gateway reused decoded DAG-PB metadata but still repeated UnixFS path
resolution for each hot root and asset request.

Implementation:

- Add a bounded positive `(root CID, path) -> resolved node` cache beside the
  existing decoded DAG-PB metadata cache.
- Use the same small capacity as the metadata cache, skip paths over 1024
  bytes, and do not cache failures or negative lookups.
- Keep the cache in memory only; no persistence and no mutable IPNS keying. IPNS
  paths are cached only after resolving to an immutable root CID.
- Extend `unixfs_metadata_cache` traces and harness summaries with
  `path_hits`, `path_misses`, `path_inserts`, `path_evictions`,
  `path_oversized_skips`, and `path_cache_len`.
- Print UnixFS metadata/path cache summaries in Rust-vs-Kubo comparison output.

Baseline:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-warm-persistent-pathcache-baseline-trace.jsonl \
  --output /tmp/ipfs-tech-warm-persistent-pathcache-baseline.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Measured warm root TTFB p50/p95: Rust `20/21ms`, Kubo `2/3ms`.
- Measured warm asset TTFB p50/p95: Rust `20/76ms`, Kubo `3/5ms`.
- Max RSS/FD: Rust `48884KiB`/`34`, Kubo `322592KiB`/`555`.
- Rust `unixfs_resource` trace p50/p90/p95/max across warmup + measured:
  `10/142/202/2973ms`.

Experiment:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-warm-persistent-pathcache-experiment-trace.jsonl \
  --output /tmp/ipfs-tech-warm-persistent-pathcache-experiment.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Measured warm root TTFB p50/p95: Rust `24/25ms`, Kubo `3/3ms`.
- Measured warm asset TTFB p50/p95: Rust `18/69ms`, Kubo `2/8ms`.
- Max RSS/FD: Rust `49888KiB`/`48`, Kubo `256884KiB`/`193`.
- Rust `unixfs_resource` trace p50/p90/p95/max across warmup + measured:
  `6/146/297/970ms`.
- Path cache summary:
  `path_hits=497`, `path_misses=172`, `path_inserts=173`,
  `path_evictions=0`, `path_oversized_skips=0`, `max_path_len=34`.

Cold sanity:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-pathcache-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-pathcache-cold-rust-vs-kubo.json
```

Cold result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `2800/7383ms`, Kubo `1691/2883ms`.
- Asset TTFB p50/p95: Rust `145/1816ms`, Kubo `118/6402ms`.
- Max RSS/FD: Rust `51428KiB`/`47`, Kubo `328932KiB`/`344`.
- Path cache summary:
  `path_hits=445`, `path_misses=509`, `path_inserts=521`,
  `path_evictions=0`, `path_oversized_skips=0`, `max_path_len=34`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. The wall-clock warm root result did not improve in this noisy
same-window sample, but asset p50/p95 improved modestly, the trace shows
hundreds of hot path-resolution hits with no evictions, and the cache is bounded
and positive-only. This is a small warm-path building block; future work should
look at the remaining hot request overhead that keeps Rust around `20ms` while
Kubo is still in the low single-digit milliseconds.

## 2026-05-05 Keep: Cache Positive UnixFS File Sizes

Motivation:
After adding the path-resolution cache, warm same-daemon traces still showed
repeated hot `block_store_get` reads for raw asset CIDs. A common pattern was:
resolve path, compute file size for ETag/range decisions, then read the same
CID again for MIME sniffing or response streaming. File size is immutable once a
CID has been verified, so it is a good fit for the same bounded in-memory
UnixFS metadata cache.

Implementation:

- Add a bounded positive `CID -> file size` cache beside decoded DAG-PB metadata
  and path-resolution entries.
- Cache only successful file-size results; do not cache directories, unsupported
  codecs, missing blocks, or other errors.
- Use the same small capacity as the metadata cache and keep entries in memory
  only.
- Extend `unixfs_metadata_cache` traces and harness summaries with
  `file_size_hits`, `file_size_misses`, `file_size_inserts`,
  `file_size_evictions`, and `file_size_cache_len`.

Baseline:
Use the path-cache experiment above as the baseline.

- Warm root TTFB p50/p95: Rust `24/25ms`, Kubo `3/3ms`.
- Warm asset TTFB p50/p95: Rust `18/69ms`, Kubo `2/8ms`.
- Rust trace: `block_store events=369`, `path_hits=497`,
  `max_path_len=34`.

Experiment:

```sh
cargo build -p freedom-ipfs-gateway
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-warm-persistent-filesize-cache-experiment-rerun-trace.jsonl \
  --output /tmp/ipfs-tech-warm-persistent-filesize-cache-experiment-rerun.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Measured warm root TTFB p50/p95: Rust `19/21ms`, Kubo `2/3ms`.
- Measured warm asset TTFB p50/p95: Rust `10/42ms`, Kubo `2/5ms`.
- Max RSS/FD: Rust `50592KiB`/`44`, Kubo `172856KiB`/`137`.
- Rust trace: `block_store events=266`, `cache_hit=181`,
  `checking_cache=85`.
- File-size cache summary:
  `file_size_hits=146`, `file_size_misses=170`,
  `file_size_inserts=166`, `file_size_evictions=0`,
  `max_file_size_len=33`.

Cold sanity:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-filesize-cache-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-filesize-cache-cold-rust-vs-kubo.json
```

Cold result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `687/918ms`, Kubo `3845/3938ms`.
- Asset TTFB p50/p95: Rust `139/731ms`, Kubo `204/484ms`.
- Max RSS/FD: Rust `52088KiB`/`49`, Kubo `294100KiB`/`378`.
- File-size cache summary:
  `file_size_hits=0`, `file_size_misses=511`,
  `file_size_inserts=501`, `file_size_evictions=0`,
  `max_file_size_len=33`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. This gives a clear warm-path win: asset p50/p95 improved from
`18/69ms` to `10/42ms`, root p50/p95 improved from `24/25ms` to `19/21ms`, and
block-store trace events dropped from `369` to `266` in the same harness mode.
The cache stays bounded, positive-only, read-only, and in-memory, so it fits the
mobile resource constraints.

## 2026-05-05 Keep: Serve Single-Chunk Gateway Bodies Directly

Motivation:
After the UnixFS path and file-size caches, warm `ipfs.tech-page-assets` runs
still spent a noticeable amount of time on many small asset responses. Most of
those assets fit in one `GATEWAY_STREAM_CHUNK_SIZE` chunk. For those responses,
the gateway can read the verified UnixFS range once and return a direct
`Bytes` body instead of constructing an async stream and scoped block-retention
wrapper. Large files, large ranges, and `HEAD` requests keep the existing
streaming path.

Implementation:

- Add a direct-body path for non-HEAD full responses with
  `len <= GATEWAY_STREAM_CHUNK_SIZE`.
- Add the same direct-body path for non-HEAD byte ranges whose range length is
  at most one gateway chunk.
- Preserve the existing streaming path for larger responses and all `HEAD`
  requests.
- Emit `gateway_direct_body` traces with CID, UnixFS path, range bounds, body
  length, and elapsed time.
- Map `gateway_direct_body` to the mobile/harness progress phase `streaming`.
- Add harness summaries for direct-body event count, total bytes, max body
  length, and max elapsed time.

Baseline:
Use the file-size cache experiment above as the baseline.

- Warm root TTFB p50/p95: Rust `19/21ms`, Kubo `2/3ms`.
- Warm asset TTFB p50/p95: Rust `10/42ms`, Kubo `2/5ms`.
- Max RSS/FD: Rust `50592KiB`/`44`, Kubo `172856KiB`/`137`.
- Rust trace: `block_store events=266`, `file_size_hits=146`.

Experiment:

```sh
cargo build -p freedom-ipfs-gateway
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/ipfs-tech-warm-persistent-direct-body-experiment-trace.jsonl \
  --output /tmp/ipfs-tech-warm-persistent-direct-body-experiment.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Measured warm root TTFB p50/p95: Rust `20/20ms`, Kubo `2/3ms`.
- Measured warm asset TTFB p50/p95: Rust `8/43ms`, Kubo `3/5ms`.
- Max RSS/FD: Rust `50400KiB`/`37`, Kubo `250156KiB`/`294`.
- Direct-body trace: `124` events, `916196` bytes total,
  `max_body_len=61741`, elapsed p50/p95/max `1/13/18ms`.

Cold sanity:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-direct-body-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-direct-body-cold-rust-vs-kubo.json
```

Cold result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1190/2629ms`, Kubo `1761/2794ms`.
- Asset TTFB p50/p95: Rust `248/1148ms`, Kubo `139/592ms`.
- Max RSS/FD: Rust `54036KiB`/`45`, Kubo `194292KiB`/`145`.
- The run was dominated by public-network/provider variance:
  `bitswap_dial_rejections=116`, retrying progress events `119`.
- Direct-body trace: `93` events.

Range-heavy sanity:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-direct-body-range-rust-vs-kubo-trace.jsonl \
  --output /tmp/vitalik-direct-body-range-rust-vs-kubo.json
```

Range-heavy result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1698/4474ms`, Kubo `1802/1958ms`.
- Max RSS/FD: Rust `38400KiB`/`23`, Kubo `163956KiB`/`69`.
- Direct-body trace: `3` events, `384` bytes total, max body `128`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Decision: keep. This is not a major retrieval breakthrough, but it is a small,
bounded warm-path win: asset p50 improved from `10ms` to `8ms` in the warm
persistent sample, p95 stayed effectively flat, the code path is limited to
already verified single-chunk responses, and resource use stayed low. Cold
network runs remain governed by provider discovery and Bitswap peer quality, so
the next higher-leverage work should return to provider/session behavior rather
than expanding this direct-body path.

## 2026-05-05 Parked: Raising Pending Bitswap Dial Cap

Hypothesis:
The cold direct-body run still showed many connection-limit dial rejections:
`116` rejected dials, mostly TCP, with only `14` established Bitswap
connections. Since established connections stayed below the configured cap,
raising only `BITSWAP_MAX_PENDING_OUTGOING_CONNECTIONS` from `16` to `24`
might reduce cold asset tails without increasing the steady established
connection bound.

Change tested:

```rust
const BITSWAP_MAX_PENDING_OUTGOING_CONNECTIONS: u32 = 24;
const BITSWAP_MAX_ESTABLISHED_CONNECTIONS: u32 = 16;
```

Validation before live run:

```sh
cargo test -p freedom-ipfs-retrieval
cargo build -p freedom-ipfs-gateway
```

Experiment:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-pending24-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-pending24-cold-rust-vs-kubo.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1769/2873ms`, Kubo `1655/2696ms`.
- Asset TTFB p50/p95: Rust `182/730ms`, Kubo `140/841ms`.
- Max RSS/FD: Rust `50508KiB`/`51`, Kubo `164792KiB`/`76`.
- Bitswap connection-limit dial rejections disappeared from the summary.
- Established Bitswap TCP connections rose to `30`.

Same-window baseline after reverting the cap to `16`:

```sh
cargo build -p freedom-ipfs-gateway
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-pending16-samewindow-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-pending16-samewindow-cold-rust-vs-kubo.json
```

Same-window baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `901/2258ms`, Kubo `2375/2445ms`.
- Asset TTFB p50/p95: Rust `139/641ms`, Kubo `83/203ms`.
- Max RSS/FD: Rust `50092KiB`/`48`, Kubo `187488KiB`/`117`.
- Bitswap dial rejections: `14`, all connection-limit, transports
  `tcp=12`, `quic=2`.
- Established Bitswap TCP connections: `23`.

Decision: do not keep. Raising the pending dial cap proved that the rejections
are tunable, but it did not improve the same-window Rust latency sample and it
increased FD usage slightly. The better next experiment is more selective:
preserve the mobile connection caps while improving which peers get the limited
pending dial slots, especially by using provider/session quality signals rather
than simply allowing more simultaneous dials.

## 2026-05-05 Parked: Sorting Provider Peers By Address Quality

Hypothesis:
Keep the mobile connection caps unchanged, but sort provider-derived Bitswap
peers by their best dialable address before the 16-peer cap and per-command dial
cap are applied. The intended effect was to keep direct IP TCP/QUIC peers ahead
of DNS/WebSocket-style peers when scarce pending dial slots are available.

Change tested:

```rust
peers.sort_by_key(bitswap_peer_best_addr_score);
peers.truncate(MAX_BITSWAP_PEERS_PER_BLOCK);
```

Validation before live run:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval orders_bitswap_peers_by_best_address_quality
cargo build -p freedom-ipfs-gateway
```

Experiment:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-peer-quality-order-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-peer-quality-order-cold-rust-vs-kubo.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `2355/2541ms`, Kubo `5421/12171ms`.
- Asset TTFB p50/p95: Rust `125/967ms`, Kubo `333/1327ms`.
- Max RSS/FD: Rust `51400KiB`/`52`, Kubo `434116KiB`/`1048`.
- Bitswap dial rejections: `44`, all connection-limit, transports
  `tcp=40`, `ws=3`, `quic=1`.
- Established Bitswap TCP connections: `35`.

Comparison notes:
This run beat a very slow Kubo sample, but compared with the same-window cap-16
baseline immediately above, Rust root p50/p95 regressed from `901/2258ms` to
`2355/2541ms`, asset p95 regressed from `641ms` to `967ms`, dial rejections
rose from `14` to `44`, and FD max rose from `48` to `52`.

Decision: do not keep. The peer-quality sort changed which peers consumed dial
slots, but it did not reduce connection pressure or tail latency in the
same-window evidence. A better next step is to collect per-peer success/failure
quality over time and bias peers with observed page-session success, rather
than ranking cold public providers by static address shape alone.

## 2026-05-05 Parked: Sorting Recent Bitswap Peers By Success Count

Hypothesis:
The existing recent-success Bitswap session cache sorts successful peers mostly
by recency. During page loads, traces usually show one or two peers delivering
most blocks. Sorting recent/session peers by repeated success count first, then
recency, might keep the strongest observed page-session peers at the front
without raising connection caps or changing provider discovery.

Change tested:

- Add `success_count` to the in-memory `SuccessfulBitswapPeer` record.
- Increment the count on each successful Bitswap delivery.
- Preserve existing addresses when a later success record has no addresses.
- Sort provider-trusted and session-only recent peers by `success_count`, then
  by `seen_at`.

Validation before live run:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-retrieval successful_bitswap
cargo build -p freedom-ipfs-gateway
```

Experiment:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-session-score-cold-rust-vs-kubo-trace.jsonl \
  --output /tmp/ipfs-tech-session-score-cold-rust-vs-kubo.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1513/3162ms`, Kubo `3876/4021ms`.
- Asset TTFB p50/p95: Rust `161/1445ms`, Kubo `210/503ms`.
- Max RSS/FD: Rust `51596KiB`/`51`, Kubo `289804KiB`/`473`.
- Bitswap dial plans: `136` events, `535` candidates, `137` new addrs,
  `196` suppressed addrs, `55` pending peers, `299` connected peers.
- Bitswap dial rejections: `55`, all connection-limit, transports
  `tcp=45`, `quic=8`, `ws=2`.

Same-window baseline after reverting the change:

```sh
cargo build -p freedom-ipfs-gateway
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-session-score-revert-samewindow-cold-trace.jsonl \
  --output /tmp/ipfs-tech-session-score-revert-samewindow-cold.json
```

Same-window baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1168/1270ms`, Kubo `2223/3382ms`.
- Asset TTFB p50/p95: Rust `206/1102ms`, Kubo `152/521ms`.
- Max RSS/FD: Rust `51512KiB`/`51`, Kubo `273036KiB`/`261`.
- Bitswap dial plans: `133` events, `532` candidates, `128` new addrs,
  `214` suppressed addrs, `31` pending peers, `312` connected peers.
- Bitswap dial rejections: `51`, all connection-limit, transports
  `tcp=37`, `quic=14`.

Decision: do not keep. Success-count ordering was plausible and stayed within
the same resource caps, but it made the same-window root p50/p95 and asset p95
worse. It also left connection-limit pressure essentially unchanged. The trace
still shows the same basic constraint: a few good peers eventually dominate
delivery, but early cold requests continue to spend limited dial slots on weak
or stale candidates before that signal is strong enough. A better next
experiment should bias against recently failed connection classes earlier, not
just reorder successful peers after the fact.

## 2026-05-05 Keep: Drop Waiters For Dials That Never Started

Motivation:
The parked dial-cap experiments showed local connection-limit rejections, and
the trace often kept many peers in a pending state. In `run_shared_bitswap_swarm`
the command path created connection waiters before calling `swarm.dial`. If
`swarm.dial` rejected every address for a scheduled peer synchronously, for
example because the local connection limit was already full, those waiters could
remain even though no dial was actually pending. Future commands then treated
that peer as pending and current attempts could wait on a connection that would
never be established.

Implementation:

- Track which scheduled Bitswap peers actually had at least one `swarm.dial`
  call accepted.
- After the dial loop, drop waiters and wait-start timestamps for scheduled
  peers where no dial started.
- Emit `bitswap_dial_waiters_dropped` with peer and waiter counts.
- Map the new trace phase to mobile/harness progress phase `retrying`.
- Keep existing connection caps unchanged.

Validation before live run:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval drops_waiters_for_dials_that_never_started
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo build -p freedom-ipfs-gateway
```

Experiment:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 240 \
  --trace-output /tmp/ipfs-tech-drop-failed-dial-waiters-cold-trace.jsonl \
  --output /tmp/ipfs-tech-drop-failed-dial-waiters-cold.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `909/1167ms`, Kubo `2472/5094ms`.
- Asset TTFB p50/p95: Rust `139/374ms`, Kubo `151/368ms`.
- Max RSS/FD: Rust `49956KiB`/`42`, Kubo `303240KiB`/`541`.
- Bitswap dial plans: `112` events, `212` candidates, `48` new addrs,
  `62` suppressed addrs, `2` pending peers, `153` connected peers.
- Bitswap dial rejections: none in the summary.
- Bitswap peer attempts dropped from the previous baseline's `459` starts to
  `198` starts.
- Oldest pending incoming wait dropped from the previous baseline's `777ms` to
  `223ms`.

Range-heavy guard:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-drop-failed-dial-waiters-range-trace.jsonl \
  --output /tmp/vitalik-drop-failed-dial-waiters-range.json
```

Range-heavy result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `636/5170ms`, Kubo `1941/2011ms`.
- Max RSS/FD: Rust `36608KiB`/`18`, Kubo `119520KiB`/`84`.
- Bitswap dial plans: `6` events, `51` candidates, `15` new peers/addrs,
  `33` suppressed peers, `79` suppressed addrs, `0` pending peers,
  `3` connected peers.
- Bitswap dial rejections: none in the summary.

Same-window baseline:
Use the immediately preceding reverted run:
`/tmp/ipfs-tech-session-score-revert-samewindow-cold.json` and
`/tmp/ipfs-tech-session-score-revert-samewindow-cold-trace.jsonl`.

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1168/1270ms`, Kubo `2223/3382ms`.
- Asset TTFB p50/p95: Rust `206/1102ms`, Kubo `152/521ms`.
- Max RSS/FD: Rust `51512KiB`/`51`, Kubo `273036KiB`/`261`.
- Bitswap dial plans: `133` events, `532` candidates, `128` new addrs,
  `214` suppressed addrs, `31` pending peers, `312` connected peers.
- Bitswap dial rejections: `51`, all connection-limit, transports
  `tcp=37`, `quic=14`.

Decision: keep. The change is narrow, fixes a concrete stale-waiter condition,
keeps mobile connection caps unchanged, and the same-window evidence improved
root p50/p95, asset p50/p95, FD max, dial-plan pressure, and pending wait
latency. The trace had no local dial rejections in this sample, and Kubo still
used much higher memory and FD counts.

## 2026-05-05 Keep: Trace Delegated Provider Lookup Latency

Motivation:
After dropping failed dial waiters, the range-heavy `vitalik-root-html-range`
guard still had a Rust p95 root tail. Manual JSONL inspection showed the slow
request spent about `4.8s` in `provider_lookup` before Bitswap fetched the block
quickly. The trace did not say whether that was delegated routing, DHT fallback,
response parsing, or retrieval work hidden under the higher-level provider
lookup span.

Implementation:

- Emit `delegated_provider_lookup` from each delegated routing endpoint request.
- Include CID, endpoint, success/failure, provider count, sanitized error, and
  elapsed time.
- Map the phase to mobile/harness progress phase `provider_lookup`.
- Add harness summary counters for delegated lookup events, successes,
  failures, total provider count, and max elapsed time.

Validation before live run:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-routing delegated_routing_races_multiple_endpoints_until_success
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo build -p freedom-ipfs-gateway
```

Experiment:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-delegated-provider-lookup-trace.jsonl \
  --output /tmp/vitalik-delegated-provider-lookup.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1341/6325ms`, Kubo `1898/1952ms`.
- Max RSS/FD: Rust `38784KiB`/`20`, Kubo `177664KiB`/`101`.
- Delegated provider lookup summary: `events=6`, `successes=6`,
  `failures=0`, `providers=136`, `max_elapsed_ms=4993`.
- Bitswap dial plans: `8` events, `69` candidates, `25` new peers/addrs,
  `37` suppressed peers, `96` suppressed addrs, `0` pending peers,
  `7` connected peers.

Decision: keep. This is diagnostics-only, but it identifies the current
`vitalik` p95 tail as delegated routing latency rather than Bitswap, UnixFS, or
range serving. The next routing experiment should test a bounded fallback or
race for slow delegated provider lookups while preserving read-only behavior
and mobile resource caps.

## 2026-05-05 Reject: Light-DHT-Only Vitalik Routing

Hypothesis:
The previous `vitalik-root-html-range` run showed delegated provider lookup
latency as high as `4993ms`. If delegated routing is the tail source, a
light-DHT-only routing mode might beat the delegated path for this range-heavy
case while keeping resource use low.

Build:

```sh
cargo build -p freedom-ipfs-gateway -p mobile-web-harness
```

Experiment:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --routing-mode light-dht \
  --dht-query-timeout-secs 10 \
  --dht-max-providers 4 \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-light-dht-routing-trace.jsonl \
  --comparison-output /tmp/vitalik-light-dht-routing.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `11322/11655ms`, Kubo `2850/3111ms`.
- Max RSS/FD: Rust `46704KiB`/`20`, Kubo `123648KiB`/`89`.
- Progress phases included `provider_lookup=6`, `providers_found=6`,
  `fetching_bitswap=46`, and `streaming=30`.
- Bitswap dial plans were small: `6` events, `8` candidates, `5` new peers,
  `10` new addrs, `0` pending peers, `3` connected peers.
- Bitswap delivery itself was quick once providers were found:
  max pending incoming wait was `481ms`.

Decision: reject. DHT-only routing preserved low FD and memory use but moved
the tail into provider discovery and was much slower than the delegated auto
baseline from the previous same-day run. This argues against replacing
delegated routing for `vitalik-root-html-range`. The more promising direction
is a bounded slow-delegated fallback/race that preserves fast delegated wins
instead of forcing every lookup through light DHT.

## 2026-05-05 Keep: Harness Delegated-Router Sweep Knob

Motivation:
The provider-quality lab needs to vary delegated router endpoints in spawned
Rust gateway runs without manually starting gateways. The gateway already
supports `--delegated-router`, including comma-separated endpoint lists, but the
mobile web harness did not forward that option.

Implementation:

- Add `mobile-web-harness --delegated-router`.
- Forward the value to spawned Rust gateways as `freedom-ipfs-gateway
  --delegated-router`.
- Document that the value can be a single endpoint or comma-separated endpoint
  list.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness args_accept_delegated_router_endpoint_list
cargo test -p mobile-web-harness
cargo build -p freedom-ipfs-gateway -p mobile-web-harness
git diff --check
```

Endpoint experiment, `cid.contact` only:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --delegated-router https://cid.contact/routing/v1 \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-cid-contact-routing-trace.jsonl \
  --comparison-output /tmp/vitalik-cid-contact-routing.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `12132/23056ms`, Kubo `1806/3075ms`.
- Max RSS/FD: Rust `50176KiB`/`21`, Kubo `127208KiB`/`95`.
- Delegated provider lookup summary: `events=7`, `successes=0`,
  `failures=7`, `providers=0`, `max_elapsed_ms=213`.
- Trace errors were `404 Not Found` responses from `cid.contact` for both the
  root CID and range block CID, causing auto routing to fall through to DHT.

Endpoint experiment, default plus `cid.contact` raced:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-raced-delegated-routing-trace.jsonl \
  --comparison-output /tmp/vitalik-raced-delegated-routing.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1861/5571ms`, Kubo `2846/3262ms`.
- Max RSS/FD: Rust `38528KiB`/`22`, Kubo `172964KiB`/`116`.
- Delegated provider lookup summary: `events=12`, `successes=6`,
  `failures=6`, `providers=132`, `max_elapsed_ms=3937`.
- The extra endpoint contributed only fast `404` failures in this sample.

Same-window default rerun:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/vitalik-default-delegated-rerun-trace.jsonl \
  --comparison-output /tmp/vitalik-default-delegated-rerun.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1816/1838ms`, Kubo `2963/3011ms`.
- Max RSS/FD: Rust `38400KiB`/`21`, Kubo `145192KiB`/`106`.
- Delegated provider lookup summary: `events=6`, `successes=6`,
  `failures=0`, `providers=129`, `max_elapsed_ms=74`.

Decision: keep the harness knob; reject adding `cid.contact` to the default
endpoint set from this evidence. For this case and network window, the current
default delegated router was both faster and cleaner than `cid.contact` alone
or a raced default-plus-`cid.contact` configuration.

## 2026-05-05 Keep: Delegated Provider Endpoint Summary

Motivation:
The delegated lookup trace records the endpoint for each completed router
request, but the harness summary only reported the aggregate across all
endpoints. Provider-quality sweeps need the report to show which endpoint
returned providers, failed, or dominated lookup latency.

Implementation:

- Add `delegated_provider_lookup_by_endpoint` to trace summaries.
- Print per-endpoint events, success/failure counts, provider totals, and max
  elapsed time under the aggregate delegated provider lookup line.
- Keep the existing aggregate fields unchanged for compatibility with previous
  reports.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p mobile-web-harness
git diff --check
```

Live smoke:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-raced-endpoint-summary-smoke-trace.jsonl \
  --comparison-output /tmp/vitalik-raced-endpoint-summary-smoke.json
```

Result:

- Rust and Kubo both passed `1/1`.
- Root TTFB: Rust `1659ms`, Kubo `2864ms`.
- Max RSS/FD: Rust `38144KiB`/`21`, Kubo `115456KiB`/`68`.
- The report printed:
  `delegated provider lookup: events=2 successes=2 failures=0 providers=43
  max_elapsed_ms=43`.
- The per-endpoint line showed
  `https://delegated-ipfs.dev/routing/v1: events=2 successes=2 failures=0
  providers=43 max_elapsed_ms=43`.

Decision: keep. This is diagnostics-only and makes future endpoint sweeps
readable from the normal harness output instead of requiring manual JSONL
inspection.

## 2026-05-05 Keep: Bitswap Connection Error Address Families

Motivation:
The current `ipfs-tech-page-assets` refresh passed but still showed dial
pressure and several `No route to host` connection errors. The harness grouped
connection errors by class and peer, but did not say whether failures were tied
to IPv4, IPv6, mixed, or unknown multiaddrs.

Implementation:

- Classify `bitswap_connection_error` error strings by embedded multiaddr
  family: `ip4`, `ip6`, `mixed`, or `unknown`.
- Add `bitswap_connection_error_addr_families` to trace summaries.
- Print the family breakdown alongside connection error class and peer counts.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p mobile-web-harness
git diff --check
```

Context refresh before adding the diagnostic:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-current-refresh-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-current-refresh.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `689/2427ms`, Kubo `3789/3931ms`.
- Asset TTFB p50/p95: Rust `200/1789ms`, Kubo `224/546ms`.
- Max RSS/FD: Rust `52484KiB`/`44`, Kubo `300896KiB`/`368`.
- Delegated lookup max was only `91ms`, so this window was not routing-bound.
- Trace still had `48` connection-limit dial rejections and connection errors
  including repeated IPv6 `No route to host` failures.

Live smoke after adding the diagnostic:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-connection-error-family-smoke-trace.jsonl \
  --comparison-output /tmp/vitalik-connection-error-family-smoke.json
```

Result:

- Rust and Kubo both passed `1/1`.
- Root TTFB: Rust `1680ms`, Kubo `2901ms`.
- The report printed `bitswap connection errors: events=2 ... classes=
  connection_refused=2 addr_families=ip4=2 ...`.

Decision: keep. This is diagnostics-only and gives future provider/address
policy experiments a clearer signal for whether connection failures are
address-family-specific before changing any dial filtering behavior.

## 2026-05-05 Reject: Skip DNS IP Expansion For Unsupported Bitswap Addrs

Hypothesis:
The latest `ipfs-tech-page-assets` refresh showed the slowest asset spending
about `2032ms` in `bitswap_peer_expand` while expanding `561` provider
multiaddrs into `576` candidates. Most expanded addresses were later rejected
because they contained relay, WebRTC, WebTransport, or certhash components.
Skipping DNS IP expansion for those always-rejected multiaddrs might reduce
asset tails without changing the supported Bitswap candidate set.

Temporary implementation:

- After DNSAddr TXT expansion, detect multiaddrs with relay/WebRTC/
  WebTransport/certhash features.
- Push those records through unchanged instead of resolving their DNS names to
  IP addresses.
- Keep websocket DNS multiaddrs unchanged as before.

Focused validation passed:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval dns_ip_expansion --lib
cargo test -p freedom-ipfs-retrieval cached_dns_expansion_reuses_dnsaddr_and_ip_results --lib
git diff --check
```

Experiment:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-skip-unsupported-dns-expansion-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-skip-unsupported-dns-expansion.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `865/2217ms`, Kubo `1645/5061ms`.
- Asset TTFB p50/p95: Rust `313/899ms`, Kubo `188/996ms`.
- Max RSS/FD: Rust `53968KiB`/`49`, Kubo `290060KiB`/`502`.
- Delegated lookup max was `667ms`.
- Bitswap dial rejections: `58`, all connection-limit.
- Bitswap connection errors: `14`, address families `ip4=9`, `ip6=5`.

Same-window baseline recheck after stashing the temporary change:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-unsupported-dns-baseline-recheck-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-unsupported-dns-baseline-recheck.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `911/946ms`, Kubo `2440/2529ms`.
- Asset TTFB p50/p95: Rust `141/641ms`, Kubo `94/2350ms`.
- Max RSS/FD: Rust `51492KiB`/`47`, Kubo `278948KiB`/`324`.
- Delegated lookup max was `187ms`.
- Bitswap dial rejections: `5`, all connection-limit.
- Bitswap connection errors: `12`, address families `ip4=12`.

Decision: reject and revert. The temporary change preserved reliability, but
the same-window baseline was better on root p50/p95, asset p50/p95, RSS, FD, and
dial pressure. The initial apparent asset-tail win was network-window noise, not
evidence to keep the filtering behavior.

## 2026-05-05 Current Daicowtf Sparse-Provider Refresh

Command:

```sh
timeout 420s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --run-timeout-secs 180 \
  --trace-output /tmp/daicowtf-current-refresh-trace.jsonl \
  --comparison-output /tmp/daicowtf-current-refresh.json
```

Result:

- Rust and Kubo both failed `3/3`.
- Root TTFB p50/p95: Rust `10934/27084ms`, Kubo `30003/30003ms`.
- Max RSS/FD: Rust `44672KiB`/`17`, Kubo `153280KiB`/`193`.
- Rust delegated provider lookup summary: `events=7`, `successes=7`,
  `failures=0`, `providers=3`, `max_elapsed_ms=161`.
- DHT fallback failed on all three low-diversity attempts:
  `provider_lookup: dht: the request timed out=3`.
- Bitswap delivery from the one delegated peer succeeded for three tiny blocks
  quickly: source peer
  `12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP`, total `244ms`,
  max `95ms`, transports `tcp=2`, `quic=1`.
- The failing child CID was
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u`; the only
  useful provider opened streams but did not return the requested block.
- Incoming stream reads timed out three times at about `6001ms`.

Conclusion:
This remains a public-network sparse/stale-provider case, not a Rust-only
regression. Kubo failed every run and used substantially more resources. Keep
`daicowtf-page-assets` as an opt-in provider-quality target. The likely future
work is better provider diversity/fallback for sparse roots, not gateway or
UnixFS serving changes.

## 2026-05-05 Keep: Mobile Progress Target Counters

Motivation:
The mobile progress snapshot already exposes bounded JSON events and active
targets, but per-load counters were mostly event-local. Swift needs cheap
per-target counters to say whether a load is making progress or only retrying.

Implementation:

- Add `blocks_loaded` and `retry_count` to progress events.
- Add the same fields to active progress targets.
- Accumulate `blocks_loaded` when a target sees `block_fetch_total`.
- Accumulate `retry_count` when a target maps to the stable `retrying` phase.
- Report stable high-level `source` values such as `cache`, `bitswap`,
  `http_provider`, `delegated_routing`, and `dht`.
- Preserve lower-level Bitswap direction as `delivery` so `source` can stay
  UI-friendly while logs can still distinguish `incoming` and `outgoing`.
- Preserve the final counter values on completed, failed, and cancelled events
  after the target is removed from `active`.
- Update `docs/mobile-progress-api.md`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-mobile progress_snapshot_records_gateway_request_phases
cargo test -p freedom-ipfs-mobile progress_snapshot_accumulates_target_counters
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p freedom-ipfs-mobile
cargo check --workspace --all-targets
git diff --check
```

Decision: keep. This is ABI-neutral because the existing Swift wrapper returns
JSON, and it directly fills part of the mobile-facing progress API requirement
for per-load counters without adding callbacks or unbounded state.

## 2026-05-05 Keep: Gateway Request Elapsed Trace Summary

Motivation:
The latest warm `ipfs-tech-page-assets` comparison showed client-observed Rust
root TTFB of `57ms` and `21ms` on the second and third measured runs, while the
gateway trace showed the corresponding warm root handlers completing in `0ms`
and `1ms`. That gap is outside UnixFS/retrieval work, so the harness needs a
first-class way to compare client TTFB with gateway-internal request elapsed
time before tuning the node.

Implementation:

- Add `gateway_request_elapsed_ms` to the parsed trace summary.
- Populate it from `request_done elapsed_ms` values.
- Print it with gateway status summaries in single-engine and Rust-vs-Kubo
  trace output.
- Include the field in JSON reports because `TraceSummary` is serialized.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_
cargo test -p mobile-web-harness
timeout 180s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --trace-output /tmp/vitalik-gateway-elapsed-trace.jsonl \
  --comparison-output /tmp/vitalik-gateway-elapsed-comparison.json
timeout 420s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-gateway-elapsed-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-gateway-elapsed-comparison.json
```

Live results:

- `vitalik-root-html-range`: Rust and Kubo both passed `1/1`. Rust root TTFB
  was `1981ms`, Kubo `1859ms`; Rust max RSS/FD was `38272KiB`/`22` versus Kubo
  `182304KiB`/`92`. Gateway request elapsed summary was
  `p50=1978ms p90=1978ms p95=1978ms max=1978ms`, confirming this cold sample
  was real retrieval time, not local HTTP overhead.
- `ipfs-tech-page-assets`: Rust and Kubo both passed `3/3`. Root TTFB p50/p95:
  Rust `19/1134ms`, Kubo `3/1243ms`. Asset TTFB p50/p95: Rust `10/197ms`,
  Kubo `5/242ms`. Rust max RSS/FD was `50824KiB`/`42`, Kubo
  `114948KiB`/`43`.
- The Rust trace reported gateway request elapsed
  `p50=6ms p90=134ms p95=201ms max=1114ms`, delegated provider lookup
  max `57ms`, and direct-body max elapsed `10ms`.

Decision: keep. This is diagnostics-only and prevents future warm-path work
from misattributing client-side/local HTTP timing to gateway, UnixFS, cache, or
retrieval internals. The next speed experiments should focus on traces where
`gateway_request_elapsed_ms` is high, not only where external TTFB is high.

## 2026-05-05 Reject: Adaptive Diverse-Provider Post-Lookup Wait

Hypothesis:
The new gateway request elapsed summary showed some `ipfs-tech-page-assets`
asset requests spending about `200ms` in
`bitswap_session_shortcut_post_lookup_wait` after delegated routing had already
returned a diverse provider set. Shortening that post-lookup wait only when
provider diversity was high might reduce asset tails without hurting sparse
provider cases.

Experiment:

- Temporarily add `BITSWAP_SESSION_DIVERSE_POST_LOOKUP_GRACE = 75ms`.
- Use it when the delegated provider result count is at least `16`.
- Keep the existing `200ms` grace for sparse provider sets.
- Add provider count to the post-lookup timeout trace.

Validation and live runs:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval post_lookup_session_grace
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
cargo build -p freedom-ipfs-gateway -p mobile-web-harness
timeout 420s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-adaptive-postlookup-built-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-adaptive-postlookup-built-comparison.json
```

Important run note:
An earlier attempt used `cargo run -p mobile-web-harness` without rebuilding
`freedom-ipfs-gateway`, so it spawned the previous gateway binary and did not
exercise the retrieval change. The metrics below are from the rebuilt gateway.

Adaptive result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `20/703ms`, Kubo `2/1513ms`.
- Asset TTFB p50/p95: Rust `14/288ms`, Kubo `4/1016ms`.
- Gateway request elapsed p50/p90/p95/max:
  `8/228/319/977ms`.
- Bitswap session: `shortcut_starts=34`, `shortcut_post_lookup_waits=13`,
  `shortcut_hits=21`.
- Dial pressure increased: `starts=137`, `outgoing_completed=22`,
  `dial_rejections=25`, all connection-limit.
- Max RSS/FD: Rust `52076KiB`/`44`, Kubo `192048KiB`/`108`.

Same-window baseline after reverting and rebuilding:

```sh
cargo build -p freedom-ipfs-gateway -p mobile-web-harness
timeout 420s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-postlookup-baseline-rerun-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-postlookup-baseline-rerun-comparison.json
```

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `19/620ms`, Kubo `3/4573ms`.
- Asset TTFB p50/p95: Rust `16/255ms`, Kubo `4/373ms`.
- Gateway request elapsed p50/p90/p95/max:
  `7/180/253/602ms`.
- Bitswap session: `shortcut_starts=34`, `shortcut_post_lookup_waits=0`,
  `shortcut_hits=34`.
- Dial pressure stayed low: `starts=39`, `outgoing_completed=0`,
  `dial_rejections=0`.
- Max RSS/FD: Rust `49416KiB`/`30`, Kubo `297960KiB`/`409`.

Decision: reject. The adaptive wait made the provider path more aggressive,
but in the rebuilt same-window run it lost shortcut hits, increased peer
attempts and connection-limit dial rejections, and worsened gateway request
p95/max versus the kept `200ms` behavior. Keep the fixed `200ms` post-lookup
grace for now; future work should use the gateway elapsed summary to look for
cases where the wait repeatedly times out without increasing dial pressure.

## 2026-05-05 Keep: Harness Gateway Build Flag

Motivation:
The adaptive post-lookup experiment initially produced a misleading run because
`cargo run -p mobile-web-harness` rebuilt only the harness. The harness then
spawned the stale `target/debug/freedom-ipfs-gateway`, so retrieval changes were
not actually under test. Long-running experiments need a low-friction way to
avoid that mistake.

Implementation:

- Add `mobile-web-harness --build-gateway`.
- When spawning the default Rust gateway, run
  `cargo build -p freedom-ipfs-gateway` once before measurement.
- Reject invalid combinations with `--gateway-url`, `--engine kubo`, or a custom
  `--gateway-bin`.
- Clear the flag for the Kubo side of `--compare-kubo`.
- Document the flag in `docs/mobile-web-readiness/README.md`.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness args_accept_build_gateway_flag
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --trace-output /tmp/vitalik-build-gateway-flag-trace.jsonl \
  --output /tmp/vitalik-build-gateway-flag.json
git diff --check
```

Decision: keep. This is harness-only and prevents stale-binary measurements
without changing the gateway, retrieval behavior, mobile ABI, or runtime
resource profile.

## 2026-05-05 Keep: Harness Progress Correlation Headers

Motivation:
The mobile progress API can already consume gateway request correlation headers,
but the live harness was not sending them. That meant page-crawl traces and
mobile-style progress snapshots could show individual gateway requests without
consistently tying subresources and conditional revalidations back to the
top-level page load. Long-running mobile-web runs need that grouping before
working on page-level latency, progress wording, or per-navigation diagnostics.

Implementation:

- Add harness-generated `X-Freedom-Request-ID`,
  `X-Freedom-Parent-Request-ID`, and `X-Freedom-Top-Level-Path` headers.
- Give each root case request a stable root correlation ID and top-level path.
- Give crawled assets child request IDs with the root request as parent, and
  give conditional revalidations child request IDs under the request they
  revalidate.
- Keep correlation optional inside lower-level request helpers so existing
  tests and direct helper uses can remain uncorrelated.
- Add a focused unit test for the generated request headers.
- Document the behavior in `docs/mobile-web-readiness/README.md`.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness request_correlation_headers_include_parent_and_top_level_path
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-correlation-headers-trace.jsonl \
  --output /tmp/ipfs-tech-correlation-headers.json
git diff --check
```

Live result:

- `ipfs-tech-page-assets` passed `1/1`.
- Root TTFB was `2195ms`.
- Asset TTFB p50/p90/p95/max was `94/1477/1790/6607ms`.
- The slowest asset tail was a network/routing tail in this sample; delegated
  provider lookup reached `5557ms` for
  `/ipns/ipfs.tech/_nuxt/Grid.CfsFuo-l.css`.
- The trace contained `33` `request_start` events. All `33` had
  `span.progress_request_id`, and `32` had `span.parent_request_id`, matching
  one root request plus asset/revalidation child requests.

Trace verification:

```sh
python3 - <<'PY'
import json
from pathlib import Path
seen = []
for line in Path('/tmp/ipfs-tech-correlation-headers-trace.jsonl').read_text().splitlines():
    event = json.loads(line)
    if event.get('phase') == 'request_start':
        span = event.get('span') or {}
        seen.append({
            'request_id': event.get('request_id'),
            'path': event.get('path'),
            'span_progress_request_id': span.get('progress_request_id'),
            'span_parent_request_id': span.get('parent_request_id'),
            'span_top_level_path': span.get('top_level_path'),
        })
for row in seen[:8]:
    print(row)
print('count', len(seen), 'with_progress', sum(1 for row in seen if row['span_progress_request_id']))
print('with_parent', sum(1 for row in seen if row['span_parent_request_id']))
PY
```

Sample output:

```text
{'request_id': 1, 'path': '/ipns/ipfs.tech/', 'span_progress_request_id': 1, 'span_parent_request_id': 0, 'span_top_level_path': '/ipns/ipfs.tech/'}
{'request_id': 2, 'path': '/ipns/ipfs.tech/_nuxt/index.CZYCeseQ.css', 'span_progress_request_id': 5, 'span_parent_request_id': 1, 'span_top_level_path': '/ipns/ipfs.tech/'}
{'request_id': 3, 'path': '/ipns/ipfs.tech/_nuxt/CBJE44gf.js', 'span_progress_request_id': 13, 'span_parent_request_id': 1, 'span_top_level_path': '/ipns/ipfs.tech/'}
count 33 with_progress 33
with_parent 32
```

Decision: keep. This is harness-only, ABI-neutral, and improves the evidence
quality of future mobile progress and page-level latency experiments without
changing gateway retrieval semantics, cache behavior, or resource limits.

## 2026-05-05 Keep: Trace Summary Progress Correlation Fields

Motivation:
After adding harness-generated progress correlation headers, live traces could
be checked with one-off JSONL scripts, but the normal harness summary still
printed only gateway-local request IDs. That made page-level diagnosis too easy
to lose in long runs. The trace summary should surface the mobile correlation
fields directly in `slow_requests` and `slow_events`.

Implementation:

- Add `progress_request_id`, `parent_progress_request_id`, and `top_level_path`
  to serialized `slow_requests`.
- Print those fields in the slow-request console summary when present.
- Copy `progress_request_id`, `parent_request_id`, and `top_level_path` from
  trace spans into slow-event details.
- Extend the existing trace-summary test fixture to cover correlation fields.
- Update `docs/mobile-web-readiness/README.md`.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
git diff --check
```

Decision: keep. This is diagnostics-only and makes the previous correlation
header change useful in regular harness output and JSON reports, without
changing request behavior or gateway runtime paths.

## 2026-05-05 Keep: Page-Level Progress Request Groups

Motivation:
`slow_requests` shows individual gateway requests, but page loads are a tree:
root HTML, assets, CSS-discovered assets, and conditional revalidations. After
adding progress correlation headers and surfacing them in slow requests, the
next useful diagnostics step is a bounded page-level aggregate that says which
top-level navigation had slow or failed subrequests.

Implementation:

- Add serialized `progress_request_groups` to trace summaries.
- Group correlated requests by top-level path and root progress request ID.
- Count total requests, child requests, completed requests, failed requests,
  response statuses, and phases per group.
- Include request elapsed latency summaries, max event latency, and the slowest
  member requests for each group.
- Print the top groups in normal single-engine and Rust-vs-Kubo comparison
  console output.
- Add a focused synthetic trace test for one top-level page with child asset
  requests plus an independent second page.
- Update `docs/mobile-web-readiness/README.md`.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_groups_progress_correlated_requests
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-progress-groups-r2-trace.jsonl \
  --output /tmp/ipfs-tech-progress-groups-r2.json
git diff --check
```

Live result:

- `ipfs-tech-page-assets` passed `1/1`.
- Root TTFB was `509ms`; asset TTFB p50/p90/p95/max was
  `115/217/347/1226ms`.
- The JSON report contained one `progress_request_groups` entry for
  `/ipns/ipfs.tech/` with `root_progress_request_id=1`, `request_count=33`,
  `child_request_count=32`, `failed_request_count=0`, and max request elapsed
  `1224ms`.
- The slowest grouped member request was
  `/ipns/ipfs.tech/_nuxt/community-hero.Cp0BCcC7.jpg` at `1224ms`.
- The first live attempt caught a useful edge: root spans carry
  `parent_request_id=0`. The harness now treats that sentinel as "no parent",
  so root requests do not count as children and group under their real progress
  ID.

Decision: keep. This is diagnostics-only and makes page-level tail analysis
possible from normal JSON reports instead of one-off trace scripts.

## 2026-05-05 Observe: Progress-Grouped Rust-vs-Kubo Refresh

Motivation:
After adding page-level progress groups, rerun the main live comparison cases
to see whether the next optimization target is cold retrieval, warm-cache
latency, or sparse-provider reliability. These runs are evidence only; no
gateway or retrieval behavior changed.

Commands:

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/ipfs-tech-progress-groups-rust-vs-kubo-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-progress-groups-rust-vs-kubo.json

timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 90 \
  --run-timeout-secs 120 \
  --trace-output /tmp/vitalik-progress-groups-rust-vs-kubo-trace.jsonl \
  --comparison-output /tmp/vitalik-progress-groups-rust-vs-kubo.json

timeout 360s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --trace-output /tmp/daicowtf-progress-groups-rust-vs-kubo-trace.jsonl \
  --comparison-output /tmp/daicowtf-progress-groups-rust-vs-kubo.json
```

`ipfs-tech-page-assets`:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `20/581ms`, Kubo `2/2890ms`.
- Asset TTFB p50/p95: Rust `17/206ms`, Kubo `4/1365ms`.
- Max RSS/FD: Rust `50576KiB`/`33`, Kubo `233528KiB`/`178`.
- Rust grouped request elapsed:
  - cold group `root_progress_request_id=1`: `33` requests, `32` children,
    `failed=0`, p50/p90/p95/max `123/223/405/563ms`.
  - warm group `34`: p50/p90/p95/max `4/15/16/16ms`.
  - warm group `67`: p50/p90/p95/max `4/11/12/12ms`.
- Rust trace: delegated provider lookup max `116ms`, gateway request elapsed
  p50/p90/p95/max `7/150/204/563ms`, direct-body max `15ms`, Bitswap
  session shortcut hits `32/32`.

`vitalik-root-html-range`:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `7/1349ms`, Kubo `4/1930ms`.
- Max RSS/FD: Rust `38400KiB`/`22`, Kubo `123980KiB`/`50`.
- Rust grouped request elapsed:
  - cold group `1`: one request, max `1346ms`.
  - warm groups `2` and `3`: one request each, max `5ms` and `4ms`.
- Rust trace: delegated provider lookup max `48ms`; the cold Bitswap source
  peer delivered two blocks with max fetch `629ms`; warm range responses used
  the direct-body path with max `3ms`.

`daicowtf-page-assets`:

- Rust and Kubo both failed `0/3`.
- Root TTFB p50/p95: Rust `10902/20638ms`, Kubo `30002/30003ms`.
- Max RSS/FD: Rust `53120KiB`/`17`, Kubo `149160KiB`/`127`.
- Rust statuses were `504`, `502`, `504`; Kubo timed out with `504` in all
  three runs.
- Rust grouped request elapsed showed one failed root request per run:
  `20636ms` (`502`), `10899ms` (`504`), and `10029ms` (`504`).
- Rust trace showed delegated routing returning only `1` provider total across
  `5` delegated lookup events, DHT provider lookup timing out `3` times,
  Bitswap shortcut misses `3/3`, incoming stream read timeouts `2`, and one
  provider refresh after failure.

Conclusion:

- Current Rust is already materially better than Kubo on cold p95 and resource
  use for `ipfs.tech` and `vitalik` in this same-window sample.
- Kubo still wins the warm-cache p50 by a few milliseconds (`2-4ms` versus
  Rust `7-20ms`), so the next speed work should focus on warm gateway/local
  response overhead only if that margin matters more than reliability work.
- `daicowtf` remains a sparse/stale-provider reliability case. Because Kubo
  also failed, this is not a parity blocker, but it is the best current target
  for provider-diversity and DHT fallback experiments. Any fix should preserve
  the low RSS/FD profile and avoid public gateway fallback.

## 2026-05-05 Keep: Provider-Diversity-Low Trace Summary

Motivation:
The `daicowtf` comparison above made the next reliability target obvious, but
the normal harness report still exposed `provider_diversity_low` mostly through
progress phase counts and truncated trace errors. The raw trace already has
structured fields for provider counts, Bitswap provider diversity, DHT fallback
counts, fallback labels, and timeout caps. The harness should summarize those
directly before any provider-diversity experiments.

Implementation:

- Add `provider_diversity_low` to serialized trace summaries.
- Count events and `ok=false` failures.
- Sum and max `provider_count`, `bitswap_provider_count`, and
  `dht_provider_count`.
- Track max `timeout_ms` and fallback labels such as `light_dht`.
- Print the summary in both single-engine and Rust-vs-Kubo comparison output.
- Extend the trace-summary fixture with three representative
  `provider_diversity_low` events, including the `ok=false` timeout shape from
  the `daicowtf` trace.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
git diff --check
```

Decision: keep. This is diagnostics-only and makes the next sparse-provider
experiment measurable without changing routing, Bitswap, gateway behavior,
cache semantics, or public gateway policy.

## 2026-05-05 Keep: DHT Provider Lookup Trace Summary

Motivation:
The low-diversity summary shows when fallback was attempted, but sparse-provider
experiments also need a direct DHT signal: did light DHT find providers, how
long did it run, and did it fail or time out? Previously that had to be inferred
from generic retrieval-level `provider_lookup` errors.

Implementation:

- Emit `dht_provider_lookup` from `LightDhtClient::providers` with `ok`,
  provider count, configured max providers, query timeout, elapsed time, and
  sanitized error on failure.
- Add `dht_provider_lookup` to serialized harness trace summaries.
- Print DHT lookup count, successes/failures, total providers, max provider cap,
  max timeout cap, and max elapsed time in single-engine and Rust-vs-Kubo
  output.
- Extend the trace-summary fixture with one failed and one successful
  `dht_provider_lookup` event.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p freedom-ipfs-routing observed_light_dht_records_provider_lookup_stats
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p freedom-ipfs-routing -p mobile-web-harness --all-targets -- -D warnings
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 120 \
  --trace-output /tmp/daicowtf-dht-provider-lookup-trace.jsonl \
  --output /tmp/daicowtf-dht-provider-lookup.json
git diff --check
```

Live result:

- The `daicowtf-page-assets` smoke failed as expected with root `504` after
  `27044ms`.
- The new DHT summary was present:
  `events=2 successes=1 failures=1 providers=0 max_providers=4
  max_timeout_ms=10000 max_elapsed_ms=10010`.
- The low-diversity summary showed `events=2 failures=1 providers_total=1`,
  `dht_providers_total=0`, and `fallbacks=light_dht=2`.
- The grouped request summary isolated the single failed root request, and the
  slow-CID summary identified child CID
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u` as the
  timeout source.

Decision: keep. This is diagnostics-only and does not change provider
selection, DHT timeout policy, retrieval behavior, cache semantics, or public
gateway policy.

## 2026-05-05 Keep: Trace Cancelled Low-Diversity DHT Fallbacks

Motivation:
The new `dht_provider_lookup` summary captures full light-DHT provider queries,
but low-diversity fallback queries can be cancelled by the short `750ms`
fallback cap before `LightDhtClient::providers` returns. That means the
low-diversity summary could report a fallback timeout while the DHT summary did
not count the attempted lookup. Sparse-provider experiments need those counts
to line up.

Implementation:

- When the low-diversity light-DHT fallback hits its outer timeout, emit a
  `dht_provider_lookup` event with `ok=false`, `cancelled=true`,
  `fallback=light_dht`, the fallback timeout cap, the configured full DHT query
  timeout, provider cap, elapsed time, and a sanitized error string.
- Keep successful and normally failed full DHT lookups unchanged.
- Extend the harness trace-summary fixture so cancelled fallback lookup events
  count as DHT lookup failures and appear in trace errors.
- Add `max_query_timeout_ms` to the harness DHT lookup summary so reports can
  distinguish the short fallback cap from the full configured DHT query budget.
- Add a deterministic routing test that proves low delegated diversity returns
  the delegated provider after the short DHT fallback cap instead of waiting for
  the full DHT query timeout.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p freedom-ipfs-routing observed_light_dht_records_provider_lookup_stats
cargo test -p freedom-ipfs-routing auto_routing_bounds_low_diversity_dht_fallback_timeout
FREEDOM_IPFS_LIVE_DHT_CID=bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u \
  timeout 90s cargo test -p freedom-ipfs-routing --lib \
  live_light_dht_finds_public_providers -- --ignored --nocapture
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 120 \
  --trace-output /tmp/daicowtf-dht-cancelled-lookup-trace.jsonl \
  --output /tmp/daicowtf-dht-cancelled-lookup.json
```

Validation results:

- Focused deterministic tests passed.
- The standalone public DHT probe for failing child CID
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u` found `0`
  providers after about `12.45s`; the ignored smoke test failed its non-empty
  assertion, which is useful evidence for this CID rather than a kept gate.
- The live `daicowtf-page-assets` smoke failed as expected with root `504` in
  `10165ms`, RSS `43008KiB`, and FD count `16`.
- That live window did not exercise the short low-diversity cancellation path:
  delegated routing returned `2` providers across two lookups, then the failing
  child CID fell through to a full DHT lookup that timed out after `10010ms`.
  The DHT summary was therefore `events=1 successes=0 failures=1 providers=0`.

Decision: keep. This is diagnostics-only and makes future low-diversity
fallback traces internally consistent without changing provider selection,
fallback timing, retrieval behavior, cache semantics, or public gateway policy.

## 2026-05-05 Observe: 3s DHT Query Cap Smoke

Hypothesis:
The latest `daicowtf` sparse-provider trace showed the failing child CID had
zero delegated providers, a failed recent-peer shortcut, and then a full
`10s` light-DHT provider lookup that found no providers. A lower DHT query cap
might improve mobile failure latency for sparse/stale CIDs while leaving normal
delegated-router wins untouched.

Commands:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 120 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-dht3-trace.jsonl \
  --output /tmp/daicowtf-dht3.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-dht3-trace.jsonl \
  --output /tmp/vitalik-dht3.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-dht3-trace.jsonl \
  --output /tmp/ipfs-tech-dht3.json
```

Results:

- `daicowtf-page-assets` still failed as expected with root `504`, but failure
  latency dropped to `3948ms` versus the previous `10165ms` `10s`-cap smoke.
  The DHT summary showed `events=2 successes=0 failures=2 providers=0`,
  `max_timeout_ms=3000`, `max_query_timeout_ms=3000`, and max elapsed `3009ms`.
  One event was the short low-diversity fallback cancellation; the other was
  the full delegated-empty child lookup.
- `vitalik-root-html-range` passed `1/1` with root TTFB `413ms`, RSS
  `37632KiB`, FD count `17`, and no DHT provider lookup events. Delegated
  provider lookup returned `41` providers with max elapsed `52ms`.
- `ipfs-tech-page-assets` passed `1/1`; root TTFB was `1057ms`, asset
  p50/p95/max was `202/703/703ms`, RSS was `51516KiB`, and FD count was `48`.
  The trace had no DHT provider lookup events; delegated provider lookups
  returned `631` providers with max elapsed `62ms`.

Decision:
Observe only. The `3s` cap is promising for known delegated-empty sparse/stale
failures and did not affect two delegated-heavy success paths in this small
sample, but one-run live evidence is not enough to lower the global DHT query
default. A safer future behavior experiment would be adaptive: use a shorter
DHT budget for page child CIDs when delegated routing returns empty and there is
already a recent page-session peer attempt, while preserving the longer full
DHT budget for explicit light-DHT routing and cases that genuinely depend on
public DHT provider discovery.

Same-window Rust/Kubo repeat with the `3s` DHT cap:

```sh
timeout 360s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-dht3-kubo-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-dht3-kubo-r3.json
```

Result:

- Rust and Kubo both failed `0/3`, so this does not change the sparse-provider
  reliability conclusion.
- Rust root TTFB p50/p95 was `3909/12070ms`; Kubo root TTFB p50/p95 was
  `30003/30004ms`.
- Rust max RSS/FD was `50668KiB`/`17`; Kubo max RSS/FD was `160608KiB`/`202`.
- Rust DHT lookup summary showed `events=5 successes=1 failures=4 providers=0`,
  `max_timeout_ms=3000`, and `max_query_timeout_ms=3000`.
- The slower Rust sample exposed the next tail after the DHT cap: the failing
  child CID first timed out the recent-peer shortcut after `2001ms`, then a
  delegated-empty/full-DHT lookup returned no providers after about `3012ms`,
  then the same session-only peer was tried again in the provider fetch path and
  hit a `6001ms` Bitswap stream read timeout before provider refresh tried DHT
  once more. That duplicate session-peer retry is a better next hypothesis than
  simply lowering DHT further.

## 2026-05-05 Keep: Avoid Duplicate Sparse-Provider Retry Waits

Hypothesis:
When a child CID has only one recent page-session Bitswap peer and delegated/DHT
provider lookup returns empty, the retriever was spending mobile-visible time on
two duplicate waits:

- retrying the same single session peer after the `2s` session shortcut already
  timed it out;
- immediately refreshing providers after an empty initial provider set produced
  `NoHttpProviders`/`NoBitswapProviders`, which repeats the same delegated/DHT
  lookup and adds another DHT cap.

Implementation:

- A `bitswap_session_shortcut` timeout now marks exactly one attempted session
  peer as temporarily bad for the existing bad-provider TTL, preventing the
  provider fetch path from re-adding that same stale peer immediately.
- Broad shortcut timeouts with more than one attempted peer are traced as
  `bitswap_peer_timeout_suppressed` but do not mass-suppress every candidate.
- If the initial provider set is empty and provider fetching returns a no-provider
  error, the retriever emits `provider_refresh_skipped_empty_provider_set` and
  returns that error instead of doing an immediate provider refresh.
- The mobile progress mapper and harness summary map
  `provider_refresh_skipped_empty_provider_set` to `failed` and include a
  provider-retry aggregate count for skipped empty-provider refreshes.

Experiment commands:

```sh
timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-session-timeout-suppress-r3-trace.jsonl \
  --output /tmp/daicowtf-session-timeout-suppress-r3.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-session-timeout-suppress-skip-refresh-r3-trace.jsonl \
  --output /tmp/daicowtf-session-timeout-suppress-skip-refresh-r3.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-session-timeout-suppress-skip-refresh-trace.jsonl \
  --output /tmp/vitalik-session-timeout-suppress-skip-refresh.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-timeout-suppress-skip-refresh-trace.jsonl \
  --output /tmp/ipfs-tech-session-timeout-suppress-skip-refresh.json

timeout 360s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-session-timeout-suppress-skip-refresh-kubo-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-session-timeout-suppress-skip-refresh-kubo-r3.json
```

Results:

- Session-peer suppression alone was the wrong half-step: `daicowtf-page-assets`
  still failed `0/3` and root p50/p95/max was `6044/6055/6055ms` because the
  empty-provider refresh added another DHT wait.
- Combining session-peer suppression with the empty-provider refresh skip still
  failed `0/3`, but root p50/p95/max improved to `3035/3940/3940ms`, with max
  RSS `48768KiB` and FD count `17`.
- `vitalik-root-html-range` passed `1/1` with root TTFB `473ms`, RSS
  `37632KiB`, FD count `18`, no DHT provider lookups, and delegated provider
  lookup max `53ms`.
- `ipfs-tech-page-assets` passed `1/1`; root TTFB was `4194ms`, asset
  p50/p95/max was `187/462/487ms`, RSS was `50296KiB`, and FD count was `41`.
  The slow root was a `3558ms` Bitswap root-block fetch; the new sparse-provider
  branches were not involved.
- Same-window Rust/Kubo repeat with both changes still failed `0/3` on both
  engines. Rust root TTFB p50/p95 was `794/7682ms`; Kubo root TTFB p50/p95 was
  `30004/30005ms`. Rust max RSS/FD was `46848KiB`/`32`; Kubo max RSS/FD was
  `153348KiB`/`165`. The first Rust run still hit a stale root provider timeout,
  but later attempts skipped it quickly.

Decision: keep. This is a narrow sparse-provider tail-latency fix. It does not
add public gateway fallback, change block verification, serve unverified data,
or lower the global DHT query default. The known remaining gap is reliability:
`daicowtf` still fails when provider discovery is empty or stale, and the first
stale provider for a root CID can still cost about one Bitswap stream timeout.
Future work should explore adaptive stale-provider suppression, bounded
multi-source racing, and richer provider discovery rather than further lowering
global DHT timeouts.

Deterministic validation:

- `cargo fmt --all --check`
- `cargo test -p freedom-ipfs-retrieval session_shortcut_timeout`
- `cargo test -p freedom-ipfs-retrieval identifies_no_provider_errors_for_empty_lookup_retry_skip`
- `cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states`
- `cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases`
- `cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts`
- `cargo test -p freedom-ipfs-retrieval`
- `cargo test -p freedom-ipfs-mobile`
- `cargo test -p mobile-web-harness`
- `cargo check --workspace --all-targets`
- `cargo clippy -p freedom-ipfs-retrieval -p freedom-ipfs-mobile -p mobile-web-harness --all-targets -- -D warnings`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

## 2026-05-05 Keep: Shorter Direct Read Cap For Single Untrusted Bitswap Provider

Hypothesis:
The same-window `daicowtf` Rust/Kubo run showed a remaining first-request tail:
when delegated routing returned exactly one untrusted public Bitswap provider for
the root CID, that provider opened a stream but never returned the block, costing
one full `6000ms` Bitswap stream read timeout before temporary peer suppression
made later attempts fail quickly. This is a mobile-visible cold-load tail and is
low-value to wait out when there is only one untrusted candidate.

Implementation:

- Keep the existing `6000ms` stream read timeout for trusted/session peers and
  multi-peer provider races.
- Use a `3000ms` stream read timeout only when the outgoing Bitswap request has
  exactly one candidate and that candidate is not trusted from recent successful
  session history.
- Add `stream_read_timeout_ms` to Bitswap attempt/request-timeout traces and the
  harness slow-event details so the chosen cap is visible in live traces.
- Extend the deterministic provider-refresh test so a silent single untrusted
  peer must refresh to a new provider before the old default stream timeout.

Experiment commands:

```sh
cargo test -p freedom-ipfs-retrieval \
  provider_refresh_after_bitswap_timeout_uses_new_peer -- --nocapture

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-single-untrusted-read3-r3-trace.jsonl \
  --output /tmp/daicowtf-single-untrusted-read3-r3.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-single-untrusted-read3-trace.jsonl \
  --output /tmp/vitalik-single-untrusted-read3.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-single-untrusted-read3-trace.jsonl \
  --output /tmp/ipfs-tech-single-untrusted-read3.json
```

Results:

- Deterministic local test passed in `3.26s`, proving a silent single untrusted
  provider can be marked bad and refreshed to a new provider before the old
  default `6000ms` stream-read cap.
- `daicowtf-page-assets` still failed `0/3` with root p50/p95/max
  `3033/3892/3892ms`, RSS max `48640KiB`, and FD max `22`. This live window did
  not exercise the new direct-read cap: the root block arrived from the single
  public provider in `61ms`, and failures remained the child CID's empty
  delegated/DHT provider path.
- `vitalik-root-html-range` passed `1/1` with root TTFB `659ms`, RSS
  `38144KiB`, FD count `18`, no DHT provider lookups, and delegated lookup max
  `48ms`.
- `ipfs-tech-page-assets` passed `1/1`; root TTFB was `533ms`, asset
  p50/p95/max was `122/226/399ms`, RSS was `49604KiB`, and FD count was `30`.

Decision: keep. This is narrower than reducing the global Bitswap stream read
timeout: it applies only to one untrusted candidate, the case that cannot benefit
from peer diversity and produced the observed `6000ms` stale-provider tail.
Trusted/session peers and multi-provider races keep the existing cap. The known
risk is a rare single public provider that would return a block between `3000ms`
and `6000ms`; the mobile tradeoff favors a faster retry/failure over waiting
for a lone untrusted public peer that has already stalled.

Deterministic validation:

- `cargo fmt --all --check`
- `cargo test -p freedom-ipfs-retrieval`
- `cargo test -p mobile-web-harness`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

## 2026-05-05 Keep: Short Negative Provider Cache

Hypothesis:
After the empty-provider refresh skip, repeated requests for the same sparse
child CID still paid repeated delegated/DHT lookup cost because empty provider
sets were not cached. This showed up in `daicowtf-page-assets`: the failing child
CID could repeat a `3000ms` DHT lookup on immediate retries even though the
previous lookup had just found no providers.

Implementation:

- Allow the provider cache to store an empty provider list.
- Cache empty provider lookups for `30s`, while keeping non-empty provider
  records at the existing `5min` TTL.
- Treat `provider_cache cache_hit=true provider_count=0` as a failed/no-provider
  progress phase in mobile and harness summaries.
- Keep this scoped to provider discovery only: it does not serve content,
  bypass verification, add public gateway fallback, or cache failed block bytes.

Experiment commands:

```sh
cargo test -p freedom-ipfs-store caches_empty_provider_records_until_ttl_expires
cargo test -p freedom-ipfs-retrieval empty_provider_lookups_are_cached_briefly
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-negative-provider-cache-r3-trace.jsonl \
  --output /tmp/daicowtf-negative-provider-cache-r3.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-negative-provider-cache-trace.jsonl \
  --output /tmp/vitalik-negative-provider-cache.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-negative-provider-cache-trace.jsonl \
  --output /tmp/ipfs-tech-negative-provider-cache.json
```

Results:

- Deterministic store/retrieval/mobile/harness focused tests passed. The
  retrieval test proves the second same-CID empty provider miss does not call
  delegated routing again.
- `daicowtf-page-assets` still failed `0/3`, but the third immediate retry used
  the cached empty provider result and failed in `3ms` instead of repeating
  another `3000ms` DHT lookup. Root p50/p95/max was `3028/4000/4000ms`, RSS max
  `47872KiB`, FD max `17`. DHT provider lookup events fell to `3` across the
  three runs; the third request had `provider_cache=1` and no provider/DHT
  lookup.
- `vitalik-root-html-range` passed `1/1` with root TTFB `2033ms`, RSS
  `38656KiB`, FD count `20`, no DHT provider lookups, and delegated lookup max
  `57ms`.
- `ipfs-tech-page-assets` passed `1/1`; root TTFB was `2580ms`, asset
  p50/p95/max was `182/2043/2214ms`, RSS was `51740KiB`, and FD count was `44`.
  This was a slow network window with many successful Bitswap fetches; there
  were no DHT provider lookups and no failed requests.

Decision: keep. A short negative provider cache is a resource and latency win
for immediate retries/page reloads against sparse CIDs. The `30s` TTL limits the
risk that newly appearing provider records remain hidden for too long, while
removing repeated DHT work within the same user-visible failure window.

Deterministic validation:

- `cargo fmt --all --check`
- `cargo test -p freedom-ipfs-store`
- `cargo test -p freedom-ipfs-retrieval`
- `cargo test -p freedom-ipfs-mobile`
- `cargo test -p mobile-web-harness`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

## 2026-05-05 Keep: Concurrent Bitswap DNS Prefetch

Hypothesis:
`bitswap_peer_expand` was still a visible latency tail on public network reads
because provider address DNS work happened serially during the per-provider
quality pass. In the live traces before this experiment,
`vitalik-root-html-range` spent `832ms` in its first peer expansion and
`ipfs-tech-page-assets` hit a `1253ms` max peer expansion. Resolving unique
`/dnsaddr` and `/dns*` multiaddr hosts concurrently before the existing quality
pass should cap that tail without changing which peers are eligible.

Implementation:

- Prefetch unique provider `/dnsaddr` TXT hosts before the Bitswap candidate
  quality loop.
- Prefetch unique DNS multiaddr host IPs from the original provider addrs plus
  resolved dnsaddr records.
- Keep the same Cloudflare DoH resolver and the same expansion/filtering logic
  used by the old path.
- Bound prefetch fanout with `BITSWAP_DNS_PREFETCH_CONCURRENCY = 8`.
- Feed prefetched results through the existing `expand_provider_multiaddrs`
  cache path, while logging first-use expansion events as non-cached so trace
  summaries still read naturally.
- Add `bitswap_dns_prefetch` as a provider-lookup progress phase in the mobile
  progress snapshot and mobile web harness trace summary.

Experiment commands:

```sh
cargo test -p freedom-ipfs-retrieval cached_dns_expansion_reuses_dnsaddr_and_ip_results
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 90 \
  --run-timeout-secs 90 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-dns-prefetch-trace.jsonl \
  --output /tmp/vitalik-dns-prefetch.json

timeout 240s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-dns-prefetch-trace.jsonl \
  --output /tmp/ipfs-tech-dns-prefetch.json
```

Results:

- Focused retrieval/mobile/harness tests passed.
- `vitalik-root-html-range` passed `1/1`; root TTFB was `474ms`, RSS was
  `37376KiB`, FD count was `18`, delegated provider lookup max was `124ms`, and
  there were no DHT provider lookups. `bitswap_peer_expand` ran once at `172ms`;
  the new `bitswap_dns_prefetch` event took `142ms`.
- `ipfs-tech-page-assets` passed `1/1`; root TTFB was `2318ms`, asset
  p50/p95/max was `122/919/941ms`, RSS was `51940KiB`, and FD count was `41`.
  There were no DHT provider lookups. `bitswap_peer_expand` ran `7` times with
  max `128ms`; `bitswap_dns_prefetch` ran `4` times with max `100ms`.

Decision: keep. This is a behavior-preserving latency reduction for provider
address expansion. The work is read-only, bounded, uses the existing verified
retrieval path after candidate selection, does not add public gateway fallback,
and does not serve or cache unverified bytes. The live results are not perfectly
A/B comparable because the public network varied, but they show the intended
effect: DNS expansion is moved into a bounded concurrent phase and
`bitswap_peer_expand` no longer dominates the observed page-load tail.

Deterministic validation:

- `cargo fmt --all --check`
- `cargo test -p freedom-ipfs-retrieval`
- `cargo test -p freedom-ipfs-mobile`
- `cargo test -p mobile-web-harness`
- `cargo check --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `git diff --check`

## 2026-05-05 Reject: Direct Untrusted WANT_BLOCK Cap 4

Hypothesis:
The latest same-window `ipfs-tech-page-assets` run after DNS prefetch showed the
slow cold root/index child block arriving from an untrusted provider that was
behind the `WANT_HAVE` boundary. Increasing
`MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS` from `3` to `4` might move that
provider into the direct `WANT_BLOCK` set and reduce the cold raw-block tail.

Prototype:

- Temporarily set `MAX_BITSWAP_DIRECT_WANT_BLOCK_UNTRUSTED_PEERS = 4`.
- Update the deterministic Bitswap mode-boundary tests so four untrusted peers
  receive direct `WANT_BLOCK` and the next peer still exercises `WANT_HAVE`.
- Keep existing peer, dial, timeout, verification, and cache semantics
  unchanged.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval multi_peer_bitswap_directs_first_untrusted_then_uses_want_have
cargo test -p freedom-ipfs-retrieval want_have_probe_falls_back_to_want_block_quickly
cargo test -p freedom-ipfs-retrieval formats_bitswap_peer_timeout_summary
cargo test -p freedom-ipfs-retrieval
```

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-direct4-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-direct4-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `20/2737ms`, Kubo `3/3058ms`.
- Asset TTFB p50/p95: Rust `13/767ms`, Kubo `3/209ms`.
- Gateway request elapsed p50/p90/p95/max: `6/198/1011/2718ms`.
- Bitswap peer attempts increased to `155` starts, compared with `127` in the
  preceding cap-3 DNS-prefetch comparison.
- Direct `WANT_HAVE` probes disappeared (`prefer_want_have=0`) and verified
  extra-block reuse collapsed to `2` total extra blocks, compared with `54` in
  the preceding cap-3 run.
- RSS/FD remained mobile-friendly at `51356KiB`/`44`, but this did not offset
  the asset and gateway elapsed tail regression.

Decision: reject and revert. Cap 4 made the page workload more aggressive but
less useful: more peer attempts, fewer extra blocks, worse asset p95, and worse
gateway elapsed p95/max. Keep the cap at `3`; future direct-fanout experiments
should be conditional on stronger peer quality evidence instead of simply
raising the global direct untrusted budget.

## 2026-05-05 Keep: Bitswap Source Request Mode Summary

Motivation:
The rejected cap-4 experiment required custom trace parsing to answer a basic
question: when a successful Bitswap block arrives from `source_peer`, was that
peer originally asked with direct `WANT_BLOCK` or with `WANT_HAVE` first? Future
fanout/session experiments need that answer in the normal harness summary.

Implementation:

- Track `bitswap_peer_attempt_start` by `(cid, peer)` and remember whether the
  attempt used `want_block` or `want_have`.
- For successful `bitswap_fetch` and `bitswap_session_shortcut` events, map
  `(cid, source_peer)` back to that request mode.
- Add serialized `trace_summary.bitswap_source_request_modes`.
- Print `bitswap source request modes: ...` in comparison trace summaries.
- Use `unknown` when a source peer cannot be correlated to a prior attempt,
  preserving robustness for partial traces or future trace shape changes.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
```

Result: both passed. This is diagnostics-only; it does not change gateway,
retrieval, network, verification, caching, or serving behavior.

## 2026-05-05 Keep: Throttle Hot-Cache SQLite Touches

Hypothesis:
Warm same-process page loads still spend local time in `block_store_get` even
when blocks are served from the in-memory hot cache. The store updated
`blocks.last_accessed_at` in SQLite on every hot-cache hit, which adds lock and
write work directly on the warm gateway path. Throttling those persistent LRU
touches should reduce warm-path overhead while keeping long-session eviction
metadata fresh enough.

Implementation:

- Add `HOT_CACHE_TOUCH_INTERVAL = 30s`.
- Track `last_persistent_touch` per hot-cache entry.
- On hot-cache hits, verify the hot block as before, but skip the SQLite
  `last_accessed_at` update until the entry has not refreshed persistent LRU
  metadata for at least `30s`.
- Keep SQLite touches on cold cache reads and block writes.
- Keep eviction, block verification, retention, trimming, and cache clearing
  semantics unchanged.

Validation and live run:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-store

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-hot-touch-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-hot-touch-rust-vs-kubo-r3.json
```

Result:

- Store tests passed, including focused tests proving repeated hot hits skip the
  SQLite touch and aged hot entries still refresh persistent LRU metadata.
- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `22/787ms`, Kubo `3/2351ms`.
- Asset TTFB p50/p95: Rust `17/235ms`, Kubo `3/149ms`.
- Gateway request elapsed p50/p90/p95/max: `11/163/234/772ms`.
- RSS/FD stayed bounded at `49600KiB`/`28`.
- Compared with the preceding cap-3/source-mode run
  `/tmp/ipfs-tech-source-modes-rust-vs-kubo-r3.*`, block-store cache-hit elapsed
  improved from p50/p90/p95/max `1/15/17/26ms` to `0/10/13/15ms`. Cache-hit
  events with nonzero elapsed fell from `84/144` to `72/144`.

Decision: keep. This is a narrow warm-path local-store optimization: it removes
redundant SQLite writes during hot same-process reloads, preserves verified
block serving, and still periodically refreshes persistent LRU timestamps for
long sessions. The live TTFB numbers remain noisy, but the trace-level
`block_store_get` improvement is directly on the intended path and resource
usage did not regress.

## 2026-05-05 Keep: Print Slow Trace Details In Kubo Comparisons

Hypothesis:
The trace summary already records slow CIDs, slow gateway requests, and slow
events, but the Rust-vs-Kubo comparison console output did not print those
details. That forced each optimization pass to re-run ad hoc JSON analysis after
a comparison run. Printing the existing slow trace details in comparison output
should make the next bottleneck visible directly in the standard benchmark loop.

Implementation:

- Factor the existing slow CID/request/event printer into one helper.
- Reuse it from both normal harness summaries and Rust-vs-Kubo comparison trace
  summaries.
- Keep output-only behavior; no retrieval, gateway, cache, network, or result
  schema semantics change.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result: all passed.

Decision: keep. This is diagnostics-only and makes same-window Kubo comparisons
more actionable by surfacing the specific slow paths/CIDs/events that drive the
aggregate latency numbers.

## 2026-05-05 Keep: Rank Successful Bitswap Peers By Latency

Hypothesis:
The recent successful Bitswap peer cache was ordered only by recency. In the
`ipfs.tech` comparison, several cold asset requests were delayed because a
recent but slow peer stayed on the trusted/session path. Recording the last
successful fetch latency and preferring lower-latency successful peers should
reduce page-asset tail latency without raising concurrency, fanout, or timeouts.

Implementation:

- Store `last_latency` alongside each successful Bitswap peer.
- Sort successful provider candidates and recent session-only peers by lowest
  last latency first, then most recent as the tie-breaker.
- Keep the existing successful-peer TTL, peer caps, WANT_BLOCK shortcut
  behavior, bad-peer suppression, and block verification semantics.
- Add deterministic tests for latency ordering in provider-candidate scoring
  and recent session shortcut peer selection.

Baseline:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-slow-details-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-slow-details-rust-vs-kubo-r3.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `19/3177ms`, Kubo `2/2717ms`.
- Asset TTFB p50/p95: Rust `14/1771ms`, Kubo `3/193ms`.
- Rust RSS/FD: `53980KiB`/`43`.
- Trace showed slow cold assets using recent trusted Bitswap peers with
  `bitswap_session_shortcut_post_lookup_wait` and slow `bitswap_fetch` wins.

Experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-latency-ranked-peers-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-latency-ranked-peers-rust-vs-kubo-r3.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `19/2226ms`, Kubo `3/1315ms`.
- Asset TTFB p50/p95: Rust `17/719ms`, Kubo `5/131ms`.
- Rust RSS/FD: `51116KiB`/`47`.
- Bitswap peer attempts fell from `271` to `209`; dial-plan peer targets fell
  from `271` to `209`; incoming max oldest pending wait fell from `1902ms` to
  `905ms`.

Additional live checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-latency-ranked-peers-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-latency-ranked-peers-rust-vs-kubo-r3.json
```

Result: inconclusive for regression, because Rust and Kubo both failed `3/3` in
the same network window. Rust root TTFB p50 was `3035ms` versus Kubo `30004ms`,
but the case is not counted as a keep/revert signal.

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-latency-ranked-peers-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-latency-ranked-peers-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `7/1814ms`, Kubo `4/1998ms`.
- Rust RSS/FD: `38528KiB`/`23`; Kubo RSS/FD: `175216KiB`/`83`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result: all passed.

Decision: keep. This is a narrow session-ranking change that directly targets
the observed slow-peer reuse, improves the primary `ipfs.tech` asset tail
without more concurrency or larger resource use, and does not change serving,
caching, verification, or fallback semantics.

## 2026-05-05 Keep: Shorten Bitswap Session Post-Lookup Grace To 100ms

Hypothesis:
After provider lookup finishes, the retrieval path waits briefly for an already
running recent-peer session shortcut before using the provider set. With
latency-ranked session peers, the old `200ms` post-lookup grace still appeared
in several slow cold `ipfs.tech` asset requests. Reducing that grace should cut
tail latency while still giving fast recent peers a chance to win.

Rejected sub-experiment:

- `50ms` was too aggressive. `cargo test -p freedom-ipfs-retrieval` failed
  `recent_bitswap_peer_can_win_after_fast_provider_lookup`, proving the window
  was too short for the deterministic local fast-session case.
- `75ms` was also too aggressive. The focused command
  `cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_can_win_after_fast_provider_lookup`
  failed the same local fast-session case, so it was reverted without live
  benchmarking.

Implementation:

- Change `BITSWAP_SESSION_POST_LOOKUP_GRACE` from `200ms` to `100ms`.
- Keep `BITSWAP_SESSION_SHORTCUT_TIMEOUT`, successful-peer TTL, peer caps,
  bad-peer suppression, provider lookup behavior, block verification, and
  timeout caps unchanged.

Baseline:

The latency-ranked-peer run immediately before this change:

- `/tmp/ipfs-tech-latency-ranked-peers-rust-vs-kubo-r3.json`
- `/tmp/ipfs-tech-latency-ranked-peers-rust-vs-kubo-r3-trace.jsonl`

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `19/2226ms`, Kubo `3/1315ms`.
- Asset TTFB p50/p95: Rust `17/719ms`, Kubo `5/131ms`.
- Rust RSS/FD: `51116KiB`/`47`.
- Bitswap peer attempts: `209`; dial-plan peer targets: `209`.

Experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-post-lookup-100ms-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-post-lookup-100ms-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `18/1547ms`, Kubo `3/4922ms`.
- Asset TTFB p50/p95: Rust `14/612ms`, Kubo `2/449ms`.
- Rust RSS/FD: `50984KiB`/`48`.
- Bitswap peer attempts fell from `209` to `155`; dial-plan peer targets fell
  from `209` to `155`; incoming max oldest pending wait fell from `905ms` to
  `837ms`.

Additional live check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-post-lookup-100ms-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-post-lookup-100ms-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `7/1360ms`, Kubo `4/2033ms`.
- Rust RSS/FD: `38272KiB`/`20`; Kubo RSS/FD: `110092KiB`/`48`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result: all passed.

Decision: keep. `100ms` retains the deterministic fast-session behavior while
cutting the cold asset tail and reducing Bitswap attempt pressure in the primary
comparison case.

## 2026-05-05 Reject: Make `cid.contact` A Default Delegated Router

Hypothesis:
The routing layer can already query multiple delegated routing endpoints and
merge low-diversity results. Adding `cid.contact` alongside
`delegated-ipfs.dev` might improve provider diversity and reduce Bitswap tails
without changing verification or adding gateway fallback.

Experiment command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --trace-output /tmp/ipfs-tech-dual-delegated-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-dual-delegated-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `24/2181ms`, Kubo `3/11841ms`.
- Asset TTFB p50/p95: Rust `13/489ms`, Kubo `4/486ms`.
- Rust RSS/FD: `57256KiB`/`49`.
- Bitswap peer attempts rose to `224` from `155` in the preceding 100ms
  post-lookup-grace run.
- The trace summary showed completed delegated lookup events only for
  `https://delegated-ipfs.dev/routing/v1`; `cid.contact` did not contribute
  visible completed results in this run before the first endpoint satisfied the
  routing policy.

Decision: reject as a default change for now. The asset p95 was better, but root
tail and resource pressure worsened, and there was no trace evidence that the
second endpoint materially contributed. Keep multi-endpoint routing available as
a CLI/mobile override for further provider-quality sweeps rather than changing
the default.

## 2026-05-05 Baseline: `ipfs.tech` Offline Replay Passes After One Online Load

Purpose:
Check cache completeness for the product question: after loading a page online,
what can a cache-only gateway replay? This uses the existing harness offline
replay mode and rewrites the observed `/ipns/ipfs.tech/` resolution to the
immutable `/ipfs/...` target before the offline pass, so the result measures
block cache completeness rather than online IPNS availability.

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --offline-replay \
  --offline-replay-resolved-ipfs \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-offline-replay-trace.jsonl \
  --output /tmp/ipfs-tech-offline-replay.json
```

Result:

- Online pass: `1/1`.
- Offline pass: `1/1`.
- The harness rewrote `/ipns/ipfs.tech/` to
  `/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/`.
- Offline replay reported `missing_urls=0`.
- Offline statuses: `200=27`, `206=6`.
- Offline progress phases: `streaming=230`, `completed=33`, `queued=33`,
  `started=33`.

Decision: keep as baseline evidence. For this page, the current online load
caches enough verified blocks for root plus discovered JS/CSS/image range assets
to replay offline through the cache-only gateway when the mutable name is
rewritten to the observed immutable root.

## 2026-05-05 Baseline: `ipfs.tech` Hero Image Range Versus Kubo

Purpose:
Measure the media/range workload for a real `206` response after the kept
Bitswap peer ranking and `100ms` post-lookup grace changes. This case fetches
the `ipfs.tech` developers hero JPEG with a range request, so it isolates the
UnixFS path, file-size discovery, range serving, and warm-cache behavior more
directly than the full page-assets case.

Command:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-hero-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-hero-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Range TTFB p50/p95: Rust `79/689ms`, Kubo `3/2667ms`.
- Rust RSS/FD: `39256KiB`/`18`; Kubo RSS/FD: `136860KiB`/`44`.
- Rust gateway statuses: `206=3`.
- Rust gateway direct-body events: `3`, total body bytes `12288`, max body
  length `4096`, max elapsed `12ms`.
- Rust Bitswap peer attempts: `7`; successful source request modes:
  `want_block=3`.
- Rust incoming Bitswap matched blocks: `3`, bytes `195392`, max oldest pending
  wait `170ms`.

Trace observations:

- First cold request took `686ms`; the two warm repeat range requests took
  `14ms` each.
- The first request fetched three blocks: the `ipfs.tech` root, the `_nuxt`
  directory, and the raw JPEG block.
- Slowest phases were `unixfs_file_size` and `unixfs_resource` at `597ms`,
  followed by root `block_fetch_total=309ms` and JPEG leaf
  `block_fetch_total=154ms`.
- Slow CID totals were:
  - root CID `bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq`:
    `28` events, total `2193ms`, max `597ms`
  - JPEG raw CID `bafkreicelmnc3ftqqdshh3eqj2zktconszajoss33mm2yq4lvg4m7j3qga`:
    `10` events, total `491ms`, max `154ms`
  - `_nuxt` directory CID `bafybeidaeoj3ctpgrkxromrf3bnregt2g3w6yxpgp6espt7ko3akaxnhua`:
    `7` events, total `367ms`, max `118ms`

Conclusion:
The current range path is already materially better than Kubo in tail latency
and mobile resource footprint for this sample, while Kubo still has a much
faster warm p50. The remaining Rust cold cost is not the `206` body write path:
it is the sequential UnixFS/file-size path that must fetch root/index/leaf
blocks before serving the range. Future range/media experiments should focus on
path-session reuse, directory metadata locality, or carefully bounded
multi-block scheduling rather than changing direct body streaming.

## 2026-05-05 Keep: Reuse Resolved File CID For Gateway Body Reads

Hypothesis:
Gateway file serving already resolves the UnixFS path while determining the file
size. Range and streaming body reads then used the root CID plus UnixFS path
again, relying on the path cache for each body read. Passing the already
resolved file CID into body reads should remove repeated path-cache lookups from
range and chunked-stream paths without changing block retrieval, verification,
caching, routing, or fallback behavior.

Implementation:

- Add cached `UnixfsResolver::file_size_cid` and
  `UnixfsResolver::read_file_cid_range` helpers.
- Carry the resolved file CID in `ServedResource::File`.
- Use the resolved file CID for MIME sniff reads, direct small bodies, range
  bodies, and chunked stream bodies.
- Preserve root CID/path based ETags and response headers.
- Keep trace fields for the root CID and add `file_cid` where body reads are
  now CID-direct.

Focused validation:

```sh
cargo test -p freedom-ipfs-unixfs cid_direct_range_reads_skip_path_resolution_cache
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-gateway
cargo check -p freedom-ipfs-unixfs -p freedom-ipfs-gateway --all-targets
cargo clippy -p freedom-ipfs-unixfs -p freedom-ipfs-gateway --all-targets -- -D warnings
```

Result: all passed.

Range comparison command:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-cid-direct-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cid-direct-range-rust-vs-kubo-r3.json
```

Range result:

- Rust and Kubo both passed `3/3`.
- Rust range TTFB moved from the saved baseline `79/689ms` p50/p95 to
  `18/648ms`.
- Kubo range TTFB was `3/1638ms` p50/p95 in the same run.
- Rust RSS/FD: `39256KiB`/`19`; Kubo RSS/FD: `112128KiB`/`46`.
- Rust UnixFS metadata cache path hits dropped from `5` to `2`; path misses and
  inserts stayed at `1`.
- Rust Bitswap peer attempts stayed at `7`; incoming matched blocks stayed at
  `3`.

Broader page-assets check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-cid-direct-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cid-direct-page-assets-rust-vs-kubo-r3.json
```

Broader result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `19/1542ms`, Kubo `2/1983ms`.
- Asset TTFB p50/p95: Rust `13/435ms`, Kubo `4/151ms`.
- Rust RSS/FD: `50348KiB`/`38`; Kubo RSS/FD: `203064KiB`/`119`.
- Compared with the kept `100ms` post-lookup-grace baseline, Rust asset p95
  improved from `612ms` to `435ms`; root p95 was effectively unchanged
  (`1547ms` to `1542ms`).

Additional range check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-cid-direct-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-cid-direct-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `3/1362ms`, Kubo `3/1966ms`.
- Rust RSS/FD: `38784KiB`/`20`; Kubo RSS/FD: `121744KiB`/`61`.
- Rust warm repeat requests were `1ms` each, and direct-body max elapsed was
  `1ms`.

Decision: keep. This is a small local UnixFS/gateway optimization with direct
test coverage for the intended cache behavior. It removes redundant path-cache
work from body reads and improves the real range sample and full `ipfs.tech`
page-assets run without increasing routing fanout or changing read-only serving
semantics.

## 2026-05-05 Reject: Skip Hot-Cache Reverification On Reads

Hypothesis:
After CID-direct body reads, warm `ipfs.tech` hero range requests were still
around `14-16ms`, while the smaller `/ipfs` vitalik range warmed at `1ms`.
Trace events showed the warm hero cost came almost entirely from
`block_store_get cache_hit=true` for the 184KB raw JPEG block. Temporarily
skipping `verify_block` on hot-cache hits would show how much of that local
cost is repeated verification versus unavoidable copying/body work.

Temporary experiment:

- Remove the hot-cache-hit `verify_block(cid, &hit.data)` call in
  `SqliteBlockStore::get`.
- Do not change SQLite reads or writes: persisted blocks are still verified on
  put and on cold read.
- Revert immediately after measurement.

Command:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-hot-no-reverify-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-hot-no-reverify-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Warm Rust repeat range requests dropped from `16ms`/`14ms` in the CID-direct
  run to `2ms`/`2ms`.
- `gateway_direct_body` max elapsed dropped from `13ms` to `0ms`.
- Cold Rust tail was not useful as a keep signal in this network window:
  root/range p95 was `2408ms`, with slow Bitswap provider behavior on the
  first request.

Decision: reject the direct change. It proves repeated hot-cache verification
is a meaningful warm media-range cost, but skipping verification at this layer
weakens the current rule that blocks are verified before serving or caching.
A future keepable version would need an explicit verified-hot-entry design,
for example storing verified block state in the hot cache and making the safety
contract clear in the store API, or introducing a CID-range read API that can
serve from verified hot cache without weakening cold-read verification.

## 2026-05-05 Keep: Verified Hot Cache Entries Avoid Rehashing Warm Blocks

Hypothesis:
The rejected direct skip showed that hot-cache rehashing was the remaining
local warm-range cost, but the implementation was too implicit. A keepable
version can make the private hot cache explicitly store only bytes that have
already been verified on `put_block` or on cold SQLite read. Hot-cache hits can
then serve from that verified in-memory entry without rehashing on every warm
range or body read, while writes and cold reads still verify before caching or
serving.

Implementation:

- Rename the private hot cache types to `VerifiedHotCache`,
  `VerifiedHotBlock`, and `VerifiedHotCacheHit`.
- Rename insertion and read APIs to `put_verified` and `get_verified`.
- Populate the verified hot cache only after `put_block` verification or cold
  SQLite read verification.
- Keep SQLite cold-read verification and all write-time verification unchanged.
- Add tests proving rejected writes and corrupt cold reads do not populate the
  verified hot cache.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-store
cargo test -p freedom-ipfs-gateway
cargo check -p freedom-ipfs-store -p freedom-ipfs-retrieval -p freedom-ipfs-gateway --all-targets
cargo clippy -p freedom-ipfs-store -p freedom-ipfs-retrieval -p freedom-ipfs-gateway --all-targets -- -D warnings
```

Result: all passed.

Range comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-verified-hot-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-verified-hot-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Rust warm repeat range requests were `0ms`/`0ms` in the gateway trace, versus
  `16ms`/`14ms` in the CID-direct baseline.
- Rust gateway direct-body max elapsed dropped from `13ms` to `0ms`.
- Rust range TTFB p50/p95 was `2/2477ms`; Kubo was `4/3480ms`. The cold Rust
  request hit unrelated provider/DNS expansion noise, but the warm-path signal
  is clear.

Broader page-assets check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-verified-hot-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-verified-hot-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `4/716ms`, Kubo `3/4741ms`.
- Asset TTFB p50/p95: Rust `7/358ms`, Kubo `4/378ms`.
- Rust RSS/FD: `50628KiB`/`43`; Kubo RSS/FD: `279756KiB`/`392`.
- Compared with the CID-direct baseline, Rust asset p50/p95 improved from
  `13/435ms` to `7/358ms`; gateway direct-body max elapsed improved from
  `10ms` to `2ms`.
- Warm page repeat groups improved from about `3-12ms` to `2-5ms`.

Additional `/ipfs` range check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-verified-hot-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-verified-hot-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `5/425ms`, Kubo `4/1851ms`.
- Rust RSS/FD: `37504KiB`/`18`; Kubo RSS/FD: `166164KiB`/`77`.
- Warm repeat requests were `2ms` each, and gateway direct-body max elapsed was
  `0ms`.

Decision: keep. This preserves verification before cache insertion and before
cold serving, makes the verified-hot-entry contract explicit in the private
store API, and removes repeated hashing from warm in-memory reads. It improves
warm media/range behavior and the full `ipfs.tech` page-assets comparison
without increasing routing fanout, adding fallback, or increasing persistent
storage work.

## 2026-05-05 Keep: Raw Range Reads Slice Verified Hot Cache Entries

Hypothesis:
After verified hot-cache entries removed repeated rehashing, small media/range
responses could still clone the whole cached raw block before slicing the
requested byte window. A narrow block-range provider API should let hot cached
raw blocks copy only the requested range, without changing network retrieval,
read-only behavior, or the rule that cold blocks are verified before serving or
caching.

Implementation:

- Add a default `BlockProvider::get_block_range(cid, start, end)` method and a
  shared `block_data_range` helper.
- Override the range method in `SqliteBlockStore` so verified hot-cache hits
  clone only the requested bytes.
- Keep cold SQLite range reads conservative: read the full block, verify it,
  then populate the verified hot cache and return the requested slice.
- Override `FetchingBlockProvider::get_block_range` so gateway reads use the
  store range path on cache hits and fall back to normal full-block retrieval
  on cache misses.
- Propagate the method through `ScopedBlockProvider` so streaming/ranged
  gateway responses keep their existing retention behavior.
- Use the range method for UnixFS raw CID range reads.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-core -p freedom-ipfs-store -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-retrieval -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result: all passed.

Range comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-range-slice-warm-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-range-slice-warm-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Rust range TTFB p50/p95 was `4/2848ms`; Kubo was `279/2461ms`.
- Rust RSS/FD: `39616KiB`/`28`; Kubo RSS/FD: `186476KiB`/`107`.
- Warm Rust repeat requests were `2ms`/`2ms`.
- Gateway direct-body max elapsed stayed at `0ms`.
- The trace shows `block_store_get_range=3`, with the two warm requests using
  the range path instead of full raw block reads.

Broader page-assets check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-range-slice-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-range-slice-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `4/2298ms`, Kubo `3/4446ms`.
- Asset TTFB p50/p95: Rust `4/484ms`, Kubo `4/272ms`.
- Rust RSS/FD: `50244KiB`/`42`; Kubo RSS/FD: `292800KiB`/`464`.
- Warm page repeat groups were `1-2ms`.
- Gateway direct-body max elapsed stayed at `0ms`.

Additional `/ipfs` range check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-range-slice-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-range-slice-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `4/353ms`, Kubo `5/4003ms`.
- Rust RSS/FD: `37376KiB`/`18`; Kubo RSS/FD: `125084KiB`/`101`.
- Warm repeat requests were `2ms` each, and gateway direct-body max elapsed was
  `0ms`.

Additional cold-only check:

```sh
cargo run -p mobile-web-harness -- \
  --case ipfs-tech-developers-hero-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --compare-kubo \
  --trace-output /tmp/ipfs-tech-range-slice-rust-vs-kubo-r3-trace.jsonl \
  --output /tmp/ipfs-tech-range-slice-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Fresh-gateway range TTFB p50/p95: Rust `2328/2389ms`, Kubo
  `3825/4959ms`.
- This mainly measured public-network/provider cold behavior. It is not the
  keep signal for this patch, but it confirmed the range API did not break cold
  reads.

Decision: keep. The live latency effect is small because the previous verified
hot-cache change already drove direct-body work to the harness millisecond
floor, but this removes an avoidable full-block clone from hot raw range reads,
keeps cold verification intact, adds focused coverage for the new provider
method, and improves the full-page warm repeat groups without increasing
routing fanout, timeout budgets, fallback scope, or persistent storage work.

## 2026-05-05 Reject: Gateway MIME Result Cache

Hypothesis:
Warm page repeat traces still showed `mime_detect` and `mime_total` for every
request. A small bounded gateway MIME cache keyed by resolved file CID plus
path might avoid repeated sniff reads and shave another millisecond or two from
warm page loads.

Temporary experiment:

- Add a bounded in-memory MIME cache to `GatewayState`.
- Cache extension, sniffed-HTML, and fallback MIME results.
- Keep the cache opportunistic: a poisoned cache lock would miss rather than
  failing a request.
- Add a focused synthetic test where two extensionless raw HTML requests drop
  from four range reads to three by skipping the second MIME sniff.

Focused validation:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-gateway mime_cache_reuses_sniffed_type_for_repeated_raw_requests
cargo test -p freedom-ipfs-gateway
```

Result: all passed while the temporary patch was applied.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-mime-cache-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-mime-cache-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `4/683ms`, Kubo `3/1775ms`.
- Asset TTFB p50/p95: Rust `6/307ms`, Kubo `4/264ms`.
- Rust RSS/FD: `51984KiB`/`41`; Kubo RSS/FD: `112104KiB`/`56`.
- Warm page repeat groups were `2-5ms`, worse than the immediately previous
  raw range-slice run's `1-2ms` groups.
- `block_store_get_range` remained `111`, matching the prior page-assets run;
  the real `ipfs.tech` page mostly uses extension-derived MIME types, so this
  cache did not avoid the hot range reads that matter in this workload.

Decision: reject and revert. The synthetic extensionless-raw case works, but
the real page workload does not justify another gateway cache. Keep MIME
optimizations focused on cases where traces show actual sniff reads or MIME
work on extensionless content, not extension-derived assets.

## 2026-05-05 Keep: Give Recent Bitswap Session Peers A Short Provider-Lookup Head Start

Hypothesis:
The `ipfs.tech` page trace still showed many delegated provider lookups even
when recent Bitswap session peers later satisfied the asset block. Starting the
provider lookup immediately keeps fallback latency low, but it also spends
mobile network, connection, and routing work on blocks that a known-good peer
can often serve. Giving recent session peers a very short head start before
starting delegated routing should reduce duplicate provider work while keeping
fallback bounded.

Implementation:

- Add `BITSWAP_SESSION_PRE_LOOKUP_GRACE = 50ms`.
- When no provider cache entry exists and recent Bitswap peers are available,
  poll the session shortcut for at most `50ms` before starting provider lookup.
- If the shortcut hits inside that window, return the verified Bitswap block and
  skip provider lookup for that CID.
- If the shortcut misses or times out, start the existing provider lookup path
  and keep the existing `100ms` post-lookup grace.
- Add `bitswap_session_shortcut_pre_lookup` tracing with `hit`, `miss`, or
  `timeout` outcome.
- Add a loopback retrieval test proving a connected recent Bitswap peer can
  serve a follow-on block without issuing a delegated routing request.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_head_start_can_avoid_provider_lookup
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway -p freedom-ipfs-mobile
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result: all passed.

Page-assets comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-headstart-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-session-headstart-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `4/662ms`, Kubo `3/3371ms`.
- Asset TTFB p50/p95: Rust `5/268ms`, Kubo `4/239ms`.
- Rust RSS/FD: `51780KiB`/`31`; Kubo RSS/FD: `168496KiB`/`178`.
- Delegated provider lookup events dropped to `19`, compared with `25` in the
  immediately previous page-assets run.
- Bitswap peer attempts dropped to `45`, compared with `112` in the immediately
  previous page-assets run.
- Bitswap session shortcut attempts/hits were `33/33`; post-lookup waits
  dropped to `1`.
- Gateway response p50/p95 was `2/277ms`.

Additional `/ipfs` range check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-session-headstart-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-session-headstart-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `5/1561ms`, Kubo `4/2894ms`.
- Rust RSS/FD: `38528KiB`/`23`; Kubo RSS/FD: `120568KiB`/`83`.
- Warm repeat requests stayed `2ms` each, with gateway direct-body max elapsed
  still `0ms`.
- The cold request was network/provider noisy and not the keep signal.

Sparse-provider check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-session-headstart-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-session-headstart-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both failed `3/3` in this network window.
- Rust failed with sparse-provider/no-provider behavior on a child raw CID:
  delegated lookup found only `1` provider, DHT fallback found `0`, then the
  cached empty provider set made warm repeats fail quickly.
- This is not a useful keep/reject signal for the session head-start change,
  but it remains a useful reminder that sparse-provider reliability still needs
  more work.

Decision: keep. This is a bounded session optimization: it does not increase
provider fanout, public fallback, timeouts, or cache trust. It reduces redundant
provider lookup and Bitswap attempt work on the real `ipfs.tech` page workload
where recent session peers are useful, while preserving the existing provider
lookup fallback after a `50ms` cap.

## 2026-05-05 Observe: `cid.contact` Does Not Help Current `daicowtf` Sparse Child CID

Question:
The `daicowtf-page-assets` check failed in the current network window because a
child raw CID had no usable providers after delegated lookup plus light-DHT
fallback. Previous `cid.contact` experiments were on other cases, so run a
single targeted probe before considering any sparse-provider fallback policy.

Command:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --trace-output /tmp/daicowtf-dual-router-sparse-r1-trace.jsonl \
  --output /tmp/daicowtf-dual-router-sparse-r1.json
```

Result:

- Rust failed `0/1` with status `504`.
- `delegated-ipfs.dev` returned the same single provider pattern; total
  delegated provider records were still `1`.
- `cid.contact` returned `404 Not Found` for both the root and the failing child
  raw CID.
- DHT fallback found `0` providers and timed out for the child CID.
- The failing child CID was
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u`.

Decision: no code change. This does not reopen the earlier rejected default
`cid.contact` change. The sparse-provider issue remains, but this endpoint did
not add diversity for the failing `daicowtf` child CID in this run.

## 2026-05-05 Keep: Let Recent Session Peers Finish When Provider Lookup Is Empty

Hypothesis:
After the session peer head-start change, a known-good recent Bitswap peer can
still lose if delegated routing quickly returns an empty provider set. In that
case, failing immediately wastes an in-flight peer request that may answer
within the existing bounded session shortcut timeout.

Implementation:

- When recent session peers exist and provider lookup returns `Ok([])`, wait for
  the already-started session shortcut to finish before failing.
- Keep the existing `100ms` post-lookup grace when provider lookup returns a
  non-empty provider set.
- Reuse the existing `2s` internal session shortcut timeout; do not increase
  provider fanout, add fallback gateways, or trust unverified blocks.
- Emit `bitswap_session_shortcut_empty_providers_wait` with `hit`/`miss`.
- Map both pre-lookup and empty-provider session shortcut trace events to the
  mobile progress phase `fetching_bitswap`.

Focused tests:

```sh
cargo test -p freedom-ipfs-retrieval empty_provider_lookup_waits_for_recent_bitswap_peer
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
```

Result:

- Both focused tests passed.
- The retrieval test covers a delayed recent Bitswap peer answering after a
  delegated `Providers: []` result.
- The mobile test keeps the new trace phase out of user-facing progress state.

Normal-path regression check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-empty-provider-wait-page-assets-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-empty-provider-wait-page-assets-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `3/1549ms`, Kubo `3/1515ms`.
- Asset TTFB p50/p95: Rust `3/405ms`, Kubo `4/435ms`.
- Rust RSS/FD: `52204KiB`/`44`; Kubo RSS/FD: `118040KiB`/`54`.
- Delegated provider lookup events were `20`, in line with the previous session
  head-start run.

Sparse-provider check:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/daicowtf-empty-provider-wait-r1-trace.jsonl \
  --output /tmp/daicowtf-empty-provider-wait-r1.json
```

Result:

- Rust failed `0/1` with status `504`.
- The root still had one provider and the failing child CID hit the existing
  provider-lookup-error/session-shortcut wait path after delegated routing found
  `0` providers and DHT timed out.
- No `bitswap_session_shortcut_empty_providers_wait` hit was expected from this
  run; this is not a useful keep/reject signal for the empty-provider branch.

Decision: keep. The change is narrow and bounded by an existing `2s` cap only
when a recent session peer exists and routing returns an empty provider set. It
does not alter the non-empty provider path or the read-only trust model, and it
adds diagnostics/progress mapping for the new wait state.

## 2026-05-05 Keep: Tune Session Peer Head Start To 75ms

Hypothesis:
The `50ms` recent-session pre-lookup grace avoids some delegated provider
lookups, but real `ipfs.tech` traces still show many session shortcuts winning
after the provider lookup has already started. A slightly longer grace may let
more known-good peers win before routing work begins while staying short enough
not to punish misses.

Implementation:

- Increase `BITSWAP_SESSION_PRE_LOOKUP_GRACE` from `50ms` to `75ms`.
- Leave the existing `100ms` post-lookup grace and the `2s` session shortcut cap
  unchanged.
- No new provider fanout, no gateway fallback, and no change to block
  verification or caching trust.

Baseline/current `50ms` run:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-grace50-page-assets-r3-trace.jsonl \
  --output /tmp/ipfs-tech-session-grace50-page-assets-r3.json
```

Result:

- Passed `3/3`.
- Cold run total/root max: `2423ms` / `856ms`.
- Asset TTFB p50/p90/p95/max: `4/368/468/710ms`.
- Delegated provider lookups: `21`.
- Bitswap fetches: `8`; session shortcuts `26/26`; post-lookup waits `8`.
- RSS/FD max: `54816KiB` / `43`.

Rejected `100ms` run:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-grace100-page-assets-r3-trace.jsonl \
  --output /tmp/ipfs-tech-session-grace100-page-assets-r3.json
```

Result:

- Passed `3/3`, but cold run total/root max worsened to `3182ms` / `1357ms`.
- Asset TTFB p95/max worsened to `633/1096ms`.
- Delegated provider lookups dropped to `10`, but the latency cost was not worth
  keeping.

Middle-point `75ms` runs:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-grace75-page-assets-r3-trace.jsonl \
  --output /tmp/ipfs-tech-session-grace75-page-assets-r3.json

timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-grace75b-page-assets-r3-trace.jsonl \
  --output /tmp/ipfs-tech-session-grace75b-page-assets-r3.json
```

Results:

- First `75ms` run passed `3/3`; cold total/root max `2026ms` / `825ms`;
  asset TTFB p95/max `617/776ms`; Bitswap fetches `4`; peer attempts `91`;
  RSS/FD max `48612KiB` / `37`.
- Second `75ms` run passed `3/3`; cold total/root max `1738ms` / `538ms`;
  asset TTFB p95/max `532/605ms`; Bitswap fetches `5`; peer attempts `97`;
  RSS/FD max `48740KiB` / `36`.
- A same-window return-to-`50ms` control passed `3/3` but had cold total/root max
  `2292ms` / `847ms`, asset TTFB p95/max `724/1432ms`, Bitswap fetches `8`, and
  peer attempts `122`
  (`/tmp/ipfs-tech-session-grace50b-page-assets-r3.json`).

Kubo comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-session-grace75-kubo-page-assets-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-session-grace75-kubo-page-assets-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `2/2326ms`, Kubo `2/2500ms`.
- Asset TTFB p50/p95: Rust `5/354ms`, Kubo `4/861ms`.
- Rust RSS/FD max: `50076KiB` / `42`; Kubo RSS/FD max:
  `122108KiB` / `64`.
- The Rust root p95 still had a cold Bitswap outlier, but the asset path was
  materially better than Kubo in this window.

Range regression check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-session-grace75-range-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-session-grace75-range-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root/range TTFB p50/p95: Rust `4/373ms`, Kubo `4/1967ms`.
- Rust RSS/FD max: `37504KiB` / `18`; Kubo RSS/FD max:
  `127248KiB` / `72`.

Decision: keep the `75ms` grace. The `100ms` variant bought fewer provider
lookups at too much latency cost. The `75ms` variant repeatedly reduced Bitswap
fetches, peer attempts, FD/RSS pressure, and cold page tail versus nearby `50ms`
controls while preserving the same bounded fallback behavior.

## 2026-05-05 Reject: Generic UnixFS Linked-Child Prefetch Hook

Hypothesis:
A bounded `BlockProvider` prefetch hook could let UnixFS start fetching linked
file children before walking them serially. This would be a small step toward
content-root session batching without changing verification, provider trust, or
gateway fallback policy.

Prototype:

- Added a default no-op `BlockProvider::prefetch_blocks(&[Cid])`.
- Had UnixFS call it before full linked-file reads and before range reads over
  intersecting child links.
- Implemented `FetchingBlockProvider` as a best-effort background fetch of up to
  `8` missing CIDs through the normal verified `fetch_block_with_source` path.
- Forwarded through `ScopedBlockProvider`.
- Added focused UnixFS tests proving full reads and range reads exposed the
  expected child CIDs.

Focused tests:

```sh
cargo test -p freedom-ipfs-unixfs prefetches_
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
```

Result: both passed.

Live checks:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-unixfs-prefetch-page-assets-r3-trace.jsonl \
  --output /tmp/ipfs-tech-unixfs-prefetch-page-assets-r3.json

timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-unixfs-prefetch-range-r1-trace.jsonl \
  --output /tmp/vitalik-unixfs-prefetch-range-r1.json

timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-range \
  --repeat 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/ipfs-tech-hero-unixfs-prefetch-r1-trace.jsonl \
  --output /tmp/ipfs-tech-hero-unixfs-prefetch-r1.json
```

Result:

- All three live checks passed.
- None of the traces contained `unixfs_link_prefetch` events.
- The current live corpus resolves these paths mostly as directory/path metadata
  plus raw leaf CIDs, not as multi-link DAG-PB file CIDs. The prototype's target
  path therefore was not exercised.
- The `ipfs.tech-page-assets` run looked good on asset tail, but since the new
  phase never appeared, that improvement is network/session noise rather than a
  keep signal for this code.

Decision: revert the code and keep only this note. This is still a plausible
future experiment, but it needs a corpus case that definitely exercises
multi-block UnixFS file CIDs before it should be carried in production. A good
next step is to add a deterministic harness fixture or stable public media file
where the requested file CID is a DAG-PB file with multiple raw children, then
retest bounded prefetch or true Bitswap multi-want against that case.

## 2026-05-05 Harness: CAR-Seeded Mobile-Web Runs

Problem:
The linked-child prefetch experiment above could not be evaluated because the
live corpus did not actually request a multi-link DAG-PB UnixFS file. Depending
on public providers to find one would make the next optimization loop noisy and
hard to compare against Kubo.

Change:

- Added `mobile-web-harness --gateway-import-car /path/to/fixture.car`.
- Rust spawned gateways receive `--import-car` before the corpus starts.
- Kubo spawned repos receive `ipfs dag import /path/to/fixture.car` before the
  daemon starts.
- JSON/console reports record the imported CAR path.
- The option is rejected with `--gateway-url`, because the harness cannot seed
  an already-running external gateway.

Intended use:

```sh
cargo run -p xtask -- generate-mobile-web-fixture \
  --car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json

cargo run -p mobile-web-harness -- \
  --build-gateway \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 5 \
  --trace-output /tmp/xtask-mobile-web-multiblock-rust-trace.jsonl \
  --output /tmp/xtask-mobile-web-multiblock-rust.json

cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 5 \
  --comparison-output /tmp/xtask-mobile-web-multiblock-rust-vs-kubo.json
```

Validation:

```sh
cargo test -p mobile-web-harness
cargo test -p xtask
cargo test -p freedom-ipfs-gateway explicit_offline_routing_runs_cache_only_gateway
cargo fmt --all --check
cargo check -p mobile-web-harness --all-targets
cargo check -p xtask --all-targets
cargo clippy -p mobile-web-harness -p xtask --all-targets -- -D warnings
git diff --check
```

Result: all passed.

Repo-native multi-block fixture generated with xtask:

```sh
cargo run -p xtask -- generate-mobile-web-fixture \
  --car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json
```

- root CID:
  `bafybeig45rg3a5hszbyqnqanqjkgbkwrx7vjcv4uligjzw4jmhomgsshty`
- CAR: `/tmp/xtask-mobile-web-multiblock.car`
- corpus: `/tmp/xtask-mobile-web-multiblock-corpus.json`
- blocks: `4`
- bytes: `600000`
- default cases:
  `multiblock-unixfs-full`,
  `multiblock-unixfs-range`,
  `multiblock-unixfs-prefix-range`,
  `multiblock-unixfs-boundary-range`, and
  `multiblock-unixfs-suffix-range`

Rust offline import smoke:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 1 \
  --trace-output /tmp/xtask-mobile-web-multiblock-rust-trace.jsonl \
  --output /tmp/xtask-mobile-web-multiblock-rust.json
```

Result:

- gateway imported `4` CAR blocks
- passed `1/1`
- root/range TTFB `4ms`, total `4ms`
- RSS/FD `21392KiB` / `11`

Rust-vs-Kubo import smoke:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 1 \
  --comparison-output /tmp/xtask-mobile-web-multiblock-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `1/1`
- root/range TTFB: Rust `1ms`, Kubo `6ms`
- RSS/FD: Rust `21004KiB` / `10`, Kubo `86712KiB` / `35`

Decision:
Keep this as measurement infrastructure. It does not change gateway retrieval
behavior, but it gives future prefetch/multi-want work a deterministic
black-box workload before touching production code again.

## 2026-05-05 Optimize: Skip MIME Sniff Reads For Deep Ranges

Hypothesis:
For no-extension byte-range requests that start after byte `0`, MIME sniffing
the first bytes can fetch an unrelated block before serving the requested
range. This is especially wasteful for media seeking and multi-block UnixFS
files on mobile.

Change:

- Parse the `Range` header before MIME detection.
- Continue using path-extension MIME detection whenever available.
- Continue sniffing full responses and ranges that start at byte `0`, using
  only bytes inside the requested prefix range when possible.
- For ranges that start after byte `0`, skip MIME sniffing and use
  `application/octet-stream` with trace source `fallback_no_sniff`.

Focused validation:

```sh
cargo test -p freedom-ipfs-gateway
```

Result: all gateway tests passed, including
`deep_byte_ranges_skip_mime_sniff_prefix_read`, which proves a deep range over a
two-link DAG-PB file does not fetch the first linked raw block just for MIME
sniffing.

Fixture validation:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 1 \
  --trace-output /tmp/xtask-mobile-web-multiblock-no-sniff-rust-trace.jsonl \
  --output /tmp/xtask-mobile-web-multiblock-no-sniff-rust.json
```

Result:

- passed `1/1`
- trace lines dropped from `11` to `10`
- `mime_sniff_read` disappeared from the trace
- `mime_detect` recorded `source=fallback_no_sniff`
- UnixFS metadata-cache hits dropped from `3` to `2`
- root/range TTFB and total were `4ms` / `4ms`
- RSS/FD `21524KiB` / `11`

Kubo comparison:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-multiblock.car \
  --corpus /tmp/xtask-mobile-web-multiblock-corpus.json \
  --case multiblock-unixfs-range \
  --repeat 1 \
  --comparison-output /tmp/xtask-mobile-web-multiblock-no-sniff-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed `1/1`
- root/range TTFB: Rust `2ms`, Kubo `9ms`
- RSS/FD: Rust `21008KiB` / `10`, Kubo `79696KiB` / `45`

Decision:
Keep. This is a bounded range-workload optimization: it avoids fetching data
outside the requested byte range while preserving HTML sniffing for full
responses and prefix ranges such as `bytes=0-127`.

## 2026-05-05 Harness: Expand Multi-Block Stream/Range Suite

Hypothesis:
A single deterministic deep range is useful, but it does not cover the range
and streaming shapes that matter for media and UnixFS traversal: full-response
streaming, prefix sniffing, a range crossing a raw-leaf boundary, and suffix
reads.

Change:

- `xtask generate-mobile-web-fixture` now emits five cases against the same CAR:
  - `multiblock-unixfs-full`: full `600000` byte response
  - `multiblock-unixfs-range`: `bytes=262100-262399`
  - `multiblock-unixfs-prefix-range`: `bytes=0-299`
  - `multiblock-unixfs-boundary-range`: `bytes=261994-262293`
  - `multiblock-unixfs-suffix-range`: `bytes=599700-599999`
- The default case ID remains `multiblock-unixfs-range`, so previous one-case
  commands still work with `--case multiblock-unixfs-range`.
- MIME trace fallback sources now distinguish `fallback_after_sniff` from
  `fallback_no_sniff`; the prefix range is the only generated range that emits
  `mime_sniff_read`.

Validation:

```sh
cargo run -p xtask -- generate-mobile-web-fixture \
  --car /tmp/xtask-mobile-web-stream-suite.car \
  --corpus /tmp/xtask-mobile-web-stream-suite-corpus.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-stream-suite.car \
  --corpus /tmp/xtask-mobile-web-stream-suite-corpus.json \
  --repeat 1 \
  --trace-output /tmp/xtask-mobile-web-stream-suite-rust-trace.jsonl \
  --output /tmp/xtask-mobile-web-stream-suite-rust.json
```

Result:

- all five Rust stream/range cases passed
- run total `58ms`
- case TTFB/total:
  - full `3ms` / `46ms`
  - deep `3ms` / `3ms`
  - prefix `2ms` / `2ms`
  - boundary `2ms` / `2ms`
  - suffix `2ms` / `2ms`
- RSS/FD `21904KiB` / `12`
- trace contained `mime_sniff_read` twice: once for the full response and once
  for `bytes=0-299`
- trace sources:
  - full and prefix `source=fallback_after_sniff`
  - deep/boundary/suffix `source=fallback_no_sniff`
- the full-response case also shows why this fixture matters for diagnostics:
  gateway `request_done` tracks response creation, while harness `root_total`
  captures full body transfer (`46ms` here)

Kubo comparison:

```sh
timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-stream-suite.car \
  --corpus /tmp/xtask-mobile-web-stream-suite-corpus.json \
  --repeat 1 \
  --comparison-output /tmp/xtask-mobile-web-stream-suite-rust-vs-kubo.json
```

Result:

- Rust and Kubo both passed all five cases
- TTFB Rust/Kubo:
  - full `3ms` / `5ms`
  - deep `1ms` / `2ms`
  - prefix `0ms` / `4ms`
  - boundary `0ms` / `2ms`
  - suffix `1ms` / `1ms`
- RSS/FD: Rust `21396KiB` / `10`, Kubo `88820KiB` / `32`

Decision:
Keep. This turns the CAR-seeded fixture from a one-off deep-range smoke into a
small deterministic stream/range harness that future prefetch, multi-want,
streaming, and body-progress diagnostics experiments can run without
public-network noise.

## 2026-05-05 Diagnostics: Trace Streamed Gateway Body Completion

Hypothesis:
For large full responses and large ranges, `request_done` is too early to
explain browser-perceived latency because it records response construction, not
body production. A low-volume completion event for streamed gateway bodies
would let the harness and mobile progress API tell whether the gateway spent
time producing the response body, without per-chunk trace spam.

Change:

- Add a `gateway_stream_done` trace event for streamed full responses and
  streamed ranges.
- Emit it after the final chunk is successfully read from UnixFS, before that
  chunk is yielded to Hyper.
- Include `range_start`, `range_end`, `body_len`, `chunks`, and `elapsed_ms`.
- Carry the request tracing span into the response-body stream so the event is
  correlated with `request_id`, `progress_request_id`, `top_level_path`, and
  gateway path.
- Map `gateway_stream_done` to mobile/harness progress phase `completed`.
- Expose `body_len` as mobile progress `bytes_loaded` for the completion event.
- Add a harness summary line for streamed bodies with event count, bytes,
  maximum body length, maximum chunk count, and maximum elapsed time.
- Keep completed harness request summaries available for later span-correlated
  stream-body events. In practice `gateway_stream_done` can arrive after
  `request_done`, because `request_done` records response construction while the
  body stream finishes afterward.
- Add `body_mode=stream|direct` to `request_done`. Mobile and harness progress
  now keep successful streamed `request_done` events in `streaming` state until
  `gateway_stream_done`; direct/error responses still complete or fail at
  `request_done`.
- Add `gateway_stream_failed` so a stream read error after headers have been
  produced can still fail mobile progress instead of leaving the target active.
- Add mobile progress `bytes_total`, populated from `file_len`, `body_len`, or
  explicit `bytes_total` fields and carried forward on the active target.
- Add snapshot-time `active_subrequests` on active progress targets, derived
  from `parent_id` relationships.

Implementation note:
An earlier version emitted from the stream terminal `None` state. The
deterministic harness showed that this is unreliable with `Content-Length`
responses because Hyper can finish once it has sent the declared byte count
without polling an extra EOF frame. Emitting after the final successful chunk is
the reliable low-volume signal. This event measures body production by the
gateway, not guaranteed client socket delivery after the last byte leaves the
process.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p freedom-ipfs-mobile progress_snapshot_records_stream_body_bytes
cargo test -p freedom-ipfs-mobile progress_snapshot_records_gateway_request_phases
cargo test -p freedom-ipfs-mobile progress_snapshot_counts_active_subrequests
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p freedom-ipfs-gateway

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-stream-suite.car \
  --corpus /tmp/xtask-mobile-web-stream-suite-corpus.json \
  --case multiblock-unixfs-full \
  --repeat 1 \
  --trace-output /tmp/xtask-mobile-web-stream-state-rust-trace.jsonl \
  --output /tmp/xtask-mobile-web-stream-state-rust.json

timeout 180s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --routing-mode offline \
  --gateway-import-car /tmp/xtask-mobile-web-stream-suite.car \
  --corpus /tmp/xtask-mobile-web-stream-suite-corpus.json \
  --repeat 1 \
  --trace-output /tmp/xtask-mobile-web-stream-state-rust-vs-kubo-trace.jsonl \
  --comparison-output /tmp/xtask-mobile-web-stream-state-rust-vs-kubo.json
```

Result:

- focused mobile progress test passed
- focused mobile streamed-byte snapshot test passed
- focused gateway progress snapshot test passed
- focused active-subrequest snapshot test passed
- focused harness progress summary test passed
- all gateway tests passed
- deterministic full-response fixture passed `1/1`
- root TTFB/total: `5ms` / `7ms`
- RSS/FD: `22416KiB` / `12`
- `request_done` included `body_mode=stream`
- trace contained one `gateway_stream_done` event:
  - `body_len=600000`
  - `range_start=0`
  - `range_end=599999`
  - `chunks=10`
  - `elapsed_ms=1`
  - correlated span fields included `request_id=1`,
    `progress_request_id=1`, and the `/ipfs/...` top-level path
- progress phases changed from `completed=2` to `streaming=8, completed=1`:
  successful streamed `request_done` no longer marks the request completed
  before the body stream finishes
- harness streamed-body summary reported one event with `600000` bytes and
  `10` chunks
- slow request and progress request group summaries now include
  `gateway_stream_done=1` even though the event appears after `request_done` in
  the JSONL trace
- slow event details include `body_mode=stream` on `request_done` and
  `body_len=600000`, `chunks=10` on `gateway_stream_done`
- mobile progress JSON now carries `bytes_total`; streamed completion events
  report `bytes_loaded=600000` and `bytes_total=600000`
- active target snapshots now include `active_subrequests`, so Swift can see
  when a page-level request has active child resource requests
- refreshed offline Rust-vs-Kubo comparison passed `1/1` for both engines on
  all five deterministic stream/range fixture cases
- comparison artifacts:
  `/tmp/xtask-mobile-web-stream-state-rust-vs-kubo-trace.jsonl` and
  `/tmp/xtask-mobile-web-stream-state-rust-vs-kubo.json`
- Kubo version: `0.41.0`
- Rust root TTFB vs Kubo:
  - full stream: `5ms` vs `6ms`
  - full range: `3ms` vs `2ms`
  - prefix range: `1ms` vs `1ms`
  - boundary range: `1ms` vs `2ms`
  - suffix range: `1ms` vs `1ms`
- resource comparison on the same offline fixture:
  - Rust max RSS/FD: `21776KiB` / `11`
  - Kubo max RSS/FD: `87268KiB` / `31`
  - Kubo repo storage max: `631379B`
- refreshed comparison trace summary preserved the expected body diagnostics:
  `52` trace events, progress phases `streaming=37, completed=5, queued=5,
  started=5`, direct bodies `4` / `1200` bytes, streamed bodies `1` /
  `600000` bytes / `10` chunks

Decision:
Keep. The event closes the diagnostic gap identified by the stream/range
fixture: future harness runs can now see both gateway response construction and
streamed body production timing, while the mobile progress layer receives a
bounded completion signal for large streamed responses.

## 2026-05-05 Bitswap Multi-Want Shared Client Building Block

Hypothesis:
The stream-level multi-want helper already proves that one Bitswap stream can
request and cancel multiple CIDs. The next safe step toward page/session
batching is to carry that through the shared Bitswap client command path, while
leaving gateway/UnixFS retrieval behavior unchanged until there is a bounded
session window to feed it.

Change:

- Add an internal `SharedBitswapClient::fetch_many` path that accepts a bounded
  CID vector and returns verified requested blocks plus verified extra blocks.
- Keep `SharedBitswapClient::fetch` as the existing single-CID API by wrapping
  the batch path and converting the one requested block back into the old result
  shape.
- Change the shared Bitswap command/result internals to carry batch results.
- Preserve the existing incoming Bitswap stream fast path for single-CID
  commands. Multi-CID commands currently use outgoing streams only; incoming
  batch matching can be added when page/session batching is wired in.
- For multi-CID requests, send direct WANT_BLOCK entries on one stream. The
  existing single-CID WANT_HAVE behavior is preserved for normal provider races.
- Extend trace fields with `cids`, `cid_count`, `requested_blocks`, and batch
  failure diagnostics so future harness runs can distinguish one-CID fetches
  from batch experiments.
- Add deterministic local tests proving the shared client sends one multi-want
  request to a loopback Bitswap peer and rejects empty batches.

Validation:

```sh
cargo fmt --all

cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many
cargo test -p freedom-ipfs-retrieval
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings

timeout 360s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-shared-batch-sanity-trace.jsonl \
  --comparison-output /tmp/vitalik-shared-batch-sanity.json
```

Result:

- focused shared-client multi-want tests passed:
  `2 passed; 0 failed`
- full retrieval test suite passed:
  `65 passed; 0 failed; 1 ignored`
- retrieval clippy passed with `-D warnings`
- live same-window `vitalik-root-html-range` sanity check passed for both Rust
  and Kubo:
  - Rust root TTFB: `1443ms`
  - Kubo root TTFB: `3003ms`
  - Rust max RSS/FD: `38528KiB` / `21`
  - Kubo max RSS/FD: `120724KiB` / `86`
  - Rust Bitswap fetches: `2`, both delivered by incoming streams
  - trace showed `cids`/`cid_count` fields on the normal single-CID path while
    preserving single-request behavior
- live artifacts:
  `/tmp/vitalik-shared-batch-sanity-trace.jsonl` and
  `/tmp/vitalik-shared-batch-sanity.json`

Decision:
Keep as a Priority-1 building block. This does not claim a page-load speed win
by itself; it removes one more internal blocker before testing a small
content-root/session multi-want window.

## 2026-05-05 Harness: Summarize Bitswap Batch Shape

Problem:
After adding the shared-client `fetch_many` path, future runs need an obvious
summary signal that says whether a trace actually exercised multi-CID Bitswap
commands. Otherwise a good or bad live result could be mistaken for a batching
result even when every command was still single-CID.

Change:

- Add `bitswap_batches` to the mobile web harness trace summary JSON.
- Summarize batch command count, multi-CID command count, total/max requested
  CIDs per command, peer-attempt starts/successes, requested block count,
  cancelled fetches, and batch failures.
- Print a concise `bitswap batches:` line in normal and Rust-vs-Kubo comparison
  trace summaries.
- Preserve compatibility with older traces by treating `bitswap_dial_plan`
  events without `cid_count` as single-CID commands.

Validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo test -p mobile-web-harness
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused harness batch/peer-attempt summary test passed
- full mobile web harness tests passed: `24 passed; 0 failed`
- mobile web harness clippy passed with `-D warnings`

Decision:
Keep. This is diagnostics-only, but it makes the next multi-want experiment
measurable at a glance: a useful run should show `multi_cid_commands > 0` and
`max_requested_blocks > 1`.

## 2026-05-05 Reject: Session-Only UnixFS Link Batch Prefetch

Hypothesis:
After adding the shared-client `fetch_many` path, a narrower version of the
earlier UnixFS prefetch hook might be safe enough to test: when UnixFS decodes a
DAG-PB file with linked children, expose only the first small child-CID window
and have `FetchingBlockProvider` ask recent successful Bitswap session peers for
those children in one multi-want batch. This would avoid public gateway
fallback, avoid new provider lookups, and preserve block verification before
cache insertion.

Prototype:

- Added a default `BlockProvider::prefetch_blocks(&[Cid])` hook.
- Had UnixFS call it before serial full-file child reads and before range reads
  over sized intersecting child links.
- Implemented `FetchingBlockProvider` as a best-effort, synchronous wrapper
  around `HttpRetriever::prefetch_recent_bitswap_blocks`.
- The retriever path used only recent Bitswap session peers and the internal
  shared-client `fetch_many` batch path.
- Added `unixfs_link_batch_prefetch` and `bitswap_batch_prefetch` traces and
  mobile/harness progress mappings.

Focused validation while the prototype was present:

```sh
cargo test -p freedom-ipfs-unixfs prefetches
cargo test -p freedom-ipfs-retrieval fetching_block_provider_prefetches_recent_bitswap_children_as_batch
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
```

Result:

- focused UnixFS prefetch tests passed
- focused `FetchingBlockProvider` batch-prefetch test passed against a local
  multi-want Bitswap peer
- full UnixFS, retrieval, and gateway test suites passed
- focused mobile/harness phase-mapping tests passed

Live checks:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-link-batch-prefetch-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-link-batch-prefetch-r3.json

timeout 360s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-link-batch-prefetch-r1-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-hero-link-batch-prefetch-r1.json
```

Live result:

- `vitalik-root-html-range`: Rust and Kubo both passed `3/3`; Rust root TTFB
  p50/p95 `545ms` / `6002ms`, Kubo `1873ms` / `2861ms`; Rust max RSS/FD
  `37888KiB` / `18`, Kubo `163460KiB` / `137`.
- `ipfs-tech-developers-hero-range`: Rust and Kubo both passed `1/1`; Rust root
  TTFB `2654ms`, Kubo `2054ms`; Rust max RSS/FD `39916KiB` / `21`, Kubo
  `110752KiB` / `44`.
- Neither trace contained `unixfs_link_batch_prefetch` or
  `bitswap_batch_prefetch`.
- The harness batch summary reported `multi_cid_commands=0` and
  `max_requested_blocks=0` in both live runs.

Decision:
Reject and revert the prototype. The focused tests prove the mechanism, but
the current live corpus still does not exercise multi-link DAG-PB file children
through the gateway. The next step should be a deterministic harness case that
fetches a multi-block UnixFS file through a local Bitswap peer, not another
production hook carried without live or harness evidence.

## 2026-05-05 Keep: Local Bitswap Seed Harness Mode

Hypothesis:
Before carrying another UnixFS child-prefetch hook, the harness needs a
deterministic case that exercises a multi-block DAG-PB UnixFS file through
Bitswap rather than a cache-imported CAR. The existing stream fixture is useful
for gateway/range parity, but importing the CAR into the gateway bypasses the
retrieval path that a multi-want experiment needs to improve.

Change:

- Add `--bitswap-seed-car` to `mobile-web-harness`.
- The option imports the CAR into a separate local Kubo seed daemon.
- For Rust gateway runs, the harness starts a tiny loopback delegated-routing
  endpoint that returns the Kubo seed's Bitswap peer ID and loopback TCP
  multiaddr for provider lookups.
- For Kubo gateway runs, the harness starts a separate Kubo client daemon and
  `swarm connect`s it to the local seed before running requests.
- The option is rejected with `--gateway-url` and with `--gateway-import-car`,
  so seeded runs exercise network retrieval instead of testing an already
  populated gateway cache.
- `RunReport` records `bitswap_seed_car` for JSON artifacts.

Validation:

```sh
cargo test -p mobile-web-harness bitswap_seed
cargo test -p mobile-web-harness
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets

cargo run -p xtask -- generate-mobile-web-fixture \
  --car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --bytes 600000 \
  --range-start 262100 \
  --range-len 300 \
  --case-id bitswap-seeded-multiblock

timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-bitswap-seed-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-bitswap-seed-boundary-rust-vs-kubo.json
```

Result:

- focused harness tests passed: `5 passed; 0 failed`
- full harness tests passed: `29 passed; 0 failed`
- mobile web harness clippy passed with `-D warnings`
- workspace check passed
- fixture root CID:
  `bafybeig45rg3a5hszbyqnqanqjkgbkwrx7vjcv4uligjzw4jmhomgsshty`
- Rust and Kubo both passed `bitswap-seeded-multiblock-boundary-range`.
- Rust root TTFB `205ms`; Kubo root TTFB `53ms`.
- Rust max RSS/FD `38108KiB` / `13`; Kubo max RSS/FD `88632KiB` / `35`.
- Rust trace showed one local delegated provider lookup returning one provider.
- Rust trace showed three Bitswap incoming blocks totaling `524447` bytes:
  the DAG-PB root plus two raw UnixFS child blocks for the boundary range.
- Harness batch summary showed the current baseline is still serial
  single-CID Bitswap requests:
  `commands=3 multi_cid_commands=0 total_cids=3 max_cids=1`.

Decision:
Keep. This gives future multi-want/prefetch work a deterministic harness target:
before a production hook is kept, this seeded boundary-range run should show
`multi_cid_commands > 0` or otherwise produce better latency/resource evidence
against the same Kubo-backed seed setup.

## 2026-05-05 Reject: Blocking Session-Only UnixFS Batch Prefetch

Hypothesis:
With the local Bitswap seed harness mode in place, the earlier session-only
UnixFS child-prefetch idea could be tested against a deterministic multi-block
boundary range. The narrow version would synchronously ask recent successful
Bitswap session peers for the intersecting child CIDs in one `fetch_many` batch
before the normal serial range reads.

Prototype:

- Reintroduced a default `BlockProvider::prefetch_blocks(&[Cid])` hook.
- Had UnixFS call it for multi-link full-file reads and for range reads whose
  byte span intersects at least two child links, capped to a small CID window.
- Implemented `FetchingBlockProvider` by calling a new
  `HttpRetriever::prefetch_recent_bitswap_blocks`.
- The retriever path used only recent successful Bitswap session peers and the
  shared `fetch_many` path; no provider lookup or public gateway fallback.
- Added `unixfs_link_batch_prefetch` and `bitswap_batch_prefetch` traces while
  the prototype was present.

Focused validation while the prototype was present:

```sh
cargo test -p freedom-ipfs-unixfs prefetches
cargo test -p freedom-ipfs-retrieval prefetch_recent_bitswap_blocks_batches_missing_children
```

Focused result:

- UnixFS tests passed: `2 passed; 0 failed`
- retrieval batch-prefetch test passed: `1 passed; 0 failed`

Seeded harness validation:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-bitswap-prefetch-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-bitswap-prefetch-boundary-rust-vs-kubo.json
```

Seeded result:

- Rust and Kubo both passed the boundary-range case, but Rust latency regressed
  badly.
- Baseline seeded run before the hook: Rust root TTFB `205ms`, Kubo `53ms`.
- Prototype seeded run: Rust root TTFB `2225ms`, Kubo `53ms`.
- Rust max RSS/FD stayed low at `38424KiB` / `13`, but latency dominated.
- Trace showed the hook did fire:
  `bitswap batches: commands=4 multi_cid_commands=1 total_cids=5 max_cids=2`.
- The multi-CID prefetch command timed out after about `2001ms` and was
  cancelled; normal single-CID child fetches still delivered the range through
  incoming Bitswap blocks.
- `bitswap_batch_prefetch` appeared as `ok=false` and the request spent most of
  its time in the blocking prefetch path.

Decision:
Reject and revert the production hook. The deterministic harness did its job:
it proved this blocking prefetch shape can create a multi-CID Bitswap command,
but against a real Kubo seed it delays the user-visible response instead of
improving it. Future work should not add a synchronous UnixFS prefetch barrier.
If this area is revisited, prefer non-blocking/background overlap or improving
normal child fetch coalescing/session behavior, and require the seeded harness
to beat the `205ms` Rust baseline before keeping the change.

## 2026-05-05 Keep: Bounded Parallel Raw Range Child Fetch

Hypothesis:
The seeded boundary-range trace showed the useful work is not a multi-want
batch: it is one DAG-PB root fetch followed by two independent raw child range
fetches. A safer optimization is to let UnixFS pass adjacent raw child ranges to
the provider as a bounded batch, and let `FetchingBlockProvider` overlap the
normal single-CID retrieval path for those children. This keeps existing block
verification, provider routing, Bitswap session behavior, and cache insertion
semantics instead of adding a new prefetch barrier.

Change:

- Add default `BlockProvider::get_block_ranges`.
- Use it from UnixFS range reads for intersecting raw child links, capped to
  `4` child ranges per batch.
- Preserve recursive `read_file_cid_range` behavior for non-raw child links.
- Override `FetchingBlockProvider::get_block_ranges` to:
  - serve cached ranges immediately
  - overlap uncached child fetches with existing `fetch_block_with_source`
  - record the same retrieval stats and trace phases as normal block fetches
  - emit `block_range_batch_fetch` for each fetched child range

Focused validation:

```sh
cargo test -p freedom-ipfs-unixfs file_range_batches_adjacent_raw_child_ranges
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_requests_multiple_blocks_from_one_peer
```

Regression validation:

```sh
cargo test -p freedom-ipfs-core
cargo test -p freedom-ipfs-unixfs
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo fmt --all --check
cargo clippy -p freedom-ipfs-core -p freedom-ipfs-unixfs -p freedom-ipfs-retrieval -p freedom-ipfs-gateway --all-targets -- -D warnings
cargo check --workspace --all-targets
git diff --check
```

Seeded comparison:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-batch-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-batch-boundary-rust-vs-kubo.json

timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-batch-boundary-r3-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-batch-boundary-r3-rust-vs-kubo.json
```

Result:

- focused UnixFS range-batch test passed
- focused retrieval Bitswap batch sanity test still passed
- regression validation passed:
  - `freedom-ipfs-core`: `5 passed`
  - `freedom-ipfs-unixfs`: `15 passed`
  - `freedom-ipfs-retrieval`: `65 passed; 1 ignored`
  - `freedom-ipfs-gateway`: gateway library, binary, CLI, and parsing tests passed
  - affected-crate clippy passed with `-D warnings`
  - workspace check passed
  - formatting and diff whitespace checks passed
- one-run seeded comparison passed Rust and Kubo:
  - Rust root TTFB `187ms`; Kubo `53ms`
  - Rust max RSS/FD `38488KiB` / `13`; Kubo `90368KiB` / `45`
- three-run fresh-gateway seeded comparison passed Rust and Kubo `3/3`:
  - Rust root TTFB p50/p95 `194ms` / `196ms`
  - Kubo root TTFB p50/p95 `56ms` / `58ms`
  - Rust max RSS/FD `39304KiB` / `13`; Kubo `91008KiB` / `38`
- Trace showed bounded overlap through normal single-CID Bitswap:
  `block_range_batch_fetch=6`, `bitswap_session_shortcut` hits `6`,
  `multi_cid_commands=0`, and no prefetch timeout errors.
- Compared with the seeded one-run baseline before this change, Rust improved
  from `205ms` to `187ms`. The repeat run shows the optimized cold path is
  stable around `194ms` p50 on this fixture.

Decision:
Keep. This is a modest latency improvement, but it is structurally safer than
the rejected multi-want prefetch: bounded fanout, no public fallback, no
unverified caching, no new provider source, no synchronous prefetch timeout, and
mobile resource usage remains low. Future work should use the seeded harness to
continue driving this gap toward Kubo's `~56ms` p50.

## 2026-05-05 Reject: Split-Response Reader and Session Range Multi-Want Hook

Hypothesis:
One reason the earlier multi-want prefetch failed could be that the outgoing
multi-want stream reader stopped after the first non-empty Bitswap response.
That only works if a peer sends every requested block in one frame. Kubo may
split requested blocks across multiple responses, so the stream reader should
continue until it has all requested CIDs, sees terminal `DONT_HAVE` coverage, or
hits a small response-frame cap.

Reader prototype:

- Add `MAX_BITSWAP_RESPONSE_FRAMES = 16`.
- For outgoing Bitswap block wants, read response frames until all requested
  CIDs are collected instead of stopping at the first non-empty frame.
- Add focused coverage:

```sh
cargo test -p freedom-ipfs-retrieval multi_want_stream_collects_blocks_split_across_responses
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_requests_multiple_blocks_from_one_peer
```

Rejected production hook:

- Prototype: after the reader fix, `FetchingBlockProvider::get_block_ranges`
  tried to fetch the uncached raw child range CIDs through one recent-session
  Bitswap `fetch_many` command with a `150ms` cap, then fell back to the
  existing parallel single-CID path.
- This used no public gateway fallback and did not bypass verification, but it
  still put a new timeout on the user-visible range path.

Validation:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-multiwant-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-multiwant-boundary-rust-vs-kubo.json
```

Result:

- focused split-response stream reader test passed
- existing shared-client multi-want local-peer test still passed
- seeded harness with the range multi-want hook passed Rust and Kubo, but Rust
  regressed:
  - Rust root TTFB `329ms`; Kubo `58ms`
  - Rust max RSS/FD `39192KiB` / `12`; Kubo `86220KiB` / `39`
- Trace showed the prototype did create a multi-CID command:
  `commands=4 multi_cid_commands=1 total_cids=5 max_cids=2`
- The session range batch timed out/cancelled at `151ms`:
  `bitswap_session_range_batch ok=false`, `bitswap_fetch_cancelled=151ms`
- The existing fallback then fetched the two child blocks successfully through
  normal session shortcuts, so the new hook only added latency.
- After reverting only the range multi-want hook and keeping the split-response
  reader, the same one-run seeded check still regressed:
  - command:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-split-reader-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-split-reader-boundary-rust-vs-kubo.json
```

  - Rust root TTFB `335ms`; Kubo `52ms`
  - Rust max RSS/FD `39576KiB` / `13`; Kubo `86500KiB` / `37`
  - Trace showed no multi-CID commands, but one child request still spent
    `180ms` in a cancelled Bitswap fetch and only completed after provider
    lookup fallback.

Decision:
Reject and revert both code changes. The split-response reader looked correct in
isolation, but live seeded evidence showed it changes the behavior of competing
single-CID outgoing requests: a losing outgoing stream can stay alive waiting for
its exact CID instead of failing/cancelling quickly while the incoming Bitswap
path delivers another block. Future multi-want work should first prove Kubo
responds through our outgoing multi-want path without adding a blocking fallback
penalty, or should race without materially increasing mobile network/resource
usage.

## 2026-05-05 Reject: Deferred Large Bitswap Store Writes

Hypothesis:
The bounded raw range batching trace showed child blocks arriving before the
gateway response completes. For 262KB raw child blocks, part of the tail appears
between `bitswap_session_shortcut` and `block_fetch_total`, which includes the
synchronous SQLite `put_block`. Deferring large no-extra Bitswap block writes to
a background blocking task might shave TTFB while still verifying before
serving.

Prototype:

- For Bitswap results with no extra blocks and requested block size at least
  `64KiB`, verify the requested block immediately, return it to the caller, and
  run `store.put_block` in `tokio::task::spawn_blocking`.
- Emit `bitswap_store_deferred` when the background write finishes.
- Keep small blocks and extra-block results on the synchronous path.

Focused validation:

```sh
cargo test -p freedom-ipfs-retrieval fetches_block_from_local_bitswap_peer
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_shortcut_fetches_when_provider_lookup_fails
```

Seeded validation:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-deferred-store-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-deferred-store-boundary-rust-vs-kubo.json

timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-deferred-store-boundary-r3-rust-trace.jsonl \
  --comparison-output /tmp/harness-deferred-store-boundary-r3-rust-vs-kubo.json
```

Result:

- focused tests passed
- one-run seeded comparison passed Rust and Kubo:
  - Rust root TTFB `186ms`; Kubo `52ms`
  - Rust max RSS/FD `39288KiB` / `12`; Kubo `87696KiB` / `45`
- three-run fresh-gateway seeded comparison passed Rust and Kubo `3/3`:
  - Rust root TTFB p50/p95 `188ms` / `194ms`
  - Kubo root TTFB p50/p95 `55ms` / `57ms`
  - Rust max RSS/FD `39236KiB` / `13`; Kubo `87472KiB` / `43`
- Compared with the kept bounded range-batch repeat baseline
  (`194ms` / `196ms` Rust p50/p95), this was a small p50 improvement and only a
  tiny p95 improvement.

Decision:
Reject and revert. The latency signal is real but modest, and the semantic cost
is not worth it: `fetch_block_with_source` can return a large Bitswap block
before that block is durably cached. That weakens offline/cache-after-browse
behavior for exactly the large media/range blocks we care about. A future cache
write optimization should preserve the post-fetch cache contract, for example
by making the store path cheaper or adding a bounded write-behind queue with an
explicit flush/visibility contract.

## 2026-05-05 Reject: Range-Batch 100ms Session Head Start

Hypothesis:
The global `75ms` Bitswap session pre-lookup grace is a good default for normal
page assets, but batched raw UnixFS range child reads have a narrower shape:
adjacent child CIDs are requested together after the parent has already reached a
useful session peer. A scoped `100ms` pre-lookup grace only for
`BlockProvider::get_block_ranges` with multiple children might let those child
blocks arrive through the session shortcut before provider lookup/post-lookup
fallback, reducing deterministic range TTFB without affecting normal requests.

Prototype:

- Added `BITSWAP_RANGE_BATCH_SESSION_PRE_LOOKUP_GRACE = 100ms`.
- Threaded an optional pre-lookup grace through uncached block fetches.
- Used the `100ms` grace only for multi-CID raw range batches; all other block
  fetches kept the existing `75ms` policy.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-unixfs file_range_batches_adjacent_raw_child_ranges
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_head_start_can_avoid_provider_lookup
```

All focused checks passed.

Seeded one-run validation:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-headstart100-boundary-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-headstart100-boundary-rust-vs-kubo.json
```

Result:

- Rust and Kubo passed.
- Rust root TTFB `132ms`; Kubo `54ms`.
- Rust max RSS/FD `39508KiB` / `13`; Kubo `88824KiB` / `40`.
- Rust gateway elapsed p50 `130ms`; gateway direct body max `60ms`.
- Delegated lookup events `1` for the root only.
- Range child shortcut hits `2/2`; no post-lookup waits.

Seeded three-run fresh-gateway validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-headstart100-boundary-r3-rust-trace.jsonl \
  --comparison-output /tmp/harness-range-headstart100-boundary-r3-rust-vs-kubo.json
```

Result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 `190ms` / `193ms`.
- Kubo root TTFB p50/p95 `52ms` / `52ms`.
- Rust max RSS/FD `39588KiB` / `13`; Kubo `91404KiB` / `35`.
- Delegated lookup events dropped to `3` total, compared with `5` in the
  previous kept bounded range-batch repeat baseline.
- Range child shortcut hits `6/6`; no post-lookup waits.
- Compared with the kept bounded range-batch baseline
  (`194ms` / `196ms` Rust p50/p95), this was only a tiny latency improvement.

Live range validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --asset-concurrency 6 \
  --trace-output /tmp/vitalik-range-headstart100-rust-vs-kubo-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-range-headstart100-rust-vs-kubo-r3.json
```

Result:

- Rust and Kubo passed `3/3`.
- Rust range/root TTFB p50/p95 `5ms` / `448ms`.
- Kubo range/root TTFB p50/p95 `3ms` / `1962ms`.
- Rust max RSS/FD `37376KiB` / `18`; Kubo `174744KiB` / `86`.
- The prior kept `75ms` range check on the same live case was better for Rust
  (`4ms` / `373ms` p50/p95), though live network variance is likely.
- This case did not meaningfully exercise multi-child `get_block_ranges`; the
  trace had `block_store_get_range=3` and no clear range-batch signal.

Decision:
Reject and revert. The seeded provider lookup reduction is real, but the
latency win is too small to justify threading a second Bitswap pre-lookup grace
through the retrieval path. Live range evidence was not better, and the only
clear deterministic gain was `194ms` / `196ms` to `190ms` / `193ms` p50/p95.
Keep the current global `75ms` pre-lookup policy unless a future experiment
finds a larger, repeatable benefit without increasing mobile request latency.

## 2026-05-05 Keep: Multi-CID Incoming Bitswap Batch Matching

Hypothesis:
The rejected range multi-want experiment may have failed partly because
`SharedBitswapClient::fetch_many` only listened for incoming Bitswap block
delivery on single-CID commands. If a Kubo peer answers a multi-want by opening
incoming Bitswap streams, the batch request can time out even though the blocks
arrive through the same mechanism that already serves normal single-CID
requests. Supporting incoming delivery for all requested CIDs is a safer
building block than reintroducing another UnixFS prefetch hook.

Change:

- Register incoming Bitswap waiters for every CID in a shared-client command,
  not only the primary CID.
- Aggregate incoming one-block results until every requested CID in the
  multi-CID command has been received.
- Keep the outgoing stream request race unchanged.
- Emit `bitswap_incoming_batch` only for multi-CID incoming completions, so
  normal single-CID trace volume does not change.
- Add a deterministic test peer that accepts a multi-want on an outgoing stream
  but delivers each requested block back over incoming Bitswap streams. This
  reproduces the response shape the earlier Kubo-backed experiment needed to
  tolerate.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many
cargo test -p freedom-ipfs-retrieval multi_want_stream
cargo test -p freedom-ipfs-retrieval
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
git diff --check
```

Result:

- focused incoming multi-CID test passed
- shared-client `fetch_many` tests passed: `3 passed`
- stream-level multi-want tests passed: `2 passed`
- full retrieval suite passed: `66 passed; 0 failed; 1 ignored`
- retrieval clippy passed with `-D warnings`
- formatting and diff whitespace checks passed

Decision:
Keep as a Priority-1 multi-want building block. This does not claim a live
page-load speed win and does not change gateway/UnixFS retrieval behavior by
itself. It makes the next range/session batching experiment better scoped:
future work can test a non-blocking or raced multi-CID range request against the
local Kubo seed while knowing that incoming Kubo-style block delivery can
satisfy the batch instead of timing out.

## 2026-05-05 Reject: Raced Multi-CID Range Shortcut

Hypothesis:
After keeping multi-CID incoming Bitswap batch matching, the range path could
race a recent-peer multi-CID shortcut against the existing individual raw-child
fetches. Unlike the earlier blocking prefetch hook, this should not delay the
current path when the batch is slow; it should only win if the batch receives
and stores all child blocks first.

Prototype:

- Added `HttpRetriever::fetch_many_from_recent_bitswap_peers`.
- In `FetchingBlockProvider::get_block_ranges_async`, for multi-CID uncached
  range batches:
  - start the normal individual `fetch_block_with_source` child fetches
  - also start a recent-session-peer `fetch_many` batch for the same child CIDs
  - return the batch result only if it completed all requested blocks first
  - otherwise let the existing individual path return as before
- Preserved verification and durable cache insertion before serving batch
  results.

Focused validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-unixfs file_range_batches_adjacent_raw_child_ranges
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks
```

Focused checks passed.

Seeded one-run validation:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-raced-multi-incoming-r1-trace.jsonl \
  --comparison-output /tmp/harness-range-raced-multi-incoming-r1.json
```

Result:

- Rust and Kubo passed.
- Rust root TTFB `181ms`; Kubo `54ms`.
- Rust max RSS/FD `39712KiB` / `13`; Kubo `88072KiB` / `42`.
- Trace exercised the new shape: `multi_cid_commands=1`,
  `bitswap_incoming_batch=1`, incoming child blocks delivered to both the batch
  waiter and the individual waiters.
- The batch did not win the response path; individual child fetches still
  produced `block_fetch_total` for both raw children.

Seeded three-run fresh-gateway validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-raced-multi-incoming-r3-trace.jsonl \
  --comparison-output /tmp/harness-range-raced-multi-incoming-r3.json
```

Result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 `169ms` / `207ms`.
- Kubo root TTFB p50/p95 `55ms` / `57ms`.
- Rust max RSS/FD `40316KiB` / `13`; Kubo `90920KiB` / `45`.
- Compared with the kept bounded range-batch baseline
  (`194ms` / `196ms` Rust p50/p95), p50 improved but p95 regressed.
- Trace showed `multi_cid_commands=3`, `bitswap_incoming_batch=3`, and
  `delivered_waiters=15`, meaning every run added extra multi-CID work.
- Only one request group clearly returned through `bitswap_session_batch_shortcut`.
  Other runs either let the individual path win or paid enough durable store
  cost after the batch completed that the response tail was worse.

Decision:
Reject and revert the production hook. The incoming batch mechanism works, but
the raced range shortcut adds duplicate Bitswap and cache-write pressure for an
unstable latency tradeoff: better p50, worse p95, and no movement toward Kubo's
`~55ms` seeded p50. Keep the lower-level multi-CID incoming support, but do not
wire it into `get_block_ranges_async` in this shape. A future attempt should
first remove duplicate large-block cache writes without weakening the
post-fetch cache contract, or find a request shape that wins before the current
individual child fetches complete.

## 2026-05-05 Keep: Summarize Incoming Bitswap Batches

Hypothesis:
After keeping `bitswap_incoming_batch`, future multi-want experiments need an
obvious harness/mobile signal for whether an incoming batch completed, how many
CIDs it covered, and whether the UI should still report Bitswap activity. The
raw trace event was visible only through slow-event output and generic phase
counts.

Change:

- Add `bitswap_incoming_batches` to the mobile web harness trace summary JSON.
- Print a concise `bitswap incoming batches:` line in normal and Kubo comparison
  trace summaries.
- Track event count, total/max CID count, requested/extra block totals, and max
  elapsed time.
- Map `bitswap_incoming_batch` to the mobile/harness progress phase
  `fetching_bitswap`.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
```

Result:

- focused harness incoming-batch summary test passed
- focused harness progress-phase summary test passed
- focused mobile progress mapping test passed

Decision:
Keep. This is diagnostics-only and does not change gateway/retrieval behavior.
It makes the kept multi-CID incoming support measurable in future live and
seeded Kubo comparisons.

## 2026-05-05 Keep: Summarize Bitswap Connection Latency

Hypothesis:
The seeded boundary-range traces show cold root latency is heavily influenced by
Bitswap connection establishment, but the harness only summarized connection
transports and errors. Future Rust-vs-Kubo comparisons need a direct connection
latency line so dial/handshake cost is visible without scanning slow events.

Change:

- Add `bitswap_connection_established` to the harness trace summary JSON.
- Summarize connection event count, `established_ms`, `wait_elapsed_ms`, and
  failed dial count.
- Print a concise `bitswap connections:` line that includes latency summaries
  and transport counts.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p mobile-web-harness trace_summary_counts_bitswap_peer_attempts
```

Result:

- focused slow-event trace summary test passed
- focused Bitswap peer/connection summary test passed

Decision:
Keep. This is harness-only and does not change retrieval behavior. It gives the
next Kubo-gap experiment an explicit dial/connection metric alongside request
TTFB, provider lookup, and Bitswap batch summaries.

## 2026-05-05 Keep: Record Seeded Harness Connection Setup

Hypothesis:
The local Bitswap seed harness is useful for deterministic Rust-vs-Kubo range
comparisons, but the two engines are intentionally wired differently: Rust uses
the seed delegated router and discovers/dials the provider during the timed
request, while the Kubo client receives a `swarm connect` to the seed before
timed requests start. Future Kubo-gap experiments need this context in the JSON
artifact and console summary, not only in this doc.

Change:

- Add `bitswap_seed_connection_setup` to `RunReport`.
- Report `delegated_router_provider_lookup` for Rust seeded runs.
- Report `swarm_connect_before_request` for Kubo seeded runs.
- Print the same value in the normal harness summary when `--bitswap-seed-car`
  is active.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness bitswap_seed
cargo test -p mobile-web-harness
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused Bitswap seed harness tests passed: `6 passed; 0 failed`
- full mobile web harness tests passed: `30 passed; 0 failed`
- harness clippy passed with `-D warnings`

Live sanity check:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-seed-setup-metadata-r1-trace.jsonl \
  --comparison-output /tmp/harness-seed-setup-metadata-r1.json
```

Live result:

- Rust and Kubo passed `bitswap-seeded-multiblock-boundary-range`.
- JSON artifact labels:
  - Rust `bitswap_seed_connection_setup`:
    `delegated_router_provider_lookup`
  - Kubo `bitswap_seed_connection_setup`: `swarm_connect_before_request`
- Rust root TTFB `180ms`; Kubo root TTFB `52ms`.
- Rust max RSS/FD `39188KiB` / `13`; Kubo max RSS/FD `85864KiB` / `35`.
- Rust trace summary reported one Bitswap connection establishment:
  `established_ms` p50/max `50ms`, `wait_elapsed_ms` p50/max `51ms`.

Decision:
Keep. This is diagnostics-only and does not change retrieval behavior. It makes
seeded comparison artifacts explicit that Kubo's seeded root TTFB excludes the
seed dial/connect step while Rust's seeded root TTFB includes provider lookup
and Bitswap connection establishment.

## 2026-05-05 Keep: Record Kubo Seed Preconnect Cost

Hypothesis:
After labeling the seeded setup, the harness should also record the actual
elapsed cost of Kubo's pre-request `ipfs swarm connect`. That connect cost is
outside the timed root request, so without recording it the JSON artifact still
does not show how much setup work Kubo has already completed before TTFB starts.

Change:

- Time the Kubo seed `swarm connect` call in `SpawnedGateway::start_kubo`.
- Store the elapsed value as `bitswap_seed_connect_elapsed_ms` on each
  `RunResult`.
- Add `bitswap_seed_connect_ms` to `RepeatSummary`.
- Print `bitswap seed connect: rust=... kubo=...` in comparison summaries when
  either engine has a measured seed connect value.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness bitswap_seed
cargo test -p mobile-web-harness repeat_summary_aggregates_measured_resource_metrics
cargo test -p mobile-web-harness
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused Bitswap seed harness tests passed: `6 passed; 0 failed`
- focused repeat-summary metric test passed: `1 passed; 0 failed`
- full mobile web harness tests passed: `30 passed; 0 failed`
- harness clippy passed with `-D warnings`

Live sanity check:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-seed-connect-elapsed-r1-trace.jsonl \
  --comparison-output /tmp/harness-seed-connect-elapsed-r1.json
```

Live result:

- Rust and Kubo passed `bitswap-seeded-multiblock-boundary-range`.
- Comparison summary printed:
  `bitswap seed connect: rust=n/a kubo=p50=57ms p90=57ms p95=57ms max=57ms`.
- JSON artifact recorded Rust `bitswap_seed_connect_elapsed_ms: null` and Kubo
  `bitswap_seed_connect_elapsed_ms: 57`.
- Rust root TTFB `205ms`; Kubo root TTFB `53ms`.
- Rust max RSS/FD `39112KiB` / `13`; Kubo max RSS/FD `90980KiB` / `37`.
- Rust Bitswap connection establishment remained inside request timing:
  `established_ms` p50/max `66ms`, `wait_elapsed_ms` p50/max `67ms`.

Decision:
Keep. This is diagnostics-only and does not change retrieval behavior. Seeded
Kubo comparisons now make the excluded preconnect cost visible next to request
TTFB, which prevents the remaining Rust-vs-Kubo gap from being interpreted as
pure block-transfer speed.

## 2026-05-05 Keep: Trace Block Store Put Latency

Hypothesis:
The seeded boundary-range trace showed time between Bitswap success and
`block_fetch_total`, especially for the two 256KiB raw child blocks. That hidden
span was likely durable cache insertion, but the trace only reported cache
reads. Before changing cache-write behavior, the harness needs direct write
timing.

Change:

- Emit `block_store_put` around HTTP-provider and Bitswap `put_block` calls.
- Include CID, source, required-vs-extra block, success/failure, byte count, and
  elapsed time.
- Keep extra Bitswap block store failures best-effort, matching previous
  behavior.
- Extend the mobile web harness trace summary with block-store put count, bytes,
  failures, total elapsed time, and max elapsed time.
- Map `block_store_put` to the mobile progress phase `streaming`, so progress
  summaries do not expose it as an unknown raw phase.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_block_store_puts
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused harness block-store-put summary test passed
- focused retrieval incoming multi-CID regression passed
- full retrieval suite passed: `66 passed; 0 failed; 1 ignored`
- full mobile web harness suite passed: `31 passed; 0 failed`
- retrieval and harness clippy passed with `-D warnings`

Live sanity check:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-block-store-put-r1-trace.jsonl \
  --comparison-output /tmp/harness-block-store-put-r1.json
```

Live result:

- Rust and Kubo passed `bitswap-seeded-multiblock-boundary-range`.
- Rust root TTFB `190ms`; Kubo root TTFB `57ms`.
- Kubo seed preconnect was `58ms`, still outside Kubo request timing.
- Rust trace recorded `block_store_put` for all three fetched blocks:
  - events `3`
  - bytes `524447`
  - failures `0`
  - total elapsed `40ms`
  - max elapsed `21ms`
- The write cost confirms that a meaningful part of the post-root child-fetch
  tail is durable cache insertion, not only Bitswap/network wait.

Decision:
Keep. This is diagnostics-only and preserves read-only, no-public-fallback, and
verified-before-caching behavior. The next optimization attempt can now measure
whether any cache-write overlap or duplicate-write reduction actually moves
request latency instead of guessing from `block_fetch_total`.

## 2026-05-05 Keep: Offload Block Store Writes From Async Workers

Hypothesis:
The `block_store_put` trace showed 256KiB child block writes sitting on the
request path. Even if the gateway still waits for durable cache insertion before
serving, running SQLite writes directly on the async worker can delay sibling
range fetches and Bitswap progress. Moving the verified `put_block` call to
Tokio's blocking pool should preserve the existing cache-before-return contract
while reducing event-loop interference.

Change:

- Make retrieval's traced block-store write helper async.
- Run `SqliteBlockStore::put_block` inside `tokio::task::spawn_blocking`.
- Still await the store result before returning HTTP-provider or Bitswap blocks.
- Continue verifying blocks before serving/caching because `put_block` remains
  the write path.
- Preserve best-effort behavior for extra Bitswap blocks.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_shortcut_fetches_when_provider_lookup_fails
```

Regression validation:

```sh
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused retrieval tests passed
- full retrieval suite passed: `66 passed; 0 failed; 1 ignored`
- full mobile web harness suite passed: `31 passed; 0 failed`
- retrieval and harness clippy passed with `-D warnings`

Seeded one-run sanity check:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-block-store-spawn-blocking-r1-trace.jsonl \
  --comparison-output /tmp/harness-block-store-spawn-blocking-r1.json
```

One-run result:

- Rust and Kubo passed.
- Rust root TTFB `184ms`; Kubo root TTFB `50ms`.
- Kubo seed preconnect was `46ms`, outside Kubo request timing.
- Rust max RSS/FD `39168KiB` / `13`; Kubo max RSS/FD `90748KiB` / `37`.
- Rust block-store puts: events `3`, bytes `524447`, total elapsed `14ms`,
  max elapsed `7ms`.

Seeded three-run fresh-gateway comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-block-store-spawn-blocking-r3-trace.jsonl \
  --comparison-output /tmp/harness-block-store-spawn-blocking-r3.json
```

Three-run result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 `179ms` / `186ms`.
- Kubo root TTFB p50/p95 `56ms` / `58ms`.
- Kubo seed preconnect p50/p95 `55ms` / `56ms`, outside Kubo request timing.
- Rust max RSS/FD `39548KiB` / `13`; Kubo max RSS/FD `90752KiB` / `43`.
- Rust block-store puts: events `9`, bytes `1573341`, total elapsed `66ms`,
  max elapsed `19ms`.
- Compared with the previous kept bounded range-batch baseline
  (`194ms` / `196ms` Rust p50/p95, RSS/FD `39304KiB` / `13`), this improves
  both p50 and p95 without increasing FD count and with only minor RSS variance.

Decision:
Keep. This is a measurable latency improvement on the deterministic seeded
range case, preserves verification and cache-before-return semantics, avoids
public fallback, and keeps mobile resource use low. The remaining seeded gap is
now less about SQLite write blocking and more about root-to-child Bitswap
round-trip structure.

Live public range validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-block-store-spawn-blocking-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-block-store-spawn-blocking-r3.json
```

Live public result:

- Rust and Kubo passed `vitalik-root-html-range` `3/3`.
- Rust root TTFB p50/p95 `1475ms` / `5243ms`.
- Kubo root TTFB p50/p95 `2831ms` / `3345ms`.
- Rust max RSS/FD `38144KiB` / `18`; Kubo max RSS/FD `170624KiB` / `98`.
- Rust block-store puts were small on this case: events `6`, bytes `116319`,
  total elapsed `9ms`, max elapsed `3ms`.
- The p95 tail came from delegated provider lookup / HTTP provider variability,
  not local store writes. The trace had two HTTP-provider hash-mismatch errors
  for the child block, but the request still passed through verified fallback.

## 2026-05-05 Keep: Summarize HTTP Provider Fetch Quality

Hypothesis:
Live `vitalik-root-html-range` checks show tails from delegated provider lookup
and verified HTTP-provider fallback, including CID hash mismatches. The harness
previously exposed HTTP-provider attempts only as raw slow events and generic
trace errors, which makes provider-quality experiments harder to compare.

Change:

- Add `bytes` to successful `http_provider_fetch` trace events.
- Add `http_provider_fetches` to the mobile web harness trace summary.
- Track HTTP-provider event count, successes, failures, returned bytes, elapsed
  latency summary, provider URL counts, and coarse error classes.
- Print a concise `http provider fetches:` line in both normal and comparison
  trace summaries.
- Classify common errors including `cid_hash_mismatch`, timeout, HTTP 404/429,
  HTTP 5xx, redirect, and other.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p mobile-web-harness trace_summary
cargo test -p freedom-ipfs-retrieval http_provider
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused HTTP-provider harness summary test passed
- focused harness trace-summary tests passed: `8 passed; 0 failed`
- focused retrieval HTTP-provider tests passed: `3 passed; 0 failed`
- full retrieval suite passed: `66 passed; 0 failed; 1 ignored`
- full mobile web harness suite passed: `32 passed; 0 failed`
- retrieval and harness clippy passed with `-D warnings`

Live sanity check:

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-provider-summary-r1-trace.jsonl \
  --comparison-output /tmp/vitalik-http-provider-summary-r1.json
```

Live result:

- Rust and Kubo passed `vitalik-root-html-range`.
- Rust root TTFB `1209ms`; Kubo root TTFB `3121ms`.
- Rust max RSS/FD `30848KiB` / `15`; Kubo max RSS/FD `118888KiB` / `71`.
- New summary line:
  `http provider fetches: events=3 successes=2 failures=1 bytes=38773
  elapsed=p50=60ms p90=994ms p95=994ms max=994ms
  providers=https://trustless.filebase.io/=2,
  https://indexer.storacha.network/=1 error_classes=cid_hash_mismatch=1`
- The slow child fetch was a `994ms` CID hash mismatch from an HTTP provider,
  followed by verified fallback. This gives the next provider-quality experiment
  a precise counter and source-provider signal.

Decision:
Keep. This is diagnostics-only and does not alter provider selection, public
fallback behavior, block verification, or caching. It makes HTTP-provider
quality visible enough to safely test provider suppression/racing changes later.

## 2026-05-05 Keep: Race Bounded HTTP Provider Candidates

Hypothesis:
The previous HTTP-provider quality summary showed a live range request spending
`994ms` on one HTTP provider that returned a CID hash mismatch before verified
fallback recovered. When delegated routing returns multiple HTTP providers for a
CID, trying them strictly one-at-a-time lets one slow, stale, or invalid provider
dominate TTFB. Racing a very small number of routing-provided HTTP providers
should reduce public-network tails while preserving the read-only, verified
retrieval model.

Change:

- Collect routing-provided HTTP provider base URLs after bad-provider filtering.
- Race at most `2` HTTP provider candidates per CID.
- Cap global concurrent HTTP-provider fetches at `4` across the retriever.
- Return the first verified block and drop slower in-flight candidates.
- Continue to request only provider records returned by routing; no public
  gateway fallback is added.
- Continue verifying each block before storing or serving it through the existing
  `fetch_from_http_provider` and block-store path.
- Keep per-provider success/failure trace events and add an `http_provider_race`
  trace event for progress and harness summaries.
- Map `http_provider_race` to the mobile-facing `fetching_http_provider` phase
  in both the mobile FFI progress snapshot and harness trace summaries.

Focused validation:

```sh
cargo test -p freedom-ipfs-retrieval races_http_provider_candidates_and_returns_first_verified_block
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
```

Regression validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo clippy -p freedom-ipfs-mobile --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- focused retrieval HTTP-provider race test passed
- focused mobile progress mapping test passed
- focused harness HTTP-provider summary test passed
- full retrieval suite passed: `67 passed; 0 failed; 1 ignored`
- full mobile suite passed: `26 passed; 0 failed`
- full mobile web harness suite passed: `32 passed; 0 failed`
- retrieval, mobile, and harness clippy passed with `-D warnings`

Live one-run sanity check:

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-provider-race-r1-trace.jsonl \
  --comparison-output /tmp/vitalik-http-provider-race-r1.json
```

One-run result:

- Rust and Kubo passed `vitalik-root-html-range`.
- Rust root TTFB `154ms`; Kubo root TTFB `1834ms`.
- Rust max RSS/FD `31104KiB` / `15`; Kubo max RSS/FD `125524KiB` / `71`.
- HTTP-provider fetches: events `2`, successes `2`, failures `0`, bytes
  `38773`, elapsed p50/p95 `20ms` / `51ms`.
- Provider lookups succeeded for both blocks, with max delegated lookup elapsed
  `50ms`.

Live three-run comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-provider-race-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-http-provider-race-r3.json
```

Three-run result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 `156ms` / `168ms`.
- Kubo root TTFB p50/p95 `2932ms` / `3208ms`.
- Rust max RSS/FD `31488KiB` / `15`; Kubo max RSS/FD `198856KiB` / `160`.
- HTTP-provider fetches: events `6`, successes `6`, failures `0`, bytes
  `116319`, elapsed p50/p95 `21ms` / `36ms`.
- Compared with the immediately previous HTTP-provider summary sanity run
  (`1209ms` Rust TTFB with one `994ms` CID hash mismatch), this removes the
  observed slow-provider tail on this live case. Public-network variance still
  means this is not a universal speed claim, but the signal is strong and the
  guardrails are narrow.

Seeded Bitswap boundary regression check:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-http-provider-race-seeded-r3-trace.jsonl \
  --comparison-output /tmp/harness-http-provider-race-seeded-r3.json
```

Seeded result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 `181ms` / `186ms`.
- Kubo root TTFB p50/p95 `57ms` / `59ms`.
- Kubo seed preconnect p50/p95 was `54ms` / `54ms`, outside Kubo request timing.
- Rust max RSS/FD `39232KiB` / `13`; Kubo max RSS/FD `87852KiB` / `33`.
- The result is neutral against the previous kept seeded baseline
  (`179ms` / `186ms` Rust p50/p95) and does not introduce HTTP-provider work in
  the seeded Bitswap-only path.

Decision:
Keep. The change is narrowly bounded, uses only routing-provided HTTP
candidates, preserves verification-before-store/serve semantics, avoids public
gateway fallback, and improves the live public range workload that motivated the
experiment without regressing the deterministic seeded Bitswap case or mobile
resource profile.

## 2026-05-05 Keep: Retry Empty Delegated Provider Results Once

Hypothesis:
`daicowtf-page-assets` still exposes a sparse-provider reliability gap. In one
same-window Rust/Kubo comparison, Kubo passed all three fresh runs while Rust
passed only one. The two Rust failures were for the recurring child CID
`bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u`: delegated
routing returned `0` providers, the full `3s` DHT lookup found none, and the
request failed. Seconds later, the same delegated router returned the Pinata WSS
provider for that CID. A single short delegated retry after an empty result may
catch transient router misses before spending the full DHT budget.

Change:

- In `AutoRoutingClient`, when delegated routing returns an empty provider set,
  wait `100ms` and query delegated routing once more before falling through to
  full light-DHT provider lookup.
- Keep this scoped to empty delegated results only. Non-empty low-diversity
  results keep the existing short DHT merge path.
- Record normal delegated lookup stats for the retry and emit a
  `delegated_provider_empty_retry` trace event.
- Map the new trace phase to `provider_lookup` progress and
  `delegated_routing` source in the mobile FFI progress snapshot and harness
  summary.
- No public gateway fallback is added; retrieval still uses only routing records
  and verified block fetches.

Baseline failure that motivated the change:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-http-provider-race-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-http-provider-race-r3.json
```

Baseline result:

- Rust passed `1/3`; Kubo passed `3/3`.
- Rust root TTFB p50/p95 `3297ms` / `4097ms`; Kubo `3327ms` / `4444ms`.
- Rust max RSS/FD `44492KiB` / `20`; Kubo `105668KiB` / `61`.
- Failed Rust responses were `504`.
- Failed child CID:
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u`.
- Failure shape: delegated provider lookup returned `0`, full DHT provider
  lookup timed out after about `3008-3012ms`, and the recent root-serving
  session peer timed out for that child.
- The one successful Rust run saw delegated routing return one WSS provider for
  that child, then fetched it from `Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4`
  over WSS in `559ms`.

Direct delegated-router sample after the failure:

```sh
for i in $(seq 1 12); do
  curl -fsS -H 'Accept: application/x-ndjson' \
    'https://delegated-ipfs.dev/routing/v1/providers/bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u' |
    wc -l
  sleep 1
done
```

Result: all twelve samples returned `1` line. The provider record was:

```json
{"Addrs":["/dnsaddr/bitswap-v3.pinata.cloud"],"ID":"Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4","Protocols":["transport-bitswap"],"Schema":"peer","transport-bitswap":"gBI="}
```

Focused validation:

```sh
cargo test -p freedom-ipfs-routing auto_routing_retries_empty_delegated_result_before_dht
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
```

Result: all focused tests passed. The routing test proves an empty delegated
response followed by a non-empty delegated retry avoids DHT fallback.

Live follow-up:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-empty-delegated-retry-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-empty-delegated-retry-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 `1632ms` / `1826ms`; Kubo `3108ms` / `3116ms`.
- Rust max RSS/FD `44100KiB` / `20`; Kubo `107224KiB` / `74`.
- Rust source peers: root from
  `12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP` over TCP; child
  blocks from `Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4` over WSS.
- The new `delegated_provider_empty_retry` event did not fire in this follow-up
  because delegated routing returned the WSS provider on the first lookup in
  each repeat. Treat this run as a guardrail, not proof of a direct live speed
  win.

Additional live guardrail:

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-empty-delegated-retry-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-empty-delegated-retry-r3.json
```

Guardrail result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 `162ms` / `514ms`; Kubo `2913ms` / `3145ms`.
- Rust max RSS/FD `31488KiB` / `15`; Kubo `123456KiB` / `78`.
- HTTP provider fetches remained healthy: events `6`, successes `6`, failures
  `0`, p50/p95 `21ms` / `47ms`.
- The new retry did not fire because delegated routing returned providers.

Longer `daicowtf` follow-up:

```sh
timeout 1800s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 10 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-empty-delegated-retry-r10-trace.jsonl \
  --output /tmp/daicowtf-empty-delegated-retry-r10.json
```

Follow-up result:

- Rust passed `10/10`.
- Root TTFB p50/p90/p95/max was `1517ms` / `1579ms` / `1742ms` / `1742ms`.
- Run total p50/p95/max was `1535ms` / `1758ms` / `1758ms`.
- RSS p50/p95/max was `43688KiB` / `44152KiB` / `44152KiB`.
- FD p50/p95/max was `18` / `20` / `20`.
- Delegated provider lookups: events `29`, successes `29`, failures `0`,
  providers `39`, max elapsed `166ms`.
- DHT fallback was still attempted for low-diversity provider sets: events `8`,
  all timed out under the existing `750ms` low-diversity cap.
- Bitswap source peers were the root peer
  `12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP` and Pinata WSS peer
  `Qmdv6yNikmUWUWXufLJLRNkv6Y9sY5cmgeX5RVWA4WNMz4`.
- `delegated_provider_empty_retry` still did not fire. Delegated routing had
  providers during every measured run.
- The remaining `daicowtf` tail in this window was not empty delegated routing:
  the slowest phases were `mime_sniff_read` / `mime_detect` / `mime_total`
  around `1174ms` p50 and up to `1344ms`, caused by fetching enough of the root
  response to sniff extensionless HTML. The slow child CID
  `bafkreiezrxpztxumjtm7g6ea7a4bhna2dkuxun4evxawb5b7lo5k4t3u5u` still dominated
  block-fetch cost, but it was available from the WSS provider in all runs.

Regression validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p freedom-ipfs-routing --all-targets -- -D warnings
cargo clippy -p freedom-ipfs-mobile --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- routing suite passed: `21 passed; 0 failed; 1 ignored`
- mobile suite passed: `26 passed; 0 failed`
- mobile web harness suite passed: `32 passed; 0 failed`
- workspace check passed
- routing, mobile, and harness clippy passed with `-D warnings`

Decision:
Keep, but do not overstate the result. The live trace that motivated this
showed transient empty delegated results for a CID whose provider record became
available seconds later; the deterministic test proves the new path handles that
exact shape. The successful live follow-ups did not exercise the retry, so this
is a bounded robustness change rather than a measured live performance win. The
cost is one extra delegated request and `100ms` only when delegated routing
returns empty, before a much more expensive DHT lookup would already happen.

## 2026-05-05 Keep: Shorten Low-Diversity DHT Fallback Cap To 250ms

Hypothesis:
The previous `daicowtf-page-assets` 10-run soak passed reliably, but its
remaining tail included low-diversity light-DHT fallback attempts that all found
zero extra providers and timed out under the existing `750ms` fallback cap.
For sparse public-provider cases where delegated routing already returned a
usable WSS provider, spending another `750ms` trying to diversify through DHT is
often wasted latency. A shorter `250ms` cap should preserve the bounded fallback
signal while reducing page-load tail when the DHT path is not producing
additional providers.

Change:

- Lower `LOW_DIVERSITY_DHT_FALLBACK_TIMEOUT` from `750ms` to `250ms`.
- Keep `LOW_DIVERSITY_DELEGATED_MERGE_TIMEOUT` at `750ms`; this only changes
  the inner DHT provider lookup cap used after a non-empty low-diversity
  delegated result.
- Keep verification-before-store/serve semantics unchanged.
- No public gateway fallback is added.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing auto_routing_bounds_low_diversity_dht_fallback_timeout
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
```

Result: all focused checks passed. The routing test covers that the
low-diversity DHT fallback is still bounded by the configured cap.

Same-window `daicowtf` comparison against the previous `750ms` cap:

Previous 750ms-cap artifact:

- trace: `/tmp/daicowtf-empty-delegated-retry-r10-trace.jsonl`
- output: `/tmp/daicowtf-empty-delegated-retry-r10.json`

Previous 750ms-cap result:

- Rust passed `10/10`.
- Root TTFB p50/p90/p95/max was `1517ms` / `1579ms` / `1742ms` / `1742ms`.
- Run total p50/p90/p95/max was `1535ms` / `1592ms` / `1758ms` / `1758ms`.
- RSS p50/p90/p95/max was `43688KiB` / `44148KiB` / `44152KiB` / `44152KiB`.
- FD p50/p90/p95/max was `18` / `20` / `20` / `20`.
- Low-diversity DHT fallback events: `8`, all failures/timeouts, max timeout
  `750ms`, max elapsed about `753ms`.

250ms-cap run:

```sh
timeout 1800s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 10 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-dht-fallback-250ms-r10-trace.jsonl \
  --output /tmp/daicowtf-dht-fallback-250ms-r10.json
```

250ms-cap result:

- Rust passed `10/10`.
- Root TTFB p50/p90/p95/max was `1124ms` / `1409ms` / `1427ms` / `1427ms`.
- Run total p50/p90/p95/max was `1138ms` / `1422ms` / `1441ms` / `1441ms`.
- RSS p50/p90/p95/max was `42788KiB` / `43120KiB` / `43240KiB` /
  `43240KiB`.
- FD p50/p90/p95/max was `19` / `20` / `20` / `20`.
- Low-diversity DHT fallback events: `10`, all failures/timeouts, max timeout
  `250ms`, max elapsed about `252ms`.
- Provider-diversity-low trace events: `29`, failures `10`, total providers
  `19`, max timeout `250ms`.

Interpretation:

- Reliability stayed at `10/10` for the target workload.
- Compared with the immediate 750ms-cap soak, root TTFB improved by about
  `393ms` at p50 and `315ms` at p95.
- Run total improved by about `397ms` at p50 and `317ms` at p95.
- RSS stayed in the same mobile-friendly band and was slightly lower in this
  window; FD usage stayed capped around `20`.
- The DHT fallback continued to find no extra providers, so this specific
  public-network window supports treating the previous `750ms` spend as wasted
  latency.

Guardrail: `vitalik-root-html-range`

First run:

- artifacts: `/tmp/vitalik-dht-fallback-250ms-r3.json` and
  `/tmp/vitalik-dht-fallback-250ms-r3-trace.jsonl`
- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 was `167ms` / `5993ms`; Kubo was `2689ms` /
  `2824ms`.
- Trace showed one high-diversity delegated provider lookup outlier at about
  `5581ms`; there was no low-diversity DHT fallback involvement. Treat this as
  delegated-router noise, not evidence against the 250ms cap.

Rerun:

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-dht-fallback-250ms-rerun-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-dht-fallback-250ms-rerun-r3.json
```

Rerun result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 was `148ms` / `152ms`; Kubo was `3432ms` /
  `4073ms`.
- Rust max RSS/FD was `31488KiB` / `15`; Kubo max RSS/FD was `176740KiB` /
  `117`.
- Delegated lookup max was `64ms`.
- HTTP provider fetches: `6/6` success, p50/p95 `21ms` / `32ms`.
- No low-diversity DHT fallback involvement.

Guardrail: `ipfs-tech-page-assets`

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-dht-fallback-250ms-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-dht-fallback-250ms-r3.json
```

Guardrail result:

- Rust and Kubo passed `3/3`.
- Rust root TTFB p50/p95 was `1559ms` / `1573ms`; Kubo was `2547ms` /
  `6429ms`.
- Rust asset TTFB p50/p95 was `308ms` / `1418ms`; Kubo was `123ms` / `352ms`.
- Rust max RSS/FD was `54740KiB` / `48`; Kubo max RSS/FD was `278224KiB` /
  `277`.
- The trace did not show low-diversity DHT fallback as the asset tail driver.
  The slowest events were Bitswap/provider tails, connection errors, HTTP
  provider fetches, and one public delegated-router lookup outlier around
  `6196ms` in a high-provider-count path.
- This remains a separate asset-tail problem: Rust passed reliably with much
  lower resources and better root p95 than Kubo, but Kubo still had better asset
  p95 in this window.

Decision:
Keep the `250ms` low-diversity DHT fallback cap. The strongest evidence is the
same-window `daicowtf` r10 comparison: reliability stayed perfect, the DHT
fallback continued to find no providers, and p50/p95 page latency improved by
hundreds of milliseconds with no resource regression. The `vitalik` and
`ipfs-tech` guardrails did not implicate the cap in their remaining tails. Keep
watching sparse-provider workloads for cases where a slightly longer DHT
fallback actually finds useful extra Bitswap peers, but the current measured
tradeoff favors the shorter cap for mobile reads.

## 2026-05-05 Keep: Stream Delegated NDJSON Provider Responses

Hypothesis:
`ipfs-tech-page-assets` still showed asset tails from high-provider delegated
routing responses. The routing client requested `application/x-ndjson`, but it
read the entire delegated response body before parsing any provider records.
For some popular CIDs, the first HTTP-capable providers arrive quickly while
the full provider list can take seconds to finish. Since retrieval already
races a bounded number of verified HTTP provider candidates, the routing layer
can return early once it has enough HTTP-capable provider records.

Live probe before the change:

```sh
timeout 30s curl -fsS -w 'lines=%{size_download}B total=%{time_total}s start=%{time_starttransfer}s\n' \
  -o /tmp/delegated-bafkreiam77queskklq2cjhaoywvlxvasghy4ydr77gmzagjioporv6xsy4.ndjson \
  -H 'Accept: application/x-ndjson' \
  'https://delegated-ipfs.dev/routing/v1/providers/bafkreiam77queskklq2cjhaoywvlxvasghy4ydr77gmzagjioporv6xsy4'

/usr/bin/time -f 'first6q real=%e' bash -lc "timeout 30s curl -fsS \
  -H 'Accept: application/x-ndjson' \
  'https://delegated-ipfs.dev/routing/v1/providers/bafkreiam77queskklq2cjhaoywvlxvasghy4ydr77gmzagjioporv6xsy4' |
  awk 'NR<=6{print} NR==6{exit}' >/tmp/delegated-first6q.txt"
```

Probe result:

- Full response for the slow `community-hero` raw CID: `46` lines,
  `77155B`, total `5.598990s`, first byte `0.234523s`.
- The first six lines, which included multiple HTTP provider records, were
  available in `0.14s`.
- This means the old full-body parser could sit on the mobile request path for
  seconds even though enough routing-provided HTTP candidates were already
  available.

Change:

- Add a streaming parser for delegated responses with content type
  `application/x-ndjson`.
- Preserve the existing full-body parser for JSON-object responses and any
  response that is not explicitly NDJSON.
- Continue enforcing `MAX_DELEGATED_ROUTING_RESPONSE_BYTES`.
- Return early from a streaming NDJSON response once either:
  - `MAX_DELEGATED_ROUTING_PROVIDERS` records have been parsed, matching the
    existing cap; or
  - `4` HTTP-capable provider URLs have been parsed.
- Keep sparse/non-HTTP responses on the full-read path. This avoids throwing
  away late Bitswap candidates for cases such as `daicowtf`.
- No public gateway fallback is added. All HTTP candidates still come from
  delegated routing, and fetched blocks are still verified before store/serve.

Deterministic validation:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-routing delegated_routing_returns_after_enough_streamed_http_providers
cargo test -p freedom-ipfs-routing
```

Result:

- The focused test passed. It serves four HTTP provider records immediately,
  delays a fifth record by `2s`, and proves the client returns within `500ms`
  without consuming the late record.
- Full routing suite passed: `22 passed; 0 failed; 1 ignored`.

Primary live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-streaming-delegated-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-streaming-delegated-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `1435ms` / `2028ms`; Kubo was `2888ms` /
  `4313ms`.
- Rust asset TTFB p50/p95/max was `292ms` / `1095ms` / `1972ms`; Kubo was
  `122ms` / `295ms` / `306ms`.
- Rust max RSS/FD was `52536KiB` / `38`; Kubo max RSS/FD was `244812KiB` /
  `176`.
- Delegated provider lookups: events `100`, successes `100`, failures `0`,
  providers `1393`, max elapsed `1744ms`.
- HTTP provider fetches: events `76`, successes `76`, failures `0`, p50/p95/max
  `160ms` / `690ms` / `1175ms`.

Comparison with the immediately previous 250ms-DHT-cap `ipfs.tech` run:

- Previous artifact: `/tmp/ipfs-tech-dht-fallback-250ms-r3.json`,
  `/tmp/ipfs-tech-dht-fallback-250ms-r3-trace.jsonl`.
- Previous Rust passed `3/3`.
- Previous Rust asset TTFB p50/p95/max was `308ms` / `1418ms` / `6976ms`.
- Previous Rust delegated lookup max was `6196ms`.
- Previous Rust max RSS/FD was `54740KiB` / `48`.
- Streaming NDJSON improved asset p50/p95/max to `292ms` / `1095ms` /
  `1972ms`, delegated lookup max to `1744ms`, and FD max to `38`.
- Root p95 moved from `1573ms` to `2028ms` in this noisy same-day window, so do
  not claim an across-the-board page win. The strongest signal is reducing
  high-provider delegated lookup and asset max tails.

Guardrail: `vitalik-root-html-range`

```sh
timeout 480s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-streaming-delegated-r3-trace.jsonl \
  --comparison-output /tmp/vitalik-streaming-delegated-r3.json
```

Guardrail result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `160ms` / `659ms`; Kubo was `1751ms` / `2682ms`.
- Rust max RSS/FD was `31360KiB` / `14`; Kubo max RSS/FD was `117564KiB` /
  `62`.
- Delegated provider lookup max was `319ms`.
- HTTP provider fetches: `6/6` success, p50/p95/max `20ms` / `43ms` / `43ms`.

Guardrail: `daicowtf-page-assets`

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-streaming-delegated-r3-trace.jsonl \
  --comparison-output /tmp/daicowtf-streaming-delegated-r3.json
```

Guardrail result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `1342ms` / `1533ms`; Kubo was `2952ms` /
  `3003ms`.
- Rust max RSS/FD was `43460KiB` / `21`; Kubo max RSS/FD was `104008KiB` /
  `63`.
- Delegated provider lookup max was `52ms`.
- DHT fallback still found `0` providers and timed out under the `250ms` cap in
  three low-diversity attempts. This is unchanged sparse-provider behavior.

Regression validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing
cargo test -p freedom-ipfs-mobile -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- routing suite passed: `22 passed; 0 failed; 1 ignored`
- mobile suite passed: `26 passed; 0 failed`
- mobile web harness suite passed: `32 passed; 0 failed`
- workspace check passed
- workspace clippy passed with `-D warnings`

Decision:
Keep. The change directly targets a measured delegated-router response-tail
shape, preserves the read-only and verification model, and keeps sparse
responses conservative. The `ipfs.tech` asset tail improved materially,
especially max latency, while `vitalik` and `daicowtf` stayed reliable and
resource-light. Rust asset p95 on `ipfs.tech` is still slower than Kubo in this
window, so the next asset-tail work should look at HTTP-provider fetch latency,
session shortcut/post-lookup behavior, and the remaining high-provider CIDs
that have few HTTP candidates.

## 2026-05-05 Reject: Race Single HTTP Provider With Recent Session Peer

Hypothesis:
After streaming delegated NDJSON, the remaining `ipfs.tech` asset tail was often
a single routing-provided HTTP provider, usually `ipfs-bridge.sia.dev`, taking
hundreds of milliseconds while recent Bitswap session peers sometimes served the
same CIDs in about `130-190ms`. A very narrow race between exactly one HTTP
provider and only recent session peers might reduce those tails without broad
provider fanout.

Prototype:

- When provider fetching saw exactly one HTTP provider and at least one recent
  successful Bitswap session peer, race:
  - the existing verified single HTTP-provider fetch; and
  - `fetch_from_recent_bitswap_peers` only, not the full provider Bitswap set.
- Keep the existing global HTTP provider limiter.
- Do not add public gateway fallback.
- Continue verifying blocks before store/serve through both paths.
- Emit the existing `http_provider_race` trace phase with `session_race=true`.

Focused validation while the temporary patch was applied:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-retrieval single_http_provider_is_raced_with_recent_bitswap_peer
cargo test -p freedom-ipfs-retrieval races_http_provider_candidates_and_returns_first_verified_block
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
```

Result:

- New focused test passed: a recent local Bitswap session peer beat a delayed
  single HTTP provider and returned a verified Bitswap block.
- Existing HTTP-provider race test passed.
- Existing recent Bitswap peer tests passed: `5 passed`.

Live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-session-race-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-session-race-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `1325ms` / `1384ms`; Kubo was `2647ms` /
  `2668ms`.
- Rust asset TTFB p50/p95/max was `242ms` / `1589ms` / `7955ms`; Kubo was
  `152ms` / `1711ms` / roughly the same tail window.
- Rust max RSS/FD was `51696KiB` / `30`; Kubo max RSS/FD was `205624KiB` /
  `195`.
- HTTP provider fetches: events `90`, successes `90`, failures `0`, p50/p95/max
  `161ms` / `673ms` / `938ms`.
- Bitswap peer attempts fell to `23`, and FD max was low, but this was not
  enough to justify the behavior.
- Delegated provider lookup max was `7361ms`, and the slowest request group had
  assets at `7955ms`, `7628ms`, and `6037ms`.

Trace interpretation:

- The prototype fired only `4` session races in the whole run.
- One session race did return through Bitswap, but the request still took
  `2314ms` because delegated provider lookup had already consumed `2227ms`
  before the race could start.
- The largest tails were delegated-provider lookup waits that happened before
  the proposed single-HTTP/session race was reachable.
- The experiment therefore targeted the wrong remaining bottleneck for this
  trace shape. It did not provide clean evidence that the extra session race is
  worth keeping.

Decision:
Reject and revert the code. Keep this note as a negative result. A future
variant would need to address slow provider lookup while a recent session peer
appears during that lookup, or improve HTTP-provider quality directly, rather
than racing only after provider lookup has already completed.

## 2026-05-05 Reject: Poll For Late Session Peers During Slow Provider Lookup

Hypothesis:
The rejected single-HTTP/session race showed one clear problem: recent session
peers could only race after provider lookup had already finished, so a slow
delegated provider lookup still sat directly on request TTFB. A small late poll
during slow provider lookup might catch a recent Bitswap peer learned by another
concurrent page request and let that peer win without waiting for the delegated
router tail.

Prototype:

- In the cold `recent_peers.is_empty()` path, start provider lookup and wait
  `150ms`.
- If lookup is still pending, poll the recent successful Bitswap peer table
  again.
- If a peer appeared, race that recent-peer Bitswap shortcut against the still
  pending provider lookup.
- Keep the existing verified block path and no public gateway fallback.
- Emit `bitswap_session_shortcut_late_lookup` with `peer_count` and `outcome`.

Focused validation while the temporary patch was applied:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-retrieval late_recent_bitswap_peer_can_win_during_slow_provider_lookup
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
```

Result:

- The new deterministic test passed: a gated delegated lookup could be bypassed
  after another task recorded a local Bitswap session peer.
- Existing recent Bitswap peer tests passed.

Live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-late-session-lookup-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-late-session-lookup-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `1898ms` / `2393ms`; Kubo was `2724ms` /
  `3071ms`.
- Rust asset TTFB p50/p95/max was `252ms` / `1053ms` / `5558ms`; Kubo was
  `113ms` / `252ms` / `287ms`.
- Rust max RSS/FD was `52940KiB` / `32`; Kubo max RSS/FD was `243428KiB` /
  `214`.
- Gateway request elapsed p50/p90/p95/max was `252ms` / `949ms` / `1208ms` /
  `5556ms`.
- Delegated provider lookups: `96` events, `96` successes, max `5484ms`.
- HTTP provider fetches: `86` events, `86` successes, p50/p95/max `162ms` /
  `755ms` / `1347ms`.
- Bitswap peer attempts: `34` starts, with low FD pressure.

Trace interpretation:

- `bitswap_session_shortcut_late_lookup` fired `10` times.
- Every event had `peer_count=0` and `outcome=no_recent_peers`; the prototype
  never actually started a late session shortcut in the live run.
- The slowest request remained
  `/ipns/ipfs.tech/_nuxt/community-hero.Cp0BCcC7.jpg`: request elapsed
  `5556ms`, block total `5553ms`, delegated lookup `5484ms`.
- This means the live bottleneck was still delegated provider lookup, but no
  useful session peer was present at the `150ms` late poll point.

Decision:
Reject and revert the code. The deterministic mechanism worked, but live
evidence did not show real hits in the target `ipfs.tech` workload. Repeated
polling might eventually catch a peer, but it would add timer wakeups and
complexity without evidence that the session peer exists in time. The next
work should focus on reducing delegated-provider lookup tails directly or on
improving HTTP-provider candidate quality once provider records arrive.

## 2026-05-05 Reject: Rank HTTP Providers By Recent Success Latency

Hypothesis:
In the late-session lookup run, successful HTTP-provider fetches showed a strong
provider split: `https://dag.w3s.link/` was much faster than
`https://ipfs-bridge.sia.dev/` (`53ms` p50 and `109ms` p95 for `dag.w3s.link`
versus `203ms` p50 and `879ms` p95 for `ipfs-bridge.sia.dev`). A small
in-memory score table for recently successful HTTP providers might promote
known-fast providers into the first two-candidate race window when delegated
routing listed them later.

Prototype:

- Track successful HTTP provider base URLs in memory for `10m`.
- Store last successful latency and observation time.
- Before starting the bounded HTTP-provider race, sort candidates so recently
  successful providers come first, ordered by lower last latency.
- Drop the success entry on provider failure.
- Emit `http_provider_rank` with candidate count, known success count, fastest
  provider, and fastest latency.

Focused validation while the temporary patch was applied:

```sh
cargo fmt --all
cargo test -p freedom-ipfs-retrieval ranks_successful_http_provider_candidates_by_latency
cargo test -p freedom-ipfs-retrieval races_http_provider_candidates_and_returns_first_verified_block
cargo test -p freedom-ipfs-retrieval
```

Result:

- The new deterministic test passed: a known-fast third HTTP provider was
  promoted ahead of two unknown hanging providers and returned the verified
  block within `500ms`.
- Existing HTTP-provider race test passed.
- Full retrieval suite passed: `68 passed; 0 failed; 1 ignored`.

Live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-rank-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-provider-rank-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95 was `1444ms` / `1631ms`; Kubo was `2689ms` /
  `2721ms`.
- Rust asset TTFB p50/p95/max was `282ms` / `1239ms` / `4198ms`; Kubo was
  `104ms` / `225ms` / `255ms`.
- Rust max RSS/FD was `52088KiB` / `32`; Kubo max RSS/FD was `217952KiB` /
  `126`.
- Delegated provider lookup max was `3767ms`.
- HTTP provider fetches: `65` events, `65` successes, p50/p95/max `174ms` /
  `864ms` / `1187ms`.
- HTTP-provider winners were still skewed toward `ipfs-bridge.sia.dev`:
  `42` wins for `ipfs-bridge.sia.dev`, `23` wins for `dag.w3s.link`.
- Bitswap peer attempts increased to `94` starts.

Trace interpretation:

- `http_provider_rank` fired `20` times.
- Every rank event promoted `https://dag.w3s.link/` as the fastest known
  provider.
- Despite that, live asset p50/p95 worsened versus the preceding accepted
  streaming-delegated baseline window (`252ms` / `1053ms` -> `282ms` /
  `1239ms`), and HTTP-provider fetch p95 worsened (`755ms` -> `864ms`).
- The behavior may increase contention on the same globally fast provider, and
  it does not address the remaining delegated lookup tails that occur before
  HTTP-provider candidates are available.

Decision:
Reject and revert the code. The deterministic ranker worked, but the real
window did not produce a latency win and showed worse asset and HTTP-provider
tails. Keep the trace output as evidence that simple last-success ranking is
not enough; a future attempt would need richer provider quality signals,
concurrency-aware scoring, or per-CID/provider availability data.

## 2026-05-05 Keep: Trace Delegated Response Milestones

Problem:
The remaining high `ipfs.tech` asset tails are often delegated provider lookup
tails, but the existing `delegated_provider_lookup` event only reported total
elapsed time and provider count. That made it unclear whether a slow lookup was
waiting for response headers, first body bytes, the first HTTP-capable provider,
or the streaming early-return target.

Implementation:

- Keep the existing delegated routing behavior unchanged.
- Extend the internal delegated response parser to return compact response
  stats alongside providers:
  - `response_bytes`
  - `response_lines`
  - `http_provider_count`
  - `response_headers_elapsed_ms`
  - `response_first_chunk_seen`
  - `response_first_chunk_elapsed_ms`
  - `response_first_http_provider_seen`
  - `response_first_http_provider_elapsed_ms`
  - `response_target_met`
  - `response_target_met_elapsed_ms`
- Report those fields on the existing `delegated_provider_lookup` trace event,
  not as extra per-line events.
- Preserve `MAX_DELEGATED_ROUTING_RESPONSE_BYTES`, the NDJSON early return
  policy, and all provider verification semantics.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing
```

Result:

- Formatting check passed.
- Routing suite passed: `23 passed; 0 failed; 1 ignored`.
- New focused test `streamed_delegated_response_reports_response_stats` proves
  streamed NDJSON stats are populated when the early HTTP-provider target is
  reached before a delayed tail.

Live smoke:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-response-stats-r1-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-response-stats-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB was `1351ms`.
- Asset TTFB p50/p95/max was `238ms` / `881ms` / `1350ms`.
- Rust max RSS/FD was `51332KiB` / `26`.
- Delegated provider lookups: `25` events, `25` successes, max `215ms`.
- The trace showed the new fields on real `delegated_provider_lookup` events,
  for example root CID `bafybeier...` had `provider_count=17`,
  `http_provider_count=1`, `response_bytes=24910`, `response_lines=17`,
  and `response_headers_elapsed_ms=211`.

Decision:
Keep. This is diagnostic-only and does not add network work, provider fanout,
fallback gateways, or trust changes. The next time delegated lookup tails spike,
these fields should show whether to optimize endpoint/header latency, stream
body latency, or the HTTP-provider early-return threshold.

## 2026-05-05 Keep: Summarize Delegated Response Milestones In Harness

Problem:
The delegated response milestone fields were emitted in raw trace events, but
the mobile web harness summary still only surfaced delegated lookup event count,
provider count, and max elapsed time. That made the new diagnostics easy to
miss in long Rust-vs-Kubo runs and JSON comparison reports.

Implementation:

- Extend `TraceDelegatedProviderLookupAggregate` and per-endpoint aggregates
  with:
  - total `http_providers`
  - total `response_bytes`
  - total `response_lines`
  - counts for first chunk, first HTTP provider, and target-met events
  - max response header, first chunk, first HTTP provider, and target-met
    elapsed times
- Print a compact `response milestones` line under delegated provider lookup
  summaries.
- Include the same fields in JSON reports under
  `trace_summary.delegated_provider_lookup` and
  `trace_summary.delegated_provider_lookup_by_endpoint`.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p mobile-web-harness
```

Result:

- Formatting check passed after rustfmt.
- Focused trace summary test passed.
- Full harness suite passed: `32 passed; 0 failed`.

Live smoke:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-summary-milestones-r1-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-summary-milestones-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB was `1249ms`.
- Asset TTFB p50/p95/max was `186ms` / `667ms` / `895ms`.
- Rust max RSS/FD was `53400KiB` / `29`.
- Delegated provider lookups: `22` events, `22` successes, `323` providers,
  `46` HTTP providers, `466669` response bytes, `323` response lines, max
  elapsed `82ms`.
- Response milestones summary: header max `75ms`, first chunk seen `22`,
  first chunk max `75ms`, first HTTP provider seen `19`, first HTTP max
  `75ms`, target-met `9`, target-met max `44ms`.
- The JSON report contains the same values under
  `/tmp/ipfs-tech-delegated-summary-milestones-r1.json`.

Decision:
Keep. This is harness/diagnostic-only and makes the previous routing trace
fields usable during long comparison runs without changing node behavior.

## 2026-05-05 Keep: Trace HTTP Provider Response Milestones

Problem:
HTTP-provider fetch tails still appear in `ipfs.tech` page runs, but the
existing `http_provider_fetch` event only reported total elapsed time, provider,
and bytes. That made it unclear whether a slow fetch was waiting for response
headers, first body bytes, body transfer, verification, or storage.

Implementation:

- Keep HTTP provider selection, fanout, verification, and caching behavior
  unchanged.
- Extend the HTTP provider body reader to return compact body stats.
- Add these fields to successful `http_provider_fetch` trace events:
  - `response_bytes`
  - `response_headers_elapsed_ms`
  - `response_first_chunk_seen`
  - `response_first_chunk_elapsed_ms`
  - `response_body_elapsed_ms`
- Extend the mobile web harness HTTP-provider summary with totals and max
  header, first-chunk, and body milestone timings.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval limited_http_response_bytes_reports_body_stats
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p freedom-ipfs-retrieval
cargo test -p mobile-web-harness
```

Result:

- Formatting check passed.
- Focused retrieval body-stats test passed.
- Focused harness summary test passed.
- Retrieval suite passed: `68 passed; 0 failed; 1 ignored`.
- Harness suite passed: `32 passed; 0 failed`.

Live smoke:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-milestones-r1-trace.jsonl \
  --output /tmp/ipfs-tech-http-provider-milestones-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB was `930ms`.
- Asset TTFB p50/p95/max was `147ms` / `857ms` / `912ms`.
- Rust max RSS/FD was `51980KiB` / `35`.
- HTTP provider fetches: `11` events, `11` successes, p50/p95/max
  `158ms` / `686ms` / `686ms`.
- HTTP provider milestone summary: `response_bytes=309555`,
  `first_chunk_events=11`, header max `642ms`, first-chunk max `650ms`, body
  max `683ms`.
- The two slowest HTTP-provider fetches were both from
  `https://ipfs-bridge.sia.dev/`:
  - `EgmQ2fGv.js`: total `686ms`, headers `642ms`, first chunk `650ms`, body
    complete `683ms`
  - `Duo5E1ke.js`: total `644ms`, headers `634ms`, first chunk `642ms`, body
    complete `642ms`

Decision:
Keep. This is diagnostic-only and confirms that in this window the largest
HTTP-provider tails were mostly header wait on `ipfs-bridge.sia.dev`, not body
transfer or block-store time. Future HTTP-provider quality work should account
for provider/header latency, not only bytes or block size.

## 2026-05-05 Keep: Summarize HTTP Provider Milestones By Provider

Problem:
The HTTP-provider response milestone summary exposed aggregate header/body
latency, but did not break those timings down by provider. That is not enough
for provider-quality experiments because the next behavior change needs to know
which provider is contributing header wait and whether later runs shift work
away from it.

Implementation:

- Extend the harness HTTP-provider summary with `provider_milestones`.
- For each provider, aggregate:
  - events, successes, failures
  - bytes and response bytes
  - first-chunk event count
  - total and max elapsed time
  - max response header, first-chunk, and body elapsed times
- Sort provider milestones by worst header latency, then max elapsed time.
- Print the top provider milestone rows under `http provider fetches`.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
```

Result:

- Formatting check passed.
- Focused HTTP-provider summary test passed.
- Full harness suite passed: `32 passed; 0 failed`.
- Workspace check passed.

Live smoke:

```sh
timeout 300s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-by-provider-r1-trace.jsonl \
  --output /tmp/ipfs-tech-http-provider-by-provider-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB was `1268ms`.
- Asset TTFB p50/p95/max was `134ms` / `291ms` / `344ms`.
- Rust max RSS/FD was `50844KiB` / `26`.
- HTTP provider fetches: `1` event, `1` success, `1362` bytes, total `671ms`.
- Provider milestone row:
  - `https://ipfs-bridge.sia.dev/`: events `1`, successes `1`, bytes `1362`,
    elapsed max `671ms`, header max `669ms`, first chunk max `670ms`, body max
    `670ms`.

Decision:
Keep. This is harness-only and makes the provider/header-latency signal visible
without changing gateway or retrieval behavior.

## 2026-05-05 Keep: Hedge Stalled HTTP Provider Races

Problem:
HTTP-provider fetches race two candidates, but if both initial candidates stall
the request waits until one completes or times out before trying later HTTP
providers. Recent provider milestone traces showed useful later providers, such
as `https://calib2.ezpdpz.net/`, sometimes sitting behind slower
`https://ipfs-bridge.sia.dev/` and `https://dag.w3s.link/` candidates.

Implementation:

- Add a single bounded HTTP-provider hedge after `250ms`.
- The initial race width remains `2`.
- If neither initial HTTP candidate has completed after the hedge delay, start
  exactly one additional provider candidate.
- Keep the existing global HTTP provider semaphore
  `MAX_CONCURRENT_HTTP_PROVIDER_FETCHES=4`.
- Emit `phase="http_provider_hedge"` with the hedged provider, delay,
  pending count, and remaining provider count.
- Map `http_provider_hedge` to mobile/harness source
  `http_provider` and phase `fetching_http_provider`.
- Add a deterministic retrieval test with two hanging local HTTP providers and a
  third fast provider. Without the hedge this test would hit the outer timeout.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval hedges_slow_http_provider_race_with_extra_candidate
cargo test -p freedom-ipfs-retrieval races_http_provider_candidates_and_returns_first_verified_block
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
```

Result:

- Formatting check passed.
- New hedge test passed.
- Existing HTTP-provider race test passed.
- Mobile progress mapping test passed.
- Harness progress summary test passed.
- Harness HTTP-provider summary test passed.

Full validation:

```sh
cargo fmt --all --check && \
cargo test -p freedom-ipfs-retrieval -p freedom-ipfs-mobile -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- Retrieval suite passed: `69 passed; 0 failed; 1 ignored`.
- Mobile suite passed: `26 passed; 0 failed`.
- Harness suite passed: `32 passed; 0 failed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-hedge-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-provider-hedge-r3.json
```

First live result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `1364ms` / `1631ms`, Kubo `3103ms` / `3360ms`.
- Asset TTFB p50/p95: Rust `311ms` / `2767ms`, Kubo `163ms` / `1260ms`.
- Rust max RSS/FD: `50400KiB` / `32`; Kubo max RSS/FD:
  `276928KiB` / `222`.
- HTTP provider fetch p50/p95/max: `167ms` / `653ms` / `1225ms`.
- HTTP provider header max: `837ms`.
- Delegated provider lookup max: `6264ms`.
- `http_provider_hedge` fired `2` times. One hedged request was won by
  `https://calib2.ezpdpz.net/` `54ms` after the hedge launched.
- Decision after this sample was mixed: HTTP-provider tail improved, but asset
  p95 was dominated by delegated routing outliers, so a second sample was run
  before keeping the change.

Second live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-hedge-r3b-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-provider-hedge-r3b.json
```

Second live result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `816ms` / `2044ms`, Kubo `1476ms` / `2485ms`.
- Asset TTFB p50/p95: Rust `282ms` / `584ms`, Kubo `154ms` / `577ms`.
- Rust max RSS/FD: `46336KiB` / `26`; Kubo max RSS/FD:
  `191012KiB` / `162`.
- HTTP provider fetch p50/p95/max: `163ms` / `506ms` / `1037ms`.
- HTTP provider header max: `734ms`.
- Delegated provider lookup max: `182ms`.
- `http_provider_hedge` fired `2` times.

Decision:
Keep. The hedge is bounded, preserves verification and read-only behavior, and
did not increase observed RSS/FD. Across the two live samples it fired rarely
but usefully, with one request demonstrably won by the hedged third provider.
Compared with recent no-hedge windows, HTTP-provider p95/max improved
(`755ms` / `1347ms` previously, then `653ms` / `1225ms`, then `506ms` /
`1037ms`), while the second live sample also kept asset p95 near Kubo. The
first sample's asset p95 regression was caused by delegated routing outliers,
not HTTP provider response time, and points to delegated routing tail work as a
better next experiment.

## 2026-05-05 Reject: Return Streamed Delegated Results On Bitswap Diversity

Hypothesis:
Some slow delegated-routing outliers appear to wait for late HTTP provider
records even after the streamed NDJSON response has already yielded Bitswap
providers. Returning once a single delegated endpoint has enough Bitswap
provider diversity might avoid multi-second delegated lookup tails and let
retrieval begin sooner.

Experiment:

- Add a streamed delegated-response early return when non-HTTP provider records
  reached `MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY`.
- Keep the existing HTTP-provider early return target at
  `STREAMING_DELEGATED_HTTP_PROVIDER_TARGET`.
- Add a deterministic test with two fast Bitswap-only provider records followed
  by a delayed HTTP-provider tail.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed
```

Result:

- First implementation failed existing streamed HTTP-provider tests because
  HTTP-provider records also carry IDs/addrs and were counted as Bitswap
  diversity.
- Tightened implementation counted only provider records with no HTTP URLs.
- Focused streamed routing tests then passed: `3 passed; 0 failed`.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-streamed-bitswap-target-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-streamed-bitswap-target-r3.json
```

Live result:

- Rust passed `2/3`; Kubo passed `3/3`.
- Rust produced one `502` for the root `/ipns/ipfs.tech/` request.
- Root TTFB p50/p95 on successful Rust runs was `1050ms` / `1276ms`.
- Asset TTFB p50/p95 on successful Rust runs was `165ms` / `444ms`.
- Delegated provider lookup max dropped to `377ms`.
- HTTP provider fetches collapsed to `7` events total, while Bitswap work rose
  sharply: `76` Bitswap peer-attempt starts and `66` shortcut starts.
- The failed root request had `bitswap_provider_candidates_empty` and no
  successful HTTP-provider path.

Decision:
Reject and revert. The idea did reduce delegated lookup latency, but it starved
the HTTP-provider path and made reliability worse. A safer future version would
need to preserve early HTTP-provider availability, for example by returning a
partial provider set only when it contains both enough Bitswap diversity and at
least one usable HTTP provider, or by racing Bitswap startup with continued
delegated stream consumption instead of dropping the stream tail.

## 2026-05-05 Reject: Return Streamed Delegated Results On Mixed Bitswap/HTTP Availability

Hypothesis:
The Bitswap-only early return was too aggressive because it dropped late HTTP
providers. A narrower version might be safe if it only returned early after the
streamed delegated response contained both:

- at least `MIN_DELEGATED_BITSWAP_PROVIDER_DIVERSITY` non-HTTP provider records
- at least one HTTP provider URL

Experiment:

- Restore the existing HTTP-provider target of four providers.
- Add a second early return condition for mixed availability:
  Bitswap diversity plus at least one HTTP provider.
- Add a focused test with two fast Bitswap provider records, one fast HTTP
  provider record, and a delayed HTTP-provider tail.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed
```

Result:

- Focused streamed routing tests passed: `3 passed; 0 failed`.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-streamed-mixed-target-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-streamed-mixed-target-r3.json
```

Live result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `1505ms` / `2062ms`, Kubo `1809ms` / `2598ms`.
- Asset TTFB p50/p95: Rust `272ms` / `1092ms`, Kubo `207ms` / `562ms`.
- Rust max RSS/FD: `51584KiB` / `30`.
- Delegated provider lookup max was still `5218ms`.
- HTTP provider fetch p50/p95/max: `162ms` / `670ms` / `1187ms`.
- Bitswap work increased: `112` fetching-bitswap progress events,
  `29` Bitswap peer-attempt starts, and `14` shortcut starts.

Decision:
Reject and revert. This was reliable, but it did not fix the delegated-routing
tail and it increased Bitswap work while asset p95 regressed versus the kept
HTTP hedge sample (`584ms` -> `1092ms`). The trace shows the worst delegated
event still waited for first HTTP provider availability at `5218ms`, so this
condition does not address the problematic slow-provider-order case.

## 2026-05-05 Observe: Delegated Router Endpoint Sweep

Goal:
Start the provider-quality lab by comparing `delegated-ipfs.dev` and
`cid.contact` endpoint behavior in the same live window before considering any
default endpoint changes.

Default delegated endpoint:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-router-default-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-default-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1061ms` / `1146ms` / `1146ms`.
- Asset TTFB p50/p95/max: `257ms` / `939ms` / `1198ms`.
- Max RSS/FD: `51284KiB` / `36`.
- Delegated lookup events: `90` successes, `0` failures, `1285` providers,
  `200` HTTP providers, max `848ms`.
- HTTP-provider fetch p50/p95/max: `162ms` / `525ms` / `875ms`.

`cid.contact` alone:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --delegated-router https://cid.contact/routing/v1 \
  --trace-output /tmp/ipfs-tech-router-cid-contact-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-cid-contact-r3.json
```

Result:

- Rust failed `0/3`.
- All three root requests returned `502`.
- Delegated lookup events: `0` successes, `3` failures, `0` providers.
- Trace errors were HTTP `404 Not Found` for
  `https://cid.contact/routing/v1/providers/bafybeier...`.
- DHT fallback returned `0` providers for the root CID in these runs.

Manual endpoint check:

```sh
curl -sS -H 'Accept: application/x-ndjson, application/json' \
  -D /tmp/cid-contact-routing-v1.headers \
  https://cid.contact/routing/v1/providers/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq \
  -o /tmp/cid-contact-routing-v1.body
```

Result:

- GET returned `HTTP/2 404` with an empty body.
- `HEAD` on the same route returned `405` with `allow: GET`, so this endpoint
  shape is advertised but not useful for this CID/window.

Default plus `cid.contact`:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --trace-output /tmp/ipfs-tech-router-delegated-plus-cid-contact-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-delegated-plus-cid-contact-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `768ms` / `831ms` / `831ms`.
- Asset TTFB p50/p95/max: `232ms` / `550ms` / `789ms`.
- Max RSS/FD: `56084KiB` / `34`.
- Combined delegated lookup events: `98` successes, `3` failures, `1423`
  providers, `215` HTTP providers, max `102ms`.
- Per-endpoint summary:
  - `https://delegated-ipfs.dev/routing/v1`: `98` successes, `0` failures,
    `1423` providers, `215` HTTP providers, max `102ms`.
  - `https://cid.contact/routing/v1`: `0` successes, `3` failures, max `42ms`.
- HTTP-provider fetch p50/p95/max: `159ms` / `346ms` / `545ms`.

Decision:
Do not change defaults. The dual-endpoint run looked faster than the default
sample, but `cid.contact` contributed only three fast `404` errors and no
provider records. The apparent improvement is not causally attributable to
`cid.contact`; it is more likely normal live-network variance in
`delegated-ipfs.dev` and HTTP provider response timing. Treat
`https://cid.contact/routing/v1` as rejected for this delegated-provider API
shape until a working provider endpoint is confirmed. Future provider-quality
sweeps should test other IPNI/delegated endpoints or a corrected `cid.contact`
API before adding any default endpoint fanout.

## 2026-05-05 Observe: HTTP Hedge Guard Cases

Goal:
After keeping the bounded HTTP-provider hedge based on `ipfs.tech`, run the
other recurring mobile smoke cases to make sure the change does not disturb
Bitswap-only or small range workloads.

Daicowtf page guard:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-http-hedge-guard-r3-trace.jsonl \
  --output /tmp/daicowtf-http-hedge-guard-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1055ms` / `1098ms` / `1098ms`.
- Max RSS/FD: `43140KiB` / `19`.
- Block sources were Bitswap-only: `bitswap=9`.
- Delegated lookup events: `9` successes, `0` failures, `12` providers,
  `0` HTTP providers, max `126ms`.
- DHT fallback timed out in `3` low-diversity cases but did not block success.
- Bitswap source transports included `tcp=3` and `wss=3`.

Vitalik range guard:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-hedge-guard-r3-trace.jsonl \
  --output /tmp/vitalik-http-hedge-guard-r3.json
```

Result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `160ms` / `369ms` / `369ms`.
- Max RSS/FD: `31872KiB` / `15`.
- Block sources were HTTP-provider-only: `http_provider=6`.
- Delegated lookup events: `6` successes, `0` failures, `157` providers,
  `12` HTTP providers, max `279ms`.
- HTTP-provider fetch p50/p95/max: `28ms` / `56ms` / `56ms` from
  `https://trustless.filebase.io/`.

Decision:
Pass as guard coverage. The HTTP hedge did not introduce regressions in these
two cases: daicowtf remains a successful Bitswap/WSS workload, while the
Vitalik range case remains a fast verified HTTP-provider workload.

## 2026-05-05 Reject: Repeated Range-Batch Recent-Peer Multi-Want Hook

Hypothesis:
`FetchingBlockProvider::get_block_ranges_async` already receives bounded
adjacent raw child ranges from UnixFS. Since the shared Bitswap client now has
multi-CID `fetch_many` support, a simple next step seemed to be trying those
multi-range misses against recent successful Bitswap peers before falling back
to the existing individual child fetches.

Prototype:

- Added `HttpRetriever::fetch_many_from_recent_bitswap_peers`.
- Used it from `FetchingBlockProvider::get_block_ranges_async` when a range
  batch had more than one uncached CID.
- Stored verified requested blocks through the normal block store path before
  serving range bytes.
- Added a focused local-peer test,
  `range_batch_uses_recent_bitswap_multi_want_peer`, proving the two requested
  raw ranges were sent as one Bitswap multi-want stream.

Focused validation while the prototype was present:

```sh
cargo test -p freedom-ipfs-retrieval range_batch_uses_recent_bitswap_multi_want_peer
cargo test -p freedom-ipfs-retrieval
```

Focused result:

- `range_batch_uses_recent_bitswap_multi_want_peer` passed.
- Full retrieval tests passed: `70 passed; 1 ignored`.

Live check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-bitswap-range-batch-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-bitswap-range-batch-r3.json
```

Live result:

- Rust and Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `1349ms` / `1855ms`, Kubo `2522ms` / `2724ms`.
- Asset TTFB p50/p95: Rust `271ms` / `1312ms`, Kubo `89ms` / `210ms`.
- Rust max RSS/FD: `52284KiB` / `35`.
- Delegated provider lookup max was `6768ms`, still the dominant tail.
- Harness batch summary showed `multi_cid_commands=0`, `max_cids=1`; the new
  hook did not engage on this live page.

Decision:
Reject and revert. This repeated a production hook shape that was already
rejected earlier in this document after the seeded
`bitswap-seeded-multiblock-boundary-range` evidence showed duplicate work and
worse p95. Today's `ipfs.tech` run added no evidence in favor because the hook
did not fire at all, while the prior seeded evidence already showed an unstable
latency tradeoff when it does fire. Do not reintroduce this range-batch
multi-want hook without first solving duplicate cache writes and proving a
request shape where the multi-CID batch consistently wins before the existing
single-CID child fetch path.

## 2026-05-05 Keep: Stream Delegated Results After Three HTTP Providers

Hypothesis:
The streamed delegated routing path waited for four HTTP-provider records
before returning partial results. The HTTP-provider fetcher currently races two
providers and has a bounded hedge for one stalled candidate, so a target of
three should preserve useful diversity while avoiding a fourth-provider tail.

Change:

- Lower `STREAMING_DELEGATED_HTTP_PROVIDER_TARGET` from `4` to `3`.
- Keep all existing verification, HTTP-provider racing, Bitswap provider
  diversity, low-diversity DHT fallback, and stale-peer behavior unchanged.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed
```

Focused result:

- Formatting passed.
- Streamed routing tests passed: `2 passed`.

Live `ipfs.tech` comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-target3-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-target3-r3.json
```

Live comparison result:

- Rust and Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `1030ms` / `1841ms`, Kubo `2337ms` / `2346ms`.
- Asset TTFB p50/p95: Rust `273ms` / `943ms`, Kubo `111ms` / `1786ms`.
- Rust max RSS/FD: `49268KiB` / `31`; Kubo max RSS/FD:
  `263476KiB` / `172`.
- Delegated lookup events: `103` successes, `0` failures, `1761` providers,
  `185` HTTP providers, max `966ms`.
- Delegated response milestones: header/first-HTTP-provider max `966ms`;
  target-met events `42`, target-met max `966ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `642ms` / `730ms`.
- HTTP providers used: `ipfs-bridge.sia.dev=54`, `dag.w3s.link=42`.
- Bitswap work stayed low: peer attempt starts `19`, commands `15`, no
  multi-CID batch activity.

This directly addressed the previous live tail seen in
`/tmp/ipfs-tech-bitswap-range-batch-r3.json`, where delegated lookup max was
`6768ms`, asset p95 was `1312ms`, root p50 was `1349ms`, and RSS/FD were
`52284KiB` / `35`.

Second Rust-only `ipfs.tech` check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-target3-r3b-trace.jsonl \
  --output /tmp/ipfs-tech-http-target3-r3b.json
```

Second result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `891ms` / `1423ms` / `1423ms`.
- Asset TTFB p50/p95/max: `275ms` / `720ms` / `1252ms`.
- Run total p50/p95/max: `3025ms` / `3255ms` / `3255ms`.
- Max RSS/FD: `45524KiB` / `28`.
- Block sources were HTTP-provider-only: `http_provider=120`.
- Delegated lookup events: `105` successes, `0` failures, `1842` providers,
  `189` HTTP providers, max `85ms`.
- Delegated milestones: header max `84ms`, first-HTTP-provider max `84ms`,
  target-met max `84ms`.
- HTTP-provider fetch p50/p95/max: `162ms` / `602ms` / `790ms`.
- HTTP providers used: `ipfs-bridge.sia.dev=63`, `dag.w3s.link=42`.

Guard, Vitalik range:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-target3-guard-r3-trace.jsonl \
  --output /tmp/vitalik-http-target3-guard-r3.json
```

Vitalik result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `137ms` / `346ms` / `346ms`.
- Max RSS/FD: `31488KiB` / `14`.
- Block sources were HTTP-provider-only: `http_provider=6`.
- Delegated lookup events: `6` successes, `0` failures, `121` providers,
  `12` HTTP providers, max `259ms`.
- Target-met events `3`, target-met max `15ms`.
- HTTP-provider fetch p50/p95/max: `20ms` / `43ms` / `43ms` from
  `https://trustless.filebase.io/`.

Guard, daicowtf:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-http-target3-guard-r3-trace.jsonl \
  --output /tmp/daicowtf-http-target3-guard-r3.json
```

Daicowtf result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1112ms` / `1365ms` / `1365ms`.
- Max RSS/FD: `43432KiB` / `20`.
- Block sources were Bitswap-only: `bitswap=9`.
- Delegated lookup events: `9` successes, `0` failures, `12` providers,
  `0` HTTP providers, max `44ms`.
- Target-met events `0`, as expected for this sparse non-HTTP-provider case.
- Low-diversity DHT fallback timed out in `3` cases without affecting success.
- Bitswap source transports included `wss=3`, `tcp=2`, and `quic=1`.

Decision:
Keep. This is a small mobile-friendly latency tune that specifically reduces
the streamed delegated routing tail for HTTP-provider-heavy page loads while
preserving the HTTP-provider race plus hedge width. The two `ipfs.tech` samples
showed lower delegated milestone tails and lower resource use than the
immediate previous target-four run, and the Vitalik and daicowtf guards did not
show regressions in fast HTTP range or sparse Bitswap-only workloads.

## 2026-05-05 Reject: Shorten HTTP Provider Hedge Delay To 150ms

Hypothesis:
After lowering the streamed delegated HTTP-provider target to three, the
remaining `ipfs.tech` asset tail still included HTTP-provider header waits,
especially from `https://ipfs-bridge.sia.dev/`. The accepted HTTP-provider
hedge fires after `250ms`; shortening that delay to `150ms` might start one
extra routing-provided provider soon enough to avoid slow-header tails while
preserving the same initial race width and global provider-fetch cap.

Baseline with the current `250ms` hedge:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-hedge250-target3-r3c-trace.jsonl \
  --output /tmp/ipfs-tech-http-hedge250-target3-r3c.json
```

Baseline result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1419ms` / `1614ms` / `1614ms`.
- Asset TTFB p50/p95/max: `240ms` / `666ms` / `2558ms`.
- Run total p50/p95/max: `3315ms` / `5434ms` / `5434ms`.
- Max RSS/FD: `51200KiB` / `32`.
- Block sources: `http_provider=108`, `bitswap=11`, `cache=1`.
- HTTP-provider fetch p50/p95/max: `162ms` / `536ms` / `664ms`.
- HTTP-provider winners: `ipfs-bridge.sia.dev=55`,
  `dag.w3s.link=38`.
- Provider milestone split:
  - `ipfs-bridge.sia.dev`: header max `663ms`.
  - `dag.w3s.link`: header max `115ms`.
- Delegated lookup max was `2380ms`, with streamed target-met max `702ms`.
- `http_provider_hedge` fired `0` times.

Prototype:

- Lowered `HTTP_PROVIDER_HEDGE_AFTER` from `250ms` to `150ms`.
- No other HTTP-provider race, verification, provider-cache, Bitswap, or
  routing behavior changed.

Focused validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval hedges_slow_http_provider_race_with_extra_candidate
```

Focused result:

- Formatting passed.
- The deterministic hedge test passed.

Live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-hedge150-target3-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-hedge150-target3-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1281ms` / `1357ms` / `1357ms`.
- Asset TTFB p50/p95/max: `215ms` / `827ms` / `1362ms`.
- Run total p50/p95/max: `2821ms` / `3408ms` / `3408ms`.
- Max RSS/FD: `51964KiB` / `31`.
- Block sources: `http_provider=80`, `bitswap=40`.
- HTTP-provider fetch p50/p95/max: `163ms` / `662ms` / `981ms`.
- HTTP-provider winners: `ipfs-bridge.sia.dev=42`,
  `dag.w3s.link=21`, `calib2.ezpdpz.net=2`.
- Provider milestone split:
  - `ipfs-bridge.sia.dev`: header max `664ms`, body max `971ms`.
  - `dag.w3s.link`: header max `170ms`.
  - `calib2.ezpdpz.net`: header max `84ms`.
- Delegated lookup max was only `78ms`, so the lower run total was mostly live
  routing variance rather than evidence for the hedge delay.
- `http_provider_hedge` fired exactly `1` time, on the favicon CID, hedging
  to `https://a-fil-http.aur.lu/`.

Decision:
Reject and revert. The shorter hedge did not produce a causal enough win in
the target path: only one hedge fired, HTTP-provider p95 worsened
`536ms -> 662ms`, asset p95 worsened `666ms -> 827ms`, and RSS increased
slightly. The better run-total and root numbers came with much lower delegated
lookup latency in that live window, not with a meaningful number of earlier
HTTP hedges. Keep the existing `250ms` delay until a broader same-window sweep
or a deterministic provider-order corpus shows that a lower hedge delay wins
without extra mobile fanout.

## 2026-05-05 Observe: Warm Same-Daemon Rust-vs-Kubo Refresh After Target3

Goal:
Refresh the warm same-daemon `ipfs.tech` comparison after the accepted streamed
HTTP-provider target change and the rejected shorter HTTP hedge experiment. This
checks whether the next work should chase warm-cache behavior or stay focused
on cold provider/retrieval tails.

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-warm-same-daemon-target3-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-warm-same-daemon-target3-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `2ms` / `49ms`, Kubo `2ms` / `4ms`.
- Asset TTFB p50/p95: Rust `3ms` / `6ms`, Kubo `3ms` / `5ms`.
- Rust max RSS/FD: `50668KiB` / `32`.
- Kubo max RSS/FD: `197708KiB` / `110`.
- Rust/Kubo resource ratios: RSS `0.26x`, FD `0.29x`.
- Rust trace included the warmup plus measured requests:
  - Warmup/root group still had cold work: root max `733ms`, asset max
    `566ms`.
  - Measured warm groups were effectively cache-hot: p50 `1-2ms`, p95 `2-3ms`,
    max `3-4ms`.
- Warmup cold fetches still showed provider/retrieval work:
  - Delegated lookup events `35`, max `178ms`.
  - HTTP-provider fetch p50/p95/max `159ms` / `321ms` / `395ms`.
  - HTTP-provider winners: `ipfs-bridge.sia.dev=20`, `dag.w3s.link=14`.
  - Bitswap work was low: `3` commands, `7` peer-attempt starts, `1` successful
    source block.

Interpretation:
Warm same-daemon behavior is already close to Kubo while using much less memory
and fewer file descriptors. The small Rust root p95 gap is worth watching, but
the larger remaining opportunity is still cold and first-warmup behavior:
provider lookup tails, single-HTTP-provider delegated records, HTTP-provider
header waits, and session-scoped reuse before provider lookup completes.

## 2026-05-05 Reject: Session-Scoped Recent HTTP Provider Shortcut

Hypothesis:
Several cold `ipfs.tech` tails come from child CIDs whose delegated lookup
eventually returns only one HTTP provider, often after waiting on response
headers. Since trustless HTTP-provider blocks are verified by CID before
store/serve, a narrowly scoped analogue to recent Bitswap peer reuse might help:
after a provider lookup stalls, try one HTTP provider that recently returned a
verified block in the same gateway process, while keeping provider lookup as
the fallback.

Prototype:

- Track recently successful HTTP provider base URLs in memory.
- Keep the session TTL very short: `5s`.
- Try at most one recent HTTP provider.
- Start the shortcut only after provider lookup stalls.
- Preserve verification-before-store/serve through the normal
  `fetch_from_http_provider` path.
- Do not mark the recent provider bad if this speculative child-CID probe
  fails, because it was not a provider record for that specific CID.
- Keep public gateway fallback out of scope: only routing-discovered HTTP
  providers that already returned a verified block were eligible.

Focused validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval recent_http_provider_shortcut_can_win_slow_provider_lookup
cargo test -p freedom-ipfs-retrieval http_provider
```

Focused result:

- Formatting passed.
- The new deterministic test passed: after a first verified HTTP-provider
  block, a second CID returned from the recent provider before a gated delegated
  lookup was released.
- Existing HTTP-provider tests passed: `6 passed`.

Live experiment, `150ms` provider-lookup stall threshold:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-session-shortcut-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-session-shortcut-r3.json
```

`150ms` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1577ms` / `1778ms` / `1778ms`.
- Asset TTFB p50/p95/max: `274ms` / `981ms` / `1101ms`.
- Run total p50/p95/max: `3680ms` / `4521ms` / `4521ms`.
- Max RSS/FD: `54016KiB` / `42`.
- Block sources: `http_provider=87`, `bitswap=32`.
- HTTP-provider fetch p50/p95/max: `172ms` / `684ms` / `774ms`.
- Delegated lookup max dropped to `792ms`, but asset p95 worsened versus the
  preceding target3/250ms-hedge baseline.
- Recent HTTP shortcut starts: `9`.
- Recent HTTP shortcut fetches: `8` successes and `1` failure.
- The failure was a verified miss shape: `dag.w3s.link` returned HTTP `404` for
  a child CID, then normal provider lookup/fallback continued.

Comparison baseline from the same session, without the prototype:

- Artifact: `/tmp/ipfs-tech-http-hedge250-target3-r3c.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1419ms` / `1614ms` / `1614ms`.
- Asset TTFB p50/p95/max: `240ms` / `666ms` / `2558ms`.
- Run total p50/p95/max: `3315ms` / `5434ms` / `5434ms`.
- Max RSS/FD: `51200KiB` / `32`.
- HTTP-provider fetch p50/p95/max: `162ms` / `536ms` / `664ms`.
- `http_provider_hedge` fired `0` times.

Live experiment, more conservative `300ms` provider-lookup stall threshold:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-session-shortcut300-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-session-shortcut300-r3.json
```

`300ms` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `851ms` / `1645ms` / `1645ms`.
- Asset TTFB p50/p95/max: `297ms` / `818ms` / `1175ms`.
- Run total p50/p95/max: `3119ms` / `3302ms` / `3302ms`.
- Max RSS/FD: `54180KiB` / `42`.
- Block sources: `http_provider=89`, `bitswap=31`.
- HTTP-provider fetch p50/p95/max: `166ms` / `685ms` / `889ms`.
- Delegated lookup max was only `112ms` in this live window.
- Recent HTTP shortcut starts: `0`.
- Recent HTTP shortcut fetches: `0`.

Decision:
Reject and revert. The deterministic mechanism worked, but live evidence did
not justify the added machinery. At `150ms`, the shortcut fired and sometimes
won, but root p95, asset p95, HTTP-provider p95, RSS, FD count, and Bitswap work
all worsened. At `300ms`, it did not fire at all, so it did not address a real
tail in that window. The concept may be worth revisiting only with a stronger
scoping key, such as an explicit page/root session ID and provider success tied
to the same resolved UnixFS root; a process-global short TTL is still too blunt
for mobile resource goals.

## 2026-05-05 Keep: Summarize Delegated HTTP Provider Distribution

Question:
After rejecting the process-global recent HTTP provider shortcut, the remaining
tail evidence still pointed at delegated provider records that sometimes expose
only one HTTP provider, or no HTTP provider at all. The harness already printed
total delegated provider and HTTP provider counts, but it did not preserve the
distribution needed to tell whether a run was mostly healthy multi-provider
lookup or dominated by sparse HTTP-provider responses.

Implementation:

- Extend the mobile web harness trace summary with delegated lookup counters for
  zero, single, and multi HTTP-provider events.
- Track single-HTTP-provider target misses separately.
- Track max elapsed time and max first-HTTP-provider time for single-provider
  delegated lookup events.
- Print the distribution both globally and per delegated routing endpoint.
- Add a focused trace-summary regression test.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_delegated_http_provider_distribution
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- Formatting passed after rustfmt.
- Focused delegated distribution test passed.
- Existing mobile progress phase summary test passed.
- Full `mobile-web-harness` tests passed: `33 passed`.
- Workspace check passed.
- Package and workspace clippy passed with `-D warnings`.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-http-dist-r3-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-http-dist-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1454ms` / `1539ms` / `1539ms`.
- Asset TTFB p50/p95/max: `236ms` / `2504ms` / `3371ms`.
- Run total p50/p95/max: `3507ms` / `5522ms` / `5522ms`.
- Max RSS/FD: `52632KiB` / `38`.
- Delegated provider lookup: `91` events, `91` successes, `1558`
  providers, `155` HTTP providers.
- HTTP provider distribution: `zero=6`, `single=50`, `multi=35`,
  `single_target_miss=50`, `single_max=2198ms`,
  `single_first_http_max=426ms`.
- Endpoint `https://delegated-ipfs.dev/routing/v1`: `http_zero=6`,
  `http_single=50`, `http_multi=35`, `http_single_target_miss=50`,
  `target_met=35`.
- HTTP-provider fetch p50/p95/max: `163ms` / `645ms` / `869ms`.
- Block sources: `http_provider=87`, `bitswap=31`, `cache=2`.

Decision:
Keep. This is harness-only instrumentation, so it does not affect node runtime
behavior or mobile resource use. The live run confirms the diagnostic value:
`50/91` delegated lookups returned exactly one HTTP provider, and all single
HTTP-provider events missed the target response threshold. That gives future
experiments a compact signal for sparse-provider tails without spelunking raw
trace JSONL.

## 2026-05-05 Keep: Bound Streamed Delegated Lookup After First HTTP Provider

Question:
The new delegated HTTP-provider distribution showed that many `ipfs.tech`
lookups returned exactly one HTTP provider and never met the configured
three-HTTP-provider streaming target. In the diagnostic baseline, those
single-provider responses could keep the delegated lookup open until the
response stream finished: max delegated lookup was `2198ms`, while the slowest
single-provider first HTTP provider had appeared by `426ms`.

Hypothesis:
For mobile page loads, once a delegated NDJSON response has produced at least
one trustless HTTP provider, waiting indefinitely for a sparse stream to finish
is often worse than starting the verified block fetch. Keep the existing fast
path that returns immediately after three HTTP providers, but if only one or two
HTTP providers appear, return after a short grace window instead of waiting for
the stream tail. Verification-before-store/serve remains unchanged because the
retrieval layer still verifies every HTTP-provider block by CID.

Implementation:

- Add `STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE = 250ms`.
- In the NDJSON delegated response parser, start that grace deadline when the
  first HTTP provider is parsed.
- Continue to return immediately if the existing
  `STREAMING_DELEGATED_HTTP_PROVIDER_TARGET = 3` target is met.
- If the grace deadline expires first, return the parsed providers with
  `target_met=false`.
- Add a deterministic routing test where one HTTP provider arrives immediately
  and a later HTTP provider stalls for two seconds; the parser must return
  during the grace window and exclude the late tail.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed_delegated_response_returns_after_first_http_provider_grace
cargo test -p freedom-ipfs-routing streamed
cargo test -p freedom-ipfs-routing
cargo test -p freedom-ipfs-retrieval http_provider
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-mobile
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- Formatting passed.
- New deterministic first-HTTP-provider grace test passed.
- Existing streamed delegated routing tests passed: `3 passed`.
- Full `freedom-ipfs-routing` tests passed: `24 passed`, `1 ignored`.
- Focused HTTP-provider retrieval tests passed: `5 passed`.
- Full gateway tests passed.
- Full mobile crate tests passed: `26 passed`.
- Workspace check and clippy passed.

Primary live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace250-r3-trace.jsonl \
  --output /tmp/ipfs-tech-first-http-grace250-r3.json
```

Primary live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1427ms` / `1706ms` / `1706ms`.
- Asset TTFB p50/p95/max: `265ms` / `1199ms` / `1541ms`.
- Run total p50/p95/max: `3811ms` / `3863ms` / `3863ms`.
- Max RSS/FD: `52776KiB` / `36`.
- Delegated provider lookup: `96` events, `96` successes, max `602ms`.
- HTTP provider distribution: `zero=2`, `single=56`, `multi=38`,
  `single_target_miss=56`, `single_max=602ms`,
  `single_first_http_max=600ms`.
- HTTP-provider fetch p50/p95/max: `163ms` / `697ms` / `1356ms`.
- Block sources: `http_provider=100`, `bitswap=19`.

Same-session diagnostic baseline before the code change:

- Artifact: `/tmp/ipfs-tech-delegated-http-dist-r3.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1454ms` / `1539ms` / `1539ms`.
- Asset TTFB p50/p95/max: `236ms` / `2504ms` / `3371ms`.
- Run total p50/p95/max: `3507ms` / `5522ms` / `5522ms`.
- Max RSS/FD: `52632KiB` / `38`.
- Delegated provider lookup max: `2198ms`.
- HTTP provider distribution: `zero=6`, `single=50`, `multi=35`,
  `single_target_miss=50`, `single_max=2198ms`,
  `single_first_http_max=426ms`.
- HTTP-provider fetch p50/p95/max: `163ms` / `645ms` / `869ms`.
- Block sources: `http_provider=87`, `bitswap=31`, `cache=2`.

Additional live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-first-http-grace250-r3-trace.jsonl \
  --output /tmp/daicowtf-first-http-grace250-r3.json
```

`daicowtf-page-assets` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1789ms` / `2329ms` / `2329ms`.
- Run total p50/p95/max: `1803ms` / `2345ms` / `2345ms`.
- Max RSS/FD: `43124KiB` / `19`.
- Delegated provider lookup max: `53ms`.
- HTTP provider distribution: `zero=7`, `single=2`, `multi=0`.
- Block sources: `bitswap=7`, `http_provider=2`.

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-first-http-grace250-r3-trace.jsonl \
  --output /tmp/vitalik-first-http-grace250-r3.json
```

`vitalik-root-html-range` result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `142ms` / `154ms` / `154ms`.
- Run total p50/p95/max: `143ms` / `155ms` / `155ms`.
- Max RSS/FD: `31488KiB` / `14`.
- Delegated provider lookup max: `55ms`.
- HTTP provider distribution: `zero=0`, `single=3`, `multi=3`.
- Block sources: `http_provider=6`.

Warm Rust-vs-Kubo comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace250-warm-compare-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-first-http-grace250-warm-compare-r3.json
```

Warm comparison result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB p50/p95: Rust `2ms` / `4ms`, Kubo `1ms` / `15ms`.
- Asset TTFB p50/p95: Rust `3ms` / `6ms`, Kubo `1ms` / `2ms`.
- Resource max: Rust `46068KiB` / `26` FDs, Kubo `371620KiB` /
  `711` FDs.
- Rust delegated lookup max in the warm trace: `60ms`.

Decision:
Keep. The change is small, bounded, and directly addresses the single-provider
tail revealed by the harness diagnostic. The main tradeoff is starting a
verified HTTP-provider fetch with fewer candidates in sparse responses, but the
primary live run shifted work toward HTTP providers, reduced Bitswap work, cut
delegated lookup max from `2198ms` to `602ms`, cut `ipfs.tech` asset p95 from
`2504ms` to `1199ms`, and reduced run p95 from `5522ms` to `3863ms` without
meaningful RSS/FD growth. Root p95 moved from `1539ms` to `1706ms`, so keep an
eye on root HTML variance in future runs, but the aggregate page-load and asset
tail improvement is strong enough to retain the optimization.

## 2026-05-05 Reject: Retune First HTTP Provider Grace to 150ms or 350ms

Question:
After keeping the `250ms` first-HTTP-provider grace, test whether a shorter or
longer grace window improves the speed/resource tradeoff. A shorter `150ms`
window might reduce sparse-stream waits further; a longer `350ms` window might
collect more late HTTP providers and improve candidate quality.

Prototype:

- Temporarily changed `STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE` from
  `250ms` to `150ms`.
- Then temporarily changed it from `150ms` to `350ms`.
- Restored the committed `250ms` value after measurement.
- No behavior besides the constant changed.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed_delegated_response_returns_after_first_http_provider_grace
```

Result:

- Formatting passed.
- The deterministic streaming response test passed at `150ms`, `350ms`, and
  after restoring `250ms`.

`150ms` live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace150-r3-trace.jsonl \
  --output /tmp/ipfs-tech-first-http-grace150-r3.json
```

`150ms` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1106ms` / `1127ms` / `1127ms`.
- Asset TTFB p50/p95/max: `292ms` / `5140ms` / `5683ms`.
- Run total p50/p95/max: `3098ms` / `7988ms` / `7988ms`.
- Max RSS/FD: `50768KiB` / `35`.
- Delegated provider lookup max: `5511ms`.
- HTTP provider distribution: `zero=4`, `single=58`, `multi=40`,
  `single_target_miss=58`, `single_first_http_max=5509ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `629ms` / `688ms`.
- Block sources: `http_provider=105`, `bitswap=12`, `cache=3`.

`350ms` live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace350-r3-trace.jsonl \
  --output /tmp/ipfs-tech-first-http-grace350-r3.json
```

`350ms` `ipfs.tech` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1450ms` / `1601ms` / `1601ms`.
- Asset TTFB p50/p95/max: `213ms` / `603ms` / `1139ms`.
- Run total p50/p95/max: `2812ms` / `3434ms` / `3434ms`.
- Max RSS/FD: `52504KiB` / `35`.
- Delegated provider lookup max: `163ms`.
- HTTP provider distribution: `zero=6`, `single=52`, `multi=37`,
  `single_target_miss=52`, `single_first_http_max=156ms`.
- HTTP-provider fetch p50/p95/max: `160ms` / `648ms` / `889ms`.
- Block sources: `http_provider=88`, `bitswap=31`, `cache=1`.

Additional `350ms` live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-first-http-grace350-r3-trace.jsonl \
  --output /tmp/daicowtf-first-http-grace350-r3.json
```

`350ms` `daicowtf-page-assets` result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1337ms` / `1759ms` / `1759ms`.
- Run total p50/p95/max: `1350ms` / `1776ms` / `1776ms`.
- Max RSS/FD: `42300KiB` / `17`.
- Delegated provider lookup max: `47ms`.
- HTTP provider distribution: `zero=3`, `single=6`, `multi=0`.
- Block sources: `http_provider=6`, `bitswap=3`.

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-first-http-grace350-r3-trace.jsonl \
  --output /tmp/vitalik-first-http-grace350-r3.json
```

`350ms` `vitalik-root-html-range` result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `502ms` / `505ms` / `505ms`.
- Run total p50/p95/max: `503ms` / `506ms` / `506ms`.
- Max RSS/FD: `31104KiB` / `14`.
- Delegated provider lookup max: `408ms`.
- HTTP provider distribution: `zero=0`, `single=3`, `multi=3`.
- Block sources: `http_provider=6`.

Current-window `250ms` rerun:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace250-rerun-r3-trace.jsonl \
  --output /tmp/ipfs-tech-first-http-grace250-rerun-r3.json
```

`250ms` `ipfs.tech` rerun result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `595ms` / `1328ms` / `1328ms`.
- Asset TTFB p50/p95/max: `204ms` / `594ms` / `851ms`.
- Run total p50/p95/max: `2831ms` / `2920ms` / `2920ms`.
- Max RSS/FD: `51424KiB` / `35`.
- Delegated provider lookup max: `149ms`.
- HTTP provider distribution: `zero=5`, `single=50`, `multi=36`,
  `single_target_miss=50`, `single_first_http_max=147ms`.
- HTTP-provider fetch p50/p95/max: `168ms` / `387ms` / `771ms`.
- Block sources: `http_provider=78`, `bitswap=42`.

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-first-http-grace250-rerun-r3-trace.jsonl \
  --output /tmp/vitalik-first-http-grace250-rerun-r3.json
```

`250ms` `vitalik-root-html-range` rerun result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `141ms` / `149ms` / `149ms`.
- Run total p50/p95/max: `141ms` / `149ms` / `149ms`.
- Max RSS/FD: `31360KiB` / `13`.
- Delegated provider lookup max: `56ms`.
- HTTP provider distribution: `zero=0`, `single=3`, `multi=3`.
- Block sources: `http_provider=6`.

Decision:
Reject both retunes and keep `250ms`. The `150ms` run had unacceptable
`ipfs.tech` asset and page-load tails. The `350ms` run looked good on
`ipfs.tech` and `daicowtf`, but it regressed the small range-shaped
`vitalik-root-html-range` case from roughly `149ms` p95 at `250ms` to `505ms`
p95 by allowing a longer single-provider wait. A same-window `250ms` rerun also
matched or beat `350ms` on `ipfs.tech` asset p95, root p95, delegated lookup
max, and run p95, so there is no evidence-based reason to move away from the
current value.

## 2026-05-05 Reject: In-Memory HTTP Provider Success Ordering

Question:
Recent traces repeatedly showed provider-specific latency spread in verified
HTTP-provider fetches. For example, `dag.w3s.link` often returned faster than
`ipfs-bridge.sia.dev`, while both remained valid trustless providers. Test
whether a small in-memory success history can improve race ordering without
adding public gateway fallback or probing providers that routing did not return
for the requested CID.

Prototype:

- Add a process-local `successful_http_providers` map to `HttpRetriever`.
- Record a provider URL only after a successful, CID-verified HTTP-provider
  block fetch.
- Keep the history bounded to `16` providers and expire entries after `10min`.
- Before an HTTP-provider race, sort only the current CID's routing-returned
  provider URLs by known success latency and recency.
- Do not add any provider that was not returned for the current CID.
- Add `scored_provider_count` to the `http_provider_race` trace event.
- Add focused tests for latency ordering and bounded history.

Focused validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval successful_http_provider
cargo test -p freedom-ipfs-retrieval http_provider
```

Focused result:

- Formatting passed.
- New successful HTTP-provider ordering and cap tests passed.
- Existing HTTP-provider verification tests passed: `7 passed`.

Live experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-score-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-provider-score-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `935ms` / `1767ms` / `1767ms`.
- Asset TTFB p50/p95/max: `294ms` / `1262ms` / `5530ms`.
- Run total p50/p95/max: `2799ms` / `8534ms` / `8534ms`.
- Max RSS/FD: `51788KiB` / `31`.
- Delegated provider lookup max: `5430ms`.
- HTTP provider distribution: `zero=3`, `single=52`, `multi=38`,
  `single_target_miss=52`, `single_first_http_max=1401ms`.
- HTTP-provider fetch p50/p95/max: `178ms` / `687ms` / `1068ms`.
- Block sources: `http_provider=102`, `bitswap=18`.
- `http_provider_race` score usage in the raw trace:
  `scored_provider_count=0` for `6` races, `1` for `79` races, and `2` for
  `2` races.

Decision:
Reject and revert. The focused mechanism worked, but the live result did not
show a page-load win and added state without a clear payoff. Most scored races
had only one current HTTP provider, where ordering cannot change behavior; only
two races had two scored providers. The run also hit a large delegated lookup
tail and worsened asset/run p95 versus the current `250ms` baseline. The idea
may be worth revisiting only after the harness summarizes score counts and race
width impact directly, or if routing regularly returns three or more HTTP
providers where the fastest candidate is outside the first two race slots.

## 2026-05-05 Keep: Summarize HTTP Provider Race Shape

Question:
The rejected HTTP-provider scoring prototype exposed a harness gap: raw traces
could show whether scoring fired, but the standard summary did not explain how
many HTTP-provider races had one provider, multiple providers, providers beyond
the race width, hedges, or optional scoring fields. Without that summary, future
HTTP-provider ordering experiments require raw JSONL spelunking.

Implementation:

- Add a `http_provider_races` trace summary aggregate.
- Count `http_provider_race` events, total provider candidates, max provider
  count, max race width, single-provider races, multi-provider races, and races
  where `provider_count > race_width`.
- Count optional `scored_provider_count` fields when future experiments emit
  them.
- Count `http_provider_hedge` events and their max pending/remaining provider
  counts.
- Print a compact `http provider races:` summary before HTTP-provider fetch
  details.
- Extend the focused HTTP-provider trace summary test.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo clippy --workspace --all-targets -- -D warnings
```

Result:

- Formatting passed.
- Focused HTTP-provider summary test passed.
- Full `mobile-web-harness` tests passed: `33 passed`.
- Workspace check passed.
- Package and workspace clippy passed with `-D warnings`.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-race-summary-r1-trace.jsonl \
  --output /tmp/ipfs-tech-http-race-summary-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB/total: `1441ms` / `1443ms`.
- Asset TTFB p50/p95/max: `320ms` / `913ms` / `1382ms`.
- Run total: `4147ms`.
- Max RSS/FD: `50908KiB` / `30`.
- HTTP-provider races: `27` events, `49` total providers, `16` single-provider
  races, `11` multi-provider races, `11` races above the race width, race width
  max `2`, provider count max `3`, `0` scored events, `0` hedges.
- HTTP-provider fetch p50/p95/max: `161ms` / `683ms` / `1156ms`.
- Provider spread: `ipfs-bridge.sia.dev` max `1156ms`,
  `dag.w3s.link` max `76ms`.

Decision:
Keep. This is harness-only diagnostics and does not change node behavior or
mobile resource usage. The first live summary already gives a useful next-step
signal: on this `ipfs.tech` run, `11/27` HTTP-provider races had more
candidates than the current race width, while `16/27` had only one provider
where ordering/scoring cannot help.

## 2026-05-05 Reject: Increase HTTP Provider Race Width to 3

Question:
The race-shape summary showed that `11/27` HTTP-provider races in a live
`ipfs.tech` smoke had more candidates than the current race width of `2`.
Maybe starting three HTTP-provider attempts immediately would let the gateway
use a faster later candidate and reduce Bitswap fallback on page assets.

Prototype:

- Temporarily changed `HTTP_PROVIDER_RACE_WIDTH` from `2` to `3`.
- Kept the global `MAX_CONCURRENT_HTTP_PROVIDER_FETCHES` cap unchanged at `4`.
- Kept the existing `250ms` hedge delay unchanged.

Validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval http_provider
```

Result:

- Formatting passed.
- Focused retrieval HTTP-provider tests passed: `5 passed`.

Live run:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-race-width3-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-race-width3-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `986ms` / `1561ms` / `1561ms`.
- Asset TTFB p50/p95/max: `264ms` / `1011ms` / `5569ms`.
- Run total p50/p95/max: `2766ms` / `8023ms` / `8023ms`.
- Max RSS/FD: `50872KiB` / `31`.
- Delegated lookup max: `5467ms`.
- HTTP provider distribution: `zero=1`, `single=62`, `multi=42`,
  `single_target_miss=62`, `single_max=969ms`,
  `single_first_http_max=968ms`.
- HTTP-provider races: `104` events, `188` total providers,
  `62` single-provider races, `42` multi-provider races, `0` races above the
  race width, race width max `3`, provider count max `3`, `0` scored events,
  `0` hedges.
- HTTP-provider fetches: `104` events, `104` successes, p50/p95/max
  `161ms` / `625ms` / `889ms`.
- Provider spread: `ipfs-bridge.sia.dev=62`, `dag.w3s.link=40`,
  `a-fil-http.aur.lu=1`, `calib2.ezpdpz.net=1`.
- Block sources: `http_provider=119`, `bitswap=1`.

Same-window committed-width comparison:

- Artifact paths:
  `/tmp/ipfs-tech-first-http-grace250-rerun-r3-trace.jsonl` and
  `/tmp/ipfs-tech-first-http-grace250-rerun-r3.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `595ms` / `1328ms` / `1328ms`.
- Asset TTFB p50/p95/max: `204ms` / `594ms` / `851ms`.
- Run total p50/p95/max: `2831ms` / `2920ms` / `2920ms`.
- Max RSS/FD: `51424KiB` / `35`.
- Delegated lookup max: `149ms`.
- HTTP provider distribution: `zero=5`, `single=50`, `multi=36`,
  `single_target_miss=50`, `single_first_http_max=147ms`.
- HTTP-provider fetch p50/p95/max: `168ms` / `387ms` / `771ms`.
- Block sources: `http_provider=78`, `bitswap=42`.

Decision:
Reject and revert. Width `3` nearly eliminated Bitswap work on this run and
still stayed within the global HTTP-provider fetch cap, but it increased HTTP
provider fetch volume and worsened the visible page-load tail: asset p95
`1011ms` versus `594ms`, asset max `5569ms` versus `851ms`, and run p95
`8023ms` versus `2920ms` in the same-window committed-width comparison. The
large delegated lookup tail means this single run is noisy, but the evidence
does not support spending more mobile resources by default. Revisit only with a
selective policy, for example provider-specific race admission or a live signal
that the first two candidates are likely slow.

## 2026-05-05 Keep: Trace HTTP Provider Race Outcomes

Question:
After rejecting both in-memory HTTP-provider scoring and default race width `3`,
the next selective-racing question needs better evidence. The existing harness
could count race candidates, but it could not tell whether the winning HTTP
provider was already inside the first two candidates, whether a hedge fired, or
how many providers were actually attempted before the race completed.

Implementation:

- Emit `http_provider_race_result` after every HTTP-provider candidate race.
- On success, include the winning provider URL, zero-based
  `winner_provider_index`, one-based `winner_provider_rank`, whether the winner
  was inside the initial race width, provider count, race width, attempted
  provider count, failed provider count, hedge status, and elapsed time.
- On failure, include provider count, race width, attempted provider count,
  failed provider count, hedge status, and elapsed time.
- Extend the harness `http_provider_races` summary with result counts,
  winner-inside-initial-width counts, late-winner counts, winner rank buckets,
  max winner rank, max attempted providers, and max race-result elapsed time.
- Map the new raw phase to `fetching_http_provider` in both the harness
  progress summary and the mobile progress snapshot so this remains a
  diagnostic detail, not a new app-facing loading state.
- Document the race-outcome summary in the mobile web readiness README.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p freedom-ipfs-retrieval http_provider
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
```

Result:

- Formatting passed.
- Focused harness HTTP-provider summary test passed.
- Focused retrieval HTTP-provider tests passed: `5 passed`.
- Focused mobile progress phase mapping test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-race-result-r1-trace.jsonl \
  --output /tmp/ipfs-tech-http-race-result-r1.json
```

Live result:

- Rust passed `1/1`.
- Root TTFB/total: `1794ms` / `1795ms`.
- Asset TTFB p50/p95/max: `345ms` / `994ms` / `1008ms`.
- Run total: `4456ms`.
- Max RSS/FD: `43896KiB` / `27`.
- Delegated provider lookup max: `640ms`.
- HTTP provider distribution: `zero=0`, `single=21`, `multi=14`,
  `single_target_miss=21`, `single_first_http_max=639ms`.
- HTTP-provider races: `35` events, `63` total providers,
  `21` single-provider races, `14` multi-provider races, `14` races above the
  race width, race width max `2`, provider count max `3`, `2` hedges,
  `35` race results, `35` successes, `0` failures, `35` winners inside the
  initial race width, `0` late winners, `34` rank-1 winners, `1` rank-2
  winner, `0` rank-3-or-later winners, max winner rank `2`, max attempted
  provider count `3`, max race-result elapsed `775ms`.
- HTTP-provider fetch p50/p95/max: `166ms` / `671ms` / `775ms`.
- Provider spread: `ipfs-bridge.sia.dev=21`, `dag.w3s.link=14`.
- Block sources: `http_provider=40`.

Decision:
Keep. This is diagnostics-only and does not alter retrieval policy or mobile
resource use. The first live run shows why the rejected width-3 result was not
surprising: even though `14/35` races had more candidates than the current race
width, every successful race was won by a candidate already inside the initial
two slots. Future HTTP-provider race work should use this result summary as a
gate: a wider or selective third-provider policy is only worth prototyping when
live traces show nonzero late winners or repeated slow initial-width winners.

## 2026-05-05 Baseline: HTTP Provider Race Outcomes Over 3 Runs

Question:
After adding HTTP-provider race-result and winner-rank summaries, collect a
slightly stronger current baseline before changing provider race policy again.

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-race-result-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-race-result-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1200ms` / `1898ms` / `1898ms`.
- Asset TTFB p50/p95/max: `248ms` / `1338ms` / `1942ms`.
- Run total p50/p95/max: `3564ms` / `4115ms` / `4115ms`.
- Max RSS/FD: `51956KiB` / `36`.
- Delegated provider lookup max: `116ms`.
- HTTP provider distribution: `zero=5`, `single=46`, `multi=36`,
  `single_target_miss=46`, `single_first_http_max=110ms`.
- HTTP-provider races: `54` events, `108` total providers,
  `27` single-provider races, `27` multi-provider races, `27` races above the
  race width, race width max `2`, provider count max `3`, `1` hedge,
  `54` race results, `54` successes, `0` failures, `54` winners inside the
  initial race width, `0` late winners, `51` rank-1 winners, `3` rank-2
  winners, `0` rank-3-or-later winners, max winner rank `2`, max attempted
  provider count `3`, max race-result elapsed `1351ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `727ms` / `1342ms`.
- Provider spread: `dag.w3s.link=27`, `ipfs-bridge.sia.dev=27`.
- Block sources: `http_provider=69`, `bitswap=49`, `cache=2`.
- Bitswap session summary: `shortcut_starts=85`, `shortcut_hits=48`,
  `shortcut_misses=0`, `shortcut_post_lookup_waits=40`.

Interpretation:
Do not widen the default HTTP-provider race on this evidence. Half of the races
had more candidates than the race width, but `0/54` winners came from rank 3 or
later. The useful next speed work is more likely in the page/session path shown
by the slow requests and Bitswap/session summaries, not broader HTTP-provider
fanout.

## 2026-05-05 Keep: Retune Session Pre-Lookup Grace Back To 50ms

Question:
`BITSWAP_SESSION_PRE_LOOKUP_GRACE=75ms` was kept earlier because it reduced
redundant provider lookups when recent session peers were often useful. Since
then, delegated routing and verified HTTP-provider retrieval have improved.
Retest whether the shorter `50ms` head start now gives a better mobile
latency/resource tradeoff by falling back to the provider path sooner.

Implementation:

- Change `BITSWAP_SESSION_PRE_LOOKUP_GRACE` from `75ms` to `50ms`.
- Keep `BITSWAP_SESSION_POST_LOOKUP_GRACE=100ms`.
- Keep the existing `2s` session shortcut cap.
- No provider fanout increase, no public fallback, and no verification/caching
  trust change.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
```

Result:

- Formatting passed.
- Focused recent Bitswap peer tests passed: `4 passed`.

Same-window `75ms` baseline:

- Artifact paths:
  `/tmp/ipfs-tech-http-race-result-r3-trace.jsonl` and
  `/tmp/ipfs-tech-http-race-result-r3.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1200ms` / `1898ms` / `1898ms`.
- Asset TTFB p50/p95/max: `248ms` / `1338ms` / `1942ms`.
- Run total p50/p95/max: `3564ms` / `4115ms` / `4115ms`.
- Max RSS/FD: `51956KiB` / `36`.
- Delegated provider lookup max: `116ms`.
- HTTP-provider races: `54` results, `51` rank-1 winners, `3` rank-2 winners,
  `0` rank-3-or-later winners.
- Block sources: `http_provider=69`, `bitswap=49`, `cache=2`.
- Bitswap session: `shortcut_starts=85`, `shortcut_hits=48`,
  `shortcut_post_lookup_waits=40`.

`50ms` `ipfs.tech` experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-session-prelookup50-r3-trace.jsonl \
  --output /tmp/ipfs-tech-session-prelookup50-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1330ms` / `1334ms` / `1334ms`.
- Asset TTFB p50/p95/max: `237ms` / `834ms` / `1523ms`.
- Run total p50/p95/max: `3225ms` / `3646ms` / `3646ms`.
- Max RSS/FD: `51844KiB` / `35`.
- Delegated provider lookup max: `88ms`.
- HTTP-provider races: `62` results, `60` rank-1 winners, `2` rank-2 winners,
  `0` rank-3-or-later winners.
- HTTP-provider fetch p50/p95/max: `172ms` / `657ms` / `1349ms`.
- Block sources: `http_provider=77`, `bitswap=43`.
- Bitswap session: `shortcut_starts=63`, `shortcut_hits=40`,
  `shortcut_post_lookup_waits=24`.
- Bitswap peer attempts fell from `106` in the `75ms` baseline to `84`.

Additional checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-session-prelookup50-r3-trace.jsonl \
  --output /tmp/vitalik-session-prelookup50-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-session-prelookup50-r3-trace.jsonl \
  --output /tmp/daicowtf-session-prelookup50-r3.json
```

Results:

- `vitalik-root-html-range` passed `3/3`; root/range TTFB p50/p95/max
  `180ms` / `368ms` / `368ms`, max RSS/FD `31616KiB` / `14`, block sources
  `http_provider=6`. This case did not exercise recent-session shortcuts.
- `daicowtf-page-assets` passed `3/3`; root TTFB p50/p95/max
  `1292ms` / `1569ms` / `1569ms`, max RSS/FD `42520KiB` / `17`, block sources
  `http_provider=6`, `bitswap=3`. This case also did not exercise recent
  session shortcuts.

Final validation:

```sh
cargo test -p freedom-ipfs-retrieval
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Full retrieval suite passed: `69 passed`, `1 ignored`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep `50ms`. In the same network window, the shorter pre-lookup grace improved
`ipfs.tech` asset p95 from `1338ms` to `834ms`, asset max from `1942ms` to
`1523ms`, run p95 from `4115ms` to `3646ms`, and root p95 from `1898ms` to
`1334ms`, while slightly reducing FD/RSS and Bitswap peer-attempt pressure. The
other live checks stayed reliable and resource-light. This supersedes the
earlier `75ms` keep decision under the newer HTTP-provider/routing behavior.

## 2026-05-05 Keep: Summarize Slow Single HTTP Provider Winners

Question:
The current race-result summary proves that widening the default HTTP-provider
race is not justified when rank-3 winners are absent, but it still hides a
different possible policy: single-provider lookups can have no alternate HTTP
candidate to race, and some of those verified wins are now the HTTP-provider
tail. Add a diagnostic summary that separates single-provider winner latency
from multi-provider winner latency and names the slowest single-provider
winners.

Implementation:

- Extend `http_provider_races` with single-provider result success/failure
  counts.
- Summarize successful single-provider race-result latency separately from
  multi-provider winner latency.
- Add a bounded `single_provider_winners` list sorted by slowest max elapsed
  time, with provider URL, event count, total elapsed time, and max elapsed
  time.
- Print these fields in the console trace summary and include them in the JSON
  report.
- This is diagnostics-only: no provider fanout increase, no public gateway
  fallback, and no verification/caching trust change.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
```

Result:

- Formatting passed.
- Focused HTTP-provider trace summary test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-winners-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-winners-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1348ms` / `2006ms` / `2006ms`.
- Asset TTFB p50/p95/max: `269ms` / `1067ms` / `13567ms`.
- Run total p50/p95/max: `3608ms` / `16090ms` / `16090ms`.
- Max RSS/FD: `55688KiB` / `38`.
- Delegated provider lookup max: `10001ms`; one delegated lookup returned an
  HTTP send error, which caused the 13.6s slow asset tail.
- HTTP-provider races: `70` results, `70` successes, `0` failures, `70`
  winners inside the initial race width, `0` late winners, `67` rank-1 winners,
  `3` rank-2 winners, `0` rank-3-or-later winners.
- Single-provider HTTP race results: `39` successes, `0` failures, winner
  elapsed p50/p95/max `221ms` / `941ms` / `1260ms`.
- Multi-provider winner elapsed p50/p95/max: `78ms` / `181ms` / `252ms`.
- Slow single-provider winner spread:
  `https://ipfs-bridge.sia.dev/` had `39` events, total `15217ms`, max
  `1260ms`.
- HTTP-provider fetch p50/p95/max: `175ms` / `805ms` / `1208ms`; provider
  spread `ipfs-bridge.sia.dev=39`, `dag.w3s.link=31`.
- Block sources: `http_provider=84`, `bitswap=34`, `cache=1`.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets
git diff --check
```

Result:

- Formatting passed.
- Full `mobile-web-harness` suite passed: `33 passed`.
- Harness clippy passed with `-D warnings`.
- Workspace check passed.
- Diff whitespace check passed.

Interpretation:
Keep the diagnostic. The live sample again shows `0` rank-3-or-later HTTP
provider winners, so default race width should stay at `2`. It also shows a
clearer future target: single-provider HTTP fetches from
`ipfs-bridge.sia.dev` can be several times slower than multi-provider winners,
but the largest page tail in this sample still came from delegated routing
failure plus Bitswap fallback, not from HTTP-provider racing. A future
single-provider mitigation should be gated on repeated slow single-provider
winners and should not broaden all HTTP fanout.

## 2026-05-05 Keep: Race Late-Arriving Session Peers During Slow Provider Lookup

Question:
The single HTTP-provider diagnostic run exposed a different tail: one asset had
no recent session peer at request start, then spent `10001ms` in a failed
delegated provider lookup and `3504ms` in DHT fallback, but the actual Bitswap
block fetch took only `49ms` once a session peer was available. The existing
recent-peer shortcut only snapshots session peers before provider lookup starts,
so it can miss peers learned by concurrent page requests while routing is
stalled.

Hypothesis:
When provider lookup is already slow and no recent peer existed at request
start, cheaply watching for a late-arriving recent Bitswap peer can avoid
multi-second routing tails without delaying fast provider lookups or increasing
provider fanout.

Implementation:

- If a block starts with no recent Bitswap session peers, race provider lookup
  against a bounded in-memory wait for recent peers.
- The wait polls only the local recent-peer table for up to `2s` at `50ms`
  intervals; it does not dial or issue Bitswap requests unless a known-good peer
  appears.
- If a late peer appears before provider lookup completes, start the existing
  verified recent-peer shortcut.
- If provider lookup completes first, keep the existing provider path.
- If both are active and provider lookup returns an empty provider set, keep the
  existing behavior of waiting for the shortcut rather than failing immediately.
- Add `bitswap_session_late_peer_wait` tracing and map it to
  `fetching_bitswap` in harness/mobile progress summaries.
- Count late-peer waits, hits, misses, and max wait time in the harness Bitswap
  session summary.
- Add a deterministic test where a gated delegated lookup is held open, a
  recent Bitswap peer is recorded after lookup starts, and the block must load
  from Bitswap before the routing response is released.

Focused validation:

```sh
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
```

Result:

- Recent Bitswap peer tests passed: `5 passed`.
- Mobile progress phase mapping test passed.
- Harness progress phase mapping test passed.

Same-window baseline:

- Artifact paths:
  `/tmp/ipfs-tech-single-http-winners-r3-trace.jsonl` and
  `/tmp/ipfs-tech-single-http-winners-r3.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1348ms` / `2006ms` / `2006ms`.
- Asset TTFB p50/p95/max: `269ms` / `1067ms` / `13567ms`.
- Run total p50/p95/max: `3608ms` / `16090ms` / `16090ms`.
- Max RSS/FD: `55688KiB` / `38`.
- Delegated provider lookup max: `10001ms`.
- Slow asset `/ipns/ipfs.tech/_nuxt/DzK6mLCt.js` spent `13561ms` in
  `block_fetch_total`, sourced from Bitswap after a `10001ms` delegated routing
  error and `3504ms` DHT fallback; the final `bitswap_fetch` was only `49ms`.

Experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-late-session-peer-r3-trace.jsonl \
  --output /tmp/ipfs-tech-late-session-peer-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1390ms` / `2060ms` / `2060ms`.
- Asset TTFB p50/p95/max: `164ms` / `939ms` / `1493ms`.
- Run total p50/p95/max: `3607ms` / `4087ms` / `4087ms`.
- Max RSS/FD: `53240KiB` / `32`.
- Delegated provider lookup max: `296ms`; no delegated lookup failures in this
  sample.
- `bitswap_session_late_peer_wait` fired once, found one peer after `283ms`,
  and the following recent-peer shortcut returned a verified block in `74ms`.
- HTTP-provider races still had `0` rank-3-or-later winners.
- Block sources: `http_provider=73`, `bitswap=46`, `cache=1`.

Additional checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-late-session-peer-r3-trace.jsonl \
  --output /tmp/vitalik-late-session-peer-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-late-session-peer-r3-trace.jsonl \
  --output /tmp/daicowtf-late-session-peer-r3.json
```

Results:

- `vitalik-root-html-range` passed `3/3`; root/range TTFB p50/p95/max
  `145ms` / `416ms` / `416ms`, max RSS/FD `31872KiB` / `14`, block sources
  `http_provider=6`. The late-peer wait did not fire.
- `daicowtf-page-assets` passed `3/3`; root TTFB p50/p95/max
  `1300ms` / `1552ms` / `1552ms`, max RSS/FD `42436KiB` / `17`, block sources
  `http_provider=6`, `bitswap=3`. The late-peer wait did not fire.

Kubo comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-rust-vs-kubo-late-session-peer-r3.json
```

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB: Rust p50/p95 `1572ms` / `1695ms`; Kubo p50/p95 `2987ms` /
  `4770ms`; Rust/Kubo ratios `0.53x` p50 and `0.36x` p95.
- Asset TTFB: Rust p50/p95 `295ms` / `848ms`; Kubo p50/p95 `186ms` /
  `415ms`; Rust/Kubo ratios `1.59x` p50 and `2.04x` p95.
- Resources: Rust max RSS/FD `53376KiB` / `31`; Kubo max RSS/FD
  `262100KiB` / `355`.
- Interpretation: Rust is currently faster on root startup and much lighter on
  mobile resources for this case, but Kubo still has better asset TTFB. The next
  optimization target should remain asset/session behavior, not root routing.

Final validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full retrieval suite passed: `70 passed`, `1 ignored`.
- Full mobile suite passed: `26 passed`.
- Full mobile web harness suite passed: `34 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. The same-window `ipfs.tech` comparison is partly helped by live-network
noise because the baseline had a delegated routing error and the experiment did
not, but the deterministic test proves the exact missed-session-peer shape and
the live run shows the new path firing once with low cost. Fast provider
lookups are not delayed because provider lookup still wins the race, and the
secondary cases stayed within previous resource and latency envelopes.

## 2026-05-05 Keep: Extend Session Wait For Single HTTP Provider Results

Question:
After the late-session-peer change, Rust was faster than Kubo on `ipfs.tech`
root startup but still slower on asset TTFB. The traces showed that
multi-provider HTTP races were already fast, while single HTTP-provider results
still often meant `ipfs-bridge.sia.dev` tails. Recent Bitswap shortcut hits in
the same page session were usually under a few hundred milliseconds, but the
generic post-lookup wait was only `100ms`.

Hypothesis:
When provider lookup returns exactly one HTTP provider URL, waiting a little
longer for an already-running recent Bitswap session shortcut can cut asset
tails. Keep the normal `100ms` post-lookup wait for zero-HTTP and multi-HTTP
results so fast HTTP races are not delayed.

Implementation:

- Add `BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE=250ms`.
- Keep `BITSWAP_SESSION_POST_LOOKUP_GRACE=100ms` for all other provider sets.
- Select the longer wait only when the provider result contains exactly one
  HTTP provider URL.
- Include `provider_count` and `http_provider_count` on
  `bitswap_session_shortcut_post_lookup_wait` trace events.
- Add deterministic tests proving:
  - a delayed recent Bitswap peer can beat a hanging single HTTP provider; and
  - multi-HTTP provider results still keep the short wait and return via HTTP.

Focused validation:

```sh
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer
cargo test -p freedom-ipfs-retrieval single_http_provider_waits_longer_for_recent_bitswap_peer
cargo test -p freedom-ipfs-retrieval multi_http_provider_keeps_short_recent_peer_wait
```

Result:

- Recent Bitswap peer tests passed: `6 passed`.
- Single HTTP provider selective-wait test passed.
- Multi HTTP provider short-wait test passed.

Same-window baseline:

- Artifact paths:
  `/tmp/ipfs-tech-late-session-peer-r3-trace.jsonl` and
  `/tmp/ipfs-tech-late-session-peer-r3.json`.
- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1390ms` / `2060ms` / `2060ms`.
- Asset TTFB p50/p95/max: `164ms` / `939ms` / `1493ms`.
- Run total p50/p95/max: `3607ms` / `4087ms` / `4087ms`.
- Max RSS/FD: `53240KiB` / `32`.
- HTTP-provider races: `58` results, `36` single-provider successes, `0`
  failures, single-provider winner elapsed p50/p95/max `305ms` / `955ms` /
  `1023ms`, multi-provider winner elapsed p50/p95/max `85ms` / `168ms` /
  `182ms`.
- Block sources: `http_provider=73`, `bitswap=46`, `cache=1`.

Experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-postlookup250-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-postlookup250-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1272ms` / `1420ms` / `1420ms`.
- Asset TTFB p50/p95/max: `244ms` / `663ms` / `1076ms`.
- Run total p50/p95/max: `2848ms` / `3814ms` / `3814ms`.
- Max RSS/FD: `53588KiB` / `36`.
- HTTP-provider races: `43` results, `18` single-provider successes, `0`
  failures, single-provider winner elapsed p50/p95/max `232ms` / `931ms` /
  `931ms`, multi-provider winner elapsed p50/p95/max `73ms` / `227ms` /
  `306ms`.
- Bitswap session shortcuts increased to `57` hits, with `24`
  post-lookup waits; late-peer wait fired twice and hit twice.
- Block sources shifted toward the session path: `bitswap=69`,
  `http_provider=47`, `cache=1`.

Additional checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-single-http-postlookup250-r3-trace.jsonl \
  --output /tmp/vitalik-single-http-postlookup250-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-single-http-postlookup250-r3-trace.jsonl \
  --output /tmp/daicowtf-single-http-postlookup250-r3.json
```

Results:

- `vitalik-root-html-range` passed `3/3`; root/range TTFB p50/p95/max
  `151ms` / `424ms` / `424ms`, max RSS/FD `31872KiB` / `13`, block sources
  `http_provider=6`.
- `daicowtf-page-assets` passed `3/3`; root TTFB p50/p95/max
  `1050ms` / `1419ms` / `1419ms`, max RSS/FD `42684KiB` / `17`, block sources
  `http_provider=6`, `bitswap=3`.

Kubo comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-rust-vs-kubo-single-http-postlookup250-r3.json
```

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB: Rust p50/p95 `707ms` / `971ms`; Kubo p50/p95 `3302ms` /
  `4695ms`; Rust/Kubo ratios `0.21x` p50 and `0.21x` p95.
- Asset TTFB: Rust p50/p95 `250ms` / `966ms`; Kubo p50/p95 `211ms` /
  `784ms`; Rust/Kubo ratios `1.18x` p50 and `1.23x` p95.
- Resources: Rust max RSS/FD `53560KiB` / `37`; Kubo max RSS/FD
  `293812KiB` / `292`.

Final validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full retrieval suite passed: `72 passed`, `1 ignored`.
- Full gateway suite passed; all non-ignored unit/integration tests passed.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. The live experiment reduced `ipfs.tech` asset p95/max versus the
immediately previous baseline and moved more blocks onto verified session
Bitswap without widening provider fanout. The Kubo comparison also narrowed the
asset gap materially, though Kubo still wins asset TTFB. The main risk is that
a fast single HTTP provider might be delayed by up to `150ms` more than before
when a recent session shortcut is active, but the secondary `vitalik` and
`daicowtf` checks stayed inside prior resource/latency envelopes.

## 2026-05-05 Keep: Trace Post-Lookup Session Wait Outcomes

Question:
After keeping the selective `250ms` post-lookup wait for single HTTP-provider
results, the next tuning question is whether that grace is too short, too long,
or only useful in a few request shapes. The existing trace only emitted
`bitswap_session_shortcut_post_lookup_wait` when the wait timed out, which made
successful waits invisible and forced tuning from block-source counts.

Implementation:

- Keep retrieval behavior unchanged.
- Emit `bitswap_session_shortcut_post_lookup_wait` for every post-lookup wait
  completion, not only timeouts.
- Add `outcome=hit|miss|timeout|error`, `elapsed_ms`, `timeout_ms`,
  `provider_count`, and `http_provider_count`.
- Preserve backwards compatibility in the harness by treating old wait events
  without `outcome` as timeouts.
- Extend the harness Bitswap session summary with post-lookup hit, miss,
  timeout, error, max elapsed, wait-budget, timeout-budget, and HTTP-provider
  count buckets.
- Add a focused harness regression test for the outcome counters.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_post_lookup_wait_outcomes
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo test -p freedom-ipfs-retrieval single_http_provider_waits_longer_for_recent_bitswap_peer
```

Focused result:

- Formatting passed.
- New post-lookup outcome summary test passed.
- Existing slow-event trace summary test passed.
- Existing single HTTP-provider selective-wait retrieval test passed.

Live smoke with corrected summary fields:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-postlookup-outcomes-v2-r3-trace.jsonl \
  --output /tmp/ipfs-tech-postlookup-outcomes-v2-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1314ms` / `1351ms` / `1351ms`.
- Asset TTFB p50/p95/max: `269ms` / `906ms` / `1262ms`.
- Run total p50/p95/max: `3333ms` / `3682ms` / `3682ms`.
- Max RSS/FD: `53056KiB` / `31`.
- Block sources: `http_provider=115`, `bitswap=4`, `cache=1`.
- Delegated lookup max: `89ms`; HTTP-provider fetch p50/p95/max
  `161ms` / `644ms` / `929ms`.
- Bitswap session summary now showed:
  - `shortcut_post_lookup_waits=5`
  - `post_lookup_hits=1`
  - `post_lookup_timeouts=4`
  - `post_lookup_errors=0`
  - `post_lookup_budgets=250=4, 100=1`
  - `post_lookup_timeout_budgets=250=3, 100=1`
  - `post_lookup_http_counts=1=4, 3=1`
- An earlier exploratory raw trace from the same instrumentation, before the
  summary label was corrected, showed a busier session window with `74`
  post-lookup waits: `48` hits and `26` timeouts. In that raw trace,
  single-HTTP-provider waits used the `250ms` budget `38` times, with `30`
  hits and `8` timeouts.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `35 passed`.
- Full retrieval suite passed: `72 passed`, `1 ignored`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is diagnostics-only and does not change provider selection, Bitswap
fanout, caching, or verification behavior. It closes the measurement gap for
the next single-HTTP-provider grace retune: future runs can now tell whether a
post-lookup wait actually produced a verified session Bitswap block, timed out,
or merely added delay before the HTTP-provider path.

## 2026-05-05 Reject: Retune Single HTTP Post-Lookup Grace To 200ms Or 225ms

Question:
With post-lookup wait outcomes visible, test whether the selective single
HTTP-provider session grace can be reduced from `250ms`. In the exploratory raw
trace above, `29/30` single-HTTP `250ms` hits completed by `200ms`, and all
completed by `223ms`, so smaller values could shave timeout cost while
preserving most session wins.

Prototype:

- Temporarily changed `BITSWAP_SESSION_SINGLE_HTTP_POST_LOOKUP_GRACE` from
  `250ms` to `200ms`.
- After the deterministic guard failed, temporarily changed it to `225ms`.
- Restored the committed `250ms` value after measurement.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval single_http_provider_waits_longer_for_recent_bitswap_peer
cargo test -p freedom-ipfs-retrieval multi_http_provider_keeps_short_recent_peer_wait
```

Focused result:

- `200ms` failed the deterministic single-HTTP-provider guard. The delayed
  recent Bitswap peer did not reliably beat the hanging single HTTP-provider
  path.
- `225ms` passed both focused guards.

Live `225ms` experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-postlookup225-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-postlookup225-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `609ms` / `1621ms` / `1621ms`.
- Asset TTFB p50/p95/max: `229ms` / `892ms` / `4426ms`.
- Run total p50/p95/max: `2174ms` / `6492ms` / `6492ms`.
- Max RSS/FD: `51908KiB` / `30`.
- Block sources: `http_provider=119`, `bitswap=1`.
- Post-lookup waits did not fire in this live window:
  `shortcut_post_lookup_waits=0`.
- The run had one unrelated cold Bitswap asset tail:
  `bitswap_fetch=4319ms` for `/ipns/ipfs.tech/_nuxt/8Bs0wEmG.js`.

Comparison context:

- The immediately preceding `250ms` diagnostic live run passed `3/3`, with
  root TTFB p50/p95 `1314ms` / `1351ms`, asset TTFB p50/p95/max
  `269ms` / `906ms` / `1262ms`, run p50/p95 `3333ms` / `3682ms`, and
  `shortcut_post_lookup_waits=5`.
- The `225ms` run had a slightly better asset p95, but worse root p95 and a
  much worse max/run p95 due to a tail unrelated to the post-lookup grace.

Decision:
Reject and restore `250ms`. The `200ms` candidate is too tight for the
deterministic guard, and the `225ms` live sample did not exercise the target
path enough to justify a behavior change. The maximum possible timeout saving
from `225ms` is only `25ms`, while losing a late session hit would be more
expensive. Keep the current `250ms` selective grace until repeated outcome
traces show a clearer cutoff with margin.

## 2026-05-05 Keep: Summarize HTTP Provider Latencies By Provider

Question:
The harness already counted HTTP-provider milestones by provider, but the
provider rows only exposed total counts and maxes. Recent live runs have mixed
two different tail shapes: slow verified HTTP-provider responses, and slow
delegated routing before the first HTTP provider is available. Add per-provider
latency summaries so future experiments can tell whether a tail belongs to a
specific HTTP provider or to the routing path before provider fetch starts.

Implementation:

- Keep gateway, retrieval, provider selection, verification, and caching
  behavior unchanged.
- Extend `TraceHttpProviderMilestoneAggregate` with `LatencySummary` fields for:
  - total HTTP-provider fetch elapsed time
  - response-header elapsed time
  - first response chunk elapsed time
  - response-body elapsed time
- Print those summaries in the provider milestone rows while preserving the
  existing max fields.
- Add focused harness assertions for provider-level p50 header, first-chunk,
  body, and total elapsed values.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
```

Focused result:

- Formatting passed.
- The HTTP-provider trace summary regression test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-provider-latency-summary-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-provider-latency-summary-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `872ms` / `882ms` / `882ms`.
- Asset TTFB p50/p95/max: `231ms` / `1109ms` / `6055ms`.
- Run total p50/p95/max: `2737ms` / `8118ms` / `8118ms`.
- Max RSS/FD: `47196KiB` / `27`.
- Block sources: `http_provider=120`.
- Delegated lookup max: `5876ms`; first HTTP-provider max: `5875ms`;
  target-met max: `5298ms`.
- HTTP-provider fetch p50/p95/max: `162ms` / `424ms` / `920ms`.
- Provider `https://ipfs-bridge.sia.dev/`: `63` events, elapsed
  p50/p90/p95/max `172ms` / `378ms` / `502ms` / `920ms`, headers
  p50/p90/p95/max `162ms` / `192ms` / `308ms` / `661ms`, bodies
  p50/p90/p95/max `170ms` / `370ms` / `484ms` / `913ms`.
- Provider `https://dag.w3s.link/`: `42` events, elapsed p50/p90/p95/max
  `57ms` / `96ms` / `107ms` / `162ms`, headers p50/p90/p95/max
  `54ms` / `94ms` / `104ms` / `156ms`, bodies p50/p90/p95/max
  `55ms` / `95ms` / `105ms` / `159ms`.

Interpretation:

- The provider-level summaries show `dag.w3s.link` was materially faster than
  `ipfs-bridge.sia.dev` in this window.
- The worst asset tail in the same run was not explained by HTTP-provider body
  latency. It came from delegated routing / first-HTTP-provider delay:
  `/ipns/ipfs.tech/_nuxt/AKg0Znx-.js` had a `6052ms` asset TTFB and
  `5876ms` delegated provider lookup.
- This supports keeping the diagnostic and points future behavior work toward
  delegated routing first-byte / first-HTTP-provider tails. It does not by
  itself justify changing HTTP-provider ranking or fanout policy.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `35 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is diagnostics-only and preserves read-only, verified-block
behavior. It gives the next long-running optimization agent a provider-level
latency lens for live Kubo comparisons and future routing/provider experiments.

## 2026-05-05 Keep: Summarize Delegated Routing Latencies

Question:
The previous HTTP-provider latency summary showed per-provider fetch costs, but
some visible page-load tails happen before the HTTP-provider race starts. The
delegated routing summary still showed only max elapsed values for response
headers, first chunk, first HTTP provider, and target-met events. Add
distribution summaries so future experiments can tell whether first-HTTP delay
is rare tail noise or a repeated routing bottleneck.

Implementation:

- Keep gateway, routing, retrieval, provider selection, verification, and cache
  behavior unchanged.
- Extend delegated-provider lookup summaries with `LatencySummary` fields for:
  - full delegated lookup elapsed time
  - response headers
  - first response chunk
  - first HTTP provider
  - target-met elapsed time
  - single-HTTP-provider lookup elapsed time
  - single-HTTP-provider first-HTTP time
- Keep raw sample vectors out of serialized summary output after finalization.
- Print the summaries globally and per delegated endpoint.
- Add focused harness assertions for the new summary fields.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_delegated_http_provider_distribution
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
```

Focused result:

- Formatting passed after rustfmt.
- Delegated HTTP-provider distribution summary test passed.
- Existing mobile progress phase summary test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-latency-summary-r3-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-latency-summary-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1551ms` / `1561ms` / `1561ms`.
- Asset TTFB p50/p95/max: `264ms` / `1001ms` / `1942ms`.
- Run total p50/p95/max: `4081ms` / `4661ms` / `4661ms`.
- Max RSS/FD: `54376KiB` / `35`.
- Block sources: `http_provider=112`, `bitswap=8`.
- Delegated lookup events: `105`, all successful, `1388` providers,
  `185` HTTP providers.
- Delegated lookup elapsed p50/p90/p95/max:
  `23ms` / `59ms` / `426ms` / `1865ms`.
- Delegated response headers p50/p90/p95/max:
  `18ms` / `54ms` / `419ms` / `1865ms`.
- First HTTP provider p50/p90/p95/max:
  `20ms` / `57ms` / `425ms` / `1865ms`.
- Target-met elapsed p50/p90/p95/max:
  `0ms` / `39ms` / `45ms` / `1865ms`.
- HTTP-provider distribution: `zero=4`, `single=59`, `multi=42`,
  `single_target_miss=59`.
- Single-HTTP-provider elapsed p50/p90/p95/max:
  `26ms` / `59ms` / `426ms` / `895ms`.
- Single-HTTP-provider first-HTTP p50/p90/p95/max:
  `23ms` / `57ms` / `425ms` / `893ms`.
- HTTP-provider races: `97` results, all successful, `0` late winners;
  `56` single-provider races and `41` multi-provider races.
- HTTP-provider fetch p50/p95/max: `163ms` / `699ms` / `739ms`.

Interpretation:

- The first-HTTP-provider tail is visible but not dominant in this live window:
  p95 is `425ms`, with one `1865ms` max. This is much less severe than the
  earlier `5875ms` first-HTTP tail, but the summary now makes that distinction
  explicit.
- Single-provider responses remain common: `59/105` delegated lookups had
  exactly one HTTP provider and all missed the three-provider target.
- The slowest request in this run was
  `/ipns/ipfs.tech/_nuxt/community-hero.Cp0BCcC7.jpg`, with the delegated
  provider lookup itself taking `1865ms`.
- This does not justify a routing behavior change by itself. It gives future
  behavior experiments a compact gate: only prototype first-HTTP hedging,
  endpoint fanout, or DHT overlap when repeated traces show persistent high
  p95, not only isolated maxes.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `35 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is harness-only diagnostics and preserves the read-only,
verified-block retrieval model. The next optimization agent can now compare
delegated-routing p95/max against HTTP-provider p95/max without spelunking raw
JSONL.

## 2026-05-05 Baseline: Current Delegated-Latency Build vs Kubo

Goal:
After adding delegated-routing and HTTP-provider latency summaries, collect a
fresh same-window Kubo comparison without changing runtime behavior. This gives
future experiments a current baseline with richer trace summaries.

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-latency-summary-compare-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-delegated-latency-summary-compare-r3.json
```

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Root TTFB: Rust p50/p95 `871ms` / `951ms`; Kubo p50/p95
  `2067ms` / `3780ms`; Rust/Kubo ratios `0.42x` p50 and `0.25x` p95.
- Asset TTFB: Rust p50/p95 `272ms` / `706ms`; Kubo p50/p95
  `228ms` / `1175ms`; Rust/Kubo ratios `1.19x` p50 and `0.60x` p95.
- Resources: Rust max RSS/FD `54092KiB` / `37`; Kubo max RSS/FD
  `271368KiB` / `285`; Rust/Kubo ratios `0.20x` RSS and `0.13x` FDs.
- Rust delegated lookup events: `102`, all successful, max `97ms`.
- Delegated lookup elapsed p50/p90/p95/max:
  `24ms` / `50ms` / `71ms` / `97ms`.
- First HTTP provider p50/p90/p95/max:
  `21ms` / `49ms` / `69ms` / `97ms`.
- HTTP-provider distribution: `zero=6`, `single=55`, `multi=41`,
  `single_target_miss=55`.
- HTTP-provider races: `75` results, all successful, `0` late winners;
  `42` single-provider races and `33` multi-provider races.
- HTTP-provider fetch p50/p95/max: `163ms` / `500ms` / `664ms`.
- Provider spread: `ipfs-bridge.sia.dev=42`, `dag.w3s.link=33`.
- Block sources: `http_provider=69`, `bitswap=49`, `cache=2`.
- Bitswap session summary: `shortcut_post_lookup_waits=38`,
  `post_lookup_hits=21`, `post_lookup_timeouts=17`.

Interpretation:

- In this window, delegated routing was not the bottleneck. First-HTTP p95 was
  only `69ms`.
- Rust is already materially faster than Kubo for root p50/p95 and asset p95
  while using far fewer resources.
- Kubo still wins asset p50. The slowest Rust sample in this run was a
  Bitswap-served asset (`BfUTpfA9.js`) at `1036ms`, not an HTTP-provider or
  delegated-routing tail.
- The next behavior experiment should be gated by trace shape: work on
  delegated routing only when repeated traces show high delegated p95; otherwise
  investigate page/session Bitswap shortcut timing, single-provider HTTP
  winners, or asset-p50 paths.

Decision:
Baseline only. No code change.

## 2026-05-05 Keep: Summarize Post-Lookup Session Wait Latencies

Question:
Post-lookup session waits now expose hit/miss/timeout outcomes, but the harness
still only prints counts and max elapsed time. Future tuning of the `100ms`
generic wait and the `250ms` single-HTTP-provider wait needs latency
distributions by outcome, especially for single-HTTP-provider waits.

Implementation:

- Keep retrieval behavior unchanged.
- Extend the harness Bitswap session summary with `LatencySummary` fields for:
  - all post-lookup waits
  - post-lookup hits
  - post-lookup timeouts
  - single-HTTP-provider post-lookup waits
  - single-HTTP-provider post-lookup hits
  - single-HTTP-provider post-lookup timeouts
- Keep raw sample vectors out of serialized summary output after finalization.
- Print a second `post-lookup latency` line when post-lookup waits are present.
- Add focused harness assertions for the new summary fields.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_post_lookup_wait_outcomes
```

Focused result:

- Formatting passed.
- The post-lookup outcome summary regression test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-postlookup-latency-summary-r3-trace.jsonl \
  --output /tmp/ipfs-tech-postlookup-latency-summary-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1290ms` / `1393ms` / `1393ms`.
- Asset TTFB p50/p95/max: `215ms` / `931ms` / `1227ms`.
- Run total p50/p95/max: `2395ms` / `4311ms` / `4311ms`.
- Max RSS/FD: `52248KiB` / `28`.
- Block sources: `bitswap=74`, `http_provider=44`.
- Delegated lookup elapsed p50/p95/max: `31ms` / `77ms` / `845ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `657ms` / `674ms`.
- Bitswap session summary: `shortcut_post_lookup_waits=72`,
  `post_lookup_hits=43`, `post_lookup_timeouts=29`,
  `post_lookup_budgets=100=39, 250=33`,
  `post_lookup_timeout_budgets=100=20, 250=9`,
  `post_lookup_http_counts=3=35, 1=33, 0=4`.
- Post-lookup elapsed p50/p90/p95/max:
  `88ms` / `250ms` / `251ms` / `299ms`.
- Post-lookup hit elapsed p50/p90/p95/max:
  `53ms` / `118ms` / `125ms` / `164ms`.
- Post-lookup timeout elapsed p50/p90/p95/max:
  `101ms` / `251ms` / `251ms` / `299ms`.
- Single-HTTP-provider elapsed p50/p90/p95/max:
  `88ms` / `251ms` / `251ms` / `299ms`.
- Single-HTTP-provider hit elapsed p50/p90/p95/max:
  `68ms` / `125ms` / `130ms` / `164ms`.
- Single-HTTP-provider timeout elapsed p50/p90/p95/max:
  `251ms` / `299ms` / `299ms` / `299ms`.

Interpretation:

- This live window does not support lengthening the single-HTTP post-lookup
  wait beyond `250ms`. Successful single-HTTP session hits were already well
  inside the budget, with p95 `130ms` and max `164ms`.
- The timeout side pays the full budget: single-HTTP timeout p50 was `251ms`
  and max was `299ms`.
- This also explains why a smaller retune needs margin: some hits are above
  `125ms`, and the deterministic `200ms` guard already failed earlier.
- Keep collecting this summary before any future retune; use repeated p95/max
  hit data, not isolated samples.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `35 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is harness-only diagnostics and preserves current session timing,
provider policy, verification, and resource behavior.

## 2026-05-05 Keep: Summarize Late Session Peer Wait Latencies

Question:
Late session peer waits are intentionally opportunistic: they only matter when
a request starts with no recent Bitswap peer and another concurrent request
discovers one while provider lookup is still pending. The harness counted
late-peer waits, hits, misses, and max elapsed time, but did not expose p50/p95
shape.

Implementation:

- Keep retrieval behavior unchanged.
- Extend the Bitswap session summary with `LatencySummary` fields for:
  - all late-peer waits
  - late-peer hits
  - late-peer misses
- Keep raw sample vectors out of serialized summary output after finalization.
- Print a `late-peer latency` line when late-peer waits are present.
- Add focused harness assertions for the new summary fields.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_late_session_peer_waits
```

Focused result:

- Formatting passed.
- The late-session-peer trace summary test passed.

Live smoke:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-late-peer-latency-summary-r3-trace.jsonl \
  --output /tmp/ipfs-tech-late-peer-latency-summary-r3.json
```

Live result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1151ms` / `1790ms` / `1790ms`.
- Asset TTFB p50/p95/max: `241ms` / `1216ms` / `1866ms`.
- Run total p50/p95/max: `3965ms` / `4629ms` / `4629ms`.
- Max RSS/FD: `53772KiB` / `34`.
- Block sources: `http_provider=66`, `bitswap=52`, `cache=1`.
- Delegated lookup elapsed p50/p95/max: `22ms` / `55ms` / `70ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `656ms` / `892ms`.
- Bitswap session summary: `shortcut_post_lookup_waits=92`,
  `post_lookup_hits=36`, `post_lookup_timeouts=56`.
- Late-peer waits did not fire in this live window:
  `late_peer_waits=0`, `late_peer_hits=0`, `late_peer_misses=0`.

Interpretation:

- This confirms the late-peer path is sparse and should not be tuned from one
  arbitrary `ipfs.tech` run.
- The summary is still useful for future regression windows where late-peer
  waits do fire, especially because earlier kept evidence showed the path can
  rescue a request after a peer appears during provider lookup.
- Do not retune `BITSWAP_SESSION_LATE_PEER_WAIT` from this sample. Wait for
  repeated traces with actual late-peer hits/misses and compare p95 against
  provider lookup p95.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `35 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is harness-only diagnostics and preserves current late-peer wait
behavior.

## 2026-05-05 Keep: Summarize Raw Range Batch Fetches

Question:
The kept bounded raw range child-fetch path improved the deterministic seeded
range workload, but the trace only showed that a child block was fetched. It did
not expose the requested byte span, batch width, uncached width, source, or
child-fetch latency. That made the remaining Rust-vs-Kubo seeded gap harder to
attribute.

Implementation:

- Keep retrieval behavior unchanged.
- Add low-volume fields to `block_range_batch_fetch` trace events:
  `range_start`, `range_end`, `range_len`, `range_count`,
  `uncached_range_count`, and `elapsed_ms`.
- Add a harness aggregate for block range batch fetches:
  events, requested bytes, latency p50/p90/p95/max, max range length, max batch
  width, max uncached width, and sources.
- Map `block_range_batch_fetch` into mobile-facing progress phases by source:
  Bitswap => `fetching_bitswap`, HTTP provider => `fetching_http_provider`,
  cache => `cache_hit`, otherwise `streaming`.
- Add focused harness and mobile progress tests.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_block_range_batch_fetches
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo check -p freedom-ipfs-retrieval --all-targets
```

Focused result:

- Formatting passed.
- The block-range summary harness test passed.
- The mobile progress phase mapping test passed.
- Retrieval crate check passed.

Live seeded comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-batch-summary-r3-trace.jsonl \
  --comparison-output /tmp/harness-range-batch-summary-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95: `172ms` / `186ms`.
- Kubo root TTFB p50/p95: `54ms` / `56ms`.
- Root TTFB ratios: p50 `3.19x`, p95 `3.32x`.
- Max RSS/FD: Rust `39292KiB` / `13`; Kubo `91392KiB` / `37`.
- Block range batch fetches: `events=6`, `bytes=900`,
  elapsed p50/p90/p95/max `70ms` / `99ms` / `99ms` / `99ms`.
- Batch shape: `max_range_len=150`, `max_range_count=2`,
  `max_uncached_range_count=2`.
- Sources: `bitswap=6`.
- Gateway direct bodies: `3` events, `900` bytes, max elapsed `101ms`.
- Bitswap connections established: `3`, established p50/p95 `56ms` / `56ms`.
- Post-lookup session waits all hit: `6` hits, p50/p95/max
  `13ms` / `43ms` / `43ms`.

Interpretation:

- The remaining seeded gap is now visible as range child Bitswap fetch latency
  plus Rust's per-run provider lookup/dial work. The actual byte ranges are tiny
  (`150` bytes each), so optimizing block transfer payload size is not the
  first lever.
- The range batch path is doing the right bounded work: two uncached raw child
  ranges per request, no HTTP provider fallback, no extra public gateway path,
  and low resource usage.
- Future seeded work should look for ways to reduce setup and child fetch
  latency without speculative duplicate work. Compare against the prior rejected
  range multi-want and prefetch experiments before trying another hook.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-mobile
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `36 passed`.
- Full retrieval suite passed: `72 passed`, `1 ignored`.
- Full mobile suite passed: `26 passed`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is diagnostics plus mobile progress classification only. It preserves
read-only behavior, avoids public gateway fallback, keeps block verification
unchanged, and keeps mobile resource usage low.

## 2026-05-05 Keep: Summarize Session Pre-Lookup Wait Latencies

Question:
The seeded range trace showed raw child range fetches repeatedly hitting the
recent-session peer path, but the pre-lookup wait event only reported the
outcome and timeout budget. Without elapsed time and a harness summary, future
retunes of the `50ms` pre-lookup grace require manual trace scanning and can
easily repeat already rejected head-start experiments.

Implementation:

- Keep retrieval behavior unchanged.
- Add `elapsed_ms` to `bitswap_session_shortcut_pre_lookup` events for hit,
  miss, and timeout outcomes.
- Extend the harness Bitswap session summary with pre-lookup wait counts,
  outcomes, budget counts, max elapsed time, and latency summaries for all
  waits/hits/misses/timeouts.
- Map `bitswap_session_shortcut_pre_lookup` to the harness progress phase
  `fetching_bitswap`.
- Add a focused harness regression test, including backward-compatible timeout
  handling for older traces without `elapsed_ms`.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_pre_lookup_wait_outcomes
cargo test -p freedom-ipfs-retrieval recent_bitswap_peer_head_start_can_avoid_provider_lookup
```

Focused result:

- Formatting passed.
- Focused pre-lookup harness summary test passed.
- Focused retrieval session head-start test passed.

Live seeded comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-prelookup-latency-summary-r3-trace.jsonl \
  --comparison-output /tmp/harness-prelookup-latency-summary-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95: `193ms` / `310ms`.
- Kubo root TTFB p50/p95: `53ms` / `58ms`.
- Max RSS/FD: Rust `40572KiB` / `13`; Kubo `90840KiB` / `37`.
- Block range batch fetches: `events=6`, `bytes=900`,
  elapsed p50/p90/p95/max `75ms` / `221ms` / `221ms` / `221ms`.
- Pre-lookup waits: `6`, hits `0`, misses `0`, timeouts `6`,
  budget `50=6`.
- Pre-lookup elapsed p50/p90/p95/max:
  `51ms` / `52ms` / `52ms` / `52ms`.
- Post-lookup waits: `6`, hits `5`, timeouts `1`,
  budget `100=6`, timeout budget `100=1`.
- Post-lookup hit elapsed p50/p90/p95/max:
  `19ms` / `51ms` / `51ms` / `51ms`.
- One run had a child range tail: `block_range_batch_fetch=221ms`,
  `bitswap_fetch_cancelled=154ms`, and request elapsed `307ms`.

Interpretation:

- The current `50ms` pre-lookup grace consistently expires before this seeded
  child range path finishes. Most requests still succeed during post-lookup,
  but this run caught one tail where the shortcut did not win inside the
  post-lookup budget.
- This supports measuring the distribution before any future retune. It does
  not by itself justify lengthening the grace: earlier `100ms` range head-start
  and multi-want range hooks had weak or unstable repeat evidence.
- Future work should use this summary to separate "pre-lookup always too short"
  from "Bitswap child fetch itself is tailing", then retune only with repeated
  p95 evidence across seeded and public page/range workloads.

Final validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo test -p freedom-ipfs-retrieval
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Full mobile web harness suite passed: `37 passed`.
- Full retrieval suite passed: `72 passed`, `1 ignored`.
- Workspace check passed.
- Workspace clippy passed with `-D warnings`.
- Diff whitespace check passed.

Decision:
Keep. This is diagnostics-only: no provider policy change, no public gateway
fallback, no cache contract change, and no block verification change.

## 2026-05-05 Reject: Replacement Multi-CID Range Shortcut

Question:
The seeded multiblock range workload still showed duplicate same-peer child
Bitswap commands and occasional shortcut wait tails. A plausible optimization
was to replace the existing parallel per-child shortcut path with a single
multi-CID request to the recent session peer for adjacent uncached ranges. The
hypothesis was that fewer commands, fewer child provider lookups, and fewer
parallel same-peer streams would reduce range TTFB.

Prototype:

- Add a `100ms` recent-peer multi-CID range shortcut before the existing
  per-child fallback path.
- For multiple uncached ranges, send one `SharedBitswapClient::fetch_many`
  command to recent Bitswap peers.
- Store returned requested blocks through the normal verified
  `store_block_with_trace(..., "bitswap", true)` path before serving.
- Store non-requested extras as `bitswap_extra`.
- Emit `bitswap_session_batch_shortcut_start` and
  `bitswap_session_batch_shortcut` trace events.
- Fall back to the existing individual `fetch_block_with_source` path on
  timeout or error.

Focused validation on the prototype:

```sh
cargo check -p freedom-ipfs-retrieval --all-targets
cargo test -p freedom-ipfs-retrieval shared_bitswap_client_fetch_many_accepts_multi_cid_incoming_blocks
cargo test -p freedom-ipfs-unixfs file_range_batches_adjacent_raw_child_ranges
```

Focused result:

- Retrieval check passed.
- Focused multi-CID Bitswap client test passed.
- Focused UnixFS adjacent range batching test passed.

Prototype live seeded comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-replacement-batch-r3-trace.jsonl \
  --comparison-output /tmp/harness-range-replacement-batch-r3.json
```

Prototype result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95: `192ms` / `197ms`.
- Kubo root TTFB p50/p95: `49ms` / `52ms`.
- Max RSS/FD: Rust `40108KiB` / `13`; Kubo `89904KiB` / `40`.
- Trace lines: `117`.
- Delegated provider lookups: `3`.
- Bitswap commands: `6`, multi-CID commands: `3`, total CIDs: `9`.
- Bitswap incoming batches: `events=3`, `total_cids=6`,
  `max_cids=2`, `requested_blocks=6`, `extra_blocks=0`,
  `max_elapsed_ms=92`.
- Block range batch fetches: `events=6`, `bytes=900`,
  elapsed p50/p90/p95/max `72ms` / `93ms` / `93ms` / `93ms`.
- No `bitswap_fetch_cancelled` events.

Same-window baseline comparison:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-range-replacement-batch-ab-baseline-r3-trace.jsonl \
  --comparison-output /tmp/harness-range-replacement-batch-ab-baseline-r3.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Rust root TTFB p50/p95: `154ms` / `171ms`.
- Kubo root TTFB p50/p95: `51ms` / `52ms`.
- Max RSS/FD: Rust `40088KiB` / `13`; Kubo `93808KiB` / `36`.
- Trace lines: `164`.
- Delegated provider lookups: `7`.
- Bitswap commands: `9`, multi-CID commands: `0`, total CIDs: `9`.
- Pre-lookup waits: `6`, hits `1`, timeouts `5`, budget `50=6`,
  elapsed p50/p90/p95/max `51ms` / `51ms` / `51ms` / `51ms`.
- Post-lookup waits: `4`, hits `4`, timeouts `0`,
  hit elapsed p50/p90/p95/max `16ms` / `29ms` / `29ms` / `29ms`.
- Block range batch fetches: `events=6`, `bytes=900`,
  elapsed p50/p90/p95/max `59ms` / `85ms` / `85ms` / `85ms`.
- No `bitswap_fetch_cancelled` events.

Interpretation:

- The prototype did reduce duplicate work: fewer trace lines, fewer delegated
  lookups, and fewer Bitswap commands.
- That reduction did not improve the user-facing metric in the same test
  window. Rust root TTFB regressed from `154ms` / `171ms` p50/p95 to
  `192ms` / `197ms`, and the block range batch fetch latency summary also
  regressed from `59ms` / `85ms` p50/p95 to `72ms` / `93ms`.
- The added batch wait shape is not justified for this latency-focused path.
  It may still be interesting only if resource reduction becomes an explicit
  product priority, or if a future variant can avoid adding a fixed batch
  budget on already-fast baseline windows.

Decision:
Reject and keep the production code on the existing per-child shortcut path.
The prototype was removed after the A/B run. Future work should not add a
replacement multi-CID range shortcut unless repeated seeded and public workload
evidence shows a real p50/p95 win without weakening verification, cache
ordering, read-only behavior, or mobile resource limits.

## 2026-05-05 Keep: Kubo Seed-Setup Adjusted Comparison Metrics

Question:
The deterministic seeded Bitswap range harness is useful for range/session
experiments, but its Rust and Kubo setup paths are intentionally different.
Rust learns the seed through the delegated-routing endpoint during the gateway
request. Kubo is swarm-connected to the seed before the gateway request starts.
That makes raw Kubo root TTFB look like pure request latency while hiding setup
work that Rust pays inside the request.

Implementation:

- Keep gateway and retrieval behavior unchanged.
- Preserve the existing raw Rust and Kubo root/asset TTFB ratios.
- Add comparison JSON fields:
  `kubo_setup_adjusted_root_ttfb_p50_ms`,
  `setup_adjusted_root_ttfb_p50_ratio`,
  `kubo_setup_adjusted_root_ttfb_p95_ms`, and
  `setup_adjusted_root_ttfb_p95_ratio`.
- Compute the adjusted Kubo metric from measured runs by adding each run's
  `bitswap_seed_connect_elapsed_ms` to that run's root `ttfb_ms`, then deriving
  p50/p95 from the adjusted samples.
- Print `bitswap seed setup` in the comparison console summary so seeded reports
  explicitly show `delegated_router_provider_lookup` versus
  `swarm_connect_before_request`.
- Print `root_ttfb_kubo_setup_adjusted` only when adjusted samples exist.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness comparison_case_reports_kubo_setup_adjusted_root_ttfb
cargo test -p mobile-web-harness
git diff --check
```

Focused result:

- Formatting passed.
- Focused adjusted comparison metric test passed.
- Full mobile web harness suite passed: `38 passed`.
- Diff whitespace check passed.

Live seeded validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 1 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-kubo-setup-adjusted-r1-trace.jsonl \
  --comparison-output /tmp/harness-kubo-setup-adjusted-r1.json
```

Live result:

- Rust and Kubo both passed `1/1`.
- Seed setup: Rust `delegated_router_provider_lookup`; Kubo
  `swarm_connect_before_request`.
- Kubo seed connect p50/p95/max: `54ms` / `54ms` / `54ms`.
- Raw root TTFB: Rust `158ms`, Kubo `52ms`, ratio `3.04x`.
- Setup-adjusted root TTFB: Rust `158ms`, Kubo `106ms`, ratio `1.49x`.
- Max RSS/FD: Rust `39548KiB` / `13`; Kubo `88192KiB` / `36`.
- Rust trace: `54` lines, block range batch fetch elapsed p50/p95/max
  `50ms` / `83ms` / `83ms`.

Decision:
Keep. This is benchmark reporting only, and it makes seeded Rust-vs-Kubo range
experiments more honest without changing gateway, retrieval, verification,
caching, provider policy, or mobile resource behavior.

## 2026-05-05 Keep: Track Insert Eviction Instead Of Rechecking Stored Block

Question:
The seeded Bitswap range trace still showed synchronous block-store write time
on the request path. `SqliteBlockStore::put_block` inserted a verified block,
ran eviction, then issued an extra `SELECT 1` to check whether the block still
existed before adding it to the verified hot cache. Can we preserve durable
cache-before-return semantics while avoiding that redundant existence query?

Implementation:

- Keep the normal insert and eviction ordering unchanged.
- Replace the post-eviction `block_exists` query with eviction tracking.
- Return whether eviction deleted the just-inserted CID from
  `evict_if_needed_tracking`.
- Populate the verified hot cache only when the just-inserted block was not
  evicted.
- Remove the old private `block_exists` helper.
- Add a focused store test proving a block inserted into an undersized cache is
  not left in the verified hot cache after immediate eviction.

This keeps the node read-only from the network perspective, still stores only
verified bytes, and does not serve or hot-cache data that was immediately
removed by the byte budget.

Baseline:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-store-exists-baseline-r3-trace.jsonl \
  --comparison-output /tmp/harness-store-exists-baseline-r3.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Raw root TTFB p50/p95: Rust `191ms` / `200ms`; Kubo `53ms` /
  `53ms`; ratio `3.60x` / `3.77x`.
- Setup-adjusted root TTFB: Rust `191ms` / `200ms`; Kubo `108ms` /
  `112ms`; ratio `1.77x` / `1.79x`.
- Max RSS/FD: Rust `39692KiB` / `13`; Kubo `89180KiB` / `36`.
- Rust trace events: `166`.
- Block store puts: `events=9`, `bytes=1573341`, total elapsed `85ms`,
  max `21ms`.
- Block range batch fetch elapsed p50/p95/max: `64ms` / `107ms` /
  `107ms`.
- Delegated provider lookups: `8`.
- Session waits: pre-lookup `6`, hits `0`, timeouts `6`; post-lookup
  `5`, hits `5`, hit elapsed p50/p95/max `31ms` / `50ms` / `50ms`.

Patched validation:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --bitswap-seed-car /tmp/harness-bitswap-seed.car \
  --corpus /tmp/harness-bitswap-seed-corpus.json \
  --case bitswap-seeded-multiblock-boundary-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --timeout-secs 60 \
  --run-timeout-secs 120 \
  --asset-concurrency 1 \
  --trace-output /tmp/harness-store-track-eviction-r3-trace.jsonl \
  --comparison-output /tmp/harness-store-track-eviction-r3.json
```

Patched result:

- Rust and Kubo both passed `3/3`.
- Raw root TTFB p50/p95: Rust `153ms` / `173ms`; Kubo `57ms` /
  `59ms`; ratio `2.68x` / `2.93x`.
- Setup-adjusted root TTFB: Rust `153ms` / `173ms`; Kubo `111ms` /
  `117ms`; ratio `1.38x` / `1.48x`.
- Max RSS/FD: Rust `39600KiB` / `13`; Kubo `88916KiB` / `41`.
- Rust trace events: `162`.
- Block store puts: `events=9`, `bytes=1573341`, total elapsed `58ms`,
  max `21ms`.
- Block range batch fetch elapsed p50/p95/max: `54ms` / `81ms` /
  `81ms`.
- Delegated provider lookups: `6`.
- Session waits: pre-lookup `6`, hits `2`, timeouts `4`; post-lookup
  `3`, hits `3`, hit elapsed p50/p95/max `21ms` / `22ms` / `22ms`.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-store
cargo check -p freedom-ipfs-store --all-targets
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-retrieval
```

Focused result:

- Formatting passed.
- Store tests passed: `26 passed`.
- Store all-target check passed.
- Gateway tests passed, including focused gateway suites and local soak/public
  corpus coverage that is enabled by default.
- Retrieval tests passed: `72 passed`, `1 ignored`.

Final validation:

```sh
cargo fmt --all --check
git diff --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Final result:

- Formatting passed.
- Diff whitespace check passed.
- Workspace all-target compile check passed.
- Workspace all-target clippy passed with warnings denied.

Decision:
Keep. The same-window seeded comparison improved Rust root TTFB, setup-adjusted
Kubo ratio, block-store write time, range batch latency, and provider/session
wait counts while keeping cache correctness explicitly covered by the new
undersized-cache test.

## 2026-05-05 Keep: Shorten Delegated First-HTTP Provider Grace

Question:
The current `ipfs.tech` comparison trace showed many delegated-routing lookups
where the streamed response exposed the first HTTP provider quickly, then waited
the full single-provider grace without finding additional HTTP diversity. In
the baseline window, single-HTTP-provider lookups had first-HTTP p95 `49ms` but
total delegated lookup p95 `277ms` and max `300ms`.

Hypothesis:
Reducing `STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE` from `250ms` to
`100ms` should cut single-provider delegated-routing latency while still giving
streamed responses a short chance to produce more HTTP providers. The behavior
still returns verified provider records only, keeps the light-DHT low-diversity
fallback, does not add public gateway fallback, and does not change block
verification.

Implementation:

- Change `STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE` from `250ms` to
  `100ms`.
- Keep `STREAMING_DELEGATED_HTTP_PROVIDER_TARGET=3`.
- Keep max delegated response bytes/provider caps unchanged.
- Keep existing response milestone tracing so the run can prove whether the
  shorter grace actually reduces the single-provider wait.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed_delegated_response_returns_after_first_http_provider_grace
cargo test -p freedom-ipfs-routing
```

Focused result:

- Formatting passed.
- The focused streamed first-HTTP-provider grace test passed.
- Full routing suite passed: `24 passed`, `1 ignored`.

Same-window baseline:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-current-shape-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-current-shape-r3.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1285ms` / `2097ms`; Kubo `2640ms` /
  `3728ms`; ratio `0.49x` / `0.56x`.
- Asset TTFB p50/p95: Rust `280ms` / `1155ms`; Kubo `106ms` /
  `318ms`; ratio `2.64x` / `3.63x`.
- Max RSS/FD: Rust `53772KiB` / `32`; Kubo `294276KiB` / `284`.
- Delegated provider lookups: `91`, p50/p95/max `32ms` / `102ms` /
  `300ms`.
- Single-HTTP-provider delegated lookups: `52`, first-HTTP p50/p95/max
  `31ms` / `49ms` / `98ms`, total p50/p95/max `37ms` / `277ms` /
  `300ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `696ms` / `1045ms`.
- Bitswap session: `shortcut_starts=89`, `post_lookup_hits=38`,
  `post_lookup_timeouts=37`.

Experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace100-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-first-http-grace100-r3.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `857ms` / `886ms`; Kubo `1600ms` /
  `2173ms`; ratio `0.54x` / `0.41x`.
- Asset TTFB p50/p95: Rust `232ms` / `702ms`; Kubo `114ms` /
  `285ms`; ratio `2.04x` / `2.46x`.
- Max RSS/FD: Rust `53120KiB` / `30`; Kubo `187148KiB` / `95`.
- Delegated provider lookups: `101`, p50/p95/max `22ms` / `50ms` /
  `295ms`.
- Single-HTTP-provider delegated lookups: `57`, first-HTTP p50/p95/max
  `20ms` / `52ms` / `193ms`, total p50/p95/max `21ms` / `54ms` /
  `295ms`.
- HTTP-provider fetch p50/p95/max: `163ms` / `626ms` / `662ms`.
- Bitswap session work dropped sharply: `shortcut_starts=20`,
  `post_lookup_hits=13`, `post_lookup_timeouts=3`.

Secondary live checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-first-http-grace100-r3-trace.jsonl \
  --output /tmp/daicowtf-first-http-grace100-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-first-http-grace100-r3-trace.jsonl \
  --output /tmp/vitalik-first-http-grace100-r3.json
```

Secondary result:

- `daicowtf-page-assets` passed `3/3`; root TTFB p50/p95/max
  `1516ms` / `1563ms` / `1563ms`, max RSS/FD `42696KiB` / `17`,
  delegated lookup p50/p95/max `13ms` / `41ms` / `41ms`.
- `vitalik-root-html-range` passed `3/3`; range TTFB p50/p95/max
  `159ms` / `256ms` / `256ms`, max RSS/FD `31616KiB` / `13`,
  delegated lookup p50/p95/max `43ms` / `141ms` / `141ms`.

Final validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-gateway
cargo test -p freedom-ipfs-retrieval
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Final result:

- Formatting passed.
- Full gateway suite passed; all non-ignored unit/integration tests passed.
- Full retrieval suite passed: `72 passed`, `1 ignored`.
- Workspace all-target compile check passed.
- Workspace all-target clippy passed with warnings denied.

Decision:
Keep. The same-window `ipfs.tech` comparison improved Rust root p95, asset
p50/p95, delegated lookup p95, HTTP provider fetch p95/max, Bitswap session
pressure, RSS, and FD usage. The secondary page/range checks stayed reliable
and within mobile resource envelopes.

## 2026-05-05 Reject: Tighten Delegated First-HTTP Provider Grace To 50ms

Question:
After keeping `100ms`, test whether the same streamed delegated-routing grace
can be tightened further to `50ms`. In the accepted `100ms` run, target-met
multi-HTTP responses were usually available under about `44ms`, so `50ms`
looked plausible as a way to shave the remaining single-provider wait.

Prototype:

- Temporarily changed `STREAMING_DELEGATED_FIRST_HTTP_PROVIDER_GRACE` from
  `100ms` to `50ms`.
- Restored the committed `100ms` value after the live run regressed.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing streamed_delegated_response
```

Focused result:

- Formatting passed.
- Streamed delegated-response focused tests passed: `2 passed`.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-first-http-grace50-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-first-http-grace50-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1313ms` / `1375ms`; Kubo `2662ms` /
  `3733ms`; ratio `0.49x` / `0.37x`.
- Asset TTFB p50/p95: Rust `241ms` / `1007ms`; Kubo `218ms` /
  `978ms`; ratio `1.11x` / `1.03x`.
- Max RSS/FD: Rust `53644KiB` / `33`; Kubo `266924KiB` / `371`.
- Delegated provider lookups: `103`, p50/p95/max `29ms` / `307ms` /
  `762ms`.
- Single-HTTP-provider delegated lookups: `59`, first-HTTP p50/p95/max
  `35ms` / `284ms` / `723ms`, total p50/p95/max `37ms` / `286ms` /
  `723ms`.
- HTTP-provider fetch p50/p95/max: `158ms` / `687ms` / `947ms`.
- Bitswap session work stayed lower than the old `250ms` baseline but worse
  than the kept `100ms` run: `shortcut_starts=19`, `post_lookup_hits=7`,
  `post_lookup_timeouts=10`.

Comparison against kept `100ms`:

- Rust root p95 worsened from `886ms` to `1375ms`.
- Rust asset p95 worsened from `702ms` to `1007ms`.
- Delegated lookup p95 worsened from `50ms` to `307ms`.
- HTTP-provider fetch p95 worsened from `626ms` to `687ms`.
- RSS/FD worsened from `53120KiB` / `30` to `53644KiB` / `33`.

Decision:
Reject and keep `100ms`. The `50ms` prototype did not preserve the accepted
latency shape and caught a worse first-HTTP/delegated-routing tail. The next
delegated grace retune should not go below `100ms` without repeated evidence
that first-HTTP p95 and target-met p95 have moved lower in the same window.

## 2026-05-05 Reject: Race Bitswap Against Single HTTP Provider

Question:
After keeping the delegated first-HTTP-provider grace at `100ms`, single
HTTP-provider fetches still appeared to dominate some asset tails, especially
when the only candidate was `ipfs-bridge.sia.dev`. Test whether racing Bitswap
after a short delay only for exactly one usable HTTP provider can reduce those
tails without hurting mobile resource usage.

Prototype:

- Added a temporary `200ms` Bitswap race only when
  `fetch_from_providers_with_source` had exactly one usable HTTP provider.
- Kept multi-provider HTTP candidate behavior unchanged.
- Emitted temporary `single_http_bitswap_race` trace events.
- Added a deterministic focused test covering a slow single HTTP provider and
  a faster local Bitswap peer.
- Restored the committed HTTP-first single-provider behavior after the live run
  regressed.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval single_http_provider_races_bitswap_when_http_is_slow
```

Focused result:

- Formatting passed.
- The focused retrieval test passed.

Live comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-bitswap-race-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-single-http-bitswap-race-r3.json
```

Live result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1041ms` / `2099ms`; Kubo `1472ms` /
  `3021ms`; ratio `0.71x` / `0.69x`.
- Asset TTFB p50/p95: Rust `227ms` / `836ms`; Kubo `119ms` / `1471ms`;
  ratio `1.91x` / `0.57x`.
- Max RSS/FD: Rust `53664KiB` / `41`; Kubo `259532KiB` / `165`.
- Delegated provider lookups: `98`, p50/p95/max `20ms` / `60ms` / `159ms`.
- HTTP provider fetches: `48`, p50/p95/max `129ms` / `370ms` / `693ms`.
- HTTP provider host detail: `ipfs-bridge.sia.dev` p95 `646ms`;
  `dag.w3s.link` p95 `119ms`.
- Bitswap peer attempts jumped to `174`; Bitswap commands jumped to `93`.

Comparison against kept `100ms`:

- Rust root p95 worsened from `886ms` to `2099ms`.
- Rust asset p95 worsened from `702ms` to `836ms`.
- Max FD worsened from `30` to `41`.
- Bitswap peer attempts worsened from `35` to `174`.
- Bitswap commands worsened from `23` to `93`.
- HTTP-provider fetch p95 improved from `626ms` to `370ms`, but that did not
  translate into better end-to-end latency and came with substantially more
  Bitswap pressure.

Decision:
Reject. The broad single-provider Bitswap race added too much peer/session
work, increased FD pressure, and worsened the accepted absolute latency shape
even though HTTP-provider fetch p95 improved. Future single-provider mitigation
should avoid broad provider Bitswap racing. Prefer narrower approaches such as
using only trusted/session peers, requiring adaptive evidence before racing, or
improving single-provider HTTP host selection and suppression.

## 2026-05-05 Keep: Score HTTP Provider Origins In Race Scheduling

Question:
`ipfs.tech` still showed asset p50/p95 sensitivity to HTTP-provider host
choice. Prior traces consistently showed `dag.w3s.link` faster than
`ipfs-bridge.sia.dev`, while the retrieval path still scheduled HTTP-provider
candidates in delegated-routing order. Can a small in-memory provider score
prefer previously fast verified HTTP providers inside the existing bounded race
without adding fallback gateways, duplicate broad races, or resource pressure?

Implementation:

- Add a bounded in-memory HTTP-provider origin score map to `HttpRetriever`.
- Score only verified successful HTTP-provider fetches.
- Use a `10m` score TTL and cap the map at `64` origins.
- Sort scored origins ahead of unscored origins for future
  `fetch_from_http_provider_candidates` calls, preserving delegated order among
  ties and unscored providers.
- Keep `HTTP_PROVIDER_RACE_WIDTH=2`, `HTTP_PROVIDER_HEDGE_AFTER=250ms`, and
  the global HTTP-provider fetch semaphore unchanged.
- Emit `scored_provider_count`, `scoring_enabled`,
  `winner_original_provider_rank`, and `winner_provider_score_ms` on
  HTTP-provider race traces.
- Add an A/B kill switch:
  `FREEDOM_IPFS_DISABLE_HTTP_PROVIDER_SCORING=1`.
- Add a deterministic retrieval test proving a previously fast provider is
  moved into the initial race window before the hedge delay.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval prefers_scored_fast_http_provider_in_initial_race_width
cargo check -p freedom-ipfs-retrieval --all-targets
```

Focused result:

- Formatting passed.
- Focused retrieval scorer test passed.
- Retrieval crate all-target check passed.

Same-window disabled baseline:

```sh
FREEDOM_IPFS_DISABLE_HTTP_PROVIDER_SCORING=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-score-baseline-disabled-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-score-baseline-disabled-r3.json
```

Baseline result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1332ms` / `1406ms`; Kubo `2536ms` /
  `3226ms`; ratio `0.53x` / `0.44x`.
- Asset TTFB p50/p95: Rust `246ms` / `765ms`; Kubo `160ms` / `380ms`;
  ratio `1.54x` / `2.01x`.
- Max RSS/FD: Rust `52596KiB` / `31`; Kubo `383460KiB` / `791`.
- HTTP-provider races: `92`, `scored_events=0`, `scored_providers=0`.
- HTTP-provider fetch p50/p95/max: `163ms` / `644ms` / `911ms`.
- Provider detail: `ipfs-bridge.sia.dev` p95 `679ms`;
  `dag.w3s.link` p95 `130ms`.

Same-window scored experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-score-enabled-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-score-enabled-r3.json
```

Experiment result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1190ms` / `1377ms`; Kubo `2031ms` /
  `4602ms`; ratio `0.59x` / `0.30x`.
- Asset TTFB p50/p95: Rust `234ms` / `568ms`; Kubo `139ms` / `468ms`;
  ratio `1.68x` / `1.21x`.
- Max RSS/FD: Rust `46572KiB` / `27`; Kubo `307028KiB` / `347`.
- HTTP-provider races: `105`, `scored_events=99`, `scored_providers=99`,
  `max_scored=1`.
- HTTP-provider fetch p50/p95/max: `163ms` / `521ms` / `997ms`.
- Provider detail: `ipfs-bridge.sia.dev` p95 `664ms`;
  `dag.w3s.link` p95 `111ms`.

Comparison against disabled baseline:

- Rust root p50 improved from `1332ms` to `1190ms`; root p95 improved from
  `1406ms` to `1377ms`.
- Rust asset p50 improved from `246ms` to `234ms`; asset p95 improved from
  `765ms` to `568ms`.
- Rust RSS/FD improved from `52596KiB` / `31` to `46572KiB` / `27`.
- HTTP-provider fetch p95 improved from `644ms` to `521ms`.
- The scored run still stayed within the same bounded HTTP race width and had
  no Bitswap pressure increase in this page window.

Secondary live checks:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-http-score-enabled-r3-trace.jsonl \
  --output /tmp/daicowtf-http-score-enabled-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-http-score-enabled-r3-trace.jsonl \
  --output /tmp/vitalik-http-score-enabled-r3.json
```

Secondary result:

- `daicowtf-page-assets` passed `3/3`; root TTFB p50/p95/max
  `296ms` / `322ms` / `322ms`, max RSS/FD `33016KiB` / `14`,
  HTTP-provider fetch p50/p95/max `66ms` / `87ms` / `87ms`.
- `vitalik-root-html-range` passed `3/3`; range TTFB p50/p95/max
  `244ms` / `257ms` / `257ms`, max RSS/FD `31872KiB` / `13`,
  HTTP-provider fetch p50/p95/max `21ms` / `62ms` / `62ms`.

Final validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-gateway
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Final result:

- Formatting passed.
- Full retrieval suite passed: `73 passed`, `1 ignored`.
- Full gateway suite passed; all non-ignored unit/integration tests passed.
- Workspace all-target compile check passed.
- Workspace all-target clippy passed with warnings denied.
- Diff whitespace check passed.

Decision:
Keep. This is a bounded in-memory scheduling hint for delegated HTTP providers
only. It does not add public gateway fallback, does not change block
verification, does not increase race width or fetch concurrency, and improved
same-window `ipfs.tech` root and asset latency while preserving low RSS/FD in
secondary live checks. Keep watching `scored_provider_count`, provider p95, and
winner original rank in future provider-quality runs.

## 2026-05-05 Keep: Summarize HTTP Provider Score Winners

Question:
The kept HTTP-provider origin scorer emits `winner_original_provider_rank` and
score fields, but the harness only summarized how many race events had any
scored providers. Future provider-quality runs need to know whether the
selected winner was scored and whether scoring moved a provider from a later
delegated rank into the scheduled race window.

Implementation:

- Add `winner_provider_scored` and `provider_scored` booleans to retrieval
  race/hedge trace events.
- Extend the harness HTTP-provider race aggregate with:
  - scored winner count
  - scored winner score latency summary
  - winner original-rank buckets
  - max winner original rank
- Print a compact `provider scoring` line in trace summaries.
- Update the existing HTTP-provider trace summary test with scored winner and
  original-rank fixture data.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo check -p freedom-ipfs-retrieval --all-targets
cargo check -p mobile-web-harness --all-targets
cargo test -p mobile-web-harness
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Result:

- Formatting passed.
- Focused HTTP-provider trace summary test passed.
- Retrieval crate all-target check passed.
- Mobile web harness all-target check passed.
- Full mobile web harness suite passed: `38 passed`.
- Workspace clippy passed with warnings denied.
- Diff whitespace check passed.

Decision:
Keep. This is diagnostics-only and does not change provider policy, fetch
concurrency, fallback behavior, block verification, or caching. It makes the
kept scorer measurable from normal harness output instead of requiring manual
JSONL inspection.

## 2026-05-05 Reject: Add `cid.contact` To Default Delegated Routers

Question:
Priority 4 in the long-running roadmap calls out delegated router comparison.
Recent `ipfs.tech` traces still occasionally show delegated lookup p95/max
tails, so test whether using the existing multi-endpoint delegated-routing mode
with both `delegated-ipfs.dev` and `cid.contact` improves page latency enough
to justify adding a second default delegated router.

Experiment:

- No code change.
- Run the spawned gateway with:
  `--delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1`.
- Compare against an immediate default-router rerun in the same network window.
- Keep the node read-only and keep block verification/caching unchanged.

Context baseline with scoring-winner summary:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-score-winner-summary-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-http-score-winner-summary-r3.json
```

Context result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1465ms` / `1536ms`; Kubo `2516ms` /
  `2556ms`.
- Asset TTFB p50/p95: Rust `263ms` / `917ms`; Kubo `138ms` / `656ms`.
- Rust max RSS/FD: `46848KiB` / `27`.
- Delegated lookup p50/p95/max: `20ms` / `280ms` / `745ms`.
- Provider scoring summary: `winner_scored=99`, original ranks
  `rank1=102`, `rank2=3`, `rank3_plus=0`.

Two-endpoint run:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --delegated-router https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1 \
  --trace-output /tmp/ipfs-tech-router-delegated-plus-cidcontact-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-delegated-plus-cidcontact-r3.json
```

Two-endpoint result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `739ms` / `849ms` / `849ms`.
- Asset TTFB p50/p95/max: `252ms` / `754ms` / `1288ms`.
- Run total p50/p95/max: `2205ms` / `3230ms` / `3230ms`.
- Max RSS/FD: `51840KiB` / `26`.
- Delegated lookup p50/p95/max: `22ms` / `55ms` / `69ms`.
- Endpoint summary only showed `https://delegated-ipfs.dev/routing/v1`;
  `cid.contact` did not contribute before the multi-endpoint client returned.
- HTTP-provider fetch p50/p95/max: `158ms` / `628ms` / `872ms`.

Immediate default-router rerun:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-router-default-rerun-after-cidcontact-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-default-rerun-after-cidcontact-r3.json
```

Default rerun result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `718ms` / `820ms` / `820ms`.
- Asset TTFB p50/p95/max: `233ms` / `547ms` / `1041ms`.
- Run total p50/p95/max: `2248ms` / `2255ms` / `2255ms`.
- Max RSS/FD: `46592KiB` / `27`.
- Delegated lookup p50/p95/max: `21ms` / `50ms` / `54ms`.
- HTTP-provider fetch p50/p95/max: `158ms` / `278ms` / `668ms`.

Comparison:

- The two-endpoint run did not show `cid.contact` contribution in the endpoint
  summary.
- The default rerun was slightly better on root p50/p95, asset p50/p95/max,
  delegated lookup p95/max, HTTP-provider fetch p95/max, RSS, and run p95.
- Both runs stayed reliable, and the endpoint addition did not cause an obvious
  failure, but it also did not produce evidence that a second default endpoint
  helps this workload.

Decision:
Reject adding `cid.contact` to the default delegated-router list for now. The
existing multi-endpoint mode remains useful for manual experiments, but current
same-window evidence does not justify extra default router traffic or a default
configuration change. Revisit only with cases where `delegated-ipfs.dev` has
actual failures/empty responses and the endpoint summary proves another router
returns useful providers before the client deadline.

Follow-up `cid.contact`-only check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --delegated-router https://cid.contact/routing/v1 \
  --trace-output /tmp/ipfs-tech-router-cidcontact-only-r3-trace.jsonl \
  --output /tmp/ipfs-tech-router-cidcontact-only-r3.json
```

Result:

- Process exited `1` because the harness found failures.
- Rust failed `0/3`.
- Root TTFB p50/p95/max: `3346ms` / `4593ms` / `4593ms`.
- Run total p50/p95/max: `3347ms` / `4594ms` / `4594ms`.
- Max RSS/FD: `46976KiB` / `30`.
- All three requests returned status `502`; bodies were only `328` bytes, no
  assets were fetched, and the body did not contain `<title>IPFS`.
- Delegated provider lookups: `3` events, `0` successes, `3` failures, `0`
  providers, `0` HTTP providers; elapsed p50/p95/max was `25ms` / `1270ms` /
  `1270ms`.
- Endpoint summary for `https://cid.contact/routing/v1`: `events=3`,
  `successes=0`, `failures=3`, `http_zero=3`, no providers.
- Trace errors were three `404 Not Found` responses for
  `https://cid.contact/routing/v1/providers/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq`
  and three `provider_refresh_skipped_empty_provider_set` events with
  `no HTTP-capable providers found`.
- DHT fallback had `3` successful lookup attempts but returned `0` providers;
  DHT lookup max was `3261ms`.
- Late peer waits: `3` waits, `3` misses, p50/p95/max all `2001ms`.

Conclusion:
This strengthens the rejection. `cid.contact` should not be a default router
and should not be used alone for this workload. The URL shape may be
incompatible with this CID/routing endpoint, or `cid.contact` may not serve this
content; either way, it is not a usable default for the mobile read path.

## 2026-05-05 Reject: Skip Slow Single HTTP Provider Host

Question:
Current `ipfs.tech` runs still show the asset tail concentrated in
single-HTTP-provider races where the only HTTP provider is
`https://ipfs-bridge.sia.dev/`. Multi-provider races usually select
`https://dag.w3s.link/` quickly, but the single-provider CIDs have no HTTP
alternative in the current delegated response.

Direct delegated-response probe:

- Queried the top slow single-provider CIDs from
  `/tmp/ipfs-tech-router-default-rerun-after-cidcontact-r3-trace.jsonl`
  directly against `https://delegated-ipfs.dev/routing/v1/providers/<cid>`.
- Used `accept: application/x-ndjson, application/json` and a non-empty
  `user-agent`.
- The five checked CIDs returned `12-35` provider records, but exactly one HTTP
  provider each: `12D3KooWKosAkdeGoRQVT5cAGRcFvBexq3q4joZG4amDWhxZzt2p`
  with `/dns4/ipfs-bridge.sia.dev/tcp/443/https`.
- Conclusion from the probe: longer delegated-routing patience will not reveal
  hidden `dag.w3s.link` diversity for these CIDs; any mitigation needs to use
  Bitswap or a different provider source.

Prototype:

- Temporarily added an opt-in `FREEDOM_IPFS_SKIP_HTTP_PROVIDER_HOSTS` retrieval
  experiment knob.
- With `FREEDOM_IPFS_SKIP_HTTP_PROVIDER_HOSTS=ipfs-bridge.sia.dev`, retrieval
  skipped only that HTTP host and fell through to existing verified Bitswap for
  affected CIDs.
- No public gateway fallback was added, and block verification/caching stayed on
  the existing paths.
- The prototype and focused skip-list test were reverted after the measurement.

Focused validation while the prototype was present:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval http_provider_skip_list_matches_host_or_origin
cargo check -p freedom-ipfs-retrieval --all-targets
```

Focused result:

- Formatting passed.
- Focused retrieval skip-list test passed.
- Retrieval all-target check passed.

Same-window baseline:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-skip-host-baseline-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-skip-host-baseline-r3.json
```

Baseline result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `829ms` / `1439ms` / `1439ms`.
- Asset TTFB p50/p95/max: `252ms` / `935ms` / `1574ms`.
- Run total p50/p95/max: `2759ms` / `4442ms` / `4442ms`.
- Max RSS/FD: `47584KiB` / `26`.
- Block sources: `http_provider=120`.
- Delegated lookup p50/p95/max: `25ms` / `62ms` / `1400ms`.
- HTTP-provider fetch p50/p95/max: `159ms` / `660ms` / `933ms`.
- Single-provider HTTP winners: `63`, all `https://ipfs-bridge.sia.dev/`,
  p50/p95/max `240ms` / `694ms` / `933ms`.
- Multi-provider winner p50/p95/max: `99ms` / `279ms` / `399ms`.

Host-skip experiment:

```sh
FREEDOM_IPFS_SKIP_HTTP_PROVIDER_HOSTS=ipfs-bridge.sia.dev \
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-http-skip-ipfs-bridge-r3-trace.jsonl \
  --output /tmp/ipfs-tech-http-skip-ipfs-bridge-r3.json
```

Experiment result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max worsened to `2652ms` / `2835ms` / `2835ms`.
- Asset TTFB p50 improved but p95/max worsened: `173ms` / `1069ms` /
  `1336ms`.
- Run total p50/p95/max worsened to `4058ms` / `5270ms` / `5270ms`.
- Max RSS/FD worsened to `54340KiB` / `39`.
- Block sources shifted to `bitswap=100`, `http_provider=19`.
- HTTP-provider fetches were only from `https://dag.w3s.link/`, with
  p50/p95/max `48ms` / `118ms` / `118ms`.
- Bitswap fetch p50/p95/max: `219ms` / `1717ms` / `1989ms`.
- Bitswap commands: `122`; incoming deliveries: `86`; extra blocks: `76`.
- Bitswap connection establishment p50/p95/max: `326ms` / `546ms` / `546ms`.
- Block-store puts increased from `105` to `181`, and put bytes increased from
  `2381466` to `2898210`.

Decision:
Reject and revert. The measurement confirms that blindly suppressing
`ipfs-bridge.sia.dev` is not a safe policy even though that host is the slow
single HTTP provider in this workload. Verified Bitswap recovered reliability
but increased root latency, p95 asset latency, total time, RSS, FD count,
connection work, and cache-write pressure. Future work should avoid static
HTTP-host suppression. More promising directions are adaptive per-CID/session
decisions with strict resource caps, or improving Bitswap connection/session
latency before using Bitswap as a broad substitute for slow single HTTP
providers.

## 2026-05-05 Keep: Summarize Block Fetch Total Latency By Source

Motivation:
The rejected host-skip experiment showed that source counts alone are not
enough. We need a compact way to see whether `cache`, `http_provider`, or
`bitswap` is contributing the block-fetch latency tail before trying adaptive
transport policy changes.

Change:
The mobile web harness trace summary now aggregates `block_fetch_total`
`elapsed_ms` by `source`. The JSON report includes
`trace_summary.block_fetch_source_latencies`, and console summaries print
per-source count, total time, and p50/p90/p95/max latency. This is
diagnostics-only and does not change retrieval behavior.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- Formatting passed.
- Focused trace-summary parser test passed.
- Harness all-target check passed.
- Harness clippy passed with `-D warnings`.

Live sanity run:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-block-fetch-source-latency-r1-trace.jsonl \
  --output /tmp/ipfs-tech-block-fetch-source-latency-r1.json
```

Result:

- Rust passed `1/1`.
- Root TTFB/total: `1489ms` / `1490ms`.
- Asset TTFB p50/p90/p95/max: `216ms` / `975ms` / `1915ms` / `2214ms`.
- Run total: `4816ms`.
- Max RSS/FD: `46564KiB` / `26`.
- Block sources: `http_provider=40`.
- New block-fetch source latency summary:
  `http_provider: count=40 total=14954ms elapsed=p50=192ms p90=764ms p95=953ms max=2209ms`.

Conclusion:
Keep. This gives future iterations a cheap first-pass signal for whether a
candidate optimization is moving latency between sources or actually reducing
the block-fetch tail. It should be used alongside provider-race, Bitswap, cache,
RSS, and FD summaries before keeping any adaptive HTTP/Bitswap policy.

## 2026-05-05 Baseline: Current Rust vs Kubo With Source-Latency Summary

Question:
After the HTTP-provider scoring work and the new block-fetch source latency
summary, where is the current Rust gateway still behind Kubo on the focused
`ipfs.tech` page workload?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-current-kubo-comparison-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-current-kubo-comparison-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1309ms` / `1371ms`; Kubo `2320ms` /
  `3092ms`; Rust ratio `0.56x` / `0.44x`.
- Asset TTFB p50/p95: Rust `251ms` / `929ms`; Kubo `146ms` / `1271ms`;
  Rust ratio `1.72x` / `0.73x`.
- Max RSS/FD: Rust `46764KiB` / `28`; Kubo `249520KiB` / `179`.
- Rust block sources: `http_provider=120`.
- Rust block-fetch source latency:
  `http_provider: count=120 total=32551ms elapsed=p50=211ms p90=558ms p95=691ms max=999ms`.
- Delegated lookup p50/p95/max: `22ms` / `50ms` / `97ms`.
- HTTP-provider fetch p50/p95/max: `161ms` / `644ms` / `923ms`.
- Single-provider HTTP winners: `63`, all `https://ipfs-bridge.sia.dev/`,
  p50/p95/max `292ms` / `693ms` / `948ms`.
- Multi-provider HTTP winner p50/p95/max: `91ms` / `191ms` / `217ms`.
- Provider detail:
  `https://ipfs-bridge.sia.dev/` p50/p95/max `168ms` / `673ms` / `923ms`;
  `https://dag.w3s.link/` p50/p95/max `43ms` / `90ms` / `94ms`;
  `https://calib2.ezpdpz.net/` p50/p95/max `30ms` / `88ms` / `88ms`.

Conclusion:
This window says Rust is already meaningfully ahead of Kubo for root TTFB and
asset p95 while using far less RSS and far fewer file descriptors. Kubo still
wins asset p50, and the Rust tail is now clearly an HTTP-provider source tail,
not Bitswap or delegated lookup. The next promising target remains narrow
single-provider HTTP mitigation, especially when the only HTTP provider is
`ipfs-bridge.sia.dev`, but previous static host suppression and broad Bitswap
substitution were rejected on resource and latency grounds.

## 2026-05-05 Keep: Queue Bursty Gateway Requests Under The Existing Cap

Question:
The gateway enforced `DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS=8` with
`try_acquire_owned()`, so the ninth concurrent browser request failed
immediately with `503 Service Unavailable`. That is resource-safe, but brittle
for real WebKit page fan-out where short bursts can exceed the active work cap.

Implementation:

- Keep the active gateway request cap unchanged.
- Replace immediate limiter failure with a bounded wait for a semaphore permit.
- Use `GATEWAY_REQUEST_QUEUE_TIMEOUT=2s`.
- Preserve `503 Service Unavailable` when a request cannot acquire a permit
  within that bounded queue window.
- Keep existing `gateway_limiter` tracing and add `timeout_ms` so queued wait
  time is visible in progress/harness summaries.
- Extend the harness trace summary with a dedicated `gateway limiter` line for
  acquired/denied counts, limiter wait latency, denied wait latency, and max
  timeout.
- Add deterministic tests proving short bursts queue and succeed while
  sustained saturation still fails.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-gateway concurrency
cargo check -p freedom-ipfs-gateway --all-targets
cargo clippy -p freedom-ipfs-gateway --all-targets -- -D warnings
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- Formatting passed.
- Focused gateway concurrency tests passed: `2 passed`.
- Gateway all-target check passed.
- Gateway clippy passed with `-D warnings`.
- Full gateway suite passed: `30` lib tests, `4` main tests, CLI test, and
  non-ignored integration tests passed.
- Focused harness trace-summary test passed.
- Harness clippy passed with `-D warnings`.

Normal page fan-out check:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-gateway-queue-asset6-r3-trace.jsonl \
  --output /tmp/ipfs-tech-gateway-queue-asset6-r3.json
```

Normal result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `887ms` / `979ms` / `979ms`.
- Asset TTFB p50/p95/max: `246ms` / `630ms` / `1200ms`.
- Run total p50/p95/max: `2454ms` / `2969ms` / `2969ms`.
- Max RSS/FD: `54136KiB` / `40`.
- Gateway limiter denials: `0`.
- Gateway response elapsed p50/p95/max: `249ms` / `745ms` / `1196ms`.
- Block sources: `http_provider=81`, `bitswap=39`.

Burst stress check above the old harness-safe asset fan-out:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 12 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-gateway-queue-asset12-r3-trace.jsonl \
  --output /tmp/ipfs-tech-gateway-queue-asset12-r3.json
```

Burst result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1452ms` / `1530ms` / `1530ms`.
- Asset TTFB p50/p95/max: `526ms` / `1001ms` / `1343ms`.
- Run total p50/p95/max: `3576ms` / `3639ms` / `3639ms`.
- Max RSS/FD: `55760KiB` / `46`.
- Gateway limiter denials: `0`.
- Gateway limiter wait p50/p95/max: `114ms` / `438ms` / `681ms`.
- Block sources: `http_provider=117`, `bitswap=9`.

Conclusion:
Keep. This is not a high-fanout speed optimization; the stress run correctly
shows queue latency and somewhat higher RSS/FD when the page loader asks for
more concurrent work than the gateway will actively run. The important behavior
change is that browser bursts no longer drop subresources immediately while the
gateway still enforces the same active request cap and still fails sustained
saturation after a bounded wait. That is a better mobile browser default than
turning transient fan-out into page-level `503` failures.

## 2026-05-05 Keep: Self-Hedge Slow Single HTTP Provider Requests

Question:
The remaining HTTP-provider tail is concentrated in single-provider races where
the only HTTP provider is often `https://ipfs-bridge.sia.dev/`. Static host
suppression and broad Bitswap substitution were rejected. Can a narrower
duplicate request to the same HTTP provider trim that tail without increasing
active gateway concurrency or adding public gateway fallback?

Implementation:

- When there is exactly one HTTP-provider candidate, start the normal verified
  HTTP raw-block fetch.
- If it is still pending after `350ms`, start one duplicate request to the same
  provider.
- Both requests use the existing global `MAX_CONCURRENT_HTTP_PROVIDER_FETCHES=4`
  semaphore.
- The first verified block wins; dropped futures cancel the slower duplicate
  where the HTTP stack allows cancellation.
- Block verification and cache insertion stay on the existing path.
- Add kill switch:
  `FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1`.
- Emit `http_provider_self_hedge` trace events.
- Extend the harness HTTP-provider race summary with self-hedge count and max
  self-hedge timeout.
- Add a deterministic local test where the second same-provider request returns
  before the first slow request.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval self_hedges_slow_single_http_provider
cargo check -p freedom-ipfs-retrieval --all-targets
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
git diff --check
```

Focused result:

- Formatting passed.
- Focused retrieval self-hedge test passed.
- Retrieval all-target check passed.
- Retrieval clippy passed with `-D warnings`.
- Focused harness HTTP-provider trace summary test passed.
- Harness all-target check passed.
- Harness clippy passed with `-D warnings`.
- Diff whitespace check passed.

Same-window disabled baseline:

```sh
FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-self-hedge-disabled-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-self-hedge-disabled-r3.json
```

Disabled result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1551ms` / `1826ms` / `1826ms`.
- Asset TTFB p50/p95/max: `238ms` / `968ms` / `1608ms`.
- Run total p50/p95/max: `3812ms` / `3963ms` / `3963ms`.
- Max RSS/FD: `53536KiB` / `36`.
- Block sources: `http_provider=74`, `bitswap=46`.
- HTTP-provider fetch p50/p95/max: `157ms` / `734ms` / `1040ms`.
- Single-provider HTTP winners: `30`, all `https://ipfs-bridge.sia.dev/`,
  p50/p95/max `347ms` / `869ms` / `1041ms`.
- `https://ipfs-bridge.sia.dev/` fetch p50/p95/max:
  `231ms` / `869ms` / `1040ms`.
- Self-hedge events: `0`.

Same-window enabled experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-self-hedge-enabled-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-self-hedge-enabled-r3.json
```

Enabled result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max improved to `926ms` / `955ms` / `955ms`.
- Asset TTFB p50/p95/max: `253ms` / `933ms` / `1637ms`.
- Run total p50/p95/max improved to `3029ms` / `3246ms` / `3246ms`.
- Max RSS/FD stayed comparable at `53524KiB` / `36`.
- Block sources shifted to `http_provider=113`, `bitswap=7`.
- HTTP-provider fetch p50/p95/max improved to `163ms` / `537ms` / `721ms`.
- Single-provider HTTP winners: `56`, all `https://ipfs-bridge.sia.dev/`,
  p50/p95/max improved to `207ms` / `698ms` / `747ms`.
- `https://ipfs-bridge.sia.dev/` fetch p50/p95/max improved to
  `186ms` / `572ms` / `721ms`.
- Self-hedge events: `12`.

Comparison:

- Root p95 improved from `1826ms` to `955ms`.
- Run-total p95 improved from `3963ms` to `3246ms`.
- HTTP-provider fetch p95 improved from `734ms` to `537ms`.
- Single-provider winner p95 improved from `869ms` to `698ms`.
- `ipfs-bridge.sia.dev` fetch p95 improved from `869ms` to `572ms`.
- Asset p95 improved slightly from `968ms` to `933ms`; asset max worsened
  slightly from `1608ms` to `1637ms`, but the enabled max was dominated by a
  separate slow Bitswap block for `ZT0_SuSb.js`, not by the targeted HTTP
  provider path.

Decision:
Keep. This is a narrow same-provider hedge, not public gateway fallback and not
provider suppression. It uses the existing verified raw-block path and the
existing global HTTP-provider concurrency cap, and it directly improved the
single-provider `ipfs-bridge.sia.dev` tail in the same-window A/B run. Continue
watching self-hedge counts, HTTP-provider bytes, and FD/RSS in future runs.

## 2026-05-05 Baseline: Self-Hedge Rust vs Kubo Refresh

Question:
After adding the narrow single-provider HTTP self-hedge, where does the current
Rust gateway stand against Kubo on the focused `ipfs.tech` page workload?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-kubo-comparison-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-self-hedge-kubo-comparison-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1024ms` / `1852ms`; Kubo `1800ms` /
  `1811ms`; Rust ratio `0.57x` / `1.02x`.
- Asset TTFB p50/p95: Rust `217ms` / `964ms`; Kubo `118ms` / `1919ms`;
  Rust ratio `1.84x` / `0.50x`.
- Max RSS/FD: Rust `53656KiB` / `39`; Kubo `291564KiB` / `385`.
- Rust block-fetch sources:
  - `http_provider`: count `50`, total `21502ms`, p50/p90/p95/max
    `235ms` / `713ms` / `948ms` / `5284ms`.
  - `bitswap`: count `67`, total `12148ms`, p50/p90/p95/max
    `132ms` / `276ms` / `353ms` / `1362ms`.
  - `cache`: count `1`, total `187ms`.
- Delegated provider lookup: events `97`, successes `97`, failures `0`,
  p50/p90/p95/max `25ms` / `53ms` / `278ms` / `5212ms`.
- HTTP-provider races: events `45`, single `20`, multi `25`, successes `45`,
  self-hedges `7`, result max `765ms`.
- HTTP-provider fetches: p50/p90/p95/max `103ms` / `619ms` / `642ms` /
  `765ms`.
- Single-provider HTTP winners: `20`, all `https://ipfs-bridge.sia.dev/`,
  p50/p90/p95/max `248ms` / `660ms` / `681ms` / `765ms`.
- Provider detail:
  `https://ipfs-bridge.sia.dev/` p50/p90/p95/max
  `189ms` / `642ms` / `671ms` / `765ms`;
  `https://dag.w3s.link/` p50/p90/p95/max
  `49ms` / `103ms` / `114ms` / `190ms`.
- Slowest Rust request:
  `/ipns/ipfs.tech/_nuxt/community-hero.Cp0BCcC7.jpg` range request,
  `5287ms` total. Its slow block was
  `bafkreiam77queskklq2cjhaoywvlxvasghy4ydr77gmzagjioporv6xsy4`, with
  `delegated_provider_lookup` taking `5212ms` before an HTTP-provider fetch
  completed quickly enough to keep the HTTP-provider race result max at
  `765ms`.

Conclusion:
The self-hedge path remains resource-friendly and visible: Rust still uses much
less RSS/FD than Kubo, wins asset p95 by roughly half, and keeps
single-provider `ipfs-bridge.sia.dev` fetch p95 under `700ms` in this window.
The current root p95 miss against Kubo is not caused by the self-hedged HTTP
fetch path; it is dominated by one delegated-router response that took `5.2s`
to return headers/first chunk/first HTTP provider for a sparse hero-image
block. The next promising target is therefore a narrow delegated-routing tail
mitigation, such as bounded endpoint hedging or earlier fallback when
`delegated-ipfs.dev` stalls before the first useful provider, while preserving
the no-public-gateway-fallback and mobile resource constraints.

## 2026-05-05 Keep: Self-Hedge Slow Single Delegated Router Requests

Question:
The self-hedge Kubo comparison exposed one `5.2s` delegated-router response
before the first useful provider for the `ipfs.tech` hero image range request.
Can a narrow same-endpoint duplicate delegated-routing request guard against
that rare tail without adding public gateway fallback or starting more DHT work?

Implementation:

- For the default single delegated router endpoint, start the normal provider
  lookup.
- If it has not completed after `750ms`, issue one duplicate lookup to the same
  delegated router endpoint.
- The first non-error response wins.
- Empty responses preserve the existing fast empty-result path into the current
  empty-delegated retry logic.
- Existing delegated response parsing, byte caps, provider verification by the
  retrieval layer, and low-diversity DHT fallback behavior are unchanged.
- Add kill switch:
  `FREEDOM_IPFS_DISABLE_SINGLE_DELEGATED_SELF_HEDGE=1`.
- Emit `delegated_provider_self_hedge` only when the duplicate lookup actually
  starts; normal fast lookups do not emit extra result events.
- Extend the harness delegated provider summary with self-hedge counts and max
  timeout.
- Add a deterministic local routing test where the second same-endpoint request
  returns before the first slow response.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-routing delegated_routing_self_hedges_slow_single_endpoint
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases
cargo test -p freedom-ipfs-routing
cargo check -p freedom-ipfs-routing --all-targets
cargo clippy -p freedom-ipfs-routing --all-targets -- -D warnings
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
git diff --check
```

Focused result:

- Formatting passed.
- Focused deterministic delegated self-hedge test passed.
- Focused harness progress summary test passed.
- Full routing suite passed: `25` passed, `1` ignored.
- Routing all-target check passed.
- Routing clippy passed with `-D warnings`.
- Full harness suite passed: `38` passed.
- Harness all-target check passed.
- Harness clippy passed with `-D warnings`.
- Diff whitespace check passed.

Same-window disabled baseline:

```sh
FREEDOM_IPFS_DISABLE_SINGLE_DELEGATED_SELF_HEDGE=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-self-hedge-disabled-r3-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-self-hedge-disabled-r3.json
```

Disabled result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1382ms` / `1457ms` / `1457ms`.
- Asset TTFB p50/p95/max: `196ms` / `715ms` / `869ms`.
- Run total p50/p95/max: `2834ms` / `3198ms` / `3198ms`.
- Max RSS/FD: `54852KiB` / `34`.
- Delegated provider lookup p50/p90/p95/max:
  `23ms` / `48ms` / `64ms` / `98ms`.
- Delegated self-hedges: `0`.

Same-window enabled experiment before suppressing no-op result traces:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-self-hedge-enabled-r3-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-self-hedge-enabled-r3.json
```

Enabled result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `1164ms` / `1734ms` / `1734ms`.
- Asset TTFB p50/p95/max: `244ms` / `767ms` / `991ms`.
- Run total p50/p95/max: `3365ms` / `3466ms` / `3466ms`.
- Max RSS/FD: `53460KiB` / `37`.
- Delegated provider lookup p50/p90/p95/max:
  `26ms` / `47ms` / `48ms` / `87ms`.
- Delegated self-hedges: `0`.
- This run confirmed the production threshold did not fire in the same-window
  sample. It also exposed that the first implementation logged
  `delegated_provider_self_hedge_result` for every normal lookup; that no-op
  trace was removed before keeping the change.

Post-fix no-op trace smoke:

```sh
timeout 420s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 1 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-self-hedge-enabled-r1-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-self-hedge-enabled-r1.json
```

Post-fix result:

- Rust passed `1/1`.
- Root TTFB/total: `1022ms` / `1023ms`.
- Asset TTFB p50/p95/max: `220ms` / `656ms` / `681ms`.
- Run total: `2617ms`.
- Max RSS/FD: `51828KiB` / `29`.
- Delegated provider lookup p50/p90/p95/max:
  `21ms` / `49ms` / `53ms` / `89ms`.
- Delegated self-hedges: `0`.
- No no-op `delegated_provider_self_hedge_result` events were emitted.

Decision:
Keep, but treat it as a rare-tail guard rather than a proven broad latency win.
The deterministic test proves the exact mechanism, and the prior comparison
showed the real failure shape this targets: a single delegated-router request
stalled for `5.2s` before any useful provider. The same-window live runs did not
hit that tail, so they cannot prove a p50/p95 improvement. The kept behavior is
bounded to one duplicate delegated lookup after `750ms`, has a kill switch, does
not add public gateway fallback, does not start more DHT work, and now has no
extra trace/progress events when the hedge does not fire. Future comparisons
should watch `delegated provider lookup self_hedges`, RSS/FD, and slow
delegated response milestones; revert if self-hedges become frequent without
reducing request tails.

## 2026-05-05 Guardrail: DAICO Sparse-Provider Page After Self-Hedges

Question:
Do the HTTP-provider and delegated-router self-hedge changes preserve the
current fast sparse-provider behavior on `daicowtf-page-assets`?

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-post-self-hedges-r3-trace.jsonl \
  --output /tmp/daicowtf-post-self-hedges-r3.json
```

Result:

- Rust passed `3/3`.
- Root TTFB p50/p95/max: `291ms` / `343ms` / `343ms`.
- Root total p50/p95/max: `294ms` / `345ms` / `345ms`.
- Run total p50/p95/max: `308ms` / `358ms` / `358ms`.
- Max RSS/FD: `33384KiB` / `14`.
- Block sources: `http_provider=9`.
- Delegated provider lookup p50/p90/p95/max:
  `14ms` / `108ms` / `108ms` / `108ms`.
- Delegated self-hedges: `0`.
- HTTP-provider races: `9` single-provider wins, all
  `https://gateway-v3.pinata.cloud/`.
- HTTP-provider fetch p50/p90/p95/max:
  `58ms` / `88ms` / `88ms` / `88ms`.
- HTTP-provider self-hedges: `0`.

Decision:
Keep as guardrail evidence. The sparse-provider DAICO page remains fast and
resource-light after the self-hedge changes. Neither hedge fired in this case,
which is the desired behavior for already-fast delegated and HTTP-provider
paths.

## 2026-05-05 Guardrail: Vitalik Root HTML Range After Self-Hedges

Question:
Do the self-hedge changes preserve the current fast HTTP range behavior on the
`vitalik-root-html-range` case?

Command:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-root-html-range-post-self-hedges-r3-trace.jsonl \
  --output /tmp/vitalik-root-html-range-post-self-hedges-r3.json
```

Result:

- Rust passed `3/3`.
- Root/range TTFB p50/p95/max: `250ms` / `257ms` / `257ms`.
- Root/range total p50/p95/max: `250ms` / `258ms` / `258ms`.
- Run total p50/p95/max: `250ms` / `258ms` / `258ms`.
- Max RSS/FD: `32128KiB` / `14`.
- Gateway statuses: `206=3`.
- Block sources: `http_provider=6`.
- Delegated provider lookup p50/p90/p95/max:
  `38ms` / `165ms` / `165ms` / `165ms`.
- Delegated self-hedges: `0`.
- HTTP-provider races: `6` successes; single-provider winners used
  `https://trustless.filebase.io/`.
- HTTP-provider fetch p50/p90/p95/max:
  `20ms` / `40ms` / `40ms` / `40ms`.
- HTTP-provider self-hedges: `0`.

Decision:
Keep as range guardrail evidence. The range path stays fast and resource-light,
and neither self-hedge fires on already-fast delegated or HTTP-provider
lookups.

## 2026-05-05 Baseline: DAICO And Vitalik Post-Self-Hedge Kubo Comparison

Question:
After the HTTP-provider and delegated-router self-hedge changes, where do the
two lightweight guardrail cases stand against Kubo?

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daico-vitalik-post-self-hedges-kubo-r3-trace.jsonl \
  --comparison-output /tmp/daico-vitalik-post-self-hedges-kubo-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- `daicowtf-page-assets`:
  - Root TTFB p50/p95: Rust `289ms` / `320ms`; Kubo `2366ms` /
    `3557ms`.
  - Rust ratio p50/p95: `0.12x` / `0.09x`.
- `vitalik-root-html-range`:
  - Root/range TTFB p50/p95: Rust `99ms` / `119ms`; Kubo `1819ms` /
    `2672ms`.
  - Rust ratio p50/p95: `0.05x` / `0.04x`.
- Shared resource maxima across the paired run:
  - Rust max RSS/FD: `34440KiB` / `14`.
  - Kubo max RSS/FD: `149108KiB` / `110`.
  - Rust/Kubo resource ratios: RSS `0.23x`, FD `0.13x`.
- Rust block sources: `http_provider=15`.
- Rust delegated provider lookup p50/p90/p95/max:
  `14ms` / `43ms` / `48ms` / `48ms`.
- Rust delegated self-hedges: `0`.
- Rust HTTP-provider fetch p50/p90/p95/max:
  `48ms` / `85ms` / `94ms` / `94ms`.
- Rust HTTP-provider self-hedges: `0`.

Conclusion:
Current Rust is meaningfully faster than Kubo on these two small guardrail
cases while using much less RSS and far fewer file descriptors. Both
self-hedge mechanisms stay idle on these already-fast paths, which supports
keeping them as bounded rare-tail guards rather than active steady-state work.

## 2026-05-05 Baseline: Warm Same-Daemon `ipfs.tech` vs Kubo

Question:
After one warmup page load against the same daemon, how close is Rust to Kubo on
the `ipfs.tech` page workload?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-warm-same-daemon-post-self-hedges-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-warm-same-daemon-post-self-hedges-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `4ms` / `4ms`; Kubo `2ms` / `2ms`.
- Asset TTFB p50/p95: Rust `3ms` / `6ms`; Kubo `2ms` / `4ms`.
- Rust/Kubo latency ratios: root p50/p95 `2.00x` / `2.00x`; asset p50/p95
  `1.50x` / `1.50x`.
- Max RSS/FD: Rust `52848KiB` / `30`; Kubo `191044KiB` / `92`.
- Rust resource ratios: RSS `0.28x`, FD `0.33x`.
- The Rust trace includes the warmup pass, so its slow request/event summaries
  show the initial cold fill. The measured passes after warmup are the
  millisecond-scale results above.
- During warmup, Rust block-fetch sources were:
  `http_provider=31`, `bitswap=8`, `cache=1`.
- Delegated lookup p50/p90/p95/max during the traced warmup/measured window:
  `34ms` / `174ms` / `424ms` / `541ms`.
- HTTP-provider fetch p50/p90/p95/max:
  `105ms` / `452ms` / `540ms` / `664ms`.

Conclusion:
Warm same-daemon Rust is now effectively hot-cache fast, though Kubo still wins
by a couple of milliseconds on this synthetic local benchmark. The remaining
warm gap is not worth aggressive network behavior: the important result is that
Rust reaches single-digit-millisecond warm page/subresource responses while
using far less RSS and fewer file descriptors. Future warm-path work should
focus on preserving this behavior under longer sessions and larger cached
working sets rather than chasing a 2ms local benchmark delta.

## 2026-05-05 Soak: Warm Same-Daemon `ipfs.tech` 30 Runs

Question:
Does a repeated warm same-daemon `ipfs.tech` page workload show obvious RSS/FD
growth or warm latency drift?

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 30 \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-warm-same-daemon-soak-r30-trace.jsonl \
  --output /tmp/ipfs-tech-warm-same-daemon-soak-r30.json
```

Result:

- Rust passed `30/30`.
- Run total p50/p90/p95/max: `20ms` / `30ms` / `50ms` / `61ms`.
- Root TTFB p50/p90/p95/max: `1ms` / `3ms` / `4ms` / `5ms`.
- Asset TTFB p50/p90/p95/max: `2ms` / `4ms` / `4ms` / `10ms`.
- Max RSS/FD: `52384KiB` / `35`.
- RSS stayed in a narrow `52128KiB` to `52384KiB` band across measured runs.
- FD count stayed between `33` and `35`.
- Per-run totals after warmup were mostly `19ms` to `22ms`, with the first few
  measured runs settling from `50ms`, `45ms`, and `30ms`, and one late `61ms`
  outlier.
- Block sources during the traced warmup/measured window:
  `http_provider=36`, `bitswap=4`.
- Delegated provider lookup p50/p90/p95/max:
  `22ms` / `43ms` / `55ms` / `80ms`.
- Delegated self-hedges: `0`.
- HTTP-provider fetch p50/p90/p95/max:
  `161ms` / `621ms` / `722ms` / `897ms`.
- HTTP-provider self-hedges: `8`, all during cold/warm-fill block fetches, not
  during the already-hot page responses.

Decision:
Keep as a resource soak baseline. There is no obvious RSS or FD growth over 30
warm page loads, and the hot measured path remains stable at roughly 20ms total
per page run. Longer soaks should use this as the short-run reference and
should separate cold/warm-fill trace events from hot measured responses when
looking at slow HTTP-provider fetches.

## 2026-05-05 Baseline: Resolved-IPFS Offline Replay After Self-Hedges

Question:
After one online `ipfs.tech` page load, can the warmed Rust cache replay the
page offline when the observed IPNS target is rewritten to its resolved
`/ipfs/...` path?

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --offline-replay \
  --offline-replay-resolved-ipfs \
  --case ipfs-tech-page-assets \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-resolved-offline-post-self-hedges-trace.jsonl \
  --output /tmp/ipfs-tech-resolved-offline-post-self-hedges.json
```

Result:

- Offline replay DB:
  `/tmp/freedom-ipfs-offline-replay.db-1563279-1778023401850`.
- Resolved path rewrite:
  `/ipns/ipfs.tech/` ->
  `/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/`.
- Online pass: `1/1`.
- Offline pass: `1/1`.
- Missing URLs: `0`.
- Offline statuses: `200=27`, `206=6`.
- Offline progress phases: `streaming=232`, `completed=33`, `queued=33`,
  `started=33`.
- Offline storage bytes reported by the harness: `4096B`.

Decision:
Keep as current cache-completeness evidence. A single online load is sufficient
for resolved-IPFS offline replay of the current `ipfs.tech` root plus same-site
asset set. Product-level offline IPNS behavior still depends on how the app
wants to handle name freshness and IPNS record caching, but the block/resource
cache has the page data needed for this resolved replay.

## 2026-05-05 Keep: Count SQLite WAL/SHM Storage In Harness

Question:
The offline replay above reported only `4096B` of storage even though the page
cache clearly contained much more data. Is the harness undercounting SQLite
storage by measuring only the main DB file and not `-wal` / `-shm` sidecars?

Finding:

The offline replay DB path had sidecars:

```text
/tmp/freedom-ipfs-offline-replay.db-1563279-1778023401850      4.0K
/tmp/freedom-ipfs-offline-replay.db-1563279-1778023401850-shm   32K
/tmp/freedom-ipfs-offline-replay.db-1563279-1778023401850-wal  2.3M
```

Implementation:

- Change the harness storage-size helper so a file path counts:
  - the main file
  - `<path>-wal`
  - `<path>-shm`
- Keep directory storage measurement recursive, so Kubo repo sizing still works.
- Add a focused unit test covering sidecar accounting.

Validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness storage_size_counts_sqlite_sidecars
cargo test -p mobile-web-harness offline_replay_summary_collects_failed_roots_and_assets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
```

Focused result:

- Formatting passed.
- Sidecar storage-size test passed.
- Offline replay summary test passed.
- Harness clippy passed with `-D warnings`.

Post-fix offline replay:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --offline-replay \
  --offline-replay-resolved-ipfs \
  --case ipfs-tech-page-assets \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-resolved-offline-storage-sidecars-trace.jsonl \
  --output /tmp/ipfs-tech-resolved-offline-storage-sidecars.json
```

Post-fix result:

- Online pass: `1/1`.
- Offline pass: `1/1`.
- Missing URLs: `0`.
- Offline statuses: `200=27`, `206=6`.
- Offline storage bytes: `2574816B`.

Decision:
Keep. This is diagnostics-only, but it matters for mobile resource accounting:
SQLite WAL mode can put most recently written cache bytes in the `-wal` file,
so measuring only the main DB substantially understated storage after offline
warmup.

## 2026-05-05 Guardrail: Conditional Revalidation After Self-Hedges

Question:
After the HTTP-provider and delegated-router self-hedges, does browser-style
conditional revalidation still stay cheap and correct for an `ipfs.tech` page
load?

Command:

```sh
timeout 600s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --conditional-revalidate \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-conditional-post-self-hedges-trace.jsonl \
  --output /tmp/ipfs-tech-conditional-post-self-hedges.json
```

Result:

- Passed: `1/1`.
- Run total: `2754ms`.
- Root TTFB/total: `918/919ms`.
- Asset TTFB p50/p90/p95/max: `281/538/1049/1122ms`.
- Asset total p50/p90/p95/max: `281/538/1049/1123ms`.
- Root revalidation: `1/1`, failed `0`, status `304`, TTFB
  p50/p90/p95/max `3/3/3/3ms`.
- Asset revalidation: `26/26`, failed `0`, all `304`, TTFB
  p50/p90/p95/max `2/3/3/3ms`.
- Gateway RSS/FD: `52636KiB` / `32`.

Trace summary:

- Trace path:
  `/tmp/ipfs-tech-conditional-post-self-hedges-trace.jsonl`.
- JSON output:
  `/tmp/ipfs-tech-conditional-post-self-hedges.json`.
- Trace events/phases: `1274` events, `31` phases.
- Gateway statuses: `200=27`, `304=27`, `206=6`.
- Block sources during the cold fill: `http_provider=27`, `bitswap=13`.
- Delegated provider lookups: `31`, successes `31`, providers `447`,
  HTTP providers `53`.
- Delegated self-hedges: `1`, max self-hedge timeout `750ms`.
- HTTP-provider races: `22`; HTTP-provider self-hedges: `2`, max result
  elapsed `470ms`.
- HTTP-provider fetch p50/p90/p95/max: `162/327/355/469ms`.

Notes:

- The single delegated self-hedge fired for
  `/ipns/ipfs.tech/_nuxt/Grid.CfsFuo-l.css` and returned providers
  successfully in `757ms`.
- The slowest cold asset was `/ipns/ipfs.tech/_nuxt/B1ETkkRH.js` at
  `1122ms` TTFB; trace events point at a late Bitswap peer wait rather than
  conditional revalidation.
- The subsequent conditional requests were served as fast local `304`
  responses and did not hit the network-heavy provider paths again.

Decision:
Keep as a browser-cache semantics guardrail. The self-hedge changes do not
break conditional request behavior, and the harness now gives a compact signal
that a mobile WebKit integration can revalidate cached page resources without
paying another cold retrieval cost.

## 2026-05-05 Baseline: Current-Head `ipfs.tech` vs Kubo

Question:
After the delegated-router self-hedge and storage diagnostics fixes, where does
the current branch stand against Kubo on the focused cold `ipfs.tech` page
workload?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-current-head-kubo-comparison-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-current-head-kubo-comparison-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `1274ms` / `1865ms`; Kubo `1690ms` /
  `1769ms`; Rust ratio `0.75x` / `1.05x`.
- Asset TTFB p50/p95: Rust `239ms` / `693ms`; Kubo `115ms` /
  `1512ms`; Rust ratio `2.08x` / `0.46x`.
- Max RSS/FD: Rust `47076KiB` / `26`; Kubo `215572KiB` / `91`.
- Kubo max storage for the run: `833145B`; Rust used the default temporary
  DB path without storage reporting in this comparison.

Rust trace summary:

- Trace path:
  `/tmp/ipfs-tech-current-head-kubo-comparison-r3-trace.jsonl`.
- Comparison JSON:
  `/tmp/ipfs-tech-current-head-kubo-comparison-r3.json`.
- Trace events/phases: `2842` events, `22` phases.
- Gateway statuses: `200=81`, `206=18`, limiter denials `0`.
- Block-fetch sources: `http_provider=120`, total `28930ms`,
  p50/p90/p95/max `217/394/557/1071ms`.
- Delegated provider lookups: `105`, successes `105`, failures `0`,
  providers `1437`, HTTP providers `189`, self-hedges `0`,
  p50/p90/p95/max `21/41/46/66ms`.
- HTTP-provider races: `105`, single-provider `63`, multi-provider `42`,
  self-hedges `13`, result max `1048ms`.
- HTTP-provider fetches: `105`, successes `105`, failures `0`, bytes
  `2381466`, p50/p90/p95/max `160/217/382/1048ms`.
- `https://ipfs-bridge.sia.dev/`: `63` fetches, p50/p90/p95/max
  `169/372/524/1048ms`.
- `https://dag.w3s.link/`: `42` fetches, p50/p90/p95/max
  `41/69/75/130ms`.

Notes:

- The current branch remains much more mobile-resource-efficient than Kubo in
  this live sample: about `22%` of Kubo's max RSS and `29%` of its max FD count.
- Rust still wins cold asset p95 substantially, while Kubo keeps a better asset
  p50. The trace points at single-provider HTTP provider latency as the main
  measured asset p50 cost, not delegated-router lookup latency.
- Delegated self-hedging stayed idle in this comparison because delegated
  lookups were already fast; HTTP-provider self-hedging fired `13` times.
- The slowest Rust root request spent `1048ms` fetching the `112239B` index
  block from a single HTTP provider, with the verified block still required
  before serving or caching.

Decision:
Keep as the current cold `ipfs.tech` Rust-vs-Kubo baseline. The next speed
experiments should avoid increasing fanout blindly and instead target the
single-provider HTTP path, provider diversity before large cold blocks, and
warm/local asset p50 overhead while preserving verified-block semantics.

## 2026-05-05 Keep: Lower Single HTTP Self-Hedge To 250ms

Question:
The current-head Kubo comparison showed that delegated lookups were fast, while
single-provider HTTP fetches from `https://ipfs-bridge.sia.dev/` still dominated
the Rust asset p50 gap. Can the same-provider self-hedge fire at `250ms`,
matching the existing multi-provider HTTP hedge, without adding too much mobile
resource pressure?

Implementation:

- Change `SINGLE_HTTP_PROVIDER_SELF_HEDGE_AFTER` from `350ms` to `250ms`.
- Keep the existing kill switch:
  `FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1`.
- Keep the same bounded HTTP provider fetch limiter:
  `MAX_CONCURRENT_HTTP_PROVIDER_FETCHES=4`.
- The duplicate request is still only used when there is exactly one HTTP
  provider candidate.
- The first verified block still wins; no public gateway fallback is added.

Baseline command at the prior `350ms` threshold:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-self-hedge350-current-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-self-hedge350-current-r3.json
```

Baseline result:

- Passed: `3/3`.
- Run total p50/p95/max: `2410/3302/3302ms`.
- Root TTFB p50/p95/max: `983/1590/1590ms`.
- Asset TTFB p50/p90/p95/max: `232/474/528/1056ms`.
- Gateway max RSS/FD: `48088KiB` / `26`.
- Block sources: `http_provider=120`.
- HTTP-provider self-hedges: `9`.
- Single-provider HTTP winner p50/p90/p95/max:
  `245/490/539/914ms`.
- HTTP-provider fetch p50/p90/p95/max: `160/245/450/639ms`.

First `250ms` experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-self-hedge250-experiment-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-self-hedge250-experiment-r3.json
```

First experiment result:

- Passed: `3/3`.
- Run total p50/p95/max: `2265/2751/2751ms`.
- Root TTFB p50/p95/max: `721/963/963ms`.
- Asset TTFB p50/p90/p95/max: `192/452/520/1038ms`.
- Gateway max RSS/FD: `52872KiB` / `33`.
- Block sources: `http_provider=92`, `bitswap=28`.
- HTTP-provider self-hedges: `10`.
- Single-provider HTTP winner p50/p90/p95/max:
  `194/272/393/443ms`.
- HTTP-provider fetch p50/p90/p95/max: `159/201/231/393ms`.

Second `250ms` experiment:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-self-hedge250-experiment-rerun-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-self-hedge250-experiment-rerun-r3.json
```

Second experiment result:

- Passed: `3/3`.
- Run total p50/p95/max: `1878/2112/2112ms`.
- Root TTFB p50/p95/max: `711/884/884ms`.
- Asset TTFB p50/p90/p95/max: `154/465/513/803ms`.
- Gateway max RSS/FD: `53940KiB` / `35`.
- Block sources: `http_provider=73`, `bitswap=47`.
- HTTP-provider self-hedges: `15`.
- Single-provider HTTP winner p50/p90/p95/max:
  `235/454/536/552ms`.
- HTTP-provider fetch p50/p90/p95/max: `161/218/388/535ms`.

Guardrails:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case daicowtf-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-single-http-self-hedge250-guardrail-r3-trace.jsonl \
  --output /tmp/daicowtf-single-http-self-hedge250-guardrail-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case vitalik-root-html-range \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/vitalik-root-html-range-single-http-self-hedge250-guardrail-r3-trace.jsonl \
  --output /tmp/vitalik-root-html-range-single-http-self-hedge250-guardrail-r3.json
```

Guardrail results:

- `daicowtf-page-assets`: passed `3/3`; root TTFB p50/p95/max
  `269/369/369ms`; max RSS/FD `33356KiB` / `14`; HTTP self-hedges `0`.
- `vitalik-root-html-range`: passed `3/3`; root/range TTFB p50/p95/max
  `251/251/251ms`; max RSS/FD `32128KiB` / `14`; HTTP self-hedges `0`.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval self_hedges_slow_single_http_provider
cargo check -p freedom-ipfs-retrieval --all-targets
cargo clippy -p freedom-ipfs-retrieval --all-targets -- -D warnings
```

Result:
All validation commands passed.

Decision:
Keep. The lower threshold produced two positive `ipfs.tech` samples, improved
root and asset p50, kept asset p95 roughly flat or better, and stayed bounded by
the existing HTTP fetch limiter. It did shift more blocks to the existing
Bitswap race in the two live samples, raising max FD/RSS modestly, but still
well below the current Kubo baseline and idle on the already-fast DAICO/Vitalik
guardrails. Revert or retune if future long soaks show duplicate HTTP pressure
or Bitswap connection growth during mobile-length sessions.

## 2026-05-05 Baseline: 250ms Self-Hedge `ipfs.tech` vs Kubo

Question:
After lowering the single HTTP self-hedge threshold to `250ms`, where does the
current Rust gateway stand against Kubo on the focused cold `ipfs.tech` page
workload?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge250-kubo-comparison-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-self-hedge250-kubo-comparison-r3.json
```

Result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `844ms` / `984ms`; Kubo `1755ms` /
  `2006ms`; Rust ratio `0.48x` / `0.49x`.
- Asset TTFB p50/p95: Rust `129ms` / `539ms`; Kubo `144ms` /
  `862ms`; Rust ratio `0.90x` / `0.63x`.
- Max RSS/FD: Rust `53032KiB` / `38`; Kubo `204604KiB` / `108`.
- Kubo max storage: `835101B`; Rust used the default temporary DB path without
  storage reporting in this comparison.

Rust trace summary:

- Trace path:
  `/tmp/ipfs-tech-self-hedge250-kubo-comparison-r3-trace.jsonl`.
- Comparison JSON:
  `/tmp/ipfs-tech-self-hedge250-kubo-comparison-r3.json`.
- Trace events/phases: `2923` events, `28` phases.
- Gateway statuses: `200=81`, `206=18`, limiter denials `0`.
- Block sources: `http_provider=63`, `bitswap=56`.
- HTTP-provider block totals p50/p90/p95/max:
  `212/345/447/696ms`.
- Bitswap block totals p50/p90/p95/max:
  `87/215/228/231ms`.
- Delegated provider lookups: `80`, successes `80`, failures `0`,
  providers `1139`, HTTP providers `137`, self-hedges `0`,
  p50/p90/p95/max `30/46/52/87ms`.
- HTTP-provider races: `49`, single-provider `27`, multi-provider `22`,
  self-hedges `11`, result max `674ms`.
- Single-provider HTTP winner p50/p90/p95/max:
  `218/453/454/674ms`.
- HTTP-provider fetch p50/p90/p95/max: `157/216/218/362ms`.
- `https://ipfs-bridge.sia.dev/`: `27` fetches, p50/p90/p95/max
  `182/218/218/362ms`.
- `https://dag.w3s.link/`: `22` fetches, p50/p90/p95/max
  `68/126/141/213ms`.
- Bitswap connections established: `14`; max FD remained `38`.

Decision:
Keep as the current post-250ms Rust-vs-Kubo baseline. In this sample Rust is
faster than Kubo at root p50/p95 and asset p50/p95 while still using far less
RSS and fewer file descriptors. The remaining follow-up is resource-oriented:
watch longer page sessions to make sure the lower HTTP self-hedge threshold does
not steadily increase Bitswap connections or duplicate HTTP bytes.

## 2026-05-05 Soak: Warm Same-Daemon `ipfs.tech` After 250ms Self-Hedge

Question:
After the lower single HTTP self-hedge threshold, does a warm same-daemon
`ipfs.tech` page workload show FD/RSS growth or hot-path latency drift?

Command:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 30 \
  --asset-concurrency 6 \
  --max-concurrent-requests 8 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-warm-same-daemon-post-250-soak-r30-trace.jsonl \
  --output /tmp/ipfs-tech-warm-same-daemon-post-250-soak-r30.json
```

Result:

- Passed: `30/30`.
- Run total p50/p90/p95/max: `20/26/44/89ms`.
- Root TTFB p50/p90/p95/max: `1/2/5/43ms`.
- Asset TTFB p50/p90/p95/max: `2/3/4/10ms`.
- Gateway RSS p50/p90/p95/max: `53248/53248/53248/53248KiB`.
- Gateway FD p50/p90/p95/max: `31/31/32/32`.
- Per-run totals after the first measured runs settled around `19-23ms`.
- Per-run RSS stayed between `52992KiB` and `53248KiB`.
- Per-run FDs stayed between `30` and `32`.

Trace summary:

- Trace path:
  `/tmp/ipfs-tech-warm-same-daemon-post-250-soak-r30-trace.jsonl`.
- JSON output:
  `/tmp/ipfs-tech-warm-same-daemon-post-250-soak-r30.json`.
- Trace includes the warmup plus measured runs.
- Gateway statuses: `200=837`, `206=186`, limiter denials `0`.
- Block sources during warm fill: `http_provider=25`, `bitswap=15`.
- HTTP-provider self-hedges: `5`, all in the warm-fill window.
- Delegated self-hedges: `0`.
- Bitswap connections established: `6`.
- Hot measured groups after the initial fill were cache-local, with page group
  max events generally in the low single-digit milliseconds.

Decision:
Keep as the post-250ms resource guardrail. The lower self-hedge threshold does
not leave obvious hot-path latency drift or FD/RSS growth in this 30-run
same-daemon page soak. Future longer soaks should still track duplicate HTTP
bytes and Bitswap connection lifetime, but this sample does not show immediate
mobile-resource regression.

## 2026-05-05 Guardrail: 250ms Self-Hedge Cold `ipfs.tech` 10 Runs

Question:
Does the 250ms single HTTP self-hedge hold up across a wider cold fresh-gateway
sample, or was the 3-run signal too noisy?

Command:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 10 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge250-cold-r10-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge250-cold-r10.json
```

Result:

- Passed: `10/10`.
- Run total p50/p90/p95/max: `1808/2174/2226/2226ms`.
- Root TTFB p50/p90/p95/max: `548/722/788/788ms`.
- Asset TTFB p50/p90/p95/max: `168/459/503/876ms`.
- Gateway RSS p50/p90/p95/max:
  `51756/52528/53192/53192KiB`.
- Gateway FD p50/p90/p95/max: `31/33/33/33`.
- Gateway statuses: `200=270`, `206=60`, limiter denials `0`.

Trace summary:

- Trace path:
  `/tmp/ipfs-tech-self-hedge250-cold-r10-trace.jsonl`.
- JSON output:
  `/tmp/ipfs-tech-self-hedge250-cold-r10.json`.
- Trace events/phases: `9842` events, `30` phases.
- Block sources: `http_provider=276`, `bitswap=123`, `cache=1`.
- HTTP-provider block totals p50/p90/p95/max:
  `204/312/346/682ms`.
- Bitswap block totals p50/p90/p95/max:
  `129/274/303/526ms`.
- Delegated provider lookups: `328`, successes `328`, failures `0`,
  self-hedges `0`, p50/p90/p95/max `23/47/53/635ms`.
- HTTP-provider races: `226`, single-provider `122`, multi-provider `104`,
  self-hedges `26`, result max `448ms`.
- Single-provider HTTP winner p50/p90/p95/max:
  `211/303/321/448ms`.
- HTTP-provider fetch p50/p90/p95/max: `159/218/234/401ms`.
- `https://ipfs-bridge.sia.dev/`: `122` fetches, p50/p90/p95/max
  `183/233/271/401ms`.
- `https://dag.w3s.link/`: `104` fetches, p50/p90/p95/max
  `50/88/95/124ms`.
- Bitswap connections established: `39` across 10 fresh gateway processes.

Decision:
Keep. The wider cold sample keeps the same direction as the 3-run experiments:
sub-second root p95, asset p50 close to warm human-perceived responsiveness,
and bounded FD/RSS. The most visible remaining tail is no longer HTTP provider
fetch result latency; it is UnixFS/root path work waiting on the slowest block
fetches and occasional delegated lookup outliers under `1s`.

## 2026-05-05 Reject: Lower Delegated Self-Hedge To 500ms

Question:
The 250ms HTTP self-hedge cold r10 sample had a rare delegated lookup outlier
at `635ms`, below the current `750ms` delegated same-endpoint self-hedge. Would
lowering `SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER` to `500ms` catch useful
delegated-router tails without adding meaningful traffic?

Temporary implementation:

- Change `SINGLE_DELEGATED_ENDPOINT_SELF_HEDGE_AFTER` from `750ms` to `500ms`.
- Keep the existing kill switch:
  `FREEDOM_IPFS_DISABLE_SINGLE_DELEGATED_SELF_HEDGE=1`.
- Keep the existing one-duplicate, same-endpoint behavior.

Focused validation:

```sh
cargo test -p freedom-ipfs-routing delegated_routing_self_hedges_slow_single_endpoint
```

Focused result:
Passed.

Live experiment:

```sh
timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-page-assets \
  --repeat 10 \
  --fresh-gateway-per-run \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-delegated-self-hedge500-cold-r10-trace.jsonl \
  --output /tmp/ipfs-tech-delegated-self-hedge500-cold-r10.json
```

Live result:

- Passed: `10/10`.
- Run total p50/p90/p95/max: `2043/2349/2367/2367ms`.
- Root TTFB p50/p90/p95/max: `550/742/843/843ms`.
- Asset TTFB p50/p90/p95/max: `214/458/539/940ms`.
- Gateway max RSS/FD: `53484KiB` / `30`.
- Delegated provider lookups: `325`, successes `325`, failures `0`.
- Delegated self-hedges: `0`.
- Delegated lookup p50/p90/p95/max: `22/47/52/94ms`.
- HTTP-provider self-hedges: `42`.
- Block sources: `http_provider=311`, `bitswap=88`, `cache=1`.

Decision:
Reject and revert. In this window the lower delegated threshold did not fire at
all, so it gave no evidence of useful tail protection. The 500ms run was also
slower than the preceding 750ms-threshold r10 sample, apparently from unrelated
network/source variance, so there is no measured reason to change the delegated
hedge threshold now. Keep `750ms` until a repeated live tail actually crosses
the current guard or a deterministic production-like test shows a narrower
threshold helps.

## 2026-05-05 Keep: Close Progress Targets For Lifecycle-Cancelled Preloads

Question:
Mobile lifecycle hooks abort active preload tasks when the app backgrounds,
receives a low-memory event, changes networks, or frees the node. Do those
implicit cancellations also close the per-load progress target that Swift polls?

Implementation:

- `stop_preloads` now emits the same `preload_cancelled` structured event used
  by explicit preload cancellation before aborting an unfinished task.
- Finished preload tasks are still drained without emitting a duplicate
  cancellation event.
- `docs/mobile-progress-api.md` now documents lifecycle cancellation semantics.

Focused validation:

```sh
cargo test -p freedom-ipfs-mobile lifecycle_preload_cancellation_clears_progress_target
```

Focused result:
Passed.

Decision:
Keep. Without this event, Swift could see a stale active preload target after a
normal lifecycle stop even though the task had been aborted. The change does not
alter the FFI ABI or Swift wrapper surface; it only makes existing progress
snapshots reflect lifecycle-driven cancellation accurately.

## 2026-05-05 Keep: Add Opt-In Media Range And HEAD Corpus Cases

Question:
The roadmap calls out range-heavy media workloads, but the live mobile corpus
only had a first-prefix image range for the `ipfs.tech` developers hero asset.
Can the harness cover middle range, suffix range, and HEAD shapes without
expanding the default live corpus cost?

Implementation:

- Add opt-in corpus cases for the DNSLink-backed
  `/ipns/ipfs.tech/_nuxt/developers-hero.BRuJDQyf.jpg` asset:
  - `ipfs-tech-developers-hero-middle-range`: `bytes=65536-69631`
  - `ipfs-tech-developers-hero-suffix-range`: `bytes=-4096`
  - `ipfs-tech-developers-hero-head`: `HEAD`
- Keep the existing first-prefix image range case enabled by default.
- Add an optional `expect_body_bytes` corpus assertion so HEAD cases can verify
  the gateway returns no response body.
- Document that stable audio/video CIDs remain a useful future corpus addition.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
```

Focused result:

- Formatting passed.
- Full mobile web harness suite passed: `39 passed`.

Live Rust validation:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-media-range-head-r3-trace.jsonl \
  --output /tmp/ipfs-tech-hero-media-range-head-r3.json
```

Live Rust result:

- Passed: `3/3`.
- Run total p50/p95/max: `1665/1884/1884ms`.
- Gateway max RSS/FD: `39692KiB` / `19`.
- Middle range passed `3/3` with `206` and `Content-Range:
  bytes 65536-69631/184141`; TTFB p50/p95/max `1658/1880/1880ms`.
- Suffix range passed `3/3` with `206` and `Content-Range:
  bytes 180045-184140/184141`; TTFB p50/p95/max `3/4/4ms`.
- HEAD passed `3/3` with `200`, `0` response bytes, and TTFB p50/p95/max
  `3/3/3ms`.
- Block sources: `http_provider=6`, `bitswap=3`.
- UnixFS metadata cache within each fresh process: path hits/misses `6/3`,
  file-size hits/misses `6/3`.

Same-window Kubo comparison:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-media-range-head-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-hero-media-range-head-kubo-r3.json
```

Same-window Kubo result:

- Rust and Kubo both passed `3/3`.
- Middle range TTFB p50/p95: Rust `788/839ms`, Kubo `3103/3335ms`.
- Suffix range TTFB p50/p95: Rust `3/3ms`, Kubo `4/16ms`.
- HEAD TTFB p50/p95: Rust `3/3ms`, Kubo `2/4ms`.
- Resource max RSS/FD: Rust `32944KiB` / `15`, Kubo `265148KiB` / `174`.

Decision:
Keep. This is harness coverage and corpus validation only; it does not change
gateway retrieval behavior. The opt-in cases exercise media-style range and
metadata shapes that WebKit can issue while preserving default harness cost and
mobile resource constraints.

## 2026-05-05 Keep: Assert Content-Length For Media Range And HEAD Cases

Question:
The media range corpus validates status, MIME type, `Content-Range`, and body
length, but browser media and metadata paths also depend on stable
`Content-Length`. Can the harness record and assert `Content-Length` without
changing gateway behavior?

Implementation:

- Capture response `Content-Length` in `mobile-web-harness` fetch results and
  serialized root/asset reports.
- Add optional corpus field `expect_content_length`.
- Assert `Content-Length: 4096` for the first, middle, and suffix
  `ipfs.tech` developers hero image range cases.
- Assert `Content-Length: 184141` and zero response body for the opt-in HEAD
  case.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
```

Focused result:

- Formatting passed.
- Full mobile web harness suite passed: `39 passed`.
- Harness check passed.

Live Rust validation:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-content-length-r3-trace.jsonl \
  --output /tmp/ipfs-tech-hero-content-length-r3.json
```

Live Rust result:

- Passed: `3/3`.
- Run total p50/p95/max: `1091/1136/1136ms`.
- Gateway max RSS/FD: `33056KiB` / `16`.
- First range passed with `206`, `Content-Range:
  bytes 0-4095/184141`, `Content-Length: 4096`, body bytes `4096`, TTFB
  p50/p95/max `1079/1124/1124ms`.
- Middle range passed with `206`, `Content-Range:
  bytes 65536-69631/184141`, `Content-Length: 4096`, body bytes `4096`, TTFB
  p50/p95/max `4/4/4ms`.
- Suffix range passed with `206`, `Content-Range:
  bytes 180045-184140/184141`, `Content-Length: 4096`, body bytes `4096`,
  TTFB p50/p95/max `3/3/3ms`.
- HEAD passed with `200`, no `Content-Range`, `Content-Length: 184141`, body
  bytes `0`, TTFB p50/p95/max `3/3/3ms`.
- Block sources: `http_provider=9`.
- UnixFS metadata cache within each fresh process: path hits/misses `9/3`,
  file-size hits/misses `9/3`.

Decision:
Keep. This is harness/reporting coverage only and confirms the existing gateway
media range and HEAD behavior surfaces the headers WebKit needs. It preserves
read-only retrieval, verified block handling, and mobile resource constraints.

## 2026-05-05 Keep: Assert Accept-Ranges For Media Range And HEAD Cases

Question:
The gateway already emits `Accept-Ranges: bytes` for file and range responses,
but the live harness did not record or assert that header. Since WebKit media
loads rely on range support, can the corpus catch regressions in this header?

Implementation:

- Capture response `Accept-Ranges` in `mobile-web-harness` fetch results and
  serialized root/asset reports.
- Add optional corpus field `expect_accept_ranges`.
- Assert `Accept-Ranges: bytes` for the first, middle, suffix, and HEAD
  `ipfs.tech` developers hero image cases.

Focused validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
```

Focused result:

- Formatting was applied.
- Full mobile web harness suite passed: `39 passed`.
- Harness check passed.

Live Rust validation:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-accept-ranges-r3-trace.jsonl \
  --output /tmp/ipfs-tech-hero-accept-ranges-r3.json
```

Live Rust result:

- Passed: `3/3`.
- Run total p50/p95/max: `847/1125/1125ms`.
- Gateway max RSS/FD: `33080KiB` / `15`.
- First range passed with `206`, `Content-Range:
  bytes 0-4095/184141`, `Content-Length: 4096`,
  `Accept-Ranges: bytes`, body bytes `4096`, TTFB p50/p95/max
  `835/1112/1112ms`.
- Middle range passed with `206`, `Content-Range:
  bytes 65536-69631/184141`, `Content-Length: 4096`,
  `Accept-Ranges: bytes`, body bytes `4096`, TTFB p50/p95/max `4/4/4ms`.
- Suffix range passed with `206`, `Content-Range:
  bytes 180045-184140/184141`, `Content-Length: 4096`,
  `Accept-Ranges: bytes`, body bytes `4096`, TTFB p50/p95/max `3/3/3ms`.
- HEAD passed with `200`, no `Content-Range`, `Content-Length: 184141`,
  `Accept-Ranges: bytes`, body bytes `0`, TTFB p50/p95/max `3/3/3ms`.
- Block sources: `http_provider=9`.

Decision:
Keep. This is harness/reporting coverage only and confirms the existing gateway
media range and HEAD responses advertise range support in the way browser media
clients expect.

## 2026-05-05 Keep: Assert Media Cache Validators In Harness

Question:
The media range and HEAD cases now validate `Content-Range`, `Content-Length`,
and `Accept-Ranges`, but they still do not assert cache validators. Browser
cache behavior depends on `ETag` and `Cache-Control`; can the harness verify
those headers without hard-coding the mutable DNSLink root CID?

Implementation:

- Add optional corpus fields:
  - `expect_etag_prefix`
  - `expect_cache_control`
- Use `expect_etag_prefix: "\"fi1:"` for the `ipfs.tech` developers hero
  image cases so the harness checks for freedom-ipfs validator format without
  pinning the current resolved DNSLink root.
- Use `expect_cache_control: "no-cache"` for these `/ipns` cases.

Focused validation:

```sh
cargo fmt --all
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
```

Focused result:

- Formatting was applied.
- Full mobile web harness suite passed: `39 passed`.
- Harness check passed.

Live Rust validation:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-cache-validator-r3-trace.jsonl \
  --output /tmp/ipfs-tech-hero-cache-validator-r3.json
```

Live Rust result:

- Passed: `3/3`.
- Run total p50/p95/max: `1097/1348/1348ms`.
- Gateway max RSS/FD: `32804KiB` / `16`.
- First range passed with `206`, `ETag: "fi1:..."`,
  `Cache-Control: no-cache`, `Content-Range: bytes 0-4095/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`, TTFB
  p50/p95/max `1089/1335/1335ms`.
- Middle range passed with `206`, `ETag: "fi1:..."`,
  `Cache-Control: no-cache`, `Content-Range: bytes 65536-69631/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`, TTFB
  p50/p95/max `4/4/4ms`.
- Suffix range passed with `206`, `ETag: "fi1:..."`,
  `Cache-Control: no-cache`, `Content-Range: bytes 180045-184140/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`, TTFB
  p50/p95/max `3/4/4ms`.
- HEAD passed with `200`, `ETag: "fi1:..."`, `Cache-Control: no-cache`, no
  `Content-Range`, `Content-Length: 184141`, `Accept-Ranges: bytes`, body
  bytes `0`, TTFB p50/p95/max `2/3/3ms`.
- Block sources: `http_provider=9`.

Decision:
Keep. This is harness/corpus coverage only and makes the browser-facing media
header checks complete enough to catch regressions in range support, response
size metadata, and cache validators without changing gateway retrieval behavior.

## 2026-05-06 Keep: Assert Media Range Body Digests In Harness

Question:
The media range cases now validate browser-facing headers, but a wrong slice
with the right `Content-Range` and length could still pass. Can the harness
assert exact small-range bytes without fetching the full media object through
the gateway?

Implementation:

- Add `sha2` to the `mobile-web-harness` dependencies.
- Add optional corpus field `expect_body_sha256`.
- Compute SHA-256 over the received response body when that field is present.
- Add a focused unit test for the digest helper.
- Assert body digests for the three 4 KiB `ipfs.tech` developers hero image
  range cases:
  - `bytes=0-4095`:
    `777dd08978fe51010bbee6601784d556917d69b70e552cabace991c83a9dc2ef`
  - `bytes=65536-69631`:
    `ba436a6cea557440db340441ba7ef405a4d8d3bf67abb0d875d403c0053482c3`
  - `bytes=-4096`:
    `40ed68130ba8a0d3204d40ba590a2729056d1251f662343ff59ba84c7bfe8595`

Digest source:

```sh
url='https://ipfs.tech/_nuxt/developers-hero.BRuJDQyf.jpg'
for spec in '0-4095' '65536-69631' '-4096'; do
  tmp=$(mktemp)
  curl -fsSL --range "$spec" "$url" -o "$tmp"
  bytes=$(wc -c < "$tmp")
  sha=$(sha256sum "$tmp" | awk '{print $1}')
  printf '%s bytes=%s sha256=%s\n' "$spec" "$bytes" "$sha"
  rm -f "$tmp"
done
```

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
```

Focused result:

- Formatting passed.
- Full mobile web harness suite passed: `40 passed`.
- Harness check passed.

Live Rust validation:

```sh
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --repeat 3 \
  --fresh-gateway-per-run \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-body-sha-r3-trace.jsonl \
  --output /tmp/ipfs-tech-hero-body-sha-r3.json
```

Live Rust result:

- Passed: `3/3`.
- Run total p50/p95/max: `914/1089/1089ms`.
- Gateway max RSS/FD: `33044KiB` / `16`.
- First range passed with `206`, `Content-Range:
  bytes 0-4095/184141`, `Content-Length: 4096`, `Accept-Ranges: bytes`,
  body bytes `4096`, and matching SHA-256; TTFB p50/p95/max
  `900/1076/1076ms`.
- Middle range passed with `206`, `Content-Range:
  bytes 65536-69631/184141`, `Content-Length: 4096`,
  `Accept-Ranges: bytes`, body bytes `4096`, and matching SHA-256; TTFB
  p50/p95/max `4/4/4ms`.
- Suffix range passed with `206`, `Content-Range:
  bytes 180045-184140/184141`, `Content-Length: 4096`,
  `Accept-Ranges: bytes`, body bytes `4096`, and matching SHA-256; TTFB
  p50/p95/max `3/3/3ms`.
- HEAD passed with `200`, no `Content-Range`, `Content-Length: 184141`,
  `Accept-Ranges: bytes`, body bytes `0`; TTFB p50/p95/max `3/3/3ms`.
- Block sources: `http_provider=9`.

Decision:
Keep. This remains harness/corpus coverage only. The media range cases now
check not just that the response is shaped like a range response, but that the
returned bytes are the expected slices, while still avoiding full-media
gateway fetches in the range assertions.

## 2026-05-06 Keep: Media Range Offline Replay Baseline

Question:
After warming the `ipfs.tech` developers hero image ranges online, can the
gateway restart in offline mode and still serve the same DNSLink-backed media
ranges and HEAD metadata from persistent cache only?

Command:

```sh
rm -f /tmp/freedom-ipfs-media-range-offline-replay.db \
  /tmp/freedom-ipfs-media-range-offline-replay.db-* \
  /tmp/ipfs-tech-hero-media-offline-replay*.json \
  /tmp/ipfs-tech-hero-media-offline-replay*.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-media-range-offline-replay.db \
  --offline-replay \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-media-offline-replay-trace.jsonl \
  --output /tmp/ipfs-tech-hero-media-offline-replay.json
```

Artifacts:

- `/tmp/ipfs-tech-hero-media-offline-replay.json`
- `/tmp/ipfs-tech-hero-media-offline-replay-trace-online.jsonl` (`92` lines)
- `/tmp/ipfs-tech-hero-media-offline-replay-trace-offline.jsonl` (`47` lines)
- `/tmp/freedom-ipfs-media-range-offline-replay.db`
- `/tmp/freedom-ipfs-media-range-offline-replay.db-wal`

Result:

- Online pass: `1/1` measured run passed, with all four selected cases passing.
  Run total `1348ms`, gateway RSS `32892KiB`, FD count `18`, storage
  `481856B`.
- Online first range: `206`, `Content-Range: bytes 0-4095/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `1335/1335ms`.
- Online middle range: `206`, `Content-Range: bytes 65536-69631/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `3/4ms`.
- Online suffix range: `206`, `Content-Range: bytes 180045-184140/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `3/3ms`.
- Online HEAD: `200`, no `Content-Range`, `Content-Length: 184141`,
  `Accept-Ranges: bytes`, body bytes `0`, TTFB/total `3/3ms`.
- Offline pass: `1/1` measured run passed, with all four selected cases
  passing. Run total `14ms`, gateway RSS `21460KiB`, FD count `15`, storage
  `502456B`.
- Offline first range: `206`, `Content-Range: bytes 0-4095/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `9/9ms`.
- Offline middle range: `206`, `Content-Range: bytes 65536-69631/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `1/1ms`.
- Offline suffix range: `206`, `Content-Range: bytes 180045-184140/184141`,
  `Content-Length: 4096`, `Accept-Ranges: bytes`, body bytes `4096`,
  matching SHA-256, TTFB/total `1/1ms`.
- Offline HEAD: `200`, no `Content-Range`, `Content-Length: 184141`,
  `Accept-Ranges: bytes`, body bytes `0`, TTFB/total `1/1ms`.
- Offline replay summary reported `missing_url_count=0`, offline statuses
  `206=3`, `200=1`, and offline progress phases `streaming=27`,
  `name_resolved=8`, `completed=4`, `queued=4`, `started=4`.
- Offline trace phases were cache-only for retrieval: `name_persistent_cache=4`,
  `unixfs_metadata_cache=4`, `gateway_direct_body=3`, and no provider lookup or
  block fetch phases.

Decision:
Keep as a baseline. The warmed `/ipns/ipfs.tech/_nuxt/developers-hero...jpg`
media slices and HEAD metadata survive a gateway restart and are served
offline from persistent name, UnixFS metadata, and block/cache state. This gives
the mobile browser a concrete offline/cache-only expectation for range-heavy
media workloads without fetching the full media object through the gateway.

## 2026-05-06 Keep: Summarize Offline Replay Cache-Only Evidence

Question:
Can the offline replay report directly show whether an offline replay stayed
cache-only, instead of requiring manual JSONL trace parsing?

Implementation:

- Add `event_phases` to `mobile-web-harness` trace summaries so all trace
  phases are counted, including events without `elapsed_ms`.
- Add these fields to `OfflineReplaySummary`:
  - `offline_network_phases`
  - `offline_cache_phases`
  - `offline_block_sources`
  - `offline_non_cache_block_sources`
- Print the same cache/network/source summaries in offline replay console
  output.
- Add focused regression coverage for extracting offline cache and network
  trace phases and filtering non-cache block sources.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness offline_replay_summary -- --nocapture
cargo check -p mobile-web-harness --all-targets
```

Focused result:

- Formatting passed.
- Focused offline replay summary tests passed: `2 passed`.
- Harness check passed.

Live validation:

```sh
rm -f /tmp/freedom-ipfs-media-range-offline-summary.db \
  /tmp/freedom-ipfs-media-range-offline-summary.db-* \
  /tmp/ipfs-tech-hero-media-offline-summary*.json \
  /tmp/ipfs-tech-hero-media-offline-summary*.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-media-range-offline-summary.db \
  --offline-replay \
  --case ipfs-tech-developers-hero-range \
  --case ipfs-tech-developers-hero-middle-range \
  --case ipfs-tech-developers-hero-suffix-range \
  --case ipfs-tech-developers-hero-head \
  --asset-concurrency 1 \
  --timeout-secs 120 \
  --run-timeout-secs 180 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-hero-media-offline-summary-trace.jsonl \
  --output /tmp/ipfs-tech-hero-media-offline-summary.json
```

Live result:

- Online pass: `1/1`; offline pass: `1/1`; missing URLs: `0`.
- Offline storage bytes: `518936B`.
- Offline statuses: `206=3`, `200=1`.
- Offline cache phases: `name_persistent_cache=4`,
  `unixfs_metadata_cache=4`, `gateway_direct_body=3`.
- Offline network phases: none.
- Offline block sources: none.
- Offline non-cache block sources: none.
- Offline progress phases: `streaming=27`, `name_resolved=8`,
  `completed=4`, `queued=4`, `started=4`.
- Offline run total `35ms`, gateway RSS `21704KiB`, FD count `15`.
- Offline first/middle/suffix/HEAD TTFB: `22/3/3/3ms`.

Artifacts:

- `/tmp/ipfs-tech-hero-media-offline-summary.json`
- `/tmp/ipfs-tech-hero-media-offline-summary-trace-online.jsonl`
- `/tmp/ipfs-tech-hero-media-offline-summary-trace-offline.jsonl`
- `/tmp/freedom-ipfs-media-range-offline-summary.db`

Decision:
Keep. Future offline replay runs now carry direct cache-only evidence in the
top-level report and console output. This makes offline/cache-completeness
experiments easier to audit and reduces the chance that a passing replay hides
provider lookup, HTTP provider fetch, or Bitswap activity in the trace.

## 2026-05-06 Keep: Full ipfs.tech Page Offline Replay Cache-Only Baseline

Question:
With the richer offline replay summary, does the full `ipfs-tech-page-assets`
case show cache-only behavior after one online page-and-assets load?

Command:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-page-offline-summary.db \
  /tmp/freedom-ipfs-ipfs-tech-page-offline-summary.db-* \
  /tmp/ipfs-tech-page-offline-summary*.json \
  /tmp/ipfs-tech-page-offline-summary*.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-page-offline-summary.db \
  --offline-replay \
  --case ipfs-tech-page-assets \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-page-offline-summary-trace.jsonl \
  --output /tmp/ipfs-tech-page-offline-summary.json
```

Artifacts:

- `/tmp/ipfs-tech-page-offline-summary.json`
- `/tmp/ipfs-tech-page-offline-summary-trace-online.jsonl` (`1008` lines)
- `/tmp/ipfs-tech-page-offline-summary-trace-offline.jsonl` (`397` lines)
- `/tmp/freedom-ipfs-ipfs-tech-page-offline-summary.db`
- `/tmp/freedom-ipfs-ipfs-tech-page-offline-summary.db-wal`

Result:

- Online pass: `1/1`; root status `200`; root TTFB/total `1302/1303ms`;
  root bytes `112239`.
- Online assets: `32` discovered, `32` fetched, `32` passed, `0` failed,
  truncated asset crawl `true`; asset TTFB p50/p95/max `245/1194/1260ms`.
- Online run total `3610ms`, gateway RSS `52600KiB`, FD count `29`, storage
  `2356456B`.
- Offline pass: `1/1`; root status `200`; root TTFB/total `15/15ms`; root
  bytes `112239`.
- Offline assets: `32` discovered, `32` fetched, `32` passed, `0` failed,
  truncated asset crawl `true`; asset TTFB p50/p95/max `6/13/19ms`.
- Offline run total `66ms`, gateway RSS `23656KiB`, FD count `20`, storage
  `2595416B`.
- Offline statuses: `200=27`, `206=6`; missing URLs: `0`.
- Offline cache phases: `name_persistent_cache=33`,
  `unixfs_metadata_cache=33`, `gateway_direct_body=31`,
  `gateway_stream_done=2`.
- Offline network phases: none.
- Offline block sources: none.
- Offline non-cache block sources: none.
- Offline progress phases: `streaming=232`, `name_resolved=66`,
  `completed=33`, `queued=33`, `started=33`.

Decision:
Keep as the current full-page cache-only baseline for `ipfs.tech`. One online
page-and-assets load warmed enough persistent state for the gateway to restart
offline and serve the root plus all discovered same-site assets in `66ms`
without provider lookup, HTTP provider fetch, Bitswap activity, or missing URLs.

## 2026-05-06 Keep: daicowtf Root Offline Replay Cache-Only Baseline

Question:
Does `daicowtf-page-assets` replay offline from persistent cache after one
online load, and does the case actually exercise same-site assets?

Command:

```sh
rm -f /tmp/freedom-ipfs-daicowtf-page-offline-summary.db \
  /tmp/freedom-ipfs-daicowtf-page-offline-summary.db-* \
  /tmp/daicowtf-page-offline-summary*.json \
  /tmp/daicowtf-page-offline-summary*.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-daicowtf-page-offline-summary.db \
  --offline-replay \
  --case daicowtf-page-assets \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/daicowtf-page-offline-summary-trace.jsonl \
  --output /tmp/daicowtf-page-offline-summary.json
```

Artifacts:

- `/tmp/daicowtf-page-offline-summary.json`
- `/tmp/daicowtf-page-offline-summary-trace-online.jsonl` (`56` lines)
- `/tmp/daicowtf-page-offline-summary-trace-offline.jsonl` (`11` lines)
- `/tmp/freedom-ipfs-daicowtf-page-offline-summary.db`

Result:

- Online pass: `1/1`; root status `200`; root TTFB/total `400/403ms`;
  root bytes `403507`; run total `415ms`; gateway RSS `33056KiB`, FD count
  `17`, storage `588976B`.
- Online crawl discovered `0` same-site fetchable assets, fetched `0`, skipped
  `7` external assets, and was not truncated.
- Offline pass: `1/1`; root status `200`; root TTFB/total `17/19ms`; root
  bytes `403507`; run total `31ms`; gateway RSS `21812KiB`, FD count `15`,
  storage `601336B`.
- Offline statuses: `200=1`; missing URLs: `0`.
- Offline cache phases: `gateway_stream_done=1`, `unixfs_metadata_cache=1`.
- Offline network phases: none.
- Offline block sources: none.
- Offline non-cache block sources: none.

Decision:
Keep, but classify this as a root-only offline/cache baseline. The current
`daicowtf-page-assets` crawl does not fetch same-site subresources, so it is not
comparable to the `ipfs-tech-page-assets` full page-and-assets cache-completeness
baseline. It still proves that the warmed root can restart and serve offline
without provider lookup or retrieval network activity.

## 2026-05-06 Observe: Warm ipfs.tech Rust-vs-Kubo Same-Daemon Comparison

Question:
After one warmup load against a single long-lived gateway/daemon, how close is
current Rust to Kubo for `ipfs-tech-page-assets`, and what resource tradeoff
does the harness report?

Command:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-warm-current.db \
  /tmp/freedom-ipfs-ipfs-tech-warm-current.db-* \
  /tmp/ipfs-tech-warm-current-rust-trace.jsonl \
  /tmp/ipfs-tech-warm-current-rust-vs-kubo.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-warm-current.db \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-warm-current-rust-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-warm-current-rust-vs-kubo.json
```

Artifacts:

- `/tmp/ipfs-tech-warm-current-rust-vs-kubo.json`
- `/tmp/ipfs-tech-warm-current-rust-trace.jsonl` (`2252` lines)
- `/tmp/freedom-ipfs-ipfs-tech-warm-current.db`
- `/tmp/freedom-ipfs-ipfs-tech-warm-current.db-wal`

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `45/49/49ms`.
- Kubo run total p50/p95/max: `37/38/38ms`.
- Rust root TTFB p50/p95/max: `5/5/5ms`.
- Kubo root TTFB p50/p95/max: `2/3/3ms`.
- Rust asset TTFB p50/p95/max: `4/8/8ms` over `96` asset requests.
- Kubo asset TTFB p50/p95/max: `3/4/5ms` over `96` asset requests.
- Ratios: root p50 `2.50x`, root p95 `1.67x`, asset p50 `1.33x`, asset p95
  `2.00x` in Kubo's favor.
- Rust max RSS/FD: `47312KiB` / `28`.
- Kubo max RSS/FD: `277520KiB` / `497`.
- Rust used `0.17x` Kubo RSS and `0.06x` Kubo FD count.
- Rust storage max: `2533616B`; Kubo storage max: `831989B`.

Rust trace notes:

- Warm measured runs were very fast after the warmup. The trace includes four
  request groups because the warmup plus three measured runs are all traced.
- Warm measured groups had root/request group maxima of `3ms`, `2ms`, and
  `2ms`; the slow events came from the warmup group.
- Warm repeated groups were dominated by `block_store_get_range`,
  `gateway_limiter`, `ipfs_path_parse`, `mime_detect`, `mime_total`,
  `name_cache`, `name_resolve`, and `request_done`.
- Across the whole traced command, direct bodies were used `124` times and
  streamed bodies `8` times.
- The warmup still fetched `40` blocks from HTTP providers and performed `35`
  delegated provider lookups; those cold/warmup costs are not part of the
  measured warm TTFB gap but remain the cold-load optimization target.

Decision:
Use this as a current warm-path comparison target. Rust is close but still not
beating Kubo on same-daemon warm TTFB; Kubo's margin is only a few milliseconds,
while Rust uses much less memory and far fewer FDs. The next warm-path
optimization should focus on reducing repeated per-request overhead in the
already-cached path without increasing RSS meaningfully.

## 2026-05-06 Keep: Add ipfs.tech CID-Direct Page-Assets Control

Question:
How much of the warm `ipfs-tech-page-assets` gap is DNSLink/IPNS resolution
overhead, and how much remains when the same page is loaded through its
immutable resolved `/ipfs` root?

Implementation:

- Add opt-in corpus case `ipfs-tech-page-assets-cid-direct`.
- Use the current observed `ipfs.tech` DNSLink target:
  `/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/`.
- Keep the same page crawl shape as `ipfs-tech-page-assets`: `32` max assets,
  `min_assets=8`, same-origin only, CSS assets enabled, and no failed assets.

Focused validation:

```sh
cargo fmt --all --check
python3 -m json.tool tools/mobile-web-harness/corpus/mobile-web.json >/tmp/mobile-web-corpus-check.json
cargo test -p mobile-web-harness corpus_entries_can_be_explicit_only -- --nocapture
```

Focused result:

- Formatting passed.
- Corpus JSON parsed successfully.
- Explicit-only corpus test passed.

Live same-daemon warm comparison:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-cid-direct-warm.db \
  /tmp/freedom-ipfs-ipfs-tech-cid-direct-warm.db-* \
  /tmp/ipfs-tech-cid-direct-warm-rust-trace.jsonl \
  /tmp/ipfs-tech-cid-direct-warm-rust-vs-kubo.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-cid-direct-warm.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-warm-rust-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cid-direct-warm-rust-vs-kubo.json
```

Artifacts:

- `/tmp/ipfs-tech-cid-direct-warm-rust-vs-kubo.json`
- `/tmp/ipfs-tech-cid-direct-warm-rust-trace.jsonl` (`1987` lines)
- `/tmp/freedom-ipfs-ipfs-tech-cid-direct-warm.db`
- `/tmp/freedom-ipfs-ipfs-tech-cid-direct-warm.db-wal`

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `42/48/48ms`.
- Kubo run total p50/p95/max: `32/40/40ms`.
- Rust root TTFB p50/p95/max: `3/5/5ms`.
- Kubo root TTFB p50/p95/max: `2/7/7ms`.
- Rust asset TTFB p50/p95/max: `4/8/11ms`.
- Kubo asset TTFB p50/p95/max: `2/6/9ms`.
- Ratios: root p50 `1.50x` in Kubo's favor, root p95 `0.71x` in Rust's
  favor, asset p50 `2.00x` in Kubo's favor, asset p95 `1.33x` in Kubo's favor.
- Rust max RSS/FD: `47284KiB` / `25`.
- Kubo max RSS/FD: `240640KiB` / `156`.
- Rust used `0.20x` Kubo RSS and `0.16x` Kubo FD count.
- Rust storage max: `2480056B`; Kubo storage max: `824420B`.

Trace notes:

- The CID-direct measured request groups remove the repeated `name_cache` /
  `name_resolve` phases seen in the `/ipns` version.
- Warm measured groups were dominated by cached path/resource work:
  `block_store_get_range`, `gateway_limiter`, `ipfs_path_parse`,
  `mime_detect`, `mime_total`, `request_done`, `request_start`, and
  `unixfs_file_size`.
- The warmup still performed `35` delegated provider lookups and `40` HTTP
  provider block fetch totals; these remain cold/warmup costs, not the measured
  warm gap.

Decision:
Keep the CID-direct control. It shows that DNSLink/IPNS resolution is part of
the warm root gap, but cached asset requests still trail Kubo even without name
resolution. The next warm-path experiment should target repeated cached
UnixFS/gateway work for asset requests rather than only name resolution.

## 2026-05-06 Keep: Bounded Small Direct-Body Cache for Warm Assets

Question:
Can a small process-local cache remove repeated UnixFS range reads for hot
small JS/CSS/SVG assets without meaningfully increasing mobile resource usage?

Implementation:

- Add a byte-bounded gateway small-body cache for full non-HEAD, non-range file
  responses up to one gateway chunk (`64KiB`).
- Default cache budget: `2MiB`, configurable with
  `--small-body-cache-max-bytes`; `0` disables the cache.
- Cache key: `(file_cid, len)`, so cached bodies remain content-addressed.
- Range responses, HEAD, and larger streamed bodies continue through the
  existing paths.
- Gateway traces now emit `gateway_small_body_cache` hit/miss/insert/eviction
  counters, and the mobile web harness reports aggregate cache stats.

Focused validation before live run:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-gateway small_body_cache -- --nocapture
cargo test -p freedom-ipfs-gateway gateway_reuses_small_direct_body_cache_for_repeated_assets -- --nocapture
cargo test -p mobile-web-harness trace_summary_includes_slowest_events_with_details -- --nocapture
cargo check -p freedom-ipfs-gateway --all-targets
cargo check -p mobile-web-harness --all-targets
cargo test -p mobile-web-harness offline_replay_summary -- --nocapture
```

Focused result:
All focused checks passed.

Disabled-cache baseline:

```sh
rm -f /tmp/freedom-ipfs-cid-direct-no-body-cache.db \
  /tmp/freedom-ipfs-cid-direct-no-body-cache.db-* \
  /tmp/ipfs-tech-cid-direct-no-body-cache-r5.json \
  /tmp/ipfs-tech-cid-direct-no-body-cache-r5-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-cid-direct-no-body-cache.db \
  --small-body-cache-max-bytes 0 \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-no-body-cache-r5-trace.jsonl \
  --output /tmp/ipfs-tech-cid-direct-no-body-cache-r5.json
```

Enabled-cache run:

```sh
rm -f /tmp/freedom-ipfs-cid-direct-small-body-cache.db \
  /tmp/freedom-ipfs-cid-direct-small-body-cache.db-* \
  /tmp/ipfs-tech-cid-direct-small-body-cache-r5.json \
  /tmp/ipfs-tech-cid-direct-small-body-cache-r5-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-cid-direct-small-body-cache.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-small-body-cache-r5-trace.jsonl \
  --output /tmp/ipfs-tech-cid-direct-small-body-cache-r5.json
```

Artifacts:

- `/tmp/ipfs-tech-cid-direct-no-body-cache-r5.json`
- `/tmp/ipfs-tech-cid-direct-no-body-cache-r5-trace.jsonl` (`3029` lines)
- `/tmp/ipfs-tech-cid-direct-small-body-cache-r5.json`
- `/tmp/ipfs-tech-cid-direct-small-body-cache-r5-trace.jsonl` (`2781` lines)

Result:

- Both runs passed `5/5`.
- Disabled run total p50/p95/max: `33/51/51ms`.
- Enabled run total p50/p95/max: `23/55/55ms`.
- Disabled root TTFB p50/p95/max: `3/4/4ms`.
- Enabled root TTFB p50/p95/max: `2/5/5ms`.
- Disabled asset TTFB p50/p95/max: `4/7/10ms` over `160` asset requests.
- Enabled asset TTFB p50/p95/max: `2/7/13ms` over `160` asset requests.
- Disabled asset total p50/p95/max: `4/7/10ms`.
- Enabled asset total p50/p95/max: `3/7/13ms`.
- Disabled RSS/FD max: `45632KiB` / `26`.
- Enabled RSS/FD max: `47340KiB` / `27`.
- Enabled cache trace: `175` cache events, `125` hits, `25` misses, `25`
  inserts, `0` evictions, `1022365` bytes served from the small-body cache,
  max cached body `61741` bytes, max cache occupancy `25` entries /
  `204473` bytes.

Same-window Kubo comparison with the cache enabled:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo.db \
  /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo.db-* \
  /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3.json \
  /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3.json
```

Artifacts:

- `/tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3.json`
- `/tmp/ipfs-tech-cid-direct-small-body-cache-kubo-r3-trace.jsonl` (`2042`
  lines)

Comparison result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `44/65/65ms`.
- Kubo run total p50/p95/max: `30/32/32ms`.
- Rust root TTFB p50/p95/max: `4/4/4ms`.
- Kubo root TTFB p50/p95/max: `2/2/2ms`.
- Rust asset TTFB p50/p95/max: `4/7/8ms`.
- Kubo asset TTFB p50/p95/max: `2/3/4ms`.
- Rust max RSS/FD: `47376KiB` / `29`.
- Kubo max RSS/FD: `285492KiB` / `187`.
- Rust used `0.17x` Kubo RSS and `0.16x` Kubo FD count.
- Rust trace cache stats: `125` cache events, `75` hits, `25` misses, `25`
  inserts, `0` evictions, `613419` bytes served from the small-body cache, max
  cache occupancy `25` entries / `204473` bytes.

No-trace same-window Kubo comparison:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo-notrace.db \
  /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo-notrace.db-* \
  /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-notrace-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-cid-direct-small-body-cache-kubo-notrace.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --comparison-output /tmp/ipfs-tech-cid-direct-small-body-cache-kubo-notrace-r3.json
```

Artifact:

- `/tmp/ipfs-tech-cid-direct-small-body-cache-kubo-notrace-r3.json`

No-trace result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `27/31/31ms`.
- Kubo run total p50/p95/max: `29/39/39ms`.
- Rust root TTFB p50/p95/max: `1/2/2ms`.
- Kubo root TTFB p50/p95/max: `2/2/2ms`.
- Rust asset TTFB p50/p95/max: `2/3/5ms`.
- Kubo asset TTFB p50/p95/max: `2/6/9ms`.
- Rust asset total p50/p95/max: `2/3/5ms`.
- Kubo asset total p50/p95/max: `2/6/11ms`.
- Rust max RSS/FD: `53680KiB` / `35`.
- Kubo max RSS/FD: `127376KiB` / `85`.
- Rust used `0.42x` Kubo RSS and `0.41x` Kubo FD count.

Rust-only no-trace warm run:

```sh
rm -f /tmp/freedom-ipfs-cid-direct-small-body-cache-notrace.db \
  /tmp/freedom-ipfs-cid-direct-small-body-cache-notrace.db-* \
  /tmp/ipfs-tech-cid-direct-small-body-cache-notrace-r5.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-cid-direct-small-body-cache-notrace.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-cid-direct-small-body-cache-notrace-r5.json
```

Artifact:

- `/tmp/ipfs-tech-cid-direct-small-body-cache-notrace-r5.json`

Rust-only no-trace result:

- Rust passed `5/5`.
- Run total p50/p95/max: `27/32/32ms`.
- Root TTFB p50/p95/max: `1/2/2ms`.
- Asset TTFB p50/p95/max: `2/3/4ms` over `160` asset requests.
- Asset total p50/p95/max: `2/3/5ms`.
- RSS/FD max: `54608KiB` / `36`.

Observation:
Compared with the traced enabled-cache run, no-trace asset p50 stayed at `2ms`
but asset p95 improved from `7ms` to `3ms` and max from `13ms` to `4ms`. The
remaining production hot-path gap is small; trace overhead and trace-induced
tail movement are now a first-class measurement concern.

Final validation:

```sh
cargo fmt --all --check
git diff --check
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-gateway --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets
```

Final validation result:
All commands passed. Gateway tests covered cache reuse and byte-budget eviction;
mobile harness tests covered the new trace aggregate.

Decision:
Keep. This is a targeted warm-path improvement for repeated cached page assets:
asset p50 improved from `4ms` to `2ms`, total run p50 improved from `33ms` to
`23ms`, and the observed cache footprint was only about `200KiB` for the
`ipfs.tech` CID-direct corpus. The traced same-window comparison still showed
Kubo ahead, but the no-trace comparison showed Rust matching or beating Kubo on
warm root and asset reads while using less RSS and fewer FDs. Follow-up work
should separate production hot-path latency from trace overhead before adding
more gateway cache layers; trace sampling or cheaper trace aggregation may be
higher leverage than another read-path cache.

## 2026-05-06 Keep: Make Full Trace Span Lists Opt-In

Question:
Can we reduce trace-induced latency and JSONL volume without losing the current
mobile harness request-correlation summaries?

Implementation:

- Keep `span` on gateway JSON events by default, since the harness parser uses
  it for request grouping and slow-event details.
- Stop emitting the duplicate full `spans` stack by default.
- Add gateway and harness CLI flag `--trace-span-list` for the rare cases where
  full span-stack output is needed.
- Add `trace_span_list` to harness JSON reports when trace output is enabled, so
  future artifacts make the trace shape explicit.

No-span-list traced run:

```sh
rm -f /tmp/freedom-ipfs-cid-direct-trace-no-span-list.db \
  /tmp/freedom-ipfs-cid-direct-trace-no-span-list.db-* \
  /tmp/ipfs-tech-cid-direct-trace-no-span-list-r5.json \
  /tmp/ipfs-tech-cid-direct-trace-no-span-list-r5-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-cid-direct-trace-no-span-list.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-trace-no-span-list-r5-trace.jsonl \
  --output /tmp/ipfs-tech-cid-direct-trace-no-span-list-r5.json
```

Span-list traced run:

```sh
rm -f /tmp/freedom-ipfs-cid-direct-trace-span-list.db \
  /tmp/freedom-ipfs-cid-direct-trace-span-list.db-* \
  /tmp/ipfs-tech-cid-direct-trace-span-list-r5.json \
  /tmp/ipfs-tech-cid-direct-trace-span-list-r5-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --gateway-db /tmp/freedom-ipfs-cid-direct-trace-span-list.db \
  --case ipfs-tech-page-assets-cid-direct \
  --warmup-runs 1 \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cid-direct-trace-span-list-r5-trace.jsonl \
  --trace-span-list \
  --output /tmp/ipfs-tech-cid-direct-trace-span-list-r5.json
```

Artifacts:

- `/tmp/ipfs-tech-cid-direct-trace-no-span-list-r5.json`
- `/tmp/ipfs-tech-cid-direct-trace-no-span-list-r5-trace.jsonl` (`2817`
  lines, `1775630` bytes)
- `/tmp/ipfs-tech-cid-direct-trace-span-list-r5.json`
- `/tmp/ipfs-tech-cid-direct-trace-span-list-r5-trace.jsonl` (`2825` lines,
  `2723594` bytes)

Result:

- Both runs passed `5/5`.
- No-span-list trace output averaged about `630` bytes/line.
- Span-list trace output averaged about `964` bytes/line.
- For this sample, dropping `spans` reduced trace bytes by about `35%`.
- No-span-list run total p50/p95/max: `28/41/41ms`.
- Span-list run total p50/p95/max: `46/60/60ms`.
- No-span-list root TTFB p50/p95/max: `2/3/3ms`.
- Span-list root TTFB p50/p95/max: `4/5/5ms`.
- No-span-list asset TTFB p50/p95/max: `2/6/7ms`.
- Span-list asset TTFB p50/p95/max: `4/8/11ms`.
- No-span-list asset total p50/p95/max: `2/6/7ms`.
- Span-list asset total p50/p95/max: `4/8/47ms`.
- The no-span-list trace still parsed successfully and preserved progress
  request groups, slow-event details, cache counters, Bitswap summaries, and
  provider summaries.

Focused validation before final gate:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness args_accept_trace_span_list_flag -- --nocapture
cargo check -p mobile-web-harness --all-targets
cargo check -p freedom-ipfs-gateway --all-targets
```

Focused result:
All focused checks passed.

Final validation:

```sh
cargo fmt --all --check
git diff --check
cargo test -p freedom-ipfs-gateway
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-gateway --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets
```

Final validation result:
All commands passed.

Decision:
Keep. The duplicate span-stack field was a material trace payload multiplier,
and the harness does not need it for normal mobile-web summaries. This does not
change production/no-trace behavior, keeps request correlation intact for traced
runs, and leaves an explicit `--trace-span-list` escape hatch for deeper tracing.

## 2026-05-06 Observe: No-Trace Warm `/ipns/ipfs.tech/` vs Kubo

Question:
After the small-body cache and trace-span-list cleanup, where does the real
`/ipns/ipfs.tech/` warm same-daemon page workload stand against Kubo when trace
output is disabled?

Command:

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-small-body-cache-kubo-notrace.db \
  /tmp/freedom-ipfs-ipfs-tech-small-body-cache-kubo-notrace.db-* \
  /tmp/ipfs-tech-small-body-cache-kubo-notrace-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-small-body-cache-kubo-notrace.db \
  --case ipfs-tech-page-assets \
  --warmup-runs 1 \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --comparison-output /tmp/ipfs-tech-small-body-cache-kubo-notrace-r3.json
```

Artifact:

- `/tmp/ipfs-tech-small-body-cache-kubo-notrace-r3.json`

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `27/28/28ms`.
- Kubo run total p50/p95/max: `22/35/35ms`.
- Rust root TTFB p50/p95/max: `1/2/2ms`.
- Kubo root TTFB p50/p95/max: `1/2/2ms`.
- Rust asset TTFB p50/p95/max: `2/3/5ms`.
- Kubo asset TTFB p50/p95/max: `1/6/8ms`.
- Rust asset total p50/p95/max: `2/3/5ms`.
- Kubo asset total p50/p95/max: `1/6/8ms`.
- Rust max RSS/FD: `52732KiB` / `32`.
- Kubo max RSS/FD: `213520KiB` / `103`.
- Rust used `0.25x` Kubo RSS and `0.31x` Kubo FD count.

Decision:
Use this as the current production-style warm `ipfs.tech` baseline. The older
traced comparison made the warm gap look larger than it is. With trace disabled,
Rust matches Kubo on root TTFB, loses only the asset p50 by `1ms`, beats Kubo's
asset p95, and keeps a much lower RSS/FD profile. The next speed work should
focus on cold load reliability/tails, progress API UX, or tracing overhead
rather than further micro-optimizing this warm same-daemon page path.

## 2026-05-06 Keep: Map Small-Body Cache Events To Stable Progress Phases

Question:
After adding `gateway_small_body_cache` traces, do mobile progress snapshots and
harness progress summaries still expose only stable UI-facing phases?

Implementation:

- Map `gateway_small_body_cache cache_hit=true` to `cache_hit` with
  `source="cache"` in the mobile progress recorder.
- Map `gateway_small_body_cache cache_hit=false` to `checking_cache`.
- Map small-body cache insert bookkeeping to `streaming` so the internal
  `gateway_small_body_cache` raw phase does not leak into UI-facing phase
  counts.
- Apply the same mapping in the mobile web harness trace progress summary.
- Update `docs/mobile-progress-api.md` to document the small-body-cache mapping.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states -- --nocapture
cargo test -p mobile-web-harness trace_summary_counts_gateway_small_body_cache -- --nocapture
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-mobile --all-targets -- -D warnings
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets
```

Result:

- Focused progress mapping tests passed.
- Full `freedom-ipfs-mobile` tests passed: `27` tests.
- Full `mobile-web-harness` tests passed: `43` tests.
- Clippy and workspace check passed.

Decision:
Keep. This is small progress API polish tied to the new gateway cache behavior.
Swift and harness summaries now see cache work as `checking_cache`, `cache_hit`,
or `streaming` instead of a new internal phase that was not in the stable mobile
phase list.

## 2026-05-06 Observe: Cold Empty-Store `ipfs.tech` vs Kubo

Question:
After the warm-path cache work, where does the current branch stand on true
cold `ipfs.tech` page loads against Kubo, with trace output disabled and each
measured run starting from an empty Rust in-memory store and a fresh Kubo repo?

Fair empty-store command:

```sh
rm -f /tmp/ipfs-tech-cold-empty-store-kubo-notrace-r3.json

timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --comparison-output /tmp/ipfs-tech-cold-empty-store-kubo-notrace-r3.json
```

Artifact:

- `/tmp/ipfs-tech-cold-empty-store-kubo-notrace-r3.json`

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust run total p50/p95/max: `2846/2995/2995ms`.
- Kubo run total p50/p95/max: `5841/10217/10217ms`.
- Rust root TTFB p50/p95/max: `1014/1249/1249ms`.
- Kubo root TTFB p50/p95/max: `2529/4810/4810ms`.
- Rust asset TTFB p50/p95/max: `250/814/1087ms` over `96` asset requests.
- Kubo asset TTFB p50/p95/max: `149/7894/8132ms`.
- Rust asset total p50/p95/max: `251/814/1088ms`.
- Kubo asset total p50/p95/max: `149/7894/8133ms`.
- Rust max RSS/FD: `48124KiB` / `25`.
- Kubo max RSS/FD: `267016KiB` / `225`.
- Kubo max repo size: `832195B`; Rust used the default in-memory store for
  this comparison.

Control note:
A second run used `--gateway-db` with `--fresh-gateway-per-run`, so Rust's store
persisted across measured daemon restarts while Kubo still used fresh repos. It
is useful as a "persistent Rust cache, fresh daemon" observation, but it is not
the fair cold baseline above.

```sh
rm -f /tmp/freedom-ipfs-ipfs-tech-cold-current-kubo-notrace.db \
  /tmp/freedom-ipfs-ipfs-tech-cold-current-kubo-notrace.db-* \
  /tmp/ipfs-tech-cold-current-kubo-notrace-r3.json

timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --fresh-gateway-per-run \
  --gateway-db /tmp/freedom-ipfs-ipfs-tech-cold-current-kubo-notrace.db \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --comparison-output /tmp/ipfs-tech-cold-current-kubo-notrace-r3.json
```

Control artifact:

- `/tmp/ipfs-tech-cold-current-kubo-notrace-r3.json`

Control result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Rust root TTFB p50/p95/max: `12/1185/1185ms`.
- Kubo root TTFB p50/p95/max: `2204/2788/2788ms`.
- Rust asset TTFB p50/p95/max: `6/254/992ms`.
- Kubo asset TTFB p50/p95/max: `101/596/777ms`.
- Rust max RSS/FD: `51636KiB` / `28`.
- Kubo max RSS/FD: `160900KiB` / `140`.
- Rust max cache size: `2274056B`; Kubo max repo size: `834127B`.

Traced Rust-only diagnostic command:

```sh
rm -f /tmp/ipfs-tech-cold-empty-store-rust-trace-r3.json \
  /tmp/ipfs-tech-cold-empty-store-rust-trace-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-cold-empty-store-rust-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-cold-empty-store-rust-trace-r3.json
```

Traced artifacts:

- `/tmp/ipfs-tech-cold-empty-store-rust-trace-r3.json`
- `/tmp/ipfs-tech-cold-empty-store-rust-trace-r3-trace.jsonl` (`3000` lines)

Traced result:

- Rust passed `3/3`.
- Run total p50/p95/max: `2101/2485/2485ms`.
- Root TTFB p50/p95/max: `769/779/779ms`.
- Asset TTFB p50/p90/p95/max: `235/420/500/849ms`.
- `block_fetch_total`: `120` events, all `http_provider`, p50/p95/max
  `214/366/605ms`.
- `http_provider_race_result`: `105` events, p50/p95/max `185/354/547ms`.
- `http_provider_fetch`: `105` events, p50/p95/max `161/265/459ms`.
- `provider_lookup`: `105` events, p50/p95/max `19/49/111ms`.
- `delegated_provider_lookup`: `105` events, `1902` providers and `189`
  HTTP providers observed, p50/p95/max `18/49/110ms`.
- Delegated lookup yielded one HTTP provider for `63` block events and multiple
  HTTP providers for `42` events.
- Single-provider HTTP-provider races were slower than multi-provider races:
  single-provider success p50/p95/max `225/426/547ms`; multi-provider success
  p50/p95/max `87/189/203ms`.
- The only single-provider winner was `https://ipfs-bridge.sia.dev/` for `63`
  events. Direct successful fetch latency by endpoint was
  `https://ipfs-bridge.sia.dev/` p50/p95/max `171/319/459ms` and
  `https://dag.w3s.link/` p50/p95/max `39/74/88ms`.
- Small-body cache behavior in true cold runs was expectedly insert-only:
  `75` misses, `75` inserts, `0` hits, max `25` entries / `204473` bytes.

Decision:
Keep as the current fair cold baseline. The current Rust gateway is already
faster than Kubo on root startup, full page time, asset p95, RSS, and FD count
for this live sample. Kubo still wins asset p50. The trace says provider lookup
is not the hot path; cold asset median is mostly bound by verified block fetches
from HTTP providers, especially single-provider `ipfs-bridge.sia.dev` blocks.
The next speed work should target cold asset median without increasing mobile
resource pressure: small bounded DAG/session prefetch, Bitswap multi-want or
session batching, or selective provider diversity for single slow HTTP-provider
blocks.

## 2026-05-06 Experiment: Opt-In Single-HTTP Bitswap Hedge

Question:
The cold empty-store trace showed that blocks with only one HTTP provider were
slower than blocks with multiple HTTP providers, and most of those single-provider
winners were `https://ipfs-bridge.sia.dev/`. Can a narrower Bitswap hedge improve
cold asset median without taking the broad Bitswap pressure hit from the rejected
2026-05-05 single-provider race?

Implementation:

- Add opt-in env var `FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE`.
- Default behavior is unchanged.
- When exactly one HTTP provider base is available and at least one provider has
  a peer ID plus a non-HTTP multiaddr, race one verified HTTP provider fetch
  against a delayed Bitswap fetch.
- The Bitswap hedge starts after `150ms`.
- The experiment avoids also self-hedging the same single HTTP provider, so the
  worst case is one HTTP attempt plus one Bitswap attempt, not two HTTP attempts
  plus Bitswap.
- Blocks are still verified before serving or caching. This is not a public
  gateway fallback.
- Add trace phases `http_provider_bitswap_hedge` and
  `http_provider_bitswap_hedge_result`, and map them to stable mobile/harness
  progress phases.

Validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval bitswap_hedge_can_win_against_slow_single_http_provider -- --nocapture
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states -- --nocapture
cargo test -p mobile-web-harness trace_summary_derives_mobile_progress_phases -- --nocapture
cargo test -p freedom-ipfs-retrieval
cargo test -p freedom-ipfs-mobile
cargo test -p mobile-web-harness
cargo clippy -p freedom-ipfs-retrieval -p freedom-ipfs-mobile -p mobile-web-harness --all-targets -- -D warnings
```

Result:

- Focused hedge test passed: a local Bitswap peer beat a deliberately slow
  single HTTP provider, the served block matched the expected CID, and only one
  HTTP request was issued.
- Full retrieval tests passed: `75` passed, `1` ignored.
- Full mobile tests passed: `27` passed.
- Full harness tests passed: `43` passed.
- Focused clippy passed for the touched crates/tools.

Disabled no-trace command:

```sh
rm -f /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-r3.json
```

Enabled no-trace command:

```sh
rm -f /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-r3.json

FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-r3.json
```

No-trace artifacts:

- `/tmp/ipfs-tech-single-http-bitswap-hedge-disabled-r3.json`
- `/tmp/ipfs-tech-single-http-bitswap-hedge-enabled-r3.json`

No-trace results:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Disabled | `3/3` | `3137/4566/4566ms` | `1381/1654/1654ms` | `239/962/1077ms` | `47432KiB` / `23` |
| Enabled | `3/3` | `3045/3648/3648ms` | `1224/1254/1254ms` | `229/880/1344ms` | `55140KiB` / `38` |

Traced disabled command:

```sh
rm -f /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3.json \
  /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3.json
```

Traced enabled command:

```sh
rm -f /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3.json \
  /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3-trace.jsonl

FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3.json
```

Traced artifacts:

- `/tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3.json`
- `/tmp/ipfs-tech-single-http-bitswap-hedge-disabled-trace-r3-trace.jsonl`
- `/tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3.json`
- `/tmp/ipfs-tech-single-http-bitswap-hedge-enabled-trace-r3-trace.jsonl`

Traced results:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Disabled | `3/3` | `2376/2490/2490ms` | `796/847/847ms` | `245/558/750ms` | `48228KiB` / `25` |
| Enabled | `3/3` | `2650/2849/2849ms` | `719/1234/1234ms` | `195/675/818ms` | `55732KiB` / `49` |

Trace findings:

- Disabled trace had `3003` events and `120` `block_fetch_total` events, all
  from `http_provider`.
- Enabled trace had `3522` events, `25` `http_provider_bitswap_hedge` starts,
  and `25` `http_provider_bitswap_hedge_result` events.
- Enabled source mix moved to `68` Bitswap blocks and `51` HTTP-provider blocks.
- Enabled source latency was Bitswap p50/p95/max `143/291/497ms` and
  HTTP-provider p50/p95/max `288/575/595ms`.
- The hedge can move work to faster Bitswap in a favorable window, but it raises
  RSS by roughly `7MiB` and FD count by `24` in the traced comparison, and it
  worsened traced p95/max page and asset latency.

Decision:
Keep the implementation only as an opt-in lab knob. Do not enable it by default.
The narrower hedge gives useful diagnostic leverage and can improve asset p50,
but the current trigger is still too broad for mobile production because it
raises connection pressure and does not improve p95/max consistently. Future work
should retune this around stricter signals, for example a slow-provider score,
recent peer success, lower per-page Bitswap caps, or a later hedge delay.

## 2026-05-06 Experiment: Score-Gated Single-HTTP Bitswap Hedge

Question:
The all-or-nothing opt-in Bitswap hedge was too broad for mobile production. Can
the same lab knob become safer by firing only after the sole HTTP provider has a
remembered slow EWMA score?

Implementation:

- Add optional env var
  `FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS`.
- This env only matters when
  `FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1` is also set.
- When the min-score env is set, the Bitswap hedge is skipped until the single
  HTTP provider has a non-expired score at or above the configured threshold.
- Skips emit `http_provider_bitswap_hedge_skip` with the provider score,
  threshold, and skip reason.
- Mobile progress and harness summaries map the skip phase to
  `fetching_http_provider` so the raw experimental diagnostic does not become a
  new UI state.
- `docs/mobile-progress-api.md` documents the raw-to-stable phase mapping.

No-trace disabled control:

```sh
rm -f /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-r3.json
```

No-trace enabled command:

```sh
rm -f /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-r3.json

FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1 \
FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS=250 \
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-r3.json
```

No-trace artifacts:

- `/tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-r3.json`
- `/tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-r3.json`

No-trace result:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Disabled | `3/3` | `2873/4233/4233ms` | `1177/1485/1485ms` | `236/910/1122ms` | `48372KiB` / `23` |
| Score-gated | `3/3` | `2466/2589/2589ms` | `1062/1155/1155ms` | `227/519/689ms` | `54132KiB` / `39` |

Traced disabled control:

```sh
rm -f /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3.json \
  /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3.json
```

Traced enabled command:

```sh
rm -f /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3.json \
  /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl

FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1 \
FREEDOM_IPFS_SINGLE_HTTP_BITSWAP_HEDGE_MIN_SCORE_MS=250 \
timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3.json
```

Traced artifacts:

- `/tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3.json`
- `/tmp/ipfs-tech-score-gated-bitswap-hedge-disabled-trace-r3-trace.jsonl`
- `/tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3.json`
- `/tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl`

Traced result:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Disabled | `3/3` | `2138/2975/2975ms` | `713/918/918ms` | `255/539/689ms` | `46584KiB` / `26` |
| Score-gated | `3/3` | `1931/2393/2393ms` | `534/564/564ms` | `229/498/713ms` | `53976KiB` / `38` |

Trace findings:

- Disabled trace had `3004` events and all `120` block fetches came from
  `http_provider`.
- Score-gated trace had `3108` events, `4`
  `http_provider_bitswap_hedge` starts, and `4`
  `http_provider_bitswap_hedge_result` events.
- Score-gated trace still had all `120` block fetches from `http_provider`.
  Bitswap attempted `3` commands, established `5` connections, and all Bitswap
  fetches were cancelled after HTTP won.
- Score-gated single-provider result latency improved from p50/p95/max
  `233/510/644ms` to `216/368/662ms`, but the max did not improve.
- Resource cost remained visible: traced max RSS rose by about `7MiB`, and max
  FD count rose from `26` to `38`.

Decision:
Keep the score-gate as an opt-in lab control, but do not enable or recommend the
`250ms` threshold as production behavior. The no-trace sample looked good, but
the trace shows the observed improvement was not due to Bitswap serving blocks.
This is useful for future controlled experiments because it bounds the broad
hedge behind provider scoring and emits skip diagnostics, but the next production
candidate should require actual recent Bitswap success or a stronger per-page
resource budget before starting extra peer work.

## 2026-05-06 Keep: Summarize HTTP/Bitswap Hedge Outcomes In Harness

Question:
The score-gated Bitswap hedge experiment proved that start/result/skip events are
the key signal for deciding whether extra peer work is useful, but the harness
only exposed those events indirectly through generic phase and progress counts.
Can the trace summary report those outcomes directly for future A/B runs?

Implementation:

- Extend `trace_summary.http_provider_races` with:
  - `bitswap_hedges`
  - `max_bitswap_hedge_timeout_ms`
  - `bitswap_hedge_results`
  - `bitswap_hedge_result_elapsed_ms`
  - `bitswap_hedge_result_sources`
  - `bitswap_hedge_skips`
  - `bitswap_hedge_skip_reasons`
- Print a concise `bitswap hedge:` line under `http provider races`.
- Preserve the existing stable progress phase mapping:
  - start/result from Bitswap source => `fetching_bitswap`
  - HTTP result/skip => `fetching_http_provider`

Focused validation:

```sh
cargo fmt --all --check
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches -- --nocapture
cargo test -p mobile-web-harness
```

Result:

- Focused HTTP-provider trace summary test passed.
- Full `mobile-web-harness` tests passed: `43` tests.

Existing score-gated trace artifact check:

```sh
rg -c '"phase":"http_provider_bitswap_hedge"' \
  /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl

rg -c '"phase":"http_provider_bitswap_hedge_result"' \
  /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl

rg -c '"phase":"http_provider_bitswap_hedge_skip"' \
  /tmp/ipfs-tech-score-gated-bitswap-hedge-enabled-trace-r3-trace.jsonl
```

Observed counts:

- Starts: `4`
- Results: `4`
- Skips: `59`
- Result sources: `http_provider=4`
- Skip reasons: `provider_score_below_threshold=56`,
  `provider_unscored=3`

Decision:
Keep. This is diagnostics-only and does not change gateway behavior. It makes
future single-provider mitigation experiments easier to judge: a promising run
should show Bitswap result sources or clearly bounded skip behavior, not only a
better latency sample in a favorable network window.

## 2026-05-06 Keep: Lower Single-HTTP Self-Hedge Default To 200ms

Question:
After the 250ms single HTTP-provider self-hedge proved useful and
resource-light, is 250ms still the right default? The focused `ipfs.tech` page
still has an HTTP-provider asset tail, and a lower delay may trim that tail
without adding Bitswap peer pressure.

Implementation:

- Lower `SINGLE_HTTP_PROVIDER_SELF_HEDGE_AFTER` from `250ms` to `200ms`.
- Add lab override `FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS` so future
  agents can run 150/200/250ms A/B samples without code edits.
- Keep `FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE` as the production kill
  switch.
- Keep the hedge scoped to the same single HTTP provider, so it does not add new
  transport surface or extra providers. Blocks are still verified before serving
  or caching.

No-trace commands:

```sh
rm -f /tmp/ipfs-tech-self-hedge-delay250-default-r3.json

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-self-hedge-delay250-default-r3.json

rm -f /tmp/ipfs-tech-self-hedge-delay200-r3.json

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS=200 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-self-hedge-delay200-r3.json

rm -f /tmp/ipfs-tech-self-hedge-delay150-r3.json

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS=150 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-self-hedge-delay150-r3.json
```

No-trace artifacts:

- `/tmp/ipfs-tech-self-hedge-delay250-default-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay200-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay150-r3.json`

No-trace result:

| Delay | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| `250ms` | `3/3` | `2547/5113/5113ms` | `1019/2258/2258ms` | `280/955/1320ms` | `46976KiB` / `26` |
| `200ms` | `3/3` | `2173/2443/2443ms` | `563/647/647ms` | `240/649/932ms` | `47484KiB` / `24` |
| `150ms` | `3/3` | `2344/2387/2387ms` | `679/733/733ms` | `272/462/588ms` | `47440KiB` / `24` |

Traced commands:

```sh
rm -f /tmp/ipfs-tech-self-hedge-delay250-default-trace-r3.json \
  /tmp/ipfs-tech-self-hedge-delay250-default-trace-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-delay250-default-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-delay250-default-trace-r3.json

rm -f /tmp/ipfs-tech-self-hedge-delay200-trace-r3.json \
  /tmp/ipfs-tech-self-hedge-delay200-trace-r3-trace.jsonl

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS=200 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-delay200-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-delay200-trace-r3.json

rm -f /tmp/ipfs-tech-self-hedge-delay150-trace-r3.json \
  /tmp/ipfs-tech-self-hedge-delay150-trace-r3-trace.jsonl

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS=150 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-delay150-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-delay150-trace-r3.json
```

Traced artifacts:

- `/tmp/ipfs-tech-self-hedge-delay250-default-trace-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay250-default-trace-r3-trace.jsonl`
- `/tmp/ipfs-tech-self-hedge-delay200-trace-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay200-trace-r3-trace.jsonl`
- `/tmp/ipfs-tech-self-hedge-delay150-trace-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay150-trace-r3-trace.jsonl`

Traced result:

| Delay | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Self-hedges | Race result p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `250ms` | `3/3` | `2160/2410/2410ms` | `718/830/830ms` | `237/565/1104ms` | `25` | `188/400/830ms` | `47144KiB` / `25` |
| `200ms` | `3/3` | `2264/2457/2457ms` | `764/819/819ms` | `249/522/702ms` | `44` | `190/425/513ms` | `47352KiB` / `27` |
| `150ms` | `3/3` | `2004/2530/2530ms` | `537/840/840ms` | `260/575/649ms` | `63` | `206/325/426ms` | `48924KiB` / `24` |

Additional trace findings:

- `250ms`: `3005` trace lines, block fetch HTTP-provider p50/p95/max
  `214/404/879ms`, HTTP provider fetch p50/p95/max `161/296/817ms`,
  single-provider result p50/p95/max `225/462/830ms`, `attempted_max=3`.
- `200ms`: `3023` trace lines, block fetch HTTP-provider p50/p95/max
  `211/453/538ms`, HTTP provider fetch p50/p95/max `161/293/512ms`,
  single-provider result p50/p95/max `251/453/513ms`, `attempted_max=2`.
- `150ms`: `3042` trace lines, block fetch HTTP-provider p50/p95/max
  `231/357/448ms`, HTTP provider fetch p50/p95/max `163/251/311ms`,
  single-provider result p50/p95/max `248/332/426ms`, `attempted_max=2`.

Guardrail command with the new default `200ms` and no override:

```sh
rm -f /tmp/self-hedge200-guardrails-r3.json /tmp/self-hedge200-guardrails-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/self-hedge200-guardrails-r3-trace.jsonl \
  --output /tmp/self-hedge200-guardrails-r3.json
```

Guardrail artifacts:

- `/tmp/self-hedge200-guardrails-r3.json`
- `/tmp/self-hedge200-guardrails-r3-trace.jsonl`

Guardrail result:

- Overall: passed `3/3`, run total p50/p95/max `512/620/620ms`, max RSS/FD
  `34548KiB` / `15`.
- `daicowtf-page-assets`: passed `3/3`, root TTFB p50/p95/max
  `357/395/395ms`.
- `vitalik-root-html-range`: passed `3/3`, root/range TTFB p50/p95/max
  `211/220/220ms`.
- Trace: `285` lines, `15` block fetches, all from HTTP providers.
- Delegated self-hedges: `0`; HTTP-provider self-hedges: `0`.

Same-window Rust/Kubo comparison with the new default `200ms`:

```sh
rm -f /tmp/ipfs-tech-self-hedge200-kubo-r3.json /tmp/ipfs-tech-self-hedge200-kubo-r3-trace.jsonl

timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge200-kubo-r3-trace.jsonl \
  --comparison-output /tmp/ipfs-tech-self-hedge200-kubo-r3.json
```

Kubo comparison artifacts:

- `/tmp/ipfs-tech-self-hedge200-kubo-r3.json`
- `/tmp/ipfs-tech-self-hedge200-kubo-r3-trace.jsonl`

Kubo comparison result:

- Rust and Kubo both passed `3/3`.
- Root TTFB p50/p95: Rust `565/580ms`; Kubo `1487/2558ms`.
- Asset TTFB p50/p95: Rust `190/576ms`; Kubo `104/670ms`.
- Max RSS/FD: Rust `53376KiB` / `26`; Kubo `191932KiB` / `114`.
- Rust/Kubo resource ratios: RSS `0.28x`, FD `0.23x`.
- Rust trace: `3062` lines; HTTP-provider self-hedges `21` with
  `self_hedge_timeout_max=200ms`; single-provider result p50/p95/max
  `189/298/340ms`.
- Rust block source mix: HTTP-provider `92` blocks, Bitswap `25` blocks, cache
  `2` blocks. The self-hedge only duplicated the same HTTP provider; the Bitswap
  blocks came from the normal retrieval/session path.

Decision:
Keep `200ms` as the new default. It keeps the guardrail cases fast and idle,
preserves the same-provider-only resource shape, and wins the same-window
`ipfs.tech` root p50/p95 and asset p95 against Kubo while using far less RSS and
FDs. Reject `150ms` as the default for now: it improves traced per-block max
latency, but it self-hedged every single-provider block in the focused page
sample (`63` self-hedges) and raised traced RSS to `48924KiB`. Keep the env
override for future longer runs; a later agent can revisit lower delays if it
adds a stronger per-page duplicate-request budget or provider-score trigger.

## 2026-05-06 Reject: 175ms Single-HTTP Self-Hedge Midpoint

Question:
After keeping `200ms` and rejecting `150ms`, does a midpoint `175ms` preserve
most of the 150ms block-tail benefit without self-hedging every single-provider
block?

Command:

```sh
rm -f /tmp/ipfs-tech-self-hedge-delay175-trace-r3.json /tmp/ipfs-tech-self-hedge-delay175-trace-r3-trace.jsonl

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_AFTER_MS=175 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-delay175-trace-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-delay175-trace-r3.json
```

Artifacts:

- `/tmp/ipfs-tech-self-hedge-delay175-trace-r3.json`
- `/tmp/ipfs-tech-self-hedge-delay175-trace-r3-trace.jsonl`

Result:

- Passed `3/3`.
- Run total p50/p95/max: `2919/4050/4050ms`.
- Root TTFB p50/p95/max: `827/1378/1378ms`.
- Asset TTFB p50/p95/max: `305/964/1093ms`.
- Max RSS/FD: `53288KiB` / `28`.
- Trace: `3132` lines, block sources `http_provider=114`, `bitswap=6`.
- HTTP-provider self-hedges: `49`, `self_hedge_timeout_max=175ms`.
- Single-provider result p50/p95/max: `270/718/968ms`.
- HTTP-provider fetch p50/p95/max: `160/623/682ms`.
- Delegated self-hedges: `1`, max `750ms`.

Decision:
Reject. In this same-window sample, `175ms` was worse than the kept `200ms`
trace on run total, root TTFB, asset p50/p95, single-provider result latency,
and RSS/FD. It also fired more duplicate same-provider requests than `200ms`
without the block-tail improvement seen at `150ms`. Keep `200ms` as the default
and move the next optimization effort away from raw self-hedge delay tuning.

## 2026-05-06 Keep: Trace Same-Provider Self-Hedge Winners

Question:
The harness could count same-provider HTTP self-hedge starts, but could not say
whether the original request or the duplicate request won. That made delay
tuning too indirect. Add explicit winner-attempt diagnostics, then use them to
decide whether more self-hedge tuning is worth doing.

Implementation:

- Add `winner_attempt_index` to `http_provider_race_result` events.
- Add `attempt_index=1` to `http_provider_self_hedge` start events.
- Add `winner_self_hedge_attempt=true|false` on same-provider self-hedge race
  winners.
- Extend the harness HTTP-provider race summary with:
  - all same-provider self-hedge winners split by `initial`, `hedged`, and
    `unknown`
  - fired self-hedge results split by `fired_initial`, `fired_hedged`, and
    `fired_unknown`
  - max winner attempt index

Focused validation:

```sh
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches -- --nocapture
cargo test -p freedom-ipfs-retrieval self_hedges_slow_single_http_provider -- --nocapture
```

Result:

- Harness HTTP-provider trace summary test passed.
- Focused retrieval self-hedge test passed.

Traced default command:

```sh
rm -f /tmp/ipfs-tech-self-hedge-winner-attempt-final-r3.json \
  /tmp/ipfs-tech-self-hedge-winner-attempt-final-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-winner-attempt-final-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-winner-attempt-final-r3.json
```

Default artifacts:

- `/tmp/ipfs-tech-self-hedge-winner-attempt-final-r3.json`
- `/tmp/ipfs-tech-self-hedge-winner-attempt-final-r3-trace.jsonl`

Default result:

- Passed `3/3`.
- Run total p50/p95/max: `2641/2851/2851ms`.
- Root TTFB p50/p95/max: `717/768/768ms`.
- Asset TTFB p50/p95/max: `239/672/836ms`.
- Max RSS/FD: `53684KiB` / `31`.
- HTTP-provider self-hedges: `32`.
- Same-provider winners: `initial=52`, `hedged=3`, `unknown=0`.
- Fired self-hedge results: `32`; `fired_initial=29`, `fired_hedged=3`,
  `fired_unknown=0`.
- Single-provider result p50/p95/max: `223/458/679ms`.
- HTTP-provider block fetch p50/p95/max: `226/533/831ms`.
- Block source mix: HTTP-provider `106`, Bitswap `14`.

Same-window disabled command:

```sh
rm -f /tmp/ipfs-tech-self-hedge-winner-disabled-r3.json \
  /tmp/ipfs-tech-self-hedge-winner-disabled-r3-trace.jsonl

FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-winner-disabled-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-winner-disabled-r3.json
```

Disabled artifacts:

- `/tmp/ipfs-tech-self-hedge-winner-disabled-r3.json`
- `/tmp/ipfs-tech-self-hedge-winner-disabled-r3-trace.jsonl`

Disabled result:

- Passed `3/3`.
- Run total p50/p95/max: `2449/2469/2469ms`.
- Root TTFB p50/p95/max: `821/840/840ms`.
- Asset TTFB p50/p95/max: `228/568/982ms`.
- Max RSS/FD: `53124KiB` / `31`.
- HTTP-provider self-hedges: `0`.
- Single-provider result p50/p95/max: `214/508/691ms`.
- HTTP-provider block fetch p50/p95/max: `229/528/743ms`.
- Block source mix: HTTP-provider `91`, Bitswap `29`.

Decision:
Keep the diagnostics. They show that in this live window only `3` of `32`
fired duplicate same-provider requests actually won. The disabled run was also
slightly faster on run total and asset p95, while using a similar RSS/FD shape
and letting the normal Bitswap session path win more blocks. Do not flip the
default from this single same-window sample, because prior Kubo comparisons
found the 200ms default competitive and public-network windows vary. The next
evidence step should be a longer no-trace enabled-vs-disabled A/B, probably
`repeat=10`, before deciding whether same-provider self-hedging should become
opt-in, score-gated, or dynamically disabled after low duplicate-win rates.

## 2026-05-06 Observe: No-Trace Self-Hedge Enabled vs Disabled r10

Question:
The traced winner-attempt sample showed a low duplicate win rate, and the traced
disabled run looked slightly faster. Does a no-trace `repeat=10` sample provide
enough evidence to disable same-provider HTTP self-hedging by default?

Default command:

```sh
rm -f /tmp/ipfs-tech-self-hedge-default-r10.json

timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 10 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-self-hedge-default-r10.json
```

Disabled command:

```sh
rm -f /tmp/ipfs-tech-self-hedge-disabled-r10.json

FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1 timeout 1200s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 10 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/ipfs-tech-self-hedge-disabled-r10.json
```

Artifacts:

- `/tmp/ipfs-tech-self-hedge-default-r10.json`
- `/tmp/ipfs-tech-self-hedge-disabled-r10.json`

Result:

| Mode | Pass | Run total p50/p90/p95/max | Root TTFB p50/p90/p95/max | Asset TTFB p50/p90/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Default `200ms` | `10/10` | `2040/2365/2454/2454ms` | `522/732/792/792ms` | `221/457/528/1038ms` | `53352KiB` / `30` |
| Disabled | `10/10` | `1907/2200/3028/3028ms` | `528/707/784/784ms` | `215/437/495/1412ms` | `53396KiB` / `32` |

Decision:
Do not flip the default from this sample. Disabling same-provider self-hedge was
slightly better at p50/p90/p95 asset TTFB and p50 run total, but the kept
`200ms` default had better run p95/max and better asset max. Both modes stayed
resource-light and passed `10/10`. The actionable conclusion is narrower: stop
tuning raw self-hedge delay and use the new winner-attempt diagnostics in future
longer A/Bs. A production policy change should require a larger sample across
`ipfs.tech`, `daicowtf`, and range cases, with Kubo comparison if the default is
changed.

## 2026-05-06 Observe: Multi-Case Self-Hedge Enabled vs Disabled r5

Question:
The prior wider no-trace sample covered only `ipfs.tech`. Before changing the
same-provider HTTP self-hedge policy, collect a same-window multi-case sample
covering the focused page workload plus the recurring sparse-provider and range
guardrails.

Default command:

```sh
rm -f /tmp/self-hedge-policy-default-multicase-r5.json

timeout 1800s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/self-hedge-policy-default-multicase-r5.json
```

Disabled command:

```sh
rm -f /tmp/self-hedge-policy-disabled-multicase-r5.json

FREEDOM_IPFS_DISABLE_SINGLE_HTTP_SELF_HEDGE=1 timeout 1800s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 5 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --output /tmp/self-hedge-policy-disabled-multicase-r5.json
```

Artifacts:

- `/tmp/self-hedge-policy-default-multicase-r5.json`
- `/tmp/self-hedge-policy-disabled-multicase-r5.json`

Result:

| Mode | Pass | Run total p50/p90/p95/max | Max RSS/FD | `ipfs.tech` root p50/p95/max | `ipfs.tech` asset p50/p95/max | DAICO root p50/p95/max | Vitalik range p50/p95/max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Default `200ms` | `5/5` | `2555/3729/3729/3729ms` | `55900KiB` / `35` | `578/799/799ms` | `189/642/1700ms` | `269/418/418ms` | `92/175/175ms` |
| Disabled | `5/5` | `2297/2654/2654/2654ms` | `55620KiB` / `40` | `619/727/727ms` | `163/483/1694ms` | `271/283/283ms` | `88/93/93ms` |

Decision:
Do not change the default from this sample alone. Disabling same-provider HTTP
self-hedge looked better on overall run p50/p95, `ipfs.tech` asset p50/p95,
DAICO max, and Vitalik range max. However, the default still had slightly better
`ipfs.tech` root p50 and lower max FD count, and these no-trace guardrails do
not expose duplicate winner rates or source mix. Treat this as stronger
evidence that the current same-provider self-hedge should become conditional,
not as enough evidence to flip it off globally. The next useful experiment is a
scored or adaptive policy that disables duplicate same-provider requests after
low observed hedge win rates, with trace validation that it keeps tail
protection for genuinely slow single-provider responses.

## 2026-05-06 Keep Lab Control: Single-HTTP Self-Hedge Score Gate

Question:
Can the same-provider HTTP self-hedge become conditional on recent provider
latency instead of firing on every single-provider request that crosses the
`200ms` delay? This should stay opt-in until live evidence proves it helps.

Implementation:

- Add opt-in env var:
  `FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS`.
- When unset, default behavior is unchanged.
- When set, a single HTTP provider with a recent EWMA score below the threshold
  skips the duplicate same-provider self-hedge.
- Unscored providers keep the existing self-hedge protection, so a new provider
  can still get rare-tail coverage.
- Emit `http_provider_self_hedge_skip` with reason
  `provider_score_below_threshold`, `scoring_disabled`, or `provider_unkeyed`.
- Extend harness summaries with same-provider self-hedge skip counts and
  reasons.
- Map the skip phase to `fetching_http_provider` in the harness and mobile
  progress mapping.

Focused validation:

```sh
cargo fmt --all --check
cargo test -p freedom-ipfs-retrieval single_http_self_hedge_score_gate_skips_fast_scored_provider
cargo test -p freedom-ipfs-retrieval self_hedges_slow_single_http_provider
cargo test -p freedom-ipfs-retrieval http_provider
cargo test -p mobile-web-harness trace_summary_counts_http_provider_fetches
cargo test -p freedom-ipfs-mobile progress_phase_maps_trace_events_to_ui_states
cargo check -p freedom-ipfs-retrieval --all-targets
cargo check -p mobile-web-harness --all-targets
git diff --check
```

Focused result:

- Formatting and diff whitespace checks passed.
- New score-gate helper test passed.
- Existing single HTTP self-hedge test passed.
- Focused HTTP-provider retrieval tests passed: `10 passed`.
- Focused harness HTTP-provider summary test passed.
- Focused mobile progress mapping test passed.
- Retrieval and harness package checks passed.

Score-gated `250ms` experiment:

```sh
rm -f /tmp/ipfs-tech-self-hedge-scoregate250-r3.json \
  /tmp/ipfs-tech-self-hedge-scoregate250-r3-trace.jsonl

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS=250 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-scoregate250-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-scoregate250-r3.json
```

Same-window default baseline:

```sh
rm -f /tmp/ipfs-tech-self-hedge-scoregate-baseline-r3.json \
  /tmp/ipfs-tech-self-hedge-scoregate-baseline-r3-trace.jsonl

timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-scoregate-baseline-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-scoregate-baseline-r3.json
```

Artifacts:

- `/tmp/ipfs-tech-self-hedge-scoregate250-r3.json`
- `/tmp/ipfs-tech-self-hedge-scoregate250-r3-trace.jsonl`
- `/tmp/ipfs-tech-self-hedge-scoregate-baseline-r3.json`
- `/tmp/ipfs-tech-self-hedge-scoregate-baseline-r3-trace.jsonl`

Result:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD | Self-hedges | Self-hedge skips | Single-provider result p50/p95/max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Score gate `250ms` | `3/3` | `2657/3615/3615ms` | `603/1453/1453ms` | `251/899/1450ms` | `52960KiB` / `33` | `14` | `34` | `234/762/1204ms` |
| Default | `3/3` | `2538/2705/2705ms` | `750/1038/1038ms` | `256/605/905ms` | `47616KiB` / `24` | `45` | `0` | `267/511/690ms` |

Trace notes:

- The score gate did exactly what it was meant to do mechanically:
  `34` skips with reason `provider_score_below_threshold`.
- That reduction in duplicate same-provider requests did not improve the
  user-facing sample. Root p95, asset p95, asset max, run p95/max, and FD/RSS
  were all worse than the same-window default baseline.
- The default run had more self-hedges (`45`) and more duplicate wins (`7`),
  and it kept the single-provider result tail materially lower.

Additional `150ms` threshold check:

```sh
rm -f /tmp/ipfs-tech-self-hedge-scoregate150-r3.json \
  /tmp/ipfs-tech-self-hedge-scoregate150-r3-trace.jsonl

FREEDOM_IPFS_SINGLE_HTTP_SELF_HEDGE_MIN_SCORE_MS=150 timeout 900s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-self-hedge-scoregate150-r3-trace.jsonl \
  --output /tmp/ipfs-tech-self-hedge-scoregate150-r3.json
```

Artifacts:

- `/tmp/ipfs-tech-self-hedge-scoregate150-r3.json`
- `/tmp/ipfs-tech-self-hedge-scoregate150-r3-trace.jsonl`

Result:

- Passed `3/3`.
- Run total p50/p95/max: `2163/2534/2534ms`.
- Root TTFB p50/p95/max: `594/789/789ms`.
- Asset TTFB p50/p95/max: `231/526/733ms`.
- Max RSS/FD: `46780KiB` / `25`.
- Self-hedges: `40`; self-hedge skips: `0`.
- Single-provider result p50/p95/max: `226/375/683ms`.

Interpretation:
Do not treat the `150ms` score-gate sample as evidence for conditional
self-hedge suppression. It looked good, but the gate did not actually engage:
`self_hedge_skips=0`. The latency improvement is live-window variance or normal
default behavior, not a score-gating win.

Decision:
Keep the opt-in env var and harness/mobile diagnostics as a lab control, but
reject `250ms` score-gating as a production/default policy. The code path is
inactive unless the env var is set, and the skip diagnostics are useful for
future controlled sweeps. Do not enable this by default without a larger
same-window A/B showing lower p95/max while preserving rare-tail protection.

## 2026-05-06 Baseline: Current-Head Multi-Case Rust-vs-Kubo

Question:
After the same-provider HTTP self-hedge diagnostics and opt-in score-gate lab
control, where does the current branch stand against Kubo across the focused
page workload plus the two recurring guardrails?

Command:

```sh
rm -f /tmp/current-head-multicase-kubo-r3.json \
  /tmp/current-head-multicase-kubo-r3-trace.jsonl

timeout 1500s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --compare-kubo \
  --kubo-bin target/tools/kubo/kubo/ipfs \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --case daicowtf-page-assets \
  --case vitalik-root-html-range \
  --repeat 3 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/current-head-multicase-kubo-r3-trace.jsonl \
  --comparison-output /tmp/current-head-multicase-kubo-r3.json
```

Artifacts:

- `/tmp/current-head-multicase-kubo-r3.json`
- `/tmp/current-head-multicase-kubo-r3-trace.jsonl`

Result:

- Rust passed `3/3`; Kubo passed `3/3`.
- Shared max resources: Rust `55764KiB` / `32` FDs, Kubo `270036KiB` /
  `225` FDs. Rust/Kubo ratios: RSS `0.21x`, FD `0.14x`.
- `daicowtf-page-assets`: root TTFB Rust p50/p95 `289/305ms`, Kubo
  `2353/2638ms`; Rust ratio `0.12x` p50 and p95.
- `vitalik-root-html-range`: root/range TTFB Rust p50/p95 `98/112ms`, Kubo
  `2379/2669ms`; Rust ratio `0.04x` p50 and p95.
- `ipfs-tech-page-assets`: root TTFB Rust p50/p95 `585/1890ms`, Kubo
  `1436/2256ms`; Rust ratio `0.41x` / `0.84x`.
- `ipfs-tech-page-assets`: asset TTFB Rust p50/p95 `226/821ms`, Kubo
  `155/1257ms`; Rust ratio `1.46x` / `0.65x`.

Rust trace notes:

- Block sources: HTTP provider `85` blocks, Bitswap `48` blocks.
- HTTP-provider block fetch elapsed p50/p95/max: `224/817/1035ms`.
- Bitswap block fetch elapsed p50/p95/max: `103/224/1618ms`.
- Delegated provider lookup p50/p95/max: `26/115/679ms`.
- HTTP-provider races: `80` results, all successful; same-provider self-hedges
  `15`; duplicate winners `4`.
- HTTP-provider fetch elapsed p50/p95/max: `115/228/316ms`.
- Bitswap session post-lookup waits: `52`; hits `22`; timeouts `30`.
- The slowest Rust root request was `/ipns/ipfs.tech/` at `1888ms`, dominated
  by a Bitswap root block fetch:
  `block_fetch_total=1618ms`, `bitswap_fetch=1494ms`.

Interpretation:

- Current Rust is still dramatically more resource-efficient than Kubo and
  materially faster on DAICO and Vitalik range guardrails.
- On `ipfs.tech`, Rust beats Kubo on root p50/p95 and asset p95, but Kubo still
  wins asset p50.
- The current worst Rust root tail was not an HTTP-provider self-hedge issue.
  It was a cold Bitswap root-block path with no session peers yet.
- Next behavior work should move away from raw HTTP self-hedge policy and
  investigate cold root/session Bitswap startup or request-shape classification
  for when the root block goes Bitswap instead of verified HTTP provider.

## 2026-05-06 Diagnostic: Classify Zero-HTTP Cold Bitswap Requests

Question:
Can the harness surface the latest slow-root pattern directly, instead of
requiring manual JSONL spelunking through the Kubo comparison trace?

Change:
Added request-level trace classification in `mobile-web-harness` for retrieval
shapes that matter to this investigation:

- `zero_http_provider_bitswap`
- `zero_http_provider_cold_bitswap`
- `cold_bitswap_peer_expand`
- `top_level_zero_http_provider_bitswap`
- `top_level_zero_http_provider_cold_bitswap`

The classifier uses gateway request spans to correlate
`delegated_provider_lookup`, `bitswap_peer_expand`, and `block_fetch_total`
events. Slow request output now includes classification labels and counters for
zero-HTTP delegated lookups, Bitswap block fetches, cold Bitswap peer expansions,
max peer count, and max session peer count. Normal and Rust-vs-Kubo trace
summaries also print aggregate request classification counts.

Existing trace spot-check:

```sh
test -f /tmp/current-head-multicase-kubo-r3-trace.jsonl && \
  sed -n '2396,2420p' /tmp/current-head-multicase-kubo-r3-trace.jsonl
```

The archived slow `/ipns/ipfs.tech/` root request has the same gateway span on:

- `delegated_provider_lookup`: `provider_count=19`, `http_provider_count=0`
- `bitswap_peer_expand`: `peer_count=5`, `session_peer_count=0`
- `bitswap_fetch`: `elapsed_ms=1494`, `source_transport=tcp`
- `block_fetch_total`: `source=bitswap`, `elapsed_ms=1618`

Validation:

```sh
cargo test -p mobile-web-harness trace_summary_classifies_zero_http_cold_bitswap_requests
cargo test -p mobile-web-harness
cargo check -p mobile-web-harness --all-targets
cargo clippy -p mobile-web-harness --all-targets -- -D warnings
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
```

Results:

- Targeted classifier test: `1 passed`.
- Harness test suite: `44 passed`.
- Harness package check/clippy passed.
- Workspace check/clippy passed.
- Formatting and whitespace checks passed.

Conclusion:
Keep this diagnostics-only change. It does not change retrieval behavior, but it
turns a recurring manual analysis step into a first-class trace summary signal.
The next behavior experiment can now target root zero-HTTP/cold-Bitswap cases
with clearer A/B evidence.

## 2026-05-06 Experiment: Single-HTTP Bitswap Hedge On IPFS.tech

Question:
Does the existing opt-in single-HTTP-provider Bitswap hedge reduce the current
`ipfs.tech` root and asset tails, and what mobile resource cost does it carry?

Default command:

```sh
rm -f /tmp/ipfs-tech-zero-http-classify-r10.json \
  /tmp/ipfs-tech-zero-http-classify-r10-trace.jsonl \
  /tmp/ipfs-tech-zero-http-classify-r10.log

timeout 1800s cargo run -p mobile-web-harness -- \
  --build-gateway \
  --fresh-gateway-per-run \
  --case ipfs-tech-page-assets \
  --repeat 10 \
  --asset-concurrency 6 \
  --timeout-secs 120 \
  --run-timeout-secs 240 \
  --dht-query-timeout-secs 3 \
  --trace-output /tmp/ipfs-tech-zero-http-classify-r10-trace.jsonl \
  --output /tmp/ipfs-tech-zero-http-classify-r10.json \
  > /tmp/ipfs-tech-zero-http-classify-r10.log 2>&1
```

Hedge command:

```sh
rm -f /tmp/ipfs-tech-bitswap-hedge-r10.json \
  /tmp/ipfs-tech-bitswap-hedge-r10-trace.jsonl \
  /tmp/ipfs-tech-bitswap-hedge-r10.log

FREEDOM_IPFS_ENABLE_SINGLE_HTTP_BITSWAP_HEDGE=1 timeout 1800s \
  cargo run -p mobile-web-harness -- \
    --build-gateway \
    --fresh-gateway-per-run \
    --case ipfs-tech-page-assets \
    --repeat 10 \
    --asset-concurrency 6 \
    --timeout-secs 120 \
    --run-timeout-secs 240 \
    --dht-query-timeout-secs 3 \
    --trace-output /tmp/ipfs-tech-bitswap-hedge-r10-trace.jsonl \
    --output /tmp/ipfs-tech-bitswap-hedge-r10.json \
    > /tmp/ipfs-tech-bitswap-hedge-r10.log 2>&1
```

Artifacts:

- `/tmp/ipfs-tech-zero-http-classify-r10.json`
- `/tmp/ipfs-tech-zero-http-classify-r10-trace.jsonl`
- `/tmp/ipfs-tech-zero-http-classify-r10.log`
- `/tmp/ipfs-tech-bitswap-hedge-r10.json`
- `/tmp/ipfs-tech-bitswap-hedge-r10-trace.jsonl`
- `/tmp/ipfs-tech-bitswap-hedge-r10.log`
- `/tmp/ipfs-tech-bitswap-hedge-score250-r10.json`
- `/tmp/ipfs-tech-bitswap-hedge-score250-r10-trace.jsonl`
- `/tmp/ipfs-tech-bitswap-hedge-score250-r10.log`
- `/tmp/ipfs-tech-bitswap-hedge-score200-r10.json`
- `/tmp/ipfs-tech-bitswap-hedge-score200-r10-trace.jsonl`
- `/tmp/ipfs-tech-bitswap-hedge-score200-r10.log`

Result:

| Mode | Pass | Run total p50/p95/max | Root TTFB p50/p95/max | Asset TTFB p50/p95/max | Max RSS/FD |
| --- | --- | --- | --- | --- | --- |
| Default | `10/10` | `2132/3344/3344ms` | `674/2003/2003ms` | `217/642/1012ms` | `54240KiB` / `31` |
| Bitswap hedge enabled | `10/10` | `1922/2498/2498ms` | `562/609/609ms` | `207/544/985ms` | `55440KiB` / `48` |
| Hedge + `250ms` score gate | `10/10` | `2028/2822/2822ms` | `564/879/879ms` | `201/541/991ms` | `53184KiB` / `34` |
| Hedge + `200ms` score gate | `10/10` | `2138/2558/2558ms` | `521/727/727ms` | `212/538/1010ms` | `54660KiB` / `49` |

Trace notes:

- Default request classifications:
  `cold_bitswap_peer_expand=10`, `zero_http_provider_bitswap=10`,
  `zero_http_provider_cold_bitswap=10`,
  `top_level_zero_http_provider_bitswap=1`,
  `top_level_zero_http_provider_cold_bitswap=1`.
- Hedge request classifications:
  `cold_bitswap_peer_expand=61`, `zero_http_provider_bitswap=10`,
  `zero_http_provider_cold_bitswap=10`.
- Default HTTP-provider race result max: `1074ms`; single-provider result max:
  `1074ms`; self-hedges: `89`; self-hedge wins from duplicate attempt: `6`.
- Hedge HTTP-provider race result max: `490ms`; single-provider result max:
  `490ms`; Bitswap hedge starts/results: `142/142`; result sources:
  `http_provider=137`, `bitswap=5`.
- Hedge mode raised max FDs from `31` to `48`, increased Bitswap peer attempt
  starts from `164` to `525`, and introduced `54` connection-limit dial
  rejections. RSS rose modestly by about `1.2MiB`.
- `250ms` score-gating skipped all Bitswap hedges:
  `starts=0`, `skips=144`, skip reasons
  `provider_score_below_threshold=134`, `provider_unscored=10`.
- `200ms` score-gating started fewer hedges:
  `starts=34`, `skips=119`, result sources `http_provider=33`,
  `bitswap=1`. It reduced median FD use versus full hedge but still hit max FD
  `49`, so the worst resource spike was not improved.

Interpretation:

The existing opt-in Bitswap hedge is a real latency lever for `ipfs.tech` in
this window: root p95/max, run p95/max, and asset p95 all improved. It also
clearly spends more mobile-relevant connection budget. Do not flip it on by
default from this single workload alone.

The score-gated variants did not produce a clean default policy. `250ms` was too
conservative and skipped every hedge. `200ms` engaged the hedge sometimes, but
kept the same worst FD spike as full hedge while giving up part of the root-tail
win. The next useful experiment is likely not a pure score gate. Better options:
reduce or parameterize the single-HTTP post-lookup session wait, or run full
hedge across DAICO/Vitalik and Kubo comparison guardrails to determine whether
the FD cost is acceptable for the broader workload.
