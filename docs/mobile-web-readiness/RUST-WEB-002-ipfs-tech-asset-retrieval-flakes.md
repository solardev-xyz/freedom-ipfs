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

Decision: keep. This is a small local UnixFS/gateway optimization with direct
test coverage for the intended cache behavior. It removes redundant path-cache
work from body reads and improves the real range sample and full `ipfs.tech`
page-assets run without increasing routing fanout or changing read-only serving
semantics.
