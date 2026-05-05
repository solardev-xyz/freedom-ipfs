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
