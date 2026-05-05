# Implementation Status

Last audited: 2026-05-03  
Repository: `github.com/flotob/freedom-ipfs`  
Spec: `/root/codex/mobile-rust-ipfs-node-spec.md`

Detailed prompt-to-artifact audit: `docs/completion-audit.md`

## Current State

This is a running Rust IPFS reader, not just a scaffold. It starts a local gateway, resolves externally supplied `/ipfs` and `/ipns` paths, discovers providers through delegated routing with light-DHT fallback, retrieves verified blocks through HTTP providers and Bitswap, reads UnixFS data, and serves browser-facing responses and HTML error pages from the local gateway.

The implementation remains iOS-first. Linux verification, live public-network retrieval, and macOS/Xcode XCFramework plus simulator command-line and app-rendering smoke verification have passed. Production browser-app integration and real-device resource profiling still require target iPhones; `docs/ios-device-verification.md` is the runbook for that final gate, `docs/ios-device-evidence-template.csv` is the structured Bee-on/Bee-off measurement template, and `cargo run -p xtask -- validate-ios-device-evidence <results.csv> --filled` validates filled device evidence before the audit is closed.

## Verification

Host verification passed:

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p xtask -- validate-ios-device-evidence
```

Kubo-generated UnixFS, no-index directory listing, empty-file, UnixFS range/`HEAD`, and HAMT-directory parity smoke passed with Kubo v0.41.0 downloaded locally to `target/tools/kubo/kubo/ipfs`:

```bash
KUBO_BIN=$PWD/target/tools/kubo/kubo/ipfs cargo test -p freedom-ipfs-gateway --test kubo_parity -- --ignored --nocapture
```

Controlled loopback Kubo daemon Bitswap interop smoke passed with the same Kubo binary:

```bash
KUBO_BIN=$PWD/target/tools/kubo/kubo/ipfs make kubo-bitswap
```

Live public-network smoke passed:

```bash
make live-smoke
```

Public corpus smoke passed for the checked-in immutable `/ipfs` paths and DNSLink-backed `/ipns` paths, including byte-range checks:

```bash
make live-corpus
```

Local cached-gateway soak passed:

```bash
make local-soak
```

Live retrieval soak passed:

```bash
make live-soak
```

iOS XCFramework CI passed:

```text
GitHub Actions run: https://github.com/flotob/freedom-ipfs/actions/runs/25286090027
Head SHA: 6d15756a80aef9106ef5997ee593c3228d951538
```

The macOS job ran on `macos-15` with Xcode 16.4 using `actions/checkout@v6` and `actions/upload-artifact@v7`, both Node 24-backed releases. It built `FreedomIpfs.xcframework`, verified headers/module maps/exported C symbols including the routing restart export and mobile telemetry/diagnostics exports, booted an iOS simulator, compiled and linked the Swift wrapper smoke, checked non-loopback bind rejection from Swift, ran the gateway smoke through `simctl spawn booted`, checked Swift-visible cached retrieval/routing counters plus the combined diagnostics snapshot and delta helper, exercised Swift background/foreground, low-memory, network-change, routing-restart, and explicit `.offline` routing controls, built and installed a generated UIKit/WebKit app, exercised the same lifecycle hooks in that app process, rendered a CAR-backed gateway fixture in `WKWebView`, verified the DOM marker, and uploaded the XCFramework artifact `6772593602` (`60300523` bytes).

Observed live-smoke result:

- `vitalik.eth` resolved at runtime to `/ipfs/bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u`
- `daicowtf.eth` resolved at runtime to `/ipfs/bafybeidznfolm74c5cephzdycedx7hk76iawno45wemcvkflieotzo2lne`
- local gateway returned `38394` bytes for `vitalik.eth`
- local gateway returned `403507` bytes for `daicowtf.eth`
- `vitalik.eth` printed `retrieval_delta=cache_hits=5,http_provider_blocks=2,bitswap_blocks=0` and `routing_delta=delegated_lookups=2,delegated_results=42,delegated_errors=0,dht_lookups=0,dht_results=0,dht_errors=0`
- `daicowtf.eth` printed `retrieval_delta=cache_hits=20,http_provider_blocks=0,bitswap_blocks=3` and `routing_delta=delegated_lookups=3,delegated_results=5,delegated_errors=0,dht_lookups=0,dht_results=0,dht_errors=0`
- retrieval stats were `cache_hits=25 http_provider_blocks=2 bitswap_blocks=3`
- routing provider stats were `delegated_lookups=5 delegated_results=47 delegated_errors=0 dht_lookups=0 dht_results=0 dht_errors=0`

Observed live-corpus result:

- `vitalik-home` fetched `/ipfs/bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u`
- `daicowtf-home` fetched `/ipfs/bafybeidznfolm74c5cephzdycedx7hk76iawno45wemcvkflieotzo2lne`
- `ipfs-tech-developers-hero` fetched `/ipns/ipfs.tech/_nuxt/developers-hero.BRuJDQyf.jpg`
- `ipfs-tech-ribbon-community-7` fetched `/ipns/ipfs.tech/_nuxt/ribbon-community-7.BM6mrSZz.jpg`
- `ipfs-tech-ribbon-community-8` fetched `/ipns/ipfs.tech/_nuxt/ribbon-community-8.kKCRQ1KB.jpg`
- `ipfs-tech-ribbon-home-1` fetched `/ipns/ipfs.tech/_nuxt/ribbon-home-1.Db3iUyss.jpg`
- `ipfs-tech-ribbon-home-2` fetched `/ipns/ipfs.tech/_nuxt/ribbon-home-2.xhPE7YJm.jpg`
- `ipfs-tech-ribbon-home-3` fetched `/ipns/ipfs.tech/_nuxt/ribbon-home-3.CsPAOEU8.jpg`
- `wikipedia-on-ipfs-en-root` fetched `/ipns/en.wikipedia-on-ipfs.org`
- byte counts were `38394`, `403507`, `184141`, `100287`, `58701`, `62518`, `120227`, `75458`, and `169`
- the first eight entries also passed a `bytes=0-127` request through the local gateway; the Wikipedia-on-IPFS root passed a `bytes=0-63` request. Every range response matched the full response prefix and included a valid `Content-Range` header.
- retrieval stats were `cache_hits=157 http_provider_blocks=8 bitswap_blocks=7`
- transient local-gateway `408`, `502`, `503`, and `504` responses are retried up to five times with backoff in opt-in live harnesses before failing a corpus, smoke, or soak run.

Observed local-soak result:

- 500 cached local-gateway requests completed against an in-memory raw block.
- Linux RSS moved from `8960` KiB to `13312` KiB, within the 32 MiB maximum growth budget.

Observed live-soak result:

- 2 cold gateway rounds completed against `vitalik-home` and `daicowtf-home`.
- total bytes fetched through the local gateway: `883802`
- retrieval stats were `cache_hits=50 http_provider_blocks=4 bitswap_blocks=6`
- Linux RSS moved from `11264` KiB to `42240` KiB, within the 128 MiB maximum growth budget.

iOS packaging command was exercised on Linux and correctly refused to run:

```bash
cargo run -p xtask -- build-xcframework
cargo run -p xtask -- verify-xcframework
```

Result:

```text
Error: build-xcframework requires macOS with Xcode command line tools; current host is linux
Error: verify-xcframework requires macOS with Xcode command line tools; current host is linux
```

## Roadmap Audit

M0 decisions and fixtures: partially complete. The repo, license, generated unit-test fixtures, deterministic libp2p fixtures, and Kubo-generated CIDv1/raw-leaf UnixFS, CIDv0/DAG-PB UnixFS, and HAMT CAR parity smokes exist. A larger checked-in public fixture corpus is still useful.

M1 workspace and mobile skeleton: mostly complete. Workspace, mobile C ABI, Swift wrapper source, loopback-only mobile gateway start/restart, cache stats, retrieval/routing counters and deltas, combined diagnostics snapshot and delta helper, active preload count, cache import/export, routing mode selection and change helpers including explicit `.offline`, multi-router configuration, local gateway URL mapping helpers, lifecycle hooks, preload/cancel with path/URI/bare-CID normalization, and an XCFramework build/verify skeleton exist. The build stages the C header plus module map and checks artifact structure/exported symbols, including the routing restart and telemetry/diagnostics exports; the verifier additionally stages a simulator Swift smoke that checks non-loopback bind rejection, imports a generated CAR fixture, starts the local gateway, fetches through loopback, checks Swift-visible cached retrieval/routing counters, diagnostics, and diagnostics deltas, exercises lifecycle/low-memory/network-change/routing-restart/offline controls, and stops on macOS. It also builds, installs, and launches a generated UIKit/WebKit simulator app that exercises lifecycle hooks, renders the same gateway fixture, and verifies the DOM marker. GitHub Actions macOS run `25286090027` verified artifact production, simulator execution, lifecycle controls, routing restart, explicit offline mode, and app rendering.

M2 CID, block verification, and store: complete for MVP. CID parse/format, verified block insertion, CAR import/export including empty raw blocks, bounded in-memory hot block cache, SQLite cache, CIDv0/CIDv1 DAG-PB alias cache lookup, eviction, active block retention, provider cache, bad-provider cache, clear, and trim are covered by tests.

M3 UnixFS reader and offline gateway: complete for MVP. Raw, dag-pb, multi-block files, empty files, percent-encoded browser paths, CIDv0 DAG-PB UnixFS, directories, directory `index.html` fallback with path-based MIME headers, escaped HTML directory listings for `/ipfs` and resolved `/ipns` directories when no `index.html` exists, namespace-preserving `/ipns` listing links, basic HAMT traversal and listing, fixed/open-ended/suffix range reads, bounded streaming gateway responses for full and ranged reads with overflow-safe stream state, `HEAD` requests for full and ranged `/ipfs` and `/ipns` reads, malformed/unsatisfiable byte-range rejection, request-scope block retention, traversal-segment rejection, Kubo RPC/WebUI route absence, explicit CLI `--routing-mode offline`, a process-level CLI smoke that launches the actual gateway binary, fetches an imported CAR fixture through `/ipfs/{cid}`, and proves an uncached CID does not hit a configured delegated router, and Kubo-generated CIDv1/raw-leaf UnixFS, CIDv0/DAG-PB UnixFS, no-index directory listings, empty-file, encoded-path, range, `HEAD`, and HAMT CAR import/gateway byte parity are implemented and tested.

M4 IPNS and DNSLink: implemented. DNSLink uses a pluggable TXT resolver trait and a generic default resolver wrapper, with Cloudflare DoH as the current shipped backend. DNS TXT TTLs are preserved when available and capped by the name cache, split quoted TXT character strings are concatenated before parsing, and bounded CNAME delegation is followed when a DNSLink TXT lookup returns a CNAME-only answer. IPNS delegated lookup across all configured delegated routers, bounded delegated IPNS response bodies, light-DHT fallback, v2 verification, expiry checks, recursion-limit failure, and name caching are implemented and tested. Native/system TXT lookup remains a follow-up.

M5 delegated routing and verified HTTP retrieval: implemented. Delegated Routing V1 parsing, CIDv1/base32 lookup normalization, optional comma-separated multi-router race/failover for provider discovery and delegated IPNS lookup, malformed/oversized routing response rejection, bounded delegated response size/provider fanout, provider caching, bounded HTTP raw block retrieval, CID verification, invalid/redirected/oversized provider block rejection, bad-provider suppression, and HTTP timeouts are implemented.

M6 minimal Bitswap client: implemented for read-only retrieval. It dials bounded provider candidates, supports TCP/WebSocket/QUIC transports, includes libp2p identify/ping behaviours, applies libp2p connection timeout/connection-limit guards, uses want-have before want-block in multi-peer Bitswap 1.2 sessions, handles DONT_HAVE responses, verifies returned blocks, caches extra payload blocks, and sends cancels. It does not serve blocks or open public listen addresses. The retrieval crate includes deterministic in-process libp2p Bitswap peer tests that validate stream negotiation, block response handling, want-have selection, cache insertion, cancel emission, and no-listener client swarm construction, plus an ignored loopback Kubo daemon interop smoke that retrieves a raw block over Bitswap from Kubo v0.41.0.

M7 light DHT fallback: implemented for provider lookup and IPNS record lookup. It uses Kademlia client mode, lazy per-lookup swarms, query timeout, provider fanout limits, libp2p identify/ping behaviours, and libp2p connection timeout/connection-limit guards. The routing crate includes deterministic local server-mode Kademlia peer tests for provider lookup and verified IPNS record lookup through the light-DHT client, plus a no-listener/client-mode swarm construction test. The ignored public Amino DHT smoke now requires `FREEDOM_IPFS_LIVE_DHT_CID` because the default live corpus CIDs repeatedly returned zero public DHT providers despite working through delegated routing.

M8 mobile resource hardening: partially complete. Bounded in-memory hot block cache, cache trim, gateway concurrency limit, bounded full/ranged gateway response streaming with overflow-safe chunk progression, mobile background/foreground hooks, low-memory trim hook, network-change provider-cache hygiene, DHT timeout/fanout knobs, provider/badness caches, bounded HTTP provider response bodies, HTTP timeouts, libp2p identify/ping behaviours, libp2p connection timeouts, and libp2p connection-limit guards exist. A host-side mobile idle smoke verifies that starting the online gateway and hitting only `/health` performs no retrieval, delegated-routing, DHT, or preload work before a content request. The generated Swift command-line and UIKit/WebKit simulator smokes exercise lifecycle, low-memory, and network-change controls, but real idle RSS, CPU, network, startup, Bee concurrency, and host-app lifecycle behavior are not measured yet.

M9 browser integration: partially complete. The local gateway path, mobile ABI, Swift wrapper source, loopback bind enforcement for mobile gateway start/restart, gateway URL mapping helpers for `ipfs://`, `ipns://`, `/ipfs`, and `/ipns` addresses, browser-facing HTML error pages, preload normalization for path/URI/bare-CID inputs, Swift-visible retrieval/routing counters and deltas, combined diagnostics snapshot and delta helper, bounded JSON progress snapshots, active preload count, routing-mode restart helpers including explicit cache-only `.offline`, lifecycle hooks, and preload/cancel controls exist, and the live smoke proves ENS-backed contenthash flows when names are resolved outside the node. Progress snapshots reuse existing structured gateway/retrieval/routing/name-system phases plus explicit preload events so Swift can poll per-request loading state without callbacks. The live smoke now prints per-target retrieval and routing deltas that distinguish cache hits, HTTP-provider blocks, Bitswap blocks, delegated provider lookup, and light-DHT fallback. The live smoke and corpus harnesses mount the online IPNS/DNSLink resolver for `/ipns` paths and retry transient local-gateway timeout/service-unavailable statuses. Swift wrapper compilation/linking, lifecycle/control calls, diagnostics deltas, routing restart, explicit offline mode, and generated `WKWebView` app rendering are verified by the GitHub Actions simulator smoke; integration into the Freedom browser app and progress UI wiring are not verified yet.

M10 interop hardening: partial. Unit tests, deterministic local Bitswap and light-DHT coverage, no-listener libp2p client swarm tests, Kubo RPC/WebUI route absence tests, Kubo-generated CIDv1/raw-leaf UnixFS, CIDv0/DAG-PB UnixFS, empty-file, encoded-path, HAMT, no-index directory listings including HAMT listings, fixed/open-ended/suffix range, full/ranged `HEAD`, and directory-index parity smokes, live ENS smoke with per-target transport/routing diagnostics, a checked-in public corpus covering immutable `/ipfs` plus `ipfs.tech` and Wikipedia-on-IPFS DNSLink-backed `/ipns` paths with live byte-range checks, multiple larger media assets, and transient-status retries, a local cached-gateway RSS soak, and a host live-retrieval RSS soak exist, but broader Kubo parity matrix and device network soaks remain follow-up work.

M11 optional features: not started except CAR export/import support, which was promoted into the MVP diagnostics/cache path.

## Known Gaps

- Real iPhone resource targets are unverified, including the provisional under-60-MiB idle RSS target beside Bee; use `docs/ios-device-verification.md` and `docs/ios-device-evidence-template.csv` to collect the missing evidence.
- iOS lifecycle hooks exist at the ABI/Swift level and are exercised in generated simulator smokes, but actual Freedom host-app background/foreground, low-memory, and network-path event wiring is not verified on device.
- DHT-only retrieval of `daicowtf.eth` is not reliable on the public DHT; current auto mode succeeds because delegated routing returns usable providers.
- The public Amino DHT smoke has no stable default CID yet; set `FREEDOM_IPFS_LIVE_DHT_CID` to a known-good advertised CID before using it as live evidence. The current `_dnslink.ipfs.tech` root, three common example CIDs, `vitalik-home`, `daicowtf-home`, and the Wikipedia-on-IPFS root returned zero public DHT providers on 2026-05-03; see `tests/fixtures/README.md`.
- DNSLink still defaults to Cloudflare DoH, with TTL-aware caching and CNAME delegation following. Native/system TXT lookup should be evaluated for artifact size and iOS behavior.
- The checked-in public corpus is expanded across two DNSLink sites but still not exhaustive; more independent sites/media and documented pass/fail cases would improve confidence.
- The soak coverage is still host-side only; iOS device memory-growth and network soaks are still missing.
- Kubo parity now covers CIDv1/raw-leaf UnixFS, CIDv0/DAG-PB UnixFS, empty files, percent-encoded browser paths, HAMT, no-index directory listings, fixed/open-ended/suffix range, full/ranged `HEAD`, and directory-index behavior, but still not a broad matrix for every supported gateway edge case.
