# freedom-ipfs

Mobile-first Rust IPFS reader for Freedom.

This repository implements the plan in `/root/codex/mobile-rust-ipfs-node-spec.md`:

- local HTTP gateway as the browser data plane,
- read-only IPFS/IPNS/DNSLink retrieval,
- verified blocks before serving,
- bounded cache,
- iOS-first packaging,
- no content providing, DHT server mode, public gateway fallback, or Kubo RPC compatibility in the first version.

The implementation is split into small crates so the local gateway, cache, routing, retrieval, namesystem, and mobile FFI surfaces can be evolved independently.

## Status

Early implementation. Current code supports:

- cache-backed local gateway for `/ipfs` and DNSLink/IPNS-backed `/ipns`,
- IPNS record retrieval and v2 signature/validity verification,
- pluggable DNSLink TXT resolver wiring, with Cloudflare DoH as the current default,
- shared multi-endpoint delegated router configuration for provider routing and delegated IPNS lookup,
- bounded delegated IPNS record response bodies before signature verification,
- binary `/ipns/` light-DHT record lookup fallback for IPNS in `auto` and `light_dht` modes,
- deterministic in-process light-DHT IPNS record lookup test coverage,
- explicit offline routing mode and offline gateway constructors keep name resolution cache-only/no-network by default,
- short-lived in-memory DNSLink/IPNS name-resolution cache,
- DNSLink CNAME delegation following in the Cloudflare DoH resolver,
- delegated routing provider lookup, including optional comma-separated multi-router race/failover,
- optional routing lookup counters for live smoke diagnostics, distinguishing delegated provider lookup from light-DHT fallback,
- bounded delegated-routing response size and provider fanout,
- client-mode light DHT provider/IPNS lookup fallback with configurable provider fanout,
- deterministic in-process light-DHT provider lookup test coverage,
- CLI/mobile knobs for DHT query timeout and provider fanout,
- short-lived multihash-keyed provider-result cache and bad-provider suppression,
- explicit HTTP timeouts for delegated routing, DNSLink/IPNS, and provider block requests,
- redirect-disabled HTTP provider block requests, matching the read-only verified retrieval model,
- bounded HTTP provider block response bodies before CID verification/cache insert,
- explicit libp2p identify, ping, connection timeout, and connection-limit behaviours for DHT and Bitswap swarms,
- host tests assert light-DHT and Bitswap client swarms start without listen addresses,
- verified HTTP raw-block retrieval from HTTP-capable providers,
- client-only Bitswap retrieval over TCP, WebSocket, and QUIC libp2p streams for Bitswap-only providers,
- bounded Bitswap peer/address fanout with one provider-refresh retry after stale-provider failures,
- Bitswap cancel messages after successful block receipt,
- verified caching of extra CIDv0/CIDv1 blocks returned in Bitswap payload responses,
- deterministic in-process libp2p Bitswap retrieval test coverage,
- opt-in loopback Kubo daemon Bitswap interop smoke,
- basic HAMT-sharded UnixFS directory traversal,
- directory `index.html` fallback with path-based MIME headers,
- escaped HTML directory listings for `/ipfs` and resolved `/ipns` directories, preserving `/ipns` link namespaces when a directory has no `index.html`,
- percent-encoded browser path segments for files with spaces and reserved characters,
- UnixFS range reads and gateway range responses that avoid assembling entire multi-block files in memory,
- gateway rejection of malformed and unsatisfiable byte-range requests,
- fixed, open-ended, and suffix byte-range responses with `Content-Range`,
- `HEAD` requests for full and ranged `/ipfs` and `/ipns` gateway reads with headers and no body,
- full and ranged local-gateway responses streamed in bounded UnixFS chunks instead of one full-file/range buffer,
- gateway rejection of `.`/`..` path traversal segments before UnixFS lookup,
- browser-facing HTML gateway error pages with escaped details and stable HTTP status codes,
- explicit test coverage that Kubo RPC/WebUI paths such as `/api/v0/version`
  and `/webui` are not exposed,
- Kubo-generated CAR parity smoke for UnixFS files/directories, no-index directory listings, empty files, percent-encoded browser paths, byte ranges, `HEAD` requests, and HAMT directories when `KUBO_BIN` is available,
- opt-in public corpus smoke that fetches documented `/ipfs` and DNSLink-backed `/ipns` paths and byte ranges through the local gateway,
- five-attempt opt-in live harness retries with backoff for transient local-gateway `408`/`502`/`503`/`504` responses caused by public-network provider timeouts,
- opt-in local gateway soak that repeats cached reads and checks bounded RSS growth on Linux,
- opt-in live retrieval soak that repeats cold public-network gateway rounds and checks bounded RSS growth on Linux,
- configurable local-gateway request concurrency limiting,
- mobile gateway start/restart rejects non-loopback bind addresses so the iOS
  data plane stays local-only,
- bounded 16 MiB in-memory hot block cache in front of the SQLite cache,
- CIDv0/CIDv1 DAG-PB aliases share block-cache and stream-retention state,
- CAR import/export for tests, diagnostics, cache warmup, and empty raw-block fixtures,
- mobile lifecycle hooks for background/foreground, low-memory cache trimming, and network-change provider cache hygiene,
- mobile progress snapshots as bounded JSON for per-request gateway, retrieval,
  routing, and preload loading feedback,
- an iOS staticlib/XCFramework build skeleton with staged C headers/module map, exported-symbol verification, a simulator Swift gateway smoke, a generated UIKit/WebKit app-rendering smoke in the verifier, and a Swift wrapper for persistent-cache node creation, cache import/export, cache trimming, retrieval/routing counters and deltas, a combined diagnostics snapshot and delta helper, active preload count, routing-mode selection and restart including `.offline`, multi-router configuration, local gateway URL mapping, lifecycle hooks, preload/cancel for `/ipfs`, `/ipns`, `ipfs://`, `ipns://`, and bare-CID inputs, and offline/online gateway start.

Still incomplete: resource profiling on device, network path integration in the host app, host-app progress UI wiring, and hardened DHT-only retrieval for sites whose DHT providers are slow or stale. See [docs/completion-audit.md](docs/completion-audit.md), [docs/mobile-progress-api.md](docs/mobile-progress-api.md), and [docs/ios-device-verification.md](docs/ios-device-verification.md) for the current prompt-to-artifact checklist, progress API shape, and remaining iPhone/app verification gate.

## Development

```bash
cargo test --workspace
cargo run -p freedom-ipfs-gateway -- --help
```

Run the gateway online with the mobile default `auto` routing mode:

```bash
cargo run -p freedom-ipfs-gateway -- --online --routing-mode auto
```

Run cache-only with explicit offline routing:

```bash
cargo run -p freedom-ipfs-gateway -- --routing-mode offline
```

Use multiple delegated routers for provider discovery by passing a comma-separated endpoint list:

```bash
cargo run -p freedom-ipfs-gateway -- --online --delegated-router https://delegated-ipfs.dev/routing/v1,https://example-router.invalid/routing/v1
```

Limit local-gateway request concurrency for mobile resource testing:

```bash
cargo run -p freedom-ipfs-gateway -- --online --max-concurrent-requests 4
```

Tune light-DHT fallback budgets when testing mobile resource behavior:

```bash
cargo run -p freedom-ipfs-gateway -- --online --dht-query-timeout-secs 15 --dht-max-providers 8
```

On macOS with Xcode command line tools installed, build the iOS static libraries and XCFramework. This also checks the artifact structure and exported C symbols:

```bash
cargo run -p xtask -- build-xcframework
```

Verify an existing XCFramework artifact. This requires a booted iOS simulator because it runs the Swift gateway smoke through `simctl spawn booted`, then installs and launches a generated UIKit/WebKit app that renders a CAR-backed fixture through the local gateway:

```bash
cargo run -p xtask -- verify-xcframework
```

The generated XCFramework is a static library package. iOS apps linking it must also link `SystemConfiguration.framework`.

The `.github/workflows/ios-xcframework.yml` workflow runs the macOS build and simulator command-line/app-rendering smoke in CI and uploads the generated XCFramework artifact.

Live smoke test, intentionally ignored by default because it uses the public IPFS network:

```bash
make live-smoke
```

Public corpus smoke, also ignored by default:

```bash
make live-corpus
```

Local cached gateway soak:

```bash
make local-soak
```

Live retrieval soak with cold gateway rounds:

```bash
make live-soak
```

The mobile web harness trace summary also reports Bitswap source peers, DNS
expansion cache hits, UnixFS metadata cache reuse, and trusted/session peer
outcomes for child-block reliability experiments.

Light-DHT provider discovery smoke:

```bash
FREEDOM_IPFS_LIVE_DHT_CID=<cid-with-known-amino-dht-providers> \
  cargo test -p freedom-ipfs-routing live_light_dht_finds_public_providers -- --ignored --nocapture
```

Kubo fixture parity smoke, if a Kubo `ipfs` binary is available:

```bash
KUBO_BIN=/path/to/ipfs make kubo-parity
```

Kubo Bitswap interop smoke, if a Kubo `ipfs` binary is available:

```bash
KUBO_BIN=/path/to/ipfs make kubo-bitswap
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
