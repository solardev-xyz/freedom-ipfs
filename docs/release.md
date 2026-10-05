# Release Versioning

`freedom-ipfs` uses the workspace Cargo version as the canonical runtime
version.

- `freedom_ipfs_version()` returns `CARGO_PKG_VERSION`, for example `0.4.1`.
- GitHub release tags must be `v{freedom_ipfs_version()}`, for example
  `v0.4.1`.
- Apps may display this as the user-facing node version, for example
  `freedom-ipfs 0.4.1`.

The mobile/native ABI is versioned separately:

- `FREEDOM_IPFS_MOBILE_FFI_ABI_VERSION` is the C ABI compatibility number.
- `freedom_ipfs_build_info_json()` returns both the runtime version and ABI
  version, plus target/build metadata and capability flags.

Before cutting a release:

1. Bump `[workspace.package].version` in `Cargo.toml`.
2. Bump `bindings/node/freedom-ipfs-node/package.json` to the same version.
3. Regenerate `Cargo.lock`.
4. Verify `freedom_ipfs_version()` matches the intended tag without the leading
   `v`.
5. Verify `freedom_ipfs_build_info_json()` reports the expected version, ABI,
   target, and capabilities.
6. Tag the release as `vX.Y.Z`.
7. Attach platform artifacts and checksums.

Do not move or rewrite an existing release tag just to fix embedded metadata.
Cut a patch release instead.

## Desktop Node/Electron Addon

The desktop addon is a private N-API package under
`bindings/node/freedom-ipfs-node`. It links the Rust `freedom-ipfs-mobile`
static library into `freedom_ipfs_native.node` and exposes the native gateway
FFI to Electron/Node consumers.

Electron loads the addon into its main process, so the node lifecycle has
Promise-returning variants that run on the libuv thread pool instead of the
JS thread: `nodeNewWithDataDirAsync`, `nodeStartNativeGatewayOnlineAsync`,
`nodeStopGatewayAsync` and `nodeFreeAsync` (same arguments and results as the
sync exports, which remain for existing callers). Async start/stop/free calls
on one handle run one at a time in call order, so an unawaited start cannot
land after a later stop, and `nodeFreeAsync` runs after the start/stop called
before it. While one is pending, the sync `nodeStartNativeGatewayOnline`,
`nodeStopGateway` and `nodeFree` throw for that handle instead of racing it.
Do not use a handle after `nodeFreeAsync`. Node free normally closes the cache
database before it returns (or resolves), even if blocking work outlives the
2 s runtime shutdown bound, so the same data dir can be reopened right away.
This is bounded, not guaranteed: free waits up to 2 s more for a SQLite
statement that leaked blocking work is still running. If that statement holds
the connection past the bound, free logs `node_free_store_close_timeout` and
returns with the database still open; it then closes when that work finishes.
So free blocks for at most about 4 s (runtime shutdown plus store close).
`node bench-lifecycle.js` in the addon directory measures how long each
lifecycle call blocks the JS thread.

Build a release artifact for the current host platform with:

```sh
cd bindings/node/freedom-ipfs-node
npm ci
cd ../../..
node scripts/package-electron-addon.js
```

The packaged artifact is written to:

```text
target/electron-addon/freedom-ipfs-node-electron41-{platform}-{arch}.tar.gz
target/electron-addon/freedom-ipfs-node-electron41-{platform}-{arch}.tar.gz.sha256
```

The archive contains `freedom_ipfs_native.node` at its root. Freedom Browser's
download script installs that file into its local `prebuilds/{os}-{arch}`
directory.

The GitHub `Electron Addon` workflow builds the addon for:

- `darwin-arm64`
- `linux-x64`
- `linux-arm64`
- `win32-x64`

Release assets must include all supported Electron addon archives and their
checksums under the same `vX.Y.Z` tag alongside the iOS XCFramework artifact.
