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

The GitHub `Electron Addon` workflow builds the addon on macOS, Linux, and
Windows. Release assets can include all supported operating systems under the
same `vX.Y.Z` tag alongside the iOS XCFramework artifact.
