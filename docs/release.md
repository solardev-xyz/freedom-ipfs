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
2. Regenerate `Cargo.lock`.
3. Verify `freedom_ipfs_version()` matches the intended tag without the leading
   `v`.
4. Verify `freedom_ipfs_build_info_json()` reports the expected version, ABI,
   target, and capabilities.
5. Tag the release as `vX.Y.Z`.
6. Attach platform artifacts and `checksums.txt`.

Do not move or rewrite an existing release tag just to fix embedded metadata.
Cut a patch release instead.
