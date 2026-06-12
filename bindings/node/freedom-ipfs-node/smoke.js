#!/usr/bin/env node

const fs = require('fs');
const os = require('os');
const path = require('path');

const addonPath = path.join(__dirname, 'build', 'Release', 'freedom_ipfs_native.node');
if (!fs.existsSync(addonPath)) {
  throw new Error(`native addon not found at ${addonPath}`);
}

const addon = require(addonPath);
const version = addon.version();
const buildInfo = JSON.parse(addon.buildInfoJson());

if (!version || version !== buildInfo.version) {
  throw new Error(`version mismatch: version=${version} buildInfo=${buildInfo.version}`);
}
if (buildInfo.name !== 'freedom-ipfs') {
  throw new Error(`unexpected build info name: ${buildInfo.name}`);
}
if (buildInfo.mobile_ffi_abi_version !== addon.constants.MOBILE_FFI_ABI_VERSION) {
  throw new Error('mobile FFI ABI version mismatch');
}

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'freedom-ipfs-node-smoke-'));
let handle = null;

try {
  handle = addon.nodeNewWithDataDir(dataDir, 1024 * 1024);
  if (!handle || handle === '0') {
    throw new Error('nodeNewWithDataDir returned an empty handle');
  }
  if (!addon.nodeStartNativeGatewayOnline(handle)) {
    throw new Error('nodeStartNativeGatewayOnline returned false');
  }
  const stats = JSON.parse(addon.nodeNativeGatewayStatsJson(handle));
  if (typeof stats.active_native_handles !== 'number') {
    throw new Error('native gateway stats JSON did not include active_native_handles');
  }
  addon.nodeStopGateway(handle);
  console.log(
    JSON.stringify(
      {
        ok: true,
        version,
        release_tag: buildInfo.release_tag,
        target: buildInfo.target,
        mobile_ffi_abi_version: buildInfo.mobile_ffi_abi_version,
      },
      null,
      2
    )
  );
} finally {
  if (handle) {
    addon.nodeFree(handle);
  }
  fs.rmSync(dataDir, { recursive: true, force: true });
}
