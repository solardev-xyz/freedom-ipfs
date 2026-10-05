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

async function main() {
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
    addon.nodeFree(handle);
    handle = null;

    // Promise-returning lifecycle variants used by Electron's main process.
    const asyncHandle = await addon.nodeNewWithDataDirAsync(dataDir, 1024 * 1024);
    if (!asyncHandle || asyncHandle === '0') {
      throw new Error('nodeNewWithDataDirAsync resolved to an empty handle');
    }
    handle = asyncHandle;
    if ((await addon.nodeStartNativeGatewayOnlineAsync(handle)) !== true) {
      throw new Error('nodeStartNativeGatewayOnlineAsync did not resolve to true');
    }
    if ((await addon.nodeStopGatewayAsync(handle)) !== true) {
      throw new Error('nodeStopGatewayAsync did not resolve to true');
    }
    handle = null;
    await addon.nodeFreeAsync(asyncHandle);
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
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
