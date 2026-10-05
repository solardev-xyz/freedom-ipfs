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
const otherDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'freedom-ipfs-node-smoke-'));
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

    // Async lifecycle calls on one handle settle in call order, even when the
    // caller does not await them, and free waits for the start/stop before it.
    const orderHandle = await addon.nodeNewWithDataDirAsync(dataDir, 1024 * 1024);
    if (!orderHandle || orderHandle === '0') {
      throw new Error('nodeNewWithDataDirAsync resolved to an empty handle');
    }
    const settled = [];
    const track = (label, promise) => promise.then((value) => settled.push(label) && value);
    const start = track('start', addon.nodeStartNativeGatewayOnlineAsync(orderHandle));
    const stop = track('stop', addon.nodeStopGatewayAsync(orderHandle));
    let syncFreeError = null;
    try {
      addon.nodeFree(orderHandle);
    } catch (err) {
      syncFreeError = err;
    }
    if (!syncFreeError || !/still pending/.test(syncFreeError.message)) {
      throw new Error('nodeFree did not refuse a handle with a pending async lifecycle call');
    }
    const free = track('free', addon.nodeFreeAsync(orderHandle));
    const results = await Promise.all([start, stop, free]);
    if (settled.join(',') !== 'start,stop,free') {
      throw new Error(`async lifecycle calls settled out of order: ${settled.join(',')}`);
    }
    if (results[0] !== true || results[1] !== true) {
      throw new Error(`unawaited start/stop resolved to ${results[0]}/${results[1]}`);
    }

    // Once a free has run, a new node may get the same address before that
    // free's promise settles. The new handle must not inherit the old one's
    // pending lifecycle queue (sync calls on it would throw "still pending").
    // The default allocators rarely hand the address back this quickly, so
    // CI also runs this file on Linux under
    //   MALLOC_CONF=narenas:1,tcache:false LD_PRELOAD=<libjemalloc.so.2>
    // which reuses it on most iterations (reported as reused_addresses), with
    // FREEDOM_IPFS_SMOKE_MIN_REUSED_ADDRESSES=1 so that run fails instead of
    // passing without ever taking the reuse path.
    let reusedAddresses = 0;
    for (let i = 0; i < 20; i += 1) {
      const oldHandle = addon.nodeNewWithDataDir(dataDir, 1024 * 1024);
      addon.nodeStartNativeGatewayOnline(oldHandle);
      const oldFree = addon.nodeFreeAsync(oldHandle);
      const newHandle =
        i % 2 === 0
          ? addon.nodeNewWithDataDir(otherDataDir, 1024 * 1024)
          : await addon.nodeNewWithDataDirAsync(otherDataDir, 1024 * 1024);
      handle = newHandle;
      if (newHandle === oldHandle) {
        reusedAddresses += 1;
        addon.nodeStopGateway(newHandle);
      }
      await oldFree;
      const newStart = addon.nodeStartNativeGatewayOnlineAsync(newHandle);
      const newStop = addon.nodeStopGatewayAsync(newHandle);
      handle = null;
      await addon.nodeFreeAsync(newHandle);
      if ((await newStart) !== true || (await newStop) !== true) {
        throw new Error('lifecycle calls on a reused node address did not resolve to true');
      }
    }
    const minReused = Number(process.env.FREEDOM_IPFS_SMOKE_MIN_REUSED_ADDRESSES || 0);
    if (!Number.isInteger(minReused) || minReused < 0) {
      throw new Error('FREEDOM_IPFS_SMOKE_MIN_REUSED_ADDRESSES must be a non-negative integer');
    }
    if (reusedAddresses < minReused) {
      throw new Error(
        `node address reused ${reusedAddresses} times, expected at least ${minReused}: ` +
          'the stale lifecycle queue check did not run'
      );
    }
    console.log(
      JSON.stringify(
        {
          ok: true,
          reused_addresses: reusedAddresses,
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
    fs.rmSync(otherDataDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
