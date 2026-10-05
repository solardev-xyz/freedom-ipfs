#!/usr/bin/env node
// Measures how long each node lifecycle call blocks the calling JS thread.
//
//   node bench-lifecycle.js [--mode sync|async] [--data-dir DIR] [--requests N]
//                           [--addon PATH]
//
// For each phase it reports `blockedMs`: the longest stretch the event loop
// could not run (a setImmediate ticker runs alongside every call), and
// `totalMs`: wall time until the call (or its Promise) finished.
//
// Phases: cold start (first open of the data dir in this process), warm
// start (re-open after free), and stop + free while `--requests` gateway
// requests are stuck on a delegated router that accepts and never answers.
// Point `--data-dir` at a populated cache to measure a realistic repo; a
// fresh temporary dir is used otherwise.

const fs = require('fs');
const os = require('os');
const path = require('path');
const { performance } = require('perf_hooks');

function parseArgs(argv) {
  const out = {
    mode: 'async',
    dataDir: null,
    requests: 16,
    addon: path.join(__dirname, 'build', 'Release', 'freedom_ipfs_native.node'),
  };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--mode') out.mode = argv[++i];
    else if (arg === '--data-dir') out.dataDir = path.resolve(argv[++i]);
    else if (arg === '--requests') out.requests = Number(argv[++i]);
    else if (arg === '--addon') out.addon = path.resolve(argv[++i]);
    else throw new Error(`unknown argument: ${arg}`);
  }
  if (out.mode !== 'sync' && out.mode !== 'async') {
    throw new Error('--mode must be sync or async');
  }
  return out;
}

const opts = parseArgs(process.argv);
const addon = require(opts.addon);
const MAX_CACHE_BYTES = 512 * 1024 * 1024;

// Longest gap between event-loop turns while `fn` runs.
async function measure(fn) {
  let running = true;
  let last = performance.now();
  let maxGap = 0;
  const tick = () => {
    const now = performance.now();
    maxGap = Math.max(maxGap, now - last);
    last = now;
    if (running) setImmediate(tick);
  };
  setImmediate(tick);
  const started = performance.now();
  const value = await fn();
  const totalMs = performance.now() - started;
  running = false;
  maxGap = Math.max(maxGap, performance.now() - last);
  return { value, blockedMs: round(maxGap), totalMs: round(totalMs) };
}

function round(ms) {
  return Math.round(ms * 10) / 10;
}

const call = {
  sync: {
    open: (dir) => addon.nodeNewWithDataDir(dir, MAX_CACHE_BYTES),
    start: (node, ...args) => addon.nodeStartNativeGatewayOnline(node, ...args),
    stop: (node) => addon.nodeStopGateway(node),
    free: (node) => addon.nodeFree(node),
  },
  async: {
    open: (dir) => addon.nodeNewWithDataDirAsync(dir, MAX_CACHE_BYTES),
    start: (node, ...args) => addon.nodeStartNativeGatewayOnlineAsync(node, ...args),
    stop: (node) => addon.nodeStopGatewayAsync(node),
    free: (node) => addon.nodeFreeAsync(node),
  },
}[opts.mode];

// A delegated router that accepts every request and never answers, so the
// gateway requests below stay in flight until the node is stopped. It runs
// in a child process so its socket work does not show up as main-thread time.
const HANGING_ROUTER = `
const http = require('http');
const server = http.createServer(() => process.send({ hit: true }));
server.listen(0, '127.0.0.1', () => process.send({ port: server.address().port }));
`;

function startHangingRouter() {
  const { spawn } = require('child_process');
  const child = spawn(process.execPath, ['-e', HANGING_ROUTER], {
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  let hits = 0;
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    child.on('message', (message) => {
      if (message.hit) {
        hits += 1;
      } else if (message.port) {
        resolve({
          url: `http://127.0.0.1:${message.port}`,
          hits: () => hits,
          close: () => child.kill(),
        });
      }
    });
  });
}

function randomCid() {
  // CIDv1 raw sha2-256 of random bytes: never in the cache.
  const digest = require('crypto').randomBytes(32);
  const bytes = Buffer.concat([Buffer.from([0x01, 0x55, 0x12, 0x20]), digest]);
  return `b${base32(bytes)}`;
}

function base32(bytes) {
  const alphabet = 'abcdefghijklmnopqrstuvwxyz234567';
  let bits = 0;
  let value = 0;
  let out = '';
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      out += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += alphabet[(value << (5 - bits)) & 31];
  return out;
}

async function main() {
  const ownDir = !opts.dataDir;
  const dataDir =
    opts.dataDir || fs.mkdtempSync(path.join(os.tmpdir(), 'freedom-ipfs-bench-'));
  const router = await startHangingRouter();
  const results = {};
  const startArgs = [router.url, addon.constants.ROUTING_MODE_DELEGATED];

  try {
    for (const phase of ['cold', 'warm']) {
      const open = await measure(() => call.open(dataDir));
      const node = open.value;
      if (!node || node === '0') throw new Error('node open failed');
      const start = await measure(() => call.start(node, ...startArgs));
      if (start.value !== true) throw new Error('gateway start failed');
      results[`${phase}Open`] = { blockedMs: open.blockedMs, totalMs: open.totalMs };
      results[`${phase}Start`] = { blockedMs: start.blockedMs, totalMs: start.totalMs };

      if (phase === 'cold') {
        const free = await measure(() => call.free(node));
        results.idleFree = { blockedMs: free.blockedMs, totalMs: free.totalMs };
        continue;
      }

      for (let i = 0; i < opts.requests; i += 1) {
        const handle = addon.gatewayRequestStart(
          node,
          JSON.stringify({ method: 'GET', path: `/ipfs/${randomCid()}` })
        );
        if (!handle || handle === '0') throw new Error('gatewayRequestStart failed');
      }
      const deadline = Date.now() + 5000;
      while (router.hits() < Math.min(opts.requests, 1) && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
      results.inFlightRouterRequests = router.hits();
      const stats = JSON.parse(addon.nodeNativeGatewayStatsJson(node));
      results.inFlightGatewayHandles = stats.active_native_handles;

      const stop = await measure(() => call.stop(node));
      results.stopInFlight = { blockedMs: stop.blockedMs, totalMs: stop.totalMs };
      const free = await measure(() => call.free(node));
      results.freeAfterInFlight = { blockedMs: free.blockedMs, totalMs: free.totalMs };
    }
  } finally {
    router.close();
    if (ownDir) fs.rmSync(dataDir, { recursive: true, force: true });
  }

  console.log(JSON.stringify({ mode: opts.mode, requests: opts.requests, ...results }));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
