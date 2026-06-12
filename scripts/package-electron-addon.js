#!/usr/bin/env node

const { spawnSync } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const repoRoot = path.resolve(__dirname, '..');
const addonDir = path.join(repoRoot, 'bindings', 'node', 'freedom-ipfs-node');
const addonName = 'freedom_ipfs_native.node';

function parseArgs(argv) {
  const out = {
    electronMajor: process.env.FREEDOM_IPFS_ELECTRON_MAJOR || '41',
    outDir: path.join(repoRoot, 'target', 'electron-addon'),
    rustRepo: repoRoot,
    skipCargoBuild: false,
    target: process.env.FREEDOM_IPFS_NODE_TARGET || `${process.platform}-${process.arch}`,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--skip-cargo-build') {
      out.skipCargoBuild = true;
    } else if (arg === '--electron-major') {
      out.electronMajor = argv[++i];
    } else if (arg === '--out-dir') {
      out.outDir = path.resolve(argv[++i]);
    } else if (arg === '--rust-repo') {
      out.rustRepo = path.resolve(argv[++i]);
    } else if (arg === '--target') {
      out.target = argv[++i];
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  return out;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: options.stdio || 'inherit',
    shell: process.platform === 'win32',
    ...options,
  });
  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed with exit code ${result.status || 1}`);
  }
  return result;
}

function readWorkspaceVersion() {
  const cargoToml = fs.readFileSync(path.join(repoRoot, 'Cargo.toml'), 'utf8');
  const match = cargoToml.match(/\[workspace\.package\][\s\S]*?\nversion\s*=\s*"([^"]+)"/);
  if (!match) {
    throw new Error('could not read [workspace.package].version from Cargo.toml');
  }
  return match[1];
}

function gitOutput(args) {
  const result = spawnSync('git', args, {
    cwd: repoRoot,
    encoding: 'utf8',
    shell: process.platform === 'win32',
  });
  return result.status === 0 ? result.stdout.trim() : '';
}

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function buildEnv() {
  const env = { ...process.env };
  if (process.platform === 'darwin' && !env.MACOSX_DEPLOYMENT_TARGET) {
    env.MACOSX_DEPLOYMENT_TARGET = '11.0';
  }
  if (process.platform === 'win32') {
    const staticCrtFlag = '-C target-feature=+crt-static';
    env.RUSTFLAGS = env.RUSTFLAGS
      ? `${env.RUSTFLAGS} ${staticCrtFlag}`
      : staticCrtFlag;
  }
  env.FREEDOM_IPFS_BUILD_GIT_COMMIT =
    env.FREEDOM_IPFS_BUILD_GIT_COMMIT || gitOutput(['rev-parse', '--short=12', 'HEAD']);
  env.FREEDOM_IPFS_BUILD_GIT_DESCRIBE =
    env.FREEDOM_IPFS_BUILD_GIT_DESCRIBE ||
    gitOutput(['describe', '--tags', '--dirty', '--always']);
  return env;
}

function packageAddon(options) {
  const version = readWorkspaceVersion();
  const packageJsonPath = path.join(addonDir, 'package.json');
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
  if (packageJson.version !== version) {
    throw new Error(
      `${packageJsonPath} version ${packageJson.version} does not match Cargo version ${version}`
    );
  }

  const env = buildEnv();
  if (!options.skipCargoBuild) {
    console.log('[freedom-ipfs-node] building Rust static library');
    run('cargo', ['build', '-p', 'freedom-ipfs-mobile', '--release'], {
      cwd: options.rustRepo,
      env,
    });
  }

  console.log('[freedom-ipfs-node] building Node/Electron addon');
  run(
    process.platform === 'win32' ? 'npx.cmd' : 'npx',
    ['node-gyp', 'rebuild', `--freedom_ipfs_rust_repo=${options.rustRepo}`],
    {
      cwd: addonDir,
      env,
    }
  );

  const builtAddon = path.join(addonDir, 'build', 'Release', addonName);
  if (!fs.existsSync(builtAddon)) {
    throw new Error(`node-gyp did not produce ${builtAddon}`);
  }

  const assetName = `freedom-ipfs-node-electron${options.electronMajor}-${options.target}.tar.gz`;
  const stageDir = path.join(options.outDir, 'stage', options.target);
  const archive = path.join(options.outDir, assetName);
  fs.rmSync(stageDir, { recursive: true, force: true });
  fs.mkdirSync(stageDir, { recursive: true });
  fs.copyFileSync(builtAddon, path.join(stageDir, addonName));
  fs.mkdirSync(options.outDir, { recursive: true });

  console.log(`[freedom-ipfs-node] packaging ${assetName}`);
  run('tar', ['-czf', archive, '-C', stageDir, addonName], { cwd: repoRoot });

  const digest = sha256(archive);
  const checksumFile = `${archive}.sha256`;
  fs.writeFileSync(checksumFile, `${digest}  ${assetName}\n`);

  console.log(`[freedom-ipfs-node] archive: ${archive}`);
  console.log(`[freedom-ipfs-node] sha256:  ${digest}`);
}

try {
  packageAddon(parseArgs(process.argv));
} catch (err) {
  console.error(`[freedom-ipfs-node] ${err.message}`);
  process.exit(1);
}
