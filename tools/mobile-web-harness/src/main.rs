use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use freedom_ipfs_gateway::{
    GatewayConfig, GatewayCore, GatewayCoreRequest, GatewayHtmlDirectoryPrefetchConfig,
    GatewayHtmlPrefetchConfig, GatewayHtmlRangeWarmConfig, PersistentNameResolver,
};
use freedom_ipfs_mobile::{
    freedom_ipfs_gateway_request_cancel, freedom_ipfs_gateway_request_free,
    freedom_ipfs_gateway_request_read, freedom_ipfs_gateway_request_response_json,
    freedom_ipfs_gateway_request_start, freedom_ipfs_gateway_wait_next_event,
    freedom_ipfs_node_free, freedom_ipfs_node_import_car,
    freedom_ipfs_node_native_gateway_stats_json, freedom_ipfs_node_new_with_data_dir,
    freedom_ipfs_node_start_gateway_online_with_config_v2, freedom_ipfs_node_stop_gateway,
    freedom_ipfs_string_free, FreedomIpfsGatewayReadResult, FreedomIpfsNode,
};
use freedom_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsRecord, IpnsResolver, NamesysError,
};
use freedom_ipfs_retrieval::FetchingBlockProvider;
use freedom_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, DEFAULT_DELEGATED_ROUTER,
};
use freedom_ipfs_store::SqliteBlockStore;
use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use reqwest::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH,
    RANGE,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::{CStr, CString, OsStr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

const DEFAULT_CORPUS: &str = "tools/mobile-web-harness/corpus/mobile-web.json";
const DEFAULT_KUBO_BIN: &str = "target/tools/kubo/kubo/ipfs";
const DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS: usize = 8;
const DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_ASSET_CONCURRENCY: usize = 6;
const MEANINGFUL_KUBO_WIN_MIN_DELTA_MS: u128 = 50;
const MEANINGFUL_KUBO_WIN_MIN_RATIO: f64 = 1.10;
const MAX_PRINTED_ASSET_KUBO_WINS: usize = 8;
const MAX_TRACE_REQUEST_PATHS: usize = 128;
const MAX_TRACE_SLOW_EVENTS: usize = 16;
const SYNTHETIC_MULTIBLOCK_RANGE_ID: &str = "synthetic-multiblock-range";
const SYNTHETIC_MULTIBLOCK_FILE_BYTES: usize = 512 * 1024;
const SYNTHETIC_MULTIBLOCK_CHUNKER: &str = "size-16384";
const SYNTHETIC_MULTIBLOCK_RANGE_START: usize = 32 * 1024;
const SYNTHETIC_MULTIBLOCK_RANGE_BYTES: usize = 64 * 1024;
const X_FREEDOM_REQUEST_ID: &str = "X-Freedom-Request-ID";
const X_FREEDOM_PARENT_REQUEST_ID: &str = "X-Freedom-Parent-Request-ID";
const X_FREEDOM_TOP_LEVEL_PATH: &str = "X-Freedom-Top-Level-Path";
const HTML_PREFETCH_MAX_ASSETS_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_MAX_ASSETS";
const HTML_PREFETCH_MAX_BYTES_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_MAX_BYTES";
const HTML_PREFETCH_CONCURRENCY_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_CONCURRENCY";
const HTML_DIRECTORY_PREFETCH_MAX_DIRS_ENV: &str =
    "FREEDOM_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_DIRS";
const HTML_DIRECTORY_PREFETCH_MAX_BYTES_ENV: &str =
    "FREEDOM_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_BYTES";
const HTML_DIRECTORY_PREFETCH_CONCURRENCY_ENV: &str =
    "FREEDOM_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_CONCURRENCY";
const HTML_RANGE_WARM_MAX_BYTES_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_RANGE_WARM_MAX_BYTES";
const HTML_RANGE_WARM_CONCURRENCY_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_RANGE_WARM_CONCURRENCY";
const RAW_LINK_TSIZE_FAST_HEADERS_ENV: &str = "FREEDOM_IPFS_ENABLE_RAW_LINK_TSIZE_FAST_HEADERS";
const STREAM_SMALL_BODIES_ENV: &str = "FREEDOM_IPFS_GATEWAY_STREAM_SMALL_BODIES";
const DEFAULT_NATIVE_TRACE_FILTER: &str =
    "freedom_ipfs_gateway=info,freedom_ipfs_retrieval=info,freedom_ipfs_namesys=info,freedom_ipfs_routing=info,warn";
const DEFAULT_NATIVE_FFI_DISPATCHERS: usize = 1;
const DEFAULT_NATIVE_FFI_READ_BUFFER_BYTES: usize = 64 * 1024;
const NATIVE_FFI_WAIT_TIMEOUT_MS: u64 = 100;
const NATIVE_FFI_COMPLETED_HANDLE_TOMBSTONE_LIMIT: usize = 4096;

const FREEDOM_IPFS_ROUTING_MODE_AUTO: u32 = 0;
const FREEDOM_IPFS_ROUTING_MODE_DELEGATED: u32 = 1;
const FREEDOM_IPFS_ROUTING_MODE_LIGHT_DHT: u32 = 2;
const FREEDOM_IPFS_ROUTING_MODE_OFFLINE: u32 = 3;

const FREEDOM_IPFS_GATEWAY_READ_PENDING: u32 = 0;
const FREEDOM_IPFS_GATEWAY_READ_BYTES: u32 = 1;
const FREEDOM_IPFS_GATEWAY_READ_END: u32 = 2;
const FREEDOM_IPFS_GATEWAY_READ_CANCELLED: u32 = 3;
const FREEDOM_IPFS_GATEWAY_READ_FAILED: u32 = 4;
const FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE: u32 = 5;

const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK: u32 = 0;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT: u32 = 1;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE: u32 = 2;
const FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED: u32 = 3;

const FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY: u32 = 1 << 0;
const FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY: u32 = 1 << 1;
const FREEDOM_IPFS_GATEWAY_EVENT_END: u32 = 1 << 2;
const FREEDOM_IPFS_GATEWAY_EVENT_FAILED: u32 = 1 << 3;
const FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED: u32 = 1 << 4;
const FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED: u32 = 1 << 5;

static NEXT_HARNESS_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TEMP_PATH_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum HarnessEngine {
    #[value(name = "rust-http", alias = "rust")]
    RustHttp,
    #[value(name = "rust-native")]
    RustNative,
    #[value(name = "rust-native-ffi")]
    RustNativeFfi,
    Kubo,
}

impl HarnessEngine {
    fn as_str(self) -> &'static str {
        match self {
            Self::RustHttp => "rust-http",
            Self::RustNative => "rust-native",
            Self::RustNativeFfi => "rust-native-ffi",
            Self::Kubo => "kubo",
        }
    }

    fn is_rust(self) -> bool {
        matches!(
            self,
            Self::RustHttp | Self::RustNative | Self::RustNativeFfi
        )
    }
}

#[derive(Clone, Debug, Parser)]
#[command(
    author,
    version,
    about = "Black-box mobile web readiness harness for freedom-ipfs gateways"
)]
struct Args {
    /// Existing gateway URL to test, for example http://127.0.0.1:50017.
    #[arg(long, env = "GATEWAY_URL")]
    gateway_url: Option<String>,
    /// Gateway engine to spawn when --gateway-url is not provided.
    #[arg(long, value_enum, default_value_t = HarnessEngine::RustHttp)]
    engine: HarnessEngine,
    /// Standalone gateway binary to spawn when --gateway-url is not provided.
    #[arg(long, env = "FREEDOM_IPFS_GATEWAY_BIN")]
    gateway_bin: Option<PathBuf>,
    /// Build the default Rust gateway binary before spawning it.
    #[arg(long)]
    build_gateway: bool,
    /// SQLite cache DB path for spawned gateways; useful for fresh-process warm-store runs.
    #[arg(long)]
    gateway_db: Option<PathBuf>,
    /// CAR file to import into each spawned gateway before running the corpus.
    #[arg(long)]
    gateway_import_car: Option<PathBuf>,
    /// CAR file to import into a separate local Kubo seed provider. Spawned gateways retrieve it over Bitswap.
    #[arg(long)]
    bitswap_seed_car: Option<PathBuf>,
    /// Kubo ipfs binary to spawn when --engine kubo is selected.
    #[arg(long, env = "KUBO_BIN", default_value = DEFAULT_KUBO_BIN)]
    kubo_bin: PathBuf,
    /// Kubo repo path. Omit for an isolated temporary repo per spawned Kubo daemon.
    #[arg(long, env = "IPFS_PATH")]
    kubo_repo: Option<PathBuf>,
    /// Generate the deterministic synthetic multi-block range fixture into this directory and exit.
    #[arg(long)]
    prepare_synthetic_multiblock_range_fixture: Option<PathBuf>,
    /// Run paired Rust and Kubo harness passes with the same corpus/options.
    #[arg(long)]
    compare_kubo: bool,
    /// Run once online to warm a Rust gateway DB, then replay the same corpus with offline routing.
    #[arg(long)]
    offline_replay: bool,
    /// In offline replay mode, rewrite /ipns corpus paths to online-observed /ipfs targets.
    #[arg(long)]
    offline_replay_resolved_ipfs: bool,
    /// Optional paired Rust-vs-Kubo JSON comparison report output path.
    #[arg(long)]
    comparison_output: Option<PathBuf>,
    /// JSON corpus file.
    #[arg(long, default_value = DEFAULT_CORPUS)]
    corpus: PathBuf,
    /// Optional newline-delimited ENS names to resolve into live /ipfs or /ipns corpus entries.
    #[arg(long)]
    ens_corpus: Option<PathBuf>,
    /// Optional case id filter; can be passed more than once.
    #[arg(long = "case")]
    cases: Vec<String>,
    /// Optional JSON report output path.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Number of measured runs to execute.
    #[arg(long, default_value_t = 1)]
    repeat: usize,
    /// Number of unmeasured warmup runs to execute before measured runs.
    #[arg(long, default_value_t = 0)]
    warmup_runs: usize,
    /// Spawn a fresh gateway for every run instead of reusing one gateway.
    #[arg(long)]
    fresh_gateway_per_run: bool,
    /// Request timeout in seconds.
    #[arg(long, default_value_t = 180)]
    timeout_secs: u64,
    /// Optional wall-clock timeout for one full corpus run. Timed-out runs are reported as failures.
    #[arg(long, default_value_t = 0)]
    run_timeout_secs: u64,
    /// Gateway request concurrency budget when spawning a gateway.
    #[arg(long, alias = "gateway-max-concurrent-requests", default_value_t = DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS)]
    max_concurrent_requests: usize,
    /// Small in-memory full-body response cache byte budget for spawned Rust gateways.
    #[arg(long, default_value_t = DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES)]
    small_body_cache_max_bytes: usize,
    /// Gateway routing mode when spawning a gateway.
    #[arg(long, default_value = "auto")]
    routing_mode: String,
    /// Delegated routing endpoint list for spawned Rust gateways.
    #[arg(long)]
    delegated_router: Option<String>,
    /// DHT query timeout when spawning a gateway.
    #[arg(long, default_value_t = 10)]
    dht_query_timeout_secs: u64,
    /// Max DHT providers when spawning a gateway.
    #[arg(long, default_value_t = 4)]
    dht_max_providers: usize,
    /// Concurrent subresource fetches for page crawls.
    #[arg(long, default_value_t = DEFAULT_ASSET_CONCURRENCY)]
    asset_concurrency: usize,
    /// Native FFI event dispatcher worker count for --engine rust-native-ffi.
    #[arg(long, default_value_t = DEFAULT_NATIVE_FFI_DISPATCHERS)]
    native_dispatchers: usize,
    /// Caller-owned native FFI read buffer size for --engine rust-native-ffi.
    #[arg(long, default_value_t = DEFAULT_NATIVE_FFI_READ_BUFFER_BYTES)]
    native_read_buffer_bytes: usize,
    /// Artificial delay after every native FFI read, to model a slow Swift/WebKit consumer.
    #[arg(long, default_value_t = 0)]
    native_slow_consumer_ms: u64,
    /// Cancel every native FFI request after its first body bytes are read.
    #[arg(long)]
    native_cancel_after_first_byte: bool,
    /// Cancel every native FFI request once this many milliseconds have elapsed.
    #[arg(long)]
    native_cancel_after_ms: Option<u64>,
    /// Stop the native FFI node during each measured run after this many milliseconds.
    #[arg(long)]
    native_stop_node_mid_run_ms: Option<u64>,
    /// Lab-only cap before starting native FFI handles; avoids starting handles that will not be drained.
    #[arg(long)]
    native_max_active_requests: Option<usize>,
    /// Re-fetch successful non-range GETs with If-None-Match when the first response has an ETag.
    #[arg(long)]
    conditional_revalidate: bool,
    /// Gateway JSONL trace output path when spawning a gateway; parsed into the report.
    #[arg(long)]
    trace_output: Option<PathBuf>,
    /// Optional tracing filter for spawned gateway trace output.
    #[arg(long)]
    trace_filter: Option<String>,
    /// Include the full tracing span stack in each spawned gateway JSONL event.
    #[arg(long)]
    trace_span_list: bool,
    /// Require a request classification count in the Rust trace, formatted as classification=min_count.
    #[arg(long = "require-request-classification")]
    require_request_classifications: Vec<String>,
    /// Require a mobile progress phase count in the Rust trace, formatted as phase=min_count.
    #[arg(long = "require-progress-phase")]
    require_progress_phases: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(fixture_dir) = &args.prepare_synthetic_multiblock_range_fixture {
        prepare_synthetic_multiblock_range_fixture(&args.kubo_bin, fixture_dir)?;
        return Ok(());
    }
    let mut corpus = Corpus::read(&args.corpus)?;
    if let Some(ens_corpus) = &args.ens_corpus {
        extend_corpus_with_ens_names(&mut corpus, ens_corpus).await?;
    }

    if args.offline_replay {
        let report = run_offline_replay(&args, &corpus).await?;
        print_offline_replay_summary(&report);
        if let Some(output) = args.output {
            let json = serde_json::to_string_pretty(&report)?;
            std::fs::write(&output, json).with_context(|| format!("write {}", output.display()))?;
            eprintln!("wrote offline replay report to {}", output.display());
        }
        if report.online.summary.fail_count > 0 || report.offline.summary.fail_count > 0 {
            bail!("mobile web offline replay found failures");
        }
        return Ok(());
    }
    if args.offline_replay_resolved_ipfs {
        bail!("--offline-replay-resolved-ipfs requires --offline-replay");
    }

    if args.compare_kubo {
        let report = run_comparison(&args, &corpus).await?;
        print_comparison_summary(&report);
        if let Some(output) = args.comparison_output.or(args.output) {
            let json = serde_json::to_string_pretty(&report)?;
            std::fs::write(&output, json).with_context(|| format!("write {}", output.display()))?;
            eprintln!("wrote comparison report to {}", output.display());
        }
        let trace_requirement_failures = trace_requirement_failure_messages(
            &report.rust.trace_requirements.request_classifications,
        );
        let progress_requirement_failures =
            trace_requirement_failure_messages(&report.rust.trace_requirements.progress_phases);
        for failure in &trace_requirement_failures {
            eprintln!("trace requirement not met: {failure}");
        }
        for failure in &progress_requirement_failures {
            eprintln!("progress requirement not met: {failure}");
        }
        if report.rust.summary.fail_count > 0 || report.kubo.summary.fail_count > 0 {
            bail!("mobile web comparison found failures");
        }
        if !trace_requirement_failures.is_empty() {
            bail!("mobile web comparison trace requirements not met");
        }
        if !progress_requirement_failures.is_empty() {
            bail!("mobile web comparison progress requirements not met");
        }
        return Ok(());
    }

    let report = run_harness(&args, &corpus).await?;
    print_summary(&report);
    if let Some(output) = args.output {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(&output, json).with_context(|| format!("write {}", output.display()))?;
        eprintln!("wrote report to {}", output.display());
    }
    let trace_requirement_failures =
        trace_requirement_failure_messages(&report.trace_requirements.request_classifications);
    for failure in &trace_requirement_failures {
        eprintln!("trace requirement not met: {failure}");
    }
    let progress_requirement_failures =
        trace_requirement_failure_messages(&report.trace_requirements.progress_phases);
    for failure in &progress_requirement_failures {
        eprintln!("progress requirement not met: {failure}");
    }

    if report.summary.fail_count > 0 {
        bail!("mobile web harness found failures");
    }
    if !trace_requirement_failures.is_empty() {
        bail!("mobile web harness trace requirements not met");
    }
    if !progress_requirement_failures.is_empty() {
        bail!("mobile web harness progress requirements not met");
    }
    Ok(())
}

async fn run_comparison(args: &Args, corpus: &Corpus) -> Result<ComparisonReport> {
    if args.gateway_url.is_some() {
        bail!("--compare-kubo cannot be used with --gateway-url");
    }
    let mut rust_args = args.clone();
    rust_args.engine = HarnessEngine::RustHttp;
    rust_args.compare_kubo = false;
    rust_args.comparison_output = None;

    let mut kubo_args = args.clone();
    kubo_args.engine = HarnessEngine::Kubo;
    kubo_args.compare_kubo = false;
    kubo_args.comparison_output = None;
    kubo_args.gateway_db = None;
    kubo_args.build_gateway = false;
    kubo_args.trace_output = None;
    kubo_args.trace_filter = None;
    kubo_args.require_request_classifications = Vec::new();
    kubo_args.require_progress_phases = Vec::new();

    let rust = run_harness(&rust_args, corpus).await?;
    let kubo = run_harness(&kubo_args, corpus).await?;
    let cases = ComparisonCase::from_reports(&rust, &kubo);
    Ok(ComparisonReport {
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        rust,
        kubo,
        cases,
    })
}

#[derive(Debug, PartialEq, Eq)]
struct RequestClassificationRequirement {
    classification: String,
    min_count: usize,
}

fn parse_request_classification_requirements(
    raw_requirements: &[String],
) -> Result<Vec<RequestClassificationRequirement>> {
    raw_requirements
        .iter()
        .map(|raw| {
            let (classification, min_count) = raw
                .split_once('=')
                .ok_or_else(|| anyhow!("expected classification=min_count, got {raw:?}"))?;
            let classification = classification.trim();
            if classification.is_empty() {
                bail!("request classification requirement has an empty classification");
            }
            let min_count = min_count
                .trim()
                .parse::<usize>()
                .with_context(|| format!("parse min_count in {raw:?}"))?;
            Ok(RequestClassificationRequirement {
                classification: classification.to_string(),
                min_count,
            })
        })
        .collect()
}

fn trace_request_classification_count(trace: &TraceSummary, classification: &str) -> usize {
    trace
        .request_classifications
        .iter()
        .find(|entry| entry.value == classification)
        .map(|entry| entry.count)
        .unwrap_or_default()
}

#[derive(Debug, Default, Serialize)]
struct TraceRequirementsReport {
    request_classifications: Vec<TraceRequirementResult>,
    progress_phases: Vec<TraceRequirementResult>,
}

#[derive(Debug, Serialize)]
struct TraceRequirementResult {
    value: String,
    min_count: usize,
    actual_count: usize,
    passed: bool,
}

fn trace_requirements_report(
    trace: Option<&TraceSummary>,
    args: &Args,
) -> Result<TraceRequirementsReport> {
    Ok(TraceRequirementsReport {
        request_classifications: request_classification_requirement_results(
            trace,
            &args.require_request_classifications,
        )?,
        progress_phases: progress_phase_requirement_results(trace, &args.require_progress_phases)?,
    })
}

fn request_classification_requirement_results(
    trace: Option<&TraceSummary>,
    raw_requirements: &[String],
) -> Result<Vec<TraceRequirementResult>> {
    parse_request_classification_requirements(raw_requirements)?
        .into_iter()
        .map(|requirement| {
            let actual_count = trace
                .map(|trace| trace_request_classification_count(trace, &requirement.classification))
                .unwrap_or_default();
            Ok(TraceRequirementResult {
                value: requirement.classification,
                min_count: requirement.min_count,
                actual_count,
                passed: actual_count >= requirement.min_count,
            })
        })
        .collect()
}

fn trace_requirement_failure_messages(results: &[TraceRequirementResult]) -> Vec<String> {
    results
        .iter()
        .filter(|result| !result.passed)
        .map(|result| {
            format!(
                "{} expected>={} actual={}",
                result.value, result.min_count, result.actual_count
            )
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct ProgressPhaseRequirement {
    phase: String,
    min_count: usize,
}

fn parse_progress_phase_requirements(
    raw_requirements: &[String],
) -> Result<Vec<ProgressPhaseRequirement>> {
    raw_requirements
        .iter()
        .map(|raw| {
            let (phase, min_count) = raw
                .split_once('=')
                .ok_or_else(|| anyhow!("expected phase=min_count, got {raw:?}"))?;
            let phase = phase.trim();
            if phase.is_empty() {
                bail!("progress phase requirement has an empty phase");
            }
            let min_count = min_count
                .trim()
                .parse::<usize>()
                .with_context(|| format!("parse min_count in {raw:?}"))?;
            Ok(ProgressPhaseRequirement {
                phase: phase.to_string(),
                min_count,
            })
        })
        .collect()
}

fn trace_progress_phase_count(trace: &TraceSummary, phase: &str) -> usize {
    trace
        .progress_phases
        .iter()
        .find(|entry| entry.value == phase)
        .map(|entry| entry.count)
        .unwrap_or_default()
}

fn progress_phase_requirement_results(
    trace: Option<&TraceSummary>,
    raw_requirements: &[String],
) -> Result<Vec<TraceRequirementResult>> {
    parse_progress_phase_requirements(raw_requirements)?
        .into_iter()
        .map(|requirement| {
            let actual_count = trace
                .map(|trace| trace_progress_phase_count(trace, &requirement.phase))
                .unwrap_or_default();
            Ok(TraceRequirementResult {
                value: requirement.phase,
                min_count: requirement.min_count,
                actual_count,
                passed: actual_count >= requirement.min_count,
            })
        })
        .collect()
}

async fn build_default_rust_gateway() -> Result<()> {
    eprintln!("building Rust gateway with `cargo build -p freedom-ipfs-gateway`");
    let status = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("freedom-ipfs-gateway")
        .status()
        .await
        .context("run cargo build -p freedom-ipfs-gateway")?;
    if !status.success() {
        bail!("cargo build -p freedom-ipfs-gateway failed with {status}");
    }
    Ok(())
}

fn prepare_synthetic_multiblock_range_fixture(kubo: &PathBuf, fixture_dir: &Path) -> Result<()> {
    let repo = fixture_dir.join("repo");
    if repo.exists()
        && repo
            .read_dir()
            .with_context(|| format!("read fixture repo {}", repo.display()))?
            .next()
            .is_some()
    {
        bail!(
            "synthetic fixture repo already exists and is not empty: {}; remove it or choose a new fixture directory",
            repo.display()
        );
    }

    std::fs::create_dir_all(&repo).with_context(|| format!("create {}", repo.display()))?;
    kubo_ok(kubo, &repo, ["init", "--profile=server"])?;

    let data_path = fixture_dir.join("multiblock.bin");
    let car_path = fixture_dir.join("multiblock.car");
    let corpus_path = fixture_dir.join("corpus.json");
    let manifest_path = fixture_dir.join("manifest.json");
    let data = synthetic_multiblock_range_bytes();
    std::fs::write(&data_path, &data).with_context(|| format!("write {}", data_path.display()))?;

    let add_output = std::process::Command::new(kubo)
        .env("IPFS_PATH", &repo)
        .env("IPFS_TELEMETRY", "off")
        .arg("add")
        .arg("-Q")
        .arg("--cid-version=1")
        .arg("--raw-leaves=true")
        .arg(format!("--chunker={SYNTHETIC_MULTIBLOCK_CHUNKER}"))
        .arg("--progress=false")
        .arg(&data_path)
        .output()
        .with_context(|| format!("run Kubo add for {}", data_path.display()))?;
    if !add_output.status.success() {
        bail!(
            "Kubo add failed with status {}: stdout={} stderr={}",
            add_output.status,
            String::from_utf8_lossy(&add_output.stdout),
            String::from_utf8_lossy(&add_output.stderr)
        );
    }
    let cid = String::from_utf8(add_output.stdout)
        .context("Kubo add returned non-UTF8 CID")?
        .trim()
        .to_string();
    if cid.is_empty() {
        bail!("Kubo add returned an empty CID");
    }

    let export_output = std::process::Command::new(kubo)
        .env("IPFS_PATH", &repo)
        .env("IPFS_TELEMETRY", "off")
        .arg("dag")
        .arg("export")
        .arg(&cid)
        .output()
        .with_context(|| format!("run Kubo dag export for {cid}"))?;
    if !export_output.status.success() {
        bail!(
            "Kubo dag export failed with status {}: stdout={} stderr={}",
            export_output.status,
            String::from_utf8_lossy(&export_output.stdout),
            String::from_utf8_lossy(&export_output.stderr)
        );
    }
    std::fs::write(&car_path, export_output.stdout)
        .with_context(|| format!("write {}", car_path.display()))?;

    let range_end = SYNTHETIC_MULTIBLOCK_RANGE_START + SYNTHETIC_MULTIBLOCK_RANGE_BYTES - 1;
    let range_sha256 = format!(
        "{:x}",
        Sha256::digest(&data[SYNTHETIC_MULTIBLOCK_RANGE_START..=range_end])
    );
    let ipfs_path = format!("/ipfs/{cid}");
    let corpus = serde_json::json!({
        "entries": [
            {
                "id": SYNTHETIC_MULTIBLOCK_RANGE_ID,
                "description": "Synthetic 512KiB UnixFS file with 16KiB raw leaves; 64KiB middle range spans four raw blocks.",
                "path": ipfs_path,
                "range": format!("bytes={SYNTHETIC_MULTIBLOCK_RANGE_START}-{range_end}"),
                "expect_status": 206,
                "expect_content_type_prefix": "application/octet-stream",
                "expect_content_range_prefix": format!("bytes {SYNTHETIC_MULTIBLOCK_RANGE_START}-{range_end}/"),
                "expect_content_length": SYNTHETIC_MULTIBLOCK_RANGE_BYTES,
                "expect_accept_ranges": "bytes",
                "expect_body_sha256": range_sha256,
                "min_bytes": SYNTHETIC_MULTIBLOCK_RANGE_BYTES,
                "max_ttfb_ms": 10000
            }
        ]
    });
    let manifest = serde_json::json!({
        "id": SYNTHETIC_MULTIBLOCK_RANGE_ID,
        "cid": cid,
        "chunker": SYNTHETIC_MULTIBLOCK_CHUNKER,
        "file_bytes": SYNTHETIC_MULTIBLOCK_FILE_BYTES,
        "range": {
            "start": SYNTHETIC_MULTIBLOCK_RANGE_START,
            "end": range_end,
            "bytes": SYNTHETIC_MULTIBLOCK_RANGE_BYTES,
            "sha256": corpus["entries"][0]["expect_body_sha256"],
        },
        "paths": {
            "data": data_path.display().to_string(),
            "car": car_path.display().to_string(),
            "corpus": corpus_path.display().to_string(),
            "repo": repo.display().to_string(),
        }
    });

    std::fs::write(&corpus_path, serde_json::to_string_pretty(&corpus)?)
        .with_context(|| format!("write {}", corpus_path.display()))?;
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)
        .with_context(|| format!("write {}", manifest_path.display()))?;

    eprintln!("prepared synthetic multi-block range fixture");
    eprintln!(
        "  path: {}",
        corpus["entries"][0]["path"].as_str().unwrap_or("")
    );
    eprintln!("  car: {}", car_path.display());
    eprintln!("  corpus: {}", corpus_path.display());
    eprintln!("  manifest: {}", manifest_path.display());
    Ok(())
}

fn synthetic_multiblock_range_bytes() -> Vec<u8> {
    (0..SYNTHETIC_MULTIBLOCK_FILE_BYTES)
        .map(|index| (((index * 31) + (index / 251)) % 256) as u8)
        .collect()
}

async fn run_offline_replay(args: &Args, corpus: &Corpus) -> Result<OfflineReplayReport> {
    if args.compare_kubo {
        bail!("--offline-replay cannot be used with --compare-kubo");
    }
    if args.gateway_url.is_some() {
        bail!("--offline-replay cannot be used with --gateway-url");
    }
    if args.engine != HarnessEngine::RustHttp {
        bail!("--offline-replay is only supported for --engine rust");
    }
    if args.fresh_gateway_per_run {
        bail!("--offline-replay cannot be used with --fresh-gateway-per-run");
    }

    let replay_db = args
        .gateway_db
        .clone()
        .unwrap_or_else(|| unique_temp_path("freedom-ipfs-offline-replay.db"));
    if let Some(parent) = replay_db
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create offline replay DB parent {}", parent.display()))?;
    }

    let mut online_args = args.clone();
    online_args.gateway_db = Some(replay_db.clone());
    online_args.routing_mode = args.routing_mode.clone();
    online_args.trace_output =
        offline_replay_trace_output(args, "online", "freedom-ipfs-offline-replay-online-trace");

    let mut offline_args = args.clone();
    offline_args.gateway_db = Some(replay_db.clone());
    offline_args.routing_mode = "offline".to_string();
    offline_args.trace_output =
        offline_replay_trace_output(args, "offline", "freedom-ipfs-offline-replay-offline-trace");

    let online = run_harness(&online_args, corpus).await?;
    let (offline_corpus, resolved_ipfs_rewrites) = if args.offline_replay_resolved_ipfs {
        let trace_output = online_args
            .trace_output
            .as_ref()
            .context("resolved-IPFS offline replay requires an online trace output path")?;
        let resolutions = successful_name_resolutions_from_trace(trace_output)?;
        rewrite_corpus_for_resolved_ipfs_replay(corpus, &resolutions, &args.cases)
    } else {
        (corpus.clone(), Vec::new())
    };
    let offline = run_harness(&offline_args, &offline_corpus).await?;
    let summary = OfflineReplaySummary::from_report(&offline);

    Ok(OfflineReplayReport {
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        replay_db: replay_db.display().to_string(),
        resolved_ipfs_replay: args.offline_replay_resolved_ipfs,
        resolved_ipfs_rewrites,
        online,
        offline,
        summary,
    })
}

async fn run_harness(args: &Args, corpus: &Corpus) -> Result<RunReport> {
    if args.gateway_url.is_some() && args.fresh_gateway_per_run {
        bail!("--fresh-gateway-per-run cannot be used with --gateway-url");
    }
    if args.gateway_url.is_some()
        && matches!(
            args.engine,
            HarnessEngine::RustNative | HarnessEngine::RustNativeFfi
        )
    {
        bail!(
            "--gateway-url cannot be used with --engine {}",
            args.engine.as_str()
        );
    }
    if args.gateway_url.is_some() && args.gateway_db.is_some() {
        bail!("--gateway-db can only be used when the harness spawns the gateway");
    }
    if args.gateway_url.is_some() && args.gateway_import_car.is_some() {
        bail!("--gateway-import-car can only be used when the harness spawns the gateway");
    }
    if args.gateway_url.is_some() && args.bitswap_seed_car.is_some() {
        bail!("--bitswap-seed-car can only be used when the harness spawns the gateway");
    }
    if args.gateway_url.is_some() && args.build_gateway {
        bail!("--build-gateway can only be used when the harness spawns the Rust gateway");
    }
    if args.gateway_import_car.is_some() && args.bitswap_seed_car.is_some() {
        bail!("--gateway-import-car cannot be combined with --bitswap-seed-car; the seed mode should exercise network retrieval");
    }
    if args.build_gateway && args.engine != HarnessEngine::RustHttp {
        bail!("--build-gateway only applies to --engine rust-http");
    }
    if args.build_gateway && args.gateway_bin.is_some() {
        bail!("--build-gateway cannot be combined with --gateway-bin");
    }
    if args.gateway_bin.is_some() && args.engine != HarnessEngine::RustHttp {
        bail!("--gateway-bin only applies to --engine rust-http");
    }
    if !args.engine.is_rust() && args.gateway_db.is_some() {
        bail!("--gateway-db only applies to Rust engines");
    }
    if !args.engine.is_rust() && args.trace_output.is_some() {
        bail!("--trace-output is only supported for Rust engines");
    }
    if !args.require_request_classifications.is_empty() && args.trace_output.is_none() {
        bail!("--require-request-classification requires --trace-output");
    }
    if !args.require_progress_phases.is_empty() && args.trace_output.is_none() {
        bail!("--require-progress-phase requires --trace-output");
    }
    if let Some(import_car) = &args.gateway_import_car {
        if !import_car.is_file() {
            bail!(
                "--gateway-import-car must point to a readable CAR file: {}",
                import_car.display()
            );
        }
    }
    if let Some(seed_car) = &args.bitswap_seed_car {
        if !seed_car.is_file() {
            bail!(
                "--bitswap-seed-car must point to a readable CAR file: {}",
                seed_car.display()
            );
        }
    }
    if args.build_gateway {
        build_default_rust_gateway().await?;
    }
    if args.gateway_url.is_none() {
        if let Some(gateway_db) = &args.gateway_db {
            if let Some(parent) = gateway_db
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create gateway DB parent {}", parent.display()))?;
            }
        }
        if let Some(trace_output) = &args.trace_output {
            match std::fs::remove_file(trace_output) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("clear previous trace output {}", trace_output.display())
                    });
                }
            }
        }
    }
    if args.native_read_buffer_bytes == 0 {
        bail!("--native-read-buffer-bytes must be greater than zero");
    }
    if args.engine == HarnessEngine::RustNative {
        return run_native_harness(args, corpus).await;
    }
    if args.engine == HarnessEngine::RustNativeFfi {
        return run_native_ffi_harness(args, corpus).await;
    }

    let timeout = Duration::from_secs(args.timeout_secs);
    let run_timeout =
        (args.run_timeout_secs > 0).then(|| Duration::from_secs(args.run_timeout_secs));
    let measured_runs = args.repeat.max(1);
    let total_runs = args.warmup_runs + measured_runs;
    let mut persistent_gateway = None;
    let mut persistent_seed = None;
    let persistent_gateway_url = if args.fresh_gateway_per_run {
        None
    } else if let Some(url) = args.gateway_url.as_deref() {
        Some(normalize_gateway_url(url))
    } else {
        persistent_seed = BitswapSeed::start_optional(args).await?;
        let gateway = SpawnedGateway::start(args, persistent_seed.as_ref()).await?;
        let url = gateway.url.clone();
        persistent_gateway = Some(gateway);
        Some(url)
    };

    let mut runs = Vec::new();
    for sequence in 0..total_runs {
        let phase = if sequence < args.warmup_runs {
            RunPhase::Warmup
        } else {
            RunPhase::Measured
        };
        let run_index = match phase {
            RunPhase::Warmup => sequence + 1,
            RunPhase::Measured => sequence - args.warmup_runs + 1,
        };

        let mut run_gateway = None;
        let mut run_seed = None;
        let gateway_url = if let Some(url) = &persistent_gateway_url {
            url.clone()
        } else {
            run_seed = BitswapSeed::start_optional(args).await?;
            let gateway = SpawnedGateway::start(args, run_seed.as_ref()).await?;
            let url = gateway.url.clone();
            run_gateway = Some(gateway);
            url
        };

        let started = Instant::now();
        let run = run_corpus_once(
            &gateway_url,
            corpus,
            timeout,
            args.engine,
            args.asset_concurrency,
            args.conditional_revalidate,
            &args.cases,
        );
        let results = if let Some(run_timeout) = run_timeout {
            match tokio::time::timeout(run_timeout, run).await {
                Ok(results) => results?,
                Err(_) => {
                    run_timeout_failure_results(&gateway_url, corpus, &args.cases, run_timeout)?
                }
            }
        } else {
            run.await?
        };
        let elapsed_ms = started.elapsed().as_millis();
        let gateway_rss_kib = if let Some(gateway) = run_gateway.as_ref() {
            gateway.rss_kib()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(SpawnedGateway::rss_kib)
        };
        let gateway_fd_count = if let Some(gateway) = run_gateway.as_ref() {
            gateway.fd_count()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(SpawnedGateway::fd_count)
        };
        let gateway_child_process_count = if let Some(gateway) = run_gateway.as_ref() {
            gateway.child_process_count()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(SpawnedGateway::child_process_count)
        };
        let gateway_storage_bytes = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_bytes()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(SpawnedGateway::storage_bytes)
        };
        let gateway_storage_path = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_path()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(SpawnedGateway::storage_path)
        };
        let bitswap_seed_connect_elapsed_ms = if let Some(gateway) = run_gateway.as_ref() {
            gateway.bitswap_seed_connect_elapsed_ms
        } else {
            persistent_gateway
                .as_ref()
                .and_then(|gateway| gateway.bitswap_seed_connect_elapsed_ms)
        };
        let kubo_bitswap_stats = if let Some(gateway) = run_gateway.as_ref() {
            gateway.kubo_bitswap_stats().await
        } else if let Some(gateway) = persistent_gateway.as_ref() {
            gateway.kubo_bitswap_stats().await
        } else {
            None
        };
        let passed = results.iter().all(|result| result.passed);
        runs.push(RunResult {
            phase,
            run_index,
            gateway_url,
            elapsed_ms,
            gateway_rss_kib,
            gateway_fd_count,
            gateway_child_process_count,
            gateway_storage_bytes,
            gateway_storage_path,
            bitswap_seed_connect_elapsed_ms,
            kubo_bitswap_stats,
            native_ffi: None,
            passed,
            results,
        });

        if let Some(mut gateway) = run_gateway {
            gateway.stop().await;
        }
        if let Some(mut seed) = run_seed {
            seed.stop().await;
        }
    }

    if let Some(mut gateway) = persistent_gateway {
        gateway.stop().await;
    }
    if let Some(mut seed) = persistent_seed {
        seed.stop().await;
    }

    let summary = RepeatSummary::from_runs(&runs);
    let trace_summary = args
        .trace_output
        .as_ref()
        .map(summarize_trace_output)
        .transpose()?;
    let trace_requirements = trace_requirements_report(trace_summary.as_ref(), args)?;
    Ok(RunReport {
        gateway_url: persistent_gateway_url,
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        repeat: measured_runs,
        warmup_runs: args.warmup_runs,
        fresh_gateway_per_run: args.fresh_gateway_per_run,
        asset_concurrency: args.asset_concurrency,
        conditional_revalidate: args.conditional_revalidate,
        run_timeout_secs: (args.run_timeout_secs > 0).then_some(args.run_timeout_secs),
        engine: args.engine,
        small_body_cache_max_bytes: args
            .engine
            .is_rust()
            .then_some(args.small_body_cache_max_bytes),
        gateway_db: args
            .gateway_db
            .as_ref()
            .map(|path| path.display().to_string()),
        gateway_import_car: args
            .gateway_import_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_car: args
            .bitswap_seed_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_connection_setup: bitswap_seed_connection_setup(args),
        kubo_repo: args
            .kubo_repo
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_output: args
            .trace_output
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_span_list: args.trace_output.as_ref().map(|_| args.trace_span_list),
        trace_summary,
        trace_requirements,
        summary,
        runs,
    })
}

async fn run_native_harness(args: &Args, corpus: &Corpus) -> Result<RunReport> {
    init_native_trace_output(args)?;
    let run_timeout =
        (args.run_timeout_secs > 0).then(|| Duration::from_secs(args.run_timeout_secs));
    let measured_runs = args.repeat.max(1);
    let total_runs = args.warmup_runs + measured_runs;
    let gateway_url = "http://freedom-ipfs-native.local".to_string();
    let mut persistent_gateway = None;
    let mut persistent_seed = None;
    let persistent_gateway_url = if args.fresh_gateway_per_run {
        None
    } else {
        persistent_seed = BitswapSeed::start_optional(args).await?;
        let gateway = NativeGateway::start(args, persistent_seed.as_ref()).await?;
        persistent_gateway = Some(gateway);
        Some(gateway_url.clone())
    };

    let mut runs = Vec::new();
    for sequence in 0..total_runs {
        let phase = if sequence < args.warmup_runs {
            RunPhase::Warmup
        } else {
            RunPhase::Measured
        };
        let run_index = match phase {
            RunPhase::Warmup => sequence + 1,
            RunPhase::Measured => sequence - args.warmup_runs + 1,
        };

        let mut run_gateway = None;
        let mut run_seed = None;
        let gateway = if let Some(gateway) = &persistent_gateway {
            gateway.clone()
        } else {
            run_seed = BitswapSeed::start_optional(args).await?;
            let gateway = NativeGateway::start(args, run_seed.as_ref()).await?;
            run_gateway = Some(gateway.clone());
            gateway
        };
        let client = GatewayClient::Native(gateway);

        let started = Instant::now();
        let run = run_corpus_once_with_client(
            &client,
            &gateway_url,
            corpus,
            args.engine,
            args.asset_concurrency,
            args.conditional_revalidate,
            &args.cases,
        );
        let results = if let Some(run_timeout) = run_timeout {
            match tokio::time::timeout(run_timeout, run).await {
                Ok(results) => results?,
                Err(_) => {
                    run_timeout_failure_results(&gateway_url, corpus, &args.cases, run_timeout)?
                }
            }
        } else {
            run.await?
        };
        let elapsed_ms = started.elapsed().as_millis();
        let gateway_storage_bytes = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_bytes()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(NativeGateway::storage_bytes)
        };
        let gateway_storage_path = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_path()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(NativeGateway::storage_path)
        };
        let passed = results.iter().all(|result| result.passed);
        runs.push(RunResult {
            phase,
            run_index,
            gateway_url: gateway_url.clone(),
            elapsed_ms,
            gateway_rss_kib: None,
            gateway_fd_count: None,
            gateway_child_process_count: None,
            gateway_storage_bytes,
            gateway_storage_path,
            bitswap_seed_connect_elapsed_ms: None,
            kubo_bitswap_stats: None,
            native_ffi: None,
            passed,
            results,
        });

        if let Some(mut gateway) = run_gateway {
            gateway.stop().await;
        }
        if let Some(mut seed) = run_seed {
            seed.stop().await;
        }
    }

    if let Some(mut gateway) = persistent_gateway {
        gateway.stop().await;
    }
    if let Some(mut seed) = persistent_seed {
        seed.stop().await;
    }

    let summary = RepeatSummary::from_runs(&runs);
    let trace_summary = args
        .trace_output
        .as_ref()
        .map(summarize_trace_output)
        .transpose()?;
    let trace_requirements = trace_requirements_report(trace_summary.as_ref(), args)?;
    Ok(RunReport {
        gateway_url: persistent_gateway_url,
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        repeat: measured_runs,
        warmup_runs: args.warmup_runs,
        fresh_gateway_per_run: args.fresh_gateway_per_run,
        asset_concurrency: args.asset_concurrency,
        conditional_revalidate: args.conditional_revalidate,
        run_timeout_secs: (args.run_timeout_secs > 0).then_some(args.run_timeout_secs),
        engine: args.engine,
        small_body_cache_max_bytes: Some(args.small_body_cache_max_bytes),
        gateway_db: args
            .gateway_db
            .as_ref()
            .map(|path| path.display().to_string()),
        gateway_import_car: args
            .gateway_import_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_car: args
            .bitswap_seed_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_connection_setup: bitswap_seed_connection_setup(args),
        kubo_repo: None,
        trace_output: args
            .trace_output
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_span_list: args.trace_output.as_ref().map(|_| args.trace_span_list),
        trace_summary,
        trace_requirements,
        summary,
        runs,
    })
}

async fn run_native_ffi_harness(args: &Args, corpus: &Corpus) -> Result<RunReport> {
    init_native_trace_output(args)?;
    let run_timeout =
        (args.run_timeout_secs > 0).then(|| Duration::from_secs(args.run_timeout_secs));
    let measured_runs = args.repeat.max(1);
    let total_runs = args.warmup_runs + measured_runs;
    let gateway_url = "http://freedom-ipfs-native.local".to_string();
    let mut persistent_gateway = None;
    let mut persistent_seed = None;
    let persistent_gateway_url = if args.fresh_gateway_per_run {
        None
    } else {
        persistent_seed = BitswapSeed::start_optional(args).await?;
        let gateway = NativeFfiGateway::start(args, persistent_seed.as_ref()).await?;
        persistent_gateway = Some(gateway);
        Some(gateway_url.clone())
    };

    let mut runs = Vec::new();
    for sequence in 0..total_runs {
        let phase = if sequence < args.warmup_runs {
            RunPhase::Warmup
        } else {
            RunPhase::Measured
        };
        let run_index = match phase {
            RunPhase::Warmup => sequence + 1,
            RunPhase::Measured => sequence - args.warmup_runs + 1,
        };

        let mut run_gateway = None;
        let mut run_seed = None;
        let gateway = if let Some(gateway) = &persistent_gateway {
            gateway.clone()
        } else {
            run_seed = BitswapSeed::start_optional(args).await?;
            let gateway = NativeFfiGateway::start(args, run_seed.as_ref()).await?;
            run_gateway = Some(gateway.clone());
            gateway
        };
        let client = GatewayClient::NativeFfi(gateway.clone());

        let stop_task = args.native_stop_node_mid_run_ms.map(|delay_ms| {
            let gateway = gateway.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                gateway.stop_node();
            })
        });

        let started = Instant::now();
        let run = run_corpus_once_with_client(
            &client,
            &gateway_url,
            corpus,
            args.engine,
            args.asset_concurrency,
            args.conditional_revalidate,
            &args.cases,
        );
        let results = if let Some(run_timeout) = run_timeout {
            match tokio::time::timeout(run_timeout, run).await {
                Ok(results) => results?,
                Err(_) => {
                    run_timeout_failure_results(&gateway_url, corpus, &args.cases, run_timeout)?
                }
            }
        } else {
            run.await?
        };
        if let Some(stop_task) = stop_task {
            stop_task.abort();
        }
        let elapsed_ms = started.elapsed().as_millis();
        let gateway_storage_bytes = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_bytes()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(NativeFfiGateway::storage_bytes)
        };
        let gateway_storage_path = if let Some(gateway) = run_gateway.as_ref() {
            gateway.storage_path()
        } else {
            persistent_gateway
                .as_ref()
                .and_then(NativeFfiGateway::storage_path)
        };
        let native_ffi = Some(gateway.report());
        let passed = results.iter().all(|result| result.passed);
        runs.push(RunResult {
            phase,
            run_index,
            gateway_url: gateway_url.clone(),
            elapsed_ms,
            gateway_rss_kib: None,
            gateway_fd_count: None,
            gateway_child_process_count: None,
            gateway_storage_bytes,
            gateway_storage_path,
            bitswap_seed_connect_elapsed_ms: None,
            kubo_bitswap_stats: None,
            native_ffi,
            passed,
            results,
        });

        if let Some(mut gateway) = run_gateway {
            gateway.stop().await;
        }
        if let Some(mut seed) = run_seed {
            seed.stop().await;
        }
    }

    if let Some(mut gateway) = persistent_gateway {
        gateway.stop().await;
    }
    if let Some(mut seed) = persistent_seed {
        seed.stop().await;
    }

    let summary = RepeatSummary::from_runs(&runs);
    let trace_summary = args
        .trace_output
        .as_ref()
        .map(summarize_trace_output)
        .transpose()?;
    let trace_requirements = trace_requirements_report(trace_summary.as_ref(), args)?;
    Ok(RunReport {
        gateway_url: persistent_gateway_url,
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        repeat: measured_runs,
        warmup_runs: args.warmup_runs,
        fresh_gateway_per_run: args.fresh_gateway_per_run,
        asset_concurrency: args.asset_concurrency,
        conditional_revalidate: args.conditional_revalidate,
        run_timeout_secs: (args.run_timeout_secs > 0).then_some(args.run_timeout_secs),
        engine: args.engine,
        small_body_cache_max_bytes: Some(args.small_body_cache_max_bytes),
        gateway_db: args
            .gateway_db
            .as_ref()
            .map(|path| path.display().to_string()),
        gateway_import_car: args
            .gateway_import_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_car: args
            .bitswap_seed_car
            .as_ref()
            .map(|path| path.display().to_string()),
        bitswap_seed_connection_setup: bitswap_seed_connection_setup(args),
        kubo_repo: None,
        trace_output: args
            .trace_output
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_span_list: args.trace_output.as_ref().map(|_| args.trace_span_list),
        trace_summary,
        trace_requirements,
        summary,
        runs,
    })
}

async fn run_corpus_once(
    gateway_url: &str,
    corpus: &Corpus,
    timeout: Duration,
    engine: HarnessEngine,
    asset_concurrency: usize,
    conditional_revalidate: bool,
    cases: &[String],
) -> Result<Vec<CaseResult>> {
    let client = GatewayClient::http(timeout)?;
    run_corpus_once_with_client(
        &client,
        gateway_url,
        corpus,
        engine,
        asset_concurrency,
        conditional_revalidate,
        cases,
    )
    .await
}

async fn run_corpus_once_with_client(
    client: &GatewayClient,
    gateway_url: &str,
    corpus: &Corpus,
    engine: HarnessEngine,
    asset_concurrency: usize,
    conditional_revalidate: bool,
    cases: &[String],
) -> Result<Vec<CaseResult>> {
    let mut results = Vec::new();
    for entry in &corpus.entries {
        if !entry_selected(entry, cases) {
            continue;
        }
        results.push(
            run_case(
                client,
                gateway_url,
                entry,
                engine,
                asset_concurrency,
                conditional_revalidate,
            )
            .await,
        );
    }
    if results.is_empty() {
        bail!("no corpus entries matched the requested case filters");
    }
    Ok(results)
}

#[derive(Clone)]
struct HarnessTraceFileWriter(Arc<std::fs::File>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for HarnessTraceFileWriter {
    type Writer = &'a std::fs::File;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.as_ref()
    }
}

fn init_native_trace_output(args: &Args) -> Result<()> {
    let Some(trace_output) = args.trace_output.as_deref() else {
        return Ok(());
    };
    let filter = if let Some(trace_filter) = args.trace_filter.as_deref() {
        tracing_subscriber::EnvFilter::try_new(trace_filter)
            .with_context(|| format!("parse trace filter {trace_filter:?}"))?
    } else {
        tracing_subscriber::EnvFilter::new(DEFAULT_NATIVE_TRACE_FILTER)
    };
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(trace_output)
        .with_context(|| format!("open native trace output {}", trace_output.display()))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(args.trace_span_list)
        .with_writer(HarnessTraceFileWriter(Arc::new(file)))
        .try_init()
        .map_err(|err| anyhow!("initialize native trace subscriber: {err}"))?;
    Ok(())
}

fn run_timeout_failure_results(
    gateway_url: &str,
    corpus: &Corpus,
    cases: &[String],
    timeout: Duration,
) -> Result<Vec<CaseResult>> {
    let mut results = Vec::new();
    for entry in &corpus.entries {
        if !entry_selected(entry, cases) {
            continue;
        }
        let url = format!("{}{}", gateway_url.trim_end_matches('/'), entry.path);
        results.push(CaseResult::failed(
            entry,
            url,
            vec![format!("run timed out after {}s", timeout.as_secs())],
        ));
    }
    if results.is_empty() {
        bail!("no corpus entries matched the requested case filters");
    }
    Ok(results)
}

fn entry_selected(entry: &CorpusEntry, cases: &[String]) -> bool {
    if cases.is_empty() {
        entry.default_enabled.unwrap_or(true)
    } else {
        cases.iter().any(|case| case == &entry.id)
    }
}

async fn run_case(
    client: &GatewayClient,
    gateway_url: &str,
    entry: &CorpusEntry,
    engine: HarnessEngine,
    asset_concurrency: usize,
    conditional_revalidate: bool,
) -> CaseResult {
    let url = format!("{}{}", gateway_url.trim_end_matches('/'), entry.path);
    let method = entry.method.as_deref().unwrap_or("GET");
    let correlation = RequestCorrelation::root(entry.path.clone());
    let response = match fetch_response(
        client,
        &url,
        method,
        entry.range.as_deref(),
        Some(&correlation),
    )
    .await
    {
        Ok(response) => response,
        Err(err) => {
            return CaseResult::failed(entry, url, vec![format!("request error: {err}")]);
        }
    };

    let body_preview =
        String::from_utf8_lossy(&response.body.iter().copied().take(180).collect::<Vec<_>>())
            .replace('\n', "\\n");
    let mut failures = Vec::new();

    if let Some(expected) = entry.expect_status {
        if response.status != expected {
            failures.push(format!("status {}, expected {expected}", response.status));
        }
    }
    if let Some(expected) = &entry.expect_content_type_prefix {
        if !response
            .content_type
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase()
            .starts_with(&expected.to_ascii_lowercase())
        {
            failures.push(format!(
                "content-type {:?}, expected prefix {expected:?}",
                response.content_type
            ));
        }
    }
    if let Some(expected) = &entry.expect_content_range_prefix {
        if !response
            .content_range
            .as_deref()
            .unwrap_or("")
            .starts_with(expected)
        {
            failures.push(format!(
                "content-range {:?}, expected prefix {expected:?}",
                response.content_range
            ));
        }
    }
    if let Some(expected) = entry.expect_content_length {
        if response.content_length != Some(expected) {
            failures.push(format!(
                "content-length {:?}, expected {expected}",
                response.content_length
            ));
        }
    }
    if let Some(expected) = &entry.expect_accept_ranges {
        match response.accept_ranges.as_deref() {
            Some(actual) if actual.eq_ignore_ascii_case(expected) => {}
            actual => failures.push(format!("accept-ranges {actual:?}, expected {expected:?}")),
        }
    }
    if gateway_specific_header_expectations_enabled(engine) {
        if let Some(expected) = &entry.expect_etag_prefix {
            match response.etag.as_deref() {
                Some(actual) if actual.starts_with(expected) => {}
                actual => failures.push(format!("etag {actual:?}, expected prefix {expected:?}")),
            }
        }
        if let Some(expected) = &entry.expect_cache_control {
            match response.cache_control.as_deref() {
                Some(actual) if actual.eq_ignore_ascii_case(expected) => {}
                actual => {
                    failures.push(format!("cache-control {actual:?}, expected {expected:?}"));
                }
            }
        }
    }
    if let Some(expected) = &entry.expect_body_contains {
        let text = String::from_utf8_lossy(&response.body);
        if !text.contains(expected) {
            failures.push(format!("body did not contain {expected:?}"));
        }
    }
    if let Some(expected) = entry.expect_body_bytes {
        if response.body.len() != expected {
            failures.push(format!(
                "body {} bytes, expected exactly {expected}",
                response.body.len()
            ));
        }
    }
    if let Some(expected) = &entry.expect_body_sha256 {
        let actual = sha256_hex(&response.body);
        if !actual.eq_ignore_ascii_case(expected) {
            failures.push(format!("body sha256 {actual}, expected {expected}"));
        }
    }
    if let Some(min_bytes) = entry.min_bytes {
        if response.body.len() < min_bytes {
            failures.push(format!(
                "body {} bytes, expected at least {min_bytes}",
                response.body.len()
            ));
        }
    }
    if let Some(max_ttfb_ms) = entry.max_ttfb_ms {
        if response.ttfb_ms > max_ttfb_ms as u128 {
            failures.push(format!(
                "TTFB {}ms exceeded {max_ttfb_ms}ms",
                response.ttfb_ms
            ));
        }
    }
    let revalidation = maybe_revalidate_response(
        client,
        &url,
        method,
        entry.range.as_deref(),
        &response,
        conditional_revalidate,
        Some(&correlation),
    )
    .await;
    if let Some(revalidation) = &revalidation {
        if !revalidation.passed {
            failures.extend(
                revalidation
                    .failures
                    .iter()
                    .map(|failure| format!("conditional revalidation: {failure}")),
            );
        }
    }

    let mut asset_summary = None;
    let mut assets = Vec::new();
    if let Some(crawl) = &entry.crawl {
        let (summary, mut crawled_assets, crawl_failures) = run_page_crawl(
            client,
            &url,
            &response.body,
            crawl,
            asset_concurrency,
            conditional_revalidate,
            &correlation,
        )
        .await;
        failures.extend(crawl_failures);
        asset_summary = Some(summary);
        assets.append(&mut crawled_assets);
    }

    CaseResult {
        id: entry.id.clone(),
        description: entry.description.clone(),
        method: method.to_string(),
        url,
        status: Some(response.status),
        content_type: response.content_type,
        content_range: response.content_range,
        content_length: response.content_length,
        accept_ranges: response.accept_ranges,
        etag: response.etag,
        cache_control: response.cache_control,
        body_bytes: response.body.len(),
        ttfb_ms: response.ttfb_ms,
        total_ms: response.total_ms,
        stream: response.stream,
        body_preview,
        revalidation,
        asset_summary,
        assets,
        passed: failures.is_empty(),
        failures,
    }
}

fn gateway_specific_header_expectations_enabled(engine: HarnessEngine) -> bool {
    engine.is_rust()
}

fn print_summary(report: &RunReport) {
    println!("engine: {}", report.engine.as_str());
    println!(
        "gateway: {}",
        report.gateway_url.as_deref().unwrap_or("fresh per run")
    );
    if let Some(gateway_db) = &report.gateway_db {
        println!("gateway_db: {gateway_db}");
    }
    if let Some(gateway_import_car) = &report.gateway_import_car {
        println!("gateway_import_car: {gateway_import_car}");
    }
    if let Some(bitswap_seed_car) = &report.bitswap_seed_car {
        println!("bitswap_seed_car: {bitswap_seed_car}");
    }
    if let Some(setup) = report.bitswap_seed_connection_setup {
        println!("bitswap_seed_connection_setup: {}", setup.as_str());
    }
    if let Some(kubo_repo) = &report.kubo_repo {
        println!("kubo_repo: {kubo_repo}");
    }
    println!(
        "runs: measured={} warmup={} fresh_gateway_per_run={} asset_concurrency={} conditional_revalidate={}",
        report.repeat,
        report.warmup_runs,
        report.fresh_gateway_per_run,
        report.asset_concurrency,
        report.conditional_revalidate
    );
    if let Some(run_timeout_secs) = report.run_timeout_secs {
        println!("run_timeout_secs: {run_timeout_secs}");
    }
    println!(
        "summary: passed={} failed={} pass_rate={:.1}%",
        report.summary.pass_count,
        report.summary.fail_count,
        report.summary.pass_rate * 100.0
    );
    if report.summary.has_resource_metrics() {
        println!(
            "resources: run_total={} rss_kib={} fds={} children={} storage_bytes={}",
            report.summary.run_total_ms,
            report.summary.gateway_rss_kib,
            report.summary.gateway_fd_count,
            report.summary.gateway_child_process_count,
            report.summary.gateway_storage_bytes
        );
    }
    if report.summary.bitswap_seed_connect_ms.count > 0 {
        println!(
            "bitswap_seed_connect_ms: {}",
            report.summary.bitswap_seed_connect_ms
        );
    }
    if report.summary.kubo_bitswap.has_values() {
        println!(
            "kubo_bitswap: blocks_received={} data_received={} blocks_sent={} data_sent={} dup_blocks_received={} dup_data_received={} messages_received={} peers={} wantlist={}",
            report.summary.kubo_bitswap.blocks_received,
            report.summary.kubo_bitswap.data_received,
            report.summary.kubo_bitswap.blocks_sent,
            report.summary.kubo_bitswap.data_sent,
            report.summary.kubo_bitswap.dup_blocks_received,
            report.summary.kubo_bitswap.dup_data_received,
            report.summary.kubo_bitswap.messages_received,
            report.summary.kubo_bitswap.peers_len,
            report.summary.kubo_bitswap.wantlist_len
        );
    }

    for case in &report.summary.cases {
        println!(
            "case {}: passed={} failed={} pass_rate={:.1}% root_ttfb={} root_total={} root_stream_first_byte={} root_stream_chunks={} root_stream_max_buffered={} asset_ttfb={} asset_total={} asset_stream_first_byte={} asset_stream_chunks={} asset_stream_max_buffered={}",
            case.id,
            case.pass_count,
            case.fail_count,
            case.pass_rate * 100.0,
            case.root_ttfb_ms,
            case.root_total_ms,
            case.root_stream_first_byte_ms,
            case.root_stream_chunks,
            case.root_stream_max_buffered_bytes,
            case.asset_ttfb_ms,
            case.asset_total_ms,
            case.asset_stream_first_byte_ms,
            case.asset_stream_chunks,
            case.asset_stream_max_buffered_bytes
        );
        if !case.asset_kind_failures.is_empty() {
            let failures = case
                .asset_kind_failures
                .iter()
                .map(|failure| format!("{}={}", failure.kind, failure.count))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  failed asset kinds: {failures}");
        }
        for group in case.failure_groups.iter().take(12) {
            println!("  failure x{}: {}", group.count, group.key);
            for example in &group.examples {
                println!("    - {example}");
            }
        }
        if case.root_revalidation_attempts > 0 || case.asset_revalidation_attempts > 0 {
            println!(
                "  revalidation: root={}/{} failed={} ttfb={} assets={}/{} failed={} ttfb={}",
                case.root_revalidation_passed,
                case.root_revalidation_attempts,
                case.root_revalidation_failed,
                case.root_revalidation_ttfb_ms,
                case.asset_revalidation_passed,
                case.asset_revalidation_attempts,
                case.asset_revalidation_failed,
                case.asset_revalidation_ttfb_ms
            );
        }
    }

    if let Some(trace) = &report.trace_summary {
        println!(
            "trace: path={} lines={} events={} phases={}",
            report.trace_output.as_deref().unwrap_or("-"),
            trace.line_count,
            trace.event_count,
            trace.phases.len()
        );
        for phase in trace.phases.iter().take(16) {
            println!(
                "  phase {}: count={} total={}ms latency={}",
                phase.phase, phase.count, phase.total_ms, phase.elapsed_ms
            );
        }
        print_trace_progress_phases(trace);
        if !trace.block_sources.is_empty() {
            println!(
                "  block sources: {}",
                format_trace_counts(&trace.block_sources)
            );
        }
        print_trace_block_fetch_source_latencies(trace);
        if trace.block_store.events > 0 || trace.block_store.puts > 0 {
            let store = &trace.block_store;
            println!(
                "  block store: events={} hits={} misses={} rechecks={} recheck_hits={} recheck_misses={} puts={} put_bytes={} put_failures={} put_total_ms={} put_max_ms={}",
                store.events,
                store.hits,
                store.misses,
                store.rechecks,
                store.recheck_hits,
                store.recheck_misses,
                store.puts,
                store.put_bytes,
                store.put_failures,
                store.put_total_ms,
                store.put_max_ms
            );
        }
        print_trace_block_range_batch_fetches(trace);
        print_trace_provider_retries(trace);
        print_trace_delegated_provider_lookup(trace);
        print_trace_dht_provider_lookup(trace);
        print_trace_provider_diversity_low(trace);
        print_trace_request_classifications(trace);
        if !trace.request_statuses.is_empty() || trace.gateway_limiter_denials > 0 {
            println!(
                "  gateway responses: statuses={} limiter_denials={} elapsed={}",
                format_trace_counts(&trace.request_statuses),
                trace.gateway_limiter_denials,
                trace.gateway_request_elapsed_ms
            );
        }
        print_trace_gateway_limiter(trace);
        print_trace_unixfs_metadata_cache(trace);
        print_trace_gateway_small_body_cache(trace);
        print_trace_gateway_direct_body(trace);
        print_trace_gateway_stream_body(trace);
        print_trace_http_provider_races(trace);
        print_trace_http_provider_fetches(trace);
        print_trace_bitswap_sources(trace);
        print_trace_bitswap_batches(trace);
        print_trace_bitswap_incoming_batches(trace);
        if trace.bitswap_extra_blocks.events > 0 {
            let extra = &trace.bitswap_extra_blocks;
            println!(
                "  bitswap extra blocks: events={} total={} max={} incoming={} outgoing={} unknown={}",
                extra.events,
                extra.total,
                extra.max,
                extra.incoming,
                extra.outgoing,
                extra.unknown
            );
        }
        print_trace_bitswap_peer_fetches(trace);
        print_trace_bitswap_want_have_probes(trace);
        if trace.bitswap_session.has_events() {
            let session = &trace.bitswap_session;
            println!(
                "  bitswap session: fetches={} with_trusted={} trusted_successes={} untrusted_successes={} trusted_failures={} request_timeouts_with_trusted={} shortcut_starts={} shortcut_pre_lookup_waits={} pre_lookup_hits={} pre_lookup_misses={} pre_lookup_timeouts={} pre_lookup_max={}ms pre_lookup_budgets={} shortcut_post_lookup_waits={} post_lookup_hits={} post_lookup_misses={} post_lookup_timeouts={} post_lookup_errors={} post_lookup_max={}ms post_lookup_budgets={} post_lookup_timeout_budgets={} post_lookup_http_counts={} shortcut_attempts={} shortcut_hits={} shortcut_misses={} late_peer_waits={} late_peer_hits={} late_peer_misses={} late_peer_max={}ms",
                session.fetches,
                session.with_trusted_peers,
                session.trusted_successes,
                session.untrusted_successes,
                session.trusted_failures,
                session.request_timeouts_with_trusted,
                session.session_shortcut_starts,
                session.session_shortcut_pre_lookup_waits,
                session.session_shortcut_pre_lookup_hits,
                session.session_shortcut_pre_lookup_misses,
                session.session_shortcut_pre_lookup_timeouts,
                session.session_shortcut_pre_lookup_max_ms,
                format_trace_counts(&sorted_trace_counts(
                    session.session_shortcut_pre_lookup_budgets.clone()
                )),
                session.session_shortcut_post_lookup_waits,
                session.session_shortcut_post_lookup_hits,
                session.session_shortcut_post_lookup_misses,
                session.session_shortcut_post_lookup_timeouts,
                session.session_shortcut_post_lookup_errors,
                session.session_shortcut_post_lookup_max_ms,
                format_trace_counts(&sorted_trace_counts(
                    session.session_shortcut_post_lookup_budgets.clone()
                )),
                format_trace_counts(&sorted_trace_counts(
                    session
                        .session_shortcut_post_lookup_timeout_budgets
                        .clone()
                )),
                format_trace_counts(&sorted_trace_counts(
                    session
                        .session_shortcut_post_lookup_http_provider_counts
                        .clone()
                )),
                session.session_shortcut_attempts,
                session.session_shortcut_hits,
                session.session_shortcut_misses,
                session.session_late_peer_waits,
                session.session_late_peer_hits,
                session.session_late_peer_misses,
                session.session_late_peer_max_ms
            );
            if session.session_shortcut_pre_lookup_waits > 0 {
                println!(
                    "    pre-lookup latency: elapsed={} hits={} misses={} timeouts={}",
                    session.session_shortcut_pre_lookup_elapsed_ms,
                    session.session_shortcut_pre_lookup_hit_elapsed_ms,
                    session.session_shortcut_pre_lookup_miss_elapsed_ms,
                    session.session_shortcut_pre_lookup_timeout_elapsed_ms
                );
            }
            if session.session_shortcut_post_lookup_waits > 0 {
                println!(
                    "    post-lookup latency: elapsed={} hits={} timeouts={} single_http={} single_http_hits={} single_http_timeouts={}",
                    session.session_shortcut_post_lookup_elapsed_ms,
                    session.session_shortcut_post_lookup_hit_elapsed_ms,
                    session.session_shortcut_post_lookup_timeout_elapsed_ms,
                    session.session_shortcut_post_lookup_single_http_elapsed_ms,
                    session.session_shortcut_post_lookup_single_http_hit_elapsed_ms,
                    session.session_shortcut_post_lookup_single_http_timeout_elapsed_ms
                );
            }
            if session.session_shortcut_post_lookup_races > 0 {
                println!(
                    "    post-lookup race: events={} provider_wins={} bitswap_wins={} errors={} provider_bitswap_wins={} single_http_provider_bitswap_wins={} elapsed={} provider_result_elapsed={} single_http_provider_bitswap_elapsed={} outcomes={} sources={} http_counts={}",
                    session.session_shortcut_post_lookup_races,
                    session.session_shortcut_post_lookup_race_provider_wins,
                    session.session_shortcut_post_lookup_race_bitswap_wins,
                    session.session_shortcut_post_lookup_race_errors,
                    session.session_shortcut_post_lookup_race_provider_bitswap_wins,
                    session
                        .session_shortcut_post_lookup_race_single_http_provider_bitswap_wins,
                    session.session_shortcut_post_lookup_race_elapsed_ms,
                    session.session_shortcut_post_lookup_race_provider_result_elapsed_ms,
                    session
                        .session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_ms,
                    format_trace_counts(&session.session_shortcut_post_lookup_race_outcomes),
                    format_trace_counts(&session.session_shortcut_post_lookup_race_sources),
                    format_trace_counts(
                        &session.session_shortcut_post_lookup_race_http_provider_counts
                    )
                );
            }
            if session.session_late_peer_waits > 0 {
                println!(
                    "    late-peer latency: elapsed={} hits={} misses={}",
                    session.session_late_peer_elapsed_ms,
                    session.session_late_peer_hit_elapsed_ms,
                    session.session_late_peer_miss_elapsed_ms
                );
            }
        }
        print_trace_timeout_recovery(trace);
        print_trace_bitswap_peer_attempts(trace);
        print_trace_bitswap_dial_plans(trace);
        print_trace_bitswap_provider_expansion(trace);
        if trace.bitswap_incoming_blocks.matches > 0 {
            let incoming = &trace.bitswap_incoming_blocks;
            println!(
                "  bitswap incoming blocks: matches={} blocks={} bytes={} delivered_waiters={} dropped_waiters={} max_oldest_pending_ms={} max_pending_waiters={} max_dropped_waiters={}",
                incoming.matches,
                incoming.blocks,
                incoming.bytes,
                incoming.delivered_waiters,
                incoming.dropped_waiters,
                incoming.max_oldest_pending_ms,
                incoming.max_pending_waiters,
                incoming.max_dropped_waiters
            );
        }
        print_trace_bitswap_incoming_reads(trace);
        if !trace.trace_errors.is_empty() {
            println!(
                "  trace errors: {}",
                format_trace_counts(&trace.trace_errors)
            );
        }
        if !trace.bitswap_addr_mix.is_empty() {
            println!(
                "  bitswap addr mix: {}",
                format_trace_counts(&trace.bitswap_addr_mix)
            );
        }
        if trace.bitswap_provider_quality.events > 0 {
            let quality = &trace.bitswap_provider_quality;
            println!(
                "  bitswap provider quality: events={} provider_addrs={} expanded={} supported={} rejected={} id_only={} no_supported={} relay={} webtransport={} webrtc={} certhash={} other_transport={} missing_peer={} unparsable={} with_relay={} with_webtransport={} with_webrtc={} with_certhash={}",
                quality.events,
                quality.provider_addr_count,
                quality.expanded_provider_addr_count,
                quality.supported_provider_addr_count,
                quality.rejected_provider_addr_count,
                quality.id_only_provider_count,
                quality.provider_without_supported_bitswap_addr_count,
                quality.unsupported_relay_addr_count,
                quality.unsupported_webtransport_addr_count,
                quality.unsupported_webrtc_addr_count,
                quality.unsupported_certhash_addr_count,
                quality.unsupported_transport_addr_count,
                quality.missing_peer_addr_count,
                quality.unparsable_addr_count,
                quality.addr_with_relay_count,
                quality.addr_with_webtransport_count,
                quality.addr_with_webrtc_count,
                quality.addr_with_certhash_count
            );
        }
        print_trace_connection_established(trace);
        print_trace_connection_errors(trace);
        print_trace_connection_backoff(trace);
        print_trace_dial_rejections(trace);
        if trace.bitswap_dns_expansion.events > 0 {
            let dns = &trace.bitswap_dns_expansion;
            println!(
                "  bitswap dns expansion: events={} cached={} uncached={} failed={} records={} ips={}",
                dns.events,
                dns.cached,
                dns.uncached,
                dns.failed,
                dns.records,
                dns.ips
            );
        }
        print_trace_slow_details(trace);
        print_trace_progress_request_groups(trace);
    }

    if report.repeat == 1 && report.warmup_runs == 0 {
        for run in report
            .runs
            .iter()
            .filter(|run| run.phase == RunPhase::Measured)
        {
            for result in &run.results {
                print_case_result(result);
            }
        }
    } else {
        for run in report
            .runs
            .iter()
            .filter(|run| run.phase == RunPhase::Measured)
        {
            let mark = if run.passed { "PASS" } else { "FAIL" };
            let rss = run
                .gateway_rss_kib
                .map(|rss| format!(" rss={rss}KiB"))
                .unwrap_or_default();
            let fds = run
                .gateway_fd_count
                .map(|fds| format!(" fds={fds}"))
                .unwrap_or_default();
            let children = run
                .gateway_child_process_count
                .map(|children| format!(" children={children}"))
                .unwrap_or_default();
            let storage = run
                .gateway_storage_bytes
                .map(|bytes| format!(" storage={bytes}B"))
                .unwrap_or_default();
            println!(
                "{mark} measured run {:02} total={}ms{rss}{fds}{children}{storage}",
                run.run_index, run.elapsed_ms
            );
        }
    }
}

fn print_comparison_summary(report: &ComparisonReport) {
    println!("comparison: rust vs kubo");
    println!(
        "rust: passed={} failed={} pass_rate={:.1}%",
        report.rust.summary.pass_count,
        report.rust.summary.fail_count,
        report.rust.summary.pass_rate * 100.0
    );
    println!(
        "kubo: passed={} failed={} pass_rate={:.1}%",
        report.kubo.summary.pass_count,
        report.kubo.summary.fail_count,
        report.kubo.summary.pass_rate * 100.0
    );
    if report.rust.summary.bitswap_seed_connect_ms.count > 0
        || report.kubo.summary.bitswap_seed_connect_ms.count > 0
    {
        println!(
            "bitswap seed setup: rust={} kubo={}",
            display_option_seed_setup(report.rust.bitswap_seed_connection_setup),
            display_option_seed_setup(report.kubo.bitswap_seed_connection_setup)
        );
        println!(
            "bitswap seed connect: rust={} kubo={}",
            report.rust.summary.bitswap_seed_connect_ms,
            report.kubo.summary.bitswap_seed_connect_ms
        );
    }
    if report.kubo.summary.kubo_bitswap.has_values() {
        println!(
            "kubo_bitswap: blocks_received={} data_received={} blocks_sent={} data_sent={} dup_blocks_received={} dup_data_received={} messages_received={} peers={} wantlist={}",
            report.kubo.summary.kubo_bitswap.blocks_received,
            report.kubo.summary.kubo_bitswap.data_received,
            report.kubo.summary.kubo_bitswap.blocks_sent,
            report.kubo.summary.kubo_bitswap.data_sent,
            report.kubo.summary.kubo_bitswap.dup_blocks_received,
            report.kubo.summary.kubo_bitswap.dup_data_received,
            report.kubo.summary.kubo_bitswap.messages_received,
            report.kubo.summary.kubo_bitswap.peers_len,
            report.kubo.summary.kubo_bitswap.wantlist_len
        );
    }
    for case in &report.cases {
        println!(
            "case {}: pass_rate rust={:.1}% kubo={:.1}%",
            case.id,
            case.rust_pass_rate * 100.0,
            case.kubo_pass_rate * 100.0
        );
        println!(
            "  root_ttfb: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
            display_option_ms(case.rust_root_ttfb_p50_ms),
            display_option_ms(case.kubo_root_ttfb_p50_ms),
            display_option_f64(case.root_ttfb_p50_ratio),
            display_option_ms(case.rust_root_ttfb_p95_ms),
            display_option_ms(case.kubo_root_ttfb_p95_ms),
            display_option_f64(case.root_ttfb_p95_ratio)
        );
        println!(
            "  root_total: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
            display_option_ms(case.rust_root_total_p50_ms),
            display_option_ms(case.kubo_root_total_p50_ms),
            display_option_f64(case.root_total_p50_ratio),
            display_option_ms(case.rust_root_total_p95_ms),
            display_option_ms(case.kubo_root_total_p95_ms),
            display_option_f64(case.root_total_p95_ratio)
        );
        if case.kubo_setup_adjusted_root_ttfb_p50_ms.is_some()
            || case.kubo_setup_adjusted_root_ttfb_p95_ms.is_some()
        {
            println!(
                "  root_ttfb_kubo_setup_adjusted: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
                display_option_ms(case.rust_root_ttfb_p50_ms),
                display_option_ms(case.kubo_setup_adjusted_root_ttfb_p50_ms),
                display_option_f64(case.setup_adjusted_root_ttfb_p50_ratio),
                display_option_ms(case.rust_root_ttfb_p95_ms),
                display_option_ms(case.kubo_setup_adjusted_root_ttfb_p95_ms),
                display_option_f64(case.setup_adjusted_root_ttfb_p95_ratio)
            );
        }
        println!(
            "  asset_ttfb: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
            display_option_ms(case.rust_asset_ttfb_p50_ms),
            display_option_ms(case.kubo_asset_ttfb_p50_ms),
            display_option_f64(case.asset_ttfb_p50_ratio),
            display_option_ms(case.rust_asset_ttfb_p95_ms),
            display_option_ms(case.kubo_asset_ttfb_p95_ms),
            display_option_f64(case.asset_ttfb_p95_ratio)
        );
        println!(
            "  asset_total: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
            display_option_ms(case.rust_asset_total_p50_ms),
            display_option_ms(case.kubo_asset_total_p50_ms),
            display_option_f64(case.asset_total_p50_ratio),
            display_option_ms(case.rust_asset_total_p95_ms),
            display_option_ms(case.kubo_asset_total_p95_ms),
            display_option_f64(case.asset_total_p95_ratio)
        );
        print_case_asset_kubo_wins(case);
        println!(
            "  resources: rust_rss_max={} kubo_rss_max={} rss_ratio={} rust_fd_max={} kubo_fd_max={} fd_ratio={} rust_storage_max={} kubo_storage_max={} storage_ratio={}",
            display_option_u64_unit(case.rust_max_rss_kib, "KiB"),
            display_option_u64_unit(case.kubo_max_rss_kib, "KiB"),
            display_option_f64(case.rss_ratio),
            display_option_u64(case.rust_max_fd_count),
            display_option_u64(case.kubo_max_fd_count),
            display_option_f64(case.fd_ratio),
            display_option_u64_unit(case.rust_max_storage_bytes, "B"),
            display_option_u64_unit(case.kubo_max_storage_bytes, "B"),
            display_option_f64(case.storage_ratio)
        );
    }
    print_meaningful_kubo_wins(report);
    print_comparison_trace_summary("rust", &report.rust);
    print_comparison_trace_summary("kubo", &report.kubo);
}

fn print_case_asset_kubo_wins(case: &ComparisonCase) {
    let mut wins = case
        .asset_comparisons
        .iter()
        .flat_map(|asset| {
            asset
                .meaningful_kubo_wins
                .iter()
                .map(move |win| (asset, win))
        })
        .collect::<Vec<_>>();
    if wins.is_empty() {
        return;
    }
    wins.sort_by(|(left_asset, left_win), (right_asset, right_win)| {
        right_win
            .delta_ms
            .cmp(&left_win.delta_ms)
            .then_with(|| left_asset.path.cmp(&right_asset.path))
            .then_with(|| left_win.metric.cmp(&right_win.metric))
    });
    println!(
        "  asset_kubo_wins: {} path/metric pair(s), showing top {}",
        wins.len(),
        wins.len().min(MAX_PRINTED_ASSET_KUBO_WINS)
    );
    for (asset, win) in wins.into_iter().take(MAX_PRINTED_ASSET_KUBO_WINS) {
        let rust_trace_details = format_comparison_asset_trace_details(asset.rust_trace.as_ref());
        println!(
            "    {} {} kind={} rust={} kubo={} delta={}ms ratio={:.2}x samples=rust:{}/{} kubo:{}/{}{}",
            asset.path,
            win.metric,
            asset.kind,
            display_option_ms(Some(win.rust_ms)),
            display_option_ms(Some(win.kubo_ms)),
            win.delta_ms,
            win.ratio,
            asset.rust_pass_count,
            asset.rust_count,
            asset.kubo_pass_count,
            asset.kubo_count,
            rust_trace_details
        );
    }
}

fn format_comparison_asset_trace_details(trace: Option<&TraceRequestPathAggregate>) -> String {
    let Some(trace) = trace else {
        return String::new();
    };
    let statuses = if trace.statuses.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&trace.statuses)
    };
    let classifications = if trace.classifications.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&trace.classifications)
    };
    let block_sources = if trace.block_sources.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&trace.block_sources)
    };
    let http_providers = if trace.http_provider_fetch_providers.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&trace.http_provider_fetch_providers)
    };
    let http_milestones = format_trace_path_http_milestones(trace);
    let phases = if trace.phase_latencies.is_empty() {
        "-".to_string()
    } else {
        trace
            .phase_latencies
            .iter()
            .take(4)
            .map(|phase| {
                format!(
                    "{}:{} total={}ms max={}ms",
                    phase.phase, phase.count, phase.total_ms, phase.max_ms
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let bitswap_details = if trace.bitswap_fetches == 0
        && trace.bitswap_source_candidate_indexes.is_empty()
        && trace.bitswap_source_request_modes.is_empty()
        && trace.bitswap_source_transports.is_empty()
    {
        String::new()
    } else {
        let source_indexes = if trace.bitswap_source_candidate_indexes.is_empty() {
            "-".to_string()
        } else {
            format_trace_counts(&trace.bitswap_source_candidate_indexes)
        };
        let source_modes = if trace.bitswap_source_request_modes.is_empty() {
            "-".to_string()
        } else {
            format_trace_counts(&trace.bitswap_source_request_modes)
        };
        let source_transports = if trace.bitswap_source_transports.is_empty() {
            "-".to_string()
        } else {
            format_trace_counts(&trace.bitswap_source_transports)
        };
        format!(
            " bitswap_fetches={} bitswap_max={}ms bitswap_bytes={} bitswap_indexes={} bitswap_modes={} bitswap_transports={}",
            trace.bitswap_fetches,
            trace.bitswap_fetch_max_ms,
            trace.bitswap_fetch_bytes,
            source_indexes,
            source_modes,
            source_transports
        )
    };
    let dht_details = format_trace_path_dht_details(trace);
    let unixfs_cache_details = format_trace_unixfs_cache_details(&trace.unixfs_metadata_cache);
    format!(
        " rust_trace=requests={} elapsed={} max_event={}ms statuses={} classifications={} block_sources={} http_fetches={} ok={} fail={} http_max={}ms http_providers={}{} phases={}{}{}{}",
        trace.request_count,
        trace.request_elapsed_ms,
        trace.max_event_ms,
        statuses,
        classifications,
        block_sources,
        trace.http_provider_fetches,
        trace.http_provider_fetch_successes,
        trace.http_provider_fetch_failures,
        trace.http_provider_fetch_max_ms,
        http_providers,
        http_milestones,
        phases,
        bitswap_details,
        dht_details,
        unixfs_cache_details
    )
}

fn format_trace_unixfs_cache_details(cache: &TraceUnixfsMetadataCacheAggregate) -> String {
    if !cache.has_events() {
        return String::new();
    }
    format!(
        " unixfs_cache=events={} metadata={}/{}/{} path={}/{}/{} file_size={}/{}/{}",
        cache.events,
        cache.hits,
        cache.misses,
        cache.inserts,
        cache.path_hits,
        cache.path_misses,
        cache.path_inserts,
        cache.file_size_hits,
        cache.file_size_misses,
        cache.file_size_inserts
    )
}

fn format_trace_path_http_milestones(trace: &TraceRequestPathAggregate) -> String {
    if trace.http_provider_fetch_response_bytes == 0
        && trace.http_provider_fetch_first_chunk_events == 0
        && trace.http_provider_fetch_headers_max_ms == 0
        && trace.http_provider_fetch_first_chunk_max_ms == 0
        && trace.http_provider_fetch_body_max_ms == 0
    {
        return String::new();
    }
    format!(
        " http_response_bytes={} http_first_chunks={} http_header_max={}ms http_first_chunk_max={}ms http_body_max={}ms",
        trace.http_provider_fetch_response_bytes,
        trace.http_provider_fetch_first_chunk_events,
        trace.http_provider_fetch_headers_max_ms,
        trace.http_provider_fetch_first_chunk_max_ms,
        trace.http_provider_fetch_body_max_ms
    )
}

fn format_trace_path_dht_details(trace: &TraceRequestPathAggregate) -> String {
    if trace.provider_diversity_low_events == 0 && trace.dht_provider_lookup_events == 0 {
        return String::new();
    }
    format!(
        " low_diversity={} low_diversity_fail={} low_diversity_max_providers={} low_diversity_max_bitswap_providers={} low_diversity_timeout={}ms dht_events={} dht_fail={} dht_providers={} dht_max={}ms dht_timeout={}ms",
        trace.provider_diversity_low_events,
        trace.provider_diversity_low_failures,
        trace.provider_diversity_low_max_provider_count,
        trace.provider_diversity_low_max_bitswap_provider_count,
        trace.provider_diversity_low_max_timeout_ms,
        trace.dht_provider_lookup_events,
        trace.dht_provider_lookup_failures,
        trace.dht_provider_lookup_providers,
        trace.dht_provider_lookup_max_elapsed_ms,
        trace.dht_provider_lookup_max_timeout_ms
    )
}

fn format_trace_request_dht_details(request: &TraceRequestAggregate) -> String {
    if request.provider_diversity_low_events == 0 && request.dht_provider_lookup_events == 0 {
        return String::new();
    }
    format!(
        " low_diversity={} low_diversity_fail={} low_diversity_max_providers={} low_diversity_max_bitswap_providers={} low_diversity_timeout={}ms dht_events={} dht_fail={} dht_providers={} dht_max={}ms dht_timeout={}ms",
        request.provider_diversity_low_events,
        request.provider_diversity_low_failures,
        request.provider_diversity_low_max_provider_count,
        request.provider_diversity_low_max_bitswap_provider_count,
        request.provider_diversity_low_max_timeout_ms,
        request.dht_provider_lookup_events,
        request.dht_provider_lookup_failures,
        request.dht_provider_lookup_providers,
        request.dht_provider_lookup_max_elapsed_ms,
        request.dht_provider_lookup_max_timeout_ms
    )
}

fn print_meaningful_kubo_wins(report: &ComparisonReport) {
    let wins = report
        .cases
        .iter()
        .flat_map(|case| {
            case.meaningful_kubo_wins
                .iter()
                .map(move |win| (case.id.as_str(), win))
        })
        .collect::<Vec<_>>();
    if wins.is_empty() {
        println!(
            "meaningful_kubo_wins: none (delta>={MEANINGFUL_KUBO_WIN_MIN_DELTA_MS}ms ratio>={MEANINGFUL_KUBO_WIN_MIN_RATIO:.2}x)"
        );
        return;
    }

    println!(
        "meaningful_kubo_wins: {} (delta>={}ms ratio>={:.2}x)",
        wins.len(),
        MEANINGFUL_KUBO_WIN_MIN_DELTA_MS,
        MEANINGFUL_KUBO_WIN_MIN_RATIO
    );
    for (case_id, win) in wins {
        println!(
            "  {} {}: rust={} kubo={} delta={}ms ratio={:.2}x",
            case_id,
            win.metric,
            display_option_ms(Some(win.rust_ms)),
            display_option_ms(Some(win.kubo_ms)),
            win.delta_ms,
            win.ratio
        );
    }
}

fn print_offline_replay_summary(report: &OfflineReplayReport) {
    println!("offline replay db: {}", report.replay_db);
    if report.resolved_ipfs_replay {
        println!(
            "resolved-IPFS replay: rewrote {} path(s)",
            report.resolved_ipfs_rewrites.len()
        );
        for rewrite in report.resolved_ipfs_rewrites.iter().take(12) {
            println!(
                "  rewrite {}: {} -> {}",
                rewrite.case_id, rewrite.original_path, rewrite.rewritten_path
            );
        }
    }
    println!(
        "online: passed={} failed={} pass_rate={:.1}%",
        report.online.summary.pass_count,
        report.online.summary.fail_count,
        report.online.summary.pass_rate * 100.0
    );
    println!(
        "offline: passed={} failed={} pass_rate={:.1}% missing_urls={} storage_bytes={}",
        report.offline.summary.pass_count,
        report.offline.summary.fail_count,
        report.offline.summary.pass_rate * 100.0,
        report.summary.missing_url_count,
        display_option_u64_unit(report.summary.offline_storage_bytes, "B")
    );
    for missing in report.summary.missing_urls.iter().take(12) {
        println!("  missing {}: {}", missing.kind, missing.url);
        for failure in &missing.failures {
            println!("    - {failure}");
        }
    }
    if !report.summary.offline_request_statuses.is_empty() {
        println!(
            "  offline statuses: {}",
            format_trace_counts(&report.summary.offline_request_statuses)
        );
    }
    if !report.summary.offline_network_phases.is_empty() {
        println!(
            "  offline network phases: {}",
            format_trace_counts(&report.summary.offline_network_phases)
        );
    } else {
        println!("  offline network phases: none");
    }
    if !report.summary.offline_cache_phases.is_empty() {
        println!(
            "  offline cache phases: {}",
            format_trace_counts(&report.summary.offline_cache_phases)
        );
    }
    if !report.summary.offline_block_sources.is_empty() {
        println!(
            "  offline block sources: {}",
            format_trace_counts(&report.summary.offline_block_sources)
        );
    }
    if !report.summary.offline_non_cache_block_sources.is_empty() {
        println!(
            "  offline non-cache block sources: {}",
            format_trace_counts(&report.summary.offline_non_cache_block_sources)
        );
    } else {
        println!("  offline non-cache block sources: none");
    }
    if !report.summary.offline_trace_errors.is_empty() {
        println!(
            "  offline trace errors: {}",
            format_trace_counts(&report.summary.offline_trace_errors)
        );
    }
    if !report.summary.offline_progress_phases.is_empty() {
        println!(
            "  offline progress phases: {}",
            format_trace_counts(&report.summary.offline_progress_phases)
        );
    }
}

fn print_comparison_trace_summary(label: &str, report: &RunReport) {
    let Some(trace) = &report.trace_summary else {
        return;
    };
    println!(
        "{label} trace: path={} lines={} events={} phases={}",
        report.trace_output.as_deref().unwrap_or("-"),
        trace.line_count,
        trace.event_count,
        trace.phases.len()
    );
    if trace.block_store.events > 0 || trace.block_store.puts > 0 {
        let store = &trace.block_store;
        println!(
            "  block store: events={} hits={} misses={} rechecks={} recheck_hits={} recheck_misses={} puts={} put_bytes={} put_failures={} put_total_ms={} put_max_ms={}",
            store.events,
            store.hits,
            store.misses,
            store.rechecks,
            store.recheck_hits,
            store.recheck_misses,
            store.puts,
            store.put_bytes,
            store.put_failures,
            store.put_total_ms,
            store.put_max_ms
        );
    }
    print_trace_block_range_batch_fetches(trace);
    print_trace_unixfs_metadata_cache(trace);
    print_trace_progress_phases(trace);
    print_trace_block_fetch_source_latencies(trace);
    print_trace_provider_retries(trace);
    print_trace_delegated_provider_lookup(trace);
    print_trace_dht_provider_lookup(trace);
    print_trace_provider_diversity_low(trace);
    print_trace_request_classifications(trace);
    if !trace.request_statuses.is_empty() || trace.gateway_limiter_denials > 0 {
        println!(
            "  gateway responses: statuses={} limiter_denials={} elapsed={}",
            format_trace_counts(&trace.request_statuses),
            trace.gateway_limiter_denials,
            trace.gateway_request_elapsed_ms
        );
    }
    print_trace_gateway_limiter(trace);
    print_trace_progress_request_groups(trace);
    print_trace_timeout_recovery(trace);
    print_trace_gateway_small_body_cache(trace);
    print_trace_gateway_direct_body(trace);
    print_trace_gateway_stream_body(trace);
    print_trace_http_provider_races(trace);
    print_trace_http_provider_fetches(trace);
    print_trace_bitswap_peer_attempts(trace);
    print_trace_bitswap_want_have_probes(trace);
    print_trace_bitswap_dial_plans(trace);
    print_trace_bitswap_provider_expansion(trace);
    print_trace_bitswap_sources(trace);
    print_trace_bitswap_batches(trace);
    print_trace_bitswap_incoming_batches(trace);
    print_trace_bitswap_peer_fetches(trace);
    if trace.bitswap_session.has_events() {
        let session = &trace.bitswap_session;
        println!(
            "  bitswap session: fetches={} with_trusted={} trusted_successes={} untrusted_successes={} trusted_failures={} request_timeouts_with_trusted={} shortcut_starts={} shortcut_pre_lookup_waits={} pre_lookup_hits={} pre_lookup_misses={} pre_lookup_timeouts={} pre_lookup_max={}ms pre_lookup_budgets={} shortcut_post_lookup_waits={} post_lookup_hits={} post_lookup_misses={} post_lookup_timeouts={} post_lookup_errors={} post_lookup_max={}ms post_lookup_budgets={} post_lookup_timeout_budgets={} post_lookup_http_counts={} shortcut_attempts={} shortcut_hits={} shortcut_misses={} late_peer_waits={} late_peer_hits={} late_peer_misses={} late_peer_max={}ms",
            session.fetches,
            session.with_trusted_peers,
            session.trusted_successes,
            session.untrusted_successes,
            session.trusted_failures,
            session.request_timeouts_with_trusted,
            session.session_shortcut_starts,
            session.session_shortcut_pre_lookup_waits,
            session.session_shortcut_pre_lookup_hits,
            session.session_shortcut_pre_lookup_misses,
            session.session_shortcut_pre_lookup_timeouts,
            session.session_shortcut_pre_lookup_max_ms,
            format_trace_counts(&sorted_trace_counts(
                session.session_shortcut_pre_lookup_budgets.clone()
            )),
            session.session_shortcut_post_lookup_waits,
            session.session_shortcut_post_lookup_hits,
            session.session_shortcut_post_lookup_misses,
            session.session_shortcut_post_lookup_timeouts,
            session.session_shortcut_post_lookup_errors,
            session.session_shortcut_post_lookup_max_ms,
            format_trace_counts(&sorted_trace_counts(
                session.session_shortcut_post_lookup_budgets.clone()
            )),
            format_trace_counts(&sorted_trace_counts(
                session
                    .session_shortcut_post_lookup_timeout_budgets
                    .clone()
            )),
            format_trace_counts(&sorted_trace_counts(
                session
                    .session_shortcut_post_lookup_http_provider_counts
                    .clone()
            )),
            session.session_shortcut_attempts,
            session.session_shortcut_hits,
            session.session_shortcut_misses,
            session.session_late_peer_waits,
            session.session_late_peer_hits,
            session.session_late_peer_misses,
            session.session_late_peer_max_ms
        );
        if session.session_shortcut_pre_lookup_waits > 0 {
            println!(
                "    pre-lookup latency: elapsed={} hits={} misses={} timeouts={}",
                session.session_shortcut_pre_lookup_elapsed_ms,
                session.session_shortcut_pre_lookup_hit_elapsed_ms,
                session.session_shortcut_pre_lookup_miss_elapsed_ms,
                session.session_shortcut_pre_lookup_timeout_elapsed_ms
            );
        }
        if session.session_shortcut_post_lookup_waits > 0 {
            println!(
                "    post-lookup latency: elapsed={} hits={} timeouts={} single_http={} single_http_hits={} single_http_timeouts={}",
                session.session_shortcut_post_lookup_elapsed_ms,
                session.session_shortcut_post_lookup_hit_elapsed_ms,
                session.session_shortcut_post_lookup_timeout_elapsed_ms,
                session.session_shortcut_post_lookup_single_http_elapsed_ms,
                session.session_shortcut_post_lookup_single_http_hit_elapsed_ms,
                session.session_shortcut_post_lookup_single_http_timeout_elapsed_ms
            );
        }
        if session.session_shortcut_post_lookup_races > 0 {
            println!(
                "    post-lookup race: events={} provider_wins={} bitswap_wins={} errors={} provider_bitswap_wins={} single_http_provider_bitswap_wins={} elapsed={} provider_result_elapsed={} single_http_provider_bitswap_elapsed={} outcomes={} sources={} http_counts={}",
                session.session_shortcut_post_lookup_races,
                session.session_shortcut_post_lookup_race_provider_wins,
                session.session_shortcut_post_lookup_race_bitswap_wins,
                session.session_shortcut_post_lookup_race_errors,
                session.session_shortcut_post_lookup_race_provider_bitswap_wins,
                session.session_shortcut_post_lookup_race_single_http_provider_bitswap_wins,
                session.session_shortcut_post_lookup_race_elapsed_ms,
                session.session_shortcut_post_lookup_race_provider_result_elapsed_ms,
                session.session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_ms,
                format_trace_counts(&session.session_shortcut_post_lookup_race_outcomes),
                format_trace_counts(&session.session_shortcut_post_lookup_race_sources),
                format_trace_counts(&session.session_shortcut_post_lookup_race_http_provider_counts)
            );
        }
        if session.session_late_peer_waits > 0 {
            println!(
                "    late-peer latency: elapsed={} hits={} misses={}",
                session.session_late_peer_elapsed_ms,
                session.session_late_peer_hit_elapsed_ms,
                session.session_late_peer_miss_elapsed_ms
            );
        }
    }
    if trace.bitswap_extra_blocks.events > 0 {
        let extra = &trace.bitswap_extra_blocks;
        println!(
            "  bitswap extra blocks: events={} total={} max={} incoming={} outgoing={} unknown={}",
            extra.events, extra.total, extra.max, extra.incoming, extra.outgoing, extra.unknown
        );
    }
    if trace.bitswap_incoming_blocks.matches > 0 {
        let incoming = &trace.bitswap_incoming_blocks;
        println!(
            "  bitswap incoming blocks: matches={} blocks={} bytes={} delivered_waiters={} dropped_waiters={} max_oldest_pending_ms={} max_pending_waiters={} max_dropped_waiters={}",
            incoming.matches,
            incoming.blocks,
            incoming.bytes,
            incoming.delivered_waiters,
            incoming.dropped_waiters,
            incoming.max_oldest_pending_ms,
            incoming.max_pending_waiters,
            incoming.max_dropped_waiters
        );
    }
    print_trace_bitswap_incoming_reads(trace);
    print_trace_connection_established(trace);
    print_trace_connection_errors(trace);
    print_trace_connection_backoff(trace);
    print_trace_dial_rejections(trace);
    print_trace_slow_details(trace);
    if !trace.trace_errors.is_empty() {
        println!(
            "  trace errors: {}",
            format_trace_counts(&trace.trace_errors)
        );
    }
}

fn print_trace_progress_phases(trace: &TraceSummary) {
    if trace.progress_phases.is_empty() {
        return;
    }
    println!(
        "  progress phases: {}",
        format_trace_counts(&trace.progress_phases)
    );
}

fn print_trace_slow_details(trace: &TraceSummary) {
    if !trace.slow_cids.is_empty() {
        println!("  slow cids:");
        for cid in trace.slow_cids.iter().take(8) {
            let phases = format_trace_counts(&cid.phases);
            let paths = format_trace_counts(&cid.paths);
            let bitswap_source_candidate_indexes =
                format_trace_counts(&cid.bitswap_source_candidate_indexes);
            let bitswap_source_peers = format_trace_counts(&cid.bitswap_source_peers);
            println!(
                "    {}: count={} total={}ms max={}ms phases={} paths={}",
                cid.cid, cid.count, cid.total_ms, cid.max_ms, phases, paths
            );
            if !cid.bitswap_source_candidate_indexes.is_empty()
                || !cid.bitswap_source_peers.is_empty()
            {
                println!(
                    "      bitswap_source_candidate_indexes={bitswap_source_candidate_indexes} bitswap_source_peers={bitswap_source_peers}"
                );
            }
        }
    }
    if !trace.slow_requests.is_empty() {
        println!("  slow requests:");
        for request in trace.slow_requests.iter().take(8) {
            let phases = format_trace_counts(&request.phases);
            let cids = format_trace_counts(&request.cids);
            let status = request.status.as_deref().unwrap_or("unknown");
            let request_id = if request.process_id.is_empty() {
                request.request_id.clone()
            } else {
                format!("{}:{}", request.process_id, request.request_id)
            };
            let correlation = format_request_correlation(request);
            let classifications = format_request_classification_details(request);
            let source_details = format_request_source_details(request);
            let latency_details = format_request_phase_latency_details(request);
            println!(
                "    {}: {}ms status={} request_id={}{}{}{} events={} max_event={}ms{} phases={} cids={}",
                request.path,
                request.elapsed_ms,
                status,
                request_id,
                correlation,
                classifications,
                source_details,
                request.event_count,
                request.max_event_ms,
                latency_details,
                phases,
                cids
            );
        }
    }
    if !trace.slow_events.is_empty() {
        println!("  slow events:");
        for event in trace.slow_events.iter().take(8) {
            let details = event
                .details
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(" ");
            if details.is_empty() {
                println!("    {}: {}ms", event.phase, event.elapsed_ms);
            } else {
                println!("    {}: {}ms {}", event.phase, event.elapsed_ms, details);
            }
        }
    }
}

fn print_trace_request_classifications(trace: &TraceSummary) {
    if trace.request_classifications.is_empty() {
        return;
    }
    println!(
        "  request classifications: {}",
        format_trace_counts(&trace.request_classifications)
    );
    if !trace.request_classification_latencies.is_empty() {
        println!("  request classification latencies:");
        for aggregate in trace.request_classification_latencies.iter().take(8) {
            let statuses = format_trace_counts(&aggregate.statuses);
            let top_level_paths = format_trace_counts(&aggregate.top_level_paths);
            println!(
                "    {}: requests={} elapsed={} max_event={} statuses={} top_level_paths={}",
                aggregate.classification,
                aggregate.request_count,
                aggregate.request_elapsed_ms,
                aggregate.max_event_ms,
                statuses,
                top_level_paths
            );
            if !aggregate.bitswap_source_candidate_indexes.is_empty() {
                println!(
                    "      bitswap_source_candidate_indexes={}",
                    format_trace_counts(&aggregate.bitswap_source_candidate_indexes)
                );
            }
            if !aggregate.bitswap_source_request_modes.is_empty() {
                println!(
                    "      bitswap_source_request_modes={}",
                    format_trace_counts(&aggregate.bitswap_source_request_modes)
                );
            }
            if !aggregate.bitswap_source_peers.is_empty() {
                println!(
                    "      bitswap_source_peers={}",
                    format_trace_counts(&aggregate.bitswap_source_peers)
                );
            }
            if !aggregate.bitswap_source_transports.is_empty() {
                println!(
                    "      bitswap_source_transports={}",
                    format_trace_counts(&aggregate.bitswap_source_transports)
                );
            }
        }
    }
}

fn print_trace_unixfs_metadata_cache(trace: &TraceSummary) {
    if trace.unixfs_metadata_cache.events == 0 {
        return;
    }
    let cache = &trace.unixfs_metadata_cache;
    println!(
        "  unixfs metadata cache: events={} hits={} misses={} inserts={} evictions={} oversized_skips={} max_len={} path_hits={} path_misses={} path_inserts={} path_evictions={} path_oversized_skips={} max_path_len={} file_size_hits={} file_size_misses={} file_size_inserts={} file_size_evictions={} max_file_size_len={} capacity={}",
        cache.events,
        cache.hits,
        cache.misses,
        cache.inserts,
        cache.evictions,
        cache.oversized_skips,
        cache.max_len,
        cache.path_hits,
        cache.path_misses,
        cache.path_inserts,
        cache.path_evictions,
        cache.path_oversized_skips,
        cache.max_path_len,
        cache.file_size_hits,
        cache.file_size_misses,
        cache.file_size_inserts,
        cache.file_size_evictions,
        cache.max_file_size_len,
        cache.max_capacity
    );
}

fn print_trace_block_range_batch_fetches(trace: &TraceSummary) {
    let ranges = &trace.block_range_batch_fetches;
    if !ranges.has_events() {
        return;
    }
    println!(
        "  block range batch fetches: events={} bytes={} elapsed={} max_range_len={} max_range_count={} max_uncached_range_count={} sources={}",
        ranges.events,
        ranges.bytes,
        ranges.elapsed_ms,
        ranges.max_range_len,
        ranges.max_range_count,
        ranges.max_uncached_range_count,
        format_trace_counts(&ranges.sources)
    );
}

fn print_trace_block_fetch_source_latencies(trace: &TraceSummary) {
    if trace.block_fetch_source_latencies.is_empty() {
        return;
    }
    println!("  block fetch totals:");
    for source in trace.block_fetch_source_latencies.iter().take(8) {
        println!(
            "    {}: count={} total={}ms elapsed={}",
            source.source, source.count, source.total_ms, source.elapsed_ms
        );
    }
}

fn print_trace_gateway_limiter(trace: &TraceSummary) {
    let limiter = &trace.gateway_limiter;
    if limiter.events == 0 {
        return;
    }
    println!(
        "  gateway limiter: events={} acquired={} denied={} elapsed={} denied_elapsed={} max_timeout_ms={}",
        limiter.events,
        limiter.acquired,
        limiter.denied,
        limiter.elapsed_ms,
        limiter.denied_elapsed_ms,
        limiter.max_timeout_ms
    );
}

fn print_trace_delegated_provider_lookup(trace: &TraceSummary) {
    let delegated = &trace.delegated_provider_lookup;
    if delegated.events == 0 {
        return;
    }
    println!(
        "  delegated provider lookup: events={} successes={} failures={} providers={} http_providers={} self_hedges={} self_hedge_timeout_max={}ms response_bytes={} response_lines={} elapsed={} max_elapsed_ms={}",
        delegated.events,
        delegated.successes,
        delegated.failures,
        delegated.providers,
        delegated.http_providers,
        delegated.self_hedges,
        delegated.max_self_hedge_timeout_ms,
        delegated.response_bytes,
        delegated.response_lines,
        delegated.elapsed_ms,
        delegated.max_elapsed_ms
    );
    if delegated.has_response_milestones() {
        println!(
            "    response milestones: headers={} header_max={}ms first_chunk_seen={} first_chunks={} first_chunk_max={}ms first_http_seen={} first_http={} first_http_max={}ms target_met={} target_met_elapsed={} target_met_max={}ms",
            delegated.response_headers_elapsed_ms,
            delegated.max_response_headers_elapsed_ms,
            delegated.first_chunk_events,
            delegated.response_first_chunk_elapsed_ms,
            delegated.max_response_first_chunk_elapsed_ms,
            delegated.first_http_provider_events,
            delegated.response_first_http_provider_elapsed_ms,
            delegated.max_response_first_http_provider_elapsed_ms,
            delegated.target_met_events,
            delegated.response_target_met_elapsed_ms,
            delegated.max_response_target_met_elapsed_ms
        );
    }
    println!(
        "    http provider distribution: zero={} single={} multi={} single_target_miss={} single_elapsed={} single_max={}ms single_first_http={} single_first_http_max={}ms",
        delegated.zero_http_provider_events,
        delegated.single_http_provider_events,
        delegated.multi_http_provider_events,
        delegated.single_http_provider_target_miss_events,
        delegated.single_http_provider_elapsed_ms,
        delegated.max_single_http_provider_elapsed_ms,
        delegated.single_http_provider_first_http_elapsed_ms,
        delegated.max_single_http_provider_first_http_elapsed_ms
    );
    for endpoint in trace.delegated_provider_lookup_by_endpoint.iter().take(4) {
        println!(
            "    {}: events={} successes={} failures={} providers={} http_providers={} http_zero={} http_single={} http_multi={} http_single_target_miss={} self_hedges={} self_hedge_timeout_max={}ms elapsed={} max_elapsed_ms={} headers={} header_max={}ms first_http={} first_http_max={}ms target_met={}",
            endpoint.endpoint,
            endpoint.events,
            endpoint.successes,
            endpoint.failures,
            endpoint.providers,
            endpoint.http_providers,
            endpoint.zero_http_provider_events,
            endpoint.single_http_provider_events,
            endpoint.multi_http_provider_events,
            endpoint.single_http_provider_target_miss_events,
            endpoint.self_hedges,
            endpoint.max_self_hedge_timeout_ms,
            endpoint.elapsed_ms,
            endpoint.max_elapsed_ms,
            endpoint.response_headers_elapsed_ms,
            endpoint.max_response_headers_elapsed_ms,
            endpoint.response_first_http_provider_elapsed_ms,
            endpoint.max_response_first_http_provider_elapsed_ms,
            endpoint.target_met_events
        );
    }
}

fn print_trace_dht_provider_lookup(trace: &TraceSummary) {
    let dht = &trace.dht_provider_lookup;
    if dht.events == 0 {
        return;
    }
    println!(
        "  dht provider lookup: events={} successes={} failures={} providers={} max_providers={} max_timeout_ms={} max_query_timeout_ms={} max_elapsed_ms={}",
        dht.events,
        dht.successes,
        dht.failures,
        dht.providers,
        dht.max_providers,
        dht.max_timeout_ms,
        dht.max_query_timeout_ms,
        dht.max_elapsed_ms
    );
}

fn print_trace_provider_diversity_low(trace: &TraceSummary) {
    let diversity = &trace.provider_diversity_low;
    if diversity.events == 0 {
        return;
    }
    println!(
        "  provider diversity low: events={} failures={} providers_total={} bitswap_providers_total={} dht_providers_total={} max_provider_count={} max_bitswap_provider_count={} max_dht_provider_count={} max_timeout_ms={} fallbacks={}",
        diversity.events,
        diversity.failures,
        diversity.provider_count_total,
        diversity.bitswap_provider_count_total,
        diversity.dht_provider_count_total,
        diversity.max_provider_count,
        diversity.max_bitswap_provider_count,
        diversity.max_dht_provider_count,
        diversity.max_timeout_ms,
        format_trace_counts(&diversity.fallbacks)
    );
}

fn print_trace_gateway_direct_body(trace: &TraceSummary) {
    let direct = &trace.gateway_direct_body;
    if direct.events == 0 {
        return;
    }
    println!(
        "  gateway direct bodies: events={} bytes={} max_body_len={} max_elapsed_ms={}",
        direct.events, direct.bytes, direct.max_body_len, direct.max_elapsed_ms
    );
}

fn print_trace_gateway_small_body_cache(trace: &TraceSummary) {
    let cache = &trace.gateway_small_body_cache;
    if cache.events == 0 {
        return;
    }
    println!(
        "  gateway small body cache: events={} hits={} misses={} inserts={} evictions={} bytes_served={} max_body_len={} max_cache_len={} max_cache_bytes={}",
        cache.events,
        cache.hits,
        cache.misses,
        cache.inserts,
        cache.evictions,
        cache.bytes_served,
        cache.max_body_len,
        cache.max_cache_len,
        cache.max_cache_bytes
    );
}

fn print_trace_gateway_stream_body(trace: &TraceSummary) {
    let stream = &trace.gateway_stream_body;
    if stream.events == 0 {
        return;
    }
    println!(
        "  gateway streamed bodies: events={} bytes={} max_body_len={} max_chunks={} max_elapsed_ms={}",
        stream.events, stream.bytes, stream.max_body_len, stream.max_chunks, stream.max_elapsed_ms
    );
}

fn print_trace_http_provider_fetches(trace: &TraceSummary) {
    let http = &trace.http_provider_fetches;
    if http.events == 0 {
        return;
    }
    println!(
        "  http provider fetches: events={} successes={} failures={} bytes={} response_bytes={} elapsed={} providers={} error_classes={}",
        http.events,
        http.successes,
        http.failures,
        http.bytes,
        http.response_bytes,
        http.elapsed_ms,
        format_trace_counts(&http.providers),
        format_trace_counts(&http.error_classes)
    );
    if http.has_response_milestones() {
        println!(
            "    response milestones: header_max={}ms first_chunk_seen={} first_chunk_max={}ms body_max={}ms",
            http.max_response_headers_elapsed_ms,
            http.first_chunk_events,
            http.max_response_first_chunk_elapsed_ms,
            http.max_response_body_elapsed_ms
        );
    }
    for provider in http.provider_milestones.iter().take(4) {
        println!(
            "    provider {}: events={} successes={} failures={} bytes={} elapsed={} headers={} first_chunks={} bodies={} elapsed_max={}ms header_max={}ms first_chunk_max={}ms body_max={}ms",
            provider.provider,
            provider.events,
            provider.successes,
            provider.failures,
            provider.bytes,
            provider.elapsed_ms,
            provider.response_headers_elapsed_ms,
            provider.response_first_chunk_elapsed_ms,
            provider.response_body_elapsed_ms,
            provider.max_ms,
            provider.max_response_headers_elapsed_ms,
            provider.max_response_first_chunk_elapsed_ms,
            provider.max_response_body_elapsed_ms
        );
    }
}

fn print_trace_http_provider_races(trace: &TraceSummary) {
    let race = &trace.http_provider_races;
    if !race.has_events() {
        return;
    }
    println!(
        "  http provider races: events={} providers={} single={} multi={} above_width={} race_width_max={} max_provider_count={} scored_events={} scored_providers={} max_scored={} hedges={} self_hedges={} self_hedge_timeout_max={}ms hedge_pending_max={} hedge_remaining_max={} results={} result_ok={} result_fail={} winner_initial={} winner_late={} winner_rank1={} winner_rank2={} winner_rank3_plus={} winner_rank_max={} attempted_max={} result_elapsed_max={}ms",
        race.events,
        race.provider_count_total,
        race.single_provider_events,
        race.multi_provider_events,
        race.above_race_width_events,
        race.max_race_width,
        race.max_provider_count,
        race.scored_events,
        race.scored_provider_count_total,
        race.max_scored_provider_count,
        race.hedges,
        race.self_hedges,
        race.max_self_hedge_timeout_ms,
        race.max_hedge_pending_count,
        race.max_hedge_remaining_provider_count,
        race.result_events,
        race.result_successes,
        race.result_failures,
        race.winner_initial_width_events,
        race.winner_late_events,
        race.winner_rank1_events,
        race.winner_rank2_events,
        race.winner_rank3_plus_events,
        race.max_winner_provider_rank,
        race.max_attempted_provider_count,
        race.max_result_elapsed_ms
    );
    if race.single_provider_result_successes > 0 || race.single_provider_result_failures > 0 {
        println!(
            "    single-provider results: ok={} fail={} winner_elapsed={}",
            race.single_provider_result_successes,
            race.single_provider_result_failures,
            race.single_provider_success_elapsed_ms
        );
        for provider in race.single_provider_winners.iter().take(4) {
            println!(
                "    single-provider winner {}: events={} elapsed_max={}ms total={}ms",
                provider.provider, provider.events, provider.max_ms, provider.total_ms
            );
        }
    }
    if race.winner_scored_events > 0 || race.max_winner_original_provider_rank > 0 {
        println!(
            "    provider scoring: winner_scored={} winner_score={} original_rank1={} original_rank2={} original_rank3_plus={} original_rank_max={}",
            race.winner_scored_events,
            race.winner_score_elapsed_ms,
            race.winner_original_rank1_events,
            race.winner_original_rank2_events,
            race.winner_original_rank3_plus_events,
            race.max_winner_original_provider_rank
        );
    }
    if race.multi_provider_success_elapsed_ms.count > 0 {
        println!(
            "    multi-provider winner elapsed={}",
            race.multi_provider_success_elapsed_ms
        );
    }
    if race.self_hedge_winner_initial_events > 0
        || race.self_hedge_winner_hedged_events > 0
        || race.self_hedge_winner_unknown_events > 0
        || race.self_hedge_skips > 0
        || race.candidate_cancellations > 0
    {
        println!(
            "    self-hedge winners: initial={} hedged={} unknown={} fired_results={} fired_initial={} fired_hedged={} fired_unknown={} winner_attempt_max={} skips={} skip_reasons={} cancelled={} cancelled_stages={} cancelled_attempts={} cancelled_providers={}",
            race.self_hedge_winner_initial_events,
            race.self_hedge_winner_hedged_events,
            race.self_hedge_winner_unknown_events,
            race.self_hedge_fired_result_events,
            race.self_hedge_fired_winner_initial_events,
            race.self_hedge_fired_winner_hedged_events,
            race.self_hedge_fired_winner_unknown_events,
            race.max_winner_attempt_index,
            race.self_hedge_skips,
            format_trace_counts(&race.self_hedge_skip_reasons),
            race.candidate_cancellations,
            format_trace_counts(&race.candidate_cancelled_stages),
            format_trace_counts(&race.candidate_cancelled_attempts),
            format_trace_counts(&race.candidate_cancelled_providers)
        );
    }
    if race.bitswap_hedges > 0 || race.bitswap_hedge_results > 0 || race.bitswap_hedge_skips > 0 {
        println!(
            "    bitswap hedge: starts={} timeout_max={}ms results={} result_elapsed={} skips={} result_sources={} skip_reasons={}",
            race.bitswap_hedges,
            race.max_bitswap_hedge_timeout_ms,
            race.bitswap_hedge_results,
            race.bitswap_hedge_result_elapsed_ms,
            race.bitswap_hedge_skips,
            format_trace_counts(&race.bitswap_hedge_result_sources),
            format_trace_counts(&race.bitswap_hedge_skip_reasons)
        );
    }
}

fn print_trace_provider_retries(trace: &TraceSummary) {
    if !trace.provider_retries.has_events() {
        return;
    }
    let retries = &trace.provider_retries;
    println!(
        "  provider retries: refresh_timeout={} refresh_failure={} skipped_empty={} retry_counts={} same_providers={} same_bitswap_peers={} request_timeout_counts={} same_bitswap_request_timeouts={} retry_request_timeout={} retry_timeout={} retry_connection_timeout={}",
        retries.refresh_after_timeout_events,
        retries.refresh_after_failure_events,
        retries.skipped_empty_provider_set_events,
        retries.retry_count_events,
        retries.same_provider_sets,
        retries.same_bitswap_peer_sets,
        retries.request_timeout_retry_counts,
        retries.same_bitswap_request_timeout_retry_counts,
        retries.request_timeout_retries,
        retries.timeout_retries,
        retries.connection_timeout_retries
    );
}

fn print_trace_timeout_recovery(trace: &TraceSummary) {
    if !trace.bitswap_timeout_recovery.has_events() {
        return;
    }
    let recovery = &trace.bitswap_timeout_recovery;
    println!(
        "  bitswap timeout recovery: request_timeout_details={} cold={} mixed_trusted={} trusted_only={} timeout_ms={} max_peers={} no_dial_plan={} mixed_no_dial_plan={} request_timeout_events={} reset_true={} reset_false={} client_resets={} retry_starts={} same_provider_retries={} refreshed_provider_retries={} retry_successes={} trusted_retry_successes={} untrusted_retry_successes={} retry_failures={} retry_unresolved={} retry_success_elapsed={}",
        recovery.request_timeouts,
        recovery.cold_request_timeouts,
        recovery.mixed_trusted_request_timeouts,
        recovery.trusted_only_request_timeouts,
        format_trace_counts(&recovery.request_timeout_budgets),
        recovery.max_request_timeout_peer_count,
        recovery.request_timeouts_without_dial_plan,
        recovery.mixed_trusted_request_timeouts_without_dial_plan,
        recovery.request_timeout_events,
        recovery.request_timeout_reset_true,
        recovery.request_timeout_reset_false,
        recovery.client_resets,
        recovery.provider_retry_starts,
        recovery.same_provider_retry_starts,
        recovery.refreshed_provider_retry_starts,
        recovery.retry_successes,
        recovery.trusted_retry_successes,
        recovery.untrusted_retry_successes,
        recovery.retry_failures,
        recovery.retry_unresolved,
        recovery.retry_success_elapsed_ms
    );
    if recovery.request_timeout_want_block_targets > 0
        || recovery.request_timeout_want_have_targets > 0
    {
        println!(
            "  bitswap timeout target modes: want_block={} want_have={} max_want_block={} max_want_have={}",
            recovery.request_timeout_want_block_targets,
            recovery.request_timeout_want_have_targets,
            recovery.max_request_timeout_want_block_targets,
            recovery.max_request_timeout_want_have_targets
        );
    }
}

fn print_trace_bitswap_peer_attempts(trace: &TraceSummary) {
    if let Some(line) = format_trace_bitswap_peer_attempts(&trace.bitswap_peer_attempts) {
        println!("  {line}");
    }
}

fn print_trace_bitswap_want_have_probes(trace: &TraceSummary) {
    if let Some(line) = format_trace_bitswap_want_have_probes(&trace.bitswap_want_have_probes) {
        println!("  {line}");
    }
}

fn format_trace_bitswap_peer_attempts(
    attempts: &TraceBitswapPeerAttemptAggregate,
) -> Option<String> {
    if !attempts.has_events() {
        return None;
    }
    Some(format!(
        "bitswap peer attempts: starts={} outgoing_completed={} successes={} failures={} connection_timeouts={} read_timeouts={} other_failures={} prefer_want_have={} cancelled={} cancelled_prefer_want_have={} cancelled_stages={} cancelled_candidate_indexes={} cancelled_modes={} cancelled_first_addr_transports={} cancelled_first_addr_families={}",
        attempts.starts,
        attempts.outgoing_completed,
        attempts.successes,
        attempts.failures,
        attempts.connection_timeouts,
        attempts.read_timeouts,
        attempts.other_failures,
        attempts.prefer_want_have,
        attempts.cancelled,
        attempts.cancelled_prefer_want_have,
        format_trace_counts(&attempts.cancelled_stages),
        format_trace_counts(&attempts.cancelled_candidate_indexes),
        format_trace_counts(&attempts.cancelled_request_modes),
        format_trace_counts(&attempts.cancelled_first_addr_transports),
        format_trace_counts(&attempts.cancelled_first_addr_families)
    ))
}

fn format_trace_bitswap_want_have_probes(
    probes: &TraceBitswapWantHaveProbeAggregate,
) -> Option<String> {
    if !probes.has_events() {
        return None;
    }
    Some(format!(
        "bitswap WANT_HAVE probes: events={} ok={} failures={} have={} dont_have={} block={} want_block_followups={} no_presence={} bytes={} extra_blocks={} max_timeout={}ms elapsed={} outcomes={} peers={} candidate_indexes={} request_modes={} first_addr_transports={} first_addr_families={} target_peer_counts={} peer_addr_counts={}",
        probes.events,
        probes.ok,
        probes.failures,
        probes.have,
        probes.dont_have,
        probes.block,
        probes.want_block_followups,
        probes.no_presence,
        probes.bytes,
        probes.extra_blocks,
        probes.max_timeout_ms,
        probes.elapsed_ms,
        format_trace_counts(&probes.outcomes),
        format_trace_counts(&probes.peers),
        format_trace_counts(&probes.candidate_indexes),
        format_trace_counts(&probes.request_modes),
        format_trace_counts(&probes.first_addr_transports),
        format_trace_counts(&probes.first_addr_families),
        format_trace_counts(&probes.target_peer_counts),
        format_trace_counts(&probes.peer_addr_counts)
    ))
}

fn print_trace_bitswap_sources(trace: &TraceSummary) {
    if !trace.bitswap_source_peers.is_empty() {
        println!(
            "  bitswap source peers: {}",
            format_trace_counts(&trace.bitswap_source_peers)
        );
    }
    if !trace.bitswap_source_transports.is_empty() {
        println!(
            "  bitswap source transports: {}",
            format_trace_counts(&trace.bitswap_source_transports)
        );
    }
    if !trace.bitswap_source_request_modes.is_empty() {
        println!(
            "  bitswap source request modes: {}",
            format_trace_counts(&trace.bitswap_source_request_modes)
        );
    }
    if !trace.bitswap_source_candidate_indexes.is_empty() {
        println!(
            "  bitswap source candidate indexes: {}",
            format_trace_counts(&trace.bitswap_source_candidate_indexes)
        );
    }
    if !trace.bitswap_source_addr_indexes.is_empty() {
        println!(
            "  bitswap source addr indexes: {}",
            format_trace_counts(&trace.bitswap_source_addr_indexes)
        );
    }
    if !trace.bitswap_source_addr_families.is_empty() {
        println!(
            "  bitswap source addr families: {}",
            format_trace_counts(&trace.bitswap_source_addr_families)
        );
    }
    if !trace.bitswap_source_addr_match_statuses.is_empty() {
        println!(
            "  bitswap source addr matches: {}",
            format_trace_counts(&trace.bitswap_source_addr_match_statuses)
        );
    }
    if !trace.bitswap_deliveries.is_empty() {
        println!(
            "  bitswap deliveries: {}",
            format_trace_counts(&trace.bitswap_deliveries)
        );
    }
}

fn print_trace_bitswap_batches(trace: &TraceSummary) {
    let batches = &trace.bitswap_batches;
    if !batches.has_events() {
        return;
    }
    println!(
        "  bitswap batches: commands={} multi_cid_commands={} total_cids={} max_cids={} peer_attempt_starts={} peer_attempt_successes={} requested_blocks={} max_requested_blocks={} cancelled={} failures={}",
        batches.commands,
        batches.multi_cid_commands,
        batches.total_cids,
        batches.max_cids,
        batches.peer_attempt_starts,
        batches.peer_attempt_successes,
        batches.requested_blocks,
        batches.max_requested_blocks,
        batches.cancelled,
        batches.failures
    );
}

fn print_trace_bitswap_incoming_batches(trace: &TraceSummary) {
    let batches = &trace.bitswap_incoming_batches;
    if batches.events == 0 {
        return;
    }
    println!(
        "  bitswap incoming batches: events={} total_cids={} max_cids={} requested_blocks={} extra_blocks={} max_elapsed_ms={}",
        batches.events,
        batches.total_cids,
        batches.max_cids,
        batches.requested_blocks,
        batches.extra_blocks,
        batches.max_elapsed_ms
    );
}

fn print_trace_bitswap_peer_fetches(trace: &TraceSummary) {
    if trace.bitswap_peer_fetches.is_empty() {
        return;
    }
    println!("  bitswap peer fetches:");
    for peer in trace.bitswap_peer_fetches.iter().take(8) {
        let transports = format_trace_counts(&peer.transports);
        println!(
            "    {}: count={} total={}ms max={}ms bytes={} transports={}",
            peer.peer, peer.count, peer.total_ms, peer.max_ms, peer.bytes, transports
        );
    }
}

fn print_trace_bitswap_dial_plans(trace: &TraceSummary) {
    let plans = &trace.bitswap_dial_plans;
    if plans.events == 0 {
        return;
    }
    println!(
        "  bitswap dial plans: events={} peer_targets={} candidates={} new_peers={} new_addrs={} suppressed_peers={} suppressed_addrs={} pending_peers={} connected_peers={} max_queued_ms={}",
        plans.events,
        plans.peer_targets,
        plans.candidate_peers,
        plans.new_dial_peers,
        plans.new_dial_addrs,
        plans.suppressed_dial_peers,
        plans.suppressed_dial_addrs,
        plans.pending_dial_peers,
        plans.connected_peers,
        plans.max_command_queued_ms
    );
}

fn print_trace_bitswap_provider_expansion(trace: &TraceSummary) {
    let Some(peer_expand) = trace
        .phases
        .iter()
        .find(|phase| phase.phase == "bitswap_peer_expand")
    else {
        return;
    };
    let quality = &trace.bitswap_provider_quality;
    let dns = &trace.bitswap_dns_expansion;
    println!(
        "  bitswap provider expansion: events={} elapsed={} provider_addrs={} expanded={} supported={} rejected={} dns_events={} dns_cached={} dns_uncached={} dns_failed={} dns_records={} dns_ips={}",
        peer_expand.count,
        peer_expand.elapsed_ms,
        quality.provider_addr_count,
        quality.expanded_provider_addr_count,
        quality.supported_provider_addr_count,
        quality.rejected_provider_addr_count,
        dns.events,
        dns.cached,
        dns.uncached,
        dns.failed,
        dns.records,
        dns.ips
    );
}

fn print_trace_bitswap_incoming_reads(trace: &TraceSummary) {
    let reads = &trace.bitswap_incoming_reads;
    if reads.events == 0 {
        return;
    }
    println!(
        "  bitswap incoming stream reads: events={} failures={} dropped={} timed_out={} max_pending_reads={} max_elapsed_ms={}",
        reads.events,
        reads.failures,
        reads.dropped,
        reads.timed_out,
        reads.max_pending_reads,
        reads.max_elapsed_ms
    );
}

fn print_trace_dial_rejections(trace: &TraceSummary) {
    if trace.bitswap_dial_rejections.events == 0
        && trace.bitswap_dial_rejected_transports.is_empty()
    {
        return;
    }
    let rejected = &trace.bitswap_dial_rejections;
    println!(
        "  bitswap dial rejections: events={} connection_limit={} other={} transports={}",
        rejected.events,
        rejected.connection_limit,
        rejected.other,
        format_trace_counts(&trace.bitswap_dial_rejected_transports)
    );
}

fn print_trace_connection_established(trace: &TraceSummary) {
    let established = &trace.bitswap_connection_established;
    if established.events == 0 && trace.bitswap_connection_transports.is_empty() {
        return;
    }
    println!(
        "  bitswap connections: established={} established_ms={} wait_elapsed_ms={} failed_dials={} transports={}",
        established.events,
        established.established_ms,
        established.wait_elapsed_ms,
        established.failed_dial_count,
        format_trace_counts(&trace.bitswap_connection_transports)
    );
}

fn print_trace_connection_errors(trace: &TraceSummary) {
    let errors = &trace.bitswap_connection_errors;
    if errors.events == 0 {
        return;
    }
    println!(
        "  bitswap connection errors: events={} with_peer={} without_peer={} classes={} addr_families={} peers={}",
        errors.events,
        errors.with_peer,
        errors.without_peer,
        format_trace_counts(&errors.classes),
        format_trace_counts(&trace.bitswap_connection_error_addr_families),
        format_trace_counts(&errors.peers)
    );
}

fn print_trace_connection_backoff(trace: &TraceSummary) {
    let backoff = &trace.bitswap_connection_backoff;
    if backoff.backoffs == 0 && backoff.skipped == 0 {
        return;
    }
    println!(
        "  bitswap connection backoff: backoffs={} skipped={} classes={} peers={} skipped_peers={}",
        backoff.backoffs,
        backoff.skipped,
        format_trace_counts(&backoff.classes),
        format_trace_counts(&backoff.peers),
        format_trace_counts(&backoff.skipped_peers)
    );
}

fn format_stream_metrics(stream: FetchStreamMetrics) -> String {
    format!(
        "chunks={} first_byte={} max_chunk={} max_buffered={} completed={} cancelled={}",
        stream.chunk_count,
        display_option_ms(stream.first_byte_ms),
        stream.max_chunk_bytes,
        stream.max_buffered_bytes,
        stream.completed,
        stream.cancelled
    )
}

fn display_option_ms(value: Option<u128>) -> String {
    value
        .map(|value| format!("{value}ms"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn display_option_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}x"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn display_option_u64_unit(value: Option<u64>, unit: &str) -> String {
    value
        .map(|value| format!("{value}{unit}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn display_option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn display_option_seed_setup(value: Option<BitswapSeedConnectionSetup>) -> &'static str {
    value
        .map(BitswapSeedConnectionSetup::as_str)
        .unwrap_or("n/a")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn print_case_result(result: &CaseResult) {
    let mark = if result.passed { "PASS" } else { "FAIL" };
    println!(
        "{mark} {:32} status={} type={} content_length={} accept_ranges={} bytes={} ttfb={}ms total={}ms stream=[{}] etag={} cache_control={}",
        result.id,
        result
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.content_type.as_deref().unwrap_or("-"),
        result
            .content_length
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.accept_ranges.as_deref().unwrap_or("-"),
        result.body_bytes,
        result.ttfb_ms,
        result.total_ms,
        format_stream_metrics(result.stream),
        result.etag.as_deref().unwrap_or("-"),
        result.cache_control.as_deref().unwrap_or("-")
    );
    if let Some(revalidation) = &result.revalidation {
        print_revalidation_result("  revalidation", revalidation);
    }
    for failure in &result.failures {
        println!("  - {failure}");
    }
    if let Some(summary) = &result.asset_summary {
        println!(
            "  assets: discovered={} fetched={} passed={} failed={} skipped_external={} skipped_unsupported={} truncated={}",
            summary.discovered,
            summary.fetched,
            summary.passed,
            summary.failed,
            summary.skipped_external,
            summary.skipped_unsupported,
            summary.truncated
        );
        for asset in result.assets.iter().filter(|asset| !asset.passed).take(8) {
            println!(
                "    - {} {} status={} type={} bytes={} total={}ms stream=[{}]",
                asset.kind,
                asset.url,
                asset
                    .status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                asset.content_type.as_deref().unwrap_or("-"),
                asset.body_bytes,
                asset.total_ms,
                format_stream_metrics(asset.stream)
            );
            for failure in &asset.failures {
                println!("      - {failure}");
            }
            if let Some(revalidation) = &asset.revalidation {
                print_revalidation_result("      revalidation", revalidation);
            }
        }
    }
}

fn print_revalidation_result(prefix: &str, result: &RevalidationResult) {
    let mark = if result.passed { "PASS" } else { "FAIL" };
    println!(
        "{prefix}: {mark} status={} bytes={} ttfb={}ms total={}ms stream=[{}] etag={} cache_control={}",
        result
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.body_bytes,
        result.ttfb_ms,
        result.total_ms,
        format_stream_metrics(result.stream),
        result.etag.as_deref().unwrap_or("-"),
        result.cache_control.as_deref().unwrap_or("-")
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestCorrelation {
    request_id: u64,
    parent_id: Option<u64>,
    top_level_path: String,
}

impl RequestCorrelation {
    fn root(top_level_path: String) -> Self {
        Self {
            request_id: next_harness_request_id(),
            parent_id: None,
            top_level_path,
        }
    }

    fn child(&self) -> Self {
        Self {
            request_id: next_harness_request_id(),
            parent_id: Some(self.request_id),
            top_level_path: self.top_level_path.clone(),
        }
    }
}

fn next_harness_request_id() -> u64 {
    NEXT_HARNESS_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone)]
enum GatewayClient {
    Http(reqwest::Client),
    Native(NativeGateway),
    NativeFfi(NativeFfiGateway),
}

impl GatewayClient {
    fn http(timeout: Duration) -> Result<Self> {
        Ok(Self::Http(
            reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .context("build reqwest client")?,
        ))
    }

    async fn fetch_response(
        &self,
        url: &str,
        method: &str,
        range: Option<&str>,
        if_none_match: Option<&str>,
        correlation: Option<&RequestCorrelation>,
    ) -> std::result::Result<FetchResponse, String> {
        match self {
            Self::Http(client) => {
                fetch_http_response(client, url, method, range, if_none_match, correlation).await
            }
            Self::Native(native) => {
                native
                    .fetch_response(url, method, range, if_none_match, correlation)
                    .await
            }
            Self::NativeFfi(native) => {
                native
                    .fetch_response(url, method, range, if_none_match, correlation)
                    .await
            }
        }
    }
}

#[derive(Clone)]
struct NativeGateway {
    core: GatewayCore,
    storage_path: Option<PathBuf>,
    remove_storage_on_stop: bool,
}

impl NativeGateway {
    async fn start(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<Self> {
        let store = if let Some(path) = &args.gateway_db {
            SqliteBlockStore::open(path, 256 * 1024 * 1024)
                .with_context(|| format!("open native gateway DB {}", path.display()))?
        } else {
            SqliteBlockStore::in_memory(256 * 1024 * 1024)?
        };

        if let Some(import_car) = &args.gateway_import_car {
            let bytes = std::fs::read(import_car)
                .with_context(|| format!("read {}", import_car.display()))?;
            let imported = store.import_car(&bytes)?;
            eprintln!("native gateway imported {} CAR blocks", imported.len());
        }

        let config = native_gateway_config(args);
        let routing_mode = if bitswap_seed.is_some() {
            NativeRoutingMode::Delegated
        } else {
            parse_native_routing_mode(&args.routing_mode)?
        };
        let core = if routing_mode == NativeRoutingMode::Offline {
            GatewayCore::with_provider_and_name_resolver_config(
                Arc::new(store.clone()),
                Arc::new(PersistentNameResolver::cache_only(store)),
                config,
            )
        } else {
            let delegated_router_list = if let Some(seed) = bitswap_seed {
                seed.router_endpoint.clone()
            } else {
                args.delegated_router
                    .clone()
                    .unwrap_or_else(|| DEFAULT_DELEGATED_ROUTER.to_string())
            };
            let delegated_router_endpoints = delegated_router_endpoints(&delegated_router_list);
            let delegated =
                DelegatedRoutingClient::with_endpoints(delegated_router_endpoints.clone());
            let dht = LightDhtClient::default()
                .with_query_timeout(Duration::from_secs(args.dht_query_timeout_secs))
                .with_max_providers(args.dht_max_providers);
            let routing = match routing_mode {
                NativeRoutingMode::Auto => {
                    ProviderRoutingClient::from(AutoRoutingClient::new(delegated, dht.clone()))
                }
                NativeRoutingMode::Delegated => ProviderRoutingClient::from(delegated),
                NativeRoutingMode::LightDht => ProviderRoutingClient::from(dht.clone()),
                NativeRoutingMode::Offline => ProviderRoutingClient::Offline,
            };
            let provider = FetchingBlockProvider::new(store.clone(), routing);
            let name_resolver = CachedNameResolver::new(PersistentNameResolver::new(
                DefaultNameResolver::new(
                    CloudflareDohResolver::default(),
                    native_ipns_resolver(routing_mode, delegated_router_endpoints, dht),
                ),
                store,
            ));
            GatewayCore::with_provider_and_name_resolver_config(
                Arc::new(provider),
                Arc::new(name_resolver),
                config,
            )
        };

        Ok(Self {
            core,
            storage_path: args.gateway_db.clone(),
            remove_storage_on_stop: false,
        })
    }

    async fn fetch_response(
        &self,
        url: &str,
        method: &str,
        range: Option<&str>,
        if_none_match: Option<&str>,
        correlation: Option<&RequestCorrelation>,
    ) -> std::result::Result<FetchResponse, String> {
        let started = Instant::now();
        let method = gateway_method(method)?;
        let (namespace, path) = native_gateway_url_path(url)?;
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            headers.insert(
                RANGE,
                HeaderValue::from_str(range).map_err(|err| err.to_string())?,
            );
        }
        if let Some(if_none_match) = if_none_match {
            headers.insert(
                IF_NONE_MATCH,
                HeaderValue::from_str(if_none_match).map_err(|err| err.to_string())?,
            );
        }
        apply_correlation_header_map(&mut headers, correlation)?;
        let request = match namespace {
            NativeGatewayNamespace::Ipfs => GatewayCoreRequest::ipfs(path, method, headers),
            NativeGatewayNamespace::Ipns => GatewayCoreRequest::ipns(path, method, headers),
        };
        let response = self.core.handle(request).await;
        let ttfb_ms = started.elapsed().as_millis();
        let status = response.status();
        let headers = response.headers().clone();
        let (body, stream) =
            collect_body_stream(response.into_body().into_data_stream(), started, None).await?;
        Ok(fetch_response_from_parts(
            status,
            &headers,
            body,
            ttfb_ms,
            started.elapsed().as_millis(),
            stream,
        ))
    }

    #[cfg(test)]
    async fn fetch_response_and_drop_after_chunks(
        &self,
        url: &str,
        method: &str,
        range: Option<&str>,
        max_chunks: usize,
    ) -> std::result::Result<FetchResponse, String> {
        let started = Instant::now();
        let method = gateway_method(method)?;
        let (namespace, path) = native_gateway_url_path(url)?;
        let mut headers = HeaderMap::new();
        if let Some(range) = range {
            headers.insert(
                RANGE,
                HeaderValue::from_str(range).map_err(|err| err.to_string())?,
            );
        }
        let request = match namespace {
            NativeGatewayNamespace::Ipfs => GatewayCoreRequest::ipfs(path, method, headers),
            NativeGatewayNamespace::Ipns => GatewayCoreRequest::ipns(path, method, headers),
        };
        let response = self.core.handle(request).await;
        let ttfb_ms = started.elapsed().as_millis();
        let status = response.status();
        let headers = response.headers().clone();
        let (body, stream) = collect_body_stream(
            response.into_body().into_data_stream(),
            started,
            Some(max_chunks),
        )
        .await?;
        Ok(fetch_response_from_parts(
            status,
            &headers,
            body,
            ttfb_ms,
            started.elapsed().as_millis(),
            stream,
        ))
    }

    async fn stop(&mut self) {
        if self.remove_storage_on_stop {
            if let Some(path) = &self.storage_path {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn storage_bytes(&self) -> Option<u64> {
        self.storage_path
            .as_ref()
            .and_then(|path| storage_path_size_bytes(path).ok())
    }

    fn storage_path(&self) -> Option<String> {
        self.storage_path
            .as_ref()
            .map(|path| path.display().to_string())
    }
}

#[derive(Clone)]
struct NativeFfiGateway {
    transport: Arc<NativeFfiTransport>,
    start_limit: Option<Arc<Semaphore>>,
}

impl NativeFfiGateway {
    async fn start(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<Self> {
        let storage_path = args
            .gateway_db
            .clone()
            .unwrap_or_else(|| unique_temp_path("freedom-ipfs-native-ffi"));
        std::fs::create_dir_all(&storage_path)
            .with_context(|| format!("create native FFI data dir {}", storage_path.display()))?;
        let storage_arg = CString::new(storage_path.display().to_string())
            .context("native FFI data dir contains NUL byte")?;
        let node = unsafe { freedom_ipfs_node_new_with_data_dir(storage_arg.as_ptr(), 0) };
        if node.is_null() {
            bail!("create native FFI node");
        }

        if let Some(import_car) = &args.gateway_import_car {
            let bytes = std::fs::read(import_car)
                .with_context(|| format!("read {}", import_car.display()))?;
            let imported =
                unsafe { freedom_ipfs_node_import_car(node, bytes.as_ptr(), bytes.len()) };
            if !imported {
                unsafe {
                    freedom_ipfs_node_free(node);
                }
                bail!("native FFI node failed to import {}", import_car.display());
            }
            eprintln!(
                "native FFI gateway imported CAR from {}",
                import_car.display()
            );
        }

        let routing_mode = native_ffi_routing_mode(args, bitswap_seed)?;
        let delegated_router = bitswap_seed
            .map(|seed| seed.router_endpoint.clone())
            .or_else(|| args.delegated_router.clone());
        let delegated_router_c = delegated_router
            .map(CString::new)
            .transpose()
            .context("delegated router contains NUL byte")?;
        let gateway_addr = CString::new("127.0.0.1:0").expect("static address has no NUL");
        let node_addr = node as usize;
        let max_concurrent_requests = args.max_concurrent_requests;
        let dht_query_timeout_secs = args.dht_query_timeout_secs;
        let dht_max_providers = args.dht_max_providers;
        let started = tokio::task::spawn_blocking(move || unsafe {
            freedom_ipfs_node_start_gateway_online_with_config_v2(
                node_addr as *mut FreedomIpfsNode,
                gateway_addr.as_ptr(),
                delegated_router_c
                    .as_ref()
                    .map(|value| value.as_ptr())
                    .unwrap_or(std::ptr::null()),
                routing_mode,
                max_concurrent_requests,
                dht_query_timeout_secs,
                dht_max_providers,
            )
        })
        .await
        .map_err(|err| anyhow!("join native FFI gateway start: {err}"))?;
        if !started {
            unsafe {
                freedom_ipfs_node_free(node);
            }
            bail!("start native FFI gateway core");
        }

        let config = NativeFfiConfig {
            dispatcher_count: args.native_dispatchers.max(1),
            read_buffer_bytes: args.native_read_buffer_bytes.max(1),
            slow_consumer_ms: args.native_slow_consumer_ms,
            cancel_after_first_byte: args.native_cancel_after_first_byte,
            cancel_after_ms: args.native_cancel_after_ms,
            stop_node_mid_run_ms: args.native_stop_node_mid_run_ms,
            max_active_request_limit: args.native_max_active_requests,
        };
        let transport =
            NativeFfiTransport::start(node, storage_path, args.gateway_db.is_none(), config);
        Ok(Self {
            transport,
            start_limit: args
                .native_max_active_requests
                .map(|limit| Arc::new(Semaphore::new(limit.max(1)))),
        })
    }

    async fn fetch_response(
        &self,
        url: &str,
        method: &str,
        range: Option<&str>,
        if_none_match: Option<&str>,
        correlation: Option<&RequestCorrelation>,
    ) -> std::result::Result<FetchResponse, String> {
        let _permit = if let Some(limit) = &self.start_limit {
            Some(
                limit
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|err| err.to_string())?,
            )
        } else {
            None
        };
        self.transport
            .fetch_response(url, method, range, if_none_match, correlation)
            .await
    }

    fn report(&self) -> NativeFfiTransportReport {
        self.transport.report()
    }

    fn stop_node(&self) {
        self.transport.stop_node();
    }

    async fn stop(&mut self) {
        self.transport.shutdown();
    }

    fn storage_bytes(&self) -> Option<u64> {
        storage_path_size_bytes(&self.transport.storage_path).ok()
    }

    fn storage_path(&self) -> Option<String> {
        Some(self.transport.storage_path.display().to_string())
    }
}

struct NativeFfiNode {
    ptr: AtomicU64,
}

unsafe impl Send for NativeFfiNode {}
unsafe impl Sync for NativeFfiNode {}

impl NativeFfiNode {
    fn new(ptr: *mut FreedomIpfsNode) -> Self {
        Self {
            ptr: AtomicU64::new(ptr as usize as u64),
        }
    }

    fn ptr(&self) -> *mut FreedomIpfsNode {
        self.ptr.load(Ordering::Acquire) as usize as *mut FreedomIpfsNode
    }

    fn stop_gateway(&self) {
        let ptr = self.ptr();
        if !ptr.is_null() {
            unsafe {
                let _ = freedom_ipfs_node_stop_gateway(ptr);
            }
        }
    }

    fn free_on_thread(&self) {
        let ptr = self.ptr.swap(0, Ordering::AcqRel) as usize as *mut FreedomIpfsNode;
        if ptr.is_null() {
            return;
        }
        let ptr_addr = ptr as usize;
        let _ = std::thread::spawn(move || unsafe {
            let ptr = ptr_addr as *mut FreedomIpfsNode;
            let _ = freedom_ipfs_node_stop_gateway(ptr);
            freedom_ipfs_node_free(ptr);
        })
        .join();
    }

    fn native_gateway_stats(&self) -> Option<NativeFfiMobileStats> {
        let ptr = self.ptr();
        if ptr.is_null() {
            return None;
        }
        let stats_ptr = unsafe { freedom_ipfs_node_native_gateway_stats_json(ptr) };
        if stats_ptr.is_null() {
            return None;
        }
        let json = unsafe { CStr::from_ptr(stats_ptr).to_string_lossy().into_owned() };
        unsafe {
            freedom_ipfs_string_free(stats_ptr);
        }
        serde_json::from_str(&json).ok()
    }
}

impl Drop for NativeFfiNode {
    fn drop(&mut self) {
        self.free_on_thread();
    }
}

struct NativeFfiTransport {
    node: Arc<NativeFfiNode>,
    storage_path: PathBuf,
    remove_storage_on_stop: bool,
    config: NativeFfiConfig,
    state: Mutex<NativeFfiTransportState>,
    stats: Mutex<NativeFfiStats>,
    shutdown: AtomicBool,
    dispatchers: Mutex<Vec<ThreadJoinHandle<()>>>,
}

#[derive(Clone, Copy)]
struct NativeFfiConfig {
    dispatcher_count: usize,
    read_buffer_bytes: usize,
    slow_consumer_ms: u64,
    cancel_after_first_byte: bool,
    cancel_after_ms: Option<u64>,
    stop_node_mid_run_ms: Option<u64>,
    max_active_request_limit: Option<usize>,
}

#[derive(Default)]
struct NativeFfiTransportState {
    active: HashMap<u64, NativeFfiActiveRequest>,
    stashed_events: HashMap<u64, u32>,
    completed_handles: HashSet<u64>,
    completed_order: VecDeque<u64>,
}

struct NativeFfiActiveRequest {
    started: Instant,
    status: Option<StatusCode>,
    headers: HeaderMap,
    body: Vec<u8>,
    stream: FetchStreamMetrics,
    sender: Option<oneshot::Sender<std::result::Result<FetchResponse, String>>>,
    servicing: bool,
    deferred_events: u32,
    cancelled_by_policy: bool,
}

impl NativeFfiTransportState {
    fn remove_active(&mut self, handle: u64) -> Option<NativeFfiActiveRequest> {
        let active = self.active.remove(&handle);
        if active.is_some() {
            self.mark_completed(handle);
        }
        active
    }

    fn take_active(&mut self) -> HashMap<u64, NativeFfiActiveRequest> {
        let active = std::mem::take(&mut self.active);
        for handle in active.keys().copied() {
            self.mark_completed(handle);
        }
        active
    }

    fn mark_completed(&mut self, handle: u64) {
        self.stashed_events.remove(&handle);
        if self.completed_handles.insert(handle) {
            self.completed_order.push_back(handle);
        }
        while self.completed_order.len() > NATIVE_FFI_COMPLETED_HANDLE_TOMBSTONE_LIMIT {
            if let Some(oldest) = self.completed_order.pop_front() {
                self.completed_handles.remove(&oldest);
            }
        }
    }
}

#[derive(Default)]
struct NativeFfiStats {
    requests_started: u64,
    responses_received: u64,
    bodies_completed: u64,
    cancelled_requests: u64,
    failed_requests: u64,
    freed_handles: u64,
    max_active_handles: u64,
    events_received: u64,
    response_ready_events: u64,
    body_ready_events: u64,
    end_events: u64,
    failed_events: u64,
    cancelled_events: u64,
    handle_freed_events: u64,
    gateway_stopped_events: u64,
    timeout_events: u64,
    invalid_node_events: u64,
    unknown_handle_events: u64,
    stashed_unknown_handle_events: u64,
    stale_events: u64,
    event_service_collisions: u64,
    read_calls: u64,
    bytes_read: u64,
    max_retained_response_body_bytes: u64,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

#[derive(Serialize)]
struct NativeFfiStartRequest {
    method: String,
    path: String,
    headers: Vec<NativeFfiHeader>,
    request_id: Option<u64>,
    parent_request_id: Option<u64>,
    top_level_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NativeFfiHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct NativeFfiResponseJson {
    state: String,
    status: Option<u16>,
    headers: Vec<NativeFfiHeader>,
    error: Option<NativeFfiErrorJson>,
}

#[derive(Clone, Debug, Deserialize)]
struct NativeFfiErrorJson {
    code: String,
    message: String,
}

impl NativeFfiTransport {
    fn start(
        node: *mut FreedomIpfsNode,
        storage_path: PathBuf,
        remove_storage_on_stop: bool,
        config: NativeFfiConfig,
    ) -> Arc<Self> {
        let transport = Arc::new(Self {
            node: Arc::new(NativeFfiNode::new(node)),
            storage_path,
            remove_storage_on_stop,
            config,
            state: Mutex::new(NativeFfiTransportState::default()),
            stats: Mutex::new(NativeFfiStats::default()),
            shutdown: AtomicBool::new(false),
            dispatchers: Mutex::new(Vec::new()),
        });
        let mut dispatchers = Vec::new();
        for _ in 0..config.dispatcher_count {
            let transport_clone = transport.clone();
            dispatchers.push(std::thread::spawn(move || {
                transport_clone.dispatch_loop();
            }));
        }
        if let Ok(mut slots) = transport.dispatchers.lock() {
            *slots = dispatchers;
        }
        transport
    }

    async fn fetch_response(
        self: &Arc<Self>,
        url: &str,
        method: &str,
        range: Option<&str>,
        if_none_match: Option<&str>,
        correlation: Option<&RequestCorrelation>,
    ) -> std::result::Result<FetchResponse, String> {
        let request = native_ffi_start_request(url, method, range, if_none_match, correlation)?;
        let request_json = serde_json::to_string(&request).map_err(|err| err.to_string())?;
        let request_json = CString::new(request_json).map_err(|err| err.to_string())?;
        let handle =
            unsafe { freedom_ipfs_gateway_request_start(self.node.ptr(), request_json.as_ptr()) };
        if handle == 0 {
            return Err("native FFI request failed to start".to_string());
        }

        let (sender, receiver) = oneshot::channel();
        let stashed = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "native FFI state lock poisoned".to_string())?;
            state.active.insert(
                handle,
                NativeFfiActiveRequest {
                    started: Instant::now(),
                    status: None,
                    headers: HeaderMap::new(),
                    body: Vec::new(),
                    stream: FetchStreamMetrics::default(),
                    sender: Some(sender),
                    servicing: false,
                    deferred_events: 0,
                    cancelled_by_policy: false,
                },
            );
            let active_len = state.active.len() as u64;
            let stashed = state.stashed_events.remove(&handle);
            drop(state);
            if let Ok(mut stats) = self.stats.lock() {
                stats.requests_started = stats.requests_started.saturating_add(1);
                stats.max_active_handles = stats.max_active_handles.max(active_len);
            }
            stashed
        };
        if let Some(events) = stashed {
            self.handle_event(handle, events);
        }

        receiver
            .await
            .map_err(|_| "native FFI response channel closed".to_string())?
    }

    fn dispatch_loop(self: Arc<Self>) {
        while !self.shutdown.load(Ordering::Acquire) {
            let event = unsafe {
                freedom_ipfs_gateway_wait_next_event(self.node.ptr(), NATIVE_FFI_WAIT_TIMEOUT_MS)
            };
            match event.status {
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK => {
                    self.record_event_flags(event.events);
                    self.handle_event(event.request_handle, event.events);
                }
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT => {
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.timeout_events = stats.timeout_events.saturating_add(1);
                    }
                }
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED => {
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.gateway_stopped_events =
                            stats.gateway_stopped_events.saturating_add(1);
                    }
                    self.fail_all_active("gateway_stopped", "native FFI gateway stopped");
                    if self.shutdown.load(Ordering::Acquire) {
                        break;
                    }
                }
                FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE => {
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.invalid_node_events = stats.invalid_node_events.saturating_add(1);
                    }
                    self.fail_all_active("invalid_node", "native FFI node is invalid");
                    break;
                }
                _ => {}
            }
        }
    }

    fn handle_event(&self, handle: u64, mut events: u32) {
        loop {
            let should_service = {
                let mut state = match self.state.lock() {
                    Ok(state) => state,
                    Err(_) => return,
                };
                let Some(active) = state.active.get_mut(&handle) else {
                    let completed_handle = state.completed_handles.contains(&handle);
                    let free_without_registration =
                        events & FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED != 0;
                    if completed_handle || free_without_registration {
                        if let Ok(mut stats) = self.stats.lock() {
                            stats.unknown_handle_events =
                                stats.unknown_handle_events.saturating_add(1);
                            stats.stale_events = stats.stale_events.saturating_add(1);
                        }
                        return;
                    }
                    state
                        .stashed_events
                        .entry(handle)
                        .and_modify(|pending| *pending |= events)
                        .or_insert(events);
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.unknown_handle_events = stats.unknown_handle_events.saturating_add(1);
                        stats.stashed_unknown_handle_events =
                            stats.stashed_unknown_handle_events.saturating_add(1);
                    }
                    return;
                };
                if active.servicing {
                    active.deferred_events |= events;
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.event_service_collisions =
                            stats.event_service_collisions.saturating_add(1);
                    }
                    false
                } else {
                    active.servicing = true;
                    true
                }
            };
            if !should_service {
                return;
            }

            self.service_active_once(handle, events);

            let next_events = {
                let mut state = match self.state.lock() {
                    Ok(state) => state,
                    Err(_) => return,
                };
                let Some(active) = state.active.get_mut(&handle) else {
                    return;
                };
                active.servicing = false;
                let next = active.deferred_events;
                active.deferred_events = 0;
                (next != 0).then_some(next)
            };
            if let Some(next_events) = next_events {
                events = next_events;
            } else {
                return;
            }
        }
    }

    fn service_active_once(&self, handle: u64, events: u32) {
        if events & FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY != 0 {
            match self.response_metadata(handle) {
                Ok(metadata) => self.apply_response_metadata(handle, metadata),
                Err(err) => self.finish_request(handle, Err(err)),
            }
        }

        if events
            & (FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY
                | FREEDOM_IPFS_GATEWAY_EVENT_END
                | FREEDOM_IPFS_GATEWAY_EVENT_FAILED
                | FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED
                | FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED)
            == 0
        {
            return;
        }
        if let Err(err) = self.ensure_response_metadata(handle) {
            self.finish_request(handle, Err(err));
            return;
        }

        let mut buffer = vec![0u8; self.config.read_buffer_bytes];
        loop {
            let read = unsafe {
                freedom_ipfs_gateway_request_read(
                    self.node.ptr(),
                    handle,
                    buffer.as_mut_ptr(),
                    buffer.len(),
                )
            };
            self.record_read_call(read);
            match read.status {
                FREEDOM_IPFS_GATEWAY_READ_BYTES => {
                    self.apply_body_bytes(handle, &buffer[..read.bytes_read]);
                    if self.should_cancel_after_read(handle) {
                        self.cancel_request(handle);
                        break;
                    }
                    if self.config.slow_consumer_ms > 0 {
                        std::thread::sleep(Duration::from_millis(self.config.slow_consumer_ms));
                    }
                }
                FREEDOM_IPFS_GATEWAY_READ_PENDING => break,
                FREEDOM_IPFS_GATEWAY_READ_END => {
                    self.finish_success(handle);
                    break;
                }
                FREEDOM_IPFS_GATEWAY_READ_CANCELLED => {
                    self.finish_request(handle, Err("native FFI request cancelled".to_string()));
                    break;
                }
                FREEDOM_IPFS_GATEWAY_READ_FAILED => {
                    let err = self
                        .response_metadata(handle)
                        .ok()
                        .and_then(|metadata| metadata.error)
                        .map(|error| format!("{}: {}", error.code, error.message))
                        .unwrap_or_else(|| "native FFI request failed".to_string());
                    self.finish_request(handle, Err(err));
                    break;
                }
                FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE => {
                    self.finish_request(handle, Err("native FFI invalid handle".to_string()));
                    break;
                }
                status => {
                    self.finish_request(
                        handle,
                        Err(format!("native FFI unknown read status {status}")),
                    );
                    break;
                }
            }
        }
    }

    fn response_metadata(&self, handle: u64) -> std::result::Result<NativeFfiResponseJson, String> {
        let ptr = unsafe { freedom_ipfs_gateway_request_response_json(self.node.ptr(), handle) };
        if ptr.is_null() {
            return Err("native FFI response metadata returned null".to_string());
        }
        let json = unsafe {
            let value = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            freedom_ipfs_string_free(ptr);
            value
        };
        serde_json::from_str(&json).map_err(|err| format!("decode native FFI metadata: {err}"))
    }

    fn ensure_response_metadata(&self, handle: u64) -> std::result::Result<(), String> {
        let needs_metadata = self
            .state
            .lock()
            .map(|state| {
                state
                    .active
                    .get(&handle)
                    .is_some_and(|active| active.status.is_none())
            })
            .unwrap_or(false);
        if !needs_metadata {
            return Ok(());
        }
        let metadata = self.response_metadata(handle)?;
        if metadata.state == "pending" && metadata.status.is_none() && metadata.error.is_none() {
            return Ok(());
        }
        self.apply_response_metadata(handle, metadata);
        Ok(())
    }

    fn apply_response_metadata(&self, handle: u64, metadata: NativeFfiResponseJson) {
        if let Some(error) = metadata.error.clone() {
            if metadata.status.is_none() && metadata.state == "failed" {
                self.record_error(&error.code, &error.message);
            }
        }
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let Some(active) = state.active.get_mut(&handle) else {
            return;
        };
        if let Some(status) = metadata
            .status
            .and_then(|status| StatusCode::from_u16(status).ok())
        {
            active.status = Some(status);
        }
        let mut headers = HeaderMap::new();
        for header in metadata.headers {
            let Ok(name) = HeaderName::from_bytes(header.name.as_bytes()) else {
                continue;
            };
            let Ok(value) = HeaderValue::from_str(&header.value) else {
                continue;
            };
            headers.insert(name, value);
        }
        active.headers = headers;
        if let Ok(mut stats) = self.stats.lock() {
            stats.responses_received = stats.responses_received.saturating_add(1);
        }
    }

    fn apply_body_bytes(&self, handle: u64, bytes: &[u8]) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let Some(active) = state.active.get_mut(&handle) else {
            return;
        };
        if active.stream.chunk_count == 0 {
            active.stream.first_byte_ms = Some(active.started.elapsed().as_millis());
        }
        active.stream.chunk_count += 1;
        active.stream.max_chunk_bytes = active.stream.max_chunk_bytes.max(bytes.len());
        active.stream.max_buffered_bytes = active.stream.max_buffered_bytes.max(bytes.len());
        active.body.extend_from_slice(bytes);
        let body_len = active.body.len() as u64;
        drop(state);
        if let Ok(mut stats) = self.stats.lock() {
            stats.bytes_read = stats.bytes_read.saturating_add(bytes.len() as u64);
            stats.max_retained_response_body_bytes =
                stats.max_retained_response_body_bytes.max(body_len);
        }
    }

    fn should_cancel_after_read(&self, handle: u64) -> bool {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return false,
        };
        let Some(active) = state.active.get_mut(&handle) else {
            return false;
        };
        if active.cancelled_by_policy {
            return false;
        }
        let cancel_after_first_byte =
            self.config.cancel_after_first_byte && !active.body.is_empty();
        let cancel_after_ms = self
            .config
            .cancel_after_ms
            .is_some_and(|limit| active.started.elapsed() >= Duration::from_millis(limit));
        if cancel_after_first_byte || cancel_after_ms {
            active.cancelled_by_policy = true;
            return true;
        }
        false
    }

    fn cancel_request(&self, handle: u64) {
        let cancelled = unsafe { freedom_ipfs_gateway_request_cancel(self.node.ptr(), handle) };
        if cancelled {
            if let Ok(mut stats) = self.stats.lock() {
                stats.cancelled_requests = stats.cancelled_requests.saturating_add(1);
            }
        }
    }

    fn finish_success(&self, handle: u64) {
        let active = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state.remove_active(handle)
        };
        let Some(mut active) = active else {
            return;
        };
        active.stream.completed = true;
        let response = match active.status {
            Some(status) => Ok(fetch_response_from_parts(
                status,
                &active.headers,
                active.body,
                active
                    .stream
                    .first_byte_ms
                    .unwrap_or_else(|| active.started.elapsed().as_millis()),
                active.started.elapsed().as_millis(),
                active.stream,
            )),
            None => Err("native FFI completed without response metadata".to_string()),
        };
        self.free_handle(handle);
        if let Ok(mut stats) = self.stats.lock() {
            stats.bodies_completed = stats.bodies_completed.saturating_add(1);
        }
        if let Some(sender) = active.sender.take() {
            let _ = sender.send(response);
        }
    }

    fn finish_request(&self, handle: u64, result: std::result::Result<FetchResponse, String>) {
        let active = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state.remove_active(handle)
        };
        if let Some(mut active) = active {
            if let Err(err) = &result {
                self.record_error("native_ffi_request_failed", err);
            }
            self.free_handle(handle);
            if let Ok(mut stats) = self.stats.lock() {
                stats.failed_requests = stats.failed_requests.saturating_add(1);
            }
            if let Some(sender) = active.sender.take() {
                let _ = sender.send(result);
            }
        }
    }

    fn fail_all_active(&self, code: &str, message: &str) {
        let active = {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state.take_active()
        };
        self.record_error(code, message);
        for (handle, mut active) in active {
            if let Some(sender) = active.sender.take() {
                let _ = sender.send(Err(message.to_string()));
            }
            self.free_handle(handle);
        }
    }

    fn free_handle(&self, handle: u64) {
        let freed = unsafe { freedom_ipfs_gateway_request_free(self.node.ptr(), handle) };
        if freed {
            if let Ok(mut stats) = self.stats.lock() {
                stats.freed_handles = stats.freed_handles.saturating_add(1);
            }
        }
    }

    fn record_event_flags(&self, events: u32) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.events_received = stats.events_received.saturating_add(1);
            if events & FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY != 0 {
                stats.response_ready_events = stats.response_ready_events.saturating_add(1);
            }
            if events & FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY != 0 {
                stats.body_ready_events = stats.body_ready_events.saturating_add(1);
            }
            if events & FREEDOM_IPFS_GATEWAY_EVENT_END != 0 {
                stats.end_events = stats.end_events.saturating_add(1);
            }
            if events & FREEDOM_IPFS_GATEWAY_EVENT_FAILED != 0 {
                stats.failed_events = stats.failed_events.saturating_add(1);
            }
            if events & FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED != 0 {
                stats.cancelled_events = stats.cancelled_events.saturating_add(1);
            }
            if events & FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED != 0 {
                stats.handle_freed_events = stats.handle_freed_events.saturating_add(1);
            }
        }
    }

    fn record_read_call(&self, _read: FreedomIpfsGatewayReadResult) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.read_calls = stats.read_calls.saturating_add(1);
        }
    }

    fn record_error(&self, code: &str, message: &str) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.last_error_code = Some(code.to_string());
            stats.last_error_message = Some(message.chars().take(240).collect());
        }
    }

    fn stop_node(&self) {
        self.node.stop_gateway();
    }

    fn report(&self) -> NativeFfiTransportReport {
        let (active_handles_at_end, stashed_event_handles_at_end) = self
            .state
            .lock()
            .map(|state| (state.active.len() as u64, state.stashed_events.len() as u64))
            .unwrap_or_default();
        let stats = self.stats.lock().ok();
        let stats = stats.as_deref();
        NativeFfiTransportReport {
            dispatcher_count: self.config.dispatcher_count,
            read_buffer_bytes: self.config.read_buffer_bytes,
            slow_consumer_ms: self.config.slow_consumer_ms,
            cancel_after_first_byte: self.config.cancel_after_first_byte,
            cancel_after_ms: self.config.cancel_after_ms,
            stop_node_mid_run_ms: self.config.stop_node_mid_run_ms,
            max_active_request_limit: self.config.max_active_request_limit,
            mobile_layer: self.node.native_gateway_stats(),
            requests_started: stats
                .map(|stats| stats.requests_started)
                .unwrap_or_default(),
            responses_received: stats
                .map(|stats| stats.responses_received)
                .unwrap_or_default(),
            bodies_completed: stats
                .map(|stats| stats.bodies_completed)
                .unwrap_or_default(),
            cancelled_requests: stats
                .map(|stats| stats.cancelled_requests)
                .unwrap_or_default(),
            failed_requests: stats.map(|stats| stats.failed_requests).unwrap_or_default(),
            freed_handles: stats.map(|stats| stats.freed_handles).unwrap_or_default(),
            active_handles_at_end,
            stashed_event_handles_at_end,
            max_active_handles: stats
                .map(|stats| stats.max_active_handles)
                .unwrap_or_default(),
            events_received: stats.map(|stats| stats.events_received).unwrap_or_default(),
            response_ready_events: stats
                .map(|stats| stats.response_ready_events)
                .unwrap_or_default(),
            body_ready_events: stats
                .map(|stats| stats.body_ready_events)
                .unwrap_or_default(),
            end_events: stats.map(|stats| stats.end_events).unwrap_or_default(),
            failed_events: stats.map(|stats| stats.failed_events).unwrap_or_default(),
            cancelled_events: stats
                .map(|stats| stats.cancelled_events)
                .unwrap_or_default(),
            handle_freed_events: stats
                .map(|stats| stats.handle_freed_events)
                .unwrap_or_default(),
            gateway_stopped_events: stats
                .map(|stats| stats.gateway_stopped_events)
                .unwrap_or_default(),
            timeout_events: stats.map(|stats| stats.timeout_events).unwrap_or_default(),
            invalid_node_events: stats
                .map(|stats| stats.invalid_node_events)
                .unwrap_or_default(),
            unknown_handle_events: stats
                .map(|stats| stats.unknown_handle_events)
                .unwrap_or_default(),
            stashed_unknown_handle_events: stats
                .map(|stats| stats.stashed_unknown_handle_events)
                .unwrap_or_default(),
            stale_events: stats.map(|stats| stats.stale_events).unwrap_or_default(),
            event_service_collisions: stats
                .map(|stats| stats.event_service_collisions)
                .unwrap_or_default(),
            read_calls: stats.map(|stats| stats.read_calls).unwrap_or_default(),
            bytes_read: stats.map(|stats| stats.bytes_read).unwrap_or_default(),
            max_retained_response_body_bytes: stats
                .map(|stats| stats.max_retained_response_body_bytes)
                .unwrap_or_default(),
            last_error_code: stats.and_then(|stats| stats.last_error_code.clone()),
            last_error_message: stats.and_then(|stats| stats.last_error_message.clone()),
        }
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.stop_node();
        if let Ok(mut dispatchers) = self.dispatchers.lock() {
            for dispatcher in dispatchers.drain(..) {
                let _ = dispatcher.join();
            }
        }
        self.fail_all_active("native_ffi_shutdown", "native FFI transport shut down");
        self.node.free_on_thread();
        if self.remove_storage_on_stop {
            let _ = std::fs::remove_dir_all(&self.storage_path);
        }
    }
}

impl Drop for NativeFfiTransport {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.node.stop_gateway();
        if let Ok(mut dispatchers) = self.dispatchers.lock() {
            for dispatcher in dispatchers.drain(..) {
                let _ = dispatcher.join();
            }
        }
        self.node.free_on_thread();
        if self.remove_storage_on_stop {
            let _ = std::fs::remove_dir_all(&self.storage_path);
        }
    }
}

fn native_ffi_start_request(
    url: &str,
    method: &str,
    range: Option<&str>,
    if_none_match: Option<&str>,
    correlation: Option<&RequestCorrelation>,
) -> std::result::Result<NativeFfiStartRequest, String> {
    let method = match method {
        "GET" | "HEAD" => method,
        other => {
            return Err(format!(
                "unsupported method {other}; only GET and HEAD are supported"
            ))
        }
    };
    let parsed = Url::parse(url).map_err(|err| err.to_string())?;
    let path = percent_decode_path(parsed.path())?;
    if !path.starts_with("/ipfs/") && !path.starts_with("/ipns/") {
        return Err(format!(
            "native FFI path must start with /ipfs/ or /ipns/: {url}"
        ));
    }
    let mut headers = Vec::new();
    if let Some(range) = range {
        headers.push(NativeFfiHeader {
            name: RANGE.as_str().to_string(),
            value: range.to_string(),
        });
    }
    if let Some(if_none_match) = if_none_match {
        headers.push(NativeFfiHeader {
            name: IF_NONE_MATCH.as_str().to_string(),
            value: if_none_match.to_string(),
        });
    }
    Ok(NativeFfiStartRequest {
        method: method.to_string(),
        path,
        headers,
        request_id: correlation.map(|correlation| correlation.request_id),
        parent_request_id: correlation.and_then(|correlation| correlation.parent_id),
        top_level_path: correlation.map(|correlation| correlation.top_level_path.clone()),
    })
}

fn native_ffi_routing_mode(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<u32> {
    if bitswap_seed.is_some() {
        return Ok(FREEDOM_IPFS_ROUTING_MODE_DELEGATED);
    }
    match args.routing_mode.as_str() {
        "auto" => Ok(FREEDOM_IPFS_ROUTING_MODE_AUTO),
        "delegated" => Ok(FREEDOM_IPFS_ROUTING_MODE_DELEGATED),
        "light-dht" | "light_dht" => Ok(FREEDOM_IPFS_ROUTING_MODE_LIGHT_DHT),
        "offline" => Ok(FREEDOM_IPFS_ROUTING_MODE_OFFLINE),
        other => bail!(
            "unsupported routing mode {other:?}; expected auto, delegated, light-dht, or offline"
        ),
    }
}

#[derive(Clone, Copy)]
enum NativeGatewayNamespace {
    Ipfs,
    Ipns,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeRoutingMode {
    Auto,
    Delegated,
    LightDht,
    Offline,
}

fn apply_correlation_headers(
    mut request: reqwest::RequestBuilder,
    correlation: Option<&RequestCorrelation>,
) -> reqwest::RequestBuilder {
    if let Some(correlation) = correlation {
        request = request
            .header(X_FREEDOM_REQUEST_ID, correlation.request_id.to_string())
            .header(
                X_FREEDOM_TOP_LEVEL_PATH,
                correlation.top_level_path.as_str(),
            );
        if let Some(parent_id) = correlation.parent_id {
            request = request.header(X_FREEDOM_PARENT_REQUEST_ID, parent_id.to_string());
        }
    }
    request
}

fn apply_correlation_header_map(
    headers: &mut HeaderMap,
    correlation: Option<&RequestCorrelation>,
) -> std::result::Result<(), String> {
    let Some(correlation) = correlation else {
        return Ok(());
    };
    headers.insert(
        X_FREEDOM_REQUEST_ID,
        HeaderValue::from_str(&correlation.request_id.to_string())
            .map_err(|err| err.to_string())?,
    );
    headers.insert(
        X_FREEDOM_TOP_LEVEL_PATH,
        HeaderValue::from_str(&correlation.top_level_path).map_err(|err| err.to_string())?,
    );
    if let Some(parent_id) = correlation.parent_id {
        headers.insert(
            X_FREEDOM_PARENT_REQUEST_ID,
            HeaderValue::from_str(&parent_id.to_string()).map_err(|err| err.to_string())?,
        );
    }
    Ok(())
}

async fn fetch_response(
    client: &GatewayClient,
    url: &str,
    method: &str,
    range: Option<&str>,
    correlation: Option<&RequestCorrelation>,
) -> std::result::Result<FetchResponse, String> {
    client
        .fetch_response(url, method, range, None, correlation)
        .await
}

async fn fetch_http_response(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    range: Option<&str>,
    if_none_match: Option<&str>,
    correlation: Option<&RequestCorrelation>,
) -> std::result::Result<FetchResponse, String> {
    let started = Instant::now();
    let response = match method {
        "GET" => {
            let mut request = client.get(url);
            if let Some(range) = range {
                request = request.header(RANGE, range);
            }
            if let Some(if_none_match) = if_none_match {
                request = request.header(IF_NONE_MATCH, if_none_match);
            }
            let request = apply_correlation_headers(request, correlation);
            request.send().await
        }
        "HEAD" => {
            apply_correlation_headers(client.head(url), correlation)
                .send()
                .await
        }
        other => {
            return Err(format!(
                "unsupported method {other}; only GET and HEAD are supported"
            ))
        }
    }
    .map_err(|err| err.to_string())?;

    let ttfb_ms = started.elapsed().as_millis();
    let status = response.status();
    let headers = response.headers().clone();
    let (body, stream) = collect_body_stream(response.bytes_stream(), started, None).await?;
    let total_ms = started.elapsed().as_millis();

    Ok(fetch_response_from_parts(
        status, &headers, body, ttfb_ms, total_ms, stream,
    ))
}

async fn collect_body_stream<S, E>(
    stream: S,
    started: Instant,
    stop_after_chunks: Option<usize>,
) -> std::result::Result<(Vec<u8>, FetchStreamMetrics), String>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>>,
    E: std::fmt::Display,
{
    let mut stream = Box::pin(stream);
    let mut body = Vec::new();
    let mut metrics = FetchStreamMetrics::default();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| format!("body error: {err}"))?;
        if metrics.chunk_count == 0 {
            metrics.first_byte_ms = Some(started.elapsed().as_millis());
        }
        metrics.chunk_count += 1;
        metrics.max_chunk_bytes = metrics.max_chunk_bytes.max(chunk.len());
        metrics.max_buffered_bytes = metrics.max_buffered_bytes.max(chunk.len());
        body.extend_from_slice(&chunk);
        if stop_after_chunks.is_some_and(|limit| metrics.chunk_count >= limit) {
            metrics.cancelled = true;
            return Ok((body, metrics));
        }
    }
    metrics.completed = true;
    Ok((body, metrics))
}

fn fetch_response_from_parts(
    status: StatusCode,
    headers: &HeaderMap,
    body: Vec<u8>,
    ttfb_ms: u128,
    total_ms: u128,
    stream: FetchStreamMetrics,
) -> FetchResponse {
    FetchResponse {
        status: status.as_u16(),
        content_type: header_string(headers, CONTENT_TYPE.as_str()),
        content_range: header_string(headers, CONTENT_RANGE.as_str()),
        content_length: header_string(headers, CONTENT_LENGTH.as_str())
            .and_then(|value| value.parse::<u64>().ok()),
        accept_ranges: header_string(headers, ACCEPT_RANGES.as_str()),
        etag: header_string(headers, ETAG.as_str()),
        cache_control: header_string(headers, CACHE_CONTROL.as_str()),
        body,
        ttfb_ms,
        total_ms,
        stream,
    }
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn gateway_method(method: &str) -> std::result::Result<Method, String> {
    match method {
        "GET" => Ok(Method::GET),
        "HEAD" => Ok(Method::HEAD),
        other => Err(format!(
            "unsupported method {other}; only GET and HEAD are supported"
        )),
    }
}

fn native_gateway_url_path(
    url: &str,
) -> std::result::Result<(NativeGatewayNamespace, String), String> {
    let url = Url::parse(url).map_err(|err| err.to_string())?;
    let decoded = percent_decode_path(url.path())?;
    let path = decoded.trim_start_matches('/');
    if let Some(rest) = path.strip_prefix("ipfs/") {
        return Ok((NativeGatewayNamespace::Ipfs, rest.to_string()));
    }
    if let Some(rest) = path.strip_prefix("ipns/") {
        return Ok((NativeGatewayNamespace::Ipns, rest.to_string()));
    }
    Err(format!(
        "native gateway URL path must start with /ipfs/ or /ipns/: {url}"
    ))
}

fn percent_decode_path(path: &str) -> std::result::Result<String, String> {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(format!("invalid percent encoding in path {path:?}"));
            }
            let high = hex_value(bytes[index + 1])
                .ok_or_else(|| format!("invalid percent encoding in path {path:?}"))?;
            let low = hex_value(bytes[index + 2])
                .ok_or_else(|| format!("invalid percent encoding in path {path:?}"))?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|err| err.to_string())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_native_routing_mode(value: &str) -> Result<NativeRoutingMode> {
    match value {
        "auto" => Ok(NativeRoutingMode::Auto),
        "delegated" => Ok(NativeRoutingMode::Delegated),
        "light-dht" | "light_dht" => Ok(NativeRoutingMode::LightDht),
        "offline" => Ok(NativeRoutingMode::Offline),
        other => bail!(
            "unsupported routing mode {other:?}; expected auto, delegated, light-dht, or offline"
        ),
    }
}

fn native_gateway_config(args: &Args) -> GatewayConfig {
    GatewayConfig::new(args.max_concurrent_requests)
        .with_small_body_cache_max_bytes(args.small_body_cache_max_bytes)
        .with_html_prefetch(GatewayHtmlPrefetchConfig::new(
            env_usize(HTML_PREFETCH_MAX_ASSETS_ENV, 0),
            env_u64(HTML_PREFETCH_MAX_BYTES_ENV, 64 * 1024),
            env_usize(HTML_PREFETCH_CONCURRENCY_ENV, 2),
        ))
        .with_html_directory_prefetch(GatewayHtmlDirectoryPrefetchConfig::new(
            env_usize(HTML_DIRECTORY_PREFETCH_MAX_DIRS_ENV, 0),
            env_u64(HTML_DIRECTORY_PREFETCH_MAX_BYTES_ENV, 64 * 1024),
            env_usize(HTML_DIRECTORY_PREFETCH_CONCURRENCY_ENV, 1),
        ))
        .with_html_range_warm(GatewayHtmlRangeWarmConfig::new(
            env_u64(HTML_RANGE_WARM_MAX_BYTES_ENV, 0),
            env_usize(HTML_RANGE_WARM_CONCURRENCY_ENV, 1),
        ))
        .with_raw_link_tsize_fast_headers(
            std::env::var_os(RAW_LINK_TSIZE_FAST_HEADERS_ENV).is_some(),
        )
        .with_stream_small_bodies(std::env::var_os(STREAM_SMALL_BODIES_ENV).is_some())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn delegated_router_endpoints(delegated_routers: &str) -> Vec<String> {
    delegated_routers
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect()
}

fn native_ipns_resolver(
    routing_mode: NativeRoutingMode,
    delegated_routers: Vec<String>,
    dht: LightDhtClient,
) -> Arc<dyn IpnsResolver> {
    match routing_mode {
        NativeRoutingMode::Auto => Arc::new(FallbackIpnsResolver::new(
            DelegatedIpnsResolver::with_endpoints(delegated_routers),
            DhtIpnsResolver::new(dht),
        )),
        NativeRoutingMode::Delegated => {
            Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers))
        }
        NativeRoutingMode::LightDht => Arc::new(DhtIpnsResolver::new(dht)),
        NativeRoutingMode::Offline => Arc::new(NativeOfflineIpnsResolver),
    }
}

#[derive(Debug, Clone)]
struct NativeOfflineIpnsResolver;

#[async_trait::async_trait]
impl IpnsResolver for NativeOfflineIpnsResolver {
    async fn resolve_ipns(&self, name: &str) -> freedom_ipfs_namesys::Result<IpnsRecord> {
        Err(NamesysError::NotFound(name.to_string()))
    }
}

async fn maybe_revalidate_response(
    client: &GatewayClient,
    url: &str,
    method: &str,
    range: Option<&str>,
    response: &FetchResponse,
    conditional_revalidate: bool,
    correlation: Option<&RequestCorrelation>,
) -> Option<RevalidationResult> {
    if !conditional_revalidate
        || method != "GET"
        || range.is_some()
        || !(200..=299).contains(&response.status)
    {
        return None;
    }
    let Some(etag) = response.etag.as_deref() else {
        return Some(RevalidationResult {
            status: None,
            etag: None,
            cache_control: response.cache_control.clone(),
            body_bytes: 0,
            ttfb_ms: 0,
            total_ms: 0,
            stream: FetchStreamMetrics::default(),
            passed: false,
            failures: vec!["response omitted ETag".to_string()],
        });
    };
    let revalidation_correlation = correlation.map(RequestCorrelation::child);
    Some(fetch_revalidation(client, url, etag, revalidation_correlation.as_ref()).await)
}

async fn fetch_revalidation(
    client: &GatewayClient,
    url: &str,
    etag: &str,
    correlation: Option<&RequestCorrelation>,
) -> RevalidationResult {
    let started = Instant::now();
    let response = match client
        .fetch_response(url, "GET", None, Some(etag), correlation)
        .await
    {
        Ok(response) => response,
        Err(err) => {
            return RevalidationResult {
                status: None,
                etag: None,
                cache_control: None,
                body_bytes: 0,
                ttfb_ms: started.elapsed().as_millis(),
                total_ms: started.elapsed().as_millis(),
                stream: FetchStreamMetrics::default(),
                passed: false,
                failures: vec![format!("request error: {err}")],
            };
        }
    };

    let total_ms = started.elapsed().as_millis();
    let mut failures = Vec::new();
    if response.status != 304 {
        failures.push(format!("status {}, expected 304", response.status));
    }
    if !response.body.is_empty() {
        failures.push(format!(
            "body {} bytes, expected empty 304 body",
            response.body.len()
        ));
    }

    RevalidationResult {
        status: Some(response.status),
        etag: response.etag,
        cache_control: response.cache_control,
        body_bytes: response.body.len(),
        ttfb_ms: response.ttfb_ms,
        total_ms,
        stream: response.stream,
        passed: failures.is_empty(),
        failures,
    }
}

async fn run_page_crawl(
    client: &GatewayClient,
    page_url: &str,
    page_body: &[u8],
    config: &CrawlConfig,
    asset_concurrency: usize,
    conditional_revalidate: bool,
    root_correlation: &RequestCorrelation,
) -> (AssetSummary, Vec<AssetResult>, Vec<String>) {
    let max_assets = config.max_assets.unwrap_or(32);
    let same_origin_only = config.same_origin_only.unwrap_or(true);
    let mut failures = Vec::new();
    let page_url = match Url::parse(page_url) {
        Ok(url) => url,
        Err(err) => {
            failures.push(format!("crawl URL parse failed: {err}"));
            return (AssetSummary::default(), Vec::new(), failures);
        }
    };
    let page_html = String::from_utf8_lossy(page_body);
    let mut discovery = discover_html_assets(&page_html, &page_url, max_assets, same_origin_only);
    let mut seen = discovery
        .assets
        .iter()
        .map(|asset| asset.url.as_str().to_string())
        .collect::<HashSet<_>>();

    let mut fetched = fetch_assets(
        client,
        discovery.assets.clone(),
        asset_concurrency,
        config.asset_max_bytes.unwrap_or(2_000_000),
        conditional_revalidate,
        root_correlation,
    )
    .await;

    if config.include_css_assets.unwrap_or(true) && !discovery.truncated {
        let css_assets = discover_css_assets_from_fetches(
            &fetched,
            &mut seen,
            max_assets.saturating_sub(fetched.len()),
            same_origin_only,
            &mut discovery,
        );
        if !css_assets.is_empty() {
            let mut css_fetched = fetch_assets(
                client,
                css_assets,
                asset_concurrency,
                config.asset_max_bytes.unwrap_or(2_000_000),
                conditional_revalidate,
                root_correlation,
            )
            .await;
            fetched.append(&mut css_fetched);
        }
    }

    let assets = fetched
        .into_iter()
        .map(|fetched| fetched.result)
        .collect::<Vec<_>>();
    let failed = assets.iter().filter(|asset| !asset.passed).count();
    let passed = assets.len().saturating_sub(failed);
    let summary = AssetSummary {
        discovered: discovery.discovered,
        fetched: assets.len(),
        passed,
        failed,
        skipped_external: discovery.skipped_external,
        skipped_unsupported: discovery.skipped_unsupported,
        truncated: discovery.truncated,
    };

    if let Some(min_assets) = config.min_assets {
        if summary.fetched < min_assets {
            failures.push(format!(
                "crawl fetched {} assets, expected at least {min_assets}",
                summary.fetched
            ));
        }
    }
    let max_failed_assets = config.max_failed_assets.unwrap_or(0);
    if summary.failed > max_failed_assets {
        failures.push(format!(
            "crawl had {} failed assets, allowed {max_failed_assets}",
            summary.failed
        ));
    }

    (summary, assets, failures)
}

async fn fetch_assets(
    client: &GatewayClient,
    assets: Vec<DiscoveredAsset>,
    concurrency: usize,
    max_bytes: usize,
    conditional_revalidate: bool,
    root_correlation: &RequestCorrelation,
) -> Vec<FetchedAsset> {
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = JoinSet::new();
    for asset in assets {
        let client = client.clone();
        let semaphore = semaphore.clone();
        let correlation = root_correlation.child();
        tasks.spawn(async move {
            let _permit = semaphore.acquire_owned().await.ok();
            fetch_asset(
                &client,
                asset,
                max_bytes,
                conditional_revalidate,
                correlation,
            )
            .await
        });
    }

    let mut fetched = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Ok(asset) = result {
            fetched.push(asset);
        }
    }
    fetched
}

async fn fetch_asset(
    client: &GatewayClient,
    asset: DiscoveredAsset,
    max_bytes: usize,
    conditional_revalidate: bool,
    correlation: RequestCorrelation,
) -> FetchedAsset {
    let range = range_for_kind(asset.kind);
    let started_url = asset.url.to_string();
    let response = fetch_response(client, &started_url, "GET", range, Some(&correlation)).await;
    let mut failures = Vec::new();
    let mut result = AssetResult {
        kind: asset.kind,
        source: asset.source,
        url: started_url,
        status: None,
        content_type: None,
        content_range: None,
        content_length: None,
        accept_ranges: None,
        etag: None,
        cache_control: None,
        body_bytes: 0,
        ttfb_ms: 0,
        total_ms: 0,
        stream: FetchStreamMetrics::default(),
        body_preview: String::new(),
        revalidation: None,
        passed: false,
        failures: Vec::new(),
    };

    let response = match response {
        Ok(response) => response,
        Err(err) => {
            result.failures.push(format!("request error: {err}"));
            return FetchedAsset {
                result,
                body_text: None,
            };
        }
    };

    result.status = Some(response.status);
    result.content_type = response.content_type.clone();
    result.content_range = response.content_range.clone();
    result.content_length = response.content_length;
    result.accept_ranges = response.accept_ranges.clone();
    result.etag = response.etag.clone();
    result.cache_control = response.cache_control.clone();
    result.body_bytes = response.body.len();
    result.ttfb_ms = response.ttfb_ms;
    result.total_ms = response.total_ms;
    result.stream = response.stream;
    result.body_preview =
        String::from_utf8_lossy(&response.body.iter().copied().take(180).collect::<Vec<_>>())
            .replace('\n', "\\n");

    if !(200..=299).contains(&response.status) {
        failures.push(format!("status {} was not 2xx", response.status));
    }
    if let Some(expected) = mime_expectation(asset.kind) {
        let content_type = response.content_type.as_deref().unwrap_or("");
        if !expected.matches(content_type) {
            failures.push(format!(
                "content-type {:?} did not match {}",
                response.content_type,
                expected.label()
            ));
        }
    }
    if matches!(asset.kind, AssetKind::Audio | AssetKind::Video)
        && response.content_range.is_none()
        && response.status == 206
    {
        failures.push("media range response omitted Content-Range".to_string());
    }
    if response.body.len() > max_bytes {
        failures.push(format!(
            "body {} bytes exceeded asset cap {max_bytes}",
            response.body.len()
        ));
    }
    result.revalidation = maybe_revalidate_response(
        client,
        &result.url,
        "GET",
        range,
        &response,
        conditional_revalidate,
        Some(&correlation),
    )
    .await;
    if let Some(revalidation) = &result.revalidation {
        if !revalidation.passed {
            failures.extend(
                revalidation
                    .failures
                    .iter()
                    .map(|failure| format!("conditional revalidation: {failure}")),
            );
        }
    }

    let body_text = if asset.kind == AssetKind::Stylesheet
        && failures.is_empty()
        && response.body.len() <= max_bytes
    {
        String::from_utf8(response.body).ok()
    } else {
        None
    };
    result.passed = failures.is_empty();
    result.failures = failures;

    FetchedAsset { result, body_text }
}

fn range_for_kind(kind: AssetKind) -> Option<&'static str> {
    match kind {
        AssetKind::Image | AssetKind::Font | AssetKind::Audio | AssetKind::Video => {
            Some("bytes=0-4095")
        }
        AssetKind::Stylesheet | AssetKind::Script | AssetKind::Manifest | AssetKind::Other => None,
    }
}

fn mime_expectation(kind: AssetKind) -> Option<MimeExpectation> {
    match kind {
        AssetKind::Stylesheet => Some(MimeExpectation::AnyOf(&["text/css"])),
        AssetKind::Script => Some(MimeExpectation::AnyOf(&[
            "text/javascript",
            "application/javascript",
            "application/ecmascript",
        ])),
        AssetKind::Image => Some(MimeExpectation::Prefix("image/")),
        AssetKind::Font => Some(MimeExpectation::AnyOf(&[
            "font/",
            "application/font",
            "application/octet-stream",
        ])),
        AssetKind::Audio => Some(MimeExpectation::Prefix("audio/")),
        AssetKind::Video => Some(MimeExpectation::Prefix("video/")),
        AssetKind::Manifest => Some(MimeExpectation::AnyOf(&[
            "application/manifest+json",
            "application/json",
        ])),
        AssetKind::Other => None,
    }
}

enum MimeExpectation {
    Prefix(&'static str),
    AnyOf(&'static [&'static str]),
}

impl MimeExpectation {
    fn matches(&self, content_type: &str) -> bool {
        let content_type = content_type.to_ascii_lowercase();
        match self {
            Self::Prefix(prefix) => content_type.starts_with(prefix),
            Self::AnyOf(options) => options
                .iter()
                .any(|option| content_type.starts_with(option)),
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Prefix(prefix) => format!("prefix {prefix:?}"),
            Self::AnyOf(options) => format!("one of {options:?}"),
        }
    }
}

fn discover_html_assets(
    html: &str,
    page_url: &Url,
    max_assets: usize,
    same_origin_only: bool,
) -> AssetDiscovery {
    let content_root = content_root_url(page_url);
    let mut discovery = AssetDiscovery::default();
    let mut seen = HashSet::new();
    let mut base_url = page_url.clone();

    for tag in parse_html_tags(html) {
        if tag.name == "base" {
            if let Some(href) = tag.attr("href") {
                if let Some(url) = resolve_asset_url(href, page_url, &content_root, false) {
                    base_url = url;
                }
            }
            continue;
        }

        let candidates = html_asset_candidates(&tag);
        for (kind, raw, source) in candidates {
            push_discovered_asset(
                &mut discovery,
                &mut seen,
                kind,
                &raw,
                &source,
                &base_url,
                &content_root,
                same_origin_only,
                max_assets,
            );
        }
    }

    discovery
}

fn discover_css_assets_from_fetches(
    fetched: &[FetchedAsset],
    seen: &mut HashSet<String>,
    remaining_slots: usize,
    same_origin_only: bool,
    discovery: &mut AssetDiscovery,
) -> Vec<DiscoveredAsset> {
    let mut css_assets = Vec::new();
    if remaining_slots == 0 {
        return css_assets;
    }

    for fetched_asset in fetched {
        let Some(css) = fetched_asset.body_text.as_deref() else {
            continue;
        };
        let Ok(stylesheet_url) = Url::parse(&fetched_asset.result.url) else {
            continue;
        };
        let content_root = content_root_url(&stylesheet_url);
        for raw in extract_css_urls(css) {
            if css_assets.len() >= remaining_slots {
                discovery.truncated = true;
                return css_assets;
            }
            let kind = kind_from_url_hint(&raw).unwrap_or(AssetKind::Other);
            let before = discovery.assets.len();
            push_discovered_asset(
                discovery,
                seen,
                kind,
                &raw,
                "css:url",
                &stylesheet_url,
                &content_root,
                same_origin_only,
                usize::MAX,
            );
            if discovery.assets.len() > before {
                if let Some(asset) = discovery.assets.last().cloned() {
                    css_assets.push(asset);
                }
            }
        }
    }

    css_assets
}

fn html_asset_candidates(tag: &ParsedTag) -> Vec<(AssetKind, String, String)> {
    let mut candidates = Vec::new();
    match tag.name.as_str() {
        "script" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((
                    AssetKind::Script,
                    src.to_string(),
                    "script[src]".to_string(),
                ));
            }
        }
        "link" => {
            if let Some(href) = tag.attr("href") {
                let rel = tag.attr("rel").unwrap_or("").to_ascii_lowercase();
                let as_attr = tag.attr("as").unwrap_or("").to_ascii_lowercase();
                let kind = if rel.contains("stylesheet") {
                    Some(AssetKind::Stylesheet)
                } else if rel.contains("modulepreload") {
                    Some(AssetKind::Script)
                } else if rel.contains("preload") || rel.contains("prefetch") {
                    match as_attr.as_str() {
                        "style" => Some(AssetKind::Stylesheet),
                        "script" => Some(AssetKind::Script),
                        "image" => Some(AssetKind::Image),
                        "font" => Some(AssetKind::Font),
                        "audio" => Some(AssetKind::Audio),
                        "video" => Some(AssetKind::Video),
                        _ => kind_from_url_hint(href),
                    }
                } else if rel.contains("icon") || rel.contains("apple-touch-icon") {
                    Some(AssetKind::Image)
                } else if rel.contains("manifest") {
                    Some(AssetKind::Manifest)
                } else {
                    None
                };
                if let Some(kind) = kind {
                    candidates.push((kind, href.to_string(), "link[href]".to_string()));
                }
            }
        }
        "img" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((AssetKind::Image, src.to_string(), "img[src]".to_string()));
            }
            if let Some(srcset) = tag.attr("srcset") {
                for src in parse_srcset(srcset) {
                    candidates.push((AssetKind::Image, src, "img[srcset]".to_string()));
                }
            }
        }
        "source" => {
            let kind = match tag.attr("type").unwrap_or("") {
                media_type if media_type.starts_with("video/") => AssetKind::Video,
                media_type if media_type.starts_with("audio/") => AssetKind::Audio,
                media_type if media_type.starts_with("image/") => AssetKind::Image,
                _ => AssetKind::Other,
            };
            if let Some(src) = tag.attr("src") {
                candidates.push((kind, src.to_string(), "source[src]".to_string()));
            }
            if let Some(srcset) = tag.attr("srcset") {
                for src in parse_srcset(srcset) {
                    candidates.push((kind, src, "source[srcset]".to_string()));
                }
            }
        }
        "video" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((AssetKind::Video, src.to_string(), "video[src]".to_string()));
            }
        }
        "audio" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((AssetKind::Audio, src.to_string(), "audio[src]".to_string()));
            }
        }
        "track" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((AssetKind::Other, src.to_string(), "track[src]".to_string()));
            }
        }
        "iframe" | "embed" => {
            if let Some(src) = tag.attr("src") {
                candidates.push((
                    AssetKind::Other,
                    src.to_string(),
                    "embedded[src]".to_string(),
                ));
            }
        }
        "object" => {
            if let Some(data) = tag.attr("data") {
                candidates.push((
                    AssetKind::Other,
                    data.to_string(),
                    "object[data]".to_string(),
                ));
            }
        }
        _ => {}
    }
    candidates
}

#[allow(clippy::too_many_arguments)]
fn push_discovered_asset(
    discovery: &mut AssetDiscovery,
    seen: &mut HashSet<String>,
    kind: AssetKind,
    raw_url: &str,
    source: &str,
    base_url: &Url,
    content_root: &Option<Url>,
    same_origin_only: bool,
    max_assets: usize,
) {
    if discovery.assets.len() >= max_assets {
        discovery.truncated = true;
        return;
    }

    let Some(url) = resolve_asset_url(raw_url, base_url, content_root, same_origin_only) else {
        if is_external_url(raw_url) {
            discovery.skipped_external += 1;
        } else {
            discovery.skipped_unsupported += 1;
        }
        return;
    };
    let key = url.as_str().to_string();
    if !seen.insert(key) {
        return;
    }
    discovery.discovered += 1;
    discovery.assets.push(DiscoveredAsset {
        kind,
        source: source.to_string(),
        url,
    });
}

fn resolve_asset_url(
    raw_url: &str,
    base_url: &Url,
    content_root: &Option<Url>,
    same_origin_only: bool,
) -> Option<Url> {
    let raw_url = raw_url.trim();
    if raw_url.is_empty()
        || raw_url.starts_with('#')
        || raw_url.contains("${")
        || raw_url.contains("{{")
        || starts_with_scheme(raw_url, "data")
        || starts_with_scheme(raw_url, "blob")
        || starts_with_scheme(raw_url, "javascript")
        || starts_with_scheme(raw_url, "mailto")
        || starts_with_scheme(raw_url, "tel")
    {
        return None;
    }

    let resolved = if raw_url.starts_with("//")
        || raw_url.starts_with("/ipfs/")
        || raw_url.starts_with("/ipns/")
    {
        base_url.join(raw_url).ok()?
    } else if raw_url.starts_with('/') {
        if let Some(content_root) = content_root {
            content_root.join(raw_url.trim_start_matches('/')).ok()?
        } else {
            base_url.join(raw_url).ok()?
        }
    } else {
        base_url.join(raw_url).ok()?
    };

    if same_origin_only && !same_origin(base_url, &resolved) {
        return None;
    }
    Some(resolved)
}

fn content_root_url(url: &Url) -> Option<Url> {
    let mut segments = url.path().split('/').filter(|segment| !segment.is_empty());
    let namespace = segments.next()?;
    if namespace != "ipfs" && namespace != "ipns" {
        return None;
    }
    let root = segments.next()?;
    let mut content_root = url.clone();
    content_root.set_path(&format!("/{namespace}/{root}/"));
    content_root.set_query(None);
    content_root.set_fragment(None);
    Some(content_root)
}

fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

fn is_external_url(raw_url: &str) -> bool {
    raw_url.starts_with("//")
        || raw_url.contains("://")
        || starts_with_scheme(raw_url, "mailto")
        || starts_with_scheme(raw_url, "tel")
}

fn starts_with_scheme(value: &str, scheme: &str) -> bool {
    value
        .get(..scheme.len() + 1)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&format!("{scheme}:")))
}

fn parse_srcset(srcset: &str) -> Vec<String> {
    srcset
        .split(',')
        .filter_map(|candidate| candidate.split_whitespace().next())
        .filter(|candidate| !candidate.is_empty())
        .map(str::to_string)
        .collect()
}

fn extract_css_urls(css: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let lower = css.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(index) = lower[offset..].find("url(") {
        let start = offset + index + 4;
        let Some(end) = css[start..].find(')') else {
            break;
        };
        let raw = css[start..start + end]
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if !raw.is_empty() {
            urls.push(raw);
        }
        offset = start + end + 1;
    }

    let mut imports = VecDeque::new();
    let mut offset = 0;
    while let Some(index) = lower[offset..].find("@import") {
        let start = offset + index + "@import".len();
        let remainder = css[start..].trim_start();
        if let Some(quote) = remainder
            .chars()
            .next()
            .filter(|quote| *quote == '"' || *quote == '\'')
        {
            if let Some(end) = remainder[1..].find(quote) {
                imports.push_back(remainder[1..1 + end].to_string());
            }
        }
        offset = start + remainder.len().min(1);
    }
    urls.extend(imports);
    urls
}

fn kind_from_url_hint(raw_url: &str) -> Option<AssetKind> {
    let path = raw_url
        .split(['?', '#'])
        .next()
        .unwrap_or(raw_url)
        .to_ascii_lowercase();
    if path.ends_with(".css") {
        Some(AssetKind::Stylesheet)
    } else if path.ends_with(".js") || path.ends_with(".mjs") {
        Some(AssetKind::Script)
    } else if matches!(
        path.rsplit('.').next(),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "svg" | "ico")
    ) {
        Some(AssetKind::Image)
    } else if matches!(
        path.rsplit('.').next(),
        Some("woff" | "woff2" | "ttf" | "otf" | "eot")
    ) {
        Some(AssetKind::Font)
    } else if matches!(path.rsplit('.').next(), Some("mp3" | "wav" | "ogg" | "m4a")) {
        Some(AssetKind::Audio)
    } else if matches!(
        path.rsplit('.').next(),
        Some("mp4" | "webm" | "mov" | "m4v")
    ) {
        Some(AssetKind::Video)
    } else if path.ends_with(".webmanifest") || path.ends_with("manifest.json") {
        Some(AssetKind::Manifest)
    } else {
        None
    }
}

fn parse_html_tags(html: &str) -> Vec<ParsedTag> {
    let mut tags = Vec::new();
    let lower = html.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(start) = html[offset..].find('<') {
        let start = offset + start + 1;
        let Some(end) = html[start..].find('>') else {
            break;
        };
        let raw = &html[start..start + end];
        if let Some(tag) = parse_html_tag(raw) {
            let raw_name = tag.name.clone();
            tags.push(tag);
            if matches!(raw_name.as_str(), "script" | "style") {
                let close_tag = format!("</{raw_name}");
                if let Some(close_start) = lower[start + end + 1..].find(&close_tag) {
                    let close_start = start + end + 1 + close_start;
                    if let Some(close_end) = lower[close_start..].find('>') {
                        offset = close_start + close_end + 1;
                        continue;
                    }
                }
            }
        }
        offset = start + end + 1;
    }
    tags
}

fn parse_html_tag(raw: &str) -> Option<ParsedTag> {
    let raw = raw.trim();
    if raw.is_empty()
        || raw.starts_with('/')
        || raw.starts_with('!')
        || raw.starts_with('?')
        || raw.starts_with("--")
    {
        return None;
    }

    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'/' {
        index += 1;
    }
    if index == 0 {
        return None;
    }
    let name = raw[..index].to_ascii_lowercase();
    let mut attrs = Vec::new();

    while index < bytes.len() {
        while index < bytes.len() && (bytes[index].is_ascii_whitespace() || bytes[index] == b'/') {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let name_start = index;
        while index < bytes.len()
            && !bytes[index].is_ascii_whitespace()
            && bytes[index] != b'='
            && bytes[index] != b'/'
        {
            index += 1;
        }
        if index == name_start {
            break;
        }
        let attr_name = raw[name_start..index].to_ascii_lowercase();
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let attr_value = if index < bytes.len() && bytes[index] == b'=' {
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            if index < bytes.len() && (bytes[index] == b'"' || bytes[index] == b'\'') {
                let quote = bytes[index];
                index += 1;
                let value_start = index;
                while index < bytes.len() && bytes[index] != quote {
                    index += 1;
                }
                let value = raw[value_start..index].to_string();
                if index < bytes.len() {
                    index += 1;
                }
                value
            } else {
                let value_start = index;
                while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                    index += 1;
                }
                raw[value_start..index].to_string()
            }
        } else {
            String::new()
        };
        attrs.push((attr_name, html_unescape_minimal(&attr_value)));
    }

    Some(ParsedTag { name, attrs })
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn normalize_gateway_url(url: &str) -> String {
    url.trim().trim_end_matches('/').to_string()
}

#[derive(Debug)]
struct BitswapSeed {
    child: Child,
    repo: PathBuf,
    router_endpoint: String,
    provider_addr: String,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
    router_task: Option<JoinHandle<()>>,
}

impl BitswapSeed {
    async fn start_optional(args: &Args) -> Result<Option<Self>> {
        match &args.bitswap_seed_car {
            Some(car) => Self::start(&args.kubo_bin, car).await.map(Some),
            None => Ok(None),
        }
    }

    async fn start(kubo: &PathBuf, car: &Path) -> Result<Self> {
        let repo = unique_temp_path("freedom-ipfs-bitswap-seed-repo");
        std::fs::create_dir_all(&repo)
            .with_context(|| format!("create Bitswap seed Kubo repo {}", repo.display()))?;

        let api_port = reserve_loopback_port().context("reserve Bitswap seed Kubo API port")?;
        let gateway_port =
            reserve_loopback_port().context("reserve Bitswap seed Kubo gateway port")?;
        prepare_kubo_repo(kubo, &repo, api_port, gateway_port)?;
        kubo_ok_os(
            kubo,
            &repo,
            [OsStr::new("dag"), OsStr::new("import"), car.as_os_str()],
        )?;

        let mut command = Command::new(kubo);
        command
            .kill_on_drop(true)
            .env("IPFS_PATH", &repo)
            .env("IPFS_TELEMETRY", "off")
            .arg("daemon")
            .arg("--migrate=true")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn Bitswap seed Kubo daemon {}", kubo.display()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Bitswap seed Kubo stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("Bitswap seed Kubo stderr was not piped"))?;
        let stdout_task = Some(log_child_lines("bitswap-seed", stdout));
        let stderr_task = Some(log_child_lines("bitswap-seed", stderr));
        wait_for_kubo_api(&mut child, api_port).await?;

        let (peer_id, provider_addr) = kubo_seed_identity(api_port).await?;
        let (router_endpoint, router_task) =
            spawn_bitswap_seed_delegated_router(peer_id.clone(), provider_addr.clone()).await?;
        eprintln!(
            "bitswap seed provider {peer_id} listening at {provider_addr}; delegated router {router_endpoint}"
        );

        Ok(Self {
            child,
            repo,
            router_endpoint,
            provider_addr,
            stdout_task,
            stderr_task,
            router_task: Some(router_task),
        })
    }

    async fn stop(&mut self) {
        if let Some(router_task) = self.router_task.take() {
            router_task.abort();
            let _ = router_task.await;
        }
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        if let Some(stdout_task) = self.stdout_task.take() {
            stdout_task.abort();
            let _ = stdout_task.await;
        }
        if let Some(stderr_task) = self.stderr_task.take() {
            stderr_task.abort();
            let _ = stderr_task.await;
        }
        let _ = std::fs::remove_dir_all(&self.repo);
    }
}

async fn kubo_seed_identity(api_port: u16) -> Result<(String, String)> {
    let url = format!("http://127.0.0.1:{api_port}/api/v0/id");
    let value: serde_json::Value = reqwest::Client::new()
        .post(url)
        .send()
        .await
        .context("request Bitswap seed Kubo id")?
        .error_for_status()
        .context("Bitswap seed Kubo id status")?
        .json()
        .await
        .context("decode Bitswap seed Kubo id")?;
    let peer_id = value
        .get("ID")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("Bitswap seed Kubo id response omitted ID"))?
        .to_string();
    let provider_addr = value
        .get("Addresses")
        .and_then(serde_json::Value::as_array)
        .and_then(|addrs| {
            addrs
                .iter()
                .filter_map(serde_json::Value::as_str)
                .find(|addr| {
                    addr.contains("/ip4/127.0.0.1/tcp/")
                        && addr.ends_with(&format!("/p2p/{peer_id}"))
                })
        })
        .ok_or_else(|| anyhow!("Bitswap seed Kubo id response omitted a loopback TCP address"))?
        .to_string();
    Ok((peer_id, provider_addr))
}

async fn spawn_bitswap_seed_delegated_router(
    peer_id: String,
    provider_addr: String,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind Bitswap seed delegated router")?;
    let addr = listener
        .local_addr()
        .context("read delegated router addr")?;
    let endpoint = format!("http://{addr}/routing/v1");
    let body = Arc::new(
        serde_json::json!({
            "Providers": [
                {
                    "ID": peer_id,
                    "Addrs": [provider_addr],
                }
            ]
        })
        .to_string(),
    );
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let body = Arc::clone(&body);
                    tokio::spawn(async move {
                        let _ = handle_bitswap_seed_router_connection(stream, body).await;
                    });
                }
                Err(err) => {
                    eprintln!("bitswap seed delegated router accept failed: {err}");
                    break;
                }
            }
        }
    });
    Ok((endpoint, task))
}

async fn handle_bitswap_seed_router_connection(
    mut stream: TcpStream,
    body: Arc<String>,
) -> std::io::Result<()> {
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while request.len() < 8192 {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buf[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let request_line = request
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default()
        .trim_end_matches('\r');
    let ok = request_line.starts_with("GET /routing/v1/providers/");
    let (status, response_body) = if ok {
        ("200 OK", body.as_str())
    } else {
        ("404 Not Found", "")
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    stream.write_all(response.as_bytes()).await
}

#[derive(Debug)]
struct SpawnedGateway {
    child: Child,
    url: String,
    storage_path: Option<PathBuf>,
    remove_storage_on_stop: bool,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
    bitswap_seed_connect_elapsed_ms: Option<u128>,
    kubo_bin: Option<PathBuf>,
    kubo_api_port: Option<u16>,
}

impl SpawnedGateway {
    async fn start(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<Self> {
        match args.engine {
            HarnessEngine::RustHttp => Self::start_rust(args, bitswap_seed).await,
            HarnessEngine::RustNative | HarnessEngine::RustNativeFfi => {
                bail!("{} does not spawn a gateway process", args.engine.as_str())
            }
            HarnessEngine::Kubo => Self::start_kubo(args, bitswap_seed).await,
        }
    }

    async fn start_rust(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<Self> {
        let bin = args
            .gateway_bin
            .clone()
            .unwrap_or_else(|| PathBuf::from("target/debug/freedom-ipfs-gateway"));
        let routing_mode = if bitswap_seed.is_some() {
            "delegated"
        } else {
            &args.routing_mode
        };
        let mut command = Command::new(&bin);
        command
            .kill_on_drop(true)
            .arg("--online")
            .arg("--routing-mode")
            .arg(routing_mode)
            .arg("--max-concurrent-requests")
            .arg(args.max_concurrent_requests.to_string())
            .arg("--small-body-cache-max-bytes")
            .arg(args.small_body_cache_max_bytes.to_string())
            .arg("--dht-query-timeout-secs")
            .arg(args.dht_query_timeout_secs.to_string())
            .arg("--dht-max-providers")
            .arg(args.dht_max_providers.to_string())
            .arg("--addr")
            .arg("127.0.0.1:0")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(trace_output) = &args.trace_output {
            command.arg("--trace-output").arg(trace_output);
        }
        if let Some(trace_filter) = &args.trace_filter {
            command.arg("--trace-filter").arg(trace_filter);
        }
        if args.trace_span_list {
            command.arg("--trace-span-list");
        }
        if let Some(seed) = bitswap_seed {
            command.arg("--delegated-router").arg(&seed.router_endpoint);
        } else if let Some(delegated_router) = &args.delegated_router {
            command.arg("--delegated-router").arg(delegated_router);
        }
        if let Some(gateway_db) = &args.gateway_db {
            command.arg("--db").arg(gateway_db);
        }
        if let Some(import_car) = &args.gateway_import_car {
            command.arg("--import-car").arg(import_car);
        }
        let mut child = command.spawn().with_context(|| {
            format!(
                "spawn {}; build it first with `cargo build -p freedom-ipfs-gateway`",
                bin.display()
            )
        })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("gateway stderr was not piped"))?;
        let mut lines = BufReader::new(stderr).lines();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let line = tokio::time::timeout_at(deadline, lines.next_line())
                .await
                .context("timed out waiting for gateway listen address")?
                .context("read gateway stderr")?;
            let Some(line) = line else {
                bail!("gateway exited before reporting listen address");
            };
            if let Some(url) = line.strip_prefix("gateway listening on ") {
                let stderr_task = tokio::spawn(async move {
                    while let Ok(Some(line)) = lines.next_line().await {
                        eprintln!("gateway: {line}");
                    }
                });
                return Ok(Self {
                    child,
                    url: normalize_gateway_url(url),
                    storage_path: args.gateway_db.clone(),
                    remove_storage_on_stop: false,
                    stdout_task: None,
                    stderr_task: Some(stderr_task),
                    bitswap_seed_connect_elapsed_ms: None,
                    kubo_bin: None,
                    kubo_api_port: None,
                });
            }
            eprintln!("gateway: {line}");
        }
    }

    async fn start_kubo(args: &Args, bitswap_seed: Option<&BitswapSeed>) -> Result<Self> {
        let kubo = &args.kubo_bin;
        let (repo, remove_storage_on_stop) = match &args.kubo_repo {
            Some(repo) => (repo.clone(), false),
            None => (unique_temp_path("freedom-ipfs-kubo-repo"), true),
        };
        std::fs::create_dir_all(&repo)
            .with_context(|| format!("create Kubo repo {}", repo.display()))?;

        let api_port = reserve_loopback_port().context("reserve Kubo API port")?;
        let gateway_port = reserve_loopback_port().context("reserve Kubo gateway port")?;
        prepare_kubo_repo(kubo, &repo, api_port, gateway_port)?;
        if let Some(import_car) = &args.gateway_import_car {
            kubo_ok_os(
                kubo,
                &repo,
                [
                    OsStr::new("dag"),
                    OsStr::new("import"),
                    import_car.as_os_str(),
                ],
            )?;
        }

        let mut command = Command::new(kubo);
        command
            .kill_on_drop(true)
            .env("IPFS_PATH", &repo)
            .env("IPFS_TELEMETRY", "off")
            .arg("daemon")
            .arg("--migrate=true")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .with_context(|| format!("spawn Kubo daemon {}", kubo.display()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Kubo stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("Kubo stderr was not piped"))?;
        let stdout_task = Some(log_child_lines("kubo", stdout));
        let stderr_task = Some(log_child_lines("kubo", stderr));
        wait_for_kubo_api(&mut child, api_port).await?;
        let bitswap_seed_connect_elapsed_ms = if let Some(seed) = bitswap_seed {
            let started = Instant::now();
            kubo_ok_os(
                kubo,
                &repo,
                [
                    OsStr::new("swarm"),
                    OsStr::new("connect"),
                    OsStr::new(seed.provider_addr.as_str()),
                ],
            )?;
            Some(started.elapsed().as_millis())
        } else {
            None
        };
        let url = format!("http://127.0.0.1:{gateway_port}");
        eprintln!("kubo gateway listening on {url}");

        Ok(Self {
            child,
            url,
            storage_path: Some(repo),
            remove_storage_on_stop,
            stdout_task,
            stderr_task,
            bitswap_seed_connect_elapsed_ms,
            kubo_bin: Some(kubo.clone()),
            kubo_api_port: Some(api_port),
        })
    }

    async fn kubo_bitswap_stats(&self) -> Option<KuboBitswapStats> {
        let kubo = self.kubo_bin.as_ref()?;
        let api_port = self.kubo_api_port?;
        let api = format!("/ip4/127.0.0.1/tcp/{api_port}");
        let output = Command::new(kubo)
            .arg("--api")
            .arg(api)
            .arg("--enc=json")
            .arg("stats")
            .arg("bitswap")
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        parse_kubo_bitswap_stats(&output.stdout).ok()
    }

    async fn stop(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
        if let Some(stdout_task) = self.stdout_task.take() {
            stdout_task.abort();
            let _ = stdout_task.await;
        }
        if let Some(stderr_task) = self.stderr_task.take() {
            stderr_task.abort();
            let _ = stderr_task.await;
        }
        if self.remove_storage_on_stop {
            if let Some(path) = &self.storage_path {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }

    fn rss_kib(&self) -> Option<u64> {
        let pid = self.child.id()?;
        child_rss_kib(pid)
    }

    fn fd_count(&self) -> Option<usize> {
        let pid = self.child.id()?;
        child_fd_count(pid)
    }

    fn child_process_count(&self) -> Option<usize> {
        let pid = self.child.id()?;
        child_process_count(pid)
    }

    fn storage_bytes(&self) -> Option<u64> {
        self.storage_path
            .as_ref()
            .and_then(|path| storage_path_size_bytes(path).ok())
    }

    fn storage_path(&self) -> Option<String> {
        self.storage_path
            .as_ref()
            .map(|path| path.display().to_string())
    }
}

#[cfg(target_os = "linux")]
fn child_rss_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn child_rss_kib(_pid: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn child_fd_count(pid: u32) -> Option<usize> {
    Some(std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count())
}

#[cfg(not(target_os = "linux"))]
fn child_fd_count(_pid: u32) -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn child_process_count(parent_pid: u32) -> Option<usize> {
    let mut count = 0usize;
    for entry in std::fs::read_dir("/proc").ok()? {
        let entry = entry.ok()?;
        let file_name = entry.file_name();
        let pid = file_name.to_string_lossy();
        if !pid.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
        if parse_proc_stat_ppid(&stat) == Some(parent_pid) {
            count += 1;
        }
    }
    Some(count)
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_stat_ppid(stat: &str) -> Option<u32> {
    let after_name = stat.rsplit_once(") ")?;
    let mut fields = after_name.1.split_whitespace();
    let _state = fields.next()?;
    fields.next()?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn child_process_count(_parent_pid: u32) -> Option<usize> {
    None
}

fn log_child_lines<R>(prefix: &'static str, stream: R) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut lines = BufReader::new(stream).lines();
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("{prefix}: {line}");
        }
    })
}

fn unique_temp_path(prefix: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let id = NEXT_TEMP_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("{prefix}-{}-{millis}-{id}", std::process::id()))
}

fn offline_replay_trace_output(args: &Args, label: &str, temp_prefix: &str) -> Option<PathBuf> {
    args.trace_output
        .as_ref()
        .map(|path| labeled_trace_output(path, label))
        .or_else(|| {
            args.offline_replay_resolved_ipfs
                .then(|| unique_temp_path(temp_prefix))
        })
}

fn labeled_trace_output(path: &Path, label: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("trace");
    let extension = path.extension().and_then(|extension| extension.to_str());
    let file_name = match extension {
        Some(extension) if !extension.is_empty() => format!("{stem}-{label}.{extension}"),
        _ => format!("{stem}-{label}"),
    };
    parent.join(file_name)
}

fn successful_name_resolutions_from_trace(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read trace output {}", path.display()))?;
    let mut resolutions = BTreeMap::new();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value.get("phase").and_then(|phase| phase.as_str()) != Some("name_resolve") {
            continue;
        }
        if value.get("ok").and_then(|ok| ok.as_bool()) != Some(true) {
            continue;
        }
        let Some(name) = value.get("name").and_then(|name| name.as_str()) else {
            continue;
        };
        let Some(resolved_target) = value
            .get("resolved_target")
            .and_then(|target| target.as_str())
        else {
            continue;
        };
        if resolved_target.starts_with("/ipfs/") {
            resolutions.insert(name.to_string(), resolved_target.to_string());
        }
    }
    Ok(resolutions)
}

fn rewrite_corpus_for_resolved_ipfs_replay(
    corpus: &Corpus,
    resolutions: &BTreeMap<String, String>,
    cases: &[String],
) -> (Corpus, Vec<OfflineReplayPathRewrite>) {
    let mut corpus = corpus.clone();
    let mut rewrites = Vec::new();
    for entry in &mut corpus.entries {
        if !cases.is_empty() && !cases.iter().any(|case| case == &entry.id) {
            continue;
        }
        if let Some(rewrite) =
            rewrite_ipns_path_to_resolved_ipfs(&entry.id, &entry.path, resolutions)
        {
            entry.path = rewrite.rewritten_path.clone();
            rewrites.push(rewrite);
        }
    }
    (corpus, rewrites)
}

fn rewrite_ipns_path_to_resolved_ipfs(
    case_id: &str,
    path: &str,
    resolutions: &BTreeMap<String, String>,
) -> Option<OfflineReplayPathRewrite> {
    let (path_without_suffix, suffix) = split_gateway_path_suffix(path);
    let ipns = path_without_suffix.strip_prefix("/ipns/")?;
    let mut parts = ipns.splitn(2, '/');
    let name = parts.next().filter(|name| !name.is_empty())?;
    let rest = parts.next().unwrap_or_default();
    let resolved_target = resolutions.get(name)?;
    let mut rewritten_path = append_gateway_path(resolved_target, rest);
    if rest.is_empty() && path_without_suffix.ends_with('/') && !rewritten_path.ends_with('/') {
        rewritten_path.push('/');
    }
    rewritten_path.push_str(suffix);
    Some(OfflineReplayPathRewrite {
        case_id: case_id.to_string(),
        name: name.to_string(),
        resolved_target: resolved_target.clone(),
        original_path: path.to_string(),
        rewritten_path,
    })
}

fn split_gateway_path_suffix(path: &str) -> (&str, &str) {
    match path
        .char_indices()
        .find(|(_, ch)| matches!(ch, '?' | '#'))
        .map(|(index, _)| index)
    {
        Some(index) => (&path[..index], &path[index..]),
        None => (path, ""),
    }
}

fn append_gateway_path(base: &str, rest: &str) -> String {
    if rest.is_empty() {
        return base.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        rest.trim_start_matches('/')
    )
}

fn reserve_loopback_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn prepare_kubo_repo(
    kubo: &PathBuf,
    repo: &PathBuf,
    api_port: u16,
    gateway_port: u16,
) -> Result<()> {
    if !repo.join("config").exists() {
        kubo_ok(kubo, repo, ["init", "--empty-repo"])?;
        kubo_ok(kubo, repo, ["config", "profile", "apply", "lowpower"])?;
    }
    kubo_ok(
        kubo,
        repo,
        [
            "config",
            "Addresses.API",
            &format!("/ip4/127.0.0.1/tcp/{api_port}"),
        ],
    )?;
    kubo_ok(
        kubo,
        repo,
        [
            "config",
            "Addresses.Gateway",
            &format!("/ip4/127.0.0.1/tcp/{gateway_port}"),
        ],
    )?;
    kubo_ok(
        kubo,
        repo,
        [
            "config",
            "--json",
            "Addresses.Swarm",
            r#"["/ip4/127.0.0.1/tcp/0","/ip4/127.0.0.1/udp/0/quic-v1"]"#,
        ],
    )?;
    kubo_ok(
        kubo,
        repo,
        ["config", "--json", "Swarm.DisableNatPortMap", "true"],
    )?;
    kubo_ok(
        kubo,
        repo,
        ["config", "--json", "Discovery.MDNS.Enabled", "false"],
    )?;
    Ok(())
}

fn kubo_ok<const N: usize>(kubo: &PathBuf, repo: &PathBuf, args: [&str; N]) -> Result<()> {
    kubo_ok_os(kubo, repo, args)
}

fn kubo_ok_os<I, S>(kubo: &PathBuf, repo: &PathBuf, args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = std::process::Command::new(kubo)
        .env("IPFS_PATH", repo)
        .env("IPFS_TELEMETRY", "off")
        .args(args)
        .output()
        .with_context(|| format!("run Kubo command {}", kubo.display()))?;
    if !output.status.success() {
        bail!(
            "Kubo command failed with status {}: stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn wait_for_kubo_api(child: &mut Child, api_port: u16) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{api_port}/api/v0/version");
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if let Some(status) = child.try_wait().context("check Kubo daemon status")? {
            bail!("Kubo daemon exited before readiness with {status}");
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for Kubo API at {url}");
        }
        match client.post(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ => tokio::time::sleep(Duration::from_millis(150)).await,
        }
    }
}

fn parse_kubo_bitswap_stats(bytes: &[u8]) -> Result<KuboBitswapStats> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).context("parse Kubo bitswap stats JSON")?;
    Ok(KuboBitswapStats {
        blocks_received: json_u64_field(&value, "BlocksReceived"),
        data_received: json_u64_field(&value, "DataReceived"),
        blocks_sent: json_u64_field(&value, "BlocksSent"),
        data_sent: json_u64_field(&value, "DataSent"),
        dup_blocks_received: json_u64_field(&value, "DupBlksReceived")
            .or_else(|| json_u64_field(&value, "DupBlocksReceived")),
        dup_data_received: json_u64_field(&value, "DupDataReceived"),
        messages_received: json_u64_field(&value, "MessagesReceived"),
        wantlist_len: json_array_len_field(&value, "Wantlist"),
        peers_len: json_array_len_field(&value, "Peers"),
    })
}

fn json_u64_field(value: &serde_json::Value, key: &str) -> Option<u64> {
    value.get(key)?.as_u64()
}

fn json_array_len_field(value: &serde_json::Value, key: &str) -> Option<u64> {
    value
        .get(key)?
        .as_array()
        .and_then(|items| u64::try_from(items.len()).ok())
}

fn storage_path_size_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        let mut total = metadata.len();
        for suffix in ["-wal", "-shm"] {
            let mut sidecar = path.as_os_str().to_os_string();
            sidecar.push(suffix);
            match std::fs::symlink_metadata(PathBuf::from(sidecar)) {
                Ok(sidecar_metadata) if sidecar_metadata.is_file() => {
                    total = total.saturating_add(sidecar_metadata.len());
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        return Ok(total);
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(storage_path_size_bytes(&entry.path())?);
    }
    Ok(total)
}

#[derive(Clone, Debug, Deserialize)]
struct Corpus {
    entries: Vec<CorpusEntry>,
}

impl Corpus {
    fn read(path: &PathBuf) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
    }
}

async fn extend_corpus_with_ens_names(corpus: &mut Corpus, path: &Path) -> Result<()> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read ENS corpus {}", path.display()))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("build ENS resolver HTTP client")?;
    let mut added = 0usize;
    for line in text.lines() {
        let name = line.split('#').next().unwrap_or("").trim();
        if name.is_empty() {
            continue;
        }
        match resolve_ens_contenthash_path(&client, name).await {
            Ok(resolved_path) => {
                eprintln!("resolved ENS corpus {name} -> {resolved_path}");
                corpus
                    .entries
                    .push(ens_live_corpus_entry(name, &resolved_path));
                added += 1;
            }
            Err(err) => {
                eprintln!("skipping ENS corpus {name}: {err:#}");
            }
        }
    }
    if added == 0 {
        bail!("no ENS corpus entries resolved from {}", path.display());
    }
    Ok(())
}

async fn resolve_ens_contenthash_path(client: &reqwest::Client, name: &str) -> Result<String> {
    let url = format!("https://api.web3.bio/profile/ens/{name}");
    let profile = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request ENS profile for {name}"))?
        .error_for_status()
        .with_context(|| format!("ENS profile returned error for {name}"))?
        .json::<EnsProfile>()
        .await
        .with_context(|| format!("decode ENS profile for {name}"))?;
    let contenthash = profile
        .contenthash
        .with_context(|| format!("{name} has no contenthash"))?;
    contenthash_to_gateway_path(&contenthash)
        .with_context(|| format!("{name} has unsupported contenthash {contenthash}"))
}

fn contenthash_to_gateway_path(contenthash: &str) -> Option<String> {
    let (namespace, path) = if let Some(path) = contenthash.strip_prefix("ipfs://") {
        ("ipfs", path)
    } else if let Some(path) = contenthash.strip_prefix("ipns://") {
        ("ipns", path)
    } else {
        return None;
    };
    let path = path.trim_start_matches('/');
    let path = path
        .strip_prefix(&format!("{namespace}/"))
        .unwrap_or(path)
        .trim_start_matches('/');
    if path.is_empty() {
        None
    } else {
        Some(format!("/{namespace}/{path}"))
    }
}

fn ens_live_corpus_entry(name: &str, path: &str) -> CorpusEntry {
    CorpusEntry {
        id: ens_live_case_id(name),
        description: Some(format!(
            "Live ENS contenthash target for {name}; resolved outside the gateway at harness startup."
        )),
        path: path.to_string(),
        default_enabled: Some(true),
        method: Some("GET".to_string()),
        range: None,
        crawl: Some(CrawlConfig {
            max_assets: Some(32),
            min_assets: Some(0),
            max_failed_assets: Some(8),
            same_origin_only: Some(true),
            include_css_assets: Some(true),
            asset_max_bytes: Some(2_000_000),
        }),
        expect_status: Some(200),
        expect_content_type_prefix: None,
        expect_content_range_prefix: None,
        expect_content_length: None,
        expect_accept_ranges: None,
        expect_etag_prefix: None,
        expect_cache_control: None,
        expect_body_contains: None,
        expect_body_bytes: None,
        expect_body_sha256: None,
        min_bytes: Some(1),
        max_ttfb_ms: Some(120_000),
    }
}

fn ens_live_case_id(name: &str) -> String {
    let mut id = String::from("ens-");
    let mut previous_dash = false;
    for ch in name.chars() {
        let next = if ch.is_ascii_alphanumeric() {
            previous_dash = false;
            Some(ch.to_ascii_lowercase())
        } else if previous_dash {
            None
        } else {
            previous_dash = true;
            Some('-')
        };
        if let Some(ch) = next {
            id.push(ch);
        }
    }
    while id.ends_with('-') {
        id.pop();
    }
    id
}

#[derive(Debug, Deserialize)]
struct EnsProfile {
    contenthash: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct CorpusEntry {
    id: String,
    description: Option<String>,
    path: String,
    default_enabled: Option<bool>,
    method: Option<String>,
    range: Option<String>,
    crawl: Option<CrawlConfig>,
    expect_status: Option<u16>,
    expect_content_type_prefix: Option<String>,
    expect_content_range_prefix: Option<String>,
    expect_content_length: Option<u64>,
    expect_accept_ranges: Option<String>,
    expect_etag_prefix: Option<String>,
    expect_cache_control: Option<String>,
    expect_body_contains: Option<String>,
    expect_body_bytes: Option<usize>,
    expect_body_sha256: Option<String>,
    min_bytes: Option<usize>,
    max_ttfb_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct CrawlConfig {
    max_assets: Option<usize>,
    min_assets: Option<usize>,
    max_failed_assets: Option<usize>,
    same_origin_only: Option<bool>,
    include_css_assets: Option<bool>,
    asset_max_bytes: Option<usize>,
}

#[derive(Debug, Serialize)]
struct RunReport {
    gateway_url: Option<String>,
    generated_at_unix_seconds: u64,
    repeat: usize,
    warmup_runs: usize,
    fresh_gateway_per_run: bool,
    asset_concurrency: usize,
    conditional_revalidate: bool,
    run_timeout_secs: Option<u64>,
    engine: HarnessEngine,
    small_body_cache_max_bytes: Option<usize>,
    gateway_db: Option<String>,
    gateway_import_car: Option<String>,
    bitswap_seed_car: Option<String>,
    bitswap_seed_connection_setup: Option<BitswapSeedConnectionSetup>,
    kubo_repo: Option<String>,
    trace_output: Option<String>,
    trace_span_list: Option<bool>,
    trace_summary: Option<TraceSummary>,
    trace_requirements: TraceRequirementsReport,
    summary: RepeatSummary,
    runs: Vec<RunResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BitswapSeedConnectionSetup {
    DelegatedRouterProviderLookup,
    SwarmConnectBeforeRequest,
}

impl BitswapSeedConnectionSetup {
    fn as_str(self) -> &'static str {
        match self {
            Self::DelegatedRouterProviderLookup => "delegated_router_provider_lookup",
            Self::SwarmConnectBeforeRequest => "swarm_connect_before_request",
        }
    }
}

fn bitswap_seed_connection_setup(args: &Args) -> Option<BitswapSeedConnectionSetup> {
    args.bitswap_seed_car.as_ref()?;
    match args.engine {
        HarnessEngine::RustHttp | HarnessEngine::RustNative | HarnessEngine::RustNativeFfi => {
            Some(BitswapSeedConnectionSetup::DelegatedRouterProviderLookup)
        }
        HarnessEngine::Kubo => Some(BitswapSeedConnectionSetup::SwarmConnectBeforeRequest),
    }
}

#[derive(Debug, Serialize)]
struct RunResult {
    phase: RunPhase,
    run_index: usize,
    gateway_url: String,
    elapsed_ms: u128,
    gateway_rss_kib: Option<u64>,
    gateway_fd_count: Option<usize>,
    gateway_child_process_count: Option<usize>,
    gateway_storage_bytes: Option<u64>,
    gateway_storage_path: Option<String>,
    bitswap_seed_connect_elapsed_ms: Option<u128>,
    kubo_bitswap_stats: Option<KuboBitswapStats>,
    native_ffi: Option<NativeFfiTransportReport>,
    passed: bool,
    results: Vec<CaseResult>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct NativeFfiTransportReport {
    dispatcher_count: usize,
    read_buffer_bytes: usize,
    slow_consumer_ms: u64,
    cancel_after_first_byte: bool,
    cancel_after_ms: Option<u64>,
    stop_node_mid_run_ms: Option<u64>,
    max_active_request_limit: Option<usize>,
    mobile_layer: Option<NativeFfiMobileStats>,
    requests_started: u64,
    responses_received: u64,
    bodies_completed: u64,
    cancelled_requests: u64,
    failed_requests: u64,
    freed_handles: u64,
    active_handles_at_end: u64,
    stashed_event_handles_at_end: u64,
    max_active_handles: u64,
    events_received: u64,
    response_ready_events: u64,
    body_ready_events: u64,
    end_events: u64,
    failed_events: u64,
    cancelled_events: u64,
    handle_freed_events: u64,
    gateway_stopped_events: u64,
    timeout_events: u64,
    invalid_node_events: u64,
    unknown_handle_events: u64,
    stashed_unknown_handle_events: u64,
    stale_events: u64,
    event_service_collisions: u64,
    read_calls: u64,
    bytes_read: u64,
    max_retained_response_body_bytes: u64,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct NativeFfiMobileStats {
    active_native_handles: u64,
    total_started: u64,
    total_completed: u64,
    total_failed: u64,
    total_cancelled: u64,
    total_freed: u64,
    bytes_read: u64,
    max_active_handles: u64,
    events_enqueued: u64,
    events_delivered: u64,
    events_coalesced: u64,
    max_event_queue_depth: u64,
    pending_event_queue_depth: u64,
    pending_event_handle_count: u64,
    stop_generation: u64,
    last_native_error_code: Option<String>,
    last_native_error_message: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct KuboBitswapStats {
    blocks_received: Option<u64>,
    data_received: Option<u64>,
    blocks_sent: Option<u64>,
    data_sent: Option<u64>,
    dup_blocks_received: Option<u64>,
    dup_data_received: Option<u64>,
    messages_received: Option<u64>,
    wantlist_len: Option<u64>,
    peers_len: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ComparisonReport {
    generated_at_unix_seconds: u64,
    rust: RunReport,
    kubo: RunReport,
    cases: Vec<ComparisonCase>,
}

#[derive(Debug, Serialize)]
struct OfflineReplayReport {
    generated_at_unix_seconds: u64,
    replay_db: String,
    resolved_ipfs_replay: bool,
    resolved_ipfs_rewrites: Vec<OfflineReplayPathRewrite>,
    online: RunReport,
    offline: RunReport,
    summary: OfflineReplaySummary,
}

#[derive(Debug, Serialize)]
struct OfflineReplayPathRewrite {
    case_id: String,
    name: String,
    resolved_target: String,
    original_path: String,
    rewritten_path: String,
}

#[derive(Debug, Serialize)]
struct OfflineReplaySummary {
    missing_url_count: usize,
    missing_urls: Vec<OfflineReplayMissingUrl>,
    offline_storage_bytes: Option<u64>,
    offline_request_statuses: Vec<TraceValueCount>,
    offline_network_phases: Vec<TraceValueCount>,
    offline_cache_phases: Vec<TraceValueCount>,
    offline_block_sources: Vec<TraceValueCount>,
    offline_non_cache_block_sources: Vec<TraceValueCount>,
    offline_trace_errors: Vec<TraceValueCount>,
    offline_progress_phases: Vec<TraceValueCount>,
}

#[derive(Debug, Serialize)]
struct OfflineReplayMissingUrl {
    kind: String,
    case_id: String,
    url: String,
    failures: Vec<String>,
}

impl OfflineReplaySummary {
    fn from_report(report: &RunReport) -> Self {
        let mut missing_urls = Vec::new();
        for run in report
            .runs
            .iter()
            .filter(|run| run.phase == RunPhase::Measured)
        {
            for result in &run.results {
                if !result.passed {
                    missing_urls.push(OfflineReplayMissingUrl {
                        kind: "root".to_string(),
                        case_id: result.id.clone(),
                        url: result.url.clone(),
                        failures: result.failures.clone(),
                    });
                }
                for asset in &result.assets {
                    if !asset.passed {
                        missing_urls.push(OfflineReplayMissingUrl {
                            kind: asset.kind.to_string(),
                            case_id: result.id.clone(),
                            url: asset.url.clone(),
                            failures: asset.failures.clone(),
                        });
                    }
                }
            }
        }
        let offline_storage_bytes = report.summary.gateway_storage_bytes.max;
        let (
            offline_request_statuses,
            offline_network_phases,
            offline_cache_phases,
            offline_block_sources,
            offline_non_cache_block_sources,
            offline_trace_errors,
            offline_progress_phases,
        ) = report
            .trace_summary
            .as_ref()
            .map(|trace| {
                (
                    trace.request_statuses.clone(),
                    trace_phase_counts_by_name(trace, OFFLINE_NETWORK_TRACE_PHASES),
                    trace_phase_counts_by_name(trace, OFFLINE_CACHE_TRACE_PHASES),
                    trace.block_sources.clone(),
                    non_cache_block_sources(&trace.block_sources),
                    trace.trace_errors.clone(),
                    trace.progress_phases.clone(),
                )
            })
            .unwrap_or_default();
        Self {
            missing_url_count: missing_urls.len(),
            missing_urls,
            offline_storage_bytes,
            offline_request_statuses,
            offline_network_phases,
            offline_cache_phases,
            offline_block_sources,
            offline_non_cache_block_sources,
            offline_trace_errors,
            offline_progress_phases,
        }
    }
}

const OFFLINE_NETWORK_TRACE_PHASES: &[&str] = &[
    "provider_lookup",
    "delegated_provider_lookup",
    "delegated_provider_empty_retry",
    "delegated_provider_self_hedge",
    "delegated_provider_self_hedge_result",
    "dht_provider_lookup",
    "provider_diversity_low",
    "provider_fetch_start",
    "provider_refresh_skipped_empty_provider_set",
    "provider_retry_after_request_timeout",
    "provider_retry_after_timeout",
    "provider_retry_after_connection_timeout",
    "retry_provider_count",
    "http_provider_race",
    "http_provider_hedge",
    "http_provider_self_hedge",
    "http_provider_self_hedge_skip",
    "http_provider_race_result",
    "http_provider_bitswap_hedge",
    "http_provider_bitswap_hedge_result",
    "http_provider_fetch",
    "http_provider_candidate_cancelled",
    "bitswap_provider_candidates_empty",
    "bitswap_peer_expand",
    "bad_peer_skipped",
    "bitswap_fetch",
    "bitswap_fetch_cancelled",
    "bitswap_request_timeout",
    "bitswap_request_timeout_detail",
    "bitswap_peer_timeout_suppressed",
    "bitswap_peer_timeout",
    "bitswap_client_reset",
    "bitswap_session_shortcut_start",
    "bitswap_session_shortcut",
    "bitswap_dns_prefetch",
    "bitswap_dnsaddr_expand",
    "bitswap_dns_multiaddr_expand",
    "bitswap_incoming_batch",
    "bitswap_batch_failed",
    "bitswap_peer_attempt_start",
    "bitswap_peer_attempt",
    "bitswap_connection_established",
    "bitswap_connection_closed",
    "bitswap_connection_error",
    "bitswap_connection_error_backoff",
    "bitswap_connection_error_peer_skipped",
    "bitswap_dial_plan",
    "bitswap_dial_rejected",
    "bitswap_dial_waiters_dropped",
    "bitswap_incoming_stream_read",
    "bitswap_incoming_block",
];

const OFFLINE_CACHE_TRACE_PHASES: &[&str] = &[
    "name_persistent_cache",
    "name_cache",
    "provider_cache",
    "block_store_get",
    "block_store_get_range",
    "unixfs_metadata_cache",
    "gateway_direct_body",
    "gateway_stream_done",
];

fn trace_phase_counts_by_name(trace: &TraceSummary, phase_names: &[&str]) -> Vec<TraceValueCount> {
    let wanted = phase_names.iter().copied().collect::<HashSet<_>>();
    trace
        .event_phases
        .iter()
        .filter(|phase| wanted.contains(phase.value.as_str()))
        .cloned()
        .collect()
}

fn non_cache_block_sources(block_sources: &[TraceValueCount]) -> Vec<TraceValueCount> {
    block_sources
        .iter()
        .filter(|source| source.value != "cache")
        .cloned()
        .collect()
}

#[derive(Debug, Serialize)]
struct ComparisonCase {
    id: String,
    rust_pass_rate: f64,
    kubo_pass_rate: f64,
    meaningful_kubo_wins: Vec<ComparisonKuboWin>,
    asset_comparisons: Vec<ComparisonAsset>,
    rust_root_ttfb_p50_ms: Option<u128>,
    kubo_root_ttfb_p50_ms: Option<u128>,
    root_ttfb_p50_ratio: Option<f64>,
    rust_root_ttfb_p95_ms: Option<u128>,
    kubo_root_ttfb_p95_ms: Option<u128>,
    root_ttfb_p95_ratio: Option<f64>,
    rust_root_total_p50_ms: Option<u128>,
    kubo_root_total_p50_ms: Option<u128>,
    root_total_p50_ratio: Option<f64>,
    rust_root_total_p95_ms: Option<u128>,
    kubo_root_total_p95_ms: Option<u128>,
    root_total_p95_ratio: Option<f64>,
    kubo_setup_adjusted_root_ttfb_p50_ms: Option<u128>,
    setup_adjusted_root_ttfb_p50_ratio: Option<f64>,
    kubo_setup_adjusted_root_ttfb_p95_ms: Option<u128>,
    setup_adjusted_root_ttfb_p95_ratio: Option<f64>,
    rust_asset_ttfb_p50_ms: Option<u128>,
    kubo_asset_ttfb_p50_ms: Option<u128>,
    asset_ttfb_p50_ratio: Option<f64>,
    rust_asset_ttfb_p95_ms: Option<u128>,
    kubo_asset_ttfb_p95_ms: Option<u128>,
    asset_ttfb_p95_ratio: Option<f64>,
    rust_asset_total_p50_ms: Option<u128>,
    kubo_asset_total_p50_ms: Option<u128>,
    asset_total_p50_ratio: Option<f64>,
    rust_asset_total_p95_ms: Option<u128>,
    kubo_asset_total_p95_ms: Option<u128>,
    asset_total_p95_ratio: Option<f64>,
    rust_max_rss_kib: Option<u64>,
    kubo_max_rss_kib: Option<u64>,
    rss_ratio: Option<f64>,
    rust_max_fd_count: Option<u64>,
    kubo_max_fd_count: Option<u64>,
    fd_ratio: Option<f64>,
    rust_max_child_process_count: Option<usize>,
    kubo_max_child_process_count: Option<usize>,
    rust_max_storage_bytes: Option<u64>,
    kubo_max_storage_bytes: Option<u64>,
    storage_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct ComparisonKuboWin {
    metric: String,
    rust_ms: u128,
    kubo_ms: u128,
    delta_ms: u128,
    ratio: f64,
}

#[derive(Debug, Serialize)]
struct ComparisonAsset {
    path: String,
    kind: String,
    source: String,
    rust_trace: Option<TraceRequestPathAggregate>,
    rust_count: usize,
    kubo_count: usize,
    rust_pass_count: usize,
    kubo_pass_count: usize,
    meaningful_kubo_wins: Vec<ComparisonKuboWin>,
    rust_ttfb_p50_ms: Option<u128>,
    kubo_ttfb_p50_ms: Option<u128>,
    ttfb_p50_ratio: Option<f64>,
    rust_ttfb_p95_ms: Option<u128>,
    kubo_ttfb_p95_ms: Option<u128>,
    ttfb_p95_ratio: Option<f64>,
    rust_total_p50_ms: Option<u128>,
    kubo_total_p50_ms: Option<u128>,
    total_p50_ratio: Option<f64>,
    rust_total_p95_ms: Option<u128>,
    kubo_total_p95_ms: Option<u128>,
    total_p95_ratio: Option<f64>,
}

#[derive(Default)]
struct AssetComparisonSamples {
    kind: String,
    source: String,
    count: usize,
    pass_count: usize,
    ttfb_ms: Vec<u128>,
    total_ms: Vec<u128>,
}

impl ComparisonAsset {
    fn max_kubo_win_delta_ms(&self) -> u128 {
        self.meaningful_kubo_wins
            .iter()
            .map(|win| win.delta_ms)
            .max()
            .unwrap_or_default()
    }

    fn detect_meaningful_kubo_wins(&self) -> Vec<ComparisonKuboWin> {
        [
            (
                "asset_ttfb_p50",
                self.rust_ttfb_p50_ms,
                self.kubo_ttfb_p50_ms,
            ),
            (
                "asset_ttfb_p95",
                self.rust_ttfb_p95_ms,
                self.kubo_ttfb_p95_ms,
            ),
            (
                "asset_total_p50",
                self.rust_total_p50_ms,
                self.kubo_total_p50_ms,
            ),
            (
                "asset_total_p95",
                self.rust_total_p95_ms,
                self.kubo_total_p95_ms,
            ),
        ]
        .into_iter()
        .filter_map(|(metric, rust_ms, kubo_ms)| meaningful_kubo_win(metric, rust_ms?, kubo_ms?))
        .collect()
    }
}

impl ComparisonCase {
    fn from_reports(rust: &RunReport, kubo: &RunReport) -> Vec<Self> {
        let mut ids = Vec::new();
        for case in &rust.summary.cases {
            if !ids.contains(&case.id) {
                ids.push(case.id.clone());
            }
        }
        for case in &kubo.summary.cases {
            if !ids.contains(&case.id) {
                ids.push(case.id.clone());
            }
        }

        ids.into_iter()
            .filter_map(|id| {
                let rust_case = rust.summary.cases.iter().find(|case| case.id == id)?;
                let kubo_case = kubo.summary.cases.iter().find(|case| case.id == id)?;
                let rust_max_rss_kib = rust.summary.gateway_rss_kib.max;
                let kubo_max_rss_kib = kubo.summary.gateway_rss_kib.max;
                let rust_max_fd_count = rust.summary.gateway_fd_count.max;
                let kubo_max_fd_count = kubo.summary.gateway_fd_count.max;
                let rust_max_child_process_count = rust
                    .summary
                    .gateway_child_process_count
                    .max
                    .and_then(|value| usize::try_from(value).ok());
                let kubo_max_child_process_count = kubo
                    .summary
                    .gateway_child_process_count
                    .max
                    .and_then(|value| usize::try_from(value).ok());
                let rust_max_storage_bytes = rust.summary.gateway_storage_bytes.max;
                let kubo_max_storage_bytes = kubo.summary.gateway_storage_bytes.max;
                let kubo_setup_adjusted_root_ttfb = setup_adjusted_root_ttfb_ms(kubo, &id);
                let asset_comparisons = asset_comparisons_for_case(rust, kubo, &id);
                let mut case = Self {
                    id,
                    rust_pass_rate: rust_case.pass_rate,
                    kubo_pass_rate: kubo_case.pass_rate,
                    meaningful_kubo_wins: Vec::new(),
                    asset_comparisons,
                    rust_root_ttfb_p50_ms: rust_case.root_ttfb_ms.p50_ms,
                    kubo_root_ttfb_p50_ms: kubo_case.root_ttfb_ms.p50_ms,
                    root_ttfb_p50_ratio: ratio(
                        rust_case.root_ttfb_ms.p50_ms,
                        kubo_case.root_ttfb_ms.p50_ms,
                    ),
                    rust_root_ttfb_p95_ms: rust_case.root_ttfb_ms.p95_ms,
                    kubo_root_ttfb_p95_ms: kubo_case.root_ttfb_ms.p95_ms,
                    root_ttfb_p95_ratio: ratio(
                        rust_case.root_ttfb_ms.p95_ms,
                        kubo_case.root_ttfb_ms.p95_ms,
                    ),
                    rust_root_total_p50_ms: rust_case.root_total_ms.p50_ms,
                    kubo_root_total_p50_ms: kubo_case.root_total_ms.p50_ms,
                    root_total_p50_ratio: ratio(
                        rust_case.root_total_ms.p50_ms,
                        kubo_case.root_total_ms.p50_ms,
                    ),
                    rust_root_total_p95_ms: rust_case.root_total_ms.p95_ms,
                    kubo_root_total_p95_ms: kubo_case.root_total_ms.p95_ms,
                    root_total_p95_ratio: ratio(
                        rust_case.root_total_ms.p95_ms,
                        kubo_case.root_total_ms.p95_ms,
                    ),
                    kubo_setup_adjusted_root_ttfb_p50_ms: kubo_setup_adjusted_root_ttfb.p50_ms,
                    setup_adjusted_root_ttfb_p50_ratio: ratio(
                        rust_case.root_ttfb_ms.p50_ms,
                        kubo_setup_adjusted_root_ttfb.p50_ms,
                    ),
                    kubo_setup_adjusted_root_ttfb_p95_ms: kubo_setup_adjusted_root_ttfb.p95_ms,
                    setup_adjusted_root_ttfb_p95_ratio: ratio(
                        rust_case.root_ttfb_ms.p95_ms,
                        kubo_setup_adjusted_root_ttfb.p95_ms,
                    ),
                    rust_asset_ttfb_p50_ms: rust_case.asset_ttfb_ms.p50_ms,
                    kubo_asset_ttfb_p50_ms: kubo_case.asset_ttfb_ms.p50_ms,
                    asset_ttfb_p50_ratio: ratio(
                        rust_case.asset_ttfb_ms.p50_ms,
                        kubo_case.asset_ttfb_ms.p50_ms,
                    ),
                    rust_asset_ttfb_p95_ms: rust_case.asset_ttfb_ms.p95_ms,
                    kubo_asset_ttfb_p95_ms: kubo_case.asset_ttfb_ms.p95_ms,
                    asset_ttfb_p95_ratio: ratio(
                        rust_case.asset_ttfb_ms.p95_ms,
                        kubo_case.asset_ttfb_ms.p95_ms,
                    ),
                    rust_asset_total_p50_ms: rust_case.asset_total_ms.p50_ms,
                    kubo_asset_total_p50_ms: kubo_case.asset_total_ms.p50_ms,
                    asset_total_p50_ratio: ratio(
                        rust_case.asset_total_ms.p50_ms,
                        kubo_case.asset_total_ms.p50_ms,
                    ),
                    rust_asset_total_p95_ms: rust_case.asset_total_ms.p95_ms,
                    kubo_asset_total_p95_ms: kubo_case.asset_total_ms.p95_ms,
                    asset_total_p95_ratio: ratio(
                        rust_case.asset_total_ms.p95_ms,
                        kubo_case.asset_total_ms.p95_ms,
                    ),
                    rust_max_rss_kib,
                    kubo_max_rss_kib,
                    rss_ratio: ratio_u64(rust_max_rss_kib, kubo_max_rss_kib),
                    rust_max_fd_count,
                    kubo_max_fd_count,
                    fd_ratio: ratio_u64(rust_max_fd_count, kubo_max_fd_count),
                    rust_max_child_process_count,
                    kubo_max_child_process_count,
                    rust_max_storage_bytes,
                    kubo_max_storage_bytes,
                    storage_ratio: ratio_u64(rust_max_storage_bytes, kubo_max_storage_bytes),
                };
                case.meaningful_kubo_wins = case.detect_meaningful_kubo_wins();
                Some(case)
            })
            .collect()
    }

    fn detect_meaningful_kubo_wins(&self) -> Vec<ComparisonKuboWin> {
        [
            (
                "root_ttfb_p50",
                self.rust_root_ttfb_p50_ms,
                self.kubo_root_ttfb_p50_ms,
            ),
            (
                "root_ttfb_p95",
                self.rust_root_ttfb_p95_ms,
                self.kubo_root_ttfb_p95_ms,
            ),
            (
                "root_total_p50",
                self.rust_root_total_p50_ms,
                self.kubo_root_total_p50_ms,
            ),
            (
                "root_total_p95",
                self.rust_root_total_p95_ms,
                self.kubo_root_total_p95_ms,
            ),
            (
                "asset_ttfb_p50",
                self.rust_asset_ttfb_p50_ms,
                self.kubo_asset_ttfb_p50_ms,
            ),
            (
                "asset_ttfb_p95",
                self.rust_asset_ttfb_p95_ms,
                self.kubo_asset_ttfb_p95_ms,
            ),
            (
                "asset_total_p50",
                self.rust_asset_total_p50_ms,
                self.kubo_asset_total_p50_ms,
            ),
            (
                "asset_total_p95",
                self.rust_asset_total_p95_ms,
                self.kubo_asset_total_p95_ms,
            ),
        ]
        .into_iter()
        .filter_map(|(metric, rust_ms, kubo_ms)| meaningful_kubo_win(metric, rust_ms?, kubo_ms?))
        .collect()
    }
}

fn asset_comparisons_for_case(
    rust: &RunReport,
    kubo: &RunReport,
    id: &str,
) -> Vec<ComparisonAsset> {
    let rust_assets = asset_samples_for_case(rust, id);
    let kubo_assets = asset_samples_for_case(kubo, id);
    let rust_trace_paths = rust
        .trace_summary
        .as_ref()
        .map(trace_request_paths_by_path)
        .unwrap_or_default();
    let mut paths = rust_assets.keys().cloned().collect::<Vec<_>>();
    for path in kubo_assets.keys() {
        if !paths.contains(path) {
            paths.push(path.clone());
        }
    }

    let mut comparisons = paths
        .into_iter()
        .map(|path| {
            let rust = rust_assets.get(&path);
            let kubo = kubo_assets.get(&path);
            let rust_trace = rust_trace_paths.get(&path).cloned();
            let rust_ttfb = rust
                .map(|samples| LatencySummary::from_values(samples.ttfb_ms.clone()))
                .unwrap_or_default();
            let kubo_ttfb = kubo
                .map(|samples| LatencySummary::from_values(samples.ttfb_ms.clone()))
                .unwrap_or_default();
            let rust_total = rust
                .map(|samples| LatencySummary::from_values(samples.total_ms.clone()))
                .unwrap_or_default();
            let kubo_total = kubo
                .map(|samples| LatencySummary::from_values(samples.total_ms.clone()))
                .unwrap_or_default();
            let mut comparison = ComparisonAsset {
                path,
                kind: rust
                    .or(kubo)
                    .map(|samples| samples.kind.clone())
                    .unwrap_or_default(),
                source: rust
                    .or(kubo)
                    .map(|samples| samples.source.clone())
                    .unwrap_or_default(),
                rust_trace,
                rust_count: rust.map(|samples| samples.count).unwrap_or_default(),
                kubo_count: kubo.map(|samples| samples.count).unwrap_or_default(),
                rust_pass_count: rust.map(|samples| samples.pass_count).unwrap_or_default(),
                kubo_pass_count: kubo.map(|samples| samples.pass_count).unwrap_or_default(),
                meaningful_kubo_wins: Vec::new(),
                rust_ttfb_p50_ms: rust_ttfb.p50_ms,
                kubo_ttfb_p50_ms: kubo_ttfb.p50_ms,
                ttfb_p50_ratio: ratio(rust_ttfb.p50_ms, kubo_ttfb.p50_ms),
                rust_ttfb_p95_ms: rust_ttfb.p95_ms,
                kubo_ttfb_p95_ms: kubo_ttfb.p95_ms,
                ttfb_p95_ratio: ratio(rust_ttfb.p95_ms, kubo_ttfb.p95_ms),
                rust_total_p50_ms: rust_total.p50_ms,
                kubo_total_p50_ms: kubo_total.p50_ms,
                total_p50_ratio: ratio(rust_total.p50_ms, kubo_total.p50_ms),
                rust_total_p95_ms: rust_total.p95_ms,
                kubo_total_p95_ms: kubo_total.p95_ms,
                total_p95_ratio: ratio(rust_total.p95_ms, kubo_total.p95_ms),
            };
            comparison.meaningful_kubo_wins = comparison.detect_meaningful_kubo_wins();
            comparison
        })
        .collect::<Vec<_>>();
    comparisons.sort_by(|left, right| {
        right
            .max_kubo_win_delta_ms()
            .cmp(&left.max_kubo_win_delta_ms())
            .then_with(|| left.path.cmp(&right.path))
    });
    comparisons
}

fn trace_request_paths_by_path(
    trace: &TraceSummary,
) -> BTreeMap<String, TraceRequestPathAggregate> {
    trace
        .request_paths
        .iter()
        .cloned()
        .map(|request| (request.path.clone(), request))
        .collect()
}

fn asset_samples_for_case(
    report: &RunReport,
    id: &str,
) -> BTreeMap<String, AssetComparisonSamples> {
    let mut samples = BTreeMap::<String, AssetComparisonSamples>::new();
    for run in report
        .runs
        .iter()
        .filter(|run| run.phase == RunPhase::Measured)
    {
        let Some(result) = run.results.iter().find(|result| result.id == id) else {
            continue;
        };
        for asset in &result.assets {
            let sample = samples.entry(asset_comparison_path(asset)).or_default();
            if sample.kind.is_empty() {
                sample.kind = asset.kind.to_string();
            }
            if sample.source.is_empty() {
                sample.source = asset.source.clone();
            }
            sample.count += 1;
            if asset.passed {
                sample.pass_count += 1;
            }
            sample.ttfb_ms.push(asset.ttfb_ms);
            sample.total_ms.push(asset.total_ms);
        }
    }
    samples
}

fn asset_comparison_path(asset: &AssetResult) -> String {
    Url::parse(&asset.url)
        .ok()
        .map(|url| {
            let mut path = url.path().to_string();
            if let Some(query) = url.query() {
                path.push('?');
                path.push_str(query);
            }
            path
        })
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| asset.url.clone())
}

fn meaningful_kubo_win(metric: &str, rust_ms: u128, kubo_ms: u128) -> Option<ComparisonKuboWin> {
    if rust_ms <= kubo_ms || kubo_ms == 0 {
        return None;
    }
    let delta_ms = rust_ms - kubo_ms;
    let ratio = rust_ms as f64 / kubo_ms as f64;
    (delta_ms >= MEANINGFUL_KUBO_WIN_MIN_DELTA_MS && ratio >= MEANINGFUL_KUBO_WIN_MIN_RATIO).then(
        || ComparisonKuboWin {
            metric: metric.to_string(),
            rust_ms,
            kubo_ms,
            delta_ms,
            ratio,
        },
    )
}

fn setup_adjusted_root_ttfb_ms(report: &RunReport, id: &str) -> LatencySummary {
    let values = report
        .runs
        .iter()
        .filter(|run| run.phase == RunPhase::Measured)
        .filter_map(|run| {
            let setup_ms = run.bitswap_seed_connect_elapsed_ms?;
            let result = run.results.iter().find(|result| result.id == id)?;
            Some(result.ttfb_ms.saturating_add(setup_ms))
        })
        .collect::<Vec<_>>();
    LatencySummary::from_values(values)
}

fn ratio(left: Option<u128>, right: Option<u128>) -> Option<f64> {
    let left = left?;
    let right = right?;
    (right > 0).then_some(left as f64 / right as f64)
}

fn ratio_u64(left: Option<u64>, right: Option<u64>) -> Option<f64> {
    let left = left?;
    let right = right?;
    (right > 0).then_some(left as f64 / right as f64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunPhase {
    Warmup,
    Measured,
}

#[derive(Debug, Serialize)]
struct RepeatSummary {
    measured_runs: usize,
    pass_count: usize,
    fail_count: usize,
    pass_rate: f64,
    run_total_ms: LatencySummary,
    gateway_rss_kib: ResourceSummary,
    gateway_fd_count: ResourceSummary,
    gateway_child_process_count: ResourceSummary,
    gateway_storage_bytes: ResourceSummary,
    bitswap_seed_connect_ms: LatencySummary,
    kubo_bitswap: KuboBitswapSummary,
    cases: Vec<CaseAggregate>,
}

impl RepeatSummary {
    fn from_runs(runs: &[RunResult]) -> Self {
        let measured = runs
            .iter()
            .filter(|run| run.phase == RunPhase::Measured)
            .collect::<Vec<_>>();
        let pass_count = measured.iter().filter(|run| run.passed).count();
        let fail_count = measured.len().saturating_sub(pass_count);
        let pass_rate = rate(pass_count, measured.len());
        let run_total_ms = LatencySummary::from_values(
            measured
                .iter()
                .map(|run| run.elapsed_ms)
                .collect::<Vec<_>>(),
        );
        let gateway_rss_kib = ResourceSummary::from_values(
            measured
                .iter()
                .filter_map(|run| run.gateway_rss_kib)
                .collect::<Vec<_>>(),
        );
        let gateway_fd_count = ResourceSummary::from_values(
            measured
                .iter()
                .filter_map(|run| run.gateway_fd_count)
                .filter_map(|value| u64::try_from(value).ok())
                .collect::<Vec<_>>(),
        );
        let gateway_child_process_count = ResourceSummary::from_values(
            measured
                .iter()
                .filter_map(|run| run.gateway_child_process_count)
                .filter_map(|value| u64::try_from(value).ok())
                .collect::<Vec<_>>(),
        );
        let gateway_storage_bytes = ResourceSummary::from_values(
            measured
                .iter()
                .filter_map(|run| run.gateway_storage_bytes)
                .collect::<Vec<_>>(),
        );
        let bitswap_seed_connect_ms = LatencySummary::from_values(
            measured
                .iter()
                .filter_map(|run| run.bitswap_seed_connect_elapsed_ms)
                .collect::<Vec<_>>(),
        );
        let kubo_bitswap = KuboBitswapSummary::from_runs(&measured);

        let mut case_ids = Vec::new();
        for run in &measured {
            for result in &run.results {
                if !case_ids.contains(&result.id) {
                    case_ids.push(result.id.clone());
                }
            }
        }

        let cases = case_ids
            .into_iter()
            .map(|id| CaseAggregate::from_runs(&id, &measured))
            .collect();

        Self {
            measured_runs: measured.len(),
            pass_count,
            fail_count,
            pass_rate,
            run_total_ms,
            gateway_rss_kib,
            gateway_fd_count,
            gateway_child_process_count,
            gateway_storage_bytes,
            bitswap_seed_connect_ms,
            kubo_bitswap,
            cases,
        }
    }

    fn has_resource_metrics(&self) -> bool {
        self.run_total_ms.count > 0
            || self.gateway_rss_kib.count > 0
            || self.gateway_fd_count.count > 0
            || self.gateway_child_process_count.count > 0
            || self.gateway_storage_bytes.count > 0
            || self.kubo_bitswap.has_values()
    }
}

#[derive(Debug, Serialize)]
struct KuboBitswapSummary {
    blocks_received: ResourceSummary,
    data_received: ResourceSummary,
    blocks_sent: ResourceSummary,
    data_sent: ResourceSummary,
    dup_blocks_received: ResourceSummary,
    dup_data_received: ResourceSummary,
    messages_received: ResourceSummary,
    wantlist_len: ResourceSummary,
    peers_len: ResourceSummary,
}

impl KuboBitswapSummary {
    fn from_runs(runs: &[&RunResult]) -> Self {
        Self {
            blocks_received: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.blocks_received)
                    .collect(),
            ),
            data_received: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.data_received)
                    .collect(),
            ),
            blocks_sent: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.blocks_sent)
                    .collect(),
            ),
            data_sent: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.data_sent)
                    .collect(),
            ),
            dup_blocks_received: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.dup_blocks_received)
                    .collect(),
            ),
            dup_data_received: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.dup_data_received)
                    .collect(),
            ),
            messages_received: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.messages_received)
                    .collect(),
            ),
            wantlist_len: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.wantlist_len)
                    .collect(),
            ),
            peers_len: ResourceSummary::from_values(
                runs.iter()
                    .filter_map(|run| run.kubo_bitswap_stats.as_ref()?.peers_len)
                    .collect(),
            ),
        }
    }

    fn has_values(&self) -> bool {
        self.blocks_received.count > 0
            || self.data_received.count > 0
            || self.blocks_sent.count > 0
            || self.data_sent.count > 0
            || self.dup_blocks_received.count > 0
            || self.dup_data_received.count > 0
            || self.messages_received.count > 0
            || self.wantlist_len.count > 0
            || self.peers_len.count > 0
    }
}

#[derive(Debug, Serialize)]
struct CaseAggregate {
    id: String,
    run_count: usize,
    pass_count: usize,
    fail_count: usize,
    pass_rate: f64,
    root_ttfb_ms: LatencySummary,
    root_total_ms: LatencySummary,
    root_stream_first_byte_ms: LatencySummary,
    root_stream_chunks: ResourceSummary,
    root_stream_max_buffered_bytes: ResourceSummary,
    asset_ttfb_ms: LatencySummary,
    asset_total_ms: LatencySummary,
    asset_stream_first_byte_ms: LatencySummary,
    asset_stream_chunks: ResourceSummary,
    asset_stream_max_buffered_bytes: ResourceSummary,
    root_revalidation_attempts: usize,
    root_revalidation_passed: usize,
    root_revalidation_failed: usize,
    root_revalidation_ttfb_ms: LatencySummary,
    asset_revalidation_attempts: usize,
    asset_revalidation_passed: usize,
    asset_revalidation_failed: usize,
    asset_revalidation_ttfb_ms: LatencySummary,
    asset_kind_failures: Vec<AssetKindFailure>,
    failure_groups: Vec<FailureGroup>,
}

impl CaseAggregate {
    fn from_runs(id: &str, runs: &[&RunResult]) -> Self {
        let mut run_count = 0usize;
        let mut pass_count = 0usize;
        let mut root_ttfb = Vec::new();
        let mut root_total = Vec::new();
        let mut root_stream_first_byte = Vec::new();
        let mut root_stream_chunks = Vec::new();
        let mut root_stream_max_buffered_bytes = Vec::new();
        let mut asset_ttfb = Vec::new();
        let mut asset_total = Vec::new();
        let mut asset_stream_first_byte = Vec::new();
        let mut asset_stream_chunks = Vec::new();
        let mut asset_stream_max_buffered_bytes = Vec::new();
        let mut root_revalidation_attempts = 0usize;
        let mut root_revalidation_passed = 0usize;
        let mut root_revalidation_ttfb = Vec::new();
        let mut asset_revalidation_attempts = 0usize;
        let mut asset_revalidation_passed = 0usize;
        let mut asset_revalidation_ttfb = Vec::new();
        let mut kind_failures = BTreeMap::<String, usize>::new();
        let mut failure_groups = BTreeMap::<String, FailureGroupBuilder>::new();

        for run in runs {
            let Some(result) = run.results.iter().find(|result| result.id == id) else {
                continue;
            };
            run_count += 1;
            if result.passed {
                pass_count += 1;
            }
            root_ttfb.push(result.ttfb_ms);
            root_total.push(result.total_ms);
            if let Some(first_byte_ms) = result.stream.first_byte_ms {
                root_stream_first_byte.push(first_byte_ms);
            }
            if let Ok(chunk_count) = u64::try_from(result.stream.chunk_count) {
                root_stream_chunks.push(chunk_count);
            }
            if let Ok(max_buffered) = u64::try_from(result.stream.max_buffered_bytes) {
                root_stream_max_buffered_bytes.push(max_buffered);
            }
            if let Some(revalidation) = &result.revalidation {
                root_revalidation_attempts += 1;
                if revalidation.passed {
                    root_revalidation_passed += 1;
                }
                root_revalidation_ttfb.push(revalidation.ttfb_ms);
            }
            if !result.passed {
                for failure in &result.failures {
                    push_failure_group(
                        &mut failure_groups,
                        format!("case failure: {failure}"),
                        result.url.clone(),
                    );
                }
            }
            for asset in &result.assets {
                asset_ttfb.push(asset.ttfb_ms);
                asset_total.push(asset.total_ms);
                if let Some(first_byte_ms) = asset.stream.first_byte_ms {
                    asset_stream_first_byte.push(first_byte_ms);
                }
                if let Ok(chunk_count) = u64::try_from(asset.stream.chunk_count) {
                    asset_stream_chunks.push(chunk_count);
                }
                if let Ok(max_buffered) = u64::try_from(asset.stream.max_buffered_bytes) {
                    asset_stream_max_buffered_bytes.push(max_buffered);
                }
                if let Some(revalidation) = &asset.revalidation {
                    asset_revalidation_attempts += 1;
                    if revalidation.passed {
                        asset_revalidation_passed += 1;
                    }
                    asset_revalidation_ttfb.push(revalidation.ttfb_ms);
                }
                if asset.passed {
                    continue;
                }
                *kind_failures.entry(asset.kind.to_string()).or_default() += 1;
                let status = asset
                    .status
                    .map(|status| format!("status={status}"))
                    .unwrap_or_else(|| "status=request_error".to_string());
                let detail = if asset.failures.is_empty() {
                    "no detail".to_string()
                } else {
                    asset.failures.join("; ")
                };
                push_failure_group(
                    &mut failure_groups,
                    format!("asset kind={} {status}: {detail}", asset.kind),
                    asset.url.clone(),
                );
            }
        }

        let fail_count = run_count.saturating_sub(pass_count);
        let mut asset_kind_failures = kind_failures
            .into_iter()
            .map(|(kind, count)| AssetKindFailure { kind, count })
            .collect::<Vec<_>>();
        asset_kind_failures.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.kind.cmp(&right.kind))
        });
        let mut failure_groups = failure_groups
            .into_values()
            .map(FailureGroupBuilder::finish)
            .collect::<Vec<_>>();
        failure_groups.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.key.cmp(&right.key))
        });

        Self {
            id: id.to_string(),
            run_count,
            pass_count,
            fail_count,
            pass_rate: rate(pass_count, run_count),
            root_ttfb_ms: LatencySummary::from_values(root_ttfb),
            root_total_ms: LatencySummary::from_values(root_total),
            root_stream_first_byte_ms: LatencySummary::from_values(root_stream_first_byte),
            root_stream_chunks: ResourceSummary::from_values(root_stream_chunks),
            root_stream_max_buffered_bytes: ResourceSummary::from_values(
                root_stream_max_buffered_bytes,
            ),
            asset_ttfb_ms: LatencySummary::from_values(asset_ttfb),
            asset_total_ms: LatencySummary::from_values(asset_total),
            asset_stream_first_byte_ms: LatencySummary::from_values(asset_stream_first_byte),
            asset_stream_chunks: ResourceSummary::from_values(asset_stream_chunks),
            asset_stream_max_buffered_bytes: ResourceSummary::from_values(
                asset_stream_max_buffered_bytes,
            ),
            root_revalidation_attempts,
            root_revalidation_passed,
            root_revalidation_failed: root_revalidation_attempts
                .saturating_sub(root_revalidation_passed),
            root_revalidation_ttfb_ms: LatencySummary::from_values(root_revalidation_ttfb),
            asset_revalidation_attempts,
            asset_revalidation_passed,
            asset_revalidation_failed: asset_revalidation_attempts
                .saturating_sub(asset_revalidation_passed),
            asset_revalidation_ttfb_ms: LatencySummary::from_values(asset_revalidation_ttfb),
            asset_kind_failures,
            failure_groups,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct LatencySummary {
    count: usize,
    p50_ms: Option<u128>,
    p90_ms: Option<u128>,
    p95_ms: Option<u128>,
    max_ms: Option<u128>,
}

impl LatencySummary {
    fn from_values(mut values: Vec<u128>) -> Self {
        values.sort_unstable();
        Self {
            count: values.len(),
            p50_ms: percentile(&values, 50),
            p90_ms: percentile(&values, 90),
            p95_ms: percentile(&values, 95),
            max_ms: values.last().copied(),
        }
    }
}

impl std::fmt::Display for LatencySummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.count == 0 {
            return formatter.write_str("n/a");
        }
        write!(
            formatter,
            "p50={}ms p90={}ms p95={}ms max={}ms",
            self.p50_ms.unwrap_or_default(),
            self.p90_ms.unwrap_or_default(),
            self.p95_ms.unwrap_or_default(),
            self.max_ms.unwrap_or_default()
        )
    }
}

#[derive(Debug, Serialize)]
struct ResourceSummary {
    count: usize,
    p50: Option<u64>,
    p90: Option<u64>,
    p95: Option<u64>,
    max: Option<u64>,
}

impl ResourceSummary {
    fn from_values(mut values: Vec<u64>) -> Self {
        values.sort_unstable();
        Self {
            count: values.len(),
            p50: percentile_u64(&values, 50),
            p90: percentile_u64(&values, 90),
            p95: percentile_u64(&values, 95),
            max: values.last().copied(),
        }
    }
}

impl std::fmt::Display for ResourceSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.count == 0 {
            return formatter.write_str("n/a");
        }
        write!(
            formatter,
            "p50={} p90={} p95={} max={}",
            self.p50.unwrap_or_default(),
            self.p90.unwrap_or_default(),
            self.p95.unwrap_or_default(),
            self.max.unwrap_or_default()
        )
    }
}

#[derive(Debug, Serialize)]
struct TraceSummary {
    line_count: usize,
    event_count: usize,
    event_phases: Vec<TraceValueCount>,
    phases: Vec<TracePhaseAggregate>,
    progress_phases: Vec<TraceValueCount>,
    slow_events: Vec<TraceSlowEvent>,
    block_sources: Vec<TraceValueCount>,
    block_fetch_source_latencies: Vec<TraceSourceLatencyAggregate>,
    block_store: TraceBlockStoreAggregate,
    block_range_batch_fetches: TraceBlockRangeBatchFetchAggregate,
    provider_retries: TraceProviderRetryAggregate,
    delegated_provider_lookup: TraceDelegatedProviderLookupAggregate,
    delegated_provider_lookup_by_endpoint: Vec<TraceDelegatedProviderEndpointAggregate>,
    dht_provider_lookup: TraceDhtProviderLookupAggregate,
    provider_diversity_low: TraceProviderDiversityLowAggregate,
    request_statuses: Vec<TraceValueCount>,
    gateway_limiter_denials: usize,
    gateway_limiter: TraceGatewayLimiterAggregate,
    gateway_request_elapsed_ms: LatencySummary,
    gateway_small_body_cache: TraceGatewaySmallBodyCacheAggregate,
    gateway_direct_body: TraceGatewayDirectBodyAggregate,
    gateway_stream_body: TraceGatewayStreamBodyAggregate,
    http_provider_races: TraceHttpProviderRaceAggregate,
    http_provider_fetches: TraceHttpProviderFetchAggregate,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
    bitswap_source_request_modes: Vec<TraceValueCount>,
    bitswap_source_candidate_indexes: Vec<TraceValueCount>,
    bitswap_source_addr_indexes: Vec<TraceValueCount>,
    bitswap_source_addr_families: Vec<TraceValueCount>,
    bitswap_source_addr_match_statuses: Vec<TraceValueCount>,
    bitswap_deliveries: Vec<TraceValueCount>,
    bitswap_batches: TraceBitswapBatchAggregate,
    bitswap_extra_blocks: TraceBitswapExtraBlockAggregate,
    bitswap_incoming_batches: TraceBitswapIncomingBatchAggregate,
    bitswap_peer_fetches: Vec<TracePeerAggregate>,
    bitswap_session: TraceBitswapSessionAggregate,
    bitswap_peer_attempts: TraceBitswapPeerAttemptAggregate,
    bitswap_want_have_probes: TraceBitswapWantHaveProbeAggregate,
    bitswap_dial_plans: TraceBitswapDialPlanAggregate,
    bitswap_incoming_blocks: TraceBitswapIncomingBlockAggregate,
    bitswap_incoming_reads: TraceBitswapIncomingReadAggregate,
    bitswap_timeout_recovery: TraceBitswapTimeoutRecoveryAggregate,
    trace_errors: Vec<TraceValueCount>,
    bitswap_addr_mix: Vec<TraceValueCount>,
    bitswap_provider_quality: TraceBitswapProviderQualityAggregate,
    bitswap_connection_established: TraceBitswapConnectionEstablishedAggregate,
    bitswap_connection_transports: Vec<TraceValueCount>,
    bitswap_connection_errors: TraceBitswapConnectionErrorAggregate,
    bitswap_connection_error_addr_families: Vec<TraceValueCount>,
    bitswap_connection_backoff: TraceBitswapConnectionBackoffAggregate,
    bitswap_dial_rejections: TraceBitswapDialRejectedAggregate,
    bitswap_dial_rejected_transports: Vec<TraceValueCount>,
    bitswap_dns_expansion: TraceBitswapDnsExpansionAggregate,
    request_classifications: Vec<TraceValueCount>,
    request_classification_latencies: Vec<TraceRequestClassificationAggregate>,
    request_paths: Vec<TraceRequestPathAggregate>,
    slow_cids: Vec<TraceCidAggregate>,
    slow_requests: Vec<TraceRequestAggregate>,
    progress_request_groups: Vec<TraceProgressRequestGroupAggregate>,
}

#[derive(Clone, Debug, Serialize)]
struct TracePhaseAggregate {
    phase: String,
    count: usize,
    total_ms: u128,
    elapsed_ms: LatencySummary,
}

#[derive(Debug, Serialize)]
struct TraceSlowEvent {
    phase: String,
    elapsed_ms: u128,
    details: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
struct TraceValueCount {
    value: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct TraceRequestClassificationAggregate {
    classification: String,
    request_count: usize,
    request_elapsed_ms: LatencySummary,
    max_event_ms: LatencySummary,
    statuses: Vec<TraceValueCount>,
    paths: Vec<TraceValueCount>,
    top_level_paths: Vec<TraceValueCount>,
    bitswap_source_candidate_indexes: Vec<TraceValueCount>,
    bitswap_source_request_modes: Vec<TraceValueCount>,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
}

#[derive(Clone, Debug, Serialize)]
struct TraceRequestPathAggregate {
    path: String,
    request_count: usize,
    request_elapsed_ms: LatencySummary,
    max_event_ms: u128,
    statuses: Vec<TraceValueCount>,
    classifications: Vec<TraceValueCount>,
    block_sources: Vec<TraceValueCount>,
    http_provider_fetches: usize,
    http_provider_fetch_successes: usize,
    http_provider_fetch_failures: usize,
    http_provider_fetch_max_ms: u128,
    http_provider_fetch_response_bytes: u128,
    http_provider_fetch_first_chunk_events: usize,
    http_provider_fetch_headers_max_ms: u128,
    http_provider_fetch_first_chunk_max_ms: u128,
    http_provider_fetch_body_max_ms: u128,
    http_provider_fetch_providers: Vec<TraceValueCount>,
    http_provider_fetch_error_classes: Vec<TraceValueCount>,
    bitswap_fetches: usize,
    bitswap_fetch_max_ms: u128,
    bitswap_fetch_bytes: u128,
    bitswap_source_candidate_indexes: Vec<TraceValueCount>,
    bitswap_source_request_modes: Vec<TraceValueCount>,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
    provider_diversity_low_events: usize,
    provider_diversity_low_failures: usize,
    provider_diversity_low_max_provider_count: u128,
    provider_diversity_low_max_bitswap_provider_count: u128,
    provider_diversity_low_max_timeout_ms: u128,
    dht_provider_lookup_events: usize,
    dht_provider_lookup_failures: usize,
    dht_provider_lookup_providers: u128,
    dht_provider_lookup_max_elapsed_ms: u128,
    dht_provider_lookup_max_timeout_ms: u128,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
    phase_latencies: Vec<TraceRequestPathPhaseAggregate>,
}

#[derive(Clone, Debug, Serialize)]
struct TraceRequestPathPhaseAggregate {
    phase: String,
    count: usize,
    total_ms: u128,
    max_ms: u128,
}

#[derive(Debug, Serialize)]
struct TraceSourceLatencyAggregate {
    source: String,
    count: usize,
    total_ms: u128,
    elapsed_ms: LatencySummary,
}

#[derive(Debug, Default, Serialize)]
struct TraceGatewayLimiterAggregate {
    events: usize,
    acquired: usize,
    denied: usize,
    elapsed_ms: LatencySummary,
    denied_elapsed_ms: LatencySummary,
    max_timeout_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBlockStoreAggregate {
    events: usize,
    hits: usize,
    misses: usize,
    rechecks: usize,
    recheck_hits: usize,
    recheck_misses: usize,
    puts: usize,
    put_bytes: u128,
    put_failures: usize,
    put_total_ms: u128,
    put_max_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBlockRangeBatchFetchAggregate {
    events: usize,
    bytes: u128,
    max_range_len: u128,
    max_range_count: u128,
    max_uncached_range_count: u128,
    elapsed_ms: LatencySummary,
    sources: Vec<TraceValueCount>,
    #[serde(skip)]
    elapsed_values: Vec<u128>,
    #[serde(skip)]
    source_counts: BTreeMap<String, usize>,
}

impl TraceBlockRangeBatchFetchAggregate {
    fn record(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.events += 1;
        let range_len = value
            .get("range_len")
            .and_then(json_u128)
            .or_else(|| {
                let start = value.get("range_start").and_then(json_u128)?;
                let end = value.get("range_end").and_then(json_u128)?;
                Some(if start <= end {
                    end.saturating_sub(start).saturating_add(1)
                } else {
                    0
                })
            })
            .unwrap_or_default();
        self.bytes += range_len;
        self.max_range_len = self.max_range_len.max(range_len);
        self.max_range_count = self.max_range_count.max(
            value
                .get("range_count")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_uncached_range_count = self.max_uncached_range_count.max(
            value
                .get("uncached_range_count")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        if let Some(elapsed_ms) = elapsed_ms {
            self.elapsed_values.push(elapsed_ms);
        }
        if let Some(source) = json_detail_string(value.get("source")) {
            *self.source_counts.entry(source).or_default() += 1;
        }
    }

    fn finish(&mut self) {
        self.elapsed_ms = LatencySummary::from_values(std::mem::take(&mut self.elapsed_values));
        self.sources = sorted_trace_counts(std::mem::take(&mut self.source_counts));
    }

    fn has_events(&self) -> bool {
        self.events > 0
    }
}

#[derive(Debug, Serialize)]
struct TraceHttpProviderFetchAggregate {
    events: usize,
    successes: usize,
    failures: usize,
    bytes: u128,
    response_bytes: u128,
    first_chunk_events: usize,
    max_response_headers_elapsed_ms: u128,
    max_response_first_chunk_elapsed_ms: u128,
    max_response_body_elapsed_ms: u128,
    elapsed_ms: LatencySummary,
    providers: Vec<TraceValueCount>,
    provider_milestones: Vec<TraceHttpProviderMilestoneAggregate>,
    error_classes: Vec<TraceValueCount>,
}

#[derive(Debug, Default, Serialize)]
struct TraceHttpProviderRaceAggregate {
    events: usize,
    provider_count_total: u128,
    max_provider_count: u128,
    max_race_width: u128,
    single_provider_events: usize,
    multi_provider_events: usize,
    above_race_width_events: usize,
    scored_events: usize,
    scored_provider_count_total: u128,
    max_scored_provider_count: u128,
    hedges: usize,
    self_hedges: usize,
    max_self_hedge_timeout_ms: u128,
    bitswap_hedges: usize,
    max_bitswap_hedge_timeout_ms: u128,
    bitswap_hedge_results: usize,
    bitswap_hedge_result_elapsed_ms: LatencySummary,
    bitswap_hedge_result_sources: Vec<TraceValueCount>,
    bitswap_hedge_skips: usize,
    bitswap_hedge_skip_reasons: Vec<TraceValueCount>,
    max_hedge_pending_count: u128,
    max_hedge_remaining_provider_count: u128,
    result_events: usize,
    result_successes: usize,
    result_failures: usize,
    winner_initial_width_events: usize,
    winner_late_events: usize,
    winner_rank1_events: usize,
    winner_rank2_events: usize,
    winner_rank3_plus_events: usize,
    max_winner_provider_rank: u128,
    winner_original_rank1_events: usize,
    winner_original_rank2_events: usize,
    winner_original_rank3_plus_events: usize,
    max_winner_original_provider_rank: u128,
    max_winner_attempt_index: u128,
    self_hedge_winner_initial_events: usize,
    self_hedge_winner_hedged_events: usize,
    self_hedge_winner_unknown_events: usize,
    self_hedge_skips: usize,
    self_hedge_skip_reasons: Vec<TraceValueCount>,
    candidate_cancellations: usize,
    candidate_cancelled_stages: Vec<TraceValueCount>,
    candidate_cancelled_providers: Vec<TraceValueCount>,
    candidate_cancelled_attempts: Vec<TraceValueCount>,
    self_hedge_fired_result_events: usize,
    self_hedge_fired_winner_initial_events: usize,
    self_hedge_fired_winner_hedged_events: usize,
    self_hedge_fired_winner_unknown_events: usize,
    winner_scored_events: usize,
    winner_score_elapsed_ms: LatencySummary,
    max_attempted_provider_count: u128,
    max_result_elapsed_ms: u128,
    single_provider_result_successes: usize,
    single_provider_result_failures: usize,
    single_provider_success_elapsed_ms: LatencySummary,
    multi_provider_success_elapsed_ms: LatencySummary,
    single_provider_winners: Vec<TraceHttpProviderRaceWinnerProviderAggregate>,
    #[serde(skip)]
    single_provider_success_elapsed_values: Vec<u128>,
    #[serde(skip)]
    multi_provider_success_elapsed_values: Vec<u128>,
    #[serde(skip)]
    winner_score_elapsed_values: Vec<u128>,
    #[serde(skip)]
    single_provider_winner_builders: BTreeMap<String, TraceHttpProviderRaceWinnerProviderBuilder>,
    #[serde(skip)]
    self_hedge_skip_reason_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    candidate_cancelled_stage_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    candidate_cancelled_provider_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    candidate_cancelled_attempt_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    bitswap_hedge_result_elapsed_values: Vec<u128>,
    #[serde(skip)]
    bitswap_hedge_result_source_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    bitswap_hedge_skip_reason_counts: BTreeMap<String, usize>,
}

impl TraceHttpProviderRaceAggregate {
    fn record_race(&mut self, value: &serde_json::Value) {
        self.events += 1;
        let provider_count = trace_count_field(value, "provider_count");
        let race_width = trace_count_field(value, "race_width");
        let scored_provider_count = trace_count_field(value, "scored_provider_count");
        self.provider_count_total += provider_count;
        self.max_provider_count = self.max_provider_count.max(provider_count);
        self.max_race_width = self.max_race_width.max(race_width);
        if provider_count == 1 {
            self.single_provider_events += 1;
        } else if provider_count > 1 {
            self.multi_provider_events += 1;
        }
        if race_width > 0 && provider_count > race_width {
            self.above_race_width_events += 1;
        }
        if scored_provider_count > 0 {
            self.scored_events += 1;
            self.scored_provider_count_total += scored_provider_count;
            self.max_scored_provider_count =
                self.max_scored_provider_count.max(scored_provider_count);
        }
    }

    fn record_hedge(&mut self, value: &serde_json::Value) {
        self.hedges += 1;
        self.max_hedge_pending_count = self
            .max_hedge_pending_count
            .max(trace_count_field(value, "pending_count"));
        self.max_hedge_remaining_provider_count = self
            .max_hedge_remaining_provider_count
            .max(trace_count_field(value, "remaining_provider_count"));
    }

    fn record_self_hedge(&mut self, value: &serde_json::Value) {
        self.self_hedges += 1;
        self.max_self_hedge_timeout_ms = self
            .max_self_hedge_timeout_ms
            .max(trace_count_field(value, "timeout_ms"));
    }

    fn record_self_hedge_skip(&mut self, value: &serde_json::Value) {
        self.self_hedge_skips += 1;
        let reason = json_detail_string(value.get("reason")).unwrap_or_else(|| "unknown".into());
        *self
            .self_hedge_skip_reason_counts
            .entry(reason)
            .or_default() += 1;
    }

    fn record_candidate_cancelled(&mut self, value: &serde_json::Value) {
        self.candidate_cancellations += 1;
        let stage = json_detail_string(value.get("stage")).unwrap_or_else(|| "unknown".into());
        *self
            .candidate_cancelled_stage_counts
            .entry(stage)
            .or_default() += 1;
        let provider =
            json_detail_string(value.get("provider")).unwrap_or_else(|| "unknown".into());
        *self
            .candidate_cancelled_provider_counts
            .entry(provider)
            .or_default() += 1;
        let attempt = value
            .get("attempt_index")
            .and_then(json_u128)
            .map(|index| index.to_string())
            .unwrap_or_else(|| "unknown".into());
        *self
            .candidate_cancelled_attempt_counts
            .entry(attempt)
            .or_default() += 1;
    }

    fn record_bitswap_hedge(&mut self, value: &serde_json::Value) {
        self.bitswap_hedges += 1;
        self.max_bitswap_hedge_timeout_ms = self
            .max_bitswap_hedge_timeout_ms
            .max(trace_count_field(value, "timeout_ms"));
    }

    fn record_bitswap_hedge_result(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.bitswap_hedge_results += 1;
        let elapsed_ms = elapsed_ms.unwrap_or_else(|| trace_count_field(value, "elapsed_ms"));
        self.bitswap_hedge_result_elapsed_values.push(elapsed_ms);
        let source = json_detail_string(value.get("source")).unwrap_or_else(|| "unknown".into());
        *self
            .bitswap_hedge_result_source_counts
            .entry(source)
            .or_default() += 1;
    }

    fn record_bitswap_hedge_skip(&mut self, value: &serde_json::Value) {
        self.bitswap_hedge_skips += 1;
        let reason = json_detail_string(value.get("reason")).unwrap_or_else(|| "unknown".into());
        *self
            .bitswap_hedge_skip_reason_counts
            .entry(reason)
            .or_default() += 1;
    }

    fn record_result(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.result_events += 1;
        let provider_count = trace_count_field(value, "provider_count");
        let elapsed_ms = elapsed_ms.unwrap_or_else(|| trace_count_field(value, "elapsed_ms"));
        self.max_attempted_provider_count = self
            .max_attempted_provider_count
            .max(trace_count_field(value, "attempted_provider_count"));
        self.max_result_elapsed_ms = self.max_result_elapsed_ms.max(elapsed_ms);
        match value.get("ok").and_then(|ok| ok.as_bool()) {
            Some(true) => {
                self.result_successes += 1;
                if provider_count == 1 {
                    self.single_provider_result_successes += 1;
                    self.single_provider_success_elapsed_values.push(elapsed_ms);
                    if let Some(provider) = json_detail_string(value.get("provider")) {
                        self.single_provider_winner_builders
                            .entry(provider)
                            .or_default()
                            .record(elapsed_ms);
                    }
                } else if provider_count > 1 {
                    self.multi_provider_success_elapsed_values.push(elapsed_ms);
                }
                let winner_rank = trace_count_field(value, "winner_provider_rank");
                let winner_original_rank =
                    trace_count_field(value, "winner_original_provider_rank");
                let winner_attempt_index = trace_count_field(value, "winner_attempt_index");
                self.max_winner_provider_rank = self.max_winner_provider_rank.max(winner_rank);
                self.max_winner_original_provider_rank = self
                    .max_winner_original_provider_rank
                    .max(winner_original_rank);
                self.max_winner_attempt_index =
                    self.max_winner_attempt_index.max(winner_attempt_index);
                if value
                    .get("single_provider_self_hedge")
                    .and_then(|enabled| enabled.as_bool())
                    == Some(true)
                {
                    let has_winner_attempt_index = value
                        .get("winner_attempt_index")
                        .and_then(json_u128)
                        .is_some();
                    let hedge_fired =
                        value.get("hedge_fired").and_then(|fired| fired.as_bool()) == Some(true);
                    if hedge_fired {
                        self.self_hedge_fired_result_events += 1;
                    }
                    if has_winner_attempt_index {
                        if winner_attempt_index == 0 {
                            self.self_hedge_winner_initial_events += 1;
                            if hedge_fired {
                                self.self_hedge_fired_winner_initial_events += 1;
                            }
                        } else {
                            self.self_hedge_winner_hedged_events += 1;
                            if hedge_fired {
                                self.self_hedge_fired_winner_hedged_events += 1;
                            }
                        }
                    } else {
                        self.self_hedge_winner_unknown_events += 1;
                        if hedge_fired {
                            self.self_hedge_fired_winner_unknown_events += 1;
                        }
                    }
                }
                match winner_rank {
                    1 => self.winner_rank1_events += 1,
                    2 => self.winner_rank2_events += 1,
                    rank if rank > 2 => self.winner_rank3_plus_events += 1,
                    _ => {}
                }
                match winner_original_rank {
                    1 => self.winner_original_rank1_events += 1,
                    2 => self.winner_original_rank2_events += 1,
                    rank if rank > 2 => self.winner_original_rank3_plus_events += 1,
                    _ => {}
                }
                if value
                    .get("winner_provider_scored")
                    .and_then(|scored| scored.as_bool())
                    == Some(true)
                {
                    self.winner_scored_events += 1;
                    if let Some(score_ms) =
                        value.get("winner_provider_score_ms").and_then(json_u128)
                    {
                        self.winner_score_elapsed_values.push(score_ms);
                    }
                }
                if value
                    .get("winner_within_initial_width")
                    .and_then(|within| within.as_bool())
                    == Some(true)
                {
                    self.winner_initial_width_events += 1;
                } else if winner_rank > 0 {
                    self.winner_late_events += 1;
                }
            }
            Some(false) => {
                self.result_failures += 1;
                if provider_count == 1 {
                    self.single_provider_result_failures += 1;
                }
            }
            None => {}
        }
    }

    fn finish(&mut self) {
        self.single_provider_success_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.single_provider_success_elapsed_values,
        ));
        self.multi_provider_success_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.multi_provider_success_elapsed_values,
        ));
        self.winner_score_elapsed_ms =
            LatencySummary::from_values(std::mem::take(&mut self.winner_score_elapsed_values));
        self.bitswap_hedge_result_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.bitswap_hedge_result_elapsed_values,
        ));
        self.bitswap_hedge_result_sources =
            sorted_trace_counts(std::mem::take(&mut self.bitswap_hedge_result_source_counts));
        self.bitswap_hedge_skip_reasons =
            sorted_trace_counts(std::mem::take(&mut self.bitswap_hedge_skip_reason_counts));
        self.self_hedge_skip_reasons =
            sorted_trace_counts(std::mem::take(&mut self.self_hedge_skip_reason_counts));
        self.candidate_cancelled_stages =
            sorted_trace_counts(std::mem::take(&mut self.candidate_cancelled_stage_counts));
        self.candidate_cancelled_providers = sorted_trace_counts(std::mem::take(
            &mut self.candidate_cancelled_provider_counts,
        ));
        self.candidate_cancelled_attempts =
            sorted_trace_counts(std::mem::take(&mut self.candidate_cancelled_attempt_counts));
        self.single_provider_winners = sorted_trace_http_provider_race_winners(std::mem::take(
            &mut self.single_provider_winner_builders,
        ));
    }

    fn has_events(&self) -> bool {
        self.events > 0
            || self.hedges > 0
            || self.self_hedges > 0
            || self.bitswap_hedges > 0
            || self.bitswap_hedge_results > 0
            || self.bitswap_hedge_skips > 0
            || self.candidate_cancellations > 0
            || self.result_events > 0
    }
}

#[derive(Debug, Serialize)]
struct TraceHttpProviderRaceWinnerProviderAggregate {
    provider: String,
    events: usize,
    total_ms: u128,
    max_ms: u128,
}

#[derive(Debug, Default)]
struct TraceHttpProviderRaceWinnerProviderBuilder {
    events: usize,
    total_ms: u128,
    max_ms: u128,
}

impl TraceHttpProviderRaceWinnerProviderBuilder {
    fn record(&mut self, elapsed_ms: u128) {
        self.events += 1;
        self.total_ms += elapsed_ms;
        self.max_ms = self.max_ms.max(elapsed_ms);
    }
}

impl TraceHttpProviderFetchAggregate {
    fn has_response_milestones(&self) -> bool {
        self.response_bytes > 0
            || self.first_chunk_events > 0
            || self.max_response_headers_elapsed_ms > 0
            || self.max_response_first_chunk_elapsed_ms > 0
            || self.max_response_body_elapsed_ms > 0
    }
}

#[derive(Debug, Serialize)]
struct TraceHttpProviderMilestoneAggregate {
    provider: String,
    events: usize,
    successes: usize,
    failures: usize,
    bytes: u128,
    response_bytes: u128,
    first_chunk_events: usize,
    elapsed_ms: LatencySummary,
    response_headers_elapsed_ms: LatencySummary,
    response_first_chunk_elapsed_ms: LatencySummary,
    response_body_elapsed_ms: LatencySummary,
    total_ms: u128,
    max_ms: u128,
    max_response_headers_elapsed_ms: u128,
    max_response_first_chunk_elapsed_ms: u128,
    max_response_body_elapsed_ms: u128,
}

#[derive(Default)]
struct TraceHttpProviderMilestoneBuilder {
    events: usize,
    successes: usize,
    failures: usize,
    bytes: u128,
    response_bytes: u128,
    first_chunk_events: usize,
    elapsed_values: Vec<u128>,
    response_headers_elapsed_values: Vec<u128>,
    response_first_chunk_elapsed_values: Vec<u128>,
    response_body_elapsed_values: Vec<u128>,
    total_ms: u128,
    max_ms: u128,
    max_response_headers_elapsed_ms: u128,
    max_response_first_chunk_elapsed_ms: u128,
    max_response_body_elapsed_ms: u128,
}

impl TraceHttpProviderMilestoneBuilder {
    fn record(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.events += 1;
        match value.get("ok").and_then(|ok| ok.as_bool()) {
            Some(true) => self.successes += 1,
            Some(false) => self.failures += 1,
            None => {}
        }
        self.bytes += value.get("bytes").and_then(json_u128).unwrap_or_default();
        self.response_bytes += value
            .get("response_bytes")
            .and_then(json_u128)
            .unwrap_or_default();
        if value
            .get("response_first_chunk_seen")
            .and_then(|seen| seen.as_bool())
            == Some(true)
        {
            self.first_chunk_events += 1;
        }
        if let Some(elapsed_ms) = elapsed_ms {
            self.total_ms += elapsed_ms;
            self.max_ms = self.max_ms.max(elapsed_ms);
            self.elapsed_values.push(elapsed_ms);
        }
        if let Some(headers_elapsed_ms) =
            value.get("response_headers_elapsed_ms").and_then(json_u128)
        {
            self.response_headers_elapsed_values
                .push(headers_elapsed_ms);
        }
        if let Some(first_chunk_elapsed_ms) = value
            .get("response_first_chunk_elapsed_ms")
            .and_then(json_u128)
        {
            self.response_first_chunk_elapsed_values
                .push(first_chunk_elapsed_ms);
        }
        if let Some(body_elapsed_ms) = value.get("response_body_elapsed_ms").and_then(json_u128) {
            self.response_body_elapsed_values.push(body_elapsed_ms);
        }
        self.max_response_headers_elapsed_ms = self.max_response_headers_elapsed_ms.max(
            value
                .get("response_headers_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_response_first_chunk_elapsed_ms = self.max_response_first_chunk_elapsed_ms.max(
            value
                .get("response_first_chunk_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_response_body_elapsed_ms = self.max_response_body_elapsed_ms.max(
            value
                .get("response_body_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceProviderRetryAggregate {
    refresh_after_timeout_events: usize,
    refresh_after_failure_events: usize,
    skipped_empty_provider_set_events: usize,
    retry_count_events: usize,
    same_provider_sets: usize,
    same_bitswap_peer_sets: usize,
    request_timeout_retry_counts: usize,
    same_bitswap_request_timeout_retry_counts: usize,
    request_timeout_retries: usize,
    timeout_retries: usize,
    connection_timeout_retries: usize,
}

impl TraceProviderRetryAggregate {
    fn has_events(&self) -> bool {
        self.refresh_after_timeout_events > 0
            || self.refresh_after_failure_events > 0
            || self.skipped_empty_provider_set_events > 0
            || self.retry_count_events > 0
            || self.request_timeout_retries > 0
            || self.timeout_retries > 0
            || self.connection_timeout_retries > 0
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceDelegatedProviderLookupAggregate {
    events: usize,
    successes: usize,
    failures: usize,
    providers: u128,
    http_providers: u128,
    zero_http_provider_events: usize,
    single_http_provider_events: usize,
    multi_http_provider_events: usize,
    single_http_provider_target_miss_events: usize,
    self_hedges: usize,
    max_self_hedge_timeout_ms: u128,
    response_bytes: u128,
    response_lines: u128,
    first_chunk_events: usize,
    first_http_provider_events: usize,
    target_met_events: usize,
    elapsed_ms: LatencySummary,
    response_headers_elapsed_ms: LatencySummary,
    response_first_chunk_elapsed_ms: LatencySummary,
    response_first_http_provider_elapsed_ms: LatencySummary,
    response_target_met_elapsed_ms: LatencySummary,
    single_http_provider_elapsed_ms: LatencySummary,
    single_http_provider_first_http_elapsed_ms: LatencySummary,
    max_elapsed_ms: u128,
    max_response_headers_elapsed_ms: u128,
    max_response_first_chunk_elapsed_ms: u128,
    max_response_first_http_provider_elapsed_ms: u128,
    max_response_target_met_elapsed_ms: u128,
    max_single_http_provider_elapsed_ms: u128,
    max_single_http_provider_first_http_elapsed_ms: u128,
    #[serde(skip)]
    elapsed_values: Vec<u128>,
    #[serde(skip)]
    response_headers_elapsed_values: Vec<u128>,
    #[serde(skip)]
    response_first_chunk_elapsed_values: Vec<u128>,
    #[serde(skip)]
    response_first_http_provider_elapsed_values: Vec<u128>,
    #[serde(skip)]
    response_target_met_elapsed_values: Vec<u128>,
    #[serde(skip)]
    single_http_provider_elapsed_values: Vec<u128>,
    #[serde(skip)]
    single_http_provider_first_http_elapsed_values: Vec<u128>,
}

impl TraceDelegatedProviderLookupAggregate {
    fn record(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.events += 1;
        if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
            self.failures += 1;
        } else {
            self.successes += 1;
        }
        self.providers += value
            .get("provider_count")
            .and_then(json_u128)
            .unwrap_or_default();
        if let Some(elapsed_ms) = elapsed_ms {
            self.elapsed_values.push(elapsed_ms);
        }
        let http_provider_count = value
            .get("http_provider_count")
            .and_then(json_u128)
            .unwrap_or_default();
        self.http_providers += http_provider_count;
        match http_provider_count {
            0 => self.zero_http_provider_events += 1,
            1 => {
                self.single_http_provider_events += 1;
                if value
                    .get("response_target_met")
                    .and_then(|seen| seen.as_bool())
                    != Some(true)
                {
                    self.single_http_provider_target_miss_events += 1;
                }
                self.max_single_http_provider_elapsed_ms = self
                    .max_single_http_provider_elapsed_ms
                    .max(elapsed_ms.unwrap_or_default());
                if let Some(elapsed_ms) = elapsed_ms {
                    self.single_http_provider_elapsed_values.push(elapsed_ms);
                }
                self.max_single_http_provider_first_http_elapsed_ms =
                    self.max_single_http_provider_first_http_elapsed_ms.max(
                        value
                            .get("response_first_http_provider_elapsed_ms")
                            .and_then(json_u128)
                            .unwrap_or_default(),
                    );
                if let Some(first_http_elapsed_ms) = value
                    .get("response_first_http_provider_elapsed_ms")
                    .and_then(json_u128)
                {
                    self.single_http_provider_first_http_elapsed_values
                        .push(first_http_elapsed_ms);
                }
            }
            _ => self.multi_http_provider_events += 1,
        }
        self.response_bytes += value
            .get("response_bytes")
            .and_then(json_u128)
            .unwrap_or_default();
        self.response_lines += value
            .get("response_lines")
            .and_then(json_u128)
            .unwrap_or_default();
        if value
            .get("response_first_chunk_seen")
            .and_then(|seen| seen.as_bool())
            == Some(true)
        {
            self.first_chunk_events += 1;
        }
        if let Some(headers_elapsed_ms) =
            value.get("response_headers_elapsed_ms").and_then(json_u128)
        {
            self.response_headers_elapsed_values
                .push(headers_elapsed_ms);
        }
        if let Some(first_chunk_elapsed_ms) = value
            .get("response_first_chunk_elapsed_ms")
            .and_then(json_u128)
        {
            self.response_first_chunk_elapsed_values
                .push(first_chunk_elapsed_ms);
        }
        if value
            .get("response_first_http_provider_seen")
            .and_then(|seen| seen.as_bool())
            == Some(true)
        {
            self.first_http_provider_events += 1;
        }
        if let Some(first_http_elapsed_ms) = value
            .get("response_first_http_provider_elapsed_ms")
            .and_then(json_u128)
        {
            self.response_first_http_provider_elapsed_values
                .push(first_http_elapsed_ms);
        }
        if value
            .get("response_target_met")
            .and_then(|seen| seen.as_bool())
            == Some(true)
        {
            self.target_met_events += 1;
        }
        if let Some(target_met_elapsed_ms) = value
            .get("response_target_met_elapsed_ms")
            .and_then(json_u128)
        {
            self.response_target_met_elapsed_values
                .push(target_met_elapsed_ms);
        }
        self.max_elapsed_ms = self.max_elapsed_ms.max(elapsed_ms.unwrap_or_default());
        self.max_response_headers_elapsed_ms = self.max_response_headers_elapsed_ms.max(
            value
                .get("response_headers_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_response_first_chunk_elapsed_ms = self.max_response_first_chunk_elapsed_ms.max(
            value
                .get("response_first_chunk_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_response_first_http_provider_elapsed_ms =
            self.max_response_first_http_provider_elapsed_ms.max(
                value
                    .get("response_first_http_provider_elapsed_ms")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
        self.max_response_target_met_elapsed_ms = self.max_response_target_met_elapsed_ms.max(
            value
                .get("response_target_met_elapsed_ms")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
    }

    fn record_self_hedge(&mut self, value: &serde_json::Value) {
        self.self_hedges += 1;
        self.max_self_hedge_timeout_ms = self
            .max_self_hedge_timeout_ms
            .max(trace_count_field(value, "timeout_ms"));
    }

    fn finish(&mut self) {
        self.elapsed_ms = LatencySummary::from_values(std::mem::take(&mut self.elapsed_values));
        self.response_headers_elapsed_ms =
            LatencySummary::from_values(std::mem::take(&mut self.response_headers_elapsed_values));
        self.response_first_chunk_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.response_first_chunk_elapsed_values,
        ));
        self.response_first_http_provider_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.response_first_http_provider_elapsed_values,
        ));
        self.response_target_met_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.response_target_met_elapsed_values,
        ));
        self.single_http_provider_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.single_http_provider_elapsed_values,
        ));
        self.single_http_provider_first_http_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.single_http_provider_first_http_elapsed_values),
        );
    }

    fn has_response_milestones(&self) -> bool {
        self.response_bytes > 0
            || self.response_lines > 0
            || self.http_providers > 0
            || self.max_response_headers_elapsed_ms > 0
            || self.first_chunk_events > 0
            || self.first_http_provider_events > 0
            || self.target_met_events > 0
    }
}

#[derive(Debug, Serialize)]
struct TraceDelegatedProviderEndpointAggregate {
    endpoint: String,
    events: usize,
    successes: usize,
    failures: usize,
    providers: u128,
    http_providers: u128,
    zero_http_provider_events: usize,
    single_http_provider_events: usize,
    multi_http_provider_events: usize,
    single_http_provider_target_miss_events: usize,
    self_hedges: usize,
    max_self_hedge_timeout_ms: u128,
    response_bytes: u128,
    response_lines: u128,
    first_chunk_events: usize,
    first_http_provider_events: usize,
    target_met_events: usize,
    elapsed_ms: LatencySummary,
    response_headers_elapsed_ms: LatencySummary,
    response_first_chunk_elapsed_ms: LatencySummary,
    response_first_http_provider_elapsed_ms: LatencySummary,
    response_target_met_elapsed_ms: LatencySummary,
    single_http_provider_elapsed_ms: LatencySummary,
    single_http_provider_first_http_elapsed_ms: LatencySummary,
    max_elapsed_ms: u128,
    max_response_headers_elapsed_ms: u128,
    max_response_first_chunk_elapsed_ms: u128,
    max_response_first_http_provider_elapsed_ms: u128,
    max_response_target_met_elapsed_ms: u128,
    max_single_http_provider_elapsed_ms: u128,
    max_single_http_provider_first_http_elapsed_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceDhtProviderLookupAggregate {
    events: usize,
    successes: usize,
    failures: usize,
    providers: u128,
    max_providers: u128,
    max_timeout_ms: u128,
    max_query_timeout_ms: u128,
    max_elapsed_ms: u128,
}

impl TraceDhtProviderLookupAggregate {
    fn record(&mut self, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.events += 1;
        if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
            self.failures += 1;
        } else {
            self.successes += 1;
        }
        self.providers += trace_count_field(value, "provider_count");
        self.max_providers = self
            .max_providers
            .max(trace_count_field(value, "max_providers"));
        if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
            self.max_timeout_ms = self.max_timeout_ms.max(timeout_ms);
        }
        if let Some(query_timeout_ms) = value.get("query_timeout_ms").and_then(json_u128) {
            self.max_query_timeout_ms = self.max_query_timeout_ms.max(query_timeout_ms);
        }
        self.max_elapsed_ms = self.max_elapsed_ms.max(elapsed_ms.unwrap_or_default());
    }
}

#[derive(Debug, Serialize)]
struct TraceProviderDiversityLowAggregate {
    events: usize,
    failures: usize,
    provider_count_total: u128,
    bitswap_provider_count_total: u128,
    dht_provider_count_total: u128,
    max_provider_count: u128,
    max_bitswap_provider_count: u128,
    max_dht_provider_count: u128,
    max_timeout_ms: u128,
    fallbacks: Vec<TraceValueCount>,
}

#[derive(Default)]
struct TraceProviderDiversityLowBuilder {
    events: usize,
    failures: usize,
    provider_count_total: u128,
    bitswap_provider_count_total: u128,
    dht_provider_count_total: u128,
    max_provider_count: u128,
    max_bitswap_provider_count: u128,
    max_dht_provider_count: u128,
    max_timeout_ms: u128,
    fallbacks: BTreeMap<String, usize>,
}

impl TraceProviderDiversityLowBuilder {
    fn record(&mut self, value: &serde_json::Value) {
        self.events += 1;
        if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
            self.failures += 1;
        }
        let provider_count = trace_count_field(value, "provider_count");
        let bitswap_provider_count = trace_count_field(value, "bitswap_provider_count");
        let dht_provider_count = trace_count_field(value, "dht_provider_count");
        self.provider_count_total += provider_count;
        self.bitswap_provider_count_total += bitswap_provider_count;
        self.dht_provider_count_total += dht_provider_count;
        self.max_provider_count = self.max_provider_count.max(provider_count);
        self.max_bitswap_provider_count =
            self.max_bitswap_provider_count.max(bitswap_provider_count);
        self.max_dht_provider_count = self.max_dht_provider_count.max(dht_provider_count);
        if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
            self.max_timeout_ms = self.max_timeout_ms.max(timeout_ms);
        }
        if let Some(fallback) = value.get("fallback").and_then(|fallback| fallback.as_str()) {
            *self.fallbacks.entry(fallback.to_string()).or_default() += 1;
        }
    }

    fn into_aggregate(self) -> TraceProviderDiversityLowAggregate {
        TraceProviderDiversityLowAggregate {
            events: self.events,
            failures: self.failures,
            provider_count_total: self.provider_count_total,
            bitswap_provider_count_total: self.bitswap_provider_count_total,
            dht_provider_count_total: self.dht_provider_count_total,
            max_provider_count: self.max_provider_count,
            max_bitswap_provider_count: self.max_bitswap_provider_count,
            max_dht_provider_count: self.max_dht_provider_count,
            max_timeout_ms: self.max_timeout_ms,
            fallbacks: sorted_trace_counts(self.fallbacks),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceGatewaySmallBodyCacheAggregate {
    events: usize,
    hits: u128,
    misses: u128,
    inserts: u128,
    evictions: u128,
    bytes_served: u128,
    max_body_len: u128,
    max_cache_len: u128,
    max_cache_bytes: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceGatewayDirectBodyAggregate {
    events: usize,
    bytes: u128,
    max_body_len: u128,
    max_elapsed_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceGatewayStreamBodyAggregate {
    events: usize,
    bytes: u128,
    max_body_len: u128,
    max_chunks: u128,
    max_elapsed_ms: u128,
}

#[derive(Clone, Debug, Default, Serialize)]
struct TraceUnixfsMetadataCacheAggregate {
    events: usize,
    hits: u128,
    misses: u128,
    inserts: u128,
    evictions: u128,
    oversized_skips: u128,
    max_len: u128,
    path_hits: u128,
    path_misses: u128,
    path_inserts: u128,
    path_evictions: u128,
    path_oversized_skips: u128,
    max_path_len: u128,
    file_size_hits: u128,
    file_size_misses: u128,
    file_size_inserts: u128,
    file_size_evictions: u128,
    max_file_size_len: u128,
    max_capacity: u128,
}

impl TraceUnixfsMetadataCacheAggregate {
    fn has_events(&self) -> bool {
        self.events > 0
    }

    fn record(&mut self, value: &serde_json::Value) {
        self.events += 1;
        self.hits += value.get("hits").and_then(json_u128).unwrap_or_default();
        self.misses += value.get("misses").and_then(json_u128).unwrap_or_default();
        self.inserts += value.get("inserts").and_then(json_u128).unwrap_or_default();
        self.evictions += value
            .get("evictions")
            .and_then(json_u128)
            .unwrap_or_default();
        self.oversized_skips += value
            .get("oversized_skips")
            .and_then(json_u128)
            .unwrap_or_default();
        self.path_hits += value
            .get("path_hits")
            .and_then(json_u128)
            .unwrap_or_default();
        self.path_misses += value
            .get("path_misses")
            .and_then(json_u128)
            .unwrap_or_default();
        self.path_inserts += value
            .get("path_inserts")
            .and_then(json_u128)
            .unwrap_or_default();
        self.path_evictions += value
            .get("path_evictions")
            .and_then(json_u128)
            .unwrap_or_default();
        self.path_oversized_skips += value
            .get("path_oversized_skips")
            .and_then(json_u128)
            .unwrap_or_default();
        self.file_size_hits += value
            .get("file_size_hits")
            .and_then(json_u128)
            .unwrap_or_default();
        self.file_size_misses += value
            .get("file_size_misses")
            .and_then(json_u128)
            .unwrap_or_default();
        self.file_size_inserts += value
            .get("file_size_inserts")
            .and_then(json_u128)
            .unwrap_or_default();
        self.file_size_evictions += value
            .get("file_size_evictions")
            .and_then(json_u128)
            .unwrap_or_default();
        self.max_len = self.max_len.max(
            value
                .get("cache_len")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_path_len = self.max_path_len.max(
            value
                .get("path_cache_len")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_file_size_len = self.max_file_size_len.max(
            value
                .get("file_size_cache_len")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
        self.max_capacity = self.max_capacity.max(
            value
                .get("cache_capacity")
                .and_then(json_u128)
                .unwrap_or_default(),
        );
    }

    fn merge(&mut self, other: &Self) {
        self.events += other.events;
        self.hits += other.hits;
        self.misses += other.misses;
        self.inserts += other.inserts;
        self.evictions += other.evictions;
        self.oversized_skips += other.oversized_skips;
        self.path_hits += other.path_hits;
        self.path_misses += other.path_misses;
        self.path_inserts += other.path_inserts;
        self.path_evictions += other.path_evictions;
        self.path_oversized_skips += other.path_oversized_skips;
        self.file_size_hits += other.file_size_hits;
        self.file_size_misses += other.file_size_misses;
        self.file_size_inserts += other.file_size_inserts;
        self.file_size_evictions += other.file_size_evictions;
        self.max_len = self.max_len.max(other.max_len);
        self.max_path_len = self.max_path_len.max(other.max_path_len);
        self.max_file_size_len = self.max_file_size_len.max(other.max_file_size_len);
        self.max_capacity = self.max_capacity.max(other.max_capacity);
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapDnsExpansionAggregate {
    events: usize,
    cached: usize,
    uncached: usize,
    failed: usize,
    records: u128,
    ips: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapProviderQualityAggregate {
    events: usize,
    provider_addr_count: u128,
    expanded_provider_addr_count: u128,
    supported_provider_addr_count: u128,
    rejected_provider_addr_count: u128,
    id_only_provider_count: u128,
    invalid_provider_id_count: u128,
    provider_without_supported_bitswap_addr_count: u128,
    unsupported_relay_addr_count: u128,
    unsupported_webtransport_addr_count: u128,
    unsupported_webrtc_addr_count: u128,
    unsupported_certhash_addr_count: u128,
    unsupported_transport_addr_count: u128,
    missing_peer_addr_count: u128,
    unparsable_addr_count: u128,
    addr_with_relay_count: u128,
    addr_with_webtransport_count: u128,
    addr_with_webrtc_count: u128,
    addr_with_certhash_count: u128,
}

impl TraceBitswapProviderQualityAggregate {
    fn accumulate(&mut self, value: &serde_json::Value) {
        self.events += 1;
        self.provider_addr_count += trace_count_field(value, "provider_addr_count");
        self.expanded_provider_addr_count +=
            trace_count_field(value, "expanded_provider_addr_count");
        self.supported_provider_addr_count +=
            trace_count_field(value, "supported_provider_addr_count");
        self.rejected_provider_addr_count +=
            trace_count_field(value, "rejected_provider_addr_count");
        self.id_only_provider_count += trace_count_field(value, "id_only_provider_count");
        self.invalid_provider_id_count += trace_count_field(value, "invalid_provider_id_count");
        self.provider_without_supported_bitswap_addr_count +=
            trace_count_field(value, "provider_without_supported_bitswap_addr_count");
        self.unsupported_relay_addr_count +=
            trace_count_field(value, "unsupported_relay_addr_count");
        self.unsupported_webtransport_addr_count +=
            trace_count_field(value, "unsupported_webtransport_addr_count");
        self.unsupported_webrtc_addr_count +=
            trace_count_field(value, "unsupported_webrtc_addr_count");
        self.unsupported_certhash_addr_count +=
            trace_count_field(value, "unsupported_certhash_addr_count");
        self.unsupported_transport_addr_count +=
            trace_count_field(value, "unsupported_transport_addr_count");
        self.missing_peer_addr_count += trace_count_field(value, "missing_peer_addr_count");
        self.unparsable_addr_count += trace_count_field(value, "unparsable_addr_count");
        self.addr_with_relay_count += trace_count_field(value, "addr_with_relay_count");
        self.addr_with_webtransport_count +=
            trace_count_field(value, "addr_with_webtransport_count");
        self.addr_with_webrtc_count += trace_count_field(value, "addr_with_webrtc_count");
        self.addr_with_certhash_count += trace_count_field(value, "addr_with_certhash_count");
    }
}

#[derive(Debug, Serialize)]
struct TraceBitswapConnectionEstablishedAggregate {
    events: usize,
    established_ms: LatencySummary,
    wait_elapsed_ms: LatencySummary,
    failed_dial_count: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapDialRejectedAggregate {
    events: usize,
    connection_limit: usize,
    other: usize,
}

#[derive(Debug, Serialize)]
struct TraceBitswapConnectionErrorAggregate {
    events: usize,
    with_peer: usize,
    without_peer: usize,
    classes: Vec<TraceValueCount>,
    peers: Vec<TraceValueCount>,
}

#[derive(Debug, Serialize)]
struct TraceBitswapConnectionBackoffAggregate {
    backoffs: usize,
    skipped: usize,
    classes: Vec<TraceValueCount>,
    peers: Vec<TraceValueCount>,
    skipped_peers: Vec<TraceValueCount>,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapSessionAggregate {
    fetches: usize,
    with_trusted_peers: usize,
    trusted_successes: usize,
    untrusted_successes: usize,
    trusted_failures: usize,
    request_timeouts_with_trusted: usize,
    session_shortcut_starts: usize,
    session_shortcut_pre_lookup_waits: usize,
    session_shortcut_pre_lookup_hits: usize,
    session_shortcut_pre_lookup_misses: usize,
    session_shortcut_pre_lookup_timeouts: usize,
    session_shortcut_pre_lookup_elapsed_ms: LatencySummary,
    session_shortcut_pre_lookup_hit_elapsed_ms: LatencySummary,
    session_shortcut_pre_lookup_miss_elapsed_ms: LatencySummary,
    session_shortcut_pre_lookup_timeout_elapsed_ms: LatencySummary,
    session_shortcut_pre_lookup_max_ms: u128,
    session_shortcut_pre_lookup_budgets: BTreeMap<String, usize>,
    session_shortcut_post_lookup_waits: usize,
    session_shortcut_post_lookup_hits: usize,
    session_shortcut_post_lookup_misses: usize,
    session_shortcut_post_lookup_timeouts: usize,
    session_shortcut_post_lookup_errors: usize,
    session_shortcut_post_lookup_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_hit_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_timeout_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_single_http_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_single_http_hit_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_single_http_timeout_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_max_ms: u128,
    session_shortcut_post_lookup_budgets: BTreeMap<String, usize>,
    session_shortcut_post_lookup_timeout_budgets: BTreeMap<String, usize>,
    session_shortcut_post_lookup_http_provider_counts: BTreeMap<String, usize>,
    session_shortcut_post_lookup_races: usize,
    session_shortcut_post_lookup_race_provider_wins: usize,
    session_shortcut_post_lookup_race_bitswap_wins: usize,
    session_shortcut_post_lookup_race_errors: usize,
    session_shortcut_post_lookup_race_provider_bitswap_wins: usize,
    session_shortcut_post_lookup_race_single_http_provider_bitswap_wins: usize,
    session_shortcut_post_lookup_race_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_race_provider_result_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_ms: LatencySummary,
    session_shortcut_post_lookup_race_outcomes: Vec<TraceValueCount>,
    session_shortcut_post_lookup_race_sources: Vec<TraceValueCount>,
    session_shortcut_post_lookup_race_http_provider_counts: Vec<TraceValueCount>,
    session_shortcut_attempts: usize,
    session_shortcut_hits: usize,
    session_shortcut_misses: usize,
    session_late_peer_waits: usize,
    session_late_peer_hits: usize,
    session_late_peer_misses: usize,
    session_late_peer_elapsed_ms: LatencySummary,
    session_late_peer_hit_elapsed_ms: LatencySummary,
    session_late_peer_miss_elapsed_ms: LatencySummary,
    session_late_peer_max_ms: u128,
    #[serde(skip)]
    session_shortcut_pre_lookup_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_pre_lookup_hit_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_pre_lookup_miss_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_pre_lookup_timeout_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_hit_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_timeout_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_single_http_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_single_http_hit_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_single_http_timeout_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_provider_result_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_outcome_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_source_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    session_shortcut_post_lookup_race_http_provider_count_counts: BTreeMap<String, usize>,
    #[serde(skip)]
    session_late_peer_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_late_peer_hit_elapsed_values: Vec<u128>,
    #[serde(skip)]
    session_late_peer_miss_elapsed_values: Vec<u128>,
}

impl TraceBitswapSessionAggregate {
    fn has_events(&self) -> bool {
        self.fetches > 0
            || self.session_shortcut_starts > 0
            || self.session_shortcut_pre_lookup_waits > 0
            || self.session_shortcut_post_lookup_waits > 0
            || self.session_shortcut_post_lookup_races > 0
            || self.session_shortcut_attempts > 0
            || self.session_late_peer_waits > 0
    }

    fn finish(&mut self) {
        self.session_shortcut_pre_lookup_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.session_shortcut_pre_lookup_elapsed_values,
        ));
        self.session_shortcut_pre_lookup_hit_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_pre_lookup_hit_elapsed_values),
        );
        self.session_shortcut_pre_lookup_miss_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_pre_lookup_miss_elapsed_values),
        );
        self.session_shortcut_pre_lookup_timeout_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_pre_lookup_timeout_elapsed_values),
        );
        self.session_shortcut_post_lookup_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.session_shortcut_post_lookup_elapsed_values,
        ));
        self.session_shortcut_post_lookup_hit_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_post_lookup_hit_elapsed_values),
        );
        self.session_shortcut_post_lookup_timeout_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_post_lookup_timeout_elapsed_values),
        );
        self.session_shortcut_post_lookup_single_http_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_post_lookup_single_http_elapsed_values),
        );
        self.session_shortcut_post_lookup_single_http_hit_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_post_lookup_single_http_hit_elapsed_values),
        );
        self.session_shortcut_post_lookup_single_http_timeout_elapsed_ms =
            LatencySummary::from_values(std::mem::take(
                &mut self.session_shortcut_post_lookup_single_http_timeout_elapsed_values,
            ));
        self.session_shortcut_post_lookup_race_elapsed_ms = LatencySummary::from_values(
            std::mem::take(&mut self.session_shortcut_post_lookup_race_elapsed_values),
        );
        self.session_shortcut_post_lookup_race_provider_result_elapsed_ms =
            LatencySummary::from_values(std::mem::take(
                &mut self.session_shortcut_post_lookup_race_provider_result_elapsed_values,
            ));
        self.session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_ms =
            LatencySummary::from_values(std::mem::take(
                &mut self
                    .session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_values,
            ));
        self.session_shortcut_post_lookup_race_outcomes = sorted_trace_counts(std::mem::take(
            &mut self.session_shortcut_post_lookup_race_outcome_counts,
        ));
        self.session_shortcut_post_lookup_race_sources = sorted_trace_counts(std::mem::take(
            &mut self.session_shortcut_post_lookup_race_source_counts,
        ));
        self.session_shortcut_post_lookup_race_http_provider_counts = sorted_trace_counts(
            std::mem::take(&mut self.session_shortcut_post_lookup_race_http_provider_count_counts),
        );
        self.session_late_peer_elapsed_ms =
            LatencySummary::from_values(std::mem::take(&mut self.session_late_peer_elapsed_values));
        self.session_late_peer_hit_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.session_late_peer_hit_elapsed_values,
        ));
        self.session_late_peer_miss_elapsed_ms = LatencySummary::from_values(std::mem::take(
            &mut self.session_late_peer_miss_elapsed_values,
        ));
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapPeerAttemptAggregate {
    starts: usize,
    outgoing_completed: usize,
    successes: usize,
    failures: usize,
    connection_timeouts: usize,
    read_timeouts: usize,
    other_failures: usize,
    prefer_want_have: usize,
    cancelled: usize,
    cancelled_prefer_want_have: usize,
    cancelled_stages: Vec<TraceValueCount>,
    cancelled_candidate_indexes: Vec<TraceValueCount>,
    cancelled_request_modes: Vec<TraceValueCount>,
    cancelled_first_addr_transports: Vec<TraceValueCount>,
    cancelled_first_addr_families: Vec<TraceValueCount>,
}

impl TraceBitswapPeerAttemptAggregate {
    fn has_events(&self) -> bool {
        self.starts > 0 || self.outgoing_completed > 0 || self.cancelled > 0
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapWantHaveProbeAggregate {
    events: usize,
    ok: usize,
    failures: usize,
    have: usize,
    dont_have: usize,
    block: usize,
    want_block_followups: usize,
    no_presence: usize,
    bytes: u128,
    extra_blocks: u128,
    max_timeout_ms: u128,
    elapsed_ms: LatencySummary,
    outcomes: Vec<TraceValueCount>,
    peers: Vec<TraceValueCount>,
    candidate_indexes: Vec<TraceValueCount>,
    request_modes: Vec<TraceValueCount>,
    first_addr_transports: Vec<TraceValueCount>,
    first_addr_families: Vec<TraceValueCount>,
    target_peer_counts: Vec<TraceValueCount>,
    peer_addr_counts: Vec<TraceValueCount>,
}

impl TraceBitswapWantHaveProbeAggregate {
    fn has_events(&self) -> bool {
        self.events > 0
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapDialPlanAggregate {
    events: usize,
    peer_targets: u128,
    candidate_peers: u128,
    new_dial_peers: u128,
    new_dial_addrs: u128,
    suppressed_dial_peers: u128,
    suppressed_dial_addrs: u128,
    pending_dial_peers: u128,
    connected_peers: u128,
    max_command_queued_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapIncomingBlockAggregate {
    matches: usize,
    blocks: u128,
    bytes: u128,
    delivered_waiters: u128,
    dropped_waiters: u128,
    max_oldest_pending_ms: u128,
    max_pending_waiters: u128,
    max_dropped_waiters: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapIncomingBatchAggregate {
    events: usize,
    total_cids: u128,
    max_cids: u128,
    requested_blocks: u128,
    extra_blocks: u128,
    max_elapsed_ms: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapIncomingReadAggregate {
    events: usize,
    failures: usize,
    dropped: usize,
    timed_out: usize,
    max_pending_reads: u128,
    max_elapsed_ms: u128,
}

#[derive(Debug, Serialize)]
struct TraceBitswapTimeoutRecoveryAggregate {
    request_timeouts: usize,
    cold_request_timeouts: usize,
    mixed_trusted_request_timeouts: usize,
    trusted_only_request_timeouts: usize,
    request_timeouts_without_dial_plan: usize,
    mixed_trusted_request_timeouts_without_dial_plan: usize,
    request_timeout_budgets: Vec<TraceValueCount>,
    max_request_timeout_peer_count: u128,
    request_timeout_want_block_targets: u128,
    request_timeout_want_have_targets: u128,
    max_request_timeout_want_block_targets: u128,
    max_request_timeout_want_have_targets: u128,
    request_timeout_events: usize,
    request_timeout_reset_true: usize,
    request_timeout_reset_false: usize,
    client_resets: usize,
    provider_retry_starts: usize,
    same_provider_retry_starts: usize,
    refreshed_provider_retry_starts: usize,
    retry_successes: usize,
    trusted_retry_successes: usize,
    untrusted_retry_successes: usize,
    retry_failures: usize,
    retry_unresolved: usize,
    retry_success_elapsed_ms: LatencySummary,
}

impl TraceBitswapTimeoutRecoveryAggregate {
    fn has_events(&self) -> bool {
        self.request_timeouts > 0
            || self.request_timeout_events > 0
            || self.client_resets > 0
            || self.provider_retry_starts > 0
            || self.retry_successes > 0
            || self.retry_failures > 0
            || self.retry_unresolved > 0
    }
}

#[derive(Default)]
struct TraceBitswapTimeoutRecoveryBuilder {
    request_timeouts: usize,
    cold_request_timeouts: usize,
    mixed_trusted_request_timeouts: usize,
    trusted_only_request_timeouts: usize,
    request_timeouts_without_dial_plan: usize,
    mixed_trusted_request_timeouts_without_dial_plan: usize,
    request_timeout_budgets: BTreeMap<String, usize>,
    max_request_timeout_peer_count: u128,
    request_timeout_want_block_targets: u128,
    request_timeout_want_have_targets: u128,
    max_request_timeout_want_block_targets: u128,
    max_request_timeout_want_have_targets: u128,
    request_timeout_events: usize,
    request_timeout_reset_true: usize,
    request_timeout_reset_false: usize,
    client_resets: usize,
    provider_retry_starts: usize,
    same_provider_retry_starts: usize,
    refreshed_provider_retry_starts: usize,
    retry_successes: usize,
    trusted_retry_successes: usize,
    untrusted_retry_successes: usize,
    retry_failures: usize,
    retry_success_elapsed_values: Vec<u128>,
}

impl TraceBitswapTimeoutRecoveryBuilder {
    fn into_aggregate(
        self,
        pending_retries: &BTreeMap<String, usize>,
    ) -> TraceBitswapTimeoutRecoveryAggregate {
        TraceBitswapTimeoutRecoveryAggregate {
            request_timeouts: self.request_timeouts,
            cold_request_timeouts: self.cold_request_timeouts,
            mixed_trusted_request_timeouts: self.mixed_trusted_request_timeouts,
            trusted_only_request_timeouts: self.trusted_only_request_timeouts,
            request_timeouts_without_dial_plan: self.request_timeouts_without_dial_plan,
            mixed_trusted_request_timeouts_without_dial_plan: self
                .mixed_trusted_request_timeouts_without_dial_plan,
            request_timeout_budgets: sorted_trace_counts(self.request_timeout_budgets),
            max_request_timeout_peer_count: self.max_request_timeout_peer_count,
            request_timeout_want_block_targets: self.request_timeout_want_block_targets,
            request_timeout_want_have_targets: self.request_timeout_want_have_targets,
            max_request_timeout_want_block_targets: self.max_request_timeout_want_block_targets,
            max_request_timeout_want_have_targets: self.max_request_timeout_want_have_targets,
            request_timeout_events: self.request_timeout_events,
            request_timeout_reset_true: self.request_timeout_reset_true,
            request_timeout_reset_false: self.request_timeout_reset_false,
            client_resets: self.client_resets,
            provider_retry_starts: self.provider_retry_starts,
            same_provider_retry_starts: self.same_provider_retry_starts,
            refreshed_provider_retry_starts: self.refreshed_provider_retry_starts,
            retry_successes: self.retry_successes,
            trusted_retry_successes: self.trusted_retry_successes,
            untrusted_retry_successes: self.untrusted_retry_successes,
            retry_failures: self.retry_failures,
            retry_unresolved: pending_retries.values().sum(),
            retry_success_elapsed_ms: LatencySummary::from_values(
                self.retry_success_elapsed_values,
            ),
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapExtraBlockAggregate {
    events: usize,
    total: u128,
    max: u128,
    incoming: u128,
    outgoing: u128,
    unknown: u128,
}

#[derive(Debug, Default, Serialize)]
struct TraceBitswapBatchAggregate {
    commands: usize,
    multi_cid_commands: usize,
    total_cids: u128,
    max_cids: u128,
    peer_attempt_starts: usize,
    peer_attempt_successes: usize,
    requested_blocks: u128,
    max_requested_blocks: u128,
    cancelled: usize,
    failures: usize,
}

impl TraceBitswapBatchAggregate {
    fn has_events(&self) -> bool {
        self.commands > 0 || self.peer_attempt_starts > 0 || self.cancelled > 0 || self.failures > 0
    }
}

#[derive(Debug, Serialize)]
struct TraceCidAggregate {
    cid: String,
    count: usize,
    total_ms: u128,
    max_ms: u128,
    phases: Vec<TraceValueCount>,
    paths: Vec<TraceValueCount>,
    bitswap_source_candidate_indexes: Vec<TraceValueCount>,
    bitswap_source_peers: Vec<TraceValueCount>,
}

#[derive(Clone, Debug, Serialize)]
struct TraceRequestAggregate {
    path: String,
    process_id: String,
    request_id: String,
    progress_request_id: Option<String>,
    parent_progress_request_id: Option<String>,
    top_level_path: Option<String>,
    status: Option<String>,
    elapsed_ms: u128,
    max_event_ms: u128,
    event_count: usize,
    phases: Vec<TraceValueCount>,
    phase_latencies: Vec<TracePhaseAggregate>,
    cids: Vec<TraceValueCount>,
    block_sources: Vec<TraceValueCount>,
    http_provider_fetches: usize,
    http_provider_fetch_successes: usize,
    http_provider_fetch_failures: usize,
    http_provider_fetch_elapsed_ms: LatencySummary,
    http_provider_fetch_response_bytes: u128,
    http_provider_fetch_first_chunk_events: usize,
    http_provider_fetch_headers_elapsed_ms: LatencySummary,
    http_provider_fetch_first_chunk_elapsed_ms: LatencySummary,
    http_provider_fetch_body_elapsed_ms: LatencySummary,
    http_provider_fetch_providers: Vec<TraceValueCount>,
    http_provider_fetch_error_classes: Vec<TraceValueCount>,
    bitswap_fetches: usize,
    bitswap_fetch_elapsed_ms: LatencySummary,
    bitswap_fetch_bytes: u128,
    bitswap_source_candidate_indexes: Vec<TraceValueCount>,
    bitswap_source_request_modes: Vec<TraceValueCount>,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
    classifications: Vec<TraceValueCount>,
    delegated_zero_http_provider_lookups: usize,
    bitswap_block_fetches: usize,
    cold_bitswap_peer_expands: usize,
    max_bitswap_peer_count: u128,
    max_bitswap_session_peer_count: u128,
    provider_diversity_low_events: usize,
    provider_diversity_low_failures: usize,
    provider_diversity_low_max_provider_count: u128,
    provider_diversity_low_max_bitswap_provider_count: u128,
    provider_diversity_low_max_timeout_ms: u128,
    dht_provider_lookup_events: usize,
    dht_provider_lookup_failures: usize,
    dht_provider_lookup_providers: u128,
    dht_provider_lookup_max_elapsed_ms: u128,
    dht_provider_lookup_max_timeout_ms: u128,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
}

#[derive(Debug, Serialize)]
struct TraceProgressRequestGroupAggregate {
    top_level_path: String,
    root_progress_request_id: Option<String>,
    request_count: usize,
    child_request_count: usize,
    completed_request_count: usize,
    failed_request_count: usize,
    request_elapsed_ms: LatencySummary,
    max_event_ms: u128,
    statuses: Vec<TraceValueCount>,
    phases: Vec<TraceValueCount>,
    slow_requests: Vec<TraceProgressRequestAggregate>,
}

#[derive(Debug, Serialize)]
struct TraceProgressRequestAggregate {
    path: String,
    progress_request_id: Option<String>,
    parent_progress_request_id: Option<String>,
    status: Option<String>,
    elapsed_ms: u128,
    max_event_ms: u128,
    block_sources: Vec<TraceValueCount>,
    http_provider_fetch_providers: Vec<TraceValueCount>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TraceRequestKey {
    process_id: String,
    request_id: String,
    path: String,
}

#[derive(Debug)]
struct TraceRequestBuilder {
    path: String,
    process_id: String,
    request_id: String,
    progress_request_id: Option<String>,
    parent_progress_request_id: Option<String>,
    top_level_path: Option<String>,
    status: Option<String>,
    elapsed_ms: Option<u128>,
    max_event_ms: u128,
    event_count: usize,
    phases: BTreeMap<String, usize>,
    phase_elapsed_values: BTreeMap<String, Vec<u128>>,
    cids: BTreeMap<String, usize>,
    block_sources: BTreeMap<String, usize>,
    http_provider_fetches: usize,
    http_provider_fetch_successes: usize,
    http_provider_fetch_failures: usize,
    http_provider_fetch_elapsed_values: Vec<u128>,
    http_provider_fetch_response_bytes: u128,
    http_provider_fetch_first_chunk_events: usize,
    http_provider_fetch_headers_elapsed_values: Vec<u128>,
    http_provider_fetch_first_chunk_elapsed_values: Vec<u128>,
    http_provider_fetch_body_elapsed_values: Vec<u128>,
    http_provider_fetch_providers: BTreeMap<String, usize>,
    http_provider_fetch_error_classes: BTreeMap<String, usize>,
    bitswap_fetches: usize,
    bitswap_fetch_elapsed_values: Vec<u128>,
    bitswap_fetch_bytes: u128,
    bitswap_source_candidate_indexes: BTreeMap<String, usize>,
    bitswap_source_request_modes: BTreeMap<String, usize>,
    bitswap_source_peers: BTreeMap<String, usize>,
    bitswap_source_transports: BTreeMap<String, usize>,
    delegated_zero_http_provider_lookups: usize,
    bitswap_block_fetches: usize,
    cold_bitswap_peer_expands: usize,
    max_bitswap_peer_count: u128,
    max_bitswap_session_peer_count: u128,
    provider_diversity_low_events: usize,
    provider_diversity_low_failures: usize,
    provider_diversity_low_max_provider_count: u128,
    provider_diversity_low_max_bitswap_provider_count: u128,
    provider_diversity_low_max_timeout_ms: u128,
    dht_provider_lookup_events: usize,
    dht_provider_lookup_failures: usize,
    dht_provider_lookup_providers: u128,
    dht_provider_lookup_max_elapsed_ms: u128,
    dht_provider_lookup_max_timeout_ms: u128,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
}

#[derive(Default)]
struct TraceRequestPathBuilder {
    path: String,
    request_count: usize,
    request_elapsed_values: Vec<u128>,
    max_event_ms: u128,
    statuses: BTreeMap<String, usize>,
    classifications: BTreeMap<String, usize>,
    block_sources: BTreeMap<String, usize>,
    http_provider_fetches: usize,
    http_provider_fetch_successes: usize,
    http_provider_fetch_failures: usize,
    http_provider_fetch_max_ms: u128,
    http_provider_fetch_response_bytes: u128,
    http_provider_fetch_first_chunk_events: usize,
    http_provider_fetch_headers_max_ms: u128,
    http_provider_fetch_first_chunk_max_ms: u128,
    http_provider_fetch_body_max_ms: u128,
    http_provider_fetch_providers: BTreeMap<String, usize>,
    http_provider_fetch_error_classes: BTreeMap<String, usize>,
    bitswap_fetches: usize,
    bitswap_fetch_max_ms: u128,
    bitswap_fetch_bytes: u128,
    bitswap_source_candidate_indexes: BTreeMap<String, usize>,
    bitswap_source_request_modes: BTreeMap<String, usize>,
    bitswap_source_peers: BTreeMap<String, usize>,
    bitswap_source_transports: BTreeMap<String, usize>,
    provider_diversity_low_events: usize,
    provider_diversity_low_failures: usize,
    provider_diversity_low_max_provider_count: u128,
    provider_diversity_low_max_bitswap_provider_count: u128,
    provider_diversity_low_max_timeout_ms: u128,
    dht_provider_lookup_events: usize,
    dht_provider_lookup_failures: usize,
    dht_provider_lookup_providers: u128,
    dht_provider_lookup_max_elapsed_ms: u128,
    dht_provider_lookup_max_timeout_ms: u128,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
    phase_latencies: BTreeMap<String, TraceRequestPathPhaseBuilder>,
}

#[derive(Default)]
struct TraceRequestPathPhaseBuilder {
    count: usize,
    total_ms: u128,
    max_ms: u128,
}

impl TraceRequestPathBuilder {
    fn record(&mut self, request: &TraceRequestAggregate) {
        if self.path.is_empty() {
            self.path = request.path.clone();
        }
        self.request_count += 1;
        self.request_elapsed_values.push(request.elapsed_ms);
        self.max_event_ms = self.max_event_ms.max(request.max_event_ms);
        match &request.status {
            Some(status) => *self.statuses.entry(status.clone()).or_default() += 1,
            None => *self.statuses.entry("active".to_string()).or_default() += 1,
        }
        merge_trace_counts(&mut self.classifications, &request.classifications);
        merge_trace_counts(&mut self.block_sources, &request.block_sources);
        self.http_provider_fetches += request.http_provider_fetches;
        self.http_provider_fetch_successes += request.http_provider_fetch_successes;
        self.http_provider_fetch_failures += request.http_provider_fetch_failures;
        self.http_provider_fetch_response_bytes += request.http_provider_fetch_response_bytes;
        self.http_provider_fetch_first_chunk_events +=
            request.http_provider_fetch_first_chunk_events;
        self.http_provider_fetch_max_ms = self.http_provider_fetch_max_ms.max(
            request
                .http_provider_fetch_elapsed_ms
                .max_ms
                .unwrap_or_default(),
        );
        self.http_provider_fetch_headers_max_ms = self.http_provider_fetch_headers_max_ms.max(
            request
                .http_provider_fetch_headers_elapsed_ms
                .max_ms
                .unwrap_or_default(),
        );
        self.http_provider_fetch_first_chunk_max_ms =
            self.http_provider_fetch_first_chunk_max_ms.max(
                request
                    .http_provider_fetch_first_chunk_elapsed_ms
                    .max_ms
                    .unwrap_or_default(),
            );
        self.http_provider_fetch_body_max_ms = self.http_provider_fetch_body_max_ms.max(
            request
                .http_provider_fetch_body_elapsed_ms
                .max_ms
                .unwrap_or_default(),
        );
        merge_trace_counts(
            &mut self.http_provider_fetch_providers,
            &request.http_provider_fetch_providers,
        );
        merge_trace_counts(
            &mut self.http_provider_fetch_error_classes,
            &request.http_provider_fetch_error_classes,
        );
        self.bitswap_fetches += request.bitswap_fetches;
        self.bitswap_fetch_bytes += request.bitswap_fetch_bytes;
        self.bitswap_fetch_max_ms = self
            .bitswap_fetch_max_ms
            .max(request.bitswap_fetch_elapsed_ms.max_ms.unwrap_or_default());
        merge_trace_counts(
            &mut self.bitswap_source_candidate_indexes,
            &request.bitswap_source_candidate_indexes,
        );
        merge_trace_counts(
            &mut self.bitswap_source_request_modes,
            &request.bitswap_source_request_modes,
        );
        merge_trace_counts(
            &mut self.bitswap_source_peers,
            &request.bitswap_source_peers,
        );
        merge_trace_counts(
            &mut self.bitswap_source_transports,
            &request.bitswap_source_transports,
        );
        self.provider_diversity_low_events += request.provider_diversity_low_events;
        self.provider_diversity_low_failures += request.provider_diversity_low_failures;
        self.provider_diversity_low_max_provider_count = self
            .provider_diversity_low_max_provider_count
            .max(request.provider_diversity_low_max_provider_count);
        self.provider_diversity_low_max_bitswap_provider_count = self
            .provider_diversity_low_max_bitswap_provider_count
            .max(request.provider_diversity_low_max_bitswap_provider_count);
        self.provider_diversity_low_max_timeout_ms = self
            .provider_diversity_low_max_timeout_ms
            .max(request.provider_diversity_low_max_timeout_ms);
        self.dht_provider_lookup_events += request.dht_provider_lookup_events;
        self.dht_provider_lookup_failures += request.dht_provider_lookup_failures;
        self.dht_provider_lookup_providers += request.dht_provider_lookup_providers;
        self.dht_provider_lookup_max_elapsed_ms = self
            .dht_provider_lookup_max_elapsed_ms
            .max(request.dht_provider_lookup_max_elapsed_ms);
        self.dht_provider_lookup_max_timeout_ms = self
            .dht_provider_lookup_max_timeout_ms
            .max(request.dht_provider_lookup_max_timeout_ms);
        self.unixfs_metadata_cache
            .merge(&request.unixfs_metadata_cache);
        for phase in &request.phase_latencies {
            let builder = self.phase_latencies.entry(phase.phase.clone()).or_default();
            builder.count += phase.count;
            builder.total_ms += phase.total_ms;
            builder.max_ms = builder
                .max_ms
                .max(phase.elapsed_ms.max_ms.unwrap_or_default());
        }
    }

    fn into_aggregate(self) -> TraceRequestPathAggregate {
        TraceRequestPathAggregate {
            path: self.path,
            request_count: self.request_count,
            request_elapsed_ms: LatencySummary::from_values(self.request_elapsed_values),
            max_event_ms: self.max_event_ms,
            statuses: sorted_trace_counts(self.statuses),
            classifications: sorted_trace_counts(self.classifications),
            block_sources: sorted_trace_counts(self.block_sources),
            http_provider_fetches: self.http_provider_fetches,
            http_provider_fetch_successes: self.http_provider_fetch_successes,
            http_provider_fetch_failures: self.http_provider_fetch_failures,
            http_provider_fetch_max_ms: self.http_provider_fetch_max_ms,
            http_provider_fetch_response_bytes: self.http_provider_fetch_response_bytes,
            http_provider_fetch_first_chunk_events: self.http_provider_fetch_first_chunk_events,
            http_provider_fetch_headers_max_ms: self.http_provider_fetch_headers_max_ms,
            http_provider_fetch_first_chunk_max_ms: self.http_provider_fetch_first_chunk_max_ms,
            http_provider_fetch_body_max_ms: self.http_provider_fetch_body_max_ms,
            http_provider_fetch_providers: sorted_trace_counts(self.http_provider_fetch_providers),
            http_provider_fetch_error_classes: sorted_trace_counts(
                self.http_provider_fetch_error_classes,
            ),
            bitswap_fetches: self.bitswap_fetches,
            bitswap_fetch_max_ms: self.bitswap_fetch_max_ms,
            bitswap_fetch_bytes: self.bitswap_fetch_bytes,
            bitswap_source_candidate_indexes: sorted_trace_counts(
                self.bitswap_source_candidate_indexes,
            ),
            bitswap_source_request_modes: sorted_trace_counts(self.bitswap_source_request_modes),
            bitswap_source_peers: sorted_trace_counts(self.bitswap_source_peers),
            bitswap_source_transports: sorted_trace_counts(self.bitswap_source_transports),
            provider_diversity_low_events: self.provider_diversity_low_events,
            provider_diversity_low_failures: self.provider_diversity_low_failures,
            provider_diversity_low_max_provider_count: self
                .provider_diversity_low_max_provider_count,
            provider_diversity_low_max_bitswap_provider_count: self
                .provider_diversity_low_max_bitswap_provider_count,
            provider_diversity_low_max_timeout_ms: self.provider_diversity_low_max_timeout_ms,
            dht_provider_lookup_events: self.dht_provider_lookup_events,
            dht_provider_lookup_failures: self.dht_provider_lookup_failures,
            dht_provider_lookup_providers: self.dht_provider_lookup_providers,
            dht_provider_lookup_max_elapsed_ms: self.dht_provider_lookup_max_elapsed_ms,
            dht_provider_lookup_max_timeout_ms: self.dht_provider_lookup_max_timeout_ms,
            unixfs_metadata_cache: self.unixfs_metadata_cache,
            phase_latencies: sorted_trace_request_path_phase_latencies(self.phase_latencies),
        }
    }
}

impl TraceRequestBuilder {
    fn new(key: TraceRequestKey) -> Self {
        Self {
            path: key.path,
            process_id: key.process_id,
            request_id: key.request_id,
            progress_request_id: None,
            parent_progress_request_id: None,
            top_level_path: None,
            status: None,
            elapsed_ms: None,
            max_event_ms: 0,
            event_count: 0,
            phases: BTreeMap::new(),
            phase_elapsed_values: BTreeMap::new(),
            cids: BTreeMap::new(),
            block_sources: BTreeMap::new(),
            http_provider_fetches: 0,
            http_provider_fetch_successes: 0,
            http_provider_fetch_failures: 0,
            http_provider_fetch_elapsed_values: Vec::new(),
            http_provider_fetch_response_bytes: 0,
            http_provider_fetch_first_chunk_events: 0,
            http_provider_fetch_headers_elapsed_values: Vec::new(),
            http_provider_fetch_first_chunk_elapsed_values: Vec::new(),
            http_provider_fetch_body_elapsed_values: Vec::new(),
            http_provider_fetch_providers: BTreeMap::new(),
            http_provider_fetch_error_classes: BTreeMap::new(),
            bitswap_fetches: 0,
            bitswap_fetch_elapsed_values: Vec::new(),
            bitswap_fetch_bytes: 0,
            bitswap_source_candidate_indexes: BTreeMap::new(),
            bitswap_source_request_modes: BTreeMap::new(),
            bitswap_source_peers: BTreeMap::new(),
            bitswap_source_transports: BTreeMap::new(),
            delegated_zero_http_provider_lookups: 0,
            bitswap_block_fetches: 0,
            cold_bitswap_peer_expands: 0,
            max_bitswap_peer_count: 0,
            max_bitswap_session_peer_count: 0,
            provider_diversity_low_events: 0,
            provider_diversity_low_failures: 0,
            provider_diversity_low_max_provider_count: 0,
            provider_diversity_low_max_bitswap_provider_count: 0,
            provider_diversity_low_max_timeout_ms: 0,
            dht_provider_lookup_events: 0,
            dht_provider_lookup_failures: 0,
            dht_provider_lookup_providers: 0,
            dht_provider_lookup_max_elapsed_ms: 0,
            dht_provider_lookup_max_timeout_ms: 0,
            unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate::default(),
        }
    }

    fn record_event(&mut self, phase: &str, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.event_count += 1;
        *self.phases.entry(phase.to_string()).or_default() += 1;
        if self.progress_request_id.is_none() {
            self.progress_request_id = trace_span_string(value, "progress_request_id");
        }
        if self.parent_progress_request_id.is_none() {
            self.parent_progress_request_id = trace_parent_progress_request_id(value);
        }
        if self.top_level_path.is_none() {
            self.top_level_path = trace_span_string(value, "top_level_path");
        }
        if let Some(cid) = json_detail_string(value.get("cid")) {
            *self.cids.entry(cid).or_default() += 1;
        }
        if phase == "delegated_provider_lookup"
            && value.get("http_provider_count").and_then(json_u128) == Some(0)
        {
            self.delegated_zero_http_provider_lookups += 1;
        }
        if phase == "provider_diversity_low" {
            self.provider_diversity_low_events += 1;
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
                self.provider_diversity_low_failures += 1;
            }
            self.provider_diversity_low_max_provider_count =
                self.provider_diversity_low_max_provider_count.max(
                    value
                        .get("provider_count")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            self.provider_diversity_low_max_bitswap_provider_count =
                self.provider_diversity_low_max_bitswap_provider_count.max(
                    value
                        .get("bitswap_provider_count")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            self.provider_diversity_low_max_timeout_ms =
                self.provider_diversity_low_max_timeout_ms.max(
                    value
                        .get("timeout_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
        }
        if phase == "dht_provider_lookup" {
            self.dht_provider_lookup_events += 1;
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
                self.dht_provider_lookup_failures += 1;
            }
            self.dht_provider_lookup_providers += value
                .get("provider_count")
                .and_then(json_u128)
                .unwrap_or_default();
            if let Some(elapsed_ms) = elapsed_ms {
                self.dht_provider_lookup_max_elapsed_ms =
                    self.dht_provider_lookup_max_elapsed_ms.max(elapsed_ms);
            }
            self.dht_provider_lookup_max_timeout_ms = self.dht_provider_lookup_max_timeout_ms.max(
                value
                    .get("timeout_ms")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
        }
        if matches!(phase, "block_fetch_total" | "block_range_batch_fetch") {
            if let Some(source) = json_detail_string(value.get("source")) {
                if !source.is_empty() {
                    *self.block_sources.entry(source).or_default() += 1;
                }
            }
        }
        if phase == "block_fetch_total"
            && value.get("source").and_then(|source| source.as_str()) == Some("bitswap")
        {
            self.bitswap_block_fetches += 1;
        }
        if phase == "http_provider_fetch" {
            self.http_provider_fetches += 1;
            match value.get("ok").and_then(|ok| ok.as_bool()) {
                Some(true) => self.http_provider_fetch_successes += 1,
                Some(false) => self.http_provider_fetch_failures += 1,
                None => {}
            }
            self.http_provider_fetch_response_bytes += value
                .get("response_bytes")
                .and_then(json_u128)
                .unwrap_or_default();
            if value
                .get("response_first_chunk_seen")
                .and_then(|seen| seen.as_bool())
                == Some(true)
            {
                self.http_provider_fetch_first_chunk_events += 1;
            }
            if let Some(elapsed_ms) = elapsed_ms {
                self.http_provider_fetch_elapsed_values.push(elapsed_ms);
            }
            if let Some(headers_elapsed_ms) =
                value.get("response_headers_elapsed_ms").and_then(json_u128)
            {
                self.http_provider_fetch_headers_elapsed_values
                    .push(headers_elapsed_ms);
            }
            if let Some(first_chunk_elapsed_ms) = value
                .get("response_first_chunk_elapsed_ms")
                .and_then(json_u128)
            {
                self.http_provider_fetch_first_chunk_elapsed_values
                    .push(first_chunk_elapsed_ms);
            }
            if let Some(body_elapsed_ms) = value.get("response_body_elapsed_ms").and_then(json_u128)
            {
                self.http_provider_fetch_body_elapsed_values
                    .push(body_elapsed_ms);
            }
            if let Some(provider) = json_detail_string(value.get("provider")) {
                if !provider.is_empty() {
                    *self
                        .http_provider_fetch_providers
                        .entry(provider)
                        .or_default() += 1;
                }
            }
            if let Some(error) = json_detail_string(value.get("error")) {
                *self
                    .http_provider_fetch_error_classes
                    .entry(http_provider_error_class(&error).to_string())
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_fetch" && value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
            self.bitswap_fetches += 1;
            self.bitswap_fetch_bytes += value.get("bytes").and_then(json_u128).unwrap_or_default();
            if let Some(elapsed_ms) = elapsed_ms {
                self.bitswap_fetch_elapsed_values.push(elapsed_ms);
            }
            if let Some(index) = json_detail_string(value.get("source_peer_candidate_index")) {
                if index != "-1" {
                    *self
                        .bitswap_source_candidate_indexes
                        .entry(index)
                        .or_default() += 1;
                }
            }
            if let Some(mode) = json_detail_string(value.get("source_peer_request_mode")) {
                if !mode.is_empty() {
                    *self.bitswap_source_request_modes.entry(mode).or_default() += 1;
                }
            }
            if let Some(peer) = json_detail_string(value.get("source_peer")) {
                if !peer.is_empty() {
                    *self.bitswap_source_peers.entry(peer).or_default() += 1;
                }
            }
            if let Some(transport) = json_detail_string(value.get("source_transport")) {
                if !transport.is_empty() {
                    *self.bitswap_source_transports.entry(transport).or_default() += 1;
                }
            }
        }
        if phase == "unixfs_metadata_cache" {
            self.unixfs_metadata_cache.record(value);
        }
        if phase == "bitswap_peer_expand" {
            let peer_count = value
                .get("peer_count")
                .and_then(json_u128)
                .or_else(|| {
                    value
                        .get("supported_provider_addr_count")
                        .and_then(json_u128)
                })
                .unwrap_or_default();
            let session_peer_count = value
                .get("session_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            self.max_bitswap_peer_count = self.max_bitswap_peer_count.max(peer_count);
            self.max_bitswap_session_peer_count =
                self.max_bitswap_session_peer_count.max(session_peer_count);
            if session_peer_count == 0 {
                self.cold_bitswap_peer_expands += 1;
            }
        }
        if phase == "request_done" {
            self.status = json_detail_string(value.get("status"));
            self.elapsed_ms = elapsed_ms;
        }
        if let Some(elapsed_ms) = elapsed_ms {
            self.max_event_ms = self.max_event_ms.max(elapsed_ms);
            self.phase_elapsed_values
                .entry(phase.to_string())
                .or_default()
                .push(elapsed_ms);
        }
    }

    fn classifications(&self) -> Vec<TraceValueCount> {
        let mut counts = BTreeMap::<String, usize>::new();
        let zero_http_bitswap =
            self.delegated_zero_http_provider_lookups > 0 && self.bitswap_block_fetches > 0;
        let single_bitswap_provider = zero_http_bitswap && self.max_bitswap_peer_count == 1;
        let has_wss_source = self
            .bitswap_source_transports
            .get("wss")
            .copied()
            .unwrap_or_default()
            > 0;
        let has_non_wss_source = self
            .bitswap_source_transports
            .iter()
            .any(|(transport, count)| transport != "wss" && *count > 0);
        let wss_only_source = has_wss_source && !has_non_wss_source;
        let low_diversity_dht_fallback =
            self.provider_diversity_low_events > 0 && self.dht_provider_lookup_events > 0;
        let low_diversity_dht_fallback_empty = low_diversity_dht_fallback
            && self.dht_provider_lookup_providers == 0
            && self.dht_provider_lookup_failures > 0;
        if zero_http_bitswap {
            counts.insert("zero_http_provider_bitswap".to_string(), 1);
        }
        if zero_http_bitswap && self.cold_bitswap_peer_expands > 0 {
            counts.insert("zero_http_provider_cold_bitswap".to_string(), 1);
        }
        if single_bitswap_provider {
            counts.insert("zero_http_single_bitswap_provider".to_string(), 1);
        }
        if single_bitswap_provider && wss_only_source {
            counts.insert("zero_http_single_wss_bitswap".to_string(), 1);
        }
        if low_diversity_dht_fallback {
            counts.insert("low_diversity_dht_fallback".to_string(), 1);
        }
        if low_diversity_dht_fallback_empty {
            counts.insert("low_diversity_dht_fallback_empty".to_string(), 1);
        }
        if single_bitswap_provider && wss_only_source && low_diversity_dht_fallback_empty {
            counts.insert("zero_http_single_wss_bitswap_dht_empty".to_string(), 1);
        }
        if self.cold_bitswap_peer_expands > 0 {
            counts.insert("cold_bitswap_peer_expand".to_string(), 1);
        }
        let is_top_level = self.parent_progress_request_id.is_none()
            && self
                .top_level_path
                .as_deref()
                .map(|top_level_path| top_level_path == self.path)
                .unwrap_or(true);
        if is_top_level && zero_http_bitswap {
            counts.insert("top_level_zero_http_provider_bitswap".to_string(), 1);
        }
        if is_top_level && zero_http_bitswap && self.cold_bitswap_peer_expands > 0 {
            counts.insert("top_level_zero_http_provider_cold_bitswap".to_string(), 1);
        }
        if is_top_level && single_bitswap_provider && wss_only_source {
            counts.insert("top_level_zero_http_single_wss_bitswap".to_string(), 1);
        }
        if is_top_level
            && single_bitswap_provider
            && wss_only_source
            && low_diversity_dht_fallback_empty
        {
            counts.insert(
                "top_level_zero_http_single_wss_bitswap_dht_empty".to_string(),
                1,
            );
        }
        sorted_trace_counts(counts)
    }

    fn into_aggregate(self) -> TraceRequestAggregate {
        let classifications = self.classifications();
        TraceRequestAggregate {
            path: self.path,
            process_id: self.process_id,
            request_id: self.request_id,
            progress_request_id: self.progress_request_id,
            parent_progress_request_id: self.parent_progress_request_id,
            top_level_path: self.top_level_path,
            status: self.status,
            elapsed_ms: self.elapsed_ms.unwrap_or(self.max_event_ms),
            max_event_ms: self.max_event_ms,
            event_count: self.event_count,
            phases: sorted_trace_counts(self.phases),
            phase_latencies: sorted_trace_phase_latencies(self.phase_elapsed_values),
            cids: sorted_trace_counts(self.cids),
            block_sources: sorted_trace_counts(self.block_sources),
            http_provider_fetches: self.http_provider_fetches,
            http_provider_fetch_successes: self.http_provider_fetch_successes,
            http_provider_fetch_failures: self.http_provider_fetch_failures,
            http_provider_fetch_elapsed_ms: LatencySummary::from_values(
                self.http_provider_fetch_elapsed_values,
            ),
            http_provider_fetch_response_bytes: self.http_provider_fetch_response_bytes,
            http_provider_fetch_first_chunk_events: self.http_provider_fetch_first_chunk_events,
            http_provider_fetch_headers_elapsed_ms: LatencySummary::from_values(
                self.http_provider_fetch_headers_elapsed_values,
            ),
            http_provider_fetch_first_chunk_elapsed_ms: LatencySummary::from_values(
                self.http_provider_fetch_first_chunk_elapsed_values,
            ),
            http_provider_fetch_body_elapsed_ms: LatencySummary::from_values(
                self.http_provider_fetch_body_elapsed_values,
            ),
            http_provider_fetch_providers: sorted_trace_counts(self.http_provider_fetch_providers),
            http_provider_fetch_error_classes: sorted_trace_counts(
                self.http_provider_fetch_error_classes,
            ),
            bitswap_fetches: self.bitswap_fetches,
            bitswap_fetch_elapsed_ms: LatencySummary::from_values(
                self.bitswap_fetch_elapsed_values,
            ),
            bitswap_fetch_bytes: self.bitswap_fetch_bytes,
            bitswap_source_candidate_indexes: sorted_trace_counts(
                self.bitswap_source_candidate_indexes,
            ),
            bitswap_source_request_modes: sorted_trace_counts(self.bitswap_source_request_modes),
            bitswap_source_peers: sorted_trace_counts(self.bitswap_source_peers),
            bitswap_source_transports: sorted_trace_counts(self.bitswap_source_transports),
            classifications,
            delegated_zero_http_provider_lookups: self.delegated_zero_http_provider_lookups,
            bitswap_block_fetches: self.bitswap_block_fetches,
            cold_bitswap_peer_expands: self.cold_bitswap_peer_expands,
            max_bitswap_peer_count: self.max_bitswap_peer_count,
            max_bitswap_session_peer_count: self.max_bitswap_session_peer_count,
            provider_diversity_low_events: self.provider_diversity_low_events,
            provider_diversity_low_failures: self.provider_diversity_low_failures,
            provider_diversity_low_max_provider_count: self
                .provider_diversity_low_max_provider_count,
            provider_diversity_low_max_bitswap_provider_count: self
                .provider_diversity_low_max_bitswap_provider_count,
            provider_diversity_low_max_timeout_ms: self.provider_diversity_low_max_timeout_ms,
            dht_provider_lookup_events: self.dht_provider_lookup_events,
            dht_provider_lookup_failures: self.dht_provider_lookup_failures,
            dht_provider_lookup_providers: self.dht_provider_lookup_providers,
            dht_provider_lookup_max_elapsed_ms: self.dht_provider_lookup_max_elapsed_ms,
            dht_provider_lookup_max_timeout_ms: self.dht_provider_lookup_max_timeout_ms,
            unixfs_metadata_cache: self.unixfs_metadata_cache,
        }
    }
}

#[derive(Debug, Serialize)]
struct TracePeerAggregate {
    peer: String,
    count: usize,
    total_ms: u128,
    max_ms: u128,
    bytes: u128,
    transports: Vec<TraceValueCount>,
}

#[derive(Default)]
struct TraceCidBuilder {
    count: usize,
    total_ms: u128,
    max_ms: u128,
    phases: BTreeMap<String, usize>,
    paths: BTreeMap<String, usize>,
    bitswap_source_candidate_indexes: BTreeMap<String, usize>,
    bitswap_source_peers: BTreeMap<String, usize>,
}

#[derive(Default)]
struct TracePeerBuilder {
    count: usize,
    total_ms: u128,
    max_ms: u128,
    bytes: u128,
    transports: BTreeMap<String, usize>,
}

fn summarize_trace_output(path: &PathBuf) -> Result<TraceSummary> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read trace output {}", path.display()))?;
    let mut line_count = 0usize;
    let mut event_count = 0usize;
    let mut event_phases = BTreeMap::<String, usize>::new();
    let mut phases = BTreeMap::<String, Vec<u128>>::new();
    let mut progress_phases = BTreeMap::<String, usize>::new();
    let mut slow_events = Vec::<TraceSlowEvent>::new();
    let mut block_sources = BTreeMap::<String, usize>::new();
    let mut block_fetch_source_latencies = BTreeMap::<String, Vec<u128>>::new();
    let mut block_store = TraceBlockStoreAggregate::default();
    let mut block_range_batch_fetches = TraceBlockRangeBatchFetchAggregate::default();
    let mut provider_retries = TraceProviderRetryAggregate::default();
    let mut delegated_provider_lookup = TraceDelegatedProviderLookupAggregate::default();
    let mut delegated_provider_lookup_by_endpoint =
        BTreeMap::<String, TraceDelegatedProviderLookupAggregate>::new();
    let mut dht_provider_lookup = TraceDhtProviderLookupAggregate::default();
    let mut provider_diversity_low = TraceProviderDiversityLowBuilder::default();
    let mut request_statuses = BTreeMap::<String, usize>::new();
    let mut gateway_limiter_denials = 0usize;
    let mut gateway_limiter_events = 0usize;
    let mut gateway_limiter_acquired = 0usize;
    let mut gateway_limiter_elapsed_values = Vec::<u128>::new();
    let mut gateway_limiter_denied_elapsed_values = Vec::<u128>::new();
    let mut gateway_limiter_max_timeout_ms = 0u128;
    let mut gateway_request_elapsed_values = Vec::<u128>::new();
    let mut gateway_small_body_cache = TraceGatewaySmallBodyCacheAggregate::default();
    let mut gateway_direct_body = TraceGatewayDirectBodyAggregate::default();
    let mut gateway_stream_body = TraceGatewayStreamBodyAggregate::default();
    let mut http_provider_races = TraceHttpProviderRaceAggregate::default();
    let mut http_provider_fetch_events = 0usize;
    let mut http_provider_fetch_successes = 0usize;
    let mut http_provider_fetch_failures = 0usize;
    let mut http_provider_fetch_bytes = 0u128;
    let mut http_provider_fetch_response_bytes = 0u128;
    let mut http_provider_fetch_first_chunk_events = 0usize;
    let mut http_provider_fetch_max_response_headers_elapsed_ms = 0u128;
    let mut http_provider_fetch_max_response_first_chunk_elapsed_ms = 0u128;
    let mut http_provider_fetch_max_response_body_elapsed_ms = 0u128;
    let mut http_provider_fetch_elapsed_values = Vec::<u128>::new();
    let mut http_provider_fetch_providers = BTreeMap::<String, usize>::new();
    let mut http_provider_fetch_provider_milestones =
        BTreeMap::<String, TraceHttpProviderMilestoneBuilder>::new();
    let mut http_provider_fetch_error_classes = BTreeMap::<String, usize>::new();
    let mut unixfs_metadata_cache = TraceUnixfsMetadataCacheAggregate::default();
    let mut bitswap_source_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_source_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_source_request_modes = BTreeMap::<String, usize>::new();
    let mut bitswap_source_candidate_indexes = BTreeMap::<String, usize>::new();
    let mut bitswap_source_addr_indexes = BTreeMap::<String, usize>::new();
    let mut bitswap_source_addr_families = BTreeMap::<String, usize>::new();
    let mut bitswap_source_addr_match_statuses = BTreeMap::<String, usize>::new();
    let mut bitswap_attempt_modes = BTreeMap::<(String, String), String>::new();
    let mut bitswap_deliveries = BTreeMap::<String, usize>::new();
    let mut bitswap_batches = TraceBitswapBatchAggregate::default();
    let mut bitswap_extra_blocks = TraceBitswapExtraBlockAggregate::default();
    let mut bitswap_incoming_batches = TraceBitswapIncomingBatchAggregate::default();
    let mut bitswap_peer_fetches = BTreeMap::<String, TracePeerBuilder>::new();
    let mut trace_errors = BTreeMap::<String, usize>::new();
    let mut bitswap_addr_mix = BTreeMap::<String, usize>::new();
    let mut bitswap_provider_quality = TraceBitswapProviderQualityAggregate::default();
    let mut bitswap_connection_established_events = 0usize;
    let mut bitswap_connection_established_ms_values = Vec::<u128>::new();
    let mut bitswap_connection_wait_elapsed_ms_values = Vec::<u128>::new();
    let mut bitswap_connection_failed_dial_count = 0u128;
    let mut bitswap_connection_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_error_events = 0usize;
    let mut bitswap_connection_error_with_peer = 0usize;
    let mut bitswap_connection_error_without_peer = 0usize;
    let mut bitswap_connection_error_classes = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_error_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_error_addr_families = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_backoffs = 0usize;
    let mut bitswap_connection_backoff_skips = 0usize;
    let mut bitswap_connection_backoff_classes = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_backoff_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_backoff_skipped_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_dial_rejections = TraceBitswapDialRejectedAggregate::default();
    let mut bitswap_dial_rejected_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_dns_expansion = TraceBitswapDnsExpansionAggregate::default();
    let mut bitswap_session = TraceBitswapSessionAggregate::default();
    let mut bitswap_peer_attempts = TraceBitswapPeerAttemptAggregate::default();
    let mut bitswap_peer_attempt_cancelled_stages = BTreeMap::<String, usize>::new();
    let mut bitswap_peer_attempt_cancelled_candidate_indexes = BTreeMap::<String, usize>::new();
    let mut bitswap_peer_attempt_cancelled_request_modes = BTreeMap::<String, usize>::new();
    let mut bitswap_peer_attempt_cancelled_first_addr_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_peer_attempt_cancelled_first_addr_families = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_events = 0usize;
    let mut bitswap_want_have_probe_ok = 0usize;
    let mut bitswap_want_have_probe_failures = 0usize;
    let mut bitswap_want_have_probe_have = 0usize;
    let mut bitswap_want_have_probe_dont_have = 0usize;
    let mut bitswap_want_have_probe_block = 0usize;
    let mut bitswap_want_have_probe_want_block_followups = 0usize;
    let mut bitswap_want_have_probe_no_presence = 0usize;
    let mut bitswap_want_have_probe_bytes = 0u128;
    let mut bitswap_want_have_probe_extra_blocks = 0u128;
    let mut bitswap_want_have_probe_max_timeout_ms = 0u128;
    let mut bitswap_want_have_probe_elapsed_values = Vec::<u128>::new();
    let mut bitswap_want_have_probe_outcomes = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_candidate_indexes = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_request_modes = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_first_addr_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_first_addr_families = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_target_peer_counts = BTreeMap::<String, usize>::new();
    let mut bitswap_want_have_probe_peer_addr_counts = BTreeMap::<String, usize>::new();
    let mut bitswap_dial_plans = TraceBitswapDialPlanAggregate::default();
    let mut bitswap_incoming_blocks = TraceBitswapIncomingBlockAggregate::default();
    let mut bitswap_incoming_reads = TraceBitswapIncomingReadAggregate::default();
    let mut bitswap_timeout_recovery = TraceBitswapTimeoutRecoveryBuilder::default();
    let mut provider_fetch_dial_plan_seen = BTreeMap::<String, bool>::new();
    let mut pending_request_timeout_retries = BTreeMap::<String, usize>::new();
    let mut slow_cids = BTreeMap::<String, TraceCidBuilder>::new();
    let mut active_requests = BTreeMap::<TraceRequestKey, TraceRequestBuilder>::new();
    let mut completed_requests = BTreeMap::<TraceRequestKey, TraceRequestBuilder>::new();

    for line in text.lines() {
        line_count += 1;
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(phase) = value.get("phase").and_then(|phase| phase.as_str()) else {
            continue;
        };
        event_count += 1;
        *event_phases.entry(phase.to_string()).or_default() += 1;
        let progress_phase = trace_progress_phase(phase, &value);
        *progress_phases
            .entry(progress_phase.to_string())
            .or_default() += 1;
        let elapsed_ms = value.get("elapsed_ms").and_then(json_u128);
        let request_key = trace_request_key(&value);
        if phase == "request_start" {
            if let Some(key) = request_key.clone() {
                active_requests
                    .entry(key.clone())
                    .or_insert_with(|| TraceRequestBuilder::new(key));
            }
        }
        if let Some(key) = request_key.as_ref() {
            if let Some(request) = active_requests.get_mut(key) {
                request.record_event(phase, &value, elapsed_ms);
            } else if let Some(request) = completed_requests.get_mut(key) {
                request.record_event(phase, &value, elapsed_ms);
            } else if phase == "request_done" {
                let mut request = TraceRequestBuilder::new(key.clone());
                request.record_event(phase, &value, elapsed_ms);
                completed_requests.insert(key.clone(), request);
            }
        }
        if phase == "request_done" {
            if let Some(key) = request_key {
                if let Some(request) = active_requests.remove(&key) {
                    completed_requests.insert(key, request);
                }
            }
        }
        if phase == "block_fetch_total" {
            if let Some(source) = value.get("source").and_then(|source| source.as_str()) {
                *block_sources.entry(source.to_string()).or_default() += 1;
                if let Some(elapsed_ms) = elapsed_ms {
                    block_fetch_source_latencies
                        .entry(source.to_string())
                        .or_default()
                        .push(elapsed_ms);
                }
            }
        }
        if phase == "block_store_get" {
            block_store.events += 1;
            let cache_hit = value.get("cache_hit").and_then(|hit| hit.as_bool());
            match cache_hit {
                Some(true) => block_store.hits += 1,
                Some(false) => block_store.misses += 1,
                None => {}
            }
            if value
                .get("rechecked")
                .and_then(|rechecked| rechecked.as_bool())
                == Some(true)
            {
                block_store.rechecks += 1;
                match cache_hit {
                    Some(true) => block_store.recheck_hits += 1,
                    Some(false) => block_store.recheck_misses += 1,
                    None => {}
                }
            }
        }
        if phase == "block_store_put" {
            block_store.puts += 1;
            block_store.put_bytes += value.get("bytes").and_then(json_u128).unwrap_or_default();
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
                block_store.put_failures += 1;
            }
            if let Some(elapsed_ms) = elapsed_ms {
                block_store.put_total_ms += elapsed_ms;
                block_store.put_max_ms = block_store.put_max_ms.max(elapsed_ms);
            }
        }
        if phase == "block_range_batch_fetch" {
            block_range_batch_fetches.record(&value, elapsed_ms);
        }
        if phase == "http_provider_fetch" {
            http_provider_fetch_events += 1;
            match value.get("ok").and_then(|ok| ok.as_bool()) {
                Some(true) => http_provider_fetch_successes += 1,
                Some(false) => http_provider_fetch_failures += 1,
                None => {}
            }
            http_provider_fetch_bytes += value.get("bytes").and_then(json_u128).unwrap_or_default();
            http_provider_fetch_response_bytes += value
                .get("response_bytes")
                .and_then(json_u128)
                .unwrap_or_default();
            if value
                .get("response_first_chunk_seen")
                .and_then(|seen| seen.as_bool())
                == Some(true)
            {
                http_provider_fetch_first_chunk_events += 1;
            }
            http_provider_fetch_max_response_headers_elapsed_ms =
                http_provider_fetch_max_response_headers_elapsed_ms.max(
                    value
                        .get("response_headers_elapsed_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            http_provider_fetch_max_response_first_chunk_elapsed_ms =
                http_provider_fetch_max_response_first_chunk_elapsed_ms.max(
                    value
                        .get("response_first_chunk_elapsed_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            http_provider_fetch_max_response_body_elapsed_ms =
                http_provider_fetch_max_response_body_elapsed_ms.max(
                    value
                        .get("response_body_elapsed_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            if let Some(elapsed_ms) = elapsed_ms {
                http_provider_fetch_elapsed_values.push(elapsed_ms);
            }
            if let Some(provider) = json_detail_string(value.get("provider")) {
                *http_provider_fetch_providers
                    .entry(provider.clone())
                    .or_default() += 1;
                http_provider_fetch_provider_milestones
                    .entry(provider)
                    .or_default()
                    .record(&value, elapsed_ms);
            }
            if let Some(error) = json_detail_string(value.get("error")) {
                *http_provider_fetch_error_classes
                    .entry(http_provider_error_class(&error).to_string())
                    .or_default() += 1;
            }
        }
        if phase == "http_provider_race" {
            http_provider_races.record_race(&value);
        }
        if phase == "http_provider_hedge" {
            http_provider_races.record_hedge(&value);
        }
        if phase == "http_provider_self_hedge" {
            http_provider_races.record_self_hedge(&value);
        }
        if phase == "http_provider_self_hedge_skip" {
            http_provider_races.record_self_hedge_skip(&value);
        }
        if phase == "http_provider_candidate_cancelled" {
            http_provider_races.record_candidate_cancelled(&value);
        }
        if phase == "http_provider_bitswap_hedge" {
            http_provider_races.record_bitswap_hedge(&value);
        }
        if phase == "http_provider_bitswap_hedge_result" {
            http_provider_races.record_bitswap_hedge_result(&value, elapsed_ms);
        }
        if phase == "http_provider_bitswap_hedge_skip" {
            http_provider_races.record_bitswap_hedge_skip(&value);
        }
        if phase == "http_provider_race_result" {
            http_provider_races.record_result(&value, elapsed_ms);
        }
        if phase == "delegated_provider_lookup" {
            delegated_provider_lookup.record(&value, elapsed_ms);
            let endpoint = value
                .get("endpoint")
                .and_then(|endpoint| endpoint.as_str())
                .unwrap_or("unknown")
                .to_string();
            delegated_provider_lookup_by_endpoint
                .entry(endpoint)
                .or_default()
                .record(&value, elapsed_ms);
        }
        if phase == "delegated_provider_self_hedge" {
            delegated_provider_lookup.record_self_hedge(&value);
            let endpoint = value
                .get("endpoint")
                .and_then(|endpoint| endpoint.as_str())
                .unwrap_or("unknown")
                .to_string();
            delegated_provider_lookup_by_endpoint
                .entry(endpoint)
                .or_default()
                .record_self_hedge(&value);
        }
        if phase == "dht_provider_lookup" {
            dht_provider_lookup.record(&value, elapsed_ms);
        }
        if phase == "provider_diversity_low" {
            provider_diversity_low.record(&value);
        }
        match phase {
            "provider_refresh_after_timeout" => {
                provider_retries.refresh_after_timeout_events += 1;
            }
            "provider_refresh_after_failure" => {
                provider_retries.refresh_after_failure_events += 1;
            }
            "provider_refresh_skipped_empty_provider_set" => {
                provider_retries.skipped_empty_provider_set_events += 1;
            }
            "retry_provider_count" => {
                provider_retries.retry_count_events += 1;
                let same_provider_set = value
                    .get("same_provider_set")
                    .and_then(|same| same.as_bool())
                    == Some(true);
                let same_bitswap_peer_set = value
                    .get("same_bitswap_peer_set")
                    .and_then(|same| same.as_bool())
                    == Some(true);
                let request_timeout = value
                    .get("request_timeout")
                    .and_then(|timeout| timeout.as_bool())
                    == Some(true);
                if same_provider_set {
                    provider_retries.same_provider_sets += 1;
                }
                if same_bitswap_peer_set {
                    provider_retries.same_bitswap_peer_sets += 1;
                }
                if request_timeout {
                    provider_retries.request_timeout_retry_counts += 1;
                    bitswap_timeout_recovery.provider_retry_starts += 1;
                    if same_provider_set {
                        bitswap_timeout_recovery.same_provider_retry_starts += 1;
                    } else {
                        bitswap_timeout_recovery.refreshed_provider_retry_starts += 1;
                    }
                    if let Some(cid) = json_detail_string(value.get("cid")) {
                        *pending_request_timeout_retries.entry(cid).or_default() += 1;
                    }
                    if same_bitswap_peer_set {
                        provider_retries.same_bitswap_request_timeout_retry_counts += 1;
                    }
                }
            }
            "provider_retry_after_request_timeout" => {
                provider_retries.request_timeout_retries += 1;
            }
            "provider_retry_after_timeout" => {
                provider_retries.timeout_retries += 1;
            }
            "provider_retry_after_connection_timeout" => {
                provider_retries.connection_timeout_retries += 1;
            }
            _ => {}
        }
        if phase == "request_done" {
            if let Some(status) = json_detail_string(value.get("status")) {
                *request_statuses.entry(status).or_default() += 1;
            }
            if let Some(elapsed_ms) = elapsed_ms {
                gateway_request_elapsed_values.push(elapsed_ms);
            }
        }
        if phase == "gateway_limiter" {
            gateway_limiter_events += 1;
            if let Some(elapsed_ms) = elapsed_ms {
                gateway_limiter_elapsed_values.push(elapsed_ms);
            }
            gateway_limiter_max_timeout_ms = gateway_limiter_max_timeout_ms.max(
                value
                    .get("timeout_ms")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
            match value
                .get("acquired")
                .and_then(|acquired| acquired.as_bool())
            {
                Some(true) => gateway_limiter_acquired += 1,
                Some(false) => {
                    gateway_limiter_denials += 1;
                    if let Some(elapsed_ms) = elapsed_ms {
                        gateway_limiter_denied_elapsed_values.push(elapsed_ms);
                    }
                }
                None => {}
            }
        }
        if phase == "gateway_small_body_cache" {
            gateway_small_body_cache.events += 1;
            match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
                Some(true) => gateway_small_body_cache.hits += 1,
                Some(false) => gateway_small_body_cache.misses += 1,
                None => {}
            }
            if value
                .get("cache_inserted")
                .and_then(|inserted| inserted.as_bool())
                == Some(true)
            {
                gateway_small_body_cache.inserts += 1;
            }
            gateway_small_body_cache.evictions +=
                value.get("evicted").and_then(json_u128).unwrap_or_default();
            let body_len = value
                .get("body_len")
                .and_then(json_u128)
                .unwrap_or_default();
            gateway_small_body_cache.bytes_served += body_len;
            gateway_small_body_cache.max_body_len =
                gateway_small_body_cache.max_body_len.max(body_len);
            gateway_small_body_cache.max_cache_len = gateway_small_body_cache.max_cache_len.max(
                value
                    .get("cache_len")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
            gateway_small_body_cache.max_cache_bytes =
                gateway_small_body_cache.max_cache_bytes.max(
                    value
                        .get("cache_bytes")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
        }
        if phase == "gateway_direct_body" {
            gateway_direct_body.events += 1;
            let body_len = value
                .get("body_len")
                .and_then(json_u128)
                .unwrap_or_default();
            gateway_direct_body.bytes += body_len;
            gateway_direct_body.max_body_len = gateway_direct_body.max_body_len.max(body_len);
            gateway_direct_body.max_elapsed_ms = gateway_direct_body
                .max_elapsed_ms
                .max(elapsed_ms.unwrap_or_default());
        }
        if phase == "gateway_stream_done" {
            gateway_stream_body.events += 1;
            let body_len = value
                .get("body_len")
                .and_then(json_u128)
                .unwrap_or_default();
            let chunks = value.get("chunks").and_then(json_u128).unwrap_or_default();
            gateway_stream_body.bytes += body_len;
            gateway_stream_body.max_body_len = gateway_stream_body.max_body_len.max(body_len);
            gateway_stream_body.max_chunks = gateway_stream_body.max_chunks.max(chunks);
            gateway_stream_body.max_elapsed_ms = gateway_stream_body
                .max_elapsed_ms
                .max(elapsed_ms.unwrap_or_default());
        }
        if phase == "unixfs_metadata_cache" {
            unixfs_metadata_cache.record(&value);
        }
        let successful_bitswap_fetch =
            phase == "bitswap_fetch" && value.get("ok").and_then(|ok| ok.as_bool()) == Some(true);
        let successful_bitswap_delivery = (phase == "bitswap_fetch"
            || phase == "bitswap_session_shortcut")
            && value.get("ok").and_then(|ok| ok.as_bool()) == Some(true);
        if phase == "bitswap_fetch" {
            bitswap_session.fetches += 1;
            let trusted_peer_count = value
                .get("trusted_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            let has_trusted_peers = trusted_peer_count > 0;
            if has_trusted_peers {
                bitswap_session.with_trusted_peers += 1;
            }
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
                if value
                    .get("source_peer_trusted")
                    .and_then(|trusted| trusted.as_bool())
                    == Some(true)
                {
                    bitswap_session.trusted_successes += 1;
                } else {
                    bitswap_session.untrusted_successes += 1;
                }
            } else if has_trusted_peers {
                bitswap_session.trusted_failures += 1;
            }
        }
        if phase == "bitswap_request_timeout_detail"
            && value
                .get("trusted_peer_count")
                .and_then(json_u128)
                .unwrap_or_default()
                > 0
        {
            bitswap_session.request_timeouts_with_trusted += 1;
        }
        if phase == "bitswap_request_timeout_detail" {
            bitswap_timeout_recovery.request_timeouts += 1;
            let timeout_cid = json_detail_string(value.get("cid"));
            let peer_count = value
                .get("peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            let trusted_peer_count = value
                .get("trusted_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_timeout_recovery.max_request_timeout_peer_count = bitswap_timeout_recovery
                .max_request_timeout_peer_count
                .max(peer_count);
            let want_block_targets = value
                .get("want_block_target_count")
                .and_then(json_u128)
                .unwrap_or_default();
            let want_have_targets = value
                .get("want_have_target_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_timeout_recovery.request_timeout_want_block_targets += want_block_targets;
            bitswap_timeout_recovery.request_timeout_want_have_targets += want_have_targets;
            bitswap_timeout_recovery.max_request_timeout_want_block_targets =
                bitswap_timeout_recovery
                    .max_request_timeout_want_block_targets
                    .max(want_block_targets);
            bitswap_timeout_recovery.max_request_timeout_want_have_targets =
                bitswap_timeout_recovery
                    .max_request_timeout_want_have_targets
                    .max(want_have_targets);
            if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
                *bitswap_timeout_recovery
                    .request_timeout_budgets
                    .entry(timeout_ms.to_string())
                    .or_default() += 1;
            }
            if trusted_peer_count == 0 {
                bitswap_timeout_recovery.cold_request_timeouts += 1;
            } else if peer_count == trusted_peer_count {
                bitswap_timeout_recovery.trusted_only_request_timeouts += 1;
            }
            if trusted_peer_count > 0 && peer_count > trusted_peer_count {
                bitswap_timeout_recovery.mixed_trusted_request_timeouts += 1;
            }
            if timeout_cid
                .as_ref()
                .and_then(|cid| provider_fetch_dial_plan_seen.get(cid))
                == Some(&false)
            {
                bitswap_timeout_recovery.request_timeouts_without_dial_plan += 1;
                if trusted_peer_count > 0 && peer_count > trusted_peer_count {
                    bitswap_timeout_recovery.mixed_trusted_request_timeouts_without_dial_plan += 1;
                }
            }
        }
        if phase == "bitswap_client_reset" {
            bitswap_timeout_recovery.client_resets += 1;
        }
        if phase == "bitswap_request_timeout" {
            bitswap_timeout_recovery.request_timeout_events += 1;
            match value
                .get("reset_client")
                .and_then(|reset_client| reset_client.as_bool())
            {
                Some(true) => bitswap_timeout_recovery.request_timeout_reset_true += 1,
                Some(false) => bitswap_timeout_recovery.request_timeout_reset_false += 1,
                None => {}
            }
        }
        if phase == "bitswap_session_shortcut_start" {
            bitswap_session.session_shortcut_starts += 1;
        }
        if phase == "bitswap_session_shortcut_pre_lookup" {
            bitswap_session.session_shortcut_pre_lookup_waits += 1;
            let outcome = value
                .get("outcome")
                .and_then(|outcome| outcome.as_str())
                .unwrap_or("timeout");
            let pre_lookup_elapsed_ms = value.get("elapsed_ms").and_then(json_u128).or_else(|| {
                if outcome == "timeout" {
                    value.get("timeout_ms").and_then(json_u128)
                } else {
                    None
                }
            });
            bitswap_session.session_shortcut_pre_lookup_max_ms = bitswap_session
                .session_shortcut_pre_lookup_max_ms
                .max(pre_lookup_elapsed_ms.unwrap_or_default());
            if let Some(elapsed_ms) = pre_lookup_elapsed_ms {
                bitswap_session
                    .session_shortcut_pre_lookup_elapsed_values
                    .push(elapsed_ms);
            }
            match outcome {
                "hit" => {
                    bitswap_session.session_shortcut_pre_lookup_hits += 1;
                    if let Some(elapsed_ms) = pre_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_pre_lookup_hit_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                "miss" => {
                    bitswap_session.session_shortcut_pre_lookup_misses += 1;
                    if let Some(elapsed_ms) = pre_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_pre_lookup_miss_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                "timeout" => {
                    bitswap_session.session_shortcut_pre_lookup_timeouts += 1;
                    if let Some(elapsed_ms) = pre_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_pre_lookup_timeout_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                _ => {}
            }
            if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
                *bitswap_session
                    .session_shortcut_pre_lookup_budgets
                    .entry(timeout_ms.to_string())
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_session_shortcut_post_lookup_wait" {
            bitswap_session.session_shortcut_post_lookup_waits += 1;
            let post_lookup_elapsed_ms = value.get("elapsed_ms").and_then(json_u128);
            bitswap_session.session_shortcut_post_lookup_max_ms = bitswap_session
                .session_shortcut_post_lookup_max_ms
                .max(post_lookup_elapsed_ms.unwrap_or_default());
            if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                bitswap_session
                    .session_shortcut_post_lookup_elapsed_values
                    .push(elapsed_ms);
            }
            let outcome = value
                .get("outcome")
                .and_then(|outcome| outcome.as_str())
                .unwrap_or("timeout");
            match outcome {
                "hit" => {
                    bitswap_session.session_shortcut_post_lookup_hits += 1;
                    if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_post_lookup_hit_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                "miss" => bitswap_session.session_shortcut_post_lookup_misses += 1,
                "timeout" => {
                    bitswap_session.session_shortcut_post_lookup_timeouts += 1;
                    if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_post_lookup_timeout_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                "error" => bitswap_session.session_shortcut_post_lookup_errors += 1,
                _ => {}
            }
            if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
                *bitswap_session
                    .session_shortcut_post_lookup_budgets
                    .entry(timeout_ms.to_string())
                    .or_default() += 1;
                if outcome == "timeout" {
                    *bitswap_session
                        .session_shortcut_post_lookup_timeout_budgets
                        .entry(timeout_ms.to_string())
                        .or_default() += 1;
                }
            }
            if let Some(http_provider_count) = value.get("http_provider_count").and_then(json_u128)
            {
                *bitswap_session
                    .session_shortcut_post_lookup_http_provider_counts
                    .entry(http_provider_count.to_string())
                    .or_default() += 1;
                if http_provider_count == 1 {
                    if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                        bitswap_session
                            .session_shortcut_post_lookup_single_http_elapsed_values
                            .push(elapsed_ms);
                    }
                    match outcome {
                        "hit" => {
                            if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                                bitswap_session
                                    .session_shortcut_post_lookup_single_http_hit_elapsed_values
                                    .push(elapsed_ms);
                            }
                        }
                        "timeout" => {
                            if let Some(elapsed_ms) = post_lookup_elapsed_ms {
                                bitswap_session
                                    .session_shortcut_post_lookup_single_http_timeout_elapsed_values
                                    .push(elapsed_ms);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if phase == "bitswap_session_shortcut_post_lookup_race" {
            bitswap_session.session_shortcut_post_lookup_races += 1;
            let elapsed_ms = value.get("elapsed_ms").and_then(json_u128);
            if let Some(elapsed_ms) = elapsed_ms {
                bitswap_session
                    .session_shortcut_post_lookup_race_elapsed_values
                    .push(elapsed_ms);
            }
            let provider_result_elapsed_ms =
                value.get("provider_result_elapsed_ms").and_then(json_u128);
            if let Some(elapsed_ms) = provider_result_elapsed_ms {
                bitswap_session
                    .session_shortcut_post_lookup_race_provider_result_elapsed_values
                    .push(elapsed_ms);
            }
            let outcome = value
                .get("outcome")
                .and_then(|outcome| outcome.as_str())
                .unwrap_or("unknown");
            *bitswap_session
                .session_shortcut_post_lookup_race_outcome_counts
                .entry(outcome.to_string())
                .or_default() += 1;
            if outcome.starts_with("provider_") {
                if outcome.contains("error") {
                    bitswap_session.session_shortcut_post_lookup_race_errors += 1;
                } else {
                    bitswap_session.session_shortcut_post_lookup_race_provider_wins += 1;
                }
            } else if outcome.starts_with("bitswap_won") {
                bitswap_session.session_shortcut_post_lookup_race_bitswap_wins += 1;
            } else if outcome.contains("error") {
                bitswap_session.session_shortcut_post_lookup_race_errors += 1;
            }

            let source = value
                .get("source")
                .and_then(|source| source.as_str())
                .unwrap_or("unknown");
            *bitswap_session
                .session_shortcut_post_lookup_race_source_counts
                .entry(source.to_string())
                .or_default() += 1;

            let http_provider_count = value.get("http_provider_count").and_then(json_u128);
            if let Some(http_provider_count) = http_provider_count {
                *bitswap_session
                    .session_shortcut_post_lookup_race_http_provider_count_counts
                    .entry(http_provider_count.to_string())
                    .or_default() += 1;
            }

            if outcome.starts_with("provider_") && source == "bitswap" {
                bitswap_session.session_shortcut_post_lookup_race_provider_bitswap_wins += 1;
                if http_provider_count == Some(1) {
                    bitswap_session
                        .session_shortcut_post_lookup_race_single_http_provider_bitswap_wins += 1;
                    if let Some(elapsed_ms) = elapsed_ms {
                        bitswap_session
                            .session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_values
                            .push(elapsed_ms);
                    }
                }
            }
        }
        if phase == "bitswap_session_late_peer_wait" {
            bitswap_session.session_late_peer_waits += 1;
            let late_peer_elapsed_ms = value.get("elapsed_ms").and_then(json_u128);
            bitswap_session.session_late_peer_max_ms = bitswap_session
                .session_late_peer_max_ms
                .max(late_peer_elapsed_ms.unwrap_or_default());
            if let Some(elapsed_ms) = late_peer_elapsed_ms {
                bitswap_session
                    .session_late_peer_elapsed_values
                    .push(elapsed_ms);
            }
            match value.get("outcome").and_then(|outcome| outcome.as_str()) {
                Some("hit") => {
                    bitswap_session.session_late_peer_hits += 1;
                    if let Some(elapsed_ms) = late_peer_elapsed_ms {
                        bitswap_session
                            .session_late_peer_hit_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                Some("miss") => {
                    bitswap_session.session_late_peer_misses += 1;
                    if let Some(elapsed_ms) = late_peer_elapsed_ms {
                        bitswap_session
                            .session_late_peer_miss_elapsed_values
                            .push(elapsed_ms);
                    }
                }
                _ => {}
            }
        }
        if phase == "bitswap_session_shortcut" {
            bitswap_session.session_shortcut_attempts += 1;
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
                bitswap_session.session_shortcut_hits += 1;
            } else {
                bitswap_session.session_shortcut_misses += 1;
            }
        }
        if phase == "bitswap_peer_attempt_start" {
            bitswap_peer_attempts.starts += 1;
            bitswap_batches.peer_attempt_starts += 1;
            if let (Some(cid), Some(peer)) = (
                json_detail_string(value.get("cid")),
                json_detail_string(value.get("peer")),
            ) {
                let mode = if value
                    .get("prefer_want_have")
                    .and_then(|prefer| prefer.as_bool())
                    == Some(true)
                {
                    "want_have"
                } else {
                    "want_block"
                };
                bitswap_attempt_modes.insert((cid, peer), mode.to_string());
            }
        }
        if phase == "bitswap_peer_attempt" {
            bitswap_peer_attempts.outgoing_completed += 1;
            if value
                .get("prefer_want_have")
                .and_then(|prefer| prefer.as_bool())
                == Some(true)
            {
                bitswap_peer_attempts.prefer_want_have += 1;
            }
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
                bitswap_peer_attempts.successes += 1;
                bitswap_batches.peer_attempt_successes += 1;
                let requested_blocks = trace_count_field(&value, "requested_blocks");
                bitswap_batches.requested_blocks += requested_blocks;
                bitswap_batches.max_requested_blocks =
                    bitswap_batches.max_requested_blocks.max(requested_blocks);
            } else {
                bitswap_peer_attempts.failures += 1;
                match value
                    .get("failure_kind")
                    .and_then(|kind| kind.as_str())
                    .unwrap_or("other")
                {
                    "connection_timeout" => bitswap_peer_attempts.connection_timeouts += 1,
                    "read_timeout" => bitswap_peer_attempts.read_timeouts += 1,
                    _ => bitswap_peer_attempts.other_failures += 1,
                }
            }
        }
        if phase == "bitswap_peer_attempt_cancelled" {
            bitswap_peer_attempts.cancelled += 1;
            if value
                .get("prefer_want_have")
                .and_then(|prefer| prefer.as_bool())
                == Some(true)
            {
                bitswap_peer_attempts.cancelled_prefer_want_have += 1;
            }
            if let Some(stage) = json_detail_string(value.get("stage")) {
                *bitswap_peer_attempt_cancelled_stages
                    .entry(stage)
                    .or_default() += 1;
            }
            if let Some(index) = value
                .get("probe_peer_candidate_index")
                .and_then(|index| json_detail_string(Some(index)))
            {
                *bitswap_peer_attempt_cancelled_candidate_indexes
                    .entry(index)
                    .or_default() += 1;
            }
            if let Some(mode) = json_detail_string(value.get("probe_peer_request_mode")) {
                *bitswap_peer_attempt_cancelled_request_modes
                    .entry(mode)
                    .or_default() += 1;
            }
            if let Some(transport) =
                json_detail_string(value.get("probe_peer_first_addr_transport"))
            {
                *bitswap_peer_attempt_cancelled_first_addr_transports
                    .entry(transport)
                    .or_default() += 1;
            }
            if let Some(family) = json_detail_string(value.get("probe_peer_first_addr_family")) {
                *bitswap_peer_attempt_cancelled_first_addr_families
                    .entry(family)
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_want_have_probe" {
            bitswap_want_have_probe_events += 1;
            match value.get("ok").and_then(|ok| ok.as_bool()) {
                Some(true) => bitswap_want_have_probe_ok += 1,
                Some(false) => bitswap_want_have_probe_failures += 1,
                None => {}
            }
            if value
                .get("has_have")
                .and_then(|has_have| has_have.as_bool())
                == Some(true)
            {
                bitswap_want_have_probe_have += 1;
            }
            if value
                .get("has_dont_have")
                .and_then(|has_dont_have| has_dont_have.as_bool())
                == Some(true)
            {
                bitswap_want_have_probe_dont_have += 1;
            }
            let mut outcome_no_presence = false;
            if let Some(outcome) = value.get("outcome").and_then(|outcome| outcome.as_str()) {
                *bitswap_want_have_probe_outcomes
                    .entry(outcome.to_string())
                    .or_default() += 1;
                match outcome {
                    "block" => bitswap_want_have_probe_block += 1,
                    "have_then_want_block"
                    | "timeout_fallback_want_block"
                    | "no_presence_fallback_want_block" => {
                        bitswap_want_have_probe_want_block_followups += 1;
                    }
                    _ => {}
                }
                if outcome == "no_presence_fallback_want_block" {
                    outcome_no_presence = true;
                }
            }
            let zero_presence = value.get("presence_count").and_then(json_u128) == Some(0);
            if outcome_no_presence || zero_presence {
                bitswap_want_have_probe_no_presence += 1;
            }
            bitswap_want_have_probe_bytes +=
                value.get("bytes").and_then(json_u128).unwrap_or_default();
            bitswap_want_have_probe_extra_blocks += value
                .get("extra_blocks")
                .and_then(json_u128)
                .unwrap_or_default();
            if let Some(timeout_ms) = value.get("timeout_ms").and_then(json_u128) {
                bitswap_want_have_probe_max_timeout_ms =
                    bitswap_want_have_probe_max_timeout_ms.max(timeout_ms);
            }
            if let Some(elapsed_ms) = elapsed_ms {
                bitswap_want_have_probe_elapsed_values.push(elapsed_ms);
            }
            if let Some(peer) = json_detail_string(value.get("peer")) {
                *bitswap_want_have_probe_peers.entry(peer).or_default() += 1;
            }
            if let Some(index) = json_detail_string(value.get("probe_peer_candidate_index")) {
                if index != "-1" {
                    *bitswap_want_have_probe_candidate_indexes
                        .entry(index)
                        .or_default() += 1;
                }
            }
            if let Some(mode) = json_detail_string(value.get("probe_peer_request_mode")) {
                if !mode.is_empty() {
                    *bitswap_want_have_probe_request_modes
                        .entry(mode)
                        .or_default() += 1;
                }
            }
            if let Some(transport) =
                json_detail_string(value.get("probe_peer_first_addr_transport"))
            {
                if !transport.is_empty() && transport != "none" && transport != "unknown" {
                    *bitswap_want_have_probe_first_addr_transports
                        .entry(transport)
                        .or_default() += 1;
                }
            }
            if let Some(family) = json_detail_string(value.get("probe_peer_first_addr_family")) {
                if !family.is_empty() && family != "none" && family != "unknown" {
                    *bitswap_want_have_probe_first_addr_families
                        .entry(family)
                        .or_default() += 1;
                }
            }
            if let Some(target_peer_count) =
                json_detail_string(value.get("probe_target_peer_count"))
            {
                *bitswap_want_have_probe_target_peer_counts
                    .entry(target_peer_count)
                    .or_default() += 1;
            }
            if let Some(peer_addr_count) = json_detail_string(value.get("probe_peer_addr_count")) {
                *bitswap_want_have_probe_peer_addr_counts
                    .entry(peer_addr_count)
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_dial_plan" {
            if let Some(cid) = json_detail_string(value.get("cid")) {
                provider_fetch_dial_plan_seen.insert(cid, true);
            }
            let cid_count = match trace_count_field(&value, "cid_count") {
                0 => 1,
                count => count,
            };
            bitswap_batches.commands += 1;
            bitswap_batches.total_cids += cid_count;
            bitswap_batches.max_cids = bitswap_batches.max_cids.max(cid_count);
            if cid_count > 1 {
                bitswap_batches.multi_cid_commands += 1;
            }
            bitswap_dial_plans.events += 1;
            bitswap_dial_plans.peer_targets += value
                .get("peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.candidate_peers += value
                .get("candidate_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.new_dial_peers += value
                .get("new_dial_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.new_dial_addrs += value
                .get("new_dial_addr_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.suppressed_dial_peers += value
                .get("suppressed_dial_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.suppressed_dial_addrs += value
                .get("suppressed_dial_addr_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.pending_dial_peers += value
                .get("pending_dial_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.connected_peers += value
                .get("connected_peer_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dial_plans.max_command_queued_ms =
                bitswap_dial_plans.max_command_queued_ms.max(
                    value
                        .get("command_queued_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
        }
        if phase == "bitswap_fetch_cancelled" {
            bitswap_batches.cancelled += 1;
        }
        if phase == "bitswap_batch_failed" {
            bitswap_batches.failures += 1;
        }
        if phase == "bitswap_incoming_batch" {
            let cid_count = value
                .get("cid_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_incoming_batches.events += 1;
            bitswap_incoming_batches.total_cids += cid_count;
            bitswap_incoming_batches.max_cids = bitswap_incoming_batches.max_cids.max(cid_count);
            bitswap_incoming_batches.requested_blocks += value
                .get("requested_blocks")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_incoming_batches.extra_blocks += value
                .get("extra_blocks")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_incoming_batches.max_elapsed_ms = bitswap_incoming_batches.max_elapsed_ms.max(
                value
                    .get("elapsed_ms")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
        }
        if phase == "provider_fetch_start" {
            if let Some(cid) = json_detail_string(value.get("cid")) {
                provider_fetch_dial_plan_seen.insert(cid, false);
            }
        }
        if phase == "bitswap_incoming_block" {
            bitswap_incoming_blocks.matches += 1;
            bitswap_incoming_blocks.blocks += value
                .get("block_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_incoming_blocks.bytes +=
                value.get("bytes").and_then(json_u128).unwrap_or_default();
            let delivered_waiters = value
                .get("delivered_waiter_count")
                .and_then(json_u128)
                .unwrap_or_default();
            let dropped_waiters = value
                .get("dropped_waiter_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_incoming_blocks.delivered_waiters += delivered_waiters;
            bitswap_incoming_blocks.dropped_waiters += dropped_waiters;
            bitswap_incoming_blocks.max_oldest_pending_ms =
                bitswap_incoming_blocks.max_oldest_pending_ms.max(
                    value
                        .get("oldest_pending_ms")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            bitswap_incoming_blocks.max_pending_waiters =
                bitswap_incoming_blocks.max_pending_waiters.max(
                    value
                        .get("pending_waiter_count")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            bitswap_incoming_blocks.max_dropped_waiters = bitswap_incoming_blocks
                .max_dropped_waiters
                .max(dropped_waiters);
        }
        if phase == "bitswap_incoming_stream_read" {
            bitswap_incoming_reads.events += 1;
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
                bitswap_incoming_reads.failures += 1;
            }
            if value.get("dropped").and_then(|dropped| dropped.as_bool()) == Some(true) {
                bitswap_incoming_reads.dropped += 1;
            }
            if value
                .get("timed_out")
                .and_then(|timed_out| timed_out.as_bool())
                == Some(true)
            {
                bitswap_incoming_reads.timed_out += 1;
            }
            bitswap_incoming_reads.max_pending_reads =
                bitswap_incoming_reads.max_pending_reads.max(
                    value
                        .get("pending_reads")
                        .and_then(json_u128)
                        .unwrap_or_default(),
                );
            bitswap_incoming_reads.max_elapsed_ms = bitswap_incoming_reads.max_elapsed_ms.max(
                value
                    .get("elapsed_ms")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
        }
        if phase == "bitswap_fetch" {
            if let Some(cid) = json_detail_string(value.get("cid")) {
                let mut remove_pending_retry = false;
                if let Some(pending_retries) = pending_request_timeout_retries.get_mut(&cid) {
                    if *pending_retries > 0 {
                        if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
                            bitswap_timeout_recovery.retry_successes += 1;
                            if value
                                .get("source_peer_trusted")
                                .and_then(|trusted| trusted.as_bool())
                                == Some(true)
                            {
                                bitswap_timeout_recovery.trusted_retry_successes += 1;
                            } else {
                                bitswap_timeout_recovery.untrusted_retry_successes += 1;
                            }
                            if let Some(elapsed_ms) = elapsed_ms {
                                bitswap_timeout_recovery
                                    .retry_success_elapsed_values
                                    .push(elapsed_ms);
                            }
                        } else if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
                            bitswap_timeout_recovery.retry_failures += 1;
                        }
                        *pending_retries -= 1;
                        remove_pending_retry = *pending_retries == 0;
                    }
                }
                if remove_pending_retry {
                    pending_request_timeout_retries.remove(&cid);
                }
            }
        }
        if successful_bitswap_fetch {
            if let Some(peer) = value.get("source_peer").and_then(|peer| peer.as_str()) {
                *bitswap_source_peers.entry(peer.to_string()).or_default() += 1;
            }
            if let Some(transport) = json_detail_string(value.get("source_transport")) {
                *bitswap_source_transports.entry(transport).or_default() += 1;
            }
            if let Some(index) = json_detail_string(value.get("source_peer_candidate_index")) {
                if index != "-1" {
                    *bitswap_source_candidate_indexes.entry(index).or_default() += 1;
                }
            }
            if let Some(index) = json_detail_string(value.get("source_peer_addr_index")) {
                if index != "-1" {
                    *bitswap_source_addr_indexes.entry(index).or_default() += 1;
                }
            }
            if let Some(family) = json_detail_string(value.get("source_peer_addr_family")) {
                if !family.is_empty() && family != "unknown" {
                    *bitswap_source_addr_families.entry(family).or_default() += 1;
                }
            }
            let addr_match_status = match (
                value
                    .get("source_peer_addr_known")
                    .and_then(|value| value.as_bool()),
                value
                    .get("source_peer_addr_matches_candidate")
                    .and_then(|value| value.as_bool()),
            ) {
                (Some(true), Some(true)) => Some("matched"),
                (Some(true), Some(false)) => Some("unmatched"),
                (Some(false), _) => Some("unknown"),
                _ => None,
            };
            if let Some(status) = addr_match_status {
                *bitswap_source_addr_match_statuses
                    .entry(status.to_string())
                    .or_default() += 1;
            }
        }
        if successful_bitswap_delivery {
            if let (Some(cid), Some(peer)) = (
                json_detail_string(value.get("cid")),
                json_detail_string(value.get("source_peer")),
            ) {
                let mode = bitswap_attempt_modes
                    .get(&(cid, peer))
                    .map(String::as_str)
                    .unwrap_or("unknown");
                *bitswap_source_request_modes
                    .entry(mode.to_string())
                    .or_default() += 1;
            }
            if let Some(delivery) = json_detail_string(value.get("bitswap_delivery")) {
                *bitswap_deliveries.entry(delivery).or_default() += 1;
            }
            let extra_blocks = value
                .get("extra_blocks")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_extra_blocks.events += 1;
            bitswap_extra_blocks.total += extra_blocks;
            bitswap_extra_blocks.max = bitswap_extra_blocks.max.max(extra_blocks);
            match json_detail_string(value.get("bitswap_delivery")).as_deref() {
                Some("incoming") => bitswap_extra_blocks.incoming += extra_blocks,
                Some("outgoing") => bitswap_extra_blocks.outgoing += extra_blocks,
                _ => bitswap_extra_blocks.unknown += extra_blocks,
            }
        }
        if let Some(error) = trace_error_key(phase, &value) {
            *trace_errors.entry(error).or_default() += 1;
        }
        if phase == "bitswap_peer_expand" {
            accumulate_trace_count(&mut bitswap_addr_mix, "tcp", &value, "tcp_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "quic", &value, "quic_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "ws", &value, "ws_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "wss", &value, "wss_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "dns", &value, "dns_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "ip4", &value, "ip4_addr_count");
            accumulate_trace_count(&mut bitswap_addr_mix, "ip6", &value, "ip6_addr_count");
            bitswap_provider_quality.accumulate(&value);
        }
        if phase == "bitswap_connection_established" {
            bitswap_connection_established_events += 1;
            if let Some(established_ms) = value.get("established_ms").and_then(json_u128) {
                bitswap_connection_established_ms_values.push(established_ms);
            }
            if let Some(wait_elapsed_ms) = value.get("wait_elapsed_ms").and_then(json_u128) {
                bitswap_connection_wait_elapsed_ms_values.push(wait_elapsed_ms);
            }
            bitswap_connection_failed_dial_count += value
                .get("failed_dial_count")
                .and_then(json_u128)
                .unwrap_or_default();
            if let Some(transport) = json_detail_string(value.get("transport")) {
                *bitswap_connection_transports.entry(transport).or_default() += 1;
            }
        }
        if phase == "bitswap_connection_error" {
            bitswap_connection_error_events += 1;
            match json_detail_string(value.get("peer")).filter(|peer| !peer.is_empty()) {
                Some(peer) => {
                    bitswap_connection_error_with_peer += 1;
                    *bitswap_connection_error_peers.entry(peer).or_default() += 1;
                }
                None => bitswap_connection_error_without_peer += 1,
            }
            let class = value
                .get("error")
                .and_then(|error| error.as_str())
                .map(bitswap_connection_error_class)
                .unwrap_or("other");
            *bitswap_connection_error_classes
                .entry(class.to_string())
                .or_default() += 1;
            let addr_family = value
                .get("error")
                .and_then(|error| error.as_str())
                .map(bitswap_connection_error_addr_family)
                .unwrap_or("unknown");
            *bitswap_connection_error_addr_families
                .entry(addr_family.to_string())
                .or_default() += 1;
        }
        if phase == "bitswap_connection_error_backoff" {
            bitswap_connection_backoffs += 1;
            if let Some(class) = json_detail_string(value.get("error_class")) {
                *bitswap_connection_backoff_classes.entry(class).or_default() += 1;
            }
            if let Some(peer) = json_detail_string(value.get("peer")) {
                *bitswap_connection_backoff_peers.entry(peer).or_default() += 1;
            }
        }
        if phase == "bitswap_connection_error_peer_skipped" {
            bitswap_connection_backoff_skips += 1;
            if let Some(peer) = json_detail_string(value.get("peer")) {
                *bitswap_connection_backoff_skipped_peers
                    .entry(peer)
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_dial_rejected" {
            bitswap_dial_rejections.events += 1;
            if value
                .get("connection_limit")
                .and_then(|limit| limit.as_bool())
                == Some(true)
            {
                bitswap_dial_rejections.connection_limit += 1;
            } else {
                bitswap_dial_rejections.other += 1;
            }
            if let Some(transport) = json_detail_string(value.get("transport")) {
                *bitswap_dial_rejected_transports
                    .entry(transport)
                    .or_default() += 1;
            }
        }
        if phase == "bitswap_dnsaddr_expand" || phase == "bitswap_dns_multiaddr_expand" {
            bitswap_dns_expansion.events += 1;
            if value.get("cached").and_then(|cached| cached.as_bool()) == Some(true) {
                bitswap_dns_expansion.cached += 1;
            } else {
                bitswap_dns_expansion.uncached += 1;
            }
            if phase == "bitswap_dnsaddr_expand"
                && value.get("ok").and_then(|ok| ok.as_bool()) == Some(false)
            {
                bitswap_dns_expansion.failed += 1;
            }
            bitswap_dns_expansion.records += value
                .get("record_count")
                .and_then(json_u128)
                .unwrap_or_default();
            bitswap_dns_expansion.ips += value
                .get("ip_count")
                .and_then(json_u128)
                .unwrap_or_default();
        }
        let Some(elapsed_ms) = elapsed_ms else {
            continue;
        };
        phases
            .entry(phase.to_string())
            .or_default()
            .push(elapsed_ms);
        slow_events.push(TraceSlowEvent {
            phase: phase.to_string(),
            elapsed_ms,
            details: trace_event_details(&value),
        });
        if successful_bitswap_fetch {
            if let Some(peer) = value.get("source_peer").and_then(|peer| peer.as_str()) {
                let entry = bitswap_peer_fetches.entry(peer.to_string()).or_default();
                entry.count += 1;
                entry.total_ms += elapsed_ms;
                entry.max_ms = entry.max_ms.max(elapsed_ms);
                entry.bytes += value.get("bytes").and_then(json_u128).unwrap_or_default();
                if let Some(transport) = json_detail_string(value.get("source_transport")) {
                    *entry.transports.entry(transport).or_default() += 1;
                }
            }
        }
        if let Some(cid) = json_detail_string(value.get("cid")) {
            let entry = slow_cids.entry(cid).or_default();
            entry.count += 1;
            entry.total_ms += elapsed_ms;
            entry.max_ms = entry.max_ms.max(elapsed_ms);
            *entry.phases.entry(phase.to_string()).or_default() += 1;
            if let Some(path) = trace_event_path(&value) {
                *entry.paths.entry(path).or_default() += 1;
            }
            if successful_bitswap_delivery {
                if let Some(index) = json_detail_string(value.get("source_peer_candidate_index")) {
                    if index != "-1" {
                        *entry
                            .bitswap_source_candidate_indexes
                            .entry(index)
                            .or_default() += 1;
                    }
                }
                if let Some(peer) = json_detail_string(value.get("source_peer")) {
                    if !peer.is_empty() {
                        *entry.bitswap_source_peers.entry(peer).or_default() += 1;
                    }
                }
            }
        }
    }

    let mut phases = phases
        .into_iter()
        .map(|(phase, values)| {
            let total_ms = values.iter().sum();
            let count = values.len();
            TracePhaseAggregate {
                phase,
                count,
                total_ms,
                elapsed_ms: LatencySummary::from_values(values),
            }
        })
        .collect::<Vec<_>>();
    phases.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.elapsed_ms.max_ms.cmp(&left.elapsed_ms.max_ms))
            .then_with(|| left.phase.cmp(&right.phase))
    });
    slow_events.sort_by(|left, right| {
        right
            .elapsed_ms
            .cmp(&left.elapsed_ms)
            .then_with(|| left.phase.cmp(&right.phase))
    });
    slow_events.truncate(MAX_TRACE_SLOW_EVENTS);
    let all_requests = completed_requests
        .into_values()
        .map(TraceRequestBuilder::into_aggregate)
        .collect::<Vec<_>>();
    let mut all_requests = all_requests;
    all_requests.extend(
        active_requests
            .into_values()
            .map(TraceRequestBuilder::into_aggregate),
    );
    let request_classifications = summarize_request_classifications(&all_requests);
    let request_classification_latencies =
        summarize_request_classification_latencies(&all_requests);
    let request_paths = summarize_trace_request_paths(&all_requests);
    let progress_request_groups = summarize_progress_request_groups(&all_requests);
    let mut slow_requests = all_requests.clone();
    slow_requests.sort_by(|left, right| {
        right
            .elapsed_ms
            .cmp(&left.elapsed_ms)
            .then_with(|| right.max_event_ms.cmp(&left.max_event_ms))
            .then_with(|| left.path.cmp(&right.path))
    });
    slow_requests.truncate(MAX_TRACE_SLOW_EVENTS);
    delegated_provider_lookup.finish();
    bitswap_session.finish();
    http_provider_races.finish();
    block_range_batch_fetches.finish();
    bitswap_peer_attempts.cancelled_stages =
        sorted_trace_counts(bitswap_peer_attempt_cancelled_stages);
    bitswap_peer_attempts.cancelled_candidate_indexes =
        sorted_trace_counts(bitswap_peer_attempt_cancelled_candidate_indexes);
    bitswap_peer_attempts.cancelled_request_modes =
        sorted_trace_counts(bitswap_peer_attempt_cancelled_request_modes);
    bitswap_peer_attempts.cancelled_first_addr_transports =
        sorted_trace_counts(bitswap_peer_attempt_cancelled_first_addr_transports);
    bitswap_peer_attempts.cancelled_first_addr_families =
        sorted_trace_counts(bitswap_peer_attempt_cancelled_first_addr_families);

    Ok(TraceSummary {
        line_count,
        event_count,
        event_phases: sorted_trace_counts(event_phases),
        phases,
        progress_phases: sorted_trace_counts(progress_phases),
        slow_events,
        block_sources: sorted_trace_counts(block_sources),
        block_fetch_source_latencies: sorted_trace_source_latencies(block_fetch_source_latencies),
        block_store,
        block_range_batch_fetches,
        provider_retries,
        delegated_provider_lookup,
        delegated_provider_lookup_by_endpoint: sorted_trace_delegated_provider_endpoints(
            delegated_provider_lookup_by_endpoint,
        ),
        dht_provider_lookup,
        provider_diversity_low: provider_diversity_low.into_aggregate(),
        request_statuses: sorted_trace_counts(request_statuses),
        gateway_limiter_denials,
        gateway_limiter: TraceGatewayLimiterAggregate {
            events: gateway_limiter_events,
            acquired: gateway_limiter_acquired,
            denied: gateway_limiter_denials,
            elapsed_ms: LatencySummary::from_values(gateway_limiter_elapsed_values),
            denied_elapsed_ms: LatencySummary::from_values(gateway_limiter_denied_elapsed_values),
            max_timeout_ms: gateway_limiter_max_timeout_ms,
        },
        gateway_request_elapsed_ms: LatencySummary::from_values(gateway_request_elapsed_values),
        gateway_small_body_cache,
        gateway_direct_body,
        gateway_stream_body,
        http_provider_races,
        http_provider_fetches: TraceHttpProviderFetchAggregate {
            events: http_provider_fetch_events,
            successes: http_provider_fetch_successes,
            failures: http_provider_fetch_failures,
            bytes: http_provider_fetch_bytes,
            response_bytes: http_provider_fetch_response_bytes,
            first_chunk_events: http_provider_fetch_first_chunk_events,
            max_response_headers_elapsed_ms: http_provider_fetch_max_response_headers_elapsed_ms,
            max_response_first_chunk_elapsed_ms:
                http_provider_fetch_max_response_first_chunk_elapsed_ms,
            max_response_body_elapsed_ms: http_provider_fetch_max_response_body_elapsed_ms,
            elapsed_ms: LatencySummary::from_values(http_provider_fetch_elapsed_values),
            providers: sorted_trace_counts(http_provider_fetch_providers),
            provider_milestones: sorted_trace_http_provider_milestones(
                http_provider_fetch_provider_milestones,
            ),
            error_classes: sorted_trace_counts(http_provider_fetch_error_classes),
        },
        unixfs_metadata_cache,
        bitswap_source_peers: sorted_trace_counts(bitswap_source_peers),
        bitswap_source_transports: sorted_trace_counts(bitswap_source_transports),
        bitswap_source_request_modes: sorted_trace_counts(bitswap_source_request_modes),
        bitswap_source_candidate_indexes: sorted_trace_counts(bitswap_source_candidate_indexes),
        bitswap_source_addr_indexes: sorted_trace_counts(bitswap_source_addr_indexes),
        bitswap_source_addr_families: sorted_trace_counts(bitswap_source_addr_families),
        bitswap_source_addr_match_statuses: sorted_trace_counts(bitswap_source_addr_match_statuses),
        bitswap_deliveries: sorted_trace_counts(bitswap_deliveries),
        bitswap_batches,
        bitswap_extra_blocks,
        bitswap_incoming_batches,
        bitswap_peer_fetches: sorted_trace_peers(bitswap_peer_fetches),
        bitswap_session,
        bitswap_peer_attempts,
        bitswap_want_have_probes: TraceBitswapWantHaveProbeAggregate {
            events: bitswap_want_have_probe_events,
            ok: bitswap_want_have_probe_ok,
            failures: bitswap_want_have_probe_failures,
            have: bitswap_want_have_probe_have,
            dont_have: bitswap_want_have_probe_dont_have,
            block: bitswap_want_have_probe_block,
            want_block_followups: bitswap_want_have_probe_want_block_followups,
            no_presence: bitswap_want_have_probe_no_presence,
            bytes: bitswap_want_have_probe_bytes,
            extra_blocks: bitswap_want_have_probe_extra_blocks,
            max_timeout_ms: bitswap_want_have_probe_max_timeout_ms,
            elapsed_ms: LatencySummary::from_values(bitswap_want_have_probe_elapsed_values),
            outcomes: sorted_trace_counts(bitswap_want_have_probe_outcomes),
            peers: sorted_trace_counts(bitswap_want_have_probe_peers),
            candidate_indexes: sorted_trace_counts(bitswap_want_have_probe_candidate_indexes),
            request_modes: sorted_trace_counts(bitswap_want_have_probe_request_modes),
            first_addr_transports: sorted_trace_counts(
                bitswap_want_have_probe_first_addr_transports,
            ),
            first_addr_families: sorted_trace_counts(bitswap_want_have_probe_first_addr_families),
            target_peer_counts: sorted_trace_counts(bitswap_want_have_probe_target_peer_counts),
            peer_addr_counts: sorted_trace_counts(bitswap_want_have_probe_peer_addr_counts),
        },
        bitswap_dial_plans,
        bitswap_incoming_blocks,
        bitswap_incoming_reads,
        bitswap_timeout_recovery: bitswap_timeout_recovery
            .into_aggregate(&pending_request_timeout_retries),
        trace_errors: sorted_trace_counts(trace_errors),
        bitswap_addr_mix: sorted_trace_counts(bitswap_addr_mix),
        bitswap_provider_quality,
        bitswap_connection_established: TraceBitswapConnectionEstablishedAggregate {
            events: bitswap_connection_established_events,
            established_ms: LatencySummary::from_values(bitswap_connection_established_ms_values),
            wait_elapsed_ms: LatencySummary::from_values(bitswap_connection_wait_elapsed_ms_values),
            failed_dial_count: bitswap_connection_failed_dial_count,
        },
        bitswap_connection_transports: sorted_trace_counts(bitswap_connection_transports),
        bitswap_connection_errors: TraceBitswapConnectionErrorAggregate {
            events: bitswap_connection_error_events,
            with_peer: bitswap_connection_error_with_peer,
            without_peer: bitswap_connection_error_without_peer,
            classes: sorted_trace_counts(bitswap_connection_error_classes),
            peers: sorted_trace_counts(bitswap_connection_error_peers),
        },
        bitswap_connection_error_addr_families: sorted_trace_counts(
            bitswap_connection_error_addr_families,
        ),
        bitswap_connection_backoff: TraceBitswapConnectionBackoffAggregate {
            backoffs: bitswap_connection_backoffs,
            skipped: bitswap_connection_backoff_skips,
            classes: sorted_trace_counts(bitswap_connection_backoff_classes),
            peers: sorted_trace_counts(bitswap_connection_backoff_peers),
            skipped_peers: sorted_trace_counts(bitswap_connection_backoff_skipped_peers),
        },
        bitswap_dial_rejections,
        bitswap_dial_rejected_transports: sorted_trace_counts(bitswap_dial_rejected_transports),
        bitswap_dns_expansion,
        request_classifications,
        request_classification_latencies,
        request_paths,
        slow_cids: sorted_trace_cids(slow_cids),
        slow_requests,
        progress_request_groups,
    })
}

fn summarize_request_classifications(requests: &[TraceRequestAggregate]) -> Vec<TraceValueCount> {
    let mut counts = BTreeMap::<String, usize>::new();
    for request in requests {
        for classification in &request.classifications {
            *counts.entry(classification.value.clone()).or_default() += classification.count;
        }
    }
    sorted_trace_counts(counts)
}

fn summarize_trace_request_paths(
    requests: &[TraceRequestAggregate],
) -> Vec<TraceRequestPathAggregate> {
    let mut builders = BTreeMap::<String, TraceRequestPathBuilder>::new();
    for request in requests {
        builders
            .entry(request.path.clone())
            .or_insert_with(|| TraceRequestPathBuilder {
                path: request.path.clone(),
                ..TraceRequestPathBuilder::default()
            })
            .record(request);
    }
    let mut paths = builders
        .into_values()
        .map(TraceRequestPathBuilder::into_aggregate)
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        right
            .request_elapsed_ms
            .max_ms
            .cmp(&left.request_elapsed_ms.max_ms)
            .then_with(|| right.max_event_ms.cmp(&left.max_event_ms))
            .then_with(|| left.path.cmp(&right.path))
    });
    paths.truncate(MAX_TRACE_REQUEST_PATHS);
    paths
}

#[derive(Default)]
struct TraceRequestClassificationBuilder {
    request_count: usize,
    request_elapsed_values: Vec<u128>,
    max_event_values: Vec<u128>,
    statuses: BTreeMap<String, usize>,
    paths: BTreeMap<String, usize>,
    top_level_paths: BTreeMap<String, usize>,
    bitswap_source_candidate_indexes: BTreeMap<String, usize>,
    bitswap_source_request_modes: BTreeMap<String, usize>,
    bitswap_source_peers: BTreeMap<String, usize>,
    bitswap_source_transports: BTreeMap<String, usize>,
}

fn summarize_request_classification_latencies(
    requests: &[TraceRequestAggregate],
) -> Vec<TraceRequestClassificationAggregate> {
    let mut builders = BTreeMap::<String, TraceRequestClassificationBuilder>::new();
    for request in requests {
        for classification in &request.classifications {
            let builder = builders.entry(classification.value.clone()).or_default();
            builder.request_count += 1;
            builder.request_elapsed_values.push(request.elapsed_ms);
            builder.max_event_values.push(request.max_event_ms);
            if let Some(status) = &request.status {
                *builder.statuses.entry(status.clone()).or_default() += 1;
            }
            *builder.paths.entry(request.path.clone()).or_default() += 1;
            if let Some(top_level_path) = &request.top_level_path {
                *builder
                    .top_level_paths
                    .entry(top_level_path.clone())
                    .or_default() += 1;
            }
            for index in &request.bitswap_source_candidate_indexes {
                *builder
                    .bitswap_source_candidate_indexes
                    .entry(index.value.clone())
                    .or_default() += index.count;
            }
            for mode in &request.bitswap_source_request_modes {
                *builder
                    .bitswap_source_request_modes
                    .entry(mode.value.clone())
                    .or_default() += mode.count;
            }
            for peer in &request.bitswap_source_peers {
                *builder
                    .bitswap_source_peers
                    .entry(peer.value.clone())
                    .or_default() += peer.count;
            }
            for transport in &request.bitswap_source_transports {
                *builder
                    .bitswap_source_transports
                    .entry(transport.value.clone())
                    .or_default() += transport.count;
            }
        }
    }

    let mut aggregates = builders
        .into_iter()
        .map(
            |(classification, builder)| TraceRequestClassificationAggregate {
                classification,
                request_count: builder.request_count,
                request_elapsed_ms: LatencySummary::from_values(builder.request_elapsed_values),
                max_event_ms: LatencySummary::from_values(builder.max_event_values),
                statuses: sorted_trace_counts(builder.statuses),
                paths: sorted_trace_counts(builder.paths),
                top_level_paths: sorted_trace_counts(builder.top_level_paths),
                bitswap_source_candidate_indexes: sorted_trace_counts(
                    builder.bitswap_source_candidate_indexes,
                ),
                bitswap_source_request_modes: sorted_trace_counts(
                    builder.bitswap_source_request_modes,
                ),
                bitswap_source_peers: sorted_trace_counts(builder.bitswap_source_peers),
                bitswap_source_transports: sorted_trace_counts(builder.bitswap_source_transports),
            },
        )
        .collect::<Vec<_>>();
    aggregates.sort_by(|left, right| {
        right
            .request_count
            .cmp(&left.request_count)
            .then_with(|| {
                right
                    .request_elapsed_ms
                    .max_ms
                    .cmp(&left.request_elapsed_ms.max_ms)
            })
            .then_with(|| left.classification.cmp(&right.classification))
    });
    aggregates
}

#[derive(Default)]
struct TraceProgressRequestGroupBuilder {
    top_level_path: String,
    root_progress_request_id: Option<String>,
    request_count: usize,
    child_request_count: usize,
    completed_request_count: usize,
    failed_request_count: usize,
    request_elapsed_values: Vec<u128>,
    max_event_ms: u128,
    statuses: BTreeMap<String, usize>,
    phases: BTreeMap<String, usize>,
    slow_requests: Vec<TraceProgressRequestAggregate>,
}

impl TraceProgressRequestGroupBuilder {
    fn record(&mut self, request: &TraceRequestAggregate) {
        self.request_count += 1;
        if request.parent_progress_request_id.is_some() {
            self.child_request_count += 1;
        }
        if let Some(status) = &request.status {
            self.completed_request_count += 1;
            *self.statuses.entry(status.clone()).or_default() += 1;
            if status.parse::<u16>().is_ok_and(|status| status >= 400) {
                self.failed_request_count += 1;
            }
        } else {
            *self.statuses.entry("active".to_string()).or_default() += 1;
        }
        self.request_elapsed_values.push(request.elapsed_ms);
        self.max_event_ms = self.max_event_ms.max(request.max_event_ms);
        for phase in &request.phases {
            *self.phases.entry(phase.value.clone()).or_default() += phase.count;
        }
        self.slow_requests.push(TraceProgressRequestAggregate {
            path: request.path.clone(),
            progress_request_id: request.progress_request_id.clone(),
            parent_progress_request_id: request.parent_progress_request_id.clone(),
            status: request.status.clone(),
            elapsed_ms: request.elapsed_ms,
            max_event_ms: request.max_event_ms,
            block_sources: request.block_sources.clone(),
            http_provider_fetch_providers: request.http_provider_fetch_providers.clone(),
        });
    }

    fn into_aggregate(mut self) -> TraceProgressRequestGroupAggregate {
        self.slow_requests.sort_by(|left, right| {
            right
                .elapsed_ms
                .cmp(&left.elapsed_ms)
                .then_with(|| right.max_event_ms.cmp(&left.max_event_ms))
                .then_with(|| left.path.cmp(&right.path))
        });
        self.slow_requests.truncate(8);
        TraceProgressRequestGroupAggregate {
            top_level_path: self.top_level_path,
            root_progress_request_id: self.root_progress_request_id,
            request_count: self.request_count,
            child_request_count: self.child_request_count,
            completed_request_count: self.completed_request_count,
            failed_request_count: self.failed_request_count,
            request_elapsed_ms: LatencySummary::from_values(self.request_elapsed_values),
            max_event_ms: self.max_event_ms,
            statuses: sorted_trace_counts(self.statuses),
            phases: sorted_trace_counts(self.phases),
            slow_requests: self.slow_requests,
        }
    }
}

fn summarize_progress_request_groups(
    requests: &[TraceRequestAggregate],
) -> Vec<TraceProgressRequestGroupAggregate> {
    let by_progress_id = requests
        .iter()
        .filter_map(|request| {
            request
                .progress_request_id
                .as_ref()
                .map(|id| (id.clone(), request))
        })
        .collect::<BTreeMap<_, _>>();
    let mut groups = BTreeMap::<(String, Option<String>), TraceProgressRequestGroupBuilder>::new();
    for request in requests {
        if request.progress_request_id.is_none()
            && request.parent_progress_request_id.is_none()
            && request.top_level_path.is_none()
        {
            continue;
        }
        let top_level_path = request
            .top_level_path
            .clone()
            .unwrap_or_else(|| request.path.clone());
        let root_progress_request_id = root_progress_request_id(request, &by_progress_id);
        let key = (top_level_path.clone(), root_progress_request_id.clone());
        let group = groups
            .entry(key)
            .or_insert_with(|| TraceProgressRequestGroupBuilder {
                top_level_path,
                root_progress_request_id,
                ..TraceProgressRequestGroupBuilder::default()
            });
        group.record(request);
    }
    let mut groups = groups
        .into_values()
        .map(TraceProgressRequestGroupBuilder::into_aggregate)
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        right
            .request_elapsed_ms
            .max_ms
            .cmp(&left.request_elapsed_ms.max_ms)
            .then_with(|| right.max_event_ms.cmp(&left.max_event_ms))
            .then_with(|| left.top_level_path.cmp(&right.top_level_path))
    });
    groups.truncate(MAX_TRACE_SLOW_EVENTS);
    groups
}

fn root_progress_request_id(
    request: &TraceRequestAggregate,
    by_progress_id: &BTreeMap<String, &TraceRequestAggregate>,
) -> Option<String> {
    let mut current_id = request.progress_request_id.clone()?;
    let mut root_id = current_id.clone();
    for _ in 0..16 {
        let Some(current) = by_progress_id.get(&current_id) else {
            break;
        };
        let Some(parent_id) = current.parent_progress_request_id.clone() else {
            break;
        };
        root_id = parent_id.clone();
        current_id = parent_id;
    }
    Some(root_id)
}

fn sorted_trace_peers(counts: BTreeMap<String, TracePeerBuilder>) -> Vec<TracePeerAggregate> {
    let mut values = counts
        .into_iter()
        .map(|(peer, builder)| TracePeerAggregate {
            peer,
            count: builder.count,
            total_ms: builder.total_ms,
            max_ms: builder.max_ms,
            bytes: builder.bytes,
            transports: sorted_trace_counts(builder.transports),
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.max_ms.cmp(&left.max_ms))
            .then_with(|| left.peer.cmp(&right.peer))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_http_provider_milestones(
    counts: BTreeMap<String, TraceHttpProviderMilestoneBuilder>,
) -> Vec<TraceHttpProviderMilestoneAggregate> {
    let mut values = counts
        .into_iter()
        .map(|(provider, builder)| {
            let elapsed_ms = LatencySummary::from_values(builder.elapsed_values);
            let response_headers_elapsed_ms =
                LatencySummary::from_values(builder.response_headers_elapsed_values);
            let response_first_chunk_elapsed_ms =
                LatencySummary::from_values(builder.response_first_chunk_elapsed_values);
            let response_body_elapsed_ms =
                LatencySummary::from_values(builder.response_body_elapsed_values);
            TraceHttpProviderMilestoneAggregate {
                provider,
                events: builder.events,
                successes: builder.successes,
                failures: builder.failures,
                bytes: builder.bytes,
                response_bytes: builder.response_bytes,
                first_chunk_events: builder.first_chunk_events,
                elapsed_ms,
                response_headers_elapsed_ms,
                response_first_chunk_elapsed_ms,
                response_body_elapsed_ms,
                total_ms: builder.total_ms,
                max_ms: builder.max_ms,
                max_response_headers_elapsed_ms: builder.max_response_headers_elapsed_ms,
                max_response_first_chunk_elapsed_ms: builder.max_response_first_chunk_elapsed_ms,
                max_response_body_elapsed_ms: builder.max_response_body_elapsed_ms,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .max_response_headers_elapsed_ms
            .cmp(&left.max_response_headers_elapsed_ms)
            .then_with(|| right.max_ms.cmp(&left.max_ms))
            .then_with(|| right.total_ms.cmp(&left.total_ms))
            .then_with(|| left.provider.cmp(&right.provider))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_http_provider_race_winners(
    counts: BTreeMap<String, TraceHttpProviderRaceWinnerProviderBuilder>,
) -> Vec<TraceHttpProviderRaceWinnerProviderAggregate> {
    let mut values = counts
        .into_iter()
        .map(
            |(provider, builder)| TraceHttpProviderRaceWinnerProviderAggregate {
                provider,
                events: builder.events,
                total_ms: builder.total_ms,
                max_ms: builder.max_ms,
            },
        )
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .max_ms
            .cmp(&left.max_ms)
            .then_with(|| right.total_ms.cmp(&left.total_ms))
            .then_with(|| right.events.cmp(&left.events))
            .then_with(|| left.provider.cmp(&right.provider))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_cids(counts: BTreeMap<String, TraceCidBuilder>) -> Vec<TraceCidAggregate> {
    let mut values = counts
        .into_iter()
        .map(|(cid, builder)| TraceCidAggregate {
            cid,
            count: builder.count,
            total_ms: builder.total_ms,
            max_ms: builder.max_ms,
            phases: sorted_trace_counts(builder.phases),
            paths: sorted_trace_counts(builder.paths),
            bitswap_source_candidate_indexes: sorted_trace_counts(
                builder.bitswap_source_candidate_indexes,
            ),
            bitswap_source_peers: sorted_trace_counts(builder.bitswap_source_peers),
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.max_ms.cmp(&left.max_ms))
            .then_with(|| left.cid.cmp(&right.cid))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn accumulate_trace_count(
    counts: &mut BTreeMap<String, usize>,
    label: &str,
    value: &serde_json::Value,
    field: &str,
) {
    let Some(count) = value.get(field).and_then(json_u128) else {
        return;
    };
    *counts.entry(label.to_string()).or_default() += count as usize;
}

fn trace_count_field(value: &serde_json::Value, field: &str) -> u128 {
    value.get(field).and_then(json_u128).unwrap_or_default()
}

fn sorted_trace_counts(counts: BTreeMap<String, usize>) -> Vec<TraceValueCount> {
    let mut values = counts
        .into_iter()
        .map(|(value, count)| TraceValueCount { value, count })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.value.cmp(&right.value))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_phase_latencies(values: BTreeMap<String, Vec<u128>>) -> Vec<TracePhaseAggregate> {
    let mut values = values
        .into_iter()
        .map(|(phase, elapsed_values)| {
            let count = elapsed_values.len();
            let total_ms = elapsed_values.iter().sum();
            let elapsed_ms = LatencySummary::from_values(elapsed_values);
            TracePhaseAggregate {
                phase,
                count,
                total_ms,
                elapsed_ms,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.elapsed_ms.max_ms.cmp(&left.elapsed_ms.max_ms))
            .then_with(|| left.phase.cmp(&right.phase))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_request_path_phase_latencies(
    values: BTreeMap<String, TraceRequestPathPhaseBuilder>,
) -> Vec<TraceRequestPathPhaseAggregate> {
    let mut values = values
        .into_iter()
        .map(|(phase, builder)| TraceRequestPathPhaseAggregate {
            phase,
            count: builder.count,
            total_ms: builder.total_ms,
            max_ms: builder.max_ms,
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.max_ms.cmp(&left.max_ms))
            .then_with(|| left.phase.cmp(&right.phase))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_source_latencies(
    values: BTreeMap<String, Vec<u128>>,
) -> Vec<TraceSourceLatencyAggregate> {
    let mut values = values
        .into_iter()
        .map(|(source, elapsed_values)| {
            let count = elapsed_values.len();
            let total_ms = elapsed_values.iter().sum();
            let elapsed_ms = LatencySummary::from_values(elapsed_values);
            TraceSourceLatencyAggregate {
                source,
                count,
                total_ms,
                elapsed_ms,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .total_ms
            .cmp(&left.total_ms)
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| left.source.cmp(&right.source))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn sorted_trace_delegated_provider_endpoints(
    endpoints: BTreeMap<String, TraceDelegatedProviderLookupAggregate>,
) -> Vec<TraceDelegatedProviderEndpointAggregate> {
    let mut values = endpoints
        .into_iter()
        .map(|(endpoint, mut aggregate)| {
            aggregate.finish();
            TraceDelegatedProviderEndpointAggregate {
                endpoint,
                events: aggregate.events,
                successes: aggregate.successes,
                failures: aggregate.failures,
                providers: aggregate.providers,
                http_providers: aggregate.http_providers,
                zero_http_provider_events: aggregate.zero_http_provider_events,
                single_http_provider_events: aggregate.single_http_provider_events,
                multi_http_provider_events: aggregate.multi_http_provider_events,
                single_http_provider_target_miss_events: aggregate
                    .single_http_provider_target_miss_events,
                self_hedges: aggregate.self_hedges,
                max_self_hedge_timeout_ms: aggregate.max_self_hedge_timeout_ms,
                response_bytes: aggregate.response_bytes,
                response_lines: aggregate.response_lines,
                first_chunk_events: aggregate.first_chunk_events,
                first_http_provider_events: aggregate.first_http_provider_events,
                target_met_events: aggregate.target_met_events,
                elapsed_ms: aggregate.elapsed_ms,
                response_headers_elapsed_ms: aggregate.response_headers_elapsed_ms,
                response_first_chunk_elapsed_ms: aggregate.response_first_chunk_elapsed_ms,
                response_first_http_provider_elapsed_ms: aggregate
                    .response_first_http_provider_elapsed_ms,
                response_target_met_elapsed_ms: aggregate.response_target_met_elapsed_ms,
                single_http_provider_elapsed_ms: aggregate.single_http_provider_elapsed_ms,
                single_http_provider_first_http_elapsed_ms: aggregate
                    .single_http_provider_first_http_elapsed_ms,
                max_elapsed_ms: aggregate.max_elapsed_ms,
                max_response_headers_elapsed_ms: aggregate.max_response_headers_elapsed_ms,
                max_response_first_chunk_elapsed_ms: aggregate.max_response_first_chunk_elapsed_ms,
                max_response_first_http_provider_elapsed_ms: aggregate
                    .max_response_first_http_provider_elapsed_ms,
                max_response_target_met_elapsed_ms: aggregate.max_response_target_met_elapsed_ms,
                max_single_http_provider_elapsed_ms: aggregate.max_single_http_provider_elapsed_ms,
                max_single_http_provider_first_http_elapsed_ms: aggregate
                    .max_single_http_provider_first_http_elapsed_ms,
            }
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right
            .events
            .cmp(&left.events)
            .then_with(|| right.failures.cmp(&left.failures))
            .then_with(|| right.max_elapsed_ms.cmp(&left.max_elapsed_ms))
            .then_with(|| left.endpoint.cmp(&right.endpoint))
    });
    values.truncate(MAX_TRACE_SLOW_EVENTS);
    values
}

fn trace_progress_phase<'a>(raw_phase: &'a str, value: &serde_json::Value) -> &'a str {
    match raw_phase {
        "request_start" | "preload_start" => "started",
        "gateway_stream_done" => "completed",
        "gateway_stream_failed" => "failed",
        "request_done" => match value.get("status").and_then(json_u128) {
            Some(status) if status >= 400 => "failed",
            Some(_) if value.get("body_mode").and_then(|mode| mode.as_str()) == Some("stream") => {
                "streaming"
            }
            _ => "completed",
        },
        "preload_done" => match value.get("ok").and_then(|ok| ok.as_bool()) {
            Some(false) => "failed",
            _ => "completed",
        },
        "preload_cancelled" => "cancelled",
        "block_store_get" => match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
            Some(true) => "cache_hit",
            _ => "checking_cache",
        },
        "gateway_small_body_cache" => match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
            Some(true) => "cache_hit",
            _ if value
                .get("cache_inserted")
                .and_then(|inserted| inserted.as_bool())
                == Some(true) =>
            {
                "streaming"
            }
            _ => "checking_cache",
        },
        "block_fetch_total" => match value.get("source").and_then(|source| source.as_str()) {
            Some("cache") => "cache_hit",
            Some("bitswap") => "fetching_bitswap",
            Some("http_provider") => "fetching_http_provider",
            _ => "streaming",
        },
        "block_range_batch_fetch" => match value.get("source").and_then(|source| source.as_str()) {
            Some("cache") => "cache_hit",
            Some("bitswap") => "fetching_bitswap",
            Some("http_provider") => "fetching_http_provider",
            _ => "streaming",
        },
        "block_fetch_coalesced" | "block_store_put" => "streaming",
        "name_cache" => match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
            Some(true) => "name_resolved",
            _ => "resolving_name",
        },
        "name_persistent_cache" => match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
            Some(true) => "name_resolved",
            _ => "resolving_name",
        },
        "name_resolve" => match value.get("ok").and_then(|ok| ok.as_bool()) {
            Some(false) => "failed",
            _ => "name_resolved",
        },
        "provider_cache" => match value.get("cache_hit").and_then(|hit| hit.as_bool()) {
            Some(true) if value.get("provider_count").and_then(json_u128) == Some(0) => "failed",
            Some(true) => "providers_found",
            _ => "provider_lookup",
        },
        "provider_lookup" if value.get("error").is_some() => "failed",
        "provider_lookup" => "providers_found",
        "provider_diversity_low" => "provider_diversity_low",
        "light_dht_provider_lookup" | "dht_provider_lookup" => "dht_fallback_started",
        "provider_fetch_start" => "providers_found",
        "delegated_provider_lookup"
        | "delegated_provider_self_hedge"
        | "delegated_provider_self_hedge_result"
        | "delegated_provider_empty_retry"
        | "bitswap_dns_prefetch"
        | "bitswap_dnsaddr_expand"
        | "bitswap_dns_multiaddr_expand" => "provider_lookup",
        "http_provider_fetch"
        | "http_provider_candidate_cancelled"
        | "http_provider_hedge"
        | "http_provider_race"
        | "http_provider_self_hedge"
        | "http_provider_self_hedge_skip"
        | "http_provider_bitswap_hedge_skip"
        | "http_provider_race_result" => "fetching_http_provider",
        "http_provider_bitswap_hedge" => "fetching_bitswap",
        "http_provider_bitswap_hedge_result" => {
            match value.get("source").and_then(|source| source.as_str()) {
                Some("bitswap") => "fetching_bitswap",
                _ => "fetching_http_provider",
            }
        }
        "bitswap_fetch"
        | "bitswap_connection_established"
        | "bitswap_incoming_block"
        | "bitswap_incoming_batch"
        | "bitswap_peer_attempt"
        | "bitswap_peer_attempt_cancelled"
        | "bitswap_peer_attempt_start"
        | "bitswap_peer_expand"
        | "bitswap_dial_plan"
        | "bitswap_session_shortcut"
        | "bitswap_session_shortcut_start"
        | "bitswap_session_shortcut_pre_lookup"
        | "bitswap_session_late_peer_wait"
        | "bitswap_session_shortcut_post_lookup_wait"
        | "bitswap_session_shortcut_post_lookup_race" => "fetching_bitswap",
        "bitswap_fetch_cancelled" => "cancelled",
        "bitswap_request_timeout_detail"
        | "retry_provider_count"
        | "provider_retry_after_connection_timeout"
        | "bitswap_connection_error_backoff"
        | "bitswap_connection_error_peer_skipped"
        | "bad_peer_skipped"
        | "bitswap_client_reset"
        | "bitswap_connection_error"
        | "bitswap_dial_rejected"
        | "bitswap_dial_waiters_dropped"
        | "bitswap_incoming_stream_read"
        | "bitswap_peer_timeout"
        | "bitswap_peer_timeout_suppressed"
        | "bitswap_provider_candidates_empty"
        | "bitswap_request_timeout"
        | "provider_retry_after_timeout"
        | "provider_retry_after_request_timeout"
        | "provider_refresh_after_timeout"
        | "provider_refresh_after_failure" => "retrying",
        "provider_refresh_skipped_empty_provider_set" => "failed",
        "ipfs_path_parse"
        | "gateway_direct_body"
        | "mime_total"
        | "mime_detect"
        | "mime_sniff_read"
        | "unixfs_resource"
        | "unixfs_metadata_cache"
        | "unixfs_file_size"
        | "unixfs_index_lookup"
        | "unixfs_list_directory" => "streaming",
        "gateway_conditional" => "cache_hit",
        "gateway_limiter"
            if value
                .get("acquired")
                .and_then(|acquired| acquired.as_bool())
                == Some(false) =>
        {
            "failed"
        }
        "gateway_limiter" => "queued",
        _ => raw_phase,
    }
}

fn format_trace_counts(counts: &[TraceValueCount]) -> String {
    counts
        .iter()
        .take(8)
        .map(|entry| format!("{}={}", entry.value, entry.count))
        .collect::<Vec<_>>()
        .join(", ")
}

fn merge_trace_counts(target: &mut BTreeMap<String, usize>, counts: &[TraceValueCount]) {
    for count in counts {
        *target.entry(count.value.clone()).or_default() += count.count;
    }
}

fn print_trace_progress_request_groups(trace: &TraceSummary) {
    if trace.progress_request_groups.is_empty() {
        return;
    }
    println!("  progress request groups:");
    for group in trace.progress_request_groups.iter().take(4) {
        let statuses = format_trace_counts(&group.statuses);
        let phases = format_trace_counts(&group.phases);
        println!(
            "    {}: root_progress_id={} requests={} children={} failed={} elapsed={} max_event={}ms statuses={} phases={}",
            group.top_level_path,
            group.root_progress_request_id.as_deref().unwrap_or("-"),
            group.request_count,
            group.child_request_count,
            group.failed_request_count,
            group.request_elapsed_ms,
            group.max_event_ms,
            statuses,
            phases
        );
        for request in group.slow_requests.iter().take(3) {
            let source_details = format_progress_request_source_details(request);
            println!(
                "      {}: {}ms status={} progress_id={} parent_progress_id={} max_event={}ms{}",
                request.path,
                request.elapsed_ms,
                request.status.as_deref().unwrap_or("unknown"),
                request.progress_request_id.as_deref().unwrap_or("-"),
                request.parent_progress_request_id.as_deref().unwrap_or("-"),
                request.max_event_ms,
                source_details
            );
        }
    }
}

fn format_request_correlation(request: &TraceRequestAggregate) -> String {
    if request.progress_request_id.is_none()
        && request.parent_progress_request_id.is_none()
        && request.top_level_path.is_none()
    {
        return String::new();
    }
    format!(
        " progress_id={} parent_progress_id={} top_level_path={}",
        request.progress_request_id.as_deref().unwrap_or("-"),
        request.parent_progress_request_id.as_deref().unwrap_or("-"),
        request.top_level_path.as_deref().unwrap_or("-")
    )
}

fn format_request_classification_details(request: &TraceRequestAggregate) -> String {
    if request.classifications.is_empty()
        && request.delegated_zero_http_provider_lookups == 0
        && request.bitswap_block_fetches == 0
        && request.bitswap_fetches == 0
        && request.cold_bitswap_peer_expands == 0
        && request.provider_diversity_low_events == 0
        && request.dht_provider_lookup_events == 0
    {
        return String::new();
    }
    let classifications = if request.classifications.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.classifications)
    };
    let source_indexes = if request.bitswap_source_candidate_indexes.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.bitswap_source_candidate_indexes)
    };
    let source_modes = if request.bitswap_source_request_modes.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.bitswap_source_request_modes)
    };
    let source_transports = if request.bitswap_source_transports.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.bitswap_source_transports)
    };
    let dht_details = format_trace_request_dht_details(request);
    format!(
        " classifications={} zero_http_lookups={} bitswap_blocks={} bitswap_fetches={} bitswap_elapsed={} bitswap_bytes={} cold_expands={} max_peers={} max_session_peers={} source_indexes={} source_modes={} source_transports={}{}",
        classifications,
        request.delegated_zero_http_provider_lookups,
        request.bitswap_block_fetches,
        request.bitswap_fetches,
        request.bitswap_fetch_elapsed_ms,
        request.bitswap_fetch_bytes,
        request.cold_bitswap_peer_expands,
        request.max_bitswap_peer_count,
        request.max_bitswap_session_peer_count,
        source_indexes,
        source_modes,
        source_transports,
        dht_details
    )
}

fn format_request_source_details(request: &TraceRequestAggregate) -> String {
    if request.block_sources.is_empty()
        && request.http_provider_fetches == 0
        && request.http_provider_fetch_providers.is_empty()
        && request.http_provider_fetch_error_classes.is_empty()
        && !request.unixfs_metadata_cache.has_events()
    {
        return String::new();
    }
    let block_sources = if request.block_sources.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.block_sources)
    };
    let http_providers = if request.http_provider_fetch_providers.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.http_provider_fetch_providers)
    };
    let http_errors = if request.http_provider_fetch_error_classes.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.http_provider_fetch_error_classes)
    };
    let http_milestones = format_request_http_milestones(request);
    let unixfs_cache_details = format_trace_unixfs_cache_details(&request.unixfs_metadata_cache);
    format!(
        " block_sources={} http_fetches={} ok={} fail={} http_elapsed={} http_providers={} http_errors={}{}{}",
        block_sources,
        request.http_provider_fetches,
        request.http_provider_fetch_successes,
        request.http_provider_fetch_failures,
        request.http_provider_fetch_elapsed_ms,
        http_providers,
        http_errors,
        http_milestones,
        unixfs_cache_details
    )
}

fn format_request_http_milestones(request: &TraceRequestAggregate) -> String {
    if request.http_provider_fetch_response_bytes == 0
        && request.http_provider_fetch_first_chunk_events == 0
        && request.http_provider_fetch_headers_elapsed_ms.count == 0
        && request.http_provider_fetch_first_chunk_elapsed_ms.count == 0
        && request.http_provider_fetch_body_elapsed_ms.count == 0
    {
        return String::new();
    }
    format!(
        " http_response_bytes={} http_first_chunks={} http_headers={} http_first_chunk={} http_body={}",
        request.http_provider_fetch_response_bytes,
        request.http_provider_fetch_first_chunk_events,
        request.http_provider_fetch_headers_elapsed_ms,
        request.http_provider_fetch_first_chunk_elapsed_ms,
        request.http_provider_fetch_body_elapsed_ms
    )
}

fn format_request_phase_latency_details(request: &TraceRequestAggregate) -> String {
    if request.phase_latencies.is_empty() {
        return String::new();
    }
    let phases = request
        .phase_latencies
        .iter()
        .take(4)
        .map(|phase| {
            format!(
                "{}:{} total={}ms",
                phase.phase, phase.elapsed_ms, phase.total_ms
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(" phase_latencies={phases}")
}

fn format_progress_request_source_details(request: &TraceProgressRequestAggregate) -> String {
    if request.block_sources.is_empty() && request.http_provider_fetch_providers.is_empty() {
        return String::new();
    }
    let block_sources = if request.block_sources.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.block_sources)
    };
    let http_providers = if request.http_provider_fetch_providers.is_empty() {
        "-".to_string()
    } else {
        format_trace_counts(&request.http_provider_fetch_providers)
    };
    format!(" block_sources={block_sources} http_providers={http_providers}")
}

fn trace_error_key(phase: &str, value: &serde_json::Value) -> Option<String> {
    if let Some(error) = json_detail_string(value.get("error")) {
        return Some(format!("{phase}: {error}"));
    }
    if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
        return Some(format!("{phase}: ok=false"));
    }
    None
}

fn http_provider_error_class(error: &str) -> &'static str {
    if error.contains("cid hash mismatch") {
        "cid_hash_mismatch"
    } else if error.contains("timed out") || error.contains("Timeout has been reached") {
        "timeout"
    } else if error.contains("429") {
        "http_429"
    } else if error.contains("404") {
        "http_404"
    } else if error.contains("500") || error.contains("502") || error.contains("503") {
        "http_5xx"
    } else if error.contains("redirect") {
        "redirect"
    } else {
        "other"
    }
}

fn bitswap_connection_error_class(error: &str) -> &'static str {
    if error.contains("Protocol negotiation failed") {
        "protocol_negotiation_failed"
    } else if error.contains("Connection refused") {
        "connection_refused"
    } else if error.contains("Connection reset by peer") {
        "connection_reset"
    } else if error.contains("No route to host") {
        "no_route_to_host"
    } else if error.contains("Timeout has been reached") {
        "timeout"
    } else {
        "other"
    }
}

fn bitswap_connection_error_addr_family(error: &str) -> &'static str {
    let has_ip4 = error.contains("/ip4/");
    let has_ip6 = error.contains("/ip6/");
    match (has_ip4, has_ip6) {
        (true, true) => "mixed",
        (true, false) => "ip4",
        (false, true) => "ip6",
        (false, false) => "unknown",
    }
}

fn trace_event_path(value: &serde_json::Value) -> Option<String> {
    json_detail_string(value.get("path"))
        .or_else(|| json_detail_string(value.get("span").and_then(|span| span.get("path"))))
}

fn trace_span_string(value: &serde_json::Value, key: &str) -> Option<String> {
    json_detail_string(value.get(key))
        .or_else(|| json_detail_string(value.get("span").and_then(|span| span.get(key))))
}

fn trace_parent_progress_request_id(value: &serde_json::Value) -> Option<String> {
    trace_span_string(value, "parent_request_id").filter(|id| id != "0")
}

fn trace_request_key(value: &serde_json::Value) -> Option<TraceRequestKey> {
    let request_id = json_detail_string(value.get("request_id")).or_else(|| {
        json_detail_string(value.get("span").and_then(|span| span.get("request_id")))
    })?;
    let process_id = json_detail_string(value.get("process_id"))
        .or_else(|| json_detail_string(value.get("span").and_then(|span| span.get("process_id"))))
        .unwrap_or_default();
    let path = trace_event_path(value)?;
    Some(TraceRequestKey {
        process_id,
        request_id,
        path,
    })
}

fn trace_event_details(value: &serde_json::Value) -> BTreeMap<String, String> {
    let mut details = BTreeMap::new();
    for key in [
        "cid",
        "path",
        "unixfs_path",
        "name",
        "resolved_target",
        "source",
        "source_peer",
        "source_peer_trusted",
        "bitswap_delivery",
        "extra_blocks",
        "ok",
        "error",
        "status",
        "provider_count",
        "provider_peer_count",
        "provider_addr_count",
        "expanded_provider_addr_count",
        "supported_provider_addr_count",
        "rejected_provider_addr_count",
        "id_only_provider_count",
        "invalid_provider_id_count",
        "provider_without_supported_bitswap_addr_count",
        "unsupported_relay_addr_count",
        "unsupported_webtransport_addr_count",
        "unsupported_webrtc_addr_count",
        "unsupported_certhash_addr_count",
        "unsupported_transport_addr_count",
        "missing_peer_addr_count",
        "unparsable_addr_count",
        "addr_with_relay_count",
        "addr_with_webtransport_count",
        "addr_with_webrtc_count",
        "addr_with_certhash_count",
        "session_peer_count",
        "peer_count",
        "trusted_peer_count",
        "timeout_ms",
        "tcp_addr_count",
        "quic_addr_count",
        "ws_addr_count",
        "wss_addr_count",
        "dns_addr_count",
        "ip4_addr_count",
        "ip6_addr_count",
        "block_count",
        "bytes",
        "body_len",
        "body_mode",
        "chunks",
        "pending_waiter_count",
        "oldest_pending_ms",
        "newest_pending_ms",
        "cache_hit",
        "process_id",
        "request_id",
        "dnsaddr_host_count",
        "dns_ip_host_count",
        "peer",
        "prefer_want_have",
        "want_have_timeout_ms",
        "stream_read_timeout_ms",
        "candidate_peer_count",
        "new_dial_peer_count",
        "new_dial_addr_count",
        "suppressed_dial_peer_count",
        "suppressed_dial_addr_count",
        "pending_dial_peer_count",
        "connected_peer_count",
        "failure_kind",
        "command_queued_ms",
        "targets",
    ] {
        if let Some(detail) = json_detail_string(value.get(key)) {
            details.insert(key.to_string(), detail);
        }
    }
    if !details.contains_key("path") {
        if let Some(span_path) = trace_event_path(value) {
            details.insert("path".to_string(), span_path);
        }
    }
    if !details.contains_key("request_id") {
        if let Some(request_id) =
            json_detail_string(value.get("span").and_then(|span| span.get("request_id")))
        {
            details.insert("request_id".to_string(), request_id);
        }
    }
    if !details.contains_key("process_id") {
        if let Some(process_id) =
            json_detail_string(value.get("span").and_then(|span| span.get("process_id")))
        {
            details.insert("process_id".to_string(), process_id);
        }
    }
    for key in ["progress_request_id", "parent_request_id", "top_level_path"] {
        if !details.contains_key(key) {
            if let Some(detail) = trace_span_string(value, key) {
                details.insert(key.to_string(), detail);
            }
        }
    }
    details
}

fn json_detail_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    let rendered = match value {
        serde_json::Value::Null => return None,
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).ok()?,
    };
    if rendered.chars().count() > 240 {
        let prefix = rendered.chars().take(240).collect::<String>();
        Some(format!("{prefix}..."))
    } else {
        Some(rendered)
    }
}

fn json_u128(value: &serde_json::Value) -> Option<u128> {
    if let Some(value) = value.as_u64() {
        return Some(value as u128);
    }
    if let Some(value) = value.as_f64() {
        if value.is_finite() && value >= 0.0 {
            return Some(value.round() as u128);
        }
    }
    if let Some(value) = value.as_str() {
        return value.parse::<u128>().ok();
    }
    None
}

#[derive(Debug, Serialize)]
struct AssetKindFailure {
    kind: String,
    count: usize,
}

#[derive(Debug, Serialize)]
struct FailureGroup {
    key: String,
    count: usize,
    examples: Vec<String>,
}

#[derive(Debug)]
struct FailureGroupBuilder {
    key: String,
    count: usize,
    examples: Vec<String>,
}

impl FailureGroupBuilder {
    fn finish(self) -> FailureGroup {
        FailureGroup {
            key: self.key,
            count: self.count,
            examples: self.examples,
        }
    }
}

fn push_failure_group(
    groups: &mut BTreeMap<String, FailureGroupBuilder>,
    key: String,
    example: String,
) {
    let group = groups
        .entry(key.clone())
        .or_insert_with(|| FailureGroupBuilder {
            key,
            count: 0,
            examples: Vec::new(),
        });
    group.count += 1;
    if group.examples.len() < 5 && !group.examples.iter().any(|seen| seen == &example) {
        group.examples.push(example);
    }
}

fn rate(count: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        count as f64 / total as f64
    }
}

fn percentile(values: &[u128], percentile: usize) -> Option<u128> {
    if values.is_empty() {
        return None;
    }
    let rank = (values.len() * percentile).div_ceil(100).max(1);
    values.get(rank - 1).copied()
}

fn percentile_u64(values: &[u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let rank = (values.len() * percentile).div_ceil(100).max(1);
    values.get(rank - 1).copied()
}

#[derive(Debug, Serialize)]
struct CaseResult {
    id: String,
    description: Option<String>,
    method: String,
    url: String,
    status: Option<u16>,
    content_type: Option<String>,
    content_range: Option<String>,
    content_length: Option<u64>,
    accept_ranges: Option<String>,
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
    stream: FetchStreamMetrics,
    body_preview: String,
    revalidation: Option<RevalidationResult>,
    asset_summary: Option<AssetSummary>,
    assets: Vec<AssetResult>,
    passed: bool,
    failures: Vec<String>,
}

impl CaseResult {
    fn failed(entry: &CorpusEntry, url: String, failures: Vec<String>) -> Self {
        Self {
            id: entry.id.clone(),
            description: entry.description.clone(),
            method: entry.method.clone().unwrap_or_else(|| "GET".to_string()),
            url,
            status: None,
            content_type: None,
            content_range: None,
            content_length: None,
            accept_ranges: None,
            etag: None,
            cache_control: None,
            body_bytes: 0,
            ttfb_ms: 0,
            total_ms: 0,
            stream: FetchStreamMetrics::default(),
            body_preview: String::new(),
            revalidation: None,
            asset_summary: None,
            assets: Vec::new(),
            passed: false,
            failures,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct FetchStreamMetrics {
    chunk_count: usize,
    max_chunk_bytes: usize,
    max_buffered_bytes: usize,
    first_byte_ms: Option<u128>,
    completed: bool,
    cancelled: bool,
}

struct FetchResponse {
    status: u16,
    content_type: Option<String>,
    content_range: Option<String>,
    content_length: Option<u64>,
    accept_ranges: Option<String>,
    etag: Option<String>,
    cache_control: Option<String>,
    body: Vec<u8>,
    ttfb_ms: u128,
    total_ms: u128,
    stream: FetchStreamMetrics,
}

#[derive(Debug, Serialize)]
struct RevalidationResult {
    status: Option<u16>,
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
    stream: FetchStreamMetrics,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AssetKind {
    Stylesheet,
    Script,
    Image,
    Font,
    Audio,
    Video,
    Manifest,
    Other,
}

impl std::fmt::Display for AssetKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Stylesheet => "stylesheet",
            Self::Script => "script",
            Self::Image => "image",
            Self::Font => "font",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Manifest => "manifest",
            Self::Other => "other",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Default, Serialize)]
struct AssetSummary {
    discovered: usize,
    fetched: usize,
    passed: usize,
    failed: usize,
    skipped_external: usize,
    skipped_unsupported: usize,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct AssetResult {
    kind: AssetKind,
    source: String,
    url: String,
    status: Option<u16>,
    content_type: Option<String>,
    content_range: Option<String>,
    content_length: Option<u64>,
    accept_ranges: Option<String>,
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
    stream: FetchStreamMetrics,
    body_preview: String,
    revalidation: Option<RevalidationResult>,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone)]
struct DiscoveredAsset {
    kind: AssetKind,
    source: String,
    url: Url,
}

struct FetchedAsset {
    result: AssetResult,
    body_text: Option<String>,
}

#[derive(Default)]
struct AssetDiscovery {
    assets: Vec<DiscoveredAsset>,
    discovered: usize,
    skipped_external: usize,
    skipped_unsupported: usize,
    truncated: bool,
}

struct ParsedTag {
    name: String,
    attrs: Vec<(String, String)>,
}

impl ParsedTag {
    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(attr_name, _)| attr_name == name)
            .map(|(_, value)| value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{
        cid_from_data, Block, BlockProvider, CoreError, Result as CoreResult, CODEC_RAW,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn gateway_specific_header_expectations_apply_only_to_rust() {
        assert!(gateway_specific_header_expectations_enabled(
            HarnessEngine::RustHttp
        ));
        assert!(gateway_specific_header_expectations_enabled(
            HarnessEngine::RustNative
        ));
        assert!(!gateway_specific_header_expectations_enabled(
            HarnessEngine::Kubo
        ));
    }

    #[test]
    fn args_accept_delegated_router_endpoint_list() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--delegated-router",
            "https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1",
        ])
        .unwrap();

        assert_eq!(
            args.delegated_router.as_deref(),
            Some("https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1")
        );
    }

    #[test]
    fn args_accept_build_gateway_flag() {
        let args = Args::try_parse_from(["mobile-web-harness", "--build-gateway"]).unwrap();
        assert!(args.build_gateway);
    }

    #[test]
    fn args_accept_trace_span_list_flag() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--trace-output",
            "/tmp/mobile-trace.jsonl",
            "--trace-span-list",
        ])
        .unwrap();

        assert_eq!(
            args.trace_output.as_deref(),
            Some(Path::new("/tmp/mobile-trace.jsonl"))
        );
        assert!(args.trace_span_list);
    }

    #[test]
    fn args_accept_request_classification_requirements() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--trace-output",
            "/tmp/mobile-trace.jsonl",
            "--require-request-classification",
            "zero_http_provider_cold_bitswap=3",
            "--require-request-classification",
            "top_level_zero_http_provider_cold_bitswap=1",
        ])
        .unwrap();

        assert_eq!(
            args.require_request_classifications,
            vec![
                "zero_http_provider_cold_bitswap=3",
                "top_level_zero_http_provider_cold_bitswap=1"
            ]
        );
    }

    #[test]
    fn args_accept_progress_phase_requirements() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--trace-output",
            "/tmp/mobile-trace.jsonl",
            "--require-progress-phase",
            "provider_lookup=1",
            "--require-progress-phase",
            "fetching_bitswap=3",
        ])
        .unwrap();

        assert_eq!(
            args.require_progress_phases,
            vec!["provider_lookup=1", "fetching_bitswap=3"]
        );
    }

    #[test]
    fn args_accept_gateway_import_car() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--gateway-import-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();

        assert_eq!(
            args.gateway_import_car.as_deref(),
            Some(Path::new("/tmp/mobile-fixture.car"))
        );
    }

    #[test]
    fn args_accept_bitswap_seed_car() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();

        assert_eq!(
            args.bitswap_seed_car.as_deref(),
            Some(Path::new("/tmp/mobile-fixture.car"))
        );
    }

    #[test]
    fn args_accept_prepare_synthetic_multiblock_range_fixture() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--prepare-synthetic-multiblock-range-fixture",
            "/tmp/synthetic-range-fixture",
        ])
        .unwrap();

        assert_eq!(
            args.prepare_synthetic_multiblock_range_fixture.as_deref(),
            Some(Path::new("/tmp/synthetic-range-fixture"))
        );
    }

    #[test]
    fn args_accept_native_and_legacy_rust_engines() {
        let native = Args::try_parse_from(["mobile-web-harness", "--engine", "rust-native"])
            .expect("parse rust-native engine");
        assert_eq!(native.engine, HarnessEngine::RustNative);

        let ffi = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--native-dispatchers",
            "4",
            "--native-read-buffer-bytes",
            "8192",
            "--native-slow-consumer-ms",
            "3",
            "--native-cancel-after-first-byte",
            "--native-max-active-requests",
            "12",
            "--ens-corpus",
            "docs/mobile-web-readiness/ens-live-corpus.txt",
        ])
        .expect("parse rust-native-ffi engine");
        assert_eq!(ffi.engine, HarnessEngine::RustNativeFfi);
        assert_eq!(ffi.native_dispatchers, 4);
        assert_eq!(ffi.native_read_buffer_bytes, 8192);
        assert_eq!(ffi.native_slow_consumer_ms, 3);
        assert!(ffi.native_cancel_after_first_byte);
        assert_eq!(ffi.native_max_active_requests, Some(12));
        assert_eq!(
            ffi.ens_corpus.as_deref(),
            Some(Path::new("docs/mobile-web-readiness/ens-live-corpus.txt"))
        );

        let legacy = Args::try_parse_from(["mobile-web-harness", "--engine", "rust"]).unwrap();
        assert_eq!(legacy.engine, HarnessEngine::RustHttp);
    }

    #[test]
    fn ens_live_corpus_entries_use_gateway_paths() {
        assert_eq!(
            contenthash_to_gateway_path("ipfs://bafyroot/index.html").unwrap(),
            "/ipfs/bafyroot/index.html"
        );
        assert_eq!(
            contenthash_to_gateway_path("ipfs://ipfs/bafyroot").unwrap(),
            "/ipfs/bafyroot"
        );
        assert_eq!(
            contenthash_to_gateway_path("ipns://example.com").unwrap(),
            "/ipns/example.com"
        );
        assert!(contenthash_to_gateway_path("https://example.com").is_none());

        let entry = ens_live_corpus_entry("Beta.WalletBeat.eth", "/ipfs/bafyroot");
        assert_eq!(entry.id, "ens-beta-walletbeat-eth");
        assert_eq!(entry.path, "/ipfs/bafyroot");
        assert_eq!(entry.default_enabled, Some(true));
        assert_eq!(entry.expect_status, Some(200));
        assert_eq!(entry.min_bytes, Some(1));
        assert!(entry.crawl.is_some());
    }

    #[test]
    fn synthetic_multiblock_range_bytes_are_stable() {
        let data = synthetic_multiblock_range_bytes();
        let range_end = SYNTHETIC_MULTIBLOCK_RANGE_START + SYNTHETIC_MULTIBLOCK_RANGE_BYTES - 1;
        let range_sha256 = format!(
            "{:x}",
            Sha256::digest(&data[SYNTHETIC_MULTIBLOCK_RANGE_START..=range_end])
        );

        assert_eq!(data.len(), SYNTHETIC_MULTIBLOCK_FILE_BYTES);
        assert_eq!(
            range_sha256,
            "008247ddb836acb6aaeea63a8d0a3b0ddcc6384bd838280d792e04de9de09df9"
        );
    }

    #[test]
    fn bitswap_seed_connection_setup_describes_engine_setup() {
        let rust_args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        assert_eq!(
            bitswap_seed_connection_setup(&rust_args),
            Some(BitswapSeedConnectionSetup::DelegatedRouterProviderLookup)
        );

        let native_args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        assert_eq!(
            bitswap_seed_connection_setup(&native_args),
            Some(BitswapSeedConnectionSetup::DelegatedRouterProviderLookup)
        );

        let kubo_args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "kubo",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        assert_eq!(
            bitswap_seed_connection_setup(&kubo_args),
            Some(BitswapSeedConnectionSetup::SwarmConnectBeforeRequest)
        );
    }

    #[tokio::test]
    async fn gateway_import_car_requires_spawned_gateway() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--gateway-url",
            "http://127.0.0.1:50017",
            "--gateway-import-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: Vec::new(),
        };

        let err = run_harness(&args, &corpus).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("--gateway-import-car can only be used when the harness spawns the gateway"));
    }

    #[tokio::test]
    async fn gateway_import_car_requires_readable_file() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "missing-mobile-fixture-{}-{}.car",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_arg = path.display().to_string();
        let args = Args::try_parse_from([
            "mobile-web-harness".to_string(),
            "--gateway-import-car".to_string(),
            path_arg,
        ])
        .unwrap();
        let corpus = Corpus {
            entries: Vec::new(),
        };

        let err = run_harness(&args, &corpus).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("--gateway-import-car must point to a readable CAR file"));
    }

    #[tokio::test]
    async fn bitswap_seed_car_requires_spawned_gateway() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--gateway-url",
            "http://127.0.0.1:50017",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: Vec::new(),
        };

        let err = run_harness(&args, &corpus).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("--bitswap-seed-car can only be used when the harness spawns the gateway"));
    }

    #[tokio::test]
    async fn bitswap_seed_car_rejects_gateway_import_car() {
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--gateway-import-car",
            "/tmp/mobile-fixture.car",
            "--bitswap-seed-car",
            "/tmp/mobile-fixture.car",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: Vec::new(),
        };

        let err = run_harness(&args, &corpus).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("--gateway-import-car cannot be combined with --bitswap-seed-car"));
    }

    #[tokio::test]
    async fn bitswap_seed_car_requires_readable_file() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "missing-bitswap-seed-{}-{}.car",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path_arg = path.display().to_string();
        let args = Args::try_parse_from([
            "mobile-web-harness".to_string(),
            "--bitswap-seed-car".to_string(),
            path_arg,
        ])
        .unwrap();
        let corpus = Corpus {
            entries: Vec::new(),
        };

        let err = run_harness(&args, &corpus).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("--bitswap-seed-car must point to a readable CAR file"));
    }

    #[tokio::test]
    async fn bitswap_seed_delegated_router_returns_provider_record() {
        let (endpoint, task) = spawn_bitswap_seed_delegated_router(
            "peer-id".to_string(),
            "/ip4/127.0.0.1/tcp/4001/p2p/peer-id".to_string(),
        )
        .await
        .unwrap();

        let body = reqwest::get(format!("{endpoint}/providers/bafytest"))
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(value["Providers"][0]["ID"], "peer-id");
        assert_eq!(
            value["Providers"][0]["Addrs"][0],
            "/ip4/127.0.0.1/tcp/4001/p2p/peer-id"
        );

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn request_correlation_headers_include_parent_and_top_level_path() {
        let root = RequestCorrelation::root("/ipns/site/".to_string());
        let child = root.child();
        let request = apply_correlation_headers(
            reqwest::Client::new().get("http://127.0.0.1/"),
            Some(&child),
        )
        .build()
        .unwrap();

        assert_ne!(child.request_id, root.request_id);
        assert_eq!(child.parent_id, Some(root.request_id));
        assert_eq!(
            request
                .headers()
                .get(X_FREEDOM_REQUEST_ID)
                .unwrap()
                .to_str()
                .unwrap(),
            child.request_id.to_string()
        );
        assert_eq!(
            request
                .headers()
                .get(X_FREEDOM_PARENT_REQUEST_ID)
                .unwrap()
                .to_str()
                .unwrap(),
            root.request_id.to_string()
        );
        assert_eq!(
            request
                .headers()
                .get(X_FREEDOM_TOP_LEVEL_PATH)
                .unwrap()
                .to_str()
                .unwrap(),
            "/ipns/site/"
        );
    }

    #[test]
    fn parses_linux_proc_stat_parent_pid() {
        assert_eq!(
            parse_proc_stat_ppid("12345 (freedom-ipfs) S 42 1 1 0 -1 4194560"),
            Some(42)
        );
        assert_eq!(
            parse_proc_stat_ppid("12345 (name with spaces) R 4242 1 1 0"),
            Some(4242)
        );
        assert_eq!(parse_proc_stat_ppid("not a stat line"), None);
    }

    #[test]
    fn trace_summary_includes_slowest_events_with_details() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":9,\"path\":\"/ipns/site/asset.js\",\"span\":{\"path\":\"/ipns/site/asset.js\",\"request_id\":9,\"progress_request_id\":77,\"parent_request_id\":1,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":\"25\",\"cid\":\"cid1\",\"ok\":true,\"bytes\":100,\"extra_blocks\":2,\"source\":\"bitswap\",\"source_peer\":\"peer1\",\"source_transport\":\"tcp\",\"bitswap_delivery\":\"incoming\",\"source_peer_trusted\":true,\"source_peer_candidate_index\":4,\"source_peer_addr_index\":1,\"source_peer_addr_family\":\"ip4\",\"source_peer_addr_known\":true,\"source_peer_addr_matches_candidate\":true,\"trusted_peer_count\":1,\"provider_peer_count\":2,\"session_peer_count\":0,\"span\":{\"path\":\"/ipns/site/asset.js\",\"request_id\":9,\"progress_request_id\":77,\"parent_request_id\":1,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":9,\"path\":\"/ipns/site/asset.js\",\"status\":200,\"elapsed_ms\":1}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":5,\"cid\":\"cid1\",\"source\":\"bitswap\"}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":4,\"cid\":\"cid5\",\"source\":\"cache\"}\n",
                "{\"phase\":\"provider_lookup\",\"elapsed_ms\":10,\"cid\":\"cid2\",\"provider_count\":3,\"error\":\"dht: timeout\"}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid2\",\"provider_count\":1,\"bitswap_provider_count\":1,\"min_bitswap_provider_count\":2,\"fallback\":\"light_dht\"}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid2\",\"provider_count\":3,\"dht_provider_count\":2,\"bitswap_provider_count\":2,\"fallback\":\"light_dht\"}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid2\",\"bitswap_provider_count\":1,\"fallback\":\"light_dht\",\"ok\":false,\"timeout_ms\":750}\n",
                "{\"phase\":\"dht_provider_lookup\",\"elapsed_ms\":7,\"cid\":\"cid2\",\"ok\":false,\"error\":\"low diversity DHT fallback timed out\",\"fallback\":\"light_dht\",\"cancelled\":true,\"max_providers\":4,\"timeout_ms\":750,\"query_timeout_ms\":10000}\n",
                "{\"phase\":\"dht_provider_lookup\",\"elapsed_ms\":6,\"cid\":\"cid2\",\"ok\":false,\"error\":\"dht: timed out\",\"max_providers\":4,\"timeout_ms\":750}\n",
                "{\"phase\":\"dht_provider_lookup\",\"elapsed_ms\":20,\"cid\":\"cid3\",\"ok\":true,\"provider_count\":2,\"max_providers\":4,\"timeout_ms\":750}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":12,\"cid\":\"cid6\",\"ok\":false,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"elapsed_ms\":3,\"cid\":\"cid7\",\"tcp_addr_count\":4,\"quic_addr_count\":2,\"ws_addr_count\":1,\"wss_addr_count\":0,\"dns_addr_count\":1,\"ip4_addr_count\":3,\"ip6_addr_count\":1,\"provider_addr_count\":10,\"expanded_provider_addr_count\":12,\"supported_provider_addr_count\":4,\"rejected_provider_addr_count\":8,\"id_only_provider_count\":1,\"invalid_provider_id_count\":2,\"provider_without_supported_bitswap_addr_count\":3,\"unsupported_relay_addr_count\":4,\"unsupported_webtransport_addr_count\":1,\"unsupported_webrtc_addr_count\":1,\"unsupported_certhash_addr_count\":1,\"unsupported_transport_addr_count\":1,\"missing_peer_addr_count\":1,\"unparsable_addr_count\":1,\"addr_with_relay_count\":6,\"addr_with_webtransport_count\":2,\"addr_with_webrtc_count\":3,\"addr_with_certhash_count\":4}\n",
                "{\"phase\":\"bitswap_session_shortcut_start\",\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid8\",\"timeout_ms\":100}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":2,\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":true,\"source_peer\":\"peer1\",\"bitswap_delivery\":\"outgoing\",\"source_peer_trusted\":true,\"extra_blocks\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":3,\"cid\":\"cid9\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":false,\"timeout\":true}\n",
                "{\"phase\":\"bitswap_connection_established\",\"peer\":\"peer1\",\"remote_addr\":\"/ip4/127.0.0.1/tcp/4001\",\"transport\":\"tcp\",\"established_ms\":\"44\",\"wait_elapsed_ms\":\"45\",\"failed_dial_count\":2}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"peer3\",\"error\":\"Failed to negotiate transport protocol(s): [(/ip6/2001:db8::1/tcp/4001/p2p/peer3: Protocol negotiation failed.)]\"}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"\",\"error\":\"Failed to negotiate transport protocol(s): [(/ip4/127.0.0.1/tcp/4001: Connection refused (os error 111))]\"}\n",
                "{\"phase\":\"bitswap_connection_error_backoff\",\"peer\":\"peer3\",\"error_class\":\"protocol_negotiation_failed\",\"count\":2,\"ttl_ms\":30000}\n",
                "{\"phase\":\"bitswap_connection_error_peer_skipped\",\"cid\":\"cid-skip\",\"peer\":\"peer3\",\"remaining_ms\":25000}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer2\",\"transport\":\"quic\",\"connection_limit\":true,\"error\":\"Dial error\"}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":false,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":true,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bad.example\",\"cached\":false,\"ok\":false,\"record_count\":0}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":false,\"ip_count\":2}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":true,\"ip_count\":2}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"elapsed_ms\":0,\"hits\":3,\"misses\":2,\"inserts\":2,\"evictions\":1,\"oversized_skips\":0,\"cache_len\":4,\"path_hits\":5,\"path_misses\":7,\"path_inserts\":6,\"path_evictions\":1,\"path_oversized_skips\":0,\"path_cache_len\":6,\"file_size_hits\":8,\"file_size_misses\":9,\"file_size_inserts\":9,\"file_size_evictions\":2,\"file_size_cache_len\":7,\"cache_capacity\":256}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid10\",\"cache_hit\":false}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid11\",\"cache_hit\":true,\"rechecked\":true}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid12\",\"cache_hit\":false,\"rechecked\":true}\n",
                "not json\n",
                "{\"phase\":\"request_start\",\"path\":\"/ipns/site/\"}\n",
                "{\"phase\":\"gateway_limiter\",\"acquired\":false,\"timeout_ms\":2000,\"elapsed_ms\":2}\n",
                "{\"phase\":\"request_done\",\"status\":503}\n",
                "{\"phase\":\"request_done\",\"status\":200}\n",
                "{\"phase\":\"unixfs_file_size\",\"elapsed_ms\":50,\"cid\":\"cid3\",\"path\":\"/ipfs/root/index.html\",\"unixfs_path\":\"index.html\",\"ok\":true}\n",
                "{\"phase\":\"bitswap_request_timeout_detail\",\"elapsed_ms\":60,\"cid\":\"cid4\",\"peer_count\":16,\"trusted_peer_count\":2,\"timeout_ms\":4000,\"targets\":\"peer@[/ip4/127.0.0.1/tcp/4001]\"}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.line_count, 40);
        assert_eq!(summary.event_count, 39);
        assert_eq!(summary.slow_events.len(), 16);
        assert_eq!(
            summary.slow_events[0].phase,
            "bitswap_request_timeout_detail"
        );
        assert_eq!(summary.slow_events[0].elapsed_ms, 60);
        assert_eq!(
            summary.slow_events[0].details.get("trusted_peer_count"),
            Some(&"2".to_string())
        );
        assert_eq!(
            summary.slow_events[0].details.get("timeout_ms"),
            Some(&"4000".to_string())
        );
        assert_eq!(
            summary.slow_events[0].details.get("targets"),
            Some(&"peer@[/ip4/127.0.0.1/tcp/4001]".to_string())
        );
        assert_eq!(summary.slow_events[1].phase, "unixfs_file_size");
        assert_eq!(summary.slow_events[1].elapsed_ms, 50);
        assert_eq!(
            summary.slow_events[1].details.get("unixfs_path"),
            Some(&"index.html".to_string())
        );
        assert_eq!(summary.slow_events[2].phase, "bitswap_fetch");
        assert_eq!(
            summary.slow_events[2].details.get("path"),
            Some(&"/ipns/site/asset.js".to_string())
        );
        assert_eq!(
            summary.slow_events[2].details.get("request_id"),
            Some(&"9".to_string())
        );
        assert_eq!(
            summary.slow_events[2].details.get("source_peer_trusted"),
            Some(&"true".to_string())
        );
        assert_eq!(summary.phases.len(), 12);
        assert_eq!(summary.block_sources.len(), 2);
        assert_eq!(summary.block_sources[0].value, "bitswap");
        assert_eq!(summary.block_sources[0].count, 1);
        assert_eq!(summary.block_sources[1].value, "cache");
        assert_eq!(summary.block_fetch_source_latencies.len(), 2);
        assert_eq!(summary.block_fetch_source_latencies[0].source, "bitswap");
        assert_eq!(summary.block_fetch_source_latencies[0].count, 1);
        assert_eq!(summary.block_fetch_source_latencies[0].total_ms, 5);
        assert_eq!(
            summary.block_fetch_source_latencies[0].elapsed_ms.p50_ms,
            Some(5)
        );
        assert_eq!(summary.block_fetch_source_latencies[1].source, "cache");
        assert_eq!(summary.block_fetch_source_latencies[1].count, 1);
        assert_eq!(summary.block_fetch_source_latencies[1].total_ms, 4);
        assert_eq!(summary.block_store.events, 3);
        assert_eq!(summary.block_store.hits, 1);
        assert_eq!(summary.block_store.misses, 2);
        assert_eq!(summary.block_store.rechecks, 2);
        assert_eq!(summary.block_store.recheck_hits, 1);
        assert_eq!(summary.block_store.recheck_misses, 1);
        assert_eq!(summary.request_statuses.len(), 2);
        assert_eq!(summary.request_statuses[0].value, "200");
        assert_eq!(summary.request_statuses[0].count, 2);
        assert_eq!(summary.request_statuses[1].value, "503");
        assert_eq!(summary.gateway_limiter_denials, 1);
        assert_eq!(summary.gateway_limiter.events, 1);
        assert_eq!(summary.gateway_limiter.acquired, 0);
        assert_eq!(summary.gateway_limiter.denied, 1);
        assert_eq!(summary.gateway_limiter.elapsed_ms.p50_ms, Some(2));
        assert_eq!(summary.gateway_limiter.denied_elapsed_ms.p50_ms, Some(2));
        assert_eq!(summary.gateway_limiter.max_timeout_ms, 2000);
        assert_eq!(summary.gateway_request_elapsed_ms.count, 1);
        assert_eq!(summary.gateway_request_elapsed_ms.p50_ms, Some(1));
        assert_eq!(summary.gateway_request_elapsed_ms.p95_ms, Some(1));
        assert_eq!(summary.provider_diversity_low.events, 3);
        assert_eq!(summary.provider_diversity_low.failures, 1);
        assert_eq!(summary.provider_diversity_low.provider_count_total, 4);
        assert_eq!(
            summary.provider_diversity_low.bitswap_provider_count_total,
            4
        );
        assert_eq!(summary.provider_diversity_low.dht_provider_count_total, 2);
        assert_eq!(summary.provider_diversity_low.max_provider_count, 3);
        assert_eq!(summary.provider_diversity_low.max_bitswap_provider_count, 2);
        assert_eq!(summary.provider_diversity_low.max_dht_provider_count, 2);
        assert_eq!(summary.provider_diversity_low.max_timeout_ms, 750);
        assert_eq!(
            summary.provider_diversity_low.fallbacks[0].value,
            "light_dht"
        );
        assert_eq!(summary.provider_diversity_low.fallbacks[0].count, 3);
        assert_eq!(summary.dht_provider_lookup.events, 3);
        assert_eq!(summary.dht_provider_lookup.successes, 1);
        assert_eq!(summary.dht_provider_lookup.failures, 2);
        assert_eq!(summary.dht_provider_lookup.providers, 2);
        assert_eq!(summary.dht_provider_lookup.max_providers, 4);
        assert_eq!(summary.dht_provider_lookup.max_timeout_ms, 750);
        assert_eq!(summary.dht_provider_lookup.max_query_timeout_ms, 10000);
        assert_eq!(summary.dht_provider_lookup.max_elapsed_ms, 20);
        assert_eq!(summary.unixfs_metadata_cache.events, 1);
        assert_eq!(summary.unixfs_metadata_cache.hits, 3);
        assert_eq!(summary.unixfs_metadata_cache.misses, 2);
        assert_eq!(summary.unixfs_metadata_cache.inserts, 2);
        assert_eq!(summary.unixfs_metadata_cache.evictions, 1);
        assert_eq!(summary.unixfs_metadata_cache.oversized_skips, 0);
        assert_eq!(summary.unixfs_metadata_cache.max_len, 4);
        assert_eq!(summary.unixfs_metadata_cache.path_hits, 5);
        assert_eq!(summary.unixfs_metadata_cache.path_misses, 7);
        assert_eq!(summary.unixfs_metadata_cache.path_inserts, 6);
        assert_eq!(summary.unixfs_metadata_cache.path_evictions, 1);
        assert_eq!(summary.unixfs_metadata_cache.path_oversized_skips, 0);
        assert_eq!(summary.unixfs_metadata_cache.max_path_len, 6);
        assert_eq!(summary.unixfs_metadata_cache.file_size_hits, 8);
        assert_eq!(summary.unixfs_metadata_cache.file_size_misses, 9);
        assert_eq!(summary.unixfs_metadata_cache.file_size_inserts, 9);
        assert_eq!(summary.unixfs_metadata_cache.file_size_evictions, 2);
        assert_eq!(summary.unixfs_metadata_cache.max_file_size_len, 7);
        assert_eq!(summary.unixfs_metadata_cache.max_capacity, 256);
        assert_eq!(summary.bitswap_source_peers.len(), 1);
        assert_eq!(summary.bitswap_source_peers[0].value, "peer1");
        assert_eq!(summary.bitswap_source_peers[0].count, 1);
        assert_eq!(summary.bitswap_source_transports.len(), 1);
        assert_eq!(summary.bitswap_source_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_source_transports[0].count, 1);
        assert_eq!(summary.bitswap_source_candidate_indexes.len(), 1);
        assert_eq!(summary.bitswap_source_candidate_indexes[0].value, "4");
        assert_eq!(summary.bitswap_source_candidate_indexes[0].count, 1);
        assert_eq!(summary.bitswap_source_addr_indexes.len(), 1);
        assert_eq!(summary.bitswap_source_addr_indexes[0].value, "1");
        assert_eq!(summary.bitswap_source_addr_indexes[0].count, 1);
        assert_eq!(summary.bitswap_source_addr_families.len(), 1);
        assert_eq!(summary.bitswap_source_addr_families[0].value, "ip4");
        assert_eq!(summary.bitswap_source_addr_families[0].count, 1);
        assert_eq!(summary.bitswap_source_addr_match_statuses.len(), 1);
        assert_eq!(
            summary.bitswap_source_addr_match_statuses[0].value,
            "matched"
        );
        assert_eq!(summary.bitswap_source_addr_match_statuses[0].count, 1);
        assert_eq!(summary.bitswap_deliveries.len(), 2);
        assert_eq!(summary.bitswap_deliveries[0].value, "incoming");
        assert_eq!(summary.bitswap_deliveries[0].count, 1);
        assert_eq!(summary.bitswap_deliveries[1].value, "outgoing");
        assert_eq!(summary.bitswap_deliveries[1].count, 1);
        assert_eq!(summary.bitswap_extra_blocks.events, 2);
        assert_eq!(summary.bitswap_extra_blocks.total, 3);
        assert_eq!(summary.bitswap_extra_blocks.max, 2);
        assert_eq!(summary.bitswap_extra_blocks.incoming, 2);
        assert_eq!(summary.bitswap_extra_blocks.outgoing, 1);
        assert_eq!(summary.bitswap_extra_blocks.unknown, 0);
        assert_eq!(summary.bitswap_peer_fetches.len(), 1);
        assert_eq!(summary.bitswap_peer_fetches[0].peer, "peer1");
        assert_eq!(summary.bitswap_peer_fetches[0].count, 1);
        assert_eq!(summary.bitswap_peer_fetches[0].total_ms, 25);
        assert_eq!(summary.bitswap_peer_fetches[0].max_ms, 25);
        assert_eq!(summary.bitswap_peer_fetches[0].bytes, 100);
        assert_eq!(summary.bitswap_peer_fetches[0].transports[0].value, "tcp");
        assert_eq!(summary.bitswap_peer_fetches[0].transports[0].count, 1);
        assert_eq!(summary.bitswap_session.fetches, 2);
        assert_eq!(summary.bitswap_session.with_trusted_peers, 2);
        assert_eq!(summary.bitswap_session.trusted_successes, 1);
        assert_eq!(summary.bitswap_session.untrusted_successes, 0);
        assert_eq!(summary.bitswap_session.trusted_failures, 1);
        assert_eq!(summary.bitswap_session.request_timeouts_with_trusted, 1);
        assert_eq!(summary.bitswap_session.session_shortcut_starts, 1);
        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_waits,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeouts,
            1
        );
        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_errors,
            0
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_budgets
                .get("100"),
            Some(&1)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeout_budgets
                .get("100"),
            Some(&1)
        );
        assert_eq!(summary.bitswap_session.session_shortcut_attempts, 2);
        assert_eq!(summary.bitswap_session.session_shortcut_hits, 1);
        assert_eq!(summary.bitswap_session.session_shortcut_misses, 1);
        let trace_errors = summary
            .trace_errors
            .iter()
            .map(|error| error.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(trace_errors.len(), 10);
        assert!(trace_errors.iter().any(|error| {
            error.starts_with("bitswap_connection_error: Failed to negotiate transport protocol")
        }));
        assert!(trace_errors.contains(&"bitswap_dial_rejected: Dial error"));
        assert!(trace_errors.contains(&"bitswap_dnsaddr_expand: ok=false"));
        assert!(trace_errors.contains(&"bitswap_fetch: ok=false"));
        assert!(trace_errors.contains(&"bitswap_session_shortcut: ok=false"));
        assert!(trace_errors.contains(&"provider_lookup: dht: timeout"));
        assert!(trace_errors.contains(&"provider_diversity_low: ok=false"));
        assert!(trace_errors.contains(&"dht_provider_lookup: dht: timed out"));
        assert!(trace_errors.contains(&"dht_provider_lookup: low diversity DHT fallback timed out"));
        assert_eq!(summary.bitswap_addr_mix[0].value, "tcp");
        assert_eq!(summary.bitswap_addr_mix[0].count, 4);
        assert_eq!(summary.bitswap_addr_mix[1].value, "ip4");
        assert_eq!(summary.bitswap_addr_mix[1].count, 3);
        assert_eq!(summary.bitswap_addr_mix[2].value, "quic");
        assert_eq!(summary.bitswap_addr_mix[2].count, 2);
        assert_eq!(summary.bitswap_provider_quality.events, 1);
        assert_eq!(summary.bitswap_provider_quality.provider_addr_count, 10);
        assert_eq!(
            summary
                .bitswap_provider_quality
                .expanded_provider_addr_count,
            12
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .supported_provider_addr_count,
            4
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .rejected_provider_addr_count,
            8
        );
        assert_eq!(summary.bitswap_provider_quality.id_only_provider_count, 1);
        assert_eq!(
            summary.bitswap_provider_quality.invalid_provider_id_count,
            2
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .provider_without_supported_bitswap_addr_count,
            3
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .unsupported_relay_addr_count,
            4
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .unsupported_webtransport_addr_count,
            1
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .unsupported_webrtc_addr_count,
            1
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .unsupported_certhash_addr_count,
            1
        );
        assert_eq!(
            summary
                .bitswap_provider_quality
                .unsupported_transport_addr_count,
            1
        );
        assert_eq!(summary.bitswap_provider_quality.missing_peer_addr_count, 1);
        assert_eq!(summary.bitswap_provider_quality.unparsable_addr_count, 1);
        assert_eq!(summary.bitswap_provider_quality.addr_with_relay_count, 6);
        assert_eq!(
            summary
                .bitswap_provider_quality
                .addr_with_webtransport_count,
            2
        );
        assert_eq!(summary.bitswap_provider_quality.addr_with_webrtc_count, 3);
        assert_eq!(summary.bitswap_provider_quality.addr_with_certhash_count, 4);
        assert_eq!(summary.bitswap_connection_transports.len(), 1);
        assert_eq!(summary.bitswap_connection_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_connection_transports[0].count, 1);
        assert_eq!(summary.bitswap_connection_established.events, 1);
        assert_eq!(
            summary.bitswap_connection_established.established_ms.p50_ms,
            Some(44)
        );
        assert_eq!(
            summary
                .bitswap_connection_established
                .wait_elapsed_ms
                .p50_ms,
            Some(45)
        );
        assert_eq!(summary.bitswap_connection_established.failed_dial_count, 2);
        assert_eq!(summary.bitswap_connection_errors.events, 2);
        assert_eq!(summary.bitswap_connection_errors.with_peer, 1);
        assert_eq!(summary.bitswap_connection_errors.without_peer, 1);
        assert_eq!(
            summary.bitswap_connection_errors.classes[0].value,
            "connection_refused"
        );
        assert_eq!(
            summary.bitswap_connection_errors.classes[1].value,
            "protocol_negotiation_failed"
        );
        assert_eq!(summary.bitswap_connection_errors.peers[0].value, "peer3");
        assert_eq!(summary.bitswap_connection_errors.peers[0].count, 1);
        assert_eq!(
            summary.bitswap_connection_error_addr_families[0].value,
            "ip4"
        );
        assert_eq!(
            summary.bitswap_connection_error_addr_families[1].value,
            "ip6"
        );
        assert_eq!(summary.bitswap_connection_backoff.backoffs, 1);
        assert_eq!(summary.bitswap_connection_backoff.skipped, 1);
        assert_eq!(
            summary.bitswap_connection_backoff.classes[0].value,
            "protocol_negotiation_failed"
        );
        assert_eq!(summary.bitswap_connection_backoff.peers[0].value, "peer3");
        assert_eq!(
            summary.bitswap_connection_backoff.skipped_peers[0].value,
            "peer3"
        );
        assert_eq!(summary.bitswap_dial_rejected_transports.len(), 1);
        assert_eq!(summary.bitswap_dial_rejected_transports[0].value, "quic");
        assert_eq!(summary.bitswap_dial_rejected_transports[0].count, 1);
        assert_eq!(summary.bitswap_dns_expansion.events, 5);
        assert_eq!(summary.bitswap_dns_expansion.cached, 2);
        assert_eq!(summary.bitswap_dns_expansion.uncached, 3);
        assert_eq!(summary.bitswap_dns_expansion.failed, 1);
        assert_eq!(summary.bitswap_dns_expansion.records, 4);
        assert_eq!(summary.bitswap_dns_expansion.ips, 4);
        let cid4 = summary
            .slow_cids
            .iter()
            .find(|cid| cid.cid == "cid4")
            .unwrap();
        assert_eq!(cid4.total_ms, 60);
        assert_eq!(cid4.max_ms, 60);
        assert_eq!(cid4.phases[0].value, "bitswap_request_timeout_detail");
        let cid3 = summary
            .slow_cids
            .iter()
            .find(|cid| cid.cid == "cid3")
            .unwrap();
        assert_eq!(cid3.paths[0].value, "/ipfs/root/index.html");
        let cid1 = summary
            .slow_cids
            .iter()
            .find(|cid| cid.cid == "cid1")
            .unwrap();
        assert_eq!(cid1.total_ms, 30);
        assert_eq!(cid1.count, 2);
        assert_eq!(
            trace_value_count(&cid1.bitswap_source_candidate_indexes, "4"),
            1
        );
        assert_eq!(trace_value_count(&cid1.bitswap_source_peers, "peer1"), 1);
        let cid8 = summary
            .slow_cids
            .iter()
            .find(|cid| cid.cid == "cid8")
            .unwrap();
        assert_eq!(trace_value_count(&cid8.bitswap_source_peers, "peer1"), 1);
        assert_eq!(summary.slow_requests.len(), 1);
        assert_eq!(summary.slow_requests[0].path, "/ipns/site/asset.js");
        assert_eq!(summary.slow_requests[0].request_id, "9");
        assert_eq!(
            summary.slow_requests[0].progress_request_id.as_deref(),
            Some("77")
        );
        assert_eq!(
            summary.slow_requests[0]
                .parent_progress_request_id
                .as_deref(),
            Some("1")
        );
        assert_eq!(
            summary.slow_requests[0].top_level_path.as_deref(),
            Some("/ipns/site/")
        );
        assert_eq!(summary.slow_requests[0].status.as_deref(), Some("200"));
        assert_eq!(summary.slow_requests[0].elapsed_ms, 1);
        assert_eq!(summary.slow_requests[0].max_event_ms, 25);
        assert_eq!(summary.slow_requests[0].event_count, 3);
        assert_eq!(summary.slow_requests[0].cids[0].value, "cid1");
        assert_eq!(summary.slow_requests[0].cids[0].count, 1);
        assert_eq!(summary.slow_requests[0].phases.len(), 3);
        let bitswap_event = summary
            .slow_events
            .iter()
            .find(|event| event.phase == "bitswap_fetch" && event.elapsed_ms == 25)
            .expect("bitswap_fetch should remain in slow events");
        assert_eq!(
            bitswap_event
                .details
                .get("progress_request_id")
                .map(String::as_str),
            Some("77")
        );
        assert_eq!(
            bitswap_event
                .details
                .get("parent_request_id")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(
            bitswap_event
                .details
                .get("top_level_path")
                .map(String::as_str),
            Some("/ipns/site/")
        );
    }

    #[test]
    fn trace_summary_groups_progress_correlated_requests() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-progress-groups-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"elapsed_ms\":20,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"gateway_stream_done\",\"elapsed_ms\":21,\"body_len\":1234,\"chunks\":1,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_start\",\"request_id\":2,\"path\":\"/ipns/site/app.js\",\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":2,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"provider_lookup\",\"elapsed_ms\":60,\"cid\":\"cid-js\",\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":2,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":2,\"path\":\"/ipns/site/app.js\",\"status\":504,\"elapsed_ms\":70,\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":2,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_start\",\"request_id\":3,\"path\":\"/ipns/site/app.css\",\"span\":{\"path\":\"/ipns/site/app.css\",\"request_id\":3,\"progress_request_id\":102,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":3,\"path\":\"/ipns/site/app.css\",\"status\":200,\"elapsed_ms\":5,\"span\":{\"path\":\"/ipns/site/app.css\",\"request_id\":3,\"progress_request_id\":102,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_start\",\"request_id\":4,\"path\":\"/ipns/other/\",\"span\":{\"path\":\"/ipns/other/\",\"request_id\":4,\"progress_request_id\":200,\"top_level_path\":\"/ipns/other/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":4,\"path\":\"/ipns/other/\",\"status\":200,\"elapsed_ms\":10,\"span\":{\"path\":\"/ipns/other/\",\"request_id\":4,\"progress_request_id\":200,\"top_level_path\":\"/ipns/other/\"}}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(summary.progress_request_groups.len(), 2);
        let group = &summary.progress_request_groups[0];
        assert_eq!(group.top_level_path, "/ipns/site/");
        assert_eq!(group.root_progress_request_id.as_deref(), Some("100"));
        assert_eq!(group.request_count, 3);
        assert_eq!(group.child_request_count, 2);
        assert_eq!(group.completed_request_count, 3);
        assert_eq!(group.failed_request_count, 1);
        assert_eq!(group.request_elapsed_ms.count, 3);
        assert_eq!(group.request_elapsed_ms.max_ms, Some(70));
        assert_eq!(group.max_event_ms, 70);
        assert_eq!(group.statuses[0].value, "200");
        assert_eq!(group.statuses[0].count, 2);
        assert_eq!(group.statuses[1].value, "504");
        assert_eq!(group.statuses[1].count, 1);
        assert_eq!(group.slow_requests[0].path, "/ipns/site/app.js");
        assert_eq!(
            group.slow_requests[0].progress_request_id.as_deref(),
            Some("101")
        );
        assert_eq!(
            group.slow_requests[0].parent_progress_request_id.as_deref(),
            Some("100")
        );
        assert!(group
            .phases
            .iter()
            .any(|phase| phase.value == "provider_lookup" && phase.count == 1));
        assert!(group
            .phases
            .iter()
            .any(|phase| phase.value == "gateway_stream_done" && phase.count == 1));

        let other = &summary.progress_request_groups[1];
        assert_eq!(other.top_level_path, "/ipns/other/");
        assert_eq!(other.root_progress_request_id.as_deref(), Some("200"));
        assert_eq!(other.request_count, 1);
        assert_eq!(other.failed_request_count, 0);
    }

    #[test]
    fn trace_summary_attaches_sources_to_slow_requests() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-request-sources-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/app.js\",\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":42,\"cid\":\"cid-a\",\"source\":\"http_provider\",\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"http_provider_fetch\",\"elapsed_ms\":40,\"cid\":\"cid-a\",\"provider\":\"https://provider-a.example\",\"ok\":true,\"response_bytes\":1024,\"response_first_chunk_seen\":true,\"response_headers_elapsed_ms\":10,\"response_first_chunk_elapsed_ms\":12,\"response_body_elapsed_ms\":38,\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":1,\"cid\":\"cid-b\",\"source\":\"cache\",\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"http_provider_fetch\",\"elapsed_ms\":120,\"cid\":\"cid-c\",\"provider\":\"https://provider-b.example\",\"ok\":false,\"error\":\"request timed out\",\"response_headers_elapsed_ms\":90,\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"elapsed_ms\":0,\"hits\":1,\"misses\":2,\"inserts\":2,\"path_hits\":3,\"path_misses\":4,\"path_inserts\":4,\"file_size_hits\":5,\"file_size_misses\":6,\"file_size_inserts\":6,\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/app.js\",\"status\":200,\"elapsed_ms\":150,\"span\":{\"path\":\"/ipns/site/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipns/site/\"}}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(summary.slow_requests.len(), 1);
        let request = &summary.slow_requests[0];
        assert_eq!(request.path, "/ipns/site/app.js");
        assert_eq!(
            trace_value_count(&request.block_sources, "http_provider"),
            1
        );
        assert_eq!(trace_value_count(&request.block_sources, "cache"), 1);
        assert_eq!(request.http_provider_fetches, 2);
        assert_eq!(request.http_provider_fetch_successes, 1);
        assert_eq!(request.http_provider_fetch_failures, 1);
        assert_eq!(request.http_provider_fetch_elapsed_ms.count, 2);
        assert_eq!(request.http_provider_fetch_elapsed_ms.max_ms, Some(120));
        assert_eq!(request.http_provider_fetch_response_bytes, 1024);
        assert_eq!(request.http_provider_fetch_first_chunk_events, 1);
        assert_eq!(request.http_provider_fetch_headers_elapsed_ms.count, 2);
        assert_eq!(
            request.http_provider_fetch_headers_elapsed_ms.max_ms,
            Some(90)
        );
        assert_eq!(
            request.http_provider_fetch_first_chunk_elapsed_ms.max_ms,
            Some(12)
        );
        assert_eq!(request.http_provider_fetch_body_elapsed_ms.max_ms, Some(38));
        let http_phase = request
            .phase_latencies
            .iter()
            .find(|phase| phase.phase == "http_provider_fetch")
            .unwrap();
        assert_eq!(http_phase.count, 2);
        assert_eq!(http_phase.total_ms, 160);
        assert_eq!(http_phase.elapsed_ms.max_ms, Some(120));
        let request_done_phase = request
            .phase_latencies
            .iter()
            .find(|phase| phase.phase == "request_done")
            .unwrap();
        assert_eq!(request_done_phase.count, 1);
        assert_eq!(request_done_phase.total_ms, 150);
        assert_eq!(
            trace_value_count(
                &request.http_provider_fetch_providers,
                "https://provider-a.example"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &request.http_provider_fetch_providers,
                "https://provider-b.example"
            ),
            1
        );
        assert_eq!(
            trace_value_count(&request.http_provider_fetch_error_classes, "timeout"),
            1
        );
        assert_eq!(request.unixfs_metadata_cache.events, 1);
        assert_eq!(request.unixfs_metadata_cache.misses, 2);
        assert_eq!(request.unixfs_metadata_cache.path_inserts, 4);
        assert_eq!(request.unixfs_metadata_cache.file_size_misses, 6);

        assert_eq!(summary.request_paths.len(), 1);
        let request_path = &summary.request_paths[0];
        assert_eq!(request_path.path, "/ipns/site/app.js");
        assert_eq!(request_path.request_count, 1);
        assert_eq!(request_path.request_elapsed_ms.p50_ms, Some(150));
        assert_eq!(
            trace_value_count(&request_path.block_sources, "http_provider"),
            1
        );
        assert_eq!(trace_value_count(&request_path.block_sources, "cache"), 1);
        assert_eq!(request_path.http_provider_fetches, 2);
        assert_eq!(request_path.http_provider_fetch_max_ms, 120);
        assert_eq!(request_path.http_provider_fetch_response_bytes, 1024);
        assert_eq!(request_path.http_provider_fetch_first_chunk_events, 1);
        assert_eq!(request_path.http_provider_fetch_headers_max_ms, 90);
        assert_eq!(request_path.http_provider_fetch_first_chunk_max_ms, 12);
        assert_eq!(request_path.http_provider_fetch_body_max_ms, 38);
        assert_eq!(request_path.unixfs_metadata_cache.events, 1);
        assert_eq!(request_path.unixfs_metadata_cache.misses, 2);
        assert_eq!(request_path.unixfs_metadata_cache.path_inserts, 4);
        assert_eq!(request_path.unixfs_metadata_cache.file_size_misses, 6);
        assert_eq!(
            trace_value_count(
                &request_path.http_provider_fetch_providers,
                "https://provider-a.example"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &request_path.http_provider_fetch_providers,
                "https://provider-b.example"
            ),
            1
        );
        let path_http_phase = request_path
            .phase_latencies
            .iter()
            .find(|phase| phase.phase == "http_provider_fetch")
            .unwrap();
        assert_eq!(path_http_phase.count, 2);
        assert_eq!(path_http_phase.total_ms, 160);
        assert_eq!(path_http_phase.max_ms, 120);

        let group = &summary.progress_request_groups[0];
        assert_eq!(group.top_level_path, "/ipns/site/");
        assert_eq!(group.slow_requests[0].path, "/ipns/site/app.js");
        assert_eq!(
            trace_value_count(&group.slow_requests[0].block_sources, "http_provider"),
            1
        );
        assert_eq!(
            trace_value_count(
                &group.slow_requests[0].http_provider_fetch_providers,
                "https://provider-b.example"
            ),
            1
        );
    }

    #[test]
    fn trace_summary_classifies_zero_http_cold_bitswap_requests() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-request-classification-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"elapsed_ms\":5,\"cid\":\"cid-root\",\"provider_count\":19,\"http_provider_count\":0,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"elapsed_ms\":6,\"cid\":\"cid-root\",\"peer_count\":5,\"session_peer_count\":0,\"supported_provider_addr_count\":41,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":1494,\"cid\":\"cid-root\",\"ok\":true,\"bytes\":1362,\"source_peer\":\"peer-root\",\"source_transport\":\"tcp\",\"source_peer_trusted\":false,\"source_peer_candidate_index\":4,\"source_peer_request_mode\":\"want_have\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":1618,\"cid\":\"cid-root\",\"source\":\"bitswap\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"elapsed_ms\":1888,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "zero_http_provider_bitswap"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "zero_http_provider_cold_bitswap"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "top_level_zero_http_provider_cold_bitswap"
            ),
            1
        );
        let classified_latency = summary
            .request_classification_latencies
            .iter()
            .find(|entry| entry.classification == "top_level_zero_http_provider_cold_bitswap")
            .unwrap();
        assert_eq!(classified_latency.request_count, 1);
        assert_eq!(classified_latency.request_elapsed_ms.p50_ms, Some(1888));
        assert_eq!(classified_latency.max_event_ms.p50_ms, Some(1888));
        assert_eq!(trace_value_count(&classified_latency.statuses, "200"), 1);
        assert_eq!(
            trace_value_count(&classified_latency.top_level_paths, "/ipns/site/"),
            1
        );
        assert_eq!(
            trace_value_count(&classified_latency.bitswap_source_candidate_indexes, "4"),
            1
        );
        assert_eq!(
            trace_value_count(
                &classified_latency.bitswap_source_request_modes,
                "want_have"
            ),
            1
        );
        assert_eq!(
            trace_value_count(&classified_latency.bitswap_source_peers, "peer-root"),
            1
        );
        assert_eq!(
            trace_value_count(&classified_latency.bitswap_source_transports, "tcp"),
            1
        );
        let request = &summary.slow_requests[0];
        assert_eq!(request.path, "/ipns/site/");
        assert_eq!(request.delegated_zero_http_provider_lookups, 1);
        assert_eq!(request.bitswap_block_fetches, 1);
        assert_eq!(request.bitswap_fetches, 1);
        assert_eq!(request.bitswap_fetch_elapsed_ms.max_ms, Some(1494));
        assert_eq!(request.bitswap_fetch_bytes, 1362);
        assert_eq!(request.cold_bitswap_peer_expands, 1);
        assert_eq!(request.max_bitswap_peer_count, 5);
        assert_eq!(request.max_bitswap_session_peer_count, 0);
        assert_eq!(
            trace_value_count(&request.classifications, "cold_bitswap_peer_expand"),
            1
        );
        assert_eq!(
            trace_value_count(&request.bitswap_source_candidate_indexes, "4"),
            1
        );
        assert_eq!(
            trace_value_count(&request.bitswap_source_request_modes, "want_have"),
            1
        );
        assert_eq!(
            trace_value_count(&request.bitswap_source_peers, "peer-root"),
            1
        );
        assert_eq!(
            trace_value_count(&request.bitswap_source_transports, "tcp"),
            1
        );
        let requirements = vec!["top_level_zero_http_provider_cold_bitswap=1".to_string()];
        let requirement_results =
            request_classification_requirement_results(Some(&summary), &requirements).unwrap();
        assert_eq!(
            requirement_results[0].value,
            "top_level_zero_http_provider_cold_bitswap"
        );
        assert_eq!(requirement_results[0].min_count, 1);
        assert_eq!(requirement_results[0].actual_count, 1);
        assert!(requirement_results[0].passed);
        assert!(trace_requirement_failure_messages(&requirement_results).is_empty());
        let request_path = &summary.request_paths[0];
        assert_eq!(request_path.bitswap_fetches, 1);
        assert_eq!(request_path.bitswap_fetch_max_ms, 1494);
        assert_eq!(request_path.bitswap_fetch_bytes, 1362);
        let requirements = vec!["top_level_zero_http_provider_cold_bitswap=2".to_string()];
        let requirement_results =
            request_classification_requirement_results(Some(&summary), &requirements).unwrap();
        assert_eq!(
            trace_requirement_failure_messages(&requirement_results),
            vec!["top_level_zero_http_provider_cold_bitswap expected>=2 actual=1"]
        );
        let requirement_results =
            request_classification_requirement_results(None, &requirements).unwrap();
        assert_eq!(
            trace_requirement_failure_messages(&requirement_results),
            vec!["top_level_zero_http_provider_cold_bitswap expected>=2 actual=0"]
        );
    }

    #[test]
    fn trace_summary_classifies_sparse_wss_dht_empty_requests() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-sparse-wss-dht-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"elapsed_ms\":20,\"cid\":\"cid-root\",\"provider_count\":1,\"http_provider_count\":0,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid-root\",\"provider_count\":1,\"bitswap_provider_count\":1,\"min_bitswap_provider_count\":2,\"fallback\":\"light_dht\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"dht_provider_lookup\",\"elapsed_ms\":251,\"cid\":\"cid-root\",\"ok\":false,\"error\":\"low diversity DHT fallback timed out\",\"fallback\":\"light_dht\",\"provider_count\":0,\"max_providers\":4,\"timeout_ms\":250,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"elapsed_ms\":4,\"cid\":\"cid-root\",\"peer_count\":1,\"session_peer_count\":0,\"tcp_addr_count\":1,\"wss_addr_count\":1,\"supported_provider_addr_count\":1,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":382,\"cid\":\"cid-root\",\"ok\":true,\"bytes\":1362,\"source_peer\":\"peer-wss\",\"source_transport\":\"wss\",\"source_peer_trusted\":false,\"source_peer_candidate_index\":0,\"source_peer_request_mode\":\"want_block\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":722,\"cid\":\"cid-root\",\"source\":\"bitswap\",\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"elapsed_ms\":900,\"span\":{\"path\":\"/ipns/site/\",\"request_id\":1,\"progress_request_id\":100,\"parent_request_id\":0,\"top_level_path\":\"/ipns/site/\"}}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "zero_http_single_bitswap_provider"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "zero_http_single_wss_bitswap"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "low_diversity_dht_fallback_empty"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "zero_http_single_wss_bitswap_dht_empty"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.request_classifications,
                "top_level_zero_http_single_wss_bitswap_dht_empty"
            ),
            1
        );

        let request = &summary.slow_requests[0];
        assert_eq!(request.provider_diversity_low_events, 1);
        assert_eq!(request.provider_diversity_low_failures, 0);
        assert_eq!(request.provider_diversity_low_max_provider_count, 1);
        assert_eq!(request.provider_diversity_low_max_bitswap_provider_count, 1);
        assert_eq!(request.dht_provider_lookup_events, 1);
        assert_eq!(request.dht_provider_lookup_failures, 1);
        assert_eq!(request.dht_provider_lookup_providers, 0);
        assert_eq!(request.dht_provider_lookup_max_elapsed_ms, 251);
        assert_eq!(request.dht_provider_lookup_max_timeout_ms, 250);
        assert_eq!(
            trace_value_count(&request.bitswap_source_transports, "wss"),
            1
        );
        assert_eq!(request.bitswap_fetches, 1);
        assert_eq!(request.bitswap_fetch_elapsed_ms.max_ms, Some(382));
        assert_eq!(request.bitswap_fetch_bytes, 1362);

        let request_path = &summary.request_paths[0];
        assert_eq!(request_path.provider_diversity_low_events, 1);
        assert_eq!(request_path.dht_provider_lookup_events, 1);
        assert_eq!(request_path.dht_provider_lookup_failures, 1);
        assert_eq!(request_path.dht_provider_lookup_max_elapsed_ms, 251);
        assert_eq!(
            trace_value_count(&request_path.bitswap_source_transports, "wss"),
            1
        );
        assert_eq!(request_path.bitswap_fetches, 1);
        assert_eq!(request_path.bitswap_fetch_max_ms, 382);
        assert_eq!(request_path.bitswap_fetch_bytes, 1362);

        let classified_latency = summary
            .request_classification_latencies
            .iter()
            .find(|entry| entry.classification == "zero_http_single_wss_bitswap_dht_empty")
            .unwrap();
        assert_eq!(classified_latency.request_count, 1);
        assert_eq!(
            trace_value_count(&classified_latency.bitswap_source_transports, "wss"),
            1
        );
    }

    #[test]
    fn trace_summary_derives_mobile_progress_phases() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-progress-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\"}\n",
                "{\"phase\":\"name_cache\",\"name\":\"site.test\",\"cache_hit\":false}\n",
                "{\"phase\":\"name_persistent_cache\",\"name\":\"site.test\",\"cache_hit\":true,\"resolved_target\":\"/ipfs/root\"}\n",
                "{\"phase\":\"name_resolve\",\"name\":\"site.test\",\"ok\":true,\"resolved_target\":\"/ipfs/root\"}\n",
                "{\"phase\":\"provider_cache\",\"cid\":\"cid-a\",\"cache_hit\":false}\n",
                "{\"phase\":\"provider_cache\",\"cid\":\"cid-z\",\"cache_hit\":true,\"provider_count\":0}\n",
                "{\"phase\":\"provider_lookup\",\"cid\":\"cid-a\",\"provider_count\":3}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"cid\":\"cid-a\",\"endpoint\":\"https://delegated-ipfs.dev/routing/v1\",\"provider_count\":3,\"http_provider_count\":2,\"response_bytes\":512,\"response_lines\":4,\"response_headers_elapsed_ms\":5,\"response_first_chunk_seen\":true,\"response_first_chunk_elapsed_ms\":6,\"response_first_http_provider_seen\":true,\"response_first_http_provider_elapsed_ms\":7,\"response_target_met\":true,\"response_target_met_elapsed_ms\":8,\"elapsed_ms\":9}\n",
                "{\"phase\":\"delegated_provider_self_hedge\",\"cid\":\"cid-a\",\"endpoint\":\"https://delegated-ipfs.dev/routing/v1\",\"timeout_ms\":750,\"reason\":\"slow_single_endpoint\"}\n",
                "{\"phase\":\"delegated_provider_empty_retry\",\"cid\":\"cid-a\",\"provider_count\":1,\"delay_ms\":100,\"elapsed_ms\":116}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid-a\",\"provider_count\":1,\"fallback\":\"light_dht\"}\n",
                "{\"phase\":\"bitswap_dns_prefetch\",\"dnsaddr_host_count\":1,\"dns_ip_host_count\":2,\"elapsed_ms\":5}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"peer.test\",\"record_count\":2}\n",
                "{\"phase\":\"block_store_get\",\"cid\":\"cid-a\",\"cache_hit\":false}\n",
                "{\"phase\":\"block_store_get\",\"cid\":\"cid-b\",\"cache_hit\":true}\n",
                "{\"phase\":\"http_provider_fetch\",\"cid\":\"cid-a\",\"ok\":true}\n",
                "{\"phase\":\"http_provider_candidate_cancelled\",\"cid\":\"cid-a\",\"provider\":\"https://provider.example\",\"stage\":\"reading_body\"}\n",
                "{\"phase\":\"http_provider_hedge\",\"cid\":\"cid-a\",\"provider\":\"https://provider.example\",\"timeout_ms\":250,\"pending_count\":2}\n",
                "{\"phase\":\"http_provider_bitswap_hedge\",\"cid\":\"cid-a\",\"provider_count\":4,\"timeout_ms\":150,\"reason\":\"slow_single_http_provider\"}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_skip\",\"cid\":\"cid-c\",\"provider\":\"https://provider.example\",\"reason\":\"provider_score_below_threshold\",\"provider_scored\":true,\"provider_score_ms\":120,\"min_score_ms\":250}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_result\",\"cid\":\"cid-a\",\"source\":\"bitswap\",\"provider_count\":4,\"elapsed_ms\":200}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_result\",\"cid\":\"cid-b\",\"source\":\"http_provider\",\"provider_count\":2,\"elapsed_ms\":75}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"cid\":\"cid-a\",\"peer_count\":2}\n",
                "{\"phase\":\"bitswap_incoming_batch\",\"cid\":\"cid-a\",\"cid_count\":2,\"requested_blocks\":2}\n",
                "{\"phase\":\"bitswap_connection_established\",\"peer\":\"peer-a\",\"transport\":\"tcp\"}\n",
                "{\"phase\":\"unixfs_resource\",\"path\":\"/ipns/site/\",\"ok\":true}\n",
                "{\"phase\":\"gateway_direct_body\",\"path\":\"/ipns/site/asset.css\",\"body_len\":4096}\n",
                "{\"phase\":\"gateway_stream_done\",\"path\":\"/ipns/site/\",\"body_len\":600000,\"chunks\":3}\n",
                "{\"phase\":\"gateway_stream_failed\",\"path\":\"/ipns/site/video.mp4\",\"error\":\"missing block\"}\n",
                "{\"phase\":\"gateway_conditional\",\"path\":\"/ipns/site/\",\"outcome\":\"not_modified\"}\n",
                "{\"phase\":\"bitswap_request_timeout\",\"cid\":\"cid-a\",\"peer_count\":2}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"peer-b\",\"error\":\"timeout\"}\n",
                "{\"phase\":\"bitswap_dial_waiters_dropped\",\"cid\":\"cid-a\",\"waiter_count\":2}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-c\",\"ok\":false,\"timed_out\":true}\n",
                "{\"phase\":\"provider_refresh_skipped_empty_provider_set\",\"cid\":\"cid-a\",\"error\":\"No Bitswap providers\",\"initial_error\":\"No Bitswap providers\"}\n",
                "{\"phase\":\"gateway_limiter\",\"acquired\":false}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"body_mode\":\"stream\"}\n",
                "{\"phase\":\"request_done\",\"request_id\":2,\"path\":\"/ipns/missing/\",\"status\":503}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(trace_value_count(&summary.progress_phases, "started"), 1);
        assert_eq!(
            trace_value_count(&summary.progress_phases, "resolving_name"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "name_resolved"),
            2
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "provider_lookup"),
            6
        );
        assert_eq!(summary.delegated_provider_lookup.events, 1);
        assert_eq!(summary.delegated_provider_lookup.successes, 1);
        assert_eq!(summary.delegated_provider_lookup.providers, 3);
        assert_eq!(summary.delegated_provider_lookup.max_elapsed_ms, 9);
        assert_eq!(summary.delegated_provider_lookup.elapsed_ms.p50_ms, Some(9));
        assert_eq!(summary.delegated_provider_lookup.http_providers, 2);
        assert_eq!(summary.delegated_provider_lookup.self_hedges, 1);
        assert_eq!(
            summary.delegated_provider_lookup.max_self_hedge_timeout_ms,
            750
        );
        assert_eq!(summary.delegated_provider_lookup.response_bytes, 512);
        assert_eq!(summary.delegated_provider_lookup.response_lines, 4);
        assert_eq!(
            summary
                .delegated_provider_lookup
                .response_headers_elapsed_ms
                .p50_ms,
            Some(5)
        );
        assert_eq!(
            summary
                .delegated_provider_lookup
                .max_response_headers_elapsed_ms,
            5
        );
        assert_eq!(summary.delegated_provider_lookup.first_chunk_events, 1);
        assert_eq!(
            summary
                .delegated_provider_lookup
                .max_response_first_chunk_elapsed_ms,
            6
        );
        assert_eq!(
            summary
                .delegated_provider_lookup
                .response_first_chunk_elapsed_ms
                .p50_ms,
            Some(6)
        );
        assert_eq!(
            summary.delegated_provider_lookup.first_http_provider_events,
            1
        );
        assert_eq!(
            summary
                .delegated_provider_lookup
                .response_first_http_provider_elapsed_ms
                .p50_ms,
            Some(7)
        );
        assert_eq!(
            summary
                .delegated_provider_lookup
                .max_response_first_http_provider_elapsed_ms,
            7
        );
        assert_eq!(summary.delegated_provider_lookup.target_met_events, 1);
        assert_eq!(
            summary
                .delegated_provider_lookup
                .max_response_target_met_elapsed_ms,
            8
        );
        assert_eq!(
            summary
                .delegated_provider_lookup
                .response_target_met_elapsed_ms
                .p50_ms,
            Some(8)
        );
        assert_eq!(summary.delegated_provider_lookup_by_endpoint.len(), 1);
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0].endpoint,
            "https://delegated-ipfs.dev/routing/v1"
        );
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0].successes,
            1
        );
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0].http_providers,
            2
        );
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0].self_hedges,
            1
        );
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0]
                .response_first_http_provider_elapsed_ms
                .p50_ms,
            Some(7)
        );
        assert_eq!(
            summary.delegated_provider_lookup_by_endpoint[0]
                .max_response_first_http_provider_elapsed_ms,
            7
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "providers_found"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "provider_diversity_low"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "checking_cache"),
            1
        );
        assert_eq!(trace_value_count(&summary.progress_phases, "cache_hit"), 2);
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_http_provider"),
            5
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            5
        );
        assert_eq!(trace_value_count(&summary.progress_phases, "streaming"), 3);
        assert_eq!(summary.gateway_direct_body.events, 1);
        assert_eq!(summary.gateway_direct_body.bytes, 4096);
        assert_eq!(summary.gateway_direct_body.max_body_len, 4096);
        assert_eq!(summary.gateway_stream_body.events, 1);
        assert_eq!(summary.gateway_stream_body.bytes, 600000);
        assert_eq!(summary.gateway_stream_body.max_body_len, 600000);
        assert_eq!(summary.gateway_stream_body.max_chunks, 3);
        assert_eq!(trace_value_count(&summary.progress_phases, "retrying"), 4);
        assert_eq!(trace_value_count(&summary.progress_phases, "completed"), 1);
        assert_eq!(trace_value_count(&summary.progress_phases, "failed"), 5);
        assert_eq!(
            summary.provider_retries.skipped_empty_provider_set_events,
            1
        );

        let requirements = vec![
            "provider_lookup=6".to_string(),
            "fetching_bitswap=5".to_string(),
        ];
        let requirement_results =
            progress_phase_requirement_results(Some(&summary), &requirements).unwrap();
        assert_eq!(requirement_results[0].value, "provider_lookup");
        assert_eq!(requirement_results[0].min_count, 6);
        assert_eq!(requirement_results[0].actual_count, 6);
        assert!(requirement_results[0].passed);
        assert!(trace_requirement_failure_messages(&requirement_results).is_empty());
        let requirements = vec!["fetching_bitswap=6".to_string()];
        let requirement_results =
            progress_phase_requirement_results(Some(&summary), &requirements).unwrap();
        assert_eq!(requirement_results[0].actual_count, 5);
        assert!(!requirement_results[0].passed);
        assert_eq!(
            trace_requirement_failure_messages(&requirement_results),
            vec!["fetching_bitswap expected>=6 actual=5"]
        );
        let requirement_results = progress_phase_requirement_results(None, &requirements).unwrap();
        assert_eq!(
            trace_requirement_failure_messages(&requirement_results),
            vec!["fetching_bitswap expected>=6 actual=0"]
        );
    }

    #[test]
    fn trace_summary_counts_gateway_small_body_cache() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-small-body-cache-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"gateway_small_body_cache\",\"cache_hit\":true,\"body_len\":512,\"elapsed_ms\":0}\n",
                "{\"phase\":\"gateway_small_body_cache\",\"cache_hit\":false,\"elapsed_ms\":0}\n",
                "{\"phase\":\"gateway_small_body_cache\",\"cache_inserted\":true,\"evicted\":1,\"cache_len\":2,\"cache_bytes\":1536}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.gateway_small_body_cache.events, 3);
        assert_eq!(summary.gateway_small_body_cache.hits, 1);
        assert_eq!(summary.gateway_small_body_cache.misses, 1);
        assert_eq!(summary.gateway_small_body_cache.inserts, 1);
        assert_eq!(summary.gateway_small_body_cache.evictions, 1);
        assert_eq!(summary.gateway_small_body_cache.bytes_served, 512);
        assert_eq!(summary.gateway_small_body_cache.max_body_len, 512);
        assert_eq!(summary.gateway_small_body_cache.max_cache_len, 2);
        assert_eq!(summary.gateway_small_body_cache.max_cache_bytes, 1536);
        assert_eq!(trace_value_count(&summary.progress_phases, "cache_hit"), 1);
        assert_eq!(
            trace_value_count(&summary.progress_phases, "checking_cache"),
            1
        );
        assert_eq!(trace_value_count(&summary.progress_phases, "streaming"), 1);
        assert_eq!(
            trace_value_count(&summary.progress_phases, "gateway_small_body_cache"),
            0
        );
    }

    #[test]
    fn trace_summary_keeps_restarted_gateway_request_ids_separate() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-restart-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\",\"span\":{\"process_id\":101,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"unixfs_file_size\",\"elapsed_ms\":10,\"cid\":\"cid-a\",\"span\":{\"process_id\":101,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"elapsed_ms\":11,\"span\":{\"process_id\":101,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipns/site/\",\"span\":{\"process_id\":202,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"unixfs_file_size\",\"elapsed_ms\":30,\"cid\":\"cid-b\",\"span\":{\"process_id\":202,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200,\"elapsed_ms\":31,\"span\":{\"process_id\":202,\"request_id\":1,\"path\":\"/ipns/site/\"}}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.slow_requests.len(), 2);
        assert_eq!(summary.slow_requests[0].process_id, "202");
        assert_eq!(summary.slow_requests[0].request_id, "1");
        assert_eq!(summary.slow_requests[0].elapsed_ms, 31);
        assert_eq!(summary.slow_requests[0].cids[0].value, "cid-b");
        assert_eq!(summary.slow_requests[1].process_id, "101");
        assert_eq!(summary.slow_requests[1].request_id, "1");
        assert_eq!(summary.slow_requests[1].elapsed_ms, 11);
        assert_eq!(summary.slow_requests[1].cids[0].value, "cid-a");
        assert_eq!(summary.gateway_request_elapsed_ms.count, 2);
        assert_eq!(summary.gateway_request_elapsed_ms.p50_ms, Some(11));
        assert_eq!(summary.gateway_request_elapsed_ms.p95_ms, Some(31));
    }

    #[test]
    fn trace_summary_counts_delegated_http_provider_distribution() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-delegated-http-dist-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"delegated_provider_lookup\",\"endpoint\":\"https://delegated-ipfs.dev/routing/v1\",\"provider_count\":3,\"http_provider_count\":0,\"response_headers_elapsed_ms\":5,\"response_first_http_provider_seen\":false,\"response_target_met\":false,\"elapsed_ms\":5}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"endpoint\":\"https://delegated-ipfs.dev/routing/v1\",\"provider_count\":12,\"http_provider_count\":1,\"response_headers_elapsed_ms\":90,\"response_first_http_provider_seen\":true,\"response_first_http_provider_elapsed_ms\":92,\"response_target_met\":false,\"elapsed_ms\":96}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"endpoint\":\"https://delegated-ipfs.dev/routing/v1\",\"provider_count\":4,\"http_provider_count\":3,\"response_headers_elapsed_ms\":20,\"response_first_http_provider_seen\":true,\"response_first_http_provider_elapsed_ms\":21,\"response_target_met\":true,\"response_target_met_elapsed_ms\":22,\"elapsed_ms\":23}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        let delegated = &summary.delegated_provider_lookup;
        assert_eq!(delegated.events, 3);
        assert_eq!(delegated.zero_http_provider_events, 1);
        assert_eq!(delegated.single_http_provider_events, 1);
        assert_eq!(delegated.multi_http_provider_events, 1);
        assert_eq!(delegated.single_http_provider_target_miss_events, 1);
        assert_eq!(delegated.max_single_http_provider_elapsed_ms, 96);
        assert_eq!(delegated.max_single_http_provider_first_http_elapsed_ms, 92);
        assert_eq!(delegated.elapsed_ms.p50_ms, Some(23));
        assert_eq!(delegated.response_headers_elapsed_ms.p50_ms, Some(20));
        assert_eq!(
            delegated.response_first_http_provider_elapsed_ms.p50_ms,
            Some(21)
        );
        assert_eq!(delegated.response_target_met_elapsed_ms.p50_ms, Some(22));
        assert_eq!(delegated.single_http_provider_elapsed_ms.p50_ms, Some(96));
        assert_eq!(
            delegated.single_http_provider_first_http_elapsed_ms.p50_ms,
            Some(92)
        );

        let endpoint = &summary.delegated_provider_lookup_by_endpoint[0];
        assert_eq!(endpoint.zero_http_provider_events, 1);
        assert_eq!(endpoint.single_http_provider_events, 1);
        assert_eq!(endpoint.multi_http_provider_events, 1);
        assert_eq!(endpoint.single_http_provider_target_miss_events, 1);
        assert_eq!(endpoint.max_single_http_provider_elapsed_ms, 96);
        assert_eq!(endpoint.max_single_http_provider_first_http_elapsed_ms, 92);
        assert_eq!(endpoint.elapsed_ms.p50_ms, Some(23));
        assert_eq!(endpoint.response_headers_elapsed_ms.p50_ms, Some(20));
        assert_eq!(
            endpoint.response_first_http_provider_elapsed_ms.p50_ms,
            Some(21)
        );
        assert_eq!(endpoint.response_target_met_elapsed_ms.p50_ms, Some(22));
        assert_eq!(endpoint.single_http_provider_elapsed_ms.p50_ms, Some(96));
        assert_eq!(
            endpoint.single_http_provider_first_http_elapsed_ms.p50_ms,
            Some(92)
        );
    }

    #[test]
    fn trace_summary_counts_bitswap_peer_attempts() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-peer-attempts-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b,cid-c\",\"cid_count\":3,\"peer\":\"peer-a\",\"prefer_want_have\":false}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":25,\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b,cid-c\",\"cid_count\":3,\"peer\":\"peer-a\",\"ok\":true,\"prefer_want_have\":false,\"bytes\":42,\"requested_blocks\":3}\n",
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"peer\":\"peer-b\",\"prefer_want_have\":true}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":5000,\"cid\":\"cid-a\",\"peer\":\"peer-b\",\"ok\":false,\"prefer_want_have\":true,\"failure_kind\":\"connection_timeout\",\"error\":\"timed out\"}\n",
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"peer\":\"peer-c\",\"prefer_want_have\":true}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":10000,\"cid\":\"cid-a\",\"peer\":\"peer-c\",\"ok\":false,\"prefer_want_have\":true,\"failure_kind\":\"read_timeout\",\"error\":\"read timed out\"}\n",
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-b\",\"peer\":\"peer-d\",\"prefer_want_have\":true}\n",
                "{\"phase\":\"bitswap_peer_attempt_cancelled\",\"elapsed_ms\":12,\"cid\":\"cid-b\",\"peer\":\"peer-d\",\"stage\":\"waiting_connection\",\"prefer_want_have\":true,\"probe_peer_candidate_index\":2,\"probe_peer_request_mode\":\"want_have\",\"probe_peer_first_addr_transport\":\"tcp\",\"probe_peer_first_addr_family\":\"ip6\"}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":40,\"cid\":\"cid-b\",\"peer_count\":2,\"trusted_peer_count\":0,\"ok\":true,\"source_peer\":\"peer-d\",\"source_transport\":\"tcp\",\"bitswap_delivery\":\"incoming\",\"source_peer_trusted\":false,\"extra_blocks\":0,\"bytes\":128}\n",
                "{\"phase\":\"bitswap_fetch_cancelled\",\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b,cid-c\",\"cid_count\":3}\n",
                "{\"phase\":\"bitswap_batch_failed\",\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b,cid-c\",\"cid_count\":3,\"failure_count\":2}\n",
                "{\"phase\":\"provider_refresh_after_timeout\",\"cid\":\"cid-a\",\"request_timeout\":true}\n",
                "{\"phase\":\"retry_provider_count\",\"cid\":\"cid-a\",\"same_provider_set\":true,\"same_bitswap_peer_set\":true,\"request_timeout\":true}\n",
                "{\"phase\":\"provider_retry_after_request_timeout\",\"cid\":\"cid-a\",\"provider_count\":3,\"request_timeout\":true}\n",
                "{\"phase\":\"bitswap_dial_plan\",\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b,cid-c\",\"cid_count\":3,\"peer_count\":4,\"candidate_peer_count\":5,\"new_dial_peer_count\":2,\"new_dial_addr_count\":3,\"suppressed_dial_peer_count\":1,\"suppressed_dial_addr_count\":4,\"pending_dial_peer_count\":2,\"connected_peer_count\":1,\"command_queued_ms\":7}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer-a\",\"transport\":\"tcp\",\"connection_limit\":true}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer-b\",\"transport\":\"ws\",\"connection_limit\":false}\n",
                "{\"phase\":\"bitswap_incoming_block\",\"cid\":\"cid-a\",\"peer\":\"peer-d\",\"source_transport\":\"tcp\",\"block_count\":2,\"bytes\":256,\"pending_waiter_count\":3,\"delivered_waiter_count\":2,\"dropped_waiter_count\":1,\"oldest_pending_ms\":75,\"newest_pending_ms\":25}\n",
                "{\"phase\":\"bitswap_incoming_batch\",\"cid\":\"cid-a\",\"cids\":\"cid-a,cid-b\",\"cid_count\":2,\"requested_blocks\":2,\"extra_blocks\":1,\"elapsed_ms\":70}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-e\",\"ok\":false,\"dropped\":true,\"pending_reads\":32}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-f\",\"ok\":false,\"timed_out\":true,\"timeout_ms\":6000,\"elapsed_ms\":6001}\n",
                "{\"phase\":\"provider_refresh_skipped_empty_provider_set\",\"cid\":\"cid-a\",\"error\":\"No Bitswap providers\",\"initial_error\":\"No Bitswap providers\"}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.bitswap_peer_attempts.starts, 4);
        assert_eq!(summary.bitswap_peer_attempts.outgoing_completed, 3);
        assert_eq!(summary.bitswap_peer_attempts.successes, 1);
        assert_eq!(summary.bitswap_peer_attempts.failures, 2);
        assert_eq!(summary.bitswap_peer_attempts.connection_timeouts, 1);
        assert_eq!(summary.bitswap_peer_attempts.read_timeouts, 1);
        assert_eq!(summary.bitswap_peer_attempts.other_failures, 0);
        assert_eq!(summary.bitswap_peer_attempts.prefer_want_have, 2);
        assert_eq!(summary.bitswap_peer_attempts.cancelled, 1);
        assert_eq!(summary.bitswap_peer_attempts.cancelled_prefer_want_have, 1);
        assert_eq!(
            summary.bitswap_peer_attempts.cancelled_stages[0].value,
            "waiting_connection"
        );
        assert_eq!(
            summary.bitswap_peer_attempts.cancelled_candidate_indexes[0].value,
            "2"
        );
        assert_eq!(
            summary.bitswap_peer_attempts.cancelled_request_modes[0].value,
            "want_have"
        );
        assert_eq!(summary.bitswap_batches.commands, 1);
        assert_eq!(summary.bitswap_batches.multi_cid_commands, 1);
        assert_eq!(summary.bitswap_batches.total_cids, 3);
        assert_eq!(summary.bitswap_batches.max_cids, 3);
        assert_eq!(summary.bitswap_batches.peer_attempt_starts, 4);
        assert_eq!(summary.bitswap_batches.peer_attempt_successes, 1);
        assert_eq!(summary.bitswap_batches.requested_blocks, 3);
        assert_eq!(summary.bitswap_batches.max_requested_blocks, 3);
        assert_eq!(summary.bitswap_batches.cancelled, 1);
        assert_eq!(summary.bitswap_batches.failures, 1);
        assert_eq!(summary.bitswap_source_request_modes[0].value, "want_have");
        assert_eq!(summary.bitswap_source_request_modes[0].count, 1);
        assert_eq!(summary.provider_retries.refresh_after_timeout_events, 1);
        assert_eq!(summary.provider_retries.refresh_after_failure_events, 0);
        assert_eq!(
            summary.provider_retries.skipped_empty_provider_set_events,
            1
        );
        assert_eq!(summary.provider_retries.retry_count_events, 1);
        assert_eq!(summary.provider_retries.same_provider_sets, 1);
        assert_eq!(summary.provider_retries.same_bitswap_peer_sets, 1);
        assert_eq!(summary.provider_retries.request_timeout_retry_counts, 1);
        assert_eq!(
            summary
                .provider_retries
                .same_bitswap_request_timeout_retry_counts,
            1
        );
        assert_eq!(summary.provider_retries.request_timeout_retries, 1);
        assert_eq!(summary.provider_retries.timeout_retries, 0);
        assert_eq!(summary.provider_retries.connection_timeout_retries, 0);
        assert_eq!(summary.bitswap_dial_plans.events, 1);
        assert_eq!(summary.bitswap_dial_plans.peer_targets, 4);
        assert_eq!(summary.bitswap_dial_plans.candidate_peers, 5);
        assert_eq!(summary.bitswap_dial_plans.new_dial_peers, 2);
        assert_eq!(summary.bitswap_dial_plans.new_dial_addrs, 3);
        assert_eq!(summary.bitswap_dial_plans.suppressed_dial_peers, 1);
        assert_eq!(summary.bitswap_dial_plans.suppressed_dial_addrs, 4);
        assert_eq!(summary.bitswap_dial_plans.pending_dial_peers, 2);
        assert_eq!(summary.bitswap_dial_plans.connected_peers, 1);
        assert_eq!(summary.bitswap_dial_plans.max_command_queued_ms, 7);
        assert_eq!(summary.bitswap_dial_rejections.events, 2);
        assert_eq!(summary.bitswap_dial_rejections.connection_limit, 1);
        assert_eq!(summary.bitswap_dial_rejections.other, 1);
        assert_eq!(summary.bitswap_dial_rejected_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_dial_rejected_transports[0].count, 1);
        assert_eq!(summary.bitswap_dial_rejected_transports[1].value, "ws");
        assert_eq!(summary.bitswap_dial_rejected_transports[1].count, 1);
        assert_eq!(summary.bitswap_incoming_blocks.matches, 1);
        assert_eq!(summary.bitswap_incoming_blocks.blocks, 2);
        assert_eq!(summary.bitswap_incoming_blocks.bytes, 256);
        assert_eq!(summary.bitswap_incoming_blocks.delivered_waiters, 2);
        assert_eq!(summary.bitswap_incoming_blocks.dropped_waiters, 1);
        assert_eq!(summary.bitswap_incoming_blocks.max_pending_waiters, 3);
        assert_eq!(summary.bitswap_incoming_blocks.max_oldest_pending_ms, 75);
        assert_eq!(summary.bitswap_incoming_blocks.max_dropped_waiters, 1);
        assert_eq!(summary.bitswap_incoming_batches.events, 1);
        assert_eq!(summary.bitswap_incoming_batches.total_cids, 2);
        assert_eq!(summary.bitswap_incoming_batches.max_cids, 2);
        assert_eq!(summary.bitswap_incoming_batches.requested_blocks, 2);
        assert_eq!(summary.bitswap_incoming_batches.extra_blocks, 1);
        assert_eq!(summary.bitswap_incoming_batches.max_elapsed_ms, 70);
        assert_eq!(summary.bitswap_incoming_reads.events, 2);
        assert_eq!(summary.bitswap_incoming_reads.failures, 2);
        assert_eq!(summary.bitswap_incoming_reads.dropped, 1);
        assert_eq!(summary.bitswap_incoming_reads.timed_out, 1);
        assert_eq!(summary.bitswap_incoming_reads.max_pending_reads, 32);
        assert_eq!(summary.bitswap_incoming_reads.max_elapsed_ms, 6001);
        assert_eq!(
            summary.slow_events[0].details.get("peer"),
            Some(&"peer-c".to_string())
        );
        assert_eq!(
            summary.slow_events[0].details.get("failure_kind"),
            Some(&"read_timeout".to_string())
        );
    }

    #[test]
    fn formats_bitswap_peer_attempt_summary() {
        assert!(
            format_trace_bitswap_peer_attempts(&TraceBitswapPeerAttemptAggregate::default())
                .is_none()
        );

        let line = format_trace_bitswap_peer_attempts(&TraceBitswapPeerAttemptAggregate {
            starts: 3,
            outgoing_completed: 2,
            successes: 1,
            failures: 1,
            connection_timeouts: 1,
            read_timeouts: 0,
            other_failures: 0,
            prefer_want_have: 2,
            cancelled: 2,
            cancelled_prefer_want_have: 1,
            cancelled_stages: vec![TraceValueCount {
                value: "requesting_blocks".to_string(),
                count: 2,
            }],
            cancelled_candidate_indexes: vec![TraceValueCount {
                value: "0".to_string(),
                count: 2,
            }],
            cancelled_request_modes: vec![TraceValueCount {
                value: "want_block".to_string(),
                count: 2,
            }],
            cancelled_first_addr_transports: vec![TraceValueCount {
                value: "tcp".to_string(),
                count: 2,
            }],
            cancelled_first_addr_families: vec![TraceValueCount {
                value: "ip4".to_string(),
                count: 2,
            }],
        })
        .unwrap();

        assert_eq!(
            line,
            "bitswap peer attempts: starts=3 outgoing_completed=2 successes=1 failures=1 connection_timeouts=1 read_timeouts=0 other_failures=0 prefer_want_have=2 cancelled=2 cancelled_prefer_want_have=1 cancelled_stages=requesting_blocks=2 cancelled_candidate_indexes=0=2 cancelled_modes=want_block=2 cancelled_first_addr_transports=tcp=2 cancelled_first_addr_families=ip4=2"
        );
    }

    #[test]
    fn trace_summary_counts_bitswap_want_have_probes() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-want-have-probes-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_want_have_probe\",\"elapsed_ms\":25,\"cid\":\"cid-a\",\"peer\":\"peer-a\",\"ok\":true,\"outcome\":\"have_then_want_block\",\"has_have\":true,\"has_dont_have\":false,\"presence_count\":1,\"probe_peer_candidate_index\":0,\"probe_peer_request_mode\":\"want_have\",\"probe_peer_first_addr_transport\":\"tcp\",\"probe_peer_first_addr_family\":\"ip4\",\"probe_target_peer_count\":4,\"probe_peer_addr_count\":2}\n",
                "{\"phase\":\"bitswap_want_have_probe\",\"elapsed_ms\":750,\"cid\":\"cid-b\",\"peer\":\"peer-b\",\"ok\":false,\"outcome\":\"timeout_fallback_want_block\",\"timeout_ms\":750,\"probe_peer_candidate_index\":3,\"probe_peer_request_mode\":\"want_have\",\"probe_peer_first_addr_transport\":\"quic-v1\",\"probe_peer_first_addr_family\":\"ip6\",\"probe_target_peer_count\":4,\"probe_peer_addr_count\":1}\n",
                "{\"phase\":\"bitswap_want_have_probe\",\"elapsed_ms\":12,\"cid\":\"cid-c\",\"peer\":\"peer-c\",\"ok\":false,\"outcome\":\"dont_have\",\"has_have\":false,\"has_dont_have\":true,\"presence_count\":1}\n",
                "{\"phase\":\"bitswap_want_have_probe\",\"elapsed_ms\":5,\"cid\":\"cid-d\",\"peer\":\"peer-a\",\"ok\":true,\"outcome\":\"block\",\"bytes\":42,\"extra_blocks\":2}\n",
                "{\"phase\":\"bitswap_want_have_probe\",\"elapsed_ms\":18,\"cid\":\"cid-e\",\"peer\":\"peer-d\",\"ok\":false,\"outcome\":\"no_presence_fallback_want_block\",\"presence_count\":0}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let probes = &summary.bitswap_want_have_probes;

        assert_eq!(probes.events, 5);
        assert_eq!(probes.ok, 2);
        assert_eq!(probes.failures, 3);
        assert_eq!(probes.have, 1);
        assert_eq!(probes.dont_have, 1);
        assert_eq!(probes.block, 1);
        assert_eq!(probes.want_block_followups, 3);
        assert_eq!(probes.no_presence, 1);
        assert_eq!(probes.bytes, 42);
        assert_eq!(probes.extra_blocks, 2);
        assert_eq!(probes.max_timeout_ms, 750);
        assert_eq!(probes.elapsed_ms.count, 5);
        assert_eq!(probes.elapsed_ms.p50_ms, Some(18));
        assert_eq!(probes.elapsed_ms.p90_ms, Some(750));
        assert_eq!(probes.outcomes[0].value, "block");
        assert_eq!(probes.outcomes[0].count, 1);
        assert_eq!(probes.outcomes[4].value, "timeout_fallback_want_block");
        assert_eq!(probes.peers[0].value, "peer-a");
        assert_eq!(probes.peers[0].count, 2);
        assert_eq!(trace_value_count(&probes.candidate_indexes, "0"), 1);
        assert_eq!(trace_value_count(&probes.candidate_indexes, "3"), 1);
        assert_eq!(trace_value_count(&probes.request_modes, "want_have"), 2);
        assert_eq!(trace_value_count(&probes.first_addr_transports, "tcp"), 1);
        assert_eq!(
            trace_value_count(&probes.first_addr_transports, "quic-v1"),
            1
        );
        assert_eq!(trace_value_count(&probes.first_addr_families, "ip4"), 1);
        assert_eq!(trace_value_count(&probes.first_addr_families, "ip6"), 1);
        assert_eq!(trace_value_count(&probes.target_peer_counts, "4"), 2);
        assert_eq!(trace_value_count(&probes.peer_addr_counts, "1"), 1);
        assert_eq!(trace_value_count(&probes.peer_addr_counts, "2"), 1);
    }

    #[test]
    fn formats_bitswap_want_have_probe_summary() {
        assert!(format_trace_bitswap_want_have_probes(
            &TraceBitswapWantHaveProbeAggregate::default()
        )
        .is_none());

        let line = format_trace_bitswap_want_have_probes(&TraceBitswapWantHaveProbeAggregate {
            events: 2,
            ok: 1,
            failures: 1,
            have: 1,
            dont_have: 0,
            block: 0,
            want_block_followups: 2,
            no_presence: 1,
            bytes: 0,
            extra_blocks: 0,
            max_timeout_ms: 750,
            elapsed_ms: LatencySummary::from_values(vec![25, 750]),
            outcomes: vec![TraceValueCount {
                value: "timeout_fallback_want_block".to_string(),
                count: 1,
            }],
            peers: vec![TraceValueCount {
                value: "peer-a".to_string(),
                count: 2,
            }],
            candidate_indexes: vec![TraceValueCount {
                value: "3".to_string(),
                count: 1,
            }],
            request_modes: vec![TraceValueCount {
                value: "want_have".to_string(),
                count: 2,
            }],
            first_addr_transports: vec![TraceValueCount {
                value: "tcp".to_string(),
                count: 1,
            }],
            first_addr_families: vec![TraceValueCount {
                value: "ip4".to_string(),
                count: 1,
            }],
            target_peer_counts: vec![TraceValueCount {
                value: "4".to_string(),
                count: 2,
            }],
            peer_addr_counts: vec![TraceValueCount {
                value: "1".to_string(),
                count: 1,
            }],
        })
        .unwrap();

        assert_eq!(
            line,
            "bitswap WANT_HAVE probes: events=2 ok=1 failures=1 have=1 dont_have=0 block=0 want_block_followups=2 no_presence=1 bytes=0 extra_blocks=0 max_timeout=750ms elapsed=p50=25ms p90=750ms p95=750ms max=750ms outcomes=timeout_fallback_want_block=1 peers=peer-a=2 candidate_indexes=3=1 request_modes=want_have=2 first_addr_transports=tcp=1 first_addr_families=ip4=1 target_peer_counts=4=2 peer_addr_counts=1=1"
        );
    }

    #[test]
    fn trace_summary_counts_bitswap_timeout_recovery() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-timeout-recovery-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"provider_fetch_start\",\"cid\":\"cid-a\",\"provider_count\":4}\n",
                "{\"phase\":\"bitswap_request_timeout_detail\",\"elapsed_ms\":4000,\"cid\":\"cid-a\",\"peer_count\":10,\"trusted_peer_count\":2,\"want_block_target_count\":4,\"want_have_target_count\":6,\"timeout_ms\":4000}\n",
                "{\"phase\":\"bitswap_client_reset\"}\n",
                "{\"phase\":\"bitswap_request_timeout\",\"elapsed_ms\":4001,\"cid\":\"cid-a\",\"peer_count\":10,\"trusted_peer_count\":2,\"timeout_ms\":4000,\"reset_client\":true}\n",
                "{\"phase\":\"retry_provider_count\",\"cid\":\"cid-a\",\"same_provider_set\":true,\"same_bitswap_peer_set\":true,\"request_timeout\":true}\n",
                "{\"phase\":\"provider_retry_after_request_timeout\",\"cid\":\"cid-a\",\"provider_count\":8,\"request_timeout\":true}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":75,\"cid\":\"cid-a\",\"ok\":true,\"trusted_peer_count\":2,\"source_peer_trusted\":false,\"source_peer\":\"peer-a\",\"source_transport\":\"tcp\",\"bitswap_delivery\":\"incoming\",\"extra_blocks\":0,\"bytes\":123}\n",
                "{\"phase\":\"provider_fetch_start\",\"cid\":\"cid-b\",\"provider_count\":1}\n",
                "{\"phase\":\"bitswap_dial_plan\",\"cid\":\"cid-b\",\"peer_count\":1,\"candidate_peer_count\":1,\"new_dial_peer_count\":1,\"new_dial_addr_count\":1,\"suppressed_dial_peer_count\":0,\"suppressed_dial_addr_count\":0,\"pending_dial_peer_count\":0,\"connected_peer_count\":0,\"command_queued_ms\":3}\n",
                "{\"phase\":\"bitswap_request_timeout_detail\",\"elapsed_ms\":15000,\"cid\":\"cid-b\",\"peer_count\":1,\"trusted_peer_count\":0,\"want_block_target_count\":1,\"want_have_target_count\":0,\"timeout_ms\":15000}\n",
                "{\"phase\":\"bitswap_request_timeout\",\"elapsed_ms\":15001,\"cid\":\"cid-b\",\"peer_count\":1,\"trusted_peer_count\":0,\"timeout_ms\":15000,\"reset_client\":false}\n",
                "{\"phase\":\"retry_provider_count\",\"cid\":\"cid-b\",\"same_provider_set\":false,\"same_bitswap_peer_set\":false,\"request_timeout\":true}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":15000,\"cid\":\"cid-b\",\"ok\":false,\"trusted_peer_count\":0,\"error\":\"bitswap request timed out\"}\n",
                "{\"phase\":\"retry_provider_count\",\"cid\":\"cid-c\",\"same_provider_set\":true,\"same_bitswap_peer_set\":true,\"request_timeout\":true}\n",
                "{\"phase\":\"provider_retry_after_request_timeout\",\"cid\":\"cid-c\",\"provider_count\":1,\"request_timeout\":true}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.bitswap_timeout_recovery.request_timeouts, 2);
        assert_eq!(summary.bitswap_timeout_recovery.cold_request_timeouts, 1);
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .mixed_trusted_request_timeouts,
            1
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .trusted_only_request_timeouts,
            0
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .request_timeouts_without_dial_plan,
            1
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .mixed_trusted_request_timeouts_without_dial_plan,
            1
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .max_request_timeout_peer_count,
            10
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .request_timeout_want_block_targets,
            5
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .request_timeout_want_have_targets,
            6
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .max_request_timeout_want_block_targets,
            4
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .max_request_timeout_want_have_targets,
            6
        );
        let timeout_budgets = summary
            .bitswap_timeout_recovery
            .request_timeout_budgets
            .iter()
            .map(|budget| (budget.value.as_str(), budget.count))
            .collect::<Vec<_>>();
        assert!(timeout_budgets.contains(&("4000", 1)));
        assert!(timeout_budgets.contains(&("15000", 1)));
        assert_eq!(summary.bitswap_timeout_recovery.request_timeout_events, 2);
        assert_eq!(
            summary.bitswap_timeout_recovery.request_timeout_reset_true,
            1
        );
        assert_eq!(
            summary.bitswap_timeout_recovery.request_timeout_reset_false,
            1
        );
        assert_eq!(summary.bitswap_timeout_recovery.client_resets, 1);
        assert_eq!(summary.bitswap_timeout_recovery.provider_retry_starts, 3);
        assert_eq!(
            summary.bitswap_timeout_recovery.same_provider_retry_starts,
            2
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .refreshed_provider_retry_starts,
            1
        );
        assert_eq!(summary.bitswap_timeout_recovery.retry_successes, 1);
        assert_eq!(summary.bitswap_timeout_recovery.trusted_retry_successes, 0);
        assert_eq!(
            summary.bitswap_timeout_recovery.untrusted_retry_successes,
            1
        );
        assert_eq!(summary.bitswap_timeout_recovery.retry_failures, 1);
        assert_eq!(summary.bitswap_timeout_recovery.retry_unresolved, 1);
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .retry_success_elapsed_ms
                .count,
            1
        );
        assert_eq!(
            summary
                .bitswap_timeout_recovery
                .retry_success_elapsed_ms
                .p50_ms,
            Some(75)
        );
    }

    #[test]
    fn repeat_summary_aggregates_measured_resource_metrics() {
        let mut runs = vec![
            run_result(
                RunPhase::Warmup,
                0,
                999,
                Some(999),
                Some(999),
                Some(999),
                Some(999),
            ),
            run_result(
                RunPhase::Measured,
                1,
                300,
                Some(60),
                Some(52),
                Some(0),
                Some(1200),
            ),
            run_result(
                RunPhase::Measured,
                2,
                100,
                Some(40),
                Some(48),
                Some(1),
                None,
            ),
        ];
        runs[0].bitswap_seed_connect_elapsed_ms = Some(999);
        runs[1].bitswap_seed_connect_elapsed_ms = Some(42);
        runs[2].bitswap_seed_connect_elapsed_ms = Some(21);
        runs[1].kubo_bitswap_stats = Some(KuboBitswapStats {
            blocks_received: Some(10),
            data_received: Some(1000),
            peers_len: Some(4),
            ..KuboBitswapStats::default()
        });
        runs[2].kubo_bitswap_stats = Some(KuboBitswapStats {
            blocks_received: Some(30),
            data_received: Some(3000),
            peers_len: Some(6),
            ..KuboBitswapStats::default()
        });

        let summary = RepeatSummary::from_runs(&runs);

        assert_eq!(summary.measured_runs, 2);
        assert_eq!(summary.run_total_ms.count, 2);
        assert_eq!(summary.run_total_ms.p50_ms, Some(100));
        assert_eq!(summary.run_total_ms.p95_ms, Some(300));
        assert_eq!(summary.gateway_rss_kib.count, 2);
        assert_eq!(summary.gateway_rss_kib.p50, Some(40));
        assert_eq!(summary.gateway_rss_kib.max, Some(60));
        assert_eq!(summary.gateway_fd_count.max, Some(52));
        assert_eq!(summary.gateway_child_process_count.max, Some(1));
        assert_eq!(summary.gateway_storage_bytes.count, 1);
        assert_eq!(summary.gateway_storage_bytes.max, Some(1200));
        assert_eq!(summary.bitswap_seed_connect_ms.count, 2);
        assert_eq!(summary.bitswap_seed_connect_ms.p50_ms, Some(21));
        assert_eq!(summary.bitswap_seed_connect_ms.max_ms, Some(42));
        assert_eq!(summary.kubo_bitswap.blocks_received.count, 2);
        assert_eq!(summary.kubo_bitswap.blocks_received.p50, Some(10));
        assert_eq!(summary.kubo_bitswap.blocks_received.max, Some(30));
        assert_eq!(summary.kubo_bitswap.data_received.max, Some(3000));
        assert_eq!(summary.kubo_bitswap.peers_len.max, Some(6));
        assert_eq!(summary.cases.len(), 1);
        assert_eq!(summary.cases[0].root_ttfb_ms.count, 2);
    }

    #[test]
    fn storage_size_counts_sqlite_sidecars() {
        let path = unique_temp_path("mobile-web-storage-size-test.db");
        let wal = PathBuf::from(format!("{}-wal", path.display()));
        let shm = PathBuf::from(format!("{}-shm", path.display()));
        std::fs::write(&path, vec![0u8; 11]).unwrap();
        std::fs::write(&wal, vec![0u8; 17]).unwrap();
        std::fs::write(&shm, vec![0u8; 23]).unwrap();

        let size = storage_path_size_bytes(&path).unwrap();

        assert_eq!(size, 51);
        for file in [&path, &wal, &shm] {
            let _ = std::fs::remove_file(file);
        }
    }

    #[test]
    fn parses_kubo_bitswap_stats_json() {
        let stats = parse_kubo_bitswap_stats(
            br#"{
                "BlocksReceived": 12,
                "DataReceived": 3456,
                "BlocksSent": 2,
                "DataSent": 128,
                "DupBlksReceived": 1,
                "DupDataReceived": 64,
                "MessagesReceived": 9,
                "Wantlist": [{"/": "bafywant"}],
                "Peers": ["12D3KooWpeer1", "12D3KooWpeer2"]
            }"#,
        )
        .unwrap();

        assert_eq!(stats.blocks_received, Some(12));
        assert_eq!(stats.data_received, Some(3456));
        assert_eq!(stats.blocks_sent, Some(2));
        assert_eq!(stats.data_sent, Some(128));
        assert_eq!(stats.dup_blocks_received, Some(1));
        assert_eq!(stats.dup_data_received, Some(64));
        assert_eq!(stats.messages_received, Some(9));
        assert_eq!(stats.wantlist_len, Some(1));
        assert_eq!(stats.peers_len, Some(2));
    }

    #[test]
    fn comparison_case_reports_kubo_setup_adjusted_root_ttfb() {
        let rust_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                200,
                Some(40),
                Some(12),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                400,
                Some(41),
                Some(12),
                Some(0),
                None,
            ),
        ];
        let mut kubo_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                80,
                Some(100),
                Some(30),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                100,
                Some(101),
                Some(31),
                Some(0),
                None,
            ),
        ];
        kubo_runs[0].bitswap_seed_connect_elapsed_ms = Some(60);
        kubo_runs[1].bitswap_seed_connect_elapsed_ms = Some(70);

        let rust = run_report(
            HarnessEngine::RustHttp,
            Some(BitswapSeedConnectionSetup::DelegatedRouterProviderLookup),
            rust_runs,
        );
        let kubo = run_report(
            HarnessEngine::Kubo,
            Some(BitswapSeedConnectionSetup::SwarmConnectBeforeRequest),
            kubo_runs,
        );

        let cases = ComparisonCase::from_reports(&rust, &kubo);
        let case = &cases[0];

        assert_eq!(case.rust_root_ttfb_p50_ms, Some(100));
        assert_eq!(case.rust_root_ttfb_p95_ms, Some(200));
        assert_eq!(case.kubo_root_ttfb_p50_ms, Some(40));
        assert_eq!(case.kubo_root_ttfb_p95_ms, Some(50));
        assert_eq!(case.rust_root_total_p50_ms, Some(200));
        assert_eq!(case.rust_root_total_p95_ms, Some(400));
        assert_eq!(case.kubo_root_total_p50_ms, Some(80));
        assert_eq!(case.kubo_root_total_p95_ms, Some(100));
        assert_eq!(case.root_total_p50_ratio, Some(2.5));
        assert_eq!(case.root_total_p95_ratio, Some(4.0));
        assert_eq!(case.kubo_setup_adjusted_root_ttfb_p50_ms, Some(100));
        assert_eq!(case.kubo_setup_adjusted_root_ttfb_p95_ms, Some(120));
        assert_eq!(case.setup_adjusted_root_ttfb_p50_ratio, Some(1.0));
        let p95_ratio = case.setup_adjusted_root_ttfb_p95_ratio.unwrap();
        assert!((p95_ratio - (200.0 / 120.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn comparison_case_reports_asset_total_latency() {
        let mut rust_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                200,
                Some(40),
                Some(12),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                400,
                Some(41),
                Some(12),
                Some(0),
                None,
            ),
        ];
        rust_runs[0].results[0].assets = vec![script_asset_result(10, 30)];
        rust_runs[1].results[0].assets = vec![script_asset_result(20, 60)];

        let mut kubo_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                100,
                Some(100),
                Some(30),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                120,
                Some(101),
                Some(31),
                Some(0),
                None,
            ),
        ];
        kubo_runs[0].results[0].assets = vec![script_asset_result(5, 10)];
        kubo_runs[1].results[0].assets = vec![script_asset_result(10, 20)];

        let rust = run_report(HarnessEngine::RustHttp, None, rust_runs);
        let kubo = run_report(HarnessEngine::Kubo, None, kubo_runs);

        let cases = ComparisonCase::from_reports(&rust, &kubo);
        let case = &cases[0];

        assert_eq!(case.rust_asset_ttfb_p50_ms, Some(10));
        assert_eq!(case.rust_asset_ttfb_p95_ms, Some(20));
        assert_eq!(case.kubo_asset_ttfb_p50_ms, Some(5));
        assert_eq!(case.kubo_asset_ttfb_p95_ms, Some(10));
        assert_eq!(case.rust_asset_total_p50_ms, Some(30));
        assert_eq!(case.rust_asset_total_p95_ms, Some(60));
        assert_eq!(case.kubo_asset_total_p50_ms, Some(10));
        assert_eq!(case.kubo_asset_total_p95_ms, Some(20));
        assert_eq!(case.asset_total_p50_ratio, Some(3.0));
        assert_eq!(case.asset_total_p95_ratio, Some(3.0));
    }

    #[test]
    fn comparison_case_reports_per_asset_kubo_wins_by_path() {
        let mut rust_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                200,
                Some(40),
                Some(12),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                400,
                Some(41),
                Some(12),
                Some(0),
                None,
            ),
        ];
        rust_runs[0].results[0].assets = vec![
            script_asset_result_for_path(200, 240, 8080, "/ipfs/root/app.js"),
            script_asset_result_for_path(30, 40, 8080, "/ipfs/root/style.css"),
        ];
        rust_runs[1].results[0].assets = vec![
            script_asset_result_for_path(220, 260, 8080, "/ipfs/root/app.js"),
            script_asset_result_for_path(40, 50, 8080, "/ipfs/root/style.css"),
        ];

        let mut kubo_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                100,
                Some(100),
                Some(30),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                120,
                Some(101),
                Some(31),
                Some(0),
                None,
            ),
        ];
        kubo_runs[0].results[0].assets = vec![
            script_asset_result_for_path(80, 90, 5001, "/ipfs/root/app.js"),
            script_asset_result_for_path(60, 70, 5001, "/ipfs/root/style.css"),
        ];
        kubo_runs[1].results[0].assets = vec![
            script_asset_result_for_path(100, 110, 5001, "/ipfs/root/app.js"),
            script_asset_result_for_path(70, 80, 5001, "/ipfs/root/style.css"),
        ];

        let rust = run_report(HarnessEngine::RustHttp, None, rust_runs);
        let kubo = run_report(HarnessEngine::Kubo, None, kubo_runs);

        let cases = ComparisonCase::from_reports(&rust, &kubo);
        let assets = &cases[0].asset_comparisons;

        assert_eq!(assets[0].path, "/ipfs/root/app.js");
        let app = assets
            .iter()
            .find(|asset| asset.path == "/ipfs/root/app.js")
            .unwrap();
        assert_eq!(app.kind, "script");
        assert_eq!(app.rust_count, 2);
        assert_eq!(app.kubo_count, 2);
        assert_eq!(app.rust_pass_count, 2);
        assert_eq!(app.kubo_pass_count, 2);
        assert_eq!(app.rust_ttfb_p50_ms, Some(200));
        assert_eq!(app.kubo_ttfb_p50_ms, Some(80));
        assert!(app.meaningful_kubo_wins.iter().any(|win| {
            win.metric == "asset_ttfb_p50" && win.rust_ms == 200 && win.kubo_ms == 80
        }));

        let style = assets
            .iter()
            .find(|asset| asset.path == "/ipfs/root/style.css")
            .unwrap();
        assert!(style.meaningful_kubo_wins.is_empty());
    }

    #[test]
    fn comparison_case_attaches_rust_trace_to_asset_kubo_wins() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-comparison-asset-trace-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_start\",\"request_id\":1,\"path\":\"/ipfs/root/app.js\",\"span\":{\"path\":\"/ipfs/root/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipfs/root\"}}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":80,\"cid\":\"cid-a\",\"source\":\"http_provider\",\"span\":{\"path\":\"/ipfs/root/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipfs/root\"}}\n",
                "{\"phase\":\"http_provider_fetch\",\"elapsed_ms\":75,\"cid\":\"cid-a\",\"provider\":\"https://provider-a.example\",\"ok\":true,\"response_bytes\":2048,\"response_first_chunk_seen\":true,\"response_headers_elapsed_ms\":30,\"response_first_chunk_elapsed_ms\":35,\"response_body_elapsed_ms\":70,\"span\":{\"path\":\"/ipfs/root/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipfs/root\"}}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"elapsed_ms\":0,\"hits\":2,\"misses\":1,\"inserts\":1,\"path_hits\":3,\"path_misses\":4,\"path_inserts\":4,\"file_size_hits\":5,\"file_size_misses\":6,\"file_size_inserts\":6,\"span\":{\"path\":\"/ipfs/root/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipfs/root\"}}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipfs/root/app.js\",\"status\":200,\"elapsed_ms\":90,\"span\":{\"path\":\"/ipfs/root/app.js\",\"request_id\":1,\"progress_request_id\":101,\"parent_request_id\":100,\"top_level_path\":\"/ipfs/root\"}}\n",
            ),
        )
        .unwrap();

        let trace_summary = summarize_trace_output(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        let mut rust_runs = vec![run_result(
            RunPhase::Measured,
            1,
            200,
            Some(40),
            Some(12),
            Some(0),
            None,
        )];
        rust_runs[0].results[0].assets = vec![script_asset_result_for_path(
            200,
            240,
            8080,
            "/ipfs/root/app.js",
        )];

        let mut kubo_runs = vec![run_result(
            RunPhase::Measured,
            1,
            120,
            Some(100),
            Some(30),
            Some(0),
            None,
        )];
        kubo_runs[0].results[0].assets = vec![script_asset_result_for_path(
            80,
            90,
            5001,
            "/ipfs/root/app.js",
        )];

        let mut rust = run_report(HarnessEngine::RustHttp, None, rust_runs);
        rust.trace_summary = Some(trace_summary);
        let kubo = run_report(HarnessEngine::Kubo, None, kubo_runs);

        let cases = ComparisonCase::from_reports(&rust, &kubo);
        let asset = cases[0]
            .asset_comparisons
            .iter()
            .find(|asset| asset.path == "/ipfs/root/app.js")
            .unwrap();
        assert!(!asset.meaningful_kubo_wins.is_empty());
        let rust_trace = asset.rust_trace.as_ref().unwrap();
        assert_eq!(rust_trace.request_count, 1);
        assert_eq!(
            trace_value_count(&rust_trace.block_sources, "http_provider"),
            1
        );
        assert_eq!(
            trace_value_count(
                &rust_trace.http_provider_fetch_providers,
                "https://provider-a.example"
            ),
            1
        );
        assert_eq!(rust_trace.http_provider_fetch_max_ms, 75);
        assert_eq!(rust_trace.http_provider_fetch_response_bytes, 2048);
        assert_eq!(rust_trace.http_provider_fetch_first_chunk_events, 1);
        assert_eq!(rust_trace.http_provider_fetch_headers_max_ms, 30);
        assert_eq!(rust_trace.http_provider_fetch_first_chunk_max_ms, 35);
        assert_eq!(rust_trace.http_provider_fetch_body_max_ms, 70);
        assert_eq!(rust_trace.unixfs_metadata_cache.events, 1);
        assert_eq!(rust_trace.unixfs_metadata_cache.hits, 2);
        assert_eq!(rust_trace.unixfs_metadata_cache.path_misses, 4);
        assert_eq!(rust_trace.unixfs_metadata_cache.file_size_inserts, 6);
    }

    #[test]
    fn comparison_case_reports_meaningful_kubo_wins() {
        let mut rust_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                400,
                Some(40),
                Some(12),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                800,
                Some(41),
                Some(12),
                Some(0),
                None,
            ),
        ];
        rust_runs[0].results[0].assets = vec![script_asset_result(3, 3)];
        rust_runs[1].results[0].assets = vec![script_asset_result(3, 3)];

        let mut kubo_runs = vec![
            run_result(
                RunPhase::Measured,
                1,
                200,
                Some(100),
                Some(30),
                Some(0),
                None,
            ),
            run_result(
                RunPhase::Measured,
                2,
                240,
                Some(101),
                Some(31),
                Some(0),
                None,
            ),
        ];
        kubo_runs[0].results[0].assets = vec![script_asset_result(1, 1)];
        kubo_runs[1].results[0].assets = vec![script_asset_result(1, 1)];

        let rust = run_report(HarnessEngine::RustHttp, None, rust_runs);
        let kubo = run_report(HarnessEngine::Kubo, None, kubo_runs);

        let cases = ComparisonCase::from_reports(&rust, &kubo);
        let wins = &cases[0].meaningful_kubo_wins;

        assert!(wins.iter().any(|win| {
            win.metric == "root_ttfb_p50"
                && win.rust_ms == 200
                && win.kubo_ms == 100
                && win.delta_ms == 100
        }));
        assert!(wins.iter().all(|win| !win.metric.starts_with("asset_")));
    }

    #[test]
    fn meaningful_kubo_win_filters_noise_and_small_ratios() {
        assert!(meaningful_kubo_win("asset_ttfb_p50", 3, 1).is_none());
        assert!(meaningful_kubo_win("root_ttfb_p50", 149, 100).is_none());
        assert!(meaningful_kubo_win("root_ttfb_p50", 109, 100).is_none());

        let win = meaningful_kubo_win("root_ttfb_p95", 160, 100).unwrap();
        assert_eq!(win.metric, "root_ttfb_p95");
        assert_eq!(win.delta_ms, 60);
        assert!((win.ratio - 1.6).abs() < f64::EPSILON);
    }

    #[test]
    fn case_aggregate_counts_conditional_revalidations() {
        let mut run = run_result(
            RunPhase::Measured,
            1,
            100,
            Some(40),
            Some(48),
            Some(0),
            Some(1200),
        );
        run.results[0].revalidation = Some(RevalidationResult {
            status: Some(304),
            etag: Some("\"root\"".to_string()),
            cache_control: Some("public, max-age=31536000, immutable".to_string()),
            body_bytes: 0,
            ttfb_ms: 3,
            total_ms: 3,
            stream: FetchStreamMetrics::default(),
            passed: true,
            failures: Vec::new(),
        });
        run.results[0].assets = vec![AssetResult {
            kind: AssetKind::Script,
            source: "app.js".to_string(),
            url: "http://127.0.0.1:8080/ipfs/root/app.js".to_string(),
            status: Some(200),
            content_type: Some("text/javascript".to_string()),
            content_range: None,
            content_length: Some(128),
            accept_ranges: None,
            etag: Some("\"asset\"".to_string()),
            cache_control: Some("public, max-age=31536000, immutable".to_string()),
            body_bytes: 128,
            ttfb_ms: 5,
            total_ms: 6,
            stream: FetchStreamMetrics::default(),
            body_preview: String::new(),
            revalidation: Some(RevalidationResult {
                status: Some(200),
                etag: Some("\"asset\"".to_string()),
                cache_control: Some("public, max-age=31536000, immutable".to_string()),
                body_bytes: 128,
                ttfb_ms: 4,
                total_ms: 5,
                stream: FetchStreamMetrics::default(),
                passed: false,
                failures: vec!["status 200, expected 304".to_string()],
            }),
            passed: false,
            failures: vec!["conditional revalidation: status 200, expected 304".to_string()],
        }];

        let runs = [run];
        let aggregate = CaseAggregate::from_runs("case", &[&runs[0]]);

        assert_eq!(aggregate.root_revalidation_attempts, 1);
        assert_eq!(aggregate.root_revalidation_passed, 1);
        assert_eq!(aggregate.root_revalidation_failed, 0);
        assert_eq!(aggregate.root_revalidation_ttfb_ms.p50_ms, Some(3));
        assert_eq!(aggregate.asset_revalidation_attempts, 1);
        assert_eq!(aggregate.asset_revalidation_passed, 0);
        assert_eq!(aggregate.asset_revalidation_failed, 1);
        assert_eq!(aggregate.asset_revalidation_ttfb_ms.p50_ms, Some(4));
        assert_eq!(aggregate.asset_kind_failures[0].kind, "script");
        assert!(aggregate
            .failure_groups
            .iter()
            .any(|group| group.key.contains("conditional revalidation")));
    }

    #[tokio::test]
    async fn conditional_revalidation_requires_etag_for_eligible_gets() {
        let client = GatewayClient::http(Duration::from_secs(1)).unwrap();
        let response = FetchResponse {
            status: 200,
            content_type: Some("text/plain".to_string()),
            content_range: None,
            content_length: Some(5),
            accept_ranges: None,
            etag: None,
            cache_control: Some("public, max-age=31536000, immutable".to_string()),
            body: b"hello".to_vec(),
            ttfb_ms: 1,
            total_ms: 1,
            stream: FetchStreamMetrics::default(),
        };

        let revalidation = maybe_revalidate_response(
            &client,
            "http://127.0.0.1:9/ipfs/root",
            "GET",
            None,
            &response,
            true,
            None,
        )
        .await
        .expect("eligible GET should produce a revalidation result");

        assert!(!revalidation.passed);
        assert_eq!(revalidation.status, None);
        assert_eq!(revalidation.failures, vec!["response omitted ETag"]);
    }

    #[test]
    fn run_timeout_failure_results_marks_matching_cases_failed() {
        let corpus = Corpus {
            entries: vec![
                CorpusEntry {
                    id: "first".to_string(),
                    description: Some("first case".to_string()),
                    path: "/ipfs/first".to_string(),
                    default_enabled: None,
                    method: None,
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_content_length: None,
                    expect_accept_ranges: None,
                    expect_etag_prefix: None,
                    expect_cache_control: None,
                    expect_body_contains: None,
                    expect_body_bytes: None,
                    expect_body_sha256: None,
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
                CorpusEntry {
                    id: "second".to_string(),
                    description: None,
                    path: "/ipfs/second".to_string(),
                    default_enabled: None,
                    method: Some("HEAD".to_string()),
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_content_length: None,
                    expect_accept_ranges: None,
                    expect_etag_prefix: None,
                    expect_cache_control: None,
                    expect_body_contains: None,
                    expect_body_bytes: Some(0),
                    expect_body_sha256: None,
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
            ],
        };

        let results = run_timeout_failure_results(
            "http://127.0.0.1:8080/",
            &corpus,
            &["second".to_string()],
            Duration::from_secs(7),
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "second");
        assert_eq!(results[0].method, "HEAD");
        assert_eq!(results[0].url, "http://127.0.0.1:8080/ipfs/second");
        assert!(!results[0].passed);
        assert_eq!(results[0].failures, vec!["run timed out after 7s"]);
    }

    #[test]
    fn corpus_entries_can_be_explicit_only() {
        let default_entry = CorpusEntry {
            id: "default".to_string(),
            description: None,
            path: "/ipfs/default".to_string(),
            default_enabled: None,
            method: None,
            range: None,
            crawl: None,
            expect_status: Some(200),
            expect_content_type_prefix: None,
            expect_content_range_prefix: None,
            expect_content_length: None,
            expect_accept_ranges: None,
            expect_etag_prefix: None,
            expect_cache_control: None,
            expect_body_contains: None,
            expect_body_bytes: None,
            expect_body_sha256: None,
            min_bytes: None,
            max_ttfb_ms: None,
        };
        let explicit_only = CorpusEntry {
            id: "explicit".to_string(),
            description: None,
            path: "/ipfs/explicit".to_string(),
            default_enabled: Some(false),
            method: None,
            range: None,
            crawl: None,
            expect_status: Some(200),
            expect_content_type_prefix: None,
            expect_content_range_prefix: None,
            expect_content_length: None,
            expect_accept_ranges: None,
            expect_etag_prefix: None,
            expect_cache_control: None,
            expect_body_contains: None,
            expect_body_bytes: None,
            expect_body_sha256: None,
            min_bytes: None,
            max_ttfb_ms: None,
        };

        assert!(entry_selected(&default_entry, &[]));
        assert!(!entry_selected(&explicit_only, &[]));
        assert!(entry_selected(&explicit_only, &["explicit".to_string()]));
        assert!(!entry_selected(&default_entry, &["explicit".to_string()]));
    }

    #[test]
    fn labeled_trace_output_adds_label_before_extension() {
        assert_eq!(
            labeled_trace_output(&PathBuf::from("/tmp/replay.jsonl"), "offline"),
            PathBuf::from("/tmp/replay-offline.jsonl")
        );
        assert_eq!(
            labeled_trace_output(&PathBuf::from("/tmp/replay"), "online"),
            PathBuf::from("/tmp/replay-online")
        );
    }

    #[test]
    fn sha256_hex_hashes_response_bodies() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[tokio::test]
    async fn collect_body_stream_records_incremental_metrics() {
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"defgh")),
        ];
        let (body, metrics) =
            collect_body_stream(futures::stream::iter(chunks), Instant::now(), None)
                .await
                .unwrap();

        assert_eq!(body, b"abcdefgh");
        assert_eq!(metrics.chunk_count, 2);
        assert_eq!(metrics.max_chunk_bytes, 5);
        assert_eq!(metrics.max_buffered_bytes, 5);
        assert!(metrics.first_byte_ms.is_some());
        assert!(metrics.completed);
        assert!(!metrics.cancelled);
    }

    #[tokio::test]
    async fn native_gateway_fetch_can_drop_after_first_stream_chunk() {
        let stream_chunk_bytes = 64 * 1024usize;
        let len = stream_chunk_bytes * 3;
        let data = (0..len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let cid = cid_from_data(CODEC_RAW, &data);
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(CountingBlockProvider {
            cid,
            data,
            calls: calls.clone(),
        });
        let native = NativeGateway {
            core: GatewayCore::with_provider(provider),
            storage_path: None,
            remove_storage_on_stop: false,
        };
        let range_end = stream_chunk_bytes * 2 + 9;
        let response = native
            .fetch_response_and_drop_after_chunks(
                &format!("http://freedom-ipfs-native.local/ipfs/{cid}"),
                "GET",
                Some(&format!("bytes=0-{range_end}")),
                1,
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::PARTIAL_CONTENT.as_u16());
        assert_eq!(response.stream.chunk_count, 1);
        assert_eq!(response.body.len(), response.stream.max_chunk_bytes);
        assert!(response.body.len() <= stream_chunk_bytes);
        assert!(response.stream.first_byte_ms.is_some());
        assert!(response.stream.cancelled);
        assert!(!response.stream.completed);
        assert!(response.stream.max_buffered_bytes <= stream_chunk_bytes);

        let calls_after_drop = calls.load(AtomicOrdering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            calls.load(AtomicOrdering::SeqCst),
            calls_after_drop,
            "dropping the native body stream should stop further range reads"
        );
    }

    #[tokio::test]
    async fn native_ffi_engine_fetches_imported_block_through_event_mux() {
        let _guard = native_ffi_harness_test_guard().await;
        let data = b"native ffi harness body".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let store = SqliteBlockStore::in_memory(4 * 1024 * 1024).unwrap();
        store.put_block(&cid, &data).unwrap();
        let car = store.export_car().unwrap();
        let car_path = unique_temp_path("native-ffi-harness.car");
        std::fs::write(&car_path, car).unwrap();

        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "5",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: vec![CorpusEntry {
                id: "native-ffi-raw-block".to_string(),
                description: None,
                path: format!("/ipfs/{cid}"),
                default_enabled: Some(true),
                method: Some("GET".to_string()),
                range: None,
                crawl: None,
                expect_status: Some(200),
                expect_content_type_prefix: None,
                expect_content_range_prefix: None,
                expect_content_length: Some(data.len() as u64),
                expect_accept_ranges: Some("bytes".to_string()),
                expect_etag_prefix: None,
                expect_cache_control: None,
                expect_body_contains: None,
                expect_body_bytes: Some(data.len()),
                expect_body_sha256: Some(sha256_hex(&data)),
                min_bytes: None,
                max_ttfb_ms: None,
            }],
        };

        let report = run_harness(&args, &corpus).await.unwrap();
        let _ = std::fs::remove_file(&car_path);
        assert_eq!(report.engine, HarnessEngine::RustNativeFfi);
        assert_eq!(report.summary.pass_count, 1);
        assert_eq!(report.summary.fail_count, 0);
        let native = report.runs[0]
            .native_ffi
            .as_ref()
            .expect("native FFI report should be attached");
        assert_eq!(native.dispatcher_count, 1);
        assert_eq!(native.read_buffer_bytes, 5);
        assert_eq!(native.requests_started, 1);
        assert_eq!(native.bodies_completed, 1);
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        assert!(native.events_received > 0);
        assert!(native.read_calls > 0);
        assert_eq!(native.bytes_read, data.len() as u64);
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert_eq!(mobile.total_started, 1);
        assert_eq!(mobile.total_completed, 1);
        assert_eq!(mobile.total_freed, 1);
        assert_eq!(mobile.bytes_read, data.len() as u64);
        assert!(mobile.events_enqueued > 0);
        assert!(mobile.events_delivered > 0);
    }

    #[tokio::test]
    async fn native_ffi_engine_drops_post_free_events_instead_of_stashing() {
        let _guard = native_ffi_harness_test_guard().await;
        let data = b"native ffi stale free event body".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let store = SqliteBlockStore::in_memory(4 * 1024 * 1024).unwrap();
        store.put_block(&cid, &data).unwrap();
        let car_path = unique_temp_path("native-ffi-stale-free.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();

        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "5",
        ])
        .unwrap();
        let mut gateway = NativeFfiGateway::start(&args, None).await.unwrap();
        let client = GatewayClient::NativeFfi(gateway.clone());
        let path = format!("/ipfs/{cid}");
        let correlation = RequestCorrelation::root(path.clone());
        let response = client
            .fetch_response(
                &format!("http://freedom-ipfs-native-ffi.local{path}"),
                "GET",
                None,
                None,
                Some(&correlation),
            )
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, data);

        let deadline = Instant::now() + Duration::from_secs(2);
        let native = loop {
            let native = gateway.report();
            if native.handle_freed_events > 0 || Instant::now() >= deadline {
                break native;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(
            native.handle_freed_events > 0,
            "dispatcher did not observe post-free event: {native:?}"
        );
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        assert!(
            native.stale_events > 0,
            "post-free event should be counted as stale instead of stashed: {native:?}"
        );
        gateway.stop().await;
        let _ = std::fs::remove_file(&car_path);
    }

    #[tokio::test]
    async fn native_ffi_engine_preserves_head_range_and_revalidation() {
        let _guard = native_ffi_harness_test_guard().await;
        let data = b"native ffi conditional and range body".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let store = SqliteBlockStore::in_memory(4 * 1024 * 1024).unwrap();
        store.put_block(&cid, &data).unwrap();
        let car_path = unique_temp_path("native-ffi-headers-fixture.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();

        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "4",
            "--conditional-revalidate",
        ])
        .unwrap();
        let path = format!("/ipfs/{cid}");
        let corpus = Corpus {
            entries: vec![
                CorpusEntry {
                    id: "native-ffi-conditional".to_string(),
                    description: None,
                    path: path.clone(),
                    default_enabled: Some(true),
                    method: Some("GET".to_string()),
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_content_length: Some(data.len() as u64),
                    expect_accept_ranges: Some("bytes".to_string()),
                    expect_etag_prefix: Some("\"fi1:".to_string()),
                    expect_cache_control: Some("public, max-age=31536000, immutable".to_string()),
                    expect_body_contains: None,
                    expect_body_bytes: Some(data.len()),
                    expect_body_sha256: Some(sha256_hex(&data)),
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
                CorpusEntry {
                    id: "native-ffi-range".to_string(),
                    description: None,
                    path: path.clone(),
                    default_enabled: Some(true),
                    method: Some("GET".to_string()),
                    range: Some("bytes=3-10".to_string()),
                    crawl: None,
                    expect_status: Some(206),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: Some("bytes 3-10/".to_string()),
                    expect_content_length: Some(8),
                    expect_accept_ranges: Some("bytes".to_string()),
                    expect_etag_prefix: Some("\"fi1:".to_string()),
                    expect_cache_control: Some("public, max-age=31536000, immutable".to_string()),
                    expect_body_contains: None,
                    expect_body_bytes: Some(8),
                    expect_body_sha256: Some(sha256_hex(&data[3..=10])),
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
                CorpusEntry {
                    id: "native-ffi-head".to_string(),
                    description: None,
                    path,
                    default_enabled: Some(true),
                    method: Some("HEAD".to_string()),
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_content_length: Some(data.len() as u64),
                    expect_accept_ranges: Some("bytes".to_string()),
                    expect_etag_prefix: Some("\"fi1:".to_string()),
                    expect_cache_control: Some("public, max-age=31536000, immutable".to_string()),
                    expect_body_contains: None,
                    expect_body_bytes: Some(0),
                    expect_body_sha256: None,
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
            ],
        };

        let report = run_harness(&args, &corpus).await.unwrap();
        let _ = std::fs::remove_file(&car_path);
        assert_eq!(report.summary.fail_count, 0);
        assert_eq!(report.runs[0].results.len(), 3);
        assert!(
            report.runs[0].results.iter().all(|result| result.passed),
            "failures: {:?}",
            report.runs[0]
                .results
                .iter()
                .map(|result| (&result.id, &result.failures))
                .collect::<Vec<_>>()
        );
        let conditional = report.runs[0]
            .results
            .iter()
            .find(|result| result.id == "native-ffi-conditional")
            .unwrap();
        assert!(conditional.revalidation.as_ref().unwrap().passed);
        assert_eq!(conditional.revalidation.as_ref().unwrap().status, Some(304));
        let native = report.runs[0].native_ffi.as_ref().unwrap();
        assert_eq!(native.requests_started, 4);
        assert_eq!(native.bodies_completed, 4);
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
    }

    #[tokio::test]
    async fn native_ffi_engine_reports_missing_path_error_response() {
        let _guard = native_ffi_harness_test_guard().await;
        let missing_cid = cid_from_data(CODEC_RAW, b"not imported into native ffi cache");
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "11",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: vec![CorpusEntry {
                id: "native-ffi-missing".to_string(),
                description: None,
                path: format!("/ipfs/{missing_cid}"),
                default_enabled: Some(true),
                method: Some("GET".to_string()),
                range: None,
                crawl: None,
                expect_status: Some(502),
                expect_content_type_prefix: Some("text/html".to_string()),
                expect_content_range_prefix: None,
                expect_content_length: None,
                expect_accept_ranges: None,
                expect_etag_prefix: None,
                expect_cache_control: None,
                expect_body_contains: None,
                expect_body_bytes: None,
                expect_body_sha256: None,
                min_bytes: Some(1),
                max_ttfb_ms: None,
            }],
        };

        let report = run_harness(&args, &corpus).await.unwrap();
        assert_eq!(
            report.summary.fail_count, 0,
            "failures: {:?}",
            report.runs[0].results[0].failures
        );
        assert_eq!(report.runs[0].results[0].status, Some(502));
        let native = report.runs[0].native_ffi.as_ref().unwrap();
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert_eq!(mobile.total_started, 1);
        assert_eq!(mobile.total_completed, 1);
        assert_eq!(mobile.total_freed, 1);
    }

    #[tokio::test]
    async fn native_ffi_engine_drives_browser_fixture_with_one_and_four_dispatchers() {
        let _guard = native_ffi_harness_test_guard().await;
        let asset_count = 50usize;
        let (car_path, corpus, expected_body_bytes) = native_ffi_browser_fixture(asset_count);

        for dispatchers in [1usize, 4] {
            let args = Args::try_parse_from([
                "mobile-web-harness",
                "--engine",
                "rust-native-ffi",
                "--routing-mode",
                "offline",
                "--gateway-import-car",
                car_path.to_str().unwrap(),
                "--native-dispatchers",
                &dispatchers.to_string(),
                "--native-read-buffer-bytes",
                "7",
            ])
            .unwrap();
            let report = run_harness(&args, &corpus).await.unwrap();
            assert_eq!(
                report.summary.pass_count,
                1,
                "failures: {:?}; failed assets: {:?}",
                report.runs[0].results[0].failures,
                report.runs[0].results[0]
                    .assets
                    .iter()
                    .filter(|asset| !asset.passed)
                    .map(|asset| (&asset.url, &asset.failures))
                    .collect::<Vec<_>>()
            );
            assert_eq!(report.summary.fail_count, 0);
            let result = &report.runs[0].results[0];
            assert_eq!(result.asset_summary.as_ref().unwrap().fetched, asset_count);
            assert_eq!(result.asset_summary.as_ref().unwrap().failed, 0);
            let native = report.runs[0]
                .native_ffi
                .as_ref()
                .expect("native FFI report should be attached");
            assert_eq!(native.dispatcher_count, dispatchers);
            assert_eq!(native.requests_started, (asset_count + 1) as u64);
            assert_eq!(native.bodies_completed, (asset_count + 1) as u64);
            assert_eq!(native.active_handles_at_end, 0);
            assert_eq!(native.stashed_event_handles_at_end, 0);
            assert_eq!(native.bytes_read, expected_body_bytes as u64);
            assert!(native.max_active_handles > 1);
            let mobile = native.mobile_layer.as_ref().unwrap();
            assert_eq!(mobile.active_native_handles, 0);
            assert_eq!(mobile.total_started, (asset_count + 1) as u64);
            assert_eq!(mobile.total_completed, (asset_count + 1) as u64);
            assert_eq!(mobile.total_freed, (asset_count + 1) as u64);
            assert_eq!(mobile.bytes_read, expected_body_bytes as u64);
            assert!(mobile.max_active_handles > 1);
            assert!(mobile.max_event_queue_depth > 0);
        }

        let _ = std::fs::remove_file(&car_path);
    }

    #[tokio::test]
    async fn native_ffi_engine_slow_consumer_completes_browser_fixture() {
        let _guard = native_ffi_harness_test_guard().await;
        let (car_path, corpus, _expected_body_bytes) = native_ffi_browser_fixture(20);
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "3",
            "--native-slow-consumer-ms",
            "1",
        ])
        .unwrap();
        let report = run_harness(&args, &corpus).await.unwrap();
        let _ = std::fs::remove_file(&car_path);
        assert_eq!(
            report.summary.pass_count, 1,
            "failures: {:?}",
            report.runs[0].results[0].failures
        );
        assert_eq!(report.summary.fail_count, 0);
        let native = report.runs[0].native_ffi.as_ref().unwrap();
        assert_eq!(native.dispatcher_count, 1);
        assert_eq!(native.slow_consumer_ms, 1);
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        assert!(native.read_calls > native.requests_started);
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert!(mobile.events_enqueued >= native.requests_started);
    }

    #[tokio::test]
    async fn native_ffi_engine_cancel_after_first_byte_returns_clean_terminal_state() {
        let _guard = native_ffi_harness_test_guard().await;
        let data = vec![42u8; 128 * 1024];
        let cid = cid_from_data(CODEC_RAW, &data);
        let store = SqliteBlockStore::in_memory(4 * 1024 * 1024).unwrap();
        store.put_block(&cid, &data).unwrap();
        let car_path = unique_temp_path("native-ffi-cancel-fixture.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "4096",
            "--native-cancel-after-first-byte",
        ])
        .unwrap();
        let corpus = Corpus {
            entries: vec![CorpusEntry {
                id: "native-ffi-cancel".to_string(),
                description: None,
                path: format!("/ipfs/{cid}"),
                default_enabled: Some(true),
                method: Some("GET".to_string()),
                range: None,
                crawl: None,
                expect_status: Some(200),
                expect_content_type_prefix: None,
                expect_content_range_prefix: None,
                expect_content_length: Some(data.len() as u64),
                expect_accept_ranges: Some("bytes".to_string()),
                expect_etag_prefix: None,
                expect_cache_control: None,
                expect_body_contains: None,
                expect_body_bytes: Some(data.len()),
                expect_body_sha256: Some(sha256_hex(&data)),
                min_bytes: None,
                max_ttfb_ms: None,
            }],
        };

        let report = run_harness(&args, &corpus).await.unwrap();
        let _ = std::fs::remove_file(&car_path);
        assert_eq!(report.summary.pass_count, 0);
        assert_eq!(report.summary.fail_count, 1);
        let native = report.runs[0].native_ffi.as_ref().unwrap();
        assert_eq!(native.requests_started, 1);
        assert!(native.cancelled_requests >= 1);
        assert_eq!(native.active_handles_at_end, 0);
        assert!(native.freed_handles >= 1);
        assert!(native.bytes_read > 0);
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert_eq!(mobile.total_started, 1);
        assert!(mobile.total_cancelled >= 1);
        assert!(mobile.total_freed >= 1);
        assert!(mobile.bytes_read > 0);
    }

    #[tokio::test]
    async fn native_ffi_engine_cancellation_storm_returns_active_handles_to_zero() {
        let _guard = native_ffi_harness_test_guard().await;
        let request_count = 20usize;
        let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
        let mut paths = Vec::new();
        for index in 0..request_count {
            let data = vec![index as u8; 64 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            paths.push(format!("/ipfs/{cid}"));
        }
        let car_path = unique_temp_path("native-ffi-cancel-storm.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "1024",
            "--native-cancel-after-first-byte",
        ])
        .unwrap();
        let mut gateway = NativeFfiGateway::start(&args, None).await.unwrap();
        let client = GatewayClient::NativeFfi(gateway.clone());
        let mut tasks = JoinSet::new();
        for path in paths {
            let client = client.clone();
            tasks.spawn(async move {
                let correlation = RequestCorrelation::root(path.clone());
                client
                    .fetch_response(
                        &format!("http://freedom-ipfs-native-ffi.local{path}"),
                        "GET",
                        None,
                        None,
                        Some(&correlation),
                    )
                    .await
            });
        }
        let mut cancelled = 0usize;
        while let Some(result) = tasks.join_next().await {
            let result = result.unwrap();
            if result
                .as_ref()
                .err()
                .is_some_and(|err| err.contains("cancelled"))
            {
                cancelled += 1;
            }
        }
        assert_eq!(cancelled, request_count);
        let native = gateway.report();
        assert_eq!(native.dispatcher_count, 1);
        assert_eq!(native.requests_started, request_count as u64);
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        assert_eq!(native.cancelled_requests, request_count as u64);
        assert_eq!(native.freed_handles, request_count as u64);
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert_eq!(mobile.total_started, request_count as u64);
        assert_eq!(mobile.total_cancelled, request_count as u64);
        assert_eq!(mobile.total_freed, request_count as u64);
        assert!(mobile.bytes_read > 0);
        gateway.stop().await;
        let _ = std::fs::remove_file(&car_path);
    }

    #[tokio::test]
    async fn native_ffi_engine_node_stop_wakes_active_request() {
        let _guard = native_ffi_harness_test_guard().await;
        let data = vec![7u8; 2 * 1024 * 1024];
        let cid = cid_from_data(CODEC_RAW, &data);
        let store = SqliteBlockStore::in_memory(8 * 1024 * 1024).unwrap();
        store.put_block(&cid, &data).unwrap();
        let car_path = unique_temp_path("native-ffi-node-stop.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();
        let args = Args::try_parse_from([
            "mobile-web-harness",
            "--engine",
            "rust-native-ffi",
            "--routing-mode",
            "offline",
            "--gateway-import-car",
            car_path.to_str().unwrap(),
            "--native-dispatchers",
            "1",
            "--native-read-buffer-bytes",
            "1",
            "--native-slow-consumer-ms",
            "25",
        ])
        .unwrap();
        let mut gateway = NativeFfiGateway::start(&args, None).await.unwrap();
        let client = GatewayClient::NativeFfi(gateway.clone());
        let path = format!("/ipfs/{cid}");
        let correlation = RequestCorrelation::root(path.clone());
        let request = tokio::spawn(async move {
            client
                .fetch_response(
                    &format!("http://freedom-ipfs-native-ffi.local{path}"),
                    "GET",
                    None,
                    None,
                    Some(&correlation),
                )
                .await
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if gateway.report().active_handles_at_end > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            gateway.report().active_handles_at_end > 0,
            "native FFI request did not become active before node-stop stress"
        );
        gateway.stop_node();
        let result = request.await.unwrap();
        let native = gateway.report();
        assert!(
            result.as_ref().err().is_some_and(|err| {
                err.contains("gateway stopped")
                    || err.contains("cancelled")
                    || err.contains("invalid handle")
            }),
            "native FFI request unexpectedly completed during node-stop stress: status={:?} body_bytes={:?} stream={:?} native={:?}",
            result.as_ref().ok().map(|response| response.status),
            result.as_ref().ok().map(|response| response.body.len()),
            result.as_ref().ok().map(|response| response.stream),
            native,
        );
        assert_eq!(native.active_handles_at_end, 0);
        assert_eq!(native.stashed_event_handles_at_end, 0);
        assert!(
            native.gateway_stopped_events > 0
                || native.cancelled_requests > 0
                || native.failed_requests > 0
        );
        let mobile = native.mobile_layer.as_ref().unwrap();
        assert_eq!(mobile.active_native_handles, 0);
        assert!(mobile.stop_generation > 0);
        assert!(mobile.total_cancelled > 0);
        gateway.stop().await;
        let _ = std::fs::remove_file(&car_path);
    }

    fn native_ffi_browser_fixture(asset_count: usize) -> (PathBuf, Corpus, usize) {
        let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
        let mut html = String::from("<!doctype html><title>native ffi fixture</title>\n");
        let mut expected_body_bytes = 0usize;
        for index in 0..asset_count {
            let data = format!("native ffi asset {index:02}\n").into_bytes();
            expected_body_bytes += data.len();
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            html.push_str(&format!("<iframe src=\"/ipfs/{cid}\"></iframe>\n"));
        }
        let root = html.into_bytes();
        expected_body_bytes += root.len();
        let root_cid = cid_from_data(CODEC_RAW, &root);
        store.put_block(&root_cid, &root).unwrap();
        let car_path = unique_temp_path("native-ffi-browser-fixture.car");
        std::fs::write(&car_path, store.export_car().unwrap()).unwrap();
        let corpus = Corpus {
            entries: vec![CorpusEntry {
                id: "native-ffi-browser-fixture".to_string(),
                description: Some("Synthetic browser-like native FFI fixture".to_string()),
                path: format!("/ipfs/{root_cid}"),
                default_enabled: Some(true),
                method: Some("GET".to_string()),
                range: None,
                crawl: Some(CrawlConfig {
                    max_assets: Some(asset_count),
                    min_assets: Some(asset_count),
                    max_failed_assets: Some(0),
                    same_origin_only: Some(true),
                    include_css_assets: Some(false),
                    asset_max_bytes: Some(128 * 1024),
                }),
                expect_status: Some(200),
                expect_content_type_prefix: None,
                expect_content_range_prefix: None,
                expect_content_length: Some(root.len() as u64),
                expect_accept_ranges: Some("bytes".to_string()),
                expect_etag_prefix: None,
                expect_cache_control: None,
                expect_body_contains: Some("native ffi fixture".to_string()),
                expect_body_bytes: Some(root.len()),
                expect_body_sha256: Some(sha256_hex(&root)),
                min_bytes: None,
                max_ttfb_ms: None,
            }],
        };
        (car_path, corpus, expected_body_bytes)
    }

    async fn native_ffi_harness_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        LOCK.lock().await
    }

    #[test]
    fn offline_replay_summary_collects_failed_roots_and_assets() {
        let mut run = run_result(
            RunPhase::Measured,
            1,
            100,
            Some(40),
            Some(48),
            Some(0),
            Some(2048),
        );
        run.passed = false;
        run.results[0].passed = false;
        run.results[0].failures = vec!["status 504, expected 200".to_string()];
        run.results[0].assets = vec![
            AssetResult {
                kind: AssetKind::Script,
                source: "app.js".to_string(),
                url: "http://127.0.0.1:8080/ipfs/root/app.js".to_string(),
                status: Some(504),
                content_type: Some("text/html".to_string()),
                content_range: None,
                content_length: None,
                accept_ranges: None,
                etag: None,
                cache_control: None,
                body_bytes: 0,
                ttfb_ms: 10,
                total_ms: 10,
                stream: FetchStreamMetrics::default(),
                body_preview: String::new(),
                revalidation: None,
                passed: false,
                failures: vec!["status 504, expected 200".to_string()],
            },
            AssetResult {
                kind: AssetKind::Image,
                source: "logo.png".to_string(),
                url: "http://127.0.0.1:8080/ipfs/root/logo.png".to_string(),
                status: Some(200),
                content_type: Some("image/png".to_string()),
                content_range: None,
                content_length: Some(128),
                accept_ranges: Some("bytes".to_string()),
                etag: None,
                cache_control: None,
                body_bytes: 128,
                ttfb_ms: 5,
                total_ms: 5,
                stream: FetchStreamMetrics::default(),
                body_preview: String::new(),
                revalidation: None,
                passed: true,
                failures: Vec::new(),
            },
        ];
        let runs = vec![run];
        let report = RunReport {
            gateway_url: None,
            generated_at_unix_seconds: 0,
            repeat: 1,
            warmup_runs: 0,
            fresh_gateway_per_run: false,
            asset_concurrency: 1,
            conditional_revalidate: false,
            run_timeout_secs: None,
            engine: HarnessEngine::RustHttp,
            small_body_cache_max_bytes: Some(DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES),
            gateway_db: Some("/tmp/replay.db".to_string()),
            gateway_import_car: None,
            bitswap_seed_car: None,
            bitswap_seed_connection_setup: None,
            kubo_repo: None,
            trace_output: None,
            trace_span_list: None,
            trace_summary: None,
            trace_requirements: TraceRequirementsReport::default(),
            summary: RepeatSummary::from_runs(&runs),
            runs,
        };

        let summary = OfflineReplaySummary::from_report(&report);

        assert_eq!(summary.missing_url_count, 2);
        assert_eq!(summary.offline_storage_bytes, Some(2048));
        assert_eq!(summary.missing_urls[0].kind, "root");
        assert_eq!(summary.missing_urls[0].case_id, "case");
        assert_eq!(
            summary.missing_urls[1].url,
            "http://127.0.0.1:8080/ipfs/root/app.js"
        );
        assert_eq!(summary.missing_urls[1].kind, "script");
        assert!(summary.offline_request_statuses.is_empty());
        assert!(summary.offline_network_phases.is_empty());
        assert!(summary.offline_cache_phases.is_empty());
        assert!(summary.offline_block_sources.is_empty());
        assert!(summary.offline_non_cache_block_sources.is_empty());
        assert!(summary.offline_trace_errors.is_empty());
        assert!(summary.offline_progress_phases.is_empty());
    }

    #[test]
    fn offline_replay_summary_extracts_cache_and_network_trace_phases() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-offline-trace-summary-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"request_done\",\"status\":206,\"elapsed_ms\":1}\n",
                "{\"phase\":\"delegated_provider_lookup\",\"provider_count\":1,\"elapsed_ms\":10}\n",
                "{\"phase\":\"http_provider_fetch\",\"ok\":true,\"bytes\":128,\"elapsed_ms\":20}\n",
                "{\"phase\":\"block_fetch_total\",\"source\":\"http_provider\",\"elapsed_ms\":21}\n",
                "{\"phase\":\"block_fetch_total\",\"source\":\"cache\",\"elapsed_ms\":0}\n",
                "{\"phase\":\"name_persistent_cache\",\"cache_hit\":true,\"elapsed_ms\":0}\n",
                "{\"phase\":\"block_store_get_range\",\"cache_hit\":true,\"elapsed_ms\":0}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"path_hits\":1,\"elapsed_ms\":0}\n",
                "{\"phase\":\"gateway_direct_body\",\"body_len\":4096,\"elapsed_ms\":0}\n",
            ),
        )
        .unwrap();

        let run = run_result(
            RunPhase::Measured,
            1,
            100,
            Some(40),
            Some(48),
            Some(0),
            Some(4096),
        );
        let runs = vec![run];
        let report = RunReport {
            gateway_url: None,
            generated_at_unix_seconds: 0,
            repeat: 1,
            warmup_runs: 0,
            fresh_gateway_per_run: false,
            asset_concurrency: 1,
            conditional_revalidate: false,
            run_timeout_secs: None,
            engine: HarnessEngine::RustHttp,
            small_body_cache_max_bytes: Some(DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES),
            gateway_db: Some("/tmp/replay.db".to_string()),
            gateway_import_car: None,
            bitswap_seed_car: None,
            bitswap_seed_connection_setup: None,
            kubo_repo: None,
            trace_output: Some(path.display().to_string()),
            trace_span_list: Some(false),
            trace_summary: Some(summarize_trace_output(&path).unwrap()),
            trace_requirements: TraceRequirementsReport::default(),
            summary: RepeatSummary::from_runs(&runs),
            runs,
        };

        let summary = OfflineReplaySummary::from_report(&report);

        assert_eq!(
            summary
                .offline_request_statuses
                .iter()
                .find(|count| count.value == "206")
                .map(|count| count.count),
            Some(1)
        );
        assert_eq!(
            trace_value_count(&summary.offline_network_phases, "delegated_provider_lookup"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_network_phases, "http_provider_fetch"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_cache_phases, "name_persistent_cache"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_cache_phases, "block_store_get_range"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_cache_phases, "unixfs_metadata_cache"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_cache_phases, "gateway_direct_body"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_block_sources, "http_provider"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_block_sources, "cache"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_non_cache_block_sources, "http_provider"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.offline_non_cache_block_sources, "cache"),
            0
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn trace_summary_counts_block_store_puts() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-block-store-put-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"block_store_get\",\"cid\":\"cid-a\",\"cache_hit\":false}\n",
                "{\"phase\":\"block_store_put\",\"cid\":\"cid-a\",\"source\":\"bitswap\",\"required\":true,\"ok\":true,\"bytes\":262144,\"elapsed_ms\":17}\n",
                "{\"phase\":\"block_store_put\",\"cid\":\"cid-b\",\"source\":\"bitswap_extra\",\"required\":false,\"ok\":false,\"bytes\":64,\"elapsed_ms\":3}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.block_store.events, 1);
        assert_eq!(summary.block_store.misses, 1);
        assert_eq!(summary.block_store.puts, 2);
        assert_eq!(summary.block_store.put_bytes, 262208);
        assert_eq!(summary.block_store.put_failures, 1);
        assert_eq!(summary.block_store.put_total_ms, 20);
        assert_eq!(summary.block_store.put_max_ms, 17);
        assert_eq!(trace_value_count(&summary.progress_phases, "streaming"), 2);
    }

    #[test]
    fn trace_summary_counts_block_range_batch_fetches() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-block-range-batch-fetch-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"block_range_batch_fetch\",\"cid\":\"cid-a\",\"source\":\"bitswap\",\"range_start\":10,\"range_end\":19,\"range_len\":10,\"range_count\":2,\"uncached_range_count\":2,\"elapsed_ms\":17}\n",
                "{\"phase\":\"block_range_batch_fetch\",\"cid\":\"cid-b\",\"source\":\"http_provider\",\"range_start\":20,\"range_end\":39,\"range_len\":20,\"range_count\":2,\"uncached_range_count\":2,\"elapsed_ms\":33}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let ranges = &summary.block_range_batch_fetches;

        assert_eq!(ranges.events, 2);
        assert_eq!(ranges.bytes, 30);
        assert_eq!(ranges.max_range_len, 20);
        assert_eq!(ranges.max_range_count, 2);
        assert_eq!(ranges.max_uncached_range_count, 2);
        assert_eq!(ranges.elapsed_ms.count, 2);
        assert_eq!(ranges.elapsed_ms.p50_ms, Some(17));
        assert_eq!(ranges.elapsed_ms.p95_ms, Some(33));
        assert_eq!(trace_value_count(&ranges.sources, "bitswap"), 1);
        assert_eq!(trace_value_count(&ranges.sources, "http_provider"), 1);
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_http_provider"),
            1
        );
    }

    #[test]
    fn trace_summary_counts_http_provider_fetches() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-http-provider-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"http_provider_race\",\"cid\":\"cid-a\",\"provider_count\":2,\"race_width\":2}\n",
                "{\"phase\":\"http_provider_race\",\"cid\":\"cid-b\",\"provider_count\":4,\"race_width\":2,\"scored_provider_count\":2}\n",
                "{\"phase\":\"http_provider_race\",\"cid\":\"cid-single-ok\",\"provider_count\":1,\"race_width\":2}\n",
                "{\"phase\":\"http_provider_race\",\"cid\":\"cid-single-fail\",\"provider_count\":1,\"race_width\":2}\n",
                "{\"phase\":\"http_provider_hedge\",\"cid\":\"cid-b\",\"provider\":\"https://provider-c.example\",\"timeout_ms\":250,\"pending_count\":2,\"remaining_provider_count\":1}\n",
                "{\"phase\":\"http_provider_self_hedge\",\"cid\":\"cid-single-ok\",\"provider\":\"https://provider-single.example\",\"timeout_ms\":350,\"provider_index\":0,\"attempt_index\":1,\"original_provider_rank\":1,\"reason\":\"slow_single_provider\"}\n",
                "{\"phase\":\"http_provider_candidate_cancelled\",\"cid\":\"cid-single-ok\",\"provider\":\"https://provider-single.example\",\"stage\":\"reading_body\",\"provider_index\":0,\"attempt_index\":1,\"original_provider_index\":0,\"provider_scored\":true,\"provider_score_ms\":250,\"elapsed_ms\":125}\n",
                "{\"phase\":\"http_provider_self_hedge_skip\",\"cid\":\"cid-single-skip\",\"provider\":\"https://provider-single.example\",\"reason\":\"provider_score_below_threshold\",\"provider_scored\":true,\"provider_score_ms\":75,\"min_score_ms\":200}\n",
                "{\"phase\":\"http_provider_bitswap_hedge\",\"cid\":\"cid-single-ok\",\"provider_count\":3,\"timeout_ms\":150,\"reason\":\"slow_single_http_provider\"}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_result\",\"cid\":\"cid-single-ok\",\"source\":\"bitswap\",\"provider_count\":3,\"elapsed_ms\":190}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_result\",\"cid\":\"cid-single-fail\",\"source\":\"http_provider\",\"provider_count\":2,\"elapsed_ms\":75}\n",
                "{\"phase\":\"http_provider_bitswap_hedge_skip\",\"cid\":\"cid-single-skip\",\"provider\":\"https://provider-single.example\",\"reason\":\"provider_unscored\",\"provider_scored\":false,\"min_score_ms\":250}\n",
                "{\"phase\":\"http_provider_race_result\",\"cid\":\"cid-a\",\"ok\":true,\"provider\":\"https://provider-a.example\",\"winner_provider_index\":1,\"winner_attempt_index\":0,\"winner_provider_rank\":2,\"winner_original_provider_rank\":3,\"winner_within_initial_width\":true,\"winner_provider_scored\":true,\"winner_provider_score_ms\":42,\"provider_count\":2,\"race_width\":2,\"attempted_provider_count\":2,\"failed_provider_count\":0,\"hedge_fired\":false,\"elapsed_ms\":25}\n",
                "{\"phase\":\"http_provider_race_result\",\"cid\":\"cid-b\",\"ok\":false,\"provider_count\":4,\"race_width\":2,\"attempted_provider_count\":4,\"failed_provider_count\":4,\"hedge_fired\":true,\"elapsed_ms\":300}\n",
                "{\"phase\":\"http_provider_race_result\",\"cid\":\"cid-single-ok\",\"ok\":true,\"provider\":\"https://provider-single.example\",\"winner_provider_index\":0,\"winner_attempt_index\":1,\"winner_self_hedge_attempt\":true,\"winner_provider_rank\":1,\"winner_original_provider_rank\":1,\"winner_within_initial_width\":true,\"winner_provider_scored\":false,\"single_provider_self_hedge\":true,\"provider_count\":1,\"race_width\":2,\"attempted_provider_count\":2,\"failed_provider_count\":0,\"hedge_fired\":true,\"elapsed_ms\":450}\n",
                "{\"phase\":\"http_provider_race_result\",\"cid\":\"cid-single-fail\",\"ok\":false,\"provider_count\":1,\"race_width\":2,\"attempted_provider_count\":1,\"failed_provider_count\":1,\"hedge_fired\":false,\"elapsed_ms\":900}\n",
                "{\"phase\":\"http_provider_fetch\",\"cid\":\"cid-a\",\"provider\":\"https://provider-a.example\",\"ok\":true,\"bytes\":128,\"response_bytes\":128,\"response_headers_elapsed_ms\":7,\"response_first_chunk_seen\":true,\"response_first_chunk_elapsed_ms\":9,\"response_body_elapsed_ms\":20,\"elapsed_ms\":25}\n",
                "{\"phase\":\"http_provider_fetch\",\"cid\":\"cid-b\",\"provider\":\"https://provider-b.example\",\"ok\":false,\"error\":\"core: cid hash mismatch for cid-b\",\"response_headers_elapsed_ms\":40,\"elapsed_ms\":40}\n",
                "{\"phase\":\"http_provider_fetch\",\"cid\":\"cid-c\",\"provider\":\"https://provider-b.example\",\"ok\":false,\"error\":\"request timed out\",\"response_headers_elapsed_ms\":60,\"elapsed_ms\":60}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.http_provider_races.events, 4);
        assert_eq!(summary.http_provider_races.provider_count_total, 8);
        assert_eq!(summary.http_provider_races.max_provider_count, 4);
        assert_eq!(summary.http_provider_races.max_race_width, 2);
        assert_eq!(summary.http_provider_races.single_provider_events, 2);
        assert_eq!(summary.http_provider_races.multi_provider_events, 2);
        assert_eq!(summary.http_provider_races.above_race_width_events, 1);
        assert_eq!(summary.http_provider_races.scored_events, 1);
        assert_eq!(summary.http_provider_races.scored_provider_count_total, 2);
        assert_eq!(summary.http_provider_races.max_scored_provider_count, 2);
        assert_eq!(summary.http_provider_races.hedges, 1);
        assert_eq!(summary.http_provider_races.self_hedges, 1);
        assert_eq!(summary.http_provider_races.max_self_hedge_timeout_ms, 350);
        assert_eq!(summary.http_provider_races.candidate_cancellations, 1);
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.candidate_cancelled_stages,
                "reading_body"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.candidate_cancelled_providers,
                "https://provider-single.example"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.candidate_cancelled_attempts,
                "1"
            ),
            1
        );
        assert_eq!(summary.http_provider_races.self_hedge_skips, 1);
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.self_hedge_skip_reasons,
                "provider_score_below_threshold"
            ),
            1
        );
        assert_eq!(summary.http_provider_races.bitswap_hedges, 1);
        assert_eq!(
            summary.http_provider_races.max_bitswap_hedge_timeout_ms,
            150
        );
        assert_eq!(summary.http_provider_races.bitswap_hedge_results, 2);
        assert_eq!(
            summary
                .http_provider_races
                .bitswap_hedge_result_elapsed_ms
                .p50_ms,
            Some(75)
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.bitswap_hedge_result_sources,
                "bitswap"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.bitswap_hedge_result_sources,
                "http_provider"
            ),
            1
        );
        assert_eq!(summary.http_provider_races.bitswap_hedge_skips, 1);
        assert_eq!(
            trace_value_count(
                &summary.http_provider_races.bitswap_hedge_skip_reasons,
                "provider_unscored"
            ),
            1
        );
        assert_eq!(summary.http_provider_races.max_hedge_pending_count, 2);
        assert_eq!(
            summary
                .http_provider_races
                .max_hedge_remaining_provider_count,
            1
        );
        assert_eq!(summary.http_provider_races.result_events, 4);
        assert_eq!(summary.http_provider_races.result_successes, 2);
        assert_eq!(summary.http_provider_races.result_failures, 2);
        assert_eq!(summary.http_provider_races.winner_initial_width_events, 2);
        assert_eq!(summary.http_provider_races.winner_late_events, 0);
        assert_eq!(summary.http_provider_races.winner_rank1_events, 1);
        assert_eq!(summary.http_provider_races.winner_rank2_events, 1);
        assert_eq!(summary.http_provider_races.winner_rank3_plus_events, 0);
        assert_eq!(summary.http_provider_races.max_winner_provider_rank, 2);
        assert_eq!(summary.http_provider_races.winner_original_rank1_events, 1);
        assert_eq!(summary.http_provider_races.winner_original_rank2_events, 0);
        assert_eq!(
            summary
                .http_provider_races
                .winner_original_rank3_plus_events,
            1
        );
        assert_eq!(
            summary
                .http_provider_races
                .max_winner_original_provider_rank,
            3
        );
        assert_eq!(summary.http_provider_races.max_winner_attempt_index, 1);
        assert_eq!(
            summary.http_provider_races.self_hedge_winner_initial_events,
            0
        );
        assert_eq!(
            summary.http_provider_races.self_hedge_winner_hedged_events,
            1
        );
        assert_eq!(
            summary.http_provider_races.self_hedge_winner_unknown_events,
            0
        );
        assert_eq!(
            summary.http_provider_races.self_hedge_fired_result_events,
            1
        );
        assert_eq!(
            summary
                .http_provider_races
                .self_hedge_fired_winner_initial_events,
            0
        );
        assert_eq!(
            summary
                .http_provider_races
                .self_hedge_fired_winner_hedged_events,
            1
        );
        assert_eq!(
            summary
                .http_provider_races
                .self_hedge_fired_winner_unknown_events,
            0
        );
        assert_eq!(summary.http_provider_races.winner_scored_events, 1);
        assert_eq!(
            summary.http_provider_races.winner_score_elapsed_ms.p50_ms,
            Some(42)
        );
        assert_eq!(summary.http_provider_races.max_attempted_provider_count, 4);
        assert_eq!(summary.http_provider_races.max_result_elapsed_ms, 900);
        assert_eq!(
            summary.http_provider_races.single_provider_result_successes,
            1
        );
        assert_eq!(
            summary.http_provider_races.single_provider_result_failures,
            1
        );
        assert_eq!(
            summary
                .http_provider_races
                .single_provider_success_elapsed_ms
                .count,
            1
        );
        assert_eq!(
            summary
                .http_provider_races
                .single_provider_success_elapsed_ms
                .p50_ms,
            Some(450)
        );
        assert_eq!(
            summary
                .http_provider_races
                .multi_provider_success_elapsed_ms
                .p50_ms,
            Some(25)
        );
        assert_eq!(summary.http_provider_races.single_provider_winners.len(), 1);
        assert_eq!(
            summary.http_provider_races.single_provider_winners[0].provider,
            "https://provider-single.example"
        );
        assert_eq!(
            summary.http_provider_races.single_provider_winners[0].events,
            1
        );
        assert_eq!(
            summary.http_provider_races.single_provider_winners[0].max_ms,
            450
        );
        assert_eq!(summary.http_provider_fetches.events, 3);
        assert_eq!(summary.http_provider_fetches.successes, 1);
        assert_eq!(summary.http_provider_fetches.failures, 2);
        assert_eq!(summary.http_provider_fetches.bytes, 128);
        assert_eq!(summary.http_provider_fetches.response_bytes, 128);
        assert_eq!(summary.http_provider_fetches.first_chunk_events, 1);
        assert_eq!(
            summary
                .http_provider_fetches
                .max_response_headers_elapsed_ms,
            60
        );
        assert_eq!(
            summary
                .http_provider_fetches
                .max_response_first_chunk_elapsed_ms,
            9
        );
        assert_eq!(
            summary.http_provider_fetches.max_response_body_elapsed_ms,
            20
        );
        assert_eq!(summary.http_provider_fetches.elapsed_ms.count, 3);
        assert_eq!(summary.http_provider_fetches.elapsed_ms.p50_ms, Some(40));
        assert_eq!(summary.http_provider_fetches.provider_milestones.len(), 2);
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0].provider,
            "https://provider-b.example"
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0].events,
            2
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0].failures,
            2
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0].max_response_headers_elapsed_ms,
            60
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0]
                .elapsed_ms
                .p50_ms,
            Some(40)
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[0]
                .response_headers_elapsed_ms
                .p50_ms,
            Some(40)
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[1].provider,
            "https://provider-a.example"
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[1].bytes,
            128
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[1].first_chunk_events,
            1
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[1]
                .response_first_chunk_elapsed_ms
                .p50_ms,
            Some(9)
        );
        assert_eq!(
            summary.http_provider_fetches.provider_milestones[1]
                .response_body_elapsed_ms
                .p50_ms,
            Some(20)
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_fetches.providers,
                "https://provider-b.example"
            ),
            2
        );
        assert_eq!(
            trace_value_count(
                &summary.http_provider_fetches.error_classes,
                "cid_hash_mismatch"
            ),
            1
        );
        assert_eq!(
            trace_value_count(&summary.http_provider_fetches.error_classes, "timeout"),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_http_provider"),
            17
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            2
        );
    }

    #[test]
    fn trace_summary_counts_pre_lookup_wait_outcomes() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-pre-lookup-wait-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_session_shortcut_pre_lookup\",\"cid\":\"cid-a\",\"outcome\":\"hit\",\"timeout_ms\":50,\"elapsed_ms\":41}\n",
                "{\"phase\":\"bitswap_session_shortcut_pre_lookup\",\"cid\":\"cid-b\",\"outcome\":\"miss\",\"timeout_ms\":50,\"elapsed_ms\":12}\n",
                "{\"phase\":\"bitswap_session_shortcut_pre_lookup\",\"cid\":\"cid-c\",\"outcome\":\"timeout\",\"timeout_ms\":50,\"elapsed_ms\":51}\n",
                "{\"phase\":\"bitswap_session_shortcut_pre_lookup\",\"cid\":\"cid-d\",\"outcome\":\"timeout\",\"timeout_ms\":75}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.bitswap_session.session_shortcut_pre_lookup_waits, 4);
        assert_eq!(summary.bitswap_session.session_shortcut_pre_lookup_hits, 1);
        assert_eq!(
            summary.bitswap_session.session_shortcut_pre_lookup_misses,
            1
        );
        assert_eq!(
            summary.bitswap_session.session_shortcut_pre_lookup_timeouts,
            2
        );
        assert_eq!(
            summary.bitswap_session.session_shortcut_pre_lookup_max_ms,
            75
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_pre_lookup_elapsed_ms
                .count,
            4
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_pre_lookup_elapsed_ms
                .p50_ms,
            Some(41)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_pre_lookup_hit_elapsed_ms
                .p50_ms,
            Some(41)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_pre_lookup_miss_elapsed_ms
                .p50_ms,
            Some(12)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_pre_lookup_timeout_elapsed_ms
                .p50_ms,
            Some(51)
        );
        assert_eq!(
            trace_value_count(
                &sorted_trace_counts(
                    summary
                        .bitswap_session
                        .session_shortcut_pre_lookup_budgets
                        .clone()
                ),
                "50"
            ),
            3
        );
        assert_eq!(
            trace_value_count(
                &sorted_trace_counts(
                    summary
                        .bitswap_session
                        .session_shortcut_pre_lookup_budgets
                        .clone()
                ),
                "75"
            ),
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            4
        );
    }

    #[test]
    fn trace_summary_counts_late_session_peer_waits() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-late-session-peer-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_session_late_peer_wait\",\"cid\":\"cid-a\",\"outcome\":\"hit\",\"peer_count\":1,\"timeout_ms\":2000,\"elapsed_ms\":283}\n",
                "{\"phase\":\"bitswap_session_late_peer_wait\",\"cid\":\"cid-b\",\"outcome\":\"miss\",\"timeout_ms\":2000,\"elapsed_ms\":2000}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.bitswap_session.session_late_peer_waits, 2);
        assert_eq!(summary.bitswap_session.session_late_peer_hits, 1);
        assert_eq!(summary.bitswap_session.session_late_peer_misses, 1);
        assert_eq!(summary.bitswap_session.session_late_peer_max_ms, 2000);
        assert_eq!(
            summary.bitswap_session.session_late_peer_elapsed_ms.p50_ms,
            Some(283)
        );
        assert_eq!(
            summary.bitswap_session.session_late_peer_elapsed_ms.p95_ms,
            Some(2000)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_late_peer_hit_elapsed_ms
                .p50_ms,
            Some(283)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_late_peer_miss_elapsed_ms
                .p50_ms,
            Some(2000)
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            2
        );
    }

    #[test]
    fn trace_summary_counts_post_lookup_wait_outcomes() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-post-lookup-wait-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid-a\",\"outcome\":\"hit\",\"timeout_ms\":250,\"elapsed_ms\":81,\"provider_count\":12,\"http_provider_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid-b\",\"outcome\":\"miss\",\"timeout_ms\":100,\"elapsed_ms\":24,\"provider_count\":4,\"http_provider_count\":3}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid-c\",\"outcome\":\"timeout\",\"timeout_ms\":250,\"elapsed_ms\":251,\"provider_count\":8,\"http_provider_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid-d\",\"timeout_ms\":100,\"provider_count\":2,\"http_provider_count\":0}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_waits,
            4
        );
        assert_eq!(summary.bitswap_session.session_shortcut_post_lookup_hits, 1);
        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_misses,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeouts,
            2
        );
        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_errors,
            0
        );
        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_max_ms,
            251
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_elapsed_ms
                .p50_ms,
            Some(81)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_hit_elapsed_ms
                .p50_ms,
            Some(81)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeout_elapsed_ms
                .p50_ms,
            Some(251)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_single_http_elapsed_ms
                .p95_ms,
            Some(251)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_single_http_hit_elapsed_ms
                .p50_ms,
            Some(81)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_single_http_timeout_elapsed_ms
                .p50_ms,
            Some(251)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_budgets
                .get("100"),
            Some(&2)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_budgets
                .get("250"),
            Some(&2)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeout_budgets
                .get("100"),
            Some(&1)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_timeout_budgets
                .get("250"),
            Some(&1)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_http_provider_counts
                .get("1"),
            Some(&2)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_http_provider_counts
                .get("3"),
            Some(&1)
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            4
        );
    }

    #[test]
    fn trace_summary_counts_post_lookup_race_outcomes() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "mobile-web-harness-trace-post-lookup-race-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_race\",\"cid\":\"cid-a\",\"outcome\":\"provider_won\",\"source\":\"bitswap\",\"timeout_ms\":125,\"elapsed_ms\":460,\"provider_result_elapsed_ms\":460,\"provider_count\":64,\"http_provider_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_race\",\"cid\":\"cid-b\",\"outcome\":\"provider_won\",\"source\":\"http_provider\",\"timeout_ms\":125,\"elapsed_ms\":120,\"provider_result_elapsed_ms\":120,\"provider_count\":12,\"http_provider_count\":2}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_race\",\"cid\":\"cid-c\",\"outcome\":\"bitswap_won\",\"timeout_ms\":100,\"elapsed_ms\":80,\"provider_count\":8,\"http_provider_count\":0}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_race\",\"cid\":\"cid-d\",\"outcome\":\"provider_error\",\"timeout_ms\":125,\"elapsed_ms\":333,\"provider_count\":64,\"http_provider_count\":1}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            summary.bitswap_session.session_shortcut_post_lookup_races,
            4
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_provider_wins,
            2
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_bitswap_wins,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_errors,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_provider_bitswap_wins,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_single_http_provider_bitswap_wins,
            1
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_elapsed_ms
                .p50_ms,
            Some(120)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_provider_result_elapsed_ms
                .p95_ms,
            Some(460)
        );
        assert_eq!(
            summary
                .bitswap_session
                .session_shortcut_post_lookup_race_single_http_provider_bitswap_elapsed_ms
                .p50_ms,
            Some(460)
        );
        assert_eq!(
            trace_value_count(
                &summary
                    .bitswap_session
                    .session_shortcut_post_lookup_race_outcomes,
                "provider_won"
            ),
            2
        );
        assert_eq!(
            trace_value_count(
                &summary
                    .bitswap_session
                    .session_shortcut_post_lookup_race_sources,
                "bitswap"
            ),
            1
        );
        assert_eq!(
            trace_value_count(
                &summary
                    .bitswap_session
                    .session_shortcut_post_lookup_race_http_provider_counts,
                "1"
            ),
            2
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            4
        );
    }

    #[test]
    fn resolved_ipfs_replay_rewrites_ipns_paths_from_trace_resolutions() {
        let mut resolutions = BTreeMap::new();
        resolutions.insert(
            "ipfs.tech".to_string(),
            "/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq".to_string(),
        );
        let corpus = Corpus {
            entries: vec![
                corpus_entry("root", "/ipns/ipfs.tech/"),
                corpus_entry("asset", "/ipns/ipfs.tech/_nuxt/app.js?cache=1"),
                corpus_entry("unresolved", "/ipns/example.test/"),
                corpus_entry("immutable", "/ipfs/bafyroot/index.html"),
            ],
        };

        let (rewritten, rewrites) =
            rewrite_corpus_for_resolved_ipfs_replay(&corpus, &resolutions, &[]);

        assert_eq!(rewrites.len(), 2);
        assert_eq!(rewrites[0].case_id, "root");
        assert_eq!(rewrites[0].name, "ipfs.tech");
        assert_eq!(
            rewritten.entries[0].path,
            "/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/"
        );
        assert_eq!(
            rewritten.entries[1].path,
            "/ipfs/bafybeierpueybjyyjypd5jfmoellbclf3bcgcrj2oaktwya2o5dlilupaq/_nuxt/app.js?cache=1"
        );
        assert_eq!(rewritten.entries[2].path, "/ipns/example.test/");
        assert_eq!(rewritten.entries[3].path, "/ipfs/bafyroot/index.html");
    }

    #[test]
    fn resolved_ipfs_replay_rewrites_only_selected_cases() {
        let mut resolutions = BTreeMap::new();
        resolutions.insert("ipfs.tech".to_string(), "/ipfs/bafyroot".to_string());
        let corpus = Corpus {
            entries: vec![
                corpus_entry("root", "/ipns/ipfs.tech/"),
                corpus_entry("asset", "/ipns/ipfs.tech/app.js"),
            ],
        };
        let cases = vec!["asset".to_string()];

        let (rewritten, rewrites) =
            rewrite_corpus_for_resolved_ipfs_replay(&corpus, &resolutions, &cases);

        assert_eq!(rewrites.len(), 1);
        assert_eq!(rewrites[0].case_id, "asset");
        assert_eq!(rewritten.entries[0].path, "/ipns/ipfs.tech/");
        assert_eq!(rewritten.entries[1].path, "/ipfs/bafyroot/app.js");
    }

    #[test]
    fn trace_name_resolution_parser_keeps_successful_ipfs_targets() {
        let path = unique_temp_path("trace-name-resolution-parser.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"phase\":\"name_resolve\",\"name\":\"site.test\",\"ok\":true,\"resolved_target\":\"/ipfs/bafyroot\"}\n",
                "{\"phase\":\"name_resolve\",\"name\":\"nested.test\",\"ok\":true,\"resolved_target\":\"/ipns/other.test\"}\n",
                "{\"phase\":\"name_resolve\",\"name\":\"bad.test\",\"ok\":false,\"error\":\"not found\"}\n"
            ),
        )
        .unwrap();

        let resolutions = successful_name_resolutions_from_trace(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(resolutions.len(), 1);
        assert_eq!(
            resolutions.get("site.test").map(String::as_str),
            Some("/ipfs/bafyroot")
        );
        assert!(!resolutions.contains_key("nested.test"));
        assert!(!resolutions.contains_key("bad.test"));
    }

    fn trace_value_count(counts: &[TraceValueCount], value: &str) -> usize {
        counts
            .iter()
            .find(|count| count.value == value)
            .map(|count| count.count)
            .unwrap_or_default()
    }

    fn run_report(
        engine: HarnessEngine,
        bitswap_seed_connection_setup: Option<BitswapSeedConnectionSetup>,
        runs: Vec<RunResult>,
    ) -> RunReport {
        let summary = RepeatSummary::from_runs(&runs);
        RunReport {
            gateway_url: None,
            generated_at_unix_seconds: 0,
            repeat: summary.measured_runs,
            warmup_runs: 0,
            fresh_gateway_per_run: true,
            asset_concurrency: 1,
            conditional_revalidate: false,
            run_timeout_secs: None,
            engine,
            small_body_cache_max_bytes: engine
                .is_rust()
                .then_some(DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES),
            gateway_db: None,
            gateway_import_car: None,
            bitswap_seed_car: Some("/tmp/mobile-fixture.car".to_string()),
            bitswap_seed_connection_setup,
            kubo_repo: None,
            trace_output: None,
            trace_span_list: None,
            trace_summary: None,
            trace_requirements: TraceRequirementsReport::default(),
            summary,
            runs,
        }
    }

    fn run_result(
        phase: RunPhase,
        run_index: usize,
        elapsed_ms: u128,
        gateway_rss_kib: Option<u64>,
        gateway_fd_count: Option<usize>,
        gateway_child_process_count: Option<usize>,
        gateway_storage_bytes: Option<u64>,
    ) -> RunResult {
        RunResult {
            phase,
            run_index,
            gateway_url: "http://127.0.0.1:8080".to_string(),
            elapsed_ms,
            gateway_rss_kib,
            gateway_fd_count,
            gateway_child_process_count,
            gateway_storage_bytes,
            gateway_storage_path: None,
            bitswap_seed_connect_elapsed_ms: None,
            kubo_bitswap_stats: None,
            native_ffi: None,
            passed: true,
            results: vec![CaseResult {
                id: "case".to_string(),
                description: None,
                method: "GET".to_string(),
                url: "http://127.0.0.1:8080/ipfs/root".to_string(),
                status: Some(200),
                content_type: Some("text/plain".to_string()),
                content_range: None,
                content_length: Some(5),
                accept_ranges: Some("bytes".to_string()),
                etag: None,
                cache_control: None,
                body_bytes: 5,
                ttfb_ms: elapsed_ms / 2,
                total_ms: elapsed_ms,
                stream: FetchStreamMetrics::default(),
                body_preview: "hello".to_string(),
                revalidation: None,
                asset_summary: None,
                assets: Vec::new(),
                passed: true,
                failures: Vec::new(),
            }],
        }
    }

    fn script_asset_result(ttfb_ms: u128, total_ms: u128) -> AssetResult {
        AssetResult {
            kind: AssetKind::Script,
            source: "app.js".to_string(),
            url: "http://127.0.0.1:8080/ipfs/root/app.js".to_string(),
            status: Some(200),
            content_type: Some("text/javascript".to_string()),
            content_range: None,
            content_length: Some(128),
            accept_ranges: None,
            etag: None,
            cache_control: None,
            body_bytes: 128,
            ttfb_ms,
            total_ms,
            stream: FetchStreamMetrics::default(),
            body_preview: String::new(),
            revalidation: None,
            passed: true,
            failures: Vec::new(),
        }
    }

    fn script_asset_result_for_path(
        ttfb_ms: u128,
        total_ms: u128,
        port: u16,
        path: &str,
    ) -> AssetResult {
        let mut asset = script_asset_result(ttfb_ms, total_ms);
        asset.source = path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or(path)
            .to_string();
        asset.url = format!("http://127.0.0.1:{port}{path}");
        asset
    }

    struct CountingBlockProvider {
        cid: cid::Cid,
        data: Vec<u8>,
        calls: Arc<AtomicUsize>,
    }

    impl BlockProvider for CountingBlockProvider {
        fn get_block(&self, cid: &cid::Cid) -> CoreResult<Option<Block>> {
            if cid != &self.cid {
                return Err(CoreError::Storage(format!("unexpected cid {cid}")));
            }
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Some(Block::unchecked(*cid, self.data.clone())))
        }
    }

    fn corpus_entry(id: &str, path: &str) -> CorpusEntry {
        CorpusEntry {
            id: id.to_string(),
            description: None,
            path: path.to_string(),
            default_enabled: None,
            method: None,
            range: None,
            crawl: None,
            expect_status: None,
            expect_content_type_prefix: None,
            expect_content_range_prefix: None,
            expect_content_length: None,
            expect_accept_ranges: None,
            expect_etag_prefix: None,
            expect_cache_control: None,
            expect_body_contains: None,
            expect_body_bytes: None,
            expect_body_sha256: None,
            min_bytes: None,
            max_ttfb_ms: None,
        }
    }
}
