use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use reqwest::header::{CONTENT_RANGE, CONTENT_TYPE, RANGE};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};

const DEFAULT_CORPUS: &str = "tools/mobile-web-harness/corpus/mobile-web.json";
const DEFAULT_KUBO_BIN: &str = "target/tools/kubo/kubo/ipfs";
const DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS: usize = 8;
const DEFAULT_ASSET_CONCURRENCY: usize = 6;
const MAX_TRACE_SLOW_EVENTS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum HarnessEngine {
    Rust,
    Kubo,
}

impl HarnessEngine {
    fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Kubo => "kubo",
        }
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
    #[arg(long, value_enum, default_value_t = HarnessEngine::Rust)]
    engine: HarnessEngine,
    /// Standalone gateway binary to spawn when --gateway-url is not provided.
    #[arg(long, env = "FREEDOM_IPFS_GATEWAY_BIN")]
    gateway_bin: Option<PathBuf>,
    /// SQLite cache DB path for spawned gateways; useful for fresh-process warm-store runs.
    #[arg(long)]
    gateway_db: Option<PathBuf>,
    /// Kubo ipfs binary to spawn when --engine kubo is selected.
    #[arg(long, env = "KUBO_BIN", default_value = DEFAULT_KUBO_BIN)]
    kubo_bin: PathBuf,
    /// Kubo repo path. Omit for an isolated temporary repo per spawned Kubo daemon.
    #[arg(long, env = "IPFS_PATH")]
    kubo_repo: Option<PathBuf>,
    /// Run paired Rust and Kubo harness passes with the same corpus/options.
    #[arg(long)]
    compare_kubo: bool,
    /// Optional paired Rust-vs-Kubo JSON comparison report output path.
    #[arg(long)]
    comparison_output: Option<PathBuf>,
    /// JSON corpus file.
    #[arg(long, default_value = DEFAULT_CORPUS)]
    corpus: PathBuf,
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
    /// Gateway routing mode when spawning a gateway.
    #[arg(long, default_value = "auto")]
    routing_mode: String,
    /// DHT query timeout when spawning a gateway.
    #[arg(long, default_value_t = 10)]
    dht_query_timeout_secs: u64,
    /// Max DHT providers when spawning a gateway.
    #[arg(long, default_value_t = 4)]
    dht_max_providers: usize,
    /// Concurrent subresource fetches for page crawls.
    #[arg(long, default_value_t = DEFAULT_ASSET_CONCURRENCY)]
    asset_concurrency: usize,
    /// Gateway JSONL trace output path when spawning a gateway; parsed into the report.
    #[arg(long)]
    trace_output: Option<PathBuf>,
    /// Optional tracing filter for spawned gateway trace output.
    #[arg(long)]
    trace_filter: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let corpus = Corpus::read(&args.corpus)?;

    if args.compare_kubo {
        let report = run_comparison(&args, &corpus).await?;
        print_comparison_summary(&report);
        if let Some(output) = args.comparison_output.or(args.output) {
            let json = serde_json::to_string_pretty(&report)?;
            std::fs::write(&output, json).with_context(|| format!("write {}", output.display()))?;
            eprintln!("wrote comparison report to {}", output.display());
        }
        if report.rust.summary.fail_count > 0 || report.kubo.summary.fail_count > 0 {
            bail!("mobile web comparison found failures");
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

    if report.summary.fail_count > 0 {
        bail!("mobile web harness found failures");
    }
    Ok(())
}

async fn run_comparison(args: &Args, corpus: &Corpus) -> Result<ComparisonReport> {
    if args.gateway_url.is_some() {
        bail!("--compare-kubo cannot be used with --gateway-url");
    }
    let mut rust_args = args.clone();
    rust_args.engine = HarnessEngine::Rust;
    rust_args.compare_kubo = false;
    rust_args.comparison_output = None;

    let mut kubo_args = args.clone();
    kubo_args.engine = HarnessEngine::Kubo;
    kubo_args.compare_kubo = false;
    kubo_args.comparison_output = None;
    kubo_args.gateway_db = None;
    kubo_args.trace_output = None;
    kubo_args.trace_filter = None;

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

async fn run_harness(args: &Args, corpus: &Corpus) -> Result<RunReport> {
    if args.gateway_url.is_some() && args.fresh_gateway_per_run {
        bail!("--fresh-gateway-per-run cannot be used with --gateway-url");
    }
    if args.gateway_url.is_some() && args.gateway_db.is_some() {
        bail!("--gateway-db can only be used when the harness spawns the gateway");
    }
    if args.engine == HarnessEngine::Kubo && args.gateway_db.is_some() {
        bail!("--gateway-db only applies to --engine rust");
    }
    if args.engine == HarnessEngine::Kubo && args.trace_output.is_some() {
        bail!("--trace-output is only supported for --engine rust");
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

    let timeout = Duration::from_secs(args.timeout_secs);
    let run_timeout =
        (args.run_timeout_secs > 0).then(|| Duration::from_secs(args.run_timeout_secs));
    let measured_runs = args.repeat.max(1);
    let total_runs = args.warmup_runs + measured_runs;
    let mut persistent_gateway = None;
    let persistent_gateway_url = if args.fresh_gateway_per_run {
        None
    } else if let Some(url) = args.gateway_url.as_deref() {
        Some(normalize_gateway_url(url))
    } else {
        let gateway = SpawnedGateway::start(args).await?;
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
        let gateway_url = if let Some(url) = &persistent_gateway_url {
            url.clone()
        } else {
            let gateway = SpawnedGateway::start(args).await?;
            let url = gateway.url.clone();
            run_gateway = Some(gateway);
            url
        };

        let started = Instant::now();
        let run = run_corpus_once(
            &gateway_url,
            corpus,
            timeout,
            args.asset_concurrency,
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
            passed,
            results,
        });

        if let Some(mut gateway) = run_gateway {
            gateway.stop().await;
        }
    }

    if let Some(mut gateway) = persistent_gateway {
        gateway.stop().await;
    }

    let summary = RepeatSummary::from_runs(&runs);
    let trace_summary = args
        .trace_output
        .as_ref()
        .map(summarize_trace_output)
        .transpose()?;
    Ok(RunReport {
        gateway_url: persistent_gateway_url,
        generated_at_unix_seconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        repeat: measured_runs,
        warmup_runs: args.warmup_runs,
        fresh_gateway_per_run: args.fresh_gateway_per_run,
        asset_concurrency: args.asset_concurrency,
        run_timeout_secs: (args.run_timeout_secs > 0).then_some(args.run_timeout_secs),
        engine: args.engine,
        gateway_db: args
            .gateway_db
            .as_ref()
            .map(|path| path.display().to_string()),
        kubo_repo: args
            .kubo_repo
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_output: args
            .trace_output
            .as_ref()
            .map(|path| path.display().to_string()),
        trace_summary,
        summary,
        runs,
    })
}

async fn run_corpus_once(
    gateway_url: &str,
    corpus: &Corpus,
    timeout: Duration,
    asset_concurrency: usize,
    cases: &[String],
) -> Result<Vec<CaseResult>> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("build reqwest client")?;
    let mut results = Vec::new();
    for entry in &corpus.entries {
        if !cases.is_empty() && !cases.iter().any(|case| case == &entry.id) {
            continue;
        }
        results.push(run_case(&client, gateway_url, entry, asset_concurrency).await);
    }
    if results.is_empty() {
        bail!("no corpus entries matched the requested case filters");
    }
    Ok(results)
}

fn run_timeout_failure_results(
    gateway_url: &str,
    corpus: &Corpus,
    cases: &[String],
    timeout: Duration,
) -> Result<Vec<CaseResult>> {
    let mut results = Vec::new();
    for entry in &corpus.entries {
        if !cases.is_empty() && !cases.iter().any(|case| case == &entry.id) {
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

async fn run_case(
    client: &reqwest::Client,
    gateway_url: &str,
    entry: &CorpusEntry,
    asset_concurrency: usize,
) -> CaseResult {
    let url = format!("{}{}", gateway_url.trim_end_matches('/'), entry.path);
    let method = entry.method.as_deref().unwrap_or("GET");
    let response = match fetch_response(client, &url, method, entry.range.as_deref()).await {
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
    if let Some(expected) = &entry.expect_body_contains {
        let text = String::from_utf8_lossy(&response.body);
        if !text.contains(expected) {
            failures.push(format!("body did not contain {expected:?}"));
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

    let mut asset_summary = None;
    let mut assets = Vec::new();
    if let Some(crawl) = &entry.crawl {
        let (summary, mut crawled_assets, crawl_failures) =
            run_page_crawl(client, &url, &response.body, crawl, asset_concurrency).await;
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
        body_bytes: response.body.len(),
        ttfb_ms: response.ttfb_ms,
        total_ms: response.total_ms,
        body_preview,
        asset_summary,
        assets,
        passed: failures.is_empty(),
        failures,
    }
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
    if let Some(kubo_repo) = &report.kubo_repo {
        println!("kubo_repo: {kubo_repo}");
    }
    println!(
        "runs: measured={} warmup={} fresh_gateway_per_run={} asset_concurrency={}",
        report.repeat, report.warmup_runs, report.fresh_gateway_per_run, report.asset_concurrency
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

    for case in &report.summary.cases {
        println!(
            "case {}: passed={} failed={} pass_rate={:.1}% root_ttfb={} root_total={} asset_ttfb={} asset_total={}",
            case.id,
            case.pass_count,
            case.fail_count,
            case.pass_rate * 100.0,
            case.root_ttfb_ms,
            case.root_total_ms,
            case.asset_ttfb_ms,
            case.asset_total_ms
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
        if !trace.block_sources.is_empty() {
            println!(
                "  block sources: {}",
                format_trace_counts(&trace.block_sources)
            );
        }
        if !trace.request_statuses.is_empty() || trace.gateway_limiter_denials > 0 {
            println!(
                "  gateway responses: statuses={} limiter_denials={}",
                format_trace_counts(&trace.request_statuses),
                trace.gateway_limiter_denials
            );
        }
        if trace.unixfs_metadata_cache.events > 0 {
            let cache = &trace.unixfs_metadata_cache;
            println!(
                "  unixfs metadata cache: events={} hits={} misses={} inserts={} evictions={} oversized_skips={} max_len={} capacity={}",
                cache.events,
                cache.hits,
                cache.misses,
                cache.inserts,
                cache.evictions,
                cache.oversized_skips,
                cache.max_len,
                cache.max_capacity
            );
        }
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
        if !trace.bitswap_peer_fetches.is_empty() {
            println!("  bitswap peer fetches:");
            for peer in trace.bitswap_peer_fetches.iter().take(8) {
                let transports = format_trace_counts(&peer.transports);
                println!(
                    "    {}: count={} total={}ms max={}ms bytes={} transports={}",
                    peer.peer, peer.count, peer.total_ms, peer.max_ms, peer.bytes, transports
                );
            }
        }
        if trace.bitswap_session.has_events() {
            let session = &trace.bitswap_session;
            println!(
                "  bitswap session: fetches={} with_trusted={} trusted_successes={} untrusted_successes={} trusted_failures={} request_timeouts_with_trusted={} shortcut_starts={} shortcut_post_lookup_waits={} shortcut_attempts={} shortcut_hits={} shortcut_misses={}",
                session.fetches,
                session.with_trusted_peers,
                session.trusted_successes,
                session.untrusted_successes,
                session.trusted_failures,
                session.request_timeouts_with_trusted,
                session.session_shortcut_starts,
                session.session_shortcut_post_lookup_waits,
                session.session_shortcut_attempts,
                session.session_shortcut_hits,
                session.session_shortcut_misses
            );
        }
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
                "  bitswap provider quality: events={} provider_addrs={} expanded={} supported={} rejected={} id_only={} no_supported={} relay={} webtransport={} webrtc={} certhash={} other_transport={} missing_peer={} unparsable={}",
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
                quality.unparsable_addr_count
            );
        }
        if !trace.bitswap_connection_transports.is_empty() {
            println!(
                "  bitswap connection transports: {}",
                format_trace_counts(&trace.bitswap_connection_transports)
            );
        }
        if !trace.bitswap_dial_rejected_transports.is_empty() {
            println!(
                "  bitswap rejected dial transports: {}",
                format_trace_counts(&trace.bitswap_dial_rejected_transports)
            );
        }
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
        if !trace.slow_cids.is_empty() {
            println!("  slow cids:");
            for cid in trace.slow_cids.iter().take(8) {
                let phases = format_trace_counts(&cid.phases);
                let paths = format_trace_counts(&cid.paths);
                println!(
                    "    {}: count={} total={}ms max={}ms phases={} paths={}",
                    cid.cid, cid.count, cid.total_ms, cid.max_ms, phases, paths
                );
            }
        }
        if !trace.slow_requests.is_empty() {
            println!("  slow requests:");
            for request in trace.slow_requests.iter().take(8) {
                let phases = format_trace_counts(&request.phases);
                let cids = format_trace_counts(&request.cids);
                let status = request.status.as_deref().unwrap_or("unknown");
                println!(
                    "    {}: {}ms status={} request_id={} events={} max_event={}ms phases={} cids={}",
                    request.path,
                    request.elapsed_ms,
                    status,
                    request.request_id,
                    request.event_count,
                    request.max_event_ms,
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
                .map(|rss| format!(" rss={}KiB", rss))
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
                .map(|bytes| format!(" storage={}B", bytes))
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
            "  asset_ttfb: rust_p50={} kubo_p50={} p50_ratio={} rust_p95={} kubo_p95={} p95_ratio={}",
            display_option_ms(case.rust_asset_ttfb_p50_ms),
            display_option_ms(case.kubo_asset_ttfb_p50_ms),
            display_option_f64(case.asset_ttfb_p50_ratio),
            display_option_ms(case.rust_asset_ttfb_p95_ms),
            display_option_ms(case.kubo_asset_ttfb_p95_ms),
            display_option_f64(case.asset_ttfb_p95_ratio)
        );
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

fn print_case_result(result: &CaseResult) {
    let mark = if result.passed { "PASS" } else { "FAIL" };
    println!(
        "{mark} {:32} status={} type={} bytes={} ttfb={}ms total={}ms",
        result.id,
        result
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.content_type.as_deref().unwrap_or("-"),
        result.body_bytes,
        result.ttfb_ms,
        result.total_ms
    );
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
                "    - {} {} status={} type={} bytes={} total={}ms",
                asset.kind,
                asset.url,
                asset
                    .status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                asset.content_type.as_deref().unwrap_or("-"),
                asset.body_bytes,
                asset.total_ms
            );
            for failure in &asset.failures {
                println!("      - {failure}");
            }
        }
    }
}

async fn fetch_response(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    range: Option<&str>,
) -> std::result::Result<FetchResponse, String> {
    let started = Instant::now();
    let response = match method {
        "GET" => {
            let mut request = client.get(url);
            if let Some(range) = range {
                request = request.header(RANGE, range);
            }
            request.send().await
        }
        "HEAD" => client.head(url).send().await,
        other => {
            return Err(format!(
                "unsupported method {other}; only GET and HEAD are supported"
            ))
        }
    }
    .map_err(|err| err.to_string())?;

    let ttfb_ms = started.elapsed().as_millis();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .bytes()
        .await
        .map_err(|err| format!("body error: {err}"))?
        .to_vec();
    let total_ms = started.elapsed().as_millis();

    Ok(FetchResponse {
        status,
        content_type,
        content_range,
        body,
        ttfb_ms,
        total_ms,
    })
}

async fn run_page_crawl(
    client: &reqwest::Client,
    page_url: &str,
    page_body: &[u8],
    config: &CrawlConfig,
    asset_concurrency: usize,
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
    client: &reqwest::Client,
    assets: Vec<DiscoveredAsset>,
    concurrency: usize,
    max_bytes: usize,
) -> Vec<FetchedAsset> {
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = JoinSet::new();
    for asset in assets {
        let client = client.clone();
        let semaphore = semaphore.clone();
        tasks.spawn(async move {
            let _permit = semaphore.acquire_owned().await.ok();
            fetch_asset(&client, asset, max_bytes).await
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
    client: &reqwest::Client,
    asset: DiscoveredAsset,
    max_bytes: usize,
) -> FetchedAsset {
    let range = range_for_kind(asset.kind);
    let started_url = asset.url.to_string();
    let response = fetch_response(client, &started_url, "GET", range).await;
    let mut failures = Vec::new();
    let mut result = AssetResult {
        kind: asset.kind,
        source: asset.source,
        url: started_url,
        status: None,
        content_type: None,
        content_range: None,
        body_bytes: 0,
        ttfb_ms: 0,
        total_ms: 0,
        body_preview: String::new(),
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
    result.body_bytes = response.body.len();
    result.ttfb_ms = response.ttfb_ms;
    result.total_ms = response.total_ms;
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
struct SpawnedGateway {
    child: Child,
    url: String,
    storage_path: Option<PathBuf>,
    remove_storage_on_stop: bool,
    stdout_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
}

impl SpawnedGateway {
    async fn start(args: &Args) -> Result<Self> {
        match args.engine {
            HarnessEngine::Rust => Self::start_rust(args).await,
            HarnessEngine::Kubo => Self::start_kubo(args).await,
        }
    }

    async fn start_rust(args: &Args) -> Result<Self> {
        let bin = args
            .gateway_bin
            .clone()
            .unwrap_or_else(|| PathBuf::from("target/debug/freedom-ipfs-gateway"));
        let mut command = Command::new(&bin);
        command
            .kill_on_drop(true)
            .arg("--online")
            .arg("--routing-mode")
            .arg(&args.routing_mode)
            .arg("--max-concurrent-requests")
            .arg(args.max_concurrent_requests.to_string())
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
        if let Some(gateway_db) = &args.gateway_db {
            command.arg("--db").arg(gateway_db);
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
                });
            }
            eprintln!("gateway: {line}");
        }
    }

    async fn start_kubo(args: &Args) -> Result<Self> {
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
        let url = format!("http://127.0.0.1:{gateway_port}");
        eprintln!("kubo gateway listening on {url}");

        Ok(Self {
            child,
            url,
            storage_path: Some(repo),
            remove_storage_on_stop,
            stdout_task,
            stderr_task,
        })
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
            .and_then(|path| path_size_bytes(path).ok())
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

#[cfg(not(target_os = "linux"))]
fn child_process_count(_parent_pid: u32) -> Option<usize> {
    None
}

fn parse_proc_stat_ppid(stat: &str) -> Option<u32> {
    let after_name = stat.rsplit_once(") ")?;
    let mut fields = after_name.1.split_whitespace();
    let _state = fields.next()?;
    fields.next()?.parse().ok()
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
    std::env::temp_dir().join(format!("{prefix}-{}-{millis}", std::process::id()))
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

fn path_size_bytes(path: &PathBuf) -> std::io::Result<u64> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(path_size_bytes(&entry.path())?);
    }
    Ok(total)
}

#[derive(Debug, Deserialize)]
struct Corpus {
    entries: Vec<CorpusEntry>,
}

impl Corpus {
    fn read(path: &PathBuf) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
    }
}

#[derive(Debug, Deserialize)]
struct CorpusEntry {
    id: String,
    description: Option<String>,
    path: String,
    method: Option<String>,
    range: Option<String>,
    crawl: Option<CrawlConfig>,
    expect_status: Option<u16>,
    expect_content_type_prefix: Option<String>,
    expect_content_range_prefix: Option<String>,
    expect_body_contains: Option<String>,
    min_bytes: Option<usize>,
    max_ttfb_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
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
    run_timeout_secs: Option<u64>,
    engine: HarnessEngine,
    gateway_db: Option<String>,
    kubo_repo: Option<String>,
    trace_output: Option<String>,
    trace_summary: Option<TraceSummary>,
    summary: RepeatSummary,
    runs: Vec<RunResult>,
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
    passed: bool,
    results: Vec<CaseResult>,
}

#[derive(Debug, Serialize)]
struct ComparisonReport {
    generated_at_unix_seconds: u64,
    rust: RunReport,
    kubo: RunReport,
    cases: Vec<ComparisonCase>,
}

#[derive(Debug, Serialize)]
struct ComparisonCase {
    id: String,
    rust_pass_rate: f64,
    kubo_pass_rate: f64,
    rust_root_ttfb_p50_ms: Option<u128>,
    kubo_root_ttfb_p50_ms: Option<u128>,
    root_ttfb_p50_ratio: Option<f64>,
    rust_root_ttfb_p95_ms: Option<u128>,
    kubo_root_ttfb_p95_ms: Option<u128>,
    root_ttfb_p95_ratio: Option<f64>,
    rust_asset_ttfb_p50_ms: Option<u128>,
    kubo_asset_ttfb_p50_ms: Option<u128>,
    asset_ttfb_p50_ratio: Option<f64>,
    rust_asset_ttfb_p95_ms: Option<u128>,
    kubo_asset_ttfb_p95_ms: Option<u128>,
    asset_ttfb_p95_ratio: Option<f64>,
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
                Some(Self {
                    id,
                    rust_pass_rate: rust_case.pass_rate,
                    kubo_pass_rate: kubo_case.pass_rate,
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
                })
            })
            .collect()
    }
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
            cases,
        }
    }

    fn has_resource_metrics(&self) -> bool {
        self.run_total_ms.count > 0
            || self.gateway_rss_kib.count > 0
            || self.gateway_fd_count.count > 0
            || self.gateway_child_process_count.count > 0
            || self.gateway_storage_bytes.count > 0
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
    asset_ttfb_ms: LatencySummary,
    asset_total_ms: LatencySummary,
    asset_kind_failures: Vec<AssetKindFailure>,
    failure_groups: Vec<FailureGroup>,
}

impl CaseAggregate {
    fn from_runs(id: &str, runs: &[&RunResult]) -> Self {
        let mut run_count = 0usize;
        let mut pass_count = 0usize;
        let mut root_ttfb = Vec::new();
        let mut root_total = Vec::new();
        let mut asset_ttfb = Vec::new();
        let mut asset_total = Vec::new();
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
            asset_ttfb_ms: LatencySummary::from_values(asset_ttfb),
            asset_total_ms: LatencySummary::from_values(asset_total),
            asset_kind_failures,
            failure_groups,
        }
    }
}

#[derive(Debug, Serialize)]
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
    phases: Vec<TracePhaseAggregate>,
    slow_events: Vec<TraceSlowEvent>,
    block_sources: Vec<TraceValueCount>,
    request_statuses: Vec<TraceValueCount>,
    gateway_limiter_denials: usize,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
    bitswap_peer_fetches: Vec<TracePeerAggregate>,
    bitswap_session: TraceBitswapSessionAggregate,
    trace_errors: Vec<TraceValueCount>,
    bitswap_addr_mix: Vec<TraceValueCount>,
    bitswap_provider_quality: TraceBitswapProviderQualityAggregate,
    bitswap_connection_transports: Vec<TraceValueCount>,
    bitswap_dial_rejected_transports: Vec<TraceValueCount>,
    bitswap_dns_expansion: TraceBitswapDnsExpansionAggregate,
    slow_cids: Vec<TraceCidAggregate>,
    slow_requests: Vec<TraceRequestAggregate>,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
struct TraceValueCount {
    value: String,
    count: usize,
}

#[derive(Debug, Default, Serialize)]
struct TraceUnixfsMetadataCacheAggregate {
    events: usize,
    hits: u128,
    misses: u128,
    inserts: u128,
    evictions: u128,
    oversized_skips: u128,
    max_len: u128,
    max_capacity: u128,
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
    }
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
    session_shortcut_post_lookup_waits: usize,
    session_shortcut_attempts: usize,
    session_shortcut_hits: usize,
    session_shortcut_misses: usize,
}

impl TraceBitswapSessionAggregate {
    fn has_events(&self) -> bool {
        self.fetches > 0
            || self.session_shortcut_starts > 0
            || self.session_shortcut_post_lookup_waits > 0
            || self.session_shortcut_attempts > 0
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
}

#[derive(Debug, Serialize)]
struct TraceRequestAggregate {
    path: String,
    request_id: String,
    status: Option<String>,
    elapsed_ms: u128,
    max_event_ms: u128,
    event_count: usize,
    phases: Vec<TraceValueCount>,
    cids: Vec<TraceValueCount>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TraceRequestKey {
    request_id: String,
    path: String,
}

#[derive(Debug)]
struct TraceRequestBuilder {
    path: String,
    request_id: String,
    status: Option<String>,
    elapsed_ms: Option<u128>,
    max_event_ms: u128,
    event_count: usize,
    phases: BTreeMap<String, usize>,
    cids: BTreeMap<String, usize>,
}

impl TraceRequestBuilder {
    fn new(key: TraceRequestKey) -> Self {
        Self {
            path: key.path,
            request_id: key.request_id,
            status: None,
            elapsed_ms: None,
            max_event_ms: 0,
            event_count: 0,
            phases: BTreeMap::new(),
            cids: BTreeMap::new(),
        }
    }

    fn record_event(&mut self, phase: &str, value: &serde_json::Value, elapsed_ms: Option<u128>) {
        self.event_count += 1;
        *self.phases.entry(phase.to_string()).or_default() += 1;
        if let Some(cid) = json_detail_string(value.get("cid")) {
            *self.cids.entry(cid).or_default() += 1;
        }
        if phase == "request_done" {
            self.status = json_detail_string(value.get("status"));
            self.elapsed_ms = elapsed_ms;
        }
        if let Some(elapsed_ms) = elapsed_ms {
            self.max_event_ms = self.max_event_ms.max(elapsed_ms);
        }
    }

    fn into_aggregate(self) -> TraceRequestAggregate {
        TraceRequestAggregate {
            path: self.path,
            request_id: self.request_id,
            status: self.status,
            elapsed_ms: self.elapsed_ms.unwrap_or(self.max_event_ms),
            max_event_ms: self.max_event_ms,
            event_count: self.event_count,
            phases: sorted_trace_counts(self.phases),
            cids: sorted_trace_counts(self.cids),
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
    let mut phases = BTreeMap::<String, Vec<u128>>::new();
    let mut slow_events = Vec::<TraceSlowEvent>::new();
    let mut block_sources = BTreeMap::<String, usize>::new();
    let mut request_statuses = BTreeMap::<String, usize>::new();
    let mut gateway_limiter_denials = 0usize;
    let mut unixfs_metadata_cache = TraceUnixfsMetadataCacheAggregate::default();
    let mut bitswap_source_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_source_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_peer_fetches = BTreeMap::<String, TracePeerBuilder>::new();
    let mut trace_errors = BTreeMap::<String, usize>::new();
    let mut bitswap_addr_mix = BTreeMap::<String, usize>::new();
    let mut bitswap_provider_quality = TraceBitswapProviderQualityAggregate::default();
    let mut bitswap_connection_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_dial_rejected_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_dns_expansion = TraceBitswapDnsExpansionAggregate::default();
    let mut bitswap_session = TraceBitswapSessionAggregate::default();
    let mut slow_cids = BTreeMap::<String, TraceCidBuilder>::new();
    let mut active_requests = BTreeMap::<TraceRequestKey, TraceRequestBuilder>::new();
    let mut slow_requests = Vec::<TraceRequestAggregate>::new();

    for line in text.lines() {
        line_count += 1;
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(phase) = value.get("phase").and_then(|phase| phase.as_str()) else {
            continue;
        };
        event_count += 1;
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
            }
        }
        if phase == "request_done" {
            if let Some(key) = request_key {
                if let Some(request) = active_requests.remove(&key) {
                    slow_requests.push(request.into_aggregate());
                }
            }
        }
        if phase == "block_fetch_total" {
            if let Some(source) = value.get("source").and_then(|source| source.as_str()) {
                *block_sources.entry(source.to_string()).or_default() += 1;
            }
        }
        if phase == "request_done" {
            if let Some(status) = json_detail_string(value.get("status")) {
                *request_statuses.entry(status).or_default() += 1;
            }
        }
        if phase == "gateway_limiter"
            && value
                .get("acquired")
                .and_then(|acquired| acquired.as_bool())
                == Some(false)
        {
            gateway_limiter_denials += 1;
        }
        if phase == "unixfs_metadata_cache" {
            unixfs_metadata_cache.events += 1;
            unixfs_metadata_cache.hits += value.get("hits").and_then(json_u128).unwrap_or_default();
            unixfs_metadata_cache.misses +=
                value.get("misses").and_then(json_u128).unwrap_or_default();
            unixfs_metadata_cache.inserts +=
                value.get("inserts").and_then(json_u128).unwrap_or_default();
            unixfs_metadata_cache.evictions += value
                .get("evictions")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.oversized_skips += value
                .get("oversized_skips")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.max_len = unixfs_metadata_cache.max_len.max(
                value
                    .get("cache_len")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
            unixfs_metadata_cache.max_capacity = unixfs_metadata_cache.max_capacity.max(
                value
                    .get("cache_capacity")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
        }
        let successful_bitswap_fetch =
            phase == "bitswap_fetch" && value.get("ok").and_then(|ok| ok.as_bool()) == Some(true);
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
        if phase == "bitswap_session_shortcut_start" {
            bitswap_session.session_shortcut_starts += 1;
        }
        if phase == "bitswap_session_shortcut_post_lookup_wait" {
            bitswap_session.session_shortcut_post_lookup_waits += 1;
        }
        if phase == "bitswap_session_shortcut" {
            bitswap_session.session_shortcut_attempts += 1;
            if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
                bitswap_session.session_shortcut_hits += 1;
            } else {
                bitswap_session.session_shortcut_misses += 1;
            }
        }
        if successful_bitswap_fetch {
            if let Some(peer) = value.get("source_peer").and_then(|peer| peer.as_str()) {
                *bitswap_source_peers.entry(peer.to_string()).or_default() += 1;
            }
            if let Some(transport) = json_detail_string(value.get("source_transport")) {
                *bitswap_source_transports.entry(transport).or_default() += 1;
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
            if let Some(transport) = json_detail_string(value.get("transport")) {
                *bitswap_connection_transports.entry(transport).or_default() += 1;
            }
        }
        if phase == "bitswap_dial_rejected" {
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
    slow_requests.extend(
        active_requests
            .into_values()
            .map(TraceRequestBuilder::into_aggregate),
    );
    slow_requests.sort_by(|left, right| {
        right
            .elapsed_ms
            .cmp(&left.elapsed_ms)
            .then_with(|| right.max_event_ms.cmp(&left.max_event_ms))
            .then_with(|| left.path.cmp(&right.path))
    });
    slow_requests.truncate(MAX_TRACE_SLOW_EVENTS);

    Ok(TraceSummary {
        line_count,
        event_count,
        phases,
        slow_events,
        block_sources: sorted_trace_counts(block_sources),
        request_statuses: sorted_trace_counts(request_statuses),
        gateway_limiter_denials,
        unixfs_metadata_cache,
        bitswap_source_peers: sorted_trace_counts(bitswap_source_peers),
        bitswap_source_transports: sorted_trace_counts(bitswap_source_transports),
        bitswap_peer_fetches: sorted_trace_peers(bitswap_peer_fetches),
        bitswap_session,
        trace_errors: sorted_trace_counts(trace_errors),
        bitswap_addr_mix: sorted_trace_counts(bitswap_addr_mix),
        bitswap_provider_quality,
        bitswap_connection_transports: sorted_trace_counts(bitswap_connection_transports),
        bitswap_dial_rejected_transports: sorted_trace_counts(bitswap_dial_rejected_transports),
        bitswap_dns_expansion,
        slow_cids: sorted_trace_cids(slow_cids),
        slow_requests,
    })
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

fn format_trace_counts(counts: &[TraceValueCount]) -> String {
    counts
        .iter()
        .take(8)
        .map(|entry| format!("{}={}", entry.value, entry.count))
        .collect::<Vec<_>>()
        .join(", ")
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

fn trace_event_path(value: &serde_json::Value) -> Option<String> {
    json_detail_string(value.get("path"))
        .or_else(|| json_detail_string(value.get("span").and_then(|span| span.get("path"))))
}

fn trace_request_key(value: &serde_json::Value) -> Option<TraceRequestKey> {
    let request_id = json_detail_string(value.get("request_id")).or_else(|| {
        json_detail_string(value.get("span").and_then(|span| span.get("request_id")))
    })?;
    let path = trace_event_path(value)?;
    Some(TraceRequestKey { request_id, path })
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
        "bytes",
        "cache_hit",
        "request_id",
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
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
    body_preview: String,
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
            body_bytes: 0,
            ttfb_ms: 0,
            total_ms: 0,
            body_preview: String::new(),
            asset_summary: None,
            assets: Vec::new(),
            passed: false,
            failures,
        }
    }
}

struct FetchResponse {
    status: u16,
    content_type: Option<String>,
    content_range: Option<String>,
    body: Vec<u8>,
    ttfb_ms: u128,
    total_ms: u128,
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
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
    body_preview: String,
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
                "{\"phase\":\"request_start\",\"request_id\":9,\"path\":\"/ipns/site/asset.js\"}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":\"25\",\"cid\":\"cid1\",\"ok\":true,\"bytes\":100,\"source\":\"bitswap\",\"source_peer\":\"peer1\",\"source_transport\":\"tcp\",\"source_peer_trusted\":true,\"trusted_peer_count\":1,\"provider_peer_count\":2,\"session_peer_count\":0,\"span\":{\"path\":\"/ipns/site/asset.js\",\"request_id\":9}}\n",
                "{\"phase\":\"request_done\",\"request_id\":9,\"path\":\"/ipns/site/asset.js\",\"status\":200,\"elapsed_ms\":1}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":5,\"cid\":\"cid1\",\"source\":\"bitswap\"}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":4,\"cid\":\"cid5\",\"source\":\"cache\"}\n",
                "{\"phase\":\"provider_lookup\",\"elapsed_ms\":10,\"cid\":\"cid2\",\"provider_count\":3,\"error\":\"dht: timeout\"}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":12,\"cid\":\"cid6\",\"ok\":false,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"elapsed_ms\":3,\"cid\":\"cid7\",\"tcp_addr_count\":4,\"quic_addr_count\":2,\"ws_addr_count\":1,\"wss_addr_count\":0,\"dns_addr_count\":1,\"ip4_addr_count\":3,\"ip6_addr_count\":1,\"provider_addr_count\":10,\"expanded_provider_addr_count\":12,\"supported_provider_addr_count\":4,\"rejected_provider_addr_count\":8,\"id_only_provider_count\":1,\"invalid_provider_id_count\":2,\"provider_without_supported_bitswap_addr_count\":3,\"unsupported_relay_addr_count\":4,\"unsupported_webtransport_addr_count\":1,\"unsupported_webrtc_addr_count\":1,\"unsupported_certhash_addr_count\":1,\"unsupported_transport_addr_count\":1,\"missing_peer_addr_count\":1,\"unparsable_addr_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_start\",\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid8\",\"timeout_ms\":100}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":2,\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":true,\"source_peer\":\"peer1\",\"source_peer_trusted\":true}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":3,\"cid\":\"cid9\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":false,\"timeout\":true}\n",
                "{\"phase\":\"bitswap_connection_established\",\"peer\":\"peer1\",\"remote_addr\":\"/ip4/127.0.0.1/tcp/4001\",\"transport\":\"tcp\"}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer2\",\"transport\":\"quic\",\"connection_limit\":true,\"error\":\"Dial error\"}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":false,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":true,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bad.example\",\"cached\":false,\"ok\":false,\"record_count\":0}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":false,\"ip_count\":2}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":true,\"ip_count\":2}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"elapsed_ms\":0,\"hits\":3,\"misses\":2,\"inserts\":2,\"evictions\":1,\"oversized_skips\":0,\"cache_len\":4,\"cache_capacity\":256}\n",
                "not json\n",
                "{\"phase\":\"request_start\",\"path\":\"/ipns/site/\"}\n",
                "{\"phase\":\"gateway_limiter\",\"acquired\":false}\n",
                "{\"phase\":\"request_done\",\"status\":503}\n",
                "{\"phase\":\"request_done\",\"status\":200}\n",
                "{\"phase\":\"unixfs_file_size\",\"elapsed_ms\":50,\"cid\":\"cid3\",\"path\":\"/ipfs/root/index.html\",\"unixfs_path\":\"index.html\",\"ok\":true}\n",
                "{\"phase\":\"bitswap_request_timeout_detail\",\"elapsed_ms\":60,\"cid\":\"cid4\",\"peer_count\":16,\"trusted_peer_count\":2,\"timeout_ms\":4000,\"targets\":\"peer@[/ip4/127.0.0.1/tcp/4001]\"}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.line_count, 27);
        assert_eq!(summary.event_count, 26);
        assert_eq!(summary.slow_events.len(), 12);
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
        assert_eq!(summary.phases.len(), 9);
        assert_eq!(summary.block_sources.len(), 2);
        assert_eq!(summary.block_sources[0].value, "bitswap");
        assert_eq!(summary.block_sources[0].count, 1);
        assert_eq!(summary.block_sources[1].value, "cache");
        assert_eq!(summary.request_statuses.len(), 2);
        assert_eq!(summary.request_statuses[0].value, "200");
        assert_eq!(summary.request_statuses[0].count, 2);
        assert_eq!(summary.request_statuses[1].value, "503");
        assert_eq!(summary.gateway_limiter_denials, 1);
        assert_eq!(summary.unixfs_metadata_cache.events, 1);
        assert_eq!(summary.unixfs_metadata_cache.hits, 3);
        assert_eq!(summary.unixfs_metadata_cache.misses, 2);
        assert_eq!(summary.unixfs_metadata_cache.inserts, 2);
        assert_eq!(summary.unixfs_metadata_cache.evictions, 1);
        assert_eq!(summary.unixfs_metadata_cache.oversized_skips, 0);
        assert_eq!(summary.unixfs_metadata_cache.max_len, 4);
        assert_eq!(summary.unixfs_metadata_cache.max_capacity, 256);
        assert_eq!(summary.bitswap_source_peers.len(), 1);
        assert_eq!(summary.bitswap_source_peers[0].value, "peer1");
        assert_eq!(summary.bitswap_source_peers[0].count, 1);
        assert_eq!(summary.bitswap_source_transports.len(), 1);
        assert_eq!(summary.bitswap_source_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_source_transports[0].count, 1);
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
        assert_eq!(summary.bitswap_session.session_shortcut_attempts, 2);
        assert_eq!(summary.bitswap_session.session_shortcut_hits, 1);
        assert_eq!(summary.bitswap_session.session_shortcut_misses, 1);
        let trace_errors = summary
            .trace_errors
            .iter()
            .map(|error| error.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(trace_errors.len(), 5);
        assert!(trace_errors.contains(&"bitswap_dial_rejected: Dial error"));
        assert!(trace_errors.contains(&"bitswap_dnsaddr_expand: ok=false"));
        assert!(trace_errors.contains(&"bitswap_fetch: ok=false"));
        assert!(trace_errors.contains(&"bitswap_session_shortcut: ok=false"));
        assert!(trace_errors.contains(&"provider_lookup: dht: timeout"));
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
        assert_eq!(summary.bitswap_connection_transports.len(), 1);
        assert_eq!(summary.bitswap_connection_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_connection_transports[0].count, 1);
        assert_eq!(summary.bitswap_dial_rejected_transports.len(), 1);
        assert_eq!(summary.bitswap_dial_rejected_transports[0].value, "quic");
        assert_eq!(summary.bitswap_dial_rejected_transports[0].count, 1);
        assert_eq!(summary.bitswap_dns_expansion.events, 5);
        assert_eq!(summary.bitswap_dns_expansion.cached, 2);
        assert_eq!(summary.bitswap_dns_expansion.uncached, 3);
        assert_eq!(summary.bitswap_dns_expansion.failed, 1);
        assert_eq!(summary.bitswap_dns_expansion.records, 4);
        assert_eq!(summary.bitswap_dns_expansion.ips, 4);
        assert_eq!(summary.slow_cids[0].cid, "cid4");
        assert_eq!(summary.slow_cids[0].total_ms, 60);
        assert_eq!(summary.slow_cids[0].max_ms, 60);
        assert_eq!(
            summary.slow_cids[0].phases[0].value,
            "bitswap_request_timeout_detail"
        );
        assert_eq!(summary.slow_cids[1].cid, "cid3");
        assert_eq!(summary.slow_cids[1].paths[0].value, "/ipfs/root/index.html");
        assert_eq!(summary.slow_cids[2].cid, "cid1");
        assert_eq!(summary.slow_cids[2].total_ms, 30);
        assert_eq!(summary.slow_cids[2].count, 2);
        assert_eq!(summary.slow_requests.len(), 1);
        assert_eq!(summary.slow_requests[0].path, "/ipns/site/asset.js");
        assert_eq!(summary.slow_requests[0].request_id, "9");
        assert_eq!(summary.slow_requests[0].status.as_deref(), Some("200"));
        assert_eq!(summary.slow_requests[0].elapsed_ms, 1);
        assert_eq!(summary.slow_requests[0].max_event_ms, 25);
        assert_eq!(summary.slow_requests[0].event_count, 3);
        assert_eq!(summary.slow_requests[0].cids[0].value, "cid1");
        assert_eq!(summary.slow_requests[0].cids[0].count, 1);
        assert_eq!(summary.slow_requests[0].phases.len(), 3);
    }

    #[test]
    fn repeat_summary_aggregates_measured_resource_metrics() {
        let runs = vec![
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
        assert_eq!(summary.cases.len(), 1);
        assert_eq!(summary.cases[0].root_ttfb_ms.count, 2);
    }

    #[test]
    fn run_timeout_failure_results_marks_matching_cases_failed() {
        let corpus = Corpus {
            entries: vec![
                CorpusEntry {
                    id: "first".to_string(),
                    description: Some("first case".to_string()),
                    path: "/ipfs/first".to_string(),
                    method: None,
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_body_contains: None,
                    min_bytes: None,
                    max_ttfb_ms: None,
                },
                CorpusEntry {
                    id: "second".to_string(),
                    description: None,
                    path: "/ipfs/second".to_string(),
                    method: Some("HEAD".to_string()),
                    range: None,
                    crawl: None,
                    expect_status: Some(200),
                    expect_content_type_prefix: None,
                    expect_content_range_prefix: None,
                    expect_body_contains: None,
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
            passed: true,
            results: vec![CaseResult {
                id: "case".to_string(),
                description: None,
                method: "GET".to_string(),
                url: "http://127.0.0.1:8080/ipfs/root".to_string(),
                status: Some(200),
                content_type: Some("text/plain".to_string()),
                content_range: None,
                body_bytes: 5,
                ttfb_ms: elapsed_ms / 2,
                total_ms: elapsed_ms,
                body_preview: "hello".to_string(),
                asset_summary: None,
                assets: Vec::new(),
                passed: true,
                failures: Vec::new(),
            }],
        }
    }
}
