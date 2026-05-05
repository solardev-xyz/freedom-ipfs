use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use reqwest::header::{CACHE_CONTROL, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH, RANGE};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
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
    /// Re-fetch successful non-range GETs with If-None-Match when the first response has an ETag.
    #[arg(long)]
    conditional_revalidate: bool,
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

async fn run_offline_replay(args: &Args, corpus: &Corpus) -> Result<OfflineReplayReport> {
    if args.compare_kubo {
        bail!("--offline-replay cannot be used with --compare-kubo");
    }
    if args.gateway_url.is_some() {
        bail!("--offline-replay cannot be used with --gateway-url");
    }
    if args.engine != HarnessEngine::Rust {
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
        conditional_revalidate: args.conditional_revalidate,
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
    conditional_revalidate: bool,
    cases: &[String],
) -> Result<Vec<CaseResult>> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .context("build reqwest client")?;
    let mut results = Vec::new();
    for entry in &corpus.entries {
        if !entry_selected(entry, cases) {
            continue;
        }
        results.push(
            run_case(
                &client,
                gateway_url,
                entry,
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
    client: &reqwest::Client,
    gateway_url: &str,
    entry: &CorpusEntry,
    asset_concurrency: usize,
    conditional_revalidate: bool,
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
    let revalidation = maybe_revalidate_response(
        client,
        &url,
        method,
        entry.range.as_deref(),
        &response,
        conditional_revalidate,
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
        etag: response.etag,
        cache_control: response.cache_control,
        body_bytes: response.body.len(),
        ttfb_ms: response.ttfb_ms,
        total_ms: response.total_ms,
        body_preview,
        revalidation,
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
        if trace.block_store.events > 0 {
            let store = &trace.block_store;
            println!(
                "  block store: events={} hits={} misses={} rechecks={} recheck_hits={} recheck_misses={}",
                store.events,
                store.hits,
                store.misses,
                store.rechecks,
                store.recheck_hits,
                store.recheck_misses
            );
        }
        print_trace_provider_retries(trace);
        if !trace.request_statuses.is_empty() || trace.gateway_limiter_denials > 0 {
            println!(
                "  gateway responses: statuses={} limiter_denials={}",
                format_trace_counts(&trace.request_statuses),
                trace.gateway_limiter_denials
            );
        }
        print_trace_unixfs_metadata_cache(trace);
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
        if !trace.bitswap_deliveries.is_empty() {
            println!(
                "  bitswap deliveries: {}",
                format_trace_counts(&trace.bitswap_deliveries)
            );
        }
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
        print_trace_timeout_recovery(trace);
        print_trace_bitswap_peer_attempts(trace);
        if trace.bitswap_dial_plans.events > 0 {
            let plans = &trace.bitswap_dial_plans;
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
        if !trace.bitswap_connection_transports.is_empty() {
            println!(
                "  bitswap connection transports: {}",
                format_trace_counts(&trace.bitswap_connection_transports)
            );
        }
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
                let request_id = if request.process_id.is_empty() {
                    request.request_id.clone()
                } else {
                    format!("{}:{}", request.process_id, request.request_id)
                };
                println!(
                    "    {}: {}ms status={} request_id={} events={} max_event={}ms phases={} cids={}",
                    request.path,
                    request.elapsed_ms,
                    status,
                    request_id,
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
    print_comparison_trace_summary("rust", &report.rust);
    print_comparison_trace_summary("kubo", &report.kubo);
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
    if trace.block_store.events > 0 {
        let store = &trace.block_store;
        println!(
            "  block store: events={} hits={} misses={} rechecks={} recheck_hits={} recheck_misses={}",
            store.events,
            store.hits,
            store.misses,
            store.rechecks,
            store.recheck_hits,
            store.recheck_misses
        );
    }
    print_trace_unixfs_metadata_cache(trace);
    print_trace_progress_phases(trace);
    print_trace_provider_retries(trace);
    print_trace_timeout_recovery(trace);
    print_trace_bitswap_peer_attempts(trace);
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
    if !trace.bitswap_connection_transports.is_empty() {
        println!(
            "  bitswap connection transports: {}",
            format_trace_counts(&trace.bitswap_connection_transports)
        );
    }
    print_trace_connection_errors(trace);
    print_trace_connection_backoff(trace);
    print_trace_dial_rejections(trace);
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

fn print_trace_unixfs_metadata_cache(trace: &TraceSummary) {
    if trace.unixfs_metadata_cache.events == 0 {
        return;
    }
    let cache = &trace.unixfs_metadata_cache;
    println!(
        "  unixfs metadata cache: events={} hits={} misses={} inserts={} evictions={} oversized_skips={} max_len={} path_hits={} path_misses={} path_inserts={} path_evictions={} path_oversized_skips={} max_path_len={} capacity={}",
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
        cache.max_capacity
    );
}

fn print_trace_provider_retries(trace: &TraceSummary) {
    if !trace.provider_retries.has_events() {
        return;
    }
    let retries = &trace.provider_retries;
    println!(
        "  provider retries: refresh_timeout={} refresh_failure={} retry_counts={} same_providers={} same_bitswap_peers={} request_timeout_counts={} same_bitswap_request_timeouts={} retry_request_timeout={} retry_timeout={} retry_connection_timeout={}",
        retries.refresh_after_timeout_events,
        retries.refresh_after_failure_events,
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

fn format_trace_bitswap_peer_attempts(
    attempts: &TraceBitswapPeerAttemptAggregate,
) -> Option<String> {
    if !attempts.has_events() {
        return None;
    }
    Some(format!(
        "bitswap peer attempts: starts={} outgoing_completed={} successes={} failures={} connection_timeouts={} read_timeouts={} other_failures={} prefer_want_have={}",
        attempts.starts,
        attempts.outgoing_completed,
        attempts.successes,
        attempts.failures,
        attempts.connection_timeouts,
        attempts.read_timeouts,
        attempts.other_failures,
        attempts.prefer_want_have
    ))
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

fn print_trace_connection_errors(trace: &TraceSummary) {
    let errors = &trace.bitswap_connection_errors;
    if errors.events == 0 {
        return;
    }
    println!(
        "  bitswap connection errors: events={} with_peer={} without_peer={} classes={} peers={}",
        errors.events,
        errors.with_peer,
        errors.without_peer,
        format_trace_counts(&errors.classes),
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
        "{mark} {:32} status={} type={} bytes={} ttfb={}ms total={}ms etag={} cache_control={}",
        result.id,
        result
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.content_type.as_deref().unwrap_or("-"),
        result.body_bytes,
        result.ttfb_ms,
        result.total_ms,
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
            if let Some(revalidation) = &asset.revalidation {
                print_revalidation_result("      revalidation", revalidation);
            }
        }
    }
}

fn print_revalidation_result(prefix: &str, result: &RevalidationResult) {
    let mark = if result.passed { "PASS" } else { "FAIL" };
    println!(
        "{prefix}: {mark} status={} bytes={} ttfb={}ms total={}ms etag={} cache_control={}",
        result
            .status
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string()),
        result.body_bytes,
        result.ttfb_ms,
        result.total_ms,
        result.etag.as_deref().unwrap_or("-"),
        result.cache_control.as_deref().unwrap_or("-")
    );
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
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let cache_control = response
        .headers()
        .get(CACHE_CONTROL)
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
        etag,
        cache_control,
        body,
        ttfb_ms,
        total_ms,
    })
}

async fn maybe_revalidate_response(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    range: Option<&str>,
    response: &FetchResponse,
    conditional_revalidate: bool,
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
            passed: false,
            failures: vec!["response omitted ETag".to_string()],
        });
    };
    Some(fetch_revalidation(client, url, etag).await)
}

async fn fetch_revalidation(client: &reqwest::Client, url: &str, etag: &str) -> RevalidationResult {
    let started = Instant::now();
    let response = match client.get(url).header(IF_NONE_MATCH, etag).send().await {
        Ok(response) => response,
        Err(err) => {
            return RevalidationResult {
                status: None,
                etag: None,
                cache_control: None,
                body_bytes: 0,
                ttfb_ms: started.elapsed().as_millis(),
                total_ms: started.elapsed().as_millis(),
                passed: false,
                failures: vec![format!("request error: {err}")],
            };
        }
    };

    let ttfb_ms = started.elapsed().as_millis();
    let status = response.status().as_u16();
    let response_etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let cache_control = response
        .headers()
        .get(CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(err) => {
            return RevalidationResult {
                status: Some(status),
                etag: response_etag,
                cache_control,
                body_bytes: 0,
                ttfb_ms,
                total_ms: started.elapsed().as_millis(),
                passed: false,
                failures: vec![format!("body error: {err}")],
            };
        }
    };
    let total_ms = started.elapsed().as_millis();
    let mut failures = Vec::new();
    if status != 304 {
        failures.push(format!("status {status}, expected 304"));
    }
    if !body.is_empty() {
        failures.push(format!(
            "body {} bytes, expected empty 304 body",
            body.len()
        ));
    }

    RevalidationResult {
        status: Some(status),
        etag: response_etag,
        cache_control,
        body_bytes: body.len(),
        ttfb_ms,
        total_ms,
        passed: failures.is_empty(),
        failures,
    }
}

async fn run_page_crawl(
    client: &reqwest::Client,
    page_url: &str,
    page_body: &[u8],
    config: &CrawlConfig,
    asset_concurrency: usize,
    conditional_revalidate: bool,
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
    conditional_revalidate: bool,
) -> Vec<FetchedAsset> {
    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut tasks = JoinSet::new();
    for asset in assets {
        let client = client.clone();
        let semaphore = semaphore.clone();
        tasks.spawn(async move {
            let _permit = semaphore.acquire_owned().await.ok();
            fetch_asset(&client, asset, max_bytes, conditional_revalidate).await
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
    conditional_revalidate: bool,
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
        etag: None,
        cache_control: None,
        body_bytes: 0,
        ttfb_ms: 0,
        total_ms: 0,
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
    result.etag = response.etag.clone();
    result.cache_control = response.cache_control.clone();
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
    result.revalidation = maybe_revalidate_response(
        client,
        &result.url,
        "GET",
        range,
        &response,
        conditional_revalidate,
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
    expect_body_contains: Option<String>,
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
        let (offline_request_statuses, offline_trace_errors, offline_progress_phases) = report
            .trace_summary
            .as_ref()
            .map(|trace| {
                (
                    trace.request_statuses.clone(),
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
            offline_trace_errors,
            offline_progress_phases,
        }
    }
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
        let mut asset_ttfb = Vec::new();
        let mut asset_total = Vec::new();
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
            asset_ttfb_ms: LatencySummary::from_values(asset_ttfb),
            asset_total_ms: LatencySummary::from_values(asset_total),
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
    progress_phases: Vec<TraceValueCount>,
    slow_events: Vec<TraceSlowEvent>,
    block_sources: Vec<TraceValueCount>,
    block_store: TraceBlockStoreAggregate,
    provider_retries: TraceProviderRetryAggregate,
    request_statuses: Vec<TraceValueCount>,
    gateway_limiter_denials: usize,
    unixfs_metadata_cache: TraceUnixfsMetadataCacheAggregate,
    bitswap_source_peers: Vec<TraceValueCount>,
    bitswap_source_transports: Vec<TraceValueCount>,
    bitswap_deliveries: Vec<TraceValueCount>,
    bitswap_extra_blocks: TraceBitswapExtraBlockAggregate,
    bitswap_peer_fetches: Vec<TracePeerAggregate>,
    bitswap_session: TraceBitswapSessionAggregate,
    bitswap_peer_attempts: TraceBitswapPeerAttemptAggregate,
    bitswap_dial_plans: TraceBitswapDialPlanAggregate,
    bitswap_incoming_blocks: TraceBitswapIncomingBlockAggregate,
    bitswap_incoming_reads: TraceBitswapIncomingReadAggregate,
    bitswap_timeout_recovery: TraceBitswapTimeoutRecoveryAggregate,
    trace_errors: Vec<TraceValueCount>,
    bitswap_addr_mix: Vec<TraceValueCount>,
    bitswap_provider_quality: TraceBitswapProviderQualityAggregate,
    bitswap_connection_transports: Vec<TraceValueCount>,
    bitswap_connection_errors: TraceBitswapConnectionErrorAggregate,
    bitswap_connection_backoff: TraceBitswapConnectionBackoffAggregate,
    bitswap_dial_rejections: TraceBitswapDialRejectedAggregate,
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

#[derive(Clone, Debug, Serialize)]
struct TraceValueCount {
    value: String,
    count: usize,
}

#[derive(Debug, Default, Serialize)]
struct TraceBlockStoreAggregate {
    events: usize,
    hits: usize,
    misses: usize,
    rechecks: usize,
    recheck_hits: usize,
    recheck_misses: usize,
}

#[derive(Debug, Default, Serialize)]
struct TraceProviderRetryAggregate {
    refresh_after_timeout_events: usize,
    refresh_after_failure_events: usize,
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
            || self.retry_count_events > 0
            || self.request_timeout_retries > 0
            || self.timeout_retries > 0
            || self.connection_timeout_retries > 0
    }
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
    path_hits: u128,
    path_misses: u128,
    path_inserts: u128,
    path_evictions: u128,
    path_oversized_skips: u128,
    max_path_len: u128,
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
}

impl TraceBitswapPeerAttemptAggregate {
    fn has_events(&self) -> bool {
        self.starts > 0 || self.outgoing_completed > 0
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
    process_id: String,
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
    process_id: String,
    request_id: String,
    path: String,
}

#[derive(Debug)]
struct TraceRequestBuilder {
    path: String,
    process_id: String,
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
            process_id: key.process_id,
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
            process_id: self.process_id,
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
    let mut progress_phases = BTreeMap::<String, usize>::new();
    let mut slow_events = Vec::<TraceSlowEvent>::new();
    let mut block_sources = BTreeMap::<String, usize>::new();
    let mut block_store = TraceBlockStoreAggregate::default();
    let mut provider_retries = TraceProviderRetryAggregate::default();
    let mut request_statuses = BTreeMap::<String, usize>::new();
    let mut gateway_limiter_denials = 0usize;
    let mut unixfs_metadata_cache = TraceUnixfsMetadataCacheAggregate::default();
    let mut bitswap_source_peers = BTreeMap::<String, usize>::new();
    let mut bitswap_source_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_deliveries = BTreeMap::<String, usize>::new();
    let mut bitswap_extra_blocks = TraceBitswapExtraBlockAggregate::default();
    let mut bitswap_peer_fetches = BTreeMap::<String, TracePeerBuilder>::new();
    let mut trace_errors = BTreeMap::<String, usize>::new();
    let mut bitswap_addr_mix = BTreeMap::<String, usize>::new();
    let mut bitswap_provider_quality = TraceBitswapProviderQualityAggregate::default();
    let mut bitswap_connection_transports = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_error_events = 0usize;
    let mut bitswap_connection_error_with_peer = 0usize;
    let mut bitswap_connection_error_without_peer = 0usize;
    let mut bitswap_connection_error_classes = BTreeMap::<String, usize>::new();
    let mut bitswap_connection_error_peers = BTreeMap::<String, usize>::new();
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
    let mut bitswap_dial_plans = TraceBitswapDialPlanAggregate::default();
    let mut bitswap_incoming_blocks = TraceBitswapIncomingBlockAggregate::default();
    let mut bitswap_incoming_reads = TraceBitswapIncomingReadAggregate::default();
    let mut bitswap_timeout_recovery = TraceBitswapTimeoutRecoveryBuilder::default();
    let mut provider_fetch_dial_plan_seen = BTreeMap::<String, bool>::new();
    let mut pending_request_timeout_retries = BTreeMap::<String, usize>::new();
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
        match phase {
            "provider_refresh_after_timeout" => {
                provider_retries.refresh_after_timeout_events += 1;
            }
            "provider_refresh_after_failure" => {
                provider_retries.refresh_after_failure_events += 1;
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
            unixfs_metadata_cache.path_hits += value
                .get("path_hits")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.path_misses += value
                .get("path_misses")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.path_inserts += value
                .get("path_inserts")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.path_evictions += value
                .get("path_evictions")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.path_oversized_skips += value
                .get("path_oversized_skips")
                .and_then(json_u128)
                .unwrap_or_default();
            unixfs_metadata_cache.max_len = unixfs_metadata_cache.max_len.max(
                value
                    .get("cache_len")
                    .and_then(json_u128)
                    .unwrap_or_default(),
            );
            unixfs_metadata_cache.max_path_len = unixfs_metadata_cache.max_path_len.max(
                value
                    .get("path_cache_len")
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
        if phase == "bitswap_peer_attempt_start" {
            bitswap_peer_attempts.starts += 1;
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
        if phase == "bitswap_dial_plan" {
            if let Some(cid) = json_detail_string(value.get("cid")) {
                provider_fetch_dial_plan_seen.insert(cid, true);
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
        }
        if successful_bitswap_delivery {
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
        progress_phases: sorted_trace_counts(progress_phases),
        slow_events,
        block_sources: sorted_trace_counts(block_sources),
        block_store,
        provider_retries,
        request_statuses: sorted_trace_counts(request_statuses),
        gateway_limiter_denials,
        unixfs_metadata_cache,
        bitswap_source_peers: sorted_trace_counts(bitswap_source_peers),
        bitswap_source_transports: sorted_trace_counts(bitswap_source_transports),
        bitswap_deliveries: sorted_trace_counts(bitswap_deliveries),
        bitswap_extra_blocks,
        bitswap_peer_fetches: sorted_trace_peers(bitswap_peer_fetches),
        bitswap_session,
        bitswap_peer_attempts,
        bitswap_dial_plans,
        bitswap_incoming_blocks,
        bitswap_incoming_reads,
        bitswap_timeout_recovery: bitswap_timeout_recovery
            .into_aggregate(&pending_request_timeout_retries),
        trace_errors: sorted_trace_counts(trace_errors),
        bitswap_addr_mix: sorted_trace_counts(bitswap_addr_mix),
        bitswap_provider_quality,
        bitswap_connection_transports: sorted_trace_counts(bitswap_connection_transports),
        bitswap_connection_errors: TraceBitswapConnectionErrorAggregate {
            events: bitswap_connection_error_events,
            with_peer: bitswap_connection_error_with_peer,
            without_peer: bitswap_connection_error_without_peer,
            classes: sorted_trace_counts(bitswap_connection_error_classes),
            peers: sorted_trace_counts(bitswap_connection_error_peers),
        },
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

fn trace_progress_phase<'a>(raw_phase: &'a str, value: &serde_json::Value) -> &'a str {
    match raw_phase {
        "request_start" | "preload_start" => "started",
        "request_done" => match value.get("status").and_then(json_u128) {
            Some(status) if status >= 400 => "failed",
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
        "block_fetch_total" => match value.get("source").and_then(|source| source.as_str()) {
            Some("cache") => "cache_hit",
            Some("bitswap") => "fetching_bitswap",
            Some("http_provider") => "fetching_http_provider",
            _ => "streaming",
        },
        "block_fetch_coalesced" => "streaming",
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
            Some(true) => "providers_found",
            _ => "provider_lookup",
        },
        "provider_lookup" if value.get("error").is_some() => "failed",
        "provider_lookup" => "providers_found",
        "provider_diversity_low" => "provider_diversity_low",
        "light_dht_provider_lookup" | "dht_provider_lookup" => "dht_fallback_started",
        "provider_fetch_start" => "providers_found",
        "bitswap_dnsaddr_expand" | "bitswap_dns_multiaddr_expand" => "provider_lookup",
        "http_provider_fetch" => "fetching_http_provider",
        "bitswap_fetch"
        | "bitswap_connection_established"
        | "bitswap_incoming_block"
        | "bitswap_peer_attempt"
        | "bitswap_peer_attempt_start"
        | "bitswap_peer_expand"
        | "bitswap_dial_plan"
        | "bitswap_session_shortcut"
        | "bitswap_session_shortcut_start"
        | "bitswap_session_shortcut_post_lookup_wait" => "fetching_bitswap",
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
        | "bitswap_incoming_stream_read"
        | "bitswap_peer_timeout"
        | "bitswap_peer_timeout_suppressed"
        | "bitswap_provider_candidates_empty"
        | "bitswap_request_timeout"
        | "provider_retry_after_timeout"
        | "provider_retry_after_request_timeout"
        | "provider_refresh_after_timeout"
        | "provider_refresh_after_failure" => "retrying",
        "ipfs_path_parse"
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

fn trace_error_key(phase: &str, value: &serde_json::Value) -> Option<String> {
    if let Some(error) = json_detail_string(value.get("error")) {
        return Some(format!("{phase}: {error}"));
    }
    if value.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
        return Some(format!("{phase}: ok=false"));
    }
    None
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

fn trace_event_path(value: &serde_json::Value) -> Option<String> {
    json_detail_string(value.get("path"))
        .or_else(|| json_detail_string(value.get("span").and_then(|span| span.get("path"))))
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
        "pending_waiter_count",
        "oldest_pending_ms",
        "newest_pending_ms",
        "cache_hit",
        "process_id",
        "request_id",
        "peer",
        "prefer_want_have",
        "want_have_timeout_ms",
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
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
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
            etag: None,
            cache_control: None,
            body_bytes: 0,
            ttfb_ms: 0,
            total_ms: 0,
            body_preview: String::new(),
            revalidation: None,
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
    etag: Option<String>,
    cache_control: Option<String>,
    body: Vec<u8>,
    ttfb_ms: u128,
    total_ms: u128,
}

#[derive(Debug, Serialize)]
struct RevalidationResult {
    status: Option<u16>,
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
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
    etag: Option<String>,
    cache_control: Option<String>,
    body_bytes: usize,
    ttfb_ms: u128,
    total_ms: u128,
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
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":\"25\",\"cid\":\"cid1\",\"ok\":true,\"bytes\":100,\"extra_blocks\":2,\"source\":\"bitswap\",\"source_peer\":\"peer1\",\"source_transport\":\"tcp\",\"bitswap_delivery\":\"incoming\",\"source_peer_trusted\":true,\"trusted_peer_count\":1,\"provider_peer_count\":2,\"session_peer_count\":0,\"span\":{\"path\":\"/ipns/site/asset.js\",\"request_id\":9}}\n",
                "{\"phase\":\"request_done\",\"request_id\":9,\"path\":\"/ipns/site/asset.js\",\"status\":200,\"elapsed_ms\":1}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":5,\"cid\":\"cid1\",\"source\":\"bitswap\"}\n",
                "{\"phase\":\"block_fetch_total\",\"elapsed_ms\":4,\"cid\":\"cid5\",\"source\":\"cache\"}\n",
                "{\"phase\":\"provider_lookup\",\"elapsed_ms\":10,\"cid\":\"cid2\",\"provider_count\":3,\"error\":\"dht: timeout\"}\n",
                "{\"phase\":\"bitswap_fetch\",\"elapsed_ms\":12,\"cid\":\"cid6\",\"ok\":false,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"elapsed_ms\":3,\"cid\":\"cid7\",\"tcp_addr_count\":4,\"quic_addr_count\":2,\"ws_addr_count\":1,\"wss_addr_count\":0,\"dns_addr_count\":1,\"ip4_addr_count\":3,\"ip6_addr_count\":1,\"provider_addr_count\":10,\"expanded_provider_addr_count\":12,\"supported_provider_addr_count\":4,\"rejected_provider_addr_count\":8,\"id_only_provider_count\":1,\"invalid_provider_id_count\":2,\"provider_without_supported_bitswap_addr_count\":3,\"unsupported_relay_addr_count\":4,\"unsupported_webtransport_addr_count\":1,\"unsupported_webrtc_addr_count\":1,\"unsupported_certhash_addr_count\":1,\"unsupported_transport_addr_count\":1,\"missing_peer_addr_count\":1,\"unparsable_addr_count\":1,\"addr_with_relay_count\":6,\"addr_with_webtransport_count\":2,\"addr_with_webrtc_count\":3,\"addr_with_certhash_count\":4}\n",
                "{\"phase\":\"bitswap_session_shortcut_start\",\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut_post_lookup_wait\",\"cid\":\"cid8\",\"timeout_ms\":100}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":2,\"cid\":\"cid8\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":true,\"source_peer\":\"peer1\",\"bitswap_delivery\":\"outgoing\",\"source_peer_trusted\":true,\"extra_blocks\":1}\n",
                "{\"phase\":\"bitswap_session_shortcut\",\"elapsed_ms\":3,\"cid\":\"cid9\",\"peer_count\":1,\"trusted_peer_count\":1,\"ok\":false,\"timeout\":true}\n",
                "{\"phase\":\"bitswap_connection_established\",\"peer\":\"peer1\",\"remote_addr\":\"/ip4/127.0.0.1/tcp/4001\",\"transport\":\"tcp\"}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"peer3\",\"error\":\"Failed to negotiate transport protocol(s): Protocol negotiation failed.\"}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"\",\"error\":\"Failed to negotiate transport protocol(s): Connection refused (os error 111)\"}\n",
                "{\"phase\":\"bitswap_connection_error_backoff\",\"peer\":\"peer3\",\"error_class\":\"protocol_negotiation_failed\",\"count\":2,\"ttl_ms\":30000}\n",
                "{\"phase\":\"bitswap_connection_error_peer_skipped\",\"cid\":\"cid-skip\",\"peer\":\"peer3\",\"remaining_ms\":25000}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer2\",\"transport\":\"quic\",\"connection_limit\":true,\"error\":\"Dial error\"}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":false,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bootstrap.example\",\"cached\":true,\"ok\":true,\"record_count\":2}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"bad.example\",\"cached\":false,\"ok\":false,\"record_count\":0}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":false,\"ip_count\":2}\n",
                "{\"phase\":\"bitswap_dns_multiaddr_expand\",\"host\":\"peer.example\",\"cached\":true,\"ip_count\":2}\n",
                "{\"phase\":\"unixfs_metadata_cache\",\"elapsed_ms\":0,\"hits\":3,\"misses\":2,\"inserts\":2,\"evictions\":1,\"oversized_skips\":0,\"cache_len\":4,\"path_hits\":5,\"path_misses\":7,\"path_inserts\":6,\"path_evictions\":1,\"path_oversized_skips\":0,\"path_cache_len\":6,\"cache_capacity\":256}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid10\",\"cache_hit\":false}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid11\",\"cache_hit\":true,\"rechecked\":true}\n",
                "{\"phase\":\"block_store_get\",\"elapsed_ms\":0,\"cid\":\"cid12\",\"cache_hit\":false,\"rechecked\":true}\n",
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

        assert_eq!(summary.line_count, 34);
        assert_eq!(summary.event_count, 33);
        assert_eq!(summary.slow_events.len(), 15);
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
        assert_eq!(summary.phases.len(), 10);
        assert_eq!(summary.block_sources.len(), 2);
        assert_eq!(summary.block_sources[0].value, "bitswap");
        assert_eq!(summary.block_sources[0].count, 1);
        assert_eq!(summary.block_sources[1].value, "cache");
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
        assert_eq!(summary.unixfs_metadata_cache.max_capacity, 256);
        assert_eq!(summary.bitswap_source_peers.len(), 1);
        assert_eq!(summary.bitswap_source_peers[0].value, "peer1");
        assert_eq!(summary.bitswap_source_peers[0].count, 1);
        assert_eq!(summary.bitswap_source_transports.len(), 1);
        assert_eq!(summary.bitswap_source_transports[0].value, "tcp");
        assert_eq!(summary.bitswap_source_transports[0].count, 1);
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
        assert_eq!(summary.bitswap_session.session_shortcut_attempts, 2);
        assert_eq!(summary.bitswap_session.session_shortcut_hits, 1);
        assert_eq!(summary.bitswap_session.session_shortcut_misses, 1);
        let trace_errors = summary
            .trace_errors
            .iter()
            .map(|error| error.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(trace_errors.len(), 7);
        assert!(trace_errors.iter().any(|error| {
            error.starts_with("bitswap_connection_error: Failed to negotiate transport protocol")
        }));
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
                "{\"phase\":\"provider_lookup\",\"cid\":\"cid-a\",\"provider_count\":3}\n",
                "{\"phase\":\"provider_diversity_low\",\"cid\":\"cid-a\",\"provider_count\":1,\"fallback\":\"light_dht\"}\n",
                "{\"phase\":\"bitswap_dnsaddr_expand\",\"host\":\"peer.test\",\"record_count\":2}\n",
                "{\"phase\":\"block_store_get\",\"cid\":\"cid-a\",\"cache_hit\":false}\n",
                "{\"phase\":\"block_store_get\",\"cid\":\"cid-b\",\"cache_hit\":true}\n",
                "{\"phase\":\"http_provider_fetch\",\"cid\":\"cid-a\",\"ok\":true}\n",
                "{\"phase\":\"bitswap_peer_expand\",\"cid\":\"cid-a\",\"peer_count\":2}\n",
                "{\"phase\":\"bitswap_connection_established\",\"peer\":\"peer-a\",\"transport\":\"tcp\"}\n",
                "{\"phase\":\"unixfs_resource\",\"path\":\"/ipns/site/\",\"ok\":true}\n",
                "{\"phase\":\"gateway_conditional\",\"path\":\"/ipns/site/\",\"outcome\":\"not_modified\"}\n",
                "{\"phase\":\"bitswap_request_timeout\",\"cid\":\"cid-a\",\"peer_count\":2}\n",
                "{\"phase\":\"bitswap_connection_error\",\"peer\":\"peer-b\",\"error\":\"timeout\"}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-c\",\"ok\":false,\"timed_out\":true}\n",
                "{\"phase\":\"gateway_limiter\",\"acquired\":false}\n",
                "{\"phase\":\"request_done\",\"request_id\":1,\"path\":\"/ipns/site/\",\"status\":200}\n",
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
            2
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
            1
        );
        assert_eq!(
            trace_value_count(&summary.progress_phases, "fetching_bitswap"),
            2
        );
        assert_eq!(trace_value_count(&summary.progress_phases, "streaming"), 1);
        assert_eq!(trace_value_count(&summary.progress_phases, "retrying"), 3);
        assert_eq!(trace_value_count(&summary.progress_phases, "completed"), 1);
        assert_eq!(trace_value_count(&summary.progress_phases, "failed"), 2);
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
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"peer\":\"peer-a\",\"prefer_want_have\":false}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":25,\"cid\":\"cid-a\",\"peer\":\"peer-a\",\"ok\":true,\"prefer_want_have\":false,\"bytes\":42}\n",
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"peer\":\"peer-b\",\"prefer_want_have\":true}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":5000,\"cid\":\"cid-a\",\"peer\":\"peer-b\",\"ok\":false,\"prefer_want_have\":true,\"failure_kind\":\"connection_timeout\",\"error\":\"timed out\"}\n",
                "{\"phase\":\"bitswap_peer_attempt_start\",\"cid\":\"cid-a\",\"peer\":\"peer-c\",\"prefer_want_have\":true}\n",
                "{\"phase\":\"bitswap_peer_attempt\",\"elapsed_ms\":10000,\"cid\":\"cid-a\",\"peer\":\"peer-c\",\"ok\":false,\"prefer_want_have\":true,\"failure_kind\":\"read_timeout\",\"error\":\"read timed out\"}\n",
                "{\"phase\":\"provider_refresh_after_timeout\",\"cid\":\"cid-a\",\"request_timeout\":true}\n",
                "{\"phase\":\"retry_provider_count\",\"cid\":\"cid-a\",\"same_provider_set\":true,\"same_bitswap_peer_set\":true,\"request_timeout\":true}\n",
                "{\"phase\":\"provider_retry_after_request_timeout\",\"cid\":\"cid-a\",\"provider_count\":3,\"request_timeout\":true}\n",
                "{\"phase\":\"bitswap_dial_plan\",\"cid\":\"cid-a\",\"peer_count\":4,\"candidate_peer_count\":5,\"new_dial_peer_count\":2,\"new_dial_addr_count\":3,\"suppressed_dial_peer_count\":1,\"suppressed_dial_addr_count\":4,\"pending_dial_peer_count\":2,\"connected_peer_count\":1,\"command_queued_ms\":7}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer-a\",\"transport\":\"tcp\",\"connection_limit\":true}\n",
                "{\"phase\":\"bitswap_dial_rejected\",\"peer\":\"peer-b\",\"transport\":\"ws\",\"connection_limit\":false}\n",
                "{\"phase\":\"bitswap_incoming_block\",\"cid\":\"cid-a\",\"peer\":\"peer-d\",\"source_transport\":\"tcp\",\"block_count\":2,\"bytes\":256,\"pending_waiter_count\":3,\"delivered_waiter_count\":2,\"dropped_waiter_count\":1,\"oldest_pending_ms\":75,\"newest_pending_ms\":25}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-e\",\"ok\":false,\"dropped\":true,\"pending_reads\":32}\n",
                "{\"phase\":\"bitswap_incoming_stream_read\",\"peer\":\"peer-f\",\"ok\":false,\"timed_out\":true,\"timeout_ms\":6000,\"elapsed_ms\":6001}\n",
            ),
        )
        .unwrap();

        let summary = summarize_trace_output(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(summary.bitswap_peer_attempts.starts, 3);
        assert_eq!(summary.bitswap_peer_attempts.outgoing_completed, 3);
        assert_eq!(summary.bitswap_peer_attempts.successes, 1);
        assert_eq!(summary.bitswap_peer_attempts.failures, 2);
        assert_eq!(summary.bitswap_peer_attempts.connection_timeouts, 1);
        assert_eq!(summary.bitswap_peer_attempts.read_timeouts, 1);
        assert_eq!(summary.bitswap_peer_attempts.other_failures, 0);
        assert_eq!(summary.bitswap_peer_attempts.prefer_want_have, 2);
        assert_eq!(summary.provider_retries.refresh_after_timeout_events, 1);
        assert_eq!(summary.provider_retries.refresh_after_failure_events, 0);
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
        })
        .unwrap();

        assert_eq!(
            line,
            "bitswap peer attempts: starts=3 outgoing_completed=2 successes=1 failures=1 connection_timeouts=1 read_timeouts=0 other_failures=0 prefer_want_have=2"
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
            etag: Some("\"asset\"".to_string()),
            cache_control: Some("public, max-age=31536000, immutable".to_string()),
            body_bytes: 128,
            ttfb_ms: 5,
            total_ms: 6,
            body_preview: String::new(),
            revalidation: Some(RevalidationResult {
                status: Some(200),
                etag: Some("\"asset\"".to_string()),
                cache_control: Some("public, max-age=31536000, immutable".to_string()),
                body_bytes: 128,
                ttfb_ms: 4,
                total_ms: 5,
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
        let client = reqwest::Client::new();
        let response = FetchResponse {
            status: 200,
            content_type: Some("text/plain".to_string()),
            content_range: None,
            etag: None,
            cache_control: Some("public, max-age=31536000, immutable".to_string()),
            body: b"hello".to_vec(),
            ttfb_ms: 1,
            total_ms: 1,
        };

        let revalidation = maybe_revalidate_response(
            &client,
            "http://127.0.0.1:9/ipfs/root",
            "GET",
            None,
            &response,
            true,
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
                    expect_body_contains: None,
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
            expect_body_contains: None,
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
            expect_body_contains: None,
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
                etag: None,
                cache_control: None,
                body_bytes: 0,
                ttfb_ms: 10,
                total_ms: 10,
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
                etag: None,
                cache_control: None,
                body_bytes: 128,
                ttfb_ms: 5,
                total_ms: 5,
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
            engine: HarnessEngine::Rust,
            gateway_db: Some("/tmp/replay.db".to_string()),
            kubo_repo: None,
            trace_output: None,
            trace_summary: None,
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
        assert!(summary.offline_trace_errors.is_empty());
        assert!(summary.offline_progress_phases.is_empty());
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
                etag: None,
                cache_control: None,
                body_bytes: 5,
                ttfb_ms: elapsed_ms / 2,
                total_ms: elapsed_ms,
                body_preview: "hello".to_string(),
                revalidation: None,
                asset_summary: None,
                assets: Vec::new(),
                passed: true,
                failures: Vec::new(),
            }],
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
            expect_body_contains: None,
            min_bytes: None,
            max_ttfb_ms: None,
        }
    }
}
