use anyhow::{Context, Result};
use axum::Router;
use clap::{Parser, ValueEnum};
use freedom_ipfs_core::parse_cid;
use freedom_ipfs_gateway::{
    router_with_provider_and_name_resolver_config, GatewayConfig, GatewayHtmlPrefetchConfig,
    GatewayHtmlRangeWarmConfig, PersistentNameResolver, DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS,
    DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES,
};
use freedom_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsRecord, IpnsResolver, NamesysError,
};
use freedom_ipfs_retrieval::FetchingBlockProvider;
use freedom_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, DEFAULT_DELEGATED_ROUTER, DEFAULT_DHT_QUERY_TIMEOUT,
    DEFAULT_MAX_DHT_PROVIDERS,
};
use freedom_ipfs_store::SqliteBlockStore;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const DEFAULT_TRACE_FILTER: &str =
    "freedom_ipfs_gateway=info,freedom_ipfs_retrieval=info,freedom_ipfs_namesys=info,freedom_ipfs_routing=info,warn";
const HTML_PREFETCH_MAX_ASSETS_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_MAX_ASSETS";
const HTML_PREFETCH_MAX_BYTES_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_MAX_BYTES";
const HTML_PREFETCH_CONCURRENCY_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_PREFETCH_CONCURRENCY";
const HTML_RANGE_WARM_MAX_BYTES_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_RANGE_WARM_MAX_BYTES";
const HTML_RANGE_WARM_CONCURRENCY_ENV: &str = "FREEDOM_IPFS_GATEWAY_HTML_RANGE_WARM_CONCURRENCY";
const RAW_LINK_TSIZE_FAST_HEADERS_ENV: &str = "FREEDOM_IPFS_ENABLE_RAW_LINK_TSIZE_FAST_HEADERS";
const STREAM_SMALL_BODIES_ENV: &str = "FREEDOM_IPFS_GATEWAY_STREAM_SMALL_BODIES";

#[derive(Debug, Parser)]
#[command(author, version, about = "Local Freedom IPFS gateway")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    addr: SocketAddr,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    import_car: Option<PathBuf>,
    #[arg(long)]
    export_car: Option<PathBuf>,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    online: bool,
    #[arg(long, default_value = DEFAULT_DELEGATED_ROUTER)]
    delegated_router: String,
    #[arg(long, value_enum, default_value_t = RoutingMode::Auto)]
    routing_mode: RoutingMode,
    #[arg(long, default_value_t = DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS)]
    max_concurrent_requests: usize,
    #[arg(long, default_value_t = DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES)]
    small_body_cache_max_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_DHT_QUERY_TIMEOUT.as_secs())]
    dht_query_timeout_secs: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_DHT_PROVIDERS)]
    dht_max_providers: usize,
    #[arg(long)]
    trace_output: Option<PathBuf>,
    #[arg(long)]
    trace_filter: Option<String>,
    #[arg(long)]
    trace_span_list: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RoutingMode {
    Auto,
    Delegated,
    LightDht,
    Offline,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing(
        args.trace_output.as_deref(),
        args.trace_filter.as_deref(),
        args.trace_span_list,
    )?;
    let start_online_gateway = should_start_online_gateway(&args);
    let store = if let Some(path) = args.db {
        SqliteBlockStore::open(path, 256 * 1024 * 1024)?
    } else {
        SqliteBlockStore::in_memory(256 * 1024 * 1024)?
    };

    if let Some(car_path) = args.import_car {
        let bytes = fs::read(&car_path).with_context(|| format!("read {}", car_path.display()))?;
        let imported = store.import_car(&bytes)?;
        eprintln!("imported {} CAR blocks", imported.len());
    }

    if let Some(car_path) = args.export_car {
        let bytes = store.export_car()?;
        fs::write(&car_path, bytes).with_context(|| format!("write {}", car_path.display()))?;
        eprintln!("exported cache CAR to {}", car_path.display());
    }

    if let Some(root) = args.root {
        let root = parse_cid(&root)?;
        eprintln!("root: {root}");
    }

    let gateway_config = GatewayConfig::new(args.max_concurrent_requests)
        .with_small_body_cache_max_bytes(args.small_body_cache_max_bytes)
        .with_html_prefetch(gateway_html_prefetch_config())
        .with_html_range_warm(gateway_html_range_warm_config())
        .with_raw_link_tsize_fast_headers(raw_link_tsize_fast_headers_enabled())
        .with_stream_small_bodies(stream_small_bodies_enabled());
    let router = if start_online_gateway {
        let delegated_routers = args.delegated_router.clone();
        let delegated_router_endpoints = delegated_router_endpoints(&delegated_routers);
        let delegated = delegated_routing_client(delegated_router_endpoints.clone());
        let dht = light_dht_client(args.dht_query_timeout_secs, args.dht_max_providers);
        let routing = match args.routing_mode {
            RoutingMode::Auto => {
                ProviderRoutingClient::from(AutoRoutingClient::new(delegated, dht.clone()))
            }
            RoutingMode::Delegated => ProviderRoutingClient::from(delegated),
            RoutingMode::LightDht => ProviderRoutingClient::from(dht.clone()),
            RoutingMode::Offline => ProviderRoutingClient::Offline,
        };
        let provider = FetchingBlockProvider::new(store.clone(), routing);
        let name_resolver = CachedNameResolver::new(PersistentNameResolver::new(
            DefaultNameResolver::new(
                CloudflareDohResolver::default(),
                ipns_resolver(args.routing_mode, delegated_router_endpoints, dht),
            ),
            store,
        ));
        router_with_provider_and_name_resolver_config(
            Arc::new(provider),
            Arc::new(name_resolver),
            gateway_config,
        )
    } else {
        router_with_provider_and_name_resolver_config(
            Arc::new(store.clone()),
            Arc::new(PersistentNameResolver::cache_only(store)),
            gateway_config,
        )
    };
    serve_router(router, args.addr).await?;
    Ok(())
}

fn init_tracing(
    trace_output: Option<&std::path::Path>,
    trace_filter: Option<&str>,
    trace_span_list: bool,
) -> Result<()> {
    let filter = if let Some(trace_filter) = trace_filter {
        tracing_subscriber::EnvFilter::try_new(trace_filter)
            .with_context(|| format!("parse trace filter {trace_filter:?}"))?
    } else if trace_output.is_some() {
        tracing_subscriber::EnvFilter::new(DEFAULT_TRACE_FILTER)
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"))
    };

    if let Some(path) = trace_output {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open trace output {}", path.display()))?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(trace_span_list)
            .with_writer(TraceFileWriter(Arc::new(file)))
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}

#[derive(Clone)]
struct TraceFileWriter(Arc<fs::File>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceFileWriter {
    type Writer = &'a fs::File;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.as_ref()
    }
}

async fn serve_router(router: Router, addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!("gateway listening on http://{bound}");
    axum::serve(listener, router).await
}

fn should_start_online_gateway(args: &Args) -> bool {
    args.online && args.routing_mode != RoutingMode::Offline
}

fn gateway_html_prefetch_config() -> GatewayHtmlPrefetchConfig {
    let max_assets = env_usize(HTML_PREFETCH_MAX_ASSETS_ENV, 0);
    let max_bytes = env_u64(HTML_PREFETCH_MAX_BYTES_ENV, 64 * 1024);
    let concurrency = env_usize(HTML_PREFETCH_CONCURRENCY_ENV, 2);
    GatewayHtmlPrefetchConfig::new(max_assets, max_bytes, concurrency)
}

fn gateway_html_range_warm_config() -> GatewayHtmlRangeWarmConfig {
    let max_bytes = env_u64(HTML_RANGE_WARM_MAX_BYTES_ENV, 0);
    let concurrency = env_usize(HTML_RANGE_WARM_CONCURRENCY_ENV, 1);
    GatewayHtmlRangeWarmConfig::new(max_bytes, concurrency)
}

fn raw_link_tsize_fast_headers_enabled() -> bool {
    std::env::var_os(RAW_LINK_TSIZE_FAST_HEADERS_ENV).is_some()
}

fn stream_small_bodies_enabled() -> bool {
    std::env::var_os(STREAM_SMALL_BODIES_ENV).is_some()
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

fn light_dht_client(dht_query_timeout_secs: u64, dht_max_providers: usize) -> LightDhtClient {
    LightDhtClient::default()
        .with_query_timeout(Duration::from_secs(dht_query_timeout_secs))
        .with_max_providers(dht_max_providers)
}

fn delegated_routing_client(delegated_routers: Vec<String>) -> DelegatedRoutingClient {
    DelegatedRoutingClient::with_endpoints(delegated_routers)
}

fn delegated_router_endpoints(delegated_routers: &str) -> Vec<String> {
    delegated_routers
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect()
}

fn ipns_resolver(
    routing_mode: RoutingMode,
    delegated_routers: Vec<String>,
    dht: LightDhtClient,
) -> Arc<dyn IpnsResolver> {
    match routing_mode {
        RoutingMode::Auto => Arc::new(FallbackIpnsResolver::new(
            DelegatedIpnsResolver::with_endpoints(delegated_routers),
            DhtIpnsResolver::new(dht),
        )),
        RoutingMode::Delegated => {
            Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers))
        }
        RoutingMode::LightDht => Arc::new(DhtIpnsResolver::new(dht)),
        RoutingMode::Offline => Arc::new(OfflineIpnsResolver),
    }
}

#[derive(Debug, Clone)]
struct OfflineIpnsResolver;

#[async_trait::async_trait]
impl IpnsResolver for OfflineIpnsResolver {
    async fn resolve_ipns(&self, name: &str) -> freedom_ipfs_namesys::Result<IpnsRecord> {
        Err(NamesysError::NotFound(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_comma_separated_delegated_router_endpoints() {
        assert_eq!(
            delegated_router_endpoints(" https://one.example/routing/v1, ,https://two.example "),
            vec![
                "https://one.example/routing/v1".to_string(),
                "https://two.example".to_string()
            ]
        );
    }

    #[test]
    fn offline_routing_mode_disables_online_gateway() {
        let args = Args::try_parse_from([
            "freedom-ipfs-gateway",
            "--online",
            "--routing-mode",
            "offline",
        ])
        .expect("parse offline routing mode");

        assert!(!should_start_online_gateway(&args));
    }

    #[test]
    fn online_gateway_requires_online_flag() {
        let args = Args::try_parse_from(["freedom-ipfs-gateway", "--routing-mode", "auto"])
            .expect("parse auto routing mode");

        assert!(!should_start_online_gateway(&args));
    }

    #[test]
    fn auto_routing_mode_starts_online_gateway_when_enabled() {
        let args =
            Args::try_parse_from(["freedom-ipfs-gateway", "--online", "--routing-mode", "auto"])
                .expect("parse online auto routing mode");

        assert!(should_start_online_gateway(&args));
    }
}
