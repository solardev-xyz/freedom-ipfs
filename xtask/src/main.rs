use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use freedom_ipfs_core::{cid_from_data, encode_car_v1, CarBlock, CODEC_DAG_PB, CODEC_RAW};
use prost::Message;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    command: XtaskCommand,
}

#[derive(Debug, Subcommand)]
enum XtaskCommand {
    BuildXcframework,
    VerifyXcframework,
    GenerateMobileWebFixture {
        #[arg(long)]
        car: PathBuf,
        #[arg(long)]
        corpus: PathBuf,
        #[arg(long, default_value_t = 600_000)]
        bytes: usize,
        #[arg(long, default_value_t = 262_100)]
        range_start: u64,
        #[arg(long, default_value_t = 300)]
        range_len: u64,
        #[arg(long, default_value = DEFAULT_MOBILE_WEB_FIXTURE_CASE_ID)]
        case_id: String,
    },
    ValidateIosDeviceEvidence {
        #[arg(default_value = "docs/ios-device-evidence-template.csv")]
        path: PathBuf,
        #[arg(long)]
        filled: bool,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        XtaskCommand::BuildXcframework => build_xcframework(),
        XtaskCommand::VerifyXcframework => verify_xcframework_command(),
        XtaskCommand::GenerateMobileWebFixture {
            car,
            corpus,
            bytes,
            range_start,
            range_len,
            case_id,
        } => generate_mobile_web_fixture(&car, &corpus, bytes, range_start, range_len, &case_id),
        XtaskCommand::ValidateIosDeviceEvidence { path, filled } => {
            validate_ios_device_evidence(&path, filled)
        }
    }
}

const UNIXFS_CHUNK_SIZE: usize = 256 * 1024;
const MOBILE_FIXTURE_PATTERN: &[u8] = b"freedom-ipfs multiblock fixture payload\n";
const DEFAULT_MOBILE_WEB_FIXTURE_CASE_ID: &str = "multiblock-unixfs-range";

#[derive(Clone, PartialEq, Message)]
struct FixturePbNode {
    #[prost(bytes, optional, tag = "1")]
    data: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "2")]
    links: Vec<FixturePbLink>,
}

#[derive(Clone, PartialEq, Message)]
struct FixturePbLink {
    #[prost(bytes, optional, tag = "1")]
    hash: Option<Vec<u8>>,
    #[prost(string, optional, tag = "2")]
    name: Option<String>,
    #[prost(uint64, optional, tag = "3")]
    tsize: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct FixtureUnixfsData {
    #[prost(enumeration = "FixtureUnixfsDataType", optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(bytes, optional, tag = "2")]
    data: Option<Vec<u8>>,
    #[prost(uint64, optional, tag = "3")]
    filesize: Option<u64>,
    #[prost(uint64, repeated, tag = "4")]
    blocksizes: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum FixtureUnixfsDataType {
    File = 2,
}

fn generate_mobile_web_fixture(
    car_path: &Path,
    corpus_path: &Path,
    bytes: usize,
    range_start: u64,
    range_len: u64,
    case_id: &str,
) -> Result<()> {
    if bytes == 0 {
        bail!("--bytes must be greater than zero");
    }
    if bytes <= UNIXFS_CHUNK_SIZE {
        bail!(
            "--bytes must be greater than the UnixFS chunk size ({UNIXFS_CHUNK_SIZE}) for a multi-block fixture"
        );
    }
    if range_len == 0 {
        bail!("--range-len must be greater than zero");
    }
    let range_end = range_start
        .checked_add(range_len - 1)
        .context("range end overflow")?;
    if range_end >= bytes as u64 {
        bail!("range {range_start}-{range_end} is outside generated fixture length {bytes}");
    }

    let payload = deterministic_fixture_payload(bytes);
    let mut blocks = Vec::new();
    let mut links = Vec::new();
    let mut blocksizes = Vec::new();
    for chunk in payload.chunks(UNIXFS_CHUNK_SIZE) {
        let data = chunk.to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        blocksizes.push(data.len() as u64);
        links.push(FixturePbLink {
            hash: Some(cid.to_bytes()),
            name: Some(String::new()),
            tsize: Some(data.len() as u64),
        });
        blocks.push(CarBlock { cid, data });
    }

    let root_data = FixturePbNode {
        data: Some(
            FixtureUnixfsData {
                r#type: Some(FixtureUnixfsDataType::File as i32),
                data: Some(Vec::new()),
                filesize: Some(bytes as u64),
                blocksizes,
            }
            .encode_to_vec(),
        ),
        links,
    }
    .encode_to_vec();
    let root = cid_from_data(CODEC_DAG_PB, &root_data);
    blocks.insert(
        0,
        CarBlock {
            cid: root,
            data: root_data,
        },
    );

    write_parent_dir(car_path)?;
    fs::write(car_path, encode_car_v1(&blocks))
        .with_context(|| format!("write {}", car_path.display()))?;

    write_parent_dir(corpus_path)?;
    let range_cases = mobile_web_fixture_range_cases(case_id, bytes as u64, range_start, range_len);
    let corpus = mobile_web_fixture_corpus(&root.to_string(), bytes as u64, case_id, &range_cases);
    fs::write(corpus_path, corpus).with_context(|| format!("write {}", corpus_path.display()))?;

    println!("root_cid: {root}");
    println!("car: {}", car_path.display());
    println!("corpus: {}", corpus_path.display());
    println!("blocks: {}", blocks.len());
    println!("bytes: {bytes}");
    println!(
        "case {}: full response bytes={bytes}",
        derived_fixture_case_id(case_id, "full")
    );
    for case in range_cases {
        println!("case {}: bytes={}-{}", case.id, case.start, case.end);
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct FixtureRangeCase {
    id: String,
    description: String,
    start: u64,
    end: u64,
}

fn mobile_web_fixture_range_cases(
    case_id: &str,
    bytes: u64,
    range_start: u64,
    range_len: u64,
) -> Vec<FixtureRangeCase> {
    let range_end = range_start + range_len - 1;
    let max_start = bytes - range_len;
    let boundary_start = (UNIXFS_CHUNK_SIZE as u64)
        .saturating_sub(range_len / 2)
        .min(max_start);
    let boundary_end = boundary_start + range_len - 1;
    let suffix_start = bytes - range_len;
    vec![
        FixtureRangeCase {
            id: case_id.to_string(),
            description:
                "Configured deep range from a deterministic multi-block DAG-PB UnixFS file."
                    .to_string(),
            start: range_start,
            end: range_end,
        },
        FixtureRangeCase {
            id: derived_fixture_case_id(case_id, "prefix-range"),
            description: "Prefix byte range from a deterministic multi-block DAG-PB UnixFS file."
                .to_string(),
            start: 0,
            end: range_len - 1,
        },
        FixtureRangeCase {
            id: derived_fixture_case_id(case_id, "boundary-range"),
            description: "Byte range crossing the first UnixFS chunk boundary.".to_string(),
            start: boundary_start,
            end: boundary_end,
        },
        FixtureRangeCase {
            id: derived_fixture_case_id(case_id, "suffix-range"),
            description: "Suffix byte range from a deterministic multi-block DAG-PB UnixFS file."
                .to_string(),
            start: suffix_start,
            end: bytes - 1,
        },
    ]
}

fn derived_fixture_case_id(case_id: &str, suffix: &str) -> String {
    if case_id == DEFAULT_MOBILE_WEB_FIXTURE_CASE_ID {
        format!("multiblock-unixfs-{suffix}")
    } else {
        format!("{case_id}-{suffix}")
    }
}

fn mobile_web_fixture_corpus(
    root: &str,
    bytes: u64,
    case_id: &str,
    cases: &[FixtureRangeCase],
) -> String {
    let mut corpus = String::from("{\n  \"entries\": [\n");
    corpus.push_str(&format!(
        concat!(
            "    {{\n",
            "      \"id\": \"{}\",\n",
            "      \"description\": \"Full deterministic multi-block DAG-PB UnixFS file generated by xtask.\",\n",
            "      \"path\": \"/ipfs/{}\",\n",
            "      \"expect_status\": 200,\n",
            "      \"min_bytes\": {},\n",
            "      \"max_ttfb_ms\": 5000\n",
            "    }}"
        ),
        escape_json(&derived_fixture_case_id(case_id, "full")),
        root,
        bytes
    ));
    for case in cases {
        corpus.push_str(",\n");
        let range_len = case.end - case.start + 1;
        corpus.push_str(&format!(
            concat!(
                "    {{\n",
                "      \"id\": \"{}\",\n",
                "      \"description\": \"{}\",\n",
                "      \"path\": \"/ipfs/{}\",\n",
                "      \"range\": \"bytes={}-{}\",\n",
                "      \"expect_status\": 206,\n",
                "      \"expect_content_range_prefix\": \"bytes {}-{}/{}\",\n",
                "      \"min_bytes\": {},\n",
                "      \"max_ttfb_ms\": 5000\n",
                "    }}"
            ),
            escape_json(&case.id),
            escape_json(&case.description),
            root,
            case.start,
            case.end,
            case.start,
            case.end,
            bytes,
            range_len
        ));
    }
    corpus.push_str("\n  ]\n}\n");
    corpus
}

fn deterministic_fixture_payload(bytes: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(bytes);
    while payload.len() < bytes {
        payload.extend_from_slice(MOBILE_FIXTURE_PATTERN);
    }
    payload.truncate(bytes);
    payload
}

fn write_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    Ok(())
}

fn escape_json(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

const IOS_DEVICE_EVIDENCE_HEADER: [&str; 25] = [
    "case_id",
    "device_model",
    "ios_version",
    "app_commit",
    "freedom_ipfs_commit",
    "bee_commit",
    "xcframework_artifact",
    "bee_state",
    "cache_state",
    "routing_mode",
    "network_type",
    "result",
    "rss_baseline_mib",
    "rss_idle_mib",
    "rss_idle_delta_mib",
    "rss_peak_mib",
    "cpu_idle_percent",
    "network_idle_bytes_per_minute",
    "first_byte_ms",
    "complete_load_ms",
    "retrieval_delta",
    "routing_delta",
    "active_preloads",
    "trace_links",
    "notes",
];

const IOS_DEVICE_IPFS_CASES: [&str; 9] = [
    "cold_idle",
    "vitalik_eth",
    "daicowtf_eth",
    "dnslink_ipns",
    "byte_range",
    "background_foreground",
    "low_memory",
    "network_change",
    "retrieval_soak",
];

const IOS_DEVICE_CASES: [&str; 10] = [
    "cold_idle",
    "vitalik_eth",
    "daicowtf_eth",
    "dnslink_ipns",
    "byte_range",
    "background_foreground",
    "low_memory",
    "network_change",
    "retrieval_soak",
    "cold_idle_baseline_bee_only",
];

const IOS_DEVICE_REQUIRED_ROWS: [(&str, &str); 19] = [
    ("cold_idle", "on"),
    ("cold_idle", "off"),
    ("vitalik_eth", "on"),
    ("vitalik_eth", "off"),
    ("daicowtf_eth", "on"),
    ("daicowtf_eth", "off"),
    ("dnslink_ipns", "on"),
    ("dnslink_ipns", "off"),
    ("byte_range", "on"),
    ("byte_range", "off"),
    ("background_foreground", "on"),
    ("background_foreground", "off"),
    ("low_memory", "on"),
    ("low_memory", "off"),
    ("network_change", "on"),
    ("network_change", "off"),
    ("retrieval_soak", "on"),
    ("retrieval_soak", "off"),
    ("cold_idle_baseline_bee_only", "baseline_on_ipfs_off"),
];

fn validate_ios_device_evidence(path: &Path, filled: bool) -> Result<()> {
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let rows =
        validate_ios_device_evidence_contents(&path.display().to_string(), &contents, filled)?;
    let mode = if filled {
        "filled evidence"
    } else {
        "template"
    };
    println!("validated {rows} {mode} rows in {}", path.display());
    Ok(())
}

fn validate_ios_device_evidence_contents(
    label: &str,
    contents: &str,
    filled: bool,
) -> Result<usize> {
    let mut records = Vec::new();
    for (line_index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields =
            parse_csv_record(line).with_context(|| format!("{label}:{}", line_index + 1))?;
        records.push((line_index + 1, fields));
    }

    let Some((_, header)) = records.first() else {
        bail!("{label} is empty");
    };
    let expected_header = IOS_DEVICE_EVIDENCE_HEADER
        .iter()
        .map(|field| field.to_string())
        .collect::<Vec<_>>();
    if header != &expected_header {
        bail!(
            "{label}: header mismatch; expected {}",
            IOS_DEVICE_EVIDENCE_HEADER.join(",")
        );
    }
    if records.len() == 1 {
        bail!("{label}: no evidence rows");
    }

    let mut seen_rows = BTreeSet::new();
    for (line_no, fields) in records.iter().skip(1) {
        if fields.len() != IOS_DEVICE_EVIDENCE_HEADER.len() {
            bail!(
                "{label}:{line_no}: expected {} columns, found {}",
                IOS_DEVICE_EVIDENCE_HEADER.len(),
                fields.len()
            );
        }
        validate_ios_device_evidence_row(label, *line_no, fields, filled)?;
        seen_rows.insert((fields[0].trim().to_string(), fields[7].trim().to_string()));
    }

    for (case_id, bee_state) in IOS_DEVICE_REQUIRED_ROWS {
        if !seen_rows.contains(&(case_id.to_string(), bee_state.to_string())) {
            bail!("{label}: missing required case_id {case_id} with bee_state {bee_state}");
        }
    }

    Ok(records.len() - 1)
}

fn validate_ios_device_evidence_row(
    label: &str,
    line_no: usize,
    fields: &[String],
    filled: bool,
) -> Result<()> {
    let case_id = fields[0].trim();
    ensure_allowed(label, line_no, "case_id", case_id, &IOS_DEVICE_CASES)?;
    ensure_allowed(
        label,
        line_no,
        "bee_state",
        fields[7].trim(),
        &["on", "off", "baseline_on_ipfs_off"],
    )?;
    ensure_allowed(
        label,
        line_no,
        "cache_state",
        fields[8].trim(),
        &["clean", "warm", "mixed"],
    )?;
    ensure_allowed(
        label,
        line_no,
        "routing_mode",
        fields[9].trim(),
        &["auto", "delegated", "light_dht", "offline", "none"],
    )?;
    ensure_allowed(
        label,
        line_no,
        "network_type",
        fields[10].trim(),
        &["wifi", "cellular", "wifi_to_cellular", "offline", "mixed"],
    )?;
    let result = fields[11].trim();
    ensure_allowed(
        label,
        line_no,
        "result",
        result,
        &["pass", "fail", "pass/fail", "pending"],
    )?;
    if filled && matches!(result, "pass/fail" | "pending") {
        bail!("{label}:{line_no}: filled evidence row still has placeholder result {result}");
    }

    validate_ios_device_case_matrix_row(label, line_no, fields)?;

    for index in [12, 13, 14, 15, 16, 17, 18, 19] {
        parse_optional_f64(
            label,
            line_no,
            IOS_DEVICE_EVIDENCE_HEADER[index],
            &fields[index],
        )?;
    }
    parse_optional_u64(label, line_no, "active_preloads", &fields[22])?;

    if filled && result == "pass" {
        validate_passing_ios_device_evidence_row(label, line_no, fields)?;
    }
    if filled && result == "fail" && fields[23].trim().is_empty() && fields[24].trim().is_empty() {
        bail!("{label}:{line_no}: fail rows must include trace_links or notes");
    }

    Ok(())
}

fn validate_ios_device_case_matrix_row(
    label: &str,
    line_no: usize,
    fields: &[String],
) -> Result<()> {
    let case_id = fields[0].trim();
    let bee_state = fields[7].trim();
    let routing_mode = fields[9].trim();

    if case_id == "cold_idle_baseline_bee_only" {
        if bee_state != "baseline_on_ipfs_off" {
            bail!("{label}:{line_no}: cold_idle_baseline_bee_only must use bee_state baseline_on_ipfs_off");
        }
        if routing_mode != "none" {
            bail!("{label}:{line_no}: cold_idle_baseline_bee_only must use routing_mode none");
        }
        return Ok(());
    }

    if bee_state == "baseline_on_ipfs_off" {
        bail!("{label}:{line_no}: bee_state baseline_on_ipfs_off is only valid for cold_idle_baseline_bee_only");
    }
    if !IOS_DEVICE_IPFS_CASES.contains(&case_id) {
        bail!("{label}:{line_no}: unsupported IPFS-enabled case_id {case_id}");
    }
    if routing_mode == "none" {
        bail!("{label}:{line_no}: IPFS-enabled rows must use an IPFS routing mode");
    }
    Ok(())
}

fn validate_passing_ios_device_evidence_row(
    label: &str,
    line_no: usize,
    fields: &[String],
) -> Result<()> {
    for index in [1, 2, 3, 4, 5, 6, 12, 13, 14, 15, 16, 17, 22, 23] {
        if fields[index].trim().is_empty() {
            bail!(
                "{label}:{line_no}: pass row is missing {}",
                IOS_DEVICE_EVIDENCE_HEADER[index]
            );
        }
    }

    let rss_idle_delta = parse_required_f64(label, line_no, "rss_idle_delta_mib", &fields[14])?;
    if fields[9].trim() != "none" && rss_idle_delta > 60.0 {
        bail!("{label}:{line_no}: pass row exceeds 60 MiB RSS idle delta ({rss_idle_delta})");
    }
    let cpu_idle = parse_required_f64(label, line_no, "cpu_idle_percent", &fields[16])?;
    if cpu_idle > 1.0 {
        bail!("{label}:{line_no}: pass row exceeds 1% idle CPU ({cpu_idle})");
    }
    let active_preloads = parse_required_u64(label, line_no, "active_preloads", &fields[22])?;
    if active_preloads != 0 {
        bail!("{label}:{line_no}: pass row has active_preloads={active_preloads}");
    }

    if requires_retrieval_measurements(fields[0].trim()) {
        for index in [18, 19, 20, 21] {
            if fields[index].trim().is_empty() {
                bail!(
                    "{label}:{line_no}: pass row is missing {}",
                    IOS_DEVICE_EVIDENCE_HEADER[index]
                );
            }
        }
        let first_byte = parse_required_f64(label, line_no, "first_byte_ms", &fields[18])?;
        let complete = parse_required_f64(label, line_no, "complete_load_ms", &fields[19])?;
        if first_byte <= 0.0 {
            bail!("{label}:{line_no}: first_byte_ms must be positive");
        }
        if complete < first_byte {
            bail!("{label}:{line_no}: complete_load_ms is smaller than first_byte_ms");
        }
    }

    Ok(())
}

fn requires_retrieval_measurements(case_id: &str) -> bool {
    matches!(
        case_id,
        "vitalik_eth"
            | "daicowtf_eth"
            | "dnslink_ipns"
            | "byte_range"
            | "background_foreground"
            | "low_memory"
            | "network_change"
            | "retrieval_soak"
    )
}

fn ensure_allowed(
    label: &str,
    line_no: usize,
    field: &str,
    value: &str,
    allowed: &[&str],
) -> Result<()> {
    if allowed.contains(&value) {
        return Ok(());
    }
    bail!(
        "{label}:{line_no}: invalid {field} value {value:?}; expected one of {}",
        allowed.join(", ")
    )
}

fn parse_optional_f64(label: &str, line_no: usize, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Ok(());
    }
    parse_required_f64(label, line_no, field, value).map(|_| ())
}

fn parse_required_f64(label: &str, line_no: usize, field: &str, value: &str) -> Result<f64> {
    value
        .trim()
        .parse::<f64>()
        .with_context(|| format!("{label}:{line_no}: {field} must be numeric"))
}

fn parse_optional_u64(label: &str, line_no: usize, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Ok(());
    }
    parse_required_u64(label, line_no, field, value).map(|_| ())
}

fn parse_required_u64(label: &str, line_no: usize, field: &str, value: &str) -> Result<u64> {
    value
        .trim()
        .parse::<u64>()
        .with_context(|| format!("{label}:{line_no}: {field} must be an unsigned integer"))
}

fn parse_csv_record(line: &str) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                let _ = chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(field);
                field = String::new();
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        bail!("unterminated quoted CSV field");
    }
    fields.push(field);
    Ok(fields)
}

fn build_xcframework() -> Result<()> {
    if env::consts::OS != "macos" {
        bail!(
            "build-xcframework requires macOS with Xcode command line tools; current host is {}",
            env::consts::OS
        );
    }

    let targets = [
        "aarch64-apple-ios",
        "aarch64-apple-ios-sim",
        "x86_64-apple-ios",
    ];

    for target in targets {
        let status = Command::new("rustup")
            .args(["target", "add", target])
            .status()
            .with_context(|| format!("rustup target add {target}"))?;
        if !status.success() {
            bail!("rustup target add {target} failed");
        }

        let status = Command::new("cargo")
            .args([
                "build",
                "-p",
                "freedom-ipfs-mobile",
                "--release",
                "--target",
                target,
            ])
            .env("IPHONEOS_DEPLOYMENT_TARGET", "16.0")
            .status()
            .with_context(|| format!("cargo build for {target}"))?;
        if !status.success() {
            bail!("cargo build for {target} failed");
        }
    }

    let out_dir = PathBuf::from("target/ios-xcframework");
    let sim_dir = out_dir.join("simulator");
    let headers_dir = out_dir.join("headers");
    fs::create_dir_all(&sim_dir).context("create simulator output directory")?;
    stage_headers(&headers_dir)?;

    let device_lib = staticlib("aarch64-apple-ios");
    let sim_arm64_lib = staticlib("aarch64-apple-ios-sim");
    let sim_x86_64_lib = staticlib("x86_64-apple-ios");
    let sim_universal_lib = sim_dir.join("libfreedom_ipfs_mobile.a");
    let framework = out_dir.join("FreedomIpfs.xcframework");
    if framework.exists() {
        fs::remove_dir_all(&framework).context("remove previous xcframework")?;
    }

    run(
        Command::new("lipo")
            .args(["-create", "-output"])
            .arg(&sim_universal_lib)
            .arg(&sim_arm64_lib)
            .arg(&sim_x86_64_lib),
        "lipo simulator static libraries",
    )?;

    run(
        Command::new("xcodebuild")
            .arg("-create-xcframework")
            .arg("-library")
            .arg(&device_lib)
            .arg("-headers")
            .arg(&headers_dir)
            .arg("-library")
            .arg(&sim_universal_lib)
            .arg("-headers")
            .arg(&headers_dir)
            .arg("-output")
            .arg(&framework),
        "xcodebuild -create-xcframework",
    )?;

    verify_xcframework(&framework, false)?;
    println!("created {}", framework.display());
    Ok(())
}

fn verify_xcframework_command() -> Result<()> {
    if env::consts::OS != "macos" {
        bail!(
            "verify-xcframework requires macOS with Xcode command line tools; current host is {}",
            env::consts::OS
        );
    }
    verify_xcframework(
        &PathBuf::from("target/ios-xcframework/FreedomIpfs.xcframework"),
        true,
    )
}

fn stage_headers(headers_dir: &Path) -> Result<()> {
    if headers_dir.exists() {
        fs::remove_dir_all(headers_dir).context("remove previous header staging directory")?;
    }
    fs::create_dir_all(headers_dir).context("create header staging directory")?;
    fs::copy(
        "ffi/include/freedom_ipfs.h",
        headers_dir.join("freedom_ipfs.h"),
    )
    .context("stage freedom_ipfs.h")?;
    fs::copy(
        "ffi/modulemap/module.modulemap",
        headers_dir.join("module.modulemap"),
    )
    .context("stage module.modulemap")?;
    Ok(())
}

fn verify_xcframework(framework: &Path, run_simulator_smoke: bool) -> Result<()> {
    if !framework.exists() {
        bail!("{} does not exist", framework.display());
    }
    let info_plist = framework.join("Info.plist");
    if !info_plist.exists() {
        bail!("{} is missing", info_plist.display());
    }

    let libraries = find_named_files(framework, "libfreedom_ipfs_mobile.a")?;
    if libraries.len() < 2 {
        bail!(
            "expected device and simulator static libraries in {}, found {}",
            framework.display(),
            libraries.len()
        );
    }
    let headers = find_named_files(framework, "freedom_ipfs.h")?;
    if headers.len() < 2 {
        bail!(
            "expected headers in each XCFramework slice in {}, found {}",
            framework.display(),
            headers.len()
        );
    }
    let modulemaps = find_named_files(framework, "module.modulemap")?;
    if modulemaps.len() < 2 {
        bail!(
            "expected module maps in each XCFramework slice in {}, found {}",
            framework.display(),
            modulemaps.len()
        );
    }

    for library in &libraries {
        verify_exported_symbols(library)?;
    }

    if run_simulator_smoke {
        verify_swift_simulator_smoke(framework, &libraries)?;
    }

    println!("verified {}", framework.display());
    Ok(())
}

fn find_named_files(root: &Path, name: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_named_files(root, name, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_named_files(path: &Path, name: &str, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_named_files(&path, name, files)?;
        } else if path.file_name().and_then(|file_name| file_name.to_str()) == Some(name) {
            files.push(path);
        }
    }
    Ok(())
}

fn verify_exported_symbols(library: &Path) -> Result<()> {
    let llvm_nm = rust_llvm_nm()?;
    let output = Command::new(&llvm_nm)
        .args(["--extern-only", "--defined-only"])
        .arg(library)
        .output()
        .with_context(|| format!("{} {}", llvm_nm.display(), library.display()))?;
    if !output.status.success() {
        bail!(
            "{} {} failed: {}",
            llvm_nm.display(),
            library.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for symbol in [
        "freedom_ipfs_version",
        "freedom_ipfs_node_new_with_data_dir",
        "freedom_ipfs_node_start_gateway_online_with_config_v2",
        "freedom_ipfs_node_restart_gateway_online_with_config_v2",
        "freedom_ipfs_node_enter_background",
        "freedom_ipfs_node_handle_low_memory",
        "freedom_ipfs_node_retrieval_stats",
        "freedom_ipfs_node_routing_stats",
        "freedom_ipfs_node_active_preload_count",
        "freedom_ipfs_node_diagnostics",
        "freedom_ipfs_node_progress_snapshot_json",
        "freedom_ipfs_node_clear_progress",
    ] {
        if !stdout.contains(symbol) {
            bail!("{} does not export {symbol}", library.display());
        }
    }
    Ok(())
}

fn rust_llvm_nm() -> Result<PathBuf> {
    let sysroot = command_stdout(
        Command::new("rustc").args(["--print", "sysroot"]),
        "rustc --print sysroot",
    )?;
    let host = rust_host_triple()?;
    let llvm_nm = Path::new(sysroot.trim())
        .join("lib")
        .join("rustlib")
        .join(host)
        .join("bin")
        .join("llvm-nm");
    if !llvm_nm.exists() {
        run(
            Command::new("rustup").args(["component", "add", "llvm-tools-preview"]),
            "rustup component add llvm-tools-preview",
        )?;
    }
    if !llvm_nm.exists() {
        bail!(
            "{} is missing after installing llvm-tools-preview",
            llvm_nm.display()
        );
    }
    Ok(llvm_nm)
}

fn rust_host_triple() -> Result<String> {
    let version = command_stdout(Command::new("rustc").arg("-vV"), "rustc -vV")?;
    for line in version.lines() {
        if let Some(host) = line.strip_prefix("host: ") {
            return Ok(host.trim().to_string());
        }
    }
    bail!("rustc -vV did not report a host triple")
}

fn verify_swift_simulator_smoke(framework: &Path, libraries: &[PathBuf]) -> Result<()> {
    let library = libraries
        .iter()
        .find(|library| library.to_string_lossy().contains("simulator"))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} is missing a simulator library slice",
                framework.display()
            )
        })?;
    let slice_dir = library
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", library.display()))?;
    let headers_dir = slice_dir.join("Headers");
    if !headers_dir.join("freedom_ipfs.h").exists() {
        bail!(
            "{} is missing the simulator slice freedom_ipfs.h header",
            headers_dir.display()
        );
    }
    if !headers_dir.join("module.modulemap").exists() {
        bail!(
            "{} is missing the simulator slice module.modulemap",
            headers_dir.display()
        );
    }

    let sdk_path = command_stdout(
        Command::new("xcrun").args(["--sdk", "iphonesimulator", "--show-sdk-path"]),
        "xcrun --sdk iphonesimulator --show-sdk-path",
    )?;
    let target = simulator_swift_target()?;
    let verify_dir = PathBuf::from("target/ios-xcframework/verify");
    if verify_dir.exists() {
        fs::remove_dir_all(&verify_dir).context("remove previous Swift verification directory")?;
    }
    fs::create_dir_all(&verify_dir).context("create Swift verification directory")?;
    let smoke = verify_dir.join("FreedomIpfsSmoke.swift");
    let fixture_bytes = b"simulator fixture";
    let fixture_cid = cid_from_data(CODEC_RAW, fixture_bytes);
    let fixture_car = encode_car_v1(&[CarBlock {
        cid: fixture_cid,
        data: fixture_bytes.to_vec(),
    }]);
    let fixture_car = format_swift_byte_array(&fixture_car);
    let fixture_body = format_swift_byte_array(fixture_bytes);
    let smoke_source = format!(
        r#"import Foundation
import FreedomIpfs

@main
enum FreedomIpfsSmoke {{
    static func main() async throws {{
        _ = FreedomIpfsReader.version
        let rejectedReader = try FreedomIpfsReader()
        do {{
            try rejectedReader.startGateway(address: "0.0.0.0:0")
            fatalError("non-loopback gateway bind unexpectedly succeeded")
        }} catch FreedomIpfsReaderError.startGatewayFailed {{
        }} catch {{
            fatalError("unexpected non-loopback gateway bind error: \(error)")
        }}
        guard rejectedReader.gatewayURL == nil else {{
            fatalError("rejected gateway unexpectedly reported a URL")
        }}

        let reader = try FreedomIpfsReader()
        try reader.importCar(Data([{fixture_car}]))
        try reader.startOnlineGateway(
            delegatedRouter: "http://127.0.0.1:9/routing/v1",
            routingMode: .delegated,
            maxConcurrentRequests: 1
        )
        guard reader.gatewayURL != nil else {{
            fatalError("gateway URL missing")
        }}
        guard reader.activePreloadCount == 0 else {{
            fatalError("unexpected active preloads before request")
        }}
        guard let url = reader.localGatewayURL(for: "/ipfs/{fixture_cid}") else {{
            fatalError("fixture gateway URL missing")
        }}
        guard reader.clearProgress() else {{
            fatalError("clear progress failed")
        }}
        let beforeDiagnostics = reader.diagnostics
        let (data, response) = try await URLSession.shared.data(from: url)
        guard (response as? HTTPURLResponse)?.statusCode == 200 else {{
            fatalError("fixture request failed")
        }}
        guard data == Data([{fixture_body}]) else {{
            fatalError("fixture body mismatch")
        }}
        let progressJSON = reader.progressSnapshotJSON
        guard let progressData = progressJSON.data(using: .utf8),
              let progressObject = try JSONSerialization.jsonObject(with: progressData) as? [String: Any],
              let progressEvents = progressObject["events"] as? [[String: Any]],
              progressEvents.contains(where: {{ event in
                  event["kind"] as? String == "gateway_request"
                      && event["path"] as? String == "/ipfs/{fixture_cid}"
                      && event["phase"] as? String == "completed"
              }}) else {{
            fatalError("progress snapshot missing completed fixture request: \(progressJSON)")
        }}
        guard reader.clearProgress() else {{
            fatalError("clear progress after request failed")
        }}
        let retrievalStats = reader.retrievalStats
        guard retrievalStats.cacheHits > 0,
              retrievalStats.httpProviderBlocks == 0,
              retrievalStats.bitswapBlocks == 0 else {{
            fatalError("unexpected retrieval stats: \(retrievalStats)")
        }}
        let routingStats = reader.routingStats
        guard routingStats.delegatedProviderLookups == 0,
              routingStats.dhtProviderLookups == 0 else {{
            fatalError("cached fixture unexpectedly routed: \(routingStats)")
        }}
        let diagnostics = reader.diagnostics
        guard diagnostics.stats.blockCount == 1,
              diagnostics.retrievalStats.cacheHits > 0,
              diagnostics.retrievalStats.httpProviderBlocks == 0,
              diagnostics.retrievalStats.bitswapBlocks == 0,
              diagnostics.routingStats.delegatedProviderLookups == 0,
              diagnostics.routingStats.dhtProviderLookups == 0,
              diagnostics.activePreloadCount == 0,
              diagnostics.isGatewayRunning,
              !diagnostics.isBackgrounded else {{
            fatalError("unexpected diagnostics snapshot: \(diagnostics)")
        }}
        let diagnosticsDelta = diagnostics.delta(since: beforeDiagnostics)
        guard diagnosticsDelta.retrievalStats.cacheHits > 0,
              diagnosticsDelta.retrievalStats.httpProviderBlocks == 0,
              diagnosticsDelta.retrievalStats.bitswapBlocks == 0,
              diagnosticsDelta.routingStats.delegatedProviderLookups == 0,
              diagnosticsDelta.routingStats.dhtProviderLookups == 0,
              diagnosticsDelta.activePreloadCount == 0,
              diagnosticsDelta.isGatewayRunning,
              !diagnosticsDelta.isBackgrounded else {{
            fatalError("unexpected diagnostics delta: \(diagnosticsDelta)")
        }}
        guard reader.enterBackground(),
              reader.diagnostics.isBackgrounded else {{
            fatalError("background lifecycle hook did not update diagnostics")
        }}
        guard reader.enterForeground(),
              !reader.diagnostics.isBackgrounded else {{
            fatalError("foreground lifecycle hook did not update diagnostics")
        }}
        guard reader.handleLowMemory(maxCacheBytes: 1024 * 1024) else {{
            fatalError("low-memory hook failed")
        }}
        guard reader.handleNetworkChange() else {{
            fatalError("network-change hook failed")
        }}
        try reader.setRoutingMode(
            .delegated,
            delegatedRouters: ["http://127.0.0.1:9/routing/v1"],
            maxConcurrentRequests: 1
        )
        guard reader.gatewayURL != nil,
              reader.diagnostics.isGatewayRunning,
              reader.activePreloadCount == 0 else {{
            fatalError("routing restart did not leave the gateway running")
        }}
        guard let restartedURL = reader.localGatewayURL(for: "/ipfs/{fixture_cid}") else {{
            fatalError("restarted fixture gateway URL missing")
        }}
        let (restartedData, restartedResponse) = try await URLSession.shared.data(from: restartedURL)
        guard (restartedResponse as? HTTPURLResponse)?.statusCode == 200,
              restartedData == Data([{fixture_body}]) else {{
            fatalError("fixture request after routing restart failed")
        }}
        guard reader.diagnostics.activePreloadCount == 0,
              !reader.diagnostics.isBackgrounded else {{
            fatalError("unexpected diagnostics after routing restart: \(reader.diagnostics)")
        }}
        try reader.setRoutingMode(.offline, maxConcurrentRequests: 1)
        guard reader.gatewayURL != nil,
              reader.diagnostics.isGatewayRunning,
              reader.activePreloadCount == 0 else {{
            fatalError("offline routing mode did not leave the gateway running")
        }}
        guard let offlineURL = reader.localGatewayURL(for: "/ipfs/{fixture_cid}") else {{
            fatalError("offline fixture gateway URL missing")
        }}
        let (offlineData, offlineResponse) = try await URLSession.shared.data(from: offlineURL)
        guard (offlineResponse as? HTTPURLResponse)?.statusCode == 200,
              offlineData == Data([{fixture_body}]) else {{
            fatalError("fixture request after offline routing restart failed")
        }}
        guard reader.routingStats == FreedomIpfsRoutingCounters(
            delegatedProviderLookups: 0,
            delegatedProviderResults: 0,
            delegatedProviderErrors: 0,
            dhtProviderLookups: 0,
            dhtProviderResults: 0,
            dhtProviderErrors: 0
        ) else {{
            fatalError("offline routing mode unexpectedly performed routing: \(reader.routingStats)")
        }}
        _ = reader.stopGateway()
    }}
}}
"#,
    );
    fs::write(&smoke, smoke_source).context("write Swift verification smoke source")?;

    let executable = verify_dir.join("FreedomIpfsSmoke");
    run(
        Command::new("xcrun")
            .args(["--sdk", "iphonesimulator", "swiftc"])
            .arg("-target")
            .arg(target)
            .arg("-sdk")
            .arg(sdk_path.trim())
            .arg("-I")
            .arg(&headers_dir)
            .arg("-L")
            .arg(slice_dir)
            .arg("-l")
            .arg("freedom_ipfs_mobile")
            .arg("-framework")
            .arg("SystemConfiguration")
            .arg("ffi/swift/FreedomIpfsReader.swift")
            .arg(&smoke)
            .arg("-o")
            .arg(&executable),
        "swiftc simulator link smoke",
    )?;

    run(
        Command::new("xcrun").args(["simctl", "bootstatus", "booted", "-b"]),
        "wait for booted iOS simulator",
    )?;
    let executable = fs::canonicalize(&executable)
        .with_context(|| format!("canonicalize {}", executable.display()))?;
    run(
        Command::new("xcrun")
            .args(["simctl", "spawn", "booted"])
            .arg(&executable),
        "simctl simulator gateway smoke",
    )?;

    verify_swift_simulator_app_smoke(
        &verify_dir,
        &headers_dir,
        slice_dir,
        sdk_path.trim(),
        target,
    )
}

fn verify_swift_simulator_app_smoke(
    verify_dir: &Path,
    headers_dir: &Path,
    slice_dir: &Path,
    sdk_path: &str,
    target: &str,
) -> Result<()> {
    let bundle_id = "xyz.floto.freedom-ipfs.AppSmoke";
    let app_dir = verify_dir.join("FreedomIpfsAppSmoke.app");
    if app_dir.exists() {
        fs::remove_dir_all(&app_dir).context("remove previous simulator app smoke bundle")?;
    }
    fs::create_dir_all(&app_dir).context("create simulator app smoke bundle")?;

    let html = br#"<!doctype html><html><head><meta charset="utf-8"><title>Freedom IPFS Smoke</title></head><body><main id="freedom-ipfs-smoke">Freedom IPFS App Smoke</main></body></html>"#;
    let cid = cid_from_data(CODEC_RAW, html);
    let car = encode_car_v1(&[CarBlock {
        cid,
        data: html.to_vec(),
    }]);
    let marker_name = "freedom-ipfs-app-smoke.ok";
    let app_source = format!(
        r#"import Darwin
import Foundation
import UIKit
import WebKit
import FreedomIpfs

@main
final class AppDelegate: UIResponder, UIApplicationDelegate {{
    var window: UIWindow?

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
    ) -> Bool {{
        let window = UIWindow(frame: UIScreen.main.bounds)
        window.rootViewController = SmokeViewController()
        window.makeKeyAndVisible()
        self.window = window
        return true
    }}
}}

final class SmokeViewController: UIViewController, WKNavigationDelegate {{
    private let webView = WKWebView(frame: .zero)
    private var reader: FreedomIpfsReader?

    override func viewDidLoad() {{
        super.viewDidLoad()
        webView.navigationDelegate = self
        webView.frame = view.bounds
        webView.autoresizingMask = [.flexibleWidth, .flexibleHeight]
        view.addSubview(webView)

        Task {{
            do {{
                let reader = try FreedomIpfsReader()
                self.reader = reader
                try reader.importCar(Data([{car}]))
                try reader.startGateway()
                guard reader.diagnostics.isGatewayRunning,
                      !reader.diagnostics.isBackgrounded else {{
                    throw SmokeError("unexpected initial app diagnostics")
                }}
                guard reader.enterBackground(),
                      reader.diagnostics.isBackgrounded else {{
                    throw SmokeError("background lifecycle hook failed")
                }}
                guard reader.enterForeground(),
                      !reader.diagnostics.isBackgrounded else {{
                    throw SmokeError("foreground lifecycle hook failed")
                }}
                guard reader.handleLowMemory(maxCacheBytes: 1024 * 1024) else {{
                    throw SmokeError("low-memory hook failed")
                }}
                guard reader.handleNetworkChange() else {{
                    throw SmokeError("network-change hook failed")
                }}
                guard let url = reader.localGatewayURL(for: "/ipfs/{cid}") else {{
                    throw SmokeError("fixture gateway URL missing")
                }}
                let (data, response) = try await URLSession.shared.data(from: url)
                guard (response as? HTTPURLResponse)?.statusCode == 200 else {{
                    throw SmokeError("fixture request failed")
                }}
                guard let html = String(data: data, encoding: .utf8),
                      html.contains("Freedom IPFS App Smoke") else {{
                    throw SmokeError("fixture body mismatch")
                }}
                webView.loadHTMLString(html, baseURL: reader.gatewayURL)
            }} catch {{
                finish("failed: \(error)")
            }}
        }}
    }}

    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {{
        webView.evaluateJavaScript("document.getElementById('freedom-ipfs-smoke')?.textContent") {{ result, error in
            if let error {{
                self.finish("failed: \(error)")
                return
            }}
            guard (result as? String) == "Freedom IPFS App Smoke" else {{
                self.finish("failed: rendered marker missing")
                return
            }}
            self.finish("ok")
        }}
    }}

    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {{
        finish("failed: \(error)")
    }}

    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) {{
        finish("failed: \(error)")
    }}

    private func finish(_ message: String) {{
        if let documents = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first {{
            let marker = documents.appendingPathComponent("{marker_name}")
            try? Data(message.utf8).write(to: marker)
        }}
        _ = reader?.stopGateway()
        exit(message == "ok" ? 0 : 1)
    }}
}}

struct SmokeError: Error, CustomStringConvertible {{
    let description: String

    init(_ description: String) {{
        self.description = description
    }}
}}
"#,
        car = format_swift_byte_array(&car),
        cid = cid,
        marker_name = marker_name
    );
    let app_source_path = verify_dir.join("FreedomIpfsAppSmoke.swift");
    fs::write(&app_source_path, app_source).context("write Swift simulator app smoke source")?;

    let info_plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleExecutable</key>
    <string>FreedomIpfsAppSmoke</string>
    <key>CFBundleIdentifier</key>
    <string>{bundle_id}</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>FreedomIpfsAppSmoke</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleVersion</key>
    <string>1</string>
    <key>LSRequiresIPhoneOS</key>
    <true/>
    <key>NSAppTransportSecurity</key>
    <dict>
        <key>NSAllowsLocalNetworking</key>
        <true/>
    </dict>
    <key>UIDeviceFamily</key>
    <array>
        <integer>1</integer>
    </array>
</dict>
</plist>
"#
    );
    fs::write(app_dir.join("Info.plist"), info_plist).context("write app smoke Info.plist")?;

    run(
        Command::new("xcrun")
            .args(["--sdk", "iphonesimulator", "swiftc"])
            .arg("-target")
            .arg(target)
            .arg("-sdk")
            .arg(sdk_path)
            .arg("-I")
            .arg(headers_dir)
            .arg("-L")
            .arg(slice_dir)
            .arg("-l")
            .arg("freedom_ipfs_mobile")
            .arg("-framework")
            .arg("SystemConfiguration")
            .arg("-framework")
            .arg("UIKit")
            .arg("-framework")
            .arg("WebKit")
            .arg("ffi/swift/FreedomIpfsReader.swift")
            .arg(&app_source_path)
            .arg("-o")
            .arg(app_dir.join("FreedomIpfsAppSmoke")),
        "swiftc simulator app smoke",
    )?;
    run(
        Command::new("codesign")
            .args(["--force", "--sign", "-"])
            .arg(&app_dir),
        "codesign simulator app smoke",
    )?;
    let _ = Command::new("xcrun")
        .args(["simctl", "uninstall", "booted", bundle_id])
        .status();
    run(
        Command::new("xcrun")
            .args(["simctl", "install", "booted"])
            .arg(&app_dir),
        "install simulator app smoke",
    )?;
    let app_container = command_stdout(
        Command::new("xcrun").args(["simctl", "get_app_container", "booted", bundle_id, "data"]),
        "get simulator app smoke container",
    )?;
    let marker = Path::new(app_container.trim())
        .join("Documents")
        .join(marker_name);
    if marker.exists() {
        fs::remove_file(&marker).with_context(|| format!("remove {}", marker.display()))?;
    }
    run(
        Command::new("xcrun").args(["simctl", "launch", "--console", "booted", bundle_id]),
        "launch simulator app smoke",
    )?;
    wait_for_app_smoke_marker(&marker)
}

fn wait_for_app_smoke_marker(marker: &Path) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if marker.exists() {
            let contents =
                fs::read_to_string(marker).with_context(|| format!("read {}", marker.display()))?;
            if contents.trim() == "ok" {
                return Ok(());
            }
            bail!("simulator app smoke failed: {}", contents.trim());
        }
        thread::sleep(Duration::from_millis(500));
    }
    bail!(
        "simulator app smoke did not write {} within 60 seconds",
        marker.display()
    )
}

fn simulator_swift_target() -> Result<&'static str> {
    match env::consts::ARCH {
        "aarch64" => Ok("arm64-apple-ios16.0-simulator"),
        "x86_64" => Ok("x86_64-apple-ios16.0-simulator"),
        arch => bail!("unsupported macOS host architecture for simulator Swift smoke: {arch}"),
    }
}

fn staticlib(target: &str) -> PathBuf {
    Path::new("target")
        .join(target)
        .join("release")
        .join("libfreedom_ipfs_mobile.a")
}

fn run(command: &mut Command, label: &str) -> Result<()> {
    let status = command.status().with_context(|| label.to_string())?;
    if !status.success() {
        bail!("{label} failed");
    }
    Ok(())
}

fn command_stdout(command: &mut Command, label: &str) -> Result<String> {
    let output = command.output().with_context(|| label.to_string())?;
    if !output.status.success() {
        bail!("{label} failed");
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn format_swift_byte_array(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(u8::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::parse_car_v1;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generates_multiblock_mobile_web_fixture() {
        let stamp = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(format!("freedom-ipfs-xtask-fixture-{stamp}"));
        let car = dir.join("fixture.car");
        let corpus = dir.join("corpus.json");

        generate_mobile_web_fixture(&car, &corpus, 600_000, 262_100, 300, "fixture-case").unwrap();

        let parsed = parse_car_v1(&fs::read(&car).unwrap()).unwrap();
        assert_eq!(parsed.blocks.len(), 4);
        assert_eq!(parsed.blocks[0].cid.codec(), CODEC_DAG_PB);
        assert_eq!(parsed.blocks[1].cid.codec(), CODEC_RAW);

        let corpus = fs::read_to_string(&corpus).unwrap();
        assert!(corpus.contains("fixture-case"));
        assert!(corpus.contains("fixture-case-full"));
        assert!(corpus.contains("fixture-case-prefix-range"));
        assert!(corpus.contains("fixture-case-boundary-range"));
        assert!(corpus.contains("fixture-case-suffix-range"));
        assert!(corpus.contains(&format!("/ipfs/{}", parsed.blocks[0].cid)));
        assert!(corpus.contains("\"expect_status\": 200"));
        assert!(corpus.contains("\"min_bytes\": 600000"));
        assert!(corpus.contains("bytes=262100-262399"));
        assert!(corpus.contains("bytes=0-299"));
        assert!(corpus.contains("bytes=261994-262293"));
        assert!(corpus.contains("bytes=599700-599999"));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn parses_quoted_csv_fields() {
        let fields = parse_csv_record(r#"case,"trace,with,commas","escaped "" quote""#).unwrap();
        assert_eq!(fields, ["case", "trace,with,commas", "escaped \" quote"]);
    }

    #[test]
    fn validates_checked_in_ios_device_evidence_template() {
        let template = include_str!("../../docs/ios-device-evidence-template.csv");
        let rows = validate_ios_device_evidence_contents("template", template, false).unwrap();
        assert_eq!(rows, IOS_DEVICE_REQUIRED_ROWS.len());
    }

    #[test]
    fn ios_device_evidence_requires_bee_comparison_rows() {
        let template = include_str!("../../docs/ios-device-evidence-template.csv");
        let reduced = template
            .lines()
            .filter(|line| !line.starts_with("vitalik_eth,,,,,,,off,"))
            .collect::<Vec<_>>()
            .join("\n");

        let error = validate_ios_device_evidence_contents("template", &reduced, false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing required case_id vitalik_eth with bee_state off"),
            "{error:#}"
        );
    }

    #[test]
    fn filled_pass_row_rejects_resource_target_miss() {
        let mut lines = vec![IOS_DEVICE_EVIDENCE_HEADER.join(",")];
        for (case_id, bee_state) in IOS_DEVICE_REQUIRED_ROWS {
            let mut row = vec![""; IOS_DEVICE_EVIDENCE_HEADER.len()];
            row[0] = case_id;
            row[1] = "iPhone";
            row[2] = "17.0";
            row[3] = "app";
            row[4] = "freedom";
            row[5] = "bee";
            row[6] = "artifact";
            row[7] = bee_state;
            row[8] = "clean";
            row[9] = if bee_state == "baseline_on_ipfs_off" {
                "none"
            } else {
                "auto"
            };
            row[10] = "wifi";
            row[11] = "pass";
            row[12] = "100";
            row[13] = "161";
            row[14] = if case_id == "cold_idle" { "61" } else { "10" };
            row[15] = "170";
            row[16] = "0.5";
            row[17] = "0";
            if requires_retrieval_measurements(case_id) {
                row[18] = "100";
                row[19] = "200";
                row[20] = "cache_hits=1";
                row[21] = "delegated_lookups=1";
            }
            row[22] = "0";
            row[23] = "trace";
            lines.push(row.join(","));
        }

        let error =
            validate_ios_device_evidence_contents("filled", &lines.join("\n"), true).unwrap_err();
        assert!(error.to_string().contains("exceeds 60 MiB"), "{error:#}");
    }
}
