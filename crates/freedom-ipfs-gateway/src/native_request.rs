//! Native gateway requests: a host's request (the add-on's request JSON, or
//! the same fields from any other host) turned into a [`GatewayCoreRequest`].
//!
//! This is the parser `freedom_ipfs_gateway_request_start` has always used,
//! moved here so every host treats a path exactly as the add-on does:
//!
//! - **The method** is `GET` (the default) or `HEAD`, in any case. Anything
//!   else is refused.
//! - **The path** is trimmed, then accepted as `/ipfs/…` or `/ipns/…`, as
//!   `ipfs://…` or `ipns://…`, or as a bare CID (`/ipfs/<cid>`). A query or
//!   fragment is cut off. The namespace must be followed by something.
//! - **The path's segments** are percent-decoded, as the router's `Path`
//!   extractor decodes them: hosts send the browser's pathname, where a
//!   space is `%20` and `ü` is `%C3%BC`. The path is split on `/` first,
//!   then each segment is decoded exactly once, so `%252F` stays the name
//!   `%2F`, and `+` stays a `+`. A segment that decodes to `.` or `..`, to
//!   anything with `/`, `\` or NUL in it, or to bytes that aren't UTF-8
//!   refuses the path: `%2F` never makes a separator.
//! - **Headers** are passed through; a name or value `http` refuses fails the
//!   request, as do more distinct names than a header map holds (at most
//!   24,576, the `x-freedom-*` headers below included). Each is `{ "name", "value" }` or, equally, a `[name, value]`
//!   pair: serde's derived structs take either, so the add-on always has.
//! - **Request IDs and the top-level path** become the
//!   `x-freedom-request-id`, `x-freedom-parent-request-id` and
//!   `x-freedom-top-level-path` headers, the last only when it isn't empty.

use std::fmt;

use axum::http::{HeaderMap, HeaderName, HeaderValue, Method};
use freedom_ipfs_core::parse_cid;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};

use crate::{
    GatewayCoreRequest, GatewayNamespace, X_FREEDOM_PARENT_REQUEST_ID, X_FREEDOM_REQUEST_ID,
    X_FREEDOM_TOP_LEVEL_PATH,
};

/// A native request, as its JSON reads:
/// `{ "method"?, "path", "headers"?: [{ "name", "value" }], "request_id"?,
/// "parent_request_id"?, "top_level_path"? }`. Other members are ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct NativeRequest {
    pub method: Option<String>,
    pub path: String,
    pub headers: Option<Vec<NativeRequestHeader>>,
    pub request_id: Option<u64>,
    pub parent_request_id: Option<u64>,
    pub top_level_path: Option<String>,
}

/// One request header.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct NativeRequestHeader {
    pub name: String,
    pub value: String,
}

/// A parsed native request: the gateway request, and the labels a host
/// reports it under.
#[derive(Debug)]
pub struct ParsedNativeRequest {
    pub request: GatewayCoreRequest,
    /// The method, upper-cased: `GET` or `HEAD`.
    pub method_label: String,
    /// The normalized path, with its namespace: `/ipfs/…` or `/ipns/…`,
    /// still percent-encoded as the host sent it.
    pub rendered_path: String,
    /// `ipfs` or `ipns`.
    pub namespace_label: String,
    pub request_id: Option<u64>,
    pub parent_request_id: Option<u64>,
    /// As given, empty included.
    pub top_level_path: Option<String>,
}

/// Why a native request was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeRequestError {
    /// Not JSON of the request's shape.
    Json(String),
    /// A method other than `GET` or `HEAD`.
    Method(String),
    /// A path that isn't a content path under `/ipfs/` or `/ipns/`.
    Path,
    /// A header name or value `http` refuses.
    Header,
    /// More headers than a header map holds.
    TooManyHeaders,
}

impl fmt::Display for NativeRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "invalid native request JSON: {error}"),
            Self::Method(method) => write!(f, "unsupported method {method:?}"),
            Self::Path => f.write_str("not an /ipfs/ or /ipns/ content path"),
            Self::Header => f.write_str("invalid header name or value"),
            Self::TooManyHeaders => f.write_str("too many headers"),
        }
    }
}

impl std::error::Error for NativeRequestError {}

/// Parses a native request's JSON, as `freedom_ipfs_gateway_request_start`
/// receives it.
pub fn parse_native_request_json(json: &str) -> Result<ParsedNativeRequest, NativeRequestError> {
    let request: NativeRequest =
        serde_json::from_str(json).map_err(|error| NativeRequestError::Json(error.to_string()))?;
    parse_native_request(request)
}

/// Parses a native request.
pub fn parse_native_request(
    request: NativeRequest,
) -> Result<ParsedNativeRequest, NativeRequestError> {
    let method_label = request
        .method
        .unwrap_or_else(|| "GET".to_string())
        .to_ascii_uppercase();
    let method = match method_label.as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        _ => return Err(NativeRequestError::Method(method_label)),
    };
    let path = normalize_gateway_request_path(&request.path).ok_or(NativeRequestError::Path)?;
    let (namespace, path_without_namespace, rendered_path) =
        split_gateway_namespace_path(&path).ok_or(NativeRequestError::Path)?;
    let path_without_namespace =
        decode_path_segments(&path_without_namespace).ok_or(NativeRequestError::Path)?;
    let mut headers = HeaderMap::new();
    for header in request.headers.into_iter().flatten() {
        insert_header(&mut headers, &header.name, &header.value)?;
    }
    if let Some(request_id) = request.request_id {
        insert_header(&mut headers, X_FREEDOM_REQUEST_ID, &request_id.to_string())?;
    }
    if let Some(parent_request_id) = request.parent_request_id {
        insert_header(
            &mut headers,
            X_FREEDOM_PARENT_REQUEST_ID,
            &parent_request_id.to_string(),
        )?;
    }
    if let Some(top_level_path) = request
        .top_level_path
        .as_ref()
        .filter(|top_level_path| !top_level_path.is_empty())
    {
        insert_header(&mut headers, X_FREEDOM_TOP_LEVEL_PATH, top_level_path)?;
    }
    let gateway_request = match namespace {
        GatewayNamespace::Ipfs => GatewayCoreRequest::ipfs(path_without_namespace, method, headers),
        GatewayNamespace::Ipns => GatewayCoreRequest::ipns(path_without_namespace, method, headers),
    };
    Ok(ParsedNativeRequest {
        request: gateway_request,
        method_label,
        rendered_path,
        namespace_label: namespace.as_str().to_string(),
        request_id: request.request_id,
        parent_request_id: request.parent_request_id,
        top_level_path: request.top_level_path,
    })
}

/// A content path, as the add-on's preloads and requests take it: trimmed,
/// then `/ipfs/…` or `/ipns/…` as given, `ipfs://…` or `ipns://…` as that
/// path, or a bare CID as `/ipfs/<cid>`. `None` for anything else.
pub fn normalize_content_path(path: &str) -> Option<String> {
    let path = path.trim();
    if path.starts_with("/ipfs/") || path.starts_with("/ipns/") {
        return Some(path.to_string());
    }
    if let Some(rest) = path.strip_prefix("ipfs://") {
        let rest = rest.trim_start_matches('/');
        if !rest.is_empty() {
            return Some(format!("/ipfs/{rest}"));
        }
    }
    if let Some(rest) = path.strip_prefix("ipns://") {
        let rest = rest.trim_start_matches('/');
        if !rest.is_empty() {
            return Some(format!("/ipns/{rest}"));
        }
    }
    parse_cid(path).ok().map(|cid| format!("/ipfs/{cid}"))
}

fn normalize_gateway_request_path(path: &str) -> Option<String> {
    let path = normalize_content_path(path)?;
    let end = path.find(['?', '#']).unwrap_or(path.len());
    let path = &path[..end];
    (!path.is_empty()).then(|| path.to_string())
}

fn split_gateway_namespace_path(path: &str) -> Option<(GatewayNamespace, String, String)> {
    if let Some(path) = path.strip_prefix("/ipfs/") {
        return (!path.is_empty()).then(|| {
            (
                GatewayNamespace::Ipfs,
                path.to_string(),
                format!("/ipfs/{path}"),
            )
        });
    }
    if let Some(path) = path.strip_prefix("/ipns/") {
        return (!path.is_empty()).then(|| {
            (
                GatewayNamespace::Ipns,
                path.to_string(),
                format!("/ipns/{path}"),
            )
        });
    }
    None
}

/// `path` with each `/`-separated segment percent-decoded once, as axum's
/// `Path` extractor decodes (the same crate, `percent-encoding`): `%XX`
/// with two hex digits is a byte, anything else is kept as written. `None`
/// when a segment decodes to `.` or `..`, holds `/`, `\` or NUL once
/// decoded, or isn't UTF-8.
fn decode_path_segments(path: &str) -> Option<String> {
    let mut decoded = Vec::new();
    for segment in path.split('/') {
        let segment = percent_decode_str(segment).decode_utf8().ok()?;
        if matches!(&*segment, "." | "..") || segment.contains(['/', '\\', '\0']) {
            return None;
        }
        decoded.push(segment);
    }
    Some(decoded.join("/"))
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &str,
    value: &str,
) -> Result<(), NativeRequestError> {
    let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| NativeRequestError::Header)?;
    let value = HeaderValue::from_str(value).map_err(|_| NativeRequestError::Header)?;
    // `insert` panics past the map's capacity, and the names are the
    // caller's: the add-on calls this inside an `extern "C"` function, where
    // a panic aborts the process.
    headers
        .try_insert(name, value)
        .map_err(|_| NativeRequestError::TooManyHeaders)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID: &str = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e";

    fn parsed(json: &str) -> ParsedNativeRequest {
        parse_native_request_json(json).unwrap()
    }

    fn header<'a>(parsed: &'a ParsedNativeRequest, name: &str) -> Option<&'a str> {
        parsed
            .request
            .headers
            .get(name)
            .map(|value| value.to_str().unwrap())
    }

    #[test]
    fn methods_default_to_get_in_any_case() {
        let get = parsed(r#"{"path":"/ipfs/x"}"#);
        assert_eq!(
            (get.request.method.clone(), get.method_label.as_str()),
            (Method::GET, "GET")
        );
        let head = parsed(r#"{"method":"hEaD","path":"/ipfs/x"}"#);
        assert_eq!(
            (head.request.method.clone(), head.method_label.as_str()),
            (Method::HEAD, "HEAD")
        );
        assert_eq!(
            parse_native_request_json(r#"{"method":"post","path":"/ipfs/x"}"#).unwrap_err(),
            NativeRequestError::Method("POST".into())
        );
    }

    #[test]
    fn paths_in_every_accepted_form() {
        for (path, namespace, rendered, inner) in [
            (
                "/ipfs/x/a.js",
                GatewayNamespace::Ipfs,
                "/ipfs/x/a.js",
                "x/a.js",
            ),
            (
                "  /ipns/example.com/ ",
                GatewayNamespace::Ipns,
                "/ipns/example.com/",
                "example.com/",
            ),
            (
                "ipfs://x/a?q=1#f",
                GatewayNamespace::Ipfs,
                "/ipfs/x/a",
                "x/a",
            ),
            (
                "ipns:///example.com",
                GatewayNamespace::Ipns,
                "/ipns/example.com",
                "example.com",
            ),
            (CID, GatewayNamespace::Ipfs, &format!("/ipfs/{CID}"), CID),
            ("/ipfs/x#frag", GatewayNamespace::Ipfs, "/ipfs/x", "x"),
        ] {
            let json = serde_json::json!({ "path": path }).to_string();
            let parsed = parsed(&json);
            assert_eq!(parsed.request.namespace, namespace, "{path}");
            assert_eq!(parsed.rendered_path, rendered, "{path}");
            assert_eq!(parsed.request.path, inner, "{path}");
            assert_eq!(parsed.namespace_label, namespace.as_str(), "{path}");
        }
        for path in [
            "",
            "/ipfs/",
            "/ipns/",
            "/ipfs/?q",
            "/ipfs/#f",
            "ipfs://",
            "https://example.com/ipfs/x",
            "/api/v0/version",
            "not-a-cid",
        ] {
            let json = serde_json::json!({ "path": path }).to_string();
            assert_eq!(
                parse_native_request_json(&json).unwrap_err(),
                NativeRequestError::Path,
                "{path:?}"
            );
        }
    }

    #[test]
    fn ids_and_top_level_path_become_headers() {
        let parsed = parsed(
            r#"{"path":"/ipfs/x","request_id":41,"parent_request_id":7,"top_level_path":"/ipfs/x/","headers":[{"name":"Range","value":"bytes=0-"}]}"#,
        );
        assert_eq!(header(&parsed, "x-freedom-request-id"), Some("41"));
        assert_eq!(header(&parsed, "x-freedom-parent-request-id"), Some("7"));
        assert_eq!(
            header(&parsed, "x-freedom-top-level-path"),
            Some("/ipfs/x/")
        );
        assert_eq!(header(&parsed, "range"), Some("bytes=0-"));
        assert_eq!(
            (parsed.request_id, parsed.parent_request_id),
            (Some(41), Some(7))
        );
        // An empty top-level path is kept as given but sends no header.
        let parsed = super::tests::parsed(r#"{"path":"/ipfs/x","top_level_path":""}"#);
        assert_eq!(parsed.top_level_path.as_deref(), Some(""));
        assert_eq!(header(&parsed, "x-freedom-top-level-path"), None);
    }

    #[test]
    fn bad_headers_and_json_are_refused() {
        for json in [
            r#"{"path":"/ipfs/x","headers":[{"name":"bad name","value":"v"}]}"#,
            r#"{"path":"/ipfs/x","headers":[{"name":"x","value":"a\nb"}]}"#,
            r#"{"path":"/ipfs/x","top_level_path":"a\u0000b"}"#,
        ] {
            assert_eq!(
                parse_native_request_json(json).unwrap_err(),
                NativeRequestError::Header,
                "{json}"
            );
        }
        for json in [
            "",
            "[]",
            r#"{"method":"GET"}"#,
            r#"{"path":7}"#,
            r#"{"path":"/ipfs/x","request_id":-1}"#,
            r#"{"path":"/ipfs/x","headers":[["range"]]}"#,
        ] {
            assert!(
                matches!(
                    parse_native_request_json(json),
                    Err(NativeRequestError::Json(_))
                ),
                "{json}"
            );
        }
        // Unknown members are ignored, as they always were.
        assert!(parse_native_request_json(r#"{"path":"/ipfs/x","extra":1}"#).is_ok());
        // A header may be a [name, value] pair too: serde's derived structs
        // take a sequence, so the add-on has always accepted this form.
        let pairs = parsed(r#"{"path":"/ipfs/x","headers":[["range","bytes=0-"]]}"#);
        assert_eq!(header(&pairs, "range"), Some("bytes=0-"));
    }

    fn with_headers(count: usize) -> NativeRequest {
        NativeRequest {
            path: "/ipfs/x".into(),
            headers: Some(
                (0..count)
                    .map(|i| NativeRequestHeader {
                        name: format!("x-h{i}"),
                        value: String::new(),
                    })
                    .collect(),
            ),
            ..NativeRequest::default()
        }
    }

    /// How many distinct names a header map takes before `try_insert`
    /// refuses: 24,576 in http 1.x, three quarters of its largest table.
    fn map_capacity() -> usize {
        let mut map = HeaderMap::new();
        (0..)
            .take_while(|i| {
                let name = HeaderName::from_bytes(format!("x-h{i}").as_bytes()).unwrap();
                map.try_insert(name, HeaderValue::from_static("")).is_ok()
            })
            .count()
    }

    #[test]
    fn more_headers_than_a_map_holds_are_refused_not_a_panic() {
        let capacity = map_capacity();
        assert!(capacity < 33_000, "{capacity}");
        // `insert` panics on the name past capacity: refused instead.
        let refused = std::panic::catch_unwind(|| parse_native_request(with_headers(33_000)).err());
        assert_eq!(
            refused.ok(),
            Some(Some(NativeRequestError::TooManyHeaders)),
            "refused, without a panic"
        );
        // A request whose own names fill the map parses; one more refuses,
        // and so does a full map with an `x-freedom-request-id` to add.
        let full = parse_native_request(with_headers(capacity)).unwrap();
        assert_eq!(full.request.headers.len(), capacity);
        assert_eq!(
            parse_native_request(with_headers(capacity + 1)).unwrap_err(),
            NativeRequestError::TooManyHeaders
        );
        let full = NativeRequest {
            request_id: Some(1),
            ..with_headers(capacity)
        };
        assert_eq!(
            parse_native_request(full).unwrap_err(),
            NativeRequestError::TooManyHeaders
        );
        // Repeated names keep one entry, the last value, as `insert` did.
        let repeated = NativeRequest {
            headers: Some(
                (0..40_000)
                    .map(|i| NativeRequestHeader {
                        name: "x-h".into(),
                        value: i.to_string(),
                    })
                    .collect(),
            ),
            ..with_headers(0)
        };
        let repeated = parse_native_request(repeated).unwrap();
        assert_eq!(repeated.request.headers.len(), 1);
        assert_eq!(header(&repeated, "x-h"), Some("39999"));
    }

    /// What `path` parses to under `/ipfs/x/`: the gateway's path, or the
    /// error.
    fn inner_path(path: &str) -> Result<String, NativeRequestError> {
        parse_native_request(NativeRequest {
            path: format!("/ipfs/x/{path}"),
            ..NativeRequest::default()
        })
        .map(|parsed| parsed.request.path)
    }

    #[test]
    fn path_segments_are_percent_decoded_once() {
        for (path, decoded) in [
            ("space%20name.txt", "space name.txt"),
            ("two%20%20spaces.txt", "two  spaces.txt"),
            ("%C3%BCn%20%C3%AF/%C3%A7%C3%B8d%C3%A9.txt", "ün ï/çødé.txt"),
            ("%c3%bcn%20%c3%af/", "ün ï/"),
            ("%E6%97%A5%E6%9C%AC%E8%AA%9E.txt", "日本語.txt"),
            ("a+b.txt", "a+b.txt"),
            ("%252F.txt", "%2F.txt"),
            ("%252E%252E", "%2E%2E"),
            ("%2541", "%41"),
            ("q%3F%23.txt", "q?#.txt"),
            ("%zz%4", "%zz%4"),
            ("dir//a", "dir//a"),
            ("..a/.b/a..", "..a/.b/a.."),
            ("%2E%2E%2E", "..."),
        ] {
            assert_eq!(
                inner_path(path).as_deref(),
                Ok(&*format!("x/{decoded}")),
                "{path}"
            );
        }
        // The CID or name is a segment too, and `/ipns/` decodes the same.
        let ipns = parse_native_request(NativeRequest {
            path: "/ipns/ex%61mple.com/%C3%A9t%C3%A9".into(),
            ..NativeRequest::default()
        })
        .unwrap();
        assert_eq!(ipns.request.path, "example.com/été");
        // The label keeps the host's form.
        assert_eq!(ipns.rendered_path, "/ipns/ex%61mple.com/%C3%A9t%C3%A9");
        for path in [
            "a%2Fb",
            "a%2fb",
            "a%5Cb",
            "a\\b",
            "a%00b",
            ".",
            "..",
            "%2E",
            "%2e%2E",
            "a/%2E%2E/b",
            "%FF.txt",
            "%C3",
            "%C3%28",
            "%ED%A0%80",
        ] {
            assert_eq!(inner_path(path), Err(NativeRequestError::Path), "{path}");
        }
        // A query or fragment is cut off before decoding: an encoded one
        // is a name's own.
        assert_eq!(inner_path("a%3Fb?c%2Fd").as_deref(), Ok("x/a?b"));
    }

    /// A directory whose entries have the names the table below asks for,
    /// each a file holding its own name.
    fn named_directory(store: &freedom_ipfs_store::SqliteBlockStore) -> cid::Cid {
        use crate::tests::{test_link, test_pb_directory, test_pb_file};
        use freedom_ipfs_core::{cid_from_data, CODEC_DAG_PB};
        let file = |name: &str| {
            let block = test_pb_file(name.as_bytes());
            let cid = cid_from_data(CODEC_DAG_PB, &block);
            store.put_block(&cid, &block).unwrap();
            test_link(name, &cid)
        };
        let sub = test_pb_directory(vec![file("çødé.txt")]);
        let sub_cid = cid_from_data(CODEC_DAG_PB, &sub);
        store.put_block(&sub_cid, &sub).unwrap();
        let root = test_pb_directory(vec![
            file("space name.txt"),
            file("two  spaces.txt"),
            test_link("ün ï", &sub_cid),
            file("日本語.txt"),
            file("a+b.txt"),
            file("a b.txt"),
            file("%2F.txt"),
            file("%2E%2E"),
            file("q?#.txt"),
            file("%zz"),
        ]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root);
        store.put_block(&root_cid, &root).unwrap();
        root_cid
    }

    async fn status_and_body(response: axum::response::Response) -> (u16, Vec<u8>) {
        let status = response.status().as_u16();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, body.to_vec())
    }

    /// The router (axum's `Path` extractor decoding the request's path)
    /// and a native request (this parser) given the same browser path, on
    /// the same store: the same answer for every path both take. The
    /// native parser refuses a path whose segments decode to a separator,
    /// a backslash, NUL, a dot segment or not UTF-8; the router decodes
    /// those whole, and answers as the table says.
    #[tokio::test]
    async fn the_router_and_native_requests_decode_paths_alike() {
        use tower::ServiceExt;
        let store = freedom_ipfs_store::SqliteBlockStore::in_memory(1 << 20).unwrap();
        let root = named_directory(&store);
        let router = crate::router(store.clone());
        let core = crate::GatewayCore::new(store);
        // The path under the directory; what the router answers, its
        // status and, for a 200, the file; and whether the native parser
        // refuses the path (or answers just as the router does).
        for (path, status, file, refused) in [
            ("space%20name.txt", 200, "space name.txt", false),
            ("two%20%20spaces.txt", 200, "two  spaces.txt", false),
            (
                "%C3%BCn%20%C3%AF/%C3%A7%C3%B8d%C3%A9.txt",
                200,
                "çødé.txt",
                false,
            ),
            ("%E6%97%A5%E6%9C%AC%E8%AA%9E.txt", 200, "日本語.txt", false),
            // Not `a b.txt`, which the directory also has.
            ("a+b.txt", 200, "a+b.txt", false),
            ("%252F.txt", 200, "%2F.txt", false),
            ("q%3F%23.txt", 200, "q?#.txt", false),
            ("%zz", 200, "%zz", false),
            // Decoded once, to the name `%2E%2E`, which the gateway refuses
            // on both paths: it would decode to a dot segment.
            ("%252E%252E", 400, "", false),
            ("a%2Fb.txt", 404, "", true),
            ("a%5Cb.txt", 404, "", true),
            ("a%00b.txt", 404, "", true),
            ("%2E", 400, "", true),
            ("%2E%2E", 400, "", true),
            ("%FF.txt", 400, "", true),
        ] {
            let uri = format!("/ipfs/{root}/{path}");
            let request = axum::http::Request::builder()
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            let routed = status_and_body(router.clone().oneshot(request).await.unwrap()).await;
            assert_eq!(routed.0, status, "router: {path}");
            if status == 200 {
                assert_eq!(routed.1, file.as_bytes(), "router: {path}");
            }
            let parsed = parse_native_request(NativeRequest {
                path: uri,
                ..NativeRequest::default()
            });
            if refused {
                assert_eq!(parsed.unwrap_err(), NativeRequestError::Path, "{path}");
                continue;
            }
            let native = status_and_body(core.handle(parsed.unwrap().request).await).await;
            assert_eq!(native.0, routed.0, "native: {path}");
            if status == 200 {
                assert_eq!(native.1, routed.1, "native: {path}");
            }
        }
    }

    /// The bug this parser had: the browser's encoded path was looked up
    /// as written, so `ün ï/çødé.txt` was not found.
    #[tokio::test]
    async fn a_native_request_finds_names_the_browser_encodes() {
        let store = freedom_ipfs_store::SqliteBlockStore::in_memory(1 << 20).unwrap();
        let root = named_directory(&store);
        let parsed = parse_native_request_json(
            &serde_json::json!({
                "path": format!("/ipfs/{root}/%C3%BCn%20%C3%AF/%C3%A7%C3%B8d%C3%A9.txt"),
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(parsed.request.path, format!("{root}/ün ï/çødé.txt"));
        let (status, body) =
            status_and_body(crate::GatewayCore::new(store).handle(parsed.request).await).await;
        assert_eq!((status, body.as_slice()), (200, "çødé.txt".as_bytes()));
    }

    #[test]
    fn the_typed_and_json_entry_points_agree() {
        let typed = parse_native_request(NativeRequest {
            method: Some("head".into()),
            path: "ipfs://x/y".into(),
            headers: Some(vec![NativeRequestHeader {
                name: "if-none-match".into(),
                value: "\"e\"".into(),
            }]),
            request_id: Some(3),
            parent_request_id: None,
            top_level_path: Some("/ipfs/x/".into()),
        })
        .unwrap();
        let json = parsed(
            r#"{"method":"head","path":"ipfs://x/y","headers":[{"name":"if-none-match","value":"\"e\""}],"request_id":3,"top_level_path":"/ipfs/x/"}"#,
        );
        assert_eq!(typed.request.headers, json.request.headers);
        assert_eq!(typed.request.path, json.request.path);
        assert_eq!(typed.rendered_path, json.rendered_path);
        assert_eq!(typed.method_label, json.method_label);
    }
}
