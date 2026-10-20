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
    /// The normalized path, with its namespace: `/ipfs/…` or `/ipns/…`.
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
