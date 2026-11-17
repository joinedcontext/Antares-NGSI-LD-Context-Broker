// SPDX-License-Identifier: EUPL-1.2
//! 5.7.2.4 distributed Query Entities over a Context Source that pages its
//! answer. The broker merges every source's matches before it cuts the
//! client's page, so it reads a source past the source's own default page
//! instead of keeping the first page and silently dropping the rest.

use antares_api::AppState;
use axum::body::Body;
use axum::http::Request;
use std::io::{Read, Write};
use tower::ServiceExt;

/// Mock Context Source holding `held` Vehicles. It honours `limit` and
/// `offset` and, like any broker, answers its own default page (20) when the
/// request names no limit.
fn paging_source(held: usize) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = [0u8; 8192];
            let n = s.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).to_string();
            let target = head.split_whitespace().nth(1).unwrap_or("").to_owned();
            let param = |k: &str| {
                target
                    .split(['?', '&'])
                    .find_map(|kv| kv.strip_prefix(&format!("{k}=")))
                    .and_then(|v| v.parse::<usize>().ok())
            };
            let limit = param("limit").unwrap_or(20);
            let offset = param("offset").unwrap_or(0);
            let page: Vec<serde_json::Value> = (offset..held.min(offset + limit))
                .map(|i| {
                    serde_json::json!({
                        "id": format!("urn:ngsi-ld:Vehicle:paged-{i:03}"),
                        "type": "Vehicle",
                        "speed": {"type": "Property", "value": i},
                    })
                })
                .collect();
            let body = serde_json::Value::Array(page).to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(reply.as_bytes());
        }
    });
    port
}

async fn call(
    st: &AppState,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> (u16, String, Vec<String>) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(b) = &body {
        req = req
            .header("Content-Type", "application/json")
            .header("Content-Length", b.len());
    }
    let req = req
        .body(body.map_or_else(Body::empty, Body::from))
        .expect("request");
    let res = antares_api::router(st.clone())
        .oneshot(req)
        .await
        .expect("response");
    let status = res.status().as_u16();
    let warns = res
        .headers()
        .get_all("NGSILD-Warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&bytes).into_owned(), warns)
}

async fn register(st: &AppState, port: u16) {
    let body = serde_json::json!({
        "id": format!("urn:ngsi-ld:ContextSourceRegistration:paged-{port}"),
        "type": "ContextSourceRegistration",
        "mode": "inclusive",
        "operations": ["queryEntity"],
        "information": [{"entities": [{"type": "Vehicle"}]}],
        "endpoint": format!("http://127.0.0.1:{port}"),
    })
    .to_string();
    let (status, text, _) = call(st, "POST", "/ngsi-ld/v1/csourceRegistrations", Some(body)).await;
    assert_eq!(status, 201, "registration create: {text}");
}

fn ids_in(text: &str) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_str(text).expect("json");
    v.as_array()
        .expect("array")
        .iter()
        .filter_map(|e| {
            e.get("id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

/// A source holding more than its default page contributes every match the
/// client's page asks for, read in pages; nothing is fetched twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_is_read_past_its_default_page() {
    antares_jsonld::allow_private_egress(true);
    let st = AppState::new("antares1".into());
    register(&st, paging_source(30)).await;
    let (status, text, warns) = call(
        &st,
        "GET",
        "/ngsi-ld/v1/entities?type=Vehicle&limit=50",
        None,
    )
    .await;
    assert_eq!(status, 200, "{text}");
    let ids = ids_in(&text);
    assert_eq!(
        ids.len(),
        30,
        "every match of the source is merged: {ids:?}"
    );
    let distinct: std::collections::HashSet<&String> = ids.iter().collect();
    assert_eq!(distinct.len(), 30, "no Entity merged twice");
    assert!(
        warns.is_empty(),
        "a complete read raises no warning: {warns:?}"
    );
    // the same through the Query body of 5.2.23
    let q = serde_json::json!({"type": "Query", "entities": [{"type": "Vehicle"}]}).to_string();
    let (status, text, _) = call(
        &st,
        "POST",
        "/ngsi-ld/v1/entityOperations/query?limit=50",
        Some(q),
    )
    .await;
    assert_eq!(status, 200, "{text}");
    assert_eq!(ids_in(&text).len(), 30, "query body merges every match");
}

/// The read of one source is bounded: past ten full pages it stops, and the
/// client learns from an NGSILD-Warning that the merge lacks matches.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_past_the_ceiling_is_cut_with_a_warning() {
    antares_jsonld::allow_private_egress(true);
    let mut st = AppState::new("antares1".into());
    st.max_limit = 2;
    st.default_limit = 2;
    register(&st, paging_source(30)).await;
    let (status, text, warns) = call(
        &st,
        "GET",
        "/ngsi-ld/v1/entities?type=Vehicle&limit=2&count=true",
        None,
    )
    .await;
    assert_eq!(status, 200, "{text}");
    assert_eq!(ids_in(&text).len(), 2, "the client's page stays its size");
    assert!(
        warns.iter().any(|w| w.starts_with("199 ")),
        "a cut source is reported: {warns:?}"
    );
}

/// Mock Context Source holding `held` Vehicles of about `pad` bytes each,
/// paged by `limit` and `offset`; with `counts` it answers
/// `NGSILD-Results-Count` when asked. It keeps every request line.
fn counting_source(
    held: usize,
    pad: usize,
    counts: bool,
) -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> = std::sync::Arc::default();
    let recorder = seen.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = [0u8; 8192];
            let n = s.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).to_string();
            let line = head.lines().next().unwrap_or_default().to_owned();
            if let Ok(mut v) = recorder.lock() {
                v.push(line.clone());
            }
            let target = line.split_whitespace().nth(1).unwrap_or("").to_owned();
            let param = |k: &str| {
                target
                    .split(['?', '&'])
                    .find_map(|kv| kv.strip_prefix(&format!("{k}=")))
                    .map(str::to_owned)
            };
            let num = |k: &str, d: usize| param(k).and_then(|v| v.parse().ok()).unwrap_or(d);
            let (limit, offset) = (num("limit", 20), num("offset", 0));
            let page: Vec<serde_json::Value> = (offset..held.min(offset + limit))
                .map(|i| {
                    serde_json::json!({
                        "id": format!("urn:ngsi-ld:Vehicle:counted-{i:03}"),
                        "type": "Vehicle",
                        "speed": {"type": "Property", "value": i},
                        "note": {"type": "Property", "value": "x".repeat(pad)},
                    })
                })
                .collect();
            let body = serde_json::Value::Array(page).to_string();
            let count = match counts && param("count").as_deref() == Some("true") {
                true => format!("NGSILD-Results-Count: {held}\r\n"),
                false => String::new(),
            };
            let reply = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n{count}\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = s.write_all(reply.as_bytes());
        }
    });
    (port, seen)
}

/// The pages read of the Context Sources draw on one byte budget per
/// request: a source that keeps answering full pages is read up to it, and
/// the client learns from an NGSILD-Warning that the merge lacks matches.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_is_read_within_the_byte_budget() {
    antares_jsonld::allow_private_egress(true);
    let mut st = AppState::new("antares1".into());
    st.max_limit = 2;
    st.default_limit = 2;
    // about two pages of two padded Entities
    st.max_answer_bytes = 5_000;
    let (port, seen) = counting_source(30, 1_000, false);
    register(&st, port).await;
    let (status, text, warns) = call(
        &st,
        "GET",
        "/ngsi-ld/v1/entities?type=Vehicle&limit=2&offset=16",
        None,
    )
    .await;
    assert_eq!(status, 200, "{text}");
    let asked = seen.lock().expect("lock").len();
    assert!(
        asked <= 3,
        "the source was read past the budget: {asked} pages"
    );
    assert!(
        warns.iter().any(|w| w.starts_with("199 ")),
        "a source cut at the budget is reported: {warns:?}"
    );
}

/// 6.3.13 on a distributed query: a source that counts its own matches is
/// asked for its count and read only as far as the client's page needs;
/// the count it answered stands for the matches not read.
#[tokio::test(flavor = "multi_thread")]
async fn the_count_is_read_from_the_source() {
    antares_jsonld::allow_private_egress(true);
    let st = AppState::new("antares1".into());
    let (port, seen) = counting_source(30, 0, true);
    register(&st, port).await;
    let req = Request::builder()
        .method("GET")
        .uri("/ngsi-ld/v1/entities?type=Vehicle&limit=2&count=true")
        .body(Body::empty())
        .expect("request");
    let res = antares_api::router(st.clone())
        .oneshot(req)
        .await
        .expect("response");
    assert_eq!(res.status().as_u16(), 200);
    let count = res
        .headers()
        .get("NGSILD-Results-Count")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(ids_in(&String::from_utf8_lossy(&bytes)).len(), 2);
    assert_eq!(count.as_deref(), Some("30"), "the source's count");
    let lines = seen.lock().expect("lock").clone();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("count=true"), "{}", lines[0]);
    assert!(
        lines[0].contains("limit=3"),
        "only what the page needs is read: {}",
        lines[0]
    );
}

/// A Context Source answering `[]` that keeps the request line of every
/// request it got.
fn recording_source() -> (u16, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> = std::sync::Arc::default();
    let recorder = seen.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = [0u8; 8192];
            let n = s.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).to_string();
            if let Ok(mut v) = recorder.lock() {
                v.push(head.lines().next().unwrap_or_default().to_owned());
            }
            let _ = s.write_all(
                b"HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 2
Connection: close

[]",
            );
        }
    });
    (port, seen)
}

/// 4.3.6.1: a source is asked only about the types it registered: of a
/// multi-type selector the registered ones, and its own types when the
/// query names none.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_is_asked_only_for_its_registered_types() {
    antares_jsonld::allow_private_egress(true);
    let st = AppState::new("antares1".into());
    let (port, seen) = recording_source();
    register(&st, port).await;
    let (status, text, _) = call(&st, "GET", "/ngsi-ld/v1/entities?type=Vehicle,Bike", None).await;
    assert_eq!(status, 200, "{text}");
    let (status, text, _) = call(&st, "GET", "/ngsi-ld/v1/entities?attrs=speed", None).await;
    assert_eq!(status, 200, "{text}");
    let lines = seen.lock().expect("lock").clone();
    assert_eq!(lines.len(), 2, "{lines:?}");
    for line in &lines {
        assert!(line.contains("type=Vehicle"), "{line}");
        assert!(
            !line.contains("Bike"),
            "an unregistered type was asked: {line}"
        );
    }
}

/// 4.3.6.1 for Create EntityMap (5.14.4.4): the forward carries only the ids
/// the registration can match and its own attribute scope, as a forwarded
/// query does.
#[tokio::test(flavor = "multi_thread")]
async fn an_entity_map_forward_is_narrowed_to_the_registration() {
    antares_jsonld::allow_private_egress(true);
    let st = AppState::new("antares1".into());
    let (port, seen) = recording_source();
    let reg = serde_json::json!({
        "id": "urn:ngsi-ld:ContextSourceRegistration:em-narrow",
        "type": "ContextSourceRegistration",
        "operations": ["createEntityMapQueryEntity"],
        "information": [{
            "entities": [{"type": "Vehicle", "id": "urn:ngsi-ld:Vehicle:1"}],
            "propertyNames": ["speed"]
        }],
        "endpoint": format!("http://127.0.0.1:{port}"),
    })
    .to_string();
    let (status, text, _) = call(&st, "POST", "/ngsi-ld/v1/csourceRegistrations", Some(reg)).await;
    assert_eq!(status, 201, "{text}");
    let (status, text, _) = call(
        &st,
        "GET",
        "/ngsi-ld/v1/entityMaps?id=urn:ngsi-ld:Vehicle:1,urn:ngsi-ld:Vehicle:2&attrs=speed,brand",
        None,
    )
    .await;
    assert!(status < 300, "{status} {text}");
    let lines = seen.lock().expect("lock").clone();
    assert_eq!(lines.len(), 1, "{lines:?}");
    // the request line carries the ids percent-encoded
    assert!(lines[0].contains("Vehicle%3A1"), "{}", lines[0]);
    assert!(
        !lines[0].contains("Vehicle%3A2"),
        "an id outside the registration: {}",
        lines[0]
    );
    assert!(
        !lines[0].contains("brand"),
        "an attribute outside the registration: {}",
        lines[0]
    );
}

/// A source answering every request with `body` (201, JSON).
fn canned_source(body: &'static str) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let mut buf = [0u8; 8192];
            let _ = s.read(&mut buf);
            let _ = s.write_all(
                format!(
                    "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });
    port
}

/// 4.3.6.1: the ids a peer's EntityMap returns join the local map only
/// where the registration can provide them (or the query named them).
#[tokio::test(flavor = "multi_thread")]
async fn an_entity_map_imports_only_the_ids_the_registration_provides() {
    antares_jsonld::allow_private_egress(true);
    let st = AppState::new("antares1".into());
    let port = canned_source(
        r#"{"id":"urn:ngsi-ld:EntityMap:peer","type":"EntityMap",
            "expiresAt":"2999-01-01T00:00:00Z","linkedMaps":{},
            "entityMap":{"urn:ngsi-ld:Vehicle:1":["@none"],"urn:ngsi-ld:Vehicle:9":["@none"]}}"#,
    );
    let reg = serde_json::json!({
        "id": "urn:ngsi-ld:ContextSourceRegistration:em-import",
        "type": "ContextSourceRegistration",
        "operations": ["createEntityMapQueryEntity"],
        "information": [{"entities": [{"type": "Vehicle", "id": "urn:ngsi-ld:Vehicle:1"}]}],
        "endpoint": format!("http://127.0.0.1:{port}"),
    })
    .to_string();
    let (status, text, _) = call(&st, "POST", "/ngsi-ld/v1/csourceRegistrations", Some(reg)).await;
    assert_eq!(status, 201, "{text}");
    let (status, text, _) = call(&st, "GET", "/ngsi-ld/v1/entityMaps?type=Vehicle", None).await;
    assert_eq!(status, 201, "{text}");
    let map: serde_json::Value = serde_json::from_str(&text).expect("json");
    let ids = map["entityMap"].as_object().expect("entityMap");
    assert!(ids.contains_key("urn:ngsi-ld:Vehicle:1"), "{map}");
    assert!(!ids.contains_key("urn:ngsi-ld:Vehicle:9"), "{map}");
}
