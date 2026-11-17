// SPDX-License-Identifier: EUPL-1.2
//! Faults on the distributed and joined paths: a local error inside a
//! distributed write keeps its own type (6.3.17), a store failure during a
//! join is a truncation (4.5.23), every forwarded batch item travels under
//! its own @context (5.5.7) and per-Entity forwards run concurrently.
#![allow(clippy::unwrap_used)] // an unwrap here is the assertion

use antares_api::AppState;
use axum::body::Body;
use axum::http::Request;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

/// Mock Context Source: records every request it sees and answers `reply`
/// after `delay`.
fn mock(reply: &'static str, delay: std::time::Duration) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let log = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let log = log.clone();
            std::thread::spawn(move || {
                let text = read_request(&mut s);
                log.lock().expect("lock").push(text);
                std::thread::sleep(delay);
                let _ = s.write_all(reply.as_bytes());
            });
        }
    });
    (port, seen)
}

fn read_request(s: &mut std::net::TcpStream) -> String {
    let mut raw = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = s.read(&mut buf).unwrap_or(0);
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
        let text = String::from_utf8_lossy(&raw).into_owned();
        let Some((h, b)) = text.split_once("\r\n\r\n") else {
            continue;
        };
        let want: usize = h
            .lines()
            .find_map(|l| {
                let l = l.to_ascii_lowercase();
                l.strip_prefix("content-length:")
                    .and_then(|v| v.trim().parse().ok())
            })
            .unwrap_or(0);
        if b.len() >= want {
            break;
        }
    }
    String::from_utf8_lossy(&raw).into_owned()
}

const NO_CONTENT: &str =
    "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

fn state() -> AppState {
    antares_jsonld::allow_private_egress(true);
    AppState::new("antares-faults".into())
}

async fn send(
    st: &AppState,
    method: &str,
    uri: &str,
    ct: &str,
    body: Option<String>,
) -> (u16, Vec<String>, String) {
    let mut req = Request::builder().method(method).uri(uri);
    let b = match body {
        Some(b) => {
            req = req
                .header("Content-Type", ct)
                .header("Content-Length", b.len());
            Body::from(b)
        }
        None => Body::empty(),
    };
    let res = antares_api::router(st.clone())
        .oneshot(req.body(b).expect("request"))
        .await
        .expect("response");
    let status = res.status().as_u16();
    let warnings = res
        .headers()
        .get_all("NGSILD-Warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        warnings,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

async fn json(st: &AppState, method: &str, uri: &str, body: &str) -> (u16, String) {
    let (status, _, text) = send(st, method, uri, "application/json", Some(body.to_owned())).await;
    (status, text)
}

async fn register(st: &AppState, port: u16, ops: &[&str]) {
    let doc = serde_json::json!({
        "type": "ContextSourceRegistration",
        "operations": ops,
        "information": [{"entities": [{"type": "Vehicle"}]}],
        "endpoint": format!("http://127.0.0.1:{port}"),
    });
    let (status, body) = json(
        st,
        "POST",
        "/ngsi-ld/v1/csourceRegistrations",
        &doc.to_string(),
    )
    .await;
    assert_eq!(status, 201, "registration create: {body}");
}

async fn create(st: &AppState, doc: serde_json::Value) {
    let (status, body) = json(st, "POST", "/ngsi-ld/v1/entities", &doc.to_string()).await;
    assert_eq!(status, 201, "create: {body}");
}

/// 6.3.17: a distributed merge whose local part fails validation (a
/// simplified value the stored GeoProperty cannot hold) reports
/// that part with its own error type, never as a missing Entity.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_error_in_a_distributed_merge_keeps_its_type() {
    let st = state();
    create(
        &st,
        serde_json::json!({"id": "urn:ngsi-ld:Vehicle:m", "type": "Vehicle",
            "place": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [1, 2]}}}),
    )
    .await;
    let (port, _) = mock(NO_CONTENT, std::time::Duration::ZERO);
    register(&st, port, &["mergeEntity"]).await;
    let (status, body) = json(
        &st,
        "PATCH",
        "/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:m?options=keyValues",
        r#"{"place":"nowhere"}"#,
    )
    .await;
    assert_ne!(status, 204, "{body}");
    assert!(body.contains("BadRequestData"), "{status} {body}");
    assert!(!body.contains("not found locally"), "{status} {body}");
}

/// 4.5.23: a linked Entity the store fails to read is a truncated join
/// (NGSILD-Warning), never a dangling link served as complete.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_failure_during_a_join_is_a_truncation() {
    let mut st = state();
    create(
        &st,
        serde_json::json!({"id": "urn:ngsi-ld:Vehicle:jb", "type": "Vehicle"}),
    )
    .await;
    create(
        &st,
        serde_json::json!({"id": "urn:ngsi-ld:Vehicle:ja", "type": "Vehicle",
            "rel": {"type": "Relationship", "object": "urn:ngsi-ld:Vehicle:jb"}}),
    )
    .await;
    st.store = Arc::new(crate::common::Double::refusing_doc(
        st.store.clone(),
        antares_store::Kind::Entity,
        "urn:ngsi-ld:Vehicle:jb",
    ));
    for join in ["inline", "flat"] {
        let (status, warnings, body) = send(
            &st,
            "GET",
            &format!("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:ja?join={join}&joinLevel=1"),
            "",
            None,
        )
        .await;
        assert_eq!(status, 200, "{join}: {body}");
        assert!(!warnings.is_empty(), "{join}: no warning, {body}");
    }
}

/// 5.5.7: each forwarded batch item is read under its own @context, so items
/// with different @contexts travel in separate requests.
#[tokio::test(flavor = "multi_thread")]
async fn each_forwarded_batch_item_travels_under_its_own_context() {
    let st = state();
    let (port, seen) = mock(NO_CONTENT, std::time::Duration::ZERO);
    register(&st, port, &["upsertBatch"]).await;
    let body = r#"[
        {"@context": {"speed": "https://a.example/speed"}, "id": "urn:ngsi-ld:Vehicle:c1",
         "type": "Vehicle", "speed": {"type": "Property", "value": 1}},
        {"@context": {"speed": "https://b.example/speed"}, "id": "urn:ngsi-ld:Vehicle:c2",
         "type": "Vehicle", "speed": {"type": "Property", "value": 2}}]"#;
    let (status, _, answer) = send(
        &st,
        "POST",
        "/ngsi-ld/v1/entityOperations/upsert",
        "application/ld+json",
        Some(body.to_owned()),
    )
    .await;
    assert!(status < 300, "{status} {answer}");
    let seen = seen.lock().unwrap().clone();
    let sent: Vec<&String> = seen
        .iter()
        .filter(|r| r.contains("entityOperations"))
        .collect();
    assert_eq!(sent.len(), 2, "{seen:?}");
    for (mine, other, id) in [
        ("a.example", "b.example", "c1"),
        ("b.example", "a.example", "c2"),
    ] {
        let req = sent
            .iter()
            .find(|r| r.contains(&format!("urn:ngsi-ld:Vehicle:{id}")))
            .unwrap();
        assert!(req.contains(mine) && !req.contains(other), "{req}");
    }
}

/// Per-Entity delete forwards run concurrently (bounded by
/// ANTARES_FED_FANOUT), so a batch holds its admission slot for about one
/// round trip, not one per Entity.
#[tokio::test(flavor = "multi_thread")]
async fn per_entity_delete_forwards_run_concurrently() {
    let st = state();
    let ids: Vec<String> = (0..6)
        .map(|i| format!("urn:ngsi-ld:Vehicle:d{i}"))
        .collect();
    for id in &ids {
        create(&st, serde_json::json!({"id": id, "type": "Vehicle"})).await;
    }
    let delay = std::time::Duration::from_millis(300);
    let (port, seen) = mock(NO_CONTENT, delay);
    register(&st, port, &["deleteEntity"]).await;
    let started = std::time::Instant::now();
    let (status, _, answer) = send(
        &st,
        "POST",
        "/ngsi-ld/v1/entityOperations/delete",
        "application/json",
        Some(serde_json::to_string(&ids).unwrap()),
    )
    .await;
    let took = started.elapsed();
    assert!(status < 300, "{status} {answer}");
    assert_eq!(seen.lock().unwrap().len(), ids.len());
    assert!(took < delay * 3, "took {took:?} for {} forwards", ids.len());
}

/// 5.7.2: the POST query reads one page from the store, as its GET twin.
#[tokio::test(flavor = "multi_thread")]
async fn a_post_query_reads_one_page_from_the_store() {
    let mut st = state();
    for i in 0..3 {
        create(
            &st,
            serde_json::json!({"id": format!("urn:ngsi-ld:Vehicle:p{i}"), "type": "Vehicle"}),
        )
        .await;
    }
    let double = Arc::new(crate::common::Double::passthrough(st.store.clone()));
    st.store = double.clone();
    let (status, body) = json(
        &st,
        "POST",
        "/ngsi-ld/v1/entityOperations/query?limit=1",
        r#"{"type":"Query","entities":[{"type":"Vehicle"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        double.unpaged_queries(),
        0,
        "the store was asked for every match"
    );
}
