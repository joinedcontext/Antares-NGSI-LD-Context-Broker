// SPDX-License-Identifier: EUPL-1.2
//! The memory/file store's per-Attribute history cap
//! (`ANTARES_MEM_MAX_INSTANCES`), end to end: an Entity's whole history is
//! one document there, rewritten on every append, so it is bounded. A
//! temporal API write past the cap is refused and writes nothing; an Entity
//! write still commits, and only its history stops growing.

use antares_api::AppState;
use antares_sql::store::any::AnyStore;
use antares_sql::store::Store;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use std::sync::Arc;
use tower::ServiceExt;

async fn send(
    st: &AppState,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> (StatusCode, String) {
    let b = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(body) => b
            .header("Content-Type", "application/json")
            .header("Content-Length", body.len())
            .body(Body::from(body)),
        None => b.body(Body::empty()),
    }
    .expect("request");
    let resp = antares_api::router(st.clone())
        .oneshot(req)
        .await
        .expect("response");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn v(i: u32) -> String {
    format!(r#"{{"type":"Property","value":{i},"observedAt":"2026-08-08T14:00:{i:02}Z"}}"#)
}

async fn history(st: &AppState) -> Vec<i64> {
    let (status, body) = send(
        st,
        "GET",
        "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Cap:1?lastN=100",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let doc: serde_json::Value = serde_json::from_str(&body).expect("json");
    let mut got: Vec<i64> = doc["v"]
        .as_array()
        .map(|a| a.iter().filter_map(|i| i["value"].as_i64()).collect())
        .unwrap_or_default();
    got.sort_unstable();
    got
}

#[tokio::test(flavor = "multi_thread")]
async fn the_history_cap_refuses_temporal_writes_and_stops_recording_entity_writes() {
    let mut st = AppState::with_store(
        "test".into(),
        Arc::new(AnyStore::Mem(Store::default()).max_instances(3)),
        "memory",
    );
    antares_api::wire(&mut st).await;
    let (status, body) = send(
        &st,
        "POST",
        "/ngsi-ld/v1/entities",
        Some(format!(
            r#"{{"id":"urn:ngsi-ld:Cap:1","type":"Cap","v":{}}}"#,
            v(1)
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for i in [2, 3] {
        let (status, body) = send(
            &st,
            "PATCH",
            "/ngsi-ld/v1/entities/urn:ngsi-ld:Cap:1/attrs/v",
            Some(v(i)),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    }
    assert_eq!(history(&st).await, [1, 2, 3]);

    // 5.6.12 past the cap: refused, and the reason is the cap
    let (status, body) = send(
        &st,
        "POST",
        "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:Cap:1/attrs",
        Some(format!(r#"{{"v":[{}]}}"#, v(4))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.contains("BadRequestData") && body.contains("3 instances"),
        "{body}"
    );
    assert_eq!(history(&st).await, [1, 2, 3], "the refused write wrote");

    // an Entity write still commits; its history is what stops
    let (status, body) = send(
        &st,
        "PATCH",
        "/ngsi-ld/v1/entities/urn:ngsi-ld:Cap:1/attrs/v",
        Some(v(5)),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let (status, body) = send(&st, "GET", "/ngsi-ld/v1/entities/urn:ngsi-ld:Cap:1", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(r#""value":5"#), "{body}");
    assert_eq!(history(&st).await, [1, 2, 3]);
}
