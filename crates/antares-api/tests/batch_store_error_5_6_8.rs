// SPDX-License-Identifier: EUPL-1.2
//! A store error in a later step of a batch write, after an earlier step
//! committed: 5.6.8.4 / 5.6.9.4 report each Entity's outcome, so the
//! committed members stay in S and the failed step's members go to E with
//! the error (207). A whole-request 5xx would tell the client nothing was
//! applied, and its retry would replay the writes that already landed. A
//! batch whose first write already fails has committed nothing, and answers
//! the store error as before.

use crate::common;
use antares_api::AppState;
use antares_store::Kind;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const OLD: &str = "urn:ngsi-ld:Vehicle:be-old";
const NEW: &str = "urn:ngsi-ld:Vehicle:be-new";

async fn send(st: &AppState, uri: &str, body: Value) -> (StatusCode, Value) {
    let body = body.to_string();
    let resp = antares_api::router(st.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("Content-Type", "application/json")
                .header("Content-Length", body.len())
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn vehicle(id: &str, speed: i64) -> Value {
    json!({"id": id, "type": "Vehicle", "speed": {"type": "Property", "value": speed}})
}

#[tokio::test(flavor = "multi_thread")]
async fn a_store_error_after_a_committed_step_is_a_207_naming_both() {
    let mut st = AppState::new("batch-store-error".into());
    let (s, b) = send(
        &st,
        "/ngsi-ld/v1/entityOperations/create",
        json!([vehicle(OLD, 1)]),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    st.store = Arc::new(common::Double::refusing_create(
        st.store.clone(),
        Kind::Entity,
        NEW,
    ));

    // options=update: OLD is updated in one step (committed), NEW falls
    // through to the replace step, whose INSERT the store refuses
    let (s, b) = send(
        &st,
        "/ngsi-ld/v1/entityOperations/upsert?options=update",
        json!([vehicle(OLD, 2), vehicle(NEW, 3)]),
    )
    .await;
    assert_eq!(s, StatusCode::MULTI_STATUS, "{b}");
    assert_eq!(b["success"], json!([OLD]), "{b}");
    let errors = b["errors"].as_array().expect("errors");
    assert_eq!(errors.len(), 1, "{b}");
    assert_eq!(errors[0]["entityId"], NEW, "{b}");
    let speed = st
        .store
        .get(&Default::default(), Kind::Entity, OLD)
        .await
        .expect("read")
        .expect("OLD");
    assert_eq!(
        speed["https://uri.etsi.org/ngsi-ld/default-context/speed"][0]["value"], 2,
        "the committed step stands: {speed}"
    );

    // a batch whose one write fails committed nothing: the store error
    let (s, b) = send(
        &st,
        "/ngsi-ld/v1/entityOperations/upsert",
        json!([vehicle(NEW, 4)]),
    )
    .await;
    assert!(s.is_server_error(), "{s}: {b}");
}
