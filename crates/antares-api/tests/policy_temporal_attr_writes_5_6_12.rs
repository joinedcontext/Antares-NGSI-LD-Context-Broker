// SPDX-License-Identifier: EUPL-1.2
//! The temporal Attribute writes (5.6.12 add, 5.6.13 delete, 5.6.14
//! modify an instance, 5.6.15 delete an instance) ask the policy engine with
//! what their Core API twins (5.6.3, 5.6.5, 5.6.4) give it: the Attribute
//! they write and, where one comes, the expanded body. Asked with the id
//! alone, an engine guarding one Attribute could not refuse a write of it
//! through the history.

use antares_api::policy::{
    Decision, DecisionFuture, NotifyDecision, Operation, PolicyEngine, Subject,
};
use antares_api::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

const CTX: &str = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld";
const DEFAULT: &str = "https://uri.etsi.org/ngsi-ld/default-context/";
const ID: &str = "urn:ngsi-ld:Vehicle:t1";

/// Each question about a temporal Attribute write: its clause, its
/// Attributes and whether a body came.
type Asked = Arc<Mutex<Vec<(String, Vec<String>, bool)>>>;

struct Recorder(Asked);

impl PolicyEngine for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }
    fn decide<'a>(&'a self, _s: &'a Subject, op: &'a Operation<'a>) -> DecisionFuture<'a> {
        if ["5.6.12", "5.6.13", "5.6.14", "5.6.15"].contains(&op.clause) {
            self.0.lock().expect("lock").push((
                op.clause.to_owned(),
                op.attrs.to_vec(),
                op.body.is_some(),
            ));
        }
        Box::pin(std::future::ready(Decision::Allow))
    }
    fn pre_notify(&self, _s: &Subject, _sub: &Value, _n: &mut Value) -> NotifyDecision {
        NotifyDecision::Deliver
    }
}

async fn call(st: &AppState, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let resp = antares_api::router(st.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("Content-Type", "application/ld+json")
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

#[tokio::test]
async fn the_engine_sees_the_attribute_and_body_of_each_temporal_attribute_write() {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    let st = AppState::new("t".into()).with_policy(Arc::new(Recorder(asked.clone())));
    let (s, b) = call(
        &st,
        "POST",
        "/ngsi-ld/v1/temporal/entities",
        Some(json!({"id": ID, "type": "Vehicle", "@context": CTX,
                    "speed": [{"type": "Property", "value": 1,
                               "observedAt": "2026-01-01T00:00:00Z"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let (s, b) = call(
        &st,
        "POST",
        &format!("/ngsi-ld/v1/temporal/entities/{ID}/attrs"),
        Some(json!({"@context": CTX,
                    "secret": [{"type": "Property", "value": 7,
                                "observedAt": "2026-01-01T01:00:00Z"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, evolution) = call(
        &st,
        "GET",
        &format!("/ngsi-ld/v1/temporal/entities/{ID}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{evolution}");
    let instance = evolution["speed"][0]["instanceId"]
        .as_str()
        .expect("an instanceId")
        .to_owned();
    let (s, b) = call(
        &st,
        "PATCH",
        &format!("/ngsi-ld/v1/temporal/entities/{ID}/attrs/speed/{instance}"),
        Some(json!({"@context": CTX,
                    "speed": [{"type": "Property", "value": 2,
                               "observedAt": "2026-01-01T00:00:00Z"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, b) = call(
        &st,
        "DELETE",
        &format!("/ngsi-ld/v1/temporal/entities/{ID}/attrs/speed/{instance}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let (s, b) = call(
        &st,
        "DELETE",
        &format!("/ngsi-ld/v1/temporal/entities/{ID}/attrs/secret"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT, "{b}");
    let asked = asked.lock().expect("lock").clone();
    let (speed, secret) = (format!("{DEFAULT}speed"), format!("{DEFAULT}secret"));
    assert_eq!(
        asked,
        vec![
            ("5.6.12".to_owned(), vec![secret.clone()], true),
            ("5.6.14".to_owned(), vec![speed.clone()], true),
            ("5.6.15".to_owned(), vec![speed], false),
            ("5.6.13".to_owned(), vec![secret], false),
        ]
    );
}
