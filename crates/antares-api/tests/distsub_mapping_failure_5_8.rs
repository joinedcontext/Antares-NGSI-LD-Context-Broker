// SPDX-License-Identifier: EUPL-1.2
//! 5.8.1.4 / 5.8.2.4: the mapping of a distributed Subscription to its
//! internal Registration Subscription and its remote copies lives in the
//! store. A read of it that fails is not "no mapping": an update made then
//! creates no second Registration Subscription and keeps the stored one.

use crate::common;
use antares_api::AppState;
use antares_model::TenantId;
use antares_store::Kind;
use axum::body::Body;
use axum::http::Request;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

const OWN: &str = "urn:ngsi-ld:Subscription:mapped";
const PREFIX: &str = "urn:ngsi-ld:CSourceSubscription:distsub:";

async fn call(st: &AppState, method: &str, uri: &str, body: Value) -> u16 {
    let body = body.to_string();
    antares_api::router(st.clone())
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("Content-Type", "application/json")
                .header("Content-Length", body.len())
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response")
        .status()
        .as_u16()
}

async fn internal_subscriptions(st: &AppState) -> Vec<String> {
    st.store
        .list(&TenantId::default(), Kind::DistSub)
        .await
        .expect("list")
        .iter()
        .filter_map(|d| d.get("id").and_then(Value::as_str))
        .filter(|id| id.starts_with(PREFIX))
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_mapping_creates_no_second_registration_subscription() {
    let mut st = AppState::new("antares-ds-mapping".into());
    antares_api::wire(&mut st).await;
    let status = call(
        &st,
        "POST",
        "/ngsi-ld/v1/subscriptions",
        json!({"id": OWN, "type": "Subscription", "entities": [{"type": "Vehicle"}],
            "notification": {"endpoint": {"uri": "http://127.0.0.1:9/n"}}}),
    )
    .await;
    assert_eq!(status, 201);
    let before = internal_subscriptions(&st).await;
    assert_eq!(before.len(), 1, "{before:?}");
    let base = st.store.clone();
    st.store = Arc::new(common::Double::refusing_doc(
        base.clone(),
        Kind::DistSub,
        OWN,
    ));
    let status = call(
        &st,
        "PATCH",
        &format!("/ngsi-ld/v1/subscriptions/{OWN}"),
        json!({"entities": [{"type": "Vehicle"}, {"type": "Bike"}]}),
    )
    .await;
    assert_eq!(status, 204);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    st.store = base;
    assert_eq!(
        internal_subscriptions(&st).await,
        before,
        "a second one was created"
    );
    let mapping = st
        .store
        .get(&TenantId::default(), Kind::DistSub, OWN)
        .await
        .expect("read")
        .expect("mapping");
    assert_eq!(mapping["csr_sub"], json!(before[0]), "{mapping}");
}
