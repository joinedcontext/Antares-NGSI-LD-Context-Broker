// SPDX-License-Identifier: EUPL-1.2
//! 4.3.6.5: `contextSourceInfo` carries the material a forward is sent with
//! (an Authorization header among it), and the endpoint may carry userinfo.
//! Retrieve and Query Context Source Registrations (5.10) answer the keys,
//! never those values, as a Subscription's `receiverInfo` is answered; a
//! client writing the answer back keeps the stored credential.

use antares_api::AppState;
use antares_model::TenantId;
use antares_store::Kind;
use axum::body::Body;
use axum::http::Request;
use serde_json::{json, Value};
use tower::ServiceExt;

const ID: &str = "urn:ngsi-ld:ContextSourceRegistration:masked";

async fn call(st: &AppState, method: &str, uri: &str, body: Option<Value>) -> (u16, Value) {
    let body = body.map(|b| b.to_string());
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
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn stored(st: &AppState) -> Value {
    let t = TenantId::new(TenantId::DEFAULT).expect("tenant");
    st.store
        .get(&t, Kind::Registration, ID)
        .await
        .expect("store")
        .expect("registration")
}

#[tokio::test]
async fn registration_reads_mask_forwarding_credentials() {
    let st = AppState::new("antares1".into());
    let reg = json!({
        "id": ID,
        "type": "ContextSourceRegistration",
        "endpoint": "http://user:pw@peer.example:9090",
        "information": [{"entities": [{"type": "Building"}]}],
        "contextSourceInfo": [
            {"key": "Authorization", "value": "Bearer s3cr3t"},
            {"key": "jsonldContext", "value": "https://example.org/ctx.jsonld"},
            {"key": "X-Caller", "value": "urn:ngsi-ld:request"}
        ]
    });
    let (status, _) = call(&st, "POST", "/ngsi-ld/v1/csourceRegistrations", Some(reg)).await;
    assert_eq!(status, 201);

    let (status, one) = call(
        &st,
        "GET",
        &format!("/ngsi-ld/v1/csourceRegistrations/{ID}"),
        None,
    )
    .await;
    assert_eq!(status, 200);
    let (status, list) = call(
        &st,
        "GET",
        "/ngsi-ld/v1/csourceRegistrations?type=Building",
        None,
    )
    .await;
    assert_eq!(status, 200);
    for shown in [&one, &list[0]] {
        let text = shown.to_string();
        assert!(!text.contains("s3cr3t"), "credential answered: {text}");
        assert!(
            !text.contains("user:pw"),
            "endpoint userinfo answered: {text}"
        );
        assert_eq!(shown["endpoint"], "http://***@peer.example:9090", "{text}");
        let csi = shown["contextSourceInfo"].as_array().expect("csi");
        assert_eq!(csi[0], json!({"key": "Authorization", "value": "***"}));
        // what the broker processes and the copy instruction stay readable
        assert_eq!(csi[1]["value"], "https://example.org/ctx.jsonld");
        assert_eq!(csi[2]["value"], "urn:ngsi-ld:request");
    }

    // the answer written back keeps the stored credential and endpoint
    let back = json!({
        "endpoint": one["endpoint"].clone(),
        "contextSourceInfo": one["contextSourceInfo"].clone(),
    });
    let (status, _) = call(
        &st,
        "PATCH",
        &format!("/ngsi-ld/v1/csourceRegistrations/{ID}"),
        Some(back),
    )
    .await;
    assert_eq!(status, 204);
    let held = stored(&st).await;
    assert_eq!(held["endpoint"], "http://user:pw@peer.example:9090");
    assert_eq!(held["contextSourceInfo"][0]["value"], "Bearer s3cr3t");

    // a masked value cannot follow the registration to another endpoint
    let moved = json!({
        "endpoint": "http://elsewhere.example:9090",
        "contextSourceInfo": [{"key": "Authorization", "value": "***"}],
    });
    let (status, _) = call(
        &st,
        "PATCH",
        &format!("/ngsi-ld/v1/csourceRegistrations/{ID}"),
        Some(moved),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        stored(&st).await["contextSourceInfo"][0]["value"],
        "Bearer s3cr3t"
    );
}

/// A stored credential stays with its endpoint: a fragment that moves the
/// registration to another endpoint and leaves `contextSourceInfo` out is
/// refused while a credential is stored, and the client sends the values
/// for the new endpoint (or none). Without a stored credential the move
/// keeps what the broker processes.
#[tokio::test]
async fn a_stored_credential_does_not_follow_a_new_endpoint() {
    let st = AppState::new("antares1".into());
    let at = format!("/ngsi-ld/v1/csourceRegistrations/{ID}");
    let reg = json!({
        "id": ID,
        "type": "ContextSourceRegistration",
        "endpoint": "http://peer.example:9090",
        "information": [{"entities": [{"type": "Building"}]}],
        "contextSourceInfo": [
            {"key": "Authorization", "value": "Bearer s3cr3t"},
            {"key": "jsonldContext", "value": "https://example.org/ctx.jsonld"}
        ]
    });
    let (status, _) = call(&st, "POST", "/ngsi-ld/v1/csourceRegistrations", Some(reg)).await;
    assert_eq!(status, 201);

    let moved = json!({"endpoint": "http://elsewhere.example:9090"});
    let (status, body) = call(&st, "PATCH", &at, Some(moved)).await;
    assert_eq!(status, 400, "{body}");
    let held = stored(&st).await;
    assert_eq!(held["endpoint"], "http://peer.example:9090");
    assert_eq!(held["contextSourceInfo"][0]["value"], "Bearer s3cr3t");

    // the same endpoint, written again, keeps the credential
    let same = json!({"endpoint": "http://peer.example:9090"});
    let (status, body) = call(&st, "PATCH", &at, Some(same)).await;
    assert_eq!(status, 204, "{body}");
    assert_eq!(
        stored(&st).await["contextSourceInfo"][0]["value"],
        "Bearer s3cr3t"
    );

    // the move with the values for the new endpoint is taken
    let resent = json!({
        "endpoint": "http://elsewhere.example:9090",
        "contextSourceInfo": [
            {"key": "Authorization", "value": "Bearer other"},
            {"key": "jsonldContext", "value": "https://example.org/ctx.jsonld"}
        ]
    });
    let (status, body) = call(&st, "PATCH", &at, Some(resent)).await;
    assert_eq!(status, 204, "{body}");
    let held = stored(&st).await;
    assert_eq!(held["endpoint"], "http://elsewhere.example:9090");
    assert_eq!(held["contextSourceInfo"][0]["value"], "Bearer other");

    // no credential stored: the move keeps the processed keys
    let processed = json!({
        "contextSourceInfo": [
            {"key": "jsonldContext", "value": "https://example.org/ctx.jsonld"}
        ]
    });
    let (status, body) = call(&st, "PATCH", &at, Some(processed)).await;
    assert_eq!(status, 204, "{body}");
    let back = json!({"endpoint": "http://peer.example:9090"});
    let (status, body) = call(&st, "PATCH", &at, Some(back)).await;
    assert_eq!(status, 204, "{body}");
    let held = stored(&st).await;
    assert_eq!(held["endpoint"], "http://peer.example:9090");
    assert_eq!(
        held["contextSourceInfo"][0]["value"],
        "https://example.org/ctx.jsonld"
    );
}
