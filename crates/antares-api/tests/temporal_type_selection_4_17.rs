// SPDX-License-Identifier: EUPL-1.2
//! 4.17 Entity Type Selection on Query Temporal Evolution of Entities
//! (5.7.4): `,` and `|` are OR, a parenthesised `;` group is AND, and `*`
//! (Table 6.4.3.2-1) selects every type. The selector is parsed, never
//! taken as one type name.

use antares_api::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(st: &AppState, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let resp = antares_api::router(st.clone())
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

async fn seed(st: &AppState, id: &str, types: Value) {
    let doc = json!({"id": id, "type": types,
        "speed": [{"type": "Property", "value": 1, "observedAt": "2026-01-01T02:00:00Z"}]});
    let (s, b) = call(st, "POST", "/ngsi-ld/v1/temporal/entities", Some(doc)).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
}

async fn ids(st: &AppState, selector: &str) -> Vec<String> {
    let uri = format!(
        "/ngsi-ld/v1/temporal/entities?type={selector}&timerel=after&timeAt=2026-01-01T00:00:00Z"
    );
    let (s, b) = call(st, "GET", &uri, None).await;
    assert_eq!(s, StatusCode::OK, "{selector}: {b}");
    let mut ids: Vec<String> = b
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|d| d["id"].as_str().map(str::to_owned))
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn clause_4_17_type_selection_on_the_temporal_query() {
    let st = AppState::new("ts".into());
    seed(&st, "urn:ngsi-ld:X:both", json!(["Home", "Vehicle"])).await;
    seed(&st, "urn:ngsi-ld:X:home", json!("Home")).await;
    seed(&st, "urn:ngsi-ld:X:car", json!("Car")).await;
    assert_eq!(ids(&st, "(Home;Vehicle)").await, ["urn:ngsi-ld:X:both"]);
    assert_eq!(
        ids(&st, "Car|(Home;Vehicle)").await,
        ["urn:ngsi-ld:X:both", "urn:ngsi-ld:X:car"]
    );
    assert_eq!(
        ids(&st, "Car,Home").await,
        [
            "urn:ngsi-ld:X:both",
            "urn:ngsi-ld:X:car",
            "urn:ngsi-ld:X:home"
        ]
    );
    assert_eq!(
        ids(&st, "*").await,
        [
            "urn:ngsi-ld:X:both",
            "urn:ngsi-ld:X:car",
            "urn:ngsi-ld:X:home"
        ]
    );
    // the AND group pages too: one Entity per page, each page the right one
    let uri = "/ngsi-ld/v1/temporal/entities?type=(Home;Vehicle)|Car\
               &timerel=after&timeAt=2026-01-01T00:00:00Z&limit=1&count=true";
    let (s, b) = call(&st, "GET", uri, None).await;
    assert!(s.is_success(), "{b}");
    assert_eq!(b.as_array().map(Vec::len), Some(1), "{b}");
}

/// 5.5.7: an application/ld+json body carries its own @context, and the
/// POST temporal query (5.7.4 via 6.24.3.1) resolves the Query's types
/// against it. Read in the core context, `Car` names the default-context
/// type and the query finds nothing.
#[tokio::test]
async fn the_post_temporal_query_reads_types_in_the_body_context() {
    let st = AppState::new("ts-ctx".into());
    let ctx = json!({"Car": "https://example.org/ns/Car", "rpm": "https://example.org/ns/rpm"});
    let ld = |uri: &'static str, body: Value| {
        let st = st.clone();
        async move {
            let body = body.to_string();
            let resp = antares_api::router(st)
                .oneshot(
                    Request::builder()
                        .method("POST")
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
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            )
        }
    };
    let (s, b) = ld(
        "/ngsi-ld/v1/temporal/entities",
        json!({"@context": ctx, "id": "urn:ngsi-ld:Car:ctx", "type": "Car",
               "rpm": [{"type": "Property", "value": 1, "observedAt": "2026-01-01T02:00:00Z"}]}),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let (s, b) = ld(
        "/ngsi-ld/v1/temporal/entityOperations/query",
        json!({"@context": ctx, "type": "Query", "entities": [{"type": "Car"}],
               "temporalQ": {"timerel": "after", "timeAt": "2026-01-01T00:00:00Z"}}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let ids: Vec<&str> = b
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|d| d["id"].as_str())
        .collect();
    assert_eq!(ids, ["urn:ngsi-ld:Car:ctx"], "{b}");
}

/// Table 6.4.3.2-1: `type=*` implies local scope and shall not be combined
/// with `local=false`, on the temporal query and the purge as on the entity
/// query.
#[tokio::test]
async fn type_star_with_local_false_is_refused() {
    let st = AppState::new("ts-star-local".into());
    for (method, uri) in [
        (
            "GET",
            "/ngsi-ld/v1/temporal/entities?type=*&local=false&timerel=after&timeAt=2026-01-01T00:00:00Z",
        ),
        ("DELETE", "/ngsi-ld/v1/entities?type=*&local=false"),
    ] {
        let (s, b) = call(&st, method, uri, None).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{method} {uri}: {b}");
    }
}
