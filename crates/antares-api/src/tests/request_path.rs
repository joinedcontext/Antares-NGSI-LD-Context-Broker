// SPDX-License-Identifier: EUPL-1.2
//! HTTP-level tests of the core request path: paging Links, the POST query,
//! attribute updates and the batch write admission.

use crate::AppState;
use axum::body::Body;
use axum::http::{HeaderMap, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;

fn state() -> AppState {
    AppState::new("antares-test".into())
}

async fn send(st: &AppState, req: Request<Body>) -> Response<Body> {
    st.call(&HeaderMap::new(), req).await
}

fn with_body(method: &str, uri: &str, ct: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", ct)
        .header("content-length", body.len())
        .body(Body::from(body.to_owned()))
        .expect("req")
}

async fn create(st: &AppState, body: &str) {
    let resp = send(
        st,
        with_body("POST", "/ngsi-ld/v1/entities", "application/json", body),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED, "create {body}");
}

async fn json_of(resp: Response<Body>) -> Value {
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn links(resp: &Response<Body>) -> Vec<String> {
    resp.headers()
        .get_all("link")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_owned))
        .collect()
}

/// 6.3.10: a page Link's type is the media type the request negotiated.
#[tokio::test]
async fn entity_page_links_carry_the_negotiated_media_type() {
    let st = state();
    for i in 0..3 {
        create(&st, &format!(r#"{{"id":"urn:ngsi-ld:T:{i}","type":"T"}}"#)).await;
    }
    for (accept, ty) in [
        ("application/ld+json", "application/ld+json"),
        ("application/geo+json", "application/geo+json"),
        ("application/json", "application/json"),
    ] {
        for extra in ["", "&count=true"] {
            let req = Request::get(format!("/ngsi-ld/v1/entities?type=T&limit=1{extra}"))
                .header("accept", accept)
                .body(Body::empty())
                .expect("req");
            let resp = send(&st, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
            let l = links(&resp);
            let body = json_of(resp).await;
            let shown = match accept {
                "application/geo+json" => &body["features"],
                _ => &body,
            };
            assert_eq!(shown.as_array().map(Vec::len), Some(1), "{body}");
            assert!(
                l.iter()
                    .any(|l| l.contains("rel=\"next\"") && l.contains(&format!("type=\"{ty}\""))),
                "{accept}{extra}: {l:?}"
            );
        }
    }
}

async fn get_entity(st: &AppState, id: &str) -> Value {
    let req = Request::get(format!("/ngsi-ld/v1/entities/{id}?options=sysAttrs"))
        .body(Body::empty())
        .expect("req");
    let resp = send(st, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    json_of(resp).await
}

/// 4.22: `expiresAt` is an Entity member, not an Attribute: an attribute
/// write sets it, the NGSI-LD Null removes it, and `noOverwrite` keeps the
/// one already stored.
#[tokio::test]
async fn attribute_writes_set_expires_at_as_an_entity_member() {
    let st = state();
    let id = "urn:ngsi-ld:T:exp";
    create(
        &st,
        &format!(r#"{{"id":"{id}","type":"T","expiresAt":"2030-01-01T00:00:00Z"}}"#),
    )
    .await;
    let attrs = format!("/ngsi-ld/v1/entities/{id}/attrs");
    let write = |method: &'static str, uri: String, body: &'static str| {
        with_body(method, &uri, "application/json", body)
    };
    let resp = send(
        &st,
        write(
            "PATCH",
            attrs.clone(),
            r#"{"expiresAt":"2031-01-01T00:00:00Z"}"#,
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        get_entity(&st, id).await["expiresAt"],
        "2031-01-01T00:00:00Z"
    );
    let resp = send(
        &st,
        write(
            "POST",
            attrs.clone(),
            r#"{"expiresAt":"2032-01-01T00:00:00Z"}"#,
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        get_entity(&st, id).await["expiresAt"],
        "2032-01-01T00:00:00Z"
    );
    let resp = send(
        &st,
        write(
            "POST",
            format!("{attrs}?options=noOverwrite"),
            r#"{"expiresAt":"2033-01-01T00:00:00Z"}"#,
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::MULTI_STATUS);
    assert_eq!(
        get_entity(&st, id).await["expiresAt"],
        "2032-01-01T00:00:00Z"
    );
    let batch = format!(r#"[{{"id":"{id}","type":"T","expiresAt":"2034-01-01T00:00:00Z"}}]"#);
    let resp = send(
        &st,
        with_body(
            "POST",
            "/ngsi-ld/v1/entityOperations/update?options=noOverwrite",
            "application/json",
            &batch,
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::MULTI_STATUS);
    assert_eq!(
        get_entity(&st, id).await["expiresAt"],
        "2032-01-01T00:00:00Z"
    );
    let resp = send(
        &st,
        write("PATCH", attrs, r#"{"expiresAt":"urn:ngsi-ld:null"}"#),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(get_entity(&st, id).await.get("expiresAt").is_none());
}

/// 5.6.21 Purge with `drop`: the Entity it changes moves its modifiedAt, as
/// every other Attribute delete does.
#[tokio::test]
async fn a_purge_that_drops_an_attribute_moves_modified_at() {
    let st = state();
    let id = "urn:ngsi-ld:T:purge";
    create(
        &st,
        &format!(r#"{{"id":"{id}","type":"T","speed":{{"type":"Property","value":1}}}}"#),
    )
    .await;
    let before = get_entity(&st, id).await["modifiedAt"].clone();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let req = Request::delete("/ngsi-ld/v1/entities?type=T&drop=speed")
        .body(Body::empty())
        .expect("req");
    let resp = send(&st, req).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let after = get_entity(&st, id).await;
    assert!(after.get("speed").is_none(), "{after}");
    assert_ne!(after["modifiedAt"], before, "{after}");
}

/// join=flat on Retrieve Entity answers an array; as geo+json that array is
/// a FeatureCollection, each Entity a Feature.
#[tokio::test]
async fn a_flat_join_retrieve_as_geojson_keeps_every_entity() {
    let st = state();
    create(&st, r#"{"id":"urn:ngsi-ld:T:b","type":"T"}"#).await;
    create(
        &st,
        r#"{"id":"urn:ngsi-ld:T:a","type":"T","rel":{"type":"Relationship","object":"urn:ngsi-ld:T:b"}}"#,
    )
    .await;
    let req = Request::get("/ngsi-ld/v1/entities/urn:ngsi-ld:T:a?join=flat&joinLevel=1")
        .header("accept", "application/geo+json")
        .body(Body::empty())
        .expect("req");
    let resp = send(&st, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_of(resp).await;
    assert_eq!(body["type"], "FeatureCollection", "{body}");
    let ids: Vec<&str> = body["features"]
        .as_array()
        .map(|f| f.iter().filter_map(|x| x["id"].as_str()).collect())
        .unwrap_or_default();
    assert_eq!(ids, ["urn:ngsi-ld:T:a", "urn:ngsi-ld:T:b"], "{body}");
}

/// 6.4.3.2: `scope` is an Entity member an orderBy names like `id`.
#[tokio::test]
async fn order_by_scope_orders_by_the_scope_member() {
    let st = state();
    for (n, scope) in [(1, "/c"), (2, "/a"), (3, "/b")] {
        create(
            &st,
            &format!(r#"{{"id":"urn:ngsi-ld:T:s{n}","type":"T","scope":"{scope}"}}"#),
        )
        .await;
    }
    let req = Request::get("/ngsi-ld/v1/entities?type=T&orderBy=scope")
        .body(Body::empty())
        .expect("req");
    let resp = send(&st, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_of(resp).await;
    let ids: Vec<&str> = body
        .as_array()
        .map(|a| a.iter().filter_map(|x| x["id"].as_str()).collect())
        .unwrap_or_default();
    assert_eq!(
        ids,
        ["urn:ngsi-ld:T:s2", "urn:ngsi-ld:T:s3", "urn:ngsi-ld:T:s1"]
    );
}

/// A dead letter listed through /q shows no credential, whatever shape its
/// key-value pairs were stored in.
#[test]
fn a_dead_letter_is_redacted_in_every_pair_shape() {
    let mut l = serde_json::json!({"uri": "http://h/x",
        "receiverInfo": [{"key": "Authorization", "value": "Bearer s3cret"}],
        "notifierInfo": [["k", "s3cret"], "s3cret"],
        "headers": {"Authorization": "s3cret"}});
    crate::ops::redact_letter(&mut l);
    assert!(!l.to_string().contains("s3cret"), "{l}");
}

/// 6.3.6: a json answer to a POST temporal query compacted with an inline
/// @context links that @context, never the core one.
#[tokio::test]
async fn a_temporal_post_query_links_its_inline_context() {
    let st = state();
    let body = r#"{"@context":[{"speed":"https://example.org/speed"},
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"],
        "type":"Query","entities":[{"type":"T"}],
        "temporalQ":{"timerel":"after","timeAt":"2020-01-01T00:00:00Z"}}"#;
    let mut req = with_body(
        "POST",
        "/ngsi-ld/v1/temporal/entityOperations/query",
        "application/ld+json",
        body,
    );
    req.headers_mut()
        .insert("accept", "application/json".parse().expect("accept"));
    let resp = send(&st, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let l = links(&resp);
    let ctx_link = l
        .iter()
        .find(|l| l.contains("ld#context"))
        .unwrap_or_else(|| panic!("no @context Link: {l:?}"));
    assert!(
        !ctx_link.contains(antares_jsonld::CORE_CONTEXT),
        "{ctx_link}"
    );
}

async fn post_query(st: &AppState, uri: &str, body: &str) -> Response<Body> {
    send(st, with_body("POST", uri, "application/json", body)).await
}

fn ids_of(body: &Value) -> Vec<String> {
    let mut ids: Vec<String> = body
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x["id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

/// 6.3.10: the POST query serializes its next/prev pointers like its GET
/// twin, on its own resource and with its own URI parameters.
#[tokio::test]
async fn a_post_query_answers_its_page_links() {
    let st = state();
    for i in 0..3 {
        create(&st, &format!(r#"{{"id":"urn:ngsi-ld:T:q{i}","type":"T"}}"#)).await;
    }
    let resp = post_query(
        &st,
        "/ngsi-ld/v1/entityOperations/query?limit=1&offset=1",
        r#"{"type":"Query","entities":[{"type":"T"}]}"#,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let l = links(&resp);
    let next = l.iter().find(|l| l.contains("rel=\"next\"")).cloned();
    let next = next.unwrap_or_else(|| panic!("no next Link: {l:?}"));
    assert!(
        next.starts_with("</ngsi-ld/v1/entityOperations/query?limit=1&offset=2>"),
        "{next}"
    );
    assert!(next.contains("type=\"application/json\""), "{next}");
    assert!(l.iter().any(|l| l.contains("rel=\"prev\"")), "{l:?}");
}

/// 5.7.2.4: the POST query runs the validation its GET twin runs.
#[tokio::test]
async fn a_post_query_is_validated_as_its_get_twin() {
    let st = state();
    for (uri, body) in [
        (
            "/ngsi-ld/v1/entityOperations/query",
            r#"{"type":"Query","entities":[{"type":"T"}],"ordering":{"collation":"x-not-a-tag-!"}}"#,
        ),
        (
            "/ngsi-ld/v1/entityOperations/query?local=false",
            r#"{"type":"Query","entities":[{"type":"*"}]}"#,
        ),
    ] {
        let resp = post_query(&st, uri, body).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{uri} {body}");
    }
}

/// 5.2.33: the EntitySelectors of a Query are a union; one selector's id
/// does not widen to the other selector's type.
#[tokio::test]
async fn a_post_query_matches_the_union_of_its_selectors() {
    let st = state();
    for doc in [
        r#"{"id":"urn:ngsi-ld:A:x","type":"A"}"#,
        r#"{"id":"urn:ngsi-ld:A:y","type":"A"}"#,
        r#"{"id":"urn:ngsi-ld:B:z","type":"B"}"#,
    ] {
        create(&st, doc).await;
    }
    for (selectors, want) in [
        (
            r#"[{"type":"A","id":"urn:ngsi-ld:A:x"},{"type":"B"}]"#,
            vec!["urn:ngsi-ld:A:x", "urn:ngsi-ld:B:z"],
        ),
        (
            r#"[{"type":"A","id":"urn:ngsi-ld:A:x"},{"type":"B","id":"urn:ngsi-ld:A:y"}]"#,
            vec!["urn:ngsi-ld:A:x"],
        ),
    ] {
        let resp = post_query(
            &st,
            "/ngsi-ld/v1/entityOperations/query",
            &format!(r#"{{"type":"Query","entities":{selectors}}}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = json_of(resp).await;
        assert_eq!(ids_of(&body), want, "{selectors}");
    }
}

/// 6.3.4: Set Attribute Value takes application/json or application/ld+json
/// like every other write; another media type is 415.
#[tokio::test]
async fn set_attribute_value_refuses_another_media_type() {
    let st = state();
    create(
        &st,
        r#"{"id":"urn:ngsi-ld:T:v","type":"T","speed":{"type":"Property","value":1}}"#,
    )
    .await;
    let uri = "/ngsi-ld/v1/entities/urn:ngsi-ld:T:v/attrs/speed/value";
    let resp = send(&st, with_body("PUT", uri, "text/plain", "5")).await;
    assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let resp = send(&st, with_body("PUT", uri, "application/json", "5")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

/// 4.8: Replace Attribute stamps the sub-Attributes of the instance it writes,
/// as every other write does.
#[tokio::test]
async fn replace_attribute_stamps_its_sub_attributes() {
    let st = state();
    create(
        &st,
        r#"{"id":"urn:ngsi-ld:T:r","type":"T","speed":{"type":"Property","value":1}}"#,
    )
    .await;
    let resp = send(
        &st,
        with_body(
            "PUT",
            "/ngsi-ld/v1/entities/urn:ngsi-ld:T:r/attrs/speed",
            "application/json",
            r#"{"type":"Property","value":3,"accuracy":{"type":"Property","value":4}}"#,
        ),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let doc = get_entity(&st, "urn:ngsi-ld:T:r").await;
    assert!(doc["speed"]["accuracy"]["createdAt"].is_string(), "{doc}");
    assert!(doc["speed"]["accuracy"]["modifiedAt"].is_string(), "{doc}");
}

/// 6.3.2 / RFC 9110 15.5.6: the 405 for a path with an empty segment carries
/// an Allow header (empty: no method applies).
#[tokio::test]
async fn an_empty_segment_405_carries_allow() {
    let st = state();
    for method in ["GET", "PATCH", "DELETE"] {
        let req = Request::builder()
            .method(method)
            .uri("/ngsi-ld/v1/entities/urn:ngsi-ld:T:1/attrs//speed")
            .body(Body::empty())
            .expect("req");
        let resp = send(&st, req).await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED, "{method}");
        assert!(resp.headers().contains_key("allow"), "{method}");
    }
}

/// Table 6.4.3.2-1: the temporal query validates `datasetId` and `attrs` as
/// the entity query does.
#[tokio::test]
async fn temporal_reads_validate_dataset_id_and_attrs() {
    // the temporal query also takes a time window
    let paths = [
        "/ngsi-ld/v1/entities?type=T",
        "/ngsi-ld/v1/temporal/entities?type=T&timerel=after&timeAt=2020-01-01T00:00:00Z",
    ];
    let st = state();
    for path in paths {
        let req = Request::get(format!("{path}&attrs=speed&datasetId=urn:d"))
            .body(Body::empty())
            .expect("req");
        assert_eq!(send(&st, req).await.status(), StatusCode::OK, "{path}");
    }
    for q in ["datasetId=not%20a%20uri", "attrs=id"] {
        for path in paths {
            let req = Request::get(format!("{path}&{q}"))
                .body(Body::empty())
                .expect("req");
            let resp = send(&st, req).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{path}&{q}");
        }
    }
}
