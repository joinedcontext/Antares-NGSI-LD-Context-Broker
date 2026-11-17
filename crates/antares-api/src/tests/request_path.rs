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
