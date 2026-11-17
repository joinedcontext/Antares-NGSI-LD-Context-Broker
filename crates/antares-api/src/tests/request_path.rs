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
