// SPDX-License-Identifier: EUPL-1.2
//! A broker that requires a subject and believes no gateway
//! (`provenance_from` = `None`): a subject header from a caller outside the
//! process proves nothing, so the request is served only with the admin
//! bearer. Its own binary: the subject header list is read once per process.

use antares_api::admin_auth::AdminAuth;
use antares_api::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const SECRET: &str = "a-test-secret-of-at-least-32-bytes!";

async fn status(st: &AppState, subject: Option<&str>, authorization: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .method("GET")
        .uri("/ngsi-ld/v1/csourceRegistrations?type=Car");
    if let Some(s) = subject {
        req = req.header("X-Subject", s);
    }
    if let Some(a) = authorization {
        req = req.header("Authorization", a);
    }
    antares_api::router(st.clone())
        .oneshot(req.body(Body::empty()).expect("request"))
        .await
        .expect("response")
        .status()
}

#[tokio::test]
async fn a_subject_header_from_an_unnamed_caller_is_not_a_vouched_subject() {
    std::env::set_var("ANTARES_POLICY_SUBJECT_HEADERS", "X-Subject");
    let mut st = AppState::new("admin-auth".into())
        .with_admin_auth(AdminAuth::new(SECRET, false).expect("secret"));
    st.subject_required = true;
    assert!(st.provenance_from.is_none(), "no gateway is believed");
    for subject in ["admin", ""] {
        assert_eq!(
            status(&st, Some(subject), None).await,
            StatusCode::UNAUTHORIZED,
            "X-Subject: {subject:?}"
        );
    }
    // the admin bearer still serves it, with or without the header
    let good = format!("Bearer {SECRET}");
    assert_eq!(
        status(&st, Some("admin"), Some(&good)).await,
        StatusCode::OK
    );
    assert_eq!(status(&st, None, Some(&good)).await, StatusCode::OK);
    // and a broker that does not require a subject serves it as before
    st.subject_required = false;
    assert_eq!(status(&st, Some("admin"), None).await, StatusCode::OK);
}
