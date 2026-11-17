// SPDX-License-Identifier: EUPL-1.2
//! A registration's forwarding credentials are sealed like a subscription's
//! notification credentials, and the index copy of a registration holds
//! none of them.

#![cfg(feature = "postgres")]

use antares_sql::store::pg::doc::{index_rows, index_shared, DocKind};
use antares_sql::store::pg::seal::{self, Row, SecretKey};
use serde_json::json;

#[test]
fn registration_credentials_are_sealed_and_not_indexed() {
    // test-only key, 64 hex digits
    let key = SecretKey::from_base64(&"11".repeat(32)).expect("key");
    let id = "urn:ngsi-ld:ContextSourceRegistration:r1";
    let doc = json!({
        "id": id, "type": "ContextSourceRegistration",
        "endpoint": "http://user:pw-in-uri@cs:9090",
        "contextSourceInfo": [{"key": "Authorization", "value": "Bearer tok-in-csi"}],
        "information": [{"entities": [{"type": "https://e.org/V"}]}]
    });
    // control: a Subscription's receiverInfo is sealed under the same key
    let sub = json!({"id": "urn:s", "notification": {"endpoint": {
        "uri": "http://user:pw-in-uri@h/n",
        "receiverInfo": [{"key": "Authorization", "value": "Bearer tok-in-csi"}]}}});
    let sub_row = Row {
        kind: DocKind::Subscription,
        tenant: "t",
        id: "urn:s",
    };
    let leaks = |s: &str| s.contains("tok-in-csi") || s.contains("pw-in-uri");
    let sub_stored = seal::sealed(Some(&key), sub_row, &sub)
        .expect("seal")
        .to_string();
    assert!(!leaks(&sub_stored));

    let row = Row {
        kind: DocKind::Registration,
        tenant: "t",
        id,
    };
    let stored = seal::sealed(Some(&key), row, &doc)
        .expect("seal")
        .to_string();
    let rows = index_rows(&doc);
    let index = format!("{:?} {:?}", rows, index_shared(&doc, rows.len()));
    assert!(
        !leaks(&stored),
        "registration row holds plaintext credentials"
    );
    assert!(
        !leaks(&index),
        "csource_index rows hold plaintext credentials"
    );
}
