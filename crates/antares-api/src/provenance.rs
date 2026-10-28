// SPDX-License-Identifier: EUPL-1.2
//! Who and what wrote a request's history: the `provenance` member a
//! history row carries. This broker reads no provenance header; every write
//! through the API is recorded as `channel: api`.

use axum::http::HeaderMap;
use serde_json::{json, Value};

/// The provenance member that marks a row kept because its write was
/// audited.
pub const AUDIT: &str = "audit";

/// Whether `provenance` is an audited write's.
pub fn audited(provenance: &Value) -> bool {
    provenance.get(AUDIT) == Some(&Value::Bool(true))
}

/// No header is read, every write is `channel: api`.
pub fn of_request(_headers: &HeaderMap, _gateway: Option<&str>) -> Value {
    json!({"channel": "api"})
}

/// `provenance` with its `operation`.
pub fn with_operation(provenance: &Value, operation: &str) -> Value {
    let mut out = provenance.clone();
    if let Some(o) = out.as_object_mut() {
        o.insert("operation".into(), json!(operation));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_request_is_the_api_channel() {
        let mut h = HeaderMap::new();
        h.insert("x-request-id", "4f1c-9a07".parse().expect("header"));
        assert_eq!(of_request(&h, None), json!({"channel": "api"}));
    }

    #[test]
    fn the_operation_is_added() {
        assert_eq!(
            with_operation(&json!({"channel": "ui"}), "delete"),
            json!({"channel": "ui", "operation": "delete"})
        );
    }
}
