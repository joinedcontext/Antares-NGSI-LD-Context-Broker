// SPDX-License-Identifier: EUPL-1.2
//! Grammar edges of the entry checks the review named.
#![allow(clippy::unwrap_used)]
use ngsild_model::EntityId;

/// Table 5.2.4-1: an Entity id is a URI, and an RFC 3986 scheme starts with
/// a letter (clause 3.1).
#[test]
fn scheme_must_start_with_a_letter() {
    assert!(EntityId::new("1:x").is_err());
    assert!(EntityId::new("+:x").is_err());
    assert!(EntityId::new("urn:ngsi-ld:T:1").is_ok());
    assert!(EntityId::new("a1+.-:x").is_ok());
}
