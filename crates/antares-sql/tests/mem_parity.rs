// SPDX-License-Identifier: EUPL-1.2
//! The memory arm answers what the Postgres arm answers, in the corners the
//! shared driver contract does not reach.

use antares_model::TenantId;
use antares_sql::store::any::AnyStore;
use antares_sql::store::Store;
use antares_store::Kind;
use serde_json::json;

/// 4.22: a limit/offset window over Entities neither serves nor counts an
/// expired Entity, and strips expired instances, as `list_page` does.
#[tokio::test]
async fn list_slice_hides_expired_entities() {
    let s = AnyStore::Mem(Store::default());
    let t = TenantId::default();
    let gone = json!({"id": "urn:x:1", "type": ["T"], "expiresAt": "2000-01-01T00:00:00Z"});
    let live = json!({"id": "urn:x:2", "type": ["T"],
                      "https://e.org/a": [{"type": "Property", "value": 1,
                                           "expiresAt": "2000-01-01T00:00:00Z"}],
                      "https://e.org/b": [{"type": "Property", "value": 2}]});
    for (id, doc) in [("urn:x:1", gone), ("urn:x:2", live)] {
        assert!(s.create(&t, Kind::Entity, id, doc).await.expect("create"));
    }
    let (rows, total) = s.list_slice(&t, Kind::Entity, 0, 10).await.expect("slice");
    assert_eq!(total, 1, "{rows:?}");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "urn:x:2");
    assert!(rows[0].get("https://e.org/a").is_none(), "{rows:?}");
}
