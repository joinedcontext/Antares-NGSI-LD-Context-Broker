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

/// An erasure with a `before` cut reads the instant an instance is keyed
/// by (observedAt, else modifiedAt, deletedAt, createdAt), as the Postgres
/// arm's `observed_at` column does, and an Attribute left with no instance
/// is gone.
#[tokio::test]
async fn erase_before_reads_the_instance_instant_and_drops_emptied_attributes() {
    use antares_store::TemporalDriver;
    let s = AnyStore::Mem(Store::default());
    let t = TenantId::default();
    let id = "urn:x:erase";
    let doc = json!({"id": id, "type": ["T"],
        "https://e.org/a": [{"type": "Property", "value": 1, "instanceId": "urn:i:1",
                             "modifiedAt": "2020-01-01T00:00:00Z"}],
        "https://e.org/b": [{"type": "Property", "value": 2, "instanceId": "urn:i:2",
                             "observedAt": "2030-01-01T00:00:00Z"}]});
    assert!(s.create(&t, Kind::Temporal, id, doc).await.expect("create"));
    let n = TemporalDriver::erase(&s, &t, id, None, Some("2025-01-01T00:00:00Z"))
        .await
        .expect("erase");
    assert_eq!(n, 1, "the modifiedAt-keyed instance is before the cut");
    let left = s
        .get(&t, Kind::Temporal, id)
        .await
        .expect("get")
        .expect("history left");
    assert!(left.get("https://e.org/a").is_none(), "{left}");
    assert!(left.get("https://e.org/b").is_some(), "{left}");
}
