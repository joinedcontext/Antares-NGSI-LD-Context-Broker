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

/// A temporal query is narrowed by its ids, types and attrs before the
/// memory arm clones a document, as the Postgres arm's WHERE narrows it:
/// what comes back is the matching history, not the tenant's.
#[tokio::test]
async fn temporal_query_clones_only_the_matching_history() {
    let s = AnyStore::Mem(Store::default());
    let t = TenantId::default();
    for (id, ty, attr) in [
        ("urn:x:t1", "https://e.org/A", "https://e.org/a"),
        ("urn:x:t2", "https://e.org/B", "https://e.org/a"),
        ("urn:x:t3", "https://e.org/A", "https://e.org/b"),
    ] {
        let doc = json!({"id": id, "type": [ty],
                         attr: [{"type": "Property", "value": 1, "instanceId": format!("{id}:i"),
                                 "observedAt": "2026-01-01T00:00:00Z"}]});
        assert!(s.create(&t, Kind::Temporal, id, doc).await.expect("create"));
    }
    let ids = ["urn:x:t1", "urn:x:t2"];
    let types = ["https://e.org/A".to_owned()];
    let attrs = ["https://e.org/a".to_owned()];
    let by_ids = antares_store::filter::TemporalFilter {
        ids: Some(&ids),
        ..Default::default()
    };
    assert_eq!(ids_of(&s, &t, &by_ids).await, ["urn:x:t1", "urn:x:t2"]);
    let by_type_and_attr = antares_store::filter::TemporalFilter {
        types: Some(&types),
        attrs: Some(&attrs),
        ..Default::default()
    };
    assert_eq!(ids_of(&s, &t, &by_type_and_attr).await, ["urn:x:t1"]);
    let all = antares_store::filter::TemporalFilter::default();
    assert_eq!(ids_of(&s, &t, &all).await.len(), 3);
}

async fn ids_of(
    s: &AnyStore,
    t: &TenantId,
    f: &antares_store::filter::TemporalFilter<'_>,
) -> Vec<String> {
    let mut got: Vec<String> = s
        .query_temporal(t, f)
        .await
        .expect("query")
        .rows
        .iter()
        .filter_map(|d| d["id"].as_str().map(str::to_owned))
        .collect();
    got.sort();
    got
}

/// 5.6.6 deletes the Entity and then its history; an append racing that
/// pair records either before the history delete or not at all, never
/// after it: the Entity check and the extension are one hold of the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_racing_a_delete_never_resurrects_history() {
    let s = std::sync::Arc::new(AnyStore::Mem(Store::default()));
    let t = TenantId::default();
    let shell = json!({"id": "urn:x:race", "type": ["T"]});
    let adds = json!({"https://e.org/a": [{"type": "Property", "value": 1,
                                           "instanceId": "urn:i:race",
                                           "observedAt": "2026-01-01T00:00:00Z"}]});
    for round in 0..500 {
        let id = format!("urn:x:race{round}");
        let mut doc = shell.clone();
        doc["id"] = json!(id);
        assert!(s
            .create(&t, Kind::Entity, &id, doc.clone())
            .await
            .expect("create"));
        let (a, d) = (s.clone(), s.clone());
        let (ta, td) = (t.clone(), t.clone());
        let (ia, idd) = (id.clone(), id.clone());
        let (sh, ad) = (doc.clone(), adds.clone());
        let append = tokio::spawn(async move {
            a.temporal_append(&ta, &ia, &sh, &ad).await.expect("append");
        });
        let delete = tokio::spawn(async move {
            d.delete(&td, Kind::Entity, &idd).await.expect("delete");
            d.delete(&td, Kind::Temporal, &idd)
                .await
                .expect("delete history");
        });
        append.await.expect("append task");
        delete.await.expect("delete task");
        assert!(
            s.get(&t, Kind::Temporal, &id).await.expect("get").is_none(),
            "round {round}: history recreated for a deleted Entity"
        );
    }
}

/// PROVENANCE_KEY: a store that keeps no provenance removes it before it
/// keeps an instance, on create and upsert as on append.
#[tokio::test]
async fn temporal_create_and_upsert_keep_no_provenance() {
    let s = AnyStore::Mem(Store::default());
    let t = TenantId::default();
    let doc = |id: &str| {
        json!({"id": id, "type": ["T"],
               "https://e.org/a": [{"type": "Property", "value": 1, "instanceId": format!("{id}:i"),
                                    "observedAt": "2026-01-01T00:00:00Z",
                                    antares_store::PROVENANCE_KEY: {"by": "x"}}]})
    };
    assert!(s
        .create(&t, Kind::Temporal, "urn:x:p1", doc("urn:x:p1"))
        .await
        .expect("create"));
    s.upsert(&t, Kind::Temporal, "urn:x:p2", doc("urn:x:p2"))
        .await
        .expect("upsert");
    for id in ["urn:x:p1", "urn:x:p2"] {
        let got = s
            .get(&t, Kind::Temporal, id)
            .await
            .expect("get")
            .expect("stored");
        assert!(
            !got.to_string().contains(antares_store::PROVENANCE_KEY),
            "{got}"
        );
    }
}
