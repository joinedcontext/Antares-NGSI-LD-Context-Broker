// SPDX-License-Identifier: EUPL-1.2
//! The outbox event of an entity write carries the same change version the
//! write's change hook gets, and a write that changes nothing enqueues
//! nothing. Skips loudly without ANTARES_TEST_DATABASE_URL.

#![cfg(feature = "postgres")]

use antares_model::TenantId;
use antares_sql::store::pg;
use antares_sql::store::pg::entity::PgEntityStore;
use serde_json::{json, Value};

macro_rules! require_db {
    () => {
        match std::env::var("ANTARES_TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("SKIP: ANTARES_TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

fn run_id() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn doc(id: &str, n: i64) -> Value {
    json!({
        "id": id, "type": ["https://e.org/T"],
        "createdAt": "2026-08-04T09:00:00.000Z", "modifiedAt": "2026-08-04T09:00:00.000Z",
        "https://e.org/n": [{"type": "Property", "value": n}]
    })
}

/// `(op, version, incarnation)` of every outbox event of `id`, oldest first.
async fn events(pool: &sqlx::PgPool, t: &TenantId, id: &str) -> Vec<(String, i64, String)> {
    let mut tx = pool.begin().await.expect("tx");
    pg::set_tenant(&mut tx, t).await.expect("tenant");
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT event FROM outbox WHERE tenant_id = $1 AND event->>'entity_id' = $2 ORDER BY seq",
    )
    .bind(t.as_str())
    .bind(id)
    .fetch_all(&mut *tx)
    .await
    .expect("outbox");
    tx.commit().await.expect("commit");
    rows.iter()
        .map(|e| {
            (
                e["op"].as_str().unwrap_or_default().to_owned(),
                e["version"].as_i64().unwrap_or_default(),
                e["incarnation"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

async fn store(t: &TenantId) -> Option<(sqlx::PgPool, PgEntityStore)> {
    let url = std::env::var("ANTARES_TEST_DATABASE_URL").ok()?;
    let pool = pg::connect(&url, 5).await.expect("connect");
    pg::ensure_tenant(&pool, t).await.expect("tenant row");
    let s = PgEntityStore::new(pool.clone());
    s.set_outbox(true);
    Some((pool, s))
}

/// Create, update and delete of one entity: each event's (incarnation,
/// version) is the hook's, so one incarnation has one spelling and the
/// delete orders after the update it follows.
#[tokio::test(flavor = "multi_thread")]
async fn single_writes_enqueue_the_hooks_change_version() {
    let _ = require_db!();
    let t = TenantId::new("pgoutboxver").expect("tenant");
    let Some((pool, s)) = store(&t).await else {
        return;
    };
    let id = format!("urn:ngsi-ld:T:one{}", run_id());
    let created = s
        .create_stamped(&t, &id, &doc(&id, 1))
        .await
        .expect("create")
        .expect("created");
    let updated = s
        .mutate_stamped(&t, &id, |d| {
            d["https://e.org/n"] = json!([{"type": "Property", "value": 2}]);
            Ok::<(), ()>(())
        })
        .await
        .expect("mutate")
        .expect("present")
        .expect("closure")
        .1;
    let (_, deleted) = s
        .delete_stamped(&t, &id)
        .await
        .expect("delete")
        .expect("deleted");
    let hooks = [created, updated, deleted]
        .map(|v| (v.version, v.incarnation))
        .to_vec();
    let enqueued: Vec<(i64, String)> = events(&pool, &t, &id)
        .await
        .into_iter()
        .map(|(_, v, i)| (v, i))
        .collect();
    assert_eq!(enqueued, hooks);
}

/// The batch paths, held to the same rule.
#[tokio::test(flavor = "multi_thread")]
async fn batch_writes_enqueue_the_hooks_change_version() {
    let _ = require_db!();
    let t = TenantId::new("pgoutboxver").expect("tenant");
    let Some((pool, s)) = store(&t).await else {
        return;
    };
    let id = format!("urn:ngsi-ld:T:batch{}", run_id());
    let created = s
        .batch_create_stamped(&t, &[(id.clone(), doc(&id, 1))])
        .await
        .expect("batch create")
        .remove(0)
        .expect("created");
    let deleted = s
        .batch_delete_stamped(&t, std::slice::from_ref(&id))
        .await
        .expect("batch delete")
        .remove(0)
        .2;
    let enqueued: Vec<(i64, String)> = events(&pool, &t, &id)
        .await
        .into_iter()
        .map(|(_, v, i)| (v, i))
        .collect();
    assert_eq!(
        enqueued,
        [
            (created.version, created.incarnation),
            (deleted.version, deleted.incarnation)
        ]
    );
}

/// A closure that leaves the document as it was writes nothing: no version
/// bump and no update event, as the change hook and the batch path already
/// treat it.
#[tokio::test(flavor = "multi_thread")]
async fn an_unchanged_mutate_writes_and_enqueues_nothing() {
    let _ = require_db!();
    let t = TenantId::new("pgoutboxver").expect("tenant");
    let Some((pool, s)) = store(&t).await else {
        return;
    };
    let id = format!("urn:ngsi-ld:T:noop{}", run_id());
    let created = s
        .create_stamped(&t, &id, &doc(&id, 1))
        .await
        .expect("create")
        .expect("created");
    let (_, v) = s
        .mutate_stamped(&t, &id, |_| Ok::<(), ()>(()))
        .await
        .expect("mutate")
        .expect("present")
        .expect("closure");
    assert_eq!(v, created, "the version an unchanged write reports");
    assert_eq!(s.version(&t, &id).await.expect("version"), Some(1));
    let ops: Vec<String> = events(&pool, &t, &id)
        .await
        .into_iter()
        .map(|(op, _, _)| op)
        .collect();
    assert_eq!(ops, ["create"]);
    s.delete(&t, &id).await.expect("cleanup");
}

/// A single upsert is one write: its hook carries the before-image and
/// the version that write left, created or replaced, recreated after a
/// delete included.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_upsert_hooks_the_write_it_made() {
    use antares_sql::store::any::{AnyStore, PgBackend};
    use antares_store::{ChangeVersion, Kind};
    use std::sync::{Arc, Mutex};
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("connect");
    let t = TenantId::new("pgupsertone").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let store = AnyStore::Pg(PgBackend::new(pool.clone()));
    type Seen = Vec<(Option<Value>, ChangeVersion)>;
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let sink = seen.clone();
    store.set_change_hook(Arc::new(move |_t, before, _after, v| {
        let sink = sink.clone();
        Box::pin(async move {
            sink.lock().expect("hook log").push((before, v));
        })
    }));
    let id = format!("urn:ngsi-ld:T:upsert{}", run_id());
    let (a, b) = (doc(&id, 1), doc(&id, 2));
    assert!(!store
        .upsert(&t, Kind::Entity, &id, a.clone())
        .await
        .expect("create"));
    assert!(store
        .upsert(&t, Kind::Entity, &id, b.clone())
        .await
        .expect("replace"));
    let log: Seen = seen.lock().expect("hook log").clone();
    assert_eq!(log.len(), 2);
    assert_eq!((log[0].0.clone(), log[0].1.version), (None, 1));
    assert_eq!((log[1].0.clone(), log[1].1.version), (Some(a), 2));
    assert_eq!(log[0].1.incarnation, log[1].1.incarnation);
    assert!(store.delete(&t, Kind::Entity, &id).await.expect("delete"));
}

/// A batch upsert enqueues one event per id, create then update, each
/// carrying its item's document and the version the hook gets.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_upsert_enqueues_each_items_event() {
    let _ = require_db!();
    let t = TenantId::new("pgoutboxver").expect("tenant");
    let Some((pool, s)) = store(&t).await else {
        return;
    };
    let run = run_id();
    let ids: Vec<String> = (0..3)
        .map(|i| format!("urn:ngsi-ld:T:up{run}x{i}"))
        .collect();
    let items = |n: i64| -> Vec<(String, Value)> {
        ids.iter().map(|id| (id.clone(), doc(id, n))).collect()
    };
    let first = s
        .batch_upsert_replace_stamped(&t, &items(1))
        .await
        .expect("create");
    let second = s
        .batch_upsert_replace_stamped(&t, &items(2))
        .await
        .expect("replace");
    for (i, id) in ids.iter().enumerate() {
        let want: Vec<(String, i64, String)> = [("create", &first[i]), ("update", &second[i])]
            .iter()
            .map(|(op, (_, _, v))| {
                let v = v.clone().expect("version");
                ((*op).to_owned(), v.version, v.incarnation)
            })
            .collect();
        assert_eq!(events(&pool, &t, id).await, want, "{id}");
        s.delete(&t, id).await.expect("cleanup");
    }
}
