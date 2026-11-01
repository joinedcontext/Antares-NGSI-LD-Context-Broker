// SPDX-License-Identifier: EUPL-1.2
//! PgTemporalStore integration (the temporal bridge). Skips
//! loudly without ANTARES_TEST_DATABASE_URL (see tests/pg.rs recipe).

// Every test here talks to the Postgres store, which only exists with the
// `postgres` feature; without it the file builds to nothing.
#![cfg(feature = "postgres")]

use antares_model::TenantId;
use antares_sql::store::pg;
use antares_sql::store::pg::temporal::PgTemporalStore;
use serde_json::json;

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

/// `occupied_range_…` drops the current week's partition as setup — destroying
/// any sibling's rows parked there (the decomposition doc's modifiedAt
/// fallback lands in the current week whenever "now" crosses into its ISO
/// week) — and conversely a sibling's maintenance pass can recreate the
/// partition it just dropped. Tests that insert into or DDL the current-week
/// partition therefore serialize on this lock.
static PARTITION_DDL: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Maintenance is single-winner by design (`FOR UPDATE SKIP LOCKED`),
/// so a concurrent caller legitimately gets "skipped" — including a sibling
/// test. A real deployment retries on the next tick; so do we, rather than
/// weakening an assertion about what a winning pass must do.
async fn maintenance_winning(
    pool: &sqlx::PgPool,
    retention_days: Option<i64>,
) -> Result<String, sqlx::Error> {
    let backend = antares_sql::store::pg::maintenance::detect_temporal_backend(pool)
        .await
        .expect("temporal backend");
    for _ in 0..50 {
        let msg = antares_sql::store::pg::maintenance::temporal_maintenance(
            pool,
            backend,
            retention_days,
        )
        .await?;
        if !msg.starts_with("skipped") {
            return Ok(msg);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("never won the maintenance claim in 50 tries");
}

/// The current-state row an auto-recorded append hangs off. Written raw: the
/// entity store is a sibling slice, and what the append needs to see is the
/// ROW, not the write path that produced it.
async fn ensure_entity(pool: &sqlx::PgPool, tenant: &TenantId, id: &str) {
    sqlx::query(
        "INSERT INTO entities (tenant_id, id, entity, types, created_at, modified_at)
         VALUES ($1, $2, jsonb_build_object('id', $2::text), ARRAY['T'], now(), now())
         ON CONFLICT (tenant_id, id) DO NOTHING",
    )
    .bind(tenant.as_str())
    .bind(id)
    .execute(pool)
    .await
    .expect("entity row");
}

#[tokio::test(flavor = "multi_thread")]
async fn temporal_doc_roundtrip_and_instance_append() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("connect");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgtemp").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:temp1";
    let _ = s.delete(&t, id).await;

    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let doc = json!({
        "id": id, "type": "Vehicle",
        "createdAt": "2026-08-04T09:00:00Z", "modifiedAt": "2026-08-04T09:00:00Z",
        attr: [{"type": "Property", "value": 80, "observedAt": "2026-08-04T09:00:00Z",
                "instanceId": "urn:ngsi-ld:Instance:1"}]
    });
    assert!(s.create(&t, id, &doc).await.expect("create"));
    assert!(
        !s.create(&t, id, &doc).await.expect("dup"),
        "existing id → false"
    );

    // byte-faithful doc roundtrip (the property the bridge must hold)
    assert_eq!(s.get(&t, id).await.expect("get").expect("present"), doc);

    // append an instance under the row lock — the 5.6.12 shape
    let r = s.mutate(&t, id, |d| {
        d[attr].as_array_mut().expect("instance array").push(
            json!({"type": "Property", "value": 85,
                         "observedAt": "2026-08-04T09:05:00Z",
                         "instanceId": "urn:ngsi-ld:Instance:2"}),
        );
        d["modifiedAt"] = json!("2026-08-04T09:05:00Z");
        Ok::<(), ()>(())
    });
    assert!(matches!(r.await, Ok(Some(Ok(())))));
    let got = s.get(&t, id).await.expect("get").expect("present");
    assert_eq!(got[attr].as_array().expect("arr").len(), 2);

    // cross-tenant invisible; list sees exactly one
    let other = TenantId::new("pgtemp_other").expect("t");
    assert!(s.get(&other, id).await.expect("get").is_none());
    assert_eq!(s.list(&t).await.expect("list").len(), 1);

    assert!(s.delete(&t, id).await.expect("delete"));
    assert!(s.get(&t, id).await.expect("get").is_none());
}

/// The bridge doc decomposes into attr_instances rows in the same tx,
/// and the maintenance pass creates plain-mode partitions under the
/// SKIP LOCKED claim.
#[tokio::test(flavor = "multi_thread")]
async fn attr_instances_decomposition_and_maintenance() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgtempdec").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    // self-clean: an interrupted earlier run must not poison this one
    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempdec'"
        )))
        .execute(&pool)
        .await
        .expect("clean");
    }
    let s = PgTemporalStore::new(pool.clone());

    let doc = serde_json::json!({
        "id": "urn:t:dec1", "type": ["T"],
        "createdAt": "2026-08-04T10:00:00Z", "modifiedAt": "2026-08-04T10:00:00Z",
        "https://ex.org/speed": [
            {"type": "Property", "value": 1, "instanceId": "urn:i:1",
             "observedAt": "2026-08-01T00:00:00Z", "modifiedAt": "2026-08-04T10:00:00Z"},
            {"type": "Property", "value": 2, "instanceId": "urn:i:2",
             "datasetId": "urn:ds:1", "modifiedAt": "2026-08-04T10:00:00Z"}
        ]
    });
    assert!(s.create(&t, "urn:t:dec1", &doc).await.expect("create"));
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attr_instances WHERE tenant_id = 'pgtempdec' AND entity_id = 'urn:t:dec1'",
    )
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(n, 2, "one row per instance");
    // observed_at falls back to modified_at when absent
    let obs: String = sqlx::query_scalar(
        "SELECT observed_at::text FROM attr_instances
         WHERE tenant_id = 'pgtempdec' AND instance_id = 'urn:i:2'",
    )
    .fetch_one(&pool)
    .await
    .expect("obs");
    assert!(
        obs.replace(' ', "T").starts_with("2026-08-04T10:00"),
        "fallback to modifiedAt: {obs}"
    );

    // delete cleans the instances (no FK on a partitioned table)
    assert!(s.delete(&t, "urn:t:dec1").await.expect("delete"));
    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM attr_instances WHERE tenant_id = 'pgtempdec'")
            .fetch_one(&pool)
            .await
            .expect("count2");
    assert_eq!(n, 0);

    // maintenance: plain mode pre-creates weekly partitions, single-winner;
    // timescale mode has chunks instead and reports a no-op without a
    // retention horizon.
    let ts = antares_sql::store::pg::maintenance::timescale_present(&pool)
        .await
        .expect("detect");
    let msg = maintenance_winning(&pool, None).await.expect("maintenance");
    if ts {
        // no partition/chunk work without a retention horizon — but the 4.22
        // expired-instance reap legitimately runs on every pass
        assert!(
            msg.contains("nothing to do") || msg.contains("(4.22)"),
            "timescale, no retention: {msg}"
        );
        assert!(
            !msg.contains("partition") && !msg.contains("drop_chunks"),
            "timescale must not do partition work without retention: {msg}"
        );
    } else {
        assert!(msg.contains("partition"), "plain-mode partitions: {msg}");
        let parts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
             WHERE p.relname = 'attr_instances'",
        )
        .fetch_one(&pool)
        .await
        .expect("parts");
        assert!(parts >= 5, "default + >=4 weekly partitions, got {parts}");
    }
}

/// A range whose rows already sit in the DEFAULT partition cannot be
/// partitioned off, and the module contract is that maintenance SKIPS it and
/// carries on. PostgreSQL aborts the whole transaction on any failed
/// statement, so "carries on" only holds if that one statement runs under its
/// own savepoint — without it the pass dies with 25P02 on the NEXT statement
/// and no partition is ever created on a live database.
#[tokio::test(flavor = "multi_thread")]
async fn occupied_range_is_skipped_without_poisoning_the_pass() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("connect");
    if antares_sql::store::pg::maintenance::timescale_present(&pool)
        .await
        .expect("detect")
    {
        return; // plain-mode-only contract; timescale has chunks
    }
    let t = TenantId::new("pgpart").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");

    // Drop any weekly partition covering now, then park a row in DEFAULT for
    // that exact range — the conflict maintenance must tolerate.
    let this_week: String = sqlx::query_scalar(
        "SELECT 'attr_instances_' || to_char(date_trunc('week', now()), 'IYYY\"w\"IW')",
    )
    .fetch_one(&pool)
    .await
    .expect("suffix");
    // The DROP waits for ACCESS EXCLUSIVE and queues every reader of
    // attr_instances behind it: a second at a time, as maintenance does, so
    // a parallel test's read neither fails it nor waits on it for long.
    let drop_week = format!("DROP TABLE IF EXISTS {this_week}");
    for attempt in 1.. {
        let mut tx = pool.begin().await.expect("tx");
        sqlx::query("SET LOCAL lock_timeout = '1s'")
            .execute(&mut *tx)
            .await
            .expect("lock_timeout");
        match sqlx::query(sqlx::AssertSqlSafe(drop_week.clone()))
            .execute(&mut *tx)
            .await
        {
            Ok(_) => {
                tx.commit().await.expect("drop");
                break;
            }
            Err(sqlx::Error::Database(e))
                if e.code().as_deref() == Some("55P03") && attempt < 60 =>
            {
                drop(tx);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("drop: {e}"),
        }
    }
    sqlx::query(
        "INSERT INTO attr_instances
           (tenant_id, entity_id, attr_id, instance_id, observed_at, created_at, modified_at, data)
         VALUES ($1, 'urn:e:part', 'urn:a:part', 'urn:i:part', now(), now(), now(), '{}'::jsonb)
         ON CONFLICT DO NOTHING",
    )
    .bind(t.as_str())
    .execute(&pool)
    .await
    .expect("park row in default");

    let msg = maintenance_winning(&pool, None)
        .await
        .expect("maintenance must survive an occupied range");
    assert!(
        msg.contains("adopted from default"),
        "an occupied range must be recovered, not abandoned: {msg}"
    );
    assert!(
        msg.contains(": ok"),
        "the pass continued past the occupied range: {msg}"
    );

    sqlx::query("DELETE FROM attr_instances WHERE tenant_id = $1")
        .bind(t.as_str())
        .execute(&pool)
        .await
        .expect("cleanup");
}

/// 4.22: expiresAt marks the point where an instance "should be deleted from
/// an NGSI-LD system" — the maintenance pass physically reaps attr_instances
/// rows whose instance-level expiresAt has passed. Durable siblings stay.
/// (Reads already refuse expired instances; this bounds the storage.)
#[tokio::test(flavor = "multi_thread")]
async fn clause_4_22_expired_attr_instances_reaped() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgtempexp").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempexp'"
        )))
        .execute(&pool)
        .await
        .expect("clean");
    }
    let s = PgTemporalStore::new(pool.clone());
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let doc = json!({
        "id": "urn:t:exp422", "type": ["T"],
        "createdAt": "2026-08-04T09:00:00Z", "modifiedAt": "2026-08-04T09:00:00Z",
        attr: [
            {"type": "Property", "value": 1, "observedAt": "2026-08-04T09:00:00Z",
             "instanceId": "urn:ngsi-ld:Instance:reapme",
             "expiresAt": "2020-01-01T00:00:00Z"},
            {"type": "Property", "value": 2, "observedAt": "2026-08-04T09:01:00Z",
             "instanceId": "urn:ngsi-ld:Instance:durable"}
        ]
    });
    assert!(s.create(&t, "urn:t:exp422", &doc).await.expect("create"));
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM attr_instances WHERE tenant_id = 'pgtempexp'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(before, 2, "both instances decomposed");

    maintenance_winning(&pool, None).await.expect("maintenance");

    let left: Vec<String> =
        sqlx::query_scalar("SELECT instance_id FROM attr_instances WHERE tenant_id = 'pgtempexp'")
            .fetch_all(&pool)
            .await
            .expect("rows");
    assert_eq!(
        left,
        vec!["urn:ngsi-ld:Instance:durable".to_string()],
        "expired instance reaped, durable sibling kept (4.22)"
    );

    sqlx::query("DELETE FROM attr_instances WHERE tenant_id = 'pgtempexp'")
        .execute(&pool)
        .await
        .expect("cleanup");
    let _ = s.delete(&t, "urn:t:exp422").await;
}

/// 4.22 + 5.6.16.4: an expired `Temporal Evolution of an Entity` is not "an
/// existing Entity whose id (URI) is equivalent held locally", so the write
/// and delete paths have to see the absence the read paths already report.
/// `get_range` and `query` carry the shared `NOT_EXPIRED` predicate — whose
/// own comment says an expired temporal entity "is absent from every read" —
/// while `delete` (5.6.16) and `mutate` (5.6.12 to 5.6.15) select the meta
/// row without it. That is a client-visible contradiction: a GET answers 404
/// for the id a DELETE answers 204 for.
#[tokio::test(flavor = "multi_thread")]
async fn clause_4_22_an_expired_temporal_entity_is_absent_from_writes_too() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgtempexp2").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempexp2'"
        )))
        .execute(&pool)
        .await
        .expect("clean");
    }
    let s = PgTemporalStore::new(pool.clone());
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let id = "urn:t:exp422w";
    let doc = json!({
        "id": id, "type": ["T"],
        "createdAt": "2026-08-04T09:00:00Z", "modifiedAt": "2026-08-04T09:00:00Z",
        "expiresAt": "2020-01-01T00:00:00Z",
        attr: [{"type": "Property", "value": 1, "observedAt": "2026-08-04T09:00:00Z",
                "instanceId": "urn:ngsi-ld:Instance:exp422w"}]
    });
    assert!(s.create(&t, id, &doc).await.expect("create"));

    // The premise: the row IS stored, and the read side already refuses it.
    // Without this every later assertion could be about a row that was never
    // written rather than about expiry.
    let stored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM temporal_entities WHERE tenant_id = 'pgtempexp2'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(stored, 1, "the expired entity is stored, not rejected");
    assert!(
        s.get(&t, id).await.expect("get").is_none(),
        "4.22: an expired temporal entity is absent from a read"
    );
    assert!(
        !s.list(&t)
            .await
            .expect("list")
            .iter()
            .any(|d| d["id"] == id),
        "4.22: an expired temporal entity is absent from a listing"
    );

    // 5.6.12 to 5.6.15 all reach the history through `mutate`. An entity the
    // read side calls absent may not be modified in place.
    let mutated = s
        .mutate::<(), ()>(&t, id, |_doc| Ok(()))
        .await
        .expect("mutate");
    assert!(
        mutated.is_none(),
        "4.22: an expired temporal entity cannot be modified — the read side calls it absent"
    );

    // 5.6.16.4: "no existing Entity whose id (URI) is equivalent held
    // locally" is ResourceNotFound, which this seam reports as `false`.
    assert!(
        !s.delete(&t, id).await.expect("delete"),
        "5.6.16.4: deleting an expired temporal entity is ResourceNotFound, not 204"
    );

    // The refused delete leaves the history intact — an expired entity keeps
    // it until the 4.22 reap or a create replaces it. Destroying it behind a
    // 404 would be the one outcome worse than either.
    let kept: i64 =
        sqlx::query_scalar("SELECT count(*) FROM attr_instances WHERE tenant_id = 'pgtempexp2'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(
        kept, 1,
        "a refused delete may not wipe the history it refused"
    );

    // The other side of the same reading: absent means a create succeeds, and
    // takes the expired entity's history with it rather than letting the new
    // entity inherit it. `AnyStore::upsert` is `create` else `mutate`, so this
    // is also what keeps an upsert over an expired entity from writing
    // nothing and reporting that it replaced something.
    let fresh = json!({
        "id": id, "type": ["T"],
        "createdAt": "2026-08-05T09:00:00Z", "modifiedAt": "2026-08-05T09:00:00Z",
        attr: [{"type": "Property", "value": 9, "observedAt": "2026-08-05T09:00:00Z",
                "instanceId": "urn:ngsi-ld:Instance:fresh"}]
    });
    assert!(
        s.create(&t, id, &fresh).await.expect("create over expired"),
        "4.22: an expired temporal entity is absent, so this is a creation"
    );
    let doc = s
        .get(&t, id)
        .await
        .expect("get")
        .expect("the fresh entity reads");
    let insts = doc[attr].as_array().expect("instances");
    assert_eq!(
        insts.len(),
        1,
        "the replaced entity does not inherit the expired one's history: {doc}"
    );
    assert_eq!(insts[0]["value"], 9, "{doc}");

    // And now that it is not expired, the write paths reach it again.
    assert!(
        s.mutate::<(), ()>(&t, id, |_doc| Ok(()))
            .await
            .expect("mutate")
            .is_some(),
        "a live temporal entity is still modifiable"
    );
    assert!(
        s.delete(&t, id).await.expect("delete"),
        "5.6.16: a live temporal evolution is deleted, 204"
    );

    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempexp2'"
        )))
        .execute(&pool)
        .await
        .expect("cleanup");
    }
}

/// 4.6.3: "In requests, also a comma instead of a decimal point may be used
/// as separator for compatibility reasons." On the temporal side that form is
/// the dangerous one. The instance stamps go straight into `::timestamptz`
/// casts, and the raise lands in the temporal drain, which absorbs it — the
/// entity write has already committed and answered 2xx, and because a whole
/// request folds into one append per entity, one attribute with a comma
/// stamp took the history of every attribute in that request with it.
///
/// The `expiresAt` half is the mirror image: the temporal meta keeps the
/// stamp as jsonb TEXT and reads it back through `try_timestamptz`, which
/// answers NULL for anything it cannot parse — and NULL means "no expiry", so
/// a comma-stamped expiry made the entity immortal instead of loud.
#[tokio::test(flavor = "multi_thread")]
async fn clause_4_6_3_a_comma_seconds_fraction_records_and_expires() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgtempcomma").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempcomma'"
        )))
        .execute(&pool)
        .await
        .expect("clean");
    }
    let s = PgTemporalStore::new(pool.clone());
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";

    let id = "urn:t:comma";
    let doc = json!({
        "id": id, "type": ["T"],
        "createdAt": "2026-08-04T09:00:00,125Z", "modifiedAt": "2026-08-04T09:00:00,125Z",
        attr: [{"type": "Property", "value": 1,
                "observedAt": "2026-08-04T09:00:00,125Z",
                "instanceId": "urn:ngsi-ld:Instance:comma"}]
    });
    assert!(s
        .create(&t, id, &doc)
        .await
        .expect("4.6.3: the comma form is legal"));

    // The instance landed, and its column holds the instant the client meant.
    let observed: Vec<bool> = sqlx::query_scalar(
        "SELECT observed_at = $1::timestamptz FROM attr_instances
          WHERE tenant_id = 'pgtempcomma'",
    )
    .bind("2026-08-04T09:00:00.125Z")
    .fetch_all(&pool)
    .await
    .expect("rows");
    assert_eq!(observed.len(), 1, "the history was not silently dropped");
    assert!(
        observed[0],
        "the comma and point forms are the same instant"
    );
    assert!(
        s.get(&t, id).await.expect("get").is_some(),
        "and the evolution reads back"
    );

    // 4.22 through a comma stamp: an expiry in the past is an expiry, not a
    // stamp the store quietly reads as absent.
    let gone = "urn:t:commagone";
    let expired = json!({
        "id": gone, "type": ["T"],
        "createdAt": "2020-01-01T00:00:00,750Z", "modifiedAt": "2020-01-01T00:00:00,750Z",
        "expiresAt": "2020-01-01T00:00:00,750Z",
        attr: [{"type": "Property", "value": 2,
                "observedAt": "2020-01-01T00:00:00,750Z",
                "instanceId": "urn:ngsi-ld:Instance:commagone"}]
    });
    assert!(s.create(&t, gone, &expired).await.expect("create expired"));
    assert!(
        s.get(&t, gone).await.expect("get").is_none(),
        "4.22 + 4.6.3: a comma-stamped expiry in the past is still an expiry"
    );

    for table in ["attr_instances", "temporal_entities"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE tenant_id = 'pgtempcomma'"
        )))
        .execute(&pool)
        .await
        .expect("cleanup");
    }
}

/// Regression: the append fast-path's shell insert was ON CONFLICT DO
/// NOTHING — an entity that gained a type after first touch stayed frozen
/// with its original types and was invisible to type-filtered temporal
/// queries forever. The shell upsert must carry new types/scopes forward.
#[tokio::test(flavor = "multi_thread")]
async fn append_refreshes_types_after_first_touch() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgtempretype").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:retype";
    let _ = s.delete(&t, id).await;
    ensure_entity(&pool, &t, id).await;
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let shell1 = json!({"id": id, "type": "Vehicle",
        "createdAt": "2026-08-15T09:00:00Z", "modifiedAt": "2026-08-15T09:00:00Z"});
    let adds = json!({attr: [{"type": "Property", "value": 1,
        "observedAt": "2026-08-15T09:00:00Z",
        "instanceId": "urn:ngsi-ld:Instance:rt1"}]});
    s.append(&t, id, &shell1, &adds).await.expect("append 1");

    let shell2 = json!({"id": id, "type": ["Vehicle", "Camera"],
        "createdAt": "2026-08-15T09:00:00Z", "modifiedAt": "2026-08-15T09:01:00Z"});
    let adds2 = json!({attr: [{"type": "Property", "value": 2,
        "observedAt": "2026-08-15T09:01:00Z",
        "instanceId": "urn:ngsi-ld:Instance:rt2"}]});
    s.append(&t, id, &shell2, &adds2).await.expect("append 2");

    let got = s.get(&t, id).await.expect("get").expect("present");
    assert!(
        got["type"].to_string().contains("Camera"),
        "a type gained after first touch must reach the temporal evolution: {}",
        got["type"]
    );
    let _ = s.delete(&t, id).await;
    sqlx::query("DELETE FROM entities WHERE tenant_id = $1")
        .bind(t.as_str())
        .execute(&pool)
        .await
        .expect("cleanup");
}

/// 4.5.6: a scope change is recorded as a scope instance in the
/// evolution's `meta`. The next append's shell carries the CURRENT scope (a
/// string array), and refreshing `meta` from it erased every recorded scope
/// instance: the history kept only the scope as it now is.
#[tokio::test(flavor = "multi_thread")]
async fn an_append_keeps_the_recorded_scope_history() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgscopehist").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:scopehist";
    let _ = s.delete(&t, id).await;
    ensure_entity(&pool, &t, id).await;
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let shell = json!({"id": id, "type": "Vehicle", "scope": ["/a"],
        "createdAt": "2026-08-15T09:00:00Z", "modifiedAt": "2026-08-15T09:00:00Z"});
    let adds = |n: u32| {
        json!({attr: [{"type": "Property", "value": n,
        "observedAt": format!("2026-08-15T09:0{n}:00Z"),
        "instanceId": format!("urn:ngsi-ld:Instance:sh{n}")}]})
    };
    s.append(&t, id, &shell, &adds(1)).await.expect("append 1");
    // what push_scope records for a 4.5.6 scope change
    let scope = json!({"type": "Property", "value": ["/a"],
        "observedAt": "2026-08-15T09:00:30Z", "instanceId": "urn:ngsi-ld:Instance:sc1"});
    s.mutate(&t, id, |doc| {
        doc["scope"] = json!([scope.clone()]);
        Ok::<(), ()>(())
    })
    .await
    .expect("mutate")
    .expect("present")
    .expect("scope recorded");
    s.append(&t, id, &shell, &adds(2)).await.expect("append 2");
    let got = s.get(&t, id).await.expect("get").expect("present");
    let _ = s.delete(&t, id).await;
    sqlx::query("DELETE FROM entities WHERE tenant_id = $1")
        .bind(t.as_str())
        .execute(&pool)
        .await
        .expect("cleanup");
    assert_eq!(
        got["scope"],
        json!([scope]),
        "the scope history was overwritten"
    );
    assert_eq!(got[attr].as_array().map(Vec::len), Some(2), "{got}");
}

/// 5.6.6 Delete Entity removes the entity, and with it the temporal evolution
/// recorded for it (5.6.11 auto-recording). Those are two transactions, so an
/// auto-recording append that started before the delete can commit after it —
/// and the history it writes then belongs to an entity that no longer exists,
/// with no later delete to clean it. The append is conditional on the entity
/// row for exactly that reason, and it takes a KEY SHARE lock on it so a
/// delete cannot slip between the check and the insert.
#[tokio::test(flavor = "multi_thread")]
async fn append_for_a_deleted_entity_does_not_resurrect_its_history() {
    let url = require_db!();
    // the appended instances are stamped "now", so they land in the current
    // week's partition — the one the maintenance tests drop and re-create
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgtempghost").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:ghost";
    let _ = s.delete(&t, id).await;
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let shell = json!({"id": id, "type": "Vehicle",
        "createdAt": "2026-08-17T09:00:00Z", "modifiedAt": "2026-08-17T09:00:00Z"});
    let adds = |n: i64, inst: &str| {
        json!({attr: [{"type": "Property", "value": n,
                       "observedAt": "2026-08-17T09:00:00Z",
                       "instanceId": inst}]})
    };
    let instances = |pool: sqlx::PgPool| async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM attr_instances
             WHERE tenant_id = 'pgtempghost' AND entity_id = 'urn:ngsi-ld:Vehicle:ghost'",
        )
        .fetch_one(&pool)
        .await
        .expect("count")
    };

    // control: with the entity present the append records normally
    ensure_entity(&pool, &t, id).await;
    s.append(&t, id, &shell, &adds(1, "urn:ngsi-ld:Instance:live"))
        .await
        .expect("append while the entity lives");
    assert!(
        s.get(&t, id).await.expect("get").is_some(),
        "history recorded"
    );
    assert_eq!(instances(pool.clone()).await, 1);

    // the delete, as the API does it: the entity row, then its evolution
    sqlx::query("DELETE FROM entities WHERE tenant_id = $1 AND id = $2")
        .bind(t.as_str())
        .bind(id)
        .execute(&pool)
        .await
        .expect("delete entity");
    assert!(s.delete(&t, id).await.expect("delete temporal"));

    // the late hook: an append for an entity that is gone records NOTHING
    s.append(&t, id, &shell, &adds(2, "urn:ngsi-ld:Instance:ghost"))
        .await
        .expect("a late append is a no-op, not an error");
    assert!(
        s.get(&t, id).await.expect("get").is_none(),
        "no temporal evolution may exist for a deleted entity"
    );
    assert_eq!(
        instances(pool.clone()).await,
        0,
        "and no instance rows either"
    );
}

/// 4.5.19 / 5.7.4.4 aggregation pushed into SQL: the numeric bucket matrix
/// the API would compute over reconstructed instances comes back from the
/// store already aggregated; a non-numeric windowed value makes the store
/// fall back to instance rows so the API keeps its eligibility verdict.
#[tokio::test(flavor = "multi_thread")]
async fn aggregation_pushdown_buckets_like_the_api() {
    use antares_sql::compile::temporal::InstanceRange;
    use antares_sql::store::filter::{Aggregate, TemporalFilter};
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("connect");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgaggr").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:aggr1";
    let _ = s.delete(&t, id).await;
    let speed = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let inst = |v: serde_json::Value, at: &str, n: u32| {
        json!({"type": "Property", "value": v, "observedAt": at,
               "instanceId": format!("urn:ngsi-ld:Instance:a{n}")})
    };
    let doc = json!({
        "id": id, "type": "Vehicle",
        "createdAt": "2026-01-01T00:00:00Z", "modifiedAt": "2026-01-01T00:00:00Z",
        speed: [inst(json!(10), "2026-01-01T00:00:00Z", 1),
                inst(json!(20), "2026-01-01T00:30:00Z", 2),
                inst(json!(30), "2026-01-01T01:15:00Z", 3),
                inst(json!(99), "2025-12-31T23:00:00Z", 4)]
    });
    assert!(s.create(&t, id, &doc).await.expect("create"));
    let methods = ["avg".to_owned(), "max".to_owned(), "totalCount".to_owned()];
    let expand = |t: &str| t.to_owned();
    let range = InstanceRange {
        timerel: "after",
        time_at: "2026-01-01T00:00:00Z",
        end_time_at: None,
        timeproperty: "observedAt",
    };
    let f = TemporalFilter {
        ids: Some(&[id]),
        range: Some(range),
        expand: &expand,
        aggregate: Some(Aggregate {
            methods: &methods,
            period_secs: Some(3600),
            anchor: Some("2026-01-01T00:00:00Z"),
        }),
        ..TemporalFilter::default()
    };
    let out = s.query(&t, &f).await.expect("query");
    assert!(out.aggregated, "the store aggregated");
    assert_eq!(out.rows.len(), 1);
    let a = &out.rows[0][speed];
    assert_eq!(a["type"], "Property");
    // [value, start, end] rows; the value compares as a number (SQL spells
    // 15 where the API spells 15.0 — the same JSON number)
    let rows = |v: &serde_json::Value| -> Vec<(f64, String, String)> {
        v.as_array()
            .expect("rows")
            .iter()
            .map(|r| {
                (
                    r[0].as_f64().expect("number"),
                    r[1].as_str().expect("start").to_owned(),
                    r[2].as_str().expect("end").to_owned(),
                )
            })
            .collect()
    };
    let b0 = (
        "2026-01-01T00:00:00Z".to_owned(),
        "2026-01-01T01:00:00Z".to_owned(),
    );
    let b1 = (
        "2026-01-01T01:00:00Z".to_owned(),
        "2026-01-01T02:00:00Z".to_owned(),
    );
    // hour buckets anchored at timeAt; the 23:00 instance is outside the window
    assert_eq!(
        rows(&a["avg"]),
        vec![
            (15.0, b0.0.clone(), b0.1.clone()),
            (30.0, b1.0.clone(), b1.1.clone())
        ]
    );
    assert_eq!(rows(&a["max"])[0].0, 20.0);
    assert_eq!(
        rows(&a["totalCount"]),
        vec![(2.0, b0.0, b0.1), (1.0, b1.0, b1.1)]
    );
    assert!(
        a.get("value").is_none(),
        "no instance members leak into an aggregate: {a}"
    );

    // PT0S: one bucket from the first instant to the last + 1 s, no anchor
    let f0 = TemporalFilter {
        ids: Some(&[id]),
        expand: &expand,
        aggregate: Some(Aggregate {
            methods: &methods,
            period_secs: None,
            anchor: None,
        }),
        ..TemporalFilter::default()
    };
    let out = s.query(&t, &f0).await.expect("query");
    assert!(out.aggregated);
    assert_eq!(
        rows(&out.rows[0][speed]["totalCount"]),
        vec![(
            4.0,
            "2025-12-31T23:00:00Z".to_owned(),
            "2026-01-01T01:15:01Z".to_owned()
        )]
    );

    // 4.5.19.1: with a temporal query the PT0S period spans THAT range, not
    // the instants that happen to fall in it. `before` names only the end.
    let before = InstanceRange {
        timerel: "before",
        time_at: "2030-01-01T00:00:00Z",
        end_time_at: None,
        timeproperty: "observedAt",
    };
    let fb = TemporalFilter {
        ids: Some(&[id]),
        range: Some(before),
        expand: &expand,
        aggregate: Some(Aggregate {
            methods: &methods,
            period_secs: None,
            anchor: Some("2030-01-01T00:00:00Z"),
        }),
        ..TemporalFilter::default()
    };
    let out = s.query(&t, &fb).await.expect("query");
    assert!(out.aggregated);
    assert_eq!(
        rows(&out.rows[0][speed]["totalCount"]),
        vec![(
            4.0,
            "2025-12-31T23:00:00Z".to_owned(),
            "2030-01-01T00:00:00Z".to_owned()
        )],
        "a before query's whole period ends at timeAt and starts at the data"
    );
    // `between` names both edges, so neither comes from the data
    let between = InstanceRange {
        timerel: "between",
        time_at: "2025-12-01T00:00:00Z",
        end_time_at: Some("2026-02-01T00:00:00Z"),
        timeproperty: "observedAt",
    };
    let fbt = TemporalFilter {
        ids: Some(&[id]),
        range: Some(between),
        expand: &expand,
        aggregate: Some(Aggregate {
            methods: &methods,
            period_secs: None,
            anchor: Some("2025-12-01T00:00:00Z"),
        }),
        ..TemporalFilter::default()
    };
    let out = s.query(&t, &fbt).await.expect("query");
    assert_eq!(
        rows(&out.rows[0][speed]["totalCount"]),
        vec![(
            4.0,
            "2025-12-01T00:00:00Z".to_owned(),
            "2026-02-01T00:00:00Z".to_owned()
        )]
    );

    // a string-valued windowed instance: not the store's class to judge —
    // plain instance rows come back and the API decides eligibility
    let r = s.mutate(&t, id, |d| {
        d[speed].as_array_mut().expect("arr").push(
            json!({"type": "Property", "value": "fast", "observedAt": "2026-01-01T00:45:00Z",
                   "instanceId": "urn:ngsi-ld:Instance:a5"}),
        );
        Ok::<(), ()>(())
    });
    assert!(matches!(r.await, Ok(Some(Ok(())))));
    let out = s.query(&t, &f).await.expect("query");
    assert!(!out.aggregated, "mixed classes fall back to instances");
    assert!(out.rows[0][speed].is_array(), "{}", out.rows[0]);
    assert!(s.delete(&t, id).await.expect("delete"));
}

/// 4.14 through the temporal reads that build SQL of their own. `get`,
/// `list` and `delete` are keyed lookups whose isolation is easy to see;
/// `query` is not. It compiles a window predicate, an optional geo
/// prefilter and an optional aggregate into statements over
/// `attr_instances`, and a tenant predicate dropped from any one of them
/// answers with another tenant's history — as instances under the asking
/// tenant's entity id, or, worse, folded into an average that names no
/// source at all. Two tenants hold the SAME entity id here, because an id
/// that differs would let a filter narrow the leak away by accident.
#[tokio::test(flavor = "multi_thread")]
async fn a_temporal_query_never_reads_another_tenants_history() {
    use antares_sql::compile::temporal::InstanceRange;
    use antares_sql::store::filter::{Aggregate, GeoSpec, Rel, TemporalFilter};
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("connect");
    let s = PgTemporalStore::new(pool.clone());
    let a = TenantId::new("pgtempiso_a").expect("tenant");
    let b = TenantId::new("pgtempiso_b").expect("tenant");
    for t in [&a, &b] {
        pg::ensure_tenant(&pool, t).await.expect("tenant row");
    }
    // Two ids, both held by BOTH tenants. The numeric one is kept clear of
    // the GeoProperty because one non-numeric windowed value is enough to
    // make the store decline the aggregate, and this test is about what the
    // aggregate reads, not about when it runs.
    let id = "urn:ngsi-ld:Vehicle:shared";
    let geo_id = "urn:ngsi-ld:Vehicle:sharedgeo";
    let speed = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let location = antares_sql::store::filter::LOCATION_IRI;
    let doc = |tag: &str, v1: i64, v2: i64| {
        json!({
            "id": id, "type": "Vehicle",
            "createdAt": "2026-01-01T00:00:00Z", "modifiedAt": "2026-01-01T00:00:00Z",
            speed: [
                {"type": "Property", "value": v1, "observedAt": "2026-01-01T00:10:00Z",
                 "instanceId": format!("urn:ngsi-ld:Instance:{tag}1")},
                {"type": "Property", "value": v2, "observedAt": "2026-01-01T00:20:00Z",
                 "instanceId": format!("urn:ngsi-ld:Instance:{tag}2")}
            ],
        })
    };
    let geo_doc = |tag: &str| {
        json!({
            "id": geo_id, "type": "Vehicle",
            "createdAt": "2026-01-01T00:00:00Z", "modifiedAt": "2026-01-01T00:00:00Z",
            location: [{
                "type": "GeoProperty",
                "value": {"type": "Point", "coordinates": [0.0, 0.0]},
                "observedAt": "2026-01-01T00:30:00Z",
                "instanceId": format!("urn:ngsi-ld:Instance:{tag}geo"),
            }],
        })
    };
    for (t, tag, v1, v2) in [(&a, "isoa", 10, 20), (&b, "isob", 1000, 2000)] {
        let _ = s.delete(t, id).await;
        let _ = s.delete(t, geo_id).await;
        assert!(
            s.create(t, id, &doc(tag, v1, v2)).await.expect("create"),
            "{tag}"
        );
        assert!(
            s.create(t, geo_id, &geo_doc(tag))
                .await
                .expect("create geo"),
            "{tag}"
        );
    }

    let expand = |t: &str| t.to_owned();
    let window = || InstanceRange {
        timerel: "after",
        time_at: "2026-01-01T00:00:00Z",
        end_time_at: None,
        timeproperty: "observedAt",
    };
    let instance_ids = |rows: &[serde_json::Value], attr: &str| -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .flat_map(|r| r[attr].as_array().cloned().unwrap_or_default())
            .filter_map(|i| i["instanceId"].as_str().map(str::to_owned))
            .collect();
        v.sort();
        v
    };

    // The windowed instance read: only this tenant's instances of the id.
    let plain = TemporalFilter {
        ids: Some(&[id]),
        range: Some(window()),
        expand: &expand,
        ..TemporalFilter::default()
    };
    let out = s.query(&a, &plain).await.expect("query a");
    assert_eq!(
        instance_ids(&out.rows, speed),
        [
            "urn:ngsi-ld:Instance:isoa1".to_owned(),
            "urn:ngsi-ld:Instance:isoa2".to_owned()
        ],
        "the window read another tenant's instances: {:?}",
        out.rows
    );

    // The aggregate, where a leak carries no id to give it away: the average
    // of this tenant's two values and nothing else.
    let methods = ["avg".to_owned(), "totalCount".to_owned()];
    let aggregated = TemporalFilter {
        ids: Some(&[id]),
        range: Some(window()),
        expand: &expand,
        aggregate: Some(Aggregate {
            methods: &methods,
            period_secs: Some(3600),
            anchor: Some("2026-01-01T00:00:00Z"),
        }),
        ..TemporalFilter::default()
    };
    let out = s.query(&a, &aggregated).await.expect("aggregate a");
    assert!(out.aggregated, "the store aggregated");
    let agg = &out.rows[0][speed];
    assert_eq!(
        agg["totalCount"][0][0].as_f64(),
        Some(2.0),
        "the bucket counted another tenant's instances: {agg}"
    );
    assert_eq!(
        agg["avg"][0][0].as_f64(),
        Some(15.0),
        "the average was taken over another tenant's values: {agg}"
    );

    // The geo prefilter, which selects instances through a second statement.
    let coordinates = json!([0.0, 0.0]);
    let spec = GeoSpec {
        rel: Rel::Near {
            max: Some(1000.0),
            min: None,
        },
        geometry: "Point",
        coordinates: &coordinates,
        geoproperty_iri: location,
    };
    let geo = TemporalFilter {
        ids: Some(&[geo_id]),
        range: Some(window()),
        timeproperty: "observedAt",
        geo: &[(spec.clone(), location.to_owned())],
        expand: &expand,
        ..TemporalFilter::default()
    };
    let out = s.query(&a, &geo).await.expect("geo a");
    assert_eq!(
        instance_ids(&out.rows, location),
        ["urn:ngsi-ld:Instance:isoageo".to_owned()],
        "the geo prefilter reached another tenant: {:?}",
        out.rows
    );

    // And the mirror image: b sees b's, so the reads are narrowed by tenant
    // rather than by whatever happened to be written first.
    let out = s.query(&b, &plain).await.expect("query b");
    assert_eq!(
        instance_ids(&out.rows, speed),
        [
            "urn:ngsi-ld:Instance:isob1".to_owned(),
            "urn:ngsi-ld:Instance:isob2".to_owned()
        ]
    );

    // A tenant that holds no history at all answers with none of it.
    let empty = TenantId::new("pgtempiso_c").expect("tenant");
    pg::ensure_tenant(&pool, &empty).await.expect("tenant row");
    assert!(
        s.query(&empty, &plain)
            .await
            .expect("query c")
            .rows
            .is_empty(),
        "a tenant with no temporal entity of that id reads nothing"
    );

    for t in [&a, &b] {
        assert!(s.delete(t, id).await.expect("delete"));
        assert!(s.delete(t, geo_id).await.expect("delete geo"));
    }
}

/// 4.6.3: "In requests, also a comma instead of a decimal point may be used
/// as separator" of the seconds fraction. PostgreSQL accepts no such stamp,
/// so every bound that reaches a `::timestamptz` cast goes through
/// `canonical_datetime` first — the whole reason
/// `migrations/0003_comma_seconds_fraction.sql` exists. The 5.7.4.4 S3
/// geo prefilter built its window bound straight from the request instead,
/// so a legal temporal geoquery raised SQLSTATE 22007 and the API answered
/// 500 to a request the spec spells out.
#[tokio::test(flavor = "multi_thread")]
async fn a_comma_seconds_fraction_survives_the_geo_prefilter() {
    use antares_sql::compile::temporal::InstanceRange;
    use antares_sql::store::filter::{GeoSpec, Rel, TemporalFilter};
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("connect");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgcommageo").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let id = "urn:ngsi-ld:Vehicle:commageo";
    let _ = s.delete(&t, id).await;
    let location = antares_sql::store::filter::LOCATION_IRI;
    let doc = json!({
        "id": id, "type": "Vehicle",
        "createdAt": "2026-01-01T00:00:00Z", "modifiedAt": "2026-01-01T00:00:00Z",
        location: [{
            "type": "GeoProperty",
            "value": {"type": "Point", "coordinates": [0.0, 0.0]},
            "observedAt": "2026-01-01T01:00:00Z",
            "instanceId": "urn:ngsi-ld:Instance:commageo1",
        }],
    });
    assert!(s.create(&t, id, &doc).await.expect("create"));
    let expand = |t: &str| t.to_owned();
    let coordinates = json!([0.0, 0.0]);
    let spec = GeoSpec {
        rel: Rel::Near {
            max: Some(1000.0),
            min: None,
        },
        geometry: "Point",
        coordinates: &coordinates,
        geoproperty_iri: location,
    };
    for time_at in [
        "2026-01-01T00:00:00.500Z", // the point form Postgres takes as it is
        "2026-01-01T00:00:00,500Z", // the comma form 4.6.3 also allows
    ] {
        let f = TemporalFilter {
            ids: Some(&[id]),
            range: Some(InstanceRange {
                timerel: "after",
                time_at,
                end_time_at: None,
                timeproperty: "observedAt",
            }),
            timeproperty: "observedAt",
            geo: &[(spec.clone(), location.to_owned())],
            expand: &expand,
            ..TemporalFilter::default()
        };
        let out = s
            .query(&t, &f)
            .await
            .unwrap_or_else(|e| panic!("timeAt {time_at} was refused by the store: {e}"));
        assert_eq!(
            out.rows.len(),
            1,
            "timeAt {time_at}: the instance is in the window"
        );
    }
}

/// the probes read only the weeks that can hold the instance, and
/// record what they recorded before. A value written this week is compared
/// with this week's newest, never with an older equal one behind it; one
/// append whose observedAts lie five weeks apart corrects both in place.
#[tokio::test(flavor = "multi_thread")]
async fn bounded_probes_record_what_the_unbounded_ones_did() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgtempbounded").expect("tenant");
    let id = "urn:ngsi-ld:Vehicle:bounded";
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let _ = s.delete(&t, id).await;
    ensure_entity(&pool, &t, id).await;
    let speed = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let name = "https://uri.etsi.org/ngsi-ld/default-context/name";
    // minutes ago; 50400 is five weeks
    let at = |minutes: i64| {
        (chrono::Utc::now() - chrono::Duration::minutes(minutes))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    };
    let shell =
        json!({"id": id, "type": "Vehicle", "createdAt": at(60000), "modifiedAt": at(60000)});
    let mut n = 0;
    let mut write = |attrs: serde_json::Value, modified: String| {
        n += 1;
        let mut attrs = attrs;
        for a in attrs.as_object_mut().expect("attrs").values_mut() {
            for (j, i) in a.as_array_mut().expect("instances").iter_mut().enumerate() {
                i["instanceId"] = json!(format!("urn:ngsi-ld:Instance:b-{n}-{j}"));
                i["modifiedAt"] = json!(modified);
                i["createdAt"] = json!(modified);
            }
        }
        attrs
    };
    let values = |attr: &str| {
        let (pool, attr) = (pool.clone(), attr.to_owned());
        async move {
            sqlx::query_scalar::<_, serde_json::Value>(
                "SELECT data->'value' FROM attr_instances
                 WHERE tenant_id = 'pgtempbounded' AND entity_id = $1 AND attr_id = $2
                 ORDER BY observed_at",
            )
            .bind(id)
            .bind(attr)
            .fetch_all(&pool)
            .await
            .expect("rows")
        }
    };
    // Without observedAt: "a" five weeks ago, then "b", "b", "a" now.
    for (v, ago) in [("a", 50400), ("b", 3), ("b", 2), ("a", 1)] {
        let adds = write(json!({name: [{"type": "Property", "value": v}]}), at(ago));
        s.append(&t, id, &shell, &adds).await.expect("append");
    }
    assert_eq!(values(name).await, [json!("a"), json!("b"), json!("a")]);
    // With observedAt: two weeks in one append, then both corrected at once.
    let (old, new) = (at(50400), at(0));
    let pair = |v: i64| {
        json!({speed: [
        {"type": "Property", "value": v, "observedAt": old},
        {"type": "Property", "value": v, "observedAt": new}]})
    };
    for v in [1, 1, 2] {
        let adds = write(pair(v), at(0));
        s.append(&t, id, &shell, &adds).await.expect("append");
    }
    assert_eq!(
        values(speed).await,
        [json!(2), json!(2)],
        "corrected in place"
    );
    let _ = s.delete(&t, id).await;
}

/// an auto-recorded append writes a row only for a new observation.
/// With observedAt: one row per (attribute, datasetId, observedAt), a new
/// value at the same observedAt corrects that row in place. Without: a row
/// only when the value differs from the newest stored one.
#[tokio::test(flavor = "multi_thread")]
async fn an_unchanged_append_records_no_new_instance() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let s = PgTemporalStore::new(pool.clone());
    let t = TenantId::new("pgtempdedup").expect("tenant");
    let other = TenantId::new("pgtempdedup2").expect("tenant");
    let id = "urn:ngsi-ld:Vehicle:dedup";
    for tenant in [&t, &other] {
        pg::ensure_tenant(&pool, tenant).await.expect("tenant row");
        let _ = s.delete(tenant, id).await;
        ensure_entity(&pool, tenant, id).await;
    }
    let speed = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let name = "https://uri.etsi.org/ngsi-ld/default-context/name";
    let shell = json!({"id": id, "type": "Vehicle",
        "createdAt": "2026-08-15T09:00:00Z", "modifiedAt": "2026-08-15T09:00:00Z"});
    let mut n = 0;
    let mut append = |tenant: &TenantId, adds: serde_json::Value| {
        n += 1;
        let at = format!("2026-08-15T09:{n:02}:00Z");
        // Every write stamps a fresh instanceId and modifiedAt, as the API does.
        let mut adds = adds;
        for (k, a) in adds.as_object_mut().expect("attrs").iter_mut() {
            for (j, i) in a.as_array_mut().expect("instances").iter_mut().enumerate() {
                i["instanceId"] =
                    json!(format!("urn:ngsi-ld:Instance:{k}-{n}-{j}").replace("https://", ""));
                i["modifiedAt"] = json!(at);
                i["createdAt"] = json!("2026-08-15T09:00:00Z");
            }
        }
        (tenant.clone(), adds)
    };
    let rows = |tenant: &TenantId, attr: &str| {
        let (pool, tenant, attr) = (pool.clone(), tenant.as_str().to_owned(), attr.to_owned());
        async move {
            sqlx::query_as::<_, (String, Option<String>, serde_json::Value)>(
                "SELECT instance_id, dataset_id, data FROM attr_instances
                 WHERE tenant_id = $1 AND entity_id = $2 AND attr_id = $3 ORDER BY observed_at",
            )
            .bind(tenant)
            .bind(id)
            .bind(attr)
            .fetch_all(&pool)
            .await
            .expect("rows")
        }
    };
    let observed =
        |v: i64, at: &str| json!({speed: [{"type": "Property", "value": v, "observedAt": at}]});

    for _ in 0..3 {
        let (tn, adds) = append(&t, observed(10, "2026-08-15T08:00:00Z"));
        s.append(&tn, id, &shell, &adds).await.expect("append");
    }
    assert_eq!(
        rows(&t, speed).await.len(),
        1,
        "three unchanged writes, one instance"
    );
    let (tn, adds) = append(&t, observed(10, "2026-08-15T08:05:00Z"));
    s.append(&tn, id, &shell, &adds).await.expect("append");
    assert_eq!(
        rows(&t, speed).await.len(),
        2,
        "a new observedAt is a new instance"
    );
    let kept = rows(&t, speed).await[1].0.clone();
    let (tn, adds) = append(&t, observed(11, "2026-08-15T08:05:00Z"));
    s.append(&tn, id, &shell, &adds).await.expect("append");
    let got = rows(&t, speed).await;
    assert_eq!(
        got.len(),
        2,
        "a corrected value at the same observedAt stays one instance"
    );
    assert_eq!(
        (got[1].0.as_str(), &got[1].2["value"]),
        (kept.as_str(), &json!(11))
    );
    // the kept id is the column (checked above); data does not repeat it
    assert_eq!(
        got[1].2.get("instanceId"),
        None,
        "the instance keeps its id in its column"
    );

    // Without observedAt: compared with the newest stored value.
    let plain = |v: &str| json!({name: [{"type": "Property", "value": v}]});
    for v in ["a", "a", "b", "b", "a"] {
        let (tn, adds) = append(&t, plain(v));
        s.append(&tn, id, &shell, &adds).await.expect("append");
    }
    let got = rows(&t, name).await;
    let values: Vec<_> = got.iter().map(|r| r.2["value"].clone()).collect();
    assert_eq!(
        values,
        [json!("a"), json!("b"), json!("a")],
        "a change, and a change back, are recorded"
    );

    // Two datasetIds are two series; two equal entries in one append are one.
    let both = |v: i64| {
        json!({speed: [
        {"type": "Property", "value": v, "observedAt": "2026-08-15T07:00:00Z", "datasetId": "urn:ds:a"},
        {"type": "Property", "value": v, "observedAt": "2026-08-15T07:00:00Z", "datasetId": "urn:ds:b"},
        {"type": "Property", "value": v, "observedAt": "2026-08-15T07:00:00Z", "datasetId": "urn:ds:b"}]})
    };
    let (tn, adds) = append(&t, both(5));
    s.append(&tn, id, &shell, &adds).await.expect("append");
    let (tn, adds) = append(&t, both(5));
    s.append(&tn, id, &shell, &adds).await.expect("append");
    let mut sets: Vec<_> = rows(&t, speed)
        .await
        .into_iter()
        .filter_map(|r| r.1)
        .collect();
    sets.sort();
    assert_eq!(sets, ["urn:ds:a", "urn:ds:b"]);

    // Another tenant's identical history never suppresses this tenant's.
    let (tn, adds) = append(&other, observed(10, "2026-08-15T08:00:00Z"));
    s.append(&tn, id, &shell, &adds).await.expect("append");
    assert_eq!(rows(&other, speed).await.len(), 1);

    // without observedAt each datasetId is compared with its own newest
    // row; the first write of a series has none and is recorded.
    let note = "https://uri.etsi.org/ngsi-ld/default-context/note";
    let in_set =
        |ds: &str, v: &str| json!({note: [{"type": "Property", "value": v, "datasetId": ds}]});
    for (ds, v) in [
        ("urn:ds:a", "x"),
        ("urn:ds:b", "x"),
        ("urn:ds:a", "x"),
        ("urn:ds:b", "y"),
    ] {
        let (tn, adds) = append(&t, in_set(ds, v));
        s.append(&tn, id, &shell, &adds).await.expect("append");
    }
    let got: Vec<_> = rows(&t, note)
        .await
        .into_iter()
        .map(|r| (r.1.unwrap_or_default(), r.2["value"].clone()))
        .collect();
    assert_eq!(
        got,
        [
            ("urn:ds:a".to_owned(), json!("x")),
            ("urn:ds:b".to_owned(), json!("x")),
            ("urn:ds:b".to_owned(), json!("y"))
        ],
        "a repeat in one series is skipped, a change in the other recorded"
    );
    // The newest row resent under its own instanceId with another value is a
    // correction of that row, not a skip and not a second row.
    let newest = rows(&t, note).await.pop().expect("a row");
    let fix = json!({note: [{"type": "Property", "value": "z", "datasetId": "urn:ds:b",
        "instanceId": newest.0, "createdAt": "2026-08-15T09:00:00Z", "modifiedAt": newest.2["modifiedAt"]}]});
    s.append(&t, id, &shell, &fix).await.expect("append");
    let got = rows(&t, note).await;
    assert_eq!(got.len(), 3, "still three rows");
    assert_eq!(got[2].2["value"], json!("z"), "the newest row corrected");

    // The reconstructed evolution reads the same series.
    let doc = s.get(&t, id).await.expect("get").expect("present");
    assert_eq!(doc[name].as_array().map(Vec::len), Some(3), "{doc}");
    for tenant in [&t, &other] {
        let _ = s.delete(tenant, id).await;
        sqlx::query("DELETE FROM entities WHERE tenant_id = $1")
            .bind(tenant.as_str())
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}

/// S3-06: one append (a drained request) that sets a value, another, then
/// the first again records all three, and an append whose first row
/// repeats the stored newest skips that row only: the rest of the batch
/// follows its own predecessor, not the row stored before the batch. In the
/// text shape and in the keyed one (a cut-over before the writes).
#[tokio::test(flavor = "multi_thread")]
async fn a_value_that_returns_within_one_append_is_recorded() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgtempreturn").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let name = "https://uri.etsi.org/ngsi-ld/default-context/name";
    let base = chrono::Utc::now() - chrono::Duration::minutes(30);
    let cut = base - chrono::Duration::days(1);
    for (shape, s) in [
        ("text", PgTemporalStore::with_cutover(pool.clone(), None)),
        (
            "keyed",
            PgTemporalStore::with_cutover(pool.clone(), Some(cut)),
        ),
    ] {
        let id = format!("urn:ngsi-ld:Vehicle:return-{shape}");
        let _ = s.delete(&t, &id).await;
        ensure_entity(&pool, &t, &id).await;
        let at = |m: i64| {
            (base + chrono::Duration::minutes(m)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        };
        let shell = json!({"id": id, "type": "Vehicle", "createdAt": at(0), "modifiedAt": at(0)});
        let batch = |vals: &[(&str, i64)]| {
            json!({name: vals.iter().map(|(v, m)| json!({"type": "Property", "value": v,
                "instanceId": format!("urn:ngsi-ld:Instance:{shape}-{m}"),
                "createdAt": at(0), "modifiedAt": at(*m)})).collect::<Vec<_>>()})
        };
        let values = || async {
            let doc = s.get(&t, &id).await.expect("get").expect("present");
            let mut inst: Vec<serde_json::Value> =
                doc[name].as_array().cloned().unwrap_or_default();
            inst.sort_by_key(|i| i["modifiedAt"].as_str().unwrap_or_default().to_owned());
            inst.into_iter()
                .map(|i| i["value"].clone())
                .collect::<Vec<_>>()
        };
        s.append(&t, &id, &shell, &batch(&[("a", 1), ("b", 2), ("a", 3)]))
            .await
            .expect("append");
        assert_eq!(
            values().await,
            [json!("a"), json!("b"), json!("a")],
            "{shape}"
        );
        s.append(&t, &id, &shell, &batch(&[("a", 4), ("b", 5), ("a", 6)]))
            .await
            .expect("append");
        assert_eq!(
            values().await,
            [json!("a"), json!("b"), json!("a"), json!("b"), json!("a")],
            "{shape}: the first row repeats the stored newest, the rest are new"
        );
        let stored: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM attr_rows WHERE tenant_id = $1 AND entity_no = iri_no($1, $2)",
        )
        .bind(t.as_str())
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("keyed rows");
        assert_eq!(stored, if shape == "keyed" { 5 } else { 0 }, "{shape}");
        let _ = s.delete(&t, &id).await;
    }
}

/// S3-11: the 4.22 current-state prune commits on its own. The history reap
/// after it may not finish (here a row of `attr_versions` held past the
/// session's 5 s lock_timeout, as a scan past statement_timeout ends the same
/// way); one that did not used to roll the prune back with it, so on a
/// history large enough to time out every tick expired current-state
/// instances were never pruned.
#[tokio::test(flavor = "multi_thread")]
async fn the_current_state_prune_survives_a_failed_history_reap() {
    let url = require_db!();
    let pool = pg::connect(&url, 5).await.expect("pool");
    let t = TenantId::new("pgprunealone").expect("tenant");
    pg::ensure_tenant(&pool, &t).await.expect("tenant row");
    let store = pg::entity::PgEntityStore::new(pool.clone());
    let id = "urn:ngsi-ld:T:prunealone";
    let _ = store.delete(&t, id).await;
    let speed = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    let doc = json!({"id": id, "type": ["T"],
        "createdAt": "2026-08-04T09:00:00Z", "modifiedAt": "2026-08-04T09:00:00Z",
        speed: [{"type": "Property", "value": 1, "datasetId": "urn:d:gone",
                 "expiresAt": "2020-01-01T00:00:00Z"},
                {"type": "Property", "value": 2}]});
    assert!(store.create(&t, id, &doc).await.expect("create"));
    // an expired superseded version the history reap must delete, held
    let held = "urn:ngsi-ld:Instance:prunealone-held";
    sqlx::query(
        "INSERT INTO attr_versions (tenant_id, entity_id, attr_id, instance_id,
                                    observed_at, version, data, expires_at)
         VALUES ($1, $2, 'urn:a', $3, now(), 1, '{\"expiresAt\": \"2020-01-01T00:00:00Z\"}',
                 '2020-01-01T00:00:00Z')
         ON CONFLICT DO NOTHING",
    )
    .bind(t.as_str())
    .bind(id)
    .bind(held)
    .execute(&pool)
    .await
    .expect("version row");
    let mut blocker = pool.begin().await.expect("blocker");
    sqlx::query("SELECT 1 FROM attr_versions WHERE tenant_id = $1 AND instance_id = $2 FOR UPDATE")
        .bind(t.as_str())
        .bind(held)
        .execute(&mut *blocker)
        .await
        .expect("hold the row");
    let due = || async {
        sqlx::query_scalar::<_, bool>(
            "SELECT attrs_expire_at IS NOT NULL AND attrs_expire_at < now()
               FROM entities WHERE tenant_id = $1 AND id = $2",
        )
        .bind(t.as_str())
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("entity row")
    };
    assert!(due().await, "the Entity holds an expired instance");
    let msg = maintenance_winning(&pool, None).await.expect("maintenance");
    blocker.rollback().await.expect("release");
    assert!(
        msg.contains("instance reap skipped"),
        "the history reap failed: {msg}"
    );
    assert!(
        !due().await,
        "the prune went down with the history reap: {msg}"
    );
    sqlx::query("DELETE FROM attr_versions WHERE tenant_id = $1")
        .bind(t.as_str())
        .execute(&pool)
        .await
        .expect("cleanup");
    let _ = store.delete(&t, id).await;
}

/// The tables of the rows of `tenants` all carry a valid `expires_at` index
/// (migration 0030): the maintenance job builds them, and a sibling test's
/// pass may hold the build while this one asks.
async fn expiry_indexes_built(pool: &sqlx::PgPool, tenants: &[&str]) {
    let tenants: Vec<String> = tenants.iter().map(|t| (*t).to_owned()).collect();
    for _ in 0..100 {
        antares_sql::store::pg::maintenance::history_expiry_index(pool)
            .await
            .expect("expiry index build");
        let unindexed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT DISTINCT tableoid FROM attr_instances WHERE tenant_id = ANY($1)
                                   UNION SELECT DISTINCT tableoid FROM attr_rows WHERE tenant_id = ANY($1)
                                   UNION SELECT 'attr_versions'::regclass::oid) t
              WHERE NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid
                                 WHERE x.indrelid = t.tableoid AND x.indisvalid
                                   AND ic.relname LIKE '%\\_expires')",
        )
        .bind(&tenants)
        .fetch_one(pool)
        .await
        .expect("indexes");
        if unindexed == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("the expires_at indexes were never built");
}

/// S3-11: the 4.22 history reap reads `expires_at` (migration 0030), which
/// both history shapes and the kept versions fill at write time, instead of
/// casting every row's `data->>'expiresAt'` each tick. Rows written before
/// the column (`expires_at` NULL, an expired `expiresAt` in `data`) are kept
/// by the tick's reap and reaped by the daily scan; a row expiring later and
/// one without expiry are kept by both, in every Tenant.
#[tokio::test(flavor = "multi_thread")]
async fn the_history_reap_finds_expired_rows_by_their_column() {
    let url = require_db!();
    let _ddl = PARTITION_DDL.lock().await;
    let pool = pg::connect(&url, 5).await.expect("pool");
    let (ta, tb) = ("pgreapcol-a", "pgreapcol-b");
    let clean = || async {
        for table in [
            "attr_instances",
            "attr_rows",
            "attr_versions",
            "temporal_entities",
            "entities",
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE tenant_id = ANY($1)"
            )))
            .bind(vec![ta, tb])
            .execute(&pool)
            .await
            .expect("clean");
        }
    };
    clean().await;
    let attr = "https://uri.etsi.org/ngsi-ld/default-context/speed";
    // observed this hour: the week's partition or chunk exists and a
    // timescale chunk this young is not compressed
    let now = chrono::Utc::now();
    let at = |m: i64| (now - chrono::Duration::minutes(m)).to_rfc3339();
    let inst = |id: &str, value: i64, m: i64, expires: Option<&str>| {
        let mut i = json!({"type": "Property", "value": value, "observedAt": at(m),
                           "instanceId": format!("urn:ngsi-ld:Instance:{id}")});
        if let Some(e) = expires {
            i["expiresAt"] = json!(e);
        }
        i
    };
    let id = "urn:ngsi-ld:T:reapcol";
    let shell = json!({"id": id, "type": ["T"], "createdAt": at(10), "modifiedAt": at(10)});
    let mut doc = shell.clone();
    doc[attr] = json!([
        inst("gone", 1, 9, Some("2020-01-01T00:00:00Z")),
        inst("later", 2, 8, Some("2099-01-01T00:00:00Z")),
        inst("durable", 3, 7, None)
    ]);
    // tenant a in the text shape, b in the keyed one (attr_rows)
    let text = PgTemporalStore::new(pool.clone());
    let keyed = PgTemporalStore::with_cutover(pool.clone(), Some(chrono::DateTime::UNIX_EPOCH));
    for (t, s) in [(ta, &text), (tb, &keyed)] {
        let t = TenantId::new(t).expect("tenant");
        pg::ensure_tenant(&pool, &t).await.expect("tenant row");
        ensure_entity(&pool, &t, id).await;
        assert!(s.create(&t, id, &doc).await.expect("create"));
        // a correction of the expired instance: its old version goes to
        // attr_versions with the column
        let fix = json!({attr: [inst("gone", 10, 9, Some("2020-01-01T00:00:00Z"))]});
        s.append(&t, id, &shell, &fix).await.expect("correct");
    }
    let stamped = |table: &'static str, t: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {table} WHERE tenant_id = $1 AND expires_at IS NOT NULL"
            )))
            .bind(t)
            .fetch_one(&pool)
            .await
            .expect("count")
        }
    };
    assert_eq!(stamped("attr_instances", ta).await, 2, "text rows stamped");
    assert_eq!(stamped("attr_rows", tb).await, 2, "keyed rows stamped");
    assert_eq!(
        stamped("attr_versions", ta).await,
        1,
        "text version stamped"
    );
    assert_eq!(
        stamped("attr_versions", tb).await,
        1,
        "keyed version stamped"
    );
    // Spent before the rows the daily scan reaps are written: the UPDATE
    // waits for a sibling's scan that holds the claim, and none starts after.
    sqlx::query("UPDATE maintenance_jobs SET last_run = now() WHERE name = 'history_expiry_scan'")
        .execute(&pool)
        .await
        .expect("the daily scan is not due");
    // rows as a release before 0030 wrote them
    for (table, iid) in [
        ("attr_instances", "urn:ngsi-ld:Instance:legacy"),
        ("attr_versions", "urn:ngsi-ld:Instance:legacyv"),
    ] {
        let cols = if table == "attr_instances" {
            "(tenant_id, entity_id, attr_id, instance_id, observed_at, created_at, modified_at, data)
             VALUES ($1, $2, $3, $4, now(), now(), now(), '{\"expiresAt\": \"2020-01-01T00:00:00Z\"}')"
        } else {
            "(tenant_id, entity_id, attr_id, instance_id, observed_at, version, data)
             VALUES ($1, $2, $3, $4, now(), 1, '{\"expiresAt\": \"2020-01-01T00:00:00Z\"}')"
        };
        for t in [ta, tb] {
            sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {table} {cols}")))
                .bind(t)
                .bind(id)
                .bind(attr)
                .bind(iid)
                .execute(&pool)
                .await
                .expect("legacy row");
        }
    }
    let left = |t: &'static str| {
        let pool = pool.clone();
        async move {
            let mut ids: Vec<String> = sqlx::query_scalar(
                "SELECT 'h:' || COALESCE(data->>'instanceId', instance_id) FROM (
                   SELECT instance_id, data FROM attr_instances WHERE tenant_id = $1
                   UNION ALL SELECT r.instance_no::text, r.data FROM attr_rows r WHERE tenant_id = $1) h
                 UNION ALL SELECT 'v:' || instance_id FROM attr_versions WHERE tenant_id = $1",
            )
            .bind(t)
            .fetch_all(&pool)
            .await
            .expect("rows");
            ids.sort();
            ids
        }
    };
    let kept = |legacy: bool| {
        let mut v = vec![
            "h:urn:ngsi-ld:Instance:durable",
            "h:urn:ngsi-ld:Instance:later",
        ];
        if legacy {
            v.extend([
                "h:urn:ngsi-ld:Instance:legacy",
                "v:urn:ngsi-ld:Instance:legacyv",
            ]);
        }
        let mut v: Vec<String> = v.into_iter().map(str::to_owned).collect();
        v.sort();
        v
    };
    for t in [ta, tb] {
        assert_eq!(left(t).await.len(), 6, "{t}: {:?}", left(t).await);
    }
    expiry_indexes_built(&pool, &[ta, tb]).await;
    let msg = maintenance_winning(&pool, None).await.expect("maintenance");
    assert!(!msg.contains("daily scan"), "{msg}");
    for t in [ta, tb] {
        assert_eq!(
            left(t).await,
            kept(true),
            "{t}: the rows the column marks expired reaped, the rest kept: {msg}"
        );
    }
    sqlx::query("UPDATE maintenance_jobs SET last_run = NULL WHERE name = 'history_expiry_scan'")
        .execute(&pool)
        .await
        .expect("the daily scan is due");
    // a sibling's pass may be the one that runs it: the rows tell
    let msg = maintenance_winning(&pool, None).await.expect("maintenance");
    for t in [ta, tb] {
        assert_eq!(
            left(t).await,
            kept(false),
            "{t}: the daily scan reaped the rows written before the column: {msg}"
        );
    }
    clean().await;
}
