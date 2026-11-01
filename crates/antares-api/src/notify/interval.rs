// SPDX-License-Identifier: EUPL-1.2
//! Periodic (timeInterval) subscriptions: due times, the cross-replica claim and the sweep tick.

use super::*;

/// When an interval subscription is next due, in epoch millis: one
/// `timeInterval` after the last Notification it sent (Table 5.2.14.2-1
/// `lastNotification`), or after its creation while it has sent none. Without
/// either anchor it is due immediately.
pub(super) fn due_at_ms(sub: &Value, interval: f64) -> i64 {
    let anchor = sub
        .get("notification")
        .and_then(|n| n.get("lastNotification"))
        .and_then(Value::as_str)
        .or_else(|| sub.get("createdAt").and_then(Value::as_str));
    match anchor.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        Some(last) => last
            .timestamp_millis()
            .saturating_add(interval_offset_ms(interval)),
        None => i64::MIN,
    }
}

/// One period of a periodic Subscription (5.2.12 `timeInterval`) in
/// milliseconds.
///
/// Table 5.2.12-1 bounds the member only as greater than 0, so the seconds a
/// client names can exceed what epoch milliseconds hold. The cast saturates,
/// and every caller adds it with `saturating_add`, which puts an interval the
/// broker cannot schedule at the end of representable time. Adding it plainly
/// wraps the sum negative, and a negative firing instant reads as permanently
/// due: the subscription then fires its whole query on every tick, and the
/// same value poisons the process-wide sweep clock those minima feed.
pub(super) fn interval_offset_ms(interval: f64) -> i64 {
    (interval * 1000.0) as i64
}

/// timeInterval subscriptions: fire when due, with all matching entities.
/// Multi-instance: claim one interval firing under the subscription row
/// lock — N matcher pods race, exactly one wins (single-winner by
/// lock, no leader election). The due-check reruns INSIDE the lock; the
/// winner stamps `lastNotification` as its claim, losers see not-due and
/// roll back. Only engaged in bus=nats mode — single-process behaviour (and
/// its 046_12 bookkeeping ordering) is untouched.
///
/// `None` = the firing is not this pod's. `Some(prev)` = claimed, carrying
/// the `lastNotification` the claim overwrote: a firing that turns out to
/// have nothing to send gives it back through [`release_interval`], because
/// Table 5.2.14.2-1 stamps the instant a notification was SENT and 5.8.6
/// sends none when nothing matches.
pub(super) async fn claim_interval(
    st: &AppState,
    tenant: &TenantId,
    kind: Kind,
    sub: &Value,
    interval: f64,
) -> Option<Option<Value>> {
    let id = sub.get("id").and_then(Value::as_str)?;
    let mut prev: Option<Value> = None;
    let res = st
        .store
        .mutate(tenant, kind, id, |doc| {
            if chrono::Utc::now().timestamp_millis() < due_at_ms(doc, interval) {
                return Err(());
            }
            let Some(sub_doc) = doc.as_object_mut() else {
                // a stored Subscription is an object; one that is not carries no
                // notification member to stamp, and Err(()) is the same "nothing
                // written" the not-due branch above returns
                return Err(());
            };
            if let Some(n) = sub_doc
                .entry("notification")
                .or_insert_with(|| json!({}))
                .as_object_mut()
            {
                prev = n.insert("lastNotification".into(), Value::String(now_iso()));
            }
            Ok(())
        })
        .await;
    matches!(res, Ok(Some(Ok(())))).then_some(prev)
}

/// Give a claimed firing back (5.8.6: nothing matched, so nothing was sent).
/// The stamp returns to what [`claim_interval`] found, which keeps
/// `lastNotification` truthful exactly as the single-process path does; the
/// sweep parks the subscription until its next interval in this pod's mirror.
pub(super) async fn release_interval(
    st: &AppState,
    tenant: &TenantId,
    kind: Kind,
    id: &str,
    prev: Option<Value>,
) {
    let res = st
        .store
        .mutate::<(), ()>(tenant, kind, id, |doc| {
            if let Some(n) = doc
                .as_object_mut()
                .and_then(|o| o.get_mut("notification"))
                .and_then(Value::as_object_mut)
            {
                match &prev {
                    Some(v) => n.insert("lastNotification".into(), v.clone()),
                    None => n.remove("lastNotification"),
                };
            }
            Ok(())
        })
        .await;
    if let Err(e) = res {
        tracing::warn!("releasing the interval claim for {id} failed: {e}");
    }
}

/// One sweep of the interval subscriptions (5.8.6, 5.11.7): "If a
/// Subscription defines a timeInterval member, a Notification shall be sent
/// periodically, when the time interval (in seconds) specified in such value
/// field is reached, regardless of Attribute changes."
///
/// The sweep runs on a fixed tick, so its idle cost is what has to stay
/// small. Two things keep it off the store. A tick that cannot fire anything
/// returns before enumerating tenants: each sweep records the earliest instant
/// a subscription it saw can next be due, and a write zeroes that clock —
/// through the mirror for Subscriptions, through `csub_written` for the
/// Context Source Registration Subscription half, which is mirrored by clock
/// rather than by document. A lost signal is repaired within
/// `CSUB_SWEEP_BACKSTOP_MS`. And a due subscription reads only the Entities
/// its own selector can match instead of its tenant's entity set.
pub async fn interval_tick(st: &AppState) {
    use std::sync::atomic::Ordering::Relaxed;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let clocks = st.sub_mirror.as_ref().map(|m| {
        (
            m.next_sub_sweep_ms.load(Relaxed),
            m.next_csub_sweep_ms.load(Relaxed),
        )
    });
    let (sweep_subs, sweep_csubs) = match clocks {
        Some((sub_clock, csub_clock)) => (now_ms >= sub_clock, now_ms >= csub_clock),
        // Never-wired fallback: no clock to keep, so every tick sweeps.
        None => (true, true),
    };
    if !sweep_subs && !sweep_csubs {
        return;
    }
    // Earliest next-due instant seen by this sweep, per half.
    let mut next_sub = i64::MAX;
    let mut next_csub = i64::MAX;
    let tenants = read_or_warn(
        st.store.subscription_tenants().await,
        "the tenants with subscriptions",
    )
    .into_iter()
    .filter_map(|t| TenantId::new_internal(&t).ok());
    // Each Tenant is swept on its own task. A Tenant waits for its own share
    // of the delivery width before it reads its next page, so a Tenant whose
    // endpoint hangs holds that wait for up to the endpoint's whole timeout:
    // swept in turn, every Tenant after it would wait as long, and one
    // Tenant would set every other Tenant's periodic deadline.
    #[cfg(not(target_arch = "wasm32"))]
    {
        let mut sweeps = tokio::task::JoinSet::new();
        let mut fold = |joined: Option<Result<(i64, i64), tokio::task::JoinError>>| match joined {
            Some(Ok((sub, csub))) => {
                next_sub = next_sub.min(sub);
                next_csub = next_csub.min(csub);
            }
            // a sweep that died decided nothing about its Tenant: the next
            // tick sweeps again
            Some(Err(_)) => {
                note_panic();
                next_sub = next_sub.min(now_ms);
                next_csub = next_csub.min(now_ms);
            }
            None => {}
        };
        for tenant in tenants {
            // ponytail: at most tenant_sweep_width() Tenants are swept at once
            if sweeps.len() >= tenant_sweep_width() {
                fold(sweeps.join_next().await);
            }
            let st = st.clone();
            sweeps.spawn(async move {
                sweep_tenant(&st, &tenant, now_ms, sweep_subs, sweep_csubs).await
            });
        }
        while let Some(joined) = sweeps.join_next().await {
            fold(Some(joined));
        }
    }
    #[cfg(target_arch = "wasm32")]
    for tenant in tenants {
        let (sub, csub) = sweep_tenant(st, &tenant, now_ms, sweep_subs, sweep_csubs).await;
        next_sub = next_sub.min(sub);
        next_csub = next_csub.min(csub);
    }
    if let (Some(m), Some((sub_clock, csub_clock))) = (&st.sub_mirror, clocks) {
        // A periodic subscription written DURING the sweep has already zeroed
        // the clock: the exchange then fails, the zero stands and the next
        // tick sweeps rather than waiting out an interval computed without it.
        if sweep_subs {
            let _ = m
                .next_sub_sweep_ms
                .compare_exchange(sub_clock, next_sub, Relaxed, Relaxed);
        }
        if sweep_csubs {
            let _ = m.next_csub_sweep_ms.compare_exchange(
                csub_clock,
                next_csub.min(now_ms.saturating_add(CSUB_SWEEP_BACKSTOP_MS)),
                Relaxed,
                Relaxed,
            );
        }
    }
}

/// How many Tenants one interval sweep visits at once. Each one reads the
/// store while it runs, so the width takes at most half the connection
/// pool (`ANTARES_PG_POOL`, default 20, which the broker has validated) and
/// never more than 8: the request path keeps the other half. A Tenant
/// waiting for its delivery share holds no connection.
#[cfg(not(target_arch = "wasm32"))]
fn tenant_sweep_width() -> usize {
    static WIDTH: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *WIDTH.get_or_init(|| sweep_width_for(std::env::var("ANTARES_PG_POOL").ok().as_deref()))
}

#[cfg(not(target_arch = "wasm32"))]
fn sweep_width_for(pool: Option<&str>) -> usize {
    let pool = pool.and_then(|v| v.parse::<usize>().ok()).unwrap_or(20);
    (pool / 2).clamp(1, 8)
}

/// One Tenant's half of [`interval_tick`]: fires its due periodic
/// Subscriptions and Context Source Registration Subscriptions and answers
/// the earliest instant each half can next be due. It returns when its
/// deliveries have settled, so ticks never overlap and a subscription cannot
/// be fired twice for one period.
async fn sweep_tenant(
    st: &AppState,
    tenant: &TenantId,
    now_ms: i64,
    sweep_subs: bool,
    sweep_csubs: bool,
) -> (i64, i64) {
    let tenant = tenant.clone();
    let mut next_sub = i64::MAX;
    let mut next_csub = i64::MAX;
    // A delivery costs up to the endpoint's whole timeout (Table 5.2.15-1,
    // 30 s at the ceiling). Awaited in turn, one unresponsive endpoint becomes
    // the deadline of every other subscriber's periodic notification. The
    // deliveries therefore run together, under the Tenant's share of the
    // width the change path uses.
    #[cfg(not(target_arch = "wasm32"))]
    let mut sending = tokio::task::JoinSet::new();
    // Same source the matcher reads: the indexed mirror, with the store
    // list only as the never-wired fallback.
    let subs = match (&st.sub_mirror, sweep_subs) {
        (_, false) => Vec::new(),
        (Some(m), _) => m.periodic_docs(tenant.as_str()),
        (None, _) => read_or_warn(
            st.store.list(&tenant, Kind::Subscription).await,
            "the periodic Subscriptions",
        )
        .into_iter()
        .map(Arc::new)
        .collect(),
    };
    for sub in subs {
        let Some(interval) = sub.get("timeInterval").and_then(Value::as_f64) else {
            continue;
        };
        if !is_active(&sub) {
            continue;
        }
        let sub_id = sub.get("id").and_then(Value::as_str).unwrap_or_default();
        // a firing that had nothing to send ends that interval (below)
        let quiet = st
            .sub_mirror
            .as_ref()
            .and_then(|m| m.quiet_until(tenant.as_str(), sub_id));
        let due_at = due_at_ms(&sub, interval).max(quiet.unwrap_or(i64::MIN));
        if now_ms < due_at {
            next_sub = next_sub.min(due_at);
            continue;
        }
        // Due: the following firing is one interval away. Recorded before
        // the claim, so a pod that LOSES the race (another one is firing
        // this subscription right now) keeps sweeping on the interval
        // instead of parking on an anchor only the winner advanced.
        next_sub = next_sub.min(now_ms.saturating_add(interval_offset_ms(interval)));
        // a stored q that no longer parses (one percent-encoded before
        // 5.2.12's read-as-written) matches nothing, as conditions_match
        // decides: no Entity to read and no Notification to send, so
        // nothing to claim either (the claim stamped
        // lastNotification for a firing that never ran)
        if sub_str(&sub, "q").is_some_and(|q| antares_ql::regex::q_node(q).is_none()) {
            continue;
        }
        let claim = if st.nats {
            match claim_interval(st, &tenant, Kind::Subscription, &sub, interval).await {
                Some(prev) => Some(prev),
                None => continue,
            }
        } else {
            None
        };
        let ctx = sub_context(st, &tenant, &sub).await;
        let now = now_iso();
        // 5.8.6: the periodic Notification "shall include all the
        // subscribed Entities that match the query, geoquery and Scope
        // query conditions" — so the read is exactly this subscription's
        // own selector (5.2.33) and conditions, never the tenant's entity
        // set. Only predicates a store reproduces without hiding a
        // candidate are offered (ids when every selector entry names one,
        // types when the index proves them plain, q/scopeQ/geoQ under the
        // store's own rule that SQL removes rows and never decides them);
        // the selector_match/conditions_match pair below stays the
        // arbiter, exactly as on the query path.
        let type_groups: Vec<Vec<String>> = match index_keys(&sub) {
            Keys::Types(ts) => ts.into_iter().map(|t| vec![t]).collect(),
            _ => Vec::new(),
        };
        let ids = selector_ids(&sub);
        // The filter borrows a term expander that is not Sync, so it lives
        // and dies inside this block: held across the delivery await it
        // would make the whole interval task non-Send.
        // the match set is read a page at a time and each
        // page's matches leave as their own Notification, so a type-only
        // selector over a large tenant never holds its whole set; a store
        // that cannot decide the filter answers it whole, once.
        let page_size = st.max_limit.max(1);
        let mut offset = 0usize;
        let mut sent = false;
        loop {
            let (rows, last) = {
                let expand = |t: &str| ctx.expand_key(t);
                let id_refs: Vec<&str> = ids.iter().flatten().map(String::as_str).collect();
                // q is read as written (5.2.12); parses shared per distinct
                // expression text, as in conditions_match — the sweep
                // re-runs per due subscription
                let q_ast = sub_str(&sub, "q").and_then(antares_ql::regex::q_node);
                let geo = sub.get("geoQ").and_then(Value::as_object).and_then(|g| {
                    let key = serde_json::to_string(g).unwrap_or_default();
                    antares_ql::regex::geo_query(&key, || {
                        antares_ql::geo::GeoQuery::from_params(&geo_params(g))
                            .ok()
                            .flatten()
                    })
                });
                let geo_spec = geo.as_ref().and_then(|g| g.to_sql_spec(&ctx));
                let filter = antares_store::filter::EntityFilter {
                    ids: ids.as_ref().map(|_| id_refs.as_slice()),
                    types: (!type_groups.is_empty()).then_some(type_groups.as_slice()),
                    q: q_ast.as_deref(),
                    scope_q: sub_str(&sub, "scopeQ"),
                    geo: geo_spec.as_slice(),
                    expand: &expand,
                    page: Some(antares_store::filter::Page {
                        offset: i64::try_from(offset).unwrap_or(i64::MAX),
                        limit: i64::try_from(page_size).unwrap_or(i64::MAX),
                        count: false,
                    }),
                    ..Default::default()
                };
                match st.store.query_entities(&tenant, &filter).await {
                    Ok(o) => {
                        let last = !o.paged || o.rows.len() < page_size;
                        (o.rows, last)
                    }
                    Err(e) => {
                        tracing::warn!("notification path: reading the Entities a periodic Subscription notifies about failed: {e}");
                        (Vec::new(), true)
                    }
                }
            };
            offset = offset.saturating_add(rows.len());
            let mut matching: Vec<Value> = Vec::new();
            for d in rows {
                if !selector_match(&sub, &d, &ctx)
                    || !linked_eval(st, &tenant, None, |l| conditions_match(&sub, &d, &ctx, l))
                        .await
                {
                    continue;
                }
                matching.extend(
                    build_data(st, &tenant, &sub, &ctx, None, Some(&d), &[], false, &now).await,
                );
            }
            if !matching.is_empty() {
                sent = true;
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // The slot is taken BEFORE the next page is read: a
                    // page waiting for one is a page held in memory, and
                    // against a slow endpoint the read would run ahead to
                    // the end of the match set.
                    let permits = delivery_permits(&tenant).await;
                    let (st, tenant, ctx, sub) =
                        (st.clone(), tenant.clone(), Arc::clone(&ctx), sub.clone());
                    sending.spawn(async move {
                        let _permits = permits;
                        deliver(&st, &tenant, &sub, matching, &ctx).await;
                    });
                }
                #[cfg(target_arch = "wasm32")]
                deliver(st, &tenant, &sub, matching, &ctx).await;
            }
            if last {
                break;
            }
        }
        if !sent {
            // 5.8.6: "If there are no matching Entities, no Notification
            // is sent" — lastNotification stays untouched, and a claim
            // taken to win the firing is given back here, or the
            // multi-pod path would stamp an instant nothing was sent at.
            // The firing still happened: the next one is when the next
            // interval is reached, so the subscription parks in the
            // mirror until then instead of re-running its query on every
            // tick (the clock above already points there). A write to it
            // clears the park.
            if let Some(prev) = claim {
                release_interval(st, &tenant, Kind::Subscription, sub_id, prev).await;
            }
            match &st.sub_mirror {
                Some(m) => m.park_quiet(
                    tenant.as_str(),
                    sub_id,
                    &sub,
                    now_ms.saturating_add(interval_offset_ms(interval)),
                ),
                None => next_sub = next_sub.min(due_at),
            }
        }
    }
    // csource timeInterval subs: periodic CSourceNotification with all
    // matching registrations, independent of changes (5.11.7). Skipped by
    // an empty list, not a return: the deliveries spawned above must settle
    // below (a dropped JoinSet aborts them).
    let csubs = if sweep_csubs {
        read_or_warn(
            docs_where(st, &tenant, Kind::CSourceSubscription, |s| {
                s.get("timeInterval").is_some()
            })
            .await,
            "the periodic Context Source Registration Subscriptions",
        )
    } else {
        Vec::new()
    };
    for sub in csubs {
        let Some(interval) = sub.get("timeInterval").and_then(Value::as_f64) else {
            continue;
        };
        if !is_active(&sub) {
            continue;
        }
        let due_at = due_at_ms(&sub, interval);
        if now_ms < due_at {
            next_csub = next_csub.min(due_at);
            continue;
        }
        next_csub = next_csub.min(now_ms.saturating_add(interval_offset_ms(interval)));
        // 5.11.7 sends the periodic CSourceNotification whatever the
        // matching set is, so this claim is never given back.
        if st.nats
            && claim_interval(st, &tenant, Kind::CSourceSubscription, &sub, interval)
                .await
                .is_none()
        {
            continue;
        }
        let ctx = sub_context(st, &tenant, &sub).await;
        let spec = crate::registry::spec_for_subscription(&sub);
        let data: Vec<Value> = read_or_warn(
            docs_where(st, &tenant, Kind::Registration, |r| {
                crate::registry::csr_matches_subscription(&sub, r, &ctx)
            })
            .await,
            "the registrations a periodic Context Source Notification carries",
        )
        .into_iter()
        .map(|r| {
            let mut p =
                crate::registry::present_registration(&filter_csr(&spec, &r, &ctx), &ctx, false);
            arrayify_entity_types(&mut p);
            p
        })
        .collect();
        #[cfg(not(target_arch = "wasm32"))]
        {
            // the Tenant's share and a slot, as for Subscriptions (a
            // bare slot let one Tenant hold them all), taken before the
            // next subscription's registrations are read
            let permits = delivery_permits(&tenant).await;
            let (st, tenant, ctx) = (st.clone(), tenant.clone(), Arc::clone(&ctx));
            sending.spawn(async move {
                let _permits = permits;
                deliver_csource(&st, &tenant, &sub, data, &ctx, "newlyMatching").await;
            });
        }
        #[cfg(target_arch = "wasm32")]
        deliver_csource(st, &tenant, &sub, data, &ctx, "newlyMatching").await;
    }
    #[cfg(not(target_arch = "wasm32"))]
    while let Some(joined) = sending.join_next().await {
        if joined.is_err() {
            note_panic();
        }
    }
    (next_sub, next_csub)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    /// SECV-03: the sweep never takes more than half a small pool, and
    /// keeps 8 on the default one.
    #[test]
    fn the_sweep_width_leaves_the_request_path_half_the_pool() {
        assert_eq!(super::sweep_width_for(None), 8);
        assert_eq!(super::sweep_width_for(Some("20")), 8);
        assert_eq!(super::sweep_width_for(Some("8")), 4);
        assert_eq!(super::sweep_width_for(Some("2")), 1);
        assert_eq!(super::sweep_width_for(Some("1")), 1);
    }
}
