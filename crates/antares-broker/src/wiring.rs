// SPDX-License-Identifier: EUPL-1.2
//! Bus wiring: roles × bus. The composition root is the ONLY
//! place that knows both the bus variant and which consumers exist.
//!
//! bus=local — single process, all roles: the store's change hook feeds the
//! in-process matcher (`antares_api::wire`), temporal recording stays synchronous
//! in the write path. Exactly v0's behaviour; the ETSI pipeline runs this.
//!
//! bus=nats — the scale-out spine:
//!   api role      produces: same-tx outbox rows, the
//!                 outbox drain publishing to `ANTARES_CHANGES` with
//!                 `Nats-Msg-Id` dedup, subscription CUD → KV,
//!                 registration CUD → `ANTARES_REGISTRY`, and the
//!                 per-instance registration mirror its federation path reads.
//!   matcher /     one shared DURABLE ("matcher"): decode → process_change →
//!   notifier      ack AFTER processing; the KV-watched subscription mirror;
//!                 the interval loop (single-winner by row-lock claim).
//!   temporal      no bus consumer — auto-recording is synchronous in the
//!                 write path; the role only carries the
//!                 plain-mode partition job, wired in main.rs.
//!
//! Concurrent drains on N api pods double-publish only within the stream's
//! duplicate window, where `Nats-Msg-Id` = outbox seq absorbs them — that is
//! the design, not an accident (at-least-once, engineered idempotent).

use antares_api::AppState;
use antares_bus::nats::{self, NatsBus};
use antares_bus::ChangeEvent;
use antares_model::TenantId;
use antares_sql::store::pg::doc::DocKind;
use antares_sql::store::pg::seal::{self, SecretKey};
use antares_sql::store::Kind;
use futures_util::StreamExt;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub struct Roles {
    pub api: bool,
    pub matcher: bool,
    pub notifier: bool,
    pub temporal: bool,
    pub registry: bool,
}

impl Roles {
    pub fn parse(spec: &str) -> Result<Self, String> {
        if spec == "all" {
            return Ok(Self {
                api: true,
                matcher: true,
                notifier: true,
                temporal: true,
                registry: true,
            });
        }
        let mut r = Self {
            api: false,
            matcher: false,
            notifier: false,
            temporal: false,
            registry: false,
        };
        for part in spec.split(',') {
            match part.trim() {
                "api" => r.api = true,
                "matcher" => r.matcher = true,
                "notifier" => r.notifier = true,
                "temporal" => r.temporal = true,
                "registry" => r.registry = true,
                other => {
                    return Err(format!(
                        "unknown role {other:?} (api|matcher|notifier|temporal|registry|all)"
                    ))
                }
            }
        }
        Ok(r)
    }

    pub fn all(&self) -> bool {
        self.api && self.matcher && self.notifier && self.temporal && self.registry
    }
}

/// ANTARES_OUTBOX_DRAIN: `on` (the default) or `off`, and nothing else. A
/// typo'd `of` read as "on" under the old permissive parse, quietly defeating
/// the dedicated-drainer split and the crash drill it exists for.
pub fn outbox_drain_enabled() -> Result<bool, String> {
    match std::env::var("ANTARES_OUTBOX_DRAIN") {
        Err(std::env::VarError::NotPresent) => Ok(true),
        Err(e) => Err(format!("ANTARES_OUTBOX_DRAIN is unreadable: {e}")),
        Ok(v) => match v.as_str() {
            "on" => Ok(true),
            "off" => Ok(false),
            other => Err(format!(
                "ANTARES_OUTBOX_DRAIN must be on|off, got {other:?}"
            )),
        },
    }
}

/// KV key for one mirrored subscription: tenant verbatim (token-safe by
/// construction), id hashed (URNs carry `:` — illegal in KV keys). The VALUE
/// carries the real tenant/id, so the key only needs uniqueness. Kind-scoped:
/// a Subscription and a Context Source Registration Subscription may carry the
/// same client-chosen id in one tenant (5.5.10), and sharing a key would let
/// one overwrite the other's mirror entry.
fn kv_key(tenant: &str, kind: Kind, id: &str) -> String {
    format!(
        "{tenant}.{}{:016x}",
        if kind == Kind::CSourceSubscription {
            "c"
        } else {
            ""
        },
        antares_bus::subjects::fnv1a64(id.as_bytes())
    )
}

/// Wire everything bus=nats needs onto the state. Async: connects, hydrates
/// mirrors, creates consumers, asserts topology — all before the broker
/// starts accepting traffic, so a mis-shapen topology is a startup failure,
/// never a silent runtime drift. The bus is handed back for the channels
/// that ride its core request/reply (a relay between pods).
///
/// `secret_key` is `ANTARES_SECRET_KEY`: the subscription KV mirror carries
/// each document's notification credentials sealed under it, as the store's
/// row does ([`seal`]), and the matcher side opens them where it applies an
/// entry.
pub async fn wire_nats(
    state: &mut AppState,
    url: &str,
    roles: Roles,
    secret_key: Option<SecretKey>,
) -> Result<Arc<NatsBus>, Box<dyn std::error::Error>> {
    let secret_key = secret_key.map(Arc::new);
    let bus = Arc::new(NatsBus::connect(url).await?);
    // /q/health `bus` member: live connection state + reconnect count.
    // Installed HERE so bus=local never carries the member at all.
    {
        let b = bus.clone();
        state.bus_stats = Some(Arc::new(move || {
            serde_json::json!({
                "mode": "nats",
                "connected": b.connected(),
                "reconnects": b.reconnects(),
            })
        }));
    }
    // Multi-process mode: interval firings need the single-winner claim
    // — keyed off this flag, not off mirror presence (local mode wires a
    // mirror too).
    state.nats = true;
    // Entity writes now enqueue their events in the write transaction.
    state.store.set_outbox(true);
    // Auto-recording stays SYNCHRONOUS in the write path in every bus mode:
    // every write goes through an api-role pod that has the
    // shared store, so recording in-request gives read-your-writes — the
    // ETSI suite asserts history immediately after a write — and kills the
    // late-replay resurrection race (a consumer re-applying a pre-delete
    // event AFTER a direct temporal delete). The recorder consumer this
    // replaced double-applied by design; it bought nothing but the races.

    // The drain nudge: a same-process write pokes its own drain, so publish
    // latency is ~1 ms, not the idle-poll interval. Cross-pod writes are
    // still covered by each pod's own nudge; the poll below stays as the
    // crash-recovery fallback.
    let nudge = Arc::new(tokio::sync::Notify::new());
    {
        let n = nudge.clone();
        // Auto-recording rides the same synchronous hook here as in bus=local
        // (one choke point for every write, no handler can forget), then nudges
        // the outbox drain.
        let st_rec = state.clone();
        state.store.set_change_hook(Arc::new(
            move |tenant: &TenantId,
                  before: Option<serde_json::Value>,
                  after: Option<serde_json::Value>,
                  _version: antares_store::ChangeVersion|
                  -> antares_store::HookFuture<'_> {
                let st_rec = st_rec.clone();
                let n = n.clone();
                Box::pin(async move {
                    antares_api::notify::record_temporal_change(
                        &st_rec,
                        tenant,
                        before.as_ref(),
                        after.as_ref(),
                    )
                    .await;
                    n.notify_one();
                })
            },
        ));
    }

    let mut durables: Vec<&'static str> = Vec::new();

    if roles.api {
        // Subscription write side: subscription CUD → KV (tombstone = null doc).
        let kv = bus.subs_kv().await?;
        let kv_for_hook = kv.clone();
        let store_for_hook = state.store.clone();
        let key_for_hook = secret_key.clone();
        state.sub_sync = Some(Arc::new(
            move |tenant: &TenantId, kind: Kind, id: &str, _doc| {
                let kv = kv_for_hook.clone();
                let store = store_for_hook.clone();
                let secret = key_for_hook.clone();
                let key = kv_key(tenant.as_str(), kind, id);
                let (tenant, id) = (tenant.clone(), id.to_owned());
                tokio::spawn(async move {
                    // The store row is already committed. A lost put leaves this
                    // subscription invisible to every matcher pod — silently, and
                    // until the next restart, because mirrors hydrate from the
                    // store only at process start. So retry rather than warn once.
                    // Named ceiling: after the last attempt the divergence stands
                    // until a restart; closing that needs periodic reconciliation.
                    //
                    // What is put is the row as the store holds it NOW, under a
                    // compare-and-set on the revision read before the store: each
                    // write's sync is its own task with its own retries, and a
                    // document captured at write time, put late, overwrote a
                    // newer write — in the KV every matcher pod replays after its
                    // hydrate. A sync that loses the race re-reads both, so the
                    // last one to succeed carries the store's last write.
                    for attempt in 0..MIRROR_SYNC_ATTEMPTS {
                        let revision = match kv.entry(key.clone()).await {
                            Ok(e) => e.map_or(0, |e| e.revision),
                            Err(e) => {
                                tracing::warn!("sub KV sync attempt {} failed: {e}", attempt + 1);
                                tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                                continue;
                            }
                        };
                        // A Context Source Registration Subscription's entry
                        // only wakes the sweep (`apply_delta`): it carries no
                        // document, so none is read, sealed or shipped.
                        let read = if kind == Kind::CSourceSubscription {
                            Ok(None)
                        } else {
                            store.get(&tenant, kind, &id).await
                        };
                        let doc = match read {
                            Ok(d) => d,
                            Err(e) => {
                                tracing::warn!("sub KV sync attempt {} failed: {e}", attempt + 1);
                                tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                                continue;
                            }
                        };
                        // The store answered the credentials opened; the
                        // bucket gets them sealed again, as the row holds them.
                        let row = seal::Row {
                            kind: sub_doc_kind(kind == Kind::CSourceSubscription),
                            tenant: tenant.as_str(),
                            id: &id,
                        };
                        let doc = match doc
                            .map(|d| {
                                seal::sealed(secret.as_deref(), row, &d).map(|c| c.into_owned())
                            })
                            .transpose()
                        {
                            Ok(d) => d,
                            Err(_) => {
                                tracing::warn!(
                                    "sub KV sync attempt {} failed: the credentials could \
                                     not be sealed",
                                    attempt + 1
                                );
                                tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                                continue;
                            }
                        };
                        let value = serde_json::json!({
                            "tenant": tenant.as_str(), "id": id, "doc": doc,
                            "csub": kind == Kind::CSourceSubscription,
                        });
                        let bytes = serde_json::to_vec(&value).unwrap_or_default();
                        match kv.update(key.clone(), bytes.into(), revision).await {
                            Ok(_) => return,
                            Err(e) => {
                                tracing::warn!("sub KV sync attempt {} failed: {e}", attempt + 1);
                                tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                            }
                        }
                    }
                    tracing::error!(
                        "sub KV sync gave up for {key} — this subscription is not mirrored \
                     until the next restart"
                    );
                });
            },
        ));

        // An entry put before ANTARES_SECRET_KEY was set still holds its
        // credentials in plaintext, and `sub_sync` rewrites an entry only
        // when its subscription is written again: seal them once, here.
        if let Some(secret) = secret_key.clone() {
            let (kv, store) = (kv.clone(), state.store.clone());
            tokio::spawn(async move {
                let keys: Vec<String> = match kv.keys().await {
                    Ok(keys) => keys.filter_map(|k| async move { k.ok() }).collect().await,
                    Err(e) => {
                        tracing::warn!("sealing the subscription KV: listing failed: {e}");
                        return;
                    }
                };
                seal_kv_entries(&kv, &*store, &secret, keys).await;
            });
        }

        // Registration write side: registration CUD → ANTARES_REGISTRY delta.
        let bus_for_reg = bus.clone();
        let store_for_reg = state.store.clone();
        state.reg_sync = Some(Arc::new(move |tenant: &TenantId, id: &str, _doc| {
            let bus = bus_for_reg.clone();
            let store = store_for_reg.clone();
            let (tenant, id) = (tenant.clone(), id.to_owned());
            tokio::spawn(async move {
                // Same contract as the subscription mirror: the row is
                // committed, so a lost delta makes the registration invisible
                // to every federation path until a restart re-hydrates.
                //
                // And the same ordering fix: what is published is the row as
                // the store holds it NOW, under a compare-and-set on the
                // registration's newest delta read before the store. A
                // document captured at write time, published late, landed
                // after a newer write's in every pod's mirror. A publish that
                // loses the race re-reads both, so the last one to succeed
                // carries the store's last write.
                for attempt in 0..MIRROR_SYNC_ATTEMPTS {
                    let revision = match bus.registration_revision(tenant.as_str(), &id).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                "registry delta publish attempt {} failed: {e}",
                                attempt + 1
                            );
                            tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                            continue;
                        }
                    };
                    let doc = match store.get(&tenant, Kind::Registration, &id).await {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(
                                "registry delta publish attempt {} failed: {e}",
                                attempt + 1
                            );
                            tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                            continue;
                        }
                    };
                    let delta = serde_json::json!({
                        "tenant": tenant.as_str(), "id": id, "doc": doc,
                    });
                    match bus
                        .publish_registration(tenant.as_str(), &id, &delta, revision)
                        .await
                    {
                        Ok(()) => return,
                        Err(e) => {
                            tracing::warn!(
                                "registry delta publish attempt {} failed: {e}",
                                attempt + 1
                            );
                            tokio::time::sleep(mirror_sync_backoff(attempt)).await;
                        }
                    }
                }
                tracing::error!(
                    "registry delta publish gave up for {id} — this registration is not \
                     mirrored until the next restart"
                );
            });
        }));

        // 5.2.34 write side: a cooldown stamp is broadcast to the other api
        // pods on the registry stream (seconds-scale state, deliberately not
        // persisted) — per-process stamps re-dial a failed source from every
        // pod behind the LB.
        let bus_for_cool = bus.clone();
        state.reg_fail_sync = Some(Arc::new(move |reg_id: &str, ok: bool| {
            let bus = bus_for_cool.clone();
            let delta = serde_json::json!({"cooldownReg": reg_id, "ok": ok});
            tokio::spawn(async move {
                if let Err(e) = bus.publish_registry("cooldown", &delta).await {
                    tracing::warn!("cooldown stamp publish failed: {e}");
                }
            });
        }));

        // Registration read side: the ONE compiled registration mirror this instance's
        // federation path reads. Consumer created BEFORE the hydrate so no
        // delta can fall between them; last-writer-wins per key converges.
        let reg_mirror = Arc::new(antares_api::mirror::DocMirror::default());
        let reg_consumer = bus.consume_registry_broadcast().await?;
        // Installed only if it is whole. A mirror that is present and SHORT
        // is read as the truth — `reg_docs` asks it and never the store — so
        // half a hydrate silently drops Context Sources for the life of the
        // process. Left uninstalled, federation matching falls back to the
        // store's own indexed narrowing: correct, and merely slower.
        match antares_api::notify::seed_mirror(
            &*state.store,
            reg_mirror.as_ref(),
            Kind::Registration,
        )
        .await
        {
            Ok(()) => state.reg_mirror = Some(reg_mirror.clone()),
            Err(e) => tracing::error!(
                "registration mirror hydrate failed ({e}); \
                 federation matching falls back to a store read per request"
            ),
        }
        tokio::spawn(watch_registry(
            bus.clone(),
            reg_consumer,
            reg_mirror,
            state.store.clone(),
            state.egress.clone(),
        ));

        // The outbox drain. Runs on every api pod; concurrent drains are
        // absorbed by Nats-Msg-Id dedup within the duplicate window.
        // ANTARES_OUTBOX_DRAIN=off leaves the rows for another pod's drain —
        // the crash-drill lever and the dedicated-drainer split.
        let drain_on = outbox_drain_enabled()?;
        if !drain_on {
            tracing::warn!("outbox drain OFF on this pod (ANTARES_OUTBOX_DRAIN=off)");
        }
        if drain_on {
            let store = state.store.clone();
            let bus_for_drain = bus.clone();
            tokio::spawn(async move {
                loop {
                    let rows = match store.outbox_peek(64).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!("outbox peek failed: {e}");
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                    if rows.is_empty() {
                        // woken by the same-process write hook, or the fallback
                        // poll for rows another pod failed to publish
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_millis(250),
                            nudge.notified(),
                        )
                        .await;
                        continue;
                    }
                    // Ack the EXACT published seqs — a blanket
                    // up-to-max delete loses a lower-seq row whose
                    // transaction commits between peek and ack.
                    let mut acked: Vec<i64> = Vec::new();
                    // Rows whose bodies were too big for the bus: the message
                    // carries a reference, and this row is the only copy of
                    // what it references. Kept, not deleted, and taken out of
                    // the next page by the same stamp.
                    let mut retained: Vec<(TenantId, i64)> = Vec::new();
                    for (seq, _tenant, event) in rows {
                        match serde_json::from_value::<ChangeEvent>(event) {
                            Ok(mut ev) => {
                                ev.seq = seq;
                                let checked = ev.claim_checked_at(antares_bus::CLAIM_CHECK_BYTES);
                                match bus_for_drain.publish(&ev).await {
                                    Ok(()) if checked => retained.push((ev.tenant, seq)),
                                    Ok(()) => acked.push(seq),
                                    Err(e) => {
                                        tracing::warn!("outbox publish of seq {seq} failed: {e}");
                                        break; // retry from here next round
                                    }
                                }
                            }
                            Err(e) => {
                                // an undecodable row would wedge the drain forever
                                tracing::error!("outbox row {seq} undecodable ({e}) — skipped");
                                acked.push(seq);
                            }
                        }
                    }
                    // Retain BEFORE ack: both statements are separate
                    // transactions, and a crash between them must leave a
                    // claim-check row alive rather than published and gone.
                    // One statement per row, under that row's tenant — the
                    // outbox UPDATE takes no service escape (0005), and an
                    // event over the bus ceiling is rare enough that grouping
                    // the page by tenant would cost more code than statements.
                    for (tenant, seq) in &retained {
                        if let Err(e) = store.outbox_retain(tenant, &[*seq]).await {
                            tracing::warn!("outbox retain of seq {seq} failed: {e}");
                        }
                    }
                    if !acked.is_empty() {
                        if let Err(e) = store.outbox_ack(&acked).await {
                            tracing::warn!("outbox ack {acked:?} failed: {e}");
                        }
                    }
                }
            });
        }
    }

    if roles.matcher || roles.notifier {
        // Subscription read side: consumer-before-hydrate, same convergence argument.
        let sub_mirror = Arc::new(antares_api::mirror::SubMirror::default());
        let kv = bus.subs_kv().await?;
        let watch = kv.watch_all().await?;
        let key = secret_key.clone();
        // Same rule as the registration mirror, and the same one `bus=local`
        // applies in `antares_api::wire`: a subscription absent from an installed
        // mirror never fires again, because the matcher reads candidates
        // from the mirror alone.
        match antares_api::notify::seed_mirror(
            &*state.store,
            sub_mirror.as_ref(),
            Kind::Subscription,
        )
        .await
        {
            Ok(()) => state.sub_mirror = Some(sub_mirror.clone()),
            Err(e) => tracing::error!(
                "subscription mirror hydrate failed ({e}); \
                 matching falls back to a store scan per change"
            ),
        }
        tokio::spawn(async move {
            // Same restart contract as the registry consumer: the watch ends
            // on a NATS restart, and a task that returns there stops seeing
            // every subscription change — i.e. this pod silently stops
            // notifying — for the process lifetime. `watch_all` replays the
            // bucket's current values, so re-opening also re-converges the
            // mirror over the gap.
            let mut watch = watch;
            loop {
                while let Some(entry) = watch.next().await {
                    let Ok(entry) = entry else { continue };
                    if let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&entry.value) {
                        open_sub_delta(key.as_deref(), &mut v);
                        apply_delta(sub_mirror.as_ref(), &v);
                    }
                }
                tracing::warn!("subscription KV watch ended — reopening");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                match kv.watch_all().await {
                    Ok(w) => watch = w,
                    Err(e) => tracing::warn!("subscription KV watch reopen failed: {e}"),
                }
            }
        });

        // The balanced matcher durable: decode → process_change → ack AFTER,
        // with progress heartbeats for every message this pod holds while
        // one of them is processed (a slow endpoint must not let ack_wait
        // pass and have the server hand them all out a second time).
        durables.push("matcher");
        let consumer = bus.consume_balanced("matcher").await?;
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let run = nats::run_balanced(&consumer, |msg| {
                    // Change lag = stream-publish → matcher-processing
                    // age, from the JetStream metadata timestamp.
                    if let Ok(info) = msg.info() {
                        note_final_delivery(
                            msg.subject.as_str(),
                            info.stream_sequence,
                            info.delivered,
                        );
                        // OffsetDateTime → SystemTime (impl in `time`/std),
                        // so no direct `time` dependency here.
                        let published: std::time::SystemTime = info.published.into();
                        if let Ok(age) = published.elapsed() {
                            metrics::histogram!("antares_change_lag_seconds")
                                .record(age.as_secs_f64());
                        }
                    }
                    let ev = nats::decode(msg);
                    let st = st.clone();
                    async move {
                        if let Some(ev) = ev {
                            let (before, after) = resolve_payloads(&st, &ev).await;
                            antares_api::notify::process_change(
                                &st,
                                ev.tenant.as_str(),
                                before,
                                after,
                            )
                            .await;
                        }
                    }
                })
                .await;
                if let Err(e) = run {
                    tracing::warn!("matcher consumer stream failed: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        // Interval subscriptions: every matcher pod ticks; the row-lock claim
        // in interval_tick makes each firing single-winner.
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                tick.tick().await;
                antares_api::notify::interval_tick(&st).await;
            }
        });
    }

    // The temporal role carries no bus consumer: auto-recording is
    // synchronous in the write path (see above), and plain-mode partition
    // maintenance runs from main.rs regardless of bus mode.

    // The server must agree these are shared durables.
    bus.assert_topology(&durables).await?;
    tracing::info!(?roles, "bus=nats wired");
    Ok(bus)
}

/// Whether a change message is on its last delivery: the matcher durable
/// gives a message up after `nats::MAX_DELIVER` deliveries, so if this
/// attempt does not finish the server drops the change and the
/// notifications it should produce. Logged at error level (subject and
/// stream sequence, never the payload) and counted, before the work, so such
/// a loss is never silent.
fn note_final_delivery(subject: &str, stream_seq: u64, delivered: i64) -> bool {
    if delivered < nats::MAX_DELIVER {
        return false;
    }
    tracing::error!(
        subject,
        stream_seq,
        delivered,
        "change message on its final delivery: if this attempt fails, the server drops it"
    );
    metrics::counter!("antares_change_final_delivery_total").increment(1);
    true
}

/// The registration mirror's read side (bus=nats, api role): every
/// registry delta into `mirror`, every 5.2.34 cooldown stamp into `egress`.
async fn watch_registry(
    bus: Arc<NatsBus>,
    reg_consumer: nats::PullConsumer,
    reg_mirror: Arc<antares_api::mirror::DocMirror>,
    store: Arc<dyn antares_store::CurrentStateDriver>,
    egress: Arc<antares_api::egress::Egress>,
) {
    // The consumer is ephemeral, so a NATS restart or an inactivity
    // gap deletes it server-side and the next pull errors. Ending the
    // task there froze this pod's registration mirror — and with it
    // all federation matching — for the process lifetime, while
    // /q/health still reported the bus connected. Re-open instead,
    // and re-hydrate from the store because a fresh consumer starts
    // at NEW and never replays what the gap dropped.
    // The gap ends on a NEW consumer: the handle of a deleted one pulls
    // nothing ever again.
    let mut reg_consumer = reg_consumer;
    loop {
        match reg_consumer.messages().await {
            Ok(mut msgs) => {
                while let Some(delta) = nats::next_delta(&mut msgs).await {
                    // 5.2.34 read side: a cooldown stamp updates this pod's
                    // map (a marker delta has no tenant/id — apply_delta
                    // ignores it on pods that predate the member).
                    if let Some(rid) = delta.get("cooldownReg").and_then(serde_json::Value::as_str)
                    {
                        egress.reg_record(rid, delta["ok"].as_bool().unwrap_or(false));
                        continue;
                    }
                    apply_delta(reg_mirror.as_ref(), &delta);
                }
                tracing::warn!("registry broadcast consumer stream ended — reopening");
            }
            Err(e) => tracing::warn!("registry broadcast consumer stream failed: {e}"),
        }
        // Consumer before re-hydrate, the startup order: no delta can
        // fall between the two.
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            match bus.consume_registry_broadcast().await {
                Ok(c) => {
                    reg_consumer = c;
                    break;
                }
                Err(e) => tracing::warn!("registry broadcast consumer reopen failed: {e}"),
            }
        }
        // Already installed, so this one cannot be withheld. A failed
        // re-hydrate leaves the mirror holding what it had before the
        // gap, which the federation path will serve as current: say
        // so at error level rather than let it pass as a warning.
        if let Err(e) = rehydrate_registrations(&*store, &reg_mirror).await {
            tracing::error!(
                "registration mirror re-hydrate failed ({e}); \
                     this pod is matching against registrations from before the gap"
            );
        }
    }
}

/// Seal the subscription KV entries named by `keys` that still hold a
/// plaintext credential, from the store's row and under a compare-and-set
/// on the entry read: a sync that wrote meanwhile put its own sealed entry,
/// and wins. An entry whose subscription this store does not hold is left to
/// the next write's sync. Answers how many entries it sealed.
///
/// ponytail: one pass at boot, sequential, one entry read per key of the
/// bucket (one key per subscription).
async fn seal_kv_entries(
    kv: &nats::KvStore,
    store: &dyn antares_store::CurrentStateDriver,
    secret: &SecretKey,
    keys: Vec<String>,
) -> usize {
    let mut sealed = 0;
    for key in keys {
        let Ok(Some(entry)) = kv.entry(key.clone()).await else {
            continue;
        };
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&entry.value) else {
            continue;
        };
        let csub = v.get("csub").and_then(serde_json::Value::as_bool) == Some(true);
        let (Some(tenant), Some(id), Some(doc)) = (
            v.get("tenant").and_then(serde_json::Value::as_str),
            v.get("id").and_then(serde_json::Value::as_str),
            v.get("doc").filter(|d| !d.is_null()),
        ) else {
            continue;
        };
        if !seal::holds_plaintext(sub_doc_kind(csub), doc) {
            continue;
        }
        let Ok(tenant_id) = TenantId::new_internal(tenant) else {
            continue;
        };
        // a Context Source Registration Subscription's entry needs no document
        let doc = if csub {
            None
        } else {
            match store.get(&tenant_id, Kind::Subscription, id).await {
                Ok(Some(d)) => {
                    let row = seal::Row {
                        kind: sub_doc_kind(false),
                        tenant,
                        id,
                    };
                    match seal::sealed(Some(secret), row, &d) {
                        Ok(d) => Some(d.into_owned()),
                        Err(_) => continue,
                    }
                }
                _ => continue,
            }
        };
        let value = serde_json::json!({"tenant": tenant, "id": id, "doc": doc, "csub": csub});
        let bytes = serde_json::to_vec(&value).unwrap_or_default();
        match kv.update(key.clone(), bytes.into(), entry.revision).await {
            Ok(_) => sealed += 1,
            Err(e) => tracing::warn!("sealing the subscription KV entry {key} failed: {e}"),
        }
    }
    sealed
}

/// Re-seed the registration mirror after a gap in its deltas. The gap may
/// have swallowed deletions too, so the store's set is seeded into a fresh
/// mirror and replaces the old contents whole; seeding over the old mirror
/// kept every registration deleted during the gap, and federation kept
/// forwarding to it. On an error the old contents stay.
async fn rehydrate_registrations(
    store: &dyn antares_store::CurrentStateDriver,
    reg_mirror: &antares_api::mirror::DocMirror,
) -> Result<(), antares_model::NgsiError> {
    let fresh = antares_api::mirror::DocMirror::default();
    antares_api::notify::seed_mirror(store, &fresh, Kind::Registration).await?;
    reg_mirror.replace_with(fresh);
    Ok(())
}

/// Hydrate a mirror from the system of record (Postgres) at startup.
/// Attempts a mirror write gets before the divergence is logged as an error.
const MIRROR_SYNC_ATTEMPTS: u32 = 5;

/// Exponential backoff between mirror-sync attempts: 0.2 s doubling to 3.2 s,
/// so the whole ladder outlives a bus reconnect without holding a task for
/// minutes.
fn mirror_sync_backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(200u64 << attempt.min(4))
}

/// Apply one `{tenant, id, doc|null, csub}` delta to a mirror. A Context
/// Source Registration Subscription carries no document into the mirror: it
/// is matched against registrations rather than entities, so the delta only
/// wakes the interval sweep (5.11.7).
fn apply_delta(mirror: &dyn antares_api::mirror::Mirror, delta: &serde_json::Value) {
    if delta.get("csub").and_then(serde_json::Value::as_bool) == Some(true) {
        mirror.csub_written();
        return;
    }
    let (Some(tenant), Some(id)) = (
        delta.get("tenant").and_then(serde_json::Value::as_str),
        delta.get("id").and_then(serde_json::Value::as_str),
    ) else {
        return;
    };
    // Hydration validates the tenant before it touches the mirror; the delta
    // path must agree. A name outside the grammar can only add entries that no
    // lookup will ever hit, so an unvalidated one is unbounded growth keyed by
    // whatever reached the bus. The grammar is the whole check here: what
    // arrives is a tenant a peer broker wrote, and a write inside a Snapshot
    // (5.5.15) carries the synthetic tenant a client may not name — refusing
    // that one would drop the mirror delta the snapshot-scoped subscription
    // matches on.
    if TenantId::new_internal(tenant).is_err() {
        return;
    }
    let doc = delta.get("doc").filter(|d| !d.is_null()).cloned();
    mirror.apply(tenant, id, doc);
}

/// The doc kind a subscription KV entry's credentials are sealed for.
fn sub_doc_kind(csub: bool) -> DocKind {
    if csub {
        DocKind::CSourceSubscription
    } else {
        DocKind::Subscription
    }
}

/// Open the credentials of one subscription KV entry in place, before
/// [`apply_delta`]. An entry this pod cannot open (no key, another key, a
/// damaged value) becomes a tombstone: the subscription leaves this pod's
/// mirror rather than notify with ciphertext in its headers or its URI.
/// A plaintext entry (one put before the key was set) opens as it is.
fn open_sub_delta(key: Option<&SecretKey>, delta: &mut serde_json::Value) {
    let csub = delta.get("csub").and_then(serde_json::Value::as_bool) == Some(true);
    let (Some(tenant), Some(id)) = (
        delta.get("tenant").and_then(serde_json::Value::as_str),
        delta.get("id").and_then(serde_json::Value::as_str),
    ) else {
        return;
    };
    let (tenant, id) = (tenant.to_owned(), id.to_owned());
    let Some(doc) = delta.get_mut("doc").filter(|d| !d.is_null()) else {
        return;
    };
    let row = seal::Row {
        kind: sub_doc_kind(csub),
        tenant: &tenant,
        id: &id,
    };
    if seal::open(key, row, doc).is_err() {
        tracing::error!(
            %tenant, %id,
            "a mirrored subscription's notification credentials cannot be opened: \
             ANTARES_SECRET_KEY is unset or differs from the key it was sealed with; \
             this pod drops the subscription from its mirror"
        );
        *doc = serde_json::Value::Null;
    }
}

/// Resolve claim-check references: bodies the bus could not carry come back
/// from the outbox row the drain kept, read by the event's own `seq`.
///
/// The store's current row is NOT that source. It answers with the entity as
/// it stands now, which is the after-image: resolving `prev_payload_ref` from
/// it hands the matcher two copies of the same document, `diff` finds nothing
/// changed and the change reaches no subscriber. It stays the fallback for
/// `payload_ref` alone, where being newer than the referenced version is the
/// ordinary at-least-once reality the matcher already tolerates.
async fn resolve_payloads(
    st: &AppState,
    ev: &ChangeEvent,
) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    if ev.payload_ref.is_none() && ev.prev_payload_ref.is_none() {
        return (ev.prev_payload.clone(), ev.payload.clone());
    }
    // 0 is the local bus, which never claim-checks: it hands the payloads to
    // the matcher in process.
    let kept = match ev.seq {
        0 => None,
        seq => st
            .store
            .outbox_event(seq, &ev.tenant)
            .await
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_value::<ChangeEvent>(v).ok()),
    };
    if let Some(kept) = kept {
        return (kept.prev_payload, kept.payload);
    }
    // Past the retention window, or a deployment whose store keeps no outbox.
    // The after-image is still recoverable; the before-image is not, and a
    // guess in its place is a notification that reports a change that did not
    // happen.
    let after = match ev.payload.clone() {
        Some(v) => Some(v),
        None => match ev.payload_ref.as_ref() {
            Some(r) => st
                .store
                .get(&ev.tenant, Kind::Entity, r.entity_id.as_str())
                .await
                .ok()
                .flatten(),
            None => None,
        },
    };
    if ev.prev_payload_ref.is_some() {
        metrics::counter!("antares_claim_check_unresolved_total").increment(1);
        tracing::warn!(
            "claim-check row for seq {} is gone: the change to {} notifies nobody",
            ev.seq,
            ev.entity_id.as_str()
        );
    }
    (ev.prev_payload.clone(), after)
}

#[cfg(test)]
mod tests {
    use super::*;
    use antares_api::mirror::DocMirror;
    use antares_bus::{ChangeOp, PayloadRef};
    use antares_model::EntityId;
    use serde_json::json;

    /// The matcher durable drops a change after MAX_DELIVER deliveries: the
    /// last one is reported, the ones before it are not.
    #[test]
    fn only_the_final_delivery_of_a_change_is_reported() {
        assert!(!note_final_delivery("antares.changes.t", 7, 1));
        assert!(!note_final_delivery(
            "antares.changes.t",
            7,
            nats::MAX_DELIVER - 1
        ));
        assert!(note_final_delivery(
            "antares.changes.t",
            7,
            nats::MAX_DELIVER
        ));
    }

    #[test]
    fn roles_parse_accepts_the_role_set_and_refuses_anything_else() {
        assert!(Roles::parse("all").expect("all").all());
        let r = Roles::parse("api").expect("api");
        assert!(r.api && !r.matcher && !r.notifier && !r.temporal && !r.registry);
        assert!(
            !r.all(),
            "a single role must never claim to be the full set"
        );
        let r = Roles::parse(" matcher , notifier ").expect("padded list");
        assert!(r.matcher && r.notifier && !r.api);
        // The enumerated full set is the same thing as "all" — a role split
        // that happens to name every role must still pass the bus=local gate.
        assert!(Roles::parse("api,matcher,notifier,temporal,registry")
            .expect("full list")
            .all());

        for bad in ["", "api,", "ALL", "apis", "api;matcher", "worker", " "] {
            let err = Roles::parse(bad).expect_err(&format!("ANTARES_ROLES={bad:?} must be fatal"));
            assert!(err.starts_with("unknown role"), "{bad:?}: {err}");
        }
    }

    /// The KV key must be legal for a NATS KV bucket (`:` from a URN is not),
    /// stable across calls, and collision-free per id, per tenant and per
    /// kind — 5.5.10 leaves the id to the client, so one URN can name both a
    /// Subscription and a Context Source Registration Subscription.
    #[test]
    fn kv_key_is_bucket_legal_and_stable() {
        let sub = Kind::Subscription;
        let k = kv_key("default", sub, "urn:ngsi-ld:Subscription:1");
        assert_eq!(
            k,
            kv_key("default", sub, "urn:ngsi-ld:Subscription:1"),
            "stable"
        );
        assert!(
            k.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-'),
            "illegal KV key character in {k:?}"
        );
        assert!(k.starts_with("default."), "tenant scoping lost: {k}");
        assert_ne!(
            kv_key("default", sub, "urn:ngsi-ld:Subscription:1"),
            kv_key("default", sub, "urn:ngsi-ld:Subscription:2")
        );
        assert_ne!(
            kv_key("t1", sub, "urn:ngsi-ld:Subscription:1"),
            kv_key("t2", sub, "urn:ngsi-ld:Subscription:1")
        );
        assert_ne!(
            kv_key("default", sub, "urn:ngsi-ld:Subscription:1"),
            kv_key(
                "default",
                Kind::CSourceSubscription,
                "urn:ngsi-ld:Subscription:1"
            ),
            "one id naming both kinds must not share a mirror entry"
        );
    }

    /// A Context Source Registration Subscription delta carries no document:
    /// it wakes the interval sweep and touches nothing the matcher reads.
    #[test]
    fn a_csub_delta_only_wakes_the_sweep() {
        let m = antares_api::mirror::SubMirror::default();
        apply_delta(
            &m,
            &serde_json::json!({
                "tenant": "default", "id": "urn:ngsi-ld:CSourceSubscription:1",
                "doc": {"id": "urn:ngsi-ld:CSourceSubscription:1", "timeInterval": 5},
                "csub": true,
            }),
        );
        assert!(
            m.docs("default").is_empty(),
            "a csource subscription must not enter the candidate index"
        );
    }

    /// The KV/registry mirrors are fed from the bus. A malformed or hostile
    /// delta must be dropped — never panic a consumer task, and never grow
    /// the mirror under a key no request can ever address (the tenant is
    /// validated on every request, so an unvalidated one is pure ballast).
    #[test]
    fn apply_delta_ignores_malformed_and_hostile_deltas() {
        let m = DocMirror::default();
        apply_delta(
            &m,
            &json!({"tenant": "default", "id": "urn:x:1", "doc": {"id": "urn:x:1"}}),
        );
        assert_eq!(m.docs("default").len(), 1, "a good delta must apply");

        // tombstone
        apply_delta(
            &m,
            &json!({"tenant": "default", "id": "urn:x:1", "doc": null}),
        );
        assert!(m.docs("default").is_empty(), "null doc must delete");
        assert!(m.tenants().is_empty(), "an emptied tenant must not linger");

        // Shapes that must be ignored without panicking.
        for junk in [
            json!({}),
            json!(null),
            json!(42),
            json!("scalar"),
            json!([1, 2, 3]),
            json!({"tenant": "default"}),
            json!({"id": "urn:x:1"}),
            json!({"tenant": 7, "id": "urn:x:1", "doc": {}}),
            json!({"tenant": "default", "id": null, "doc": {}}),
            json!({"cooldownReg": "urn:reg:1", "ok": false}),
        ] {
            apply_delta(&m, &junk);
        }
        assert!(m.tenants().is_empty(), "junk deltas grew the mirror");

        // Hostile tenants: not addressable by any request (the header is
        // validated to [A-Za-z0-9_-]{1,64}), so they may not take memory.
        for hostile in [
            "a".repeat(4096),
            "../../etc".into(),
            "a b".into(),
            "".into(),
        ] {
            apply_delta(
                &m,
                &json!({"tenant": hostile, "id": "urn:x:1", "doc": {"id": "urn:x:1"}}),
            );
        }
        assert!(
            m.tenants().is_empty(),
            "an unaddressable tenant grew the mirror without bound: {:?}",
            m.tenants()
        );
    }

    async fn state_with(entity: Option<serde_json::Value>) -> AppState {
        let st = AppState::new("antares".into());
        if let Some(doc) = entity {
            let id = doc["id"].as_str().expect("id").to_owned();
            st.store
                .create(&TenantId::default(), Kind::Entity, &id, doc)
                .await
                .expect("seed");
        }
        st
    }

    fn event(payload: Option<serde_json::Value>, r#ref: Option<PayloadRef>) -> ChangeEvent {
        ChangeEvent {
            tenant: TenantId::default(),
            entity_id: EntityId::new("urn:ngsi-ld:T:1").expect("id"),
            types: vec!["T".into()],
            op: ChangeOp::Update,
            changed_attrs: vec![],
            payload,
            prev_payload: None,
            version: 1,
            incarnation: String::new(),
            seq: 0,
            payload_ref: r#ref,
            prev_payload_ref: None,
        }
    }

    /// Claim-check resolution: inline wins, a reference is fetched, and a
    /// reference to a row that is gone resolves to None instead of panicking
    /// the matcher task.
    #[tokio::test]
    async fn resolve_payloads_prefers_inline_and_tolerates_a_dangling_reference() {
        let doc = json!({"id": "urn:ngsi-ld:T:1", "type": "T"});
        let st = state_with(Some(doc.clone())).await;

        let (before, after) =
            resolve_payloads(&st, &event(Some(json!({"inline": true})), None)).await;
        assert_eq!(after, Some(json!({"inline": true})), "inline payload wins");
        assert_eq!(before, None);

        let r = PayloadRef {
            entity_id: EntityId::new("urn:ngsi-ld:T:1").expect("id"),
            version: 1,
        };
        let (_, after) = resolve_payloads(&st, &event(None, Some(r.clone()))).await;
        assert_eq!(
            after.as_ref().and_then(|a| a["id"].as_str()),
            Some("urn:ngsi-ld:T:1"),
            "a claim-check reference must be fetched from the store"
        );

        // The row was deleted between publish and consumption.
        let gone = state_with(None).await;
        let (before, after) = resolve_payloads(&gone, &event(None, Some(r))).await;
        assert_eq!(after, None, "a dangling reference must resolve to None");
        assert_eq!(before, None);

        let (before, after) = resolve_payloads(&st, &event(None, None)).await;
        assert!(before.is_none() && after.is_none(), "no payload, no fetch");
    }

    /// A claim-check fetch is scoped to the EVENT's tenant: a reference must
    /// never resolve against another tenant's row of the same id.
    #[tokio::test]
    async fn resolve_payloads_never_crosses_a_tenant_boundary() {
        let st = state_with(Some(json!({"id": "urn:ngsi-ld:T:1", "type": "T"}))).await;
        let mut ev = event(
            None,
            Some(PayloadRef {
                entity_id: EntityId::new("urn:ngsi-ld:T:1").expect("id"),
                version: 1,
            }),
        );
        ev.tenant = TenantId::new("other").expect("tenant");
        let (_, after) = resolve_payloads(&st, &ev).await;
        assert_eq!(
            after, None,
            "a reference resolved another tenant's entity: {after:?}"
        );
    }

    /// A change whose bodies were both too big for the bus reaches the
    /// matcher with the before-image the write actually replaced. Resolving
    /// the reference against the store's current row instead hands back the
    /// after-image twice, `diff` finds nothing and the change notifies
    /// nobody. Skips without ANTARES_TEST_DATABASE_URL — the outbox is a
    /// Postgres table, and the memory arm has none.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_retained_row_gives_the_matcher_the_before_image_the_write_replaced() {
        let Ok(url) = std::env::var("ANTARES_TEST_DATABASE_URL") else {
            eprintln!("SKIP: ANTARES_TEST_DATABASE_URL not set");
            return;
        };
        let pool = antares_sql::store::pg::connect(&url, 5)
            .await
            .expect("connect");
        let tenant = TenantId::new("claimcheck").expect("tenant");
        antares_sql::store::pg::ensure_tenant(&pool, &tenant)
            .await
            .expect("tenant row");

        let id = "urn:ngsi-ld:T:oversized";
        let wide = |v: &str| {
            json!({"id": id, "type": ["T"],
                   "https://uri.etsi.org/ngsi-ld/default-context/note":
                       [{"type": "Property", "value": format!("{v}{}", "x".repeat(300 * 1024))}]})
        };
        let before_doc = wide("before-");
        let after_doc = wide("after-");

        let mut ev = ChangeEvent {
            tenant: tenant.clone(),
            entity_id: EntityId::new(id).expect("id"),
            types: vec!["T".into()],
            op: ChangeOp::Update,
            changed_attrs: vec![],
            payload: Some(after_doc.clone()),
            prev_payload: Some(before_doc.clone()),
            version: 2,
            incarnation: String::new(),
            seq: 0,
            payload_ref: None,
            prev_payload_ref: None,
        };
        let mut tx = pool.begin().await.expect("tx");
        antares_sql::store::pg::set_tenant(&mut tx, &tenant)
            .await
            .expect("set tenant");
        let seq = antares_sql::store::pg::outbox::enqueue(
            &mut tx,
            &tenant,
            &serde_json::to_value(&ev).expect("event json"),
        )
        .await
        .expect("enqueue");
        tx.commit().await.expect("commit");
        antares_sql::store::pg::outbox::retain(&pool, &tenant, &[seq])
            .await
            .expect("retain");

        let st = AppState::with_store(
            "antares".into(),
            std::sync::Arc::new(antares_sql::store::any::AnyStore::Pg(
                antares_sql::store::any::PgBackend::new(pool.clone()),
            )),
            "postgres",
        );
        // The current row is the AFTER image — the document the old
        // resolution handed back for both halves.
        let _ = st.store.delete(&tenant, Kind::Entity, id).await;
        st.store
            .create(&tenant, Kind::Entity, id, after_doc.clone())
            .await
            .expect("seed current row");

        ev.seq = seq;
        let wire = ev.claim_check(antares_bus::CLAIM_CHECK_BYTES);
        assert!(
            wire.prev_payload_ref.is_some() && wire.payload_ref.is_some(),
            "the fixture must be over the claim-check ceiling"
        );
        let (before, after) = resolve_payloads(&st, &wire).await;
        assert_eq!(before.as_ref(), Some(&before_doc), "before-image lost");
        assert_eq!(after.as_ref(), Some(&after_doc));
        assert_ne!(
            before, after,
            "both halves resolved to the current row: the change notifies nobody"
        );

        let _ = st.store.delete(&tenant, Kind::Entity, id).await;
        let _ = antares_sql::store::pg::outbox::reap_published(&pool, 0).await;
    }

    /// The outbox-drain switch is a config value like any other: the two
    /// documented spellings decide, anything else is fatal instead of
    /// silently leaving the drain on (a typo'd `ANTARES_OUTBOX_DRAIN=of`
    /// would otherwise read as "on" and quietly defeat the crash drill).
    #[test]
    fn outbox_drain_switch_is_total() {
        std::env::remove_var("ANTARES_OUTBOX_DRAIN");
        assert!(outbox_drain_enabled().expect("default"), "default is on");
        std::env::set_var("ANTARES_OUTBOX_DRAIN", "off");
        assert!(!outbox_drain_enabled().expect("off"));
        std::env::set_var("ANTARES_OUTBOX_DRAIN", "on");
        assert!(outbox_drain_enabled().expect("on"));
        for bad in ["", "of", "false", "0", "OFF", "no"] {
            std::env::set_var("ANTARES_OUTBOX_DRAIN", bad);
            let err = outbox_drain_enabled()
                .expect_err(&format!("ANTARES_OUTBOX_DRAIN={bad:?} must be fatal"));
            assert!(err.contains("ANTARES_OUTBOX_DRAIN"), "{err}");
        }
        std::env::remove_var("ANTARES_OUTBOX_DRAIN");
    }

    /// Live NATS for the mirror-sync tests; they skip without it.
    fn nats_url() -> Option<String> {
        let url = std::env::var("ANTARES_TEST_NATS_URL").ok();
        if url.is_none() {
            eprintln!("SKIP: ANTARES_TEST_NATS_URL not set");
        }
        url
    }

    async fn wait_until<F: std::future::Future<Output = bool>>(
        what: &str,
        mut f: impl FnMut() -> F,
    ) {
        let rounds = 100 * antares_api::state::slow_factor();
        for _ in 0..rounds {
            if f().await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// The registry broadcast consumer is ephemeral: a NATS restart or an
    /// inactivity gap deletes it on the server. The watcher has to come back
    /// on a NEW consumer; pulling again on the deleted one never delivers,
    /// so every later delta (a deleted registration included) is lost to
    /// this pod's mirror for the life of the process.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_registry_watcher_survives_its_consumer_being_deleted() {
        let Some(url) = nats_url() else { return };
        let bus = Arc::new(NatsBus::connect(&url).await.expect("connect"));
        let st = AppState::new("antares".into());
        let mirror = Arc::new(DocMirror::default());
        let consumer = bus.consume_registry_broadcast().await.expect("consumer");
        let name = consumer.cached_info().name.clone();
        let watcher = tokio::spawn(watch_registry(
            bus.clone(),
            consumer,
            mirror.clone(),
            st.store.clone(),
            st.egress.clone(),
        ));
        let tenant = format!("regwatch{}", std::process::id());
        let delta = |id: &str| json!({"tenant": tenant, "id": id, "doc": {"id": id, "type": "ContextSourceRegistration"}});
        let held = |id: &'static str| {
            let (m, t) = (mirror.clone(), tenant.clone());
            move || {
                let (m, t) = (m.clone(), t.clone());
                async move { m.docs(&t).iter().any(|d| d["id"] == id) }
            }
        };
        bus.publish_registry(&tenant, &delta("urn:reg:before"))
            .await
            .expect("publish");
        wait_until("the delta before the gap", held("urn:reg:before")).await;

        bus.delete_registry_consumer(&name).await.expect("delete");
        // the delta keeps being published until a consumer exists to see it:
        // a fresh consumer starts at NEW
        let deadline = std::time::Instant::now()
            + std::time::Duration::from_secs(20 * antares_api::state::slow_factor());
        loop {
            bus.publish_registry(&tenant, &delta("urn:reg:after"))
                .await
                .expect("publish");
            if held("urn:reg:after")().await {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no delta reached the mirror after its consumer was deleted"
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        watcher.abort();
    }

    /// A gap in the registry deltas can swallow a deletion: the re-hydrate
    /// that ends it must drop the registration as well as add the ones it
    /// missed, or federation keeps forwarding to a deleted Context Source.
    #[tokio::test]
    async fn a_rehydrate_drops_a_registration_deleted_during_the_gap() {
        let st = AppState::new("antares".into());
        let tenant = antares_model::TenantId::new("rehydrategap").expect("tenant");
        let reg = |id: &str| {
            json!({"id": id, "type": "ContextSourceRegistration",
                   "information": [{"entities": [{"type": "Vehicle"}]}],
                   "endpoint": "http://127.0.0.1:9"})
        };
        st.store
            .create(
                &tenant,
                Kind::Registration,
                "urn:reg:kept",
                reg("urn:reg:kept"),
            )
            .await
            .expect("kept");
        let mirror = DocMirror::default();
        mirror.apply(tenant.as_str(), "urn:reg:kept", Some(reg("urn:reg:kept")));
        // deleted from the store while no delta reached this pod
        mirror.apply(tenant.as_str(), "urn:reg:gone", Some(reg("urn:reg:gone")));
        rehydrate_registrations(&*st.store, &mirror)
            .await
            .expect("rehydrate");
        let ids: Vec<serde_json::Value> = mirror
            .docs(tenant.as_str())
            .iter()
            .map(|d| d["id"].clone())
            .collect();
        assert_eq!(ids, vec![json!("urn:reg:kept")]);
    }

    /// The KV holds what the store holds. Each write's sync runs as its own
    /// task with its own retries, so one that lands late (a retried put, or
    /// a task scheduled after a newer write's) used to overwrite the newer
    /// value with the document it captured — and the KV is what every
    /// matcher pod replays, after its store hydrate, on every (re)start.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_late_subscription_sync_never_overwrites_a_newer_write() {
        use antares_store::CurrentStateDriverExt;
        let Some(url) = nats_url() else { return };
        let mut st = AppState::new("antares".into());
        let _bus = wire_nats(&mut st, &url, Roles::parse("api").expect("roles"), None)
            .await
            .expect("wire");
        let tenant = TenantId::default();
        let id = format!("urn:ngsi-ld:Subscription:late{}", std::process::id());
        let doc = |v: i64| {
            json!({"id": id, "type": "Subscription", "v": v,
            "entities": [{"type": "https://x/T"}]})
        };
        st.store
            .create(&tenant, Kind::Subscription, &id, doc(1))
            .await
            .expect("create");
        st.store
            .mutate::<(), ()>(&tenant, Kind::Subscription, &id, |d| {
                *d = doc(2);
                Ok(())
            })
            .await
            .expect("update");
        let sync = st.sub_sync.clone().expect("api role installs the sync");
        let kv = NatsBus::connect(&url)
            .await
            .expect("connect")
            .subs_kv()
            .await
            .expect("kv");
        let key = kv_key(tenant.as_str(), Kind::Subscription, &id);
        // the subscription's `v` as the KV holds it
        macro_rules! kv_v {
            () => {{
                let (kv, key) = (kv.clone(), key.clone());
                async move {
                    kv.get(key)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                        .and_then(|v| v["doc"]["v"].as_i64())
                }
            }};
        }
        // the newer write's sync lands first ...
        sync(&tenant, Kind::Subscription, &id, Some(&doc(2)));
        wait_until("the newer write in the KV", || {
            let v = kv_v!();
            async move { v.await == Some(2) }
        })
        .await;
        // ... and the older write's sync after it
        sync(&tenant, Kind::Subscription, &id, Some(&doc(1)));
        tokio::time::sleep(std::time::Duration::from_millis(
            1_000 * antares_api::state::slow_factor(),
        ))
        .await;
        assert_eq!(
            kv_v!().await,
            Some(2),
            "the KV went back to the document an older write captured"
        );
    }

    /// 32 zero bytes in base64: a test key that is plainly not a secret.
    const TEST_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    /// With ANTARES_SECRET_KEY the subscription KV mirror holds what the
    /// store's row holds: the receiverInfo values and a URI with userinfo
    /// sealed, never the plaintext. The matcher side opens them into its
    /// mirror; a pod without the key, or with another, drops the entry
    /// instead of mirroring ciphertext it would send as headers.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_subscription_kv_holds_the_credentials_sealed() {
        let Some(url) = nats_url() else { return };
        let mut st = AppState::new("antares".into());
        let _bus = wire_nats(
            &mut st,
            &url,
            Roles::parse("api").expect("roles"),
            Some(SecretKey::from_base64(TEST_KEY).expect("key")),
        )
        .await
        .expect("wire");
        let tenant = TenantId::default();
        let id = format!("urn:ngsi-ld:Subscription:sealedkv{}", std::process::id());
        let doc = json!({"id": id, "type": "Subscription",
            "entities": [{"type": "https://x/T"}],
            "notification": {"endpoint": {
                "uri": "http://hook:pa55word@localhost:1/n",
                "receiverInfo": [{"key": "Authorization", "value": "Bearer kv-token"}]}}});
        st.store
            .create(&tenant, Kind::Subscription, &id, doc.clone())
            .await
            .expect("create");
        let kv = NatsBus::connect(&url)
            .await
            .expect("connect")
            .subs_kv()
            .await
            .expect("kv");
        let key = kv_key(tenant.as_str(), Kind::Subscription, &id);
        let sync = st.sub_sync.clone().expect("api role installs the sync");
        sync(&tenant, Kind::Subscription, &id, Some(&doc));
        wait_until("the subscription in the KV", || {
            let (kv, key) = (kv.clone(), key.clone());
            async move { kv.get(key).await.ok().flatten().is_some() }
        })
        .await;
        let raw = kv.get(key.clone()).await.expect("get").expect("entry");
        let text = String::from_utf8_lossy(&raw).into_owned();
        assert!(
            !text.contains("kv-token") && !text.contains("pa55word"),
            "plaintext credential in the KV: {text}"
        );
        assert!(text.contains(seal::PREFIX), "{text}");

        let entry: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        let mirrored = |key: Option<&SecretKey>| {
            let m = antares_api::mirror::SubMirror::default();
            let mut v = entry.clone();
            open_sub_delta(key, &mut v);
            apply_delta(&m, &v);
            m.docs(tenant.as_str())
                .into_iter()
                .map(|d| (*d).clone())
                .collect::<Vec<_>>()
        };
        let own = SecretKey::from_base64(TEST_KEY).expect("key");
        let opened = mirrored(Some(&own));
        assert_eq!(opened.len(), 1);
        assert_eq!(
            opened[0]["notification"]["endpoint"],
            doc["notification"]["endpoint"]
        );
        let other =
            SecretKey::from_base64("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").expect("key");
        assert!(mirrored(None).is_empty(), "mirrored without the key");
        assert!(
            mirrored(Some(&other)).is_empty(),
            "mirrored under another key"
        );
        let _ = kv.purge(key).await;
    }

    /// A Context Source Registration Subscription's KV entry only wakes the
    /// sweep (`apply_delta` reads no document from it), so it ships none:
    /// neither the read nor the credentials it would carry.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_csource_subscription_kv_entry_carries_no_document() {
        let Some(url) = nats_url() else { return };
        let mut st = AppState::new("antares".into());
        let _bus = wire_nats(&mut st, &url, Roles::parse("api").expect("roles"), None)
            .await
            .expect("wire");
        let tenant = TenantId::new("csubnodoc").expect("tenant");
        let id = format!("urn:ngsi-ld:Subscription:csubnodoc{}", std::process::id());
        let doc = json!({"id": id, "type": "ContextSourceRegistrationSubscription",
            "entities": [{"type": "https://x/T"}],
            "notification": {"endpoint": {
                "uri": "http://localhost:1/n",
                "receiverInfo": [{"key": "Authorization", "value": "Bearer test-token"}]}}});
        st.store
            .create(&tenant, Kind::CSourceSubscription, &id, doc.clone())
            .await
            .expect("create");
        let kv = NatsBus::connect(&url)
            .await
            .expect("connect")
            .subs_kv()
            .await
            .expect("kv");
        let key = kv_key(tenant.as_str(), Kind::CSourceSubscription, &id);
        let sync = st.sub_sync.clone().expect("api role installs the sync");
        sync(&tenant, Kind::CSourceSubscription, &id, Some(&doc));
        wait_until("the entry in the KV", || {
            let (kv, key) = (kv.clone(), key.clone());
            async move { kv.get(key).await.ok().flatten().is_some() }
        })
        .await;
        let raw = kv.get(key.clone()).await.expect("get").expect("entry");
        let _ = kv.purge(key).await;
        let entry: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        assert_eq!(entry["csub"], true);
        assert!(entry["doc"].is_null(), "a document was shipped");
    }

    /// An entry put before ANTARES_SECRET_KEY was set holds its credentials
    /// in plaintext; the boot pass seals it from the store's row, and the
    /// entry still opens into the same document.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_boot_pass_seals_a_plaintext_subscription_kv_entry() {
        let Some(url) = nats_url() else { return };
        let st = AppState::new("antares".into());
        let tenant = TenantId::new("sealbootpass").expect("tenant");
        let id = format!("urn:ngsi-ld:Subscription:sealboot{}", std::process::id());
        let doc = json!({"id": id, "type": "Subscription",
            "entities": [{"type": "https://x/T"}],
            "notification": {"endpoint": {
                "uri": "http://hook:pa55word@localhost:1/n",
                "receiverInfo": [{"key": "Authorization", "value": "Bearer test-token"}]}}});
        st.store
            .create(&tenant, Kind::Subscription, &id, doc.clone())
            .await
            .expect("create");
        let kv = NatsBus::connect(&url)
            .await
            .expect("connect")
            .subs_kv()
            .await
            .expect("kv");
        let key = kv_key(tenant.as_str(), Kind::Subscription, &id);
        let plain = json!({"tenant": tenant.as_str(), "id": id, "doc": doc, "csub": false});
        kv.put(
            key.clone(),
            serde_json::to_vec(&plain).expect("json").into(),
        )
        .await
        .expect("put");
        let secret = SecretKey::from_base64(TEST_KEY).expect("key");
        let n = seal_kv_entries(&kv, &*st.store, &secret, vec![key.clone()]).await;
        let raw = kv.get(key.clone()).await.expect("get").expect("entry");
        let _ = kv.purge(key).await;
        assert_eq!(n, 1);
        let text = String::from_utf8_lossy(&raw).into_owned();
        assert!(
            !text.contains("test-token") && !text.contains("pa55word"),
            "plaintext credential left in the KV entry"
        );
        let mut entry: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        open_sub_delta(Some(&secret), &mut entry);
        assert_eq!(
            entry["doc"]["notification"]["endpoint"],
            doc["notification"]["endpoint"]
        );
    }

    /// The registration mirror converges on what the store holds. Each
    /// write's delta was its own task publishing the document captured at
    /// write time, so one that landed late (a retried publish, or a task
    /// scheduled after a newer write's) left every api pod's mirror holding
    /// the older registration, and the federation path reads that mirror as
    /// the truth until a restart re-hydrates it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_late_registration_sync_never_overwrites_a_newer_write() {
        use antares_store::CurrentStateDriverExt;
        let Some(url) = nats_url() else { return };
        let mut st = AppState::new("antares".into());
        let bus = wire_nats(&mut st, &url, Roles::parse("api").expect("roles"), None)
            .await
            .expect("wire");
        let tenant = TenantId::new(&format!("reglate{}", std::process::id())).expect("tenant");
        let id = format!(
            "urn:ngsi-ld:ContextSourceRegistration:late{}",
            std::process::id()
        );
        let doc = |v: i64| json!({"id": id, "type": "ContextSourceRegistration", "v": v});
        st.store
            .create(&tenant, Kind::Registration, &id, doc(1))
            .await
            .expect("create");
        st.store
            .mutate::<(), ()>(&tenant, Kind::Registration, &id, |d| {
                *d = doc(2);
                Ok(())
            })
            .await
            .expect("update");
        // a second pod's mirror, fed from the stream
        let mirror = Arc::new(DocMirror::default());
        let watcher = tokio::spawn(watch_registry(
            bus.clone(),
            bus.consume_registry_broadcast().await.expect("consumer"),
            mirror.clone(),
            st.store.clone(),
            st.egress.clone(),
        ));
        let held = || {
            let (m, t) = (mirror.clone(), tenant.as_str().to_owned());
            async move {
                m.docs(&t)
                    .iter()
                    .find_map(|d| d.get("v").and_then(serde_json::Value::as_i64))
            }
        };
        let sync = st.reg_sync.clone().expect("api role installs the sync");
        // the newer write's delta lands first ...
        sync(&tenant, &id, Some(&doc(2)));
        wait_until("the newer write in the mirror", || {
            let v = held();
            async move { v.await == Some(2) }
        })
        .await;
        // ... and the older write's delta after it
        sync(&tenant, &id, Some(&doc(1)));
        tokio::time::sleep(std::time::Duration::from_millis(
            1_000 * antares_api::state::slow_factor(),
        ))
        .await;
        assert_eq!(
            held().await,
            Some(2),
            "the mirror went back to the registration an older write captured"
        );
        watcher.abort();
    }
}
