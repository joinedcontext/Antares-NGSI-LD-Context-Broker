// SPDX-License-Identifier: EUPL-1.2
//! Live-NATS integration (env-gated, same pattern as the live-PG
//! tests): the broadcast-vs-balanced assertion, the claim check on
//! the wire, and Nats-Msg-Id dedup. Skips loudly without
//! ANTARES_TEST_NATS_URL (CI runs a nats:2 -js service).

use antares_bus::nats::NatsBus;
use antares_bus::{ChangeEvent, ChangeOp};
use antares_model::{EntityId, TenantId};
use futures_util::StreamExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

macro_rules! require_nats {
    () => {
        match std::env::var("ANTARES_TEST_NATS_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("SKIP: ANTARES_TEST_NATS_URL not set");
                return;
            }
        }
    };
}

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn event(tenant: &TenantId, id: &str, seq: i64) -> ChangeEvent {
    ChangeEvent {
        tenant: tenant.clone(),
        entity_id: EntityId::new(id).expect("id"),
        types: vec!["https://x/T".into()],
        op: ChangeOp::Create,
        changed_attrs: vec![],
        payload: Some(serde_json::json!({"id": id, "type": ["https://x/T"]})),
        prev_payload: None,
        version: 1,
        incarnation: "2026-08-05T00:00:00Z".into(),
        seq,
        payload_ref: None,
        prev_payload_ref: None,
    }
}

/// Two instances on the SAME durable split the work — each message is
/// delivered to exactly one of them; two ephemeral registry consumers each
/// see EVERY delta. The distinction that Scorpio's `$[quarkus.uuid}` typo
/// silently destroyed, asserted against a live server.
#[tokio::test(flavor = "multi_thread")]
async fn balanced_splits_and_broadcast_duplicates() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let n = nonce();
    let tenant = TenantId::new(&format!("bal{n}")).expect("tenant");

    // two consumers, ONE durable — a shared work queue
    let durable = format!("bal-{n}");
    let c1 = bus.consume_balanced(&durable).await.expect("consumer 1");
    let c2 = bus.consume_balanced(&durable).await.expect("consumer 2");

    const N: usize = 8;
    for i in 0..N {
        bus.publish(&event(&tenant, &format!("urn:bal:{n}:{i}"), i as i64 + 1))
            .await
            .expect("publish");
    }

    // both instances pull; the union must be exactly the N messages, no
    // message seen twice (each counted by its unique entity id)
    let mut seen = std::collections::HashSet::new();
    let mut total = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut s1 = c1.messages().await.expect("stream 1");
    let mut s2 = c2.messages().await.expect("stream 2");
    while total < N && tokio::time::Instant::now() < deadline {
        tokio::select! {
            Some(Ok(m)) = s1.next() => {
                if let Some(ev) = antares_bus::nats::decode(&m) {
                    if ev.tenant.as_str() == tenant.as_str() {
                        assert!(seen.insert(ev.entity_id.as_str().to_owned()),
                            "balanced durable delivered {} twice", ev.entity_id.as_str());
                        total += 1;
                    }
                }
                m.ack().await.expect("ack");
            }
            Some(Ok(m)) = s2.next() => {
                if let Some(ev) = antares_bus::nats::decode(&m) {
                    if ev.tenant.as_str() == tenant.as_str() {
                        assert!(seen.insert(ev.entity_id.as_str().to_owned()),
                            "balanced durable delivered {} twice", ev.entity_id.as_str());
                        total += 1;
                    }
                }
                m.ack().await.expect("ack");
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }
    assert_eq!(total, N, "balanced durable must deliver every message once");

    // broadcast: two EPHEMERAL registry consumers each see the same delta
    let b1 = bus.consume_registry_broadcast().await.expect("bc 1");
    let b2 = bus.consume_registry_broadcast().await.expect("bc 2");
    let delta =
        serde_json::json!({"tenant": tenant.as_str(), "id": format!("urn:reg:{n}"), "doc": {}});
    bus.publish_registry(tenant.as_str(), &delta)
        .await
        .expect("registry publish");
    for (label, c) in [("bc1", b1), ("bc2", b2)] {
        let mut s = c.messages().await.expect("stream");
        let got = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let Some(Ok(m)) = s.next().await else {
                    continue;
                };
                let v: serde_json::Value = serde_json::from_slice(&m.payload).expect("json");
                m.ack().await.expect("ack");
                if v["tenant"] == tenant.as_str() {
                    return v;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{label} never saw the broadcast delta"));
        assert_eq!(got["id"], format!("urn:reg:{n}"));
    }
}

/// A payload over the claim-check threshold travels as a reference, and
/// the whole message stays far under NATS's ~1 MB cap.
#[tokio::test(flavor = "multi_thread")]
async fn claim_check_strips_oversized_payloads_on_the_wire() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let n = nonce();
    let tenant = TenantId::new(&format!("cc{n}")).expect("tenant");
    let durable = format!("cc-{n}");
    let c = bus.consume_balanced(&durable).await.expect("consumer");

    let mut ev = event(&tenant, &format!("urn:cc:{n}"), 1);
    ev.payload = Some(serde_json::Value::String(
        "x".repeat(antares_bus::CLAIM_CHECK_BYTES + 1),
    ));
    bus.publish(&ev).await.expect("publish");

    let mut s = c.messages().await.expect("stream");
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let Some(Ok(m)) = s.next().await else {
                continue;
            };
            let ev = antares_bus::nats::decode(&m);
            m.ack().await.expect("ack");
            if let Some(ev) = ev {
                if ev.tenant.as_str() == tenant.as_str() {
                    return ev;
                }
            }
        }
    })
    .await
    .expect("claim-checked event never arrived");
    assert!(
        got.payload.is_none(),
        "oversized body must not travel inline"
    );
    let r = got.payload_ref.expect("reference set");
    assert_eq!(r.entity_id.as_str(), format!("urn:cc:{n}"));
    assert_eq!(r.version, 1);
}

/// A drain retry republishing the same outbox seq is absorbed by the
/// stream's duplicate window — one delivery, not two.
#[tokio::test(flavor = "multi_thread")]
async fn msg_id_dedup_absorbs_republish() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let n = nonce();
    let tenant = TenantId::new(&format!("dd{n}")).expect("tenant");
    let durable = format!("dd-{n}");
    let c = bus.consume_balanced(&durable).await.expect("consumer");

    let ev = event(&tenant, &format!("urn:dd:{n}"), 7);
    bus.publish(&ev).await.expect("publish 1");
    bus.publish(&ev)
        .await
        .expect("publish 2 (same Nats-Msg-Id)");

    let mut s = c.messages().await.expect("stream");
    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            Some(Ok(m)) = s.next() => {
                if let Some(got) = antares_bus::nats::decode(&m) {
                    if got.tenant.as_str() == tenant.as_str() {
                        count += 1;
                    }
                }
                m.ack().await.expect("ack");
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
    assert_eq!(
        count, 1,
        "duplicate-window dedup must swallow the republish"
    );
}

/// The recovery contract the registry mirror depends on.
///
/// `consume_registry_broadcast` builds an EPHEMERAL consumer, which a NATS
/// restart or an inactivity gap deletes server-side. When that happens the
/// message stream must END rather than hang, because that is the signal the
/// broker's watcher loop reopens and re-hydrates on; a stream that blocks
/// forever would freeze this pod's registration mirror — and with it all
/// federation matching — while /q/health still reported the bus connected.
/// The second half asserts the other side of the contract: a freshly built
/// consumer really does receive what is published after the gap.
#[tokio::test(flavor = "multi_thread")]
async fn deleted_broadcast_consumer_ends_its_stream_and_a_fresh_one_resumes() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let n = nonce();
    let tenant = format!("reopen{n}");

    let consumer = bus
        .consume_registry_broadcast()
        .await
        .expect("broadcast consumer");
    let name = consumer.cached_info().name.clone();
    let mut msgs = antares_bus::nats::messages(&consumer)
        .await
        .expect("stream");

    bus.publish_registry(&tenant, &serde_json::json!({"id": "urn:reg:before"}))
        .await
        .expect("publish before");
    let got = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(d) = antares_bus::nats::next_delta(&mut msgs).await {
                if d["id"] == "urn:reg:before" {
                    return d;
                }
            } else {
                panic!("stream ended before the first delta arrived");
            }
        }
    })
    .await
    .expect("first delta within 10s");
    assert_eq!(got["id"], "urn:reg:before");

    // The gap: delete the consumer exactly as a server restart would.
    bus.delete_registry_consumer(&name)
        .await
        .expect("delete consumer");

    // Contract: the stream terminates. A hang here is the freeze bug. The
    // stream is shared with every test publishing registry deltas, so what
    // was already delivered before the delete may still come first.
    tokio::time::timeout(Duration::from_secs(15), async {
        while antares_bus::nats::next_delta(&mut msgs).await.is_some() {}
    })
    .await
    .expect("stream must end within 15s after its consumer is deleted, not hang");

    // Recovery: a new consumer sees traffic published after the gap.
    let fresh = bus
        .consume_registry_broadcast()
        .await
        .expect("fresh consumer");
    let mut fresh_msgs = antares_bus::nats::messages(&fresh).await.expect("stream");
    bus.publish_registry(&tenant, &serde_json::json!({"id": "urn:reg:after"}))
        .await
        .expect("publish after");
    let got = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(d) = antares_bus::nats::next_delta(&mut fresh_msgs).await {
                if d["id"] == "urn:reg:after" {
                    return d;
                }
            } else {
                panic!("fresh stream ended instead of delivering");
            }
        }
    })
    .await
    .expect("delta after recovery within 10s");
    assert_eq!(got["id"], "urn:reg:after");
}

/// The subscription mirror's other half: the KV watch is reopenable, and a
/// reopened watch sees writes made while nothing was watching. The broker's
/// watcher reopens on end-of-watch and re-reads the bucket, so a write that
/// lands in the gap must not be lost to a pod that reconnects.
#[tokio::test(flavor = "multi_thread")]
async fn subscription_kv_survives_a_watch_gap() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let kv = bus.subs_kv().await.expect("kv bucket");
    let n = nonce();
    let key = format!("sub.gap.{n}");

    let watch = kv.watch(&key).await.expect("watch");
    kv.put(
        &key,
        serde_json::to_vec(&serde_json::json!({"v": 1}))
            .expect("encode")
            .into(),
    )
    .await
    .expect("put v1");
    drop(watch);

    // Written with nobody watching — the gap.
    kv.put(
        &key,
        serde_json::to_vec(&serde_json::json!({"v": 2}))
            .expect("encode")
            .into(),
    )
    .await
    .expect("put v2");

    // A reopened watch replays current state, so the gap write is not lost.
    let mut reopened = kv.watch_with_history(&key).await.expect("reopen watch");
    let seen = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(Ok(entry)) = reopened.next().await {
            let doc: serde_json::Value = serde_json::from_slice(&entry.value).expect("decode");
            if doc["v"] == 2 {
                return doc;
            }
        }
        panic!("watch ended without replaying the gap write");
    })
    .await
    .expect("reopened watch replays within 10s");
    assert_eq!(seen["v"], 2, "a reopened watch must see the gap write");
}

/// Core request/reply (a relay channel between pods): a request reaches
/// the pod replying on its subject and carries the reply back; with no
/// replier, after the replier is aborted, or once it declines, the request
/// is `None` instead of hanging.
#[tokio::test(flavor = "multi_thread")]
async fn core_request_reaches_the_replier_and_is_none_without_one() {
    let url = require_nats!();
    let holder = NatsBus::connect(&url).await.expect("holder");
    let asker = NatsBus::connect(&url).await.expect("asker");
    let subject = format!("antares.ws.test{}", nonce());
    let within = Duration::from_secs(2);
    assert_eq!(
        asker.request(subject.clone(), b"x".to_vec(), within).await,
        None
    );

    let task = holder.reply(subject.clone(), |p| {
        (p != b"stop").then(|| [b"seen:".as_slice(), p].concat())
    });
    let mut answered = None;
    for _ in 0..50 {
        answered = asker
            .request(subject.clone(), b"hello".to_vec(), within)
            .await;
        if answered.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(answered.as_deref(), Some(b"seen:hello".as_slice()));

    assert_eq!(
        asker
            .request(subject.clone(), b"stop".to_vec(), within)
            .await,
        None
    );
    task.await.expect("the replier ends when it declines");

    let task = holder.reply(subject.clone(), |p| Some(p.to_vec()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    task.abort();
    let _aborted = task.await;
    assert_eq!(asker.request(subject, b"late".to_vec(), within).await, None);
}

/// `ANTARES_CHANGES` keeps a message only while a consumer is interested in
/// it (Interest retention). The matcher durable is the interest, so it has to
/// exist before the first change is published, on every role: an api pod
/// that connects and publishes before any matcher pod has run must not
/// publish into a stream that discards what it is given.
#[tokio::test(flavor = "multi_thread")]
async fn a_change_published_before_the_matcher_ever_ran_reaches_it() {
    let url = require_nats!();
    // a deployment whose matcher has never run: no "matcher" durable
    let js = async_nats::jetstream::new(async_nats::connect(&url).await.expect("raw connect"));
    if let Ok(s) = js.get_stream(antares_bus::nats::CHANGES_STREAM).await {
        let _ = s.delete_consumer("matcher").await;
    }
    // the api pod: connect, publish
    let api = NatsBus::connect(&url).await.expect("connect api");
    let n = nonce();
    let tenant = TenantId::new(&format!("early{n}")).expect("tenant");
    let id = format!("urn:early:{n}");
    api.publish(&event(&tenant, &id, 1)).await.expect("publish");
    // the matcher pod starts afterwards
    let matcher = NatsBus::connect(&url).await.expect("connect matcher");
    let c = matcher
        .consume_balanced("matcher")
        .await
        .expect("matcher durable");
    let mut s = c.messages().await.expect("stream");
    let got = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(m)) = s.next().await {
            m.ack().await.expect("ack");
            if antares_bus::nats::decode(&m).is_some_and(|ev| ev.entity_id.as_str() == id) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        got,
        "the change published before the matcher durable existed was discarded"
    );
}

/// A registration delta is a compare-and-set on the registration's own
/// subject: a publish whose revision a newer delta has passed is refused, so
/// a late writer re-reads instead of landing after the newer one in every
/// mirror. A delta for another registration of the same tenant is no
/// conflict.
#[tokio::test(flavor = "multi_thread")]
async fn a_registration_delta_that_lost_the_race_is_refused() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let tenant = format!("regcas{}", nonce());
    let delta = |v: i64| serde_json::json!({"tenant": tenant, "id": "urn:reg:a", "v": v});
    assert_eq!(
        bus.registration_revision(&tenant, "urn:reg:a")
            .await
            .expect("revision"),
        0,
        "nothing held yet"
    );
    bus.publish_registration(&tenant, "urn:reg:a", &delta(1), 0)
        .await
        .expect("first delta");
    let rev = bus
        .registration_revision(&tenant, "urn:reg:a")
        .await
        .expect("revision");
    assert!(rev > 0);
    // another registration of the tenant does not move this one's revision
    bus.publish_registration(&tenant, "urn:reg:b", &delta(9), 0)
        .await
        .expect("a sibling registration is no conflict");
    assert!(
        bus.publish_registration(&tenant, "urn:reg:a", &delta(0), 0)
            .await
            .is_err(),
        "a delta read before the first one landed must be refused"
    );
    bus.publish_registration(&tenant, "urn:reg:a", &delta(2), rev)
        .await
        .expect("the current revision publishes");
    assert!(
        bus.publish_registration(&tenant, "urn:reg:a", &delta(1), rev)
            .await
            .is_err(),
        "a stale revision must be refused"
    );
}

/// At-least-once without self-inflicted duplicates: a message is acked only
/// after its work (ADR-0002), and while that work outlasts the consumer's
/// `ack_wait` (one slow notification endpoint), neither it nor the messages
/// of the same pull already queued behind it may be redelivered. They were:
/// the server re-sent every one of them, to this pod or another, and each
/// was matched (and notified) twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_worked_on_past_ack_wait_is_not_redelivered() {
    use async_nats::jetstream::{self, stream};
    let url = require_nats!();
    let js = jetstream::new(async_nats::connect(&url).await.expect("connect"));
    let n = nonce();
    let name = format!("ANTARES_TEST_SLOW_{n}");
    let subject = format!("slowwork{n}");
    let s = js
        .create_stream(stream::Config {
            name: name.clone(),
            subjects: vec![format!("{subject}.>")],
            ..Default::default()
        })
        .await
        .expect("stream");
    let mut cfg = antares_bus::nats::balanced_config("worker");
    cfg.ack_wait = Duration::from_secs(1);
    let consumer: antares_bus::nats::PullConsumer = s.create_consumer(cfg).await.expect("consumer");
    for i in 0..3 {
        js.publish(format!("{subject}.m"), i.to_string().into())
            .await
            .expect("publish")
            .await
            .expect("ack");
    }
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let log = seen.clone();
    let runner = tokio::spawn(async move {
        let _ = antares_bus::nats::run_balanced(&consumer, |msg| {
            let body = String::from_utf8_lossy(&msg.payload).into_owned();
            log.lock().expect("lock").push(body.clone());
            async move {
                if body == "0" {
                    // 3.5 ack_waits on the first message
                    tokio::time::sleep(Duration::from_millis(3_500)).await;
                }
            }
        })
        .await;
    });
    tokio::time::sleep(Duration::from_millis(6_000)).await;
    runner.abort();
    let info = s.consumer_info("worker").await.expect("info");
    let seen = seen.lock().expect("lock").clone();
    let _ = js.delete_stream(&name).await;
    assert_eq!(seen, ["0", "1", "2"], "worked on more than once");
    assert_eq!(info.num_redelivered, 0, "the server redelivered");
    assert_eq!(info.num_ack_pending, 0, "a message was left unacked");
}

/// The heartbeat runs across the whole pull, not per message: a batch of
/// messages each shorter than a third of `ack_wait`, together longer than
/// it, kept the messages still queued behind them unacknowledged for their
/// whole wait, and the server redelivered them (each matched and notified
/// twice). The clock restarted with every message and never ticked.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_of_short_messages_longer_than_ack_wait_is_not_redelivered() {
    use async_nats::jetstream::{self, stream};
    let url = require_nats!();
    let js = jetstream::new(async_nats::connect(&url).await.expect("connect"));
    let n = nonce();
    let name = format!("ANTARES_TEST_BATCHBEAT_{n}");
    let subject = format!("batchbeat{n}");
    let s = js
        .create_stream(stream::Config {
            name: name.clone(),
            subjects: vec![format!("{subject}.>")],
            ..Default::default()
        })
        .await
        .expect("stream");
    let mut cfg = antares_bus::nats::balanced_config("worker");
    // a beat every 300 ms; each message takes 220 ms, six take 1.3 s
    cfg.ack_wait = Duration::from_millis(900);
    let consumer: antares_bus::nats::PullConsumer = s.create_consumer(cfg).await.expect("consumer");
    for i in 0..6 {
        js.publish(format!("{subject}.m"), i.to_string().into())
            .await
            .expect("publish")
            .await
            .expect("ack");
    }
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let log = seen.clone();
    let runner = tokio::spawn(async move {
        let _ = antares_bus::nats::run_balanced(&consumer, |msg| {
            log.lock()
                .expect("lock")
                .push(String::from_utf8_lossy(&msg.payload).into_owned());
            tokio::time::sleep(Duration::from_millis(220))
        })
        .await;
    });
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    runner.abort();
    let info = s.consumer_info("worker").await.expect("info");
    let seen = seen.lock().expect("lock").clone();
    let _ = js.delete_stream(&name).await;
    assert_eq!(
        seen,
        ["0", "1", "2", "3", "4", "5"],
        "worked on more than once"
    );
    assert_eq!(info.num_redelivered, 0, "the server redelivered");
    assert_eq!(info.num_ack_pending, 0, "a message was left unacked");
}

/// A balanced durable is created with the explicit ack_wait and
/// max_deliver the heartbeat is paced from. A throwaway durable on the
/// shared change stream (Interest retention: deleted at the end so it holds
/// nothing back).
#[tokio::test(flavor = "multi_thread")]
async fn a_balanced_durable_has_an_explicit_ack_wait_and_max_deliver() {
    let url = require_nats!();
    let bus = NatsBus::connect(&url).await.expect("connect");
    let durable = format!("cfgprobe{}", nonce());
    let mut c = bus.consume_balanced(&durable).await.expect("durable");
    let info = c.info().await.expect("info").clone();
    let js = async_nats::jetstream::new(async_nats::connect(&url).await.expect("connect"));
    let _ = js
        .get_stream(antares_bus::nats::CHANGES_STREAM)
        .await
        .expect("stream")
        .delete_consumer(&durable)
        .await;
    assert_eq!(info.config.durable_name.as_deref(), Some(durable.as_str()));
    assert_eq!(info.config.ack_wait, antares_bus::nats::ACK_WAIT);
    assert_eq!(info.config.max_deliver, antares_bus::nats::MAX_DELIVER);
    assert_eq!(
        info.config.max_ack_pending,
        antares_bus::nats::MAX_ACK_PENDING
    );
}
