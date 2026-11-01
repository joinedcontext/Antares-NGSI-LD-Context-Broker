// SPDX-License-Identifier: EUPL-1.2
//! Notification credentials sealed at rest.
//!
//! A Subscription's `endpoint.receiverInfo` values are the headers its
//! notifications carry (6.3.8), typically a bearer token. With
//! `ANTARES_SECRET_KEY` set, the doc store writes each value as
//! `enc:v1:<base64(nonce | AES-256-GCM ciphertext)>` and opens it again on
//! every read, so every caller above the store sees the plaintext and a dump
//! or a `SELECT` sees none. A dead letter keeps the resolved pairs it would
//! have sent (`receiverInfo`, and `headers` in letters from before the sink
//! registry) and is sealed the same way.
//!
//! A value without the prefix is read as it is, so rows written before the
//! key was set stay readable; [`super::doc::PgDocStore::seal_plaintext`]
//! rewrites them at boot. Without a key, a client's own text that starts
//! with `enc:` is stored behind [`LITERAL`], so `enc:v1:` in a row always
//! means ciphertext. A sealed value read without the key, or under
//! another key, is an error: a notification never goes out with ciphertext in
//! its headers.
//!
//! A Snapshot's `receiverInfo` (Table 5.2.41-1) is the header set of its
//! 5.16.6 notification and is sealed the same way. An endpoint URI that
//! carries `user:password@` userinfo (7.2; an HTTP client sends it as Basic
//! auth) is a credential as well: such a URI is sealed whole, in every kind
//! above. A URI without userinfo stays readable.
//!
//! The bus KV mirror (ANTARES_BUS=nats) carries the Subscription documents
//! between pods; the wiring seals them with [`sealed`] before the put and
//! opens them with [`open`] where the matcher applies an entry, so the
//! bucket holds what the row holds. The memory/file store keeps plaintext;
//! the Pg store is the one deployments run, seal it when a deployment runs it.

use base64::Engine as _;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::Value;

use super::doc::DocKind;

/// The marker of a sealed value; `v1` names AES-256-GCM with a random
/// 96-bit nonce prepended to the ciphertext.
pub const PREFIX: &str = "enc:v1:";

/// The marker of a client's own text stored without a key: a value that
/// starts with `enc:` (`PREFIX` or this marker itself) is written behind it,
/// and `open` strips it again. Without it a client's `enc:v1:…` text read
/// back as ciphertext and failed every read of its row, the listing included.
const LITERAL: &str = "enc:lit:";

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// The key `ANTARES_SECRET_KEY` names: 32 bytes, as 64 hex digits (what the
/// deployment's secret generator writes with `encoding: hex`) or base64.
pub struct SecretKey {
    key: LessSafeKey,
    rng: SystemRandom,
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

impl SecretKey {
    /// The key from 64 hex digits or its base64 text (standard alphabet,
    /// padded). The error names the variable and never echoes the value.
    pub fn from_base64(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        let hex = (raw.len() == 64 && raw.bytes().all(|b| b.is_ascii_hexdigit())).then(|| {
            raw.as_bytes()
                .chunks(2)
                .filter_map(|p| u8::from_str_radix(std::str::from_utf8(p).ok()?, 16).ok())
                .collect::<Vec<u8>>()
        });
        let bytes = match hex {
            Some(b) => b,
            None => B64.decode(raw).map_err(|_| {
                "ANTARES_SECRET_KEY must be 32 bytes as 64 hex digits or base64 \
                 (openssl rand -hex 32)"
                    .to_owned()
            })?,
        };
        if bytes.len() != 32 {
            return Err(format!(
                "ANTARES_SECRET_KEY must be 32 bytes in base64 (got {} bytes; \
                 openssl rand -base64 32)",
                bytes.len()
            ));
        }
        let unbound = UnboundKey::new(&AES_256_GCM, &bytes)
            .map_err(|_| "ANTARES_SECRET_KEY: the key was refused".to_owned())?;
        Ok(Self {
            key: LessSafeKey::new(unbound),
            rng: SystemRandom::new(),
        })
    }

    fn seal(&self, plain: &str, aad: &[u8]) -> Result<String, Unreadable> {
        let mut nonce = [0u8; NONCE_LEN];
        self.rng.fill(&mut nonce).map_err(|_| Unreadable)?;
        let mut buf = plain.as_bytes().to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut buf,
            )
            .map_err(|_| Unreadable)?;
        let mut out = nonce.to_vec();
        out.append(&mut buf);
        Ok(format!("{PREFIX}{}", B64.encode(out)))
    }

    fn open(&self, sealed: &str, aad: &[u8]) -> Result<String, Unreadable> {
        let mut raw = B64.decode(sealed).map_err(|_| Unreadable)?;
        let nonce = raw.get(..NONCE_LEN).ok_or(Unreadable)?;
        let nonce = Nonce::try_assume_unique_for_key(nonce).map_err(|_| Unreadable)?;
        // Opened inside the decoded buffer: the plain text moves to its
        // front, so the buffer becomes the String without a copy.
        let len = self
            .key
            .open_within(nonce, Aad::from(aad), &mut raw, NONCE_LEN..)
            .map_err(|_| Unreadable)?
            .len();
        raw.truncate(len);
        String::from_utf8(raw).map_err(|_| Unreadable)
    }
}

/// A sealed value this process cannot open: no key, another key, or a
/// damaged value.
#[derive(Debug)]
pub struct Unreadable;

impl From<Unreadable> for sqlx::Error {
    fn from(_: Unreadable) -> Self {
        tracing::error!(
            "a stored notification credential (receiverInfo) cannot be opened: \
             ANTARES_SECRET_KEY is unset or differs from the key it was sealed with"
        );
        sqlx::Error::Configuration(Box::new(antares_model::NgsiError::InternalError(
            "a stored notification credential cannot be read".into(),
        )))
    }
}

/// Whether an endpoint URI carries userinfo (`scheme://user:pass@host`), the
/// rule `antares_notifier::redact_userinfo` strips. A URI starting with
/// `enc:` is read as one too: it is the sealed (or escaped) form of such a URI,
/// or a client's text that has to round-trip like one.
fn credential_uri(uri: &Value) -> bool {
    let Some(uri) = uri.as_str() else {
        return false;
    };
    if uri.starts_with("enc:") {
        return true;
    }
    uri.split_once("//").is_some_and(|(_, rest)| {
        rest.split(['/', '?', '#'])
            .next()
            .is_some_and(|authority| authority.contains('@'))
    })
}

/// The `receiverInfo` KeyValuePair values (5.2.22) and the endpoint URI with
/// userinfo of one endpoint object: `uri` names the URI member.
fn endpoint_credentials<'a>(
    endpoint: &'a mut serde_json::Map<String, Value>,
    uri: &str,
) -> Vec<&'a mut Value> {
    let mut out = Vec::new();
    for (k, v) in endpoint.iter_mut() {
        if k == uri && credential_uri(v) {
            out.push(v);
        } else if k == "receiverInfo" {
            if let Some(pairs) = v.as_array_mut() {
                out.extend(pairs.iter_mut().filter_map(|p| p.get_mut("value")));
            }
        }
    }
    out
}

/// The string values of `doc` that hold a credential of `kind`.
fn credentials(kind: DocKind, doc: &mut Value) -> Vec<&mut Value> {
    match kind {
        DocKind::Subscription | DocKind::CSourceSubscription => doc
            .pointer_mut("/notification/endpoint")
            .and_then(Value::as_object_mut)
            .map(|ep| endpoint_credentials(ep, "uri"))
            .unwrap_or_default(),
        // Table 5.2.41-1 keeps `endpoint` (the URI) and `receiverInfo` as
        // members of the Snapshot itself
        DocKind::Snapshot => doc
            .as_object_mut()
            .map(|o| endpoint_credentials(o, "endpoint"))
            .unwrap_or_default(),
        // Outbound::receiver_info and the old rendered header list are
        // `[key, value]` pairs.
        DocKind::DeadLetter => {
            let Some(o) = doc.as_object_mut() else {
                return Vec::new();
            };
            let mut out = Vec::new();
            for (k, v) in o.iter_mut() {
                if k == "uri" && credential_uri(v) {
                    out.push(v);
                } else if k == "receiverInfo" || k == "headers" {
                    if let Some(pairs) = v.as_array_mut() {
                        out.extend(pairs.iter_mut().filter_map(|p| p.get_mut(1)));
                    }
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Whether `kind` holds credentials this module seals.
pub(crate) fn holds_credentials(kind: DocKind) -> bool {
    matches!(
        kind,
        DocKind::Subscription
            | DocKind::CSourceSubscription
            | DocKind::Snapshot
            | DocKind::DeadLetter
    )
}

/// The row a sealed value belongs to. It is the AEAD's associated data, so
/// a value opens only in the row it was sealed for: an `enc:v1:` text copied
/// out of a dump into another subscription (another tenant, kind or id)
/// fails to open instead of being delivered to its new receiver.
#[derive(Clone, Copy)]
pub struct Row<'a> {
    pub kind: DocKind,
    pub tenant: &'a str,
    pub id: &'a str,
}

impl Row<'_> {
    fn aad(&self) -> Vec<u8> {
        let kind = match self.kind {
            DocKind::Subscription => "subscription",
            DocKind::CSourceSubscription => "csource-subscription",
            DocKind::Snapshot => "snapshot",
            DocKind::DeadLetter => "dead-letter",
            _ => "other",
        };
        // NUL never occurs in a tenant name or an id, so the parts cannot
        // run into each other
        format!("antares-seal-v1\0{kind}\0{}\0{}", self.tenant, self.id).into_bytes()
    }
}

/// `doc` as it is stored: every credential sealed under `key` for `row`. A
/// value the client sent already starting with `enc:v1:` is sealed like any
/// other text, so it reads back as that literal text, never decrypted.
/// Without a key such a value is kept behind [`LITERAL`] for the same reason;
/// nothing to seal or escape writes the doc as it came.
pub fn sealed<'a>(
    key: Option<&SecretKey>,
    row: Row<'_>,
    doc: &'a Value,
) -> Result<std::borrow::Cow<'a, Value>, Unreadable> {
    if !holds_credentials(row.kind) {
        return Ok(std::borrow::Cow::Borrowed(doc));
    }
    let Some(key) = key else {
        let mut out = doc.clone();
        let mut escaped = false;
        for v in credentials(row.kind, &mut out) {
            if let Some(s) = v.as_str().filter(|s| s.starts_with("enc:")) {
                *v = Value::String(format!("{LITERAL}{s}"));
                escaped = true;
            }
        }
        return Ok(if escaped {
            std::borrow::Cow::Owned(out)
        } else {
            std::borrow::Cow::Borrowed(doc)
        });
    };
    let aad = row.aad();
    let mut out = doc.clone();
    for v in credentials(row.kind, &mut out) {
        if let Some(s) = v.as_str() {
            *v = Value::String(key.seal(s, &aad)?);
        }
    }
    Ok(std::borrow::Cow::Owned(out))
}

/// Open every sealed credential of the stored `doc` of `row` in place;
/// plaintext values (rows from before the key) pass through, and a client's
/// text kept behind [`LITERAL`] comes back as it was sent.
pub fn open(key: Option<&SecretKey>, row: Row<'_>, doc: &mut Value) -> Result<(), Unreadable> {
    let aad = row.aad();
    for v in credentials(row.kind, doc) {
        let Some(s) = v.as_str() else {
            continue;
        };
        if let Some(text) = s.strip_prefix(LITERAL) {
            *v = Value::String(text.to_owned());
        } else if let Some(body) = s.strip_prefix(PREFIX) {
            *v = Value::String(key.ok_or(Unreadable)?.open(body, &aad)?);
        }
    }
    Ok(())
}

/// Whether `doc` of `kind` still holds a credential that is not sealed: a
/// value put before the key was set, or one kept behind [`LITERAL`].
pub fn holds_plaintext(kind: DocKind, doc: &Value) -> bool {
    let mut doc = doc.clone();
    credentials(kind, &mut doc)
        .into_iter()
        .any(|v| v.as_str().is_some_and(|s| !s.starts_with(PREFIX)))
}

/// The jsonpath that finds a row still holding a plaintext credential of
/// `kind` (the boot pass's filter). The URI regex is [`credential_uri`]'s
/// userinfo rule: the authority after the first `//` holds an `@`.
pub(crate) fn plaintext_path(kind: DocKind) -> Option<&'static str> {
    match kind {
        DocKind::Subscription | DocKind::CSourceSubscription => Some(
            "$ ? (exists(@.notification.endpoint.receiverInfo[*].value ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\"))) \
             || exists(@.notification.endpoint.uri ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\") && @ like_regex \"^([^/]|/[^/])*//[^/?#]*@\")))",
        ),
        DocKind::Snapshot => Some(
            "$ ? (exists(@.receiverInfo[*].value ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\"))) \
             || exists(@.endpoint ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\") && @ like_regex \"^([^/]|/[^/])*//[^/?#]*@\")))",
        ),
        DocKind::DeadLetter => Some(
            "$ ? (exists(@.receiverInfo[*][1] ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\"))) \
             || exists(@.headers[*][1] ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\"))) \
             || exists(@.uri ? (@.type() == \"string\" && !(@ starts with \"enc:v1:\") && @ like_regex \"^([^/]|/[^/])*//[^/?#]*@\")))",
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(byte: u8) -> SecretKey {
        SecretKey::from_base64(&B64.encode([byte; 32])).expect("key")
    }

    fn row(kind: DocKind) -> Row<'static> {
        Row {
            kind,
            tenant: "t",
            id: "urn:ngsi-ld:Subscription:s",
        }
    }

    /// The bounce of a value copied into another row (tenant, kind
    /// or id) does not open there, and a client-sent `enc:v1:` text is kept
    /// as text, never decrypted.
    #[test]
    fn a_sealed_value_opens_only_in_its_own_row() {
        let k = key(8);
        let stored = sealed(Some(&k), row(DocKind::Subscription), &sub("Bearer t"))
            .expect("seal")
            .into_owned();
        let elsewhere = [
            Row {
                id: "urn:ngsi-ld:Subscription:other",
                ..row(DocKind::Subscription)
            },
            Row {
                tenant: "u",
                ..row(DocKind::Subscription)
            },
            row(DocKind::CSourceSubscription),
        ];
        for other in elsewhere {
            assert!(open(Some(&k), other, &mut stored.clone()).is_err());
        }
        let pasted = value_of(&stored);
        let again = sealed(Some(&k), row(DocKind::Subscription), &sub(&pasted))
            .expect("seal")
            .into_owned();
        let mut back = again.clone();
        open(Some(&k), row(DocKind::Subscription), &mut back).expect("open");
        assert_eq!(value_of(&back), pasted);
    }

    fn value_of(doc: &Value) -> String {
        doc["notification"]["endpoint"]["receiverInfo"][0]["value"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    fn sub(value: &str) -> Value {
        json!({"id": "urn:ngsi-ld:Subscription:s", "notification": {"endpoint": {
            "uri": "http://h/n",
            "receiverInfo": [{"key": "Authorization", "value": value}]
        }}})
    }

    #[test]
    fn a_sealed_subscription_shows_no_credential_and_opens_to_it() {
        let k = key(7);
        let doc = sub("Bearer t");
        let stored = sealed(Some(&k), row(DocKind::Subscription), &doc).expect("seal");
        let text = stored.to_string();
        assert!(!text.contains("Bearer t"), "{text}");
        assert!(text.contains(PREFIX), "{text}");
        let mut back = stored.into_owned();
        open(Some(&k), row(DocKind::Subscription), &mut back).expect("open");
        assert_eq!(back, doc);
    }

    /// A Snapshot's `receiverInfo` (Table 5.2.41-1) is its notification's
    /// header set: sealed, and opened back in its own row only.
    #[test]
    fn a_sealed_snapshot_shows_no_credential_and_opens_to_it() {
        let k = key(10);
        let doc = json!({"id": "urn:ngsi-ld:Snapshot:s", "type": "Snapshot",
                         "endpoint": "http://h/n",
                         "receiverInfo": [{"key": "Authorization", "value": "Bearer t"}]});
        let stored = sealed(Some(&k), row(DocKind::Snapshot), &doc).expect("seal");
        assert!(!stored.to_string().contains("Bearer t"), "{stored}");
        assert_eq!(stored["receiverInfo"][0]["key"], "Authorization");
        assert_eq!(stored["endpoint"], "http://h/n");
        let mut back = stored.clone().into_owned();
        open(Some(&k), row(DocKind::Snapshot), &mut back).expect("open");
        assert_eq!(back, doc);
        let elsewhere = Row {
            id: "urn:ngsi-ld:Snapshot:other",
            ..row(DocKind::Snapshot)
        };
        assert!(open(Some(&k), elsewhere, &mut stored.into_owned()).is_err());
    }

    /// 7.2: `user:password@` in an endpoint URI is a credential (an HTTP
    /// client sends it as Basic auth). Such a URI is sealed whole in every
    /// kind that keeps one; a URI without userinfo, or with an `@` only in
    /// its path, stays readable.
    #[test]
    fn an_endpoint_uri_with_userinfo_is_sealed_whole() {
        let k = key(11);
        let secret = "https://user:pa55@h/n?x=1";
        let docs = [
            (
                DocKind::Subscription,
                json!({"notification": {"endpoint": {"uri": secret}}}),
                "/notification/endpoint/uri",
            ),
            (
                DocKind::CSourceSubscription,
                json!({"notification": {"endpoint": {"uri": secret}}}),
                "/notification/endpoint/uri",
            ),
            (DocKind::Snapshot, json!({"endpoint": secret}), "/endpoint"),
            (DocKind::DeadLetter, json!({"uri": secret}), "/uri"),
        ];
        for (kind, doc, at) in docs {
            let stored = sealed(Some(&k), row(kind), &doc).expect("seal");
            assert!(!stored.to_string().contains("pa55"), "{kind:?}: {stored}");
            assert!(
                stored
                    .pointer(at)
                    .and_then(Value::as_str)
                    .is_some_and(|u| u.starts_with(PREFIX)),
                "{kind:?}: {stored}"
            );
            let mut back = stored.into_owned();
            open(Some(&k), row(kind), &mut back).expect("open");
            assert_eq!(back, doc, "{kind:?}");
        }
        for bare in ["https://h/n", "mqtt://h:1883/a@b", "urn:antares:distsub:x"] {
            let doc = json!({"notification": {"endpoint": {"uri": bare}}});
            let stored = sealed(Some(&k), row(DocKind::Subscription), &doc).expect("seal");
            assert_eq!(stored["notification"]["endpoint"]["uri"], bare);
        }
    }

    #[test]
    fn a_dead_letter_seals_its_resolved_pairs() {
        let k = key(1);
        let doc = json!({"receiverInfo": [["Authorization", "Bearer t"]],
                         "headers": [["X-A", "b"]], "uri": "http://h/n"});
        let stored = sealed(Some(&k), row(DocKind::DeadLetter), &doc).expect("seal");
        assert_eq!(stored["receiverInfo"][0][0], "Authorization");
        assert!(!stored.to_string().contains("Bearer t"));
        assert!(!stored.to_string().contains("\"b\""));
        let mut back = stored.into_owned();
        open(Some(&k), row(DocKind::DeadLetter), &mut back).expect("open");
        assert_eq!(back, doc);
    }

    /// S3-05: without a key a client's own `enc:v1:` text is stored and read
    /// back as that text, not opened as a sealed value no key can open (which
    /// failed every read of the row, and the whole listing with it). The boot
    /// pass of a key set later still finds it as plaintext and seals it.
    #[test]
    fn a_client_prefixed_value_reads_back_without_a_key() {
        for text in ["enc:v1:AAAA", "enc:lit:x", "Bearer t"] {
            let doc = sub(text);
            let stored = sealed(None, row(DocKind::Subscription), &doc)
                .expect("no key")
                .into_owned();
            let mut back = stored.clone();
            open(None, row(DocKind::Subscription), &mut back)
                .expect("a client literal is not a sealed value");
            assert_eq!(value_of(&back), text);
            assert!(
                !value_of(&stored).starts_with(PREFIX),
                "{text} is stored as {}",
                value_of(&stored)
            );
            let k = key(9);
            let mut later = stored;
            open(Some(&k), row(DocKind::Subscription), &mut later).expect("open under a key");
            let resealed = sealed(Some(&k), row(DocKind::Subscription), &later)
                .expect("seal")
                .into_owned();
            let mut again = resealed.clone();
            open(Some(&k), row(DocKind::Subscription), &mut again).expect("open");
            assert_eq!(value_of(&again), text);
        }
    }

    #[test]
    fn plaintext_passes_and_ciphertext_never_leaves_without_its_key() {
        let mut plain = sub("Bearer t");
        assert!(open(None, row(DocKind::Subscription), &mut plain).is_ok());
        assert_eq!(plain, sub("Bearer t"));

        let stored = sealed(Some(&key(2)), row(DocKind::Subscription), &sub("Bearer t"))
            .expect("seal")
            .into_owned();
        assert!(open(None, row(DocKind::Subscription), &mut stored.clone()).is_err());
        assert!(open(
            Some(&key(3)),
            row(DocKind::Subscription),
            &mut stored.clone()
        )
        .is_err());
    }

    #[test]
    fn a_damaged_value_is_unreadable() {
        let k = key(6);
        let aad = row(DocKind::Subscription).aad();
        let stored = k.seal("Bearer t", &aad).expect("seal");
        let body = stored.strip_prefix(PREFIX).expect("prefix");
        assert_eq!(k.open(body, &aad).expect("open"), "Bearer t");
        assert!(k.open(&B64.encode([1u8; 5]), &aad).is_err());
        assert!(k.open(&B64.encode([1u8; NONCE_LEN + 3]), &aad).is_err());
        let mut raw = B64.decode(body).expect("b64");
        if let Some(last) = raw.last_mut() {
            *last ^= 1;
        }
        assert!(k.open(&B64.encode(raw), &aad).is_err());
    }

    #[test]
    fn no_key_or_another_kind_stores_the_doc_as_it_came() {
        let doc = sub("Bearer t");
        assert_eq!(
            *sealed(None, row(DocKind::Subscription), &doc).expect("none"),
            doc
        );
        assert_eq!(
            *sealed(Some(&key(4)), row(DocKind::View), &doc).expect("view"),
            doc
        );
    }

    #[test]
    fn the_key_is_32_bytes_of_base64() {
        assert!(SecretKey::from_base64("not base64!").is_err());
        let short = SecretKey::from_base64(&B64.encode([0u8; 16])).expect_err("short");
        assert!(short.contains("got 16 bytes"), "{short}");
        assert!(SecretKey::from_base64(&format!(" {} \n", B64.encode([9u8; 32]))).is_ok());
        assert_eq!(format!("{:?}", key(5)), "SecretKey(..)");
        // 64 hex digits are the 32 bytes they spell, the same key as base64
        let hex = SecretKey::from_base64(&"07".repeat(32)).expect("hex");
        let stored = sealed(Some(&hex), row(DocKind::Subscription), &sub("Bearer t"))
            .expect("seal")
            .into_owned();
        assert!(open(
            Some(&key(7)),
            row(DocKind::Subscription),
            &mut stored.clone()
        )
        .is_ok());
    }
}
