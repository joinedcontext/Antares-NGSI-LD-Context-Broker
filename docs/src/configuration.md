# Configuration reference

All configuration is environment variables — no config file. Unknown
`ANTARES_STORE`/`ANTARES_BUS` values are fatal at startup, never silently
defaulted. This table is checked against the source by
`dev/check-env-docs.sh` (CI fails when a variable exists in code but not
here).

## Core

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_STORE` | `memory` | Store mode: `memory`, `file`, `postgres`, `timescale`. Unknown value = fatal. |
| `ANTARES_TEMPORAL` | follows `ANTARES_STORE` | Temporal driver: a store mode, or `none` — history off. Mix freely with `ANTARES_STORE`, e.g. `file` current state with `timescale` history; temporal reads answer `OperationNotSupported` (422, CIM 009 Table 6.3.2-1) and nothing is recorded. A backend different from the store builds a second store instance used only for history. |
| `ANTARES_HTTP_PORT` | `9090` | HTTP listen port. |
| `ANTARES_METRICS_PORT` | unset | A second listener that serves `GET /q/metrics` and answers every other path `404`; with it set, the API port answers `/q/metrics` `404`, so a NetworkPolicy can admit a scraper to this port without the API. Unset, `/q/metrics` stays on the API port. Port 0, the API port or a non-number is fatal at startup. `ANTARES_ADMIN_METRICS` applies on this port as on the API port. |
| `ANTARES_ROLES` | `all` | Comma list of roles this process runs: `api`, `matcher`, `notifier`, `temporal`, `registry` — the role-split fleet shape. |
| `ANTARES_BUS` | `local` | Change-event bus: `local` (in-process, single node) or `nats` (JetStream, multi-pod). Unknown value = fatal. |
| `ANTARES_HOST_ALIAS` | `antares` | This broker's name in federation `Via` chains (CIM 009 6.3.18) — loop detection identity. Two LB'd replicas of one logical broker share one alias. |
| `ANTARES_PUBLIC_URL` | `http://{host_alias}:{port}` | The URL peers can reach this broker at: forwarded subscription copies notify `{ANTARES_PUBLIC_URL}/ex/v1/remote-notify` (5.8.1.4). Set it whenever the default is not routable from peers. |

## Store backends

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_DATA_DIR` | — (required for `file`) | Directory for the redb file. Must be a mounted volume — data never lives inside the image. |
| `ANTARES_DATABASE_URL` | — (required for `postgres`/`timescale`) | PostgreSQL connection string; PostGIS required, TimescaleDB for `timescale`. Bounded startup retry while the DB boots. |
| `ANTARES_REQUIRE_RLS` | unset | `1`/`true`: refuse to start when the DB role bypasses Row-Level Security (defense-in-depth for shared-schema multi-tenancy). |
| `ANTARES_PG_POOL` | `20` | Connection-pool size for `postgres`/`timescale`. Unparsable value = fatal. Sessions carry `lock_timeout` 5 s. A request that waits the pool's 5 s acquire timeout without getting a connection is answered `503` with `Retry-After` — see the sizing formula in the [operations runbook](operations.md#sizing-the-connection-pool). |
| `ANTARES_PG_WRITE_RESERVE` | `0` | How many of the `ANTARES_PG_POOL` connections only batch writes (create, upsert, update) take: the reads share the other `ANTARES_PG_POOL - reserve`, so a flood of reads queued on the pool cannot hold a pipeline's batch upsert behind them. `0` is off (one shared pool). Must be below `ANTARES_PG_POOL`; anything else is fatal. Size it to the batches that run at once (`ANTARES_BATCH_INFLIGHT`, 8): a batch that finds the reserve full waits for it, up to the 5 s acquire timeout. Published as `storeInfo.poolWriteReserve` in `/q/health`. |
| `ANTARES_PG_STATEMENT_TIMEOUT_MS` | `30000` | Per-session `statement_timeout` on every pooled connection: a query past it is cancelled and answered `InternalError` (500, "database statement timeout"; CIM 009 5.5.2 names database timeouts as InternalError). Migrations are exempt. Not a positive integer = fatal. |
| `ANTARES_PG_BATCH_ASYNC_COMMIT` | `false` | `true`: the batch writes (`entityOperations` create, upsert, update and merge, plus the bulk attribute purge that runs through the same store calls) commit with `synchronous_commit = off`, set LOCAL to their own transaction; every other write stays synchronous. A Postgres crash then loses at most the last ~3 x `wal_writer_delay` (600 ms by default) of acknowledged batches, never their atomicity; notifications already sent for them stay sent. For a feed that rewrites its Entities every second (live vehicle positions), where the next batch restores what a crash lost. Not a boolean = fatal. |
| `ANTARES_SECRET_KEY` | unset | 32 bytes as 64 hex digits (`openssl rand -hex 32`) or base64, from a Secret. With it, every Subscription and Context Source Registration Subscription `endpoint.receiverInfo` value and the resolved credentials of a dead letter are stored sealed (AES-256-GCM, `enc:v1:` prefix) in the `postgres`/`timescale` store and opened on read; at boot the broker seals the rows written before the key was set. Without it the values are stored in plaintext and the boot logs a warning. A value that is not 32 bytes = fatal. A sealed value read without the key, or under another key, fails that read with a 500, so keep the key for as long as the database holds rows sealed under it. The memory and file stores ignore it. |
| `ANTARES_MIGRATE` | on | `0`/`false` skips running migrations from this process, so serving replicas do not race the DDL — run them once from a job or init container instead. |
| `ANTARES_ALLOW_SHARED_LOCAL` | unset | `1` permits `bus=local` with a `postgres`/`timescale` store — safe ONLY for a strictly single-process deployment; two such processes double-fire notifications. |
| `ANTARES_TEMPORAL_RECORD` | `all` | History gate for the entity endpoints. `all`: every changed attribute instance is recorded. `observed`: only instances carrying `observedAt` enter history — a metadata-only write (no `observedAt`) updates current state and its `modifiedAt` but leaves no history, so `timeproperty=modifiedAt`/`createdAt` temporal queries return nothing for never-observed attributes. `none`: the entity endpoints record nothing; the temporal API still stores and serves what it is given directly (unlike `ANTARES_TEMPORAL=none`, which switches the temporal seam off). `ruled`: history is opt-in, as `all` for an Entity whose own `historyRetention`, or a stored retention rule for its type or Tenant, gives a duration or `forever` (recording `observed` unless its rule names another `record`), nothing for any other. The ETSI temporal suites assume `all`, which is why it stays the default. Unknown value = fatal. |
| `ANTARES_NON_OBJECT_BODY` | `invalid-request` | Behaviour profile (`docs/conformance/profiles.md`): the error a JSON body that is no object (`[]`, `[1,2,3]`) gets where an Entity, fragment, Subscription or registration is expected. `invalid-request` as ETSI TP 001_02_02 asserts; `bad-request-data` per TS 104 175 8.2.3 (valid JSON, no valid datatype), as forge all-fixes and Coraine assert. A body that is not JSON stays InvalidRequest. Unknown value = fatal. |
| `ANTARES_FED_WRITE_SOURCE_404` | `report` | Behaviour profile: on a distributed write, a part answered 404 beside a successful part. `report` keeps the 404 in a 207 (ETSI D002_02_01_inc, D002_02_02_inc); `ignore` reads it as "no data" (TS 104 176 6.3.5) and answers the success (forge all-fixes, Coraine). A 404 on every part stays one 404, any other failure stays in the 207. Unknown value = fatal. |
| `ANTARES_IMPLICIT_TENANTS` | `with-upsert` | Behaviour profile: which writes create an unknown Tenant (TS 104 175 7.8.2). `with-upsert`: the operations 7.8.2 lists plus Batch Upsert; `listed`: only the 7.8.2 list, so Batch Upsert on an unknown Tenant answers NonexistentTenant (Coraine tenant_implicit_create). Unknown value = fatal. |
| `ANTARES_TEMPORAL_RETENTION_DAYS` | unset (keep forever) | Temporal history retention; the sweep job prunes older attribute instances. Applies to the temporal half wherever it lives: a `file` store with `postgres` history still runs the job. |
| `ANTARES_TEMPORAL_DEDUP` | `on` | `off` records a history row for every admitted write; on, the Postgres stores record a new observation only (Storage, "What enters history"). |
| `ANTARES_TEMPORAL_INSTANCE_LIMIT` | `1000` | Most instances of one Attribute a temporal response serves before it answers 206 with a Content-Range (6.3.10): a week of a 10-minute sensor. The ETSI suite's brokers set `9`, since its 206 cases create 20 instances. |
| `ANTARES_MAX_LIMIT` | `1000` | Most items one page answers (`limit`; past it 403 TooManyResults): every paged list, Entities (`GET /entities`, `POST /entityOperations/query`), Temporal Evolutions (`GET /temporal/entities`, `POST /temporal/entityOperations/query`), Subscriptions, Context Source Registrations and their Subscriptions, and the admin dead letters. |
| `ANTARES_DEFAULT_LIMIT` | `100` | Items a page of the same lists answers when the request names no `limit`; a `Link: rel="next"` names the next page. Never above `ANTARES_MAX_LIMIT`. |
| `ANTARES_TEMPORAL_DEFAULT_INSTANCES` | `100` | Instances of one Attribute a temporal response serves when the request names no `lastN`/`firstN`; past it the answer is 206 with a Content-Range, as at the ceiling. Never above `ANTARES_TEMPORAL_INSTANCE_LIMIT` (the ETSI brokers at `9` stay at 9). |
| `ANTARES_SWEEP_SECS` | `900` | Cadence of the background GC sweep (expired entities/registrations, 4.22) — identical across store modes. |

## NATS scale-out

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_NATS_URL` | — (required for `bus=nats`) | NATS server URL; JetStream streams and the subscription KV bucket are asserted at startup. |
| `ANTARES_NATS_REPLICAS` | `1` | JetStream replica count for streams/KV (set 3 on a 3-node NATS cluster). |
| `ANTARES_OUTBOX_DRAIN` | on | `off` disables the notification outbox drainer in this process (crash-drill lever / dedicated-drainer split). |

## Federation & egress hardening

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_EGRESS_ALLOW_PRIVATE` | `true` | Broker-initiated HTTP and MQTT (notifications, forwards, `@context` fetches) may reach loopback, link-local and RFC 1918 destinations. Set `false` (or `0`) on an internet-exposed deployment to deny those together with carrier-grade NAT (`100.64.0.0/10`), `0.0.0.0/8`, the IETF assignment and benchmarking blocks and the reserved space above `240.0.0.0`. The cloud instance-metadata endpoints are refused whatever this is set to, in every IPv6 spelling. The scheme allowlist, redirect cap, DNS pinning and response-size caps apply regardless. A refused delivery is booked as a failure (`lastFailure`, `status: failed`) and never retried. |
| `ANTARES_EGRESS_PRIVATE_HOSTS` | unset | Comma-separated host names that broker-initiated HTTP and MQTT may still reach at a private address while `ANTARES_EGRESS_ALLOW_PRIVATE=false`: the in-cluster peers, such as a gateway Service or a federated context source. Names compare case-insensitively without a trailing dot; empty members and address literals are ignored (an address is judged by its range). A listed name never reaches loopback, `0.0.0.0/8` or instance metadata. Every unlisted private destination stays refused. Read once at start. |
| `ANTARES_FED_FANOUT` | `8` | Concurrent forwards per distributed read (4.3.6.1 orders the merge, not the requests). |
| `ANTARES_FED_INFLIGHT` | `256` | Forwarded requests in flight for the whole process; callers over the cap wait. Bounds the buffers and connections open federated queries hold (6 000 open queries × 34 sources once reached 7.7 GB). |
| `ANTARES_MAX_FED_RESPONSE_BYTES` | `16777216` (16 MiB) | Ceiling on one forwarded response body — one misbehaving peer cannot balloon broker memory. Over-cap parts fail as warning 111 (Table 6.3.17-1). |
| `ANTARES_MAX_BATCH_ITEMS` | `1000` | Batch entity-count cap (DoS bound; the spec sets none). Raise for trusted bulk producers. |
| `ANTARES_MAX_BODY_BYTES` | `4194304` (4 MiB) | Request body cap, answered with a bare 413 (6.3.4). One number governs the extractor limit and the bounds wall. |
| `ANTARES_POLICY` | `allow-all` | The policy engine every operation is asked about (ADR-0020). The shipped binary is built with `allow-all` alone, which decides nothing; an unknown name is fatal at startup and names the shelf the binary was built with, so a typo cannot quietly serve every request wide open. An engine that refuses is answered `403` with the ProblemDetails type `urn:antares:error:AccessDenied` and the engine's own reason — this broker's own URN, because Table 6.3.2-1 names no access-denied error and none is invented under the ETSI namespace. An engine that narrows instead conjoins its condition into the query the store runs and drops the members it named from every document served; a read narrowed that way answers `Antares-Results-Restricted: true` when the engine asked for the marker, and is otherwise silent. A single Entity outside the narrowing answers `404` like an absent one, so a caller cannot tell the two apart. The engine is also asked about each notification before it is sent: it may drop one — which is no delivery attempt, so `timesSent` and `lastNotification` do not move — or project the entities it carries. |
| `ANTARES_POLICY_SUBJECT_HEADERS` | unset (no headers) | Comma list of request headers copied into the subject the engine is given, matched case-insensitively. The broker never interprets them and never lets them leave: they are stripped from forwarded requests and absent from notifications, dead letters and logs. One copy is persisted: a Subscription, an EntityMap and a Snapshot each keep their creator's headers, because all three are used again after the request that made them — 5.8.6 delivery is broker-initiated, a snapshot's fill runs once its request has been answered, and an EntityMap presented by a different subject is treated as one that cannot be accessed (5.5.14), so a new one is built for that request. That copy is a broker-internal member — no representation renders it, no client can set it, and the 5.8.1.4 copy forwarded to a Context Source is stripped of it — but it does live in the store, so name a header that identifies the subscriber, not one that authenticates them. An embedding binary that sets `subject_required` on the state makes a request that carries none of them a control-plane call: the NGSI-LD API serves it only with the `ANTARES_ADMIN_TOKEN_FILE` bearer and answers `401` otherwise, or always when no admin token is configured. |
| `ANTARES_POLICY_TIMEOUT_MS` | `250` | How long the policy engine has to answer one request before the seam stops waiting and denies (ADR-0020). The engine the broker ships allows everything and never waits, so this matters only to a deployment that attached its own. |
| `ANTARES_CORS_ORIGINS` | unset (no CORS headers) | Browser origins allowed, comma-separated, or `*`. Preflights are answered for every method and header; `Link`, `NGSILD-Tenant` and `NGSILD-Results-Count` are exposed. |
| `ANTARES_API_SURFACES` | `admin` | Comma list of HTTP surfaces mounted beside the NGSI-LD API root, each under its own reserved prefix (`admin` serves `/q`). An unknown name is fatal at startup and names the shelf the binary was built with; a selection that leaves out `admin` serves no `/q` at all, probes included. |
| `ANTARES_ADMIN_TOKEN_FILE` | unset (no credential) | A file holding the bearer secret every `/q` route except health and readiness then requires (`Authorization: Bearer <secret>`, at least 32 bytes, one trailing line break ignored; [Admin API](admin-api.md)). Read once at start: an unreadable file or a shorter secret is fatal, so a configured credential never silently becomes none. Unset, `/q` asks for nothing and the broker logs one warning at start. |
| `ANTARES_ADMIN_METRICS` | `token` | `open` serves `/q/metrics` without the admin secret, for a scraper that cannot send one; `token` keeps it behind the secret like the rest of `/q`. Any other value is fatal at startup. Without `ANTARES_ADMIN_TOKEN_FILE` it changes nothing. |
| `ANTARES_HISTORY_BACKFILL` | unset (off) | `on` makes a value older than the current one a backfill into history only, leaving the current state as it is. Off keeps NGSI-LD's last-write-wins, which the ETSI suite checks. |
| `ANTARES_EXTRA_CA_FILE` | unset | PEM bundle of ADDITIONAL trust anchors for egress TLS (private CAs). Egress TLS trusts the host's certificate store (`/etc/ssl/certs` on Linux; the shipped image carries one), and this widens it. Verification itself is never disableable. |

## Notification delivery

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_NOTIFY_ATTEMPTS` | `1` | Delivery attempts per notification, first one included. `1` is 5.8.6 as written: one send, the outcome booked. Higher values retry on their own task with exponential backoff; the retries never move `timesSent` again. |
| `ANTARES_NOTIFY_BACKOFF_MS` | `1000` | Delay before the first retry; doubles per retry (±20 % jitter, 60 s ceiling). |
| `ANTARES_NOTIFY_MAX_AGE_SECS` | `300` | No retry starts later than this after the first attempt. When the attempts or the age run out the notification becomes a dead letter (`/q/dead-letters`, see [operations](operations.md#notification-delivery)). |
| `ANTARES_DELIVERY_WIDTH` | `64` | Notifications in flight at once across the whole broker. A slot is held until the endpoint answers, so the number that fits depends on the subscribers: local sinks free a slot in milliseconds, a remote endpoint sitting at its 30 s timeout (Table 5.2.15-1) holds one for the whole timeout. Published as `deliveryWidth` in `/q/health`. |
| `ANTARES_DELIVERY_WIDTH_PER_TENANT` | `8` | Of that width, what one tenant may hold, capped at the width. Without it a single tenant with enough dead endpoints holds every slot and nothing leaves the broker for anyone else. Published as `deliveryWidthPerTenant`. |
| `ANTARES_BATCH_INFLIGHT` | `8` | Batch writes running at once, whatever their size. Each holds a database connection while it runs, so keep it under `ANTARES_PG_POOL`: the rest of the pool stays with the reads, and a stream of small upserts cannot make a visitor wait for a connection. A batch waits for a slot inside the same 20 s as for its byte share, then gets `503` with `Retry-After`. Published as `batchInflight` in `/q/health`. |
| `ANTARES_BATCH_INFLIGHT_BYTES` | `4194304` (4 MiB) | Batch write bodies (create, upsert, update, merge) in flight at once, in bytes. A batch costs about a hundred times its body while it runs, so a 1000-entity upsert of 3 MB holds about 300 MB. A batch that does not fit waits up to 20 s for running ones to finish, then gets a bare `503` with `Retry-After`, counted in `antares_batch_overloaded_total`. One batch always runs alone, however large. Published as `batchInflightBytes` in `/q/health`. |
| `ANTARES_DELIVERY_LANE_BYTES` | `134217728` (128 MiB) | Estimated heap all subscription delivery lanes may hold at once, across every tenant. A change group that would push the total past it is not queued: that subscription's lane counts a drop (`antares_notification_lane_dropped_total`), as a full lane does. One group is always admitted into empty lanes. The held bytes are the gauge `antares_notification_lane_bytes`; published as `deliveryLaneBytes` in `/q/health`. |

## Lifecycle & observability

| Variable | Default | Effect |
|---|---|---|
| `ANTARES_HEADER_READ_TIMEOUT_MS` | `10000` | A connection that has not finished its request HEAD within this window is closed (slow-loris bound). |
| `ANTARES_MAX_CONNECTIONS` | `10000` | Concurrent-connection ceiling; further accepts are dropped. Counts keep-alive and LB health-check connections too — size accordingly. |
| `ANTARES_DISCOVERY_SCAN_MAX` | `100000` | Documents one unpaginated whole-tenant fold may read or hold: the `/types`/`/attributes` discovery folds (5.7.5-5.7.10 define no pagination) the registration query (5.10.2.4 filters before it pages, so the whole match set is held) and the candidate walk of a paged geoquery or `scopeQ`. Past it the answer is 403 TooManyResults (5.5.6) instead of an unbounded scan. Published as `maxFoldDocs` in `/q/health`. |
| `ANTARES_DRAIN_DELAY_MS` | `2000` | Rolling update, step 2: keep serving this long after `/q/health` flips to 503 — the load balancer's notice window, sized so a health poll actually observes the 503 before the socket goes. |
| `ANTARES_DRAIN_DEADLINE_SECS` | `20` | Bound on waiting for in-flight connections during drain. Container `stop_grace_period` / `terminationGracePeriodSeconds` MUST exceed delay + deadline. |
| `ANTARES_TELEMETRY` | off | Any value but an off spelling enables the metrics recorder and, with the endpoint, the OTLP span and log pipelines. |
| `ANTARES_OTLP_ENDPOINT` | unset | OTLP/HTTP collector for traces and logs, e.g. `http://collector:4318/v1/traces`; log records go to the `v1/logs` twin of that URL with the same resource attributes. Unset costs nothing. |

Compile-time bounds (no variable sets them; spec-shaped rejections): URI
8 KiB → 414, JSON depth 64 → 400. The body cap is `ANTARES_MAX_BODY_BYTES`
above. Every bound in force is reported live by `GET /q/health` under
`limits`.

Node-shim (wasm tier) extras: `ANTARES_FILE` (redb path per shim) — see
the [browser guide](wasm.md).
