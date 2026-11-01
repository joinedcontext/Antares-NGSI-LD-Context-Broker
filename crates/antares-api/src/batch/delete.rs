// SPDX-License-Identifier: EUPL-1.2
//! Batch Entity Delete (5.6.10): the per-member decision, one multi-row
//! DELETE, the 5.6.10.4 distributed ladder and the S/E outcome.

use super::*;

// ---------- POST /entityOperations/delete (5.6.10) ----------

pub async fn batch_delete(
    State(st): State<AppState>,
    CleanParams(params): CleanParams,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    batch_delete_inner(&st, &params, &headers, &body)
        .await
        .unwrap_or_else(|e| e.into_response())
}

async fn batch_delete_inner(
    st: &AppState,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
    body: &[u8],
) -> ApiResult<Response> {
    let tenant = tenant_from(headers)?;
    check_params(params, &["local"])?;
    // A delete batch holds a connection as long as a write batch does.
    let _admitted = crate::bounds::admit_batch(body.len())
        .await
        .ok_or(ApiError::Overloaded(crate::negotiate::RETRY_AFTER_SECONDS))?;
    let ids = delete_ids(headers, body)?;
    let mut out = BatchOutcome::new();
    // ADR-0020 per member, as for the write batches: each id
    // is decided as the single-Entity delete decides it, with the types
    // stored under it. The body has to be read to know them, so the gate
    // sits behind the parse the way the single-Entity delete sits behind
    // its path parameter. A refused id lands in E, the rest are deleted.
    let mut ids: Vec<&Value> = ids.iter().collect();
    let mut conds = Conditions::default();
    decide_deletes(st, &tenant, headers, &mut ids, &mut out, &mut conds).await?;
    let spec = crate::registry::CsrSpec {
        ids: Some(
            ids.iter()
                .filter_map(|v| v.as_str())
                .map(str::to_owned)
                .collect(),
        ),
        ..Default::default()
    };
    let regs =
        match crate::federation::write_plan(st, &tenant, &spec, &st.loader.core(), params, headers)
            .await?
        {
            crate::federation::WritePlan::Answered(r) => return Ok(*r),
            crate::federation::WritePlan::Forward(regs) => regs,
        };
    conds.unforwarded(!regs.is_empty())?;
    // One multi-row DELETE for the whole batch; flags in input order.
    let id_strs: Vec<String> = ids
        .iter()
        .copied()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let (flags, failed) = delete_local(st, &tenant, &id_strs, &conds).await?;
    // a store error after a committed delete: the ids from the failing one
    // on were not attempted, and go to E with that error (5.6.10.4 reports
    // each Entity) rather than a 5xx that hides the deletes that landed
    let attempted = flags.len();
    let mut flags = flags.into_iter();
    let mut local_ok: std::collections::HashSet<String> = Default::default();
    let mut local_miss: Vec<String> = Vec::new();
    let mut seen = 0usize;
    for id in ids {
        let Some(id) = id.as_str() else {
            out.errors.push(err_entry(
                None,
                &NgsiError::BadRequestData("entity id must be a string".into()),
            ));
            continue;
        };
        seen = seen.saturating_add(1);
        if let Some(e) = failed.as_ref().filter(|_| seen > attempted) {
            out.errors.push(err_entry(Some(id), e));
            continue;
        }
        if flags.next().unwrap_or(false) {
            // 5.6.10 deletes carry the same temporal-deletion semantics
            // as 5.6.6 — without this, batch-deleted entities live on in
            // the temporal store (the reset's batch delete leaked
            // every prior suite's Buildings into the orderBy queries).
            crate::history::mirror_delete_entity(st, &tenant, id).await;
            local_ok.insert(id.to_owned());
        } else {
            // proxied entities may live remotely only (4.3.6.3) — a
            // local miss becomes an error only if no forward covers it.
            local_miss.push(id.to_owned());
        }
    }
    let id_strs = if failed.is_some() {
        id_strs.get(..attempted).unwrap_or(&id_strs)
    } else {
        &id_strs
    };
    let remote = forward_deletes(st, &tenant, headers, &regs, id_strs).await;
    delete_outcome(&mut out, id_strs, &local_ok, local_miss, remote);
    Ok(out.respond(&tenant, StatusCode::NO_CONTENT, false))
}

/// The Batch Entity Delete body: a non-empty JSON array without null items,
/// inside the batch ceiling.
fn delete_ids(headers: &HeaderMap, body: &[u8]) -> ApiResult<Vec<Value>> {
    let ct = content_type(headers)?;
    if ct != "application/json" && ct != "application/ld+json" {
        return Err(ApiError::Bare(StatusCode::UNSUPPORTED_MEDIA_TYPE));
    }
    let value: Value = serde_json::from_slice(body)
        .map_err(|e| NgsiError::InvalidRequest(format!("body is not valid JSON: {e}")))?;
    let ids = match value {
        Value::Array(a) if !a.is_empty() => a,
        _ => {
            return Err(NgsiError::BadRequestData(
                "batch delete body must be a non-empty array".into(),
            )
            .into())
        }
    };
    // 5.6.10.4: a null item fails the whole request
    if ids.iter().any(Value::is_null) {
        return Err(
            NgsiError::BadRequestData("batch array must not contain null items".into()).into(),
        );
    }
    // Batch entity count cap — the same ceiling the write batches carry,
    // and the one that bounds the per-id forwarded DELETE fan-out.
    if ids.len() > *crate::bounds::MAX_BATCH_ITEMS {
        return Err(NgsiError::BadRequestData(format!(
            "batch of {} exceeds the {}-entity limit",
            ids.len(),
            *crate::bounds::MAX_BATCH_ITEMS
        ))
        .into());
    }
    Ok(ids)
}

/// The engine's verdict per id: a refused id leaves `ids` for E, an allowed
/// one leaves its condition in `conds`; every id refused is the 403.
async fn decide_deletes(
    st: &AppState,
    tenant: &antares_model::TenantId,
    headers: &HeaderMap,
    ids: &mut Vec<&Value>,
    out: &mut BatchOutcome,
    conds: &mut Conditions,
) -> ApiResult<()> {
    let Some(engine) = &st.policy else {
        return Ok(());
    };
    // One read for every id, failing closed as `pre_image`.
    let wanted: Vec<&str> = ids.iter().filter_map(|v| v.as_str()).collect();
    let stored = if wanted.is_empty() {
        HashMap::new()
    } else {
        st.store.get_entities(tenant, &wanted).await?
    };
    let pres: Vec<crate::policy::PreImage> = ids
        .iter()
        .map(|v| match v.as_str() {
            Some(id) => crate::policy::image(stored.get(id).cloned(), None),
            None => crate::policy::PreImage::default(),
        })
        .collect();
    let one: Vec<Vec<&str>> = ids
        .iter()
        .map(|v| v.as_str().into_iter().collect())
        .collect();
    let ops: Vec<crate::policy::Operation<'_>> = one
        .iter()
        .zip(&pres)
        .map(|(ids, pre)| crate::policy::Operation {
            ids,
            types: &pre.types,
            stored: pre.stored.as_ref(),
            ..crate::policy::Operation::new("5.6.10")
        })
        .collect();
    let asking = crate::snapshots::asking_tenant(st, tenant).await;
    let refused = crate::policy::gate_each(engine.as_ref(), &asking, headers, &ops).await;
    if let Some(why) = whole_refusal(&refused) {
        return Err(crate::policy::Denied(why).into());
    }
    let mut verdicts = refused.into_iter();
    ids.retain(|id| match verdicts.next() {
        Some(Err(why)) => {
            out.errors.push(denied_entry(id.as_str(), &why));
            false
        }
        Some(Ok(granted)) => {
            conds.add(id.as_str(), &granted);
            true
        }
        None => true,
    });
    Ok(())
}

/// The local deletes, a flag per id in input order. Under a policy view
/// or an engine's condition each delete reads its Entity under the lock and
/// leaves one out of the view, or one the condition does not select, which
/// then answers as absent.
///
/// Those deletes commit one by one. A store error before any of them
/// deleted anything is the request's answer; one after is returned beside
/// the flags of the ids attempted before it, and the rest are not tried.
async fn delete_local(
    st: &AppState,
    tenant: &antares_model::TenantId,
    id_strs: &[String],
    conds: &Conditions,
) -> Result<(Vec<bool>, Option<NgsiError>), NgsiError> {
    let judged = !conds.0.is_empty();
    if !judged {
        return Ok((st.store.batch_delete(tenant, id_strs).await?, None));
    }
    let mut flags = Vec::with_capacity(id_strs.len());
    for id in id_strs {
        let keep = |d: &Value| conds.holds(id, d);
        match st.store.delete_entity_if(tenant, id, &keep).await {
            Ok(deleted) => flags.push(deleted),
            Err(e) if !flags.contains(&true) => return Err(e),
            Err(e) => return Ok((flags, Some(e))),
        }
    }
    Ok((flags, None))
}

/// 5.6.10.4 support ladder: deleteBatch -> one batch forward; else
/// per-entity Delete Entity forwards; else proxy modes get Conflict
/// per entity. Remote outcomes merge into S/E (never opaque parts).
async fn forward_deletes(
    st: &AppState,
    tenant: &antares_model::TenantId,
    headers: &HeaderMap,
    regs: &[crate::federation::FedReg],
    id_strs: &[String],
) -> Remote {
    let mut remote = Remote::default();
    if regs.is_empty() {
        return remote;
    }
    let ctx_url = crate::federation::ctx_link_url(headers, &st.loader.core().source);
    for reg in regs {
        // "Remove from IN all Entities not matched by CSR" — an
        // id-scoped registration (exact ids OR idPattern, 5.12)
        // only receives its own ids (4.3.6.1).
        let sent_ids: Vec<String> = id_strs
            .iter()
            .filter(|i| reg.can_match_id(i))
            .cloned()
            .collect();
        if sent_ids.is_empty() {
            continue;
        }
        if reg.supports("deleteBatch") {
            let sent_vals: Vec<Value> = sent_ids.iter().cloned().map(Value::String).collect();
            let (status, body, _) = crate::federation::forward(
                st,
                reqwest::Method::POST,
                format!("{}/ngsi-ld/v1/entityOperations/delete", reg.endpoint),
                &[],
                headers,
                tenant,
                reg,
                &ctx_url,
                Some(Value::Array(sent_vals)),
            )
            .await;
            merge_remote_batch(
                status,
                &body,
                &sent_ids,
                false,
                &mut remote.ok,
                &mut remote.err,
            );
        } else if reg.supports("deleteEntity") {
            for id in &sent_ids {
                let status = forward_delete(st, tenant, headers, reg, &ctx_url, id).await;
                if (200..300).contains(&status) && status != 207 {
                    remote.ok.push((id.clone(), false));
                } else {
                    remote.err.push(err_remote(
                        Some(id),
                        status,
                        &format!("forwarded delete returned {status}"),
                    ));
                }
            }
        } else if reg.is_proxy() {
            // 5.6.10.4 last rung: an exclusive or redirect CSR that
            // supports neither delete operation contributes an Error
            // of type Conflict for each Entity in IN.
            for id in &sent_ids {
                remote.err.push(err_entry(
                    Some(id),
                    &NgsiError::Conflict(
                        "registration does not accept the operation deleteBatch".into(),
                    ),
                ));
            }
        }
    }
    remote
}

/// One Delete Entity (5.6.6) forwarded to `reg`; the status it answered.
async fn forward_delete(
    st: &AppState,
    tenant: &antares_model::TenantId,
    headers: &HeaderMap,
    reg: &crate::federation::FedReg,
    ctx_url: &str,
    id: &str,
) -> u16 {
    let (status, _, _) = crate::federation::forward(
        st,
        reqwest::Method::DELETE,
        format!(
            "{}/ngsi-ld/v1/entities/{}",
            reg.endpoint,
            crate::federation::path_segment(id)
        ),
        &[],
        headers,
        tenant,
        reg,
        ctx_url,
        None,
    )
    .await;
    status
}

/// S in input order (local first occurrence), then remote-only; E gets the
/// remote errors, then each local miss no forward resolved.
fn delete_outcome(
    out: &mut BatchOutcome,
    id_strs: &[String],
    local_ok: &std::collections::HashSet<String>,
    local_miss: Vec<String>,
    remote: Remote,
) {
    let remote_success: std::collections::HashSet<String> =
        remote.ok.iter().map(|(id, _)| id.clone()).collect();
    for id in id_strs.iter().filter(|id| local_ok.contains(*id)) {
        out.succeed(id);
    }
    for (id, _) in &remote.ok {
        out.succeed(id);
    }
    let erred: Vec<String> = remote
        .err
        .iter()
        .filter_map(|e| e.get("entityId").and_then(Value::as_str).map(str::to_owned))
        .collect();
    out.errors.extend(remote.err);
    // 5.6.10.4 local step (5.6.6 limited to local): a missed occurrence
    // is ResourceNotFound unless a FORWARD resolved that id — a local
    // success does not excuse it (5.5.11.4 duplicate-id semantics: the
    // second occurrence of the same id errors).
    for id in local_miss {
        if !remote_success.contains(&id) && !erred.contains(&id) {
            out.errors.push(err_entry(
                Some(&id),
                &NgsiError::ResourceNotFound(format!("entity {id} not found")),
            ));
        }
    }
}
