// SPDX-License-Identifier: EUPL-1.2
//! Query Entities over POST (5.7.2, resource 6.23): the Query body read as
//! the GET twin reads its URI, then paged, shaped and joined.

use super::*;

// ---------- POST /entityOperations/query (6.23) ----------

pub async fn batch_query(
    State(st): State<AppState>,
    CleanParams(params): CleanParams,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match batch_query_inner(&st, &params, &headers, &body).await {
        Ok(r) => r,
        Err(e) => e.into_response(),
    }
}

async fn batch_query_inner(
    st: &AppState,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
    body: &[u8],
) -> ApiResult<Response> {
    let tenant = tenant_from(headers)?;
    check_params(
        params,
        &["limit", "offset", "count", "options", "format", "local"],
    )?;
    // POST query IS Query Entities: geo+json is a valid Accept here (6.3.15)
    let accept = parse_accept_geo(headers)?;
    let parsed = parse_body(st, headers, body, BodyKind::Standard).await?;
    let q = parsed.object(NgsiError::BadRequestData(
        "query body must be an object".into(),
    ))?;
    if q.get("type").and_then(Value::as_str) != Some("Query") {
        return Err(NgsiError::BadRequestData("body type must be Query (5.2.23)".into()).into());
    }
    // Convert Query members into virtual params reusing the GET filter path.
    let mut vp: HashMap<String, String> = HashMap::new();
    query_doc_params(q, false, &mut vp)?;
    if let Some(l) = params.get("local") {
        vp.insert("local".into(), l.clone());
    }
    // ADR-0020: the engine is shown every member of the Query body
    let named = crate::entities::ReadMembers::with_ctx(st, &vp, &parsed.ctx);
    let filter = gate!(
        st, &tenant, headers, "5.7.2",
        q: named.q.as_ref(),
        scope_q: vp.get("scopeQ").map(String::as_str),
        types: &named.types,
        attrs: &named.attrs,
        geo: named.geo.as_ref(),
    )
    .await?;
    refuse_too_wide(&vp)?;
    // ADR-0020: the engine's narrowing joins the Query's own filters here,
    // after the "too wide" judgement — which is about what the client asked
    // for. Everything below reads the narrowed query, the forwarded one
    // included.
    // what the client's own Query reads, before the engine's q joins it
    //
    let narrow = crate::repr::narrowing_reading(&filter, &vp, &parsed.ctx)?;
    let vp = filter.narrow_params(&vp, &parsed.ctx)?;
    let mut page_params = params.clone();
    page_params.extend(vp.clone());
    // 5.7.2.4: the validation its GET twin runs, on the same parameters
    crate::entities::check_query_params(st, &tenant, headers, &parsed.ctx, &page_params, accept)
        .await?;
    let q_ast = vp
        .get("q")
        .map(|q| antares_ql::parse_narrowed_q(q))
        .transpose()?;
    let join = crate::entities::check_query_join(&vp, q_ast.as_ref())?;
    // 5.2.33: several EntitySelectors are a union no single flat filter
    // states; their candidates are matched against it here
    let selectors = Selectors::of(q)?;
    let fed = fed_matches(st, &tenant, headers, &parsed.ctx, &vp).await?;
    // the page is read from the store, as the GET twin reads it, whenever
    // the flat filter is the whole Query
    let (offset, limit, _) = crate::paging::page_params(st, &page_params)?;
    let push =
        selectors.is_none() && crate::entities::page_pushdown_allowed(fed.is_empty(), &page_params);
    let filtered = crate::entities::filter_entities_paged(
        st,
        &tenant,
        &page_params,
        &parsed.ctx,
        fed,
        crate::entities::Pushdown {
            page: push.then_some((offset, limit)),
            proj: None,
            order_keys: None,
        },
        Some(&narrow),
    )
    .await?;
    let paged = filtered.paged.then_some(filtered.total);
    let mut matches = filtered.docs;
    if let Some(sel) = &selectors {
        matches.retain(|d| sel.matches(d, &parsed.ctx));
    }
    // 5.2.43 ordering: same 4.23 keys as the GET twin, applied pre-pagination
    if let Some(spec) = page_params.get("orderBy") {
        crate::repr::order_as_shown(&mut matches, Some(&narrow), |m| {
            crate::paging::order_entities(m, spec, &page_params, &parsed.ctx)
        })?;
    }
    // 6.3.10: the Links name this resource and its own URI parameters; the
    // Query itself travels in the body the client sends again
    let path = "/ngsi-ld/v1/entityOperations/query";
    let (page, count_hdr, links) = match paged {
        Some(total) => {
            let total = total.unwrap_or(matches.len());
            crate::paging::paginate_pre(st, params, matches, path, accept, total)?
        }
        None => paginate(st, params, matches, path, accept)?,
    };
    // body members (pick/omit/attrs/lang/datasetId) shape the representation
    // exactly like their 6.3.7 query-parameter twins
    let mut repr = parse_repr(&page_params, &parsed.ctx)?;
    crate::repr::narrow_repr(&mut repr, &filter);
    crate::entities::check_linked_projection(&repr, &join)?;
    let (rendered_from, mut payload): (Vec<&Value>, Vec<Value>) =
        shaped_page(&page, &repr, &parsed.ctx).into_iter().unzip();
    if let Some((mode, level)) = &join {
        // 4.5.23.1 bounds the WIDTH of the retrieval per REQUEST, so one
        // allowance is spent across the whole page, exactly as the GET twin
        // spends it. Minting a fresh allowance per payload Entity multiplies
        // the ceiling by the page size, which is the request the ceiling
        // exists to bound: a page of densely linked Entities.
        let scope = JoinScope {
            st,
            tenant: &tenant,
            ctx: &parsed.ctx,
            repr: &repr,
            level: *level,
            held: crate::entities::contained_by(&page_params),
        };
        match mode.as_str() {
            "inline" => {
                let remote = crate::federation::active(&page_params).then_some(headers);
                scope.inline(&mut payload, &rendered_from, remote).await;
            }
            "flat" => payload.extend(scope.flat(&page).await),
            _ => {}
        }
    }
    let out = if accept == Accept::GeoJson {
        crate::repr::to_geojson_collection(payload, None)
    } else {
        Value::Array(payload)
    };
    // 6.3.6: the json answer's Link names the @context it was compacted in
    let served = crate::contexts::linkable(st, &tenant, headers, &parsed.ctx, accept).await?;
    let mut resp = respond_prefer(StatusCode::OK, out, &served, accept, &tenant, headers);
    attach_paging(&mut resp, count_hdr, &links);
    // 6.4.3.2: entityMap=true — the EntityMap for this Query is created, as
    // on the GET twin, and named in NGSILD-EntityMap with 201 Created
    if page_params.get("entityMap").map(String::as_str) == Some("true") {
        let map = crate::entities::build_query_map(
            st,
            &tenant,
            headers,
            &parsed.ctx,
            &page_params,
            &filter,
        )
        .await?;
        *resp.status_mut() = StatusCode::CREATED;
        if let Some(id) = map.get("id").and_then(Value::as_str) {
            if let Ok(v) = format!("/ngsi-ld/v1/entityMaps/{id}").parse() {
                resp.headers_mut().insert("NGSILD-EntityMap", v);
            }
        }
    }
    filter.mark(&st.host_alias, resp.headers_mut());
    Ok(resp)
}

/// 5.2.33: the EntitySelectors of a Query, each a (type, id, idPattern)
/// triple, `id` taking precedence over `idPattern` within its selector.
/// [`query_doc_params`] states one selector exactly as flat parameters;
/// several are only a superset there, so their candidates are matched here.
struct Selectors(Vec<Selector>);

struct Selector {
    types: String,
    ids: Vec<String>,
    pattern: Option<String>,
}

impl Selectors {
    /// None for a Query with at most one selector (already exact); the
    /// members were validated by `query_doc_params`.
    fn of(q: &serde_json::Map<String, Value>) -> ApiResult<Option<Self>> {
        let Some(Value::Array(es)) = q.get("entities") else {
            return Ok(None);
        };
        if es.len() < 2 {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(es.len());
        for e in es {
            let types = e.get("type").and_then(Value::as_str).unwrap_or_default();
            let ids = match e.get("id") {
                Some(Value::String(s)) => vec![s.clone()],
                Some(Value::Array(a)) => a
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
                _ => Vec::new(),
            };
            let pattern = e
                .get("idPattern")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(p) = &pattern {
                antares_ql::regex::compile(p)
                    .map_err(|_| NgsiError::BadRequestData(format!("invalid idPattern {p:?}")))?;
            }
            out.push(Selector {
                types: types.to_owned(),
                ids,
                pattern,
            });
        }
        Ok(Some(Self(out)))
    }

    /// Whether `doc` is selected by any selector.
    fn matches(&self, doc: &Value, ctx: &antares_jsonld::Context) -> bool {
        let id = doc.get("id").and_then(Value::as_str).unwrap_or_default();
        let types: Vec<&str> = match doc.get("type") {
            Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
            Some(Value::String(t)) => vec![t.as_str()],
            _ => Vec::new(),
        };
        self.0.iter().any(|s| {
            let typed = s.types == "*" || antares_ql::type_selection_matches(&s.types, &types, ctx);
            let named = if !s.ids.is_empty() {
                s.ids.iter().any(|i| i == id)
            } else if let Some(p) = &s.pattern {
                antares_ql::regex::compile(p).is_ok_and(|r| r.is_match(id))
            } else {
                true
            };
            typed && named
        })
    }
}

/// Each page Entity in the requested representation, beside the Entity it
/// was rendered from; one a `pick` leaves empty is dropped.
fn shaped_page<'p>(
    page: &'p [Value],
    repr: &crate::repr::Repr,
    ctx: &antares_jsonld::Context,
) -> Vec<(&'p Value, Value)> {
    page.iter()
        .filter_map(|doc| {
            let shaped = apply(doc, repr);
            if repr.pick.is_some() && shaped.as_object().is_some_and(|o| o.is_empty()) {
                return None;
            }
            Some((doc, crate::repr::compact_for(repr, &shaped, ctx)))
        })
        .collect()
}

/// 5.7.2.4: the Query must name what it reads, and every filter in it must
/// parse.
fn refuse_too_wide(vp: &HashMap<String, String>) -> ApiResult<()> {
    // 5.7.2.4 (p. 201): "At least one of the following input data shall be
    // provided: a) selector of Entity Types; b) list of Attribute names,
    // including at least one non-system Attribute; c) NGSI-LD Query,
    // including at least one non-system Attribute; d) NGSI-LD GeoQuery;
    // e) local scope. If none of the above is provided, then an error of type
    // BadRequestData shall be raised (too wide query)." Query Entities is ONE
    // operation: the resource that carries the Query in a body answers to the
    // same behaviour clause as the one that carries it in the URI, and
    // without this a bare `{"type":"Query"}` reads the whole tenant and fans
    // the filterless query out to every matching registration.
    let q_ast = vp.get("q").map(|q| antares_ql::parse_q(q)).transpose()?;
    // 5.7.2.4: "if ... the query, geoquery or context source filter are not
    // syntactically valid (as per the referred clauses 4.9 and 4.10) an error
    // of type BadRequestData shall be raised." Registration matching parses
    // the csf again and drops what it cannot parse, which widens the fan-out
    // to the Context Sources the filter was there to exclude — so it is
    // refused here, the way the twin carrying the Query in the URI refuses it.
    if let Some(csf) = vp.get("csf") {
        antares_ql::parse_q(csf)?;
    }
    if !crate::entities::qualifies_non_wide(vp, q_ast.as_ref()) {
        return Err(NgsiError::BadRequestData(
            "query needs at least one of type, attrs, q, georel (5.7.2.4)".into(),
        )
        .into());
    }
    Ok(())
}

/// The matches the registrations add, unless this request came round the
/// federation loop.
async fn fed_matches(
    st: &AppState,
    tenant: &antares_model::TenantId,
    headers: &HeaderMap,
    ctx: &antares_jsonld::Context,
    vp: &HashMap<String, String>,
) -> ApiResult<Vec<(bool, Value)>> {
    Ok(
        if crate::federation::active(vp)
            && !crate::federation::via_loop(
                headers,
                &crate::federation::alias_for(&st.host_alias, tenant),
            )
        {
            // 6.3.17 scopes NGSILD-Warning to GET /entities(/{id}) — collected
            // here for the log only, never emitted on entityOperations/query
            let mut warnings = Vec::new();
            let fed =
                crate::federation::fed_query(st, tenant, headers, ctx, vp, &mut warnings).await?;
            for w in &warnings {
                tracing::debug!("distributed query warning (batch query): {w}");
            }
            fed
        } else {
            Vec::new()
        },
    )
}

/// The 4.5.23 join of one page: what both join modes walk with.
struct JoinScope<'a> {
    st: &'a AppState,
    tenant: &'a antares_model::TenantId,
    ctx: &'a antares_jsonld::Context,
    repr: &'a crate::repr::Repr,
    level: usize,
    held: Vec<String>,
}

impl JoinScope<'_> {
    /// 4.5.23 inline join: the linked Entities inside each payload Entity,
    /// each beside the Entity it was rendered from; `remote` carries the
    /// request headers when the query is federated.
    async fn inline(&self, payload: &mut [Value], stored: &[&Value], remote: Option<&HeaderMap>) {
        let mut budget = crate::repr::MAX_JOIN_LOOKUPS;
        for (p, stored) in payload.iter_mut().zip(stored) {
            crate::repr::inline_join_beyond(
                self.st,
                self.tenant,
                self.ctx,
                self.repr,
                p,
                stored,
                self.level,
                &self.held,
                &mut budget,
                remote,
            )
            .await;
        }
    }

    /// 4.5.23 flat join: the linked Entities the page does not hold, to follow it.
    async fn flat(&self, page: &[Value]) -> Vec<Value> {
        let (st, tenant, ctx, repr, level, held) = (
            self.st,
            self.tenant,
            self.ctx,
            self.repr,
            self.level,
            &self.held[..],
        );
        let mut budget = crate::repr::MAX_JOIN_LOOKUPS;
        let mut linked = std::collections::BTreeMap::new();
        for doc in page {
            crate::repr::collect_flat_beyond(
                st,
                tenant,
                ctx,
                repr,
                doc,
                level,
                &mut linked,
                held,
                &mut budget,
            )
            .await;
        }
        let page_ids: Vec<&str> = page.iter().filter_map(|d| d["id"].as_str()).collect();
        linked
            .into_iter()
            .filter(|(id, _)| !page_ids.contains(&id.as_str()))
            .map(|(_, (ldoc, lrepr))| crate::repr::compact_for(&lrepr, &apply(&ldoc, &lrepr), ctx))
            .collect()
    }
}
