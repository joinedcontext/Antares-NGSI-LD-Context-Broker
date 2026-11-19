#!/usr/bin/env python3
"""Render the ETSI matrix results as a static site.

Usage: etsi-report-site.py <cells-dir> <out-dir>
  <cells-dir> is the ETSI-matrix-results bundle layout: one
  ETSI-cell-<store>/ per store (run-summary.md, gate-status.txt,
  failures.csv, per-suite robot report.html/log.html). Cells come from the
  STORES env when set (a listed-but-missing cell renders red); otherwise
  they are AUTO-DISCOVERED from the bundle, so the Pages fold renders
  whatever the newest matrix actually ran — a 3-cell bundle from before the
  six-cell matrix still renders green instead of inventing missing cells.

Writes <out-dir>/:
  index.html      our suite first: one summary row per cell, then each
                  cell's per-suite rows linking Robot's report/log; then the
                  official ETSI suite as one short table (pass/total and
                  baseline gate per cell, Robot reports folded away)
  badge.json      shields.io endpoint schema ("ETSI 6×1652 green" / red)
  badge-<cell>.json  one endpoint badge PER CELL ("1652/1652")
  badge-official.json  the official suite over every cell that ran it
  badge-official-<cell>.json  the official suite in one cell ("1309/1429")
  <store>/<suite>/report.html|log.html   copied from the bundle
  upstream/<store>/<suite>/report.html|log.html   the official ETSI suite's
                  run per cell (ETSI-upstream-cell-<store>/)

The AntaresSpecificTests row is shown but not counted in a cell's ETSI
total or badge: it is not ETSI-derived.
"""
import glob
import html
import json
import os
import re
import shutil
import sys

cells = sys.argv[1] if len(sys.argv) > 1 else "cells"
out = sys.argv[2] if len(sys.argv) > 2 else "site/reports/latest"
os.makedirs(out, exist_ok=True)

PREFERRED = "memory file postgres timescale postgres-nats timescale-nats".split()
if os.environ.get("STORES"):
    STORES = os.environ["STORES"].split()
else:
    found = [
        os.path.basename(d)[len("ETSI-cell-"):]
        for d in glob.glob(f"{cells}/ETSI-cell-*")
        if os.path.isdir(d)
    ]
    STORES = [s for s in PREFERRED if s in found] + sorted(
        s for s in found if s not in PREFERRED
    )
    if not STORES:  # empty/absent bundle: keep the historical shape (renders red)
        STORES = ["file", "postgres", "timescale"]


OWN = "AntaresSpecificTests"


def find(d, name):
    hits = sorted(glob.glob(f"{d}/**/{name}", recursive=True))
    return hits[0] if hits else None


def read_suites(d, prefix):
    """per-suite rows of one cell's run-summary.md, Robot pages copied to
    <out>/<prefix>/<suite>/"""
    suites = []
    p = find(d, "run-summary.md")
    summary_dir = os.path.dirname(p) if p else d
    if p:
        for name, ok, fail, skip in re.findall(
            r"^\| (?!Suite)(\S+) \| (\d+) \| (\d+) \| (\d+) \|$", open(p).read(), re.M
        ):
            # the robot artifacts for this suite live next to run-summary.md
            link = None
            for f in ("report.html", "log.html"):
                src = find(os.path.join(summary_dir, name), f)
                if src:
                    dst = os.path.join(out, prefix, name, f)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    shutil.copyfile(src, dst)
                    if link is None:
                        link = f"{prefix}/{name}/{f}"
            suites.append((name, int(ok), int(fail), int(skip), link))
    return suites


def etsi_rows(suites):
    return [r for r in suites if r[0] != OWN]


stores = []
for st in STORES:
    d = f"{cells}/ETSI-cell-{st}"
    gate = "MISSING"
    p = find(d, "gate-status.txt")
    if p:
        gate = open(p).read().strip()
    stores.append((st, gate, read_suites(d, st)))

# The official ETSI suite, one cell per store that ran it (auto-discovered:
# a bundle from before the two-suite matrix simply has none).
found = [os.path.basename(d)[len("ETSI-upstream-cell-"):]
         for d in glob.glob(f"{cells}/ETSI-upstream-cell-*") if os.path.isdir(d)]
upstream = []
for st in [s for s in PREFERRED + ["wasm-file"] if s in found] + sorted(
    s for s in found if s not in PREFERRED + ["wasm-file"]
):
    d = f"{cells}/ETSI-upstream-cell-{st}"
    p = find(d, "baseline-result.json")
    result = json.load(open(p)) if p else {"status": "MISSING", "upstream": {}}
    upstream.append((st, result, read_suites(d, f"upstream/{st}")))

all_green = bool(stores) and all(g == "PASS" for _, g, _ in stores)
totals = [sum(ok + f + sk for _, ok, f, sk, _ in etsi_rows(su)) for _, _, su in stores]
if all_green and totals and len(set(totals)) == 1:
    message = f"{len(stores)}×{totals[0]} green"
elif all_green:
    message = "green"
else:
    red = [st for st, g, _ in stores if g != "PASS"]
    message = "red: " + ",".join(red)
json.dump(
    {
        "schemaVersion": 1,
        "label": "ETSI CIM 009",
        "message": message,
        "color": "brightgreen" if all_green else "red",
    },
    open(os.path.join(out, os.pardir, "badge.json"), "w"),
)

# One shields endpoint badge PER CELL next to the combined one — the
# README's live "file 1652/1652" row reads these.
for st, gate, suites in stores:
    ok = sum(o for _, o, _, _, _ in etsi_rows(suites))
    tot = sum(o + f + sk for _, o, f, sk, _ in etsi_rows(suites))
    green = gate == "PASS"
    json.dump(
        {
            "schemaVersion": 1,
            "label": st,
            "message": f"{ok}/{tot}" if tot else gate.lower(),
            "color": "brightgreen" if green else "red",
        },
        open(os.path.join(out, os.pardir, f"badge-{st}.json"), "w"),
    )

def cell_counts(suites):
    ok = sum(o for _, o, _, _, _ in etsi_rows(suites))
    tot = sum(o + f + sk for _, o, f, sk, _ in etsi_rows(suites))
    return ok, tot


# The official suite: one badge per cell and one over all cells. Without a
# baseline the run reports and does not gate, so its badge is informational
# (blue); a baseline gate FAIL is red, a PASS green.
OFFICIAL_COLOR = {"PASS": "brightgreen", "FAIL": "red", "MISSING": "red"}
off_counts = []
off_red = []
for st, result, suites in upstream:
    gate = result.get("status", "MISSING")
    # test points as the baseline gate counted them, the table's figure
    ok = result.get("passed", sum(o for _, o, _, _, _ in suites))
    tot = result.get("tps", sum(o + f + sk for _, o, f, sk, _ in suites))
    off_counts.append((ok, tot))
    if gate in ("FAIL", "MISSING"):
        off_red.append(st)
    json.dump(
        {
            "schemaVersion": 1,
            "label": f"official {st}",
            "message": f"{ok}/{tot}" if tot else gate.lower(),
            "color": OFFICIAL_COLOR.get(gate, "blue"),
        },
        open(os.path.join(out, os.pardir, f"badge-official-{st}.json"), "w"),
    )
if upstream:
    json.dump(
        {
            "schemaVersion": 1,
            "label": "official ETSI suite",
            # the same figure in every cell, else the lowest cell's
            "message": (f"red: {','.join(off_red)}" if off_red
                        else "%d/%d" % off_counts[0] if len(set(off_counts)) == 1
                        else "lowest %d/%d of %d cells" % (*min(off_counts), len(off_counts))),
            "color": "red" if off_red else (
                "brightgreen" if all(r.get("status") == "PASS" for _, r, _ in upstream)
                else "blue"),
        },
        open(os.path.join(out, os.pardir, "badge-official.json"), "w"),
    )


def suite_table(suites, mark_bad):
    t = ["<table><tr><th>Suite</th><th>Pass</th><th>Fail</th><th>Skip</th><th>Robot</th></tr>"]
    for name, o, f, sk, link in suites:
        robot = f'<a href="{html.escape(link)}">report</a>' if link else "—"
        cls = ' class="bad"' if (f and mark_bad) else ""
        t.append(f"<tr{cls}><td>{html.escape(name)}</td><td>{o}</td>"
                 f"<td>{f}</td><td>{sk}</td><td>{robot}</td></tr>")
    t.append("</table>")
    return "".join(t)


rows = []
# Our suite: the summary first, one row per cell
rows.append(
    "<h2>Our suite</h2><p><code>ngsi-ld-test-suite/</code>: the ETSI suite "
    "corrected and extended (no test weakened), every suite in every store "
    "mode. A cell is green only when every test case passes.</p>"
    '<table class="summary"><tr><th>Cell</th><th>ETSI test cases</th>'
    "<th>Antares-specific</th><th>Gate</th></tr>"
)
for st, gate, suites in stores:
    ok, tot = cell_counts(suites)
    own = [(o, o + f + sk) for n, o, f, sk, _ in suites if n == OWN]
    own_txt = ", ".join(f"{o}/{t}" for o, t in own) or "—"
    mark = "✅" if gate == "PASS" else "❌"
    cls = "" if gate == "PASS" else ' class="bad"'
    rows.append(
        f'<tr{cls}><td><a href="#ours-{html.escape(st)}">{html.escape(st)}</a></td>'
        f"<td><b>{ok}/{tot}</b></td><td>{own_txt}</td>"
        f"<td>{mark} {html.escape(gate)}</td></tr>"
    )
rows.append("</table>")

# The official suite: short, one row per cell
if upstream:
    ups = [r.get("upstream") or {} for _, r, _ in upstream]
    commits = sorted({(u.get("commit") or "")[:12] for u in ups} - {""})
    ran = ", ".join(f"<code>{html.escape(c)}</code>" for c in commits) or "did not run"
    ref = html.escape(next((u.get("ref") for u in ups if u.get("ref")), "develop"))
    rows.append(
        f"<h2>Official ETSI suite</h2><p>As published on forge.etsi.org "
        f"({ref} @ {ran}), fetched fresh for each run, nothing edited. Gated by "
        "a baseline of known failures per cell "
        "(<code>conformance/etsi-baseline/</code>); without one it reports "
        "and does not gate.</p>"
        '<table class="summary"><tr><th>Cell</th><th>Test points</th>'
        "<th>Baseline gate</th><th>Robot</th></tr>"
    )
    for st, result, suites in upstream:
        gate = result.get("status", "MISSING")
        mark = {"PASS": "✅", "FAIL": "❌", "MISSING": "❌",
                "NOBASELINE": "ℹ️", "SKIPPED": "⚠️", "STALE": "⚠️"}.get(gate, "⚠️")
        tps, passed = result.get("tps"), result.get("passed")
        count = f"{passed}/{tps}" if tps else "0/0"
        flags = []
        for key, title in (("regressions", "regressions"), ("shrink", "baseline entries passing"),
                           ("review", "to review"), ("outside", "outside TP/"),
                           ("incomplete", "incomplete")):
            items = result.get(key) or []
            if items:
                flags.append(f"{len(items)} {title}: " + ", ".join(html.escape(i) for i in items))
        flag_txt = "".join(f"<br><small>{f}</small>" for f in flags)
        links = " ".join(f'<a href="{html.escape(link)}">{html.escape(name)}</a>'
                         for name, _, _, _, link in suites if link)
        robot = f"<details><summary>{len(suites)} suites</summary>{links}</details>" if links else "—"
        cls = ' class="bad"' if gate in ("FAIL", "MISSING") else ""
        rows.append(
            f"<tr{cls}><td>{html.escape(st)}</td><td><b>{count}</b></td>"
            f"<td>{mark} {html.escape(gate)}{flag_txt}</td><td>{robot}</td></tr>"
        )
    rows.append("</table>")

# Our suite in detail, one section per cell
rows.append("<h2>Our suite, per cell</h2>")
for st, gate, suites in stores:
    ok, tot = cell_counts(suites)
    mark = "✅" if gate == "PASS" else "❌"
    rows.append(
        f'<section id="ours-{html.escape(st)}"><h3>{mark} {html.escape(st)}: '
        f"{ok}/{tot} (gate {html.escape(gate)})</h3>{suite_table(suites, True)}</section>"
    )

banner = "Our suite: every store green" if all_green else "Our suite: RED cells, see below"
page = f"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Antares — ETSI CIM 009 conformance</title>
<style>
  body {{ font: 15px/1.5 system-ui, sans-serif; margin: 2rem auto; max-width: 60rem; padding: 0 1rem; color: #1a1a1a; background: #fff; }}
  h1 {{ font-size: 1.5rem; }}
  nav {{ margin-bottom: 1rem; font-size: .9rem; }}
  nav a {{ margin-right: 1rem; }}
  .banner {{ padding: .6rem 1rem; border-radius: .5rem; font-weight: 600;
             background: {"#e6f6e6" if all_green else "#fde8e8"};
             color: {"#176617" if all_green else "#8f1d1d"}; }}
  table {{ border-collapse: collapse; margin: .5rem 0 1.5rem; width: 100%; }}
  th, td {{ text-align: left; padding: .3rem .7rem; border-bottom: 1px solid #e5e5e5; }}
  tr.bad td {{ background: #fdf0f0; }}
  table.summary td, table.summary th {{ padding: .45rem .8rem; font-size: 1.05rem; }}
  details summary {{ cursor: pointer; }}
  details a {{ margin-right: .6rem; font-size: .9rem; }}
  footer {{ color: #666; font-size: .85rem; }}
  @media (prefers-color-scheme: dark) {{
    body {{ background: #111; color: #ddd; }}
    th, td {{ border-color: #333; }}
    tr.bad td {{ background: #3a1d1d; }}
    a {{ color: #7ab8ff; }}
  }}
</style>
<h1>Antares — ETSI NGSI-LD (CIM 009 V1.9.1) conformance</h1>
<nav><a href="../unit/">Unit &amp; integration tests →</a>
<a href="../coverage/">Coverage →</a></nav>
<p class="banner">{banner}</p>
<p>One full run per store mode of our suite and of the official ETSI
suite; each suite links Robot's own drill-down for that run. Produced by
the <code>etsi-matrix</code> workflow.</p>
{"".join(rows)}
<footer>Generated by dev/etsi-report-site.py from the ETSI-matrix-results
bundle.</footer>
"""
open(os.path.join(out, "index.html"), "w").write(page)
# Rendering never gates: a red cell is reported here and gated by
# dev/etsi-matrix-summary.py, which exits nonzero on it.
print(f"site: {out}/index.html  badge: {message}")
