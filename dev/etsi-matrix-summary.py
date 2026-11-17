#!/usr/bin/env python3
"""Fold the per-store ETSI results into ONE bundle + ONE report and gate.

Usage: etsi-matrix-summary.py <cells-dir>
  <cells-dir> holds one ETSI-cell-<store>/ dir per store job — each the
  results of ONE pipeline run over ALL suites (CI: downloaded artifacts;
  local: written by dev/etsi-local.sh). Expected stores come from the
  STORES env (default "file postgres timescale"); a missing dir is a
  red cell, not a silent skip.

Writes <cells-dir>/_combined/:
  all-resource-samples.csv  EVERY 1 Hz CPU/RSS sample from EVERY store run,
                            each row prefixed with its store (`phase` already
                            names the suite, `test` the TP under test)
  all-failures.csv          every failing TP from every store: store, suite,
                            test, the keyword that ran (top step → failing
                            call), the FAIL log (expected vs got), message
  matrix-summary.md         the report below, as a file

The official ETSI suite (TESTSUITE=etsi|both): one ETSI-upstream-cell-<store>/
dir per store as well, gated by its baseline (dev/etsi-baseline.py writes
baseline-status.txt there): FAIL or a missing dir is red; PASS, SKIPPED
(forge unreachable), NOBASELINE and STALE are not. Its failures go to
_combined/upstream-failures.csv, its samples into all-resource-samples.csv
with run `upstream:<store>`.

The AntaresSpecificTests row is listed with the suites and totalled on its
own line: it is not ETSI-derived.

Prints the report to stdout (the workflow appends it to the step summary
together with the bundle download link). Exit: nonzero unless ALL stores
green.
"""
import csv
import glob
import os
import re
import sys
from collections import defaultdict

STORES = (os.environ.get("STORES") or "file postgres timescale").split()
TESTSUITE = os.environ.get("TESTSUITE") or "ours"
COPIES = {"ours": ["ours"], "etsi": ["etsi"], "both": ["ours", "etsi"]}.get(TESTSUITE)
if COPIES is None:
    sys.exit(f"unknown TESTSUITE={TESTSUITE} (ours|etsi|both)")
OWN = "AntaresSpecificTests"  # our copy's non-ETSI suite: its own total
SAMPLE_COLS = ["run", "ts", "iso", "container", "phase", "test", "cpu_pct", "rss_mib"]
FAIL_COLS = ["run", "suite", "test", "tags", "start", "elapsed_s",
             "keyword", "fail_log", "message"]

cells_dir = sys.argv[1] if len(sys.argv) > 1 else "cells"
comb = os.path.join(cells_dir, "_combined")
os.makedirs(comb, exist_ok=True)


def find(d, name):
    hits = glob.glob(f"{d}/**/{name}", recursive=True)
    return hits[0] if hits else None


red, out_md, all_failures, upstream_failures = [], [], [], []
samples_out = open(f"{comb}/all-resource-samples.csv", "w", newline="")
samples_w = csv.DictWriter(samples_out, SAMPLE_COLS, extrasaction="ignore")
samples_w.writeheader()

def suite_table(rows, res):
    out = ["| Suite | Pass | Fail | Skip | RSS avg | RSS peak | CPU peak | Samples |",
           "|---|---|---|---|---|---|---|---|"]
    for name, p, f, s in rows:
        rss, cpu = res[name]["rss"], res[name]["cpu"]
        rss_avg = f"{sum(rss) / len(rss):.0f} MiB" if rss else "—"
        rss_peak = f"{max(rss):.0f} MiB" if rss else "—"
        cpu_peak = f"{max(cpu):.0f}%" if cpu else "—"
        out.append(f"| {name} | {p} | {f} | {s} "
                   f"| {rss_avg} | {rss_peak} | {cpu_peak} | {max(len(rss), len(cpu))} |")
    etsi = [r for r in rows if r[0] != OWN]
    own = [r for r in rows if r[0] == OWN]
    if rows:
        tp, tf, ts = (sum(int(row[i]) for row in etsi) for i in (1, 2, 3))
        out.append(f"| **Total** | **{tp}/{tp + tf + ts}** | {tf} | {ts} | | | | |")
        for _, p, f, s in own:
            out.append(f"| **Antares-specific** | **{p}/{int(p) + int(f) + int(s)}** | {f} | {s} | | | | |")
    else:
        out.append("| _no results produced_ | — | — | — | — | — | — | — |")
    out.append("")
    return out


def fold(d, run, failures_into):
    """per-suite rows, samples (folded into the combined file) and failures"""
    rows = []
    path = find(d, "run-summary.md")
    if path:
        rows = re.findall(r"^\| (?!Suite)(\S+) \| (\d+) \| (\d+) \| (\d+) \|$",
                          open(path).read(), re.M)
    res = defaultdict(lambda: {"rss": [], "cpu": []})
    path = find(d, "resource-samples.csv")
    if path:
        for r in csv.DictReader(open(path)):
            r["run"] = run
            samples_w.writerow(r)
            bucket = res[r.get("phase") or "?"]
            for key, col in (("rss", "rss_mib"), ("cpu", "cpu_pct")):
                try:
                    bucket[key].append(float(r[col]))
                except (KeyError, TypeError, ValueError):
                    pass
    path = find(d, "failures.csv")
    if path:
        for r in csv.DictReader(open(path)):
            r["run"] = run
            failures_into.append(r)
    return rows, res


for st in (STORES if "ours" in COPIES else []):
    d = f"{cells_dir}/ETSI-cell-{st}"
    gate = "MISSING"
    path = find(d, "gate-status.txt")
    if path:
        gate = open(path).read().strip()
    mark = "✅" if gate == "PASS" else "❌"
    if gate != "PASS":
        red.append(f"{st}: {gate}")

    suite_rows, res = fold(d, st, all_failures)
    out_md.append(f"## ETSI results — store: `{st}` — gate {mark} {gate}\n")
    out_md.extend(suite_table(suite_rows, res))

# The official suite: gated by its baseline, not by 100% green.
UPSTREAM_GREEN = ("PASS", "SKIPPED", "NOBASELINE", "STALE")
for st in (STORES if "etsi" in COPIES else []):
    d = f"{cells_dir}/ETSI-upstream-cell-{st}"
    gate = "MISSING"
    path = find(d, "baseline-status.txt")
    if path:
        gate = open(path).read().strip()
    if gate not in UPSTREAM_GREEN:
        red.append(f"upstream:{st}: {gate}")
    mark = "✅" if gate == "PASS" else ("❌" if gate not in UPSTREAM_GREEN else "⚠️")
    suite_rows, res = fold(d, f"upstream:{st}", upstream_failures)
    out_md.append(f"## Official ETSI suite — store: `{st}` — baseline gate {mark} {gate}\n")
    path = find(d, "baseline-report.md")
    if path:
        out_md.append(open(path).read().strip() + "\n")
    out_md.extend(suite_table(suite_rows, res))

samples_out.close()
with open(f"{comb}/all-failures.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, FAIL_COLS, extrasaction="ignore")
    w.writeheader()
    w.writerows(all_failures)
if "etsi" in COPIES:
    with open(f"{comb}/upstream-failures.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, FAIL_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(upstream_failures)

# ---- the detailed error list (full data lives in all-failures.csv) ----
# Our copy's only: the official suite's known failures are its baseline,
# listed per cell above, every one in upstream-failures.csv.
out_md.append(f"## All failures — {len(all_failures)} across the matrix\n")
if not all_failures and "ours" in COPIES:
    out_md.append("None. 🎉\n")
SHOW = 200
for i, fl in enumerate(all_failures[:SHOW], 1):
    detail = fl.get("fail_log") or fl.get("message") or ""
    out_md.append(f"**{i}. `{fl['run']} × {fl.get('suite', '?')}` — {fl.get('test', '?')}**")
    if fl.get("keyword"):
        out_md.append(f"- ran: `{fl['keyword'][:300]}`")
    out_md.append(f"- error: {detail[:600]}")
    out_md.append("")
if len(all_failures) > SHOW:
    out_md.append(f"…and {len(all_failures) - SHOW} more — see `all-failures.csv` in the bundle.\n")
if red:
    out_md.append("**Red stores:**\n" + "".join(f"- {r}\n" for r in red))

report = "\n".join(out_md) + "\n"
open(f"{comb}/matrix-summary.md", "w").write(report)
print(report)
sys.exit(f"{len(red)} red store(s)" if red else 0)
