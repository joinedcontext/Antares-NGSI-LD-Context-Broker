#!/usr/bin/env python3
"""Gate a run of the official ETSI suite against its baseline of known failures.

The official suite (dev/etsi-upstream.sh, fetched fresh per run) is not
100% green on Antares, and it moves: every run takes develop's HEAD. Its
gate is therefore relative to a baseline: conformance/etsi-baseline/<cell>.txt
lists the TPs that failed at one upstream commit, each with the reason.

  etsi-baseline.py check <results> [--baseline FILE]
  etsi-baseline.py write <results> <FILE>

<results> is one run of the official copy: tp-status.csv (dev/etsi-report.py),
etsi-upstream.txt (the commit it ran) and, when the baseline names a commit,
upstream-changed.txt (`git diff --name-status` baseline..run, written by the
pipeline while the clone exists) or upstream-changed.unreachable.

check gates (exit 1) on:
  regression  a TP that fails now, is not in the baseline, and whose .robot
              file is unchanged upstream since the baseline commit: it passed
              there, so the broker broke it
  shrink      a baseline TP that passes now: drop its line
  incomplete  a robot run that left no output.xml (the run cannot be judged)
and reports without gating:
  review      a failing TP whose file is new or changed upstream since the
              baseline commit: upstream changed, review it and baseline it
  absent      a baseline TP the run did not execute (removed upstream, or
              filtered out by SUITES)
Forge unreachable (etsi-upstream.txt status skipped) is SKIPPED; no baseline
file for the cell is NOBASELINE; a baseline commit upstream no longer has
(history rewritten) is STALE. None of the three gates; each says so.

write turns a run into a baseline: every failing TP with the reason already
recorded for it, `our-bug  unclassified` for a new one (classify by hand).

Writes <results>/baseline-status.txt (PASS FAIL SKIPPED NOBASELINE STALE),
baseline-result.json and baseline-report.md.

Baseline file format, one TP per line after the header:
  commit: <upstream sha the baseline was made at>
  date: <its commit date>
  <TP id>  <reason>  [note]
reason: our-bug (broker defect, to fix) | suite-bug (the ETSI test is wrong;
the note links ngsi-ld-test-suite/testsuite-doubts.md or the upstream issue)
| newer-spec (tests a feature beyond CIM 009 V1.9.1) | env (harness
limitation). suite-bug, newer-spec and env need a note.
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
from pathlib import Path

REASONS = ("our-bug", "suite-bug", "newer-spec", "env")
NEEDS_NOTE = ("suite-bug", "newer-spec", "env")


def tp_id(test_name):
    """The TP id is the test name's first token: `001_01_01 MinimalEntity`."""
    return (test_name.split() or ["?"])[0]


def read_kv(path):
    out = {}
    if os.path.exists(path):
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out.setdefault(k, v)
    return out


def read_baseline(path):
    """(header, {tp: (reason, note)}, errors)"""
    header, entries, errors = {}, {}, []
    for n, raw in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(commit|date|url|ref):\s*(\S+)$", line)
        if m:
            header[m.group(1)] = m.group(2)
            continue
        parts = line.split(None, 2)
        if len(parts) < 2 or parts[1] not in REASONS:
            errors.append(
                f"{path}:{n}: want `<TP id>  <{'|'.join(REASONS)}>  [note]`: {line}"
            )
            continue
        tp, reason, note = parts[0], parts[1], (parts[2] if len(parts) > 2 else "")
        if reason in NEEDS_NOTE and not note:
            errors.append(
                f"{path}:{n}: {tp} {reason} needs a note (link or one line why)"
            )
        if tp in entries:
            errors.append(f"{path}:{n}: {tp} listed twice")
        entries[tp] = (reason, note)
    if "commit" not in header:
        errors.append(
            f"{path}: no `commit:` line — the upstream commit the baseline was made at"
        )
    return header, entries, errors


def read_run(results):
    """{tp: {"status": PASS|FAIL, "sources": set, "suite": str}}; a TP is red
    when any of its test cases is (a data-driven id can repeat)."""
    run = {}
    path = os.path.join(results, "tp-status.csv")
    if not os.path.exists(path):
        return run
    for r in csv.DictReader(Path(path).read_text(encoding="utf-8").splitlines()):
        tp = tp_id(r.get("test") or "")
        e = run.setdefault(
            tp, {"status": "PASS", "sources": set(), "suite": r.get("suite", "")}
        )
        if r.get("status") != "PASS":
            e["status"] = "FAIL"
        if r.get("source"):
            e["sources"].add(r["source"])
    return run


def incomplete_runs(results):
    """robot runs (one console log each) that left no output.xml"""
    out = []
    for log in sorted(glob.glob(os.path.join(results, "*-console.log"))):
        name = os.path.basename(log)[: -len("-console.log")]
        if not os.path.exists(os.path.join(results, name, "output.xml")):
            out.append(name)
    return out


def changed_files(results):
    """None when no diff was recorded, else {path: A|M|D|...}"""
    path = os.path.join(results, "upstream-changed.txt")
    if not os.path.exists(path):
        return None
    out = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            out[parts[-1]] = parts[0]
    return out


def write_outputs(results, status, result, md):
    out = Path(results)
    (out / "baseline-status.txt").write_text(status + "\n")
    result["status"] = status
    (out / "baseline-result.json").write_text(
        json.dumps(result, indent=1, sort_keys=True) + "\n"
    )
    (out / "baseline-report.md").write_text(md)
    print(md)


def check(results, baseline_path):
    up = read_kv(os.path.join(results, "etsi-upstream.txt"))
    upstream = {k: up.get(k, "") for k in ("url", "ref", "commit", "date", "status")}
    result = {"upstream": upstream, "baseline": baseline_path}
    head = (
        f"### ETSI suite {upstream['ref'] or 'develop'} @ {upstream['commit'][:12] or '?'}, "
        f"{upstream['date'] or '?'}\n\n"
    )

    if upstream["status"].startswith("skipped"):
        write_outputs(
            results,
            "SKIPPED",
            result,
            head
            + f"**skipped: {upstream['status'][len('skipped: ') :]}** — the official "
            "suite did not run; nothing gated.\n",
        )
        return 0
    if not upstream["status"].startswith("fetched"):
        write_outputs(
            results,
            "FAIL",
            result,
            head + f"**error: {upstream['status'] or 'no etsi-upstream.txt'}**\n",
        )
        return 1

    run = read_run(results)
    passed = sum(1 for e in run.values() if e["status"] == "PASS")
    result.update({"tps": len(run), "passed": passed})
    head += f"{passed}/{len(run)} TPs pass.\n\n"
    broken = incomplete_runs(results)
    result["incomplete"] = broken

    if not os.path.exists(baseline_path):
        failing = sorted(tp for tp, e in run.items() if e["status"] != "PASS")
        result["unbaselined"] = failing
        md = (
            head
            + f"**no baseline** (`{baseline_path}`) — {len(failing)} failing TPs reported, "
            "nothing gated. Create it from this run: "
            f"`dev/etsi-baseline.py write <results> {baseline_path}`, then classify each line.\n"
        )
        status = "NOBASELINE"
        if broken:
            md += f"\n**incomplete:** no output.xml for {', '.join(broken)}\n"
            status = "FAIL"
        write_outputs(results, status, result, md)
        return 1 if status == "FAIL" else 0

    header, entries, errors = read_baseline(baseline_path)
    if errors:
        write_outputs(
            results,
            "FAIL",
            result,
            head
            + "**baseline file invalid:**\n\n"
            + "".join(f"- {e}\n" for e in errors),
        )
        return 1
    result["baseline_commit"] = header["commit"]
    by_reason = {r: sum(1 for v in entries.values() if v[0] == r) for r in REASONS}
    result["by_reason"] = by_reason

    unreachable = os.path.exists(os.path.join(results, "upstream-changed.unreachable"))
    changed = {} if header["commit"] == upstream["commit"] else changed_files(results)
    if changed is None and not unreachable:
        unreachable = True  # no diff recorded although the commits differ

    regressions, review, shrink, absent, expected = [], [], [], [], []
    for tp, e in sorted(run.items()):
        if e["status"] == "PASS":
            if tp in entries:
                shrink.append(tp)
            continue
        if tp in entries:
            expected.append(tp)
            continue
        # new or modified upstream (a deleted file cannot hold a running TP)
        touched = sorted(s for s in e["sources"] if (changed or {}).get(s, "D") != "D")
        if unreachable or touched or not e["sources"]:
            review.append(
                (
                    tp,
                    ", ".join(touched)
                    or ("diff unavailable" if unreachable else "source unknown"),
                )
            )
        else:
            regressions.append(tp)
    absent = sorted(tp for tp in entries if tp not in run)
    result.update(
        {
            "regressions": regressions,
            "shrink": shrink,
            "absent": absent,
            "review": [t for t, _ in review],
            "expected": len(expected),
            "changed_files": len(changed or {}),
        }
    )

    md = [head]
    md.append(
        f"Baseline `{baseline_path}` made at {header['commit'][:12]} ({header.get('date', '?')}): "
        f"{len(entries)} known failures — "
        + ", ".join(f"{n} {r}" for r, n in by_reason.items() if n)
        + ".\n"
    )
    if header["commit"] != upstream["commit"]:
        md.append(
            f"Upstream moved since the baseline: {len(changed or {})} files changed.\n"
            if not unreachable
            else f"**The baseline commit {header['commit'][:12]} is not reachable upstream** "
            "(history rewritten?): no failure can be told apart from an upstream change. "
            "Re-baseline.\n"
        )

    def section(title, items, note=""):
        if items:
            md.append(f"\n**{title}** ({len(items)}){note}\n")
            md.extend(f"- {i}\n" for i in items)

    section(
        "Regressions — passed at the baseline commit, file unchanged, fail now",
        regressions,
    )
    section("Baseline entries that pass now — remove these lines", shrink)
    section(
        "Upstream change, review and baseline",
        [f"{t} ({w})" for t, w in review],
        " — not gated",
    )
    section(
        "Baseline entries not run",
        absent,
        " — removed upstream or filtered out; not gated",
    )
    if broken:
        section("Incomplete — no output.xml", broken)

    if unreachable and not regressions and not shrink and not broken:
        status = "STALE"
    else:
        status = "FAIL" if (regressions or shrink or broken) else "PASS"
    md.insert(1, f"**baseline gate: {status}**\n\n")
    write_outputs(results, status, result, "".join(md))
    return 1 if status == "FAIL" else 0


def write(results, baseline_path):
    up = read_kv(os.path.join(results, "etsi-upstream.txt"))
    if not up.get("commit"):
        sys.exit(
            f"{results}/etsi-upstream.txt names no commit: not a run of the official suite"
        )
    old = {}
    if os.path.exists(baseline_path):
        _, old, _ = read_baseline(baseline_path)
    run = read_run(results)
    if not run:
        sys.exit(f"{results}/tp-status.csv: no TPs")
    failing = sorted(tp for tp, e in run.items() if e["status"] != "PASS")
    cell = os.path.splitext(os.path.basename(baseline_path))[0]
    lines = [
        f"# Official ETSI NGSI-LD suite: the TPs that fail on Antares, cell `{cell}`.",
        "# Every other TP that ran passed at this commit. Gate: dev/etsi-baseline.py;",
        '# docs/src/conformance.md, "Two suites". Line: <TP id>  <reason>  [note]',
        "# reason: our-bug | suite-bug | newer-spec | env",
        f"url: {up.get('url', '')}",
        f"commit: {up['commit']}",
        f"date: {up.get('date', '')}",
        "",
    ]
    width = max((len(t) for t in failing), default=8) + 2
    for tp in failing:
        reason, note = old.get(tp, ("our-bug", "unclassified"))
        lines.append(f"{tp:<{width}}{reason:<12}{note}".rstrip())
    os.makedirs(os.path.dirname(baseline_path) or ".", exist_ok=True)
    Path(baseline_path).write_text("\n".join(lines) + "\n")
    print(
        f"{baseline_path}: {len(failing)} failing TPs of {len(run)} at {up['commit'][:12]}"
    )


# ---------------------------------------------------------------- selftest
def _robot_xml(path, tests):
    """a minimal Robot 7 output.xml: tests = [(source, name, PASS|FAIL)]"""
    by_src = {}
    for src, name, st in tests:
        by_src.setdefault(src, []).append((name, st))
    body, n = [], 0
    for src, cases in by_src.items():
        body.append(
            f'<suite id="s1-s{len(body) + 1}" name="x" source="/tmp/ngsi-ld-test-suite/{src}">'
        )
        for name, st in cases:
            n += 1
            body.append(
                f'<test id="t{n}" name="{name}"><status status="{st}" '
                f'start="2026-11-17T12:00:00.000000" elapsed="0.1">m</status></test>'
            )
        body.append("</suite>")
    p = sum(1 for _, _, st in tests if st == "PASS")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        '<?xml version="1.0"?><robot><suite id="s1" name="top">'
        + "".join(body)
        + f'</suite><statistics><total><stat pass="{p}" fail="{len(tests) - p}" '
        'skip="0">All Tests</stat></total></statistics></robot>'
    )


def selftest():
    """Fixture runs through etsi-report.py and this checker, plus the fetch
    helper's diff against a local git repository standing in for forge."""
    import shutil
    import subprocess
    import tempfile

    here = Path(__file__).resolve().parent
    me = [sys.executable, str(Path(__file__).resolve())]
    upstream_sh = str(here / "etsi-upstream.sh")

    def git(repo, *args):
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                *args,
            ],
            check=True,
            capture_output=True,
        )

    def run(results, tests, commit, changed=None, unreachable=False, status="fetched"):
        results = Path(results)
        shutil.rmtree(results, ignore_errors=True)
        results.mkdir(parents=True)
        for suite in {t[0].split("/")[2] for t in tests}:
            _robot_xml(
                results / suite / "output.xml",
                [t for t in tests if t[0].split("/")[2] == suite],
            )
            (results / f"{suite}-console.log").write_text("")
        subprocess.run(
            [sys.executable, str(here / "etsi-report.py")],
            check=True,
            capture_output=True,
            env={**os.environ, "RESULTS": str(results)},
        )
        (results / "etsi-upstream.txt").write_text(
            f"url=file://x\nref=develop\ncommit={commit}\ndate=2026-11-17\nstatus={status}\n"
        )
        if changed is not None:
            (results / "upstream-changed.txt").write_text(changed)
        if unreachable:
            (results / "upstream-changed.unreachable").write_text("x\n")

    def check_(results, base):
        r = subprocess.run(
            [*me, "check", str(results), "--baseline", str(base)],
            capture_output=True,
            text=True,
            check=False,
        )
        res = json.loads((Path(results) / "baseline-result.json").read_text())
        return r.returncode, res

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # an upstream with history: A = baseline commit, B = the run's commit
        up = tmp / "upstream"
        up.mkdir()
        git(up, "init", "-q", "-b", "develop")
        for rel in (
            "TP/NGSI-LD/Prov/Ent/a.robot",
            "TP/NGSI-LD/Prov/Ent/b.robot",
            "TP/NGSI-LD/Cons/Q/c.robot",
            "IOP_TP/NGSI-LD/I/i.robot",
        ):
            (up / rel).parent.mkdir(parents=True, exist_ok=True)
            (up / rel).write_text("*** Test Cases ***\n")
        git(up, "add", "-A")
        git(up, "commit", "-q", "-m", "A")
        a = subprocess.run(
            ["git", "-C", str(up), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        (up / "TP/NGSI-LD/Cons/Q/c.robot").write_text("*** Test Cases ***\nchanged\n")
        (up / "TP/NGSI-LD/Cons/Q/d.robot").write_text("*** Test Cases ***\n")
        git(up, "add", "-A")
        git(up, "commit", "-q", "-m", "B")

        # the fetch helper: clone at develop, robot dirs, diff against A
        env = {**os.environ, "ETSI_UPSTREAM_URL": f"file://{up}", "ETSI_REF": "develop"}
        f = subprocess.run(
            [upstream_sh, "fetch", str(tmp / "clone")],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert f.returncode == 0, f.stderr
        info = read_kv(tmp / "clone" / "etsi-upstream.txt")
        b = info["commit"]
        assert f.stdout.strip() == b and info["status"] == "fetched", info
        suite = tmp / "clone" / "ngsi-ld-test-suite"
        dirs = subprocess.run(
            [upstream_sh, "suites", str(suite)],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        assert dirs == ["Cons/Q", "Prov/Ent"], dirs
        diff = subprocess.run(
            [upstream_sh, "changed", str(suite), a],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        assert diff == "M\tTP/NGSI-LD/Cons/Q/c.robot\nA\tTP/NGSI-LD/Cons/Q/d.robot\n", (
            diff
        )
        gone = subprocess.run(
            [upstream_sh, "changed", str(suite), "0" * 40],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert gone.returncode == 4, gone
        bad = subprocess.run(
            [upstream_sh, "fetch", str(tmp / "nowhere")],
            env={**env, "ETSI_UPSTREAM_URL": f"file://{tmp}/missing"},
            capture_output=True,
            text=True,
            check=False,
        )
        assert bad.returncode == 3, bad
        assert read_kv(tmp / "nowhere" / "etsi-upstream.txt")["status"].startswith(
            "skipped"
        ), bad
        print(
            "  fetch helper ok (clone, robot dirs, diff, unreachable commit and remote)"
        )

        A, B, C, D = (
            "TP/NGSI-LD/Prov/Ent/a.robot",
            "TP/NGSI-LD/Prov/Ent/b.robot",
            "TP/NGSI-LD/Cons/Q/c.robot",
            "TP/NGSI-LD/Cons/Q/d.robot",
        )
        IOP = "IOP_TP/NGSI-LD/I/i.robot"
        base = tmp / "base.txt"
        base.write_text(
            f"# x\ncommit: {a}\ndate: 2026-11-16\n"
            "001_02  our-bug  notify order\n"
            "001_03  suite-bug  testsuite-doubts.md #2\n"
            "IOP_001_01  env  IOP mock port\n"
        )
        r = tmp / "run"
        green = [
            (A, "001_01 Min", "PASS"),
            (A, "001_02 Bad", "FAIL"),
            (B, "001_03 Wrong", "FAIL"),
            (C, "003_01 Q", "PASS"),
            (IOP, "IOP_001_01 X", "FAIL"),
        ]

        # same failures as the baseline, upstream moved (c.robot changed)
        run(r, green, b, changed=diff)
        rc, res = check_(r, base)
        assert rc == 0 and res["status"] == "PASS" and res["expected"] == 3, res
        assert res["by_reason"] == {
            "our-bug": 1,
            "suite-bug": 1,
            "newer-spec": 0,
            "env": 1,
        }, res

        # the same run against a baseline missing one line: that TP regresses
        short = tmp / "short.txt"
        short.write_text(
            "".join(
                ln + "\n"
                for ln in base.read_text().splitlines()
                if not ln.startswith("001_02")
            )
        )
        rc, res = check_(r, short)
        assert rc == 1 and res["regressions"] == ["001_02"], res

        # a TP of an unchanged file breaks: regression, red
        run(
            r,
            [
                *green[:3],
                (C, "003_01 Q", "PASS"),
                (IOP, "IOP_001_01 X", "FAIL"),
                (B, "001_04 New", "FAIL"),
            ],
            b,
            changed=diff,
        )
        rc, res = check_(r, base)
        assert rc == 1 and res["regressions"] == ["001_04"], res

        # a TP in a changed or new upstream file fails: review, not gated
        run(
            r,
            [
                *green[:3],
                (C, "003_01 Q", "FAIL"),
                (D, "004_01 New", "FAIL"),
                (IOP, "IOP_001_01 X", "FAIL"),
            ],
            b,
            changed=diff,
        )
        rc, res = check_(r, base)
        assert (
            rc == 0
            and res["status"] == "PASS"
            and res["review"] == ["003_01", "004_01"]
        ), res

        # a baseline entry passes now: the baseline must shrink, red
        run(r, [*green[:2], (B, "001_03 Wrong", "PASS"), *green[3:]], b, changed=diff)
        rc, res = check_(r, base)
        assert rc == 1 and res["shrink"] == ["001_03"], res

        # a baseline entry the run did not execute: reported, not gated
        run(r, green[:4], b, changed=diff)
        rc, res = check_(r, base)
        assert rc == 0 and res["absent"] == ["IOP_001_01"], res

        # a robot run that wrote no output.xml: red
        run(r, green, b, changed=diff)
        (r / "Broken-console.log").write_text("robot died\n")
        rc, res = check_(r, base)
        assert rc == 1 and res["incomplete"] == ["Broken"], res

        # the baseline commit is gone upstream: STALE, new failures reviewed
        run(r, [*green, (B, "001_04 New", "FAIL")], b, unreachable=True)
        rc, res = check_(r, base)
        assert rc == 0 and res["status"] == "STALE" and res["review"] == ["001_04"], res

        # forge unreachable: SKIPPED; no baseline file: NOBASELINE; both pass
        run(r, green, "", status="skipped: forge.etsi.org unreachable")
        rc, res = check_(r, base)
        assert rc == 0 and res["status"] == "SKIPPED", res
        run(r, green, b, changed=diff)
        rc, res = check_(r, tmp / "none.txt")
        assert rc == 0 and res["status"] == "NOBASELINE", res

        # an invalid baseline (unknown reason, suite-bug without a note): red
        bad_base = tmp / "bad.txt"
        bad_base.write_text(f"commit: {a}\n001_02  flaky\n001_03  suite-bug\n")
        rc, res = check_(r, bad_base)
        assert rc == 1 and res["status"] == "FAIL", res

        # write keeps each listed TP's reason and note, new ones unclassified
        new_failure = [(A, "001_02 Bad", "FAIL"), (C, "003_01 Q", "FAIL")]
        run(r, new_failure, b, changed=diff)
        out = tmp / "written.txt"
        out.write_text(base.read_text())
        subprocess.run(
            [*me, "write", str(r), str(out)], check=True, capture_output=True
        )
        header, entries, errors = read_baseline(out)
        assert not errors and header["commit"] == b, (header, errors)
        assert entries == {
            "001_02": ("our-bug", "notify order"),
            "003_01": ("our-bug", "unclassified"),
        }, entries
        print(
            "  baseline gate ok (pass, regression, removed line, review, shrink, absent,"
            " incomplete, stale, skipped, no baseline, invalid file, write)"
        )
    print("OK")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("mode", choices=("check", "write", "selftest"))
    ap.add_argument("results", nargs="?")
    ap.add_argument("baseline", nargs="?")
    ap.add_argument("--baseline", dest="baseline_opt")
    a = ap.parse_args()
    if a.mode == "selftest":
        print("etsi baseline self-check")
        return selftest()
    if not a.results:
        ap.error(f"{a.mode} needs the results dir")
    path = a.baseline_opt or a.baseline
    if a.mode == "write":
        if not path:
            ap.error("write needs the baseline file")
        write(a.results, path)
        return 0
    if not path:
        cell = (
            os.environ.get("ETSI_BASELINE_CELL") or os.environ.get("STORE") or "memory"
        )
        path = f"conformance/etsi-baseline/{cell}.txt"
    return check(a.results, path)


if __name__ == "__main__":
    sys.exit(main())
