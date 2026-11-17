#!/usr/bin/env python3
"""Vet every dependency version a pull request changes, in one table.

A green matrix proves the broker still conforms; it says nothing about
whether a new upstream release is trustworthy. This reads the lockfiles,
workflows, Dockerfiles and compose files at the pull request's merge base
and at its head, lists every package whose version moved (new transitive
packages included), and checks each new version for three things:

- age: published at least MIN_AGE_DAYS before now. A hijacked or broken
  release is usually yanked within days. A date that cannot be fetched is
  "unknown" and fails; nothing is guessed.
- advisories: OSV (one querybatch per ecosystem), plus `cargo deny check
  advisories` on every changed Cargo.lock and `npm audit` on every changed
  package-lock.json when the tools are passed in. Any advisory on a new
  version fails, except one the base branch's deny.toml ignores.
- supply-chain signals: crates that gain a build script or become a
  proc-macro, yanked versions, a lockfile checksum that differs from the
  registry's, trusted publishing dropped; npm versions published by another
  account than the previous one, install scripts newly present, provenance
  dropped, new dependencies that are not vetted rows of the same table;
  action tags (the commit each tag names is recorded); image digests.

The table goes to stdout or --summary (the job summary); exit 1 when any
row fails, so the pull request shows a red check naming the reason.

Network: stdlib urllib only, https only, allow-listed hosts only (redirects
included), timeouts and size caps on every response. Nothing downloaded is
extracted or executed: a .crate is read as a tar stream for its manifest.

    dev/deps-vet.py check --base <ref> --head <ref> [--summary FILE] ...
    dev/deps-vet.py selftest     # diffing and verdict rules, no network
"""

import argparse
import base64
import dataclasses
import datetime as dt
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

MIN_AGE_DAYS = 7
TIMEOUT = 30
ALLOWED_HOSTS = {
    "crates.io",
    "static.crates.io",
    "index.crates.io",
    "registry.npmjs.org",
    "api.osv.dev",
    "api.github.com",
    "registry-1.docker.io",
    "hub.docker.com",
}
CAP_API = 8 << 20
CAP_PACKUMENT = 96 << 20  # a full npm packument: vite's is ~40 MB
CAP_CRATE = 32 << 20
CAP_MANIFEST = 1 << 20
HUB_PAGES = 10  # Docker Hub tag pages (100 tags each) searched for a pinned digest
USER_AGENT = "antares-deps-vet (+https://github.com/joinedcontext/Antares-NGSI-LD-Context-Broker)"
CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
NPM_REGISTRY = "https://registry.npmjs.org/"
OSV_ECOSYSTEM = {"cargo": "crates.io", "npm": "npm", "actions": "GitHub Actions"}
INSTALL_HOOKS = ("preinstall", "install", "postinstall")


# --------------------------------------------------------------------------
# network


class Refused(Exception):
    """A request this script will not make or a response it will not read."""


def _check_url(url):
    u = urllib.parse.urlsplit(url)
    if u.scheme != "https" or u.hostname not in ALLOWED_HOSTS:
        raise Refused(f"host not allow-listed: {url}")


class _Redirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_Redirects)
_last_crates_api = [0.0]


def fetch(url, cap=CAP_API, data=None, headers=None):
    """Bytes of an allow-listed https URL, refused past `cap` bytes."""
    _check_url(url)
    host = urllib.parse.urlsplit(url).hostname
    if host == "crates.io":  # crawler policy: at most one request a second
        wait = 1.0 - (time.monotonic() - _last_crates_api[0])
        if wait > 0:
            time.sleep(wait)
        _last_crates_api[0] = time.monotonic()
    req = urllib.request.Request(url, data=data, headers={"User-Agent": USER_AGENT, **(headers or {})})
    token = os.environ.get("GITHUB_TOKEN")
    if host == "api.github.com" and token:
        # unredirected: never forwarded to another host
        req.add_unredirected_header("Authorization", f"Bearer {token}")
    with _OPENER.open(req, timeout=TIMEOUT) as resp:
        body = resp.read(cap + 1)
    if len(body) > cap:
        raise Refused(f"response over {cap} bytes: {url}")
    return body


def fetch_json(url, cap=CAP_API, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Accept": "application/json", **(headers or {})}
    if data is not None:
        hdrs["Content-Type"] = "application/json"
    return json.loads(fetch(url, cap, data, hdrs))


def quote(s):
    return urllib.parse.quote(s, safe="")


def parse_time(s):
    if not s:
        return None
    try:
        t = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.UTC)


# --------------------------------------------------------------------------
# reading the manifests at a ref


def git(*args, cwd=None):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def git_show(ref, path, cwd=None):
    """The file at `ref`, or "" when it does not exist there."""
    r = subprocess.run(["git", "show", f"{ref}:{path}"], check=False, cwd=cwd, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else ""


def kind_of(path):
    """Which differ reads `path`, or None."""
    base = path.rsplit("/", 1)[-1]
    if base == "Cargo.lock":
        return "cargo"
    if base == "package-lock.json":
        return "npm"
    if re.fullmatch(r"\.github/workflows/[^/]+\.ya?ml", path) or (
        path.startswith(".github/actions/") and base in ("action.yml", "action.yaml")
    ):
        return "actions"
    if base.startswith("Dockerfile") or re.search(r"compose[\w.-]*\.ya?ml$", base):
        return "docker"
    if path.startswith("deploy/") and re.search(r"\.ya?ml$", base):  # k8s manifests
        return "docker"
    return None


def images_by_repo(path, text):
    out = {}
    for ref in image_refs(path, text):
        repo, tag, digest = split_image(ref)
        out[(repo, f"{tag or ''}@{digest}" if digest else tag or "")] = {"tag": tag, "digest": digest}
    return out


def cargo_packages(text):
    """{(name, version): {source, checksum, deps}} of a Cargo.lock."""
    out = {}
    if not text.strip():
        return out
    for p in tomllib.loads(text).get("package", []):
        deps = [d.split(" ")[0] for d in p.get("dependencies", [])]
        out[(p["name"], p["version"])] = {"source": p.get("source"), "checksum": p.get("checksum"), "deps": deps}
    return out


def npm_name(path, entry):
    if entry.get("name"):
        return entry["name"]
    return path.rsplit("node_modules/", 1)[-1]


def npm_packages(text):
    """{(name, version): {resolved, integrity, install, deps}} of a v2/v3 package-lock.json."""
    out = {}
    if not text.strip():
        return out
    doc = json.loads(text)
    if "packages" not in doc:
        raise ValueError("package-lock.json without `packages` (lockfileVersion 1) is not supported")
    for path, e in doc["packages"].items():
        if not path or e.get("link") or "version" not in e:
            continue
        key = (npm_name(path, e), e["version"])
        cur = out.setdefault(key, {"resolved": e.get("resolved"), "integrity": e.get("integrity"),
                                   "install": False, "deps": set(), "paths": []})
        cur["install"] = cur["install"] or bool(e.get("hasInstallScript"))
        cur["deps"] |= set(e.get("dependencies", {})) | set(e.get("optionalDependencies", {}))
        cur["paths"].append(path)
    return out


USES = re.compile(r"""^\s*(?:-\s*)?uses:\s*["']?([^\s"'#]+)""", re.MULTILINE)


def action_refs(text):
    """{(owner/repo[/path], ref)} named by `uses:` in one workflow/action file."""
    out = set()
    for spec in USES.findall(text):
        if spec.startswith(("./", "docker://")) or "@" not in spec:
            continue
        name, ref = spec.rsplit("@", 1)
        out.add((name, ref))
    return out


FROM = re.compile(r"^\s*FROM\s+(?:--\S+\s+)*(\S+)(?:\s+AS\s+(\S+))?", re.IGNORECASE | re.MULTILINE)
IMAGE = re.compile(r"""^\s*image:\s*["']?([^\s"'#]+)""", re.MULTILINE)


def image_refs(path, text):
    """{image reference} named by FROM (minus stage names) or compose `image:`."""
    out, stages = set(), set()
    if path.rsplit("/", 1)[-1].startswith("Dockerfile"):
        for img, alias in FROM.findall(text):
            if img.lower() not in stages and "$" not in img and img != "scratch":
                out.add(img)
            if alias:
                stages.add(alias.lower())
    else:
        for img in IMAGE.findall(text):
            m = re.fullmatch(r"\$\{\w+:-([^}]+)\}", img)
            img = m.group(1) if m else img
            if "$" not in img:
                out.add(img)
    for spec in USES.findall(text):
        if spec.startswith("docker://"):
            out.add(spec[len("docker://"):])
    return out


def split_image(ref):
    """(repository, tag, digest) of an image reference."""
    digest = None
    if "@" in ref:
        ref, digest = ref.split("@", 1)
    tag = None
    last = ref.rsplit("/", 1)[-1]
    if ":" in last:
        ref, tag = ref.rsplit(":", 1)
    return ref, tag, digest


def vkey(v):
    """A sort key good enough to pick the nearest older version."""
    return [(0, int(p), "") if p.isdigit() else (1, 0, p) for p in re.split(r"[.+\-]", v.lstrip("v"))]


@dataclasses.dataclass
class Change:
    eco: str
    name: str
    old: str | None  # None: the name is new to this lockfile
    new: str
    where: list
    lock_old: dict = dataclasses.field(default_factory=dict)
    lock_new: dict = dataclasses.field(default_factory=dict)
    alongside: bool = False  # the old version stays in the tree too
    required_by: list = dataclasses.field(default_factory=list)
    facts: dict = dataclasses.field(default_factory=dict)
    advisories: list = dataclasses.field(default_factory=list)
    fails: list = dataclasses.field(default_factory=list)
    signals: list = dataclasses.field(default_factory=list)


def pair(eco, where, base, head):
    """Changes between two {(name, version): meta} maps, plus removed packages."""
    bnames, hnames = {}, {}
    for n, v in base:
        bnames.setdefault(n, set()).add(v)
    for n, v in head:
        hnames.setdefault(n, set()).add(v)
    changes, removed = [], []
    for name in sorted(set(bnames) | set(hnames)):
        bv, hv = bnames.get(name, set()), hnames.get(name, set())
        gone = sorted(bv - hv, key=vkey)
        paired = set()
        for v in sorted(hv - bv, key=vkey):
            pool = gone or sorted(bv, key=vkey)
            older = [o for o in pool if vkey(o) <= vkey(v)]
            old = (older or pool or [None])[-1]
            paired.add(old)
            c = Change(eco, name, old, v, [where], lock_new=head[(name, v)],
                       lock_old=base.get((name, old), {}) if old else {}, alongside=bool(old) and not gone)
            changes.append(c)
        for v in sorted(hv & bv, key=vkey):  # same version, different bytes
            for k in ("checksum", "integrity", "source", "resolved"):
                was, now = base[(name, v)].get(k), head[(name, v)].get(k)
                if was and now and was != now:
                    c = Change(eco, name, v, v, [where], lock_old=base[(name, v)], lock_new=head[(name, v)])
                    c.signals.append((f"{k} changed for the same version", True))
                    changes.append(c)
                    break
        removed.extend(f"{name} {v}" for v in gone if v not in paired)
    return changes, removed


def requirers(head, name):
    return sorted({f"{n} {v}" for (n, v), m in head.items() if name in m.get("deps", ())})


def diff_files(files):
    """Changes and removals across {path: (base_text, head_text)}."""
    changes, removed, used = [], [], []
    for path in sorted(files):
        btxt, htxt = files[path]
        kind = kind_of(path)
        if kind == "cargo":
            b, h = cargo_packages(btxt), cargo_packages(htxt)
            b = {k: m for k, m in b.items() if m["source"]}  # workspace members carry no source
            h = {k: m for k, m in h.items() if m["source"]}
            views = [(kind, b, h)]
        elif kind == "npm":
            views = [(kind, npm_packages(btxt), npm_packages(htxt))]
        elif kind == "actions":  # the `uses:` refs, and the service/container images
            views = [(kind, {k: {} for k in action_refs(btxt)}, {k: {} for k in action_refs(htxt)}),
                     ("docker", images_by_repo(path, btxt), images_by_repo(path, htxt))]
        elif kind == "docker":
            views = [(kind, images_by_repo(path, btxt), images_by_repo(path, htxt))]
        else:
            continue
        used.append(path)
        for k, b, h in views:
            cs, rm = pair(k, path, b, h)
            for c in cs:
                if c.old is None and k in ("cargo", "npm"):
                    c.required_by = requirers(h, c.name)
            changes += cs
            removed += [f"{r} ({path})" for r in rm]
    merged = {}
    for c in changes:  # one row per version change, however many files carry it
        k = (c.eco, c.name, c.old, c.new)
        if k in merged:
            merged[k].where += [w for w in c.where if w not in merged[k].where]
            merged[k].required_by = sorted(set(merged[k].required_by) | set(c.required_by))
            merged[k].signals += [s for s in c.signals if s not in merged[k].signals]
        else:
            merged[k] = c
    return list(merged.values()), removed, used


# --------------------------------------------------------------------------
# facts from the registries (network); each returns a plain dict


def crate_manifest_facts(blob, name, version):
    """build script / proc-macro of a .crate, read as a tar stream, never extracted."""
    root = f"{name}-{version}/"
    names, manifest = set(), None
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tar:
        for m in tar:
            names.add(m.name)
            if m.name == root + "Cargo.toml" and m.isfile() and m.size <= CAP_MANIFEST:
                manifest = tar.extractfile(m).read().decode("utf-8", "replace")
    if manifest is None:
        raise Refused(f"{name} {version}: no Cargo.toml in the .crate")
    doc = tomllib.loads(manifest)
    pkg, lib = doc.get("package", {}), doc.get("lib", {})
    build = pkg.get("build")
    if build is False:
        has_build = False
    elif isinstance(build, str):
        has_build = True
    else:
        has_build = (root + "build.rs") in names
    proc = bool(lib.get("proc-macro") or lib.get("proc_macro"))
    return {"build": has_build, "proc_macro": proc}


class Registry:
    """Live lookups; selftest swaps in canned answers with the same shape."""

    def __init__(self):
        self.cache = {}

    def _memo(self, key, fn):
        if key not in self.cache:
            try:
                self.cache[key] = fn()
            except (urllib.error.URLError, OSError, ValueError, Refused, tarfile.TarError, KeyError) as e:
                self.cache[key] = {"error": f"{type(e).__name__}: {e}"[:200]}
        return self.cache[key]

    def crate(self, name, version):
        def go():
            v = fetch_json(f"https://crates.io/api/v1/crates/{quote(name)}/{quote(version)}")["version"]
            blob = fetch(f"https://static.crates.io/crates/{quote(name)}/{quote(name)}-{quote(version)}.crate", CAP_CRATE)
            return {
                "published": v.get("created_at"),
                "yanked": bool(v.get("yanked")),
                "checksum": v.get("checksum"),
                "sha256": hashlib.sha256(blob).hexdigest(),
                "trusted": bool(v.get("trustpub_data")),
                **crate_manifest_facts(blob, name, version),
            }
        return self._memo(("crate", name, version), go)

    def packument(self, name):
        return self._memo(("npm", name), lambda: fetch_json(NPM_REGISTRY + quote(name).replace("%40", "@", 1), CAP_PACKUMENT))

    def npm(self, name, version):
        doc = self.packument(name)
        if "error" in doc:
            return doc
        v = doc.get("versions", {}).get(version)
        if v is None:
            return {"error": f"{name} {version} not in the registry"}
        scripts = v.get("scripts") or {}
        user = v.get("_npmUser") or {}
        repo = v.get("repository")
        return {
            "published": (doc.get("time") or {}).get(version),
            "publisher": user.get("name"),
            "trusted": bool(user.get("trustedPublisher")),
            "repository": repo.get("url") if isinstance(repo, dict) else repo,
            "install": any(h in scripts for h in INSTALL_HOOKS) or bool(v.get("gypfile")),
            "deps": sorted(set(v.get("dependencies") or {}) | set(v.get("optionalDependencies") or {})),
            "provenance": bool((v.get("dist") or {}).get("attestations")),
            "deprecated": v.get("deprecated"),
        }

    def provenance(self, name, version):
        """Source repository and tarball sha512 named by the version's SLSA provenance.

        Read as the registry serves it; the sigstore signature is not
        verified here (stdlib has no crypto for it)."""
        def go():
            url = NPM_REGISTRY + "-/npm/v1/attestations/" + quote(f"{name}@{version}").replace("%40", "@")
            for a in fetch_json(url).get("attestations", []):
                if not a.get("predicateType", "").startswith("https://slsa.dev/provenance/"):
                    continue
                st = json.loads(base64.b64decode(a["bundle"]["dsseEnvelope"]["payload"]))
                wf = st["predicate"]["buildDefinition"]["externalParameters"]["workflow"]
                digests = [x["digest"].get("sha512") for x in st.get("subject", [])]
                return {"repo": wf.get("repository"), "sha512": digests}
            return {"error": "no SLSA provenance attestation"}
        return self._memo(("prov", name, version), go)

    def action(self, name, ref):
        repo = "/".join(name.split("/")[:2])

        def go():
            api = f"https://api.github.com/repos/{repo}"
            out = {"published": None}
            sha, tag_date = None, None
            if re.fullmatch(r"[0-9a-f]{40}", ref):
                sha = ref
            else:
                try:
                    obj = fetch_json(f"{api}/git/ref/tags/{quote(ref)}")["object"]
                except urllib.error.HTTPError as e:
                    if e.code != 404:
                        raise
                    obj = fetch_json(f"{api}/git/ref/heads/{quote(ref)}")["object"]
                    out["branch"] = True
                if obj["type"] == "tag":  # annotated: peel to the commit
                    tag = fetch_json(f"{api}/git/tags/{obj['sha']}")
                    tag_date = (tag.get("tagger") or {}).get("date")
                    obj = tag["object"]
                sha = obj["sha"]
            out["sha"] = sha
            tags = fetch_json(f"{api}/tags?per_page=100")
            same = sorted((t["name"] for t in tags if t["commit"]["sha"] == sha), key=lambda t: (len(t), t))
            versioned = [t for t in same if re.fullmatch(r"v?\d+\.\d+\.\d+([-.+]\S*)?", t)]
            out["version_tag"] = versioned[-1] if versioned else None
            for t in [ref] + versioned:
                try:
                    rel = fetch_json(f"{api}/releases/tags/{quote(t)}")
                except urllib.error.HTTPError as e:
                    if e.code == 404:
                        continue
                    raise
                if rel.get("published_at"):
                    out["published"], out["date_from"] = rel["published_at"], f"release {t}"
                    break
            if not out["published"] and tag_date:
                out["published"], out["date_from"] = tag_date, f"tag {ref}"
            return out
        return self._memo(("action", name, ref), go)

    def image(self, repo, tag, digest):
        def go():
            parts = repo.split("/")
            if "." in parts[0] or ":" in parts[0] or parts[0] == "localhost":
                if parts[0] not in ("docker.io", "index.docker.io"):
                    return {"published": None, "why": f"registry {parts[0]} is not queried"}
                parts = parts[1:]
            if len(parts) == 1:
                parts = ["library"] + parts
            if not tag:
                return {"published": None, "why": "no tag to look up"}
            api = f"https://hub.docker.com/v2/repositories/{quote(parts[0])}/{quote('/'.join(parts[1:]))}/tags"
            t = fetch_json(f"{api}/{quote(tag)}")
            if not digest or t.get("digest") == digest:
                return {"published": t.get("tag_last_pushed") or t.get("last_updated"), "date_from": f"tag {tag}"}
            # The tag has moved on since the pin. Docker Hub keeps no history
            # per digest, but the tags still pointing at it carry a push date,
            # and the digest is at least as old as the earliest of them.
            url, seen = f"{api}?page_size=100&ordering=last_updated", []
            for _ in range(HUB_PAGES):
                page = fetch_json(url)
                seen += [(r.get("tag_last_pushed"), r["name"]) for r in page.get("results", [])
                         if r.get("digest") == digest and r.get("tag_last_pushed")]
                url = page.get("next")
                if not url:
                    break
            if not seen:
                return {"published": None, "why": f"no recent tag points at the pinned digest (searched {HUB_PAGES} pages)"}
            when, name = min(seen)
            return {"published": when, "date_from": f"tag {name}"}
        return self._memo(("image", repo, tag, digest), go)

    def osv(self, queries):
        """{index: [vuln id]} for [(ecosystem, name, version)]; one call."""
        if not queries:
            return {}
        body = {"queries": [{"package": {"ecosystem": e, "name": n}, "version": v} for e, n, v in queries]}
        res = fetch_json("https://api.osv.dev/v1/querybatch", CAP_API, body)["results"]
        return {i: sorted({x["id"] for x in r.get("vulns", [])}) for i, r in enumerate(res)}


def collect(changes, reg):
    """Fill c.facts (and c.facts['old']) from the registry for every change."""
    for c in changes:
        if c.eco == "cargo":
            if c.lock_new.get("source") != CRATES_IO:
                c.facts = {"error": f"source {c.lock_new.get('source')} is not crates.io"}
                continue
            c.facts = dict(reg.crate(c.name, c.new))
            if c.old and c.old != c.new:
                c.facts["old"] = reg.crate(c.name, c.old)
        elif c.eco == "npm":
            c.facts = dict(reg.npm(c.name, c.new))
            if c.old and c.old != c.new:
                c.facts["old"] = old = reg.npm(c.name, c.old)
                if c.facts.get("trusted") and c.facts.get("publisher") != old.get("publisher"):
                    c.facts["prov"] = reg.provenance(c.name, c.new)
        elif c.eco == "actions":
            c.facts = dict(reg.action(c.name, c.new))
        elif c.eco == "docker":
            m = c.lock_new
            c.facts = dict(reg.image(c.name, m.get("tag"), m.get("digest")))


def osv_lookup(changes, reg):
    """Attach OSV ids to each change; one querybatch per ecosystem."""
    for eco, osv_eco in OSV_ECOSYSTEM.items():
        rows, queries = [], []
        for c in changes:
            if c.eco != eco:
                continue
            version = c.new
            if eco == "actions":
                version = c.facts.get("version_tag") or (c.new if re.fullmatch(r"v?\d+(\.\d+){2}", c.new) else None)
                if not version:
                    c.facts["osv"] = "no version to query (major tag only)"
                    continue
                version = version.lstrip("v")
            rows.append(c)
            queries.append((osv_eco, "/".join(c.name.split("/")[:2]) if eco == "actions" else c.name, version))
        if not queries:
            continue
        try:
            found = reg.osv(queries)
        except (urllib.error.URLError, OSError, ValueError, Refused, KeyError) as e:
            for c in rows:
                c.facts["osv_error"] = f"{type(e).__name__}: {e}"[:200]
            continue
        for i, c in enumerate(rows):
            c.advisories += [a for a in found.get(i, []) if a not in c.advisories]


# --------------------------------------------------------------------------
# advisory tools on the head tree


def run_cargo_deny(binary, tree, lockfiles, config):
    """{(name, version): [ids]} and a list of tool errors."""
    hits, errors = {}, []
    for lock in lockfiles:
        manifest = os.path.join(tree, os.path.dirname(lock), "Cargo.toml")
        cmd = [binary, "--format", "json", "--log-level", "warn", "--color", "never",
               "--manifest-path", manifest, "--config", config, "--locked", "check", "advisories"]
        try:
            r = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=900)
        except (OSError, subprocess.TimeoutExpired) as e:
            errors.append(f"cargo-deny on {lock}: {e}")
            continue
        seen = 0
        for line in r.stderr.splitlines() + r.stdout.splitlines():
            try:
                d = json.loads(line)
            except ValueError:
                continue
            f = d.get("fields") or {}
            adv = f.get("advisory")
            if not adv:
                continue
            seen += 1
            for g in f.get("graphs") or []:
                k = g.get("Krate") or {}
                hits.setdefault((k.get("name"), k.get("version")), set()).add(adv.get("id"))
        if r.returncode != 0 and not seen:
            errors.append(f"cargo-deny on {lock} exited {r.returncode}: {r.stderr.strip()[-300:]}")
    return {k: sorted(v) for k, v in hits.items()}, errors


def run_npm_audit(npm, tree, lockfiles):
    hits, errors = {}, []
    for lock in lockfiles:
        d = os.path.join(tree, os.path.dirname(lock))
        cmd = [npm, "audit", "--package-lock-only", "--json", "--registry", NPM_REGISTRY]
        try:
            r = subprocess.run(cmd, check=False, cwd=d, capture_output=True, text=True, timeout=300)
            doc = json.loads(r.stdout)
            with open(os.path.join(d, "package-lock.json"), encoding="utf-8") as fh:
                packages = json.load(fh)["packages"]
        except (OSError, subprocess.TimeoutExpired, ValueError, KeyError) as e:
            errors.append(f"npm audit on {lock}: {e}")
            continue
        if "error" in doc:
            errors.append(f"npm audit on {lock}: {json.dumps(doc['error'])[:300]}")
            continue
        for name, v in (doc.get("vulnerabilities") or {}).items():
            ids = set()
            for via in v.get("via") or []:
                if isinstance(via, dict):
                    ids.add((via.get("url") or "").rsplit("/", 1)[-1] or via.get("title") or "advisory")
            if not ids:
                continue  # only inherited through a vulnerable dependency, which has its own entry
            for node in v.get("nodes") or []:
                ver = (packages.get(node) or {}).get("version")
                hits.setdefault((name, ver), set()).update(ids)
    return {k: sorted(v) for k, v in hits.items()}, errors


def deny_ignores(text):
    try:
        ign = tomllib.loads(text).get("advisories", {}).get("ignore", []) if text else []
    except tomllib.TOMLDecodeError:
        return set()
    return {(i["id"] if isinstance(i, dict) else i) for i in ign}


# --------------------------------------------------------------------------
# the rules (pure: facts in, verdict out)


def age_days(published, now):
    t = parse_time(published)
    return None if t is None else (now - t).total_seconds() / 86400


def judge(c, now, ignored=frozenset()):
    """Fill c.fails and c.signals from c.facts and c.advisories."""
    f = c.facts
    old = f.get("old") or {}
    sig = c.signals

    if "error" in f:
        c.fails.append(f"lookup failed: {f['error']}")
    age = age_days(f.get("published"), now)
    if age is None:
        c.fails.append("publish date unknown" + (f" ({f['why']})" if f.get("why") else ""))
    elif age < MIN_AGE_DAYS:
        c.fails.append(f"published {age:.1f} days ago, under {MIN_AGE_DAYS}")
    c.facts["age"] = age

    kept = [a for a in c.advisories if a not in ignored]
    for a in c.advisories:
        if a in ignored:
            sig.append((f"{a} ignored by deny.toml", False))
    if kept:
        c.fails.append("advisory " + ", ".join(kept))
    if f.get("osv_error"):
        c.fails.append("OSV lookup failed")

    if c.old is None and c.eco in ("cargo", "npm"):
        by = ", ".join(c.required_by) or "a manifest"
        sig.append((f"new package, required by {by}", False))
    if c.alongside:
        sig.append((f"added next to {c.old}, which stays", False))

    if c.eco == "cargo" and "error" not in f:
        if f.get("yanked"):
            sig.append(("yanked", True))
        if c.lock_new.get("checksum") and f.get("checksum") and c.lock_new["checksum"] != f["checksum"]:
            sig.append(("Cargo.lock checksum differs from crates.io", True))
        if f.get("checksum") and f.get("sha256") and f["checksum"] != f["sha256"]:
            sig.append(("downloaded .crate does not match its crates.io checksum", True))
        if "error" in old:
            sig.append((f"previous version unreadable: {old['error']}", True))
        if f.get("build") and not old.get("build"):
            sig.append(("adds a build script (build.rs)" if c.old else "has a build script (build.rs)", True))
        if f.get("proc_macro") and not old.get("proc_macro"):
            sig.append(("becomes a proc-macro" if c.old else "is a proc-macro", True))
        if old and old.get("trusted") and not f.get("trusted"):
            sig.append(("trusted publishing dropped", True))

    if c.eco == "npm" and "error" not in f:
        resolved = c.lock_new.get("resolved")
        if resolved and not resolved.startswith(NPM_REGISTRY):
            sig.append((f"resolved outside {NPM_REGISTRY}", True))
        if "error" in old:
            sig.append((f"previous version unreadable: {old['error']}", True))
        elif old:
            if old.get("publisher") != f.get("publisher"):
                sig.append(publisher_signal(c, f, old))
            if old.get("provenance") and not f.get("provenance"):
                sig.append(("provenance dropped", True))
            added = sorted(set(f.get("deps") or []) - set(old.get("deps") or []))
            if added:
                c.facts["added_deps"] = added
        install_now = f.get("install") or c.lock_new.get("install")
        install_before = old.get("install") or c.lock_old.get("install")
        if install_now and not install_before:
            sig.append(("install script" + (" newly added" if c.old else ""), True))
        if f.get("deprecated"):
            sig.append((f"deprecated: {str(f['deprecated'])[:80]}", False))

    if c.eco == "actions" and f.get("sha"):
        sig.append((f"{c.new} = commit {f['sha'][:12]}", False))
        if f.get("branch"):
            sig.append((f"{c.new} is a branch, not a tag", True))
        if f.get("osv"):
            sig.append((f["osv"], False))

    if c.eco == "docker":
        od, nd = c.lock_old.get("digest"), c.lock_new.get("digest")
        if od != nd:
            sig.append((f"digest {short(od)} -> {short(nd)}", False))
        if f.get("date_from"):
            sig.append((f"date of {f['date_from']}", False))

    c.fails += [s for s, bad in sig if bad]


def repo_id(url):
    """github.com/owner/repo of a repository URL in any of npm's spellings."""
    if not url:
        return None
    u = url.strip().lower().split("#")[0]
    u = re.sub(r"^git\+", "", u)
    u = re.sub(r"^[a-z]+://", "", u)
    u = re.sub(r"^git@([^:/]+)[:/]", r"\1/", u)
    u = re.sub(r"^github:", "github.com/", u)
    return re.sub(r"(\.git)?/*$", "", u) or None


def npm_sha512_hex(integrity):
    for part in (integrity or "").split():
        if part.startswith("sha512-"):
            try:
                return base64.b64decode(part[7:]).hex()
            except ValueError:
                return None
    return None


def publisher_signal(c, f, old):
    """A publisher change fails, except a move to trusted publishing whose
    provenance names the repository the previous version declared and the
    exact tarball the lockfile pins."""
    was, now = old.get("publisher"), f.get("publisher")
    text = f"publisher {was} -> {now}"
    prov = f.get("prov") or {}
    if not (f.get("trusted") and f.get("provenance")) or "error" in prov:
        return (text + (f" ({prov['error']})" if "error" in prov else ""), True)
    built, declared = repo_id(prov.get("repo")), repo_id(old.get("repository"))
    if not built or built != declared:
        return (f"{text}: provenance repo {built} is not the declared {declared}", True)
    pinned = npm_sha512_hex(c.lock_new.get("integrity"))
    if pinned and pinned not in (prov.get("sha512") or []):
        return (f"{text}: provenance is for another tarball than the lockfile's", True)
    return (f"{text} (trusted publishing from {built}, provenance matches)", False)


def explain_new_deps(changes, base_names):
    """An npm version's new dependency is explained when it was already in the
    tree or has a passing row of its own; anything else fails the parent."""
    rows = {(c.eco, c.name): c for c in changes}
    for c in changes:
        for dep in c.facts.get("added_deps", []):
            row = rows.get((c.eco, dep))
            if row is not None and not row.fails:
                c.signals.append((f"adds dependency {dep} (vetted below)", False))
            elif row is not None:
                c.signals.append((f"adds dependency {dep}, which fails", True))
                c.fails.append(f"adds dependency {dep}, which fails")
            elif dep in base_names.get(c.eco, set()):
                c.signals.append((f"adds dependency {dep} (already in the tree)", False))
            else:
                c.signals.append((f"adds dependency {dep}, not in the lockfile", True))
                c.fails.append(f"adds dependency {dep}, not in the lockfile")


def short(d):
    return d.split(":", 1)[-1][:12] if d else "none"


# --------------------------------------------------------------------------
# report


def cell(s):
    return str(s).replace("|", "\\|").replace("\n", " ")


def render(changes, removed, notes, now, header, tool_failed=False):
    n = len(changes)
    bad = [c for c in changes if c.fails]
    lines = []
    if tool_failed:
        lines.append(f"**Do not approve yet: an advisory tool failed** (notes below); {len(bad)} of {n} packages fail the vet.")
    elif not changes:
        lines.append("No dependency version changed in this pull request.")
    elif not bad:
        lines.append(f"**Safe to approve: {n} packages, all >= {MIN_AGE_DAYS} days, no advisories, no supply-chain signals**")
        new = sum(1 for c in changes if c.old is None and c.eco in ("cargo", "npm"))
        if new:
            lines.append("")
            lines.append(f"{new} of them are new to a lockfile; each one is vetted in its own row.")
    else:
        lines.append(f"**Do not approve yet: {len(bad)} of {n} packages fail the vet** (Verdict column).")
    lines += ["", header, ""]
    if changes:
        lines.append("| Package | Ecosystem | From -> To | Published | Age (days) | Advisories | Supply-chain signals | Verdict |")
        lines.append("|---|---|---|---|---|---|---|---|")
        order = {"cargo": 0, "npm": 1, "actions": 2, "docker": 3}
        for c in sorted(changes, key=lambda c: (bool(not c.fails), order[c.eco], c.name, vkey(c.new))):
            f = c.facts
            primary = c.where in (["Cargo.lock"], ["www/package-lock.json"])
            eco = c.eco + (f" ({', '.join(c.where)})" if c.eco in ("cargo", "npm") and not primary else "")
            pub = (parse_time(f.get("published")) or "unknown")
            pub = pub.strftime("%Y-%m-%d") if not isinstance(pub, str) else pub
            age = f.get("age")
            age = "unknown" if age is None else f"{age:.0f}"
            if c.eco == "docker" and age != "unknown":
                age = f">= {age}"
            adv = ", ".join(c.advisories) or ("n/a (no image feed)" if c.eco == "docker" else "none")
            signals = "; ".join(s for s, _ in c.signals) or "none"
            verdict = "FAIL: " + "; ".join(c.fails) if c.fails else "pass"
            frm = c.old or "(new)"
            if c.eco == "docker":
                frm, new = (re.sub(r"(sha256:[0-9a-f]{12})[0-9a-f]+", r"\1", x) for x in (frm, c.new))
            else:
                new = c.new
            lines.append("| " + " | ".join(cell(x) for x in (c.name, eco, f"{frm} -> {new}", pub, age, adv, signals, verdict)) + " |")
    if removed:
        lines += ["", "Removed (not vetted): " + ", ".join(removed)]
    for note in notes:
        lines += ["", note]
    rule = (f"Vetted at {now.strftime('%Y-%m-%d %H:%M UTC')}; rule: published >= {MIN_AGE_DAYS} days before this run, "
            "no OSV/cargo-deny/npm-audit advisory on the new version, no unexplained supply-chain signal.")
    lines += ["", rule]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------
# commands


def check(args):
    now = (parse_time(args.now) if args.now else dt.datetime.now(dt.UTC)).astimezone(dt.UTC)
    base = git("merge-base", args.base, args.head).strip()
    head = git("rev-parse", args.head).strip()
    paths = [p for p in git("diff", "--name-only", base, head).split("\n") if p and kind_of(p)]
    files = {p: (git_show(base, p), git_show(head, p)) for p in paths}
    changes, removed, used = diff_files(files)
    base_names = {}
    for p, (btxt, _) in files.items():
        k = kind_of(p)
        if k == "cargo":
            base_names.setdefault(k, set()).update(n for n, _ in cargo_packages(btxt))
        elif k == "npm":
            base_names.setdefault(k, set()).update(n for n, _ in npm_packages(btxt))

    reg = Registry()
    collect(changes, reg)
    osv_lookup(changes, reg)

    notes = []
    ignored = deny_ignores(git_show(base, "deny.toml"))
    tool_hits = {}
    cargo_locks = [p for p in used if kind_of(p) == "cargo" and git_show(head, p)]
    npm_locks = [p for p in used if kind_of(p) == "npm" and git_show(head, p)]
    if args.cargo_deny and cargo_locks:
        with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as t:
            t.write(git_show(base, "deny.toml") or "[advisories]\n")
        try:
            hits, errors = run_cargo_deny(args.cargo_deny, args.tree, cargo_locks, t.name)
        finally:
            os.unlink(t.name)
        tool_hits["cargo"] = hits
        notes += [f"cargo-deny error: {e}" for e in errors]
    if args.npm and npm_locks:
        hits, errors = run_npm_audit(args.npm, args.tree, npm_locks)
        tool_hits["npm"] = hits
        notes += [f"npm audit error: {e}" for e in errors]
    changed = {(c.eco, c.name, c.new) for c in changes}
    for eco, hits in tool_hits.items():
        for c in changes:
            if c.eco == eco:
                c.advisories += [a for a in hits.get((c.name, c.new), []) if a not in c.advisories]
        other = sorted(f"{n} {v}: {', '.join(i for i in ids if i not in ignored)}" for (n, v), ids in hits.items()
                       if (eco, n, v) not in changed and any(i not in ignored for i in ids))
        if other:
            notes.append(f"Already in the tree before this change, not failing it ({eco}): " + "; ".join(other))

    for c in changes:
        judge(c, now, ignored if c.eco == "cargo" else frozenset())
    explain_new_deps(changes, base_names)
    tool_failed = any(n.startswith(("cargo-deny error", "npm audit error")) for n in notes)

    header = f"Compared `{base[:12]}` (merge base) with `{head[:12]}`: " + (", ".join(f"`{p}`" for p in used) or "no dependency file")
    report = render(changes, removed, notes, now, header, tool_failed)
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as fh:
            fh.write(report)
    else:
        sys.stdout.write(report)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump([{"eco": c.eco, "name": c.name, "old": c.old, "new": c.new, "fails": c.fails,
                        "signals": [s for s, _ in c.signals], "advisories": c.advisories} for c in changes], fh, indent=1)
    return 1 if tool_failed or any(c.fails for c in changes) else 0


# --------------------------------------------------------------------------
# selftest: fixture lockfiles, canned registry answers, no network


LOCK_BASE = """
version = 4
[[package]]
name = "app"
version = "0.1.0"
dependencies = ["sha2", "serde"]

[[package]]
name = "sha2"
version = "0.11.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "aa"

[[package]]
name = "serde"
version = "1.0.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "s1"

[[package]]
name = "digest"
version = "0.10.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "d0"

[[package]]
name = "gone"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "g1"
"""

LOCK_HEAD = """
version = 4
[[package]]
name = "app"
version = "0.1.0"
dependencies = ["sha2", "serde"]

[[package]]
name = "sha2"
version = "0.11.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "bb"
dependencies = ["cpufeatures", "digest 0.11.0"]

[[package]]
name = "serde"
version = "1.0.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "s2"

[[package]]
name = "digest"
version = "0.10.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "d0"

[[package]]
name = "digest"
version = "0.11.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "d1"

[[package]]
name = "cpufeatures"
version = "0.3.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "cf"
"""

NPM_BASE = {"lockfileVersion": 3, "packages": {
    "": {"name": "www", "dependencies": {"vite": "^8"}},
    "node_modules/vite": {"version": "8.3.5", "resolved": "https://registry.npmjs.org/vite/-/vite-8.3.5.tgz", "integrity": "sha512-a"},
    "node_modules/@scope/pkg": {"version": "1.0.0", "resolved": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.0.0.tgz", "integrity": "sha512-b"},
    "node_modules/postcss": {"version": "8.0.0", "resolved": "https://registry.npmjs.org/postcss/-/postcss-8.0.0.tgz"},
}}
NPM_HEAD = {"lockfileVersion": 3, "packages": {
    "": {"name": "www", "dependencies": {"vite": "^8"}},
    "node_modules/vite": {"version": "8.3.6", "resolved": "https://registry.npmjs.org/vite/-/vite-8.3.6.tgz",
                          "integrity": "sha512-c", "dependencies": {"postcss": "^8", "evil": "1"}},
    "node_modules/@scope/pkg": {"version": "1.1.0", "resolved": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.1.0.tgz",
                                "integrity": "sha512-d", "hasInstallScript": True},
    "node_modules/postcss": {"version": "8.0.0", "resolved": "https://registry.npmjs.org/postcss/-/postcss-8.0.0.tgz"},
    "node_modules/evil": {"version": "1.0.0", "resolved": "https://evil.example/evil-1.0.0.tgz"},
    "node_modules/vite/node_modules/@scope/pkg": {"version": "1.0.0"},
}}

WF_BASE = """
jobs:
  a:
    steps:
      - uses: actions/checkout@v6
      - uses: ./.github/actions/failure-issue
      - uses: "docker/build-push-action@v7" # pinned
      - uses: docker://alpine:3.20
"""
WF_HEAD = WF_BASE.replace("checkout@v6", "checkout@v7").replace("alpine:3.20", "alpine:3.21")

DOCKER_BASE = """FROM rust:1-slim@sha256:aaaa1111aaaa1111 AS chef
FROM chef AS build
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cccc
"""
DOCKER_HEAD = DOCKER_BASE.replace("sha256:aaaa1111aaaa1111", "sha256:bbbb2222bbbb2222")
COMPOSE_BASE = "services:\n  db:\n    image: ${DB_IMAGE:-timescale/timescaledb-ha:pg17}\n  b:\n    image: antares-local:latest\n"


def _crate_blob(name, version, build=None, proc=False, build_rs=False):
    manifest = f'[package]\nname = "{name}"\nversion = "{version}"\n'
    if build is not None:
        manifest += f"build = {json.dumps(build)}\n"
    if proc:
        manifest += "[lib]\nproc-macro = true\n"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        members = {"Cargo.toml": manifest, "src/lib.rs": ""}
        if build_rs:
            members["build.rs"] = "fn main() {}"
        for fn, body in members.items():
            data = body.encode()
            ti = tarfile.TarInfo(f"{name}-{version}/{fn}")
            ti.size = len(data)
            tar.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


class _Canned(Registry):
    def __init__(self, crates=None, npm=None, actions=None, images=None, osv=None):
        super().__init__()
        self.data = {"crate": crates or {}, "npm": npm or {}, "action": actions or {}, "image": images or {}}
        self.osv_ids = osv or {}

    def crate(self, name, version):
        return self.data["crate"].get((name, version), {"error": "not canned"})

    def npm(self, name, version):
        return self.data["npm"].get((name, version), {"error": "not canned"})

    def action(self, name, ref):
        return self.data["action"].get((name, ref), {"error": "not canned"})

    def image(self, repo, tag, digest):
        return self.data["image"].get(repo, {"published": None, "why": "not canned"})

    def osv(self, queries):
        return {i: self.osv_ids.get((n, v), []) for i, (_, n, v) in enumerate(queries)}


def selftest():
    failures = []

    def expect(cond, what):
        if not cond:
            failures.append(what)

    now = dt.datetime(2026, 11, 17, 12, tzinfo=dt.UTC)
    old_day, young_day = "2026-10-01T00:00:00Z", "2026-11-14T00:00:00Z"

    # kind_of
    expect(kind_of("fuzz/Cargo.lock") == "cargo", "fuzz lockfile")
    expect(kind_of("www/package-lock.json") == "npm", "npm lockfile")
    expect(kind_of(".github/workflows/ci.yml") == "actions", "workflow")
    expect(kind_of(".github/actions/x/action.yml") == "actions", "composite action")
    expect(kind_of("compose-files/docker-compose-etsi.yml") == "docker", "compose file")
    expect(kind_of("examples/subscriptions/mqtt-compose.yml") == "docker", "compose suffix")
    expect(kind_of("Dockerfile.wasm") == "docker", "Dockerfile.wasm")
    expect(kind_of("Cargo.toml") is None and kind_of("README.md") is None, "non-lockfiles ignored")

    # cargo diff: bump, new transitive, second version alongside, checksum swap, removal
    ch, rm, _ = diff_files({"Cargo.lock": (LOCK_BASE, LOCK_HEAD)})
    by = {(c.name, c.new): c for c in ch}
    expect(set(by) == {("sha2", "0.11.1"), ("cpufeatures", "0.3.0"), ("digest", "0.11.0"), ("serde", "1.0.1")}, f"cargo rows {sorted(by)}")
    expect(by[("sha2", "0.11.1")].old == "0.11.0", "sha2 paired with its old version")
    expect(by[("cpufeatures", "0.3.0")].old is None and by[("cpufeatures", "0.3.0")].required_by == ["sha2 0.11.1"], "new transitive crate named with its parent")
    expect(by[("digest", "0.11.0")].alongside and by[("digest", "0.11.0")].old == "0.10.0", "second version alongside the old")
    expect(any("checksum changed" in s for s, _ in by[("serde", "1.0.1")].signals), "same version, new checksum flagged")
    expect(rm == ["gone 1.0.0 (Cargo.lock)"], f"removed {rm}")
    expect(all(c.name != "app" for c in ch), "workspace members are not rows")

    # the same bump in two lockfiles is one row
    ch2, _, _ = diff_files({"Cargo.lock": (LOCK_BASE, LOCK_HEAD), "fuzz/Cargo.lock": (LOCK_BASE, LOCK_HEAD)})
    sha = [c for c in ch2 if c.name == "sha2"]
    expect(len(sha) == 1 and sha[0].where == ["Cargo.lock", "fuzz/Cargo.lock"], "one row per change across lockfiles")

    # npm diff: scoped names, nested paths, new package, install script
    nch, nrm, _ = diff_files({"www/package-lock.json": (json.dumps(NPM_BASE), json.dumps(NPM_HEAD))})
    nby = {(c.name, c.new): c for c in nch}
    expect(set(nby) == {("vite", "8.3.6"), ("@scope/pkg", "1.1.0"), ("evil", "1.0.0")}, f"npm rows {sorted(nby)}")
    expect(nby[("@scope/pkg", "1.1.0")].old == "1.0.0" and nby[("@scope/pkg", "1.1.0")].alongside, "scoped pair, old copy stays nested")
    expect(not any(c.signals for c in nch), "a nested copy without `resolved` is not a changed tarball")
    expect(nby[("evil", "1.0.0")].required_by == ["vite 8.3.6"], "npm new package named with its parent")
    expect(nrm == [], f"npm removed {nrm}")

    # actions and images
    ach, _, _ = diff_files({".github/workflows/ci.yml": (WF_BASE, WF_HEAD)})
    expect(sorted((c.eco, c.name, c.old, c.new) for c in ach) == [("actions", "actions/checkout", "v6", "v7"), ("docker", "alpine", "3.20", "3.21")],
           f"workflow rows {[(c.name, c.old, c.new) for c in ach]}")
    expect(kind_of("deploy/k8s/nats.yaml") == "docker", "k8s manifests carry images")
    expect(action_refs(WF_BASE) == {("actions/checkout", "v6"), ("docker/build-push-action", "v7")}, "local and docker:// uses skipped")
    expect(image_refs("Dockerfile", DOCKER_BASE) == {"rust:1-slim@sha256:aaaa1111aaaa1111", "gcr.io/distroless/cc-debian13:nonroot@sha256:cccc"}, "stage names skipped")
    expect(image_refs("compose.yml", COMPOSE_BASE) == {"timescale/timescaledb-ha:pg17", "antares-local:latest"}, "compose default image")
    expect(split_image("gcr.io/x/y:tag@sha256:1") == ("gcr.io/x/y", "tag", "sha256:1"), "split image")
    expect(split_image("localhost:5000/y") == ("localhost:5000/y", None, None), "registry port is not a tag")
    dch, _, _ = diff_files({"Dockerfile": (DOCKER_BASE, DOCKER_HEAD)})
    expect([(c.name, c.old, c.new) for c in dch] == [("rust", "1-slim@sha256:aaaa1111aaaa1111", "1-slim@sha256:bbbb2222bbbb2222")], "digest bump row")

    # crate manifests: build script and proc-macro detection without extracting
    expect(crate_manifest_facts(_crate_blob("a", "1.0.0"), "a", "1.0.0") == {"build": False, "proc_macro": False}, "plain crate")
    expect(crate_manifest_facts(_crate_blob("a", "1.0.0", build_rs=True), "a", "1.0.0")["build"], "auto-detected build.rs")
    expect(crate_manifest_facts(_crate_blob("a", "1.0.0", build="build.rs", build_rs=True), "a", "1.0.0")["build"], "explicit build")
    expect(not crate_manifest_facts(_crate_blob("a", "1.0.0", build=False, build_rs=True), "a", "1.0.0")["build"], "build = false")
    expect(crate_manifest_facts(_crate_blob("a", "1.0.0", proc=True), "a", "1.0.0")["proc_macro"], "proc-macro")

    # verdicts
    clean = {"published": old_day, "yanked": False, "trusted": True, "build": False, "proc_macro": False}
    reg = _Canned(
        crates={("sha2", "0.11.1"): {**clean, "checksum": "bb", "sha256": "bb"},
                ("sha2", "0.11.0"): {**clean, "checksum": "aa", "sha256": "aa"},
                ("cpufeatures", "0.3.0"): {**clean, "checksum": "cf", "sha256": "cf", "build": True},
                ("digest", "0.11.0"): {**clean, "published": young_day, "checksum": "d1", "sha256": "d1"},
                ("digest", "0.10.0"): {**clean, "checksum": "d0", "sha256": "d0"},
                ("serde", "1.0.1"): {**clean, "checksum": "s1", "sha256": "s1"}},
        osv={("sha2", "0.11.1"): ["RUSTSEC-2024-0436"]},
    )
    collect(ch, reg)
    osv_lookup(ch, reg)
    for c in ch:
        judge(c, now, {"RUSTSEC-2024-0436"})
    expect(not by[("sha2", "0.11.1")].fails, f"clean bump passes, ignored advisory noted: {by[('sha2', '0.11.1')].fails}")
    expect(any("ignored by deny.toml" in s for s, _ in by[("sha2", "0.11.1")].signals), "ignored advisory is shown")
    expect(any("build script" in f for f in by[("cpufeatures", "0.3.0")].fails), "new crate with a build script fails")
    expect(any("under 7" in f for f in by[("digest", "0.11.0")].fails), "young release fails")
    expect(any("checksum" in f for f in by[("serde", "1.0.1")].fails), "checksum swap fails")

    def one(eco, facts, old="1.0.0", new="1.1.0", advisories=(), lock_new=None, lock_old=None, ignored=frozenset()):
        c = Change(eco, "p", old, new, ["x"], lock_new=lock_new or {}, lock_old=lock_old or {})
        c.facts, c.advisories = facts, list(advisories)
        judge(c, now, ignored)
        return c

    expect(not one("cargo", {**clean, "old": clean}).fails, "cargo clean")
    expect(one("cargo", {**clean, "published": None}).fails == ["publish date unknown"], "unknown date fails")
    expect(any("RUSTSEC-2099-0001" in f for f in one("cargo", clean, advisories=["RUSTSEC-2099-0001"]).fails), "advisory fails")
    expect("yanked" in one("cargo", {**clean, "yanked": True, "old": clean}).fails, "yanked fails")
    expect(any("adds a build script" in f for f in one("cargo", {**clean, "build": True, "old": clean}).fails), "build.rs added fails")
    expect(not one("cargo", {**clean, "build": True, "old": {**clean, "build": True}}).fails, "build.rs kept passes")
    expect(any("proc-macro" in f for f in one("cargo", {**clean, "proc_macro": True, "old": clean}).fails), "becomes proc-macro fails")
    expect(any("trusted publishing" in f for f in one("cargo", {**clean, "trusted": False, "old": clean}).fails), "trusted publishing dropped fails")
    expect(any("differs from crates.io" in f for f in one("cargo", {**clean, "checksum": "x", "old": clean}, lock_new={"checksum": "y"}).fails), "lock checksum mismatch fails")
    expect(any("OSV" in f for f in one("cargo", {**clean, "osv_error": "timeout"}).fails), "OSV outage fails")

    npm_ok = {"published": old_day, "publisher": "alice", "install": False, "deps": ["a"], "provenance": True}
    expect(not one("npm", {**npm_ok, "old": npm_ok}).fails, "npm clean")
    expect(any("publisher alice -> mallory" in f for f in one("npm", {**npm_ok, "publisher": "mallory", "old": npm_ok}).fails), "publisher change fails")
    expect(any("install script" in f for f in one("npm", {**npm_ok, "install": True, "old": npm_ok}).fails), "install script added fails")
    expect(any("install script" in f for f in one("npm", {**npm_ok, "old": npm_ok}, lock_new={"install": True}).fails), "lockfile hasInstallScript added fails")
    expect(not one("npm", {**npm_ok, "install": True, "old": {**npm_ok, "install": True}}).fails, "install script kept passes")
    expect(any("provenance" in f for f in one("npm", {**npm_ok, "provenance": False, "old": npm_ok}).fails), "provenance dropped fails")
    # a move to trusted publishing passes only when provenance names the declared repo and the pinned tarball
    tarball = hashlib.sha512(b"tgz").digest()
    integ = "sha512-" + base64.b64encode(tarball).decode()
    human = {**npm_ok, "publisher": "mbostock", "provenance": False, "repository": "git+https://github.com/d3/d3-color.git"}
    oidc = {**npm_ok, "publisher": "GitHub Actions", "trusted": True, "provenance": True,
            "prov": {"repo": "https://github.com/d3/d3-color", "sha512": [tarball.hex()]}}
    expect(not one("npm", {**oidc, "old": human}, lock_new={"integrity": integ}).fails, "move to trusted publishing with matching provenance passes")
    expect(one("npm", {**oidc, "prov": {**oidc["prov"], "repo": "https://github.com/evil/d3-color"}, "old": human}, lock_new={"integrity": integ}).fails, "provenance from another repo fails")
    expect(one("npm", {**oidc, "old": human}, lock_new={"integrity": "sha512-" + base64.b64encode(b"x" * 64).decode()}).fails, "provenance for another tarball fails")
    expect(one("npm", {**oidc, "trusted": False, "old": human}).fails, "a person replacing a person still fails")
    expect(one("npm", {**human, "publisher": "mallory", "old": {**oidc, "provenance": True}}).fails, "trusted publishing to a person fails")
    expect(repo_id("git+ssh://git@github.com/d3/d3-color.git") == repo_id("https://github.com/D3/d3-color/") == "github.com/d3/d3-color", "repo spellings")
    expect(any("resolved outside" in f for f in one("npm", {**npm_ok, "old": npm_ok}, lock_new={"resolved": "https://evil.example/x.tgz"}).fails), "foreign tarball fails")

    # new npm dependencies: explained by a passing row or the old tree, else fail
    p1 = one("npm", {**npm_ok, "deps": ["a", "b", "c", "d"], "old": npm_ok})
    rows = [p1]
    for name, fails in (("b", []), ("c", ["publish date unknown"])):
        r = Change("npm", name, None, "1.0.0", ["x"])
        r.fails = fails
        rows.append(r)
    explain_new_deps(rows, {"npm": {"d"}})
    expect(p1.fails == ["adds dependency c, which fails"], f"new deps judged by their rows: {p1.fails}")
    expect(any("b (vetted below)" in s for s, _ in p1.signals) and any("d (already in the tree)" in s for s, _ in p1.signals), "explained deps listed")
    p2 = one("npm", {**npm_ok, "deps": ["a", "z"], "old": npm_ok})
    explain_new_deps([p2], {"npm": set()})
    expect(p2.fails == ["adds dependency z, not in the lockfile"], "unexplained dependency fails")

    act = one("actions", {"published": old_day, "sha": "3d3c42e5aac5ba805825da76410c181273ba90b1"}, old="v6", new="v7")
    expect(not act.fails and any("commit 3d3c42e5aac5" in s for s, _ in act.signals), "action tag sha recorded")
    expect(any("branch" in f for f in one("actions", {"published": old_day, "sha": "a" * 40, "branch": True}).fails), "branch ref fails")
    img = one("docker", {"published": old_day, "date_from": "tag 1.99.0-slim"}, lock_old={"digest": "sha256:aaaa"}, lock_new={"digest": "sha256:bbbb"})
    expect(not img.fails and any("digest aaaa -> bbbb" in s for s, _ in img.signals), "image digest listed")
    expect(one("docker", {"published": None, "why": "registry gcr.io is not queried"}).fails == ["publish date unknown (registry gcr.io is not queried)"], "unqueried registry fails")

    # the report's first line
    ok = render([one("cargo", {**clean, "old": clean})], [], [], now, "h").splitlines()[0]
    expect(ok == "**Safe to approve: 1 packages, all >= 7 days, no advisories, no supply-chain signals**", f"headline {ok!r}")
    expect(render([one("cargo", {**clean, "yanked": True})], [], [], now, "h").startswith("**Do not approve yet: 1 of 1"), "fail headline")
    expect(render([], [], [], now, "h").startswith("No dependency version changed"), "empty headline")
    expect("a\\|b" in cell("a|b"), "pipes escaped")

    # the network layer refuses what the allow-list does not name
    for url in ("http://crates.io/x", "https://evil.example/x", "https://crates.io.evil.example/x"):
        try:
            _check_url(url)
            failures.append(f"allowed {url}")
        except Refused:
            pass

    for f in failures:
        print(f"FAIL: {f}")
    print(f"deps-vet selftest: {'ok' if not failures else f'{len(failures)} failure(s)'}")
    return 1 if failures else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("selftest", help="diffing and verdict rules on fixtures, no network")
    c = sub.add_parser("check", help="vet the dependency changes between two refs")
    c.add_argument("--base", required=True, help="base ref (the merge base with --head is used)")
    c.add_argument("--head", required=True, help="head ref")
    c.add_argument("--tree", default=".", help="checkout of --head, for cargo-deny and npm audit")
    c.add_argument("--cargo-deny", help="cargo-deny binary: also run `check advisories` on changed Cargo.locks")
    c.add_argument("--npm", help="npm binary: also run `npm audit --package-lock-only` on changed lockfiles")
    c.add_argument("--summary", help="append the markdown report here (the job summary) instead of stdout")
    c.add_argument("--json", help="write the rows as JSON here")
    c.add_argument("--now", help="ISO time to judge ages against (default: now)")
    args = ap.parse_args()
    return selftest() if args.cmd == "selftest" else check(args)


if __name__ == "__main__":
    sys.exit(main())
