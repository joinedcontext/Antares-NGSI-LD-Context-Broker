#!/usr/bin/env bash
# The official ETSI NGSI-LD test suite, fetched fresh for ONE run.
#
# Nothing of the ETSI suite lives in this repository: every run clones
# ETSI_UPSTREAM_URL at ETSI_REF into a temp dir the caller owns and deletes,
# and runs it as published. Adaptation to this harness happens in the runner
# (robot --variable), never in the fetched tree.
#
#   dev/etsi-upstream.sh fetch <dir>             clone into <dir>/ngsi-ld-test-suite;
#                                                writes <dir>/etsi-upstream.txt
#                                                (url ref commit date status);
#                                                exit 3 = forge unreachable
#   dev/etsi-upstream.sh changed <suite> <base>  name-status of every file changed
#                                                between commit <base> and the
#                                                checkout (fetches <base> when the
#                                                clone lacks it); exit 4 = <base>
#                                                not reachable upstream
#   dev/etsi-upstream.sh suites <suite>          the serial robot dirs of that copy
#   dev/etsi-upstream.sh venv <suite> <venv>     a venv with that copy's own
#                                                requirements.txt, release cooldown on
#
# Env: ETSI_UPSTREAM_URL (default the forge repo), ETSI_REF (branch, tag or
# commit; default develop), ETSI_FETCH_TIMEOUT (seconds, default 300).
#
# The tree is untrusted data until a runner uses it: the clone runs no hooks,
# follows no submodules, offers no credentials (the credential helpers are
# switched off, so a forge auth prompt can never receive another host's
# token) and executes nothing at fetch time.
set -euo pipefail

URL="${ETSI_UPSTREAM_URL:-https://forge.etsi.org/rep/data/ngsi-ld-test-suite.git}"
REF="${ETSI_REF:-develop}"
TIMEOUT="${ETSI_FETCH_TIMEOUT:-300}"
# pip 26.3 is the first release that honours --uploaded-prior-to; a stock
# venv ships an older pip that ignores the cooldown silently.
PIP_PIN="${ETSI_PIP_PIN:-pip==26.3}"
COOLDOWN="${PIP_UPLOADED_PRIOR_TO:-P7D}"

export GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false
GIT=(git -c credential.helper= -c core.hooksPath=/dev/null
     -c protocol.allow=never -c protocol.https.allow=always
     -c advice.detachedHead=false)
# a file:// URL is the offline self-test's stand-in for forge
# (dev/etsi-baseline.py selftest); only https otherwise
case "$URL" in file://*) GIT+=(-c protocol.file.allow=always) ;; esac

case "${1:-}" in
  fetch)
    dir="${2:?usage: fetch <dir>}"
    dst="$dir/ngsi-ld-test-suite"
    mkdir -p "$dst"
    "${GIT[@]}" init -q "$dst"
    "${GIT[@]}" -C "$dst" remote add origin "$URL"
    # blob-less: the whole history of commits and trees, file contents only
    # for the checkout, so `changed` diffs against the baseline commit
    # without a second copy and without downloading every old blob
    if ! timeout "$TIMEOUT" "${GIT[@]}" -C "$dst" fetch -q --filter=blob:none --no-tags origin "$REF" \
       || ! timeout "$TIMEOUT" "${GIT[@]}" -C "$dst" checkout -q FETCH_HEAD; then
      printf 'url=%s\nref=%s\nstatus=skipped: %s unreachable\n' \
        "$URL" "$REF" "$(echo "$URL" | sed -E 's|^[a-z]+://([^/]+).*|\1|')" > "$dir/etsi-upstream.txt"
      echo "ETSI upstream: fetch of $REF from $URL failed or timed out" >&2
      exit 3
    fi
    commit=$("${GIT[@]}" -C "$dst" rev-parse HEAD)
    date=$("${GIT[@]}" -C "$dst" log -1 --format=%cI HEAD)
    printf 'url=%s\nref=%s\ncommit=%s\ndate=%s\nstatus=fetched\n' \
      "$URL" "$REF" "$commit" "$date" > "$dir/etsi-upstream.txt"
    echo "$commit"
    ;;
  changed)
    suite="${2:?usage: changed <suite> <base>}"; base="${3:?usage: changed <suite> <base>}"
    if ! "${GIT[@]}" -C "$suite" cat-file -e "$base^{commit}" 2>/dev/null; then
      timeout "$TIMEOUT" "${GIT[@]}" -C "$suite" fetch -q --filter=blob:none --no-tags origin "$base" 2>/dev/null \
        || { echo "baseline commit $base is not reachable upstream" >&2; exit 4; }
    fi
    # trees only (no rename detection), so the blob-less clone fetches nothing
    "${GIT[@]}" -C "$suite" diff --name-status --no-renames "$base" HEAD
    ;;
  suites)
    suite="${2:?usage: suites <suite>}"
    # every second-level dir under TP/NGSI-LD is one robot run (and one
    # broker reset), taken from the copy itself: the official tree's layout
    # differs from ours and may change with any commit
    ( cd "$suite/TP/NGSI-LD" && find . -mindepth 2 -maxdepth 2 -type d | sed 's|^\./||' | LC_ALL=C sort )
    # a .robot file directly under a top-level dir would otherwise never run
    ( cd "$suite/TP/NGSI-LD" && find . -mindepth 2 -maxdepth 2 -name '*.robot' -printf '%h\n' \
        | sed 's|^\./||' | LC_ALL=C sort -u ) | while read -r top; do
      echo "warning: TP/NGSI-LD/$top has .robot files outside a subdir; they are not run" >&2
    done
    ;;
  venv)
    suite="${2:?usage: venv <suite> <venv>}"; venv="${3:?usage: venv <suite> <venv>}"
    python3 -m venv "$venv"
    "$venv/bin/python" -m pip -q install --disable-pip-version-check "$PIP_PIN"
    # pip runs FROM the suite dir: requirements.txt names the vendored HttpCtrl
    # by a relative editable path. The env form reaches the isolated build
    # environments too.
    # pip's own chatter (setuptools build warnings) goes to a log, shown on failure
    ( cd "$suite" && PIP_UPLOADED_PRIOR_TO="$COOLDOWN" \
        "$venv/bin/pip" -q install --disable-pip-version-check \
          --uploaded-prior-to "$COOLDOWN" -r requirements.txt ) > "$venv/pip-install.log" 2>&1 \
      || { cat "$venv/pip-install.log" >&2; echo "venv: pip install of $suite/requirements.txt failed" >&2; exit 1; }
    "$venv/bin/robot" --version >/dev/null 2>&1 || [ $? = 251 ] \
      || { echo "venv: robot not runnable after install" >&2; exit 1; }
    ;;
  *)
    sed -n '2,29p' "$0"; exit 2 ;;
esac
