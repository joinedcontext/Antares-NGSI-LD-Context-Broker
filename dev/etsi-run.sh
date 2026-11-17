#!/usr/bin/env bash
# Run the ETSI NGSI-LD Robot suite against a local Antares broker.
# Adapted from ScorpioBroker's dev/etsi-serial.sh (the reference recipe).
#
# Env knobs (defaults = local dev box):
#   BROKER_URL     default http://localhost:9090/ngsi-ld/v1
#   SUITE          default ./ngsi-ld-test-suite (vendored copy in this repo)
#   CALLBACK_HOST  host the broker POSTs notifications to (default: localhost)
#   SUITES         robot suite dirs, default: the Scorpio serial order
#   STOP_ON_ERROR  default 1 (local loop: stop at the FIRST failing TP);
#                  CI sets 0 to run the whole suite and report everything
#   TESTSUITE      ours (default) | etsi | both. etsi = the official suite,
#                  fetched fresh (dev/etsi-upstream.sh, ETSI_REF) into a temp
#                  dir with its own venv, both deleted on exit; both = ours,
#                  then etsi into ETSI_RESULTS_DIR (default $RESULTS_DIR-etsi)
#   ETSI_SUITE     an already fetched official tree to run instead (the
#                  pipeline passes its own; the caller owns and deletes it)
#   ETSI_VENV      venv for that tree (default: next to it)
#   ETSI_SUITES    robot dirs of the official copy for TESTSUITE=both
#                  (default: every TP/NGSI-LD/*/* dir of that copy)
#   ETSI_STOP_ON_ERROR  the official copy's STOP_ON_ERROR under both (default
#                  0: its gate, dev/etsi-baseline.py, judges a whole run)
# -e stays off: a failing TP is a result to report, not an abort.
set -uo pipefail
cd "$(dirname "$0")/.."

BROKER_URL="${BROKER_URL:-http://localhost:9090/ngsi-ld/v1}"
SUITE="${SUITE:-./ngsi-ld-test-suite}"
STOP_ON_ERROR="${STOP_ON_ERROR:-1}"
CALLBACK_HOST="${CALLBACK_HOST:-localhost}"
RESULTS_DIR="${RESULTS_DIR:-results}"

# Which copy of the suite: ours (default, ./ngsi-ld-test-suite), etsi (the
# official suite, fetched fresh by dev/etsi-upstream.sh — never stored here),
# or both, one after the other, each with its own results dir and venv.
case "${TESTSUITE:-ours}" in
  ours) ;;
  both)
    TESTSUITE=ours dev/etsi-run.sh; ours_status=$?
    [ "$ours_status" != 0 ] && [ "$STOP_ON_ERROR" = 1 ] && exit "$ours_status"
    TESTSUITE=etsi SUITES="${ETSI_SUITES:-}" STOP_ON_ERROR="${ETSI_STOP_ON_ERROR:-0}" \
      RESULTS_DIR="${ETSI_RESULTS_DIR:-$RESULTS_DIR-etsi}" dev/etsi-run.sh; etsi_status=$?
    exit $(( ours_status || etsi_status )) ;;
  etsi)
    # The official copy, run as published: no file in it is touched. What
    # our copy gets by sed on variables.py it gets as robot --variable.
    # ETSI_SUITE given = a tree the caller fetched (and deletes); unset = fetch one
    # for this run into a temp dir, removed on exit with its venv.
    if [ -z "${ETSI_SUITE:-}" ]; then
      ETSI_TMP=$(mktemp -d)
      trap 'rm -rf "$ETSI_TMP"' EXIT
      mkdir -p "$RESULTS_DIR"
      if ! dev/etsi-upstream.sh fetch "$ETSI_TMP" >/dev/null; then
        cp "$ETSI_TMP/etsi-upstream.txt" "$RESULTS_DIR/"
        echo "ETSI upstream suite skipped: $(sed -n 's/^status=//p' "$RESULTS_DIR/etsi-upstream.txt")"
        exit 0
      fi
      cp "$ETSI_TMP/etsi-upstream.txt" "$RESULTS_DIR/"
      ETSI_SUITE="$ETSI_TMP/ngsi-ld-test-suite"
    fi
    VENV="${ETSI_VENV:-$(dirname "$ETSI_SUITE")/venv}"
    # its own venv from its own requirements.txt (they may pin other versions
    # than ours); HttpCtrl is an editable install out of the fetched tree, so
    # a venv that outlived its tree is rebuilt
    { [ -x "$VENV/bin/robot" ] && "$VENV/bin/python" -c "import HttpCtrl" >/dev/null 2>&1; } \
      || { rm -rf "$VENV"; dev/etsi-upstream.sh venv "$ETSI_SUITE" "$VENV"; } \
      || { echo "venv for the ETSI copy failed"; exit 1; }
    SUITES="${SUITES:-$(dev/etsi-upstream.sh suites "$ETSI_SUITE")}"
    # a copy whose own `url` names no /ngsi-ld/v1 adds the path itself
    api_url=$BROKER_URL
    grep -q "^url = '.*/ngsi-ld/v1/\?'" "$ETSI_SUITE/resources/variables.py" \
      || api_url=${BROKER_URL%/ngsi-ld/v1}
    hostpath="${BROKER_URL#*://}"
    root_path=""
    [ "$hostpath" = "${hostpath#*/}" ] || root_path="/${hostpath#*/}"
    # Same values as our copy's sed lines. Not set: the suite @context and the
    # MQTT host, which stay the copy's own (its context from forge, its own
    # mosquitto on 127.0.0.1 — the brokers are host-networked).
    EXTRA=(--variable "url:$api_url" --variable "temporal_api_url:$api_url"
           --variable "notification_server_host:$CALLBACK_HOST"
           --variable "context_source_host:$CALLBACK_HOST"
           --variable "context_server_host:$CALLBACK_HOST"
           --variable "entities_path:${root_path%/}/entities"
           --exclude config_no_temporal)
    [ "$STOP_ON_ERROR" = 1 ] && EXTRA+=(--exitonfailure)
    [ "${MQTT:-1}" = 1 ] || EXTRA+=(--exclude '*mqtt*')
    mkdir -p "$RESULTS_DIR"
    out=$(cd "$RESULTS_DIR" && pwd)
    status=0
    for s in $SUITES; do
      [ -d "$ETSI_SUITE/TP/NGSI-LD/$s" ] || { echo "ETSI copy has no TP/NGSI-LD/$s"; status=1; continue; }
      name="${s//\//_}"
      echo "=== ETSI upstream: $s ==="
      [ -n "${PHASE_FILE:-}" ] && echo "$name" > "$PHASE_FILE"
      bash dev/reset-broker.sh "$BROKER_URL"
      (cd "$ETSI_SUITE" && "$VENV/bin/robot" \
          --outputdir "$out/$name" \
          "${EXTRA[@]}" \
          "TP/NGSI-LD/$s") 2>&1 | tee "$out/$name-console.log" \
        || { status=$?; [ "$STOP_ON_ERROR" = 1 ] && break; }
    done
    exit $status ;;
  *) echo "unknown TESTSUITE=${TESTSUITE} (ours|etsi|both)"; exit 2 ;;
esac

. dev/etsi-suites.sh
SUITES="${SUITES:-$SERIAL_ALL}"

[ -d "$SUITE" ] || { echo "test suite not found at $SUITE"; exit 1; }
check_suites_complete "$SUITE" || exit 1

# venv with the suite's requirements (robotframework + vendored HttpCtrl).
# pip runs FROM the suite dir: requirements.txt uses relative editable paths.
# The bootstrap gate VERIFIES the vendored HttpCtrl actually imports — a venv
# that predates a suite move keeps a working `robot` but a stale editable
# path (easy-install.pth), and every mock-server keyword then fails with
# "No keyword with name 'Start Server'" (two full runs
# poisoned). Heal with a forced editable reinstall, not just presence checks.
VENV="$PWD/.venv"
if [ ! -x "$VENV/bin/robot" ]; then
  python3 -m venv "$VENV"
  (cd "$SUITE" && "$VENV/bin/pip" -q install -r requirements.txt)
fi
if ! "$VENV/bin/python" -c "import HttpCtrl" >/dev/null 2>&1; then
  echo "venv: vendored HttpCtrl not importable — reinstalling editable"
  (cd "$SUITE" && "$VENV/bin/pip" -q install -r requirements.txt \
    && "$VENV/bin/pip" -q install --no-deps --force-reinstall \
         -e ./libraries/robotframework-httpctrl)
  "$VENV/bin/python" -c "import HttpCtrl" \
    || { echo "venv: HttpCtrl still broken — aborting"; exit 1; }
fi

# Point the suite at this broker (same sed recipe as Scorpio's runner).
# Restore the sed-ed file on exit: a dirty suite tree should mean a real
# change, not a leftover run configuration.
trap 'git -C "$SUITE" checkout -- resources/variables.py 2>/dev/null || true' EXIT
# The suite's @contexts from this checkout: the pipeline serves them
# and exports SUITE_CONTEXT; a caller that runs this script alone (examples,
# etsi-coverage, a dev box on the vendored suite) gets the same server here,
# so no run reads them from GitHub or forge at run time.
own_context=0
if [ -z "${SUITE_CONTEXT:-}" ] && [ "$SUITE" -ef ./ngsi-ld-test-suite ]; then
  SUITE_CONTEXT=$(RESULTS="$RESULTS_DIR" dev/etsi-local-context.sh start) \
    || { echo "could not serve the suite context locally"; exit 1; }
  own_context=1
  trap 'RESULTS="$RESULTS_DIR" dev/etsi-local-context.sh stop || true
        git -C "$SUITE" checkout -- resources/variables.py 2>/dev/null || true' EXIT
fi
# FederationReset.py reads it from the environment (its Link header)
export SUITE_CONTEXT="${SUITE_CONTEXT:-}"
( cd "$SUITE/resources"
  # A suite whose own `url` names no /ngsi-ld/v1 adds the path itself (forge's later commits):
  # it gets the broker's root, or every request went to /ngsi-ld/v1/ngsi-ld/v1.
  api_url=$BROKER_URL
  grep -q "^url = '.*/ngsi-ld/v1/\?'" variables.py || api_url=${BROKER_URL%/ngsi-ld/v1}
  sed -i "s|^url = .*|url = '$api_url'|" variables.py
  sed -i "s|^temporal_api_url = .*|temporal_api_url = '$api_url'|" variables.py
  sed -i "s|^notification_server_host = .*|notification_server_host = '$CALLBACK_HOST'|" variables.py
  sed -i "s|^context_source_host = .*|context_source_host = '$CALLBACK_HOST'|" variables.py
  sed -i "s|^context_server_host = .*|context_server_host = '$CALLBACK_HOST'|" variables.py
  # the 058 TPs reach the mosquitto the pipeline starts by its container name, as the vendored
  # suite names it; forge's later commits say 127.0.0.1, where nothing listens
  sed -i "s|^mqtt_broker_host = .*|mqtt_broker_host = 'ngsi-ld-test-suite-mosquitto-container'|" variables.py
  # the suite context the pipeline serves from this checkout, appended: the last
  # assignment wins, and forge integration/all-fixes spells the default over three lines
  [ -z "${SUITE_CONTEXT:-}" ] \
    || printf "\nngsild_test_suite_context = '%s'\n" "$SUITE_CONTEXT" >> variables.py )

mkdir -p "$RESULTS_DIR"
EXTRA=()
[ "$STOP_ON_ERROR" = 1 ] && EXTRA+=(--exitonfailure)
# MQTT TPs (058_*) launch mosquitto via `docker run` (MqttUtils.resource), so
# they run wherever docker works. MQTT=0 excludes them on
# dockerless boxes; the CI definition of green INCLUDES them.
[ "${MQTT:-1}" = 1 ] || EXTRA+=(--exclude '*mqtt*')
# TPs needing a specially-configured broker (ANTARES_TEMPORAL=none) never
# belong in a default conformance run — they get their own harness.
EXTRA+=(--exclude config_no_temporal)
# 019_31 spells the root's path into the Link headers it expects.
hostpath="${BROKER_URL#*://}"
root_path=""
[ "$hostpath" = "${hostpath#*/}" ] || root_path="/${hostpath#*/}"
EXTRA+=(--variable "entities_path:${root_path%/}/entities")

status=0
for s in $SUITES; do
  name="${s//\//_}"
  echo "=== $s ==="
  # Tell the 1 Hz sampler which suite owns the samples from here on —
  # written before the reset so the reset's own churn is attributed too.
  [ -n "${PHASE_FILE:-}" ] && echo "$name" > "$PHASE_FILE"
  bash dev/reset-broker.sh "$BROKER_URL"   # state reset between suites
  # TP/NGSI-LD/<entry>, or the files of AntaresSpecificTests/ (suite_paths)
  mapfile -t paths < <(suite_paths "$s" "$SUITE")
  [ "${#paths[@]}" -gt 0 ] || { echo "$SUITE holds no $s — skipped"; continue; }
  # several files: name the combined top suite, not "A & B & C"
  named=()
  [ "${#paths[@]}" -le 1 ] || named=(--name "$name")
  # Console tee'd next to output.xml: when robot dies before writing it,
  # the CI artifact must still say why (see the IOP step's twin note).
  (cd "$SUITE" && "$VENV/bin/robot" \
      --outputdir "$OLDPWD/$RESULTS_DIR/$name" \
      "${named[@]}" "${EXTRA[@]}" \
      "${paths[@]}") 2>&1 | tee "$RESULTS_DIR/$name-console.log" \
    || { status=$?; [ "$STOP_ON_ERROR" = 1 ] && break; }
done
if [ "$own_context" = 1 ]; then
  RESULTS="$RESULTS_DIR" dev/etsi-local-context.sh check | tee "$RESULTS_DIR/local-context-check.txt" \
    || status=1
fi
exit $status
