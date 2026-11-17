#!/usr/bin/env bash
# The LOCAL gate: workspace tests + the SAME store jobs CI runs
# (.github/workflows/etsi-matrix.yml etsi-cell job) — one pipeline run per
# store over ALL suites, then the per-store table via
# dev/etsi-matrix-summary.py.
#
# ONE store mode per local run — the one you are touching. CI fans the three
# store jobs (file, postgres, timescale) out in PARALLEL on every push and is
# the authority. Locally you want the fast loop, not the matrix.
#
#   STORE=memory (default)     the mode under test; also file|postgres|timescale
#   STORE=all                  the CI trio file postgres timescale, serially
#   STOP_ON_ERROR=1 (default)  halt at the FIRST failing TP
#   STOP_ON_ERROR=0            run every suite, gate at the summary
#   TESTSUITE=ours (default)   our copy only, as CI's etsi-cell job
#   TESTSUITE=etsi|both        also (or only) the official suite, fetched
#                              fresh from forge.etsi.org and gated by its
#                              baseline (conformance/etsi-baseline/<store>.txt),
#                              as CI's etsi-upstream-cell job
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== workspace tests ==="
# -j 2: default parallelism OOM-kills the linker on dev boxes
cargo test --workspace -j 2

case "${STORE:-memory}" in
  all) STORES=(file postgres timescale) ;;
  *)   STORES=("${STORE:-memory}") ;;
esac
CELLS=results/cells
rm -rf "$CELLS"

echo "=== build image under test ==="
docker build -t antares-local:latest .

for store in "${STORES[@]}"; do
  echo "=== ETSI $store (all suites) ==="
  STORE=$store SKIP_BUILD=1 \
  RESULTS_DIR="$CELLS/ETSI-cell-$store" \
  ETSI_RESULTS_DIR="$CELLS/ETSI-upstream-cell-$store" \
  STOP_ON_ERROR="${STOP_ON_ERROR:-1}" \
    dev/etsi-pipeline.sh
done

echo "=== matrix summary ==="
STORES="${STORES[*]}" TESTSUITE="${TESTSUITE:-ours}" dev/etsi-matrix-summary.py "$CELLS"
echo "LOCAL GATE GREEN (${STORES[*]}) — CI gates file postgres timescale in parallel"
