# Sourced by dev/etsi-pipeline.sh and dev/etsi-run.sh — the one definition of
# which serial suites exist. No `set` line: it would leak into the caller.
SERIAL_ALL="CommonBehaviours ContextInformation/Consumption ContextInformation/EntityMap ContextInformation/Provision ContextInformation/Snapshot ContextInformation/Subscription ContextSource jsonldContext DistributedOperations AntaresSpecificTests"

# AntaresSpecificTests/ is not ETSI-derived: behaviour Antares defines where
# CIM 009 is silent. It runs in every cell as its own suite entry, reported
# on its own row, apart from TP/. A file that needs a specially built broker
# is named here with the workflow that runs it instead; every other .robot
# file in the folder runs, so a new one cannot be skipped by omission.
ANTARES_SPECIFIC_ELSEWHERE="policy_engine.robot:.github/workflows/examples.yml"

# The robot paths one SERIAL_ALL entry runs, relative to the suite root.
suite_paths() { # $1 = SERIAL_ALL entry, $2 = suite root
  local f skip e
  case "$1" in
    AntaresSpecificTests)
      for f in "$2"/AntaresSpecificTests/*.robot; do
        [ -e "$f" ] || continue   # a tree without the folder (another fork)
        skip=""
        for e in $ANTARES_SPECIFIC_ELSEWHERE; do
          [ "$(basename "$f")" = "${e%%:*}" ] && skip=1
        done
        [ -n "$skip" ] || echo "AntaresSpecificTests/$(basename "$f")"
      done ;;
    *) echo "TP/NGSI-LD/$1" ;;
  esac
}

# Completeness guard: a suite dir the runner doesn't know is a hard error, so
# a fork update can never add tests that silently don't run. Three roots:
# TP/NGSI-LD/* (serial suites), repo-root *_TP trees (IOP_TP's pattern) and
# AntaresSpecificTests/ (every file runs unless named as running elsewhere).
check_suites_complete() { # $1 = suite repo root
  local root="$1/TP/NGSI-LD" d s e
  for d in "$root"/*/; do
    [ -d "$d" ] || continue
    d="$(basename "$d")"
    case " $SERIAL_ALL " in
      *" $d "*) ;;
      *" $d/"*) # listed only via subpaths — every immediate subdir must appear
        for s in "$root/$d"/*/; do
          [ -d "$s" ] || continue
          s="$(basename "$s")"
          case " $SERIAL_ALL " in *" $d/$s "*) ;; *)
            echo "TP/NGSI-LD/$d/$s is not in SERIAL_ALL (dev/etsi-suites.sh) — its tests would never run"
            return 1 ;;
          esac
        done ;;
      *)
        echo "TP/NGSI-LD/$d is not in SERIAL_ALL (dev/etsi-suites.sh) — its tests would never run"
        return 1 ;;
    esac
  done
  for d in "$1"/*_TP/; do
    [ -d "$d" ] || continue
    d="$(basename "$d")"
    case "$d" in IOP_TP) ;; *)
      echo "$d has no pipeline step (only IOP_TP is wired) — extend dev/etsi-pipeline.sh"
      return 1 ;;
    esac
  done
  # Antares-specific: the folder is flat (suite_paths takes *.robot at its
  # top), and each file kept out names a workflow that really runs it
  d="$1/AntaresSpecificTests"
  if [ -d "$d" ]; then
    case " $SERIAL_ALL " in *" AntaresSpecificTests "*) ;; *)
      echo "AntaresSpecificTests is not in SERIAL_ALL (dev/etsi-suites.sh) — its tests would never run"
      return 1 ;;
    esac
    s=$(find "$d" -mindepth 2 -name '*.robot' | head -1)
    [ -z "$s" ] || { echo "$s: AntaresSpecificTests/ is run flat — a .robot file in a subdir would never run"; return 1; }
    for e in $ANTARES_SPECIFIC_ELSEWHERE; do
      [ -f "$d/${e%%:*}" ] || { echo "ANTARES_SPECIFIC_ELSEWHERE names ${e%%:*}, which $d does not hold"; return 1; }
      grep -qF "AntaresSpecificTests/${e%%:*}" "${e#*:}" 2>/dev/null \
        || { echo "AntaresSpecificTests/${e%%:*} is kept out of the cells for ${e#*:}, which does not run it"; return 1; }
    done
  fi
  # any other folder of .robot files at the suite root would never run
  s=$(cd "$1" && find . -name '*.robot' -not -path './TP/*' -not -path './*_TP/*' \
        -not -path './AntaresSpecificTests/*' -not -path './libraries/*' | head -1)
  [ -z "$s" ] || { echo "$1/${s#./}: no runner step runs this folder — wire it in dev/etsi-suites.sh"; return 1; }
}
