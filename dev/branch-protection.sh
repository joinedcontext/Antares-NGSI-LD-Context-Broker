#!/usr/bin/env bash
# Applies the branch model of CONTRIBUTING.md to the GitHub repository,
# and the description, homepage and topics its front page shows:
#
#   dev    — default branch. Maintainers push, everyone else opens a pull
#            request that lands by rebase or squash with ci.yml green.
#            No force push, no deletion, linear history.
#   main   — the latest release. Only the release run of full.yml moves
#            it (a merge commit of the released commit); nobody else
#            updates it, no force push, no deletion.
#   v*     — release tags. Once pushed, never moved and never deleted.
#
# Needs a token with Administration: write on the repository, in GH_TOKEN
# or ~/.config/antares/gh-token. Rulesets bind on a public repository (or
# a paid plan), so run it once the repository is public. Idempotent: an
# existing dev branch is kept and a ruleset of the same name is replaced.
#
# Review count is 0 on purpose: a sole maintainer cannot approve their own
# pull request, so requiring one would deadlock every merge into dev. Raise
# it when a second maintainer joins.
set -euo pipefail

REPO=${REPO:-joinedcontext/Antares-NGSI-LD-Context-Broker}
TOKEN=${GH_TOKEN:-$(cat ~/.config/antares/gh-token)}
API="https://api.github.com/repos/$REPO"

gh_api() { # method path [json]
  curl -fsS -X "$1" "$API$2" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/vnd.github+json" \
    ${3:+-d "$3"}
}

# 0. what the repository page and GitHub search show
gh_api PATCH "" '{
  "description": "NGSI-LD Context Broker in Rust. ETSI CIM 009 V1.9.1 conformant, one small native binary, PostgreSQL/TimescaleDB storage, NATS JetStream scale-out, and a WebAssembly build that runs the same broker in the browser. Contributions go to the dev branch.",
  "homepage": "https://antaresbroker.joinedcontext.com/" }' >/dev/null
gh_api PUT /topics '{ "names": [
  "ngsi-ld", "context-broker", "etsi", "etsi-cim", "fiware", "rust",
  "smart-cities", "digital-twin", "iot", "linked-data", "json-ld",
  "webassembly", "wasm", "postgresql", "timescaledb", "nats", "mqtt",
  "temporal-data", "federation", "multi-tenant" ] }' >/dev/null
echo "description, homepage, topics set"

# 1. dev starts where main is
if gh_api GET /git/ref/heads/dev >/dev/null 2>&1; then
  echo "dev exists, kept"
else
  sha=$(gh_api GET /git/ref/heads/main | python3 -c 'import json,sys; print(json.load(sys.stdin)["object"]["sha"])')
  gh_api POST /git/refs "{\"ref\":\"refs/heads/dev\",\"sha\":\"$sha\"}" >/dev/null
  echo "created dev at $sha"
fi

# 2. dev is the default branch: new pull requests target it and the
#    scheduled workflows run on it.
gh_api PATCH "" '{"default_branch":"dev"}' >/dev/null
echo "default branch: dev"

# 3. rulesets
ruleset() { # name ref bypass-actors-json rules-json
  local old target=branch
  case $2 in refs/tags/*) target=tag ;; esac
  old=$(gh_api GET /rulesets | python3 -c '
import json, sys
print(next((r["id"] for r in json.load(sys.stdin) if r["name"] == sys.argv[1]), ""))' "$1")
  [ -z "$old" ] || gh_api DELETE "/rulesets/$old"
  gh_api POST /rulesets "{
    \"name\": \"$1\", \"target\": \"$target\", \"enforcement\": \"active\",
    \"bypass_actors\": $3,
    \"conditions\": { \"ref_name\": { \"include\": [\"$2\"], \"exclude\": [] } },
    \"rules\": $4 }" >/dev/null
  echo "ruleset $1 active"
}

pull_request() { # merge-methods-json
  echo "{ \"type\": \"pull_request\", \"parameters\": {
    \"allowed_merge_methods\": $1,
    \"required_approving_review_count\": 0,
    \"dismiss_stale_reviews_on_push\": false,
    \"require_code_owner_review\": false,
    \"require_last_push_approval\": false,
    \"required_review_thread_resolution\": false } }"
}

# Check contexts are "<caller job name> / <called job name>"; if GitHub
# reports one as expected-but-never-reported, copy the exact name from a
# pull request's checks tab. Not strict: dev is linear, a pull request is
# rebased onto it when it lands.
checks() { # context...
  local list="" c
  for c in "$@"; do list+="{\"context\":\"$c\"},"; done
  echo "{ \"type\": \"required_status_checks\", \"parameters\": {
    \"strict_required_status_checks_policy\": false,
    \"required_status_checks\": [ ${list%,} ] } }"
}

hard='{ "type": "deletion" }, { "type": "non_fast_forward" }'
# RepositoryRole 5 is admin: the maintainers, who push to dev.
admins='[ { "actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always" } ]'
# Integration 15368 is the GitHub Actions app: full.yml's release run.
actions='[ { "actor_id": 15368, "actor_type": "Integration", "bypass_mode": "always" } ]'

# Nobody bypasses the tag and dev rules.
ruleset protect-tags 'refs/tags/v*' '[]' "[ $hard, { \"type\": \"update\" } ]"
ruleset protect-dev refs/heads/dev '[]' "[ $hard, { \"type\": \"required_linear_history\" } ]"
# The release run moves main after the whole grid is green on the
# released commit; no pull request or person updates it.
ruleset protect-main refs/heads/main "$actions" "[ $hard, { \"type\": \"update\" } ]"

# Maintainers bypass this one, which is what lets them push commits to dev.
ruleset dev-pull-requests refs/heads/dev "$admins" "[
  $(pull_request '["rebase", "squash"]'),
  $(checks "Workspace tests / Workspace tests" \
           "ETSI cell matrix / Matrix summary") ]"

# 4. Actions and outside contributors. A workflow gets a read-only token
#    unless it asks for more, no workflow approves a pull request, and a
#    run on a pull request from anyone without write access waits for a
#    maintainer to press "Approve and run": read the diff of .github/ and
#    dev/ first, because that run executes the contributor's code.
gh_api PUT /actions/permissions/workflow \
  '{"default_workflow_permissions":"read","can_approve_pull_request_reviews":false}'
echo "workflow token: read-only by default"
gh_api PUT /actions/permissions/fork-pr-contributor-approval \
  '{"approval_policy":"all_external_contributors"}' \
  && echo "fork pull requests: every outside contributor needs approval" \
  || echo "SET BY HAND: Actions → General → Require approval for all external contributors"

cat <<'EOF'

Done. By hand, in the repository settings:
 - General → Pull Requests: allow merge commits, squash and rebase (the
   rulesets narrow them per branch).
Every clone: git fetch && git switch dev
EOF
