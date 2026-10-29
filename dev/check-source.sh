#!/usr/bin/env bash
# The release rules of CONTRIBUTING.md, held mechanically by full.yml before
# a runner is spent on the grid. A release is a dispatch of full.yml with
# RELEASE set (0.MINOR.PATCH):
#
#   - it runs on dev,
#   - the workspace version in Cargo.toml is RELEASE,
#   - the tag vRELEASE does not exist yet (a pushed v* tag is final).
#
# Every other run passes. Reads the GitHub Actions environment plus RELEASE;
# needs the tags fetched. `dev/check-source.sh --self-test` proves the rules
# in a scratch repository.
set -euo pipefail

check() {
  [ -n "${RELEASE:-}" ] || return 0
  if [ "$GITHUB_REF" != refs/heads/dev ]; then
    echo "::error::a release runs on dev, not $GITHUB_REF"
    return 1
  fi
  local version
  version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
  if [ "$version" != "$RELEASE" ]; then
    echo "::error::release $RELEASE, workspace version $version; bump Cargo.toml on dev first"
    return 1
  fi
  if git rev-parse -q --verify "refs/tags/v$RELEASE" >/dev/null; then
    echo "::error::v$RELEASE is already tagged; a release tag is final, release the next PATCH"
    return 1
  fi
}

self_test() {
  local d; d=$(mktemp -d); trap "rm -rf '$d'" EXIT
  cd "$d"
  git init -q -b dev . && git config user.email t@t && git config user.name t
  printf '[workspace.package]\nversion = "1.2.3"\n' > Cargo.toml
  git add . && git commit -qm one
  git tag v1.2.2
  expect() { # pass|fail release ref
    local want=$1 got=pass
    RELEASE=$2 GITHUB_REF=$3 check >/dev/null || got=fail
    [ "$got" = "$want" ] || { echo "self-test: $* gave $got"; exit 1; }
  }
  expect pass ""    refs/heads/dev    # schedule or plain dispatch
  expect pass ""    refs/heads/topic
  expect pass 1.2.3 refs/heads/dev
  expect fail 1.2.3 refs/heads/topic  # a release off dev
  expect fail 9.9.9 refs/heads/dev    # Cargo.toml not bumped
  printf '[workspace.package]\nversion = "1.2.2"\n' > Cargo.toml
  expect fail 1.2.2 refs/heads/dev    # already tagged
  echo "self-test: ok"
}

if [ "${1:-}" = --self-test ]; then self_test; else check; fi
