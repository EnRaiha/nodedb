#!/usr/bin/env bash
# Stamp the workspace version from a release tag.
#
#   scripts/ci/stamp_version.sh <version>       # e.g. 0.1.0, 0.1.0-beta.2
#
# Rewrites `[workspace.package] version` in the root Cargo.toml, and for a
# prerelease also pins every internal `nodedb*` path-dep requirement to the
# exact version — a bare `version = "0.1.0"` requirement does not match
# `0.1.0-beta.2` under semver, so publishing would fail without this.
#
# The dep pin is a generic match on `nodedb*` rather than an explicit crate
# list, so adding a workspace crate can never silently break a release.
#
# No-ops when Cargo.toml already carries the target version, which keeps
# re-running a stage idempotent.

set -euo pipefail

VERSION="${1:?usage: stamp_version.sh <version>}"

CURRENT=$(cargo metadata --no-deps --format-version=1 \
    | jq -r '.packages[] | select(.name == "nodedb-types") | .version')

if [[ "$VERSION" == "$CURRENT" ]]; then
    echo "Version already $VERSION — nothing to stamp."
    exit 0
fi

# First `version = "..."` in the file is [workspace.package].
perl -i -pe 'if (!$done && /^version = "/) { s/^version = ".*"/version = "'"$VERSION"'"/; $done=1 }' Cargo.toml

if [[ "$VERSION" == *-* ]]; then
    sed -i -E 's/(nodedb[a-z0-9-]* = \{ [^}]*version = )"[^"]*"/\1"='"$VERSION"'"/' Cargo.toml
    echo "Pinned internal nodedb* dep requirements to =$VERSION"
fi

echo "Stamped workspace version: $CURRENT -> $VERSION"
