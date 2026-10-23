#!/usr/bin/env bash
# Validate that a release's tag, source SHA, checked-out SHA, Cargo.toml,
# Cargo.lock and Release Please manifest all agree. Exits non-zero on any
# mismatch.
set -euo pipefail

: "${VERSION:?VERSION is required}"
: "${SOURCE_SHA:?SOURCE_SHA is required}"
: "${CHECKED_SHA:?CHECKED_SHA is required}"
: "${TAG_NAME:?TAG_NAME is required}"

TAG_NAME="${TAG_NAME#v}"
if [ "$TAG_NAME" != "$VERSION" ]; then
  echo "::error::TAG_NAME ($TAG_NAME) != VERSION ($VERSION)" >&2
  exit 1
fi

if ! [[ "$SOURCE_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "::error::SOURCE_SHA is not a full 40-char SHA: $SOURCE_SHA" >&2
  exit 1
fi
if ! [[ "$CHECKED_SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "::error::CHECKED_SHA is not a full 40-char SHA: $CHECKED_SHA" >&2
  exit 1
fi
if [ "$CHECKED_SHA" != "$SOURCE_SHA" ]; then
  echo "::error::checked-out SHA ($CHECKED_SHA) != source SHA ($SOURCE_SHA)" >&2
  exit 1
fi

cargo_version=$(python3 -c 'import re,sys; print(re.search(r"^version = \"([^\"]+)\"", open("Cargo.toml").read(), re.M).group(1))')
if [ "$cargo_version" != "$VERSION" ]; then
  echo "::error::Cargo.toml version ($cargo_version) != $VERSION" >&2
  exit 1
fi

manifest_version=$(python3 -c 'import json; print(json.load(open(".release-please-manifest.json"))["."])')
if [ "$manifest_version" != "$VERSION" ]; then
  echo "::error::manifest version ($manifest_version) != $VERSION" >&2
  exit 1
fi

lock_version=$(python3 -c 'import re; print(re.search(r"name = \"kilo-provider\"\nversion = \"([^\"]+)\"","".join(open("Cargo.lock")),re.M).group(1))' 2>/dev/null || true)
if [ -n "$lock_version" ] && [ "$lock_version" != "$VERSION" ]; then
  echo "::error::Cargo.lock version ($lock_version) != $VERSION" >&2
  exit 1
fi

# GHCR tag uniqueness / fully qualified check helper.
python3 - "$VERSION" "$CHECKED_SHA" <<'PY'
import re, sys
version, sha = sys.argv[1], sys.argv[2]
image = "ghcr.io/ecoma-io/kilo-provider"
tags = [f"{image}:{version}"]
major, minor, patch = version.split(".")
if int(major) != 0:
    tags += [f"{image}:{major}.{minor}", f"{image}:{major}"]
tags += [f"{image}:latest", f"{image}:sha-{sha[:7]}"]
assert all(t.startswith("ghcr.io/") for t in tags), tags
assert len(tags) == len(set(tags)), ("duplicate tags", tags)
for t in tags:
    tagpart = t.rsplit(":", 1)[-1]
    assert tagpart and re.fullmatch(r"[A-Za-z0-9._-]+", tagpart), tagpart
print("metadata+tag validation ok:", len(tags), "tags")
PY

echo "release metadata consistent: version=$VERSION sha=$SOURCE_SHA tag=v$VERSION"