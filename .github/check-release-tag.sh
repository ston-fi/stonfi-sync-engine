#!/usr/bin/env bash
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

release_tag=${1:-}
if [[ ! "$release_tag" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
  echo "release tag must match vMAJOR.MINOR.PATCH, got: $release_tag" >&2
  exit 1
fi

release_version=${BASH_REMATCH[1]}
metadata=$(cargo metadata --locked --no-deps --format-version 1)

for package in stonfi_sync_core stonfi_distributed_sync; do
  package_version=$(jq -er \
    --arg package "$package" \
    '.packages[] | select(.name == $package) | .version' \
    <<<"$metadata")

  if [[ "$package_version" != "$release_version" ]]; then
    echo "$package version $package_version does not match tag $release_tag" >&2
    exit 1
  fi
done

echo "release tag $release_tag matches all workspace package versions"
