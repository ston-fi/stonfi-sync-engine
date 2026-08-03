#!/usr/bin/env bash
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

current_tag=""
if [[ "${GITHUB_REF_TYPE:-}" == "tag" ]]; then
  current_tag=${GITHUB_REF_NAME:-}
fi

baseline_tag=""
while IFS= read -r tag; do
  if [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ && "$tag" != "$current_tag" ]]; then
    baseline_tag=$tag
    break
  fi
done < <(git tag --merged HEAD --sort=-version:refname)

if [[ -z "$baseline_tag" ]]; then
  echo "No previous stable release tag; skipping SemVer comparison"
  exit 0
fi

for package in stonfi_sync_core stonfi_distributed_sync; do
  manifest="crates/${package#stonfi_}/Cargo.toml"
  if ! git cat-file -e "$baseline_tag:$manifest" 2>/dev/null; then
    echo "$package is absent from $baseline_tag; skipping its first-release comparison"
    continue
  fi

  cargo semver-checks check-release \
    --package "$package" \
    --baseline-rev "$baseline_tag" \
    --all-features
done
