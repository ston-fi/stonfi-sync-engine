#!/usr/bin/env bash
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
cd "$repository_root"

release_tag=${1:-}
expected_ref=${2:-}
if [[ -z "$release_tag" || -z "$expected_ref" ]]; then
  echo "usage: $0 <vMAJOR.MINOR.PATCH> <expected-git-ref>" >&2
  exit 1
fi

bash .github/check-release-tag.sh "$release_tag"
expected_commit=$(git rev-parse --verify "$expected_ref^{commit}")
remote_tag_ref="refs/tags/$release_tag"
inspection_ref="refs/release-inspection/$release_tag"

release_exists=false
if gh release view "$release_tag" >/dev/null 2>&1; then
  release_exists=true
fi

remote_tag_commit=""
refresh_remote_tag() {
  if git ls-remote --exit-code --tags --refs origin "$remote_tag_ref" >/dev/null 2>&1; then
    git fetch --quiet --force --no-tags origin "$remote_tag_ref:$inspection_ref"
    remote_tag_commit=$(git rev-parse --verify "$inspection_ref^{commit}")
  else
    remote_tag_commit=""
  fi
}

create_github_release() {
  if gh release create "$release_tag" \
    --generate-notes \
    --title "$release_tag" \
    --verify-tag; then
    return
  fi

  if gh release view "$release_tag" >/dev/null 2>&1; then
    echo "GitHub Release $release_tag was created concurrently"
    return
  fi

  echo "failed to create GitHub Release $release_tag" >&2
  return 1
}

refresh_remote_tag

if [[ -n "$remote_tag_commit" ]]; then
  if [[ "$release_exists" == true ]]; then
    if [[ "$remote_tag_commit" == "$expected_commit" ]] ||
      git merge-base --is-ancestor "$remote_tag_commit" "$expected_commit"; then
      echo "GitHub Release $release_tag already exists at $remote_tag_commit"
      exit 0
    fi

    echo "release tag $release_tag points outside the tested history" >&2
    exit 1
  fi

  if [[ "$remote_tag_commit" != "$expected_commit" ]]; then
    echo "release tag $release_tag already points to $remote_tag_commit, expected $expected_commit" >&2
    exit 1
  fi

  create_github_release
  exit 0
fi

if [[ "$release_exists" == true ]]; then
  echo "GitHub Release $release_tag exists without a resolvable remote tag" >&2
  exit 1
fi

if git show-ref --verify --quiet "$remote_tag_ref"; then
  echo "local tag $release_tag exists while the remote tag is absent" >&2
  exit 1
fi

git -c user.name=github-actions -c user.email=github-actions@github.com \
  tag -a "$release_tag" "$expected_commit" -m "Release $release_tag"

if ! git push origin "$remote_tag_ref"; then
  refresh_remote_tag
  if [[ "$remote_tag_commit" != "$expected_commit" ]]; then
    echo "failed to create release tag $release_tag at $expected_commit" >&2
    exit 1
  fi
fi

create_github_release
