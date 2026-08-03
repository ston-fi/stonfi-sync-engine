#!/usr/bin/env bash
set -euo pipefail

repository_root=$(git rev-parse --show-toplevel)
repository_root=$(cd "$repository_root" && pwd -P)
snapshot_dir=$(mktemp -d)
consumer_dir=""
cleanup() {
  rm -rf -- "$snapshot_dir"
  if [[ -n "$consumer_dir" ]]; then
    rm -rf -- "$consumer_dir"
  fi
}
trap cleanup EXIT

rsync -a \
  --exclude .git \
  --exclude .worktrees \
  --exclude target \
  "$repository_root/" \
  "$snapshot_dir/"
git -C "$snapshot_dir" init -q
git -C "$snapshot_dir" config user.name "External Consumer Check"
git -C "$snapshot_dir" config user.email "external-consumer-check@example.invalid"
git -C "$snapshot_dir" add -A
git -C "$snapshot_dir" commit -qm "external consumer validation snapshot"

repository_root=$(cd "$snapshot_dir" && pwd -P)
revision=$(git -C "$repository_root" rev-parse HEAD)
consumer_dir=$(mktemp -d)

mkdir -p "$consumer_dir/src"
sed \
  -e "s|__REPOSITORY_URL__|file://$repository_root|g" \
  -e "s|__REVISION__|$revision|g" \
  "$repository_root/.github/external-consumer/Cargo.toml.in" \
  > "$consumer_dir/Cargo.toml"
cp "$repository_root/.github/external-consumer/main.rs" "$consumer_dir/src/main.rs"

cargo +1.95.0 check --manifest-path "$consumer_dir/Cargo.toml"
