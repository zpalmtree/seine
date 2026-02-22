#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/release_tag.sh <version-or-tag>

Examples:
  scripts/release_tag.sh 0.1.10
  scripts/release_tag.sh v0.1.10

What it does:
  1) Updates [package].version in Cargo.toml
  2) Updates the root package version entry in Cargo.lock
  3) Creates a commit: "release: vX.Y.Z"
  4) Creates an annotated tag: vX.Y.Z
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

input="$1"
if [[ "$input" == v* || "$input" == V* ]]; then
  version="${input:1}"
else
  version="$input"
fi
tag="v${version}"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "error: version must look like X.Y.Z (optionally with suffix), got: $version" >&2
  exit 1
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "error: must run inside a git repository" >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "error: tag already exists: ${tag}" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is not clean; commit or stash changes first" >&2
  exit 1
fi

tmp_toml="$(mktemp)"
awk -v ver="$version" '
BEGIN { in_pkg = 0; replaced = 0 }
{
  if ($0 ~ /^\[package\]/) {
    in_pkg = 1
  } else if ($0 ~ /^\[/ && in_pkg) {
    in_pkg = 0
  }

  if (in_pkg && !replaced && $0 ~ /^version[[:space:]]*=/) {
    print "version = \"" ver "\""
    replaced = 1
    next
  }

  print
}
END {
  if (!replaced) {
    exit 2
  }
}
' Cargo.toml >"$tmp_toml"
mv "$tmp_toml" Cargo.toml

if [[ -f Cargo.lock ]]; then
  tmp_lock="$(mktemp)"
  awk -v ver="$version" '
BEGIN { in_pkg = 0; is_root = 0; replaced = 0 }
{
  if ($0 ~ /^\[\[package\]\]/) {
    in_pkg = 1
    is_root = 0
  }

  if (in_pkg && $0 == "name = \"seine\"") {
    is_root = 1
  }

  if (in_pkg && is_root && !replaced && $0 ~ /^version = "/) {
    print "version = \"" ver "\""
    replaced = 1
    next
  }

  print
}
END {
  if (!replaced) {
    exit 3
  }
}
' Cargo.lock >"$tmp_lock"
  mv "$tmp_lock" Cargo.lock
fi

diff_paths=(Cargo.toml)
if [[ -f Cargo.lock ]]; then
  diff_paths+=(Cargo.lock)
fi

if ! git diff --quiet -- "${diff_paths[@]}"; then
  git add "${diff_paths[@]}"
  git commit -m "release: ${tag}"
else
  echo "version already set to ${version}; no version file changes to commit"
fi

git tag -a "$tag" -m "$tag"
echo "created tag ${tag}"
echo "next: git push && git push origin ${tag}"
