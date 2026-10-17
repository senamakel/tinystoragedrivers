#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="${STORAGE_NEEDS_CONSUMER_MANIFEST:-$repo_root/proof-workspaces/storage-needs/Cargo.toml}"

cargo run --locked --target-dir "$repo_root/target/storage-needs-proof" --manifest-path "$manifest"

dependency_tree="$(cargo tree --locked --manifest-path "$manifest" --prefix none -e normal,build)"
if [[ "$dependency_tree" != *"tinystoragedrivers-bus v"* ]]; then
    printf 'storage-needs consumer dependency graph omitted its contract crate:\n%s\n' \
        "$dependency_tree" >&2
    exit 1
fi

for forbidden_crate in tinybus tokio async-std async-trait reqwest hyper ureq mongodb bson rusqlite libsqlite3-sys \
    tinystoragedrivers-core tinystoragedrivers-client tinystoragedrivers-busproxy \
    tinystoragedrivers-sqlite tinystoragedrivers-mongodb tinystoragedrivers-file \
    tinystoragedrivers-s3 tinystoragedrivers-secrets; do
    if [[ "$dependency_tree" == *"$forbidden_crate v"* ]]; then
        printf 'storage-needs consumer linked forbidden dependency %s:\n%s\n' \
            "$forbidden_crate" "$dependency_tree" >&2
        exit 1
    fi
done

printf '%s\n' "$dependency_tree"
printf 'storage-needs consumer dependency graph is data-only\n'
