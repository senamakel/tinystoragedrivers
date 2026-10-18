#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$repo_root"

cargo build -p tinystoragedrivers-governed-consumer-fixture

case "$(uname -s)" in
    Darwin) artifact_suffix="dylib" ;;
    MINGW*|MSYS*|CYGWIN*|Windows_NT) artifact_suffix="dll" ;;
    *) artifact_suffix="so" ;;
esac

artifact=""
while IFS= read -r candidate; do
    artifact="$candidate"
    break
done < <(find target/debug -type f -name "*tinystoragedrivers_governed_consumer_fixture*.$artifact_suffix" -print)

if [[ -z "$artifact" ]]; then
    echo "governed consumer cdylib was not found under target/debug" >&2
    exit 1
fi

TSD_GOVERNED_TEST_MODULE="$repo_root/$artifact" \
    cargo test -p tinystoragedrivers-tinybus --lib \
    a_loaded_cdylib_uses_the_governed_descriptor_and_never_falls_back_to_host_name \
    -- --ignored --nocapture
