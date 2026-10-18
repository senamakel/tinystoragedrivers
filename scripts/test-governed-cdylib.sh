#!/usr/bin/env bash
set -euo pipefail

coverage=0
case "${1:-}" in
  "") ;;
  --coverage)
    coverage=1
    shift
    ;;
  *)
    echo "usage: $0 [--coverage]" >&2
    exit 2
    ;;
esac
if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--coverage]" >&2
  exit 2
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$repo_root"
manifest="$repo_root/Cargo.toml"
target_dir="$(cargo metadata --manifest-path "$manifest" --locked --no-deps --format-version 1 \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"

build_log="$target_dir/governed-cdylib-build.jsonl"
mkdir -p "$target_dir"
if cargo build --manifest-path "$manifest" --locked \
  -p tinystoragedrivers-governed-consumer-fixture \
  --message-format=json-render-diagnostics >"$build_log" 2>&1; then
  :
else
  status=$?
  cat "$build_log" >&2
  exit "$status"
fi

case "$(uname -s)" in
  Darwin) artifact_suffix="dylib" ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) artifact_suffix="dll" ;;
  *) artifact_suffix="so" ;;
esac

if ! artifact="$(python3 .github/scripts/find_governed_cdylib.py \
  "$build_log" "$target_dir" "$artifact_suffix")"; then
  exit 1
fi
echo "governed consumer cdylib selected from Cargo artifact metadata: $artifact"

test_filter="storage_host_tests::a_loaded_cdylib_uses_the_governed_descriptor_and_never_falls_back_to_host_name"
if [[ "$coverage" -eq 1 ]]; then
  test_log="$target_dir/governed-cdylib-coverage.log"
  if TSD_GOVERNED_TEST_MODULE="$artifact" cargo llvm-cov test \
    --no-report --manifest-path "$manifest" --locked --all-features \
    -p tinystoragedrivers-tinybus --lib "$test_filter" -- \
    --ignored --exact --nocapture >"$test_log" 2>&1; then
    cat "$test_log"
  else
    status=$?
    cat "$test_log" >&2
    exit "$status"
  fi
  python3 .github/scripts/governed_cdylib_coverage.py "$test_log" 0
else
  TSD_GOVERNED_TEST_MODULE="$artifact" cargo test \
    --manifest-path "$manifest" --locked -p tinystoragedrivers-tinybus --lib \
    "$test_filter" -- --ignored --nocapture
fi
