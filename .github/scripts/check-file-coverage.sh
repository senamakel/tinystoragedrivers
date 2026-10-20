#!/usr/bin/env bash
set -euo pipefail

minimum="${1:-90}"
report="${2:-coverage.json}"
workspace_root="$(pwd -P)/"
# Every crate lives under `crates/<package>/src/`, so one prefix covers the
# whole workspace. `worktrees/` sits outside it and is
# excluded by the same test.
source_root="${workspace_root}crates/"

# Keep the exact fixture exclusion and native-proof result check under tests.
# The actual integration proof runs below under cargo-llvm-cov, and a missing
# fixture, failed test, or ignored result fails this gate before it can exclude
# the fixture source from the production denominator.
python3 .github/scripts/test_file_coverage_policy.py
python3 .github/scripts/test_governed_cdylib_coverage.py
python3 .github/scripts/test_migration_cdylib_llvm_cov.py
python3 .github/scripts/test_migration_cdylib_marker_handoff.py

cargo llvm-cov \
  --locked \
  --workspace \
  --all-targets \
  --all-features \
  --json \
  --output-path "$report"

# These tests load the real compiled consumer and migration cdylibs through
# TinyBus hosts. Coverage builds instrument the cdylibs as well as host tests.
# cargo llvm-cov's --no-report run preserves and appends the actual
# module-call execution to workspace profiles before the final per-file report.
bash scripts/test-governed-cdylib.sh --coverage

# Include the real native publication failure and concurrency executions in
# this full-workspace report, just as the dedicated native secrets lane does.
python3 .github/scripts/native_profile_tests.py
python3 .github/scripts/check-native-io-fault.py
python3 .github/scripts/check-native-key-race.py
python3 .github/scripts/check-native-secret-service.py --coverage
module_cdylib="$(python3 .github/scripts/migration_cdylib_path.py "$(pwd -P)")"
# The adapter executes inside a loaded cdylib, so its counters are in a separate
# LLVM object. Keep cargo llvm-cov's complete workspace object list and append
# this instrumented module when exporting the final unmodified coverage report.
llvm_coverage_tools="$(python3 .github/scripts/migration_cdylib_llvm_cov.py "$(pwd -P)" "$module_cdylib")"
export LLVM_COV="${llvm_coverage_tools%%$'\n'*}"
export LLVM_PROFDATA="${llvm_coverage_tools#*$'\n'}"
cargo llvm-cov report --json --output-path "$report"
unset LLVM_COV LLVM_PROFDATA
python3 .github/scripts/file_coverage_policy.py "$minimum" "$report" "$(pwd -P)"
