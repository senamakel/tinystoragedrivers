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

cargo llvm-cov \
  --locked \
  --workspace \
  --all-targets \
  --all-features \
  --json \
  --output-path "$report"

# This test loads the real compiled consumer cdylib from a linked TinyBus host.
# cargo llvm-cov's --no-report run preserves and appends the actual
# module-call execution to workspace profiles before the final per-file report.
bash scripts/test-governed-cdylib.sh --coverage

# Include the real native publication failure and concurrency executions in
# this full-workspace report, just as the dedicated native secrets lane does.
python3 .github/scripts/native_profile_tests.py
python3 .github/scripts/check-native-io-fault.py
python3 .github/scripts/check-native-key-race.py
python3 .github/scripts/check-native-secret-service.py --coverage
cargo llvm-cov report --json --output-path "$report"
python3 .github/scripts/file_coverage_policy.py "$minimum" "$report" "$(pwd -P)"
