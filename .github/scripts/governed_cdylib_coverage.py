"""Require the exact ignored governed-cdylib integration test to run once."""

from __future__ import annotations

import re
import sys
from pathlib import Path


TEST_NAME = (
    "storage_host_tests::"
    "a_loaded_cdylib_uses_the_governed_descriptor_and_never_falls_back_to_host_name"
)
RESULT = re.compile(
    r"^test result: ok\. 1 passed; 0 failed; 0 ignored; "
    r"(?:\d+ measured; )?\d+ filtered out(?:; finished in [0-9.]+s)?$",
    re.MULTILINE,
)
PASSED = re.compile(rf"^test {re.escape(TEST_NAME)} \.\.\. ok$", re.MULTILINE)


def validate_native_test_output(output: str, exit_code: int = 0) -> None:
    """Reject missing, ignored, failed, or non-exact native proof output."""
    if exit_code != 0:
        raise ValueError(f"governed cdylib test command exited {exit_code}")
    if len(PASSED.findall(output)) != 1 or len(RESULT.findall(output)) != 1:
        raise ValueError(
            "expected exactly one passed governed cdylib test and zero ignored tests"
        )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: governed_cdylib_coverage.py LOG EXIT_CODE", file=sys.stderr)
        return 2
    try:
        exit_code = int(argv[2])
        validate_native_test_output(Path(argv[1]).read_text(), exit_code)
    except (OSError, ValueError) as error:
        print(f"governed cdylib coverage proof failed: {error}", file=sys.stderr)
        return 1
    print("one governed cdylib test passed; zero ignored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
