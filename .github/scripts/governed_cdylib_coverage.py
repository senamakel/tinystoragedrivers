"""Require an exact ignored native-cdylib integration test to run once."""

from __future__ import annotations

import re
import sys
from pathlib import Path


GOVERNED_TEST_NAME = (
    "storage_host_tests::"
    "a_loaded_cdylib_uses_the_governed_descriptor_and_never_falls_back_to_host_name"
)
TEST_NAME = GOVERNED_TEST_NAME
RESULT = re.compile(
    r"^test result: ok\. 1 passed; 0 failed; 0 ignored; "
    r"(?:\d+ measured; )?\d+ filtered out(?:; finished in [0-9.]+s)?$",
    re.MULTILINE,
)
def validate_native_test_output(
    output: str,
    exit_code: int = 0,
    test_name: str = GOVERNED_TEST_NAME,
) -> None:
    """Reject missing, ignored, failed, or non-exact native proof output."""
    if exit_code != 0:
        raise ValueError(f"governed cdylib test command exited {exit_code}")
    passed = re.compile(rf"^test {re.escape(test_name)} \.\.\. ok$", re.MULTILINE)
    if len(passed.findall(output)) != 1 or len(RESULT.findall(output)) != 1:
        raise ValueError(
            "expected exactly one passed governed cdylib test and zero ignored tests"
        )


def main(argv: list[str]) -> int:
    if len(argv) not in (3, 5) or (len(argv) == 5 and argv[3] != "--test-name"):
        print(
            "usage: governed_cdylib_coverage.py LOG EXIT_CODE [--test-name NAME]",
            file=sys.stderr,
        )
        return 2
    try:
        exit_code = int(argv[2])
        test_name = argv[4] if len(argv) == 5 else GOVERNED_TEST_NAME
        validate_native_test_output(Path(argv[1]).read_text(), exit_code, test_name)
    except (OSError, ValueError) as error:
        print(f"governed cdylib coverage proof failed: {error}", file=sys.stderr)
        return 1
    print("one native cdylib test passed; zero ignored")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
