"""Per-file production coverage policy for the TSD workspace."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any


PRODUCTION_ROOT = "crates/"
TEST_CDYLIB_PREFIX = "crates/tinystoragedrivers-tinybus/test-cdylib/src/"


def production_files(report: dict[str, Any], workspace_root: Path) -> list[dict[str, Any]]:
    """Return executable crate files subject to the production coverage floor.

    The governed consumer cdylib is a non-published integration fixture. The
    coverage gate separately builds and loads it in a real TinyBus host before
    calling this function, so only that exact fixture source subtree is omitted.
    """
    root = workspace_root.resolve().as_posix().rstrip("/") + "/"
    files: list[dict[str, Any]] = []
    for section in report.get("data", []):
        for entry in section.get("files", []):
            filename = entry.get("filename")
            if not isinstance(filename, str) or not filename.startswith(root):
                continue
            relative = filename[len(root) :]
            if not relative.startswith(PRODUCTION_ROOT):
                continue
            if relative.startswith(TEST_CDYLIB_PREFIX):
                continue
            lines = entry.get("summary", {}).get("lines", {})
            if int(lines.get("count", 0)) > 0:
                files.append(
                    {
                        "file": relative,
                        "percent": float(lines.get("percent", 0.0)),
                        "covered": int(lines.get("covered", 0)),
                        "count": int(lines.get("count", 0)),
                    }
                )
    return files


def below_threshold(files: list[dict[str, Any]], minimum: float) -> list[dict[str, Any]]:
    """Return production files below the unchanged per-file line threshold."""
    return [entry for entry in files if entry["percent"] < minimum]


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: file_coverage_policy.py MINIMUM REPORT WORKSPACE_ROOT", file=sys.stderr)
        return 2
    minimum = float(argv[1])
    report_path = Path(argv[2])
    workspace_root = Path(argv[3])
    report = json.loads(report_path.read_text())
    files = production_files(report, workspace_root)
    if not files:
        print("coverage report contains no executable production files under crates/", file=sys.stderr)
        return 1

    print("File\tLine coverage\tCovered lines\tCoverable lines")
    for entry in files:
        print(
            f"{entry['file']}\t{entry['percent']:.2f}%\t"
            f"{entry['covered']}\t{entry['count']}"
        )

    if github_summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(github_summary).open("a") as summary:
            summary.write("### Per-file line coverage\n\n")
            summary.write("| File | Coverage | Lines |\n| --- | ---: | ---: |\n")
            for entry in files:
                summary.write(
                    f"| {entry['file']} | {entry['percent']:.2f}% | "
                    f"{entry['covered']}/{entry['count']} |\n"
                )

    failures = below_threshold(files, minimum)
    if failures:
        print(f"\nFiles below {minimum:g}% line coverage:", file=sys.stderr)
        for entry in failures:
            print(f"{entry['file']}: {entry['percent']:.2f}%", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
