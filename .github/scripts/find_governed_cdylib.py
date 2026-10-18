"""Select the fixture cdylib from Cargo's exact compiler-artifact record."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PACKAGE_NAME = "tinystoragedrivers-governed-consumer-fixture"
TARGET_NAME = "tinystoragedrivers_governed_consumer_fixture"


def artifact_paths(
    build_output: str, target_directory: Path, suffix: str
) -> list[Path]:
    """Return existing cdylib files from the exact fixture package record."""
    selected: list[Path] = []
    for line in build_output.splitlines():
        try:
            message: Any = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(message, dict):
            continue
        if message.get("reason") != "compiler-artifact":
            continue
        package_name = str(message.get("package_id", "")).rsplit("#", 1)[-1].split("@", 1)[0]
        if package_name != PACKAGE_NAME:
            continue
        target = message.get("target", {})
        if target.get("name") != TARGET_NAME or "cdylib" not in target.get("kind", []):
            continue
        for filename in message.get("filenames", []):
            path = Path(filename)
            if path.suffix.lstrip(".") != suffix:
                continue
            if not path.is_absolute():
                path = target_directory / path
            try:
                path.resolve().relative_to(target_directory.resolve())
            except ValueError:
                continue
            if path.is_file():
                selected.append(path.resolve())
    return selected


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: find_governed_cdylib.py BUILD_LOG TARGET_DIRECTORY SUFFIX", file=sys.stderr)
        return 2
    build_log = Path(argv[1])
    target_directory = Path(argv[2])
    try:
        paths = artifact_paths(build_log.read_text(), target_directory, argv[3])
    except OSError as error:
        print(f"could not read governed cdylib compiler artifacts: {error}", file=sys.stderr)
        return 1
    if len(paths) != 1:
        print(
            f"expected one fixture cdylib compiler artifact, found {len(paths)}",
            file=sys.stderr,
        )
        return 1
    print(paths[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
