#!/usr/bin/env python3
"""Resolve the built migration cdylib through Cargo's configured target dir."""

import json
from pathlib import Path
import subprocess
import sys


def migration_cdylib_path(repo_root: Path) -> Path:
    manifest = repo_root / "Cargo.toml"
    metadata = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            str(manifest),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    target_dir = Path(json.loads(metadata.stdout)["target_directory"])
    marker = target_dir / "migration-module-cdylib-path.txt"
    try:
        module = Path(marker.read_text(encoding="utf-8").strip())
    except OSError as error:
        raise SystemExit(f"missing compiled migration cdylib marker: {marker}") from error
    if not module.is_absolute():
        raise SystemExit(f"migration cdylib marker is not an absolute path: {marker}")
    if not module.is_file():
        raise SystemExit(f"compiled migration cdylib is missing: {module}")
    return module


def main() -> None:
    repo_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    print(migration_cdylib_path(repo_root))


if __name__ == "__main__":
    main()
