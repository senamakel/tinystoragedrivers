"""Create an LLVM tool wrapper that includes the instrumented migration module object."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def toolchain_llvm_tool(name: str) -> Path:
    """Return a tool from the active rustc toolchain's llvm-tools directory."""
    rustc = shutil.which("rustc")
    if not rustc:
        raise RuntimeError(f"rustc is unavailable; cannot locate llvm-{name}")
    sysroot = Path(subprocess.check_output([rustc, "--print", "sysroot"], text=True).strip())
    host = next(
        line.split(": ", 1)[1]
        for line in subprocess.check_output([rustc, "-vV"], text=True).splitlines()
        if line.startswith("host: ")
    )
    candidate = sysroot / "lib" / "rustlib" / host / "bin" / f"llvm-{name}"
    if os.name == "nt":
        candidate = candidate.with_suffix(".exe")
    if not candidate.is_file():
        raise RuntimeError(f"matching llvm-{name} binary is missing: {candidate}")
    return candidate


def active_llvm_cov() -> Path:
    """Return llvm-cov from the active rustc toolchain, or its configured override."""
    if configured := os.environ.get("LLVM_COV"):
        return Path(configured)
    return toolchain_llvm_tool("cov")


def wrapper_source(llvm_cov: Path, module_object: Path) -> str:
    """Forward LLVM commands while adding the loaded TinyBus module object."""
    real_tool = shlex.quote(str(llvm_cov))
    object_path = shlex.quote(str(module_object))
    return f'''#!/usr/bin/env bash
set -euo pipefail
case "${{1:-}}" in
  export|report|show)
    operation="$1"
    shift
    exec {real_tool} "$operation" --object {object_path} "$@"
    ;;
  *)
    exec {real_tool} "$@"
    ;;
esac
'''


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "usage: migration_cdylib_llvm_cov.py WORKSPACE_ROOT MODULE_CDYLIB",
            file=sys.stderr,
        )
        return 2
    workspace_root = Path(argv[1]).resolve()
    module_object = Path(argv[2]).resolve()
    if not module_object.is_file():
        print(f"instrumented migration module is missing: {module_object}", file=sys.stderr)
        return 1
    wrapper = workspace_root / "target" / "migration-llvm-cov-wrapper.sh"
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    wrapper.write_text(wrapper_source(active_llvm_cov(), module_object))
    wrapper.chmod(0o755)
    print(wrapper)
    print(os.environ.get("LLVM_PROFDATA") or toolchain_llvm_tool("profdata"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
