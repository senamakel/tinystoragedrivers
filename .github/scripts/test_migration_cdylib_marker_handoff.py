#!/usr/bin/env python3
"""Prove the coverage gate reads the module marker from Cargo's target dir."""

from pathlib import Path
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True


class MigrationCdylibMarkerHandoffTests(unittest.TestCase):
    def test_coverage_gate_follows_metadata_target_outside_checkout_target(self):
        repo_root = Path(__file__).resolve().parents[2]
        coverage_script = repo_root / ".github/scripts/check-file-coverage.sh"

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "checkout"
            target = Path(temporary) / "configured-shared-target"
            bin_dir = Path(temporary) / "bin"
            for directory in [root / "target", target, bin_dir]:
                directory.mkdir(parents=True, exist_ok=True)

            module_path = target / "debug/libtinystoragedrivers_migration_module_fixture.so"
            module_path.parent.mkdir(parents=True, exist_ok=True)
            module_path.write_text("compiled module fixture")

            cargo = bin_dir / "cargo"
            cargo.write_text(
                "#!/bin/sh\n"
                "if [ \"${1:-}\" = metadata ]; then\n"
                "  python3 -c 'import json,os; print(json.dumps({\"target_directory\": os.environ[\"FAKE_TARGET\"]}))'\n"
                "fi\n"
                "exit 0\n"
            )
            cargo.chmod(0o755)

            script_names = [
                "test_file_coverage_policy.py",
                "test_governed_cdylib_coverage.py",
                "test_migration_cdylib_llvm_cov.py",
                "test_migration_cdylib_marker_handoff.py",
                "file_coverage_policy.py",
                "native_profile_tests.py",
                "check-native-io-fault.py",
                "check-native-key-race.py",
                "check-native-secret-service.py",
            ]
            for name in script_names:
                path = root / ".github/scripts" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("pass\n")

            helper = root / ".github/scripts/migration_cdylib_path.py"
            shutil.copy2(repo_root / ".github/scripts/migration_cdylib_path.py", helper)

            runner = root / "scripts/test-governed-cdylib.sh"
            runner.parent.mkdir(parents=True, exist_ok=True)
            runner.write_text(
                "#!/bin/sh\n"
                "mkdir -p \"$FAKE_TARGET\"\n"
                "printf '%s\\n' \"$FAKE_MODULE\" > \"$FAKE_TARGET/migration-module-cdylib-path.txt\"\n"
            )
            runner.chmod(0o755)

            wrapper = root / ".github/scripts/migration_cdylib_llvm_cov.py"
            wrapper.write_text(
                "#!/usr/bin/env python3\n"
                "import os\n"
                "import sys\n"
                "open(os.environ['FAKE_PATH_SEEN'], 'w').write(sys.argv[2])\n"
                "print(os.environ['FAKE_LLVM_COV'])\n"
                "print(os.environ['FAKE_LLVM_PROFDATA'])\n"
            )
            wrapper.chmod(0o755)

            environment = {
                **os.environ,
                "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                "FAKE_TARGET": str(target),
                "FAKE_MODULE": str(module_path),
                "FAKE_PATH_SEEN": str(Path(temporary) / "path-seen.txt"),
                "FAKE_LLVM_COV": str(temporary) + "/llvm-cov-wrapper",
                "FAKE_LLVM_PROFDATA": str(temporary) + "/llvm-profdata",
            }
            result = subprocess.run(
                ["bash", str(coverage_script), "90", "target/coverage.json"],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(Path(environment["FAKE_PATH_SEEN"]).read_text(), str(module_path))


if __name__ == "__main__":
    unittest.main()
