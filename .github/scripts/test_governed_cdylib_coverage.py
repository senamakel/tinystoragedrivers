#!/usr/bin/env python3
"""Exercise the native coverage gate's pass and fail closed paths."""

from pathlib import Path
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from governed_cdylib_coverage import TEST_NAME, validate_native_test_output  # noqa: E402
from find_governed_cdylib import artifact_paths  # noqa: E402


PASS = f"""running 1 test
test {TEST_NAME} ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 26 filtered out; finished in 0.1s
"""


class GovernedCdylibCoverageTests(unittest.TestCase):
    def test_requires_one_exact_pass_with_no_ignored_test(self):
        validate_native_test_output(PASS)

    def test_failed_process_status_is_not_hidden_by_tee(self):
        with self.assertRaisesRegex(ValueError, "exited 7"):
            validate_native_test_output(PASS, exit_code=7)

    def test_missing_or_ignored_native_proof_fails(self):
        for output in [
            "running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 filtered out",
            f"running 1 test\ntest {TEST_NAME} ... ignored\ntest result: ok. 0 passed; 0 failed; 1 ignored; 0 filtered out",
            f"running 2 tests\ntest {TEST_NAME} ... ok\ntest {TEST_NAME} ... ok\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 filtered out",
        ]:
            with self.assertRaises(ValueError):
                validate_native_test_output(output)

    def run_coverage_runner(self, output, exit_code=0, omit_artifact=False, build_exit=0):
        repo_root = Path(__file__).resolve().parents[2]
        runner = repo_root / "scripts/test-governed-cdylib.sh"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bin_dir = root / "bin"
            target = root / "target"
            bin_dir.mkdir()
            fake_cargo = bin_dir / "cargo"
            fake_cargo.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "args=sys.argv[1:]\n"
                "if args[0] == 'metadata':\n"
                " print(json.dumps({'target_directory': os.environ['FAKE_TARGET']}))\n"
                "elif args[0] == 'build':\n"
                " if int(os.environ.get('FAKE_BUILD_EXIT', '0')) != 0: sys.exit(int(os.environ['FAKE_BUILD_EXIT']))\n"
                " if os.environ.get('FAKE_OMIT_ARTIFACT') != '1':\n"
                "  p=os.path.join(os.environ['FAKE_TARGET'], 'debug', 'deps', 'libtinystoragedrivers_governed_consumer_fixture.' + os.environ['FAKE_SUFFIX'])\n"
                "  os.makedirs(os.path.dirname(p), exist_ok=True)\n"
                "  open(p, 'w').write('fixture')\n"
                "  print(json.dumps({'reason':'compiler-artifact','package_id':'path+file:///fixture#tinystoragedrivers-governed-consumer-fixture@1.5.0','target':{'name':'tinystoragedrivers_governed_consumer_fixture','kind':['cdylib']},'filenames':[p]}))\n"
                "elif args[0:2] == ['llvm-cov','test']:\n"
                " open(os.environ['FAKE_INVOCATION'], 'w').write(' '.join(args))\n"
                " print(os.environ['FAKE_TEST_OUTPUT'], end='')\n"
                " sys.exit(int(os.environ.get('FAKE_TEST_EXIT', '0')))\n"
                "else:\n"
                " print('unexpected cargo command', args, file=sys.stderr)\n"
                " sys.exit(91)\n"
            )
            fake_cargo.chmod(fake_cargo.stat().st_mode | stat.S_IXUSR)
            env = {
                **os.environ,
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "FAKE_TARGET": str(target),
                "FAKE_SUFFIX": "dylib" if sys.platform == "darwin" else "dll" if os.name == "nt" else "so",
                "FAKE_TEST_OUTPUT": output,
                "FAKE_TEST_EXIT": str(exit_code),
                "FAKE_BUILD_EXIT": str(build_exit),
                "FAKE_OMIT_ARTIFACT": "1" if omit_artifact else "0",
                "FAKE_INVOCATION": str(root / "cargo-invocation.txt"),
            }
            result = subprocess.run(
                ["bash", str(runner), "--coverage"],
                cwd=repo_root,
                env=env,
                text=True,
                capture_output=True,
            )
            invocation = (root / "cargo-invocation.txt").read_text() if (root / "cargo-invocation.txt").exists() else ""
            return result, invocation

    def test_coverage_runner_loads_artifact_and_runs_exact_ignored_test(self):
        result, invocation = self.run_coverage_runner(PASS)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--no-report", invocation)
        self.assertNotIn("--no-clean", invocation)
        self.assertIn("--all-features", invocation)
        self.assertIn(TEST_NAME, invocation)
        self.assertIn("--ignored --exact --nocapture", invocation)

    def test_coverage_runner_propagates_native_test_exit_status(self):
        result, _ = self.run_coverage_runner(PASS, exit_code=7)
        self.assertEqual(result.returncode, 7)

    def test_coverage_runner_rejects_an_ignored_or_missing_test(self):
        result, _ = self.run_coverage_runner(
            f"running 1 test\ntest {TEST_NAME} ... ignored\n"
            "test result: ok. 0 passed; 0 failed; 1 ignored; 0 filtered out\n"
        )
        self.assertNotEqual(result.returncode, 0)

    def test_coverage_runner_fails_when_the_compiled_cdylib_is_missing(self):
        result, _ = self.run_coverage_runner(PASS, omit_artifact=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected one fixture cdylib compiler artifact, found 0", result.stderr)

    def test_coverage_runner_propagates_fixture_build_failure(self):
        result, _ = self.run_coverage_runner(PASS, build_exit=23)
        self.assertEqual(result.returncode, 23)

    def test_artifact_selection_uses_the_exact_cargo_cdylib_record(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary)
            exact = target / "debug" / "deps" / "libtinystoragedrivers_governed_consumer_fixture.so"
            decoy = target / "debug" / "deps" / "libsome_other_fixture.so"
            exact.parent.mkdir(parents=True)
            exact.write_text("fixture")
            decoy.write_text("decoy")
            output = "\n".join(
                [
                    json.dumps(
                        {
                            "reason": "compiler-artifact",
                            "package_id": "path+file:///other#other-fixture@1.0.0",
                            "target": {"name": "some_other_fixture", "kind": ["cdylib"]},
                            "filenames": [str(decoy)],
                        }
                    ),
                    json.dumps(
                        {
                            "reason": "compiler-artifact",
                            "package_id": "path+file:///fixture#tinystoragedrivers-governed-consumer-fixture@1.5.0",
                            "target": {
                                "name": "tinystoragedrivers_governed_consumer_fixture",
                                "kind": ["cdylib"],
                            },
                            "filenames": [str(exact)],
                        }
                    ),
                ]
            )
            self.assertEqual(artifact_paths(output, target, "so"), [exact.resolve()])


if __name__ == "__main__":
    unittest.main()
