#!/usr/bin/env python3
"""Exercise the callback proof wrapper without invoking Cargo or a compiler."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = Path(os.environ.get("TSD_DOCUMENT_CALLBACK_HELPER", ROOT / "scripts/test-document-callback.sh"))


class CallbackRunnerTests(unittest.TestCase):
    def setUp(self):
        (ROOT / "target").mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=ROOT / "target")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scripts = self.root / "scripts"
        self.scripts.mkdir()
        helper = self.scripts / "test-document-callback.sh"
        shutil.copy2(HELPER, helper)
        self.fake_bin = self.root / "fake-bin"
        self.fake_bin.mkdir()
        self.calls = self.root / "cargo-calls.jsonl"
        self.default_target = self.root / "target/default callback proof"
        self.write_executable(
            self.fake_bin / "cargo",
            """#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['FAKE_CARGO_CALLS'], 'a', encoding='utf-8') as calls:
    calls.write(json.dumps({'args': args, 'module': os.environ.get('TSD_DOCUMENT_MODULE')}) + '\\n')
if args[:1] == ['metadata']:
    print(json.dumps({'target_directory': os.environ['FAKE_DEFAULT_TARGET']}))
elif args[:1] == ['build']:
    target = pathlib.Path(args[args.index('--target-dir') + 1]) if '--target-dir' in args else pathlib.Path(os.environ['FAKE_DEFAULT_TARGET'])
    artifact = target / 'debug' / 'libdocument_callback_module_proof.so'
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.touch()
elif args[:1] != ['test']:
    raise SystemExit('unexpected Cargo invocation: ' + repr(args))
""",
        )
        self.write_executable(self.fake_bin / "uname", "#!/bin/sh\nprintf '%s\\n' Linux\n")
        self.env = {
            **os.environ,
            "PATH": f"{self.fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_CARGO_CALLS": str(self.calls),
            "FAKE_DEFAULT_TARGET": str(self.default_target),
        }

    @staticmethod
    def write_executable(path, contents):
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o755)

    def run_helper(self, *args):
        return subprocess.run(
            ["bash", "scripts/test-document-callback.sh", *args],
            cwd=self.root,
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
        )

    def calls_for(self, expected_target_arg):
        calls = [json.loads(line) for line in self.calls.read_text(encoding="utf-8").splitlines()]
        cargo_calls = [call for call in calls if call["args"][:1] != ["metadata"]]
        self.assertEqual([call["args"][0] for call in cargo_calls], ["build", "test"])
        for call in cargo_calls:
            args = call["args"]
            self.assertEqual(args[args.index("--target-dir") + 1] if "--target-dir" in args else None, expected_target_arg)
        return cargo_calls

    def test_default_uses_the_proof_workspace_target(self):
        result = self.run_helper()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls_for(None)
        self.assertEqual(calls[-1]["module"], str(self.default_target / "debug/libdocument_callback_module_proof.so"))

    def test_relative_target_dir_is_shared_by_build_test_and_lookup(self):
        target = "target/relative callback proof"
        result = self.run_helper("--target-dir", target)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls_for(target)
        expected_module = str(self.root / target / "debug/libdocument_callback_module_proof.so")
        self.assertEqual(calls[1]["module"], expected_module)

    def test_absolute_target_dir_with_spaces_is_shared_by_build_test_and_lookup(self):
        target = self.root / "target/absolute callback proof"
        result = self.run_helper("--target-dir", str(target))
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls_for(str(target))
        expected_module = str(target / "debug/libdocument_callback_module_proof.so")
        self.assertEqual(calls[1]["module"], expected_module)


if __name__ == "__main__":
    unittest.main()
