#!/usr/bin/env python3
"""Verify the wrapper adds the compiled cdylib without replacing LLVM objects."""

from pathlib import Path
import shlex
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from migration_cdylib_llvm_cov import wrapper_source  # noqa: E402


class MigrationCdylibLlvmCovTests(unittest.TestCase):
    def test_report_commands_add_the_cdylib_object_and_keep_existing_arguments(self):
        llvm_cov = Path("/toolchain/bin/llvm-cov")
        module = Path("/checkout/target/debug/libmigration module.dylib")
        source = wrapper_source(llvm_cov, module)
        tool = shlex.quote(str(llvm_cov))
        object_path = shlex.quote(str(module))

        self.assertIn(f'exec {tool} "$operation" --object {object_path} "$@"', source)
        self.assertIn("export|report|show)", source)
        self.assertIn("*)", source)


if __name__ == "__main__":
    unittest.main()
