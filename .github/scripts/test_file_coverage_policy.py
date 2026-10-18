#!/usr/bin/env python3
"""Policy regressions for the exact test-only cdylib coverage exclusion."""

from pathlib import Path
import sys
import unittest

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from file_coverage_policy import below_threshold, production_files  # noqa: E402


class FileCoveragePolicyTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("/checkout").resolve()

    def entry(self, relative, percent, count=100, covered=0):
        return {
            "filename": (self.root / relative).as_posix(),
            "summary": {
                "lines": {
                    "count": count,
                    "covered": covered,
                    "percent": percent,
                }
            },
        }

    def test_only_the_exact_nonpublished_cdylib_fixture_is_excluded(self):
        report = {
            "data": [
                {
                    "files": [
                        self.entry(
                            "crates/tinystoragedrivers-tinybus/test-cdylib/src/lib.rs",
                            0,
                            count=533,
                        ),
                        self.entry(
                            "crates/tinystoragedrivers-tinybus/src/storage_host.rs",
                            81.69,
                        ),
                        self.entry(
                            "crates/tinystoragedrivers-tinybus/test-cdylib-extra/src/lib.rs",
                            89.99,
                        ),
                        self.entry("crates/other/src/lib.rs", 100),
                        self.entry("outside/crates/src/lib.rs", 0),
                    ]
                }
            ]
        }

        files = production_files(report, self.root)
        names = {entry["file"] for entry in files}
        self.assertNotIn(
            "crates/tinystoragedrivers-tinybus/test-cdylib/src/lib.rs", names
        )
        self.assertIn("crates/tinystoragedrivers-tinybus/src/storage_host.rs", names)
        self.assertIn(
            "crates/tinystoragedrivers-tinybus/test-cdylib-extra/src/lib.rs", names
        )
        failures = below_threshold(files, 90)
        self.assertEqual(
            {entry["file"] for entry in failures},
            {
                "crates/tinystoragedrivers-tinybus/src/storage_host.rs",
                "crates/tinystoragedrivers-tinybus/test-cdylib-extra/src/lib.rs",
            },
        )

    def test_reports_without_the_fixture_still_fail_for_low_production_coverage(self):
        files = production_files(
            {
                "data": [
                    {
                        "files": [
                            self.entry("crates/tinystoragedrivers-busproxy/src/host.rs", 89.9)
                        ]
                    }
                ]
            },
            self.root,
        )
        self.assertEqual(len(below_threshold(files, 90)), 1)

    def test_unexecuted_production_files_are_not_silently_removed(self):
        files = production_files(
            {
                "data": [
                    {
                        "files": [
                            self.entry("crates/tinystoragedrivers-busproxy/src/host.rs", 0)
                        ]
                    }
                ]
            },
            self.root,
        )
        self.assertEqual(files[0]["percent"], 0)
        self.assertEqual(len(below_threshold(files, 90)), 1)


if __name__ == "__main__":
    unittest.main()
