"""Verify reproducible source-release archives and checksum manifests."""
import hashlib
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / ".github/scripts/create-release-assets.py"


class ReleaseAssets(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.run_command("git", "init", "--quiet")
        self.run_command("git", "config", "user.name", "Release assets test")
        self.run_command("git", "config", "user.email", "test@example.invalid")
        (self.root / "README.md").write_text("release source fixture\n")
        self.run_command("git", "add", "README.md")
        self.run_command("git", "commit", "--quiet", "-m", "release fixture")
        self.run_command("git", "tag", "v2.1.2")

    def run_command(self, *command, check=True):
        result = subprocess.run(command, cwd=self.root, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and result.returncode:
            self.fail(f"{command}: exit {result.returncode}\n{result.stdout}\n{result.stderr}")
        return result

    def generate(self, tag="v2.1.2"):
        return self.run_command(sys.executable, str(HELPER), "--repository", str(self.root),
                                "--tag", tag, "--output-dir", str(self.root / "release"),
                                check=False)

    def test_tagged_source_archive_is_attached_to_a_valid_sha256_manifest(self):
        result = self.generate()
        self.assertEqual(result.returncode, 0, result.stderr)
        archive_name = "tinystoragedrivers-v2.1.2.tar.gz"
        archive_path = self.root / "release" / archive_name
        checksum_path = self.root / "release" / "checksum.toml"
        self.assertTrue(archive_path.is_file())
        self.assertTrue(checksum_path.is_file())

        manifest = tomllib.loads(checksum_path.read_text())
        self.assertEqual(manifest, {
            "sha256": {archive_name: hashlib.sha256(archive_path.read_bytes()).hexdigest()}
        })
        with tarfile.open(archive_path, "r:gz") as archive:
            self.assertEqual(archive.getnames(), [
                "tinystoragedrivers-v2.1.2/README.md",
            ])

    def test_archive_generation_requires_a_semver_release_tag(self):
        result = self.generate("feature/branch")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be a vMAJOR.MINOR.PATCH tag", result.stderr)

    def test_archive_generation_requires_the_tag_to_point_at_checked_out_head(self):
        self.run_command("git", "checkout", "--quiet", "-b", "later")
        (self.root / "README.md").write_text("changed after the release tag\n")
        self.run_command("git", "add", "README.md")
        self.run_command("git", "commit", "--quiet", "-m", "changed source")
        result = self.generate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not point at HEAD", result.stderr)


if __name__ == "__main__":
    unittest.main()
