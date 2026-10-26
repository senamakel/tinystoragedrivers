"""Exercise release lockfiles with real offline Cargo resolution and Git staging."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = Path(os.environ.get("RELEASE_VERSION_HELPER",
                            ROOT / ".github/scripts/update-release-version.py"))


class ReleaseLocks(unittest.TestCase):
    def setUp(self):
        directory = ROOT / "target/release-lock-fix"
        directory.mkdir(parents=True, exist_ok=True)
        self.fixture = tempfile.TemporaryDirectory(dir=directory)
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n'
                   'resolver = "2"\n[workspace.package]\nversion = "1.0.0"\n'
                   '[workspace.dependencies]\n'
                   'leaf = { version = "1.0.0", path = "crates/leaf" }\n'
                   'outside-fixture = { version = "1.0.0", path = "external" }\n')
        for name in ["facade", "leaf"]:
            dependency = ('\n[dependencies]\nleaf = { version = "1.0.0", path = "../leaf" }\n'
                          if name == "facade" else "")
            self.write(f"crates/{name}/Cargo.toml", f'[package]\nname = "{name}"\n'
                       f'version.workspace = true\nedition = "2021"\n{dependency}')
            self.write(f"crates/{name}/src/lib.rs", "//! Fixture.\n")
        for name in ["first", "future-proof"]:
            self.write(f"proof-workspaces/{name}/Cargo.toml", '[workspace]\n'
                       f'[package]\nname = "{name}"\nversion = "0.0.0"\nedition = "2021"\n'
                   '[dependencies]\nfacade = { version = "1.0.0", path = "../../crates/facade" }\n')
            self.write(f"proof-workspaces/{name}/src/lib.rs", "//! Independent consumer.\n")
        self.write("external/Cargo.toml", '[package]\nname = "outside-fixture"\n'
                   'version = "1.0.0"\nedition = "2021"\n')
        self.write("external/src/lib.rs", "//! Not a workspace member.\n")
        self.manifests = [Path("Cargo.toml"), *[p.relative_to(self.root) for p in
                          sorted(self.root.glob("proof-workspaces/*/Cargo.toml"))]]
        self.run_command("git", "init", "--quiet")
        for manifest in self.manifests:
            self.metadata(manifest)
        self.run_command("git", "add", ".")
        self.run_command("git", "-c", "user.name=Release test", "-c", "user.email=test@example.invalid",
                         "commit", "--quiet", "-m", "Initial fixture")

    def write(self, path, contents):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(contents)

    def run_command(self, *command, check=True, env=None):
        result = subprocess.run(command, cwd=self.root,
                                env={**os.environ, "CARGO_NET_OFFLINE": "true", **(env or {})},
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and result.returncode:
            self.fail(f"{command}: exit {result.returncode}\n{result.stdout}\n{result.stderr}")
        return result

    def metadata(self, manifest, locked=False):
        command = ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)]
        if locked:
            command.append("--locked")
        return self.run_command(*command)

    def release_files(self):
        crate_manifests = sorted(
            path.relative_to(self.root) for path in self.root.glob("crates/**/Cargo.toml")
        )
        paths = [Path("Cargo.toml"), *crate_manifests,
                 *[manifest for manifest in self.manifests if manifest != Path("Cargo.toml")], *[
            manifest.with_name("Cargo.lock") for manifest in self.manifests]]
        return {path: (self.root / path).read_bytes() if (self.root / path).exists() else None
                for path in paths}

    def test_major_bump_updates_validates_and_stages_every_workspace_lock(self):
        self.write("unrelated.txt", "leave this unstaged\n")
        self.run_command(sys.executable, str(HELPER), "2.0.0", "facade")
        for manifest in self.manifests:
            self.metadata(manifest, locked=True)
            lock = (self.root / manifest).with_name("Cargo.lock").read_text()
            self.assertIn('version = "2.0.0"', lock)
            self.assertNotIn('version = "1.0.0"', lock)
        staged = self.run_command("git", "diff", "--cached", "--name-only").stdout.splitlines()
        self.assertEqual(sorted(staged), sorted(["Cargo.toml", "crates/facade/Cargo.toml",
                                                  "proof-workspaces/first/Cargo.toml",
                                                  "proof-workspaces/future-proof/Cargo.toml", *[
            str(manifest.with_name("Cargo.lock")) for manifest in self.manifests]]))
        self.assertEqual(self.run_command("git", "diff", "--name-only").stdout, "")
        root_manifest = (self.root / "Cargo.toml").read_text()
        facade_manifest = (self.root / "crates/facade/Cargo.toml").read_text()
        self.assertIn('leaf = { version = "2.0.0", path = "crates/leaf" }', root_manifest)
        self.assertIn('outside-fixture = { version = "1.0.0", path = "external" }', root_manifest)
        self.assertIn('facade = { version = "2.0.0", path = "../../crates/facade" }',
                      (self.root / "proof-workspaces/first/Cargo.toml").read_text())
        self.assertIn('leaf = { version = "2.0.0", path = "../leaf" }', facade_manifest)

    def test_failed_proof_update_stops_before_staging(self):
        self.write("proof-workspaces/future-proof/Cargo.toml", '[workspace]\n[package]\n'
                   'name = "broken"\nversion = "0.0.0"\n[dependencies]\n'
                   'missing = { path = "../../missing" }\n')
        before = self.release_files()
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "facade", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.release_files(), before)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")


    def test_wrong_release_package_stops_before_staging(self):
        before = self.release_files()
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "absent", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("version bump did not take", result.stderr)
        self.assertEqual(self.release_files(), before)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")

    def test_failed_locked_proof_validation_stops_before_staging(self):
        # Fault injection only removes a proof lock after update. Every command
        # still executes real Cargo, including the locked resolver rejection.
        cargo = shutil.which("cargo")
        self.assertIsNotNone(cargo)
        self.write("bin/cargo", f"#!{sys.executable}\n"
                   "import os, sys\nfrom pathlib import Path\n"
                   "if 'metadata' in sys.argv and '--locked' in sys.argv "
                   "and 'proof-workspaces/first/Cargo.toml' in sys.argv:\n"
                   "    Path('proof-workspaces/first/Cargo.lock').unlink()\n"
                   f"os.execv({cargo!r}, [{cargo!r}, *sys.argv[1:]])\n")
        (self.root / "bin/cargo").chmod(0o755)
        before = self.release_files()
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "facade", check=False,
                                  env={"PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"]})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--locked", result.stderr)
        self.assertEqual(self.release_files(), before)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")

    def test_late_failure_restores_existing_bytes_and_removes_new_proof_lock(self):
        # Preserve local edits too, including valid comments Cargo rewrites.
        affected = list(self.release_files())
        new_lock = Path("proof-workspaces/future-proof/Cargo.lock")
        (self.root / new_lock).unlink()
        for path in affected:
            if (self.root / path).exists():
                with (self.root / path).open("ab") as handle:
                    handle.write(b"\n# Preserve this preexisting local comment.\n")
        self.write("unrelated.txt", "preserve this local file\n")
        before = {path: (self.root / path).read_bytes() for path in affected
                  if (self.root / path).exists()}
        unrelated = (self.root / "unrelated.txt").read_bytes()
        cargo = shutil.which("cargo")
        self.assertIsNotNone(cargo)
        self.write("bin/cargo", f"#!{sys.executable}\n"
                   "import os, sys\nfrom pathlib import Path\n"
                   "if 'metadata' in sys.argv and '--all-features' in sys.argv "
                   "and 'proof-workspaces/first/Cargo.toml' in sys.argv:\n"
                   "    release_inputs = ['Cargo.toml', 'Cargo.lock', 'crates/facade/Cargo.toml', "
                   "'proof-workspaces/first/Cargo.toml', 'proof-workspaces/first/Cargo.lock', "
                   "'proof-workspaces/future-proof/Cargo.toml', 'proof-workspaces/future-proof/Cargo.lock']\n"
                   "    assert all('version = \"2.0.0\"' in Path(p).read_text() "
                   "for p in release_inputs)\n"
                   "    Path('all-locks-updated').write_text('verified')\n"
                   "    Path('proof-workspaces/first/Cargo.lock').unlink()\n"
                   f"os.execv({cargo!r}, [{cargo!r}, *sys.argv[1:]])\n")
        (self.root / "bin/cargo").chmod(0o755)
        result = self.run_command(sys.executable, str(HELPER), "2.0.0", "facade", check=False,
                                  env={"PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"]})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--locked", result.stderr)
        self.assertTrue((self.root / "all-locks-updated").exists())
        for path, contents in before.items():
            with self.subTest(path=path):
                self.assertTrue((self.root / path).exists())
                self.assertEqual((self.root / path).read_bytes(), contents)
        self.assertFalse((self.root / new_lock).exists())
        self.assertEqual((self.root / "unrelated.txt").read_bytes(), unrelated)
        self.assertEqual(self.run_command("git", "diff", "--cached", "--name-only").stdout, "")


class ReleaseWorkflowRefs(unittest.TestCase):
    def test_release_jobs_allow_canonical_main_and_exact_fork_epics(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        legacy_condition = ("(github.repository == 'tinyhumansai/tinystoragedrivers' && "
                           "github.ref == 'refs/heads/main') || "
                           "(github.repository == 'senamakel/tinystoragedrivers' && "
                           "github.ref == 'refs/heads/epic/8131-storage-primitives')")
        migration_condition = (legacy_condition + " || "
                               "(github.repository == 'senamakel/tinystoragedrivers' && "
                               "github.ref == 'refs/heads/epic/8369-security-migration')")
        self.assertEqual(workflow.count(migration_condition), 2)

        def allowed(repository, ref):
            return (repository, ref) in {
                ("tinyhumansai/tinystoragedrivers", "refs/heads/main"),
                ("senamakel/tinystoragedrivers", "refs/heads/epic/8131-storage-primitives"),
                ("senamakel/tinystoragedrivers", "refs/heads/epic/8369-security-migration"),
            }

        self.assertTrue(allowed("tinyhumansai/tinystoragedrivers", "refs/heads/main"))
        self.assertTrue(allowed("senamakel/tinystoragedrivers",
                                "refs/heads/epic/8131-storage-primitives"))
        self.assertTrue(allowed("senamakel/tinystoragedrivers",
                                "refs/heads/epic/8369-security-migration"))
        self.assertFalse(allowed("senamakel/tinystoragedrivers", "refs/heads/main"))
        self.assertFalse(allowed("tinyhumansai/tinystoragedrivers", "refs/heads/feature"))

    def test_release_uploads_a_hashed_source_archive_and_checksum(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertIn(".github/scripts/create-release-assets.py", workflow)
        self.assertIn('gh release upload "$RELEASE_TAG"', workflow)
        self.assertIn("target/release-assets/checksum.toml", workflow)


if __name__ == "__main__":
    unittest.main()
