"""Exercise release lockfiles with real offline Cargo resolution and Git staging."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / ".github/scripts/update-release-version.py"


class ReleaseLocks(unittest.TestCase):
    def setUp(self):
        directory = ROOT / "target/release-lock-fix"
        directory.mkdir(parents=True, exist_ok=True)
        self.fixture = tempfile.TemporaryDirectory(dir=directory)
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.write("Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n'
                   'resolver = "2"\n[workspace.package]\nversion = "1.0.0"\n')
        for name in ["facade", "leaf"]:
            dependency = '\n[dependencies]\nleaf = { path = "../leaf" }\n' if name == "facade" else ""
            self.write(f"crates/{name}/Cargo.toml", f'[package]\nname = "{name}"\n'
                       f'version.workspace = true\nedition = "2021"\n{dependency}')
            self.write(f"crates/{name}/src/lib.rs", "//! Fixture.\n")
        for name in ["first", "future-proof"]:
            self.write(f"proof-workspaces/{name}/Cargo.toml", '[workspace]\n'
                       f'[package]\nname = "{name}"\nversion = "0.0.0"\nedition = "2021"\n'
                       '[dependencies]\nfacade = { path = "../../crates/facade" }\n')
            self.write(f"proof-workspaces/{name}/src/lib.rs", "//! Independent consumer.\n")
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
        paths = [Path("Cargo.toml"), *[
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
        self.assertEqual(sorted(staged), sorted(["Cargo.toml", *[
            str(manifest.with_name("Cargo.lock")) for manifest in self.manifests]]))
        self.assertEqual(self.run_command("git", "diff", "--name-only").stdout, "")

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
        affected = [Path("Cargo.toml"), *[
            manifest.with_name("Cargo.lock") for manifest in self.manifests]]
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
                   f"    locks = {[str(path) for path in affected[1:]]!r}\n"
                   "    assert all('version = \"2.0.0\"' in Path(p).read_text() for p in locks)\n"
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


class ReleaseWorkflowAdmission(unittest.TestCase):
    workflow = ROOT / ".github/workflows/release.yml"

    def test_release_concurrency_group_serializes_main_and_epic_runs(self):
        text = self.workflow.read_text()
        concurrency = re.search(r"(?ms)^concurrency:\n(.*?)(?=^permissions:\n)", text)
        self.assertIsNotNone(concurrency, "release workflow must declare concurrency")
        group = re.search(r"(?m)^  group: (.+)$", concurrency.group(1))
        cancel = re.search(r"(?m)^  cancel-in-progress: (.+)$", concurrency.group(1))
        self.assertIsNotNone(group, "release workflow must declare its concurrency group")
        self.assertIsNotNone(cancel, "release workflow must declare cancellation behavior")

        def group_for_ref(ref):
            return group.group(1).replace("${{ github.ref_name }}", ref)

        self.assertEqual(group_for_ref("main"), group_for_ref("epic/8056-modules-wiring"))
        self.assertEqual(group_for_ref("main"), "release-main")
        self.assertEqual(cancel.group(1), "false")

    def test_offline_git_tag_collision_leaves_the_losing_branch_bumped(self):
        text = self.workflow.read_text()
        branch_push = 'git push origin "HEAD:${GITHUB_REF_NAME}"'
        tag_push = 'git push origin "${RELEASE_TAG}"'
        self.assertIn(branch_push, text)
        self.assertIn(tag_push, text)
        self.assertLess(text.index(branch_push), text.index(tag_push))

        def git(cwd, *args, check=True):
            result = subprocess.run(
                ["git", *args],
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False,
            )
            if check:
                self.assertEqual(
                    result.returncode,
                    0,
                    f"git {args!r} failed:\n{result.stdout}\n{result.stderr}",
                )
            return result

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote = root / "remote.git"
            seed = root / "seed"
            remote.mkdir()
            seed.mkdir()
            git(remote, "init", "--bare", "--initial-branch=main")
            git(seed, "init", "--initial-branch=main")
            git(seed, "config", "user.name", "Release fixture")
            git(seed, "config", "user.email", "release-fixture@example.invalid")
            (seed / "Cargo.toml").write_text(
                '[workspace.package]\nversion = "1.7.0"\n'
            )
            git(seed, "add", "Cargo.toml")
            git(seed, "commit", "-m", "Initial 1.7.0")
            git(seed, "remote", "add", "origin", str(remote))
            git(seed, "push", "origin", "HEAD:refs/heads/main")
            epic_branch = "epic/8056-modules-wiring"
            git(seed, "checkout", "-b", epic_branch)
            (seed / "epic.txt").write_text("epic-only source\n")
            git(seed, "add", "epic.txt")
            git(seed, "commit", "-m", "Epic-only source")
            git(seed, "push", "origin", f"HEAD:refs/heads/{epic_branch}")

            jobs = {}
            commits = {}
            for branch, name in [("main", "main-job"), (epic_branch, "epic-job")]:
                clone = root / name
                git(root, "clone", "--branch", branch, str(remote), str(clone))
                git(clone, "config", "user.name", "Release fixture")
                git(clone, "config", "user.email", "release-fixture@example.invalid")
                manifest = clone / "Cargo.toml"
                manifest.write_text(manifest.read_text().replace('1.7.0', '1.8.0'))
                git(clone, "add", "Cargo.toml")
                git(clone, "commit", "-m", "Release v1.8.0")
                commits[branch] = git(clone, "rev-parse", "HEAD").stdout.strip()
                git(clone, "push", "origin", f"HEAD:refs/heads/{branch}")
                git(clone, "tag", "-a", "v1.8.0", "-m", "Release v1.8.0")
                jobs[branch] = clone

            git(jobs["main"], "push", "origin", "refs/tags/v1.8.0")
            losing_tag = git(
                jobs[epic_branch], "push", "origin", "refs/tags/v1.8.0", check=False
            )
            self.assertNotEqual(losing_tag.returncode, 0)

            self.assertEqual(
                git(remote, "rev-parse", "refs/heads/main").stdout.strip(),
                commits["main"],
            )
            self.assertEqual(
                git(remote, "rev-parse", f"refs/heads/{epic_branch}").stdout.strip(),
                commits[epic_branch],
            )
            tag_commit = git(remote, "rev-parse", "refs/tags/v1.8.0^{commit}").stdout.strip()
            self.assertEqual(tag_commit, commits["main"])
            self.assertNotEqual(tag_commit, commits[epic_branch])

    def test_release_prepare_admits_only_main_and_the_exact_epic_branch(self):
        text = self.workflow.read_text()
        prepare = re.search(r"(?ms)^  prepare:\n(.*?)(?=^  github-release:\n)", text)
        self.assertIsNotNone(prepare, "release prepare job must exist")
        condition = re.search(r"(?m)^    if: \$\{\{ (.+) \}\}$", prepare.group(1))
        self.assertIsNotNone(condition, "release prepare job must have a ref guard")
        expression = condition.group(1)
        expected = (
            "github.ref == 'refs/heads/main' || "
            "github.ref == 'refs/heads/epic/8056-modules-wiring'"
        )
        self.assertEqual(expression, expected)

        admitted_refs = set(re.findall(r"github\.ref == '([^']+)'", expression))
        self.assertEqual(
            admitted_refs,
            {"refs/heads/main", "refs/heads/epic/8056-modules-wiring"},
        )
        self.assertTrue(all(ref in admitted_refs for ref in [
            "refs/heads/main",
            "refs/heads/epic/8056-modules-wiring",
        ]))
        self.assertFalse(any(ref in admitted_refs for ref in [
            "refs/heads/epic-8056-tsd-release18",
            "refs/heads/epic/8056-other",
            "refs/tags/v1.8.0",
        ]))

    def test_release_coverage_starts_rabbitmq_before_running_the_full_profile(self):
        text = self.workflow.read_text()
        rabbit_url = "TSD_RABBITMQ_URL: amqp://guest:guest@127.0.0.1:5672/%2f"
        rabbit_start = "run: .github/scripts/start-rabbitmq.sh 5672"
        coverage = "run: .github/scripts/check-file-coverage.sh 90 target/coverage.json"
        self.assertTrue(rabbit_url in text, "release coverage requires the RabbitMQ URL")
        self.assertTrue(rabbit_start in text, "release coverage must start RabbitMQ")
        self.assertLess(text.index(rabbit_start), text.index(coverage))

    def test_release_installs_strace_before_native_coverage_proofs(self):
        text = self.workflow.read_text()
        strace_install = "run: sudo apt-get install --yes strace"
        coverage = "run: .github/scripts/check-file-coverage.sh 90 target/coverage.json"
        self.assertTrue(strace_install in text, "native coverage requires strace")
        self.assertLess(text.index(strace_install), text.index(coverage))


if __name__ == "__main__":
    unittest.main()
