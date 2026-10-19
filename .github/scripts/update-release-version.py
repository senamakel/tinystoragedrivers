"""Bump a release, validate every independent workspace, then stage exact paths.

Run from the repository root with VERSION PACKAGE. Cargo's workspace update
preserves unrelated locked dependencies where possible. No paths are staged
until every resolver succeeds. Failed updates or validation restore the manifest
and all affected lockfiles to their original bytes and existence.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("package")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version):
        parser.error("version must be MAJOR.MINOR.PATCH")
    root = Path("Cargo.toml")
    text, replacements = re.subn(
        r'(\[workspace\.package\][\s\S]*?\nversion = ")[^"]+(")',
        lambda match: match[1] + args.version + match[2], root.read_text(), count=1,
    )
    if replacements != 1:
        parser.error("workspace.package.version was not found")
    manifests = [root, *sorted(Path("proof-workspaces").glob("*/Cargo.toml"))]
    affected = [root, *[manifest.with_name("Cargo.lock") for manifest in manifests]]
    # Capture local edits and absent locks before the first mutation. Git's
    # index is not a preimage: the operator may have unstaged changes here.
    preimages = {path: path.read_bytes() if path.exists() else None for path in affected}
    try:
        root.write_text(text)
        for manifest in manifests:
            # Each proof owns an independent resolution graph and Cargo.lock.
            subprocess.run(["cargo", "update", "--workspace", "--manifest-path", str(manifest)], check=True)
        for manifest in manifests:
            for features in [[], ["--all-features"]]:
                result = subprocess.run(
                    ["cargo", "metadata", "--format-version", "1", "--locked",
                     "--manifest-path", str(manifest), *features],
                    check=True, stdout=subprocess.PIPE, text=True,
                )
                if manifest == root:
                    packages = json.loads(result.stdout)["packages"]
                    version = next((p["version"] for p in packages if p["name"] == args.package), None)
                    if version != args.version:
                        parser.error(f"version bump did not take: expected {args.version}, got {version}")
    except BaseException as failure:
        rollback_errors = []
        for path, contents in preimages.items():
            try:
                if contents is None:
                    path.unlink(missing_ok=True)
                else:
                    path.write_bytes(contents)
            except OSError as error:
                rollback_errors.append(f"{path}: {error}")
        if rollback_errors:
            raise RuntimeError("release update failed; rollback also failed: "
                               + "; ".join(rollback_errors)) from failure
        raise
    subprocess.run(["git", "add", "--", str(root),
                    *[str(manifest.with_name("Cargo.lock")) for manifest in manifests]], check=True)


if __name__ == "__main__":
    main()
