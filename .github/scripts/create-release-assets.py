"""Create a tagged source archive and TinyBus-compatible checksum manifest."""
import argparse
import hashlib
from pathlib import Path
import re
import subprocess
import sys

PACKAGE = "tinystoragedrivers"
TAG_PATTERN = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+\Z")


def git_output(repository, *arguments):
    return subprocess.run(["git", *arguments], cwd=repository, check=True,
                          stdout=subprocess.PIPE, text=True).stdout.strip()


def create_assets(repository, tag, output_dir):
    """Archive the exact release tag and write its SHA-256 allowlist."""
    if not TAG_PATTERN.fullmatch(tag):
        raise ValueError("release tag must be a vMAJOR.MINOR.PATCH tag")

    tagged_commit = git_output(repository, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
    checked_out_commit = git_output(repository, "rev-parse", "HEAD")
    if tagged_commit != checked_out_commit:
        raise ValueError(f"release tag {tag} does not point at HEAD")

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_name = f"{PACKAGE}-{tag}.tar.gz"
    archive_path = output_dir / archive_name
    try:
        with archive_path.open("wb") as archive:
            subprocess.run(
                ["git", "archive", "--format=tar.gz", f"--prefix={PACKAGE}-{tag}/", tag],
                cwd=repository,
                stdout=archive,
                check=True,
            )
        digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        checksum_path = output_dir / "checksum.toml"
        checksum_path.write_text(f'[sha256]\n"{archive_name}" = "{digest}"\n')
    except BaseException:
        archive_path.unlink(missing_ok=True)
        raise
    return archive_path, checksum_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--tag", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        archive_path, checksum_path = create_assets(args.repository.resolve(), args.tag,
                                                    args.output_dir.resolve())
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    print(f"created {archive_path}")
    print(f"created {checksum_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
