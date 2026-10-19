#!/usr/bin/env python3
"""Prove the schema's default/all-features normal and build dependency closure."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {
    "schema-contract-proof", "tinystoragedrivers-schema", "serde", "serde_core",
    "serde_derive", "serde_json", "thiserror", "thiserror-impl", "proc-macro2",
    "quote", "syn", "unicode-ident", "itoa", "memchr", "zmij", "ryu",
}


def check(manifest, all_features):
    """Resolve an isolated consumer and reject unexpected transitive packages."""
    command = ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)]
    if all_features:
        command.append("--all-features")
    data = json.loads(subprocess.check_output(command, cwd=ROOT, text=True))
    if all_features:
        # --all-features alone enables only the consumer's features. Explicitly
        # enable every schema feature too, including any introduced later.
        schema = next(p for p in data["packages"] if p["name"] == "tinystoragedrivers-schema")
        features = [f"tinystoragedrivers-schema/{name}" for name in schema["features"]]
        if features:
            command.extend(["--features", ",".join(features)])
            data = json.loads(subprocess.check_output(command, cwd=ROOT, text=True))
    packages = {p["id"]: p for p in data["packages"]}
    nodes = {node["id"]: node for node in data["resolve"]["nodes"]}
    queue = [data["resolve"]["root"]]
    visited = set()
    while queue:
        package_id = queue.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        package = packages[package_id]
        if package["name"] not in ALLOWED:
            raise ValueError(f"forbidden dependency: {package['name']}")
        if package["name"] == "tinystoragedrivers-schema":
            if any(target["kind"] == ["custom-build"] for target in package["targets"]):
                raise ValueError("schema crate must not have a build script")
            # Deny even an inactive optional dependency outside the direct contract.
            for dependency in package["dependencies"]:
                if dependency["kind"] != "dev" and dependency["name"] not in {"serde", "serde_json", "thiserror"}:
                    raise ValueError(f"forbidden schema dependency: {dependency['name']}")
        for dependency in nodes[package_id]["deps"]:
            if any(kind["kind"] != "dev" for kind in dependency["dep_kinds"]):
                queue.append(dependency["pkg"])
    return sorted(packages[p]["name"] for p in visited)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--negative-test", action="store_true")
    args = parser.parse_args()
    manifest = ROOT / "proof-workspaces/schema-contract/Cargo.toml"
    for all_features in [False, True]:
        closure = check(manifest, all_features)
        print(f"{'all-features' if all_features else 'default'}: {', '.join(closure)}")
    if args.negative_test:
        # Keep proof output under this checkout, never a shared or temporary target.
        negative = ROOT / "target/schema-contract-negative"
        (negative / "src").mkdir(parents=True, exist_ok=True)
        text = manifest.read_text().replace("../../crates/tinystoragedrivers-schema", str(ROOT / "crates/tinystoragedrivers-schema"))
        (negative / "Cargo.toml").write_text(text + '\ntokio = { version = "1", default-features = false }\n')
        (negative / "src/lib.rs").write_text("//! Intentionally invalid dependency fixture.\n")
        for all_features in [False, True]:
            try:
                check(negative / "Cargo.toml", all_features)
            except ValueError as error:
                if "tokio" not in str(error):
                    raise
                print(f"negative fixture rejected: {error}")
            else:
                raise ValueError("negative fixture unexpectedly accepted tokio")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
