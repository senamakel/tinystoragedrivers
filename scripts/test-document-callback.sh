#!/usr/bin/env bash
# Actual SDK cdylib, hashed and admitted before confidential CRUD dispatch.
set -euo pipefail
cd -- "$(dirname -- "$0")/.."
proof_manifest="$PWD/proof-workspaces/document-callback/Cargo.toml"
target_args=()
proof_target=""
if [[ "${1:-}" == "--target-dir" && -n "${2:-}" && -z "${3:-}" ]]; then
    target_args=(--target-dir "$2")
    proof_target="$PWD/$2"
elif [[ $# -ne 0 ]]; then
    echo "usage: $0 [--target-dir PATH]" >&2
    exit 2
fi
cargo build "${target_args[@]}" --locked --manifest-path "$proof_manifest" -p document-callback-module-proof
if [[ -z "$proof_target" ]]; then
    proof_target="$(cargo metadata --manifest-path "$proof_manifest" --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
fi
case "$(uname -s)" in
    Darwin) proof_library="libdocument_callback_module_proof.dylib" ;;
    MINGW*|MSYS*|CYGWIN*) proof_library="document_callback_module_proof.dll" ;;
    *) proof_library="libdocument_callback_module_proof.so" ;;
esac
proof_artifact="$proof_target/debug/$proof_library"
test -f "$proof_artifact"
TSD_DOCUMENT_MODULE="$proof_artifact" cargo test "${target_args[@]}" --locked --manifest-path "$proof_manifest" -p document-callback-proof
