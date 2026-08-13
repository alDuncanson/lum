#!/usr/bin/env bash
# Build the release artifact for the host platform.
#
#   scripts/build-release.sh 0.2.0
#
# This is what .github/workflows/release.yml runs, so the shipped build path
# can be exercised on a laptop instead of only on a tag. That gap is how three
# of four targets shipped broken: the release path was the one path nobody
# could run.
#
# Deliberately does NOT use Nix, even though the flake is the source of truth
# for development and CI. `nix build` links libonnxruntime from /nix/store, so
# the result only starts on a machine that has that exact derivation. Release
# binaries link a statically-vendored ONNX Runtime and nothing outside the
# system libraries.
set -euo pipefail

version="${1:?usage: build-release.sh <version>}"
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

case "$(uname -s)" in
  Darwin) os=darwin ;;
  Linux)  os=linux ;;
  *) echo "unsupported OS $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  arm64|aarch64) arch=arm64 ;;
  x86_64)        arch=x86_64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
target="$os-$arch"

echo "==> building lum $version"
# Unset so a developer's dynamic-linking setup cannot leak into a release; the
# whole point here is the vendored, statically linked runtime.
unset ORT_LIB_LOCATION ORT_PREFER_DYNAMIC_LINK
cargo build --release --locked
binary="target/release/lum"

# A binary that links something only the build machine has starts fine here and
# nowhere else, so this is checked rather than assumed.
echo "==> checking the binary is self-contained"
if [ "$os" = darwin ]; then
  if otool -L "$binary" | tail -n +2 | grep -vE '/usr/lib/|/System/Library/'; then
    echo "$binary links outside the system libraries" >&2; exit 1
  fi
else
  if ldd "$binary" | grep -qiE 'libssl|libcrypto|onnxruntime'; then
    echo "$binary needs a shared library a user may not have" >&2; exit 1
  fi
fi

echo "==> smoke testing"
stage=$(mktemp -d)
cp "$binary" "$stage/"
test "$("$stage/lum" --version)" = "lum $version"

data=$(mktemp -d)
mkdir -p "$data/repo"
printf 'package main\n\n// retryBackoff sleeps longer after each failure.\nfunc retryBackoff() {}\n' \
  > "$data/repo/main.go"
# A short data directory: the socket lives in it, and Unix socket addresses are
# length-limited. mktemp on macOS produces paths long enough to matter.
export LUM_DATA_DIR="/tmp/lum-release-$$"
cleanup() {
  "$stage/lum" stop >/dev/null 2>&1 || true
  rm -rf "$stage" "$data" "$LUM_DATA_DIR"
}
trap cleanup EXIT

# The real test: index and search with this binary. It downloads the embedding
# model, so it exercises TLS, inference, and storage — not just that the process
# starts. No explicit `serve`: the on-demand spawn is the path users take.
"$stage/lum" add "$data/repo" --wait >/dev/null
"$stage/lum" search --root "$data/repo" --json "retry backoff" | grep -q retryBackoff \
  || { echo "search did not find the indexed symbol" >&2; tail -30 "$LUM_DATA_DIR/daemon.log" >&2; exit 1; }
echo "    indexed and searched with the built binary"

echo "==> packaging"
name="lum-$version-$target"
rm -rf dist; mkdir -p "dist/$name"
cp "$binary" README.md LICENSE "dist/$name/"
tar -C dist -czf "dist/$name.tar.gz" "$name"
if command -v sha256sum >/dev/null; then
  (cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
else
  (cd dist && shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
fi
cat "dist/$name.tar.gz.sha256"
echo "==> dist/$name.tar.gz"
