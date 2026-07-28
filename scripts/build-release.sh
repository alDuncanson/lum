#!/usr/bin/env bash
# Build the release artifacts for the host platform.
#
#   scripts/build-release.sh 0.1.0
#
# This is what .github/workflows/release.yml runs, so the shipped build path
# can be exercised on a laptop instead of only on a tag. That gap is how three
# of four targets shipped broken: the release path was the one path nobody
# could run.
#
# Deliberately does NOT use Nix, even though the flake is the source of truth
# for development and CI. `nix build .#lum-worker` links libonnxruntime from
# /nix/store, so the result only starts on a machine that has that exact
# derivation. Release binaries link a statically-vendored ONNX Runtime and
# nothing outside the system libraries.
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

echo "==> building lum-worker"
(cd worker && cargo build --release --locked)

echo "==> building lum $version"
(cd dispatcher && go build -trimpath \
  -ldflags "-s -w -X github.com/alDuncanson/lum/dispatcher/internal/version.Value=$version" \
  -o lum ./cmd/lum)

# A binary that links something only the build machine has starts fine here
# and nowhere else, so this is checked rather than assumed.
echo "==> checking both binaries are self-contained"
for binary in dispatcher/lum worker/target/release/lum-worker; do
  if [ "$os" = darwin ]; then
    if otool -L "$binary" | tail -n +2 | grep -vE '/usr/lib/|/System/Library/'; then
      echo "$binary links outside the system libraries" >&2; exit 1
    fi
  else
    if ldd "$binary" | grep -qiE 'libssl|libcrypto|onnxruntime'; then
      echo "$binary needs a shared library a user may not have" >&2; exit 1
    fi
  fi
done

echo "==> smoke testing"
stage=$(mktemp -d); trap 'rm -rf "$stage"' EXIT
# Side by side, because that is how `lum` finds `lum-worker` — the property
# the install instructions depend on.
cp dispatcher/lum worker/target/release/lum-worker "$stage/"
test "$("$stage/lum" version)" = "$version"

data=$(mktemp -d)
mkdir -p "$data/repo"
printf 'package main\n\n// retryBackoff sleeps longer after each failure.\nfunc retryBackoff() {}\n' \
  > "$data/repo/main.go"
export LUM_DATA_DIR="$data/lum" LUM_HTTP_ADDR="127.0.0.1:7431"
"$stage/lum" serve >"$data/serve.log" 2>&1 &
serve=$!
cleanup() { "$stage/lum" stop >/dev/null 2>&1 || true; kill "$serve" 2>/dev/null || true; rm -rf "$stage" "$data"; }
trap cleanup EXIT

for _ in $(seq 1 60); do "$stage/lum" status >/dev/null 2>&1 && break; sleep 1; done
"$stage/lum" status >/dev/null || { echo "daemon never answered; see $data/serve.log" >&2; tail -20 "$data/serve.log" >&2; exit 1; }

# The real test: index and search with these binaries. Downloads the embedding
# model, so it exercises TLS, the worker, the whole pipeline — not just that
# the process starts.
"$stage/lum" add "$data/repo" >/dev/null
indexed=0
for _ in $(seq 1 300); do
  indexed=$("$stage/lum" status | awk '/^documents:/{print $2}')
  [ "${indexed:-0}" -ge 1 ] && break
  sleep 2
done
[ "${indexed:-0}" -ge 1 ] || { echo "nothing was indexed; see $data/serve.log" >&2; tail -30 "$data/serve.log" >&2; exit 1; }
"$stage/lum" search --root "$data/repo" --json "retry backoff" | grep -q retryBackoff \
  || { echo "search did not find the indexed symbol" >&2; exit 1; }
echo "    indexed and searched with the built binaries"

echo "==> packaging"
name="lum-$version-$target"
rm -rf dist; mkdir -p "dist/$name"
cp dispatcher/lum worker/target/release/lum-worker README.md LICENSE "dist/$name/"
tar -C dist -czf "dist/$name.tar.gz" "$name"
if command -v sha256sum >/dev/null; then
  (cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
else
  (cd dist && shasum -a 256 "$name.tar.gz" > "$name.tar.gz.sha256")
fi
cat "dist/$name.tar.gz.sha256"
echo "==> dist/$name.tar.gz"
