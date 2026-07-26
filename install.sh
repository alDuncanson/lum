#!/bin/sh
# Install lum.
#
#   curl -fsSL https://raw.githubusercontent.com/alDuncanson/lum/main/install.sh | sh
#
# Downloads the release for this platform, checks it against the published
# SHA256SUMS, and puts both binaries in one directory. They have to stay
# together: `lum` looks for `lum-worker` beside itself.
#
# Environment:
#   LUM_INSTALL_DIR   where to put the binaries (default ~/.local/bin)
#   LUM_VERSION       a specific version, e.g. 0.2.0 (default: latest release)
#   LUM_RELEASE_BASE_URL  an internal mirror serving the same file names
#
# POSIX sh on purpose: this runs before lum exists, on whatever the machine
# has. No bashisms, no jq, no python.

set -eu

REPO="alDuncanson/lum"
INSTALL_DIR="${LUM_INSTALL_DIR:-$HOME/.local/bin}"

say() { printf '%s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required"; }

# --- what are we running on ---

os=$(uname -s)
case "$os" in
  Darwin) os=darwin ;;
  Linux)  os=linux ;;
  *) die "no lum build for $os. lum needs a Unix domain socket, so Windows is not supported; WSL works." ;;
esac

arch=$(uname -m)
case "$arch" in
  arm64|aarch64) arch=arm64 ;;
  x86_64|amd64)  arch=x86_64 ;;
  *) die "no lum build for $arch" ;;
esac
target="$os-$arch"

need uname
need tar
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
  fetch_stdout() { curl -fsSL "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
  fetch_stdout() { wget -qO- "$1"; }
else
  die "curl or wget is required"
fi

# --- which version ---

version="${LUM_VERSION:-}"
if [ -z "$version" ]; then
  # The redirect target of /releases/latest ends in the tag. Cheaper and less
  # brittle than parsing the API's JSON without a JSON parser.
  if command -v curl >/dev/null 2>&1; then
    tag=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
      "https://github.com/$REPO/releases/latest" 2>/dev/null | sed 's:.*/::')
  else
    tag=$(wget -qS --max-redirect=10 -O /dev/null \
      "https://github.com/$REPO/releases/latest" 2>&1 |
      sed -n 's/.*Location:.*\/tag\/\(v[^ ]*\).*/\1/p' | tail -1)
  fi
  [ -n "${tag:-}" ] || die "could not determine the latest release; set LUM_VERSION"
  version="${tag#v}"
fi
base="${LUM_RELEASE_BASE_URL:-https://github.com/$REPO/releases/download/v$version}"
archive="lum-$version-$target.tar.gz"

# --- download and verify ---

tmp=$(mktemp -d "${TMPDIR:-/tmp}/lum-install.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

say "downloading lum $version for $target"
fetch "$base/$archive" "$tmp/$archive" || die "could not download $base/$archive"

# The checksum is the only thing between a redirected or truncated download
# and an executable this script is about to put on your PATH.
if fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" 2>/dev/null; then
  expected=$(awk -v want="$archive" '$2 == want || $2 == "*"want { print $1 }' "$tmp/SHA256SUMS")
  [ -n "$expected" ] || die "SHA256SUMS has no entry for $archive"
  if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/$archive" | cut -d' ' -f1)
  elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$tmp/$archive" | cut -d' ' -f1)
  else
    die "sha256sum or shasum is required to verify the download"
  fi
  [ "$actual" = "$expected" ] || die "checksum mismatch for $archive (expected $expected, got $actual)"
  say "checksum ok"
else
  die "could not fetch SHA256SUMS; refusing to install an unverified binary"
fi

# --- install ---

tar -xzf "$tmp/$archive" -C "$tmp"
unpacked="$tmp/lum-$version-$target"
[ -x "$unpacked/lum" ] || [ -f "$unpacked/lum" ] || die "$archive did not contain lum"

mkdir -p "$INSTALL_DIR"
# Both, into the same directory: lum finds lum-worker as a sibling.
for binary in lum lum-worker; do
  cp "$unpacked/$binary" "$INSTALL_DIR/$binary.tmp"
  chmod 755 "$INSTALL_DIR/$binary.tmp"
  # Replacing a running binary in place fails on some systems; a rename does
  # not, and is atomic.
  mv -f "$INSTALL_DIR/$binary.tmp" "$INSTALL_DIR/$binary"
done

say "installed lum $version to $INSTALL_DIR"

case ":$PATH:" in
  *":$INSTALL_DIR:"*)
    say ""
    say "try:  lum search --root . \"where is retry backoff handled\""
    ;;
  *)
    say ""
    say "$INSTALL_DIR is not on your PATH. Add it:"
    say ""
    say "    export PATH=\"\$PATH:$INSTALL_DIR\""
    ;;
esac
