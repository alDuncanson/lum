-- Find lum's binary, or fetch it.
--
-- lum is two native binaries, so it cannot ship inside a Neovim plugin the way
-- a pure-Lua one can. Nix users already have it on PATH and this module does
-- nothing for them. Everyone else would otherwise have to install a Rust
-- toolchain and wait out an ONNX Runtime build, which is a lot to ask before
-- you have seen whether you like the search.
--
-- So: `:LumInstall` downloads the release archive for this platform into
-- Neovim's data directory and verifies its checksum. Not automatic — an editor
-- plugin that quietly fetches 90 MB of executable on startup is not a thing
-- lum should do — but one command, and the picker says so by name when the
-- binary is missing.
--
-- The pinned version is deliberate. A plugin and a binary that shipped
-- together are a tested pair; resolving "latest" at runtime would silently mix
-- versions that were never tried together. `nix flake check` fails if this
-- disagrees with flake.nix.

local M = {}

--- Release this plugin expects. Bump with flake.nix; the `plugin-version`
--- flake check keeps them honest.
M.version = "0.1.0"

local REPO = "alDuncanson/lum"

--- Where release archives are fetched from. Overridable for an internal
--- mirror, and for the test that exercises this whole path against a local
--- server rather than against the real internet.
local function base_url()
  local override = vim.env.LUM_RELEASE_BASE_URL
  if override and override ~= "" then
    return (override:gsub("/$", ""))
  end
  return ("https://github.com/%s/releases/download/v%s"):format(REPO, M.version)
end

--- Where a downloaded release lives. Not added to PATH — callers get an
--- absolute path, so nothing outside Neovim is affected.
function M.root()
  return vim.fs.joinpath(vim.fn.stdpath("data"), "lum")
end

local function binary_dir()
  return vim.fs.joinpath(M.root(), M.version)
end

--- The release asset for this platform, or nil and a reason.
---
--- Mirrors the target names in .github/workflows/release.yml.
function M.target()
  local uname = vim.uv.os_uname()
  local os_name = ({ Darwin = "darwin", Linux = "linux" })[uname.sysname]
  if not os_name then
    -- Windows is not supported at all: the dispatcher talks to the worker over
    -- a Unix domain socket.
    return nil, ("lum has no build for %s"):format(uname.sysname)
  end
  local arch = ({ arm64 = "arm64", aarch64 = "arm64", x86_64 = "x86_64", amd64 = "x86_64" })[uname.machine]
  if not arch then
    return nil, ("lum has no build for %s %s"):format(uname.sysname, uname.machine)
  end
  return ("%s-%s"):format(os_name, arch)
end

local function executable(path)
  return vim.uv.fs_stat(path) ~= nil
end

--- The lum executable to use, or nil.
---
--- Order matters: an explicitly configured path wins, then PATH — so a Nix or
--- Homebrew install is preferred over anything downloaded — then a previous
--- `:LumInstall`.
function M.resolve(configured)
  configured = configured or "lum"
  -- An absolute or relative path, rather than a bare command name.
  if configured:find("/") then
    return executable(configured) and configured or nil
  end
  if vim.fn.executable(configured) == 1 then
    return configured
  end
  local downloaded = vim.fs.joinpath(binary_dir(), "lum")
  return executable(downloaded) and downloaded or nil
end

--- Whether a downloaded copy exists for the pinned version.
function M.installed()
  return executable(vim.fs.joinpath(binary_dir(), "lum"))
end

local function run(cmd, cwd)
  local result = vim.system(cmd, { cwd = cwd, text = true }):wait()
  if result.code ~= 0 then
    local detail = (result.stderr or ""):gsub("%s+$", "")
    if detail == "" then
      detail = ("exit %d"):format(result.code)
    end
    return nil, ("%s: %s"):format(cmd[1], detail)
  end
  return (result.stdout or "")
end

local function fetch(url, into)
  if vim.fn.executable("curl") == 1 then
    return run({ "curl", "--fail", "--location", "--silent", "--show-error", "--output", into, url })
  end
  if vim.fn.executable("wget") == 1 then
    return run({ "wget", "--quiet", "--output-document", into, url })
  end
  return nil, "neither curl nor wget is available to download with"
end

--- Hex sha256 of a file.
---
--- Prefers the system tool because the alternative reads ninety megabytes into
--- a Lua string; vim.fn.sha256 is the fallback for a machine that has neither.
local function sha256(path)
  for _, cmd in ipairs({ { "sha256sum", path }, { "shasum", "-a", "256", path } }) do
    if vim.fn.executable(cmd[1]) == 1 then
      local out, err = run(cmd)
      if not out then
        return nil, err
      end
      return out:match("^(%x+)")
    end
  end
  local handle = io.open(path, "rb")
  if not handle then
    return nil, "cannot read " .. path
  end
  local contents = handle:read("*a")
  handle:close()
  return vim.fn.sha256(contents)
end

--- Download, verify, and unpack the release for this platform.
---
--- Synchronous on purpose. It is an explicit command whose entire content is
--- "wait for a download", and reporting progress from a backgrounded curl is
--- more machinery than the one-off deserves.
function M.install(opts)
  opts = opts or {}
  local target, why = M.target()
  if not target then
    return nil, why
  end
  if M.installed() and not opts.force then
    return vim.fs.joinpath(binary_dir(), "lum")
  end

  local archive = ("lum-%s-%s.tar.gz"):format(M.version, target)
  local base = base_url()
  local scratch = vim.fn.tempname()
  vim.fn.mkdir(scratch, "p")

  local tarball = vim.fs.joinpath(scratch, archive)
  vim.notify(("lum: downloading %s"):format(archive), vim.log.levels.INFO)
  local ok, err = fetch(("%s/%s"):format(base, archive), tarball)
  if not ok then
    return nil, err
  end

  -- Verify before unpacking, not after: the checksum is the only thing
  -- standing between a redirected download and an executable that runs.
  local sums = vim.fs.joinpath(scratch, "SHA256SUMS")
  ok, err = fetch(("%s/SHA256SUMS"):format(base), sums)
  if not ok then
    return nil, ("could not fetch SHA256SUMS to verify the download: %s"):format(err)
  end
  local expected
  for line in io.lines(sums) do
    local digest, name = line:match("^(%x+)%s+%*?(.+)$")
    if name == archive then
      expected = digest
    end
  end
  if not expected then
    return nil, ("SHA256SUMS lists no entry for %s"):format(archive)
  end
  local actual
  actual, err = sha256(tarball)
  if not actual then
    return nil, err
  end
  if actual:lower() ~= expected:lower() then
    return nil, ("checksum mismatch for %s: expected %s, got %s"):format(archive, expected, actual)
  end

  local dir = binary_dir()
  vim.fn.mkdir(dir, "p")
  -- --strip-components drops the versioned directory inside the archive, so
  -- the binaries land side by side, which is how the dispatcher finds its
  -- worker.
  ok, err = run({ "tar", "-xzf", tarball, "-C", dir, "--strip-components", "1" })
  if not ok then
    return nil, err
  end

  local lum = vim.fs.joinpath(dir, "lum")
  if not executable(lum) then
    return nil, ("%s did not contain lum"):format(archive)
  end
  for _, name in ipairs({ "lum", "lum-worker" }) do
    vim.uv.fs_chmod(vim.fs.joinpath(dir, name), 493) -- 0755
  end
  vim.fn.delete(scratch, "rf")
  return lum
end

--- What to tell someone whose binary is missing. Names the command rather
--- than describing the problem.
function M.missing_message(configured)
  local target = M.target()
  if not target then
    local _, why = M.target()
    return why .. " — see the README for building from source"
  end
  return ("lum: %q not found. Run :LumInstall to download it, or install it with Nix or from source.")
    :format(configured or "lum")
end

function M.command()
  vim.api.nvim_create_user_command("LumInstall", function(args)
    local path, err = M.install({ force = args.bang })
    if not path then
      vim.notify(("lum: install failed — %s"):format(err), vim.log.levels.ERROR)
      return
    end
    vim.notify(("lum %s installed at %s"):format(M.version, path), vim.log.levels.INFO)
  end, {
    bang = true,
    desc = "Download lum's binaries for this platform (! re-downloads)",
  })
end

return M
