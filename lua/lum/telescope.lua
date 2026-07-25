-- Telescope picker for the lum semantic-search CLI.
local M = {}

local notify = require("lum.notify")

local config = {
  executable = "lum",
  limit = 50,
  debounce_ms = 200,
  -- Report indexing activity through vim.notify. Off by default: it starts
  -- a background subscription, and by extension the lum daemon. See
  -- lua/lum/notify.lua for the options this accepts.
  notify = false,
  -- Register and index the current Git repository when Neovim opens, rather
  -- than when the picker is first opened.
  --
  -- The picker registers its repository through `lum search --root`, which
  -- blocks until the *first* index of that repository finishes. On a cold
  -- repository that is a model download plus a full embed, during which the
  -- picker shows nothing — and since Telescope respawns the job on every
  -- keystroke, typing actively restarts the wait. Starting at open moves
  -- that work off the critical path: an LSP attaches when you open a file
  -- for the same reason.
  --
  -- Off by default because it starts a background daemon in every Neovim
  -- session, including the ones where you never search. Worth turning on if
  -- you use lum regularly.
  index_on_open = false,
}

local function workspace_root(opts)
  if opts.root and opts.root ~= "" then
    return vim.fs.normalize(vim.fn.fnamemodify(opts.root, ":p"))
  end

  local buffer_name = vim.api.nvim_buf_get_name(0)
  if buffer_name ~= "" then
    local git_root = vim.fs.root(buffer_name, ".git")
    if git_root then
      return vim.fs.normalize(git_root)
    end
  end
  return vim.fs.normalize(vim.uv.cwd() or vim.fn.getcwd())
end

local function result_path(uri, root)
  if type(uri) ~= "string" or uri == "" then
    return nil
  end

  local path
  if uri:match("^file://") then
    local ok, decoded = pcall(vim.uri_to_fname, uri)
    if not ok then
      return nil
    end
    path = decoded
  elseif uri:match("^%a[%w+.-]*://") then
    return nil
  else
    path = uri
  end
  if not vim.startswith(path, "/") then
    path = vim.fs.joinpath(root, path)
  end
  return vim.fs.normalize(path)
end

local function make_entry(root)
  return function(line)
    local ok, result = pcall(vim.json.decode, line)
    if not ok or type(result) ~= "table" then
      return nil
    end

    local path = result_path(result.uri, root)
    local lnum = tonumber(result.start_line)
    local score = tonumber(result.score)
    if not path or not lnum or not score or type(result.text) ~= "string" then
      return nil
    end

    lnum = math.max(1, math.floor(lnum))
    local snippet = result.text:gsub("%s+", " "):match("^%s*(.-)%s*$")
    local relative = vim.fs.relpath(root, path) or path
    local display = string.format("%s:%d  %.4f  %s", relative, lnum, score, snippet)
    return {
      value = result,
      ordinal = display,
      display = display,
      path = path,
      filename = path,
      lnum = lnum,
      end_lnum = tonumber(result.end_line),
      score = score,
      snippet = snippet,
      text = snippet,
    }
  end
end

function M.setup(opts)
  config = vim.tbl_deep_extend("force", config, opts or {})
  -- `notify = true` is shorthand for enabling it with the defaults; a table
  -- configures it. Anything else (including the default false) leaves the
  -- subscription off.
  local notify_opts = config.notify
  if notify_opts == true then
    notify_opts = { enabled = true }
  elseif type(notify_opts) == "table" then
    notify_opts = vim.tbl_extend("keep", notify_opts, { enabled = true })
  else
    notify_opts = { enabled = false }
  end
  notify.setup(notify_opts)

  if config.index_on_open then
    -- Wait for startup to finish: setup() runs during plugin loading, when
    -- the buffer that determines the repository root may not exist yet, and
    -- when adding work to the startup path is least welcome.
    if vim.v.vim_did_enter == 1 then
      vim.schedule(M.start_indexing)
    else
      vim.api.nvim_create_autocmd("VimEnter", {
        once = true,
        callback = function()
          M.start_indexing()
        end,
      })
    end
  end
end

-- start_indexing warms the current repository's index in the background.
--
-- Fire and forget by design: nothing waits on it, and failures are the
-- notification channel's problem rather than something to interrupt startup
-- over. A repository that is already indexed makes this a canonical-path
-- lookup and a rescan of unchanged files, which is cheap.
function M.start_indexing(opts)
  opts = vim.tbl_deep_extend("force", {}, config, opts or {})
  local root = workspace_root(opts)
  -- Only inside a repository. Indexing whatever directory Neovim happened to
  -- start in — $HOME, /tmp — is not what anyone means by this option.
  if not vim.uv.fs_stat(vim.fs.joinpath(root, ".git")) then
    return false
  end

  notify.start(opts.executable)
  -- `add` rather than `search --root`: registration without a query, and it
  -- returns as soon as the scan is queued instead of blocking on it.
  vim.system({ opts.executable, "add", root }, { text = true }, function(result)
    if result.code ~= 0 then
      local detail = (result.stderr or ""):gsub("%s+$", "")
      vim.schedule(function()
        vim.notify("lum could not index this repository: " .. detail, vim.log.levels.WARN)
      end)
    end
  end)
  return true
end

function M.lum(opts)
  opts = vim.tbl_deep_extend("force", {}, config, opts or {})
  local root = workspace_root(opts)

  -- Subscribe on first use rather than at startup: this is the moment lum
  -- is about to be started anyway, and the first index — the slow, silent
  -- one that prompted all this — is about to run.
  notify.start(opts.executable)
  local debounce = math.max(0, tonumber(opts.debounce_ms) or 200) / 1000
  local limit = math.min(100, math.max(1, math.floor(tonumber(opts.limit) or 50)))

  local finders = require("telescope.finders")
  local pickers = require("telescope.pickers")
  local telescope_config = require("telescope.config").values
  local sorters = require("telescope.sorters")

  pickers.new(opts, {
    prompt_title = "Lum search",
    finder = finders.new_job(function(prompt)
      if not prompt or prompt == "" then
        return nil
      end
      -- Values after the script are positional parameters, never shell source.
      return {
        "sh",
        "-c",
        'sleep "$1"; shift; exec "$@"',
        "telescope-lum",
        string.format("%.3f", debounce),
        opts.executable,
        "search",
        "--root",
        root,
        "--jsonl",
        "--limit",
        tostring(limit),
        "--",
        prompt,
      }
    end, make_entry(root), nil, root),
    previewer = telescope_config.qflist_previewer(opts),
    sorter = sorters.highlighter_only(opts),
  }):find()
end

return M
