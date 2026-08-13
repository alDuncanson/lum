-- Telescope picker for lum.
--
-- The finder is a custom async one rather than `new_job`. Telescope's job
-- finder spawns a process per keystroke, which is what made the old picker cost
-- 290 ms per character; this one writes a line to a socket that is already
-- open. Two things follow that the process-per-keystroke shape could not do:
--
-- - **The query does not wait for indexing.** It asks for what is indexed right
--   now and renders it, while progress for the rest arrives on the same
--   connection and is reported through `$/progress`. The old picker blocked on
--   the first full index, showing nothing, and Telescope restarted that wait on
--   every keystroke.
-- - **A superseded keystroke is cancelled**, not merely ignored. Debouncing is
--   a `vim.uv` timer that gets reset, so the request is never sent.

local M = {}

local client = require("lum.client")
local install = require("lum.install")
local notify = require("lum.notify")

local config = {
  -- A command name is looked up on PATH, then among binaries `:LumInstall`
  -- downloaded. A path with a slash is used as given. See lua/lum/install.lua.
  executable = "lum",
  limit = 50,
  debounce_ms = 80,
  -- Chunks any one file may contribute. 0 returns raw nearest neighbours.
  per_file = 2,
  exclude_tests = false,
  -- Report indexing activity through $/progress and vim.notify. Off by
  -- default: it holds a subscription open, and by extension keeps the daemon
  -- from idling out. See lua/lum/notify.lua for the options this accepts.
  notify = false,
  -- Register and index the current Git repository when Neovim opens, rather
  -- than when the picker is first opened.
  --
  -- Much less load-bearing than it used to be: the picker no longer blocks on
  -- a first index, so a cold repository shows results as they arrive instead
  -- of nothing at all. Still worth turning on if you use lum regularly, since
  -- it moves the embedding off the moment you first want to search.
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
  local path = uri
  if uri:match("^file://") then
    local ok, decoded = pcall(vim.uri_to_fname, uri)
    if not ok then
      return nil
    end
    path = decoded
  elseif uri:match("^%a[%w+.-]*://") then
    return nil
  end
  if not vim.startswith(path, "/") then
    path = vim.fs.joinpath(root, path)
  end
  return vim.fs.normalize(path)
end

local function make_entry(result, root)
  local path = result_path(result.uri, root)
  local lnum = tonumber(result.start_line)
  local score = tonumber(result.score)
  if not path or not lnum or not score or type(result.text) ~= "string" then
    return nil
  end
  lnum = math.max(1, math.floor(lnum))
  local snippet = result.text:gsub("%s+", " "):match("^%s*(.-)%s*$")
  local relative = result.path
  if not relative or relative == "" then
    relative = vim.fs.relpath(root, path) or path
  end
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

--- A Telescope finder that queries lum over the open socket.
---
--- Telescope calls this on every prompt change and expects results through
--- `process_result` and a terminating `process_complete`. Replies for a prompt
--- that has already been superseded are dropped: without that, a slow answer to
--- "ret" would repopulate the list after "retry backoff" had already answered.
local function make_finder(opts, root, executable)
  local generation = 0
  local timer = nil

  local function cancel_timer()
    if timer then
      timer:stop()
      if not timer:is_closing() then
        timer:close()
      end
      timer = nil
    end
  end

  return setmetatable({
    close = cancel_timer,
  }, {
    __call = function(_, prompt, process_result, process_complete)
      cancel_timer()
      if not prompt or prompt == "" then
        return process_complete()
      end
      generation = generation + 1
      local mine = generation

      timer = vim.uv.new_timer()
      timer:start(
        opts.debounce_ms,
        0,
        vim.schedule_wrap(function()
          cancel_timer()
          if mine ~= generation then
            return
          end
          client.request(executable, {
            op = "search",
            q = prompt,
            limit = opts.limit,
            root = root,
            per_file = opts.per_file,
            exclude_tests = opts.exclude_tests,
            -- Deliberately not waiting on a first index: show what exists now.
            wait = false,
          }, function(err, response)
            if mine ~= generation then
              return
            end
            if err then
              vim.schedule(function()
                vim.notify(err, vim.log.levels.ERROR)
                process_complete()
              end)
              return
            end
            vim.schedule(function()
              for _, result in ipairs((response or {}).results or {}) do
                local entry = make_entry(result, root)
                if entry then
                  process_result(entry)
                end
              end
              process_complete()
            end)
          end)
        end)
      )
    end,
  })
end

function M.setup(opts)
  config = vim.tbl_deep_extend("force", config, opts or {})
  local notify_opts = config.notify
  if notify_opts == true then
    notify_opts = { enabled = true }
  elseif type(notify_opts) == "table" then
    notify_opts = vim.tbl_extend("keep", notify_opts, { enabled = true })
  else
    notify_opts = { enabled = false }
  end
  notify.setup(notify_opts)
  install.command()

  if config.index_on_open then
    -- Wait for startup to finish: setup() runs during plugin loading, when the
    -- buffer that determines the repository root may not exist yet, and when
    -- adding work to the startup path is least welcome.
    if vim.v.vim_did_enter == 1 then
      vim.schedule(M.start_indexing)
    else
      vim.api.nvim_create_autocmd("VimEnter", { once = true, callback = M.start_indexing })
    end
  end

  vim.api.nvim_create_autocmd("VimLeavePre", {
    callback = function()
      client.close()
    end,
  })
end

--- Warm the current repository's index in the background.
---
--- Fire and forget by design: nothing waits on it, and failures are the
--- notification channel's problem rather than something to interrupt startup
--- over. An already-indexed repository makes this a scan of unchanged files,
--- which is cheap.
function M.start_indexing(opts)
  opts = vim.tbl_deep_extend("force", {}, config, opts or {})
  local root = workspace_root(opts)
  -- Only inside a repository. Indexing whatever directory Neovim happened to
  -- start in — $HOME, /tmp — is not what anyone means by this option.
  if not vim.uv.fs_stat(vim.fs.joinpath(root, ".git")) then
    return false
  end
  -- Silent when lum is not installed: this runs at startup, and a missing
  -- binary is worth reporting when someone asks for a search, not before.
  local executable = install.resolve(opts.executable)
  if not executable then
    return false
  end
  notify.start(executable)
  client.request(executable, { op = "add_source", uri = root, wait = false }, function(err)
    if err then
      vim.schedule(function()
        vim.notify("lum could not index this repository: " .. err, vim.log.levels.WARN)
      end)
    end
  end)
  return true
end

function M.lum(opts)
  opts = vim.tbl_deep_extend("force", {}, config, opts or {})
  local root = workspace_root(opts)
  local executable = install.resolve(opts.executable)
  if not executable then
    vim.notify(install.missing_message(opts.executable), vim.log.levels.ERROR)
    return
  end

  -- Subscribe on first use rather than at startup: this is the moment lum is
  -- about to be started anyway, and the first index — the slow, silent one that
  -- prompted all this — is about to run.
  notify.start(executable)
  opts.limit = math.min(100, math.max(1, math.floor(tonumber(opts.limit) or 50)))
  opts.debounce_ms = math.max(0, tonumber(opts.debounce_ms) or 80)

  local finders = require("telescope.finders")
  local pickers = require("telescope.pickers")
  local telescope_config = require("telescope.config").values
  local sorters = require("telescope.sorters")

  pickers
    .new(opts, {
      prompt_title = "Lum search",
      finder = make_finder(opts, root, executable),
      previewer = telescope_config.qflist_previewer(opts),
      -- Results arrive ranked by meaning. A fuzzy sorter would re-rank them by
      -- how the query looks as a substring, which is the thing lum exists to
      -- not do; highlighter_only keeps the order and still highlights matches.
      sorter = sorters.highlighter_only(opts),
    })
    :find()
end

return M
