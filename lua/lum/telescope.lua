-- Telescope picker for the lum semantic-search CLI.
local M = {}

local config = {
  executable = "lum",
  limit = 50,
  debounce_ms = 200,
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
end

function M.lum(opts)
  opts = vim.tbl_deep_extend("force", {}, config, opts or {})
  local root = workspace_root(opts)
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
