-- Where lum's progress goes.
--
-- Two sinks, and the first one is strongly preferred:
--
-- - **lsp** — emit LSP `$/progress` (see lum/lsp.lua). Whatever renders
--   rust-analyzer's progress renders lum's, in the same place and style, and
--   the two stack instead of covering each other.
-- - **window** — one line lum draws itself, bottom right. Only for sessions
--   with nothing that displays LSP progress, where the alternative is no
--   progress at all.
--
-- `mode = "auto"` picks between them by asking whether anything is listening
-- for `LspProgress`. That is a real signal rather than a guess: noice,
-- fidget and snacks all register the autocmd, and core Neovim displays
-- nothing on its own.
--
-- Owning a window was the wrong default. It cannot help colliding — a float
-- in the bottom-right corner lands on whatever your notifier already put
-- there, and no zindex resolves two things wanting the same cells.

local M = {}

local lsp = require("lum.lsp")

local SPINNER = { "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" }
local SPIN_MS = 120
local BAR_WIDTH = 14

local config = {
  -- "auto" | "lsp" | "window"
  mode = "auto",
  -- Window sink only. Which corner, and how far in from it.
  anchor = "SE",
  row_offset = 0,
  col_offset = 0,
  zindex = 100,
  border = "rounded",
  winblend = 10,
}

local state = {
  win = nil,
  buf = nil,
  text = nil,
  frame = 1,
  spinner = true,
  timer = nil,
  hide_timer = nil,
  -- Window sink: what each token is currently saying, and which spoke last.
  lines = {},
  current = nil,
}

function M.setup(opts)
  if type(opts) == "table" then
    config = vim.tbl_extend("force", config, opts)
  end
end

--- Which sink is in use right now. Re-asked on each activity rather than
--- cached: plugins load lazily, and a session can gain a consumer between
--- one index and the next.
local function use_lsp()
  if config.mode == "lsp" then
    return true
  end
  if config.mode == "window" then
    return false
  end
  return lsp.has_consumer()
end

-- ---- the window sink ----

local function close()
  if state.win and vim.api.nvim_win_is_valid(state.win) then
    vim.api.nvim_win_close(state.win, true)
  end
  state.win = nil
end

local function stop_timer()
  if state.timer then
    state.timer:stop()
    if not state.timer:is_closing() then
      state.timer:close()
    end
    state.timer = nil
  end
end

local function cancel_hide()
  if state.hide_timer then
    state.hide_timer:stop()
    if not state.hide_timer:is_closing() then
      state.hide_timer:close()
    end
    state.hide_timer = nil
  end
end

local function draw()
  if not state.text then
    return close()
  end
  local prefix = state.spinner and (SPINNER[state.frame] .. " ") or "  "
  local line = " " .. prefix .. "lum  " .. state.text .. " "
  local width = math.min(vim.fn.strdisplaywidth(line), math.max(20, vim.o.columns - 4))

  if not (state.buf and vim.api.nvim_buf_is_valid(state.buf)) then
    state.buf = vim.api.nvim_create_buf(false, true)
  end
  vim.bo[state.buf].modifiable = true
  vim.api.nvim_buf_set_lines(state.buf, 0, -1, false, { line })
  vim.bo[state.buf].modifiable = false

  -- Clear of the statusline and the cmdline at the bottom, of the tabline
  -- at the top.
  local south = config.anchor:sub(1, 1) ~= "N"
  local row = south and math.max(1, vim.o.lines - vim.o.cmdheight - 1 - config.row_offset)
    or math.min(vim.o.lines - 2, config.row_offset)
  local east = config.anchor:sub(2, 2) ~= "W"
  local col = east and math.max(0, vim.o.columns - config.col_offset) or config.col_offset

  local window = {
    relative = "editor",
    anchor = config.anchor,
    row = row,
    col = col,
    width = width,
    height = 1,
    style = "minimal",
    border = config.border,
    focusable = false,
    noautocmd = true,
    zindex = config.zindex,
  }
  if state.win and vim.api.nvim_win_is_valid(state.win) then
    vim.api.nvim_win_set_config(state.win, window)
  else
    local ok, win = pcall(vim.api.nvim_open_win, state.buf, false, window)
    if not ok then
      return
    end
    state.win = win
    vim.wo[state.win].winblend = config.winblend
    vim.wo[state.win].winhighlight = "NormalFloat:NormalFloat,FloatBorder:Comment"
  end
end

local function start_timer()
  if state.timer or not state.spinner then
    return
  end
  state.timer = vim.uv.new_timer()
  state.timer:start(
    SPIN_MS,
    SPIN_MS,
    vim.schedule_wrap(function()
      if not state.text then
        stop_timer()
        return
      end
      state.frame = (state.frame % #SPINNER) + 1
      draw()
    end)
  )
end

--- A fixed-width meter. The window sink draws its own because it has to;
--- LSP consumers draw theirs from the percentage.
function M.bar(percentage)
  if not percentage then
    return nil
  end
  local filled = math.floor(math.min(1, percentage / 100) * BAR_WIDTH + 0.5)
  return ("▕%s▏"):format(string.rep("█", filled) .. string.rep("░", BAR_WIDTH - filled))
end

--- What the window shows for one token: the bar, if there is one, then
--- whatever is actually happening.
function M.line(opts)
  local bar = M.bar(opts.percentage)
  local text = opts.message or opts.title
  if bar then
    return bar .. " " .. text
  end
  return text
end

local function window_show(text)
  cancel_hide()
  state.text = text
  state.spinner = true
  draw()
  start_timer()
end

--- Leave a final message up briefly, without a spinner, then clear. The pause
--- is the point: "69 indexed in 40.6s" is the confirmation that the thing you
--- were waiting for is done, and it is useless if it vanishes with the bar.
local function window_finish(text, linger_ms)
  stop_timer()
  cancel_hide()
  state.spinner = false
  state.text = text
  draw()
  state.hide_timer = vim.uv.new_timer()
  state.hide_timer:start(
    linger_ms or 4000,
    0,
    vim.schedule_wrap(function()
      cancel_hide()
      M.hide()
    end)
  )
end

-- ---- the shared interface ----

--- Begin or advance `key`. `title` names the operation and is fixed for its
--- lifetime; `message` and `percentage` are what move.
function M.report(key, opts)
  if use_lsp() and lsp.report(key, opts) then
    -- The window may have been drawing before a consumer appeared.
    if state.lines[key] then
      state.lines[key] = nil
      M.hide()
    end
    return
  end
  state.lines[key] = M.line(opts)
  state.current = key
  window_show(state.lines[key])
end

--- End `key`, showing `message` for a moment first.
function M.finish(key, message, linger_ms)
  if lsp.finish(key, message) then
    return
  end
  state.lines[key] = nil
  -- Another token may still be working; it takes the line back rather than
  -- the summary hiding work that is still going on.
  local other, text = next(state.lines)
  if other then
    state.current = other
    window_show(text)
  elseif message then
    window_finish(message, linger_ms)
  else
    M.hide()
  end
end

--- Drop everything, immediately.
function M.hide()
  stop_timer()
  cancel_hide()
  state.text = nil
  state.lines = {}
  state.current = nil
  close()
end

function M.stop()
  M.hide()
  lsp.stop()
end

function M.is_visible()
  return state.win ~= nil and vim.api.nvim_win_is_valid(state.win)
end

return M
