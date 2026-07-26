-- One persistent line showing what lum is doing right now.
--
-- Deliberately not vim.notify. That is built for discrete messages, and
-- notifiers disagree about whether a message can be updated in place at all:
-- asking them to nicely produced either three hundred stacked copies of a
-- progress bar or, once that was detected and stopped, no progress at all.
-- Progress is a status *display*, which is why fidget exists separately from
-- nvim-notify. Owning ~100 lines of window is the price of it working the
-- same way everywhere.
--
-- One line, bottom right by default, above Telescope, never focusable, gone
-- when there is nothing to say. Discrete events — a crash, a failure, a
-- summary worth keeping — still go through vim.notify, where they belong.
--
-- The corner is configurable because bottom right is contested: fidget and
-- noice put LSP progress there, so lum's line and rust-analyzer's end up
-- stacked on the same cells. No zindex resolves that — whichever wins hides
-- the other — so the fix is to move one of them.

local M = {}

local SPINNER = { "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" }
local SPIN_MS = 120
local BAR_WIDTH = 14

local window = {
  -- Which corner: "SE", "SW", "NE", "NW".
  anchor = "SE",
  -- Cells to move in from that corner. `row_offset = 1` in the default
  -- corner lifts the line clear of a notifier already sitting there.
  row_offset = 0,
  col_offset = 0,
  -- Above Telescope's windows (50), so the line stays readable while a
  -- picker is open — which is exactly when it matters.
  zindex = 100,
  border = "rounded",
  winblend = 10,
}

function M.setup(opts)
  if type(opts) == "table" then
    window = vim.tbl_extend("force", window, opts)
  end
end

local state = {
  win = nil,
  buf = nil,
  text = nil,
  frame = 1,
  spinner = true,
  timer = nil,
  hide_timer = nil,
}

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
  local south = window.anchor:sub(1, 1) ~= "N"
  local row = south and math.max(1, vim.o.lines - vim.o.cmdheight - 1 - window.row_offset)
    or math.min(vim.o.lines - 2, window.row_offset)
  local east = window.anchor:sub(2, 2) ~= "W"
  local col = east and math.max(0, vim.o.columns - window.col_offset) or window.col_offset

  local config = {
    relative = "editor",
    anchor = window.anchor,
    row = row,
    col = col,
    width = width,
    height = 1,
    style = "minimal",
    border = window.border,
    focusable = false,
    noautocmd = true,
    zindex = window.zindex,
  }
  if state.win and vim.api.nvim_win_is_valid(state.win) then
    vim.api.nvim_win_set_config(state.win, config)
  else
    local ok, win = pcall(vim.api.nvim_open_win, state.buf, false, config)
    if not ok then
      return
    end
    state.win = win
    vim.wo[state.win].winblend = window.winblend
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

-- bar renders a fixed-width meter. Kept here rather than in the caller so
-- every phase draws the same shape regardless of what it counts.
function M.bar(done, total)
  if not total or total <= 0 then
    return nil
  end
  local ratio = math.min(1, (done or 0) / total)
  local filled = math.floor(ratio * BAR_WIDTH + 0.5)
  return ("▕%s▏"):format(string.rep("█", filled) .. string.rep("░", BAR_WIDTH - filled))
end

-- show updates the line, creating the window if needed. Idempotent: calling
-- it with unchanged text costs a redraw and nothing else.
function M.show(text)
  cancel_hide()
  state.text = text
  state.spinner = true
  draw()
  start_timer()
end

-- finish leaves a final message up briefly, without a spinner, then clears.
-- The pause is the point: "68 indexed in 40.6s" is the confirmation that the
-- thing you were waiting for is done, and it is useless if it vanishes in
-- the same frame the bar does.
function M.finish(text, linger_ms)
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

function M.hide()
  stop_timer()
  cancel_hide()
  state.text = nil
  close()
end

function M.is_visible()
  return state.win ~= nil and vim.api.nvim_win_is_valid(state.win)
end

return M
