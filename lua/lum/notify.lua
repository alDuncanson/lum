-- Report what lum is doing, so you never have to wonder whether something is
-- running, stuck, or dead.
--
-- Two channels, because these are two different kinds of thing.
--
-- Progress goes out as LSP `$/progress`, the same way rust-analyzer reports
-- indexing (see lum/progress.lua and lum/lsp.lua). Whatever renders a language
-- server's progress renders lum's, in the same corner and the same style, and
-- they stack rather than covering each other.
--
-- Discrete events go through `vim.notify`, where they belong: a failed scan, a
-- file that could not be indexed. Whichever notifier is installed renders
-- those and persists them however it persists things.
--
-- Two tiers of detail. The default reports only what makes someone wait or
-- tells them something broke. `verbose = true` adds per-document failures,
-- scans that changed nothing, and routine lifecycle churn.
--
-- The transport is the shared socket (lum/client.lua) rather than a subprocess
-- streaming events over stdout. That removes the failure mode this module used
-- to have to apologize for: a stream that died on its own, leaving a spinner up
-- forever with no way to tell that from "nothing is happening".

local M = {}

local client = require("lum.client")
local progress = require("lum.progress")

-- Phase labels, in the order they occur. Keys match the daemon's `phase`
-- field. The model download is absent on purpose: it is a separate operation
-- with its own token, the way rust-analyzer reports "Roots Scanned" apart from
-- "Indexing".
local PHASE_LABELS = {
  reading = "reading files",
  parsing = "parsing",
  embedding = "embedding",
  storing = "storing",
}

local state = {
  running = false,
  unsubscribe = nil,
  scans = {},
  -- Last state acted on, so transitions are measured from when this session
  -- attached rather than from the daemon's boot.
  daemon_state = nil,
  activity = nil,
}

local function new_activity()
  return {
    -- Files, from the scan planner. Coarse: a whole batch resolves at once.
    files_total = 0,
    files_done = 0,
    failed = 0,
    -- The current phase and its own units. This is what actually advances
    -- during the slow part.
    phase = nil,
    phase_done = 0,
    phase_total = 0,
    phase_unit = nil,
  }
end

state.activity = new_activity()

local defaults = {
  enabled = false,
  verbose = false,
  -- Progress reporting. `false` turns it off; a table is passed to
  -- lum.progress.setup, where `mode` chooses between LSP progress ("lsp"), a
  -- line lum draws itself ("window"), and asking whether anything renders LSP
  -- progress before deciding ("auto", the default).
  progress = true,
  -- Stay quiet about scans faster than this that changed nothing. A warm
  -- rescan finishes in milliseconds and announcing it is flicker.
  min_scan_ms = 750,
  -- How long a completion summary stays up before it clears. The pause is the
  -- point: it is the confirmation that what you waited for is done.
  summary_ms = 4000,
  -- Per-level dismissal for vim.notify messages, in milliseconds. `false`
  -- means stay until dismissed, which is what an error wants.
  timeouts = { info = 4000, warn = 10000, error = false },
  -- Merged into the opts table passed to vim.notify.
  opts = { title = "lum" },
  -- Receives each decoded event instead of any of the above.
  on_event = nil,
}

local config = vim.deepcopy(defaults)

-- `progress` is a boolean or a table of options, so "is it on" is a question
-- rather than a field.
local function progress_on()
  local p = config.progress
  if p == false or (type(p) == "table" and p.enabled == false) then
    return false
  end
  return true
end

local function level_key(level)
  if level == vim.log.levels.ERROR then
    return "error"
  elseif level == vim.log.levels.WARN then
    return "warn"
  end
  return "info"
end

local function notify(message, level, extra)
  local opts = vim.tbl_extend("force", vim.deepcopy(config.opts), extra or {})
  if opts.timeout == nil then
    local timeout = config.timeouts[level_key(level)]
    if timeout ~= nil then
      opts.timeout = timeout
    end
  end
  vim.notify(message, level, opts)
end

-- ---- progress ----

-- indexing composes the current activity into the three fields LSP progress
-- carries: a fixed title, a message that changes, and a percentage. Exposed
-- for testing — these rules are the whole design.
--
-- The percentage tracks the current phase rather than the whole scan, because
-- the phase is the only thing reporting a denominator. A bar that restarts at
-- each phase is honest about that; one interpolated across phases would be
-- inventing the ratio between them.
function M.indexing()
  local a = state.activity
  local label = a.phase and (PHASE_LABELS[a.phase] or a.phase) or nil

  local message, percentage
  if label and a.phase_total > 0 then
    message = ("%s %d/%d %s"):format(label, a.phase_done, a.phase_total, a.phase_unit or "")
    percentage = math.floor(a.phase_done / a.phase_total * 100)
  elseif label then
    message = label
  elseif a.files_total > 0 then
    -- Between phases. Nothing is counting yet, so say what is known.
    message = ("%d files"):format(a.files_total)
  else
    return nil
  end

  if a.failed > 0 then
    message = message .. (" · %d failed"):format(a.failed)
  end
  return { title = "indexing", message = (message:gsub("%s+$", "")), percentage = percentage }
end

local function render()
  if not progress_on() then
    return
  end
  local report = M.indexing()
  if report then
    progress.report("index", report)
  end
end

-- ---- daemon state ----

function M.state_transition(to, detail)
  if to == nil or to == "" or to == state.daemon_state then
    return
  end
  local from = state.daemon_state
  state.daemon_state = to

  if to == "downloading-model" then
    if progress_on() then
      progress.report("model", {
        title = "downloading the embedding model",
        message = "~70 MB, first run",
      })
    end
    return
  end

  if to == "failed" then
    -- Discrete and serious: this belongs in the notifier, sticky, not on a
    -- progress report that clears itself.
    state.activity = new_activity()
    progress.hide()
    notify(("lum could not start: %s"):format(detail or "unknown"), vim.log.levels.ERROR)
    return
  end

  if to == "ready" and from == "downloading-model" then
    if progress_on() then
      progress.finish("model", "embedding model ready", config.summary_ms)
    end
    return
  end

  if config.verbose and to == "starting" then
    notify("lum starting", vim.log.levels.INFO)
  end
end

-- ---- events ----

-- describe folds one event into the display. Exposed for testing.
function M.describe(event)
  local kind = event.event
  local a = state.activity

  if kind == "state" or kind == "snapshot" then
    M.state_transition(event.state, event.detail)
    if kind == "snapshot" and (event.pending_documents or 0) > a.files_total then
      a.files_total = event.pending_documents
    end
    return
  end

  if kind == "scan_started" then
    state.scans[event.source] = vim.uv.now()
    return
  end

  if kind == "progress" then
    a.phase = event.phase
    a.phase_done = event.done or 0
    a.phase_total = event.total or 0
    a.phase_unit = event.unit
    render()
    return
  end

  if kind == "doc_indexed" or kind == "doc_deleted" then
    a.files_done = a.files_done + 1
    if a.files_done > a.files_total then
      a.files_total = a.files_done
    end
    return
  end

  if kind == "doc_failed" then
    a.files_done = a.files_done + 1
    a.failed = a.failed + 1
    if config.verbose then
      notify(
        ("could not index %s: %s"):format(event.path or "?", event.error or "unknown"),
        vim.log.levels.WARN
      )
    end
    return
  end

  if kind == "scan_failed" then
    state.activity = new_activity()
    progress.finish("index")
    notify(("indexing failed: %s"):format(event.error or "unknown"), vim.log.levels.ERROR)
    return
  end

  if kind == "scan_finished" then
    local started = state.scans[event.source]
    state.scans[event.source] = nil
    local took = event.took_ms or (started and (vim.uv.now() - started)) or 0
    local had_work = a.files_total > 0 or a.phase ~= nil
    state.activity = new_activity()

    local indexed, removed, failed = event.indexed or 0, event.removed or 0, event.failed or 0
    if indexed == 0 and removed == 0 and failed == 0 and took < config.min_scan_ms and not config.verbose then
      -- Nothing changed and it was quick: the common case after the first
      -- index, and not news.
      progress.finish("index")
      return
    end

    local parts = {}
    if indexed > 0 then
      table.insert(parts, ("%d indexed"):format(indexed))
    end
    if removed > 0 then
      table.insert(parts, ("%d removed"):format(removed))
    end
    if failed > 0 then
      table.insert(parts, ("%d failed"):format(failed))
    end
    if #parts == 0 then
      table.insert(parts, ("%d unchanged"):format(event.unchanged or 0))
    end
    local summary = ("%s in %.1fs"):format(table.concat(parts, ", "), took / 1000)

    if progress_on() and had_work then
      progress.finish("index", summary, config.summary_ms)
    else
      progress.finish("index")
    end
    if failed > 0 then
      -- Failures should outlive the report that mentions them.
      notify(summary, vim.log.levels.WARN)
    end
    return
  end
end

function M.setup(opts)
  config = vim.tbl_deep_extend("force", vim.deepcopy(defaults), opts or {})
  if type(config.progress) == "table" then
    progress.setup(config.progress)
  end
end

function M.is_running()
  return state.running
end

-- subscribed_kinds is derived rather than configured: progress needs the
-- per-document and phase events, and subscribing to them with progress off
-- would be traffic nobody reads. Filtered daemon-side.
local function subscribed_kinds()
  local kinds = { "state", "snapshot", "scan_started", "scan_finished", "scan_failed" }
  if progress_on() then
    vim.list_extend(kinds, {
      "doc_indexed",
      "doc_deleted",
      "doc_failed",
      -- The one that actually moves: chunks embedded while a batch is in
      -- flight.
      "progress",
    })
  elseif config.verbose then
    table.insert(kinds, "doc_failed")
  end
  return kinds
end

local function handle(event)
  if config.on_event then
    config.on_event(event)
    return
  end
  M.describe(event)
end

-- start subscribes to the event stream. Safe to call repeatedly; only the
-- first call in a session subscribes.
function M.start(executable)
  if not config.enabled or state.running then
    return
  end
  state.running = true
  state.daemon_state = nil
  state.activity = new_activity()

  -- No replay: the daemon keeps a ring buffer for late joiners, which would
  -- otherwise arrive as a burst of reports about work that finished before
  -- Neovim started.
  state.unsubscribe = client.subscribe(executable or "lum", subscribed_kinds(), function(event)
    vim.schedule(function()
      handle(event)
    end)
  end, false)

  vim.api.nvim_create_autocmd("VimLeavePre", {
    once = true,
    callback = function()
      M.stop()
    end,
  })
end

function M.stop()
  progress.stop()
  if state.unsubscribe then
    state.unsubscribe()
    state.unsubscribe = nil
  end
  state.running = false
end

return M
