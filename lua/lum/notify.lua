-- Report what lum is doing, so you never have to wonder whether something is
-- running, stuck, or dead.
--
-- Two channels, because these are two different kinds of thing.
--
-- Progress goes to one self-updating line that lum draws itself (see
-- lum/progress.lua). It appears when work starts, changes as the phase
-- changes, and stays until lum is ready to use. vim.notify is the wrong
-- primitive for this: it is built for discrete messages, and notifiers
-- disagree about whether one can be updated in place — asking them to
-- produced either three hundred stacked copies of a progress bar, or, once
-- that was detected and stopped, no progress at all.
--
-- Discrete events go through vim.notify, where they belong: a crashed
-- worker, a failed scan, a file that could not be indexed. Whichever
-- notifier is installed renders those and persists them however it
-- persists things.
--
-- Two tiers of detail. The default reports only what makes someone wait or
-- tells them something broke. `verbose = true` adds per-document failures,
-- scans that changed nothing, and routine worker lifecycle churn.

local M = {}

local progress = require("lum.progress")

-- Phase labels, in the order they occur. Keys match the worker's `phase`
-- field plus the two phases only the dispatcher knows about.
local PHASE_LABELS = {
  ["downloading-model"] = "downloading the embedding model (~70 MB, first run)",
  reading = "reading files",
  parsing = "parsing",
  embedding = "embedding",
  storing = "storing",
  deleting = "removing deleted files",
}

local state = {
  job = nil,
  stopping = false,
  scans = {},
  -- Last worker state acted on, so transitions are measured from when this
  -- session attached rather than from the daemon's boot. The server only
  -- publishes a transition once it has a previous state to compare against,
  -- so the first state its loop sees — on a cold start, the model download —
  -- never produces one. Snapshots carry worker_state unconditionally, so
  -- comparing them here sees it regardless.
  worker_state = nil,
  activity = nil,
}

local function new_activity()
  return {
    -- Files, from the dispatcher. Coarse: a whole batch resolves at once.
    files_total = 0,
    files_done = 0,
    failed = 0,
    -- The current phase and its own units, from inside the worker. This is
    -- what actually advances during the slow part.
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
  -- The self-updating progress line. `false` turns it off; a table is
  -- passed to lum.progress.setup — `{ row_offset = 1 }` or
  -- `{ anchor = "SW" }` moves it out from under a notifier that already
  -- owns the bottom-right corner, which fidget and noice both do.
  progress = true,
  -- Stay quiet about scans faster than this that changed nothing. A warm
  -- rescan finishes in milliseconds and announcing it is flicker.
  min_scan_ms = 750,
  -- How long a completion summary stays up before the line clears. The pause
  -- is the point: it is the confirmation that what you waited for is done.
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

-- `progress` is a boolean or a table of window options, so "is it on" is a
-- question rather than a field.
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

local function relative(uri)
  local root = vim.uv.cwd()
  if root and vim.startswith(uri, root .. "/") then
    return uri:sub(#root + 2)
  end
  return uri
end

-- ---- the progress line ----

-- line composes the current activity into one string. Exposed for testing:
-- these rules are the whole design.
function M.line()
  local a = state.activity
  local label = a.phase and (PHASE_LABELS[a.phase] or a.phase) or nil

  local text
  if label and a.phase_total > 0 then
    -- A phase reporting its own units: the bar that actually moves.
    text = ("%s %s %d/%d %s")
      :format(label, progress.bar(a.phase_done, a.phase_total), a.phase_done, a.phase_total, a.phase_unit or "")
  elseif label then
    -- A phase with nothing to count yet — the model download, mainly.
    text = label
  elseif a.files_total > 0 then
    -- Between phases. A bar over files would sit empty for a whole batch,
    -- so say what is known instead of drawing an empty meter.
    text = ("indexing %d files"):format(a.files_total)
  else
    return nil
  end

  if a.failed > 0 then
    text = text .. (" · %d failed"):format(a.failed)
  end
  return (text:gsub("%s+$", ""))
end

local function render()
  if not progress_on() then
    return
  end
  local text = M.line()
  if text then
    progress.show(text)
  end
end

-- ---- worker state ----

function M.worker_transition(to, detail)
  if to == nil or to == "" or to == state.worker_state then
    return
  end
  local from = state.worker_state
  state.worker_state = to

  if to == "downloading-model" then
    state.activity.phase = "downloading-model"
    render()
    return
  end

  if to == "crashed" then
    -- Discrete and serious: this belongs in the notifier, sticky, not on a
    -- line that clears itself.
    state.activity = new_activity()
    progress.hide()
    notify(("worker crashed: %s"):format(detail or "unknown"), vim.log.levels.ERROR)
    return
  end

  if to == "ready" and from == "downloading-model" then
    -- Only retire the download label. The worker can already be reporting a
    -- real phase by the time the snapshot loop notices it went ready, and
    -- clearing that showed a flicker back to "indexing N files".
    if state.activity.phase == "downloading-model" then
      state.activity.phase = nil
    end
    if state.activity.files_total > 0 or state.activity.phase then
      render() -- indexing already started; it takes the line over
    else
      progress.finish("embedding model ready", config.summary_ms)
    end
    return
  end

  if config.verbose and (to == "idle" or to == "starting") then
    notify(("worker %s"):format(to), vim.log.levels.INFO)
  end
end

-- ---- events ----

-- describe folds one event into the display. Exposed for testing.
function M.describe(event)
  local kind = event.kind
  local a = state.activity

  if kind == "snapshot" or kind == "worker_state_changed" then
    M.worker_transition(event.worker_state, event.detail)
    return
  end

  if kind == "scan_started" then
    state.scans[event.source_id] = vim.uv.now()
    return
  end

  if kind == "worker_progress" then
    a.phase = event.phase
    a.phase_done = event.done or 0
    a.phase_total = event.total or 0
    a.phase_unit = event.unit
    render()
    return
  end

  if kind == "document_queued" then
    a.files_total = a.files_total + 1
    render()
    return
  end

  if kind == "document_ingested" or kind == "document_deleted" then
    a.files_done = a.files_done + 1
    return
  end

  if kind == "document_failed" then
    a.files_done = a.files_done + 1
    a.failed = a.failed + 1
    if config.verbose then
      notify(
        ("could not index %s: %s"):format(relative(event.uri or "?"), event.error or "unknown"),
        vim.log.levels.WARN
      )
    end
    return
  end

  if kind == "scan_finished" then
    local started = state.scans[event.source_id]
    state.scans[event.source_id] = nil
    local took = event.took_ms or (started and (vim.uv.now() - started)) or 0
    state.activity = new_activity()

    if event.error and event.error ~= "" then
      progress.hide()
      notify(("indexing failed: %s"):format(event.error), vim.log.levels.ERROR)
      return
    end

    local ingested, removed, failed = event.ingested or 0, event.removed or 0, event.failed or 0
    if ingested == 0 and removed == 0 and failed == 0 and took < config.min_scan_ms and not config.verbose then
      -- Nothing changed and it was quick: the common case after the first
      -- index, and not news.
      progress.hide()
      return
    end

    local parts = {}
    if ingested > 0 then
      table.insert(parts, ("%d indexed"):format(ingested))
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

    if progress_on() then
      progress.finish(summary, config.summary_ms)
    end
    if failed > 0 then
      -- Failures should outlive the line that reports them.
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
  return state.job ~= nil
end

-- subscribed_types is derived rather than configured: the progress line needs
-- the per-document and worker-progress events, and subscribing to them with
-- progress off would be traffic nobody reads. Filtered server-side.
local function subscribed_types()
  local types = { "scan_started", "scan_finished", "worker_state_changed", "snapshot" }
  if progress_on() then
    vim.list_extend(types, {
      "document_queued",
      "document_ingested",
      "document_deleted",
      "document_failed",
      -- The one that actually moves: chunks embedded, documents stored,
      -- reported from inside the worker while a batch is in flight.
      "worker_progress",
    })
  elseif config.verbose then
    table.insert(types, "document_failed")
  end
  return types
end

local function handle(event)
  if config.on_event then
    config.on_event(event)
    return
  end
  M.describe(event)
end

-- start subscribes to the event stream. Safe to call repeatedly; only the
-- first call in a session starts a job.
function M.start(executable)
  if not config.enabled or state.job then
    return
  end
  state.worker_state = nil
  state.activity = new_activity()
  state.stopping = false

  -- --no-replay: the server keeps a ring buffer for late joiners, which would
  -- otherwise arrive as a burst of reports about work that finished before
  -- Neovim started.
  local cmd = { executable or "lum", "events", "--no-replay", "--types", table.concat(subscribed_types(), ",") }

  local ok, job = pcall(vim.system, cmd, {
    text = true,
    stdout = function(err, data)
      if err or not data then
        return
      end
      for line in data:gmatch("[^\r\n]+") do
        local decoded, event = pcall(vim.json.decode, line)
        if decoded and type(event) == "table" then
          vim.schedule(function()
            handle(event)
          end)
        end
      end
    end,
  }, function(result)
    local was_stopping = state.stopping
    state.job = nil
    if was_stopping then
      return
    end
    -- The stream died on its own. Clear the line — leaving a spinner up
    -- forever is the exact impression this module exists to avoid — and say
    -- so, because silence here is indistinguishable from "nothing is
    -- happening".
    vim.schedule(function()
      state.activity = new_activity()
      progress.hide()
      notify(
        ("stopped following lum activity (exit %s); run :Telescope lum to resume")
          :format(tostring(result and result.code or "?")),
        vim.log.levels.WARN
      )
    end)
  end)

  if not ok then
    return
  end
  state.job = job

  vim.api.nvim_create_autocmd("VimLeavePre", {
    once = true,
    callback = function()
      M.stop()
    end,
  })
end

function M.stop()
  progress.hide()
  if state.job then
    state.stopping = true
    pcall(function()
      state.job:kill("sigterm")
    end)
    state.job = nil
  end
end

return M
