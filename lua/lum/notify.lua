-- Report what lum is doing, the way an LSP reports progress.
--
-- The goal is that you never have to wonder whether something is running,
-- stuck, or dead. Indexing shows a live progress bar while it runs, the
-- one-time model download announces itself, and a crashed worker says so and
-- stays on screen. Everything else keeps quiet, because a channel that
-- reports non-events is one you learn to ignore.
--
-- Two tiers. The default is what a user needs: work that makes them wait,
-- and failures. `verbose = true` adds the dev tier — per-document failures,
-- every scan including no-op ones, and routine worker lifecycle churn.
--
-- Everything routes through vim.notify, so fidget, nvim-notify, snacks, or
-- the built-in all render it with no per-plugin code. Progress needs the
-- notifier to support replacing a message; if it does not, that is detected
-- on the first update and progress degrades to start/finish milestones
-- rather than a new message every tick.

local M = {}

local PROGRESS_WIDTH = 16
local PROGRESS_THROTTLE_MS = 200
-- The counter alone is not enough to show liveness. Documents are embedded a
-- whole batch at a time, and the dispatcher only learns any of them finished
-- when the batch RPC returns — so on this repository the bar sat at 1/68 for
-- 47 seconds and then jumped to done. Correct, and indistinguishable from
-- hung. A spinner on its own timer keeps saying "still working" through the
-- gap, which is the entire point of reporting progress.
local SPINNER_FRAMES = { "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏" }
local SPINNER_INTERVAL_MS = 150

local state = {
  job = nil,
  stopping = false,
  scans = {},
  -- Last worker state reported on, so transitions are measured from when
  -- this session attached rather than from the daemon's boot. The server
  -- only publishes a transition once it has a previous state to compare
  -- against, so the first state its loop sees — on a cold start, the model
  -- download — never produces one. Snapshots carry worker_state
  -- unconditionally, so comparing them here sees it regardless.
  worker_state = nil,
  worker_handle = nil,
  progress = nil,
}

local function new_progress(supported)
  return {
    total = 0,
    done = 0,
    failed = 0,
    handle = nil,
    last_render = 0,
    supported = supported ~= false,
    frame = 1,
    stage = nil,
    timer = nil,
    rendered = 0,
  }
end

state.progress = new_progress(true)

local defaults = {
  enabled = false,
  -- Adds the dev tier: per-document failures, scans that changed nothing,
  -- and worker idle/respawn churn.
  verbose = false,
  -- Live-updating progress bar while indexing.
  progress = true,
  -- Stay quiet about scans faster than this that changed nothing. A warm
  -- rescan finishes in milliseconds and announcing it is flicker.
  min_scan_ms = 750,
  -- Per-level dismissal, in milliseconds. `false` means stay until
  -- dismissed, which is what an error wants: the whole point is that it is
  -- still on screen when you look up.
  timeouts = { info = 4000, warn = 10000, error = false },
  -- Merged into the opts table passed to vim.notify.
  opts = { title = "lum" },
  -- Receives each decoded event instead of any of the above.
  on_event = nil,
}

local config = vim.deepcopy(defaults)

local function level_key(level)
  if level == vim.log.levels.ERROR then
    return "error"
  elseif level == vim.log.levels.WARN then
    return "warn"
  end
  return "info"
end

-- notify wraps vim.notify with this module's opts, and returns whatever the
-- notifier hands back — a handle for the plugins that support replacing a
-- message, nil for the ones that do not.
local function notify(message, level, extra)
  local opts = vim.tbl_extend("force", vim.deepcopy(config.opts), extra or {})
  if opts.timeout == nil then
    local timeout = config.timeouts[level_key(level)]
    if timeout ~= nil then
      opts.timeout = timeout
    end
  end
  return vim.notify(message, level, opts)
end

local function relative(uri)
  local root = vim.uv.cwd()
  if root and vim.startswith(uri, root .. "/") then
    return uri:sub(#root + 2)
  end
  return uri
end

-- ---- progress ----

local function progress_bar(done, total)
  local ratio = total > 0 and math.min(1, done / total) or 0
  local filled = math.floor(ratio * PROGRESS_WIDTH + 0.5)
  return string.rep("█", filled) .. string.rep("░", PROGRESS_WIDTH - filled)
end

local stop_spinner, start_spinner

local function render_progress(force)
  local p = state.progress
  if not config.progress or not p.supported or p.total == 0 then
    return
  end
  local now = vim.uv.now()
  if not force and (now - p.last_render) < PROGRESS_THROTTLE_MS then
    return
  end
  p.last_render = now

  -- Until something completes, a bar is definitionally empty and reads as
  -- stuck. Documents are embedded a whole batch at a time and none of them
  -- resolve until the batch returns, so on a small repository that is the
  -- entire run. Show the count of files in flight instead, and switch to a
  -- bar once there is real movement to draw.
  local message
  if p.done == 0 then
    message = ("%s indexing %d files"):format(SPINNER_FRAMES[p.frame], p.total)
  else
    message = ("%s indexing ▕%s▏ %d/%d")
      :format(SPINNER_FRAMES[p.frame], progress_bar(p.done, p.total), p.done, p.total)
  end
  if p.stage and p.stage ~= "" then
    message = message .. (" · %s"):format(p.stage)
  end
  if p.failed > 0 then
    message = message .. (" (%d failed)"):format(p.failed)
  end
  -- id for snacks.nvim, replace for nvim-notify: whichever the notifier
  -- understands, it updates one message rather than stacking a new one.
  -- timeout=false keeps the bar up while work is in flight.
  -- Replacement is asked for two ways because notifiers disagree:
  -- nvim-notify takes the record a previous call returned as `replace`,
  -- snacks.nvim takes a stable `id`. A notifier honoring neither silently
  -- stacks a new message every tick — hundreds over one index.
  local handle = notify(message, vim.log.levels.INFO, {
    id = "lum-progress",
    replace = p.handle,
    timeout = false,
  })

  -- Decide, on the second update, whether this notifier actually replaces.
  -- Returning non-nil is not evidence: a notifier that ignores both keys
  -- still hands back a handle, just a different one each time, because it
  -- made a new notification. Same handle twice means one message is being
  -- updated. Assuming otherwise is what produced a wall of identical
  -- "indexing 0/68" lines.
  if p.rendered == 1 then
    local replacing = handle ~= nil and p.handle ~= nil and handle == p.handle
    if not replacing then
      p.supported = false
      stop_spinner()
      -- The one line already on screen is the fallback: it says indexing
      -- started, and scan_finished will report the outcome.
      return
    end
  end

  p.rendered = (p.rendered or 0) + 1
  p.handle = handle
  start_spinner()
end

-- The spinner runs on its own timer rather than on events, because the gap
-- this covers is precisely the one where no events arrive.
function start_spinner()
  local p = state.progress
  if p.timer or not p.supported or not config.progress then
    return
  end
  p.timer = vim.uv.new_timer()
  p.timer:start(
    SPINNER_INTERVAL_MS,
    SPINNER_INTERVAL_MS,
    vim.schedule_wrap(function()
      local current = state.progress
      if current.total == 0 or not current.supported then
        stop_spinner()
        return
      end
      current.frame = (current.frame % #SPINNER_FRAMES) + 1
      render_progress(true)
    end)
  )
end

function stop_spinner()
  local p = state.progress
  if p.timer then
    p.timer:stop()
    if not p.timer:is_closing() then
      p.timer:close()
    end
    p.timer = nil
  end
end

local function finish_progress(summary, level)
  local p = state.progress
  local handle = p.handle
  stop_spinner()
  state.progress = new_progress(p.supported)
  if summary then
    -- Replace the bar with the outcome, and let this one time out normally.
    notify(summary, level, { id = "lum-progress", replace = handle })
  end
end

-- ---- worker state ----

-- worker_transition reports a worker state change worth hearing about.
-- Idempotent: repeated snapshots of the same state say nothing.
function M.worker_transition(to, detail)
  if to == nil or to == "" or to == state.worker_state then
    return
  end
  local from = state.worker_state
  state.worker_state = to

  if to == "downloading-model" then
    -- Sticky and replaceable: a 70 MB download is the longest unexplained
    -- wait lum ever imposes, and it should still be on screen when you look
    -- back at the window.
    state.worker_handle = notify(
      "downloading the embedding model (~70 MB, first run only)",
      vim.log.levels.INFO,
      { id = "lum-worker", timeout = false }
    )
    return
  end
  if to == "crashed" then
    notify(("worker crashed: %s"):format(detail or "unknown"), vim.log.levels.ERROR, { id = "lum-worker" })
    return
  end
  if to == "ready" and from == "downloading-model" then
    notify("embedding model ready", vim.log.levels.INFO, { id = "lum-worker", replace = state.worker_handle })
    state.worker_handle = nil
    return
  end
  if config.verbose and (to == "idle" or to == "starting") then
    notify(("worker %s"):format(to), vim.log.levels.INFO)
  end
end

-- ---- event handling ----

-- describe folds one event into the notification state, emitting whatever it
-- warrants. Exposed for testing; the rules here are the design.
function M.describe(event)
  local kind = event.kind

  if kind == "snapshot" or kind == "worker_state_changed" then
    M.worker_transition(event.worker_state, event.detail)
    -- Snapshots are the only place the current pipeline stage is visible,
    -- and "embedding" is a more useful thing to show during a long batch
    -- than a counter that cannot move until it returns.
    if kind == "snapshot" and state.progress.total > 0 then
      state.progress.stage = event.active_stage
    end
    return
  end

  if kind == "scan_started" then
    -- Recorded, not announced: the progress line already says a scan began,
    -- and this one only had a source UUID to show for itself.
    state.scans[event.source_id] = vim.uv.now()
    return
  end

  if kind == "document_queued" then
    state.progress.total = state.progress.total + 1
    render_progress(false)
    return
  end

  if kind == "document_ingested" or kind == "document_deleted" then
    state.progress.done = state.progress.done + 1
    render_progress(false)
    return
  end

  if kind == "document_failed" then
    state.progress.done = state.progress.done + 1
    state.progress.failed = state.progress.failed + 1
    render_progress(false)
    if config.verbose then
      -- Normally summarized by scan_finished; one notification per bad file
      -- is noise in a repository with a handful of them.
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

    if event.error and event.error ~= "" then
      finish_progress(("indexing failed: %s"):format(event.error), vim.log.levels.ERROR)
      return
    end

    local ingested, removed, failed = event.ingested or 0, event.removed or 0, event.failed or 0
    if ingested == 0 and removed == 0 and failed == 0 and took < config.min_scan_ms and not config.verbose then
      -- Nothing changed and it was quick: the common case after the first
      -- index, and not news.
      finish_progress(nil)
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
    finish_progress(
      ("%s in %.1fs"):format(table.concat(parts, ", "), took / 1000),
      failed > 0 and vim.log.levels.WARN or vim.log.levels.INFO
    )
    return
  end
end

function M.setup(opts)
  config = vim.tbl_deep_extend("force", vim.deepcopy(defaults), opts or {})
end

function M.is_running()
  return state.job ~= nil
end

-- subscribed_types is derived rather than configured: progress needs the
-- per-document events, and subscribing to them when progress is off would be
-- traffic nobody reads. Filtering happens server-side.
local function subscribed_types()
  local types = { "scan_started", "scan_finished", "worker_state_changed", "snapshot" }
  if config.progress then
    vim.list_extend(types, { "document_queued", "document_ingested", "document_deleted", "document_failed" })
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
  state.worker_handle = nil
  stop_spinner()
  state.progress = new_progress(true)
  state.stopping = false

  -- --no-replay: the server keeps a ring buffer for late joiners, which
  -- would otherwise arrive as a burst of notifications about work that
  -- finished before Neovim started.
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
    -- The stream died on its own. Say so: silence here is indistinguishable
    -- from "nothing is happening", which is the confusion this module exists
    -- to prevent.
    vim.schedule(function()
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
  stop_spinner()
  if state.job then
    state.stopping = true
    pcall(function()
      state.job:kill("sigterm")
    end)
    state.job = nil
  end
end

return M
