-- Surface lum's indexing activity through vim.notify, the way an LSP
-- reports progress.
--
-- Off by default and started lazily. Subscribing is not free — the first
-- request starts the lum daemon, and merely opening Neovim should not do
-- that. The Telescope picker starts it on first use instead, which is
-- exactly when the first index runs and when you want to watch it.
--
-- Everything routes through vim.notify rather than drawing anything, so
-- whichever notification plugin is installed (fidget, nvim-notify, snacks,
-- or the built-in) renders it. Anyone wanting different behavior passes
-- on_event and gets the decoded stream.

local M = {}

local state = {
  job = nil,
  scans = {},
  -- Last worker state we reported on, so transitions are measured from
  -- when this session attached rather than from the daemon's boot.
  --
  -- Tracking it here rather than relying on worker_state_changed is
  -- deliberate. The server only publishes a transition once it has a
  -- previous state to compare against, so the *first* state its snapshot
  -- loop observes never produces one — and on a cold start that first
  -- state is downloading-model, the slowest and most worth reporting step
  -- there is. Snapshots carry worker_state unconditionally, so comparing
  -- them ourselves sees it regardless of when we attached.
  worker_state = nil,
}

local defaults = {
  enabled = false,
  -- Filtered server-side, so an unwanted kind costs nothing on the wire.
  -- document_* kinds are omitted on purpose: one notification per file is
  -- noise, and scan_finished already summarizes the same work.
  -- snapshot is included because it carries worker_state unconditionally,
  -- which is how a cold start is observed at all. See state.worker_state.
  types = { "scan_started", "scan_finished", "worker_state_changed", "document_failed", "snapshot" },
  -- Stay quiet about scans faster than this that changed nothing. A warm
  -- rescan finishes in a couple hundred milliseconds; announcing it is
  -- flicker, not information.
  min_scan_ms = 750,
  -- Passed through to vim.notify as its opts table.
  --
  -- timeout is the one that matters. Without it every notifier applies its
  -- own default, and several keep messages until they are dismissed by hand
  -- — which turns a progress channel into a pile that outlives the work it
  -- was describing. Progress should disappear when it stops being news.
  -- Both nvim-notify and snacks.nvim honor this key; fidget and the built-in
  -- ignore it harmlessly.
  opts = { title = "lum", timeout = 4000 },
  -- Receives each decoded event instead of the built-in formatting.
  on_event = nil,
}

local config = vim.deepcopy(defaults)

local function relative(uri)
  local root = vim.uv.cwd()
  if root and vim.startswith(uri, root .. "/") then
    return uri:sub(#root + 2)
  end
  return uri
end

-- describe returns a message and level for an event, or nil to stay quiet.
-- Exposed for testing; the rules here are the whole design of this module.
function M.describe(event)
  local kind = event.kind

  if kind == "scan_started" then
    state.scans[event.source_id] = vim.uv.now()
    return nil -- reported on completion, and only if it was worth reporting
  end

  if kind == "scan_finished" then
    local started = state.scans[event.source_id]
    state.scans[event.source_id] = nil
    local took = event.took_ms or (started and (vim.uv.now() - started)) or 0

    if event.error and event.error ~= "" then
      return ("indexing failed: %s"):format(event.error), vim.log.levels.ERROR
    end

    local ingested = event.ingested or 0
    local removed = event.removed or 0
    local failed = event.failed or 0
    if ingested == 0 and removed == 0 and failed == 0 and took < config.min_scan_ms then
      return nil
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
    return ("%s in %.1fs"):format(table.concat(parts, ", "), took / 1000),
      failed > 0 and vim.log.levels.WARN or vim.log.levels.INFO
  end

  if kind == "worker_state_changed" or kind == "snapshot" then
    return M.worker_transition(event.worker_state, event.detail)
  end

  if kind == "document_failed" then
    return ("could not index %s: %s"):format(relative(event.uri or "?"), event.error or "unknown"),
      vim.log.levels.WARN
  end

  return nil
end

-- worker_transition reports a worker state change, if it is one worth
-- hearing about. Idempotent per state: repeated snapshots of the same state
-- say nothing.
function M.worker_transition(to, detail)
  if to == nil or to == "" or to == state.worker_state then
    return nil
  end
  local from = state.worker_state
  state.worker_state = to

  if to == "downloading-model" then
    -- The one genuinely slow and otherwise invisible step.
    return "downloading the embedding model (~70 MB, first run only)", vim.log.levels.INFO
  end
  if to == "crashed" then
    return ("worker crashed: %s"):format(detail or "unknown"), vim.log.levels.ERROR
  end
  if to == "ready" and from == "downloading-model" then
    return "embedding model ready", vim.log.levels.INFO
  end
  -- Everything else is routine: idle shedding, the respawn after it, and
  -- the brief unavailable window while the worker binds its socket.
  -- Reporting those would train you to ignore this channel.
  return nil
end

function M.setup(opts)
  config = vim.tbl_deep_extend("force", vim.deepcopy(defaults), opts or {})
end

function M.is_running()
  return state.job ~= nil
end

local function handle(event)
  if config.on_event then
    config.on_event(event)
    return
  end
  local message, level = M.describe(event)
  if message then
    vim.notify(message, level, vim.deepcopy(config.opts))
  end
end

-- start subscribes to the event stream. Safe to call repeatedly; only the
-- first call in a session starts a job.
function M.start(executable)
  if not config.enabled or state.job then
    return
  end

  -- --no-replay, so the ring buffer the server keeps for late joiners does
  -- not arrive as a burst of notifications about work that finished before
  -- Neovim started.
  state.worker_state = nil
  local cmd = { executable or "lum", "events", "--no-replay" }
  if config.types and #config.types > 0 then
    vim.list_extend(cmd, { "--types", table.concat(config.types, ",") })
  end

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
  }, function()
    state.job = nil
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
  if state.job then
    pcall(function()
      state.job:kill("sigterm")
    end)
    state.job = nil
  end
end

return M
