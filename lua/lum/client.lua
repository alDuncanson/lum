-- One socket, held open for the session.
--
-- Connects once with `vim.uv` and speaks newline-delimited JSON down that one
-- pipe, so a query costs a write and a read (~5 ms per keystroke, measured)
-- rather than a process spawn. Debouncing is a `vim.uv` timer: free, and —
-- the part that matters — cancellable, so a superseded keystroke stops
-- existing instead of racing to deliver results nobody wants.

local M = {}

local state = {
  pipe = nil,
  status = "closed", -- closed | connecting | open
  buffer = "",
  next_id = 1,
  pending = {},
  subscribers = {},
  waiters = {},
  spawn_attempted = false,
}

--- Where the daemon listens. Mirrors src/config.rs.
---
--- `os.getenv` rather than `vim.env`, and memoized. `vim.env` goes through
--- Vimscript's `getenv`, which throws E5560 in a fast event context — and the
--- connect retry runs inside a `vim.uv` timer callback, which is one. The
--- first attempt would succeed and every retry after it would throw, so a
--- daemon that was not already running could never be connected to.
local socket_path
function M.socket_path()
  if not socket_path then
    local dir = os.getenv("LUM_DATA_DIR")
    if not dir or dir == "" then
      dir = (vim.uv.os_homedir() or ".") .. "/.lum"
    end
    socket_path = dir .. "/lum.sock"
  end
  return socket_path
end

--- Hand control back to a consumer, always on the main loop.
---
--- Connect, write, and timer callbacks all run in a fast event context, where
--- most of the Vim API is unavailable. Scheduling here means no caller of this
--- module has to know that — which is the mistake `socket_path` above records.
local function resolve(callback, ...)
  local args = { ... }
  vim.schedule(function()
    callback(unpack(args))
  end)
end

local function fail_everything(reason)
  local pending, waiters = state.pending, state.waiters
  state.pending, state.waiters = {}, {}
  for _, callback in pairs(pending) do
    resolve(callback, reason, nil)
  end
  for _, waiter in ipairs(waiters) do
    resolve(waiter, reason)
  end
end

local function teardown(reason)
  if state.pipe and not state.pipe:is_closing() then
    state.pipe:close()
  end
  state.pipe = nil
  state.status = "closed"
  state.buffer = ""
  -- Subscribers survive a disconnect: the daemon shutting down when idle is
  -- normal, and a notifier that silently stopped working after the first idle
  -- timeout would be worse than one that reconnects.
  fail_everything(reason or "lum: connection closed")
end

--- Called from `on_data`, which is `vim.schedule_wrap`ped — so consumers here
--- are already on the main loop and need no further scheduling.
local function dispatch(message)
  if message.id then
    local callback = state.pending[message.id]
    if callback then
      state.pending[message.id] = nil
      callback(message.error, message.ok)
    end
    return
  end
  if message.event then
    for _, handler in pairs(state.subscribers) do
      handler(message)
    end
  end
end

local function on_data(err, chunk)
  if err or not chunk then
    teardown(err and ("lum: " .. err) or nil)
    return
  end
  state.buffer = state.buffer .. chunk
  while true do
    local newline = state.buffer:find("\n", 1, true)
    if not newline then
      return
    end
    local line = state.buffer:sub(1, newline - 1)
    state.buffer = state.buffer:sub(newline + 1)
    if line ~= "" then
      local ok, message = pcall(vim.json.decode, line)
      if ok and type(message) == "table" then
        dispatch(message)
      end
    end
  end
end

--- Start a daemon. One spawn per session at most, and only when nothing is
--- listening — the binary starts itself on demand for every other client too.
---
--- Through a shell so the daemon's stderr lands in `daemon.log`, which is where
--- every other way of starting it puts them and where its own error messages
--- tell you to look. `vim.system` would otherwise buffer them inside Neovim and
--- discard them on exit, so a daemon that failed to start left an empty log and
--- nothing to read. Values after the script are positional parameters, never
--- shell source.
local function spawn(executable)
  if state.spawn_attempted then
    return
  end
  state.spawn_attempted = true
  local log = M.socket_path():gsub("lum%.sock$", "daemon.log")
  vim.system(
    { "sh", "-c", 'exec "$1" serve >>"$2" 2>&1', "lum-spawn", executable, log },
    { detach = true }
  )
end

local function open(executable, callback)
  if state.status == "open" then
    return callback(nil)
  end
  table.insert(state.waiters, callback)
  if state.status == "connecting" then
    return
  end
  state.status = "connecting"

  local attempts = 0
  local function try()
    attempts = attempts + 1
    local pipe = vim.uv.new_pipe(false)
    if not pipe then
      state.status = "closed"
      return fail_everything("lum: could not create a pipe")
    end
    pipe:connect(M.socket_path(), function(err)
      if not err then
        state.pipe = pipe
        state.status = "open"
        state.spawn_attempted = false
        pipe:read_start(vim.schedule_wrap(on_data))
        local waiters = state.waiters
        state.waiters = {}
        for _, waiter in ipairs(waiters) do
          resolve(waiter, nil)
        end
        return
      end
      if not pipe:is_closing() then
        pipe:close()
      end
      -- Nothing listening: start it, then keep trying while it comes up. A
      -- first run includes a 133 MB model download, so the ceiling is
      -- generous; every attempt after the first is a cheap connect.
      if attempts == 1 then
        -- vim.system touches the Vim API, and this is a connect callback.
        vim.schedule(function()
          spawn(executable)
        end)
      end
      if attempts > 600 then
        state.status = "closed"
        return fail_everything(
          ("lum: no daemon appeared at %s. If `lum` on your PATH is older than 0.2, "):format(M.socket_path())
            .. "it serves HTTP instead of this socket and will never create one — "
            .. "point `executable` at the new binary. Otherwise see the daemon log."
        )
      end
      local timer = vim.uv.new_timer()
      timer:start(50, 0, function()
        timer:close()
        try()
      end)
    end)
  end
  try()
end

--- Send a request. `callback(err, result)` runs on the main loop.
function M.request(executable, request, callback)
  open(executable, function(err)
    if err then
      return callback(err, nil)
    end
    -- The connection can go away between `open` resolving and this running —
    -- an idle-timeout shutdown lands exactly here. Without the guard that is a
    -- Lua error indexing a nil pipe, thrown from inside a Telescope callback,
    -- which reports as a broken picker rather than a closed socket.
    if state.status ~= "open" or not state.pipe then
      return callback("lum: connection closed before the request was sent", nil)
    end
    local id = state.next_id
    state.next_id = id + 1
    request.id = id
    state.pending[id] = callback
    local ok, encoded = pcall(vim.json.encode, request)
    if not ok then
      state.pending[id] = nil
      return callback("lum: could not encode request", nil)
    end
    state.pipe:write(encoded .. "\n", function(write_err)
      if write_err and state.pending[id] then
        state.pending[id] = nil
        vim.schedule(function()
          callback("lum: " .. write_err, nil)
        end)
      end
    end)
  end)
end

--- Receive events until the returned function is called.
---
--- `replay` is false by default and should stay that way for anything that
--- reacts to events rather than displaying them: replaying the ring buffer
--- would announce work that finished long before Neovim started.
function M.subscribe(executable, kinds, handler, replay)
  local key = tostring(handler)
  state.subscribers[key] = handler
  M.request(executable, { op = "subscribe", kinds = kinds or {}, replay = replay or false }, function() end)
  return function()
    state.subscribers[key] = nil
  end
end

function M.close()
  state.subscribers = {}
  teardown()
end

function M.is_connected()
  return state.status == "open"
end

return M
