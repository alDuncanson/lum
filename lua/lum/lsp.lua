-- Report progress the way a language server does.
--
-- rust-analyzer does not draw anything. It sends LSP `$/progress`
-- notifications; Neovim's client records them and fires `LspProgress`; and
-- whichever plugin you installed — noice, fidget, snacks — renders them. That
-- is why every server's progress looks the same, sits in the same place, and
-- stacks instead of overlapping. Anything that draws its own window is
-- competing for those cells, and no zindex settles it: whichever wins hides
-- the other.
--
-- lum is not a language server, but it can speak the one part of the protocol
-- that matters here. `vim.lsp.start` accepts an in-process server — `cmd` as a
-- function returning request/notify handlers rather than a process to spawn —
-- so lum registers a client that implements `initialize` and `shutdown`,
-- answers nothing else, attaches to no buffer, and exists only to emit
-- `$/progress`. Its progress then goes wherever rust-analyzer's goes, styled
-- and positioned by the same plugin, because it is the same mechanism.
--
-- The cost is honest and worth naming: `vim.lsp.get_clients()` will list a
-- client called "lum". It attaches to no buffer, so buffer-scoped lookups —
-- which is what statusline components use — do not see it.

local M = {}

local state = {
  client_id = nil,
  dispatchers = nil,
  -- Tokens currently in flight, so a second report knows to send `report`
  -- rather than a second `begin`.
  tokens = {},
}

--- The in-process server. Neovim calls this with its dispatchers and expects
--- a `vim.lsp.rpc.PublicClient` back.
local function serve(dispatchers)
  state.dispatchers = dispatchers
  local closing = false
  local last_id = 0

  local function exit()
    if closing then
      return
    end
    closing = true
    state.dispatchers = nil
    state.tokens = {}
    -- Scheduled: Neovim calls terminate() from inside client teardown, and
    -- reporting the exit synchronously re-enters it.
    vim.schedule(function()
      dispatchers.on_exit(0, 0)
    end)
  end

  return {
    request = function(method, _, callback)
      last_id = last_id + 1
      if method == "initialize" then
        callback(nil, { capabilities = {}, serverInfo = { name = "lum" } })
      elseif method == "shutdown" then
        callback(nil, vim.NIL)
      else
        -- Deliberately nothing else. This client reports progress; it does
        -- not answer questions about code. Search goes over lum's own API.
        callback({ code = -32601, message = method .. " is not implemented" }, nil)
      end
      return true, last_id
    end,
    notify = function(method)
      if method == "exit" then
        exit()
      end
      return true
    end,
    is_closing = function()
      return closing
    end,
    terminate = exit,
  }
end

--- Whether anything in this session would render LSP progress.
---
--- noice, fidget and snacks all register an `LspProgress` autocmd. Core
--- Neovim records progress but displays none of it — `vim.lsp.status()` is
--- offered for your statusline and that is all — so with no listener a report
--- goes nowhere, and lum should draw its own line instead.
function M.has_consumer()
  local ok, autocmds = pcall(vim.api.nvim_get_autocmds, { event = "LspProgress" })
  return ok and #autocmds > 0
end

--- Register the client, if it is not already running.
function M.start()
  if state.client_id and vim.lsp.get_client_by_id(state.client_id) then
    return state.dispatchers ~= nil
  end
  state.client_id = nil
  local ok, client_id = pcall(vim.lsp.start, {
    name = "lum",
    cmd = serve,
    root_dir = vim.uv.cwd(),
  }, { attach = false, silent = true })
  if not ok or not client_id then
    return false
  end
  state.client_id = client_id
  return state.dispatchers ~= nil
end

local function send(params)
  if not state.dispatchers then
    return false
  end
  return pcall(state.dispatchers.notification, "$/progress", params)
end

--- Begin or advance one progress token.
---
--- `title` is fixed at `begin` and carried forward by Neovim; `message` and
--- `percentage` are what move. That split is the protocol's, and it is why
--- the model download and the index are separate tokens rather than one token
--- whose title changes — exactly as rust-analyzer reports "Roots Scanned" and
--- "Indexing" separately.
function M.report(key, opts)
  if not M.start() then
    return false
  end
  local token = "lum/" .. key
  if state.tokens[token] then
    return send({
      token = token,
      value = { kind = "report", message = opts.message, percentage = opts.percentage },
    })
  end
  state.tokens[token] = true
  -- The spec has the server ask before using a token it invented. Neovim
  -- always agrees, but a consumer keyed off the create request would miss
  -- everything reported under a token it never saw announced.
  pcall(state.dispatchers.server_request, "window/workDoneProgress/create", { token = token })
  return send({
    token = token,
    value = {
      kind = "begin",
      title = opts.title,
      message = opts.message,
      percentage = opts.percentage,
    },
  })
end

--- End a token. Consumers show the final message briefly and drop it.
function M.finish(key, message)
  local token = "lum/" .. key
  if not state.tokens[token] then
    return false
  end
  state.tokens[token] = nil
  return send({ token = token, value = { kind = "end", message = message } })
end

--- End every token and deregister the client.
function M.stop()
  for token in pairs(state.tokens) do
    send({ token = token, value = { kind = "end" } })
  end
  state.tokens = {}
  local client = state.client_id and vim.lsp.get_client_by_id(state.client_id)
  if client then
    pcall(function()
      client:stop(true)
    end)
  end
  state.client_id = nil
  state.dispatchers = nil
end

return M
