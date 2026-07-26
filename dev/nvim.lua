-- Minimal, isolated Neovim config for exercising the local lum build.
--
-- Deliberately not your config: no colorscheme, no LSP, no other pickers.
-- When a result looks wrong you want the only variable to be lum, and a
-- personal config is a large pile of variables.
--
-- Loaded by `lum-nvim-dev`, which builds the dispatcher from the working
-- tree, puts it first on PATH, and sets the LUM_DEV_* variables below.
-- Run that rather than sourcing this by hand.

local function required(name)
  local value = vim.env[name]
  if not value or value == "" then
    error(name .. " is unset — launch this through `lum-nvim-dev`, not `nvim -u` directly")
  end
  return value
end

local repo = required("LUM_DEV_REPO")

vim.g.mapleader = " "

-- Bare Neovim's vim.notify writes to the message area, which the Telescope
-- prompt occupies and redraws over — so during a search, which is exactly
-- when lum has something to say, the messages are invisible. Real users have
-- fidget/nvim-notify/snacks and see them properly; this stands in for that so
-- a dev session shows what a real setup would.
--
-- Run with --user-config to use your own Neovim instead of this file.
do
  local entries, win, buf, next_id = {}, nil, nil, 0
  local highlights = {
    [vim.log.levels.ERROR] = "ErrorMsg",
    [vim.log.levels.WARN] = "WarningMsg",
    [vim.log.levels.INFO] = "Comment",
  }

  local function close()
    if win and vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_close(win, true)
    end
    win = nil
  end

  local function render()
    if #entries == 0 then
      return close()
    end
    local lines, width = {}, 20
    for _, entry in ipairs(entries) do
      table.insert(lines, " " .. entry.text .. " ")
      width = math.max(width, vim.fn.strdisplaywidth(entry.text) + 2)
    end
    width = math.min(width, math.floor(vim.o.columns * 0.6))

    if not (buf and vim.api.nvim_buf_is_valid(buf)) then
      buf = vim.api.nvim_create_buf(false, true)
    end
    vim.bo[buf].modifiable = true
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
    vim.bo[buf].modifiable = false
    local ns = vim.api.nvim_create_namespace("lum_notify")
    vim.api.nvim_buf_clear_namespace(buf, ns, 0, -1)
    for index, entry in ipairs(entries) do
      vim.api.nvim_buf_set_extmark(buf, ns, index - 1, 0, {
        end_row = index,
        hl_group = highlights[entry.level] or "Comment",
        hl_eol = true,
      })
    end

    local opts = {
      relative = "editor",
      anchor = "NE",
      row = 1,
      col = vim.o.columns - 1,
      width = width,
      height = #lines,
      style = "minimal",
      border = "rounded",
      focusable = false,
      noautocmd = true,
      -- Above Telescope's windows, or this solves nothing.
      zindex = 300,
    }
    if win and vim.api.nvim_win_is_valid(win) then
      vim.api.nvim_win_set_config(win, opts)
    else
      win = vim.api.nvim_open_win(buf, false, opts)
      vim.wo[win].winblend = 10
    end
  end

  local function expire(entry, level)
    if level == false then
      return
    end
    entry.generation = (entry.generation or 0) + 1
    local generation = entry.generation
    vim.defer_fn(function()
      -- A replaced entry gets a new generation; this timer is then stale and
      -- must not remove the message that took its place.
      if entry.generation ~= generation then
        return
      end
      for index, candidate in ipairs(entries) do
        if candidate == entry then
          table.remove(entries, index)
          break
        end
      end
      render()
    end, level)
  end

  -- Supports opts.replace / opts.id so a repeated message updates one line
  -- instead of stacking, the way a real notifier does.
  vim.notify = function(message, level, opts)
    opts = opts or {}
    level = level or vim.log.levels.INFO
    local flat = tostring(message):gsub("%s*\n%s*", " · ")
    -- Errors stay until something replaces them; progress carries an
    -- explicit timeout of false while in flight.
    local ttl = opts.timeout
    if ttl == nil then
      ttl = level >= vim.log.levels.ERROR and false or 8000
    end

    local key = opts.replace or opts.id
    local existing
    for _, entry in ipairs(entries) do
      if key ~= nil and entry.id == key then
        existing = entry
        break
      end
    end

    local entry = existing
    if entry then
      entry.text, entry.level = flat, level
    else
      next_id = next_id + 1
      entry = { id = opts.id or next_id, text = flat, level = level }
      table.insert(entries, entry)
    end
    expire(entry, ttl)

    -- :messages history too, flattened: echoing more lines than cmdheight
    -- triggers the hit-enter prompt.
    vim.api.nvim_echo({ { "[lum] " .. flat } }, true, {})
    render()
    return entry.id
  end

  -- Stand in for what noice, fidget and snacks do with LSP progress. lum
  -- reports through `$/progress` like rust-analyzer, so with nothing
  -- listening there is nothing to see — and worse, lum would notice that and
  -- fall back to drawing its own line, so a dev session would exercise the
  -- path real users do not take. Rendering it here keeps the loop honest.
  vim.api.nvim_create_autocmd("LspProgress", {
    callback = function(args)
      local params = args.data.params
      local value = params.value
      local client = vim.lsp.get_client_by_id(args.data.client_id)
      local parts = {}
      if value.percentage then
        local filled = math.floor(value.percentage / 100 * 14 + 0.5)
        table.insert(parts, ("▕%s▏"):format(("█"):rep(filled) .. ("░"):rep(14 - filled)))
      end
      table.insert(parts, value.message or value.title or "")
      table.insert(parts, ("(%s %s)"):format(client and client.name or "lsp", value.title or ""))
      vim.notify(table.concat(parts, " "), vim.log.levels.INFO, {
        id = "progress:" .. tostring(params.token),
        timeout = value.kind == "end" and 4000 or false,
      })
    end,
  })
end

-- Telescope and its plenary dependency come from the Nix store; they are
-- fixed inputs here, not something being tested.
vim.opt.runtimepath:append(required("LUM_DEV_TELESCOPE"))
vim.opt.runtimepath:append(required("LUM_DEV_PLENARY"))

-- The repository root, not its lua/ subdirectory: `require` resolves
-- "lum.telescope" against <rtp entry>/lua/, so pointing at lua/ directly
-- would have it look for lua/lua/. This is the same shape buildVimPlugin
-- produces for the packaged lum-nvim.
--
-- Prepended so the working tree shadows any installed lum-nvim. Edits to
-- lua/lum/telescope.lua then apply on the next Neovim start, with nothing
-- to rebuild.
vim.opt.runtimepath:prepend(repo)

require("telescope").setup({
  extensions = {
    lum = {
      -- Resolved from PATH, which lum-nvim-dev points at the working-tree
      -- build. Naming the binary rather than an absolute path keeps this
      -- honest: it exercises the same lookup a real user's setup does.
      executable = "lum",
      limit = 50,
      debounce_ms = 200,
      -- Both on here, off by default in the plugin: watching indexing happen
      -- is most of the point of a dev session, and waiting on a cold index
      -- inside the picker is the thing being avoided.
      -- verbose adds the dev tier: per-document failures, scans that
      -- changed nothing, worker idle/respawn churn.
      notify = { verbose = true },
      index_on_open = true,
    },
  },
})
require("telescope").load_extension("lum")

vim.keymap.set("n", "<leader>fs", function()
  require("telescope").extensions.lum.lum()
end, { desc = "lum: semantic code search" })

-- Search a directory other than this repository without leaving the shell.
vim.api.nvim_create_user_command("LumRoot", function(opts)
  require("telescope").extensions.lum.lum({ root = opts.args })
end, { nargs = 1, complete = "dir", desc = "lum: search a specific root" })

vim.api.nvim_create_autocmd("VimEnter", {
  once = true,
  callback = function()
    vim.notify(table.concat({
      "lum dev session",
      "  <leader>fs or :Telescope lum   search this repository",
      "  :LumRoot <dir>                 search another directory",
      "  binary: " .. (vim.fn.exepath("lum") ~= "" and vim.fn.exepath("lum") or "NOT ON PATH"),
      "  data:   " .. (vim.env.LUM_DATA_DIR or "?"),
      "  api:    " .. (vim.env.LUM_HTTP_ADDR or "?"),
    }, "\n"), vim.log.levels.INFO)
  end,
})
