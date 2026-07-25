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
