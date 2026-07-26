-- Attach the working-tree lum to an already-loaded Neovim configuration.
--
-- Used by `lum-nvim-dev --user-config`, which starts your own Neovim — your
-- plugins, your notification handler, your keymaps — and then runs this.
--
-- The shell has already put the working-tree `lum` first on PATH and passed
-- --cmd 'set runtimepath^=<repo>'. What is left is the part that can only
-- happen after your config has loaded: Telescope must exist before an
-- extension can register with it.

local repo = vim.env.LUM_DEV_REPO
if not repo or repo == "" then
  vim.notify("LUM_DEV_REPO is unset; run this through lum-nvim-dev", vim.log.levels.ERROR)
  return
end

local function diagnose()
  -- Report what was actually searched, since "not available" is useless on
  -- its own: the cause is either no Telescope installed, or a plugin manager
  -- that has not loaded it yet.
  local found = vim.api.nvim_get_runtime_file("lua/telescope/init.lua", false)
  local lines = {
    "lum dev: could not load the Telescope extension.",
    found[1] and ("Telescope is on the runtimepath (" .. found[1] .. ") but did not load.")
      or "Telescope was not found on the runtimepath at all.",
    "",
    "If your config lazy-loads Telescope, open it once and run :LumAttach.",
    "If you do not use Telescope, run lum-nvim-dev without --user-config",
    "for the isolated config, which brings its own.",
  }
  vim.notify(table.concat(lines, "\n"), vim.log.levels.WARN)
end

local function attach(quiet)
  -- Re-assert the runtimepath. --cmd put it there before init, but plugin
  -- managers rebuild runtimepath during setup and routinely drop entries
  -- they did not add, which would leave `require("lum.telescope")` resolving
  -- to an installed lum-nvim — or to nothing.
  local paths = vim.opt.runtimepath:get()
  local present = false
  for _, path in ipairs(paths) do
    if path == repo then
      present = true
      break
    end
  end
  if not present then
    vim.opt.runtimepath:prepend(repo)
  end

  local ok, telescope = pcall(require, "telescope")
  if not ok then
    if not quiet then
      diagnose()
    end
    return false
  end

  -- Merge rather than replace: your config may already have set Telescope
  -- options, and clobbering them would make this a different Neovim than the
  -- one being tested.
  telescope.setup({
    extensions = {
      lum = {
        executable = "lum", -- PATH points at the working-tree build
        notify = true,
        index_on_open = true,
      },
    },
  })

  local loaded, err = pcall(telescope.load_extension, "lum")
  if not loaded then
    vim.notify("lum dev: loading the extension failed: " .. tostring(err), vim.log.levels.ERROR)
    return false
  end

  -- index_on_open normally fires on VimEnter, which has already passed by
  -- the time a lazy-loaded Telescope gets here.
  pcall(function()
    require("lum.telescope").start_indexing()
  end)

  vim.notify(
    ("lum dev attached — :Telescope lum\n  binary: %s"):format(
      vim.fn.exepath("lum") ~= "" and vim.fn.exepath("lum") or "NOT ON PATH"
    ),
    vim.log.levels.INFO
  )
  return true
end

-- An escape hatch for configs that load Telescope on first use: attaching at
-- startup cannot work there, and retrying forever would be worse than a
-- command you run once.
vim.api.nvim_create_user_command("LumAttach", function()
  attach(false)
end, { desc = "lum dev: attach the working-tree build to this session" })

-- Wait for the config to finish loading. Running inside -c would race plugin
-- managers, and an error there surfaces as "Error in command line", which
-- buries the actual explanation.
if vim.v.vim_did_enter == 1 then
  vim.schedule(function()
    attach(false)
  end)
else
  vim.api.nvim_create_autocmd("VimEnter", {
    once = true,
    callback = function()
      vim.schedule(function()
        attach(false)
      end)
    end,
  })
end
