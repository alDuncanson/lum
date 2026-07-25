-- Attach the working-tree lum to an already-loaded Neovim configuration.
--
-- Used by `lum-nvim-dev --user-config`, which starts your own Neovim (your
-- plugins, your notification handler, your keymaps) and then runs this. The
-- shell script has already prepended the repository to the runtimepath via
-- --cmd, so `require("lum.telescope")` resolves to the working tree before
-- your config loads and any installed lum-nvim is shadowed.
--
-- What is left to do here is the part that must happen *after* your config:
-- Telescope has to exist before an extension can register with it.

local repo = vim.env.LUM_DEV_REPO
if not repo or repo == "" then
  vim.notify("LUM_DEV_REPO is unset; run this through lum-nvim-dev", vim.log.levels.ERROR)
  return
end

local ok, telescope = pcall(require, "telescope")
if not ok then
  vim.notify(
    "lum dev: Telescope is not available in this configuration, so the picker cannot load.\n"
      .. "Install telescope.nvim, or use the isolated config: lum-nvim-dev (without --user-config)",
    vim.log.levels.ERROR
  )
  return
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
  vim.notify("lum dev: could not load the extension: " .. tostring(err), vim.log.levels.ERROR)
  return
end

vim.schedule(function()
  vim.notify(
    ("lum dev attached\n  :Telescope lum\n  binary: %s"):format(
      vim.fn.exepath("lum") ~= "" and vim.fn.exepath("lum") or "NOT ON PATH"
    ),
    vim.log.levels.INFO
  )
end)
