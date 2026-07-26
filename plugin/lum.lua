-- Register :LumInstall at startup.
--
-- The command exists here rather than only in setup() because of when it is
-- needed: someone who has just added the plugin and has no lum binary yet.
-- Under a plugin manager that lazy-loads on a keymap, setup() has not run at
-- that point, so a command defined there would not exist until after the
-- thing it fixes has already failed.
--
-- Nothing else happens at startup. Requiring lum.install costs one small file
-- and touches no network, no daemon, and no state.

if vim.g.loaded_lum then
  return
end
vim.g.loaded_lum = true

require("lum.install").command()
