-- Telescope extension registration for lum semantic search.
local lum = require("lum.telescope")

return require("telescope").register_extension({
  setup = lum.setup,
  exports = {
    lum = lum.lum,
  },
})
