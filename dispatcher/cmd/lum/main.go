// Command lum is the single user-facing binary of the lum system.
//
// It plays two roles depending on the subcommand:
//
//   - `lum serve` runs the long-lived dispatcher daemon (lumd): it
//     spawns and supervises the Rust worker (lum-worker), owns the SQLite
//     catalog, and exposes the local HTTP API that everything else talks
//     to.
//
//   - every other subcommand (`add`, `search`, `sources`, `status`,
//     `mcp`) is a thin HTTP client of that API. The CLI holds no state
//     and contains no business logic — by design, anything the CLI can
//     do, any other API client (an MCP agent, curl, a future TUI) can
//     do identically. `lum mcp` extends that same API to AI agents over
//     the Model Context Protocol.
package main

import (
	"fmt"
	"os"

	"github.com/alDuncanson/lum/dispatcher/internal/cli"
)

func main() {
	if err := cli.Root().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
