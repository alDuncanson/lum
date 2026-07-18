package cli

import (
	"github.com/spf13/cobra"

	"github.com/alDuncanson/lum/control-plane/internal/mcpserver"
)

// Version is the version string reported to MCP clients. Bumped by
// hand for now; a release process could stamp it via -ldflags later.
const Version = "0.1.0"

// mcpCmd serves the Model Context Protocol over stdio so local AI
// agents (Claude Desktop, Amp, ...) can call lum as a set of tools.
//
// MCP clients spawn this command themselves — you configure the client
// with `command: lum, args: [mcp]` rather than running it by hand. It
// is a thin adapter over the REST API, so `lum serve` must be running.
func mcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Serve the Model Context Protocol over stdio (for local AI agents)",
		Long: `Serve the Model Context Protocol over stdio.

Exposes lum to local AI agents as MCP tools: search, add_source,
list_sources, and status. This command is normally spawned by an MCP
client (configure it with command "lum" and args ["mcp"]), and it
requires a running daemon (` + "`lum serve`" + `) to talk to.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return mcpserver.Run(cmd.Context(), Version)
		},
	}
}
