// Package cli defines the lum command tree (cobra).
//
// Structure: `serve` runs the daemon; everything else is a client of it
// over HTTP (via internal/apiclient). Keeping the CLI dumb guarantees
// the REST API stays complete — if a feature isn't reachable over HTTP,
// the CLI can't have it either.
package cli

import "github.com/spf13/cobra"

// Root builds the top-level `lum` command.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "lum",
		Short: "Local-first semantic search over your own sources",
		Long: `lum ingests documents from registered sources (local directories today,
more source types tomorrow), embeds them locally, and serves semantic
search — over a CLI, a REST API, and MCP. All data stays on your machine.`,
		SilenceUsage: true,
		// main() prints the returned error; without this cobra would
		// print it a second time.
		SilenceErrors: true,
	}
	root.AddCommand(
		serveCmd(),
		addCmd(),
		searchCmd(),
		sourcesCmd(),
		statusCmd(),
		mcpCmd(),
	)
	return root
}
