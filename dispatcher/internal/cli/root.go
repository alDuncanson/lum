// Package cli defines the lum command tree (cobra).
//
// Structure: `serve` runs the daemon; everything else is a client of it
// over HTTP (via internal/apiclient). Keeping the CLI dumb guarantees
// the REST API stays complete — if a feature isn't reachable over HTTP,
// the CLI can't have it either.
package cli

import (
	"github.com/spf13/cobra"

	productversion "github.com/alDuncanson/lum/dispatcher/internal/version"
)

// Root builds the top-level `lum` command.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "lum",
		Short: "Local semantic code search for repositories",
		Long: `Lum indexes repositories locally and searches them by meaning from the
CLI, Neovim, REST API, or MCP. Source code, embeddings, and the index stay
on your machine.`,
		// Both spellings: `lum version` for the subcommand people discover in
		// --help, and `--version` for the flag they type without looking.
		Version:      productversion.Value,
		SilenceUsage: true,
		// main() prints the returned error; without this cobra would
		// print it a second time.
		SilenceErrors: true,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.AddCommand(
		serveCmd(),
		addCmd(),
		searchCmd(),
		sourcesCmd(),
		statusCmd(),
		topCmd(),
		eventsCmd(),
		stopCmd(),
		mcpCmd(),
		versionCmd(),
		removeCmd(),
	)
	return root
}
