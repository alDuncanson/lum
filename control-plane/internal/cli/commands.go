package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/alDuncanson/lum/control-plane/internal/apiclient"
)

// addCmd registers a new source: `lum add ~/Documents`.
func addCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <uri>",
		Short: "Register a source (a local directory) and start indexing it",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := apiclient.New().AddSource(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if resp.Created {
				fmt.Printf("added %s source %s\n", resp.Source.Type, resp.Source.URI)
			} else {
				fmt.Printf("source %s already registered\n", resp.Source.URI)
			}
			fmt.Println("indexing in the background — check progress with `lum status`")
			return nil
		},
	}
}

// searchCmd queries the index: `lum search "how do I rotate keys"`.
func searchCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Semantic search across everything lum has indexed",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.Join(args, " ")
			results, err := apiclient.New().Search(cmd.Context(), query, limit)
			if err != nil {
				return err
			}
			if len(results) == 0 {
				fmt.Println("no results")
				return nil
			}
			for i, r := range results {
				fmt.Printf("%2d. %.3f  %s (chunk %d)\n", i+1, r.Score, r.URI, r.ChunkIndex)
				fmt.Printf("    %s\n\n", snippet(r.Text, 240))
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum results to return")
	return cmd
}

// sourcesCmd lists registered sources.
func sourcesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sources",
		Short: "List registered sources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			sources, err := apiclient.New().ListSources(cmd.Context())
			if err != nil {
				return err
			}
			if len(sources) == 0 {
				fmt.Println("no sources yet — add one with `lum add <directory>`")
				return nil
			}
			for _, s := range sources {
				fmt.Printf("%s  %-9s %s\n", s.ID, s.Type, s.URI)
			}
			return nil
		},
	}
}

// statusCmd reports daemon, data plane, and index stats.
func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show daemon health and index statistics",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := apiclient.New().Status(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Printf("daemon:      %s\n", resp.Daemon)
			fmt.Printf("data plane:  %s (%s)\n", resp.DataPlane, resp.Detail)
			fmt.Printf("sources:     %d\n", resp.Stats.Sources)
			fmt.Printf("documents:   %d\n", resp.Stats.Documents)
			fmt.Printf("chunks:      %d\n", resp.Stats.Chunks)
			return nil
		},
	}
}

// snippet trims chunk text to a single displayable line.
func snippet(text string, max int) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > max {
		text = text[:max] + "…"
	}
	return text
}
