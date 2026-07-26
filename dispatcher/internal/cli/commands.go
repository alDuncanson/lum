package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/alDuncanson/lum/dispatcher/internal/apiclient"
	"github.com/alDuncanson/lum/dispatcher/internal/apiv1"
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
	var sourceID string
	var root string
	var jsonOutput bool
	var jsonl bool
	var perFile int
	var noTests bool

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Semantic search across everything lum has indexed",
		Args:  cobra.MinimumNArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			if jsonOutput && jsonl {
				return errors.New("--json and --jsonl are mutually exclusive")
			}
			if root != "" && sourceID != "" {
				return errors.New("--root and --source cannot be combined")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.Join(args, " ")
			api := apiclient.New()
			if root != "" {
				ensured, err := api.EnsureSource(cmd.Context(), root)
				if err != nil {
					return err
				}
				sourceID = ensured.Source.ID
			}
			opts := apiclient.SearchOptions{Limit: limit, SourceID: sourceID, ExcludeTests: noTests}
			if cmd.Flags().Changed("per-file") {
				opts.PerFile = &perFile
			}
			results, err := api.SearchWith(cmd.Context(), query, opts)
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetEscapeHTML(false)
			if jsonOutput {
				return encoder.Encode(apiv1.SearchEnvelope{Query: query, Results: results})
			}
			if jsonl {
				for _, result := range results {
					if err := encoder.Encode(result); err != nil {
						return err
					}
				}
				return nil
			}
			if len(results) == 0 {
				fmt.Println("no results")
				return nil
			}
			for i, r := range results {
				location := r.URI
				if r.StartLine > 0 {
					location = fmt.Sprintf("%s:%d", location, r.StartLine)
				}
				fmt.Printf("%2d. %.3f  %s (chunk %d)\n", i+1, r.Score, location, r.ChunkIndex)
				fmt.Printf("    %s\n\n", snippet(r.Text, 240))
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 10, "maximum results to return")
	cmd.Flags().StringVar(&sourceID, "source", "", "restrict results to one source ID (see `lum sources`)")
	cmd.Flags().StringVar(&root, "root", "", "ensure and search only this local workspace")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit a compact JSON search envelope")
	cmd.Flags().BoolVar(&jsonl, "jsonl", false, "emit one compact JSON result per line")
	cmd.Flags().IntVar(&perFile, "per-file", 2, "chunks any one file may contribute; 0 returns raw nearest neighbours")
	cmd.Flags().BoolVar(&noTests, "no-tests", false, "omit test files from results")
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

// statusCmd reports daemon, worker, and index stats.
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
			fmt.Printf("worker:      %s (%s)\n", resp.Worker, resp.Detail)
			fmt.Printf("sources:     %d\n", resp.Stats.Sources)
			fmt.Printf("documents:   %d\n", resp.Stats.Documents)
			fmt.Printf("chunks:      %d\n", resp.Stats.Chunks)
			fmt.Printf("failures:    %d\n", resp.Stats.Failures)
			for _, failure := range resp.Failures {
				fmt.Printf("  %s (attempts: %d): %s\n", failure.URI, failure.Attempts, failure.Error)
			}
			return nil
		},
	}
}

// stopCmd requests a graceful shutdown of the running daemon, if any.
// Unlike every other command, it must never auto-spawn a daemon on a
// refused connection (#13) — there being nothing to stop is success, not
// a reason to start one.
func stopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the running lum daemon, if any",
		RunE: func(cmd *cobra.Command, _ []string) error {
			err := apiclient.New().Stop(cmd.Context())
			if errors.Is(err, apiclient.ErrNoDaemonRunning) {
				fmt.Println("no lum daemon is running")
				return nil
			}
			if err != nil {
				return err
			}
			fmt.Println("daemon stopped")
			return nil
		},
	}
}

// snippet trims chunk text to a single displayable line.
func snippet(text string, max int) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > max {
		// Cut on a rune boundary: chunk text is source, and source is full
		// of comments containing characters a byte slice would cut in half.
		cut := max
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "…"
	}
	return text
}
