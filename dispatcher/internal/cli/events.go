package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/alDuncanson/lum/dispatcher/internal/apiclient"
)

// eventsCmd streams the event bus as newline-delimited JSON.
//
// `lum top` already renders these events, but a TUI is not something another
// program can consume. This is the same stream in the plainest possible
// shape: one JSON object per line, flushed as it arrives, so `lum events |
// jq` works and so does a Neovim job — no SSE parser and no curl dependency
// for clients that already have the lum binary.
//
// It is a pure REST client like every other command, and starts the daemon
// on demand. Deliberately so: an integration that watches lum should not
// need a privileged channel that curl users lack.
func eventsCmd() *cobra.Command {
	var types []string
	var kinds bool
	var noReplay bool

	cmd := &cobra.Command{
		Use:   "events",
		Short: "Stream indexing and worker activity as newline-delimited JSON",
		Long: `Stream lum's internal event bus as newline-delimited JSON.

One event per line, written as it happens, until interrupted. Use --types to
narrow the stream server-side, which is cheaper than filtering afterwards.

  lum events
  lum events --types scan_started,scan_finished
  lum events | jq -r 'select(.kind == "document_failed") | .uri'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if kinds {
				for _, kind := range apiclient.EventKinds() {
					fmt.Fprintln(cmd.OutOrStdout(), kind)
				}
				return nil
			}

			client := apiclient.New()
			next := client.Events
			if noReplay {
				next = client.EventsLive
			}
			stream, err := next(cmd.Context(), types)
			if err != nil {
				return fmt.Errorf("connecting to lum: %w", err)
			}
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetEscapeHTML(false)
			for event := range stream {
				if err := encoder.Encode(event); err != nil {
					// A closed pipe is how `lum events | head` ends; that is
					// the caller being done, not a failure worth reporting.
					if errors.Is(err, os.ErrClosed) || strings.Contains(err.Error(), "broken pipe") {
						return nil
					}
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringSliceVar(&types, "types", nil,
		"only stream these event kinds (comma-separated; see --kinds)")
	cmd.Flags().BoolVar(&noReplay, "no-replay", false,
		"skip recent history and stream only events from now on")
	cmd.Flags().BoolVar(&kinds, "kinds", false, "list the event kinds and exit")
	return cmd
}
