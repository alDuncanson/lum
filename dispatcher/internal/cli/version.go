package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	productversion "github.com/alDuncanson/lum/dispatcher/internal/version"
)

func versionCmd() *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print the Lum product version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if jsonOutput {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					Version string `json:"version"`
				}{Version: productversion.Value})
			}
			fmt.Fprintln(cmd.OutOrStdout(), productversion.Value)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit the version as JSON")
	return cmd
}
