package cli

import (
	"bytes"
	"encoding/json"
	"testing"

	productversion "github.com/alDuncanson/lum/dispatcher/internal/version"
)

func TestVersionCommandJSON(t *testing.T) {
	original := productversion.Value
	productversion.Value = "1.2.3-test"
	t.Cleanup(func() { productversion.Value = original })

	cmd := versionCmd()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs([]string{"--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}

	var got struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(&stdout).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Version != "1.2.3-test" {
		t.Fatalf("version = %q, want %q", got.Version, "1.2.3-test")
	}
}
