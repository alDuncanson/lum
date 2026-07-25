package config

import (
	"strings"
	"testing"
)

func validConfig(dataDir string) Config {
	return Config{DataDir: dataDir, EmbeddingModel: DefaultEmbeddingModel}
}

func TestValidateAcceptsAReasonableDataDir(t *testing.T) {
	if err := validConfig("/tmp/lum").Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

// A data directory deep enough to push the socket past sun_path used to
// surface only as the worker exiting with "path must be shorter than
// SUN_LEN" while the dispatcher reported it as idle — indistinguishable
// from a deliberate memory shed.
func TestValidateRejectsADataDirTooDeepForTheWorkerSocket(t *testing.T) {
	cfg := validConfig("/" + strings.Repeat("a", maxSocketPathLen))
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate() = nil, want an error for socket path %q (%d bytes, max %d)",
			cfg.GRPCSocketPath(), len(cfg.GRPCSocketPath()), maxSocketPathLen)
	}
	// The message has to name the knob that fixes it, or it is no better
	// than the error it replaced.
	if !strings.Contains(err.Error(), "LUM_DATA_DIR") {
		t.Errorf("error %q does not mention LUM_DATA_DIR", err)
	}
}

// The boundary is worth pinning: off by one here either rejects a data dir
// that works or admits one that dies at bind time.
func TestValidateSocketPathBoundary(t *testing.T) {
	// What filepath.Join appends to DataDir to form the socket path.
	suffix := len("/lum-worker.sock")
	for _, tc := range []struct {
		name      string
		socketLen int
		wantErr   bool
	}{
		{"exactly at the limit", maxSocketPathLen, false},
		{"one over the limit", maxSocketPathLen + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Leading "/" accounts for one of the data dir's bytes.
			cfg := validConfig("/" + strings.Repeat("a", tc.socketLen-suffix-1))
			socket := cfg.GRPCSocketPath()
			if len(socket) != tc.socketLen {
				t.Fatalf("test setup produced a %d-byte socket path, want %d", len(socket), tc.socketLen)
			}
			err := cfg.validateSocketPath()
			if tc.wantErr && err == nil {
				t.Errorf("validateSocketPath() = nil for a %d-byte path, want an error", len(socket))
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateSocketPath() = %v for a %d-byte path, want nil", err, len(socket))
			}
		})
	}
}

// A relative DataDir is resolved before measuring, since that is the path
// Dial hands the kernel.
func TestValidateMeasuresTheResolvedSocketPath(t *testing.T) {
	if err := validConfig(strings.Repeat("a", maxSocketPathLen)).Validate(); err == nil {
		t.Fatal("Validate() = nil for a long relative data dir, want an error")
	}
}

func TestValidateRejectsAnUnknownEmbeddingModel(t *testing.T) {
	cfg := validConfig("/tmp/lum")
	cfg.EmbeddingModel = "gigantic"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil for an unknown embedding model, want an error")
	}
	if !strings.Contains(err.Error(), "gigantic") {
		t.Errorf("error %q does not name the offending value", err)
	}
}

func TestValidateAcceptsBothEmbeddingModels(t *testing.T) {
	for _, model := range []string{"standard", "quantized"} {
		cfg := validConfig("/tmp/lum")
		cfg.EmbeddingModel = model
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() = %v for model %q, want nil", err, model)
		}
	}
}
