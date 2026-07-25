package source

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCanonicalizesDirectorySymlink(t *testing.T) {
	real := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "workspace-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	_, realURI, err := Resolve(real)
	if err != nil {
		t.Fatal(err)
	}
	_, linkURI, err := Resolve(link)
	if err != nil {
		t.Fatal(err)
	}
	if linkURI != realURI {
		t.Fatalf("symlink URI = %q, real URI = %q", linkURI, realURI)
	}
}
