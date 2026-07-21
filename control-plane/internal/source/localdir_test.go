package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalDirWatchTracksNewSubdirectories(t *testing.T) {
	root := t.TempDir()
	src, err := NewLocalDir(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes, failures, err := src.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	subdir := filepath.Join(root, "notes")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	waitForWatchChange(t, changes, failures)

	if err := os.WriteFile(filepath.Join(subdir, "live.md"), []byte("live"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForWatchChange(t, changes, failures)
}

func waitForWatchChange(t *testing.T, changes <-chan struct{}, failures <-chan error) {
	t.Helper()
	select {
	case _, ok := <-changes:
		if !ok {
			t.Fatal("filesystem change channel closed")
		}
	case err, ok := <-failures:
		if !ok {
			t.Fatal("filesystem failure channel closed")
		}
		t.Fatalf("watch failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for filesystem change")
	}
}

func TestLocalDirWatchStopsWhenRootIsRemoved(t *testing.T) {
	root := t.TempDir()
	src, err := NewLocalDir(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes, _, err := src.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-changes:
		if ok {
			t.Fatal("received change instead of watcher shutdown")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watcher did not stop after source root removal")
	}
}

func TestLocalDirWatchIgnoresHiddenDirectories(t *testing.T) {
	root := t.TempDir()
	src, err := NewLocalDir(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes, failures, err := src.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changes:
		t.Fatal("hidden directory triggered a change")
	case err := <-failures:
		t.Fatalf("watch failed: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}
