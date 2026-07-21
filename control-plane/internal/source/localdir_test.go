package source

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestScanSkipsUnreadableSubtreeButLogsAndKeepsOtherFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission bits behave differently on windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root ignores permission bits, so this can't force a ReadDir failure")
	}

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "readable.md"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(root, "locked")
	if err := os.Mkdir(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "hidden.md"), []byte("hidden"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) }) // let TempDir cleanup remove it

	var logs bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(original) })

	src, err := NewLocalDir(root)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := src.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan returned an error for one unreadable subtree, want it skipped: %v", err)
	}
	if len(refs) != 1 || refs[0].URI != filepath.Join(root, "readable.md") {
		t.Fatalf("refs = %+v, want only readable.md", refs)
	}
	if !strings.Contains(logs.String(), "skipping unreadable path") || !strings.Contains(logs.String(), locked) {
		t.Fatalf("log output = %q, want a warning naming the unreadable path", logs.String())
	}
}
