package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

// TypeLocalDir is the catalog identifier for local directory sources.
const TypeLocalDir = "localdir"

// indexableExtensions maps file extensions to MIME types the data plane
// can parse. Extending format support is a two-step change: add the
// extension → MIME mapping here, and (if it's a new MIME type) a Parser
// in data-plane/src/pipeline/parser.rs.
var indexableExtensions = map[string]string{
	".txt":      "text/plain",
	".text":     "text/plain",
	".md":       "text/markdown",
	".markdown": "text/markdown",
}

// LocalDir indexes a directory tree on the local filesystem.
type LocalDir struct {
	root string
}

// NewLocalDir validates that root exists and is a directory.
func NewLocalDir(root string) (*LocalDir, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolving source directory: %w", err)
	}
	root = filepath.Clean(abs)
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("source directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("source %q is not a directory", root)
	}
	return &LocalDir{root: root}, nil
}

func (l *LocalDir) Type() string { return TypeLocalDir }

// Scan walks the tree and returns a ref for every indexable file.
//
// Hashing note: we read every file to fingerprint its content, which is
// simple and exact. For very large trees, a cheaper first-pass
// fingerprint (size + mtime) with hashing only on suspicion of change
// is the classic optimization — a good future exercise.
func (l *LocalDir) Scan(ctx context.Context) ([]DocumentRef, error) {
	var refs []DocumentRef
	err := filepath.WalkDir(l.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Skip unreadable entries rather than failing the whole
			// scan; one bad permission shouldn't hide every document.
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			// Don't descend into hidden directories (.git, .obsidian,
			// node_modules-style caches are out of scope for v1).
			if hiddenDirectory(l.root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		mime, ok := indexableExtensions[strings.ToLower(filepath.Ext(path))]
		if !ok {
			return nil
		}
		hash, err := hashFile(path)
		if err != nil {
			return nil // racing deletes etc.; skip
		}
		refs = append(refs, DocumentRef{URI: path, MimeType: mime, ContentHash: hash})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanning %s: %w", l.root, err)
	}
	return refs, nil
}

// Watch reports index-relevant filesystem changes under the directory tree.
// fsnotify watches are not recursive, so every visible directory is added and
// newly created subtrees are added as they appear.
func (l *LocalDir) Watch(ctx context.Context) (<-chan struct{}, <-chan error, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, err
	}
	// Install the parent sentinel before walking the tree so a concurrent root
	// rename cannot move all directory handles before the sentinel exists.
	if parent := filepath.Dir(l.root); parent != l.root {
		if err := watcher.Add(parent); err != nil {
			_ = watcher.Close()
			return nil, nil, fmt.Errorf("watching source parent %s: %w", parent, err)
		}
	}
	directories := make(map[string]struct{})
	if err := l.addWatchTree(watcher, l.root, directories); err != nil {
		_ = watcher.Close()
		return nil, nil, err
	}
	changes := make(chan struct{}, 1)
	failures := make(chan error, 1)
	go func() {
		defer close(changes)
		defer close(failures)
		defer watcher.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				select {
				case failures <- err:
				default:
				}
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Name == l.root && (event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename)) {
					return
				}
				if event.Name != l.root && !strings.HasPrefix(event.Name, l.root+string(os.PathSeparator)) {
					continue
				}
				_, isDirectory := directories[event.Name]
				if event.Has(fsnotify.Create) {
					if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
						if hiddenDirectory(l.root, event.Name, info.Name()) {
							continue
						}
						isDirectory = true
						if err := l.addWatchTree(watcher, event.Name, directories); err != nil {
							select {
							case failures <- err:
							default:
							}
						}
					}
				}
				if isDirectory && event.Has(fsnotify.Rename) {
					// Path-based Remove is unreliable after rename on Windows.
					// Closing releases the entire tree; the consumer activates
					// its authoritative periodic-scan fallback.
					return
				}
				if isDirectory && event.Has(fsnotify.Remove) {
					for path := range directories {
						if path == event.Name || strings.HasPrefix(path, event.Name+string(os.PathSeparator)) {
							if err := watcher.Remove(path); err != nil && !errors.Is(err, fsnotify.ErrNonExistentWatch) {
								select {
								case failures <- err:
								default:
								}
							}
							delete(directories, path)
						}
					}
				}
				if l.relevantWatchEvent(event, isDirectory) {
					select {
					case changes <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return changes, failures, nil
}

func (l *LocalDir) addWatchTree(watcher *fsnotify.Watcher, root string, directories map[string]struct{}) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if hiddenDirectory(l.root, path, d.Name()) {
			return filepath.SkipDir
		}
		if err := watcher.Add(path); err != nil {
			return fmt.Errorf("watching %s: %w", path, err)
		}
		directories[path] = struct{}{}
		return nil
	})
}

func (l *LocalDir) relevantWatchEvent(event fsnotify.Event, isDirectory bool) bool {
	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
		return false
	}
	if isDirectory {
		return true
	}
	if _, ok := indexableExtensions[strings.ToLower(filepath.Ext(event.Name))]; ok {
		return true
	}
	return false
}

func hiddenDirectory(root, path, name string) bool {
	return path != root && strings.HasPrefix(name, ".")
}

func (l *LocalDir) Read(_ context.Context, ref DocumentRef) ([]byte, error) {
	return os.ReadFile(ref.URI)
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
