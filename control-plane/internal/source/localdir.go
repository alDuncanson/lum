package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
			if d.Name() != "." && strings.HasPrefix(d.Name(), ".") && path != l.root {
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
