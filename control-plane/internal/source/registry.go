package source

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolve maps a user-supplied URI (the argument of `lum add`) to a
// Source implementation. This is the single dispatch point for source
// types: to teach lum a new scheme, add a case here and implement the
// Source interface.
//
// Examples:
//
//	~/Documents             → localdir
//	/var/notes              → localdir
//	https://x.com/feed.xml  → (future) rss
func Resolve(uri string) (Source, string, error) {
	switch {
	case strings.HasPrefix(uri, "http://"), strings.HasPrefix(uri, "https://"):
		return nil, "", fmt.Errorf("remote sources are not implemented yet (got %q)", uri)
	default:
		// Treat anything else as a local directory path.
		path, err := normalizePath(uri)
		if err != nil {
			return nil, "", err
		}
		src, err := NewLocalDir(path)
		return src, path, err
	}
}

// FromCatalog reconstructs a Source from its persisted (type, uri) pair
// when the daemon restarts.
func FromCatalog(sourceType, uri string) (Source, error) {
	switch sourceType {
	case TypeLocalDir:
		return NewLocalDir(uri)
	default:
		return nil, fmt.Errorf("unknown source type %q in catalog", sourceType)
	}
}

// normalizePath expands ~ and resolves to an absolute path so that the
// same directory always produces the same canonical source URI.
func normalizePath(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expanding ~: %w", err)
		}
		p = filepath.Join(home, strings.TrimPrefix(p, "~"))
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolving path %q: %w", p, err)
	}
	return abs, nil
}
