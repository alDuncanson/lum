package api

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
)

// Registering a directory inside — or containing — one already registered
// indexes the overlap twice.
//
// Documents are keyed on (source_id, uri), deliberately: two sources that see
// the same path own independent rows, which is what makes sources safe to add
// and delete without disturbing each other. The consequence is that the same
// file gets embedded twice, stored twice, and returned twice, under two
// document IDs that collapsing cannot merge because they really are two
// documents. Adding lum's own repository and then its dispatcher/ subtree took
// the index from 80 documents to 126 and put the same file in the results
// twice, with no warning at any point.
//
// So it is refused rather than warned about. A warning on a command whose
// output is two lines and scrolls past is a warning nobody reads, and the
// resulting duplication is silent afterwards and annoying to diagnose.
//
// Only for paths. Source types with no containment relationship — a future
// remote source — return no conflict.
func nestingConflict(candidate string, existing []catalog.Source) (catalog.Source, string, bool) {
	if !filepath.IsAbs(candidate) {
		return catalog.Source{}, "", false
	}
	candidate = filepath.Clean(candidate)
	for _, source := range existing {
		if !filepath.IsAbs(source.URI) {
			continue
		}
		registered := filepath.Clean(source.URI)
		switch {
		case registered == candidate:
			// Not a conflict: re-adding the same directory is how `--root`
			// registers idempotently on every search.
			return catalog.Source{}, "", false
		case isUnder(candidate, registered):
			return source, fmt.Sprintf(
				"%s is already indexed as part of %s; search it with `lum search --root %s`, or remove the parent first",
				candidate, registered, registered), true
		case isUnder(registered, candidate):
			return source, fmt.Sprintf(
				"%s contains %s, which is already indexed; remove it first with `lum remove %s`",
				candidate, registered, registered), true
		}
	}
	return catalog.Source{}, "", false
}

// isUnder reports whether path is inside dir. Compares whole components, so
// /a/bc is not under /a/b.
func isUnder(path, dir string) bool {
	if dir == path {
		return false
	}
	prefix := dir
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(path, prefix)
}
