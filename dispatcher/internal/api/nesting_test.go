package api

import (
	"strings"
	"testing"

	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
)

func sources(uris ...string) []catalog.Source {
	out := make([]catalog.Source, 0, len(uris))
	for i, uri := range uris {
		out = append(out, catalog.Source{ID: string(rune('a' + i)), Type: "localdir", URI: uri})
	}
	return out
}

func TestAddingASubdirectoryOfARegisteredSourceConflicts(t *testing.T) {
	_, reason, conflict := nestingConflict("/repo/dispatcher", sources("/repo"))
	if !conflict {
		t.Fatal("a subdirectory of an indexed source must be refused")
	}
	if !strings.Contains(reason, "--root /repo") {
		t.Fatalf("the message should point at the way to search it: %q", reason)
	}
}

func TestAddingAParentOfARegisteredSourceConflicts(t *testing.T) {
	_, reason, conflict := nestingConflict("/repo", sources("/repo/dispatcher"))
	if !conflict {
		t.Fatal("a directory containing an indexed source must be refused")
	}
	if !strings.Contains(reason, "lum remove /repo/dispatcher") {
		t.Fatalf("the message should name the way out: %q", reason)
	}
}

func TestReaddingTheSameDirectoryIsAllowed(t *testing.T) {
	// `lum search --root` re-registers on every search; refusing that would
	// break the picker.
	if _, _, conflict := nestingConflict("/repo", sources("/repo")); conflict {
		t.Fatal("re-adding the same directory must stay idempotent")
	}
	// Trailing separators and uncleaned paths are the same directory.
	if _, _, conflict := nestingConflict("/repo", sources("/repo/")); conflict {
		t.Fatal("a trailing slash is the same directory")
	}
}

func TestSiblingsWithASharedPrefixAreNotNested(t *testing.T) {
	// /repo/bar is not inside /repo/ba, though the string starts with it.
	for _, pair := range [][2]string{
		{"/repo/bar", "/repo/ba"},
		{"/repo-two", "/repo"},
		{"/a/b", "/a/c"},
	} {
		if _, _, conflict := nestingConflict(pair[0], sources(pair[1])); conflict {
			t.Errorf("%s and %s are siblings, not nested", pair[0], pair[1])
		}
	}
}

func TestUnrelatedSourcesCoexist(t *testing.T) {
	if _, _, conflict := nestingConflict("/b", sources("/a", "/c")); conflict {
		t.Fatal("unrelated directories must both be allowed")
	}
}
