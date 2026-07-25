package source

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	dotignore "github.com/codeglyph/go-dotignore/v2"
	"github.com/fsnotify/fsnotify"
)

// TypeLocalDir is the catalog identifier for local directory sources.
const TypeLocalDir = "localdir"

// indexableExtensions maps file extensions to MIME types the worker
// can parse. Extending format support is a two-step change: add the
// extension → MIME mapping here, and (if it's a new MIME type) a Parser
// in worker/src/pipeline/parser.rs.
var indexableExtensions = map[string]string{
	".txt":      "text/plain",
	".text":     "text/plain",
	".md":       "text/markdown",
	".markdown": "text/markdown",
	".go":       "text/x-go",
	".rs":       "text/x-rust",
	".lua":      "text/x-lua",
	".nix":      "text/x-nix",
	".py":       "text/x-python",
	".pyi":      "text/x-python",
	".js":       "text/javascript",
	".mjs":      "text/javascript",
	".cjs":      "text/javascript",
	".jsx":      "text/jsx",
	".ts":       "text/typescript",
	".mts":      "text/typescript",
	".cts":      "text/typescript",
	".tsx":      "text/tsx",
	".java":     "text/x-java-source",
	".kt":       "text/x-kotlin",
	".kts":      "text/x-kotlin",
	".c":        "text/x-c",
	".h":        "text/x-c",
	".cc":       "text/x-c++",
	".cpp":      "text/x-c++",
	".cxx":      "text/x-c++",
	".hh":       "text/x-c++",
	".hpp":      "text/x-c++",
	".hxx":      "text/x-c++",
	".cs":       "text/x-csharp",
	".rb":       "text/x-ruby",
	".php":      "text/x-php",
	".swift":    "text/x-swift",
	".scala":    "text/x-scala",
	".sc":       "text/x-scala",
	".sh":       "text/x-shellscript",
	".bash":     "text/x-shellscript",
	".zsh":      "text/x-shellscript",
	".fish":     "text/x-shellscript",
	".sql":      "text/x-sql",
	".yaml":     "text/yaml",
	".yml":      "text/yaml",
	".toml":     "text/x-toml",
	".json":     "text/json",
	".jsonc":    "text/json",
	".html":     "text/html",
	".htm":      "text/html",
	".css":      "text/css",
	".scss":     "text/x-scss",
	".sass":     "text/x-sass",
	".less":     "text/x-less",
	".proto":    "text/x-protobuf",
	".xml":      "text/xml",
	".svg":      "text/xml",
}

// defaultExcludedDirectories are skipped by name wherever they appear,
// independent of `.gitignore`.
//
// Honoring `.gitignore` already covers well-kept repositories, but it is
// not a safety net: a repository that forgets to ignore its dependency
// tree makes Lum walk, read, and embed thousands of files nobody wants to
// search, and node_modules in particular is mostly indexable extensions.
//
// The list is deliberately short and limited to names that are generated
// or vendored by universal convention. Riskier candidates (build, dist,
// out, bin) are left off: a repository can plausibly keep real sources
// there, and silently skipping sources is worse than indexing junk.
// LUM_EXCLUDE_DIRS replaces this list for anyone who disagrees.
var defaultExcludedDirectories = []string{
	"node_modules", // npm/yarn/pnpm dependencies
	"vendor",       // Go, PHP composer, Ruby bundler
	"target",       // Cargo, Maven, sbt
	"__pycache__",  // CPython bytecode
}

// excludedDirectories is resolved once at process start. LUM_EXCLUDE_DIRS
// replaces (rather than extends) the defaults, so the effective list is
// always exactly what the variable says; setting it empty disables
// name-based exclusion entirely and leaves only `.gitignore` and hidden
// directories.
var excludedDirectories = loadExcludedDirectories(os.LookupEnv)

// loadExcludedDirectories takes a lookup func rather than reading the
// environment directly so tests can cover the unset / empty / listed cases;
// LookupEnv rather than Getenv because "set but empty" means "exclude
// nothing", which is distinct from "unset".
func loadExcludedDirectories(lookup func(string) (string, bool)) map[string]struct{} {
	names := defaultExcludedDirectories
	if raw, set := lookup("LUM_EXCLUDE_DIRS"); set {
		names = nil
		for _, name := range strings.Split(raw, ",") {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, name)
			}
		}
	}
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
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

// fingerprintRaceWindow is how recently a file must have been modified for
// its size+mtime fingerprint to be considered untrustworthy.
//
// The hole a size+mtime fingerprint has is the one git calls "racily
// clean": if a file is modified again inside the same mtime tick we
// observed it in, and its size does not change, the fingerprint is
// identical and the edit is invisible. Some filesystems (HFS+, a few
// network mounts) only keep whole-second mtimes, which makes that window
// wide enough to hit in practice — an editor writing twice in a second, or
// a scripted in-place substitution that preserves length.
//
// So Scan hashes anything modified within this window and lets the
// fingerprint stand only for files that have been quiet longer than any
// plausible mtime granularity. The cost is bounded by how many files
// changed in the last couple of seconds, which is approximately zero on
// every scan except the ones immediately following an edit.
const fingerprintRaceWindow = 2 * time.Second

// Scan walks the tree and returns a ref for every indexable file.
//
// Scans are frequent — startup, every debounced watch event, a periodic
// fallback ticker, and every ingest retry — so Scan is deliberately
// stat-only in the common case. It reports a size+mtime fingerprint and
// leaves ContentHash empty; the ingest planner reads and hashes only the
// documents whose fingerprint moved. Previously this hashed the entire
// tree on every scan, which made watching a large repository cost a full
// read of it per change.
func (l *LocalDir) Scan(ctx context.Context) ([]DocumentRef, error) {
	var refs []DocumentRef
	ignores, err := dotignore.NewRepositoryMatcher(l.root)
	if err != nil {
		return nil, fmt.Errorf("loading gitignore rules: %w", err)
	}
	err = filepath.WalkDir(l.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Skip unreadable entries rather than failing the whole
			// scan; one bad permission shouldn't hide every document.
			// But do surface it — a silently skipped subtree, given no
			// other errors, is hard to notice let alone diagnose.
			slog.Warn("skipping unreadable path during scan", "path", path, "error", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			// Hidden trees (.git, .obsidian) and generated trees
			// (node_modules, vendor, ...) are never indexed.
			if skipDirectory(l.root, path, d.Name()) {
				return filepath.SkipDir
			}
			ignored, matchErr := ignoredPath(ignores, path, true)
			if matchErr != nil {
				return matchErr
			}
			if ignored {
				return filepath.SkipDir
			}
			return nil
		}
		ignored, matchErr := ignoredPath(ignores, path, false)
		if matchErr != nil {
			return matchErr
		}
		if ignored {
			return nil
		}
		mime, ok := indexableExtensions[strings.ToLower(filepath.Ext(path))]
		if !ok {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil // racing deletes etc.; skip
		}
		ref := DocumentRef{URI: path, MimeType: mime, Fingerprint: fingerprint(info)}
		if time.Since(info.ModTime()) < fingerprintRaceWindow {
			// Too fresh to trust a fingerprint; pay for the read now.
			hash, err := hashFile(path)
			if err != nil {
				return nil
			}
			ref.ContentHash = hash
		}
		refs = append(refs, ref)
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
	ignores, err := dotignore.NewRepositoryMatcher(l.root)
	if err != nil {
		_ = watcher.Close()
		return nil, nil, fmt.Errorf("loading gitignore rules: %w", err)
	}
	if err := l.addWatchTree(watcher, l.root, directories, ignores); err != nil {
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
				isIgnoreFile := filepath.Base(event.Name) == ".gitignore"
				_, isDirectory := directories[event.Name]
				if event.Has(fsnotify.Create) {
					if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
						if skipDirectory(l.root, event.Name, info.Name()) {
							continue
						}
						isDirectory = true
						ignored, matchErr := ignoredPath(ignores, event.Name, true)
						if matchErr == nil && ignored {
							continue
						}
						if err := l.addWatchTree(watcher, event.Name, directories, ignores); err != nil {
							select {
							case failures <- err:
							default:
							}
						}
					}
				}
				if !isIgnoreFile && !isDirectory {
					ignored, matchErr := ignoredPath(ignores, event.Name, false)
					if matchErr == nil && ignored {
						continue
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
				if isIgnoreFile {
					if refreshed, refreshErr := dotignore.NewRepositoryMatcher(l.root); refreshErr == nil {
						ignores = refreshed
						if err := l.addWatchTree(watcher, l.root, directories, ignores); err != nil {
							select {
							case failures <- err:
							default:
							}
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

func (l *LocalDir) addWatchTree(watcher *fsnotify.Watcher, root string, directories map[string]struct{}, ignores *dotignore.RepositoryMatcher) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if skipDirectory(l.root, path, d.Name()) {
			return filepath.SkipDir
		}
		ignored, matchErr := ignoredPath(ignores, path, true)
		if matchErr != nil {
			return matchErr
		}
		if ignored {
			return filepath.SkipDir
		}
		if _, watched := directories[path]; !watched {
			if err := watcher.Add(path); err != nil {
				return fmt.Errorf("watching %s: %w", path, err)
			}
			directories[path] = struct{}{}
		}
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
	if filepath.Base(event.Name) == ".gitignore" {
		return true
	}
	if _, ok := indexableExtensions[strings.ToLower(filepath.Ext(event.Name))]; ok {
		return true
	}
	return false
}

func ignoredPath(ignores *dotignore.RepositoryMatcher, path string, directory bool) (bool, error) {
	if directory {
		path += string(os.PathSeparator)
	}
	return ignores.Matches(path)
}

// skipDirectory reports whether a directory should be excluded from both
// scans and watches. Scans and watches must agree: a directory watched but
// not scanned produces change events that reconcile to nothing, and one
// scanned but not watched goes stale until the next periodic rescan.
//
// The root itself is never skipped — `lum add ~/code/vendor` should index
// that directory, and the name only means "generated" relative to a
// repository containing it.
func skipDirectory(root, path, name string) bool {
	if path == root {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return true
	}
	_, excluded := excludedDirectories[name]
	return excluded
}

func (l *LocalDir) Read(_ context.Context, ref DocumentRef) ([]byte, error) {
	return os.ReadFile(ref.URI)
}

// fingerprint is the cheap change signal for a local file: size plus
// mtime in nanoseconds. Both come from the stat the directory walk already
// performed, so producing it costs nothing beyond formatting.
//
// Size is included because mtime alone is the weaker signal — a write that
// lands in the same mtime tick is common, whereas one that also preserves
// length is much less so.
func fingerprint(info fs.FileInfo) string {
	return fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano())
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
