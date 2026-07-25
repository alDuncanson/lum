//go:build eval

// Package eval measures retrieval quality against a fixture of questions
// with known-correct answers.
//
// It is behind a build tag because it is not a unit test: it needs a real
// daemon, a real embedding model, and a real index of this repository, and
// it takes as long as indexing takes. `go test ./...` and `nix flake check`
// must stay fast and hermetic, so neither runs this.
//
//	go test -tags eval -v -timeout 30m ./internal/eval/
//	nix run .#eval            # builds lum first, uses its own data directory
//
// Why it exists: parsing, chunking, and embedding changes are judged on
// whether results "look better", which is exactly the judgement least
// available to whoever just made the change. These numbers are not precise
// — thirty questions is a small sample — but they are comparable between
// two runs, which is the only property required to tell an improvement from
// a preference.
package eval

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alDuncanson/lum/dispatcher/internal/apiclient"
	"github.com/alDuncanson/lum/dispatcher/internal/apiv1"
)

var (
	writeJSON = flag.String("eval.json", "", "write the full result set to this path, for diffing between runs")
	limit     = flag.Int("eval.limit", 10, "results to request per question")
	label     = flag.String("eval.label", "", "name this run in the output (e.g. a chunker version)")
)

// Question is one fixture entry. Files are repository-relative.
type Question struct {
	Q     string   `yaml:"q"`
	Files []string `yaml:"files"`
	// Contains is an optional substring that the matched chunk must include.
	// It measures what recall cannot: whether the chunk returned is the right
	// *part* of the right file.
	//
	// A substring rather than a line range, deliberately. Line numbers in a
	// living repository drift with every edit above them, so a line-based
	// answer key silently rots into a permanent miss; an identifier does not
	// move when code around it does.
	Contains string `yaml:"contains"`
}

type fixture struct {
	Questions []Question `yaml:"questions"`
}

// Outcome is one question's result, kept in the JSON output so a regression
// can be traced to the query that caused it rather than just a moved average.
type Outcome struct {
	Question      string   `json:"question"`
	Want          []string `json:"want"`
	Got           []string `json:"got"`
	FirstHitRank  int      `json:"first_hit_rank"` // 1-based; 0 means no hit
	DistinctFiles int      `json:"distinct_files_in_top5"`
	ChunkHit      *bool    `json:"chunk_hit,omitempty"`
}

// Report is the comparable artifact: the same shape every run, so two runs
// diff cleanly.
type Report struct {
	Label       string             `json:"label,omitempty"`
	RanAt       string             `json:"ran_at"`
	Questions   int                `json:"questions"`
	Documents   int                `json:"indexed_documents"`
	Chunks      int                `json:"indexed_chunks"`
	Recall      map[string]float64 `json:"recall"`
	MRR         float64            `json:"mrr"`
	ChunkHits   float64            `json:"chunk_hit_rate"`
	ChunkScored int                `json:"chunk_scored_questions"`
	DistinctAt5 float64            `json:"mean_distinct_files_in_top5"`
	Outcomes    []Outcome          `json:"outcomes"`
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func loadFixture(t *testing.T, root string) fixture {
	t.Helper()
	path := filepath.Join(root, "eval", "questions.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	var f fixture
	if err := yaml.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if len(f.Questions) == 0 {
		t.Fatalf("%s has no questions", path)
	}
	for i, q := range f.Questions {
		if q.Q == "" || len(q.Files) == 0 {
			t.Fatalf("question %d needs both a q and at least one file", i+1)
		}
		for _, file := range q.Files {
			if _, err := os.Stat(filepath.Join(root, file)); err != nil {
				t.Fatalf("question %q names %s, which does not exist — a stale answer key scores as a miss forever", q.Q, file)
			}
		}
		if q.Contains != "" && len(q.Files) != 1 {
			t.Fatalf("question %q: contains only makes sense with exactly one file", q.Q)
		}
	}
	return f
}

func TestRetrieval(t *testing.T) {
	root := repoRoot(t)
	f := loadFixture(t, root)
	ctx := context.Background()
	client := apiclient.New()

	t.Logf("indexing %s (this is the slow part; the model downloads once)", root)
	source, err := client.EnsureSource(ctx, root)
	if err != nil {
		t.Fatalf("indexing the repository: %v", err)
	}
	status, err := client.Status(ctx)
	if err != nil {
		t.Fatalf("reading status: %v", err)
	}
	t.Logf("index: %d documents, %d chunks", status.Stats.Documents, status.Stats.Chunks)

	report := Report{
		Label:     *label,
		RanAt:     time.Now().UTC().Format(time.RFC3339),
		Questions: len(f.Questions),
		Documents: status.Stats.Documents,
		Chunks:    status.Stats.Chunks,
		Recall:    map[string]float64{},
	}

	var reciprocalSum, distinctSum float64
	var chunkHits, chunkScored int
	hitsAt := map[int]int{1: 0, 5: 0, 10: 0}

	for _, question := range f.Questions {
		results, err := client.Search(ctx, question.Q, *limit, source.Source.ID)
		if err != nil {
			t.Fatalf("searching %q: %v", question.Q, err)
		}
		outcome := score(root, question, results, hitsAt)
		reciprocalSum += reciprocal(outcome.FirstHitRank)
		distinctSum += float64(outcome.DistinctFiles)
		if outcome.ChunkHit != nil {
			chunkScored++
			if *outcome.ChunkHit {
				chunkHits++
			}
		}
		report.Outcomes = append(report.Outcomes, outcome)
	}

	total := float64(len(f.Questions))
	for _, k := range []int{1, 5, 10} {
		report.Recall[fmt.Sprintf("@%d", k)] = float64(hitsAt[k]) / total
	}
	report.MRR = reciprocalSum / total
	report.DistinctAt5 = distinctSum / total
	report.ChunkScored = chunkScored
	if chunkScored > 0 {
		report.ChunkHits = float64(chunkHits) / float64(chunkScored)
	}

	printReport(t, report)
	if *writeJSON != "" {
		blob, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*writeJSON, append(blob, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", *writeJSON)
	}
}

// score evaluates one question's results. hitsAt is accumulated across
// questions for recall@k.
func score(root string, question Question, results []apiv1.SearchResult, hitsAt map[int]int) Outcome {
	want := make(map[string]bool, len(question.Files))
	for _, file := range question.Files {
		want[file] = true
	}

	outcome := Outcome{Question: question.Q, Want: question.Files}
	seen := map[string]bool{}
	for rank, result := range results {
		file := strings.TrimPrefix(strings.TrimPrefix(result.URI, root), "/")
		if len(outcome.Got) < 5 {
			outcome.Got = append(outcome.Got, fmt.Sprintf("%s:%d", file, result.StartLine))
		}
		if rank < 5 && !seen[file] {
			seen[file] = true
			outcome.DistinctFiles++
		}
		if !want[file] {
			continue
		}
		if outcome.FirstHitRank == 0 {
			outcome.FirstHitRank = rank + 1
			for _, k := range []int{1, 5, 10} {
				if rank < k {
					hitsAt[k]++
				}
			}
		}
		// Only counts within the requested window: a chunk nobody would
		// scroll to is not a useful answer.
		if question.Contains != "" && rank < 10 {
			hit := strings.Contains(result.Text, question.Contains)
			if outcome.ChunkHit == nil || (!*outcome.ChunkHit && hit) {
				outcome.ChunkHit = &hit
			}
		}
	}
	// A question whose file never matched still counts as a chunk miss
	// rather than going unscored.
	if question.Contains != "" && outcome.ChunkHit == nil {
		miss := false
		outcome.ChunkHit = &miss
	}
	return outcome
}

func reciprocal(rank int) float64 {
	if rank == 0 {
		return 0
	}
	return 1 / float64(rank)
}

func printReport(t *testing.T, report Report) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== retrieval over %d questions", report.Questions)
	if report.Label != "" {
		fmt.Fprintf(&b, " [%s]", report.Label)
	}
	fmt.Fprintf(&b, " ===\n")
	fmt.Fprintf(&b, "  index            %d documents, %d chunks\n", report.Documents, report.Chunks)
	keys := make([]string, 0, len(report.Recall))
	for k := range report.Recall {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "  recall%-10s %.3f\n", k, report.Recall[k])
	}
	fmt.Fprintf(&b, "  MRR              %.3f\n", report.MRR)
	if report.ChunkScored > 0 {
		fmt.Fprintf(&b, "  chunk hit rate   %.3f  (%d questions naming a required substring)\n", report.ChunkHits, report.ChunkScored)
	}
	fmt.Fprintf(&b, "  distinct files   %.2f of 5   (higher is less duplication)\n", report.DistinctAt5)

	fmt.Fprintf(&b, "\n  misses:\n")
	misses := 0
	for _, outcome := range report.Outcomes {
		if outcome.FirstHitRank != 0 && outcome.FirstHitRank <= 5 {
			continue
		}
		misses++
		status := "not in top 10"
		if outcome.FirstHitRank != 0 {
			status = fmt.Sprintf("rank %d", outcome.FirstHitRank)
		}
		fmt.Fprintf(&b, "    %-6s %s\n", status, outcome.Question)
		fmt.Fprintf(&b, "           want %s\n", strings.Join(outcome.Want, ", "))
		fmt.Fprintf(&b, "           got  %s\n", strings.Join(outcome.Got, ", "))
	}
	if misses == 0 {
		fmt.Fprintf(&b, "    (none outside the top 5)\n")
	}
	t.Log(b.String())
}
