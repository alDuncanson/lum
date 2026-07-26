# Retrieval evaluation

Measures whether lum finds the right code, so that changes to parsing,
chunking, and embedding can be judged by a number rather than by whether the
results feel better to whoever just changed them.

```sh
nix run .#eval                          # against the existing eval index
nix run .#eval -- --fresh               # wipe and re-index first
nix run .#eval -- -eval.json /tmp/after.json -eval.label "tree-sitter"
```

Or directly, against whatever daemon is already running:

```sh
cd dispatcher && go test -tags eval -v -timeout 30m ./internal/eval/
```

It runs on its own port and data directory (`/tmp/lum-eval`), so a measurement
never depends on — or disturbs — your real index. Use `--fresh` whenever the
change under test invalidates existing vectors, which anything touching the
chunker, the parsers, or the model does.

## Baseline

Word-window chunker (220 words, 40 overlap), `bge-small-en-v1.5`, path context
in the embedded text, 40 phrase queries over 65 documents / 402 chunks:

| metric | no path context | with path context |
|---|---|---|
| recall@1 | 0.450 | **0.550** |
| recall@5 | 0.775 | **0.825** |
| recall@10 | 0.850 | **0.925** |
| MRR | 0.584 | **0.659** |
| chunk hit rate | 0.769 | **0.846** |
| distinct files in top 5 | 3.52 | 3.30 |

Prepending the repository-relative path to each chunk before embedding — not
storing it, only embedding it — moved every retrieval metric. Three queries
that previously found nothing in the top ten now rank ("protocol boundaries
table", "live activity tui", "environment variables"), and no query fell out.

The one number that got worse is duplication: 3.52 → 3.30 distinct files in
the top five. Expected, and the cost of the trick. Every chunk of a file now
shares a prefix, which makes chunks of the same file more similar to each
other and more likely to cluster in one result list. Collapsing results by
file would pay for itself here.

### The first baseline was measuring the wrong thing

An earlier fixture used full natural-language questions ("how does lum avoid
keeping the embedding model in memory when nobody is searching?") and scored
recall@1 0.133 / MRR 0.259. Rewriting the same 40-odd intents as the phrases
someone would actually type moved recall@1 to 0.450 and MRR to 0.584 — with no
change whatsoever to lum.

Nothing got better; the benchmark stopped being wrong. lum is a bi-encoder:
it embeds your text and returns nearest neighbours by cosine similarity.
Nothing reads a sentence and reasons about it, so a fixture of questions was
measuring a capability the system does not have and was never going to.

Worth keeping in mind whenever these numbers move: the query distribution is
part of the measurement, and it is the part easiest to get wrong.

## What the current misses say

Six of forty find nothing in the top ten. Four of them share a cause:

    ingestion diagram          -> wanted docs/diagrams.md
    protocol boundaries table  -> wanted docs/diagrams.md
    picker entry formatting    -> wanted lua/lum/telescope.lua
    live activity tui          -> wanted dispatcher/internal/cli/top.go

**The file path is not embedded.** Only `chunk.text` becomes a vector; the URI
is stored in the payload and used for display, provenance, and filtering, but
never for matching. So "ingestion diagram" cannot match `docs/diagrams.md` on
its name — only on prose that happens to contain those words. Since people
routinely search with words that live in the path, prepending a compact
context header to each chunk before embedding (path, and later the enclosing
symbol) is plausibly a larger and much cheaper win than a smarter chunker.

The other two show a different problem: for "retry backoff" and "environment
variables", test files and prose about the feature outrank the implementation.

## The metrics

**recall@k** — fraction of queries where a correct file appears in the top k.
The headline number, and the coarsest: it says nothing about ranking or about
which part of the file came back.

**MRR** — mean of 1/rank of the first correct result. Rewards ranking rather
than mere presence, so a fix that moves answers from rank 8 to rank 2 shows up
here while recall@10 stays flat.

**chunk hit rate** — for queries naming a `contains` substring, whether a
returned chunk actually includes it. This is the precision metric: the right
file with boundaries straddling two unrelated functions still counts as a
recall hit, and is still not an answer.

**distinct files in top 5** — how many of the top five results are different
files. Overlapping chunks let one function occupy several slots; 5.0 means no
duplication, and lower means wasted screen.

## Writing queries

`queries.yaml` is the benchmark. Editing it changes what is being measured,
so a few rules keep it honest:

- **Write what you would type, and stop.** "idle shedding", not "where is idle
  shedding implemented and why". Two to four words is the realistic shape.
- **Prefer phrases whose words are not already in the answer.** Where they are,
  grep wins and this measures nothing interesting.
- **Keep `files` to what genuinely answers the query.** Padding it inflates
  recall without improving anything.
- **Use `contains` for an identifier that will not move**, not a line range.
  Line numbers drift with every edit above them, and a rotted answer key scores
  as a permanent miss.
- **Write queries before the change you intend to make**, not after. A fixture
  authored by the person optimizing against it drifts toward what the system
  already does well.

The harness fails loudly if a query names a file that no longer exists,
since a stale answer key otherwise scores as a miss forever.

## Caveats worth keeping in mind

Forty queries is a small sample; a single one flipping moves recall by
2.5 points. The numbers are not precise. They are *comparable* between two
runs, which is all that is needed to tell an improvement from a preference.

The fixture is excluded from the index by the runner. It contains every
query verbatim, so indexing it made it the best match for its own queries —
it appeared in the top five for half of them before that was caught, which is a
good illustration of how easily a benchmark measures itself.

This repository is also unusual: heavily commented code and a large `docs/`
tree written in the same vocabulary the queries use. Prose files outrank code
on several queries, which is real signal about the embedding model, but it
means results here will not transfer directly to a repository with sparser
comments.
