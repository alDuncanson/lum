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

## Current

`bge-small-en-v1.5`, path context in the embedded text, tree-sitter chunking
for code and markdown, at most two chunks per file, 40 phrase queries over 70
documents / 705 chunks:

| recall@1 | recall@5 | recall@10 | MRR | chunk hit | distinct@5 |
|---|---|---|---|---|---|
| 0.600 | 0.825 | 0.950 | 0.695 | 0.692 | 3.77 |

Everything below is how it got there. Read the columns, not the rows across
sections: the corpus is this repository and it changes underneath the
benchmark, so only measurements taken on the same tree on the same day
compare.

### Syntax chunking beat word windows

Word windows (220 words, 40 overlap) against tree-sitter, same tree, 69
documents:

| metric | word window | tree-sitter |
|---|---|---|
| chunks | 450 | 661 |
| recall@1 | 0.525 | **0.600** |
| recall@5 | 0.775 | **0.800** |
| recall@10 | 0.875 | **0.900** |
| MRR | 0.627 | **0.688** |
| chunk hit rate | 0.846 | 0.846 |
| distinct files in top 5 | **3.23** | 3.00 |

Splitting code at declaration boundaries instead of every 220 words moved
every retrieval metric. Only 43 of the 49 answer-key files were in a language
with a grammar at the time, so the whole gain came from three quarters of the
fixture.

Duplication got worse, 3.23 → 3.00 distinct files in the top five, for the
reason smaller chunks always cost duplication: more of one file can crowd into
five slots. Collapsing results by file would pay for this and for the
regression path context caused.

### Markdown: sections helped, the document title hurt

Adding a markdown grammar was really two changes, and measuring them together
would have shipped the wrong one. Four runs on one tree, 70 documents:

| metric | prose | sections | + full trail | + trail, no title |
|---|---|---|---|---|
| chunks | 684 | 703 | 703 | 703 |
| recall@1 | 0.600 | **0.625** | 0.575 | 0.600 |
| recall@5 | 0.800 | 0.750 | 0.775 | **0.825** |
| recall@10 | 0.875 | 0.900 | **0.925** | **0.925** |
| MRR | 0.685 | 0.698 | 0.668 | **0.692** |
| distinct files in top 5 | 3.17 | **3.20** | 3.12 | 3.12 |

Splitting on headings rather than every 220 words helped on its own.

Then the heading trail — prepending `Ingestion data flow > Level 0` before
embedding, the same trick as the path — made things *worse*: recall@1 0.625 →
0.575, MRR 0.698 → 0.668. It did what it was meant to, pulling both
`docs/diagrams.md` queries from nowhere into the top ten, and then kept going:
`README.md` and `docs/architecture.md` started outranking the code they
describe, for queries like "telescope plugin setup" and "gitignore rules".
Documentation describes implementation, so making documentation easier to find
makes implementation harder to find.

The cause was the document title. Every trail in a file began with the same
`# lum`, which is also what the path says, so the cost was paid on every chunk
and bought nothing. Dropping it — precisely: dropping the outermost heading
level when a document has exactly one heading there, since a document with
several has sections rather than a title — recovered the precision and kept
the recall: recall@5 0.750 → 0.825, recall@10 0.925, MRR 0.692.

The lesson is not about markdown. Context is not free: every token spent
saying where a chunk lives is a token not spent on what it says, and context
shared by every chunk in a file makes that file compete for queries it should
lose.

### Chunk size is not monotonic

The budget is the one number worth tuning, and 1200 bytes is a real optimum
rather than a guess:

| max_bytes | chunks | recall@1 | recall@10 | MRR | chunk hit |
|---|---|---|---|---|---|
| 800 | 961 | 0.575 | 0.825 | 0.663 | 0.769 |
| **1200** | 661 | **0.600** | **0.900** | **0.688** | **0.846** |
| 1800 | 481 | 0.575 | 0.825 | 0.650 | 0.846 |

Both directions lose, for different reasons. At 800 the chunk hit rate falls
to 0.769: chunks get too small to contain the substring the query is actually
after, so the right file comes back around the wrong lines. At 1800 the hit
rate holds but recall@10 drops to 0.825 — a chunk holding three functions has
a vector near none of them, and 1800 bytes of code is roughly 600 tokens
against bge-small's 512, so the tail is being truncated before it is embedded.

### Path context

Prepending the repository-relative path to each chunk before embedding — not
storing it, only embedding it — was the previous change, measured on a
65-document tree with the word-window chunker: recall@1 0.450 → 0.550,
recall@10 0.850 → 0.925, MRR 0.584 → 0.659, chunk hit rate 0.769 → 0.846.
Three queries that found nothing in the top ten started ranking ("protocol
boundaries table", "live activity tui", "environment variables"), and
distinct files fell 3.52 → 3.30. Every chunk of a file shares a prefix, which
makes chunks of the same file look more alike.

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

### Collapsing by file trades precision for coverage

Capping how many chunks one file may contribute, measured on one index by
re-querying it — no re-ingest, so these four rows are exactly comparable:

| per_file | recall@1 | recall@5 | recall@10 | MRR | chunk hit | distinct@5 |
|---|---|---|---|---|---|---|
| 0 (off) | 0.600 | 0.825 | 0.925 | 0.692 | **0.769** | 3.10 |
| 1 | 0.600 | **0.900** | **0.975** | **0.713** | 0.538 | **5.00** |
| **2** | 0.600 | 0.825 | **0.950** | 0.695 | 0.692 | 3.77 |
| 3 | 0.600 | 0.825 | 0.925 | 0.692 | 0.692 | 3.42 |

One chunk per file looks like the obvious winner and is not. It finds far more
of the right *files* — recall@5 0.825 → 0.900, recall@10 0.975, five distinct
files in five slots by construction — and lands on the wrong *lines* far more
often: chunk hit rate 0.769 → 0.538. Where several chunks of the right file
used to come back, only the highest-scoring one now does, and the substring
the query was actually after was often in one of the others. For a picker that
jumps to a line range, the right file at the wrong function is a worse answer
than it looks in a recall column.

Two is the default because it keeps most of the coverage (recall@10 0.950,
distinct 3.10 → 3.77) for one query's worth of chunk-hit precision. Three
gives back the coverage and keeps the precision loss, which is the worst of
both.

## What the current misses say

Three of forty find nothing in the top ten, and four more rank outside the top
five. They fall into three groups.

**Tests outrank implementations.**

    change detection fingerprint  -> rank 6, above it: catalog.go, ingest_test.go, localdir_test.go
    worker crashed state          -> rank 8, above it: client_test.go, manager_test.go x2
    live activity tui             -> not in top 10; server_test.go leads

A test names the feature repeatedly, in prose-like assertion messages, and
does so in short focused functions — close to a description of what scores
well here. It is not obviously wrong to return them, but nobody searching
"worker crashed state" wants two test files before the state machine. Whether
that is fixed by ranking, by a filter, or by leaving it alone is open.

**One file crowds out the rest.**

    picker entry formatting  -> not in top 10; three chunks of syntax.rs lead

Nothing in the top five was the answer, and three of the five were the same
file. Collapsing results by file would give the real answer somewhere to go.

**No grammar for protobuf.**

    grpc contract  -> wanted proto/lum/v1/worker.proto; got client.go and the generated stubs

The `.proto` file is the contract; the generated Go and the hand-written
client that calls it both describe it more verbosely than it describes itself.
A protobuf grammar is one dependency and one match arm, now that markdown has
proved the shape.

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
