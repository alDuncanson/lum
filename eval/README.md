# Retrieval evaluation

Measures whether lum finds the right code, so changes to parsing, chunking,
and embedding are judged by a number rather than by whether the results feel
better to whoever just changed them.

```sh
nix run .#eval                          # against the existing eval index
nix run .#eval -- --fresh               # wipe and re-index first
```

Or directly, against whatever daemon is already running:

```sh
cargo test --release --test eval -- --ignored --nocapture
```

It runs against its own data directory (`/tmp/lum-eval`), so a measurement
never depends on — or disturbs — your real index. Use `--fresh` whenever the
change under test invalidates existing vectors, which anything touching the
chunker, the parsers, or the model does.

The corpus is this repository, which changes underneath the benchmark — so
only runs taken on the same tree compare, and the numbers are comparable
rather than precise. With ~50 queries, one flipping moves recall by two
points.

## Current

`bge-small-en-v1.5`, path and heading-trail context in the embedded text,
tree-sitter chunking, hybrid retrieval (vector ranking with fusion-selected
BM25 slots), at most two chunks per file. 54 phrase queries over 113
documents / 1119 chunks:

| recall@1 | recall@5 | recall@10 | MRR | chunk hit |
|---|---|---|---|---|
| 0.63 | 0.89 | 0.98 | 0.734 | 0.88 |

One whole miss remains ("live activity tui"). `LUM_KEYWORD_SEARCH=off`
reverts to pure-vector retrieval, which is how the hybrid comparison below
was measured and how a "was that hit lexical or semantic?" question gets
answered.

## What the fixture has already decided

Each of these was measured, and several reversed the intuitive answer. Full
tables live in git history; the deltas below are what matters.

**Syntax chunking beats word windows.** Splitting code at declaration
boundaries instead of every 220 words moved every metric (MRR 0.627 → 0.688).
A chunk that is one function has a vector near that function; a window
straddling two is near neither.

**Chunk size is a real optimum, not a preference.** 1200 bytes beat both 800
and 1800. Too small and chunks stop containing the substring the query wants;
too large and one vector averages over three functions — and 1800 bytes of
code overflows the model's 512-token context, truncating the tail before it is
embedded.

**Embedded context is not free.** Prepending the repository-relative path
helped everywhere (MRR 0.584 → 0.659): people search with words that live in
the path. Prepending the markdown heading trail helped only after dropping the
document title from it — a title repeats what the path already says, on every
chunk of the file, which made prose outrank the code it describes. Context
shared by every chunk in a file makes that file compete for queries it should
lose.

**Two chunks per file is the default for a reason.** Collapsing to one chunk
per file wins every recall column and loses where it counts: the chunk
containing the thing you searched for is often the file's *second*-best chunk,
and a picker that jumps to the wrong function found the right file uselessly.
Two keeps most of the coverage; three is the worst of both.

**Down-weighting test files is wrong at every strength.** Tests outrank
implementations for some queries — a test names the feature repeatedly, in
prose-like assertion names. Scaling test scores by 0.95, 0.9, 0.8, and 0 made
every metric monotonically worse from the first 5%, once the fixture contained
queries whose answer *is* a test (people do look for tests). What shipped is
`--no-tests`: all or nothing, off by default. The general lesson is about
fixtures — a benchmark with no counter-examples to a change will endorse it,
so check whether the fixture is capable of disagreeing before measuring.

**Keyword search helps as an assist, and only as an assist.** Pure vector
scored recall@1 0.63 / recall@10 0.93 / chunk hit 0.73; its whole misses were
queries whose exact words are in the file but not near anything the embedding
considers similar ("indexable file extensions" missing the literal
`EXTENSIONS` table). Fusing BM25 in with reciprocal rank fusion fixed the
tail and broke the head — recall@10 0.98 but recall@1 down to 0.54, because
RRF's gap between semantic ranks one and two (~0.0003) is smaller than any
useful keyword bonus, so a confident semantic first place is structurally
indefensible. Slotting by raw BM25 order protected the head and lost the tail
gain again: common-word matches ate the slots. What shipped keeps the ranking
purely semantic and slots the best fusion-scored keyword hits *not already on
screen* into 5th/8th/10th place: recall@1 0.63, recall@10 0.98, chunk hit
0.88, MRR unchanged, for one recall@5 query. Both losing designs are recorded
in `slot_keyword_hits`'s doc comment.

**Query phrasing is part of the measurement.** An earlier fixture of full
natural-language questions scored MRR 0.259; rewriting the same intents as the
two-to-four-word phrases people actually type scored 0.584 with no change to
lum. A bi-encoder embeds text and returns nearest neighbours — nothing reads a
sentence and reasons about it, so a fixture of questions measures a capability
the system does not have.

## The metrics

- **recall@k** — a correct file appears in the top k. The headline, and the
  coarsest: it says nothing about rank or which part of the file came back.
- **MRR** — mean of 1/rank of the first correct result. A fix that moves
  answers from rank 8 to rank 2 shows up here while recall@10 stays flat.
- **chunk hit** — for queries naming a `contains` substring, whether a
  returned chunk actually includes it. The precision metric: the right file at
  the wrong lines counts for recall and is still not an answer.

## Writing queries

`queries.yaml` is the benchmark; editing it changes what is measured.

- **Write what you would type, and stop.** "idle shedding", not "where is idle
  shedding implemented and why".
- **Prefer phrases whose words are not already in the answer.** Where they
  are, grep wins and this measures nothing interesting.
- **Keep `files` to what genuinely answers the query.** Padding it inflates
  recall without improving anything.
- **Use `contains` with an identifier, never a line number.** Line numbers
  drift with every edit above them and rot into permanent misses.
- **Write queries before the change you intend to make.** A fixture authored
  by the person optimizing against it drifts toward what the system already
  does well.

Two guards run in the ordinary test suite, because a stale answer key
otherwise scores as a miss forever: every `files` entry must exist, and the
fixture itself is excluded from the index — it contains every query verbatim,
and indexing it once made it the best match for half of its own queries.
