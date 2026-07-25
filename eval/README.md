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

Word-window chunker (220 words, 40 overlap), `bge-small-en-v1.5`, 30 questions
over 65 documents / 394 chunks:

| metric | value |
|---|---|
| recall@1 | 0.133 |
| recall@5 | 0.433 |
| recall@10 | 0.600 |
| MRR | 0.259 |
| chunk hit rate | 0.222 |
| distinct files in top 5 | 3.83 |

Read that as: for two questions in three, a correct file is somewhere in the
top ten — but only one in eight puts it first, and when the right file *is*
found, the chunk usually is not the part that answers the question. That last
number is the one a syntax-aware chunker should move most.

## The metrics

**recall@k** — fraction of questions where a correct file appears in the top k.
The headline number, and the coarsest: it says nothing about ranking or about
which part of the file came back.

**MRR** — mean of 1/rank of the first correct result. Rewards ranking rather
than mere presence, so a fix that moves answers from rank 8 to rank 2 shows up
here while recall@10 stays flat.

**chunk hit rate** — for questions naming a `contains` substring, whether a
returned chunk actually includes it. This is the precision metric: the right
file with boundaries straddling two unrelated functions still counts as a
recall hit, and is still not an answer.

**distinct files in top 5** — how many of the top five results are different
files. Overlapping chunks let one function occupy several slots; 5.0 means no
duplication, and lower means wasted screen.

## Writing questions

`questions.yaml` is the benchmark. Editing it changes what is being measured,
so a few rules keep it honest:

- **Ask what you would actually ask.** "where are retries handled" is a
  question. "the replaceRetry function in ingest.go" is a lookup, and grep
  already wins those.
- **Prefer questions whose answer words are not in the answer.** That is the
  only place semantic search can beat a regex, and where prose-shaped chunking
  hurts most.
- **Keep `files` to what genuinely answers the question.** Padding it inflates
  recall without improving anything.
- **Use `contains` for an identifier that will not move**, not a line range.
  Line numbers drift with every edit above them, and a rotted answer key scores
  as a permanent miss.
- **Write questions before the change you intend to make**, not after. A
  fixture authored by the person optimizing against it drifts toward what the
  system already does well.

The harness fails loudly if a question names a file that no longer exists,
since a stale answer key otherwise scores as a miss forever.

## Caveats worth keeping in mind

Thirty questions is a small sample; a single question flipping moves recall by
3.3 points. The numbers are not precise. They are *comparable* between two
runs, which is all that is needed to tell an improvement from a preference.

The fixture is excluded from the index by the runner. It contains every
question verbatim, so indexing it made it the best match for its own queries —
it appeared in the top five for half of them before that was caught, which is a
good illustration of how easily a benchmark measures itself.

This repository is also unusual: heavily commented code and a large `docs/`
tree written in the same natural language as the questions. Prose files
currently outrank code on many queries, which is real signal about the
embedding model, but it means results here will not transfer directly to a
repository with sparser comments.
