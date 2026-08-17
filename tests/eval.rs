//! Retrieval quality, measured against `eval/queries.yaml`.
//!
//! Not a unit test: it needs a real embedding model and a real index of this
//! repository, and it takes as long as indexing takes. `cargo test` must stay
//! fast and hermetic, so this is `#[ignore]` and runs only when asked:
//!
//! ```text
//! nix run .#eval                                  # builds, isolates, runs
//! cargo test --release --test eval -- --ignored --nocapture
//! ```
//!
//! Why it exists: parsing, chunking, and embedding changes are judged on
//! whether results "look better", which is exactly the judgement least
//! available to whoever just made the change. These numbers are not precise —
//! fifty queries is a small sample — but they are comparable between two runs,
//! which is the only property required to tell an improvement from a
//! preference.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Results requested per query. Recall is reported at 1, 5, and this.
const LIMIT: usize = 10;

struct Case {
    query: String,
    files: Vec<String>,
    /// A substring the matched chunk must contain, to check that the right
    /// *part* of the right file came back.
    contains: Option<String>,
}

/// A deliberately small parser for the subset of YAML this fixture uses.
///
/// A YAML dependency for one flat list of three scalar keys would be a
/// dependency to audit, update, and explain. If the fixture ever needs
/// anchors or block scalars, that trade changes.
fn parse(source: &str) -> Vec<Case> {
    let mut cases: Vec<Case> = Vec::new();
    for raw in source.lines() {
        let line = raw.split('#').next().unwrap_or("").trim_end();
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(query) = trimmed.strip_prefix("- query:") {
            cases.push(Case { query: query.trim().to_owned(), files: Vec::new(), contains: None });
        } else if let Some(files) = trimmed.strip_prefix("files:") {
            if let Some(case) = cases.last_mut() {
                case.files = files
                    .trim()
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(',')
                    .map(str::trim)
                    .filter(|entry| !entry.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
        } else if let Some(contains) = trimmed.strip_prefix("contains:") {
            if let Some(case) = cases.last_mut() {
                case.contains = Some(contains.trim().to_owned());
            }
        }
    }
    cases
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Ask the binary, rather than reaching into the crate.
///
/// The thing worth measuring is what a person gets when they type `lum
/// search`, which includes collapsing and the result ordering the CLI applies.
/// Calling the engine directly would measure a pipeline nobody uses.
fn search(root: &Path, query: &str) -> Vec<(String, String)> {
    let output = Command::new(env!("CARGO_BIN_EXE_lum"))
        .args(["search", "--root"])
        .arg(root)
        .args(["--limit", &LIMIT.to_string(), "--jsonl", "--"])
        .arg(query)
        .output()
        .expect("running lum search");
    assert!(
        output.status.success(),
        "lum search failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            Some((value.get("path")?.as_str()?.to_owned(), value.get("text")?.as_str()?.to_owned()))
        })
        .collect()
}

#[test]
#[ignore = "needs the embedding model and a full index of this repository"]
fn retrieval_quality() {
    let root = repo_root();
    let fixture =
        std::fs::read_to_string(root.join("eval/queries.yaml")).expect("reading eval/queries.yaml");
    let cases = parse(&fixture);
    assert!(!cases.is_empty(), "the fixture parsed to nothing");

    // One indexing pass up front. `--root` registers and waits, so the first
    // query pays for the whole index and the rest measure retrieval.
    eprintln!("indexing {} ...", root.display());
    let _ = search(&root, "warm the index");

    let mut hit_at_1 = 0usize;
    let mut hit_at_5 = 0usize;
    let mut hit_at_n = 0usize;
    let mut reciprocal_rank = 0.0f64;
    let mut contains_checked = 0usize;
    let mut contains_hit = 0usize;
    let mut misses: Vec<&Case> = Vec::new();

    for case in &cases {
        let results = search(&root, &case.query);
        let rank =
            results.iter().position(|(path, _)| case.files.iter().any(|wanted| path == wanted));

        match rank {
            Some(index) => {
                if index == 0 {
                    hit_at_1 += 1;
                }
                if index < 5 {
                    hit_at_5 += 1;
                }
                hit_at_n += 1;
                reciprocal_rank += 1.0 / (index as f64 + 1.0);
            }
            None => misses.push(case),
        }

        if let Some(needle) = &case.contains {
            contains_checked += 1;
            // Only over chunks from an expected file: whether some unrelated
            // file happens to mention the identifier is not the question.
            let found = results
                .iter()
                .filter(|(path, _)| case.files.iter().any(|wanted| path == wanted))
                .any(|(_, text)| text.contains(needle.as_str()));
            if found {
                contains_hit += 1;
            }
        }
    }

    let total = cases.len() as f64;
    println!("\n  queries          {}", cases.len());
    println!(
        "  recall@1         {:.0}%  ({hit_at_1}/{})",
        hit_at_1 as f64 / total * 100.0,
        cases.len()
    );
    println!(
        "  recall@5         {:.0}%  ({hit_at_5}/{})",
        hit_at_5 as f64 / total * 100.0,
        cases.len()
    );
    println!(
        "  recall@{LIMIT}        {:.0}%  ({hit_at_n}/{})",
        hit_at_n as f64 / total * 100.0,
        cases.len()
    );
    println!("  MRR              {:.3}", reciprocal_rank / total);
    if contains_checked > 0 {
        println!(
            "  right chunk      {:.0}%  ({contains_hit}/{contains_checked})",
            contains_hit as f64 / contains_checked as f64 * 100.0
        );
    }
    if !misses.is_empty() {
        println!("\n  missed entirely:");
        for case in &misses {
            println!("    {:<45} expected {}", case.query, case.files.join(", "));
        }
    }
    println!();

    // A floor, not a target. It exists so a change that halves retrieval fails
    // CI instead of being noticed three weeks later; the numbers above are what
    // you actually read when tuning.
    let recall_at_5 = hit_at_5 as f64 / total;
    assert!(
        recall_at_5 >= 0.60,
        "recall@5 fell to {:.0}%, below the 60% floor",
        recall_at_5 * 100.0
    );
}

#[test]
fn the_fixture_parses_and_points_at_files_that_exist() {
    // Runs in the ordinary suite, unlike the measurement itself. A fixture
    // naming a path that no longer exists cannot ever be satisfied, and it
    // shows up as a permanent miss that reads like a retrieval regression —
    // which is exactly what happened to every entry in it during the rewrite.
    let root = repo_root();
    let fixture = std::fs::read_to_string(root.join("eval/queries.yaml")).unwrap();
    let cases = parse(&fixture);
    assert!(cases.len() > 20, "parsed only {} cases", cases.len());

    for case in &cases {
        assert!(!case.query.is_empty(), "a case has no query");
        assert!(!case.files.is_empty(), "{:?} lists no files", case.query);
        for file in &case.files {
            assert!(
                root.join(file).exists(),
                "{:?} expects {file}, which does not exist",
                case.query
            );
        }
    }
}

#[test]
fn the_parser_handles_the_shapes_the_fixture_uses() {
    let cases = parse(
        "queries:\n\
         \x20 # a comment\n\
         \x20 - query: ingestion diagram\n\
         \x20   files: [docs/diagrams.md]\n\
         \x20   contains: Ingestion data flow\n\
         \n\
         \x20 - query: architecture overview\n\
         \x20   files: [docs/architecture.md, README.md]\n",
    );
    assert_eq!(cases.len(), 2);
    assert_eq!(cases[0].query, "ingestion diagram");
    assert_eq!(cases[0].files, vec!["docs/diagrams.md"]);
    assert_eq!(cases[0].contains.as_deref(), Some("Ingestion data flow"));
    assert_eq!(cases[1].files, vec!["docs/architecture.md", "README.md"]);
    assert!(cases[1].contains.is_none(), "contains must not leak between cases");
}
