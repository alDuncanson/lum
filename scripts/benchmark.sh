#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/benchmark.sh [options] <workspace>

Measure an isolated lum instance and emit one JSON result to stdout.

Options:
  --lum PATH          lum binary to measure (default: lum from PATH)
  --query TEXT        benchmark query (default: semantic search)
  --runs N           number of warm queries (default: 10)
  --addr HOST:PORT   isolated API address (default: 127.0.0.1:17420)
  --model-cache DIR reuse an existing models directory (avoids a download)
  --keep-data        keep the temporary data directory
EOF
}

lum_bin=${LUM_BENCH_BINARY:-lum}
query="semantic search"
runs=10
addr=127.0.0.1:17420
model_cache=""
keep_data=false

while (($#)); do
  case "$1" in
    --lum) lum_bin=$2; shift 2 ;;
    --query) query=$2; shift 2 ;;
    --runs) runs=$2; shift 2 ;;
    --addr) addr=$2; shift 2 ;;
    --model-cache) model_cache=$2; shift 2 ;;
    --keep-data) keep_data=true; shift ;;
    -h|--help) usage; exit 0 ;;
    --*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)
      if [[ -n ${workspace:-} ]]; then
        echo "expected exactly one workspace" >&2
        exit 2
      fi
      workspace=$1
      shift
      ;;
  esac
done

if [[ -z ${workspace:-} ]]; then
  usage >&2
  exit 2
fi
if [[ ! $runs =~ ^[1-9][0-9]*$ ]]; then
  echo "--runs must be a positive integer" >&2
  exit 2
fi
for command in curl perl ps awk; do
  command -v "$command" >/dev/null || { echo "missing required command: $command" >&2; exit 1; }
done
if [[ $lum_bin == */* ]]; then
  lum_bin=$(cd "$(dirname "$lum_bin")" && pwd)/$(basename "$lum_bin")
else
  lum_bin=$(command -v "$lum_bin")
fi
workspace=$(cd "$workspace" && pwd)

data_dir=$(mktemp -d "${TMPDIR:-/tmp}/lum-bench.XXXXXX")
daemon_pid=""
cleanup() {
  if [[ -n $daemon_pid ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    LUM_DATA_DIR=$data_dir LUM_HTTP_ADDR=$addr "$lum_bin" stop >/dev/null 2>&1 || kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  if [[ $keep_data == true ]]; then
    echo "benchmark data: $data_dir" >&2
  else
    rm -rf "$data_dir"
  fi
}
trap cleanup EXIT INT TERM

if [[ -n $model_cache ]]; then
  model_cache=$(cd "$model_cache" && pwd)
  ln -s "$model_cache" "$data_dir/models"
fi

now_ms() {
  perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000'
}

export LUM_DATA_DIR=$data_dir
export LUM_HTTP_ADDR=$addr
export LUM_LUMEN_PATH=${LUM_LUMEN_PATH:-$(dirname "$lum_bin")/lumen}

if curl -fsS "http://$addr/v1/status" >/dev/null 2>&1; then
  echo "refusing to benchmark: $addr is already serving lum; choose another --addr" >&2
  exit 1
fi

cold_started=$(now_ms)
"$lum_bin" serve >"$data_dir/benchmark.log" 2>&1 &
daemon_pid=$!
ready=false
for _ in {1..600}; do
  if curl -fsS "http://$addr/v1/status" >"$data_dir/status.json" 2>/dev/null; then
    if BENCH_STATUS="$data_dir/status.json" perl -MJSON::PP -e '
      open my $fh, "<", $ENV{BENCH_STATUS} or die $!;
      my $status = decode_json(do { local $/; <$fh> });
      exit(($status->{data_plane} // "") eq "ready" ? 0 : 1);
    '; then
      ready=true
      break
    fi
  fi
  if ! kill -0 "$daemon_pid" 2>/dev/null; then
    cat "$data_dir/benchmark.log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ $ready != true ]]; then
  echo "lum did not become ready within 60 seconds; see $data_dir/benchmark.log" >&2
  keep_data=true
  exit 1
fi
cold_start_ms=$(( $(now_ms) - cold_started ))

index_started=$(now_ms)
"$lum_bin" search --root "$workspace" --limit 10 --json -- "$query" >"$data_dir/initial-search.json"
index_ms=$(( $(now_ms) - index_started ))
curl -fsS "http://$addr/v1/status" >"$data_dir/status.json"
if ! BENCH_STATUS="$data_dir/status.json" perl -MJSON::PP -e '
  open my $fh, "<", $ENV{BENCH_STATUS} or die $!;
  my $status = decode_json(do { local $/; <$fh> });
  exit(($status->{stats}{failures} // 0) == 0 ? 0 : 1);
'; then
  echo "initial indexing reported failures; refusing to publish partial measurements" >&2
  keep_data=true
  exit 1
fi

: >"$data_dir/latencies.txt"
for ((i = 0; i < runs; i++)); do
  started=$(now_ms)
  "$lum_bin" search --root "$workspace" --limit 10 --json -- "$query" >/dev/null
  echo $(( $(now_ms) - started )) >>"$data_dir/latencies.txt"
done

curl -fsS "http://$addr/v1/status" >"$data_dir/status.json"
data_plane_pid=$(ps -axo ppid=,pid= | awk -v parent="$daemon_pid" '$1 == parent { print $2; exit }')
daemon_rss_kb=$(ps -o rss= -p "$daemon_pid" | awk '{print $1 + 0}')
data_plane_rss_kb=0
if [[ -n $data_plane_pid ]]; then
  data_plane_rss_kb=$(ps -o rss= -p "$data_plane_pid" | awk '{print $1 + 0}')
fi

export BENCH_ROOT=$workspace BENCH_QUERY=$query BENCH_RUNS=$runs
export BENCH_COLD_MS=$cold_start_ms BENCH_INDEX_MS=$index_ms
export BENCH_DAEMON_RSS=$daemon_rss_kb BENCH_DATA_RSS=$data_plane_rss_kb
export BENCH_STATUS=$data_dir/status.json BENCH_LATENCIES=$data_dir/latencies.txt
perl -MJSON::PP -MPOSIX=strftime -e '
  open my $sf, "<", $ENV{BENCH_STATUS} or die $!;
  my $status = decode_json(do { local $/; <$sf> });
  open my $lf, "<", $ENV{BENCH_LATENCIES} or die $!;
  my @latency = sort { $a <=> $b } map { 0 + $_ } <$lf>;
  my $percentile = sub {
    my ($p) = @_;
    my $index = int(($p * @latency + 99) / 100) - 1;
    $index = 0 if $index < 0;
    return $latency[$index];
  };
  print JSON::PP->new->canonical->pretty->encode({
    schema_version => 1,
    measured_at => strftime("%Y-%m-%dT%H:%M:%SZ", gmtime()),
    platform => $^O,
    workspace => $ENV{BENCH_ROOT},
    query => $ENV{BENCH_QUERY},
    runs => 0 + $ENV{BENCH_RUNS},
    documents => 0 + $status->{stats}{documents},
    chunks => 0 + $status->{stats}{chunks},
    cold_start_ms => 0 + $ENV{BENCH_COLD_MS},
    initial_index_and_search_ms => 0 + $ENV{BENCH_INDEX_MS},
    warm_query_ms => {
      min => $latency[0],
      median => $percentile->(50),
      p95 => $percentile->(95),
      max => $latency[-1],
    },
    rss_kb => {
      control_plane => 0 + $ENV{BENCH_DAEMON_RSS},
      data_plane => 0 + $ENV{BENCH_DATA_RSS},
      total => 0 + $ENV{BENCH_DAEMON_RSS} + $ENV{BENCH_DATA_RSS},
    },
  });
'
