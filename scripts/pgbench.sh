#!/usr/bin/env bash
set -euo pipefail

PG_BIN="/usr/lib/postgresql/18/bin"
PGHOST="localhost"
PGPORT=5432
PGUSER="$(whoami)"
PGDB="bench"

# Defaults
SCALE=10
CLIENTS=1
DURATION=60

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  init    Populate the database with pgbench tables
  run     Run the benchmark
  help    Show this message

Options for init:
  -s <scale>     Scale factor, number of 100k rows in pgbench_accounts (default: $SCALE)

Options for run:
  -c <clients>   Number of concurrent clients (default: $CLIENTS)
  -T <seconds>   Duration of the benchmark in seconds (default: $DURATION)
EOF
}

init() {
    while getopts "s:" opt; do
        case "$opt" in
            s) SCALE="$OPTARG" ;;
            *) usage; exit 1 ;;
        esac
    done

    echo "Initializing pgbench schema (scale=$SCALE)..."
    "$PG_BIN/pgbench" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -i -s "$SCALE" "$PGDB"
    echo "pgbench schema ready."
}

run() {
    while getopts "c:T:" opt; do
        case "$opt" in
            c) CLIENTS="$OPTARG" ;;
            T) DURATION="$OPTARG" ;;
            *) usage; exit 1 ;;
        esac
    done

    echo "Running pgbench (clients=$CLIENTS, duration=${DURATION}s)..."
    "$PG_BIN/pgbench" \
        -h "$PGHOST" \
        -p "$PGPORT" \
        -U "$PGUSER" \
        -c "$CLIENTS" \
        -j "$CLIENTS" \
        -T "$DURATION" \
        -P 10 \
        "$PGDB"
}

CMD="${1:-help}"
shift || true

case "$CMD" in
    init) init "$@" ;;
    run)  run  "$@" ;;
    help) usage ;;
    *) echo "Unknown command: $CMD"; usage; exit 1 ;;
esac
