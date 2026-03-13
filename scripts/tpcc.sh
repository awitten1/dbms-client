#!/usr/bin/env bash
set -euo pipefail

TPCC_BIN="$(dirname "$(dirname "$(realpath "$0")")")/tpcc"
PGHOST="localhost"
PGPORT=5432
PGUSER="$(whoami)"
PGDB="bench"

# Defaults
WAREHOUSES=1
CLIENTS=1
DURATION=60

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  init    Build the binary and load TPC-C data into the database
  run     Run the TPC-C benchmark
  help    Show this message

Options for init:
  -w <warehouses>  Scale factor (default: $WAREHOUSES)

Options for run:
  -w <warehouses>  Number of warehouses loaded (default: $WAREHOUSES)
  -c <clients>     Parallel clients (default: $CLIENTS)
  -T <seconds>     Duration in seconds (default: $DURATION)
EOF
}

build() {
    echo "Building tpcc..."
    make -C "$(dirname "$(dirname "$(realpath "$0")")")" --no-print-directory
}

init() {
    while getopts "w:" opt; do
        case "$opt" in
            w) WAREHOUSES="$OPTARG" ;;
            *) usage; exit 1 ;;
        esac
    done

    build
    echo "Loading TPC-C data (warehouses=$WAREHOUSES)..."
    "$TPCC_BIN" load \
        -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" \
        -w "$WAREHOUSES"
}

run() {
    while getopts "w:c:T:" opt; do
        case "$opt" in
            w) WAREHOUSES="$OPTARG" ;;
            c) CLIENTS="$OPTARG" ;;
            T) DURATION="$OPTARG" ;;
            *) usage; exit 1 ;;
        esac
    done

    echo "Running TPC-C (warehouses=$WAREHOUSES, clients=$CLIENTS, duration=${DURATION}s)..."
    "$TPCC_BIN" run \
        -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" \
        -w "$WAREHOUSES" -c "$CLIENTS" -T "$DURATION"
}

CMD="${1:-help}"
shift || true

case "$CMD" in
    init) init "$@" ;;
    run)  run  "$@" ;;
    help) usage ;;
    *) echo "Unknown command: $CMD"; usage; exit 1 ;;
esac
