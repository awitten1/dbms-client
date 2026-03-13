#!/bin/bash

#
# After running this script connect to postgres with `psql "host=localhost port=5432 dbname=bench"`.
#

set -euo pipefail

dir=$(pwd)

mkdir -p data_dir

pg_versions=(/usr/lib/postgresql/*)
pg_version=${pg_versions[-1]}

PGPORT=5432
PGUSER="$(whoami)"
PGDB="bench"
PGLOG="$dir/data_dir/logs/postgres.log"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  init    Initialize the database cluster
  start   Start the PostgreSQL instance (init if needed)
  stop    Stop the running PostgreSQL instance
  help    Show this message
EOF
}

set_pg_option() {
    key=$1
    value=$2
    pg_conf="$3/postgresql.conf"

    sed -i "/$1/d" $pg_conf
    echo "$key = $value" >> $pg_conf
}

init() {
    if [ ! -f data_dir/PG_VERSION ]; then
        $pg_version/bin/pg_ctl init -D data_dir
        set_pg_option 'unix_socket_directories' "'/tmp'" "data_dir"
        set_pg_option 'listen_addresses' "'localhost'" "data_dir"
        set_pg_option 'port' "$PGPORT" "data_dir"
        set_pg_option 'logging_collector' "true" "data_dir"
        set_pg_option 'log_destination' "'csvlog,jsonlog'" "data_dir"
        set_pg_option 'log_directory' "'logs'" "data_dir"
        set_pg_option 'max_connections' '2000' 'data_dir'
    else
        echo "Data directory already initialized, skipping."
    fi
}

start() {
    init

    $pg_version/bin/pg_ctl start -D data_dir

    # Wait for ready
    echo "Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 15); do
        if $pg_version/bin/pg_isready -h localhost -p "$PGPORT" -U "$PGUSER" &>/dev/null; then
            echo "PostgreSQL is ready."
            break
        fi
        if [ "$i" -eq 15 ]; then
            echo "Timed out waiting for PostgreSQL. Check $PGLOG for details."
            exit 1
        fi
        sleep 1
    done

    # Create benchmark database if it doesn't exist
    if ! $pg_version/bin/psql -h localhost -p "$PGPORT" -U "$PGUSER" -lqt | cut -d'|' -f1 | grep -qw "$PGDB"; then
        echo "Creating database '$PGDB'..."
        $pg_version/bin/createdb -h localhost -p "$PGPORT" -U "$PGUSER" "$PGDB"
    else
        echo "Database '$PGDB' already exists."
    fi

    echo ""
    echo "PostgreSQL is running on localhost:$PGPORT"
    echo "  Connect : $pg_version/bin/psql -h localhost -p $PGPORT -U $PGUSER $PGDB"
    echo "  Logs    : $PGLOG"
}

stop() {
    $pg_version/bin/pg_ctl stop -D data_dir -m fast
}

case "${1:-help}" in
    init)  init  ;;
    start) start ;;
    stop)  stop  ;;
    help)  usage ;;
    *) echo "Unknown command: $1"; usage; exit 1 ;;
esac
