#!/usr/bin/env bash
set -euo pipefail

PG_BIN="/usr/lib/postgresql/18/bin"
PGDATA="$(pwd)/pgdata"
PGPORT=5432
PGUSER="$(whoami)"
PGDB="bench"
PGLOG="$PGDATA/postgres.log"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start   Initialize (if needed) and start the PostgreSQL instance
  stop    Stop the running PostgreSQL instance
  help    Show this message
EOF
}

start() {
    # Initialize cluster if needed
    if [ -d "$PGDATA" ]; then
        echo "Data directory $PGDATA already exists, skipping initdb."
    else
        echo "Initializing PostgreSQL cluster at $PGDATA..."
        "$PG_BIN/initdb" -D "$PGDATA" --username="$PGUSER" --auth=trust

        # Force TCP listening (needed for network stack profiling)
        cat >> "$PGDATA/postgresql.conf" <<EOF

# dbms-client profiling config
listen_addresses = 'localhost'
port = $PGPORT
EOF
    fi

    # Start server
    echo "Starting PostgreSQL..."
    "$PG_BIN/pg_ctl" -D "$PGDATA" -l "$PGLOG" start

    # Wait for ready
    echo "Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 15); do
        if "$PG_BIN/pg_isready" -h localhost -p "$PGPORT" -U "$PGUSER" &>/dev/null; then
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
    if ! "$PG_BIN/psql" -h localhost -p "$PGPORT" -U "$PGUSER" -lqt | cut -d'|' -f1 | grep -qw "$PGDB"; then
        echo "Creating database '$PGDB'..."
        "$PG_BIN/createdb" -h localhost -p "$PGPORT" -U "$PGUSER" "$PGDB"
    else
        echo "Database '$PGDB' already exists."
    fi

    echo ""
    echo "PostgreSQL is running on localhost:$PGPORT"
    echo "  Connect : $PG_BIN/psql -h localhost -p $PGPORT -U $PGUSER $PGDB"
    echo "  Logs    : $PGLOG"
}

stop() {
    if [ ! -d "$PGDATA" ]; then
        echo "Data directory $PGDATA does not exist. Nothing to stop."
        exit 1
    fi

    echo "Stopping PostgreSQL..."
    "$PG_BIN/pg_ctl" -D "$PGDATA" stop
    echo "PostgreSQL stopped."
}

case "${1:-help}" in
    start) start ;;
    stop)  stop  ;;
    help)  usage ;;
    *) echo "Unknown command: $1"; usage; exit 1 ;;
esac
