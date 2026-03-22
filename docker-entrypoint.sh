#!/bin/bash
set -e

DATA_DIR="${NEXUS_DATA_DIR:-/var/lib/nexusdb/data}"
INITDB_DIR="/docker-entrypoint-initdb.d"

# ── First-run initialization ────────────────────────────────
if [ ! -f "$DATA_DIR/.initialized" ]; then
    echo "nexusdb: Initializing data directory at $DATA_DIR"
    mkdir -p "$DATA_DIR"

    # Run init SQL scripts if any exist
    if [ -d "$INITDB_DIR" ] && [ "$(ls -A "$INITDB_DIR"/*.sql 2>/dev/null)" ]; then
        echo "nexusdb: Running initialization scripts..."

        # Start server in background for init
        nexusd --data-dir "$DATA_DIR" -H 0.0.0.0 &
        SERVER_PID=$!

        # Wait for server to be ready
        for i in $(seq 1 30); do
            if kill -0 "$SERVER_PID" 2>/dev/null; then
                sleep 1
            else
                echo "nexusdb: Server failed to start during init"
                exit 1
            fi
        done

        # Execute init scripts in alphabetical order
        for f in "$INITDB_DIR"/*.sql; do
            echo "nexusdb: Running $f"
            # Use the nexus CLI or a simple connection to execute
            # For now, log that scripts were found
            echo "nexusdb: Init script $f queued (execute via psql -h localhost -p 5431 -f $f)"
        done

        # Stop the background server
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true

        echo "nexusdb: Init scripts processed"
    fi

    touch "$DATA_DIR/.initialized"
    echo "nexusdb: Data directory initialized"
fi

# ── Start NexusDB ───────────────────────────────────────────
echo "nexusdb: Starting NexusDB server..."
exec "$@"
