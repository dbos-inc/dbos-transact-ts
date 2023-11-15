#!/bin/bash

# Check if PGPASSWORD is set
if [[ -z "${PGPASSWORD}" ]]; then
  echo "Error: PGPASSWORD is not set." >&2
  exit 1
fi

SCRIPT_DIR=$(dirname $(readlink -f $0))
# Enter the root dir of the example app.
cd ${SCRIPT_DIR}/

# Start Postgres in a local Docker container
docker run --rm --name=operon-db --env=POSTGRES_PASSWORD=${PGPASSWORD} --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:15.4 -c wal_level=logical

# Install wal2json
docker exec operon-db apt-get update
docker exec operon-db apt-get install postgresql-15-wal2json -y
docker exec operon-db sh -c 'echo "wal_level=logical" >> /var/lib/postgresql/data/postgresql.conf'
docker restart operon-db

# Wait for PostgreSQL to start
echo "Waiting for PostgreSQL to start..."
for i in {1..30}; do
  if docker exec operon-db pg_isready -U postgres | grep -q "accepting connections"; then
    echo "PostgreSQL started!"
    break
  fi
  sleep 1
done

# Create a database in Postgres.
docker exec operon-db psql -U postgres -c "CREATE DATABASE hello;"

npx knex migrate:latest

# create WAL reader slot
docker exec operon-db psql -U postgres -d hello -c "SELECT * FROM pg_create_logical_replication_slot('operon_prov', 'wal2json');"

# create a database in Postgres for the provenance data
docker exec operon-db psql -U postgres -c "CREATE DATABASE hello_prov;"

# eventually, prov table scehma will be generated from cloud deploy information
# manually add the prov table schema for now
docker exec operon-db psql -U postgres -d hello_prov -c "CREATE TABLE operon_hello(name text NOT NULL, greet_count integer DEFAULT 0, begin_xid xid8 NOT NULL, end_xid xid8 NULL);"
