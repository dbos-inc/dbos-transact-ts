#!/bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"

cd ${SCRIPT_DIR}

# Start a local postgres docker container.
docker run --name=operon-pg --env=POSTGRES_PASSWORD=dbos --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:15.4
