#!/bin/bash

# Start Postgres in a local Docker container
docker run --name=operon-db --env=POSTGRES_PASSWORD=dbos --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:latest