#! /bin/bash

if [[ -z "${POSTGRES_HOST}" ]]; then
  export POSTGRES_HOST="localhost"
fi

################################################################
# Create the `hello` user and DB.
################################################################

# Create the new user 'hello'
psql -U postgres -h $POSTGRES_HOST -c "CREATE USER hello WITH PASSWORD 'hello';"
psql -U postgres -h $POSTGRES_HOST -c "ALTER USER hello CREATEDB;"
psql -U postgres -h $POSTGRES_HOST -d postgres -c "DROP DATABASE IF EXISTS hello;"

# Save the current value of PGPASSWORD to a variable
OLD_PGPASSWORD="$PGPASSWORD"

export PGPASSWORD='hello'
psql -U hello -h $POSTGRES_HOST -d postgres -c "CREATE DATABASE hello;"

export PGPASSWORD="$OLD_PGPASSWORD"
psql -U postgres -h $POSTGRES_HOST -d hello -c "GRANT CREATE, USAGE ON SCHEMA public TO hello;"

################################################################
# Create tables for hello app.
################################################################

export PGPASSWORD='hello'
psql -U hello -h $POSTGRES_HOST -d hello -c "CREATE TABLE IF NOT EXISTS OperonHello (greeting_id SERIAL PRIMARY KEY, greeting TEXT);"