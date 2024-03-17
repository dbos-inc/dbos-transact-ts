@echo off
setlocal EnableDelayedExpansion

rem Check if PGPASSWORD is set
if "%PGPASSWORD%"=="" (
  echo Error: PGPASSWORD is not set.
  exit /b 1
)

rem Start Postgres in a local Docker container
docker run --rm --name=dbos-db --env=POSTGRES_PASSWORD=%PGPASSWORD% --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:16.1

rem Wait for PostgreSQL to start
echo Waiting for PostgreSQL to start...
for /l %%i in (1,1,30) do (
  docker exec dbos-db psql -U postgres -c "SELECT 1;" >NUL 2>&1
  if !errorlevel! == 0 (
    echo PostgreSQL started!
    goto break
  )
  timeout /t 1 /nobreak
)
:break

echo Database started successfully^^!