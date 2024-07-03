const { execSync } = require('child_process');

// Default PostgreSQL port
let port = '5432';

// Set the host PostgreSQL port with the -p/--port flag.
process.argv.forEach((val, index) => {
  if (val === '-p' || val === '--port') {
    if (process.argv[index + 1]) {
      port = process.argv[index + 1];
    }
  }
});

if (!process.env.PGPASSWORD) {
  console.error("Error: PGPASSWORD is not set.");
  process.exit(1);
}

try {
  execSync(`docker run --rm --name=dbos-db --env=POSTGRES_PASSWORD="${process.env.PGPASSWORD}" --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p ${port}:5432 -d sibedge/postgres-plv8`);
  console.log("Waiting for PostgreSQL to start...");

  let attempts = 30;
  const checkDatabase = setInterval(() => {
    try {
      execSync('docker exec dbos-db psql -U postgres -c "SELECT 1;"', { stdio: 'ignore' });
      console.log("PostgreSQL started!");
      clearInterval(checkDatabase);
      console.log("Database started successfully!");
    } catch (error) {
      if (--attempts === 0) {
        clearInterval(checkDatabase);
        console.error("Failed to start PostgreSQL.");
      }
    }
  }, 1000);
} catch (error) {
  console.error("Error starting PostgreSQL in Docker:", error.message);
}
