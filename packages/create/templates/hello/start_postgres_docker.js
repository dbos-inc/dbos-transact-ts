const { execSync } = require('child_process');

if (!process.env.PGPASSWORD) {
  console.error("Error: PGPASSWORD is not set.");
  process.exit(1);
}

try {
  execSync(`docker run --rm --name=dbos-db --env=POSTGRES_PASSWORD=${process.env.PGPASSWORD} --env=PGDATA=/var/lib/postgresql/data --volume=/var/lib/postgresql/data -p 5432:5432 -d postgres:16.1`);
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
