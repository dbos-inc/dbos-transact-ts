import { Client } from 'pg';

/**
 * Drop `databaseUrl`'s database, via the admin (`/postgres`) database on the same server.
 * Throws if the database still exists afterwards.
 */
export async function dropPGDatabase(databaseUrl: string, log: (msg: string) => void): Promise<void> {
  const quoted = quotePGIdentifier(getDatabaseNameFromUrl(databaseUrl));
  const client = new Client(getPGClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
  // An 'error' event with no listener would take down the process.
  client.on('error', (err: Error) => log(`Unexpected error in admin client: ${err}`));
  try {
    await client.connect();
    try {
      await client.query(`DROP DATABASE IF EXISTS ${quoted} WITH (FORCE)`);
    } catch (e) {
      // CockroachDB and Postgres before 13 reject WITH (FORCE), and drop an idle database without it.
      if ((e as { code?: string }).code !== '42601') throw e;
      await client.query(`DROP DATABASE IF EXISTS ${quoted}`);
    }
  } finally {
    await client.end().catch(() => {});
  }
}

/**
 * Create `databaseUrl`'s database if it does not already exist, via the admin (`/postgres`)
 * database on the same server. Best-effort: a database we cannot provision may still be one
 * we can connect to and use, so every failure is logged and swallowed rather than thrown.
 */
export async function ensurePGDatabase(databaseUrl: string, log: (msg: string) => void): Promise<void> {
  const dbName = getDatabaseNameFromUrl(databaseUrl);
  const client = new Client(getPGClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
  // An 'error' event with no listener would take down the process.
  client.on('error', (err: Error) => log(`Unexpected error in startup client: ${err}`));
  try {
    await client.connect();
    const { rowCount } = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [dbName]);
    if (!rowCount) {
      log(`Creating system database ${dbName}`);
      await client.query(`CREATE DATABASE ${quotePGIdentifier(dbName)}`);
    }
  } catch (e) {
    log(`Could not verify the existence of database ${dbName}, continuing: ${(e as Error).message}`);
  } finally {
    await client.end().catch(() => {});
  }
}

export function deriveDatabaseUrl(urlStr: string, otherDbName: string): string {
  try {
    const u = new URL(urlStr);
    u.pathname = `/${otherDbName}`;
    return u.toString();
  } catch {
    return urlStr; // best effort; connect will fail with clear message
  }
}

// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
export function getPGClientConfig(databaseUrl: string | URL) {
  const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
  const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
  return {
    connectionString,
    connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
  };

  function getTimeout(url: URL) {
    try {
      const $timeout = url.searchParams.get('connect_timeout');
      return $timeout ? parseInt($timeout, 10) : undefined;
    } catch {
      // Ignore errors in parsing the connect_timeout parameter
      return undefined;
    }
  }
}

export function getDatabaseNameFromUrl(urlStr: string) {
  const u = new URL(urlStr);
  // The path is percent-encoded, so a name like `"db".'v1'` arrives here as `%22db%22.'v1'`.
  return decodeURIComponent(u.pathname?.replace(/^\//, '') || '');
}

export function maskDatabaseUrl(urlStr: string): string {
  try {
    const u = new URL(urlStr);
    if (u.password) {
      u.password = encodeURIComponent('***');
    }
    return u.toString();
  } catch {
    return urlStr;
  }
}

function quotePGIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
