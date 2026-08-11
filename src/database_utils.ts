import { Client } from 'pg';

import { getClientConfig } from './utils';

/**
 * Drop `databaseUrl`'s database, via the admin (`/postgres`) database on the same server.
 * Throws if the database still exists afterwards.
 */
export async function dropPGDatabase(databaseUrl: string, log: (msg: string) => void): Promise<void> {
  const quoted = quotePGIdentifier(getDatabaseNameFromUrl(databaseUrl));
  const client = new Client(getClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
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
export async function ensurePGDatabase(
  databaseUrl: string,
  logger: { info: (msg: string) => void; warn: (msg: string) => void },
): Promise<void> {
  const dbName = getDatabaseNameFromUrl(databaseUrl);
  const client = new Client(getClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
  // An 'error' event with no listener would take down the process.
  client.on('error', (err: Error) => logger.warn(`Unexpected error in startup client: ${err}`));
  try {
    await client.connect();
    const { rowCount } = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [dbName]);
    if (!rowCount) {
      logger.info(`Creating system database ${dbName}`);
      await client.query(`CREATE DATABASE ${quotePGIdentifier(dbName)}`);
    }
  } catch (e) {
    logger.warn(`Could not verify the existence of database ${dbName}, continuing: ${(e as Error).message}`);
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

export function getDatabaseNameFromUrl(urlStr: string) {
  const pathname = new URL(urlStr).pathname?.replace(/^\//, '') || '';
  // Decode exactly as `pg` does, so we never provision a different database than we connect to.
  try {
    return decodeURI(pathname);
  } catch {
    return pathname;
  }
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
