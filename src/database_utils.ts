import { Client } from 'pg';

import { DBOSError } from './error';
import { getClientConfig } from './utils';

/** Both helpers address the database by name, so a URL without one is a caller error, not a runtime failure. */
function requireDatabaseName(databaseUrl: string): string {
  const dbName = getDatabaseNameFromUrl(databaseUrl);
  if (!dbName) {
    throw new DBOSError(`No database name in ${maskDatabaseUrl(databaseUrl)}; expected a path such as /mydb`);
  }
  return dbName;
}

/**
 * Drop `databaseUrl`'s database, via the admin (`/postgres`) database on the same server.
 * Throws if the drop could not be carried out.
 */
export async function dropPGDatabase(databaseUrl: string, logger: { warn: (msg: string) => void }): Promise<void> {
  const quoted = quotePGIdentifier(requireDatabaseName(databaseUrl));
  const client = new Client(getClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
  // An 'error' event with no listener would take down the process.
  client.on('error', (err: Error) => logger.warn(`Unexpected error in admin client: ${err}`));
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
 * we can connect to and use, so connection and creation failures are logged rather than thrown.
 * A URL carrying no database name is a caller error and does throw.
 */
export async function ensurePGDatabase(
  databaseUrl: string,
  logger: { info: (msg: string) => void; warn: (msg: string) => void },
): Promise<void> {
  const dbName = requireDatabaseName(databaseUrl);
  const client = new Client(getClientConfig(deriveDatabaseUrl(databaseUrl, 'postgres')));
  // An 'error' event with no listener would take down the process.
  client.on('error', (err: Error) => logger.warn(`Unexpected error in startup client: ${err}`));
  let creating = false;
  try {
    await client.connect();
    const { rowCount } = await client.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [dbName]);
    if (!rowCount) {
      creating = true;
      await client.query(`CREATE DATABASE ${quotePGIdentifier(dbName)}`);
      logger.info(`Created database ${dbName}`);
    }
  } catch (e) {
    // Name the operation that failed: "verify" and "create" need very different fixes.
    const attempted = creating ? `create database ${dbName}` : `verify the existence of database ${dbName}`;
    logger.warn(`Could not ${attempted}, continuing: ${(e as Error).message}`);
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
