import { Client } from 'pg';

export async function ensureDB(client: Client, name: string) {
  const results = await client.query('SELECT 1 FROM pg_database WHERE datname = $1', [name]);
  if (results.rows.length === 0) {
    await client.query(`CREATE DATABASE ${name}`);
  }
}

export async function dropDB(client: Client, name: string, force: boolean = false) {
  const withForce = force ? ' WITH (FORCE)' : '';
  await client.query(`DROP DATABASE IF EXISTS ${name} ${withForce}`);
}
