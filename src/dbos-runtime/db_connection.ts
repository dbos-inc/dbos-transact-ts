import * as path from 'path';
import * as fs from 'fs';
import { readFileSync } from '../utils';

const DB_CONNECTION_PATH = path.join('.dbos', 'db_connection');

export interface DatabaseConnection {
  hostname: string | null;
  port: number | null;
  username: string | null;
  password: string | null;
  local_suffix: boolean | null;
}

export function loadDatabaseConnection(): DatabaseConnection {
  try {
    const rawData = readFileSync(DB_CONNECTION_PATH, 'utf8');
    const data = JSON.parse(rawData) as DatabaseConnection;
    return {
      hostname: data.hostname ?? null,
      port: data.port ?? null,
      username: data.username ?? null,
      password: data.password ?? null,
      local_suffix: data.local_suffix ?? null,
    };
  } catch (e) {
    return {
      hostname: null,
      port: null,
      username: null,
      password: null,
      local_suffix: null,
    };
  }
}

export function saveDatabaseConnection(connection: DatabaseConnection): void {
  fs.mkdirSync('.dbos', { recursive: true });
  fs.writeFileSync(DB_CONNECTION_PATH, JSON.stringify(connection));
}
