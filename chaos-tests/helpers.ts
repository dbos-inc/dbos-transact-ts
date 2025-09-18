import { Client } from 'pg';
import { DBOSConfig } from '../src';
import { startDockerPg, stopDockerPg } from '../src/cli/docker_pg_helper';

export class PostgresChaosMonkey {
  private stopEvent: boolean = false;
  private chaosTimeout: NodeJS.Timeout | null = null;

  constructor() {}

  start(): void {
    const chaosLoop = async (): Promise<void> => {
      while (!this.stopEvent) {
        const waitTime = Math.random() * (10 - 5) + 5; // Random between 5-10 seconds

        await new Promise<void>((resolve) => {
          this.chaosTimeout = setTimeout(() => {
            if (!this.stopEvent) {
              resolve();
            }
          }, waitTime * 1000);
        });

        if (!this.stopEvent) {
          console.log(`🐒 ChaosMonkey strikes after ${waitTime.toFixed(2)} seconds! Restarting Postgres...`);

          try {
            await stopDockerPg();
            const downTime = Math.random() * 2; // Random between 0-2 seconds
            await new Promise((resolve) => setTimeout(resolve, downTime * 1000));
            await startDockerPg();
          } catch (error) {
            console.error('ChaosMonkey error:', error);
          }
        }
      }
    };

    this.stopEvent = false;
    chaosLoop().catch(console.error);
  }

  stop(): void {
    this.stopEvent = true;
    if (this.chaosTimeout) {
      clearTimeout(this.chaosTimeout);
      this.chaosTimeout = null;
    }
  }
}

export async function dropDatabases(config: DBOSConfig) {
  const dbUrl = new URL(config.systemDatabaseUrl as string);
  const baseConnectionConfig = {
    host: dbUrl.hostname,
    port: parseInt(dbUrl.port) || 5432,
    user: dbUrl.username,
    password: dbUrl.password,
  };
  const adminClient = new Client({
    ...baseConnectionConfig,
    database: 'postgres',
  });
  try {
    await adminClient.connect();
    const dbName = 'dbostest';
    await adminClient.query(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`);
  } finally {
    await adminClient.end();
  }
}
