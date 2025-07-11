import { Client } from 'pg';
import { PrismaDataSource } from '../index';
import { dropDB, ensureDB } from './test-helpers';

import { PrismaClient } from '@prisma/client';

const config = { user: 'postgres', database: 'prisma_ds_test' };

process.env['DATABASE_URL'] =
  process.env['DATABSE_URL'] ||
  `postgresql://${config.user}:${process.env['PGPASSWORD'] || 'dbos'}@${process.env['PGHOST'] || 'localhost'}:${process.env['PGPORT'] || '5432'}/${config.database}`;

describe('PrismaDataSource.initializeDBOSSchema', () => {
  const prisma = new PrismaClient();

  beforeEach(async () => {
    const client = new Client({ ...config, database: 'postgres' });
    try {
      await client.connect();
      await dropDB(client, config.database, true);
      await ensureDB(client, config.database);
    } finally {
      await client.end();
    }
    await prisma.$connect();
  });

  afterEach(async () => {
    await prisma.$disconnect();
  });

  async function queryTxCompletionTable(client: Client) {
    const result = await client.query(
      /*sql*/
      'SELECT workflow_id, function_num, output, error FROM dbos.transaction_completion',
    );
    return result.rowCount;
  }

  async function txCompletionTableExists(client: Client) {
    const result = await client.query<{ exists: boolean }>(
      /*sql*/
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = 'dbos' AND table_name = 'transaction_completion');",
    );
    if (result.rowCount !== 1) throw new Error(`unexpected rowcount ${result.rowCount}`);
    return result.rows[0].exists;
  }

  test('initializeDBOSSchema-with-config', async () => {
    await PrismaDataSource.initializeDBOSSchema(prisma);

    const client = new Client(config);
    try {
      await client.connect();
      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await PrismaDataSource.uninitializeDBOSSchema(prisma);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
    }
  });

  test('initializeDBOSSchema-with-prisma', async () => {
    const client = new Client(config);
    try {
      await PrismaDataSource.initializeDBOSSchema(prisma);
      await client.connect();

      await expect(txCompletionTableExists(client)).resolves.toBe(true);
      await expect(queryTxCompletionTable(client)).resolves.toEqual(0);

      await PrismaDataSource.uninitializeDBOSSchema(prisma);

      await expect(txCompletionTableExists(client)).resolves.toBe(false);
    } finally {
      await client.end();
    }
  });
});
