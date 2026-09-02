// Typechecked by esmtypes.test.ts, never executed: .mts makes it ESM regardless of the package type.
import { KyselyDataSource } from '@dbos-inc/kysely-datasource';
import type { Kysely } from 'kysely';

interface Database {
  greetings: { greeting: string };
}

declare const dataSource: KyselyDataSource<Database>;
declare const existingClient: Kysely<Database>;

export const client: Kysely<Database> = dataSource.client;
export const fromExistingClient = new KyselyDataSource<Database>('app', existingClient);
