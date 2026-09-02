// Typechecked by esmtypes.test.ts, never executed: .mts makes it ESM regardless of the package type.
import { DrizzleDataSource } from '@dbos-inc/drizzle-datasource';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';

declare const dataSource: DrizzleDataSource;

export const instanceClient: NodePgDatabase<{ [key: string]: object }> = dataSource.client;
export const staticClient: NodePgDatabase<{ [key: string]: object }> = DrizzleDataSource.client;
