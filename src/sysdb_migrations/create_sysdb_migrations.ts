// tools/gen-migrations.ts
import fs from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import type { Knex } from 'knex';
import { DBOSJSON } from '../utils';
import knexFactory from 'knex';
import { RecordedStatement } from './migration_types';

// Knex's compiled output shape across builders/raw
type CompiledQueryItem = {
  sql?: string;
  bindings?: unknown[];
  // method, options, etc. exist but aren't needed here
};
type CompiledQuery = CompiledQueryItem | ReadonlyArray<CompiledQueryItem>;

type MinimalBuilder = {
  toSQL(): CompiledQuery;
};

function isFunction(value: unknown): value is (...args: unknown[]) => unknown {
  return typeof value === 'function';
}

function asMinimalBuilder(value: unknown): value is MinimalBuilder {
  return (
    typeof value === 'object' && value !== null && 'toSQL' in value && isFunction((value as { toSQL: unknown }).toSQL)
  );
}

function normalizeCompiled(compiled: CompiledQuery): ReadonlyArray<CompiledQueryItem> {
  const arr: ReadonlyArray<CompiledQueryItem> = Array.isArray(compiled) ? compiled : [compiled];
  return arr;
}

function captureCompiled(compiled: CompiledQuery, into: RecordedStatement[]): void {
  for (const item of normalizeCompiled(compiled)) {
    if (item && typeof item.sql === 'string') {
      into.push({ sql: item.sql, bindings: item.bindings ?? [] });
    }
  }
}

export function makeRealKnex(config: Knex.Config): Knex {
  const knex = knexFactory({ ...config, pool: { min: 0, max: 1 } });
  if (!knex) throw new Error(`Could not make knex`);
  return knex;
}

/**
 * Wrap a Knex instance so:
 *  - any builder you "await" records its toSQL() output and resolves immediately
 *  - knex.raw(...) records immediately and resolves
 *  - selected schema introspection helpers return defaults (false) to keep codegen deterministic
 *      This would only be useful in migration development
 */
export function makeRecordingKnex<K extends Knex>(
  realKnex: K,
): {
  knex: K;
  getStatements: () => ReadonlyArray<RecordedStatement>;
} {
  const statements: RecordedStatement[] = [];

  // Thenable wrapper: on await, capture toSQL() and resolve
  const wrapBuilder = <T extends MinimalBuilder & object>(builder: T): T => {
    const handler: ProxyHandler<T> = {
      get(target, prop, receiver) {
        if (prop === 'then') {
          // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
          return (
            onFulfilled?: (v: unknown) => unknown | Promise<unknown>,
            onRejected?: (r: unknown) => unknown | Promise<unknown>,
          ) => {
            captureCompiled(target.toSQL(), statements);
            return Promise.resolve(undefined).then(onFulfilled, onRejected);
          };
        }
        if (prop === 'catch') {
          // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
          return (onRejected?: (r: unknown) => unknown | Promise<unknown>) =>
            Promise.resolve(undefined).catch(onRejected);
        }
        if (prop === 'finally') {
          // eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
          return (onFinally?: () => unknown | Promise<unknown>) => Promise.resolve(undefined).finally(onFinally);
        }

        const value = Reflect.get(target, prop, receiver);

        if (typeof value === 'function') {
          return (...args: unknown[]) => {
            const result = (value as (...a: ReadonlyArray<unknown>) => unknown).apply(target, args);
            return asMinimalBuilder(result) ? wrapBuilder(result as MinimalBuilder & object) : result;
          };
        }

        return value;
      },
    };
    return new Proxy<T>(builder, handler);
  };

  // raw(): record immediately and resolve
  const wrapRaw = (...args: ReadonlyArray<unknown>): Promise<void> => {
    const compiled = (realKnex as unknown as { raw: (...a: ReadonlyArray<unknown>) => MinimalBuilder })
      .raw(...args)
      .toSQL();
    captureCompiled(compiled, statements);
    return Promise.resolve();
  };

  // schema.* wrapping: builders become "await-record", has*/introspection return defaults
  const wrapSchema = (schemaObj: object): object => {
    const handler: ProxyHandler<object> = {
      get(target, prop, receiver) {
        const value = Reflect.get(target, prop, receiver) as unknown;

        // Introspection helpers -> default false
        if (prop === 'hasTable' || prop === 'hasColumn' || prop === 'hasIndex') {
          console.warn(`Schema introspection detected: ${prop}`);
          return async () => Promise.resolve(false);
        }

        // Methods that return builders -> wrap
        if (typeof value === 'function') {
          return (...args: unknown[]) => {
            const result = (value as (...a: ReadonlyArray<unknown>) => unknown).apply(target, args);
            return asMinimalBuilder(result) ? wrapBuilder(result as MinimalBuilder & object) : result;
          };
        }

        return value;
      },
    };
    return new Proxy<object>(schemaObj, handler);
  };

  // Top-level proxy around the Knex instance (callable + props)
  const handler: ProxyHandler<K> = {
    apply(target, thisArg, argArray) {
      // knex('table') -> QueryBuilder
      const qb = Reflect.apply(
        target as unknown as (...a: ReadonlyArray<unknown>) => unknown,
        thisArg,
        argArray as ReadonlyArray<unknown>,
      );
      return asMinimalBuilder(qb) ? wrapBuilder(qb as MinimalBuilder & object) : qb;
    },
    get(target, prop, receiver) {
      if (prop === 'raw') return wrapRaw;
      if (prop === 'schema') {
        const schemaObj = Reflect.get(target, prop, receiver);
        return wrapSchema(schemaObj);
      }
      return Reflect.get(target, prop, receiver);
    },
  };

  const proxy = new Proxy<K>(realKnex, handler);

  return {
    knex: proxy,
    getStatements: () => statements.slice(),
  };
}

type Dialect = 'pg' | 'sqlite3';

type GenOptions = {
  srcDir: string;
  outDir: string;
  dialects: ReadonlyArray<Dialect>;
};

type MigrationModule = {
  up: (k: Knex) => unknown;
  down?: (k: Knex) => unknown;
};

type PerDialect = Record<Dialect, { up: ReadonlyArray<RecordedStatement>; down: ReadonlyArray<RecordedStatement> }>;

function isMigrationModule(m: unknown): m is MigrationModule {
  return typeof m === 'object' && m !== null && 'up' in m && typeof (m as { up: unknown }).up === 'function';
}

async function loadMigration(modUrl: string): Promise<MigrationModule> {
  const mod = (await import(modUrl)) as unknown;
  if (isMigrationModule(mod)) return mod;
  // CommonJS default export support
  if (isMigrationModule((mod as { default?: unknown }).default)) return (mod as { default: MigrationModule }).default;
  throw new Error(`Module ${modUrl} does not export { up, down }`);
}

async function recordSqlFor(
  migrationFn: (k: Knex) => unknown,
  dialect: Dialect,
): Promise<ReadonlyArray<RecordedStatement>> {
  const real = makeRealKnex({ client: dialect });
  const { knex, getStatements } = makeRecordingKnex(real);
  await migrationFn(knex);
  return getStatements();
}

function escTemplate(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/`/g, '\\`');
}

function serializeBindings(b: ReadonlyArray<unknown> | undefined): string {
  if (!b || b.length === 0) return '[]';
  return DBOSJSON.stringify(b);
}

function toIdentifier(name: string): string {
  return name.replace(/\W+/g, '_').replace(/^(\d)/, '_$1').replace(/_+$/g, '');
}

async function writeOneOutFile(outDir: string, baseName: string, perDialect: Partial<PerDialect>): Promise<void> {
  const id = toIdentifier(baseName);
  const file = path.join(outDir, `${baseName}.ts`);

  const header = `/* Auto-generated from Knex migrations. Do not edit by hand. */
import type { GeneratedMigration, SqlStatement } from "../migration_types";
`;

  const bodies: string[] = [];

  const dialects: ReadonlyArray<Dialect> = ['pg', 'sqlite3'];
  for (const d of dialects) {
    const pd = perDialect[d];
    if (!pd) continue;

    const upArr = pd.up
      .map((s) => `  { sql: \`${escTemplate(s.sql)}\`, bindings: ${serializeBindings(s.bindings)} }`)
      .join(',\n');
    const downArr = pd.down
      .map((s) => `  { sql: \`${escTemplate(s.sql)}\`, bindings: ${serializeBindings(s.bindings)} }`)
      .join(',\n');

    bodies.push(`const up_${d}_${id}: ReadonlyArray<SqlStatement> = [\n${upArr}\n];`);
    bodies.push(`const down_${d}_${id}: ReadonlyArray<SqlStatement> = [\n${downArr}\n];`);
  }

  const mapFor = (dir: 'up' | 'down') =>
    ['pg', 'sqlite3']
      .map((d) => {
        const key = `${dir}_${d}_${id}`;
        return (perDialect as Partial<Record<string, unknown>>)[d] ? `    ${d as Dialect}: ${key}` : '';
      })
      .filter(Boolean)
      .join(',\n  ');

  const footer = `
export const migration: GeneratedMigration = {
  name: "${baseName}",
  up: {
${mapFor('up')}
  },
  down: {
${mapFor('down')}
  },
};
`;

  await fs.writeFile(file, header + bodies.join('\n\n') + footer, 'utf8');
}

async function main(opts: GenOptions, logger?: (l: string) => void): Promise<void> {
  const srcDir = path.resolve(opts.srcDir);
  const outDir = path.resolve(opts.outDir);
  await fs.mkdir(outDir, { recursive: true });
  if (!logger) logger = console.log;

  const files = (await fs.readdir(srcDir)).filter((f) => /\.(t|j)s$/u.test(f)).sort((a, b) => a.localeCompare(b, 'en'));

  const indexExports: string[] = [];
  const indexImports: string[] = [];
  const indexIds: string[] = [];

  let idx = 0;
  for (const file of files) {
    ++idx;
    const id = `sysdb_${idx}`;
    console.log(`Processing: ${file}`);
    const full = path.join(srcDir, file);
    const baseName = path.basename(file, path.extname(file));
    const modUrl = pathToFileURL(full).href;
    indexImports.push(`import { migration as ${id} } from "./${baseName}";`);
    indexIds.push(id);

    const { up, down } = await loadMigration(modUrl);

    const perDialect: Partial<PerDialect> = {};

    for (const d of opts.dialects) {
      const upStmts = await recordSqlFor(up, d);
      const downStmts = down ? await recordSqlFor(down, d) : [];
      perDialect[d] = { up: upStmts, down: downStmts } as {
        up: ReadonlyArray<RecordedStatement>;
        down: ReadonlyArray<RecordedStatement>;
      };
    }

    await writeOneOutFile(outDir, baseName, perDialect);
    indexExports.push(`export { migration as ${toIdentifier(baseName)} } from "./${baseName}";`);
  }

  await fs.writeFile(
    path.join(outDir, 'index.ts'),
    `/* Auto-generated. */
import type { GeneratedMigration } from "../migration_types";
${indexImports.join('\n')}

export const allMigrations: ReadonlyArray<GeneratedMigration> = [
  ${indexIds.join(',\n  ')}
];
`,
    'utf8',
  );
  logger(`Generated ${files.length} migration module(s) to ${outDir}`);
}

if (require.main === module) {
  const srcDir = process.env.MIG_SRC ?? process.argv[2] ?? './migrations';
  const outDir = process.env.MIG_OUT ?? process.argv[3] ?? './src/sysdb_migrations/internal';
  const dialects = (process.env.MIG_DIALECTS ?? process.argv[4] ?? 'pg')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0) as ReadonlyArray<Dialect>;

  void main({ srcDir, outDir, dialects });
}
