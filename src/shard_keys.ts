// Shard keys a caller supplies for the workflows it addresses, read only by a sharded system database.

import { AsyncLocalStorage } from 'async_hooks';
import { inspect } from 'util';
import { DBOSError } from './error';

interface ShardKeyState {
  // Workflow ID -> the shard key it lives under, for the workflows a caller addresses.
  bindings?: ReadonlyMap<string, string>;
  // The shard key of statements that address no single workflow.
  scope?: string;
}

const shardKeyState = new AsyncLocalStorage<ShardKeyState>();

/** Throw unless `shardKey` is undefined, null, or a non-empty string. */
export function validateShardKey(shardKey: unknown): void {
  if (shardKey !== undefined && shardKey !== null && (typeof shardKey !== 'string' || shardKey === '')) {
    throw new DBOSError(`Invalid shard key ${inspect(shardKey)}: a shard key must be a non-empty string`);
  }
}

/** Run `callback` with each workflow ID in `keys` living under its shard key. Undefined and null keys are ignored. */
export function bindShardKeys<R>(
  keys: Iterable<readonly [string, string | null | undefined]> | Record<string, string | null | undefined>,
  callback: () => R,
): R {
  const entries = Symbol.iterator in keys ? Array.from(keys) : Object.entries(keys);
  const given = entries.filter((entry): entry is [string, string] => entry[1] !== undefined && entry[1] !== null);
  for (const [, key] of given) {
    validateShardKey(key);
  }
  if (given.length === 0) {
    return callback();
  }
  const current = shardKeyState.getStore();
  const bindings = new Map(current?.bindings);
  for (const [workflowID, key] of given) {
    bindings.set(workflowID, key);
  }
  return shardKeyState.run({ ...current, bindings }, callback);
}

/** The shard key bound for this workflow ID, if any. */
export function boundShardKey(workflowID: string): string | undefined {
  return shardKeyState.getStore()?.bindings?.get(workflowID);
}

/** Run `callback` with statements not addressing a specific workflow running under `key`. */
export function shardKeyScope<R>(key: string | null | undefined, callback: () => R): R {
  validateShardKey(key);
  return shardKeyState.run({ ...shardKeyState.getStore(), scope: key ?? undefined }, callback);
}

/** The shard key of the enclosing scope, if any. */
export function scopedShardKey(): string | undefined {
  return shardKeyState.getStore()?.scope;
}

/** Run `callback` with every binding and scope emptied, so a workflow body inherits none from its caller. */
export function withClearedShardKeyState<R>(callback: () => R): R {
  return shardKeyState.run({}, callback);
}
