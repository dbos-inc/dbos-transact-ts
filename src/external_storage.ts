// External storage: values kept in an external store, checkpointed as small refs.

import { DBOSInitializationError } from './error';

/** A reference to a value stored with `DBOS.storeExternalObject`. */
export interface ExternalObjectRef<T = unknown> {
  version: number;
  store: string;
  key: string;
  size: number;
  sha256: string;
  serialization: string;
  encoding: string;
  /** Never set: carries the stored value's type, so loading a ref returns it. */
  readonly __valueType?: T;
}

/**
 * Where external objects' bytes live; each call retries its own transient failures.
 * Its `storeID` names the location, so it must not change while refs to it exist.
 */
export interface ExternalStore {
  readonly storeID: string;
  put(key: string, data: Uint8Array): Promise<void>;
  /** The object's bytes, or undefined if no object has this key. */
  get(key: string): Promise<Uint8Array | undefined>;
  /** Delete these objects; keys with no object are ignored. */
  delete(keys: string[]): Promise<void>;
}

/** The configured stores, each with its own storeID; empty means none. */
export function checkExternalStores(stores: unknown): ExternalStore[] {
  if (stores === undefined || stores === null) {
    return [];
  }
  if (!Array.isArray(stores)) {
    const type = (stores as object).constructor?.name ?? typeof stores;
    throw new DBOSInitializationError(`externalStores must be an array of stores, not a single ${type}; pass [store]`);
  }
  const configured = stores as ExternalStore[];
  const ids = configured.map((store) => store.storeID);
  const repeated = [...new Set(ids.filter((id, i) => ids.indexOf(id) !== i))].sort();
  if (repeated.length > 0) {
    throw new DBOSInitializationError(
      `Each external store needs its own storeID, but these repeat: ${repeated.join(', ')}`,
    );
  }
  return [...configured];
}
