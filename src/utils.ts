import fs from 'node:fs';
import path from 'node:path';

/*
 * A wrapper of readFile used for mocking in tests
 **/
export async function readFile(path: string, encoding: BufferEncoding = 'utf8'): Promise<string> {
  return await fs.promises.readFile(path, { encoding });
}

function loadDbosVersion(): string {
  try {
    function findPackageRoot(start: string | string[]): string {
      if (typeof start === 'string') {
        if (!start.endsWith(path.sep)) {
          start += path.sep;
        }
        start = start.split(path.sep);
      }

      if (start.length === 0) {
        throw new Error('package.json not found in path');
      }

      start.pop();
      const dir = start.join(path.sep);

      if (fs.existsSync(path.join(dir, 'package.json'))) {
        return dir;
      }

      return findPackageRoot(start);
    }
    const packageJsonPath = path.join(findPackageRoot(__dirname), 'package.json');
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const packageJson = require(packageJsonPath) as { version: string };
    return packageJson.version;
  } catch (error) {
    // Return "unknown" if package.json cannot be found or loaded
    // This can happen in bundled environments where the file system structure is different
    return 'unknown';
  }
}

// Enable OTLP by default only in DBOS Cloud. Otherwise, enable through configuration.
export function defaultEnableOTLP() {
  return process.env.DBOS__CLOUD === 'true';
}

export const globalParams = {
  appVersion: process.env.DBOS__APPVERSION || '', // The one true source of appVersion
  wasComputed: false, // Was app version set or computed? Stored procs don't support computed versions.
  executorID: process.env.DBOS__VMID || 'local', // The one true source of executorID
  appID: process.env.DBOS__APPID || '', // The one true source of appID
  enableOTLP: defaultEnableOTLP(), // Whether OTLP is enabled
  dbosVersion: loadDbosVersion(), // The version of the DBOS library
};
export const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

// The name of the internal queue used by DBOS
export const INTERNAL_QUEUE_NAME = '_dbos_internal_queue';
export const DEBOUNCER_WORKLOW_NAME = '_dbos_debouncer_workflow';

/*
A cancellable sleep function that returns a promise and a callback
The promise can be awaited for and will automatically resolve after the given time
When cancel is called, not only it clears the timeout, but also resolves the promise
So any waiters on the cancelable sleep will be resolved
*/
export function cancellableSleep(ms: number) {
  let timeoutId: ReturnType<typeof setTimeout> | undefined = undefined;
  let resolvePromise: () => void;
  let resolved = false;

  const promise = new Promise<void>((resolve) => {
    resolvePromise = () => {
      if (resolved) return;
      resolved = true;
      resolve();
      timeoutId = undefined;
    };
    timeoutId = setTimeout(resolvePromise, ms);
  });

  const cancel = () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = undefined;
      resolvePromise();
    }
  };

  return { promise, cancel };
}

export type ValuesOf<T> = T[keyof T];

export function exhaustiveCheckGuard(_: never): never {
  throw new Error('Exaustive matching is not applied');
}

// Capture original functions
const originalStdoutWrite = process.stdout.write.bind(process.stdout);
const originalStderrWrite = process.stderr.write.bind(process.stderr);

export function interceptStreams(onMessage: (msg: string, stream: 'stdout' | 'stderr') => void) {
  const intercept = (stream: 'stdout' | 'stderr', originalWrite: typeof process.stdout.write) => {
    return (
      chunk: Uint8Array | string,
      encodingOrCb?: BufferEncoding | ((err?: Error | null) => void),
      cb?: (err?: Error | null) => void,
    ): boolean => {
      const message = chunk.toString();
      onMessage(message, stream);
      if (typeof encodingOrCb === 'function') {
        return originalWrite(chunk, encodingOrCb); // Handle case where encodingOrCb is a callback
      }
      return originalWrite(chunk, encodingOrCb as BufferEncoding, cb); // Handle case where encodingOrCb is a BufferEncoding
    };
  };

  process.stdout.write = intercept('stdout', originalStdoutWrite);
  process.stderr.write = intercept('stderr', originalStderrWrite);
}

// The `pg` package we use does not parse the connect_timeout parameter, so we need to handle it ourselves.
export function getClientConfig(databaseUrl: string | URL) {
  const connectionString = typeof databaseUrl === 'string' ? databaseUrl : databaseUrl.toString();
  const timeout = getTimeout(typeof databaseUrl === 'string' ? new URL(databaseUrl) : databaseUrl);
  return {
    connectionString,
    connectionTimeoutMillis: timeout ? timeout * 1000 : 10000,
  };

  function getTimeout(url: URL) {
    try {
      const $timeout = url.searchParams.get('connect_timeout');
      return $timeout ? Number.parseInt($timeout, 10) : undefined;
    } catch {
      // Ignore errors in parsing the connect_timeout parameter
      return undefined;
    }
  }
}
