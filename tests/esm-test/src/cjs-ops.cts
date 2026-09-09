import { DBOS } from '@dbos-inc/dbos-sdk';
import { serializeError } from 'serialize-error';

// A CommonJS module in an ESM app: its require() of an ESM-only package needs Node >= 20.19.
export class CjsOps {
  @DBOS.step()
  static describeError(): Promise<string> {
    return Promise.resolve(String(serializeError(new Error('from cjs')).message));
  }
}
