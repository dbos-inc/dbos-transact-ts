import { DBOSJSON } from '../src/utils';

/**
 * DBOSJSON uses SuperJSON internally for rich type support.
 *
 * Test structure:
 * 1. "dbos-json-reviver-replacer" - (dates, bigints, buffers)
 * 2. "SuperJSON enhanced types" - (Sets, Maps, undefined, RegExp, etc.)
 * 3. "Backwards compatibility" - Verify old database data still works
 *
 * Caution: Altering results of this test likely means a backward compatibility break.
 */
describe('dbos-json-reviver-replacer', () => {
  test('Replace revive dates', () => {
    const obj = {
      datesObj: {
        date1: new Date(2023, 10, 2, 23, 12, 200),
        date2: new Date(2024, 4, 1, 13, 11, 223),
      },
      datesArray: [new Date(2023, 10, 2, 23, 12, 200), new Date(2024, 4, 1, 13, 11, 223)],
      date: new Date(2024, 4, 1, 13, 11, 223),
    };
    const stringified = DBOSJSON.stringify(obj);
    const parsed = DBOSJSON.parse(stringified) as typeof obj;
    expect(parsed).toEqual(obj);
  });

  test('Replace revive buffers', () => {
    const obj = {
      stringBuffer: Buffer.from('A utf-8 string', 'utf-8'),
      buffers: {
        stringBuffer: Buffer.from('A utf-8 string', 'utf-8'),
      },
      bufferArray: [Buffer.from('A utf-8 string', 'utf-8')],
    };
    const stringified = DBOSJSON.stringify(obj);
    const parsed = DBOSJSON.parse(stringified) as typeof obj;
    expect(parsed).toEqual(obj);
  });

  test('Replace revive bigint', () => {
    const obj = {
      value: BigInt('12345678901234567890'),
      values: {
        value: BigInt('12345678901234567890'),
      },
      valueArray: [BigInt('12345678901234567890'), BigInt('12345678901234567890'), BigInt('12345678901234567890')],
    };
    const stringified = DBOSJSON.stringify(obj);
    const parsed = DBOSJSON.parse(stringified) as typeof obj;
    expect(parsed).toEqual(obj);
  });
});

describe('SuperJSON enhanced types', () => {
  test('serializes Sets', () => {
    const set = new Set([1, 2, 3]);
    const serialized = DBOSJSON.stringify(set);
    const deserialized = DBOSJSON.parse(serialized);
    expect(deserialized).toEqual(set);
  });

  test('serializes Maps', () => {
    const map = new Map([
      ['key1', 'value1'],
      ['key2', 'value2'],
    ]);
    const serialized = DBOSJSON.stringify(map);
    const deserialized = DBOSJSON.parse(serialized);
    expect(deserialized).toEqual(map);
  });

  test('preserves undefined values', () => {
    const obj = { defined: 'value', undefined: undefined };
    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as typeof obj;
    expect(deserialized).toEqual(obj);
    expect('undefined' in deserialized).toBe(true);
  });

  test('serializes RegExp patterns', () => {
    const regex = /test.*pattern/gi;
    const serialized = DBOSJSON.stringify(regex);
    const deserialized = DBOSJSON.parse(serialized) as RegExp;
    expect(deserialized.source).toBe(regex.source);
    expect(deserialized.flags).toBe(regex.flags);
  });

  test('handles NaN and Infinity', () => {
    const obj = {
      nan: Number.NaN,
      inf: Number.POSITIVE_INFINITY,
      negInf: Number.NEGATIVE_INFINITY,
    };
    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as typeof obj;
    expect(Number.isNaN(deserialized.nan)).toBe(true);
    expect(deserialized.inf).toBe(Number.POSITIVE_INFINITY);
    expect(deserialized.negInf).toBe(Number.NEGATIVE_INFINITY);
  });

  test('handles null and undefined', () => {
    const obj = {
      undefined: undefined,
      null: null,
    };
    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as typeof obj;
    expect(deserialized.undefined).toBeUndefined();
    expect(deserialized.null).toBeNull();
  });

  test('handles circular references', () => {
    type Obj = { name: string; self?: Obj };
    const obj: Obj = { name: 'circular' };
    obj.self = obj;

    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as Obj;

    expect(deserialized.name).toBe('circular');
    expect(deserialized.self).toBe(deserialized); // Same reference
  });

  test('complex nested structures with mixed types', () => {
    const complex = {
      set: new Set([1, 2, 3]),
      map: new Map([['key', 'value']]),
      date: new Date('2024-01-01'),
      bigint: BigInt(123456789),
      buffer: Buffer.from('test'),
      undefined: undefined,
      null: null,
      nested: {
        regex: /pattern/g,
        array: [new Set([4, 5, 6])],
      },
    };
    const serialized = DBOSJSON.stringify(complex);
    const deserialized = DBOSJSON.parse(serialized) as typeof complex;

    expect(deserialized.set).toEqual(complex.set);
    expect(deserialized.map).toEqual(complex.map);
    expect(deserialized.date).toEqual(complex.date);
    expect(deserialized.bigint).toEqual(complex.bigint);
    expect(deserialized.buffer).toEqual(complex.buffer);
    expect(deserialized.undefined).toBe(undefined);
    expect(deserialized.null).toBe(null);
    expect(deserialized.nested.regex.source).toBe(complex.nested.regex.source);
    expect(deserialized.nested.array[0]).toEqual(complex.nested.array[0]);
  });
});
