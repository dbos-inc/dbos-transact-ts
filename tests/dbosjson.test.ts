import { DBOSJSON, DBOSJSONLegacy } from '../src/utils';
import superjson from 'superjson';

/**
 * DBOSJSON was upgraded to use SuperJSON internally for richer type support.
 *
 * What changed: DBOSJSON.stringify() now uses SuperJSON, creating a different format.
 * Why it matters: Production databases contain millions of rows serialized with the OLD format.
 * The requirement: New DBOSJSON MUST deserialize both old AND new formats perfectly.
 *
 * Test structure:
 * 1. "dbos-json-reviver-replacer" - Original features that already worked (dates, bigints, buffers)
 * 2. "SuperJSON enhanced types" - New capabilities we added (Sets, Maps, undefined, RegExp, etc.)
 * 3. "Backwards compatibility" - THE CRITICAL TESTS that verify old database data still works
 *
 * If backwards compatibility tests fail, DO NOT MERGE. It means the upgrade will break
 * production by making existing database data unreadable.
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
    const obj = { nan: NaN, inf: Infinity, negInf: -Infinity };
    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as typeof obj;
    expect(Number.isNaN(deserialized.nan)).toBe(true);
    expect(deserialized.inf).toBe(Infinity);
    expect(deserialized.negInf).toBe(-Infinity);
  });

  test('handles circular references', () => {
    const obj: any = { name: 'circular' };
    obj.self = obj;

    const serialized = DBOSJSON.stringify(obj);
    const deserialized = DBOSJSON.parse(serialized) as any;

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
    const deserialized = DBOSJSON.parse(serialized) as any;

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

describe('Backwards compatibility', () => {
  /**
   * These tests simulate reading data that's ALREADY in production databases.
   *
   * Context: Before this PR, DBOSJSON used custom replacer/reviver functions that created
   * formats like {"dbos_type": "dbos_Date", "dbos_data": "2024-01-01T00:00:00.000Z"}.
   *
   * After this PR: DBOSJSON creates SuperJSON format like {"json": {...}, "meta": {...}}.
   *
   * The problem: Millions of workflow states in production were serialized with the OLD format.
   * The solution: DBOSJSON.parse() now detects which format and uses the appropriate deserializer.
   *
   * These tests verify that solution works by:
   * 1. Using DBOSJSONLegacy (the old implementation) to create old-format strings
   * 2. Parsing those strings with the NEW DBOSJSON
   * 3. Verifying the data is correctly restored
   *
   * If these tests fail, existing production data becomes unreadable. DO NOT MERGE if broken.
   */

  test('parses legacy DBOSJSON dates', () => {
    const date = new Date('2024-01-01T12:00:00Z');
    const legacySerialized = DBOSJSONLegacy.stringify(date);
    const deserialized = DBOSJSON.parse(legacySerialized);
    expect(deserialized).toEqual(date);
  });

  test('parses legacy DBOSJSON BigInts', () => {
    const bigint = BigInt(123456789);
    const legacySerialized = DBOSJSONLegacy.stringify(bigint);
    const deserialized = DBOSJSON.parse(legacySerialized);
    expect(deserialized).toEqual(bigint);
  });

  test('parses legacy DBOSJSON complex objects', () => {
    const obj = {
      date: new Date('2024-01-01'),
      bigint: BigInt(42),
      buffer: Buffer.from('legacy'),
      nested: {
        anotherDate: new Date('2024-12-31'),
        array: [BigInt(1), BigInt(2)],
      },
    };
    const legacySerialized = DBOSJSONLegacy.stringify(obj);
    const deserialized = DBOSJSON.parse(legacySerialized);
    expect(deserialized).toEqual(obj);
  });

  test('new DBOSJSON can parse old database data', () => {
    // Simulate data that was stored in DB with old DBOSJSON
    const originalData = {
      id: 'test-123',
      createdAt: new Date('2023-01-01'),
      count: BigInt(999999999999),
      metadata: { key: 'value' },
    };

    // This is what's in the database (serialized with old format)
    const dbStored = DBOSJSONLegacy.stringify(originalData);

    // New DBOSJSON should be able to parse it
    const parsed = DBOSJSON.parse(dbStored);
    expect(parsed).toEqual(originalData);
  });

  test('handles null correctly', () => {
    expect(DBOSJSON.parse(null)).toBe(null);
    // Includes our marker to avoid ambiguity
    const nullSerialized = DBOSJSON.stringify(null);
    const nullParsed = JSON.parse(nullSerialized);
    expect(nullParsed).toHaveProperty('json', null);
    expect(nullParsed).toHaveProperty('__serializer', 'superjson');
  });

  test('parses plain JSON', () => {
    const plain = { simple: 'object', number: 42, bool: true };
    const plainSerialized = JSON.stringify(plain);
    const deserialized = DBOSJSON.parse(plainSerialized);
    expect(deserialized).toEqual(plain);
  });

  test('does not confuse user data with SuperJSON format', () => {
    // User data that happens to have a 'json' field should not be treated as SuperJSON
    const userDataWithJson = {
      json: { some: 'data' },
      otherField: 'value',
      anotherField: 123,
    };
    const serialized = JSON.stringify(userDataWithJson);
    const deserialized = DBOSJSON.parse(serialized);
    expect(deserialized).toEqual(userDataWithJson);

    // Critical case: {json: {foo: 'bar'}} should be treated as user data, not SuperJSON
    const ambiguousUserData = { json: { foo: 'bar' } };
    const ambiguousSerialized = JSON.stringify(ambiguousUserData);
    const ambiguousDeserialized = DBOSJSON.parse(ambiguousSerialized);
    expect(ambiguousDeserialized).toEqual(ambiguousUserData); // Should preserve the structure!

    // Only {json, meta} together should be treated as SuperJSON
    const moreAmbiguous = { json: 'test', someOtherProp: true };
    const moreSerialized = JSON.stringify(moreAmbiguous);
    const moreDeserialized = DBOSJSON.parse(moreSerialized);
    expect(moreDeserialized).toEqual(moreAmbiguous);
  });

  test('new DBOSJSON always includes serializer marker to avoid ambiguity', () => {
    // Simple values should get our marker
    const simpleValue = { foo: 'bar' };
    const serialized = DBOSJSON.stringify(simpleValue);
    const parsed = JSON.parse(serialized);

    expect(parsed).toHaveProperty('json');
    expect(parsed).toHaveProperty('__serializer', 'superjson');
    expect(parsed.json).toEqual(simpleValue);

    // Complex types also get our marker
    const complexValue = new Set([1, 2, 3]);
    const complexSerialized = DBOSJSON.stringify(complexValue);
    const complexParsed = JSON.parse(complexSerialized);

    expect(complexParsed).toHaveProperty('json');
    expect(complexParsed).toHaveProperty('__serializer', 'superjson');
    expect(complexParsed).toHaveProperty('meta'); // Complex types have meta from SuperJSON
  });
});
