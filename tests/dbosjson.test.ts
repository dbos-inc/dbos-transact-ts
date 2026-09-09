import {
  DBOSJSON,
  DBOSJSONLegacy,
  SERIALIZER_MARKER_KEY,
  SERIALIZER_MARKER_VALUE,
  deserializeResError,
  serializeResErrorWithSerializer,
} from '../src/serialization';

/**
 * DBOSJSON uses SuperJSON internally for rich type support.
 *
 * Test structure:
 * 1. "dbos-json-reviver-replacer" - (dates, bigints, buffers)
 * 2. "SuperJSON enhanced types" - (Sets, Maps, undefined, RegExp, etc.)
 * 3. "Backwards compatibility" - Verify old database data still works
 * 4. "Error serialization" - (cause chains, AggregateError, non-Error throws)
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
    // These are legacy cases
    expect(DBOSJSON.parse(null)).toBe(null);
    expect(DBOSJSON.parse(undefined)).toBe(null);

    expect(DBOSJSON.parse(DBOSJSON.stringify(null))).toBeNull();
    expect(DBOSJSON.parse(DBOSJSON.stringify(undefined))).toBeUndefined();
    expect(DBOSJSON.stringify(null)).toBeTruthy();
    expect(DBOSJSON.stringify(undefined)).toBeTruthy();
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
    const parsed = JSON.parse(serialized) as { json?: unknown };

    expect(parsed).toHaveProperty('json');
    expect(parsed).toHaveProperty(SERIALIZER_MARKER_KEY, SERIALIZER_MARKER_VALUE);
    expect(parsed.json).toEqual(simpleValue);

    // Complex types also get our marker
    const complexValue = new Set([1, 2, 3]);
    const complexSerialized = DBOSJSON.stringify(complexValue);
    const complexParsed = JSON.parse(complexSerialized) as { json?: unknown };

    expect(complexParsed).toHaveProperty('json');
    expect(complexParsed).toHaveProperty(SERIALIZER_MARKER_KEY, SERIALIZER_MARKER_VALUE);
    expect(complexParsed).toHaveProperty('meta'); // Complex types have meta from SuperJSON
  });

  test('parses an error row written before causes were captured', async () => {
    // A workflow_status.error row as written before cause chains were captured.
    const legacyRow = JSON.stringify({
      json: {
        name: 'Error',
        message: 'External Test endpoint failed',
        stack: 'Error: External Test endpoint failed\n    at sendTestEvent (/project/src/test_V1.ts:52:19)',
      },
      [SERIALIZER_MARKER_KEY]: SERIALIZER_MARKER_VALUE,
    });

    const restored = await deserializeResError(legacyRow, DBOSJSON.name(), DBOSJSON);

    expect(restored).toBeInstanceOf(Error);
    expect(restored.name).toBe('Error');
    expect(restored.message).toBe('External Test endpoint failed');
    expect(restored.stack).toContain('sendTestEvent');
    expect(restored.cause).toBeUndefined();
  });

  test('parses an error row holding a bare string', async () => {
    const bareRow = JSON.stringify({ json: 'something went wrong', [SERIALIZER_MARKER_KEY]: SERIALIZER_MARKER_VALUE });

    await expect(deserializeResError(bareRow, DBOSJSON.name(), DBOSJSON)).resolves.toBeInstanceOf(Error);
  });
});

describe('Error serialization', () => {
  const roundTrip = async (err: Error) => {
    const { serializedValue, serialization } = await serializeResErrorWithSerializer(err, DBOSJSON, DBOSJSON.name());
    return { serializedValue, restored: await deserializeResError(serializedValue, serialization, DBOSJSON) };
  };

  test('stores and restores a cause', async () => {
    const { serializedValue, restored } = await roundTrip(new Error('outer', { cause: new Error('inner') }));

    expect(serializedValue).toContain('inner');
    expect(restored.message).toBe('outer');
    expect(restored.cause).toBeInstanceOf(Error);
    expect((restored.cause as Error).message).toBe('inner');
  });

  test('stores and restores a nested cause chain', async () => {
    const { restored } = await roundTrip(new Error('a', { cause: new Error('b', { cause: new Error('c') }) }));

    const b = restored.cause as Error;
    const c = b.cause as Error;
    expect(b.message).toBe('b');
    expect(c).toBeInstanceOf(Error);
    expect(c.message).toBe('c');
  });

  test('keeps custom properties on the cause', async () => {
    const inner = new Error('inner');
    (inner as Error & { code: string }).code = 'E_INNER';
    const { restored } = await roundTrip(new Error('outer', { cause: inner }));

    expect((restored.cause as Error & { code?: string }).code).toBe('E_INNER');
  });

  test('stores the members of an AggregateError', async () => {
    const { serializedValue } = await roundTrip(new AggregateError([new Error('e1'), new Error('e2')], 'many'));

    expect(serializedValue).toContain('e1');
    expect(serializedValue).toContain('e2');
  });

  test('wraps a thrown non-Error rather than storing it bare', async () => {
    const { serializedValue } = await roundTrip('a bare string' as unknown as Error);

    expect(serializedValue).toContain('Non-error value');
    expect(serializedValue).toContain('a bare string');
  });
});
