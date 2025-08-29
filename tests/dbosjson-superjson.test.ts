import { DBOSJSON, DBOSJSONLegacy } from '../src/utils';
import superjson from 'superjson';

describe('DBOSJSON with SuperJSON support', () => {
  describe('New SuperJSON serialization', () => {
    test('serializes Sets correctly', () => {
      const set = new Set([1, 2, 3]);
      const serialized = DBOSJSON.stringify(set);
      const deserialized = DBOSJSON.parse(serialized);
      expect(deserialized).toEqual(set);
    });

    test('serializes Maps correctly', () => {
      const map = new Map([
        ['key1', 'value1'],
        ['key2', 'value2'],
      ]);
      const serialized = DBOSJSON.stringify(map);
      const deserialized = DBOSJSON.parse(serialized);
      expect(deserialized).toEqual(map);
    });

    test('serializes undefined values', () => {
      const obj = { defined: 'value', undefined: undefined };
      const serialized = DBOSJSON.stringify(obj);
      const deserialized = DBOSJSON.parse(serialized) as typeof obj;
      expect(deserialized).toEqual(obj);
      expect('undefined' in deserialized).toBe(true);
    });

    test('serializes RegExp', () => {
      const regex = /test.*pattern/gi;
      const serialized = DBOSJSON.stringify(regex);
      const deserialized = DBOSJSON.parse(serialized) as RegExp;
      expect(deserialized.source).toBe(regex.source);
      expect(deserialized.flags).toBe(regex.flags);
    });

    test('serializes NaN and Infinity', () => {
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

      // SuperJSON handles circular references
      const serialized = DBOSJSON.stringify(obj);
      const deserialized = DBOSJSON.parse(serialized) as any;

      expect(deserialized.name).toBe('circular');
      expect(deserialized.self).toBe(deserialized); // Same reference
    });

    test('handles complex nested structures', () => {
      const complex = {
        set: new Set([1, 2, 3]),
        map: new Map([['key', 'value']]),
        date: new Date('2024-01-01'),
        bigint: BigInt(123456789),
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
      expect(deserialized.undefined).toBe(undefined);
      expect(deserialized.null).toBe(null);
      expect(deserialized.nested.regex.source).toBe(complex.nested.regex.source);
      expect(deserialized.nested.array[0]).toEqual(complex.nested.array[0]);
    });
  });

  describe('Backwards compatibility with old DBOSJSON format', () => {
    test('parses legacy DBOSJSON dates correctly', () => {
      const date = new Date('2024-01-01T12:00:00Z');
      const legacySerialized = DBOSJSONLegacy.stringify(date);
      const deserialized = DBOSJSON.parse(legacySerialized);
      expect(deserialized).toEqual(date);
    });

    test('parses legacy DBOSJSON BigInts correctly', () => {
      const bigint = BigInt(123456789);
      const legacySerialized = DBOSJSONLegacy.stringify(bigint);
      const deserialized = DBOSJSON.parse(legacySerialized);
      expect(deserialized).toEqual(bigint);
    });

    test('parses legacy DBOSJSON complex objects', () => {
      const obj = {
        date: new Date('2024-01-01'),
        bigint: BigInt(42),
        nested: {
          anotherDate: new Date('2024-12-31'),
          array: [BigInt(1), BigInt(2)],
        },
      };
      const legacySerialized = DBOSJSONLegacy.stringify(obj);
      const deserialized = DBOSJSON.parse(legacySerialized);
      expect(deserialized).toEqual(obj);
    });

    test('parses manually created SuperJSON format', () => {
      const set = new Set([1, 2, 3]);
      const superjsonSerialized = JSON.stringify(superjson.serialize(set));
      const deserialized = DBOSJSON.parse(superjsonSerialized);
      expect(deserialized).toEqual(set);
    });

    test('parses plain JSON correctly', () => {
      const plain = { simple: 'object', number: 42, bool: true };
      const plainSerialized = JSON.stringify(plain);
      const deserialized = DBOSJSON.parse(plainSerialized);
      expect(deserialized).toEqual(plain);
    });

    test('handles null values', () => {
      expect(DBOSJSON.parse(null)).toBe(null);
      expect(DBOSJSON.stringify(null)).toBe('{"json":null}');
    });
  });

  describe('Compatibility between old and new serialization', () => {
    test('new DBOSJSON can parse data serialized with old DBOSJSON', () => {
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

    test('dates and bigints work in both formats', () => {
      const data = {
        date: new Date('2024-06-15T10:30:00Z'),
        bigint: BigInt('12345678901234567890'),
        regular: 'string',
      };

      // Old format
      const oldSerialized = DBOSJSONLegacy.stringify(data);
      const fromOld = DBOSJSON.parse(oldSerialized);
      expect(fromOld).toEqual(data);

      // New format
      const newSerialized = DBOSJSON.stringify(data);
      const fromNew = DBOSJSON.parse(newSerialized);
      expect(fromNew).toEqual(data);
    });
  });
});
