import { DBOSJSON } from '../src/utils';

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
