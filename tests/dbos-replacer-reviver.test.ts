import { DBOSReviver, DBOSReplacer } from "../src/utils";
import _ from 'lodash';

describe("dbos-json-reviver-replacer", () => {
  test("Replace revive dates", () => {
    const obj = {
      datesObj: {
        date1: new Date(2023, 10, 2, 23, 12, 200),
        date2: new Date(2024, 4, 1, 13, 11, 223)
      },
      datesArray: [
        new Date(2023, 10, 2, 23, 12, 200),
        new Date(2024, 4, 1, 13, 11, 223)
      ],
      date: new Date(2024, 4, 1, 13, 11, 223)
    }
    const stringified = JSON.stringify(obj, DBOSReplacer);
    const parsed = JSON.parse(stringified, DBOSReviver) as typeof obj;
    expect(_.isEqual(obj, parsed)).toBe(true);
  });

  test("Replace revive buffers", () => {
    const obj = {
      stringBuffer: Buffer.from('A utf-8 string', "utf-8"),
      buffers: {
        stringBuffer: Buffer.from('A utf-8 string', "utf-8"),
      },
      bufferArray: [
        Buffer.from('A utf-8 string', "utf-8")
      ],
    }
    const stringified = JSON.stringify(obj, DBOSReplacer);
    const parsed = JSON.parse(stringified, DBOSReviver) as typeof obj;
    expect(_.isEqual(obj, parsed)).toBe(true);
  });
});

