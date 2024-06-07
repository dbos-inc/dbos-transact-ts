import { createPlaceholders, prepareForSQL } from "../src/utils_sql";
import _ from 'lodash';

describe("sql utils", () => {
  test("createPlaceholders test", () => {
    const placeholders = createPlaceholders(8).join(',');
    expect(placeholders).toEqual('$1,$2,$3,$4,$5,$6,$7,$8');
  });

  test("createPlaceholders with offset test", () => {
    const placeholders = createPlaceholders(8, 7).join(',');
    expect(placeholders).toEqual('$8,$9,$10,$11,$12,$13,$14,$15');
  });

  test("prepareForSQL test", () => {
    const data = {
      "a": "a",
      "b": "b",
      "c": "c",
      "d": "d",
      "e": "e",
      "f": "f"
    };
    const {columns, placeholders, values} = prepareForSQL(data)
    expect(_.isEqual(values, Object.values(data))).toBe(true)
    expect(columns).toEqual('a,b,c,d,e,f')
    expect(placeholders).toEqual('$1,$2,$3,$4,$5,$6')
  });
});
