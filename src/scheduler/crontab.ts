// This code was based on code from node-cron 4.6.0:
//   https://github.com/node-cron/node-cron
/*
ISC License
Copyright (c) 2016, Lucas Merencia <lucas.merencia@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any
 purpose with or without fee is hereby granted, provided that the above
 copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

//////////
/// Expression conversion
//////////

type FieldValue = number | string;

// Fields in order: second, minute, hour, day of month, month, day of week
type CronFields = [number[], number[], number[], FieldValue[], number[], FieldValue[]];

const NICKNAMES: Record<string, string> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
};

function resolveNickname(expression: string): string {
  return NICKNAMES[expression.trim().toLowerCase()] ?? expression;
}

function removeExtraSpaces(str: string): string {
  return str.replace(/\s{2,}/g, ' ').trim();
}

function prependSecondExpression(expressions: string[]): string[] {
  if (expressions.length === 5) {
    return ['0'].concat(expressions);
  }
  return expressions;
}

// `?` is an alias for `*`, accepted only as a whole day-of-month or day-of-week field
function convertQuestionMarks(expressions: string[]): string[] {
  if (expressions[3] === '?') expressions[3] = '*';
  if (expressions[5] === '?') expressions[5] = '*';
  return expressions;
}

const months = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];
const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

function convertMonthNameI(expression: string, items: string[]): string {
  for (let i = 0; i < items.length; i++) {
    expression = expression.replace(new RegExp(items[i], 'gi'), `${i + 1}`);
  }
  return expression;
}

function monthNamesConversion(monthExpression: string): string {
  monthExpression = convertMonthNameI(monthExpression, months);
  monthExpression = convertMonthNameI(monthExpression, shortMonths);
  return monthExpression;
}

const weekDays = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
const shortWeekDays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

function convertWeekDayName(weekExpression: string, items: string[]) {
  for (let i = 0; i < items.length; i++) {
    weekExpression = weekExpression.replace(new RegExp(items[i], 'gi'), `${i}`);
  }
  return weekExpression;
}

function weekDayNamesConversion(expression: string) {
  expression = convertWeekDayName(expression, weekDays);
  return convertWeekDayName(expression, shortWeekDays);
}

// Converts every `*` token in a comma list, not just the first
function convertAsterisk(expression: string, replacement: string) {
  return expression
    .split(',')
    .map((token) => (token.indexOf('*') !== -1 ? token.replace('*', replacement) : token))
    .join(',');
}

// Based on position in full crontab, convert asterisk to appropriate range
function convertAsterisksToRanges(expressions: string[]) {
  expressions[0] = convertAsterisk(expressions[0], '0-59');
  expressions[1] = convertAsterisk(expressions[1], '0-59');
  expressions[2] = convertAsterisk(expressions[2], '0-23');
  expressions[3] = convertAsterisk(expressions[3], '1-31');
  expressions[4] = convertAsterisk(expressions[4], '1-12');
  expressions[5] = convertAsterisk(expressions[5], '0-6');
  return expressions;
}

// Only whole `n-n` or `n-n/step` tokens are expanded; malformed ones are left for validation to reject
const rangeRegEx = /^(\d+)-(\d+)(?:\/(\d+))?$/;

const FIELD_BOUNDS = [
  { min: 0, max: 59 },
  { min: 0, max: 59 },
  { min: 0, max: 23 },
  { min: 1, max: 31 },
  { min: 1, max: 12 },
  { min: 0, max: 6 },
];

function expandRange(initTxt: string, endTxt: string, stepTxt: string, bounds: { min: number; max: number }) {
  const step = parseInt(stepTxt, 10);
  // A non-positive step would never terminate; leave the token for validation to reject.
  if (!(step >= 1)) return `${initTxt}-${endTxt}/${stepTxt}`;

  const first = parseInt(initTxt, 10);
  const last = parseInt(endTxt, 10);

  const numbers: number[] = [];
  if (first <= last) {
    for (let i = first; i <= last; i += step) {
      numbers.push(i);
    }
    return numbers.join();
  }

  // An inverted range wraps through the field's upper bound (e.g. hours `22-2` -> 22,23,0,1,2)
  const { min, max } = bounds;
  const size = max - min + 1;
  const span = (((last - first) % size) + size) % size;
  for (let offset = 0; offset <= span; offset += step) {
    let value = first + offset;
    if (value > max) value -= size;
    numbers.push(value);
  }
  return numbers.join();
}

function convertRange(expression: string, bounds: { min: number; max: number }) {
  return expression
    .split(',')
    .map((token) => {
      const match = rangeRegEx.exec(token.trim());
      return match ? expandRange(match[1], match[2], match[3] || '1', bounds) : token;
    })
    .join();
}

function convertAllRanges(expressions: string[]) {
  for (let i = 0; i < expressions.length; i++) {
    expressions[i] = convertRange(expressions[i], FIELD_BOUNDS[i]);
  }
  return expressions;
}

// Parses integers, keeping the L / L-n / nW / LW / nL / n#m tokens as uppercase literals
function normalizeIntegers(expressions: string[]): FieldValue[][] {
  return expressions.map((expression) =>
    expression.split(',').map((raw) => {
      const token = raw.trim();
      if (/^\d+$/.test(token)) return parseInt(token, 10);
      if (/^l$/i.test(token) || /^l-\d{1,2}$/i.test(token) || /^[0-7]l$/i.test(token) || /w/i.test(token)) {
        return token.toUpperCase();
      }
      // Anything else is kept verbatim so validation rejects it rather than parseInt truncating it
      return token;
    }),
  );
}

/*
 * Converts a crontab into six arrays of allowed values, translating month and week day names,
 * asterisks, ranges and steps into integers.
 *
 * Month names example:
 *  - expression 0 1 1 January,Sep *
 *  - Will be translated to 0 1 1 1,9 *
 *
 * Week day names example:
 *  - expression 0 1 1 2 Monday,Sat
 *  - Will be translated to 0 1 1 1,5 *
 *
 * Ranges example:
 *  - expression 1-5 * * * *
 *  - Will be translated to 1,2,3,4,5 * * * *
 */
export function convertExpression(crontab: string): FieldValue[][] {
  let expressions = removeExtraSpaces(resolveNickname(crontab)).split(' ');
  expressions = prependSecondExpression(expressions);
  expressions = convertQuestionMarks(expressions);
  expressions[4] = monthNamesConversion(expressions[4]);
  expressions[5] = weekDayNamesConversion(expressions[5]);
  expressions = convertAsterisksToRanges(expressions);
  expressions = convertAllRanges(expressions);

  const fields = normalizeIntegers(expressions);

  // Fold day-of-week 7 (Sunday) into 0 only after range expansion, so ranges like 5-7 survive
  fields[5] = [
    ...new Set(
      fields[5].map((value) => {
        if (value === 7) return 0;
        if (typeof value === 'string' && value.startsWith('7')) return '0' + value.slice(1);
        return value;
      }),
    ),
  ];

  return fields;
}

//////////
/// Validation
//////////

const validationRegex = /^(?:\d+|\*|\*\/\d+)$/;

// `#` is allowed for the day-of-week `n#m` token and `?` for the day-field alias
const ALLOWED_CHARS_REGEX = /^[a-zA-Z0-9-*/,#? ]+$/;

// Check a field's values to see if they are in range
function isValidExpression(values: FieldValue[], min: number, max: number): boolean {
  for (const value of values) {
    const valueAsInt = parseInt(`${value}`, 10);

    if ((!Number.isNaN(valueAsInt) && (valueAsInt < min || valueAsInt > max)) || !validationRegex.test(`${value}`)) {
      return false;
    }
  }

  return true;
}

function isInvalidSecond(values: FieldValue[]) {
  return !isValidExpression(values, 0, 59);
}
function isInvalidMinute(values: FieldValue[]) {
  return !isValidExpression(values, 0, 59);
}
function isInvalidHour(values: FieldValue[]) {
  return !isValidExpression(values, 0, 23);
}

const DAY_OF_MONTH_W_TOKEN = /^(\d{1,2}|L)W$/;
const DAY_OF_MONTH_OFFSET_TOKEN = /^L-(\d{1,2})$/;

function isInvalidDayOfMonth(values: FieldValue[]) {
  // L, LW, nW (1-31) and L-n (1-30) are valid only in this field; everything else must be a day number
  const days = values.filter((value) => {
    if (value === 'L') return false;
    const weekday = DAY_OF_MONTH_W_TOKEN.exec(`${value}`);
    if (weekday) {
      if (weekday[1] === 'L') return false;
      const target = parseInt(weekday[1], 10);
      return target < 1 || target > 31;
    }
    const offset = DAY_OF_MONTH_OFFSET_TOKEN.exec(`${value}`);
    if (offset) {
      const n = parseInt(offset[1], 10);
      return n < 1 || n > 30;
    }
    return true;
  });
  return !isValidExpression(days, 1, 31);
}

// `W` is only valid on a single day (`15W`) or `L` (`LW`); checked before range expansion hides misuse like `1-15W`
function hasInvalidWModifier(rawDayOfMonth: string) {
  if (!/w/i.test(rawDayOfMonth)) return false;
  return rawDayOfMonth.split(',').some((token) => {
    const value = token.trim();
    return /w/i.test(value) && !/^(\d{1,2}|L)W$/i.test(value);
  });
}

function isInvalidMonth(values: FieldValue[]) {
  return !isValidExpression(values, 1, 12);
}

function isInvalidWeekDay(values: FieldValue[]) {
  // `n#m` (mth n-day of the month) and `nL` (last n-day of the month) are valid only in this field
  const days = values.filter((value) => !isNthWeekdayToken(value) && !/^[0-7]L$/.test(`${value}`));
  return !isValidExpression(days, 0, 7);
}

// The last day each month can ever reach, counting February as 29
const MAX_DAYS_IN_MONTH: Record<number, number> = {
  1: 31,
  2: 29,
  3: 31,
  4: 30,
  5: 31,
  6: 30,
  7: 31,
  8: 31,
  9: 30,
  10: 31,
  11: 30,
  12: 31,
};

// True when no listed day exists in any listed month (e.g. `30 2`), ignoring date-dependent tokens
function isImpossibleDayOfMonth(days: FieldValue[], months: FieldValue[]) {
  if (days.some((day) => typeof day !== 'number')) return false;
  return !months.some((month) => days.some((day) => (day as number) <= MAX_DAYS_IN_MONTH[month as number]));
}

function validateFields(patterns: string[], executablePatterns: FieldValue[][]) {
  if (isInvalidSecond(executablePatterns[0])) throw new Error(`${patterns[0]} is a invalid expression for second`);

  if (isInvalidMinute(executablePatterns[1])) throw new Error(`${patterns[1]} is a invalid expression for minute`);

  if (isInvalidHour(executablePatterns[2])) throw new Error(`${patterns[2]} is a invalid expression for hour`);

  if (isInvalidDayOfMonth(executablePatterns[3]) || hasInvalidWModifier(patterns[3]))
    throw new Error(`${patterns[3]} is a invalid expression for day of month`);

  if (isInvalidMonth(executablePatterns[4])) throw new Error(`${patterns[4]} is a invalid expression for month`);

  if (isInvalidWeekDay(executablePatterns[5])) throw new Error(`${patterns[5]} is a invalid expression for week day`);

  if (isImpossibleDayOfMonth(executablePatterns[3], executablePatterns[4]))
    throw new Error(`${patterns[3]} ${patterns[4]} is an impossible day of month for the given month`);
}

function parseCrontab(pattern: string): CronFields {
  if (typeof pattern !== 'string') throw new TypeError('pattern must be a string!');

  const resolved = resolveNickname(pattern);
  if (!ALLOWED_CHARS_REGEX.test(resolved)) throw new TypeError('pattern includes illegal characters!');

  const raw = removeExtraSpaces(resolved).split(' ');
  if (raw.length !== 5 && raw.length !== 6) throw new Error(`expected 5 or 6 fields but got ${raw.length}`);

  const patterns = raw.length === 5 ? ['0', ...raw] : raw;
  const executablePatterns = convertExpression(resolved);

  validateFields(patterns, executablePatterns);

  return executablePatterns as CronFields;
}

/**
 * Validates a Cron-Job expression pattern.
 *   Throws on error.
 */
export function validateCrontab(pattern: string): void {
  parseCrontab(pattern);
}

/**
 * Validates an IANA timezone string.
 *   Throws on error.
 */
export function validateTimezone(timezone: string): void {
  try {
    Intl.DateTimeFormat(undefined, { timeZone: timezone });
  } catch {
    throw new Error(`Invalid timezone: '${timezone}'`);
  }
}

//////////
/// Day matching
//////////

function weekdayOf(year: number, month: number, day: number): number {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay();
}

function lastDayOfMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

// The nearest weekday (Mon-Fri) to `target` without leaving the month, or -1 if the month has no such day
function nearestWeekday(year: number, month: number, target: number): number {
  const last = lastDayOfMonth(year, month);
  if (target < 1 || target > last) return -1;
  const weekday = weekdayOf(year, month, target);
  if (weekday === 6) return target === 1 ? target + 2 : target - 1;
  if (weekday === 0) return target === last ? target - 2 : target + 1;
  return target;
}

function matchesDayOfMonth(field: FieldValue[], year: number, month: number, day: number): boolean {
  for (const value of field) {
    if (value === day) return true;
    if (typeof value !== 'string') continue;
    if (value === 'L' && day === lastDayOfMonth(year, month)) return true;
    const weekdayMatch = DAY_OF_MONTH_W_TOKEN.exec(value);
    if (weekdayMatch) {
      const target = weekdayMatch[1] === 'L' ? lastDayOfMonth(year, month) : parseInt(weekdayMatch[1], 10);
      if (nearestWeekday(year, month, target) === day) return true;
    }
    const offsetMatch = DAY_OF_MONTH_OFFSET_TOKEN.exec(value);
    if (offsetMatch) {
      const target = lastDayOfMonth(year, month) - parseInt(offsetMatch[1], 10);
      if (target >= 1 && target === day) return true;
    }
  }
  return false;
}

const LAST_WEEKDAY_REGEX = /^([0-7])L$/;
const NTH_WEEKDAY_REGEX = /^([0-7])#([1-5])$/;

function isNthWeekdayToken(value: FieldValue): value is string {
  return typeof value === 'string' && NTH_WEEKDAY_REGEX.test(value);
}

function matchesDayOfWeek(field: FieldValue[], year: number, month: number, day: number): boolean {
  const weekday = weekdayOf(year, month, day);
  for (const value of field) {
    if (value === weekday) return true;
    if (typeof value !== 'string') continue;
    const nth = NTH_WEEKDAY_REGEX.exec(value);
    if (nth) {
      if (parseInt(nth[1], 10) % 7 === weekday && Math.floor((day - 1) / 7) + 1 === parseInt(nth[2], 10)) return true;
      continue;
    }
    const last = LAST_WEEKDAY_REGEX.exec(value);
    if (last && parseInt(last[1], 10) % 7 === weekday && day + 7 > lastDayOfMonth(year, month)) return true;
  }
  return false;
}

//////////
/// Time zones
//////////

interface WallClock {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
}

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;

// Constructing a DateTimeFormat is expensive, so reuse one per timezone
const formatters = new Map<string, Intl.DateTimeFormat>();

function getFormatter(timezone?: string): Intl.DateTimeFormat {
  const key = timezone ?? '';
  let formatter = formatters.get(key);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: 'numeric',
      second: 'numeric',
      hourCycle: 'h23',
      timeZone: timezone,
    });
    formatters.set(key, formatter);
  }
  return formatter;
}

function wallClock(ms: number, timezone?: string): WallClock {
  const parts: Partial<Record<Intl.DateTimeFormatPartTypes, number>> = {};
  for (const part of getFormatter(timezone).formatToParts(ms)) {
    if (part.type !== 'literal') parts[part.type] = parseInt(part.value, 10);
  }
  return {
    year: parts.year!,
    month: parts.month!,
    day: parts.day!,
    hour: parts.hour! % 24,
    minute: parts.minute!,
    second: parts.second!,
  };
}

// The timezone's UTC offset (local minus UTC) in effect at the given instant
function offsetAt(ms: number, timezone?: string): number {
  const w = wallClock(ms, timezone);
  return Date.UTC(w.year, w.month - 1, w.day, w.hour, w.minute, w.second) - Math.floor(ms / 1000) * 1000;
}

//////////
/// Time matcher
//////////

interface DayOffsets {
  dayStart: number;
  before: number;
  after: number;
  // The first instant on the `after` offset
  transition: number;
}

// A century is far beyond any real recurrence, so a schedule not found within it never fires
const MAX_SEARCH_DAYS = 366 * 100;

export class TimeMatcher {
  readonly #timezone?: string;
  readonly #seconds: number[];
  readonly #minutes: number[];
  readonly #hours: number[];
  readonly #daysOfMonth: FieldValue[];
  readonly #months: number[];
  readonly #daysOfWeek: FieldValue[];
  #dayOffsets?: DayOffsets;

  constructor(pattern: string, timezone?: string) {
    const fields = parseCrontab(pattern);
    // Fail fast on an invalid timezone
    getFormatter(timezone);
    this.#timezone = timezone;
    this.#seconds = [...fields[0]].sort((a, b) => a - b);
    this.#minutes = [...fields[1]].sort((a, b) => a - b);
    this.#hours = [...fields[2]].sort((a, b) => a - b);
    this.#daysOfMonth = fields[3];
    this.#months = fields[4];
    this.#daysOfWeek = fields[5];
  }

  match(date: Date | number): boolean {
    const w = wallClock(typeof date === 'number' ? date : date.getTime(), this.#timezone);
    return (
      this.#seconds.includes(w.second) &&
      this.#minutes.includes(w.minute) &&
      this.#hours.includes(w.hour) &&
      this.#runsThisDay(w.year, w.month, w.day)
    );
  }

  #runsThisDay(year: number, month: number, day: number): boolean {
    return (
      this.#months.includes(month) &&
      matchesDayOfMonth(this.#daysOfMonth, year, month, day) &&
      matchesDayOfWeek(this.#daysOfWeek, year, month, day)
    );
  }

  /**
   * Returns the first matching instant strictly after `date`, walking the calendar day by day
   *   instead of scanning every second.  If the schedule can never fire, returns a non-matching
   *   instant at the end of the search horizon.
   */
  nextWakeupTime(date: Date | number): Date {
    const baseMs = typeof date === 'number' ? date : date.getTime();
    let { year, month, day } = wallClock(baseMs, this.#timezone);
    for (let i = 0; i < MAX_SEARCH_DAYS; i++) {
      if (this.#runsThisDay(year, month, day)) {
        const next = this.#firstMatchOnDay(year, month, day, baseMs);
        if (next !== undefined) return new Date(next);
      }
      const nextDay = new Date(Date.UTC(year, month - 1, day + 1));
      year = nextDay.getUTCFullYear();
      month = nextDay.getUTCMonth() + 1;
      day = nextDay.getUTCDate();
    }
    return new Date(baseMs + MAX_SEARCH_DAYS * DAY_MS);
  }

  // The offset before and after the (at most one) UTC offset change affecting this local day
  #offsetsForDay(dayStart: number): DayOffsets {
    if (this.#dayOffsets?.dayStart === dayStart) return this.#dayOffsets;
    // Every instant whose local date is this day lies within this window, as offsets span UTC-12 to UTC+14
    let lo = dayStart - 14 * HOUR_MS;
    let hi = dayStart + DAY_MS + 12 * HOUR_MS;
    const before = offsetAt(lo, this.#timezone);
    const after = offsetAt(hi, this.#timezone);
    if (before !== after) {
      // Binary search for the first second on the new offset
      while (hi - lo > 1000) {
        const mid = lo + Math.floor((hi - lo) / 2000) * 1000;
        if (offsetAt(mid, this.#timezone) === before) lo = mid;
        else hi = mid;
      }
    }
    this.#dayOffsets = { dayStart, before, after, transition: hi };
    return this.#dayOffsets;
  }

  // Earliest instant after baseMs whose local time on this day matches the time-of-day fields
  #firstMatchOnDay(year: number, month: number, day: number, baseMs: number): number | undefined {
    const dayStart = Date.UTC(year, month - 1, day);
    const { before, after, transition } = this.#offsetsForDay(dayStart);
    const minOffset = Math.min(before, after);
    const maxOffset = Math.max(before, after);

    // Local times are ascending, so stop once no later one can map to an instant before `best`
    let best = Infinity;
    for (const hour of this.#hours) {
      const hourStart = dayStart + hour * HOUR_MS;
      if (hourStart - maxOffset >= best) break;
      if (hourStart + HOUR_MS - minOffset <= baseMs) continue;
      for (const minute of this.#minutes) {
        const minuteStart = hourStart + minute * 60_000;
        if (minuteStart - maxOffset >= best) break;
        if (minuteStart + 60_000 - minOffset <= baseMs) continue;
        for (const second of this.#seconds) {
          const local = minuteStart + second * 1000;
          if (local - maxOffset >= best) break;
          // A local time repeated by a fall-back exists under both offsets; one skipped by a spring-forward under neither
          const withBefore = local - before;
          if (withBefore > baseMs && withBefore < best && withBefore < transition) best = withBefore;
          const withAfter = local - after;
          if (withAfter > baseMs && withAfter < best && withAfter >= transition) best = withAfter;
        }
      }
    }
    return best === Infinity ? undefined : best;
  }
}
