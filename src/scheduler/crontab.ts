// This code was based on code from node-cron:
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

function removeExtraSpaces(str: string): string {
    return str.replace(/\s{2,}/g, ' ').trim();
}

function prependSecondExpression(expressions: string[]) : string[]
{
    if (expressions.length === 5) {
        return ['0'].concat(expressions);
    }
    return expressions;
}

const months = ['january','february','march','april','may','june','july',
        'august','september','october','november','december'];
const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug',
        'sep', 'oct', 'nov', 'dec'];

function convertMonthNameI(expression:string, items:string[]) : string {
    for (let i = 0; i < items.length; i++){
        expression = expression.replace(new RegExp(items[i], 'gi'), `${i + 1}`);
    }
    return expression;
}

function monthNamesConversion(monthExpression: string): string {
    monthExpression = convertMonthNameI(monthExpression, months);
    monthExpression = convertMonthNameI(monthExpression, shortMonths);
    return monthExpression;
}

const weekDays = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday',
'friday', 'saturday'];
const shortWeekDays = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];

function convertWeekDayName(weekExpression: string, items: string[]){
    for(let i = 0; i < items.length; i++){
        weekExpression = weekExpression.replace(new RegExp(items[i], 'gi'), `${i}`);
    }
    return weekExpression;
}

function weekDayNamesConversion(expression: string){
    expression = expression.replace('7', '0');
    expression = convertWeekDayName(expression, weekDays);
    return convertWeekDayName(expression, shortWeekDays);
}

function convertAsterisk(expression: string, replacement: string){
    if(expression.indexOf('*') !== -1){
        return expression.replace('*', replacement);
    }
    return expression;
}

// Based on position in full crontab, convert asterisk to appropriate range
export function convertAsterisksToRanges(expressions: string[]) {
    expressions[0] = convertAsterisk(expressions[0], '0-59');
    expressions[1] = convertAsterisk(expressions[1], '0-59');
    expressions[2] = convertAsterisk(expressions[2], '0-23');
    expressions[3] = convertAsterisk(expressions[3], '1-31');
    expressions[4] = convertAsterisk(expressions[4], '1-12');
    expressions[5] = convertAsterisk(expressions[5], '0-6');
    return expressions;
}

function replaceWithRange(expression: string, text: string, init: string, end:string) {
    const numbers: string[] = [];
    let last = parseInt(end);
    let first = parseInt(init);

    if(first > last){
        last = parseInt(init);
        first = parseInt(end);
    }

    for(let i = first; i <= last; i++) {
        numbers.push(i.toString());
    }

    return expression.replace(new RegExp(text, 'i'), numbers.join());
}

function convertRange(expression: string){
    const rangeRegEx = /(\d+)-(\d+)/;
    let match = rangeRegEx.exec(expression);
    while(match !== null && match.length > 0){
        expression = replaceWithRange(expression, match[0], match[1], match[2]);
        match = rangeRegEx.exec(expression);
    }
    return expression;
}

function convertAllRanges(expressions: string[]){
    for(let i = 0; i < expressions.length; i++){
        expressions[i] = convertRange(expressions[i]);
    }
    return expressions;
}

function convertSteps(expressions: string[]){
    const stepValuePattern = /^(.+)\/(\w+)$/;
    for (let i = 0; i < expressions.length; i++){
        const match = stepValuePattern.exec(expressions[i]);
        const isStepValue = match !== null && match.length > 0;
        if (isStepValue){
            const baseDivider = match[2];
            if(isNaN(parseInt(baseDivider))){
                throw new Error(baseDivider + ' is not a valid step value');
            }
            const values = match[1].split(',');
            const stepValues: string[] = [];
            const divider = parseInt(baseDivider, 10);
            for (let j = 0; j <= values.length; j++) {
                const value = parseInt(values[j], 10);
                if (value % divider === 0){
                    stepValues.push(`${value}`);
                }
            }
            expressions[i] = stepValues.join(',');
        }
    }
    return expressions;
}
// Function that takes care of normalization.
function normalizeIntegers(expressions: string[]) {
    for (let i=0; i < expressions.length; i++){
        const numbers = expressions[i].split(',');
        for (let j=0; j<numbers.length; j++){
            numbers[j] = parseInt(numbers[j]).toString();
        }
        expressions[i] = numbers.join(',');
    }
    return expressions;
}

const validationRegex = /^(?:\d+|\*|\*\/\d+)$/;

// Check comma-delimited list to see if elements are in range
function isValidExpression(expression:string, min:number, max:number): boolean {
    const options = expression.split(',');

    for (const option of options) {
        const optionAsInt = parseInt(option, 10);

        if ((!Number.isNaN(optionAsInt) &&
                (optionAsInt < min || optionAsInt > max)) ||
            !validationRegex.test(option)
        ) {
            return false;
        }
    }

    return true;
}

function isInvalidSecond(expression: string) { return !isValidExpression(expression, 0, 59); }
function isInvalidMinute(expression: string) { return !isValidExpression(expression, 0, 59); }
function isInvalidHour(expression: string) { return !isValidExpression(expression, 0, 23); }
function isInvalidDayOfMonth(expression: string) { return !isValidExpression(expression, 1, 31); }
function isInvalidMonth(expression: string) { return !isValidExpression(expression, 1, 12); }
function isInvalidWeekDay(expression: string) { return !isValidExpression(expression, 0, 7); }

function validateFields(patterns: string[], executablePatterns :string[]) {
    if (isInvalidSecond(executablePatterns[0]))
        throw new Error(`${patterns[0]} is a invalid expression for second`);

    if (isInvalidMinute(executablePatterns[1]))
        throw new Error(`${patterns[1]} is a invalid expression for minute`);

    if (isInvalidHour(executablePatterns[2]))
        throw new Error(`${patterns[2]} is a invalid expression for hour`);

    if (isInvalidDayOfMonth(executablePatterns[3]))
        throw new Error(
            `${patterns[3]} is a invalid expression for day of month`
        );

    if (isInvalidMonth(executablePatterns[4]))
        throw new Error(`${patterns[4]} is a invalid expression for month`);

    if (isInvalidWeekDay(executablePatterns[5]))
        throw new Error(`${patterns[5]} is a invalid expression for week day`);
}

/**
 * Validates a Cron-Job expression pattern.
 *   Throws on error.
 */
export function validateCrontab(pattern: string) {
    if (typeof pattern !== 'string')
        throw new TypeError('pattern must be a string!');

    const patterns = pattern.split(' ');
    const executablePatterns = convertExpression(pattern).split(' ');

    if (patterns.length === 5) patterns.unshift('0');

    validateFields(patterns, executablePatterns);

    return executablePatterns.join(' ');
}

/*
* The node-cron core allows only numbers (including multiple numbers e.g 1,2).
* This module is going to translate the month names, week day names and ranges
* to integers relatives.
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
export function convertExpression(crontab: string){
    let expressions = removeExtraSpaces(crontab).split(' ');
    expressions = prependSecondExpression(expressions);
    expressions[4] = monthNamesConversion(expressions[4]);
    expressions[5] = weekDayNamesConversion(expressions[5]);
    expressions = convertAsterisksToRanges(expressions);
    expressions = convertAllRanges(expressions);
    expressions = convertSteps(expressions);

    expressions = normalizeIntegers(expressions);

    return expressions.join(' ');
}

//////////
/// Time matcher
//////////

function matchPattern(pattern: string, nvalue: number){
    const value = `${nvalue}`;
    if (pattern.indexOf(',') !== -1) {
        const patterns = pattern.split(',');
        return patterns.includes(value);
    }
    return pattern === value;
}

export class TimeMatcher {
    pattern: string;
    expressions: string[];
    dtf: Intl.DateTimeFormat | null;
    timezone?: string;

    constructor(pattern: string, timezone?: string) {
        validateCrontab(pattern);
        this.pattern = convertExpression(pattern);
        this.timezone = timezone;
        this.expressions = this.pattern.split(' ');
        this.dtf = this.timezone
            ? new Intl.DateTimeFormat('en-US', {
                year: 'numeric',
                month: '2-digit',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit',
                hourCycle: 'h23',
                fractionalSecondDigits: 3,
                timeZone: this.timezone
            }) : null;
    }

    match(date: Date){
        date = this.apply(date);

        const runOnSecond = matchPattern(this.expressions[0], date.getSeconds());
        const runOnMinute = matchPattern(this.expressions[1], date.getMinutes());
        const runOnHour = matchPattern(this.expressions[2], date.getHours());
        const runOnDay = this.runsThisDay(date);

        return runOnSecond && runOnMinute && runOnHour && runOnDay;
    }

    private runsThisDay(date: Date) {
        const runOnDay = matchPattern(this.expressions[3], date.getDate());
        const runOnMonth = matchPattern(this.expressions[4], date.getMonth() + 1);
        const runOnWeekDay = matchPattern(this.expressions[5], date.getDay());

        return runOnDay && runOnMonth && runOnWeekDay;
    }

    nextWakeupTime(date: Date) {
        // This is conservative.  Some schedules never occur, such as the 30th of February, but you can ask for them
        let msec = Math.round(date.getTime());
        // This can be optimized by skipping ahead, but unit test first
        for (let maxIters = 3600; --maxIters; maxIters > 0) {
            msec += 1000;
            const nd = new Date(msec);
            if (this.match(nd)) return nd;
        }
        return new Date(msec);
    }

    apply(date: Date){
        if(this.dtf){
            return new Date(this.dtf.format(date));
        }

        return date;
    }
}
