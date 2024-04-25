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
    if(expressions.length === 5){
        return ['0'].concat(expressions);
    }
    return expressions;
}

const months = ['january','february','march','april','may','june','july',
        'august','september','october','november','december'];
const shortMonths = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug',
        'sep', 'oct', 'nov', 'dec'];

function convertMonthNameI(expression:string, items:string[]) : string {
    for(let i = 0; i < items.length; i++){
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
        weekExpression = weekExpression.replace(new RegExp(items[i], 'gi'), `${i+1}`);
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
    for(let i = 0; i < expressions.length; i++){
        const match = stepValuePattern.exec(expressions[i]);
        const isStepValue = match !== null && match.length > 0;
        if (isStepValue){
            const baseDivider = match[2];
            if(isNaN(parseInt(baseDivider))){
                throw baseDivider + ' is not a valid step value';
            }
            const values = match[1].split(',');
            const stepValues = [];
            const divider = parseInt(baseDivider, 10);
            for (let j = 0; j <= values.length; j++) {
                const value = parseInt(values[j], 10);
                if (value % divider === 0){
                    stepValues.push(value.toString());
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
export function interpret(crontab: string){
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
