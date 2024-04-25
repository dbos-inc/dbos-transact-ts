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

import {validateCrontab as validate, convertExpression as conversion} from '../../src/scheduler/crontab';

//////////////////
// Conversion tests
//////////////////
describe('asterisk-to-range-conversion', () => {
    it('should convert * to ranges', () => {
        const expressions = '* * * * * *';
        const expression = conversion(expressions);
        expect(expression).toBe('0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31 1,2,3,4,5,6,7,8,9,10,11,12 0,1,2,3,4,5,6');
    });

    it('should convert * to ranges', () => {
        const expressions = '* * * * *';
        const expression = conversion(expressions);
        expect(expression).toBe('0 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31 1,2,3,4,5,6,7,8,9,10,11,12 0,1,2,3,4,5,6');
    });
});

describe('names-conversion', () => {
    it('should convert month names', () => {
        const expression = conversion('* * * * January,February *');
        const expressions = expression.split(' ');
        expect(expressions[4]).toBe('1,2');
    });

    it('should convert week day names', () => {
        const expression = conversion('* * * * * Mon,Sun');
        const expressions = expression.split(' ');
        expect(expressions[5]).toBe('1,0');
    });
});

describe('month-names-conversion', () => {
    it('should convert month names', () => {
        const months = conversion('* * * * January,February,March,April,May,June,July,August,September,October,November,December').split(' ')[4];
        expect(months).toBe('1,2,3,4,5,6,7,8,9,10,11,12');
    });

    it('should convert month names', () => {
        const months = conversion('* * * * Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec').split(' ')[4];
        expect(months).toBe('1,2,3,4,5,6,7,8,9,10,11,12');
    });
});

describe('range-conversion', () => {
    it('should convert ranges to numbers', () => {
        const expressions = '0-3 0-3 0-2 1-3 1-2 0-3';
        const expression = conversion(expressions);
        expect(expression).toBe('0,1,2,3 0,1,2,3 0,1,2 1,2,3 1,2 0,1,2,3');
    });

    it('should convert ranges to numbers', () => {
        const expressions = '0-3 0-3 8-10 1-3 1-2 0-3';
        const expression = conversion(expressions);
        expect(expression).toBe('0,1,2,3 0,1,2,3 8,9,10 1,2,3 1,2 0,1,2,3');
    });

    it('should convert comma delimited ranges to numbers', () => {
        const expressions = '0-2,10-23 * * * * *';
        const expression = conversion(expressions).split(' ')[0];
        expect(expression).toBe('0,1,2,10,11,12,13,14,15,16,17,18,19,20,21,22,23');
    });
});
/////////////
// Validation tests
/////////////
