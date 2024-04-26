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

import {validateCrontab as validate, convertExpression as conversion, TimeMatcher} from '../../src/scheduler/crontab';

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

describe('step-values-conversion', () => {
    it('should convert step values', () => {
        const expression = '1,2,3,4,5,6,7,8,9,10/2 0,1,2,3,4,5,6,7,8,9/5 */3 * * *';
        const expressions = conversion(expression).split(' ');
        expect(expressions[0]).toBe('2,4,6,8,10');
        expect(expressions[1]).toBe('0,5');
        expect(expressions[2]).toBe('0,3,6,9,12,15,18,21');
    });

    it('should throw an error if step value is not a number', () => {
        const expressions = '1,2,3,4,5,6,7,8,9,10/someString 0,1,2,3,4,5,6,7,8,9/5 * * * *';
        expect(() => conversion(expressions)).toThrow('someString is not a valid step value');
    });
});

describe('week-day-names-conversion', () => {
    it('should convert week day names names', () => {
        const weekDays = conversion('* * * * Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday').split(' ')[5];
        expect(weekDays).toBe('1,2,3,4,5,6,0');
    });

    it('should convert short week day names names', () => {
        const weekDays = conversion('* * * * Mon,Tue,Wed,Thu,Fri,Sat,Sun').split(' ')[5];
        expect(weekDays).toBe('1,2,3,4,5,6,0');
    });

    it('should convert 7 to 0', () => {
        const weekDays = conversion('* * * * 7').split(' ')[5];
        expect(weekDays).toBe('0');
    });
});

/////////////
// Validation tests
/////////////

describe('pattern-validation', () => {
    describe('validate day of month', () => {
        it('should fail with invalid day of month', () => {
            expect(() => {
                validate('* * 32 * *');
            }).toThrow('32 is a invalid expression for day of month');
        });

        it('should not fail with valid day of month', () => {
            expect(() => {
                validate('0 * * 15 * *');
            }).not.toThrow();
        });

        it('should not fail with * for day of month', () => {
            expect(() => {
                validate('* * * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2 for day of month', () => {
            expect(() => {
                validate('* * */2 * *');
            }).not.toThrow();
        });
    });
});

describe('pattern-validation', () => {
    describe('validate hour', () => {
        it('should fail with invalid hour', () => {
            expect(() => {
                validate('* 25 * * *');
            }).toThrow('25 is a invalid expression for hour');
        });

        it('should not fail with valid hour', () => {
            expect(() => {
                validate('* 12 * * *');
            }).not.toThrow();
        });

        it('should not fail with * for hour', () => {
            expect(() => {
                validate('* * * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2 for hour', () => {
            expect(() => {
                validate('* */2 * * *');
            }).not.toThrow();
        });

        it('should accept range for hours', () => {
            expect(() => {
                validate('* 3-20 * * *');
            }).not.toThrow();
        });
    });
});

describe('pattern-validation', () => {
    describe('validate minutes', () => {
        it('should fail with invalid minute', () => {
            expect(() => {
                validate('63 * * * *');
            }).toThrow('63 is a invalid expression for minute');
        });

        it('should not fail with valid minute', () => {
            expect(() => {
                validate('30 * * * *');
            }).not.toThrow();
        });

        it('should not fail with *', () => {
            expect(() => {
                validate('* * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2', () => {
            expect(() => {
                validate('*/2 * * * *');
            }).not.toThrow();
        });
    });
});

describe('pattern-validation',  () => {
    describe('validate month',  () => {
        it('should fail with invalid month',  () => {
            expect( () => {
                validate('* * * 13 *');
            }).toThrow('13 is a invalid expression for month');
        });

        it('should fail with invalid month name',  () => {
            expect( () => {
                validate('* * * foo *');
            }).toThrow('foo is a invalid expression for month');
        });

        it('should not fail with valid month',  () => {
            expect( () => {
                validate('* * * 10 *');
            }).not.toThrow();
        });

        it('should not fail with valid month name',  () => {
            expect( () => {
                validate('* * * September *');
            }).not.toThrow();
        });

        it('should not fail with * for month',  () => {
            expect( () => {
                validate('* * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2 for month',  () => {
            expect( () => {
                validate('* * * */2 *');
            }).not.toThrow();
        });
    });
});

describe('pattern-validation', () => {
    describe('validate seconds', () => {
        it('should fail with invalid second', () => {
            expect(() => {
                validate('63 * * * * *');
            }).toThrow('63 is a invalid expression for second');
        });

        it('should not fail with valid second', () => {
            expect(() => {
                validate('30 * * * * *');
            }).not.toThrow();
        });

        it('should not fail with * for second', () => {
            expect(() => {
                validate('* * * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2 for second', () => {
            expect(() => {
                validate('*/2 * * * * *');
            }).not.toThrow();
        });
    });
});

describe('pattern-validation', () => {
    it('should succeed with a valid expression', () =>  {
        expect(() => {
            validate('59 * * * *');
        }).not.toThrow();
    });

    it('should fail with an invalid expression', () =>  {
        expect(() => {
            validate('60 * * * *');
        }).toThrow('60 is a invalid expression for minute');
    });

    it('should fail without a string', () =>  {
        expect(() => {
            validate(50 as unknown as string);
        }).toThrow('pattern must be a string!');
    });
});

describe('pattern-validation', () => {
    describe('validate week day', () => {
        it('should fail with invalid week day', () => {
            expect(() => {
                validate('* * * * 9');
            }).toThrow('9 is a invalid expression for week day');
        });

        it('should fail with invalid week day name', () => {
            expect(() => {
                validate('* * * * foo');
            }).toThrow('foo is a invalid expression for week day');
        });

        it('should not fail with valid week day', () => {
            expect(() => {
                validate('* * * * 5');
            }).not.toThrow();
        });

        it('should not fail with valid week day name', () => {
            expect(() => {
                validate('* * * * Friday');
            }).not.toThrow();
        });

        it('should not fail with * for week day', () => {
            expect(() => {
                validate('* * * * *');
            }).not.toThrow();
        });

        it('should not fail with */2 for week day', () => {
            expect(() => {
                validate('* * * */2 *');
            }).not.toThrow();
        });

        it('should not fail with Monday-Sunday for week day', () => {
            expect(() => {
                validate('* * * * Monday-Sunday');
            }).not.toThrow();
        });

        it('should not fail with 1-7 for week day', () => {
            expect(() => {
                validate('0 0 1 1 1-7');
            }).not.toThrow();
        });
    });
});

/////////
// Time matcher
/////////

describe('TimeMatcher', () => {
    describe('wildcard', () => {
        it('should accept wildcard for second', () => {
            const matcher = new TimeMatcher('* * * * * *');
            expect(matcher.match(new Date())).toBe(true);
        });

        it('should accept wildcard for minute', () => {
            const matcher = new TimeMatcher('0 * * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 10, 20, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 10, 20, 1))).toBe(false);
        });

        it('should accept wildcard for hour', () => {
            const matcher = new TimeMatcher('0 0 * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 10, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 10, 1, 0))).toBe(false);
        });

        it('should accept wildcard for day', () => {
            const matcher = new TimeMatcher('0 0 0 * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 1, 0, 0))).toBe(false);
        });

        it('should accept wildcard for month', () => {
            const matcher = new TimeMatcher('0 0 0 1 * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 2, 0, 0, 0))).toBe(false);
        });

        it('should accept wildcard for week day', () => {
            const matcher = new TimeMatcher('0 0 0 1 4 *');
            expect(matcher.match(new Date(2018, 3, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 3, 2, 0, 0, 0))).toBe(false);
        });
    });

    describe('single value', () => {
        it('should accept single value for second', () => {
            const matcher = new TimeMatcher('5 * * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 5))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 6))).toBe(false);
        });

        it('should accept single value for minute', () => {
            const matcher = new TimeMatcher('0 5 * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 5, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 6, 0))).toBe(false);
        });

        it('should accept single value for hour', () => {
            const matcher = new TimeMatcher('0 0 5 * * *');
            expect(matcher.match(new Date(2018, 0, 1, 5, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 6, 0, 0))).toBe(false);
        });

        it('should accept single value for day', () => {
            const matcher = new TimeMatcher('0 0 0 5 * *');
            expect(matcher.match(new Date(2018, 0, 5, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 6, 0, 0, 0))).toBe(false);
        });

        it('should accept single value for month', () => {
            const matcher = new TimeMatcher('0 0 0 1 5 *');
            expect(matcher.match(new Date(2018, 4, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 5, 1, 0, 0, 0))).toBe(false);
        });

        it('should accept single value for week day', () => {
            const matcher = new TimeMatcher('0 0 0 * * monday');
            expect(matcher.match(new Date(2018, 4, 7, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 8, 0, 0, 0))).toBe(false);
        });
    });

    describe('multiple values', () => {
        it('should accept multiple values for second', () => {
            const matcher = new TimeMatcher('5,6 * * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 5))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 6))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 7))).toBe(false);
        });

        it('should accept multiple values for minute', () => {
            const matcher = new TimeMatcher('0 5,6 * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 5, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 6, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 7, 0))).toBe(false);
        });
        
        it('should accept multiple values for hour', () => {
            const matcher = new TimeMatcher('0 0 5,6 * * *');
            expect(matcher.match(new Date(2018, 0, 1, 5, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 6, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 7, 0, 0))).toBe(false);
        });

        it('should accept multiple values for day', () => {
            const matcher = new TimeMatcher('0 0 0 5,6 * *');
            expect(matcher.match(new Date(2018, 0, 5, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 6, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 7, 0, 0, 0))).toBe(false);
        });

        it('should accept multiple values for month', () => {
            const matcher = new TimeMatcher('0 0 0 1 may,june *');
            expect(matcher.match(new Date(2018, 4, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 5, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 6, 1, 0, 0, 0))).toBe(false);
        });

        it('should accept multiple values for week day', () => {
            const matcher = new TimeMatcher('0 0 0 * * monday,tue');
            expect(matcher.match(new Date(2018, 4, 7, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 8, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 9, 0, 0, 0))).toBe(false);
        });
    });

    describe('range', () => {
        it('should accept range for second', () => {
            const matcher = new TimeMatcher('5-7 * * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 5))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 6))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 7))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 8))).toBe(false);
        });

        it('should accept range for minute', () => {
            const matcher = new TimeMatcher('0 5-7 * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 5, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 6, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 7, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 8, 0))).toBe(false);
        });

        it('should accept range for hour', () => {
            const matcher = new TimeMatcher('0 0 5-7 * * *');
            expect(matcher.match(new Date(2018, 0, 1, 5, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 6, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 7, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 8, 0, 0))).toBe(false);
        });

        it('should accept range for day', () => {
            const matcher = new TimeMatcher('0 0 0 5-7 * *');
            expect(matcher.match(new Date(2018, 0, 5, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 6, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 7, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 8, 0, 0, 0))).toBe(false);
        });

        it('should accept range for month', () => {
            const matcher = new TimeMatcher('0 0 0 1 may-july *');
            expect(matcher.match(new Date(2018, 4, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 5, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 6, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 7, 1, 0, 0, 0))).toBe(false);
        });

        it('should accept range for week day', () => {
            const matcher = new TimeMatcher('0 0 0 * * monday-wed');
            expect(matcher.match(new Date(2018, 4, 7, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 8, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 9, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 10, 0, 0, 0))).toBe(false);
        });
    });

    describe('step values', () => {
        it('should accept step values for second', () => {
            const matcher = new TimeMatcher('*/2 * * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 2))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 6))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 0, 7))).toBe(false);
        });

        it('should accept step values for minute', () => {
            const matcher = new TimeMatcher('0 */2 * * * *');
            expect(matcher.match(new Date(2018, 0, 1, 0, 2, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 6, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 0, 7, 0))).toBe(false);
        });
        
        it('should accept step values for hour', () => {
            const matcher = new TimeMatcher('0 0 */2 * * *');
            expect(matcher.match(new Date(2018, 0, 1, 2, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 6, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 1, 7, 0, 0))).toBe(false);
        });

        it('should accept step values for day', () => {
            const matcher = new TimeMatcher('0 0 0 */2 * *');
            expect(matcher.match(new Date(2018, 0, 2, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 6, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 0, 7, 0, 0, 0))).toBe(false);
        });

        it('should accept step values for month', () => {
            const matcher = new TimeMatcher('0 0 0 1 */2 *');
            expect(matcher.match(new Date(2018, 1, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 5, 1, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 6, 1, 0, 0, 0))).toBe(false);
        });

        it('should accept step values for week day', () => {
            const matcher = new TimeMatcher('0 0 0 * * */2');
            expect(matcher.match(new Date(2018, 4, 6, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 8, 0, 0, 0))).toBe(true);
            expect(matcher.match(new Date(2018, 4, 9, 0, 0, 0))).toBe(false);
        });
    });

    describe('timezone', ()=>{
        it('should match with timezone America/Sao_Paulo', () => {
            const matcher = new TimeMatcher('0 0 0 * * *', 'America/Sao_Paulo');
            const utcTime = new Date('Thu Oct 11 2018 03:00:00Z');
            expect(matcher.match(utcTime)).toBe(true); 
        });

        it('should match with timezone Europe/Rome', () => {
            const matcher = new TimeMatcher('0 0 0 * * *', 'Europe/Rome');
            const utcTime = new Date('Thu Oct 11 2018 22:00:00Z');
            expect(matcher.match(utcTime)).toBe(true);
        });

        /*
        it('should match with all available timezone of moment-timezone', () => {
            const allTimeZone = moment.tz.names();
            for (const zone in allTimeZone) {
                const tmp = moment();
                const expected = moment.tz(tmp,allTimeZone[zone]);
                const pattern = expected.second() + ' ' + expected.minute() + ' ' + expected.hour() + ' ' + expected.date() + ' ' + (expected.month()+1) + ' ' + expected.day();
                const matcher = new TimeMatcher(pattern, allTimeZone[zone]);
                const utcTime = new Date(tmp.year(), tmp.month(), tmp.date(), tmp.hour(), tmp.minute(), tmp.second(), tmp.millisecond());
                expect(matcher.match(utcTime));
            }
        });
        */
    });
});