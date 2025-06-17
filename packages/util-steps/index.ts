import { DBOS } from '@dbos-inc/dbos-sdk';

/////////
// BCrypt
/////////
import bcryptjs from 'bcryptjs';

async function bcryptGenSaltInternal(saltRounds?: number): Promise<string> {
  if (!saltRounds) saltRounds = 10;
  return await bcryptjs.genSalt(saltRounds);
}

/**
 * DBOS step function that returns bcryptjs salt
 * @param saltRounds?: Number of salt rounds
 */
export const bcryptGenSalt = DBOS.registerStep(bcryptGenSaltInternal, { retriesAllowed: false, name: 'bcryptGenSalt' });

async function bcryptHashInternal(txt: string, saltRounds?: number): Promise<string> {
  if (!saltRounds) saltRounds = 10;
  return await bcryptjs.hash(txt, saltRounds);
}

/**
 * DBOS step function that returns bcryptjs hash
 * @param txt: String to hash
 * @param saltRounds?: Number of salt rounds
 */
export const bcryptHash = DBOS.registerStep(bcryptHashInternal, { retriesAllowed: false, name: 'bcryptHash' });

export async function bcryptCompare(txt: string, hashedTxt: string): Promise<boolean> {
  const isMatch = bcryptjs.compare(txt, hashedTxt);
  return Promise.resolve(isMatch);
}

///////////
// Date
///////////
async function getCurrentDateInternal(): Promise<Date> {
  return Promise.resolve(new Date());
}

/**
 * DBOS step function that returns the current date+time as a `Date`, like `new Date()`
 */
export const getCurrentDate = DBOS.registerStep(getCurrentDateInternal, {
  name: 'getCurrentDate',
  retriesAllowed: false,
});

/**
 * DBOS step function that returns the current date+time as a `number`, like `Date.now()`
 */
async function getCurrentTimeInternal(): Promise<number> {
  return Promise.resolve(Date.now());
}
export const getCurrentTime = DBOS.registerStep(getCurrentTimeInternal, {
  name: 'getCurrentTime',
  retriesAllowed: false,
});

///////////
// Random
///////////
import { randomInt as nRandomInt } from 'node:crypto';
async function randomInternal() {
  return Promise.resolve(Math.random());
}

/**
 * DBOS step function that returns a random number in the range 0-1, as would `Math.random()`
 */
export const random = DBOS.registerStep(randomInternal, { name: 'random', retriesAllowed: false });

async function randomIntInternal(s: number, e: number) {
  return Promise.resolve(nRandomInt(s, e));
}

/**
 * DBOS step function that returns a random number in the range [start,end), as would `node:crypto.randomInt()`
 */
export const randomInt = DBOS.registerStep(randomIntInternal, { name: 'randomInt', retriesAllowed: false });

///////////
// UUID
///////////
import { randomUUID as nRandomUUID } from 'node:crypto';

/**
 * DBOS step function that returns a random UUID, as would `node:crypto.randomUUID`
 */
function randomUUIDInternal() {
  return Promise.resolve(nRandomUUID());
}

export const randomUUID = DBOS.registerStep(randomUUIDInternal, { name: 'randomUUID', retriesAllowed: false });
