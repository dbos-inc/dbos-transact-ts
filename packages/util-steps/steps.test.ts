import * as dbosutil from '.';
import { DBOS } from '@dbos-inc/dbos-sdk';

describe('bcryptWrapper', () => {
  const password = 'securePassword123';
  let hashedPassword: string;

  beforeAll(async () => {
    DBOS.setConfig({
      name: 'steptest',
    });
    await DBOS.launch();
  });
  afterAll(async () => {
    await DBOS.shutdown();
  });

  it('should hash a password', async () => {
    const _salt = await dbosutil.bcryptGenSalt();
    hashedPassword = await dbosutil.bcryptHash(password);
    expect(hashedPassword).toBeDefined();
    expect(typeof hashedPassword).toBe('string');
  });

  it('should verify the correct password', async () => {
    const isValid = await dbosutil.bcryptCompare(password, hashedPassword);
    expect(isValid).toBe(true);
  });

  it('should not verify an incorrect password', async () => {
    const isValid = await dbosutil.bcryptCompare('wrongPassword', hashedPassword);
    expect(isValid).toBe(false);
  });

  it('should generate uuid', async () => {
    const uuid = await dbosutil.randomUUID();
    expect(uuid[0]).not.toBe('y');
  });

  it('Should get dates', async () => {
    const st = Date.now();
    const t1 = await dbosutil.getCurrentTime();
    const t2 = (await dbosutil.getCurrentDate()).getTime();
    const et = Date.now();

    expect(t1).toBeGreaterThanOrEqual(st);
    expect(t2).toBeGreaterThanOrEqual(st);
    expect(t1).toBeLessThanOrEqual(et);
    expect(t2).toBeLessThanOrEqual(et);
  });

  it('Should generate random numbers', async () => {
    const r1 = await dbosutil.random();
    const r2 = await dbosutil.randomInt(1, 11);

    expect(r1).toBeGreaterThanOrEqual(0);
    expect(r2).toBeGreaterThanOrEqual(1);
    expect(r1).toBeLessThanOrEqual(1);
    expect(r2).toBeLessThanOrEqual(10);
  });
});
