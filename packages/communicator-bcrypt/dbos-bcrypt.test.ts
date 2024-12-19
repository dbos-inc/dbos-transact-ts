import { BcryptStep, BcryptCommunicator } from ".";
import { DBOS } from '@dbos-inc/dbos-sdk';

describe('bcryptWrapper', () => {
  const password = 'securePassword123';
  let hashedPassword: string;

  beforeAll(async () => {
    await DBOS.launch();
  });
  afterAll(async () => {
    await DBOS.shutdown();
  });

  it('should hash a password', async () => {
    hashedPassword = await BcryptStep.bcryptHash(password);
    expect(hashedPassword).toBeDefined();
    expect(typeof hashedPassword).toBe('string');
  });

  it('should verify the correct password', async () => {
    const isValid = await BcryptStep.bcryptCompare(password, hashedPassword);
    expect(isValid).toBe(true);
  });

  it('should not verify an incorrect password', async () => {
    const isValid = await BcryptStep.bcryptCompare('wrongPassword', hashedPassword);
    expect(isValid).toBe(false);
  });

  it('should hash a password', async () => {
    hashedPassword = "";
    hashedPassword = await DBOS.invoke(BcryptCommunicator).bcryptHash(password);
    expect(hashedPassword).toBeDefined();
    expect(typeof hashedPassword).toBe('string');
  });

  it('should verify the correct password', async () => {
    const isValid = await BcryptCommunicator.bcryptCompare(password, hashedPassword);
    expect(isValid).toBe(true);
  });

  it('should not verify an incorrect password', async () => {
    const isValid = await BcryptCommunicator.bcryptCompare('wrongPassword', hashedPassword);
    expect(isValid).toBe(false);
  })
});