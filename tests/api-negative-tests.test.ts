import { DBOS } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { KnexDataSource } from '../packages/knex-datasource';

DBOS.logger.info('This should not cause a kaboom.');

const config = generateDBOSTestConfig();
const dbConfig = { client: 'pg', connection: { user: 'postgres', database: 'dbostest_dbos_sys' } };
const knexds = new KnexDataSource('app-db', dbConfig);

class TransitionTests {
  @DBOS.workflow()
  static async calledWorkflow() {
    return Promise.resolve('ohno');
  }

  @knexds.transaction()
  static async leafTransaction() {
    return Promise.resolve('ok');
  }

  @knexds.transaction()
  static async oopsCallTransaction() {
    return await TransitionTests.leafTransaction();
  }

  @DBOS.workflow()
  static async oopsCallTransactionWF() {
    return await TransitionTests.oopsCallTransaction();
  }

  @knexds.transaction()
  static async oopsCallSleep() {
    await DBOS.sleepms(100);
  }

  @DBOS.workflow()
  static async oopsCallSleepWF() {
    return await TransitionTests.oopsCallSleep();
  }

  @DBOS.step({ retriesAllowed: false })
  static async sleepStep() {
    await DBOS.sleepms(100);
  }

  @knexds.transaction()
  static async oopsCallStep() {
    await TransitionTests.sleepStep();
  }

  @DBOS.workflow()
  static async oopsCallStepWF() {
    return await TransitionTests.oopsCallStep();
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallTransactionFromStep() {
    return TransitionTests.leafTransaction();
  }

  @DBOS.step({ retriesAllowed: false })
  static async callStepFromStep() {
    return TransitionTests.sleepStep();
  }

  @knexds.transaction()
  static async oopsCallSendFromTx() {
    await DBOS.send('aaa', 'a', 'aa');
  }

  @DBOS.workflow()
  static async oopsCallSendFromTxWF() {
    return await TransitionTests.oopsCallSendFromTx();
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallSendFromStep() {
    await DBOS.send('aaa', 'a', 'aa');
  }

  @knexds.transaction()
  static async oopsCallGetFromTx() {
    await DBOS.getEvent('aaa', 'a');
  }

  @DBOS.workflow()
  static async oopsCallGetFromTxWF() {
    await TransitionTests.oopsCallGetFromTx();
  }

  @DBOS.step({ retriesAllowed: false })
  static async oopsCallGetFromStep() {
    await DBOS.getEvent('aaa', 'a');
  }

  @knexds.transaction()
  static async oopsCallWFFromTransaction() {
    return await TransitionTests.calledWorkflow();
  }

  @DBOS.workflow()
  static async oopsCallWFFromTransactionWF() {
    return await TransitionTests.oopsCallWFFromTransaction();
  }

  @DBOS.step()
  static async oopsCallWFFromStep() {
    return await TransitionTests.calledWorkflow();
  }

  @knexds.transaction()
  static async oopsCallStartWFFromTransaction() {
    return await DBOS.startWorkflow(TransitionTests).calledWorkflow();
  }

  @DBOS.workflow()
  static async oopsCallStartWFFromTransactionWF() {
    return await TransitionTests.oopsCallStartWFFromTransaction();
  }

  @DBOS.step()
  static async oopsCallStartWFFromStep() {
    return await DBOS.startWorkflow(TransitionTests).calledWorkflow();
  }
}

async function main9() {
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  try {
    await TransitionTests.leafTransaction();
    await expect(() => TransitionTests.oopsCallTransactionWF()).rejects.toThrow(
      'Invalid call to a `transaction` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallSleepWF()).rejects.toThrow(
      'Invalid call to `DBOS.sleep` inside a `transaction`',
    );
    await TransitionTests.sleepStep();
    await expect(() => TransitionTests.oopsCallStepWF()).rejects.toThrow(
      'Invalid call to a `step` function from within a `transaction`',
    );
    await TransitionTests.callStepFromStep();
    await expect(() => TransitionTests.oopsCallTransactionFromStep()).rejects.toThrow(
      'Invalid call to a `transaction` function from within a `step`',
    );

    await expect(() => TransitionTests.oopsCallSendFromTxWF()).rejects.toThrow(
      'Invalid call to `DBOS.send` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallSendFromStep()).rejects.toThrow(
      'Invalid call to `DBOS.send` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallGetFromTxWF()).rejects.toThrow(
      'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallGetFromStep()).rejects.toThrow(
      'Invalid call to `DBOS.getEvent` inside a `step` or `transaction`',
    );

    await expect(() => TransitionTests.oopsCallWFFromTransactionWF()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallWFFromStep()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallStartWFFromTransactionWF()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
    await expect(() => TransitionTests.oopsCallStartWFFromStep()).rejects.toThrow(
      'Invalid call to a `workflow` function from within a `step` or `transaction`',
    );
  } finally {
    await DBOS.shutdown();
  }
}

async function main11() {
  expect(() => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    require('./baddecorator'); // Load it dynamically inside expect
  }).toThrow('Operation (Name: BadDecoratorClass.cantBeBoth) is already registered with a conflicting function type');
  return Promise.resolve();
}

describe('dbos-v2api-tests-main', () => {
  test('transitions', async () => {
    await main9();
  }, 15000);

  test('double decorator error', async () => {
    await main11();
  }, 15000);
});
