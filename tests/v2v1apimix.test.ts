import { ConfiguredInstance, DBOS, StepContext, TransactionContext, WorkflowContext } from '../src';
import { Step, Transaction, Workflow } from '../src';
import { PoolClient } from 'pg';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { TestingRuntime } from '../src'; // Use of TestingRuntime is intentional for this compatibility test
import { createInternalTestRuntime } from '../src/testing/testing_runtime';

class TestFunctions {
  @DBOS.transaction()
  static async doTransactionV2(arg: string) {
    await DBOS.pgClient.query('SELECT 1');
    return Promise.resolve(`selected ${arg}`);
  }

  @DBOS.step()
  static async doStepV2(name: string) {
    return Promise.resolve(`step ${name} done`);
  }

  @DBOS.workflow()
  static async doWorkflowV2() {
    return 'wv2' + (await TestFunctions.doTransactionV2('tv2')) + (await TestFunctions.doStepV2('sv2'));
  }

  @Transaction()
  static async doTransactionV1(ctx: TransactionContext<PoolClient>, arg: string) {
    await ctx.client.query('SELECT 1');
    return Promise.resolve(`selected ${arg}`);
  }

  @Step()
  static async doStepV1(_ctx: StepContext, arg: string) {
    return Promise.resolve(`step ${arg} done`);
  }

  @Workflow()
  static async doWorkflowV1(ctx: WorkflowContext): Promise<string> {
    return (
      'wv1' +
      (await ctx.invoke(TestFunctions).doTransactionV1('tv1')) +
      (await ctx.invoke(TestFunctions).doStepV1('sv1'))
    );
  }

  @DBOS.workflow()
  static async doWorkflowV2_V2V1(): Promise<string> {
    return 'wv2' + (await TestFunctions.doTransactionV2('tv2')) + (await DBOS.invoke(TestFunctions).doStepV1('sv1'));
  }

  @DBOS.workflow()
  static async doWorkflowV2_V1V1(): Promise<string> {
    return (
      'wv2' +
      (await DBOS.invoke(TestFunctions).doTransactionV1('tv1')) +
      (await DBOS.invoke(TestFunctions).doStepV1('sv1'))
    );
  }

  @DBOS.workflow()
  static async doWorkflowV2_V1V2(): Promise<string> {
    return 'wv2' + (await DBOS.invoke(TestFunctions).doTransactionV1('tv1')) + (await TestFunctions.doStepV2('sv2'));
  }

  @Workflow()
  static async doWorkflowV1_V2V1(ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await TestFunctions.doTransactionV2('tv2')) + (await ctx.invoke(TestFunctions).doStepV1('sv1'));
  }

  @Workflow()
  static async doWorkflowV1_V1V2(ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await ctx.invoke(TestFunctions).doTransactionV1('tv1')) + (await TestFunctions.doStepV2('sv2'));
  }

  @Workflow()
  static async doWorkflowV1_V2V2(_ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await TestFunctions.doTransactionV2('tv2')) + (await TestFunctions.doStepV2('sv2'));
  }
}

async function main() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);

  await DBOS.launch();

  const res2 = await TestFunctions.doWorkflowV2();
  expect(res2).toBe('wv2selected tv2step sv2 done');

  const res221 = await TestFunctions.doWorkflowV2_V2V1();
  expect(res221).toBe('wv2selected tv2step sv1 done');

  const res212 = await TestFunctions.doWorkflowV2_V1V2();
  expect(res212).toBe('wv2selected tv1step sv2 done');

  const res211 = await TestFunctions.doWorkflowV2_V1V1();
  expect(res211).toBe('wv2selected tv1step sv1 done');

  await DBOS.shutdown();
}

describe('dbos-v1v2api-mix-tests-main', () => {
  test('v2start', async () => {
    await main();
  }, 15000);

  test('v1start', async () => {
    let testRuntime: TestingRuntime | undefined = undefined;
    try {
      const config = generateDBOSTestConfig();
      await setUpDBOSTestDb(config);
      testRuntime = await createInternalTestRuntime(undefined, config);

      const res1 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1();
      expect(res1).toBe('wv1selected tv1step sv1 done');

      const res121 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V2V1();
      expect(res121).toBe('wv1selected tv2step sv1 done');

      const res112 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V1V2();
      expect(res112).toBe('wv1selected tv1step sv2 done');

      const res122 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V2V2();
      expect(res122).toBe('wv1selected tv2step sv2 done');
    } finally {
      await testRuntime?.destroy();
    }
  });
});

class TestFunctionsInst extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  async initialize() {
    return Promise.resolve();
  }

  @DBOS.transaction()
  async doTransactionV2(arg: string) {
    await DBOS.pgClient.query('SELECT 1');
    return Promise.resolve(`selected ${arg}`);
  }

  @DBOS.step()
  async doStepV2(name: string) {
    return Promise.resolve(`step ${name} done`);
  }

  @DBOS.workflow()
  async doWorkflowV2() {
    return 'wv2' + (await this.doTransactionV2('tv2')) + (await this.doStepV2('sv2'));
  }

  @Transaction()
  async doTransactionV1(ctx: TransactionContext<PoolClient>, arg: string) {
    await ctx.client.query('SELECT 1');
    return Promise.resolve(`selected ${arg}`);
  }

  @Step()
  async doStepV1(_ctx: StepContext, arg: string) {
    return Promise.resolve(`step ${arg} done`);
  }

  @Workflow()
  async doWorkflowV1(ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await ctx.invoke(this).doTransactionV1('tv1')) + (await ctx.invoke(this).doStepV1('sv1'));
  }

  @DBOS.workflow()
  async doWorkflowV2_V2V1(): Promise<string> {
    return 'wv2' + (await this.doTransactionV2('tv2')) + (await DBOS.invoke(this).doStepV1('sv1'));
  }

  @DBOS.workflow()
  async doWorkflowV2_V1V1(): Promise<string> {
    return 'wv2' + (await DBOS.invoke(this).doTransactionV1('tv1')) + (await DBOS.invoke(this).doStepV1('sv1'));
  }

  @DBOS.workflow()
  async doWorkflowV2_V1V2(): Promise<string> {
    return 'wv2' + (await DBOS.invoke(this).doTransactionV1('tv1')) + (await this.doStepV2('sv2'));
  }

  @Workflow()
  async doWorkflowV1_V2V1(ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await this.doTransactionV2('tv2')) + (await ctx.invoke(this).doStepV1('sv1'));
  }

  @Workflow()
  async doWorkflowV1_V1V2(ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await ctx.invoke(this).doTransactionV1('tv1')) + (await this.doStepV2('sv2'));
  }

  @Workflow()
  async doWorkflowV1_V2V2(_ctx: WorkflowContext): Promise<string> {
    return 'wv1' + (await this.doTransactionV2('tv2')) + (await this.doStepV2('sv2'));
  }
}

class ChildWorkflowsV1 {
  @Transaction({ readOnly: true })
  static async childTx(tx: TransactionContext<PoolClient>) {
    await tx.client.query('SELECT 1');
    return Promise.resolve(`selected ${DBOS.workflowID}`);
  }

  @Workflow()
  static async childWF(ctx: WorkflowContext) {
    const tres = await ctx.invoke(ChildWorkflowsV1).childTx();
    return `ChildID:${ctx.workflowUUID}|${tres}`;
  }

  @Workflow()
  static async callSubWF(ctx: WorkflowContext) {
    const cres = await ctx.invokeWorkflow(ChildWorkflowsV1).childWF();
    return `ParentID:${ctx.workflowUUID}|${cres}`;
  }

  @Workflow()
  static async startSubWF(ctx: WorkflowContext) {
    const cwfh = await ctx.startWorkflow(ChildWorkflowsV1).childWF();
    const cres = await cwfh.getResult();
    return `ParentID:${ctx.workflowUUID}|${cres}`;
  }
}

const inst = new TestFunctionsInst('i');

async function mainInst() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);

  await DBOS.launch();

  const res2 = await inst.doWorkflowV2();
  expect(res2).toBe('wv2selected tv2step sv2 done');

  const res221 = await inst.doWorkflowV2_V2V1();
  expect(res221).toBe('wv2selected tv2step sv1 done');

  const res212 = await inst.doWorkflowV2_V1V2();
  expect(res212).toBe('wv2selected tv1step sv2 done');

  const res211 = await inst.doWorkflowV2_V1V1();
  expect(res211).toBe('wv2selected tv1step sv1 done');

  const resX = await DBOS.invoke(ChildWorkflowsV1).childTx();
  expect(resX.startsWith('selected')).toBeTruthy();

  const resS = await DBOS.invoke(TestFunctions).doStepV1('bare');
  expect(resS).toBe('step bare done');

  await DBOS.shutdown();
}

describe('dbos-v1v2api-mix-tests-main-inst', () => {
  test('v2start', async () => {
    await mainInst();
  }, 15000);

  test('v1start', async () => {
    let testRuntime: TestingRuntime | undefined = undefined;
    try {
      const config = generateDBOSTestConfig();
      await setUpDBOSTestDb(config);
      testRuntime = await createInternalTestRuntime(undefined, config);

      const res1 = await testRuntime.invokeWorkflow(inst).doWorkflowV1();
      expect(res1).toBe('wv1selected tv1step sv1 done');

      const res121 = await testRuntime.invokeWorkflow(inst).doWorkflowV1_V2V1();
      expect(res121).toBe('wv1selected tv2step sv1 done');

      const res112 = await testRuntime.invokeWorkflow(inst).doWorkflowV1_V1V2();
      expect(res112).toBe('wv1selected tv1step sv2 done');

      const res122 = await testRuntime.invokeWorkflow(inst).doWorkflowV1_V2V2();
      expect(res122).toBe('wv1selected tv2step sv2 done');

      const rescwfv1 = await testRuntime.invokeWorkflow(ChildWorkflowsV1, 'child-direct').callSubWF();
      expect(rescwfv1).toBe('ParentID:child-direct|ChildID:child-direct-0|selected child-direct-0');

      const rescwfv1h = await testRuntime.startWorkflow(ChildWorkflowsV1, 'child-start').startSubWF();
      expect(await rescwfv1h.getResult()).toBe('ParentID:child-start|ChildID:child-start-0|selected child-start-0');
    } finally {
      await testRuntime?.destroy();
    }
  });
});
