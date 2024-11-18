import {DBOS, StepContext, TransactionContext, WorkflowContext} from '../src';
import {Step, Transaction, Workflow} from '../src';
import { PoolClient } from 'pg';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { TestingRuntime } from '../src';
import { createInternalTestRuntime } from '../src/testing/testing_runtime';

class TestFunctions
{
  @DBOS.transaction()
  static async doTransactionV2(arg: string) {
    await DBOS.pgClient.query("SELECT 1");
    return Promise.resolve(`selected ${arg}`);
  }

  @DBOS.step()
  static async doStepV2(name: string) {
    return Promise.resolve(`step ${name} done`);
  }

  @DBOS.workflow()
  static async doWorkflowV2() {
    return "wv2"
         + await TestFunctions.doTransactionV2("tv2")
         + await TestFunctions.doStepV2("sv2");
  }

  @Transaction()
  static async doTransactionV1(ctx: TransactionContext<PoolClient>, arg: string) {
    await ctx.client.query("SELECT 1");
    return Promise.resolve(`selected ${arg}`);
  }

  @Step()
  static async doStepV1(_ctx: StepContext, arg: string) {
    return Promise.resolve(`step ${arg} done`);
  }

  @Workflow()
  static async doWorkflowV1(ctx: WorkflowContext): Promise<string> {
    return "wv1"
         + await ctx.invoke(TestFunctions).doTransactionV1('tv1')
         + await ctx.invoke(TestFunctions).doStepV1('sv1');
  }

  @DBOS.workflow()
  static async doWorkflowV2_V2V1(): Promise<string> {
    return "wv2"
         + await TestFunctions.doTransactionV2("tv2")
         + await DBOS.invoke(TestFunctions).doStepV1("sv1");
  }

  @DBOS.workflow()
  static async doWorkflowV2_V1V1(): Promise<string> {
    return "wv2"
         + await DBOS.invoke(TestFunctions).doTransactionV1("tv1")
         + await DBOS.invoke(TestFunctions).doStepV1("sv1");
  }

  @DBOS.workflow()
  static async doWorkflowV2_V1V2(): Promise<string> {
    return "wv2"
         + await DBOS.invoke(TestFunctions).doTransactionV1("tv1")
         + await TestFunctions.doStepV2("sv2");
  }

  @Workflow()
  static async doWorkflowV1_V2V1(ctx: WorkflowContext): Promise<string> {
    return "wv1"
         + await TestFunctions.doTransactionV2("tv2")
         + await ctx.invoke(TestFunctions).doStepV1('sv1');
  }

  @Workflow()
  static async doWorkflowV1_V1V2(ctx: WorkflowContext): Promise<string> {
    return "wv1"
         + await ctx.invoke(TestFunctions).doTransactionV1('tv1')
         + await TestFunctions.doStepV2('sv2');
  }

  @Workflow()
  static async doWorkflowV1_V2V2(_ctx: WorkflowContext): Promise<string> {
    return "wv1"
         + await TestFunctions.doTransactionV2("tv2")
         +  await TestFunctions.doStepV2('sv2');
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

describe("dbos-v1v2api-mix-tests-main", () => {
  test("v2start", async () => {
    await main();
  }, 15000);

  test("v1start", async () => {
    let testRuntime: TestingRuntime | undefined = undefined;
    try {
      const config = generateDBOSTestConfig();
      await setUpDBOSTestDb(config);
      testRuntime = await createInternalTestRuntime(undefined, config);

      const res1 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1();
      expect (res1).toBe('wv1selected tv1step sv1 done');

      const res121 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V2V1();
      expect (res121).toBe('wv1selected tv2step sv1 done');

      const res112 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V1V2();
      expect (res112).toBe('wv1selected tv1step sv2 done');

      const res122 = await testRuntime.invokeWorkflow(TestFunctions).doWorkflowV1_V2V2();
      expect (res122).toBe('wv1selected tv2step sv2 done');
    }
    finally {
      await testRuntime?.destroy();
    }
  });
});