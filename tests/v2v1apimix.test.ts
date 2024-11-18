import {DBOS, Step, StepContext, Transaction, TransactionContext, Workflow, WorkflowContext} from '../src';
import { PoolClient } from 'pg';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';

export class TestFunctions
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
  static async doWorkflowV1(ctx: WorkflowContext) {
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
  await DBOS.shutdown();
}

describe("dbos-v1v2api-mix-tests-main", () => {
  test("v2start", async () => {
    await main();
  }, 15000);
});
