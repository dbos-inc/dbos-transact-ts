import { ArgSource, ArgSources, DBOS, DBOSResponseError } from '../src';
import { WorkflowUUIDHeader } from '../src/httpServer/server';
import { generateDBOSTestConfig, setUpDBOSTestDb, TestKvTable } from './helpers';

const testTableName = "dbos_test_kv";

class TestEndpoints {
  @DBOS.transaction()
  static async getKey() {
    return Promise.resolve();
  }

  @DBOS.getApi("/hello")
  static async hello() {
    return Promise.resolve({ message: "hello!" });
  }

  @DBOS.getApi("/hello/:id")
  static async helloUrl(id: string) {
    // Customize status code and response.
    DBOS.koaContext.body = `wow ${id}`;
    DBOS.koaContext.status = 301;
    return Promise.resolve(`hello ${id}`);
  }

  @DBOS.getApi("/redirect")
  static async redirectUrl() {
    const url = DBOS.request.url || "bad url"; // Get the raw url from request.
    DBOS.koaContext.redirect(url + "-dbos");
    return Promise.resolve();
  }

  @DBOS.getApi("/query")
  static async helloQuery(name: string) {
    DBOS.logger.info(`query with name ${name}`); // Test logging.
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.getApi("/querybody")
  static async helloQueryBody(@ArgSource(ArgSources.BODY) name: string) {
    DBOS.logger.info(`query with name ${name}`); // Test logging.
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi("/testdeletequery")
  static async testdeletequeryparam(name: string) {
    DBOS.logger.info(`delete with param from query with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi("/testdeleteurl/:name")
  static async testdeleteurlparam(name: string) {
    DBOS.logger.info(`delete with param from url with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.deleteApi("/testdeletebody")
  static async testdeletebodyparam(@ArgSource(ArgSources.BODY) name: string) {
    DBOS.logger.info(`delete with param from url with name ${name}`);
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.postApi("/testpost")
  static async testpost(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.putApi("/testput")
  static async testput(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.patchApi("/testpatch")
  static async testpatch(name: string) {
    return Promise.resolve(`hello ${name}`);
  }

  @DBOS.getApi("/dbos-error")
  @DBOS.transaction()
  static async dbosErr() {
    return Promise.reject(new DBOSResponseError("customize error", 503));
  }

  @DBOS.getApi("/handler/:name")
  static async testHandler(name: string) {
    const workflowUUID = DBOS.koaContext.get(WorkflowUUIDHeader);
    // Invoke a workflow using the given UUID.
    return await DBOS.withNextWorkflowID(workflowUUID, ()=> {
      return TestEndpoints.testWorkflow(name);
    });
  }

  @DBOS.getApi("/testStartWorkflow/:name")
  static async testStartWorkflow(name: string): Promise<string> {
    const wfh = await DBOS.startWorkflow(TestEndpoints).testWorkflow(name);
    return await wfh.getResult();
  }

  @DBOS.getApi("/testInvokeWorkflow/:name")
  static async testInvokeWorkflow(name: string): Promise<string> {
    return await TestEndpoints.testWorkflow(name);
  }

  @DBOS.postApi("/transaction/:name")
  @DBOS.transaction()
  static async testTransaction(name: string) {
    const { rows } = await DBOS.pgClient.query<TestKvTable>(`INSERT INTO ${testTableName}(id, value) VALUES (1, $1) RETURNING id`, [name]);
    return `hello ${rows[0].id}`;
  }

  @DBOS.getApi("/step/:input")
  @DBOS.step()
  static async testStep(input: string) {
    return Promise.resolve(input);
  }

  @DBOS.postApi("/workflow")
  @DBOS.workflow()
  static async testWorkflow(@ArgSource(ArgSources.QUERY) name: string) {
    const res = await TestEndpoints.testTransaction(name);
    return await TestEndpoints.testStep(res);
  }

  @DBOS.postApi("/error")
  @DBOS.workflow()
  static async testWorkflowError(name: string) {
    // This workflow should encounter duplicate primary key error.
    let res = await TestEndpoints.testTransaction(name);
    res = await TestEndpoints.testTransaction(name);
    return res;
  }
}

async function main() {
  const config = generateDBOSTestConfig();
  await setUpDBOSTestDb(config);
  DBOS.setConfig(config);
  await DBOS.launch();
  await DBOS.executor.queryUserDB(`DROP TABLE IF EXISTS ${testTableName};`);
  await DBOS.executor.queryUserDB(`CREATE TABLE IF NOT EXISTS ${testTableName} (id SERIAL PRIMARY KEY, value TEXT);`);

  await TestEndpoints.getKey();

  await DBOS.shutdown();
}

describe("dbos-v2api-tests-http", () => {
  test("simple-functions", async () => {
    await main();
  }, 15000);
});
