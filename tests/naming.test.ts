import { ConfiguredInstance, DBOS, DBOSConfig, DBOSMethodMiddlewareInstaller, MethodRegistrationBase } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

@DBOS.className('ClassA')
class TestClass {
  @DBOS.workflow({ name: 'wfAStatic' })
  static decoratedWorkflow(value: number): Promise<number> {
    return TestClass.stepTestStatic(value);
  }

  @DBOS.step({ name: 'tsbs' })
  static stepTestStatic(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }
}

@DBOS.className('ClassB')
class TestClassInst extends ConfiguredInstance {
  @DBOS.workflow({ name: 'wfBStatic' })
  static async decoratedWorkflow(value: number): Promise<number> {
    return TestClassInst.stepTestStatic(value);
  }

  @DBOS.step({ name: 'tibs' })
  static async stepTestStatic(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }

  @DBOS.workflow({ name: 'wfBInstance' })
  async decoratedWorkflowInst(value: number): Promise<number> {
    return await this.stepTest(value);
  }

  @DBOS.step({ name: 'tibi' })
  async stepTest(value: number): Promise<number> {
    expect(DBOS.stepStatus).toBeDefined();
    return Promise.resolve(value * 100);
  }
}

const instA = new TestClassInst('A');
const instB = new TestClassInst('B');

class TestMWC implements DBOSMethodMiddlewareInstaller {
  seenClasses: Set<string> = new Set();
  seenMethods: Set<string> = new Set();

  installMiddleware(methodReg: MethodRegistrationBase): void {
    const rcn = methodReg.className;
    const rfn = methodReg.name;

    this.seenClasses.add(rcn);
    this.seenMethods.add(`${rcn}/${rfn}`);
  }
}

describe('rename_tests', () => {
  let config: DBOSConfig;
  let collector: TestMWC = new TestMWC();

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    expect(config.systemDatabaseUrl).toBeDefined();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    collector = new TestMWC();
    DBOS.registerMiddlewareInstaller(collector);
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('class-names-decorator', async () => {
    const rvs = await TestClass.decoratedWorkflow(10);
    const rvis = await TestClassInst.decoratedWorkflow(10);
    const rvia = await instA.decoratedWorkflowInst(10);
    const rvib = await instB.decoratedWorkflowInst(10);

    expect(rvs).toBe(1000);
    expect(rvis).toBe(1000);
    expect(rvia).toBe(1000);
    expect(rvib).toBe(1000);

    // Get registered classes
    const sc: string[] = [];
    collector.seenClasses.forEach((v) => sc.push(v));
    expect(sc.toSorted()).toStrictEqual(['ClassA', 'ClassB']);

    // Get registered workflows
    const sw: string[] = [];
    collector.seenMethods.forEach((v) => sw.push(v));
    expect(sw.filter((n) => n.includes('wf')).toSorted()).toStrictEqual([
      'ClassA/wfAStatic',
      'ClassB/wfBInstance',
      'ClassB/wfBStatic',
    ]);

    // Check class names in SysDB
    const classnames = (await DBOS.listWorkflows({})).map((wf) => wf.workflowClassName).sort();
    expect(classnames).toStrictEqual(['ClassA', 'ClassB', 'ClassB', 'ClassB']);

    // Check wf names in SysDB
    const wfnames = (await DBOS.listWorkflows({})).map((wf) => wf.workflowName).sort();
    expect(wfnames).toStrictEqual(['wfAStatic', 'wfBInstance', 'wfBInstance', 'wfBStatic']);

    // Check step names in SysDB
    const wfids = (await DBOS.listWorkflows({})).map((wf) => wf.workflowID);
    const stepnames: string[] = [];
    for (const id of wfids) {
      const lsr = await DBOS.listWorkflowSteps(id);
      for (const s of lsr ?? []) {
        if (!stepnames.includes(s.name)) stepnames.push(s.name);
      }
    }
    stepnames.sort();
    expect(stepnames).toStrictEqual(['tibi', 'tibs', 'tsbs']);
  });

  // TODO: Test enqueue
  // TODOL Test client
  // TODO: Test recover
  // TODO: Test external registrations (event rec stuff)

  // TODO: Negative testing (conflicts)
  // TODO: register calls; hybrid
});
