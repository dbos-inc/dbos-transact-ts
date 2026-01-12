import { ConfiguredInstance, DBOS, WorkflowQueue } from '../src';

export class DBOSWFTest {
  @DBOS.step()
  static async runStep() {
    return Promise.resolve('B');
  }

  @DBOS.workflow()
  static async runWF() {
    return await DBOSWFTest.runStep();
  }

  static ran = false;

  @DBOS.scheduled({ crontab: '* * * * * *' })
  @DBOS.workflow()
  static async scheduledWF(_1: Date, _2: Date) {
    DBOSWFTest.ran = true;
    return await DBOSWFTest.runStep();
  }
}

export const queue = new WorkflowQueue('example_queue');

class TestFunctions extends ConfiguredInstance {
  constructor(name: string) {
    super(name);
  }

  @DBOS.step()
  async doStep(name: string) {
    return Promise.resolve(`step ${name} done from ${this.name}`);
  }

  @DBOS.workflow()
  async doWorkflow() {
    await this.doStep('');
    return `done ${this.name}`;
  }
}

export const instA = new TestFunctions('A2');
export const instB = new TestFunctions('B');
