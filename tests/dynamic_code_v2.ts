import { DBOS } from '../src';

export class DBOSWFTest {
  @DBOS.step()
  static async runStep() {
    return Promise.resolve('B');
  }

  @DBOS.workflow()
  static async runWF() {
    return await DBOSWFTest.runStep();
  }
}
