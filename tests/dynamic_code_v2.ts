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

  static ran = false;

  @DBOS.scheduled({ crontab: '* * * * * *' })
  @DBOS.workflow()
  static async scheduledWF(_1: Date, _2: Date) {
    DBOSWFTest.ran = true;
    return await DBOSWFTest.runStep();
  }
}
