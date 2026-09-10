import { ConfiguredInstance, DBOS } from '../src';

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

  // Scheduled by clear-reg.test.ts after launch, since schedules live in the database.
  @DBOS.workflow()
  static async scheduledWF(_scheduledDate: Date, _context: unknown) {
    DBOSWFTest.ran = true;
    await DBOSWFTest.runStep();
  }
}

// Registered as a database-backed queue by clear-reg.test.ts after launch.
export const queue = { name: 'example_queue' };

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
