import { DBOS } from '@dbos-inc/dbos-sdk';

export class BadDecoratorClass {
  @DBOS.workflow()
  @DBOS.step()
  static async cantBeBoth() {
    return Promise.resolve();
  }
}
