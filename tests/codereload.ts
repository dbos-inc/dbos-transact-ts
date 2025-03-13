import { DBOS } from '@dbos-inc/dbos-sdk';

// There is nothing wrong with this class, but the usage will be bad...
export class ImproperlyLoadedClass {
  @DBOS.workflow()
  static async justARegularWorkflow() {
    return Promise.resolve();
  }
}
