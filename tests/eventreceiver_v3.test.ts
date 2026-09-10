import { DBOS, DBOSConfig, DBOSLifecycleCallback } from '../src';
import { generateDBOSTestConfig, setUpDBOSTestSysDb } from './helpers';

export interface ERDefaults {
  classval?: string;
}

export interface ERSpecifics {
  methodval?: string;
}

// Listener class
const sleepms = (ms: number) => new Promise((r) => setTimeout(r, ms));

class ERD implements DBOSLifecycleCallback {
  initialized = false;

  async deliver3Events() {
    for (let i = 1; i <= 3; ++i) {
      await sleepms(100);
      const eps = DBOS.getAssociatedInfo(this);
      for (const e of eps) {
        const { methodConfig, classConfig, methodReg } = e;
        const cs = classConfig as ERDefaults;
        const ms = methodConfig as ERSpecifics;

        await DBOS.withAuthedContext('ER', ['Event', 'Receiver'], async () => {
          return await methodReg.invoke(undefined, [cs.classval, ms.methodval, i]);
        });
      }
    }
  }

  async destroy() {
    this.initialized = false;
    return Promise.resolve();
  }

  async initialize() {
    this.initialized = true;
    return Promise.resolve();
  }
}

const erd = new ERD();

// Decorators - class
export function EventReceiverConfigure(cfg: string) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    const erInfo = DBOS.associateClassWithInfo(erd, ctor) as ERDefaults;
    erInfo.classval = cfg;
  }
  return clsdec;
}

// Decorators - method
export function EventConsumer(config?: string) {
  function mtddec<This, Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: [string, string, number]) => Promise<Return>>,
  ) {
    const { regInfo: receiverInfo } = DBOS.associateFunctionWithInfo(erd, inDescriptor.value!, {
      ctorOrProto: target,
      name: propertyKey,
    });

    const mRegistration = receiverInfo as ERSpecifics;
    mRegistration.methodval = config;

    return inDescriptor;
  }
  return mtddec;
}

@EventReceiverConfigure('myclass')
class MyEventReceiver {
  static callNumSum = 0;

  @EventConsumer('method1')
  @DBOS.workflow()
  static async method1(cv: string, mv: string, en: number) {
    if (cv !== 'myclass' || mv !== 'method1') throw new Error('Info missing!');
    if (DBOS.authenticatedUser !== 'ER') throw new Error('NOT correct user');
    MyEventReceiver.callNumSum += en;
    return Promise.resolve();
  }

  @EventConsumer('method2')
  @DBOS.workflow()
  static async method2(cv: string, mv: string, en: number) {
    if (cv !== 'myclass' || mv !== 'method2') throw new Error('Info missing!');
    MyEventReceiver.callNumSum += 10 * en;
    return Promise.resolve();
  }
}

describe('event-receiver-tests-v3', () => {
  let config: DBOSConfig;

  beforeAll(async () => {
    config = generateDBOSTestConfig();
    await setUpDBOSTestSysDb(config);
    DBOS.setConfig(config);
  });

  beforeEach(async () => {
    await DBOS.launch();
  });

  afterEach(async () => {
    await DBOS.shutdown();
  });

  test('wf-event', async () => {
    const _dropPromise = erd.deliver3Events();
    // Things will naturally start happening
    //   We simply wait until we get all the calls, 10 seconds tops
    for (let i = 0; i < 100; ++i) {
      if (MyEventReceiver.callNumSum === 66) break;
      await sleepms(100);
    }
    expect(MyEventReceiver.callNumSum).toBe(66);
  });
});
