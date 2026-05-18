import { interruptibleSleep, waitForAbort } from '../src/utils';

describe('interruptibleSleep', () => {
  it('resolves after the timeout when the signal is never aborted', async () => {
    const controller = new AbortController();
    const start = Date.now();
    await interruptibleSleep(50, controller.signal);
    const elapsed = Date.now() - start;
    expect(elapsed).toBeGreaterThanOrEqual(40);
    expect(elapsed).toBeLessThan(500);
  });

  it('resolves early when the signal is aborted mid-sleep', async () => {
    const controller = new AbortController();
    const start = Date.now();
    const sleep = interruptibleSleep(10_000, controller.signal);
    setTimeout(() => controller.abort(), 20);
    await sleep;
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(500);
  });

  it('resolves immediately if the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const start = Date.now();
    await interruptibleSleep(10_000, controller.signal);
    expect(Date.now() - start).toBeLessThan(50);
  });

  // https://github.com/dbos-inc/dbos-transact-ts/issues/1253:
  // every call to the old `Promise.race([timer, stopPromise])`
  // pattern attached a `.then` to `stopPromise` that was never released. Here we
  // verify the new helper detaches its abort listener on the timer path — so
  // calling it repeatedly against a long-lived signal does not accumulate
  // listeners on the controller.
  it('does not accumulate abort listeners across repeated timer-wins calls', async () => {
    const controller = new AbortController();
    const addSpy = jest.spyOn(controller.signal, 'addEventListener');
    const removeSpy = jest.spyOn(controller.signal, 'removeEventListener');

    const iterations = 50;
    for (let i = 0; i < iterations; i++) {
      await interruptibleSleep(1, controller.signal);
    }

    const adds = addSpy.mock.calls.filter((c) => c[0] === 'abort').length;
    const removes = removeSpy.mock.calls.filter((c) => c[0] === 'abort').length;
    expect(adds).toBe(iterations);
    expect(removes).toBe(iterations);

    addSpy.mockRestore();
    removeSpy.mockRestore();
  });
});

describe('waitForAbort', () => {
  it('resolves when the signal is aborted', async () => {
    const controller = new AbortController();
    const start = Date.now();
    const wait = waitForAbort(controller.signal);
    setTimeout(() => controller.abort(), 20);
    await wait;
    expect(Date.now() - start).toBeLessThan(500);
  });

  it('resolves immediately if the signal is already aborted', async () => {
    const controller = new AbortController();
    controller.abort();
    const start = Date.now();
    await waitForAbort(controller.signal);
    expect(Date.now() - start).toBeLessThan(50);
  });
});
