import { interruptibleSleep, Semaphore, waitForAbort } from '../src/utils';

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

describe('Semaphore', () => {
  // Track concurrent runners and assert the high-water mark never exceeds the limit.
  async function runUnderLimiter(limit: number, tasks: number) {
    const sem = new Semaphore(limit);
    let inFlight = 0;
    let peak = 0;
    await Promise.all(
      Array.from({ length: tasks }, () =>
        sem.runExclusive(async () => {
          inFlight++;
          peak = Math.max(peak, inFlight);
          // Yield a few times so overlapping runners would be observed if permitted.
          await new Promise((r) => setTimeout(r, 10));
          inFlight--;
        }),
      ),
    );
    return peak;
  }

  it('never runs more than `limit` operations concurrently', async () => {
    expect(await runUnderLimiter(3, 20)).toBe(3);
    expect(await runUnderLimiter(1, 10)).toBe(1);
  });

  it('is an unbounded passthrough when the limit is non-positive', async () => {
    expect(await runUnderLimiter(0, 8)).toBe(8);
    expect(await runUnderLimiter(-5, 8)).toBe(8);
  });

  it('releases the permit even when the task throws', async () => {
    const sem = new Semaphore(1);
    await expect(sem.runExclusive(() => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
    // If the permit leaked, this second acquire would hang forever.
    let ran = false;
    await sem.runExclusive(() => {
      ran = true;
      return Promise.resolve();
    });
    expect(ran).toBe(true);
  });

  it('hands permits to waiters in FIFO order', async () => {
    const sem = new Semaphore(1);
    const order: number[] = [];
    await sem.acquire(); // hold the only permit
    const waiters = [1, 2, 3].map((i) =>
      sem.acquire().then(() => {
        order.push(i);
        sem.release();
      }),
    );
    sem.release(); // wake the queue
    await Promise.all(waiters);
    expect(order).toEqual([1, 2, 3]);
  });
});
