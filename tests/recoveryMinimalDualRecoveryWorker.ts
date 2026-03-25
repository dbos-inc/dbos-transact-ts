import { DBOS } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig } from './helpers';
import { access } from 'node:fs/promises';

/** Child process for `repro-minimal-dual-local-recovery-with-conflicting-recv` in `recovery.test.ts`. */
class MinimalDualRecoveryWorker {
  @DBOS.workflow()
  static async workflow(topic: string, timeout: number) {
    const beforeRecvMs = Math.max(0, parseInt(process.env.DBOS_TEST_MINIMAL_BEFORE_RECV_MS ?? '0', 10) || 0);

    await DBOS.runStep(() => Promise.resolve({ phase: 'preflight' } as const), { name: 'preflight' });

    try {
      await DBOS.runStep(
        async () => {
          if (beforeRecvMs > 0) {
            await sleepms(beforeRecvMs);
          }
          return { phase: 'beforeRecv' } as const;
        },
        { name: 'beforeRecv' },
      );

      const msg = (await DBOS.recv<string>(topic, timeout)) ?? 'NULL';
      await DBOS.runStep(() => Promise.resolve({ msg }), { name: 'afterFirstRecv' });
      return msg;
    } catch (err) {
      await DBOS.runStep(() => Promise.resolve({ cleanup: 1 } as const), { name: 'cleanupOne' });
      await DBOS.runStep(() => Promise.resolve({ cleanup: 2 } as const), { name: 'cleanupTwo' });
      throw err;
    }
  }
}

async function waitForBarrier(barrierPath: string) {
  while (true) {
    try {
      await access(barrierPath);
      return;
    } catch {
      await sleepms(25);
    }
  }
}

async function main() {
  const mode = process.argv[2];
  const workflowID = process.argv[3];
  const topic = process.argv[4];
  const timeout = Number(process.argv[5] ?? '5');

  if (!mode || !workflowID || !topic) {
    console.error(
      'Usage: ts-node recoveryMinimalDualRecoveryWorker.ts <start|recover> <workflowID> <topic> <timeout> [barrierPath]',
    );
    process.exit(1);
  }

  DBOS.setConfig({
    ...generateDBOSTestConfig(),
    runAdminServer: false,
  });
  await DBOS.launch();

  if (mode === 'start') {
    await DBOS.startWorkflow(MinimalDualRecoveryWorker, { workflowID }).workflow(topic, timeout);
    console.log('STARTED');
    const preExitGrace = Math.max(0, parseInt(process.env.DBOS_TEST_START_WORKER_PREEXIT_GRACE_MS ?? '0', 10) || 0);
    if (preExitGrace > 0) {
      await sleepms(preExitGrace);
    }
    process.exit(0);
  }

  if (mode === 'recover') {
    const barrierPath = process.argv[6];
    if (!barrierPath) {
      console.error('Recovery mode requires barrierPath');
      process.exit(1);
    }

    console.log('PREPARED');
    await waitForBarrier(barrierPath);

    const recoveredHandles = await DBOSExecutor.globalInstance!.recoverPendingWorkflows(['local']);
    console.log(`RECOVERED:${recoveredHandles.map((h) => h.workflowID).join(',')}`);

    await sleepms(100);
    console.log('READY');

    const handle =
      recoveredHandles.find((h) => h.workflowID === workflowID) ?? DBOS.retrieveWorkflow<string>(workflowID);
    const result = await handle.getResult();
    console.log(`RESULT:${String(result)}`);

    await DBOS.shutdown();
    process.exit(0);
  }

  console.error(`Unknown mode: ${String(mode)}`);
  process.exit(1);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
