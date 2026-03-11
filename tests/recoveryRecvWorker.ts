import { DBOS } from '../src';
import { DBOSExecutor } from '../src/dbos-executor';
import { sleepms } from '../src/utils';
import { generateDBOSTestConfig } from './helpers';
import { access } from 'node:fs/promises';

class RecoveryRecvWorker {
  @DBOS.workflow()
  static async recvWorkflow(topic: string, timeout: number) {
    return (await DBOS.recv<string>(topic, timeout)) ?? 'NULL';
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
    console.error('Usage: ts-node recoveryRecvWorker.ts <start|recover> <workflowID> <topic> <timeout> [barrierPath]');
    process.exit(1);
  }

  DBOS.setConfig({
    ...generateDBOSTestConfig(),
    runAdminServer: false,
  });
  await DBOS.launch();

  if (mode === 'start') {
    await DBOS.startWorkflow(RecoveryRecvWorker, { workflowID }).recvWorkflow(topic, timeout);
    console.log('STARTED');
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
    const result = (await handle.getResult()) as string;
    console.log(`RESULT:${result}`);

    await DBOS.shutdown();
    process.exit(0);
  }

  console.error(`Unknown mode: ${mode}`);
  process.exit(1);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
