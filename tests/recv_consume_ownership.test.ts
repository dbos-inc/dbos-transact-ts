import { SystemDatabase, DBOS_FUNCNAME_RECV } from '../src/system_database';
import { DBOSWorkflowConflictError, DBOSUnexpectedStepError } from '../src/error';
import { runWithTopContext } from '../src/context';

describe('recv-consume-ownership', () => {
  const workflowID = 'test-workflow-id';
  const functionID = 42;
  const topic = 'test-topic';
  const startTime = Date.now();

  function createSysdbMock(
    storedResult?: { functionName?: string; output?: string | null; serialization?: string | null },
    ownerXidInDb: string | null = 'current-owner',
  ) {
    const sysdb = Object.create(SystemDatabase.prototype) as SystemDatabase;
    (sysdb as unknown as { nullTopic: string }).nullTopic = '__null__topic__';

    const getOperationResultAndThrowIfCancelled = jest.fn().mockResolvedValue(storedResult);
    const getWorkflowOwner = jest.fn().mockResolvedValue(ownerXidInDb);
    sysdb.getOperationResultAndThrowIfCancelled = getOperationResultAndThrowIfCancelled;
    sysdb.getWorkflowOwner = getWorkflowOwner;

    return { sysdb, getWorkflowOwner, getOperationResultAndThrowIfCancelled };
  }

  test('throws DBOSWorkflowConflictError when execution lost ownership of recorded recv', async () => {
    const { sysdb, getWorkflowOwner } = createSysdbMock(
      { functionName: DBOS_FUNCNAME_RECV, output: '"recorded-message"', serialization: null },
      'new-owner',
    );

    await expect(
      runWithTopContext({ workflowId: workflowID, ownerXid: 'stale-owner' }, async () => {
        return await sysdb.recvConsume(workflowID, functionID, topic, startTime);
      }),
    ).rejects.toThrow(DBOSWorkflowConflictError);

    expect(getWorkflowOwner).toHaveBeenCalledWith(workflowID);
  });

  test('returns recorded message when execution still owns the workflow', async () => {
    const { sysdb, getWorkflowOwner } = createSysdbMock(
      { functionName: DBOS_FUNCNAME_RECV, output: '"recorded-message"', serialization: 'json' },
      'current-owner',
    );

    const res = await runWithTopContext({ workflowId: workflowID, ownerXid: 'current-owner' }, async () => {
      return await sysdb.recvConsume(workflowID, functionID, topic, startTime);
    });

    expect(res).toEqual({ serializedValue: '"recorded-message"', serialization: 'json' });
    expect(getWorkflowOwner).toHaveBeenCalledWith(workflowID);
  });

  test('returns recorded message when called outside workflow context (ownerXid undefined)', async () => {
    const { sysdb, getWorkflowOwner } = createSysdbMock(
      { functionName: DBOS_FUNCNAME_RECV, output: '"recorded-message"', serialization: null },
      'any-owner',
    );

    const res = await sysdb.recvConsume(workflowID, functionID, topic, startTime);
    expect(res).toEqual({ serializedValue: '"recorded-message"', serialization: null });
    expect(getWorkflowOwner).not.toHaveBeenCalled();
  });

  test('throws DBOSUnexpectedStepError when recorded result function name is not recv', async () => {
    const { sysdb } = createSysdbMock(
      { functionName: 'otherFunction', output: '"message"', serialization: null },
      'current-owner',
    );

    await expect(
      runWithTopContext({ workflowId: workflowID, ownerXid: 'current-owner' }, async () => {
        return await sysdb.recvConsume(workflowID, functionID, topic, startTime);
      }),
    ).rejects.toThrow(DBOSUnexpectedStepError);
  });
});
