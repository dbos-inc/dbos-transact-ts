const { DBOSQueueDuplicatedError } = require('@dbos-inc/dbos-sdk');

describe('public API exports', () => {
  test('exports DBOSQueueDuplicatedError from the package root', () => {
    const err = new DBOSQueueDuplicatedError('workflow-id', 'queue-name', 'dedup-id');

    expect(err).toBeInstanceOf(DBOSQueueDuplicatedError);
    expect(err.workflowID).toBe('workflow-id');
    expect(err.queue).toBe('queue-name');
    expect(err.deduplicationID).toBe('dedup-id');
  });
});
