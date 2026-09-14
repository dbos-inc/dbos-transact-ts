export class DebugAction {
  callback?: () => void;
  asyncCallback?: () => Promise<void>;
}

export const pointTriggers: Map<string, DebugAction> = new Map();

export async function debugTriggerPoint(name: string): Promise<void> {
  if (pointTriggers.has(name)) {
    const pt = pointTriggers.get(name)!;
    if (pt.asyncCallback) {
      await pt.asyncCallback();
    }
    if (pt.callback) {
      pt.callback();
    }
  }
}

export function setDebugTrigger(name: string, action: DebugAction) {
  pointTriggers.set(name, action);
}

export function clearDebugTriggers() {
  pointTriggers.clear();
}

export const DEBUG_TRIGGER_WORKFLOW_QUEUE_START = 'DEBUG_TRIGGER_WORKFLOW_QUEUE_START';
export const DEBUG_TRIGGER_WORKFLOW_ENQUEUE = 'DEBUG_TRIGGER_WORKFLOW_ENQUEUE';
export const DEBUG_TRIGGER_STEP_COMMIT = 'DEBUG_TRIGGER_STEP_COMMIT';
export const DEBUG_TRIGGER_INITWF_COMMIT = 'DEBUG_TRIGGER_INITWF_COMMIT';
// Fires inside runQueue between dispatching consecutive partition keys.
export const DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES = 'DEBUG_TRIGGER_BETWEEN_PARTITION_DISPATCHES';
// Fires inside findAndMarkStartableWorkflows after the SELECT FOR UPDATE NOWAIT
// but before COMMIT (i.e. while the row lock is held). Tests can throw a
// synthetic 55P03 here to simulate a concurrent executor winning the lock race,
// which is the exact condition that triggers the orphan-PENDING bug.
export const DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT = 'DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT';
// Fires inside findAndMarkStartablePartitionedWorkflows between the candidate-head snapshot and the lock select, where a test can move a candidate to another queue to exercise the claim guard.
export const DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES = 'DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES';
