import { sleepms } from './utils';

export function getCallSiteInfo(): { fileName: string; lineNumber: number } {
  const err = new Error();
  const stack = err.stack?.split('\n');

  if (stack && stack.length > 2) {
    // The third line usually contains the callsite information.
    // Different environments (Node, browser) format the stack trace differently.
    // Adjust the regex to your environment as needed.
    const match = stack[2].match(/at\s+(.*):(\d+):(\d+)/);
    if (match) {
      const fileName = match[1];
      const lineNumber = parseInt(match[2], 10);
      return { fileName, lineNumber };
    }
  }
  return { fileName: 'unknown', lineNumber: -1 };
}

export interface DebugPoint {
  name: string;
  fileName: string;
  lineNumber: number;
  hitCount: number;
}

export class DebugAction {
  sleepms?: number; // Sleep at point
  awaitEvent?: Promise<void>; // Wait at point
  callback?: () => void;
  asyncCallback?: () => Promise<void>;
}

export const pointTriggers: Map<string, DebugAction> = new Map();
export const pointLocations: Map<string, DebugPoint> = new Map();

export async function debugTriggerPoint(name: string): Promise<void> {
  const cpi = getCallSiteInfo();
  if (!pointLocations.has(name)) {
    pointLocations.set(name, { name, ...cpi, hitCount: 0 });
  }
  const p = pointLocations.get(name)!;
  if (p.fileName !== cpi.fileName || cpi.lineNumber !== p.lineNumber) {
    throw new Error(`Duplicate debug point name: ${name}`);
  }

  if (pointTriggers.has(name)) {
    const pt = pointTriggers.get(name)!;
    if (pt.sleepms) {
      await sleepms(pt.sleepms);
    }
    if (pt.asyncCallback) {
      await pt.asyncCallback();
    }
    if (pt.callback) {
      pt.callback();
    }
    if (pt.awaitEvent) {
      await pt.awaitEvent;
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
