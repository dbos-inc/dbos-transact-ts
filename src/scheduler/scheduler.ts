import { type SystemDatabase, WorkflowScheduleInternal, type WorkflowStatusInternal } from '../system_database';
import { DBOSSerializer, serializeArgs } from '../serialization';
import { randomUUID } from 'crypto';
import { interruptibleSleep, INTERNAL_QUEUE_NAME } from '../utils';
import { TimeMatcher } from './crontab';
import { DBOSError } from '../error';
import type { GlobalLogger } from '../telemetry/logs';
import { StatusString } from '../workflow';
import { runWithAbortableDbRetries } from '../context';

// Because of function contravariance, we need to use any here.
// We can't use generics because they don't support applySchedules and its array of schedules.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ScheduledWorkflowFn = (scheduledDate: Date, context: any) => Promise<void>;

export interface ScheduleOptions {
  automaticBackfill?: boolean;
  cronTimezone?: string;
  queueName?: string;
  // Shard key the schedule's workflows run under; required on a sharded system database.
  shardKey?: string;
}

export interface WorkflowSchedule {
  scheduleId: string;
  scheduleName: string;
  workflowName: string;
  workflowClassName: string;
  schedule: string;
  status: string; // "ACTIVE" | "PAUSED"
  context: unknown; // deserialized
  lastFiredAt: string | null;
  automaticBackfill: boolean;
  cronTimezone: string | null;
  queueName: string | null;
  // The application that owns this schedule and runs its workflows; undefined if unclaimed.
  applicationName?: string;
  // Shard key the schedule's workflows run under; undefined on an unsharded database.
  shardKey?: string;
}

export async function toWorkflowSchedule(
  internal: WorkflowScheduleInternal,
  serializer: DBOSSerializer,
): Promise<WorkflowSchedule> {
  const context = await serializer.parse(internal.context);

  return {
    scheduleId: internal.scheduleId,
    scheduleName: internal.scheduleName,
    workflowName: internal.workflowName,
    workflowClassName: internal.workflowClassName,
    schedule: internal.schedule,
    status: internal.status,
    context,
    lastFiredAt: internal.lastFiredAt,
    automaticBackfill: internal.automaticBackfill,
    cronTimezone: internal.cronTimezone,
    queueName: internal.queueName,
    applicationName: internal.applicationName,
    shardKey: internal.shardKey,
  };
}

export function createScheduleId(): string {
  return randomUUID();
}

// Definition fields an armed schedule captures; a change re-arms it. Runtime state (schedule_id, status, last_fired_at) is excluded so an unchanged re-apply doesn't re-arm it.
export function scheduleSignature(sched: WorkflowScheduleInternal): string {
  return JSON.stringify([
    sched.workflowName,
    sched.workflowClassName,
    sched.schedule,
    sched.context,
    sched.cronTimezone ?? null,
    sched.queueName ?? null,
    sched.shardKey ?? null,
  ]);
}

export const schedulerConfig = {
  /** Most scheduled runs enqueued in one transaction. */
  fireBatchSize: 1000,
  /** How long a standby keeps a tick no poll has shown fired, or three polling intervals if longer. */
  bufferMinMs: 3_600_000,
  /** How far back a starting scheduler arms each schedule, so a tick missed while its leader died still fires. */
  startupLookbackMs: 30_000,
  /** How often a scheduler that isn't firing checks whether it has taken over. */
  standbyCheckMs: 1000,
};

// During the first minute after startup, poll for schedules every second so schedules
// registered around launch are picked up promptly rather than after a full polling interval.
const STARTUP_FAST_POLL_DURATION_MS = 60_000;
const STARTUP_FAST_POLL_INTERVAL_MS = 1_000;

// Timers can't sleep past ~24.8 days, so wait for distant ticks in bounded chunks.
const MAX_SCHEDULE_SLEEP_MS = 60 * 60 * 1000;

/** What a schedule's runs are enqueued with. */
export interface ScheduleDefinition {
  scheduleName: string;
  workflowName: string;
  workflowClassName: string;
  context: unknown;
  queueName?: string;
  applicationName?: string;
  shardKey?: string;
}

export async function scheduleDefinition(
  sched: WorkflowScheduleInternal,
  serializer: DBOSSerializer,
): Promise<ScheduleDefinition> {
  return {
    scheduleName: sched.scheduleName,
    workflowName: sched.workflowName,
    workflowClassName: sched.workflowClassName,
    context: await serializer.parse(sched.context),
    queueName: sched.queueName ?? undefined,
    applicationName: sched.applicationName,
    shardKey: sched.shardKey,
  };
}

/** The first tick strictly after `after`, or Infinity if the schedule can never fire again. */
function nextTick(matcher: TimeMatcher, after: number): number {
  const next = matcher.nextWakeupTime(after).getTime();
  // TimeMatcher returns a non-matching instant only past its search horizon.
  return matcher.match(next) ? next : Infinity;
}

/** An active schedule in the scheduler and its next tick. */
export class ScheduleEntry {
  readonly scheduleId: string;
  // Definition snapshot; the scheduler re-arms the entry when it changes.
  readonly signature: string;
  nextTime: number;

  private constructor(
    sched: WorkflowScheduleInternal,
    readonly definition: ScheduleDefinition,
    readonly matcher: TimeMatcher,
    after: number,
  ) {
    this.scheduleId = sched.scheduleId;
    this.signature = scheduleSignature(sched);
    this.nextTime = nextTick(matcher, after);
  }

  /** Arm `sched` at its first tick after `after`, by default now. Throws if it can't run. */
  static async create(
    sched: WorkflowScheduleInternal,
    matcher: TimeMatcher,
    serializer: DBOSSerializer,
    after?: number,
  ): Promise<ScheduleEntry> {
    const definition = await scheduleDefinition(sched, serializer);
    return new ScheduleEntry(sched, definition, matcher, after ?? Date.now());
  }

  advance(): void {
    this.nextTime = nextTick(this.matcher, this.nextTime);
  }

  advancePast(lastFired: number): void {
    while (this.nextTime <= lastFired) {
      this.advance();
    }
  }
}

/** A min-heap of entries by next tick, ties in push order. */
class TickHeap {
  readonly #items: { at: number; seq: number; entry: ScheduleEntry }[] = [];
  #seq = 0;

  get size(): number {
    return this.#items.length;
  }

  /** The earliest tick, or Infinity if empty. */
  peek(): number {
    return this.#items[0]?.at ?? Infinity;
  }

  push(entry: ScheduleEntry): void {
    const items = this.#items;
    items.push({ at: entry.nextTime, seq: this.#seq++, entry });
    let i = items.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!this.#before(i, parent)) break;
      [items[i], items[parent]] = [items[parent], items[i]];
      i = parent;
    }
  }

  pop(): ScheduleEntry {
    const items = this.#items;
    const top = items[0];
    const last = items.pop()!;
    if (items.length > 0) {
      items[0] = last;
      let i = 0;
      while (true) {
        const left = 2 * i + 1;
        const right = left + 1;
        let least = i;
        if (left < items.length && this.#before(left, least)) least = left;
        if (right < items.length && this.#before(right, least)) least = right;
        if (least === i) break;
        [items[i], items[least]] = [items[least], items[i]];
        i = least;
      }
    }
    return top.entry;
  }

  #before(a: number, b: number): boolean {
    const x = this.#items[a];
    const y = this.#items[b];
    return x.at < y.at || (x.at === y.at && x.seq < y.seq);
  }
}

/** Fires every active schedule from one loop, enqueueing each tick's runs in batches. */
export class Scheduler {
  // Active schedules keyed by schedule ID.
  readonly entries = new Map<string, ScheduleEntry>();
  // One item per entry in `entries`; rebuilt by every poll.
  heap = new TickHeap();
  // Whether a poll has armed the schedules this scheduler started with.
  #started = false;

  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly serializer: DBOSSerializer,
    readonly logger: GlobalLogger,
  ) {}

  /** Sync the armed schedules with the database, then rebuild the heap from them. */
  async poll(): Promise<void> {
    try {
      await this.#reconcile();
    } finally {
      // Even after a partial reconcile, so the heap never holds dropped entries or misses one.
      this.heap = new TickHeap();
      for (const entry of this.entries.values()) {
        this.heap.push(entry);
      }
    }
  }

  async #reconcile(): Promise<void> {
    const schedules = await this.systemDatabase.listSchedules({ applicationName: this.systemDatabase.appName });
    const currentIds = new Set(schedules.map((s) => s.scheduleId));
    for (const scheduleId of this.entries.keys()) {
      if (!currentIds.has(scheduleId)) {
        this.entries.delete(scheduleId);
      }
    }
    for (const sched of schedules) {
      const entry = this.entries.get(sched.scheduleId);
      if (sched.status !== 'ACTIVE') {
        // Paused: drop it; resuming re-arms it, backfilling if enabled.
        this.entries.delete(sched.scheduleId);
        continue;
      }
      if (entry !== undefined) {
        // Running: re-arm on a changed definition, without backfill.
        if (entry.signature !== scheduleSignature(sched)) {
          this.entries.delete(sched.scheduleId);
          await this.#arm(sched);
        } else {
          this.#skipFired(entry, sched);
        }
        continue;
      }
      // Not running: arm it, then backfill missed executions if enabled; a broken one is retried next poll.
      const armed = await this.#arm(sched, this.#started ? undefined : startupAfter(sched));
      if (armed === undefined) continue;
      if (sched.automaticBackfill && sched.lastFiredAt) {
        try {
          const lastFired = new Date(sched.lastFiredAt);
          const now = new Date();
          if (lastFired < now) {
            await backfillSchedule(this.systemDatabase, this.serializer, sched.scheduleName, lastFired, now);
          }
        } catch (e) {
          this.logger.warn(`Exception during automatic backfill for schedule '${sched.scheduleName}': ${errorText(e)}`);
        }
      }
    }
    this.#started = true;
  }

  /** Pass over the ticks this schedule's last_fired_at shows some process enqueued. */
  #skipFired(entry: ScheduleEntry, sched: WorkflowScheduleInternal): void {
    if (!sched.lastFiredAt) return;
    const lastFired = Date.parse(sched.lastFiredAt);
    // Unreadable: the entry keeps its ticks, refired at worst.
    if (Number.isNaN(lastFired)) return;
    try {
      entry.advancePast(lastFired);
    } catch (e) {
      this.#drop(entry, e);
    }
  }

  /** Register a new entry for this schedule, or log why it can't run and return undefined. */
  async #arm(sched: WorkflowScheduleInternal, after?: number): Promise<ScheduleEntry | undefined> {
    let matcher: TimeMatcher;
    try {
      matcher = new TimeMatcher(sched.schedule, sched.cronTimezone ?? undefined);
    } catch (e) {
      this.logger.error(
        `Cannot run schedule '${sched.scheduleName}'. Invalid crontab "${sched.schedule}": ${errorText(e)}`,
      );
      return undefined;
    }
    let entry: ScheduleEntry;
    try {
      entry = await ScheduleEntry.create(sched, matcher, this.serializer, after);
    } catch (e) {
      this.logger.error(`Cannot run schedule '${sched.scheduleName}': ${errorText(e)}`);
      return undefined;
    }
    this.entries.set(entry.scheduleId, entry);
    return entry;
  }

  /** Milliseconds until the earliest tick is `lag` milliseconds old, Infinity if none. */
  msUntilDue(lag: number = 0): number {
    return this.heap.peek() + lag - Date.now();
  }

  /** Fire every due tick, batch by batch, until `signal` aborts. */
  async fireDue(signal: AbortSignal): Promise<void> {
    const now = Date.now();
    const due: ScheduleEntry[] = [];
    while (this.heap.size > 0 && this.heap.peek() <= now) {
      due.push(this.heap.pop());
    }
    const batchSize = schedulerConfig.fireBatchSize;
    let fired = 0;
    while (fired < due.length && !signal.aborted) {
      await this.#fire(due.slice(fired, fired + batchSize));
      fired += batchSize;
    }
    for (const entry of due.slice(0, fired)) {
      this.#advance(entry);
    }
    // Left unstamped, so the process that takes over fires them.
    for (const entry of due.slice(fired)) {
      this.heap.push(entry);
    }
  }

  /** Pass over ticks older than `before` without firing them. */
  skipDue(before: number): void {
    while (this.heap.size > 0 && this.heap.peek() <= before) {
      this.#advance(this.heap.pop());
    }
  }

  #advance(entry: ScheduleEntry): void {
    // A late tick's successor may already be due; it fires on the next pass.
    try {
      entry.advance();
    } catch (e) {
      this.#drop(entry, e);
      return;
    }
    this.heap.push(entry);
  }

  #drop(entry: ScheduleEntry, e: unknown): void {
    // Dropped; the next poll tries to arm it again.
    this.entries.delete(entry.scheduleId);
    this.logger.error(`Cannot run schedule '${entry.definition.scheduleName}': ${errorText(e)}`);
  }

  async #fire(batch: ScheduleEntry[]): Promise<void> {
    try {
      await enqueueTicks(
        this.systemDatabase,
        this.serializer,
        batch.map((entry) => [entry.definition, entry.nextTime]),
        true,
      );
      return;
    } catch (e) {
      if (batch.length === 1) {
        this.logger.warn(`Exception in schedule '${batch[0].definition.scheduleName}': ${errorText(e)}`);
        return;
      }
      // Logged here, as the per-schedule retries may succeed and never show it.
      this.logger.warn(
        `Exception enqueueing ${batch.length} scheduled workflows, retrying each schedule alone: ${errorText(e)}`,
      );
    }
    // One failing schedule must not hold back the rest of its batch.
    for (const entry of batch) {
      await this.#fire([entry]);
    }
  }
}

/** Where a starting scheduler arms a schedule: past its last fired tick, back at most the startup look-back. Later arms (creates, resumes) start from now. */
function startupAfter(sched: WorkflowScheduleInternal): number | undefined {
  if (!sched.lastFiredAt) return undefined;
  const lastFired = Date.parse(sched.lastFiredAt);
  if (Number.isNaN(lastFired)) return undefined;
  return Math.max(lastFired, Date.now() - schedulerConfig.startupLookbackMs);
}

function errorText(e: unknown): string {
  return e instanceof Error ? (e.stack ?? e.message) : String(e);
}

/** Poll schedules until aborted, firing only while `leading()`; a standby keeps each tick until a poll shows it fired, so a taker fires all it missed. */
export async function schedulerLoop(
  signal: AbortSignal,
  scheduler: Scheduler,
  pollingIntervalMs: number,
  leading: () => boolean,
): Promise<void> {
  const bufferMs = Math.max(schedulerConfig.bufferMinMs, 3 * pollingIntervalMs);
  const startupDeadline = performance.now() + STARTUP_FAST_POLL_DURATION_MS;
  const pollTimeout = () =>
    performance.now() < startupDeadline
      ? Math.min(STARTUP_FAST_POLL_INTERVAL_MS, pollingIntervalMs)
      : pollingIntervalMs;

  let nextPoll = performance.now();
  while (!signal.aborted) {
    if (performance.now() >= nextPoll) {
      try {
        await scheduler.poll();
      } catch (e) {
        scheduler.logger.warn(`Exception polling schedules: ${errorText(e)}`);
      }
      nextPoll = performance.now() + pollTimeout();
    }
    // Checked once per pass: a holder losing the lock mid-pass still fires all it took.
    const firing = leading();
    try {
      if (firing) {
        await scheduler.fireDue(signal);
      } else {
        scheduler.skipDue(Date.now() - bufferMs);
      }
    } catch (e) {
      scheduler.logger.warn(`Exception firing schedules: ${errorText(e)}`);
    }
    const untilPoll = nextPoll - performance.now();
    const wait = firing
      ? Math.min(untilPoll, scheduler.msUntilDue())
      : Math.min(untilPoll, scheduler.msUntilDue(bufferMs), schedulerConfig.standbyCheckMs);
    // Always yields, so a pass that finds more due work never starves the event loop.
    await interruptibleSleep(Math.min(Math.max(wait, 0), MAX_SCHEDULE_SLEEP_MS), signal);
  }
}

/** Runs the scheduler loop from `initialize` until `destroy`, firing while `leading()`. */
export class DynamicSchedulerLoop {
  #controller: AbortController | undefined;
  #promise: Promise<void> | undefined;
  readonly #pollingIntervalMs: number;

  constructor(
    readonly systemDatabase: SystemDatabase,
    readonly serializer: DBOSSerializer,
    readonly logger: GlobalLogger,
    pollingIntervalMs: number | undefined,
    readonly leading: () => boolean,
  ) {
    this.#pollingIntervalMs = pollingIntervalMs ?? 30000;
  }

  /** Start the loop; a no-op while it runs. */
  async initialize(): Promise<void> {
    if (this.#controller !== undefined) return;
    const controller = new AbortController();
    this.#controller = controller;
    const scheduler = new Scheduler(this.systemDatabase, this.serializer, this.logger);
    // Only enqueues, never runs a workflow, so its retries may be abandoned when the loop stops.
    this.#promise = runWithAbortableDbRetries(controller.signal, () =>
      schedulerLoop(controller.signal, scheduler, this.#pollingIntervalMs, this.leading),
    );
    await Promise.resolve();
  }

  /** Stop the loop and wait for it to end; a no-op if it isn't running. */
  async destroy(): Promise<void> {
    const controller = this.#controller;
    const promise = this.#promise;
    // Cleared first, so an initialize while this one winds down starts a fresh loop.
    this.#controller = undefined;
    this.#promise = undefined;
    controller?.abort();
    if (promise !== undefined) {
      await Promise.allSettled([promise]);
    }
  }
}

/** The ENQUEUED status row of one scheduled run. */
async function scheduledStatus(
  definition: ScheduleDefinition,
  scheduledDate: Date,
  workflowID: string,
  applicationVersion: string,
  owner: string | undefined,
  serializer: DBOSSerializer,
): Promise<WorkflowStatusInternal> {
  const serparam = await serializeArgs([scheduledDate, definition.context], undefined, serializer, undefined);
  return {
    workflowUUID: workflowID,
    status: StatusString.ENQUEUED,
    workflowName: definition.workflowName,
    workflowClassName: definition.workflowClassName,
    workflowConfigName: '',
    applicationVersion,
    queueName: definition.queueName || INTERNAL_QUEUE_NAME,
    authenticatedUser: '',
    output: null,
    error: null,
    assumedRole: '',
    authenticatedRoles: [],
    executorId: '',
    applicationID: '',
    input: serparam.serializedValue,
    deduplicationID: undefined,
    priority: 0,
    queuePartitionKey: undefined,
    serialization: serparam.serialization,
    scheduleName: definition.scheduleName,
    // Owned where possible: the internal queue's shared name cannot route an unclaimed row.
    applicationName: owner,
    shardKey: definition.shardKey,
  };
}

/** The schedule's owner routes its runs, whoever fires them; an unclaimed one falls back to the firing handle, which may have no identity itself. */
function scheduleOwner(systemDatabase: SystemDatabase, definition: ScheduleDefinition): string | undefined {
  return definition.applicationName ?? systemDatabase.appName;
}

/** Enqueue these scheduled runs in one batch, skipping existing ones. Returns their workflow IDs. */
export async function enqueueTicks(
  systemDatabase: SystemDatabase,
  serializer: DBOSSerializer,
  ticks: [ScheduleDefinition, number][],
  stampLastFired: boolean,
): Promise<string[]> {
  // Scheduled workflows are always enqueued to their owner's latest application version.
  const versions = new Map<string | undefined, string>();
  const statuses: WorkflowStatusInternal[] = [];
  const firedAt = new Map<string, string>();
  for (const [definition, at] of ticks) {
    const owner = scheduleOwner(systemDatabase, definition);
    let version = versions.get(owner);
    if (version === undefined) {
      version = (await systemDatabase.getLatestApplicationVersion(owner)).versionName;
      versions.set(owner, version);
    }
    const scheduledDate = new Date(at);
    const workflowID = `sched-${definition.scheduleName}-${scheduledDate.toISOString()}`;
    statuses.push(await scheduledStatus(definition, scheduledDate, workflowID, version, owner, serializer));
    if (stampLastFired) {
      firedAt.set(workflowID, scheduledDate.toISOString());
    }
  }
  await systemDatabase.enqueueScheduledWorkflows(statuses, firedAt);
  return statuses.map((status) => status.workflowUUID);
}

export async function triggerSchedule(
  systemDatabase: SystemDatabase,
  serializer: DBOSSerializer,
  name: string,
): Promise<string> {
  const sched = await systemDatabase.getSchedule(name);
  if (!sched) {
    throw new DBOSError(`Schedule "${name}" not found`);
  }
  const definition = await scheduleDefinition(sched, serializer);
  const now = new Date();
  const workflowID = `sched-${name}-trigger-${now.toISOString()}`;
  const owner = scheduleOwner(systemDatabase, definition);
  const latestVersion = await systemDatabase.getLatestApplicationVersion(owner);
  await systemDatabase.initWorkflowStatus(
    await scheduledStatus(definition, now, workflowID, latestVersion.versionName, owner, serializer),
    null,
  );
  return workflowID;
}

/** Enqueue every scheduled execution between `start` and `end`. Returns their workflow IDs. */
export async function backfillSchedule(
  systemDatabase: SystemDatabase,
  serializer: DBOSSerializer,
  name: string,
  start: Date,
  end: Date,
): Promise<string[]> {
  const sched = await systemDatabase.getSchedule(name);
  if (!sched) {
    throw new DBOSError(`Schedule "${name}" not found`);
  }
  const definition = await scheduleDefinition(sched, serializer);
  const matcher = new TimeMatcher(sched.schedule, sched.cronTimezone ?? undefined);
  const times: number[] = [];
  for (let next = nextTick(matcher, start.getTime()); next < end.getTime(); next = nextTick(matcher, next)) {
    times.push(next);
  }
  const batchSize = schedulerConfig.fireBatchSize;
  const workflowIDs: string[] = [];
  for (let i = 0; i < times.length; i += batchSize) {
    const batch = times.slice(i, i + batchSize).map((at): [ScheduleDefinition, number] => [definition, at]);
    workflowIDs.push(...(await enqueueTicks(systemDatabase, serializer, batch, false)));
  }
  return workflowIDs;
}

/** The shard key a schedule's workflows run under. */
export async function scheduleShardKey(systemDatabase: SystemDatabase, name: string): Promise<string | undefined> {
  return (await systemDatabase.getSchedule(name))?.shardKey;
}
