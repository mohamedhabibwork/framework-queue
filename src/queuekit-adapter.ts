import { FrameworkError, type JsonValue } from '@framework/core';
import { type BullMqConfig, BullMqProvider } from '@mohamedhabibwork/queuekit/bullmq';
import { type ConnectionOptions, type Job, Queue, UnrecoverableError, Worker } from 'bullmq';
import type { JobEnvelope } from './job';
import type { JobState, JobStatus } from './manager';

export { UnrecoverableError };

/**
 * Internal seam between `@framework/queue` and the queue transport. Queuekit
 * (`@mohamedhabibwork/queuekit`, bullmq provider) owns the provider instance
 * and the enqueue path; everything it cannot express stays on direct BullMQ
 * behind this one module — `manager.ts` and `worker.ts` never import bullmq
 * at runtime.
 *
 * Division of ownership:
 * - **queuekit** owns the provider lifecycle (connection plumbing derived
 *   from the framework's `ConnectionOptions`, BullMQ queue clients created
 *   lazily per destination, provider close) and the enqueue transport:
 *   `publish` with the framework's idempotency key as the BullMQ `jobId`,
 *   delay, attempts/backoff and retention options.
 * - **the framework** owns job contracts (`JobEnvelope`), per-job scopes,
 *   timeouts and execution contexts in the worker, batch bookkeeping (Redis
 *   hashes in `batch.ts`), and the job-inspection surface below.
 *
 * Documented queuekit gaps (0.4.1) that keep direct-BullMQ paths here:
 * - **No inspection surface.** The provider exposes only
 *   publish/publishMany/consume/native/close — no `getJob`, `getFailed`,
 *   `getJobCounts`, `pause`, `resume`, `obliterate` or `job.retry`. The
 *   manager needs all of those (status, failed, retry, counts, pause,
 *   `queue.obliterate` teardown), so the adapter keeps one direct `Queue`
 *   handle pointing at the same Redis queue (name/prefix/connection
 *   identical to the provider's).
 * - **Consumer wrapper loses the job result.** queuekit's `consume` wrapper
 *   `await`s the handler and discards its return value, but BullMQ stores
 *   that value as the job's `returnvalue` — which the framework surfaces as
 *   `JobStatus.result`. It also hides the raw `Worker`, which the framework
 *   needs for `autorun: false` start control and the `failed` hook. Worker
 *   creation therefore stays direct, keeping the framework's processor
 *   function (scopes, timeouts, execution contexts) verbatim.
 */

/**
 * Wire shape of a job dispatched through queuekit: the framework envelope
 * rides in the queuekit message's `payload` field (queuekit stores the whole
 * message object as the BullMQ job data).
 */
export interface QueueKitJobData {
  readonly type: string;
  readonly payload: JobEnvelope;
  readonly correlationId: string;
}

/**
 * Normalizes job data to a {@link JobEnvelope}: jobs published through
 * queuekit wrap it in {@link QueueKitJobData}, while jobs added directly on
 * the framework queue handle (e.g. the scheduler's fire jobs, whose data is
 * a raw envelope) keep the original shape.
 */
export function jobEnvelopeOf(data: QueueKitJobData | JobEnvelope): JobEnvelope {
  return 'v' in data ? data : data.payload;
}

/** Options of {@link QueueKitAdapter.enqueue}; mirrors the manager's dispatch knobs. */
export interface EnqueueOptions {
  /** Doubles as the BullMQ job id, collapsing duplicate dispatches into one job. */
  readonly jobId?: string;
  readonly delayMs?: number;
  readonly attempts: number;
  readonly backoffMs: number;
  readonly keepCompleted: number;
  readonly keepFailed: number;
}

const STATES: readonly JobState[] = ['waiting', 'active', 'completed', 'failed', 'delayed', 'paused'];

async function toStatus(job: Job<JobEnvelope, JsonValue>, state: string): Promise<JobStatus | null> {
  const known =
    STATES.find((s) => s === state) ?? (state === 'prioritized' || state === 'waiting-children' ? 'waiting' : null);
  if (!known || !job.id) return null;
  return {
    id: job.id,
    name: job.name,
    state: known,
    progress: typeof job.progress === 'number' ? job.progress : 0,
    attempts: job.attemptsMade,
    result: job.returnvalue ?? null,
    failedReason: job.failedReason ?? null,
    createdAt: new Date(job.timestamp).toISOString(),
    finishedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
  };
}

/**
 * Adapts the framework's queue operations onto queuekit's bullmq provider,
 * falling back to direct BullMQ where queuekit has no surface (see the
 * module-level gap documentation).
 */
export class QueueKitAdapter {
  readonly queueName: string;
  /**
   * Direct BullMQ queue handle for the inspection/pause surface queuekit
   * lacks, and the public `QueueManager.queue` surface external packages
   * program against (schedulers, obliterate teardown).
   */
  readonly queue: Queue<JobEnvelope, JsonValue>;
  readonly #provider: BullMqProvider;
  readonly #connection: ConnectionOptions;
  readonly #prefix: string;

  constructor(options: { readonly connection: ConnectionOptions; readonly prefix: string; readonly queue: string }) {
    this.#connection = options.connection;
    this.#prefix = options.prefix;
    this.queueName = options.queue;
    // Queuekit owns this provider (and the BullMQ queue client it creates
    // lazily on first publish). The framework's ConnectionOptions carry the
    // full BullMQ surface (maxRetriesPerRequest: null, db, auth); queuekit's
    // config type only names the host/port subset it passes through.
    this.#provider = new BullMqProvider({
      type: 'bullmq',
      connection: options.connection as unknown as BullMqConfig['connection'],
      prefix: options.prefix,
    });
    this.queue = new Queue<JobEnvelope, JsonValue>(options.queue, {
      connection: options.connection,
      prefix: options.prefix,
    });
  }

  /**
   * Enqueues through queuekit's provider. The optional job id becomes the
   * idempotency key (`jobId`), so duplicate dispatches collapse into one job.
   */
  async enqueue(name: string, envelope: JobEnvelope, options: EnqueueOptions): Promise<{ readonly id: string }> {
    const result = await this.#provider.publish<JobEnvelope>(
      this.queueName,
      // The envelope rides in `payload` (see QueueKitJobData); the envelope's
      // correlation id is mirrored at message level for queuekit middleware.
      { type: name, payload: envelope, correlationId: envelope.correlationId },
      {
        idempotencyKey: options.jobId,
        delay: options.delayMs,
        native: {
          attempts: options.attempts,
          backoff: { type: 'exponential', delay: options.backoffMs },
          removeOnComplete: { count: options.keepCompleted },
          removeOnFail: { count: options.keepFailed },
        },
      },
    );
    if (!result.messageId) throw new FrameworkError('JOB_DISPATCH_FAILED', `Queue did not assign an id to "${name}"`);
    return { id: result.messageId };
  }

  async status(id: string): Promise<JobStatus | null> {
    const job = await this.queue.getJob(id);
    if (!job) return null;
    const state = await job.getState();
    // The job hash (`getJob`) and its list membership (`getState`) are read in
    // two round trips, so a job that transitions between them would otherwise
    // be reported as terminal while the fields written by that same transition
    // (`failedReason`, `finishedOn`, `attemptsMade`, `returnvalue`) are still
    // stale. Once a terminal state is observed, re-read the hash so the
    // snapshot is consistent, and always report the state we branched on —
    // never a later re-read — so state and fields can never disagree.
    if (state === 'completed' || state === 'failed') {
      return toStatus((await this.queue.getJob(id)) ?? job, state);
    }
    return toStatus(job, state);
  }

  async failed(limit: number, offset: number): Promise<JobStatus[]> {
    const jobs = await this.queue.getFailed(offset, offset + limit - 1);
    // `getFailed` only returns jobs in the failed set, so their state is known
    // without a per-job round trip.
    return (await Promise.all(jobs.map((job) => toStatus(job, 'failed')))).filter((s): s is JobStatus => s !== null);
  }

  /** Re-queues a failed job; payload (and thus idempotency) is unchanged. */
  async retry(id: string): Promise<boolean> {
    const job = await this.queue.getJob(id);
    if (!job || (await job.getState()) !== 'failed') return false;
    await job.retry('failed');
    return true;
  }

  /** Removes one failed-job record; false when it no longer exists or is not failed. */
  async forget(id: string): Promise<boolean> {
    const job = await this.queue.getJob(id);
    if (!job || (await job.getState()) !== 'failed') return false;
    await job.remove();
    return true;
  }

  /** Re-queues up to `limit` failed jobs oldest-first; returns how many were replayed. */
  async retryAll(limit: number): Promise<number> {
    const jobs = await this.queue.getFailed(0, Math.max(0, limit - 1));
    let replayed = 0;
    for (const job of jobs) {
      if ((await job.getState()) === 'failed') {
        await job.retry('failed');
        replayed += 1;
      }
    }
    return replayed;
  }

  /** Removes failed/completed records older than `graceMs`; returns how many were removed. */
  async prune(graceMs: number): Promise<number> {
    const failed = await this.queue.clean(graceMs, 1_000, 'failed');
    const completed = await this.queue.clean(graceMs, 1_000, 'completed');
    return failed.length + completed.length;
  }

  counts(): Promise<Readonly<Record<string, number>>> {
    return this.queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
  }

  pause(): Promise<void> {
    return this.queue.pause();
  }

  resume(): Promise<void> {
    return this.queue.resume();
  }

  /**
   * Creates the consumer worker. Direct BullMQ on purpose (see the module
   * docs): queuekit's consumer wrapper drops the processor's return value —
   * the framework's `JobStatus.result` — and hides the raw worker needed for
   * `autorun: false` and the `failed` hook.
   */
  createWorker(
    processor: (job: Job<QueueKitJobData, JsonValue>) => Promise<JsonValue>,
    options: { readonly concurrency: number },
  ): Worker<QueueKitJobData, JsonValue> {
    return new Worker<QueueKitJobData, JsonValue>(this.queueName, processor, {
      connection: this.#connection,
      prefix: this.#prefix,
      concurrency: options.concurrency,
      autorun: false,
    });
  }

  /** Closes queuekit-owned resources (provider, its queue clients and workers), then the direct handle. */
  async close(): Promise<void> {
    await this.#provider.close();
    await this.queue.close();
  }
}
