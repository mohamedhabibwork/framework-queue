import { randomUUID } from 'node:crypto';
import { type ExecutionContext, FrameworkError, type JsonObject, type JsonValue, token } from '@framework/core';
import type { ConnectionOptions, Queue } from 'bullmq';
import Redis, { type RedisOptions } from 'ioredis';
import { z } from 'zod/v4';
import { type BatchCompletionJob, type BatchRecord, BatchRegistry, type BatchState } from './batch';
import type { JobDefinition, JobEnvelope } from './job';
import { QueueKitAdapter } from './queuekit-adapter';

export type JobState = 'waiting' | 'active' | 'completed' | 'failed' | 'delayed' | 'paused';

export interface JobStatus {
  readonly id: string;
  readonly name: string;
  readonly state: JobState;
  readonly progress: number;
  readonly attempts: number;
  readonly result: JsonValue;
  readonly failedReason: string | null;
  readonly createdAt: string;
  readonly finishedAt: string | null;
}

/** JSON schema of {@link JobStatus} (OpenAPI documentation of status endpoints). */
export const jobStatusSchema = z.object({
  id: z.string(),
  name: z.string(),
  state: z.enum(['waiting', 'active', 'completed', 'failed', 'delayed', 'paused']),
  progress: z.number().min(0).max(100),
  attempts: z.number().int().min(0),
  result: z.lazy(() =>
    z.union([z.string(), z.number(), z.boolean(), z.null(), z.array(jsonValue), z.record(z.string(), jsonValue)]),
  ),
  failedReason: z.string().nullable(),
  createdAt: z.string(),
  finishedAt: z.string().nullable(),
});

const jsonValue: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([z.string(), z.number(), z.boolean(), z.null(), z.array(jsonValue), z.record(z.string(), jsonValue)]),
);

/** BullMQ connection fields beyond the URL (auth, database, TLS). */
export interface QueueConnectionOptions {
  readonly host?: string;
  readonly port?: number;
  readonly username?: string;
  readonly password?: string;
  /** Redis logical database. */
  readonly db?: number;
  /** TLS options for `rediss://`-style endpoints. */
  readonly tls?: Record<string, unknown>;
  /** Fail fast instead of retrying forever (BullMQ requires `null` for workers). */
  readonly maxRetriesPerRequest?: number | null;
  readonly enableReadyCheck?: boolean;
  readonly connectTimeout?: number;
}

export interface QueueOptions {
  readonly url: string;
  /** Redis key prefix so several apps can share one Redis. */
  readonly prefix: string;
  readonly queue?: string;
  /** Completed/failed jobs kept for inspection (bounded). */
  readonly keepCompleted?: number;
  readonly keepFailed?: number;
  /** Extra BullMQ connection fields (db, auth, TLS) merged over the URL. */
  readonly connection?: QueueConnectionOptions;
  /**
   * Default BullMQ job options applied to every dispatch
   * (`attempts`, `backoff`, `removeOnComplete`, `removeOnFail`, `priority`, …).
   * Per-dispatch options win over these.
   */
  readonly defaultJobOptions?: Readonly<Record<string, unknown>>;
  /** BullMQ rate limiter (`max` jobs per `duration` ms). */
  readonly limiter?: { readonly max: number; readonly duration: number };
  /**
   * Extra BullMQ `Queue` options forwarded verbatim (`streams`, `skipVersionCheck`,
   * `defaultJobOptions`, …). The escape hatch for anything not modelled above.
   */
  readonly native?: Readonly<Record<string, unknown>>;
}

export interface DispatchOptions {
  readonly jobId?: string;
  readonly delayMs?: number;
  /** Overrides the definition's attempt count for this dispatch. */
  readonly attempts?: number;
  /** BullMQ priority (lower runs first; 0 is the default). */
  readonly priority?: number;
  /**
   * Raw BullMQ `JobsOptions` for this dispatch, merged last — the escape hatch
   * for `removeOnComplete`, `backoff`, `failParentOnFailure`, `stackTraceLimit`, …
   */
  readonly native?: Readonly<Record<string, unknown>>;
}

export interface BatchDispatchOptions {
  /** Deduplicates concurrent dispatches of the same logical batch. Generated when omitted. */
  readonly batchId?: string;
  /** Job dispatched once every job of the batch reached a terminal state. Must be registered on workers. */
  readonly onComplete?: BatchCompletionJob;
  /** Bookkeeping retention in seconds (default 7 days). */
  readonly ttlSeconds?: number;
}

export type { BatchRecord, BatchState };

/** Result summary of a finished or in-flight batch. */
export interface BatchStatus {
  readonly id: string;
  readonly state: BatchState;
  readonly total: number;
  readonly completed: number;
  readonly failed: number;
  readonly createdAt: string | null;
  readonly finishedAt: string | null;
}

/** Default bookkeeping retention: one week. */
export const DEFAULT_BATCH_TTL_SECONDS = 7 * 24 * 60 * 60;

export function redisConnection(url: string): ConnectionOptions {
  const parsed = new URL(url);
  return {
    host: parsed.hostname,
    port: Number(parsed.port || 6379),
    ...(parsed.password ? { password: decodeURIComponent(parsed.password) } : {}),
    ...(parsed.username ? { username: decodeURIComponent(parsed.username) } : {}),
    db: parsed.pathname.length > 1 ? Number(parsed.pathname.slice(1)) : 0,
    maxRetriesPerRequest: null,
  };
}

/** Dispatches validated jobs and inspects them. Workers run in a separate process. */
export class QueueManager {
  /** BullMQ handle for callers that program the queue directly (schedulers, teardown). See {@link QueueKitAdapter.queue}. */
  readonly queue: Queue<JobEnvelope, JsonValue>;
  readonly connection: ConnectionOptions;
  /** Dedicated bookkeeping connection for batch state (full Redis command surface). */
  readonly redis: Redis;
  readonly batches: BatchRegistry;
  /**
   * Queue transport (queuekit adapter). Package-internal seam: `JobWorker`
   * creates its consumer through it. Not re-exported from the package root.
   */
  readonly transport: QueueKitAdapter;

  constructor(readonly options: QueueOptions) {
    this.connection = { ...redisConnection(options.url), ...(options.connection ?? {}) };
    // queuekit owns the enqueue transport and provider lifecycle; job
    // contracts, scopes and batches stay framework-owned (see adapter docs).
    this.transport = new QueueKitAdapter({
      connection: this.connection,
      prefix: options.prefix,
      queue: options.queue ?? 'default',
      ...(options.defaultJobOptions !== undefined ? { defaultJobOptions: options.defaultJobOptions } : {}),
      ...(options.limiter !== undefined ? { limiter: options.limiter } : {}),
      ...(options.native !== undefined ? { native: options.native } : {}),
    });
    this.queue = this.transport.queue;
    this.redis = new Redis({ ...this.connection, maxRetriesPerRequest: null, lazyConnect: false } as RedisOptions);
    this.batches = new BatchRegistry(this.redis, options.prefix);
  }

  /**
   * Validates the payload before enqueueing (invalid payloads never reach
   * Redis). The idempotency key, if defined, becomes the job id so duplicate
   * dispatches collapse into one job.
   */
  async dispatch<TPayload extends JsonObject, TResult extends JsonValue>(
    definition: JobDefinition<TPayload, TResult>,
    payload: TPayload,
    context: ExecutionContext,
    options: DispatchOptions = {},
  ): Promise<{ readonly id: string }> {
    const result = definition.payload.safeParse(payload);
    if (!result.success) {
      throw new FrameworkError(
        'JOB_PAYLOAD_INVALID',
        `Invalid payload for job "${definition.name}": ${result.error.message}`,
      );
    }
    return this.#enqueue(definition, result.data, context, options);
  }

  /**
   * Fans out one job per payload and tracks them as a single batch. All
   * payloads are validated before anything reaches Redis. When the last job
   * reaches a terminal state, the (optional) completion job is dispatched.
   */
  async dispatchBatch<TPayload extends JsonObject, TResult extends JsonValue>(
    definition: JobDefinition<TPayload, TResult>,
    payloads: readonly TPayload[],
    context: ExecutionContext,
    options: BatchDispatchOptions = {},
  ): Promise<{ readonly batchId: string; readonly jobIds: readonly string[] }> {
    if (payloads.length === 0) {
      throw new FrameworkError('BATCH_EMPTY', `Batch for job "${definition.name}" has no payloads`);
    }
    const validated: TPayload[] = payloads.map((payload) => {
      const result = definition.payload.safeParse(payload);
      if (!result.success) {
        throw new FrameworkError(
          'JOB_PAYLOAD_INVALID',
          `Invalid payload for job "${definition.name}": ${result.error.message}`,
        );
      }
      return result.data;
    });
    const batchId = options.batchId ?? randomUUID();
    const created = await this.batches.create(
      batchId,
      validated.length,
      options.onComplete ? JSON.stringify(options.onComplete) : null,
      options.ttlSeconds ?? DEFAULT_BATCH_TTL_SECONDS,
    );
    if (!created) {
      throw new FrameworkError('BATCH_EXISTS', `Batch "${batchId}" already exists`);
    }
    const jobIds: string[] = [];
    for (const payload of validated) {
      const { id } = await this.#enqueue(definition, payload, context, { batchId });
      jobIds.push(id);
    }
    return { batchId, jobIds };
  }

  async #enqueue<TPayload extends JsonObject, TResult extends JsonValue>(
    definition: JobDefinition<TPayload, TResult>,
    payload: TPayload,
    context: ExecutionContext,
    options: DispatchOptions & { readonly batchId?: string },
  ): Promise<{ readonly id: string }> {
    const envelope: JobEnvelope = {
      v: definition.version,
      payload,
      actor: context.actor,
      correlationId: context.correlationId,
      ...(options.batchId ? { batchId: options.batchId } : {}),
    };
    // Transport goes through queuekit; the idempotency key (when defined)
    // becomes the BullMQ job id so duplicate dispatches collapse into one job.
    return this.transport.enqueue(definition.name, envelope, {
      jobId: options.jobId ?? definition.idempotencyKey?.(payload),
      delayMs: options.delayMs,
      attempts: options.attempts ?? definition.attempts ?? 3,
      backoffMs: definition.backoffMs ?? 1_000,
      keepCompleted: this.options.keepCompleted ?? 1_000,
      keepFailed: this.options.keepFailed ?? 5_000,
      ...(options.priority !== undefined ? { priority: options.priority } : {}),
      ...(options.native !== undefined ? { native: options.native } : {}),
    });
  }

  async batchStatus(batchId: string): Promise<BatchStatus | null> {
    const record = await this.batches.get(batchId);
    return record ? { ...record } : null;
  }

  async batchFailures(batchId: string, limit = 50): Promise<JobStatus[]> {
    const ids = await this.batches.failedIds(batchId, limit);
    return (await Promise.all(ids.map(async (id) => (await this.status(id)) ?? null))).filter(
      (s): s is JobStatus => s !== null,
    );
  }

  /** Marks a running batch cancelled; pending jobs fail fast with an unrecoverable error. */
  async cancelBatch(batchId: string): Promise<boolean> {
    return this.batches.cancel(batchId);
  }

  /**
   * Records one terminal job of a batch and, when it was the last, dispatches
   * the stored completion job. Called by the worker, not by producers.
   */
  async recordBatchTerminal(batchId: string, failed: boolean, jobId: string): Promise<void> {
    const finished = await this.batches.recordTerminal(batchId, failed, jobId);
    if (!finished) return;
    const onComplete = await this.batches.takeOnComplete(batchId);
    if (!onComplete) return;
    const parsed = JSON.parse(onComplete) as BatchCompletionJob;
    await this.transport.enqueue(
      parsed.name,
      { v: parsed.version, payload: parsed.payload, actor: { id: null, type: 'system' }, correlationId: batchId },
      {
        jobId: `batch-${batchId}-complete`,
        attempts: parsed.attempts ?? 3,
        backoffMs: 1_000,
        keepCompleted: this.options.keepCompleted ?? 1_000,
        keepFailed: this.options.keepFailed ?? 5_000,
      },
    );
  }

  async status(id: string): Promise<JobStatus | null> {
    return this.transport.status(id);
  }

  async failed(limit = 50, offset = 0): Promise<JobStatus[]> {
    return this.transport.failed(limit, offset);
  }

  /** Re-queues a failed job; payload (and thus idempotency) is unchanged. */
  async retry(id: string): Promise<boolean> {
    return this.transport.retry(id);
  }

  /** Removes one failed-job record; false when it no longer exists. */
  async forget(id: string): Promise<boolean> {
    return this.transport.forget(id);
  }

  /** Re-queues up to `limit` failed jobs oldest-first; returns how many were replayed. */
  async retryAll(limit = 100): Promise<number> {
    return this.transport.retryAll(limit);
  }

  /** Removes failed/completed records older than `graceMs`; returns how many were removed. */
  async prune(graceMs: number): Promise<number> {
    return this.transport.prune(graceMs);
  }

  async counts(): Promise<Readonly<Record<string, number>>> {
    return this.transport.counts();
  }

  // Inspection/pause have no queuekit surface (see adapter docs); the
  // transport's direct BullMQ handle serves them.
  pause(): Promise<void> {
    return this.transport.pause();
  }

  resume(): Promise<void> {
    return this.transport.resume();
  }

  async close(): Promise<void> {
    // Closes queuekit-owned resources (provider and its queue clients) and
    // the framework's direct queue handle.
    await this.transport.close();
    await this.redis.quit();
  }
}

export const QUEUE = token<QueueManager>('queue.manager');
