import {
  type Application,
  EXECUTION_CONTEXT,
  ExecutionContext,
  FrameworkError,
  type JsonValue,
  runWithExecution,
} from '@framework/core';
import type { Job, Worker } from 'bullmq';
import { type AnyJobDefinition, erase, type JobDefinition } from './job';
import type { QueueManager } from './manager';
import { jobEnvelopeOf, type QueueKitJobData, UnrecoverableError } from './queuekit-adapter';

export interface WorkerOptions {
  readonly concurrency?: number;
  /** Default per-job timeout when a definition has none. */
  readonly timeoutMs?: number;
  readonly onFailed?: (job: { id?: string; name: string }, error: Error) => void;
}

const DEFAULT_TIMEOUT_MS = 5 * 60_000;

// Definitions are heterogeneous; erase them to one runtime shape keyed by name.
// oxlint-disable-next-line typescript/no-explicit-any -- accepts definitions with any payload/result types
export type JobList = readonly JobDefinition<any, any>[];

/**
 * Consumes the queue. Each job runs in a fresh `job` scope whose execution
 * context carries the dispatcher's actor and correlation id, so audit rows
 * and events written by the job are attributed correctly.
 */
export class JobWorker {
  readonly #jobs = new Map<string, AnyJobDefinition>();
  readonly #worker: Worker<QueueKitJobData, JsonValue>;

  constructor(
    private readonly manager: QueueManager,
    private readonly application: Application<object>,
    jobs: JobList,
    private readonly options: WorkerOptions = {},
  ) {
    for (const job of jobs) {
      if (this.#jobs.has(job.name)) throw new FrameworkError('DUPLICATE_JOB', `Job "${job.name}" is registered twice`);
      this.#jobs.set(job.name, erase(job));
    }
    // The consumer is created through the manager's queuekit transport
    // adapter, but the processor function stays framework-owned on purpose:
    // queuekit's consumer wrapper discards the processor's return value (the
    // framework's JobStatus.result) and hides the raw worker needed for
    // autorun control and the `failed` hook (see queuekit-adapter docs).
    this.#worker = manager.transport.createWorker((job) => this.#process(job), {
      concurrency: options.concurrency ?? 4,
    });
    this.#worker.on('failed', (job, error) =>
      this.options.onFailed?.({ id: job?.id, name: job?.name ?? 'unknown-job' }, error),
    );
  }

  start(): void {
    void this.#worker.run();
  }

  async #process(job: Job<QueueKitJobData, JsonValue>): Promise<JsonValue> {
    const definition = this.#jobs.get(job.name);
    // Accepts both wire shapes: jobs published through queuekit wrap the
    // envelope, jobs added directly on the queue handle (scheduler fires)
    // carry it raw (see queuekit-adapter).
    const envelope = jobEnvelopeOf(job.data);
    if (!definition) throw new UnrecoverableError(`No handler registered for job "${job.name}"`);
    if (envelope.v !== definition.version) {
      throw new UnrecoverableError(`Job "${job.name}" v${envelope.v} cannot run on handler v${definition.version}`);
    }
    let payload: ReturnType<AnyJobDefinition['parse']>;
    try {
      payload = definition.parse(envelope.payload);
    } catch (error) {
      throw new UnrecoverableError(`Invalid payload: ${error instanceof Error ? error.message : String(error)}`);
    }

    const timeoutMs = definition.timeoutMs ?? this.options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const batchId = envelope.batchId;
    if (batchId) {
      const batchState = await this.manager.batches.state(batchId);
      if (batchState === 'cancelled') {
        throw new UnrecoverableError(`Batch "${batchId}" was cancelled`);
      }
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const scope = this.application.createScope('job');
    const execution = new ExecutionContext('job', envelope.correlationId, envelope.actor);
    scope.override(EXECUTION_CONTEXT, execution);
    try {
      // Ambient execution context for helpers inside the handler's async chain.
      const result = await runWithExecution(execution, () => {
        const run = definition.run(payload, {
          jobId: job.id ?? '',
          attempt: job.attemptsMade + 1,
          scope,
          execution,
          signal: controller.signal,
          progress: (percent) => job.updateProgress(Math.max(0, Math.min(100, Math.round(percent)))),
        });
        const timeout = new Promise<never>((_, reject) =>
          controller.signal.addEventListener('abort', () => reject(new Error(`Job timed out after ${timeoutMs}ms`)), {
            once: true,
          }),
        );
        return Promise.race([run, timeout]);
      });
      if (batchId) await this.manager.recordBatchTerminal(batchId, false, job.id ?? '');
      return result;
    } catch (error) {
      // Only the final attempt counts as a terminal batch failure; BullMQ retries the rest.
      const totalAttempts = definition.attempts ?? 3;
      const willRetry = job.attemptsMade + 1 < totalAttempts;
      if (batchId && !willRetry) await this.manager.recordBatchTerminal(batchId, true, job.id ?? '');
      throw error;
    } finally {
      clearTimeout(timer);
      await scope.dispose();
    }
  }

  /** Stops taking jobs and waits for active ones (bounded by the caller's shutdown deadline). */
  close(): Promise<void> {
    return this.#worker.close();
  }
}
