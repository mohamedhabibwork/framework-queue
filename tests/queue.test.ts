import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { Application, type ExecutionContext as Ctx, EXECUTION_CONTEXT, ExecutionContext } from '@framework/core';
import { z } from 'zod/v4';
import {
  type BatchStatus,
  createQueueProvider,
  defineJob,
  type JobStatus,
  JobWorker,
  QUEUE,
  QueueKitAdapter,
  QueueManager,
} from '../src';

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) throw new Error('REDIS_URL is not set; run `bun run db:up` and see .env.test.example');

const context = new ExecutionContext('request', 'corr-q', { id: 'u-q', type: 'user' });
const seenContexts: Ctx[] = [];
let flakyCalls = 0;

const double = defineJob({
  name: 'math.double',
  version: 1,
  payload: z.object({ value: z.number().int() }),
  idempotencyKey: (p) => `double-${p.value}`,
  async handle(payload, ctx) {
    seenContexts.push(ctx.scope.resolve(EXECUTION_CONTEXT));
    await ctx.progress(100);
    return { doubled: payload.value * 2 };
  },
});

const flaky = defineJob({
  name: 'math.flaky',
  version: 1,
  payload: z.object({ failTimes: z.number().int() }),
  attempts: 3,
  // Generous backoff: coverage instrumentation slows the whole suite, and the
  // retry semantics under test must not depend on scheduler precision.
  backoffMs: 50,
  async handle(payload) {
    flakyCalls += 1;
    if (flakyCalls <= payload.failTimes) throw new Error(`transient failure ${flakyCalls}`);
    return { calls: flakyCalls };
  },
});

const broken = defineJob({
  name: 'math.broken',
  version: 1,
  payload: z.object({}),
  attempts: 2,
  backoffMs: 50,
  async handle() {
    throw new Error('always fails');
  },
});

const slow = defineJob({
  name: 'math.slow',
  version: 1,
  payload: z.object({}),
  attempts: 1,
  timeoutMs: 200,
  async handle() {
    await Bun.sleep(2000);
    return null;
  },
});

const flakyBatchEntry = defineJob({
  name: 'math.batch-flaky',
  version: 1,
  payload: z.object({ failOnce: z.boolean() }),
  attempts: 2,
  backoffMs: 50,
  async handle(payload) {
    if (payload.failOnce && flakyBatchCalls === 0) {
      flakyBatchCalls += 1;
      throw new Error('transient batch failure');
    }
    return null;
  },
});
let flakyBatchCalls = 0;

const summarize = defineJob({
  name: 'math.batch-summary',
  version: 1,
  payload: z.object({ batchId: z.string(), total: z.number().int() }),
  async handle(payload) {
    summaries.push(payload);
    return payload;
  },
});
const summaries: { batchId: string; total: number }[] = [];
const prefix = `test-${crypto.randomUUID()}`;
const manager = new QueueManager({ url: redisUrl, prefix, queue: 'tests' });
const application = new Application({ config: {}, providers: [] });
let worker: JobWorker;

async function settle(id: string, timeoutMs = 15_000): Promise<JobStatus> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const status = await manager.status(id);
    if (status && (status.state === 'completed' || status.state === 'failed')) return status;
    await Bun.sleep(25);
  }
  throw new Error(`job ${id} did not settle`);
}

async function settleBatch(batchId: string, timeoutMs = 15_000): Promise<BatchStatus> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const batch = await manager.batchStatus(batchId);
    if (batch && (batch.state === 'completed' || batch.state === 'partially-failed' || batch.state === 'cancelled')) {
      return batch;
    }
    await Bun.sleep(25);
  }
  throw new Error(`batch ${batchId} did not settle`);
}

beforeAll(async () => {
  await application.boot();
  worker = new JobWorker(manager, application, [double, flaky, broken, slow, flakyBatchEntry, summarize], {
    concurrency: 2,
  });
  worker.start();
});

afterAll(async () => {
  await worker.close();
  await manager.queue.obliterate({ force: true });
  await manager.close();
  await application.shutdown();
});

describe('Queue (BullMQ on real Redis)', () => {
  test('runs a job in its own scope with the dispatcher actor and correlation id', async () => {
    const { id } = await manager.dispatch(double, { value: 21 }, context);
    const status = await settle(id);
    expect(status).toMatchObject({ state: 'completed', result: { doubled: 42 }, progress: 100 });
    expect(seenContexts.at(-1)).toMatchObject({
      kind: 'job',
      correlationId: 'corr-q',
      actor: { id: 'u-q', type: 'user' },
    });
  });

  test('idempotency key collapses duplicate dispatches into one job', async () => {
    const [a, b] = await Promise.all([
      manager.dispatch(double, { value: 5 }, context),
      manager.dispatch(double, { value: 5 }, context),
    ]);
    expect(a.id).toBe(b.id);
  });

  test('invalid payloads are rejected before reaching Redis', async () => {
    await expect(manager.dispatch(double, { value: 1.5 }, context)).rejects.toThrow(/Invalid payload/);
  });

  test('transient failures are retried until success', async () => {
    const { id } = await manager.dispatch(flaky, { failTimes: 2 }, context);
    expect(await settle(id)).toMatchObject({ state: 'completed', result: { calls: 3 }, attempts: 3 });
  });

  test('exhausted retries end failed and can be inspected and retried', async () => {
    const { id } = await manager.dispatch(broken, {}, context);
    const status = await settle(id);
    expect(status).toMatchObject({ state: 'failed', failedReason: 'always fails', attempts: 2 });
    expect((await manager.failed()).map((s) => s.id)).toContain(id);
    expect(await manager.retry(id)).toBe(true);
  });

  test('jobs exceeding their timeout fail', async () => {
    const { id } = await manager.dispatch(slow, {}, context);
    expect(await settle(id)).toMatchObject({ state: 'failed', failedReason: 'Job timed out after 200ms' });
  });
});

describe('Queue batches (BullMQ on real Redis)', () => {
  test('completes a batch and fires the completion job', async () => {
    const batchId = `sum-${crypto.randomUUID()}`;
    const { batchId: createdId } = await manager.dispatchBatch(
      double,
      [{ value: 1 }, { value: 2 }, { value: 3 }],
      context,
      { batchId, onComplete: { name: 'math.batch-summary', version: 1, payload: { batchId, total: 3 } } },
    );
    expect(createdId).toBe(batchId);
    const batch = await settleBatch(batchId);
    expect(batch).toMatchObject({ state: 'completed', total: 3, completed: 3, failed: 0 });
    const deadline = Date.now() + 5_000;
    while (summaries.length === 0 && Date.now() < deadline) await Bun.sleep(25);
    expect(summaries[summaries.length - 1]).toMatchObject({ batchId, total: 3 });
  });

  test('tracks partial failure and lists the failed jobs', async () => {
    const { batchId, jobIds } = await manager.dispatchBatch(broken, [{}, {}], context);
    const batch = await settleBatch(batchId);
    expect(batch).toMatchObject({ state: 'partially-failed', total: 2, failed: 2 });
    const failures = await manager.batchFailures(batchId);
    expect(failures.map((s) => s.id).sort()).toEqual([...jobIds].sort());
  });

  test('retried jobs only count once toward the batch', async () => {
    const { batchId } = await manager.dispatchBatch(
      flakyBatchEntry,
      [{ failOnce: true }, { failOnce: false }],
      context,
    );
    const batch = await settleBatch(batchId);
    expect(batch).toMatchObject({ state: 'completed', total: 2, completed: 2, failed: 0 });
  });

  test('cancelling a batch fails the remaining jobs', async () => {
    const { batchId, jobIds } = await manager.dispatchBatch(slow, [{}, {}], context);
    expect(await manager.cancelBatch(batchId)).toBe(true);
    const batch = await settleBatch(batchId, 10_000);
    expect(batch.state).toBe('cancelled');
    expect(jobIds.length).toBe(2);
  });

  test('duplicate batch ids are rejected', async () => {
    const batchId = `dup-${crypto.randomUUID()}`;
    await manager.dispatchBatch(double, [{ value: 1 }], context, { batchId });
    await expect(manager.dispatchBatch(double, [{ value: 2 }], context, { batchId })).rejects.toThrow(/already exists/);
  });

  test('createQueueProvider binds QUEUE backed by QueueKitAdapter and closes cleanly', async () => {
    const providerPrefix = `prov-${crypto.randomUUID()}`;
    type QueueAppConfig = { queue: { url: string; prefix: string; queue: string } };
    const app = new Application<QueueAppConfig>({
      config: { queue: { url: redisUrl, prefix: providerPrefix, queue: 'tests' } },
      providers: [createQueueProvider<QueueAppConfig>((cfg) => cfg.queue)],
    });
    await app.boot();
    try {
      const bound = app.container.resolve(QUEUE);
      expect(bound).toBeInstanceOf(QueueManager);
      expect(bound.transport).toBeInstanceOf(QueueKitAdapter);
      const counts = await bound.counts();
      expect(typeof counts.waiting).toBe('number');
    } finally {
      await app.shutdown();
    }
  });
});
