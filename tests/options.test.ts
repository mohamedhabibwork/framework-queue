import { afterAll, describe, expect, test } from 'bun:test';
import { ExecutionContext } from '@framework/core';
import { z } from 'zod/v4';
import { defineJob, QueueManager } from '../src';

/**
 * Queue option surface: connection extras, default job options, limiter and the
 * per-dispatch `priority` / `native` escape hatch. Assertions read the job's
 * stored `opts`, so an option that never reached BullMQ fails here.
 */

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) throw new Error('REDIS_URL is not set; run `bun run db:up` and see .env.test.example');

const prefix = `test-queue-options-${crypto.randomUUID()}`;
const context = new ExecutionContext('request', 'corr-opts', { id: 'u-opts', type: 'user' });

const echo = defineJob({
  name: 'options.echo',
  version: 1,
  payload: z.object({ value: z.string() }),
  async handle(payload) {
    return { value: payload.value };
  },
});

const manager = new QueueManager({
  url: redisUrl,
  prefix,
  queue: 'options',
  // Connection extras merged over the URL.
  connection: { db: 0 },
  // Applied to every dispatch unless the framework or the caller overrides it.
  defaultJobOptions: { stackTraceLimit: 3 },
  limiter: { max: 50, duration: 1_000 },
  native: { skipVersionCheck: true },
});

afterAll(async () => {
  await manager.queue.obliterate({ force: true }).catch(() => undefined);
  await manager.close();
});

describe('dispatch options', () => {
  test('priority and native jobs options reach BullMQ', async () => {
    const { id } = await manager.dispatch(echo, { value: 'hi' }, context, {
      priority: 5,
      native: { stackTraceLimit: 9, failParentOnFailure: true },
    });
    const job = await manager.queue.getJob(id);
    expect(job?.opts.priority).toBe(5);
    // Per-dispatch native wins over defaultJobOptions.
    expect(job?.opts.stackTraceLimit).toBe(9);
    expect(job?.opts.failParentOnFailure).toBe(true);
  });

  test('defaultJobOptions apply when the dispatch does not override them', async () => {
    const { id } = await manager.dispatch(echo, { value: 'defaults' }, context);
    const job = await manager.queue.getJob(id);
    expect(job?.opts.stackTraceLimit).toBe(3);
    // Framework-owned policy still wins for the keys it manages.
    expect(job?.opts.attempts).toBe(3);
    expect(job?.opts.removeOnComplete).toEqual({ count: 1_000 });
  });

  test('the queue carries the configured limiter and native queue options', async () => {
    // BullMQ exposes the limiter on the queue's meta; asserting it here proves
    // the constructor options were forwarded rather than dropped.
    const queueOptions = manager.queue.opts as { limiter?: unknown; skipVersionCheck?: boolean };
    expect(queueOptions.limiter).toEqual({ max: 50, duration: 1_000 });
    expect(queueOptions.skipVersionCheck).toBe(true);
  });

  test('connection extras are merged over the parsed URL', async () => {
    // `ConnectionOptions` is a union in BullMQ (options object or client
    // instance), so the assertion narrows to the object form it is built from.
    const connection = manager.connection as { db?: number; maxRetriesPerRequest?: number | null };
    expect(connection.db).toBe(0);
    expect(connection.maxRetriesPerRequest).toBeNull();
  });
});
