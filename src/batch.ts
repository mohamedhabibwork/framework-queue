import type { JsonObject } from '@framework/core';
import type { Redis } from 'ioredis';

/** Lifecycle of a dispatch batch, tracked in Redis bookkeeping hashes. */
export type BatchState = 'running' | 'completed' | 'partially-failed' | 'cancelled';

export interface BatchRecord {
  readonly id: string;
  readonly state: BatchState;
  readonly total: number;
  readonly completed: number;
  readonly failed: number;
  readonly createdAt: string | null;
  readonly finishedAt: string | null;
}

export const batchKey = (prefix: string, id: string): string => `${prefix}:batch:${id}`;
export const batchFailedKey = (prefix: string, id: string): string => `${prefix}:batch:${id}:failed`;

/**
 * Redis bookkeeping for job batches: counters, failed ids, terminal states.
 * Counters move with atomic HINCRBY (no WATCH: EXEC from a concurrent
 * recorder would clear the connection-wide watch and lose updates). The
 * batch-finished flag is claimed with HSETNX so exactly one worker wins the
 * terminal transition and fires the completion job.
 */
export class BatchRegistry {
  constructor(
    private readonly redis: Redis,
    private readonly prefix: string,
  ) {}

  /** Creates the batch hash; returns false when the id already exists. */
  async create(id: string, total: number, onComplete: string | null, ttlSeconds: number): Promise<boolean> {
    const key = batchKey(this.prefix, id);
    const created = await this.redis.hsetnx(key, 'total', String(total));
    if (!created) return false;
    const multi = this.redis.multi();
    multi.hset(key, { status: 'running', completed: '0', failed: '0' });
    if (onComplete) multi.hset(key, 'onComplete', onComplete);
    multi.expire(key, ttlSeconds);
    multi.expire(batchFailedKey(this.prefix, id), ttlSeconds);
    await multi.exec();
    return true;
  }

  async state(id: string): Promise<BatchState | null> {
    const status = await this.redis.hget(batchKey(this.prefix, id), 'status');
    return status === 'running' || status === 'completed' || status === 'partially-failed' || status === 'cancelled'
      ? status
      : null;
  }

  /**
   * Records one terminal job of a batch; returns true when this very call
   * made the batch terminal (all jobs completed or failed).
   */
  async recordTerminal(id: string, failed: boolean, jobId: string): Promise<boolean> {
    const key = batchKey(this.prefix, id);
    const failedKey = batchFailedKey(this.prefix, id);
    await this.redis.hincrby(key, failed ? 'failed' : 'completed', 1);
    if (failed) await this.redis.rpush(failedKey, jobId);
    const raw = await this.redis.hgetall(key);
    const done = Number(raw.completed ?? 0) + Number(raw.failed ?? 0) >= Number(raw.total ?? Number.MAX_SAFE_INTEGER);
    if (!done) return false;
    // Exactly one caller claims the terminal transition.
    const won = await this.redis.hsetnx(key, 'finished', '1');
    if (!won) return true;
    await this.redis.hset(key, {
      status: Number(raw.failed ?? 0) > 0 ? 'partially-failed' : 'completed',
      finishedAt: new Date().toISOString(),
    });
    return true;
  }

  /** Reads and clears the stored completion-job reference of a finished batch. */
  async takeOnComplete(id: string): Promise<string | null> {
    const key = batchKey(this.prefix, id);
    const onComplete = await this.redis.hget(key, 'onComplete');
    if (onComplete) await this.redis.hdel(key, 'onComplete');
    return onComplete;
  }

  async get(id: string): Promise<BatchRecord | null> {
    const raw = await this.redis.hgetall(batchKey(this.prefix, id));
    if (!raw || Object.keys(raw).length === 0) return null;
    return {
      id,
      state: (raw.status as BatchState | undefined) ?? 'running',
      total: Number(raw.total ?? 0),
      completed: Number(raw.completed ?? 0),
      failed: Number(raw.failed ?? 0),
      createdAt: raw.createdAt ?? null,
      finishedAt: raw.finishedAt ?? null,
    };
  }

  async failedIds(id: string, limit: number): Promise<string[]> {
    return this.redis.lrange(batchFailedKey(this.prefix, id), 0, limit - 1);
  }

  async cancel(id: string): Promise<boolean> {
    const key = batchKey(this.prefix, id);
    const status = await this.redis.hget(key, 'status');
    if (status !== 'running') return false;
    await this.redis.hset(key, 'status', 'cancelled');
    return true;
  }
}

/** The completion-job reference stored inside a batch hash. */
export interface BatchCompletionJob {
  readonly name: string;
  readonly version: number;
  readonly attempts?: number;
  readonly payload: JsonObject;
}
