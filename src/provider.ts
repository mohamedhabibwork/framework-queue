import { ConfigError, defineProvider, type ServiceProvider } from '@framework/core';
import { z } from 'zod/v4';
import { QUEUE } from './constants';
import { QueueManager, type QueueOptions } from './manager';

/** Validated shape of the application's `queue` configuration section. */
export const queueConfigSchema = z.object({
  url: z.string().min(1),
  prefix: z.string().min(1),
  queue: z.string().min(1).default('default'),
  keepCompleted: z.number().int().positive().optional(),
  keepFailed: z.number().int().positive().optional(),
});

/**
 * Registers the `QueueManager` singleton under `QUEUE` and closes its
 * queuekit provider and Redis bookkeeping connection at container shutdown.
 */
export const createQueueProvider = <TConfig extends object>(
  select: (config: TConfig) => QueueOptions,
): ServiceProvider<TConfig> =>
  defineProvider<TConfig>({
    name: 'queue',
    register({ container, config }) {
      const parsed = queueConfigSchema.safeParse(select(config));
      if (!parsed.success) {
        throw new ConfigError(parsed.error.issues.map((issue) => `queue.${issue.path.join('.')}: ${issue.message}`));
      }
      container.singleton(
        QUEUE,
        () => new QueueManager(parsed.data),
        (manager) => manager.close(),
      );
    },
  });
