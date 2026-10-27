// Public API of @framework/queue.
// Convention: every interface/type lives in `./types`, every constant in
// `./constants`; runtime values (classes, functions) stay in their modules.

export { BatchRegistry, batchFailedKey, batchKey } from './batch';
export { DEFAULT_TIMEOUT_MS, QUEUE } from './constants';
export { defineJob, erase } from './job';
export { DEFAULT_BATCH_TTL_SECONDS, jobStatusSchema, QueueManager } from './manager';

// Grouped conventions: every type in ./types, every constant in ./constants.
export type {
  AnyJobDefinition,
  BatchCompletionJob,
  BatchDispatchOptions,
  BatchRecord,
  BatchState,
  BatchStatus,
  DispatchOptions,
  JobContext,
  JobDefinition,
  JobEnvelope,
  JobList,
  JobState,
  JobStatus,
  QueueOptions,
  WorkerOptions,
} from './types';
export { JobWorker } from './worker';
