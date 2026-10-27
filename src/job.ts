import type { Actor, ExecutionContext, JsonObject, JsonValue, Scope } from '@framework/core';
import type { z } from 'zod/v4';

/** What a handler receives besides its payload. Runs inside its own `job` scope. */
export interface JobContext {
  readonly jobId: string;
  readonly attempt: number;
  readonly scope: Scope;
  readonly execution: ExecutionContext;
  readonly signal: AbortSignal;
  progress(percent: number): Promise<void>;
}

/**
 * A versioned, validated job. Handlers must be idempotent: BullMQ delivers
 * at least once, and `idempotencyKey` deduplicates concurrent dispatches.
 */
export interface JobDefinition<TPayload extends JsonObject, TResult extends JsonValue> {
  readonly name: string;
  readonly version: number;
  readonly payload: z.ZodType<TPayload>;
  readonly attempts?: number;
  readonly backoffMs?: number;
  readonly timeoutMs?: number;
  readonly idempotencyKey?: (payload: TPayload) => string;
  handle(payload: TPayload, context: JobContext): Promise<TResult>;
}

/** Stored job data: payload plus the dispatcher's identity for audit/correlation. */
export interface JobEnvelope {
  readonly v: number;
  readonly payload: JsonObject;
  readonly actor: Actor;
  readonly correlationId: string;
  /** Set when the job belongs to a dispatch batch (`dispatchBatch`). */
  readonly batchId?: string;
}

export const defineJob = <TPayload extends JsonObject, TResult extends JsonValue>(
  definition: JobDefinition<TPayload, TResult>,
): JobDefinition<TPayload, TResult> => definition;

/** Type-erased view used by registries that hold many job kinds. */
export interface AnyJobDefinition {
  readonly name: string;
  readonly version: number;
  readonly attempts?: number;
  readonly backoffMs?: number;
  readonly timeoutMs?: number;
  parse(payload: JsonObject): JsonObject;
  run(payload: JsonObject, context: JobContext): Promise<JsonValue>;
}

export function erase<TPayload extends JsonObject, TResult extends JsonValue>(
  definition: JobDefinition<TPayload, TResult>,
): AnyJobDefinition {
  return {
    name: definition.name,
    version: definition.version,
    attempts: definition.attempts,
    backoffMs: definition.backoffMs,
    timeoutMs: definition.timeoutMs,
    parse: (payload) => definition.payload.parse(payload),
    run: (payload, context) => definition.handle(definition.payload.parse(payload), context),
  };
}
