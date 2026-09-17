import { AsyncLocalStorage } from "node:async_hooks";

import { ReadConsistencyContextError } from "./errors.js";

export const DEFAULT_PIN_TO_WRITER_MS = 5_000;

interface PinState {
  pinnedToWriterUntil: number;
  reason?: string;
}

interface ConsistencyContext {
  pins: Map<object, PinState>;
}

const storage = new AsyncLocalStorage<ConsistencyContext>();

export function runWithReadConsistency<T>(fn: () => T | Promise<T>): T | Promise<T> {
  const parent = storage.getStore();
  return storage.run({ pins: new Map(parent?.pins) }, fn);
}

export function pinReadsToWriter(
  token: object,
  options: { ttlMs?: number; reason?: string } | undefined,
  defaultTtlMs = DEFAULT_PIN_TO_WRITER_MS,
): void {
  const context = storage.getStore();

  if (!context) {
    throw new ReadConsistencyContextError("pinReadsToWriter() must be called inside runWithReadConsistency().");
  }

  const ttlMs = Math.max(0, options?.ttlMs ?? defaultTtlMs);
  context.pins.set(token, {
    pinnedToWriterUntil: Date.now() + ttlMs,
    reason: options?.reason,
  });
}

export function isPinnedToWriter(token: object): boolean {
  const context = storage.getStore();
  const state = context?.pins.get(token);

  if (!context || !state) {
    return false;
  }

  if (state.pinnedToWriterUntil <= Date.now()) {
    context.pins.delete(token);
    return false;
  }

  return true;
}
