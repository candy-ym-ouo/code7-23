import { MEDIA_STATUSES, type MediaStatus } from "./types";

/**
 * Legal lifecycle edges for a media asset.
 *
 * The happy path is quarantined -> processing -> scanning -> processing ->
 * (manual_review | ready). Failures collapse any in-flight state to `failed`,
 * and the maintenance sweep recovers rows that died while `scanning` or
 * `processing` (worker timeout / crash) back to `processing`.
 *
 * Moderation owns the edges into/out of `rejected`; the worker only reads that
 * state and may retry a rejected row via the API retry endpoint.
 */
const TRANSITIONS: Record<MediaStatus, ReadonlySet<MediaStatus>> = {
  quarantined: new Set(["processing", "deleted", "failed"]),
  scanning: new Set(["processing", "failed", "deleted"]),
  processing: new Set(["scanning", "manual_review", "ready", "failed", "deleted"]),
  manual_review: new Set(["ready", "rejected", "deleted", "failed"]),
  ready: new Set(["deleted", "processing"]),
  rejected: new Set(["deleted", "processing"]),
  failed: new Set(["scanning", "deleted"]),
  deleted: new Set<MediaStatus>()
};

export function isMediaStatus(value: string): value is MediaStatus {
  return (MEDIA_STATUSES as readonly string[]).includes(value);
}

/**
 * Whether the worker is allowed to move a media row from `from` to `to`.
 * `from === to` is always allowed: it models a harmless re-entrant replay that
 * converges without changing state (e.g. a duplicate BullMQ delivery).
 */
export function canTransition(from: MediaStatus, to: MediaStatus): boolean {
  if (from === to) return true;
  return TRANSITIONS[from]?.has(to) ?? false;
}

/** Throws when an edge is not part of the lifecycle contract. */
export function assertTransition(from: MediaStatus, to: MediaStatus): void {
  if (!canTransition(from, to)) {
    throw new Error(`Illegal media status transition: ${from} -> ${to}`);
  }
}

/** A state from which no further automatic progress is possible without an external action. */
export function isTerminal(status: MediaStatus): boolean {
  return status === "manual_review" || status === "ready" || status === "rejected" || status === "deleted";
}

/** States the maintenance sweep is allowed to recover after a suspected worker crash. */
export function isRecoverableInFlight(status: MediaStatus): boolean {
  return status === "scanning" || status === "processing";
}
