import { randomBytes, createHash } from "node:crypto";
import type { PrivacyRegion } from "@map/shared/contracts";
import {
  type FinalizeMediaInput,
  type FailMediaInput,
  type MediaAuditEvent,
  type MediaProcessorDeps,
  type MediaRow,
  type MediaStatus,
  type ProcessedImage
} from "./types";
import { canTransition } from "./state-machine";

/**
 * Deterministic in-memory fakes for fault-injection testing.
 *
 * They implement the exact MediaProcessorDeps surface the production worker
 * uses, so processMediaWith() runs real orchestration logic against them.
 * They enforce the same invariants production relies on: state-machine
 * legality, atomic (status + audit) commits and content-derived object keys.
 */

export type FaultPoint =
  | "markScanning"
  | "readQuarantine"
  | "scanMalware"
  | "markProcessingAfterScan"
  | "processImage"
  | "writeProcessed"
  | "writeThumbnail"
  | "copyPublic"
  | "copyThumbnail"
  | "finalize"
  | "fail";

export type Fault = {
  point: FaultPoint;
  /** Error surfaced to the orchestrator (drives failure_code / audit metadata). */
  message: string;
  /** Fire only on the n-th processing attempt (1-based) of the configured media. */
  attempt?: number;
};

export const QUARANTINE_BUCKET = "quarantine";
export const PUBLIC_BUCKET = "public";

/** In-memory S3 stand-in with separate quarantine/public buckets. */
export class FakeObjectStore {
  readonly objects = new Map<string, Buffer>();

  private key(bucket: string, key: string): string {
    return `${bucket}/${key}`;
  }

  put(bucket: string, key: string, body: Buffer): void {
    this.objects.set(this.key(bucket, key), body);
  }

  has(bucket: string, key: string): boolean {
    return this.objects.has(this.key(bucket, key));
  }

  get(bucket: string, key: string): Buffer | undefined {
    return this.objects.get(this.key(bucket, key));
  }

  delete(bucket: string, key: string): void {
    this.objects.delete(this.key(bucket, key));
  }

  /** Simulate a lost object (S3 NoSuchKey) even if DB metadata points at it. */
  lose(bucket: string, key: string): void {
    this.objects.delete(this.key(bucket, key));
  }
}

export type RecordedAudit = MediaAuditEvent & { attempt: number; at: number };

/**
 * In-memory media table. Every mutating port validates the state-machine edge
 * before applying it, and finalize/fail commit status + audit atomically.
 */
export class FakeMediaTable {
  readonly rows = new Map<string, MediaRow>();
  readonly audits: RecordedAudit[] = [];
  /** Observed (from,to) edges in order. */
  readonly transitions: Array<{ id: string; from: MediaStatus; to: MediaStatus }> = [];
  private attempts = new Map<string, number>();
  private clock = 0;
  private illegalEdges: string[] = [];
  /** Rows that died in flight past the sweep timeout; only these are recoverable. */
  private stale = new Set<string>();

  insert(id: string, status: MediaStatus = "processing", privacyReport: MediaRow["privacy_report"] = null): MediaRow {
    const row: MediaRow = {
      id,
      privacy_status: status,
      quarantine_object_key: `quarantine/owner/${id}.jpg`,
      processed_object_key: null,
      thumbnail_object_key: null,
      public_object_key: null,
      public_thumbnail_object_key: null,
      width: null,
      height: null,
      sha256: null,
      perceptual_hash: null,
      privacy_report: privacyReport,
      failure_code: null
    };
    this.rows.set(id, row);
    return row;
  }

  attemptFor(id: string): number {
    return this.attempts.get(id) ?? 0;
  }

  get(id: string): MediaRow | null {
    return this.rows.get(id) ?? null;
  }

  /**
   * Test-only: emulate a worker crash leaving the row in an in-flight state
   * with no corresponding completed effects (e.g. died during scanning). The
   * row is marked stale so only a (timeout-delayed) sweep can recover it.
   */
  simulateCrashInFlight(id: string, status: Extract<MediaStatus, "scanning" | "processing">): void {
    const row = this.rows.get(id);
    if (!row) throw new Error("Media record not found");
    row.privacy_status = status;
    this.stale.add(id);
  }

  illegalTransitions(): readonly string[] {
    return this.illegalEdges;
  }

  /** True when no in-flight row remains (nothing the worker could still advance). */
  isQuiescent(): boolean {
    for (const row of this.rows.values()) {
      if (row.privacy_status === "scanning" || row.privacy_status === "processing") return false;
    }
    return true;
  }

  private mutate(id: string, to: MediaStatus, patch: (row: MediaRow) => void): void {
    const row = this.rows.get(id);
    if (!row) throw new Error("Media record not found");
    const from = row.privacy_status;
    if (!canTransition(from, to)) {
      this.illegalEdges.push(`${from} -> ${to}`);
      throw new Error(`Illegal media status transition: ${from} -> ${to}`);
    }
    if (from !== to) this.transitions.push({ id, from, to });
    patch(row);
    row.privacy_status = to;
  }

  private bumpAttempt(id: string): number {
    const next = (this.attempts.get(id) ?? 0) + 1;
    this.attempts.set(id, next);
    return next;
  }

  private recordAudit(event: MediaAuditEvent, attempt: number): void {
    this.clock += 1;
    this.audits.push({ ...event, attempt, at: this.clock });
  }

  /**
   * Maintenance sweep recovery: scanning/processing -> processing with a
   * `media.recovered` audit row. Mirrors recoverStuckMedia() in pg-media.ts.
   */
  recover(id: string, reason = "worker_timeout"): void {
    const row = this.rows.get(id);
    if (!row) throw new Error("Media record not found");
    // The maintenance sweep only recovers rows that died in flight AND have
    // been stuck past the timeout. A fresh, actively-worked `processing` row,
    // a terminal row, or a `failed` row waiting for explicit retry: no-op.
    if (!this.stale.has(id)) return;
    if (row.privacy_status !== "scanning" && row.privacy_status !== "processing") {
      this.stale.delete(id);
      return;
    }
    this.stale.delete(id);
    this.mutate(id, "processing", (target) => {
      target.failure_code = "Recovered after worker timeout";
    });
    this.recordAudit(
      {
        action: "media.recovered",
        resourceType: "media",
        resourceId: id,
        metadata: { reason }
      },
      this.attemptFor(id)
    );
  }

  /**
   * Reconciliation for a `ready` row whose public object vanished: move it
   * back to `processing` so a replay re-copies the derivation. ready ->
   * processing is a legal reconciliation edge in the state machine.
   */
  markForReconciliation(id: string): void {
    this.mutate(id, "processing", () => {});
  }

  buildDeps(options: {
    mediaId: string;
    store: FakeObjectStore;
    faults?: Fault[];
    autoPublish: boolean;
    retentionHours?: number;
    image?: () => Promise<ProcessedImage> | ProcessedImage;
    scan?: (source: Buffer) => Promise<void>;
    now?: () => Date;
  }): MediaProcessorDeps {
    const { store, mediaId } = options;
    const faults = options.faults ?? [];
    const self = this;

    const throwIf = (point: FaultPoint): void => {
      // getMedia() already bumped the counter for this invocation, so the
      // current value is the 1-based replay/attempt number of this run.
      const attempt = self.attemptFor(mediaId);
      for (const fault of faults) {
        if (fault.point !== point) continue;
        if (fault.attempt === undefined || fault.attempt === attempt) {
          throw new Error(fault.message);
        }
      }
    };

    return {
      autoPublish: options.autoPublish,
      retentionHours: options.retentionHours ?? 24,
      now: options.now ?? (() => new Date(Date.UTC(2026, 0, 1) + self.clock * 1000)),
      log: () => undefined,

      getMedia: async (id) => {
        const row = self.get(id);
        // Only a runnable job counts as an attempt; a duplicate delivery of a
        // terminal row is a skip and must not shift fault/attempt alignment.
        if (row && (row.privacy_status === "processing" || row.privacy_status === "failed")) {
          self.bumpAttempt(id);
        }
        return row;
      },
      markScanning: async (id) => {
        throwIf("markScanning");
        // A live attempt is actively progressing, so any previous stale flag
        // (e.g. replay after recovery) is cleared.
        self.stale.delete(id);
        self.mutate(id, "scanning", () => {});
      },
      readQuarantine: async (key) => {
        throwIf("readQuarantine");
        const body = store.get(QUARANTINE_BUCKET, key);
        if (!body) throw new Error("Quarantine object missing (NoSuchKey)");
        return Buffer.from(body);
      },
      scanMalware: async (source) => {
        throwIf("scanMalware");
        if (options.scan) await options.scan(source);
      },
      markProcessingAfterScan: async (id) => {
        throwIf("markProcessingAfterScan");
        self.mutate(id, "processing", () => {});
      },
      processImage: async (source, regions) => {
        throwIf("processImage");
        if (options.image) return options.image();
        return deterministicImage(source, regions);
      },
      writeQuarantine: async (key, body) => {
        if (key.endsWith(".thumb.webp")) throwIf("writeThumbnail");
        else throwIf("writeProcessed");
        store.put(QUARANTINE_BUCKET, key, body);
      },
      copyToPublic: async (processedKey, publicKey) => {
        if (publicKey.endsWith(".thumb.webp")) throwIf("copyThumbnail");
        else throwIf("copyPublic");
        const body = store.get(QUARANTINE_BUCKET, processedKey);
        if (!body) throw new Error("Cannot copy missing source object");
        store.put(PUBLIC_BUCKET, publicKey, Buffer.from(body));
      },
      finalize: async (input: FinalizeMediaInput) => {
        throwIf("finalize");
        self.mutate(input.mediaId, input.status, (row) => {
          row.processed_object_key = input.processedKey;
          row.thumbnail_object_key = input.thumbnailKey;
          row.public_object_key = input.publicKey;
          row.public_thumbnail_object_key = input.publicThumbnailKey;
          row.width = input.width;
          row.height = input.height;
          row.sha256 = input.sha256;
          row.perceptual_hash = input.perceptualHash;
          row.privacy_report = input.report as MediaRow["privacy_report"];
          row.failure_code = null;
        });
        self.recordAudit(input.audit, self.attemptFor(input.mediaId));
      },
      fail: async (input: FailMediaInput) => {
        throwIf("fail");
        self.mutate(input.mediaId, "failed", (row) => {
          row.failure_code = input.failureCode;
        });
        self.recordAudit(input.audit, self.attemptFor(input.mediaId));
      },
      deletePublicObjects: async ({ publicKey, publicThumbnailKey }) => {
        store.delete(PUBLIC_BUCKET, publicKey);
        store.delete(PUBLIC_BUCKET, publicThumbnailKey);
      }
    };
  }
}

/** Deterministic processed image; bytes derive from inputs so replays converge. */
export function deterministicImage(source: Buffer, regions: PrivacyRegion[]): ProcessedImage {
  const regionTag = regions.map((r) => `${r.x},${r.y},${r.width},${r.height}`).join("|");
  const image = Buffer.concat([Buffer.from("webp:"), source.subarray(0, 16), Buffer.from(regionTag)]);
  const thumbnail = Buffer.concat([Buffer.from("thumb:"), image]);
  const hash = createHash("sha256").update(image).digest("hex");
  return {
    image,
    thumbnail,
    width: 320,
    height: 240,
    sha256: hash,
    perceptualHash: hash.slice(0, 16),
    detectorRegions: [],
    manualRegions: regions
  };
}

export function makeSource(bytes = 64): Buffer {
  return randomBytes(bytes);
}
