import { processMediaWith } from "./processor";
import {
  FakeMediaTable,
  FakeObjectStore,
  PUBLIC_BUCKET,
  type Fault
} from "./fakes";

/**
 * Queue-level harness modelling BullMQ's at-least-once delivery and the
 * maintenance sweep on top of the deterministic fakes.
 */

export type StepKind =
  | "deliver"
  | "deliverCrashInScan"
  | "recover"
  | "losePublic"
  | "loseQuarantine"
  | "reconcile";

export type Step =
  | { kind: "deliver"; mediaId: string; duplicate?: boolean }
  | { kind: "deliverCrashInScan"; mediaId: string }
  | { kind: "recover"; mediaId: string }
  | { kind: "losePublic"; mediaId: string }
  | { kind: "loseQuarantine"; mediaId: string }
  | { kind: "reconcile"; mediaId: string };

export type HarnessOptions = {
  autoPublish: boolean;
  faults?: Fault[];
};

export class MediaHarness {
  readonly table = new FakeMediaTable();
  readonly store = new FakeObjectStore();

  constructor(private readonly options: HarnessOptions) {}

  seed(mediaId: string, source: Buffer, status: "processing" | "failed" = "processing"): void {
    this.table.insert(mediaId, status);
    const key = this.table.get(mediaId)!.quarantine_object_key;
    this.store.put("quarantine", key, source);
  }

  /** Seed a row that already completed processing and was published. */
  seedReady(mediaId: string, source: Buffer, sha256: string): void {
    this.seed(mediaId, source, "processing");
    const { processed, thumbnail, public: pub, publicThumb } = this.keys(mediaId);
    this.store.put("quarantine", processed, Buffer.from("derived"));
    this.store.put("quarantine", thumbnail, Buffer.from("thumb"));
    this.store.put(PUBLIC_BUCKET, pub, Buffer.from("derived"));
    this.store.put(PUBLIC_BUCKET, publicThumb, Buffer.from("thumb"));
    const row = this.table.get(mediaId)!;
    row.privacy_status = "ready";
    row.processed_object_key = processed;
    row.thumbnail_object_key = thumbnail;
    row.public_object_key = pub;
    row.public_thumbnail_object_key = publicThumb;
    row.sha256 = sha256;
    this.table.audits.push({
      action: "media.processing_succeeded",
      resourceType: "media",
      resourceId: mediaId,
      metadata: { status: "ready", autoPublish: true, sha256 },
      attempt: 0,
      at: 0
    });
  }

  /** One BullMQ-style delivery. Rejections are returned, not thrown. */
  async deliver(mediaId: string): Promise<{ status: string; error?: string }> {
    const deps = this.table.buildDeps({
      mediaId,
      store: this.store,
      ...(this.options.faults ? { faults: this.options.faults } : {}),
      autoPublish: this.options.autoPublish
    });
    try {
      const status = await processMediaWith(deps, mediaId);
      return { status: String(status) };
    } catch (error) {
      return { status: "threw", error: error instanceof Error ? error.message : String(error) };
    }
  }

  /**
   * A delivery whose worker process is killed after the row entered `scanning`
   * (but before scanMalware resolves). Models SIGKILL/OOM mid-job: the row is
   * left in-flight with neither failure rollback nor audit, exactly as in
   * production, until the maintenance sweep recovers it.
   */
  async deliverCrashInScan(mediaId: string): Promise<void> {
    const deps = this.table.buildDeps({
      mediaId,
      store: this.store,
      ...(this.options.faults ? { faults: this.options.faults } : {}),
      autoPublish: this.options.autoPublish
    });
    await deps.getMedia(mediaId); // bump attempt counter like a real start
    await deps.markScanning(mediaId);
    // process dies here; mark the row stale so a (timeout-delayed) sweep, not
    // an immediate redelivery, recovers it. No fail(), audit, or rollback.
    this.table.simulateCrashInFlight(mediaId, "scanning");
  }

  async step(step: Step): Promise<void> {
    switch (step.kind) {
      case "deliver":
        await this.deliver(step.mediaId);
        return;
      case "recover":
        this.table.recover(step.mediaId);
        return;
      case "deliverCrashInScan":
        await this.deliverCrashInScan(step.mediaId);
        return;
      case "losePublic":
        this.losePublic(step.mediaId);
        return;
      case "loseQuarantine":
        this.loseQuarantine(step.mediaId);
        return;
      case "reconcile":
        this.reconcileLostPublic(step.mediaId);
        return;
    }
  }

  private keys(mediaId: string) {
    return {
      processed: `processed/${mediaId}.webp`,
      thumbnail: `processed/${mediaId}.thumb.webp`,
      public: `media/${mediaId}.webp`,
      publicThumb: `media/${mediaId}.thumb.webp`
    };
  }

  losePublic(mediaId: string): void {
    this.store.lose(PUBLIC_BUCKET, this.keys(mediaId).public);
    this.store.lose(PUBLIC_BUCKET, this.keys(mediaId).publicThumb);
  }

  loseQuarantine(mediaId: string): void {
    this.store.lose("quarantine", this.table.get(mediaId)!.quarantine_object_key);
  }

  /**
   * Consistency repair used by the queue/replay story: when a `ready` row's
   * public object is missing, the system re-derives/re-copies by replaying the
   * processing job (ready -> processing -> ready).
   */
  reconcileLostPublic(mediaId: string): void {
    const row = this.table.get(mediaId)!;
    const publicExists = this.store.has(PUBLIC_BUCKET, this.keys(mediaId).public);
    if (row.privacy_status === "ready" && !publicExists) {
      this.table.markForReconciliation(mediaId);
    }
  }

  /**
   * Drain one media row to a stable point:
   *   - scanning/processing (worker crash) -> maintenance sweep recovers it
   *   - failed (transient fault)           -> job is replayed
   *   - ready with a missing public object -> reconciled, then replayed
   * Stops at manual_review / ready-with-objects / rejected / deleted.
   * Bounded to guard against permanent faults (which would loop forever).
   */
  async runToQuiescence(mediaId: string, maxRounds = 50): Promise<void> {
    for (let round = 0; round < maxRounds; round += 1) {
      const row = this.table.get(mediaId)!;
      const publicExists = this.store.has(PUBLIC_BUCKET, this.keys(mediaId).public);

      if (row.privacy_status === "ready" && !publicExists) {
        this.reconcileLostPublic(mediaId);
        continue;
      }
      if (row.privacy_status === "scanning" || row.privacy_status === "processing") {
        this.table.recover(mediaId);
        await this.deliver(mediaId);
        continue;
      }
      if (row.privacy_status === "failed") {
        await this.deliver(mediaId);
        continue;
      }
      return; // manual_review / ready (complete) / rejected / deleted / quarantined
    }
    throw new Error("runToQuiescence exceeded bound; faults may be permanent");
  }

  async runSteps(steps: Step[]): Promise<void> {
    for (const step of steps) {
      await this.step(step);
    }
  }
}

/** Mulberry32 deterministic PRNG so reordered runs are fully reproducible. */
export function makeRng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Fisher–Yates shuffle using an injectable (seedable) RNG. */
export function shuffle<T>(items: readonly T[], rng: () => number): T[] {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

export type ConsistencySnapshot = {
  status: string;
  failureCode: string | null;
  /** Expected public objects present (only meaningful when auto-publish). */
  publicPresent: boolean;
  /** Derivations exist whenever the row reached a post-processing state. */
  derivationsPresent: boolean;
  /** Distinct sha256 the row ever settled on (must be exactly one). */
  sha256: string | null;
  auditActions: string[];
  auditCount: number;
};

export function snapshot(harness: MediaHarness, mediaId: string): ConsistencySnapshot {
  const row = harness.table.get(mediaId)!;
  const keys = {
    processed: `processed/${mediaId}.webp`,
    thumbnail: `processed/${mediaId}.thumb.webp`,
    public: `media/${mediaId}.webp`,
    publicThumb: `media/${mediaId}.thumb.webp`
  };
  const postProcessed = row.privacy_status === "ready" || row.privacy_status === "manual_review";
  return {
    status: row.privacy_status,
    failureCode: row.failure_code,
    publicPresent:
      harness.store.has(PUBLIC_BUCKET, keys.public) &&
      harness.store.has(PUBLIC_BUCKET, keys.publicThumb),
    derivationsPresent:
      harness.store.has("quarantine", keys.processed) &&
      harness.store.has("quarantine", keys.thumbnail),
    sha256: row.sha256,
    auditActions: harness.table.audits.map((a) => a.action),
    auditCount: harness.table.audits.length,
    ...(postProcessed ? {} : {})
  };
}
