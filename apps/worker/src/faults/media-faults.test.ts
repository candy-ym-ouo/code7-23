import { afterAll, beforeAll, describe, expect, it } from "vitest";
import sharp from "sharp";
import {
  ALLOWED_TRANSITIONS,
  FaultHarness,
  canConnect,
  delay,
  seededShuffle,
  type MediaRow,
  type WorkerScanner
} from "./harness";
import { FakeClamServer, type FakeClamMode } from "./fake-clamav";

/**
 * Media failure-injection suite.
 *
 * Covers the four fault classes required by the media pipeline design:
 *   1. scan timeout    — scanner accepts but never replies (real socket timeout)
 *   2. blur failure    — quarantine object is corrupt, Sharp decode fails
 *   3. object missing  — quarantine object is gone (NoSuchKey)
 *   4. queue replay    — replay after failure, duplicate replay, worker crash,
 *                        stuck-job maintenance recovery
 *
 * Every test asserts the state machine (server-side transition trap), the
 * audit trail and eventual consistency. The final blocks re-run the full
 * fault -> recover cycles in seeded step permutations: reordering the steps
 * must still reproduce the same end state, audit set and objects.
 */

const harness = new FaultHarness();

beforeAll(async () => {
  const checks = await Promise.all([
    canConnect(Number(new URL(process.env.DATABASE_URL ?? "postgres://x@localhost:5432/x").port)),
    canConnect(Number(new URL(process.env.REDIS_URL ?? "redis://localhost:6379/0").port)),
    canConnect(Number(new URL(process.env.S3_ENDPOINT ?? "http://localhost:9000").port))
  ]);
  const [dbUp, redisUp, s3Up] = checks;
  if (!dbUp || !redisUp || !s3Up) {
    throw new Error(
      `Fault-injection suite needs real services running (postgres=${dbUp}, redis=${redisUp}, s3=${s3Up})`
    );
  }
  await harness.start();
}, 30_000);

afterAll(async () => {
  await harness.stop();
});

async function legalTransitions(mediaId: string): Promise<void> {
  const hops = await harness.transitionsOf(mediaId);
  for (const hop of hops) {
    const allowed = (ALLOWED_TRANSITIONS as Record<string, readonly string[]>)[hop.from] ?? [];
    expect(allowed, `transition ${hop.from} -> ${hop.to} must be declared in the state machine`).toContain(hop.to);
  }
}

/**
 * Failure assertions shared by scan/blur/missing faults.
 * `originalRetained` is false for the missing-object fault (nothing to retain).
 */
async function assertFailedConsistently(
  mediaId: string,
  failureMatcher: RegExp,
  originalRetained = true
): Promise<MediaRow> {
  const failed = await harness.waitForStatus(mediaId, ["failed"]);
  expect(failed.failure_code).toMatch(failureMatcher);
  // 7-day retention for forensics/retry is set on failure.
  expect(failed.delete_after).not.toBeNull();
  expect(failed.public_object_key).toBeNull();
  expect(failed.public_thumbnail_object_key).toBeNull();
  expect(await harness.objectExists(harness.publicBucket(), `media/${mediaId}.webp`)).toBe(false);
  // Nothing derived was produced.
  expect(failed.processed_object_key).toBeNull();
  if (originalRetained) {
    expect(await harness.objectExists(harness.quarantineBucket(), failed.quarantine_object_key)).toBe(true);
  }
  expect(await harness.auditActions(mediaId)).toContain("media.processing_failed");
  return failed;
}

async function spawnWithScanner(mode: FakeClamMode | "disabled"): Promise<FakeClamServer | null> {
  let scanner: FakeClamServer | null = null;
  let spec: WorkerScanner;
  if (mode === "disabled") {
    spec = { kind: "disabled" };
  } else {
    scanner = new FakeClamServer();
    const port = await scanner.start(mode);
    spec = { kind: "port", port };
  }
  await harness.spawnWorker(spec);
  return scanner;
}

async function stopScanner(scanner: FakeClamServer | null): Promise<void> {
  if (scanner) await scanner.stop();
}

/** Restore the object, switch to a healthy scanner and replay the queue. */
async function replayHealthy(mediaId: string): Promise<{ done: MediaRow; scanner: FakeClamServer }> {
  await harness.restoreValidObject(mediaId);
  await harness.killAllWorkers();
  const scanner = new FakeClamServer();
  const port = await scanner.start("ok");
  await harness.spawnWorker({ kind: "port", port });
  await harness.enqueue(mediaId); // queue replay
  const done = await harness.waitForStatus(mediaId, ["manual_review"], 20_000);
  await harness.waitForQueueDrain(20_000);
  // Scanner must stay up until the job completed; the caller stops it after.
  return { done, scanner };
}

async function assertConverged(done: MediaRow, mediaId: string, expectedAudits: string[]): Promise<void> {
  expect(done.privacy_status).toBe("manual_review");
  expect(done.failure_code).toBeNull();
  expect(done.processed_object_key).toBe(`processed/${mediaId}.webp`);
  expect(done.thumbnail_object_key).toBe(`processed/${mediaId}.thumb.webp`);
  // Detector not configured -> never copied to the public bucket.
  expect(done.public_object_key).toBeNull();
  expect(done.public_thumbnail_object_key).toBeNull();
  expect(await harness.objectExists(harness.publicBucket(), `media/${mediaId}.webp`)).toBe(false);
  expect(await harness.objectExists(harness.quarantineBucket(), done.processed_object_key!)).toBe(true);
  expect(await harness.objectExists(harness.quarantineBucket(), done.thumbnail_object_key!)).toBe(true);
  expect(done.privacy_report.completedAt).toBeTruthy();
  expect(done.delete_after).not.toBeNull();

  const actions = await harness.auditActions(mediaId);
  for (const action of expectedAudits) expect(actions).toContain(action);
}

async function fingerprint(done: MediaRow, mediaId: string): Promise<unknown> {
  return {
    status: done.privacy_status,
    failureCleared: done.failure_code === null,
    publicLeak: await harness.objectExists(harness.publicBucket(), `media/${mediaId}.webp`),
    processed: await harness.objectExists(harness.quarantineBucket(), done.processed_object_key!),
    thumbnail: await harness.objectExists(harness.quarantineBucket(), done.thumbnail_object_key!),
    audits: (await harness.auditActions(mediaId)).filter((action) =>
      ["media.processed_manual_review", "media.processing_failed"].includes(action)
    )
  };
}

const CONVERGED_FINGERPRINT = {
  status: "manual_review",
  failureCleared: true,
  publicLeak: false,
  processed: true,
  thumbnail: true,
  audits: ["media.processing_failed", "media.processed_manual_review"]
};

describe("media failure injection: single faults", () => {
  it("scan timeout: a scanner that never replies fails; replay converges", async () => {
    const mediaId = await harness.createMedia({ status: "processing" });
    await harness.watch(mediaId);
    const scanner = await spawnWithScanner("hang");
    await harness.enqueue(mediaId);
    let replayScanner: FakeClamServer | null = null;
    try {
      await assertFailedConsistently(mediaId, /timed out/);
      expect(await harness.transitionsOf(mediaId)).toContainEqual({ from: "scanning", to: "failed" });

      const replay = await replayHealthy(mediaId);
      replayScanner = replay.scanner;
      await legalTransitions(mediaId);
      await assertConverged(replay.done, mediaId, ["media.processing_failed", "media.processed_manual_review"]);
    } finally {
      await harness.killAllWorkers();
      await stopScanner(scanner);
      await stopScanner(replayScanner);
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });

  it("scan FOUND verdict fails and the original never becomes public", async () => {
    const mediaId = await harness.createMedia({ status: "processing" });
    await harness.watch(mediaId);
    const scanner = await spawnWithScanner("found");
    await harness.enqueue(mediaId);
    try {
      const failed = await assertFailedConsistently(mediaId, /Malware detected|FOUND/);
      expect(failed.sha256).toBeNull();
      expect(await harness.transitionsOf(mediaId)).toContainEqual({ from: "scanning", to: "failed" });
    } finally {
      await harness.killAllWorkers();
      await stopScanner(scanner);
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });

  it("blur failure: undecodable quarantine object fails; replay after re-upload converges", async () => {
    const mediaId = await harness.createMedia({ status: "processing", corruptObject: true });
    await harness.watch(mediaId);
    const scanner = await spawnWithScanner("ok");
    await harness.enqueue(mediaId);
    let replayScanner: FakeClamServer | null = null;
    try {
      await assertFailedConsistently(mediaId, /(Unsupported image|Input|truncated|Vips|unsupported|corrupt)/i);

      const replay = await replayHealthy(mediaId);
      replayScanner = replay.scanner;
      await legalTransitions(mediaId);
      await assertConverged(replay.done, mediaId, ["media.processing_failed", "media.processed_manual_review"]);
    } finally {
      await harness.killAllWorkers();
      await stopScanner(scanner);
      await stopScanner(replayScanner);
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });

  it("object missing: lost quarantine object fails; replay after object returns converges", async () => {
    const mediaId = await harness.createMedia({ status: "processing", missingObject: true });
    await harness.watch(mediaId);
    const scanner = await spawnWithScanner("ok");
    await harness.enqueue(mediaId);
    let replayScanner: FakeClamServer | null = null;
    try {
      await assertFailedConsistently(mediaId, /(NoSuchKey|NotFound|not found|does not exist)/i, false);

      const replay = await replayHealthy(mediaId);
      replayScanner = replay.scanner;
      await legalTransitions(mediaId);
      await assertConverged(replay.done, mediaId, ["media.processing_failed", "media.processed_manual_review"]);
    } finally {
      await harness.killAllWorkers();
      await stopScanner(scanner);
      await stopScanner(replayScanner);
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });
});

describe("media failure injection: queue replay", () => {
  it("duplicate replays while the fault persists stay failed; healthy replay yields a clean WebP", async () => {
    const mediaId = await harness.createMedia({ status: "processing" });
    await harness.watch(mediaId);

    const hang = new FakeClamServer();
    const hangPort = await hang.start("hang");
    try {
      await harness.spawnWorker({ kind: "port", port: hangPort });
      await harness.enqueue(mediaId);
      await harness.waitForStatus(mediaId, ["failed"]);
    } finally {
      await harness.killAllWorkers();
      await hang.stop();
    }

    // Two duplicate replays while the scanner still hangs: both stay failed and
    // nothing leaks into the public bucket (no partial/public objects).
    const hang2 = new FakeClamServer();
    const hangPort2 = await hang2.start("hang");
    try {
      await harness.spawnWorker({ kind: "port", port: hangPort2 });
      await harness.enqueue(mediaId);
      await harness.enqueue(mediaId);
      await harness.waitForQueueDrain(20_000);
      const still = await harness.getMedia(mediaId);
      expect(still!.privacy_status).toBe("failed");
      expect(await harness.objectExists(harness.publicBucket(), `media/${mediaId}.webp`)).toBe(false);
    } finally {
      await harness.killAllWorkers();
      await hang2.stop();
    }

    // Healthy replay converges exactly once: one derived pair, real WebP, no EXIF.
    const ok = new FakeClamServer();
    const okPort = await ok.start("ok");
    try {
      await harness.spawnWorker({ kind: "port", port: okPort });
      await harness.enqueue(mediaId);
      const done = await harness.waitForStatus(mediaId, ["manual_review"], 20_000);
      await harness.waitForQueueDrain(20_000);

      expect(await harness.objectExists(harness.quarantineBucket(), done.processed_object_key!)).toBe(true);
      expect(await harness.objectExists(harness.quarantineBucket(), done.thumbnail_object_key!)).toBe(true);
      const processed = await harness.objectBody(harness.quarantineBucket(), done.processed_object_key!);
      const meta = await sharp(processed).metadata();
      expect(meta.format).toBe("webp");
      expect(meta.exif).toBeUndefined();
      await legalTransitions(mediaId);
    } finally {
      await harness.killAllWorkers();
      await ok.stop();
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });

  it("worker crash mid-scan is reclaimed by maintenance only after the stale window, then replays", async () => {
    const mediaId = await harness.createMedia({ status: "processing" });
    await harness.watch(mediaId);

    const hang = new FakeClamServer();
    const hangPort = await hang.start("hang");
    try {
      // A very long scan timeout so the worker genuinely hangs (rather than the
      // 750ms scan timeout firing first) until we SIGKILL it.
      await harness.spawnWorker({ kind: "port", port: hangPort, scanTimeoutMs: 60_000 });
      await harness.enqueue(mediaId);
      await harness.waitForStatus(mediaId, ["scanning"]);
      await delay(300);
      harness.crashLatestWorker(); // hard crash: no graceful release of the job
      await delay(500);
      await harness.evictQueueClients(); // drop its lingering blocking-pop connection
    } finally {
      await hang.stop();
      await harness.killAllWorkers();
    }

    // The crashed worker left its job locked in Redis; production's BullMQ
    // stalled check would reclaim it on its own interval. Clear it here before
    // driving the database recovery, so the replay queue starts clean.
    await harness.discardActiveJobs();

    // Fresh asset: maintenance must not reclaim it yet (20-minute window).
    await harness.resetStaleRowsExcept(mediaId);
    expect(await harness.recoverStuck()).toEqual([]);
    expect((await harness.getMedia(mediaId))!.privacy_status).toBe("scanning");

    // Simulate 21 minutes passing: recovery reclaims this asset and audits it.
    await harness.backdate(mediaId, 21);
    expect(await harness.recoverStuck()).toEqual([mediaId]);
    expect(await harness.auditActions(mediaId)).toContain("media.recovered_after_timeout");
    expect((await harness.getMedia(mediaId))!.privacy_status).toBe("processing");

    // Replay onto a healthy worker converges exactly once.
    const ok = new FakeClamServer();
    const okPort = await ok.start("ok");
    try {
      await harness.spawnWorker({ kind: "port", port: okPort });
      await harness.enqueue(mediaId);
      const done = await harness.waitForStatus(mediaId, ["manual_review"], 20_000);
      await harness.waitForQueueDrain(20_000);
      await legalTransitions(mediaId);
      await assertConverged(done, mediaId, [
        "media.recovered_after_timeout",
        "media.processed_manual_review"
      ]);
    } finally {
      await harness.killAllWorkers();
      await ok.stop();
      harness.stopWatching(mediaId);
      await harness.cleanup([mediaId]);
    }
  });
});

describe("media failure injection: step reordering reproduces the same outcome", () => {
  const scenarios = [
    { key: "timeout", mode: "hang" as FakeClamMode, corrupt: false, missing: false },
    { key: "blur", mode: "ok" as FakeClamMode, corrupt: true, missing: false },
    { key: "missing", mode: "ok" as FakeClamMode, corrupt: false, missing: true }
  ];

  for (const seed of [1, 2, 3, 7]) {
    it(`seed ${seed}: fault/recover steps may run in any order`, async () => {
      // Phase 1: run the fault steps in the shuffled order.
      const faultOrder = seededShuffle(scenarios, seed);
      const faultedIds: Record<string, string> = {};
      for (const scenario of faultOrder) {
        const mediaId = await harness.createMedia({
          status: "processing",
          corruptObject: scenario.corrupt,
          missingObject: scenario.missing
        });
        faultedIds[scenario.key] = mediaId;
        await harness.watch(mediaId);

        const scanner = await spawnWithScanner(scenario.mode);
        await harness.enqueue(mediaId);
        await harness.waitForStatus(mediaId, ["failed"]);
        await harness.killAllWorkers();
        await stopScanner(scanner);
      }

      // Phase 2: recover in the reverse shuffle order. Reordering recovery
      // relative to the faults must not change any outcome. Each replay is
      // fully torn down before the next starts, so a worker bound to a dead
      // scanner can never steal a later job.
      const fingerprints: Record<string, unknown> = {};
      const created = Object.values(faultedIds);
      let replayScanner: FakeClamServer | null = null;
      try {
        for (const scenario of [...faultOrder].reverse()) {
          const mediaId = faultedIds[scenario.key]!;
          const replay = await replayHealthy(mediaId);
          replayScanner = replay.scanner;
          await legalTransitions(mediaId);
          fingerprints[scenario.key] = await fingerprint(replay.done, mediaId);
          harness.stopWatching(mediaId);
          // Tear this replay down before the next iteration binds a new scanner.
          await harness.killAllWorkers();
          await replayScanner.stop();
          replayScanner = null;
        }

        for (const scenario of scenarios) {
          expect(fingerprints[scenario.key], `scenario ${scenario.key}`).toEqual(CONVERGED_FINGERPRINT);
        }
      } finally {
        await harness.killAllWorkers();
        await stopScanner(replayScanner);
        await harness.cleanup(created);
      }
    });
  }

  it("interleaved faults across two assets converge independently", async () => {
    const a = await harness.createMedia({ status: "processing" });
    const b = await harness.createMedia({ status: "processing", corruptObject: true });
    await harness.watch(a);
    await harness.watch(b);

    // Fail A (hang) then B (blur); recovery order is reversed below.
    const hang = new FakeClamServer();
    const hangPort = await hang.start("hang");
    try {
      await harness.spawnWorker({ kind: "port", port: hangPort });
      await harness.enqueue(a);
      await harness.waitForStatus(a, ["failed"]);
    } finally {
      await harness.killAllWorkers();
      await hang.stop();
    }
    const ok = new FakeClamServer();
    const okPort = await ok.start("ok");
    try {
      await harness.spawnWorker({ kind: "port", port: okPort });
      await harness.enqueue(b);
      await harness.waitForStatus(b, ["failed"]);
    } finally {
      await harness.killAllWorkers();
      await ok.stop();
    }

    await harness.restoreValidObject(b);
    const healthyB = new FakeClamServer();
    const portB = await healthyB.start("ok");
    try {
      await harness.spawnWorker({ kind: "port", port: portB });
      await harness.enqueue(b); // recover B first
      await harness.waitForStatus(b, ["manual_review"], 20_000);
      await harness.waitForQueueDrain(20_000);
    } finally {
      await harness.killAllWorkers();
      await healthyB.stop();
    }

    await harness.restoreValidObject(a);
    const healthyA = new FakeClamServer();
    const portA = await healthyA.start("ok");
    try {
      await harness.spawnWorker({ kind: "port", port: portA });
      await harness.enqueue(a); // recover A second
      await harness.waitForStatus(a, ["manual_review"], 20_000);
      await harness.waitForQueueDrain(20_000);
    } finally {
      await harness.killAllWorkers();
      await healthyA.stop();
    }

    expect((await harness.getMedia(a))!.privacy_status).toBe("manual_review");
    expect((await harness.getMedia(b))!.privacy_status).toBe("manual_review");
    expect((await harness.getMedia(a))!.failure_code).toBeNull();
    expect((await harness.getMedia(b))!.failure_code).toBeNull();
    await legalTransitions(a);
    await legalTransitions(b);

    harness.stopWatching(a);
    harness.stopWatching(b);
    await harness.cleanup([a, b]);
  });
});
