import { describe, expect, it } from "vitest";
import { MEDIA_AUDIT_ACTIONS } from "./types";
import { PUBLIC_BUCKET, QUARANTINE_BUCKET, makeSource } from "./fakes";
import {
  MediaHarness,
  makeRng,
  shuffle,
  snapshot,
  type Step
} from "./harness";

const MEDIA = "11111111-1111-4111-8111-111111111111";

describe("media fault injection — single failures", () => {
  it("scan timeout: collapses to failed with audit, then a replay converges to manual_review", async () => {
    // 1) first attempt: scanner hangs -> timeout
    const harness = new MediaHarness({
      autoPublish: false,
      faults: [{ point: "scanMalware", message: "ClamAV scan timed out", attempt: 1 }]
    });
    harness.seed(MEDIA, makeSource());

    const first = await harness.deliver(MEDIA);
    expect(first.error).toMatch(/timed out/);
    let row = harness.table.get(MEDIA)!;
    expect(row.privacy_status).toBe("failed");
    expect(row.failure_code).toMatch(/timed out/);
    // failure must not publish anything
    expect(harness.store.has(PUBLIC_BUCKET, `media/${MEDIA}.webp`)).toBe(false);
    // audit trail records the failed attempt
    expect(harness.table.audits.map((a) => a.action)).toContain(MEDIA_AUDIT_ACTIONS.failed);
    // every observed edge is legal
    expect(harness.table.illegalTransitions()).toEqual([]);

    // 2) retry (fault is spent) -> converges, emits success audit exactly once
    await harness.runToQuiescence(MEDIA);
    row = harness.table.get(MEDIA)!;
    expect(row.privacy_status).toBe("manual_review");
    expect(row.failure_code).toBeNull();
    const succeeded = harness.table.audits.filter((a) => a.action === MEDIA_AUDIT_ACTIONS.succeeded);
    expect(succeeded).toHaveLength(1);
    // failure audit carries the fromStatus for forensics
    const failedAudit = harness.table.audits.find((a) => a.action === MEDIA_AUDIT_ACTIONS.failed)!;
    expect(failedAudit.metadata.fromStatus).toBe("processing");
  });

  it("blur failure (processImage): marks failed and a later replay succeeds", async () => {
    const harness = new MediaHarness({
      autoPublish: false,
      faults: [{ point: "processImage", message: "Unsupported image format: gif", attempt: 1 }]
    });
    harness.seed(MEDIA, makeSource());

    const first = await harness.deliver(MEDIA);
    expect(first.error).toMatch(/Unsupported image format/);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("failed");
    // scanning -> processing had already happened; failure edge is processing -> failed
    const edges = harness.table.transitions.map((t) => `${t.from}->${t.to}`);
    expect(edges).toContain("scanning->processing");
    expect(edges).toContain("processing->failed");

    await harness.runToQuiescence(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("manual_review");
    expect(harness.table.illegalTransitions()).toEqual([]);
  });

  it("object loss: a missing quarantine original fails fast; restoring it lets replay converge", async () => {
    const harness = new MediaHarness({ autoPublish: false });
    harness.seed(MEDIA, makeSource());

    // lose the original before processing -> readQuarantine NoSuchKey
    harness.loseQuarantine(MEDIA);
    const first = await harness.deliver(MEDIA);
    expect(first.error).toMatch(/NoSuchKey/);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("failed");
    expect(harness.table.audits.map((a) => a.action)).toContain(MEDIA_AUDIT_ACTIONS.failed);

    // object restored (re-upload / storage recovery) -> replay succeeds
    const key = harness.table.get(MEDIA)!.quarantine_object_key;
    harness.store.put(QUARANTINE_BUCKET, key, makeSource());
    await harness.runToQuiescence(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("manual_review");
    expect(snapshot(harness, MEDIA).derivationsPresent).toBe(true);
  });

  it("auto-publish rollback: failure after copying public objects leaves no orphan", async () => {
    const harness = new MediaHarness({
      autoPublish: true,
      faults: [{ point: "finalize", message: "DB connection reset", attempt: 1 }]
    });
    harness.seed(MEDIA, makeSource());

    await harness.deliver(MEDIA);
    // copyToPublic ran before finalize; the catch block must have rolled both back
    expect(harness.store.has(PUBLIC_BUCKET, `media/${MEDIA}.webp`)).toBe(false);
    expect(harness.store.has(PUBLIC_BUCKET, `media/${MEDIA}.thumb.webp`)).toBe(false);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("failed");

    await harness.runToQuiescence(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("ready");
    const snap = snapshot(harness, MEDIA);
    expect(snap.publicPresent).toBe(true);
    expect(snap.derivationsPresent).toBe(true);
  });
});

describe("media queue replay", () => {
  it("duplicate delivery of an already-ready job is a no-op (at-least-once safe)", async () => {
    const harness = new MediaHarness({ autoPublish: true });
    harness.seed(MEDIA, makeSource());
    await harness.runToQuiescence(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("ready");

    const auditsBefore = harness.table.audits.length;
    const result = await harness.deliver(MEDIA); // redelivered by BullMQ
    expect(result.status).toBe("skipped");
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("ready");
    // no extra state edge, no duplicate success audit
    expect(harness.table.audits.length).toBe(auditsBefore);
    expect(harness.store.has(PUBLIC_BUCKET, `media/${MEDIA}.webp`)).toBe(true);
  });

  it("crash while scanning is recovered by the sweep and then converges", async () => {
    const harness = new MediaHarness({ autoPublish: false });
    harness.seed(MEDIA, makeSource());

    // worker process is killed after entering scanning
    await harness.step({ kind: "deliverCrashInScan", mediaId: MEDIA });
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("scanning");

    // maintenance sweep recovery
    harness.table.recover(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("processing");
    expect(harness.table.audits.map((a) => a.action)).toContain(MEDIA_AUDIT_ACTIONS.recovered);

    await harness.runToQuiescence(MEDIA);
    expect(harness.table.get(MEDIA)!.privacy_status).toBe("manual_review");
    expect(harness.table.illegalTransitions()).toEqual([]);
  });

  it("lost public object after ready is reconciled by replay (eventual consistency)", async () => {
    const harness = new MediaHarness({ autoPublish: true });
    harness.seed(MEDIA, makeSource());
    await harness.runToQuiescence(MEDIA);
    expect(snapshot(harness, MEDIA).publicPresent).toBe(true);

    // object storage loses the public derivations (DB still says ready)
    harness.losePublic(MEDIA);
    expect(snapshot(harness, MEDIA).publicPresent).toBe(false);

    // reconciliation loop detects the gap and replays; converges again
    await harness.runToQuiescence(MEDIA);
    const snap = snapshot(harness, MEDIA);
    expect(snap.status).toBe("ready");
    expect(snap.publicPresent).toBe(true);
  });
});

describe("media fault injection — reordered steps reproduce the same outcome", () => {
  // The requirement "步骤重排仍可复现" is verified by running the *same set*
  // of fault/queue steps in many seeded permutations and asserting that after
  // draining to quiescence every run converges to an identical result. Only
  // order-independent invariants are compared (terminal status, object set,
  // settled content hash, success-audit count, no illegal edges).

  const SEEDS = [1, 2, 7, 42, 99, 12345, 777, 2026];
  const FIXED_SOURCE = Buffer.from("fixed-source-bytes-for-reorder-reproducibility");

  async function runPermutation(
    autoPublish: boolean,
    faults: import("./fakes").Fault[],
    steps: Step[],
    seed: number,
    initial: "processing" | "ready" = "processing"
  ) {
    const harness = new MediaHarness({ autoPublish, faults });
    if (initial === "ready") {
      harness.seedReady(MEDIA, FIXED_SOURCE, "seed-sha256");
    } else {
      harness.seed(MEDIA, FIXED_SOURCE);
    }
    await harness.runSteps(seed === 0 ? steps : shuffle(steps, makeRng(seed)));
    await harness.runToQuiescence(MEDIA);
    return {
      snap: snapshot(harness, MEDIA),
      illegalEdges: harness.table.illegalTransitions(),
      quiescent: harness.table.isQuiescent()
    };
  }

  // Experiment A: three transient failures at distinct pipeline points are
  // consumed on attempts 1-3; attempt 4 succeeds and later deliveries are
  // skips. A sweep/recover step is interleaved (no-op while failed). Running
  // this set in arbitrary order must converge to the identical result, proving
  // duplicate deliveries and recoveries neither shift attempt alignment nor
  // change the outcome.
  for (const autoPublish of [true, false]) {
    it(`A: transient failures converge identically regardless of order (autoPublish=${autoPublish})`, async () => {
      const faults: import("./fakes").Fault[] = [
        { point: "scanMalware", message: "ClamAV scan timed out", attempt: 1 },
        { point: "processImage", message: "blur pipeline crashed", attempt: 2 },
        { point: autoPublish ? "copyPublic" : "writeProcessed", message: "transient io error", attempt: 3 }
      ];
      const steps: Step[] = [
        { kind: "deliver", mediaId: MEDIA }, // attempt 1 -> scan fault
        { kind: "deliver", mediaId: MEDIA }, // attempt 2 -> blur fault
        { kind: "recover", mediaId: MEDIA }, // sweep (no-op while failed)
        { kind: "deliver", mediaId: MEDIA }, // attempt 3 -> io fault
        { kind: "deliver", mediaId: MEDIA }, // attempt 4 -> success
        { kind: "deliver", mediaId: MEDIA }  // now terminal -> skipped
      ];

      const reference = await runPermutation(autoPublish, faults, steps, SEEDS[0]!);
      expect(reference.quiescent).toBe(true);
      expect(reference.illegalEdges).toEqual([]);
      for (const seed of SEEDS.slice(1)) {
        const result = await runPermutation(autoPublish, faults, steps, seed);
        expect(result.snap, `seed ${seed}`).toEqual(reference.snap);
        expect(result.illegalEdges, `seed ${seed}`).toEqual([]);
        expect(result.quiescent, `seed ${seed}`).toBe(true);
      }
      // canonical order converges to the exact same point
      const canonical = await runPermutation(autoPublish, faults, steps, 0);
      expect(canonical.snap).toEqual(reference.snap);
    });
  }

  // Experiment B: post-ready disruptions — public object loss, gap
  // reconciliation, a sweep tick and duplicate delivery — all commute to a
  // single, identical repaired `ready` state.
  it("B: object loss + reconciliation + duplicate delivery commute to one repaired state", async () => {
    const autoPublish = true;
    // Every step here is a legal no-op unless its precondition holds, so the
    // set is fully permutation-safe. Mid-flight crash recovery is exercised by
    // a dedicated test above (it is only reachable during an active attempt).
    const steps: Step[] = [
      { kind: "losePublic", mediaId: MEDIA },   // public derivations vanish
      { kind: "reconcile", mediaId: MEDIA },    // ready -> processing gap repair
      { kind: "recover", mediaId: MEDIA },      // sweep (no-op unless in flight)
      { kind: "deliver", mediaId: MEDIA },      // replay re-copies derivations
      { kind: "deliver", mediaId: MEDIA }       // duplicate once ready -> skip
    ];

    const reference = await runPermutation(autoPublish, [], steps, SEEDS[0]!, "ready");
    expect(reference.snap.status).toBe("ready");
    expect(reference.snap.publicPresent).toBe(true);
    expect(reference.quiescent).toBe(true);
    expect(reference.illegalEdges).toEqual([]);
    for (const seed of SEEDS.slice(1)) {
      const result = await runPermutation(autoPublish, [], steps, seed, "ready");
      expect(result.snap, `seed ${seed}`).toEqual(reference.snap);
      expect(result.illegalEdges, `seed ${seed}`).toEqual([]);
    }
    const canonical = await runPermutation(autoPublish, [], steps, 0, "ready");
    expect(canonical.snap).toEqual(reference.snap);
  });
});
