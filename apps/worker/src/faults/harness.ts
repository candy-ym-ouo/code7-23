import { randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import net from "node:net";
import {
  CreateBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client
} from "@aws-sdk/client-s3";
import { Queue } from "bullmq";
import IORedis, { type RedisOptions } from "ioredis";
import pg from "pg";
import sharp from "sharp";

const { Pool } = pg;
const require = createRequire(import.meta.url);
const here = dirname(fileURLToPath(import.meta.url));

export const ALLOWED_TRANSITIONS = {
  quarantined: ["processing", "deleted"],
  scanning: ["processing", "failed"],
  processing: ["scanning", "manual_review", "ready", "failed", "deleted"],
  manual_review: ["ready", "rejected", "deleted"],
  ready: ["deleted"],
  rejected: ["processing", "deleted"],
  // Retry/replay enqueues a job straight from `failed`; the job first flips
  // the row to `scanning`, so failed -> scanning is a real edge.
  failed: ["processing", "scanning", "deleted"],
  deleted: []
} as const;

export type MediaStatus = keyof typeof ALLOWED_TRANSITIONS;

export type MediaRow = {
  id: string;
  owner_id: string;
  privacy_status: string;
  quarantine_object_key: string;
  processed_object_key: string | null;
  thumbnail_object_key: string | null;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
  failure_code: string | null;
  sha256: string | null;
  privacy_report: { completedAt?: string };
  delete_after: Date | null;
};

export type WorkerScanner =
  | { kind: "disabled"; scanTimeoutMs?: number }
  | { kind: "port"; port: number; scanTimeoutMs?: number };

/**
 * Real-stack failure injection harness. Every dependency is a real service:
 * PostgreSQL rows, Redis/BullMQ jobs, S3/MinIO objects and the genuine
 * `processMediaJob` pipeline (spawned as its own process). Faults are injected
 * at the system boundary (scanner socket, object storage contents) rather than
 * by mocking application modules.
 */
export class FaultHarness {
  private pool!: pg.Pool;
  private s3!: S3Client;
  private redisUrl = process.env.REDIS_URL ?? "redis://localhost:6379/0";
  private queuePrefix = `faults-${randomUUID()}`;
  private queueName = `media-faults`;
  private queue!: Queue;
  private workers: ChildProcess[] = [];
  private workerNames: string[] = [];
  private userId = "";
  readonly workerLogs: string[] = [];

  get prefix(): string {
    return this.queuePrefix;
  }

  async start(): Promise<void> {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL ?? "postgres://map:map@localhost:5432/map",
      max: 4
    });
    await this.installTransitionTrap();
    this.s3 = new S3Client({
      endpoint: process.env.S3_ENDPOINT ?? "http://localhost:9000",
      region: process.env.S3_REGION ?? "us-east-1",
      forcePathStyle: true,
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY ?? "minioadmin",
        secretAccessKey: process.env.S3_SECRET_KEY ?? "minioadmin"
      }
    });
    await Promise.all([this.ensureBucket(this.quarantineBucket()), this.ensureBucket(this.publicBucket())]);

    // Isolated BullMQ keyspace prefix: fault jobs never touch a shared queue.
    this.queue = new Queue(this.queueName, {
      connection: redisConnectionOptions(this.redisUrl),
      prefix: this.queuePrefix
    });

    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO users(email, email_normalized, password_hash, display_name, status, email_verified_at)
       VALUES ($1, $1, 'fault-test-hash', 'Fault Tester', 'active', now())
       RETURNING id`,
      [`fault-${randomUUID()}@example.test`]
    );
    this.userId = result.rows[0]!.id;
  }

  private async ensureBucket(name: string): Promise<void> {
    await this.s3
      .send(new CreateBucketCommand({ Bucket: name }))
      .catch((error: { name?: string; $metadata?: { httpStatusCode?: number } }) => {
        if (error?.name === "BucketAlreadyOwnedByYou" || error?.$metadata?.httpStatusCode === 409) return;
        throw error;
      });
  }

  /**
   * Install a transition trap: a Postgres trigger records *every*
   * media_assets status change for watched ids, so the state-machine test
   * cannot miss a fast `scanning -> processing` hop the way polling could.
   */
  private async installTransitionTrap(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS fault_media_transitions (
        media_id uuid NOT NULL,
        from_status text,
        to_status text NOT NULL,
        at timestamptz NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS fault_watched_media (media_id uuid PRIMARY KEY);
      CREATE OR REPLACE FUNCTION fault_media_transition_fn() RETURNS trigger AS $$
      BEGIN
        IF NEW.privacy_status IS DISTINCT FROM OLD.privacy_status
           AND EXISTS (SELECT 1 FROM fault_watched_media w WHERE w.media_id = NEW.id) THEN
          INSERT INTO fault_media_transitions(media_id, from_status, to_status)
          VALUES (NEW.id, OLD.privacy_status, NEW.privacy_status);
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      DROP TRIGGER IF EXISTS fault_media_transition_trg ON media_assets;
      CREATE TRIGGER fault_media_transition_trg
        AFTER UPDATE ON media_assets
        FOR EACH ROW EXECUTE FUNCTION fault_media_transition_fn();
    `);
  }

  /** Create a media row in a starting status plus a real object in quarantine. */
  async createMedia(
    input: { status?: MediaStatus; corruptObject?: boolean; missingObject?: boolean } = {}
  ): Promise<string> {
    const id = randomUUID();
    const key = `quarantine/${this.userId}/${id}.png`;
    if (!input.missingObject) {
      const image = input.corruptObject
        ? Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), Buffer.from("corrupted-image-payload")])
        : await makeValidPng();
      await this.s3.send(
        new PutObjectCommand({
          Bucket: this.quarantineBucket(),
          Key: key,
          Body: image,
          ContentType: "image/png"
        })
      );
    }

    await this.pool.query(
      `INSERT INTO media_assets
         (id, owner_id, original_filename, mime_type, byte_size, quarantine_object_key, privacy_status, privacy_report)
       VALUES ($1, $2, 'fault.png', 'image/png', $3, $4, $5,
         '{"manualRegions":[],"containsPeopleOrPlates":false}'::jsonb)`,
      [id, this.userId, input.corruptObject ? 30 : 320 * 240 * 3, key, input.status ?? "processing"]
    );
    return id;
  }

  /** Register a media asset for complete server-side transition capture. */
  async watch(mediaId: string): Promise<void> {
    await this.pool.query("INSERT INTO fault_watched_media(media_id) VALUES ($1) ON CONFLICT DO NOTHING", [mediaId]);
  }

  stopWatching(mediaId: string): void {
    void this.pool.query("DELETE FROM fault_watched_media WHERE media_id = $1", [mediaId]);
  }

  async transitionsOf(mediaId: string): Promise<Array<{ from: string; to: string }>> {
    const result = await this.pool.query<{ from_status: string | null; to_status: string }>(
      "SELECT from_status, to_status FROM fault_media_transitions WHERE media_id = $1 ORDER BY at, ctid",
      [mediaId]
    );
    return result.rows
      .filter((row): row is { from_status: string; to_status: string } => row.from_status !== null)
      .map((row) => ({ from: row.from_status, to: row.to_status }));
  }

  async getMedia(mediaId: string): Promise<MediaRow | null> {
    const result = await this.pool.query<MediaRow>(
      `SELECT id, owner_id, privacy_status, quarantine_object_key, processed_object_key,
              thumbnail_object_key, public_object_key, public_thumbnail_object_key,
              failure_code, sha256, privacy_report, delete_after
       FROM media_assets WHERE id = $1`,
      [mediaId]
    );
    return result.rows[0] ?? null;
  }

  async setStatus(mediaId: string, status: MediaStatus, failureCode: string | null = null): Promise<void> {
    await this.pool.query(
      "UPDATE media_assets SET privacy_status = $2, failure_code = $3, updated_at = now() WHERE id = $1",
      [mediaId, status, failureCode]
    );
  }

  /** Backdate a stuck asset so `recoverStuckMedia` claims it. */
  async backdate(mediaId: string, minutes: number): Promise<void> {
    await this.pool.query(
      "UPDATE media_assets SET updated_at = now() - ($2::text || ' minutes')::interval WHERE id = $1",
      [mediaId, String(minutes)]
    );
  }

  async restoreValidObject(mediaId: string): Promise<void> {
    const media = await this.getMedia(mediaId);
    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.quarantineBucket(),
        Key: media!.quarantine_object_key,
        Body: await makeValidPng(),
        ContentType: "image/png"
      })
    );
  }

  async deleteQuarantineObject(mediaId: string): Promise<void> {
    const media = await this.getMedia(mediaId);
    await this.s3.send(new DeleteObjectCommand({ Bucket: this.quarantineBucket(), Key: media!.quarantine_object_key }));
  }

  async objectExists(bucket: string, key: string | null): Promise<boolean> {
    if (!key) return false;
    try {
      await this.s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      return true;
    } catch {
      return false;
    }
  }

  async objectBody(bucket: string, key: string): Promise<Buffer> {
    const response = await this.s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    return Buffer.from(await response.Body!.transformToByteArray());
  }

  quarantineBucket(): string {
    return process.env.S3_QUARANTINE_BUCKET ?? "quarantine-test";
  }

  publicBucket(): string {
    return process.env.S3_PUBLIC_BUCKET ?? "public-test";
  }

  async auditActions(mediaId: string): Promise<string[]> {
    const result = await this.pool.query<{ action: string }>(
      "SELECT action FROM audit_logs WHERE resource_type = 'media' AND resource_id = $1 ORDER BY created_at, id",
      [mediaId]
    );
    return result.rows.map((row) => row.action);
  }

  async recordAudit(mediaId: string, action: string): Promise<void> {
    await this.pool.query(
      "INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata) VALUES ($1, $2, 'media', $3, '{}'::jsonb)",
      [this.userId, action, mediaId]
    );
  }

  /**
   * Spawn the real worker pipeline in an isolated child process (real restart /
   * crash semantics). The scanner endpoint points at a fake ClamAV socket or
   * scanning is disabled.
   */
  async spawnWorker(scanner: WorkerScanner): Promise<void> {
    // No previous worker may still be draining this queue, or it could pop the
    // new job before the freshly spawned worker does.
    await this.killAllWorkers();
    // Resolve the tsx CLI through the package location (the dist/cli.mjs
    // subpath is not in tsx's "exports" map, so require.resolve rejects it).
    const tsxPackage = require.resolve("tsx/package.json");
    const tsxCli = join(dirname(tsxPackage), "dist", "cli.mjs");
    const scriptPath = join(here, "worker-process.ts");
    const workerName = `w-${randomUUID()}`;
    const child = spawn(process.execPath, [tsxCli, scriptPath], {
      env: {
        ...process.env,
        FAULT_QUEUE_NAME: this.queueName,
        FAULT_QUEUE_PREFIX: this.queuePrefix,
        FAULT_WORKER_NAME: workerName,
        NODE_ENV: "test",
        PRIVACY_DETECTOR_URL: "",
        CLAMAV_ENABLED: scanner.kind === "disabled" ? "false" : "true",
        CLAMAV_HOST: "127.0.0.1",
        CLAMAV_PORT: scanner.kind === "port" ? String(scanner.port) : "1",
        CLAMAV_TIMEOUT_MS: String(scanner.scanTimeoutMs ?? Number(process.env.CLAMAV_TIMEOUT_MS ?? 750))
      },
      stdio: ["ignore", "pipe", "pipe"],
      // Own process group so a crash/teardown can SIGKILL the worker AND its
      // tsx child together; otherwise the node worker survives orphaned.
      detached: true
    });
    child.stdout!.on("data", (chunk: Buffer) => this.workerLogs.push(chunk.toString()));
    child.stderr!.on("data", (chunk: Buffer) => this.workerLogs.push(chunk.toString()));
    child.on("exit", (code, signal) => {
      if (code && code !== 0) this.workerLogs.push(`worker exited code=${code} signal=${signal}`);
    });
    this.workers.push(child);
    this.workerNames.push(workerName);

    // Wait until BullMQ reports THIS named worker as a connected client. The
    // readiness log alone races: Worker construction's Redis connect and
    // blocking-pop setup are async, so an older worker still draining after its
    // SIGTERM could otherwise steal a newly enqueued job.
    const deadline = Date.now() + 15_000;
    for (;;) {
      const registered = await this.queue.getWorkers().catch(() => []);
      if (registered.some((worker) => (worker as { rawname?: string }).rawname?.endsWith(`:w:${workerName}`))) return;
      if (child.exitCode !== null) throw new Error(`fault worker exited before registering\n${this.workerLogs.slice(-15).join("")}`);
      if (Date.now() > deadline) throw new Error("fault worker did not register in time");
      await delay(25);
    }
  }

  private signal(child: ChildProcess, signal: NodeJS.Signals | "SIGKILL"): void {
    if (child.pid === undefined || child.exitCode !== null) return;
    try {
      process.kill(-child.pid, signal); // whole process group (detached)
    } catch {
      child.kill(signal); // group already gone; signal the child directly
    }
  }

  async killAllWorkers(): Promise<void> {
    const pending = this.workers.splice(0);
    const names = this.workerNames.splice(0);
    await Promise.all(
      pending.map(
        (child) =>
          new Promise<void>((resolve) => {
            if (child.killed || child.exitCode !== null) return resolve();
            let settled = false;
            const finish = () => {
              if (settled) return;
              settled = true;
              clearTimeout(timer);
              resolve();
            };
            const timer = setTimeout(() => {
              if (child.exitCode === null) this.signal(child, "SIGKILL");
              // Give the kernel a moment, then resolve even if no exit event
              // reaches us (the event may have fired before this listener).
              setTimeout(finish, 500).unref();
            }, 3_000);
            child.once("exit", finish);
            this.signal(child, "SIGTERM");
          })
      )
    );
    await this.evictQueueClients();
    // Wait until THIS harness's named workers leave the BullMQ registry, so a
    // worker whose scanner is gone cannot steal a subsequently enqueued job.
    const queuePrefixToken = `:${Buffer.from(this.queueName).toString("base64")}:`;
    const deadline = Date.now() + 5_000;
    for (;;) {
      const workers = await this.queue.getWorkers().catch(() => []);
      const own = workers.filter((worker) => {
        const rawname = (worker as { rawname?: string }).rawname ?? "";
        if (names.some((name) => rawname.endsWith(`:w:${name}`))) return true;
        // Also drain orphaned workers from earlier runs on this exact queue.
        return rawname.startsWith(`${this.queuePrefix}:`) && rawname.includes(queuePrefixToken);
      });
      if (own.length === 0) return;
      if (Date.now() > deadline) return;
      await delay(25);
    }
  }

  async enqueue(mediaId: string): Promise<void> {
    await this.queue.add("process", { mediaId }, {
      jobId: `media-${mediaId}-${randomUUID()}`,
      removeOnComplete: true,
      removeOnFail: true
    });
  }

  async waitForQueueDrain(timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const counts = await this.queue.getJobCounts("active", "waiting", "delayed");
      if ((counts.active ?? 0) + (counts.waiting ?? 0) + (counts.delayed ?? 0) === 0) return;
      if (Date.now() > deadline) throw new Error("queue did not drain in time");
      await delay(50);
    }
  }

  /**
   * Remove jobs left locked by a crashed worker. In production BullMQ's stalled
   * check reclaims these on its own interval; after a forced SIGKILL in a test
   * the job is locked by a dead worker and job.remove() cannot unlock it, so we
   * clear the active list, job hashes and locks directly.
   */
  async discardActiveJobs(): Promise<number> {
    const jobs = await this.queue.getJobs(["active", "waiting", "delayed", "prioritized"], 0, 100);
    await Promise.all(jobs.map((job) => job.remove().catch(() => {})));
    // Hard cleanup for entries still locked by the dead worker. BullMQ key
    // layout: wait/active are lists; delayed/prioritized are sorted sets; a job
    // is a hash at <base>:<jobId> with a lock key <base>:<jobId>:lock.
    const redis = new IORedis(this.redisUrl);
    try {
      const base = `${this.queuePrefix}:${this.queueName}`;
      const activeIds = await redis.lrange(`${base}:active`, 0, -1);
      for (const id of activeIds) {
        await redis.del(`${base}:${id}`);
        await redis.del(`${base}:${id}:lock`);
        await redis.lrem(`${base}:active`, 0, id);
      }
    } finally {
      redis.disconnect();
    }
    return jobs.length;
  }

  async waitForStatus(mediaId: string, statuses: string[], timeoutMs = 15_000): Promise<MediaRow> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const media = await this.getMedia(mediaId);
      if (media && statuses.includes(media.privacy_status)) return media;
      if (Date.now() > deadline) {
        throw new Error(
          `media ${mediaId} did not reach ${statuses.join("/")}; last=${media?.privacy_status}\n` +
            this.workerLogs.slice(-20).join("")
        );
      }
      await delay(50);
    }
  }

  /** Run the production `recoverStuckMedia` maintenance query in-process. */
  async recoverStuck(): Promise<string[]> {
    const { recoverStuckMedia } = await import("../media-job");
    return recoverStuckMedia();
  }

  /** Reset any stale scanning/processing rows from prior tests so recovery is deterministic. */
  async resetStaleRowsExcept(mediaId: string): Promise<void> {
    await this.pool.query(
      `UPDATE media_assets SET updated_at = now()
       WHERE privacy_status IN ('scanning','processing') AND id <> $1 AND deleted_at IS NULL`,
      [mediaId]
    );
  }

  /** Hard-kill the most recently spawned worker and its whole group (crash). */
  crashLatestWorker(): void {
    const child = this.workers[this.workers.length - 1];
    if (child) this.signal(child, "SIGKILL");
  }

  /**
   * After a hard crash the OS process is gone but its Redis blocking-pop
   * connection can linger until the TCP timeout, still eligible to receive
   * jobs. Forcibly disconnect any client registered for this harness's queue.
   */
  async evictQueueClients(): Promise<void> {
    const redis = new IORedis(this.redisUrl);
    try {
      const raw = await redis.client("LIST");
      const lines = typeof raw === "string" ? raw.split(/\r?\n/) : [];
      for (const line of lines) {
        const addr = /\baddr=([^\s]+)/.exec(line)?.[1];
        const fd = /\bfd=([^\s]+)/.exec(line)?.[1];
        const name = /\bname=([^\s]+)/.exec(line)?.[1] ?? "";
        if (name.startsWith(`${this.queuePrefix}:`) && addr && fd) {
          await redis.client("KILL", "ADDR", addr, "FD", fd).catch(() => {});
        }
      }
    } finally {
      redis.disconnect();
    }
  }

  async cleanup(mediaIds: string[]): Promise<void> {
    const keys: Array<{ bucket: string; key: string }> = [];
    for (const mediaId of mediaIds) {
      const media = await this.getMedia(mediaId);
      if (!media) continue;
      for (const key of [media.quarantine_object_key, media.processed_object_key, media.thumbnail_object_key]) {
        if (key) keys.push({ bucket: this.quarantineBucket(), key });
      }
      for (const key of [media.public_object_key, media.public_thumbnail_object_key]) {
        if (key) keys.push({ bucket: this.publicBucket(), key });
      }
    }
    await Promise.all(
      keys.map(({ bucket, key }) => this.s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key })).catch(() => {}))
    );
    await this.pool.query("DELETE FROM fault_media_transitions WHERE media_id = ANY($1::uuid[])", [mediaIds]);
    await this.pool.query("DELETE FROM fault_watched_media WHERE media_id = ANY($1::uuid[])", [mediaIds]);
    await this.pool.query("DELETE FROM audit_logs WHERE resource_type = 'media' AND resource_id = ANY($1::uuid[])", [mediaIds]);
    await this.pool.query("DELETE FROM media_assets WHERE id = ANY($1::uuid[])", [mediaIds]);
    // The owner user is shared by all tests and removed once in stop().
  }

  async stop(): Promise<void> {
    await this.killAllWorkers();
    await this.queue.obliterate({ force: true }).catch(() => {});
    await this.queue.close();
    if (this.userId) {
      await this.pool.query("DELETE FROM users WHERE id = $1", [this.userId]).catch(() => {});
    }
    await this.pool.end();
    this.s3.destroy();
  }
}

export async function makeValidPng(): Promise<Buffer> {
  return sharp({
    create: { width: 320, height: 240, channels: 3, background: { r: 120, g: 90, b: 40 } }
  }).png().toBuffer();
}

/** Parse a redis:// URL into ioredis/BullMQ connection options. */
function redisConnectionOptions(url: string): RedisOptions {
  const parsed = new URL(url);
  return {
    host: parsed.hostname,
    port: Number(parsed.port || 6379),
    username: parsed.username || undefined,
    password: parsed.password || undefined,
    db: parsed.pathname && parsed.pathname !== "/" ? Number(parsed.pathname.slice(1)) : 0,
    maxRetriesPerRequest: null
  };
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Deterministic Fisher-Yates shuffle with a numeric seed (for step reordering). */
export function seededShuffle<T>(items: T[], seed: number): T[] {
  const copy = [...items];
  let state = seed >>> 0;
  const random = () => {
    // xorshift32
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return state / 0x1_0000_0000;
  };
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const j = Math.floor(random() * (index + 1));
    [copy[index], copy[j]] = [copy[j]!, copy[index]!];
  }
  return copy;
}

/** True when a TCP port accepts a connection. */
export function canConnect(port: number, host = "127.0.0.1"): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => resolve(false));
    socket.setTimeout(1_000, () => {
      socket.destroy();
      resolve(false);
    });
  });
}
