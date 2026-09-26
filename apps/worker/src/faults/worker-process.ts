import { Worker } from "bullmq";
import IORedis from "ioredis";
import { config } from "../config";
import { pool } from "../db";
import { processMediaJob } from "../media-job";

/**
 * Standalone worker process for the fault-injection suite. It runs the exact
 * production `processMediaJob` pipeline on an isolated BullMQ queue whose name
 * and key prefix come from FAULT_QUEUE_* env vars, so it never touches a
 * shared media queue.
 */
const queueName = process.env.FAULT_QUEUE_NAME;
const prefix = process.env.FAULT_QUEUE_PREFIX;
const workerName = process.env.FAULT_WORKER_NAME;
if (!queueName || !prefix) {
  console.error("FAULT_QUEUE_NAME and FAULT_QUEUE_PREFIX are required");
  process.exit(2);
}

const connection = new IORedis(config.REDIS_URL, { maxRetriesPerRequest: null });
connection.on("error", (error) => console.error({ error }, "fault worker redis error"));

const worker = new Worker(
  queueName,
  async (job) => {
    if (job.name !== "process") return;
    console.log(JSON.stringify({ phase: "job-start", mediaId: job.data.mediaId }));
    await processMediaJob(String(job.data.mediaId));
    console.log(JSON.stringify({ phase: "job-done", mediaId: job.data.mediaId }));
  },
  {
    connection,
    prefix,
    concurrency: 1,
    ...(workerName ? { name: workerName } : {})
  }
);

worker.on("failed", (job, error) => {
  console.error(JSON.stringify({ phase: "job-failed", jobId: job?.id, error: error.message }));
});

// Readiness marker emitted after the Worker is constructed. The harness waits
// for this worker's unique Redis client name rather than the log line.
console.log(JSON.stringify({ phase: "ready", workerName, queue: queueName, prefix }));

async function shutdown(signal: string) {
  console.log(JSON.stringify({ phase: "shutdown", signal }));
  await worker.close();
  if (connection.status !== "end") connection.disconnect();
  await pool.end();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
