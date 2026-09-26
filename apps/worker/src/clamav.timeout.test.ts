import { spawn } from "node:child_process";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

// Verifies the real ClamAV TCP client honours its read timeout. A silent TCP
// listener accepts but never replies; the client (run in an isolated child
// process so config is parsed with the ephemeral port) must reject quickly.
const WORKER_DIR = fileURLToPath(new URL(".", import.meta.url));
const CLIENT_SOURCE = `
import net from "node:net";
const silent = net.createServer((socket) => socket.on("error", () => undefined));
silent.listen(0, "127.0.0.1", async () => {
  process.env.DATABASE_URL = "x";
  process.env.S3_ENDPOINT = "http://localhost:9000";
  process.env.S3_PUBLIC_ENDPOINT = "http://localhost:9000";
  process.env.S3_ACCESS_KEY = "a";
  process.env.S3_SECRET_KEY = "b";
  process.env.S3_QUARANTINE_BUCKET = "q";
  process.env.S3_PUBLIC_BUCKET = "p";
  process.env.CLAMAV_ENABLED = "true";
  process.env.CLAMAV_HOST = "127.0.0.1";
  process.env.CLAMAV_PORT = String(silent.address().port);
  process.env.CLAMAV_TIMEOUT_MS = "50";
  process.env.PRIVACY_DETECTOR_URL = "";
  const { scanForMalware } = await import("${WORKER_DIR}clamav.ts");
  const started = Date.now();
  try {
    await scanForMalware(Buffer.from("hello"));
    console.log("RESULT resolved");
  } catch (error) {
    console.log("RESULT rejected", Date.now() - started, error.message);
  } finally {
    silent.close();
    process.exit(0);
  }
});
`;

function runClient(): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", "tsx", "--eval", CLIENT_SOURCE], {
      cwd: WORKER_DIR,
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("client subprocess timed out"));
    }, 10_000);
    child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
    child.on("error", reject);
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) reject(new Error(`client exited ${code}: ${stderr}`));
      else resolve(stdout);
    });
  });
}

describe("ClamAV client scan timeout", () => {
  it("rejects quickly with a timed out error against a silent scanner", async () => {
    // Sanity check the host can create servers; mainly guards sandbox quirks.
    const probe = net.createServer();
    await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
    await new Promise<void>((resolve) => probe.close(() => resolve()));

    const output = await runClient();
    const line = output.split("\n").find((entry) => entry.startsWith("RESULT"));
    expect(line, `missing RESULT in output:\n${output}`).toBeDefined();
    const [, outcome, elapsedMs, ...rest] = line!.split(" ");
    expect(outcome).toBe("rejected");
    expect(rest.join(" ")).toMatch(/timed out/);
    expect(Number(elapsedMs)).toBeLessThan(2_000);
  }, 15_000);
});
