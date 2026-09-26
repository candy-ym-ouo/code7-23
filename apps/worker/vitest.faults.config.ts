import { defineConfig } from "vitest/config";

// Failure-injection / integration suite.
// Requires real backing services (PostgreSQL/PostGIS, Redis, S3/MinIO); the
// ClamAV scanner itself is replaced by an in-suite fake zINSTREAM server. Run:
//   pnpm --filter @map/worker test:integration
// Connection details come from the same env vars as the worker (DATABASE_URL,
// REDIS_URL, S3_ENDPOINT, S3_ACCESS_KEY, ...) with local-dev defaults.
export default defineConfig({
  test: {
    include: ["src/faults/**/*.test.ts"],
    hookTimeout: 60_000,
    testTimeout: 60_000,
    fileParallelism: false,
    pool: "forks",
    env: {
      NODE_ENV: "test",
      DATABASE_URL: process.env.DATABASE_URL ?? "postgres://map:map@localhost:5432/map",
      REDIS_URL: process.env.REDIS_URL ?? "redis://localhost:6379/0",
      S3_ENDPOINT: process.env.S3_ENDPOINT ?? "http://localhost:9000",
      S3_PUBLIC_ENDPOINT: process.env.S3_PUBLIC_ENDPOINT ?? "http://localhost:9000",
      S3_REGION: process.env.S3_REGION ?? "us-east-1",
      S3_ACCESS_KEY: process.env.S3_ACCESS_KEY ?? "minioadmin",
      S3_SECRET_KEY: process.env.S3_SECRET_KEY ?? "minioadmin",
      S3_QUARANTINE_BUCKET: process.env.S3_QUARANTINE_BUCKET ?? "quarantine-test",
      S3_PUBLIC_BUCKET: process.env.S3_PUBLIC_BUCKET ?? "public-test",
      PRIVACY_DETECTOR_URL: "",
      CLAMAV_ENABLED: "true",
      CLAMAV_HOST: "127.0.0.1",
      CLAMAV_PORT: process.env.CLAMAV_PORT ?? "3310",
      CLAMAV_TIMEOUT_MS: "750",
      ORIGINAL_RETENTION_HOURS: "24"
    }
  }
});
