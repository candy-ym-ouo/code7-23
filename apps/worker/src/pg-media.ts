import type { PrivacyRegion } from "@map/shared/contracts";
import { config } from "./config";
import { pool } from "./db";
import { deleteObject, readQuarantineObject, writeQuarantineObject, copyToPublic } from "./storage";
import { scanForMalware } from "./clamav";
import { processPrivacyImage } from "./privacy";
import { recordMediaAudit } from "./audit";
import { MEDIA_AUDIT_ACTIONS, type MediaProcessorDeps, type MediaRow } from "./media/types";

/**
 * Production wiring for the media orchestrator. Every state change and its
 * audit row are committed in one transaction; object storage effects are the
 * retry-able side effects (derivations are content-addressed by media id and
 * overwritten on replay, public objects are rolled back on failure).
 */
export function createMediaDeps(): MediaProcessorDeps {
  return {
    autoPublish: Boolean(config.PRIVACY_DETECTOR_URL),
    retentionHours: config.ORIGINAL_RETENTION_HOURS,
    now: () => new Date(),
    log: (message) => console.log(message),

    async getMedia(mediaId: string): Promise<MediaRow | null> {
      const result = await pool.query<MediaRow>(
        `SELECT id, privacy_status, quarantine_object_key,
                processed_object_key, thumbnail_object_key,
                public_object_key, public_thumbnail_object_key,
                privacy_report, failure_code
         FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
        [mediaId]
      );
      return result.rows[0] ?? null;
    },

    markScanning(mediaId: string) {
      return pool
        .query(
          "UPDATE media_assets SET privacy_status = 'scanning', updated_at = now() WHERE id = $1",
          [mediaId]
        )
        .then(() => undefined);
    },

    readQuarantine: (key) => readQuarantineObject(key),
    scanMalware: (source) => scanForMalware(source),

    markProcessingAfterScan(mediaId: string) {
      return pool
        .query(
          "UPDATE media_assets SET privacy_status = 'processing', updated_at = now() WHERE id = $1",
          [mediaId]
        )
        .then(() => undefined);
    },

    processImage: (source: Buffer, manualRegions: PrivacyRegion[]) =>
      processPrivacyImage(source, manualRegions),

    writeQuarantine: (key, body, contentType) =>
      writeQuarantineObject(key, body, contentType),

    copyToPublic: (processedKey, publicKey) => copyToPublic(processedKey, publicKey),

    async finalize(input) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `UPDATE media_assets
           SET privacy_status = $2,
               processed_object_key = $3,
               thumbnail_object_key = $4,
               public_object_key = $5,
               public_thumbnail_object_key = $12,
               width = $6,
               height = $7,
               sha256 = $8,
               perceptual_hash = $9,
               privacy_report = $10::jsonb,
               failure_code = NULL,
               processed_at = now(),
               delete_after = now() + ($11::text || ' hours')::interval,
               updated_at = now()
           WHERE id = $1`,
          [
            input.mediaId,
            input.status,
            input.processedKey,
            input.thumbnailKey,
            input.publicKey,
            input.width,
            input.height,
            input.sha256,
            input.perceptualHash,
            JSON.stringify(input.report),
            String(input.retentionHours),
            input.publicThumbnailKey
          ]
        );
        await recordMediaAudit(client, input.audit);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    },

    async fail(input) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `UPDATE media_assets
           SET privacy_status = 'failed', failure_code = $2,
               delete_after = now() + interval '7 days', updated_at = now()
           WHERE id = $1`,
          [input.mediaId, input.failureCode]
        );
        await recordMediaAudit(client, input.audit);
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      } finally {
        client.release();
      }
    },

    deletePublicObjects({ publicKey, publicThumbnailKey }) {
      return Promise.allSettled([
        deleteObject(config.S3_PUBLIC_BUCKET, publicKey),
        deleteObject(config.S3_PUBLIC_BUCKET, publicThumbnailKey)
      ]).then(() => undefined);
    }
  };
}

/**
 * Recover media rows that died while `scanning`/`processing` (worker timeout
 * or crash). The status reset and the recovery audit rows commit in one
 * statement, so a crash during recovery cannot leave a row without an audit
 * trail. Returns the media ids that were requeued.
 */
export async function recoverStuckMedia(): Promise<string[]> {
  const result = await pool.query<{ id: string }>(
    `WITH recovered AS (
       UPDATE media_assets
       SET privacy_status = 'processing',
           failure_code = 'Recovered after worker timeout',
           updated_at = now()
       WHERE privacy_status IN ('scanning', 'processing')
         AND updated_at < now() - interval '20 minutes'
         AND deleted_at IS NULL
       RETURNING id
     )
     INSERT INTO audit_logs(action, resource_type, resource_id, metadata)
     SELECT $1, 'media', id, jsonb_build_object('reason', 'worker_timeout')
     FROM recovered
     RETURNING resource_id AS id`,
    [MEDIA_AUDIT_ACTIONS.recovered]
  );
  return result.rows.map((row) => String(row.id));
}
