import type { PrivacyRegion } from "@map/shared/contracts";
import {
  MEDIA_AUDIT_ACTIONS,
  type MediaAuditEvent,
  type MediaProcessorDeps,
  type MediaRow
} from "./types";
import { assertTransition } from "./state-machine";

/**
 * Run one media processing attempt against injected ports.
 *
 * The ordering of effects deliberately matches the production pipeline so the
 * fault-injection tests exercise real control flow:
 *
 *   processing -> scanning -> (read quarantine, malware scan)
 *              -> processing -> (blur/re-encode, persist derivations)
 *              -> manual_review | ready
 *
 * On any error the row is marked `failed`, the audit log records the failure
 * and any already-copied public objects are rolled back. The function rethrows
 * so queue layers (BullMQ, tests) can observe the failure and replay later.
 */
export async function processMediaWith(deps: MediaProcessorDeps, mediaId: string): Promise<MediaRow["privacy_status"] | "skipped"> {
  const media = await deps.getMedia(mediaId);
  if (!media) throw new Error("Media record not found");
  if (!["processing", "failed"].includes(media.privacy_status)) {
    deps.log(`skip media ${mediaId}: status=${media.privacy_status}`);
    return "skipped";
  }

  const fromStatus = media.privacy_status;
  const publicKey = `media/${mediaId}.webp`;
  const publicThumbnailKey = `media/${mediaId}.thumb.webp`;

  try {
    assertTransition(fromStatus, "scanning");
    await deps.markScanning(mediaId);

    const source = await deps.readQuarantine(media.quarantine_object_key);
    await deps.scanMalware(source);

    assertTransition("scanning", "processing");
    await deps.markProcessingAfterScan(mediaId);

    const manualRegions: PrivacyRegion[] = media.privacy_report?.manualRegions ?? [];
    const processed = await deps.processImage(source, manualRegions);

    const processedKey = `processed/${mediaId}.webp`;
    const thumbnailKey = `processed/${mediaId}.thumb.webp`;
    await deps.writeQuarantine(processedKey, processed.image, "image/webp");
    await deps.writeQuarantine(thumbnailKey, processed.thumbnail, "image/webp");

    if (deps.autoPublish) {
      await deps.copyToPublic(processedKey, publicKey);
      await deps.copyToPublic(thumbnailKey, publicThumbnailKey);
    }

    const finalStatus = deps.autoPublish ? "ready" : "manual_review";
    assertTransition("processing", finalStatus);

    const report = {
      manualRegions: processed.manualRegions,
      detectorRegions: processed.detectorRegions,
      detectorConfigured: deps.autoPublish,
      originalMetadataRemoved: true,
      serverReencoded: true,
      width: processed.width,
      height: processed.height,
      sha256: processed.sha256,
      perceptualHash: processed.perceptualHash,
      completedAt: deps.now().toISOString()
    };

    const audit: MediaAuditEvent = {
      action: MEDIA_AUDIT_ACTIONS.succeeded,
      resourceType: "media",
      resourceId: mediaId,
      metadata: {
        status: finalStatus,
        autoPublish: deps.autoPublish,
        sha256: processed.sha256
      }
    };

    await deps.finalize({
      mediaId,
      status: finalStatus,
      processedKey,
      thumbnailKey,
      publicKey: deps.autoPublish ? publicKey : null,
      publicThumbnailKey: deps.autoPublish ? publicThumbnailKey : null,
      width: processed.width,
      height: processed.height,
      sha256: processed.sha256,
      perceptualHash: processed.perceptualHash,
      report,
      retentionHours: deps.retentionHours,
      completedAt: deps.now().toISOString(),
      audit
    });

    deps.log(`media ${mediaId} processed as ${finalStatus}`);
    return finalStatus;
  } catch (error) {
    const failureCode = error instanceof Error ? error.message.slice(0, 500) : "Unknown media processing error";
    const audit: MediaAuditEvent = {
      action: MEDIA_AUDIT_ACTIONS.failed,
      resourceType: "media",
      resourceId: mediaId,
      metadata: { failureCode, fromStatus, autoPublish: deps.autoPublish }
    };
    await deps.fail({ mediaId, failureCode, audit });

    if (deps.autoPublish) {
      await deps.deletePublicObjects({ publicKey, publicThumbnailKey });
    }
    throw error;
  }
}
