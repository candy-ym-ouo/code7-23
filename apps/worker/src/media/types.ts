import type { PrivacyRegion } from "@map/shared/contracts";

/**
 * Lifecycle states of a media asset, mirroring the `media_status` Postgres enum
 * in packages/db/migrations/0001_init.sql.
 */
export const MEDIA_STATUSES = [
  "quarantined",
  "scanning",
  "processing",
  "manual_review",
  "ready",
  "rejected",
  "failed",
  "deleted"
] as const;

export type MediaStatus = (typeof MEDIA_STATUSES)[number];

/** Audit actions emitted by the worker-side media lifecycle. */
export const MEDIA_AUDIT_ACTIONS = {
  succeeded: "media.processing_succeeded",
  failed: "media.processing_failed",
  recovered: "media.recovered"
} as const;

export type MediaAuditEvent = {
  action: (typeof MEDIA_AUDIT_ACTIONS)[keyof typeof MEDIA_AUDIT_ACTIONS];
  resourceType: "media";
  resourceId: string;
  metadata: Record<string, unknown>;
};

export type MediaPrivacyReport = {
  manualRegions?: PrivacyRegion[];
} | null;

export type MediaRow = {
  id: string;
  privacy_status: MediaStatus;
  quarantine_object_key: string;
  processed_object_key: string | null;
  thumbnail_object_key: string | null;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
  width: number | null;
  height: number | null;
  sha256: string | null;
  perceptual_hash: string | null;
  privacy_report: MediaPrivacyReport;
  failure_code: string | null;
};

export type ProcessedImage = {
  image: Buffer;
  thumbnail: Buffer;
  width: number;
  height: number;
  sha256: string;
  perceptualHash: string;
  detectorRegions: PrivacyRegion[];
  manualRegions: PrivacyRegion[];
};

export type FinalizeMediaInput = {
  mediaId: string;
  status: Extract<MediaStatus, "ready" | "manual_review">;
  processedKey: string;
  thumbnailKey: string;
  publicKey: string | null;
  publicThumbnailKey: string | null;
  width: number;
  height: number;
  sha256: string;
  perceptualHash: string;
  report: Record<string, unknown>;
  retentionHours: number;
  completedAt: string;
  audit: MediaAuditEvent;
};

export type FailMediaInput = {
  mediaId: string;
  failureCode: string;
  audit: MediaAuditEvent;
};

export type PublicObjectKeys = {
  publicKey: string;
  publicThumbnailKey: string;
};

/**
 * Ports the media processing orchestrator needs. Production wires these to
 * PostgreSQL/S3/ClamAV/Sharp (see pg-media.ts); fault-injection tests wire
 * deterministic in-memory fakes through the exact same surface.
 */
export type MediaProcessorDeps = {
  autoPublish: boolean;
  retentionHours: number;
  now: () => Date;
  getMedia(mediaId: string): Promise<MediaRow | null>;
  markScanning(mediaId: string): Promise<void>;
  readQuarantine(key: string): Promise<Buffer>;
  scanMalware(source: Buffer): Promise<void>;
  markProcessingAfterScan(mediaId: string): Promise<void>;
  processImage(source: Buffer, manualRegions: PrivacyRegion[]): Promise<ProcessedImage>;
  writeQuarantine(key: string, body: Buffer, contentType: string): Promise<void>;
  copyToPublic(processedKey: string, publicKey: string): Promise<void>;
  finalize(input: FinalizeMediaInput): Promise<void>;
  fail(input: FailMediaInput): Promise<void>;
  deletePublicObjects(keys: PublicObjectKeys): Promise<void>;
  log(message: string): void;
};
