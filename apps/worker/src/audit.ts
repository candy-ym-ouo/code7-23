import { pool } from "./db";

export async function recordMediaAudit(input: {
  actorId?: string | null;
  action: string;
  mediaId: string;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  await pool.query(
    `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
     VALUES ($1, $2, 'media', $3, $4::jsonb)`,
    [input.actorId ?? null, input.action, input.mediaId, JSON.stringify(input.metadata ?? {})]
  );
}
