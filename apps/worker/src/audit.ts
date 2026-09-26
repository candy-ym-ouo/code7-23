import type { MediaAuditEvent } from "./media/types";

type QueryRunner = {
  query(text: string, params?: unknown[]): Promise<unknown>;
};

/**
 * Insert a worker lifecycle audit row. Always called inside the same
 * transaction as the state change it describes, so status and audit history
 * commit atomically (docs/项目文档.md §12 一致性).
 */
export async function recordMediaAudit(runner: QueryRunner, event: MediaAuditEvent): Promise<void> {
  await runner.query(
    `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
     VALUES (NULL, $1, $2, $3, $4::jsonb)`,
    [event.action, event.resourceType, event.resourceId, JSON.stringify(event.metadata)]
  );
}
