import { transaction } from "./db";
import { recordAudit } from "./audit";

export type MediaProcessingClaim = {
  attempt: number;
  previousStatus: string;
};

/**
 * Atomically moves a media asset into `processing` and records the audit
 * event in the same transaction.
 *
 * The conditional UPDATE ... RETURNING is the state-machine guard: exactly
 * one concurrent request can move an asset out of `fromStatuses`. Every
 * successful claim increments `processing_attempt`, which becomes the
 * idempotency key for the queue job derived from it.
 *
 * Returns null when the asset is not currently in one of `fromStatuses`
 * (for example a concurrent retry already claimed it).
 */
export async function claimMediaProcessing(input: {
  mediaId: string;
  actorId: string;
  fromStatuses: string[];
  action: string;
  metadata?: Record<string, unknown>;
  privacyReport?: unknown;
}): Promise<MediaProcessingClaim | null> {
  return transaction(async (client) => {
    const result = await client.query<{ processing_attempt: number; previous_status: string }>(
      `WITH prior AS (
         SELECT id, privacy_status FROM media_assets WHERE id = $1 AND deleted_at IS NULL
       )
       UPDATE media_assets AS m
       SET privacy_status = 'processing',
           failure_code = NULL,
           processing_attempt = m.processing_attempt + 1,
           privacy_report = COALESCE($3::jsonb, m.privacy_report),
           updated_at = now()
       FROM prior
       WHERE m.id = prior.id
         AND m.privacy_status = ANY($2::media_status[])
       RETURNING m.processing_attempt, prior.privacy_status AS previous_status`,
      [
        input.mediaId,
        input.fromStatuses,
        input.privacyReport === undefined ? null : JSON.stringify(input.privacyReport)
      ]
    );
    const row = result.rows[0];
    if (!row) return null;
    await recordAudit(client, {
      actorId: input.actorId,
      action: input.action,
      resourceType: "media",
      resourceId: input.mediaId,
      metadata: {
        ...input.metadata,
        attempt: row.processing_attempt,
        previousStatus: row.previous_status
      }
    });
    return { attempt: row.processing_attempt, previousStatus: row.previous_status };
  });
}

/**
 * Best-effort rollback when the queue cannot accept the job right after a
 * claim. Conditional on the exact (status, attempt) pair produced by the
 * claim, so it can never clobber a concurrent transition such as a delete,
 * a newer retry or a maintenance recovery.
 */
export async function failMediaProcessingClaim(input: {
  mediaId: string;
  attempt: number;
  actorId: string;
  failureCode: string;
}): Promise<void> {
  await transaction(async (client) => {
    const result = await client.query(
      `UPDATE media_assets
       SET privacy_status = 'failed', failure_code = $3, updated_at = now()
       WHERE id = $1
         AND deleted_at IS NULL
         AND privacy_status = 'processing'
         AND processing_attempt = $2`,
      [input.mediaId, input.attempt, input.failureCode]
    );
    if (!result.rowCount) return;
    await recordAudit(client, {
      actorId: input.actorId,
      action: "media.processing_enqueue_failed",
      resourceType: "media",
      resourceId: input.mediaId,
      metadata: { attempt: input.attempt, failureCode: input.failureCode }
    });
  });
}
