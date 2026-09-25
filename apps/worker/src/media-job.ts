import type { Queue } from "bullmq";
import type { PrivacyRegion } from "@map/shared/contracts";
import { MEDIA_PROCESSING_JOB_NAME, mediaProcessingJobId } from "@map/shared/media-jobs";
import { config } from "./config";
import { pool } from "./db";
import { deleteObject, objectExists, readQuarantineObject, writeQuarantineObject, copyToPublic } from "./storage";
import { scanForMalware } from "./clamav";
import { processPrivacyImage } from "./privacy";

export async function processMediaJob(mediaId: string, attempt?: number): Promise<void> {
  // Atomically claim the asset: exactly one concurrent job can move it out of
  // a processable state. Jobs from superseded attempts (a newer retry already
  // bumped processing_attempt) and historical duplicate jobs both lose the
  // claim and skip. Legacy jobs without an attempt keep the previous behavior.
  const claim = await pool.query<{
    quarantine_object_key: string;
    privacy_report: { manualRegions?: PrivacyRegion[] } | null;
  }>(
    `UPDATE media_assets
     SET privacy_status = 'scanning', updated_at = now()
     WHERE id = $1
       AND deleted_at IS NULL
       AND privacy_status IN ('processing', 'failed')
       AND ($2::integer IS NULL OR processing_attempt = $2)
     RETURNING quarantine_object_key, privacy_report`,
    [mediaId, attempt ?? null]
  );
  const media = claim.rows[0];
  if (!media) {
    console.log(`skip media ${mediaId}: already claimed or not processable`);
    return;
  }

  const autoPublish = Boolean(config.PRIVACY_DETECTOR_URL);
  const publicKey = `media/${mediaId}.webp`;
  const publicThumbnailKey = `media/${mediaId}.thumb.webp`;

  try {
    const source = await readQuarantineObject(media.quarantine_object_key);
    await scanForMalware(source);

    await pool.query("UPDATE media_assets SET privacy_status = 'processing', updated_at = now() WHERE id = $1", [mediaId]);
    const manualRegions = media.privacy_report?.manualRegions ?? [];
    const processed = await processPrivacyImage(source, manualRegions);

    const processedKey = `processed/${mediaId}.webp`;
    const thumbnailKey = `processed/${mediaId}.thumb.webp`;
    await writeQuarantineObject(processedKey, processed.image, "image/webp");
    await writeQuarantineObject(thumbnailKey, processed.thumbnail, "image/webp");

    if (autoPublish) {
      await copyToPublic(processedKey, publicKey);
      await copyToPublic(thumbnailKey, publicThumbnailKey);
    }

    const report = {
      manualRegions: processed.manualRegions,
      detectorRegions: processed.detectorRegions,
      detectorConfigured: autoPublish,
      originalMetadataRemoved: true,
      serverReencoded: true,
      width: processed.width,
      height: processed.height,
      sha256: processed.sha256,
      perceptualHash: processed.perceptualHash,
      completedAt: new Date().toISOString()
    };

    await pool.query(
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
        mediaId,
        autoPublish ? "ready" : "manual_review",
        processedKey,
        thumbnailKey,
        autoPublish ? publicKey : null,
        processed.width,
        processed.height,
        processed.sha256,
        processed.perceptualHash,
        JSON.stringify(report),
        String(config.ORIGINAL_RETENTION_HOURS),
        autoPublish ? publicThumbnailKey : null
      ]
    );

    console.log(`media ${mediaId} processed as ${autoPublish ? "ready" : "manual_review"}`);
  } catch (error) {
    const message = error instanceof Error ? error.message.slice(0, 500) : "Unknown media processing error";
    await pool.query(
      `UPDATE media_assets
       SET privacy_status = 'failed', failure_code = $2,
           delete_after = now() + interval '7 days', updated_at = now()
       WHERE id = $1`,
      [mediaId, message]
    );
    if (autoPublish) {
      await Promise.allSettled([
        deleteObject(config.S3_PUBLIC_BUCKET, publicKey),
        deleteObject(config.S3_PUBLIC_BUCKET, publicThumbnailKey)
      ]);
    }
    throw error;
  }
}

export async function cleanupOriginalMedia(): Promise<void> {
  const abandoned = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE privacy_status = 'quarantined'
       AND created_at < now() - interval '24 hours'
       AND deleted_at IS NULL
     LIMIT 50`
  );
  for (const row of abandoned.rows) {
    try {
      if (await objectExists(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key)) {
        await deleteObject(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key);
      }
      await pool.query(
        `UPDATE media_assets
         SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE id = $1`,
        [row.id]
      );
    } catch (error) {
      console.error({ mediaId: row.id, error }, "failed to clean abandoned upload");
    }
  }

  const result = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE delete_after IS NOT NULL AND delete_after <= now()
       AND quarantine_object_key IS NOT NULL
       AND privacy_status IN ('ready', 'manual_review', 'rejected', 'failed', 'deleted')
     LIMIT 50`
  );
  for (const row of result.rows) {
    try {
      if (await objectExists(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key)) {
        await deleteObject(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key);
      }
      await pool.query("UPDATE media_assets SET delete_after = NULL, updated_at = now() WHERE id = $1", [row.id]);
    } catch (error) {
      console.error({ mediaId: row.id, error }, "failed to clean original media");
    }
  }
}

export async function markStaleFeatures(): Promise<void> {
  await pool.query(
    `UPDATE map_features
     SET needs_review_at = COALESCE(needs_review_at, now()), updated_at = now()
     WHERE status = 'published' AND freshness_expires_at <= now() AND needs_review_at IS NULL`
  );
}

/**
 * Re-enqueues media whose database state is stuck in an in-flight status.
 *
 * The deterministic job id makes this safe to run on every maintenance tick:
 * if the job for the current attempt is still queued or running, the row is
 * not orphaned and is left alone; if the retained terminal job would block a
 * re-add, it is removed first. Historical duplicate jobs created before
 * deterministic job ids are neutralized by the atomic claim in
 * processMediaJob: only one of them can win the status transition.
 */
export async function recoverStuckMedia(mediaQueue: Queue): Promise<string[]> {
  const stuck = await pool.query<{ id: string; processing_attempt: number }>(
    `SELECT id, processing_attempt FROM media_assets
     WHERE privacy_status IN ('scanning', 'processing')
       AND updated_at < now() - interval '20 minutes'
       AND deleted_at IS NULL
     LIMIT 50`
  );
  const recovered: string[] = [];
  for (const row of stuck.rows) {
    const jobId = mediaProcessingJobId(row.id, row.processing_attempt);
    try {
      const existing = await mediaQueue.getJob(jobId);
      if (existing) {
        const state = await existing.getState();
        if (state !== "completed" && state !== "failed" && state !== "unknown") {
          continue;
        }
        await existing.remove();
      }
      await pool.query(
        `UPDATE media_assets
         SET privacy_status = 'processing', failure_code = 'Recovered after worker timeout', updated_at = now()
         WHERE id = $1 AND privacy_status IN ('scanning', 'processing') AND deleted_at IS NULL`,
        [row.id]
      );
      await mediaQueue.add(MEDIA_PROCESSING_JOB_NAME, { mediaId: row.id, attempt: row.processing_attempt }, {
        jobId,
        removeOnComplete: 1000,
        removeOnFail: 1000
      });
      recovered.push(row.id);
    } catch (error) {
      console.error({ mediaId: row.id, error }, "failed to recover stuck media");
    }
  }
  return recovered;
}

export async function cleanupDeletedMediaObjects(): Promise<void> {
  const result = await pool.query<{
    id: string;
    quarantine_object_key: string;
    processed_object_key: string | null;
    thumbnail_object_key: string | null;
    public_object_key: string | null;
    public_thumbnail_object_key: string | null;
  }>(
    `SELECT id, quarantine_object_key, processed_object_key, thumbnail_object_key,
            public_object_key, public_thumbnail_object_key
     FROM media_assets
     WHERE privacy_status = 'deleted'
       AND (quarantine_object_key NOT LIKE 'deleted/%'
         OR processed_object_key IS NOT NULL
         OR thumbnail_object_key IS NOT NULL
         OR public_object_key IS NOT NULL
         OR public_thumbnail_object_key IS NOT NULL)
     LIMIT 50`
  );

  for (const item of result.rows) {
    try {
      const removals: Array<Promise<void>> = [];
      if (!item.quarantine_object_key.startsWith("deleted/")) {
        removals.push(deleteObject(config.S3_QUARANTINE_BUCKET, item.quarantine_object_key));
      }
      if (item.processed_object_key) removals.push(deleteObject(config.S3_QUARANTINE_BUCKET, item.processed_object_key));
      if (item.thumbnail_object_key) removals.push(deleteObject(config.S3_QUARANTINE_BUCKET, item.thumbnail_object_key));
      if (item.public_object_key) removals.push(deleteObject(config.S3_PUBLIC_BUCKET, item.public_object_key));
      if (item.public_thumbnail_object_key) removals.push(deleteObject(config.S3_PUBLIC_BUCKET, item.public_thumbnail_object_key));
      await Promise.all(removals);

      await pool.query(
        `UPDATE media_assets
         SET quarantine_object_key = $2,
             processed_object_key = NULL,
             thumbnail_object_key = NULL,
             public_object_key = NULL,
             public_thumbnail_object_key = NULL,
             delete_after = NULL,
             updated_at = now()
         WHERE id = $1`,
        [item.id, `deleted/${item.id}.object`]
      );
    } catch (error) {
      console.error({ mediaId: item.id, error }, "failed to clean deleted media objects");
    }
  }
}

export async function markUnreferencedMediaDeleted(): Promise<void> {
  await pool.query(
    `UPDATE media_assets ma
     SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
     WHERE ma.deleted_at IS NULL
       AND ma.created_at < now() - interval '7 days'
       AND NOT EXISTS (
         SELECT 1 FROM revision_media rm WHERE rm.media_id = ma.id
       )`
  );
}
