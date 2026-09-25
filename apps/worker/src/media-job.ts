import type { PrivacyRegion } from "@map/shared/contracts";
import { config } from "./config";
import { pool } from "./db";
import { deleteObject, objectExists, readQuarantineObject, writeQuarantineObject, copyToPublic } from "./storage";
import { scanForMalware } from "./clamav";
import { processPrivacyImage } from "./privacy";

/**
 * 媒体处理作业。attempt 为作业所声明的处理代次：
 * - 与 media_assets.processing_attempt 不一致的作业是重试/恢复后残留的旧代次作业，直接跳过；
 * - 所有状态推进都使用带 privacy_status / processing_attempt 条件的守卫式 UPDATE，
 *   即使旧作业漏过入口检查，也无法覆盖新代次的状态与产物。
 */
export async function processMediaJob(mediaId: string, attempt?: number): Promise<void> {
  const result = await pool.query<{
    id: string;
    privacy_status: string;
    processing_attempt: number;
    quarantine_object_key: string;
    privacy_report: { manualRegions?: PrivacyRegion[] } | null;
  }>(
    `SELECT id, privacy_status, processing_attempt, quarantine_object_key, privacy_report
     FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
    [mediaId]
  );
  const media = result.rows[0];
  if (!media) throw new Error("Media record not found");

  // 旧幂等键格式的遗留作业没有代次，视为当前代次；启动清理会移除这些作业。
  const jobAttempt = typeof attempt === "number" ? attempt : media.processing_attempt;
  if (jobAttempt !== media.processing_attempt) {
    console.log(`skip media ${mediaId}: stale attempt job=${jobAttempt} current=${media.processing_attempt}`);
    return;
  }
  if (media.privacy_status !== "processing") {
    console.log(`skip media ${mediaId}: status=${media.privacy_status}`);
    return;
  }

  const autoPublish = Boolean(config.PRIVACY_DETECTOR_URL);
  const publicKey = `media/${mediaId}.webp`;
  const publicThumbnailKey = `media/${mediaId}.thumb.webp`;

  // 守卫式进入 scanning：只允许本代次且仍在 processing 的行。若行已被更新的
  // 代次/状态（例如恢复流程重新入队）接管，本作业放弃执行。
  try {
    const claimed = await pool.query(
      `UPDATE media_assets SET privacy_status = 'scanning', updated_at = now()
       WHERE id = $1 AND processing_attempt = $2 AND privacy_status = 'processing'`,
      [mediaId, jobAttempt]
    );
    if (claimed.rowCount === 0) {
      console.log(`skip media ${mediaId}: superseded at scan claim (attempt=${jobAttempt})`);
      return;
    }

    const source = await readQuarantineObject(media.quarantine_object_key);
    await scanForMalware(source);

    const imageProcessing = await pool.query(
      `UPDATE media_assets SET privacy_status = 'processing', updated_at = now()
       WHERE id = $1 AND processing_attempt = $2 AND privacy_status = 'scanning'`,
      [mediaId, jobAttempt]
    );
    if (imageProcessing.rowCount === 0) {
      console.log(`skip media ${mediaId}: superseded during scan (attempt=${jobAttempt})`);
      return;
    }

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
      completedAt: new Date().toISOString(),
      attempt: jobAttempt
    };

    // 终态写入只接受本代次且仍处于 processing 的行，杜绝旧作业覆盖新结果。
    const finalized = await pool.query(
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
       WHERE id = $1 AND processing_attempt = $13 AND privacy_status = 'processing'`,
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
        autoPublish ? publicThumbnailKey : null,
        jobAttempt
      ]
    );
    if (finalized.rowCount === 0) {
      console.log(`skip media ${mediaId}: superseded before finalize (attempt=${jobAttempt})`);
      if (autoPublish) {
        await Promise.allSettled([
          deleteObject(config.S3_PUBLIC_BUCKET, publicKey),
          deleteObject(config.S3_PUBLIC_BUCKET, publicThumbnailKey)
        ]);
      }
      return;
    }

    console.log(`media ${mediaId} processed as ${autoPublish ? "ready" : "manual_review"} (attempt=${jobAttempt})`);
  } catch (error) {
    const message = error instanceof Error ? error.message.slice(0, 500) : "Unknown media processing error";
    const failed = await pool.query(
      `UPDATE media_assets
       SET privacy_status = 'failed', failure_code = $3,
           delete_after = now() + interval '7 days', updated_at = now()
       WHERE id = $1 AND processing_attempt = $2 AND privacy_status IN ('scanning', 'processing')`,
      [mediaId, jobAttempt, message]
    );
    if (failed.rowCount === 0) {
      // 行已被更新的代次接管，旧作业不得改写其状态，也不得清理新代次的公开对象。
      console.log(`skip failure write for media ${mediaId}: superseded (attempt=${jobAttempt})`);
      throw error;
    }
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
 * 恢复卡死（worker 崩溃/超时）的媒体作业。每恢复一次处理代次单调递增，
 * 并写入系统审计，旧代次的残留作业之后会被 processMediaJob 的代次守卫跳过。
 */
export async function recoverStuckMedia(): Promise<Array<{ id: string; attempt: number }>> {
  const result = await pool.query<{ id: string; attempt: number }>(
    `UPDATE media_assets
     SET privacy_status = 'processing',
         processing_attempt = processing_attempt + 1,
         failure_code = NULL,
         updated_at = now()
     WHERE privacy_status IN ('scanning', 'processing')
       AND updated_at < now() - interval '20 minutes'
       AND deleted_at IS NULL
     RETURNING id, processing_attempt AS attempt`
  );
  for (const row of result.rows) {
    await pool.query(
      `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
       VALUES (NULL, 'media.processing_recovered', 'media', $1, $2::jsonb)`,
      [row.id, JSON.stringify({ attempt: row.attempt, reason: "Recovered after worker timeout" })]
    );
  }
  return result.rows;
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
