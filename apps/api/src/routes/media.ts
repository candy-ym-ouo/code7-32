import { randomUUID } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { mediaUploadCompleteSchema, mediaUploadInitSchema } from "@map/shared/contracts";
import { config } from "../config";
import { query, transaction } from "../db";
import { AppError, conflict, forbidden, notFound } from "../errors";
import { requireAuth, requireModerator, requireVerifiedContributor } from "../auth";
import {
  createPreviewUrl,
  createUploadUrl,
  deleteObject,
  getQuarantineMetadata,
  publishMediaObject,
  publicMediaUrl
} from "../storage";
import { enqueueMediaProcessing } from "../queue";
import { recordAudit } from "../audit";

function extensionForMime(mime: string) {
  if (mime === "image/jpeg") return "jpg";
  if (mime === "image/png") return "png";
  return "webp";
}

function mediaResponse(row: {
  id: string;
  privacy_status: string;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
  privacy_report: unknown;
  failure_code: string | null;
  created_at: Date;
  processed_at: Date | null;
}) {
  return {
    id: row.id,
    status: row.privacy_status,
    url: row.privacy_status === "ready" ? publicMediaUrl(row.public_object_key) : null,
    thumbnailUrl: row.privacy_status === "ready" ? publicMediaUrl(row.public_thumbnail_object_key) : null,
    privacyReport: row.privacy_report,
    failureCode: row.failure_code,
    createdAt: row.created_at,
    processedAt: row.processed_at
  };
}

export async function mediaRoutes(app: FastifyInstance) {
  app.post("/media/uploads", { preHandler: requireVerifiedContributor }, async (request, reply) => {
    const input = mediaUploadInitSchema.parse(request.body);
    if (input.byteSize > config.MEDIA_MAX_BYTES) {
      throw new AppError(400, "VALIDATION_FAILED", `File exceeds ${config.MEDIA_MAX_BYTES} bytes`);
    }
    const id = randomUUID();
    const key = `quarantine/${request.user!.id}/${id}.${extensionForMime(input.mimeType)}`;
    await query(
      `INSERT INTO media_assets(id, owner_id, original_filename, mime_type, byte_size, quarantine_object_key)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [id, request.user!.id, input.filename, input.mimeType, input.byteSize, key]
    );
    const uploadUrl = await createUploadUrl(key, input.mimeType);
    return reply.code(201).send({ id, uploadUrl, expiresInSeconds: 600 });
  });

  app.post("/media/uploads/:id/complete", { preHandler: requireVerifiedContributor }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const input = mediaUploadCompleteSchema.parse(request.body);
    const result = await query<{
      id: string;
      owner_id: string;
      byte_size: string;
      mime_type: string;
      quarantine_object_key: string;
      privacy_status: string;
    }>(
      `SELECT id, owner_id, byte_size, mime_type, quarantine_object_key, privacy_status
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.owner_id !== request.user!.id) throw forbidden();
    if (media.privacy_status !== "quarantined") throw conflict("Media upload was already completed");

    let metadata;
    try {
      metadata = await getQuarantineMetadata(media.quarantine_object_key);
    } catch {
      throw new AppError(409, "CONFLICT", "Uploaded object was not found in quarantine storage");
    }
    const actualBytes = Number(metadata.ContentLength ?? 0);
    const actualContentType = metadata.ContentType?.split(";")[0]?.trim();
    if (!actualBytes || actualBytes > config.MEDIA_MAX_BYTES || actualBytes !== Number(media.byte_size)) {
      throw new AppError(400, "VALIDATION_FAILED", "Uploaded object size does not match the declared size");
    }
    if (actualContentType && actualContentType !== media.mime_type) {
      throw new AppError(400, "VALIDATION_FAILED", "Uploaded object content type does not match the declared type");
    }

    const attempt = await transaction(async (client) => {
      // 条件 UPDATE 是状态机权威：并发 complete 只有一个事务能从 quarantined 抢占成功，
      // 行锁串行化竞争，落选者得到 0 行。
      const claim = await client.query<{ attempt: number }>(
        `UPDATE media_assets
         SET privacy_status = 'processing',
             privacy_report = $2::jsonb,
             failure_code = NULL,
             updated_at = now()
         WHERE id = $1 AND privacy_status = 'quarantined' AND deleted_at IS NULL
         RETURNING processing_attempt AS attempt`,
        [params.id, JSON.stringify({
          manualRegions: input.privacyRegions,
          containsPeopleOrPlates: input.containsPeopleOrPlates,
          rightsConfirmedAt: new Date().toISOString(),
          detector: "pending"
        })]
      );
      if (claim.rowCount === 0) return null;
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.processing_requested",
        resourceType: "media",
        resourceId: params.id,
        metadata: { regionCount: input.privacyRegions.length, attempt: claim.rows[0]!.attempt }
      });
      return claim.rows[0]!.attempt;
    });
    if (attempt === null) throw conflict("Media upload was already completed");

    try {
      await enqueueMediaProcessing(params.id, attempt);
    } catch (error) {
      await query(
        `UPDATE media_assets
         SET privacy_status = 'failed', failure_code = 'QUEUE_UNAVAILABLE', updated_at = now()
         WHERE id = $1 AND processing_attempt = $2`,
        [params.id, attempt]
      );
      throw new AppError(503, "QUEUE_UNAVAILABLE", "Media processing queue is unavailable. Retry later.");
    }
    return { status: "processing", attempt };
  });

  app.get("/media/:id", { preHandler: requireAuth }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string; owner_id: string; privacy_status: string; public_object_key: string | null;
      public_thumbnail_object_key: string | null; privacy_report: unknown; failure_code: string | null;
      created_at: Date; processed_at: Date | null;
    }>(
      `SELECT id, owner_id, privacy_status, public_object_key, public_thumbnail_object_key,
              privacy_report, failure_code, created_at, processed_at
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const row = result.rows[0];
    if (!row) throw notFound("Media not found");
    if (row.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();
    return mediaResponse(row);
  });

  app.post("/media/:id/retry", { preHandler: requireVerifiedContributor }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{ owner_id: string; privacy_status: string }>(
      "SELECT owner_id, privacy_status FROM media_assets WHERE id = $1 AND deleted_at IS NULL",
      [params.id]
    );
    const row = result.rows[0];
    if (!row) throw notFound("Media not found");
    if (row.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();
    if (!["failed", "rejected"].includes(row.privacy_status)) {
      throw conflict("Only failed media can be retried");
    }

    // 条件 UPDATE + 审计在同一事务中完成状态转换。并发重试由行锁串行化：
    // 只有一个请求能把 failed/rejected 推进到 processing 并拿到新的处理代次。
    const attempt = await transaction(async (client) => {
      const claim = await client.query<{ attempt: number }>(
        `UPDATE media_assets
         SET privacy_status = 'processing',
             processing_attempt = processing_attempt + 1,
             failure_code = NULL,
             updated_at = now()
         WHERE id = $1 AND privacy_status IN ('failed', 'rejected') AND deleted_at IS NULL
         RETURNING processing_attempt AS attempt`,
        [params.id]
      );
      if (claim.rowCount === 0) return null;
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.retried",
        resourceType: "media",
        resourceId: params.id,
        metadata: { attempt: claim.rows[0]!.attempt, previousStatus: row.privacy_status }
      });
      return claim.rows[0]!.attempt;
    });
    if (attempt === null) throw conflict("Media is already being processed");

    try {
      // 幂等键与处理代次绑定：并发/重复重试同代次只产生一个 BullMQ 作业
      await enqueueMediaProcessing(params.id, attempt);
    } catch (error) {
      // 仅当状态仍属于本次代次时回滚，避免覆盖更新的状态（例如已被恢复流程接管）
      await query(
        `UPDATE media_assets
         SET privacy_status = 'failed', failure_code = 'QUEUE_UNAVAILABLE', updated_at = now()
         WHERE id = $1 AND processing_attempt = $2 AND privacy_status = 'processing'`,
        [params.id, attempt]
      );
      throw new AppError(503, "QUEUE_UNAVAILABLE", "Media processing queue is unavailable. Retry later.");
    }
    return { status: "processing", attempt };
  });

  app.get("/media/:id/preview", { preHandler: requireModerator }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{ processed_object_key: string | null; privacy_status: string }>(
      "SELECT processed_object_key, privacy_status FROM media_assets WHERE id = $1 AND deleted_at IS NULL",
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (!media.processed_object_key) throw conflict("Processed preview is not available");
    await query(
      `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
       VALUES ($1, 'media.preview_viewed', 'media', $2, '{}'::jsonb)`,
      [request.user!.id, params.id]
    );
    return {
      status: media.privacy_status,
      url: await createPreviewUrl(media.processed_object_key),
      expiresInSeconds: 600
    };
  });

  app.post("/media/:id/privacy-approve", { preHandler: requireModerator }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string;
      privacy_status: string;
      processed_object_key: string | null;
      thumbnail_object_key: string | null;
      public_object_key: string | null;
      public_thumbnail_object_key: string | null;
    }>(
      `SELECT id, privacy_status, processed_object_key, thumbnail_object_key,
              public_object_key, public_thumbnail_object_key
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.privacy_status !== "manual_review" || !media.processed_object_key) {
      throw conflict("Media is not waiting for manual privacy approval");
    }

    const publicKey = `media/${params.id}.webp`;
    const thumbnailKey = `media/${params.id}.thumb.webp`;

    // 先在事务内做条件状态转换：并发批准只有一个能从 manual_review 抢占成功。
    const claimed = await transaction(async (client) => {
      const claim = await client.query(
        `UPDATE media_assets
         SET privacy_status = 'ready', updated_at = now()
         WHERE id = $1 AND privacy_status = 'manual_review' AND deleted_at IS NULL
         RETURNING id`,
        [params.id]
      );
      if (claim.rowCount === 0) return false;
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.privacy_approved",
        resourceType: "media",
        resourceId: params.id
      });
      return true;
    });
    if (!claimed) throw conflict("Media is not waiting for manual privacy approval");

    try {
      await publishMediaObject(media.processed_object_key, publicKey);
      if (media.thumbnail_object_key) await publishMediaObject(media.thumbnail_object_key, thumbnailKey);

      const result = await query(
        `UPDATE media_assets
         SET public_object_key = $2,
             public_thumbnail_object_key = $3, processed_at = now(), updated_at = now()
         WHERE id = $1 AND privacy_status = 'ready' AND public_object_key IS NULL`,
        [params.id, publicKey, media.thumbnail_object_key ? thumbnailKey : null]
      );
      if (result.rowCount === 0) {
        // 并发批准已完成发布；清理本次重复拷贝，保留已有公开对象
        await Promise.allSettled([
          deleteObject(config.S3_PUBLIC_BUCKET, publicKey),
          media.thumbnail_object_key ? deleteObject(config.S3_PUBLIC_BUCKET, thumbnailKey) : Promise.resolve()
        ]);
      }
    } catch (error) {
      await Promise.allSettled([
        deleteObject(config.S3_PUBLIC_BUCKET, publicKey),
        media.thumbnail_object_key ? deleteObject(config.S3_PUBLIC_BUCKET, thumbnailKey) : Promise.resolve()
      ]);
      // 发布失败：回滚到人工审核队列，避免留下没有公开对象的 ready 行；
      // 此前的 media.privacy_approved 审计保留了操作痕迹。
      await query(
        `UPDATE media_assets
         SET privacy_status = 'manual_review', processed_at = NULL, updated_at = now()
         WHERE id = $1 AND privacy_status = 'ready' AND public_object_key IS NULL`,
        [params.id]
      );
      throw error;
    }

    return { status: "ready", url: publicMediaUrl(publicKey), thumbnailUrl: media.thumbnail_object_key ? publicMediaUrl(thumbnailKey) : null };
  });

  app.delete("/media/:id", { preHandler: requireAuth }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string;
      owner_id: string;
      quarantine_object_key: string;
      processed_object_key: string | null;
      thumbnail_object_key: string | null;
      public_object_key: string | null;
      public_thumbnail_object_key: string | null;
    }>(
      `SELECT id, owner_id, quarantine_object_key, processed_object_key, thumbnail_object_key,
              public_object_key, public_thumbnail_object_key
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();

    const publishedReference = await query<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM revision_media rm
         JOIN map_features mf ON mf.current_revision_id = rm.revision_id
         WHERE rm.media_id = $1
           AND mf.status = 'published'
           AND mf.deleted_at IS NULL
       ) AS exists`,
      [params.id]
    );
    if (publishedReference.rows[0]?.exists) {
      throw conflict("Media attached to published content cannot be deleted separately");
    }

    const deleted = await transaction(async (client) => {
      // 条件翻转：并发删除只产生一次审计；处理中的媒体不允许与删除竞争。
      const claim = await client.query(
        `UPDATE media_assets
         SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE id = $1 AND deleted_at IS NULL
           AND privacy_status NOT IN ('scanning', 'processing')
         RETURNING id`,
        [params.id]
      );
      if (claim.rowCount === 0) return false;
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.deleted",
        resourceType: "media",
        resourceId: params.id
      });
      return true;
    });
    if (!deleted) throw conflict("Media is being processed or was already deleted");

    const removals = [
      deleteObject(config.S3_QUARANTINE_BUCKET, media.quarantine_object_key),
      media.processed_object_key ? deleteObject(config.S3_QUARANTINE_BUCKET, media.processed_object_key) : Promise.resolve(),
      media.public_object_key ? deleteObject(config.S3_PUBLIC_BUCKET, media.public_object_key) : Promise.resolve(),
      media.public_thumbnail_object_key ? deleteObject(config.S3_PUBLIC_BUCKET, media.public_thumbnail_object_key) : Promise.resolve(),
      media.thumbnail_object_key ? deleteObject(config.S3_QUARANTINE_BUCKET, media.thumbnail_object_key) : Promise.resolve()
    ];
    await Promise.allSettled(removals);
    return { status: "deleted" };
  });
}
