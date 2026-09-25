import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
import { mediaJobId } from "@map/shared";
import { config } from "./config";
import { pool } from "./db";
import { processMediaJob, cleanupOriginalMedia, cleanupDeletedMediaObjects, markStaleFeatures, recoverStuckMedia, markUnreferencedMediaDeleted } from "./media-job";
import { dispatchOutbox, recoverStuckOutbox } from "./outbox";
import { purgeDeletedAccounts } from "./account-job";

const redisOptions = { maxRetriesPerRequest: null } as const;
const queueConnection = new IORedis(config.REDIS_URL, redisOptions);
const mediaWorkerConnection = new IORedis(config.REDIS_URL, redisOptions);
const outboxWorkerConnection = new IORedis(config.REDIS_URL, redisOptions);

for (const [name, connection] of [
  ["queue", queueConnection],
  ["media worker", mediaWorkerConnection],
  ["outbox worker", outboxWorkerConnection]
] as const) {
  connection.on("error", (error) => console.error({ error, connection: name }, "Redis connection error"));
}
const mediaQueue = new Queue("media", { connection: queueConnection });

const mediaWorker = new Worker("media", async (job) => {
  if (job.name !== "process") return;
  const mediaId = String(job.data.mediaId);
  // 新版作业携带处理代次；旧格式作业缺省，由 processMediaJob 按当前代次兜底。
  const attempt = typeof job.data.attempt === "number" ? job.data.attempt : undefined;
  await processMediaJob(mediaId, attempt);
}, { connection: mediaWorkerConnection, concurrency: 2 });

const outboxWorker = new Worker("outbox", async (job) => {
  if (job.name !== "dispatch") return;
  await dispatchOutbox(job.data?.eventId ? String(job.data.eventId) : undefined);
}, { connection: outboxWorkerConnection, concurrency: 2 });

mediaWorker.on("failed", (job, error) => console.error({ jobId: job?.id, error }, "media job failed"));
outboxWorker.on("failed", (job, error) => console.error({ jobId: job?.id, error }, "outbox job failed"));

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_resolve, reject) => {
      setTimeout(() => reject(new Error(`Redis queue operation timed out after ${timeoutMs}ms`)), timeoutMs).unref();
    })
  ]);
}

let maintenanceRunning = false;

async function enqueueRecoveredMedia(stuckMedia: Array<{ id: string; attempt: number }>): Promise<void> {
  for (const media of stuckMedia) {
    try {
      // 幂等键与恢复后的新代次绑定，重复恢复同代次不会产生重复作业
      await withTimeout(mediaQueue.add("process", { mediaId: media.id, attempt: media.attempt }, {
        jobId: mediaJobId(media.id, media.attempt),
        removeOnComplete: 1000,
        removeOnFail: 1000
      }), 3_000);
    } catch (error) {
      console.error({ mediaId: media.id, attempt: media.attempt, error }, "failed to enqueue recovered media");
    }
  }
}

/**
 * 清理并发安全重构之前入队的旧格式媒体作业：
 *   media-<id>            （complete）
 *   media-<id>-<ts>       （失败重试：每次点击一个新 jobId，正是多任务来源）
 *   media-recover-<id>-<ts>（卡死恢复）
 * 终态/重复作业直接移除；仍在活动状态的媒体按当前代次重新入队一次。
 */
async function pruneLegacyMediaJobs(): Promise<void> {
  const states = ["active", "waiting", "delayed", "paused", "completed", "failed"] as const;
  const legacyJobs = (await mediaQueue.getJobs([...states]))
    .filter((job) => typeof job.id === "string" && /^media(-recover)?-/.test(job.id));
  if (legacyJobs.length === 0) return;

  const activeMediaIds = new Set<string>();
  for (const job of legacyJobs) {
    const mediaId = typeof job.data?.mediaId === "string" ? job.data.mediaId : null;
    try {
      await job.remove();
    } catch (error) {
      console.error({ jobId: job.id, error }, "failed to remove legacy media job");
      continue;
    }
    if (mediaId) activeMediaIds.add(mediaId);
  }

  if (activeMediaIds.size === 0) return;
  const active = await pool.query<{ id: string; processing_attempt: number }>(
    `SELECT id, processing_attempt FROM media_assets
     WHERE id = ANY($1::uuid[])
       AND privacy_status IN ('scanning', 'processing')
       AND updated_at >= now() - interval '20 minutes'
       AND deleted_at IS NULL`,
    [[...activeMediaIds]]
  );
  for (const row of active.rows) {
    try {
      await withTimeout(mediaQueue.add("process", { mediaId: row.id, attempt: row.processing_attempt }, {
        jobId: mediaJobId(row.id, row.processing_attempt),
        removeOnComplete: 1000,
        removeOnFail: 1000
      }), 3_000);
    } catch (error) {
      // 已有同代次作业（BullMQ 去重）或 Redis 暂时不可用：恢复流程下个周期会重试
      console.error({ mediaId: row.id, error }, "failed to re-enqueue active media after legacy prune");
    }
  }
  console.log({ removed: legacyJobs.length, reEnqueued: active.rowCount }, "pruned legacy media jobs");
}

async function maintenanceTick() {
  if (maintenanceRunning) return;
  maintenanceRunning = true;
  try {
    await recoverStuckOutbox();
    await dispatchOutbox();
    const stuckMedia = await recoverStuckMedia();
    await enqueueRecoveredMedia(stuckMedia);
    await cleanupOriginalMedia();
    await markUnreferencedMediaDeleted();
    await cleanupDeletedMediaObjects();
    await markStaleFeatures();
    await purgeDeletedAccounts();
  } catch (error) {
    console.error({ error }, "maintenance tick failed");
  } finally {
    maintenanceRunning = false;
  }
}

void pruneLegacyMediaJobs().catch((error) => {
  console.error({ error }, "legacy media job prune failed");
});
await maintenanceTick();
const maintenanceTimer = setInterval(() => void maintenanceTick(), 60_000);
maintenanceTimer.unref();

async function shutdown(signal: string) {
  console.log(`worker shutting down: ${signal}`);
  clearInterval(maintenanceTimer);
  await Promise.all([mediaWorker.close(), outboxWorker.close(), mediaQueue.close()]);
  for (const connection of [queueConnection, mediaWorkerConnection, outboxWorkerConnection]) {
    if (connection.status !== "end") connection.disconnect();
  }
  await pool.end();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
