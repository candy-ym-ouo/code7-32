import { Queue } from "bullmq";
import IORedis from "ioredis";
import { MEDIA_PROCESSING_JOB_NAME, mediaProcessingJobId } from "@map/shared/media-jobs";
import { config } from "./config";

const redisOptions = { maxRetriesPerRequest: null } as const;
export const mediaRedis = new IORedis(config.REDIS_URL, redisOptions);
export const outboxRedis = new IORedis(config.REDIS_URL, redisOptions);
mediaRedis.on("error", (error) => console.error({ error }, "media Redis connection error"));
outboxRedis.on("error", (error) => console.error({ error }, "outbox Redis connection error"));

export const mediaQueue = new Queue("media", { connection: mediaRedis });
export const outboxQueue = new Queue("outbox", { connection: outboxRedis });

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_resolve, reject) => {
      setTimeout(() => reject(new Error(`Redis queue operation timed out after ${timeoutMs}ms`)), timeoutMs).unref();
    })
  ]);
}

/**
 * Enqueues the processing job for one claimed attempt. The job id is
 * deterministic per (media, attempt), so calling this twice for the same
 * attempt is a no-op and can never create a duplicate job.
 */
export async function enqueueMediaProcessing(mediaId: string, attempt: number): Promise<void> {
  await withTimeout(
    mediaQueue.add(MEDIA_PROCESSING_JOB_NAME, { mediaId, attempt }, {
      jobId: mediaProcessingJobId(mediaId, attempt),
      removeOnComplete: 1000,
      removeOnFail: 1000
    }),
    3_000
  );
}

export async function enqueueOutbox(eventId: string): Promise<void> {
  try {
    await withTimeout(
      outboxQueue.add("dispatch", { eventId }, { removeOnComplete: 1000, removeOnFail: 1000 }),
      3_000
    );
  } catch (error) {
    // The database outbox remains the source of truth. A worker maintenance tick retries pending rows.
    console.error({ eventId, error }, "failed to enqueue outbox event");
  }
}

export async function closeQueues(): Promise<void> {
  await Promise.all([mediaQueue.close(), outboxQueue.close()]);
  if (mediaRedis.status !== "end") mediaRedis.disconnect();
  if (outboxRedis.status !== "end") outboxRedis.disconnect();
}
