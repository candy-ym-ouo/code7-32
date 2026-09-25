import { Queue, Worker } from "bullmq";
import IORedis from "ioredis";
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
  const attempt = job.data?.attempt == null ? undefined : Number(job.data.attempt);
  await processMediaJob(String(job.data.mediaId), attempt);
}, { connection: mediaWorkerConnection, concurrency: 2 });

const outboxWorker = new Worker("outbox", async (job) => {
  if (job.name !== "dispatch") return;
  await dispatchOutbox(job.data?.eventId ? String(job.data.eventId) : undefined);
}, { connection: outboxWorkerConnection, concurrency: 2 });

mediaWorker.on("failed", (job, error) => console.error({ jobId: job?.id, error }, "media job failed"));
outboxWorker.on("failed", (job, error) => console.error({ jobId: job?.id, error }, "outbox job failed"));

let maintenanceRunning = false;

async function maintenanceTick() {
  if (maintenanceRunning) return;
  maintenanceRunning = true;
  try {
    await recoverStuckOutbox();
    await dispatchOutbox();
    const recoveredMedia = await recoverStuckMedia(mediaQueue);
    if (recoveredMedia.length) console.log({ recoveredMedia }, "recovered stuck media");
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
