/**
 * Shared contract between the API (enqueue) and the worker (consume/recover)
 * for media processing jobs.
 *
 * The BullMQ job id is deterministic per (media, processing attempt): the
 * database increments `media_assets.processing_attempt` atomically for every
 * accepted processing request, so a given attempt can only ever produce one
 * job id. Re-adding the same id is a BullMQ no-op, which makes enqueueing
 * idempotent; a new attempt always yields a fresh id.
 */
export const MEDIA_PROCESSING_JOB_NAME = "process" as const;

export function mediaProcessingJobId(mediaId: string, attempt: number): string {
  return `media-${mediaId}-attempt-${attempt}`;
}
