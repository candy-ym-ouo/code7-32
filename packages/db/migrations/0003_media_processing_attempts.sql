-- Media processing idempotency epoch.
--
-- Every accepted processing request (upload completion or retry of a failed
-- asset) increments processing_attempt exactly once inside the same atomic
-- status transition. Queue job ids are derived from (media id, attempt), so
-- duplicate enqueues of one attempt collapse onto a single BullMQ job and
-- jobs left over from superseded attempts can be detected and skipped.
--
-- Rows that already went through processing are backfilled to attempt 1 so
-- their next accepted retry uses a job id that has never existed before.
ALTER TABLE media_assets
  ADD COLUMN processing_attempt integer NOT NULL DEFAULT 0;

UPDATE media_assets
SET processing_attempt = 1
WHERE privacy_status <> 'quarantined';
