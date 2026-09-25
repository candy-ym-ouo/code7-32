-- 媒体处理并发安全重构：
-- 1) processing_attempt 记录当前处理代次。每次从终态（失败/拒绝/卡死恢复）重新入队时
--    单调递增，作业的幂等键与该代次绑定，过期代次的作业由状态机守卫拒绝。
ALTER TABLE media_assets
  ADD COLUMN processing_attempt integer NOT NULL DEFAULT 1;

-- 卡死作业恢复扫描（privacy_status IN ('scanning','processing') AND updated_at < ...）
CREATE INDEX media_assets_active_stuck_idx
  ON media_assets(updated_at)
  WHERE privacy_status IN ('scanning', 'processing');

-- 2) 清理重复历史：并发 complete/retry 在修复前会为同一媒体写入多条
--    media.processing_requested 审计。每个媒体仅保留最早的一条。
DELETE FROM audit_logs AS a
USING (
  SELECT id,
         row_number() OVER (PARTITION BY resource_id ORDER BY created_at ASC, id ASC) AS rn
  FROM audit_logs
  WHERE action = 'media.processing_requested'
    AND resource_type = 'media'
    AND resource_id IS NOT NULL
) AS ranked
WHERE a.id = ranked.id
  AND ranked.rn > 1;
