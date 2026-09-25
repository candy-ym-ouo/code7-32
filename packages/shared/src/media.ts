/**
 * 媒体处理作业的确定性幂等键，与 media_assets.processing_attempt 代次绑定。
 *
 * - 首次处理：media:<id>:1
 * - 每次重试/卡死恢复：代次单调递增，键随之变化
 *
 * BullMQ 以 jobId 去重：同代次的并发入队只产生一个作业；旧代次作业即使
 * 残留，也会在 worker 的状态机守卫处按代次跳过。
 */
export function mediaJobId(mediaId: string, attempt: number): string {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new Error(`Invalid media processing attempt: ${attempt}`);
  }
  return `media:${mediaId}:${attempt}`;
}
