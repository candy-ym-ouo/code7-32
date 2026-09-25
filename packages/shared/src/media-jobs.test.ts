import { describe, expect, it } from "vitest";
import { MEDIA_PROCESSING_JOB_NAME, mediaProcessingJobId } from "./media-jobs";

describe("mediaProcessingJobId", () => {
  it("is deterministic for the same media and attempt", () => {
    const mediaId = "5f0e4f6a-7d9b-4c2a-9f6d-2f3c9f0d9a11";
    expect(mediaProcessingJobId(mediaId, 2)).toBe(mediaProcessingJobId(mediaId, 2));
  });

  it("changes with every attempt so a retry always gets a fresh job", () => {
    const mediaId = "5f0e4f6a-7d9b-4c2a-9f6d-2f3c9f0d9a11";
    const attempts = [1, 2, 3].map((attempt) => mediaProcessingJobId(mediaId, attempt));
    expect(new Set(attempts).size).toBe(attempts.length);
  });

  it("is scoped per media asset", () => {
    const first = mediaProcessingJobId("5f0e4f6a-7d9b-4c2a-9f6d-2f3c9f0d9a11", 1);
    const second = mediaProcessingJobId("1b6f9c3e-2a4d-4e5b-8c7d-6e5f4a3b2c1d", 1);
    expect(first).not.toBe(second);
    expect(first).toContain("attempt-1");
  });

  it("uses the shared job name consumed by the worker", () => {
    expect(MEDIA_PROCESSING_JOB_NAME).toBe("process");
  });
});
