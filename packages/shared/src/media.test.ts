import { describe, expect, it } from "vitest";
import { mediaJobId } from "./media";

describe("mediaJobId", () => {
  it("binds the idempotency key to the processing attempt", () => {
    expect(mediaJobId("11111111-1111-1111-1111-111111111111", 1))
      .toBe("media:11111111-1111-1111-1111-111111111111:1");
    expect(mediaJobId("11111111-1111-1111-1111-111111111111", 2))
      .toBe("media:11111111-1111-1111-1111-111111111111:2");
  });

  it("is stable for the same attempt so concurrent retries deduplicate in BullMQ", () => {
    const id = "22222222-2222-2222-2222-222222222222";
    expect(mediaJobId(id, 3)).toBe(mediaJobId(id, 3));
  });

  it("produces distinct keys across attempts so stale jobs cannot claim a new run", () => {
    const id = "33333333-3333-3333-3333-333333333333";
    expect(mediaJobId(id, 1)).not.toBe(mediaJobId(id, 2));
  });

  it("rejects invalid attempts", () => {
    expect(() => mediaJobId("id", 0)).toThrow();
    expect(() => mediaJobId("id", -1)).toThrow();
    expect(() => mediaJobId("id", 1.5)).toThrow();
  });
});
