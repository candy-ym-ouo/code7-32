import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";

const migration = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../migrations/0001_init.sql"),
  "utf8"
);

describe("initial migration", () => {
  it("contains the core audited entities", () => {
    for (const table of [
      "users", "sessions", "auth_tokens", "categories", "map_features",
      "feature_revisions", "media_assets", "comments", "reports",
      "moderation_actions", "outbox_events", "audit_logs", "notifications"
    ]) {
      expect(migration).toContain(`CREATE TABLE ${table}`);
    }
  });

  it("adds public thumbnail and outbox recovery fields in migration 0002", () => {
    const followup = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "../migrations/0002_media_public_thumb.sql"),
      "utf8"
    );
    expect(followup).toContain("public_thumbnail_object_key");
    expect(followup).toContain("updated_at timestamptz");
  });

  it("adds the processing attempt generation column and stuck-job index in migration 0003", () => {
    const followup = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "../migrations/0003_media_processing_attempt.sql"),
      "utf8"
    );
    expect(followup).toContain("ADD COLUMN processing_attempt integer NOT NULL DEFAULT 1");
    expect(followup).toContain("media_assets_active_stuck_idx");
  });

  it("migration 0003 collapses duplicate processing audit history", () => {
    const followup = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "../migrations/0003_media_processing_attempt.sql"),
      "utf8"
    );
    expect(followup).toContain("DELETE FROM audit_logs");
    expect(followup).toContain("'media.processing_requested'");
    expect(followup).toContain("row_number()");
  });

  it("uses PostGIS geography points and spatial indexes", () => {
    expect(migration).toContain("geography(Point, 4326)");
    expect(migration).toContain("USING gist (geom)");
  });
});
