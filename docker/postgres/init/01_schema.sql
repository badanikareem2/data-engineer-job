-- Create schema & tables for the assignment

-- Content catalogue
CREATE TABLE IF NOT EXISTS content (
    id              UUID PRIMARY KEY,
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER,
    publish_ts      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw engagement telemetry
CREATE TABLE IF NOT EXISTS engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms  INTEGER,
    device       TEXT,
    raw_payload  JSONB
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_engagement_events_eventts ON engagement_events (event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_engagement_events_content ON engagement_events (content_id);

-- Seed content
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
VALUES
    (gen_random_uuid(), 'pod-hello', 'Hello Podcast', 'podcast', 1800, now()),
    (gen_random_uuid(), 'vid-101', 'Intro Video', 'video', 600, now()),
    (gen_random_uuid(), 'news-weekly', 'Weekly Newsletter', 'newsletter', NULL, now())
ON CONFLICT DO NOTHING;
