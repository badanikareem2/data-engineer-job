-- Insert 500 random-ish engagement events across existing content
-- - event_ts in last ~10 days
-- - duration_ms: realistic per event_type (NULL for clicks, content length for finish)
-- - random device, A/B flag, fake IP

WITH ins AS (
  INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
  SELECT
    c.id AS content_id,
    gen_random_uuid() AS user_id,
    et AS event_type,
    now() - (random() * interval '10 days') AS event_ts,
    CASE et
      WHEN 'click'  THEN NULL
      WHEN 'finish' THEN CASE WHEN c.length_seconds IS NULL THEN NULL ELSE c.length_seconds * 1000 END
      ELSE GREATEST(250, FLOOR(250 + random() * 299750))::int
    END AS duration_ms,
    (ARRAY['ios','android','web-chrome','web-safari','tv-os'])[FLOOR(random()*5)::int + 1] AS device,
    jsonb_build_object(
      'app_version', '1.' || (FLOOR(random()*10))::int || '.' || (FLOOR(random()*10))::int,
      'ip', '192.168.' || (FLOOR(random()*256))::int || '.' || (1 + FLOOR(random()*254))::int,
      'ab', (ARRAY['A','B'])[FLOOR(random()*2)::int + 1]
    ) AS raw_payload
  FROM generate_series(1, 500) g
  CROSS JOIN LATERAL (
    SELECT id, length_seconds FROM content ORDER BY random() LIMIT 1
  ) c
  CROSS JOIN LATERAL (
    SELECT (ARRAY['play','pause','finish','click'])[FLOOR(random()*4)::int + 1]::text AS et
  ) e
  RETURNING 1
)
SELECT count(*) AS inserted FROM ins;
