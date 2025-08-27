-- Adds up to 15 content rows with reasonable lengths; skips if slug exists
WITH base AS (
  SELECT g AS n,
         (ARRAY['podcast','video','newsletter'])[FLOOR(random()*3)::int + 1] AS ct
  FROM generate_series(1,15) g
),
rows AS (
  SELECT
    gen_random_uuid() AS id,
    (
      CASE ct WHEN 'podcast' THEN 'pod'
              WHEN 'video' THEN 'vid'
              ELSE 'news' END
    ) || '-' || to_char(n,'FM000') || '-' || substr(md5(random()::text),1,6) AS slug,
    INITCAP(ct) || ' #' || n AS title,
    ct AS content_type,
    CASE
      WHEN ct = 'podcast'  THEN (ARRAY[900,1200,1800,2400])[FLOOR(random()*4)::int + 1]  -- 15–40 min
      WHEN ct = 'video'    THEN (ARRAY[300,600,900])[FLOOR(random()*3)::int + 1]        -- 5–15 min
      ELSE NULL
    END AS length_seconds,
    now() - (random() * interval '30 days') AS publish_ts
  FROM base
)
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
SELECT id, slug, title, content_type, length_seconds, publish_ts FROM rows
ON CONFLICT (slug) DO NOTHING;

-- Show what we added in this run
SELECT content_type, count(*) AS inserted_now
FROM content
WHERE publish_ts > now() - interval '1 minute'
GROUP BY 1
ORDER BY 1;
