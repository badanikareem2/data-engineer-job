SET 'execution.checkpointing.interval' = '10 s';

-- ====== SOURCES (Debezium JSON, no key.format) ======
CREATE TABLE content_src (
  id STRING,
  slug STRING,
  title STRING,
  content_type STRING,
  length_seconds INT,
  publish_ts TIMESTAMP_LTZ(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dbserver1.public.content',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);

CREATE TABLE engagement_events_src (
  id BIGINT,
  content_id STRING,
  user_id STRING,
  event_type STRING,
  event_ts TIMESTAMP_LTZ(3),
  duration_ms INT,
  device STRING,
  raw_payload STRING,
  proc_time AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'dbserver1.public.engagement_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);

-- ====== SINK (plain Kafka JSON append) ======
DROP TABLE IF EXISTS engagements_enriched_kafka;

CREATE TABLE engagements_enriched_kafka (
  id BIGINT,
  content_id STRING,
  user_id STRING,
  event_type STRING,
  event_ts TIMESTAMP_LTZ(3),
  device STRING,
  engagement_seconds DOUBLE,
  engagement_pct DOUBLE,
  content_type STRING,
  length_seconds INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'engagements.enriched.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'sink.partitioner' = 'round-robin',
  'sink.delivery-guarantee' = 'at-least-once',
  'properties.transaction.timeout.ms' = '900000'
);

-- ====== ENRICHMENT ======
INSERT INTO engagements_enriched_kafka
SELECT
  e.id,
  e.content_id,
  e.user_id,
  e.event_type,
  e.event_ts,
  e.device,
  CASE WHEN e.duration_ms IS NULL THEN NULL
       ELSE ROUND(CAST(e.duration_ms AS DOUBLE) / 1000.0, 2)
  END AS engagement_seconds,
  CASE
    WHEN e.duration_ms IS NULL OR c.length_seconds IS NULL OR c.length_seconds = 0
      THEN NULL
    ELSE ROUND( (CAST(e.duration_ms AS DOUBLE) / 1000.0) / CAST(c.length_seconds AS DOUBLE), 2 )
  END AS engagement_pct,
  c.content_type,
  c.length_seconds
FROM engagement_events_src AS e
LEFT JOIN content_src FOR SYSTEM_TIME AS OF e.proc_time AS c
  ON e.content_id = c.id;
