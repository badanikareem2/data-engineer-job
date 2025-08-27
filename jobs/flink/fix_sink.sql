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
  length_seconds INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'engagements.enriched.v1',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'key.fields' = 'id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);
