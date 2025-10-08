CREATE TABLE IF NOT EXISTS ${CATALOG}.silver.${SOURCE_SYSTEM}_silver
USING DELTA AS
SELECT
  CAST(NULL AS TIMESTAMP)  AS event_ts,
  CAST(NULL AS DATE)       AS event_dt_utc,
  CAST(NULL AS BIGINT)     AS flight_id,
  CAST(NULL AS STRING)     AS callsign_norm,
  CAST(NULL AS STRING)     AS batch_id,
  CAST(NULL AS TIMESTAMP)  AS _ingested_at
WHERE 1=0;

