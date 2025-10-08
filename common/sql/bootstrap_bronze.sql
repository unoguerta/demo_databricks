CREATE TABLE IF NOT EXISTS ${CATALOG}.bronze.${SOURCE_SYSTEM}_raw (
  event_ts TIMESTAMP,
  flight_id BIGINT,
  callsign STRING,
  source_system STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP
) USING DELTA;

