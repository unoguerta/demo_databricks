import argparse
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F

def _spark(catalog, schema):
    s = SparkSession.getActiveSession()
    s.sql(f"USE CATALOG {catalog}")
    s.sql(f"USE {schema}")
    return s

def transform_generic():
    p = argparse.ArgumentParser()
    p.add_argument("--env", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--source-system", required=True, dest="source_system")
    p.add_argument("--run-date-utc", required=False, dest="run_date_utc")
    p.add_argument("--batch-id", required=False, dest="batch_id")
    args = p.parse_args()

    if not args.run_date_utc:
        args.run_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    spark = _spark(args.catalog, args.schema)
    bronze_tbl = f"{args.catalog}.bronze.{args.source_system}_raw"
    b = spark.table(bronze_tbl).filter(F.to_date("event_ts") == F.lit(args.run_date_utc))

    s = (b
         .withColumn("event_dt_utc", F.to_date("event_ts"))
         .withColumn("callsign_norm", F.upper(F.col("callsign")))
         .withColumn("batch_id", F.lit(args.batch_id))
         .select("event_ts","event_dt_utc","flight_id","callsign_norm", "batch_id","_ingested_at"))

    target = f"{args.catalog}.{args.schema}.{args.source_system}_silver"
    s.write.mode("append").saveAsTable(target)

