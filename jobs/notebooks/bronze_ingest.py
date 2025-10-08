
%run /Repos/<common-notebook>
from pyspark.sql import functions as F
from datetime import datetime, timezone

if params["s3_bucket"] and params["s3_key"]:
    path = f"s3://{params['s3_bucket']}/{params['s3_key']}"
    df = spark.read.format("json").load(path)
else:
    path = f"s3://<bucket-path>/*/*.json*"
    df = spark.read.format("json").load(path)

bronze = (df
  .withColumn("event_ts", F.coalesce(F.col("ts").cast("timestamp"), F.col("time").cast("timestamp")))
  .withColumn("flight_id", F.col("properties.flightId").cast("long"))
  .withColumn("callsign", F.coalesce(F.col("properties.callsign"), F.col("callsign")))
  .withColumn("source_system", F.lit(params["source_system"]))
  .withColumn("_source_file", F.input_file_name())
  .withColumn("_ingested_at", F.current_timestamp())
)

bronze.write.mode("append").saveAsTable(f"{CATALOG}.bronze.{params['source_system']}_raw")
batch_id = params["batch_id"] or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)

