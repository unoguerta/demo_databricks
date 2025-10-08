from datetime import datetime, timezone

for k, default in {
  "env":"","catalog":"","schema":"","run_date_utc":"",
  "source_system":"","batch_id":"","s3_bucket":"","s3_key":"","s3_etag":"",
  "output_mode":"append"
}.items():
    try: dbutils.widgets.text(k, default)
    except: pass

def _get(n): 
    try: return dbutils.widgets.get(n)
    except: return None

params = {k: _get(k) for k in [
  "env","catalog","schema","run_date_utc","source_system",
  "batch_id","s3_bucket","s3_key","s3_etag","output_mode"
]}

if not params["env"] or not params["catalog"] or not params["schema"]:
    raise ValueError("Missing env/catalog/schema")
if not params["run_date_utc"]:
    params["run_date_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

spark.sql(f"USE CATALOG {params['catalog']}")
spark.sql(f"USE {params['schema']}")

ENV=params["env"]; CATALOG=params["catalog"]; SCHEMA=params["schema"]; RUN_DATE=params["run_date_utc"]; OUTPUT_MODE=params["output_mode"]

