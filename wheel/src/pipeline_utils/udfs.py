from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType

@udf(StringType())
def parse_callsign(c):
    return (c or "").strip().upper()

