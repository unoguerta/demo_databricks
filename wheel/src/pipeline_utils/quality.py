from pyspark.sql.functions import col
def assert_non_nulls(df, cols):
    for c in cols:
        n = df.filter(col(c).isNull()).limit(1).count()
        if n > 0:
            raise AssertionError(f"Nulls found in {c}")

