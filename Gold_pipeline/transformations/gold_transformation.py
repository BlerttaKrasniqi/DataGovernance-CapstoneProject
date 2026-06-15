import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold_compliant_metadata",
    comment="Compliant metadata records ready for reporting"
)
def gold_compliant_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(F.col("is_compliant") == 1)
    )

@dlt.table(
    name="gold_non_compliant_metadata",
    comment="Non-compliant metadata records requiring remediation"
)
def gold_non_compliant_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(F.col("is_compliant") == 0)
    )