import dlt
from pyspark.sql import functions as F


# Create a Gold table that contains only compliant metadata records.
# These records passed the validation and compliance checks in the Silver layer.
@dlt.table(
    name="gold_compliant_metadata",
    comment="Compliant metadata records ready for reporting"
)
def gold_compliant_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(F.col("is_compliant") == 1)
    )


# Create a Gold table that contains only non-compliant metadata records.
# These records failed one or more validation checks and require review.
@dlt.table(
    name="gold_non_compliant_metadata",
    comment="Non-compliant metadata records requiring review"
)
def gold_non_compliant_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(F.col("is_compliant") == 0)
    )


# Create a Gold table that identifies potentially sensitive metadata fields.
# The detection is based on common sensitive column name patterns.
@dlt.table(
    name="gold_sensitive_metadata",
    comment="Metadata records containing sensitive data indicators"
)
def gold_sensitive_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(
            (F.lower(F.col("column_name")).contains("email")) |
            (F.lower(F.col("column_name")).contains("phone")) |
            (F.lower(F.col("column_name")).contains("ssn"))
        )
    )


# Create a Gold table that classifies metadata records
# into governance categories based on sensitivity indicators.
@dlt.table(
    name="gold_classified_metadata",
    comment="Metadata records classified by sensitivity and governance level"
)
def gold_classified_metadata():
    return (
        spark.read.table("silver_metadata")
        .withColumn(
            "data_classification",
            F.when(
                F.lower(F.col("column_name")).contains("email") |
                F.lower(F.col("column_name")).contains("phone") |
                F.lower(F.col("column_name")).contains("ssn") |
                F.lower(F.col("column_name")).contains("account") |
                F.lower(F.col("column_name")).contains("birth"),
                "Sensitive"
            )
            .when(
                F.lower(F.col("column_name")).contains("id"),
                "Internal"
            )
            .otherwise("Public")
        )
    )

# Create a Gold table that contains metadata records
# identified as Personally Identifiable Information (PII).
# These records are used for governance reporting,
# dashboard metrics, and AI governance insights.

@dlt.table(
    name="gold_pii_metadata",
    comment="Gold table containing metadata records identified as PII"
)
def gold_pii_metadata():
    return (
        spark.read.table("silver_metadata")
        .filter(F.lower(F.col("pii_flag").cast("string")) == "true")
    )

# Create a Gold table that provides business definitions
# and descriptions for metadata fields.
# This table supports data governance, metadata search,
# reporting, and AI-driven insights by establishing
# a common business vocabulary across the platform.

@dlt.table(
    name="gold_business_glossary",
    comment="Gold table containing business definitions and descriptions for metadata fields"
)
def gold_business_glossary():
    return (
        spark.read.table("silver_metadata")
        .withColumn(
            "business_definition",
            F.when(
                F.lower(F.col("column_name")).contains("email"),
                "Customer email address"
            )
            .when(
                F.lower(F.col("column_name")).contains("phone"),
                "Customer phone number"
            )
            .when(
                F.lower(F.col("column_name")).contains("ssn"),
                "Social Security Number"
            )
            .when(
                F.lower(F.col("column_name")).contains("account"),
                "Account identifier"
            )
            .otherwise("General business attribute")
        )
    )

# Create a Gold table that assigns data ownership information
# to metadata records.
# This table supports governance accountability by identifying
# the responsible business owner for each metadata asset.

@dlt.table(
    name="gold_data_ownership_registry",
    comment="Gold table containing ownership information for metadata records"
)
def gold_data_ownership_registry():
    return (
        spark.read.table("silver_metadata")
        .withColumn(
            "data_owner",
            F.when(
                F.lower(F.col("table_name")).contains("customer"),
                "Customer Data Owner"
            )
            .when(
                F.lower(F.col("table_name")).contains("payment"),
                "Finance Data Owner"
            )
            .when(
                F.lower(F.col("table_name")).contains("order"),
                "Sales Data Owner"
            )
            .otherwise("General Data Owner")
        )
    )

# Create a Gold table that enriches metadata records
# with additional governance information.
# This table improves reporting, metadata search,
# and AI-driven insights by adding useful attributes.

@dlt.table(
    name="gold_enriched_metadata",
    comment="Gold table containing enriched metadata records for governance reporting"
)
def gold_enriched_metadata():
    return (
        spark.read.table("silver_metadata")
        .withColumn(
            "governance_status",
            F.when(F.col("is_compliant") == 1, "Approved")
            .otherwise("Needs Review")
        )
        .withColumn(
            "metadata_priority",
            F.when(
                F.lower(F.col("pii_flag").cast("string")) == "true",
                "High"
            )
            .when(
                F.col("is_compliant") == 0,
                "Medium"
            )
            .otherwise("Low")
        )
    )
# Create a Gold table that assigns tags to metadata records.
# These tags improve metadata discovery, governance,
# reporting, and AI-driven insights.

@dlt.table(
    name="gold_metadata_tags",
    comment="Gold table containing metadata tags for governance and search"
)
def gold_metadata_tags():
    return (
        spark.read.table("silver_metadata")
        .withColumn(
            "metadata_tag",
            F.when(
                F.lower(F.col("column_name")).contains("email"),
                "PII"
            )
            .when(
                F.lower(F.col("column_name")).contains("phone"),
                "Sensitive"
            )
            .when(
                F.col("is_compliant") == 0,
                "Needs Review"
            )
            .otherwise("Standard")
        )
    )