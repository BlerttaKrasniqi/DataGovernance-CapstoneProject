import dlt
from pyspark.sql import functions as F


SILVER_METADATA = "workspace.silver.silver_metadata"
STRUCTURAL_STANDARD_MIN_COLUMNS = 10   # ADDED: column standard used for structural compliance


# ADDED: enrich each metadata record with table-level structural compliance and an
# OVERALL compliance flag = documentation-compliant (is_compliant) AND structurally-compliant
# (enough columns). Also builds a human-readable reason for the chatbot.
def _enriched():
    m = spark.read.table(SILVER_METADATA)
    counts = m.groupBy("table_name").agg(
        F.countDistinct("column_name").alias("table_column_count"))
    m = m.join(counts, "table_name", "left")
    m = m.withColumn("follows_standard",
            (F.col("table_column_count") >= F.lit(STRUCTURAL_STANDARD_MIN_COLUMNS)).cast("int"))
    m = m.withColumn("overall_compliant",
            (F.col("is_compliant").cast("int") * F.col("follows_standard")).cast("int"))
    m = m.withColumn("noncompliance_reason", F.concat_ws("; ",
            F.when(F.col("is_compliant") == 0, F.concat(F.lit("Missing: "), F.col("missing_elements"))),
            F.when(F.col("follows_standard") == 0,
                   F.concat(F.lit("Structural: only "), F.col("table_column_count"),
                            F.lit(" columns (min "), F.lit(STRUCTURAL_STANDARD_MIN_COLUMNS), F.lit(")")))))
    return m


# PRIMARY OUTPUT 1: fully compliant records (documentation AND structural).
@dlt.table(
    name="gold_compliant_metadata",
    comment="Compliant metadata records (documentation + structural) ready for reporting"
)
def gold_compliant_metadata():
    return _enriched().filter(F.col("overall_compliant") == 1)


# PRIMARY OUTPUT 2: non-compliant records with the reason (feeds dashboard + chatbot).
@dlt.table(
    name="gold_non_compliant_metadata",
    comment="Non-compliant metadata records requiring review, with reason"
)
def gold_non_compliant_metadata():
    return (_enriched().filter(F.col("overall_compliant") == 0)
            .select("table_name", "column_name", "is_compliant", "follows_standard",
                    "table_column_count", "missing_elements", "noncompliance_reason"))


# Sensitive metadata fields. CHANGED: use the real pii_flag as source of truth,
# with column-name patterns as a supplement.
@dlt.table(
    name="gold_sensitive_metadata",
    comment="Metadata records containing sensitive data indicators"
)
def gold_sensitive_metadata():
    return (
        spark.read.table(SILVER_METADATA)
        .filter(
            (F.lower(F.col("pii_flag").cast("string")) == "true") |
            (F.lower(F.col("column_name")).contains("email")) |
            (F.lower(F.col("column_name")).contains("phone")) |
            (F.lower(F.col("column_name")).contains("ssn"))
        )
    )


# Classify metadata records into governance categories.
@dlt.table(
    name="gold_classified_metadata",
    comment="Metadata records classified by sensitivity and governance level"
)
def gold_classified_metadata():
    return (
        spark.read.table(SILVER_METADATA)
        .withColumn(
            "data_classification",
            F.when(
                (F.lower(F.col("pii_flag").cast("string")) == "true") |
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


# PII records (uses the governance pii_flag).
@dlt.table(
    name="gold_pii_metadata",
    comment="Gold table containing metadata records identified as PII"
)
def gold_pii_metadata():
    return (
        spark.read.table(SILVER_METADATA)
        .filter(F.lower(F.col("pii_flag").cast("string")) == "true")
    )


# Business glossary.
@dlt.table(
    name="gold_business_glossary",
    comment="Gold table containing business definitions and descriptions for metadata fields"
)
def gold_business_glossary():
    return (
        spark.read.table(SILVER_METADATA)
        .withColumn(
            "business_definition",
            F.when(F.lower(F.col("column_name")).contains("email"), "Customer email address")
            .when(F.lower(F.col("column_name")).contains("phone"), "Customer phone number")
            .when(F.lower(F.col("column_name")).contains("ssn"), "Social Security Number")
            .when(F.lower(F.col("column_name")).contains("account"), "Account identifier")
            .otherwise("General business attribute")
        )
    )


# Ownership registry.
@dlt.table(
    name="gold_data_ownership_registry",
    comment="Gold table containing ownership information for metadata records"
)
def gold_data_ownership_registry():
    return (
        spark.read.table(SILVER_METADATA)
        .withColumn(
            "data_owner",
            F.when(F.lower(F.col("table_name")).contains("customer"), "Customer Data Owner")
            .when(F.lower(F.col("table_name")).contains("payment"), "Finance Data Owner")
            .when(F.lower(F.col("table_name")).contains("order"), "Sales Data Owner")
            .otherwise("General Data Owner")
        )
    )


# Enriched metadata. CHANGED: governance_status/priority now use OVERALL compliance.
@dlt.table(
    name="gold_enriched_metadata",
    comment="Gold table containing enriched metadata records for governance reporting"
)
def gold_enriched_metadata():
    return (
        _enriched()
        .withColumn(
            "governance_status",
            F.when(F.col("overall_compliant") == 1, "Approved").otherwise("Needs Review")
        )
        .withColumn(
            "metadata_priority",
            F.when(F.lower(F.col("pii_flag").cast("string")) == "true", "High")
            .when(F.col("overall_compliant") == 0, "Medium")
            .otherwise("Low")
        )
    )


# Tags.
@dlt.table(
    name="gold_metadata_tags",
    comment="Gold table containing metadata tags for governance and search"
)
def gold_metadata_tags():
    return (
        spark.read.table(SILVER_METADATA)
        .withColumn(
            "metadata_tag",
            F.when(F.lower(F.col("pii_flag").cast("string")) == "true", "PII")
            .when(F.lower(F.col("column_name")).contains("phone"), "Sensitive")
            .when(F.col("is_compliant") == 0, "Needs Review")
            .otherwise("Standard")
        )
    )


# Per-record quality score (0-100) from the 8 silver flags.
@dlt.table(
    name="gold_quality_metrics",
    comment="Gold table containing quality scores for metadata records"
)
def gold_quality_metrics():
    return (
        spark.read.table(SILVER_METADATA)
        .withColumn(
            "quality_score",
            (
                F.col("flag_table_desc") +
                F.col("flag_column_desc") +
                F.col("flag_term_name") +
                F.col("flag_data_steward") +
                F.col("flag_security_classification") +
                F.col("flag_source_system_tag") +
                F.col("flag_pii_flag") +
                F.col("flag_dq_pass")
            ) * 100 / 8
        )
    )


# =========================== DASHBOARD KPIs (ADDED) ===========================
# Aggregated summary tables for the shareholder dashboard (Objective 2).

# Per-table inventory: columns, steward, certification, DQ rate, structural + overall compliance.
@dlt.table(name="gold_table_inventory", comment="Per-table governance summary")
def gold_table_inventory():
    e = _enriched()
    return (e.groupBy("table_name").agg(
        F.max("table_column_count").alias("number_of_columns"),
        F.max("data_steward").alias("data_steward"),
        F.max("certification_level").alias("certification_level"),
        F.max("dq_invalid_rate").alias("dq_invalid_rate"),
        F.max("follows_standard").alias("follows_standard"),
        F.max("is_compliant").alias("doc_compliant"),
        F.max("overall_compliant").alias("overall_compliant"))
        .withColumn("data_quality_score",
                    F.round((F.lit(1.0) - F.coalesce(F.col("dq_invalid_rate"), F.lit(0.0))) * 100, 2)))


# One-row executive summary for the dashboard.
@dlt.table(name="gold_governance_kpis", comment="Executive KPI summary for the dashboard")
def gold_governance_kpis():
    inv = dlt.read("gold_table_inventory")
    return inv.agg(
        F.countDistinct("table_name").alias("total_tables"),
        F.round(F.avg("number_of_columns"), 1).alias("avg_columns_per_table"),
        F.sum("overall_compliant").alias("compliant_tables"),
        F.round(F.sum("overall_compliant") / F.countDistinct("table_name") * 100, 1).alias("pct_tables_compliant"),
        F.round(F.avg("data_quality_score"), 2).alias("avg_data_quality_score"))


# Structural standard check (flags billing_transactions etc.).
@dlt.table(name="gold_structural_consistency", comment="Tables below the column standard")
def gold_structural_consistency():
    inv = dlt.read("gold_table_inventory")
    return (inv.select("table_name", "number_of_columns", "follows_standard")
              .withColumn("expected_min_columns", F.lit(STRUCTURAL_STANDARD_MIN_COLUMNS))
              .withColumn("issue",
                  F.when(F.col("follows_standard") == 0,
                         F.concat(F.lit("Only "), F.col("number_of_columns"),
                                  F.lit(" columns; below standard of "), F.lit(STRUCTURAL_STANDARD_MIN_COLUMNS)))
                   .otherwise(F.lit("OK"))))


# PII column share per table.
@dlt.table(name="gold_pii_summary", comment="PII column share per table")
def gold_pii_summary():
    m = spark.read.table(SILVER_METADATA)
    return (m.groupBy("table_name").agg(
        F.countDistinct("column_name").alias("total_columns"),
        F.countDistinct(F.when(F.lower(F.col("pii_flag").cast("string")) == "true",
                               F.col("column_name"))).alias("pii_columns"))
        .withColumn("pii_share_pct", F.round(F.col("pii_columns") / F.col("total_columns") * 100, 2)))


# Table count by certification level (for a pie/bar chart).
@dlt.table(name="gold_certification_breakdown", comment="Table count by certification level")
def gold_certification_breakdown():
    inv = dlt.read("gold_table_inventory")
    return (inv.groupBy(F.coalesce(F.col("certification_level"), F.lit("UNCLASSIFIED")).alias("certification_level"))
               .agg(F.countDistinct("table_name").alias("table_count")))


# =================== CURATED SILVER BUSINESS TABLES (kept) ===================
SILVER_CUSTOMERS = "workspace.silver.silver_customers"
SILVER_ORDERS = "workspace.silver.silver_orders"
SILVER_PRODUCTS = "workspace.silver.silver_products"
SILVER_PAYMENTS = "workspace.silver.silver_payments"
SILVER_BILLING_TRANSACTIONS = "workspace.silver.silver_billing_transactions"


@dlt.table(name="gold_customers", comment="Curated customer records for reporting and analytics")
def gold_customers():
    return spark.read.table(SILVER_CUSTOMERS)


@dlt.table(name="gold_orders", comment="Curated order records for reporting and analytics")
def gold_orders():
    return spark.read.table(SILVER_ORDERS)


@dlt.table(name="gold_products", comment="Curated product records for reporting and analytics")
def gold_products():
    return spark.read.table(SILVER_PRODUCTS)


@dlt.table(name="gold_payments", comment="Curated payment records for reporting and analytics")
def gold_payments():
    return spark.read.table(SILVER_PAYMENTS)


@dlt.table(name="gold_billing_transactions", comment="Curated billing transaction records for reporting and analytics")
def gold_billing_transactions():
    return spark.read.table(SILVER_BILLING_TRANSACTIONS)