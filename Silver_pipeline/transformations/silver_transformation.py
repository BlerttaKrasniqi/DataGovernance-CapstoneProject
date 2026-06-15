import dlt
from pyspark.sql import functions as F

BRONZE_PREFIX = ""                    # set to "catalog.schema." only if bronze is elsewhere
DQ_INVALID_THRESHOLD = 0.10           # 10% max invalid-record rate
STRUCTURAL_STANDARD_MIN_COLUMNS = 10  # min columns a "standard" table must have

def bronze(name):
    return f"{BRONZE_PREFIX}{name}"

@dlt.table(name="silver_customers", comment="Cleansed & validated customers.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({"customer_id_not_null": "customer_id IS NOT NULL"})
@dlt.expect_all({
    "first_name_present": "first_name IS NOT NULL AND length(trim(first_name)) > 0",
    "email_valid_format": "email IS NULL OR email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
    "country_present":    "country IS NOT NULL AND length(trim(country)) > 0",
    "status_allowed":     "status IS NULL OR lower(status) IN ('active','inactive','suspended','closed')",
})
def silver_customers():
    df = spark.read.table(bronze("bronze_customers"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))
    df = df.dropDuplicates(["customer_id"])
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

@dlt.table(name="silver_orders", comment="Cleansed & validated orders.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
})
@dlt.expect_all({
    "quantity_positive":         "quantity IS NULL OR quantity > 0",
    "unit_price_non_negative":   "unit_price IS NULL OR unit_price >= 0",
    "total_amount_non_negative": "total_amount IS NULL OR total_amount >= 0",
    "order_date_not_future":     "order_date IS NULL OR to_date(order_date) <= current_date()",
    "order_status_allowed":      "order_status IS NULL OR lower(order_status) IN ('pending','shipped','delivered','cancelled','returned','completed')",
})
def silver_orders():
    df = spark.read.table(bronze("bronze_orders"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))
    df = df.dropDuplicates(["order_id"])
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

@dlt.table(name="silver_products", comment="Cleansed & validated products.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({"product_id_not_null": "product_id IS NOT NULL"})
@dlt.expect_all({
    "product_name_present":    "product_name IS NOT NULL AND length(trim(product_name)) > 0",
    "unit_price_non_negative": "unit_price IS NULL OR unit_price >= 0",
    "stock_qty_non_negative":  "stock_quantity IS NULL OR stock_quantity >= 0",
})
def silver_products():
    df = spark.read.table(bronze("bronze_products"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))
    df = df.dropDuplicates(["product_id"])
    return df.withColumn("_silver_loaded_at", F.current_timestamp())


@dlt.table(name="silver_payments", comment="Cleansed & validated payments.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "payment_id_not_null": "payment_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
})
@dlt.expect_all({
    "amount_non_negative":    "amount IS NULL OR amount >= 0",
    "payment_method_allowed": "payment_method IS NULL OR lower(payment_method) IN ('card','credit_card','debit_card','paypal','bank_transfer','cash')",
    "payment_status_allowed": "payment_status IS NULL OR lower(payment_status) IN ('completed','pending','failed','refunded')",
})
def silver_payments():
    df = spark.read.table(bronze("bronze_payments"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))
    df = df.dropDuplicates(["payment_id"])
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

@dlt.table(name="silver_billing_transactions", comment="Cleansed & validated billing.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "transaction_id_not_null": "transaction_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
})
def silver_billing_transactions():
    df = spark.read.table(bronze("bronze_billing_transactions"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))
    df = df.dropDuplicates(["transaction_id"])
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

@dlt.table(name="silver_metadata",
           comment="Metadata catalog with governance compliance flags.",
           table_properties={"layer": "silver"})
@dlt.expect_all({
    "table_name_present": "table_name IS NOT NULL AND length(trim(table_name)) > 0",
    "table_description_present": "table_desc IS NOT NULL AND length(trim(table_desc)) > 0",
    "column_description_present": "column_desc IS NOT NULL AND length(trim(column_desc)) > 0",
    "term_name_present": "term_name IS NOT NULL AND length(trim(term_name)) > 0",
    "data_steward_present": "data_steward IS NOT NULL AND length(trim(data_steward)) > 0",
    "security_classification_present": "security_classification IS NOT NULL",
    "source_system_tag_present": "tag_name IS NOT NULL",
    "pii_flag_present": "pii_flag IS NOT NULL",
})
def silver_metadata():
    df = spark.read.table(bronze("bronze_metadata"))
    for c, t in df.dtypes:
        if t == "string": df = df.withColumn(c, F.trim(F.col(c)))

    def present(col):
        return (F.col(col).isNotNull() & (F.length(F.trim(F.col(col).cast("string"))) > 0)).cast("int")

    df = (df.withColumn("flag_table_desc", present("table_desc"))
            .withColumn("flag_column_desc", present("column_desc"))
            .withColumn("flag_term_name", present("term_name"))
            .withColumn("flag_data_steward", present("data_steward"))
            .withColumn("flag_security_classification", present("security_classification"))
            .withColumn("flag_source_system_tag", present("tag_name"))
            .withColumn("flag_pii_flag", F.col("pii_flag").isNotNull().cast("int")))

    df = df.withColumn("dq_invalid_rate",
        F.when((F.col("total_record_count").isNotNull()) & (F.col("total_record_count") > 0),
               F.col("invalid_record_count") / F.col("total_record_count"))
         .otherwise(F.lit(None).cast("double")))
    df = df.withColumn("flag_dq_pass",
        F.when(F.col("dq_invalid_rate").isNull(), F.lit(0))
         .when(F.col("dq_invalid_rate") < F.lit(DQ_INVALID_THRESHOLD), F.lit(1))
         .otherwise(F.lit(0)))

    required = ["flag_table_desc","flag_column_desc","flag_term_name","flag_data_steward",
                "flag_security_classification","flag_source_system_tag","flag_pii_flag","flag_dq_pass"]
    overall = F.col(required[0])
    for f in required[1:]:
        overall = overall * F.col(f)
    df = df.withColumn("is_compliant", overall)

    df = df.withColumn("missing_elements", F.concat_ws(", ",
        *[F.when(F.col(f) == 0, F.lit(label)) for f, label in [
            ("flag_table_desc","table_description"), ("flag_column_desc","column_description"),
            ("flag_term_name","term_name"), ("flag_data_steward","data_steward"),
            ("flag_security_classification","security_classification"),
            ("flag_source_system_tag","source_system_tag"), ("flag_pii_flag","pii_flag"),
            ("flag_dq_pass","data_quality_below_threshold")]]))
    return df.withColumn("_silver_loaded_at", F.current_timestamp())
