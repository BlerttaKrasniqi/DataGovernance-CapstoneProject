import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window 

BRONZE_PREFIX = spark.conf.get("bronze_prefix", "")
DQ_INVALID_THRESHOLD = float(spark.conf.get("dq_invalid_threshold", "0.10"))
STRUCTURAL_STANDARD_MIN_COLUMNS = int(spark.conf.get("structural_standard_min_columns", "10"))

def bronze(name):
    return f"{BRONZE_PREFIX}{name}"

def dedup_latest(df, key, order_col=None):
    if order_col and order_col in df.columns:
        w = Window.partitionBy(key).orderBy(F.to_date(F.col(order_col)).desc_nulls_last())
        return (df.withColumn("_rn", F.row_number().over(w))
                  .filter(F.col("_rn") == 1)
                  .drop("_rn"))
    return df.dropDuplicates([key])

CUSTOMERS_WARN_RULES = {
    "first_name_present": "first_name IS NOT NULL AND length(trim(first_name)) > 0",
    "email_valid_format": "email IS NULL OR email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'",
    "country_present":    "country IS NOT NULL AND length(trim(country)) > 0",
    "status_allowed":     "status IS NULL OR lower(status) IN ('active','inactive','suspended','closed')",
    "created_date_valid_type": "created_date IS NULL OR to_date(created_date) IS NOT NULL",
}

@dlt.table(name="silver_customers", comment="Cleansed & validated customers.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({"customer_id_not_null": "customer_id IS NOT NULL"})
@dlt.expect_all(CUSTOMERS_WARN_RULES)
def silver_customers():
    df = spark.read.table(bronze("bronze_customers"))
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)
    df = dedup_latest(df, "customer_id", "created_date")   # CHANGED (deterministic dedup): keep latest by created_date
    _cond = " AND ".join(f"({e})" for e in CUSTOMERS_WARN_RULES.values())
    df = df.withColumn("_dq_passed", F.coalesce(F.expr(_cond), F.lit(False)).cast("int"))
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

ORDERS_WARN_RULES = {
    "quantity_positive":         "quantity IS NULL OR quantity > 0",
    "unit_price_non_negative":   "unit_price IS NULL OR unit_price >= 0",
    "total_amount_non_negative": "total_amount IS NULL OR total_amount >= 0",
    "order_date_not_future":     "order_date IS NULL OR to_date(order_date) <= current_date()",
    "order_status_allowed":      "order_status IS NULL OR lower(order_status) IN ('pending','shipped','delivered','cancelled','returned','completed')",
    "order_date_valid_type":     "order_date IS NULL OR to_date(order_date) IS NOT NULL",
}

@dlt.table(name="silver_orders", comment="Cleansed & validated orders.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
})
@dlt.expect_all(ORDERS_WARN_RULES)
def silver_orders():
    df = spark.read.table(bronze("bronze_orders"))
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)
    df = dedup_latest(df, "order_id", "order_date")   # CHANGED (deterministic dedup): keep latest by order_date
    _cond = " AND ".join(f"({e})" for e in ORDERS_WARN_RULES.values())
    df = df.withColumn("_dq_passed", F.coalesce(F.expr(_cond), F.lit(False)).cast("int"))
    return df.withColumn("_silver_loaded_at", F.current_timestamp())
 

PRODUCTS_WARN_RULES = {
    "product_name_present":    "product_name IS NOT NULL AND length(trim(product_name)) > 0",
    "unit_price_non_negative": "unit_price IS NULL OR unit_price >= 0",
    "stock_qty_non_negative":  "stock_quantity IS NULL OR stock_quantity >= 0",
    "created_date_valid_type": "created_date IS NULL OR to_date(created_date) IS NOT NULL",
}
@dlt.table(name="silver_products", comment="Cleansed & validated products.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({"product_id_not_null": "product_id IS NOT NULL"})
@dlt.expect_all(PRODUCTS_WARN_RULES)
def silver_products():
    df = spark.read.table(bronze("bronze_products"))
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)
    df = dedup_latest(df, "product_id", "created_date")   # CHANGED (deterministic dedup): keep latest by created_date
    _cond = " AND ".join(f"({e})" for e in PRODUCTS_WARN_RULES.values())
    df = df.withColumn("_dq_passed", F.coalesce(F.expr(_cond), F.lit(False)).cast("int"))
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

PAYMENTS_WARN_RULES = {
    "amount_non_negative":    "amount IS NULL OR amount >= 0",
    "payment_method_allowed": "payment_method IS NULL OR lower(payment_method) IN ('card','credit_card','debit_card','paypal','bank transfer','bank_transfer','cash')",
    "payment_status_allowed": "payment_status IS NULL OR lower(payment_status) IN ('completed','successful','pending','failed','refunded')",
    "payment_date_valid_type": "payment_date IS NULL OR to_date(payment_date) IS NOT NULL",
}
@dlt.table(name="silver_payments", comment="Cleansed & validated payments.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "payment_id_not_null": "payment_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
})
@dlt.expect_all(PAYMENTS_WARN_RULES)
def silver_payments():
    df = spark.read.table(bronze("bronze_payments"))
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)
    df = dedup_latest(df, "payment_id", "payment_date")   # CHANGED (deterministic dedup): keep latest by payment_date
    _cond = " AND ".join(f"({e})" for e in PAYMENTS_WARN_RULES.values())
    df = df.withColumn("_dq_passed", F.coalesce(F.expr(_cond), F.lit(False)).cast("int"))
    return df.withColumn("_silver_loaded_at", F.current_timestamp())

@dlt.table(name="silver_billing_transactions", comment="Cleansed & validated billing.",
           table_properties={"layer": "silver"})
@dlt.expect_all_or_drop({
    "transaction_id_not_null": "transaction_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
})
def silver_billing_transactions():
    df = spark.read.table(bronze("bronze_billing_transactions"))
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)
    df = dedup_latest(df, "transaction_id")   # CHANGED (deterministic dedup): no date column -> plain distinct on key
    df = df.withColumn("_dq_passed", F.lit(1))
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
    dtypes = df.dtypes
    string_cols = [c for c, t in dtypes if t == "string"]
    exprs = [F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c, _ in dtypes]
    df = df.select(*exprs)

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

@dlt.table(name="quarantine_customers",
           comment="Customer rows that failed at least one data quality rule.",
           table_properties={"layer": "silver"})
def quarantine_customers():
    return dlt.read("silver_customers").filter("_dq_passed = 0")
 
@dlt.table(name="quarantine_orders",
           comment="Order rows that failed at least one data quality rule.",
           table_properties={"layer": "silver"})
def quarantine_orders():
    return dlt.read("silver_orders").filter("_dq_passed = 0")
 
@dlt.table(name="quarantine_products",
           comment="Product rows that failed at least one data quality rule.",
           table_properties={"layer": "silver"})
def quarantine_products():
    return dlt.read("silver_products").filter("_dq_passed = 0")
 
@dlt.table(name="quarantine_payments",
           comment="Payment rows that failed at least one data quality rule.",
           table_properties={"layer": "silver"})
def quarantine_payments():
    return dlt.read("silver_payments").filter("_dq_passed = 0")


