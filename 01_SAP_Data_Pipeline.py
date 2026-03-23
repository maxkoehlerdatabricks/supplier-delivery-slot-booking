# Databricks notebook source
# MAGIC %md
# MAGIC # SAP MM Data Pipeline
# MAGIC
# MAGIC This notebook processes raw SAP MM (Materials Management) purchasing data and creates
# MAGIC an enriched, analytics-ready table.
# MAGIC
# MAGIC ## SAP MM Data Model
# MAGIC
# MAGIC | Table | Description | Key Fields |
# MAGIC |-------|-------------|------------|
# MAGIC | **EKKO** | Purchase Order Headers | EBELN (PO number), LIFNR (vendor), BEDAT (PO date) |
# MAGIC | **EKPO** | Purchase Order Items | EBELN (PO number), EBELP (item), MATNR (material), MENGE (quantity) |
# MAGIC
# MAGIC ## Pipeline Steps
# MAGIC 1. Read raw EKKO and EKPO tables from Delta Lake
# MAGIC 2. Run data quality checks (nulls, valid quantities, deletion flags)
# MAGIC 3. Join headers with items to create an enriched view
# MAGIC 4. Write the enriched table for downstream consumption

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Pipeline target: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Tables

# COMMAND ----------

ekko_df = spark.read.table(f"{FULL_SCHEMA}.ekko")
ekpo_df = spark.read.table(f"{FULL_SCHEMA}.ekpo")

print(f"EKKO rows: {ekko_df.count()}")
print(f"EKPO rows: {ekpo_df.count()}")

# COMMAND ----------

print("EKKO schema:")
ekko_df.printSchema()

# COMMAND ----------

print("EKPO schema:")
ekpo_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC
# MAGIC Validate the raw data before processing:
# MAGIC - EBELN must not be null
# MAGIC - MENGE (quantity) must be > 0
# MAGIC - Filter out deleted items (LOEKZ = 'X')

# COMMAND ----------

from pyspark.sql import functions as F

# --- EKKO quality checks ---
ekko_total = ekko_df.count()
ekko_null_ebeln = ekko_df.filter(F.col("EBELN").isNull()).count()
print(f"EKKO total rows:      {ekko_total}")
print(f"EKKO null EBELN:      {ekko_null_ebeln}")
print(f"EKKO valid rows:      {ekko_total - ekko_null_ebeln}")

# --- EKPO quality checks ---
ekpo_total = ekpo_df.count()
ekpo_null_ebeln = ekpo_df.filter(F.col("EBELN").isNull()).count()
ekpo_zero_qty = ekpo_df.filter(F.col("MENGE") <= 0).count()
ekpo_deleted = ekpo_df.filter(F.col("LOEKZ") == "X").count()
ekpo_valid = ekpo_df.filter(
    (F.col("EBELN").isNotNull()) &
    (F.col("MENGE") > 0) &
    ((F.col("LOEKZ") != "X") | (F.col("LOEKZ").isNull()) | (F.col("LOEKZ") == ""))
).count()

print(f"\nEKPO total rows:      {ekpo_total}")
print(f"EKPO null EBELN:      {ekpo_null_ebeln}")
print(f"EKPO zero quantity:   {ekpo_zero_qty}")
print(f"EKPO deleted (LOEKZ): {ekpo_deleted}")
print(f"EKPO valid rows:      {ekpo_valid}")
print(f"EKPO filtered out:    {ekpo_total - ekpo_valid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality Summary

# COMMAND ----------

quality_summary = [
    {"check": "EKKO null EBELN", "failures": ekko_null_ebeln, "status": "PASS" if ekko_null_ebeln == 0 else "FAIL"},
    {"check": "EKPO null EBELN", "failures": ekpo_null_ebeln, "status": "PASS" if ekpo_null_ebeln == 0 else "FAIL"},
    {"check": "EKPO zero quantity", "failures": ekpo_zero_qty, "status": "PASS" if ekpo_zero_qty == 0 else "WARN"},
    {"check": "EKPO deleted items", "failures": ekpo_deleted, "status": "INFO"},
]

quality_df = spark.createDataFrame(quality_summary)
display(quality_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter and Clean EKPO
# MAGIC
# MAGIC Remove deleted items and invalid quantities before joining.

# COMMAND ----------

ekpo_clean = ekpo_df.filter(
    (F.col("EBELN").isNotNull()) &
    (F.col("MENGE") > 0) &
    ((F.col("LOEKZ") != "X") | (F.col("LOEKZ").isNull()) | (F.col("LOEKZ") == ""))
)

print(f"EKPO after filtering: {ekpo_clean.count()} rows (removed {ekpo_total - ekpo_clean.count()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join EKKO + EKPO (Enriched View)
# MAGIC
# MAGIC LEFT JOIN PO headers with cleaned PO items on EBELN to create a single enriched table.

# COMMAND ----------

ekpo_enriched = (
    ekpo_clean.alias("po")
    .join(ekko_df.alias("hdr"), on="EBELN", how="left")
    .select(
        F.col("hdr.EBELN").alias("EBELN"),
        F.col("po.EBELP").alias("EBELP"),
        F.col("hdr.BUKRS").alias("BUKRS"),
        F.col("hdr.EKORG").alias("EKORG"),
        F.col("hdr.BEDAT").alias("BEDAT"),
        F.col("hdr.LIFNR").alias("LIFNR"),
        F.col("hdr.BSART").alias("BSART"),
        F.col("po.MATNR").alias("MATNR"),
        F.col("po.WERKS").alias("WERKS"),
        F.col("po.MENGE").alias("MENGE"),
        F.col("po.MEINS").alias("MEINS"),
        F.col("po.NETPR").alias("NETPR"),
        F.col("po.ELIKZ").alias("ELIKZ"),
        # Computed columns
        (F.col("po.MENGE") * F.col("po.NETPR")).alias("LINE_VALUE"),
    )
)

print(f"Enriched table rows: {ekpo_enriched.count()}")
display(ekpo_enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Enriched Table

# COMMAND ----------

ekpo_enriched.write.format("delta").mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ekpo_enriched")
print(f"Wrote enriched table to {FULL_SCHEMA}.ekpo_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

verify_df = spark.read.table(f"{FULL_SCHEMA}.ekpo_enriched")

print(f"Row count: {verify_df.count()}")
print(f"\nSchema:")
verify_df.printSchema()

# COMMAND ----------

print("Sample rows:")
display(verify_df.limit(10))

# COMMAND ----------

# Key statistics
print("POs per vendor:")
display(
    verify_df
    .groupBy("LIFNR")
    .agg(
        F.countDistinct("EBELN").alias("po_count"),
        F.count("*").alias("line_count"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value")
    )
    .orderBy("total_value", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What was created
# MAGIC
# MAGIC | Table | Description | Row Count |
# MAGIC |-------|-------------|-----------|
# MAGIC | `ekpo_enriched` | Joined PO headers + items with computed LINE_VALUE | See above |
# MAGIC
# MAGIC ### Data quality actions taken
# MAGIC - Removed items with deletion flag (`LOEKZ = 'X'`)
# MAGIC - Filtered out items with zero or negative quantity
# MAGIC - Validated non-null PO numbers across both tables
# MAGIC
# MAGIC ### How to verify
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM serverless_stable_nyu9oz_catalog.delivery_slot_booking_ppmaxkohler.ekpo_enriched;
# MAGIC SELECT LIFNR, COUNT(DISTINCT EBELN) as po_count FROM ...ekpo_enriched GROUP BY LIFNR;
# MAGIC ```
# MAGIC
# MAGIC ### Next steps
# MAGIC - Run `02_Lakebase_Setup` to provision the Lakebase database
# MAGIC - Run `03_Data_Exploration` to visualize the data
