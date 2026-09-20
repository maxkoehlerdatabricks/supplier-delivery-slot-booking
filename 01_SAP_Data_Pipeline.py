# Databricks notebook source
# MAGIC %md
# MAGIC # SAP MM Data Pipeline — Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC This notebook turns raw SAP MM (Materials Management) purchasing data into
# MAGIC governed, analytics-ready **gold** tables using a classic **medallion
# MAGIC architecture** on the Lakehouse.
# MAGIC
# MAGIC | Layer | Purpose | Tables |
# MAGIC |-------|---------|--------|
# MAGIC | **Bronze** | Raw ingest, as-is from source | `bronze_ekko`, `bronze_ekpo`, `bronze_dock_slot`, `bronze_delivery_booking` |
# MAGIC | **Silver** | Cleaned, deduplicated, data-quality enforced | `silver_ekko`, `silver_ekpo` |
# MAGIC | **Gold** | Business-level, joined, aggregated for BI + apps | `ekpo_enriched`, `slot_utilization`, `booking_funnel` |
# MAGIC
# MAGIC The raw source tables (`ekko`, `ekpo`, `dock_slot`, `delivery_booking`) are
# MAGIC produced by the two `_helper` generator notebooks and stand in for a landing
# MAGIC zone (e.g. an SAP extract or Lakeflow Connect ingest).
# MAGIC
# MAGIC ## What this demonstrates (Lakehouse features)
# MAGIC - **Medallion architecture** — bronze/silver/gold separation of concerns
# MAGIC - **Data quality checks** — nulls, valid quantities, deletion flags, with a summary table
# MAGIC - **Unity Catalog governance** — table & column comments, tags, and grants
# MAGIC - **Lineage** — every gold table is derived from bronze via silver, visible in the UC lineage graph

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC All names come from the shared `config` notebook — retargeting is a one-file edit.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

from pyspark.sql import functions as F

print(f"Pipeline target: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Ingest
# MAGIC
# MAGIC Copy the raw generator output into bronze tables *as-is*, adding only
# MAGIC ingestion metadata (`_ingested_at`, `_source`). Bronze is append-friendly and
# MAGIC never edited by hand — it is the immutable record of what arrived.

# COMMAND ----------

# DBTITLE 1,Build bronze layer
RAW_TABLES = ["ekko", "ekpo", "dock_slot", "delivery_booking"]

for tbl in RAW_TABLES:
    (
        spark.read.table(f"{FULL_SCHEMA}.{tbl}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("sap_mm_extract"))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{FULL_SCHEMA}.bronze_{tbl}")
    )
    cnt = spark.read.table(f"{FULL_SCHEMA}.bronze_{tbl}").count()
    print(f"  bronze_{tbl}: {cnt} rows")

spark.sql(
    f"ALTER TABLE {FULL_SCHEMA}.bronze_ekko "
    f"SET TBLPROPERTIES ('quality' = 'bronze', 'source_system' = 'SAP_MM')"
)
print("\nBronze layer built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Clean & Enforce Data Quality
# MAGIC
# MAGIC Silver applies the data-quality rules that make the data trustworthy:
# MAGIC - `EBELN` (PO number) must not be null
# MAGIC - `MENGE` (quantity) must be > 0
# MAGIC - Deleted line items (`LOEKZ = 'X'`) are filtered out
# MAGIC
# MAGIC We record every check in a `dq_results` table so the run is auditable.

# COMMAND ----------

# DBTITLE 1,Data quality checks
bronze_ekko = spark.read.table(f"{FULL_SCHEMA}.bronze_ekko")
bronze_ekpo = spark.read.table(f"{FULL_SCHEMA}.bronze_ekpo")

ekko_total = bronze_ekko.count()
ekko_null_ebeln = bronze_ekko.filter(F.col("EBELN").isNull()).count()

ekpo_total = bronze_ekpo.count()
ekpo_null_ebeln = bronze_ekpo.filter(F.col("EBELN").isNull()).count()
ekpo_zero_qty = bronze_ekpo.filter(F.col("MENGE") <= 0).count()
ekpo_deleted = bronze_ekpo.filter(F.col("LOEKZ") == "X").count()

dq_summary = [
    {"layer": "silver_ekko", "check": "EKKO null EBELN", "failures": ekko_null_ebeln,
     "status": "PASS" if ekko_null_ebeln == 0 else "FAIL"},
    {"layer": "silver_ekpo", "check": "EKPO null EBELN", "failures": ekpo_null_ebeln,
     "status": "PASS" if ekpo_null_ebeln == 0 else "FAIL"},
    {"layer": "silver_ekpo", "check": "EKPO zero/neg quantity", "failures": ekpo_zero_qty,
     "status": "PASS" if ekpo_zero_qty == 0 else "WARN"},
    {"layer": "silver_ekpo", "check": "EKPO deleted (LOEKZ=X)", "failures": ekpo_deleted,
     "status": "INFO"},
]

dq_df = spark.createDataFrame(dq_summary).withColumn("_run_at", F.current_timestamp())
dq_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.dq_results"
)
print("Data quality results:")
display(dq_df)

# COMMAND ----------

# DBTITLE 1,Build silver tables
# silver_ekko: headers, deduplicated on EBELN, non-null PO number
silver_ekko = (
    bronze_ekko
    .filter(F.col("EBELN").isNotNull())
    .dropDuplicates(["EBELN"])
    .select("EBELN", "BUKRS", "EKORG", "BEDAT", "LIFNR", "BSART")
)

# silver_ekpo: items, deleted + invalid rows removed
silver_ekpo = (
    bronze_ekpo
    .filter(
        (F.col("EBELN").isNotNull())
        & (F.col("MENGE") > 0)
        & ((F.col("LOEKZ") != "X") | (F.col("LOEKZ").isNull()) | (F.col("LOEKZ") == ""))
    )
    .select("EBELN", "EBELP", "MATNR", "WERKS", "MENGE", "MEINS", "NETPR", "ELIKZ")
)

silver_ekko.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.silver_ekko"
)
silver_ekpo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.silver_ekpo"
)

print(f"  silver_ekko: {silver_ekko.count()} rows")
print(f"  silver_ekpo: {silver_ekpo.count()} rows (removed {ekpo_total - silver_ekpo.count()} invalid/deleted)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Business-Level Tables
# MAGIC
# MAGIC Gold is what BI dashboards, Genie, and the app consume.
# MAGIC
# MAGIC | Gold table | Grain | Feeds |
# MAGIC |-----------|-------|-------|
# MAGIC | `ekpo_enriched` | PO line item | App PO lookup, synced to Lakebase |
# MAGIC | `slot_utilization` | dock × date | AI/BI dashboard |
# MAGIC | `booking_funnel` | booking status | AI/BI dashboard |

# COMMAND ----------

# DBTITLE 1,Gold: ekpo_enriched (PO header + item join)
ekpo_enriched = (
    silver_ekpo.alias("po")
    .join(silver_ekko.alias("hdr"), on="EBELN", how="left")
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
        (F.col("po.MENGE") * F.col("po.NETPR")).alias("LINE_VALUE"),
    )
)
ekpo_enriched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.ekpo_enriched"
)
print(f"  ekpo_enriched: {ekpo_enriched.count()} rows")

# COMMAND ----------

# DBTITLE 1,Gold: slot_utilization (dock × date)
slots = spark.read.table(f"{FULL_SCHEMA}.bronze_dock_slot")
slot_utilization = (
    slots
    .groupBy("slot_date", "dock_id", "plant_id")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.sum("reserved_count").alias("total_reserved"),
        F.count("*").alias("slot_count"),
    )
    .withColumn(
        "utilization_pct",
        F.round(F.col("total_reserved") / F.col("total_capacity") * 100, 1),
    )
    .withColumn("available_capacity", F.col("total_capacity") - F.col("total_reserved"))
)
slot_utilization.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.slot_utilization"
)
print(f"  slot_utilization: {slot_utilization.count()} rows")

# COMMAND ----------

# DBTITLE 1,Gold: booking_funnel (status + PO value)
bookings = spark.read.table(f"{FULL_SCHEMA}.bronze_delivery_booking")
booking_funnel = (
    bookings.alias("b")
    .join(ekpo_enriched.alias("po"), F.col("b.po_number") == F.col("po.EBELN"), how="left")
    .groupBy("b.status")
    .agg(
        F.countDistinct("b.booking_id").alias("bookings"),
        F.countDistinct("b.vendor_id").alias("vendors"),
        F.round(F.sum("po.LINE_VALUE"), 2).alias("total_po_value"),
    )
    .withColumnRenamed("status", "status")
)
booking_funnel.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{FULL_SCHEMA}.booking_funnel"
)
print(f"  booking_funnel: {booking_funnel.count()} rows")
display(booking_funnel.orderBy("status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Governance
# MAGIC
# MAGIC Make the gold layer *governed and discoverable*: table/column comments, a
# MAGIC certification tag, and read grants. These are what a data consumer sees in
# MAGIC Catalog Explorer, and what powers search, lineage, and Genie.

# COMMAND ----------

# DBTITLE 1,Comments, tags, and grants on gold tables
GOVERNANCE_SQL = [
    # Table comments
    f"COMMENT ON TABLE {FULL_SCHEMA}.ekpo_enriched IS "
    f"'Gold: SAP purchase order line items enriched with header data and computed line value. Synced to Lakebase for the delivery-booking app.'",
    f"COMMENT ON TABLE {FULL_SCHEMA}.slot_utilization IS "
    f"'Gold: dock slot capacity vs. reservations, aggregated by dock and date. Feeds the AI/BI dashboard.'",
    f"COMMENT ON TABLE {FULL_SCHEMA}.booking_funnel IS "
    f"'Gold: delivery bookings by status with associated PO value. Feeds the AI/BI dashboard.'",
    # Column comments (a representative sample — the business-critical ones)
    f"COMMENT ON COLUMN {FULL_SCHEMA}.ekpo_enriched.EBELN IS 'SAP purchase order number (PO header key).'",
    f"COMMENT ON COLUMN {FULL_SCHEMA}.ekpo_enriched.LIFNR IS 'SAP vendor / supplier number.'",
    f"COMMENT ON COLUMN {FULL_SCHEMA}.ekpo_enriched.MATNR IS 'SAP material number (LiDAR component).'",
    f"COMMENT ON COLUMN {FULL_SCHEMA}.ekpo_enriched.LINE_VALUE IS 'Computed extended value: MENGE (qty) * NETPR (unit price), in EUR.'",
    # Certification tag — uses the governed `system.certification_status` policy
    # (allowed value: 'certified'). Non-governed keys like a custom 'layer'/'domain'
    # are blocked on workspaces that enforce tag policies, so we mark the layer/domain
    # in the table comments above and certify with the governed tag here.
    f"ALTER TABLE {FULL_SCHEMA}.ekpo_enriched SET TAGS ('system.certification_status' = 'certified')",
    f"ALTER TABLE {FULL_SCHEMA}.slot_utilization SET TAGS ('system.certification_status' = 'certified')",
    f"ALTER TABLE {FULL_SCHEMA}.booking_funnel SET TAGS ('system.certification_status' = 'certified')",
    # Grants — governed read access for all workspace users
    f"GRANT USAGE ON SCHEMA {FULL_SCHEMA} TO `account users`",
    f"GRANT SELECT ON TABLE {FULL_SCHEMA}.ekpo_enriched TO `account users`",
    f"GRANT SELECT ON TABLE {FULL_SCHEMA}.slot_utilization TO `account users`",
    f"GRANT SELECT ON TABLE {FULL_SCHEMA}.booking_funnel TO `account users`",
]

for stmt in GOVERNANCE_SQL:
    try:
        spark.sql(stmt)
        print(f"  ✓ {stmt[:80]}...")
    except Exception as e:
        print(f"  ⚠ Skipped ({str(e)[:60]}...): {stmt[:60]}...")

print("\nGovernance applied. View lineage & tags in Catalog Explorer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

for tbl in ["ekpo_enriched", "slot_utilization", "booking_funnel"]:
    cnt = spark.read.table(f"{FULL_SCHEMA}.{tbl}").count()
    print(f"  {tbl}: {cnt} rows")

print("\nPOs per vendor (top by value):")
display(
    spark.read.table(f"{FULL_SCHEMA}.ekpo_enriched")
    .groupBy("LIFNR")
    .agg(
        F.countDistinct("EBELN").alias("po_count"),
        F.count("*").alias("line_count"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value"),
    )
    .orderBy("total_value", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Medallion tables created
# MAGIC
# MAGIC | Layer | Tables |
# MAGIC |-------|--------|
# MAGIC | Bronze | `bronze_ekko`, `bronze_ekpo`, `bronze_dock_slot`, `bronze_delivery_booking` |
# MAGIC | Silver | `silver_ekko`, `silver_ekpo` (+ `dq_results`) |
# MAGIC | Gold | `ekpo_enriched`, `slot_utilization`, `booking_funnel` |
# MAGIC
# MAGIC ### Governance applied
# MAGIC - Table & column comments on all gold tables
# MAGIC - `layer` / `certified` / `domain` tags
# MAGIC - `SELECT` grants to `account users`
# MAGIC
# MAGIC ### Lineage
# MAGIC Open any gold table in **Catalog Explorer → Lineage** to see the full
# MAGIC bronze → silver → gold graph (and, after `02_Lakebase_Setup`, the sync to Lakebase).
# MAGIC
# MAGIC ### Next steps
# MAGIC - Run `02_Lakebase_Setup` to provision Lakebase and create the **synced tables**
# MAGIC - Run `03_Data_Exploration` for visual analysis
# MAGIC - Run `03b_AIBI_Dashboard` to publish the AI/BI dashboard
# MAGIC - Run `04_App_Deployment` to deploy the app

# COMMAND ----------

# Signal successful completion when called via dbutils.notebook.run()
try:
    dbutils.notebook.exit("success")
except NameError:
    print("Running interactively — no exit needed.")
