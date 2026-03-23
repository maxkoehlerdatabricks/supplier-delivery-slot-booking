# Databricks notebook source
# MAGIC %md
# MAGIC # Generate SAP MM Purchasing Data
# MAGIC
# MAGIC This notebook generates realistic SAP MM (Materials Management) purchasing data
# MAGIC for the Supplier Delivery Slot Booking project.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `ekko` - Purchase Order headers (~50 rows)
# MAGIC - `ekpo` - Purchase Order items (~150 rows, 1-5 per PO)
# MAGIC
# MAGIC **SAP MM Data Model:**
# MAGIC - EKKO contains the header-level PO data (vendor, dates, purchasing org)
# MAGIC - EKPO contains the line-item details (materials, quantities, prices)
# MAGIC - Linked by EBELN (PO number)

# COMMAND ----------

# Configuration
CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"Full:    {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate EKKO (Purchase Order Headers)

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)

random.seed(42)

# Parameters
NUM_POS = 50
VENDORS = [f"VENDOR_{str(i).zfill(3)}" for i in range(1, 11)]
PO_TYPES = ["NB", "UB", "FO"]
TODAY = datetime.now().date()

# Generate EKKO rows
ekko_rows = []
for i in range(1, NUM_POS + 1):
    ebeln = f"450000{str(i).zfill(4)}"
    bedat = TODAY - timedelta(days=random.randint(0, 30))
    lifnr = random.choice(VENDORS)
    bsart = random.choice(PO_TYPES)

    ekko_rows.append(Row(
        EBELN=ebeln,
        BUKRS="1000",
        EKORG="1000",
        BEDAT=bedat,
        LIFNR=lifnr,
        BSART=bsart
    ))

ekko_schema = StructType([
    StructField("EBELN", StringType(), False),
    StructField("BUKRS", StringType(), False),
    StructField("EKORG", StringType(), False),
    StructField("BEDAT", DateType(), False),
    StructField("LIFNR", StringType(), False),
    StructField("BSART", StringType(), False),
])

ekko_df = spark.createDataFrame(ekko_rows, schema=ekko_schema)
display(ekko_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate EKPO (Purchase Order Items)

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType

MATERIALS = [
    "LIDAR-SENSOR-01",
    "LIDAR-MOUNT-KIT",
    "LIDAR-OPTICS-MODULE",
    "LIDAR-PCB-BOARD",
    "LIDAR-HOUSING"
]

# Generate EKPO rows (1-5 items per PO)
ekpo_rows = []
for i in range(1, NUM_POS + 1):
    ebeln = f"450000{str(i).zfill(4)}"
    num_items = random.randint(1, 5)

    for j in range(1, num_items + 1):
        ebelp = str(j * 10).zfill(5)
        matnr = random.choice(MATERIALS)
        menge = random.randint(1, 50)
        netpr = round(random.uniform(50.00, 5000.00), 2)
        elikz = "X" if random.random() < 0.1 else ""
        loekz = "X" if random.random() < 0.05 else ""

        ekpo_rows.append(Row(
            EBELN=ebeln,
            EBELP=ebelp,
            MATNR=matnr,
            WERKS="1100",
            MENGE=menge,
            MEINS="EA",
            NETPR=netpr,
            ELIKZ=elikz,
            LOEKZ=loekz
        ))

ekpo_schema = StructType([
    StructField("EBELN", StringType(), False),
    StructField("EBELP", StringType(), False),
    StructField("MATNR", StringType(), False),
    StructField("WERKS", StringType(), False),
    StructField("MENGE", IntegerType(), False),
    StructField("MEINS", StringType(), False),
    StructField("NETPR", DoubleType(), False),
    StructField("ELIKZ", StringType(), True),
    StructField("LOEKZ", StringType(), True),
])

ekpo_df = spark.createDataFrame(ekpo_rows, schema=ekpo_schema)
display(ekpo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta Tables

# COMMAND ----------

# Write EKKO
ekko_df.write.format("delta").mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ekko")
print(f"Wrote {ekko_df.count()} rows to {FULL_SCHEMA}.ekko")

# Write EKPO
ekpo_df.write.format("delta").mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.ekpo")
print(f"Wrote {ekpo_df.count()} rows to {FULL_SCHEMA}.ekpo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify EKKO
print("=" * 60)
print("EKKO (Purchase Order Headers)")
print("=" * 60)
ekko_verify = spark.read.table(f"{FULL_SCHEMA}.ekko")
print(f"Row count: {ekko_verify.count()}")
print(f"Distinct vendors: {ekko_verify.select('LIFNR').distinct().count()}")
print(f"PO type distribution:")
display(ekko_verify.groupBy("BSART").count().orderBy("BSART"))

# COMMAND ----------

# Verify EKPO
print("=" * 60)
print("EKPO (Purchase Order Items)")
print("=" * 60)
ekpo_verify = spark.read.table(f"{FULL_SCHEMA}.ekpo")
print(f"Row count: {ekpo_verify.count()}")
print(f"Distinct POs: {ekpo_verify.select('EBELN').distinct().count()}")
print(f"Material distribution:")
display(ekpo_verify.groupBy("MATNR").count().orderBy("count", ascending=False))

# COMMAND ----------

# Sample rows
print("EKKO sample:")
display(ekko_verify.limit(5))
print("\nEKPO sample:")
display(ekpo_verify.limit(5))
