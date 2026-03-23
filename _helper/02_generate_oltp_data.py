# Databricks notebook source
# MAGIC %md
# MAGIC # Generate OLTP Application Data
# MAGIC
# MAGIC This notebook generates the app-local OLTP data for the Supplier Delivery Slot Booking system.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `dock_slot` - Available delivery time slots across docks (~60 rows)
# MAGIC - `delivery_booking` - Supplier delivery bookings (~40 rows)
# MAGIC
# MAGIC **Dependencies:**
# MAGIC - Requires `ekko` table to exist (run `01_generate_sap_data` first)
# MAGIC - Uses valid vendor IDs and PO numbers from SAP data

# COMMAND ----------

# Configuration
CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Full schema: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read EKKO for Valid Vendor IDs and PO Numbers

# COMMAND ----------

ekko_df = spark.read.table(f"{FULL_SCHEMA}.ekko")
vendor_ids = [row.LIFNR for row in ekko_df.select("LIFNR").distinct().collect()]
po_numbers = [row.EBELN for row in ekko_df.select("EBELN").collect()]

print(f"Available vendors: {vendor_ids}")
print(f"Available PO numbers: {len(po_numbers)} POs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dock Slots
# MAGIC
# MAGIC Creates slots across 3 docks, for the next 10 business days, with morning (08:00-12:00)
# MAGIC and afternoon (13:00-17:00) windows.

# COMMAND ----------

import random
from datetime import datetime, timedelta, date
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType
)

random.seed(123)

DOCKS = ["DOCK-A", "DOCK-B", "DOCK-C"]
TIME_WINDOWS = [
    ("08:00", "12:00"),
    ("13:00", "17:00"),
]

# Calculate next 10 business days
def get_next_business_days(start_date, count):
    """Return the next `count` business days starting from start_date."""
    business_days = []
    current = start_date
    while len(business_days) < count:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday=0, Friday=4
            business_days.append(current)
    return business_days

today = date.today()
business_days = get_next_business_days(today, 10)

# Generate dock slot rows
slot_rows = []
slot_id = 1
for slot_date in business_days:
    for dock_id in DOCKS:
        for start_time, end_time in TIME_WINDOWS:
            capacity = random.choice([2, 3])
            slot_rows.append(Row(
                slot_id=slot_id,
                dock_id=dock_id,
                plant_id="1100",
                slot_date=slot_date,
                time_window_start=start_time,
                time_window_end=end_time,
                capacity=capacity,
                reserved_count=0  # Will be updated after bookings
            ))
            slot_id += 1

slot_schema = StructType([
    StructField("slot_id", IntegerType(), False),
    StructField("dock_id", StringType(), False),
    StructField("plant_id", StringType(), False),
    StructField("slot_date", DateType(), False),
    StructField("time_window_start", StringType(), False),
    StructField("time_window_end", StringType(), False),
    StructField("capacity", IntegerType(), False),
    StructField("reserved_count", IntegerType(), False),
])

slots_df = spark.createDataFrame(slot_rows, schema=slot_schema)
print(f"Generated {slots_df.count()} dock slots")
display(slots_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Delivery Bookings
# MAGIC
# MAGIC Creates ~40 bookings distributed across statuses:
# MAGIC - 10 requested, 10 confirmed, 10 checked_in, 10 completed

# COMMAND ----------

from pyspark.sql.types import TimestampType

GERMAN_FIRST_NAMES = [
    "Hans", "Klaus", "Peter", "Wolfgang", "Dieter",
    "Stefan", "Thomas", "Michael", "Andreas", "Markus",
    "Bernd", "Juergen", "Frank", "Uwe", "Ralf"
]
GERMAN_LAST_NAMES = [
    "Mueller", "Schmidt", "Schneider", "Fischer", "Weber",
    "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann",
    "Koch", "Richter", "Wolf", "Braun", "Zimmermann"
]
PLATE_CITIES = ["M", "B", "HH", "K", "F", "S", "D", "N", "HB", "DO"]

STATUSES = (["requested"] * 10 + ["confirmed"] * 10 +
            ["checked_in"] * 10 + ["completed"] * 10)
random.shuffle(STATUSES)

now = datetime.now()
available_slot_ids = [row.slot_id for row in slots_df.collect()]

booking_rows = []
for i in range(1, 41):
    slot_id = random.choice(available_slot_ids)
    vendor_id = random.choice(vendor_ids)
    po_number = random.choice(po_numbers)
    city = random.choice(PLATE_CITIES)
    letters = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=2))
    digits = random.randint(1000, 9999)
    truck_plate = f"{city}-{letters}-{digits}"
    driver_name = f"{random.choice(GERMAN_FIRST_NAMES)} {random.choice(GERMAN_LAST_NAMES)}"
    status = STATUSES[i - 1]

    created_at = now - timedelta(
        days=random.randint(0, 7),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    updated_at = created_at + timedelta(
        hours=random.randint(0, 12),
        minutes=random.randint(0, 59)
    )

    booking_rows.append(Row(
        booking_id=i,
        slot_id=slot_id,
        vendor_id=vendor_id,
        po_number=po_number,
        truck_plate=truck_plate,
        driver_name=driver_name,
        status=status,
        created_at=created_at,
        updated_at=updated_at
    ))

booking_schema = StructType([
    StructField("booking_id", IntegerType(), False),
    StructField("slot_id", IntegerType(), False),
    StructField("vendor_id", StringType(), False),
    StructField("po_number", StringType(), False),
    StructField("truck_plate", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("status", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
])

bookings_df = spark.createDataFrame(booking_rows, schema=booking_schema)
print(f"Generated {bookings_df.count()} delivery bookings")
display(bookings_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Dock Slot Reserved Counts
# MAGIC
# MAGIC Set `reserved_count` on each dock slot to match the actual number of bookings assigned to it.

# COMMAND ----------

from pyspark.sql import functions as F

# Count bookings per slot
booking_counts = (
    bookings_df
    .groupBy("slot_id")
    .agg(F.count("*").alias("actual_reserved"))
)

# Update slots with actual reserved counts
updated_slots_df = (
    slots_df
    .join(booking_counts, on="slot_id", how="left")
    .withColumn(
        "reserved_count",
        F.coalesce(F.col("actual_reserved"), F.lit(0))
    )
    .drop("actual_reserved")
)

print("Slot utilization summary:")
display(
    updated_slots_df
    .groupBy("dock_id")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.sum("reserved_count").alias("total_reserved")
    )
    .orderBy("dock_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta Tables

# COMMAND ----------

# Write dock_slot
updated_slots_df.write.format("delta").mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.dock_slot")
print(f"Wrote {updated_slots_df.count()} rows to {FULL_SCHEMA}.dock_slot")

# Write delivery_booking
bookings_df.write.format("delta").mode("overwrite").saveAsTable(f"{FULL_SCHEMA}.delivery_booking")
print(f"Wrote {bookings_df.count()} rows to {FULL_SCHEMA}.delivery_booking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify dock_slot
print("=" * 60)
print("DOCK_SLOT")
print("=" * 60)
ds_verify = spark.read.table(f"{FULL_SCHEMA}.dock_slot")
print(f"Row count: {ds_verify.count()}")
print(f"Date range: {ds_verify.agg(F.min('slot_date'), F.max('slot_date')).collect()[0]}")
display(ds_verify.limit(5))

# COMMAND ----------

# Verify delivery_booking
print("=" * 60)
print("DELIVERY_BOOKING")
print("=" * 60)
db_verify = spark.read.table(f"{FULL_SCHEMA}.delivery_booking")
print(f"Row count: {db_verify.count()}")
print("Status distribution:")
display(db_verify.groupBy("status").count().orderBy("status"))

# COMMAND ----------

# Sample bookings
display(db_verify.limit(5))
