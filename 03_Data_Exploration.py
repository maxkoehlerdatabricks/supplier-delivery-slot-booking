# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration & Visualization
# MAGIC
# MAGIC This notebook provides rich visualizations across all data layers of the
# MAGIC Supplier Delivery Slot Booking system.
# MAGIC
# MAGIC ## Data Sources
# MAGIC - **ekpo_enriched** - Joined SAP PO headers + items (Delta Lake)
# MAGIC - **dock_slot** - Delivery dock time slots (Delta Lake)
# MAGIC - **delivery_booking** - Supplier delivery bookings (Delta Lake)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

print(f"Reading from: {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Purchase Order Overview
# MAGIC
# MAGIC Analyze the enriched PO data by vendor, material, and value.

# COMMAND ----------

ekpo_df = spark.read.table(f"{FULL_SCHEMA}.ekpo_enriched")
print(f"Total enriched PO items: {ekpo_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### POs by Vendor
# MAGIC
# MAGIC Which vendors have the most purchase orders?

# COMMAND ----------

display(
    ekpo_df
    .groupBy("LIFNR")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Materials by Total Quantity
# MAGIC
# MAGIC Which LIDAR components are ordered in the highest volumes?

# COMMAND ----------

display(
    ekpo_df
    .groupBy("MATNR")
    .agg(
        F.sum("MENGE").alias("total_qty"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value"),
        F.count("*").alias("line_count")
    )
    .orderBy("total_qty", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PO Value by Vendor and Material
# MAGIC
# MAGIC Breakdown of total order value across vendors and materials.

# COMMAND ----------

display(
    ekpo_df
    .groupBy("LIFNR", "MATNR")
    .agg(
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value"),
        F.sum("MENGE").alias("total_qty")
    )
    .orderBy("total_value", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PO Type Distribution

# COMMAND ----------

display(
    ekpo_df
    .groupBy("BSART")
    .agg(
        F.count("*").alias("item_count"),
        F.countDistinct("EBELN").alias("po_count"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value")
    )
    .orderBy("BSART")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Slot Utilization
# MAGIC
# MAGIC Analyze dock slot capacity and reservation patterns across dates and docks.

# COMMAND ----------

slots_df = spark.read.table(f"{FULL_SCHEMA}.dock_slot")
print(f"Total dock slots: {slots_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilization by Date and Dock
# MAGIC
# MAGIC Shows total capacity versus reserved count as a heatmap-style view.

# COMMAND ----------

display(
    slots_df
    .groupBy("slot_date", "dock_id")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.sum("reserved_count").alias("total_reserved")
    )
    .withColumn("utilization_pct", F.round(F.col("total_reserved") / F.col("total_capacity") * 100, 1))
    .orderBy("slot_date", "dock_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Utilization by Dock

# COMMAND ----------

display(
    slots_df
    .groupBy("dock_id")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.sum("reserved_count").alias("total_reserved"),
        F.count("*").alias("slot_count")
    )
    .withColumn("utilization_pct", F.round(F.col("total_reserved") / F.col("total_capacity") * 100, 1))
    .orderBy("dock_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Window Utilization
# MAGIC
# MAGIC Compare morning (08:00-12:00) vs afternoon (13:00-17:00) utilization.

# COMMAND ----------

display(
    slots_df
    .groupBy("time_window_start", "time_window_end")
    .agg(
        F.sum("capacity").alias("total_capacity"),
        F.sum("reserved_count").alias("total_reserved")
    )
    .withColumn("utilization_pct", F.round(F.col("total_reserved") / F.col("total_capacity") * 100, 1))
    .orderBy("time_window_start")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Booking Status Flow
# MAGIC
# MAGIC Analyze delivery booking statuses and their distribution.

# COMMAND ----------

bookings_df = spark.read.table(f"{FULL_SCHEMA}.delivery_booking")
print(f"Total bookings: {bookings_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Status Distribution

# COMMAND ----------

display(
    bookings_df
    .groupBy("status")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bookings by Vendor

# COMMAND ----------

display(
    bookings_df
    .groupBy("vendor_id")
    .agg(
        F.count("*").alias("booking_count"),
        F.countDistinct("po_number").alias("distinct_pos")
    )
    .orderBy("booking_count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Booking Timeline
# MAGIC
# MAGIC When were bookings created over the past week?

# COMMAND ----------

display(
    bookings_df
    .withColumn("created_date", F.to_date("created_at"))
    .groupBy("created_date", "status")
    .count()
    .orderBy("created_date", "status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cross-Table Analysis
# MAGIC
# MAGIC Join bookings with PO data to get a unified view of delivery operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bookings with PO Details
# MAGIC
# MAGIC Join delivery bookings with enriched PO data to see material and value context
# MAGIC for each booking.

# COMMAND ----------

bookings_with_po = (
    bookings_df.alias("b")
    .join(
        ekpo_df.alias("po"),
        F.col("b.po_number") == F.col("po.EBELN"),
        how="left"
    )
    .select(
        F.col("b.booking_id"),
        F.col("b.vendor_id"),
        F.col("b.po_number"),
        F.col("b.status"),
        F.col("b.truck_plate"),
        F.col("b.driver_name"),
        F.col("po.MATNR"),
        F.col("po.MENGE"),
        F.col("po.LINE_VALUE"),
        F.col("po.BSART"),
        F.col("b.created_at")
    )
)

print(f"Bookings with PO details: {bookings_with_po.count()} rows")
display(bookings_with_po.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Value by Booking Status
# MAGIC
# MAGIC How much PO value is in each stage of the delivery process?

# COMMAND ----------

display(
    bookings_with_po
    .groupBy("status")
    .agg(
        F.count("*").alias("line_count"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value"),
        F.round(F.avg("LINE_VALUE"), 2).alias("avg_line_value")
    )
    .orderBy("status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Vendors by Booking Value

# COMMAND ----------

display(
    bookings_with_po
    .groupBy("vendor_id")
    .agg(
        F.countDistinct("booking_id").alias("bookings"),
        F.round(F.sum("LINE_VALUE"), 2).alias("total_value")
    )
    .orderBy("total_value", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Application Flow Diagram
# MAGIC
# MAGIC The delivery slot booking process follows this workflow:
# MAGIC
# MAGIC ```
# MAGIC Supplier
# MAGIC   |
# MAGIC   v
# MAGIC Books Delivery Slot -------> Booking Created (status: requested)
# MAGIC                                      |
# MAGIC                                      v
# MAGIC Warehouse Clerk Reviews -----> Confirms Booking (status: confirmed)
# MAGIC                                      |
# MAGIC                                      v
# MAGIC Truck Arrives at Dock -------> Clerk Checks In (status: checked_in)
# MAGIC                                      |
# MAGIC                                      v
# MAGIC Goods Received & Verified ---> Completed (status: completed)
# MAGIC ```
# MAGIC
# MAGIC ### Status Transitions
# MAGIC
# MAGIC | From | To | Trigger | Actor |
# MAGIC |------|----|---------|-------|
# MAGIC | -- | `requested` | Supplier books a slot | Supplier |
# MAGIC | `requested` | `confirmed` | Clerk reviews and approves | Warehouse Clerk |
# MAGIC | `confirmed` | `checked_in` | Truck arrives, clerk checks in | Warehouse Clerk |
# MAGIC | `checked_in` | `completed` | Goods received and verified | Warehouse Clerk |
# MAGIC | Any | `cancelled` | Booking cancelled | Supplier or Clerk |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This exploration notebook provides visibility into:
# MAGIC
# MAGIC 1. **Purchase Orders** - Vendor distribution, material volumes, order values
# MAGIC 2. **Slot Utilization** - Dock capacity vs. reservations by date and time window
# MAGIC 3. **Booking Flow** - Status distribution and timeline of bookings
# MAGIC 4. **Cross-table Insights** - PO value by booking status, top vendors by delivery value
# MAGIC
# MAGIC Use these visualizations to monitor operations and identify bottlenecks in the
# MAGIC delivery process.
