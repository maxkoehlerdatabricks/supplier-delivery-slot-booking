# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Demo Configuration
# MAGIC
# MAGIC Single source of truth for catalog, schema, Lakebase, and app names.
# MAGIC **Retargeting the demo to a new workspace is a one-file edit** — change the
# MAGIC values below and every notebook picks them up via `%run ./config`.
# MAGIC
# MAGIC Usage inside a notebook:
# MAGIC ```python
# MAGIC # MAGIC %run ../config      # from _helper/
# MAGIC # MAGIC %run ./config       # from repo root
# MAGIC ```
# MAGIC After the `%run`, the names below are available as plain Python variables.

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# Unity Catalog target
# ─────────────────────────────────────────────────────────────────────────────
CATALOG = "serverless_stable_7qzrfp_catalog"
SCHEMA = "delivery_slot_booking"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Synced-table (Delta → Lakebase) UC landing schema.
# The Lakebase Postgres schema takes THIS name, and the synced tables keep their
# UC table names (ekko, ekpo_enriched). The app's Postgres search_path is set to
# this schema so its unqualified queries resolve to the synced tables.
SYNC_SCHEMA = "delivery_slot_booking_sync"
FULL_SYNC_SCHEMA = f"{CATALOG}.{SYNC_SCHEMA}"

# Lakehouse Sync (Lakebase → Delta) target schema — reverse-sync landing zone
# (NOTE: reverse sync is NOT supported on the Lakebase Autoscaling tier; 02b gates on this.)
SYNC_BACK_SCHEMA = "delivery_slot_booking_live"
FULL_SYNC_BACK_SCHEMA = f"{CATALOG}.{SYNC_BACK_SCHEMA}"

# ─────────────────────────────────────────────────────────────────────────────
# Lakebase (Databricks Postgres)
# ─────────────────────────────────────────────────────────────────────────────
PROJECT = "delivery-slot-booking"     # Lakebase project / app name
DB_NAME = "delivery_app"              # Postgres database inside the project

# ─────────────────────────────────────────────────────────────────────────────
# Databricks App
# ─────────────────────────────────────────────────────────────────────────────
APP_NAME = "delivery-slot-booking"    # kept identical to PROJECT by convention

# ─────────────────────────────────────────────────────────────────────────────
# AI/BI Dashboard
# ─────────────────────────────────────────────────────────────────────────────
DASHBOARD_NAME = "Supplier Delivery & Purchasing — Overview"

print("Demo configuration loaded:")
print(f"  Catalog / schema : {FULL_SCHEMA}")
print(f"  Reverse-sync zone: {FULL_SYNC_BACK_SCHEMA}")
print(f"  Lakebase project : {PROJECT}  (db: {DB_NAME})")
print(f"  App name         : {APP_NAME}")
print(f"  Dashboard        : {DASHBOARD_NAME}")
