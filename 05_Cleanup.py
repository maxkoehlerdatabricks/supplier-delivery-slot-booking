# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup — Delete All Demo Assets
# MAGIC
# MAGIC This notebook **deletes all resources** created by the Supplier Delivery Slot Booking demo.
# MAGIC Run each cell sequentially. Every step is safe to re-run if something was already deleted.
# MAGIC
# MAGIC ### What gets deleted
# MAGIC
# MAGIC | Resource | Name / Location |
# MAGIC |----------|----------------|
# MAGIC | Databricks App | `delivery-slot-booking` |
# MAGIC | Lakebase project | `delivery-slot-booking` (cascades to all branches, endpoints, databases) |
# MAGIC | Secret scope | `delivery-slot-booking` |
# MAGIC | SQL Warehouse | `delivery-slot-booking-warehouse` |
# MAGIC | Delta tables | `ekko`, `ekpo`, `ekpo_enriched`, `dock_slot`, `delivery_booking` |
# MAGIC | Schema | `{CATALOG}.{SCHEMA}` |
# MAGIC | Workspace files | Uploaded app source at `/Workspace/Users/{user}/apps/delivery-slot-booking` |
# MAGIC
# MAGIC ### What is preserved
# MAGIC
# MAGIC - The **Unity Catalog** (catalog itself)
# MAGIC - Your **Databricks CLI profile** and authentication
# MAGIC - This repository (local clone)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update these values to match the settings you used during setup.

# COMMAND ----------

import subprocess
import json
import time

CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

PROFILE = "fe-vm-fevm-serverless-stable-nyu9oz"
APP_NAME = "delivery-slot-booking"
PROJECT = "delivery-slot-booking"
WAREHOUSE_NAME = "delivery-slot-booking-warehouse"
SECRET_SCOPE = "delivery-slot-booking"

print(f"Catalog:        {CATALOG}")
print(f"Schema:         {SCHEMA}")
print(f"Profile:        {PROFILE}")
print(f"App:            {APP_NAME}")
print(f"Lakebase:       {PROJECT}")
print(f"Warehouse:      {WAREHOUSE_NAME}")
print(f"Secret scope:   {SECRET_SCOPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Delete the Databricks App
# MAGIC
# MAGIC Stops and deletes the deployed web application and its service principal.

# COMMAND ----------

print("Deleting Databricks App...")
result = subprocess.run(
    ["databricks", "apps", "delete", APP_NAME, "-p", PROFILE],
    capture_output=True, text=True,
)
if result.returncode == 0:
    print(f"✓ App '{APP_NAME}' deleted successfully.")
elif "does not exist" in (result.stderr or "").lower() or "not found" in (result.stderr or "").lower():
    print(f"⊘ App '{APP_NAME}' does not exist — skipping.")
else:
    print(f"⚠ App deletion returned: {result.stderr.strip() or result.stdout.strip()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete the Lakebase Project
# MAGIC
# MAGIC Deleting the project cascades to **all** branches, endpoints, databases, and data.
# MAGIC This is irreversible.

# COMMAND ----------

print("Deleting Lakebase project...")

# First, unprotect the production branch if it's protected
result = subprocess.run(
    [
        "databricks", "postgres", "update-branch",
        f"projects/{PROJECT}/branches/production",
        "spec.is_protected",
        "--json", '{"spec": {"is_protected": false}}',
        "-p", PROFILE,
    ],
    capture_output=True, text=True,
)
if result.returncode == 0:
    print("  Unprotected production branch.")

# Delete the project (cascades to everything)
result = subprocess.run(
    [
        "databricks", "postgres", "delete-project",
        f"projects/{PROJECT}",
        "-p", PROFILE,
    ],
    capture_output=True, text=True,
)
if result.returncode == 0:
    print(f"✓ Lakebase project '{PROJECT}' deleted successfully.")
elif "not found" in (result.stderr or "").lower() or "does not exist" in (result.stderr or "").lower():
    print(f"⊘ Lakebase project '{PROJECT}' does not exist — skipping.")
else:
    print(f"⚠ Project deletion returned: {result.stderr.strip() or result.stdout.strip()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delete the Secret Scope
# MAGIC
# MAGIC Removes the secret scope that stored the Lakebase connection password.

# COMMAND ----------

print("Deleting secret scope...")
result = subprocess.run(
    ["databricks", "secrets", "delete-scope", SECRET_SCOPE, "-p", PROFILE],
    capture_output=True, text=True,
)
if result.returncode == 0:
    print(f"✓ Secret scope '{SECRET_SCOPE}' deleted.")
elif "does not exist" in (result.stderr or "").lower() or "RESOURCE_DOES_NOT_EXIST" in (result.stderr or ""):
    print(f"⊘ Secret scope '{SECRET_SCOPE}' does not exist — skipping.")
else:
    print(f"⚠ Secret scope deletion returned: {result.stderr.strip() or result.stdout.strip()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Delete the SQL Warehouse
# MAGIC
# MAGIC Deletes the serverless SQL warehouse created for this demo.

# COMMAND ----------

print("Looking up SQL warehouse...")
result = subprocess.run(
    ["databricks", "warehouses", "list", "-p", PROFILE, "-o", "json"],
    capture_output=True, text=True,
)
if result.returncode == 0:
    warehouses = json.loads(result.stdout)
    # Handle both list and dict-with-warehouses formats
    if isinstance(warehouses, dict):
        warehouses = warehouses.get("warehouses", [])
    target = [w for w in warehouses if w.get("name") == WAREHOUSE_NAME]
    if target:
        wh_id = target[0]["id"]
        print(f"  Found warehouse '{WAREHOUSE_NAME}' (ID: {wh_id}). Deleting...")
        del_result = subprocess.run(
            ["databricks", "warehouses", "delete", wh_id, "-p", PROFILE],
            capture_output=True, text=True,
        )
        if del_result.returncode == 0:
            print(f"✓ SQL Warehouse '{WAREHOUSE_NAME}' deleted.")
        else:
            print(f"⚠ Warehouse deletion returned: {del_result.stderr.strip()}")
    else:
        print(f"⊘ SQL Warehouse '{WAREHOUSE_NAME}' not found — skipping.")
else:
    print(f"⚠ Could not list warehouses: {result.stderr.strip()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Drop Delta Tables & Schema
# MAGIC
# MAGIC Drops all demo tables from Unity Catalog, then drops the schema.
# MAGIC The catalog itself is preserved.

# COMMAND ----------

TABLES = ["ekko", "ekpo", "ekpo_enriched", "dock_slot", "delivery_booking"]

print(f"Dropping tables from {FULL_SCHEMA}...")
for table in TABLES:
    full_name = f"{FULL_SCHEMA}.{table}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        print(f"  ✓ Dropped {full_name}")
    except Exception as e:
        print(f"  ⚠ Could not drop {full_name}: {e}")

print(f"\nDropping schema {FULL_SCHEMA}...")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {FULL_SCHEMA} CASCADE")
    print(f"✓ Schema {FULL_SCHEMA} dropped.")
except Exception as e:
    print(f"⚠ Could not drop schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Delete Workspace Files
# MAGIC
# MAGIC Removes the uploaded app source code from the Databricks workspace.

# COMMAND ----------

# Get current user for workspace path
result = subprocess.run(
    ["databricks", "current-user", "me", "-p", PROFILE, "-o", "json"],
    capture_output=True, text=True,
)
if result.returncode == 0:
    user = json.loads(result.stdout)["userName"]
else:
    user = "unknown"
    print(f"⚠ Could not determine current user: {result.stderr.strip()}")

workspace_path = f"/Workspace/Users/{user}/apps/{APP_NAME}"
print(f"Deleting workspace files at: {workspace_path}")

result = subprocess.run(
    ["databricks", "workspace", "delete", workspace_path, "--recursive", "-p", PROFILE],
    capture_output=True, text=True,
)
if result.returncode == 0:
    print(f"✓ Workspace files deleted.")
elif "not found" in (result.stderr or "").lower() or "does not exist" in (result.stderr or "").lower():
    print(f"⊘ Workspace path does not exist — skipping.")
else:
    print(f"⚠ Workspace deletion returned: {result.stderr.strip() or result.stdout.strip()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC If all steps succeeded, the following resources have been cleaned up:
# MAGIC
# MAGIC | Resource | Status |
# MAGIC |----------|--------|
# MAGIC | Databricks App (`delivery-slot-booking`) | Deleted |
# MAGIC | Lakebase project (`delivery-slot-booking`) | Deleted (all branches, endpoints, data) |
# MAGIC | Secret scope (`delivery-slot-booking`) | Deleted |
# MAGIC | SQL Warehouse (`delivery-slot-booking-warehouse`) | Deleted |
# MAGIC | Delta tables (ekko, ekpo, ekpo_enriched, dock_slot, delivery_booking) | Dropped |
# MAGIC | Schema (`delivery_slot_booking_ppmaxkohler`) | Dropped |
# MAGIC | Workspace files | Deleted |
# MAGIC
# MAGIC **Preserved:** The Unity Catalog (`serverless_stable_nyu9oz_catalog`) remains intact.
# MAGIC
# MAGIC To re-deploy the demo, re-run the notebooks starting from `_helper/01_generate_sap_data.py`.
