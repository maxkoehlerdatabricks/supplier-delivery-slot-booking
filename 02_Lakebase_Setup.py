# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Lakebase Setup
# MAGIC
# MAGIC This notebook provisions the Lakebase (Databricks Postgres) infrastructure for the
# MAGIC Supplier Delivery Slot Booking application.
# MAGIC
# MAGIC ## What is Lakebase?
# MAGIC
# MAGIC Lakebase is Databricks' built-in PostgreSQL-compatible database service. It provides:
# MAGIC - **OLTP capabilities** for transactional workloads (booking, slot management)
# MAGIC - **Branching model** similar to Git for safe development and testing
# MAGIC - **Delta Lake sync** for replicating analytics tables into Lakebase for low-latency reads
# MAGIC
# MAGIC ## What this notebook creates
# MAGIC
# MAGIC 1. A Lakebase **project** (`delivery-slot-booking`)
# MAGIC 2. A **database** (`delivery_app`) with OLTP tables
# MAGIC 3. Tables: `dock_slot`, `delivery_booking`
# MAGIC 4. Data loaded from Delta Lake into Lakebase
# MAGIC 5. A **dev branch** with its own read-write endpoint
# MAGIC 6. **Synced tables** for `ekko` and `ekpo_enriched`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import subprocess
import json
import os
import time

CATALOG = "classic_stable_4rp118_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

PROJECT = "delivery-slot-booking"
DB_NAME = "delivery_app"

print(f"Project:  {PROJECT}")
print(f"Database: {DB_NAME}")
print(f"Schema:   {FULL_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Delete Lakebase Project
# Delete the Lakebase project (safe to run if it doesn't exist)
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

try:
    w.api_client.do("DELETE", f"/api/2.0/postgres/projects/{PROJECT}")
    print(f"Project '{PROJECT}' deleted successfully.")
except NotFound:
    print(f"Project '{PROJECT}' does not exist - nothing to delete.")

print("\nWorkspace is clean. Ready to run from Step 1.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Lakebase Project

# COMMAND ----------

# DBTITLE 1,Create Lakebase Project
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

w = WorkspaceClient()

try:
    response = w.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects?project_id={PROJECT}",
        body={
            "spec": {
                "display_name": "Delivery Slot Booking"
            }
        }
    )
    print(f"Project '{PROJECT}' creation initiated.")
    print(response)
except BadRequest as e:
    if "already exists" in str(e):
        print(f"Project '{PROJECT}' already exists - continuing.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Wait for Project Ready
# MAGIC
# MAGIC Poll the project status until it is ready to accept connections.

# COMMAND ----------

# DBTITLE 1,Wait for Project Ready
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

MAX_WAIT_SECONDS = 300
POLL_INTERVAL = 15

for attempt in range(MAX_WAIT_SECONDS // POLL_INTERVAL):
    try:
        response = w.api_client.do(
            "GET",
            f"/api/2.0/postgres/projects/{PROJECT}/branches/production/endpoints"
        )
        endpoints = response.get("endpoints", [])
        if endpoints:
            status = endpoints[0].get("status", {}).get("current_state", "UNKNOWN")
            print(f"Attempt {attempt + 1}: Endpoint status = {status}")
            if status == "ACTIVE":
                print("Project is ready!")
                break
        else:
            print(f"Attempt {attempt + 1}: No endpoints found yet...")
    except Exception as e:
        print(f"Attempt {attempt + 1}: {e}")

    time.sleep(POLL_INTERVAL)
else:
    print(f"WARNING: Project not ready after {MAX_WAIT_SECONDS}s. Continuing anyway...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Connection Details
# MAGIC
# MAGIC Helper function to retrieve host, token, and user email for connecting to Lakebase.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def get_connection_details(branch="production", endpoint="primary"):
    """Get Lakebase connection details for the specified branch and endpoint."""
    # Get host
    response = w.api_client.do(
        "GET",
        f"/api/2.0/postgres/projects/{PROJECT}/branches/{branch}/endpoints"
    )
    endpoints = response.get("endpoints", [])
    if not endpoints:
        raise RuntimeError(f"No endpoints found for branch '{branch}'")
    host = endpoints[0]["status"]["hosts"]["host"]

    # Get token
    response = w.api_client.do(
        "POST",
        "/api/2.0/postgres/credentials",
        body={"endpoint": f"projects/{PROJECT}/branches/{branch}/endpoints/{endpoint}"}
    )
    token = response["token"]

    # Get email
    email = w.current_user.me().user_name

    print(f"Host:  {host}")
    print(f"User:  {email}")
    print(f"Token: {token[:10]}...")

    return host, token, email

host, token, email = get_connection_details()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Database

# COMMAND ----------

import psycopg2

conn = psycopg2.connect(
    host=host, port=5432, dbname="postgres",
    user=email, password=token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

try:
    cur.execute(f"CREATE DATABASE {DB_NAME};")
    print(f"Database '{DB_NAME}' created.")
except psycopg2.errors.DuplicateDatabase:
    print(f"Database '{DB_NAME}' already exists - continuing.")
finally:
    cur.close()
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create OLTP Tables
# MAGIC
# MAGIC Create the `dock_slot` and `delivery_booking` tables in the Lakebase database.

# COMMAND ----------

# Refresh token in case it expired
host, token, email = get_connection_details()

DDL_SQL = """
CREATE TABLE IF NOT EXISTS dock_slot (
    slot_id SERIAL PRIMARY KEY,
    dock_id VARCHAR(20) NOT NULL,
    plant_id VARCHAR(10) NOT NULL,
    slot_date DATE NOT NULL,
    time_window_start TIME NOT NULL,
    time_window_end TIME NOT NULL,
    capacity INTEGER DEFAULT 2,
    reserved_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS delivery_booking (
    booking_id SERIAL PRIMARY KEY,
    slot_id INTEGER REFERENCES dock_slot(slot_id),
    vendor_id VARCHAR(20) NOT NULL,
    po_number VARCHAR(20) NOT NULL,
    truck_plate VARCHAR(20),
    driver_name VARCHAR(100),
    status VARCHAR(20) DEFAULT 'requested' CHECK (status IN ('requested','confirmed','checked_in','completed','cancelled')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
"""

import psycopg2

conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=email, password=token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()
cur.execute(DDL_SQL)
print("Tables 'dock_slot' and 'delivery_booking' created.")
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Load Data from Delta Lake into Lakebase
# MAGIC
# MAGIC Read the Delta tables (`dock_slot`, `delivery_booking`) and INSERT them into the
# MAGIC Lakebase Postgres tables using psycopg2.

# COMMAND ----------

import psycopg2

# Refresh credentials
host, token, email = get_connection_details()

conn = psycopg2.connect(
    host=host,
    port=5432,
    database=DB_NAME,
    user=email,
    password=token,
    sslmode="require"
)
cur = conn.cursor()

# --- Load dock_slot ---
print("Loading dock_slot data...")
slots_df = spark.read.table(f"{FULL_SCHEMA}.dock_slot").collect()
for row in slots_df:
    cur.execute("""
        INSERT INTO dock_slot (slot_id, dock_id, plant_id, slot_date, time_window_start, time_window_end, capacity, reserved_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (slot_id) DO NOTHING
    """, (row.slot_id, row.dock_id, row.plant_id, row.slot_date,
          row.time_window_start, row.time_window_end, row.capacity, row.reserved_count))

print(f"Inserted {len(slots_df)} dock_slot rows")

# --- Load delivery_booking ---
print("Loading delivery_booking data...")
bookings_df = spark.read.table(f"{FULL_SCHEMA}.delivery_booking").collect()
for row in bookings_df:
    cur.execute("""
        INSERT INTO delivery_booking (booking_id, slot_id, vendor_id, po_number, truck_plate, driver_name, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (booking_id) DO NOTHING
    """, (row.booking_id, row.slot_id, row.vendor_id, row.po_number,
          row.truck_plate, row.driver_name, row.status, row.created_at, row.updated_at))

print(f"Inserted {len(bookings_df)} delivery_booking rows")

conn.commit()
cur.close()
conn.close()
print("Data load complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Sync Delta Tables to Lakebase
# MAGIC
# MAGIC Replicate `ekko` and `ekpo_enriched` from Delta Lake into Lakebase as read-only
# MAGIC synced tables. This enables low-latency PO lookups from the application without
# MAGIC querying the lakehouse directly.

# COMMAND ----------

# Sync ekko and ekpo_enriched from Delta Lake to Lakebase
# This creates read-only replicas in Lakebase for low-latency PO lookups
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

w = WorkspaceClient()

tables_to_sync = {
    "ekko": ["EBELN"],
    "ekpo_enriched": ["EBELN", "EBELP"],
}

for table_name, pk_cols in tables_to_sync.items():
    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    try:
        response = w.api_client.do(
            "POST",
            f"/api/2.0/postgres/synced_tables?synced_table_id={full_table}",
            body={
                "spec": {
                    "source_table_full_name": full_table,
                    "project": f"projects/{PROJECT}",
                    "branch": f"projects/{PROJECT}/branches/production",
                    "primary_key_columns": pk_cols,
                    "scheduling_policy": "TRIGGERED",
                    "postgres_database": DB_NAME,
                    "create_database_objects_if_missing": True
                }
            }
        )
        print(f"Syncing {table_name}: initiated")
        print(f"  Operation: {response.get('name', response)}")
    except BadRequest as e:
        if "already exists" in str(e).lower():
            print(f"Syncing {table_name}: already synced - continuing.")
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Dev Branch
# MAGIC
# MAGIC Create an isolated `dev` branch from `production` for safe development and testing.
# MAGIC Changes on `dev` do not affect production data.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

w = WorkspaceClient()

try:
    response = w.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects/{PROJECT}/branches?branch_id=dev",
        body={
            "spec": {
                "source_branch": f"projects/{PROJECT}/branches/production",
                "no_expiry": True
            }
        }
    )
    print(f"Dev branch creation initiated.")
    print(response)
except BadRequest as e:
    if "already exists" in str(e).lower():
        print("Dev branch already exists - continuing.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Read-Write Endpoint on Dev Branch
# MAGIC
# MAGIC The dev branch needs its own endpoint so developers can connect and test changes independently.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

w = WorkspaceClient()

try:
    response = w.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects/{PROJECT}/branches/dev/endpoints?endpoint_id=read-write",
        body={
            "spec": {
                "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                "autoscaling_limit_min_cu": 0.5,
                "autoscaling_limit_max_cu": 2.0
            }
        }
    )
    print("Dev branch read-write endpoint creation initiated.")
    print(response)
except BadRequest as e:
    if "already exists" in str(e).lower():
        print("Dev branch read-write endpoint already exists - continuing.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Verify Tables on Both Branches

# COMMAND ----------

import psycopg2

def verify_branch(branch_name, endpoint="primary"):
    """Verify tables exist and contain data on a given branch."""
    print(f"\n{'=' * 60}")
    print(f"Verifying branch: {branch_name}")
    print(f"{'=' * 60}")

    try:
        h, t, e = get_connection_details(branch=branch_name, endpoint=endpoint)
        conn = psycopg2.connect(
            host=h, port=5432, dbname=DB_NAME,
            user=e, password=t, sslmode="require"
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT 'dock_slot' AS tbl, COUNT(*) AS cnt FROM dock_slot
            UNION ALL
            SELECT 'delivery_booking', COUNT(*) FROM delivery_booking;
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} rows")
        cur.close()
        conn.close()
    except Exception as ex:
        print(f"  Could not verify branch '{branch_name}': {ex}")

# Verify production
verify_branch("production")

# Verify dev (endpoint may still be provisioning)
verify_branch("dev", endpoint="read-write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Infrastructure created
# MAGIC
# MAGIC | Resource | Name | Details |
# MAGIC |----------|------|---------|
# MAGIC | Lakebase Project | `delivery-slot-booking` | PostgreSQL-compatible database |
# MAGIC | Database | `delivery_app` | Contains OLTP tables |
# MAGIC | Table | `dock_slot` | Delivery dock time slots |
# MAGIC | Table | `delivery_booking` | Supplier delivery bookings |
# MAGIC | Synced Table | `ekko` | PO headers (read-only from Delta) |
# MAGIC | Synced Table | `ekpo_enriched` | Enriched PO items (read-only from Delta) |
# MAGIC | Branch | `production` | Main branch with primary endpoint |
# MAGIC | Branch | `dev` | Development branch with read-write endpoint |
# MAGIC
# MAGIC ### Connection info
# MAGIC
# MAGIC Use `get_connection_details(branch, endpoint)` to retrieve host, token, and email
# MAGIC for any branch/endpoint combination.
# MAGIC
# MAGIC ### Next steps
# MAGIC - Run `03_Data_Exploration` to visualize the data
# MAGIC - Run `04_App_Deployment` to deploy the web application
