# Databricks notebook source
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

CATALOG = "serverless_stable_nyu9oz_catalog"
SCHEMA = "delivery_slot_booking_ppmaxkohler"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

PROFILE = "fe-vm-fevm-serverless-stable-nyu9oz"
PROJECT = "delivery-slot-booking"
DB_NAME = "delivery_app"

print(f"Profile:  {PROFILE}")
print(f"Project:  {PROJECT}")
print(f"Database: {DB_NAME}")
print(f"Schema:   {FULL_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Lakebase Project

# COMMAND ----------

result = subprocess.run([
    "databricks", "postgres", "create-project", PROJECT,
    "--json", json.dumps({"spec": {"display_name": "Delivery Slot Booking"}}),
    "--no-wait", "-p", PROFILE
], capture_output=True, text=True)
print(result.stdout or result.stderr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Wait for Project Ready
# MAGIC
# MAGIC Poll the project status until it is ready to accept connections.

# COMMAND ----------

MAX_WAIT_SECONDS = 300
POLL_INTERVAL = 15

for attempt in range(MAX_WAIT_SECONDS // POLL_INTERVAL):
    result = subprocess.run([
        "databricks", "postgres", "get-project", PROJECT,
        "-p", PROFILE, "-o", "json"
    ], capture_output=True, text=True)

    if result.returncode == 0:
        project_info = json.loads(result.stdout)
        status = project_info.get("status", {}).get("state", "UNKNOWN")
        print(f"Attempt {attempt + 1}: Project status = {status}")

        if status in ("ACTIVE", "READY", "STATE_ACTIVE"):
            print("Project is ready!")
            break
    else:
        print(f"Attempt {attempt + 1}: {result.stderr.strip()}")

    time.sleep(POLL_INTERVAL)
else:
    print(f"WARNING: Project not ready after {MAX_WAIT_SECONDS}s. Continuing anyway...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Connection Details
# MAGIC
# MAGIC Helper function to retrieve host, token, and user email for connecting to Lakebase.

# COMMAND ----------

def get_connection_details(branch="production", endpoint="primary"):
    """Get Lakebase connection details for the specified branch and endpoint."""
    # Get host
    result = subprocess.run([
        "databricks", "postgres", "list-endpoints",
        f"projects/{PROJECT}/branches/{branch}",
        "-p", PROFILE, "-o", "json"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to list endpoints: {result.stderr}")
    endpoints = json.loads(result.stdout)
    host = endpoints[0]["status"]["hosts"]["host"]

    # Get token
    result = subprocess.run([
        "databricks", "postgres", "generate-database-credential",
        f"projects/{PROJECT}/branches/{branch}/endpoints/{endpoint}",
        "-p", PROFILE, "-o", "json"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to generate credential: {result.stderr}")
    token = json.loads(result.stdout)["token"]

    # Get email
    result = subprocess.run([
        "databricks", "current-user", "me",
        "-p", PROFILE, "-o", "json"
    ], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get current user: {result.stderr}")
    email = json.loads(result.stdout)["userName"]

    print(f"Host:  {host}")
    print(f"User:  {email}")
    print(f"Token: {token[:10]}...")

    return host, token, email

host, token, email = get_connection_details()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Database

# COMMAND ----------

result = subprocess.run([
    "psql", f"host={host} port=5432 dbname=postgres user={email} sslmode=require",
    "-c", f"CREATE DATABASE {DB_NAME};"
], env={**os.environ, "PGPASSWORD": token}, capture_output=True, text=True)

if "already exists" in (result.stderr or ""):
    print(f"Database '{DB_NAME}' already exists - continuing.")
else:
    print(result.stdout or result.stderr)

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

result = subprocess.run([
    "psql", f"host={host} port=5432 dbname={DB_NAME} user={email} sslmode=require",
    "-c", DDL_SQL
], env={**os.environ, "PGPASSWORD": token}, capture_output=True, text=True)
print(result.stdout or result.stderr)

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
for table_name in ["ekko", "ekpo_enriched"]:
    result = subprocess.run([
        "databricks", "database", "create-synced-database-table",
        f"{CATALOG}.{SCHEMA}.{table_name}",
        "--database-instance-name", PROJECT,
        "--logical-database-name", DB_NAME,
        "-p", PROFILE
    ], capture_output=True, text=True)
    print(f"Syncing {table_name}: {result.stdout or result.stderr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Dev Branch
# MAGIC
# MAGIC Create an isolated `dev` branch from `production` for safe development and testing.
# MAGIC Changes on `dev` do not affect production data.

# COMMAND ----------

result = subprocess.run([
    "databricks", "postgres", "create-branch", f"projects/{PROJECT}", "dev",
    "--json", json.dumps({
        "spec": {
            "source_branch": f"projects/{PROJECT}/branches/production",
            "no_expiry": True
        }
    }),
    "-p", PROFILE
], capture_output=True, text=True)
print(result.stdout or result.stderr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Read-Write Endpoint on Dev Branch
# MAGIC
# MAGIC The dev branch needs its own endpoint so developers can connect and test changes independently.

# COMMAND ----------

result = subprocess.run([
    "databricks", "postgres", "create-endpoint", f"projects/{PROJECT}/branches/dev", "read-write",
    "--json", json.dumps({
        "spec": {
            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
            "autoscaling_limit_min_cu": 0.5,
            "autoscaling_limit_max_cu": 2.0
        }
    }),
    "-p", PROFILE
], capture_output=True, text=True)
print(result.stdout or result.stderr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Verify Tables on Both Branches

# COMMAND ----------

def verify_branch(branch_name, endpoint="primary"):
    """Verify tables exist and contain data on a given branch."""
    print(f"\n{'=' * 60}")
    print(f"Verifying branch: {branch_name}")
    print(f"{'=' * 60}")

    try:
        h, t, e = get_connection_details(branch=branch_name, endpoint=endpoint)
        result = subprocess.run([
            "psql", f"host={h} port=5432 dbname={DB_NAME} user={e} sslmode=require",
            "-c", "SELECT 'dock_slot' as tbl, COUNT(*) as cnt FROM dock_slot UNION ALL SELECT 'delivery_booking', COUNT(*) FROM delivery_booking;"
        ], env={**os.environ, "PGPASSWORD": t}, capture_output=True, text=True)
        print(result.stdout or result.stderr)
    except Exception as ex:
        print(f"Could not verify branch '{branch_name}': {ex}")

# Verify production
verify_branch("production")

# Verify dev (endpoint may still be provisioning)
try:
    verify_branch("dev", endpoint="read-write")
except Exception as ex:
    print(f"Dev branch not ready yet: {ex}")

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
