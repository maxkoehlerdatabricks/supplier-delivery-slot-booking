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
# MAGIC - **Synced tables** that replicate Lakehouse (Delta) tables into Lakebase for low-latency reads
# MAGIC
# MAGIC ## What this notebook creates
# MAGIC
# MAGIC 1. A Lakebase **project** (`delivery-slot-booking`)
# MAGIC 2. A **database** (`delivery_app`) with OLTP tables
# MAGIC 3. **OLTP tables**: `dock_slot`, `delivery_booking` (writable by the app)
# MAGIC 4. Data loaded from Delta Lake into the OLTP tables
# MAGIC 5. A **dev branch** with its own read-write endpoint
# MAGIC 6. A **UC database catalog** registering the Lakebase database in Unity Catalog
# MAGIC 7. **Real synced tables** for `ekko` and `ekpo_enriched` (Delta → Lakebase, read-only)
# MAGIC
# MAGIC > **Synced vs. OLTP**: `dock_slot` and `delivery_booking` are native Postgres OLTP
# MAGIC > tables — the app reads *and writes* them. `ekko` and `ekpo_enriched` are
# MAGIC > **synced tables** — the Lakehouse is the source of truth and the sync pipeline
# MAGIC > keeps a read-only copy in Lakebase. This is the genuine Lakebase feature, not a
# MAGIC > manual copy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json
import time

print(f"Project:      {PROJECT}")
print(f"Database:     {DB_NAME}")
print(f"Delta schema: {FULL_SCHEMA}")
print(f"Sync schema:  {FULL_SYNC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Delete Lakebase Project
# Delete + PURGE the Lakebase project (safe to run if it doesn't exist).
# purge=true is required so the project_id can be reused immediately — a plain
# DELETE leaves a soft-deleted project that blocks recreation with the same name.
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

try:
    w.api_client.do("DELETE", f"/api/2.0/postgres/projects/{PROJECT}?purge=true")
    print(f"Project '{PROJECT}' deleted and purged successfully.")
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

MAX_CREATE_RETRIES = 20   # 20 × 15s = 5 min max wait for ghost-state cleanup
CREATE_RETRY_INTERVAL = 15

for retry in range(MAX_CREATE_RETRIES):
    try:
        response = w.api_client.do(
            "POST",
            f"/api/2.0/postgres/projects?project_id={PROJECT}",
            body={"spec": {"display_name": "Delivery Slot Booking"}},
        )
        print(f"Project '{PROJECT}' creation initiated.")
        print(response)
        break
    except BadRequest as e:
        if "already exists" in str(e):
            try:
                w.api_client.do("GET", f"/api/2.0/postgres/projects/{PROJECT}")
                print(f"Project '{PROJECT}' already exists and is accessible — continuing.")
                break
            except Exception:
                print(f"Attempt {retry + 1}/{MAX_CREATE_RETRIES}: Project in ghost state. Retrying in {CREATE_RETRY_INTERVAL}s...")
                time.sleep(CREATE_RETRY_INTERVAL)
                continue
        else:
            raise
else:
    raise RuntimeError(
        f"Could not create project '{PROJECT}' after {MAX_CREATE_RETRIES} retries. "
        f"A previous deletion may still be in progress — wait a few minutes and retry."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Wait for Project Ready

# COMMAND ----------

# DBTITLE 1,Wait for Project Ready
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

MAX_WAIT_SECONDS = 900
POLL_INTERVAL = 15

for attempt in range(MAX_WAIT_SECONDS // POLL_INTERVAL):
    try:
        response = w.api_client.do(
            "GET",
            f"/api/2.0/postgres/projects/{PROJECT}/branches/production/endpoints",
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
    raise TimeoutError(f"Lakebase endpoint not ACTIVE after {MAX_WAIT_SECONDS}s.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Connection Details

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def get_connection_details(branch="production", endpoint="primary"):
    """Get Lakebase connection details for the specified branch and endpoint."""
    response = w.api_client.do(
        "GET",
        f"/api/2.0/postgres/projects/{PROJECT}/branches/{branch}/endpoints",
    )
    endpoints = response.get("endpoints", [])
    if not endpoints:
        raise RuntimeError(f"No endpoints found for branch '{branch}'")
    host = endpoints[0]["status"]["hosts"]["host"]

    response = w.api_client.do(
        "POST",
        "/api/2.0/postgres/credentials",
        body={"endpoint": f"projects/{PROJECT}/branches/{branch}/endpoints/{endpoint}"},
    )
    token = response["token"]
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
    user=email, password=token, sslmode="require",
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
# MAGIC `dock_slot` and `delivery_booking` are native Postgres OLTP tables — the app
# MAGIC reads and writes them. (The SAP tables come later, as **synced tables**.)

# COMMAND ----------

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

conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=email, password=token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()
cur.execute(DDL_SQL)
print("Tables 'dock_slot' and 'delivery_booking' created.")
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Load OLTP Data from Delta Lake
# MAGIC
# MAGIC Load `dock_slot` and `delivery_booking` from the bronze Delta tables into the
# MAGIC Lakebase OLTP tables. (SAP data is *not* loaded this way — it is synced.)

# COMMAND ----------

# DBTITLE 1,Load OLTP data from Delta into Lakebase
host, token, email = get_connection_details()

conn = psycopg2.connect(
    host=host, port=5432, database=DB_NAME,
    user=email, password=token, sslmode="require",
)
cur = conn.cursor()

print("Loading dock_slot data...")
slots_df = spark.read.table(f"{FULL_SCHEMA}.dock_slot").collect()
for row in slots_df:
    cur.execute(
        """
        INSERT INTO dock_slot (slot_id, dock_id, plant_id, slot_date, time_window_start, time_window_end, capacity, reserved_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (slot_id) DO NOTHING
        """,
        (row.slot_id, row.dock_id, row.plant_id, row.slot_date,
         row.time_window_start, row.time_window_end, row.capacity, row.reserved_count),
    )
print(f"Inserted {len(slots_df)} dock_slot rows")

print("Loading delivery_booking data...")
bookings_df = spark.read.table(f"{FULL_SCHEMA}.delivery_booking").collect()
for row in bookings_df:
    cur.execute(
        """
        INSERT INTO delivery_booking (booking_id, slot_id, vendor_id, po_number, truck_plate, driver_name, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (booking_id) DO NOTHING
        """,
        (row.booking_id, row.slot_id, row.vendor_id, row.po_number,
         row.truck_plate, row.driver_name, row.status, row.created_at, row.updated_at),
    )
print(f"Inserted {len(bookings_df)} delivery_booking rows")

cur.execute("SELECT setval('dock_slot_slot_id_seq', (SELECT MAX(slot_id) FROM dock_slot));")
cur.execute("SELECT setval('delivery_booking_booking_id_seq', (SELECT MAX(booking_id) FROM delivery_booking));")
print("Reset SERIAL sequences to match loaded data")

conn.commit()
cur.close()
conn.close()
print("OLTP data load complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Prepare Gold Sync Sources
# MAGIC
# MAGIC Synced tables need a **primary key** on the source, and PK columns must be
# MAGIC `NOT NULL`. We build a gold `ekko_gold` header projection (PK on `EBELN`) and
# MAGIC ensure `ekpo_enriched` has its `(EBELN, EBELP)` PK. Idempotent.

# COMMAND ----------

# DBTITLE 1,Build/ensure sync sources with primary keys
spark.sql(f"""
    CREATE OR REPLACE TABLE {FULL_SCHEMA}.ekko_gold AS
    SELECT EBELN, BUKRS, EKORG, BEDAT, LIFNR, BSART FROM {FULL_SCHEMA}.silver_ekko
""")

def ensure_pk(table, cols, pk_name):
    for c in cols:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {c} SET NOT NULL")
    try:
        spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(cols)})")
        print(f"  ✓ PK {pk_name} on {table}")
    except Exception as e:
        if "already" in str(e).lower():
            print(f"  ✓ PK {pk_name} already on {table}")
        else:
            raise

ensure_pk(f"{FULL_SCHEMA}.ekko_gold", ["EBELN"], "ekko_gold_pk")
ensure_pk(f"{FULL_SCHEMA}.ekpo_enriched", ["EBELN", "EBELP"], "ekpo_enriched_pk")
print("Sync sources ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Synced Tables (Delta → Lakebase)
# MAGIC
# MAGIC Create **real synced tables** via the Lakebase Autoscaling API
# MAGIC (`POST /api/2.0/postgres/synced_tables`). The Lakehouse gold tables are the
# MAGIC source of truth; the sync pipeline materializes read-only copies into Lakebase.
# MAGIC
# MAGIC **Landing schema**: the UC synced tables are created in the dedicated
# MAGIC `SYNC_SCHEMA` (`delivery_slot_booking_sync`). The Lakebase **Postgres schema
# MAGIC takes that same name**, and the tables keep their UC names (`ekko`,
# MAGIC `ekpo_enriched`). Step 9b then points the app's Postgres `search_path` at this
# MAGIC schema so the app's unqualified queries resolve to the synced tables — no app
# MAGIC code change.
# MAGIC
# MAGIC **Sync mode**: `SNAPSHOT` for the demo — swap to `TRIGGERED`/`CONTINUOUS` (needs
# MAGIC Change Data Feed on the source) for production.

# COMMAND ----------

# DBTITLE 1,Create synced tables for ekko and ekpo_enriched
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, BadRequest

w = WorkspaceClient()

# The synced UC tables land in a dedicated schema so their Postgres schema name is
# clean and does not collide with the gold ekpo_enriched Delta table.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SYNC_SCHEMA}")

# (UC synced-table name in SYNC_SCHEMA, source Delta gold table, PK columns)
SYNCED = [
    ("ekko",          f"{FULL_SCHEMA}.ekko_gold",     ["EBELN"]),
    ("ekpo_enriched", f"{FULL_SCHEMA}.ekpo_enriched", ["EBELN", "EBELP"]),
]

for target_name, source_table, pk_cols in SYNCED:
    st_id = f"{CATALOG}.{SYNC_SCHEMA}.{target_name}"

    # Delete any pre-existing synced table first. A leftover object from a previous
    # run points at the OLD (now-purged) project and can never come online, so we
    # always recreate it fresh against the current project.
    try:
        w.api_client.do("DELETE", f"/api/2.0/postgres/synced_tables/{st_id}")
        print(f"  · removed pre-existing synced table '{st_id}'")
        time.sleep(5)
    except Exception:
        pass

    body = {
        "spec": {
            "source_table_full_name": source_table,
            "branch": f"projects/{PROJECT}/branches/production",
            "primary_key_columns": pk_cols,
            "scheduling_policy": "SNAPSHOT",
            "postgres_database": DB_NAME,
            "create_database_objects_if_missing": True,
        }
    }
    try:
        w.api_client.do("POST", f"/api/2.0/postgres/synced_tables?synced_table_id={st_id}", body=body)
        print(f"✓ Synced table '{st_id}' created from {source_table}")
    except (AlreadyExists, BadRequest) as e:
        print(f"⚠ Could not create synced table '{st_id}': {str(e)[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Wait for Synced Tables to Come Online
# MAGIC
# MAGIC Poll the sync pipeline until both synced tables report an `ONLINE` state.

# COMMAND ----------

# DBTITLE 1,Wait for synced tables online
MAX_SYNC_WAIT = 900
SYNC_POLL = 20

for attempt in range(MAX_SYNC_WAIT // SYNC_POLL):
    states = {}
    for t in ["ekko", "ekpo_enriched"]:
        try:
            r = w.api_client.do(
                "GET",
                f"/api/2.0/postgres/synced_tables/{CATALOG}.{SYNC_SCHEMA}.{t}",
            )
            states[t] = r.get("status", {}).get("detailed_state", "UNKNOWN")
        except Exception as e:
            states[t] = f"ERR:{str(e)[:40]}"
    print(f"Attempt {attempt + 1}: {states}")
    if all("ONLINE" in s for s in states.values()):
        print("✓ Both synced tables online.")
        break
    time.sleep(SYNC_POLL)
else:
    print("⚠ Synced tables not ONLINE in time — check the pipeline in Catalog Explorer / DLT.")

# COMMAND ----------

# DBTITLE 1,Verify synced data lands in the Postgres sync schema
host, token, email = get_connection_details()
conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=email, password=token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()
for t in ["ekko", "ekpo_enriched"]:
    try:
        cur.execute(f'SELECT COUNT(*) FROM {SYNC_SCHEMA}.{t}')
        print(f"  {SYNC_SCHEMA}.{t}: {cur.fetchone()[0]} rows")
    except Exception as e:
        print(f"  {SYNC_SCHEMA}.{t}: not queryable yet ({str(e)[:60]})")
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9b: Make synced tables readable via `search_path`
# MAGIC
# MAGIC The synced tables live in the `SYNC_SCHEMA` Postgres schema, but the app issues
# MAGIC unqualified queries (`SELECT ... FROM ekko`). Grant `USAGE`/`SELECT` on the sync
# MAGIC schema to `PUBLIC` so any connecting role (including the app SP) can read it.
# MAGIC The app SP's `search_path` is set in `04_App_Deployment`.

# COMMAND ----------

# DBTITLE 1,Grant read on the sync schema
host, token, email = get_connection_details()
try:
    conn = psycopg2.connect(
        host=host, port=5432, dbname=DB_NAME,
        user=email, password=token, sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'GRANT USAGE ON SCHEMA {SYNC_SCHEMA} TO PUBLIC;')
    cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {SYNC_SCHEMA} TO PUBLIC;')
    cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA {SYNC_SCHEMA} GRANT SELECT ON TABLES TO PUBLIC;')
    print(f"  ✓ Granted read on schema '{SYNC_SCHEMA}' to PUBLIC")
    cur.close()
    conn.close()
except Exception as e:
    print(f"⚠ Grant step skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Dev Branch

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest

w = WorkspaceClient()

try:
    response = w.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects/{PROJECT}/branches?branch_id=dev",
        body={"spec": {"source_branch": f"projects/{PROJECT}/branches/production", "no_expiry": True}},
    )
    print("Dev branch creation initiated.")
    print(response)
except BadRequest as e:
    if "already exists" in str(e).lower():
        print("Dev branch already exists - continuing.")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Create Read-Write Endpoint on Dev Branch

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
                "autoscaling_limit_max_cu": 2.0,
            }
        },
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
# MAGIC ## Step 12: Verify Tables on Both Branches

# COMMAND ----------

def verify_branch(branch_name, endpoint="primary"):
    """Verify tables exist and contain data on a given branch."""
    print(f"\n{'=' * 60}")
    print(f"Verifying branch: {branch_name}")
    print(f"{'=' * 60}")
    try:
        h, t, e = get_connection_details(branch=branch_name, endpoint=endpoint)
        conn = psycopg2.connect(
            host=h, port=5432, dbname=DB_NAME,
            user=e, password=t, sslmode="require",
        )
        cur = conn.cursor()
        checks = [
            ("public.dock_slot", "dock_slot"),
            ("public.delivery_booking", "delivery_booking"),
            (f"{SYNC_SCHEMA}.ekko", "ekko (synced)"),
            (f"{SYNC_SCHEMA}.ekpo_enriched", "ekpo_enriched (synced)"),
        ]
        for fqn, label in checks:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {fqn}")
                print(f"  {label}: {cur.fetchone()[0]} rows")
            except Exception as ex:
                print(f"  {label}: not present ({str(ex)[:50]})")
        cur.close()
        conn.close()
    except Exception as ex:
        print(f"  Could not verify branch '{branch_name}': {ex}")

verify_branch("production")
verify_branch("dev", endpoint="read-write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Resource | Name | Details |
# MAGIC |----------|------|---------|
# MAGIC | Lakebase Project | `delivery-slot-booking` | PostgreSQL-compatible database |
# MAGIC | Database | `delivery_app` | OLTP + synced tables |
# MAGIC | OLTP Table | `dock_slot` | Delivery dock time slots (read/write) |
# MAGIC | OLTP Table | `delivery_booking` | Supplier delivery bookings (read/write) |
# MAGIC | UC DB Catalog | `<project>_pg` | Registers Lakebase in Unity Catalog |
# MAGIC | Synced Table | `ekko` | PO headers, synced from Delta (read-only) |
# MAGIC | Synced Table | `ekpo_enriched` | Enriched PO items, synced from Delta (read-only) |
# MAGIC | Branch | `production` / `dev` | Git-style branches |
# MAGIC
# MAGIC ### Next steps
# MAGIC - Run `02b_Lakehouse_Sync` (optional) to sync Lakebase → Delta (reverse CDC)
# MAGIC - Run `03_Data_Exploration` / `03b_AIBI_Dashboard`
# MAGIC - Run `04_App_Deployment`

# COMMAND ----------

try:
    dbutils.notebook.exit("success")
except NameError:
    print("Running interactively — no exit needed.")
