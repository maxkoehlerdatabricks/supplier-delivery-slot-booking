# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Lakehouse Sync (Lakebase → Delta)  — optional
# MAGIC
# MAGIC This notebook enables the **reverse** sync direction: it continuously replicates
# MAGIC the Lakebase OLTP tables (`dock_slot`, `delivery_booking`) back into Unity Catalog
# MAGIC as managed **Delta** tables using **CDC** (the `wal2delta` Postgres extension).
# MAGIC
# MAGIC This is what lets BI dashboards and ML models consume live transactional data
# MAGIC without querying Lakebase directly — every insert/update/delete in Postgres is
# MAGIC captured to Delta (SCD Type 2, with `_pg_change_type` / `_pg_lsn` / `_timestamp`
# MAGIC system columns).
# MAGIC
# MAGIC > **Entitlement**: Lakehouse Sync (reverse) may not be enabled on every workspace.
# MAGIC > This notebook **detects and reports** if it is unavailable rather than failing
# MAGIC > the whole run — it is optional in the demo flow.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, BadRequest, PermissionDenied

w = WorkspaceClient()

print(f"Project:            {PROJECT}")
print(f"Database:           {DB_NAME}")
print(f"Reverse-sync target: {FULL_SYNC_BACK_SCHEMA}")

# Ensure the target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SYNC_BACK_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Lakehouse Sync
# MAGIC
# MAGIC Configure a reverse-sync pipeline from the Lakebase `public` schema to the UC
# MAGIC target schema. Tables need `REPLICA IDENTITY FULL` so updates/deletes carry the
# MAGIC full old row image for CDC.

# COMMAND ----------

# DBTITLE 1,Set REPLICA IDENTITY FULL on OLTP tables
import psycopg2

def get_conn():
    resp = w.api_client.do("GET", f"/api/2.0/postgres/projects/{PROJECT}/branches/production/endpoints")
    host = resp["endpoints"][0]["status"]["hosts"]["host"]
    cred = w.api_client.do("POST", "/api/2.0/postgres/credentials",
                           body={"endpoint": f"projects/{PROJECT}/branches/production/endpoints/primary"})
    email = w.current_user.me().user_name
    return psycopg2.connect(host=host, port=5432, dbname=DB_NAME,
                            user=email, password=cred["token"], sslmode="require")

try:
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    for t in ["dock_slot", "delivery_booking"]:
        cur.execute(f"ALTER TABLE {t} REPLICA IDENTITY FULL;")
        print(f"  ✓ {t}: REPLICA IDENTITY FULL")
    cur.close()
    conn.close()
except Exception as e:
    print(f"⚠ Could not set REPLICA IDENTITY: {e}")

# COMMAND ----------

# DBTITLE 1,Create the reverse-sync (Lakebase → Delta)
# NOTE: verify the exact reverse-sync API against the live workspace — the endpoint
# shape has changed across Lakebase releases. This cell degrades gracefully.
reverse_sync_body = {
    "project": PROJECT,
    "source_branch": "production",
    "source_database": DB_NAME,
    "source_schema": "public",
    "target_catalog": CATALOG,
    "target_schema": SYNC_BACK_SCHEMA,
    "tables": ["dock_slot", "delivery_booking"],
    "mode": "CONTINUOUS",
}

try:
    resp = w.api_client.do(
        "POST",
        f"/api/2.0/postgres/projects/{PROJECT}/branches/production/lakehouse-sync",
        body=reverse_sync_body,
    )
    print("✓ Lakehouse Sync enabled.")
    print(json.dumps(resp, indent=2)[:800])
except (NotFound, BadRequest, PermissionDenied) as e:
    print("⚠ Lakehouse Sync (reverse) is not available or the API differs on this workspace.")
    print(f"   Details: {str(e)[:300]}")
    print("\n   This step is OPTIONAL. In the demo, show the 'Lakehouse sync' tab in the")
    print("   Lakebase UI and walk through the configuration there (Part 2.8 of the script).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify (if enabled)
# MAGIC
# MAGIC Once syncing, the OLTP tables appear as Delta tables in the target schema with
# MAGIC CDC system columns. Query them from SQL / dashboards / ML.

# COMMAND ----------

for t in ["dock_slot", "delivery_booking"]:
    fqn = f"{FULL_SYNC_BACK_SCHEMA}.{t}"
    try:
        cnt = spark.read.table(fqn).count()
        print(f"  {fqn}: {cnt} rows (synced)")
    except Exception:
        print(f"  {fqn}: not present yet — sync may still be initializing or not enabled.")

# COMMAND ----------

try:
    dbutils.notebook.exit("success")
except NameError:
    print("Running interactively — no exit needed.")
