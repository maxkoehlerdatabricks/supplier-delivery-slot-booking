# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # Supplier Delivery Slot Booking — App Deployment
# MAGIC
# MAGIC This notebook deploys the **Supplier Delivery Slot Booking** application as a
# MAGIC Databricks App. It is designed to be **self-contained and reproducible** on any
# MAGIC Databricks workspace with Lakebase Autoscaling enabled.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
# MAGIC │  React Frontend  │─────▶│  FastAPI Backend  │─────▶│   Lakebase DB    │
# MAGIC │  (Static SPA)    │      │  (Python API)     │      │  (delivery_app)  │
# MAGIC └──────────────────┘      └──────────────────┘      └──────────────────┘
# MAGIC                                    │
# MAGIC                                    ▼
# MAGIC                           ┌──────────────────┐
# MAGIC                           │   Delta Lake     │
# MAGIC                           │   (SAP tables)   │
# MAGIC                           └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | Component | Role |
# MAGIC |---|---|
# MAGIC | **React Frontend** | Single-page app for suppliers (slot booking) and warehouse clerks (check-in/confirm) |
# MAGIC | **FastAPI Backend** | REST API for booking CRUD, slot management, PO lookups |
# MAGIC | **Lakebase** | OLTP Postgres database for transactional data (bookings, slots, PO copies) |
# MAGIC | **Delta Lake** | Source-of-truth SAP data (`ekko`, `ekpo_enriched`), loaded into Lakebase at setup time |
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC The backend uses the **Databricks Python SDK** (pre-authenticated via the app's
# MAGIC service principal) to dynamically resolve the Lakebase endpoint host and generate
# MAGIC short-lived OAuth database credentials at startup. No hardcoded passwords or
# MAGIC CLI profiles are needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before running this notebook on a **new workspace**, ensure:
# MAGIC
# MAGIC 1. **Lakebase Autoscaling** is enabled on the workspace (contact your admin if
# MAGIC    you don't see Lakebase in the app switcher).
# MAGIC 2. **Lakebase project** `delivery-slot-booking` is provisioned and the
# MAGIC    `delivery_app` database exists with all tables populated.  
# MAGIC    → Run **`02_Lakebase_Setup`** first.
# MAGIC 3. The **`app/`** directory (sibling of this notebook) contains the application
# MAGIC    source code (frontend `dist/` already built, backend Python files).
# MAGIC 4. The notebook is attached to **serverless compute** (or any cluster with the
# MAGIC    Databricks Python SDK available).
# MAGIC
# MAGIC > **Note:** No SQL warehouse is required. The app connects directly to Lakebase
# MAGIC > via the Postgres protocol. The Databricks CLI is also *not* required — all
# MAGIC > operations use the Databricks Python SDK which is pre-authenticated on
# MAGIC > Databricks compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC The cells below set workspace-relative paths for deployment.
# MAGIC **No hardcoded profiles or warehouse IDs** — everything is derived from the
# MAGIC current user context.
# MAGIC
# MAGIC | Variable | Purpose |
# MAGIC |---|---|
# MAGIC | `APP_NAME` | Databricks App name (must match the Lakebase project name) |
# MAGIC | `USER` | Current user email (auto-detected via SDK) |
# MAGIC | `WORKSPACE_APP_PATH` | Target path where app files are uploaded for deployment |
# MAGIC
# MAGIC > **Note:** No SQL warehouse is needed. The app connects directly to Lakebase
# MAGIC > via the Postgres protocol, and all credentials are resolved dynamically via
# MAGIC > the Databricks SDK.

# COMMAND ----------

# DBTITLE 1,App Configuration
import json
import os

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

APP_NAME = "delivery-slot-booking"
USER = w.current_user.me().user_name
WORKSPACE_APP_PATH = f"/Workspace/Users/{USER}/apps/{APP_NAME}"

print(f"App name:         {APP_NAME}")
print(f"User:             {USER}")
print(f"Workspace path:   {WORKSPACE_APP_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Configuration (`app.yaml`)
# MAGIC
# MAGIC The `app.yaml` in the `app/` directory defines runtime settings. When deploying
# MAGIC to a **new workspace**, the only value you may need to change is **`PGDATABASE`**
# MAGIC — only if you used a different database name in `02_Lakebase_Setup`.
# MAGIC
# MAGIC > **No SQL warehouse resource binding needed.** The app connects directly to
# MAGIC > Lakebase via the Postgres protocol. Any `resources` section referencing a
# MAGIC > SQL warehouse is automatically stripped during the upload step.
# MAGIC
# MAGIC The `app.yaml` used for deployment:
# MAGIC
# MAGIC ```yaml
# MAGIC command:
# MAGIC   - uvicorn
# MAGIC   - app:app
# MAGIC   - --workers
# MAGIC   - "4"
# MAGIC env:
# MAGIC   - name: LAKEBASE_PROJECT
# MAGIC     value: "delivery-slot-booking"
# MAGIC   - name: LAKEBASE_BRANCH
# MAGIC     value: "production"
# MAGIC   - name: LAKEBASE_ENDPOINT
# MAGIC     value: "primary"
# MAGIC   - name: PGDATABASE
# MAGIC     value: "delivery_app"
# MAGIC ```
# MAGIC
# MAGIC > **Important:** No `PGHOST`, `PGUSER`, or `PGPASSWORD` env vars are needed.
# MAGIC > The backend resolves the host and generates OAuth credentials dynamically
# MAGIC > via the Databricks SDK, authenticated as the app's service principal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload App Files to Workspace
# MAGIC
# MAGIC Copies the `app/` source directory to the workspace path where Databricks Apps
# MAGIC expects to find it. This uses standard Python file operations (no CLI needed).
# MAGIC
# MAGIC > **Tip:** Do not include `node_modules/` in the source — it causes deployment
# MAGIC > timeouts. Only the pre-built `frontend/dist/` folder is needed.

# COMMAND ----------

# DBTITLE 1,Upload app files
import shutil
import os
import re

source_dir = f"/Workspace/Users/{USER}/supplier-delivery-slot-booking/app"
dest_dir = WORKSPACE_APP_PATH

if not os.path.exists(source_dir):
    raise FileNotFoundError(f"App source not found at {source_dir}")

if os.path.exists(dest_dir):
    shutil.rmtree(dest_dir)
shutil.copytree(source_dir, dest_dir)

# ── Patch app.yaml: remove SQL warehouse resource binding (not needed) ───────
app_yaml = os.path.join(dest_dir, "app.yaml")
if os.path.exists(app_yaml):
    with open(app_yaml) as f:
        text = f.read()
    # Strip the top-level 'resources:' block (and all its indented children)
    patched = re.sub(r'\nresources:\s*\n(?:[ \t]+.*\n?)*', '\n', text)
    if patched != text:
        with open(app_yaml, "w") as f:
            f.write(patched)
        print("Patched app.yaml: removed 'resources' section (no SQL warehouse needed)")
    else:
        print("app.yaml already has no resources section")

# Ensure app.yml copy exists (some runtimes expect .yml)
app_yml = os.path.join(dest_dir, "app.yml")
if os.path.exists(app_yaml) and not os.path.exists(app_yml):
    shutil.copy2(app_yaml, app_yml)
    print("Created app.yml from app.yaml")
elif os.path.exists(app_yml):
    # Re-copy patched version
    shutil.copy2(app_yaml, app_yml)
    print("Updated app.yml with patched app.yaml")

file_count = sum(len(files) for _, _, files in os.walk(dest_dir))
print(f"\nApp files synced successfully ({file_count} files).")
print(f"  From: {source_dir}")
print(f"  To:   {dest_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Databricks App
# MAGIC
# MAGIC Registers the application with the Databricks Apps service. No SQL warehouse
# MAGIC resource binding is needed — the app connects directly to Lakebase via the
# MAGIC Postgres protocol.
# MAGIC
# MAGIC > **Replicating?** No configuration changes needed — just run the cells in order.

# COMMAND ----------

# DBTITLE 1,Create app
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists

w = WorkspaceClient()

app_config = {
    "name": APP_NAME,
    "description": "Supplier Delivery Slot Booking",
}

try:
    response = w.api_client.do("POST", "/api/2.0/apps", body=app_config)
    print(f"App '{APP_NAME}' created.")
    print(response)
except AlreadyExists:
    print(f"App '{APP_NAME}' already exists - will deploy new version.")

w.api_client.do(
    "PATCH",
    f"/api/2.0/permissions/apps/{APP_NAME}",
    body={"access_control_list": [
        {"group_name": "users", "permission_level": "CAN_USE"}
    ]}
)
print(f"Granted CAN_USE on app '{APP_NAME}' to all workspace users.")

# COMMAND ----------

# DBTITLE 1,Step 2b: Grant SP Permissions Pre-Deployment
# MAGIC %md
# MAGIC ## Step 2b: Grant Service Principal Permissions (Pre-Deployment)
# MAGIC
# MAGIC The app's service principal must have Lakebase access **before** deployment,
# MAGIC otherwise the connection pool fails on startup. This cell grants:
# MAGIC - `CAN_MANAGE` on the Lakebase project (so the SDK can resolve endpoints)
# MAGIC - A Postgres OAuth role for the SP
# MAGIC - `SELECT`, `INSERT`, `UPDATE`, `DELETE` on all tables
# MAGIC - `USAGE`, `SELECT` on all sequences (required for SERIAL auto-increment)

# COMMAND ----------

# DBTITLE 1,Grant SP all Lakebase permissions
import time
import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

PROJECT = "delivery-slot-booking"
DB_NAME = "delivery_app"

# Wait for the service principal to be assigned (async after app creation)
for _attempt in range(20):
    app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    if "service_principal_name" in app_info:
        break
    print("Waiting for service principal assignment...")
    time.sleep(5)
else:
    raise RuntimeError("Service principal not assigned after 100s. Check app status.")

sp_name = app_info["service_principal_name"]
sp_client_id = app_info["service_principal_client_id"]
print(f"Service Principal: {sp_name}")
print(f"Client ID:         {sp_client_id}")

w.api_client.do(
    "PATCH",
    f"/api/2.0/permissions/database-projects/{PROJECT}",
    body={"access_control_list": [
        {"service_principal_name": sp_name, "permission_level": "CAN_MANAGE"}
    ]}
)
print(f"\n\u2713 Granted CAN_MANAGE on project '{PROJECT}' to {sp_name}")

response = w.api_client.do(
    "GET",
    f"/api/2.0/postgres/projects/{PROJECT}/branches/production/endpoints"
)
ep = response["endpoints"][0]
host = ep["status"]["hosts"]["host"]

cred = w.api_client.do(
    "POST",
    "/api/2.0/postgres/credentials",
    body={"endpoint": ep["name"]}
)
token = cred["token"]
email = w.current_user.me().user_name

conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=email, password=token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth;")
print("\u2713 databricks_auth extension ready")

try:
    cur.execute(f"SELECT databricks_create_role('{sp_client_id}', 'service_principal');")
    print(f"\u2713 Created Postgres OAuth role for {sp_client_id}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"\u2713 Postgres OAuth role already exists for {sp_client_id}")
        conn.rollback()
        conn.autocommit = True
    else:
        raise

grants = [
    f'GRANT CONNECT ON DATABASE {DB_NAME} TO "{sp_client_id}"',
    f'GRANT USAGE ON SCHEMA public TO "{sp_client_id}"',
    f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp_client_id}"',
    f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{sp_client_id}"',
]
for g in grants:
    cur.execute(g)
    print(f"\u2713 {g[:70]}...")

cur.close()
conn.close()
print(f"\n\u2713 SP has full Lakebase access \u2014 ready for deployment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deploy the App
# MAGIC
# MAGIC Triggers a deployment from the uploaded source code. The cell first waits for
# MAGIC the app's compute to reach `ACTIVE` state (takes ~2 min on first create), then
# MAGIC initiates the deployment.

# COMMAND ----------

# DBTITLE 1,Deploy app
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

MAX_WAIT = 300
POLL_INTERVAL = 15

print("Waiting for app compute to be ready...")
for attempt in range(MAX_WAIT // POLL_INTERVAL):
    app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
    print(f"  Attempt {attempt + 1}: Compute state = {compute_state}")

    if compute_state == "ACTIVE":
        break
    elif compute_state in ("ERROR", "FAILED"):
        raise RuntimeError(f"App compute failed: {app_info.get('compute_status', {})}")

    time.sleep(POLL_INTERVAL)
else:
    print(f"WARNING: Compute not ready after {MAX_WAIT}s. Attempting deploy anyway...")

response = w.api_client.do(
    "POST",
    f"/api/2.0/apps/{APP_NAME}/deployments",
    body={"source_code_path": WORKSPACE_APP_PATH}
)

print("\nDeployment initiated successfully.")
print(json.dumps(response, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the App Is Running
# MAGIC
# MAGIC Polls the app status until `app_status.state == RUNNING` and the active
# MAGIC deployment is `SUCCEEDED`. The app URL is printed once live.

# COMMAND ----------

# DBTITLE 1,Verify app running
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

MAX_WAIT = 300
POLL_INTERVAL = 15

for attempt in range(MAX_WAIT // POLL_INTERVAL):
    try:
        app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
        app_state = app_info.get("app_status", {}).get("state", "UNKNOWN")
        deploy_state = app_info.get("active_deployment", {}).get("status", {}).get("state", "PENDING")
        url = app_info.get("url", "pending...")

        print(f"Attempt {attempt + 1}: App = {app_state}, Deployment = {deploy_state}")

        if app_state == "RUNNING" and deploy_state == "SUCCEEDED":
            print(f"\nApp is live!")
            print(f"URL: {url}")
            break
        elif app_state in ("FAILED", "ERROR") or deploy_state in ("FAILED", "ERROR"):
            msg = app_info.get("app_status", {}).get("message", "Unknown error")
            print(f"\nApp deployment failed: {msg}")
            break
    except Exception as e:
        print(f"Attempt {attempt + 1}: {e}")

    time.sleep(POLL_INTERVAL)
else:
    print(f"App not ready after {MAX_WAIT}s. Check status manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: View App Details (Optional)
# MAGIC
# MAGIC Retrieve the full app configuration, deployment history, and service principal
# MAGIC details. Useful for debugging or extracting the `service_principal_client_id`
# MAGIC needed for post-deployment setup (Step 6).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Walkthrough
# MAGIC
# MAGIC Once the app is deployed and post-deployment setup is complete:
# MAGIC
# MAGIC ### Scenario 1: Supplier Books a Delivery Slot
# MAGIC 1. Open the app URL in your browser
# MAGIC 2. Navigate to the **Supplier Portal** tab
# MAGIC 3. Select a **Vendor ID** from the dropdown (e.g., `VENDOR_001`)
# MAGIC 4. Select a **Purchase Order** (filtered by the chosen vendor)
# MAGIC 5. Pick an available **date** from the calendar
# MAGIC 6. Choose a **dock** and **time slot** from the grid
# MAGIC 7. Fill in delivery details (truck plate, driver name)
# MAGIC 8. Submit the booking
# MAGIC
# MAGIC ### Scenario 2: Warehouse Clerk Manages Bookings
# MAGIC 1. Switch to the **Warehouse Clerk** tab
# MAGIC 2. View all bookings with their current status
# MAGIC 3. **Check in** an arriving delivery → status changes to `checked_in`
# MAGIC
# MAGIC ### Scenario 3: Dashboard Overview
# MAGIC 1. Navigate to the **Dashboard** tab
# MAGIC 2. View booking statistics and slot utilization

# COMMAND ----------

# DBTITLE 1,Step 6: Re-grant SP Permissions (if needed)
# MAGIC %md
# MAGIC ## Step 6: Post-Deployment — Re-grant SP Permissions (if needed)
# MAGIC
# MAGIC The cells below re-run the same SP permission grants from Step 2b.
# MAGIC They are **idempotent** and useful if you need to re-grant permissions
# MAGIC after recreating the app or changing the service principal.
# MAGIC
# MAGIC > When running this notebook top-to-bottom (or via `run_all`), Step 2b
# MAGIC > already handles permissions before deployment. These cells are only
# MAGIC > needed for manual troubleshooting.

# COMMAND ----------

# DBTITLE 1,Grant SP access to Lakebase project
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PROJECT = "delivery-slot-booking"

# Get the app's service principal details
app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
sp_name = app_info["service_principal_name"]
sp_client_id = app_info["service_principal_client_id"]
print(f"Service Principal: {sp_name}")
print(f"Client ID:         {sp_client_id}")

# Grant CAN_MANAGE on the Lakebase project (permissions API uses project name, not UID)
w.api_client.do(
    "PATCH",
    f"/api/2.0/permissions/database-projects/{PROJECT}",
    body={"access_control_list": [
        {"service_principal_name": sp_name, "permission_level": "CAN_MANAGE"}
    ]}
)
print(f"\n✓ Granted CAN_MANAGE on project '{PROJECT}' to {sp_name}")

# COMMAND ----------

# DBTITLE 1,Create Postgres OAuth role and grant table access
import psycopg2
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PROJECT = "delivery-slot-booking"
DB_NAME = "delivery_app"

# Get connection details
response = w.api_client.do(
    "GET",
    f"/api/2.0/postgres/projects/{PROJECT}/branches/production/endpoints"
)
ep = response["endpoints"][0]
host = ep["status"]["hosts"]["host"]

cred = w.api_client.do(
    "POST",
    "/api/2.0/postgres/credentials",
    body={"endpoint": ep["name"]}
)
token = cred["token"]
email = w.current_user.me().user_name

# Get SP client ID from previous cell
app_info = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
sp_client_id = app_info["service_principal_client_id"]

print(f"Host:         {host}")
print(f"SP Client ID: {sp_client_id}")

# Connect to Lakebase as project owner
conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=email, password=token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

# Create OAuth role for the service principal
cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth;")
print("\n\u2713 databricks_auth extension ready")

try:
    cur.execute(f"SELECT databricks_create_role('{sp_client_id}', 'service_principal');")
    print(f"\u2713 Created Postgres OAuth role for {sp_client_id}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"\u2713 Postgres OAuth role already exists for {sp_client_id}")
        conn.rollback()
        conn.autocommit = True
    else:
        raise

# Grant permissions on the database
grants = [
    f'GRANT CONNECT ON DATABASE {DB_NAME} TO "{sp_client_id}"',
    f'GRANT USAGE ON SCHEMA public TO "{sp_client_id}"',
    f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{sp_client_id}"',
    f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{sp_client_id}"',
]
for g in grants:
    cur.execute(g)
    print(f"\u2713 {g[:70]}...")

cur.close()
conn.close()
print(f"\n\u2713 All done \u2014 the app's service principal can now query Lakebase.")
print(f"  Refresh the app to verify the Vendor ID dropdown populates.")

# COMMAND ----------

# DBTITLE 1,Replication checklist and troubleshooting
# MAGIC %md
# MAGIC ## Replication Checklist
# MAGIC
# MAGIC Use this checklist when deploying to a **new workspace**:
# MAGIC
# MAGIC | # | Step | Where | Notes |
# MAGIC |---|------|-------|-------|
# MAGIC | 1 | Run `02_Lakebase_Setup` | Notebook | Creates project, database, tables, loads data |
# MAGIC | 2 | Run all cells in order | This notebook | Sets config, uploads files (auto-patches `app.yaml`), creates app, deploys, waits for live |
# MAGIC | 3 | Refresh browser | App URL | Verify Vendor ID dropdown populates and dates load |
# MAGIC
# MAGIC > **No SQL warehouse ID is needed.** The app connects directly to Lakebase via
# MAGIC > the Postgres protocol. The upload step automatically strips any `resources`
# MAGIC > section from `app.yaml`.
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Symptom | Likely Cause | Fix |
# MAGIC |---------|-------------|-----|
# MAGIC | App compute stuck in STARTING | First-time provisioning | Wait up to 3 minutes |
# MAGIC | `Failed to fetch available dates` | SP has no Lakebase project access | Re-run the SP permissions cell (grant `CAN_MANAGE`) |
# MAGIC | Vendor ID dropdown empty | Missing Postgres OAuth role for SP | Re-run the Postgres OAuth role cell (create role + GRANTs) |
# MAGIC | Dropdowns empty (no vendors/POs) | `ekko`/`ekpo_enriched` tables missing in Lakebase | Re-run `02_Lakebase_Setup` data loading cells |
# MAGIC | "Booking failed" on submit | SP missing USAGE on SERIAL sequences | Re-run the Postgres OAuth role cell (grants sequence permissions) |
# MAGIC | "Booking failed" after fresh data load | SERIAL sequence not reset after bulk load | `02_Lakebase_Setup` Cell 16 resets sequences; re-run it |
# MAGIC | Deployment stuck IN_PROGRESS | `node_modules/` included in sync | Delete `frontend/node_modules/`, re-sync, redeploy |
# MAGIC | `App process did not start within 10 min` | Crash on startup | Verify `databricks-sdk>=0.50.0` in `requirements.txt` |
# MAGIC | 401 when calling app URL externally | App uses Databricks OAuth proxy | Access via browser (auto-authenticated) |
# MAGIC | `Database instance not found` | Wrong API for Autoscaling | Use `/api/2.0/postgres/` endpoints, not `/api/2.0/database/` |
# MAGIC | `InvalidAuthorizationSpecificationError` | Missing Postgres OAuth role | Re-run the Postgres OAuth role cell (create role + GRANTs) |
# MAGIC
# MAGIC ## Key Files
# MAGIC
# MAGIC | File | Purpose |
# MAGIC |------|--------|
# MAGIC | `app/app.yaml` | App runtime config, env vars (resources section auto-stripped at deploy) |
# MAGIC | `app/server/config.py` | Lakebase connection logic (SDK-based, no hardcoded creds) |
# MAGIC | `app/server/db.py` | asyncpg connection pool manager |
# MAGIC | `app/server/routes/pos.py` | PO/vendor lookup endpoints (query `ekko`) |
# MAGIC | `app/requirements.txt` | Python deps (`databricks-sdk>=0.50.0`, `asyncpg`, etc.) |
# MAGIC | `app/frontend/dist/` | Pre-built React SPA (do **not** include `node_modules/`) |
# MAGIC
# MAGIC ## App Details
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | App Name | `delivery-slot-booking` |
# MAGIC | Backend | FastAPI + uvicorn (4 workers) |
# MAGIC | Frontend | React SPA (Vite build) |
# MAGIC | Database | Lakebase Autoscaling → `delivery_app` |
# MAGIC | Auth | Databricks SDK OAuth (auto-managed by service principal) |
# MAGIC | Source | `app/` directory (sibling of this notebook) |

# COMMAND ----------

# DBTITLE 1,Signal completion for dbutils.notebook.run()
# Signal successful completion when called via dbutils.notebook.run()
# This cell must be the LAST cell in the notebook.
try:
    dbutils.notebook.exit("success")
except NameError:
    print("Running interactively — no exit needed.")
