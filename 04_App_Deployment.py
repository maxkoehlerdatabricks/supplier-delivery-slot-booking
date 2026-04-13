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
# MAGIC 4. A **SQL warehouse** is available in the workspace — you will need its ID for
# MAGIC    the app resource binding (cell 9).
# MAGIC 5. The notebook is attached to **serverless compute** (or any cluster with the
# MAGIC    Databricks Python SDK available).
# MAGIC
# MAGIC > **Note:** The Databricks CLI is *not* required. All operations use the
# MAGIC > Databricks Python SDK which is pre-authenticated on Databricks compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC The cells below set workspace-relative paths and the SQL warehouse ID.
# MAGIC **No hardcoded profiles** — everything is derived from the current user context.
# MAGIC
# MAGIC | Variable | Purpose |
# MAGIC |---|---|
# MAGIC | `SQL_WAREHOUSE_ID` | SQL warehouse for the app resource binding — **set in the cell below** |
# MAGIC | `APP_NAME` | Databricks App name (must match the Lakebase project name) |
# MAGIC | `USER` | Current user email (auto-detected via SDK) |
# MAGIC | `WORKSPACE_APP_PATH` | Target path where app files are uploaded for deployment |
# MAGIC
# MAGIC ### How to find your SQL Warehouse ID
# MAGIC
# MAGIC You have three options:
# MAGIC
# MAGIC 1. **Run the cell below** — it lists all warehouses in your workspace so you can
# MAGIC    pick the right one.
# MAGIC 2. **From the UI** — go to **SQL Warehouses** in the sidebar, click the warehouse,
# MAGIC    and copy the ID from the URL: `.../sql/warehouses/<THIS_IS_THE_ID>`.
# MAGIC 3. **From the warehouse details page** — open the warehouse and look for the
# MAGIC    **ID** field in the **Connection details** tab.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List available SQL warehouses
print("Available SQL Warehouses:\n")
print(f"{'ID':<24} {'Name':<40} {'State'}")
print("-" * 80)
for wh in w.warehouses.list():
    print(f"{wh.id:<24} {wh.name:<40} {wh.state.value if wh.state else 'N/A'}")

print("\n" + "=" * 80)
print("Set SQL_WAREHOUSE_ID below to one of the IDs above.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Set SQL Warehouse ID
# ── SET YOUR SQL WAREHOUSE ID HERE ──────────────────────────────────────────────
# Replace the value below with a warehouse ID from the list above.
# This is the ONLY value you need to change when replicating to a new workspace.

SQL_WAREHOUSE_ID = "29e7967254530371"

print(f"Using SQL Warehouse ID: {SQL_WAREHOUSE_ID}")

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
print(f"SQL Warehouse ID: {SQL_WAREHOUSE_ID}")
print(f"User:             {USER}")
print(f"Workspace path:   {WORKSPACE_APP_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Configuration (`app.yaml`)
# MAGIC
# MAGIC The `app.yaml` in the `app/` directory defines runtime settings. When deploying
# MAGIC to a **new workspace**, you must update two things:
# MAGIC
# MAGIC 1. **SQL warehouse ID** in `resources` → replace with a valid warehouse from
# MAGIC    your workspace (find it in SQL Warehouses → warehouse details → ID).
# MAGIC 2. **`PGDATABASE`** → only change if you used a different database name in
# MAGIC    `02_Lakebase_Setup`.
# MAGIC
# MAGIC The current `app.yaml`:
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
# MAGIC resources:
# MAGIC   - name: sql-warehouse
# MAGIC     sql_warehouse:
# MAGIC       id: "<YOUR_SQL_WAREHOUSE_ID>"   # ← replace with your warehouse ID
# MAGIC       permission: CAN_USE
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

import shutil
import os

# Source: app/ directory next to this notebook in the project folder
source_dir = f"/Workspace/Users/{USER}/supplier-delivery-slot-booking/app"
dest_dir = WORKSPACE_APP_PATH

if not os.path.exists(source_dir):
    raise FileNotFoundError(f"App source not found at {source_dir}")

# Clean destination and copy fresh
if os.path.exists(dest_dir):
    shutil.rmtree(dest_dir)
shutil.copytree(source_dir, dest_dir)

file_count = sum(len(files) for _, _, files in os.walk(dest_dir))
print(f"App files synced successfully ({file_count} files).")
print(f"  From: {source_dir}")
print(f"  To:   {dest_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Databricks App
# MAGIC
# MAGIC Registers the application with the Databricks Apps service. The app is created
# MAGIC with the `SQL_WAREHOUSE_ID` variable set in the configuration cells above.
# MAGIC
# MAGIC > **Replicating?** Just update the warehouse ID in Cell 5 before running — no other code edits needed.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists

w = WorkspaceClient()

app_config = {
    "name": APP_NAME,
    "description": "Supplier Delivery Slot Booking",
    "resources": [
        {
            "name": "lakebase",
            "description": "Lakebase database",
            "sql_warehouse": {
                "id": SQL_WAREHOUSE_ID,
                "permission": "CAN_USE"
            }
        }
    ]
}

try:
    response = w.api_client.do("POST", "/api/2.0/apps", body=app_config)
    print(f"App '{APP_NAME}' created.")
    print(response)
except AlreadyExists:
    print(f"App '{APP_NAME}' already exists - will deploy new version.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deploy the App
# MAGIC
# MAGIC Triggers a deployment from the uploaded source code. The cell first waits for
# MAGIC the app's compute to reach `ACTIVE` state (takes ~2 min on first create), then
# MAGIC initiates the deployment.

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Wait for app compute to reach RUNNING state before deploying
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

# Deploy
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

# MAGIC %md
# MAGIC ## Step 6: Post-Deployment Setup (Required on New Workspaces)
# MAGIC
# MAGIC After the app is deployed and running, three additional steps are needed to
# MAGIC connect it to Lakebase. These are **one-time setup steps** per workspace.
# MAGIC
# MAGIC ### 6a. Grant the App's Service Principal Access to the Lakebase Project
# MAGIC
# MAGIC The app runs as a service principal that needs `CAN_MANAGE` on the Lakebase
# MAGIC project to resolve endpoints and generate credentials.
# MAGIC
# MAGIC ```python
# MAGIC import json
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Get project UID
# MAGIC project = w.api_client.do("GET", "/api/2.0/postgres/projects/delivery-slot-booking")
# MAGIC project_uid = project["uid"]
# MAGIC
# MAGIC # Get the app's service principal name
# MAGIC app_info = w.api_client.do("GET", "/api/2.0/apps/delivery-slot-booking")
# MAGIC sp_name = app_info["service_principal_name"]
# MAGIC sp_client_id = app_info["service_principal_client_id"]
# MAGIC print(f"SP: {sp_name} (client_id: {sp_client_id})")
# MAGIC
# MAGIC # Grant CAN_MANAGE
# MAGIC w.api_client.do("PATCH",
# MAGIC     f"/api/2.0/permissions/database-projects/{project_uid}",
# MAGIC     body={"access_control_list": [
# MAGIC         {"service_principal_name": sp_name, "permission_level": "CAN_MANAGE"}
# MAGIC     ]})
# MAGIC print("Permission granted.")
# MAGIC ```
# MAGIC
# MAGIC ### 6b. Create a Postgres OAuth Role for the Service Principal
# MAGIC
# MAGIC Connect to Lakebase as the project owner and create a role so the service
# MAGIC principal can authenticate via OAuth:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC SELECT databricks_create_role('<SP_CLIENT_ID>', 'service_principal');
# MAGIC
# MAGIC GRANT CONNECT ON DATABASE delivery_app TO "<SP_CLIENT_ID>";
# MAGIC GRANT USAGE ON SCHEMA public TO "<SP_CLIENT_ID>";
# MAGIC GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "<SP_CLIENT_ID>";
# MAGIC ```
# MAGIC
# MAGIC Replace `<SP_CLIENT_ID>` with the `service_principal_client_id` from step 6a.
# MAGIC
# MAGIC ### 6c. Verify
# MAGIC
# MAGIC Refresh the app in your browser. The Vendor ID dropdown should now populate
# MAGIC and available dates should load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replication Checklist
# MAGIC
# MAGIC Use this checklist when deploying to a **new workspace**:
# MAGIC
# MAGIC | # | Step | Where | Notes |
# MAGIC |---|------|-------|-------|
# MAGIC | 1 | Run `02_Lakebase_Setup` | Notebook | Creates project, database, tables, loads data |
# MAGIC | 2 | Update `SQL_WAREHOUSE_ID` | Cell 5 | Set to a valid warehouse from your workspace (run Cell 4 to list them) |
# MAGIC | 3 | Run cells 5 → 15 | This notebook | Sets config, uploads files, creates app, deploys, waits for live |
# MAGIC | 4 | Grant SP project access | Post-deploy (Step 6a) | `CAN_MANAGE` via Permissions API |
# MAGIC | 5 | Create Postgres OAuth role | Post-deploy (Step 6b) | `databricks_create_role` + GRANTs |
# MAGIC | 6 | Refresh browser | App URL | Verify dropdowns and dates load |
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Symptom | Likely Cause | Fix |
# MAGIC |---------|-------------|-----|
# MAGIC | App compute stuck in STARTING | First-time provisioning | Wait up to 3 minutes |
# MAGIC | `Failed to fetch available dates` | SP has no Lakebase project access | Run Step 6a (grant `CAN_MANAGE`) |
# MAGIC | Dropdowns empty (no vendors/POs) | `ekko`/`ekpo_enriched` tables missing in Lakebase | Re-run `02_Lakebase_Setup` data loading cells |
# MAGIC | Deployment stuck IN_PROGRESS | `node_modules/` included in sync | Delete `frontend/node_modules/`, re-sync, redeploy |
# MAGIC | `App process did not start within 10 min` | Crash on startup | Verify `databricks-sdk>=0.50.0` in `requirements.txt` |
# MAGIC | 401 when calling app URL externally | App uses Databricks OAuth proxy | Access via browser (auto-authenticated) |
# MAGIC | `Database instance not found` | Wrong API for Autoscaling | Use `/api/2.0/postgres/` endpoints, not `/api/2.0/database/` |
# MAGIC | `InvalidAuthorizationSpecificationError` | Missing Postgres OAuth role | Run Step 6b (create role + GRANTs) |
# MAGIC
# MAGIC ## Key Files
# MAGIC
# MAGIC | File | Purpose |
# MAGIC |------|--------|
# MAGIC | `app/app.yaml` | App runtime config, env vars, resource bindings |
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
