# Databricks notebook source
# MAGIC %md
# MAGIC # App Deployment
# MAGIC
# MAGIC This notebook deploys the Supplier Delivery Slot Booking application as a
# MAGIC Databricks App.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC +------------------+      +------------------+      +------------------+
# MAGIC |   React Frontend |----->|  FastAPI Backend  |----->|    Lakebase DB   |
# MAGIC |   (Static SPA)   |      |  (Python API)    |      |  (delivery_app)  |
# MAGIC +------------------+      +------------------+      +------------------+
# MAGIC                                    |
# MAGIC                                    v
# MAGIC                           +------------------+
# MAGIC                           |  Delta Lake      |
# MAGIC                           |  (SAP tables)    |
# MAGIC                           +------------------+
# MAGIC ```
# MAGIC
# MAGIC - **React Frontend**: Single-page app for suppliers and warehouse clerks
# MAGIC - **FastAPI Backend**: REST API handling booking CRUD, slot management, PO lookups
# MAGIC - **Lakebase**: OLTP database for transactional data (bookings, slots)
# MAGIC - **Delta Lake**: Synced SAP data for PO validation and enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Before running this notebook, ensure:
# MAGIC
# MAGIC 1. Lakebase project `delivery-slot-booking` is provisioned (run `02_Lakebase_Setup`)
# MAGIC 2. All tables are populated with data
# MAGIC 3. The `app/` directory contains the application source code
# MAGIC 4. Databricks CLI is configured with the correct profile

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import subprocess
import json
import os

PROFILE = "fe-vm-fevm-serverless-stable-nyu9oz"
APP_NAME = "delivery-slot-booking"

# Get current user for workspace path
result = subprocess.run([
    "databricks", "current-user", "me",
    "-p", PROFILE, "-o", "json"
], capture_output=True, text=True)

if result.returncode == 0:
    user_info = json.loads(result.stdout)
    USER = user_info["userName"]
    WORKSPACE_APP_PATH = f"/Workspace/Users/{USER}/apps/{APP_NAME}"
else:
    print(f"Warning: Could not get current user: {result.stderr}")
    USER = "unknown"
    WORKSPACE_APP_PATH = f"/Workspace/Users/{USER}/apps/{APP_NAME}"

print(f"Profile:        {PROFILE}")
print(f"App name:       {APP_NAME}")
print(f"User:           {USER}")
print(f"Workspace path: {WORKSPACE_APP_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Configuration
# MAGIC
# MAGIC The `app.yaml` defines the app's runtime configuration, environment variables,
# MAGIC and resource bindings.
# MAGIC
# MAGIC ```yaml
# MAGIC command:
# MAGIC   - uvicorn
# MAGIC   - app.main:app
# MAGIC   - --host=0.0.0.0
# MAGIC   - --port=8000
# MAGIC
# MAGIC env:
# MAGIC   - name: LAKEBASE_PROJECT
# MAGIC     value: delivery-slot-booking
# MAGIC   - name: LAKEBASE_DATABASE
# MAGIC     value: delivery_app
# MAGIC   - name: LAKEBASE_BRANCH
# MAGIC     value: production
# MAGIC   - name: DATABRICKS_PROFILE
# MAGIC     value: fe-vm-fevm-serverless-stable-nyu9oz
# MAGIC
# MAGIC resources:
# MAGIC   - name: lakebase
# MAGIC     description: Lakebase database for OLTP operations
# MAGIC     sql_warehouse:
# MAGIC       id: "2d0a20c121efb7e5"
# MAGIC       permission: CAN_USE
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload App to Workspace
# MAGIC
# MAGIC Sync the local `app/` directory to the Databricks workspace.

# COMMAND ----------

result = subprocess.run([
    "databricks", "sync", "./app", WORKSPACE_APP_PATH,
    "-p", PROFILE
], capture_output=True, text=True)

if result.returncode == 0:
    print("App files synced successfully.")
    print(result.stdout)
else:
    print(f"Sync output: {result.stdout}")
    print(f"Sync errors: {result.stderr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Databricks App
# MAGIC
# MAGIC Register the application with Databricks Apps service.

# COMMAND ----------

app_config = {
    "description": "Supplier Delivery Slot Booking",
    "resources": [
        {
            "name": "lakebase",
            "description": "Lakebase database",
            "sql_warehouse": {
                "id": "2d0a20c121efb7e5",
                "permission": "CAN_USE"
            }
        }
    ]
}

result = subprocess.run([
    "databricks", "apps", "create", APP_NAME,
    "--json", json.dumps(app_config),
    "-p", PROFILE
], capture_output=True, text=True)

if "already exists" in (result.stderr or ""):
    print(f"App '{APP_NAME}' already exists - will deploy new version.")
else:
    print(result.stdout or result.stderr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deploy the App
# MAGIC
# MAGIC Trigger a new deployment from the uploaded source code.

# COMMAND ----------

result = subprocess.run([
    "databricks", "apps", "deploy", APP_NAME,
    "--source-code-path", WORKSPACE_APP_PATH,
    "-p", PROFILE
], capture_output=True, text=True)

if result.returncode == 0:
    print("Deployment initiated successfully.")
    deploy_info = result.stdout
    print(deploy_info)
else:
    print(f"Deployment output: {result.stdout}")
    print(f"Deployment errors: {result.stderr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Get App URL and Status
# MAGIC
# MAGIC Retrieve the deployed app's URL and current status.

# COMMAND ----------

import time

MAX_WAIT = 300
POLL_INTERVAL = 15

for attempt in range(MAX_WAIT // POLL_INTERVAL):
    result = subprocess.run([
        "databricks", "apps", "get", APP_NAME,
        "-p", PROFILE, "-o", "json"
    ], capture_output=True, text=True)

    if result.returncode == 0:
        app_info = json.loads(result.stdout)
        status = app_info.get("status", {}).get("state", "UNKNOWN")
        url = app_info.get("url", "pending...")

        print(f"Attempt {attempt + 1}: Status = {status}")
        print(f"App URL: {url}")

        if status in ("RUNNING", "ACTIVE", "DEPLOYED"):
            print("\nApp is live!")
            break
        elif status in ("FAILED", "ERROR"):
            print("\nApp deployment failed. Check logs for details.")
            break
    else:
        print(f"Attempt {attempt + 1}: {result.stderr.strip()}")

    time.sleep(POLL_INTERVAL)
else:
    print(f"App not ready after {MAX_WAIT}s. Check status manually.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: View App Logs (Optional)
# MAGIC
# MAGIC If the deployment has issues, check the application logs.

# COMMAND ----------

result = subprocess.run([
    "databricks", "apps", "get", APP_NAME,
    "-p", PROFILE, "-o", "json"
], capture_output=True, text=True)

if result.returncode == 0:
    app_info = json.loads(result.stdout)
    print(json.dumps(app_info, indent=2))
else:
    print(result.stderr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Walkthrough
# MAGIC
# MAGIC Once the app is deployed and running, test each scenario:
# MAGIC
# MAGIC ### Scenario 1: Supplier Books a Delivery Slot
# MAGIC 1. Open the app URL in your browser
# MAGIC 2. Log in as a supplier (e.g., VENDOR_001)
# MAGIC 3. Navigate to "Book Slot"
# MAGIC 4. Select a date, dock, and time window
# MAGIC 5. Enter PO number, truck plate, and driver name
# MAGIC 6. Submit the booking
# MAGIC 7. Verify status is "requested"
# MAGIC
# MAGIC ### Scenario 2: Warehouse Clerk Confirms a Booking
# MAGIC 1. Switch to clerk view
# MAGIC 2. Navigate to "Pending Bookings"
# MAGIC 3. Review the booking details (PO, material, vendor)
# MAGIC 4. Click "Confirm"
# MAGIC 5. Verify status changes to "confirmed"
# MAGIC
# MAGIC ### Scenario 3: Truck Check-In
# MAGIC 1. In clerk view, navigate to "Today's Deliveries"
# MAGIC 2. Find the confirmed booking
# MAGIC 3. Click "Check In"
# MAGIC 4. Verify status changes to "checked_in"
# MAGIC
# MAGIC ### Scenario 4: Goods Receipt / Completion
# MAGIC 1. After unloading, find the checked-in booking
# MAGIC 2. Click "Complete"
# MAGIC 3. Verify status changes to "completed"
# MAGIC 4. Check that dock slot `reserved_count` is updated
# MAGIC
# MAGIC ### Scenario 5: View Analytics
# MAGIC 1. Navigate to the dashboard view
# MAGIC 2. Verify slot utilization charts display correctly
# MAGIC 3. Check booking status flow visualization
# MAGIC 4. Confirm PO data from synced Delta tables is accessible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Deployment Checklist
# MAGIC
# MAGIC | Step | Action | Status |
# MAGIC |------|--------|--------|
# MAGIC | 1 | Upload app to workspace | See output above |
# MAGIC | 2 | Create Databricks App | See output above |
# MAGIC | 3 | Deploy from source | See output above |
# MAGIC | 4 | Verify app is running | See output above |
# MAGIC
# MAGIC ### App Details
# MAGIC
# MAGIC | Property | Value |
# MAGIC |----------|-------|
# MAGIC | App Name | `delivery-slot-booking` |
# MAGIC | Source Path | See `WORKSPACE_APP_PATH` above |
# MAGIC | Backend | FastAPI (uvicorn) |
# MAGIC | Frontend | React SPA |
# MAGIC | Database | Lakebase `delivery_app` |
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC - **App not starting**: Check `app.yaml` configuration and environment variables
# MAGIC - **Database connection errors**: Verify Lakebase project is active and credentials are valid
# MAGIC - **Missing data**: Re-run `02_Lakebase_Setup` to reload data from Delta tables
# MAGIC - **Sync issues**: Check synced table status in the Databricks UI
