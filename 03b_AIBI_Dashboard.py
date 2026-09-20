# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# MAGIC %md
# MAGIC # AI/BI Dashboard — Purchasing & Delivery Overview
# MAGIC
# MAGIC Publishes a **Databricks AI/BI (Lakeview) dashboard** over the Lakehouse **gold**
# MAGIC tables produced by `01_SAP_Data_Pipeline`. This is the BI-on-the-Lakehouse half of
# MAGIC the demo — governed Delta gold tables, queried by a serverless SQL warehouse, with
# MAGIC AI-assisted authoring available in the dashboard editor.
# MAGIC
# MAGIC ## Datasets (all gold Delta tables)
# MAGIC | Dataset | Source table | Shows |
# MAGIC |---------|--------------|-------|
# MAGIC | PO value by vendor | `ekpo_enriched` | Total PO line value per vendor |
# MAGIC | PO value by material | `ekpo_enriched` | Spend per LiDAR component |
# MAGIC | Slot utilization | `slot_utilization` | Dock capacity vs. reservations by date |
# MAGIC | Booking funnel | `booking_funnel` | Bookings + PO value by status |
# MAGIC
# MAGIC > **Prereq**: run `01_SAP_Data_Pipeline` first so the gold tables exist.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

# ─────────────────────────────────────────────────────────────────────────────
# Resolve a serverless SQL warehouse to back the dashboard datasets
# ─────────────────────────────────────────────────────────────────────────────
warehouse_id = None
for wh in w.warehouses.list():
    if wh.enable_serverless_compute:
        warehouse_id = wh.id
        print(f"Using serverless warehouse: {wh.name} ({wh.id})")
        break
if warehouse_id is None:
    # fall back to the first available warehouse
    whs = list(w.warehouses.list())
    if not whs:
        raise RuntimeError("No SQL warehouse available. Create one first.")
    warehouse_id = whs[0].id
    print(f"No serverless warehouse; using: {whs[0].name} ({whs[0].id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the dashboard
# MAGIC
# MAGIC The dashboard is defined as a Lakeview serialized spec: datasets (SQL over gold
# MAGIC tables) plus a page of widgets bound to those datasets.

# COMMAND ----------

# DBTITLE 1,Build the Lakeview dashboard spec
datasets = [
    {
        "name": "po_value_by_vendor",
        "displayName": "PO value by vendor",
        "queryLines": [
            f"SELECT LIFNR AS vendor, ",
            f"       COUNT(DISTINCT EBELN) AS po_count, ",
            f"       ROUND(SUM(LINE_VALUE), 2) AS total_value ",
            f"FROM {FULL_SCHEMA}.ekpo_enriched ",
            f"GROUP BY LIFNR ORDER BY total_value DESC",
        ],
    },
    {
        "name": "po_value_by_material",
        "displayName": "Spend by material",
        "queryLines": [
            f"SELECT MATNR AS material, ",
            f"       SUM(MENGE) AS total_qty, ",
            f"       ROUND(SUM(LINE_VALUE), 2) AS total_value ",
            f"FROM {FULL_SCHEMA}.ekpo_enriched ",
            f"GROUP BY MATNR ORDER BY total_value DESC",
        ],
    },
    {
        "name": "slot_utilization",
        "displayName": "Avg dock utilization %",
        "queryLines": [
            f"SELECT dock_id, ROUND(AVG(utilization_pct), 1) AS avg_utilization_pct ",
            f"FROM {FULL_SCHEMA}.slot_utilization ",
            f"GROUP BY dock_id ",
            f"ORDER BY dock_id",
        ],
    },
    {
        "name": "booking_funnel",
        "displayName": "Booking funnel",
        "queryLines": [
            f"SELECT status, bookings, total_po_value ",
            f"FROM {FULL_SCHEMA}.booking_funnel ",
            f"ORDER BY CASE status "
            f"WHEN 'requested' THEN 1 WHEN 'confirmed' THEN 2 "
            f"WHEN 'checked_in' THEN 3 WHEN 'completed' THEN 4 ELSE 5 END",
        ],
    },
]

def bar_widget(name, dataset, x_field, y_field, title, x_scale="categorical"):
    # The widget's query MUST be named exactly "main_query" (the dataset link is
    # via `datasetName` inside the query, NOT the query name). Encodings then
    # reference that single query implicitly. A dataset-suffixed query name gives
    # "This widget reads from main_query, but its available queries are: ...".
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": dataset,
                    "fields": [
                        {"name": x_field, "expression": f"`{x_field}`"},
                        {"name": y_field, "expression": f"`{y_field}`"},
                    ],
                    "disaggregated": False,
                },
            }],
            "spec": {
                "version": 3,
                "widgetType": "bar",
                "encodings": {
                    "x": {"fieldName": x_field, "scale": {"type": x_scale}, "displayName": x_field},
                    "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_field},
                },
                "frame": {"title": title, "showTitle": True},
            },
        }
    }

layout = [
    {"widget": bar_widget("w_vendor", "po_value_by_vendor", "vendor", "total_value",
                          "PO value by vendor (EUR)")["widget"],
     "position": {"x": 0, "y": 0, "width": 3, "height": 6}},
    {"widget": bar_widget("w_material", "po_value_by_material", "material", "total_value",
                          "Spend by material (EUR)")["widget"],
     "position": {"x": 3, "y": 0, "width": 3, "height": 6}},
    {"widget": bar_widget("w_funnel", "booking_funnel", "status", "bookings",
                          "Booking funnel")["widget"],
     "position": {"x": 0, "y": 6, "width": 3, "height": 6}},
    {"widget": bar_widget("w_slots", "slot_utilization", "dock_id", "avg_utilization_pct",
                          "Avg dock utilization % by dock")["widget"],
     "position": {"x": 3, "y": 6, "width": 3, "height": 6}},
]

dashboard_spec = {
    "datasets": datasets,
    "pages": [{
        "name": "overview",
        "displayName": "Overview",
        "layout": layout,
    }],
}

print("Dashboard spec built with", len(datasets), "datasets and", len(layout), "widgets.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the dashboard
# MAGIC
# MAGIC Idempotent: if a dashboard with this name exists in the user's home folder it is
# MAGIC updated in place, otherwise a new one is created. The dashboard is then published.

# COMMAND ----------

# DBTITLE 1,Publish the Lakeview dashboard
# Uses the version-stable Lakeview REST API directly (the serverless-runtime SDK may
# be too old for the `w.lakeview` typed models).
USER = w.current_user.me().user_name
parent_path = f"/Workspace/Users/{USER}"
serialized = json.dumps(dashboard_spec)

# Find existing dashboard by name (list API is paginated)
existing_id = None
resp = w.api_client.do("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100})
for d in resp.get("dashboards", []):
    if d.get("display_name") == DASHBOARD_NAME and not d.get("trashed_at"):
        existing_id = d.get("dashboard_id")
        break

if existing_id:
    dash = w.api_client.do(
        "PATCH",
        f"/api/2.0/lakeview/dashboards/{existing_id}",
        body={"display_name": DASHBOARD_NAME, "serialized_dashboard": serialized, "warehouse_id": warehouse_id},
    )
    dashboard_id = existing_id
    print(f"Updated existing dashboard: {dashboard_id}")
else:
    dash = w.api_client.do(
        "POST",
        "/api/2.0/lakeview/dashboards",
        body={"display_name": DASHBOARD_NAME, "serialized_dashboard": serialized,
              "warehouse_id": warehouse_id, "parent_path": parent_path},
    )
    dashboard_id = dash.get("dashboard_id")
    print(f"Created dashboard: {dashboard_id}")

# Publish so it can be viewed
try:
    w.api_client.do(
        "POST",
        f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
        body={"warehouse_id": warehouse_id},
    )
    print("✓ Dashboard published.")
except Exception as e:
    print(f"⚠ Publish step: {e}")

print(f"\nOpen it at: {w.config.host}/dashboardsv3/{dashboard_id}/published")

# COMMAND ----------

try:
    dbutils.notebook.exit("success")
except NameError:
    print("Running interactively — no exit needed.")
