# Supplier Delivery Slot Booking

A demo application for a **LiDAR sensor manufacturing plant** that provides:
- **Supplier Portal** — Book delivery time slots at the plant's loading docks
- **Warehouse Clerk View** — Look up PO details and confirm goods receipt
- **Dashboard** — Real-time overview of bookings and slot utilization

Built on **Databricks** with SAP MM data replicated to Delta Lake, OLTP tables in **Lakebase** (managed PostgreSQL), and a **React + FastAPI** web application deployed as a Databricks App.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Databricks Workspace                        │
│                                                                     │
│  ┌──────────────────┐     ┌──────────────────┐                     │
│  │  Delta Lake       │     │  Lakebase (PG)   │                     │
│  │                   │     │                   │                     │
│  │  ekko (PO hdr)   │────▶│  ekko (synced)   │                     │
│  │  ekpo (PO items) │     │  ekpo_enriched   │◀──┐                 │
│  │  ekpo_enriched   │────▶│  (synced)        │   │                 │
│  │  dock_slot       │     │                   │   │                 │
│  │  delivery_booking│     │  dock_slot (OLTP) │   │  ┌────────────┐│
│  └──────────────────┘     │  delivery_booking │◀──┼──│ Databricks ││
│                            │  (OLTP)          │   │  │ App        ││
│                            └──────────────────┘   │  │            ││
│                                                    │  │ React +   ││
│  ┌──────────────────┐                             │  │ FastAPI   ││
│  │ Notebooks         │                             │  │            ││
│  │ 01_SAP_Pipeline  │ ← Run in order              │  │ /supplier ││
│  │ 02_Lakebase_Setup│                             └──│ /clerk    ││
│  │ 03_Exploration   │                                │ /dashboard││
│  │ 04_Deployment    │                                └────────────┘│
│  └──────────────────┘                                              │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Model

### SAP MM Tables (Delta Lake → Synced to Lakebase)

| Table | Description | Key Fields |
|-------|-------------|------------|
| `ekko` | PO Headers | EBELN (PO#), LIFNR (vendor), BEDAT (date), BUKRS, EKORG |
| `ekpo` | PO Items | EBELN, EBELP (item#), MATNR (material), MENGE (qty), NETPR (price) |
| `ekpo_enriched` | Joined PO data | All EKPO fields + EKKO header fields |

### OLTP Tables (Lakebase)

| Table | Description | Key Fields |
|-------|-------------|------------|
| `dock_slot` | Loading dock time slots | slot_id, dock_id, slot_date, time_window, capacity, reserved_count |
| `delivery_booking` | Supplier delivery bookings | booking_id, slot_id, vendor_id, po_number, status |

### Materials (LiDAR Components)

| Material ID | Description |
|-------------|-------------|
| LIDAR-SENSOR-01 | LiDAR Sensor Module |
| LIDAR-MOUNT-KIT | Mounting Kit |
| LIDAR-OPTICS-MODULE | Optics Module |
| LIDAR-PCB-BOARD | PCB Board |
| LIDAR-HOUSING | Sensor Housing |

---

## Prerequisites

1. **Databricks Workspace** with:
   - Unity Catalog enabled
   - Serverless SQL Warehouse
   - Lakebase enabled
   - Databricks Apps enabled

2. **Databricks CLI** v0.285.0+ installed and authenticated:
   ```bash
   databricks --version  # Must be 0.285.0+
   databricks auth login --host https://your-workspace.cloud.databricks.com --profile your-profile
   ```

3. **psql client** (for Lakebase setup):
   ```bash
   brew install postgresql@16  # macOS
   ```

4. **Node.js 18+** (only needed if rebuilding the frontend):
   ```bash
   node --version  # v18+
   ```

---

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone https://github.com/maxkoehlerdatabricks/supplier-delivery-slot-booking.git
cd supplier-delivery-slot-booking
```

### Step 2: Configure Your Environment

Update the following constants in each notebook to match your workspace:

```python
CATALOG = "your_catalog"
SCHEMA = "your_schema"
PROFILE = "your-databricks-profile"
```

### Step 3: Run Notebooks in Order

Import the notebooks into your Databricks workspace and run them sequentially:

1. **`_helper/01_generate_sap_data.py`** — Generates simulated SAP EKKO/EKPO data
   - Creates ~50 PO headers and ~150 PO items
   - Writes Delta tables: `ekko`, `ekpo`

2. **`_helper/02_generate_oltp_data.py`** — Generates simulated OLTP data
   - Creates ~60 dock slots and ~40 delivery bookings
   - Writes Delta tables: `dock_slot`, `delivery_booking`

3. **`01_SAP_Data_Pipeline.py`** — Processes SAP data
   - Joins EKKO + EKPO with data quality checks
   - Creates enriched Delta table: `ekpo_enriched`

4. **`02_Lakebase_Setup.py`** — Sets up Lakebase
   - Creates Lakebase project with branching
   - Creates OLTP tables (dock_slot, delivery_booking)
   - Loads data from Delta tables
   - Sets up synced tables for PO data

5. **`03_Data_Exploration.py`** — Explore the data
   - Visualizations of POs, slots, bookings
   - Cross-table joins and analytics

6. **`04_App_Deployment.py`** — Deploys the web application
   - Creates and deploys the Databricks App
   - Provides the app URL

### Step 4: Open the App

After deployment, open the app URL provided in the last notebook. You'll see three views:

- **Supplier Portal** (`/`) — Book delivery slots
- **Warehouse Clerk** (`/clerk`) — Look up POs and manage bookings
- **Dashboard** (`/dashboard`) — Overview of all activity

---

## Demo Scenarios

### Scenario 1: Supplier Books a Delivery Slot

1. Open the app → **Supplier Portal**
2. Select an available date from the calendar
3. View the slot grid — see available time windows per dock
4. Click an available slot (e.g., DOCK-A, 08:00-12:00)
5. Fill in the booking form:
   - PO Number: `4500000001`
   - Vendor ID: `VENDOR_001`
   - Truck Plate: `M-AB-1234`
   - Driver: `Hans Mueller`
6. Submit → Booking created with status **"requested"**

### Scenario 2: Warehouse Clerk Pre-Checks Delivery

1. Open the app → **Warehouse Clerk**
2. Search for PO `4500000001`
3. View PO details:
   - Header: Vendor VENDOR_001, PO type NB
   - Items: LIDAR-SENSOR-01 (10 EA), LIDAR-MOUNT-KIT (20 EA)
4. See linked bookings for this PO
5. Click **"Confirm"** → Status changes to **"confirmed"**
6. When truck arrives, click **"Check In"** → Status: **"checked_in"**
7. After goods received, click **"Complete"** → Status: **"completed"**

### Scenario 3: Dashboard Overview

1. Open the app → **Dashboard**
2. View summary stats: total bookings by status
3. See today's slot utilization per dock
4. Monitor recent activity feed

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Data Lake | Databricks Delta Lake |
| OLTP Database | Databricks Lakebase (PostgreSQL) |
| Data Pipeline | PySpark (Databricks Notebooks) |
| Backend API | FastAPI (Python) |
| Frontend | React + TypeScript + TailwindCSS |
| Deployment | Databricks Apps |
| Auth | Databricks OAuth (dual-mode) |

---

## Project Structure

```
supplier-delivery-slot-booking/
├── README.md
├── _helper/
│   ├── 01_generate_sap_data.py        # Generate SAP EKKO & EKPO data
│   └── 02_generate_oltp_data.py        # Generate dock slots & bookings
├── 01_SAP_Data_Pipeline.py             # Join & enrich SAP data
├── 02_Lakebase_Setup.py                # Create Lakebase project & tables
├── 03_Data_Exploration.py              # Data visualizations
├── 04_App_Deployment.py                # Deploy Databricks App
└── app/                                # Web application
    ├── app.yaml                        # Databricks App config
    ├── app.py                          # FastAPI entry point
    ├── requirements.txt
    ├── server/
    │   ├── config.py                   # Auth configuration
    │   ├── db.py                       # Lakebase connection pool
    │   └── routes/
    │       ├── slots.py                # Slot endpoints
    │       ├── bookings.py             # Booking endpoints
    │       └── pos.py                  # PO lookup endpoints
    └── frontend/
        ├── package.json
        ├── vite.config.ts
        ├── tailwind.config.js
        ├── src/
        │   ├── App.tsx                 # Router + navigation
        │   ├── pages/
        │   │   ├── SupplierPortal.tsx  # Slot booking flow
        │   │   ├── WarehouseClerk.tsx  # PO lookup + receipt
        │   │   └── Dashboard.tsx       # Status overview
        │   └── components/
        │       ├── SlotCalendar.tsx
        │       ├── BookingForm.tsx
        │       ├── PODetail.tsx
        │       └── StatusBadge.tsx
        └── dist/                       # Built frontend (committed)
```

---

## Local Development

To run the app locally:

```bash
# Backend
cd app
pip install -r requirements.txt
uvicorn app:app --reload --port 8000

# Frontend (in separate terminal)
cd app/frontend
npm install
npm run dev
```

The frontend dev server proxies API calls to `localhost:8000`.

---

## Booking Status Flow

```
requested → confirmed → checked_in → completed
    ↓           ↓            ↓
 cancelled   cancelled    cancelled
```

| Status | Description |
|--------|-------------|
| `requested` | Supplier has booked a slot, pending confirmation |
| `confirmed` | Warehouse team has confirmed the booking |
| `checked_in` | Truck has arrived and been checked in |
| `completed` | Goods received and booking completed |
| `cancelled` | Booking was cancelled |
