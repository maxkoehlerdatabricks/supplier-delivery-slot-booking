from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from server.db import db_pool
from datetime import datetime

router = APIRouter(tags=["bookings"])


class BookingCreate(BaseModel):
    slot_id: int
    vendor_id: str
    po_number: str
    truck_plate: str | None = None
    driver_name: str | None = None


class StatusUpdate(BaseModel):
    status: str


@router.post("/bookings")
async def create_booking(booking: BookingCreate):
    """Create a new delivery booking."""
    # Check slot availability
    slot = await db_pool.fetchrow(
        "SELECT capacity, reserved_count FROM dock_slot WHERE slot_id = $1",
        booking.slot_id,
    )
    if not slot:
        raise HTTPException(status_code=404, detail="Slot not found")
    if slot["reserved_count"] >= slot["capacity"]:
        raise HTTPException(status_code=409, detail="Slot is fully booked")

    # Create booking
    row = await db_pool.fetchrow(
        """
        INSERT INTO delivery_booking (slot_id, vendor_id, po_number, truck_plate, driver_name, status)
        VALUES ($1, $2, $3, $4, $5, 'requested')
        RETURNING booking_id, slot_id, vendor_id, po_number, truck_plate, driver_name, status, created_at, updated_at
    """,
        booking.slot_id,
        booking.vendor_id,
        booking.po_number,
        booking.truck_plate,
        booking.driver_name,
    )

    # Update slot reserved count
    await db_pool.execute(
        "UPDATE dock_slot SET reserved_count = reserved_count + 1 WHERE slot_id = $1",
        booking.slot_id,
    )

    return dict(row)


@router.get("/bookings")
async def get_bookings(
    po_number: str | None = Query(default=None),
    vendor_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    slot_date: str | None = Query(default=None),
):
    """Search bookings by PO number, vendor, status, or date."""
    query = """
        SELECT b.booking_id as id, b.booking_id, b.slot_id, b.vendor_id, b.po_number,
               b.truck_plate, b.driver_name, b.status,
               b.created_at, b.updated_at,
               s.dock_id as dock_name, s.slot_date,
               s.time_window_start::text || ' - ' || s.time_window_end::text as time_window
        FROM delivery_booking b
        JOIN dock_slot s ON b.slot_id = s.slot_id
        WHERE 1=1
    """
    params = []
    idx = 1

    if po_number:
        query += f" AND b.po_number = ${idx}"
        params.append(po_number)
        idx += 1
    if vendor_id:
        query += f" AND b.vendor_id = ${idx}"
        params.append(vendor_id)
        idx += 1
    if status:
        query += f" AND b.status = ${idx}"
        params.append(status)
        idx += 1
    if slot_date:
        query += f" AND s.slot_date = ${idx}::date"
        params.append(slot_date)
        idx += 1

    query += " ORDER BY b.created_at DESC"

    rows = await db_pool.fetch(query, *params)
    return [dict(r) for r in rows]


@router.get("/bookings/{booking_id}")
async def get_booking(booking_id: int):
    """Get a single booking by ID."""
    row = await db_pool.fetchrow(
        """
        SELECT b.booking_id as id, b.booking_id, b.slot_id, b.vendor_id, b.po_number,
               b.truck_plate, b.driver_name, b.status, b.created_at, b.updated_at,
               s.dock_id as dock_name, s.slot_date,
               s.time_window_start::text || ' - ' || s.time_window_end::text as time_window
        FROM delivery_booking b
        JOIN dock_slot s ON b.slot_id = s.slot_id
        WHERE b.booking_id = $1
    """,
        booking_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Booking not found")
    return dict(row)


@router.put("/bookings/{booking_id}/status")
async def update_booking_status(booking_id: int, update: StatusUpdate):
    """Update booking status. Valid transitions: requested->confirmed->checked_in->completed, any->cancelled."""
    VALID_TRANSITIONS = {
        "requested": ["confirmed", "cancelled"],
        "confirmed": ["checked_in", "cancelled"],
        "checked_in": ["completed", "cancelled"],
    }

    current = await db_pool.fetchrow(
        "SELECT status, slot_id FROM delivery_booking WHERE booking_id = $1",
        booking_id,
    )
    if not current:
        raise HTTPException(status_code=404, detail="Booking not found")

    allowed = VALID_TRANSITIONS.get(current["status"], [])
    if update.status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot transition from '{current['status']}' to '{update.status}'. Allowed: {allowed}",
        )

    row = await db_pool.fetchrow(
        """
        UPDATE delivery_booking
        SET status = $1, updated_at = NOW()
        WHERE booking_id = $2
        RETURNING booking_id, slot_id, vendor_id, po_number, truck_plate, driver_name, status, created_at, updated_at
    """,
        update.status,
        booking_id,
    )

    # If cancelled, decrement slot reserved count
    if update.status == "cancelled":
        await db_pool.execute(
            "UPDATE dock_slot SET reserved_count = GREATEST(reserved_count - 1, 0) WHERE slot_id = $1",
            current["slot_id"],
        )

    return dict(row)


@router.get("/bookings/stats/summary")
async def get_booking_stats():
    """Get booking statistics for the dashboard."""
    status_counts = await db_pool.fetch(
        "SELECT status, COUNT(*) as count FROM delivery_booking GROUP BY status"
    )
    counts = {r["status"]: r["count"] for r in status_counts}
    total = sum(counts.values())

    today_utilization = await db_pool.fetch(
        """
        SELECT dock_id, SUM(reserved_count) as used, SUM(capacity) as total
        FROM dock_slot
        WHERE slot_date = CURRENT_DATE
        GROUP BY dock_id
        ORDER BY dock_id
    """
    )

    recent = await db_pool.fetch(
        """
        SELECT b.booking_id as id, b.booking_id, b.po_number, b.vendor_id, b.status,
               b.created_at,
               s.dock_id as dock_name, s.slot_date,
               s.time_window_start::text || ' - ' || s.time_window_end::text as time_window
        FROM delivery_booking b
        JOIN dock_slot s ON b.slot_id = s.slot_id
        ORDER BY b.updated_at DESC
        LIMIT 10
    """
    )

    return {
        "total_bookings": total,
        "requested": counts.get("requested", 0),
        "confirmed": counts.get("confirmed", 0),
        "checked_in": counts.get("checked_in", 0),
        "completed": counts.get("completed", 0),
        "cancelled": counts.get("cancelled", 0),
        "today_slots": [
            {"dock_id": r["dock_id"], "dock_name": r["dock_id"], "used": r["used"], "total": r["total"]}
            for r in today_utilization
        ],
        "recent_bookings": [dict(r) for r in recent],
    }
