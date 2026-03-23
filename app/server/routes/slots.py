from fastapi import APIRouter, HTTPException, Query
from server.db import db_pool
from datetime import date

router = APIRouter(tags=["slots"])


@router.get("/slots")
async def get_slots(
    plant_id: str = Query(default="1100"),
    slot_date: date | None = Query(default=None),
    dock_id: str | None = Query(default=None),
):
    """Get available delivery slots. Optionally filter by date and dock."""
    query = """
        SELECT slot_id, dock_id, plant_id, slot_date,
               time_window_start::text, time_window_end::text,
               capacity, reserved_count,
               (capacity - reserved_count) as available
        FROM dock_slot
        WHERE plant_id = $1
    """
    params = [plant_id]
    idx = 2

    if slot_date:
        query += f" AND slot_date = ${idx}"
        params.append(slot_date)
        idx += 1

    if dock_id:
        query += f" AND dock_id = ${idx}"
        params.append(dock_id)
        idx += 1

    query += " ORDER BY slot_date, time_window_start, dock_id"

    rows = await db_pool.fetch(query, *params)
    return [dict(r) for r in rows]


@router.get("/slots/dates")
async def get_available_dates(plant_id: str = Query(default="1100")):
    """Get dates that have available slots."""
    rows = await db_pool.fetch(
        """
        SELECT DISTINCT slot_date
        FROM dock_slot
        WHERE plant_id = $1 AND (capacity - reserved_count) > 0
        ORDER BY slot_date
    """,
        plant_id,
    )
    return [r["slot_date"].isoformat() for r in rows]
