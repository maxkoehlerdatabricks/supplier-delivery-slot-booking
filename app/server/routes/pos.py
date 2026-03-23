from fastapi import APIRouter, HTTPException, Query
from server.db import db_pool

router = APIRouter(tags=["purchase-orders"])


@router.get("/pos/numbers")
async def list_po_numbers():
    """List all PO numbers with vendor info for dropdown selection."""
    rows = await db_pool.fetch(
        """
        SELECT "EBELN", "LIFNR", "BEDAT"::text, "BSART"
        FROM ekko
        ORDER BY "BEDAT" DESC, "EBELN"
    """
    )
    return [dict(r) for r in rows]


@router.get("/pos/{ebeln}")
async def get_po(ebeln: str):
    """Get PO header and items by PO number."""
    header = await db_pool.fetchrow(
        """
        SELECT "EBELN", "BUKRS", "EKORG", "BEDAT", "LIFNR", "BSART"
        FROM ekko WHERE "EBELN" = $1
    """,
        ebeln,
    )

    if not header:
        raise HTTPException(status_code=404, detail=f"PO {ebeln} not found")

    items = await db_pool.fetch(
        """
        SELECT "EBELN", "EBELP", "MATNR", "WERKS", "MENGE", "MEINS", "NETPR", "ELIKZ"
        FROM ekpo_enriched WHERE "EBELN" = $1
        ORDER BY "EBELP"
    """,
        ebeln,
    )

    total_value = sum(float(i["MENGE"] or 0) * float(i["NETPR"] or 0) for i in items)

    return {
        "ebeln": header["EBELN"],
        "lifnr": header["LIFNR"],
        "ekorg": header["EKORG"],
        "bsart": header["BSART"],
        "bedat": str(header["BEDAT"]),
        "status": "Active",
        "total_value": round(total_value, 2),
        "waers": "EUR",
        "items": [
            {
                "ebeln": i["EBELN"],
                "ebelp": i["EBELP"],
                "matnr": i["MATNR"],
                "maktx": i["MATNR"],
                "menge": float(i["MENGE"] or 0),
                "meins": i["MEINS"] or "EA",
                "netpr": float(i["NETPR"] or 0),
                "waers": "EUR",
                "werks": i["WERKS"],
            }
            for i in items
        ],
    }


@router.get("/pos")
async def search_pos(
    vendor_id: str | None = Query(default=None),
    material: str | None = Query(default=None),
    limit: int = Query(default=20, le=100),
):
    """Search POs by vendor or material."""
    query = """
        SELECT DISTINCT e."EBELN", e."LIFNR", e."BEDAT", e."BSART",
               COUNT(ei."EBELP") as item_count,
               SUM(ei."MENGE" * ei."NETPR") as total_value
        FROM ekko e
        LEFT JOIN ekpo_enriched ei ON e."EBELN" = ei."EBELN"
        WHERE 1=1
    """
    params = []
    idx = 1

    if vendor_id:
        query += f' AND e."LIFNR" = ${idx}'
        params.append(vendor_id)
        idx += 1
    if material:
        query += f' AND ei."MATNR" ILIKE ${idx}'
        params.append(f"%{material}%")
        idx += 1

    query += f' GROUP BY e."EBELN", e."LIFNR", e."BEDAT", e."BSART" ORDER BY e."BEDAT" DESC LIMIT ${idx}'
    params.append(limit)

    rows = await db_pool.fetch(query, *params)
    return [dict(r) for r in rows]
