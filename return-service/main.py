from fastapi import FastAPI, HTTPException
from database import get_connection, init_db

app = FastAPI(title="ReturnService", version="1.0.0")

@app.on_event("startup")
def startup():
    init_db()

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"])
def health():
    return {"status": "ok", "service": "return-service"}

# ---------------------------------------------------------------------------
# Returns
# ---------------------------------------------------------------------------

@app.get("/returns", tags=["returns"])
def get_returns():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, customer_id, reason, status, created_at, updated_at
        FROM returns.[return]
        ORDER BY created_at DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "order_id": str(r[1]),
            "customer_id": str(r[2]),
            "reason": r[3],
            "status": r[4],
            "created_at": r[5].isoformat(),
            "updated_at": r[6].isoformat(),
        }
        for r in rows
    ]

@app.get("/returns/{order_id}", tags=["returns"])
def get_return_by_order(order_id: str):
    """Read-only status check by order_id. Does not trigger any processing."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, customer_id, reason, status, created_at, updated_at
        FROM returns.[return]
        WHERE order_id = ?
    """, order_id)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Return not found for this order")
    return {
        "id": str(row[0]),
        "order_id": str(row[1]),
        "customer_id": str(row[2]),
        "reason": row[3],
        "status": row[4],
        "created_at": row[5].isoformat(),
        "updated_at": row[6].isoformat(),
    }

# ---------------------------------------------------------------------------
# Return items
# ---------------------------------------------------------------------------

@app.get("/returns/{order_id}/items", tags=["returns"])
def get_return_items(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ri.id, ri.return_id, ri.product_id, ri.quantity
        FROM returns.return_item ri
        JOIN returns.[return] r ON ri.return_id = r.id
        WHERE r.order_id = ?
    """, order_id)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "return_id": str(r[1]),
            "product_id": str(r[2]),
            "quantity": r[3],
        }
        for r in rows
    ]

# ---------------------------------------------------------------------------
# Status history
# ---------------------------------------------------------------------------

@app.get("/returns/{order_id}/history", tags=["returns"])
def get_return_history(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT rsh.id, rsh.return_id, rsh.status, rsh.changed_at
        FROM returns.return_status_history rsh
        JOIN returns.[return] r ON rsh.return_id = r.id
        WHERE r.order_id = ?
        ORDER BY rsh.changed_at ASC
    """, order_id)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "return_id": str(r[1]),
            "status": r[2],
            "changed_at": r[3].isoformat(),
        }
        for r in rows
    ]
