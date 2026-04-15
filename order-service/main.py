from fastapi import FastAPI, HTTPException
from database import get_connection, init_db

app = FastAPI(title="OrderService", version="1.0.0")

@app.on_event("startup")
def startup():
    init_db()

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"])
def health():
    return {"status": "ok", "service": "order-service"}

# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

@app.get("/orders", tags=["orders"])
def get_orders():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, customer_id, status, created_at, updated_at
        FROM orders.[order]
        ORDER BY created_at DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "customer_id": str(r[1]),
            "status": r[2],
            "created_at": r[3].isoformat(),
            "updated_at": r[4].isoformat(),
        }
        for r in rows
    ]

@app.get("/orders/{order_id}", tags=["orders"])
def get_order(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, customer_id, status, created_at, updated_at
        FROM orders.[order]
        WHERE id = ?
    """, order_id)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "id": str(row[0]),
        "customer_id": str(row[1]),
        "status": row[2],
        "created_at": row[3].isoformat(),
        "updated_at": row[4].isoformat(),
    }

# ---------------------------------------------------------------------------
# Order items
# ---------------------------------------------------------------------------

@app.get("/orders/{order_id}/items", tags=["orders"])
def get_order_items(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, product_id, quantity, unit_price
        FROM orders.order_item
        WHERE order_id = ?
    """, order_id)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "order_id": str(r[1]),
            "product_id": str(r[2]),
            "quantity": r[3],
            "unit_price": float(r[4]),
        }
        for r in rows
    ]

# ---------------------------------------------------------------------------
# Status history
# ---------------------------------------------------------------------------

@app.get("/orders/{order_id}/history", tags=["orders"])
def get_order_history(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, status, changed_at
        FROM orders.order_status_history
        WHERE order_id = ?
        ORDER BY changed_at ASC
    """, order_id)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "order_id": str(r[1]),
            "status": r[2],
            "changed_at": r[3].isoformat(),
        }
        for r in rows
    ]
