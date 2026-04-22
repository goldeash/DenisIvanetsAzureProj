import json
import os
from fastapi import FastAPI, HTTPException
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from database import get_connection, init_db
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="OrderService", version="1.0.0")

@app.on_event("startup")
def startup():
    init_db()

def send_return_event(order_id: str, customer_id: str, reason: str):
    conn_str = os.getenv("SB_SEND")
    queue = os.getenv("SB_QUEUE")
    payload = json.dumps({
        "order_id": order_id,
        "customer_id": customer_id,
        "reason": reason
    })
    with ServiceBusClient.from_connection_string(conn_str) as client:
        with client.get_queue_sender(queue) as sender:
            sender.send_messages(ServiceBusMessage(payload))
    print(f"[order-service] Sent return event for order {order_id}")

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
        FROM DenisIvanets_orders.[order]
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
        FROM DenisIvanets_orders.[order]
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
# Cancel order
# ---------------------------------------------------------------------------

@app.post("/orders/{order_id}/cancel", tags=["orders"])
def cancel_order(order_id: str, reason: str = "No reason provided"):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, customer_id, status
        FROM DenisIvanets_orders.[order]
        WHERE id = ?
    """, order_id)
    row = cursor.fetchone()

    if not row:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Order not found")

    if row[2] in ("cancelled", "delivered"):
        cursor.close()
        conn.close()
        raise HTTPException(status_code=400, detail=f"Cannot cancel order with status '{row[2]}'")

    customer_id = str(row[1])

    cursor.execute("""
        UPDATE DenisIvanets_orders.[order]
        SET status = 'cancelled', updated_at = SYSDATETIME()
        WHERE id = ?
    """, order_id)

    cursor.execute("""
        INSERT INTO DenisIvanets_orders.order_status_history (order_id, status)
        VALUES (?, 'cancelled')
    """, order_id)

    conn.commit()
    cursor.close()
    conn.close()

    send_return_event(order_id, customer_id, reason)

    return {"message": "Order cancelled", "order_id": order_id}

# ---------------------------------------------------------------------------
# Order items
# ---------------------------------------------------------------------------

@app.get("/orders/{order_id}/items", tags=["orders"])
def get_order_items(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, product_id, quantity, unit_price
        FROM DenisIvanets_orders.order_item
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
        FROM DenisIvanets_orders.order_status_history
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

@app.post("/orders/test-create", tags=["orders"])
def create_test_order():
    import uuid
    conn = get_connection()
    cursor = conn.cursor()
    new_id = uuid.uuid4()
    customer_id = uuid.uuid4()
    cursor.execute(
        "INSERT INTO DenisIvanets_orders.[order] (id, customer_id, status) VALUES (?,?,?)",
        new_id, customer_id, "pending"
    )
    conn.commit()
    cursor.close()
    conn.close()
    return {"id": str(new_id), "status": "pending"}