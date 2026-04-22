import json
import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from azure.servicebus import ServiceBusClient
from database import get_connection, init_db
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="ReturnService", version="1.0.0")

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(poll_service_bus())

async def poll_service_bus():
    conn_str = os.getenv("SB_LISTEN")
    queue = os.getenv("SB_QUEUE")
    print("[return-service] Starting Service Bus poller (every 10s)...")
    while True:
        try:
            with ServiceBusClient.from_connection_string(conn_str) as client:
                with client.get_queue_receiver(queue, max_wait_time=5) as receiver:
                    messages = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                    for msg in messages:
                        body = json.loads(str(msg))
                        print(f"[return-service] Received message: {body}")
                        process_return(body)
                        receiver.complete_message(msg)
        except Exception as e:
            print(f"[return-service] Poller error: {e}")
        await asyncio.sleep(10)

def process_return(data: dict):
    order_id = data.get("order_id")
    customer_id = data.get("customer_id")
    reason = data.get("reason", "Cancelled order")

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM DenisIvanets_returns.[return]
        WHERE order_id = ?
    """, order_id)
    if cursor.fetchone()[0] > 0:
        print(f"[return-service] Return for order {order_id} already exists, skipping.")
        cursor.close()
        conn.close()
        return

    return_id = uuid.uuid4()

    cursor.execute("""
        INSERT INTO DenisIvanets_returns.[return] (id, order_id, customer_id, reason, status)
        VALUES (?, ?, ?, ?, 'requested')
    """, return_id, order_id, customer_id, reason)

    cursor.execute("""
        INSERT INTO DenisIvanets_returns.return_status_history (return_id, status)
        VALUES (?, 'requested')
    """, return_id)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[return-service] Created return {return_id} for order {order_id}")

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
        FROM DenisIvanets_returns.[return]
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
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, order_id, customer_id, reason, status, created_at, updated_at
        FROM DenisIvanets_returns.[return]
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

@app.get("/returns/{order_id}/items", tags=["returns"])
def get_return_items(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ri.id, ri.return_id, ri.product_id, ri.quantity
        FROM DenisIvanets_returns.return_item ri
        JOIN DenisIvanets_returns.[return] r ON ri.return_id = r.id
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

@app.get("/returns/{order_id}/history", tags=["returns"])
def get_return_history(order_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT rsh.id, rsh.return_id, rsh.status, rsh.changed_at
        FROM DenisIvanets_returns.return_status_history rsh
        JOIN DenisIvanets_returns.[return] r ON rsh.return_id = r.id
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