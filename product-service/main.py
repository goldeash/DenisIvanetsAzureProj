from fastapi import FastAPI, HTTPException
from database import get_connection, init_db

app = FastAPI(title="ProductService", version="1.0.0")

@app.on_event("startup")
def startup():
    init_db()

# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"])
def health():
    return {"status": "ok", "service": "product-service"}

# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------

@app.get("/categories", tags=["categories"])
def get_categories():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, description FROM products.category")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"id": str(r[0]), "name": r[1], "description": r[2]} for r in rows]

# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

@app.get("/products", tags=["products"])
def get_products(category_id: str = None):
    conn = get_connection()
    cursor = conn.cursor()
    if category_id:
        cursor.execute("""
            SELECT id, category_id, name, description, price, created_at
            FROM products.product
            WHERE category_id = ?
            ORDER BY name
        """, category_id)
    else:
        cursor.execute("""
            SELECT id, category_id, name, description, price, created_at
            FROM products.product
            ORDER BY name
        """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "id": str(r[0]),
            "category_id": str(r[1]),
            "name": r[2],
            "description": r[3],
            "price": float(r[4]),
            "created_at": r[5].isoformat(),
        }
        for r in rows
    ]

@app.get("/products/{product_id}", tags=["products"])
def get_product(product_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, category_id, name, description, price, created_at
        FROM products.product
        WHERE id = ?
    """, product_id)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Product not found")
    return {
        "id": str(row[0]),
        "category_id": str(row[1]),
        "name": row[2],
        "description": row[3],
        "price": float(row[4]),
        "created_at": row[5].isoformat(),
    }

# ---------------------------------------------------------------------------
# Stock
# ---------------------------------------------------------------------------

@app.get("/products/{product_id}/stock", tags=["stock"])
def get_stock(product_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, product_id, quantity, updated_at
        FROM products.stock
        WHERE product_id = ?
    """, product_id)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Stock record not found")
    return {
        "id": str(row[0]),
        "product_id": str(row[1]),
        "quantity": row[2],
        "updated_at": row[3].isoformat(),
    }
