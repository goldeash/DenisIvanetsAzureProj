import pyodbc
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={os.getenv('DB_SERVER')},1433;"
        f"Database={os.getenv('DB_DATABASE')};"
        f"Uid={os.getenv('DB_USERNAME')};"
        f"Pwd={os.getenv('DB_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


CAT1 = uuid.UUID("aaaaaaaa-0001-0001-0001-000000000001")
CAT2 = uuid.UUID("aaaaaaaa-0002-0002-0002-000000000002")

PROD1 = uuid.UUID("bbbbbbbb-0001-0001-0001-000000000001")
PROD2 = uuid.UUID("bbbbbbbb-0002-0002-0002-000000000002")
PROD3 = uuid.UUID("bbbbbbbb-0003-0003-0003-000000000003")


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'products')
        EXEC('CREATE SCHEMA products')
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'products' AND t.name = 'category'
        )
        CREATE TABLE products.category (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            name NVARCHAR(100) NOT NULL UNIQUE,
            description NVARCHAR(255)
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'products' AND t.name = 'product'
        )
        CREATE TABLE products.product (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            category_id UNIQUEIDENTIFIER NOT NULL,
            name NVARCHAR(150) NOT NULL,
            description NVARCHAR(500),
            price DECIMAL(10,2) NOT NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'products' AND t.name = 'stock'
        )
        CREATE TABLE products.stock (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            product_id UNIQUEIDENTIFIER NOT NULL UNIQUE,
            quantity INT NOT NULL DEFAULT 0,
            updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM products.category")
    if cursor.fetchone()[0] == 0:

        cursor.execute(
            "INSERT INTO products.category (id,name,description) VALUES (?,?,?)",
            CAT1, "Paints", "All paints"
        )

        cursor.execute(
            "INSERT INTO products.category (id,name,description) VALUES (?,?,?)",
            CAT2, "Brushes", "All brushes"
        )

        cursor.execute(
            "INSERT INTO products.product (id,category_id,name,description,price) VALUES (?,?,?,?,?)",
            PROD1, CAT1, "Acrylic Set", "12 colors", 12.99
        )

        cursor.execute(
            "INSERT INTO products.product (id,category_id,name,description,price) VALUES (?,?,?,?,?)",
            PROD2, CAT1, "Oil Paint White", "200ml", 24.50
        )

        cursor.execute(
            "INSERT INTO products.product (id,category_id,name,description,price) VALUES (?,?,?,?,?)",
            PROD3, CAT2, "Brush Set", "6 pcs", 18.00
        )

        cursor.execute("INSERT INTO products.stock (product_id,quantity) VALUES (?,?)", PROD1, 50)
        cursor.execute("INSERT INTO products.stock (product_id,quantity) VALUES (?,?)", PROD2, 30)
        cursor.execute("INSERT INTO products.stock (product_id,quantity) VALUES (?,?)", PROD3, 75)

        conn.commit()

    cursor.close()
    conn.close()
    print("[product-service] DB initialized.")