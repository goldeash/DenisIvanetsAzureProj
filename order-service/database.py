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


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'DenisIvanets_orders')
        EXEC('CREATE SCHEMA DenisIvanets_orders')
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_orders' AND t.name = 'order'
        )
        CREATE TABLE DenisIvanets_orders.[order] (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            customer_id UNIQUEIDENTIFIER NOT NULL,
            status NVARCHAR(20) NOT NULL DEFAULT 'pending',
            created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_orders' AND t.name = 'order_item'
        )
        CREATE TABLE DenisIvanets_orders.order_item (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            order_id UNIQUEIDENTIFIER NOT NULL REFERENCES DenisIvanets_orders.[order](id),
            product_id UNIQUEIDENTIFIER NOT NULL,
            quantity INT NOT NULL,
            unit_price DECIMAL(10,2) NOT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_orders' AND t.name = 'order_status_history'
        )
        CREATE TABLE DenisIvanets_orders.order_status_history (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            order_id UNIQUEIDENTIFIER NOT NULL REFERENCES DenisIvanets_orders.[order](id),
            status NVARCHAR(20) NOT NULL,
            changed_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM DenisIvanets_orders.[order]")
    if cursor.fetchone()[0] == 0:

        o1 = uuid.uuid4()
        o2 = uuid.uuid4()
        c1 = uuid.uuid4()
        c2 = uuid.uuid4()
        p1 = uuid.uuid4()
        p2 = uuid.uuid4()

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.[order] (id, customer_id, status) VALUES (?,?,?)",
            o1, c1, "delivered"
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.[order] (id, customer_id, status) VALUES (?,?,?)",
            o2, c2, "pending"
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.order_item (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)",
            o1, p1, 2, 12.99
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.order_item (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)",
            o2, p2, 1, 24.50
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.order_status_history (order_id, status) VALUES (?,?)",
            o1, "delivered"
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_orders.order_status_history (order_id, status) VALUES (?,?)",
            o2, "pending"
        )

        conn.commit()

    cursor.close()
    conn.close()
    print("[order-service] DB initialized.")