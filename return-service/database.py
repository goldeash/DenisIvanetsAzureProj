import pyodbc
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

# ✅ ВАЖНО: переменные должны быть здесь (глобально)
ORDER1 = uuid.UUID("a1b2c3d4-0001-0001-0001-000000000001")
ORDER2 = uuid.UUID("a1b2c3d4-0002-0002-0002-000000000002")

CUST1 = uuid.UUID("cccccccc-0001-0001-0001-000000000001")
CUST2 = uuid.UUID("cccccccc-0002-0002-0002-000000000002")

PROD1 = uuid.UUID("bbbbbbbb-0001-0001-0001-000000000001")


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
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'DenisIvanets_returns')
        EXEC('CREATE SCHEMA DenisIvanets_returns')
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_returns' AND t.name = 'return'
        )
        CREATE TABLE DenisIvanets_returns.[return] (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            order_id UNIQUEIDENTIFIER NOT NULL,
            customer_id UNIQUEIDENTIFIER NOT NULL,
            reason NVARCHAR(500),
            status NVARCHAR(20) NOT NULL DEFAULT 'requested',
            created_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_returns' AND t.name = 'return_item'
        )
        CREATE TABLE DenisIvanets_returns.return_item (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            return_id UNIQUEIDENTIFIER NOT NULL,
            product_id UNIQUEIDENTIFIER NOT NULL,
            quantity INT NOT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = 'DenisIvanets_returns' AND t.name = 'return_status_history'
        )
        CREATE TABLE DenisIvanets_returns.return_status_history (
            id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
            return_id UNIQUEIDENTIFIER NOT NULL,
            status NVARCHAR(20) NOT NULL,
            changed_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        )
    """)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM DenisIvanets_returns.[return]")
    if cursor.fetchone()[0] == 0:

        RET1 = uuid.UUID("12345678-0001-0001-0001-000000000001")

        cursor.execute(
            "INSERT INTO DenisIvanets_returns.[return] (id,order_id,customer_id,reason,status) VALUES (?,?,?,?,?)",
            RET1, ORDER1, CUST1, "Damaged product", "approved"
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_returns.return_item (return_id,product_id,quantity) VALUES (?,?,?)",
            RET1, PROD1, 1
        )

        cursor.execute(
            "INSERT INTO DenisIvanets_returns.return_status_history (return_id,status) VALUES (?,?)",
            RET1, "approved"
        )

        conn.commit()

    cursor.close()
    conn.close()
    print("[return-service] DB initialized.")