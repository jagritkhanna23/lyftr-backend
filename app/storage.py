import sqlite3
from sqlite3 import Connection
from datetime import datetime
import os
from app.config import DATABASE_URL

# Ensure absolute path
if DATABASE_URL.startswith("sqlite:///"):
    db_path = DATABASE_URL.replace("sqlite:///","")
    # Convert to absolute path based on project root
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), db_path)
else:
    raise RuntimeError("Only sqlite is supported")

def get_connection() -> Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        from_msisdn TEXT NOT NULL,
        to_msisdn TEXT NOT NULL,
        ts TEXT NOT NULL,
        text TEXT,
        created_at TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def insert_message(message: dict) -> bool:
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO messages (message_id, from_msisdn, to_msisdn, ts, text, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                message["message_id"],
                message["from"],
                message["to"],
                message["ts"],
                message.get("text"),
                datetime.utcnow().isoformat() + "Z"
            )
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

# Initialize DB
init_db()
