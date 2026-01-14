from fastapi import FastAPI, Request, HTTPException, Query
from pydantic import BaseModel, Field, validator
from typing import Optional
from app.config import WEBHOOK_SECRET, DATABASE_URL
from app.storage import insert_message, get_connection
import hmac, hashlib
import sqlite3
import os

app = FastAPI()

# ---------- Models ----------
class WebhookMessage(BaseModel):
    message_id: str
    from_: str = Field(..., alias="from")
    to: str
    ts: str
    text: Optional[str] = None

    @validator("message_id")
    def id_not_empty(cls, v):
        if not v:
            raise ValueError("message_id must be non-empty")
        return v

    @validator("from_", "to")
    def validate_e164(cls, v):
        if not v.startswith("+") or not v[1:].isdigit():
            raise ValueError("must be E.164-like (+digits)")
        return v

    @validator("ts")
    def validate_ts(cls, v):
        if not v.endswith("Z"):
            raise ValueError("ts must end with Z")
        return v

    @validator("text")
    def validate_text(cls, v):
        if v is not None and len(v) > 4096:
            raise ValueError("text too long")
        return v

# ---------- Health ----------
@app.get("/health/live")
def live():
    return {"status": "alive"}

@app.get("/health/ready")
def ready():
    try:
        conn = get_connection()
        conn.execute("SELECT 1")
        conn.close()
        if not WEBHOOK_SECRET:
            raise RuntimeError()
        return {"status": "ready"}
    except:
        raise HTTPException(status_code=503, detail="not ready")

# ---------- Webhook ----------
@app.post("/webhook")
async def webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("X-Signature")
    if not signature:
        raise HTTPException(status_code=401, detail="invalid signature")

    computed = hmac.new(
        key=WEBHOOK_SECRET.encode(),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(signature, computed):
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = WebhookMessage.parse_raw(raw_body)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    inserted = insert_message(payload.dict(by_alias=True))
    result = "created" if inserted else "duplicate"

    return {"status": "ok", "result": result}

# ---------- Messages ----------
@app.get("/messages")
def get_messages(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    from_: Optional[str] = Query(None, alias="from"),
    since: Optional[str] = None,
    q: Optional[str] = None
):
    conn = get_connection()
    cursor = conn.cursor()

    conditions = []
    params = []

    if from_:
        conditions.append("from_msisdn = ?")
        params.append(from_)

    if since:
        conditions.append("ts >= ?")
        params.append(since)

    if q:
        conditions.append("text LIKE ?")
        params.append(f"%{q}%")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Total count
    total_sql = f"SELECT COUNT(*) FROM messages {where}"
    total = cursor.execute(total_sql, params).fetchone()[0]

    # Fetch messages with limit/offset
    data_sql = f"""
    SELECT message_id, from_msisdn AS "from", to_msisdn AS "to", ts, text
    FROM messages
    {where}
    ORDER BY ts ASC, message_id ASC
    LIMIT ? OFFSET ?
"""

    cursor.execute(data_sql, params + [limit, offset])
    rows = cursor.fetchall()
    conn.close()

    data = [dict(row) for row in rows]

    return {
        "data": data,
        "total": total,
        "limit": limit,
        "offset": offset
    }

# ---------- Stats ----------
@app.get("/stats")
def stats():
    conn = get_connection()
    cursor = conn.cursor()

    total_messages = cursor.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    senders_count = cursor.execute("SELECT COUNT(DISTINCT from_msisdn) FROM messages").fetchone()[0]

    messages_per_sender = cursor.execute("""
    SELECT from_msisdn AS "from", COUNT(*) AS count
    FROM messages
    GROUP BY from_msisdn
    ORDER BY count DESC
    LIMIT 10
""").fetchall()


    first_message_ts = cursor.execute("SELECT ts FROM messages ORDER BY ts ASC LIMIT 1").fetchone()
    last_message_ts = cursor.execute("SELECT ts FROM messages ORDER BY ts DESC LIMIT 1").fetchone()

    conn.close()

    return {
        "total_messages": total_messages,
        "senders_count": senders_count,
        "messages_per_sender": [dict(row) for row in messages_per_sender],
        "first_message_ts": first_message_ts["ts"] if first_message_ts else None,
        "last_message_ts": last_message_ts["ts"] if last_message_ts else None
    }
