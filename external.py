from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

HERE = Path(__file__).resolve().parent

with open(HERE / "config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

COMMAND_ROOT = Path(CFG.get("COMMAND_ROOT", "command"))
if not COMMAND_ROOT.is_absolute():
    COMMAND_ROOT = HERE / COMMAND_ROOT

DB_PATH = COMMAND_ROOT / "commands.db"
WEB_PORT = CFG.get("WEB_PORT", 8080)

app = FastAPI(title="sgntext")

# ---------------------------------------------------------------- db helpers

def get_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    # Change this line to use DELETE mode
    conn.execute("PRAGMA synchronous=FULL") 
    conn.execute("PRAGMA journal_mode=DELETE")
    return conn

@app.on_event("startup")
def init_db():
    COMMAND_ROOT.mkdir(parents=True, exist_ok=True)
    (COMMAND_ROOT / "output").mkdir(exist_ok=True)
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            job_name TEXT,
            current_command TEXT,
            status TEXT,
            exit_code INTEGER,
            kill_requested INTEGER DEFAULT 0,
            created_at REAL,
            updated_at REAL
        )
    """)
    db.commit()
    db.close()

# ---------------------------------------------------------------- api models

class RunRequest(BaseModel):
    command: str

class RenameRequest(BaseModel):
    job_name: str

# ---------------------------------------------------------------- routes

@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(HERE / "index.html")

@app.get("/api/sessions")
def list_sessions():
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close() 

@app.post("/api/sessions")
def create_session():
    db = get_db()
    sid = uuid.uuid4().hex
    job_name = f"Worker{sid[:8]}"
    now = time.time()
    db.execute(
        "INSERT INTO sessions (session_id, job_name, status, created_at, updated_at) VALUES (?, ?, 'exited', ?, ?)",
        (sid, job_name, now, now)
    )
    db.commit()
    return {"session_id": sid}

@app.delete("/api/sessions/{sid}")
def delete_session(sid: str):
    db = get_db()
    db.execute("DELETE FROM sessions WHERE session_id=?", (sid,))
    db.commit()
    return {"ok": True}

@app.patch("/api/sessions/{sid}")
def rename_session(sid: str, body: RenameRequest):
    db = get_db()
    db.execute("UPDATE sessions SET job_name=? WHERE session_id=?", (body.job_name, sid))
    db.commit()
    return {"ok": True}

@app.post("/api/sessions/{sid}/run")
def run_command(sid: str, body: RunRequest):
    db = get_db()
    row = db.execute("SELECT status FROM sessions WHERE session_id=?", (sid,)).fetchone()
    if not row:
        raise HTTPException(404, "Session not found")
    if row["status"] not in ("exited", "error"):
        raise HTTPException(400, "Session is busy")
    
    db.execute(
        "UPDATE sessions SET current_command=?, status='requested', updated_at=? WHERE session_id=?",
        (body.command, time.time(), sid)
    )
    db.commit()
    return {"ok": True}

@app.post("/api/sessions/{sid}/kill")
def kill_command(sid: str):
    db = get_db()
    db.execute("UPDATE sessions SET kill_requested=1 WHERE session_id=?", (sid,))
    db.commit()
    return {"ok": True}

@app.get("/api/sessions/{sid}/output")
def get_output(sid: str):
    out_file = COMMAND_ROOT / "output" / f"{sid}.txt"
    if not out_file.exists():
        return {"output": ""}
    
    try:
        content = out_file.read_text(encoding="utf-8", errors="replace")
        return {"output": content}
    except Exception as e:
        return {"output": f"Error reading file: {e}"}

if __name__ == "__main__":
    print(f"Starting sgntext web UI on http://0.0.0.0:{WEB_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=WEB_PORT)