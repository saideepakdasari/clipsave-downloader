from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse
from pydantic import BaseModel
import yt_dlp
import httpx
import imageio_ffmpeg
import sqlite3
from datetime import datetime
import os
import uuid
import tempfile

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

DB_PATH = "clipsave.db"

# --- Helper: Parse Device (Mobile vs Laptop) & Browser ---
def parse_device_and_browser(ua: str) -> str:
    ua_lower = ua.lower()
    if any(k in ua_lower for k in ["iphone", "android", "mobile", "ipad"]):
        device = "Mobile / Tablet"
    elif any(k in ua_lower for k in ["macintosh", "windows", "linux", "cros"]):
        device = "Laptop / Desktop"
    else:
        device = "Other Device"

    if "edg/" in ua_lower:
        browser = "Edge"
    elif "opr/" in ua_lower or "opera" in ua_lower:
        browser = "Opera"
    elif "chrome/" in ua_lower and "safari/" in ua_lower:
        browser = "Chrome"
    elif "firefox/" in ua_lower:
        browser = "Firefox"
    elif "safari/" in ua_lower:
        browser = "Safari"
    else:
        browser = "Browser"

    return f"{device} ({browser})"

# --- SQLite Database Setup ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS unique_visitors (
                visitor_id TEXT PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                ip_address TEXT,
                device_browser TEXT,
                visit_count INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                ip_address TEXT,
                device_info TEXT,
                platform TEXT,
                video_title TEXT,
                quality TEXT,
                video_url TEXT
            )
        """)
        conn.commit()

def register_unique_visitor(visitor_id: str, ip: str, ua: str) -> bool:
    """Returns True if this is a brand-new unique browser/device, False if it's a refresh."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    device_browser = parse_device_and_browser(ua)
    with sqlite3.connect(DB_PATH) as conn:
        existing = conn.execute(
            "SELECT visit_count FROM unique_visitors WHERE visitor_id = ?", (visitor_id,)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE unique_visitors SET last_seen = ?, ip_address = ?, visit_count = visit_count + 1 WHERE visitor_id = ?",
                (now, ip, visitor_id)
            )
            conn.commit()
            return False
        else:
            conn.execute(
                "INSERT INTO unique_visitors (visitor_id, first_seen, last_seen, ip_address, device_browser) VALUES (?, ?, ?, ?, ?)",
                (visitor_id, now, now, ip, device_browser)
            )
            conn.commit()
            return True

def log_event(event_type: str, ip: str, device: str, platform: str = "-", title: str = "-", quality: str = "-", url: str = "-"):
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        device_label = parse_device_and_browser(device)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """INSERT INTO activity_logs 
                   (timestamp, event_type, ip_address, device_info, platform, video_title, quality, video_url) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, event_type, ip, device_label, platform, title[:100], quality, url)
            )
            conn.commit()
    except Exception:
        pass

def get_total_unique_visitors() -> int:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT COUNT(*) FROM unique_visitors").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0

init_db()

# --- Real-Time Active & Unique Visitor Tracker ---
class VisitorManager:
    def __init__(self):
        # Maps WebSocket -> visitor_id so multiple tabs from the same browser count as 1 live user
        self.active_connections: dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, visitor_id: str):
        await websocket.accept()
        self.active_connections[websocket] = visitor_id
        client_ip = websocket.client.host if websocket.client else "Unknown"
        user_agent = websocket.headers.get("user-agent", "Unknown Browser")

        is_new = register_unique_visitor(visitor_id, client_ip, user_agent)
        if is_new:
            log_event("NEW_VISITOR", client_ip, user_agent)

        await self.broadcast_count()

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            del self.active_connections[websocket]
        await self.broadcast_count()

    async def broadcast_count(self):
        # Count distinct visitor_ids currently connected
        live_unique = len(set(self.active_connections.values()))
        total_unique = get_total_unique_visitors()
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_json({
                    "live_visitors": live_unique,
                    "total_visitors": total_unique
                })
            except Exception:
                pass

visitors = VisitorManager()

@app.websocket("/ws/visitors")
async def websocket_visitors(websocket: WebSocket, visitor_id: str = Query("anonymous")):
    await visitors.connect(websocket, visitor_id)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await visitors.disconnect(websocket)

# --- Admin Database Viewer (/admin/logs) ---
@app.get("/admin/logs", response_class=HTMLResponse)
def view_database_logs():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT 200").fetchall()
        total_downloads = conn.execute("SELECT COUNT(*) FROM activity_logs WHERE event_type='DOWNLOAD'").fetchone()[0]
        total_unique = conn.execute("SELECT COUNT(*) FROM unique_visitors").fetchone()[0]
        mobile_count = conn.execute("SELECT COUNT(*) FROM unique_visitors WHERE device_browser LIKE 'Mobile%'").fetchone()[0]
        desktop_count = conn.execute("SELECT COUNT(*) FROM unique_visitors WHERE device_browser LIKE 'Laptop%'").fetchone()[0]

    table_rows = ""
    for r in rows:
        badge_color = "#16a34a" if r["event_type"] == "DOWNLOAD" else "#2563eb"
        table_rows += f"""
        <tr>
          <td>#{r['id']}</td>
          <td>{r['timestamp']}</td>
          <td><span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:6px;font-size:12px;font-weight:700;">{r['event_type']}</span></td>
          <td><code>{r['ip_address']}</code></td>
          <td><strong>{r['device_info']}</strong></td>
          <td>{r['platform']}</td>
          <td>{r['video_title']}</td>
          <td>{r['quality']}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>ClipSave — Database Activity Logs</title>
      <style>
        body {{ font-family: system-ui, sans-serif; background: #f5f4f0; padding: 24px; color: #18181b; }}
        .stats {{ display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }}
        .stat-box {{ background: #fff; padding: 16px 22px; border-radius: 12px; border: 1px solid #e4e4e7; }}
        table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
        th, td {{ padding: 12px 14px; text-align: left; border-bottom: 1px solid #e4e4e7; font-size: 13px; }}
        th {{ background: #18181b; color: #fff; }}
      </style>
    </head>
    <body>
      <h2>📊 ClipSave Unique Visitors & Activity Logs</h2>
      <div class="stats">
        <div class="stat-box"><strong>Unique Visitors:</strong> {total_unique}</div>
        <div class="stat-box"><strong>📱 Mobile Users:</strong> {mobile_count}</div>
        <div class="stat-box"><strong>💻 Laptop/Desktop Users:</strong> {desktop_count}</div>
        <div class="stat-box"><strong>Total Downloads:</strong> {total_downloads}</div>
        <div class="stat-box"><strong>Live Right Now:</strong> {len(set(visitors.active_connections.values()))}</div>
      </div>
      <table>
        <thead>
          <tr><th>ID</th><th>Timestamp</th><th>Event</th><th>IP Address</th><th>Device & Browser</th><th>Platform</th><th>Video Title</th><th>Quality</th></tr>
        </thead>
        <tbody>{table_rows or '<tr><td colspan="8">No activity recorded yet.</td></tr>'}</tbody>
      </table>
    </body>
    </html>
    """