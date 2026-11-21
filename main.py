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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS support_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                user_name TEXT,
                device_info TEXT,
                ip_address TEXT,
                message TEXT NOT NULL
            )
        """)
        conn.commit()

def register_unique_visitor(visitor_id: str, ip: str, ua: str) -> bool:
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

# --- Home Route ---
@app.get("/")
def serve_home():
    return FileResponse("static/index.html")

# --- Help Desk Support Query Endpoint ---
class SupportQueryRequest(BaseModel):
    name: str = "Anonymous User"
    message: str

@app.post("/api/support")
def submit_support_query(req: SupportQueryRequest, request: Request):
    clean_msg = req.message.strip()
    if not clean_msg:
        raise HTTPException(status_code=400, detail="Please enter your question or feedback.")

    clean_name = req.name.strip()[:40] or "Anonymous User"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    client_ip = request.client.host if request.client else "Unknown"
    device_label = parse_device_and_browser(request.headers.get("user-agent", "Unknown"))

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO support_queries (timestamp, user_name, device_info, ip_address, message) VALUES (?, ?, ?, ?, ?)",
            (now, clean_name, device_label, client_ip, clean_msg[:500])
        )
        conn.commit()

    return {"status": "ok", "message": "Thanks! Your query has been logged for our team."}

# --- Admin Database Viewer (/admin/logs) ---
@app.get("/admin/logs", response_class=HTMLResponse)
def view_database_logs():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT 200").fetchall()
        queries = conn.execute("SELECT * FROM support_queries ORDER BY id DESC LIMIT 50").fetchall()
        total_downloads = conn.execute("SELECT COUNT(*) FROM activity_logs WHERE event_type='DOWNLOAD'").fetchone()[0]
        total_unique = conn.execute("SELECT COUNT(*) FROM unique_visitors").fetchone()[0]
        mobile_count = conn.execute("SELECT COUNT(*) FROM unique_visitors WHERE device_browser LIKE 'Mobile%'").fetchone()[0]
        desktop_count = conn.execute("SELECT COUNT(*) FROM unique_visitors WHERE device_browser LIKE 'Laptop%'").fetchone()[0]

    query_rows = ""
    for q in queries:
        query_rows += f"""
        <tr>
          <td>#{q['id']}</td>
          <td>{q['timestamp']}</td>
          <td><strong>{q['user_name']}</strong></td>
          <td>{q['device_info']}</td>
          <td>{q['message']}</td>
        </tr>
        """

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
      <title>ClipSave — Database & Support Logs</title>
      <style>
        body {{ font-family: system-ui, sans-serif; background: #f5f4f0; padding: 24px; color: #18181b; }}
        .stats {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
        .stat-box {{ background: #fff; padding: 16px 22px; border-radius: 12px; border: 1px solid #e4e4e7; }}
        h3 {{ margin: 24px 0 12px 0; }}
        table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.04); margin-bottom: 24px; }}
        th, td {{ padding: 12px 14px; text-align: left; border-bottom: 1px solid #e4e4e7; font-size: 13px; }}
        th {{ background: #18181b; color: #fff; }}
      </style>
    </head>
    <body>
      <h2>📊 ClipSave Admin Dashboard (SQLite)</h2>
      <div class="stats">
        <div class="stat-box"><strong>Unique Visitors:</strong> {total_unique}</div>
        <div class="stat-box"><strong>📱 Mobile Users:</strong> {mobile_count}</div>
        <div class="stat-box"><strong>💻 Laptop Users:</strong> {desktop_count}</div>
        <div class="stat-box"><strong>Total Downloads:</strong> {total_downloads}</div>
        <div class="stat-box"><strong>Support Queries:</strong> {len(queries)}</div>
        <div class="stat-box"><strong>Live Right Now:</strong> {len(set(visitors.active_connections.values()))}</div>
      </div>

      <h3>💬 Help Desk User Queries</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>Timestamp</th><th>User Name</th><th>Device & Browser</th><th>Question / Feedback</th></tr>
        </thead>
        <tbody>{query_rows or '<tr><td colspan="5">No help desk messages yet.</td></tr>'}</tbody>
      </table>

      <h3>📥 Activity & Download Logs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>Timestamp</th><th>Event</th><th>IP Address</th><th>Device & Browser</th><th>Platform</th><th>Video Title</th><th>Quality</th></tr>
        </thead>
        <tbody>{table_rows or '<tr><td colspan="8">No activity recorded yet.</td></tr>'}</tbody>
      </table>
    </body>
    </html>
    """

# --- Video Extraction & High-Quality Merging ---
class VideoRequest(BaseModel):
    url: str

@app.post("/api/extract")
def extract_video_info(req: VideoRequest, request: Request):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'noplaylist': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(req.url, download=False)

            if 'entries' in info and info['entries']:
                info = next((entry for entry in info['entries'] if entry), info)

            title = info.get("title") or info.get("description") or "video"
            title = title[:80].strip()
            platform = info.get("extractor_key", "Web")
            default_headers = info.get("http_headers", {})
            raw_formats = info.get("formats") or []

            client_ip = request.client.host if request.client else "Unknown"
            user_agent = request.headers.get("user-agent", "Unknown")
            log_event("LINK_SEARCHED", client_ip, user_agent, platform, title, "-", req.url)

            formats = []
            available_heights = set()

            for f in raw_formats:
                h = f.get('height')
                vcodec = f.get('vcodec')
                if h and vcodec and vcodec != 'none':
                    available_heights.add(h)

            target_resolutions = sorted(list(available_heights), reverse=True)
            for h in target_resolutions[:3]:
                formats.append({
                    "quality": f"{h}p HD" if h >= 720 else f"{h}p MP4",
                    "ext": "mp4",
                    "size": "Best Audio + Video",
                    "is_merged": True,
                    "height": h,
                    "source_url": req.url,
                    "url": "",
                    "headers": {}
                })

            if not formats and (raw_formats or info.get('url')):
                formats.append({
                    "quality": "Best Available Quality",
                    "ext": "mp4",
                    "size": "Auto-Merged MP4",
                    "is_merged": True,
                    "height": 0,
                    "source_url": req.url,
                    "url": "",
                    "headers": default_headers
                })

            if not formats:
                raise HTTPException(
                    status_code=400,
                    detail="No video stream found in this link (it may be an image-only post or private account)."
                )

            return {
                "title": title,
                "thumbnail": info.get("thumbnail", ""),
                "platform": platform,
                "formats": formats
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not fetch video: {str(e)}")

@app.get("/api/download")
async def proxy_download(
        request: Request,
        background_tasks: BackgroundTasks,
        url: str = Query(""),
        source_url: str = Query(""),
        is_merged: bool = Query(False),
        height: int = Query(0),
        title: str = Query("video"),
        referer: str = Query(""),
        user_agent: str = Query("")
):
    safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip() or "video"
    client_ip = request.client.host if request.client else "Unknown"
    browser_ua = request.headers.get("user-agent", "Unknown")
    quality_tag = f"{height}p" if height else "Best Quality"

    log_event("DOWNLOAD", client_ip, browser_ua, "Media", title, quality_tag, source_url or url)

    if is_merged and source_url:
        temp_dir = tempfile.gettempdir()
        temp_id = uuid.uuid4().hex
        output_template = os.path.join(temp_dir, f"{temp_id}.%(ext)s")
        final_file = os.path.join(temp_dir, f"{temp_id}.mp4")

        format_selector = (
            f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={height}]+bestaudio/best[height<={height}]/best"
            if height > 0 else
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
        )

        ydl_opts = {
            'format': format_selector,
            'merge_output_format': 'mp4',
            'outtmpl': output_template,
            'ffmpeg_location': imageio_ffmpeg.get_ffmpeg_exe(),
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([source_url])

            if not os.path.exists(final_file):
                raise HTTPException(status_code=500, detail="Failed to merge video and audio.")

            background_tasks.add_task(os.remove, final_file)

            return FileResponse(
                path=final_file,
                media_type="video/mp4",
                filename=f"{safe_filename}_{height}p.mp4" if height else f"{safe_filename}.mp4"
            )
        except Exception as e:
            if os.path.exists(final_file):
                os.remove(final_file)
            raise HTTPException(status_code=500, detail=f"Download Error: {str(e)}")

    headers = {}
    if referer:
        headers["Referer"] = referer
    if user_agent:
        headers["User-Agent"] = user_agent

    async def stream_generator():
        async with httpx.AsyncClient(follow_redirects=True, timeout=None) as client:
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code >= 400:
                    raise HTTPException(status_code=response.status_code, detail="CDN rejected stream")
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    yield chunk

    return StreamingResponse(
        stream_generator(),
        media_type="video/mp4",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}.mp4"'
        }
    )