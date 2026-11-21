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

# --- SQLite Database Setup (Zero Setup, Built into Python) ---
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
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

def log_event(event_type: str, ip: str, device: str, platform: str = "-", title: str = "-", quality: str = "-", url: str = "-"):
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """INSERT INTO activity_logs 
                   (timestamp, event_type, ip_address, device_info, platform, video_title, quality, video_url) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, event_type, ip, device[:120], platform, title[:100], quality, url)
            )
            conn.commit()
    except Exception:
        pass

init_db()

# --- Real-Time Active Visitor Tracker ---
class VisitorManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        client_ip = websocket.client.host if websocket.client else "Unknown"
        user_agent = websocket.headers.get("user-agent", "Unknown Browser")
        log_event("VISITOR_JOINED", client_ip, user_agent)
        await self.broadcast_count()

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        await self.broadcast_count()

    async def broadcast_count(self):
        count = len(self.active_connections)
        for connection in list(self.active_connections):
            try:
                await connection.send_json({"live_visitors": count})
            except Exception:
                pass

visitors = VisitorManager()

@app.websocket("/ws/visitors")
async def websocket_visitors(websocket: WebSocket):
    await visitors.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await visitors.disconnect(websocket)

# --- Admin Database Viewer (Open http://127.0.0.1:8000/admin/logs) ---
@app.get("/admin/logs", response_class=HTMLResponse)
def view_database_logs():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM activity_logs ORDER BY id DESC LIMIT 200").fetchall()
        total_downloads = conn.execute("SELECT COUNT(*) FROM activity_logs WHERE event_type='DOWNLOAD'").fetchone()[0]
        total_visits = conn.execute("SELECT COUNT(*) FROM activity_logs WHERE event_type='VISITOR_JOINED'").fetchone()[0]

    table_rows = ""
    for r in rows:
        badge_color = "#16a34a" if r["event_type"] == "DOWNLOAD" else "#2563eb"
        table_rows += f"""
        <tr>
          <td>#{r['id']}</td>
          <td>{r['timestamp']}</td>
          <td><span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:6px;font-size:12px;font-weight:700;">{r['event_type']}</span></td>
          <td><code>{r['ip_address']}</code></td>
          <td>{r['platform']}</td>
          <td>{r['video_title']}</td>
          <td>{r['quality']}</td>
          <td style="font-size:12px;color:#52525b;">{r['device_info']}</td>
        </tr>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>ClipSave — Database Activity Logs</title>
      <style>
        body {{ font-family: system-ui, sans-serif; background: #f5f4f0; padding: 24px; color: #18181b; }}
        .stats {{ display: flex; gap: 16px; margin-bottom: 20px; }}
        .stat-box {{ background: #fff; padding: 16px 22px; border-radius: 12px; border: 1px solid #e4e4e7; }}
        table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
        th, td {{ padding: 12px 14px; text-align: left; border-bottom: 1px solid #e4e4e7; font-size: 13px; }}
        th {{ background: #18181b; color: #fff; }}
      </style>
    </head>
    <body>
      <h2>📊 ClipSave Database Logs (SQLite)</h2>
      <div class="stats">
        <div class="stat-box"><strong>Total Visits:</strong> {total_visits}</div>
        <div class="stat-box"><strong>Total Downloads:</strong> {total_downloads}</div>
        <div class="stat-box"><strong>Live Right Now:</strong> {len(visitors.active_connections)}</div>
      </div>
      <table>
        <thead>
          <tr><th>ID</th><th>Timestamp</th><th>Event</th><th>IP Address</th><th>Platform</th><th>Video Title</th><th>Quality</th><th>Device / Browser</th></tr>
        </thead>
        <tbody>{table_rows or '<tr><td colspan="8">No activity recorded yet.</td></tr>'}</tbody>
      </table>
    </body>
    </html>
    """

# --- Video Extraction & High-Quality Merging ---
class VideoRequest(BaseModel):
    url: str

@app.get("/")
def serve_home():
    return FileResponse("static/index.html")

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

            # Log link check into SQLite database
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

    # Log actual download into SQLite database
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