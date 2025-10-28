import os
import tempfile
import glob
import shutil
import subprocess
import json
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request, Response, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from yt_dlp import YoutubeDL
from pydantic import BaseModel
import uuid
import aiofiles

# FastAPI setup
app = FastAPI(
    title="Video Downloader API",
    description="Backend service for fetching and downloading video/audio info (transcode for compatibility)",
    version="2.0.0",
)

# CORS setup for Render deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
CONFIG_FILE = "config.json"
COOKIES_DIR = "cookies"
os.makedirs(COOKIES_DIR, exist_ok=True)

# Default configuration
DEFAULT_CONFIG = {
    "download_root": "ydl_downloads",
    "max_file_age_hours": 24,
    "cleanup_interval_hours": 6,
    "enable_cookies": True,
    "allowed_domains": ["youtube.com", "instagram.com", "twitter.com", "tiktok.com", "facebook.com"],
    "rate_limit_requests": 100,
    "rate_limit_period": 3600,
    "cookie_domains": {
        "youtube.com": "youtube.txt",
        "instagram.com": "instagram.txt",
        "facebook.com": "facebook.txt",
        "tiktok.com": "tiktok.txt"
    }
}

# Load or create configuration
def load_config():
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        else:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(DEFAULT_CONFIG, f, indent=2)
            return DEFAULT_CONFIG
    except Exception as e:
        print(f"[CONFIG ERROR] {e}")
        return DEFAULT_CONFIG

# Load cookies from files
def load_cookies_from_files():
    cookies_data = {}
    cookie_domains = config.get("cookie_domains", {})

    for domain, filename in cookie_domains.items():
        cookie_path = os.path.join(COOKIES_DIR, filename)
        if os.path.exists(cookie_path):
            try:
                with open(cookie_path, 'r', encoding='utf-8') as f:
                    cookies_data[domain] = f.read().strip()
                print(f"[COOKIES] Loaded cookies for {domain} from {filename}")
            except Exception as e:
                print(f"[COOKIES ERROR] Failed to load {cookie_path}: {e}")

    return cookies_data

# Save cookies to file
def save_cookies_to_file(domain: str, cookies_text: str):
    try:
        cookie_domains = config.get("cookie_domains", {})
        filename = cookie_domains.get(domain, f"{domain.replace('.', '_')}.txt")
        cookie_path = os.path.join(COOKIES_DIR, filename)

        with open(cookie_path, 'w', encoding='utf-8') as f:
            f.write(cookies_text)

        # Update config if new domain
        if domain not in cookie_domains:
            config["cookie_domains"][domain] = filename
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=2)

        return True
    except Exception as e:
        print(f"[COOKIES SAVE ERROR] {e}")
        return False

# Delete cookies file
def delete_cookies_file(domain: str):
    try:
        cookie_domains = config.get("cookie_domains", {})
        if domain in cookie_domains:
            filename = cookie_domains[domain]
            cookie_path = os.path.join(COOKIES_DIR, filename)
            if os.path.exists(cookie_path):
                os.remove(cookie_path)
                return True
        return False
    except Exception as e:
        print(f"[COOKIES DELETE ERROR] {e}")
        return False

# Initialize config and cookies
config = load_config()
cookies_data = load_cookies_from_files()

# Download folder setup
DOWNLOAD_ROOT = os.path.abspath(config["download_root"])
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

# Session management
user_sessions = {}

def get_user_session(request: Request):
    """Get or create user session"""
    session_id = request.cookies.get("session_id")
    if not session_id or session_id not in user_sessions:
        session_id = str(uuid.uuid4())
        user_sessions[session_id] = {
            "created_at": subprocess.run(["date", "-u"], capture_output=True, text=True).stdout.strip(),
            "download_count": 0,
            "last_activity": subprocess.run(["date", "-u"], capture_output=True, text=True).stdout.strip()
        }
    return session_id, user_sessions[session_id]

def remove_file_later(path: str):
    """Delete file and its parent folder if empty"""
    try:
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        # remove parent tmp folder if empty
        if os.path.isdir(parent) and not os.listdir(parent):
            os.rmdir(parent)
    except Exception as e:
        print(f"[CLEANUP ERROR] {e}")

def run_subprocess(cmd: list, check: bool = True, capture: bool = False):
    """Helper to run subprocess and raise readable exception on failure."""
    try:
        if capture:
            completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)
            return completed.stdout.decode("utf-8", errors="ignore"), completed.stderr.decode("utf-8", errors="ignore")
        else:
            subprocess.run(cmd, check=check)
            return None, None
    except subprocess.CalledProcessError as e:
        out = e.stdout.decode("utf-8", errors="ignore") if e.stdout else ""
        err = e.stderr.decode("utf-8", errors="ignore") if e.stderr else str(e)
        raise RuntimeError(f"Command `{cmd}` failed: {err}\n{out}")

def ffprobe_has_audio(path: str) -> bool:
    """Return True if file has at least one audio stream (uses ffprobe)."""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "csv=p=0",
        path,
    ]
    try:
        out, err = run_subprocess(cmd, capture=True)
        # output lines may contain "audio" and "video"
        return "audio" in out.splitlines()
    except Exception as e:
        print(f"[ffprobe error] {e}")
        # be conservative: assume has audio if ffprobe fails
        return True

def ffprobe_stream_info(path: str) -> dict:
    """
    Returns a dict of stream counts and codecs using ffprobe.
    Example return: {'audio': ['aac'], 'video': ['h264']}
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:a",
        "-show_entries",
        "stream=index,codec_type,codec_name",
        "-of",
        "json",
        path,
    ]
    try:
        out, _ = run_subprocess(cmd, capture=True)
        import json

        data = json.loads(out) if out else {}
        streams = data.get("streams", [])
        info = {"audio": [], "video": []}
        for s in streams:
            ctype = s.get("codec_type")
            cname = s.get("codec_name")
            if ctype and cname:
                if ctype in info:
                    info[ctype].append(cname)
        return info
    except Exception as e:
        print(f"[ffprobe info error] {e}")
        return {"audio": [], "video": []}

def transcode_to_compatible_mp4(video_path: str,
                                audio_path: Optional[str],
                                output_path: str):
    """
    Merge or transcode into an MP4 (H.264 + AAC).
    If audio_path is None or missing, inject a silent stereo AAC track.
    """
    cmd = ["ffmpeg", "-y"]

    if audio_path and os.path.exists(audio_path):
        # normal merge
        cmd += [
            "-i", video_path,
            "-i", audio_path,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-map", "0:v:0", "-map", "1:a:0",
            "-shortest", "-movflags", "+faststart", output_path
        ]
    else:
        # no audio file: add silent AAC track
        cmd += [
            "-i", video_path,
            "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
            "-shortest",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-map", "0:v:0", "-map", "1:a:0",
            "-movflags", "+faststart", output_path
        ]

    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print("[ffmpeg error]", e.stderr.decode(errors="ignore"))
        return False

class FetchRequest(BaseModel):
    url: str

class CookieData(BaseModel):
    domain: str
    cookies: str

@app.get("/")
async def root(request: Request):
    session_id, session = get_user_session(request)
    response = JSONResponse({
        "status": "running",
        "message": "Go to /docs for Swagger UI.",
        "session_id": session_id,
        "download_count": session["download_count"],
        "cookies_configured": list(cookies_data.keys())
    })
    response.set_cookie(key="session_id", value=session_id, httponly=True, max_age=3600*24)
    return response

@app.get("/config")
async def get_config():
    return config

@app.post("/config")
async def update_config(new_config: dict):
    global config
    config.update(new_config)
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        return {"status": "success", "message": "Configuration updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save config: {e}")

@app.get("/cookies")
async def get_cookies():
    return {
        "domains": list(cookies_data.keys()),
        "cookie_files": [f for f in os.listdir(COOKIES_DIR) if f.endswith('.txt')]
    }

@app.post("/cookies")
async def add_cookies(cookie_data: CookieData):
    if save_cookies_to_file(cookie_data.domain, cookie_data.cookies):
        # Reload cookies
        global cookies_data
        cookies_data = load_cookies_from_files()
        return {"status": "success", "message": f"Cookies added for {cookie_data.domain}"}
    else:
        raise HTTPException(status_code=500, detail="Failed to save cookies")

@app.post("/upload-cookies-file")
async def upload_cookies_file(file: UploadFile = File(...), domain: str = Query(...)):
    try:
        contents = await file.read()
        cookies_text = contents.decode('utf-8')

        if save_cookies_to_file(domain, cookies_text):
            global cookies_data
            cookies_data = load_cookies_from_files()
            return {"status": "success", "message": f"Cookie file uploaded for {domain}"}
        else:
            raise HTTPException(status_code=500, detail="Failed to save cookie file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")

@app.delete("/cookies/{domain}")
async def delete_cookies(domain: str):
    if delete_cookies_file(domain):
        global cookies_data
        cookies_data = load_cookies_from_files()
        return {"status": "success", "message": f"Cookies removed for {domain}"}
    raise HTTPException(status_code=404, detail="Domain not found in cookies")

@app.get("/cookies/{domain}")
async def get_cookies_for_domain(domain: str):
    if domain in cookies_data:
        return {"domain": domain, "cookies_available": True}
    return {"domain": domain, "cookies_available": False}

@app.post("/fetch_info")
async def fetch_info(req: FetchRequest, request: Request):
    session_id, session = get_user_session(request)
    url = req.url.strip()

    # Update session activity
    session["last_activity"] = subprocess.run(["date", "-u"], capture_output=True, text=True).stdout.strip()

    if "instagram.com/reels/audio" in url:
        raise HTTPException(
            status_code=400,
            detail="Instagram audio pages are not downloadable. Use the actual reel link instead."
        )

    try:
        if not url:
            raise HTTPException(status_code=400, detail="Missing 'url' field")

        print(f"[INFO] Fetching info for: {url}")

        # Prepare yt-dlp options with cookies if available
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "noplaylist": True,
            "geo_bypass": True,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        # Add cookies from files if available for the domain
        if config.get("enable_cookies", True):
            for cookie_domain, cookie_content in cookies_data.items():
                if cookie_domain in url:
                    ydl_opts["cookiefile"] = os.path.join(COOKIES_DIR, config["cookie_domains"][cookie_domain])
                    print(f"[INFO] Using cookie file for domain: {cookie_domain}")
                    break

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        audio_formats = {}
        video_formats = []

        duration = info.get("duration") or 0

        for f in info.get("formats", []):
            if not f.get("acodec") and not f.get("vcodec"):
                continue

            # AUDIO ONLY
            if f.get("vcodec") == "none" and f.get("acodec") != "none":
                abr = float(f.get("abr") or 0)
                if abr <= 0:
                    continue

                if abr < 90:
                    quality = "Low"
                elif abr < 170:
                    quality = "Medium"
                else:
                    quality = "High"

                filesize = f.get("filesize") or f.get("filesize_approx")
                if not filesize and duration > 0:
                    estimated_bytes = (abr * 1000 / 8) * duration
                    if estimated_bytes > 100 * 1024:
                        filesize = int(estimated_bytes)

                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                if quality not in audio_formats or abr > audio_formats[quality]["abr"]:
                    audio_formats[quality] = {
                        "format_id": f.get("format_id"),
                        "ext": f.get("ext"),
                        "format_note": f"{quality} ({round(abr)} kbps)",
                        "format_note_clean": quality.lower(),
                        "filesize": filesize,
                        "filesize_mb": filesize_mb,
                        "filesize_text": filesize_str,
                        "vcodec": f.get("vcodec"),
                        "acodec": f.get("acodec"),
                        "abr": round(abr),
                        "bitrate": round(abr),
                    }

            # VIDEO FORMATS
            elif f.get("vcodec") != "none":
                height = f.get("height")
                label = f"{height}p" if height else f.get("format_note") or "Video"
                filesize = f.get("filesize") or f.get("filesize_approx")

                if not filesize and duration and f.get("tbr"):
                    filesize = int((f["tbr"] * 1000 / 8) * duration)

                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                video_formats.append({
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "format_note": label,
                    "filesize": filesize,
                    "filesize_mb": filesize_mb,
                    "filesize_text": filesize_str,
                    "width": f.get("width"),
                    "height": height,
                    "vcodec": f.get("vcodec"),
                    "acodec": f.get("acodec"),
                })

        formats_out = list(audio_formats.values()) + video_formats

        return {
            "id": info.get("id"),
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": duration,
            "uploader": info.get("uploader"),
            "formats": formats_out,
            "session_id": session_id,
        }

    except Exception as e:
        print(f"[ERROR] fetch_info: {e}")
        raise HTTPException(status_code=500, detail=f"yt-dlp extract failed: {e}")

@app.get("/download")
async def download(
        video_url: str = Query(...),
        format_id: Optional[str] = Query(None),
        background_tasks: BackgroundTasks = None,
        request: Request = None
):
    """
    Always produces an MP4 that *has* audio.
    For FB/IG: fetch bestvideo + bestaudio if the chosen format is mute.
    """
    session_id, session = get_user_session(request)
    session["download_count"] += 1

    print(f"[INFO] Downloading {video_url} | fmt={format_id} | session={session_id}")
    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    # Prepare yt-dlp options with cookies if available
    ydl_base_opts = {
        "quiet": True,
        "no_warnings": True,
    }

    # Add cookies from files if available for the domain
    if config.get("enable_cookies", True):
        for cookie_domain, cookie_content in cookies_data.items():
            if cookie_domain in video_url:
                ydl_base_opts["cookiefile"] = os.path.join(COOKIES_DIR, config["cookie_domains"][cookie_domain])
                print(f"[INFO] Using cookie file for domain: {cookie_domain}")
                break

    # Step 1: probe formats to see if selected one has audio
    info = None
    with YoutubeDL(ydl_base_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    chosen = next((f for f in info.get("formats", [])
                   if f.get("format_id") == format_id), None)
    has_audio = chosen and chosen.get("acodec") != "none"

    # Step 2: download video (and possibly audio)
    video_path = None
    audio_path = None

    try:
        # --- Video download ---
        video_fmt = format_id or "bestvideo+bestaudio/best"
        video_ydl_opts = {
            **ydl_base_opts,
            "outtmpl": outtmpl,
            "format": video_fmt
        }

        with YoutubeDL(video_ydl_opts) as ydl:
            ydl.download([video_url])

        files = glob.glob(os.path.join(tmpdir, "*"))
        if not files:
            raise HTTPException(status_code=500, detail="No file downloaded.")
        files.sort(key=os.path.getsize, reverse=True)
        video_path = files[0]

        # --- Separate audio if needed ---
        if not has_audio or not ffprobe_has_audio(video_path):
            print("[INFO] Fetching best audio stream …")
            audio_out = os.path.join(tmpdir, "audio.m4a")
            try:
                audio_ydl_opts = {
                    **ydl_base_opts,
                    "outtmpl": audio_out,
                    "format": "bestaudio"
                }
                with YoutubeDL(audio_ydl_opts) as ydl:
                    ydl.download([video_url])
                if os.path.exists(audio_out):
                    audio_path = audio_out
            except Exception as e:
                print("[WARN] bestaudio fetch failed", e)

        # --- Step 3: Transcode / merge to final MP4 ---
        output_path = os.path.join(tmpdir, "final.mp4")
        ok = transcode_to_compatible_mp4(video_path, audio_path, output_path)
        final_path = output_path if ok and os.path.exists(output_path) else video_path

        # cleanup later
        if background_tasks:
            background_tasks.add_task(remove_file_later, final_path)

        print(f"[SUCCESS] Serving {final_path} for session {session_id}")
        response = FileResponse(
            final_path,
            filename=os.path.basename(final_path),
            media_type="video/mp4"
        )
        response.set_cookie(key="session_id", value=session_id, httponly=True, max_age=3600*24)
        return response

    except Exception as e:
        print("[ERROR] download", e)
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sessions")
async def list_sessions():
    return user_sessions

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "download_root": DOWNLOAD_ROOT,
        "sessions_count": len(user_sessions),
        "cookies_domains": list(cookies_data.keys()),
        "cookie_files": [f for f in os.listdir(COOKIES_DIR) if f.endswith('.txt')]
    }

# Cleanup old sessions (basic implementation)
@app.on_event("startup")
async def startup_event():
    print(f"[INFO] Server starting up on Render")
    print(f"[INFO] Download root: {DOWNLOAD_ROOT}")
    print(f"[INFO] Config loaded: {len(config)} items")
    print(f"[INFO] Cookies loaded: {len(cookies_data)} domains")
    print(f"[INFO] Cookie files: {[f for f in os.listdir(COOKIES_DIR) if f.endswith('.txt')]}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)