import os
import tempfile
import glob
import shutil
import subprocess
import json
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from yt_dlp import YoutubeDL
from pydantic import BaseModel

# ---------------- FASTAPI SETUP ---------------- #
app = FastAPI(
    title="Global Video & Audio Downloader API",
    description="FastAPI backend supporting YouTube, Instagram, Facebook, TikTok with cookie-based authentication",
    version="3.1.0",
)

# Allow cross-platform access (for Flutter apps)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- DIRECTORIES ---------------- #
DOWNLOAD_ROOT = os.path.abspath("ydl_downloads")
COOKIE_DIR = os.path.abspath("cookies")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
os.makedirs(COOKIE_DIR, exist_ok=True)

# ---------------- COOKIE MAPPING ---------------- #
COOKIES_MAP = {
    "youtube.com": os.path.join(COOKIE_DIR, "youtube.txt"),
    "youtu.be": os.path.join(COOKIE_DIR, "youtube.txt"),
    "facebook.com": os.path.join(COOKIE_DIR, "facebook.txt"),
    "fb.watch": os.path.join(COOKIE_DIR, "facebook.txt"),
    "instagram.com": os.path.join(COOKIE_DIR, "instagram.txt"),
    "tiktok.com": os.path.join(COOKIE_DIR, "tiktok.txt"),
}

def get_cookie_file(url: str) -> Optional[str]:
    """Return correct cookie file path for given domain"""
    for key, path in COOKIES_MAP.items():
        if key in url and os.path.exists(path):
            return path
    return None


def remove_file_later(path: str):
    """Background cleanup of files"""
    try:
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        if os.path.isdir(parent) and not os.listdir(parent):
            os.rmdir(parent)
    except Exception as e:
        print(f"[CLEANUP ERROR] {e}")


# ---------------- MODELS ---------------- #
class FetchRequest(BaseModel):
    url: str


# ---------------- ROUTES ---------------- #
@app.get("/")
async def root():
    return {"status": "running", "message": "Video Downloader API v3 active"}


@app.post("/fetch_info")
async def fetch_info(req: FetchRequest):
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="Missing URL")

    cookies = get_cookie_file(url)
    print(f"[INFO] Fetching metadata for: {url}")
    if cookies:
        print(f"[INFO] Using cookies: {cookies}")

    try:
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "geo_bypass": True,
            "cookiefile": cookies,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        # Save `info` to disk (used and prevents unused-import warnings for json)
        info_id = info.get("id") or info.get("webpage_url") or "video"
        safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(info_id))[:200]
        info_json_path = os.path.join(DOWNLOAD_ROOT, f"{safe_id}_info.json")
        try:
            with open(info_json_path, "w", encoding="utf-8") as jf:
                json.dump(info, jf, indent=2, ensure_ascii=False)
        except Exception as e:
            # non-fatal: continue even if saving fails
            print(f"[WARN] Could not write info json: {e}")
            info_json_path = None

        # Build full format list
        formats_out = []
        for f in info.get("formats", []):
            fmt = {
                "format_id": f.get("format_id"),
                "ext": f.get("ext"),
                "format_note": f.get("format_note"),
                "filesize": f.get("filesize") or f.get("filesize_approx"),
                "vcodec": f.get("vcodec"),
                "acodec": f.get("acodec"),
                "width": f.get("width"),
                "height": f.get("height"),
                "fps": f.get("fps"),
                "tbr": f.get("tbr"),
            }
            formats_out.append(fmt)

        return {
            "id": info.get("id"),
            "title": info.get("title"),
            "uploader": info.get("uploader"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "webpage_url": info.get("webpage_url"),
            "formats": formats_out,
            "info_json": info_json_path,
        }

    except Exception as e:
        print(f"[ERROR] fetch_info: {e}")
        raise HTTPException(status_code=500, detail=f"yt-dlp error: {str(e)}")


@app.get("/download")
async def download(video_url: str = Query(...), format_id: Optional[str] = Query(None),
                   background_tasks: BackgroundTasks = None):
    """Download selected format (video/audio)"""
    print(f"[INFO] Downloading {video_url} | format={format_id}")

    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    cookies = get_cookie_file(video_url)

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "outtmpl": outtmpl,
        "format": format_id or "best",
        "cookiefile": cookies,
        "merge_output_format": "mp4",
    }

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=True)

            # use info to derive a recommended filename (ensures 'info' is used)
            recommended_name = None
            try:
                recommended_name = ydl.prepare_filename(info)
            except Exception:
                recommended_name = None

            files = glob.glob(os.path.join(tmpdir, "*"))
            if not files:
                raise HTTPException(status_code=500, detail="No file downloaded.")
            files.sort(key=os.path.getsize, reverse=True)
            file_path = files[0]

        # if we have a recommended_name and it exists, prefer that path
        if recommended_name and os.path.exists(recommended_name):
            file_path = recommended_name

        if background_tasks:
            background_tasks.add_task(remove_file_later, file_path)

        filename = os.path.basename(file_path)
        # choose a sensible mime type based on extension
        ext = filename.lower().split(".")[-1]
        if ext in ("mp3", "m4a", "aac", "opus"):
            mime_type = "audio/mpeg"
        elif ext in ("webm", "mkv"):
            # serve as generic binary if not mp4
            mime_type = "video/webm" if ext == "webm" else "video/x-matroska"
        else:
            mime_type = "video/mp4"

        return FileResponse(file_path, filename=filename, media_type=mime_type)

    except Exception as e:
        print(f"[ERROR] download: {e}")
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))
