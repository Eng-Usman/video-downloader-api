import os
import logging
import tempfile
import subprocess
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from yt_dlp import YoutubeDL

# Initialize FastAPI app
app = FastAPI(title="Universal Video Downloader API")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("server")

# Directory for cookies
COOKIES_DIR = "cookies"
os.makedirs(COOKIES_DIR, exist_ok=True)

# Directory for temporary downloads
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# --- Helper: detect correct cookie file ---
def get_cookie_file(url: str) -> str | None:
    mapping = {
        "youtube.com": "youtube.txt",
        "youtu.be": "youtube.txt",
        "facebook.com": "facebook.txt",
        "fb.watch": "facebook.txt",
        "instagram.com": "instagram.txt",
        "tiktok.com": "tiktok.txt",
        "x.com": "twitter.txt",
        "twitter.com": "twitter.txt",
    }
    for domain, filename in mapping.items():
        if domain in url:
            cookie_path = os.path.join(COOKIES_DIR, filename)
            if os.path.exists(cookie_path):
                return cookie_path
    return None


# --- Core metadata extraction ---
def fetch_metadata(url: str):
    logger.info(f"[INFO] Fetching metadata for: {url}")
    cookie_file = get_cookie_file(url)

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": False,
        "forcejson": True,
        "no_warnings": True,
        "geo_bypass": True,
        "noplaylist": True,
        "source_address": "0.0.0.0",
        "age_limit": 0,
        "format_sort": ["res,br"],
    }

    if cookie_file:
        logger.info(f"[INFO] Using cookies: {cookie_file}")
        ydl_opts["cookiefile"] = cookie_file

    try:
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        if not info:
            raise Exception("Failed to extract metadata")

        formats = []
        for f in info.get("formats", []):
            if not f.get("url"):
                continue

            media_type = "unknown"
            if f.get("vcodec") != "none" and f.get("acodec") != "none":
                media_type = "video+audio"
            elif f.get("vcodec") != "none":
                media_type = "video"
            elif f.get("acodec") != "none":
                media_type = "audio"

            formats.append({
                "format_id": f.get("format_id"),
                "ext": f.get("ext"),
                "resolution": f.get("format_note") or f.get("resolution"),
                "filesize": f.get("filesize"),
                "tbr": f.get("tbr"),
                "media_type": media_type,
                "url": f.get("url"),
            })

        return {
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "formats": formats,
        }

    except Exception as e:
        logger.error(f"[ERROR] fetch_metadata: {e}")
        # Retry without cookies
        if cookie_file:
            logger.warning("[WARN] Retrying without cookies...")
            ydl_opts.pop("cookiefile", None)
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            formats = [
                {
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "resolution": f.get("format_note") or f.get("resolution"),
                    "url": f.get("url"),
                }
                for f in info.get("formats", [])
                if f.get("url")
            ]
            return {
                "title": info.get("title"),
                "thumbnail": info.get("thumbnail"),
                "duration": info.get("duration"),
                "formats": formats,
            }
        raise e


# --- Download specific format ---
def download_media(url: str, format_id: str, is_audio: bool = False) -> str:
    logger.info(f"[INFO] Downloading: {url} [{format_id}]")

    cookie_file = get_cookie_file(url)
    output_template = os.path.join(DOWNLOAD_DIR, "%(title)s.%(ext)s")

    ydl_opts = {
        "format": format_id,
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "merge_output_format": "mp4" if not is_audio else "mp3",
        "postprocessors": [],
    }

    # Enable ffmpeg postprocessing for audio
    if is_audio:
        ydl_opts["postprocessors"].append({
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        })
    else:
        ydl_opts["postprocessors"].append({
            "key": "FFmpegVideoConvertor",
            "preferedformat": "mp4",
        })

    if cookie_file:
        ydl_opts["cookiefile"] = cookie_file

    try:
        with YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(url, download=True)
            filename = ydl.prepare_filename(result)
            # Convert file extension if ffmpeg postprocessed it
            if is_audio and not filename.endswith(".mp3"):
                base = os.path.splitext(filename)[0]
                filename = base + ".mp3"
            elif not is_audio and not filename.endswith(".mp4"):
                base = os.path.splitext(filename)[0]
                filename = base + ".mp4"
            return filename

    except Exception as e:
        logger.error(f"[ERROR] download_media: {e}")
        raise e


# --- API Routes ---
@app.post("/fetch_info")
async def fetch_info(request: Request):
    try:
        data = await request.json()
        url = data.get("url")
        if not url:
            return JSONResponse({"error": "Missing URL"}, status_code=400)
        result = fetch_metadata(url)
        return JSONResponse(result)
    except Exception as e:
        logger.exception("fetch_info failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/download")
async def download(request: Request):
    try:
        data = await request.json()
        url = data.get("url")
        format_id = data.get("format_id")
        is_audio = data.get("is_audio", False)

        if not url or not format_id:
            return JSONResponse({"error": "Missing parameters"}, status_code=400)

        file_path = download_media(url, format_id, is_audio=is_audio)

        if not os.path.exists(file_path):
            return JSONResponse({"error": "Download failed"}, status_code=500)

        return FileResponse(
            path=file_path,
            media_type="audio/mpeg" if is_audio else "video/mp4",
            filename=os.path.basename(file_path)
        )
    except Exception as e:
        logger.exception("download failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/")
def root():
    return {"status": "✅ Video Downloader API is running"}


# --- Optional cleanup ---
@app.on_event("shutdown")
def cleanup_temp_files():
    try:
        for f in os.listdir(DOWNLOAD_DIR):
            os.remove(os.path.join(DOWNLOAD_DIR, f))
        logger.info("Cleaned up temporary files.")
    except Exception as e:
        logger.warning(f"Cleanup failed: {e}")
