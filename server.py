import os
import tempfile
import glob
import shutil
import subprocess
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from yt_dlp import YoutubeDL
from pydantic import BaseModel
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration for Cloud/Render ---
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", "/tmp/ydl_downloads")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
logger.info(f"Using DOWNLOAD_ROOT: {DOWNLOAD_ROOT}")

COOKIES_FILE = os.environ.get("COOKIES_FILE_PATH", "cookies.txt")
if not os.path.exists(COOKIES_FILE):
    logger.warning(f"Cookies file not found at {COOKIES_FILE}. Social media downloads might fail.")

# FastAPI setup (rest of the setup is unchanged)
app = FastAPI(
    title="Video Downloader API",
    description="Backend service for fetching and downloading video/audio info with cookie support.",
    version="1.3.1",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ... (Helper functions like remove_file_later, run_subprocess, ffprobe_has_audio,
# ffprobe_stream_info, and transcode_to_compatible_mp4 are unchanged and omitted for brevity) ...

# --- CORE LOGIC WITH FIXES ---

def get_ydl_options(cookie_file: str) -> dict:
    """Central function to get base yt-dlp options, including cookies and bot mitigation."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "noplaylist": True,
        "geo_bypass": True,
        "force_ipv4": True, # ADDED: Helps mitigate 'Sign in to confirm you’re not a bot' errors
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    if os.path.exists(cookie_file):
        ydl_opts["cookiefile"] = cookie_file
        logger.info(f"Using cookies from: {cookie_file}")

    return ydl_opts


@app.get("/")
async def root():
    return {"status": "running", "message": "Go to /docs for Swagger UI."}


@app.post("/fetch_info")
async def fetch_info(req: FetchRequest):
    url = req.url.strip()
    try:
        # ... (unchanged logic to extract info and parse formats, including MP3 option) ...
        if not url:
            raise HTTPException(status_code=400, detail="Missing 'url' field")

        logger.info(f"Fetching info for: {url}")

        ydl_opts = get_ydl_options(COOKIES_FILE)

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        audio_formats = {}
        video_formats = []
        duration = info.get("duration") or 0

        # --- Aggregation for Formats ---

        # 1. Gather all audio-only formats
        for f in info.get("formats", []):
            if f.get("vcodec") == "none" and f.get("acodec") != "none":
                abr = float(f.get("abr") or 0)
                if abr <= 0: continue

                quality = "High" if abr >= 170 else ("Medium" if abr >= 90 else "Low")
                filesize = f.get("filesize") or f.get("filesize_approx")
                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                if quality not in audio_formats or abr > audio_formats[quality]["abr"]:
                    audio_formats[quality] = {
                        "format_id": f.get("format_id"),
                        "ext": f.get("ext"),
                        "format_note": f"Audio Only - {quality} ({round(abr)} kbps)",
                        "filesize_mb": filesize_mb,
                        "filesize_text": filesize_str,
                        "vcodec": f.get("vcodec"),
                        "acodec": f.get("acodec"),
                        "abr": round(abr),
                    }

            # 2. Gather all video formats (NOTE: These are typically listed by resolution like 1080p, 720p)
            elif f.get("vcodec") != "none":
                height = f.get("height")
                label = f"{height}p" if height else f.get("format_note") or "Video"
                filesize = f.get("filesize") or f.get("filesize_approx")

                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                video_formats.append({
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "format_note": label,
                    "filesize_mb": filesize_mb,
                    "filesize_text": filesize_str,
                    "width": f.get("width"),
                    "height": height,
                    "vcodec": f.get("vcodec"),
                    "acodec": f.get("acodec"),
                })

        # 3. Add universal MP3 audio option
        if audio_formats:
            best_audio = max(audio_formats.values(), key=lambda x: x['abr'])

            mp3_option = {
                "format_id": best_audio["format_id"],
                "ext": "mp3",
                "format_note": f"Audio Only - MP3 (Conversion)",
                "filesize_mb": best_audio["filesize_mb"],
                "filesize_text": f"~{best_audio['filesize_text']}",
                "vcodec": "none",
                "acodec": "mp3",
                "abr": best_audio["abr"],
            }
            audio_formats["MP3"] = mp3_option


        formats_out = list(audio_formats.values()) + video_formats

        return {
            "id": info.get("id"),
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": duration,
            "uploader": info.get("uploader"),
            "formats": formats_out,
        }

    except Exception as e:
        logger.error(f"[ERROR] fetch_info: {e}", exc_info=True)
        # Re-raise the exception with a clear message
        raise HTTPException(status_code=500, detail=f"yt-dlp extract failed: {e}")

@app.get("/download")
async def download(video_url: str = Query(...),
                   format_id: Optional[str] = Query(None),
                   ext: Optional[str] = Query("mp4"), # New optional extension query
                   background_tasks: BackgroundTasks = None):
    """
    Handles video (default MP4) and audio (MP3 conversion) downloads.
    """
    logger.info(f"Downloading {video_url} | fmt={format_id} | ext={ext}")

    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")
    base_ydl_opts = get_ydl_options(COOKIES_FILE)

    # --- HANDLE PURE MP3 AUDIO DOWNLOAD (TikTok fix) ---
    if ext.lower() == "mp3":
        logger.info("[INFO] Starting MP3 audio extraction...")

        output_path = os.path.join(tmpdir, "audio.mp3")

        audio_ydl_opts = base_ydl_opts.copy()
        audio_ydl_opts.update({
            "skip_download": False,
            "format": format_id or "bestaudio/best",
            "outtmpl": output_path, # ydl_p will replace .mp3 with best ext, then post-process
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            },
                {"key": "FFmpegMetadata"}], # Embed metadata
        })

        try:
            with YoutubeDL(audio_ydl_opts) as ydl:
                ydl.download([video_url])

            # Find the actual output file (yt-dlp may use a temp extension)
            final_files = glob.glob(os.path.join(tmpdir, "*.mp3"))

            if not final_files:
                raise RuntimeError("MP3 conversion failed or no file generated.")

            final_path = final_files[0]

            if background_tasks:
                background_tasks.add_task(shutil.rmtree, tmpdir, ignore_errors=True)

            logger.info(f"[SUCCESS] Serving MP3: {final_path}")
            return FileResponse(final_path, filename=os.path.basename(final_path),
                                media_type="audio/mpeg")

        except Exception as e:
            logger.error("[ERROR] MP3 download/conversion", exc_info=True)
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise HTTPException(status_code=500, detail=f"MP3 download failed: {e}")

    # --- HANDLE VIDEO (MP4) DOWNLOAD AND MERGE (Default behavior) ---

    # Step 1: Probe info
    info = None
    with YoutubeDL(base_ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)

    chosen = next((f for f in info.get("formats", [])
                   if f.get("format_id") == format_id), None)
    has_audio = chosen and chosen.get("acodec") != "none"

    video_path = None
    audio_path = None

    try:
        # --- Video download ---
        video_fmt = format_id or "bestvideo[ext!=mp4]+bestaudio[ext!=mp4]/best" # Use best non-MP4 to force merge/transcode
        video_ydl_opts = base_ydl_opts.copy()
        video_ydl_opts.update({
            "skip_download": False,
            "outtmpl": outtmpl,
            "format": video_fmt
        })

        with YoutubeDL(video_ydl_opts) as ydl:
            ydl.download([video_url])

        files = glob.glob(os.path.join(tmpdir, "*"))
        if not files:
            raise HTTPException(status_code=500, detail="No file downloaded.")
        files.sort(key=os.path.getsize, reverse=True)
        video_path = files[0]

        # --- Separate audio if needed ---
        if not has_audio or not ffprobe_has_audio(video_path):
            logger.info("Fetching best audio stream for merge...")
            audio_out = os.path.join(tmpdir, "audio.m4a")
            try:
                audio_ydl_opts = base_ydl_opts.copy()
                audio_ydl_opts.update({
                    "skip_download": False,
                    "outtmpl": audio_out,
                    "format": "bestaudio/best"
                })
                with YoutubeDL(audio_ydl_opts) as ydl:
                    ydl.download([video_url])
                if os.path.exists(audio_out):
                    audio_path = audio_out
            except Exception as e:
                logger.warning(f"[WARN] bestaudio fetch failed: {e}")

        # --- Step 3: Transcode / merge to final MP4 ---
        output_path = os.path.join(tmpdir, "final.mp4")
        ok = transcode_to_compatible_mp4(video_path, audio_path, output_path)

        # If transcode fails, fallback to the raw downloaded video
        final_path = output_path if ok and os.path.exists(output_path) else video_path

        # cleanup later
        if background_tasks:
            background_tasks.add_task(shutil.rmtree, tmpdir, ignore_errors=True)

        logger.info(f"[SUCCESS] Serving MP4: {final_path}")
        return FileResponse(final_path, filename=os.path.basename(final_path),
                            media_type="video/mp4")

    except Exception as e:
        logger.error("[ERROR] MP4 download/merge", exc_info=True)
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))