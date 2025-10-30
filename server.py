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
from yt_dlp.utils import DownloadError, ExtractorError
from pydantic import BaseModel
import logging

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration for Cloud/Render ---
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", "/tmp/ydl_downloads")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
logger.info(f"Using DOWNLOAD_ROOT: {DOWNLOAD_ROOT}")

# --- Cookie Configuration for /cookies folder structure ---
COOKIES_DIR = "cookies"
COOKIES_MAP = {
    "youtube": "youtube.txt",
    "instagram": "instagram.txt",
    "facebook": "facebook.txt",
    "tiktok": "tiktok.txt",
}
os.makedirs(COOKIES_DIR, exist_ok=True)


# FastAPI setup
app = FastAPI(
    title="Video Downloader API",
    description="Backend service for fetching and downloading video/audio info with comprehensive cookie handling and format support.",
    version="1.3.4", # Updated to final fixed version
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------
# --- HELPER FUNCTIONS ---
# ----------------------------------------------------------------------

def remove_file_later(path: str):
    """Delete file and its parent folder if empty"""
    try:
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        if os.path.isdir(parent) and not os.listdir(parent) and parent.startswith(DOWNLOAD_ROOT):
            os.rmdir(parent)
    except Exception as e:
        logger.error(f"[CLEANUP ERROR] {e}")


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
        out, _ = run_subprocess(cmd, capture=True)
        return "audio" in out.splitlines()
    except Exception as e:
        logger.error(f"[ffprobe error] {e}")
        return True


def transcode_to_compatible_mp4(video_path: str,
                                audio_path: Optional[str],
                                output_path: str):
    """
    Merge or transcode into an MP4 (H.264 + AAC).
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
        # no audio file: inject silent AAC track
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
        run_subprocess(cmd)
        return True
    except RuntimeError as e:
        logger.error(f"[ffmpeg error] {e}")
        return False


# ----------------------------------------------------------------------
# --- CORE FUNCTIONS with Cookie Handling ---
# ----------------------------------------------------------------------

def get_ydl_options(temp_dir: str) -> str:
    """
    Merge all site-specific cookies into one temporary file and returns
    the path to the temporary merged cookie file.
    """
    temp_cookie_path = os.path.join(temp_dir, "merged_cookies.txt")

    with open(temp_cookie_path, 'w', encoding='utf-8') as outfile:
        outfile.write("# Netscape HTTP Cookie File\n")

    cookie_files_to_use = []

    for filename in COOKIES_MAP.values():
        path = os.path.join(COOKIES_DIR, filename)
        if os.path.exists(path):
            cookie_files_to_use.append(path)

    if cookie_files_to_use:
        for cookie_file in cookie_files_to_use:
            try:
                with open(cookie_file, 'r', encoding='utf-8') as infile:
                    content_lines = [line for line in infile if not line.strip().startswith(('#', 'Netscape')) and line.strip()]
                    if content_lines:
                        with open(temp_cookie_path, 'a', encoding='utf-8') as outfile:
                            outfile.writelines(content_lines)

                logger.info(f"Merged cookies from: {os.path.basename(cookie_file)}")

            except Exception as e:
                logger.warning(f"Failed to read/merge cookie file {cookie_file}: {e}")
    else:
        logger.warning("No cookie files found. Social media downloads requiring login might fail.")

    return temp_cookie_path

class FetchRequest(BaseModel):
    url: str


@app.get("/")
async def root():
    return {"status": "running", "message": "Go to /docs for Swagger UI."}


@app.post("/fetch_info")
async def fetch_info(req: FetchRequest):
    url = req.url.strip()
    if "instagram.com/reels/audio" in url:
        raise HTTPException(
            status_code=400,
            detail="Instagram audio pages are not downloadable. Use the actual reel link instead."
        )

    tmpdir = tempfile.mkdtemp(prefix="ydl_info_", dir="/tmp")
    best_audio_details = None # Initialize outside try for cleanup access

    try:
        if not url:
            raise HTTPException(status_code=400, detail="Missing 'url' field")

        logger.info(f"Fetching info for: {url}")

        cookie_path = get_ydl_options(tmpdir)

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "noplaylist": True,
            "geo_bypass": True,
            "force_ipv4": True,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        if os.path.exists(cookie_path) and os.path.getsize(cookie_path) > 30:
            ydl_opts["cookiefile"] = cookie_path
            logger.info("Using merged cookies for extraction.")

        # --- Extraction ---
        with YoutubeDL(ydl_opts) as ydl:
            # Added a timeout for better resilience against network hangs or site issues
            info = ydl.extract_info(url, download=False, timeout=30)

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

                    details = {
                        "format_id": f.get("format_id"),
                        "ext": f.get("ext"),
                        "format_note": f"Audio Only - {quality} ({round(abr)} kbps)",
                        "format_note_clean": quality.lower(),
                        "filesize": filesize,
                        "filesize_mb": filesize_mb,
                        "filesize_text": filesize_str,
                        "vcodec": f.get("vcodec"),
                        "acodec": f.get("acodec"),
                        "abr": round(abr),
                        "bitrate": round(abr),
                    }
                    audio_formats[quality] = details

                    # Track the overall best audio for MP3 conversion
                    if best_audio_details is None or abr > best_audio_details["abr"]:
                        best_audio_details = details


            # VIDEO FORMATS
            elif f.get("vcodec") != "none":
                height = f.get("height")
                label = f"{height}p" if height else f.get("format_note") or "Video"
                filesize = f.get("filesize") or f.get("filesize_approx")

                if not filesize and duration and f.get("tbr"):
                    filesize = int((f["tbr"] * 1000 / 8) * duration)

                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                is_video_only = f.get("acodec") == "none"
                format_label = f"Video Only ({label})" if is_video_only else f"MP4/WebM ({label})"

                video_formats.append({
                    "format_id": f.get("format_id"),
                    "ext": f.get("ext"),
                    "format_note": format_label,
                    "filesize": filesize,
                    "filesize_mb": filesize_mb,
                    "filesize_text": filesize_str,
                    "width": f.get("width"),
                    "height": height,
                    "vcodec": f.get("vcodec"),
                    "acodec": f.get("acodec"),
                    "is_video_only": is_video_only,
                })

        # Fix 1: Add universal MP3 audio option using the tracked best audio details
        formats_out = []
        if best_audio_details:

            # Estimate MP3 size is slightly smaller
            mp3_filesize_mb = round(best_audio_details["filesize_mb"] * 0.9, 2) if best_audio_details["filesize_mb"] else None

            mp3_option = {
                "format_id": best_audio_details["format_id"],
                "ext": "mp3",
                "format_note": f"Audio Only - MP3 (192 kbps Conversion)",
                "filesize": best_audio_details["filesize"],
                "filesize_mb": mp3_filesize_mb,
                "filesize_text": f"~{mp3_filesize_mb} MB" if mp3_filesize_mb else "N/A",
                "vcodec": "none",
                "acodec": "mp3",
                "abr": best_audio_details["abr"],
                "is_conversion": True
            }
            formats_out.append(mp3_option)

        formats_out.extend(list(audio_formats.values()))
        formats_out.extend(video_formats)

        return {
            "id": info.get("id"),
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": duration,
            "uploader": info.get("uploader"),
            "formats": formats_out,
        }

    except (DownloadError, ExtractorError) as e:
        logger.error(f"[ERROR] fetch_info: {e}", exc_info=True)
        # Check for authentication/rate-limit errors and provide a specific response
        error_str = str(e)
        if "login required" in error_str or "rate-limit" in error_str or "Requested content is not available" in error_str:
            raise HTTPException(status_code=403, detail=f"Authentication Failed for target site. Please ensure your **{COOKIES_DIR}** files contain **valid, up-to-date cookies**.")
        raise HTTPException(status_code=500, detail=f"yt-dlp extract failed: {error_str}")

    except Exception as e:
        logger.error(f"[ERROR] fetch_info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.get("/download")
async def download(video_url: str = Query(...),
                   format_id: Optional[str] = Query(None),
                   background_tasks: BackgroundTasks = None):
    """
    Handles download, merging, and transcoding.
    """
    logger.info(f"[INFO] Downloading {video_url} | fmt={format_id}")
    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    cookie_tmpdir = tempfile.mkdtemp(prefix="cookie_tmp_", dir="/tmp")

    video_path: Optional[str] = None
    audio_path: Optional[str] = None
    final_path: Optional[str] = None
    final_ext = "mp4"

    try:
        # Step 1: Prepare cookie path and ydl options
        cookie_path = get_ydl_options(cookie_tmpdir)

        ydl_opts_base = {
            "quiet": True,
            "no_warnings": True,
            "outtmpl": outtmpl,
            "noplaylist": True,
            "geo_bypass": True,
            "force_ipv4": True,
        }

        if os.path.exists(cookie_path) and os.path.getsize(cookie_path) > 30:
            ydl_opts_base["cookiefile"] = cookie_path
            logger.info("Using merged cookies for download.")

        # Step 2: Probe the selected format
        with YoutubeDL({**ydl_opts_base, "skip_download": True}) as ydl:
            info = ydl.extract_info(video_url, download=False, timeout=30)

        chosen = next((f for f in info.get("formats", [])
                       if f.get("format_id") == format_id), None)

        # Fix 2: Add safety check for 'chosen' to avoid NoneType error (e.g., TikTok failure)
        is_mp3_conversion = False
        if chosen and chosen.get("format_note"):
            is_mp3_conversion = format_id and "mp3" in chosen["format_note"].lower()

        has_audio = chosen and chosen.get("acodec") != "none" and not is_mp3_conversion

        # --- A. MP3 Conversion (Audio Only) ---
        if is_mp3_conversion:
            audio_out_path = os.path.join(tmpdir, "audio.mp3")
            final_ext = "mp3"

            ydl_opts_audio = {
                **ydl_opts_base,
                "outtmpl": audio_out_path,
                "format": format_id,
                "postprocessors": [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }]
            }
            with YoutubeDL(ydl_opts_audio) as ydl:
                ydl.download([video_url])

            final_path = audio_out_path

        # --- B. Video (MP4) Download/Merge ---
        else:
            # Use a robust format string for social media if a specific one fails (e.g. TikTok)
            if "tiktok.com" in video_url.lower() or "instagram.com" in video_url.lower():
                # Force best quality video + audio for maximum compatibility on platforms like TikTok
                video_fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"
            else:
                video_fmt = format_id or "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"

            # If a video-only format ID is requested, request the best audio too for merging
            if not has_audio and format_id:
                video_fmt = f"{format_id}+bestaudio/best"

            ydl_opts_video = {**ydl_opts_base, "outtmpl": outtmpl, "format": video_fmt}

            with YoutubeDL(ydl_opts_video) as ydl:
                ydl.download([video_url])

            files = glob.glob(os.path.join(tmpdir, "*"))
            if not files:
                raise HTTPException(status_code=500, detail="No file downloaded.")

            files.sort(key=os.path.getsize, reverse=True)
            video_path = files[0]

            # If the downloaded file is a video and doesn't have an audio stream,
            # we need to explicitly download the audio.
            if not ffprobe_has_audio(video_path):
                logger.info("[INFO] Downloaded video stream is audio-less. Forcing best audio download.")
                audio_out = os.path.join(tmpdir, "audio.m4a")
                try:
                    with YoutubeDL({**ydl_opts_base, "outtmpl": audio_out, "format": "bestaudio"}) as ydl:
                        ydl.download([video_url])
                    if os.path.exists(audio_out):
                        audio_path = audio_out
                except Exception as e:
                    logger.warning(f"[WARN] bestaudio fetch failed: {e}")

            # ii. Transcode / merge to final MP4
            output_path = os.path.join(tmpdir, "final.mp4")

            if audio_path or not video_path.lower().endswith((".mp4", ".mov", ".m4v")):
                ok = transcode_to_compatible_mp4(video_path, audio_path, output_path)
                final_path = output_path if ok and os.path.exists(output_path) else video_path
            else:
                final_path = video_path

        # Step 3: Cleanup and serve

        if not final_path or not os.path.exists(final_path):
            raise HTTPException(status_code=500, detail="Final file path not found after download/transcode.")

        if background_tasks:
            background_tasks.add_task(shutil.rmtree, tmpdir, ignore_errors=True)
            background_tasks.add_task(shutil.rmtree, cookie_tmpdir, ignore_errors=True)

        filename = os.path.basename(final_path)
        logger.info(f"[SUCCESS] Serving {filename}")

        return FileResponse(final_path, filename=filename, media_type=f"video/{final_ext}" if final_ext == "mp4" else f"audio/{final_ext}")


    except (DownloadError, ExtractorError) as e:
        logger.error(f"[ERROR] download: {e}", exc_info=True)
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(cookie_tmpdir, ignore_errors=True)
        # Check for authentication/rate-limit errors and provide a specific response
        error_str = str(e)
        if "login required" in error_str or "rate-limit" in error_str or "Requested content is not available" in error_str:
            raise HTTPException(status_code=403, detail=f"Authentication Failed for target site. Please ensure your **{COOKIES_DIR}** files contain **valid, up-to-date cookies**.")
        raise HTTPException(status_code=500, detail=f"yt-dlp download failed: {error_str}")

    except Exception as e:
        logger.error(f"[ERROR] download: {e}", exc_info=True)
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(cookie_tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))