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

# --- Logging Configuration ---
# Use logging instead of print for better production visibility
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration for Cloud/Render ---
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_ROOT", "/tmp/ydl_downloads")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
logger.info(f"Using DOWNLOAD_ROOT: {DOWNLOAD_ROOT}")

# --- NEW: Cookie Configuration for /cookies folder structure ---
COOKIES_DIR = "cookies"
COOKIES_MAP = {
    "youtube": "youtube.txt",
    "instagram": "instagram.txt",
    "facebook": "facebook.txt",
    "tiktok": "tiktok.txt",
}
# Create the cookies directory if it doesn't exist (for local testing/deployment)
os.makedirs(COOKIES_DIR, exist_ok=True)


# FastAPI setup
app = FastAPI(
    title="Video Downloader API",
    description="Backend service for fetching and downloading video/audio info with comprehensive cookie handling and format support.",
    version="1.3.2", # Updated version number
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
# --- HELPER FUNCTIONS (unchanged logic from your provided code) ---
# ----------------------------------------------------------------------

def remove_file_later(path: str):
    """Delete file and its parent folder if empty"""
    try:
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        # remove parent tmp folder if empty, ensure it's within DOWNLOAD_ROOT
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
        out, _ = run_subprocess(cmd, cmd, capture=True)
        # output lines may contain "audio" and "video"
        return "audio" in out.splitlines()
    except Exception as e:
        logger.error(f"[ffprobe error] {e}")
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
        logger.error(f"[ffprobe info error] {e}")
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
        run_subprocess(cmd)
        return True
    except RuntimeError as e:
        logger.error(f"[ffmpeg error] {e}")
        return False


# ----------------------------------------------------------------------
# --- NEW/MODIFIED CORE FUNCTIONS ---
# ----------------------------------------------------------------------

def get_ydl_options(temp_dir: str) -> str:
    """
    Merge all site-specific cookies into one temporary file and returns
    the base yt-dlp options dictionary with bot-mitigation flags.
    Returns the path to the temporary merged cookie file.
    """
    temp_cookie_path = os.path.join(temp_dir, "merged_cookies.txt")

    # 1. Initialize the temporary file with a Netscape header
    with open(temp_cookie_path, 'w', encoding='utf-8') as outfile:
        outfile.write("# Netscape HTTP Cookie File\n")

    cookie_files_to_use = []

    # 2. Collect all existing cookie files
    for filename in COOKIES_MAP.values():
        path = os.path.join(COOKIES_DIR, filename)
        if os.path.exists(path):
            cookie_files_to_use.append(path)

    # 3. Append content of each cookie file to the temporary merged file
    if cookie_files_to_use:
        for cookie_file in cookie_files_to_use:
            try:
                with open(cookie_file, 'r', encoding='utf-8') as infile:
                    # Read content, skipping header/comments, and append to the merged file
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

    # CRITICAL: Create a temporary directory for the merged cookie file
    tmpdir = tempfile.mkdtemp(prefix="ydl_info_", dir="/tmp")

    try:
        if not url:
            raise HTTPException(status_code=400, detail="Missing 'url' field")

        logger.info(f"Fetching info for: {url}")

        # Get merged cookie path
        cookie_path = get_ydl_options(tmpdir)

        # Configure yt-dlp options
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "noplaylist": True,
            "geo_bypass": True,
            "force_ipv4": True, # ADDED: Bot mitigation flag
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        # Add cookiefile if the merged file exists and is not tiny
        if os.path.exists(cookie_path) and os.path.getsize(cookie_path) > 30:
            ydl_opts["cookiefile"] = cookie_path
            logger.info("Using merged cookies for extraction.")

        # --- Extraction ---
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        audio_formats = {}
        video_formats = []
        duration = info.get("duration") or 0

        for f in info.get("formats", []):
            if not f.get("acodec") and not f.get("vcodec"):
                continue

            # AUDIO ONLY (MP3 is added as a universal conversion option later)
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

                # Keep the highest bitrate for each category
                if quality not in audio_formats or abr > audio_formats[quality]["abr"]:
                    audio_formats[quality] = {
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

            # VIDEO FORMATS (These are your MP4 video qualities, listed by resolution)
            elif f.get("vcodec") != "none":
                height = f.get("height")
                label = f"{height}p" if height else f.get("format_note") or "Video"
                filesize = f.get("filesize") or f.get("filesize_approx")

                if not filesize and duration and f.get("tbr"):
                    filesize = int((f["tbr"] * 1000 / 8) * duration)

                filesize_mb = round(filesize / (1024 * 1024), 2) if filesize else None
                filesize_str = f"{filesize_mb} MB" if filesize_mb else "N/A"

                # Check for format type to separate video only (DASH) from combined formats
                is_video_only = f.get("acodec") == "none"
                format_label = f"Video Only ({label})" if is_video_only else f"Video + Audio ({label})"


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

        # Add universal MP3 audio option (using the best available audio stream)
        if audio_formats:
            best_audio = max(audio_formats.values(), key=lambda x: x['abr'])

            mp3_option = {
                "format_id": best_audio["format_id"],
                "ext": "mp3",
                "format_note": f"Audio Only - MP3 (Conversion)",
                "filesize": best_audio["filesize"],
                "filesize_mb": best_audio["filesize_mb"],
                "filesize_text": f"~{best_audio['filesize_text']}",
                "vcodec": "none",
                "acodec": "mp3",
                "abr": best_audio["abr"],
                "is_conversion": True
            }
            # Put MP3 in the front of audio formats list
            formats_out = [mp3_option] + list(audio_formats.values()) + video_formats
        else:
            formats_out = video_formats


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
    finally:
        # CRITICAL: Clean up the temporary folder (and the merged cookie file inside it)
        shutil.rmtree(tmpdir, ignore_errors=True)


@app.get("/download")
async def download(video_url: str = Query(...),
                   format_id: Optional[str] = Query(None),
                   background_tasks: BackgroundTasks = None):
    """
    Always produces an MP4 that *has* audio.
    Uses cookies for authentication.
    """
    logger.info(f"[INFO] Downloading {video_url} | fmt={format_id}")
    # Use the configured DOWNLOAD_ROOT
    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    # CRITICAL: Create a temporary directory for the merged cookie file
    cookie_tmpdir = tempfile.mkdtemp(prefix="cookie_tmp_", dir="/tmp")

    try:
        # Step 1: Prepare cookie path and ydl options
        cookie_path = get_ydl_options(cookie_tmpdir)

        # Base YDL Options for download
        ydl_opts_base = {
            "quiet": True,
            "no_warnings": True,
            "outtmpl": outtmpl,
            "noplaylist": True,
            "geo_bypass": True,
            "force_ipv4": True,
        }

        # Add cookiefile if the merged file exists
        if os.path.exists(cookie_path) and os.path.getsize(cookie_path) > 30:
            ydl_opts_base["cookiefile"] = cookie_path
            logger.info("Using merged cookies for download.")

        # Step 2: Probe the selected format (or default to bestvideo+bestaudio)
        probe_fmt = format_id if format_id and "mp3" not in format_id else "bestvideo+bestaudio/best"

        info = None
        with YoutubeDL({"quiet": True, "no_warnings": True, **ydl_opts_base, "skip_download": True}) as ydl:
            # Need to re-extract info to check audio availability if format_id is provided
            info = ydl.extract_info(video_url, download=False)

        chosen = next((f for f in info.get("formats", [])
                       if f.get("format_id") == format_id), None)

        is_mp3_conversion = format_id and "mp3" in chosen.get("format_note", "").lower() if chosen else False

        # Determine if the chosen video stream is audio-less (DASH format)
        has_audio = chosen and chosen.get("acodec") != "none" and not is_mp3_conversion

        video_path = None
        audio_path = None
        final_ext = "mp4"

        # --- A. MP3 Conversion (Audio Only) ---
        if is_mp3_conversion:
            audio_out_path = os.path.join(tmpdir, "audio.mp3")
            final_ext = "mp3"

            # The format_id passed is the best audio stream ID
            ydl_opts_audio = {
                **ydl_opts_base,
                "outtmpl": audio_out_path,
                "format": format_id,
                "postprocessors": [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192', # High quality MP3
                }]
            }
            with YoutubeDL(ydl_opts_audio) as ydl:
                ydl.download([video_url])

            final_path = audio_out_path

        # --- B. Video (MP4) Download/Merge ---
        else:
            # i. Download the video stream(s)
            video_fmt = format_id or "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"

            # If the format is DASH (video-only), we need to tell yt-dlp to merge
            # The logic here is tricky, safest is often to request the final merged format if possible
            if not has_audio and format_id:
                # If a video-only format ID is requested, we need to request the best audio too,
                # or yt-dlp might fail to merge.
                video_fmt = f"{format_id}+bestaudio/best"


            ydl_opts_video = {**ydl_opts_base, "outtmpl": outtmpl, "format": video_fmt}

            with YoutubeDL(ydl_opts_video) as ydl:
                ydl.download([video_url])

            files = glob.glob(os.path.join(tmpdir, "*"))
            if not files:
                raise HTTPException(status_code=500, detail="No file downloaded.")

            # Find the main downloaded file (usually the largest)
            files.sort(key=os.path.getsize, reverse=True)
            video_path = files[0]

            # If the downloaded file is a video and doesn't have an audio stream,
            # we need to ensure the merge/transcode handles it.
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

            # Only transcode if we have a separate audio track or if the video is webm/mkv (not compatible mp4)
            if audio_path or not video_path.lower().endswith(".mp4"):
                ok = transcode_to_compatible_mp4(video_path, audio_path, output_path)
                final_path = output_path if ok and os.path.exists(output_path) else video_path
            else:
                # If it's already an MP4 with audio, just use it
                final_path = video_path

        # Step 3: Cleanup and serve

        # Add tasks to clean up both the download folder and the temporary cookie folder
        if background_tasks:
            background_tasks.add_task(shutil.rmtree, tmpdir, ignore_errors=True)
            background_tasks.add_task(shutil.rmtree, cookie_tmpdir, ignore_errors=True)

        filename = os.path.basename(final_path)
        logger.info(f"[SUCCESS] Serving {filename}")

        return FileResponse(final_path, filename=filename, media_type=f"video/{final_ext}" if final_ext == "mp4" else f"audio/{final_ext}")


    except Exception as e:
        logger.error(f"[ERROR] download: {e}", exc_info=True)
        # Ensure cleanup on error
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(cookie_tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))