
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

# FastAPI setup
app = FastAPI(
    title="Video Downloader API",
    description="Backend service for fetching and downloading video/audio info (transcode for compatibility)",
    version="1.1.0",
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Download folder
DOWNLOAD_ROOT = os.path.abspath("ydl_downloads")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)


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

    try:
        if not url:
            raise HTTPException(status_code=400, detail="Missing 'url' field")

        print(f"[INFO] Fetching info for: {url}")

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
        }

    except Exception as e:
        print(f"[ERROR] fetch_info: {e}")
        raise HTTPException(status_code=500, detail=f"yt-dlp extract failed: {e}")


@app.get("/download")
async def download(video_url: str = Query(...),
                   format_id: Optional[str] = Query(None),
                   background_tasks: BackgroundTasks = None):
    """
    Always produces an MP4 that *has* audio.
    For FB/IG: fetch bestvideo + bestaudio if the chosen format is mute.
    """
    print(f"[INFO] Downloading {video_url} | fmt={format_id}")
    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    # Step 1: probe formats to see if selected one has audio
    info = None
    with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
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
        with YoutubeDL({
            "quiet": True, "no_warnings": True,
            "outtmpl": outtmpl, "format": video_fmt
        }) as ydl:
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
                with YoutubeDL({
                    "quiet": True, "no_warnings": True,
                    "outtmpl": audio_out, "format": "bestaudio"
                }) as ydl:
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

        print(f"[SUCCESS] Serving {final_path}")
        return FileResponse(final_path, filename=os.path.basename(final_path),
                            media_type="video/mp4")

    except Exception as e:
        print("[ERROR] download", e)
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))
