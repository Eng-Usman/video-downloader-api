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

# -------------------------------------------------------------
# FASTAPI CONFIG
# -------------------------------------------------------------
app = FastAPI(
    title="Video Downloader API",
    description="Backend for downloading video/audio with yt-dlp + ffmpeg",
    version="2.0.0",
)

# -------------------------------------------------------------
# CORS SETTINGS
# -------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------------------------------------
# FILE PATHS
# -------------------------------------------------------------
DOWNLOAD_ROOT = os.path.abspath("ydl_downloads")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)


# -------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------
def remove_file_later(path: str):
    """Delete file and its parent folder if empty."""
    try:
        if os.path.exists(path):
            os.remove(path)
        parent = os.path.dirname(path)
        if os.path.isdir(parent) and not os.listdir(parent):
            os.rmdir(parent)
    except Exception as e:
        print(f"[CLEANUP ERROR] {e}")


def run_subprocess(cmd: list, check: bool = True, capture: bool = False):
    """Run subprocess with safe error handling."""
    try:
        if capture:
            completed = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check)
            return (
                completed.stdout.decode("utf-8", errors="ignore"),
                completed.stderr.decode("utf-8", errors="ignore"),
            )
        else:
            subprocess.run(cmd, check=check)
            return None, None
    except subprocess.CalledProcessError as e:
        out = e.stdout.decode("utf-8", errors="ignore") if e.stdout else ""
        err = e.stderr.decode("utf-8", errors="ignore") if e.stderr else str(e)
        raise RuntimeError(f"Command `{cmd}` failed:\n{err}\n{out}")


def ffprobe_has_audio(path: str) -> bool:
    """Check if a file has an audio stream."""
    cmd = ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type", "-of", "csv=p=0", path]
    try:
        out, _ = run_subprocess(cmd, capture=True)
        return "audio" in out.splitlines()
    except Exception as e:
        print(f"[ffprobe error] {e}")
        return True


def ffprobe_stream_info(path: str) -> dict:
    """Return audio/video stream info as JSON."""
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


def transcode_to_compatible_mp4(video_path: str, audio_path: Optional[str], output_path: str):
    """Merge or transcode to MP4 (H.264 + AAC)."""
    cmd = ["ffmpeg", "-y"]

    if audio_path and os.path.exists(audio_path):
        cmd += [
            "-i", video_path,
            "-i", audio_path,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-map", "0:v:0", "-map", "1:a:0",
            "-shortest", "-movflags", "+faststart", output_path,
        ]
    else:
        cmd += [
            "-i", video_path,
            "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
            "-shortest",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-map", "0:v:0", "-map", "1:a:0",
            "-movflags", "+faststart", output_path,
        ]

    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print("[ffmpeg error]", e.stderr.decode(errors="ignore"))
        return False


# -------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------
class FetchRequest(BaseModel):
    url: str


@app.get("/")
async def root():
    return {"status": "running", "message": "Visit /docs for Swagger UI"}


@app.post("/fetch_info")
async def fetch_info(req: FetchRequest):
    """Fetch video metadata using yt-dlp."""
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="Missing 'url' field")

    try:
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

        # Save info.json for record/debug
        json_path = os.path.join(DOWNLOAD_ROOT, f"{info.get('id', 'video')}_info.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(info, f, indent=2, ensure_ascii=False)

        formats = []
        for f in info.get("formats", []):
            if not f.get("vcodec") and not f.get("acodec"):
                continue
            formats.append({
                "format_id": f.get("format_id"),
                "ext": f.get("ext"),
                "resolution": f.get("format_note") or f.get("height"),
                "filesize": f.get("filesize") or f.get("filesize_approx"),
                "vcodec": f.get("vcodec"),
                "acodec": f.get("acodec"),
            })

        return {
            "id": info.get("id"),
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "uploader": info.get("uploader"),
            "formats": formats,
            "info_json": json_path,
        }

    except Exception as e:
        print(f"[ERROR] fetch_info: {e}")
        raise HTTPException(status_code=500, detail=f"yt-dlp extract failed: {e}")


@app.get("/download")
async def download(video_url: str = Query(...),
                   format_id: Optional[str] = Query(None),
                   background_tasks: BackgroundTasks = None):
    """Download video + ensure playable MP4 with audio."""
    print(f"[INFO] Downloading {video_url} (format={format_id})")

    tmpdir = tempfile.mkdtemp(prefix="ydl_", dir=DOWNLOAD_ROOT)
    outtmpl = os.path.join(tmpdir, "%(title).200s.%(ext)s")

    try:
        with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(video_url, download=False)

        chosen = next((f for f in info.get("formats", []) if f.get("format_id") == format_id), None)
        has_audio = chosen and chosen.get("acodec") != "none"

        # --- Download ---
        video_fmt = format_id or "bestvideo+bestaudio/best"
        with YoutubeDL({"quiet": True, "no_warnings": True, "outtmpl": outtmpl, "format": video_fmt}) as ydl:
            ydl.download([video_url])

        files = glob.glob(os.path.join(tmpdir, "*"))
        if not files:
            raise HTTPException(status_code=500, detail="No file downloaded.")
        files.sort(key=os.path.getsize, reverse=True)
        video_path = files[0]

        audio_path = None
        if not has_audio or not ffprobe_has_audio(video_path):
            audio_out = os.path.join(tmpdir, "audio.m4a")
            try:
                with YoutubeDL({"quiet": True, "no_warnings": True, "outtmpl": audio_out, "format": "bestaudio"}) as ydl:
                    ydl.download([video_url])
                if os.path.exists(audio_out):
                    audio_path = audio_out
            except Exception as e:
                print("[WARN] audio fetch failed:", e)

        output_path = os.path.join(tmpdir, "final.mp4")
        ok = transcode_to_compatible_mp4(video_path, audio_path, output_path)
        final_path = output_path if ok and os.path.exists(output_path) else video_path

        if background_tasks:
            background_tasks.add_task(remove_file_later, final_path)

        print(f"[SUCCESS] Serving {final_path}")
        return FileResponse(final_path, filename=os.path.basename(final_path), media_type="video/mp4")

    except Exception as e:
        print("[ERROR] download", e)
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=str(e))
