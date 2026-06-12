"""
ambient-video-processor / app.py
Render.com web service — fetches Pexels footage, loops it with FFmpeg,
then serves the finished file for n8n to download and upload to YouTube.

Endpoints
---------
POST /process   — queue a new video job
GET  /status/<id> — poll job progress
GET  /download/<id> — stream the finished MP4 (file is deleted after download)
GET  /health    — keep-alive / health check
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid

import requests
from flask import Flask, Response, jsonify, request

app = Flask(__name__)

# ── Config (set these as Render environment variables) ────────────────────────
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
SERVICE_SECRET = os.environ.get("SERVICE_SECRET", "change-me")  # shared secret with n8n
DB_PATH        = "/tmp/jobs.db"

# ── Scene presets ─────────────────────────────────────────────────────────────
# Each scene has a Pexels search query, a default ambient audio URL (royalty-free
# from Pixabay), and pre-written YouTube metadata.
SCENES = {
    "fireplace": {
        "pexels_query": "fireplace burning cozy indoor",
        "audio_url": "https://cdn.pixabay.com/audio/2022/03/09/audio_c1ee5f8f2c.mp3",
        "yt_title":  "Cozy Fireplace — {dur}H Ambience 🔥 | Crackling Fire Sounds",
        "yt_desc": (
            "Sit back and relax to the warm crackle of a cozy fireplace.\n\n"
            "Perfect for studying, reading, sleeping, or working from home.\n\n"
            "🔥 {dur} hours of continuous ambient fire sounds and visuals.\n\n"
            "#fireplace #ambience #relaxing #cozysounds #studymusic"
        ),
        "yt_tags": [
            "fireplace", "ambience", "cozy", "relaxing", "fire sounds",
            "background sounds", "study music", "sleep sounds", "crackling fire",
        ],
    },
    "waterfall": {
        "pexels_query": "waterfall forest nature peaceful stream",
        "audio_url": "https://cdn.pixabay.com/audio/2021/09/06/audio_d6e3b05956.mp3",
        "yt_title":  "Peaceful Waterfall — {dur}H Nature Sounds 💧",
        "yt_desc": (
            "Let the sound of flowing water wash away your stress.\n\n"
            "Perfect for meditation, yoga, sleep, focus, and relaxation.\n\n"
            "💧 {dur} hours of continuous waterfall nature sounds.\n\n"
            "#waterfall #naturesounds #relaxing #meditation #sleepsounds"
        ),
        "yt_tags": [
            "waterfall", "nature sounds", "relaxing", "meditation",
            "sleep sounds", "ambient", "water sounds", "background",
        ],
    },
    "meadow": {
        "pexels_query": "grass meadow field gentle wind breeze sunny",
        "audio_url": "https://cdn.pixabay.com/audio/2022/06/07/audio_0ef1c19f28.mp3",
        "yt_title":  "Gentle Meadow Breeze — {dur}H Nature Ambience 🌿",
        "yt_desc": (
            "Breathe in the calm of a sun-drenched meadow with a soft breeze.\n\n"
            "Perfect for focus, relaxation, background ambience, and ASMR.\n\n"
            "🌿 {dur} hours of gentle nature sounds.\n\n"
            "#meadow #naturesounds #relaxing #ambient #focus"
        ),
        "yt_tags": [
            "meadow", "nature sounds", "wind sounds", "relaxing", "ambient",
            "background", "focus", "breeze", "countryside",
        ],
    },
    "cabin": {
        "pexels_query": "snow falling winter window cabin snowfall",
        "audio_url": "https://cdn.pixabay.com/audio/2022/01/26/audio_e8e52bc4ae.mp3",
        "yt_title":  "Cozy Snowy Cabin — {dur}H Winter Ambience ❄️",
        "yt_desc": (
            "Watch snow fall softly outside a warm, cozy cabin window.\n\n"
            "Perfect for sleeping, relaxing, studying, and winter vibes.\n\n"
            "❄️ {dur} hours of peaceful winter ambience.\n\n"
            "#snow #cabin #winter #ambience #cozysounds #relaxing"
        ),
        "yt_tags": [
            "snow", "cabin", "winter ambience", "cozy", "relaxing",
            "sleep sounds", "background", "snowfall", "winter sounds",
        ],
    },
}

# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id         TEXT PRIMARY KEY,
                status     TEXT DEFAULT 'pending',
                message    TEXT DEFAULT '',
                file_path  TEXT,
                metadata   TEXT,
                created_at REAL
            )
            """
        )
        conn.commit()


def db_set(job_id: str, **kwargs):
    with sqlite3.connect(DB_PATH) as conn:
        pairs = ", ".join(f"{k}=?" for k in kwargs)
        conn.execute(f"UPDATE jobs SET {pairs} WHERE id=?", [*kwargs.values(), job_id])
        conn.commit()


def db_get(job_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute(
            "SELECT status, message, file_path, metadata FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()


init_db()

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_pexels_url(query: str) -> str:
    """Search Pexels and return the best HD/4K landscape video URL."""
    r = requests.get(
        "https://api.pexels.com/videos/search",
        headers={"Authorization": PEXELS_API_KEY},
        params={"query": query, "per_page": 5, "size": "large", "orientation": "landscape"},
        timeout=15,
    )
    r.raise_for_status()
    videos = r.json().get("videos", [])
    if not videos:
        raise ValueError(f'Pexels: no results for "{query}"')

    # Try first 3 results, prefer 4K then 1080p
    for video in videos[:3]:
        files = sorted(
            video["video_files"],
            key=lambda f: f.get("width", 0) * f.get("height", 0),
            reverse=True,
        )
        for f in files:
            if f.get("width", 0) >= 1920:
                return f["link"]

    # Fallback: highest resolution from first result
    files = sorted(videos[0]["video_files"], key=lambda f: f.get("width", 0), reverse=True)
    return files[0]["link"]


def download_to_tmp(url: str, dest: str):
    """Stream a URL to a local temp file."""
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=65_536):
                f.write(chunk)


# ── Encoding job (runs in background thread) ─────────────────────────────────

def run_encoding_job(
    job_id: str,
    video_url: str,
    audio_url: str | None,
    duration_secs: int,
    output_path: str,
):
    video_tmp = output_path.replace(".mp4", "_src.mp4")
    audio_tmp = output_path.replace(".mp4", "_audio.mp3") if audio_url else None

    try:
        # 1 — Download source video locally so FFmpeg can loop it reliably
        db_set(job_id, status="downloading", message="Downloading source video…")
        download_to_tmp(video_url, video_tmp)

        # 2 — Download audio
        if audio_url and audio_tmp:
            db_set(job_id, message="Downloading audio…")
            download_to_tmp(audio_url, audio_tmp)

        # 3 — Encode
        hours = duration_secs // 3600
        db_set(job_id, status="encoding", message=f"Encoding {hours}h video (this takes a while)…")

        cmd = [
            "ffmpeg", "-y",
            "-stream_loop", "-1", "-i", video_tmp,
        ]
        if audio_tmp:
            cmd += ["-stream_loop", "-1", "-i", audio_tmp]

        cmd += ["-t", str(duration_secs)]
        cmd += ["-map", "0:v:0"]
        cmd += ["-map", "1:a:0"] if audio_tmp else ["-map", "0:a:0?"]

        cmd += [
            # Video — scale to exactly 1920×1080 with letterbox if needed
            "-c:v",    "libx264",
            "-preset", "ultrafast",   # fastest encode; quality still good for ambient content
            "-crf",    "28",          # ~1–3 Mbps for static scenes
            "-vf",     (
                "scale=1920:1080:force_original_aspect_ratio=decrease,"
                "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1"
            ),
            "-r",      "30",
            "-pix_fmt","yuv420p",
            # Audio
            "-c:a",    "aac",
            "-b:a",    "192k",
            # Allow fast-start (helps YouTube ingest)
            "-movflags", "+faststart",
            output_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7_200)

        if result.returncode != 0:
            raise RuntimeError(f"FFmpeg error:\n{result.stderr[-800:]}")

        if not os.path.exists(output_path) or os.path.getsize(output_path) < 1_000:
            raise RuntimeError("FFmpeg produced no output file")

        size_mb = os.path.getsize(output_path) / 1_048_576
        db_set(job_id, status="ready", message=f"Done — {size_mb:.0f} MB", file_path=output_path)

    except Exception as exc:
        db_set(job_id, status="error", message=str(exc)[:600])

    finally:
        for tmp in [video_tmp, audio_tmp]:
            if tmp and os.path.exists(tmp):
                try:
                    os.unlink(tmp)
                except OSError:
                    pass


# ── Routes ────────────────────────────────────────────────────────────────────

def authorized() -> bool:
    return request.headers.get("X-Secret") == SERVICE_SECRET


@app.route("/health")
def health():
    return jsonify({"ok": True, "time": time.time()})


@app.route("/process", methods=["POST"])
def process():
    if not authorized():
        return jsonify({"error": "Unauthorized"}), 401

    data           = request.get_json(force=True) or {}
    scene_type     = data.get("scene_type", "fireplace")
    duration_hours = float(data.get("duration_hours", 1))
    video_url      = data.get("video_url")   # optional — fetched from Pexels if omitted
    audio_url      = data.get("audio_url")   # optional — uses scene default if omitted

    scene = SCENES.get(scene_type)
    if not scene:
        return jsonify({"error": f'Unknown scene "{scene_type}". Valid: {list(SCENES)}'}), 400

    try:
        if not video_url:
            video_url = fetch_pexels_url(scene["pexels_query"])
        if not audio_url:
            audio_url = scene["audio_url"]

        job_id      = str(uuid.uuid4())
        output_path = f"/tmp/ambient_{job_id}.mp4"
        dur_secs    = int(duration_hours * 3_600)
        dur_label   = int(duration_hours)

        metadata = {
            "yt_title":       scene["yt_title"].format(dur=dur_label),
            "yt_description": scene["yt_desc"].format(dur=dur_label),
            "yt_tags":        scene["yt_tags"],
            "yt_category_id": "22",   # People & Blogs — change to 10 (Music) if preferred
        }

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO jobs VALUES (?,?,?,?,?,?)",
                (job_id, "pending", "Queued", None, json.dumps(metadata), time.time()),
            )
            conn.commit()

        t = threading.Thread(
            target=run_encoding_job,
            args=(job_id, video_url, audio_url, dur_secs, output_path),
            daemon=True,
        )
        t.start()

        return jsonify({"job_id": job_id, **metadata})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/status/<job_id>")
def status(job_id):
    if not authorized():
        return jsonify({"error": "Unauthorized"}), 401

    row = db_get(job_id)
    if not row:
        return jsonify({"error": "Job not found"}), 404

    job_status, message, file_path, metadata_json = row
    resp: dict = {"status": job_status, "message": message}

    if metadata_json:
        resp["metadata"] = json.loads(metadata_json)

    if job_status == "ready" and file_path and os.path.exists(file_path):
        resp["file_size_mb"]  = round(os.path.getsize(file_path) / 1_048_576, 1)
        resp["download_url"]  = f"/download/{job_id}"

    return jsonify(resp)


@app.route("/download/<job_id>")
def download(job_id):
    if not authorized():
        return jsonify({"error": "Unauthorized"}), 401

    row = db_get(job_id)
    if not row or row[0] != "ready" or not row[2]:
        return jsonify({"error": "File not ready"}), 404

    file_path = row[2]
    if not os.path.exists(file_path):
        return jsonify({"error": "File gone — service restarted. Re-run the job."}), 410

    file_size = os.path.getsize(file_path)

    def stream_file():
        with open(file_path, "rb") as f:
            while chunk := f.read(1_048_576):  # 1 MB chunks
                yield chunk
        # Clean up once the download is complete
        try:
            os.unlink(file_path)
            db_set(job_id, status="downloaded", message="Downloaded and cleaned up")
        except OSError:
            pass

    return Response(
        stream_file(),
        mimetype="video/mp4",
        headers={
            "Content-Length":      str(file_size),
            "Content-Disposition": f'attachment; filename="ambient_{job_id[:8]}.mp4"',
        },
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
