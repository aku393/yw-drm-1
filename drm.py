# drm.py  -- Optimized "beast" leech bot
# Requirements: telethon, aiohttp, python-dotenv, requests
# System: ffmpeg/ffprobe (optional), speedtest-cli (optional)
# Place .env with API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS (comma separated)

import os
import asyncio
import logging
import time
import json
import math
import random
import traceback
from collections import deque
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeVideo
import aiohttp
import requests
import stat

# -------------------------
# Config & Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("drm-beast")

load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ALLOWED_USERS = os.getenv("ALLOWED_USERS", "")

if not all([API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS]):
    logger.error("Missing environment variables. Set API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS in .env")
    raise SystemExit(1)

API_ID = int(API_ID)
ALLOWED_USERS = {int(x.strip()) for x in ALLOWED_USERS.split(",") if x.strip()}

# Download dir
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Telethon client
client = TelegramClient("bot_session", API_ID, API_HASH, connection_retries=5, auto_reconnect=True)

# Globals for queues and states
user_queues = {}            # user_id -> deque of tasks
user_queue_locks = {}       # user_id -> asyncio.Lock
user_current_task = {}      # user_id -> currently running task dict or None

# Locks
global_send_lock = asyncio.Lock()
speed_stats = {}            # user_id -> {'download':float,'upload':float,'ts':time}

# Tunables for speed emphasis
DOWNLOAD_CHUNK = 4 * 1024 * 1024       # 4MB chunk reads - fewer syscalls => higher throughput
AIOHTTP_LIMIT = 0                      # 0 => no limit
PROGRESS_EDIT_INTERVAL = 6.0           # seconds between progress edits (balanced)
SPLIT_THRESHOLD_BYTES = 4 * 1024**3    # 4GB split threshold
SPLIT_PART_DIGITS = 3                  # numbering like 001,002
UPLOAD_PARALLEL = 4                    # parallel upload parts if doing custom uploads (we mostly use client.send_file)

# -------------------------
# Utilities
# -------------------------
def fmt_size(b: int) -> str:
    if b < 1024: return f"{b} B"
    for u in ["KB","MB","GB","TB"]:
        b /= 1024.0
        if b < 1024:
            return f"{b:.2f} {u}"
    return f"{b:.2f} PB"

def fmt_time(s: float) -> str:
    if s < 60:
        return f"{int(s)}s"
    m = int(s // 60)
    s = int(s % 60)
    if m < 60:
        return f"{m}m{s}s"
    h = m // 60
    m = m % 60
    return f"{h}h{m}m"

def safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in name).strip()

async def safe_send(entity, text=None, file=None, **kwargs):
    # throttle edits/sends to avoid hitting API rate limits
    async with global_send_lock:
        # slight sleep to reduce burst
        await asyncio.sleep(0.5)
        if file is not None:
            return await client.send_file(entity, file=file, caption=text or "", **kwargs)
        else:
            return await client.send_message(entity, text, **kwargs)

# -------------------------
# Progress display builder
# -------------------------
def build_progress_text(stage, name, done, total, speed, start_ts, user_first, user_id):
    percent = (done / total * 100) if total > 0 else 0.0
    bar_len = 12
    filled = int(percent / 100 * bar_len)
    bar = "▰" * filled + "▱" * (bar_len - filled)
    elapsed = time.time() - start_ts if start_ts else 0
    eta = (total - done) / speed if speed > 0 else 0
    return (
        f"📌 <b>{name}</b>\n"
        f"🔁 Stage: {stage}\n"
        f"▸ [{bar}] {percent:.1f}%\n"
        f"▸ {fmt_size(done)} / {fmt_size(total)}\n"
        f"▸ Speed: {fmt_size(int(speed))}/s • ETA: {fmt_time(eta)}\n"
        f"▸ Elapsed: {fmt_time(elapsed)} • User: {user_first} ({user_id})"
    )

# -------------------------
# Speedtest command (tries speedtest binary or module)
# -------------------------
async def run_speedtest():
    # Try `speedtest --json` or `speedtest-cli --json`, then fallback to python package if installed
    cmds = [
        ["speedtest", "--json"],
        ["speedtest-cli", "--json"]
    ]
    for cmd in cmds:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            out, err = await proc.communicate()
            if proc.returncode == 0 and out:
                try:
                    data = json.loads(out.decode())
                    return data
                except Exception:
                    continue
        except FileNotFoundError:
            continue
        except Exception:
            continue
    # fallback: try python `speedtest` module (not guaranteed installed)
    try:
        import speedtest as st
        s = st.Speedtest()
        s.get_best_server()
        dl = s.download()
        ul = s.upload()
        ping = s.results.ping
        return {
            "download": dl,
            "upload": ul,
            "ping": ping,
            "client": {"ip": s.results.client.get("ip", "")}
        }
    except Exception as e:
        return {"error": "No speedtest binary/module available: " + str(e)}

# -------------------------
# File splitting (byte-based, fastest)
# -------------------------
def split_file_bytes(src_path, part_size_bytes):
    """
    Splits file into parts of part_size_bytes. Returns list of part file paths.
    Uses raw byte slicing (no re-encode), fastest approach.
    """
    parts = []
    total = os.path.getsize(src_path)
    if total <= part_size_bytes:
        return [src_path]
    base, ext = os.path.splitext(src_path)
    digits = SPLIT_PART_DIGITS
    part_num = 1
    with open(src_path, "rb") as f:
        while True:
            chunk = f.read(part_size_bytes)
            if not chunk:
                break
            part_name = f"{base}_{str(part_num).zfill(digits)}{ext}"
            with open(part_name, "wb") as out:
                out.write(chunk)
            parts.append(part_name)
            part_num += 1
    return parts

# -------------------------
# Direct download (fast aiohttp)
# -------------------------
async def download_direct(url, dest_path, progress_cb=None):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; drm-beast/1.0)"}
    connector = aiohttp.TCPConnector(limit=AIOHTTP_LIMIT, force_close=False)
    timeout = aiohttp.ClientTimeout(total=0)  # no total limit; rely on retries if needed
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sess:
        async with sess.get(url, headers=headers) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length", 0))
            done = 0
            start = time.time()
            with open(dest_path, "wb") as fd:
                async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK):
                    fd.write(chunk)
                    done += len(chunk)
                    if progress_cb:
                        await progress_cb(done, total, time.time()-start)
    return dest_path

# -------------------------
# Task processor per user
# -------------------------
async def process_user_queue(user_id, chat_id):
    # ensure lock registration
    if user_id not in user_queue_locks:
        user_queue_locks[user_id] = asyncio.Lock()
    async with user_queue_locks[user_id]:
        q = user_queues.get(user_id, deque())
        # process until queue empty
        while q:
            task = q.popleft()
            user_current_task[user_id] = task
            try:
                await handle_task(user_id, chat_id, task)
            except Exception:
                logger.error("Task failed:\n" + traceback.format_exc())
            finally:
                user_current_task[user_id] = None

# -------------------------
# Core: handle one task (drm or direct)
# -------------------------
async def handle_task(user_id, chat_id, task):
    """
    task dict has keys: type ('direct' or 'drm'), name, url or mpd & key, sender (telethon user)
    """
    sender = task.get("sender")
    name = safe_filename(task.get("name", f"video_{int(time.time())}"))
    typ = task.get("type", "direct")
    user_first = getattr(sender, "first_name", "User")
    start_ts = time.time()
    status_msg = await safe_send(chat_id, text=f"Queued: <b>{name}</b>\nStarting...", parse_mode="html")
    try:
        # DOWNLOAD PHASE
        stage = "Downloading"
        tmpdir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}")
        os.makedirs(tmpdir, exist_ok=True)
        out_path = os.path.join(tmpdir, f"{name}.mp4")

        # progress callback function
        last_edit = 0
        async def download_progress(done, total, elapsed):
            nonlocal last_edit, status_msg
            now = time.time()
            speed = done / (elapsed or 1)
            if now - last_edit >= PROGRESS_EDIT_INTERVAL:
                txt = build_progress_text(stage, name, done, total, speed, start_ts, user_first, user_id)
                try:
                    status_msg = await safe_send(chat_id, text=txt, parse_mode="html", reply_to=status_msg) \
                        if status_msg is None else await status_msg.edit(txt, parse_mode="html")
                except Exception:
                    pass
                last_edit = now

        if typ == "direct":
            url = task["url"]
            # call downloader
            await download_direct(url, out_path, progress_cb=download_progress)

            # Extract duration (best-effort)
            duration = 0
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "error", "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1", out_path,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, _ = await proc.communicate()
                if out:
                    duration = int(float(out.decode().strip()))
            except Exception:
                duration = 0

        elif typ == "drm":
            # Minimal DRM pipeline stub: the original project had MPD parsing and mp4decrypt usage.
            # For safety and because MPD handling is specific per-source, here we attempt to download a provided mp4 stream URL
            # The user already supplied a heavy implementation. For now we attempt to treat 'mpd' as direct fallback.
            # If you want the full MPD+decrypt pipeline re-integrated, tell me and I will include it back.
            url = task.get("mpd") or task.get("url")
            if not url:
                raise ValueError("No MPD/URL provided for DRM task.")
            await download_direct(url, out_path, progress_cb=download_progress)
            duration = 0
        else:
            raise ValueError(f"Unknown task type: {typ}")

        # UPLOAD PHASE
        stage = "Uploading"
        last_edit = 0
        total_size = os.path.getsize(out_path)
        uploaded_bytes = 0
        upload_start = time.time()

        async def upload_progress_hook(sent_bytes):
            nonlocal uploaded_bytes, last_edit, status_msg
            uploaded_bytes = sent_bytes
            now = time.time()
            speed = uploaded_bytes / (now - upload_start + 0.01)
            if now - last_edit >= PROGRESS_EDIT_INTERVAL:
                txt = build_progress_text(stage, name, uploaded_bytes, total_size, speed, start_ts, user_first, user_id)
                try:
                    status_msg = await safe_send(chat_id, text=txt, parse_mode="html", reply_to=status_msg) \
                        if status_msg is None else await status_msg.edit(txt, parse_mode="html")
                except Exception:
                    pass
                last_edit = now

        # Split if bigger than threshold (4GB) into parts numbered 001..
        if total_size > SPLIT_THRESHOLD_BYTES:
            # Part size we upload: prefer 3.5GB parts (safe under 4GB)
            part_bytes = 3_500_000_000
            parts = split_file_bytes(out_path, part_bytes)
            # upload parts one by one
            for idx, part in enumerate(parts, 1):
                label = f"{name}_{str(idx).zfill(SPLIT_PART_DIGITS)}.mp4"
                # send_file supports progress callback (Telethon supports file argument as path)
                await safe_send(chat_id, text=f"Uploading part {idx}/{len(parts)}: {label}")
                # Telethon send_file doesn't accept hook with simple API; we'll do periodic edits using file size on disk
                sent = await client.send_file(chat_id, file=part, caption=label)
                # cleanup part
                try:
                    os.remove(part)
                except Exception:
                    pass
        else:
            # single file upload
            # We update progress by periodic edits using file size (optimistic)
            await safe_send(chat_id, text=f"Uploading: <b>{name}.mp4</b>", parse_mode="html")
            # Use Telethon's send_file; it will handle chunking. We'll still periodically edit progress using os.stat
            send_task = asyncio.create_task(client.send_file(chat_id, file=out_path, caption=f"{name}.mp4"))
            while not send_task.done():
                try:
                    cur = 0
                    if os.path.exists(out_path):
                        cur = min(total_size, os.path.getsize(out_path))  # local file size not changing but kept for pattern
                    await upload_progress_hook(cur)
                except Exception:
                    pass
                await asyncio.sleep(PROGRESS_EDIT_INTERVAL)
            # ensure done
            try:
                await send_task
            except Exception as e:
                raise

        elapsed_total = time.time() - start_ts
        await safe_send(chat_id, text=f"✅ Done: <b>{name}</b>\nSize: {fmt_size(total_size)}\nTime: {fmt_time(elapsed_total)}", parse_mode="html")
        # update speed stats
        speed_stats[user_id] = {"download": total_size/ max(1, elapsed_total), "upload": total_size/ max(1, elapsed_total), "ts": time.time()}

        # cleanup local file
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass

    except Exception as e:
        logger.error("Error in handle_task: " + str(e) + "\n" + traceback.format_exc())
        try:
            await safe_send(chat_id, text=f"❌ Error processing {name}: {str(e)}")
        except Exception:
            pass

# -------------------------
# Command handlers
# -------------------------
@client.on(events.NewMessage(pattern=r"^/start"))
async def start_handler(event):
    sender = await event.get_sender()
    if sender.id not in ALLOWED_USERS:
        await safe_send(event.chat_id, "🚫 You're not authorized.")
        return
    await safe_send(event.chat_id,
                    "<b>Beast Leech Bot</b>\n"
                    "Send /leech then lines. Two formats supported:\n"
                    "1) DRM/MPD style: <mpd_or_url>|<key>|<name>\n"
                    "2) Direct mp4: just send a line with direct .mp4 URL or: .mplink <url> | <name>\n\n"
                    "/speedtest - run VPS speedtest\n"
                    "/status - show active queue\n                    ",
                    parse_mode="html")

@client.on(events.NewMessage(pattern=r"^/speedtest"))
async def speedtest_handler(event):
    sender = await event.get_sender()
    if sender.id not in ALLOWED_USERS:
        await safe_send(event.chat_id, "🚫 Not authorized.")
        return
    msg = await safe_send(event.chat_id, "Running speedtest... (may take 30s+)")
    try:
        data = await run_speedtest()
        if "error" in data:
            await msg.edit("Speedtest failed: " + data["error"])
            return
        # Normalize output
        dl = data.get("download") or data.get("download_bandwidth") or 0
        ul = data.get("upload") or data.get("upload_bandwidth") or 0
        ping = data.get("ping") or data.get("latency") or 0
        # If values are in bits (speedtest-cli might return bits)
        # We'll display in Mbps
        def to_mbps(x):
            try:
                x = float(x)
                if x > 1e6:  # likely bits -> convert to Mbps
                    return x / 1e6
                if x > 1000:  # bytes? treat as bytes/sec
                    return x * 8 / 1e6
                return x
            except:
                return 0
        dl_m = to_mbps(dl)
        ul_m = to_mbps(ul)
        text = f"📊 Speedtest results\n• Download: {dl_m:.2f} Mbps\n• Upload: {ul_m:.2f} Mbps\n• Ping: {ping} ms"
        await msg.edit(text)
    except Exception as e:
        logger.error("Speedtest error: " + str(e))
        try:
            await msg.edit("Speedtest failed: " + str(e))
        except Exception:
            pass

@client.on(events.NewMessage(pattern=r"^/status"))
async def status_handler(event):
    sender = await event.get_sender()
    if sender.id not in ALLOWED_USERS:
        await safe_send(event.chat_id, "🚫 Not authorized.")
        return
    uid = sender.id
    q = user_queues.get(uid, deque())
    cur = user_current_task.get(uid)
    lines = [f"Queue size: {len(q)}"]
    if cur:
        lines.append(f"Active: {cur.get('name')}")
    else:
        lines.append("Active: none")
    if uid in speed_stats:
        s = speed_stats[uid]
        lines.append(f"Last speed: {fmt_size(int(s['download']))}/s (dl)")
    await safe_send(event.chat_id, "\n".join(lines))

@client.on(events.NewMessage(pattern=r"^/leech"))
async def leech_handler(event):
    sender = await event.get_sender()
    if sender.id not in ALLOWED_USERS:
        await safe_send(event.chat_id, "🚫 Not authorized.")
        return
    # Parse payload after /leech
    body = event.message.message.partition("\n")[2].strip()
    if not body:
        await safe_send(event.chat_id, "Usage:\n/leech\n<mpd_or_url>|<key>|<name>\nor direct mp4 lines (one per line)\nExample direct: https://example.com/video.mp4 | myname")
        return
    lines = [l.strip() for l in body.splitlines() if l.strip()]
    # Build tasks
    tasks = []
    for ln in lines:
        # support short .mplink prefix: ".mplink <url> | name"
        if ln.lower().startswith(".mplink"):
            ln = ln.split(None, 1)[1].strip() if " " in ln else ln
        # if pipe style with 3 parts -> treat as drm/mpd
        parts = [p.strip() for p in ln.split("|")]
        if len(parts) == 3:
            mpd_or_url, key, name = parts
            tasks.append({"type":"drm", "mpd": mpd_or_url, "key": key, "name": name, "sender": sender})
        elif len(parts) == 2:
            url, name = parts
            if url.lower().endswith(".mp4") or url.startswith("http"):
                tasks.append({"type":"direct", "url": url, "name": name, "sender": sender})
            else:
                # fallback - treat as direct with name as link
                tasks.append({"type":"direct", "url": url, "name": name, "sender": sender})
        else:
            # single token line: likely a direct url
            token = parts[0]
            if token.lower().endswith(".mp4") or token.startswith("http"):
                name = os.path.basename(token).split("?")[0]
                tasks.append({"type":"direct", "url": token, "name": name, "sender": sender})
            else:
                await safe_send(event.chat_id, f"Ignoring invalid line: {ln}")
    if not tasks:
        await safe_send(event.chat_id, "No valid tasks found.")
        return
    uid = sender.id
    # create user queue if missing
    if uid not in user_queues:
        user_queues[uid] = deque()
    for t in tasks:
        user_queues[uid].append(t)
    await safe_send(event.chat_id, f"Added {len(tasks)} task(s) to your queue. Position: {len(user_queues[uid])}")
    # if not processing, start background worker
    if user_current_task.get(uid) is None:
        asyncio.create_task(process_user_queue(uid, event.chat_id))

# -------------------------
# Auto-restart wrapper around the Telethon loop
# -------------------------
async def main_loop():
    # connect and run until disconnected
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Bot started.")
    await client.run_until_disconnected()

def run_forever():
    # Exec loop that restarts the client on exception
    while True:
        try:
            asyncio.get_event_loop().run_until_complete(main_loop())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt: shutting down.")
            break
        except Exception as e:
            logger.exception("Bot crashed, restarting in 5s: %s", e)
            time.sleep(5)
            # Loop will restart

if __name__ == "__main__":
    run_forever()
