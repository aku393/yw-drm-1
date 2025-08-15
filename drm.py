# Fix JSON loading initialization
import os
import xml.etree.ElementTree as ET
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo, InputFileBig
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.functions.messages import UploadMediaRequest
from telethon.tl.types import InputMediaUploadedDocument
import asyncio
import aiohttp
from aiohttp import web
import logging
import time
import base64
from dotenv import load_dotenv
import subprocess
import traceback
import zipfile
import requests
import stat  # For setting file permissions
import inspect  # For debugging class methods
import random  # For generating unique file IDs
import mmap  # For zero-copy file part reads to speed up uploads
import re
import sys
from urllib.parse import urlparse, unquote
import psutil
from telethon.errors.rpcerrorlist import FloodWaitError  # Import FloodWaitError
from collections import deque  # For task queue
import json

# Set up simple logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

print("Bot starting...")

# Load .env file
load_dotenv()

# Config from .env
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
SESSION_STRING = os.getenv('SESSION_STRING') or os.getenv('STRING_SESSION')  # Check both names
ALLOWED_USERS = os.getenv('ALLOWED_USERS', '')
LOG_CHANNEL_ID = -1002552440579  # Log channel ID

if not all([API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS]):
    logging.error("Missing env vars: Set API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS in .env")
    raise ValueError("Missing env vars: Set API_ID, API_HASH, BOT_TOKEN, ALLOWED_USERS in .env")

try:
    API_ID = int(API_ID)
except ValueError:
    logging.error(f"Invalid API_ID: {API_ID}. Must be a valid integer.")
    raise ValueError(f"Invalid API_ID: {API_ID}. Must be a valid integer.")

try:
    ALLOWED_USERS = [int(uid.strip()) for uid in ALLOWED_USERS.split(',') if uid.strip()]
except ValueError as e:
    logging.error(f"Invalid ALLOWED_USERS format: {ALLOWED_USERS}. Error: {e}")
    raise ValueError(f"Invalid ALLOWED_USERS format: {ALLOWED_USERS}. Must be comma-separated integers.")

# Set DOWNLOAD_DIR based on environment
if os.getenv('RENDER') == 'true':
    DOWNLOAD_DIR = '/app/downloads'
else:
    DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', 'downloads')

# Path to mp4decrypt will be set after downloading Bento4 SDK
MP4DECRYPT_PATH = os.path.join(os.getcwd(), 'Bento4-SDK', 'bin', 'mp4decrypt')

# Global locks and task queue - now per user
global_download_lock = asyncio.Lock()
message_rate_limit_lock = asyncio.Lock()  # Lock to throttle message sends

# User-specific task queues and processing states
user_task_queues = {}  # Format: {user_id: deque()}
user_processing_states = {}  # Format: {user_id: bool}
user_queue_locks = {}  # Format: {user_id: asyncio.Lock()}
user_active_tasks = {}  # Format: {user_id: asyncio.Task()}
user_bot_instances = {}  # Format: {user_id: MPDLeechBot}

# JSON storage for loadjson/processjson functionality
user_json_data = {}  # Format: {user_id: json_data}
json_lock = asyncio.Lock()  # Lock for JSON data management

# User management storage
authorized_users = set(ALLOWED_USERS)  # Use a set for faster lookups
user_lock = asyncio.Lock() # Lock to manage authorized_users

# Thumbnail storage for users
user_thumbnails = {}  # Format: {user_id: thumbnail_file_path}
thumbnail_lock = asyncio.Lock()  # Lock for thumbnail management

# Speed tracking for users
user_speed_stats = {}  # Format: {user_id: {'download_speed': float, 'upload_speed': float, 'last_updated': timestamp}}
speed_lock = asyncio.Lock()  # Lock for speed tracking

# Bulk JSON processing storage
user_bulk_data = {}  # Format: {user_id: [json_data1, json_data2, ...]}
bulk_lock = asyncio.Lock()  # Lock for bulk processing

# Thumbnail storage for users
user_thumbnails = {}  # Format: {user_id: thumbnail_file_path}
thumbnail_lock = asyncio.Lock()  # Lock for thumbnail management

# Speed tracking for users
user_speed_stats = {}  # Format: {user_id: {'download_speed': float, 'upload_speed': float, 'last_updated': timestamp}}
speed_lock = asyncio.Lock()  # Lock for speed tracking

# Bulk JSON processing storage
user_bulk_data = {}  # Format: {user_id: [json_data1, json_data2, ...]}
bulk_lock = asyncio.Lock()  # Lock for bulk processing

# Thumbnail storage for users
user_thumbnails = {}  # Format: {user_id: thumbnail_file_path}
thumbnail_lock = asyncio.Lock()  # Lock for thumbnail management

# Speed tracking for users
user_speed_stats = {}  # Format: {user_id: {'download_speed': float, 'upload_speed': float, 'last_updated': timestamp}}
speed_lock = asyncio.Lock()  # Lock for speed tracking

# Bulk JSON processing storage
user_bulk_data = {}  # Format: {user_id: [json_data1, json_data2, ...]}
bulk_lock = asyncio.Lock()  # Lock for bulk processing

# Download and extract Bento4 SDK if not present
def setup_bento4():
    try:
        bento4_dir = os.path.join(os.getcwd(), 'Bento4-SDK')
        mp4decrypt_path = os.path.join(bento4_dir, 'bin', 'mp4decrypt')
        if not os.path.isfile(mp4decrypt_path):
            logging.info("Downloading Bento4 SDK for mp4decrypt...")
            # Use a GitHub release URL for reliability
            bento4_urls = [
                "https://github.com/axiomatic-systems/Bento4/releases/download/v1.6.0-641/Bento4-SDK-1.6.0-641-x86_64-unknown-linux.zip",
                "https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-641.x86_64-unknown-linux.zip"  # Fallback URL
            ]
            zip_path = os.path.join(os.getcwd(), 'Bento4-SDK.zip')
            response = None

            # Try each URL until one succeeds
            for url in bento4_urls:
                logging.info(f"Attempting to download from: {url}")
                try:
                    response = requests.get(url, stream=True)
                    if response.status_code == 200:
                        logging.info(f"Successfully accessed URL: {url}")
                        break
                    else:
                        logging.warning(f"Failed to download from {url}: HTTP {response.status_code}")
                except Exception as e:
                    logging.warning(f"Error accessing {url}: {str(e)}")

            if not response or response.status_code != 200:
                raise Exception(f"Failed to download Bento4 SDK: All URLs failed (last status: {response.status_code if response else 'No response'})")

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # Validate the zip file
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.testzip()
                    logging.info("Zip file validation successful")
            except zipfile.BadZipFile:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                raise Exception("Downloaded file is not a valid zip file - check the URL")

            # Extract the zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(os.getcwd())

            # Log the extracted contents
            extracted_files = os.listdir(os.getcwd())
            logging.info(f"Extracted files: {extracted_files}")

            # Find the extracted Bento4 folder (exclude the zip file itself)
            extracted_folders = [f for f in os.listdir() if f.startswith('Bento4-SDK') and not f.endswith('.zip')]
            if not extracted_folders:
                raise Exception("No Bento4-SDK folder found after extraction. Extracted contents: " + str(extracted_files))
            extracted_folder = extracted_folders[0]
            logging.info(f"Found Bento4 folder: {extracted_folder}")

            # Rename the extracted folder to Bento4-SDK
            if extracted_folder != 'Bento4-SDK':
                os.rename(extracted_folder, 'Bento4-SDK')
                logging.info(f"Renamed {extracted_folder} to Bento4-SDK")

            # Verify the bin directory exists
            bin_dir = os.path.join(bento4_dir, 'bin')
            if not os.path.isdir(bin_dir):
                raise FileNotFoundError(f"Bento4-SDK/bin directory not found at {bin_dir}. Directory contents: {os.listdir(bento4_dir)}")

            # Log the contents of the bin directory
            bin_contents = os.listdir(bin_dir)
            logging.info(f"Contents of {bin_dir}: {bin_contents}")

            # Verify mp4decrypt exists
            if not os.path.isfile(mp4decrypt_path):
                raise FileNotFoundError(f"mp4decrypt not found at {mp4decrypt_path} after extraction. Bin directory contents: {bin_contents}")

            # Set executable permissions for mp4decrypt
            os.chmod(mp4decrypt_path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)  # 775 permissions
            logging.info(f"Set executable permissions for {mp4decrypt_path}")

            # Clean up the zip file with better error handling
            try:
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                    logging.info(f"Removed zip file: {zip_path}")
                else:
                    logging.info(f"Zip file {zip_path} already removed or never existed")
            except Exception as e:
                logging.warning(f"Failed to remove zip file {zip_path}: {str(e)}")

            print("Bento4 SDK ready")
    except Exception as e:
        print(f"Bento4 setup failed: {e}")
        raise

# Run setup on startup
try:
    setup_bento4()
except Exception as e:
    print(f"Setup error: {e}")
    raise

# Initialize Telegram clients
# Admin session client for log channel uploads
admin_client = None

# Bot client for user interactions
client = TelegramClient('bot', API_ID, API_HASH, connection_retries=5, auto_reconnect=True)

# Helper function to get user-specific locks and queues
def get_user_resources(user_id):
    if user_id not in user_task_queues:
        user_task_queues[user_id] = deque()
    if user_id not in user_processing_states:
        user_processing_states[user_id] = False
    if user_id not in user_queue_locks:
        user_queue_locks[user_id] = asyncio.Lock()
    if user_id not in user_active_tasks:
        user_active_tasks[user_id] = None
    return user_task_queues[user_id], user_processing_states, user_queue_locks[user_id]

# Enhanced flood wait protection with user-specific tracking
user_last_message_time = {}  # Track last message time per user
user_flood_penalty = {}      # Track flood penalties per user
user_message_cache = {}      # Cache last message content to avoid duplicate edits

# Helper function to handle flood wait errors and throttle message sends
async def send_message_with_flood_control(entity, message, event=None, edit_message=None):
    # Extract user ID for tracking
    user_id = None
    if event and hasattr(event, 'sender_id'):
        user_id = event.sender_id
    elif hasattr(entity, 'id'):
        user_id = entity.id
    elif edit_message and hasattr(edit_message, 'sender_id'):
        user_id = edit_message.sender_id

    max_retries = 3
    retry_count = 0

    async with message_rate_limit_lock:
        while retry_count <= max_retries:
            try:
                # Calculate adaptive delay based on user's flood history
                current_time = time.time()
                base_delay = 1.5  # Base delay of 1.5 seconds

                if user_id:
                    # Add penalty-based delay for users who hit flood limits
                    penalty = user_flood_penalty.get(user_id, 0)
                    adaptive_delay = base_delay + (penalty * 0.5)  # Add 0.5s per penalty point

                    # Ensure minimum time between messages for this user
                    last_msg_time = user_last_message_time.get(user_id, 0)
                    time_since_last = current_time - last_msg_time
                    if time_since_last < adaptive_delay:
                        sleep_time = adaptive_delay - time_since_last
                        await asyncio.sleep(sleep_time)
                else:
                    await asyncio.sleep(base_delay)

                # Check for duplicate message content to avoid unnecessary edits
                if edit_message and user_id:
                    cache_key = f"{user_id}_{id(edit_message)}"
                    if cache_key in user_message_cache and user_message_cache[cache_key] == message:
                        logging.debug(f"Skipping duplicate message edit for user {user_id}")
                        return edit_message
                elif user_id: # For new messages, clear cache for this user if it exists
                    cache_key_clear = f"{user_id}_clear_cache" # Placeholder to clear cache
                    if cache_key_clear in user_message_cache:
                        del user_message_cache[cache_key_clear]


                # Attempt to send/edit the message
                if edit_message:
                    logging.debug(f"Editing message for user {user_id} (attempt {retry_count + 1})")
                    await edit_message.edit(message)
                    if user_id:
                        user_message_cache[f"{user_id}_{id(edit_message)}"] = message
                        user_last_message_time[user_id] = time.time()
                    # Reduce flood penalty on success
                    if user_id in user_flood_penalty and user_flood_penalty[user_id] > 0:
                        user_flood_penalty[user_id] = max(0, user_flood_penalty[user_id] - 0.3)
                    return edit_message
                else:
                    logging.debug(f"Sending new message for user {user_id} (attempt {retry_count + 1})")
                    if event:
                        result = await event.reply(message)
                    else:
                        result = await client.send_message(entity, message)
                    if user_id:
                        user_last_message_time[user_id] = time.time()
                    # Reduce flood penalty on success
                    if user_id in user_flood_penalty and user_flood_penalty[user_id] > 0:
                        user_flood_penalty[user_id] = max(0, user_flood_penalty[user_id] - 0.3)
                    return result

            except FloodWaitError as e:
                # More aggressive FloodWaitError handling
                actual_wait_time = e.seconds
                retry_count += 1

                # More conservative wait times
                if retry_count == 1:
                    wait_time = min(actual_wait_time, 120)  # Max 2 minutes
                elif retry_count == 2:
                    wait_time = min(actual_wait_time, 300)  # Max 5 minutes
                else:
                    wait_time = min(actual_wait_time, 600)  # Max 10 minutes

                # Increase penalty more aggressively
                if user_id:
                    user_flood_penalty[user_id] = user_flood_penalty.get(user_id, 0) + 1.0

                if retry_count <= max_retries:
                    logging.warning(f"FloodWaitError for user {user_id}: Waiting {wait_time}s (attempt {retry_count}/{max_retries + 1}, penalty: {user_flood_penalty.get(user_id, 0):.1f})")
                    await asyncio.sleep(wait_time)

                    # Longer buffer time after flood wait
                    buffer_time = min(10 + (retry_count * 5), 30)  # 10-30 seconds buffer
                    await asyncio.sleep(buffer_time)
                    continue
                else:
                    logging.error(f"FloodWaitError: Max retries exceeded for user {user_id}, giving up")
                    raise

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    logging.warning(f"Message send failed for user {user_id} (attempt {retry_count}/{max_retries + 1}): {str(e)}")
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    continue
                else:
                    logging.error(f"Failed to send/edit message after {max_retries} retries: {str(e)}\n{traceback.format_exc()}")
                    raise

        # This should never be reached due to the raise in the loop
        raise Exception(f"Message send failed after {max_retries} retries")

# Helper function for retry with exponential backoff
async def retry_with_backoff(coroutine, max_retries=3, base_delay=2, operation_name="operation"):
    for attempt in range(max_retries + 1):
        try:
            return await coroutine()
        except Exception as e:
            if attempt == max_retries:
                logging.error(f"{operation_name} failed after {max_retries} retries: {str(e)}\n{traceback.format_exc()}")
                raise
            delay = base_delay * (2 ** attempt)  # Exponential backoff: 2s, 4s, 8s, etc.
            logging.warning(f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}): {str(e)}. Retrying after {delay} seconds...")
            await asyncio.sleep(delay)
    # This line should never be reached due to the raise in the loop, but included for clarity
    raise Exception(f"{operation_name} failed after {max_retries} retries")

def parse_duration(duration_str):
    if duration_str.startswith('PT'):
        time_part = duration_str[2:]
        seconds = 0
        if 'H' in time_part:
            h, time_part = time_part.split('H')
            seconds += int(h) * 3600
        if 'M' in time_part:
            m, time_part = time_part.split('M')
            seconds += int(m) * 60
        if 'S' in time_part:
            s = time_part.replace('S', '')
            seconds += float(s)
        return int(seconds)
    return 0

def format_size(bytes_size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f}{unit}"
        bytes_size /= 1024

def format_time(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = seconds // 60
    seconds = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m{seconds}s"
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours}h{minutes}m{seconds}s"

def derive_name_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        path = unquote(parsed.path or '')
        filename = os.path.basename(path)
        if not filename:
            return "video"
        # Remove extension if present
        base, _ext = os.path.splitext(filename)
        base = base or filename
        # Sanitize
        safe = re.sub(r'[^\w\-_. ]+', '_', base)
        return safe or "video"
    except Exception:
        return "video"

def format_completion_message(completed_tasks, failed_tasks, total_initial_tasks):
    """Format completion message in parts if it exceeds Telegram's limit"""
    messages = []

    # Main summary
    summary_message = f"🎉 **All Tasks Completed!**\n\n"
    summary_message += f"📊 **Summary:**\n"
    summary_message += f"✅ Completed: {len(completed_tasks)}/{total_initial_tasks}\n"

    if failed_tasks:
        summary_message += f"❌ Failed: {len(failed_tasks)}/{total_initial_tasks}\n"

    messages.append(summary_message)

    # Failed tasks (if any)
    if failed_tasks:
        failed_message = f"**❌ Failed Tasks:**\n"
        for name, error in failed_tasks:
            error_short = error[:30] + "..." if len(error) > 30 else error
            task_line = f"• {name}.mp4 - {error_short}\n"

            # Check if adding this line would exceed limit
            if len(failed_message + task_line) > 3500:
                messages.append(failed_message)
                failed_message = f"**❌ Failed Tasks (continued):**\n{task_line}"
            else:
                failed_message += task_line

        if failed_message.strip():
            messages.append(failed_message)

    # Completed tasks
    if completed_tasks:
        completed_message = f"**✅ Completed Tasks:**\n"
        for name in completed_tasks:
            task_line = f"• {name}.mp4\n"

            # Check if adding this line would exceed limit
            if len(completed_message + task_line) > 3500:
                messages.append(completed_message)
                completed_message = f"**✅ Completed Tasks (continued):**\n{task_line}"
            else:
                completed_message += task_line

        if completed_message.strip():
            messages.append(completed_message)

    return messages

async def generate_random_thumbnail(output_path):
    """Generate a random colored thumbnail"""
    try:
        import random
        # Generate random RGB values
        r = random.randint(0, 255)
        g = random.randint(0, 255)
        b = random.randint(0, 255)

        # Create a 320x180 thumbnail with random color using FFmpeg
        cmd = [
            'ffmpeg', '-f', 'lavfi',
            '-i', f'color=c=#{r:02x}{g:02x}{b:02x}:size=320x180:duration=1',
            '-frames:v', '1',
            output_path, '-y'
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logging.info(f"Generated random thumbnail: {output_path}")
            return True
        else:
            logging.error(f"Failed to generate random thumbnail: {stderr.decode()}")
            return False
    except Exception as e:
        logging.error(f"Error generating random thumbnail: {str(e)}")
        return False

async def extract_video_frame_thumbnail(video_path, output_path):
    """Extract a frame from video as thumbnail"""
    try:
        # Extract frame at 5 seconds
        random_time = 5

        # Extract frame at specified time
        cmd = [
            'ffmpeg', '-i', video_path,
            '-ss', str(random_time),
            '-frames:v', '1',
            '-s', '320x180',
            '-q:v', '2',
            output_path, '-y'
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logging.info(f"Extracted video frame thumbnail: {output_path} at {random_time}s")
            return True
        else:
            logging.error(f"Failed to extract video frame: {stderr.decode()}")
            return False
    except Exception as e:
        logging.error(f"Error extracting video frame thumbnail: {str(e)}")
        return False

async def save_thumbnail_from_message(event, user_id):
    """Save thumbnail from user message"""
    try:
        if not event.photo:
            return False, "Please send a photo to use as thumbnail."

        # Create user thumbnail directory
        thumbnail_dir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}", "thumbnails")
        if not os.path.exists(thumbnail_dir):
            os.makedirs(thumbnail_dir)

        # Download the photo
        thumbnail_path = os.path.join(thumbnail_dir, "user_thumbnail.jpg")
        await event.download_media(file=thumbnail_path)

        # Store in user thumbnails
        async with thumbnail_lock:
            user_thumbnails[user_id] = thumbnail_path

        logging.info(f"Saved thumbnail for user {user_id}: {thumbnail_path}")
        return True, f"Thumbnail saved successfully!"

    except Exception as e:
        logging.error(f"Error saving thumbnail: {str(e)}")
        return False, f"Error saving thumbnail: {str(e)}"

def progress_display(stage, percent, done, total, speed, elapsed, user, user_type, filename):
    bar_length = 20
    filled = int((percent / 100) * bar_length)
    spinner = ['⣿', '⣷', '⣯', '⣟', '⡿', '⢿', '⣿'][int(time.time() * 10) % 7]
    progress_bar = '█' * filled + '░' * (bar_length - filled)
    eta = (total - done) / speed if speed > 0 and done < total else 0
    stage_info = {
        "Downloading": ("📥", f"Downloading {percent:.1f}%"),
        "Decrypting": ("🔐", "Decrypting"),
        "Merging": ("🎬", "Merging"),
        "Uploading": ("📤", f"Uploading {percent:.1f}%"),
        "Splitting": ("✂️", "Splitting"),
        "Finalizing": ("🧩", "Finalizing on Telegram"),
        "Completed": ("✅", "Completed"),
        "Initializing": ("🟡", "Initializing"),
    }
    emoji, status_text = stage_info.get(stage, ("🚀", stage))
    
    # Format user type with appropriate emoji
    if user_type == "SESSION":
        user_type_display = "🔑 Session String"
    elif user_type == "PREMIUM":
        user_type_display = "⭐ Premium"
    else:  # FREE
        user_type_display = "🆓 Free"
    
    return (
        f"{spinner} {filename}\n"
        f"{emoji} {status_text}\n"
        f"[{progress_bar}] {percent:.1f}%\n"
        f"⚡ {format_size(speed)}/s | ⏱️ {format_time(elapsed)} | ⌛ {format_time(eta)}\n"
        f"📦 {format_size(done)} / {format_size(total)}\n"
        f"👤 {user} | {user_type_display}"
    )

class MPDLeechBot:
    def __init__(self, user_id):
        self.user_id = user_id
        self.user_download_dir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}")
        self.setup_dirs()
        self.has_notified_split = False  # Flag to prevent duplicate split messages
        self.progress_task = None  # To track the progress task
        self.update_lock = asyncio.Lock()  # Lock to prevent concurrent updates
        self.is_downloading = False  # Flag to prevent overlapping downloads in the same instance
        self.current_filename = None  # Track current file name for /status
        self.abort_event = asyncio.Event()  # Signal to skip/cancel current task
        # Progress tracking state
        self.progress_state = {
            'stage': 'Initializing',
            'percent': 0.0,
            'done_size': 0,
            'total_size': 0,
            'speed': 0,
            'elapsed': 0,
            'start_time': 0
        }
        self._is_json_batch = False # New flag to indicate if this bot instance is processing JSON batch
        logging.info(f"MPDLeechBot initialized for user {user_id}")

    def setup_dirs(self):
        if not os.path.exists(self.user_download_dir):
            os.makedirs(self.user_download_dir)
            logging.info(f"Created directory: {self.user_download_dir}")

    async def download_direct_file(self, event, url, name, sender):
        """Download a direct file from URL"""
        if self.is_downloading:
            logging.info(f"Another download is already in progress for user {self.user_id} - rejecting new request")
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="Another download is already in progress for your account. Please wait.",
                event=event
            )
            return None, None, None, None

        self.is_downloading = True
        status_msg = None  # Initialize status_msg to None
        try:
            output_file = os.path.join(self.user_download_dir, f"{name}.mp4")
            status_msg = await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"Starting direct download for {name}...",
                event=event
            )
            self.progress_state['start_time'] = time.time()
            self.progress_state['stage'] = "Downloading"
            self.progress_state['percent'] = 0.0
            self.progress_state['done_size'] = 0
            self.progress_state['total_size'] = 0
            self.progress_state['speed'] = 0
            self.progress_state['elapsed'] = 0
            # Background progress updater
            last_update_time = 0
            async def update_progress_direct():
                nonlocal status_msg, last_update_time
                UPDATE_INTERVAL = 6.0  # Exactly 6 seconds between updates

                while self.progress_state['stage'] == "Downloading" and not self.abort_event.is_set():
                    current_time = time.time()

                    # Only update every 6 seconds exactly
                    if current_time - last_update_time < UPDATE_INTERVAL:
                        await asyncio.sleep(0.5)
                        continue

                    try:
                        self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                        self.progress_state['speed'] = (self.progress_state['done_size'] / self.progress_state['elapsed']) if self.progress_state['elapsed'] > 0 else 0

                        # Detect user type for progress display
                        user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if await self.detect_premium_status(sender.id) else "FREE")
                        
                        display = progress_display(
                            self.progress_state['stage'],
                            self.progress_state['percent'],
                            self.progress_state['done_size'],
                            self.progress_state['total_size'],
                            self.progress_state['speed'],
                            self.progress_state['elapsed'],
                            sender.first_name,
                            user_type,
                            name + ".mp4"
                        )

                        async with self.update_lock:
                            result = await send_message_with_flood_control(
                                entity=event.chat_id,
                                message=display,
                                edit_message=status_msg
                            )
                            if result is not None:
                                status_msg = result
                                last_update_time = current_time
                                logging.debug(f"Direct download progress updated: {self.progress_state['percent']:.1f}%")

                    except Exception as e:
                        logging.warning(f"Direct download progress update failed: {e}")
                        last_update_time = current_time  # Still update time to prevent spam

                    # Wait exactly 6 seconds before next update
                    await asyncio.sleep(UPDATE_INTERVAL)

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'video/mp4,application/mp4,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Connection': 'keep-alive',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-Dest': 'empty'
            }

            timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=30)
            connector = aiohttp.TCPConnector(
                limit=20,  # Set reasonable limit instead of unlimited
                limit_per_host=10,
                enable_cleanup_closed=True,
                keepalive_timeout=30,
                ttl_dns_cache=300
            )
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                async with session.get(url, headers=headers, allow_redirects=True) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}: {response.reason}")

                    total_size = int(response.headers.get('Content-Length', 0))
                    self.progress_state['total_size'] = total_size
                    downloaded = 0

                    # Start progress updater task
                    progress_task = asyncio.create_task(update_progress_direct())

                    with open(output_file, 'wb', buffering=0) as f:
                        async for chunk in response.content.iter_chunked(4 * 1024 * 1024):  # 4MB chunks
                            if self.abort_event.is_set():
                                raise asyncio.CancelledError()
                            f.write(chunk)
                            downloaded += len(chunk)
                            self.progress_state['done_size'] = downloaded
                            self.progress_state['percent'] = (downloaded / total_size * 100) if total_size > 0 else 0
                            elapsed = time.time() - self.progress_state['start_time']
                            self.progress_state['speed'] = downloaded / elapsed if elapsed > 0 else 0
                            self.progress_state['elapsed'] = elapsed

                    # Stop progress updater
                    progress_task.cancel()
                    try:
                        await progress_task
                    except:
                        pass

            # Get video duration using ffprobe
            try:
                duration_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', output_file]
                process = await asyncio.create_subprocess_exec(*duration_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                duration = int(float(stdout.decode().strip())) if stdout.decode().strip() else 0
            except:
                duration = 0

            final_size = os.path.getsize(output_file)
            logging.info(f"Direct download complete: {output_file}, size: {format_size(final_size)}")

            # Update user speed statistics for direct download
            elapsed = time.time() - self.progress_state['start_time']
            download_speed = final_size / elapsed if elapsed > 0 else 0
            async with speed_lock:
                if self.user_id not in user_speed_stats:
                    user_speed_stats[self.user_id] = {}
                user_speed_stats[self.user_id]['download_speed'] = download_speed
                user_speed_stats[self.user_id]['last_updated'] = time.time()

            self.progress_state['stage'] = "Completed"
            return output_file, status_msg, final_size, duration

        except asyncio.CancelledError:
            logging.info(f"Direct download cancelled for {name}")
            # Cancel progress task on cancellation
            if 'progress_task' in locals() and progress_task and not progress_task.done():
                progress_task.cancel()
                try:
                    await progress_task
                except asyncio.CancelledError:
                    pass
            raise

        except Exception as e:
            logging.error(f"Direct download error for {name}: {str(e)}\n{traceback.format_exc()}")
            error_message = f"Direct download failed for {name}: {str(e)}"
            if status_msg:
                status_msg = await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=error_message,
                    edit_message=status_msg
                )
            else:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=error_message,
                    event=event
                )
            raise
        finally:
            self.is_downloading = False

    async def fetch_segment(self, url, progress, total_segments, range_header=None, output_file=None, session=None):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'video/mp4,application/mp4,*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'identity',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-Dest': 'empty'
        }
        if range_header:
            headers['Range'] = range_header

        # Define the download operation as a coroutine
        async def download_operation():
            logging.info(f"Fetching: {url}, range={range_header}")
            async with session.get(url, headers=headers, allow_redirects=True) as response:
                if response.status == 403:
                    raise Exception(f"403 Forbidden: {url}")
                if response.status not in [200, 206]:
                    raise Exception(f"HTTP {response.status}: {response.reason} for {url}")
                response.raise_for_status()
                total_size = int(response.headers.get('Content-Length', 0))
                downloaded = 0
                with open(output_file, 'wb') as f:
                    async for chunk in response.content.iter_chunked(4 * 1024 * 1024):
                        if self.abort_event.is_set():
                            raise asyncio.CancelledError()
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress['done_size'] += len(chunk)
                        # Update byte-based progress for MPD
                        self.progress_state['done_size'] = progress['done_size']
                logging.info(f"Fetched segment: {url}, size={downloaded} bytes")
                progress['completed'] += 1
                return downloaded

        # Use retry_with_backoff for the download operation
        try:
            return await retry_with_backoff(
                coroutine=download_operation,
                max_retries=3,
                base_delay=2,
                operation_name=f"Download segment {url}"
            )
        except Exception as e:
            raise Exception(f"Fetch failed after retries: {str(e)}")

    async def decrypt_with_mp4decrypt(self, input_file, output_file, kid, key):
        try:
            cmd = [
                MP4DECRYPT_PATH,
                '--key', f"{kid}:{key}",
                input_file,
                output_file
            ]
            logging.info(f"Running mp4decrypt: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                logging.info(f"mp4decrypt succeeded: {output_file}")
                # Verify the output file exists and has content
                if not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
                    raise Exception(f"mp4decrypt output file {output_file} is missing or empty")
                return output_file
            else:
                logging.error(f"mp4decrypt failed: {stderr.decode()}")
                raise Exception(f"mp4decrypt failed: {stderr.decode()}")
        except Exception as e:
            logging.error(f"mp4decrypt error: {str(e)}")
            raise

    async def split_file(self, input_file, max_size_mb=1950, progress_cb=None, cancel_event=None):
        """Split large files into chunks using FFmpeg segment splitting"""
        max_size = max_size_mb * 1024 * 1024  # Convert MB to bytes
        file_size = os.path.getsize(input_file)

        # If file is within size limit, return as-is
        if file_size <= max_size:
            logging.info(f"File {input_file} ({format_size(file_size)}) is within {max_size_mb}MB limit, no splitting needed")
            return [input_file]

        logging.info(f"File {input_file} ({format_size(file_size)}) exceeds {max_size_mb}MB limit, splitting into {max_size_mb}MB parts")

        base_name = os.path.splitext(input_file)[0]
        ext = os.path.splitext(input_file)[1]
        chunks = []

        # Calculate approximate duration per chunk based on file size
        # Get total duration of the video first
        try:
            duration_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', input_file]
            process = await asyncio.create_subprocess_exec(*duration_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            total_duration = float(stdout.decode().strip()) if stdout.decode().strip() else 0
        except:
            total_duration = 0

        if total_duration == 0:
            logging.warning("Could not determine video duration, falling back to simple file splitting using FFmpeg segment")
            # Fallback to simple file size splitting using FFmpeg segment
            segment_size = max_size_mb * 1024 * 1024  # Size in bytes

            ffmpeg_cmd = [
                'ffmpeg', '-i', input_file,
                '-c', 'copy',  # Stream copy to maintain quality
                '-f', 'segment',
                '-segment_size', str(segment_size),
                '-segment_format', 'mp4',
                '-reset_timestamps', '1',
                f"{base_name}_part%03d{ext}",
                '-y'
            ]
        else:
            # Calculate segment duration based on target file size
            avg_bitrate = (file_size * 8) / total_duration  # bits per second
            target_duration = (max_size * 8) / avg_bitrate  # seconds per segment

            # Ensure minimum duration of 10 seconds to avoid too many small segments
            segment_duration = max(target_duration, 10.0)

            logging.info(f"Splitting {format_time(total_duration)} video into segments of ~{format_time(segment_duration)} each")

            ffmpeg_cmd = [
                'ffmpeg', '-i', input_file,
                '-c', 'copy',  # Stream copy to maintain quality
                '-f', 'segment',
                '-segment_time', str(segment_duration),
                '-segment_format', 'mp4',
                '-reset_timestamps', '1',
                f"{base_name}_part%03d{ext}",
                '-y'
            ]

        logging.info(f"Running FFmpeg segment command: {' '.join(ffmpeg_cmd)}")

        try:
            # Update progress callback if provided
            if progress_cb:
                try:
                    await progress_cb(0, 1, 0.0)
                except Exception as e:
                    logging.warning(f"Progress callback error: {e}")

            # Run FFmpeg segmentation
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                # Find all generated segments
                import glob
                segment_pattern = f"{base_name}_part*{ext}"
                generated_files = sorted(glob.glob(segment_pattern))

                # Clean up any temporary .raw files that might have been created
                raw_pattern = f"{base_name}_temp_part*.raw"
                raw_files = glob.glob(raw_pattern)
                for raw_file in raw_files:
                    try:
                        os.remove(raw_file)
                        logging.info(f"Cleaned up temporary file: {raw_file}")
                    except:
                        pass

                if generated_files:
                    # Validate that all generated files are proper video files
                    valid_chunks = []
                    for chunk in generated_files:
                        try:
                            # Quick validation using ffprobe
                            validate_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', chunk]
                            validate_process = await asyncio.create_subprocess_exec(*validate_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                            validate_stdout, validate_stderr = await validate_process.communicate()

                            if validate_process.returncode == 0 and os.path.getsize(chunk) > 1024:  # At least 1KB
                                valid_chunks.append(chunk)
                                logging.info(f"✅ Valid chunk: {os.path.basename(chunk)} ({format_size(os.path.getsize(chunk))})")
                            else:
                                logging.warning(f"⚠️ Invalid chunk detected, removing: {chunk}")
                                try:
                                    os.remove(chunk)
                                except:
                                    pass
                        except Exception as e:
                            logging.warning(f"⚠️ Could not validate chunk {chunk}: {e}")
                            # Keep it anyway, might still be valid
                            valid_chunks.append(chunk)

                    chunks = valid_chunks
                    if chunks:
                        logging.info(f"✅ FFmpeg segmentation successful: {len(chunks)} valid parts created")

                        # Log details of each chunk
                        total_chunks_size = 0
                        oversized_chunks = []
                        for i, chunk in enumerate(chunks, 1):
                            chunk_size = os.path.getsize(chunk)
                            total_chunks_size += chunk_size
                            size_check = "✅" if chunk_size <= max_size * 1.05 else "⚠️"  # 5% tolerance
                            logging.info(f"{size_check} Part {i}/{len(chunks)}: {format_size(chunk_size)} ({os.path.basename(chunk)})")

                            if chunk_size > max_size * 1.05:
                                oversized_chunks.append((i, chunk, chunk_size))

                        if oversized_chunks:
                            logging.warning(f"Found {len(oversized_chunks)} oversized chunks:")
                            for part_num, chunk_path, chunk_size in oversized_chunks:
                                logging.warning(f"Part {part_num}: {format_size(chunk_size)} > {format_size(max_size)}")

                        logging.info(f"✅ Total segments size: {format_size(total_chunks_size)}")
                    else:
                        error_msg = "All generated segments were invalid or corrupted"
                        logging.error(f"❌ FFmpeg segmentation failed: {error_msg}")
                        raise Exception(f"FFmpeg segmentation failed: {error_msg}")
                else:
                    error_msg = stderr.decode() if stderr else "No output files generated"
                    logging.error(f"❌ FFmpeg segmentation failed: {error_msg}")
                    raise Exception(f"FFmpeg segmentation failed: {error_msg}")
            else:
                error_msg = stderr.decode() if stderr else "Unknown FFmpeg error"
                logging.error(f"❌ FFmpeg segmentation failed: {error_msg}")
                raise Exception(f"FFmpeg segmentation failed: {error_msg}")

        except Exception as e:
            logging.error(f"❌ Exception during file splitting: {str(e)}")
            raise Exception(f"File splitting failed: {str(e)}")

        if not chunks:
            raise Exception("Failed to create any valid chunks - check video file integrity and ensure FFmpeg is available")

        return chunks

    async def download_and_decrypt(self, event, mpd_url, key, name, sender):
        if self.is_downloading:
            logging.info(f"Another download is already in progress for user {self.user_id} - rejecting new request")
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="Another download is already in progress for your account. Please wait.",
                event=event
            )
            return None, None, None, None

        # Check available disk space
        try:
            import shutil
            free_space = shutil.disk_usage(self.user_download_dir).free
            if free_space < 500 * 1024 * 1024:  # Less than 500MB free
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="⚠️ Low disk space! Cleaning up old files...",
                    event=event
                )
                # Force cleanup
                self.cleanup(None)
        except Exception as e:
            logging.warning(f"Could not check disk space: {e}")

        self.is_downloading = True
        status_msg = None  # Initialize status_msg to None
        try:
            raw_video_file = os.path.join(self.user_download_dir, f"{name}_raw_video.mp4")
            raw_audio_file = os.path.join(self.user_download_dir, f"{name}_raw_audio.mp4")
            decrypted_video_file = os.path.join(self.user_download_dir, f"{name}_decrypted_video.mp4")
            decrypted_audio_file = os.path.join(self.user_download_dir, f"{name}_decrypted_audio.mp4")
            output_file = os.path.join(self.user_download_dir, f"{name}.mp4")
            status_msg = await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"Fetching MPD playlist for {name}...",
                event=event
            )
            self.progress_state['start_time'] = time.time()
            # Maximum concurrent segment downloads for full speed
            # Unlimited chunk size for maximum data transfer
            # Optimized progress updates for maximum bandwidth

            self.progress_state['stage'] = "Downloading"
            self.progress_state['percent'] = 0.0
            self.progress_state['done_size'] = 0
            self.progress_state['total_size'] = 0
            self.progress_state['speed'] = 0
            self.progress_state['elapsed'] = 0

            # Stage 1: Fetch MPD with retries and updated headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/dash+xml,video/mp4,application/mp4,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Connection': 'keep-alive',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-Dest': 'empty'
            }

            # Configure connection limits to prevent "too many open files"
            timeout = aiohttp.ClientTimeout(total=300, sock_connect=30, sock_read=60)
            connector = aiohttp.TCPConnector(
                limit=20,  # Total connection pool size
                limit_per_host=10,  # Max connections per host
                enable_cleanup_closed=True,
                keepalive_timeout=30,
                ttl_dns_cache=300
            )

            # Define the MPD fetch operation as a coroutine
            async def fetch_mpd_operation():
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    logging.info(f"Fetching MPD: {mpd_url}")
                    async with session.get(mpd_url, headers=headers) as response:
                        if response.status == 403:
                            raise Exception(f"403 Forbidden: {mpd_url}")
                        if response.status not in [200, 206]:
                            raise Exception(f"HTTP {response.status}: {response.reason} for {mpd_url}")
                        response.raise_for_status()
                        return await response.text()

            # Use retry_with_backoff for the MPD fetch
            try:
                mpd_content = await retry_with_backoff(
                    coroutine=fetch_mpd_operation,
                    max_retries=3,
                    base_delay=2,
                    operation_name=f"Fetch MPD {mpd_url}"
                )
            except Exception as e:
                raise Exception(f"Failed to fetch MPD after retries: {str(e)}. The URL may require authentication, specific headers, or may be invalid/expired.")

            logging.info(f"MPD content (full): {mpd_content}")

            root = ET.fromstring(mpd_content)
            namespace = {'ns': 'urn:mpeg:dash:schema:mpd:2011'}
            video_segments = []
            audio_segments = []
            base_url = mpd_url.rsplit('/', 1)[0] + '/'
            duration = parse_duration(root.get('mediaPresentationDuration', 'PT0S'))

            for period in root.findall('.//ns:Period', namespace):
                logging.info(f"Processing Period: {period.get('id', 'no-id')}")
                for adaptation_set in period.findall('.//ns:AdaptationSet', namespace):
                    # Check both contentType and mimeType attributes
                    content_type = adaptation_set.get('contentType', '')
                    mime_type = adaptation_set.get('mimeType', '')

                    # Determine if this is video or audio based on mimeType or contentType
                    if content_type == 'video' or 'video/' in mime_type:
                        segments = video_segments
                        media_type = 'video'
                    elif content_type == 'audio' or 'audio/' in mime_type:
                        segments = audio_segments
                        media_type = 'audio'
                    else:
                        logging.info(f"Skipping AdaptationSet: contentType={content_type}, mimeType={mime_type}")
                        continue

                    logging.info(f"Processing {media_type} AdaptationSet: contentType={content_type}, mimeType={mime_type}")
                    for representation in adaptation_set.findall('.//ns:Representation', namespace):
                        mime = representation.get('mimeType', '')
                        codec = representation.get('codecs', '')
                        logging.info(f"Representation: mime={mime}, codec={codec}")

                        # Check for SegmentTemplate first (common in DASH)
                        segment_template = representation.find('.//ns:SegmentTemplate', namespace)
                        if segment_template is not None:
                            # Handle SegmentTemplate format
                            media_template = segment_template.get('media', '')
                            init_template = segment_template.get('initialization', '')
                            start_number = int(segment_template.get('startNumber', '1'))

                            if media_template and init_template:
                                # Get segment timeline for segment count
                                segment_timeline = segment_template.find('.//ns:SegmentTimeline', namespace)
                                segment_count = 1  # Default

                                if segment_timeline is not None:
                                    s_elements = segment_timeline.findall('.//ns:S', namespace)
                                    segment_count = 0
                                    for s_elem in s_elements:
                                        r_attr = s_elem.get('r', '0')
                                        repeat_count = int(r_attr) if r_attr else 0
                                        segment_count += repeat_count + 1

                                logging.info(f"Found SegmentTemplate for {media_type}: {segment_count} segments")

                                # Add initialization segment
                                init_url = base_url + init_template
                                segments.append((init_url, None))

                                # Add media segments
                                for seg_num in range(start_number, start_number + segment_count):
                                    media_url = base_url + media_template.replace('$Number%09d$', f"{seg_num:09d}").replace('$Number$', str(seg_num))
                                    segments.append((media_url, None))

                                logging.info(f"Added {len(segments)} {media_type} segments from SegmentTemplate")
                                break

                        # Fallback to BaseURL method if no SegmentTemplate
                        base_url_elem = representation.find('.//ns:BaseURL', namespace)
                        if base_url_elem is not None:
                            stream_url = base_url + base_url_elem.text.strip()
                            logging.info(f"Locked {media_type} BaseURL: {stream_url}")
                            segment_base = representation.find('.//ns:SegmentBase', namespace)
                            if segment_base is not None:
                                init = segment_base.find('.//ns:Initialization', namespace)
                                init_range = init.get('range') if init else None
                                logging.info(f"Found {media_type} Initialization range: {init_range}")
                                index_range = segment_base.get('indexRange')
                                if index_range:
                                    segments.append((stream_url, init_range))
                                    segments.append((stream_url, f"bytes={index_range.split('-')[1]}-"))
                                    logging.info(f"{media_type} SegmentBase segments: {segments}")
                            if not segments:
                                segments.append((stream_url, None))
                                logging.info(f"Added full {media_type} URL: {stream_url}")
                            break

            logging.info(f"Final parsing results:")
            logging.info(f"Video segments found: {len(video_segments)}")
            logging.info(f"Audio segments found: {len(audio_segments)}")

            # Log the actual URLs found
            for i, (url, range_header) in enumerate(video_segments):
                logging.info(f"Video segment {i+1}: {url} (range: {range_header})")
            for i, (url, range_header) in enumerate(audio_segments):
                logging.info(f"Audio segment {i+1}: {url} (range: {range_header})")

            if not video_segments:
                status_msg = await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"No video segments found for {name}—check log for MPD content.",
                    edit_message=status_msg
                )
                raise ValueError("No video segments found in MPD—check log for raw content.")
            logging.info(f"Final video segments: {len(video_segments)} - {video_segments}")
            logging.info(f"Final audio segments: {len(audio_segments)} - {audio_segments}")

            kid, key_hex = key.split(':')
            logging.info(f"Using KID: {kid}, KEY: {key_hex}")

            total_segments = len(video_segments) + len(audio_segments)
            progress = {'done_size': 0, 'completed': 0}
            total_size = 0
            max_total_size_est = 0  # To stabilize the total size estimate

            # Try HEAD requests to estimate total sizes for accurate progress
            async def head_size(url, range_header=None):
                try:
                    async with aiohttp.ClientSession() as session:
                        headers_local = headers.copy()
                        headers_local['Accept'] = 'video/mp4,application/mp4,*/*'
                        if range_header:
                            headers_local['Range'] = range_header
                        async with session.head(url, headers=headers_local, allow_redirects=True) as resp:
                            cl = resp.headers.get('Content-Length')
                            return int(cl) if cl else 0
                except Exception:
                    return 0

            est_sizes = await asyncio.gather(*[
                head_size(u, r) for (u, r) in (video_segments + audio_segments)
            ])
            estimated_total_bytes = sum(est_sizes) if any(est_sizes) else 0
            if estimated_total_bytes > 0:
                self.progress_state['total_size'] = estimated_total_bytes

            last_update_time = 0  # For debouncing
            async def update_progress(filename, user, user_id):
                nonlocal max_total_size_est, last_update_time, status_msg
                UPDATE_INTERVAL = 6.0  # Exactly 6 seconds between updates
                last_significant_update = 0
                last_content_hash = None
                consecutive_identical = 0

                while self.progress_state['stage'] != "Completed":
                    try:
                        current_time = time.time()

                        # Only update every 6 seconds exactly
                        if current_time - last_update_time < UPDATE_INTERVAL:
                            await asyncio.sleep(0.5)  # Small sleep to prevent tight loop
                            continue

                        async with self.update_lock:  # Ensure only one update at a time
                            self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                            self.progress_state['speed'] = self.progress_state['done_size'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0

                            if self.progress_state['stage'] == "Downloading":
                                # Prefer byte-accurate progress if we could estimate sizes
                                if estimated_total_bytes > 0:
                                    self.progress_state['percent'] = (self.progress_state['done_size'] / estimated_total_bytes) * 100
                                    self.progress_state['total_size'] = estimated_total_bytes
                                else:
                                    total_size_est = self.progress_state['done_size'] * total_segments / max(progress['completed'], 1)
                                    max_total_size_est = max(max_total_size_est, total_size_est)
                                    self.progress_state['total_size'] = max_total_size_est
                                    self.progress_state['percent'] = progress['completed'] * 100 / total_segments

                            # Detect user type for progress display
                            user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if await self.detect_premium_status(user_id) else "FREE")
                            
                            display = progress_display(
                                self.progress_state['stage'],
                                self.progress_state['percent'],
                                self.progress_state['done_size'],
                                self.progress_state['total_size'],
                                self.progress_state['speed'],
                                self.progress_state['elapsed'],
                                user,
                                user_type,
                                filename
                            )

                            # Create content hash for efficient comparison
                            import hashlib
                            content_hash = hashlib.md5(display.encode()).hexdigest()

                            # Check if content has meaningfully changed
                            if content_hash != last_content_hash:
                                try:
                                    result = await send_message_with_flood_control(
                                        entity=event.chat_id,
                                        message=display,
                                        edit_message=status_msg
                                    )
                                    if result is not None:
                                        status_msg = result
                                        last_update_time = current_time
                                        last_significant_update = current_time
                                        last_content_hash = content_hash
                                        consecutive_identical = 0
                                        logging.info(f"Progress updated for user {user_id}: {self.progress_state['stage']} - {self.progress_state['percent']:.1f}%")
                                    else:
                                        # Failed to update, try again next cycle
                                        await asyncio.sleep(1)
                                        continue

                                except Exception as e:
                                    logging.warning(f"Progress update failed for user {user_id}: {e}")
                                    last_update_time = current_time  # Still update time to prevent spam
                                    await asyncio.sleep(2)
                                    continue
                            else:
                                # Content identical, but still update timestamp
                                consecutive_identical += 1
                                last_update_time = current_time

                                # If content hasn't changed for too long, force an update
                                if consecutive_identical >= 5:  # 30 seconds of identical content
                                    logging.debug(f"Forcing progress update after {consecutive_identical * UPDATE_INTERVAL}s of identical content")
                                    consecutive_identical = 0
                                    last_content_hash = None  # Force next update

                    except Exception as e:
                        logging.error(f"Error in progress update loop for user {user_id}: {e}")
                        await asyncio.sleep(UPDATE_INTERVAL)  # Continue after error
                        continue

                    # Wait exactly 6 seconds before next update
                    await asyncio.sleep(UPDATE_INTERVAL)

                logging.info(f"update_progress task completed for {name}")

            # Cancel any existing progress task with stricter cancellation
            if self.progress_task and not self.progress_task.done():
                logging.info("Cancelling existing progress task")
                self.progress_task.cancel()
                try:
                    await self.progress_task
                except asyncio.CancelledError:
                    logging.info("Existing progress task cancelled successfully")
                except Exception as e:
                    logging.error(f"Failed to cancel existing progress task: {str(e)}")
                finally:
                    self.progress_task = None

            logging.info(f"Starting new update_progress task for {name}")
            self.progress_task = asyncio.create_task(update_progress(name + ".mp4", sender.first_name, sender.id))

            # Stage 2: Download Segments with shared session and concurrency control
            self.progress_state['stage'] = "Downloading"
            self.progress_state['percent'] = 0.0  # Reset percent for download stage
            video_files = [os.path.join(self.user_download_dir, f"{name}_video_seg{i}.mp4") for i in range(len(video_segments))]
            audio_files = [os.path.join(self.user_download_dir, f"{name}_audio_seg{i}.mp4") for i in range(len(audio_segments))]

            # Use shared session and semaphore to control concurrent downloads
            max_concurrent_downloads = 8  # Reduced from unlimited to prevent file descriptor exhaustion
            semaphore = asyncio.Semaphore(max_concurrent_downloads)
            
            async def download_with_semaphore(seg_url, range_header, output_file, session):
                async with semaphore:
                    return await self.fetch_segment(seg_url, progress, total_segments, range_header, output_file, session)

            # Create shared session for all downloads
            timeout = aiohttp.ClientTimeout(total=300, sock_connect=30, sock_read=60)
            connector = aiohttp.TCPConnector(
                limit=20,  # Total connection pool size
                limit_per_host=10,  # Max connections per host
                enable_cleanup_closed=True,
                keepalive_timeout=30,
                ttl_dns_cache=300
            )

            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                tasks = []

                for i, (seg_url, range_header) in enumerate(video_segments):
                    tasks.append(download_with_semaphore(seg_url, range_header, video_files[i], session))
                for i, (seg_url, range_header) in enumerate(audio_segments):
                    tasks.append(download_with_semaphore(seg_url, range_header, audio_files[i], session))

                segment_sizes = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(segment_sizes):
                    if isinstance(result, Exception):
                        raise result

            with open(raw_video_file, 'wb') as outfile:
                for seg_file in video_files:
                    with open(seg_file, 'rb') as infile:
                        outfile.write(infile.read())
                    os.remove(seg_file)
            if audio_segments:
                with open(raw_audio_file, 'wb') as outfile:
                    for seg_file in audio_files:
                        with open(seg_file, 'rb') as infile:
                            outfile.write(infile.read())
                        os.remove(seg_file)

            total_size = os.path.getsize(raw_video_file)
            logging.info(f"Raw video file written: {raw_video_file}, size={total_size}")
            if audio_segments:
                total_size += os.path.getsize(raw_audio_file)
                logging.info(f"Raw audio file written: {raw_audio_file}, size={os.path.getsize(raw_audio_file)}")
            self.progress_state['total_size'] = total_size
            self.progress_state['done_size'] = total_size
            self.progress_state['percent'] = 100.0

            # Stage 3: Decrypt Files
            self.progress_state['stage'] = "Decrypting"
            self.progress_state['percent'] = 0.0  # Reset percent for decryption stage
            await self.decrypt_with_mp4decrypt(raw_video_file, decrypted_video_file, kid, key_hex)
            if audio_segments:
                await self.decrypt_with_mp4decrypt(raw_audio_file, decrypted_audio_file, kid, key_hex)
            self.progress_state['percent'] = 100.0

            # Stage 4: Merge Files
            self.progress_state['stage'] = "Merging"
            self.progress_state['percent'] = 0.0  # Reset percent for merging stage
            total_size = os.path.getsize(decrypted_video_file)
            cmd = ['ffmpeg', '-i', decrypted_video_file]
            if audio_segments:
                cmd.extend(['-i', decrypted_audio_file, '-c', 'copy', '-map', '0:v', '-map', '1:a'])
            else:
                cmd.extend(['-c', 'copy', '-map', '0'])
            cmd.extend([output_file, '-y'])
            logging.info(f"Running FFmpeg: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                logging.info(f"FFmpeg merged MP4: {output_file}")
                final_file = output_file
            else:
                logging.error(f"FFmpeg failed: {stderr.decode()}")
                raise Exception(f"FFmpeg failed: {stderr.decode()}")
            self.progress_state['percent'] = 100.0

            total_size = os.path.getsize(final_file)
            self.progress_state['total_size'] = total_size
            self.progress_state['done_size'] = total_size
            elapsed = time.time() - self.progress_state['start_time']
            self.progress_state['speed'] = total_size / elapsed if elapsed > 0 else 0
            self.progress_state['elapsed'] = elapsed
            logging.info(f"Download complete: {final_file}")

            # Update user speed statistics
            async with speed_lock:
                if self.user_id not in user_speed_stats:
                    user_speed_stats[self.user_id] = {}
                user_speed_stats[self.user_id]['download_speed'] = self.progress_state['speed']
                user_speed_stats[self.user_id]['last_updated'] = time.time()

            # Mark as completed to stop the progress task
            self.progress_state['stage'] = "Completed"
            if self.progress_task and not self.progress_task.done():
                logging.info("Cancelling progress task after download")
                self.progress_task.cancel()
                try:
                    await self.progress_task
                except asyncio.CancelledError:
                    logging.info("Progress task cancelled successfully after download")
                except Exception as e:
                    logging.error(f"Failed to cancel progress task after download: {str(e)}")
                finally:
                    self.progress_task = None

            return final_file, status_msg, total_size, duration

        except asyncio.CancelledError:
            logging.info(f"Download cancelled for {name}")
            # Cancel progress task on cancellation
            if self.progress_task and not self.progress_task.done():
                self.progress_task.cancel()
                try:
                    await self.progress_task
                except asyncio.CancelledError:
                    pass
            raise

        except Exception as e:
            logging.error(f"Download error for {name}: {str(e)}\n{traceback.format_exc()}")
            # Ensure the progress task is cancelled on error
            if self.progress_task and not self.progress_task.done():
                logging.info("Cancelling progress task due to error")
                self.progress_task.cancel()
                try:
                    await self.progress_task
                except asyncio.CancelledError:
                    logging.info("Progress task cancelled successfully due to error")
                except Exception as e:
                    logging.error(f"Failed to cancel progress task due to error: {str(e)}")
                finally:
                    self.progress_task = None
            error_message = f"Download failed for {name}: {str(e)}. Please check if the MPD URL is valid, requires authentication, or needs specific headers (e.g., Referer, Cookies)."
            if status_msg:
                status_msg = await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=error_message,
                    edit_message=status_msg
                )
            else:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=error_message,
                    event=event
                )
            raise
        finally:
            self.is_downloading = False

    def cleanup(self, filepath):
        """Aggressive cleanup of all related files"""
        try:
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
                logging.info(f"Cleaned up: {filepath}")
        except Exception as e:
            logging.warning(f"Failed to cleanup {filepath}: {e}")

        # Clean up download directory of old files
        try:
            import glob
            import time
            current_time = time.time()

            # Remove files older than 1 hour
            for file_path in glob.glob(os.path.join(self.user_download_dir, "*")):
                if os.path.isfile(file_path):
                    file_age = current_time - os.path.getmtime(file_path)
                    if file_age > 3600:  # 1 hour
                        try:
                            os.remove(file_path)
                            logging.info(f"Cleaned up old file: {file_path}")
                        except Exception as e:
                            logging.warning(f"Failed to cleanup old file {file_path}: {e}")
        except Exception as e:
            logging.warning(f"Failed to cleanup old files: {e}")

    async def upload_file(self, event, filepath, status_msg, total_size, sender, duration):
        try:
            file_size = os.path.getsize(filepath)
            logging.info(f"Processing upload for {filepath}, size: {format_size(file_size)}")

            # Detect premium status more reliably
            is_premium = await self.detect_premium_status(sender.id)

            self.progress_state['start_time'] = time.time()
            self.progress_state['total_size'] = file_size
            self.progress_state['done_size'] = 0
            self.progress_state['percent'] = 0.0
            self.progress_state['speed'] = 0
            self.progress_state['elapsed'] = 0

            # Set size limits based on session string availability first, then premium status
            if admin_client and SESSION_STRING:
                # When session string is available, use higher limits for better upload capability
                max_size_mb = 3950  # 3.95GB when session string is available
                max_size_bytes = int(3.95 * 1024 * 1024 * 1024)  # 3.95GB limit
                user_type = "SESSION"
                logging.info(f"User {sender.id} using session string upload - max file size: {format_size(max_size_bytes)}")
            elif is_premium:
                max_size_mb = 3980  # 3.98GB for premium users
                max_size_bytes = int(3.98 * 1024 * 1024 * 1024)  # 3.98GB limit
                user_type = "PREMIUM"
                logging.info(f"User {sender.id} is premium, max file size: {format_size(max_size_bytes)}")
            else:
                max_size_mb = 1950  # 1.95GB for free users
                max_size_bytes = int(1.95 * 1024 * 1024 * 1024)  # 1.95GB limit
                user_type = "FREE"
                logging.info(f"User {sender.id} is free user, max file size: {format_size(max_size_bytes)}")

            # Check if file needs to be split
            if file_size > max_size_bytes:
                if not self.has_notified_split:
                    split_size_display = f"{split_size_mb}MB" if admin_client and SESSION_STRING else "1900MB"
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"📁 **File Splitting Required**\n\n"
                               f"👤 User Type: {user_type}\n"
                               f"📊 File Size: {format_size(file_size)}\n"
                               f"⚖️ Max Size: {format_size(max_size_bytes)}\n"
                               f"✂️ Splitting into {split_size_display} parts...",
                        edit_message=status_msg
                    )
                    self.has_notified_split = True
                    logging.info(f"File {format_size(file_size)} exceeds limit {format_size(max_size_bytes)} for {user_type} user {sender.id}")

                splitting_start = time.time()

                # Enhanced splitting progress callback with better tracking
                async def splitting_progress(current_index, total_parts, part_percent):
                    try:
                        # Calculate more accurate progress
                        parts_completed = current_index
                        current_part_progress = part_percent / 100.0
                        total_progress = (parts_completed + current_part_progress) / total_parts

                        processed_bytes = int(file_size * total_progress)
                        elapsed = time.time() - splitting_start

                        # Calculate speed based on processed data
                        if elapsed > 0:
                            speed = processed_bytes / elapsed
                        else:
                            speed = 0

                        # Update progress state
                        self.progress_state['stage'] = "Splitting"
                        self.progress_state['total_size'] = file_size
                        self.progress_state['done_size'] = processed_bytes
                        self.progress_state['percent'] = min(100.0, max(0.0, total_progress * 100.0))
                        self.progress_state['elapsed'] = elapsed
                        self.progress_state['speed'] = speed

                        # Create enhanced progress display
                        eta = (file_size - processed_bytes) / speed if speed > 0 else 0
                        # Detect user type for progress display
                        user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if await self.detect_premium_status(sender.id) else "FREE")
                        
                        display = progress_display(
                            self.progress_state['stage'],
                            self.progress_state['percent'],
                            self.progress_state['done_size'],
                            self.progress_state['total_size'],
                            self.progress_state['speed'],
                            self.progress_state['elapsed'],
                            sender.first_name,
                            user_type,
                            f"{os.path.basename(filepath)} (Part {min(current_index+1, total_parts)}/{total_parts})"
                        )

                        nonlocal status_msg
                        async with self.update_lock:
                            status_msg = await send_message_with_flood_control(
                                entity=event.chat_id,
                                message=display,
                                edit_message=status_msg
                            )

                    except Exception as e:
                        logging.error(f"Error in splitting progress callback: {e}")

                # Split file with proper size limits (use 3950MB when session string is available)
                split_size_mb = 3950 if (admin_client and SESSION_STRING) else max_size_mb
                chunks = await self.split_file(
                    filepath,
                    max_size_mb=split_size_mb,
                    progress_cb=splitting_progress,
                    cancel_event=self.abort_event
                )
                # Process each chunk with proper upload handling
                total_chunks = len(chunks)
                uploaded_chunks = []  # Track successfully uploaded chunks for cleanup
                overall_start_time = time.time()

                for i, chunk in enumerate(chunks):
                    chunk_size = os.path.getsize(chunk)
                    progress = {'uploaded': 0}
                    last_update_time = 0
                    chunk_start_time = time.time()

                    # Update stage for current chunk
                    self.progress_state['stage'] = "Uploading"
                    self.progress_state['total_size'] = chunk_size
                    self.progress_state['done_size'] = 0
                    self.progress_state['percent'] = 0.0
                    self.progress_state['start_time'] = chunk_start_time

                    logging.info(f"Starting upload of Part {i+1}/{total_chunks} ({format_size(chunk_size)}) for user {sender.id}")

                    # Generate unique file ID for each chunk
                    file_id = random.getrandbits(63)
                    part_size = 524288  # 512KB chunks for Telegram
                    total_parts = (chunk_size + part_size - 1) // part_size

                    # Calculate optimal part size for large files
                    # For 3.8GB+ files, we need larger parts to stay under 4000 part limit
                    min_part_size = 1024 * 1024  # 1MB minimum
                    max_part_size = 512 * 1024 * 1024  # 512MB maximum (Telegram limit)
                    
                    # Calculate required part size to stay under 4000 parts
                    required_part_size = (chunk_size + 3999) // 4000
                    
                    # Round up to nearest 1MB for efficiency
                    optimal_part_size = ((required_part_size + 1024 * 1024 - 1) // (1024 * 1024)) * (1024 * 1024)
                    
                    # Ensure part size is within limits
                    part_size = max(min_part_size, min(optimal_part_size, max_part_size))
                    
                    # Recalculate total parts with optimal size
                    total_parts = (chunk_size + part_size - 1) // part_size
                    
                    # Final validation
                    if total_parts > 4000:
                        # If still too many parts, use maximum allowed part size
                        part_size = max_part_size
                        total_parts = (chunk_size + part_size - 1) // part_size
                        
                    if total_parts <= 0 or total_parts > 4000:
                        logging.error(f"CRITICAL: Cannot upload chunk {i+1}: {total_parts} parts (chunk_size: {chunk_size}, part_size: {part_size})")
                        continue

                    # Use session string client for large file uploads if available
                    upload_client = admin_client if admin_client else client
                    
                    max_concurrent = 6  # Reduced for large files to avoid rate limits
                    semaphore = asyncio.Semaphore(max_concurrent)
                    logging.info(f"Uploading Part {i+1}/{total_chunks}: {format_size(chunk_size)}, {total_parts} parts, file_id: {file_id}, part_size: {format_size(part_size)} via {type(upload_client).__name__}")

                    async def upload_part_fast(file_id, part_num, part_size, total_parts, chunk_path, progress, semaphore, upload_client):
                        async with semaphore:
                            retries = 5  # Increased retries for large files
                            for attempt in range(retries):
                                try:
                                    with open(chunk_path, 'rb') as f:
                                        f.seek(part_num * part_size)
                                        data = f.read(part_size)

                                    if not data:
                                        return (part_num, False, "No data")

                                    # Strict validation for large files
                                    if part_num >= total_parts:
                                        return (part_num, False, f"Part number {part_num} exceeds total parts {total_parts}")
                                    
                                    if part_num < 0:
                                        return (part_num, False, f"Invalid part number {part_num}")
                                    
                                    if len(data) > 512 * 1024 * 1024:  # 512MB max
                                        return (part_num, False, f"Part size {len(data)} exceeds 512MB limit")

                                    # Extended timeout for large parts
                                    upload_timeout = 180 if len(data) > 100 * 1024 * 1024 else 120

                                    # Use the appropriate client (session string preferred for large files)
                                    result = await asyncio.wait_for(
                                        upload_client(SaveBigFilePartRequest(
                                            file_id=file_id,
                                            file_part=part_num,
                                            file_total_parts=total_parts,
                                            bytes=data
                                        )),
                                        timeout=upload_timeout
                                    )

                                    if result:
                                        progress['uploaded'] += len(data)
                                        return (part_num, True, None)
                                    else:
                                        if attempt < retries - 1:
                                            await asyncio.sleep(3 * (attempt + 1))  # Longer backoff for large files
                                            continue
                                        return (part_num, False, "Upload returned False")

                                except Exception as e:
                                    error_msg = str(e)
                                    if "invalid" in error_msg.lower() and "parts" in error_msg.lower():
                                        logging.error(f"Parts validation error - file_id: {file_id}, part: {part_num}, total: {total_parts}, data_size: {len(data) if 'data' in locals() else 'unknown'}")
                                        logging.error(f"Chunk path: {chunk_path}, chunk size: {os.path.getsize(chunk_path) if os.path.exists(chunk_path) else 'N/A'}")
                                    
                                    if "flood" in error_msg.lower() or "wait" in error_msg.lower():
                                        # Handle flood wait with exponential backoff
                                        wait_time = min(60, 5 * (2 ** attempt))
                                        logging.warning(f"Flood wait detected, waiting {wait_time}s before retry")
                                        await asyncio.sleep(wait_time)
                                    
                                    if attempt < retries - 1:
                                        logging.warning(f"Part {part_num} upload attempt {attempt + 1} failed: {e}, retrying...")
                                        await asyncio.sleep(3 * (attempt + 1))
                                        continue
                                    return (part_num, False, str(e))

                            return (part_num, False, "Max retries exceeded")

                    async def update_progress():
                        nonlocal last_update_time, status_msg
                        UPDATE_INTERVAL = 6.0  # Exactly 6 seconds between updates

                        while progress['uploaded'] < chunk_size and not asyncio.current_task().cancelled():
                            current_time = time.time()

                            # Only update every 6 seconds exactly
                            if current_time - last_update_time < UPDATE_INTERVAL:
                                await asyncio.sleep(0.5)
                                continue

                            try:
                                self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                                current_speed = progress['uploaded'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0

                                # Calculate percentage based on uploaded bytes for the current chunk
                                current_percent = (progress['uploaded'] / chunk_size * 100) if chunk_size > 0 else 0

                                self.progress_state['speed'] = current_speed
                                self.progress_state['done_size'] = progress['uploaded']
                                self.progress_state['percent'] = current_percent

                                # Detect user type for progress display
                                user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if is_premium else "FREE")
                                
                                display = progress_display(
                                    self.progress_state['stage'],
                                    self.progress_state['percent'],
                                    self.progress_state['done_size'],
                                    self.progress_state['total_size'],
                                    self.progress_state['speed'],
                                    self.progress_state['elapsed'],
                                    sender.first_name,
                                    user_type,
                                    f"{os.path.basename(chunk)} (Part {i+1}/{total_chunks})"
                                )

                                async with self.update_lock:
                                    result = await send_message_with_flood_control(
                                        entity=event.chat_id,
                                        message=display,
                                        edit_message=status_msg
                                    )
                                    if result is not None:
                                        status_msg = result
                                        last_update_time = current_time
                                        logging.debug(f"Progress updated: {current_percent:.1f}% (Part {i+1}/{total_chunks})")

                            except Exception as e:
                                logging.warning(f"Progress update failed: {e}")
                                last_update_time = current_time  # Still update time to prevent spam

                            # Wait exactly 6 seconds before next update
                            await asyncio.sleep(UPDATE_INTERVAL)

                    # Upload all parts in parallel using appropriate client
                    tasks = []
                    for part_num in range(total_parts):
                        tasks.append(upload_part_fast(file_id, part_num, part_size, total_parts, chunk, progress, semaphore, upload_client))

                    progress_task = asyncio.create_task(update_progress())

                    try:
                        results = await asyncio.gather(*tasks, return_exceptions=False)

                        # Cancel progress task
                        progress_task.cancel()
                        try:
                            await progress_task
                        except asyncio.CancelledError:
                            pass

                        # Check for failed parts
                        failed_parts = [(part_num, error) for part_num, success, error in results if not success]
                        if failed_parts:
                            error_msgs = [f"Part {part_num}: {error}" for part_num, error in failed_parts[:5]]  # Show first 5 errors
                            raise Exception(f"Upload failed for {len(failed_parts)} parts: " + "; ".join(error_msgs))

                        # Prepare thumbnail
                        thumbnail_file = None
                        async with thumbnail_lock:
                            if sender.id in user_thumbnails and os.path.exists(user_thumbnails[sender.id]):
                                thumbnail_file = user_thumbnails[sender.id]

                        if not thumbnail_file:
                            temp_thumb_path = os.path.join(self.user_download_dir, f"temp_thumb_{i+1}.jpg")
                            if await extract_video_frame_thumbnail(chunk, temp_thumb_path):
                                thumbnail_file = temp_thumb_path
                            elif await generate_random_thumbnail(temp_thumb_path):
                                thumbnail_file = temp_thumb_path

                        # Finalize upload
                        input_file_big = InputFileBig(
                            id=file_id,
                            parts=total_parts,
                            name=os.path.basename(chunk)
                        )

                        self.progress_state['stage'] = "Finalizing"
                        self.progress_state['percent'] = 100.0
                        # Detect user type for progress display
                        user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if is_premium else "FREE")
                        
                        display = progress_display(
                            self.progress_state['stage'],
                            self.progress_state['percent'],
                            self.progress_state['done_size'],
                            self.progress_state['total_size'],
                            self.progress_state['speed'],
                            self.progress_state['elapsed'],
                            sender.first_name,
                            user_type,
                            f"{os.path.basename(chunk)} (Part {i+1}/{total_chunks})"
                        )
                        async with self.update_lock:
                            status_msg = await send_message_with_flood_control(entity=event.chat_id, message=display, edit_message=status_msg)

                        # Get video metadata for proper attributes
                        video_duration = 0
                        video_width = 1280
                        video_height = 720
                        
                        try:
                            # Extract metadata from the chunk
                            metadata_cmd = [
                                'ffprobe', '-v', 'quiet', '-print_format', 'json', 
                                '-show_format', '-show_streams', chunk
                            ]
                            process = await asyncio.create_subprocess_exec(
                                *metadata_cmd, 
                                stdout=asyncio.subprocess.PIPE, 
                                stderr=asyncio.subprocess.PIPE
                            )
                            stdout, stderr = await process.communicate()
                            
                            if process.returncode == 0:
                                metadata = json.loads(stdout.decode())
                                for stream in metadata.get('streams', []):
                                    if stream.get('codec_type') == 'video':
                                        video_width = stream.get('width', 1280)
                                        video_height = stream.get('height', 720)
                                        break
                                
                                # Get duration from format
                                format_info = metadata.get('format', {})
                                if 'duration' in format_info:
                                    video_duration = int(float(format_info['duration']))
                                    
                        except Exception as e:
                            logging.warning(f"Failed to extract video metadata: {e}")

                        # Upload to log channel using session string client (preferred for 4GB+ files)
                        async def upload_to_log_channel():
                            # Use session string client if available, otherwise fall back to bot client
                            upload_client = admin_client if admin_client else client
                            if thumbnail_file and os.path.exists(thumbnail_file):
                                return await upload_client.send_file(
                                    LOG_CHANNEL_ID,
                                    file=input_file_big,
                                    caption=f"Part {i+1}/{total_chunks}: {os.path.basename(filepath)} - User: {sender.id}",
                                    thumb=thumbnail_file,
                                    attributes=[DocumentAttributeVideo(
                                        duration=video_duration, 
                                        w=video_width, 
                                        h=video_height, 
                                        supports_streaming=True
                                    )],
                                    force_document=False
                                )
                            else:
                                return await upload_client.send_file(
                                    LOG_CHANNEL_ID,
                                    file=input_file_big,
                                    caption=f"Part {i+1}/{total_chunks}: {os.path.basename(filepath)} - User: {sender.id}",
                                    attributes=[DocumentAttributeVideo(
                                        duration=video_duration, 
                                        w=video_width, 
                                        h=video_height, 
                                        supports_streaming=True
                                    )],
                                    force_document=False
                                )

                        # Upload to log channel
                        log_msg = await retry_with_backoff(
                            coroutine=upload_to_log_channel,
                            max_retries=3,
                            base_delay=1,
                            operation_name=f"Upload file part {i+1} to log channel"
                        )

                        # Send file from log channel to user (appears as new message without forwarding label)
                        async def send_to_user():
                            return await client.send_file(
                                entity=event.chat_id,
                                file=log_msg.media,
                                caption=f"Part {i+1}/{total_chunks}: {os.path.basename(filepath)}",
                                thumb=thumbnail_file if thumbnail_file and os.path.exists(thumbnail_file) else None,
                                attributes=[DocumentAttributeVideo(
                                    duration=video_duration, 
                                    w=video_width, 
                                    h=video_height, 
                                    supports_streaming=True
                                )],
                                force_document=False
                            )

                        sent_msg = await retry_with_backoff(
                            coroutine=send_to_user,
                            max_retries=3,
                            base_delay=1,
                            operation_name=f"Send file part {i+1} to user"
                        )

                        # Clean up temp thumbnail
                        if thumbnail_file and thumbnail_file.startswith(os.path.join(self.user_download_dir, "temp_thumb_")):
                            try:
                                os.remove(thumbnail_file)
                            except:
                                pass

                        uploaded_chunks.append(chunk)
                        logging.info(f"✅ Part {i+1}/{total_chunks} uploaded successfully: {os.path.basename(chunk)} ({format_size(chunk_size)})")

                    except Exception as e:
                        logging.error(f"❌ Part {i+1}/{total_chunks} upload failed: {str(e)}")
                        # Cancel progress task
                        if not progress_task.done():
                            progress_task.cancel()
                            try:
                                await progress_task
                            except asyncio.CancelledError:
                                pass
                        raise

                # Calculate upload statistics
                total_upload_time = time.time() - self.progress_state['start_time']
                avg_speed = file_size / total_upload_time if total_upload_time > 0 else 0

                # Send completion message only for single downloads, not JSON batch processing
                # Check if this is from JSON processing by looking at the task source
                is_json_batch = hasattr(self, '_is_json_batch') and self._is_json_batch
                if not is_json_batch:
                    split_info = f"{split_size_mb}MB each" if admin_client and SESSION_STRING else "1900MB each"
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"🎉 **Upload Complete!**\n\n"
                               f"📁 File: {os.path.basename(filepath)}\n"
                               f"✂️ Split into: {len(uploaded_chunks)} parts ({split_info})\n"
                               f"📊 Total Size: {format_size(file_size)}\n"
                               f"⚡ Average Speed: {format_size(avg_speed)}/s\n"
                               f"⏱️ Total Time: {format_time(total_upload_time)}\n"
                               f"🧹 Cleanup: Complete",
                        event=event
                    )

                # Cleanup uploaded chunks and original file AFTER completion message
                try:
                    for chunk_path in uploaded_chunks:
                        try:
                            if os.path.exists(chunk_path):
                                os.remove(chunk_path)
                                logging.info(f"🗑️ Cleaned up chunk: {os.path.basename(chunk_path)}")
                        except Exception as e:
                            logging.warning(f"Failed to delete chunk {chunk_path}: {e}")

                    if os.path.exists(filepath):
                        os.remove(filepath)
                        logging.info(f"🗑️ Cleaned up original file: {os.path.basename(filepath)}")

                except Exception as cleanup_error:
                    logging.error(f"Cleanup error: {cleanup_error}")

            else:
                # File is under size limit - upload as single file
                logging.info(f"File {format_size(file_size)} is under limit {format_size(max_size_bytes)}, uploading as single file")

                self.progress_state['stage'] = "Uploading"
                self.progress_state['total_size'] = file_size
                self.progress_state['done_size'] = 0
                self.progress_state['percent'] = 0.0
                self.progress_state['start_time'] = time.time()

                # Generate unique file ID
                file_id = random.getrandbits(63)
                part_size = 524288  # 512KB chunks
                total_parts = (file_size + part_size - 1) // part_size

                # Calculate optimal part size for large single files
                min_part_size = 1024 * 1024  # 1MB minimum
                max_part_size = 512 * 1024 * 1024  # 512MB maximum
                
                # Calculate required part size to stay under 4000 parts
                required_part_size = (file_size + 3999) // 4000
                
                # Round up to nearest 1MB for efficiency
                optimal_part_size = ((required_part_size + 1024 * 1024 - 1) // (1024 * 1024)) * (1024 * 1024)
                
                # Ensure part size is within limits
                part_size = max(min_part_size, min(optimal_part_size, max_part_size))
                
                # Recalculate total parts with optimal size
                total_parts = (file_size + part_size - 1) // part_size
                
                # Final validation
                if total_parts > 4000:
                    part_size = max_part_size
                    total_parts = (file_size + part_size - 1) // part_size

                if total_parts <= 0 or total_parts > 4000:
                    raise Exception(f"CRITICAL: Cannot upload file: {total_parts} parts (file_size: {file_size}, part_size: {part_size})")

                # Use session string client for large file uploads if available
                upload_client = admin_client if admin_client else client
                
                max_concurrent = 6  # Reduced for large files
                semaphore = asyncio.Semaphore(max_concurrent)
                progress = {'uploaded': 0}
                last_update_time = 0
                logging.info(f"Single file upload: {format_size(file_size)}, {total_parts} parts, part_size: {format_size(part_size)} via {type(upload_client).__name__}")

                async def upload_part_single(file_id, part_num, part_size, total_parts, file_path, progress, semaphore, upload_client):
                    async with semaphore:
                        retries = 5  # Increased retries for large files
                        for attempt in range(retries):
                            try:
                                with open(file_path, 'rb') as f:
                                    f.seek(part_num * part_size)
                                    data = f.read(part_size)

                                if not data:
                                    return (part_num, False, "No data")

                                # Strict validation for large files
                                if part_num >= total_parts:
                                    return (part_num, False, f"Part number {part_num} exceeds total parts {total_parts}")
                                
                                if part_num < 0:
                                    return (part_num, False, f"Invalid part number {part_num}")
                                
                                if len(data) > 512 * 1024 * 1024:  # 512MB max
                                    return (part_num, False, f"Part size {len(data)} exceeds 512MB limit")

                                # Extended timeout for large parts
                                upload_timeout = 180 if len(data) > 100 * 1024 * 1024 else 120

                                # Use the appropriate client (session string preferred for large files)
                                result = await asyncio.wait_for(
                                    upload_client(SaveBigFilePartRequest(
                                        file_id=file_id,
                                        file_part=part_num,
                                        file_total_parts=total_parts,
                                        bytes=data
                                    )),
                                    timeout=upload_timeout
                                )

                                if result:
                                    progress['uploaded'] += len(data)
                                    return (part_num, True, None)
                                else:
                                    if attempt < retries - 1:
                                        await asyncio.sleep(3 * (attempt + 1))  # Longer backoff for large files
                                        continue
                                    return (part_num, False, "Upload returned False")

                            except Exception as e:
                                error_msg = str(e)
                                if "invalid" in error_msg.lower() and "parts" in error_msg.lower():
                                    logging.error(f"Parts validation error - file_id: {file_id}, part: {part_num}, total: {total_parts}, data_size: {len(data) if 'data' in locals() else 'unknown'}")
                                    logging.error(f"File path: {file_path}, file size: {os.path.getsize(file_path) if os.path.exists(file_path) else 'N/A'}")
                                
                                if "flood" in error_msg.lower() or "wait" in error_msg.lower():
                                    # Handle flood wait with exponential backoff
                                    wait_time = min(60, 5 * (2 ** attempt))
                                    logging.warning(f"Flood wait detected, waiting {wait_time}s before retry")
                                    await asyncio.sleep(wait_time)
                                
                                if attempt < retries - 1:
                                    logging.warning(f"Part {part_num} upload attempt {attempt + 1} failed: {e}, retrying...")
                                    await asyncio.sleep(3 * (attempt + 1))
                                    continue
                                return (part_num, False, str(e))

                        return (part_num, False, "Max retries exceeded")

                async def update_single_progress():
                    nonlocal last_update_time, status_msg
                    UPDATE_INTERVAL = 6.0  # Exactly 6 seconds between updates

                    while progress['uploaded'] < file_size and not asyncio.current_task().cancelled():
                        current_time = time.time()

                        if current_time - last_update_time < UPDATE_INTERVAL:
                            await asyncio.sleep(0.5)
                            continue

                        try:
                            self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                            current_speed = progress['uploaded'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0
                            current_percent = (progress['uploaded'] / file_size * 100) if file_size > 0 else 0

                            self.progress_state['speed'] = current_speed
                            self.progress_state['done_size'] = progress['uploaded']
                            self.progress_state['percent'] = current_percent

                            # Detect user type for progress display
                            user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if is_premium else "FREE")
                            
                            display = progress_display(
                                self.progress_state['stage'],
                                self.progress_state['percent'],
                                self.progress_state['done_size'],
                                self.progress_state['total_size'],
                                self.progress_state['speed'],
                                self.progress_state['elapsed'],
                                sender.first_name,
                                user_type,
                                os.path.basename(filepath)
                            )

                            async with self.update_lock:
                                result = await send_message_with_flood_control(
                                    entity=event.chat_id,
                                    message=display,
                                    edit_message=status_msg
                                )
                                if result is not None:
                                    status_msg = result
                                    last_update_time = current_time

                        except Exception as e:
                            logging.warning(f"Single upload progress update failed: {e}")
                            last_update_time = current_time

                        # Wait exactly 6 seconds before next update
                        await asyncio.sleep(UPDATE_INTERVAL)

                # Upload all parts in parallel using appropriate client
                tasks = []
                for part_num in range(total_parts):
                    tasks.append(upload_part_single(file_id, part_num, part_size, total_parts, filepath, progress, semaphore, upload_client))

                progress_task = asyncio.create_task(update_single_progress())

                try:
                    results = await asyncio.gather(*tasks, return_exceptions=False)

                    # Cancel progress task
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass

                    # Check for failed parts
                    failed_parts = [(part_num, error) for part_num, success, error in results if not success]
                    if failed_parts:
                        error_msgs = [f"Part {part_num}: {error}" for part_num, error in failed_parts[:5]]
                        raise Exception(f"Upload failed for {len(failed_parts)} parts: " + "; ".join(error_msgs))

                    # Prepare thumbnail
                    thumbnail_file = None
                    async with thumbnail_lock:
                        if sender.id in user_thumbnails and os.path.exists(user_thumbnails[sender.id]):
                            thumbnail_file = user_thumbnails[sender.id]

                    if not thumbnail_file:
                        temp_thumb_path = os.path.join(self.user_download_dir, f"temp_thumb_single.jpg")
                        if await extract_video_frame_thumbnail(filepath, temp_thumb_path):
                            thumbnail_file = temp_thumb_path
                        elif await generate_random_thumbnail(temp_thumb_path):
                            thumbnail_file = temp_thumb_path

                    # Finalize upload
                    input_file_big = InputFileBig(
                        id=file_id,
                        parts=total_parts,
                        name=os.path.basename(filepath)
                    )

                    self.progress_state['stage'] = "Finalizing"
                    self.progress_state['percent'] = 100.0
                    # Detect user type for progress display
                    user_type = "SESSION" if (admin_client and SESSION_STRING) else ("PREMIUM" if is_premium else "FREE")
                    
                    display = progress_display(
                        self.progress_state['stage'],
                        self.progress_state['percent'],
                        self.progress_state['done_size'],
                        self.progress_state['total_size'],
                        self.progress_state['speed'],
                        self.progress_state['elapsed'],
                        sender.first_name,
                        user_type,
                        os.path.basename(filepath)
                    )
                    async with self.update_lock:
                        status_msg = await send_message_with_flood_control(entity=event.chat_id, message=display, edit_message=status_msg)

                    # Get proper video metadata for single file
                    video_width = 1280
                    video_height = 720
                    video_duration = duration if duration > 0 else 0
                    
                    try:
                        # Extract detailed metadata
                        metadata_cmd = [
                            'ffprobe', '-v', 'quiet', '-print_format', 'json', 
                            '-show_format', '-show_streams', filepath
                        ]
                        process = await asyncio.create_subprocess_exec(
                            *metadata_cmd, 
                            stdout=asyncio.subprocess.PIPE, 
                            stderr=asyncio.subprocess.PIPE
                        )
                        stdout, stderr = await process.communicate()
                        
                        if process.returncode == 0:
                            metadata = json.loads(stdout.decode())
                            for stream in metadata.get('streams', []):
                                if stream.get('codec_type') == 'video':
                                    video_width = stream.get('width', 1280)
                                    video_height = stream.get('height', 720)
                                    break
                            
                            # Get duration from format if not already available
                            if video_duration == 0:
                                format_info = metadata.get('format', {})
                                if 'duration' in format_info:
                                    video_duration = int(float(format_info['duration']))
                                    
                    except Exception as e:
                        logging.warning(f"Failed to extract video metadata: {e}")

                    # Upload to log channel using session string client (preferred for large files)
                    async def upload_single_to_log_channel():
                        # Use session string client if available, otherwise fall back to bot client
                        upload_client = admin_client if admin_client else client
                        if thumbnail_file and os.path.exists(thumbnail_file):
                            return await upload_client.send_file(
                                LOG_CHANNEL_ID,
                                file=input_file_big,
                                caption=f"{os.path.basename(filepath)} - User: {sender.id}",
                                thumb=thumbnail_file,
                                attributes=[DocumentAttributeVideo(
                                    duration=video_duration, 
                                    w=video_width, 
                                    h=video_height, 
                                    supports_streaming=True
                                )],
                                force_document=False
                            )
                        else:
                            return await upload_client.send_file(
                                LOG_CHANNEL_ID,
                                file=input_file_big,
                                caption=f"{os.path.basename(filepath)} - User: {sender.id}",
                                attributes=[DocumentAttributeVideo(
                                    duration=video_duration, 
                                    w=video_width, 
                                    h=video_height, 
                                    supports_streaming=True
                                )],
                                force_document=False
                            )

                    # Upload to log channel
                    log_msg = await retry_with_backoff(
                        coroutine=upload_single_to_log_channel,
                        max_retries=3,
                        base_delay=1,
                        operation_name="Upload single file to log channel"
                    )

                    # Send file from log channel to user (appears as new message without forwarding label)
                    async def send_single_to_user():
                        return await client.send_file(
                            entity=event.chat_id,
                            file=log_msg.media,
                            caption=f"{os.path.basename(filepath)}",
                            thumb=thumbnail_file if thumbnail_file and os.path.exists(thumbnail_file) else None,
                            attributes=[DocumentAttributeVideo(
                                duration=video_duration, 
                                w=video_width, 
                                h=video_height, 
                                supports_streaming=True
                            )],
                            force_document=False
                        )

                    sent_msg = await retry_with_backoff(
                        coroutine=send_single_to_user,
                        max_retries=3,
                        base_delay=1,
                        operation_name="Send single file to user"
                    )

                    # Clean up temp thumbnail
                    if thumbnail_file and thumbnail_file.startswith(os.path.join(self.user_download_dir, "temp_thumb_")):
                        try:
                            os.remove(thumbnail_file)
                        except:
                            pass

                    # Update user speed statistics for upload
                    elapsed = time.time() - self.progress_state['start_time']
                    upload_speed = file_size / elapsed if elapsed > 0 else 0
                    async with speed_lock:
                        if self.user_id not in user_speed_stats:
                            user_speed_stats[self.user_id] = {}
                        user_speed_stats[self.user_id]['upload_speed'] = upload_speed
                        user_speed_stats[self.user_id]['last_updated'] = time.time()

                    logging.info(f"✅ Single file uploaded successfully: {os.path.basename(filepath)} ({format_size(file_size)})")

                    # Calculate upload statistics
                    elapsed = time.time() - self.progress_state['start_time']
                    upload_speed = file_size / elapsed if elapsed > 0 else 0

                    # Send completion message only for single downloads, not JSON batch processing
                    # Check if this is from JSON processing by looking at the task source
                    is_json_batch = hasattr(self, '_is_json_batch') and self._is_json_batch
                    if not is_json_batch:
                        await send_message_with_flood_control(
                            entity=event.chat_id,
                            message=f"🎉 **Upload Complete!**\n\n"
                                   f"📁 File: {os.path.basename(filepath)}\n"
                                   f"📊 Size: {format_size(file_size)}\n"
                                   f"⚡ Speed: {format_size(upload_speed)}/s\n"
                                   f"⏱️ Time: {format_time(elapsed)}",
                            event=event
                        )

                    # Cleanup original file
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            logging.info(f"🗑️ Cleaned up original file: {os.path.basename(filepath)}")
                    except Exception as cleanup_error:
                        logging.error(f"Cleanup error: {cleanup_error}")

                except Exception as e:
                    logging.error(f"❌ Single file upload failed: {str(e)}")
                    # Cancel progress task
                    if not progress_task.done():
                        progress_task.cancel()
                        try:
                            await progress_task
                        except asyncio.CancelledError:
                            pass
                    raise

        except asyncio.CancelledError:
            logging.info(f"Upload cancelled for user {sender.id} with file {filepath}")
            raise # Re-raise CancelledError to be caught by the outer process_task handler

        except Exception as e:
            logging.error(f"Upload failed: {str(e)}\n{traceback.format_exc()}")
            error_msg = f"Upload failed: {str(e)}"
            # Add retry information if applicable, but for permanent failures, just report error
            retry_info = "" # No specific retry logic for upload failures in this part
            status_msg = await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"❌ Upload failed{retry_info}: {str(e)}\n\n💡 JSON data preserved for future use",
                edit_message=status_msg
            )
            logging.error(f"Upload failed permanently for user {self.user_id}: {error_msg}")
            raise Exception(error_msg)
        finally:
            self.has_notified_split = False  # Reset the flag after upload

    async def process_task(self, event, task_data, sender, starting_msg=None):
        """Process a single task (download and upload) - supports both DRM and direct downloads."""
        filepath = None
        status_msg = None
        try:
            task_type = task_data.get('type', 'drm')
            name = task_data['name']

            logging.info(f"Starting task processing for {name} (type: {task_type}) - user {self.user_id}")

            if task_type == 'drm':
                # DRM protected content
                mpd_url = task_data['mpd_url']
                key = task_data['key']
                result = await self.download_and_decrypt(event, mpd_url, key, name, sender)
            elif task_type == 'direct':
                # Direct download
                url = task_data['url']
                result = await self.download_direct_file(event, url, name, sender)
            else:
                raise ValueError(f"Unsupported task type: {task_type}")

            if result is None:  # Download was rejected due to another ongoing download
                logging.warning(f"Download rejected for {name} - user {self.user_id}")
                return False, "Download rejected"

            filepath, status_msg, total_size, duration = result
            logging.info(f"Download completed for {name}, starting upload - user {self.user_id}")

            # Upload the video
            await self.upload_file(event, filepath, status_msg, total_size, sender, duration)
            logging.info(f"Upload completed for {name} - user {self.user_id}")

            # Delete starting message if provided
            if starting_msg:
                try:
                    await starting_msg.delete()
                    logging.info(f"Deleted starting message for task: {name}")
                except Exception as e:
                    logging.warning(f"Could not delete starting message: {e}")

            # Delete status message
            if status_msg:
                try:
                    await status_msg.delete()
                    logging.info(f"Deleted status message for task: {name}")
                except Exception as e:
                    logging.warning(f"Could not delete status message: {e}")

            return True, None  # Success

        except asyncio.CancelledError:
            logging.info(f"Task {task_data.get('name', 'unknown')} was cancelled")

            # Cancel progress task immediately
            if self.progress_task and not self.progress_task.done():
                self.progress_task.cancel()
                try:
                    await self.progress_task
                except asyncio.CancelledError:
                    logging.info("Progress task cancelled successfully")
                except Exception as e:
                    logging.error(f"Error cancelling progress task: {e}")

            # Send skip notification if starting message exists
            if starting_msg:
                try:
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"⏭️ **Skipped:** {task_data.get('name', 'unknown')}.mp4",
                        event=event
                    )
                except Exception:
                    pass # Ignore errors during skip notification

            return True, None # Task was cancelled, consider it "successful" for queue processing

        except Exception as e:
            logging.error(f"Task processing failed for {task_data.get('name', 'unknown')}: {str(e)}\n{traceback.format_exc()}")

            # Delete starting message if provided
            if starting_msg:
                try:
                    await starting_msg.delete()
                    logging.info(f"Deleted starting message for failed task: {task_data.get('name', 'unknown')}")
                except Exception as e:
                    logging.warning(f"Could not delete starting message: {e}")

            # Delete status message if exists
            if status_msg:
                try:
                    await status_msg.delete()
                    logging.info(f"Deleted status message for failed task: {task_data.get('name', 'unknown')}")
                except Exception as e:
                    logging.warning(f"Could not delete status message: {e}")

            return False, str(e)  # Failure with error message

        finally:
            # Cleanup files
            if filepath:
                self.cleanup(filepath)

            # Clean up all temporary files for this task
            cleanup_patterns = [
                f"{task_data.get('name', 'unknown')}_raw_video.mp4",
                f"{task_data.get('name', 'unknown')}_raw_audio.mp4",
                f"{task_data.get('name', 'unknown')}_decrypted_video.mp4",
                f"{task_data.get('name', 'unknown')}_decrypted_audio.mp4",
                f"{task_data.get('name', 'unknown')}.mp4",
                f"{task_data.get('name', 'unknown')}_video_seg*.mp4",
                f"{task_data.get('name', 'unknown')}_audio_seg*.mp4",
                f"{task_data.get('name', 'unknown')}_part*.mp4"
            ]

            import glob
            for pattern in cleanup_patterns:
                files = glob.glob(os.path.join(self.user_download_dir, pattern))
                for file in files:
                    try:
                        if os.path.exists(file):
                            os.remove(file)
                            logging.info(f"Cleaned up: {file}")
                    except Exception as cleanup_error:
                        logging.warning(f"Failed to cleanup {file}: {cleanup_error}")

            logging.info(f"Cleanup completed for task: {task_data.get('name', 'unknown')}")

    async def process_queue(self, event):
        """Process tasks in the queue one at a time for this user."""
        user_queue, user_states, user_lock = get_user_resources(self.user_id)
        # Persist instance reference for status/speed lookups
        user_bot_instances[self.user_id] = self
        logging.info(f"Starting queue processor for user {self.user_id}")

        total_initial_tasks = len(user_queue)  # Store initial queue size
        current_task_number = 1
        completed_tasks = []
        failed_tasks = []

        while True:
            # Check if there are tasks in the queue
            async with user_lock:
                if not user_queue:
                    user_states[self.user_id] = False
                    logging.info(f"Queue is empty for user {self.user_id}, stopping queue processor.")
                    break  # Exit the loop if the queue is empty
                # Get the next task from the queue
                task = user_queue.popleft()
                remaining_tasks = len(user_queue)
                user_states[self.user_id] = True
                logging.info(f"Processing task for user {self.user_id}: {task['name']}.mp4, Position: {current_task_number}/{total_initial_tasks}, Queue length: {remaining_tasks}")

            # Extract task details
            name = task['name']
            sender = task['sender']

            # Notify user that this task is starting
            starting_msg = await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"Starting task {current_task_number}/{total_initial_tasks}: {name}.mp4",
                event=event
            )

            # Process the task
            success, error = await self.process_task(event, task, sender, starting_msg)

            if success:
                completed_tasks.append(name)
            else:
                failed_tasks.append((name, error))

            # Increment task counter
            current_task_number += 1

        # Send final summary when all tasks are completed
        if total_initial_tasks > 0:
            completion_messages = format_completion_message(completed_tasks, failed_tasks, total_initial_tasks)

            for msg in completion_messages:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=msg,
                    event=event
                )

        logging.info(f"Queue processor finished for user {self.user_id}")

    async def detect_premium_status(self, user_id):
        """Detect if user has premium status with multiple methods using session string client"""
        try:
            # Use admin client (session string) if available, otherwise fall back to bot client
            detection_client = admin_client if admin_client else client
            
            # Get full user entity with all attributes
            user = await detection_client.get_entity(user_id)

            # Method 1: Check premium attribute directly
            if hasattr(user, 'premium') and user.premium:
                logging.info(f"User {user_id} detected as premium via premium attribute")
                return True

            # Method 2: Check alternative premium attributes
            premium_indicators = ['is_premium', 'premium_flag', 'has_premium']
            for attr in premium_indicators:
                if hasattr(user, attr) and getattr(user, attr, False):
                    logging.info(f"User {user_id} detected as premium via {attr}")
                    return True

            # Method 3: Check user flags (Telegram stores premium status in flags)
            if hasattr(user, 'flags') and user.flags:
                # Premium users typically have different flag patterns
                if user.flags & (1 << 4):  # Premium flag bit
                    logging.info(f"User {user_id} detected as premium via flags")
                    return True

            # Method 4: If using session string, check session client's own premium status as additional indicator
            if admin_client and detection_client == admin_client:
                try:
                    session_me = await admin_client.get_me()
                    if hasattr(session_me, 'premium') and session_me.premium:
                        logging.info(f"Session account is premium, allowing premium limits for user {user_id}")
                        return True
                except Exception as e:
                    logging.warning(f"Could not check session account premium status: {e}")

            # Method 5: Default to free user for proper file size limits
            logging.info(f"User {user_id} - detected as free user")
            return False

        except Exception as e:
            logging.warning(f"Could not detect premium status for user {user_id}: {e}, assuming free user")
            return False  # Default to free user for proper limits


@client.on(events.NewMessage(pattern=r'^/start'))
async def start_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /start command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="🚫 You're not authorized to use this bot.\n\nContact the admin to get access.",
            event=event
        )
        logging.info(f"Unauthorized access attempt by {sender.id}")
        return

    welcome_message = (
        "✨ ————  𝚉𝚎𝚛𝚘𝚃𝚛𝚊𝚌𝚎 𝙻𝚎𝚎𝚌𝚑 𝙱𝚘𝚝  ———— ✨\n\n"
        "Hello! I'm your ultra-fast Telegram leech bot. Here's what I can do for you:\n\n"
        "📥  𝗟𝗲𝗲𝗰𝗵 (DRM/Direct)\n"
        "   • /leech\n"
        "   • `<mpd_url>|<key>|<name>`\n"
        "   • `<direct_url>|<name>` or `/leech <direct_url>`\n\n"
        "⚡  𝗤𝘂𝗶𝗰𝗸 𝗠𝗣𝟰 𝗟𝗲𝗲𝗰𝗵\n"
        "   • /mplink `<direct_url>|<name>`\n\n"
        "📋  𝗝𝗦𝗢𝗡 𝗪𝗼𝗿𝗸𝗳𝗹𝗼𝘄\n"
        "   • /loadjson — send JSON file or text\n"
        "   • /processjson [range] — e.g. `all`, `1-50`, `5`\n\n"
        "📦  𝗕𝘂𝗹𝗸 𝗣𝗿𝗼𝗰𝗲𝘀𝘀𝗶𝗻𝗴\n"
        "   • /bulk — start bulk mode\n"
        "   • /processbulk — process each JSON sequentially\n"
        "   • /clearbulk — clear stored JSONs\n\n"
        "🎛️  𝗛𝗲𝗹𝗽𝗳𝘂𝗹 𝗖𝗼𝗻𝘁𝗿𝗼𝗹𝘀\n"
        "   • /speed — live VPS speed test\n"
        "   • /status — current task status\n"
        "   • /skip — skip current task\n"
        "   • /skip 3-5 — skip queued tasks 3 to 5\n"
        "   • /clearall — stop and clear queue\n"
        "   • /clear — full cleanup\n\n"
        "🖼️  𝗧𝗵𝘂𝗺𝗯𝗻𝗮𝗶𝗹𝘀\n"
        "   • /addthumbnail — send a photo\n"
        "   • /removethumbnail — remove custom thumbnail\n\n"
        "🛡️  𝗔𝗱𝗺𝗶𝗻\n"
        "   • /addadmin <id>\n"
        "   • /removeadmin <id>\n\n"
        "Ready to go. Drop links and I'll fly! 🚀"
    )

    await send_message_with_flood_control(
        entity=event.chat_id,
        message=welcome_message,
        event=event
    )

@client.on(events.NewMessage(pattern=r'^/(leech|mplink)'))
async def leech_handler(event):
    sender = await event.get_sender()
    raw_message = event.raw_text  # Log the raw message text
    logging.info(f"Received /leech command from user {sender.id}: {raw_message}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized, fuck off.",
            event=event
        )
        logging.info(f"Unauthorized access attempt by {sender.id}")
        return

    # Get user-specific resources
    user_queue, user_states, user_lock = get_user_resources(sender.id)

    try:
        # Extract the message content after the command
        message_content = event.raw_text.split('\n', 1)
        if len(message_content) < 2:
            # Accept inline usage: /leech <url> [| name]
            parts = raw_message.split(maxsplit=1)
            if len(parts) == 2:
                inline = parts[1].strip()
                if inline.startswith('http'):
                    # treat same as one-line payload
                    message_content = [parts[0], inline]
                else:
                    cmd = event.pattern_match.group(1)
                    if cmd == 'mplink':
                        usage = "Format: /mplink\n<direct_url>|<name>\n(direct .mp4 or any direct file)"
                    else:
                        usage = "Format: /leech\n<mpd_url>|<key>|<name>\n<direct_url>|<name>\n...\nOr use /loadjson for batch processing"
                    await send_message_with_flood_control(entity=event.chat_id, message=usage, event=event)
                    return
            else:
                cmd = event.pattern_match.group(1)
                if cmd == 'mplink':
                    usage = "Format: /mplink\n<direct_url>|<name>\n(direct .mp4 or any direct file)"
                else:
                    usage = "Format: /leech\n<mpd_url>|<key>|<name>\n<direct_url>|<name>\n...\nOr use /loadjson for batch processing"
                await send_message_with_flood_control(entity=event.chat_id, message=usage, event=event)
                return

        # Split the remaining content into individual lines (each line is a link)
        links = message_content[1].strip().split('\n')
        links = [link.strip() for link in links if link.strip()]  # Remove empty lines

        # Validate and parse links (regular format only)
        tasks_to_add = []
        invalid_links = []

        # Process both DRM and direct .mp4 links
        for i, link in enumerate(links, 1):
            args = link.split('|')
            # Support two formats:
            # 1) mpd_url|key|name  (DRM)
            # 2) direct_url|name   (Direct .mp4 or any file)
            if len(args) == 3:
                mpd_url, key, name = [arg.strip() for arg in args]
                logging.info(f"Processing DRM link {i}: {mpd_url} | {key} | {name}")
                if not mpd_url.startswith("http") or ".mpd" not in mpd_url:
                    invalid_links.append(f"Link {i}: Invalid MPD URL ({mpd_url})")
                    continue
                if ":" not in key or len(key.split(":")) != 2:
                    invalid_links.append(f"Link {i}: Key must be in KID:KEY format ({key})")
                    continue
                tasks_to_add.append({
                    'type': 'drm',
                    'mpd_url': mpd_url,
                    'key': key,
                    'name': name,
                    'sender': sender
                })
            elif len(args) == 2:
                direct_url, name = [arg.strip() for arg in args]
                logging.info(f"Processing Direct link {i}: {direct_url} | {name}")
                if not direct_url.startswith("http"):
                    invalid_links.append(f"Link {i}: Invalid URL ({direct_url})")
                    continue
                tasks_to_add.append({
                    'type': 'direct',
                    'url': direct_url,
                    'name': name,
                    'sender': sender
                })
            elif len(args) == 1 and args[0].strip().startswith('http'):
                # Direct URL only, derive name from URL
                direct_url = args[0].strip()
                name = derive_name_from_url(direct_url)
                logging.info(f"Processing Direct link {i}: {direct_url} | (derived name) {name}")
                tasks_to_add.append({
                    'type': 'direct',
                    'url': direct_url,
                    'name': name,
                    'sender': sender
                })
            else:
                invalid_links.append(f"Link {i}: Invalid format (expected mpd_url|key|name OR direct_url|name)")

        # If there are invalid links, notify the user
        if invalid_links:
            error_message = "The following links are invalid and will be skipped:\n" + "\n".join(invalid_links)
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=error_message,
                event=event
            )

        # If no valid tasks were found, stop here
        if not tasks_to_add:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="No valid links were found. Please check the format and try again.",
                event=event
            )
            return

        # Add the valid tasks to the user's queue
        async with user_lock:
            # Add each task to the queue
            for task in tasks_to_add:
                user_queue.append(task)
                position = len(user_queue)
                logging.info(f"Task added to queue for user {sender.id}: {task['name']}.mp4, Position: {position}/{len(user_queue)}")

            # Reset abort flag if an instance exists
            if sender.id in user_bot_instances and user_bot_instances[sender.id]:
                try:
                    user_bot_instances[sender.id].abort_event.clear()
                except Exception:
                    pass

            # Notify user about the tasks added to the queue
            if len(tasks_to_add) <= 10:
                # Show all tasks if 10 or fewer
                queue_message = f"Added {len(tasks_to_add)} task(s) to your queue:\n"
                start_position = len(user_queue) - len(tasks_to_add) + 1
                for i, task in enumerate(tasks_to_add, start_position):
                    queue_message += f"Task {i}: {task['name']}.mp4 (Position {i}/{len(user_queue)})\n"
            else:
                # Show summary for large batches
                queue_message = f"Added {len(tasks_to_add)} task(s) to your queue:\n"
                start_position = len(user_queue) - len(tasks_to_add) + 1
                # Show first 5 tasks
                for i, task in enumerate(tasks_to_add[:5], start_position):
                    queue_message += f"Task {i}: {task['name']}.mp4 (Position {i}/{len(user_queue)})\n"
                queue_message += f"... and {len(tasks_to_add) - 5} more tasks\n"
                queue_message += f"Total queue size: {len(user_queue)} tasks\n"

            if user_states.get(sender.id, False):
                queue_message += "A task is currently being processed. Your tasks will start soon… ⏳"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=queue_message,
                event=event
            )

            # Start the queue processor if it's not already running for this user
            if not user_states.get(sender.id, False) and (not user_active_tasks.get(sender.id) or user_active_tasks[sender.id].done()):
                logging.info(f"Starting queue processor for user {sender.id} from /leech handler")
                bot = MPDLeechBot(sender.id)
                bot._is_json_batch = False # Explicitly mark as not JSON batch for single link
                user_bot_instances[sender.id] = bot
                user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

    except Exception as e:
        logging.error(f"Leech handler error: {str(e)}\n{traceback.format_exc()}")
        error_message = f"Failed to add tasks: {str(e)}"
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=error_message,
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/(clearall|stop)$'))
async def clearall_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /clearall or /stop command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    user_queue, user_states, user_lock = get_user_resources(sender.id)

    # Cancel active task if running
    if sender.id in user_active_tasks and user_active_tasks[sender.id]:
        active_task = user_active_tasks[sender.id]
        if not active_task.done():
            logging.info(f"Cancelling active task for user {sender.id}")
            # Signal skip/abort if instance exists
            bot_inst = user_bot_instances.get(sender.id)
            if bot_inst:
                bot_inst.abort_event.set()
            active_task.cancel()
            try:
                await active_task
            except asyncio.CancelledError:
                logging.info(f"Active task cancelled successfully for user {sender.id}")
            except Exception as e:
                logging.error(f"Error cancelling active task for user {sender.id}: {e}")
        user_active_tasks[sender.id] = None

    async with user_lock:
        cleared_count = len(user_queue)
        user_queue.clear()
        user_states[sender.id] = False
        user_bot_instances[sender.id] = None
        logging.info(f"Cleared {cleared_count} tasks from queue for user {sender.id}")

    await send_message_with_flood_control(entity=event.chat_id, message=f"🛑 Stopped and cleared {cleared_count} queued task(s).", event=event)

@client.on(events.NewMessage(pattern=r'^/clear'))
async def clear_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /clear command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    user_queue, user_states, user_lock = get_user_resources(sender.id)

    try:
        # Cancel active task if running
        if sender.id in user_active_tasks and user_active_tasks[sender.id]:
            active_task = user_active_tasks[sender.id]
            if not active_task.done():
                logging.info(f"Cancelling active task for user {sender.id}")
                bot_inst = user_bot_instances.get(sender.id)
                if bot_inst:
                    bot_inst.abort_event.set()
                active_task.cancel()
                try:
                    await active_task
                except asyncio.CancelledError:
                    logging.info(f"Active task cancelled successfully for user {sender.id}")
                except Exception as e:
                    logging.error(f"Error cancelling active task for user {sender.id}: {e}")
            user_active_tasks[sender.id] = None

        # Clear queue and set processing state to False
        async with user_lock:
            cleared_count = len(user_queue)
            user_queue.clear()
            user_states[sender.id] = False
            user_bot_instances[sender.id] = None
            logging.info(f"Cleared {cleared_count} tasks from queue for user {sender.id}")

        # Clear JSON data
        async with json_lock:
            if sender.id in user_json_data:
                del user_json_data[sender.id]
                logging.info(f"Cleared JSON data for user {sender.id}")

        # Clear user download directory
        user_download_dir = os.path.join(DOWNLOAD_DIR, f"user_{sender.id}")
        if os.path.exists(user_download_dir):
            import shutil
            shutil.rmtree(user_download_dir)
            os.makedirs(user_download_dir)
            logging.info(f"Cleared download directory for user {sender.id}")

        # Clear thumbnail
        async with thumbnail_lock:
            if sender.id in user_thumbnails:
                thumbnail_path = user_thumbnails[sender.id]
                try:
                    if os.path.exists(thumbnail_path):
                        os.remove(thumbnail_path)
                    del user_thumbnails[sender.id]
                    logging.info(f"Cleared thumbnail for user {sender.id}")
                except Exception as e:
                    logging.warning(f"Failed to clear thumbnail for user {sender.id}: {e}")

        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"🧹 **Complete Cleanup Done!**\n\n✅ Stopped active downloads\n✅ Cleared {cleared_count} task(s) from queue\n✅ Cleared stored JSON data\n✅ Cleared all downloaded videos\n✅ Cleared custom thumbnail\n\nYour account is now clean! 🎉",
            event=event
        )

    except Exception as e:
        logging.error(f"Error in clear command for user {sender.id}: {str(e)}")
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ Error during cleanup: {str(e)}",
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/loadjson'))
async def loadjson_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /loadjson command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    await send_message_with_flood_control(
        entity=event.chat_id,
        message="📥 Ready to receive JSON data!\n\nYou can:\n1. Upload a .json file\n2. Send JSON text directly\n\n**Auto-Detection Formats:**\n```json\n[\n  {\n    \"video_name\": \"Pleural Effusion\",\n    \"mpd_url\": \"https://example.com/manifest.mpd\",\n    \"keys\": [\"kid:key\"]\n  },\n  {\n    \"name\": \"Direct Video\",\n    \"url\": \"https://example.com/video.mp4\"\n  }\n]\n```\n\n**Features:**\n• Auto-detects DRM (mpd_url + keys) vs Direct (url)\n• Supports both `video_name` and `name` fields\n• No `type` field required!\n\nAfter sending JSON, use `/processjson` to start processing!",
        event=event
    )

@client.on(events.NewMessage())
async def json_data_handler(event):
    """Handle JSON file uploads and JSON text input"""
    sender = await event.get_sender()

    # Only process JSON from authorized users
    if sender.id not in authorized_users:
        return

    # Ignore commands and messages not starting with JSON syntax
    if event.text and (event.text.strip().startswith('/') or not (event.text.strip().startswith('[') or event.text.strip().startswith('{'))):
        return

    try:
        json_data = None
        filename = None

        # Check if it's a file upload
        if event.document and event.document.mime_type == 'application/json':
            logging.info(f"JSON file uploaded by user {sender.id}")

            # Get filename
            for attr in getattr(event.document, 'attributes', []):
                if hasattr(attr, 'file_name'):
                    filename = attr.file_name
                    break
            filename = filename or f"upload_{int(time.time())}.json"

            # Download and parse the file
            file_path = await event.download_media()
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.loads(f.read())
            os.remove(file_path)

        # Check if it's JSON text (starts with [ or {)
        elif event.text and (event.text.strip().startswith('[') or event.text.strip().startswith('{')):
            logging.info(f"JSON text received from user {sender.id}")
            json_data = json.loads(event.text.strip())
            filename = f"text_input_{int(time.time())}.json"

        # Process JSON data if found
        if json_data:
            item_count = len(json_data)

            # Check if user is in bulk mode
            async with bulk_lock:
                is_bulk_mode = sender.id in user_bulk_data

            if is_bulk_mode:
                # Bulk mode - add to bulk data
                user_bulk_data[sender.id].append(json_data)
                total_bulk = len(user_bulk_data[sender.id])
                await event.reply(f"📦 **Bulk #{total_bulk}:** {item_count} items ({filename})\n\nUse `/processbulk` to start or send more JSON files.")
            else:
                # Regular mode - store and send quick response
                async with json_lock:
                    user_json_data[sender.id] = json_data
                await event.reply(f"✅ **JSON Loaded:** {item_count} items from {filename}\n\n📋 Use `/processjson all` to process all items\n**Range:** 1-{len(json_data)}")

            logging.info(f"JSON processed for user {sender.id}: {item_count} items")

    except json.JSONDecodeError as e:
        await event.reply(f"❌ Invalid JSON: {str(e)}")
    except Exception as e:
        logging.error(f"JSON processing error for user {sender.id}: {str(e)}")
        await event.reply(f"❌ Error: {str(e)}")

@client.on(events.NewMessage(pattern=r'^/processjson(?:\s+(.+))?'))
async def processjson_handler(event):
    sender = await event.get_sender()
    range_input = event.pattern_match.group(1)
    logging.info(f"Received /processjson command from user {sender.id} with range: {range_input}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    async with json_lock:
        if sender.id not in user_json_data:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="❌ No JSON data found. Use /loadjson first to load JSON data.",
                event=event
            )
            return

        json_data = user_json_data[sender.id]

    # Handle range selection
    if not range_input:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"📋 **JSON Data Loaded: {len(json_data)} items**\n\nPlease specify range:\n\n**Examples:**\n• `/processjson all` - Process all items\n• `/processjson 1-10` - Process items 1 to 10\n• `/processjson 5` - Process only item 1\n\n**Current range: 1-{len(json_data)}**",
            event=event
        )
        return

    # Parse range input
    try:
        if range_input.lower() == "all":
            start_idx, end_idx = 0, len(json_data)
            selected_data = json_data
        elif "-" in range_input:
            start, end = map(int, range_input.split("-"))
            start_idx, end_idx = start - 1, end  # Convert to 0-based indexing
            if start_idx < 0 or end_idx > len(json_data) or start_idx >= end_idx:
                raise ValueError("Invalid range")
            selected_data = json_data[start_idx:end_idx]
        else:
            item_num = int(range_input)
            start_idx, end_idx = item_num - 1, item_num
            if start_idx < 0 or start_idx >= len(json_data):
                raise ValueError("Invalid item number")
            selected_data = [json_data[start_idx]]
    except (ValueError, IndexError):
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ Invalid range format. Use:\n• `all` for all items\n• `1-10` for range\n• `5` for single item\n\nValid range: 1-{len(json_data)}",
            event=event
        )
        return

    user_queue, user_states, user_lock = get_user_resources(sender.id)

    try:
        tasks_to_add = []
        invalid_items = []

        for i, item in enumerate(selected_data, start_idx + 1):
            try:
                # Support both 'name' and 'video_name' fields
                name = item.get('video_name') or item.get('name', f'Video_{i}')

                # Auto-detect content type based on available fields
                if item.get('mpd_url') and item.get('keys'):
                    # DRM content detected
                    mpd_url = item.get('mpd_url')
                    keys = item.get('keys', [])
                    key = keys[0] if isinstance(keys, list) else keys

                    task = {
                        'type': 'drm',
                        'mpd_url': mpd_url,
                        'key': key,
                        'name': name,
                        'sender': sender
                    }
                    tasks_to_add.append(task)
                    logging.info(f"Added DRM task from JSON: {name}")

                elif item.get('url'):
                    # Direct content detected
                    url = item.get('url')
                    task = {
                        'type': 'direct',
                        'url': url,
                        'name': name,
                        'sender': sender
                    }
                    tasks_to_add.append(task)
                    logging.info(f"Added direct task from JSON: {name}")

                else:
                    # Fallback to explicit type checking
                    item_type = item.get('type', 'drm').lower()
                    if item_type == 'drm':
                        mpd_url = item.get('mpd_url')
                        keys = item.get('keys', [])
                        if mpd_url and keys:
                            key = keys[0] if isinstance(keys, list) else keys
                            task = {
                                'type': 'drm',
                                'mpd_url': mpd_url,
                                'key': key,
                                'name': name,
                                'sender': sender
                            }
                            tasks_to_add.append(task)
                            logging.info(f"Added DRM task (explicit) from JSON: {name}")
                    elif item_type == 'direct':
                        url = item.get('url')
                        if url:
                            task = {
                                'type': 'direct',
                                'url': url,
                                'name': name,
                                'sender': sender
                            }
                            tasks_to_add.append(task)
                            logging.info(f"Added direct task (explicit) from JSON: {name}")

                    if not tasks_to_add or len(tasks_to_add) == 0 or tasks_to_add[-1]['name'] != name:
                        invalid_items.append(f"Item {i}: No valid content detected (need mpd_url+keys or url)")

            except Exception as e:
                invalid_items.append(f"Item {i}: Error processing - {str(e)}")

        # Report invalid items
        if invalid_items:
            error_message = "The following items are invalid and will be skipped:\n" + "\n".join(invalid_items)
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=error_message,
                event=event
            )

        # If no valid tasks, stop here
        if not tasks_to_add:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="No valid items found in JSON data.",
                event=event
            )
            return

        # Add tasks to queue
        async with user_lock:
            for task in tasks_to_add:
                # Use JSON name as filename
                filename = task['name']
                user_queue.append(task)
                logging.info(f"JSON task added to queue for user {sender.id}: {task['name']}.mp4")

            # Reset abort flag if an instance exists
            if sender.id in user_bot_instances and user_bot_instances[sender.id]:
                try:
                    user_bot_instances[sender.id].abort_event.clear()
                except Exception:
                    pass

            # Format the start and end index based on range input
            # Use JSON name as filename
            if range_input.lower() == "all":
                range_message = f"1-{len(json_data)}"
            else:
                range_message = range_input

            queue_message = f"📋 Selected Range: {range_message}\n"
            queue_message += f"Added {len(tasks_to_add)} task(s) from JSON to your queue:\n"
            first_name = tasks_to_add[0]['name']
            task_type_emoji = "🔐" if tasks_to_add[0]['type'] == 'drm' else "📥"
            queue_message += f"Task 1: {task_type_emoji} {first_name}.mp4\n"

            if user_states.get(sender.id, False):
                queue_message += "\nA task is currently being processed. Your tasks will start soon… ⏳"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=queue_message,
                event=event
            )

            # Start queue processor if not running
            if not user_states.get(sender.id, False) and (not user_active_tasks.get(sender.id) or user_active_tasks[sender.id].done()):
                logging.info(f"Starting queue processor for user {sender.id} from /processjson handler")
                bot = MPDLeechBot(sender.id)
                bot._is_json_batch = True # Mark this instance as processing a JSON batch
                user_bot_instances[sender.id] = bot
                user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

        # Don't clear JSON data after processing - keep it for future range selections
        logging.info(f"Processed range {start_idx + 1}-{end_idx} from JSON data for user {sender.id}")

    except Exception as e:
        logging.error(f"ProcessJSON handler error: {str(e)}\n{traceback.format_exc()}")
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ Failed to process JSON: {str(e)}",
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/addthumbnail'))
async def addthumbnail_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /addthumbnail command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    await send_message_with_flood_control(
        entity=event.chat_id,
        message="🖼️ Please send a photo to use as your custom thumbnail.\n\nThe photo will be used for all your future video uploads.",
        event=event
    )

@client.on(events.NewMessage())
async def thumbnail_photo_handler(event):
    """Handle thumbnail photo uploads"""
    sender = await event.get_sender()

    # Only process photos from authorized users
    if sender.id not in authorized_users or not event.photo:
        return

    # Only process if it's actually a photo (not JSON text)
    if event.text and (event.text.strip().startswith('[') or event.text.strip().startswith('{')):
        return  # Let JSON handler process this

    success, message = await save_thumbnail_from_message(event, sender.id)

    if success:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"✅ {message}",
            event=event
        )
    else:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ {message}",
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/removethumbnail'))
async def removethumbnail_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /removethumbnail command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    async with thumbnail_lock:
        if sender.id in user_thumbnails:
            # Remove the thumbnail file
            thumbnail_path = user_thumbnails[sender.id]
            try:
                if os.path.exists(thumbnail_path):
                    os.remove(thumbnail_path)
                del user_thumbnails[sender.id]
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="🗑️ Custom thumbnail removed successfully!",
                    event=event
                )
                logging.info(f"Removed thumbnail for user {sender.id}")
            except Exception as e:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"❌ Error removing thumbnail: {str(e)}",
                    event=event
                )
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="ℹ️ You don't have a custom thumbnail set.",
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/addadmin (\d+)$'))
async def addadmin_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /addadmin command from user {sender.id}")

    # Only allow existing authorized users to add new admins
    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized to add admins.",
            event=event
        )
        return

    user_id = int(event.pattern_match.group(1))

    async with user_lock:
        if user_id not in authorized_users:
            authorized_users.add(user_id)
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ Admin {user_id} has been added with full bot access.",
                event=event
            )
            logging.info(f"Admin {user_id} added to authorized users by {sender.id}")
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"ℹ️ User {user_id} is already an admin.",
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/removeadmin (\d+)$'))
async def removeadmin_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /removeadmin command from user {sender.id}")

    # Only allow existing authorized users to remove admins
    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized to remove admins.",
            event=event
        )
        return

    user_id = int(event.pattern_match.group(1))

    # Prevent removing yourself (safety check)
    if user_id == sender.id:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="❌ You cannot remove yourself as admin.",
            event=event
        )
        return

    async with user_lock:
        if user_id in authorized_users:
            authorized_users.remove(user_id)
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"🗑️ Admin {user_id} has been removed from bot access.",
                event=event
            )
            logging.info(f"Admin {user_id} removed from authorized users by {sender.id}")
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"ℹ️ User {user_id} is not an admin.",
                event=event
            )

async def perform_internet_speed_test():
    """Perform live internet speed test for both download and upload"""
    # Download test URLs
    download_urls = [
        "https://speed.cloudflare.com/__down?bytes=100000000",  # 100MB from Cloudflare
        "https://speed.hetzner.de/100MB.bin",  # Hetzner 100MB test
        "https://proof.ovh.net/files/100Mb.dat",  # OVH test file
        "https://speedtest.tele2.net/100MB.zip",  # Tele2 100MB test
    ]

    # Upload test URLs (these accept POST requests for upload testing)
    upload_urls = [
        "https://httpbin.org/post",  # httpbin accepts POST data
        "https://speed.cloudflare.com/__up",  # Cloudflare upload test
    ]

    test_size = 150 * 1024 * 1024  # up to 150MB to better utilize 1Gbps
    max_test_time = 8  # tighter but enough to sample

    # Test download speed
    download_speed = None
    download_bytes = 0
    download_time = 0

    try:
        for url in download_urls:
            try:
                logging.info(f"Testing download speed with URL: {url}")

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'video/mp4,application/mp4,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Connection': 'keep-alive',
                }

                timeout = aiohttp.ClientTimeout(total=max_test_time + 5)
                start_time = time.time()
                downloaded_bytes = 0

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers, allow_redirects=True) as response:
                        if response.status != 200:
                            continue

                        # Read data in chunks and measure speed
                        async for chunk in response.content.iter_chunked(4 * 1024 * 1024):  # 4MB chunks for higher throughput
                            downloaded_bytes += len(chunk)
                            elapsed = time.time() - start_time

                            # Stop after max test time or when we have enough data
                            if elapsed >= max_test_time or downloaded_bytes >= test_size:
                                break

                elapsed = time.time() - start_time
                if elapsed > 0 and downloaded_bytes > 1024 * 1024:  # At least 1MB downloaded
                    download_speed = downloaded_bytes / elapsed
                    download_bytes = downloaded_bytes
                    download_time = elapsed
                    logging.info(f"Download test successful: {format_size(download_speed)}/s, downloaded {format_size(downloaded_bytes)} in {elapsed:.2f}s")
                    break

            except Exception as e:
                logging.warning(f"Download test failed for {url}: {e}")
                continue

        # If download test failed, try fallback
        if download_speed is None:
            try:
                url = "https://httpbin.org/bytes/10485760"  # 10MB from httpbin
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'video/mp4,application/mp4,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Connection': 'keep-alive',
                }
                timeout = aiohttp.ClientTimeout(total=max_test_time)

                start_time = time.time()
                downloaded_bytes = 0

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                downloaded_bytes += len(chunk)
                                elapsed = time.time() - start_time
                                if elapsed >= max_test_time:
                                    break

                elapsed = time.time() - start_time
                if elapsed > 0 and downloaded_bytes > 0:
                    download_speed = downloaded_bytes / elapsed
                    download_bytes = downloaded_bytes
                    download_time = elapsed

            except Exception as e:
                logging.error(f"Fallback download test also failed: {e}")

    except Exception as e:
        logging.error(f"Download speed test error: {e}")

    # Test upload speed
    upload_speed = None
    upload_bytes = 0
    upload_time = 0

    try:
        # Create test data for upload (10MB)
        upload_data = b'0' * (10 * 1024 * 1024)  # 10MB of zeros

        for url in upload_urls:
            try:
                logging.info(f"Testing upload speed with URL: {url}")

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'video/mp4,application/mp4,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/octet-stream'
                }

                timeout = aiohttp.ClientTimeout(total=max_test_time + 5)
                start_time = time.time()

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, data=upload_data, headers=headers, allow_redirects=True) as response:
                        # Don't care about response status for upload test, just measure upload time
                        elapsed = time.time() - start_time

                        if elapsed > 0:
                            upload_speed = len(upload_data) / elapsed
                            upload_bytes = len(upload_data)
                            upload_time = elapsed
                            logging.info(f"Upload test successful: {format_size(upload_speed)}/s, uploaded {format_size(upload_bytes)} in {elapsed:.2f}s")
                            break

            except Exception as e:
                logging.warning(f"Upload test failed for {url}: {e}")
                continue

        # Fallback upload test with smaller data
        if upload_speed is None:
            try:
                upload_data = b'0' * (5 * 1024 * 1024)  # 5MB fallback
                url = "https://httpbin.org/post"
                headers = {'User-Agent': 'SpeedTest/1.0', 'Content-Type': 'application/octet-stream'}
                timeout = aiohttp.ClientTimeout(total=max_test_time)

                start_time = time.time()
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, data=upload_data, headers=headers) as response:
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            upload_speed = len(upload_data) / elapsed
                            upload_bytes = len(upload_data)
                            upload_time = elapsed

            except Exception as e:
                logging.error(f"Fallback upload test also failed: {e}")

    except Exception as e:
        logging.error(f"Upload speed test error: {e}")

    return download_speed, download_bytes, download_time, upload_speed, upload_bytes, upload_time

@client.on(events.NewMessage(pattern=r'^/(speed|status)$'))
async def speed_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /speed or /status command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    # Send initial message
    status_msg = await send_message_with_flood_control(
        entity=event.chat_id,
        message="🌐 **Internet Speed Test** 🌐\n\n⏳ Testing your internet speed...\n\nPlease wait while we measure your download and upload speeds...",
        event=event
    )

    try:
        # Update message to show download test in progress
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="🌐 **Internet Speed Test** 🌐\n\n📥 Testing download speed...\n\nPlease wait...",
            edit_message=status_msg
        )

        # Perform live internet speed test (both download and upload)
        download_speed, download_bytes, download_time, upload_speed, upload_bytes, upload_time = await perform_internet_speed_test()

        # Update message to show upload test in progress
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="🌐 **Internet Speed Test** 🌐\n\n📤 Testing upload speed...\n\nPlease wait...",
            edit_message=status_msg
        )

        # Process download results
        download_message = ""
        download_rating = ""
        download_emoji = ""

        if download_speed is not None:
            # Convert to different units for better readability
            download_mbps = download_speed / (1024 * 1024)
            download_kbps = download_speed / 1024

            # Determine best unit to display for download
            if download_mbps >= 1:
                download_primary = f"{download_mbps:.2f} MB/s"
                download_secondary = f"({download_kbps:.0f} KB/s)"
            else:
                download_primary = f"{download_kbps:.2f} KB/s"
                download_secondary = f"({download_speed:.0f} B/s)"

            # Create download speed rating
            if download_mbps >= 50:
                download_rating = "🚀 Excellent"
                download_emoji = "🟢"
            elif download_mbps >= 25:
                download_rating = "⚡ Very Good"
                download_emoji = "🟢"
            elif download_mbps >= 10:
                download_rating = "✅ Good"
                download_emoji = "🟡"
            elif download_mbps >= 5:
                download_rating = "📶 Average"
                download_emoji = "🟡"
            elif download_mbps >= 1:
                download_rating = "🐌 Slow"
                download_emoji = "🟠"
            else:
                download_rating = "🦥 Very Slow"
                download_emoji = "🔴"

            download_message = f"📥 **Download:** {download_primary} {download_secondary}\n{download_emoji} **Rating:** {download_rating}\n📦 **Downloaded:** {format_size(download_bytes)}\n⏱️ **Time:** {download_time:.2f}s"
        else:
            download_message = "📥 **Download:** ❌ Failed\n⚠️ Unable to test download speed"

        # Process upload results
        upload_message = ""

        if upload_speed is not None:
            # Convert to different units for better readability
            upload_mbps = upload_speed / (1024 * 1024)
            upload_kbps = upload_speed / 1024

            # Determine best unit to display for upload
            if upload_mbps >= 1:
                upload_primary = f"{upload_mbps:.2f} MB/s"
                upload_secondary = f"({upload_kbps:.0f} KB/s)"
            else:
                upload_primary = f"{upload_kbps:.2f} KB/s"
                upload_secondary = f"({upload_speed:.0f} B/s)"

            # Create upload speed rating
            if upload_mbps >= 25:
                upload_rating = "🚀 Excellent"
                upload_emoji = "🟢"
            elif upload_mbps >= 10:
                upload_rating = "⚡ Very Good"
                upload_emoji = "🟢"
            elif upload_mbps >= 5:
                upload_rating = "✅ Good"
                upload_emoji = "🟡"
            elif upload_mbps >= 2:
                upload_rating = "📶 Average"
                upload_emoji = "🟡"
            elif upload_mbps >= 0.5:
                upload_rating = "🐌 Slow"
                upload_emoji = "🟠"
            else:
                upload_rating = "🦥 Very Slow"
                upload_emoji = "🔴"

            upload_message = f"📤 **Upload:** {upload_primary} {upload_secondary}\n{upload_emoji} **Rating:** {upload_rating}\n📦 **Uploaded:** {format_size(upload_bytes)}\n⏱️ **Time:** {upload_time:.2f}s"
        else:
            upload_message = "📤 **Upload:** ❌ Failed\n⚠️ Unable to test upload speed"

        # Combine results
        speed_message = (
            f"🌐 **Internet Speed Test Results** 🌐\n\n"
            f"{download_message}\n\n"
            f"{upload_message}\n\n"
            f"💡 *Live speed test completed*"
        )

    except Exception as e:
        logging.error(f"Error in speed test for user {sender.id}: {e}")
        speed_message = (
            f"🌐 **Internet Speed Test** 🌐\n\n"
            f"❌ **Speed test error**\n"
            f"⚠️ {str(e)}\n\n"
            f"💡 Try again in a few moments"
        )

    # Check if user has an active task running and add that info
    user_queue, user_states, user_lock = get_user_resources(sender.id)

    if user_states.get(sender.id, False):
        # Try to get current transfer speed from active task
        try:
            bot_inst = user_bot_instances.get(sender.id)
            if bot_inst:
                stage = bot_inst.progress_state.get('stage', 'Initializing')
                current_speed = bot_inst.progress_state.get('speed', 0)
                percent = bot_inst.progress_state.get('percent', 0)
                done = bot_inst.progress_state.get('done_size', 0)
                total = bot_inst.progress_state.get('total_size', 0)
                filename = getattr(bot_inst, 'current_filename', 'Current Task')
                elapsed = bot_inst.progress_state.get('elapsed', 0)

                if stage in ['Downloading']:
                    speed_type = "📥 Active Download"
                    speed_emoji = "⬇️"
                elif stage in ['Uploading']:
                    speed_type = "📤 Active Upload"
                    speed_emoji = "⬆️"
                elif stage == 'Merging':
                    speed_type = "🎬 Merging"
                    speed_emoji = "🎬"
                elif stage == 'Decrypting':
                    speed_type = "🔐 Decrypting"
                    speed_emoji = "🔐"
                else:
                    speed_type = f"🔄 {stage}"
                    speed_emoji = "⚡"

                speed_message += (
                    f"\n\n📊 **Current Task** 📊\n"
                    f"📄 {filename}\n"
                    f"{speed_emoji} **{speed_type}:** {format_size(current_speed)}/s\n"
                    f"📈 **Progress:** {percent:.1f}%\n"
                    f"📦 {format_size(done)} / {format_size(total)}\n"
                    f"⏱️ {format_time(elapsed)}"
                )
            else:
                speed_message += (
                    f"\n\n📊 **Current Task** 📊\n"
                    f"🔄 Task is running (Processing/Merging)\n"
                    f"💡 Transfer speed will show during download/upload"
                )
        except Exception as e:
            logging.warning(f"Could not get active task speed: {e}")

    # Update the status message with results
    await send_message_with_flood_control(
        entity=event.chat_id,
        message=speed_message,
        edit_message=status_msg
    )

@client.on(events.NewMessage(pattern=r'^/bulk'))
async def bulk_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /bulk command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    # Initialize bulk data storage for user
    async with bulk_lock:
        user_bulk_data[sender.id] = []

    await send_message_with_flood_control(
        entity=event.chat_id,
        message="📦 **Bulk JSON Processing** 📦\n\nSend multiple JSON files or JSON text messages. Each JSON will be processed completely before starting the next one.\n\n**Usage:**\n1. Send multiple JSON files/text\n2. Use `/processbulk` to start sequential processing\n3. Use `/clearbulk` to clear stored JSON data\n\n**Example Format:**\n```json\n[\n  {\n    \"name\": \"Video1\",\n    \"type\": \"drm\",\n    \"mpd_url\": \"https://example.com/manifest1.mpd\",\n    \"keys\": [\"kid:key\"]\n  },\n  {\n    \"name\": \"MyMovie\",\n    \"type\": \"direct\",\n    \"url\": \"https://example.com/mymovie.mp4\"\n  }\n]\n```\n\nReady to receive JSON data! 🚀",
        event=event
    )

@client.on(events.NewMessage(pattern=r'^/processbulk'))
async def processbulk_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /processbulk command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    async with bulk_lock:
        if sender.id not in user_bulk_data or not user_bulk_data[sender.id]:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="❌ No bulk JSON data found. Use /bulk and send JSON files/text first.",
                event=event
            )
            return

        bulk_data_list = user_bulk_data[sender.id]
        total_jsons = len(bulk_data_list)

    # Check if user already has tasks running
    user_queue, user_states, user_lock = get_user_resources(sender.id)
    if user_states.get(sender.id, False):
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="❌ You already have tasks running. Use /clearall first or wait for completion.",
            event=event
        )
        return

    await send_message_with_flood_control(
        entity=event.chat_id,
        message=f"🚀 **Starting Bulk Processing** 🚀\n\n"
               f"📦 Processing {total_jsons} JSON files sequentially\n"
               f"⏳ Each JSON will be completed before starting the next\n\n"
               f"Processing will begin shortly...",
        event=event
    )

    # Process each JSON sequentially
    for json_index, json_data in enumerate(bulk_data_list, 1):
        try:
            # Notify user about current JSON with task type summary
            await send_message_with_flood_control(entity=event.chat_id, message=f"📋 **JSON {json_index}/{total_jsons}** - Starting processing of {len(json_data)} items", event=event)

            # Add all tasks from current JSON to queue
            tasks_to_add = []
            for item in json_data:
                try:
                    # Support both 'name' and 'video_name' fields
                    name = item.get('video_name') or item.get('name', f'Video_{json_index}_{len(tasks_to_add)+1}')

                    # Auto-detect content type based on available fields
                    if item.get('mpd_url') and item.get('keys'):
                        # DRM content detected
                        mpd_url = item.get('mpd_url')
                        keys = item.get('keys', [])
                        key = keys[0] if isinstance(keys, list) else keys

                        task = {
                            'type': 'drm',
                            'mpd_url': mpd_url,
                            'key': key,
                            'name': name,
                            'sender': sender
                        }
                        tasks_to_add.append(task)
                        logging.info(f"Added DRM task from JSON: {name}")

                    elif item.get('url'):
                        # Direct content detected
                        url = item.get('url')
                        task = {
                            'type': 'direct',
                            'url': url,
                            'name': name,
                            'sender': sender
                        }
                        tasks_to_add.append(task)
                        logging.info(f"Added direct task from JSON: {name}")

                    else:
                        # Fallback to explicit type checking
                        item_type = item.get('type', 'drm').lower()
                        if item_type == 'drm':
                            mpd_url = item.get('mpd_url')
                            keys = item.get('keys', [])
                            if mpd_url and keys:
                                key = keys[0] if isinstance(keys, list) else keys
                                task = {
                                    'type': 'drm',
                                    'mpd_url': mpd_url,
                                    'key': key,
                                    'name': name,
                                    'sender': sender
                                }
                                tasks_to_add.append(task)
                                logging.info(f"Added DRM task (explicit) from JSON: {name}")
                        elif item_type == 'direct':
                            url = item.get('url')
                            if url:
                                task = {
                                    'type': 'direct',
                                    'url': url,
                                    'name': name,
                                    'sender': sender
                                }
                                tasks_to_add.append(task)
                                logging.info(f"Added direct task (explicit) from JSON: {name}")

                    if not tasks_to_add or len(tasks_to_add) == 0 or tasks_to_add[-1]['name'] != name:
                        invalid_items.append(f"Item {i}: No valid content detected (need mpd_url+keys or url)")
                except Exception as e:
                    logging.warning(f"Skipping invalid item in JSON {json_index}: {e}")

            if not tasks_to_add:
                await send_message_with_flood_control(entity=event.chat_id, message=f"⚠️ JSON {json_index}/{total_jsons} - No valid items found, skipping", event=event)
                continue

            # Add tasks to queue
            async with user_lock:
                for task in tasks_to_add:
                    user_queue.append(task)

            # Start processing this JSON and wait for completion
            if not user_states.get(sender.id, False) and (not user_active_tasks.get(sender.id) or user_active_tasks[sender.id].done()):
                bot = MPDLeechBot(sender.id)
                bot._is_json_batch = True # Mark this instance as processing a JSON batch
                user_bot_instances[sender.id] = bot
                user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

            # Wait for this JSON to complete before starting next
            while user_states.get(sender.id, False) or (user_active_tasks.get(sender.id) and not user_active_tasks[sender.id].done()):
                await asyncio.sleep(5)

            # Send completion message for this JSON
            await send_message_with_flood_control(entity=event.chat_id, message=f"✅ **JSON {json_index}/{total_jsons} Completed!** All {len(tasks_to_add)} tasks processed.\n\n{'🎉 All JSONs completed!' if json_index == total_jsons else f'⏭️ Moving to JSON {json_index + 1}/{total_jsons}...'}", event=event)

        except Exception as e:
            logging.error(f"Error processing JSON {json_index} for user {sender.id}: {e}")
            await send_message_with_flood_control(entity=event.chat_id, message=f"❌ **JSON {json_index}/{total_jsons} Failed:** {str(e)}\n\n{'Moving to next JSON...' if json_index < total_jsons else 'Bulk processing completed with errors.'}", event=event)

    # Final completion message
    await send_message_with_flood_control(
        entity=event.chat_id,
        message=f"🎊 **Bulk Processing Complete!** 🎊\n\n"
               f"✅ Processed {total_jsons} JSON files\n"
               f"🚀 All tasks completed successfully!\n\n"
               f"You can now start new tasks or use /bulk again for more JSON files.",
        event=event
    )

@client.on(events.NewMessage(pattern=r'^/clearbulk'))
async def clearbulk_handler(event):
    sender = await event.get_sender()
    logging.info(f"Received /clearbulk command from user {sender.id}")

    if sender.id not in authorized_users:
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="You're not authorized.",
            event=event
        )
        return

    async with bulk_lock:
        if sender.id in user_bulk_data:
            cleared_count = len(user_bulk_data[sender.id])
            del user_bulk_data[sender.id]
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"🧹 Cleared {cleared_count} stored JSON files from bulk processing.",
                event=event
            )
            logging.info(f"Cleared bulk JSON data for user {sender.id}: {cleared_count} files")
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="ℹ️ No bulk JSON data found to clear.",
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/(skip)(?:\s+(\d+)(?:-(\d+))?)?$'))
async def skip_handler(event):
    sender = await event.get_sender()
    if sender.id not in authorized_users:
        return

    user_queue, user_states, user_lock = get_user_resources(sender.id)

    try:
        start = event.pattern_match.group(2)
        end = event.pattern_match.group(3)

        if not start:
            # Skip current task
            if sender.id in user_active_tasks and user_active_tasks[sender.id] and not user_active_tasks[sender.id].done():
                bot_inst = user_bot_instances.get(sender.id)
                if bot_inst:
                    bot_inst.abort_event.set()
                user_active_tasks[sender.id].cancel()
                await send_message_with_flood_control(entity=event.chat_id, message="⏭️ Skipping current task...", event=event)
            else:
                await send_message_with_flood_control(entity=event.chat_id, message="ℹ️ No active task to skip.", event=event)
            return

        # Skip a range or single index from the queue (1-based positions relative to queue head)
        start_idx = int(start)
        end_idx = int(end) if end else start_idx
        if start_idx <= 0 or end_idx < start_idx:
            await send_message_with_flood_control(entity=event.chat_id, message="❌ Invalid range for /skip. Use /skip or /skip 3-5", event=event)
            return

        removed = []
        async with user_lock:
            # Convert to 0-based indices
            new_queue = []
            for pos, task in enumerate(list(user_queue), start=1):
                if start_idx <= pos <= end_idx:
                    removed.append(task.get('name', 'unknown'))
                else:
                    new_queue.append(task)
            user_queue.clear()
            user_queue.extend(new_queue)

        if removed:
            await send_message_with_flood_control(entity=event.chat_id, message=f"🗑️ Skipped tasks: {', '.join(removed)}", event=event)
        else:
            await send_message_with_flood_control(entity=event.chat_id, message="ℹ️ No tasks matched the skip range.", event=event)
    except Exception as e:
        logging.error(f"/skip error for user {sender.id}: {e}")
        await send_message_with_flood_control(entity=event.chat_id, message=f"❌ Error processing /skip: {str(e)}", event=event)

# Main function to start the bot
async def main():
    global admin_client
    
    while True:
        try:
            # Initialize and start admin client if session string is available
            if SESSION_STRING and SESSION_STRING.strip():
                try:
                    session = StringSession(SESSION_STRING.strip())
                    admin_client = TelegramClient(session, API_ID, API_HASH, connection_retries=5, auto_reconnect=True)
                    await admin_client.start()
                    admin_me = await admin_client.get_me()
                    print(f"Admin session ready: @{admin_me.username if hasattr(admin_me, 'username') and admin_me.username else admin_me.id}")
                except Exception as e:
                    print(f"Admin session failed: {e}")
                    admin_client = None

            # Start bot client
            await client.start(bot_token=BOT_TOKEN)
            me = await client.get_me()
            print(f"Bot ready: @{me.username if hasattr(me, 'username') and me.username else me.id}")

            await client.run_until_disconnected()
        except Exception as e:
            print(f"Bot crashed: {e}, restarting...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())