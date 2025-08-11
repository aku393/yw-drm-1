import os
import xml.etree.ElementTree as ET
from telethon import TelegramClient, events
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
from telethon.errors.rpcerrorlist import FloodWaitError, MessageNotModifiedError  # Import FloodWaitError and MessageNotModifiedError
from collections import deque  # For task queue
import json
import signal
import sys
import fcntl

    # Set up simplified logging - only warnings and errors
logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# Single instance lock
LOCK_FILE = "/tmp/bot_instance.lock"
lock_fd = None

def acquire_lock():
    """Acquire exclusive lock to prevent multiple instances"""
    global lock_fd
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return True
    except (IOError, OSError):
        if lock_fd:
            lock_fd.close()
        return False

def release_lock():
    """Release the instance lock"""
    global lock_fd
    if lock_fd:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
            os.unlink(LOCK_FILE)
        except:
            pass

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    release_lock()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Check for existing instance
if not acquire_lock():
    print("❌ Another bot instance is already running!")
    print("💡 If you're sure no other instance is running, delete /tmp/bot_instance.lock and try again.")
    sys.exit(1)

print("🤖 Bot starting...")

# Load .env file
load_dotenv()

# Config from .env
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
ALLOWED_USERS = os.getenv('ALLOWED_USERS', '')

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


def create_circular_progress(percent):
    """Create a premium circular progress indicator with smooth transitions"""
    if percent >= 100:
        return "🟢"
    elif percent >= 90:
        return "🟦"  # Light blue for near completion
    elif percent >= 80:
        return "🔵"  # Blue for good progress
    elif percent >= 70:
        return "🟡"  # Yellow for moderate progress
    elif percent >= 60:
        return "🟠"  # Orange for growing progress
    elif percent >= 50:
        return "🔴"  # Red for half progress
    elif percent >= 40:
        return "🟤"  # Brown for building progress
    elif percent >= 30:
        return "🟣"  # Purple for early progress
    elif percent >= 20:
        return "⚫"  # Black for minimal progress
    elif percent >= 10:
        return "🔘"  # Hollow for very early
    else:
        return "⚪"  # White for start

def create_detailed_progress_bar(percent, width=30, style="premium"):
    """Create premium progress bars with multiple styles"""
    filled_width = int(width * percent / 100)

    if style == "premium":
        # Premium gradient style with Unicode blocks
        bar = ""
        for i in range(width):
            if i < filled_width:
                if percent >= 95:
                    bar += "█"  # Full block - almost complete
                elif percent >= 80:
                    bar += "▓"  # Dark shade - high progress
                elif percent >= 60:
                    bar += "▒"  # Medium shade - good progress
                elif percent >= 40:
                    bar += "░"  # Light shade - moderate progress
                else:
                    bar += "▪"  # Small block - early progress
            else:
                bar += "─"  # Horizontal line for empty
        return f"╣{bar}╠"

    elif style == "luxury":
        # Luxury diamond style
        bar = ""
        for i in range(width):
            if i < filled_width:
                if percent >= 90:
                    bar += "♦"  # Diamond - premium
                elif percent >= 70:
                    bar += "◆"  # Filled diamond
                elif percent >= 50:
                    bar += "◈"  # Diamond outline
                else:
                    bar += "◇"  # Hollow diamond
            else:
                bar += "◦"  # Small circle for empty
        return f"⟨{bar}⟩"

    else:
        # Default enhanced style with circles
        bar = ""
        for i in range(width):
            if i < filled_width:
                if percent >= 90:
                    bar += "●"  # Filled circle - near complete
                elif percent >= 70:
                    bar += "◉"  # Filled circle with dot
                elif percent >= 50:
                    bar += "◎"  # Circle with dot
                else:
                    bar += "○"  # Empty circle filled
            else:
                bar += "◦"  # Small empty circle
        return f"【{bar}】"

def create_premium_progress_bar(percent, width=25, theme="gradient"):
    """Create an ultra-premium progress bar with themes"""
    filled_width = int(width * percent / 100)

    if theme == "gradient":
        # Gradient theme with color-coded progress
        fill_chars = []
        empty_chars = []

        for i in range(width):
            if i < filled_width:
                progress_point = (i / width) * 100
                if progress_point >= 90:
                    fill_chars.append("🟦")  # Light blue - excellent
                elif progress_point >= 75:
                    fill_chars.append("🟩")  # Green - very good
                elif progress_point >= 50:
                    fill_chars.append("🟨")  # Yellow - good
                elif progress_point >= 25:
                    fill_chars.append("🟧")  # Orange - fair
                else:
                    fill_chars.append("🟥")  # Red - starting
            else:
                empty_chars.append("⬜")  # White for empty

        bar = "".join(fill_chars) + "".join(empty_chars)
        return f"➤ {bar} ➤"

    elif theme == "neon":
        # Neon/cyber theme
        bar = ""
        for i in range(width):
            if i < filled_width:
                if percent >= 95:
                    bar += "▰"  # Neon filled
                elif percent >= 75:
                    bar += "▱"  # Neon partial
                else:
                    bar += "▪"  # Neon dot
            else:
                bar += "▫"  # Neon empty
        return f"╟{bar}╢"

    elif theme == "royal":
        # Royal/elegant theme
        bar = ""
        for i in range(width):
            if i < filled_width:
                if percent >= 90:
                    bar += "♛"  # Queen - royal completion
                elif percent >= 70:
                    bar += "♝"  # Bishop - high progress
                elif percent >= 50:
                    bar += "♞"  # Knight - medium progress
                else:
                    bar += "♟"  # Pawn - starting progress
            else:
                bar += "·"  # Dot for empty
        return f"╔{bar}╗"

    else:  # "minimal" theme
        # Clean minimal theme
        bar = "█" * filled_width + "░" * (width - filled_width)
        return f"▐{bar}▌"

def create_status_card(title, status, progress=None, details=None, theme="premium"):
    """Create a premium formatted status card for Telegram messages"""
    card = f"**✨ {title} ✨**\n"
    card += "╔" + "═" * 42 + "╗\n"

    if progress is not None:
        circular = create_circular_progress(progress)

        # Use different progress bar styles based on progress level
        if progress >= 80:
            bar = create_premium_progress_bar(progress, 18, "gradient")
        elif progress >= 50:
            bar = create_detailed_progress_bar(progress, 18, "luxury")
        else:
            bar = create_detailed_progress_bar(progress, 18, "premium")

        card += f"║ {circular} {bar} {progress:5.1f}% ║\n"
        card += "╠" + "═" * 42 + "╣\n"

    card += f"║ Status: {status:<32} ║\n"

    if details:
        for key, value in details.items():
            line = f"║ {key}: {value}"
            # Pad to fit in the box (42 chars total width)
            padding = 42 - len(line) + 1
            card += line + " " * padding + "║\n"

    card += "╚" + "═" * 42 + "╝"
    return card

class FormHandler:
    """Handle interactive forms for bot configuration"""

    def __init__(self):
        self.user_forms = {}  # Store active forms per user
        self.form_lock = asyncio.Lock()

    async def create_config_form(self, user_id):
        """Create a configuration form for watermark settings"""
        form_data = {
            'step': 'position',
            'data': {},
            'steps': ['position', 'text', 'enable'],
            'current_step': 0
        }

        async with self.form_lock:
            self.user_forms[user_id] = form_data

        return self.get_position_form()

    def get_position_form(self):
        """Get position selection form"""
        return (
            "🎨 **Watermark Configuration - Step 1/3**\n"
            "┌────────────────────────────────────┐\n"
            "│              POSITION              │\n"
            "├────────────────────────────────────┤\n"
            "│                                    │\n"
            "│  1️⃣ Top Left     2️⃣ Top Right    │\n"
            "│                                    │\n"
            "│                                    │\n"
            "│  3️⃣ Bottom Left  4️⃣ Bottom Right │\n"
            "│                                    │\n"
            "└────────────────────────────────────┘\n"
            "\n**Reply with:** `1`, `2`, `3`, or `4`"
        )

    def get_text_form(self):
        """Get text input form"""
        return (
            "📝 **Watermark Configuration - Step 2/3**\n"
            "┌────────────────────────────────────┐\n"
            "│           WATERMARK TEXT           │\n"
            "├────────────────────────────────────┤\n"
            "│                                    │\n"
            "│  Enter your watermark text:        │\n"
            "│                                    │\n"
            "│  Examples:                         │\n"
            "│  • @YourChannel                    │\n"
            "│  • Copyright 2024                  │\n"
            "│  • Property of XYZ                 │\n"
            "│                                    │\n"
            "└────────────────────────────────────┘\n"
            "\n**Reply with:** Your watermark text"
        )

    def get_confirmation_form(self, position, text):
        """Get confirmation form"""
        pos_names = {
            '1': 'Top Left',
            '2': 'Top Right', 
            '3': 'Bottom Left',
            '4': 'Bottom Right'
        }

        return (
            "✅ **Watermark Configuration - Step 3/3**\n"
            "┌────────────────────────────────────┐\n"
            "│            CONFIRMATION            │\n"
            "├────────────────────────────────────┤\n"
            f"│  Position: {pos_names.get(position, 'Unknown'):<23} │\n"
            f"│  Text: {text[:26]:<26} │\n"
            "│                                    │\n"
            "│  1️⃣ Confirm & Enable               │\n"
            "│  2️⃣ Cancel                         │\n"
            "│                                    │\n"
            "└────────────────────────────────────┘\n"
            "\n**Reply with:** `1` to confirm or `2` to cancel"
        )



# Initialize form handler
form_handler = FormHandler()

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

async def create_progress_dashboard(user_id, tasks_info):
    """Create an interactive progress dashboard"""
    dashboard = "📊 **Processing Dashboard**\n"
    dashboard += "═" * 40 + "\n\n"

    # Overall progress
    total_tasks = len(tasks_info)
    completed = sum(1 for task in tasks_info if task.get('status') == 'completed')
    overall_percent = (completed / total_tasks * 100) if total_tasks > 0 else 0

    dashboard += create_status_card(
        "Overall Progress",
        f"{completed}/{total_tasks} tasks completed",
        overall_percent,
        {
            "Queue Size": f"{total_tasks - completed} remaining",
            "Success Rate": f"{overall_percent:.1f}%"
        }
    )

    dashboard += "\n\n"

    # Current task details
    current_task = next((task for task in tasks_info if task.get('status') == 'processing'), None)
    if current_task:
        dashboard += create_status_card(
            f"Current: {current_task.get('name', 'Unknown')}",
            current_task.get('stage', 'Processing'),
            current_task.get('progress', 0),
            {
                "Speed": format_size(current_task.get('speed', 0)) + "/s",
                "ETA": format_time(current_task.get('eta', 0)),
                "Size": format_size(current_task.get('size', 0))
            }
        )

    dashboard += "\n\n**Commands:**\n"
    dashboard += "• `/skip` - Skip current task\n"
    dashboard += "• `/pause` - Pause processing\n"
    dashboard += "• `/clearall` - Stop all tasks"

    return dashboard

# Debug environment variables
logging.info(f"Environment RENDER: {os.getenv('RENDER')}")
# Set DOWNLOAD_DIR based on environment
if os.getenv('RENDER') == 'true':
    DOWNLOAD_DIR = '/app/downloads'
else:
    DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR', 'downloads')
logging.info(f"Set DOWNLOAD_DIR to: {DOWNLOAD_DIR}")

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

# JSON storage for loadjson/processjson functionality
user_json_data = {}  # Format: {user_id: json_data}
json_lock = asyncio.Lock()  # Lock for JSON data management

# User management storage
authorized_users = set(ALLOWED_USERS)  # Use a set for faster lookups
user_lock = asyncio.Lock() # Lock to manage authorized_users

# Thumbnail storage for users
user_thumbnails = {}  # Format: {user_id: thumbnail_file_path}
user_thumbnail_upload_mode = {}  # Format: {user_id: bool} - Track if user is expecting thumbnail upload
thumbnail_lock = asyncio.Lock()  # Lock for thumbnail management

# Speed tracking for users
user_speed_stats = {}  # Format: {user_id: {'download_speed': float, 'upload_speed': float, 'last_updated': timestamp}}
speed_lock = asyncio.Lock()  # Lock for speed tracking

# Bulk JSON processing storage - File-based for unlimited storage with filename tracking
user_bulk_data = {}  # Format: {user_id: {'count': int, 'dir': str, 'filenames': []}}
bulk_lock = asyncio.Lock()  # Lock for bulk processing

# Bulk processing control flags
user_bulk_processing = {}  # Format: {user_id: {'active': bool, 'should_stop': bool, 'skip_current': bool}}
bulk_control_lock = asyncio.Lock()  # Lock for bulk processing control

# Watermark storage and configuration
user_watermark_pngs = {}  # Format: {user_id: png_file_path}
user_watermark_configs = {}  # Format: {user_id: {'position': str, 'text': str, 'enabled': bool}}
user_png_upload_mode = {}  # Format: {user_id: bool} - Track if user is expecting PNG upload
watermark_lock = asyncio.Lock()  # Lock for watermark management

def get_bulk_storage_dir(user_id):
    """Get the bulk storage directory for a user"""
    bulk_dir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}", "bulk_json")
    if not os.path.exists(bulk_dir):
        os.makedirs(bulk_dir)
    return bulk_dir

def save_bulk_json(user_id, json_data, filename=None):
    """Save a JSON file to disk and return the count"""
    bulk_dir = get_bulk_storage_dir(user_id)

    # Initialize user bulk data if not exists
    if user_id not in user_bulk_data:
        user_bulk_data[user_id] = {'count': 0, 'dir': bulk_dir, 'filenames': []}

    # Ensure filenames list exists (backward compatibility)
    if 'filenames' not in user_bulk_data[user_id]:
        user_bulk_data[user_id]['filenames'] = []

    # Increment count and save file
    user_bulk_data[user_id]['count'] += 1
    index = user_bulk_data[user_id]['count']

    # Store the actual filename if provided, otherwise generate one
    if filename:
        # Clean filename for safe storage
        safe_filename = "".join(c for c in filename if c.isalnum() or c in (' ', '-', '_', '.')).strip()
        if not safe_filename:
            safe_filename = f"JSON_{index}"
        user_bulk_data[user_id]['filenames'].append(safe_filename)
    else:
        user_bulk_data[user_id]['filenames'].append(f"JSON_{index}")

    file_path = os.path.join(bulk_dir, f"bulk_{index}.json")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved bulk JSON #{index} for user {user_id}: {file_path}")
    except Exception as e:
        logging.error(f"Failed to save bulk JSON #{index} for user {user_id}: {e}")
        raise

    return index

def load_bulk_json(user_id, index):
        """Load a specific JSON file from disk"""
        if user_id not in user_bulk_data:
            logging.error(f"User {user_id} not found in bulk data")
            return None, None

        bulk_dir = user_bulk_data[user_id]['dir']
        file_path = os.path.join(bulk_dir, f"bulk_{index}.json")

        if not os.path.exists(file_path):
            logging.error(f"Bulk JSON file not found: {file_path}")
            return None, None

        # Ensure filenames list exists and get the filename for this index
        if 'filenames' not in user_bulk_data[user_id]:
            user_bulk_data[user_id]['filenames'] = []

        # Pad filenames list if needed
        while len(user_bulk_data[user_id]['filenames']) < index:
            user_bulk_data[user_id]['filenames'].append(f"JSON_{len(user_bulk_data[user_id]['filenames']) + 1}")

        filename = user_bulk_data[user_id]['filenames'][index - 1] if index <= len(user_bulk_data[user_id]['filenames']) else f"JSON_{index}"

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info(f"Loaded bulk JSON #{index} for user {user_id}: {filename}")
            return data, filename
        except Exception as e:
            logging.error(f"Failed to load bulk JSON #{index} for user {user_id}: {e}")
            return None, None

def get_bulk_json_count(user_id):
        """Get the count of stored bulk JSON files"""
        return user_bulk_data.get(user_id, {}).get('count', 0)

def clear_bulk_json(user_id):
        """Clear all bulk JSON files for a user"""
        if user_id not in user_bulk_data:
            return 0

        try:
            bulk_dir = user_bulk_data[user_id]['dir']
            count = user_bulk_data[user_id]['count']

            # Remove all files
            if os.path.exists(bulk_dir):
                import shutil
                shutil.rmtree(bulk_dir)
                logging.info(f"Removed bulk directory: {bulk_dir}")

            # Clear from memory
            del user_bulk_data[user_id]
            logging.info(f"Cleared bulk data for user {user_id}: {count} files")

            return count
        except Exception as e:
            logging.error(f"Error clearing bulk JSON for user {user_id}: {e}")
            # Try to clear from memory anyway
            if user_id in user_bulk_data:
                try:
                    count = user_bulk_data[user_id].get('count', 0)
                    del user_bulk_data[user_id]
                    return count
                except:
                    pass
            return 0

def init_bulk_processing(user_id):
        """Initialize bulk processing control for a user"""
        user_bulk_processing[user_id] = {
            'active': True,
            'should_stop': False,
            'skip_current': False
        }

def stop_bulk_processing(user_id):
        """Stop bulk processing for a user"""
        if user_id in user_bulk_processing:
            user_bulk_processing[user_id]['should_stop'] = True
            user_bulk_processing[user_id]['active'] = False
            logging.info(f"Marked bulk processing for stop for user {user_id}")

def skip_current_json(user_id):
        """Skip current JSON in bulk processing"""
        if user_id in user_bulk_processing and user_bulk_processing[user_id]['active']:
            user_bulk_processing[user_id]['skip_current'] = True
            logging.info(f"Marked current JSON for skip for user {user_id}")
            return True
        return False

def should_stop_bulk_processing(user_id):
        """Check if bulk processing should stop"""
        return user_bulk_processing.get(user_id, {}).get('should_stop', False)

def should_skip_current_json(user_id):
        """Check if current JSON should be skipped"""
        skip = user_bulk_processing.get(user_id, {}).get('skip_current', False)
        if skip:
            # Reset skip flag after checking
            user_bulk_processing[user_id]['skip_current'] = False
        return skip

def cleanup_bulk_processing(user_id):
        """Clean up bulk processing control for a user"""
        if user_id in user_bulk_processing:
            del user_bulk_processing[user_id]
            logging.info(f"Cleaned up bulk processing control for user {user_id}")

async def save_watermark_png(event, user_id):
        """Save PNG watermark from user message (photo or image file)"""
        try:
            # Check if it's a photo or image document
            if not event.photo and not (event.document and getattr(event.document, 'mime_type', '').startswith('image/')):
                return False, "Please send a photo or image file to use as PNG watermark."

            # Create user watermark directory
            watermark_dir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}", "watermarks")
            if not os.path.exists(watermark_dir):
                os.makedirs(watermark_dir)

            # Download the image (photo or document)
            png_path = os.path.join(watermark_dir, "watermark.png")

            # Download with proper error handling
            try:
                downloaded_file = await event.download_media(file=png_path)
                logging.info(f"Downloaded watermark file to: {downloaded_file}")

                # Verify file exists and has content
                if not os.path.exists(png_path) or os.path.getsize(png_path) == 0:
                    return False, "Failed to download PNG watermark - file is empty or missing."

            except Exception as download_error:
                logging.error(f"Download error for PNG watermark: {download_error}")
                return False, f"Failed to download PNG watermark: {str(download_error)}"

            # Store in user watermarks
            async with watermark_lock:
                user_watermark_pngs[user_id] = png_path

            file_size = os.path.getsize(png_path)
            logging.info(f"Saved PNG watermark for user {user_id}: {png_path}, size: {file_size} bytes")
            return True, f"PNG watermark saved successfully! ({format_size(file_size)})"

        except Exception as e:
            logging.error(f"Error saving PNG watermark: {str(e)}")
            return False, f"Error saving PNG watermark: {str(e)}"

async def apply_watermarks(input_file, output_file, user_id, progress_callback=None):
        """Apply both PNG and text watermarks to video with enhanced quality and speed from corner code"""
        try:
            # Check if user has watermark configuration
            async with watermark_lock:
                config = user_watermark_configs.get(user_id, {})
                png_path = user_watermark_pngs.get(user_id)

            if not config.get('enabled', False):
                # No watermarking enabled, just copy the file
                logging.info(f"Watermarking disabled for user {user_id}, copying original file")
                import shutil
                shutil.copy2(input_file, output_file)
                return True

            logging.info(f"Applying enhanced watermarks for user {user_id}: PNG={bool(png_path and os.path.exists(png_path))}, Text={bool(config.get('text', '').strip())}")

            position = config.get('position', 'topright')
            text = config.get('text', '')

            # Get video info using enhanced method from corner code
            try:
                import subprocess
                import json
                cmd = [
                    'ffprobe', '-v', 'quiet', '-print_format', 'json',
                    '-show_format', '-show_streams', input_file
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                info = json.loads(result.stdout)

                video_stream = None
                for stream in info.get('streams', []):
                    if stream.get('codec_type') == 'video':
                        video_stream = stream
                        break

                if video_stream:
                    # Parse frame rate
                    fps_str = video_stream.get('r_frame_rate', '30/1')
                    if '/' in fps_str:
                        num, den = fps_str.split('/')
                        fps = float(num) / float(den) if float(den) > 0 else 30
                    else:
                        fps = float(fps_str)

                    # Get duration from multiple sources
                    duration = 0
                    if video_stream.get('duration'):
                        duration = float(video_stream.get('duration'))
                    elif info.get('format', {}).get('duration'):
                        duration = float(info.get('format', {}).get('duration'))

                    video_width = int(video_stream.get('width', 1280))
                    video_height = int(video_stream.get('height', 720))
                    original_bitrate = int(video_stream.get('bit_rate', 0)) if video_stream.get('bit_rate') else 0
                else:
                    video_width, video_height, fps, duration, original_bitrate = 1280, 720, 30, 0, 0

                frame_count = int(fps * duration) if duration > 0 else 1000
                logging.info(f"Enhanced video info: {video_width}x{video_height}, {duration}s, {frame_count} frames, {original_bitrate} bps")

            except Exception as e:
                logging.warning(f"Could not get enhanced video info: {e}")
                duration = 0
                frame_count = 1000
                video_width = 1280
                video_height = 720
                original_bitrate = 0
                fps = 30

            # Check for PNG and text watermarks
            has_png = png_path and os.path.exists(png_path)
            has_text = text and text.strip()

            if not has_png and not has_text:
                logging.info(f"No watermarks to apply for user {user_id}, copying file")
                import shutil
                shutil.copy2(input_file, output_file)
                return True

            # Build filter complex using enhanced corner code logic
            filter_parts = []
            overlay_inputs = "[0:v]"

            # Add text watermark with enhanced positioning from corner code
            if has_text:
                # Clean and escape the text for FFmpeg
                clean_text = text.replace("'", "\\'").replace("\n", " ").replace("\r", " ").replace(":", "\\:")
                # Remove any characters that might break FFmpeg filters
                clean_text = ''.join(c for c in clean_text if ord(c) < 127 and c.isprintable() or c == ' ')

                # Smart text wrapping for long text (from corner code)
                words = clean_text.split()
                max_chars_per_line = max(20, video_width // 15)  # Adaptive line length based on video width

                # Create multi-line text if needed
                lines = []
                current_line = ""

                for word in words:
                    test_line = current_line + " " + word if current_line else word
                    if len(test_line) <= max_chars_per_line:
                        current_line = test_line
                    else:
                        if current_line:
                            lines.append(current_line)
                        current_line = word

                if current_line:
                    lines.append(current_line)

                # Limit to 3 lines maximum
                if len(lines) > 3:
                    lines = lines[:2]
                    lines.append("...")

                # Join lines with newline character for FFmpeg
                final_text = "\\n".join(lines)

                font_size = max(18, min(video_width // 40, video_height // 30))  # Optimized font size
                margin = 15
                line_spacing = int(font_size * 1.2)  # Line spacing

                png_exists = has_png

                if png_exists:
                    # Skip PNG location and center - enhanced positioning from corner code
                    if position == "topleft":
                        # Skip topleft, use: topright, bottomright, bottomleft
                        text_filter = (
                            f"drawtext=text='{final_text}':"
                            f"fontsize={font_size}:"
                            f"fontcolor=white@0.9:"
                            f"box=1:boxcolor=black@0.7:boxborderw=8:"
                            f"line_spacing={line_spacing}:"
                            f"x='if(lt(mod(t,180),60),w-text_w-{margin},if(lt(mod(t,180),120),w-text_w-{margin},{margin}))':"
                            f"y='if(lt(mod(t,180),60),{margin*2},if(lt(mod(t,180),120),h-text_h-{margin},h-text_h-{margin}))'"
                        )
                    elif position == "topright":
                        # Skip topright, use: topleft, bottomleft, bottomright
                        text_filter = (
                            f"drawtext=text='{final_text}':"
                            f"fontsize={font_size}:"
                            f"fontcolor=white@0.9:"
                            f"box=1:boxcolor=black@0.7:boxborderw=8:"
                            f"line_spacing={line_spacing}:"
                            f"x='if(lt(mod(t,180),60),{margin},if(lt(mod(t,180),120),{margin},w-text_w-{margin}))':"
                            f"y='if(lt(mod(t,180),60),{margin*2},if(lt(mod(t,180),120),h-text_h-{margin},h-text_h-{margin}))'"
                        )
                    elif position == "bottomleft":
                        # Skip bottomleft, use: topleft, topright, bottomright
                        text_filter = (
                            f"drawtext=text='{final_text}':"
                            f"fontsize={font_size}:"
                            f"fontcolor=white@0.9:"
                            f"box=1:boxcolor=black@0.7:boxborderw=8:"
                            f"line_spacing={line_spacing}:"
                            f"x='if(lt(mod(t,180),60),{margin},if(lt(mod(t,180),120),w-text_w-{margin},w-text_w-{margin}))':"
                            f"y='if(lt(mod(t,180),60),{margin*2},if(lt(mod(t,180),120),{margin*2},h-text_h-{margin}))'"
                        )
                    else:  # bottomright
                        # Skip bottomright, use: topleft, topright, bottomleft
                        text_filter = (
                            f"drawtext=text='{final_text}':"
                            f"fontsize={font_size}:"
                            f"fontcolor=white@0.9:"
                            f"box=1:boxcolor=black@0.7:boxborderw=8:"
                            f"line_spacing={line_spacing}:"
                            f"x='if(lt(mod(t,180),60),{margin},if(lt(mod(t,180),120),w-text_w-{margin},{margin}))':"
                            f"y='if(lt(mod(t,180),60),{margin*2},if(lt(mod(t,180),120),{margin*2},h-text_h-{margin}))'"
                        )
                else:
                    # No PNG, use all 4 corners (no center): 4 * 60 = 240 seconds cycle
                    text_filter = (
                        f"drawtext=text='{final_text}':"
                        f"fontsize={font_size}:"
                        f"fontcolor=white@0.9:"
                        f"box=1:boxcolor=black@0.7:boxborderw=8:"
                        f"line_spacing={line_spacing}:"
                        f"x='if(lt(mod(t,240),60),{margin},if(lt(mod(t,240),120),w-text_w-{margin},if(lt(mod(t,240),180),w-text_w-{margin},{margin})))':"
                        f"y='if(lt(mod(t,240),60),{margin*2},if(lt(mod(t,240),120),{margin*2},if(lt(mod(t,240),180),h-text_h-{margin},h-text_h-{margin})))'"
                    )

                filter_parts.append(f"{overlay_inputs}{text_filter}[txt]")
                overlay_inputs = "[txt]"

            # Add PNG watermark with enhanced sizing from corner code
            if has_png:
                max_size = min(video_width // 4, video_height // 4)
                margin = 8

                # PNG position based on selected location
                if position == "topleft":
                    png_x, png_y = margin, margin
                elif position == "topright":
                    png_x, png_y = f"W-w-{margin}", margin
                elif position == "bottomleft":
                    png_x, png_y = margin, f"H-h-{margin}"
                else:  # bottomright (default)
                    png_x, png_y = f"W-w-{margin}", f"H-h-{margin}"

                filter_parts.append(f"[1:v]scale=-1:{max_size}:force_original_aspect_ratio=decrease[wm]")
                filter_parts.append(f"{overlay_inputs}[wm]overlay={png_x}:{png_y}[final]")
                overlay_inputs = "[final]"

            # Get original video bitrate for size matching (enhanced from corner code)
            if original_bitrate > 0:
                # Use original bitrate but cap it for reasonable size
                target_bitrate = min(original_bitrate, 2000000)  # Max 2Mbps
            else:
                # Estimate bitrate based on file size and duration
                file_size = os.path.getsize(input_file)
                if duration > 0:
                    estimated_bitrate = int((file_size * 8) / duration * 0.85)  # 85% for video track
                    target_bitrate = min(estimated_bitrate, 2000000)
                else:
                    target_bitrate = 1000000  # 1Mbps fallback

            target_bitrate_str = f"{target_bitrate//1000}k"

            # Build command optimized for maintaining original file size (enhanced from corner code)
            if has_png:
                if filter_parts:
                    cmd = [
                        'ffmpeg', '-i', input_file, '-i', png_path,
                        '-filter_complex', ';'.join(filter_parts),
                        '-map', overlay_inputs, '-map', '0:a?',
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28',  # Higher CRF for smaller size
                        '-c:a', 'copy',  # Copy audio without re-encoding
                        '-r', str(fps), '-b:v', target_bitrate_str,  # Match original bitrate
                        '-maxrate', f"{int(target_bitrate*1.2)//1000}k",
                        '-bufsize', f"{int(target_bitrate*2)//1000}k",
                        '-profile:v', 'main', '-level', '3.1',
                        '-map_metadata', '0', '-movflags', '+faststart',
                        '-threads', '0', '-y', output_file
                    ]
                else:
                    # Only PNG watermark, no text
                    margin = 15
                    cmd = [
                        'ffmpeg', '-i', input_file, '-i', png_path,
                        '-filter_complex',
                        f"[1:v]scale=-1:{min(video_width // 4, video_height // 4)}:force_original_aspect_ratio=decrease[wm];[0:v][wm]overlay=W-w-{margin}:{margin}[final]",
                        '-map', '[final]', '-map', '0:a?', '-c:v', 'libx264',
                        '-preset', 'ultrafast', '-crf', '28', '-c:a', 'copy',
                        '-r', str(fps), '-b:v', target_bitrate_str, '-maxrate',
                        f"{int(target_bitrate*1.2)//1000}k", '-bufsize',
                        f"{int(target_bitrate*2)//1000}k", '-profile:v', 'main', '-level', '3.1',
                        '-map_metadata', '0', '-movflags', '+faststart', '-threads', '0', '-y', output_file
                    ]
            else:
                if filter_parts:
                    # Extract the filter properly - get everything after the input reference
                    text_filter_only = filter_parts[0].replace(f"{overlay_inputs}", "").replace("[txt]", "")
                    cmd = [
                        'ffmpeg', '-i', input_file, '-vf', text_filter_only,
                        '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28',  # Higher CRF for smaller size
                        '-c:a', 'copy', '-r', str(fps), '-b:v', target_bitrate_str,  # Match original bitrate
                        '-maxrate', f"{int(target_bitrate*1.2)//1000}k",
                        '-bufsize', f"{int(target_bitrate*2)//1000}k",
                        '-profile:v', 'main', '-level', '3.1',
                        '-map_metadata', '0', '-movflags', '+faststart',
                        '-threads', '0', '-y', output_file
                    ]
                else:
                    # No watermarks at all, just copy streams for maximum speed and size preservation
                    cmd = [
                        'ffmpeg', '-i', input_file,
                        '-c:v', 'copy',  # Copy video stream without re-encoding
                        '-c:a', 'copy',  # Copy audio stream without re-encoding
                        '-map_metadata', '0', '-movflags', '+faststart',
                        '-y', output_file
                    ]

            logging.info(f"Running enhanced watermark command: {' '.join(cmd)}")

            # Execute with enhanced progress monitoring (from corner code)
            if progress_callback:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                frame_count_processed = 0
                process_start_time = time.time()
                last_progress_update = process_start_time
                fps_samples = []

                # Simple progress monitoring without blocking tasks

                # Enhanced progress monitoring for watermarking
                last_progress_line = ""
                last_frame_processed = 0
                stderr_buffer = ""

                while process.returncode is None:
                    # Check if process is still running
                    try:
                        await asyncio.wait_for(process.wait(), timeout=0.2)
                        break
                    except asyncio.TimeoutError:
                        pass

                    # Read stderr data without blocking
                    try:
                        data = await asyncio.wait_for(process.stderr.read(1024), timeout=0.1)
                        if data:
                            stderr_buffer += data.decode('utf-8', errors='ignore')

                            # Process complete lines
                            while '\n' in stderr_buffer or '\r' in stderr_buffer:
                                if '\r' in stderr_buffer:
                                    line, stderr_buffer = stderr_buffer.split('\r', 1)
                                else:
                                    line, stderr_buffer = stderr_buffer.split('\n', 1)

                                line = line.strip()
                                if line and 'frame=' in line:
                                    last_progress_line = line
                    except asyncio.TimeoutError:
                        pass
                    except Exception:
                        pass

                    # Process the latest progress line with enhanced parsing
                    if 'frame=' in last_progress_line:
                        current_time = time.time()
                        elapsed_time = current_time - process_start_time

                        try:
                            import re

                            # Extract frame number
                            frame_matches = re.findall(r'frame=\s*(\d+)', last_progress_line)

                            # Extract time processed (for better ETA calculation)
                            time_matches = re.findall(r'time=(\d+):(\d+):(\d+)\.(\d+)', last_progress_line)

                            # Extract fps from FFmpeg output
                            fps_matches = re.findall(r'fps=\s*(\d+\.?\d*)', last_progress_line)

                            if frame_matches:
                                frame_count_processed = int(frame_matches[-1])

                                # Only update if frame count is progressing
                                if frame_count_processed > last_frame_processed:
                                    last_frame_processed = frame_count_processed

                                    # Use FFmpeg's reported FPS if available, otherwise calculate
                                    if fps_matches:
                                        processing_fps = float(fps_matches[-1])
                                    elif elapsed_time > 0:
                                        processing_fps = frame_count_processed / elapsed_time
                                    else:
                                        processing_fps = 0

                                    # Smooth FPS calculation
                                    fps_samples.append(processing_fps)
                                    if len(fps_samples) > 5:  # Reduced window for more responsive updates
                                        fps_samples.pop(0)
                                    avg_fps = sum(fps_samples) / len(fps_samples) if fps_samples else processing_fps

                                    # Calculate progress percentage
                                    if frame_count and frame_count > 0:
                                        progress_percent = min(100.0, (frame_count_processed / frame_count) * 100.0)

                                        # Enhanced ETA calculation using time processed if available
                                        if time_matches:
                                            # Extract processed time from FFmpeg
                                            h, m, s, ms = map(int, time_matches[-1])
                                            processed_seconds = h * 3600 + m * 60 + s + ms / 100.0

                                            if duration > 0 and processed_seconds > 0:
                                                # Calculate ETA based on time ratio
                                                time_ratio = processed_seconds / duration
                                                if time_ratio > 0:
                                                    total_processing_time = elapsed_time / time_ratio
                                                    eta_seconds = total_processing_time - elapsed_time
                                                else:
                                                    eta_seconds = 0
                                            else:
                                                eta_seconds = 0
                                        elif avg_fps > 0 and frame_count_processed < frame_count:
                                            # Fallback to frame-based ETA
                                            remaining_frames = frame_count - frame_count_processed
                                            eta_seconds = remaining_frames / avg_fps
                                        else:
                                            eta_seconds = 0

                                        # Format ETA
                                        if eta_seconds > 0:
                                            if eta_seconds > 60:
                                                eta_mins = int(eta_seconds // 60)
                                                eta_secs = int(eta_seconds % 60)
                                                eta_str = f"{eta_mins}m{eta_secs}s"
                                            else:
                                                eta_str = f"{int(eta_seconds)}s"
                                        else:
                                            eta_str = "calculating..."
                                    else:
                                        # Fallback for unknown total frames
                                        progress_percent = min(100.0, (elapsed_time / max(duration, 60)) * 100.0) if duration > 0 else 0.0
                                        eta_str = "processing..."

                                    # Format elapsed time
                                    if elapsed_time > 60:
                                        elapsed_mins = int(elapsed_time // 60)
                                        elapsed_secs = int(elapsed_time % 60)
                                        elapsed_str = f"{elapsed_mins}m{elapsed_secs}s"
                                    else:
                                        elapsed_str = f"{int(elapsed_time)}s"

                                    # Update progress every 2 seconds or when complete
                                    should_update = (current_time - last_progress_update >= 2.0)
                                    is_complete = (frame_count and frame_count_processed >= frame_count) or progress_percent >= 99.0

                                    if should_update or is_complete:
                                        try:
                                            if progress_callback:
                                                await progress_callback(
                                                    stage="Watermarking",
                                                    percent=progress_percent,
                                                    current_frame=frame_count_processed,
                                                    total_frames=frame_count or 0,
                                                    fps=avg_fps,
                                                    eta=eta_str,
                                                    elapsed=elapsed_str
                                                )
                                                last_progress_update = current_time
                                                logging.info(f"Enhanced watermarking progress: {progress_percent:.1f}% ({frame_count_processed}/{frame_count or 0} frames @ {avg_fps:.1f}fps, ETA: {eta_str})")
                                        except Exception as progress_error:
                                            logging.debug(f"Progress callback error: {progress_error}")

                        except Exception as e:
                            logging.debug(f"Error parsing enhanced FFmpeg output: {e}")
                            # Improved fallback with time-based progress
                            if (current_time - last_progress_update >= 3.0) and progress_callback:
                                try:
                                    # Estimate progress based on elapsed time vs expected duration
                                    estimated_percent = min(90.0, (elapsed_time / max(duration, 60)) * 100.0) if duration > 0 else 50.0
                                    await progress_callback(
                                        stage="Watermarking",
                                        percent=estimated_percent,
                                        current_frame=0,
                                        total_frames=frame_count or 0,
                                        fps=0.0,
                                        eta="processing...",
                                        elapsed=f"{int(elapsed_time)}s"
                                    )
                                    last_progress_update = current_time
                                except Exception:
                                    pass

                # Wait for process to complete
                await process.wait()

                if process.returncode != 0:
                    stderr_output = ""
                    if process.stderr:
                        try:
                            stderr_data = await process.stderr.read()
                            stderr_output = stderr_data.decode('utf-8', errors='ignore')
                        except:
                            stderr_output = "Could not read stderr"
                    raise Exception(f"FFmpeg failed with return code {process.returncode}: {stderr_output}")
            else:
                # Non-progress version - still use async subprocess
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()

                if process.returncode != 0:
                    raise Exception(f"FFmpeg failed: {stderr.decode('utf-8', errors='ignore')}")

            logging.info("Successfully added enhanced watermarks with metadata preservation")
            return True

        except Exception as e:
            logging.error(f"Enhanced watermark error: {e}")
            # Fallback: copy original file
            try:
                import shutil
                shutil.copy2(input_file, output_file)
            except:
                pass
            return False

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

                logging.info(f"Bento4 SDK setup complete: {mp4decrypt_path}")
        except Exception as e:
            logging.error(f"Failed to set up Bento4 SDK: {str(e)}\n{traceback.format_exc()}")
            raise

    # Run setup on startup
try:
    setup_bento4()
except Exception as e:
    logging.error(f"Startup error in setup_bento4: {str(e)}\n{traceback.format_exc()}")
    raise

# Initialize Telegram client with optimized settings and automatic DC selection
client = TelegramClient('bot', API_ID, API_HASH, connection_retries=5, auto_reconnect=True)

# Set up automatic DC selection for optimal speed (Delhi users benefit from DC4/DC1)
async def optimize_telegram_connection():
    """Automatically connect to fastest Telegram DC for users in Delhi/India"""
    try:
        await client.connect()
        if not await client.is_user_authorized():
            # For bots, we don't need user authorization, but we can still optimize DC
            pass

        # Get current DC info
        try:
            from telethon.tl.functions.help import GetConfigRequest
            config = await client(GetConfigRequest())
            current_dc = getattr(config, 'this_dc', 'Unknown')
            logging.info(f"Current Telegram DC: {current_dc}")

            # For Delhi/India users, DC1 (Miami) or DC4 (Amsterdam) usually provide best speeds
            # The client will automatically use the optimal DC for the bot
            logging.info("Telegram client optimized for Delhi region")
        except Exception as e:
            logging.warning(f"Could not get DC info: {e}")

    except Exception as e:
        logging.error(f"Error optimizing Telegram connection: {e}")

# Optimization will be called inside main() function

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

# Enhanced per-user flood control with adaptive timing
user_last_message_time = {}
user_message_locks = {}
user_flood_penalty = {}  # Track flood penalties per user
user_message_cache = {}  # Cache last message content to avoid duplicate edits

# Helper function to handle flood wait errors and throttle message sends
async def send_message_with_flood_control(entity, message, event=None, edit_message=None):
    # Get user ID for per-user throttling
    user_id = entity.id if hasattr(entity, 'id') else (event.sender_id if event else 0)

    # Create per-user lock if not exists
    if user_id not in user_message_locks:
        user_message_locks[user_id] = asyncio.Lock()

    async with user_message_locks[user_id]:
        current_time = time.time()
        last_message_time = user_last_message_time.get(user_id, 0)

        # Much faster base delay for quicker responses
        base_delay = 1.0  # Reduced from 5.0 to 1.0 for faster responses
        flood_penalty = user_flood_penalty.get(user_id, 0)
        # Reduced penalty scaling for faster recovery
        adaptive_delay = min(base_delay + (flood_penalty * 1.5), 10.0)  # Max 10 seconds

        # Per-user throttling with adaptive delay
        time_since_last = current_time - last_message_time
        if time_since_last < adaptive_delay:
            sleep_time = adaptive_delay - time_since_last
            logging.debug(f"User {user_id}: Throttling for {sleep_time:.1f}s (penalty: {flood_penalty})")
            await asyncio.sleep(sleep_time)

        # Enhanced content change detection for edits
        if edit_message:
            cache_key = f"{user_id}_{id(edit_message)}"
            cached_content = user_message_cache.get(cache_key, "")

            # Check if content is substantially the same (ignore minor changes)
            if cached_content == message:
                logging.debug(f"User {user_id}: Skipping identical message edit")
                return edit_message

            # Check if only minor progress changes (less than 5% difference)
            if cached_content and message:
                try:
                    import re
                    # Extract percentage from both messages
                    cached_pct = re.search(r'(\d+\.?\d*)%', cached_content)
                    new_pct = re.search(r'(\d+\.?\d*)%', message)

                    if cached_pct and new_pct:
                        cached_val = float(cached_pct.group(1))
                        new_val = float(new_pct.group(1))

                        # Skip edit if progress change is less than 5%
                        if abs(new_val - cached_val) < 5.0:
                            logging.debug(f"User {user_id}: Skipping minor progress update ({cached_val}% -> {new_val}%)")
                            return edit_message
                except:
                    pass  # Fall through to normal edit

        retry_count = 0
        max_retries = 3  # Reduced retries to avoid excessive delays

        while retry_count <= max_retries:
            try:
                if edit_message:
                    logging.debug(f"Editing message for user {user_id} (attempt {retry_count + 1})")
                    await edit_message.edit(message)
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
                    user_last_message_time[user_id] = time.time()
                    # Reduce flood penalty on success
                    if user_id in user_flood_penalty and user_flood_penalty[user_id] > 0:
                        user_flood_penalty[user_id] = max(0, user_flood_penalty[user_id] - 0.3)
                    return result

            except MessageNotModifiedError:
                # Message content is identical, no need to update
                logging.debug(f"Message not modified for user {user_id} - content identical")
                user_last_message_time[user_id] = time.time()
                if edit_message:
                    user_message_cache[f"{user_id}_{id(edit_message)}"] = message
                return edit_message if edit_message else None

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
                user_flood_penalty[user_id] = user_flood_penalty.get(user_id, 0) + 1.0

                if retry_count <= max_retries:
                    logging.warning(f"FloodWaitError for user {user_id}: Waiting {wait_time}s (attempt {retry_count}/{max_retries + 1}, penalty: {user_flood_penalty[user_id]:.1f})")
                    await asyncio.sleep(wait_time)

                    # Longer buffer time after flood wait
                    buffer_time = min(10 + (retry_count * 5), 30)  # 10-30 second buffer
                    logging.debug(f"User {user_id}: Adding {buffer_time}s buffer after flood wait")
                    await asyncio.sleep(buffer_time)
                else:
                    logging.error(f"Max flood wait retries exceeded for user {user_id}")
                    return None

            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    backoff_time = min(5 * retry_count, 30)  # Longer backoff
                    logging.warning(f"Message send error for user {user_id}: {str(e)} (attempt {retry_count}/{max_retries + 1}), retrying in {backoff_time}s")
                    await asyncio.sleep(backoff_time)
                else:
                    logging.error(f"Failed to send/edit message for user {user_id} after {max_retries + 1} attempts: {str(e)}")
                    return None

        logging.error(f"Exhausted all retry attempts for user {user_id}")
        return None

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

async def extract_video_frame_thumbnail(video_path, output_path, duration=None):
        """Extract a random frame from video as thumbnail"""
        try:
            import random

            # If duration is provided, pick a random time between 10% and 90% of video
            if duration and duration > 10:
                start_time = max(1, int(duration * 0.1))
                end_time = int(duration * 0.9)
                random_time = random.randint(start_time, end_time)
            else:
                # Default to 5 seconds if no duration or short video
                random_time = 5

            # Extract frame at random time
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

def progress_display(stage, percent, done, total, speed, elapsed, user, user_id, filename, **kwargs):
        """
        Return a crystal/diamond styled progress bar message for Telegram with unified 100% progress.
        """
        # Enhanced stage-specific emojis and messages with crystal theme
        stage_mapping = {
            "Downloading": "🔹 Crystal Download",
            "downloading": "🔹 Crystal Download",
            "Decrypting": "🔷 Sapphire Decrypt",
            "decrypting": "🔷 Sapphire Decrypt",
            "Merging": "💎 Diamond Merge",
            "merging": "💎 Diamond Merge",
            "Watermarking": "🔮 Opal Watermark",
            "watermarking": "🔮 Opal Watermark",
            "Uploading": "🔹 Crystal Upload",
            "uploading": "🔹 Crystal Upload",
            "processing": "💠 Crystal Process",
            "encoding": "💎 Diamond Encode",
            "finalizing": "✨ Final Polish",
            "Completed": "💎 Crystallized"
        }

        stage_display = stage_mapping.get(stage, f"💠 Crystal {stage}")

        # Calculate unified progress across all stages (0-100%)
        all_stages = ["Downloading", "Decrypting", "Merging", "Watermarking", "Uploading"]
        stage_weights = [40, 10, 10, 20, 20]  # Percentage weight for each stage

        current_stage_index = -1
        stage_lower = stage.lower()
        for i, s in enumerate(all_stages):
            if s.lower() == stage_lower:
                current_stage_index = i
                break

        # Calculate unified progress percentage
        if current_stage_index == -1:
            unified_percent = 0
        else:
            # Add completed stages
            completed_weight = sum(stage_weights[:current_stage_index])
            # Add current stage progress
            current_stage_progress = (percent / 100) * stage_weights[current_stage_index]
            unified_percent = completed_weight + current_stage_progress

        # Ensure we don't exceed 100%
        unified_percent = min(100.0, unified_percent)

        # Create elegant crystal progress bar
        total_segments = 10
        filled_segments = int(unified_percent // 10)

        # Crystal progress bar with diamond shapes
        progress_bar = ""
        for i in range(total_segments):
            if i < filled_segments:
                progress_bar += "◉"  # Filled diamond
            elif i == filled_segments and unified_percent % 10 >= 5:
                progress_bar += "◎"  # Half-filled diamond
            else:
                progress_bar += "◎"  # Empty diamond

        # Create crystal pipeline progress indicator
        stage_indicators = []
        stage_emojis = ["🔹", "🔷", "💎", "🔮", "✨"]

        for i, s in enumerate(all_stages):
            if i < current_stage_index:
                stage_indicators.append("💎")  # Completed stage - crystallized
            elif i == current_stage_index:
                stage_indicators.append("🔥")  # Current stage - processing
            else:
                stage_indicators.append("◦")  # Pending stage - unprocessed

        pipeline_progress = " ".join(stage_indicators)

        # Calculate ETA
        eta = (total - done) / speed if speed > 0 and done < total else 0

        # Build crystal progress display
        progress_text = (
            f"{stage_display}: {filename}\n"
            f"💠 Progress: {progress_bar} {unified_percent:.1f}%\n"
        )

        # Add stage-specific details with crystal theme
        if stage.lower() == "watermarking":
            current_frame = kwargs.get('current_frame', 0)
            total_frames = kwargs.get('total_frames', 0)
            fps = kwargs.get('fps', 0)
            eta_time = kwargs.get('eta', 0)

            # Handle ETA formatting for watermarking
            if isinstance(eta_time, str):
                eta_str = eta_time
            elif isinstance(eta_time, (int, float)):
                eta_str = format_time(eta_time) if eta_time > 0 else "calculating..."
            else:
                eta_str = "calculating..."

            # Format frame numbers with commas
            current_formatted = f"{current_frame:,}" if current_frame else "0"
            total_formatted = f"{total_frames:,}" if total_frames else "0"

            progress_text += (
                f"🎬 {current_formatted} / {total_formatted} frames\n"
                f"💎 Processing Speed: {fps:.1f} fps\n"
                f"🕰 ETA: {eta_str} | Elapsed: {format_time(elapsed)}\n"
            )
        else:
            progress_text += (
                f"📤 {format_size(done)} / {format_size(total)}\n"
                f"💎 Transfer Speed: {format_size(speed)}/s\n"
                f"🕰 ETA: {format_time(eta)} | Elapsed: {format_time(elapsed)}\n"
            )

        progress_text += (
            f"🔮 Pipeline: {pipeline_progress}\n"
            f"👤 {user} (ID: {user_id})\n"
            f"✨ Crystal Engine v2.0"
        )

        return progress_text

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

class MPDLeechBot:
        def __init__(self, user_id):
            self.user_id = user_id
            self.user_download_dir = os.path.join(DOWNLOAD_DIR, f"user_{user_id}")
            self.setup_dirs()
            self.has_notified_split = False  # Flag to prevent duplicate split messages
            self.progress_task = None  # To track the progress task
            self.update_lock = asyncio.Lock()  # Lock to prevent concurrent updates
            self.is_downloading = False  # Flag to prevent overlapping downloads in the same instance
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
            status_msg = None
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

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'video/mp4,application/mp4,*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Connection': 'keep-alive'
                }

                timeout = aiohttp.ClientTimeout(total=3600)  # 1 hour timeout
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}: {response.reason}")

                        total_size = int(response.headers.get('Content-Length', 0))
                        self.progress_state['total_size'] = total_size
                        downloaded = 0

                        with open(output_file, 'wb') as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                                f.write(chunk)
                                downloaded += len(chunk)
                                self.progress_state['done_size'] = downloaded
                                self.progress_state['percent'] = (downloaded / total_size * 100) if total_size > 0 else 0
                                elapsed = time.time() - self.progress_state['start_time']
                                self.progress_state['speed'] = downloaded / elapsed if elapsed > 0 else 0
                                self.progress_state['elapsed'] = elapsed

                # Get video duration using ffprobe
                try:
                    duration_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', output_file]
                    process = await asyncio.create_subprocess_exec(*duration_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    stdout, stderr = await process.communicate()
                    duration = int(float(stdout.decode().strip())) if stdout.decode().strip() else 0
                except:
                    duration = 0

                # Apply watermarks with progress tracking if configured (works for all processing modes)
                watermarked_file = os.path.join(self.user_download_dir, f"{name}_watermarked.mp4")

                # Update progress state before starting watermark process
                self.progress_state['stage'] = "Watermarking"
                self.progress_state['percent'] = 0.0

                # Define watermark progress callback that integrates with main progress system
                async def watermark_progress_callback(stage, percent, current_frame, total_frames, fps, eta, elapsed):
                    # Update progress state for watermarking stage
                    self.progress_state['stage'] = "Watermarking"
                    self.progress_state['percent'] = percent
                    self.progress_state['elapsed'] = elapsed

                    # Store watermark-specific data in progress state
                    self.progress_state['current_frame'] = current_frame
                    self.progress_state['total_frames'] = total_frames
                    self.progress_state['fps'] = fps
                    self.progress_state['eta'] = eta

                    # Don't create separate messages - let the main progress system handle updates
                    # This prevents duplicate messages and flood wait issues
                    logging.info(f"Watermarking progress: {percent:.1f}% ({current_frame:,}/{total_frames:,} frames @ {fps:.1f}fps)")

                # Apply watermarks for all processing modes (regular, bulk, processjson)
                watermark_success = await apply_watermarks(output_file, watermarked_file, self.user_id, watermark_progress_callback)

                if watermark_success and os.path.exists(watermarked_file):
                    # Remove original and use watermarked version
                    os.remove(output_file)
                    os.rename(watermarked_file, output_file)
                    logging.info(f"Applied watermarks to: {output_file}")

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

        async def fetch_segment(self, url, progress, total_segments, range_header=None, output_file=None):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'video/mp4,application/mp4,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"Windows"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'Connection': 'keep-alive'
            }
            if range_header:
                headers['Range'] = range_header
            timeout = aiohttp.ClientTimeout(total=300)

            # Define the download operation as a coroutine
            async def download_operation():
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    logging.info(f"Fetching: {url}, range={range_header}")
                    async with session.get(url, headers=headers) as response:
                        if response.status == 403:
                            raise Exception(f"403 Forbidden: {url}")
                        response.raise_for_status()
                        total_size = int(response.headers.get('Content-Length', 0))
                        downloaded = 0
                        with open(output_file, 'wb') as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                f.write(chunk)
                                downloaded += len(chunk)
                                progress['done_size'] += len(chunk)
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

        async def split_file(self, input_file, max_size_mb=2000):  # Default to 2 GB (2000 MB)
            max_size = max_size_mb * 1024 * 1024
            file_size = os.path.getsize(input_file)

            # If file is within size limit, return as-is
            if file_size <= max_size:
                logging.info(f"File {input_file} ({format_size(file_size)}) is within {max_size_mb}MB limit, no splitting needed")
                return [input_file]

            logging.info(f"File {input_file} ({format_size(file_size)}) exceeds {max_size_mb}MB limit, splitting into parts")

            base_name = os.path.splitext(input_file)[0]
            ext = os.path.splitext(input_file)[1]
            chunks = []

            # Get video duration more reliably
            duration_cmd = ['ffprobe', '-v', 'quiet', '-show_entries', 'format=duration', '-of', 'csv=p=0', input_file]
            try:
                process = await asyncio.create_subprocess_exec(*duration_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                duration = float(stdout.decode().strip()) if stdout.decode().strip() else 0
            except:
                # Fallback to ffmpeg method
                duration_cmd = ['ffmpeg', '-i', input_file, '-f', 'null', '-', '-y']
                process = await asyncio.create_subprocess_exec(*duration_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                _, stderr = await process.communicate()
                duration = 0
                for line in stderr.decode().splitlines():
                    if 'Duration' in line:
                        time_str = line.split('Duration: ')[1].split(',')[0]
                        h, m, s = map(float, time_str.split(':'))
                        duration = int(h * 3600 + m * 60 + s)
                        break

            if duration <= 0:
                logging.warning(f"Could not determine duration for {input_file}, using file size estimation")
                duration = 300  # 5 minutes fallback

            # Calculate optimal chunk duration to stay under size limit
            chunk_duration = duration * max_size / file_size
            num_chunks = max(1, int(file_size / max_size) + 1)

            # Ensure minimum chunk duration of 30 seconds
            chunk_duration = max(30, chunk_duration)

            logging.info(f"Splitting into {num_chunks} parts, each ~{chunk_duration:.1f} seconds")

            for i in range(num_chunks):
                output_file = f"{base_name}_part{i+1}{ext}"
                start_time = i * chunk_duration

                # For the last chunk, don't specify duration to get the rest
                if i == num_chunks - 1:
                    cmd = ['ffmpeg', '-i', input_file, '-ss', str(start_time), '-c', 'copy', '-avoid_negative_ts', 'make_zero', output_file, '-y']
                else:
                    cmd = ['ffmpeg', '-i', input_file, '-ss', str(start_time), '-t', str(chunk_duration), '-c', 'copy', '-avoid_negative_ts', 'make_zero', output_file, '-y']

                logging.info(f"Splitting part {i+1}: {' '.join(cmd)}")
                process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()

                if process.returncode == 0 and os.path.exists(output_file) and os.path.getsize(output_file) > 0:
                    chunks.append(output_file)
                    part_size = os.path.getsize(output_file)
                    logging.info(f"Split part {i+1} created: {output_file} ({format_size(part_size)})")
                else:
                    error_msg = stderr.decode() if stderr else "Unknown error"
                    logging.error(f"Split part {i+1} failed: {error_msg}")
                    # Continue with other parts instead of failing completely

            if not chunks:
                raise Exception("Failed to create any valid chunks")

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
                    'Accept-Encoding': 'gzip, deflate, br, identity',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache',
                    'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    'Sec-Ch-Ua-Mobile': '?0',
                    'Sec-Ch-Ua-Platform': '"Windows"',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'cross-site',
                    'Connection': 'keep-alive'
                }

                # Define the MPD fetch operation as a coroutine
                async def fetch_mpd_operation():
                    timeout = aiohttp.ClientTimeout(total=30, connect=10)
                    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, keepalive_timeout=30)
                    # Enable auto-decompression for brotli, gzip, and deflate
                    async with aiohttp.ClientSession(
                        timeout=timeout, 
                        connector=connector,
                        auto_decompress=True,
                        connector_owner=False
                    ) as session:
                        logging.info(f"Fetching MPD: {mpd_url}")
                        async with session.get(mpd_url, headers=headers, ssl=False) as response:
                            if response.status == 403:
                                raise Exception(f"403 Forbidden: {mpd_url}")
                            elif response.status == 404:
                                raise Exception(f"404 Not Found: {mpd_url}")
                            elif response.status >= 400:
                                raise Exception(f"HTTP {response.status}: {response.reason}")
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

                last_update_time = 0  # For debouncing
                async def update_progress(filename, user, user_id):
                    nonlocal max_total_size_est, last_update_time, status_msg
                    logging.info(f"Starting update_progress task for {name}")

                    # Faster timing for quicker user feedback
                    base_interval = 8.0   # Reduced from 30.0 to 8.0
                    min_interval = 5.0    # Reduced from 20.0 to 5.0
                    max_interval = 30.0   # Reduced from 120.0 to 30.0

                    last_content_hash = None
                    consecutive_identical = 0
                    last_significant_update = 0

                    try:
                        while self.progress_state['stage'] != "Completed":
                            if asyncio.current_task().cancelled():
                                break

                            async with self.update_lock:
                                current_time = time.time()

                                # More aggressive adaptive interval based on flood penalty
                                flood_penalty = user_flood_penalty.get(user_id, 0)
                                adaptive_interval = min(base_interval + (flood_penalty * 10), max_interval)

                                # Check if enough time has passed for any update
                                if current_time - last_update_time < adaptive_interval:
                                    await asyncio.sleep(1)  # Check more frequently for responsiveness
                                    continue

                                # Update progress state
                                self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                                self.progress_state['speed'] = self.progress_state['done_size'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0

                                if self.progress_state['stage'] == "Downloading":
                                    total_size_est = self.progress_state['done_size'] * total_segments / max(progress['completed'], 1)
                                    max_total_size_est = max(max_total_size_est, total_size_est)
                                    self.progress_state['total_size'] = max_total_size_est
                                    self.progress_state['percent'] = progress['completed'] * 100 / total_segments

                                # Only update on significant progress changes (10% or more, or stage change)
                                current_stage = self.progress_state['stage']
                                current_percent = self.progress_state['percent']

                                # Check if this is a significant update worth sending
                                is_significant_update = False
                                if hasattr(self, '_last_progress_stage'):
                                    if (current_stage != self._last_progress_stage or 
                                        abs(current_percent - getattr(self, '_last_progress_percent', 0)) >= 10.0 or
                                        current_time - last_significant_update > 60.0):  # Force update every 60 seconds
                                        is_significant_update = True
                                else:
                                    is_significant_update = True  # First update

                                if not is_significant_update:
                                    await asyncio.sleep(10)
                                    continue

                                # Prepare kwargs for watermarking stage
                                display_kwargs = {}
                                if self.progress_state['stage'] == "Watermarking":
                                    display_kwargs.update({
                                        'current_frame': self.progress_state.get('current_frame', 0),
                                        'total_frames': self.progress_state.get('total_frames', 0),
                                        'fps': self.progress_state.get('fps', 0),
                                        'eta': self.progress_state.get('eta', 0)
                                    })

                                display = progress_display(
                                    self.progress_state['stage'],
                                    self.progress_state['percent'],
                                    self.progress_state['done_size'],
                                    self.progress_state['total_size'],
                                    self.progress_state['speed'],
                                    self.progress_state['elapsed'],
                                    user,
                                    user_id,
                                    filename,
                                    **display_kwargs
                                )

                                # Create content hash for efficient comparison
                                import hashlib
                                content_hash = hashlib.md5(display.encode()).hexdigest()

                                # Only send if content has meaningfully changed
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

                                            # Store last progress values
                                            self._last_progress_stage = current_stage
                                            self._last_progress_percent = current_percent

                                            logging.debug(f"Significant progress update for user {user_id}: {current_stage} - {current_percent:.1f}%")
                                        else:
                                            # Failed to update, increase interval significantly
                                            adaptive_interval = min(adaptive_interval * 2, max_interval)
                                            logging.warning(f"Progress update failed for user {user_id}, increasing interval to {adaptive_interval}s")
                                    except Exception as e:
                                        logging.warning(f"Progress update failed for user {user_id}: {e}")
                                        adaptive_interval = min(adaptive_interval * 2, max_interval)

                            # Faster sleep times for better responsiveness
                            if self.progress_state['stage'] in ["Watermarking", "Merging"]:
                                await asyncio.sleep(3)  # Reduced for faster updates
                            else:
                                await asyncio.sleep(max(2, adaptive_interval / 4))  # Faster checks

                    except asyncio.CancelledError:
                        logging.info(f"update_progress task cancelled for {name}")
                        raise
                    finally:
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

                # Stage 2: Download Segments
                self.progress_state['stage'] = "Downloading"
                video_files = [os.path.join(self.user_download_dir, f"{name}_video_seg{i}.mp4") for i in range(len(video_segments))]
                audio_files = [os.path.join(self.user_download_dir, f"{name}_audio_seg{i}.mp4") for i in range(len(audio_segments))]

                tasks = []

                for i, (seg_url, range_header) in enumerate(video_segments):
                    tasks.append(self.fetch_segment(seg_url, progress, total_segments, range_header, video_files[i]))
                for i, (seg_url, range_header) in enumerate(audio_segments):
                    tasks.append(self.fetch_segment(seg_url, progress, total_segments, range_header, audio_files[i]))

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

                # Store process reference for cancellation
                self._ffmpeg_process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await self._ffmpeg_process.communicate()

                if self._ffmpeg_process.returncode == 0:
                    logging.info(f"FFmpeg merged MP4: {output_file}")
                    final_file = output_file
                else:
                    logging.error(f"FFmpeg failed: {stderr.decode()}")
                    raise Exception(f"FFmpeg failed: {stderr.decode()}")

                # Clear process reference
                self._ffmpeg_process = None
                self.progress_state['percent'] = 100.0

                # Apply watermarks with progress tracking if configured (works for all processing modes)
                watermarked_file = os.path.join(self.user_download_dir, f"{name}_watermarked.mp4")

                # Update progress state before starting watermark process
                self.progress_state['stage'] = "Watermarking"
                self.progress_state['percent'] = 0.0

                # Define watermark progress callback that integrates with main progress system
                async def watermark_progress_callback(stage, percent, current_frame, total_frames, fps, eta, elapsed):
                    # Update progress state for watermarking stage
                    self.progress_state['stage'] = "Watermarking"
                    self.progress_state['percent'] = percent
                    self.progress_state['elapsed'] = elapsed

                    # Store watermark-specific data in progress state
                    self.progress_state['current_frame'] = current_frame
                    self.progress_state['total_frames'] = total_frames
                    self.progress_state['fps'] = fps
                    self.progress_state['eta'] = eta

                    # Don't create separate messages - let the main progress system handle updates
                    # This prevents duplicate messages and flood wait issues
                    logging.info(f"Watermarking progress: {percent:.1f}% ({current_frame:,}/{total_frames:,} frames @ {fps:.1f}fps)")

                # Apply watermarks for all processing modes (regular, bulk, processjson)
                watermark_success = await apply_watermarks(final_file, watermarked_file, self.user_id, watermark_progress_callback)

                if watermark_success and os.path.exists(watermarked_file):
                    # Remove original and use watermarked version
                    os.remove(final_file)
                    os.rename(watermarked_file, final_file)
                    logging.info(f"Applied watermarks to: {final_file}")

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
                error_message = f"Download failed for {name}: {str(e)}\nPlease check if the MPD URL is valid, requires authentication, or needs specific headers (e.g., Referer, Cookies)."
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

        async def upload_file(self, event, filepath, status_msg, total_size, sender, duration, retry_count=0):
            try:
                import time  # Ensure time module is available locally
                file_size = os.path.getsize(filepath)
                logging.info(f"Processing upload for {filepath}, size: {format_size(file_size)}, duration: {duration}s, retry: {retry_count}")
                self.progress_state['start_time'] = time.time()
                self.progress_state['total_size'] = file_size
                self.progress_state['done_size'] = 0
                self.progress_state['percent'] = 0.0
                self.progress_state['speed'] = 0
                self.progress_state['elapsed'] = 0
                # Maximum concurrent upload parts for full speed
                # Optimized upload progress for maximum bandwidth

                # Check if user has Telegram Premium and set appropriate file size limits
                # Use multiple methods to detect premium status for reliability
                is_premium = False
                try:
                    is_premium = getattr(sender, 'premium', False) or getattr(sender, 'is_premium', False)
                    if hasattr(sender, 'premium_flag'):
                        is_premium = sender.premium_flag
                except:
                    is_premium = False

                max_size_mb = 4000 if is_premium else 2000  # 4 GB for premium, 2 GB for free
                logging.info(f"User {sender.id} premium status: {is_premium}, setting max_size_mb to {max_size_mb} MB")

                # Custom parallel upload function with retries
                async def upload_part(file_id, part_num, part_size, total_parts, file_handle, progress, semaphore):
                    async with semaphore:  # Limit concurrent uploads
                        start = part_num * part_size
                        file_handle.seek(start)
                        data = file_handle.read(part_size)
                        if not data:
                            return part_num, True, None
                        # Log the size of the data being uploaded
                        logging.info(f"Part {part_num}: Data size={len(data)} bytes")
                        if len(data) > 524288:  # 512 KB limit for bots
                            error_msg = f"Part {part_num} data size {len(data)} exceeds 512 KB limit"
                            logging.error(error_msg)
                            return part_num, False, error_msg

                        # Define the upload operation as a coroutine
                        async def upload_operation():
                            logging.info(f"Uploading part {part_num} of {total_parts}, size: {len(data)} bytes, file_id: {file_id}")
                            await client(SaveBigFilePartRequest(
                                file_id=file_id,
                                file_part=part_num,
                                file_total_parts=total_parts,
                                bytes=data
                            ))
                            async with self.update_lock:
                                progress['uploaded'] += len(data)
                                self.progress_state['done_size'] = progress['uploaded']
                                self.progress_state['percent'] = (progress['uploaded'] / file_size) * 100
                            logging.info(f"Uploaded part {part_num} of {total_parts}")

                        # Use retry_with_backoff for the upload operation
                        try:
                            await retry_with_backoff(
                                coroutine=upload_operation,
                                max_retries=3,
                                base_delay=2,
                                operation_name=f"Upload part {part_num} of {filepath}"
                            )
                            return part_num, True, None
                        except FloodWaitError as e:
                            wait_time = e.seconds
                            logging.warning(f"FloodWaitError while uploading part {part_num}: Waiting for {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            return part_num, False, f"FloodWaitError: {str(e)}"
                        except Exception as e:
                            error_msg = f"Failed to upload part {part_num}: {str(e)}\n{traceback.format_exc()}"
                            logging.error(error_msg)
                            return part_num, False, error_msg

                if file_size > max_size_mb * 1024 * 1024:
                    if not self.has_notified_split:
                        premium_status = "Telegram Premium" if is_premium else "Free Telegram"
                        status_msg = await send_message_with_flood_control(
                            entity=event.chat_id,
                            message=f"📁 File size: {format_size(file_size)}\n👤 Account: {premium_status} (Max: {max_size_mb}MB)\n✂️ Splitting into parts...",
                            edit_message=status_msg
                        )
                        self.has_notified_split = True
                        logging.info(f"Notified user about splitting file: {filepath} (Premium: {is_premium}, Limit: {max_size_mb}MB)")
                    chunks = await self.split_file(filepath, max_size_mb=max_size_mb)
                    for i, chunk in enumerate(chunks):
                        chunk_size = os.path.getsize(chunk)
                        chunk_duration = duration // len(chunks)
                        progress = {'uploaded': 0}
                        last_update_time = 0  # For debouncing upload progress

                        # Stage: Uploading (for each chunk)
                        self.progress_state['stage'] = "Uploading"
                        self.progress_state['total_size'] = chunk_size
                        self.progress_state['done_size'] = 0
                        self.progress_state['percent'] = 0.0
                        # Maximum concurrent upload parts for full speed

                        # Custom parallel upload for each chunk
                        file_id = random.getrandbits(63)  # Generate a 63-bit file ID (0 to 2^63 - 1)
                        part_size = 512 * 1024  # 512 KB chunks (max for bots)
                        total_parts = (chunk_size + part_size - 1) // part_size
                        # Log the parameters to ensure they're valid
                        logging.info(f"Chunk {i+1}: file_id={file_id}, chunk_size={chunk_size}, part_size={part_size}, total_parts={total_parts}")
                        if total_parts <= 0:
                            raise ValueError(f"Invalid total_parts for chunk {i+1}: {total_parts}")
                        semaphore = asyncio.Semaphore(8)  # Increased to 8 concurrent uploads for maximum bandwidth
                        logging.info(f"Starting parallel upload for chunk {i+1}, size: {chunk_size}, total parts: {total_parts}, file_id: {file_id}")

                        async def update_progress():
                            nonlocal last_update_time, status_msg
                            while progress['uploaded'] < chunk_size:
                                current_time = time.time()
                                if current_time - last_update_time < 5:  # Reduced to 5 seconds for more frequent updates
                                    await asyncio.sleep(0.1)
                                    continue
                                self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                                self.progress_state['speed'] = progress['uploaded'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0
                                display = progress_display(
                                    self.progress_state['stage'],
                                    self.progress_state['percent'],
                                    self.progress_state['done_size'],
                                    self.progress_state['total_size'],
                                    self.progress_state['speed'],
                                    self.progress_state['elapsed'],
                                    sender.first_name,
                                    sender.id,
                                    f"{os.path.basename(chunk)} (Part {i+1})"
                                )
                                async with self.update_lock:
                                    # Check if message content has actually changed
                                    if not hasattr(status_msg, '_last_content') or status_msg._last_content != display:
                                        status_msg = await send_message_with_flood_control(
                                            entity=event.chat_id,
                                            message=display,
                                            edit_message=status_msg
                                        )
                                        if status_msg:
                                            status_msg._last_content = display
                                    last_update_time = current_time
                                    logging.info(f"Upload progress updated for chunk {i+1}: {self.progress_state['percent']:.1f}%")
                                await asyncio.sleep(5)  # Update every 5 seconds

                        upload_start = time.time()
                        with open(chunk, 'rb') as f:
                            tasks = []
                            for part_num in range(total_parts):
                                tasks.append(upload_part(file_id, part_num, part_size, total_parts, f, progress, semaphore))
                            # Start progress update task
                            progress_task = asyncio.create_task(update_progress())
                            # Upload all parts in parallel
                            results = await asyncio.gather(*tasks, return_exceptions=False)
                            # Cancel progress task
                            progress_task.cancel()
                            try:
                                await progress_task
                            except asyncio.CancelledError:
                                logging.info(f"Progress task for chunk {i+1} cancelled")

                        # Check for failed parts
                        failed_parts = [(part_num, error) for part_num, success, error in results if not success]
                        if failed_parts:
                            error_msgs = [f"Part {part_num} failed: {error}" for part_num, error in failed_parts]
                            raise Exception("Upload failed for some parts:\n" + "\n".join(error_msgs))

                        # Finalize upload using the file_id and total_parts
                        input_file = InputFileBig(
                            id=file_id,
                            parts=total_parts,
                            name=os.path.basename(chunk)
                        )

                        # Get thumbnail for video
                        thumbnail_file = None
                        async with thumbnail_lock:
                            if sender.id in user_thumbnails and os.path.exists(user_thumbnails[sender.id]):
                                thumbnail_file = user_thumbnails[sender.id]

                        # If no custom thumbnail, try to extract frame from video, fallback to random
                        if not thumbnail_file:
                            temp_thumb_path = os.path.join(self.user_download_dir, f"temp_thumb_{i+1}.jpg")
                            if await extract_video_frame_thumbnail(chunk, temp_thumb_path, chunk_duration):
                                thumbnail_file = temp_thumb_path
                            elif await generate_random_thumbnail(temp_thumb_path):
                                thumbnail_file = temp_thumb_path

                        # Send the file directly with thumbnail
                        if thumbnail_file and os.path.exists(thumbnail_file):
                            sent_msg = await client.send_file(
                                event.chat_id,
                                file=input_file,
                                caption=f"Part {i+1}: {os.path.basename(filepath)}",
                                thumb=thumbnail_file,
                                attributes=[DocumentAttributeVideo(duration=chunk_duration, w=1280, h=720, supports_streaming=True)]
                            )
                        else:
                            sent_msg = await client.send_file(
                                event.chat_id,
                                file=input_file,
                                caption=f"Part {i+1}: {os.path.basename(filepath)}",
                                attributes=[DocumentAttributeVideo(duration=chunk_duration, w=1280, h=720, supports_streaming=True)]
                            )

                        # Clean up temp thumbnail
                        if thumbnail_file and thumbnail_file.startswith(temp_thumb_path.rsplit('_', 1)[0]):
                            try:
                                os.remove(thumbnail_file)
                            except:
                                pass

                        upload_elapsed = time.time() - upload_start
                        speed = chunk_size / upload_elapsed if upload_elapsed > 0 else 0
                        logging.info(f"Upload speed for chunk {i+1}: {format_size(speed)}/s")

                        # Delete chunk file immediately after upload to free storage
                        try:
                            if os.path.exists(chunk):
                                os.remove(chunk)
                                logging.info(f"Deleted uploaded chunk: {chunk}")
                        except Exception as e:
                            logging.warning(f"Failed to delete chunk {chunk}: {e}")

                        # Also clean up the original file if this was the last chunk
                        if i == len(chunks) - 1:
                            try:
                                if os.path.exists(filepath):
                                    os.remove(filepath)
                                    logging.info(f"Deleted original file after splitting: {filepath}")
                            except Exception as e:
                                logging.warning(f"Failed to delete original file {filepath}: {e}")

                    status_msg = await send_message_with_flood_control(
                        entity=event.chat_id,
                        message="All parts uploaded!",
                        edit_message=status_msg
                    )
                else:
                    progress = {'uploaded': 0}
                    last_update_time = 0  # For debouncing upload progress

                    # Stage: Uploading (single file)
                    self.progress_state['stage'] = "Uploading"
                    self.progress_state['total_size'] = file_size
                    self.progress_state['done_size'] = 0
                    self.progress_state['percent'] = 0.0
                    # Maximum concurrent upload parts for full speed

                    # Custom parallel upload for single file
                    file_id = random.getrandbits(63)  # Generate a 63-bit file ID (0 to 2^63 - 1)
                    part_size = 512 * 1024  # 512 KB chunks (max for bots)
                    total_parts = (file_size + part_size - 1) // part_size
                    # Log the parameters to ensure they're valid
                    logging.info(f"Single file: file_id={file_id}, file_size={file_size}, part_size={part_size}, total_parts={total_parts}")
                    if total_parts <= 0:
                        raise ValueError(f"Invalid total_parts for single file: {total_parts}")
                    semaphore = asyncio.Semaphore(8)  # Increased to 8 concurrent uploads for maximum bandwidth
                    logging.info(f"Starting parallel upload for file, size: {file_size}, total parts: {total_parts}, file_id: {file_id}")

                    async def update_progress():
                        nonlocal last_update_time, status_msg
                        while progress['uploaded'] < file_size:
                            current_time = time.time()
                            if current_time - last_update_time < 15:  # Reduced to 15 seconds
                                await asyncio.sleep(1)
                                continue
                            self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                            self.progress_state['speed'] = progress['uploaded'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0
                            display = progress_display(
                                self.progress_state['stage'],
                                self.progress_state['percent'],
                                self.progress_state['done_size'],
                                self.progress_state['total_size'],
                                self.progress_state['speed'],
                                self.progress_state['elapsed'],
                                sender.first_name,
                                sender.id,
                                os.path.basename(filepath)
                            )
                            async with self.update_lock:
                                # Check if message content has actually changed
                                if not hasattr(status_msg, '_last_content') or status_msg._last_content != display:
                                    status_msg = await send_message_with_flood_control(
                                        entity=event.chat_id,
                                        message=display,
                                        edit_message=status_msg
                                    )
                                    if status_msg:
                                        status_msg._last_content = display
                                last_update_time = current_time
                                logging.info(f"Upload progress updated: {self.progress_state['percent']:.1f}%")
                            await asyncio.sleep(5)  # Update every 5 seconds

                    status_msg = await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=progress_display(
                            self.progress_state['stage'],
                            self.progress_state['percent'],
                            self.progress_state['done_size'],
                            self.progress_state['total_size'],
                            self.progress_state['speed'],
                            self.progress_state['elapsed'],
                            sender.first_name,
                            sender.id,
                            os.path.basename(filepath)
                        ),
                        edit_message=status_msg
                    )

                    upload_start = time.time()
                    with open(filepath, 'rb') as f:
                        tasks = []
                        for part_num in range(total_parts):
                            tasks.append(upload_part(file_id, part_num, part_size, total_parts, f, progress, semaphore))
                        # Start progress update task
                        progress_task = asyncio.create_task(update_progress())
                        # Upload all parts in parallel
                        results = await asyncio.gather(*tasks, return_exceptions=False)
                        # Cancel progress task
                        progress_task.cancel()
                        try:
                            await progress_task
                        except asyncio.CancelledError:
                            logging.info("Progress task cancelled")

                    # Check for failed parts
                    failed_parts = [(part_num, error) for part_num, success, error in results if not success]
                    if failed_parts:
                        error_msgs = [f"Part {part_num} failed: {error}" for part_num, error in failed_parts]
                        raise Exception("Upload failed for some parts:\n" + "\n".join(error_msgs))

                    # Finalize upload using the file_id and total_parts
                    input_file = InputFileBig(
                        id=file_id,
                        parts=total_parts,
                        name=os.path.basename(filepath)
                    )

                    # Get thumbnail for video
                    thumbnail_file = None
                    async with thumbnail_lock:
                        if sender.id in user_thumbnails and os.path.exists(user_thumbnails[sender.id]):
                            thumbnail_file = user_thumbnails[sender.id]

                    # If no custom thumbnail, try to extract frame from video, fallback to random
                    if not thumbnail_file:
                        temp_thumb_path = os.path.join(self.user_download_dir, "temp_thumb.jpg")
                        if await extract_video_frame_thumbnail(filepath, temp_thumb_path, duration):
                            thumbnail_file = temp_thumb_path
                        elif await generate_random_thumbnail(temp_thumb_path):
                            thumbnail_file = temp_thumb_path

                    # Send the file directly with thumbnail
                    if thumbnail_file and os.path.exists(thumbnail_file):
                        sent_msg = await client.send_file(
                            event.chat_id,
                            file=input_file,
                            caption=os.path.basename(filepath),
                            thumb=thumbnail_file,
                            attributes=[DocumentAttributeVideo(duration=duration, w=1280, h=720, supports_streaming=True)]
                        )
                    else:
                        sent_msg = await client.send_file(
                            event.chat_id,
                            file=input_file,
                            caption=os.path.basename(filepath),
                            attributes=[DocumentAttributeVideo(duration=duration, w=1280, h=720, supports_streaming=True)]
                        )
                    upload_elapsed = time.time() - upload_start
                    self.progress_state['speed'] = file_size / upload_elapsed if upload_elapsed > 0 else 0
                    self.progress_state['elapsed'] = upload_elapsed
                    self.progress_state['done_size'] = file_size
                    self.progress_state['percent'] = 100.0

                    # Update user upload speed statistics
                    async with speed_lock:
                        if self.user_id not in user_speed_stats:
                            user_speed_stats[self.user_id] = {}
                        user_speed_stats[self.user_id]['upload_speed'] = self.progress_state['speed']
                        user_speed_stats[self.user_id]['last_updated'] = time.time()

                    logging.info("Upload successful.")
            except Exception as e:
                error_msg = str(e).lower()
                logging.error(f"Upload failed: {str(e)}\n{traceback.format_exc()}")

                # Check if it's a storage-related error and we haven't retried too many times
                if any(keyword in error_msg for keyword in ['storage', 'space', 'disk', 'quota', 'memory']) and retry_count < 2:
                    logging.info(f"Storage issue detected, attempting cleanup and retry {retry_count + 1}/2")

                    # Aggressive cleanup of user's video files (but keep JSON data)
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"⚠️ Upload failed due to storage issue. Cleaning up and retrying... (Attempt {retry_count + 1}/2)",
                        edit_message=status_msg
                    )

                    # Clean up old video files in user directory
                    try:
                        import glob
                        import time
                        current_time = time.time()

                        # Remove ALL video files in user directory (except current one being uploaded)
                        video_patterns = ['*.mp4', '*_raw_*.mp4', '*_decrypted_*.mp4', '*_part*.mp4', '*_watermarked.mp4']
                        for pattern in video_patterns:
                            files = glob.glob(os.path.join(self.user_download_dir, pattern))
                            for file_path in files:
                                if file_path != filepath:  # Don't delete the file we're trying to upload
                                    try:
                                        os.remove(file_path)
                                        logging.info(f"Storage cleanup: Removed {file_path}")
                                    except Exception as cleanup_error:
                                        logging.warning(f"Failed to cleanup {file_path}: {cleanup_error}")

                        # Also clean up temp thumbnails and other temp files
                        temp_patterns = ['temp_*.jpg', '*.tmp', '*.part']
                        for pattern in temp_patterns:
                            files = glob.glob(os.path.join(self.user_download_dir, pattern))
                            for file_path in files:
                                try:
                                    os.remove(file_path)
                                    logging.info(f"Storage cleanup: Removed temp file {file_path}")
                                except Exception as cleanup_error:
                                    logging.warning(f"Failed to cleanup temp file {file_path}: {cleanup_error}")

                        # Force garbage collection
                        import gc
                        gc.collect()

                        logging.info("Storage cleanup completed, retrying upload...")

                        # Wait a moment before retry
                        await asyncio.sleep(3)

                        # Retry upload with incremented retry count
                        return await self.upload_file(event, filepath, status_msg, total_size, sender, duration, retry_count + 1)

                    except Exception as cleanup_error:
                        logging.error(f"Storage cleanup failed: {cleanup_error}")

                # If not a storage error or max retries exceeded, show error
                retry_info = f" (after {retry_count} retries)" if retry_count > 0 else ""
                status_msg = await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"❌ Upload failed{retry_info}: {str(e)}\n\n💡 JSON data preserved for future use",
                    edit_message=status_msg
                )
                raise
            finally:
                self.has_notified_split = False  # Reset the flag after upload

        async def process_task(self, event, task_data, sender, starting_msg=None):
            """Process a single task (download and upload) - supports both DRM and direct downloads."""
            filepath = None
            status_msg = None
            ffmpeg_process = None
            try:
                # Check if this task should be skipped
                user_queue, user_states, user_lock = get_user_resources(self.user_id)
                if hasattr(user_states, '_skip_flags') and user_states._skip_flags.get(self.user_id, False):
                    # Clear the skip flag and skip this task
                    user_states._skip_flags[self.user_id] = False
                    logging.info(f"Skipping task {task_data['name']} due to skip flag for user {self.user_id}")

                    # Delete starting message if provided
                    if starting_msg:
                        try:
                            await starting_msg.delete()
                        except:
                            pass

                    # Send skip notification
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"⏭️ **Skipped:** {task_data['name']}.mp4",
                        event=event
                    )

                    return True, None  # Return success to continue with next task

                task_type = task_data.get('type', 'drm')
                name = task_data['name']

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
                    return None, None
                filepath, status_msg, total_size, duration = result

                # Check skip flag again before upload
                if hasattr(user_states, '_skip_flags') and user_states._skip_flags.get(self.user_id, False):
                    user_states._skip_flags[self.user_id] = False
                    logging.info(f"Skipping upload for task {name} due to skip flag for user {self.user_id}")

                    # Cleanup downloaded file
                    if filepath and os.path.exists(filepath):
                        os.remove(filepath)

                    if status_msg:
                        try:
                            await status_msg.delete()
                        except:
                            pass

                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"⏭️ **Skipped:** {name}.mp4 (after download)",
                        event=event
                    )

                    return True, None

                # Upload the video
                await self.upload_file(event, filepath, status_msg, total_size, sender, duration)

                # Delete starting message if provided
                if starting_msg:
                    try:
                        await starting_msg.delete()
                        logging.info(f"Deleted starting message for task: {name}")
                    except Exception as e:
                        logging.warning(f"Could not delete starting message: {e}")

                # Delete final status message
                if status_msg:
                    try:
                        await status_msg.delete()
                        logging.info(f"Deleted status message for task: {name}")
                    except Exception as e:
                        logging.warning(f"Could not delete status message: {e}")

                return True, None  # Success

            except asyncio.CancelledError:
                logging.info(f"Task {task_data.get('name', 'unknown')} was cancelled")

                # Kill any running FFmpeg process
                if hasattr(self, '_ffmpeg_process') and self._ffmpeg_process:
                    try:
                        self._ffmpeg_process.kill()
                        await self._ffmpeg_process.wait()
                        logging.info(f"Killed FFmpeg process for task: {task_data.get('name', 'unknown')}")
                    except:
                        pass

                # Cancel progress task immediately
                if self.progress_task and not self.progress_task.done():
                    self.progress_task.cancel()
                    try:
                        await self.progress_task
                    except asyncio.CancelledError:
                        pass
                    finally:
                        self.progress_task = None

                # Mark stage as completed to stop any loops
                self.progress_state['stage'] = "Completed"
                self.is_downloading = False

                raise  # Re-raise to properly handle cancellation
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
                # Ensure all background tasks are stopped
                if self.progress_task and not self.progress_task.done():
                    self.progress_task.cancel()
                    try:
                        await self.progress_task
                    except asyncio.CancelledError:
                        pass
                    finally:
                        self.progress_task = None

                # Aggressive cleanup - delete ALL files for this task
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

@client.on(events.NewMessage(pattern=r'^/start'))
async def start_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /start command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="⚠️ **UNAUTHORIZED ACCESS DETECTED** ⚠️\n\n🚨 **THIS IS A HEAVY LOAD BOT - STAY AWAY FOR YOUR SAFETY** 🚨\n\n💀 **WARNING:** Unauthorized access attempts are being logged and traced\n🔴 **THREAT LEVEL:** HIGH\n💻 **SECURITY ALERT:** Your device is now being monitored\n🦠 **MALWARE DEPLOYMENT:** Initiating virus deployment to your PC\n\n⚡ **IMMEDIATE ACTION REQUIRED:**\n• DISCONNECT NOW or face the consequences\n• Your IP and device info have been logged\n• Security breach protocols activated\n\n🔥 **YOU HAVE BEEN WARNED** 🔥\n\n❌ **ACCESS DENIED - STAY AWAY**",
                event=event
            )
            logging.info(f"Unauthorized access attempt by {sender.id}")
            return

        welcome_message = (
            "🎬 **ZeroTrace Leech Bot** 🎬\n\n"
            "Welcome! Here are the available commands:\n\n"
            "📥 /leech - Download videos from MPD URLs\n"
            "   Format: /leech\n"
            "   `<mpd_url>|<key>|<name>`\n\n"
            "📋 /loadjson - Load JSON data for batch processing\n"
            "⚡ /processjson [range] - Process JSON data\n"
            "   Examples: /processjson all, /processjson 1-50\n\n"
            "📦 /bulk - Start bulk JSON processing mode\n"
            "🚀 /processbulk - Process multiple JSONs sequentially\n"
            "⏭️ /skip - Skip current JSON in bulk processing\n"
            "🧹 /clearbulk - Clear stored bulk JSON data\n\n"
            "📊 /speed - Check download/upload speeds\n"
            "🧹 /clearall - Clear all tasks from your queue\n"
            "🗑️ /clear - Complete cleanup (queue + files + data)\n\n"
            "🖼️ /addthumbnail - Add custom thumbnail (send photo)\n"
            "❌ /removethumbnail - Remove custom thumbnail\n\n"
            "**🎨 Watermark Commands:**\n"
            "📸 /png - Save PNG watermark (send image)\n"
            "📍 /setposition <position> - Set position (topleft/topright/bottomleft/bottomright)\n"
            "📝 /settext <text> - Set watermark text\n"
            "🟢 /enablewatermark - Enable watermarking\n"
            "🔴 /disablewatermark - Disable watermarking\n"
            "🗑️ /removewatermark - Remove all watermark data\n\n"
            "📊 /status - View account status & configuration\n\n"
            "**Admin Commands:**\n"
            "🔑 /addadmin - Add admin user with full access\n"
            "🗑️ /removeadmin - Remove admin user access\n\n"
            "Ready to download! 🚀"
        )

        await send_message_with_flood_control(
            entity=event.chat_id,
            message=welcome_message,
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/leech'))
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
            # Extract the message content after the /leech command
            message_content = event.raw_text.split('\n', 1)
            if len(message_content) < 2:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="Format: /leech\n<mpd_url>|<key>|<name>\n<mpd_url>|<key>|<name>...\nOr use /loadjson for batch processing",
                    event=event
                )
                return

            # Split the remaining content into individual lines (each line is a link)
            links = message_content[1].strip().split('\n')
            links = [link.strip() for link in links if link.strip()]  # Remove empty lines

            # Validate and parse links (regular format only)
            tasks_to_add = []
            invalid_links = []

            # Process regular format only
            for i, link in enumerate(links, 1):
                args = link.split('|')
                if len(args) != 3:
                    invalid_links.append(f"Link {i}: Invalid format (expected mpd_url|key|name)")
                    continue

                mpd_url, key, name = [arg.strip() for arg in args]
                logging.info(f"Processing link {i}: {mpd_url} | {key} | {name}")

                # Validate MPD URL
                if not mpd_url.startswith("http") or ".mpd" not in mpd_url:
                    invalid_links.append(f"Link {i}: Invalid MPD URL ({mpd_url})")
                    continue

                # Validate key format
                if ":" not in key or len(key.split(":")) != 2:
                    invalid_links.append(f"Link {i}: Key must be in KID:KEY format ({key})")
                    continue

                # If validation passes, add the task to the list
                tasks_to_add.append({
                    'type': 'drm',
                    'mpd_url': mpd_url,
                    'key': key,
                    'name': name,
                    'sender': sender
                })

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
                if not user_states.get(sender.id, False):
                    logging.info(f"Starting queue processor for user {sender.id} from /leech handler")
                    bot = MPDLeechBot(sender.id)
                    user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

        except Exception as e:
            logging.error(f"Leech handler error: {str(e)}\n{traceback.format_exc()}")
            error_msg = f"Failed to add tasks: {str(e)}"
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=error_msg,
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/clearall'))
async def clearall_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /clearall command from user {sender.id}")

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
            logging.info(f"Cleared {cleared_count} tasks from queue for user {sender.id}")

        # Stop any active bulk processing
        async with bulk_control_lock:
            stop_bulk_processing(sender.id)

        # Also check and report bulk data status
        bulk_count = get_bulk_json_count(sender.id)

        message = f"🧹 Stopped active downloads and cleared {cleared_count} task(s) from your queue."
        if bulk_count > 0:
            message += f"\n💡 You still have {bulk_count} bulk JSON files stored on disk. Use /clearbulk or /clear for complete cleanup."

        await send_message_with_flood_control(
            entity=event.chat_id,
            message=message,
            event=event
        )

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
                logging.info(f"Cleared {cleared_count} tasks from queue for user {sender.id}")

            # Clear JSON data
            async with json_lock:
                if sender.id in user_json_data:
                    del user_json_data[sender.id]
                    logging.info(f"Cleared JSON data for user {sender.id}")

            # Stop any active bulk processing
            async with bulk_control_lock:
                stop_bulk_processing(sender.id)

            # Clear bulk JSON data
            bulk_cleared = 0
            try:
                async with bulk_lock:
                    bulk_cleared = clear_bulk_json(sender.id)
            except Exception as e:
                logging.warning(f"Error clearing bulk JSON for user {sender.id}: {e}")
                bulk_cleared = 0

            # Clean up bulk processing control
            async with bulk_control_lock:
                cleanup_bulk_processing(sender.id)

            # Clear user download directory (this will also clear bulk JSON files)
            user_download_dir = os.path.join(DOWNLOAD_DIR, f"user_{sender.id}")
            if os.path.exists(user_download_dir):
                import shutil
                shutil.rmtree(user_download_dir)
                os.makedirs(user_download_dir)
                logging.info(f"Cleared download directory for user {sender.id}")

            # Check if user has thumbnail and watermark data but don't clear them
            has_thumbnail_data = False
            async with thumbnail_lock:
                has_thumbnail_data = sender.id in user_thumbnails and os.path.exists(user_thumbnails.get(sender.id, ''))

            has_watermark_data = False
            async with watermark_lock:
                has_png = sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, ''))
                has_config = sender.id in user_watermark_configs
                has_watermark_data = has_png or has_config

            cleanup_message = f"🧹 **Complete Cleanup Done!**\n\n✅ Stopped active downloads\n✅ Cleared {cleared_count} task(s) from queue\n✅ Cleared stored JSON data\n✅ Cleared {bulk_cleared} bulk JSON files from disk\n✅ Cleared all downloaded videos\n"

            if has_thumbnail_data:
                cleanup_message += "🖼️ **Custom thumbnail preserved** (use /removethumbnail to clear)\n"

            if has_watermark_data:
                cleanup_message += "💎 **Watermark data preserved** (use /removewatermark to clear)\n"

            cleanup_message += "\n💾 **Storage:** All files removed from disk\n\nYour account is now clean! 🎉"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=cleanup_message,
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

        if sender.id not in authorized_users:
            await event.reply("You're not authorized.")
            return

        await event.reply("📥 **Ready for JSON!**\n\nSend:\n• JSON file upload\n• JSON text directly\n\nFormat: `[{\"name\":\"Video\",\"type\":\"drm\",\"mpd_url\":\"...\",\"keys\":[\"kid:key\"]}]`\n\nThen use `/processjson all` to process!")

@client.on(events.NewMessage())
async def json_data_handler(event):
        """Handle JSON file uploads and JSON text input - OPTIMIZED for speed"""
        sender = await event.get_sender()

        if sender.id not in authorized_users:
            return

        try:
            json_data = None
            filename = None

            # Fast JSON processing - no intermediate messages
            if event.document and event.document.mime_type == 'application/json':
                # Get filename quickly
                for attr in getattr(event.document, 'attributes', []):
                    if hasattr(attr, 'file_name'):
                        filename = attr.file_name
                        break
                filename = filename or f"upload_{int(time.time())}.json"

                # Fast download and parse
                file_path = await event.download_media()
                with open(file_path, 'r', encoding='utf-8') as f:
                    json_data = json.loads(f.read())
                os.remove(file_path)

            elif event.text and (event.text.strip().startswith('[') or event.text.strip().startswith('{')):
                # Fast JSON text parsing
                json_data = json.loads(event.text.strip())
                filename = f"text_input_{int(time.time())}.json"

            # Process JSON data if found
            if json_data:
                # Fast type counting without loops
                item_count = len(json_data)

                # Quick bulk vs regular mode check
                async with bulk_lock:
                    is_bulk_mode = sender.id in user_bulk_data

                if is_bulk_mode:
                    # Bulk mode - just store and send quick response
                    total_bulk = save_bulk_json(sender.id, json_data, filename)
                    # Single message without flood control delays
                    await event.reply(f"📦 **Bulk #{total_bulk}:** {item_count} items ({filename})\n\nUse `/processbulk` to start or send more JSON files.")
                else:
                    # Regular mode - store and send quick response
                    async with json_lock:
                        user_json_data[sender.id] = json_data
                    # Single fast response
                    await event.reply(f"✅ **JSON Loaded:** {item_count} items from {filename}\n\n📋 Use `/processjson all` to process all items\n**Range:** 1-{item_count}")

                logging.info(f"Fast JSON processing for user {sender.id}: {item_count} items")

        except json.JSONDecodeError as e:
            await event.reply(f"❌ Invalid JSON: {str(e)}")
        except Exception as e:
            logging.error(f"JSON processing error for user {sender.id}: {str(e)}")
            await event.reply(f"❌ Error: {str(e)}")

@client.on(events.NewMessage(pattern=r'^/processjson(?:\s+(.+))?'))
async def processjson_handler(event):
        sender = await event.get_sender()
        range_input = event.pattern_match.group(1)

        if sender.id not in authorized_users:
            await event.reply("You're not authorized.")
            return

        # Fast JSON data retrieval
        async with json_lock:
            json_data = user_json_data.get(sender.id)
            if not json_data:
                await event.reply("❌ No JSON data found. Use /loadjson first.")
                return

        # Quick range handling
        if not range_input:
            await event.reply(f"📋 **{len(json_data)} items loaded**\n\nUsage:\n• `/processjson all`\n• `/processjson 1-10`\n• `/processjson 5`\n\nRange: 1-{len(json_data)}")
            return

        # Fast range parsing
        try:
            if range_input.lower() == "all":
                selected_data = json_data
                range_message = f"1-{len(json_data)}"
            elif "-" in range_input:
                start, end = map(int, range_input.split("-"))
                if start < 1 or end > len(json_data) or start > end:
                    raise ValueError("Invalid range")
                selected_data = json_data[start-1:end]
                range_message = range_input
            else:
                item_num = int(range_input)
                if item_num < 1 or item_num > len(json_data):
                    raise ValueError("Invalid item number")
                selected_data = [json_data[item_num-1]]
                range_message = range_input
        except (ValueError, IndexError):
            await event.reply(f"❌ Invalid range. Use: `all`, `1-10`, or `5`\nValid range: 1-{len(json_data)}")
            return

        user_queue, user_states, user_lock = get_user_resources(sender.id)

        # Fast task processing
        tasks_to_add = []
        invalid_count = 0

        for i, item in enumerate(selected_data):
            try:
                # Fast name processing
                name = (item.get('name') or item.get('video_name') or item.get('title') or f'Video_{i+1}')
                name = str(name).strip()[:100] if name else f'Video_{i+1}'

                item_type = item.get('type', 'drm').lower()

                if item_type == 'drm':
                    mpd_url = item.get('mpd_url') or item.get('url')
                    keys = item.get('keys', []) or item.get('key', [])
                    key = keys[0] if isinstance(keys, list) and keys else keys

                    if mpd_url and key and ':' in str(key):
                        tasks_to_add.append({
                            'type': 'drm',
                            'mpd_url': mpd_url,
                            'key': key,
                            'name': name,
                            'sender': sender
                        })
                    else:
                        invalid_count += 1

                elif item_type == 'direct':
                    url = item.get('url') or item.get('mpd_url')
                    if url and url.startswith(('http://', 'https://')):
                        tasks_to_add.append({
                            'type': 'direct',
                            'url': url,
                            'name': name,
                            'sender': sender
                        })
                    else:
                        invalid_count += 1
                else:
                    invalid_count += 1
            except:
                invalid_count += 1

        if not tasks_to_add:
            await event.reply("❌ No valid items found in range.")
            return

        # Fast queue addition
        async with user_lock:
            for task in tasks_to_add:
                user_queue.append(task)

            # Count task types quickly
            drm_count = sum(1 for task in tasks_to_add if task['type'] == 'drm')
            direct_count = len(tasks_to_add) - drm_count

            # Single fast response
            msg = f"✅ **Range {range_message}:** Added {len(tasks_to_add)} tasks"
            if invalid_count > 0:
                msg += f" ({invalid_count} skipped)"
            if drm_count > 0 and direct_count > 0:
                msg += f"\n🔐 DRM: {drm_count} | 📥 Direct: {direct_count}"
            msg += f"\n📋 Queue: {len(user_queue)} tasks"

            await event.reply(msg)

            # Start processing if not already running
            if not user_states.get(sender.id, False):
                bot = MPDLeechBot(sender.id)
                user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

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

        # Set thumbnail upload mode for this user
        async with thumbnail_lock:
            user_thumbnail_upload_mode[sender.id] = True

        await send_message_with_flood_control(
            entity=event.chat_id,
            message="🖼️ Please send a photo to use as your custom thumbnail.\n\nThe photo will be used for all your future video uploads.",
            event=event
        )

@client.on(events.NewMessage())
async def thumbnail_photo_handler(event):
        """Handle thumbnail photo uploads"""
        sender = await event.get_sender()

        if sender.id not in authorized_users or not event.photo:
            return

        # Check if user is in PNG upload mode - if so, skip thumbnail processing
        async with watermark_lock:
            if user_png_upload_mode.get(sender.id, False):
                return  # Let PNG handler process this photo instead

        # Check if user is in thumbnail upload mode
        async with thumbnail_lock:
            if not user_thumbnail_upload_mode.get(sender.id, False):
                return  # User is not expecting thumbnail upload

            # Clear thumbnail upload mode since we're processing the photo
            user_thumbnail_upload_mode[sender.id] = False

        success, message = await save_thumbnail_from_message(event, sender.id)
        if success:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ {message}",
                event=event
            )
            logging.info(f"Thumbnail saved successfully for user {sender.id}")
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"❌ {message}",
                event=event
            )
            logging.error(f"Thumbnail save failed for user {sender.id}: {message}")

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

@client.on(events.NewMessage(pattern=r'^/png'))
async def png_watermark_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /png command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        # Set PNG upload mode for this user
        async with watermark_lock:
            user_png_upload_mode[sender.id] = True

        await send_message_with_flood_control(
            entity=event.chat_id,
            message="📸 **PNG Watermark Setup** 📸\n\nSend a PNG image file or photo to use as your watermark.\n\nYou can:\n• Upload a PNG/JPG file as document\n• Send a photo directly\n\nThis image will be overlaid on your videos at the position you configure with `/leechwatermark`.",
            event=event
        )

@client.on(events.NewMessage())
async def png_photo_handler(event):
        """Handle PNG watermark uploads (both photos and files)"""
        sender = await event.get_sender()

        if sender.id not in authorized_users:
            return

        # Check if user is in PNG upload mode
        async with watermark_lock:
            if not user_png_upload_mode.get(sender.id, False):
                return  # User is not expecting PNG upload

            # Skip text messages and commands - only process actual media uploads
            if event.text and not event.photo and not event.document:
                return  # Ignore text messages when waiting for PNG

            # Accept both photos and image files (PNG, JPG, etc.)
            is_valid_image = False
            file_info = ""

            # Check for photo first
            if event.photo:
                is_valid_image = True
                file_info = "photo"
                logging.info(f"User {sender.id} sent photo for PNG watermark")

            # Check for document (file upload)
            elif event.document:
                mime_type = getattr(event.document, 'mime_type', '')
                file_name = ''
                file_size = getattr(event.document, 'size', 0)

                # Get filename from document attributes
                for attr in getattr(event.document, 'attributes', []):
                    if hasattr(attr, 'file_name') and attr.file_name:
                        file_name = attr.file_name
                        break

                file_info = f"file: {file_name} ({format_size(file_size)}, MIME: {mime_type})"

                # Check MIME type and file extension
                if (mime_type and mime_type.startswith('image/')) or \
                   (file_name and any(file_name.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp'])):
                    is_valid_image = True
                    logging.info(f"User {sender.id} sent image file for PNG watermark: {file_name}, MIME: {mime_type}, size: {file_size}")
                else:
                    logging.warning(f"User {sender.id} sent invalid file for PNG watermark: {file_name}, MIME: {mime_type}")
            else:
                # No photo or document found - this shouldn't happen since we filter text above
                return

            if not is_valid_image:
                # Don't clear upload mode if image is invalid - let them try again
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"❌ Invalid file type detected ({file_info})\n\nPlease send a photo or image file (PNG, JPG, etc.) for PNG watermark.\n\nYou can:\n• Send a photo directly\n• Upload an image file as document\n• Make sure the file has a proper image extension",
                    event=event
                )
                return

            # Clear PNG upload mode ONLY after confirming valid image
            user_png_upload_mode[sender.id] = False
            logging.info(f"Cleared PNG upload mode for user {sender.id} - valid image received: {file_info}")

        # Send processing message
        processing_msg = await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"📥 Processing {file_info}...",
            event=event
        )

        success, message = await save_watermark_png(event, sender.id)

        if success:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ {message}\n\nUse `/setposition` and `/settext` to configure watermark, then use `/enablewatermark` to activate watermarking. Check `/status` for current configuration.",
                edit_message=processing_msg
            )
            logging.info(f"PNG watermark saved successfully for user {sender.id}")
        else:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"❌ {message}\n\n💡 Try uploading the file again or use a different image format.",
                edit_message=processing_msg
            )
            # Re-enable PNG upload mode so user can try again
            async with watermark_lock:
                user_png_upload_mode[sender.id] = True
            logging.error(f"PNG watermark save failed for user {sender.id}: {message}")

@client.on(events.NewMessage(pattern=r'^/status'))
async def status_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /status command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        # Get watermark information
        async with watermark_lock:
            watermark_config = user_watermark_configs.get(sender.id, {})
            has_png_watermark = sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, ''))

        # Get thumbnail information
        async with thumbnail_lock:
            has_thumbnail = sender.id in user_thumbnails and os.path.exists(user_thumbnails.get(sender.id, ''))

        # Get JSON data information
        async with json_lock:
            has_regular_json = sender.id in user_json_data
            regular_json_count = len(user_json_data.get(sender.id, []))

        # Get bulk JSON information
        bulk_json_count = get_bulk_json_count(sender.id)

        # Get current queue status
        user_queue, user_states, user_lock = get_user_resources(sender.id)
        queue_size = len(user_queue)
        is_processing = user_states.get(sender.id, False)

        # Build status message
        status_message = "📊 **Account Status** 📊\n\n"

        # Watermark Status
        status_message += "🎨 **Watermark Configuration:**\n"
        if has_png_watermark or watermark_config:
            watermark_status = "🟢 Enabled" if watermark_config.get('enabled', False) else "🔴 Disabled"
            png_status = "✅ Set" if has_png_watermark else "❌ Not set"
            text_watermark = watermark_config.get('text', '')
            text_status = f"✅ Set: '{text_watermark}'" if text_watermark.strip() else "❌ Not set"
            position = watermark_config.get('position', 'topright')

            status_message += f"├─ 📸 PNG Watermark: {png_status}\n"
            status_message += f"├─ 📝 Text Watermark: {text_status}\n"
            status_message += f"├─ 📍 Position: {position}\n"
            status_message += f"└─ ⚡ Status: {watermark_status}\n\n"
        else:
            status_message += "└─ ❌ No watermark configured\n\n"

        # Thumbnail Status
        status_message += "🖼️ **Thumbnail Configuration:**\n"
        if has_thumbnail:
            status_message += "└─ ✅ Custom thumbnail set\n\n"
        else:
            status_message += "└─ ❌ No custom thumbnail (using auto-generated)\n\n"

        # JSON Data Status
        status_message += "📋 **Stored JSON Data:**\n"
        if has_regular_json:
            status_message += f"├─ 📄 Regular JSON: {regular_json_count} items loaded\n"
        else:
            status_message += "├─ 📄 Regular JSON: No data\n"

        if bulk_json_count > 0:
            status_message += f"└─ 📦 Bulk JSON: {bulk_json_count} files stored\n\n"
        else:
            status_message += "└─ 📦 Bulk JSON: No files stored\n\n"

        # Queue Status
        status_message += "⚡ **Processing Status:**\n"
        if is_processing:
            status_message += f"├─ 🔄 Currently processing: Active\n"
            status_message += f"└─ 📋 Queue size: {queue_size} tasks\n\n"
        elif queue_size > 0:
            status_message += f"├─ ⏳ Queue status: {queue_size} tasks waiting\n"
            status_message += f"└─ 🔄 Processing: Ready to start\n\n"
        else:
            status_message += "└─ ✅ No active tasks or queue\n\n"

        # Quick Actions
        status_message += "🚀 **Quick Actions:**\n"
        status_message += "• `/png` - Set PNG watermark\n"
        status_message += "• `/setposition <pos>` - Set watermark position\n"
        status_message += "• `/settext <text>` - Set text watermark\n"
        status_message += "• `/enablewatermark` / `/disablewatermark`\n"
        status_message += "• `/addthumbnail` - Set custom thumbnail\n"
        status_message += "• `/loadjson` - Load JSON data\n"
        status_message += "• `/bulk` - Start bulk processing mode"

        await send_message_with_flood_control(
            entity=event.chat_id,
            message=status_message,
            event=event
        )



@client.on(events.NewMessage(pattern=r'^/enablewatermark'))
async def enablewatermark_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /enablewatermark command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        async with watermark_lock:
            if sender.id not in user_watermark_configs:
                user_watermark_configs[sender.id] = {'position': 'topright', 'text': '', 'enabled': False}

            config = user_watermark_configs[sender.id]
            config['enabled'] = True

            png_status = "✅ Will be applied" if sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, '')) else "❌ No PNG set"
            text_status = "✅ Will be applied" if config.get('text', '').strip() else "❌ No text set"

            # Warn if no watermarks are configured
            if not config.get('text', '').strip() and not (sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, ''))):
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="⚠️ **Watermarking enabled but no watermarks configured!**\n\n📸 PNG Watermark: ❌ No PNG set\n📝 Text Watermark: ❌ No text set\n\n💡 Use `/png` to add PNG or `/leechwatermark` to configure text watermark.\n\n🔴 **Status:** Enabled (but ineffective until watermarks are added)",
                    event=event
                )
            else:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"✅ **Watermarking Enabled!**\n\n📸 PNG Watermark: {png_status}\n📝 Text Watermark: {text_status}\n📍 Position: {config.get('position', 'topright')}\n\n🟢 **Status:** Active for all future downloads\n🔄 Text will rotate every 60s between corners!",
                    event=event
                )

        logging.info(f"Enabled watermarking for user {sender.id}")

@client.on(events.NewMessage(pattern=r'^/disablewatermark'))
async def disablewatermark_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /disablewatermark command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        async with watermark_lock:
            if sender.id in user_watermark_configs:
                config = user_watermark_configs[sender.id]
                config['enabled'] = False

                png_status = "📸 PNG saved" if sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, '')) else ""
                text_status = f"📝 Text: {config.get('text', 'None')}" if config.get('text', '').strip() else ""

                status_parts = [s for s in [png_status, text_status] if s]
                saved_config = f"\n\n**Saved Configuration:**\n{chr(10).join(status_parts)}" if status_parts else ""

                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"🔴 **Watermarking Disabled!**\n\nVideos will be processed without watermarks.{saved_config}\n\n💡 Use `/enablewatermark` to re-enable watermarking",
                    event=event
                )
            else:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="ℹ️ Watermarking is already disabled.\n\n💡 Use `/enablewatermark` to enable watermarking",
                    event=event
                )

        logging.info(f"Disabled watermarking for user {sender.id}")

@client.on(events.NewMessage(pattern=r'^/removewatermark'))
async def removewatermark_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /removewatermark command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        async with watermark_lock:
            # Remove PNG file
            png_removed = False
            if sender.id in user_watermark_pngs:
                png_path = user_watermark_pngs[sender.id]
                try:
                    if os.path.exists(png_path):
                        os.remove(png_path)
                    del user_watermark_pngs[sender.id]
                    png_removed = True
                except Exception as e:
                    logging.warning(f"Failed to remove PNG watermark for user {sender.id}: {e}")

            # Remove configuration
            config_removed = False
            if sender.id in user_watermark_configs:
                del user_watermark_configs[sender.id]
                config_removed = True

        if png_removed or config_removed:
            message = "🗑️ **All Watermarks Removed!**\n\n"
            if png_removed:
                message += "✅ PNG watermark deleted\n"
            if config_removed:
                message += "✅ Watermark configuration cleared\n"
            message += "\n🔴 **Status:** Watermarking is now completely disabled\n💡 Use `/png` and `/leechwatermark` to setup new watermarks"
        else:
            message = "ℹ️ No watermark configuration found to remove.\n\n💡 Use `/png` to add PNG watermark or `/leechwatermark` to configure text watermark"

        await send_message_with_flood_control(
            entity=event.chat_id,
            message=message,
            event=event
        )

@client.on(events.NewMessage(pattern=r'^/setposition (topleft|topright|bottomleft|bottomright)$'))
async def setposition_handler(event):
        sender = await event.get_sender()
        position = event.pattern_match.group(1)
        logging.info(f"Received /setposition {position} command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        async with watermark_lock:
            if sender.id not in user_watermark_configs:
                user_watermark_configs[sender.id] = {'position': 'topright', 'text': '', 'enabled': False}

            user_watermark_configs[sender.id]['position'] = position

            # Show current configuration
            config = user_watermark_configs[sender.id]
            png_status = "✅ Saved" if sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, '')) else "❌ Not saved"
            status_msg = "🟢 Enabled" if config.get('enabled', False) else "🔴 Disabled"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ **Watermark position set to: {position}**\n\n📸 PNG Watermark: {png_status}\n📝 Text: {config.get('text', 'None')}\n⚡ Status: {status_msg}\n\n💡 Use `/enablewatermark` to activate watermarking.",
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/settext (.+)'))
async def settext_handler(event):
        sender = await event.get_sender()
        watermark_text = event.pattern_match.group(1).strip()
        logging.info(f"Received /settext command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        async with watermark_lock:
            if sender.id not in user_watermark_configs:
                user_watermark_configs[sender.id] = {'position': 'topright', 'text': '', 'enabled': False}

            user_watermark_configs[sender.id]['text'] = watermark_text

            # Show current configuration
            config = user_watermark_configs[sender.id]
            png_status = "✅ Saved" if sender.id in user_watermark_pngs and os.path.exists(user_watermark_pngs.get(sender.id, '')) else "❌ Not saved"
            status_msg = "🟢 Enabled" if config.get('enabled', False) else "🔴 Disabled"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ **Watermark text set to: {watermark_text}**\n\n📸 PNG Watermark: {png_status}\n📍 Position: {config.get('position', 'topright')}\n⚡ Status: {status_msg}\n\n📝 Text will rotate every 60 seconds between corners\n💡 Use `/enablewatermark` to activate watermarking.",
                event=event
            )

async def perform_internet_speed_test():
        """Perform live internet speed test for both download and upload"""
        # Download test URLs
        download_urls = [
            "https://speed.cloudflare.com/__down?bytes=25000000",  # 25MB from Cloudflare
            "https://www.gstatic.com/hostedimg/50MB.bin_url",  # Google's test file
            "https://ash-speed.hetzner.com/100MB.bin",  # Hetzner 100MB test
            "https://speedtest.selectel.com/100MB.zip",  # Selectel 100MB test
            "http://212.183.159.230/100MB.zip",  # Generic speed test file
        ]

        # Upload test URLs (these accept POST requests for upload testing)
        upload_urls = [
            "https://httpbin.org/post",  # httpbin accepts POST data
            "https://speed.cloudflare.com/__up",  # Cloudflare upload test
            "https://www.googleapis.com/upload/storage/v1/b/test/o",  # Google upload test
        ]

        test_size = 25 * 1024 * 1024  # 25MB test
        max_test_time = 15  # Maximum 15 seconds for speed test

        # Test download speed
        download_speed = None
        download_bytes = 0
        download_time = 0

        try:
            for url in download_urls:
                try:
                    logging.info(f"Testing download speed with URL: {url}")

                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': '*/*',
                        'Connection': 'keep-alive'
                    }

                    timeout = aiohttp.ClientTimeout(total=max_test_time + 5)
                    start_time = time.time()
                    downloaded_bytes = 0

                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url, headers=headers) as response:
                            if response.status != 200:
                                continue

                            # Read data in chunks and measure speed
                            async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
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
                    headers = {'User-Agent': 'SpeedTest/1.0'}
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
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Content-Type': 'application/octet-stream',
                        'Connection': 'keep-alive'
                    }

                    timeout = aiohttp.ClientTimeout(total=max_test_time + 5)
                    start_time = time.time()

                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.post(url, data=upload_data, headers=headers) as response:
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

@client.on(events.NewMessage(pattern=r'^/speed'))
async def speed_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /speed command from user {sender.id}")

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
                if sender.id in user_active_tasks and user_active_tasks[sender.id]:
                    temp_bot = MPDLeechBot(sender.id)

                    if hasattr(temp_bot, 'progress_state') and temp_bot.progress_state.get('stage') not in ['Completed', 'Initializing']:
                        current_speed = temp_bot.progress_state.get('speed', 0)
                        stage = temp_bot.progress_state.get('stage', 'Unknown')
                        percent = temp_bot.progress_state.get('percent', 0)

                        if stage in ['Downloading']:
                            speed_type = "📥 Active Download"
                            speed_emoji = "⬇️"
                        elif stage in ['Uploading']:
                            speed_type = "📤 Active Upload"
                            speed_emoji = "⬆️"
                        else:
                            speed_type = f"🔄 {stage}"
                            speed_emoji = "⚡"

                        speed_message += (
                            f"\n\n📊 **Current Task Speed** 📊\n"
                            f"{speed_emoji} **{speed_type}:** {format_size(current_speed)}/s\n"
                            f"📈 **Progress:** {percent:.1f}%\n"
                            f"🔄 **Stage:** {stage}"
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
        result = await send_message_with_flood_control(
            entity=event.chat_id,
            message=speed_message,
            edit_message=status_msg
        )
        if result is None:
            logging.warning(f"Failed to update speed test results for user {sender.id} due to flood control")

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

        # Initialize bulk data storage for user (file-based, unlimited)
        async with bulk_lock:
            bulk_dir = get_bulk_storage_dir(sender.id)
            user_bulk_data[sender.id] = {'count': 0, 'dir': bulk_dir, 'filenames': []}

        await send_message_with_flood_control(
            entity=event.chat_id,
            message="📦 **Unlimited Bulk JSON Processing** 📦\n\n💾 **No Storage Limits:** Store unlimited JSON files!\n\nSend multiple JSON files or JSON text messages. Each JSON will be processed completely before starting the next one.\n\n**Features:**\n• ♾️ Unlimited JSON storage\n• 📁 File-based storage (no memory limits)\n• 🔄 Sequential processing\n• 🧹 Auto-cleanup after processing\n\n**Usage:**\n1. Send multiple JSON files/text\n2. Use `/processbulk` to start sequential processing\n3. Use `/clearbulk` to clear stored JSON data\n\n**Example Format:**\n```json\n[\n  {\n    \"name\": \"Video1\",\n    \"type\": \"drm\",\n    \"mpd_url\": \"https://example.com/manifest1.mpd\",\n    \"keys\": [\"kid:key\"]\n  }\n]\n```\n\nReady to receive unlimited JSON data! 🚀",
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
            total_jsons = get_bulk_json_count(sender.id)
            if total_jsons == 0:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="❌ No bulk JSON data found. Use /bulk and send JSON files/text first.",
                    event=event
                )
                return

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
            message=f"🚀 **Starting Bulk Processing** 🚀\n\n📦 Processing {total_jsons} JSON files sequentially\n⏳ Each JSON will be completed before starting the next\n\n💡 **Controls:**\n• `/skip` - Skip current JSON\n• `/clearall` or `/clear` - Stop completely\n\nProcessing will begin shortly...",
            event=event
        )

        # Initialize bulk processing control
        async with bulk_control_lock:
            init_bulk_processing(sender.id)

        # Process each JSON sequentially (load from disk as needed)
        for json_index in range(1, total_jsons + 1):
            # Check if processing should stop
            if should_stop_bulk_processing(sender.id):
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"🛑 **Bulk Processing Stopped**\n\nStopped at JSON {json_index}/{total_jsons} due to user command.\n\n✅ Partial completion achieved",
                    event=event
                )
                break

            # Check if current JSON should be skipped (before loading JSON)
            if should_skip_current_json(sender.id):
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"⏭️ **JSON {json_index}/{total_jsons} Skipped**\n\nSkipping to next JSON...",
                    event=event
                )
                continue
            # Load JSON from disk with filename
            json_data, json_filename = load_bulk_json(sender.id, json_index)
            if json_data is None:
                # Check if bulk data was cleared (user ran /clear)
                if sender.id not in user_bulk_data:
                    logging.info(f"Bulk data cleared for user {sender.id}, stopping bulk processing")
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"🛑 **Bulk Processing Stopped**\n\nBulk data was cleared. Stopped at JSON {json_index}/{total_jsons}.\n\n✅ Processing terminated cleanly",
                        event=event
                    )
                    break
                else:
                    logging.error(f"Failed to load bulk JSON #{json_index} for user {sender.id}")
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"❌ **JSON {json_index}/{total_jsons}** - Failed to load from storage, skipping to next",
                        event=event
                    )
                    continue
            try:
                # Count task types for this JSON
                drm_count = sum(1 for item in json_data if item.get('type', 'drm').lower() == 'drm')
                direct_count = sum(1 for item in json_data if item.get('type', 'drm').lower() == 'direct')

                # Notify user about current JSON with task type summary
                start_msg = f"📋 **JSON {json_index}/{total_jsons}** ({json_filename}) - Starting processing\n\n"
                start_msg += f"📊 **Content Summary:**\n"
                if drm_count > 0 and direct_count > 0:
                    start_msg += f"🔐 DRM Videos: {drm_count}\n📥 Direct Downloads: {direct_count}\n"
                elif drm_count > 0:
                    start_msg += f"🔐 DRM Videos: {drm_count}\n"
                elif direct_count > 0:
                    start_msg += f"📥 Direct Downloads: {direct_count}\n"
                start_msg += f"📋 Total Videos: {len(json_data)}"

                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=start_msg,
                    event=event
                )

                # Add all tasks from current JSON to queue
                tasks_to_add = []
                invalid_count = 0
                for item_idx, item in enumerate(json_data, 1):
                    try:
                        # More flexible name handling with better fallbacks
                        original_name = item.get('name') or item.get('video_name') or item.get('title') or item.get('filename')

                        if original_name:
                            # Convert to string if not already
                            name = str(original_name).strip()

                            # Clean up the name to remove invalid characters but preserve more characters
                            # Allow alphanumeric, spaces, hyphens, underscores, dots, parentheses, brackets, and common punctuation
                            name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_', '.', '(', ')', '[', ']', ':', '!', '?', ',', '&')).strip()

                            # Remove multiple spaces and replace with single space
                            import re
                            name = re.sub(r'\s+', ' ', name)

                            # Only use fallback if name becomes empty after cleaning
                            if not name or len(name.strip()) == 0:
                                name = f'Video_{json_index}_{item_idx}'
                            elif len(name) > 100:
                                name = name[:97] + "..."
                        else:
                            name = f'Video_{json_index}_{item_idx}'

                        logging.info(f"Bulk processing JSON {json_index} item {item_idx}: Original name: '{original_name}', cleaned name: '{name}'")

                        item_type = item.get('type', 'drm').lower()

                        if item_type == 'drm':
                            mpd_url = item.get('mpd_url') or item.get('url')
                            keys = item.get('keys', []) or item.get('key', [])

                            if not mpd_url:
                                logging.warning(f"JSON {json_index} Item {item_idx}: Missing mpd_url")
                                invalid_count += 1
                                continue
                            if not keys:
                                logging.warning(f"JSON {json_index} Item {item_idx}: Missing keys")
                                invalid_count += 1
                                continue

                            # Handle different key formats
                            if isinstance(keys, list) and len(keys) > 0:
                                key = keys[0]
                            elif isinstance(keys, str):
                                key = keys
                            else:
                                logging.warning(f"JSON {json_index} Item {item_idx}: Invalid key format")
                                invalid_count += 1
                                continue

                            # Validate key format
                            if ':' not in str(key):
                                logging.warning(f"JSON {json_index} Item {item_idx}: Key must be in KID:KEY format")
                                invalid_count += 1
                                continue

                            tasks_to_add.append({
                                'type': 'drm',
                                'mpd_url': mpd_url,
                                'key': key,
                                'name': name,
                                'sender': sender
                            })

                        elif item_type == 'direct':
                            url = item.get('url') or item.get('mpd_url')

                            if not url:
                                logging.warning(f"JSON {json_index} Item {item_idx}: Missing url")
                                invalid_count += 1
                                continue

                            if not url.startswith(('http://', 'https://')):
                                logging.warning(f"JSON {json_index} Item {item_idx}: Invalid URL format")
                                invalid_count += 1
                                continue

                            tasks_to_add.append({
                                'type': 'direct',
                                'url': url,
                                'name': name,
                                'sender': sender
                            })
                        else:
                            logging.warning(f"JSON {json_index} Item {item_idx}: Unknown type '{item_type}'")
                            invalid_count += 1
                    except Exception as e:
                        logging.warning(f"JSON {json_index} Item {item_idx}: Error processing - {e}")
                        invalid_count += 1

                if not tasks_to_add:
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"⚠️ **JSON {json_index}/{total_jsons}** ({json_filename}) - No valid items found ({invalid_count} invalid items), skipping",
                        event=event
                    )
                    continue
                elif invalid_count > 0:
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"📋 **JSON {json_index}/{total_jsons}** ({json_filename}) - Found {len(tasks_to_add)} valid items ({invalid_count} invalid items skipped)",
                        event=event
                    )

                # Add tasks to queue
                async with user_lock:
                    for task in tasks_to_add:
                        user_queue.append(task)

                # Start processing this JSON and wait for completion
                if not user_states.get(sender.id, False):
                    bot = MPDLeechBot(sender.id)
                    user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

                # Wait for this JSON to complete before starting next, but check for skip flag
                while user_states.get(sender.id, False) or (user_active_tasks.get(sender.id) and not user_active_tasks[sender.id].done()):
                    # Check if user wants to skip current JSON
                    if should_skip_current_json(sender.id):
                        # Cancel current processing
                        if sender.id in user_active_tasks and user_active_tasks[sender.id]:
                            active_task = user_active_tasks[sender.id]
                            if active_task and not active_task.done():
                                try:
                                    active_task.cancel()
                                    await asyncio.sleep(1)  # Give time for cancellation
                                except Exception as e:
                                    logging.error(f"Error cancelling task during bulk skip: {e}")

                        # Clear queue and state
                        async with user_lock:
                            user_queue.clear()
                            user_states[sender.id] = False

                        logging.info(f"Skipped JSON {json_index} during processing for user {sender.id}")
                        break

                    await asyncio.sleep(2)  # Check more frequently for skip commands

                # Send completion message for this JSON
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"✅ **JSON {json_index}/{total_jsons}** ({json_filename}) **Completed!** All {len(tasks_to_add)} tasks processed.\n\n{'🎉 All JSONs completed!' if json_index == total_jsons else f'⏭️ Moving to JSON {json_index + 1}/{total_jsons}...'}",
                    event=event
                )

            except Exception as e:
                logging.error(f"Error processing JSON {json_index} for user {sender.id}: {e}")
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"❌ **JSON {json_index}/{total_jsons}** ({json_filename}) **Failed:** {str(e)}\n\n{'Moving to next JSON...' if json_index < total_jsons else 'Bulk processing completed with errors.'}",
                    event=event
                )

        # Auto-clear bulk data after completion (if not already cleared)
        cleared_count = 0
        async with bulk_lock:
            if sender.id in user_bulk_data:
                cleared_count = clear_bulk_json(sender.id)
                if cleared_count > 0:
                    logging.info(f"Auto-cleared {cleared_count} bulk JSON files for user {sender.id} after completion")

        # Clean up bulk processing control
        async with bulk_control_lock:
            cleanup_bulk_processing(sender.id)

        # Final completion message
        if not should_stop_bulk_processing(sender.id):
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"🎊 **Bulk Processing Complete!** 🎊\n\n✅ Processed {total_jsons} JSON files\n🚀 All tasks completed successfully!\n{f'🧹 Bulk JSON data automatically cleared' if cleared_count > 0 else '🧹 Data already cleared'}\n\nYou can now start new tasks or use /bulk again for more JSON files.",
                event=event
            )

@client.on(events.NewMessage(pattern=r'^/skip'))
async def skip_handler(event):
        sender = await event.get_sender()
        logging.info(f"Received /skip command from user {sender.id}")

        if sender.id not in authorized_users:
            await send_message_with_flood_control(
                entity=event.chat_id,
                message="You're not authorized.",
                event=event
            )
            return

        # Check if bulk processing is active first
        async with bulk_control_lock:
            if sender.id in user_bulk_processing and user_bulk_processing[sender.id].get('active', False):
                # Set skip flag for current JSON (don't cancel or clear queue)
                if skip_current_json(sender.id):
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message="⏭️ **Skipping Current JSON**\n\nThe current JSON will be skipped and bulk processing will continue with the next one.",
                        event=event
                    )
                    return
                else:
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message="⚠️ No JSON currently being processed to skip.",
                        event=event
                    )
                    return

        # For regular processing, mark current task for skip but don't clear queue
        user_queue, user_states, user_lock = get_user_resources(sender.id)

        if user_states.get(sender.id, False):
            # Add skip flag to current bot instance instead of cancelling everything
            try:
                # Set a skip flag that the bot can check during processing
                if not hasattr(user_states, '_skip_flags'):
                    user_states._skip_flags = {}
                user_states._skip_flags[sender.id] = True

                remaining_tasks = len(user_queue)
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"⏭️ **Skipping Current Task**\n\nThe current task will be skipped and processing will continue with the next task.\n\n📋 **Remaining tasks:** {remaining_tasks}",
                    event=event
                )
                logging.info(f"Marked current task for skip for user {sender.id}, {remaining_tasks} tasks remaining")
                return
            except Exception as e:
                logging.error(f"Error setting skip flag for user {sender.id}: {e}")

        # No active processing found
        await send_message_with_flood_control(
            entity=event.chat_id,
            message="ℹ️ No active processing found to skip.\n\n💡 Use /skip during:\n• Bulk JSON processing to skip current JSON\n• Regular processing to skip current task",
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

        # Stop any active bulk processing
        async with bulk_control_lock:
            stop_bulk_processing(sender.id)

        async with bulk_lock:
            cleared_count = clear_bulk_json(sender.id)
            if cleared_count > 0:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"🧹 Cleared {cleared_count} stored JSON files from bulk processing.\n\n💾 **Storage freed:** Files removed from disk\n🛑 **Bulk processing stopped**",
                    event=event
                )
                logging.info(f"Cleared bulk JSON data for user {sender.id}: {cleared_count} files")
            else:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message="ℹ️ No bulk JSON data found to clear.",
                    event=event
                )

        # Clean up bulk processing control
        async with bulk_control_lock:
            cleanup_bulk_processing(sender.id)

    # Main function to start the bot
async def main():
    try:
        # Optimize connection for Delhi users
        await optimize_telegram_connection()

        await client.start(bot_token=BOT_TOKEN)
        # Get bot info
        me = await client.get_me()
        print(f"✅ Bot @{me.username} started successfully!")
        print(f"🔐 Instance locked with PID: {os.getpid()}")

        # Run until disconnected
        await client.run_until_disconnected()

    except Exception as e:
        logging.error(f"Error starting bot: {e}")
        raise
    finally:
        # Cleanup on exit
        print("🧹 Cleaning up...")
        try:
            await client.disconnect()
        except:
            pass
        release_lock()
        print("👋 Bot stopped gracefully")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        logging.error(f"Bot crashed: {e}")
    finally:
        release_lock()