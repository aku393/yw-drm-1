# Fix JSON loading initialization
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
from telethon.errors.rpcerrorlist import FloodWaitError  # Import FloodWaitError
from collections import deque  # For task queue
import json

# Set up logging at the very start - console only to save disk space
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logging.info("Script started.")

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
                "https://www.bok.net/Bento4/binaries/Bento4-SDK-1-6-0-640.x86_64-unknown-linux.zip"  # Fallback URL
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

# Initialize Telegram client with optimized settings
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

# Helper function to handle flood wait errors and throttle message sends
async def send_message_with_flood_control(entity, message, event=None, edit_message=None):
    async with message_rate_limit_lock:
        # Throttle message sends to 1 per 2 seconds to avoid hitting rate limits
        await asyncio.sleep(2)  # Increased from 1 to 2 seconds
        while True:
            try:
                if edit_message:
                    logging.info(f"Editing message: {message}")
                    await edit_message.edit(message)
                    return edit_message
                else:
                    logging.info(f"Sending message to {entity.id if hasattr(entity, 'id') else entity}: {message}")
                    if event:
                        return await event.reply(message)
                    else:
                        return await client.send_message(entity, message)
            except FloodWaitError as e:
                wait_time = e.seconds
                logging.warning(f"FloodWaitError: Waiting for {wait_time} seconds before retrying...")
                logging.info(f"Bot is rate-limited by Telegram. Retrying after {wait_time} seconds.")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logging.error(f"Failed to send/edit message: {str(e)}\n{traceback.format_exc()}")
                raise

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

def progress_display(stage, percent, done, total, speed, elapsed, user, user_id, filename):
    bar_length = 10
    filled = int(percent / 100 * bar_length)
    pulse = ['▰', '▮', '▰', '▯'][int(time.time() * 2) % 4]
    progress_bar = pulse * filled + '▱' * (bar_length - filled)
    eta = (total - done) / speed if speed > 0 and done < total else 0
    # Define stage-specific emojis and messages
    stage_info = {
        "Downloading": ("📥", f"Downloading: {percent:.1f}%"),
        "Decrypting": ("🔐", "Decrypting..."),
        "Merging": ("🎬", "Merging..."),
        "Uploading": ("📤", f"Uploading: {percent:.1f}%")
    }
    emoji, status_text = stage_info.get(stage, ("🚀", stage))
    return (
        f"#Task1: {filename}\n"
        f"➜ [{progress_bar}] {percent:.1f}%\n"
        f"➜ Stage: {status_text} {emoji}\n"
        f"➜ Done: {format_size(done)} / {format_size(total)}\n"
        f"➜ Speed: {format_size(speed)}/s ⚡\n"
        f"➜ ETA: {format_time(eta)}\n"
        f"➜ Elapsed: {format_time(elapsed)}\n"
        f"➜ User: {user} 👤\n"
        f"➜ UserID: {user_id}\n"
        f"➜ Destination: Telegram MP4 📤\n"
        f"➜ Engine: Python v1 🐍"
    )

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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://d4p80xvwvkugy.cloudfront.net/',
            'Origin': 'https://d4p80xvwvkugy.cloudfront.net',
            'Accept': '*/*',
            'Accept-Encoding': 'identity',
            'Host': 'd4p80xvwvkugy.cloudfront.net',
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
        if file_size <= max_size:
            return [input_file]

        base_name = os.path.splitext(input_file)[0]
        ext = os.path.splitext(input_file)[1]
        chunks = []
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

        chunk_duration = duration * max_size / file_size
        num_chunks = int(file_size / max_size) + 1

        for i in range(num_chunks):
            output_file = f"{base_name}_part{i+1}{ext}"
            start_time = i * chunk_duration
            cmd = ['ffmpeg', '-i', input_file, '-ss', str(start_time), '-t', str(chunk_duration), '-c', 'copy', output_file, '-y']
            logging.info(f"Splitting: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                chunks.append(output_file)
                logging.info(f"Split part created: {output_file}")
            else:
                logging.error(f"Split failed: {stderr.decode()}")
                raise Exception(f"Split failed: {stderr.decode()}")
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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://d4p80xvwvkugy.cloudfront.net/',
                'Origin': 'https://d4p80xvwvkugy.cloudfront.net',
                'Accept': '*/*',
                'Accept-Encoding': 'identity',
                'Host': 'd4p80xvwvkugy.cloudfront.net',
                'Connection': 'keep-alive'
            }

            # Define the MPD fetch operation as a coroutine
            async def fetch_mpd_operation():
                async with aiohttp.ClientSession() as session:
                    logging.info(f"Fetching MPD: {mpd_url}")
                    async with session.get(mpd_url, headers=headers) as response:
                        if response.status == 403:
                            raise Exception(f"403 Forbidden: {mpd_url}")
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
                    content_type = adaptation_set.get('contentType', '')
                    if content_type not in ['video', 'audio']:
                        logging.info(f"Skipping AdaptationSet: contentType={content_type}")
                        continue
                    segments = video_segments if content_type == 'video' else audio_segments
                    for representation in adaptation_set.findall('.//ns:Representation', namespace):
                        mime = representation.get('mimeType', '')
                        codec = representation.get('codecs', '')
                        logging.info(f"Representation: mime={mime}, codec={codec}")
                        if (content_type == 'video' and ('video' not in mime.lower() or 'avc' not in codec.lower())) or \
                           (content_type == 'audio' and 'audio' not in mime.lower()):
                            logging.info("Skipping non-matching representation")
                            continue
                        base_url_elem = representation.find('.//ns:BaseURL', namespace)
                        if base_url_elem is not None:
                            stream_url = base_url + base_url_elem.text.strip()
                            logging.info(f"Locked {content_type} BaseURL: {stream_url}")
                            segment_base = representation.find('.//ns:SegmentBase', namespace)
                            if segment_base is not None:
                                init = segment_base.find('.//ns:Initialization', namespace)
                                init_range = init.get('range') if init else None
                                logging.info(f"Found {content_type} Initialization range: {init_range}")
                                index_range = segment_base.get('indexRange')
                                if index_range:
                                    segments.append((stream_url, init_range))
                                    segments.append((stream_url, f"bytes={index_range.split('-')[1]}-"))
                                    logging.info(f"{content_type} SegmentBase segments: {segments}")
                            if not segments:
                                segments.append((stream_url, None))
                                logging.info(f"Added full {content_type} URL: {stream_url}")
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
                while self.progress_state['stage'] != "Completed":
                    async with self.update_lock:  # Ensure only one update at a time
                        current_time = time.time()
                        # Debounce: Only update if at least 30 seconds have passed since the last update
                        if current_time - last_update_time < 30:
                            await asyncio.sleep(1)
                            continue
                        self.progress_state['elapsed'] = current_time - self.progress_state['start_time']
                        self.progress_state['speed'] = self.progress_state['done_size'] / self.progress_state['elapsed'] if self.progress_state['elapsed'] > 0 else 0
                        if self.progress_state['stage'] == "Downloading":
                            total_size_est = self.progress_state['done_size'] * total_segments / max(progress['completed'], 1)
                            max_total_size_est = max(max_total_size_est, total_size_est)
                            self.progress_state['total_size'] = max_total_size_est
                            self.progress_state['percent'] = progress['completed'] * 100 / total_segments
                        display = progress_display(
                            self.progress_state['stage'],
                            self.progress_state['percent'],
                            self.progress_state['done_size'],
                            self.progress_state['total_size'],
                            self.progress_state['speed'],
                            self.progress_state['elapsed'],
                            user,
                            user_id,
                            filename
                        )
                        status_msg = await send_message_with_flood_control(
                            entity=event.chat_id,
                            message=display,
                            edit_message=status_msg
                        )
                        last_update_time = current_time
                        logging.info(f"Progress message updated: {self.progress_state['stage']} - {self.progress_state['percent']:.1f}%")
                    await asyncio.sleep(10)  # Update every 10 seconds
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

    async def upload_file(self, event, filepath, status_msg, total_size, sender, duration):
        try:
            file_size = os.path.getsize(filepath)
            logging.info(f"Processing upload for {filepath}, size: {format_size(file_size)}, duration: {duration}s")
            self.progress_state['start_time'] = time.time()
            self.progress_state['total_size'] = file_size
            self.progress_state['done_size'] = 0
            self.progress_state['percent'] = 0.0
            self.progress_state['speed'] = 0
            self.progress_state['elapsed'] = 0
            # Maximum concurrent upload parts for full speed
            # Optimized upload progress for maximum bandwidth

            # Determine the max size based on user status (premium or free)
            max_size_mb = 4000 if sender.premium else 2000  # 4 GB for premium, 2 GB for free
            logging.info(f"User {sender.id} is {'premium' if sender.premium else 'free'}, setting max_size_mb to {max_size_mb} MB")

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
                    status_msg = await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"File > {max_size_mb} MB—splitting into parts...",
                        edit_message=status_msg
                    )
                    self.has_notified_split = True
                    logging.info(f"Notified user about splitting file: {filepath}")
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
                    semaphore = asyncio.Semaphore(2)  # Maximum concurrent uploads
                    logging.info(f"Starting parallel upload for chunk {i+1}, size: {chunk_size}, total parts: {total_parts}, file_id: {file_id}")

                    async def update_progress():
                        nonlocal last_update_time, status_msg
                        while progress['uploaded'] < chunk_size:
                            current_time = time.time()
                            if current_time - last_update_time < 10:
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
                                status_msg = await send_message_with_flood_control(
                                    entity=event.chat_id,
                                    message=display,
                                    edit_message=status_msg
                                )
                                last_update_time = current_time
                                logging.info(f"Upload progress updated for chunk {i+1}: {self.progress_state['percent']:.1f}%")
                            await asyncio.sleep(10)  # Update every 10 seconds

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
                semaphore = asyncio.Semaphore(2)  # Maximum concurrent uploads
                logging.info(f"Starting parallel upload for file, size: {file_size}, total parts: {total_parts}, file_id: {file_id}")

                async def update_progress():
                    nonlocal last_update_time, status_msg
                    while progress['uploaded'] < file_size:
                        current_time = time.time()
                        if current_time - last_update_time < 30:
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
                            status_msg = await send_message_with_flood_control(
                                entity=event.chat_id,
                                message=display,
                                edit_message=status_msg
                            )
                            last_update_time = current_time
                            logging.info(f"Upload progress updated: {self.progress_state['percent']:.1f}%")
                        await asyncio.sleep(10)  # Update every 10 seconds

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
            logging.error(f"Upload failed: {str(e)}\n{traceback.format_exc()}")
            status_msg = await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"Upload failed: {str(e)}",
                edit_message=status_msg
            )
            raise
        finally:
            self.has_notified_split = False  # Reset the flag after upload

    async def process_task(self, event, task_data, sender, starting_msg=None):
        """Process a single task (download and upload) - supports both DRM and direct downloads."""
        filepath = None
        status_msg = None
        try:
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
            message="🚫 You're not authorized to use this bot.\n\nContact the admin to get access.",
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
        "🧹 /clearbulk - Clear stored bulk JSON data\n\n"
        "📊 /speed - Check download/upload speeds\n"
        "🧹 /clearall - Clear all tasks from your queue\n"
        "🗑️ /clear - Complete cleanup (queue + files + data)\n\n"
        "🖼️ /addthumbnail - Add custom thumbnail (send photo)\n"
        "❌ /removethumbnail - Remove custom thumbnail\n\n"
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

    await send_message_with_flood_control(
        entity=event.chat_id,
        message=f"🧹 Stopped active downloads and cleared {cleared_count} task(s) from your queue.",
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
        message="📥 Ready to receive JSON data!\n\nYou can:\n1. Upload a .json file\n2. Send JSON text directly\n\nExpected format:\n```json\n[\n  {\n    \"name\": \"Video Name\",\n    \"type\": \"drm\",\n    \"mpd_url\": \"https://example.com/manifest.mpd\",\n    \"keys\": [\"kid:key\"]\n  },\n  {\n    \"name\": \"Direct Video\",\n    \"type\": \"direct\",\n    \"url\": \"https://example.com/video.mp4\"\n  }\n]\n```\n\nAfter sending JSON data, use /processjson to start processing all videos!",
        event=event
    )

@client.on(events.NewMessage())
async def json_data_handler(event):
    """Handle JSON file uploads and JSON text input"""
    sender = await event.get_sender()

    if sender.id not in authorized_users:
        return

    try:
        json_data = None

        # Check if it's a file upload
        if event.document and event.document.mime_type == 'application/json':
            logging.info(f"JSON file uploaded by user {sender.id}")

            # Download the file
            file_path = await event.download_media()

            # Read JSON content
            with open(file_path, 'r', encoding='utf-8') as f:
                json_content = f.read()

            # Clean up downloaded file
            os.remove(file_path)

            # Parse JSON
            json_data = json.loads(json_content)

            # Check if user is in bulk mode (has bulk data or recent /bulk command)
            async with bulk_lock:
                if sender.id in user_bulk_data:
                    # Add to bulk data
                    user_bulk_data[sender.id].append(json_data)
                    total_bulk = len(user_bulk_data[sender.id])
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"📦 **Bulk JSON #{total_bulk}** loaded! Found {len(json_data)} items.\n\nSend more JSON files or use /processbulk to start sequential processing.",
                        event=event
                    )
                else:
                    # Regular JSON processing
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"✅ JSON file loaded successfully! Found {len(json_data)} items.\n\nUse /processjson to start processing.",
                        event=event
                    )

        # Check if it's JSON text (starts with [ or {)
        elif event.text and (event.text.strip().startswith('[') or event.text.strip().startswith('{')):
            logging.info(f"JSON text received from user {sender.id}")

            # Parse JSON
            json_data = json.loads(event.text.strip())

            # Check if user is in bulk mode
            async with bulk_lock:
                if sender.id in user_bulk_data:
                    # Add to bulk data
                    user_bulk_data[sender.id].append(json_data)
                    total_bulk = len(user_bulk_data[sender.id])
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"📦 **Bulk JSON #{total_bulk}** loaded! Found {len(json_data)} items.\n\nSend more JSON data or use /processbulk to start sequential processing.",
                        event=event
                    )
                else:
                    # Regular JSON processing
                    await send_message_with_flood_control(
                        entity=event.chat_id,
                        message=f"✅ JSON data loaded successfully! Found {len(json_data)} items.\n\nUse /processjson to start processing.",
                        event=event
                    )

        # Store JSON data for user
        if json_data:
            async with bulk_lock:
                if sender.id not in user_bulk_data:
                    # Regular JSON storage
                    async with json_lock:
                        user_json_data[sender.id] = json_data
                    logging.info(f"Stored JSON data for user {sender.id}: {len(json_data)} items")

    except json.JSONDecodeError as e:
        logging.error(f"JSON parsing error from user {sender.id}: {str(e)}")
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ Invalid JSON format: {str(e)}\n\nPlease check your JSON syntax and try again.",
            event=event
        )
    except Exception as e:
        logging.error(f"Error handling JSON data from user {sender.id}: {str(e)}")
        await send_message_with_flood_control(
            entity=event.chat_id,
            message=f"❌ Error processing JSON: {str(e)}",
            event=event
        )

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
            message=f"📋 **JSON Data Loaded: {len(json_data)} items**\n\nPlease specify range:\n\n**Examples:**\n• `/processjson all` - Process all items\n• `/processjson 1-10` - Process items 1 to 10\n• `/processjson 5-25` - Process items 5 to 25\n• `/processjson 1` - Process only item 1\n\n**Current range: 1-{len(json_data)}**",
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
                name = item.get('name', f'Video_{i}')
                item_type = item.get('type', 'drm').lower()

                if item_type == 'drm':
                    # DRM protected content
                    mpd_url = item.get('mpd_url')
                    keys = item.get('keys', [])

                    if not mpd_url:
                        invalid_items.append(f"Item {i}: Missing mpd_url")
                        continue
                    if not keys:
                        invalid_items.append(f"Item {i}: Missing keys")
                        continue

                    # Use first key if multiple keys provided
                    key = keys[0] if isinstance(keys, list) else keys

                    tasks_to_add.append({
                        'type': 'drm',
                        'mpd_url': mpd_url,
                        'key': key,
                        'name': name,
                        'sender': sender
                    })

                elif item_type == 'direct':
                    # Direct download
                    url = item.get('url')

                    if not url:
                        invalid_items.append(f"Item {i}: Missing url")
                        continue

                    tasks_to_add.append({
                        'type': 'direct',
                        'url': url,
                        'name': name,
                        'sender': sender
                    })

                else:
                    invalid_items.append(f"Item {i}: Unknown type '{item_type}' (supported: drm, direct)")

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

            # queue_message = f"📋 **Selected Range: {start_idx + 1}-{end_idx}**\n"
            # queue_message += f"Added {len(tasks_to_add)} task(s) from JSON to your queue:\n"

            # if len(tasks_to_add) <= 10:
            #     # Show all tasks if 10 or fewer
            #     start_position = len(user_queue) - len(tasks_to_add) + 1
            #     for i, task in enumerate(tasks_to_add, start_position):
            #         task_type_emoji = "🔐" if task['type'] == 'drm' else "📥"
            #         queue_message += f"Task {i}: {task_type_emoji} {task['name']}.mp4\n"
            # else:
            #     # Show summary for large batches
            #     drm_count = sum(1 for task in tasks_to_add if task['type'] == 'drm')
            #     direct_count = sum(1 for task in tasks_to_add if task['type'] == 'direct')
            #     queue_message += f"🔐 DRM Videos: {drm_count}\n📥 Direct Downloads: {direct_count}\n"
            #     queue_message += f"Total queue size: {len(user_queue)} tasks\n"

            # if user_states.get(sender.id, False):
            #     queue_message += "\nA task is currently being processed. Your tasks will start soon… ⏳"

            # Format the start and end index based on range input
            # Use JSON name as filename
            if range_input.lower() == "all":
                range_message = f"1-{len(json_data)}"
            else:
                range_message = range_input

            queue_message = f"📋 Selected Range: {range_message}\n"
            queue_message += f"Added {len(tasks_to_add)} task(s) from JSON to your queue:\n"
            task_type_emoji = "🔐" if tasks_to_add[0]['type'] == 'drm' else "📥"
            queue_message += f"Task 1: {task_type_emoji} {filename}.mp4\n" # Use JSON name as filename

            if user_states.get(sender.id, False):
                queue_message += "\nA task is currently being processed. Your tasks will start soon… ⏳"

            await send_message_with_flood_control(
                entity=event.chat_id,
                message=queue_message,
                event=event
            )

            # Start queue processor if not running
            if not user_states.get(sender.id, False):
                logging.info(f"Starting queue processor for user {sender.id} from /processjson handler")
                bot = MPDLeechBot(sender.id)
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

    if sender.id not in authorized_users or not event.photo:
        return

    # Check if user recently used /addthumbnail (simple check)
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
        message="📦 **Bulk JSON Processing** 📦\n\nSend multiple JSON files or JSON text messages. Each JSON will be processed completely before starting the next one.\n\n**Usage:**\n1. Send multiple JSON files/text\n2. Use `/processbulk` to start sequential processing\n3. Use `/clearbulk` to clear stored JSON data\n\n**Example Format:**\n```json\n[\n  {\n    \"name\": \"Video1\",\n    \"type\": \"drm\",\n    \"mpd_url\": \"https://example.com/manifest1.mpd\",\n    \"keys\": [\"kid:key\"]\n  }\n]\n```\n\nReady to receive JSON data! 🚀",
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
        message=f"🚀 **Starting Bulk Processing** 🚀\n\n📦 Processing {total_jsons} JSON files sequentially\n⏳ Each JSON will be completed before starting the next\n\nProcessing will begin shortly...",
        event=event
    )

    # Process each JSON sequentially
    for json_index, json_data in enumerate(bulk_data_list, 1):
        try:
            # Notify user about current JSON
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"📋 **JSON {json_index}/{total_jsons}** - Starting processing of {len(json_data)} items",
                event=event
            )

            # Add all tasks from current JSON to queue
            tasks_to_add = []
            for item in json_data:
                try:
                    name = item.get('name', f'Video_{json_index}_{len(tasks_to_add)+1}')
                    item_type = item.get('type', 'drm').lower()

                    if item_type == 'drm':
                        mpd_url = item.get('mpd_url')
                        keys = item.get('keys', [])
                        if mpd_url and keys:
                            key = keys[0] if isinstance(keys, list) else keys
                            tasks_to_add.append({
                                'type': 'drm',
                                'mpd_url': mpd_url,
                                'key': key,
                                'name': name,
                                'sender': sender
                            })
                    elif item_type == 'direct':
                        url = item.get('url')
                        if url:
                            tasks_to_add.append({
                                'type': 'direct',
                                'url': url,
                                'name': name,
                                'sender': sender
                            })
                except Exception as e:
                    logging.warning(f"Skipping invalid item in JSON {json_index}: {e}")

            if not tasks_to_add:
                await send_message_with_flood_control(
                    entity=event.chat_id,
                    message=f"⚠️ JSON {json_index}/{total_jsons} - No valid items found, skipping",
                    event=event
                )
                continue

            # Add tasks to queue
            async with user_lock:
                for task in tasks_to_add:
                    user_queue.append(task)

            # Start processing this JSON and wait for completion
            if not user_states.get(sender.id, False):
                bot = MPDLeechBot(sender.id)
                user_active_tasks[sender.id] = asyncio.create_task(bot.process_queue(event))

            # Wait for this JSON to complete before starting next
            while user_states.get(sender.id, False) or (user_active_tasks.get(sender.id) and not user_active_tasks[sender.id].done()):
                await asyncio.sleep(5)

            # Send completion message for this JSON
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"✅ **JSON {json_index}/{total_jsons} Completed!** All {len(tasks_to_add)} tasks processed.\n\n{'🎉 All JSONs completed!' if json_index == total_jsons else f'⏭️ Moving to JSON {json_index + 1}/{total_jsons}...'}",
                event=event
            )

        except Exception as e:
            logging.error(f"Error processing JSON {json_index} for user {sender.id}: {e}")
            await send_message_with_flood_control(
                entity=event.chat_id,
                message=f"❌ **JSON {json_index}/{total_jsons} Failed:** {str(e)}\n\n{'Moving to next JSON...' if json_index < total_jsons else 'Bulk processing completed with errors.'}",
                event=event
            )

    # Final completion message
    await send_message_with_flood_control(
        entity=event.chat_id,
        message=f"🎊 **Bulk Processing Complete!** 🎊\n\n✅ Processed {total_jsons} JSON files\n🚀 All tasks completed successfully!\n\nYou can now start new tasks or use /bulk again for more JSON files.",
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

# Main function to start the bot
async def main():
    await client.start(bot_token=BOT_TOKEN)
    logging.info("Bot started successfully!")

    # Get bot info
    me = await client.get_me()
    logging.info(f"Bot username: @{me.username}")
    logging.info(f"Bot ID: {me.id}")

    # Run until disconnected
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())