import os
import json
import logging
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from telethon.types import MessageMediaPhoto
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from dotenv import load_dotenv
import asyncio

# --- LOGGING SETUP ---
LOG_DIR = Path('logs/') 
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / 'telegram_scraper.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# ---------------------

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE_NUMBER')

# Define channels to scrape
TARGET_CHANNELS = { 
    '@CheMed123': 'Chemed',
    '@lobelia4cosmetics': 'Lobelia4Cosmetics', 
    '@tikvahpharma': 'TikvahPharma'
}

# Data lake paths
BASE_DATA_PATH = Path('data/raw/telegram_messages')
IMAGE_PATH = Path('data/images') # For image collection

async def scrape_channel(client, channel_username, date_str):
    """Scrape messages and photos from a Telegram channel and save to data lake."""
    channel_name = TARGET_CHANNELS.get(channel_username)
    if not channel_name:
        logger.error(f"Unknown channel username: {channel_username}. Skipping.")
        return

    try:
        # Get channel entity
        entity = await client.get_entity(channel_username)
        logger.info(f"Scraping channel: {channel_username} (Internal name: {channel_name})")

        # Ensure directory exists for raw JSON
        channel_json_path = BASE_DATA_PATH / date_str
        channel_json_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure image directory exists
        IMAGE_PATH.mkdir(parents=True, exist_ok=True) # This will create 'data/images'

        messages_data = []
        download_images = channel_name in ['Chemed', 'Lobelia4Cosmetics']

        async for message in client.iter_messages(entity, limit=100): # Scrape exactly 100 messages
            message_data = {
                'message_id': message.id,
                'channel': channel_name, # Use the internal name for consistency
                'date': message.date.isoformat(),
                'text': message.text or '',
                'views': message.views if message.views is not None else 0,
                'forwards': message.forwards if message.forwards is not None else 0,
                'has_photo': bool(message.photo),
                'photo_path': None
            }

            # Handle photos only for specified channels
            if download_images and isinstance(message.media, MessageMediaPhoto):
                try:
                    # Ensure path is relative to the project root for consistency in the JSON
                    photo_filename = f"{channel_name}_{message.id}.jpg"
                    full_photo_path = IMAGE_PATH / photo_filename
                    await client.download_media(message.media, file=full_photo_path)
                    message_data['photo_path'] = str(full_photo_path.relative_to(Path('.'))) # Store relative path
                    logger.info(f"Downloaded photo for message {message.id} from {channel_name} to {full_photo_path}")
                except Exception as e:
                    logger.error(f"Failed to download photo for message {message.id} from {channel_name}: {e}")

            messages_data.append(message_data)

        # Save messages as JSON
        output_file = channel_json_path / f"{channel_name}.json"
        with output_file.open('w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(messages_data)} messages from {channel_name} to {output_file}")

    except FloodWaitError as e:
        logger.error(f"Rate limit hit for {channel_username}: wait {e.seconds} seconds")
    except Exception as e:
        logger.error(f"Error scraping {channel_username}: {e}")

async def main():
    """Main function to scrape all specified channels."""

    # Initialize Telegram client
    async with TelegramClient('telegram_medical_session', api_id, api_hash) as client:
        try:
            # Authenticate
            if not await client.is_user_authorized():
                logger.info("Authentication required. Sending code request.")
                await client.send_code_request(phone)
                code = input("Enter the code you received on telegram: ")
                try:
                    await client.sign_in(phone, code)
                except SessionPasswordNeededError:
                    password = input("Enter your 2FA password: ")
                    await client.sign_in(password=password)
            logger.info("Telegram client authenticated successfully")

            # Get current date for directory structure
            date_str = datetime.now().strftime('%Y-%m-%d')

            # Scrape each channel
            for channel_username in TARGET_CHANNELS.keys(): # Iterate over keys
                logger.info(f"Starting scrape for channel: {TARGET_CHANNELS[channel_username]}")
                await scrape_channel(client, channel_username, date_str)
                logger.info(f"Completed scrape for channel: {TARGET_CHANNELS[channel_username]}")

        except Exception as e:
            logger.error(f"Client error during authentication or main loop: {e}")

if __name__ == '__main__':
    asyncio.run(main())