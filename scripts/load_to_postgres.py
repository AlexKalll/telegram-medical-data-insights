import json
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

# Configure logging
LOG_DIR = Path('logs/')
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / 'load_to_postgres.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env'))
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# Data lake path
BASE_DATA_PATH = Path('data/raw/telegram_messages')

# Channel name to username mapping
CHANNEL_USERNAME_MAP = {
    'Chemed': '@CheMed123',
    'Lobelia4Cosmetics': '@lobelia4cosmetics',
    'TikvahPharma': '@tikvahpharma'
}

def create_raw_table(conn):
    """Create raw.telegram_messages table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                    message_id BIGINT,
                    channel_name VARCHAR(255),
                    channel_username VARCHAR(255),
                    scrape_date DATE,
                    message_date DATE,
                    message_text TEXT,
                    message_length INTEGER,
                    views INTEGER,
                    forwards INTEGER,
                    has_photo BOOLEAN,
                    photo_path TEXT,
                    PRIMARY KEY (message_id, channel_username)
                );
            """)
        conn.commit()
        logger.info("Created raw.telegram_messages table")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()

def load_json_to_postgres(json_file, conn):
    """Load a JSON file into raw.telegram_messages table."""
    try:
        with json_file.open('r', encoding='utf-8') as f:
            messages = json.load(f)

        if not messages:
            logger.info(f"No messages found in {json_file}. Skipping.")
            return
        recorded = skipped = 0
        with conn.cursor() as cur:
            for msg in messages:
                scrape_date = datetime.now().strftime('%Y-%m-%d')
                cur.execute("""
                    INSERT INTO raw.telegram_messages (
                        message_id, channel_name, channel_username, scrape_date, message_date,
                        message_text, message_length, views, forwards, has_photo, photo_path
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, channel_username) DO NOTHING
                    """, (
                        msg['message_id'],
                        msg['channel'],
                        CHANNEL_USERNAME_MAP.get(msg['channel'], msg['channel']),
                        scrape_date,  
                        msg['date'],  # message_date from JSON
                        msg['text'],
                        len(msg['text'] or ''),
                        msg['views'],
                        msg['forwards'],
                        msg['has_photo'],
                        msg['photo_path']
                    )
                    )
                if cur.rowcount == 1:
                    recorded += 1
                else:
                    skipped += 1

            conn.commit()
        logger.info(f"Loaded {recorded} messages from {json_file} to PostgreSQL, {skipped} skipped")
    except Exception as e:
        logger.error(f"Error loading {json_file}: {e}")
        conn.rollback()

def main():
    """Load all JSON files from data lake to PostgreSQL."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        logger.info("Connected to PostgreSQL database: telegram_medical_data")
        
        # Create raw table
        create_raw_table(conn)
        
        # Find all JSON files
        date_str = datetime.now().strftime('%Y-%m-%d')
        json_files = BASE_DATA_PATH.glob(f"{date_str}/*.json")
        
        # Load each JSON file
        for json_file in json_files:
            logger.info(f"Processing file: {json_file}")
            load_json_to_postgres(json_file, conn)
        
        conn.close()
        logger.info("PostgreSQL connection closed")
    
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == '__main__':
    main()