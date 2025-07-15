import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

# Configure logging
LOG_DIR = Path('logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / 'check_image_detections.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')
db_params = {
    'dbname': os.getenv('POSTGRES_DB', 'telegram_medical_data'),
    'user': os.getenv('POSTGRES_USER', 'user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password'),
    'host': os.getenv('POSTGRES_HOST', 'postgres_db'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}

def check_image_detections():
    """Check if raw.image_detections table is accumulating data correctly."""
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        logger.info("Connected to PostgreSQL database: telegram_medical_data")
        
        with conn.cursor() as cur:
            # Total record count
            cur.execute("SELECT count(*) FROM raw.image_detections")
            total_count = cur.fetchone()[0]
            logger.info(f"Total records in raw.image_detections: {total_count}")
            
            # Records by channel_username
            cur.execute("SELECT channel_username, count(*) FROM raw.image_detections GROUP BY channel_username")
            channel_counts = cur.fetchall()
            for channel, count in channel_counts:
                logger.info(f"Channel {channel}: {count} detections")
            
            # Check for duplicates (based on detection_id)
            cur.execute("SELECT detection_id, count(*) FROM raw.image_detections GROUP BY detection_id HAVING count(*) > 1")
            duplicates = cur.fetchall()
            if duplicates:
                logger.warning("Found duplicate detection_ids:")
                for dup in duplicates:
                    logger.warning(f"detection_id: {dup[0]}, count: {dup[1]}")
            else:
                logger.info("No duplicate detection_ids found")
            
            # Sample data (first 5 rows)
            cur.execute("SELECT detection_id, message_id, channel_username, image_path, detected_object_class, confidence_score FROM raw.image_detections LIMIT 5")
            sample_rows = cur.fetchall()
            logger.info("Sample rows from raw.image_detections:")
            for row in sample_rows:
                logger.info(f"detection_id: {row[0]}, message_id: {row[1]}, channel_username: {row[2]}, image_path: {row[3]}, detected_object_class: {row[4]}, confidence_score: {row[5]}")
            
            # Check for unmatched message_ids
            cur.execute("""
                SELECT d.message_id, d.channel_username
                FROM raw.image_detections d
                LEFT JOIN raw.telegram_messages m
                ON d.message_id = m.message_id AND d.channel_username = m.channel_username
                WHERE m.message_id IS NULL
            """)
            unmatched = cur.fetchall()
            if unmatched:
                logger.warning("Found detections with unmatched message_ids:")
                for row in unmatched:
                    logger.warning(f"message_id: {row[0]}, channel_username: {row[1]}")
            else:
                logger.info("All detections have matching message_ids in raw.telegram_messages")
        
    except Exception as e:
        logger.error(f"Error checking raw.image_detections: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("PostgreSQL connection closed")

if __name__ == '__main__':
    check_image_detections()