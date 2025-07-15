import os
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from ultralytics import YOLO

# Configure logging
LOG_DIR = Path('logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'yolo_detection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT')
}

# Channel name to username mapping
CHANNEL_USERNAME_MAP = {
    'Chemed': '@CheMed123',
    'Lobelia4Cosmetics': '@lobelia4cosmetics',
    'TikvahPharma': '@tikvahpharma'
}

# Image and model paths
IMAGE_DIR = Path('data/images')
YOLO_MODEL_PATH = 'yolov8n.pt'

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(**db_params)
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def create_detection_table(conn):
    """Creates the raw.image_detections table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE TABLE IF NOT EXISTS raw.image_detections (
                    detection_id VARCHAR(255) PRIMARY KEY,
                    message_id BIGINT,
                    channel_username VARCHAR(255),
                    image_path TEXT,
                    detected_object_class VARCHAR(255),
                    confidence_score NUMERIC(5, 4),
                    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()
        logger.info("Created raw.image_detections table successfully.")
    except Exception as e:
        logger.error(f"Error creating raw.image_detections table: {e}")
        conn.rollback()
        raise

def get_processed_images(conn):
    """Retrieves a set of image paths that have already been processed."""
    processed_images = set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT image_path FROM raw.image_detections;")
            for row in cur.fetchall():
                processed_images.add(row[0])
        logger.info(f"Found {len(processed_images)} images already processed.")
    except Exception as e:
        logger.error(f"Error retrieving processed images: {e}")
    return processed_images

def main():
    """Run YOLO object detection and store results in the database."""
    logger.info("Starting YOLO object detection process...")
    
    # Load YOLO model
    try:
        model = YOLO(YOLO_MODEL_PATH)
        logger.info(f"YOLO model '{YOLO_MODEL_PATH}' loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load YOLO model: {e}")
        return

    conn = None
    try:
        conn = get_db_connection()
        create_detection_table(conn)
        processed_images = get_processed_images(conn)

        images_to_process = []
        if not IMAGE_DIR.exists():
            logger.error(f"Image directory not found: {IMAGE_DIR}. Please ensure images are scraped.")
            return

        for image_file in IMAGE_DIR.iterdir():
            if image_file.is_file() and image_file.suffix.lower() in ['.jpg', '.jpeg', '.png']:
                relative_image_path = str(image_file.relative_to(Path('.')))
                if relative_image_path not in processed_images:
                    images_to_process.append(image_file)
        
        logger.info(f"Found {len(images_to_process)} new images to process.")

        if not images_to_process:
            logger.info("No new images to process. Exiting.")
            return

        detection_results = []
        for image_path_obj in images_to_process:
            try:
                # Parse filename (e.g., Chemed_97.jpg or Chemed_97_20230101.jpg)
                parts = image_path_obj.stem.split('_')
                if len(parts) >= 2:
                    channel_name = parts[0]
                    message_id = parts[1]
                else:
                    logger.warning(f"Invalid filename format: {image_path_obj.name}. Expected Channel_MessageID[_Date].jpg. Skipping.")
                    continue

                # Map channel name to username
                channel_username = CHANNEL_USERNAME_MAP.get(channel_name)
                if not channel_username:
                    logger.warning(f"No username mapping for channel '{channel_name}' in {image_path_obj.name}. Skipping.")
                    continue

                # Perform detection
                results = model(str(image_path_obj))
                
                # Extract results
                for r in results:
                    if not r.boxes:
                        logger.info(f"No objects detected in {image_path_obj.name}.")
                        continue
                    for box in r.boxes:
                        class_id = int(box.cls[0])
                        confidence = round(float(box.conf[0]), 4)
                        detected_class = model.names[class_id]
                        detection_id = f"{image_path_obj.stem}_{detected_class}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                        
                        detection_results.append((
                            detection_id,
                            int(message_id),
                            channel_username,
                            str(image_path_obj.relative_to(Path('.'))),
                            detected_class,
                            confidence,
                            datetime.now()
                        ))
                logger.info(f"Processed {image_path_obj.name} with {len(r.boxes)} detections.")
            except Exception as e:
                logger.error(f"Error processing image {image_path_obj.name}: {e}")
                continue

        if detection_results:
            try:
                with conn.cursor() as cur:
                    insert_query = """
                        INSERT INTO raw.image_detections (
                            detection_id, message_id, channel_username, image_path,
                            detected_object_class, confidence_score, detection_timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (detection_id) DO NOTHING
                    """
                    execute_batch(cur, insert_query, detection_results, page_size=1000)
                    inserted_count = cur.rowcount
                    conn.commit()
                    logger.info(f"Inserted {inserted_count} detection results into raw.image_detections.")
            except Exception as e:
                logger.error(f"Error inserting detection results: {e}")
                conn.rollback()
        else:
            logger.info("No detection results to insert.")

    except Exception as e:
        logger.error(f"Main YOLO process error: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()