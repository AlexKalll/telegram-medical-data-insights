# api/main.py

from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from datetime import date, datetime
import psycopg2.extras
from collections import Counter
import re

from .database import get_db_connection
from .schemas import Message, TopProduct, ChannelActivity, Channel, ImageDetection

app = FastAPI(
    title="Telegram Medical Data Insights API",
    description="API for querying transformed Telegram medical channel data.",
    version="1.0.0"
)

# --- Helper function to fetch data ---
def fetch_data(query: str, params: Optional[tuple] = None):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # Returns rows as dictionaries
        cur.execute(query, params)
        return cur.fetchall()
    except Exception as e:
        print(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if conn:
            conn.close()

# --- Analytical Endpoints ---

# 1. GET /api/reports/top-products?limit=10
# Returns the most frequently mentioned products/drugs.
# This is a simplified approach; a real-world solution might use NER.
@app.get("/api/reports/top-products", response_model=List[TopProduct])
async def get_top_products(limit: int = Query(10, ge=1, le=100)):
    """
    Returns the top N most frequently mentioned medical products or drugs across all channels.
    This implementation uses a simple keyword frequency count.
    """
    # Fetch all message texts
    messages = fetch_data("SELECT message_text FROM marts.fct_messages WHERE message_text IS NOT NULL")
    
    all_words = []
    # Define a list of common medical product/drug keywords (expand as needed)
    # This is a simplification; a robust solution would use a pre-defined dictionary or NER.
    medical_keywords = [
        "paracetamol", "ibuprofen", "antibiotic", "vaccine", "insulin", "syrup",
        "tablet", "cream", "ointment", "capsule", "injection", "mask", "sanitizer",
        "vitamin", "supplement", "drug", "medicine", "pill", "gel", "lotion",
        "diagnostic", "equipment", "test kit", "bandage", "disinfectant", "gloves",
        "thermometer", "blood pressure monitor", "nebulizer", "crutches", "wheelchair"
    ]
    
    for msg in messages:
        text = msg['message_text'].lower()
        # Simple tokenization and filter for keywords
        words = re.findall(r'\b\w+\b', text) # Extract words
        for word in words:
            if word in medical_keywords: # Check if word is in our predefined list
                all_words.append(word)
            # Also check for phrases
            for keyword in medical_keywords:
                if len(keyword.split()) > 1 and keyword in text:
                    all_words.append(keyword)

    if not all_words:
        return []

    word_counts = Counter(all_words)
    top_products_data = []
    for product, count in word_counts.most_common(limit):
        top_products_data.append({"product_name": product, "mention_count": count})
    
    return top_products_data

# 2. GET /api/channels/{channel_username}/activity
# Returns the posting activity for a specific channel.
@app.get("/api/channels/{channel_username}/activity", response_model=List[ChannelActivity])
async def get_channel_activity(channel_username: str):
    """
    Returns the daily posting activity for a specific Telegram channel.
    """
    query = """
        SELECT
            dd.full_date AS activity_date,
            COUNT(fm.message_sk) AS message_count
        FROM
            marts.fct_messages fm
        JOIN
            marts.dim_channels dc ON fm.channel_sk = dc.channel_sk
        JOIN
            marts.dim_dates dd ON fm.message_date_sk = dd.date_sk
        WHERE
            dc.channel_username = %s
        GROUP BY
            dd.full_date
        ORDER BY
            dd.full_date;
    """
    results = fetch_data(query, (channel_username,))
    if not results:
        raise HTTPException(status_code=404, detail=f"No activity found for channel: {channel_username}")
    
    return [ChannelActivity(activity_date=row['activity_date'], message_count=row['message_count']) for row in results]

# 3. GET /api/search/messages?query=paracetamol
# Searches for messages containing a specific keyword.
@app.get("/api/search/messages", response_model=List[Message])
async def search_messages(query: str = Query(..., min_length=2)):
    """
    Searches for Telegram messages containing a specific keyword (case-insensitive).
    """
    search_pattern = f"%{query.lower()}%"
    sql_query = """
        SELECT
            message_sk, message_id, channel_sk, channel_username, message_date_sk, scrape_date_sk,
            message_text, message_length, views, forwards, has_photo, photo_path,
            is_urgent_message, is_vacancy_message, message_count
        FROM
            marts.fct_messages
        WHERE
            LOWER(message_text) LIKE %s
        ORDER BY
            message_date_sk DESC
        LIMIT 100; -- Limit results for performance
    """
    results = fetch_data(sql_query, (search_pattern,))
    if not results:
        raise HTTPException(status_code=404, detail=f"No messages found for query: '{query}'")
    
    # Convert RealDictRow to Message Pydantic model
    return [Message(**row) for row in results]

# Endpoint to get all channels (useful for UI or discovery)
@app.get("/api/channels", response_model=List[Channel])
async def get_all_channels():
    """
    Returns a list of all unique Telegram channels.
    """
    query = "SELECT channel_sk, channel_username, channel_name FROM marts.dim_channels ORDER BY channel_name;"
    results = fetch_data(query)
    return [Channel(**row) for row in results]

# Endpoint to get image detections for a message (or all detections)
@app.get("/api/image-detections", response_model=List[ImageDetection])
async def get_image_detections(message_sk: Optional[str] = None):
    """
    Returns image detection results. Can filter by message_sk.
    """
    sql_query = """
        SELECT
            image_detection_sk, message_sk, detected_object_class, confidence_score, image_path, detection_timestamp
        FROM
            marts.fct_image_detections
    """
    params = None
    if message_sk:
        sql_query += " WHERE message_sk = %s"
        params = (message_sk,)
    
    sql_query += " LIMIT 100;" # Limit results for better performance
    
    results = fetch_data(sql_query, params)
    if not results and message_sk:
        raise HTTPException(status_code=404, detail=f"No image detections found for message_sk: {message_sk}")
    
    return [ImageDetection(**row) for row in results]

