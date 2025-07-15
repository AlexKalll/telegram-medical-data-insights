# api/database.py

import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from the project root's .env file
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

# Database connection parameters
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def get_db_connection():
    """Establishes and returns a new database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}") # Use print for simplicity in API context
        raise

# Example usage (not directly used by FastAPI, but for testing connection)
if __name__ == "__main__":
    try:
        conn = get_db_connection()
        print("Successfully connected to PostgreSQL!")
        conn.close()
    except Exception as e:
        print(f"Could not connect: {e}")
