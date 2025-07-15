# dagster_pipeline/definitions.py

import os
import subprocess
from pathlib import Path
from datetime import datetime

from dagster import Definitions, job, op, ScheduleDefinition, AssetSelection, define_asset_job

# Define the base directory for scripts relative to this file
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
DBT_DIR = Path(__file__).parent.parent / "medical_dbt" # Your dbt project directory

# --- Ops Definitions ---

@op(name="scrape_telegram_data_op", description="Scrapes raw messages and images from Telegram channels.")
def scrape_telegram_data():
    """
    Executes the telegram_scraper.py script to collect raw data.
    This op assumes the scraper runs within the Docker container.
    For Dagster running on the host, we'll use `docker exec`.
    """
    logger = scrape_telegram_data.log
    logger.info("Starting Telegram data scraping...")
    try:
        # Assuming docker-compose is running and fastapi_app is the service name
        command = ["docker", "exec", "fastapi_app", "python", str(SCRIPTS_DIR / "telegram_scraper.py")]
        
        # Run the command, capturing output
        process = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.info(f"Scraper stdout:\n{process.stdout}")
        if process.stderr:
            logger.warning(f"Scraper stderr:\n{process.stderr}")
        logger.info("Telegram data scraping completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Telegram data scraping failed: {e}")
        logger.error(f"Scraper stdout:\n{e.stdout}")
        logger.error(f"Scraper stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        logger.error("Docker command not found. Is Docker installed and running?")
        raise


@op(name="load_raw_to_postgres_op", description="Loads raw JSON data from data lake to PostgreSQL.")
def load_raw_to_postgres():
    """
    Executes the load_to_postgres.py script to load raw data into PostgreSQL.
    This script runs on the host machine.
    """
    logger = load_raw_to_postgres.log
    logger.info("Starting raw data loading to PostgreSQL...")
    try:
        command = ["python", str(SCRIPTS_DIR / "load_to_postgres.py")]
        process = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.info(f"Loader stdout:\n{process.stdout}")
        if process.stderr:
            logger.warning(f"Loader stderr:\n{process.stderr}")
        logger.info("Raw data loading to PostgreSQL completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Raw data loading failed: {e}")
        logger.error(f"Loader stdout:\n{e.stdout}")
        logger.error(f"Loader stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        logger.error("Python command not found. Is Python installed and in PATH?")
        raise


@op(name="run_dbt_transformations_op", description="Executes dbt models to transform data into star schema.")
def run_dbt_transformations():
    """
    Executes 'dbt run' and 'dbt test' commands.
    """
    logger = run_dbt_transformations.log
    logger.info("Starting dbt transformations...")
    try:
        # Run dbt models
        dbt_run_command = ["dbt", "run"]
        logger.info(f"Running dbt command: {' '.join(dbt_run_command)}")
        process_run = subprocess.run(dbt_run_command, cwd=DBT_DIR, capture_output=True, text=True, check=True)
        logger.info(f"dbt run stdout:\n{process_run.stdout}")
        if process_run.stderr:
            logger.warning(f"dbt run stderr:\n{process_run.stderr}")
        logger.info("dbt models run completed successfully.")

        # Run dbt tests
        dbt_test_command = ["dbt", "test"]
        logger.info(f"Running dbt command: {' '.join(dbt_test_command)}")
        process_test = subprocess.run(dbt_test_command, cwd=DBT_DIR, capture_output=True, text=True, check=True)
        logger.info(f"dbt test stdout:\n{process_test.stdout}")
        if process_test.stderr:
            logger.warning(f"dbt test stderr:\n{process_test.stderr}")
        logger.info("dbt tests completed successfully.")

        logger.info("dbt transformations and tests completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed: {e}")
        logger.error(f"dbt stdout:\n{e.stdout}")
        logger.error(f"dbt stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        logger.error("dbt command not found. Is dbt installed and in PATH?")
        raise


@op(name="run_yolo_enrichment_op", description="Performs YOLO object detection on images and loads results.")
def run_yolo_enrichment():
    """
    Executes the yolo_detection.py script to enrich data with YOLO results.
    This script runs on the host machine.
    """
    logger = run_yolo_enrichment.log
    logger.info("Starting YOLO enrichment...")
    try:
        command = ["python", str(SCRIPTS_DIR / "yolo_detection.py")]
        process = subprocess.run(command, capture_output=True, text=True, check=True)
        logger.info(f"YOLO script stdout:\n{process.stdout}")
        if process.stderr:
            logger.warning(f"YOLO script stderr:\n{process.stderr}")
        logger.info("YOLO enrichment completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"YOLO enrichment failed: {e}")
        logger.error(f"YOLO script stdout:\n{e.stdout}")
        logger.error(f"YOLO script stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        logger.error("Python command not found. Is Python installed and in PATH?")
        raise


# --- Job Definition ---

@job(name="telegram_data_pipeline", description="End-to-end pipeline for Telegram medical data insights.")
def telegram_data_pipeline():
    """
    Defines the full data pipeline as a series of connected ops.
    """
    # Define dependencies:
    # 1. Scrape data first
    scrape_telegram_data()
    # 2. Load raw data to Postgres (depends on scraping)
    load_raw_to_postgres()
    # 3. Run YOLO enrichment (depends on scraping, as it needs images)
    run_yolo_enrichment()
    # 4. Run dbt transformations (depends on raw data load and YOLO enrichment)
    run_dbt_transformations()


# --- Schedule Definition ---

# Schedule to run the pipeline daily at a specific time (e.g., 2 AM UTC)
daily_pipeline_schedule = ScheduleDefinition(
    job=telegram_data_pipeline,
    cron_schedule="0 2 * * *", # Every day at 2:00 AM UTC
    execution_timezone="UTC",
    name="daily_telegram_pipeline"
)

# --- Definitions (for Dagster UI) ---
defs = Definitions(
    jobs=[telegram_data_pipeline],
    schedules=[daily_pipeline_schedule]
)
