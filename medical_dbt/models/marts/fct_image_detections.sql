-- medical_dbt/models/marts/fct_image_detections.sql

-- Fact table for image detections from YOLO.
-- This is a placeholder and will be populated in Task 3.

{{ config(materialized='table') }}

SELECT
    -- Placeholder columns for now.
    -- These will be populated with actual YOLO detection results later.
    CAST(NULL AS VARCHAR) AS image_detection_sk, -- Surrogate key for this fact
    CAST(NULL AS VARCHAR) AS message_sk,         -- Foreign key to fct_messages
    CAST(NULL AS VARCHAR) AS detected_object_class,
    CAST(NULL AS NUMERIC) AS confidence_score,
    CAST(NULL AS TEXT) AS image_path,
    CAST(NULL AS TIMESTAMP) AS detection_timestamp

WHERE 1 = 0 -- Ensures the table is created but remains empty initially
