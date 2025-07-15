-- medical_dbt/models/marts/fct_image_detections.sql

-- Fact table for image detection results from YOLO.

{{ config(materialized='table') }}

SELECT
    -- Surrogate key for this fact, combining detection_id and detected_object_class
    {{ dbt_utils.generate_surrogate_key(['rid.detection_id', 'rid.detected_object_class']) }} AS image_detection_sk,
    
    fm.message_sk,         -- Foreign key to fct_messages
    rid.detected_object_class,
    rid.confidence_score,
    rid.image_path,
    rid.detection_timestamp,

    -- Add a count metric for detections
    1 AS detection_count

FROM
    {{ source('raw', 'image_detections') }} rid
LEFT JOIN
    {{ ref('fct_messages') }} fm ON rid.message_id = fm.message_id AND rid.channel_username = fm.channel_username
-- Note: We are joining on message_id and channel_username.
-- Ensure that the channel_username stored in raw.image_detections matches the one in fct_messages for joins to work.
-- If your image filename parsing for channel_username is not robust, this join might fail.
