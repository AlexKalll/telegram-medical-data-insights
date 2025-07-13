-- medical_dbt/models/staging/stg_telegram_messages.sql

-- This staging model selects raw message data, cleans it,
-- and prepares it for further transformation into dimension and fact tables.

{{ config(materialized='view') }} -- Staging models are typically materialized as views

SELECT
    -- Primary Key for the staging model (composite of message_id and channel_username)
    -- This ensures uniqueness for messages across different channels.
    CAST(message_id AS BIGINT) AS message_id,
    CAST(channel_username AS VARCHAR) AS channel_username,

    -- Attributes for dim_channels
    CAST(channel_name AS VARCHAR) AS channel_name,

    -- Attributes for dim_dates
    CAST(message_date AS DATE) AS message_date,
    CAST(scrape_date AS DATE) AS scrape_date,

    -- Attributes for fct_messages
    CAST(message_text AS TEXT) AS message_text,
    CAST(message_length AS INTEGER) AS message_length,
    CAST(views AS INTEGER) AS views,
    CAST(forwards AS INTEGER) AS forwards,
    CAST(has_photo AS BOOLEAN) AS has_photo,
    CAST(photo_path AS TEXT) AS photo_path,

    -- Add a unique surrogate key for the message fact table
    -- This combines message_id and channel_username for a robust unique identifier
    {{ dbt_utils.generate_surrogate_key(['message_id', 'channel_username']) }} AS message_sk

FROM
    {{ source('raw', 'telegram_messages') }}

-- Add any initial filtering or basic cleaning here if necessary
-- For example, filtering out messages with no text or invalid dates
-- WHERE message_text IS NOT NULL AND message_text != ''
