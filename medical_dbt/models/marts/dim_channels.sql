-- medical_dbt/models/marts/dim_channels.sql

-- Dimension table for Telegram channels.
-- Contains unique channel information and a surrogate key.

{{ config(materialized='table') }}

WITH ranked_channels AS (
    SELECT
        channel_username,
        channel_name,
        scrape_date,
        -- Deduplicate in case of multiple entries for the same channel_username
        -- (e.g., if channel_name changed over time, pick the latest scrape_date)
        ROW_NUMBER() OVER (PARTITION BY channel_username ORDER BY scrape_date DESC) as rn
    FROM
        {{ ref('stg_telegram_messages') }}
)
SELECT
    -- Surrogate Key for dim_channels
    -- Generates a unique ID for each channel based on its username
    {{ dbt_utils.generate_surrogate_key(['channel_username']) }} AS channel_sk,

    -- Natural Key (unique identifier from source)
    channel_username,

    -- Descriptive Attributes
    channel_name
    -- Add any other relevant channel metadata if available in raw data
    -- For example, channel creation date, subscriber count (if scraped)

FROM
    ranked_channels
WHERE rn = 1
