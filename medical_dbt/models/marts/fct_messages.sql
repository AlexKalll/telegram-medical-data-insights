-- medical_dbt/models/marts/fct_messages.sql

-- Fact table for Telegram messages.
-- Contains key metrics and foreign keys to dimension tables.

{{ config(materialized='table') }}

SELECT
    stm.message_sk, -- Surrogate key from staging, acts as primary key for this fact
    stm.message_id,
    dc.channel_sk, -- Foreign key to dim_channels
    stm.channel_username, -- ADDED: Include channel_username in the fact table
    dd.date_sk AS message_date_sk, -- Foreign key to dim_dates for message_date
    CAST(TO_CHAR(stm.scrape_date, 'YYYYMMDD') AS INTEGER) AS scrape_date_sk, -- Foreign key for scrape_date
    
    stm.message_text,
    stm.message_length,
    stm.views,
    stm.forwards,
    stm.has_photo,
    stm.photo_path,

    -- Add any other relevant metrics or flags based on text content
    CASE WHEN stm.message_text ILIKE '%urgent%' THEN TRUE ELSE FALSE END AS is_urgent_message,
    CASE WHEN stm.message_text ILIKE '%vacancy%' THEN TRUE ELSE FALSE END AS is_vacancy_message,
    
    -- Add a count metric
    1 AS message_count

FROM
    {{ ref('stg_telegram_messages') }} stm
LEFT JOIN
    {{ ref('dim_channels') }} dc ON stm.channel_username = dc.channel_username
LEFT JOIN
    {{ ref('dim_dates') }} dd ON stm.message_date = dd.full_date
