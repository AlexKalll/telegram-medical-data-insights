-- medical_dbt/models/marts/dim_dates.sql

-- Dimension table for dates.
-- Contains various date attributes for time-based analysis.

{{ config(materialized='table') }}

WITH date_spine AS (
    -- Generate a series of dates from the earliest message date to the latest scrape date
    SELECT
        GENERATE_SERIES(
            MIN(message_date),
            MAX(scrape_date),
            '1 day'::interval
        )::DATE AS full_date
    FROM
        {{ ref('stg_telegram_messages') }}
)
SELECT
    -- Surrogate Key for dim_dates (YYYYMMDD format for easy integer key)
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_sk,

    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(MONTH FROM full_date) AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(DAY FROM full_date) AS day_of_month,
    EXTRACT(DOW FROM full_date) AS day_of_week_num, -- 0=Sunday, 6=Saturday
    TO_CHAR(full_date, 'Day') AS day_of_week_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(WEEK FROM full_date) AS week_of_year,
    EXTRACT(DOY FROM full_date) AS day_of_year,
    CASE
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    -- Add more date attributes as needed (e.g., is_holiday, fiscal_period)

    -- Generate a unique incremental number for each day, as requested (though date_sk is usually sufficient)
    ROW_NUMBER() OVER (ORDER BY full_date) AS incremental_num

FROM
    date_spine
ORDER BY
    full_date
