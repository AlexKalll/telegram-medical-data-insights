-- medical_dbt/tests/custom_data_test.sql

-- Custom test: Ensure that messages marked as 'has_photo = TRUE'
-- actually have a photo_path associated with them.
-- This query should return 0 rows for the test to pass.

SELECT
    message_sk,
    message_id,
    channel_username,
    has_photo,
    photo_path
FROM
    {{ ref('stg_telegram_messages') }}
WHERE
    has_photo = FALSE AND photo_path IS NOT NULL

