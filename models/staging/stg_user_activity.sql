WITH source_data AS (
    SELECT
        user_id,
        activity_date
    FROM {{ source('user_event', 'user_activity') }}
)
SELECT
    user_id,
    TO_TIMESTAMP(activity_date) as activity_date
FROM source_data