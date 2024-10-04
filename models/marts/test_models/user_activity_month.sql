{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

WITH user_activity_month AS (
    SELECT
        user_id,
        DATE_TRUNC('month', activity_date) AS month,
        TRUE AS active_status
    FROM {{ ref('stg_user_activity') }}
    {% if is_incremental() %}
        WHERE month > (SELECT MAX(month) FROM {{ this }})
    {% endif %}
)

SELECT
    user_id,
    month,
    active_status
FROM user_activity_month
