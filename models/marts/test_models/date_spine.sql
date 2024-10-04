{{
    config(
        materialized='incremental',
        alias='date_spine'
    )
}}

WITH dates AS (
    SELECT
        TO_TIMESTAMP(DATEADD('day', -generated_number::int, (current_date + 1))) AS date
    FROM ({{ dbt_utils.generate_series(upper_bound=365) }})
    WHERE date < (SELECT date_trunc('day', max(activity_date)) FROM {{ ref('stg_user_activity') }})
    {% if is_incremental() %}
        AND date > (SELECT max(date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM dates