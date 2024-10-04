WITH date_spine_month AS (
    SELECT DISTINCT
        DATE_TRUNC('month', date) AS month
    FROM {{ ref('date_spine') }}
)

SELECT
    *
FROM date_spine_month