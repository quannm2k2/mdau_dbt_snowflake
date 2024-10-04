WITH user_status_cumulative AS (
    SELECT
        user_id,
        ARRAY_AGG(active_status) WITHIN GROUP (ORDER BY month) AS active_status_list,
        MAX(CASE WHEN active_status = TRUE THEN month END) AS last_active_month
    FROM {{ ref('all_user_months') }}
    GROUP BY user_id
)

SELECT
    *
FROM user_status_cumulative
