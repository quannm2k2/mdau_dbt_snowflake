WITH unique_user_activity_month AS (
  SELECT DISTINCT user_id, month, active_status
  FROM {{ ref('user_activity_month') }}
)
, unique_users AS (
  SELECT DISTINCT user_id
  FROM unique_user_activity_month
)
, all_user_months AS (
  SELECT
    uu.user_id,
    dsm.month
  FROM unique_users uu
  CROSS JOIN {{ ref('date_spine_month') }} dsm
)
SELECT
  aum.user_id,
  aum.month,
  COALESCE(uam.active_status, FALSE) AS active_status
FROM all_user_months aum
LEFT JOIN unique_user_activity_month uam
  ON aum.user_id = uam.user_id
  AND aum.month = uam.month
ORDER BY aum.user_id