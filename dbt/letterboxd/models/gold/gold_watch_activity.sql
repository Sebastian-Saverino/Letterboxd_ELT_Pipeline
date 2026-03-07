SELECT
    DATE_TRUNC('month', watched_date) AS month,
    COUNT(*) AS films_watched
FROM {{ ref('silver_diary') }}
GROUP BY 1
ORDER BY 1