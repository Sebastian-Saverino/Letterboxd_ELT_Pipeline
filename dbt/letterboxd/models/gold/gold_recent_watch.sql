SELECT
    name,
    year,
    watched_date
FROM {{ ref('silver_diary') }}
ORDER BY watched_date DESC
LIMIT 1