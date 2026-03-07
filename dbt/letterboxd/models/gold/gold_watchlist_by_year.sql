SELECT
    year,
    COUNT(*) AS films_in_watchlist
FROM {{ ref('silver_watchlist') }}
GROUP BY year
ORDER BY year