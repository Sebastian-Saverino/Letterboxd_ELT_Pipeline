-- 

SELECT
    year,
    COUNT(*) AS watchlist_count
FROM "letterboxd_warehouse"."silver"."silver_watchlist"
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year ASC