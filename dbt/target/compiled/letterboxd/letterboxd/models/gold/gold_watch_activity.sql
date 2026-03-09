-- 

SELECT
    DATE_TRUNC('month', watched_date)::date AS watch_month,
    COUNT(*) AS films_watched
FROM "letterboxd_warehouse"."silver"."silver_diary"
WHERE watched_date IS NOT NULL
GROUP BY watch_month
ORDER BY watch_month