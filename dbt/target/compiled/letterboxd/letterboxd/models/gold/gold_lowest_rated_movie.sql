-- 

SELECT
    name,
    year,
    rating,
    letterboxd_uri
FROM "letterboxd_warehouse"."silver"."silver_ratings"
WHERE rating IS NOT NULL
ORDER BY rating ASC, year DESC, name ASC
LIMIT 1