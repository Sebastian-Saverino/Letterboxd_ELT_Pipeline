SELECT
    year,
    AVG(rating) AS avg_rating,
    COUNT(*) AS films_watched
FROM {{ ref('silver_ratings') }}
GROUP BY year
HAVING COUNT(*) >= 3
ORDER BY avg_rating DESC