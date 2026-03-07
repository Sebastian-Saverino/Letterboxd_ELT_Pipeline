{{ config(materialized='table') }}

SELECT
    year,
    COUNT(*) AS films_rated,
    ROUND(AVG(rating)::numeric, 2) AS avg_rating
FROM {{ ref('silver_ratings') }}
WHERE rating IS NOT NULL
  AND year IS NOT NULL
GROUP BY year
HAVING COUNT(*) >= 3
ORDER BY avg_rating DESC, films_rated DESC, year ASC