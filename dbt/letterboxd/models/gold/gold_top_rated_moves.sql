SELECT
    name,
    year,
    rating
FROM {{ ref('silver_ratings') }}
ORDER BY rating DESC
LIMIT 10