SELECT
    name,
    year,
    rating
FROM {{ ref('silver_ratings') }}
ORDER BY rating ASC
LIMIT 10