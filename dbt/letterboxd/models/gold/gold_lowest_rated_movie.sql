{{ config(materialized='table') }}

SELECT
    name,
    year,
    rating,
    letterboxd_uri
FROM {{ ref('silver_ratings') }}
WHERE rating IS NOT NULL
ORDER BY rating ASC, year DESC, name ASC
LIMIT 1