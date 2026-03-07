{{ config(materialized='table') }}

SELECT
    name,
    year,
    watched_date,
    rating,
    rewatch,
    letterboxd_uri
FROM {{ ref('silver_diary') }}
WHERE watched_date IS NOT NULL
ORDER BY watched_date DESC, name ASC
LIMIT 1