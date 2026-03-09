
  
    

  create  table "letterboxd_warehouse"."gold"."gold_lowest_rated_movie__dbt_tmp"
  
  
    as
  
  (
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
  );
  