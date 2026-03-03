{{ config(materialized='view') }}

with src as (

    select
        -- raw columns (keep the quoted names if your bronze table kept CSV headers)
        "Date" as list_date_raw,
        "Name" as name_raw,
        "Year" as year_raw,
        "Letterboxd URI" as letterboxd_uri_raw,
        "Rating" as rating_raw,
        "Rewatch" as rewatch_raw,
        "Tags" as tags_raw,
        "Watched Date" as watched_date_raw

    from {{ source('bronze', 'bronze_diary') }}

),

clean as (

    select
        -- Dates: Letterboxd exports are usually yyyy-mm-dd (but we defensively null-out blanks)
        nullif(trim(list_date_raw), '')::date as list_date,

        nullif(trim(name_raw), '') as name,

        nullif(trim(year_raw), '')::int as year,

        nullif(trim(letterboxd_uri_raw), '') as letterboxd_uri,

        -- Rating: often like "3.5" or blank
        nullif(trim(rating_raw), '')::numeric(3,1) as rating,

        -- Rewatch: often "Yes"/"" (or "No"). Normalize to boolean.
        case
            when lower(trim(rewatch_raw)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(rewatch_raw)) in ('no', 'n', 'false', '0') then false
            when nullif(trim(rewatch_raw), '') is null then false
            else null
        end as is_rewatch,

        -- Tags: keep as a single string for now (you can split later in a bridge table)
        nullif(trim(tags_raw), '') as tags,

        nullif(trim(watched_date_raw), '')::date as watched_date

    from src
)

select *
from clean;