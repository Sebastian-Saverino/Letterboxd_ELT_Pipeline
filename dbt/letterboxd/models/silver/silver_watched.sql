{{ config(materialized='table', schema='silver') }}

with src as (
    select
        nullif(trim(list_date::text), '') as list_date_txt,
        nullif(trim(name::text), '') as name,
        nullif(trim(year::text), '') as year_txt,
        nullif(trim(letterboxd_uri::text), '') as letterboxd_uri,
        nullif(trim(rating::text), '') as rating_txt,
        nullif(trim(rewatch::text), '') as rewatch_txt,
        nullif(trim(review::text), '') as review,
        nullif(trim(tags::text), '') as tags,
        nullif(trim(watched_date::text), '') as watched_date_txt
    from {{ source('bronze', 'reviews') }}
),

typed as (
    select
        case when list_date_txt ~ '^\d{4}-\d{2}-\d{2}$' then list_date_txt::date end as list_date,
        name,
        case when year_txt ~ '^\d{4}$' then year_txt::int end as year,
        letterboxd_uri,
        case when rating_txt ~ '^\d+(\.\d+)?$' then rating_txt::numeric(3,1) end as rating,

        case
            when lower(rewatch_txt) in ('yes','y','true','t','1') then true
            when lower(rewatch_txt) in ('no','n','false','f','0') then false
            else null
        end as rewatch,

        review,
        tags,

        case
            when watched_date_txt ~ '^\d{4}-\d{2}-\d{2}$' then watched_date_txt::date
            else null
        end as watched_date
    from src
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by letterboxd_uri, watched_date
                order by list_date desc nulls last
            ) as rn
        from typed
    ) t
    where rn = 1
)

select
    list_date,
    name,
    year,
    letterboxd_uri
from deduped
where letterboxd_uri is not null